"""Event-driven, read-only CEX <-> DEX candidate monitor.

Unlike :mod:`market_data_lab.rolling_cycle_monitor`, this monitor never waits
for a broad market sweep.  Public CEX depth is kept in persistent WebSocket
streams.  Each DEX provider runs continuously at its own safe public request
budget; every returned exact quote is evaluated immediately, then re-evaluated
on each fresh matching CEX book while that DEX quote is still timing-valid.

Only candidate lifecycle events, aggregate statistics and a bounded diagnostic
ledger are written to disk.  Raw order-book and raw quote rows stay in memory.
No exchange credentials, wallet keys, transaction payloads or orders are used.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import time
from collections import Counter, defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from market_data_lab.account_fee_audit import SpotFeeRate
from market_data_lab.account_fee_audit import load_spot_fee_audit
from market_data_lab.account_fee_audit import resolve_spot_fee_rate
from market_data_lab.cex_book_streams import PublicBookStream
from market_data_lab.cex_book_streams import build_public_book_stream
from market_data_lab.cex_book_streams import stream_health
from market_data_lab.cex_dex_cycles import CEX_BOOK_ENDPOINTS
from market_data_lab.cex_dex_cycles import DEFAULT_NETWORK_COST_FLOORS
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import CycleMarket
from market_data_lab.cex_dex_cycles import _best_dex_records
from market_data_lab.cex_dex_cycles import build_cycle_providers
from market_data_lab.cex_dex_cycles import calculate_cycle
from market_data_lab.cex_dex_cycles import market_for_cex
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import DexQuoteProvider
from market_data_lab.dex_quotes import quote_route_labels
from market_data_lab.dex_quotes import OMNISTON_WS_ENDPOINT
from market_data_lab.dex_quotes import _decimal_text
from market_data_lab.dex_quotes import _redact_url
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id
from market_data_lab.rolling_cycle_monitor import CandidateTracker
from market_data_lab.rolling_cycle_monitor import DEFAULT_CEX_TAKER_FEES
from market_data_lab.rolling_cycle_monitor import MAXIMUM_COVERAGE_MARKETS


def _parse_names(value: str) -> list[str]:
    names = [item.strip() for item in value.split(",") if item.strip()]
    unknown = [item for item in names if item not in MARKETS]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown market names: {', '.join(unknown)}")
    if not names:
        raise argparse.ArgumentTypeError("at least one market is required")
    return list(dict.fromkeys(names))


def _parse_venues(value: str) -> list[str]:
    venues = [item.strip().upper() for item in value.split(",") if item.strip()]
    unknown = [item for item in venues if item not in CEX_BOOK_ENDPOINTS]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown CEX venues: {', '.join(unknown)}")
    if not venues:
        raise argparse.ArgumentTypeError("at least one CEX venue is required")
    return list(dict.fromkeys(venues))


def _parse_decimals(value: str, name: str) -> list[Decimal]:
    try:
        values = [Decimal(item.strip()) for item in value.split(",") if item.strip()]
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"{name} must be decimal values") from exc
    if not values or any(item <= 0 or not item.is_finite() for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain finite positive values")
    return values


def _parse_costs(value: str) -> dict[str, Decimal]:
    parsed = dict(DEFAULT_NETWORK_COST_FLOORS)
    try:
        for item in value.split(","):
            chain, cost = item.strip().split("=", 1)
            parsed[chain.strip().lower()] = Decimal(cost)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError("costs must look like solana=0.01,ton=0.1") from exc
    if any(cost < 0 or not cost.is_finite() for cost in parsed.values()):
        raise argparse.ArgumentTypeError("network costs must be finite and non-negative")
    return parsed


def _parse_fees(value: str) -> dict[str, Decimal]:
    parsed = dict(DEFAULT_CEX_TAKER_FEES)
    try:
        for item in value.split(","):
            venue, fee = item.strip().split("=", 1)
            venue = venue.upper()
            if venue not in parsed:
                raise ValueError(f"unknown venue {venue}")
            parsed[venue] = Decimal(fee)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError("fees must look like MEXC=5,BYBIT=10,OKX=10,BINANCE=10") from exc
    if any(fee < 0 or fee >= 10_000 or not fee.is_finite() for fee in parsed.values()):
        raise argparse.ArgumentTypeError("fees must be finite and in [0, 10000)")
    return parsed


class ContinuousStatistics:
    """Aggregate only, with a compact bounded error ledger for diagnosis."""

    def __init__(self, *, error_ledger_limit: int = 100) -> None:
        self.dex_rounds: Counter[str] = Counter()
        self.dex_records: Counter[str] = Counter()
        self.aggregator_route_labels: Counter[str] = Counter()
        self.cex_updates: Counter[str] = Counter()
        self.cycle_observations = 0
        self.timing_valid_observations = 0
        self.positive_after_floor_observations = 0
        self.positive_with_account_verified_fee_observations = 0
        self.calculation_errors = 0
        self.provider_errors: Counter[str] = Counter()
        self.cex_errors: Counter[str] = Counter()
        self.cex_unavailable_symbols: Counter[str] = Counter()
        self.statuses: Counter[str] = Counter()
        self.routes: dict[str, dict[str, Any]] = {}
        self.errors: deque[dict[str, Any]] = deque(maxlen=error_ledger_limit)
        self.candidate_event_limit = 0
        self.candidate_events_persisted = 0
        self.candidate_events_dropped = 0
        self.last_dex_quote_at: str | None = None
        self.last_cex_update_at: str | None = None

    def add_error(self, *, kind: str, key: str, error: str) -> None:
        self.errors.append(
            {
                "at": datetime.now(UTC).isoformat(),
                "kind": kind,
                "key": key,
                "error": error[:512],
            },
        )

    def observe_dex_records(self, records: Sequence[Mapping[str, Any]]) -> None:
        for record in records:
            for label in quote_route_labels(record):
                self.aggregator_route_labels[label] += 1

    def observe_cycle(self, cycle: dict[str, Any]) -> None:
        self.cycle_observations += 1
        status = str(cycle.get("status", "unknown"))
        self.statuses[status] += 1
        if status == "calculation_error":
            self.calculation_errors += 1
            self.add_error(
                kind="calculation_error",
                key=f"{cycle.get('cex_venue')}:{cycle.get('market')}",
                error=str(cycle.get("error", "unknown calculation error")),
            )
            return
        timing_valid = cycle.get("timing_valid") is True
        positive = cycle.get("positive_after_minimum_network") is True
        account_fee_verified = cycle.get("cex_fee_account_verified") is True
        if timing_valid:
            self.timing_valid_observations += 1
        if positive:
            self.positive_after_floor_observations += 1
        if positive and account_fee_verified:
            self.positive_with_account_verified_fee_observations += 1
        route_key = "|".join(
            str(cycle.get(field, ""))
            for field in ("cex_venue", "market", "cycle_direction", "requested_notional_quote")
        )
        route = self.routes.setdefault(
            route_key,
            {
                "observations": 0,
                "timing_valid_observations": 0,
                "positive_after_minimum_network": 0,
                "positive_with_account_verified_fee": 0,
                "best_net_edge_after_minimum_network_bps": None,
            },
        )
        route["observations"] += 1
        if timing_valid:
            route["timing_valid_observations"] += 1
        if positive:
            route["positive_after_minimum_network"] += 1
        if positive and account_fee_verified:
            route["positive_with_account_verified_fee"] += 1
        edge = cycle.get("net_edge_after_minimum_network_bps")
        if edge is not None:
            try:
                decimal_edge = Decimal(str(edge))
            except InvalidOperation:
                return
            existing = route["best_net_edge_after_minimum_network_bps"]
            if existing is None or decimal_edge > Decimal(str(existing)):
                route["best_net_edge_after_minimum_network_bps"] = _decimal_text(decimal_edge)

    def snapshot(
        self,
        *,
        started_at: str,
        duration_wall_seconds: float,
        tracker: CandidateTracker,
        streams: Mapping[str, PublicBookStream],
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "status": "running",
            "started_at": started_at,
            "updated_at": datetime.now(UTC).isoformat(),
            "duration_wall_seconds": round(duration_wall_seconds, 6),
            "raw_market_data_persisted": False,
            "dex_rounds": dict(sorted(self.dex_rounds.items())),
            "dex_records": dict(sorted(self.dex_records.items())),
            "aggregator_route_labels": dict(sorted(self.aggregator_route_labels.items())),
            "cex_updates": dict(sorted(self.cex_updates.items())),
            "last_dex_quote_at": self.last_dex_quote_at,
            "last_cex_update_at": self.last_cex_update_at,
            "cycle_observations": self.cycle_observations,
            "timing_valid_observations": self.timing_valid_observations,
            "positive_after_minimum_network_observations": self.positive_after_floor_observations,
            "positive_with_account_verified_fee_observations": (
                self.positive_with_account_verified_fee_observations
            ),
            "calculation_errors": self.calculation_errors,
            "provider_errors": dict(sorted(self.provider_errors.items())),
            "cex_errors": dict(sorted(self.cex_errors.items())),
            "cex_unavailable_symbols": dict(sorted(self.cex_unavailable_symbols.items())),
            "cycle_statuses": dict(sorted(self.statuses.items())),
            "candidate_lifecycle": {
                "started": tracker.started,
                "improved": tracker.improved,
                "closed": tracker.closed,
                "active": len(tracker.active),
            },
            "candidate_event_persistence": {
                "limit": self.candidate_event_limit,
                "persisted": self.candidate_events_persisted,
                "dropped_after_limit": self.candidate_events_dropped,
                "format": "compact_candidate_lifecycle_v1",
            },
            "stream_health": {venue: stream_health(stream) for venue, stream in sorted(streams.items())},
            "routes": dict(sorted(self.routes.items())),
            "recent_errors": list(self.errors),
        }


def _compact_candidate_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Keep candidate evidence useful without persisting raw quote payloads.

    Exact DEX responses can contain full route plans and account metadata.  In
    a high-frequency monitor those fields would turn a small candidate log into
    a large raw-data archive.  The compact form retains the timing, PnL and
    route identity needed to decide which observation deserves a later replay.
    """

    compact = {
        key: event.get(key)
        for key in (
            "schema_version",
            "event",
            "candidate_key",
            "started_at",
            "last_seen_at",
            "event_at",
            "duration_seconds",
            "positive_observations",
            "max_net_edge_after_minimum_network_bps",
            "max_net_pnl_after_minimum_network_quote",
            "close_reason",
        )
        if key in event
    }
    best = event.get("best_cycle")
    if isinstance(best, Mapping):
        compact["best_cycle"] = {
            key: best.get(key)
            for key in (
                "round_id",
                "market",
                "chain",
                "dex_provider",
                "dex_pair",
                "cex_venue",
                "cex_symbol",
                "cycle_direction",
                "requested_notional_quote",
                "quote_symbol",
                "asset_equivalence",
                "status",
                "timing_valid",
                "response_skew_ms",
                "max_response_skew_ms",
                "dex_request_rtt_ms",
                "cex_request_rtt_ms",
                "dex_average_price",
                "cex_vwap",
                "cex_taker_fee_bps",
                "cex_taker_buy_fee_bps",
                "cex_taker_sell_fee_bps",
                "cex_fee_side_used",
                "cex_fee_source",
                "cex_fee_account_verified",
                "cex_fee_assumptions",
                "minimum_network_cost_quote",
                "net_edge_after_minimum_network_bps",
                "net_pnl_after_minimum_network_quote",
            )
            if key in best
        }
    return compact


def _calculation_error_cycle(
    *,
    market: CycleMarket,
    venue: str,
    dex_record: dict[str, Any],
    error: BaseException,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "round_id": dex_record.get("round_id"),
        "market": market.name,
        "chain": market.chain,
        "dex_provider": market.provider,
        "dex_pair": market.dex_pair,
        "cex_venue": venue,
        "cex_symbol": market_for_cex(market, venue).cex_symbol,
        "cycle_direction": (
            "buy_dex_sell_cex" if dex_record.get("direction") == "buy_base" else "buy_cex_sell_dex"
        ),
        "requested_notional_quote": dex_record.get("requested_notional_quote"),
        "quote_symbol": market.quote_symbol,
        "status": "calculation_error",
        "error": f"{type(error).__name__}: {error}",
        "timing_valid": False,
    }


def _provider_gate_key(market: CycleMarket) -> str | None:
    """Return an extra shared budget for providers without an internal pacer."""

    name = market.provider
    if name.startswith("STONFI"):
        return "STONFI"
    if name.startswith("OMNISTON"):
        return "OMNISTON"
    if name.startswith("UNISWAP_BASE"):
        return "UNISWAP_BASE"
    if name.startswith("UNISWAP_POLYGON"):
        return "UNISWAP_POLYGON"
    # Raydium and Jupiter already share a request-start pacer created by
    # build_cycle_providers.  Adding a second one would make their advertised
    # safe capacity artificially lower.
    return None


async def record_continuous_cycle_monitor(
    markets: Sequence[CycleMarket],
    providers: Mapping[str, DexQuoteProvider],
    *,
    notionals: Sequence[Decimal],
    duration_seconds: float | None,
    cex_venues: Sequence[str],
    cex_taker_fees: Mapping[str, Decimal],
    network_cost_floors: Mapping[str, Decimal],
    max_response_skew_ms: Decimal,
    max_dex_cache_age_ms: Decimal,
    output_directory: Path,
    proxy_url: str | None,
    timeout_seconds: float,
    history_capacity_per_symbol: int = 256,
    stats_flush_seconds: float = 2.0,
    max_persisted_candidate_events: int = 5_000,
    auxiliary_provider_min_round_intervals: Mapping[str, float] | None = None,
    shared_provider_gates: Mapping[str, AsyncRequestPacer] | None = None,
    stdout_candidates: bool = False,
    cex_streams: Mapping[str, PublicBookStream] | None = None,
    account_fee_rates: Mapping[tuple[str, str], SpotFeeRate] | None = None,
    fee_audit_file: Path | None = None,
    require_account_verified_fees_for_candidates: bool = True,
) -> dict[str, Any]:
    """Run a continuous read-only monitor, optionally for a bounded duration.

    ``duration_seconds=None`` runs until the surrounding supervisor cancels the
    task.  Raw prices never accumulate on disk in either mode.
    """

    if output_directory.exists():
        raise FileExistsError(f"Refusing to overwrite continuous monitor output: {output_directory}")
    if (
        (duration_seconds is not None and duration_seconds <= 0)
        or timeout_seconds <= 0
        or stats_flush_seconds <= 0
    ):
        raise ValueError("duration, timeout and stats flush interval must be positive")
    if max_response_skew_ms < 0 or max_dex_cache_age_ms < 0:
        raise ValueError("timing windows cannot be negative")
    if max_persisted_candidate_events <= 0:
        raise ValueError("candidate event limit must be positive")
    if not markets or not notionals:
        raise ValueError("markets and notionals cannot be empty")
    if len({market.provider for market in markets}) != len(markets):
        raise ValueError("continuous monitor requires one market per DEX provider")
    if any(market.provider not in providers for market in markets):
        raise ValueError("every market needs its DEX provider")
    if any(venue not in CEX_BOOK_ENDPOINTS for venue in cex_venues):
        raise ValueError("unsupported CEX venue")
    if any(venue not in cex_taker_fees for venue in cex_venues):
        raise ValueError("a CEX taker fee is required for every selected venue")
    if any(market.chain not in network_cost_floors for market in markets):
        raise ValueError("a network-cost floor is required for every selected chain")
    if fee_audit_file is not None and account_fee_rates is None:
        raise ValueError("fee_audit_file requires loaded account_fee_rates")
    for (venue, symbol), fee in (account_fee_rates or {}).items():
        if (
            venue.upper() not in CEX_BOOK_ENDPOINTS
            or not symbol
            or fee.venue.upper() != venue.upper()
            or fee.symbol.upper() != symbol.upper()
        ):
            raise ValueError("account fee-rate mapping contains an invalid venue or symbol")

    output_directory.mkdir(parents=True)
    network_route = configure_process_network_route(proxy_url)
    started_at = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()
    stats = ContinuousStatistics()
    stats.candidate_event_limit = max_persisted_candidate_events
    tracker = CandidateTracker(Decimal("0"))
    stop_event = asyncio.Event()
    candidate_path = output_directory / "candidate_events.jsonl"
    stats_path = output_directory / "stats.json"

    symbols_by_venue: dict[str, list[str]] = {
        venue: sorted(
            {
                market_for_cex(market, venue).cex_symbol
                for market in markets
            },
        )
        for venue in cex_venues
    }
    effective_fees: dict[tuple[str, str], SpotFeeRate] = {
        (venue, symbol): resolve_spot_fee_rate(
            venue=venue,
            symbol=symbol,
            fallback_taker_bps=cex_taker_fees[venue],
            account_fee_rates=account_fee_rates,
        )
        for venue, symbols in symbols_by_venue.items()
        for symbol in symbols
    }
    active_streams: dict[str, PublicBookStream] = {}
    streams_to_start: dict[str, PublicBookStream] = dict(cex_streams or {})
    if not streams_to_start:
        streams_to_start = {
            venue: build_public_book_stream(
                venue,
                symbols_by_venue[venue],
                timeout_seconds=timeout_seconds,
                proxy_url=proxy_url,
                history_capacity_per_symbol=history_capacity_per_symbol,
            )
            for venue in cex_venues
        }
    else:
        unexpected = set(streams_to_start) - set(cex_venues)
        if unexpected:
            raise ValueError(f"test CEX streams contain unrequested venues: {sorted(unexpected)}")

    markets_by_venue_symbol: dict[tuple[str, str], list[CycleMarket]] = defaultdict(list)
    for market in markets:
        for venue in cex_venues:
            markets_by_venue_symbol[(venue, market_for_cex(market, venue).cex_symbol)].append(market)

    cache_by_market: dict[str, dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    cache_age_ns = int(max_dex_cache_age_ms * Decimal(1_000_000))
    reported_unavailable_books: set[str] = set()
    quote_gates = dict(shared_provider_gates or {})
    quote_gates.update(
        {
            key: AsyncRequestPacer(interval)
            for key, interval in (auxiliary_provider_min_round_intervals or {}).items()
            if interval > 0 and key not in quote_gates
        },
    )

    def persist_candidate_event(output: Any, event: dict[str, Any]) -> None:
        if stats.candidate_events_persisted >= max_persisted_candidate_events:
            stats.candidate_events_dropped += 1
            return
        compact = _compact_candidate_event(event)
        output.write(json.dumps(compact, ensure_ascii=False, separators=(",", ":")) + "\n")
        stats.candidate_events_persisted += 1
        if stdout_candidates:
            print(json.dumps(compact, ensure_ascii=False, separators=(",", ":")), flush=True)

    def evaluate(
        output: Any,
        *,
        market: CycleMarket,
        venue: str,
        dex_record: dict[str, Any],
        book: BookSnapshot,
        observed_realtime_ns: int,
    ) -> None:
        try:
            cex_market = market_for_cex(market, venue)
            effective_fee = effective_fees[(venue, cex_market.cex_symbol)]
            cycle = calculate_cycle(
                market=cex_market,
                dex_record=dex_record,
                book=book,
                cex_taker_fee_bps=cex_taker_fees[venue],
                cex_buy_taker_fee_bps=effective_fee.taker_buy_bps,
                cex_sell_taker_fee_bps=effective_fee.taker_sell_bps,
                cex_fee_source=effective_fee.source,
                cex_fee_account_verified=effective_fee.account_verified,
                cex_fee_assumptions=effective_fee.assumptions,
                network_cost_floor_quote=network_cost_floors[market.chain],
                max_response_skew_ms=max_response_skew_ms,
                cex_venue=venue,
            )
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            cycle = _calculation_error_cycle(
                market=market,
                venue=venue,
                dex_record=dex_record,
                error=exc,
            )
        stats.observe_cycle(cycle)
        if cycle.get("status") != "calculation_error":
            if not require_account_verified_fees_for_candidates:
                cycle["candidate_eligible_with_account_verified_fee"] = True
            for event in tracker.observe(cycle, observed_realtime_ns=observed_realtime_ns):
                persist_candidate_event(output, event)

    async def quote_worker(output: Any, market: CycleMarket) -> None:
        provider = providers[market.provider]
        gate = quote_gates.get(_provider_gate_key(market) or "")
        round_id = 0
        while not stop_event.is_set():
            if gate is not None:
                await gate.wait()
            try:
                records = await provider.quote_round(round_id, notionals)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stats.provider_errors[market.provider] += 1
                stats.add_error(
                    kind="dex_provider_exception",
                    key=market.provider,
                    error=f"{type(exc).__name__}: {exc}",
                )
                await asyncio.sleep(0.1)
                continue
            stats.dex_rounds[market.provider] += 1
            stats.dex_records[market.provider] += len(records)
            stats.observe_dex_records(records)
            stats.last_dex_quote_at = datetime.now(UTC).isoformat()
            selected = _best_dex_records(records)
            for (notional, direction), record in selected.items():
                if record.get("status") != "ok":
                    stats.provider_errors[market.provider] += 1
                    stats.add_error(
                        kind="dex_quote_error",
                        key=market.provider,
                        error=str(record.get("error", record.get("status"))),
                    )
                    continue
                cache_by_market[market.name][(notional, direction)] = record
                target_ns = int(record["response_received_realtime_ns"])
                for venue, stream in active_streams.items():
                    symbol = market_for_cex(market, venue).cex_symbol
                    book = stream.nearest_snapshot(symbol, target_ns)
                    if book is None:
                        unavailable_key = f"{venue}:{market.name}"
                        if unavailable_key not in reported_unavailable_books:
                            # A symbol absent from an otherwise healthy public
                            # feed is a coverage limitation, not a repeating
                            # provider failure.  Record it once and continue
                            # monitoring the venues where the asset exists.
                            reported_unavailable_books.add(unavailable_key)
                            stats.cex_unavailable_symbols[unavailable_key] += 1
                            stats.add_error(
                                kind="cex_symbol_unavailable",
                                key=unavailable_key,
                                error=f"no public websocket book for {symbol}",
                            )
                        continue
                    evaluate(
                        output,
                        market=market,
                        venue=venue,
                        dex_record=record,
                        book=book,
                        observed_realtime_ns=target_ns,
                    )
            round_id += 1
            # Real providers always await network I/O; this cooperative yield
            # keeps deterministic in-memory test providers from monopolising
            # the event loop.
            await asyncio.sleep(0)

    async def cex_update_worker(output: Any, venue: str, stream: PublicBookStream) -> None:
        while not stop_event.is_set():
            book = await stream.next_update()
            stats.cex_updates[venue] += 1
            stats.last_cex_update_at = datetime.now(UTC).isoformat()
            for market in markets_by_venue_symbol.get((venue, book.symbol), ()):
                for record in tuple(cache_by_market[market.name].values()):
                    received_ns = int(record.get("response_received_realtime_ns", 0))
                    if time.time_ns() - received_ns > cache_age_ns:
                        continue
                    evaluate(
                        output,
                        market=market,
                        venue=venue,
                        dex_record=record,
                        book=book,
                        observed_realtime_ns=book.response.received_realtime_ns,
                    )

    async def periodic_stats_writer() -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=stats_flush_seconds)
            except TimeoutError:
                elapsed = time.monotonic() - started_monotonic
                atomic_json(
                    stats_path,
                    stats.snapshot(
                        started_at=started_at,
                        duration_wall_seconds=elapsed,
                        tracker=tracker,
                        streams=active_streams,
                    ),
                )

    manifest: dict[str, Any] = {
        "schema_version": 1,
        "status": "starting",
        "started_at": started_at,
        "stopped_at": None,
        "duration_requested_seconds": duration_seconds,
        "mode": "event_driven_continuous",
        "markets": [asdict(market) for market in markets],
        "cex": {
            "venues": list(cex_venues),
            "websocket_symbols": symbols_by_venue,
            # Kept for older report readers.  These are fallbacks, not a
            # claim about the account that will eventually execute.
            "taker_fee_bps": {venue: _decimal_text(cex_taker_fees[venue]) for venue in cex_venues},
            "configured_fallback_taker_fee_bps": {
                venue: _decimal_text(cex_taker_fees[venue]) for venue in cex_venues
            },
            "account_fee_audit": {
                "file": str(fee_audit_file.resolve()) if fee_audit_file is not None else None,
                "rates_loaded": len(account_fee_rates or {}),
                "credentials_read_by_this_monitor_process": False,
                "separate_read_only_audit_was_supplied": fee_audit_file is not None,
                "matching_effective_rates_by_symbol": {
                    f"{venue}:{symbol}": {
                        "taker_buy_bps": _decimal_text(fee.taker_buy_bps),
                        "taker_sell_bps": _decimal_text(fee.taker_sell_bps),
                        "account_verified": fee.account_verified,
                        "source": fee.source,
                        "assumptions": list(fee.assumptions),
                    }
                    for (venue, symbol), fee in sorted(effective_fees.items())
                },
                "candidate_policy": (
                    "matching account-verified symbol fees are required"
                    if require_account_verified_fees_for_candidates
                    else "public baseline fees are allowed for research candidates and remain explicitly unverified"
                ),
            },
        },
        "dex": {
            "providers": [providers[market.provider].config() for market in markets],
            "exact_quote_cache_max_age_ms": _decimal_text(max_dex_cache_age_ms),
            "max_response_skew_ms": _decimal_text(max_response_skew_ms),
            "auxiliary_provider_min_round_intervals_seconds": dict(
                sorted((auxiliary_provider_min_round_intervals or {}).items()),
            ),
        },
        "minimum_network_cost_quote_by_chain": {
            chain: _decimal_text(cost) for chain, cost in network_cost_floors.items()
        },
        "retention": {
            "raw_market_data_persisted": False,
            "max_persisted_candidate_events": max_persisted_candidate_events,
            "policy": "only compact candidate lifecycle events, aggregate stats and a bounded error ledger are persisted",
        },
        "network_route": network_route,
        "api_credentials_used": any(
            provider.config().get("api_credentials_used") is True for provider in providers.values()
        ),
        "wallet_or_private_key_used": False,
        "transactions_submitted": False,
        "model_scope": {
            "included": [
                "public CEX WebSocket depth retained in memory",
                "DEX exact-input quote with returned route fee and price impact",
                "CEX depth walk, side-specific account-audited CEX taker fee when available and minimum network-cost floor",
            ],
            "excluded": [
                "withdrawal, deposit, bridge, wrapper redemption and rebalance costs",
                "priority fee, inclusion probability and state change before execution",
                "fill probability, inventory, borrow and capital costs",
            ],
            "interpretation": (
                "A positive candidate is not an order instruction or proof of executable arbitrage. "
                + (
                    "Baseline-only positives are diagnostic only and are not written as candidates."
                    if require_account_verified_fees_for_candidates
                    else "Baseline-only positives may be retained as explicitly unverified research candidates."
                )
            ),
        },
        "files": {
            "candidate_events": str(candidate_path.resolve()),
            "stats": str(stats_path.resolve()),
            "manifest": str((output_directory / "manifest.json").resolve()),
        },
        "error": None,
        "warning": None,
    }
    atomic_json(output_directory / "manifest.json", manifest)

    final_status = "completed"
    final_error: str | None = None
    workers: list[asyncio.Task[None]] = []
    try:
        starts = await asyncio.gather(
            *(stream.start() for stream in streams_to_start.values()),
            return_exceptions=True,
        )
        for venue, stream, result in zip(streams_to_start, streams_to_start.values(), starts, strict=True):
            if isinstance(result, BaseException):
                stats.cex_errors[f"{venue}:websocket_start"] += 1
                stats.add_error(
                    kind="cex_stream_start",
                    key=venue,
                    error=f"{type(result).__name__}: {result}",
                )
                with contextlib.suppress(Exception):
                    await stream.close()
            else:
                active_streams[venue] = stream
        if not active_streams:
            raise RuntimeError("no CEX websocket stream produced an initial valid book")

        with candidate_path.open("x", encoding="utf-8", buffering=1) as candidate_output:
            workers = [
                *(asyncio.create_task(quote_worker(candidate_output, market)) for market in markets),
                *(
                    asyncio.create_task(cex_update_worker(candidate_output, venue, stream))
                    for venue, stream in active_streams.items()
                ),
                asyncio.create_task(periodic_stats_writer()),
            ]
            try:
                if duration_seconds is None:
                    await stop_event.wait()
                else:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=duration_seconds)
                    except TimeoutError:
                        pass
            finally:
                stop_event.set()
                for task in workers:
                    task.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                workers.clear()
                close_reason = (
                    "requested_duration_elapsed" if duration_seconds is not None else "scanner_stopped"
                )
                for event in tracker.close_all(reason=close_reason):
                    persist_candidate_event(candidate_output, event)
    except asyncio.CancelledError:
        final_status = "stopped"
    except Exception as exc:
        final_status = "error"
        final_error = f"{type(exc).__name__}: {exc}"
    finally:
        stop_event.set()
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        for stream in streams_to_start.values():
            with contextlib.suppress(Exception):
                await stream.close()

    elapsed = time.monotonic() - started_monotonic
    final_stats = stats.snapshot(
        started_at=started_at,
        duration_wall_seconds=elapsed,
        tracker=tracker,
        streams=active_streams,
    )
    final_stats["status"] = final_status
    atomic_json(stats_path, final_stats)
    manifest.update(
        status=final_status,
        stopped_at=datetime.now(UTC).isoformat(),
        duration_wall_seconds=round(elapsed, 6),
        error=final_error,
        warning=("one or more CEX websocket streams were unavailable" if stats.cex_errors else None),
    )
    atomic_json(output_directory / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", type=_parse_names, default=list(MAXIMUM_COVERAGE_MARKETS))
    parser.add_argument("--cex-venues", type=_parse_venues, default=["MEXC", "BYBIT", "OKX", "BINANCE"])
    parser.add_argument("--notionals", type=lambda value: _parse_decimals(value, "notionals"), default=[Decimal("100")])
    parser.add_argument("--duration-seconds", type=float, default=600.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-response-skew-ms", type=Decimal, default=Decimal("300"))
    parser.add_argument("--max-dex-cache-age-ms", type=Decimal, default=Decimal("300"))
    parser.add_argument("--history-capacity-per-symbol", type=int, default=256)
    parser.add_argument("--stats-flush-seconds", type=float, default=2.0)
    parser.add_argument("--max-persisted-candidate-events", type=int, default=5_000)
    parser.add_argument("--minimum-network-costs", type=_parse_costs, default=dict(DEFAULT_NETWORK_COST_FLOORS))
    parser.add_argument("--cex-taker-fees-bps", type=_parse_fees, default=dict(DEFAULT_CEX_TAKER_FEES))
    parser.add_argument(
        "--cex-fee-audit-file",
        type=Path,
        help=(
            "Completed JSON from audit-cex-fees. Matching account-verified symbol rates replace "
            "public fallbacks; missing symbols remain diagnostic-only."
        ),
    )
    parser.add_argument("--raydium-slippage-bps", type=int, default=50)
    parser.add_argument("--raydium-min-request-interval-seconds", type=float, default=0.65)
    parser.add_argument("--jupiter-api-key-env", default="JUPITER_API_KEY")
    parser.add_argument("--jupiter-min-request-interval-seconds", type=float)
    parser.add_argument("--stonfi-slippage-tolerance", type=Decimal, default=Decimal("0.005"))
    parser.add_argument("--omniston-ws-url", default=OMNISTON_WS_ENDPOINT)
    parser.add_argument("--omniston-quote-selection-window-seconds", type=float, default=0.5)
    parser.add_argument("--omniston-max-price-slippage-bps", type=int, default=50)
    parser.add_argument("--omniston-max-routes", type=int, default=4)
    parser.add_argument("--omniston-allow-risky-routes", action="store_true")
    parser.add_argument("--base-rpc-url", default="https://mainnet-preconf.base.org")
    parser.add_argument("--polygon-rpc-url", default="https://polygon.drpc.org")
    parser.add_argument("--stonfi-min-round-interval-seconds", type=float, default=1.0)
    parser.add_argument("--omniston-min-round-interval-seconds", type=float, default=1.0)
    parser.add_argument("--uniswap-base-min-round-interval-seconds", type=float, default=0.5)
    parser.add_argument("--uniswap-polygon-min-round-interval-seconds", type=float, default=0.5)
    parser.add_argument("--stdout-candidates", action="store_true")
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/continuous-cycles"))
    parser.add_argument("--run-id")
    return parser


def main() -> None:
    args = _parser().parse_args()
    numeric_values = (
        args.duration_seconds,
        args.timeout_seconds,
        args.stats_flush_seconds,
        args.raydium_min_request_interval_seconds,
        args.stonfi_min_round_interval_seconds,
        args.omniston_min_round_interval_seconds,
        args.uniswap_base_min_round_interval_seconds,
        args.uniswap_polygon_min_round_interval_seconds,
    )
    if any(value <= 0 for value in numeric_values):
        raise SystemExit("all continuous timing intervals must be positive")
    if (
        args.history_capacity_per_symbol <= 0
        or args.max_persisted_candidate_events <= 0
        or args.max_response_skew_ms < 0
        or args.max_dex_cache_age_ms < 0
    ):
        raise SystemExit("capacities must be positive and timing windows non-negative")
    if args.raydium_slippage_bps < 0 or args.omniston_max_price_slippage_bps < 0:
        raise SystemExit("slippage values cannot be negative")
    if args.jupiter_min_request_interval_seconds is not None and args.jupiter_min_request_interval_seconds < 0:
        raise SystemExit("Jupiter request interval cannot be negative")
    if not Decimal(0) <= args.stonfi_slippage_tolerance < Decimal(1):
        raise SystemExit("STON.fi slippage tolerance must be in [0, 1)")
    if args.omniston_max_routes <= 0 or args.omniston_quote_selection_window_seconds < 0:
        raise SystemExit("Omniston settings are invalid")
    try:
        account_fee_rates = (
            load_spot_fee_audit(args.cex_fee_audit_file)
            if args.cex_fee_audit_file is not None
            else None
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    run_id = args.run_id or default_run_id("continuous-cex-dex")
    validate_run_id(run_id)
    providers = build_cycle_providers(
        args.markets,
        base_rpc_url=args.base_rpc_url,
        polygon_rpc_url=args.polygon_rpc_url,
        fee_tiers=(100, 500, 3000),
        proxy_url=args.proxy_url,
        timeout_seconds=args.timeout_seconds,
        raydium_slippage_bps=args.raydium_slippage_bps,
        raydium_min_request_interval_seconds=args.raydium_min_request_interval_seconds,
        stonfi_slippage_tolerance=args.stonfi_slippage_tolerance,
        jupiter_api_key=os.getenv(args.jupiter_api_key_env) if args.jupiter_api_key_env else None,
        jupiter_min_request_interval_seconds=args.jupiter_min_request_interval_seconds,
        omniston_ws_url=args.omniston_ws_url,
        omniston_quote_selection_window_seconds=args.omniston_quote_selection_window_seconds,
        omniston_max_price_slippage_bps=args.omniston_max_price_slippage_bps,
        omniston_max_routes=args.omniston_max_routes,
        omniston_allow_risky_routes=args.omniston_allow_risky_routes,
    )
    manifest = asyncio.run(
        record_continuous_cycle_monitor(
            [MARKETS[name] for name in args.markets],
            providers,
            notionals=args.notionals,
            duration_seconds=args.duration_seconds,
            cex_venues=args.cex_venues,
            cex_taker_fees=args.cex_taker_fees_bps,
            network_cost_floors=args.minimum_network_costs,
            max_response_skew_ms=args.max_response_skew_ms,
            max_dex_cache_age_ms=args.max_dex_cache_age_ms,
            output_directory=args.output_root / run_id,
            proxy_url=args.proxy_url,
            timeout_seconds=args.timeout_seconds,
            history_capacity_per_symbol=args.history_capacity_per_symbol,
            stats_flush_seconds=args.stats_flush_seconds,
            max_persisted_candidate_events=args.max_persisted_candidate_events,
            auxiliary_provider_min_round_intervals={
                "STONFI": args.stonfi_min_round_interval_seconds,
                "OMNISTON": args.omniston_min_round_interval_seconds,
                "UNISWAP_BASE": args.uniswap_base_min_round_interval_seconds,
                "UNISWAP_POLYGON": args.uniswap_polygon_min_round_interval_seconds,
            },
            stdout_candidates=args.stdout_candidates,
            account_fee_rates=account_fee_rates,
            fee_audit_file=args.cex_fee_audit_file,
        ),
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    if manifest["status"] == "error":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
