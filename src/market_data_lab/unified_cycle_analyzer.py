"""Event-driven, read-only cycle analysis for the unified market-data bus.

The collector owns all public WebSocket and quote-provider connections.  This
module is deliberately a *consumer* of that state: it never opens a new venue
connection, requests an extra quote, signs anything, or submits an order.

It evaluates two explicitly modeled cycle families when compatible updates
arrive close enough in time:

* one exact-input DEX swap closed against one CEX spot book; and
* one exact-input cross-asset DEX swap closed through two CEX spot books.

Only compact lifecycle events for timing-valid, net-positive *modelled*
cycles are written to disk.  The records retain the public/account-fee status
and all execution caveats; a positive event is never an execution signal.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter, defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from market_data_lab.account_fee_audit import SpotFeeRate
from market_data_lab.account_fee_audit import load_spot_fee_audit
from market_data_lab.account_fee_audit import resolve_spot_fee_rate
from market_data_lab.cex_dex_cycles import DEFAULT_NETWORK_COST_FLOORS
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import CycleMarket
from market_data_lab.cex_dex_cycles import calculate_cycle
from market_data_lab.cex_dex_cycles import market_for_cex
from market_data_lab.live_common import atomic_json
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.rolling_cycle_monitor import DEFAULT_CEX_TAKER_FEES
from market_data_lab.solana_realtime_scanner import CexBookStateSource
from market_data_lab.triangle_cycle_monitor import TRIANGLE_MARKETS
from market_data_lab.triangle_cycle_monitor import TriangleMarket
from market_data_lab.triangle_cycle_monitor import calculate_triangle_cycle
from market_data_lab.triangle_cycle_monitor import cex_symbol


# These are deliberately conservative public defaults, not assertions about a
# user's account tier, promotions, fee-token discount, or symbol exception.
# Bitget is not covered by the existing read-only account-fee adapter yet, so
# it remains explicitly non-verified too.
DEFAULT_PUBLIC_SPOT_TAKER_FEES: dict[str, Decimal] = {
    **DEFAULT_CEX_TAKER_FEES,
    "BITGET": Decimal("10"),
}


def _decimal_text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _utc_iso_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, UTC).isoformat()


def _notional_key(value: Decimal | None) -> str | None:
    if value is None or not value.is_finite() or value <= 0:
        return None
    return format(value, "f")


@dataclass
class _ActiveCandidate:
    key: str
    analysis_kind: str
    started_realtime_ns: int
    started_at: str
    last_seen_realtime_ns: int
    last_seen_at: str
    observations: int
    max_edge_bps: Decimal
    max_pnl_quote: Decimal
    best_cycle: dict[str, Any]


class UnifiedCycleAnalyzer:
    """Coalesce public quote/book events into compact candidate lifecycles.

    The event handler only marks a bounded set of route checks dirty.  A small
    worker performs depth walks at most once per coalescing interval, so a hot
    CEX book cannot starve the collector's common event bus.
    """

    def __init__(
        self,
        *,
        output_directory: Path,
        cex_sources: Sequence[CexBookStateSource],
        fee_audit_file: Path | None = None,
        max_response_skew_ms: Decimal = Decimal("1000"),
        minimum_net_edge_bps: Decimal = Decimal("0"),
        coalesce_interval_ms: float = 50.0,
        max_candidate_events: int = 5_000,
    ) -> None:
        if max_response_skew_ms <= 0 or not max_response_skew_ms.is_finite():
            raise ValueError("max_response_skew_ms must be finite and positive")
        if not minimum_net_edge_bps.is_finite():
            raise ValueError("minimum_net_edge_bps must be finite")
        if coalesce_interval_ms <= 0:
            raise ValueError("coalesce_interval_ms must be positive")
        if max_candidate_events <= 0:
            raise ValueError("max_candidate_events must be positive")
        names = [source.name for source in cex_sources]
        if len(names) != len(set(names)):
            raise ValueError("CEX source names must be unique")

        self.output_directory = output_directory
        self.analysis_directory = output_directory / "cycle_analysis"
        self.candidate_events_path = self.analysis_directory / "candidate_events.jsonl"
        self.stats_path = self.analysis_directory / "stats.json"
        self.max_response_skew_ms = max_response_skew_ms
        self.minimum_net_edge_bps = minimum_net_edge_bps
        self.coalesce_interval_seconds = coalesce_interval_ms / 1_000
        self.max_candidate_events = max_candidate_events
        self._spot_cex_sources = {
            source.config.venue.upper(): source
            for source in cex_sources
            if source.config.category == "spot"
        }
        self._spot_cex_by_name = {source.name: source for source in self._spot_cex_sources.values()}
        self._direct_markets = {market.provider: market for market in MARKETS.values()}
        self._triangle_markets = {market.provider: market for market in TRIANGLE_MARKETS}
        self._direct_quotes: dict[tuple[str, str, str], ExactInputQuote] = {}
        self._triangle_quotes: dict[tuple[str, str, str], ExactInputQuote] = {}
        self._quote_keys_by_direct_provider: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
        self._quote_keys_by_triangle_provider: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
        self._direct_by_book: dict[tuple[str, str], set[str]] = defaultdict(set)
        self._triangle_by_book: dict[tuple[str, str], set[str]] = defaultdict(set)
        self._build_book_indexes()

        self._dirty_direct: set[tuple[str, str, str, str]] = set()
        self._dirty_triangle: set[tuple[str, str, str, str]] = set()
        self._wake = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        self._closed = False
        self._active: dict[str, _ActiveCandidate] = {}
        self._candidate_events: deque[dict[str, Any]] = deque()
        self._candidate_started = 0
        self._candidate_improved = 0
        self._candidate_closed = 0
        self._counts: Counter[str] = Counter()
        self._route_stats: dict[str, dict[str, Any]] = {}
        self._recent_errors: deque[str] = deque(maxlen=20)
        self._fee_rates, self._fee_audit_error = self._load_fee_audit(fee_audit_file)
        self._started_at = datetime.now(UTC).isoformat()

    def _build_book_indexes(self) -> None:
        for venue in self._spot_cex_sources:
            for provider, market in self._direct_markets.items():
                self._direct_by_book[(venue, market_for_cex(market, venue).cex_symbol)].add(provider)
            for provider, market in self._triangle_markets.items():
                self._triangle_by_book[(venue, cex_symbol(market.base, venue))].add(provider)
                self._triangle_by_book[(venue, cex_symbol(market.quote, venue))].add(provider)

    @staticmethod
    def _load_fee_audit(
        fee_audit_file: Path | None,
    ) -> tuple[dict[tuple[str, str], SpotFeeRate], str | None]:
        if fee_audit_file is None:
            return {}, None
        try:
            return load_spot_fee_audit(fee_audit_file), None
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            return {}, f"{type(exc).__name__}: {exc}"[:512]

    def _fee_rate(self, *, venue: str, symbol: str) -> SpotFeeRate:
        normalized_venue = venue.upper()
        fallback = DEFAULT_PUBLIC_SPOT_TAKER_FEES.get(normalized_venue)
        if fallback is None:
            raise ValueError(f"no public taker-fee baseline configured for {normalized_venue}")
        if normalized_venue in DEFAULT_CEX_TAKER_FEES:
            return resolve_spot_fee_rate(
                venue=normalized_venue,
                symbol=symbol,
                fallback_taker_bps=fallback,
                account_fee_rates=self._fee_rates,
            )
        return SpotFeeRate(
            venue=normalized_venue,
            symbol=symbol.upper(),
            maker_buy_bps=fallback,
            maker_sell_bps=fallback,
            taker_buy_bps=fallback,
            taker_sell_bps=fallback,
            account_verified=False,
            source="configured_public_baseline_not_account_verified",
            assumptions=("read_only_account_fee_audit_not_implemented_for_venue",),
        )

    @staticmethod
    def _quote_record(quote: ExactInputQuote) -> dict[str, Any] | None:
        if (
            quote.status != "ok"
            or quote.direction not in {"buy_base", "sell_base"}
            or quote.requested_notional_quote is None
            or quote.base_amount is None
            or quote.quote_amount is None
            or quote.average_price_quote_per_base is None
            or quote.base_amount <= 0
            or quote.quote_amount <= 0
        ):
            return None
        return {
            "round_id": quote.round_id,
            "source_epoch": quote.source_epoch,
            "provider": quote.provider,
            "chain": quote.chain,
            "pair": quote.pair,
            "direction": quote.direction,
            "requested_notional_quote": _decimal_text(quote.requested_notional_quote),
            "reference_notional_usdt": _decimal_text(quote.reference_notional_usdt),
            "base_amount": _decimal_text(quote.base_amount),
            "quote_amount": _decimal_text(quote.quote_amount),
            "average_price_quote_per_base": _decimal_text(quote.average_price_quote_per_base),
            "status": quote.status,
            "response_received_realtime_ns": quote.response_received_realtime_ns,
            "request_rtt_ms": quote.request_rtt_ms,
            "fee_bps": _decimal_text(quote.fee_bps),
        }

    def _quote_key(self, quote: ExactInputQuote) -> tuple[str, str, str] | None:
        notional = _notional_key(quote.requested_notional_quote)
        if notional is None or quote.direction not in {"buy_base", "sell_base"}:
            return None
        return quote.provider, notional, quote.direction

    def _schedule_direct(self, quote_key: tuple[str, str, str], venue: str) -> None:
        provider, notional, direction = quote_key
        self._dirty_direct.add((provider, notional, direction, venue))

    def _schedule_triangle(self, quote_key: tuple[str, str, str], venue: str) -> None:
        provider, notional, direction = quote_key
        self._dirty_triangle.add((provider, notional, direction, venue))

    def _ensure_worker(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._run_worker())

    async def handle_event(self, event: MarketEvent) -> None:
        """Accept an event without blocking the collector on a depth walk."""

        if self._closed:
            return
        self._ensure_worker()
        self._counts["events_seen"] += 1
        if event.kind == "exact_input_quote" and isinstance(event.value, ExactInputQuote):
            quote = (
                event.value
                if event.value.source_epoch == event.source_epoch
                else replace(event.value, source_epoch=event.source_epoch)
            )
            key = self._quote_key(quote)
            if key is not None:
                if quote.provider in self._direct_markets:
                    self._direct_quotes[key] = quote
                    self._quote_keys_by_direct_provider[quote.provider].add(key)
                    for venue in self._spot_cex_sources:
                        self._schedule_direct(key, venue)
                    self._counts["direct_quote_events"] += 1
                elif quote.provider in self._triangle_markets:
                    self._triangle_quotes[key] = quote
                    self._quote_keys_by_triangle_provider[quote.provider].add(key)
                    for venue in self._spot_cex_sources:
                        self._schedule_triangle(key, venue)
                    self._counts["triangle_quote_events"] += 1
                else:
                    self._counts["unmapped_exact_quote_events"] += 1
                self._wake.set()
            else:
                self._counts["malformed_exact_quote_events"] += 1
            return

        if event.kind != "order_book":
            return
        # Resolve by the exact source name: this avoids interpreting a linear
        # book (or another future CEX event type) as a spot leg.
        source = self._spot_cex_by_name.get(event.source)
        if source is None or source.config.category != "spot":
            return
        symbol = getattr(event.value, "symbol", None)
        if not isinstance(symbol, str):
            return
        venue = source.config.venue.upper()
        # Existing exact quotes only remain timing-comparable for the strict
        # skew window.  Do not re-evaluate a minutes-old HTTP quote on every
        # hot CEX book update.
        freshness_ns = int(self.max_response_skew_ms * Decimal(1_000_000))
        for provider in self._direct_by_book.get((venue, symbol.upper()), ()):
            for key in self._quote_keys_by_direct_provider.get(provider, ()):
                quote = self._direct_quotes.get(key)
                if quote is not None and abs(event.received_realtime_ns - quote.response_received_realtime_ns) <= freshness_ns:
                    self._schedule_direct(key, venue)
        for provider in self._triangle_by_book.get((venue, symbol.upper()), ()):
            for key in self._quote_keys_by_triangle_provider.get(provider, ()):
                quote = self._triangle_quotes.get(key)
                if quote is not None and abs(event.received_realtime_ns - quote.response_received_realtime_ns) <= freshness_ns:
                    self._schedule_triangle(key, venue)
        self._counts["cex_book_events"] += 1
        self._wake.set()

    async def _run_worker(self) -> None:
        try:
            while not self._closed:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.coalesce_interval_seconds)
                except TimeoutError:
                    pass
                self._wake.clear()
                try:
                    await self._drain_dirty()
                    self._close_stale_candidates()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Analysis must never take the collector down.  Preserve
                    # a compact diagnosis and wait for the next dirty batch.
                    self._counts["worker_errors"] += 1
                    self._recent_errors.append(f"worker: {type(exc).__name__}: {exc}"[:512])
                    await asyncio.sleep(self.coalesce_interval_seconds)
        except asyncio.CancelledError:
            raise

    async def _drain_dirty(self) -> None:
        # Bound one pass so a burst remains coalescible and the scanner's bus
        # can keep consuming source events.  Later passes pick up the rest.
        direct_jobs = tuple(self._dirty_direct)[:512]
        triangle_jobs = tuple(self._dirty_triangle)[:512]
        self._dirty_direct.difference_update(direct_jobs)
        self._dirty_triangle.difference_update(triangle_jobs)
        for provider, notional, direction, venue in direct_jobs:
            self._evaluate_direct((provider, notional, direction), venue)
        for provider, notional, direction, venue in triangle_jobs:
            self._evaluate_triangle((provider, notional, direction), venue)
        if self._dirty_direct or self._dirty_triangle:
            self._wake.set()
        # Let source and consumer tasks run after a sizeable calculation batch.
        await asyncio.sleep(0)

    def _direct_unavailable_cycle(
        self,
        *,
        market: CycleMarket,
        venue: str,
        quote: ExactInputQuote,
        status: str,
        detail: str | None = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "market": market.name,
            "chain": market.chain,
            "dex_provider": market.provider,
            "dex_pair": market.dex_pair,
            "cex_venue": venue,
            "cex_symbol": market_for_cex(market, venue).cex_symbol,
            "cycle_direction": (
                "buy_dex_sell_cex" if quote.direction == "buy_base" else "buy_cex_sell_dex"
            ),
            "requested_notional_quote": _decimal_text(quote.requested_notional_quote),
            "quote_symbol": market.quote_symbol,
            "status": status,
            "detail": detail,
            "timing_valid": False,
            "positive_after_minimum_network": False,
        }

    def _evaluate_direct(self, key: tuple[str, str, str], venue: str) -> None:
        provider, _, _ = key
        market = self._direct_markets.get(provider)
        quote = self._direct_quotes.get(key)
        source = self._spot_cex_sources.get(venue)
        if market is None or quote is None or source is None:
            return
        record = self._quote_record(quote)
        if record is None:
            self._observe_cycle(
                self._direct_unavailable_cycle(
                    market=market,
                    venue=venue,
                    quote=quote,
                    status="dex_quote_unavailable",
                    detail=quote.error,
                ),
                analysis_kind="direct_inventory",
                observed_realtime_ns=quote.response_received_realtime_ns,
            )
            return
        cex_market = market_for_cex(market, venue)
        book = source.latest_book(cex_market.cex_symbol)
        if book is None or book.status != "ok":
            self._counts["missing_cex_book"] += 1
            self._observe_cycle(
                self._direct_unavailable_cycle(
                    market=market,
                    venue=venue,
                    quote=quote,
                    status="cex_book_unavailable",
                    detail=book.error if book is not None else "no_current_book",
                ),
                analysis_kind="direct_inventory",
                observed_realtime_ns=quote.response_received_realtime_ns,
            )
            return
        try:
            fee = self._fee_rate(venue=venue, symbol=cex_market.cex_symbol)
            cycle = calculate_cycle(
                market=cex_market,
                dex_record=record,
                book=book,
                cex_taker_fee_bps=DEFAULT_PUBLIC_SPOT_TAKER_FEES[venue],
                cex_buy_taker_fee_bps=fee.taker_buy_bps,
                cex_sell_taker_fee_bps=fee.taker_sell_bps,
                cex_fee_source=fee.source,
                cex_fee_account_verified=fee.account_verified,
                cex_fee_assumptions=fee.assumptions,
                network_cost_floor_quote=DEFAULT_NETWORK_COST_FLOORS[market.chain],
                max_response_skew_ms=self.max_response_skew_ms,
                cex_venue=venue,
            )
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            insufficient_depth = isinstance(exc, ValueError) and str(exc).startswith("insufficient ")
            if insufficient_depth:
                self._counts["insufficient_cex_depth"] += 1
            else:
                self._counts["calculation_errors"] += 1
                self._recent_errors.append(
                    f"direct {market.name}/{venue}: {type(exc).__name__}: {exc}"[:512],
                )
            cycle = self._direct_unavailable_cycle(
                market=market,
                venue=venue,
                quote=quote,
                status="insufficient_cex_depth" if insufficient_depth else "calculation_error",
                detail=f"{type(exc).__name__}: {exc}",
            )
        self._observe_cycle(
            cycle,
            analysis_kind="direct_inventory",
            observed_realtime_ns=max(quote.response_received_realtime_ns, book.response.received_realtime_ns),
        )

    def _triangle_unavailable_cycle(
        self,
        *,
        market: TriangleMarket,
        venue: str,
        quote: ExactInputQuote,
        status: str,
        detail: str | None = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": 2,
            "market": market.name,
            "chain": market.chain,
            "dex_provider": market.provider,
            "dex_pair": market.dex_pair,
            "cex_venue": venue,
            "cycle_direction": (
                "buy_cex_quote_dex_sell_cex_base"
                if quote.direction == "buy_base"
                else "buy_cex_base_sell_dex_sell_cex_quote"
            ),
            "requested_notional_quote": _decimal_text(quote.requested_notional_quote),
            "analysis_notional_bucket_usdt": _decimal_text(quote.reference_notional_usdt),
            "quote_symbol": "USDT",
            "status": status,
            "detail": detail,
            "timing_valid": False,
            "positive_after_minimum_network": False,
        }

    def _evaluate_triangle(self, key: tuple[str, str, str], venue: str) -> None:
        provider, _, _ = key
        market = self._triangle_markets.get(provider)
        quote = self._triangle_quotes.get(key)
        source = self._spot_cex_sources.get(venue)
        if market is None or quote is None or source is None:
            return
        record = self._quote_record(quote)
        if record is None:
            self._observe_cycle(
                self._triangle_unavailable_cycle(
                    market=market,
                    venue=venue,
                    quote=quote,
                    status="dex_quote_unavailable",
                    detail=quote.error,
                ),
                analysis_kind="cex_dex_cex_triangle",
                observed_realtime_ns=quote.response_received_realtime_ns,
            )
            return
        base_symbol = cex_symbol(market.base, venue)
        quote_symbol = cex_symbol(market.quote, venue)
        base_book = source.latest_book(base_symbol)
        quote_book = source.latest_book(quote_symbol)
        if base_book is None or quote_book is None or base_book.status != "ok" or quote_book.status != "ok":
            self._counts["missing_cex_book"] += 1
            missing = base_symbol if base_book is None or base_book.status != "ok" else quote_symbol
            self._observe_cycle(
                self._triangle_unavailable_cycle(
                    market=market,
                    venue=venue,
                    quote=quote,
                    status="cex_book_unavailable",
                    detail=f"missing_or_unhealthy:{missing}",
                ),
                analysis_kind="cex_dex_cex_triangle",
                observed_realtime_ns=quote.response_received_realtime_ns,
            )
            return
        try:
            if quote.direction == "buy_base":
                buy_fee = self._fee_rate(venue=venue, symbol=quote_symbol)
                sell_fee = self._fee_rate(venue=venue, symbol=base_symbol)
            else:
                buy_fee = self._fee_rate(venue=venue, symbol=base_symbol)
                sell_fee = self._fee_rate(venue=venue, symbol=quote_symbol)
            cycle = calculate_triangle_cycle(
                market=market,
                dex_record=record,
                base_book=base_book,
                quote_book=quote_book,
                cex_taker_fee_bps=DEFAULT_PUBLIC_SPOT_TAKER_FEES[venue],
                cex_buy_taker_fee_bps=buy_fee.taker_buy_bps,
                cex_sell_taker_fee_bps=sell_fee.taker_sell_bps,
                cex_buy_fee_source=buy_fee.source,
                cex_sell_fee_source=sell_fee.source,
                cex_buy_fee_account_verified=buy_fee.account_verified,
                cex_sell_fee_account_verified=sell_fee.account_verified,
                cex_buy_fee_assumptions=buy_fee.assumptions,
                cex_sell_fee_assumptions=sell_fee.assumptions,
                network_cost_floor_usdt=DEFAULT_NETWORK_COST_FLOORS[market.chain],
                max_response_skew_ms=self.max_response_skew_ms,
                # Cross-asset quote sources use their input asset's units.
                # The function's legacy reference field is overwritten below
                # with the actual walked CEX cost in USDT.
                reference_notional_usdt=Decimal("0"),
                cex_venue=venue,
            )
            if cycle.get("gross_cost_quote") is not None:
                cycle["reference_notional_usdt"] = cycle.get("gross_cost_quote")
                cycle["reference_notional_mode"] = "walked_cex_buy_cost_usdt"
                cycle["requested_reference_notional_usdt"] = _decimal_text(
                    quote.reference_notional_usdt,
                )
                cycle["analysis_notional_bucket_usdt"] = _decimal_text(
                    quote.reference_notional_usdt,
                )
                cycle["dex_input_asset"] = quote.input_symbol
                cycle["dex_input_amount"] = _decimal_text(quote.quote_amount)
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            insufficient_depth = isinstance(exc, ValueError) and str(exc).startswith("insufficient ")
            if insufficient_depth:
                self._counts["insufficient_cex_depth"] += 1
            else:
                self._counts["calculation_errors"] += 1
                self._recent_errors.append(
                    f"triangle {market.name}/{venue}: {type(exc).__name__}: {exc}"[:512],
                )
            cycle = self._triangle_unavailable_cycle(
                market=market,
                venue=venue,
                quote=quote,
                status="insufficient_cex_depth" if insufficient_depth else "calculation_error",
                detail=f"{type(exc).__name__}: {exc}",
            )
        self._observe_cycle(
            cycle,
            analysis_kind="cex_dex_cex_triangle",
            observed_realtime_ns=max(
                quote.response_received_realtime_ns,
                base_book.response.received_realtime_ns,
                quote_book.response.received_realtime_ns,
            ),
        )

    @staticmethod
    def _cycle_key(cycle: Mapping[str, Any], analysis_kind: str) -> str:
        return "|".join(
            (
                analysis_kind,
                str(cycle.get("cex_venue")),
                str(cycle.get("market")),
                str(cycle.get("cycle_direction")),
                str(cycle.get("analysis_notional_bucket_usdt") or cycle.get("requested_notional_quote")),
            ),
        )

    def _is_modelled_candidate(self, cycle: Mapping[str, Any]) -> bool:
        try:
            return (
                cycle.get("status") == "ok"
                and cycle.get("timing_valid") is True
                and cycle.get("positive_after_minimum_network") is True
                and Decimal(str(cycle["net_edge_after_minimum_network_bps"]))
                >= self.minimum_net_edge_bps
            )
        except (InvalidOperation, KeyError, TypeError, ValueError):
            return False

    @staticmethod
    def _execution_blockers(cycle: Mapping[str, Any]) -> list[str]:
        blockers = [
            "public_data_model_only_no_orders_wallet_or_transactions",
            "inventory_rebalance_and_transfer_costs_not_included",
            "chain_inclusion_slippage_and_fill_risk_not_simulated",
        ]
        if cycle.get("cex_fee_account_verified") is not True:
            blockers.append("account_specific_cex_taker_fee_not_verified")
        return blockers

    def _candidate_event(
        self,
        event: str,
        state: _ActiveCandidate,
        *,
        observed_realtime_ns: int,
        current_cycle: Mapping[str, Any] | None = None,
        close_reason: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": 1,
            "event": event,
            "analysis_kind": state.analysis_kind,
            "candidate_key": state.key,
            "candidate_model": "timing_valid_public_or_audited_fee_after_minimum_network_floor",
            "execution_ready": False,
            "started_at": state.started_at,
            "last_seen_at": state.last_seen_at,
            "event_at": _utc_iso_from_ns(observed_realtime_ns),
            "duration_seconds": round(
                max(0, observed_realtime_ns - state.started_realtime_ns) / 1_000_000_000,
                6,
            ),
            "positive_observations": state.observations,
            "max_net_edge_after_minimum_network_bps": _decimal_text(state.max_edge_bps),
            "max_net_pnl_after_minimum_network_quote": _decimal_text(state.max_pnl_quote),
            "best_cycle": state.best_cycle,
            "execution_blockers": self._execution_blockers(state.best_cycle),
        }
        if current_cycle is not None:
            payload["current_cycle"] = dict(current_cycle)
        if close_reason is not None:
            payload["close_reason"] = close_reason
        return payload

    def _persist_candidate_event(self, event: dict[str, Any]) -> None:
        self.analysis_directory.mkdir(parents=True, exist_ok=True)
        rewrite = len(self._candidate_events) >= self.max_candidate_events
        if rewrite:
            self._candidate_events.popleft()
        self._candidate_events.append(event)
        if rewrite:
            temporary = self.candidate_events_path.with_suffix(".jsonl.tmp")
            with temporary.open("w", encoding="utf-8") as output:
                for row in self._candidate_events:
                    output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            temporary.replace(self.candidate_events_path)
        else:
            with self.candidate_events_path.open("a", encoding="utf-8") as output:
                output.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._counts["candidate_events_written"] += 1
        if event["event"] in {"candidate_started", "candidate_improved"}:
            cycle = event["best_cycle"]
            print(
                "[cycle] "
                f"{event['event']} {event['analysis_kind']} "
                f"{cycle.get('market')} {cycle.get('cex_venue')} "
                f"{cycle.get('cycle_direction')} "
                f"edge={event['max_net_edge_after_minimum_network_bps']}bps "
                "modelled; not execution-ready",
                flush=True,
            )

    def _observe_candidate(
        self,
        cycle: dict[str, Any],
        *,
        analysis_kind: str,
        observed_realtime_ns: int,
    ) -> None:
        key = self._cycle_key(cycle, analysis_kind)
        current = self._active.get(key)
        if not self._is_modelled_candidate(cycle):
            if current is not None:
                current.last_seen_realtime_ns = observed_realtime_ns
                current.last_seen_at = _utc_iso_from_ns(observed_realtime_ns)
                self._active.pop(key)
                self._candidate_closed += 1
                self._persist_candidate_event(
                    self._candidate_event(
                        "candidate_closed",
                        current,
                        observed_realtime_ns=observed_realtime_ns,
                        current_cycle=cycle,
                        close_reason="not_positive_or_timing_invalid",
                    ),
                )
            return
        edge = Decimal(str(cycle["net_edge_after_minimum_network_bps"]))
        pnl = Decimal(str(cycle["net_pnl_after_minimum_network_quote"]))
        if current is None:
            current = _ActiveCandidate(
                key=key,
                analysis_kind=analysis_kind,
                started_realtime_ns=observed_realtime_ns,
                started_at=_utc_iso_from_ns(observed_realtime_ns),
                last_seen_realtime_ns=observed_realtime_ns,
                last_seen_at=_utc_iso_from_ns(observed_realtime_ns),
                observations=1,
                max_edge_bps=edge,
                max_pnl_quote=pnl,
                best_cycle=cycle,
            )
            self._active[key] = current
            self._candidate_started += 1
            self._persist_candidate_event(
                self._candidate_event(
                    "candidate_started",
                    current,
                    observed_realtime_ns=observed_realtime_ns,
                    current_cycle=cycle,
                ),
            )
            return
        current.last_seen_realtime_ns = observed_realtime_ns
        current.last_seen_at = _utc_iso_from_ns(observed_realtime_ns)
        current.observations += 1
        if edge > current.max_edge_bps:
            current.max_edge_bps = edge
            current.max_pnl_quote = max(current.max_pnl_quote, pnl)
            current.best_cycle = cycle
            self._candidate_improved += 1
            self._persist_candidate_event(
                self._candidate_event(
                    "candidate_improved",
                    current,
                    observed_realtime_ns=observed_realtime_ns,
                    current_cycle=cycle,
                ),
            )
        else:
            current.max_pnl_quote = max(current.max_pnl_quote, pnl)

    def _observe_cycle(
        self,
        cycle: dict[str, Any],
        *,
        analysis_kind: str,
        observed_realtime_ns: int,
    ) -> None:
        self._counts[f"{analysis_kind}_evaluations"] += 1
        self._counts["cycle_evaluations"] += 1
        timing_valid = cycle.get("timing_valid") is True
        positive_after_floor = cycle.get("positive_after_minimum_network") is True
        if timing_valid:
            self._counts["timing_valid_evaluations"] += 1
        if positive_after_floor:
            self._counts["positive_after_minimum_network_evaluations"] += 1
            if timing_valid:
                self._counts["timing_valid_positive_after_minimum_network_evaluations"] += 1
            else:
                self._counts["timing_invalid_positive_after_minimum_network_evaluations"] += 1
        route_key = self._cycle_key(cycle, analysis_kind)
        route = self._route_stats.setdefault(
            route_key,
            {
                "analysis_kind": analysis_kind,
                "market": cycle.get("market"),
                "cex_venue": cycle.get("cex_venue"),
                "cycle_direction": cycle.get("cycle_direction"),
                "requested_notional_quote": cycle.get("requested_notional_quote"),
                "evaluations": 0,
                "timing_valid": 0,
                "positive_after_minimum_network": 0,
                "timing_valid_positive_after_minimum_network": 0,
                "best_timing_valid_net_edge_after_minimum_network_bps": None,
                "best_timing_valid_cycle": None,
            },
        )
        route["evaluations"] += 1
        if timing_valid:
            route["timing_valid"] += 1
        if positive_after_floor:
            route["positive_after_minimum_network"] += 1
            if timing_valid:
                route["timing_valid_positive_after_minimum_network"] += 1
        try:
            edge = Decimal(str(cycle["net_edge_after_minimum_network_bps"]))
        except (InvalidOperation, KeyError, TypeError, ValueError):
            edge = None
        if edge is not None and timing_valid:
            best = route["best_timing_valid_net_edge_after_minimum_network_bps"]
            if best is None or edge > Decimal(str(best)):
                route["best_timing_valid_net_edge_after_minimum_network_bps"] = _decimal_text(edge)
                route["best_timing_valid_cycle"] = cycle
        self._observe_candidate(
            cycle,
            analysis_kind=analysis_kind,
            observed_realtime_ns=observed_realtime_ns,
        )

    def _close_stale_candidates(self) -> None:
        now = time.time_ns()
        max_idle_ns = int(self.max_response_skew_ms * Decimal(1_000_000))
        for key, state in tuple(self._active.items()):
            if now - state.last_seen_realtime_ns <= max_idle_ns:
                continue
            self._active.pop(key)
            self._candidate_closed += 1
            self._persist_candidate_event(
                self._candidate_event(
                    "candidate_closed",
                    state,
                    observed_realtime_ns=now,
                    close_reason="no_fresh_timing_valid_evaluation",
                ),
            )

    def snapshot(self) -> Mapping[str, Any]:
        top_routes = sorted(
            (
                item
                for item in self._route_stats.values()
                if item["best_timing_valid_net_edge_after_minimum_network_bps"] is not None
            ),
            key=lambda item: Decimal(
                str(item["best_timing_valid_net_edge_after_minimum_network_bps"]),
            ),
            reverse=True,
        )[:30]
        payload: dict[str, Any] = {
            "schema_version": 1,
            "status": "closed" if self._closed else "running",
            "mode": "event_driven_unified_cycle_analysis",
            "started_at": self._started_at,
            "updated_at": datetime.now(UTC).isoformat(),
            "collector_connections_opened_by_analyzer": 0,
            "raw_market_data_persisted": False,
            "candidate_event_persistence": {
                "path": "cycle_analysis/candidate_events.jsonl",
                "format": "bounded_compact_candidate_lifecycle_v1",
                "retained": len(self._candidate_events),
                "maximum": self.max_candidate_events,
            },
            "timing": {
                "max_response_skew_ms": _decimal_text(self.max_response_skew_ms),
                "evaluation_coalesce_interval_ms": round(self.coalesce_interval_seconds * 1_000, 3),
            },
            "fee_policy": {
                "mode": "account_fee_audit_when_available_else_public_baseline",
                "public_taker_fee_bps_by_venue": {
                    venue: _decimal_text(fee) for venue, fee in sorted(DEFAULT_PUBLIC_SPOT_TAKER_FEES.items())
                },
                "fee_audit_loaded_rates": len(self._fee_rates),
                "fee_audit_load_error": self._fee_audit_error,
            },
            "counts": dict(sorted(self._counts.items())),
            "candidate_lifecycle": {
                "started": self._candidate_started,
                "improved": self._candidate_improved,
                "closed": self._candidate_closed,
                "active": len(self._active),
            },
            "route_count": len(self._route_stats),
            "top_routes": top_routes,
            "top_routes_policy": "timing_valid_only; timing-invalid edges are excluded",
            "recent_calculation_errors": list(self._recent_errors),
            "execution_note": (
                "positive rows are public-data models after a minimum network floor; "
                "they exclude rebalance, transfer, inclusion, fill and inventory risk"
            ),
        }
        if self.output_directory.exists():
            self.analysis_directory.mkdir(parents=True, exist_ok=True)
            atomic_json(self.stats_path, payload)
        return payload

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        now = time.time_ns()
        for key, state in tuple(self._active.items()):
            self._active.pop(key)
            self._candidate_closed += 1
            self._persist_candidate_event(
                self._candidate_event(
                    "candidate_closed",
                    state,
                    observed_realtime_ns=now,
                    close_reason="scanner_shutdown",
                ),
            )
        self.snapshot()
