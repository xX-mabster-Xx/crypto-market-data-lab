"""Bounded-retention CEX <-> DEX inventory-cycle monitor.

The monitor is deliberately read-only: it uses public CEX books and public DEX
quote endpoints, does not load exchange credentials or wallet keys, and never
submits an order or transaction.

Unlike the research recorder in :mod:`market_data_lab.cex_dex_cycles`, it does
not build an ever-growing raw data set.  ``recent.jsonl`` is atomically
rewritten from an in-memory window (60 seconds by default).  The only
append-only file is ``candidate_events.jsonl``: it contains the lifecycle of
timing-valid, net-positive cycles and their best observed values.  ``stats.json``
keeps aggregate counters and per-route extrema for a long-running session.

A positive observation remains an *inventory-cycle candidate*, not proof that
an atomic or transfer-based arbitrage is executable.  The configured floor does
not include priority auctions, inclusion risk, inventory carrying costs or
withdrawal/rebalance costs.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from market_data_lab.account_fee_audit import SpotFeeRate
from market_data_lab.account_fee_audit import load_spot_fee_audit
from market_data_lab.account_fee_audit import resolve_spot_fee_rate
from market_data_lab.cex_dex_cycles import CEX_BOOK_ENDPOINTS
from market_data_lab.cex_dex_cycles import DEFAULT_NETWORK_COST_FLOORS
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import CycleMarket
from market_data_lab.cex_dex_cycles import MexcPartialDepthStream
from market_data_lab.cex_dex_cycles import _best_dex_records
from market_data_lab.cex_dex_cycles import _book_batch
from market_data_lab.cex_dex_cycles import build_cycle_providers
from market_data_lab.cex_dex_cycles import calculate_cycle
from market_data_lab.cex_dex_cycles import choose_nearest_book
from market_data_lab.cex_dex_cycles import market_for_cex
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import DexQuoteProvider
from market_data_lab.dex_quotes import JsonFetcher
from market_data_lab.dex_quotes import OMNISTON_WS_ENDPOINT
from market_data_lab.dex_quotes import _decimal_text
from market_data_lab.dex_quotes import _fetch_json_sync
from market_data_lab.dex_quotes import _redact_url
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id


# These routes have been useful for an initial broad screen: liquid Solana
# routes, a TON route and two EVM reference pools.  They are intentionally
# bounded; extra long-tail markets can be passed with --markets.
ROLLING_DEFAULT_MARKETS = (
    "PUMP_SOLANA_RAYDIUM_USDT",
    "SOL_SOLANA_RAYDIUM_USDT",
    "PUMP_SOLANA_JUPITER_USDT",
    "SOL_SOLANA_JUPITER_USDT",
    "BTC_SOLANA_JUPITER_USDT",
    "GRAM_TON_OMNISTON",
    "NOT_TON_OMNISTON",
    "GRAM_TON_STONFI",
    "NOT_TON_STONFI",
    "ETH_POLYGON_UNISWAP",
    "BTC_POLYGON_UNISWAP",
)
ROLLING_DEFAULT_CEX_VENUES = ("MEXC", "BYBIT", "OKX", "BINANCE")
DEFAULT_CEX_TAKER_FEES = {
    # Public VIP0-style baselines only, never a claim about the user's
    # account-specific tier or promotion.  In particular, MEXC's documented
    # standard spot taker fee is 5 bps; its zero-fee offer is pair- and
    # eligibility-specific, so assuming zero creates false candidates.
    # The current monitor manifest records the configured values.
    "MEXC": Decimal("5"),
    "BYBIT": Decimal("10"),
    "OKX": Decimal("10"),
    "BINANCE": Decimal("10"),
}

# Starting a public MEXC depth subscription for an arbitrary, custom symbol
# can block initialization if that market does not support the documented feed.
# The default symbols below have already been observed on the public endpoint;
# all other MEXC symbols transparently use REST snapshots instead.
MEXC_WS_KNOWN_SYMBOLS = frozenset({"SOLUSDT", "PUMPUSDT", "BTCUSDT", "GRAMUSDT", "NOTUSDT"})


@dataclass(frozen=True)
class MonitorProfile:
    """One independent rate-budgeted part of the full research universe."""

    markets: tuple[str, ...]
    cex_venues: tuple[str, ...]
    interval_seconds: float
    description: str


# These profiles partition every route for which we have already collected a
# live CEX<->DEX observation.  They intentionally run in independent
# processes: putting all routes into one quote loop would make the liquid core
# wait for slow long-tail and EVM requests.  The intervals keep the aggregate
# request starts well below the documented/public research limits:
# - core Jupiter: six starts / about 20--30s, below keyless 0.5 req/s;
# - core Raydium: six starts / about 20--30s, far below 120 req/min;
# - all long-tail Raydium/Jupiter profiles: below 10 starts/min each.
# Each process owns its own public API pacer, so the profiles must stay in the
# listed disjoint groups.
MAXIMUM_COVERAGE_PROFILES: dict[str, MonitorProfile] = {
    "core": MonitorProfile(
        markets=(
            "PUMP_SOLANA_RAYDIUM_USDT",
            "PUMP_SOLANA_JUPITER_USDT",
            "SOL_SOLANA_RAYDIUM_USDT",
            "SOL_SOLANA_JUPITER_USDT",
            "BTC_SOLANA_RAYDIUM_USDT",
            "BTC_SOLANA_JUPITER_USDT",
            "GRAM_TON_OMNISTON",
            "GRAM_TON_STONFI",
            "NOT_TON_OMNISTON",
            "NOT_TON_STONFI",
        ),
        cex_venues=ROLLING_DEFAULT_CEX_VENUES,
        interval_seconds=20.0,
        description="Liquid Solana and TON routes against all configured CEXs",
    ),
    "solana-mexc-longtail": MonitorProfile(
        markets=(
            "BONK_SOLANA_RAYDIUM_USDT",
            "FARTCOIN_SOLANA_RAYDIUM_USDT",
            "HNT_SOLANA_RAYDIUM_USDT",
            "JUP_SOLANA_RAYDIUM_USDT",
            "MEW_SOLANA_RAYDIUM_USDT",
            "PNUT_SOLANA_RAYDIUM_USDT",
            "POPCAT_SOLANA_RAYDIUM_USDT",
            "PYTH_SOLANA_RAYDIUM_USDT",
            "RENDER_SOLANA_RAYDIUM_USDT",
            "TRUMP_SOLANA_RAYDIUM_USDT",
        ),
        cex_venues=("MEXC",),
        interval_seconds=120.0,
        description="Previously observed Solana long-tail USDT routes on MEXC",
    ),
    "solana-bybit-usdc": MonitorProfile(
        markets=(
            "BTC_SOLANA_RAYDIUM",
            "JUP_SOLANA_JUPITER",
            "JUP_SOLANA_RAYDIUM",
            "MEW_SOLANA_JUPITER",
            "MEW_SOLANA_RAYDIUM",
            "PUMP_SOLANA_JUPITER",
            "PUMP_SOLANA_RAYDIUM",
            "PYTH_SOLANA_JUPITER",
            "PYTH_SOLANA_RAYDIUM",
            "RENDER_SOLANA_JUPITER",
            "RENDER_SOLANA_RAYDIUM",
            "SOL_SOLANA_RAYDIUM",
            "TRUMP_SOLANA_JUPITER",
            "TRUMP_SOLANA_RAYDIUM",
        ),
        cex_venues=("BYBIT",),
        interval_seconds=180.0,
        description="Previously observed Solana USDC routes on Bybit",
    ),
    "ton-longtail": MonitorProfile(
        markets=(
            "CATI_TON_OMNISTON",
            "CATI_TON_STONFI",
            "DOGS_TON_OMNISTON",
            "DOGS_TON_STONFI",
            "HMSTR_TON_STONFI",
            "MAJOR_TON_STONFI",
        ),
        cex_venues=("BYBIT",),
        interval_seconds=120.0,
        description="Previously observed TON long-tail routes on Bybit",
    ),
    "evm-reference": MonitorProfile(
        markets=(
            "ETH_BASE_UNISWAP",
            "BTC_BASE_UNISWAP",
            "ETH_POLYGON_UNISWAP",
            "BTC_POLYGON_UNISWAP",
        ),
        cex_venues=("BYBIT",),
        interval_seconds=180.0,
        description="Base and Polygon WETH/WBTC reference routes on Bybit",
    ),
}
MAXIMUM_COVERAGE_MARKETS = tuple(
    dict.fromkeys(
        market
        for profile in MAXIMUM_COVERAGE_PROFILES.values()
        for market in profile.markets
    ),
)


def _validate_profiles() -> None:
    unknown = [name for name in MAXIMUM_COVERAGE_MARKETS if name not in MARKETS]
    if unknown:
        raise RuntimeError(f"maximum coverage profile has unknown markets: {unknown}")
    if len(MAXIMUM_COVERAGE_MARKETS) != sum(
        len(profile.markets) for profile in MAXIMUM_COVERAGE_PROFILES.values()
    ):
        raise RuntimeError("maximum coverage profiles must not overlap")


_validate_profiles()


def _utc_iso_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat()


def _atomic_jsonl(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    """Replace a bounded JSONL window atomically, without a partial reader view."""

    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    temporary.replace(path)


class RecentWindow:
    """Keep only a bounded receive-time window of diagnostic observations."""

    def __init__(self, retention_seconds: float) -> None:
        if retention_seconds <= 0:
            raise ValueError("retention_seconds must be positive")
        self.retention_ns = int(retention_seconds * 1_000_000_000)
        self._rows: deque[tuple[int, dict[str, Any]]] = deque()

    def append(self, row: dict[str, Any], *, observed_realtime_ns: int | None = None) -> None:
        observed = observed_realtime_ns if observed_realtime_ns is not None else time.time_ns()
        payload = dict(row)
        payload.setdefault("observed_realtime_ns", observed)
        payload.setdefault("observed_at", _utc_iso_from_ns(observed))
        self._rows.append((observed, payload))
        self.evict(now_realtime_ns=observed)

    def evict(self, *, now_realtime_ns: int | None = None) -> None:
        now = now_realtime_ns if now_realtime_ns is not None else time.time_ns()
        cutoff = now - self.retention_ns
        while self._rows and self._rows[0][0] < cutoff:
            self._rows.popleft()

    def rows(self) -> list[dict[str, Any]]:
        self.evict()
        return [row for _, row in self._rows]


def candidate_key(cycle: dict[str, Any]) -> str:
    return "|".join(
        (
            str(cycle.get("cex_venue")),
            str(cycle.get("market")),
            str(cycle.get("cycle_direction")),
            str(cycle.get("requested_notional_quote")),
        ),
    )


def _decimal_value(row: dict[str, Any], field: str) -> Decimal:
    value = Decimal(str(row[field]))
    if not value.is_finite():
        raise ValueError(f"{field} must be finite")
    return value


@dataclass
class ActiveCandidate:
    key: str
    started_realtime_ns: int
    started_at: str
    last_seen_realtime_ns: int
    last_seen_at: str
    observations: int
    max_net_edge_bps: Decimal
    max_net_pnl_quote: Decimal
    best_cycle: dict[str, Any]


class CandidateTracker:
    """Persist only starts, improvements and ends of positive candidate windows."""

    def __init__(self, minimum_net_edge_bps: Decimal) -> None:
        if not minimum_net_edge_bps.is_finite():
            raise ValueError("minimum_net_edge_bps must be finite")
        self.minimum_net_edge_bps = minimum_net_edge_bps
        self.active: dict[str, ActiveCandidate] = {}
        self.started = 0
        self.closed = 0
        self.improved = 0

    def _is_candidate(self, cycle: dict[str, Any]) -> bool:
        try:
            return (
                cycle.get("status") == "ok"
                and cycle.get("timing_valid") is True
                and cycle.get("positive_after_minimum_network") is True
                # Continuous monitors set this explicitly to False when they
                # have only a public fee baseline.  Older recorders omit it,
                # preserving their historical diagnostic behavior.
                and cycle.get("candidate_eligible_with_account_verified_fee") is not False
                and _decimal_value(cycle, "net_edge_after_minimum_network_bps")
                >= self.minimum_net_edge_bps
            )
        except (InvalidOperation, KeyError, TypeError, ValueError):
            return False

    def _event(
        self,
        event: str,
        state: ActiveCandidate,
        *,
        observed_realtime_ns: int,
        cycle: dict[str, Any] | None = None,
        close_reason: str | None = None,
    ) -> dict[str, Any]:
        duration_seconds = max(0, observed_realtime_ns - state.started_realtime_ns) / 1_000_000_000
        payload: dict[str, Any] = {
            "schema_version": 1,
            "event": event,
            "candidate_key": state.key,
            "started_at": state.started_at,
            "last_seen_at": state.last_seen_at,
            "event_at": _utc_iso_from_ns(observed_realtime_ns),
            "duration_seconds": round(duration_seconds, 6),
            "positive_observations": state.observations,
            "max_net_edge_after_minimum_network_bps": _decimal_text(state.max_net_edge_bps),
            "max_net_pnl_after_minimum_network_quote": _decimal_text(state.max_net_pnl_quote),
            "best_cycle": state.best_cycle,
        }
        if cycle is not None:
            payload["current_cycle"] = cycle
        if close_reason is not None:
            payload["close_reason"] = close_reason
        return payload

    def observe(
        self,
        cycle: dict[str, Any],
        *,
        observed_realtime_ns: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return zero or more lifecycle events for one comparable cycle check."""

        observed = observed_realtime_ns if observed_realtime_ns is not None else time.time_ns()
        key = candidate_key(cycle)
        existing = self.active.get(key)
        if not self._is_candidate(cycle):
            if existing is None:
                return []
            existing.last_seen_realtime_ns = observed
            existing.last_seen_at = _utc_iso_from_ns(observed)
            self.closed += 1
            self.active.pop(key)
            return [
                self._event(
                    "candidate_closed",
                    existing,
                    observed_realtime_ns=observed,
                    cycle=cycle,
                    close_reason="not_positive_or_timing_invalid",
                ),
            ]

        edge = _decimal_value(cycle, "net_edge_after_minimum_network_bps")
        pnl = _decimal_value(cycle, "net_pnl_after_minimum_network_quote")
        if existing is None:
            state = ActiveCandidate(
                key=key,
                started_realtime_ns=observed,
                started_at=_utc_iso_from_ns(observed),
                last_seen_realtime_ns=observed,
                last_seen_at=_utc_iso_from_ns(observed),
                observations=1,
                max_net_edge_bps=edge,
                max_net_pnl_quote=pnl,
                best_cycle=cycle,
            )
            self.active[key] = state
            self.started += 1
            return [
                self._event(
                    "candidate_started",
                    state,
                    observed_realtime_ns=observed,
                    cycle=cycle,
                ),
            ]

        existing.last_seen_realtime_ns = observed
        existing.last_seen_at = _utc_iso_from_ns(observed)
        existing.observations += 1
        if edge > existing.max_net_edge_bps:
            existing.max_net_edge_bps = edge
            existing.max_net_pnl_quote = max(existing.max_net_pnl_quote, pnl)
            existing.best_cycle = cycle
            self.improved += 1
            return [
                self._event(
                    "candidate_improved",
                    existing,
                    observed_realtime_ns=observed,
                    cycle=cycle,
                ),
            ]
        existing.max_net_pnl_quote = max(existing.max_net_pnl_quote, pnl)
        return []

    def close_all(self, *, observed_realtime_ns: int | None = None, reason: str) -> list[dict[str, Any]]:
        observed = observed_realtime_ns if observed_realtime_ns is not None else time.time_ns()
        events: list[dict[str, Any]] = []
        for key, state in tuple(self.active.items()):
            state.last_seen_realtime_ns = observed
            state.last_seen_at = _utc_iso_from_ns(observed)
            events.append(
                self._event(
                    "candidate_closed",
                    state,
                    observed_realtime_ns=observed,
                    close_reason=reason,
                ),
            )
            self.active.pop(key)
            self.closed += 1
        return events


class RollingStatistics:
    """Small, bounded state: counters and one best observation per route."""

    def __init__(self) -> None:
        self.rounds = 0
        self.cycle_observations = 0
        self.timing_valid_observations = 0
        self.positive_after_floor_observations = 0
        self.positive_with_account_verified_fee_observations = 0
        self.calculation_errors = 0
        self.provider_errors: Counter[str] = Counter()
        self.cex_errors: Counter[str] = Counter()
        self.statuses: Counter[str] = Counter()
        self.per_route: dict[str, dict[str, Any]] = {}
        self.last_completed_round_at: str | None = None
        self.last_round_elapsed_seconds: float | None = None

    def observe_cycle(self, cycle: dict[str, Any]) -> None:
        self.cycle_observations += 1
        status = str(cycle.get("status", "unknown"))
        self.statuses[status] += 1
        if status == "calculation_error":
            self.calculation_errors += 1
            return
        if cycle.get("timing_valid") is True:
            self.timing_valid_observations += 1
        if cycle.get("positive_after_minimum_network") is True:
            self.positive_after_floor_observations += 1
            if cycle.get("cex_fee_account_verified") is True:
                self.positive_with_account_verified_fee_observations += 1
        key = candidate_key(cycle)
        route = self.per_route.setdefault(
            key,
            {
                "market": cycle.get("market"),
                "cex_venue": cycle.get("cex_venue"),
                "direction": cycle.get("cycle_direction"),
                "notional_quote": cycle.get("requested_notional_quote"),
                "observations": 0,
                "timing_valid_observations": 0,
                "positive_after_minimum_network": 0,
                "positive_with_account_verified_fee": 0,
                "best_net_edge_after_minimum_network_bps": None,
                "best_observation": None,
            },
        )
        route["observations"] += 1
        if cycle.get("timing_valid") is True:
            route["timing_valid_observations"] += 1
        if cycle.get("positive_after_minimum_network") is True:
            route["positive_after_minimum_network"] += 1
            if cycle.get("cex_fee_account_verified") is True:
                route["positive_with_account_verified_fee"] += 1
        try:
            edge = _decimal_value(cycle, "net_edge_after_minimum_network_bps")
        except (InvalidOperation, KeyError, TypeError, ValueError):
            return
        current_best = route["best_net_edge_after_minimum_network_bps"]
        if current_best is None or edge > Decimal(str(current_best)):
            route["best_net_edge_after_minimum_network_bps"] = _decimal_text(edge)
            route["best_observation"] = cycle

    def snapshot(
        self,
        *,
        started_at: str,
        duration_wall_seconds: float,
        tracker: CandidateTracker,
        recent_observations: int,
        running: bool,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "status": "running" if running else "completed",
            "started_at": started_at,
            "updated_at": datetime.now(UTC).isoformat(),
            "duration_wall_seconds": round(duration_wall_seconds, 6),
            "rounds": self.rounds,
            "last_completed_round_at": self.last_completed_round_at,
            "last_round_elapsed_seconds": self.last_round_elapsed_seconds,
            "cycle_observations": self.cycle_observations,
            "timing_valid_observations": self.timing_valid_observations,
            "positive_after_minimum_network_observations": self.positive_after_floor_observations,
            "positive_with_account_verified_fee_observations": (
                self.positive_with_account_verified_fee_observations
            ),
            "calculation_errors": self.calculation_errors,
            "provider_errors": dict(sorted(self.provider_errors.items())),
            "cex_errors": dict(sorted(self.cex_errors.items())),
            "cycle_statuses": dict(sorted(self.statuses.items())),
            "candidate_lifecycle": {
                "started": tracker.started,
                "improved": tracker.improved,
                "closed": tracker.closed,
                "active": len(tracker.active),
            },
            "recent_window_observations": recent_observations,
            "routes": dict(sorted(self.per_route.items())),
        }


async def _fetch_books(
    market: CycleMarket,
    *,
    cex_venues: Sequence[str],
    mexc_ws_symbols: frozenset[str],
    depth_by_venue: dict[str, int],
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher,
) -> tuple[dict[str, BookSnapshot], dict[str, str]]:
    """Fetch a one-symbol REST bracket for venues not covered by MEXC WS."""

    requested: list[tuple[str, CycleMarket]] = []
    for venue in cex_venues:
        cex_market = market_for_cex(market, venue)
        if venue == "MEXC" and cex_market.cex_symbol in mexc_ws_symbols:
            continue
        requested.append((venue, cex_market))

    async def one(venue: str, cex_market: CycleMarket) -> tuple[str, BookSnapshot]:
        books = await _book_batch(
            (cex_market.cex_symbol,),
            venue=venue,
            depth=depth_by_venue[venue],
            endpoint=CEX_BOOK_ENDPOINTS[venue],
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            fetch_json=fetch_json,
        )
        return venue, books[cex_market.cex_symbol]

    results = await asyncio.gather(
        *(one(venue, cex_market) for venue, cex_market in requested),
        return_exceptions=True,
    )
    snapshots: dict[str, BookSnapshot] = {}
    errors: dict[str, str] = {}
    for requested_item, result in zip(requested, results, strict=True):
        venue = requested_item[0]
        if isinstance(result, BaseException):
            errors[venue] = f"{type(result).__name__}: {result}"
        else:
            actual_venue, snapshot = result
            snapshots[actual_venue] = snapshot
            if snapshot.status != "ok":
                errors[actual_venue] = snapshot.error or snapshot.status
    return snapshots, errors


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


def _manifest(
    *,
    status: str,
    started_at: str,
    stopped_at: str | None,
    duration_requested_seconds: float,
    duration_wall_seconds: float,
    markets: Sequence[CycleMarket],
    providers: dict[str, DexQuoteProvider],
    cex_venues: Sequence[str],
    cex_taker_fees: dict[str, Decimal],
    effective_fees: Mapping[tuple[str, str], SpotFeeRate],
    fee_audit_file: Path | None,
    network_cost_floors: dict[str, Decimal],
    max_response_skew_ms: Decimal,
    retention_seconds: float,
    mexc_ws_symbols: Sequence[str],
    network_route: dict[str, object],
    output_directory: Path,
    error: str | None = None,
    warning: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": status,
        "pid": os.getpid(),
        "started_at": started_at,
        "stopped_at": stopped_at,
        "duration_requested_seconds": duration_requested_seconds,
        "duration_wall_seconds": round(duration_wall_seconds, 6),
        "retention": {
            "raw_observation_window_seconds": retention_seconds,
            "recent_file": "recent.jsonl",
            "policy": "recent.jsonl is atomically replaced; only candidate lifecycle events append",
        },
        "markets": [asdict(market) for market in markets],
        "providers": [providers[name].config() for name in sorted(providers)],
        "cex": {
            "venues": list(cex_venues),
            "category": "spot",
            "endpoints": {venue: _redact_url(CEX_BOOK_ENDPOINTS[venue]) for venue in cex_venues},
            "book_depth": {venue: depth for venue, depth in sorted(DEFAULT_DEPTHS.items()) if venue in cex_venues},
            "taker_fee_bps": {
                venue: _decimal_text(cex_taker_fees[venue]) for venue in cex_venues
            },
            "account_fee_audit": {
                "file": str(fee_audit_file.resolve()) if fee_audit_file is not None else None,
                "account_verified_effective_rates": sum(
                    fee.account_verified for fee in effective_fees.values()
                ),
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
                    "a positive baseline-only observation is retained in the diagnostic window "
                    "but is not persisted as a candidate"
                ),
            },
            "mexc_websocket_symbols": list(mexc_ws_symbols),
        },
        "minimum_network_cost_quote_by_chain": {
            chain: _decimal_text(value) for chain, value in network_cost_floors.items()
        },
        "max_response_skew_ms": _decimal_text(max_response_skew_ms),
        "network_route": network_route,
        "api_credentials_used": any(
            provider.config().get("api_credentials_used") is True for provider in providers.values()
        ),
        "wallet_or_private_key_used": False,
        "transactions_submitted": False,
        "files": {
            "recent": str((output_directory / "recent.jsonl").resolve()),
            "candidate_events": str((output_directory / "candidate_events.jsonl").resolve()),
            "stats": str((output_directory / "stats.json").resolve()),
            "manifest": str((output_directory / "manifest.json").resolve()),
        },
        "model_scope": {
            "included": [
                "DEX exact-input quote with reported pool fee and price impact",
                "CEX spot order-book depth walk for the received base amount",
                "side-specific account-audited CEX taker fee when available and network-cost floor",
                "nearest MEXC partial-depth event for selected known USDT symbols",
            ],
            "excluded": [
                "priority fee, tip, inclusion probability and state change before execution",
                "withdrawal, deposit, bridge, wrapper redemption and rebalance costs",
                "fill probability, capital, borrow and inventory carrying costs",
            ],
            "interpretation": (
                "A positive candidate is not an order instruction or proof of executable arbitrage. "
                "Baseline-only positives are diagnostic only."
            ),
        },
        "error": error,
        "warning": warning,
    }


# Used in the manifest and as the conservative default request depth.  MEXC's
# WebSocket only supports 5/10/20, so the monitor uses 20 there and REST depth
# appropriate to each other public endpoint.
DEFAULT_DEPTHS = {"MEXC": 20, "BYBIT": 200, "OKX": 200, "BINANCE": 100}


async def record_rolling_cycle_monitor(
    markets: Sequence[CycleMarket],
    providers: dict[str, DexQuoteProvider],
    *,
    notionals: Sequence[Decimal],
    duration_seconds: float,
    interval_seconds: float,
    cex_venues: Sequence[str],
    cex_taker_fees: dict[str, Decimal],
    network_cost_floors: dict[str, Decimal],
    max_response_skew_ms: Decimal,
    retention_seconds: float,
    output_directory: Path,
    proxy_url: str | None,
    timeout_seconds: float,
    use_mexc_websocket: bool = True,
    stdout_candidates: bool = False,
    fetch_json: JsonFetcher = _fetch_json_sync,
    account_fee_rates: Mapping[tuple[str, str], SpotFeeRate] | None = None,
    fee_audit_file: Path | None = None,
) -> dict[str, Any]:
    """Run the bounded monitor and return its final manifest.

    Every provider in ``markets`` must be unique.  This is true for the market
    registry and avoids accidentally mapping one provider quote to two assets.
    """

    if output_directory.exists():
        raise FileExistsError(f"Refusing to overwrite rolling monitor output: {output_directory}")
    if duration_seconds <= 0 or interval_seconds <= 0 or timeout_seconds <= 0:
        raise ValueError("duration, interval and timeout must be positive")
    if max_response_skew_ms < 0:
        raise ValueError("max_response_skew_ms cannot be negative")
    if not markets or not notionals:
        raise ValueError("markets and notionals cannot be empty")
    if len({market.provider for market in markets}) != len(markets):
        raise ValueError("rolling monitor requires one market per DEX provider")
    if any(venue not in CEX_BOOK_ENDPOINTS for venue in cex_venues):
        raise ValueError("unsupported CEX venue")
    if any(venue not in cex_taker_fees for venue in cex_venues):
        raise ValueError("a CEX taker fee is required for every selected venue")
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
    started_monotonic_ns = time.monotonic_ns()
    recent = RecentWindow(retention_seconds)
    stats = RollingStatistics()
    candidate_minimum = Decimal("0")
    tracker = CandidateTracker(candidate_minimum)
    by_provider = {market.provider: market for market in markets}
    effective_fees: dict[tuple[str, str], SpotFeeRate] = {
        (venue, cex_market.cex_symbol): resolve_spot_fee_rate(
            venue=venue,
            symbol=cex_market.cex_symbol,
            fallback_taker_bps=cex_taker_fees[venue],
            account_fee_rates=account_fee_rates,
        )
        for market in markets
        for venue in cex_venues
        for cex_market in (market_for_cex(market, venue),)
    }

    mexc_ws_symbols = frozenset(
        market.cex_symbol
        for market in markets
        if (
            "MEXC" in cex_venues
            and market.cex_symbol in MEXC_WS_KNOWN_SYMBOLS
        )
    )
    mexc_stream: MexcPartialDepthStream | None = None
    mexc_stream_error: str | None = None
    manifest = _manifest(
        status="starting",
        started_at=started_at,
        stopped_at=None,
        duration_requested_seconds=duration_seconds,
        duration_wall_seconds=0,
        markets=markets,
        providers=providers,
        cex_venues=cex_venues,
        cex_taker_fees=cex_taker_fees,
        effective_fees=effective_fees,
        fee_audit_file=fee_audit_file,
        network_cost_floors=network_cost_floors,
        max_response_skew_ms=max_response_skew_ms,
        retention_seconds=retention_seconds,
        mexc_ws_symbols=sorted(mexc_ws_symbols),
        network_route=network_route,
        output_directory=output_directory,
    )
    atomic_json(output_directory / "manifest.json", manifest)
    _atomic_jsonl(output_directory / "recent.jsonl", [])

    if use_mexc_websocket and mexc_ws_symbols:
        try:
            mexc_stream = MexcPartialDepthStream(
                sorted(mexc_ws_symbols),
                levels=DEFAULT_DEPTHS["MEXC"],
                timeout_seconds=timeout_seconds,
                proxy_url=proxy_url,
            )
            await mexc_stream.start()
        except Exception as exc:
            mexc_stream_error = f"{type(exc).__name__}: {exc}"
            stats.cex_errors["MEXC:websocket_start"] += 1
            if mexc_stream is not None:
                with contextlib.suppress(Exception):
                    await mexc_stream.close()
            mexc_stream = None
            mexc_ws_symbols = frozenset()

    candidate_path = output_directory / "candidate_events.jsonl"
    final_status = "completed"
    final_error: str | None = None
    try:
        with candidate_path.open("x", encoding="utf-8", buffering=1) as candidate_output:
            def persist_candidate_event(event: dict[str, Any]) -> None:
                candidate_output.write(
                    json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n",
                )
                if not stdout_candidates:
                    return
                cycle = event.get("current_cycle") or event.get("best_cycle") or {}
                console_row = {
                    "kind": "positive_cycle_candidate",
                    "event": event["event"],
                    "at": event["event_at"],
                    "candidate_key": event["candidate_key"],
                    "duration_seconds": event["duration_seconds"],
                    "max_net_edge_bps": event[
                        "max_net_edge_after_minimum_network_bps"
                    ],
                    "max_net_pnl_quote": event[
                        "max_net_pnl_after_minimum_network_quote"
                    ],
                    "market": cycle.get("market"),
                    "cex_venue": cycle.get("cex_venue"),
                    "direction": cycle.get("cycle_direction"),
                    "notional_quote": cycle.get("requested_notional_quote"),
                    "timing_valid": cycle.get("timing_valid"),
                    "response_skew_ms": cycle.get("response_skew_ms"),
                }
                print(
                    json.dumps(console_row, ensure_ascii=False, separators=(",", ":")),
                    flush=True,
                )

            while True:
                round_started_ns = time.monotonic_ns()
                round_id = stats.rounds
                for provider_name in sorted(providers):
                    market = by_provider[provider_name]
                    request_pacer = getattr(providers[provider_name], "request_pacer", None)
                    if isinstance(request_pacer, AsyncRequestPacer):
                        ready_delay = request_pacer.seconds_until_ready()
                        if ready_delay > 0:
                            await asyncio.sleep(ready_delay)

                    pre_books, pre_errors = await _fetch_books(
                        market,
                        cex_venues=cex_venues,
                        mexc_ws_symbols=mexc_ws_symbols,
                        depth_by_venue=DEFAULT_DEPTHS,
                        proxy_url=proxy_url,
                        timeout_seconds=timeout_seconds,
                        fetch_json=fetch_json,
                    )
                    for venue, error in pre_errors.items():
                        stats.cex_errors[f"{venue}:{market.name}"] += 1
                        recent.append(
                            {
                                "kind": "cex_book_error",
                                "round_id": round_id,
                                "market": market.name,
                                "cex_venue": venue,
                                "phase": "before_dex",
                                "error": error,
                            },
                        )
                    try:
                        dex_records = await providers[provider_name].quote_round(round_id, notionals)
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                        stats.provider_errors[provider_name] += 1
                        recent.append(
                            {
                                "kind": "dex_provider_error",
                                "round_id": round_id,
                                "market": market.name,
                                "dex_provider": provider_name,
                                "error": error,
                            },
                        )
                        continue
                    post_books, post_errors = await _fetch_books(
                        market,
                        cex_venues=cex_venues,
                        mexc_ws_symbols=mexc_ws_symbols,
                        depth_by_venue=DEFAULT_DEPTHS,
                        proxy_url=proxy_url,
                        timeout_seconds=timeout_seconds,
                        fetch_json=fetch_json,
                    )
                    for venue, error in post_errors.items():
                        stats.cex_errors[f"{venue}:{market.name}"] += 1
                        recent.append(
                            {
                                "kind": "cex_book_error",
                                "round_id": round_id,
                                "market": market.name,
                                "cex_venue": venue,
                                "phase": "after_dex",
                                "error": error,
                            },
                        )

                    selected = _best_dex_records(dex_records)
                    for notional in notionals:
                        for dex_direction in ("buy_base", "sell_base"):
                            dex_record = selected.get((_decimal_text(notional), dex_direction))
                            if dex_record is None:
                                recent.append(
                                    {
                                        "kind": "dex_quote_unavailable",
                                        "round_id": round_id,
                                        "market": market.name,
                                        "dex_provider": provider_name,
                                        "notional_quote": _decimal_text(notional),
                                        "dex_direction": dex_direction,
                                    },
                                )
                                continue
                            dex_received_ns = int(dex_record["response_received_realtime_ns"])
                            for venue in cex_venues:
                                cex_market = market_for_cex(market, venue)
                                if (
                                    venue == "MEXC"
                                    and mexc_stream is not None
                                    and cex_market.cex_symbol in mexc_ws_symbols
                                ):
                                    book = mexc_stream.nearest_snapshot(
                                        cex_market.cex_symbol,
                                        dex_received_ns,
                                    )
                                else:
                                    book = choose_nearest_book(
                                        dex_received_ns,
                                        tuple(
                                            snapshot
                                            for snapshot in (
                                                pre_books.get(venue),
                                                post_books.get(venue),
                                            )
                                            if snapshot is not None
                                        ),
                                    )
                                if book is None:
                                    stats.cex_errors[f"{venue}:{market.name}"] += 1
                                    recent.append(
                                        {
                                            "kind": "cex_book_unavailable",
                                            "round_id": round_id,
                                            "market": market.name,
                                            "cex_venue": venue,
                                            "dex_provider": provider_name,
                                            "notional_quote": _decimal_text(notional),
                                            "dex_direction": dex_direction,
                                        },
                                    )
                                    continue
                                try:
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
                                recent.append({"kind": "cycle", "cycle": cycle})
                                if cycle.get("status") != "calculation_error":
                                    for event in tracker.observe(cycle):
                                        persist_candidate_event(event)

                stats.rounds += 1
                stats.last_completed_round_at = datetime.now(UTC).isoformat()
                stats.last_round_elapsed_seconds = round(
                    (time.monotonic_ns() - round_started_ns) / 1_000_000_000,
                    6,
                )
                elapsed = (time.monotonic_ns() - started_monotonic_ns) / 1_000_000_000
                _atomic_jsonl(output_directory / "recent.jsonl", recent.rows())
                atomic_json(
                    output_directory / "stats.json",
                    stats.snapshot(
                        started_at=started_at,
                        duration_wall_seconds=elapsed,
                        tracker=tracker,
                        recent_observations=len(recent.rows()),
                        running=True,
                    ),
                )
                if elapsed >= duration_seconds:
                    break
                await asyncio.sleep(min(max(0.0, interval_seconds - stats.last_round_elapsed_seconds), duration_seconds - elapsed))
            for event in tracker.close_all(reason="requested_duration_elapsed"):
                persist_candidate_event(event)
    except Exception as exc:
        final_status = "error"
        final_error = f"{type(exc).__name__}: {exc}"
        # The bounded recent window and all previous candidate events remain
        # inspectable even if an unexpected programming or I/O error occurs.
        _atomic_jsonl(output_directory / "recent.jsonl", recent.rows())
        with contextlib.suppress(Exception):
            atomic_json(
                output_directory / "stats.json",
                stats.snapshot(
                    started_at=started_at,
                    duration_wall_seconds=(time.monotonic_ns() - started_monotonic_ns)
                    / 1_000_000_000,
                    tracker=tracker,
                    recent_observations=len(recent.rows()),
                    running=False,
                ),
            )
    finally:
        if mexc_stream is not None:
            with contextlib.suppress(Exception):
                await mexc_stream.close()

    stopped_at = datetime.now(UTC).isoformat()
    duration_wall_seconds = (time.monotonic_ns() - started_monotonic_ns) / 1_000_000_000
    _atomic_jsonl(output_directory / "recent.jsonl", recent.rows())
    atomic_json(
        output_directory / "stats.json",
        stats.snapshot(
            started_at=started_at,
            duration_wall_seconds=duration_wall_seconds,
            tracker=tracker,
            recent_observations=len(recent.rows()),
            running=False,
        ),
    )
    manifest = _manifest(
        status=final_status,
        started_at=started_at,
        stopped_at=stopped_at,
        duration_requested_seconds=duration_seconds,
        duration_wall_seconds=duration_wall_seconds,
        markets=markets,
        providers=providers,
        cex_venues=cex_venues,
        cex_taker_fees=cex_taker_fees,
        effective_fees=effective_fees,
        fee_audit_file=fee_audit_file,
        network_cost_floors=network_cost_floors,
        max_response_skew_ms=max_response_skew_ms,
        retention_seconds=retention_seconds,
        mexc_ws_symbols=sorted(mexc_ws_symbols),
        network_route=network_route,
        output_directory=output_directory,
        error=(
            final_error
        ),
        warning=(f"MEXC websocket fallback to REST: {mexc_stream_error}" if mexc_stream_error else None),
    )
    atomic_json(output_directory / "manifest.json", manifest)
    return manifest


def _parse_names(value: str) -> list[str]:
    names = list(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    invalid = [name for name in names if name not in MARKETS]
    if not names or invalid:
        raise argparse.ArgumentTypeError(
            f"markets must be comma-separated values from {', '.join(sorted(MARKETS))}",
        )
    return names


def _parse_venues(value: str) -> list[str]:
    venues = list(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    invalid = [venue for venue in venues if venue not in CEX_BOOK_ENDPOINTS]
    if not venues or invalid:
        raise argparse.ArgumentTypeError(
            f"CEX venues must be comma-separated values from {', '.join(CEX_BOOK_ENDPOINTS)}",
        )
    return venues


def _parse_decimals(value: str, label: str) -> list[Decimal]:
    try:
        values = list(dict.fromkeys(Decimal(item.strip()) for item in value.split(",")))
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"{label} must be comma-separated decimals") from exc
    if not values or any(item <= 0 or not item.is_finite() for item in values):
        raise argparse.ArgumentTypeError(f"{label} must contain positive finite values")
    return values


def _parse_costs(value: str) -> dict[str, Decimal]:
    parsed = dict(DEFAULT_NETWORK_COST_FLOORS)
    try:
        for item in value.split(","):
            chain, amount = item.strip().split("=", 1)
            if chain not in parsed:
                raise ValueError(f"unknown chain {chain}")
            parsed[chain] = Decimal(amount)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "network costs must look like base=0.03,polygon=0.02,solana=0.01,ton=0.10",
        ) from exc
    if any(value < 0 or not value.is_finite() for value in parsed.values()):
        raise argparse.ArgumentTypeError("network costs must be finite and non-negative")
    return parsed


def _parse_fees(value: str) -> dict[str, Decimal]:
    parsed = dict(DEFAULT_CEX_TAKER_FEES)
    try:
        for item in value.split(","):
            venue, fee = item.strip().split("=", 1)
            venue = venue.upper()
            if venue not in parsed:
                raise ValueError(f"unknown CEX venue {venue}")
            parsed[venue] = Decimal(fee)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "CEX fees must look like MEXC=5,BYBIT=10,OKX=10,BINANCE=10",
        ) from exc
    if any(value < 0 or value >= 10_000 or not value.is_finite() for value in parsed.values()):
        raise argparse.ArgumentTypeError("CEX fees must be finite and in [0, 10000)")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        choices=tuple(MAXIMUM_COVERAGE_PROFILES),
        help="Rate-budgeted subset of the maximum 44-route research universe",
    )
    parser.add_argument("--markets", type=_parse_names)
    parser.add_argument("--cex-venues", type=_parse_venues)
    parser.add_argument(
        "--notionals",
        type=lambda value: _parse_decimals(value, "notionals"),
        default=[Decimal("100")],
    )
    parser.add_argument("--duration-seconds", type=float, default=10_800.0)
    parser.add_argument(
        "--interval-seconds",
        type=float,
        help="Overrides the chosen profile's rate-budgeted cadence",
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-response-skew-ms", type=Decimal, default=Decimal("750"))
    parser.add_argument("--retention-seconds", type=float, default=60.0)
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
    parser.add_argument(
        "--jupiter-api-key-env",
        default="JUPITER_API_KEY",
        help="Optional key environment variable; its value is never persisted",
    )
    parser.add_argument("--jupiter-min-request-interval-seconds", type=float)
    parser.add_argument("--stonfi-slippage-tolerance", type=Decimal, default=Decimal("0.005"))
    parser.add_argument("--omniston-ws-url", default=OMNISTON_WS_ENDPOINT)
    parser.add_argument("--omniston-quote-selection-window-seconds", type=float, default=0.5)
    parser.add_argument("--omniston-max-price-slippage-bps", type=int, default=50)
    parser.add_argument("--omniston-max-routes", type=int, default=4)
    parser.add_argument("--omniston-allow-risky-routes", action="store_true")
    parser.add_argument("--base-rpc-url", default="https://mainnet-preconf.base.org")
    parser.add_argument("--polygon-rpc-url", default="https://polygon.drpc.org")
    parser.add_argument("--no-mexc-websocket", action="store_true")
    parser.add_argument(
        "--stdout-candidates",
        action="store_true",
        help="Print one compact JSON line for each confirmed positive-candidate lifecycle event",
    )
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/rolling-cycles"))
    parser.add_argument("--run-id")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.profile is not None and (args.markets is not None or args.cex_venues is not None):
        raise SystemExit("--profile cannot be combined with --markets or --cex-venues")
    profile = MAXIMUM_COVERAGE_PROFILES.get(args.profile) if args.profile else None
    market_names = list(profile.markets) if profile is not None else (
        args.markets if args.markets is not None else list(ROLLING_DEFAULT_MARKETS)
    )
    cex_venues = list(profile.cex_venues) if profile is not None else (
        args.cex_venues if args.cex_venues is not None else list(ROLLING_DEFAULT_CEX_VENUES)
    )
    interval_seconds = (
        args.interval_seconds
        if args.interval_seconds is not None
        else (profile.interval_seconds if profile is not None else 3.0)
    )
    if args.duration_seconds <= 0 or interval_seconds <= 0 or args.timeout_seconds <= 0:
        raise SystemExit("duration, interval and timeout must be positive")
    if args.retention_seconds <= 0:
        raise SystemExit("retention must be positive")
    if args.max_response_skew_ms < 0:
        raise SystemExit("max response skew cannot be negative")
    if args.raydium_slippage_bps < 0 or args.raydium_min_request_interval_seconds < 0:
        raise SystemExit("Raydium settings cannot be negative")
    if args.jupiter_min_request_interval_seconds is not None and args.jupiter_min_request_interval_seconds < 0:
        raise SystemExit("Jupiter request interval cannot be negative")
    if not Decimal(0) <= args.stonfi_slippage_tolerance < Decimal(1):
        raise SystemExit("STON.fi slippage tolerance must be in [0, 1)")
    if args.omniston_quote_selection_window_seconds < 0 or args.omniston_max_price_slippage_bps < 0:
        raise SystemExit("Omniston timing and slippage settings cannot be negative")
    if args.omniston_max_routes <= 0:
        raise SystemExit("Omniston max routes must be positive")
    try:
        account_fee_rates = (
            load_spot_fee_audit(args.cex_fee_audit_file)
            if args.cex_fee_audit_file is not None
            else None
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    run_id = args.run_id or default_run_id("rolling-cex-dex")
    validate_run_id(run_id)
    providers = build_cycle_providers(
        market_names,
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
        record_rolling_cycle_monitor(
            [MARKETS[name] for name in market_names],
            providers,
            notionals=args.notionals,
            duration_seconds=args.duration_seconds,
            interval_seconds=interval_seconds,
            cex_venues=cex_venues,
            cex_taker_fees=args.cex_taker_fees_bps,
            network_cost_floors=args.minimum_network_costs,
            max_response_skew_ms=args.max_response_skew_ms,
            retention_seconds=args.retention_seconds,
            output_directory=args.output_root / run_id,
            proxy_url=args.proxy_url,
            timeout_seconds=args.timeout_seconds,
            use_mexc_websocket=not args.no_mexc_websocket,
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
