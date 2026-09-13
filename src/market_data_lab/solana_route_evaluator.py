"""Event-driven, read-only local Solana pool/CEX route evaluation.

This layer is intentionally narrower than a trading system.  It combines an
exact-input quote from a locally supervised pool engine with the *latest*
public CEX books, walks visible CEX depth, and keeps only bounded aggregate
statistics plus compact positive-window lifecycle events.  It never receives
a wallet, creates a transaction, accesses a private exchange endpoint, or
submits an order.

The first supported route shape is useful for a large part of the Solana
universe:

``settlement -> bridge on CEX -> base on one DEX pool -> settlement on CEX``

and its reverse.  For example, ``USDT -> SOL -> PUMP -> USDT`` uses the
PUMP/SOL pool plus the PUMP/USDT and SOL/USDT books.  A direct DEX pair with a
settlement-token bridge simply omits the second CEX leg.  This makes the
wrapper explicit instead of silently treating SOL, USDC, or USDT as one
interchangeable dollar.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any, Protocol

from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import _buy_base
from market_data_lab.cex_dex_cycles import _sell_base
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RollingStateStore


class ExactInputQuoteSource(Protocol):
    """The deliberately tiny surface used by the evaluator.

    Keeping this protocol separate from the Raydium implementation lets a
    later Meteora adapter use the same route evaluator without importing a
    transaction or swap-building SDK into Python.
    """

    async def quote_exact_input(
        self,
        *,
        request_id: str,
        pool_id: str,
        input_mint: str,
        output_mint: str,
        input_amount_raw: int,
        minimum_state_slot: int | None = None,
    ) -> dict[str, Any]: ...


class GatedQuoteVerifier(Protocol):
    """Optional low-rate, quote-only independent cross-check."""

    async def verify_exact_input(
        self,
        *,
        input_mint: str,
        output_mint: str,
        input_amount_raw: int,
    ) -> Mapping[str, object]: ...

    def snapshot(self) -> Mapping[str, object]: ...


def _decimal(value: object, *, field_name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be a finite decimal")
    return result


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _raw_to_decimal(value: object, *, decimals: int, field_name: str) -> Decimal:
    try:
        raw = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} is not an integer raw amount") from exc
    if raw < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return Decimal(raw) / (Decimal(10) ** decimals)


def _decimal_to_raw_floor(value: Decimal, *, decimals: int) -> int:
    if value <= 0 or not value.is_finite():
        return 0
    return int((value * (Decimal(10) ** decimals)).to_integral_value(rounding=ROUND_DOWN))


def _utc_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat()


def _book_key(*, venue: str, symbol: str) -> str:
    return f"cex:{venue.upper()}:spot:{symbol.upper()}"


def _pool_key(pool_id: str, protocol: str) -> str:
    return f"solana:{protocol.replace('_', '-')}:{pool_id}"


@dataclass(frozen=True)
class LocalSpotRoute:
    """One single-pool CEX/DEX inventory-cycle screen.

    ``base`` and ``bridge`` are the two on-chain mints in the configured pool.
    Both CEX symbols are denominated in the same explicit settlement asset
    (for example USDT).  This is still only an inventory-cycle model: balance
    locations, token transfer compatibility and later rebalance are recorded
    as open constraints rather than silently assumed away.
    """

    route_id: str
    pool_id: str
    base_mint: str
    bridge_mint: str
    base_decimals: int
    bridge_decimals: int
    base_symbol: str
    bridge_symbol: str
    settlement_symbol: str
    cex_venue: str
    base_cex_symbol: str
    bridge_cex_symbol: str | None
    bridge_is_settlement: bool
    notional_settlement: Decimal
    base_buy_taker_fee_bps: Decimal
    base_sell_taker_fee_bps: Decimal
    bridge_buy_taker_fee_bps: Decimal
    bridge_sell_taker_fee_bps: Decimal
    network_cost_floor_settlement: Decimal
    asset_equivalence: str
    pool_protocol: str = "raydium_clmm"
    base_fee_source: str = "configured_public_baseline_not_account_verified"
    bridge_fee_source: str = "configured_public_baseline_not_account_verified"
    base_fee_account_verified: bool = False
    bridge_fee_account_verified: bool = False

    def __post_init__(self) -> None:
        required = {
            "route_id": self.route_id,
            "pool_id": self.pool_id,
            "base_mint": self.base_mint,
            "bridge_mint": self.bridge_mint,
            "base_symbol": self.base_symbol,
            "bridge_symbol": self.bridge_symbol,
            "settlement_symbol": self.settlement_symbol,
            "cex_venue": self.cex_venue,
            "base_cex_symbol": self.base_cex_symbol,
            "asset_equivalence": self.asset_equivalence,
        }
        if any(not isinstance(value, str) or not value.strip() for value in required.values()):
            raise ValueError("route identifiers, symbols and equivalence note must be non-empty strings")
        if self.base_mint == self.bridge_mint:
            raise ValueError("base_mint and bridge_mint must differ")
        if self.pool_protocol not in {
            "raydium_clmm",
            "raydium_cpmm",
            "raydium_amm_v4",
            "meteora_dlmm",
            "orca_whirlpool",
        }:
            raise ValueError(
                "pool_protocol must be a supported local Raydium, Meteora or Orca protocol",
            )
        for field_name, decimals in (
            ("base_decimals", self.base_decimals),
            ("bridge_decimals", self.bridge_decimals),
        ):
            if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 18:
                raise ValueError(f"{field_name} must be an integer in [0, 18]")
        if self.bridge_is_settlement:
            if self.bridge_cex_symbol is not None:
                raise ValueError("bridge_cex_symbol must be omitted when bridge_is_settlement is true")
        elif not isinstance(self.bridge_cex_symbol, str) or not self.bridge_cex_symbol.strip():
            raise ValueError("bridge_cex_symbol is required unless bridge_is_settlement is true")
        if self.notional_settlement <= 0 or not self.notional_settlement.is_finite():
            raise ValueError("notional_settlement must be positive and finite")
        if self.network_cost_floor_settlement < 0 or not self.network_cost_floor_settlement.is_finite():
            raise ValueError("network_cost_floor_settlement must be finite and non-negative")
        for field_name, fee in (
            ("base_buy_taker_fee_bps", self.base_buy_taker_fee_bps),
            ("base_sell_taker_fee_bps", self.base_sell_taker_fee_bps),
            ("bridge_buy_taker_fee_bps", self.bridge_buy_taker_fee_bps),
            ("bridge_sell_taker_fee_bps", self.bridge_sell_taker_fee_bps),
        ):
            if not fee.is_finite() or fee < 0 or fee >= Decimal(10_000):
                raise ValueError(f"{field_name} must be in [0, 10000) bps")

    @property
    def pool_state_key(self) -> str:
        return _pool_key(self.pool_id, self.pool_protocol)

    @property
    def base_book_key(self) -> str:
        return _book_key(venue=self.cex_venue, symbol=self.base_cex_symbol)

    @property
    def bridge_book_key(self) -> str | None:
        if self.bridge_is_settlement:
            return None
        assert self.bridge_cex_symbol is not None
        return _book_key(venue=self.cex_venue, symbol=self.bridge_cex_symbol)

    @property
    def all_fees_account_verified(self) -> bool:
        return self.base_fee_account_verified and (
            self.bridge_is_settlement or self.bridge_fee_account_verified
        )

    def safe_descriptor(self) -> dict[str, object]:
        return {
            "route_id": self.route_id,
            "pool_id": self.pool_id,
            "pool_protocol": self.pool_protocol,
            "base": {"symbol": self.base_symbol, "mint": self.base_mint, "decimals": self.base_decimals},
            "bridge": {
                "symbol": self.bridge_symbol,
                "mint": self.bridge_mint,
                "decimals": self.bridge_decimals,
                "is_settlement": self.bridge_is_settlement,
            },
            "settlement_symbol": self.settlement_symbol,
            "cex": {
                "venue": self.cex_venue,
                "base_symbol": self.base_cex_symbol,
                "bridge_symbol": self.bridge_cex_symbol,
            },
            "notional_settlement": _decimal_text(self.notional_settlement),
            "network_cost_floor_settlement": _decimal_text(self.network_cost_floor_settlement),
            "fee_account_verified": self.all_fees_account_verified,
            "asset_equivalence": self.asset_equivalence,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }


@dataclass(frozen=True)
class LocalRouteEvaluatorConfig:
    """Bounded work and evidence limits for an evaluator instance."""

    routes: tuple[LocalSpotRoute, ...]
    minimum_quote_interval_ms: int = 100
    maximum_book_age_ms: int = 1_000
    # Pool account subscriptions are change feeds: no notification means the
    # state did not change, not that it became invalid.  The local worker also
    # takes a periodic RPC snapshot, so this limit is a missed-update watchdog
    # rather than a requirement that a pool trades every few seconds.
    maximum_pool_state_age_ms: int = 60_000
    maximum_timing_skew_ms: int = 1_000
    candidate_event_limit: int = 5_000
    minimum_candidate_edge_bps: Decimal = Decimal("0")
    candidate_improvement_bps: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if not self.routes:
            raise ValueError("at least one local spot route is required")
        route_ids = [route.route_id for route in self.routes]
        if len(set(route_ids)) != len(route_ids):
            raise ValueError("local spot route_id values must be unique")
        for name, value in (
            ("minimum_quote_interval_ms", self.minimum_quote_interval_ms),
            ("maximum_book_age_ms", self.maximum_book_age_ms),
            ("maximum_pool_state_age_ms", self.maximum_pool_state_age_ms),
            ("maximum_timing_skew_ms", self.maximum_timing_skew_ms),
            ("candidate_event_limit", self.candidate_event_limit),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not self.minimum_candidate_edge_bps.is_finite() or not self.candidate_improvement_bps.is_finite():
            raise ValueError("candidate thresholds must be finite")
        if self.candidate_improvement_bps < 0:
            raise ValueError("candidate_improvement_bps must be non-negative")

    def safe_descriptor(self) -> dict[str, object]:
        return {
            "mode": "event_driven_local_exact_pool_quote_plus_cex_depth",
            "routes": [route.safe_descriptor() for route in self.routes],
            "minimum_quote_interval_ms": self.minimum_quote_interval_ms,
            "maximum_book_age_ms": self.maximum_book_age_ms,
            "maximum_pool_state_age_ms": self.maximum_pool_state_age_ms,
            "maximum_timing_skew_ms": self.maximum_timing_skew_ms,
            "candidate_event_limit": self.candidate_event_limit,
            "raw_market_data_persisted": False,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }


@dataclass(frozen=True)
class _CexLeg:
    gross_settlement: Decimal
    net_settlement: Decimal
    selected_fee_model: str


def _conservative_buy_cost(
    book: BookSnapshot,
    *,
    desired_net_base: Decimal,
    fee_bps: Decimal,
) -> _CexLeg | None:
    """Walk asks under both common fee-asset conventions, choose the worse.

    Public fee schedules usually reveal the rate but not the exact fee-asset
    choice at a future fill.  Assuming the better convention would manufacture
    false positives.  We therefore compare a base-asset fee and a quote-asset
    fee, retain the higher settlement cost, and label that conservative model.
    """

    if desired_net_base <= 0:
        return None
    fraction = fee_bps / Decimal(10_000)
    if fraction >= 1:
        return None
    gross_without_fee = _buy_base(book.asks, desired_net_base)
    if gross_without_fee is None:
        return None
    base_fee_cost = _buy_base(book.asks, desired_net_base / (Decimal(1) - fraction))
    if base_fee_cost is None:
        return None
    quote_fee_cost = gross_without_fee * (Decimal(1) + fraction)
    if base_fee_cost >= quote_fee_cost:
        return _CexLeg(
            gross_settlement=gross_without_fee,
            net_settlement=base_fee_cost,
            selected_fee_model="fee_charged_in_base",
        )
    return _CexLeg(
        gross_settlement=gross_without_fee,
        net_settlement=quote_fee_cost,
        selected_fee_model="fee_charged_in_quote",
    )


def _conservative_sell_proceeds(
    book: BookSnapshot,
    *,
    available_base: Decimal,
    fee_bps: Decimal,
) -> _CexLeg | None:
    """Walk bids under both common fee-asset conventions, choose the worse."""

    if available_base <= 0:
        return None
    fraction = fee_bps / Decimal(10_000)
    gross_without_fee = _sell_base(book.bids, available_base)
    if gross_without_fee is None:
        return None
    quote_fee_proceeds = gross_without_fee * (Decimal(1) - fraction)
    # If a base fee is deducted from the amount already available, only this
    # fraction can be offered into the book.  It is deliberately conservative
    # when the exact exchange fee-asset selection is unknown.
    base_fee_proceeds = _sell_base(book.bids, available_base * (Decimal(1) - fraction))
    if base_fee_proceeds is None:
        return None
    if base_fee_proceeds <= quote_fee_proceeds:
        return _CexLeg(
            gross_settlement=gross_without_fee,
            net_settlement=base_fee_proceeds,
            selected_fee_model="fee_charged_in_base",
        )
    return _CexLeg(
        gross_settlement=gross_without_fee,
        net_settlement=quote_fee_proceeds,
        selected_fee_model="fee_charged_in_quote",
    )


@dataclass
class _CandidateState:
    started_realtime_ns: int
    last_seen_realtime_ns: int
    observations: int
    best_edge_bps: Decimal
    best_pnl: Decimal
    best_cycle: dict[str, Any]


def _compact_cycle(cycle: Mapping[str, Any]) -> dict[str, Any]:
    """Persist only the fields needed to judge a positive screen later."""

    fields = (
        "route_id",
        "pool_id",
        "pool_protocol",
        "direction",
        "cex_venue",
        "base_cex_symbol",
        "bridge_cex_symbol",
        "settlement_symbol",
        "requested_notional_settlement",
        "dex_input_symbol",
        "dex_input_amount",
        "dex_output_symbol",
        "dex_output_amount",
        "gross_cost_settlement",
        "gross_proceeds_settlement",
        "net_pnl_after_network_floor_settlement",
        "net_edge_after_network_floor_bps",
        "network_cost_floor_settlement",
        "dex_state_slot",
        "pool_state_age_ms",
        "cex_book_age_ms",
        "timing_skew_ms",
        "timing_skew_scope",
        "pool_to_latest_cex_observation_gap_ms",
        "quote_round_trip_ms",
        "cex_fee_account_verified",
        "fee_assumption",
        "asset_equivalence",
        "rebalance_included",
        "wrapper_basis_included",
        "cex_lot_and_minimums_verified",
    )
    compact = {field: cycle.get(field) for field in fields if field in cycle}
    verification = cycle.get("jupiter_verification")
    if isinstance(verification, Mapping):
        compact["jupiter_verification"] = {
            field: verification.get(field)
            for field in (
                "status",
                "source",
                "request_rtt_ms",
                "out_amount_raw",
                "router",
                "mode",
                "fee_bps",
                "fee_mint",
                "price_impact_pct",
                "route_labels",
                "transaction_returned_nonempty",
                "error",
            )
            if field in verification
        }
    return compact


class _CandidateLedger:
    """Bounded start/improvement/close tracking for positive *screens*.

    The ledger intentionally tracks a public-fee screen even before a user
    supplies an account-specific fee audit.  Every event retains an explicit
    ``cex_fee_account_verified`` flag, so a screen is never upgraded to an
    executable claim merely because it was persisted.
    """

    def __init__(self, *, minimum_edge_bps: Decimal, improvement_bps: Decimal) -> None:
        self.minimum_edge_bps = minimum_edge_bps
        self.improvement_bps = improvement_bps
        self.active: dict[str, _CandidateState] = {}
        self.started = 0
        self.improved = 0
        self.closed = 0

    def observe(self, cycle: Mapping[str, Any], *, observed_realtime_ns: int) -> list[dict[str, Any]]:
        route_id = str(cycle.get("route_id", ""))
        direction = str(cycle.get("direction", ""))
        key = f"{route_id}|{direction}"
        status = cycle.get("status")
        timing_valid = cycle.get("timing_valid") is True
        positive = cycle.get("positive_after_network_floor") is True
        try:
            edge = _decimal(cycle.get("net_edge_after_network_floor_bps"), field_name="candidate edge")
            pnl = _decimal(
                cycle.get("net_pnl_after_network_floor_settlement"),
                field_name="candidate pnl",
            )
        except ValueError:
            edge = Decimal("-Infinity")
            pnl = Decimal("-Infinity")
        qualifies = status == "ok" and timing_valid and positive and edge >= self.minimum_edge_bps
        state = self.active.get(key)
        if not qualifies:
            if state is None:
                return []
            self.active.pop(key)
            self.closed += 1
            state.last_seen_realtime_ns = observed_realtime_ns
            return [self._event("candidate_closed", key, state, observed_realtime_ns, cycle, "not_positive_or_not_timing_valid")]

        compact = _compact_cycle(cycle)
        if state is None:
            state = _CandidateState(
                started_realtime_ns=observed_realtime_ns,
                last_seen_realtime_ns=observed_realtime_ns,
                observations=1,
                best_edge_bps=edge,
                best_pnl=pnl,
                best_cycle=compact,
            )
            self.active[key] = state
            self.started += 1
            return [self._event("candidate_started", key, state, observed_realtime_ns, cycle, None)]

        state.last_seen_realtime_ns = observed_realtime_ns
        state.observations += 1
        if edge >= state.best_edge_bps + self.improvement_bps:
            state.best_edge_bps = edge
            state.best_pnl = max(state.best_pnl, pnl)
            state.best_cycle = compact
            self.improved += 1
            return [self._event("candidate_improved", key, state, observed_realtime_ns, cycle, None)]
        state.best_pnl = max(state.best_pnl, pnl)
        return []

    def close_all(self, *, observed_realtime_ns: int, reason: str) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for key, state in tuple(self.active.items()):
            self.active.pop(key)
            self.closed += 1
            state.last_seen_realtime_ns = observed_realtime_ns
            events.append(self._event("candidate_closed", key, state, observed_realtime_ns, None, reason))
        return events

    @staticmethod
    def _event(
        event: str,
        key: str,
        state: _CandidateState,
        observed_realtime_ns: int,
        cycle: Mapping[str, Any] | None,
        close_reason: str | None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": 1,
            "event": event,
            "candidate_key": key,
            "started_at": _utc_from_ns(state.started_realtime_ns),
            "last_seen_at": _utc_from_ns(state.last_seen_realtime_ns),
            "event_at": _utc_from_ns(observed_realtime_ns),
            "duration_seconds": round(
                max(0, observed_realtime_ns - state.started_realtime_ns) / 1_000_000_000,
                6,
            ),
            "positive_observations": state.observations,
            "max_net_edge_after_network_floor_bps": _decimal_text(state.best_edge_bps),
            "max_net_pnl_after_network_floor_settlement": _decimal_text(state.best_pnl),
            "best_cycle": state.best_cycle,
        }
        if cycle is not None:
            result["current_cycle"] = _compact_cycle(cycle)
        if close_reason is not None:
            result["close_reason"] = close_reason
        return result


@dataclass
class _RouteRuntime:
    task: asyncio.Task[None] | None = None
    rerun_requested: bool = False
    last_quote_start_monotonic_ns: int | None = None
    evaluations: int = 0
    timing_valid_evaluations: int = 0
    positive_evaluations: int = 0
    quote_unavailable: int = 0
    insufficient_cex_depth: int = 0
    stale_or_skewed: int = 0
    errors: int = 0
    jupiter_verifications: int = 0
    jupiter_verification_errors: int = 0
    last_jupiter_verified_edge_bps: dict[str, Decimal] = field(default_factory=dict)
    edge_samples_by_direction: dict[str, deque[Decimal]] = field(
        default_factory=lambda: {
            "buy_dex_base_sell_cex_base": deque(maxlen=4_096),
            "buy_cex_base_sell_dex_base": deque(maxlen=4_096),
        },
        repr=False,
    )
    quote_round_trip_ms: deque[Decimal] = field(
        default_factory=lambda: deque(maxlen=4_096),
        repr=False,
    )
    last_status: str | None = None
    last_evaluated_at: str | None = None
    best_net_edge_bps: Decimal | None = None
    best_net_pnl: Decimal | None = None


@dataclass
class SolanaRouteEvaluator:
    """Coalesced event-driven exact route evaluator.

    One task per configured route is sufficient: incoming CEX and pool events
    merely mark that route dirty, and a running task re-reads latest in-memory
    state before it asks the local quote worker.  Under a burst this discards
    obsolete work instead of creating an unbounded quote backlog.
    """

    config: LocalRouteEvaluatorConfig
    store: RollingStateStore
    quote_source: ExactInputQuoteSource
    output_directory: Path
    gated_verifier: GatedQuoteVerifier | None = None
    _runtime: dict[str, _RouteRuntime] = field(init=False, repr=False)
    _routes_by_key: dict[str, tuple[LocalSpotRoute, ...]] = field(init=False, repr=False)
    _ledger: _CandidateLedger = field(init=False, repr=False)
    _candidate_path: Path = field(init=False, repr=False)
    _candidate_events_persisted: int = field(default=0, init=False, repr=False)
    _candidate_events_dropped: int = field(default=0, init=False, repr=False)
    _persistence_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self._runtime = {route.route_id: _RouteRuntime() for route in self.config.routes}
        relevant: dict[str, list[LocalSpotRoute]] = {}
        for route in self.config.routes:
            for key in (route.pool_state_key, route.base_book_key, route.bridge_book_key):
                if key is not None:
                    relevant.setdefault(key, []).append(route)
        self._routes_by_key = {key: tuple(value) for key, value in relevant.items()}
        self._ledger = _CandidateLedger(
            minimum_edge_bps=self.config.minimum_candidate_edge_bps,
            improvement_bps=self.config.candidate_improvement_bps,
        )
        self._candidate_path = self.output_directory / "candidate_events.jsonl"

    async def handle_event(self, event: MarketEvent) -> None:
        if self._closed:
            return
        for route in self._routes_by_key.get(event.key, ()):
            runtime = self._runtime[route.route_id]
            if runtime.task is not None and not runtime.task.done():
                runtime.rerun_requested = True
                continue
            runtime.task = asyncio.create_task(self._run_route(route, runtime))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        tasks = [runtime.task for runtime in self._runtime.values() if runtime.task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._persist_events(
            self._ledger.close_all(observed_realtime_ns=time.time_ns(), reason="scanner_stopped"),
        )

    def snapshot(self) -> dict[str, Any]:
        route_state: dict[str, dict[str, Any]] = {}
        for route in self.config.routes:
            runtime = self._runtime[route.route_id]
            route_state[route.route_id] = {
                "pool_protocol": route.pool_protocol,
                "route_evaluation_rounds": runtime.evaluations,
                "timing_valid_direction_checks": runtime.timing_valid_evaluations,
                "positive_after_network_floor_direction_checks": runtime.positive_evaluations,
                "quote_unavailable": runtime.quote_unavailable,
                "insufficient_cex_depth": runtime.insufficient_cex_depth,
                "stale_or_skewed": runtime.stale_or_skewed,
                "errors": runtime.errors,
                "jupiter_verifications": runtime.jupiter_verifications,
                "jupiter_verification_errors": runtime.jupiter_verification_errors,
                "last_status": runtime.last_status,
                "last_evaluated_at": runtime.last_evaluated_at,
                "best_net_edge_after_network_floor_bps": (
                    _decimal_text(runtime.best_net_edge_bps)
                    if runtime.best_net_edge_bps is not None
                    else None
                ),
                "best_net_pnl_after_network_floor_settlement": (
                    _decimal_text(runtime.best_net_pnl)
                    if runtime.best_net_pnl is not None
                    else None
                ),
                "fee_account_verified": route.all_fees_account_verified,
                "net_edge_after_network_floor_bps": {
                    direction: self._edge_summary(samples)
                    for direction, samples in runtime.edge_samples_by_direction.items()
                },
                "quote_round_trip_ms": self._edge_summary(runtime.quote_round_trip_ms),
            }
        result: dict[str, Any] = {
            "schema_version": 1,
            "mode": "event_driven_local_exact_pool_quote_plus_cex_depth",
            "raw_market_data_persisted": False,
            "route_count": len(self.config.routes),
            "candidate_lifecycle": {
                "started": self._ledger.started,
                "improved": self._ledger.improved,
                "closed": self._ledger.closed,
                "active": len(self._ledger.active),
            },
            "candidate_event_persistence": {
                "limit": self.config.candidate_event_limit,
                "persisted": self._candidate_events_persisted,
                "dropped_after_limit": self._candidate_events_dropped,
                "format": "compact_local_exact_route_candidate_v1",
            },
            "routes": route_state,
            "limitations": [
                "public CEX books only; private account fee schedules are optional and separately audited",
                "CEX fee asset is modelled as worst of base-asset or quote-asset charging",
                "CEX lot sizes, minimums, fills, token deposit/withdraw support and balances are not verified",
                "network floor is a configured estimate; priority auction, inclusion and rebalance are excluded",
                "a positive screen is not an execution instruction or realised PnL",
            ],
        }
        if self.gated_verifier is not None:
            try:
                result["gated_jupiter_verifier"] = dict(self.gated_verifier.snapshot())
            except Exception as exc:
                result["gated_jupiter_verifier"] = {
                    "status_provider_error": f"{type(exc).__name__}: {exc}"[:256],
                }
        return result

    async def _run_route(self, route: LocalSpotRoute, runtime: _RouteRuntime) -> None:
        try:
            while not self._closed:
                runtime.rerun_requested = False
                if runtime.last_quote_start_monotonic_ns is not None:
                    elapsed_ms = (time.monotonic_ns() - runtime.last_quote_start_monotonic_ns) / 1_000_000
                    delay_ms = self.config.minimum_quote_interval_ms - elapsed_ms
                    if delay_ms > 0:
                        await asyncio.sleep(delay_ms / 1_000)
                runtime.last_quote_start_monotonic_ns = time.monotonic_ns()
                await self._evaluate_route(route, runtime)
                if not runtime.rerun_requested:
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            runtime.errors += 1
            runtime.last_status = "evaluator_error"
            runtime.last_evaluated_at = datetime.now(UTC).isoformat()
        finally:
            runtime.task = None

    async def _evaluate_route(self, route: LocalSpotRoute, runtime: _RouteRuntime) -> None:
        runtime.evaluations += 1
        runtime.last_evaluated_at = datetime.now(UTC).isoformat()
        state = self._latest_state(route)
        if state is None:
            runtime.last_status = "waiting_for_required_state"
            return
        pool_event, base_event, bridge_event, base_book, bridge_book = state
        validation = self._validate_freshness(route, pool_event, base_event, bridge_event)
        if validation is not None:
            runtime.stale_or_skewed += 1
            runtime.last_status = validation
            await self._observe_invalid(route, status=validation)
            return
        if not self._pool_matches_route(route, pool_event):
            runtime.errors += 1
            runtime.last_status = "pool_mint_or_decimal_mismatch"
            await self._observe_invalid(route, status=runtime.last_status)
            return
        if base_book.status != "ok" or (bridge_book is not None and bridge_book.status != "ok"):
            runtime.last_status = "cex_book_unavailable"
            await self._observe_invalid(route, status=runtime.last_status)
            return

        forward = await self._evaluate_direction(
            route,
            pool_event=pool_event,
            base_event=base_event,
            bridge_event=bridge_event,
            base_book=base_book,
            bridge_book=bridge_book,
            direction="buy_dex_base_sell_cex_base",
        )
        reverse = await self._evaluate_direction(
            route,
            pool_event=pool_event,
            base_event=base_event,
            bridge_event=bridge_event,
            base_book=base_book,
            bridge_book=bridge_book,
            direction="buy_cex_base_sell_dex_base",
        )
        for cycle in (forward, reverse):
            # Both quotes may await RPC. Recheck the captured books at result
            # time, not just before dispatch, before counting/persisting profit.
            validation = self._validate_freshness(route, pool_event, base_event, bridge_event)
            if validation is not None:
                runtime.stale_or_skewed += 1
                cycle.update(status=validation, timing_valid=False, positive_after_network_floor=False)
            now_monotonic = time.monotonic_ns()
            cycle["cex_book_age_ms"] = round(max(
                (now_monotonic - event.received_monotonic_ns) / 1_000_000
                for event in (base_event, bridge_event) if event is not None
            ), 3)
            cycle["pool_state_age_ms"] = round(
                (now_monotonic - pool_event.received_monotonic_ns) / 1_000_000, 3,
            )
            status = str(cycle.get("status", "unknown"))
            runtime.last_status = status
            quote_rtt = cycle.get("quote_round_trip_ms")
            if quote_rtt is not None:
                runtime.quote_round_trip_ms.append(
                    _decimal(quote_rtt, field_name="quote round-trip milliseconds"),
                )
            if status == "quote_unavailable":
                runtime.quote_unavailable += 1
            elif status == "insufficient_cex_depth":
                runtime.insufficient_cex_depth += 1
            elif status == "ok":
                runtime.timing_valid_evaluations += 1
                edge = _decimal(cycle["net_edge_after_network_floor_bps"], field_name="net edge")
                runtime.edge_samples_by_direction[cycle["direction"]].append(edge)
                if cycle.get("positive_after_network_floor") is True:
                    runtime.positive_evaluations += 1
                    pnl = _decimal(
                        cycle["net_pnl_after_network_floor_settlement"],
                        field_name="net pnl",
                    )
                    if runtime.best_net_edge_bps is None or edge > runtime.best_net_edge_bps:
                        runtime.best_net_edge_bps = edge
                    if runtime.best_net_pnl is None or pnl > runtime.best_net_pnl:
                        runtime.best_net_pnl = pnl
                    await self._maybe_verify_with_jupiter(route, runtime, cycle)
            await self._persist_events(
                self._ledger.observe(cycle, observed_realtime_ns=time.time_ns()),
            )

    def _latest_state(
        self,
        route: LocalSpotRoute,
    ) -> tuple[MarketEvent, MarketEvent, MarketEvent | None, BookSnapshot, BookSnapshot | None] | None:
        pool_event = self.store.latest(route.pool_state_key)
        base_event = self.store.latest(route.base_book_key)
        bridge_event = self.store.latest(route.bridge_book_key) if route.bridge_book_key is not None else None
        if pool_event is None or base_event is None or (route.bridge_book_key is not None and bridge_event is None):
            return None
        base_book = base_event.value
        bridge_book = bridge_event.value if bridge_event is not None else None
        if not isinstance(base_book, BookSnapshot):
            return None
        if bridge_book is not None and not isinstance(bridge_book, BookSnapshot):
            return None
        return pool_event, base_event, bridge_event, base_book, bridge_book

    def _validate_freshness(
        self,
        route: LocalSpotRoute,
        pool_event: MarketEvent,
        base_event: MarketEvent,
        bridge_event: MarketEvent | None,
    ) -> str | None:
        now = time.monotonic_ns()
        pool_age_ms = (now - pool_event.received_monotonic_ns) / 1_000_000
        if pool_age_ms > self.config.maximum_pool_state_age_ms:
            return "pool_state_stale"
        cex_events = [base_event] + ([bridge_event] if bridge_event is not None else [])
        if any((now - item.received_monotonic_ns) / 1_000_000 > self.config.maximum_book_age_ms for item in cex_events):
            return "cex_book_stale"
        # An unchanged account can legitimately have an old notification
        # timestamp.  Comparing it with a fast CEX book manufactured false
        # "skew" precisely for quiet pools.  Only independently updated CEX
        # books must be close to one another; pool validity is guarded above by
        # periodic on-chain snapshots plus its own age limit.
        cex_times = [item.received_realtime_ns for item in cex_events]
        skew_ms = (max(cex_times) - min(cex_times)) / 1_000_000
        if skew_ms > self.config.maximum_timing_skew_ms:
            return "timing_skew_exceeded"
        return None

    @staticmethod
    def _pool_matches_route(route: LocalSpotRoute, pool_event: MarketEvent) -> bool:
        summary = pool_event.summary
        token_a = summary.get("token_a_mint", summary.get("token_0_mint"))
        token_b = summary.get("token_b_mint", summary.get("token_1_mint"))
        decimals_a = summary.get("token_a_decimals", summary.get("token_0_decimals"))
        decimals_b = summary.get("token_b_decimals", summary.get("token_1_decimals"))
        observed = {
            (token_a, decimals_a),
            (token_b, decimals_b),
        }
        expected = {
            (route.base_mint, route.base_decimals),
            (route.bridge_mint, route.bridge_decimals),
        }
        return observed == expected

    async def _observe_invalid(self, route: LocalSpotRoute, *, status: str) -> None:
        for direction in ("buy_dex_base_sell_cex_base", "buy_cex_base_sell_dex_base"):
            await self._persist_events(
                self._ledger.observe(
                    {
                        "route_id": route.route_id,
                        "direction": direction,
                        "status": status,
                        "timing_valid": False,
                        "positive_after_network_floor": False,
                    },
                    observed_realtime_ns=time.time_ns(),
                ),
            )

    async def _evaluate_direction(
        self,
        route: LocalSpotRoute,
        *,
        pool_event: MarketEvent,
        base_event: MarketEvent,
        bridge_event: MarketEvent | None,
        base_book: BookSnapshot,
        bridge_book: BookSnapshot | None,
        direction: str,
    ) -> dict[str, Any]:
        cex_timestamps = [base_event.received_realtime_ns]
        if bridge_event is not None:
            cex_timestamps.append(bridge_event.received_realtime_ns)
        now_monotonic = time.monotonic_ns()
        pool_age_ms = round((now_monotonic - pool_event.received_monotonic_ns) / 1_000_000, 3)
        cex_age_ms = round(
            max((now_monotonic - item.received_monotonic_ns) / 1_000_000 for item in (base_event, bridge_event) if item is not None),
            3,
        )
        timing_skew_ms = round(
            (max(cex_timestamps) - min(cex_timestamps)) / 1_000_000,
            3,
        )
        pool_to_latest_cex_gap_ms = round(
            abs(max(cex_timestamps) - pool_event.received_realtime_ns) / 1_000_000,
            3,
        )
        common: dict[str, Any] = {
            "schema_version": 1,
            "route_id": route.route_id,
            "pool_id": route.pool_id,
            "pool_protocol": route.pool_protocol,
            "direction": direction,
            "cex_venue": route.cex_venue,
            "base_cex_symbol": route.base_cex_symbol,
            "bridge_cex_symbol": route.bridge_cex_symbol,
            "settlement_symbol": route.settlement_symbol,
            "requested_notional_settlement": _decimal_text(route.notional_settlement),
            "dex_state_slot": pool_event.chain_position,
            "pool_state_age_ms": pool_age_ms,
            "cex_book_age_ms": cex_age_ms,
            "timing_skew_ms": timing_skew_ms,
            "timing_skew_scope": "cex_books_only",
            "pool_to_latest_cex_observation_gap_ms": pool_to_latest_cex_gap_ms,
            "maximum_timing_skew_ms": self.config.maximum_timing_skew_ms,
            "timing_valid": True,
            "network_cost_floor_settlement": _decimal_text(route.network_cost_floor_settlement),
            "cex_fee_account_verified": route.all_fees_account_verified,
            "fee_assumption": "worst_of_base_or_quote_fee_asset; account fee verification flag is explicit",
            "base_fee_source": route.base_fee_source,
            "bridge_fee_source": route.bridge_fee_source if not route.bridge_is_settlement else "not_applicable",
            "asset_equivalence": route.asset_equivalence,
            "rebalance_included": False,
            "wrapper_basis_included": False,
            "cex_lot_and_minimums_verified": False,
        }

        if direction == "buy_dex_base_sell_cex_base":
            if route.bridge_is_settlement:
                dex_input = route.notional_settlement
                cost_leg: _CexLeg | None = _CexLeg(
                    gross_settlement=dex_input,
                    net_settlement=dex_input,
                    selected_fee_model="bridge_is_settlement_no_cex_leg",
                )
            else:
                assert bridge_book is not None
                if not bridge_book.asks:
                    return {**common, "status": "insufficient_cex_depth", "positive_after_network_floor": False}
                dex_input = route.notional_settlement / bridge_book.asks[0][0]
                cost_leg = _conservative_buy_cost(
                    bridge_book,
                    desired_net_base=dex_input,
                    fee_bps=route.bridge_buy_taker_fee_bps,
                )
            input_raw = _decimal_to_raw_floor(dex_input, decimals=route.bridge_decimals)
            if input_raw <= 0 or cost_leg is None:
                return {**common, "status": "insufficient_cex_depth", "positive_after_network_floor": False}
            quote = await self._quote(
                route,
                direction=direction,
                pool_event=pool_event,
                input_mint=route.bridge_mint,
                output_mint=route.base_mint,
                input_raw=input_raw,
            )
            if quote.get("status") != "ok":
                return {
                    **common,
                    "status": "quote_unavailable",
                    "positive_after_network_floor": False,
                    "quote_error": quote.get("error"),
                    "quote_round_trip_ms": quote.get("quote_round_trip_ms"),
                }
            base_output = _raw_to_decimal(
                quote.get("output_amount_raw"),
                decimals=route.base_decimals,
                field_name="Raydium base output",
            )
            proceeds_leg = _conservative_sell_proceeds(
                base_book,
                available_base=base_output,
                fee_bps=route.base_sell_taker_fee_bps,
            )
            if proceeds_leg is None:
                return {**common, "status": "insufficient_cex_depth", "positive_after_network_floor": False}
            return self._finish_cycle(
                common,
                route=route,
                quote=quote,
                dex_input_symbol=route.bridge_symbol,
                dex_input=_raw_to_decimal(input_raw, decimals=route.bridge_decimals, field_name="bridge input"),
                dex_output_symbol=route.base_symbol,
                dex_output=base_output,
                cost_leg=cost_leg,
                proceeds_leg=proceeds_leg,
            )

        if direction == "buy_cex_base_sell_dex_base":
            if not base_book.asks:
                return {**common, "status": "insufficient_cex_depth", "positive_after_network_floor": False}
            dex_input = route.notional_settlement / base_book.asks[0][0]
            cost_leg = _conservative_buy_cost(
                base_book,
                desired_net_base=dex_input,
                fee_bps=route.base_buy_taker_fee_bps,
            )
            input_raw = _decimal_to_raw_floor(dex_input, decimals=route.base_decimals)
            if input_raw <= 0 or cost_leg is None:
                return {**common, "status": "insufficient_cex_depth", "positive_after_network_floor": False}
            quote = await self._quote(
                route,
                direction=direction,
                pool_event=pool_event,
                input_mint=route.base_mint,
                output_mint=route.bridge_mint,
                input_raw=input_raw,
            )
            if quote.get("status") != "ok":
                return {
                    **common,
                    "status": "quote_unavailable",
                    "positive_after_network_floor": False,
                    "quote_error": quote.get("error"),
                    "quote_round_trip_ms": quote.get("quote_round_trip_ms"),
                }
            bridge_output = _raw_to_decimal(
                quote.get("output_amount_raw"),
                decimals=route.bridge_decimals,
                field_name="Raydium bridge output",
            )
            if route.bridge_is_settlement:
                proceeds_leg: _CexLeg | None = _CexLeg(
                    gross_settlement=bridge_output,
                    net_settlement=bridge_output,
                    selected_fee_model="bridge_is_settlement_no_cex_leg",
                )
            else:
                assert bridge_book is not None
                proceeds_leg = _conservative_sell_proceeds(
                    bridge_book,
                    available_base=bridge_output,
                    fee_bps=route.bridge_sell_taker_fee_bps,
                )
            if proceeds_leg is None:
                return {**common, "status": "insufficient_cex_depth", "positive_after_network_floor": False}
            return self._finish_cycle(
                common,
                route=route,
                quote=quote,
                dex_input_symbol=route.base_symbol,
                dex_input=_raw_to_decimal(input_raw, decimals=route.base_decimals, field_name="base input"),
                dex_output_symbol=route.bridge_symbol,
                dex_output=bridge_output,
                cost_leg=cost_leg,
                proceeds_leg=proceeds_leg,
            )
        raise ValueError(f"unsupported local route direction {direction}")

    async def _quote(
        self,
        route: LocalSpotRoute,
        *,
        direction: str,
        pool_event: MarketEvent,
        input_mint: str,
        output_mint: str,
        input_raw: int,
    ) -> dict[str, Any]:
        started = time.monotonic_ns()
        request_id = f"{route.route_id}:{direction}:{started}"
        result = await self.quote_source.quote_exact_input(
            request_id=request_id,
            pool_id=route.pool_id,
            input_mint=input_mint,
            output_mint=output_mint,
            input_amount_raw=input_raw,
            minimum_state_slot=pool_event.chain_position,
        )
        result = dict(result)
        result["quote_round_trip_ms"] = round((time.monotonic_ns() - started) / 1_000_000, 3)
        return result

    async def _maybe_verify_with_jupiter(
        self,
        route: LocalSpotRoute,
        runtime: _RouteRuntime,
        cycle: dict[str, Any],
    ) -> None:
        """Use the aggregator only after a positive local exact screen.

        The local pool quote remains the actual route calculation.  Jupiter is
        just a gated independent observation: it may choose a different route
        and must never turn a screen into a transaction request.
        """

        verifier = self.gated_verifier
        if verifier is None:
            return
        try:
            edge = _decimal(cycle["net_edge_after_network_floor_bps"], field_name="net edge")
            direction = str(cycle["direction"])
            prior = runtime.last_jupiter_verified_edge_bps.get(direction)
            if prior is not None and edge < prior + self.config.candidate_improvement_bps:
                return
            if direction == "buy_dex_base_sell_cex_base":
                input_mint, output_mint, decimals = route.bridge_mint, route.base_mint, route.bridge_decimals
            else:
                input_mint, output_mint, decimals = route.base_mint, route.bridge_mint, route.base_decimals
            input_amount = _decimal(cycle["dex_input_amount"], field_name="DEX input amount")
            input_raw = _decimal_to_raw_floor(input_amount, decimals=decimals)
            if input_raw <= 0:
                return
            verification = dict(
                await verifier.verify_exact_input(
                    input_mint=input_mint,
                    output_mint=output_mint,
                    input_amount_raw=input_raw,
                ),
            )
            cycle["jupiter_verification"] = verification
            runtime.jupiter_verifications += 1
            if verification.get("status") == "ok":
                runtime.last_jupiter_verified_edge_bps[direction] = edge
            else:
                runtime.jupiter_verification_errors += 1
        except (TypeError, ValueError, KeyError):
            runtime.jupiter_verification_errors += 1

    @staticmethod
    def _edge_summary(samples: Sequence[Decimal]) -> dict[str, object]:
        """Small aggregate only; individual market observations stay in RAM."""

        if not samples:
            return {"samples": 0, "min": None, "p50": None, "p95": None, "max": None}
        ordered = sorted(samples)

        def percentile(fraction: float) -> Decimal:
            index = round((len(ordered) - 1) * fraction)
            return ordered[index]

        return {
            "samples": len(ordered),
            "min": _decimal_text(ordered[0]),
            "p50": _decimal_text(percentile(0.50)),
            "p95": _decimal_text(percentile(0.95)),
            "max": _decimal_text(ordered[-1]),
        }

    @staticmethod
    def _finish_cycle(
        common: Mapping[str, Any],
        *,
        route: LocalSpotRoute,
        quote: Mapping[str, Any],
        dex_input_symbol: str,
        dex_input: Decimal,
        dex_output_symbol: str,
        dex_output: Decimal,
        cost_leg: _CexLeg,
        proceeds_leg: _CexLeg,
    ) -> dict[str, Any]:
        gross_pnl = proceeds_leg.gross_settlement - cost_leg.gross_settlement
        net_before_floor = proceeds_leg.net_settlement - cost_leg.net_settlement
        net_after_floor = net_before_floor - route.network_cost_floor_settlement
        edge = net_after_floor / cost_leg.net_settlement * Decimal(10_000)
        return {
            **common,
            "status": "ok",
            "dex_input_symbol": dex_input_symbol,
            "dex_input_amount": _decimal_text(dex_input),
            "dex_output_symbol": dex_output_symbol,
            "dex_output_amount": _decimal_text(dex_output),
            "pool_fee_raw": quote.get("pool_fee_raw"),
            "price_impact_pct": quote.get("price_impact_pct"),
            "all_trade": quote.get("all_trade"),
            "tick_cache_age_ms": quote.get("tick_cache_age_ms"),
            "chain_time_age_ms": quote.get("chain_time_age_ms"),
            "quote_round_trip_ms": quote.get("quote_round_trip_ms"),
            "gross_cost_settlement": _decimal_text(cost_leg.gross_settlement),
            "gross_proceeds_settlement": _decimal_text(proceeds_leg.gross_settlement),
            "gross_pnl_settlement": _decimal_text(gross_pnl),
            "net_pnl_before_network_floor_settlement": _decimal_text(net_before_floor),
            "net_pnl_after_network_floor_settlement": _decimal_text(net_after_floor),
            "net_edge_after_network_floor_bps": _decimal_text(edge),
            "positive_after_network_floor": net_after_floor > 0,
            "cex_buy_fee_model": cost_leg.selected_fee_model,
            "cex_sell_fee_model": proceeds_leg.selected_fee_model,
        }

    async def _persist_events(self, events: Sequence[Mapping[str, Any]]) -> None:
        if not events:
            return
        async with self._persistence_lock:
            remaining = self.config.candidate_event_limit - self._candidate_events_persisted
            accepted = list(events[:max(0, remaining)])
            self._candidate_events_dropped += len(events) - len(accepted)
            if not accepted:
                return
            # Only compact candidate lifecycles reach this append-only file.
            # No order-book levels, raw RPC payloads or endpoint credentials are
            # present in these records.
            with self._candidate_path.open("a", encoding="utf-8") as output:
                for event in accepted:
                    output.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            self._candidate_events_persisted += len(accepted)
