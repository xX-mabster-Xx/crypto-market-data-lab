"""Broker-to-worker bridge for post-trade AMM simulation.

The worker owns the immutable snapshot registry and the pure protocol
adapters; this module translates typed domain requests into the narrow
JSON-lines protocol and back.  A worker-fed snapshot can also be decoded into
the typed :class:`AmmSnapshot` and executed locally by the pure
``PathSimulator`` so the analyzer's sequential DEX/perp model never depends on
a second implementation of the curve math.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from .contracts import (
    AmmPathRequest,
    AmmSimulationLimits,
    AmmSnapshot,
    PoolRef,
    SequentialUnwindResult,
    SwapLeg,
)
from .engine import PathSimulator
from .replay import SimulationEvidenceBundle, _decode_snapshot, build_evidence_bundle


class WorkerSimulation(Protocol):
    async def capture_snapshot(
        self,
        *,
        request_id: str,
        pool_ids: frozenset[str],
        required_consistency: str = "validated_multi_account_snapshot",
        timeout_seconds: float = 10.0,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class WorkerBackend:
    """Adapter translating ``LocalAmmBackend`` calls into worker protocol."""

    worker: Any

    async def capture_snapshot(
        self,
        *,
        request_id: str,
        pool_ids: frozenset[str],
        required_consistency: str = "validated_multi_account_snapshot",
        timeout_seconds: float = 10.0,
    ) -> dict[str, Any]:
        return await self.worker.capture_snapshot(
            request_id=request_id,
            pool_ids=tuple(pool_ids),
            required_consistency=required_consistency,
            timeout_seconds=timeout_seconds,
        )

    async def simulate_path(
        self,
        *,
        request_id: str,
        snapshot_token: str,
        legs: tuple[dict[str, Any], ...],
        initial_balances: tuple[dict[str, Any], ...] = (),
        timeout_seconds: float = 10.0,
    ) -> dict[str, Any]:
        return await self.worker.simulate_path(
            request_id=request_id,
            snapshot_token=snapshot_token,
            legs=legs,
            initial_balances=initial_balances,
            timeout_seconds=timeout_seconds,
        )


@dataclass(frozen=True, slots=True)
class SequentialExecution:
    """Typed sequential buy-then-sell projection produced by a worker snapshot."""

    snapshot: AmmSnapshot
    pool_ref: PoolRef
    buy_input_raw: int
    sell_net_base_raw: int
    sell_stable_raw: int
    stable_asset_id: str
    base_asset_id: str
    stable_decimals: int
    initial_stable: Decimal
    final_stable: Decimal
    simulation_complete: bool
    reason: str | None = None
    evidence_hash: str | None = None
    evidence_bundle: SimulationEvidenceBundle | None = None

    def as_result(
        self,
        *,
        evidence_hash: str | None = None,
        scenario_kind: str = "frozen_market",
        buy_leg_id: str = "leg-buy",
        sell_leg_id: str = "leg-sell",
    ) -> SequentialUnwindResult:
        return SequentialUnwindResult(
            snapshot_id=self.snapshot.snapshot_id,
            snapshot_hash=self.snapshot.snapshot_hash,
            worker_generation=self.snapshot.worker_generation,
            source_epoch=self.snapshot.source_epoch,
            boot_id=self.snapshot.boot_id,
            context_slot=self.snapshot.context_slot,
            evidence_hash=(evidence_hash if evidence_hash is not None else self.evidence_hash),
            scenario_kind=scenario_kind,
            provider=self.pool_ref.protocol,
            pool_id=self.pool_ref.pool_id,
            base_asset_id=self.base_asset_id,
            stable_asset_id=self.stable_asset_id,
            buy_leg_id=buy_leg_id,
            sell_leg_id=sell_leg_id,
            buy_input_raw=self.buy_input_raw,
            buy_output_raw=self.sell_net_base_raw,
            sell_input_raw=self.sell_net_base_raw,
            sell_output_raw=self.sell_stable_raw,
           initial_stable=self.initial_stable,
           final_stable=self.final_stable,
            execution_ready=False,
       )


def run_sequential_path(
    snapshot: AmmSnapshot,
    *,
    pool_ref: PoolRef,
    stable_asset_id: str,
    base_asset_id: str,
    stable_decimals: int,
    buy_stable_raw: int,
    monotonic_ns: Any | None = None,
) -> SequentialExecution:
    """Execute a buy-base then sell-net-base path on the post-state of a snapshot.

    This is a pure local projection over an immutable snapshot.  It never
    touches a wallet, order, or transaction.  ``stable_decimals`` scales the raw
    stable balances into the human Decimal quote amounts the analyzer combines
    with the perp BBO.
    """

    if stable_decimals < 0:
        raise ValueError("stable_decimals must be non-negative")
    if buy_stable_raw <= 0:
        raise ValueError("buy_stable_raw must be positive")
    # A live path must use the host monotonic clock.  The explicit clock seam
    # remains available for deterministic unit tests and offline replay.
    clock = monotonic_ns if callable(monotonic_ns) else time.monotonic_ns
    if clock() >= snapshot.state_valid_until_monotonic_ns:
        return SequentialExecution(
            snapshot=snapshot,
            pool_ref=pool_ref,
            buy_input_raw=buy_stable_raw,
            sell_net_base_raw=0,
            sell_stable_raw=0,
            stable_asset_id=stable_asset_id,
            base_asset_id=base_asset_id,
            stable_decimals=stable_decimals,
            initial_stable=Decimal(buy_stable_raw).scaleb(-stable_decimals),
            final_stable=Decimal(0),
            simulation_complete=False,
            reason="snapshot expired",
        )
    zero_for_one = pool_ref.asset_0_id == stable_asset_id
    base_asset = pool_ref.asset_1_id if zero_for_one else pool_ref.asset_0_id
    if base_asset != base_asset_id:
        raise ValueError("pool asset order does not match the requested base asset")

    simulator = PathSimulator(monotonic_ns=clock)
    initial = ((stable_asset_id, buy_stable_raw),)
    request = AmmPathRequest(
        schema_version=1,
        request_id=f"seq-{snapshot.snapshot_id}",
        reason="shadow_sequential_unwind",
        priority="candidate",
        snapshot=snapshot,
        legs=(
            SwapLeg(
                leg_id="leg-buy",
                pool_ref=pool_ref,
                input_asset_id=stable_asset_id,
                output_asset_id=base_asset_id,
                mode="exact_in",
                amount_source="literal",
                amount_raw=buy_stable_raw,
                previous_leg_id=None,
            ),
            SwapLeg(
                leg_id="leg-sell",
                pool_ref=pool_ref,
                input_asset_id=base_asset_id,
                output_asset_id=stable_asset_id,
                mode="exact_in",
                amount_source="previous_output",
                previous_leg_id="leg-buy",
                amount_raw=None,
            ),
        ),
        initial_balances=initial,
        scenario_id="worker-sequential-buy-sell",
        scenario_kind="frozen_market",
        required_consistency=snapshot.chain_consistency,
        limits=AmmSimulationLimits(max_path_legs=4),
        deadline_monotonic_ns=snapshot.state_valid_until_monotonic_ns,
    )
    result = simulator.simulate(request)
    buy_result = result.leg_results[0] if result.leg_results else None
    sell_result = result.leg_results[1] if len(result.leg_results) > 1 else None
    if not result.complete or buy_result is None or sell_result is None:
        return SequentialExecution(
            snapshot=snapshot,
            pool_ref=pool_ref,
            buy_input_raw=buy_stable_raw,
            sell_net_base_raw=buy_result.actual_net_output_raw if buy_result else 0,
            sell_stable_raw=sell_result.actual_net_output_raw if sell_result else 0,
            stable_asset_id=stable_asset_id,
            base_asset_id=base_asset_id,
            stable_decimals=stable_decimals,
            initial_stable=Decimal(buy_stable_raw).scaleb(-stable_decimals),
            final_stable=Decimal(0),
            simulation_complete=False,
            reason=result.reason,
        )
    final_stable_raw = dict(result.final_balances).get(stable_asset_id, 0)
    # A successful sequential result must carry a stable content hash.  Build
    # it from the exact request, immutable snapshot, and deterministic result
    # before exposing the economic projection to the analyzer.
    try:
        evidence_bundle = build_evidence_bundle(request, result)
        evidence_hash = evidence_bundle.evidence_hash
    except Exception as exc:  # noqa: BLE001
        return SequentialExecution(
            snapshot=snapshot,
            pool_ref=pool_ref,
            buy_input_raw=buy_result.actual_gross_input_raw,
            sell_net_base_raw=buy_result.actual_net_output_raw,
            sell_stable_raw=sell_result.actual_net_output_raw,
            stable_asset_id=stable_asset_id,
            base_asset_id=base_asset_id,
            stable_decimals=stable_decimals,
            initial_stable=Decimal(buy_stable_raw).scaleb(-stable_decimals),
            final_stable=Decimal(final_stable_raw).scaleb(-stable_decimals),
            simulation_complete=False,
            reason=f"evidence construction failed: {type(exc).__name__}",
        )
    return SequentialExecution(
        snapshot=snapshot,
        pool_ref=pool_ref,
        buy_input_raw=buy_result.actual_gross_input_raw,
        sell_net_base_raw=buy_result.actual_net_output_raw,
        sell_stable_raw=sell_result.actual_net_output_raw,
        stable_asset_id=stable_asset_id,
        base_asset_id=base_asset_id,
        stable_decimals=stable_decimals,
        initial_stable=Decimal(buy_stable_raw).scaleb(-stable_decimals),
        final_stable=Decimal(final_stable_raw).scaleb(-stable_decimals),
        simulation_complete=True,
        evidence_hash=evidence_hash,
        evidence_bundle=evidence_bundle,
    )


__all__ = ["WorkerBackend", "WorkerSimulation", "SequentialExecution", "run_sequential_path"]


def decode_worker_snapshot(payload: dict[str, Any]) -> AmmSnapshot:
    """Decode a worker ``snapshot`` bundle into the typed immutable snapshot."""
    return _decode_snapshot(payload)


class WorkerSequentialSimulator:
    """Composition-ready shadow simulator fed by one immutable worker snapshot.

    The snapshot is captured asynchronously by the owning scanner source and
    stored here as an immutable :class:`AmmSnapshot`.  ``simulate_buy_sell`` is
    synchronous (it is called from the analyzer's coalesced worker) and only
    reads the cached snapshot; it never opens a connection.  A complete local
    snapshot therefore costs 0 remote calls per path evaluation.
    """

    __slots__ = (
        "_snapshot", "_pool_ref", "_stable_asset_id", "_base_asset_id",
        "_stable_decimals", "_calls", "_snapshot_received_monotonic_ns",
        "_snapshot_received_realtime_ns", "_perp_symbol", "_last_evidence",
    )

    def __init__(
        self,
        *,
        snapshot: AmmSnapshot,
        pool_ref: PoolRef,
        stable_asset_id: str,
        base_asset_id: str,
        stable_decimals: int,
        snapshot_received_monotonic_ns: int | None = None,
        snapshot_received_realtime_ns: int | None = None,
        perp_symbol: str | None = None,
    ) -> None:
        self._snapshot = snapshot
        self._pool_ref = pool_ref
        self._stable_asset_id = stable_asset_id
        self._base_asset_id = base_asset_id
        self._stable_decimals = stable_decimals
        self._perp_symbol = perp_symbol.upper() if isinstance(perp_symbol, str) else None
        self._calls = 0
        self._snapshot_received_monotonic_ns = (
            snapshot_received_monotonic_ns
            if isinstance(snapshot_received_monotonic_ns, int)
            else time.monotonic_ns()
        )
        self._snapshot_received_realtime_ns = (
            snapshot_received_realtime_ns
            if isinstance(snapshot_received_realtime_ns, int)
            else time.time_ns()
        )
        self._last_evidence: SimulationEvidenceBundle | None = None

    @property
    def calls(self) -> int:
        return self._calls

    def simulate_buy_sell(
        self,
        quote: Any,
        *,
        buy_stable_raw: int | None = None,
    ) -> SequentialUnwindResult | None:
        amount = buy_stable_raw
        if amount is None:
            amount = getattr(quote, "input_amount_raw", None)
        if not isinstance(amount, int) or amount <= 0:
            return None
        execution = run_sequential_path(
            self._snapshot,
            pool_ref=self._pool_ref,
            stable_asset_id=self._stable_asset_id,
            base_asset_id=self._base_asset_id,
            stable_decimals=self._stable_decimals,
            buy_stable_raw=amount,
        )
        self._calls += 1
        if not execution.simulation_complete:
            return None
        self._last_evidence = execution.evidence_bundle
        return execution.as_result()

    def evidence_bundle(self) -> SimulationEvidenceBundle | None:
        return self._last_evidence

    def describe(self) -> dict[str, object]:
        return {
            "mode": "sequential_direct_path",
            "initialized": True,
            "provider": self._pool_ref.protocol,
            "pool_id": self._pool_ref.pool_id,
            "stable_asset_id": self._stable_asset_id,
            "base_asset_id": self._base_asset_id,
            "perp_symbol": self._perp_symbol,
            "snapshot_id": self._snapshot.snapshot_id,
            "snapshot_hash": self._snapshot.snapshot_hash,
            "worker_generation": self._snapshot.worker_generation,
            "source_epoch": self._snapshot.source_epoch,
            "boot_id": self._snapshot.boot_id,
            "context_slot": self._snapshot.context_slot,
            "state_valid_until_monotonic_ns": self._snapshot.state_valid_until_monotonic_ns,
            "snapshot_received_monotonic_ns": self._snapshot_received_monotonic_ns,
            "snapshot_received_realtime_ns": self._snapshot_received_realtime_ns,
            "snapshots_consumed": self._calls,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }


class LazySnapshotSequentialSimulator:
    """Sequential simulator that captures its snapshot from the worker once.

    The source owns the worker lifecycle; this mapper is called by the
    composition root after the worker is started.  It captures an immutable
    Raydium CPMM snapshot for the first configured pool and indexes the result
    for the analyzer's ``simulate_buy_sell`` seam.  If capture or decode fails,
    it reports the failure instead of silently disabling the feature.
    """

    __slots__ = (
        "_source", "_pool_id", "_stable_asset_id", "_base_asset_id",
        "_stable_decimals", "_perp_symbol", "_simulator", "_error", "_initialized",
        "_initializing", "_initialize_timeout_seconds", "_retry_cooldown_seconds",
        "_next_retry_monotonic_ns", "_attempts", "_successes", "_failures",
    )

    def __init__(
        self,
        *,
        source: Any,
        pool_id: str,
        stable_asset_id: str,
        base_asset_id: str,
        stable_decimals: int,
        perp_symbol: str | None = None,
        initialize_timeout_seconds: float = 10.0,
        retry_cooldown_seconds: float = 1.0,
    ) -> None:
        self._source = source
        self._pool_id = pool_id
        self._stable_asset_id = stable_asset_id
        self._base_asset_id = base_asset_id
        self._stable_decimals = stable_decimals
        self._perp_symbol = perp_symbol.upper() if isinstance(perp_symbol, str) else None
        self._simulator: WorkerSequentialSimulator | None = None
        self._error: str | None = None
        self._initialized = False
        self._initializing = False
        if initialize_timeout_seconds <= 0:
            raise ValueError("initialize_timeout_seconds must be positive")
        if retry_cooldown_seconds <= 0:
            raise ValueError("retry_cooldown_seconds must be positive")
        self._initialize_timeout_seconds = initialize_timeout_seconds
        self._retry_cooldown_seconds = retry_cooldown_seconds
        self._next_retry_monotonic_ns = 0
        self._attempts = 0
        self._successes = 0
        self._failures = 0

    def needs_refresh(self, *, now_monotonic_ns: int | None = None) -> bool:
        now = time.monotonic_ns() if now_monotonic_ns is None else now_monotonic_ns
        if self._initializing:
            return False
        if now < self._next_retry_monotonic_ns:
            return False
        if self._simulator is None:
            return True
        expiry = self._simulator.describe().get("state_valid_until_monotonic_ns")
        return not isinstance(expiry, int) or now >= expiry

    async def initialize(self) -> None:
        if self._initializing:
            return
        if not self.needs_refresh():
            self._initialized = True
            return
        self._initializing = True
        self._attempts += 1
        attempt_started = time.monotonic_ns()

        def failed(message: str) -> None:
            self._failures += 1
            self._error = message[:512]
            self._next_retry_monotonic_ns = (
                attempt_started + int(self._retry_cooldown_seconds * 1_000_000_000)
            )
            self._initialized = True
            self._initializing = False

        capture = getattr(self._source, "capture_snapshot", None)
        if not callable(capture):
            failed("local AMM source does not expose capture_snapshot")
            return
        try:
            result = await asyncio.wait_for(
                capture(
                    request_id=f"comp-seq-{self._pool_id}",
                    pool_ids=(self._pool_id,),
                    required_consistency="validated_multi_account_snapshot",
                ),
                timeout=self._initialize_timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            failed(f"snapshot capture failed: {type(exc).__name__}: {exc}")
            return
        if not isinstance(result, dict) or result.get("status") != "ok":
            reason = result.get("reason") if isinstance(result, dict) else None
            failed(f"snapshot capture failed: {reason or 'unknown'}")
            return
        bundle = result.get("snapshot")
        if not isinstance(bundle, dict):
            failed("snapshot capture returned no bundle")
            return
        try:
            snapshot = _decode_snapshot(bundle)
        except Exception as exc:  # noqa: BLE001
            failed(f"snapshot decode failed: {type(exc).__name__}: {exc}")
            return
        matches = [
            pool for pool in snapshot.pools
            if pool.pool_ref.protocol == "raydium_cpmm"
            and (pool.pool_ref.pool_id == self._pool_id or pool.pool_ref.pool_address == self._pool_id)
            and pool.pool_ref.asset_0_id in {self._stable_asset_id, self._base_asset_id}
            and pool.pool_ref.asset_1_id in {self._stable_asset_id, self._base_asset_id}
            and pool.pool_ref.asset_0_id != pool.pool_ref.asset_1_id
        ]
        if len(matches) != 1:
            failed("snapshot has no unique allowlisted CPMM pool with configured assets")
            return
        self._simulator = WorkerSequentialSimulator(
            snapshot=snapshot,
            pool_ref=matches[0].pool_ref,
            stable_asset_id=self._stable_asset_id,
            base_asset_id=self._base_asset_id,
            stable_decimals=self._stable_decimals,
            snapshot_received_monotonic_ns=(
                result.get("response_received_monotonic_ns")
                if isinstance(result.get("response_received_monotonic_ns"), int)
                else result.get("captured_monotonic_ns")
                if isinstance(result.get("captured_monotonic_ns"), int)
                else time.monotonic_ns()
            ),
            snapshot_received_realtime_ns=(
                result.get("response_received_realtime_ns")
                if isinstance(result.get("response_received_realtime_ns"), int)
                else time.time_ns()
            ),
            perp_symbol=self._perp_symbol,
        )
        self._successes += 1
        self._error = None
        self._next_retry_monotonic_ns = 0
        self._initialized = True
        self._initializing = False

    def simulate_buy_sell(self, quote: Any) -> Any:
        if not self._initialized:
            return None
        if self._error is not None or self._simulator is None:
            return None
        return self._simulator.simulate_buy_sell(quote)

    def describe(self) -> dict[str, object]:
        if self._simulator is not None:
            status = self._simulator.describe()
            status.update({
                "error": self._error,
                "attempts": self._attempts,
                "successes": self._successes,
                "failures": self._failures,
                "next_retry_monotonic_ns": self._next_retry_monotonic_ns,
            })
            return status
        return {
            "mode": "sequential_direct_path",
            "pool_id": self._pool_id,
            "initialized": self._initialized,
            "initializing": self._initializing,
            "attempts": self._attempts,
            "successes": self._successes,
            "failures": self._failures,
            "next_retry_monotonic_ns": self._next_retry_monotonic_ns,
            "error": self._error,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }

    def evidence_bundle(self) -> SimulationEvidenceBundle | None:
        if self._simulator is None:
            return None
        return self._simulator.evidence_bundle()
