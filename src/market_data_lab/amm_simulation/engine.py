from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .adapters import UnsupportedVariant, adapter_for
from .adapters import ProtocolMismatch
from .codec import canonical_hash
from .contracts import (
    AmmPathRequest,
    AmmPathResult,
    AmmSnapshot,
    CpmmPoolBody,
    PoolBody,
    PathStatus,
    PoolRef,
    SwapLeg,
    SwapLegResult,
    VirtualStateRef,
)


MonotonicClock = Callable[[], int]


def _projected_reserve(body: PoolBody, index: int) -> int:
    if isinstance(body, CpmmPoolBody):
        return body.reserve_0_raw if index == 0 else body.reserve_1_raw
    effective_0, effective_1 = body.effective_reserves()
    return effective_0 if index == 0 else effective_1


def _state_hash(snapshot: AmmSnapshot, pools: dict[str, PoolBody], branch: str, step: int) -> str:
    projection = {
        "branch_id": branch,
        "step_index": step,
        "snapshot_hash": snapshot.snapshot_hash,
        "pools": [pool.economic_projection for pool in pools.values()],
    }
    return canonical_hash(projection, domain="amm_virtual_state")


def _failed_result(
    request: AmmPathRequest,
    status: PathStatus,
    reason: str,
    *,
    failed_leg_id: str | None,
    legs: tuple[SwapLegResult, ...],
    balances: dict[str, int],
) -> AmmPathResult:
    return AmmPathResult(
        request_id=request.request_id,
        snapshot_id=request.snapshot.snapshot_id,
        snapshot_hash=request.snapshot.snapshot_hash,
        status=status,
        reason=reason,
        complete=False,
        failed_leg_id=failed_leg_id,
        leg_results=legs,
        final_balances=tuple(sorted(balances.items())),
        worker_generation=request.snapshot.worker_generation,
        source_epoch=request.snapshot.source_epoch,
        boot_id=request.snapshot.boot_id,
        context_slot=request.snapshot.context_slot,
    )


class PathSimulator:
    """Deterministic pure executor for linear exact AMM paths."""

    def __init__(self, *, monotonic_ns: MonotonicClock) -> None:
        self._monotonic_ns = monotonic_ns

    def simulate(self, request: AmmPathRequest, *, replay: bool = False) -> AmmPathResult:
        """Simulate one immutable path.

        ``replay=True`` is an explicit historical/offline mode.  It disables
        live freshness and request-deadline gates so an archived evidence
        bundle can be replayed after its original TTL/deadline.  No caller
        may use that mode for a live path.
        """
        if request.execution_policy != "require_complete":
            return _failed_result(
                request,
                "unsupported",
                "best_effort execution is unsupported",
                failed_leg_id=None,
                legs=(),
                balances=dict(request.initial_balances),
            )
        if not request.legs:
            return _failed_result(
                request,
                "invalid_request",
                "path must contain at least one leg",
                failed_leg_id=None,
                legs=(),
                balances=dict(request.initial_balances),
            )
        if request.snapshot.chain_consistency != request.required_consistency:
            return _failed_result(
                request,
                "unsupported",
                "snapshot consistency does not satisfy request",
                failed_leg_id=None,
                legs=(),
                balances=dict(request.initial_balances),
            )

        # Check snapshot freshness
        if not replay and self._monotonic_ns() >= request.snapshot.state_valid_until_monotonic_ns:
            return _failed_result(
                request,
                "state_unavailable",
                "snapshot expired",
                failed_leg_id=None,
                legs=(),
                balances=dict(request.initial_balances),
            )

        deadline = request.deadline_monotonic_ns
        if not replay and deadline is not None and self._monotonic_ns() >= deadline:
            return _failed_result(
                request,
                "deadline_exceeded",
                "deadline already expired",
                failed_leg_id=None,
                legs=(),
                balances=dict(request.initial_balances),
            )

        pools = {pool.pool_ref.pool_id: pool for pool in request.snapshot.pools}
        balances = dict(request.initial_balances)
        results: list[SwapLegResult] = []
        previous_output_by_leg: dict[str, int] = {}
        branch_id = request.request_id
        previous_leg_id: str | None = None

        for step, leg in enumerate(request.legs):
            if previous_leg_id is not None and leg.amount_source == "previous_output" and leg.previous_leg_id != previous_leg_id:
                return _failed_result(
                    request,
                    "unsupported",
                    "route shape is not a linear path",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            if previous_leg_id is not None and leg.input_asset_id != request.legs[step - 1].output_asset_id:
                return _failed_result(
                    request,
                    "invalid_request",
                    "leg assets are not connected",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            if not replay and self._monotonic_ns() >= request.snapshot.state_valid_until_monotonic_ns:
                return _failed_result(
                    request,
                    "state_unavailable",
                    "snapshot expired between path legs",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            if not replay and deadline is not None and self._monotonic_ns() >= deadline:
                return _failed_result(
                    request,
                    "deadline_exceeded",
                    "deadline expired during path",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )

            state_before_hash = _state_hash(request.snapshot, pools, branch_id, step)
            try:
                pool = pools[leg.pool_ref.pool_id]
            except KeyError:
                return _failed_result(
                    request,
                    "invalid_request",
                    "pool is absent from the snapshot",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            if pool.pool_ref != leg.pool_ref:
                return _failed_result(
                    request,
                    "invalid_request",
                    "leg pool identity does not match snapshot pool",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            pool_assets = {pool.pool_ref.asset_0_id, pool.pool_ref.asset_1_id}
            if {leg.input_asset_id, leg.output_asset_id} != pool_assets or leg.input_asset_id == leg.output_asset_id:
                return _failed_result(
                    request,
                    "invalid_request",
                    "pool asset order does not match leg direction",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )

            requested = (
                leg.amount_raw
                if leg.amount_source == "literal"
                else previous_output_by_leg.get(leg.previous_leg_id or "")
            )
            if requested is None or requested <= 0:
                return _failed_result(
                    request,
                    "invalid_request",
                    "requested amount is unavailable",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            zero_for_one = pool.pool_ref.asset_0_id == leg.input_asset_id
            if leg.mode == "exact_in" and balances.get(leg.input_asset_id, 0) < requested:
                return _failed_result(
                    request,
                    "insufficient_balance",
                    "initial scenario balance does not cover the leg",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            if leg.mode == "exact_out" and (
                leg.maximum_gross_input_raw is None
                or balances.get(leg.input_asset_id, 0) < leg.maximum_gross_input_raw
            ):
                return _failed_result(
                    request,
                    "insufficient_balance",
                    "exact-out requires a bounded funded input limit",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )

            try:
                adapter = adapter_for(pool.pool_ref.protocol, pool.pool_ref.pool_spec_version)
            except UnsupportedVariant as exc:
                return _failed_result(
                    request,
                    "unsupported",
                    str(exc)[:512],
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            try:
                transition = (
                    adapter.exact_in(pool, requested, zero_for_one)
                    if leg.mode == "exact_in"
                    else adapter.exact_out(pool, requested, zero_for_one)
                )
            except ArithmeticError as exc:
                return _failed_result(
                    request,
                    "arithmetic_error",
                    str(exc)[:512],
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            except ProtocolMismatch as exc:
                return _failed_result(
                    request,
                    "unsupported",
                    str(exc)[:512],
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            except UnsupportedVariant as exc:
                return _failed_result(
                    request,
                    "insufficient_liquidity",
                    str(exc)[:512],
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )

            if (
                leg.mode == "exact_in"
                and leg.minimum_net_output_raw is not None
                and transition.net_output < leg.minimum_net_output_raw
            ):
                return _failed_result(
                    request,
                    "limit_exceeded",
                    "minimum output is not satisfied",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )
            if leg.mode == "exact_out" and transition.gross_input > leg.maximum_gross_input_raw:
                return _failed_result(
                    request,
                    "limit_exceeded",
                    "maximum input is exceeded",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )

            # Do not publish a complete leg whose calculation crossed the
            # caller's live deadline.  The transition is still discarded from
            # the externally visible prefix, preserving fail-closed semantics.
            if not replay and deadline is not None and self._monotonic_ns() >= deadline:
                return _failed_result(
                    request,
                    "deadline_exceeded",
                    "deadline expired during leg",
                    failed_leg_id=leg.leg_id,
                    legs=tuple(results),
                    balances=balances,
                )

            balances[leg.input_asset_id] = balances.get(leg.input_asset_id, 0) - transition.gross_input
            balances[leg.output_asset_id] = balances.get(leg.output_asset_id, 0) + transition.net_output
            pools[pool.pool_ref.pool_id] = transition.body_after
            state_after_hash = _state_hash(request.snapshot, pools, branch_id, step + 1)
            results.append(
                SwapLegResult(
                    leg_id=leg.leg_id,
                    status="complete",
                    complete=True,
                    requested_amount_raw=requested,
                    actual_gross_input_raw=transition.gross_input,
                    input_received_by_pool_raw=transition.gross_input,
                    input_used_for_curve_raw=transition.effective_input,
                    gross_pool_output_raw=transition.gross_pool_output,
                    actual_net_output_raw=transition.net_output,
                    unconsumed_input_raw=0,
                    fee_amount_raw=transition.gross_input - transition.effective_input,
                    reserve_0_after_raw=_projected_reserve(transition.body_after, 0),
                    reserve_1_after_raw=_projected_reserve(transition.body_after, 1),
                    state_before_hash=state_before_hash,
                    state_after_hash=state_after_hash,
                    reason="swap completed",
                ),
            )
            previous_output_by_leg[leg.leg_id] = transition.net_output
            previous_leg_id = leg.leg_id

            # A snapshot cannot become live again while a path is running.
            # Check after the transition as well as before the next leg so a
            # clock advancing inside an adapter cannot promote a stale result.
            if not replay and self._monotonic_ns() >= request.snapshot.state_valid_until_monotonic_ns:
                return _failed_result(
                    request,
                    "state_unavailable",
                    "snapshot expired during path",
                    failed_leg_id=(
                        request.legs[step + 1].leg_id
                        if step + 1 < len(request.legs)
                        else None
                    ),
                    legs=tuple(results),
                    balances=balances,
                )

        return AmmPathResult(
            request_id=request.request_id,
            snapshot_id=request.snapshot.snapshot_id,
            snapshot_hash=request.snapshot.snapshot_hash,
            status="complete",
            complete=True,
            failed_leg_id=None,
            leg_results=tuple(results),
            final_balances=tuple(sorted(balances.items())),
            reason="all legs completed",
            worker_generation=request.snapshot.worker_generation,
            source_epoch=request.snapshot.source_epoch,
            boot_id=request.snapshot.boot_id,
            context_slot=request.snapshot.context_slot,
        )


__all__ = ["PathSimulator"]
