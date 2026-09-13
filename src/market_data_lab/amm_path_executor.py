"""
Stateful exact AMM path simulation with snapshot/overlay semantics.

Immutability contract:
- Input snapshots are never mutated.
- Each swap consumes the post-state of the previous step as its input state.
- Observed (real) state remains unchanged even during concurrent calls.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from market_data_lab.amm_core import (
    CpmPoolState,
    CpmSwapResult,
    ClmmPoolState,
    ClmmSwapResult,
    PostTradePath,
    PostTradeSnapshot,
    _decimal_to_raw,
    _raw_to_decimal,
    simulate_cpm_swap_exact_in,
    simulate_cpm_swap_exact_out,
    simulate_clmm_swap_exact_in,
)


# ---------------------------------------------------------------------------
# Snapshot bundle (collection of pool states to execute against)
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SnapShot:
    """Immutable snapshot of one pool for simulation."""

    pool_id: str
    protocol: str
    base_mint: str
    quote_mint: str
    base_decimals: int
    quote_decimals: int
    reserve_base_raw: int
    reserve_quote_raw: int
    fee_bps: Decimal
    sqrt_price_x64: int | None = None
    tick_current: int | None = None
    tick_liquidity: dict[int, int] | None = None
    observed_realtime_ns: int = 0
    observed_monotonic_ns: int = 0
    source_epoch: int = 0

    @classmethod
    def from_clmm(cls, state: ClmmPoolState, **extra: Any) -> "SnapShot":
        return cls(
            pool_id=state.pool_id,
            protocol="raydium_clmm",
            base_mint=state.token_0_mint,
            quote_mint=state.token_1_mint,
            base_decimals=state.token_0_decimals,
            quote_decimals=state.token_1_decimals,
            reserve_base_raw=0,  # CLMM doesn't expose reserves directly
            reserve_quote_raw=0,
            fee_bps=Decimal(5),  # Default Raydium CLMM fee
            sqrt_price_x64=state.sqrt_price_x64,
            tick_current=state.tick_current,
            observed_realtime_ns=state.received_realtime_ns,
            observed_monotonic_ns=state.received_monotonic_ns,
            **extra,
        )

    @classmethod
    def from_cpm(
        cls,
        pool_id: str,
        base_mint: str,
        quote_mint: str,
        base_decimals: int,
        quote_decimals: int,
        reserve_base_raw: int,
        reserve_quote_raw: int,
        fee_bps: Decimal,
        **extra: Any,
    ) -> "SnapShot":
        return cls(
            pool_id=pool_id,
            protocol="cpmm",
            base_mint=base_mint,
            quote_mint=quote_mint,
            base_decimals=base_decimals,
            quote_decimals=quote_decimals,
            reserve_base_raw=reserve_base_raw,
            reserve_quote_raw=reserve_quote_raw,
            fee_bps=fee_bps,
            observed_realtime_ns=0,
            observed_monotonic_ns=0,
            **extra,
        )

    def to_cpm(self) -> CpmPoolState:
        return CpmPoolState(
            pool_id=self.pool_id,
            base_mint=self.base_mint,
            quote_mint=self.quote_mint,
            base_decimals=self.base_decimals,
            quote_decimals=self.quote_decimals,
            reserve_base_raw=self.reserve_base_raw,
            reserve_quote_raw=self.reserve_quote_raw,
            fee_bps=self.fee_bps,
            protocol="cpmm",
            observed_realtime_ns=self.observed_realtime_ns,
            observed_monotonic_ns=self.observed_monotonic_ns,
        )


@dataclass(frozen=True, slots=True)
class PathStep:
    """One step in a multi-hop path."""

    pool_id: str
    amount_in_raw: int
    amount_out_raw: int
    fee_raw: int
    reserve_before_base: int
    reserve_before_quote: int
    reserve_after_base: int
    reserve_after_quote: int
    sqrt_price_start_x64: int | None = None
    sqrt_price_end_x64: int | None = None
    tick_start: int | None = None
    tick_end: int | None = None
    status: str = "ok"
    error: str | None = None
    direction: str = ""

    def to_post_snapshot(self, snap: SnapShot) -> PostTradeSnapshot:
        return PostTradeSnapshot(
            pool_id=snap.pool_id,
            protocol=snap.protocol,
            base_mint=snap.base_mint,
            quote_mint=snap.quote_mint,
            base_decimals=snap.base_decimals,
            quote_decimals=snap.quote_decimals,
            reserve_base_raw=self.reserve_after_base,
            reserve_quote_raw=self.reserve_after_quote,
            sqrt_price_x64=self.sqrt_price_end_x64,
            tick_current=self.tick_end,
            fee_bps=snap.fee_bps,
            swap_direction=self.direction,
            amount_in_raw=self.amount_in_raw,
            amount_out_raw=self.amount_out_raw,
            fee_raw=self.fee_raw,
            status=self.status,
            error=self.error,
            observed_realtime_ns=snap.observed_realtime_ns,
            observed_monotonic_ns=snap.observed_monotonic_ns,
            source_epoch=snap.source_epoch,
        )


# ---------------------------------------------------------------------------
# Path executor
# ---------------------------------------------------------------------------

class PathExecutor:
    """Execute exact-amount AMM paths against a snapshot bundle.

    Usage:
        executor = PathExecutor(snapshots={...})
        result = executor.execute(
            path=[("pool1", "base_in", amount), ("pool2", "quote_in", amount)],
            start_amount=Decimal(...),
            start_token="base",
        )
    """

    def __init__(self, snapshots: dict[str, SnapShot]) -> None:
        if not snapshots:
            raise ValueError("snapshot bundle must not be empty")
        self._snapshots = dict(snapshots)
        self._post_states: dict[str, SnapShot] = {}

    def clear_post_states(self) -> None:
        """Reset post-states so path execution starts fresh from observed state."""
        self._post_states.clear()

    def _get_pool_state_for_step(self, pool_id: str) -> SnapShot:
        """Return the appropriate snapshot for a pool: post-state if exists, else observed."""
        if pool_id in self._post_states:
            return self._post_states[pool_id]
        if pool_id not in self._snapshots:
            raise KeyError(f"pool {pool_id} not in snapshot bundle")
        return self._snapshots[pool_id]

    def execute_exact_in(
        self,
        path: list[tuple[str, str, Decimal]],
        start_amount: Decimal,
        start_token: str,
    ) -> PostTradePath:
        """Execute an exact-input swap through a multi-hop path.

        Args:
            path: list of (pool_id, direction, amount) where direction is "base_in" or "quote_in"
            start_amount: the exact input amount for the first swap
            start_token: "base" or "quote" - the token being input

        Returns PostTradePath with all step results.
        """

        if not path:
            return PostTradePath(
                steps=(),
                overall_direction="",
                total_amount_in_raw=0,
                total_amount_out_raw=0,
                total_fee_raw=0,
                status="invalid_request",
                error="empty path",
            )

        self.clear_post_states()

        current_token = start_token
        current_amount = start_amount
        steps: list[PostTradeSnapshot] = []
        total_in_raw = 0
        total_out_raw = 0
        total_fee_raw = 0
        path_ids: list[str] = []

        for i, (pool_id, direction_hint, _) in enumerate(path):
            try:
                snap = self._get_pool_state_for_step(pool_id)
            except KeyError as e:
                return PostTradePath(
                    steps=tuple(steps),
                    overall_direction="",
                    total_amount_in_raw=total_in_raw,
                    total_amount_out_raw=total_out_raw,
                    total_fee_raw=total_fee_raw,
                    status="pool_not_found",
                    error=str(e),
                    path_ids=tuple(path_ids),
                )

            path_ids.append(pool_id)

            if i == 0:
                # First step uses start_amount
                step_amount = start_amount
            else:
                # Subsequent steps use the output from the previous step
                step_amount = _raw_to_decimal(current_amount, snap.base_decimals if current_token == "base" else snap.quote_decimals)

            if step_amount <= 0:
                return PostTradePath(
                    steps=tuple(steps),
                    overall_direction="",
                    total_amount_in_raw=total_in_raw,
                    total_amount_out_raw=total_out_raw,
                    total_fee_raw=total_fee_raw,
                    status="insufficient_amount",
                    error=f"step {i}: amount is zero or negative",
                    path_ids=tuple(path_ids),
                )

            # Determine direction for this step
            if i == 0:
                step_direction = direction_hint
            else:
                # Alternate based on path construction
                if current_token == "base":
                    step_direction = "base_in" if "base_in" in direction_hint else "quote_in"
                else:
                    step_direction = "quote_in" if "quote_in" in direction_hint else "base_in"

            base_in_raw = 0
            quote_in_raw = 0

            if step_direction == "base_in":
                base_in_raw = _decimal_to_raw(step_amount, snap.base_decimals)
            elif step_direction == "quote_in":
                quote_in_raw = _decimal_to_raw(step_amount, snap.quote_decimals)
            else:
                return PostTradePath(
                    steps=tuple(steps),
                    overall_amount_in_raw=0,
                    total_amount_out_raw=0,
                    total_fee_raw=0,
                    status="invalid_direction",
                    error=f"step {i}: unknown direction {step_direction}",
                    path_ids=tuple(path_ids),
                )

            # Execute the swap based on protocol
            if snap.protocol == "cpmm":
                cpm_snap = snap.to_cpm()
                result = simulate_cpm_swap_exact_in(
                    cpm_snap,
                    base_in_raw=base_in_raw,
                    quote_in_raw=quote_in_raw,
                )

                if result.status != "ok":
                    return PostTradePath(
                        steps=tuple(steps),
                        overall_direction="",
                        total_amount_in_raw=total_in_raw,
                        total_amount_out_raw=total_out_raw,
                        total_fee_raw=total_fee_raw,
                        status=result.status,
                        error=result.error,
                        path_ids=tuple(path_ids),
                    )

                # Create post-state snapshot
                post_snap = SnapShot(
                    pool_id=snap.pool_id,
                    protocol=snap.protocol,
                    base_mint=snap.base_mint,
                    quote_mint=snap.quote_mint,
                    base_decimals=snap.base_decimals,
                    quote_decimals=snap.quote_decimals,
                    reserve_base_raw=result.reserve_base_after_raw,
                    reserve_quote_raw=result.reserve_quote_after_raw,
                    fee_bps=snap.fee_bps,
                    observed_realtime_ns=snap.observed_realtime_ns,
                    observed_monotonic_ns=snap.observed_monotonic_ns,
                    source_epoch=snap.source_epoch,
                )
                self._post_states[pool_id] = post_snap

                step = PathStep(
                    pool_id=pool_id,
                    amount_in_raw=result.amount_in_raw,
                    amount_out_raw=result.amount_out_raw,
                    fee_raw=result.fee_raw,
                    reserve_before_base=cpm_snap.reserve_base_raw if step_direction == "base_in" else result.reserve_base_after_raw,
                    reserve_before_quote=cpm_snap.reserve_quote_raw if step_direction == "quote_in" else result.reserve_quote_after_raw,
                    reserve_after_base=result.reserve_base_after_raw,
                    reserve_after_quote=result.reserve_quote_after_raw,
                    status=result.status,
                    error=result.error,
                    direction=result.direction,
                )
                current_amount = _raw_to_decimal(result.amount_out_raw, snap.quote_decimals if step_direction == "base_in" else snap.base_decimals)
                current_token = "quote" if step_direction == "base_in" else "base"

            elif snap.protocol == "raydium_clmm":
                # CLMM simulation (simplified)
                clmm_result = simulate_clmm_swap_exact_in(
                    ClmmPoolState(
                        pool_id=snap.pool_id,
                        token_0_mint=snap.base_mint,
                        token_1_mint=snap.quote_mint,
                        token_0_decimals=snap.base_decimals,
                        token_1_decimals=snap.quote_decimals,
                        sqrt_price_x64=snap.sqrt_price_x64 or 0,
                        tick_current=snap.tick_current or 0,
                        slot=None,
                        received_realtime_ns=snap.observed_realtime_ns,
                        received_monotonic_ns=snap.observed_monotonic_ns,
                        source="simulation",
                    ),
                    base_in_raw=base_in_raw,
                    quote_in_raw=quote_in_raw,
                    tick_liquidity=snap.tick_liquidity,
                )

                if clmm_result.status != "ok":
                    return PostTradePath(
                        steps=tuple(steps),
                        overall_direction="",
                        total_amount_in_raw=total_in_raw,
                        total_amount_out_raw=total_out_raw,
                        total_fee_raw=total_fee_raw,
                        status=clmm_result.status,
                        error=clmm_result.error,
                        path_ids=tuple(path_ids),
                    )

                # Post-CLMM state (approximate using sqrt price change)
                post_snap = SnapShot(
                    pool_id=snap.pool_id,
                    protocol=snap.protocol,
                    base_mint=snap.base_mint,
                    quote_mint=snap.quote_mint,
                    base_decimals=snap.base_decimals,
                    quote_decimals=snap.quote_decimals,
                    reserve_base_raw=0,
                    reserve_quote_raw=0,
                    fee_bps=snap.fee_bps,
                    sqrt_price_x64=clmm_result.sqrt_price_end_x64,
                    tick_current=clmm_result.tick_current_end,
                    tick_liquidity=snap.tick_liquidity,
                    observed_realtime_ns=snap.observed_realtime_ns,
                    observed_monotonic_ns=snap.observed_monotonic_ns,
                    source_epoch=snap.source_epoch,
                )
                self._post_states[pool_id] = post_snap

                step = PathStep(
                    pool_id=pool_id,
                    amount_in_raw=clmm_result.amount_in_raw,
                    amount_out_raw=clmm_result.amount_out_raw,
                    fee_raw=clmm_result.fee_raw,
                    reserve_before_base=0,
                    reserve_before_quote=0,
                    reserve_after_base=0,
                    reserve_after_quote=0,
                    sqrt_price_start_x64=clmm_result.sqrt_price_start_x64,
                    sqrt_price_end_x64=clmm_result.sqrt_price_end_x64,
                    tick_start=clmm_result.tick_current_start,
                    tick_end=clmm_result.tick_current_end,
                    status=clmm_result.status,
                    error=clmm_result.error,
                    direction=clmm_result.direction,
                )
                current_amount = _raw_to_decimal(clmm_result.amount_out_raw, snap.quote_decimals if step_direction == "base_in" else snap.base_decimals)
                current_token = "quote" if step_direction == "base_in" else "base"

            else:
                return PostTradePath(
                    steps=tuple(steps),
                    overall_direction="",
                    total_amount_in_raw=total_amount_in_raw,
                    total_amount_out_raw=total_out_raw,
                    total_fee_raw=total_fee_raw,
                    status="unsupported_protocol",
                    error=f"pool {pool_id} has unsupported protocol {snap.protocol}",
                    path_ids=tuple(path_ids),
                )

            total_in_raw += step.amount_in_raw
            total_out_raw += step.amount_out_raw
            total_fee_raw += step.fee_raw
            steps.append(step.to_post_snapshot(snap))

        return PostTradePath(
            steps=tuple(steps),
            overall_direction=f"exact_in_{start_token}",
            total_amount_in_raw=total_in_raw,
            total_amount_out_raw=total_amount_out_raw,
            total_fee_raw=total_fee_raw,
            status="ok",
            path_ids=tuple(path_ids),
        )

    def execute_exact_out(
        self,
        path: list[tuple[str, str, Decimal]],
        target_amount: Decimal,
        target_token: str,
    ) -> PostTradePath:
        """Execute an exact-output swap through a multi-hop path.

        Args:
            path: list of (pool_id, direction, _) tuples
            target_amount: the exact output amount desired
            target_token: "base" or "quote" - the token being received

        Returns PostTradePath with all step results.
        """

        if not path:
            return PostTradePath(
                steps=(),
                overall_direction="",
                total_amount_in_raw=0,
                total_amount_out_raw=0,
                total_fee_raw=0,
                status="invalid_request",
                error="empty path",
            )

        if target_amount <= 0:
            return PostTradePath(
                steps=(),
                overall_direction="",
                total_amount_in_raw=0,
                total_amount_out_raw=0,
                total_fee_raw=0,
                status="invalid_request",
                error="target amount must be positive",
            )

        self.clear_post_states()

        # Work backwards: determine required input for each pool
        current_target = target_amount
        current_token = target_token

        steps: list[PostTradeSnapshot] = []
        total_in_raw = 0
        total_out_raw = 0
        total_fee_raw = 0
        path_ids: list[str] = []

        # Execute forward, tracking required inputs
        forward_steps: list[tuple[str, str, int, int, int]] = []  # (pool_id, direction, amount_in_raw, amount_out_raw, fee_raw)

        for i, (pool_id, _, _) in enumerate(reversed(path)):
            try:
                snap = self._get_pool_state_for_step(pool_id)
            except KeyError as e:
                return PostTradePath(
                    steps=tuple(steps),
                    overall_direction="",
                    total_amount_in_raw=total_in_raw,
                    total_amount_out_raw=total_out_raw,
                    total_fee_raw=total_fee_raw,
                    status="pool_not_found",
                    error=str(e),
                    path_ids=tuple(path_ids),
                )

            # Determine direction for exact-out
            if current_token == "base":
                direction = "quote_in_base_out"
                out_raw = _decimal_to_raw(current_target, snap.base_decimals)
            else:
                direction = "base_in_quote_out"
                out_raw = _decimal_to_raw(current_target, snap.quote_decimals)

            if snap.protocol == "cpmm":
                cpm_snap = snap.to_cpm()
                result = simulate_cpm_swap_exact_out(
                    cpm_snap,
                    base_out_raw=out_raw if current_token == "base" else 0,
                    quote_out_raw=out_raw if current_token == "quote" else 0,
                )

                if result.status != "ok":
                    return PostTradePath(
                        steps=tuple(steps),
                        overall_direction="",
                        total_amount_in_raw=total_amount_in_raw,
                        total_amount_out_raw=total_out_raw,
                        total_fee_raw=total_fee_raw,
                        status=result.status,
                        error=result.error,
                        path_ids=tuple(path_ids),
                    )

                # Track for forward execution
                forward_steps.append((pool_id, result.direction, result.amount_in_raw, result.amount_out_raw, result.fee_raw))

                current_target = _raw_to_decimal(result.amount_in_raw, snap.base_decimals if result.direction.startswith("swap_base") else snap.quote_decimals)
                current_token = "quote" if result.direction.startswith("swap_base") else "base"

            elif snap.protocol == "raydium_clmm":
                # For CLMM exact-out, we approximate by inverting exact-in
                # Full implementation would require more complex math
                clmm_result = simulate_clmm_swap_exact_in(
                    ClmmPoolState(
                        pool_id=snap.pool_id,
                        token_0_mint=snap.base_mint,
                        token_1_mint=snap.quote_mint,
                        token_0_decimals=snap.base_decimals,
                        token_1_decimals=snap.quote_decimals,
                        sqrt_price_x64=snap.sqrt_price_x64 or 0,
                        tick_current=snap.tick_current or 0,
                        slot=None,
                        received_realtime_ns=snap.observed_realtime_ns,
                        received_monotonic_ns=snap.observed_monotonic_ns,
                        source="simulation",
                    ),
                    base_in_raw=0,
                    quote_in_raw=_decimal_to_raw(current_target, snap.quote_decimals) if current_token == "quote" else 0,
                    tick_liquidity=snap.tick_liquidity,
                )

                # Use the input as the required output for next step
                current_target = _raw_to_decimal(clmm_result.amount_in_raw, snap.base_decimals if current_token == "base" else snap.quote_decimals)
                current_token = "quote" if current_token == "base" else "base"

                forward_steps.append((pool_id, clmm_result.direction, clmm_result.amount_in_raw, clmm_result.amount_out_raw, clmm_result.fee_raw))

            else:
                return PostTradePath(
                    steps=tuple(steps),
                    overall_direction="",
                    total_amount_in_raw=0,
                    total_amount_out_raw=0,
                    total_fee_raw=0,
                    status="unsupported_protocol",
                    error=f"pool {pool_id} has unsupported protocol {snap.protocol}",
                )

        # Now execute forward with computed amounts
        for i, (pool_id, direction, in_raw, out_raw, fee_raw) in enumerate(reversed(forward_steps)):
            try:
                snap = self._get_pool_state_for_step(pool_id)
            except KeyError:
                return PostTradePath(
                    steps=tuple(steps),
                    overall_direction="",
                    total_amount_in_raw=total_amount_in_raw,
                    total_amount_out_raw=total_amount_out_raw,
                    total_fee_raw=total_fee_raw,
                    status="pool_not_found",
                    error=f"pool {pool_id} not found during forward execution",
                    path_ids=tuple(path_ids),
                )

            path_ids.append(pool_id)

            if snap.protocol == "cpmm":
                cpm_snap = snap.to_cpm()
                if "quote_in" in direction:
                    result = simulate_cpm_swap_exact_in(cpm_snap, quote_in_raw=in_raw)
                else:
                    result = simulate_cpm_swap_exact_in(cpm_snap, base_in_raw=in_raw)

                post_snap = SnapShot(
                    pool_id=snap.pool_id,
                    protocol=snap.protocol,
                    base_mint=snap.base_mint,
                    quote_mint=snap.quote_mint,
                    base_decimals=snap.base_decimals,
                    quote_decimals=snap.quote_decimals,
                    reserve_base_raw=result.reserve_base_after_raw,
                    reserve_quote_raw=result.reserve_quote_after_raw,
                    fee_bps=snap.fee_bps,
                    observed_realtime_ns=snap.observed_realtime_ns,
                    observed_monotonic_ns=snap.observed_monotonic_ns,
                    source_epoch=snap.source_epoch,
                )
                self._post_states[pool_id] = post_snap

                step = PathStep(
                    pool_id=pool_id,
                    amount_in_raw=result.amount_in_raw,
                    amount_out_raw=result.amount_out_raw,
                    fee_raw=result.fee_raw,
                    reserve_before_base=cpm_snap.reserve_base_raw,
                    reserve_before_quote=cpm_snap.reserve_quote_raw,
                    reserve_after_base=result.reserve_base_after_raw,
                    reserve_after_quote=result.reserve_quote_after_raw,
                    status=result.status,
                    error=result.error,
                    direction=result.direction,
                )

            elif snap.protocol == "raydium_clmm":
                clmm_result = simulate_clmm_swap_exact_in(
                    ClmmPoolState(
                        pool_id=snap.pool_id,
                        token_0_mint=snap.base_mint,
                        token_1_mint=snap.quote_mint,
                        token_0_decimals=snap.base_decimals,
                        token_1_decimals=snap.quote_decimals,
                        sqrt_price_x64=snap.sqrt_price_x64 or 0,
                        tick_current=snap.tick_current or 0,
                        slot=None,
                        received_realtime_ns=snap.observed_realtime_ns,
                        received_monotonic_ns=snap.observed_monotonic_ns,
                        source="simulation",
                    ),
                    base_in_raw=in_raw if "base_in" in direction else 0,
                    quote_in_raw=in_raw if "quote_in" in direction else 0,
                    tick_liquidity=snap.tick_liquidity,
                )

                post_snap = SnapShot(
                    pool_id=snap.pool_id,
                    protocol=snap.protocol,
                    base_mint=snap.base_mint,
                    quote_mint=snap.quote_mint,
                    base_decimals=snap.base_decimals,
                    quote_decimals=snap.quote_decimals,
                    reserve_base_raw=0,
                    reserve_quote_raw=0,
                    fee_bps=snap.fee_bps,
                    sqrt_price_x64=clmm_result.sqrt_price_end_x64,
                    tick_current=clmm_result.tick_current_end,
                    tick_liquidity=snap.tick_liquidity,
                    observed_realtime_ns=snap.observed_realtime_ns,
                    observed_monotonic_ns=snap.observed_monotonic_ns,
                    source_epoch=snap.source_epoch,
                )
                self._post_states[pool_id] = post_snap

                step = PathStep(
                    pool_id=pool_id,
                    amount_in_raw=clmm_result.amount_in_raw,
                    amount_out_raw=clmm_result.amount_out_raw,
                    fee_raw=clmm_result.fee_raw,
                    reserve_before_base=0,
                    reserve_before_quote=0,
                    reserve_after_base=0,
                    reserve_after_quote=0,
                    sqrt_price_start_x64=clmm_result.sqrt_price_start_x64,
                    sqrt_price_end_x64=clmm_result.sqrt_price_end_x64,
                    tick_start=clmm_result.tick_current_start,
                    tick_end=clmm_result.tick_current_end,
                    status=clmm_result.status,
                    error=clmm_result.error,
                    direction=clmm_result.direction,
                )

            else:
                return PostTradePath(
                    steps=tuple(steps),
                    overall_direction="",
                    total_amount_in_raw=total_amount_in_raw,
                    total_amount_out_raw=total_amount_out_raw,
                    total_fee_raw=total_fee_raw,
                    status="unsupported_protocol",
                    error=f"pool {pool_id} has unsupported protocol during forward execution",
                    path_ids=tuple(path_ids),
                )

            total_in_raw += step.amount_in_raw
            total_out_raw += step.amount_out_raw
            total_fee_raw += step.fee_raw
            steps.append(step.to_post_snapshot(snap))

        # Determine overall direction from first step
        overall_direction = f"exact_out_{target_token}" if steps else ""

        return PostTradePath(
            steps=tuple(steps),
            overall_direction=overall_direction,
            total_amount_in_raw=total_in_raw,
            total_amount_out_raw=total_amount_out_raw,
            total_fee_raw=total_fee_raw,
            status="ok" if steps else "invalid_request",
            path_ids=tuple(path_ids),
        )


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------

def simulate_path_exact_in(
    snapshots: dict[str, SnapShot],
    path: list[tuple[str, str, Decimal]],
    start_amount: Decimal,
    start_token: str,
) -> PostTradePath:
    """One-shot path simulation (stateless convenience wrapper)."""

    executor = PathExecutor(snapshots)
    return executor.execute_exact_in(path, start_amount, start_token)


def simulate_path_exact_out(
    snapshots: dict[str, SnapShot],
    path: list[tuple[str, str, Decimal]],
    target_amount: Decimal,
    target_token: str,
) -> PostTradePath:
    """One-shot exact-output path simulation (stateless convenience wrapper)."""

    executor = PathExecutor(snapshots)
    return executor.execute_exact_out(path, target_amount, target_token)
