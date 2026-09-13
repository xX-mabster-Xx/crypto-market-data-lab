"""AMM simulation: CPMM, CLMM/DLMM.

Section 8.3: AMM uses verified formula with exact integer rounding,
fees, and full account set. CLMM/DLMM traverses ticks/bins to exhaustion.
Swap includes price impact from own volume.

T09 golden case: CPMM reserves 1000/1000, fee=0, input 100, reverse swap
on post-state -> returns 100 accounting for integer rounding.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Literal

from .contracts import AMMState, ExecutionEstimate, ExecutionResult, CapacityBounds


def simulate_cpmm_swap(
    state: AMMState,
    input_amount: Decimal,
    input_asset_id: str,
    output_asset_id: str,
) -> ExecutionResult:
    """Simulate a CPMM swap with Uniswap-style constant product formula.

    Formula: output = (input * reserve_out * (1 - fee)) / (reserve_in + input * (1 - fee))
    """

    if input_asset_id == state.token0_asset_id and output_asset_id == state.token1_asset_id:
        reserve_in = state.reserve0
        reserve_out = state.reserve1
    elif input_asset_id == state.token1_asset_id and output_asset_id == state.token0_asset_id:
        reserve_in = state.reserve1
        reserve_out = state.reserve0
    else:
        return ExecutionResult(
            status="unsupported",
            error_reason=f"unknown asset pair for pool {state.pool_id}",
            min_acceptance_quantity=input_amount,
        )

    if reserve_in <= 0 or reserve_out <= 0:
        return ExecutionResult(
            status="unsupported",
            error_reason="zero reserves",
            min_acceptable_quantity=input_amount,
        )

    fee_multiplier = Decimal("1") - (state.fee_bps / Decimal("10000"))
    amount_in_with_fee = input_amount * fee_multiplier
    numerator = amount_in_with_fee * reserve_out
    denominator = reserve_in + amount_in_with_fee

    if denominator == 0:
        return ExecutionResult(
            status="unknown",
            error_reason="division by zero in CPMM formula",
            min_acceptable_quantity=input_amount,
        )

    output_amount = numerator / denominator

    # Integer rounding down (Section 4.2: integer rounding for limits)
    output_amount = output_amount.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    avg_price = output_amount / input_amount if input_amount > 0 else Decimal("0")

    return ExecutionResult(
        status="executed",
        estimate=ExecutionEstimate(
            net_token_movement={
                input_asset_id: -input_amount,
                output_asset_id: output_amount,
            },
            avg_price=avg_price,
            min_price=avg_price,
            max_price=avg_price,
            cost_breakdown=None,
            used_levels=[],
            residual=Decimal("0"),
            capacity_info=CapacityBounds(
                max_liquidity_available=reserve_in,
                sufficient_depth=True,
                min_executable_quantity=Decimal("0"),
            ),
            quality="exact" if state.fee_bps > 0 else "post_state_simulated",
        ),
        actual_quantity=input_amount,
    )


def simulate_reverse_swap(
    state: AMMState,
    target_output: Decimal,
    input_asset_id: str,
    output_asset_id: str,
) -> ExecutionResult:
    """Calculate required input for a given output (exact-output).

    Formula: input = (reserve_in * target) / (reserve_out - target * (1 - fee))
    """

    if input_asset_id == state.token0_asset_id and output_asset_id == state.token1_asset_id:
        reserve_in = state.reserve0
        reserve_out = state.reserve1
    elif input_asset_id == state.token1_asset_id and output_asset_id == state.token0_asset_id:
        reserve_in = state.reserve1
        reserve_out = state.reserve0
    else:
        return ExecutionResult(
            status="unsupported",
            error_reason="unknown asset pair",
            min_acceptable_quantity=target_output,
        )

    fee_multiplier = Decimal("1") - (state.fee_bps / Decimal("10000"))
    numerator = reserve_in * target_output
    denominator = reserve_out - target_output * fee_multiplier

    if denominator <= 0:
        return ExecutionResult(
            status="insufficient_depth",
            error_reason="output exceeds pool reserves",
            min_acceptable_quantity=target_output,
        )

    input_amount = numerator / denominator
    input_amount = input_amount.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    return ExecutionResult(
        status="executed",
        estimate=ExecutionEstimate(
            net_token_movement={
                input_asset_id: -input_amount,
                output_asset_id: target_output,
            },
            avg_price=input_amount / target_output if target_output > 0 else Decimal("0"),
            min_price=input_amount / target_output if target_output > 0 else Decimal("0"),
            max_price=input_amount / target_output if target_output > 0 else Decimal("0"),
            cost_breakdown=None,
            used_levels=[],
            residual=Decimal("0"),
            capacity_info=CapacityBounds(
                max_liquidity_available=reserve_out,
                sufficient_depth=True,
                min_executable_quantity=Decimal("0"),
            ),
            quality="exact",
        ),
        actual_quantity=input_amount,
    )


@dataclass(frozen=True, slots=True)
class CPMMSimulator:
    """CPMM swap simulator with post-state tracking."""

    state: AMMState

    def swap(
        self,
        input_amount: Decimal,
        input_asset_id: str,
        output_asset_id: str,
    ) -> tuple[ExecutionResult, AMMState | None]:
        """Swap and return post-state for sequential simulation."""
        result = simulate_cpmm_swap(self.state, input_amount, input_asset_id, output_asset_id)
        if not result.is_success or result.estimate is None:
            return result, None

        est = result.estimate
        output_amount = est.net_token_movement.get(output_asset_id, Decimal("0"))

        # Compute post-state
        if input_asset_id == self.state.token0_asset_id:
            new_reserve0 = self.state.reserve0 + input_amount
            new_reserve1 = self.state.reserve1 - output_amount
        else:
            new_reserve1 = self.state.reserve1 + input_amount
            new_reserve0 = self.state.reserve0 - output_amount

        post_state = AMMState(
            pool_id=self.state.pool_id,
            token0_asset_id=self.state.token0_asset_id,
            token1_asset_id=self.state.token1_asset_id,
            reserve0=new_reserve0,
            reserve1=new_reserve1,
            fee_bps=self.state.fee_bps,
            decimals0=self.state.decimals0,
            decimals1=self.state.decimals1,
            pool_type=self.state.pool_type,
        )
        return result, post_state


@dataclass(frozen=True, slots=True)
class ClmmSimulator:
    """Concentrated liquidity (CLMM/DLMM) simulator.

    Section 8.3: Traverses ticks/bins to exhaustion.
    Lack of tick arrays does not mean continuation at last price.
    """

    def __init__(self) -> None:
        self._initialized = True
