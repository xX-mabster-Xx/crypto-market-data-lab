"""Execution Cost Engine — unified cost calculator.

Section 6.2: Execution Cost Engine owns depth, AMM, exact quantity, fees, FX.
Does NOT compute mark/TVL as executable liquidity.

Section 8: EXE-01 interface for cost_to_acquire, proceeds_from_sell,
apply_virtual_fill, capacity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping, Sequence

from .contracts import (
    ExecutionEstimate,
    ExecutionResult,
    OrderBookSnapshot,
    CapacityBounds,
)
from .orderbook import depth_cost_to_acquire, depth_proceeds_from_sell, apply_virtual_fill_to_book
from .amm import simulate_cpmm_swap, simulate_reverse_swap
from .currency import FXConversion, estimate_capital_charge
from ..cost_breakdown.contracts import CostBreakdown, CostRecord, FeeTier, GasEstimate
from ..cost_breakdown.calculator import aggregate_costs
from ..domain.instruments import InstrumentCapability


@dataclass
class ExecutionCostEngine:
    """Unified execution cost engine.

    Combines L2 order book, AMM, and FX cost calculations.
    Per Section 6.2: does NOT compute mark/TVL as executable liquidity.
    """

    fx_conversion: FXConversion = field(default_factory=FXConversion)
    _capability_cache: dict[str, InstrumentCapability] = field(default_factory=dict)

    def register_capability(self, instrument_id: str, capability: InstrumentCapability) -> None:
        """Register instrument capabilities for fee/rate lookup."""
        self._capability_cache[instrument_id] = capability

    def get_capability(self, instrument_id: str) -> InstrumentCapability | None:
        return self._capability_cache.get(instrument_id)

    def cost_to_acquire(
        self,
        net_quantity: Decimal,
        instrument_id: str,
        book: OrderBookSnapshot | None = None,
        amm_state=None,
        fee_tier: FeeTier | None = None,
        quote_asset_id: str = "USDT",
        gas_estimate: GasEstimate | None = None,
        include_capital_charge: bool = False,
        annual_rate_bps: Decimal | None = None,
        horizon_seconds: Decimal | None = None,
    ) -> ExecutionResult:
        """Calculate cost to acquire net_quantity.

        EXE-01: cost_to_acquire(net_quantity, state, fee_context) -> ExecutionEstimate
        """

        # Try AMM first if state provided
        if amm_state is not None:
            result = simulate_cpmm_swap(
                amm_state, net_quantity, amm_state.token1_asset_id, amm_state.token0_asset_id
            )
            if result.is_success:
                return result

        # Fall back to order book
        if book is not None:
            return depth_cost_to_acquire(
                book, net_quantity, fee_tier, quote_asset_id
            )

        return ExecutionResult(
            status="unsupported",
            error_reason="no execution mechanism available",
            min_acceptable_quantity=net_quantity,
        )

    def proceeds_from_sell(
        self,
        gross_quantity: Decimal,
        instrument_id: str,
        book: OrderBookSnapshot | None = None,
        amm_state=None,
        fee_tier: FeeTier | None = None,
        quote_asset_id: str = "USDT",
    ) -> ExecutionResult:
        """Calculate proceeds from selling gross_quantity.

        EXE-01: proceeds_from_sell(gross_quantity, state, fee_context) -> ExecutionEstimate
        """

        if amm_state is not None:
            # Determine direction for reverse swap
            if amm_state.token0_asset_id == instrument_id:
                return simulate_reverse_swap(
                    amm_state, gross_quantity, amm_state.token1_asset_id, amm_state.token0_asset_id
                )
            else:
                return simulate_reverse_swap(
                    amm_state, gross_quantity, amm_state.token0_asset_id, amm_state.token1_asset_id
                )

        if book is not None:
            return depth_proceeds_from_sell(
                book, gross_quantity, fee_tier, quote_asset_id
            )

        return ExecutionResult(
            status="unsupported",
            error_reason="no execution mechanism available",
            min_acceptable_quantity=gross_quantity,
        )

    def apply_virtual_fill(
        self,
        current_state: OrderBookSnapshot,
        estimate: ExecutionEstimate,
    ) -> OrderBookSnapshot:
        """Apply a virtual fill to update the book state.

        EXE-01: apply_virtual_fill(state, estimate) -> post_state
        """
        return apply_virtual_fill_to_book(
            current_state, estimate.used_levels, Decimal("0")
        )

    def capacity(
        self,
        instrument_id: str,
        book: OrderBookSnapshot | None = None,
        amm_state=None,
    ) -> CapacityBounds:
        """Return known feasible region.

        EXE-01: capacity(state, constraints) -> CapacityBounds
        """
        if book is not None and book.asks:
            max_depth = sum(level.size for level in book.asks)
            return CapacityBounds(
                max_liquidity_available=max_depth,
                sufficient_depth=True,
                min_executable_quantity=Decimal("0"),
            )
        if amm_state is not None:
            return CapacityBounds(
                max_liquidity_available=amm_state.reserve0 if amm_state.token0_asset_id == instrument_id else amm_state.reserve1,
                sufficient_depth=True,
                min_executable_quantity=Decimal("0"),
            )
        return CapacityBounds(
            min_executable_quantity=Decimal("0"),
            sufficient_depth=False,
        )

    def total_cost_quote(
        self,
        results: Sequence[ExecutionResult],
        quote_asset_id: str = "USDT",
    ) -> Decimal:
        """Aggregate total cost from multiple execution results."""
        total = Decimal("0")
        for result in results:
            if result.estimate is not None:
                movement = result.estimate.net_token_movement.get(quote_asset_id, Decimal("0"))
                if movement < 0:
                    total += -movement
        return total
