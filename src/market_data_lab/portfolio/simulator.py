"""Position simulator with latency and partial fill modeling.

Section 12.4: Latency from measurements or scenario grid.
Partial fills, pool route changes, funding event missed, etc.
Section 10.4: Sequential liquidity consumption between virtual operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Sequence, Callable
import random

from ..position_engine.contracts import (
    PositionId, VirtualFill,
)


@dataclass
class LatencyModel:
    """Latency model for virtual fills.

    Section 12.4: Latency from system measurements or scenario grid.
    """

    mean_ms: float = 50.0
    std_ms: float = 20.0
    distribution: str = "normal"  # "normal" | "fixed" | "grid"
    grid_values: list[int] = field(default_factory=lambda: [10, 50, 100, 200, 500])

    def sample(self) -> int:
        """Sample a latency in nanoseconds."""
        if self.distribution == "fixed":
            latency_ms = 50.0
        elif self.distribution == "grid":
            latency_ms = random.choice(self.grid_values)
        else:
            latency_ms = max(0, random.gauss(self.mean_ms, self.std_ms))
        return int(latency_ms * 1_000_000)


@dataclass
class FillResult:
    """Result of a virtual fill simulation."""

    filled_raw: int
    fill_rate: Decimal  # fraction of requested filled
    price_raw: int
    fee_raw: int
    latency_ns: int
    partial: bool = False
    failed: bool = False
    failure_reason: str | None = None


class PositionSimulator:
    """Simulates virtual fills with latency and partial execution.

    Section 12.4: Scenarios — partial fill, source lost during hold,
    pool route changed, exit more expensive, etc.
    Section 10.4: Each virtual fill uses state available at its time,
    including liquidity consumed by previous virtual operations.
    """

    def __init__(self, latency_model: LatencyModel | None = None) -> None:
        self._latency_model = latency_model or LatencyModel()
        self._consumed_liquidity: dict[str, Decimal] = {}

    def simulate_fill(
        self,
        requested_qty: int,
        available_depth: Decimal,
        avg_price: Decimal,
        fee_tier_bps: Decimal,
        leg_id: str,
    ) -> FillResult:
        """Simulate a single virtual fill.

        Section 12.4: Partial fill if depth insufficient.
        Section 10.4: Track consumed liquidity for sequential operations.
        """

        requested_decimal = Decimal(str(requested_qty))
        # Deduct previously consumed liquidity (Section 10.4, EXE-02)
        already_used = self._consumed_liquidity.get(leg_id, Decimal("0"))
        remaining_depth = max(Decimal("0"), available_depth - already_used)

        filled = min(requested_decimal, remaining_depth)
        fill_rate = filled / requested_decimal if requested_decimal > 0 else Decimal("0")

        fee = filled * avg_price * (fee_tier_bps / Decimal("10000"))

        # Track consumed liquidity
        self._consumed_liquidity[leg_id] = already_used + filled

        latency_ns = self._latency_model.sample()

        return FillResult(
            filled_raw=int(filled),
            fill_rate=fill_rate,
            price_raw=int(avg_price),
            fee_raw=int(fee),
            latency_ns=latency_ns,
            partial=fill_rate < Decimal("1"),
            failed=False,
        )

    def simulate_partial_fill(
        self,
        requested_qty: int,
        fill_ratio: Decimal,
        avg_price: Decimal,
        fee_tier_bps: Decimal,
    ) -> FillResult:
        """Simulate a partial fill with given ratio."""
        filled = Decimal(str(requested_qty)) * fill_ratio
        fee = filled * avg_price * (fee_tier_bps / Decimal("10000"))

        return FillResult(
            filled_raw=int(filled),
            fill_rate=fill_ratio,
            price_raw=int(avg_price),
            fee_raw=int(fee),
            latency_ns=self._latency_model.sample(),
            partial=True,
        )

    def reset_consumed_liquidity(self) -> None:
        """Reset tracked liquidity consumption between evaluations."""
        self._consumed_liquidity.clear()

    def create_virtual_fill(
        self,
        leg_id: str,
        fill_result: FillResult,
        requested_raw: int,
    ) -> VirtualFill:
        """Create a VirtualFill from a FillResult."""
        return VirtualFill(
            leg_id=leg_id,
            requested_raw=requested_raw,
            filled_raw=fill_result.filled_raw,
            price_raw=fill_result.price_raw,
            fee_raw=fill_result.fee_raw,
            received_at_offset_ns=fill_result.latency_ns,
        )
