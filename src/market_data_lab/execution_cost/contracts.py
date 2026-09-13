"""Data contracts for execution cost engine.

Section 8: Execution estimates, depth levels, AMM states, capacity bounds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Sequence

from ..cost_breakdown.contracts import CostBreakdown
from ..domain.instruments import InstrumentCapability


@dataclass(frozen=True, slots=True)
class DepthLevel:
    """A single price level in an order book."""

    price: Decimal
    size: Decimal  # in contract units
    # cumulative prefix sums for efficient size search
    cum_size: Decimal = Decimal("0")
    cum_value: Decimal = Decimal("0")


@dataclass
class OrderBookSnapshot:
    """Immutable snapshot of a book at a point in time."""

    instrument_id: str
    venue_id: str
    bids: list[DepthLevel]  # sorted descending by price
    asks: list[DepthLevel]  # sorted ascending by price
    timestamp_ns: int
    version: int
    sequence: int | None = None
    checksum: str | None = None

    @property
    def best_bid(self) -> DepthLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> DepthLevel | None:
        return self.asks[0] if self.asks else None

    @property
    def spread(self) -> Decimal | None:
        bb = self.best_bid
        ba = self.best_ask
        if bb is None or ba is None:
            return None
        return ba.price - bb.price


@dataclass(frozen=True, slots=True)
class OrderBook:
    """Order book with prefix sums for efficient size queries."""

    snapshot: OrderBookSnapshot
    _ask_prefix: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    _bid_prefix: list[tuple[Decimal, Decimal]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self._ask_prefix:
            self._build_prefixes()

    def _build_prefixes(self) -> None:
        # Build prefix sums for asks (ascending price) and bids (descending price)
        cum_size = Decimal("0")
        cum_value = Decimal("0")
        for ask in sorted(self.snapshot.asks, key=lambda x: x.price):
            cum_size += ask.size
            cum_value += ask.size * ask.price
            object.__setattr__(self, "_ask_prefix", [*self._ask_prefix, (cum_size, cum_value)])
        cum_size = Decimal("0")
        cum_value = Decimal("0")
        for bid in sorted(self.snapshot.bids, key=lambda x: -x.price):
            cum_size += bid.size
            cum_value += bid.size * bid.price
            object.__setattr__(self, "_bid_prefix", [*self._bid_prefix, (cum_size, cum_value)])

    def cost_to_acquire(self, net_quantity: Decimal) -> Decimal:
        """Total cost to buy `net_quantity` from the ask side, including partial fills."""
        remaining = net_quantity
        total_cost = Decimal("0")
        for cum_size, cum_value in self._ask_prefix:
            if cum_size >= net_quantity:
                # Partial fill on this level
                consumed = net_quantity - (cum_size - remaining)  # simplified
                total_cost = cum_value
                # Adjust for exact partial
                excess = cum_size - net_quantity
                if excess > 0 and remaining > 0:
                    avg_price = cum_value / cum_size if cum_size > 0 else Decimal("0")
                    total_cost -= excess * avg_price
                return total_cost
            remaining = net_quantity - cum_size
        # Not enough depth
        return self._ask_prefix[-1][1] if self._ask_prefix else Decimal("0")


@dataclass(frozen=True, slots=True)
class ExecutionEstimate:
    """Result of cost_to_acquire or proceeds_from_sell."""

    net_token_movement: dict[str, Decimal]  # asset_id -> signed amount
    avg_price: Decimal | None
    min_price: Decimal | None
    max_price: Decimal | None
    cost_breakdown: CostBreakdown
    used_levels: Sequence[DepthLevel]
    residual: Decimal = Decimal("0")
    capacity_info: "CapacityBounds | None" = None
    quality: str = "exact"
    post_state: "OrderBookSnapshot | None" = None
    warnings: list[str] = field(default_factory=list)


AMMQuality = Literal["exact", "indicative", "post_state_simulated", "response_time_only"]


@dataclass(frozen=True, slots=True)
class AMMState:
    """AMM pool state for simulation."""

    pool_id: str
    token0_asset_id: str
    token1_asset_id: str
    reserve0: Decimal
    reserve1: Decimal
    fee_bps: Decimal  # e.g. 0.30 = 30bps
    decimals0: int = 9
    decimals1: int = 6
    pool_type: Literal["cpmm", "clmm", "dlmm"] = "cpmm"
    tick_spacing: Decimal | None = None
    liquidity: Decimal | None = None
    sqrt_price: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CapacityBounds:
    """Known feasible region for a given market state."""

    min_executable_quantity: Decimal = Decimal("0")
    max_liquidity_available: Decimal = Decimal("0")
    sufficient_depth: bool = True


@dataclass
class ExecutionResult:
    """Complete execution result with status."""

    status: "ExecutionStatus"
    estimate: ExecutionEstimate | None = None
    error_reason: str | None = None
    min_acceptable_quantity: Decimal | None = None
    actual_quantity: Decimal = Decimal("0")

    @property
    def is_success(self) -> bool:
        return self.status == "executed"


ExecutionStatus = Literal[
    "executed",
    "partial",
    "insufficient_depth",
    "stale",
    "unsupported",
    "unknown",
]
