"""L2 order book cost calculations.

Section 8.2: BBO for screening, depth for exact quantities.
If only BBO is available and quantity exceeds first level -> insufficient_known_depth.
Book recovery follows venue protocol: snapshot + deltas, sequence/checksum check.
"""

from __future__ import annotations

from decimal import Decimal
from dataclasses import dataclass
from typing import Sequence

from .contracts import DepthLevel, ExecutionEstimate, ExecutionResult, OrderBookSnapshot, CapacityBounds
from ..cost_breakdown.contracts import CostBreakdown, CostRecord, FeeTier
from ..cost_breakdown.calculator import aggregate_costs


def depth_cost_to_acquire(
    book: OrderBookSnapshot,
    net_quantity: Decimal,
    fee_tier: FeeTier | None = None,
    quote_asset_id: str = "USDT",
) -> ExecutionResult:
    """Calculate total cost to acquire net_quantity from asks.

    Per Section 8.2: Sum cost of consumed levels until target quantity reached.
    If BBO only and quantity exceeds available known depth -> insufficient_known_depth.
    """

    if book.asks is None or len(book.asks) == 0:
        return ExecutionResult(
            status="unknown",
            error_reason="no ask levels available",
            min_acceptable_quantity=net_quantity,
        )

    used_levels: list[DepthLevel] = []
    remaining = net_quantity
    total_cost = Decimal("0")
    total_consumed = Decimal("0")

    # Sort asks by price ascending
    sorted_asks = sorted(book.asks, key=lambda x: x.price)

    for level in sorted_asks:
        if remaining <= 0:
            break
        consumed = min(level.size, remaining)
        cost = consumed * level.price
        total_cost += cost
        total_consumed += consumed
        used_levels.append(DepthLevel(
            price=level.price, size=consumed,
            cum_size=total_consumed, cum_value=total_cost,
        ))
        remaining -= consumed

    if remaining > 0:
        # Not enough depth — do NOT extrapolate (Section 8.2)
        capacity = CapacityBounds(
            max_liquidity_available=total_consumed,
            sufficient_depth=False,
            min_executable_quantity=Decimal("0"),
        )
        return ExecutionResult(
            status="insufficient_depth",
            estimate=ExecutionEstimate(
                net_token_movement={quote_asset_id: -total_cost},
                avg_price=total_cost / total_consumed if total_consumed > 0 else Decimal("0"),
                min_price=used_levels[-1].price if used_levels else None,
                max_price=used_levels[0].price if used_levels else None,
                cost_breakdown=aggregate_costs("depth", _fee_records(fee_tier, total_cost, quote_asset_id)),
                used_levels=used_levels,
                capacity_info=capacity,
                quality="exact",
            ),
            error_reason="insufficient_known_depth",
            min_acceptable_quantity=remaining,
            actual_quantity=total_consumed,
        )

    # Add trading fee
    fee_amount = Decimal("0")
    records = []
    if fee_tier:
        fee_amount = total_cost * (fee_tier.taker_bps / Decimal("10000"))
        records.append(CostRecord(
            record_id="depth_fee",
            category="trading_fee",
            native_amount=fee_amount,
            native_currency=quote_asset_id,
            price_conversion=Decimal("1"),
            quote_currency=quote_asset_id,
            source=fee_tier.source,
            included_in_quote=False,
            quality="verified" if fee_tier.source == "public" else "estimated",
            fee_tier=fee_tier,
        ))

    breakdown = aggregate_costs("depth", _fee_records(fee_tier, fee_amount, quote_asset_id))

    return ExecutionResult(
        status="executed",
        estimate=ExecutionEstimate(
            net_token_movement={quote_asset_id: -(total_cost + fee_amount)},
            avg_price=total_cost / total_consumed if total_consumed > 0 else Decimal("0"),
            min_price=used_levels[-1].price if used_levels else None,
            max_price=used_levels[0].price if used_levels else None,
            cost_breakdown=breakdown,
            used_levels=used_levels,
            residual=Decimal("0"),
            capacity_info=CapacityBounds(
                max_liquidity_available=total_consumed,
                sufficient_depth=True,
                min_executable_quantity=net_quantity,
            ),
            quality="exact",
        ),
        actual_quantity=total_consumed,
    )


def depth_proceeds_from_sell(
    book: OrderBookSnapshot,
    gross_quantity: Decimal,
    fee_tier: FeeTier | None = None,
    quote_asset_id: str = "USDT",
) -> ExecutionResult:
    """Calculate proceeds from selling gross_quantity into bids."""

    if book.bids is None or len(book.bids) == 0:
        return ExecutionResult(
            status="unknown",
            error_reason="no bid levels available",
            min_acceptable_quantity=gross_quantity,
        )

    used_levels: list[DepthLevel] = []
    remaining = gross_quantity
    total_proceeds = Decimal("0")
    total_sold = Decimal("0")

    sorted_bids = sorted(book.bids, key=lambda x: -x.price)

    for level in sorted_bids:
        if remaining <= 0:
            break
        sold = min(level.size, remaining)
        proceeds = sold * level.price
        total_proceeds += proceeds
        total_sold += sold
        used_levels.append(DepthLevel(
            price=level.price, size=sold,
            cum_size=total_sold, cum_value=total_proceeds,
        ))
        remaining -= sold

    if remaining > 0:
        capacity = CapacityBounds(
            max_liquidity_available=total_sold,
            sufficient_depth=False,
            min_executable_quantity=Decimal("0"),
        )
        return ExecutionResult(
            status="insufficient_depth",
            estimate=ExecutionEstimate(
                net_token_movement={quote_asset_id: total_proceeds},
                avg_price=total_proceeds / total_sold if total_sold > 0 else Decimal("0"),
                min_price=used_levels[-1].price if used_levels else None,
                max_price=used_levels[0].price if used_levels else None,
                cost_breakdown=aggregate_costs("depth_sell", _fee_records(fee_tier, Decimal("0"), quote_asset_id)),
                used_levels=used_levels,
                capacity_info=capacity,
                quality="exact",
            ),
            error_reason="insufficient_depth",
            min_acceptable_quantity=remaining,
            actual_quantity=total_sold,
        )

    # Fee deducted from proceeds
    fee_amount = Decimal("0")
    if fee_tier:
        fee_amount = total_proceeds * (fee_tier.taker_bps / Decimal("10000"))

    net_proceeds = total_proceeds - fee_amount
    breakdown = aggregate_costs("depth_sell", _fee_records(fee_tier, fee_amount, quote_asset_id))

    return ExecutionResult(
        status="executed",
        estimate=ExecutionEstimate(
            net_token_movement={quote_asset_id: net_proceeds},
            avg_price=total_proceeds / total_sold if total_sold > 0 else Decimal("0"),
            min_price=used_levels[-1].price if used_levels else None,
            max_price=used_levels[0].price if used_levels else None,
            cost_breakdown=breakdown,
            used_levels=used_levels,
            residual=Decimal("0"),
            capacity_info=CapacityBounds(
                max_liquidity_available=total_sold,
                sufficient_depth=True,
                min_executable_quantity=gross_quantity,
            ),
            quality="exact",
        ),
        actual_quantity=total_sold,
    )


def apply_virtual_fill_to_book(
    book: OrderBookSnapshot,
    consumed_levels: Sequence[DepthLevel],
    traded_size: Decimal,
) -> OrderBookSnapshot:
    """Return a new book snapshot with virtual fills applied (Section 8.4).

    Per EXE-02: subsequent operations on the same pool/level use the
    modified state, not the original.
    """
    # Copy the book and remove consumed depths
    new_bids = list(book.bids)
    new_asks = list(book.asks)

    # Reduce ask sizes based on consumed levels
    for cl in consumed_levels:
        for i, ask in enumerate(new_asks):
            if ask.price == cl.price:
                remaining_size = ask.size - cl.size
                if remaining_size > 0:
                    new_asks[i] = DepthLevel(
                        price=ask.price,
                        size=remaining_size,
                        cum_size=Decimal("0"),
                        cum_value=Decimal("0"),
                    )
                else:
                    new_asks.pop(i)
                break

    return OrderBookSnapshot(
        instrument_id=book.instrument_id,
        venue_id=book.venue_id,
        bids=new_bids,
        asks=new_asks,
        timestamp_ns=book.timestamp_ns,
        version=book.version + 1,
        sequence=book.sequence,
        checksum=book.checksum,
    )


def _fee_records(
    fee_tier: FeeTier | None,
    fee_amount: Decimal,
    currency: str,
) -> list:
    from ..cost_breakdown.calculator import calc_trading_fee
    if fee_amount == 0:
        return []
    return [calc_trading_fee("depth_fee", Decimal("1"), Decimal("1"), fee_tier,
                             quality="verified" if fee_tier and fee_tier.source == "public" else "estimated")]


@dataclass
class L2OrderBook:
    """Wrapper around OrderBookSnapshot with validation.

    Section 8.2: Book recovery follows venue protocol:
    snapshot + deltas, sequence/checksum validation, resnapshot after gap.
    """

    snapshot: OrderBookSnapshot
    _validated: bool = False
    _last_sequence: int | None = None

    def validate_sequence(self, prev_sequence: int | None) -> bool:
        """Validate that this snapshot/delta follows the expected sequence."""
        if not self.snapshot.sequence:
            return True
        if prev_sequence is None:
            self._last_sequence = self.snapshot.sequence
            return True
        expected = prev_sequence + 1
        if self.snapshot.sequence != expected:
            return False
        self._last_sequence = self.snapshot.sequence
        return True

    def validate_checksum(self, expected_checksum: str | None) -> bool:
        """Validate book checksum against exchange-provided value."""
        if expected_checksum is None:
            return True
        if not self.snapshot.checksum:
            return False
        return self.snapshot.checksum == expected_checksum

    def needs_resync(self) -> bool:
        """Whether this book requires resync from venue."""
        return self.snapshot.is_crossed() if hasattr(self.snapshot, 'is_crossed') else False

    @property
    def best_bid(self):
        return self.snapshot.best_bid

    @property
    def best_ask(self):
        return self.snapshot.best_ask

    @property
    def spread(self):
        return self.snapshot.spread
