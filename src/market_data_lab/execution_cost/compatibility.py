"""Backward-compatible functions for legacy code."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

FeeCurrency = str


@dataclass
class ExecutionEstimate:
    avg_price: Decimal
    total_cost: Decimal
    consumed_size: Decimal
    levels_consumed: int
    min_price: Decimal | None
    max_price: Decimal | None
    post_state: object | None = None


def cost_to_acquire(
    net_quantity, depth_levels, fee_bps=Decimal("0"),
    fee_currency="quote", base_currency="BASE", quote_currency="QUOTE",
) -> ExecutionEstimate:
    remaining = net_quantity
    total_cost = Decimal("0")
    total_consumed = Decimal("0")
    levels_consumed = 0
    min_price = None
    max_price = None
    for price, size in sorted(depth_levels, key=lambda x: x[0]):
        if remaining <= 0:
            break
        consumed = min(size, remaining)
        cost = consumed * price
        total_cost += cost
        total_consumed += consumed
        levels_consumed += 1
        remaining -= consumed
        if min_price is None or price < min_price:
            min_price = price
        if max_price is None or price > max_price:
            max_price = price
    if fee_currency == "quote":
        total_cost += total_cost * (fee_bps / Decimal("10000"))
    avg_price = total_cost / total_consumed if total_consumed > 0 else Decimal("0")
    return ExecutionEstimate(
        avg_price=avg_price,
        total_cost=total_cost,
        consumed_size=total_consumed,
        levels_consumed=levels_consumed,
        min_price=min_price,
        max_price=max_price,
    )


def proceeds_from_sell(
    gross_quantity, depth_levels, fee_bps=Decimal("0"), fee_currency="quote",
) -> ExecutionEstimate:
    remaining = gross_quantity
    total_proceeds = Decimal("0")
    total_sold = Decimal("0")
    levels_consumed = 0
    min_price = None
    max_price = None
    for price, size in sorted(depth_levels, key=lambda x: -x[0]):
        if remaining <= 0:
            break
        sold = min(size, remaining)
        proceeds = sold * price
        total_proceeds += proceeds
        total_sold += sold
        levels_consumed += 1
        remaining -= sold
        if min_price is None or price < min_price:
            min_price = price
        if max_price is None or price > max_price:
            max_price = price
    if fee_currency == "quote":
        total_proceeds -= total_proceeds * (fee_bps / Decimal("10000"))
    avg_price = total_proceeds / total_sold if total_sold > 0 else Decimal("0")
    return ExecutionEstimate(
        avg_price=avg_price,
        total_cost=-total_proceeds,
        consumed_size=total_sold,
        levels_consumed=levels_consumed,
        min_price=min_price,
        max_price=max_price,
    )
