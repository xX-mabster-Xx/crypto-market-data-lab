"""Typed, conservative execution-cost primitives for visible CEX depth.

The functions in this module are pure: they do not fetch market data, place
orders, or mutate a shared book.  A caller supplies one immutable side of an
already validated L2 snapshot and receives an explicit estimate.  Missing
depth remains ``insufficient_known_depth``; it is never extrapolated from the
last visible price.

Two fee locations are modelled because they change the quantity that must be
walked:

* a quote-currency buy fee increases quote cash paid; and
* a base-currency buy fee requires buying more gross base to receive the
  requested net base quantity.

The latter distinction is the important T02 invariant: replacing
``cost(q) / (1 - fee)`` with a walk of ``q / (1 - fee)`` is necessary when
the gross quantity crosses another order-book level.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal


BookSide = Literal["asks", "bids"]
FeeCurrency = Literal["base", "quote"]
ExecutionAction = Literal["acquire_base", "sell_base"]
ExecutionStatus = Literal["ok", "insufficient_known_depth", "book_invalid"]
Level = tuple[Decimal, Decimal]


def _decimal_text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _positive_finite(value: Decimal) -> bool:
    return value.is_finite() and value > 0


def _validate_request(quantity: Decimal, fee_bps: Decimal, fee_currency: str) -> None:
    if not _positive_finite(quantity):
        raise ValueError("base quantity must be finite and positive")
    if not fee_bps.is_finite() or fee_bps < 0 or fee_bps >= Decimal(10_000):
        raise ValueError("fee_bps must be finite, non-negative, and below 10000")
    if fee_currency not in {"base", "quote"}:
        raise ValueError("fee_currency must be 'base' or 'quote'")


def _book_invalid_reason(levels: Sequence[Level], *, side: BookSide) -> str | None:
    previous_price: Decimal | None = None
    for index, level in enumerate(levels):
        if not isinstance(level, tuple) or len(level) != 2:
            return f"malformed_{side}_level_at_index_{index}"
        price, quantity = level
        if not isinstance(price, Decimal) or not isinstance(quantity, Decimal):
            return f"non_decimal_{side}_level_at_index_{index}"
        if not _positive_finite(price) or not _positive_finite(quantity):
            return f"non_positive_or_non_finite_{side}_level_at_index_{index}"
        if previous_price is not None:
            if side == "asks" and price < previous_price:
                return f"unordered_{side}_level_at_index_{index}"
            if side == "bids" and price > previous_price:
                return f"unordered_{side}_level_at_index_{index}"
        previous_price = price
    return None


@dataclass(frozen=True, slots=True)
class CostComponent:
    """One native-currency component of an execution estimate."""

    cost_type: str
    amount: Decimal
    currency: str
    source: str
    quality: str
    included_in_book_quote: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "type": self.cost_type,
            "native_amount": _decimal_text(self.amount),
            "native_currency": self.currency,
            "source": self.source,
            "quality": self.quality,
            "included_in_book_quote": self.included_in_book_quote,
        }


@dataclass(frozen=True, slots=True)
class ExecutionEstimate:
    """Finite-size result against one known order-book side.

    ``net_base_movement`` and ``net_quote_movement`` are signed from the
    strategy's perspective.  The exact book quantity is retained separately
    so fee replacement cannot be mistaken for free or hidden base.
    """

    action: ExecutionAction
    status: ExecutionStatus
    reason: str | None
    base_currency: str
    quote_currency: str
    requested_base_quantity: Decimal
    requested_book_base_quantity: Decimal
    filled_book_base_quantity: Decimal
    unfilled_book_base_quantity: Decimal
    known_book_capacity_base: Decimal
    gross_book_quote_amount: Decimal
    net_base_movement: Decimal
    net_quote_movement: Decimal
    average_price: Decimal | None
    marginal_price: Decimal | None
    levels_consumed: int
    fee_bps: Decimal
    fee_currency: FeeCurrency
    costs: tuple[CostComponent, ...]
    state_version: str | None = None

    @property
    def complete(self) -> bool:
        return self.status == "ok" and self.unfilled_book_base_quantity == 0

    @property
    def fee_amount(self) -> Decimal:
        return self.costs[0].amount if self.costs else Decimal(0)

    def as_dict(self) -> dict[str, object]:
        market_evidence = {
            "ok": "depth_checked",
            "insufficient_known_depth": "partial_depth",
            "book_invalid": "invalid",
        }[self.status]
        return {
            "schema_version": 1,
            "action": self.action,
            "status": self.status,
            "reason": self.reason,
            "complete": self.complete,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "requested_base_quantity": _decimal_text(self.requested_base_quantity),
            "requested_book_base_quantity": _decimal_text(
                self.requested_book_base_quantity
            ),
            "filled_book_base_quantity": _decimal_text(self.filled_book_base_quantity),
            "unfilled_book_base_quantity": _decimal_text(
                self.unfilled_book_base_quantity
            ),
            "known_book_capacity_base": _decimal_text(self.known_book_capacity_base),
            "gross_book_quote_amount": _decimal_text(self.gross_book_quote_amount),
            "net_base_movement": _decimal_text(self.net_base_movement),
            "net_quote_movement": _decimal_text(self.net_quote_movement),
            "average_price": _decimal_text(self.average_price),
            "marginal_price": _decimal_text(self.marginal_price),
            "levels_consumed": self.levels_consumed,
            "fee_bps": _decimal_text(self.fee_bps),
            "fee_currency": self.fee_currency,
            "costs": [item.as_dict() for item in self.costs],
            "state_version": self.state_version,
            "market_evidence": market_evidence,
        }


@dataclass(frozen=True, slots=True)
class _WalkResult:
    status: ExecutionStatus
    reason: str | None
    requested_base: Decimal
    filled_base: Decimal
    unfilled_base: Decimal
    known_capacity_base: Decimal
    gross_quote: Decimal
    average_price: Decimal | None
    marginal_price: Decimal | None
    levels_consumed: int


def _walk_base_quantity(
    levels: Sequence[Level],
    target_base_quantity: Decimal,
    *,
    side: BookSide,
) -> _WalkResult:
    invalid_reason = _book_invalid_reason(levels, side=side)
    if invalid_reason is not None:
        return _WalkResult(
            status="book_invalid",
            reason=invalid_reason,
            requested_base=target_base_quantity,
            filled_base=Decimal(0),
            unfilled_base=target_base_quantity,
            known_capacity_base=Decimal(0),
            gross_quote=Decimal(0),
            average_price=None,
            marginal_price=None,
            levels_consumed=0,
        )
    known_capacity = sum((quantity for _, quantity in levels), Decimal(0))

    remaining = target_base_quantity
    filled = Decimal(0)
    gross_quote = Decimal(0)
    marginal_price: Decimal | None = None
    levels_consumed = 0
    for price, available_base in levels:
        if remaining == 0:
            break
        take = min(remaining, available_base)
        if take == 0:
            continue
        filled += take
        gross_quote += take * price
        remaining -= take
        marginal_price = price
        levels_consumed += 1

    complete = remaining == 0
    return _WalkResult(
        status="ok" if complete else "insufficient_known_depth",
        reason=None if complete else "insufficient_known_depth",
        requested_base=target_base_quantity,
        filled_base=filled,
        unfilled_base=remaining,
        known_capacity_base=known_capacity,
        gross_quote=gross_quote,
        average_price=(gross_quote / filled if filled > 0 else None),
        marginal_price=marginal_price,
        levels_consumed=levels_consumed,
    )


def cost_to_acquire(
    net_base_quantity: Decimal,
    asks: Sequence[Level],
    *,
    fee_bps: Decimal,
    fee_currency: FeeCurrency,
    base_currency: str,
    quote_currency: str,
    fee_source: str,
    fee_quality: str,
    state_version: str | None = None,
) -> ExecutionEstimate:
    """Return quote cash needed to acquire an exact *net* base quantity."""

    _validate_request(net_base_quantity, fee_bps, fee_currency)
    fee_fraction = fee_bps / Decimal(10_000)
    requested_book_quantity = (
        net_base_quantity / (Decimal(1) - fee_fraction)
        if fee_currency == "base"
        else net_base_quantity
    )
    walked = _walk_base_quantity(asks, requested_book_quantity, side="asks")

    if fee_currency == "base":
        fee_amount = walked.filled_base * fee_fraction
        net_base_movement = walked.filled_base - fee_amount
        net_quote_movement = -walked.gross_quote
        fee_native_currency = base_currency
    else:
        fee_amount = walked.gross_quote * fee_fraction
        net_base_movement = walked.filled_base
        net_quote_movement = -(walked.gross_quote + fee_amount)
        fee_native_currency = quote_currency
    costs = (
        CostComponent(
            cost_type="trading_fee",
            amount=fee_amount,
            currency=fee_native_currency,
            source=fee_source,
            quality=fee_quality,
            included_in_book_quote=False,
        ),
    )
    return ExecutionEstimate(
        action="acquire_base",
        status=walked.status,
        reason=walked.reason,
        base_currency=base_currency,
        quote_currency=quote_currency,
        requested_base_quantity=net_base_quantity,
        requested_book_base_quantity=requested_book_quantity,
        filled_book_base_quantity=walked.filled_base,
        unfilled_book_base_quantity=walked.unfilled_base,
        known_book_capacity_base=walked.known_capacity_base,
        gross_book_quote_amount=walked.gross_quote,
        net_base_movement=net_base_movement,
        net_quote_movement=net_quote_movement,
        average_price=walked.average_price,
        marginal_price=walked.marginal_price,
        levels_consumed=walked.levels_consumed,
        fee_bps=fee_bps,
        fee_currency=fee_currency,
        costs=costs,
        state_version=state_version,
    )


def proceeds_from_sell(
    gross_base_quantity: Decimal,
    bids: Sequence[Level],
    *,
    fee_bps: Decimal,
    fee_currency: FeeCurrency,
    base_currency: str,
    quote_currency: str,
    fee_source: str,
    fee_quality: str,
    state_version: str | None = None,
) -> ExecutionEstimate:
    """Return proceeds from selling an exact gross book quantity of base."""

    _validate_request(gross_base_quantity, fee_bps, fee_currency)
    fee_fraction = fee_bps / Decimal(10_000)
    walked = _walk_base_quantity(bids, gross_base_quantity, side="bids")
    if fee_currency == "quote":
        fee_amount = walked.gross_quote * fee_fraction
        net_base_movement = -walked.filled_base
        net_quote_movement = walked.gross_quote - fee_amount
        fee_native_currency = quote_currency
    else:
        fee_amount = walked.filled_base * fee_fraction
        net_base_movement = -(walked.filled_base + fee_amount)
        net_quote_movement = walked.gross_quote
        fee_native_currency = base_currency
    costs = (
        CostComponent(
            cost_type="trading_fee",
            amount=fee_amount,
            currency=fee_native_currency,
            source=fee_source,
            quality=fee_quality,
            included_in_book_quote=False,
        ),
    )
    return ExecutionEstimate(
        action="sell_base",
        status=walked.status,
        reason=walked.reason,
        base_currency=base_currency,
        quote_currency=quote_currency,
        requested_base_quantity=gross_base_quantity,
        requested_book_base_quantity=gross_base_quantity,
        filled_book_base_quantity=walked.filled_base,
        unfilled_book_base_quantity=walked.unfilled_base,
        known_book_capacity_base=walked.known_capacity_base,
        gross_book_quote_amount=walked.gross_quote,
        net_base_movement=net_base_movement,
        net_quote_movement=net_quote_movement,
        average_price=walked.average_price,
        marginal_price=walked.marginal_price,
        levels_consumed=walked.levels_consumed,
        fee_bps=fee_bps,
        fee_currency=fee_currency,
        costs=costs,
        state_version=state_version,
    )
