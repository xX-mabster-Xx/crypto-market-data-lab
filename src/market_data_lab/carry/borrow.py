"""Borrow model for short funding and reverse-spot strategies.

Section 5.4: Stores available size, haircut, borrow rate,
min period, revocability, account limit, and fees.

FND-04: Borrow interest in base increases repay quantity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from ..domain.instruments import InstrumentCapability


@dataclass(frozen=True, slots=True)
class BorrowCapacity:
    """Confirmed borrow capacity for an asset."""

    asset_id: str
    available_amount: Decimal
    haircut: Decimal  # e.g. 0.95 = 95% LTV
    rate_fraction: Decimal  # per second
    min_borrow_period_ns: int
    is_revocable: bool
    account_limit: Decimal | None
    source: str
    verified: bool


@dataclass
class BorrowPosition:
    """An active borrow position."""

    asset_id: str
    principal: Decimal
    interest_accrued: Decimal
    borrow_rate: Decimal  # per second
    opened_at_ns: int
    interest_asset_id: str  # interest paid in which asset
    fees_paid: Decimal
    is_settled: bool = False
    settled_at_ns: int | None = None

    @property
    def repayment_required(self) -> Decimal:
        """Total to repay (principal + interest).

        T16: Interest in base increases repay quantity.
        """
        return self.principal + self.interest_accrued

    def accrue_interest(self, elapsed_seconds: Decimal) -> Decimal:
        """Accrue interest over elapsed time."""
        interest = self.principal * self.borrow_rate * elapsed_seconds
        self.interest_accrued += interest
        return interest


@dataclass
class BorrowModel:
    """Manages borrow positions and capacity checks.

    Section 5.4: Public rate without confirmed availability =
    borrow_capacity_unknown.
    """

    _capacity: dict[str, BorrowCapacity] = field(default_factory=dict)
    _positions: dict[str, BorrowPosition] = field(default_factory=dict)
    _tx_counter: int = 0

    def register_capacity(self, capacity: BorrowCapacity) -> None:
        self._capacity[capacity.asset_id] = capacity

    def get_capacity(self, asset_id: str) -> BorrowCapacity | None:
        return self._capacity.get(asset_id)

    def can_borrow(self, asset_id: str, amount: Decimal) -> bool:
        """Check if we can borrow the given amount.

        T15: If borrow unavailable → borrow_capacity_unknown.
        """
        cap = self.get_capacity(asset_id)
        if cap is None or not cap.verified:
            return False
        if amount > cap.available_amount * cap.haircut:
            return False
        return True

    def borrow(self, asset_id: str, amount: Decimal, opened_at_ns: int) -> BorrowPosition | None:
        """Open a borrow position. Returns None if capacity unavailable."""
        if not self.can_borrow(asset_id, amount):
            return None

        cap = self._capacity[asset_id]
        self._tx_counter += 1

        position = BorrowPosition(
            asset_id=asset_id,
            principal=amount,
            interest_accrued=Decimal("0"),
            borrow_rate=cap.rate_fraction,
            opened_at_ns=opened_at_ns,
            interest_asset_id=asset_id,  # interest in base asset
            fees_paid=Decimal("0"),
        )
        self._positions[f"borrow-{self._tx_counter}"] = position

        # Reduce available capacity
        self._capacity[asset_id] = BorrowCapacity(
            asset_id=asset_id,
            available_amount=cap.available_amount - amount,
            haircut=cap.haircut,
            rate_fraction=cap.rate_fraction,
            min_borrow_period_ns=cap.min_borrow_period_ns,
            is_revocable=cap.is_revocable,
            account_limit=cap.account_limit,
            source=cap.source,
            verified=cap.verified,
        )

        return position

    def repay(self, position_id: str, repayment_amount: Decimal, settled_at_ns: int) -> Decimal:
        """Repay a borrow position.

        T16: Principal is not a loss; interest and fees are expense.
        """
        position = self._positions.get(position_id)
        if position is None or position.is_settled:
            return Decimal("0")

        remaining = repayment_amount
        # Interest paid first
        interest_paid = min(position.interest_accrued, remaining)
        position.interest_accrued -= interest_paid
        remaining -= interest_paid

        principal_paid = min(position.principal, remaining)
        position.principal -= principal_paid

        if position.principal <= 0:
            position.is_settled = True
            position.settled_at_ns = settled_at_ns

        # Return unused capacity
        if position.is_settled and position.principal >= 0:
            cap = self._capacity.get(position.asset_id)
            if cap:
                self._capacity[position.asset_id] = BorrowCapacity(
                    asset_id=cap.asset_id,
                    available_amount=cap.available_amount + position.principal,
                    haircut=cap.haircut,
                    rate_fraction=cap.rate_fraction,
                    min_borrow_period_ns=cap.min_borrow_period_ns,
                    is_revocable=cap.is_revocable,
                    account_limit=cap.account_limit,
                    source=cap.source,
                    verified=cap.verified,
                )

        return repayment_amount

    def get_open_positions(self) -> list[BorrowPosition]:
        return [p for p in self._positions.values() if not p.is_settled]

    def get_all_positions(self) -> list[BorrowPosition]:
        return list(self._positions.values())

    def capacity_status(self, asset_id: str) -> Literal["available", "unknown", "none"]:
        """Per Section 5.4: public rate without confirmed availability = unknown."""
        cap = self.get_capacity(asset_id)
        if cap is None:
            return "none"
        if not cap.verified:
            return "unknown"
        if cap.available_amount > 0:
            return "available"
        return "none"
