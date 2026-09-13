"""Ledger accounting — source of truth for money, assets, liabilities.

Section 4: ACC-01 ledger is the source of truth.
ACC-02 all amounts in native units; Decimal for prices/limits, no float.
ACC-03 rates stored with explicit units.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from ..position_engine.contracts import PositionId


class BalanceType(Enum):
    FREE = "free_funds"
    RESERVED = "reserved_funds"
    COLLATERAL = "collateral"
    SPOT = "spot"
    LIABILITY = "liability"
    POSITION_ENTRY = "position_entry_cost_basis"
    TRADING_FEE = "trading_fee"
    FUNDING = "funding"
    BORROW_INTEREST = "borrow_interest"
    GAS = "gas"
    TRANSFER_FEE = "transfer_fee"
    SETTLEABLE_PNL = "settleable_pnl"
    UNCLAIMED_PNL = "unclaimed_pnl"
    WITHDRAWABLE_PNL = "withdrawable_pnl"


class CashFlowDirection(Enum):
    INFLOW = "inflow"
    OUTFLOW = "outflow"


@dataclass(frozen=True, slots=True)
class CashFlow:
    """A single cash flow entry.

    Per ACC-02: amount in native units, Decimal, never float.
    Per ACC-03: rates include units (e.g. bps, fraction).
    """

    asset_id: str
    amount_native: Decimal
    direction: CashFlowDirection
    balance_type: BalanceType
    source: str
    effective_time_ns: int
    reference_id: str = ""

    @property
    def signed_amount(self) -> Decimal:
        if self.direction == CashFlowDirection.INFLOW:
            return self.amount_native
        return -self.amount_native


@dataclass
class LedgerEntry:
    """A single ledger entry after aggregation."""

    asset_id: str
    balance_type: BalanceType
    amount_native: Decimal
    updated_at_ns: int
    source: str = ""
    transaction_id: str = ""


@dataclass
class Ledger:
    """Ledger tracking money, assets, liabilities, and positions.

    Per ACC-01, every aggregated formula must converge with the ledger
    on the same source data.
    """

    entries: dict[tuple[str, BalanceType], Decimal] = field(default_factory=dict)
    _history: list[LedgerEntry] = field(default_factory=list)
    _tx_counter: int = 0

    def _key(self, asset_id: str, balance_type: BalanceType) -> tuple[str, BalanceType]:
        return (asset_id, balance_type)

    def get_balance(self, asset_id: str, balance_type: BalanceType) -> Decimal:
        return self.entries.get(self._key(asset_id, balance_type), Decimal("0"))

    def get_total_for_asset(self, asset_id: str) -> Decimal:
        """Sum all entries for an asset (net position)."""
        total = Decimal("0")
        for (aid, btype), amount in self.entries.items():
            if aid == asset_id:
                total += amount
        return total

    def apply(self, flow: CashFlow) -> LedgerEntry:
        """Apply a cash flow and return the new ledger entry."""
        key = self._key(flow.asset_id, flow.balance_type)
        current = self.entries.get(key, Decimal("0"))
        new_amount = current + flow.signed_amount
        self.entries[key] = new_amount

        self._tx_counter += 1
        entry = LedgerEntry(
            asset_id=flow.asset_id,
            balance_type=flow.balance_type,
            amount_native=new_amount,
            updated_at_ns=flow.effective_time_ns,
            source=flow.source,
            transaction_id=f"tx-{self._tx_counter:08d}",
        )
        self._history.append(entry)
        return entry

    def transfer(
        self,
        asset_id: str,
        from_type: BalanceType,
        to_type: BalanceType,
        amount: Decimal,
        effective_time_ns: int,
        source: str,
    ) -> list[LedgerEntry]:
        """Transfer between balance types.

        Per Section 4.1: moving money between free funds and collateral
        is NOT an expense or income.
        """
        if amount <= 0:
            return []
        outflow = self.apply(
            CashFlow(
                asset_id=asset_id,
                amount_native=amount,
                direction=CashFlowDirection.OUTFLOW,
                balance_type=from_type,
                source=source,
                effective_time_ns=effective_time_ns,
            )
        )
        inflow = self.apply(
            CashFlow(
                asset_id=asset_id,
                amount_native=amount,
                direction=CashFlowDirection.INFLOW,
                balance_type=to_type,
                source=source,
                effective_time_ns=effective_time_ns,
            )
        )
        return [outflow, inflow]

    def reserve_funds(
        self,
        asset_id: str,
        amount: Decimal,
        effective_time_ns: int,
        source: str,
    ) -> LedgerEntry:
        """Move from free funds to reserved (for a position)."""
        return self.transfer(
            asset_id=asset_id,
            from_type=BalanceType.FREE,
            to_type=BalanceType.RESERVED,
            amount=amount,
            effective_time_ns=effective_time_ns,
            source=source,
        )[0]

    def release_reservation(
        self,
        asset_id: str,
        amount: Decimal,
        effective_time_ns: int,
        source: str,
    ) -> LedgerEntry:
        """Move from reserved back to free funds."""
        return self.transfer(
            asset_id=asset_id,
            from_type=BalanceType.RESERVED,
            to_type=BalanceType.FREE,
            amount=amount,
            effective_time_ns=effective_time_ns,
            source=source,
        )[0]

    def record_pnl(
        self,
        asset_id: str,
        amount: Decimal,
        is_realized: bool,
        effective_time_ns: int,
        source: str,
        position_id: PositionId | None = None,
    ) -> LedgerEntry:
        """Record realized or unrealized PnL."""
        balance_type = (
            BalanceType.SETTLEABLE_PNL if is_realized else BalanceType.UNCLAIMED_PNL
        )
        return self.apply(
            CashFlow(
                asset_id=asset_id,
                amount_native=amount,
                direction=CashFlowDirection.INFLOW if amount >= 0 else CashFlowDirection.OUTFLOW,
                balance_type=balance_type,
                source=source,
                effective_time_ns=effective_time_ns,
                reference_id=str(position_id) if position_id else "",
            )
        )

    def snapshot(self) -> list[LedgerEntry]:
        """Return current state of all ledger entries."""
        result: list[LedgerEntry] = []
        now = int(time.monotonic_ns())
        for (asset_id, btype), amount in self.entries.items():
            entry = LedgerEntry(
                asset_id=asset_id,
                balance_type=btype,
                amount_native=amount,
                updated_at_ns=now,
                source="snapshot",
            )
            result.append(entry)
        return result
