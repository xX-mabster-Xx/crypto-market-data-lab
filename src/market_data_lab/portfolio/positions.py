"""Position Manager — integrates PositionEngine with Ledger.

Section 12: POS-01–05 — entry facts, separate exit marks, funding/borrow
only when position exists and is eligible.
POS-04: Exit policy defined before evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping

from ..domain.accounting import BalanceType, CashFlow, CashFlowDirection, Ledger
from ..position_engine.engine import PositionEngine
from ..position_engine.contracts import (
    PositionId, PositionIntent, VirtualFill, ExitPolicy,
    PositionRecord, PositionState,
)
from ..carry.funding import FundingCalendar, FundingAccrual
from ..carry.borrow import BorrowModel


@dataclass
class PositionManager:
    """Manages position lifecycle integrated with ledger.

    POS-01: Entry fixes specific quantities and virtual fills.
    POS-02: Separate entry facts and exit marks.
    POS-03: Funding/borrow only when position exists and eligible.
    """

    engine: PositionEngine = field(default_factory=PositionEngine)
    ledger: Ledger = field(default_factory=Ledger)
    funding_calendar: FundingCalendar | None = None
    borrow_model: BorrowModel | None = None

    def open_position(
        self,
        intent: PositionIntent,
        entry_fills: dict[str, VirtualFill],
        effective_time_ns: int,
    ) -> PositionRecord:
        """Open a virtual position with entry fills.

        POS-01: Fixes specific quantities, not promises.
        """

        record = self.engine.propose(intent)

        for leg_id, fill in entry_fills.items():
            self.engine.open_leg(intent.position_id, leg_id, fill)

            # Record in ledger
            cashflow = CashFlow(
                asset_id=intent.legs[0].input_asset_id,  # simplified
                amount_native=Decimal(str(fill.filled_raw)),
                direction=CashFlowDirection.OUTFLOW,
                balance_type=BalanceType.POSITION_ENTRY,
                source="position_open",
                effective_time_ns=effective_time_ns,
                reference_id=str(intent.position_id),
            )
            self.ledger.apply(cashflow)

        return record

    def evaluate_exit(
        self,
        position_id: PositionId,
        exit_fills: dict[str, VirtualFill],
        effective_time_ns: int,
    ) -> Decimal:
        """Evaluate exit for a position.

        POS-02: Exit on actual entered quantity, not a new quote.
        T12: Open position q0, next quote gives q1 — exit evaluated on q0.
        """

        position = self.engine.position(position_id)
        if position is None:
            raise KeyError(position_id)

        # POS-03: Only if position exists and eligible
        if position.state not in {"Hedged", "Partial", "Closing"}:
            return Decimal("0")

        total_pnl = Decimal("0")
        for leg_id, fill in exit_fills.items():
            facts = position.leg(leg_id)
            if facts.entry is None:
                continue

            # Calculate realized PnL from entry to exit
            entry_value = Decimal(str(facts.entry.filled_raw)) * Decimal(str(facts.entry.price_raw))
            exit_value = Decimal(str(fill.filled_raw)) * Decimal(str(fill.price_raw))
            fees = Decimal(str(facts.entry.fee_raw)) + Decimal(str(fill.fee_raw))

            leg_pnl = exit_value - entry_value - fees
            total_pnl += leg_pnl

            # Record in ledger
            self.ledger.record_pnl(
                asset_id=fill.leg_id,
                amount=leg_pnl,
                is_realized=True,
                effective_time_ns=effective_time_ns,
                source="position_close",
                position_id=position_id,
            )

            # Mark exit in position engine
            self.engine.mark_exit(position_id, leg_id, fill)

        return total_pnl

    def apply_periodic_funding(
        self,
        position_id: PositionId,
        current_time_ns: int,
    ) -> Decimal:
        """Apply funding accrual to a position.

        POS-03: Funding only when position exists and eligible.
        FND-04: Idempotent — use cumulative index tracking.
        T08: Duplicated event -> single accrual.
        T19: Single funding ticker recalculated 1000x = 1 evidence.
        """
        if self.funding_calendar is None:
            return Decimal("0")

        position = self.engine.position(position_id)
        if position is None:
            return Decimal("0")

        if position.state not in {"Hedged", "Partial"}:
            return Decimal("0")

        # Check for new events since last application
        history = self.funding_calendar.accrual_history()
        already_applied_ids = set()

        for accrual in history:
            if accrual.rate.event_id:
                already_applied_ids.add(accrual.rate.event_id)

        # Only apply events not yet processed
        new_events = [
            e for e in self.funding_calendar._events  # noqa: protected-access
            if (not e.event_id) or e.event_id not in already_applied_ids
        ]

        total_funding = Decimal("0")
        for event in new_events:
            rate_fraction = event.as_fraction
            quantity = Decimal(str(position.intent.legs[0].requested_raw))
            amount = quantity * rate_fraction

            # FND-03: Use oracle price for oracle-reference shorts
            if event.reference_price_value and event.reference_price_value > 0:
                amount = quantity * event.reference_price_value * rate_fraction

            self.engine.apply_funding(position_id, int(amount))
            total_funding += amount

        return total_funding

    def close_position(
        self,
        position_id: PositionId,
        reason: str,
    ) -> PositionRecord | None:
        """Close a position and release reservations."""
        record = self.engine.record(position_id)
        if record is None:
            return None

        position = record.position
        current_state = position.state

        if current_state in {"Hedged", "Partial", "Closing"}:
            self.engine.close(position_id, reason)
        elif current_state in {"Hedged", "Partial", "Closing", "DataImpaired", "Unwinding"}:
            self.engine.unwind(position_id, reason)

        # Release reservations in ledger
        for asset_id, amount in position.reserved_balances.items():
            self.ledger.release_reservation(
                asset_id, amount, 0, f"release:{position_id}"
            )

        return record
