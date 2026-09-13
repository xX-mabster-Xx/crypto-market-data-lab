"""Resource management for virtual portfolio.

Section 13.2: RISK-01 — consider funds by location/account segment,
used by other virtual plans, collateral w/ haircut, max position/OI/risk
tiers, min/max order size, borrow capacity, gas asset, settlement restrictions.

Section 10.6: Resource conflicts between candidates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping, Sequence

from ..domain.accounting import BalanceType, Ledger
from ..carry.borrow import BorrowModel


@dataclass(frozen=True, slots=True)
class ResourceReservation:
    """Reserved capital for a position."""

    position_id: str
    asset_id: str
    amount: Decimal
    balance_type: BalanceType
    reserved_at_ns: int


@dataclass
class ResourceConflict:
    """Detected resource conflict between candidates."""

    position_a: str
    position_b: str
    asset_id: str
    required: Decimal
    available: Decimal
    resolution: str  # "winner_a" | "winner_b" | "split" | "deferred"


@dataclass
class ResourceManager:
    """Manages resource reservations and conflicts for virtual portfolio.

    RISK-01: Available funds by location/account segment.
    Section 10.6: Greedy allocation by verified ranking.
    """

    ledger: Ledger = field(default_factory=Ledger)
    borrow_model: BorrowModel | None = None
    _reservations: dict[str, list[ResourceReservation]] = field(default_factory=dict)
    _conflicts: list[ResourceConflict] = field(default_factory=list)

    def set_balance(self, asset_id: str, amount: Decimal, balance_type: BalanceType = BalanceType.FREE) -> None:
        """Set initial portfolio balance."""
        from ..domain.accounting import CashFlow, CashFlowDirection
        flow = CashFlow(
            asset_id=asset_id,
            amount_native=amount,
            direction=CashFlowDirection.INFLOW,
            balance_type=balance_type,
            source="initial_config",
            effective_time_ns=0,
        )
        self.ledger.apply(flow)

    def available(self, asset_id: str, balance_type: BalanceType = BalanceType.FREE) -> Decimal:
        """Check available funds (free minus reservations)."""
        total = self.ledger.get_balance(asset_id, balance_type)
        reserved = sum(
            r.amount for r in self._reservations.get(asset_id, [])
        )
        return total - reserved

    def try_reserve(
        self,
        position_id: str,
        asset_id: str,
        amount: Decimal,
        balance_type: BalanceType = BalanceType.FREE,
    ) -> bool:
        """Try to reserve resources for a position.

        RISK-01: "Available on another exchange" != "available for margin now."
        """
        available = self.available(asset_id, balance_type)
        if available < amount:
            return False

        reservation = ResourceReservation(
            position_id=position_id,
            asset_id=asset_id,
            amount=amount,
            balance_type=balance_type,
            reserved_at_ns=0,
        )

        if asset_id not in self._reservations:
            self._reservations[asset_id] = []
        self._reservations[asset_id].append(reservation)

        # Actually reserve in ledger (free -> reserved)
        self.ledger.reserve_funds(asset_id, amount, 0, f"position:{position_id}")
        return True

    def release(self, position_id: str, asset_id: str, amount: Decimal) -> None:
        """Release a reservation when position closes."""
        reservations = self._reservations.get(asset_id, [])
        for i, res in enumerate(reservations):
            if res.position_id == position_id:
                self.ledger.release_reservation(asset_id, amount, 0, f"release:{position_id}")
                reservations.pop(i)
                break

    def check_collateral_haircut(
        self,
        asset_id: str,
        haircut: Decimal,
    ) -> Decimal:
        """Get collateral value after haircut.

        RISK-01: Collateral has haircut applied.
        """
        collateral = self.ledger.get_balance(asset_id, BalanceType.COLLATERAL)
        return collateral * (Decimal("1") - haircut)

    def detect_conflicts(
        self,
        candidate_a: str,
        candidate_b: str,
        required: Mapping[str, Decimal],
    ) -> list[ResourceConflict]:
        """Detect resource conflicts between two candidates."""
        conflicts: list[ResourceConflict] = []
        for asset_id, amount in required.items():
            avail = self.available(asset_id)
            if avail < amount:
                conflicts.append(ResourceConflict(
                    position_a=candidate_a,
                    position_b=candidate_b,
                    asset_id=asset_id,
                    required=amount,
                    available=avail,
                    resolution="deferred",
                ))
                self._conflicts.append(conflicts[-1])
        return conflicts

    def greedy_allocate(
        self,
        ranked_candidates: Sequence[tuple[str, Mapping[str, Decimal]]],
    ) -> dict[str, bool]:
        """Greedy allocation by verified ranking (Section 10.6).

        Cannot sum best PnL of all candidates as simultaneously available.
        """
        results: dict[str, bool] = {}
        for candidate_id, requirements in ranked_candidates:
            approved = True
            for asset_id, amount in requirements.items():
                if self.available(asset_id) < amount:
                    approved = False
                    break
            if approved:
                for asset_id, amount in requirements.items():
                    self.try_reserve(candidate_id, asset_id, amount)
                results[candidate_id] = True
            else:
                results[candidate_id] = False
        return results

    def stress_by_asset_group(
        self,
        asset_id: str,
        moves: Sequence[Decimal],
    ) -> list[Decimal]:
        """Stress-test portfolio value under price moves.

        RISK-02: Stress grid for asset groups.
        """
        results = []
        for move in moves:
            equity_impact = self.ledger.get_total_for_asset(asset_id) * move
            results.append(equity_impact)
        return results
