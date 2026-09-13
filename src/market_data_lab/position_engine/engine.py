from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Mapping, Sequence

from .contracts import (
    ExitPolicy,
    Leg,
    LegFacts,
    PortfolioMode,
    Position,
    PositionId,
    PositionIntent,
    PositionRecord,
    PositionState,
    VirtualFill,
)

_ALLOWED_TRANSITIONS: Mapping[PositionState, set[PositionState]] = {
    "Proposed": {"Reserved", "Rejected", "Opening"},
    "Reserved": {"Opening", "Rejected"},
    "Opening": {"Hedged", "Partial", "Rejected"},
    "Partial": {"Hedged", "Unwinding"},
    "Hedged": {"Closing", "DataImpaired", "Unwinding"},
    "DataImpaired": {"Hedged", "Unwinding"},
    "Closing": {"Closed", "Partial"},
    "Unwinding": {"Closed"},
    "Closed": set(),
    "Rejected": set(),
}

_TERMINAL_STATES = {"Closed", "Rejected"}


@dataclass
class Reservation:
    position_id: PositionId
    balances: dict[str, int] = field(default_factory=dict)


@dataclass
class PositionEngine:
    """Research-only position lifecycle simulator."""

    mode: PortfolioMode = "isolated_case"
    monotonic_ns: callable = time.monotonic_ns

    def __post_init__(self) -> None:
        self._positions: dict[PositionId, Position] = {}
        self._records: dict[PositionId, PositionRecord] = {}
        self._reservations: dict[PositionId, Reservation] = {}
        self._counts: dict[str, int] = defaultdict(int)
        self._portfolio_balances: dict[str, int] = {}

    def set_portfolio_balance(self, asset_id: str, amount: int) -> None:
        self._portfolio_balances[asset_id] = amount

    def get_portfolio_balance(self, asset_id: str) -> int:
        return self._portfolio_balances.get(asset_id, 0)

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    def position(self, position_id: PositionId) -> Position | None:
        return self._positions.get(position_id)

    def record(self, position_id: PositionId) -> PositionRecord | None:
        return self._records.get(position_id)

    def propose(self, intent: PositionIntent) -> PositionRecord:
        if intent.position_id in self._positions:
            raise ValueError(f"position {intent.position_id} already exists")

        position = Position(intent=intent, state="Proposed")
        self._positions[intent.position_id] = position

        if self.mode == "portfolio_replay":
            if not self._can_reserve(intent):
                return self._reject(intent.position_id, "insufficient_portfolio_resources")
            self._reserve(intent)
            self._transition(position, "Reserved", "resources_reserved")

        return self._record(intent.position_id)

    def open_leg(self, position_id: PositionId, leg_id: str, fill: VirtualFill) -> None:
        position = self._get_position(position_id)
        if position.state not in {"Proposed", "Reserved", "Opening", "Partial"}:
            raise ValueError(f"cannot open leg from state {position.state}")

        if position.state in {"Proposed", "Reserved"}:
            self._transition(position, "Opening", "first_leg_opened")

        leg_facts = position.leg(leg_id)
        leg_facts.entry = fill

        if self._all_legs_filled(position):
            self._transition(position, "Hedged", "all_legs_filled")
        elif fill.filled_raw < fill.requested_raw:
            self._transition(position, "Partial", "partial_fill")

    def mark_exit(self, position_id: PositionId, leg_id: str, fill: VirtualFill) -> None:
        position = self._get_position(position_id)
        if position.state not in {"Hedged", "Closing", "Partial"}:
            raise ValueError(f"cannot mark exit from state {position.state}")

        leg_facts = position.leg(leg_id)
        leg_facts.exit = fill

        if self._all_exits_filled(position):
            self._transition(position, "Closed", "exit_completed")
        elif position.state == "Hedged":
            self._transition(position, "Closing", "exit_initiated")

    def apply_funding(self, position_id: PositionId, amount_raw: int) -> None:
        position = self._get_position(position_id)
        if position.state not in {"Hedged", "Partial"}:
            return
        position.accumulated_funding_raw += amount_raw

    def apply_borrow(self, position_id: PositionId, amount_raw: int) -> None:
        position = self._get_position(position_id)
        if position.state not in {"Hedged", "Partial"}:
            return
        position.accumulated_borrow_raw += amount_raw

    def evaluate_exit_policy(self, position_id: PositionId) -> str | None:
        position = self._get_position(position_id)
        if position.state not in {"Hedged", "Partial", "DataImpaired"}:
            return None

        policy = position.intent.exit_policy
        now = self.monotonic_ns()
        elapsed = now - position.intent.opened_at_offset_ns

        if policy.max_holding_horizon_ns is not None and elapsed >= policy.max_holding_horizon_ns:
            return "max_holding_horizon_exceeded"
        if policy.data_outage_max_ns is not None and elapsed >= policy.data_outage_max_ns:
            return "data_outage_timeout"

        return None

    def close(self, position_id: PositionId, reason: str) -> None:
        position = self._get_position(position_id)
        if position.state not in {"Hedged", "Partial", "Closing"}:
            raise ValueError(f"cannot close from state {position.state}")

        if self._all_exits_filled(position):
            self._transition(position, "Closed", reason)
            self._release_reservation(position_id)
        elif position.state != "Closing":
            self._transition(position, "Closing", reason)

    def unwind(self, position_id: PositionId, reason: str) -> None:
        position = self._get_position(position_id)
        if position.state in _TERMINAL_STATES:
            return
        self._transition(position, "Unwinding", reason)
        self._transition(position, "Closed", f"unwound: {reason}")
        self._release_reservation(position_id)

    def mark_data_impaired(self, position_id: PositionId, reason: str) -> None:
        position = self._get_position(position_id)
        if position.state not in {"Hedged", "Closing"}:
            return
        self._transition(position, "DataImpaired", reason)

    def restore_data(self, position_id: PositionId) -> None:
        position = self._get_position(position_id)
        if position.state != "DataImpaired":
            return
        self._transition(position, "Hedged", "data_restored")

    def _can_reserve(self, intent: PositionIntent) -> bool:
        for asset_id, amount in intent.initial_balances:
            available = self._portfolio_balances.get(asset_id, 0)
            if available < amount:
                return False
        return True

    def _reserve(self, intent: PositionIntent) -> None:
        reservation = Reservation(position_id=intent.position_id)
        for asset_id, amount in intent.initial_balances:
            available = self._portfolio_balances.get(asset_id, 0)
            self._portfolio_balances[asset_id] = available - amount
            reservation.balances[asset_id] = amount
        self._reservations[intent.position_id] = reservation

    def _release_reservation(self, position_id: PositionId) -> None:
        reservation = self._reservations.pop(position_id, None)
        if reservation is None:
            return
        for asset_id, amount in reservation.balances.items():
            available = self._portfolio_balances.get(asset_id, 0)
            self._portfolio_balances[asset_id] = available + amount

    def _all_legs_filled(self, position: Position) -> bool:
        return all(
            facts.entry is not None
            for facts in position.leg_facts.values()
        ) and len(position.leg_facts) == len(position.intent.legs)

    def _any_leg_filled(self, position: Position) -> bool:
        return any(facts.entry is not None for facts in position.leg_facts.values())

    def _all_exits_filled(self, position: Position) -> bool:
        legs_with_entry = [f for f in position.leg_facts.values() if f.entry is not None]
        if not legs_with_entry:
            return False
        return all(facts.exit is not None for facts in legs_with_entry)

    def _transition(self, position: Position, target: PositionState, reason: str) -> None:
        current = position.state
        allowed = _ALLOWED_TRANSITIONS.get(current, set())
        if target not in allowed:
            raise ValueError(f"invalid transition: {current} -> {target}")

        position.state = target
        position.version += 1
        position.last_evaluated_at_offset_ns = self.monotonic_ns()

        record = self._record(position.intent.position_id)
        record.state_reason = reason
        record.state_changed_at_offset_ns = position.last_evaluated_at_offset_ns
        record.scenario_log.append(f"{current}->{target}: {reason}")

        self._counts[f"state_{target.lower()}"] += 1

    def _reject(self, position_id: PositionId, reason: str) -> PositionRecord:
        position = self._get_position(position_id)
        self._transition(position, "Rejected", reason)
        return self._record(position_id)

    def _get_position(self, position_id: PositionId) -> Position:
        position = self._positions.get(position_id)
        if position is None:
            raise KeyError(position_id)
        return position

    def _record(self, position_id: PositionId) -> PositionRecord:
        if position_id not in self._records:
            self._records[position_id] = PositionRecord(position=self._positions[position_id])
        return self._records[position_id]

    def active_positions(self) -> Sequence[Position]:
        return [
            p for p in self._positions.values()
            if p.state not in _TERMINAL_STATES
        ]

    def terminal_positions(self) -> Sequence[Position]:
        return [
            p for p in self._positions.values()
            if p.state in _TERMINAL_STATES
        ]
