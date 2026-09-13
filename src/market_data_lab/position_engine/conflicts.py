from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

from .contracts import Position, PositionId


@dataclass
class LiquidityConflict:
    position_a: PositionId
    position_b: PositionId
    asset_id: str
    amount_a: int
    amount_b: int
    available: int


@dataclass
class ConflictResolution:
    conflict: LiquidityConflict
    winner: PositionId | None
    reason: str


@dataclass
class PortfolioConflictTracker:
    """Tracks and resolves liquidity conflicts between active positions."""

    active_positions: dict[PositionId, Position] = field(default_factory=dict)
    _counts: dict[str, int] = field(default_factory=dict)

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    def detect_conflicts(
        self,
        candidate: Position,
        required: Mapping[str, int],
    ) -> Sequence[LiquidityConflict]:
        conflicts: list[LiquidityConflict] = []
        current_usage: dict[str, int] = {}

        for pid, pos in self.active_positions.items():
            for asset_id, amount in pos.intent.initial_balances:
                current_usage[asset_id] = current_usage.get(asset_id, 0) + amount

        for asset_id, amount in required.items():
            used = current_usage.get(asset_id, 0)
            # For demo purposes, assume available = used + amount (simplified)
            # In real implementation, this would come from portfolio balances
            available = used + amount
            if used + amount > available:
                for pid, pos in self.active_positions.items():
                    for a, amt in pos.intent.initial_balances:
                        if a == asset_id:
                            conflicts.append(LiquidityConflict(
                                position_a=candidate.intent.position_id,
                                position_b=pid,
                                asset_id=asset_id,
                                amount_a=amount,
                                amount_b=amt,
                                available=available,
                            ))
                            self._counts["conflicts_detected"] += 1
                            break

        return conflicts

    def resolve(
        self,
        conflict: LiquidityConflict,
        strategy: str = "fifo",
    ) -> ConflictResolution:
        if strategy == "fifo":
            return ConflictResolution(
                conflict=conflict,
                winner=conflict.position_a,
                reason="first_in_first_out",
            )
        if strategy == "largest_first":
            winner = (
                conflict.position_a
                if conflict.amount_a >= conflict.amount_b
                else conflict.position_b
            )
            return ConflictResolution(
                conflict=conflict,
                winner=winner,
                reason="largest_first",
            )
        return ConflictResolution(
            conflict=conflict,
            winner=None,
            reason="no_resolution_strategy",
        )

    def add_position(self, position: Position) -> None:
        self.active_positions[position.intent.position_id] = position

    def remove_position(self, position_id: PositionId) -> None:
        self.active_positions.pop(position_id, None)
