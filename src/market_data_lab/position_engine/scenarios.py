from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from .contracts import (
    ExitPolicy,
    Leg,
    Position,
    PositionId,
    VirtualFill,
)
from .engine import PositionEngine


@dataclass(frozen=True)
class ScenarioResult:
    scenario: str
    success: bool
    reason: str
    final_state: str
    position_version: int


def scenario_both_legs_filled(
    engine: PositionEngine,
    position_id: PositionId,
    fills: Mapping[str, VirtualFill],
) -> ScenarioResult:
    """Both legs filled successfully."""
    for leg_id, fill in fills.items():
        engine.open_leg(position_id, leg_id, fill)
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="both_legs_filled",
        success=position.state == "Hedged",
        reason="all legs filled",
        final_state=position.state,
        position_version=position.version,
    )


def scenario_cex_filled_dex_stale(
    engine: PositionEngine,
    position_id: PositionId,
    cex_fill: VirtualFill,
) -> ScenarioResult:
    """CEX leg filled, DEX leg became stale/failed."""
    engine.open_leg(position_id, cex_fill.leg_id, cex_fill)
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="cex_filled_dex_stale",
        success=position.state == "Partial",
        reason="dex leg stale",
        final_state=position.state,
        position_version=position.version,
    )


def scenario_dex_filled_perp_unavailable(
    engine: PositionEngine,
    position_id: PositionId,
    dex_fill: VirtualFill,
) -> ScenarioResult:
    """DEX leg filled, perp leg unavailable."""
    engine.open_leg(position_id, dex_fill.leg_id, dex_fill)
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="dex_filled_perp_unavailable",
        success=position.state == "Partial",
        reason="perp unavailable",
        final_state=position.state,
        position_version=position.version,
    )


def scenario_partial_fill(
    engine: PositionEngine,
    position_id: PositionId,
    partial_fill: VirtualFill,
    fill_ratio: float = 0.5,
) -> ScenarioResult:
    """Partial fill on one leg."""
    adjusted = VirtualFill(
        leg_id=partial_fill.leg_id,
        requested_raw=partial_fill.requested_raw,
        filled_raw=int(partial_fill.filled_raw * fill_ratio),
        price_raw=partial_fill.price_raw,
        fee_raw=partial_fill.fee_raw,
        received_at_offset_ns=partial_fill.received_at_offset_ns,
    )
    engine.open_leg(position_id, adjusted.leg_id, adjusted)
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="partial_fill",
        success=position.state == "Partial",
        reason=f"partial fill ratio={fill_ratio}",
        final_state=position.state,
        position_version=position.version,
    )


def scenario_pool_route_changed(
    engine: PositionEngine,
    position_id: PositionId,
    reason: str = "pool_route_changed",
) -> ScenarioResult:
    """Pool route changed during execution."""
    engine.unwind(position_id, reason)
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="pool_route_changed",
        success=position.state == "Closed",
        reason=reason,
        final_state=position.state,
        position_version=position.version,
    )


def scenario_funding_event_missed(
    engine: PositionEngine,
    position_id: PositionId,
    expected_funding_raw: int,
) -> ScenarioResult:
    """Funding event was missed."""
    engine.apply_funding(position_id, expected_funding_raw)
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="funding_event_missed",
        success=position.state == "Hedged",
        reason=f"funding applied={expected_funding_raw}",
        final_state=position.state,
        position_version=position.version,
    )


def scenario_exit_more_expensive(
    engine: PositionEngine,
    position_id: PositionId,
    exit_fills: Mapping[str, VirtualFill],
    cost_overrun_bps: int = 10,
) -> ScenarioResult:
    """Exit cost exceeded expectations."""
    for leg_id, fill in exit_fills.items():
        engine.mark_exit(position_id, leg_id, fill)
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="exit_more_expensive",
        success=position.state in {"Closed", "Closing"},
        reason=f"exit cost overrun={cost_overrun_bps}bps",
        final_state=position.state,
        position_version=position.version,
    )


def scenario_withdraw_borrow_temporarily_closed(
    engine: PositionEngine,
    position_id: PositionId,
    reason: str = "withdraw_borrow_temporarily_closed",
) -> ScenarioResult:
    """Withdraw/borrow temporarily closed."""
    engine.mark_data_impaired(position_id, reason)
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="withdraw_borrow_temporarily_closed",
        success=position.state == "DataImpaired",
        reason=reason,
        final_state=position.state,
        position_version=position.version,
    )


def scenario_source_lost_during_hold(
    engine: PositionEngine,
    position_id: PositionId,
    reason: str = "source_lost_during_hold",
) -> ScenarioResult:
    """Data source disappeared during position hold."""
    engine.mark_data_impaired(position_id, reason)
    engine.unwind(position_id, f"unwind_after_{reason}")
    position = engine.position(position_id)
    return ScenarioResult(
        scenario="source_lost_during_hold",
        success=position.state == "Closed",
        reason=reason,
        final_state=position.state,
        position_version=position.version,
    )


ALL_SCENARIOS = [
    "both_legs_filled",
    "cex_filled_dex_stale",
    "dex_filled_perp_unavailable",
    "partial_fill",
    "pool_route_changed",
    "funding_event_missed",
    "exit_more_expensive",
    "withdraw_borrow_temporarily_closed",
    "source_lost_during_hold",
]
