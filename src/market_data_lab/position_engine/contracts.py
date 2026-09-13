from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, NamedTuple, Sequence

PositionState = Literal[
    "Proposed",
    "Reserved",
    "Opening",
    "Hedged",
    "Partial",
    "Closing",
    "Unwinding",
    "DataImpaired",
    "Closed",
    "Rejected",
]

PortfolioMode = Literal["isolated_case", "portfolio_replay"]

ScenarioKind = Literal["frozen_market", "live_replay", "counterfactual_grid"]


class PositionId(NamedTuple):
    strategy: str
    position_id: str

    def __str__(self) -> str:
        return f"{self.strategy}:{self.position_id}"


@dataclass(frozen=True, slots=True)
class VirtualFill:
    leg_id: str
    requested_raw: int
    filled_raw: int
    price_raw: int
    fee_raw: int
    received_at_offset_ns: int


@dataclass
class LegFacts:
    leg_id: str
    entry: VirtualFill | None = None
    exit: VirtualFill | None = None
    entry_marks: dict[str, int] = field(default_factory=dict)
    exit_marks: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExitPolicy:
    target_net_pnl_raw: int | None = None
    max_holding_horizon_ns: int | None = None
    adverse_basis_threshold_bps: int | None = None
    funding_reversal_threshold_raw: int | None = None
    min_liquidity_raw: int | None = None
    margin_buffer_raw: int | None = None
    data_outage_max_ns: int | None = None


@dataclass(frozen=True, slots=True)
class Leg:
    leg_id: str
    kind: Literal["spot", "perp", "amm"]
    direction: Literal["buy", "sell"]
    input_asset_id: str
    output_asset_id: str
    requested_raw: int


@dataclass(frozen=True, slots=True)
class PositionIntent:
    position_id: PositionId
    scenario_kind: ScenarioKind
    legs: tuple[Leg, ...]
    initial_balances: tuple[tuple[str, int], ...]
    exit_policy: ExitPolicy
    opened_at_offset_ns: int = 0


@dataclass
class Position:
    intent: PositionIntent
    state: PositionState = "Proposed"
    leg_facts: dict[str, LegFacts] = field(default_factory=dict)
    reserved_balances: dict[str, int] = field(default_factory=dict)
    last_evaluated_at_offset_ns: int = 0
    accumulated_funding_raw: int = 0
    accumulated_borrow_raw: int = 0
    version: int = 0

    def leg(self, leg_id: str) -> LegFacts:
        if leg_id not in self.leg_facts:
            self.leg_facts[leg_id] = LegFacts(leg_id=leg_id)
        return self.leg_facts[leg_id]


@dataclass
class PositionRecord:
    position: Position
    state_reason: str | None = None
    state_changed_at_offset_ns: int = 0
    scenario_log: list[str] = field(default_factory=list)
