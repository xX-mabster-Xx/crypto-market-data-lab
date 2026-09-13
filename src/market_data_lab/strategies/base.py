"""Base strategy template interface.

Section 6.2: Route/Strategy Engine forms valid templates and pre-screens.
Section 10.7: Pseudo-code shows template.shortlist(group, view, budget) →
sized_plan, required_quotes, verifier.build_and_check.

Section 19.2: Proposed package structure includes strategies/*.py
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping, Sequence

from ..execution_cost.engine import ExecutionCostEngine
from ..execution_cost.contracts import (
    ExecutionEstimate,
    ExecutionResult,
    OrderBookSnapshot,
    AMMState,
)
from ..cost_breakdown.contracts import CostBreakdown, CostRecord, FeeTier
from ..carry.funding import FundingCalendar
from ..domain.instruments import InstrumentCapability
from ..position_engine.contracts import ExitPolicy, Leg


@dataclass(frozen=True, slots=True)
class ScreeningBounds:
    """Screening bounds for a strategy.

    Section 10: If bounds have proven negative upper bound → reject.
    """

    min_quantity: Decimal
    max_quantity: Decimal
    capital_required: Decimal
    has_proven_negative_upper_bound: bool = False
    upper_bound_pnl: Decimal | None = None


@dataclass
class StrategyResult:
    """Result of strategy screening/evaluation."""

    strategy_id: str
    route: tuple[str, ...]
    quantities: dict[str, Decimal]
    entry_cost_quote: Decimal
    projected_pnl_by_scenario: dict[str, Decimal]
    costs_total_quote: Decimal
    capital_required: Decimal
    confidence: str  # "exact_quote", "quote_checked", "screened"
    evidence_bundle_id: str | None = None
    quality: str = "screened"
    constraints: list[str] = field(default_factory=list)
    residual: Decimal = Decimal("0")
    residual_asset_id: str | None = None

    @property
    def net_pnl_best_case(self) -> Decimal:
        if not self.projected_pnl_by_scenario:
            return Decimal("0")
        return max(self.projected_pnl_by_scenario.values())


class StrategyTemplate(ABC):
    """Base class for all strategy templates.

    Section 19.2: Each strategy is a template with requirements for
    data, quantities, positions, and cashflows.
    Section 6.2: Does NOT create own network connections.
    """

    strategy_id: str
    required_data: tuple[str, ...]
    supported_contract_models: tuple[str, ...]

    def __init__(self) -> None:
        self._cost_engine: ExecutionCostEngine | None = None
        self._funding_calendar: FundingCalendar | None = None

    def set_cost_engine(self, engine: ExecutionCostEngine) -> None:
        self._cost_engine = engine

    def set_funding_calendar(self, calendar: FundingCalendar) -> None:
        self._funding_calendar = calendar

    @abstractmethod
    async def screen(
        self,
        view: Mapping[str, object],
        capital_available: Decimal,
        constraints: Mapping[str, Decimal | None],
    ) -> ScreeningBounds | None:
        """Cheap screening check. Returns bounds or None if not viable.

        ARCH-01: No remote calls in screening.
        """

    @abstractmethod
    async def evaluate(
        self,
        bounds: ScreeningBounds,
        view: Mapping[str, object],
        exit_policy: ExitPolicy,
    ) -> StrategyResult:
        """Full evaluation with exact quotes.

        ARCH-02: May use QuoteBroker for additional depth/quotes.
        """

    def create_legs(
        self,
        quantities: dict[str, Decimal],
        asset_map: Mapping[str, str],
    ) -> tuple[Leg, ...]:
        """Create the legs for this strategy.

        Section 12: Position records entry facts with specific quantities.
        """

    def make_exit_policy(
        self,
        target_pnl: Decimal | None = None,
        max_holding_seconds: int | None = None,
        adverse_basis_threshold_bps: int | None = None,
    ) -> ExitPolicy:
        """Create exit policy before evaluation (POS-04)."""
        return ExitPolicy(
            target_net_pnl_raw=int(target_pnl) if target_pnl else None,
            max_holding_horizon_ns=max_holding_seconds * 1_000_000_000 if max_holding_seconds else None,
            adverse_basis_threshold_bps=adverse_basis_threshold_bps,
        )
