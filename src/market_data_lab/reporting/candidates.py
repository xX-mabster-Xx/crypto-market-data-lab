"""Candidate report generation (Section 14.3-14.4).

Section 14.3: CandidateCard contains strategy, route, direction, exact
quantities, prices/VWAP, estimated gas, all fees, cashflows, residual,
carry events, exit scenarios, PnL by types, capital, data age, hashes.
Section 14.4: Explainable rejections with minimal reason codes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping, Sequence

from ..candidates.contracts import (
    CandidateCard,
    QualityDimensions,
    RejectionReason,
    SignalEvent,
    SignalLifecycle,
    MarketEvidence,
    Economics,
    ResourceContext,
    FundingQuality,
    Validation,
    SearchCompleteness,
)


@dataclass
class CandidateReport:
    """Full report for a candidate (Section 14.3)."""

    card: CandidateCard
    strategy_id: str
    route: tuple[str, ...]
    direction: str
    exact_quantities: Mapping[str, str]
    prices_vwap: Mapping[str, str]
    estimated_gas: Mapping[str, str]
    all_fees: list[dict]
    cashflows_post_step: list[dict]
    required_residuals: Mapping[str, str]
    carry_events: list[dict]
    exit_scenarios: dict[str, Mapping[str, str]]
    pnl_by_type: dict[str, str]
    capital_required: Mapping[str, str]
    data_age_ms: dict[str, float]
    evidence_hashes: list[str]
    constraint_reasons: list[str]

    @classmethod
    def from_card(cls, card: CandidateCard) -> "CandidateReport":
        """Build report from a CandidateCard."""
        return cls(
            card=card,
            strategy_id=card.strategy,
            route=(),
            direction="",
            exact_quantities=card.quantities,
            prices_vwap=card.entry_basis_value or {},
            estimated_gas=card.costs_total or {},
            all_fees=[],
            cashflows_post_step=[],
            required_residuals={},
            carry_events=[],
            exit_scenarios=dict(card.projected_pnl_by_scenario),
            pnl_by_type=card.projected_pnl_by_scenario,
            capital_required={},
            data_age_ms={},
            evidence_hashes=[card.evidence_bundle_id or ""],
            constraint_reasons=[],
        )

    def to_json(self) -> dict:
        """Serialize to JSON (Section 14.5: JSON is primary format)."""
        return {
            "schema_version": 2,
            "strategy": self.strategy_id,
            "candidate_id": self.card.candidate_id,
            "quantities": dict(self.exact_quantities),
            "entry_basis_value": dict(self.card.entry_basis_value) if self.card.entry_basis_value else None,
            "projected_pnl_by_scenario": dict(self.card.projected_pnl_by_scenario),
            "funding_assumption": dict(self.card.funding_assumption) if self.card.funding_assumption else None,
            "costs_total": dict(self.card.costs_total) if self.card.costs_total else None,
            "realized_pnl": dict(self.card.realized_pnl) if self.card.realized_pnl else None,
            "resource_context": self.card.resource_context,
            "validation": self.card.validation,
            "execution_enabled": self.card.execution_enabled,
            "evidence_bundle_id": self.card.evidence_bundle_id,
            "quality_dimensions": {
                "market_evidence": self.card.quality_dimensions.market_evidence,
                "economics": self.card.quality_dimensions.economics,
                "resource_context": self.card.quality_dimensions.resource_context,
                "funding_quality": self.card.quality_dimensions.funding_quality,
                "validation": self.card.quality_dimensions.validation,
                "search_completeness": self.card.quality_dimensions.search_completeness,
                "execution_enabled": self.card.quality_dimensions.execution_enabled,
            },
            "signal_lifecycle": self.card.signal_lifecycle.as_dict(),
        }


@dataclass
class ReportGenerator:
    """Generates reports for candidates."""

    def generate(self, card: CandidateCard) -> CandidateReport:
        return CandidateReport.from_card(card)
