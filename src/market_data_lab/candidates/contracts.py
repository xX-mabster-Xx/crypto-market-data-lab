from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Mapping, NamedTuple, Sequence

MarketEvidence = Literal["indicative", "bbo", "depth_checked", "exact_quote", "sequential_simulation"]
Economics = Literal["entry_basis_only", "spot_conversion", "position_scenario", "closed_paper_position"]
ResourceContext = Literal["unknown", "hypothetical", "account_checked"]
FundingQuality = Literal["none_required", "unknown", "projected", "historically_verified", "settled"]
Validation = Literal["screened", "quote_checked", "paper_open", "paper_closed", "real_fills_imported"]
SearchCompleteness = Literal["exhaustive_in_configured_subset", "bounded_shortlist", "budget_limited"]

SignalEvent = Literal["detected", "verification_pending", "verified", "improved", "expired", "invalidated", "rejected"]

RejectionReason = Literal[
    "asset_identity_unverified",
    "unsupported_contract_model",
    "unit_mismatch",
    "fee_unknown",
    "borrow_capacity_unknown",
    "insufficient_inventory",
    "insufficient_margin",
    "insufficient_known_depth",
    "book_invalid",
    "state_stale",
    "quote_expired",
    "quantity_not_executable",
    "residual_limit",
    "funding_semantics_unknown",
    "funding_time_ambiguous",
    "exit_model_incomplete",
    "fx_conversion_missing",
    "rebalance_unknown",
    "liquidity_overlap_unknown",
    "provider_budget_exhausted",
    "verification_deadline_missed",
    "unprofitable_after_costs",
]


class CandidateId(NamedTuple):
    strategy: str
    candidate_id: str

    def __str__(self) -> str:
        return f"{self.strategy}:{self.candidate_id}"


@dataclass(frozen=True, slots=True)
class QualityDimensions:
    market_evidence: MarketEvidence = "indicative"
    economics: Economics = "entry_basis_only"
    resource_context: ResourceContext = "unknown"
    funding_quality: FundingQuality = "none_required"
    validation: Validation = "screened"
    search_completeness: SearchCompleteness = "bounded_shortlist"
    execution_enabled: bool = False


@dataclass(frozen=True, slots=True)
class SignalLifecycle:
    events: list[SignalEvent] = field(default_factory=list)
    evaluation_count: int = 0
    distinct_evidence_count: int = 0
    dex_rounds: int = 0
    duration_ns: int = 0
    outage_ns: int = 0

    @property
    def is_active(self) -> bool:
        return "invalidated" not in self.events and "rejected" not in self.events

    def as_dict(self) -> dict[str, object]:
        return {
            "events": self.events,
            "evaluation_count": self.evaluation_count,
            "distinct_evidence_count": self.distinct_evidence_count,
            "dex_rounds": self.dex_rounds,
            "duration_ns": self.duration_ns,
            "outage_ns": self.outage_ns,
        }


@dataclass
class CandidateCard:
    schema_version: int = 2
    strategy: str = "unknown"
    candidate_id: str = ""
    quantities: Mapping[str, str] = field(default_factory=dict)
    entry_basis_value: Mapping[str, str] | None = None
    projected_pnl_by_scenario: Mapping[str, str] = field(default_factory=dict)
    funding_assumption: Mapping[str, str] | None = None
    costs_total: Mapping[str, str] | None = None
    realized_pnl: Mapping[str, str] | None = None
    resource_context: ResourceContext = "unknown"
    validation: Validation = "screened"
    execution_enabled: bool = False
    evidence_bundle_id: str | None = None
    quality_dimensions: QualityDimensions = field(default_factory=QualityDimensions)
    signal_lifecycle: SignalLifecycle = field(default_factory=SignalLifecycle)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "strategy": self.strategy,
            "candidate_id": self.candidate_id,
            "quantities": dict(self.quantities),
            "entry_basis_value": dict(self.entry_basis_value) if self.entry_basis_value else None,
            "projected_pnl_by_scenario": dict(self.projected_pnl_by_scenario),
            "funding_assumption": dict(self.funding_assumption) if self.funding_assumption else None,
            "costs_total": dict(self.costs_total) if self.costs_total else None,
            "realized_pnl": dict(self.realized_pnl) if self.realized_pnl else None,
            "resource_context": self.resource_context,
            "validation": self.validation,
            "execution_enabled": self.execution_enabled,
            "evidence_bundle_id": self.evidence_bundle_id,
            "quality_dimensions": {
                "market_evidence": self.quality_dimensions.market_evidence,
                "economics": self.quality_dimensions.economics,
                "resource_context": self.quality_dimensions.resource_context,
                "funding_quality": self.quality_dimensions.funding_quality,
                "validation": self.quality_dimensions.validation,
                "search_completeness": self.quality_dimensions.search_completeness,
                "execution_enabled": self.quality_dimensions.execution_enabled,
            },
            "signal_lifecycle": self.signal_lifecycle.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class ExplainableRejection:
    candidate_id: str
    reason: RejectionReason
    details: str
    quality_dimensions: QualityDimensions = field(default_factory=QualityDimensions)

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "reason": self.reason,
            "details": self.details,
            "quality_dimensions": {
                "market_evidence": self.quality_dimensions.market_evidence,
                "economics": self.quality_dimensions.economics,
                "resource_context": self.quality_dimensions.resource_context,
                "funding_quality": self.quality_dimensions.funding_quality,
                "validation": self.quality_dimensions.validation,
                "search_completeness": self.quality_dimensions.search_completeness,
                "execution_enabled": self.quality_dimensions.execution_enabled,
            },
        }
