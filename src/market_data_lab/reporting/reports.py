"""Report generation — Markdown and structured output.

Section 14.5: Local read-only interfaces; JSON is primary machine format,
Markdown/HTML for representation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping, Sequence

from ..candidates.contracts import CandidateCard
from .evidence import EvidenceBundle
from ..strategies.base import StrategyResult


@dataclass
class StrategyReport:
    """Full report for a strategy evaluation."""

    strategy_id: str
    candidate_id: str
    result: StrategyResult | None = None
    candidate_card: CandidateCard | None = None
    evidence_bundle: EvidenceBundle | None = None
    risk_classes: list[str] = field(default_factory=list)
    constraints: list[str] = field(default_factory=list)

    def to_markdown(self) -> str:
        """Generate markdown report (Section 14.3)."""
        lines: list[str] = []

        if self.candidate_card:
            card = self.candidate_card
            lines.append(f"## {card.strategy}")
            lines.append(f"**Candidate:** {card.candidate_id}")
            lines.append(f"**Schema Version:** {card.schema_version}")
            lines.append("")

            lines.append("### Quantities")
            for k, v in card.quantities.items():
                lines.append(f"- {k}: {v}")
            lines.append("")

            if card.entry_basis_value:
                lines.append("### Entry Basis")
                for k, v in card.entry_basis_value.items():
                    lines.append(f"- {k}: {v}")
                lines.append("")

            if card.projected_pnl_by_scenario:
                lines.append("### Projected PnL by Scenario")
                for k, v in card.projected_pnl_by_scenario.items():
                    lines.append(f"- {k}: {v}")
                lines.append("")

            if card.costs_total:
                lines.append("### Costs")
                for k, v in card.costs_total.items():
                    lines.append(f"- {k}: {v}")
                lines.append("")

            # Quality dimensions (Section 14.1)
            qd = card.quality_dimensions
            lines.append("### Quality Dimensions")
            lines.append(f"- market_evidence: {qd.market_evidence}")
            lines.append(f"- economics: {qd.economics}")
            lines.append(f"- resource_context: {qd.resource_context}")
            lines.append(f"- funding_quality: {qd.funding_quality}")
            lines.append(f"- validation: {qd.validation}")
            lines.append(f"- search_completeness: {qd.search_completeness}")
            lines.append(f"- execution_enabled: {qd.execution_enabled}")
            lines.append("")

            if self.constraints:
                lines.append("### Constraints / Rejection Reasons")
                for c in self.constraints:
                    lines.append(f"- {c}")
                lines.append("")

            if self.risk_classes:
                lines.append("### Risk Classes (RISK-02)")
                for r in self.risk_classes:
                    lines.append(f"- {r}")
                lines.append("")

        return "\n".join(lines)

    def to_dict(self) -> dict:
        """Convert to dict for JSON export."""
        result = {
            "strategy_id": self.strategy_id,
            "candidate_id": self.candidate_id,
        }
        if self.candidate_card:
            result["candidate_card"] = self.candidate_card.as_dict()
        if self.evidence_bundle:
            result["evidence_bundle_id"] = self.evidence_bundle.bundle_id
            result["evidence_bundle_path"] = f"bundles/{self.evidence_bundle.bundle_id}"
        result["risk_classes"] = self.risk_classes
        result["constraints"] = self.constraints
        return result


def generate_markdown_report(
    candidates: Sequence[CandidateCard],
    rejected: Sequence = (),
    title: str = "Market Research Report",
) -> str:
    """Generate a full markdown report from candidates.

    Section 14.5: Markdown/HTML for representation; JSON is primary format.
    """
    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(f"**Active Candidates:** {len(candidates)}")
    lines.append(f"**Rejected:** {len(rejected)}")
    lines.append("")

    if candidates:
        lines.append("## Active Candidates")
        for card in candidates:
            lines.append(f"### {card.strategy} — {card.candidate_id}")
            lines.append(f"- Quality: {card.quality_dimensions.market_evidence}")
            lines.append(f"- Validation: {card.quality_dimensions.validation}")
            lines.append(f"- Evidence: {card.evidence_bundle_id or 'none'}")
            if card.costs_total:
                lines.append(f"- Costs: {card.costs_total.get('amount', 'N/A')} {card.costs_total.get('currency', '')}")
            lines.append("")

    if rejected:
        lines.append("## Rejected Candidates")
        for rej in rejected:
            lines.append(f"- {rej.candidate_id}: {rej.reason} — {rej.details}")
        lines.append("")

    return "\n".join(lines)


@dataclass
class ReportGenerator:
    """Generates various report formats."""

    def candidate_report(self, card: CandidateCard) -> StrategyReport:
        return StrategyReport(
            strategy_id=card.strategy,
            candidate_id=card.candidate_id,
            candidate_card=card,
        )

    def full_report(
        self,
        candidates: Sequence[CandidateCard],
        rejections: Sequence = (),
    ) -> str:
        return generate_markdown_report(candidates, rejections)
