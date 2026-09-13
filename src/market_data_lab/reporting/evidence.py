"""Evidence Bundle — full proof of calculation for a result.

Section 7.5: EvidenceBundle contains exact amounts/quotes, versions,
calculation path, funding events, exit scenarios, breakdown, constraints.

Section 15: STORE-01 to STORE-04 — decision bundle for quote_checked+.
Section 15.3: decision_replay and path_replay reproduction.
STORE-04: schema version, code/content hash, model version, config hash,
provider spec revision, determinism seed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping, Sequence

from ..cost_breakdown.contracts import CostBreakdown
from ..execution_cost.contracts import ExecutionResult, AMMState, OrderBookSnapshot
from ..domain.events import StateVersion
from ..carry.funding import FundingRate
from ..position_engine.contracts import PositionIntent, VirtualFill


def _compute_content_hash(data: dict) -> str:
    """Compute SHA-256 hash of content."""
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass
class EvidenceBundle:
    """Complete evidence bundle for a candidate or position.

    Section 7.5: Cannot leave only PnL number — full bundle required.
    Section 15: Minimum decision bundle for quote_checked+.
    """

    bundle_id: str
    schema_version: int = 2
    strategy_id: str = ""
    candidate_id: str = ""
    request: dict = field(default_factory=dict)
    result: dict = field(default_factory=dict)

    # Exact amounts and quotes
    exact_amounts: dict[str, str] = field(default_factory=dict)
    exact_quotes: list[dict] = field(default_factory=list)

    # Versions of all data dependencies
    state_versions: dict[str, dict] = field(default_factory=dict)
    funding_events: list[dict] = field(default_factory=dict)

    # Cost breakdown
    cost_breakdown: dict | None = None

    # Exit scenarios
    exit_scenarios: dict[str, dict] = field(default_factory=dict)

    # Residual positions
    residual_positions: dict[str, str] = field(default_factory=dict)

    # Constraints and rejections
    constraints: list[str] = field(default_factory=list)
    rejection_reason: str | None = None

    # Versioning (STORE-04)
    code_hash: str | None = None
    model_version: str | None = None
    config_hash: str | None = None
    provider_spec_revision: str | None = None
    determinism_seed: str | None = None
    content_hash: str | None = None

    metadata: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.content_hash is None:
            self.content_hash = _compute_content_hash(self.result)

    def add_state_version(self, key: str, version: StateVersion) -> None:
        """Track state version used for this calculation."""
        self.state_versions[key] = {
            "version": version.version,
            "published_at_ns": version.published_at_ns,
            "source_ids": list(version.source_ids),
        }

    def add_funding_event(self, event: FundingRate) -> None:
        """Add a funding event to evidence."""
        self.funding_events.append({
            "rate": str(event.rate),
            "rate_unit": event.rate_unit.value if hasattr(event.rate_unit, 'value') else str(event.rate_unit),
            "event_time_ns": event.event_time_ns,
            "event_id": event.event_id,
            "quality": event.quality,
            "reference_price": event.reference_price,
        })

    def add_cost_breakdown(self, breakdown: CostBreakdown) -> None:
        """Add a cost breakdown to evidence."""
        self.cost_breakdown = breakdown.as_dict()

    def add_execution_result(self, result: ExecutionResult, label: str) -> None:
        """Add an execution estimate to evidence."""
        if result.estimate:
            self.exact_quotes.append({
                "label": label,
                "input_amount": str(result.input_amount_raw) if hasattr(result, 'input_amount_raw') else "0",
                "output_amount": str(result.output_amount_raw) if hasattr(result, 'output_amount_raw') else "0",
                "cost_breakdown": result.estimate.cost_breakdown.as_dict() if result.estimate.cost_breakdown else None,
                "used_levels": len(result.estimate.used_levels) if result.estimate.used_levels else 0,
                "post_state": True if result.estimate.post_state else False,
            })

    def to_dict(self) -> dict:
        return {
            "bundle_id": self.bundle_id,
            "schema_version": self.schema_version,
            "strategy_id": self.strategy_id,
            "candidate_id": self.candidate_id,
            "request": self.request,
            "result": self.result,
            "exact_amounts": self.exact_amounts,
            "exact_quotes": self.exact_quotes,
            "state_versions": self.state_versions,
            "funding_events": self.funding_events,
            "cost_breakdown": self.cost_breakdown,
            "exit_scenarios": self.exit_scenarios,
            "residual_positions": self.residual_positions,
            "constraints": self.constraints,
            "rejection_reason": self.rejection_reason,
            "code_hash": self.code_hash,
            "model_version": self.model_version,
            "config_hash": self.config_hash,
            "provider_spec_revision": self.provider_spec_revision,
            "determinism_seed": self.determinism_seed,
            "content_hash": self.content_hash,
            "metadata": self.metadata,
        }

    def save_to_file(self, path: str) -> None:
        """Save bundle to file for reproduction."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)

    @classmethod
    def load_from_file(cls, path: str) -> "EvidenceBundle":
        """Load bundle from file."""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls(**data)


@dataclass
class EvidenceBundleBuilder:
    """Builds evidence bundles for candidates and positions."""

    code_hash: str | None = None
    model_version: str | None = None
    config_hash: str | None = None
    provider_spec_revision: str | None = None
    determinism_seed: str | None = None

    def build(
        self,
        strategy_id: str,
        candidate_id: str,
        request: dict,
        result: dict,
        state_versions: list[tuple[str, StateVersion]],
        funding_events: list[FundingRate],
        cost_breakdown: CostBreakdown | None = None,
        execution_results: list[tuple[str, ExecutionResult]] | None = None,
        exit_scenarios: dict[str, dict] | None = None,
        residual_positions: dict[str, Decimal] | None = None,
        constraints: list[str] | None = None,
    ) -> EvidenceBundle:
        """Build a complete evidence bundle."""
        import time

        bundle = EvidenceBundle(
            bundle_id=f"bundle-{int(time.monotonic_ns())}",
            schema_version=2,
            strategy_id=strategy_id,
            candidate_id=candidate_id,
            request=request,
            result=result,
            code_hash=self.code_hash,
            model_version=self.model_version,
            config_hash=self.config_hash,
            provider_spec_revision=self.provider_spec_revision,
            determinism_seed=self.determinism_seed,
        )

        for key, version in state_versions:
            bundle.add_state_version(key, version)

        for event in funding_events:
            bundle.add_funding_event(event)

        if cost_breakdown:
            bundle.add_cost_breakdown(cost_breakdown)

        if execution_results:
            for label, exec_result in execution_results:
                bundle.add_execution_result(exec_result, label)

        if exit_scenarios:
            bundle.exit_scenarios = exit_scenarios

        if residual_positions:
            bundle.residual_positions = {k: str(v) for k, v in residual_positions.items()}

        if constraints:
            bundle.constraints = constraints

        return bundle
