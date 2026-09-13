from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Mapping, Sequence

from .contracts import (
    CandidateCard,
    CandidateId,
    ExplainableRejection,
    QualityDimensions,
    RejectionReason,
    SignalEvent,
    SignalLifecycle,
)


@dataclass
class CandidateTracker:
    """Track candidates with signal lifecycle and explainable rejections."""

    active_candidates: dict[CandidateId, CandidateCard] = field(default_factory=dict)
    historical_best: list[tuple[int, CandidateCard]] = field(default_factory=list)
    rejections: list[ExplainableRejection] = field(default_factory=list)
    _counts: dict[str, int] = field(default_factory=dict)

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    def register(self, card: CandidateCard) -> None:
        candidate_id = CandidateId(card.strategy, card.candidate_id)
        card.signal_lifecycle = replace(
            card.signal_lifecycle,
            events=[*card.signal_lifecycle.events, "detected"],
        )
        self.active_candidates[candidate_id] = card
        self._counts["registered"] = self._counts.get("registered", 0) + 1

    def update_signal(
        self,
        candidate_id: CandidateId,
        event: SignalEvent,
        quality_dimensions: QualityDimensions | None = None,
    ) -> None:
        card = self.active_candidates.get(candidate_id)
        if card is None:
            return
        new_events = [*card.signal_lifecycle.events, event]
        new_distinct = card.signal_lifecycle.distinct_evidence_count
        if event in ("verified", "improved"):
            new_distinct += 1
        card.signal_lifecycle = replace(
            card.signal_lifecycle,
            events=new_events,
            evaluation_count=card.signal_lifecycle.evaluation_count + 1,
            distinct_evidence_count=new_distinct,
        )
        if quality_dimensions:
            card.quality_dimensions = quality_dimensions

        if event in ("invalidated", "rejected"):
            self._counts[f"signal_{event}"] = self._counts.get(f"signal_{event}", 0) + 1

    def reject(
        self,
        candidate_id: str,
        reason: RejectionReason,
        details: str,
        quality_dimensions: QualityDimensions | None = None,
    ) -> ExplainableRejection:
        rejection = ExplainableRejection(
            candidate_id=candidate_id,
            reason=reason,
            details=details,
            quality_dimensions=quality_dimensions or QualityDimensions(),
        )
        self.rejections.append(rejection)
        self._counts["rejected"] = self._counts.get("rejected", 0) + 1

        # Remove from active if present
        to_remove = [k for k in self.active_candidates if k.candidate_id == candidate_id]
        for key in to_remove:
            del self.active_candidates[key]

        return rejection

    def historical_best_add(self, timestamp: int, card: CandidateCard) -> None:
        self.historical_best.append((timestamp, card))

    def get_active(self) -> Sequence[CandidateCard]:
        return list(self.active_candidates.values())

    def get_rejections_by_reason(self, reason: RejectionReason) -> list[ExplainableRejection]:
        return [r for r in self.rejections if r.reason == reason]

    def explain(self, candidate_id: str) -> CandidateCard | None:
        for key, card in self.active_candidates.items():
            if key.candidate_id == candidate_id:
                return card
        for _, card in self.historical_best:
            if card.candidate_id == candidate_id:
                return card
        return None
