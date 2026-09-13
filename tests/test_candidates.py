from __future__ import annotations

import unittest

from market_data_lab.candidates.contracts import (
    CandidateCard,
    CandidateId,
    QualityDimensions,
    RejectionReason,
    SignalEvent,
    SignalLifecycle,
    Validation,
)
from market_data_lab.candidates.tracker import CandidateTracker


class QualityDimensionsTest(unittest.TestCase):
    def test_default_dimensions(self) -> None:
        dims = QualityDimensions()
        self.assertEqual(dims.market_evidence, "indicative")
        self.assertEqual(dims.execution_enabled, False)
        self.assertEqual(dims.validation, "screened")

    def test_custom_dimensions(self) -> None:
        dims = QualityDimensions(
            market_evidence="exact_quote",
            validation="quote_checked",
            economics="position_scenario",
        )
        self.assertEqual(dims.market_evidence, "exact_quote")
        self.assertEqual(dims.validation, "quote_checked")


class SignalLifecycleTest(unittest.TestCase):
    def test_is_active(self) -> None:
        lifecycle = SignalLifecycle(events=["detected", "verified"])
        self.assertTrue(lifecycle.is_active)

    def test_is_inactive_after_rejection(self) -> None:
        lifecycle = SignalLifecycle(events=["detected", "verified", "rejected"])
        self.assertFalse(lifecycle.is_active)

    def test_counters(self) -> None:
        lifecycle = SignalLifecycle(
            evaluation_count=10,
            distinct_evidence_count=3,
            dex_rounds=2,
        )
        self.assertEqual(lifecycle.evaluation_count, 10)
        self.assertEqual(lifecycle.distinct_evidence_count, 3)


class CandidateCardTest(unittest.TestCase):
    def test_as_dict_schema_version(self) -> None:
        card = CandidateCard(
            strategy="test_strategy",
            candidate_id="test-1",
            quantities={"base": "1.0"},
        )
        result = card.as_dict()
        self.assertEqual(result["schema_version"], 2)
        self.assertEqual(result["strategy"], "test_strategy")

    def test_full_card(self) -> None:
        card = CandidateCard(
            strategy="long_spot_short_perp",
            candidate_id="test-1",
            quantities={"spot_base": "1.0", "perp_base": "-1.0"},
            entry_basis_value={"amount": "5.00", "currency": "USDT"},
            projected_pnl_by_scenario={"unchanged_basis": "-0.20"},
            funding_assumption={"receipt": "0.20", "quality": "projected"},
            costs_total={"amount": "0.40", "currency": "USDT"},
            resource_context="hypothetical",
            validation="quote_checked",
        )
        result = card.as_dict()
        self.assertEqual(result["resource_context"], "hypothetical")
        self.assertEqual(result["validation"], "quote_checked")
        self.assertFalse(result["execution_enabled"])


class CandidateTrackerTest(unittest.TestCase):
    def test_register(self) -> None:
        tracker = CandidateTracker()
        card = CandidateCard(strategy="test", candidate_id="c1")
        tracker.register(card)
        self.assertEqual(len(tracker.get_active()), 1)
        self.assertEqual(tracker.counts["registered"], 1)

    def test_update_signal(self) -> None:
        tracker = CandidateTracker()
        card = CandidateCard(strategy="test", candidate_id="c1")
        tracker.register(card)
        tracker.update_signal(
            CandidateId("test", "c1"),
            "verified",
            QualityDimensions(validation="quote_checked"),
        )
        updated = tracker.explain("c1")
        self.assertEqual(updated.quality_dimensions.validation, "quote_checked")
        self.assertEqual(updated.signal_lifecycle.distinct_evidence_count, 1)

    def test_reject(self) -> None:
        tracker = CandidateTracker()
        card = CandidateCard(strategy="test", candidate_id="c1")
        tracker.register(card)
        rejection = tracker.reject(
            "c1",
            "unprofitable_after_costs",
            "costs exceed edge",
        )
        self.assertEqual(len(tracker.get_active()), 0)
        self.assertEqual(rejection.reason, "unprofitable_after_costs")

    def test_historical_best(self) -> None:
        tracker = CandidateTracker()
        card = CandidateCard(strategy="test", candidate_id="best-1")
        tracker.historical_best_add(1000, card)
        self.assertEqual(len(tracker.historical_best), 1)
        self.assertEqual(tracker.historical_best[0][0], 1000)

    def test_explain(self) -> None:
        tracker = CandidateTracker()
        card = CandidateCard(strategy="test", candidate_id="explain-me")
        tracker.register(card)
        found = tracker.explain("explain-me")
        self.assertEqual(found.candidate_id, "explain-me")

    def test_get_rejections_by_reason(self) -> None:
        tracker = CandidateTracker()
        tracker.reject("c1", "unprofitable_after_costs", "reason 1")
        tracker.reject("c2", "fee_unknown", "reason 2")
        tracker.reject("c3", "unprofitable_after_costs", "reason 3")
        unprofitable = tracker.get_rejections_by_reason("unprofitable_after_costs")
        self.assertEqual(len(unprofitable), 2)


if __name__ == "__main__":
    unittest.main()
