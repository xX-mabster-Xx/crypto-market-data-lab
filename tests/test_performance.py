from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.performance import (
    BenchmarkResult,
    BenchmarkHarness,
    BenchmarkProfile,
    DataQualityConfig,
    InitialConfig,
    PortfolioConfig,
    QuoteConfig,
    RetentionConfig,
    SearchConfig,
    run_golden_cases,
)


class InitialConfigTest(unittest.TestCase):
    def test_default_config(self) -> None:
        config = InitialConfig()
        self.assertEqual(config.mode, "research")
        self.assertFalse(config.execution_enabled)
        self.assertEqual(config.search.dirty_group_coalesce_ms, 25)
        self.assertEqual(config.quotes.max_pending_requests, 256)
        self.assertFalse(config.data_quality.stablecoin_parity_assumption)
        self.assertEqual(config.portfolio.context, "hypothetical")
        self.assertFalse(config.retention.raw_ticks)


class SearchConfigTest(unittest.TestCase):
    def test_defaults(self) -> None:
        config = SearchConfig()
        self.assertEqual(config.dirty_group_coalesce_ms, 25)
        self.assertEqual(config.shortlist_per_group_and_horizon, 16)
        self.assertEqual(len(config.notional_grid_numeraire), 6)
        self.assertEqual(len(config.horizons_hours), 4)


class QuoteConfigTest(unittest.TestCase):
    def test_defaults(self) -> None:
        config = QuoteConfig()
        self.assertEqual(config.max_remote_requests_per_verification, 6)
        self.assertEqual(config.priority_shares["exit"], 0.30)
        self.assertEqual(config.priority_shares["verification"], 0.50)


class DataQualityConfigTest(unittest.TestCase):
    def test_defaults(self) -> None:
        config = DataQualityConfig()
        self.assertEqual(config.max_execution_book_age_ms, 500)
        self.assertEqual(config.max_remote_quote_age_ms, 1500)
        self.assertFalse(config.stablecoin_parity_assumption)


class PortfolioConfigTest(unittest.TestCase):
    def test_defaults(self) -> None:
        config = PortfolioConfig()
        self.assertEqual(config.context, "hypothetical")
        self.assertFalse(config.allow_unconfirmed_borrow)
        self.assertEqual(config.residue_policy, "bounded_residual")


class RetentionConfigTest(unittest.TestCase):
    def test_defaults(self) -> None:
        config = RetentionConfig()
        self.assertFalse(config.raw_ticks)
        self.assertEqual(config.ram_history_seconds, 180.0)
        self.assertEqual(config.ordinary_evidence_ttl_days, 7)


class BenchmarkProfileTest(unittest.TestCase):
    def test_w1_defaults(self) -> None:
        profile = BenchmarkProfile()
        self.assertEqual(profile.name, "W1")
        self.assertEqual(profile.instruments, 200)
        self.assertEqual(profile.pools, 200)
        self.assertEqual(profile.updates_per_second, 5000)
        self.assertEqual(profile.target_p95_screen_ms, 50.0)

    def test_w2_profile(self) -> None:
        profile = BenchmarkProfile(
            name="W2",
            instruments=1000,
            pools=1000,
            updates_per_second=20000,
        )
        self.assertEqual(profile.instruments, 1000)


class BenchmarkHarnessTest(unittest.TestCase):
    def test_run_benchmark(self) -> None:
        harness = BenchmarkHarness()
        result = harness.run_state_update_benchmark(update_count=100)
        self.assertEqual(result.total_events, 100)
        self.assertGreater(result.duration_seconds, 0)
        self.assertGreater(result.events_per_second, 0)

    def test_meets_slo_pass(self) -> None:
        harness = BenchmarkHarness()
        result = BenchmarkResult(
            profile="W1",
            screen_p95_ms=40.0,
            screen_p99_ms=150.0,
        )
        meets, failures = harness.meets_slo(result)
        self.assertTrue(meets)
        self.assertEqual(len(failures), 0)

    def test_meets_slo_fail(self) -> None:
        harness = BenchmarkHarness()
        result = BenchmarkResult(
            profile="W1",
            screen_p95_ms=60.0,
            screen_p99_ms=250.0,
        )
        meets, failures = harness.meets_slo(result)
        self.assertFalse(meets)
        self.assertGreater(len(failures), 0)


class GoldenCasesTest(unittest.TestCase):
    def test_all_golden_cases_pass(self) -> None:
        results = run_golden_cases()
        for result in results:
            self.assertTrue(result.passed, f"{result.case_id}: {result.message}")

    def test_t01(self) -> None:
        results = run_golden_cases()
        t01 = [r for r in results if r.case_id == "T01"][0]
        self.assertTrue(t01.passed)

    def test_t04(self) -> None:
        results = run_golden_cases()
        t04 = [r for r in results if r.case_id == "T04"][0]
        self.assertTrue(t04.passed)
        self.assertEqual(t04.actual, Decimal("2.80"))


if __name__ == "__main__":
    unittest.main()
