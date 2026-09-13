from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.funding_model import project_common_rate_discrete_funding


class FundingModelTest(unittest.TestCase):
    def test_known_next_hourly_event_is_projected_once_in_one_hour_horizon(self) -> None:
        now_ns = 1_000_000_000_000
        projection = project_common_rate_discrete_funding(
            rate=Decimal("0.001"),
            rate_kind="next_hourly_rate",
            interval_minutes=60,
            next_event_time_ms=2_800_000,
            reference_price=Decimal("100"),
            reference_price_kind="venue_oracle",
            quantity=Decimal("2"),
            side="short",
            now_realtime_ns=now_ns,
            horizon_hours=Decimal("1"),
        )

        self.assertEqual(projection.normalized_cashflow_per_hour, Decimal("0.2"))
        self.assertEqual(projection.scheduled_cashflow_for_horizon, Decimal("0.2"))
        self.assertEqual(projection.scheduled_event_times_ms, (2_800_000,))
        self.assertEqual(projection.quality, "projected")
        self.assertIsNone(projection.reason)

    def test_horizon_does_not_fractionally_accrue_a_discrete_event(self) -> None:
        now_ns = 1_000_000_000_000
        projection = project_common_rate_discrete_funding(
            rate=Decimal("0.001"),
            rate_kind="next_hourly_rate",
            interval_minutes=60,
            next_event_time_ms=2_800_000,
            reference_price=Decimal("100"),
            reference_price_kind="venue_oracle",
            quantity=Decimal("2"),
            side="short",
            now_realtime_ns=now_ns,
            horizon_hours=Decimal("0.1"),
        )

        self.assertEqual(projection.normalized_cashflow_per_hour, Decimal("0.2"))
        self.assertEqual(projection.scheduled_cashflow_for_horizon, Decimal("0"))
        self.assertEqual(projection.scheduled_event_times_ms, ())

    def test_rate_without_known_next_event_is_not_horizon_pnl(self) -> None:
        projection = project_common_rate_discrete_funding(
            rate=Decimal("0.001"),
            rate_kind="next_hourly_rate",
            interval_minutes=60,
            next_event_time_ms=None,
            reference_price=Decimal("100"),
            reference_price_kind="venue_oracle",
            quantity=Decimal("2"),
            side="short",
            now_realtime_ns=1_000_000_000_000,
            horizon_hours=Decimal("1"),
        )

        self.assertEqual(projection.normalized_cashflow_per_hour, Decimal("0.2"))
        self.assertIsNone(projection.scheduled_cashflow_for_horizon)
        self.assertEqual(projection.quality, "unknown")
        self.assertEqual(projection.reason, "next_funding_event_unknown")

    def test_current_rate_is_not_silently_treated_as_next_event_rate(self) -> None:
        projection = project_common_rate_discrete_funding(
            rate=Decimal("0.001"),
            rate_kind="current_hourly_rate",
            interval_minutes=60,
            next_event_time_ms=2_800_000,
            reference_price=Decimal("100"),
            reference_price_kind="venue_mark",
            quantity=Decimal("2"),
            side="long",
            now_realtime_ns=1_000_000_000_000,
            horizon_hours=Decimal("1"),
        )

        self.assertEqual(projection.normalized_cashflow_per_hour, Decimal("-0.2"))
        self.assertIsNone(projection.scheduled_cashflow_for_horizon)
        self.assertEqual(projection.reason, "funding_rate_not_explicitly_for_next_event")


if __name__ == "__main__":
    unittest.main()
