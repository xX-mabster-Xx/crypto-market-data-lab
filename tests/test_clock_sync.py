from __future__ import annotations

import unittest

from market_data_lab.clock_sync import ClockOffsetEstimator
from market_data_lab.clock_sync import LocalClockContinuity
from market_data_lab.clock_sync import LocalClockReading
from market_data_lab.clock_sync import calculate_clock_sample


def _sample(rtt_ms: int, offset_ms: int, sequence: int) -> dict[str, object]:
    request_realtime_ns = 1_000_000_000 + sequence * 1_000_000_000
    request_monotonic_ns = 10_000_000_000 + sequence * 1_000_000_000
    rtt_ns = rtt_ms * 1_000_000
    request = LocalClockReading(request_realtime_ns, request_monotonic_ns)
    receive = LocalClockReading(
        request_realtime_ns + rtt_ns,
        request_monotonic_ns + rtt_ns,
    )
    return calculate_clock_sample(
        request,
        receive,
        server_time_ns=request_realtime_ns + rtt_ns // 2 + offset_ms * 1_000_000,
        server_time_resolution_ns=1,
        venue="TEST",
        endpoint="GET /time",
    )


class ClockSyncTest(unittest.TestCase):
    def test_realtime_step_does_not_change_monotonic_rtt(self) -> None:
        sample = calculate_clock_sample(
            LocalClockReading(1_000_000_000, 5_000_000_000, 100),
            LocalClockReading(1_030_000_000, 5_020_000_000, 120),
            server_time_ns=1_015_000_000,
            server_time_resolution_ns=1_000_000,
            venue="TEST",
            endpoint="GET /time",
        )

        self.assertEqual(sample["rtt_ms"], 20.0)
        self.assertEqual(sample["wall_monotonic_divergence_ms"], 10.0)
        self.assertEqual(sample["offset_ms"], 5.0)

    def test_estimator_rejects_high_delay_offset_outliers(self) -> None:
        estimator = ClockOffsetEstimator()
        for index, (rtt_ms, offset_ms) in enumerate(
            [(10, 2), (12, 3), (14, 4), (500, 100), (600, -100)],
        ):
            estimator.add(_sample(rtt_ms, offset_ms, index))

        summary = estimator.to_dict()

        self.assertEqual(summary["samples"], 5)
        self.assertEqual(summary["selected_low_rtt_samples"], 3)
        self.assertEqual(summary["low_rtt_cutoff_ms"], 14.0)
        self.assertEqual(summary["offset_estimate_ms"], 3.0)
        self.assertEqual(summary["selected_offset_mad_ms"], 1.0)

    def test_continuity_monitor_detects_realtime_step(self) -> None:
        monitor = LocalClockContinuity(step_threshold_ns=5_000_000)
        monitor.add(LocalClockReading(1_000_000_000, 10_000_000_000, 100))
        monitor.add(LocalClockReading(2_000_000_000, 11_000_000_000, 100))
        monitor.add(LocalClockReading(3_010_000_000, 12_000_000_000, 100))

        summary = monitor.to_dict()

        self.assertEqual(summary["suspected_discontinuities"], 1)
        self.assertEqual(summary["maximum_absolute_elapsed_divergence_ms"], 10.0)
        self.assertEqual(summary["events"][0]["elapsed_divergence_ms"], 10.0)


if __name__ == "__main__":
    unittest.main()
