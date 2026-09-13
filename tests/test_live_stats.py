from __future__ import annotations

import unittest

from market_data_lab.live_stats import BookSequenceMetric
from market_data_lab.live_stats import StreamMetric


class LiveStatsTest(unittest.TestCase):
    def test_stream_metric_counts_records_and_clock_delta(self) -> None:
        metric = StreamMetric()
        metric.add(ts_event=1_000_000_000, ts_init=1_002_000_000, records=4)
        metric.add(ts_event=1_010_000_000, ts_init=1_013_000_000, records=2)

        result = metric.to_dict()
        self.assertEqual(result["messages"], 2)
        self.assertEqual(result["records"], 6)
        self.assertEqual(result["max_interarrival_ms"], 11.0)
        self.assertEqual(result["observed_clock_delta"]["min_ms"], 2.0)
        self.assertEqual(result["observed_clock_delta"]["max_ms"], 3.0)

    def test_book_sequence_detects_gap_duplicate_and_regression(self) -> None:
        metric = BookSequenceMetric()
        metric.add(update_id=100, cross_sequence=1_000, snapshot=True)
        metric.add(update_id=101, cross_sequence=1_001, snapshot=False)
        metric.add(update_id=104, cross_sequence=1_002, snapshot=False)
        metric.add(update_id=104, cross_sequence=1_003, snapshot=False)
        metric.add(update_id=103, cross_sequence=999, snapshot=False)

        result = metric.to_dict()
        self.assertEqual(result["snapshots"], 1)
        self.assertEqual(result["update_id_gap_events"], 1)
        self.assertEqual(result["missing_update_ids"], 2)
        self.assertEqual(result["update_id_duplicates"], 1)
        self.assertEqual(result["update_id_regressions"], 1)
        self.assertEqual(result["cross_sequence_regressions"], 1)


if __name__ == "__main__":
    unittest.main()

