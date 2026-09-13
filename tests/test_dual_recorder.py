from __future__ import annotations

import unittest

from market_data_lab.dual_recorder import calculate_overlaps
from market_data_lab.dual_recorder import extract_book_window


def _manifest(instrument_id: str, first: int, last: int) -> dict[str, object]:
    return {
        "actor_stats": {
            "streams": {
                "order_book_deltas": {
                    instrument_id: {
                        "first_init_ns": first,
                        "last_init_ns": last,
                    },
                },
            },
        },
    }


class DualRecorderTest(unittest.TestCase):
    def test_extract_book_window_rejects_missing_or_reversed_window(self) -> None:
        self.assertIsNone(extract_book_window({}, "SOLUSDT-LINEAR.BYBIT"))
        reversed_manifest = _manifest("SOLUSDT-LINEAR.BYBIT", 20, 10)
        self.assertIsNone(
            extract_book_window(reversed_manifest, "SOLUSDT-LINEAR.BYBIT"),
        )

    def test_overlap_uses_intersection_of_actual_arrival_windows(self) -> None:
        bybit = _manifest("SOLUSDT-LINEAR.BYBIT", 1_000_000_000, 11_000_000_000)
        okx = _manifest("SOL-USDT-SWAP.OKX", 2_000_000_000, 10_000_000_000)

        overlap = calculate_overlaps(bybit, okx, ["SOL"], 10.0)["SOL"]

        self.assertEqual(overlap["status"], "ok")
        self.assertEqual(overlap["overlap_start_ns"], 2_000_000_000)
        self.assertEqual(overlap["overlap_end_ns"], 10_000_000_000)
        self.assertEqual(overlap["overlap_seconds"], 8.0)
        self.assertEqual(overlap["requested_duration_ratio"], 0.8)


if __name__ == "__main__":
    unittest.main()
