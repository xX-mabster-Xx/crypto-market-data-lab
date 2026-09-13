from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from market_data_lab.bybit_archive import import_archive


class BybitArchiveRoundTripTest(unittest.TestCase):
    def test_snapshot_delta_catalog_round_trip(self) -> None:
        messages = [
            {
                "type": "snapshot",
                "ts": 1_000,
                "data": {
                    "s": "SOLUSDT",
                    "b": [["10.00", "1.0"], ["9.99", "2.0"]],
                    "a": [["10.01", "1.5"], ["10.02", "2.5"]],
                    "seq": 1,
                },
            },
            {
                "type": "delta",
                "ts": 1_001,
                "data": {
                    "s": "SOLUSDT",
                    "b": [["10.00", "1.2"]],
                    "a": [["10.01", "0"]],
                    "seq": 2,
                },
            },
        ]

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            archive_path = root / "sample.zip"
            with ZipFile(archive_path, "w", ZIP_DEFLATED) as archive:
                payload = "".join(json.dumps(message) + "\n" for message in messages)
                archive.writestr("sample.data", payload)

            summary = import_archive(
                archive_path=archive_path,
                catalog_path=root / "catalog",
                limit=100,
                source_url=None,
            )

            self.assertEqual(summary.instrument_id, "SOLUSDT-LINEAR.BYBIT")
            self.assertEqual(summary.source_rows, 7)
            self.assertEqual(summary.stored_deltas, 7)
            self.assertEqual(summary.price_increment, "0.01")
            self.assertEqual(summary.size_increment, "0.1")
            self.assertEqual(summary.final_bid, "10.00")
            self.assertEqual(summary.final_ask, "10.02")
            self.assertEqual(summary.final_spread, "0.02")
            self.assertEqual(summary.timestamp_regressions, 0)
            self.assertEqual(summary.sequence_regressions, 0)
            self.assertTrue((root / "catalog" / "manifest.json").is_file())


if __name__ == "__main__":
    unittest.main()
