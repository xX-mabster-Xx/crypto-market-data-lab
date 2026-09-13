from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from market_data_lab.live_common import ArrivalSidecar
from market_data_lab.live_common import configure_process_network_route


class LiveCommonTest(unittest.TestCase):
    def test_arrival_sidecar_persists_callback_clock_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "arrivals.jsonl"
            sidecar = ArrivalSidecar(path)
            sidecar.open()
            sidecar.write(
                data_type="order_book_deltas",
                instrument_id="SOLUSDT-LINEAR.BYBIT",
                ts_event_ns=10,
                ts_init_ns=20,
                records=3,
                sequence=7,
            )
            sidecar.close()

            row = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(row["ts_event_ns"], 10)
            self.assertEqual(row["ts_init_ns"], 20)
            self.assertEqual(row["records"], 3)
            self.assertEqual(row["sequence"], 7)
            self.assertGreater(row["callback_monotonic_ns"], 0)
            self.assertEqual(sidecar.summary()["records"], 1)

    def test_direct_mode_ignores_ambient_proxy_for_this_process(self) -> None:
        with patch.dict(
            os.environ,
            {"HTTP_PROXY": "socks5://127.0.0.1:2060", "ALL_PROXY": "socks5://127.0.0.1:2060"},
            clear=False,
        ):
            route = configure_process_network_route(None)

            self.assertEqual(route["mode"], "direct")
            self.assertIn("HTTP_PROXY", route["ambient_proxy_variables_ignored"])
            self.assertNotIn("HTTP_PROXY", os.environ)
            self.assertNotIn("ALL_PROXY", os.environ)

    def test_explicit_proxy_preserves_environment(self) -> None:
        with patch.dict(os.environ, {"HTTPS_PROXY": "http://proxy.example:8080"}, clear=False):
            route = configure_process_network_route("http://explicit.example:8080")

            self.assertEqual(route["mode"], "explicit_proxy")
            self.assertEqual(os.environ["HTTPS_PROXY"], "http://proxy.example:8080")


if __name__ == "__main__":
    unittest.main()
