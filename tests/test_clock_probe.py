from __future__ import annotations

import unittest

from market_data_lab.clock_probe import parse_server_time_ns


class ClockProbeTest(unittest.TestCase):
    def test_parse_bybit_nanoseconds(self) -> None:
        payload = {
            "retCode": 0,
            "result": {"timeNano": "1688639403423213947"},
        }

        self.assertEqual(
            parse_server_time_ns("BYBIT", payload),
            1_688_639_403_423_213_947,
        )

    def test_parse_okx_milliseconds(self) -> None:
        payload = {"code": "0", "data": [{"ts": "1597026383085"}]}

        self.assertEqual(
            parse_server_time_ns("OKX", payload),
            1_597_026_383_085_000_000,
        )


if __name__ == "__main__":
    unittest.main()
