from __future__ import annotations

import unittest
from types import SimpleNamespace

from market_data_lab.live_bybit import parse_server_time_ns


class BybitHelpersTest(unittest.TestCase):
    def test_parse_server_time_object(self) -> None:
        response = SimpleNamespace(time_nano="1688639403423213947")

        self.assertEqual(parse_server_time_ns(response), 1_688_639_403_423_213_947)

    def test_parse_server_time_json(self) -> None:
        response = {"result": {"timeNano": "1688639403423213947"}}

        self.assertEqual(parse_server_time_ns(response), 1_688_639_403_423_213_947)


if __name__ == "__main__":
    unittest.main()
