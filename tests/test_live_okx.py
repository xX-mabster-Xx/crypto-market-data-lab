from __future__ import annotations

import unittest

from market_data_lab.clock_sync import LocalClockReading
from market_data_lab.clock_sync import calculate_clock_sample
from market_data_lab.live_common import _replay_book
from market_data_lab.live_okx import parse_open_interest_response
from nautilus_trader.model import BookAction
from nautilus_trader.model import BookOrder
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import OrderBookDelta
from nautilus_trader.model import OrderSide
from nautilus_trader.model import Price
from nautilus_trader.model import Quantity
from nautilus_trader.model import RecordFlag


def _delta(price: str, sequence: int, flags: int) -> OrderBookDelta:
    return OrderBookDelta(
        InstrumentId.from_str("BTC-USDT-SWAP.OKX"),
        BookAction.ADD,
        BookOrder(
            OrderSide.BUY,
            Price.from_str(price),
            Quantity.from_str("1"),
            0,
        ),
        flags,
        sequence,
        sequence,
        sequence,
    )


class OKXHelpersTest(unittest.TestCase):
    def test_clock_offset_uses_rtt_midpoint(self) -> None:
        sample = calculate_clock_sample(
            LocalClockReading(1_000_000_000, 5_000_000_000),
            LocalClockReading(1_020_000_000, 5_020_000_000),
            server_time_ns=1_015_000_000,
            server_time_resolution_ns=1_000_000,
            venue="OKX",
            endpoint="GET /api/v5/public/time",
        )

        self.assertEqual(sample["local_midpoint_ns"], 1_010_000_000)
        self.assertEqual(sample["rtt_ms"], 20.0)
        self.assertEqual(sample["offset_ms"], 5.0)
        self.assertEqual(sample["network_uncertainty_bound_ms"], 10.0)
        self.assertEqual(sample["total_uncertainty_bound_ms"], 11.0)

    def test_parse_open_interest_response(self) -> None:
        parsed = parse_open_interest_response(
            {
                "code": "0",
                "data": [
                    {
                        "instId": "BTC-USDT-SWAP",
                        "instType": "SWAP",
                        "oi": "12.5",
                        "oiCcy": "125",
                        "oiUsd": "10000",
                        "ts": "1234",
                    },
                ],
            },
            "BTC-USDT-SWAP",
        )

        self.assertEqual(parsed["open_interest_contracts"], "12.5")
        self.assertEqual(parsed["exchange_ts_ms"], 1234)

    def test_second_adjacent_okx_snapshot_replaces_first(self) -> None:
        snapshot = RecordFlag.F_SNAPSHOT.value
        last = RecordFlag.F_LAST.value
        records = [
            _delta("100", 1, snapshot | last),
            _delta("99", 2, snapshot | last),
        ]

        book = _replay_book(records, "BTC-USDT-SWAP.OKX")

        self.assertEqual(str(book.best_bid_price()), "99")
        self.assertEqual(len(book.bids()), 1)


if __name__ == "__main__":
    unittest.main()
