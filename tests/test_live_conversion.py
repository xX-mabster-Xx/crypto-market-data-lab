from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from market_data_lab.live_bybit import _normalize_clear_delta
from market_data_lab.live_common import convert_streams
from nautilus_trader.common import Cache
from nautilus_trader.common import Clock
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import BookAction
from nautilus_trader.model import BookOrder
from nautilus_trader.model import OrderBookDelta
from nautilus_trader.model import OrderSide
from nautilus_trader.model import Price
from nautilus_trader.model import Quantity
from nautilus_trader.model import QuoteTick
from nautilus_trader.persistence import StreamingFeatherWriter


class LiveConversionTest(unittest.TestCase):
    def test_snapshot_clear_uses_instrument_precision(self) -> None:
        instrument_id = InstrumentId.from_str("BTCUSDT-LINEAR.BYBIT")
        clear = OrderBookDelta(
            instrument_id,
            BookAction.CLEAR,
            BookOrder(
                OrderSide.NO_ORDER_SIDE,
                Price.from_str("0"),
                Quantity.from_str("0"),
                0,
            ),
            32,
            123,
            1_000,
            1_100,
        )

        normalized = _normalize_clear_delta(clear, price_precision=2, size_precision=3)

        self.assertEqual(normalized.order.price.precision, 2)
        self.assertEqual(normalized.order.size.precision, 3)
        self.assertEqual(normalized.flags, clear.flags)
        self.assertEqual(normalized.sequence, clear.sequence)

    def test_completed_feather_stream_converts_and_round_trips(self) -> None:
        instrument_id = InstrumentId.from_str("BTCUSDT-LINEAR.BYBIT")
        first_quote = QuoteTick(
            instrument_id,
            Price.from_str("100.00"),
            Price.from_str("100.01"),
            Quantity.from_str("1.000"),
            Quantity.from_str("2.000"),
            1_000_000_000,
            1_002_000_000,
        )
        second_quote = QuoteTick(
            instrument_id,
            Price.from_str("100.01"),
            Price.from_str("100.02"),
            Quantity.from_str("1.500"),
            Quantity.from_str("2.500"),
            1_001_000_000,
            1_002_000_000,
        )
        actor_stats = {
            "streams": {
                "quotes": {
                    str(instrument_id): {
                        "messages": 2,
                        "records": 2,
                    },
                },
            },
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            catalog_root = Path(temporary_directory) / "catalog"
            stream_path = catalog_root / "live" / "test-run"
            stream_path.mkdir(parents=True)
            clock = Clock.new_test()
            clock.set_time(10_000_000_000)
            writer = StreamingFeatherWriter(
                path=str(stream_path),
                cache=Cache(),
                clock=clock,
                include_types=["quotes"],
            )
            writer.write(first_quote)
            writer.flush()
            clock.set_time(11_000_000_000)
            writer.write(second_quote)
            writer.close()

            result = convert_streams(catalog_root, "test-run", actor_stats, 100)

            self.assertEqual(result["conversion"]["quotes"]["status"], "converted")
            validation = result["validations"][f"quotes:{instrument_id}"]
            self.assertEqual(validation["status"], "ok")
            self.assertEqual(validation["actual"], 2)


if __name__ == "__main__":
    unittest.main()
