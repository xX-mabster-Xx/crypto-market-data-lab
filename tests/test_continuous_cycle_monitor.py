from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path

from market_data_lab.account_fee_audit import SpotFeeRate
from market_data_lab.cex_book_streams import BitgetBooks50Stream
from market_data_lab.cex_book_streams import BinancePartialDepthStream
from market_data_lab.cex_book_streams import BybitOrderBookStream
from market_data_lab.cex_book_streams import OkxBooks5Stream
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import CycleMarket
from market_data_lab.continuous_cycle_monitor import record_continuous_cycle_monitor
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.solana_realtime_scanner import CexTopOfBookEvent


MARKET = CycleMarket(
    name="TEST_CONTINUOUS",
    provider="TEST_PROVIDER",
    chain="base",
    dex_pair="TEST/USDC",
    cex_symbol="TESTUSDC",
    cex_base_symbol="TEST",
    quote_symbol="USDC",
    asset_equivalence="test",
)


def _book(symbol: str = "TESTUSDC") -> BookSnapshot:
    now_realtime = time.time_ns()
    now_monotonic = time.monotonic_ns()
    return BookSnapshot(
        symbol=symbol,
        category="spot",
        status="ok",
        error=None,
        bids=((Decimal("102"), Decimal("10")),),
        asks=((Decimal("103"), Decimal("10")),),
        exchange_system_time_ms=None,
        matching_engine_time_ms=None,
        update_id=1,
        cross_sequence=1,
        response=TimedResponse(
            payload=None,
            error=None,
            sent_realtime_ns=now_realtime,
            received_realtime_ns=now_realtime,
            sent_monotonic_ns=now_monotonic,
            received_monotonic_ns=now_monotonic,
        ),
        source="test",
    )


class _FakeStream:
    symbols = ("TESTUSDC",)

    def __init__(self) -> None:
        self.book = _book()
        self._updates: asyncio.Queue[BookSnapshot] = asyncio.Queue()

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def next_update(self) -> BookSnapshot:
        return await self._updates.get()

    def nearest_snapshot(self, symbol: str, target_realtime_ns: int) -> BookSnapshot | None:
        return self.book if symbol == "TESTUSDC" else None


class _FakeProvider:
    name = "TEST_PROVIDER"

    async def quote_round(self, round_id: int, notionals: list[Decimal]) -> list[dict[str, object]]:
        await asyncio.sleep(0.002)
        received = time.time_ns()
        return [
            {
                "schema_version": 1,
                "round_id": round_id,
                "status": "ok",
                "direction": "buy_base",
                "requested_notional_quote": "100",
                "base_amount": "1",
                "quote_amount": "100",
                "average_price_quote_per_base": "100",
                "response_received_realtime_ns": received,
                "request_rtt_ms": 1,
            },
            {
                "schema_version": 1,
                "round_id": round_id,
                "status": "ok",
                "direction": "sell_base",
                "requested_notional_quote": "100",
                "base_amount": "1",
                "quote_amount": "100",
                "average_price_quote_per_base": "100",
                "response_received_realtime_ns": received,
                "request_rtt_ms": 1,
            },
        ]

    def config(self) -> dict[str, object]:
        return {"provider": self.name, "api_credentials_used": False}


class CexBookStreamParserTest(unittest.TestCase):
    def test_bybit_applies_snapshot_then_delta(self) -> None:
        stream = BybitOrderBookStream(("BTCUSDT",), timeout_seconds=1, proxy_url=None)
        stream._handle_raw(
            json.dumps(
                {
                    "topic": "orderbook.50.BTCUSDT",
                    "type": "snapshot",
                    "ts": 10,
                    "seq": 3,
                    "data": {"s": "BTCUSDT", "b": [["100", "2"]], "a": [["101", "3"]], "u": 4, "cts": 9},
                },
            ),
        )
        stream._handle_raw(
            json.dumps(
                {
                    "topic": "orderbook.50.BTCUSDT",
                    "type": "delta",
                    "ts": 11,
                    "seq": 4,
                    "data": {"s": "BTCUSDT", "b": [["100", "0"], ["99", "5"]], "a": [["101", "2"]], "u": 5, "cts": 10},
                },
            ),
        )

        book = stream.nearest_snapshot("BTCUSDT", time.time_ns())
        self.assertIsNotNone(book)
        assert book is not None
        self.assertEqual(book.bids, ((Decimal("99"), Decimal("5")),))
        self.assertEqual(book.asks, ((Decimal("101"), Decimal("2")),))
        self.assertEqual(book.update_id, 5)

    def test_binance_and_okx_parse_public_snapshot_shapes(self) -> None:
        binance = BinancePartialDepthStream(("BTCUSDT",), timeout_seconds=1, proxy_url=None)
        binance._handle_raw(
            json.dumps(
                {"stream": "btcusdt@depth20@100ms", "data": {"E": 10, "lastUpdateId": 7, "bids": [["100", "1"]], "asks": [["101", "1"]]}},
            ),
        )
        okx = OkxBooks5Stream(("BTC-USDT",), timeout_seconds=1, proxy_url=None)
        okx._handle_raw(
            json.dumps(
                {"arg": {"channel": "books5", "instId": "BTC-USDT"}, "data": [{"ts": "11", "seqId": 8, "bids": [["100", "1", "0", "1"]], "asks": [["101", "1", "0", "1"]]}]},
            ),
        )

        self.assertEqual(binance.nearest_snapshot("BTCUSDT", time.time_ns()).source, "websocket_partial_depth_20_100ms")
        self.assertEqual(okx.nearest_snapshot("BTC-USDT", time.time_ns()).source, "websocket_books5")

    def test_bitget_v3_books50_snapshot_and_text_pong(self) -> None:
        bitget = BitgetBooks50Stream(("BTCUSDT",), timeout_seconds=1, proxy_url=None)
        bitget._handle_raw("pong")
        bitget._handle_raw(
            json.dumps(
                {
                    "arg": {"instType": "spot", "topic": "books50", "symbol": "BTCUSDT"},
                    "action": "snapshot",
                    "data": [
                        {
                            "b": [["100", "2"]],
                            "a": [["101", "3"]],
                            "seq": "17",
                            "pseq": "0",
                            "ts": "12",
                        },
                    ],
                },
            ),
        )

        book = bitget.nearest_snapshot("BTCUSDT", time.time_ns())
        self.assertIsNotNone(book)
        assert book is not None
        self.assertEqual(book.source, "websocket_books50_v3")
        self.assertEqual(book.bids, ((Decimal("100"), Decimal("2")),))
        self.assertEqual(book.update_id, 17)
        self.assertEqual(book.cross_sequence, 0)
        top = CexTopOfBookEvent.from_book(venue="BITGET", book=book)
        self.assertEqual(top.best_bid, Decimal("100"))
        self.assertEqual(top.best_bid_size, Decimal("2"))
        self.assertEqual(top.best_ask, Decimal("101"))


class ContinuousMonitorTest(unittest.TestCase):
    def test_keeps_raw_data_in_memory_and_persists_only_candidate_lifecycle(self) -> None:
        async def run(output: Path) -> dict[str, object]:
            return await record_continuous_cycle_monitor(
                [MARKET],
                {"TEST_PROVIDER": _FakeProvider()},
                notionals=[Decimal("100")],
                duration_seconds=0.025,
                cex_venues=("BYBIT",),
                cex_taker_fees={"BYBIT": Decimal("0")},
                network_cost_floors={"base": Decimal("0")},
                max_response_skew_ms=Decimal("1_000"),
                max_dex_cache_age_ms=Decimal("1_000"),
                output_directory=output,
                proxy_url=None,
                timeout_seconds=1,
                stats_flush_seconds=0.01,
                auxiliary_provider_min_round_intervals={"TEST": 0.1},
                cex_streams={"BYBIT": _FakeStream()},
                account_fee_rates={
                    ("BYBIT", "TESTUSDC"): SpotFeeRate(
                        venue="BYBIT",
                        symbol="TESTUSDC",
                        maker_buy_bps=Decimal("0"),
                        maker_sell_bps=Decimal("0"),
                        taker_buy_bps=Decimal("0"),
                        taker_sell_bps=Decimal("0"),
                        account_verified=True,
                        source="test_account_fee_api",
                    ),
                },
            )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "continuous"
            manifest = asyncio.run(run(output))
            stats = json.loads((output / "stats.json").read_text())
            events = [json.loads(line) for line in (output / "candidate_events.jsonl").read_text().splitlines()]

            self.assertEqual(manifest["status"], "completed")
            self.assertFalse((output / "recent.jsonl").exists())
            self.assertFalse(stats["raw_market_data_persisted"])
            self.assertGreater(stats["cycle_observations"], 0)
            self.assertEqual(stats["candidate_event_persistence"]["format"], "compact_candidate_lifecycle_v1")
            self.assertEqual(stats["candidate_event_persistence"]["persisted"], 2)
            self.assertEqual(
                manifest["dex"]["auxiliary_provider_min_round_intervals_seconds"],
                {"TEST": 0.1},
            )
            self.assertGreater(stats["positive_with_account_verified_fee_observations"], 0)
            self.assertEqual([event["event"] for event in events], ["candidate_started", "candidate_closed"])
            self.assertTrue(events[0]["best_cycle"]["cex_fee_account_verified"])
            self.assertEqual(events[0]["best_cycle"]["cex_fee_source"], "test_account_fee_api")
            self.assertNotIn("current_cycle", events[0])

    def test_fallback_fee_observations_are_not_persisted_as_account_confirmed_candidates(self) -> None:
        async def run(output: Path) -> dict[str, object]:
            return await record_continuous_cycle_monitor(
                [MARKET],
                {"TEST_PROVIDER": _FakeProvider()},
                notionals=[Decimal("100")],
                duration_seconds=0.025,
                cex_venues=("BYBIT",),
                cex_taker_fees={"BYBIT": Decimal("0")},
                network_cost_floors={"base": Decimal("0")},
                max_response_skew_ms=Decimal("1_000"),
                max_dex_cache_age_ms=Decimal("1_000"),
                output_directory=output,
                proxy_url=None,
                timeout_seconds=1,
                stats_flush_seconds=0.01,
                auxiliary_provider_min_round_intervals={"TEST": 0.1},
                cex_streams={"BYBIT": _FakeStream()},
            )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "continuous-fallback"
            manifest = asyncio.run(run(output))
            stats = json.loads((output / "stats.json").read_text())
            events = (output / "candidate_events.jsonl").read_text().splitlines()

            self.assertEqual(manifest["status"], "completed")
            self.assertGreater(stats["positive_after_minimum_network_observations"], 0)
            self.assertEqual(stats["positive_with_account_verified_fee_observations"], 0)
            self.assertEqual(stats["candidate_event_persistence"]["persisted"], 0)
            self.assertEqual(events, [])
            fee = manifest["cex"]["account_fee_audit"]["matching_effective_rates_by_symbol"][
                "BYBIT:TESTUSDC"
            ]
            self.assertFalse(fee["account_verified"])


if __name__ == "__main__":
    unittest.main()
