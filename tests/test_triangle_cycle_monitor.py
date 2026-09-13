from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path

from market_data_lab.account_fee_audit import SpotFeeRate
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.dex_quotes import Asset
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.triangle_cycle_monitor import TriangleAsset
from market_data_lab.triangle_cycle_monitor import TriangleMarket
from market_data_lab.triangle_cycle_monitor import TRIANGLE_MARKETS
from market_data_lab.triangle_cycle_monitor import calculate_triangle_cycle
from market_data_lab.triangle_cycle_monitor import record_triangle_cycle_monitor


def _book(symbol: str, *, bid: str, ask: str) -> BookSnapshot:
    now_realtime = time.time_ns()
    now_monotonic = time.monotonic_ns()
    return BookSnapshot(
        symbol=symbol,
        category="spot",
        status="ok",
        error=None,
        bids=((Decimal(bid), Decimal("10")),),
        asks=((Decimal(ask), Decimal("10")),),
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


BASE = TriangleAsset("A", Asset("A", "A", 6), "A", "test A")
QUOTE = TriangleAsset("B", Asset("B", "B", 6), "B", "test B")
MARKET = TriangleMarket(
    name="TEST_TRIANGLE",
    provider="TEST_PROVIDER",
    provider_kind="test",
    chain="solana",
    dex_pair="A/B",
    base=BASE,
    quote=QUOTE,
    asset_equivalence="test",
)


class _FakeStream:
    symbols = ("AUSDT", "BUSDT")

    def __init__(self) -> None:
        self.books = {
            "AUSDT": _book("AUSDT", bid="102", ask="103"),
            "BUSDT": _book("BUSDT", bid="99", ask="100"),
        }
        self._updates: asyncio.Queue[BookSnapshot] = asyncio.Queue()

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def next_update(self) -> BookSnapshot:
        return await self._updates.get()

    def nearest_snapshot(self, symbol: str, target_realtime_ns: int) -> BookSnapshot | None:
        return self.books.get(symbol)

    @property
    def available_symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self.books))

    @property
    def error(self) -> None:
        return None


class _FakeProvider:
    name = "TEST_PROVIDER"

    async def quote_round(self, round_id: int, notionals: list[Decimal]) -> list[dict[str, object]]:
        await asyncio.sleep(0.002)
        received = time.time_ns()
        return [
            {
                "round_id": round_id,
                "status": "ok",
                "direction": "buy_base",
                "requested_notional_quote": "1",
                "base_amount": "1",
                "quote_amount": "1",
                "average_price_quote_per_base": "1",
                "response_received_realtime_ns": received,
                "request_rtt_ms": 1,
            },
            {
                "round_id": round_id,
                "status": "ok",
                "direction": "sell_base",
                "requested_notional_quote": "1",
                "base_amount": "1",
                "quote_amount": "1",
                "average_price_quote_per_base": "1",
                "response_received_realtime_ns": received,
                "request_rtt_ms": 1,
            },
        ]

    def config(self) -> dict[str, object]:
        return {"provider": self.name, "api_credentials_used": False}


class TriangleMonitorTest(unittest.TestCase):
    def test_default_universe_has_expected_source_pair_count(self) -> None:
        self.assertEqual(len(TRIANGLE_MARKETS), 95)
        self.assertEqual(sum(market.chain == "solana" for market in TRIANGLE_MARKETS), 78)
        self.assertEqual(sum(market.chain == "ton" for market in TRIANGLE_MARKETS), 15)
        self.assertEqual(sum(market.chain in {"base", "polygon"} for market in TRIANGLE_MARKETS), 2)

    def test_triangle_walks_both_cex_books_and_applies_two_fees(self) -> None:
        dex_received = time.time_ns()
        base_book = _book("AUSDT", bid="102", ask="103")
        quote_book = _book("BUSDT", bid="99", ask="100")
        cycle = calculate_triangle_cycle(
            market=MARKET,
            dex_record={
                "round_id": 1,
                "direction": "buy_base",
                "requested_notional_quote": "1",
                "base_amount": "1",
                "quote_amount": "1",
                "average_price_quote_per_base": "1",
                "response_received_realtime_ns": dex_received,
                "request_rtt_ms": 1,
            },
            base_book=base_book,
            quote_book=quote_book,
            cex_taker_fee_bps=Decimal("0"),
            network_cost_floor_usdt=Decimal("0.1"),
            max_response_skew_ms=Decimal("1_000"),
            reference_notional_usdt=Decimal("100"),
            cex_venue="BYBIT",
        )

        self.assertEqual(cycle["cycle_direction"], "buy_cex_quote_dex_sell_cex_base")
        self.assertEqual(cycle["gross_pnl_quote"], "2")
        self.assertEqual(cycle["net_pnl_after_minimum_network_quote"], "1.9")
        self.assertTrue(cycle["positive_after_minimum_network"])

    def test_triangle_uses_symbol_specific_buy_and_sell_fee_rates(self) -> None:
        dex_received = time.time_ns()
        cycle = calculate_triangle_cycle(
            market=MARKET,
            dex_record={
                "round_id": 1,
                "direction": "buy_base",
                "requested_notional_quote": "1",
                "base_amount": "1",
                "quote_amount": "1",
                "average_price_quote_per_base": "1",
                "response_received_realtime_ns": dex_received,
                "request_rtt_ms": 1,
            },
            base_book=_book("AUSDT", bid="102", ask="103"),
            quote_book=_book("BUSDT", bid="99", ask="100"),
            cex_taker_fee_bps=Decimal("0"),
            cex_buy_taker_fee_bps=Decimal("100"),
            cex_sell_taker_fee_bps=Decimal("50"),
            cex_buy_fee_source="account_fee_api",
            cex_sell_fee_source="account_fee_api",
            cex_buy_fee_account_verified=True,
            cex_sell_fee_account_verified=True,
            network_cost_floor_usdt=Decimal("0"),
            max_response_skew_ms=Decimal("1_000"),
            reference_notional_usdt=Decimal("100"),
            cex_venue="BYBIT",
        )

        self.assertEqual(cycle["cex_buy_taker_fee_bps"], "100")
        self.assertEqual(cycle["cex_sell_taker_fee_bps"], "50")
        self.assertIsNone(cycle["cex_taker_fee_bps_per_leg"])
        self.assertTrue(cycle["cex_fee_account_verified"])
        self.assertTrue(cycle["candidate_eligible_with_account_verified_fee"])

    def test_monitor_persists_only_compact_candidate_lifecycle(self) -> None:
        async def run(output: Path) -> dict[str, object]:
            return await record_triangle_cycle_monitor(
                [MARKET],
                {"TEST_PROVIDER": _FakeProvider()},
                reference_notional_usdt=Decimal("100"),
                duration_seconds=0.03,
                cex_venues=("BYBIT",),
                cex_taker_fees={"BYBIT": Decimal("0")},
                network_cost_floors={"solana": Decimal("0")},
                max_response_skew_ms=Decimal("1_000"),
                max_dex_cache_age_ms=Decimal("1_000"),
                output_directory=output,
                proxy_url=None,
                timeout_seconds=1,
                stats_flush_seconds=0.01,
                cex_streams={"BYBIT": _FakeStream()},
                account_fee_rates={
                    ("BYBIT", "AUSDT"): SpotFeeRate(
                        venue="BYBIT",
                        symbol="AUSDT",
                        maker_buy_bps=Decimal("0"),
                        maker_sell_bps=Decimal("0"),
                        taker_buy_bps=Decimal("0"),
                        taker_sell_bps=Decimal("0"),
                        account_verified=True,
                        source="test_account_fee_api",
                    ),
                    ("BYBIT", "BUSDT"): SpotFeeRate(
                        venue="BYBIT",
                        symbol="BUSDT",
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
            output = Path(directory) / "triangle"
            manifest = asyncio.run(run(output))
            stats = json.loads((output / "stats.json").read_text())
            events = [
                json.loads(line)
                for line in (output / "candidate_events.jsonl").read_text().splitlines()
            ]

            self.assertEqual(manifest["status"], "completed")
            self.assertFalse(stats["raw_market_data_persisted"])
            self.assertGreater(stats["cycle_observations"], 0)
            self.assertGreaterEqual(len(events), 2)
            self.assertIn("cex_buy_symbol", events[0]["best_cycle"])
            self.assertTrue(events[0]["best_cycle"]["cex_fee_account_verified"])
            self.assertNotIn("dex_quote_service_metadata", events[0]["best_cycle"])

    def test_rate_limit_defer_only_slows_a_shared_pacer(self) -> None:
        async def run() -> float:
            pacer = AsyncRequestPacer(0.01)
            await pacer.defer(cooldown_seconds=0, minimum_interval_seconds=0.2)
            return pacer.minimum_interval_seconds

        self.assertEqual(asyncio.run(run()), 0.2)


if __name__ == "__main__":
    unittest.main()
