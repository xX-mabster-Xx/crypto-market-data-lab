"""Offline deterministic stress test and clean shutdown test."""

from __future__ import annotations

import asyncio
import gc
import tempfile
import time
import unittest
import warnings
from decimal import Decimal
from pathlib import Path
from typing import Any

from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RealtimeScanner
from market_data_lab.solana_realtime_scanner import CexBookStateSource
from market_data_lab.solana_realtime_scanner import CexStreamConfig
from market_data_lab.unified_cycle_analyzer import UnifiedCycleAnalyzer


def _book(*, symbol: str, now_realtime_ns: int, now_monotonic_ns: int) -> BookSnapshot:
    return BookSnapshot(
        symbol=symbol, category="spot", status="ok", error=None,
        bids=((Decimal("102"), Decimal("10")),),
        asks=((Decimal("103"), Decimal("10")),),
        exchange_system_time_ms=None, matching_engine_time_ms=None,
        update_id=1, cross_sequence=1,
        response=TimedResponse(
            payload=None, error=None,
            sent_realtime_ns=now_realtime_ns, received_realtime_ns=now_realtime_ns,
            sent_monotonic_ns=now_monotonic_ns, received_monotonic_ns=now_monotonic_ns,
        ),
        source="test",
    )


def _make_quote(*, now_realtime_ns: int, now_monotonic_ns: int, amount: Decimal, source_epoch: int = 0) -> ExactInputQuote:
    market = MARKETS["SOL_SOLANA_RAYDIUM"]
    return ExactInputQuote(
        provider=market.provider, chain="solana", protocol="test", source_kind="test",
        pair="SOL/USDC", direction="buy_base", round_id=1,
        requested_notional_quote=amount, reference_notional_usdt=amount,
        quote_slot_id="notional:" + str(amount),
        base_amount=Decimal("1"), quote_amount=Decimal("100"),
        input_symbol="USDC", output_symbol="SOL",
        input_amount_raw=100_000_000, output_amount_raw=1_000_000_000,
        average_price_quote_per_base=Decimal("100"), fee_bps=Decimal("20"),
        request_rtt_ms=5, status="ok", error=None,
        response_received_realtime_ns=now_realtime_ns,
        response_received_monotonic_ns=now_monotonic_ns,
        block_number=None, source_epoch=source_epoch,
    )


class DeterministicStressTest(unittest.IsolatedAsyncioTestCase):
    """Section 23.2: offline deterministic stress test."""

    async def test_stress_state_key_count_bounded_and_indexes_consistent(self) -> None:
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1, proxy_url=None,
        )
        source._latest_books["SOLUSDC"] = _book(
            symbol="SOLUSDC", now_realtime_ns=now_realtime_ns, now_monotonic_ns=now_monotonic_ns,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output, cex_sources=(source,),
                max_response_skew_ms=Decimal("1000"), coalesce_interval_ms=10,
            )
            num_updates = 10_000
            for i in range(num_updates):
                amount = Decimal(str(float(i) * 0.001 + 1))
                quote = _make_quote(
                    now_realtime_ns=now_realtime_ns + i,
                    now_monotonic_ns=now_monotonic_ns + i,
                    amount=amount,
                )
                await analyzer.handle_event(MarketEvent(
                    source="dexquote:RAYDIUM", key="test", kind="exact_input_quote",
                    value=quote, summary={},
                    received_realtime_ns=now_realtime_ns + i,
                    received_monotonic_ns=now_monotonic_ns + i,
                ))
            for i in range(1_000):
                book = _book(
                    symbol="SOLUSDC", now_realtime_ns=now_realtime_ns + i,
                    now_monotonic_ns=now_monotonic_ns + i,
                )
                source._latest_books["SOLUSDC"] = book
                await analyzer.handle_event(MarketEvent(
                    source="test", key="SOLUSDC", kind="order_book",
                    value=book, summary={},
                    received_realtime_ns=now_realtime_ns + i,
                    received_monotonic_ns=now_monotonic_ns + i,
                ))
            analyzer.handle_source_epoch_change("dexquote:RAYDIUM", old_epoch=0, new_epoch=1)
            analyzer.handle_source_epoch_change("dexquote:RAYDIUM", old_epoch=1, new_epoch=2)
            analyzer._wake.set()
            await asyncio.sleep(0.05)
            analyzer.snapshot()
            direct_keys = len(analyzer._direct_quotes)
            triangle_keys = len(analyzer._triangle_quotes)
            self.assertLess(direct_keys + triangle_keys, num_updates)
            self.assertEqual(direct_keys + triangle_keys, 0)
            for provider, keys in analyzer._quote_keys_by_direct_provider.items():
                self.assertEqual(
                    len(keys), sum(1 for k in analyzer._direct_quotes if k[0] == provider),
                )
            for provider, keys in analyzer._quote_keys_by_triangle_provider.items():
                self.assertEqual(
                    len(keys), sum(1 for k in analyzer._triangle_quotes if k[0] == provider),
                )
            snapshot = analyzer.snapshot()
            self.assertGreater(snapshot["counts"].get("direct_quote_events", 0), 0)
            await analyzer.close()

    async def test_stress_slow_consumer_cold_key_not_lost(self) -> None:
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1, proxy_url=None,
        )
        source._latest_books["SOLUSDC"] = _book(
            symbol="SOLUSDC", now_realtime_ns=now_realtime_ns, now_monotonic_ns=now_monotonic_ns,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output, cex_sources=(source,), coalesce_interval_ms=1,
            )
            hot_quote = _make_quote(
                now_realtime_ns=now_realtime_ns, now_monotonic_ns=now_monotonic_ns,
                amount=Decimal("100"),
            )
            for i in range(500):
                await analyzer.handle_event(MarketEvent(
                    source="dexquote:RAYDIUM", key="test", kind="exact_input_quote",
                    value=hot_quote, summary={},
                    received_realtime_ns=now_realtime_ns + i,
                    received_monotonic_ns=now_monotonic_ns + i,
                ))
            cold_quote = _make_quote(
                now_realtime_ns=now_realtime_ns, now_monotonic_ns=now_monotonic_ns,
                amount=Decimal("200"),
            )
            await analyzer.handle_event(MarketEvent(
                source="dexquote:RAYDIUM", key="test", kind="exact_input_quote",
                value=cold_quote, summary={},
                received_realtime_ns=now_realtime_ns + 500,
                received_monotonic_ns=now_monotonic_ns + 500,
            ))
            analyzer._wake.set()
            await asyncio.sleep(0.05)
            cold_key = analyzer._quote_key(cold_quote)
            self.assertIsNotNone(cold_key)
            self.assertIn(cold_key, analyzer._direct_quotes)
            await analyzer.close()


class CleanShutdownTest(unittest.IsolatedAsyncioTestCase):
    """Section 23.3: clean shutdown without hanging or pending tasks."""

    async def test_scanner_stop_does_not_hang(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"

            class _SlowSource:
                name = "test:slow"

                def describe(self) -> dict[str, Any]:
                    return {"source": self.name, "mode": "test"}

                async def run(self, publish: Any, stop_event: asyncio.Event) -> None:
                    now = time.time_ns()
                    for i in range(10):
                        await publish(MarketEvent(
                            source=self.name, key="test:slow-key", kind="test_state",
                            value={"i": i}, summary={"safe": "summary"},
                            received_realtime_ns=now,
                            received_monotonic_ns=time.monotonic_ns(),
                            chain_position=i,
                        ))
                        await asyncio.sleep(0.005)
                        if stop_event.is_set():
                            return

            scanner = RealtimeScanner(
                sources=[_SlowSource()], output_directory=output,
                retention_seconds=60, max_events_per_key=8,
                event_bus_capacity=4, status_flush_seconds=0.01,
            )
            run_task = asyncio.create_task(scanner.run())
            await asyncio.sleep(0.03)
            scanner.stop_event.set()
            await asyncio.wait_for(run_task, timeout=5.0)
            pending = [
                task for task in asyncio.all_tasks()
                if task is not asyncio.current_task() and not task.done()
            ]
            self.assertEqual(pending, [], "Tasks left pending after shutdown")

    async def test_analyzer_close_does_not_hang(self) -> None:
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1, proxy_url=None,
        )
        source._latest_books["SOLUSDC"] = _book(
            symbol="SOLUSDC", now_realtime_ns=now_realtime_ns, now_monotonic_ns=now_monotonic_ns,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output, cex_sources=(source,), coalesce_interval_ms=1,
            )
            market = MARKETS["SOL_SOLANA_RAYDIUM"]
            quote = ExactInputQuote(
                provider=market.provider, chain="solana", protocol="test", source_kind="test",
                pair="SOL/USDC", direction="buy_base", round_id=1,
                requested_notional_quote=Decimal("100"), reference_notional_usdt=Decimal("100"),
                quote_slot_id="notional:100:buy_base",
                base_amount=Decimal("1"), quote_amount=Decimal("100"),
                input_symbol="USDC", output_symbol="SOL",
                input_amount_raw=100_000_000, output_amount_raw=1_000_000_000,
                average_price_quote_per_base=Decimal("100"), fee_bps=Decimal("20"),
                request_rtt_ms=5, status="ok", error=None,
                response_received_realtime_ns=now_realtime_ns,
                response_received_monotonic_ns=now_monotonic_ns,
                block_number=None,
            )
            await analyzer.handle_event(MarketEvent(
                source="dexquote:RAYDIUM", key="test", kind="exact_input_quote",
                value=quote, summary={},
                received_realtime_ns=now_realtime_ns,
                received_monotonic_ns=now_monotonic_ns,
            ))
            await asyncio.sleep(0.05)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await analyzer.close()
                gc.collect()
            pending = [
                task for task in asyncio.all_tasks()
                if task is not asyncio.current_task() and not task.done()
            ]
            self.assertEqual(pending, [], "Tasks left pending after analyzer shutdown")


if __name__ == "__main__":
    unittest.main()
