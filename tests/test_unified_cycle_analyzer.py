from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path

from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.solana_realtime_scanner import CexBookStateSource
from market_data_lab.solana_realtime_scanner import CexStreamConfig
from market_data_lab.unified_cycle_analyzer import UnifiedCycleAnalyzer


def _book(*, symbol: str, now_realtime_ns: int, now_monotonic_ns: int) -> BookSnapshot:
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
            sent_realtime_ns=now_realtime_ns,
            received_realtime_ns=now_realtime_ns,
            sent_monotonic_ns=now_monotonic_ns,
            received_monotonic_ns=now_monotonic_ns,
        ),
        source="test",
    )


class UnifiedCycleAnalyzerTest(unittest.IsolatedAsyncioTestCase):
    async def test_records_bounded_modelled_lifecycle_without_requerying_sources(self) -> None:
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        source._latest_books["SOLUSDC"] = _book(  # noqa: SLF001 - intentional current-book fixture
            symbol="SOLUSDC",
            now_realtime_ns=now_realtime_ns,
            now_monotonic_ns=now_monotonic_ns,
        )
        market = MARKETS["SOL_SOLANA_RAYDIUM"]
        quote = ExactInputQuote(
            provider=market.provider,
            chain="solana",
            protocol="test",
            source_kind="test",
            pair="SOL/USDC",
            direction="buy_base",
            round_id=1,
            requested_notional_quote=Decimal("100"),
            reference_notional_usdt=Decimal("100"),
            base_amount=Decimal("1"),
            quote_amount=Decimal("100"),
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            average_price_quote_per_base=Decimal("100"),
            fee_bps=Decimal("20"),
            request_rtt_ms=5,
            status="ok",
            error=None,
            response_received_realtime_ns=now_realtime_ns,
            response_received_monotonic_ns=now_monotonic_ns,
            block_number=None,
        )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output,
                cex_sources=(source,),
                coalesce_interval_ms=1,
            )
            await analyzer.handle_event(
                MarketEvent(
                    source="dexquote:RAYDIUM",
                    key="test",
                    kind="exact_input_quote",
                    value=quote,
                    summary={},
                    received_realtime_ns=now_realtime_ns,
                    received_monotonic_ns=now_monotonic_ns,
                ),
            )
            await asyncio.sleep(0.03)
            snapshot = analyzer.snapshot()
            await analyzer.close()

            events = [
                json.loads(line)
                for line in (output / "cycle_analysis" / "candidate_events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertGreater(snapshot["counts"]["cycle_evaluations"], 0)
            self.assertEqual(events[0]["event"], "candidate_started")
            self.assertFalse(events[0]["execution_ready"])
            self.assertFalse(events[0]["best_cycle"]["cex_fee_account_verified"])
            self.assertEqual(events[-1]["event"], "candidate_closed")
            self.assertFalse((output / "raw.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
