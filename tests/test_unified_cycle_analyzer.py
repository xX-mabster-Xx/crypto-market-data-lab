from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path

from dataclasses import replace
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.solana_realtime_scanner import CexBookStateSource
from market_data_lab.solana_realtime_scanner import CexStreamConfig
from market_data_lab.unified_cycle_analyzer import UnifiedCycleAnalyzer
from market_data_lab.unified_cycle_analyzer import _ActiveCandidate


def _good_quote(*, now_realtime_ns: int, now_monotonic_ns: int) -> ExactInputQuote:
    """Build a valid buy_base ExactInputQuote for the SOL/SOLANA_RAYDIUM market."""
    market = MARKETS["SOL_SOLANA_RAYDIUM"]
    return ExactInputQuote(
        provider=market.provider,
        chain="solana",
        protocol="test",
        source_kind="test",
        pair="SOL/USDC",
        direction="buy_base",
        round_id=1,
        requested_notional_quote=Decimal("100"),
        reference_notional_usdt=Decimal("100"),
        quote_slot_id="notional:100:buy_base",
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
    async def test_candidate_idle_lifecycle_uses_monotonic_not_wall_clock(self) -> None:
        clocks = {"realtime": 10_000_000_000, "monotonic": 1_000_000_000}

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output,
                cex_sources=(),
                max_response_skew_ms=Decimal("1000"),
                monotonic_ns=lambda: clocks["monotonic"],
                realtime_ns=lambda: clocks["realtime"],
            )
            state = _ActiveCandidate(
                key="candidate",
                analysis_kind="direct_inventory",
                started_realtime_ns=clocks["realtime"],
                started_monotonic_ns=clocks["monotonic"],
                started_at="start",
                last_seen_realtime_ns=clocks["realtime"],
                last_seen_monotonic_ns=clocks["monotonic"],
                last_seen_at="start",
                observations=1,
                max_edge_bps=Decimal("1"),
                max_pnl_quote=Decimal("1"),
                best_cycle={},
            )
            analyzer._active[state.key] = state

            # Wall clock jumps by an hour, while only 100ms elapsed locally.
            clocks["realtime"] += 3_600_000_000_000
            clocks["monotonic"] += 100_000_000
            analyzer._close_stale_candidates()
            self.assertIn(state.key, analyzer._active)

            # A backward wall-clock correction must not prevent a monotonic
            # timeout from closing the candidate.
            clocks["realtime"] -= 7_200_000_000_000
            clocks["monotonic"] += 1_001_000_000
            analyzer._close_stale_candidates()
            self.assertNotIn(state.key, analyzer._active)

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
            quote_slot_id="notional:100:buy_base",
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

    async def test_bug014_remove_quote_key_cleans_all_indexes(self) -> None:
        """Section 20.1: removing a quote key must clean all secondary indexes."""
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        source._latest_books["SOLUSDC"] = _book(  # noqa: SLF001
            symbol="SOLUSDC",
            now_realtime_ns=now_realtime_ns,
            now_monotonic_ns=now_monotonic_ns,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output,
                cex_sources=(source,),
                coalesce_interval_ms=1,
            )
            quote = _good_quote(
                now_realtime_ns=now_realtime_ns,
                now_monotonic_ns=now_monotonic_ns,
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
            key = analyzer._quote_key(quote)
            self.assertIsNotNone(key)
            self.assertIn(key, analyzer._direct_quotes)
            # Remove and verify all indexes are cleaned.
            removed = analyzer._remove_quote_key(key)
            self.assertTrue(removed)
            self.assertNotIn(key, analyzer._direct_quotes)
            self.assertNotIn(key, analyzer._quote_keys_by_direct_provider[quote.provider])
            self.assertFalse(any(dk == key for dk in analyzer._dirty_direct))
            # Cardinality must match.
            self.assertEqual(
                len(analyzer._direct_quotes),
                sum(len(v) for v in analyzer._quote_keys_by_direct_provider.values()),
            )

    async def test_bug014_purge_provider_cleans_all_indexes(self) -> None:
        """Section 20.1: _purge_provider must clean all secondary indexes."""
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        source._latest_books["SOLUSDC"] = _book(  # noqa: SLF001
            symbol="SOLUSDC",
            now_realtime_ns=now_realtime_ns,
            now_monotonic_ns=now_monotonic_ns,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output,
                cex_sources=(source,),
                coalesce_interval_ms=1,
            )
            quote = _good_quote(
                now_realtime_ns=now_realtime_ns,
                now_monotonic_ns=now_monotonic_ns,
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
            removed = analyzer._purge_provider(quote.provider)
            self.assertGreaterEqual(removed, 1)
            self.assertNotIn(quote.provider, analyzer._quote_keys_by_direct_provider)
            self.assertEqual(
                len(analyzer._direct_quotes),
                len(analyzer._quote_keys_by_direct_provider.get(quote.provider, set())),
            )
            self.assertFalse(any(dk[0] == quote.provider for dk in analyzer._dirty_direct))

    async def test_bug014_purge_source_epoch_cleans_all_indexes(self) -> None:
        """Section 20.1: _purge_source_epoch must not leave dangling index entries."""
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        source._latest_books["SOLUSDC"] = _book(  # noqa: SLF001
            symbol="SOLUSDC",
            now_realtime_ns=now_realtime_ns,
            now_monotonic_ns=now_monotonic_ns,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output,
                cex_sources=(source,),
                coalesce_interval_ms=1,
            )
            old_quote = replace(
                _good_quote(now_realtime_ns=now_realtime_ns, now_monotonic_ns=now_monotonic_ns),
                source_epoch=2,
            )
            await analyzer.handle_event(
                MarketEvent(
                    source="dexquote:RAYDIUM",
                    key="test",
                    kind="exact_input_quote",
                    value=old_quote,
                    summary={},
                    received_realtime_ns=now_realtime_ns,
                    received_monotonic_ns=now_monotonic_ns,
                    source_epoch=2,
                ),
            )
            key = analyzer._quote_key(old_quote)
            self.assertIn(key, analyzer._direct_quotes)
            removed = analyzer._purge_source_epoch("dexquote:RAYDIUM", old_epoch=2)
            self.assertGreaterEqual(removed, 1)
            self.assertNotIn(key, analyzer._direct_quotes)
            self.assertNotIn(key, analyzer._quote_keys_by_direct_provider[old_quote.provider])
            # Cardinality must match.
            self.assertEqual(
                len(analyzer._direct_quotes),
                sum(len(v) for v in analyzer._quote_keys_by_direct_provider.values()),
            )

    async def test_bug014_prune_expired_quotes_physically_removes(self) -> None:
        """Section 20.2: expired quotes must be physically removed + indexes cleaned."""
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        source._latest_books["SOLUSDC"] = _book(  # noqa: SLF001
            symbol="SOLUSDC",
            now_realtime_ns=now_realtime_ns,
            now_monotonic_ns=now_monotonic_ns,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(
                output_directory=output,
                cex_sources=(source,),
                max_response_skew_ms=Decimal("1000"),
                coalesce_interval_ms=1,
            )
            stale_quote = replace(
                _good_quote(now_realtime_ns=now_realtime_ns, now_monotonic_ns=now_monotonic_ns),
                response_received_monotonic_ns=now_monotonic_ns - 5_000_000_000,  # 5 seconds old
            )
            await analyzer.handle_event(
                MarketEvent(
                    source="dexquote:RAYDIUM",
                    key="test",
                    kind="exact_input_quote",
                    value=stale_quote,
                    summary={},
                    received_realtime_ns=now_realtime_ns,
                    received_monotonic_ns=now_monotonic_ns,
                ),
            )
            key = analyzer._quote_key(stale_quote)
            self.assertIn(key, analyzer._direct_quotes)
            pruned = analyzer._prune_expired_quotes(time.monotonic_ns())
            self.assertGreaterEqual(pruned, 1)
            self.assertNotIn(key, analyzer._direct_quotes)
            # No key exists only in the secondary index.
            self.assertEqual(
                len(analyzer._direct_quotes),
                sum(len(v) for v in analyzer._quote_keys_by_direct_provider.values()),
            )


if __name__ == "__main__":
    unittest.main()
