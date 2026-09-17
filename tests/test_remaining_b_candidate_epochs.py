from __future__ import annotations

import tempfile
import unittest
import asyncio
import time
from decimal import Decimal
from pathlib import Path

from market_data_lab.cex_dex_cycles import BookSnapshot, MARKETS
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.realtime_scanner import SourceEpochChange
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.solana_realtime_scanner import CexBookStateSource, CexStreamConfig, CexTopOfBookEvent
from market_data_lab.unified_cycle_analyzer import UnifiedCycleAnalyzer
from market_data_lab.unified_perp_analyzer import UnifiedPerpAnalyzer


def _change(source: str, epoch: int, *, previous: int | None = None) -> SourceEpochChange:
    return SourceEpochChange(
        source=source,
        source_epoch=epoch,
        reason="transport_reconnect",
        realtime_ns=epoch * 1_000_000_000,
        monotonic_ns=epoch * 1_000_000_000,
        previous_source_epoch=previous,
    )


def _cycle(
    route_id: str,
    dependencies: tuple[tuple[str, int], ...],
    *,
    analysis_kind: str = "direct_inventory",
) -> dict[str, object]:
    cycle: dict[str, object] = {
        "status": "ok",
        "analysis_kind": analysis_kind,
        "market": "TEST/SOL",
        # Synthetic venues make the lifecycle key distinct for each candidate;
        # source provenance remains explicit below and is what epoch logic uses.
        "cex_venue": route_id,
        "cycle_direction": "buy_dex_sell_cex",
        "route_id": route_id,
        "requested_notional_quote": "100",
        "net_edge_after_minimum_network_bps": "10",
        "net_pnl_after_minimum_network_quote": "1",
        "positive_after_minimum_network": True,
        "timing_valid": True,
    }
    # Explicit source pairs model the source/epoch provenance that production
    # calculators attach to their legs.  Keeping this in the cycle makes the
    # lifecycle test independent of a network/SDK fixture.
    cycle["legs"] = [
        {"source": source, "source_epoch": epoch}
        for source, epoch in dependencies
    ]
    return cycle


def _perp_cycle(
    route_id: str,
    dependencies: tuple[tuple[str, int], ...],
) -> dict[str, object]:
    cycle: dict[str, object] = {
        "status": "ok",
        "analysis_kind": "spot_spot_inventory_cycle",
        "strategy": "cex_spot_cross_venue_inventory_cycle",
        "route_id": route_id,
        "base": "SOL",
        "direction": "buy_first_spot_sell_second_spot",
        "timing_valid": True,
        "market_data_fresh": True,
        "positive_after_modeled_costs": True,
        "candidate_eligible": True,
        "candidate_eligible_with_account_verified_fees": False,
        "net_edge_after_modeled_costs_bps": "10",
        "net_pnl_after_modeled_costs_usdt": "1",
        "buy_spot": {"source": dependencies[0][0], "source_epoch": dependencies[0][1]},
        "sell_spot": {"source": dependencies[-1][0], "source_epoch": dependencies[-1][1]},
    }
    return cycle


def _actual_book(now_real: int, now_mono: int) -> BookSnapshot:
    return BookSnapshot(
        symbol="SOLUSDC", category="spot", status="ok", error=None,
        bids=((Decimal("102"), Decimal("10")),),
        asks=((Decimal("103"), Decimal("10")),),
        exchange_system_time_ms=None, matching_engine_time_ms=None,
        update_id=1, cross_sequence=1,
        response=TimedResponse(
            payload=None, error=None,
            sent_realtime_ns=now_real, received_realtime_ns=now_real,
            sent_monotonic_ns=now_mono, received_monotonic_ns=now_mono,
        ),
        source="test",
    )


def _actual_quote(now_real: int, now_mono: int) -> ExactInputQuote:
    market = MARKETS["SOL_SOLANA_RAYDIUM"]
    return ExactInputQuote(
        provider=market.provider, chain="solana", protocol="test", source_kind="test",
        pair="SOL/USDC", direction="buy_base", round_id=1,
        requested_notional_quote=Decimal("100"), reference_notional_usdt=Decimal("100"),
        quote_slot_id="notional:100:buy_base", base_amount=Decimal("1"),
        quote_amount=Decimal("100"), input_symbol="USDC", output_symbol="SOL",
        input_amount_raw=100_000_000, output_amount_raw=1_000_000_000,
        average_price_quote_per_base=Decimal("100"), fee_bps=Decimal("20"),
        request_rtt_ms=5, status="ok", error=None,
        response_received_realtime_ns=now_real,
        response_received_monotonic_ns=now_mono, block_number=None,
    )


def _actual_spot(venue: str, bid: str, ask: str, now_real: int, now_mono: int) -> CexTopOfBookEvent:
    return CexTopOfBookEvent(
        venue=venue, category="spot", symbol="SOLUSDT",
        best_bid=Decimal(bid), best_bid_size=Decimal("10"),
        best_ask=Decimal(ask), best_ask_size=Decimal("10"),
        source="test", exchange_system_time_ms=None,
        matching_engine_time_ms=None, update_id=1,
        received_realtime_ns=now_real, received_monotonic_ns=now_mono,
    )


class CandidateEpochLifecycleTest(unittest.TestCase):
    def test_cycle_real_evaluation_preserves_unrelated_epoch(self) -> None:
        async def run() -> None:
            now_real = time.time_ns()
            now_mono = time.monotonic_ns()
            source = CexBookStateSource(
                config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
                timeout_seconds=1, proxy_url=None,
            )
            source._latest_books["SOLUSDC"] = _actual_book(now_real, now_mono)
            with tempfile.TemporaryDirectory() as directory:
                analyzer = UnifiedCycleAnalyzer(
                    output_directory=Path(directory), cex_sources=(source,), coalesce_interval_ms=1,
                )
                quote = _actual_quote(now_real, now_mono)
                await analyzer.handle_event(MarketEvent(
                    source="dexquote:RAYDIUM", key="quote", kind="exact_input_quote", value=quote,
                    summary={}, received_realtime_ns=now_real, received_monotonic_ns=now_mono,
                ))
                book = CexTopOfBookEvent.from_book(venue="MEXC", book=source._latest_books["SOLUSDC"])
                await analyzer.handle_event(MarketEvent(
                    source=source.name, key="book", kind="order_book", value=book,
                    summary={}, received_realtime_ns=now_real, received_monotonic_ns=now_mono,
                ))
                await asyncio.sleep(0.03)
                self.assertTrue(analyzer._active, "production evaluation did not create a candidate")
                candidate = next(iter(analyzer._active.values()))
                self.assertIn(("dexquote:RAYDIUM", 0), candidate.dependencies)
                self.assertIn((source.name, 0), candidate.dependencies)
                analyzer.handle_source_epoch_change(_change("unrelated-source", 1))
                self.assertIn(candidate.key, analyzer._active)
                await analyzer.close()
        asyncio.run(run())

    def test_cycle_unrelated_epoch_preserves_candidate_and_dirty_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            analyzer = UnifiedCycleAnalyzer(
                output_directory=Path(directory), cex_sources=(),
            )
            analyzer.handle_source_epoch_change(_change("source-a", 1))
            analyzer._observe_cycle(
                _cycle("route-a", (("source-a", 1),)),
                analysis_kind="direct_inventory",
                observed_realtime_ns=1_000,
                observed_monotonic_ns=1_000,
            )
            started = next(iter(analyzer._active.values()))
            started_at = started.started_monotonic_ns
            self.assertEqual(started.dependencies, frozenset({("source-a", 1)}))
            analyzer._dirty_direct.add(("unrelated", "slot", "buy_base", "MEXC"))

            analyzer.handle_source_epoch_change(_change("source-b", 1))

            self.assertIn(started.key, analyzer._active)
            self.assertEqual(analyzer._active[started.key].started_monotonic_ns, started_at)
            self.assertEqual(analyzer._candidate_closed, 0)
            self.assertIn(("unrelated", "slot", "buy_base", "MEXC"), analyzer._dirty_direct)

    def test_cycle_affected_epoch_closes_once_and_fresh_epoch_reopens(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            analyzer = UnifiedCycleAnalyzer(output_directory=Path(directory), cex_sources=())
            analyzer.handle_source_epoch_change(_change("source-a", 1))
            cycle = _cycle("route-a", (("source-a", 1),))
            analyzer._observe_cycle(
                cycle, analysis_kind="direct_inventory",
                observed_realtime_ns=1, observed_monotonic_ns=1,
            )
            key = next(iter(analyzer._active))
            analyzer.handle_source_epoch_change(_change("source-a", 2, previous=1))
            self.assertNotIn(key, analyzer._active)
            self.assertEqual(analyzer._candidate_closed, 1)
            self.assertEqual(analyzer._counts["epoch_invalidated_quotes"], 0)
            with self.assertRaisesRegex(RuntimeError, "regressed"):
                analyzer.handle_source_epoch_change(_change("source-a", 1, previous=2))
            # Same transition is idempotent and does not emit a second close.
            analyzer.handle_source_epoch_change(_change("source-a", 2, previous=1))
            self.assertEqual(analyzer._candidate_closed, 1)
            reopened = _cycle("route-a", (("source-a", 2),))
            analyzer._observe_cycle(
                reopened, analysis_kind="direct_inventory",
                observed_realtime_ns=2, observed_monotonic_ns=2,
            )
            self.assertEqual(len(analyzer._active), 1)

    def test_cycle_same_base_and_multisource_dependencies_are_precise(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            analyzer = UnifiedCycleAnalyzer(output_directory=Path(directory), cex_sources=())
            analyzer.handle_source_epoch_change(_change("source-a", 1))
            analyzer.handle_source_epoch_change(_change("source-b", 1))
            analyzer.handle_source_epoch_change(_change("source-c", 1))
            for route, deps in (
                ("affected", (("source-a", 1),)),
                ("unaffected", (("source-b", 1),)),
                ("multi", (("source-a", 1), ("source-c", 1))),
            ):
                analyzer._observe_cycle(
                    _cycle(route, deps), analysis_kind="direct_inventory",
                    observed_realtime_ns=1, observed_monotonic_ns=1,
                )
            analyzer.handle_source_epoch_change(_change("source-a", 2, previous=1))
            self.assertEqual(
                {state.key.split("|")[2] for state in analyzer._active.values()},
                {"TEST/SOL"},
            )
            self.assertEqual(len(analyzer._active), 1)
            self.assertIn("unaffected", next(iter(analyzer._active)))

    def test_cycle_pending_candidate_not_counted_shorter_on_unrelated_epoch(self) -> None:
        clocks = {"real": 1_000, "mono": 1_000}
        with tempfile.TemporaryDirectory() as directory:
            analyzer = UnifiedCycleAnalyzer(
                output_directory=Path(directory), cex_sources=(),
                monotonic_ns=lambda: clocks["mono"], realtime_ns=lambda: clocks["real"],
            )
            analyzer.handle_source_epoch_change(_change("source-a", 1))
            analyzer._observe_cycle(
                _cycle("pending", (("source-a", 1),)),
                analysis_kind="direct_inventory", observed_realtime_ns=1, observed_monotonic_ns=1,
            )
            analyzer.handle_source_epoch_change(_change("source-b", 1))
            self.assertEqual(analyzer._counts["candidate_shorter_than_minimum_persistence"], 0)
            analyzer.handle_source_epoch_change(_change("source-a", 2, previous=1))
            self.assertEqual(analyzer._candidate_closed, 1)

    def test_perp_unrelated_epoch_preserves_candidate_dirty_work_and_counters(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            analyzer = UnifiedPerpAnalyzer(output_directory=Path(directory))
            analyzer.handle_source_epoch_change(_change("source-a", 1))
            analyzer._observe_cycle(
                _perp_cycle("route-a", (("source-a", 1),)),
                observed_realtime_ns=1, observed_monotonic_ns=1,
            )
            key = next(iter(analyzer._active))
            self.assertEqual(analyzer._active[key].dependencies, frozenset({("source-a", 1)}))
            started = analyzer._active[key].started_monotonic_ns
            analyzer._dirty_bases.add("OTHER")
            analyzer.handle_source_epoch_change(_change("source-b", 1))
            self.assertIn(key, analyzer._active)
            self.assertEqual(analyzer._active[key].started_monotonic_ns, started)
            self.assertEqual(analyzer._candidate_closed, 0)
            self.assertIn("OTHER", analyzer._dirty_bases)

    def test_perp_real_evaluation_preserves_unrelated_epoch(self) -> None:
        async def run() -> None:
            now_real = time.time_ns()
            now_mono = time.monotonic_ns()
            with tempfile.TemporaryDirectory() as directory:
                analyzer = UnifiedPerpAnalyzer(output_directory=Path(directory), coalesce_interval_ms=1)
                for value in (
                    _actual_spot("MEXC", "99", "100", now_real, now_mono),
                    _actual_spot("BINANCE", "103", "104", now_real, now_mono),
                ):
                    await analyzer.handle_event(MarketEvent(
                        source="cex:spot:test", key=f"book:{value.venue}", kind="order_book",
                        value=value, summary={}, received_realtime_ns=now_real,
                        received_monotonic_ns=now_mono,
                    ))
                await asyncio.sleep(0.04)
                self.assertTrue(analyzer._active, "production perp evaluation did not create a candidate")
                candidate = next(iter(analyzer._active.values()))
                self.assertIn(("cex:spot:test", 0), candidate.dependencies)
                analyzer.handle_source_epoch_change(_change("unrelated-source", 1))
                self.assertIn(candidate.key, analyzer._active)
                await analyzer.close()
        asyncio.run(run())

    def test_perp_affected_and_multisource_candidates_close_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            analyzer = UnifiedPerpAnalyzer(output_directory=Path(directory))
            for source in ("source-a", "source-b", "source-c"):
                analyzer.handle_source_epoch_change(_change(source, 1))
            analyzer._observe_cycle(
                _perp_cycle("affected", (("source-a", 1),)),
                observed_realtime_ns=1, observed_monotonic_ns=1,
            )
            analyzer._observe_cycle(
                _perp_cycle("unaffected", (("source-b", 1),)),
                observed_realtime_ns=1, observed_monotonic_ns=1,
            )
            analyzer._observe_cycle(
                _perp_cycle("multi", (("source-a", 1), ("source-c", 1))),
                observed_realtime_ns=1, observed_monotonic_ns=1,
            )
            # `_perp_cycle` has two leg records, so the multi-source state
            # already carries both actual dependencies.
            analyzer.handle_source_epoch_change(_change("source-a", 2, previous=1))
            self.assertEqual(analyzer._candidate_closed, 2)
            self.assertEqual(len(analyzer._active), 1)
            self.assertIn("unaffected", next(iter(analyzer._active)))
            analyzer.handle_source_epoch_change(_change("source-a", 2, previous=1))
            self.assertEqual(analyzer._candidate_closed, 2)

    def test_perp_pending_unrelated_transition_does_not_increment_shorter_counter(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            analyzer = UnifiedPerpAnalyzer(
                output_directory=Path(directory), candidate_min_persistence_ms=Decimal("500"),
            )
            analyzer.handle_source_epoch_change(_change("source-a", 1))
            analyzer._observe_cycle(
                _perp_cycle("pending", (("source-a", 1),)),
                observed_realtime_ns=1, observed_monotonic_ns=1,
            )
            analyzer.handle_source_epoch_change(_change("source-b", 1))
            self.assertEqual(analyzer._counts["candidate_shorter_than_minimum_persistence"], 0)
            analyzer.handle_source_epoch_change(_change("source-a", 2, previous=1))
            self.assertEqual(analyzer._counts["candidate_shorter_than_minimum_persistence"], 1)

    def test_shutdown_clears_active_candidate_state_after_epoch_indexing(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as directory:
                analyzer = UnifiedPerpAnalyzer(output_directory=Path(directory))
                analyzer.handle_source_epoch_change(_change("source-a", 1))
                analyzer._observe_cycle(
                    _perp_cycle("route", (("source-a", 1),)),
                    observed_realtime_ns=1, observed_monotonic_ns=1,
                )
                await analyzer.close()
                self.assertFalse(analyzer._active)
                self.assertFalse(analyzer._dirty_bases)

        import asyncio
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
