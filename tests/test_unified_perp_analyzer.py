from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path

from market_data_lab.perp_venue_feeds import PerpQuoteEvent
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.solana_realtime_scanner import CexTopOfBookEvent
from market_data_lab.unified_perp_analyzer import UnifiedPerpAnalyzer
from market_data_lab.unified_perp_analyzer import _ActiveCandidate


def _event(*, value: object, kind: str, now_real: int, now_mono: int) -> MarketEvent:
    return MarketEvent(
        source="test",
        key=f"test:{kind}:{id(value)}",
        kind=kind,
        value=value,
        summary={},
        received_realtime_ns=now_real,
        received_monotonic_ns=now_mono,
    )


def _perp(
    *,
    venue: str,
    base: str,
    bid: str,
    ask: str,
    funding: str | None,
    now_real: int,
    now_mono: int,
    next_funding_time_ms: int | None = None,
    funding_rate_kind: str = "current_hourly_rate",
    funding_interval_minutes: int | None = 60,
    contract_type: str | None = "linear_perpetual",
    quantity_step: Decimal | None = Decimal("0.001"),
    settlement: str = "USDC",
) -> PerpQuoteEvent:
    return PerpQuoteEvent(
        venue=venue,
        venue_symbol=f"{base}-USD",
        base=base,
        settlement=settlement,
        best_bid=Decimal(bid),
        best_ask=Decimal(ask),
        funding_rate=Decimal(funding) if funding is not None else None,
        mark_price=(Decimal(bid) + Decimal(ask)) / Decimal("2"),
        index_price=(Decimal(bid) + Decimal(ask)) / Decimal("2"),
        received_realtime_ns=now_real,
        received_monotonic_ns=now_mono,
        best_bid_size=Decimal("10"),
        best_ask_size=Decimal("10"),
        book_received_realtime_ns=now_real,
        book_received_monotonic_ns=now_mono,
        context_received_realtime_ns=now_real,
        context_received_monotonic_ns=now_mono,
        next_funding_time_ms=next_funding_time_ms,
        funding_interval_minutes=funding_interval_minutes,
        funding_rate_kind=funding_rate_kind,
        quantity_step=quantity_step,
        public_taker_fee_bps=Decimal("0"),
        fee_source="test",
        contract_type=contract_type,
        execution_model="central_limit_order_book",
    )


def _dex_quote(
    *,
    direction: str,
    round_id: int,
    base_amount: str,
    quote_amount: str,
    input_symbol: str,
    output_symbol: str,
    input_amount_raw: int,
    output_amount_raw: int,
    now_real: int,
    now_mono: int,
) -> ExactInputQuote:
    return ExactInputQuote(
        provider="RAYDIUM",
        chain="solana",
        protocol="test",
        source_kind="test",
        pair="SOL/USDC",
        direction=direction,
        round_id=round_id,
        requested_notional_quote=Decimal("100"),
        reference_notional_usdt=Decimal("100"),
        quote_slot_id="notional:100:buy_base" if direction == "buy_base" else "notional:100:sell_base",
        base_amount=Decimal(base_amount),
        quote_amount=Decimal(quote_amount),
        input_symbol=input_symbol,
        output_symbol=output_symbol,
        input_amount_raw=input_amount_raw,
        output_amount_raw=output_amount_raw,
        average_price_quote_per_base=Decimal(quote_amount) / Decimal(base_amount),
        fee_bps=Decimal("20"),
        request_rtt_ms=5,
        status="ok",
        error=None,
        response_received_realtime_ns=now_real,
        response_received_monotonic_ns=now_mono,
        block_number=None,
    )


def _sequential_worker_bundle() -> dict[str, object]:
    return {
        "schema_version": 1,
        "snapshot_id": "snapshot-req-seq",
        "worker_generation": 3,
        "source_epoch": 0,
        "boot_id": "boot-seq",
        "model_version": "raydium_cpmm_v1",
        "pool_refs": [
            {
                "chain_namespace": "solana",
                "chain_id": "mainnet",
                "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
                "pool_address": "cpmm-pool",
                "protocol": "raydium_cpmm",
                "protocol_revision": "v1",
                "asset_0_id": "solana:mainnet:mint-a:6",
                "asset_1_id": "solana:mainnet:mint-b:9",
                "pool_spec_version": 1,
            }
        ],
        "dependency_vector": [],
        "pools": [
            {
                "pool_id": "solana:mainnet:cpmm-pool",
                "vault_a_raw": "1000000000",
                "vault_b_raw": "1000000000",
                "protocol_fees_a_raw": "0",
                "protocol_fees_b_raw": "0",
                "fund_fees_a_raw": "0",
                "fund_fees_b_raw": "0",
                "creator_fees_a_raw": "0",
                "creator_fees_b_raw": "0",
                "trade_fee_rate": "2500",
                "creator_fee_rate": "120",
                "protocol_fee_rate": "120",
                "fund_fee_rate": "40",
                "fee_on": "0",
            }
        ],
        "context_slot": 50,
        "chain_consistency": "validated_multi_account_snapshot",
        "sdk_versions": [["@raydium-io/raydium-sdk-v2", "latest"]],
    }


class UnifiedPerpAnalyzerTest(unittest.IsolatedAsyncioTestCase):
    async def test_candidate_idle_lifecycle_uses_monotonic_not_wall_clock(self) -> None:
        clocks = {"realtime": 10_000_000_000, "monotonic": 1_000_000_000}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(
                output_directory=output,
                monotonic_ns=lambda: clocks["monotonic"],
                realtime_ns=lambda: clocks["realtime"],
            )
            state = _ActiveCandidate(
                key="candidate",
                analysis_kind="perp_perp_funding_carry",
                started_realtime_ns=clocks["realtime"],
                started_monotonic_ns=clocks["monotonic"],
                started_at="start",
                last_seen_realtime_ns=clocks["realtime"],
                last_seen_monotonic_ns=clocks["monotonic"],
                last_seen_at="start",
                observations=1,
                max_edge_bps=Decimal("1"),
                max_pnl_usdt=Decimal("1"),
                best_cycle={},
                persisted=False,
            )
            analyzer._active[state.key] = state

            clocks["realtime"] += 3_600_000_000_000
            clocks["monotonic"] += 100_000_000
            analyzer._close_stale_candidates()
            self.assertIn(state.key, analyzer._active)

            clocks["realtime"] -= 7_200_000_000_000
            clocks["monotonic"] += 1_001_000_000
            analyzer._close_stale_candidates()
            self.assertNotIn(state.key, analyzer._active)
            await analyzer.close()

    async def test_terminal_journal_appends_batches_and_stops_at_cap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(
                output_directory=output,
                max_candidate_events=2,
            )

            analyzer._persist_candidate_event(
                {"event": "candidate_closed", "candidate_key": "first"}
            )
            analyzer._flush_candidate_events(force=True)
            journal = output / "perp_analysis" / "candidate_events.jsonl"
            first_inode = journal.stat().st_ino

            analyzer._persist_candidate_event(
                {"event": "candidate_closed", "candidate_key": "second"}
            )
            analyzer._flush_candidate_events(force=True)
            self.assertEqual(journal.stat().st_ino, first_inode)

            analyzer._persist_candidate_event(
                {"event": "candidate_closed", "candidate_key": "third"}
            )
            snapshot = analyzer.snapshot()
            await analyzer.close()

            rows = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([row["candidate_key"] for row in rows], ["first", "second"])
            self.assertEqual(
                snapshot["candidate_event_persistence"]["format"],
                "bounded_compact_perp_candidate_terminal_summary_v4",
            )
            self.assertEqual(
                snapshot["candidate_event_persistence"]["write_mode"],
                "batched_append_until_count_cap",
            )
            self.assertEqual(snapshot["counts"]["candidate_events_dropped_after_limit"], 1)

    async def test_cross_perp_funding_candidate_is_bounded_and_not_execution_ready(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        next_event_ms = now_real // 1_000_000 + 1_000
        # A long on A receives negative funding, while a short on B receives
        # positive funding.  The one-hour carry exceeds the current BBO
        # round-trip loss, so the research model should surface it.
        long_a = _perp(
            venue="A",
            base="BTC",
            bid="99",
            ask="100",
            funding="-0.02",
            now_real=now_real,
            now_mono=now_mono,
            next_funding_time_ms=next_event_ms,
            funding_rate_kind="next_hourly_rate",
        )
        short_b = _perp(
            venue="B",
            base="BTC",
            bid="105",
            ask="106",
            funding="0.02",
            now_real=now_real,
            now_mono=now_mono,
            next_funding_time_ms=next_event_ms,
            funding_rate_kind="next_hourly_rate",
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(
                output_directory=output,
                coalesce_interval_ms=1,
                candidate_min_persistence_ms=Decimal("0"),
            )
            await analyzer.handle_event(
                _event(value=long_a, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=short_b, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            await analyzer.close()

            events = [
                json.loads(line)
                for line in (output / "perp_analysis" / "candidate_events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertGreater(
                snapshot["counts"]["perp_perp_funding_carry_evaluations"],
                0,
            )
            self.assertEqual(events[0]["event"], "candidate_closed")
            self.assertFalse(events[0]["execution_ready"])
            self.assertFalse(events[0]["best_cycle"]["candidate_eligible_with_account_verified_fees"])
            self.assertGreaterEqual(events[0]["duration_seconds"], 0)
            self.assertFalse((output / "raw.jsonl").exists())

    async def test_funding_rate_without_a_known_event_is_not_horizon_pnl(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        long_a = _perp(
            venue="A",
            base="BTC",
            bid="99",
            ask="100",
            funding="-0.02",
            now_real=now_real,
            now_mono=now_mono,
        )
        short_b = _perp(
            venue="B",
            base="BTC",
            bid="105",
            ask="106",
            funding="0.02",
            now_real=now_real,
            now_mono=now_mono,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=long_a, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=short_b, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)

            routes = [
                route
                for route in analyzer._route_stats.values()
                if (
                    route["analysis_kind"] == "perp_perp_funding_carry"
                    and route["route_id"] == "long:A:BTC-USD|short:B:BTC-USD"
                )
            ]
            self.assertEqual(len(routes), 1)
            cycle = routes[0]["best_timing_valid_cycle"]
            self.assertIsNotNone(cycle)
            assert cycle is not None
            self.assertIsNotNone(cycle["funding_pnl_per_hour_usdt"])
            self.assertIsNone(cycle["funding_pnl_for_horizon_usdt"])
            self.assertTrue(cycle["funding_not_included_in_net_pnl"])
            self.assertFalse(cycle["funding_horizon_model_complete"])
            self.assertFalse(cycle["candidate_eligible"])
            self.assertEqual(cycle["quality"]["funding_quality"], "unknown")
            self.assertEqual(
                cycle["funding_details"][0]["funding_projection_reason"],
                "funding_rate_not_explicitly_for_next_event",
            )
            await analyzer.close()

    async def test_known_next_event_without_interval_is_horizon_pnl_without_hourly_rate(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        next_event_ms = now_real // 1_000_000 + 1_000
        long_a = _perp(
            venue="A",
            base="BTC",
            bid="99",
            ask="100",
            funding="-0.02",
            now_real=now_real,
            now_mono=now_mono,
            next_funding_time_ms=next_event_ms,
            funding_rate_kind="next_funding_rate",
            funding_interval_minutes=None,
        )
        short_b = _perp(
            venue="B",
            base="BTC",
            bid="105",
            ask="106",
            funding="0.02",
            now_real=now_real,
            now_mono=now_mono,
            next_funding_time_ms=next_event_ms,
            funding_rate_kind="next_funding_rate",
            funding_interval_minutes=None,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=long_a, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=short_b, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)

            routes = [
                route
                for route in analyzer._route_stats.values()
                if (
                    route["analysis_kind"] == "perp_perp_funding_carry"
                    and route["route_id"] == "long:A:BTC-USD|short:B:BTC-USD"
                )
            ]
            self.assertEqual(len(routes), 1)
            cycle = routes[0]["best_timing_valid_cycle"]
            self.assertIsNotNone(cycle)
            assert cycle is not None
            self.assertIsNone(cycle["funding_pnl_per_hour_usdt"])
            self.assertEqual(
                Decimal(cycle["funding_pnl_for_horizon_usdt"]),
                Decimal("4.10"),
            )
            self.assertTrue(cycle["funding_horizon_model_complete"])
            self.assertFalse(cycle["funding_not_included_in_net_pnl"])
            self.assertEqual(cycle["quality"]["funding_quality"], "projected")
            await analyzer.close()

    async def test_inverse_contract_is_explicitly_rejected_before_linear_cashflow_math(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        inverse = _perp(
            venue="INVERSE",
            base="BTC",
            bid="99",
            ask="100",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
            contract_type="inverse_perpetual",
        )
        linear = _perp(
            venue="LINEAR",
            base="BTC",
            bid="105",
            ask="106",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=inverse, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=linear, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()

            self.assertGreater(snapshot["counts"]["perp_perp_unsupported_contract_model"], 0)
            self.assertEqual(snapshot["route_count"], 0)
            self.assertEqual(
                snapshot["coverage"]["unsupported_perp_contract_states"],
                [
                    {
                        "venue": "INVERSE",
                        "venue_symbol": "BTC-USD",
                        "base": "BTC",
                        "contract_type": "inverse_perpetual",
                        "reason": "unsupported_contract_model",
                    }
                ],
            )
            await analyzer.close()

    async def test_perp_pair_uses_shared_lot_lattice_not_sequential_rounding(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        first = _perp(
            venue="A",
            base="BTC",
            bid="99",
            ask="100",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
            quantity_step=Decimal("0.03"),
        )
        second = _perp(
            venue="B",
            base="BTC",
            bid="105",
            ask="106",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
            quantity_step=Decimal("0.02"),
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(
                output_directory=output,
                target_notional_usdt=Decimal("4"),
                coalesce_interval_ms=1,
            )
            await analyzer.handle_event(
                _event(value=first, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=second, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()

            # 0.04 is below the shared 0.06 lot.  The old sequential
            # rounding could emit 0.02, which is invalid on the 0.03 venue.
            self.assertEqual(snapshot["route_count"], 0)
            self.assertGreater(
                snapshot["counts"]["perp_perp_insufficient_visible_size_or_contract_step"],
                0,
            )
            await analyzer.close()

    async def test_cross_stable_perp_pair_is_rejected_without_executable_fx(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        usdc_perp = _perp(
            venue="USDC",
            base="BTC",
            bid="99",
            ask="100",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
            settlement="USDC",
        )
        usdt_perp = _perp(
            venue="USDT",
            base="BTC",
            bid="105",
            ask="106",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
            settlement="USDT",
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=usdc_perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=usdt_perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()

            self.assertEqual(snapshot["route_count"], 0)
            self.assertGreater(
                snapshot["counts"]["perp_perp_cross_settlement_fx_unavailable"],
                0,
            )
            await analyzer.close()

    async def test_dex_perp_cross_settlement_is_not_valued_at_parity(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        perp = _perp(
            venue="A",
            base="SOL",
            bid="102",
            ask="103",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
            settlement="USDT",
        )
        dex = _dex_quote(
            direction="buy_base",
            round_id=1,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=dex, kind="exact_input_quote", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()

            self.assertEqual(snapshot["route_count"], 0)
            self.assertGreater(
                snapshot["counts"]["dex_perp_cross_settlement_fx_unavailable"],
                0,
            )
            await analyzer.close()

    async def test_delayed_exact_quote_does_not_replace_newer_latest_state(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        fresh = _dex_quote(
            direction="buy_base",
            round_id=2,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        delayed = _dex_quote(
            direction="buy_base",
            round_id=1,
            base_amount="1",
            quote_amount="99",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=99_000_000,
            output_amount_raw=1_000_000_000,
            now_real=now_real - 1_000_000,
            now_mono=now_mono - 1_000_000,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=fresh, kind="exact_input_quote", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(
                    value=delayed,
                    kind="exact_input_quote",
                    now_real=now_real,
                    now_mono=now_mono,
                ),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()

            self.assertIs(
                analyzer._dex_quotes[("RAYDIUM", "notional:100:buy_base", "buy_base")],
                fresh,
            )
            self.assertEqual(snapshot["counts"]["exact_quote_out_of_order_ignored"], 1)
            await analyzer.close()

    async def test_capability_manifest_is_metadata_only_and_written_on_first_snapshot(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        perp = _perp(
            venue="A",
            base="BTC",
            bid="99",
            ask="100",
            funding="0.001",
            now_real=now_real,
            now_mono=now_mono,
            next_funding_time_ms=now_real // 1_000_000 + 60_000,
            funding_rate_kind="next_hourly_rate",
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            manifest = json.loads(
                (output / "perp_analysis" / "capabilities.json").read_text(encoding="utf-8")
            )

            self.assertFalse(manifest["execution_enabled"])
            self.assertEqual(
                manifest["markets"]["perpetuals"][0]["contract_model"]["model_id"],
                "linear_base_quantity_perpetual_v1",
            )
            self.assertTrue(
                manifest["markets"]["perpetuals"][0]["funding"]["next_event_observed"]
            )
            self.assertNotIn("best_bid", manifest["markets"]["perpetuals"][0])
            self.assertNotIn("funding_rate", manifest["markets"]["perpetuals"][0]["funding"])
            self.assertFalse(snapshot["capability_manifest"]["dirty_in_memory"])
            await analyzer.close()

    async def test_spot_and_dex_hedges_are_evaluated_without_claiming_dex_exit_profit(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        perp = _perp(
            venue="A",
            base="SOL",
            bid="102",
            ask="103",
            funding="0.001",
            now_real=now_real,
            now_mono=now_mono,
        )
        spot = CexTopOfBookEvent(
            venue="MEXC",
            category="spot",
            symbol="SOLUSDC",
            best_bid=Decimal("100"),
            best_bid_size=Decimal("10"),
            best_ask=Decimal("101"),
            best_ask_size=Decimal("10"),
            source="test",
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=1,
            received_realtime_ns=now_real,
            received_monotonic_ns=now_mono,
        )
        dex = ExactInputQuote(
            provider="RAYDIUM",
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
            response_received_realtime_ns=now_real,
            response_received_monotonic_ns=now_mono,
            block_number=None,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=spot, kind="order_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=dex, kind="exact_input_quote", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            await analyzer.close()

            counts = snapshot["counts"]
            self.assertGreater(counts["spot_perp_flat_price_cycle_evaluations"], 0)
            self.assertGreater(counts["dex_perp_entry_hedge_evaluations"], 0)
            dex_rows = [
                route["best_timing_valid_cycle"]
                for route in snapshot["top_reconnaissance_entry_signals"]
                if route["analysis_kind"] == "dex_perp_entry_hedge"
            ]
            self.assertTrue(dex_rows)
            self.assertFalse(dex_rows[0]["candidate_eligible"])
            self.assertFalse(dex_rows[0]["full_exit_model"])
            self.assertFalse(dex_rows[0]["pnl_model_complete"])
            self.assertTrue(dex_rows[0]["entry_basis_is_not_realised_pnl"])
            self.assertIsNone(dex_rows[0]["net_pnl_after_modeled_costs_usdt"])
            self.assertEqual(dex_rows[0]["model_currency"], "USDC")

    async def test_dex_perp_paired_quotes_model_flat_pnl_without_candidate(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        next_event_ms = now_real // 1_000_000 + 1_000
        perp = _perp(
            venue="A",
            base="SOL",
            bid="102",
            ask="103",
            funding="0.001",
            now_real=now_real,
            now_mono=now_mono,
            next_funding_time_ms=next_event_ms,
            funding_rate_kind="next_hourly_rate",
        )
        buy = _dex_quote(
            direction="buy_base",
            round_id=7,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        sell = _dex_quote(
            direction="sell_base",
            round_id=7,
            base_amount="1",
            quote_amount="99",
            input_symbol="SOL",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=99_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        # Simulate a later fee-tier/route response for the same notional.  It
        # overwrites the legacy latest-sell slot but must not discard the exact
        # raw-quantity reverse already held by the bounded pair cache.
        later_other_size_sell = _dex_quote(
            direction="sell_base",
            round_id=8,
            base_amount="0.999",
            quote_amount="98",
            input_symbol="SOL",
            output_symbol="USDC",
            input_amount_raw=999_000_000,
            output_amount_raw=98_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=buy, kind="exact_input_quote", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=sell, kind="exact_input_quote", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(
                    value=later_other_size_sell,
                    kind="exact_input_quote",
                    now_real=now_real,
                    now_mono=now_mono,
                ),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            await analyzer.close()

            paired_rows = [
                route["best_timing_valid_cycle"]
                for route in snapshot["top_unqualified_closed_models"]
                if route["analysis_kind"] == "dex_perp_paired_exact_quote_flat_model"
            ]
            self.assertEqual(len(paired_rows), 1)
            cycle = paired_rows[0]
            self.assertEqual(cycle["dex_roundtrip_pnl_usdt"], "-1")
            self.assertEqual(cycle["perp_short_roundtrip_pnl_usdt"], "-1")
            self.assertEqual(cycle["net_before_network_and_funding_usdt"], "-2")
            self.assertEqual(cycle["net_pnl_after_modeled_costs_usdt"], "-2.02")
            self.assertTrue(cycle["full_exit_model"])
            self.assertTrue(cycle["pnl_model_complete"])
            self.assertFalse(cycle["candidate_eligible"])
            self.assertFalse(cycle["execution_ready"])
            self.assertTrue(cycle["reverse_dex_exact_quote_for_same_base_quantity_available"])
            self.assertFalse(cycle["dex_post_trade_pool_state_simulated"])
            self.assertEqual(
                cycle["candidate_eligibility_blocker"],
                "dex_paired_quotes_do_not_simulate_post_trade_pool_state",
            )
            self.assertGreater(
                snapshot["counts"]["dex_perp_paired_exact_quote_funding_scenario_evaluations"],
                0,
            )

    async def test_t11_dex_quote_is_not_scaled_to_perp_lot(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        perp = _perp(
            venue="A",
            base="SOL",
            bid="102",
            ask="103",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
            quantity_step=Decimal("0.001"),
        )
        # Flooring to the perp lattice leaves only 3.9984 bps, below the old
        # five-bps screen.  The exact DEX amount must still never be scaled.
        buy = _dex_quote(
            direction="buy_base",
            round_id=7,
            base_amount="1.0004",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_400_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(
                    value=buy,
                    kind="exact_input_quote",
                    now_real=now_real,
                    now_mono=now_mono,
                ),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            await analyzer.close()

        self.assertGreater(
            snapshot["counts"]["dex_perp_exact_quote_quantity_mismatch"],
            0,
        )
        self.assertNotIn("dex_perp_entry_hedge_evaluations", snapshot["counts"])
        self.assertNotIn(
            "dex_perp_paired_exact_quote_flat_model_evaluations",
            snapshot["counts"],
        )

    async def test_dex_perp_sequential_flat_model_from_configured_simulator(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        perp = _perp(
            venue="A",
            base="SOL",
            bid="102",
            ask="103",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
        )
        buy = _dex_quote(
            direction="buy_base",
            round_id=9,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )

        from market_data_lab.amm_simulation.backend import (
            WorkerSequentialSimulator,
            decode_worker_snapshot,
        )

        snapshot = decode_worker_snapshot(_sequential_worker_bundle())
        pool_ref = snapshot.pools[0].pool_ref
        simulator = WorkerSequentialSimulator(
            snapshot=snapshot,
            pool_ref=pool_ref,
            stable_asset_id="solana:mainnet:mint-a:6",
            base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6,
        )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(
                output_directory=output,
                coalesce_interval_ms=1,
                sequential_amm_simulator=simulator,
            )
            await analyzer.handle_event(
                _event(value=perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=buy, kind="exact_input_quote", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            await analyzer.close()

        sequential_rows = [
            route["best_timing_valid_cycle"]
            for route in snapshot["top_unqualified_closed_models"]
            if route["analysis_kind"] == "dex_perp_sequential_flat_model"
        ]
        self.assertEqual(len(sequential_rows), 1)
        cycle = sequential_rows[0]
        self.assertTrue(cycle["dex_post_trade_pool_state_simulated"])
        self.assertFalse(cycle["execution_ready"])
        self.assertFalse(cycle["candidate_eligible"])
        self.assertEqual(cycle["dex_sequential_scenario_kind"], "frozen_market")
        self.assertGreater(
            snapshot["counts"]["dex_perp_sequential_flat_model_evaluations"],
            0,
        )

    async def test_dex_perp_uses_fresh_cross_round_reverse_as_unqualified_model(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        perp = _perp(
            venue="A",
            base="SOL",
            bid="102",
            ask="103",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
        )
        buy = _dex_quote(
            direction="buy_base",
            round_id=7,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        sell = _dex_quote(
            direction="sell_base",
            round_id=8,
            base_amount="1",
            quote_amount="99",
            input_symbol="SOL",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=99_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            for value in (perp, buy, sell):
                await analyzer.handle_event(
                    _event(
                        value=value,
                        kind="perp_book" if value is perp else "exact_input_quote",
                        now_real=now_real,
                        now_mono=now_mono,
                    ),
                )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            await analyzer.close()

            paired_rows = [
                route["best_timing_valid_cycle"]
                for route in snapshot["top_unqualified_closed_models"]
                if route["analysis_kind"] == "dex_perp_paired_exact_quote_flat_model"
            ]
            self.assertEqual(len(paired_rows), 1)
            cycle = paired_rows[0]
            self.assertEqual(cycle["reverse_dex_pairing"], "cross_round_unpinned_state")
            self.assertFalse(cycle["reverse_dex_same_source_round"])
            self.assertFalse(cycle["candidate_eligible"])
            self.assertTrue(cycle["pnl_model_complete"])
            self.assertGreater(
                snapshot["coverage"]["exact_quote_pair_cache"]["counts"]["lookup_pair_matched"],
                0,
            )

    async def test_dex_perp_rejects_reverse_quote_with_different_raw_base_quantity(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        perp = _perp(
            venue="A",
            base="SOL",
            bid="102",
            ask="103",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
        )
        buy = _dex_quote(
            direction="buy_base",
            round_id=7,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        sell = _dex_quote(
            direction="sell_base",
            round_id=7,
            base_amount="0.999",
            quote_amount="99",
            input_symbol="SOL",
            output_symbol="USDC",
            input_amount_raw=999_000_000,
            output_amount_raw=99_000_000,
            now_real=now_real,
            now_mono=now_mono,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            for value in (perp, buy, sell):
                await analyzer.handle_event(
                    _event(
                        value=value,
                        kind="perp_book" if value is perp else "exact_input_quote",
                        now_real=now_real,
                        now_mono=now_mono,
                    ),
                )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            await analyzer.close()

            self.assertGreater(
                snapshot["counts"]["dex_perp_reverse_quote_raw_base_quantity_mismatch"],
                0,
            )
            self.assertNotIn(
                "dex_perp_paired_exact_quote_flat_model_evaluations",
                snapshot["counts"],
            )

    async def test_dex_perp_stale_quote_is_not_a_timing_valid_entry_signal(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        perp = _perp(
            venue="A",
            base="SOL",
            bid="102",
            ask="103",
            funding=None,
            now_real=now_real,
            now_mono=now_mono,
        )
        stale_buy = _dex_quote(
            direction="buy_base",
            round_id=7,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="SOL",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            now_real=now_real - 2_000_000_000,
            now_mono=now_mono - 2_000_000_000,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=perp, kind="perp_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(
                    value=stale_buy,
                    kind="exact_input_quote",
                    now_real=now_real,
                    now_mono=now_mono,
                ),
            )
            await asyncio.sleep(0.04)

            routes = [
                route
                for route in analyzer._route_stats.values()
                if route["analysis_kind"] == "dex_perp_entry_hedge"
            ]
            self.assertEqual(len(routes), 1)
            self.assertEqual(routes[0]["timing_valid"], 0)
            self.assertIsNone(routes[0]["best_timing_valid_entry_basis_cycle"])
            self.assertEqual(
                analyzer.snapshot()["timing"]["max_exact_quote_age_ms"],
                "1500",
            )
            await analyzer.close()

    async def test_freshness_requires_both_realtime_and_monotonic_receipts(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output)
            fresh, ages = analyzer._receipt_freshness(
                now_realtime_ns=now_real,
                now_monotonic_ns=now_mono,
                max_age_ms=Decimal("500"),
                receipts={
                    # This can occur after a suspend or wall-clock jump: one
                    # local clock alone must never extend the quote's TTL.
                    "quote": (now_real - 2_000_000_000, now_mono),
                },
            )
            self.assertFalse(fresh)
            self.assertGreater(ages["quote"]["realtime"] or 0, 500)
            self.assertLessEqual(ages["quote"]["monotonic"] or 0, 500)
            await analyzer.close()

    async def test_cross_cex_spot_inventory_cycle_uses_both_fees(self) -> None:
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        buy_spot = CexTopOfBookEvent(
            venue="MEXC",
            category="spot",
            symbol="SOLUSDT",
            best_bid=Decimal("99"),
            best_bid_size=Decimal("10"),
            best_ask=Decimal("100"),
            best_ask_size=Decimal("10"),
            source="test",
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=1,
            received_realtime_ns=now_real,
            received_monotonic_ns=now_mono,
        )
        sell_spot = CexTopOfBookEvent(
            venue="BINANCE",
            category="spot",
            symbol="SOLUSDT",
            best_bid=Decimal("103"),
            best_bid_size=Decimal("10"),
            best_ask=Decimal("104"),
            best_ask_size=Decimal("10"),
            source="test",
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=1,
            received_realtime_ns=now_real,
            received_monotonic_ns=now_mono,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output, coalesce_interval_ms=1)
            await analyzer.handle_event(
                _event(value=buy_spot, kind="order_book", now_real=now_real, now_mono=now_mono),
            )
            await analyzer.handle_event(
                _event(value=sell_spot, kind="order_book", now_real=now_real, now_mono=now_mono),
            )
            await asyncio.sleep(0.04)
            snapshot = analyzer.snapshot()
            await analyzer.close()

            rows = [
                route["best_timing_valid_cycle"]
                for route in snapshot["top_routes"]
                if route["analysis_kind"] == "spot_spot_inventory_cycle"
            ]
            self.assertTrue(rows)
            cycle = rows[0]
            self.assertTrue(cycle["positive_after_modeled_costs"])
            self.assertTrue(cycle["candidate_eligible"])
            self.assertFalse(cycle["candidate_eligible_with_account_verified_fees"])
            self.assertFalse(cycle["rebalance_cost_included"])
            self.assertGreater(snapshot["counts"]["spot_spot_inventory_cycle_evaluations"], 0)


if __name__ == "__main__":
    unittest.main()
