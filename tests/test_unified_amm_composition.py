"""Offline composition-root checks for the CPMM sequential shadow slice."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab import unified_market_data
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.amm_simulation.backend import WorkerSequentialSimulator, decode_worker_snapshot
from market_data_lab.amm_simulation.backend import LazySnapshotSequentialSimulator
from market_data_lab.amm_simulation.replay import load_evidence_bundle, replay_evidence_bundle
from market_data_lab.perp_venue_feeds import PerpQuoteEvent
from market_data_lab.unified_perp_analyzer import UnifiedPerpAnalyzer


def _bundle() -> dict[str, object]:
    return {
        "schema_version": 1,
        "snapshot_id": "composition-snapshot",
        "worker_generation": 7,
        "source_epoch": 0,
        "boot_id": "composition-boot",
        "model_version": "raydium_cpmm_v1",
        "pool_refs": [{
            "chain_namespace": "solana", "chain_id": "mainnet",
            "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
            "pool_address": "cpmm-pool", "protocol": "raydium_cpmm",
            "protocol_revision": "v1",
            "asset_0_id": "solana:mainnet:stable:6",
            "asset_1_id": "solana:mainnet:base:9", "pool_spec_version": 1,
        }],
        "dependency_vector": [],
        "pools": [{
            "pool_id": "solana:mainnet:cpmm-pool", "protocol": "raydium_cpmm",
            "vault_a_raw": "1000000000", "vault_b_raw": "1000000000",
            "protocol_fees_a_raw": "0", "protocol_fees_b_raw": "0",
            "fund_fees_a_raw": "0", "fund_fees_b_raw": "0",
            "creator_fees_a_raw": "0", "creator_fees_b_raw": "0",
            "trade_fee_rate": "2500", "creator_fee_rate": "120",
            "protocol_fee_rate": "120", "fund_fee_rate": "40", "fee_on": "0",
        }],
        "context_slot": 42,
        "chain_consistency": "validated_multi_account_snapshot",
        "sdk_versions": [["raydium", "test"]],
    }


class _FakeLocalSource:
    name = "fake-local"

    def __init__(self, result: dict[str, object], *, delay_seconds: float = 0.0) -> None:
        self.result = result
        self.delay_seconds = delay_seconds
        self.capture_calls = 0

    def describe(self) -> dict[str, object]:
        return {"source": self.name, "wallet_or_private_key_used": False}

    async def capture_snapshot(self, **_: object) -> dict[str, object]:
        self.capture_calls += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        return self.result


def _config(*, enabled: bool) -> SimpleNamespace:
    return SimpleNamespace(
        amm_simulation=SimpleNamespace(
            enabled=enabled,
            allowed_protocols=("raydium_cpmm",) if enabled else (),
        ),
        sequential_amm_pool=SimpleNamespace(
            pool_id="cpmm-pool",
            stable_asset_id="solana:mainnet:stable:6",
            base_asset_id="solana:mainnet:base:9",
            stable_decimals=6,
            perp_symbol="SOL",
        ),
        raydium_standard_pools=(
            SimpleNamespace(pool_id="cpmm-pool", protocol="raydium_cpmm"),
        ),
        local_route_evaluator=SimpleNamespace(fee_audit_file=None),
        timeout_seconds=1.0,
        rpc_http_url="https://example.invalid",
        rpc_ws_url="wss://example.invalid",
        proxy_url=None,
        retention_seconds=60.0,
        max_events_per_key=100,
        event_bus_capacity=100,
        status_flush_seconds=60.0,
    )


def _event() -> MarketEvent:
    return MarketEvent(
        source="test", key="test", kind="source_health", value={}, summary={},
        received_realtime_ns=1, received_monotonic_ns=1,
    )


class UnifiedAmmCompositionTest(unittest.IsolatedAsyncioTestCase):
    async def _build(self, config: SimpleNamespace, local: _FakeLocalSource):
        with (
            patch.object(
                unified_market_data,
                "build_solana_market_sources",
                return_value=((local,), local),
            ),
            patch.object(unified_market_data, "build_perp_venue_sources", return_value=()),
            patch.object(unified_market_data, "_build_exact_quote_sources", return_value=()),
            patch.object(unified_market_data, "RaydiumLocalQuoteStateSource", _FakeLocalSource),
        ):
            return unified_market_data.build_unified_market_data_scanner(
                config=config,
                output_directory=Path(tempfile.mkdtemp()),
                hyperliquid_coins=(),
            )

    async def test_flag_off_keeps_legacy_composition(self) -> None:
        local = _FakeLocalSource({"status": "ok", "snapshot": _bundle()})
        scanner = await self._build(_config(enabled=False), local)
        self.assertNotIn("sequential_amm", scanner.status_providers)
        await scanner.event_handler(_event())
        self.assertEqual(local.capture_calls, 0)

    async def test_flag_on_success_initializes_fake_worker(self) -> None:
        local = _FakeLocalSource({"status": "ok", "snapshot": _bundle()})
        scanner = await self._build(_config(enabled=True), local)
        self.assertIn("sequential_amm", scanner.status_providers)
        await scanner.event_handler(_event())
        await asyncio.sleep(0)
        self.assertEqual(local.capture_calls, 1)
        status = scanner.status_providers["sequential_amm"]()
        self.assertTrue(status["initialized"])
        self.assertEqual(status["snapshot_id"], "composition-snapshot")
        self.assertEqual(status["worker_generation"], 7)
        self.assertFalse(status["wallet_or_private_key_used"])
        self.assertFalse(status["transactions_submitted"])

        # One matching synthetic perp event must reach the same analyzer
        # instance; no network source or remote quote budget is involved.
        result = WorkerSequentialSimulator(
            snapshot=decode_worker_snapshot(_bundle()),
            pool_ref=decode_worker_snapshot(_bundle()).pools[0].pool_ref,
            stable_asset_id="solana:mainnet:stable:6",
            base_asset_id="solana:mainnet:base:9",
            stable_decimals=6,
            perp_symbol="SOL",
        ).simulate_buy_sell(SimpleNamespace(input_amount_raw=100_000_000))
        assert result is not None
        now_real = time.time_ns()
        now_mono = time.monotonic_ns()
        await scanner.event_handler(MarketEvent(
            source="test", key="perp", kind="perp_book",
            value=PerpQuoteEvent(
                venue="TEST", venue_symbol="SOL-USD", base="SOL", settlement="USDC",
                best_bid=Decimal("102"), best_ask=Decimal("103"),
                funding_rate=None, mark_price=Decimal("102.5"), index_price=Decimal("102.5"),
                received_realtime_ns=now_real, received_monotonic_ns=now_mono,
                best_bid_size=Decimal("10"), best_ask_size=Decimal("10"),
                book_received_realtime_ns=now_real, book_received_monotonic_ns=now_mono,
                context_received_realtime_ns=now_real, context_received_monotonic_ns=now_mono,
                next_funding_time_ms=None, funding_interval_minutes=None,
                funding_rate_kind=None, quantity_step=Decimal("0.00000001"),
                public_taker_fee_bps=Decimal("0"), fee_source="test",
                contract_type="linear_perpetual", execution_model="central_limit_order_book",
            ), summary={}, received_realtime_ns=now_real,
            received_monotonic_ns=now_mono,
        ))
        await scanner.event_handler(MarketEvent(
            source="test", key="quote", kind="exact_input_quote",
            value=ExactInputQuote(
                provider="RAYDIUM", chain="solana", protocol="raydium_cpmm",
                source_kind="local_worker", pair="SOL/USDC", direction="buy_base",
                round_id=1, requested_notional_quote=Decimal("100"),
                reference_notional_usdt=Decimal("100"),
                quote_slot_id="notional:100:buy_base",
                base_amount=Decimal(result.buy_output_raw).scaleb(-9),
                quote_amount=Decimal("100"), input_symbol="USDC", output_symbol="SOL",
                input_amount_raw=result.buy_input_raw, output_amount_raw=result.buy_output_raw,
                average_price_quote_per_base=Decimal("100"), fee_bps=Decimal("20"),
                request_rtt_ms=1, status="ok", error=None,
                response_received_realtime_ns=now_real,
                response_received_monotonic_ns=now_mono, block_number=None,
                source_epoch=0, input_asset_id="solana:mainnet:stable:6",
                output_asset_id="solana:mainnet:base:9",
            ), summary={}, received_realtime_ns=now_real,
            received_monotonic_ns=now_mono,
        ))
        await asyncio.sleep(0.05)
        analysis = scanner.status_providers["perp_analysis"]()
        self.assertGreater(analysis["counts"]["dex_perp_sequential_flat_model_evaluations"], 0)
        rows = [
            route["best_timing_valid_cycle"]
            for route in analysis["top_unqualified_closed_models"]
            if route["analysis_kind"] == "dex_perp_sequential_flat_model"
        ]
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["market_data_fresh"])
        self.assertIsInstance(rows[0]["dex_initial_stable_raw"], int)
        self.assertIsInstance(rows[0]["dex_final_stable_raw"], int)
        self.assertEqual(rows[0]["dex_initial_stable_raw"], result.buy_input_raw)
        self.assertEqual(rows[0]["dex_final_stable_raw"], result.sell_output_raw)

    async def test_flag_on_capture_failure_is_visible_without_fallback(self) -> None:
        local = _FakeLocalSource({"status": "error", "reason": "dependency lock"})
        scanner = await self._build(_config(enabled=True), local)
        await scanner.event_handler(_event())
        await asyncio.sleep(0)
        status = scanner.status_providers["sequential_amm"]()
        self.assertTrue(status["initialized"])
        self.assertIn("snapshot capture failed", status["error"])
        self.assertNotIn("simulator", status)

    async def test_slow_capture_does_not_block_event_handler(self) -> None:
        local = _FakeLocalSource(
            {"status": "ok", "snapshot": _bundle()}, delay_seconds=0.05,
        )
        scanner = await self._build(_config(enabled=True), local)
        started = time.monotonic()
        await scanner.event_handler(_event())
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 0.03)
        await asyncio.sleep(0.07)
        self.assertEqual(local.capture_calls, 1)

    async def test_capture_failure_retries_after_cooldown(self) -> None:
        class FlakySource(_FakeLocalSource):
            async def capture_snapshot(self, **kwargs: object) -> dict[str, object]:
                self.capture_calls += 1
                if self.capture_calls == 1:
                    return {"status": "error", "reason": "worker not ready"}
                return {"status": "ok", "snapshot": _bundle()}

        local = FlakySource({})
        simulator = LazySnapshotSequentialSimulator(
            source=local, pool_id="cpmm-pool",
            stable_asset_id="solana:mainnet:stable:6",
            base_asset_id="solana:mainnet:base:9",
            stable_decimals=6, retry_cooldown_seconds=0.01,
        )
        await simulator.initialize()
        self.assertEqual(local.capture_calls, 1)
        self.assertEqual(simulator.describe()["failures"], 1)
        await simulator.initialize()
        self.assertEqual(local.capture_calls, 1)
        await asyncio.sleep(0.02)
        await simulator.initialize()
        status = simulator.describe()
        self.assertEqual(local.capture_calls, 2)
        self.assertEqual(status["successes"], 1)

    async def test_expired_snapshot_refreshes(self) -> None:
        expiring = dict(_bundle())
        expiring["state_valid_until_monotonic_ns"] = str(time.monotonic_ns() + 20_000_000)
        local = _FakeLocalSource({"status": "ok", "snapshot": expiring})
        simulator = LazySnapshotSequentialSimulator(
            source=local, pool_id="cpmm-pool",
            stable_asset_id="solana:mainnet:stable:6",
            base_asset_id="solana:mainnet:base:9", stable_decimals=6,
        )
        await simulator.initialize()
        self.assertEqual(local.capture_calls, 1)
        await asyncio.sleep(0.03)
        await simulator.initialize()
        self.assertEqual(local.capture_calls, 2)

    def test_evidence_artifact_is_replayable_and_epoch_is_worker_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = decode_worker_snapshot(_bundle())
            simulator = WorkerSequentialSimulator(
                snapshot=snapshot, pool_ref=snapshot.pools[0].pool_ref,
                stable_asset_id="solana:mainnet:stable:6",
                base_asset_id="solana:mainnet:base:9", stable_decimals=6,
                perp_symbol="SOL",
            )
            result = simulator.simulate_buy_sell(SimpleNamespace(input_amount_raw=100_000_000))
            assert result is not None
            now = time.time_ns()
            quote = ExactInputQuote(
                provider="RAYDIUM", chain="solana", protocol="raydium_cpmm",
                source_kind="local_worker", pair="SOL/USDC", direction="buy_base",
                round_id=1, requested_notional_quote=Decimal("100"),
                reference_notional_usdt=Decimal("100"),
                quote_slot_id="notional:100:buy_base",
                base_amount=None,
                quote_amount=Decimal("100"), input_symbol="USDC", output_symbol="SOL",
                input_amount_raw=result.buy_input_raw, output_amount_raw=None,
                average_price_quote_per_base=None, fee_bps=None, request_rtt_ms=1,
                status="ok", error=None, response_received_realtime_ns=now,
                response_received_monotonic_ns=time.monotonic_ns(), block_number=None,
                source_epoch=999, input_asset_id="solana:mainnet:stable:6",
                output_asset_id="solana:mainnet:base:9",
            )
            analyzer = UnifiedPerpAnalyzer(
                output_directory=Path(directory), sequential_amm_simulator=simulator,
            )
            # Polling source epoch is deliberately unrelated to worker epoch.
            self.assertIsNotNone(analyzer._sequential_result_for(quote))
            path = analyzer._sequential_evidence_paths[result.evidence_hash]
            loaded = load_evidence_bundle(path)
            replayed = replay_evidence_bundle(loaded)
            self.assertTrue(replayed.complete)

    def test_live_cpmm_binding_uses_local_output_not_quote_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            from market_data_lab.unified_perp_analyzer import UnifiedPerpAnalyzer

            snapshot = decode_worker_snapshot(_bundle())
            simulator = WorkerSequentialSimulator(
                snapshot=snapshot,
                pool_ref=snapshot.pools[0].pool_ref,
                stable_asset_id="solana:mainnet:stable:6",
                base_asset_id="solana:mainnet:base:9",
                stable_decimals=6,
                perp_symbol="BASE",
            )
            result = simulator.simulate_buy_sell(SimpleNamespace(input_amount_raw=100_000_000))
            assert result is not None
            now = time.time_ns()
            quote = ExactInputQuote(
                provider="RAYDIUM", chain="solana", protocol="raydium_cpmm",
                source_kind="local_worker", pair="BASE/USDC", direction="buy_base",
                round_id=1, requested_notional_quote=Decimal("100"),
                reference_notional_usdt=Decimal("100"),
                quote_slot_id="notional:100:buy_base",
                base_amount=Decimal(result.buy_output_raw).scaleb(-9),
                quote_amount=Decimal("100"), input_symbol="USDC", output_symbol="BASE",
                input_amount_raw=result.buy_input_raw, output_amount_raw=result.buy_output_raw + 1,
                average_price_quote_per_base=Decimal("100"), fee_bps=Decimal("20"),
                request_rtt_ms=1, status="ok", error=None,
                response_received_realtime_ns=now,
                response_received_monotonic_ns=time.monotonic_ns(), block_number=None,
                source_epoch=0, input_asset_id="solana:mainnet:stable:6",
                output_asset_id="solana:mainnet:base:9",
            )
            analyzer = UnifiedPerpAnalyzer(
                output_directory=Path(directory), sequential_amm_simulator=simulator,
            )
            # The external quote's output is deliberately different; local
            # immutable post-state output remains authoritative.
            self.assertIsNotNone(analyzer._sequential_result_for(quote))

            wrong_asset_quote = replace(
                quote,
                output_asset_id="solana:mainnet:different-base:9",
            )
            self.assertIsNone(analyzer._sequential_result_for(wrong_asset_quote))


if __name__ == "__main__":
    unittest.main()
