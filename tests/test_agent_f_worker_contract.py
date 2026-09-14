from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from market_data_lab.solana_quote_worker import QuoteWorkerPool, RaydiumLocalQuoteWorker
from market_data_lab.solana_realtime_scanner import (
    RaydiumLocalQuoteStateSource,
    RaydiumLocalQuoteWorkerConfig,
)


class _FakeWorker:
    message: dict[str, object]

    def __init__(self, **_: object) -> None:
        self.closed = False

    async def start(self, **_: object) -> dict[str, object]:
        return {"type": "ready"}

    async def next_event(self) -> dict[str, object]:
        return self.message

    async def close(self) -> None:
        self.closed = True


def _source() -> RaydiumLocalQuoteStateSource:
    return RaydiumLocalQuoteStateSource(
        pools=(),
        raydium_standard_pools=(),
        meteora_pools=(),
        orca_pools=(),
        http_url="https://rpc.example",
        ws_url="wss://rpc.example",
        timeout_seconds=1.0,
        tick_cache_max_age_ms=300_000,
        state_snapshot_refresh_interval_ms=15_000,
        rpc_http_min_request_interval_ms=200,
    )


class AgentFWorkerContractTest(unittest.IsolatedAsyncioTestCase):
    def test_stale_refresh_and_emit_controls_are_exposed_and_validated(self) -> None:
        worker = RaydiumLocalQuoteWorker(
            rpc_http_url="https://rpc.example",
            rpc_ws_url="wss://rpc.example",
            pools=(QuoteWorkerPool("pool-1", "POOL"),),
            core_refresh_after_ms=20_000,
            maintenance_scan_interval_ms=500,
            refresh_stagger_window_ms=4_000,
            pool_state_emit_min_interval_ms=50,
        )
        descriptor = worker.safe_descriptor()
        self.assertEqual(descriptor["core_refresh_after_ms"], 20_000)
        self.assertEqual(descriptor["maintenance_scan_interval_ms"], 500)
        self.assertEqual(descriptor["refresh_stagger_window_ms"], 4_000)
        self.assertEqual(descriptor["pool_state_emit_min_interval_ms"], 50)
        with self.assertRaisesRegex(ValueError, "refresh_stagger_window_ms"):
            RaydiumLocalQuoteWorkerConfig(
                core_refresh_after_ms=1_000,
                refresh_stagger_window_ms=1_001,
            )

    async def test_pool_state_keeps_core_and_dependency_provenance_separate(self) -> None:
        _FakeWorker.message = {
            "type": "pool_state",
            "protocol": "raydium_clmm",
            "pool_id": "pool-1",
            "label": "SOL/USDC",
            "slot": 105,
            "core_state_slot": 105,
            "dependency_slot_min": 108,
            "dependency_slot_max": 110,
            "dependency_generation": 7,
            "token_a_mint": "mint-a",
            "token_b_mint": "mint-b",
            "token_a_decimals": 9,
            "token_b_decimals": 6,
            "tick_current": 12,
            "sqrt_price_x64": "123",
            "tick_cache_age_ms": 5,
        }
        stop = asyncio.Event()
        events = []

        async def publish(event: object) -> None:
            events.append(event)
            stop.set()

        with patch(
            "market_data_lab.solana_realtime_scanner.RaydiumLocalQuoteWorker",
            _FakeWorker,
        ):
            await _source().run(publish, stop)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.chain_position, 105)
        self.assertEqual(event.summary["core_state_slot"], 105)
        self.assertEqual(event.summary["dependency_slot_max"], 110)
        self.assertEqual(event.summary["dependency_generation"], 7)

    async def test_legacy_slot_cannot_claim_dependency_max(self) -> None:
        _FakeWorker.message = {
            "type": "pool_state",
            "protocol": "raydium_clmm",
            "pool_id": "pool-1",
            "label": "SOL/USDC",
            "slot": 110,
            "core_state_slot": 105,
            "dependency_slot_min": 108,
            "dependency_slot_max": 110,
            "dependency_generation": 7,
            "token_a_mint": "mint-a",
            "token_b_mint": "mint-b",
            "token_a_decimals": 9,
            "token_b_decimals": 6,
            "tick_current": 12,
            "sqrt_price_x64": "123",
        }
        stop = asyncio.Event()

        async def publish(_: object) -> None:
            stop.set()

        with patch(
            "market_data_lab.solana_realtime_scanner.RaydiumLocalQuoteWorker",
            _FakeWorker,
        ):
            with self.assertRaisesRegex(RuntimeError, "provenance"):
                await _source().run(publish, stop)


if __name__ == "__main__":
    unittest.main()
