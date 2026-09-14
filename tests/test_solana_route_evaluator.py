from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RollingStateStore
from market_data_lab.solana_route_evaluator import LocalRouteEvaluatorConfig
from market_data_lab.solana_route_evaluator import LocalSpotRoute
from market_data_lab.solana_route_evaluator import CexDepthState
from market_data_lab.solana_route_evaluator import SolanaRouteEvaluator


class _FakeQuoteSource:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    async def quote_exact_input(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs)
        if kwargs["input_mint"] == "bridge-mint":
            # 0.1 bridge -> 1.1 base, both with the configured raw decimals.
            output = "1100000"
        else:
            # 1 base -> 0.08 bridge.
            output = "80000000"
        return {
            "status": "ok",
            "state_slot": kwargs["minimum_state_slot"],
            "output_amount_raw": output,
            "pool_fee_raw": "0",
            "price_impact_pct": "0",
            "all_trade": True,
            "tick_cache_age_ms": 1,
            "chain_time_age_ms": 1,
        }


class _FakeGatedVerifier:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def verify_exact_input(self, **kwargs: Any) -> dict[str, object]:
        self.calls.append(kwargs)
        return {
            "status": "ok",
            "source": "fake_jupiter",
            "out_amount_raw": "123",
            "transaction_returned_nonempty": False,
        }

    def snapshot(self) -> dict[str, object]:
        return {"requests": len(self.calls), "transaction_requested": False}


def _book(symbol: str, *, bids: list[tuple[str, str]], asks: list[tuple[str, str]]) -> BookSnapshot:
    now = time.time_ns()
    return BookSnapshot(
        symbol=symbol,
        category="spot",
        status="ok",
        error=None,
        bids=tuple((Decimal(price), Decimal(size)) for price, size in bids),
        asks=tuple((Decimal(price), Decimal(size)) for price, size in asks),
        exchange_system_time_ms=None,
        matching_engine_time_ms=None,
        update_id=1,
        cross_sequence=None,
        response=TimedResponse(
            payload=None,
            error=None,
            sent_realtime_ns=now,
            received_realtime_ns=now,
            sent_monotonic_ns=time.monotonic_ns(),
            received_monotonic_ns=time.monotonic_ns(),
        ),
        source="test",
    )


def _event(*, key: str, value: Any, summary: dict[str, Any], slot: int | None = None) -> MarketEvent:
    realtime_ns = time.time_ns()
    monotonic_ns = time.monotonic_ns()
    return MarketEvent(
        source="test:source",
        key=key,
        kind="test",
        value=value,
        summary=summary,
        received_realtime_ns=realtime_ns,
        received_monotonic_ns=monotonic_ns,
        chain_position=slot,
        event_id=f"test:{key}:{monotonic_ns}",
    )


class _FakeDepthProvider:
    def __init__(self, pairs: tuple[tuple[MarketEvent, BookSnapshot], ...]) -> None:
        self.depths = {
            book.symbol: CexDepthState(
                book=book,
                source=event.source,
                source_epoch=event.source_epoch,
                event_id=event.event_id or "missing",
                received_realtime_ns=event.received_realtime_ns,
                received_monotonic_ns=event.received_monotonic_ns,
            )
            for event, book in pairs
        }

    def latest_depth(self, venue: str, symbol: str) -> CexDepthState | None:
        return self.depths.get(symbol) if venue == "MEXC" else None


class LocalRouteEvaluatorTest(unittest.IsolatedAsyncioTestCase):
    def _route(self) -> LocalSpotRoute:
        return LocalSpotRoute(
            route_id="base-bridge-test",
            pool_id="pool",
            base_mint="base-mint",
            bridge_mint="bridge-mint",
            base_decimals=6,
            bridge_decimals=9,
            base_symbol="BASE",
            bridge_symbol="BRIDGE",
            settlement_symbol="USDT",
            cex_venue="MEXC",
            base_cex_symbol="BASEUSDT",
            bridge_cex_symbol="BRIDGEUSDT",
            bridge_is_settlement=False,
            notional_settlement=Decimal("10"),
            base_buy_taker_fee_bps=Decimal("10"),
            base_sell_taker_fee_bps=Decimal("10"),
            bridge_buy_taker_fee_bps=Decimal("10"),
            bridge_sell_taker_fee_bps=Decimal("10"),
            network_cost_floor_settlement=Decimal("0.01"),
            asset_equivalence="test-only same underlying assets",
        )

    async def test_exact_pool_quote_and_two_cex_depth_legs_create_compact_screen_event(self) -> None:
        route = self._route()
        store = RollingStateStore(retention_seconds=60, max_events_per_key=8)
        pool = _event(
            key=route.pool_state_key,
            value={"not_serialized": True},
            summary={
                "token_a_mint": "base-mint",
                "token_b_mint": "bridge-mint",
                "token_a_decimals": 6,
                "token_b_decimals": 9,
                "slot": 42,
            },
            slot=42,
        )
        base_book = _book("BASEUSDT", bids=[("9.90", "100")], asks=[("10", "100")])
        base = _event(
            key=route.base_book_key,
            value={"compact_bbo_only": True},
            summary={},
        )
        bridge_book = _book("BRIDGEUSDT", bids=[("100", "100")], asks=[("100", "100")])
        bridge = _event(
            key=route.bridge_book_key or "unexpected",
            value={"compact_bbo_only": True},
            summary={},
        )
        for event in (pool, base, bridge):
            store.add(event)

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            output.mkdir()
            source = _FakeQuoteSource()
            verifier = _FakeGatedVerifier()
            evaluator = SolanaRouteEvaluator(
                config=LocalRouteEvaluatorConfig(routes=(route,), candidate_improvement_bps=Decimal("1")),
                store=store,
                quote_source=source,
                output_directory=output,
                depth_provider=_FakeDepthProvider(((base, base_book), (bridge, bridge_book))),
                gated_verifier=verifier,
            )
            runtime = evaluator._runtime[route.route_id]
            await evaluator._evaluate_route(route, runtime)

            self.assertEqual(len(source.requests), 2)
            self.assertEqual(source.requests[0]["input_mint"], "bridge-mint")
            self.assertEqual(source.requests[0]["minimum_state_slot"], 42)
            self.assertEqual(len(verifier.calls), 1)
            self.assertEqual(verifier.calls[0]["input_mint"], "bridge-mint")
            self.assertEqual(runtime.positive_evaluations, 1)
            status = evaluator.snapshot()
            self.assertEqual(status["candidate_lifecycle"]["started"], 1)
            self.assertFalse(status["routes"][route.route_id]["fee_account_verified"])

            rows = [json.loads(line) for line in (output / "candidate_events.jsonl").read_text().splitlines()]
            self.assertEqual(rows[0]["event"], "candidate_started")
            best = rows[0]["best_cycle"]
            self.assertEqual(best["direction"], "buy_dex_base_sell_cex_base")
            self.assertFalse(best["cex_fee_account_verified"])
            self.assertEqual(best["jupiter_verification"]["source"], "fake_jupiter")
            self.assertNotIn("bids", json.dumps(rows))
            self.assertNotIn("not_serialized", json.dumps(rows))

            # Fresh at dispatch, stale after awaiting the quote worker: neither
            # direction may count as valid or create another positive event.
            valid_before = runtime.timing_valid_evaluations
            positive_before = runtime.positive_evaluations
            with patch.object(evaluator, "_validate_freshness", side_effect=[
                None, "cex_book_stale", "cex_book_stale",
            ]):
                await evaluator._evaluate_route(route, runtime)
            self.assertEqual(runtime.timing_valid_evaluations, valid_before)
            self.assertEqual(runtime.positive_evaluations, positive_before)
            self.assertEqual(runtime.last_status, "cex_book_stale")

    async def test_stale_pool_state_does_not_call_quote_worker(self) -> None:
        route = self._route()
        store = RollingStateStore(retention_seconds=60, max_events_per_key=8)
        stale_monotonic = time.monotonic_ns() - 5_000_000_000
        pool = MarketEvent(
            source="test",
            key=route.pool_state_key,
            kind="pool",
            value={},
            summary={
                "token_a_mint": "base-mint",
                "token_b_mint": "bridge-mint",
                "token_a_decimals": 6,
                "token_b_decimals": 9,
            },
            received_realtime_ns=time.time_ns() - 5_000_000_000,
            received_monotonic_ns=stale_monotonic,
            chain_position=1,
        )
        store.add(pool)
        base_book = _book("BASEUSDT", bids=[("9", "100")], asks=[("10", "100")])
        base = _event(key=route.base_book_key, value={"compact_bbo_only": True}, summary={})
        bridge_book = _book("BRIDGEUSDT", bids=[("99", "100")], asks=[("100", "100")])
        bridge = _event(key=route.bridge_book_key or "unexpected", value={"compact_bbo_only": True}, summary={})
        store.add(base)
        store.add(bridge)

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            output.mkdir()
            source = _FakeQuoteSource()
            depth_provider = _FakeDepthProvider(((base, base_book), (bridge, bridge_book)))
            evaluator = SolanaRouteEvaluator(
                config=LocalRouteEvaluatorConfig(routes=(route,), maximum_pool_state_age_ms=100),
                store=store,
                quote_source=source,
                output_directory=output,
                depth_provider=depth_provider,
            )
            runtime = evaluator._runtime[route.route_id]
            await evaluator._evaluate_route(route, runtime)
            self.assertEqual(source.requests, [])
            self.assertEqual(runtime.last_status, "pool_state_stale")

    async def test_quiet_pool_is_not_rejected_as_cross_venue_timing_skew(self) -> None:
        route = self._route()
        store = RollingStateStore(retention_seconds=60, max_events_per_key=8)
        pool = MarketEvent(
            source="test",
            key=route.pool_state_key,
            kind="pool",
            value={},
            summary={
                "token_a_mint": "base-mint",
                "token_b_mint": "bridge-mint",
                "token_a_decimals": 6,
                "token_b_decimals": 9,
            },
            received_realtime_ns=time.time_ns() - 5_000_000_000,
            received_monotonic_ns=time.monotonic_ns() - 5_000_000_000,
            chain_position=1,
        )
        base_book = _book("BASEUSDT", bids=[("9", "100")], asks=[("10", "100")])
        base = _event(
            key=route.base_book_key,
            value={"compact_bbo_only": True},
            summary={},
        )
        bridge_book = _book("BRIDGEUSDT", bids=[("99", "100")], asks=[("100", "100")])
        bridge = _event(
            key=route.bridge_book_key or "unexpected",
            value={"compact_bbo_only": True},
            summary={},
        )
        for event in (pool, base, bridge):
            store.add(event)

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            output.mkdir()
            source = _FakeQuoteSource()
            depth_provider = _FakeDepthProvider(((base, base_book), (bridge, bridge_book)))
            evaluator = SolanaRouteEvaluator(
                config=LocalRouteEvaluatorConfig(
                    routes=(route,),
                    maximum_pool_state_age_ms=10_000,
                    maximum_timing_skew_ms=100,
                ),
                store=store,
                quote_source=source,
                output_directory=output,
                depth_provider=depth_provider,
            )
            runtime = evaluator._runtime[route.route_id]
            await evaluator._evaluate_route(route, runtime)

            self.assertEqual(len(source.requests), 2)
            self.assertEqual(runtime.timing_valid_evaluations, 2)

            skewed_bridge = replace(
                bridge,
                received_realtime_ns=base.received_realtime_ns - 500_000_000,
                received_monotonic_ns=time.monotonic_ns(),
            )
            depth_provider.depths[bridge_book.symbol] = CexDepthState(
                book=bridge_book,
                source=skewed_bridge.source,
                source_epoch=skewed_bridge.source_epoch,
                event_id=skewed_bridge.event_id or "missing",
                received_realtime_ns=skewed_bridge.received_realtime_ns,
                received_monotonic_ns=skewed_bridge.received_monotonic_ns,
            )
            self.assertEqual(
                evaluator._validate_freshness(route, pool, base, skewed_bridge),
                "timing_skew_exceeded",
            )


if __name__ == "__main__":
    unittest.main()
