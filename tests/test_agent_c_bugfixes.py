"""Literal acceptance coverage for Agent C BUG-003/004/021/022."""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from market_data_lab import solana_realtime_scanner
from market_data_lab import unified_market_data
from market_data_lab.cex_book_streams import BybitOrderBookStream
from market_data_lab.cex_book_streams import ShardedPublicBookStream
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RealtimeScanner
from market_data_lab.realtime_scanner import SourceEpochChange
from market_data_lab.solana_realtime_scanner import AmmSimulationConfig
from market_data_lab.solana_realtime_scanner import CexBookStateSource
from market_data_lab.solana_realtime_scanner import CexStreamConfig
from market_data_lab.solana_realtime_scanner import JupiterConfig
from market_data_lab.solana_realtime_scanner import LocalRouteEvaluatorSettings
from market_data_lab.solana_realtime_scanner import CexTopOfBookEvent
from market_data_lab.solana_route_evaluator import LocalSpotRoute
from market_data_lab.unified_cycle_analyzer import UnifiedCycleAnalyzer
from market_data_lab.unified_perp_analyzer import UnifiedPerpAnalyzer
from market_data_lab.perp_venue_feeds import PerpQuoteEvent


def _epoch(source: str, value: int, *, reason: str = "restart") -> SourceEpochChange:
    return SourceEpochChange(
        source=source,
        source_epoch=value,
        previous_source_epoch=value - 1,
        reason=reason,  # type: ignore[arg-type]
        realtime_ns=time.time_ns(),
        monotonic_ns=time.monotonic_ns(),
    )


def _book(symbol: str, *, bid: str = "10", ask: str = "11", update_id: int = 1) -> BookSnapshot:
    realtime_ns = time.time_ns()
    monotonic_ns = time.monotonic_ns()
    return BookSnapshot(
        symbol=symbol,
        category="spot",
        status="ok",
        error=None,
        bids=((Decimal(bid), Decimal("100")),),
        asks=((Decimal(ask), Decimal("100")),),
        exchange_system_time_ms=None,
        matching_engine_time_ms=None,
        update_id=update_id,
        cross_sequence=None,
        response=TimedResponse(
            payload=None,
            error=None,
            sent_realtime_ns=realtime_ns,
            received_realtime_ns=realtime_ns,
            sent_monotonic_ns=monotonic_ns,
            received_monotonic_ns=monotonic_ns,
        ),
        source="test_depth",
    )


def _exact_quote(*, epoch: int) -> ExactInputQuote:
    market = MARKETS["SOL_SOLANA_RAYDIUM"]
    realtime_ns = time.time_ns()
    monotonic_ns = time.monotonic_ns()
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
        quote_slot_id="notional:100",
        base_amount=Decimal("1"),
        quote_amount=Decimal("100"),
        input_symbol="USDC",
        output_symbol="SOL",
        input_amount_raw=100_000_000,
        output_amount_raw=1_000_000_000,
        average_price_quote_per_base=Decimal("100"),
        fee_bps=Decimal("20"),
        request_rtt_ms=1,
        status="ok",
        error=None,
        response_received_realtime_ns=realtime_ns,
        response_received_monotonic_ns=monotonic_ns,
        block_number=None,
        source_epoch=epoch,
    )


def _market_event(source: str, value: object, *, kind: str, epoch: int, key: str = "test") -> MarketEvent:
    return MarketEvent(
        source=source,
        key=key,
        kind=kind,
        value=value,
        summary={},
        received_realtime_ns=time.time_ns(),
        received_monotonic_ns=time.monotonic_ns(),
        source_epoch=epoch,
    )


class SourceEpochAcceptanceTest(unittest.IsolatedAsyncioTestCase):
    async def test_async_control_plane_finishes_before_first_new_epoch_event(self) -> None:
        order: list[str] = []

        class _Source:
            name = "test:ordered"

            def describe(self) -> dict[str, object]:
                return {"source": self.name}

            async def run(self, publish: Any, stop_event: asyncio.Event) -> None:
                order.append("source_run")
                await publish(_market_event(self.name, {}, kind="book", epoch=0))
                stop_event.set()

        async def epoch_handler(change: SourceEpochChange) -> None:
            self.assertEqual(change.reason, "initial_start")
            order.append("epoch_handler_started")
            await asyncio.sleep(0)
            order.append("epoch_handler_finished")

        with tempfile.TemporaryDirectory() as directory:
            scanner = RealtimeScanner(
                sources=(_Source(),),
                output_directory=Path(directory) / "run",
                epoch_change_handlers=(epoch_handler,),
                status_flush_seconds=0.01,
            )
            await scanner.run()
        self.assertEqual(
            order[:3],
            ["epoch_handler_started", "epoch_handler_finished", "source_run"],
        )
        self.assertEqual(scanner.store.latest("test").source_epoch, 1)  # type: ignore[union-attr]

    async def test_cycle_analyzer_rejects_old_and_future_events_after_transition(self) -> None:
        cex = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("SOLUSDC",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedCycleAnalyzer(output_directory=output, cex_sources=(cex,))
            source = "dexquote:RAYDIUM"
            analyzer.handle_source_epoch_change(_epoch(source, 1, reason="initial_start"))
            quote1 = _exact_quote(epoch=1)
            event1 = _market_event(source, quote1, kind="exact_input_quote", epoch=1)
            await analyzer.handle_event(event1)
            key = analyzer._quote_key(quote1)
            self.assertIn(key, analyzer._direct_quotes)

            analyzer.handle_source_epoch_change(_epoch(source, 2))
            self.assertNotIn(key, analyzer._direct_quotes)
            self.assertFalse(analyzer._dirty_direct)
            self.assertFalse(analyzer._active)
            cex.handle_source_epoch(_epoch(cex.name, 1, reason="initial_start"))
            analyzer.handle_source_epoch_change(_epoch(cex.name, 1, reason="initial_start"))
            cex._latest_books["SOLUSDC"] = _book("SOLUSDC", bid="101", ask="102")
            cex_event = CexTopOfBookEvent.from_book(
                venue="MEXC",
                book=cex._latest_books["SOLUSDC"],
            )
            with patch.object(analyzer, "_evaluate_direct") as calculate:
                await analyzer.handle_event(
                    _market_event(cex.name, cex_event, kind="order_book", epoch=1),
                )
                await analyzer._drain_dirty()
                calculate.assert_not_called()
            await analyzer.handle_event(event1)
            self.assertNotIn(key, analyzer._direct_quotes)
            self.assertGreater(analyzer._counts["old_epoch_events_rejected"], 0)

            with self.assertRaisesRegex(RuntimeError, "before SourceEpochChange"):
                await analyzer.handle_event(
                    _market_event(source, _exact_quote(epoch=3), kind="exact_input_quote", epoch=3),
                )
            quote2 = _exact_quote(epoch=2)
            await analyzer.handle_event(
                _market_event(source, quote2, kind="exact_input_quote", epoch=2),
            )
            self.assertIn(analyzer._quote_key(quote2), analyzer._direct_quotes)
            await analyzer.close()

    async def test_perp_analyzer_purges_spot_perp_and_dex_legs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            output.mkdir()
            analyzer = UnifiedPerpAnalyzer(output_directory=output)
            now_real = time.time_ns()
            now_mono = time.monotonic_ns()
            spot_source = "cex:TEST:spot"
            perp_source = "perp:test"
            dex_source = "dexquote:RAYDIUM"
            for source in (spot_source, perp_source, dex_source):
                analyzer.handle_source_epoch_change(_epoch(source, 1, reason="initial_start"))
            spot = CexTopOfBookEvent(
                venue="TEST", category="spot", symbol="SOLUSDC",
                best_bid=Decimal("100"), best_bid_size=Decimal("10"),
                best_ask=Decimal("101"), best_ask_size=Decimal("10"),
                source="test", exchange_system_time_ms=None,
                matching_engine_time_ms=None, update_id=1,
                received_realtime_ns=now_real, received_monotonic_ns=now_mono,
            )
            perp = PerpQuoteEvent(
                venue="TESTPERP", venue_symbol="SOL-USD", base="SOL", settlement="USDC",
                best_bid=Decimal("100"), best_ask=Decimal("101"),
                funding_rate=None, mark_price=Decimal("100"), index_price=Decimal("100"),
                received_realtime_ns=now_real, received_monotonic_ns=now_mono,
                best_bid_size=Decimal("10"), best_ask_size=Decimal("10"),
                book_received_realtime_ns=now_real, book_received_monotonic_ns=now_mono,
                context_received_realtime_ns=None, context_received_monotonic_ns=None,
                next_funding_time_ms=None, funding_interval_minutes=None,
                funding_rate_kind=None, quantity_step=Decimal("0.001"),
                public_taker_fee_bps=Decimal("1"), fee_source="test",
                contract_type="linear_perpetual", execution_model="central_limit_order_book",
            )
            await analyzer.handle_event(_market_event(spot_source, spot, kind="order_book", epoch=1))
            await analyzer.handle_event(_market_event(perp_source, perp, kind="perp_book", epoch=1))
            quote = _exact_quote(epoch=1)
            await analyzer.handle_event(
                _market_event(dex_source, quote, kind="exact_input_quote", epoch=1),
            )
            self.assertTrue(analyzer._spots)
            self.assertTrue(analyzer._perps)
            self.assertTrue(analyzer._dex_quotes)

            analyzer.handle_source_epoch_change(_epoch(spot_source, 2))
            analyzer.handle_source_epoch_change(_epoch(perp_source, 2))
            analyzer.handle_source_epoch_change(_epoch(dex_source, 2))
            self.assertFalse(analyzer._spots)
            self.assertFalse(analyzer._perps)
            self.assertFalse(analyzer._dex_quotes)
            await analyzer.handle_event(_market_event(spot_source, spot, kind="order_book", epoch=1))
            self.assertFalse(analyzer._spots)
            await analyzer.close()


class _FakeShard:
    def __init__(self, symbol: str, *, failure: Exception | None = None) -> None:
        self.symbols = (symbol,)
        self.failure = failure
        self.closed = False
        self.wait = asyncio.Event()

    async def start(self) -> None:
        return None

    async def next_update(self) -> BookSnapshot:
        if self.failure is not None:
            raise self.failure
        await self.wait.wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True
        self.wait.set()

    def nearest_snapshot(self, symbol: str, target_realtime_ns: int) -> None:
        return None


class CexReconnectAcceptanceTest(unittest.IsolatedAsyncioTestCase):
    async def test_evented_stream_next_update_surfaces_dead_receiver(self) -> None:
        snapshot = json.dumps({
            "topic": "orderbook.50.SOLUSDT", "type": "snapshot",
            "data": {"s": "SOLUSDT", "b": [["100", "1"]], "a": [["101", "1"]], "u": 1},
        })

        class _Socket:
            def __init__(self) -> None:
                self.items: list[object] = [snapshot, ConnectionError("socket died")]

            async def send(self, payload: object) -> None:
                return None

            async def recv(self) -> object:
                item = self.items.pop(0)
                if isinstance(item, Exception):
                    raise item
                return item

            async def close(self) -> None:
                return None

        socket = _Socket()

        async def connect(*_args: object, **_kwargs: object) -> _Socket:
            return socket

        stream = BybitOrderBookStream(
            ("SOLUSDT",), timeout_seconds=1, proxy_url=None, connect_websocket=connect,
        )
        try:
            await stream.start()
            self.assertEqual((await stream.next_update()).symbol, "SOLUSDT")
            with self.assertRaisesRegex(RuntimeError, "transport failed"):
                await asyncio.wait_for(stream.next_update(), timeout=0.1)
        finally:
            await stream.close()

    async def test_disconnect_restarts_source_with_new_epoch_and_cleared_depth(self) -> None:
        first_book = _book("BASEUSDT", update_id=1)
        second_book = _book("BASEUSDT", update_id=2)

        class _Session:
            symbols = ("BASEUSDT",)

            def __init__(self, book: BookSnapshot, *, disconnect: bool) -> None:
                self.book = book
                self.disconnect = disconnect
                self.delivered = False
                self.release = asyncio.Event()

            async def start(self) -> None:
                return None

            async def next_update(self) -> BookSnapshot:
                if not self.delivered:
                    if not self.disconnect:
                        await self.release.wait()
                    self.delivered = True
                    return self.book
                if self.disconnect:
                    raise ConnectionError("simulated socket disconnect")
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            async def close(self) -> None:
                return None

            def nearest_snapshot(self, symbol: str, target_realtime_ns: int) -> BookSnapshot | None:
                return self.book if self.delivered else None

        first = _Session(first_book, disconnect=True)
        second = _Session(second_book, disconnect=False)
        sessions = iter((first, second))
        source = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("BASEUSDT",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        source.supervisor_retry_initial_seconds = 0.001
        source.supervisor_retry_max_seconds = 0.001
        changes: list[SourceEpochChange] = []
        events: list[MarketEvent] = []
        epoch_two = asyncio.Event()

        def observe_epoch(change: SourceEpochChange) -> None:
            changes.append(change)
            if change.source_epoch == 2:
                self.assertIsNone(scanner.store.latest(f"{source.name}:BASEUSDT"))
                self.assertIsNone(source.latest_depth("MEXC", "BASEUSDT"))
                epoch_two.set()

        async def observe_event(event: MarketEvent) -> None:
            events.append(event)

        with tempfile.TemporaryDirectory() as directory, patch.object(
            solana_realtime_scanner,
            "build_public_book_stream",
            side_effect=lambda *_args, **_kwargs: next(sessions),
        ):
            scanner = RealtimeScanner(
                sources=(source,),
                output_directory=Path(directory) / "run",
                event_handler=observe_event,
                epoch_change_handlers=(source.handle_source_epoch, observe_epoch),
                status_flush_seconds=0.01,
            )
            run_task = asyncio.create_task(scanner.run())
            await asyncio.wait_for(epoch_two.wait(), timeout=1)
            second.release.set()
            deadline = time.monotonic() + 1
            while len(events) < 2 and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            scanner.stop_event.set()
            await asyncio.wait_for(run_task, timeout=1)

        self.assertEqual([event.source_epoch for event in events[:2]], [1, 2])
        self.assertEqual(changes[0].reason, "initial_start")
        self.assertEqual(changes[1].reason, "transport_reconnect")
        self.assertIs(source.latest_depth("MEXC", "BASEUSDT").book, second_book)  # type: ignore[union-attr]

    async def test_one_shard_failure_terminates_whole_logical_stream(self) -> None:
        failed = _FakeShard("A", failure=ConnectionError("shard disconnected"))
        healthy = _FakeShard("B")
        stream = ShardedPublicBookStream((failed, healthy))
        await stream.start()
        with self.assertRaisesRegex(RuntimeError, "shard transport failed"):
            await asyncio.wait_for(stream.next_update(), timeout=0.2)
        await stream.close()
        self.assertTrue(failed.closed)
        self.assertTrue(healthy.closed)

    def test_bybit_delta_requires_fresh_snapshot_after_session_reset(self) -> None:
        stream = BybitOrderBookStream(
            ("SOLUSDT",), timeout_seconds=1, proxy_url=None,
        )
        delta = json.dumps({
            "topic": "orderbook.50.SOLUSDT", "type": "delta",
            "data": {"s": "SOLUSDT", "b": [["100", "1"]], "a": [["101", "1"]], "u": 1},
        })
        snapshot = json.dumps({
            "topic": "orderbook.50.SOLUSDT", "type": "snapshot",
            "data": {"s": "SOLUSDT", "b": [["100", "1"]], "a": [["101", "1"]], "u": 2},
        })
        stream._handle_raw(delta)
        self.assertFalse(stream.available_symbols)
        stream._handle_raw(snapshot)
        self.assertEqual(stream.available_symbols, ("SOLUSDT",))
        stream._reset_session_state()
        stream._latest.clear()
        stream._handle_raw(delta)
        self.assertFalse(stream.available_symbols)


class _OneBookStream:
    symbols = ("BASEUSDT",)

    def __init__(self, book: BookSnapshot) -> None:
        self.book = book
        self.sent = False
        self.closed = asyncio.Event()

    async def start(self) -> None:
        return None

    async def next_update(self) -> BookSnapshot:
        if not self.sent:
            self.sent = True
            return self.book
        await self.closed.wait()
        raise asyncio.CancelledError

    async def close(self) -> None:
        self.closed.set()

    def nearest_snapshot(self, symbol: str, target_realtime_ns: int) -> BookSnapshot | None:
        return self.book


class _FakeLocalQuoteSource:
    name = "solana:local-worker"

    def __init__(self, route: LocalSpotRoute) -> None:
        self.route = route
        self.requests: list[dict[str, Any]] = []

    def describe(self) -> dict[str, object]:
        return {"source": self.name, "mode": "fake_local_worker"}

    async def run(self, publish: Any, stop_event: asyncio.Event) -> None:
        await publish(MarketEvent(
            source=self.name,
            key=self.route.pool_state_key,
            kind="pool_state",
            value={"compact": True},
            summary={
                "token_a_mint": self.route.base_mint,
                "token_b_mint": self.route.bridge_mint,
                "token_a_decimals": self.route.base_decimals,
                "token_b_decimals": self.route.bridge_decimals,
            },
            received_realtime_ns=time.time_ns(),
            received_monotonic_ns=time.monotonic_ns(),
            chain_position=42,
        ))
        await stop_event.wait()

    async def quote_exact_input(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs)
        output = 2_000_000 if kwargs["input_mint"] == self.route.bridge_mint else 12_000_000
        return {
            "status": "ok", "state_slot": 42, "output_amount_raw": str(output),
            "pool_fee_raw": "0", "price_impact_pct": "0", "all_trade": True,
        }


class ProductionWiringAcceptanceTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _route() -> LocalSpotRoute:
        return LocalSpotRoute(
            route_id="production-wiring",
            pool_id="pool",
            base_mint="base-mint",
            bridge_mint="bridge-mint",
            base_decimals=6,
            bridge_decimals=6,
            base_symbol="BASE",
            bridge_symbol="USDT",
            settlement_symbol="USDT",
            cex_venue="MEXC",
            base_cex_symbol="BASEUSDT",
            bridge_cex_symbol=None,
            bridge_is_settlement=True,
            notional_settlement=Decimal("10"),
            base_buy_taker_fee_bps=Decimal("1"),
            base_sell_taker_fee_bps=Decimal("1"),
            bridge_buy_taker_fee_bps=Decimal("0"),
            bridge_sell_taker_fee_bps=Decimal("0"),
            network_cost_floor_settlement=Decimal("0"),
            asset_equivalence="test fixture",
        )

    @staticmethod
    def _config(route: LocalSpotRoute, *, enabled: bool = True) -> SimpleNamespace:
        return SimpleNamespace(
            local_route_evaluator=LocalRouteEvaluatorSettings(
                enabled=enabled,
                routes=(route,) if enabled else (),
                minimum_quote_interval_ms=1,
            ),
            amm_simulation=AmmSimulationConfig(enabled=False),
            sequential_amm_pool=None,
            jupiter=JupiterConfig(enabled=False),
            timeout_seconds=1.0,
            rpc_http_url="https://example.invalid",
            rpc_ws_url="wss://example.invalid",
            proxy_url=None,
            retention_seconds=60,
            max_events_per_key=32,
            max_state_keys=128,
            event_bus_capacity=32,
            status_flush_seconds=0.01,
        )

    async def test_production_builder_uses_compact_store_and_epoch_depth_resolver(self) -> None:
        route = self._route()
        local = _FakeLocalQuoteSource(route)
        cex = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("BASEUSDT",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        book = _book("BASEUSDT")
        stream = _OneBookStream(book)
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                unified_market_data,
                "build_solana_market_sources",
                return_value=((local, cex), local),
            ),
            patch.object(unified_market_data, "build_perp_venue_sources", return_value=()),
            patch.object(unified_market_data, "_build_exact_quote_sources", return_value=()),
            patch.object(unified_market_data, "RaydiumLocalQuoteStateSource", _FakeLocalQuoteSource),
            patch.object(
                solana_realtime_scanner,
                "RaydiumLocalQuoteStateSource",
                _FakeLocalQuoteSource,
            ),
            patch.object(solana_realtime_scanner, "build_public_book_stream", return_value=stream),
        ):
            scanner = unified_market_data.build_unified_market_data_scanner(
                config=self._config(route),
                output_directory=Path(directory) / "run",
                hyperliquid_coins=(),
            )
            self.assertEqual(scanner.runtime_components, {
                "local_route_evaluator_requested": True,
                "local_route_evaluator_attached": True,
                "local_route_count": 1,
                "local_quote_worker_attached": True,
            })
            evaluator = scanner.status_providers["local_route_evaluator"].__self__
            self.assertEqual(
                sum(getattr(handler, "__self__", None) is evaluator for handler in scanner.shutdown_handlers),
                1,
            )
            run_task = asyncio.create_task(scanner.run())
            deadline = time.monotonic() + 1
            while not local.requests and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            self.assertTrue(local.requests, "route calculation did not use resolved full depth")
            stored = scanner.store.latest(route.base_book_key)
            self.assertIsInstance(stored.value, CexTopOfBookEvent)  # type: ignore[union-attr]
            self.assertNotIsInstance(stored.value, BookSnapshot)  # type: ignore[union-attr]
            self.assertIs(cex.latest_depth("MEXC", "BASEUSDT").book, book)  # type: ignore[union-attr]

            calls_before = len(local.requests)
            scanner.store.advance_source_epoch(cex.name, 2)
            await scanner.epoch_change_handlers[0](_epoch(cex.name, 2, reason="transport_reconnect"))
            self.assertIsNone(cex.latest_depth("MEXC", "BASEUSDT"))
            fresh_bbo_without_depth = replace(
                stored,
                event_id="epoch-2-bbo-without-depth",
                source_epoch=2,
                received_realtime_ns=time.time_ns(),
                received_monotonic_ns=time.monotonic_ns(),
            )
            self.assertTrue(scanner.store.add(fresh_bbo_without_depth))
            await scanner.event_handler(fresh_bbo_without_depth)
            await asyncio.sleep(0.02)
            self.assertEqual(len(local.requests), calls_before)

            scanner.stop_event.set()
            await asyncio.wait_for(run_task, timeout=1)
            self.assertTrue(evaluator._closed)
            self.assertTrue(all(runtime.task is None for runtime in evaluator._runtime.values()))
            manifest = scanner._manifest(status="completed")
            self.assertEqual(manifest["runtime_components"], scanner.runtime_components)

    async def test_enabled_routes_fail_fast_when_worker_cannot_build_evaluator(self) -> None:
        route = self._route()
        local = _FakeLocalQuoteSource(route)
        cex = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("BASEUSDT",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                unified_market_data,
                "build_solana_market_sources",
                return_value=((local, cex), local),
            ),
            patch.object(unified_market_data, "build_perp_venue_sources", return_value=()),
            patch.object(unified_market_data, "_build_exact_quote_sources", return_value=()),
        ):
            with self.assertRaisesRegex(ValueError, "managed local Raydium quote worker"):
                unified_market_data.build_unified_market_data_scanner(
                    config=self._config(route),
                    output_directory=Path(directory) / "run",
                    hyperliquid_coins=(),
                )

    async def test_disabled_configuration_does_not_construct_evaluator(self) -> None:
        route = self._route()
        local = _FakeLocalQuoteSource(route)
        cex = CexBookStateSource(
            config=CexStreamConfig(venue="MEXC", category="spot", symbols=("BASEUSDT",)),
            timeout_seconds=1,
            proxy_url=None,
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                unified_market_data,
                "build_solana_market_sources",
                return_value=((local, cex), local),
            ),
            patch.object(unified_market_data, "build_perp_venue_sources", return_value=()),
            patch.object(unified_market_data, "_build_exact_quote_sources", return_value=()),
            patch.object(solana_realtime_scanner, "SolanaRouteEvaluator") as evaluator_class,
        ):
            scanner = unified_market_data.build_unified_market_data_scanner(
                config=self._config(route, enabled=False),
                output_directory=Path(directory) / "run",
                hyperliquid_coins=(),
            )
        evaluator_class.assert_not_called()
        self.assertNotIn("local_route_evaluator", scanner.status_providers)
        self.assertEqual(scanner.runtime_components["local_route_evaluator_requested"], False)
        self.assertEqual(scanner.runtime_components["local_route_evaluator_attached"], False)
        self.assertEqual(scanner.runtime_components["local_route_count"], 0)


if __name__ == "__main__":
    unittest.main()
