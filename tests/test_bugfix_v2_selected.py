from __future__ import annotations

import asyncio
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from market_data_lab.numeric_text import canonical_decimal_text
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.polling_quote_sources import PollingDexQuoteSource
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RollingStateStore
from market_data_lab.solana_realtime_scanner import CexBookStateSource
from market_data_lab.solana_realtime_scanner import CexStreamConfig
from market_data_lab.triangle_cycle_monitor import TRIANGLE_MARKETS
from market_data_lab.unified_cycle_analyzer import UnifiedCycleAnalyzer


class _FixtureProvider:
    name = "FIXTURE"


def _event(key: str, value: object, *, received: int, epoch: int = 0) -> MarketEvent:
    return MarketEvent(
        source="fixture",
        key=key,
        kind="quote",
        value=value,
        summary={},
        received_realtime_ns=received,
        received_monotonic_ns=received,
        event_id=f"{key}:{epoch}:{received}",
        source_epoch=epoch,
        boot_id="boot-v2",
    )


def _quote(*, amount: Decimal, slot: str, received: int, direction: str = "buy_base") -> ExactInputQuote:
    return ExactInputQuote(
        provider=TRIANGLE_MARKETS[0].provider,
        chain="solana",
        protocol="fixture",
        source_kind="fixture",
        pair="SOL/USDC",
        direction=direction,
        round_id=received,
        requested_notional_quote=amount,
        reference_notional_usdt=Decimal("100"),
        base_amount=Decimal("1"),
        quote_amount=amount,
        input_symbol="USDC",
        output_symbol="SOL",
        input_amount_raw=100,
        output_amount_raw=100,
        average_price_quote_per_base=amount,
        fee_bps=Decimal("1"),
        request_rtt_ms=1,
        status="ok",
        error=None,
        response_received_realtime_ns=received,
        response_received_monotonic_ns=received,
        block_number=None,
        quote_slot_id=slot,
    )


class DecimalAndSlotBugfixTest(unittest.IsolatedAsyncioTestCase):
    def test_bug012_decimal_canonicalization_is_finite_and_fixed_point(self) -> None:
        examples = {
            "1.00": "1",
            "0.5000": "0.5",
            "100": "100",
            "1E-8": "0.00000001",
            "-0.000": "0",
        }
        for raw, expected in examples.items():
            self.assertEqual(canonical_decimal_text(Decimal(raw)), expected)
        for raw in ("NaN", "Infinity", "-Infinity"):
            with self.assertRaises(ValueError):
                canonical_decimal_text(Decimal(raw))

    async def test_bug001_dynamic_slot_stays_one_key_for_10000_updates(self) -> None:
        source = PollingDexQuoteSource(
            provider=_FixtureProvider(),  # type: ignore[arg-type]
            notionals=(Decimal("100"),),
            notional_supplier=lambda: (),
        )
        mapped = source._with_reference_notional(  # noqa: SLF001 - canonicalization regression
            {"requested_notional_quote": "0.500"},
            reference_by_notional={"0.5": Decimal("100.00")},
        )
        self.assertEqual(mapped["quote_slot_id"], "triangle-reference-usdt:100")
        events: list[MarketEvent] = []

        async def publish(event: MarketEvent) -> None:
            events.append(event)

        for index in range(10_000):
            amount = Decimal(index + 1) / Decimal("1000")
            await source._publish_record(  # noqa: SLF001 - regression fixture
                publish,
                {
                    "provider": "FIXTURE",
                    "direction": "buy_base",
                    "status": "ok",
                    "requested_notional_quote": format(amount, "f"),
                    "reference_notional_usdt": "100.00",
                    "base_amount": "1",
                    "quote_amount": format(amount, "f"),
                    "average_price_quote_per_base": format(amount, "f"),
                    "response_received_realtime_ns": index + 1,
                    "response_received_monotonic_ns": index + 1,
                },
            )

        self.assertEqual({event.key for event in events}, {"dexquote:FIXTURE:triangle-reference-usdt:100:buy_base"})
        self.assertEqual(events[-1].value.requested_notional_quote, Decimal("10"))

        await source._publish_record(  # noqa: SLF001 - regression fixture
            publish,
            {
                "provider": "FIXTURE",
                "direction": "sell_base",
                "status": "ok",
                "requested_notional_quote": "100.00",
                "reference_notional_usdt": "100.00",
                "base_amount": "1",
                "quote_amount": "100",
                "average_price_quote_per_base": "100",
                "response_received_realtime_ns": 20_001,
                "response_received_monotonic_ns": 20_001,
            },
        )
        await source._publish_record(  # noqa: SLF001 - regression fixture
            publish,
            {
                "provider": "FIXTURE",
                "direction": "buy_base",
                "status": "ok",
                "requested_notional_quote": "100.00",
                "base_amount": "1",
                "quote_amount": "100",
                "average_price_quote_per_base": "100",
                "response_received_realtime_ns": 20_002,
                "response_received_monotonic_ns": 20_002,
            },
        )
        self.assertEqual(
            {event.key for event in events[-2:]},
            {
                "dexquote:FIXTURE:triangle-reference-usdt:100:sell_base",
                "dexquote:FIXTURE:notional:100:buy_base",
            },
        )
        for index, notional in enumerate(("100", "1000"), start=30_000):
            await source._publish_record(  # noqa: SLF001 - regression fixture
                publish,
                {
                    "provider": "FIXTURE",
                    "direction": "buy_base",
                    "status": "ok",
                    "requested_notional_quote": notional,
                    "base_amount": "1",
                    "quote_amount": notional,
                    "average_price_quote_per_base": notional,
                    "response_received_realtime_ns": index,
                    "response_received_monotonic_ns": index,
                },
            )
        self.assertIn("dexquote:FIXTURE:notional:100:buy_base", {event.key for event in events})
        self.assertIn("dexquote:FIXTURE:notional:1000:buy_base", {event.key for event in events})

    async def test_bug001_unmapped_provider_records_use_one_diagnostic_key(self) -> None:
        source = PollingDexQuoteSource(
            provider=_FixtureProvider(),  # type: ignore[arg-type]
            notionals=(Decimal("100"),),
        )
        source._last_round_inputs = source._round_inputs()  # noqa: SLF001
        events: list[MarketEvent] = []

        async def publish(event: MarketEvent) -> None:
            events.append(event)

        for index in range(100):
            await source._publish_record(  # noqa: SLF001 - regression fixture
                publish,
                {
                    "provider": "FIXTURE",
                    "direction": f"unexpected-{index}",
                    "quote_slot_id": f"provider-generated-{index}",
                    "status": "request_error",
                    "error": "unmapped response",
                    "response_received_realtime_ns": index + 1,
                    "response_received_monotonic_ns": index + 1,
                },
            )

        self.assertEqual({event.key for event in events}, {"dexquote:FIXTURE:unmapped:unknown"})

    async def test_bug001_analyzer_replaces_dynamic_slot_and_keeps_directions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = CexBookStateSource(
                config=CexStreamConfig(venue="MEXC", category="spot", symbols=()),
                timeout_seconds=1,
                proxy_url=None,
            )
            analyzer = UnifiedCycleAnalyzer(
                output_directory=Path(directory),
                cex_sources=(source,),
                coalesce_interval_ms=1,
            )
            for index in range(10_000):
                quote = _quote(
                    amount=Decimal(index + 1) / Decimal("1000"),
                    slot="triangle-reference-usdt:100",
                    received=index + 1,
                )
                await analyzer.handle_event(
                    MarketEvent(
                        source="fixture",
                        key=f"quote:{index}",
                        kind="exact_input_quote",
                        value=quote,
                        summary={},
                        received_realtime_ns=index + 1,
                        received_monotonic_ns=index + 1,
                    ),
                )
            self.assertEqual(len(analyzer._triangle_quotes), 1)  # noqa: SLF001
            self.assertEqual(
                analyzer._triangle_quotes[(TRIANGLE_MARKETS[0].provider, "triangle-reference-usdt:100", "buy_base")].requested_notional_quote,  # noqa: SLF001
                Decimal("10"),
            )
            await analyzer.handle_event(
                MarketEvent(
                    source="fixture",
                    key="quote:sell",
                    kind="exact_input_quote",
                    value=_quote(
                        amount=Decimal("10"),
                        slot="triangle-reference-usdt:100",
                        received=20_000,
                        direction="sell_base",
                    ),
                    summary={},
                    received_realtime_ns=20_000,
                    received_monotonic_ns=20_000,
                ),
            )
            self.assertEqual(len(analyzer._triangle_quotes), 2)  # noqa: SLF001
            await analyzer.close()

    def test_bug005_idle_and_capacity_retirement_are_physical(self) -> None:
        store = RollingStateStore(
            retention_seconds=1,
            max_events_per_key=4,
            max_state_keys=2,
            boot_id="boot-v2",
        )
        self.assertTrue(store.add(_event("a", "a", received=1_000_000_000)))
        self.assertTrue(store.add(_event("b", "b", received=1_000_000_001)))
        self.assertTrue(store.add(_event("c", "c", received=1_000_000_002)))
        self.assertIsNone(store.latest("a"))
        self.assertEqual(store.snapshot(now_monotonic_ns=1_000_000_002)["keys"], 2)
        self.assertEqual(store.snapshot(now_monotonic_ns=1_000_000_002)["capacity_evictions"], 1)
        self.assertEqual(store._versioned.snapshot()["states"], 2)  # noqa: SLF001

        self.assertEqual(store.sweep(now_monotonic_ns=3_000_000_000), ("b", "c"))
        self.assertEqual(store.snapshot(now_monotonic_ns=3_000_000_000)["keys"], 0)
        self.assertEqual(store.recent("b"), ())
        self.assertEqual(store._versioned.snapshot()["states"], 0)  # noqa: SLF001

    def test_bug005_epoch_transition_clears_latest_history_and_indexes(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=4,
            boot_id="boot-v2",
        )
        for key in ("a", "b"):
            self.assertTrue(store.add(_event(key, key, received=1_000, epoch=1)))
        invalidated = store.advance_source_epoch("fixture", 2)
        self.assertEqual(set(invalidated), {"a", "b"})
        self.assertEqual(store.snapshot(now_monotonic_ns=2_000)["keys"], 0)
        self.assertEqual(store.recent("a"), ())
        self.assertEqual(store._versioned.snapshot()["states"], 0)  # noqa: SLF001


if __name__ == "__main__":
    unittest.main()
