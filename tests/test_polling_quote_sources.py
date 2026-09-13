from __future__ import annotations

import asyncio
import time
import unittest
from dataclasses import replace
from decimal import Decimal

from market_data_lab.dex_quotes import Asset
from market_data_lab.polling_quote_sources import PollingDexQuoteSource
from market_data_lab.polling_quote_sources import QuoteRoundInput
from market_data_lab.quote_broker import QuoteBroker
from market_data_lab.quote_broker import QuoteRequest
from market_data_lab.quote_broker import SharedQuoteBudgetManager


class _DynamicNotionalProvider:
    name = "TEST_CROSS"

    def __init__(self) -> None:
        self.calls: list[tuple[Decimal, ...]] = []

    async def quote_round(self, round_id: int, notionals: tuple[Decimal, ...]) -> list[dict[str, object]]:
        self.calls.append(tuple(notionals))
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        notional = format(notionals[0], "f")
        return [
            {
                "provider": self.name,
                "round_id": round_id,
                "status": "ok",
                "direction": "buy_base",
                "requested_notional_quote": notional,
                "base_amount": "1",
                "quote_amount": notional,
                "average_price_quote_per_base": notional,
                "response_received_realtime_ns": now_realtime_ns,
                "response_received_monotonic_ns": now_monotonic_ns,
            },
        ]


class _RateLimitedProvider:
    name = "TEST_RATE_LIMIT"

    async def quote_round(self, round_id: int, notionals: tuple[Decimal, ...]) -> list[dict[str, object]]:
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        return [
            {
                "provider": self.name,
                "round_id": round_id,
                "status": "quote_unavailable",
                "error": "JSON-RPC error -32016: rate limit exceeded",
                "response_received_realtime_ns": now_realtime_ns,
                "response_received_monotonic_ns": now_monotonic_ns,
            },
        ]


class _BrokerProvider:
    name = "TEST_BROKER"
    base = Asset("BASE", "baseMint111111111111111111111111111111111", 9)
    quote = Asset("USDC", "quoteMint11111111111111111111111111111111", 6)

    def config(self) -> dict[str, object]:
        return {
            "provider": self.name,
            "chain": "solana",
            "protocol": "test-v1",
            "source_kind": "fixture",
            "endpoint_origin": "https://example.invalid",
            "slippage_bps": 50,
        }

    async def quote_round(
        self,
        round_id: int,
        notionals: tuple[Decimal, ...],
    ) -> list[dict[str, object]]:
        raise AssertionError("normalization fixture does not perform network calls")


class PollingQuoteSourceTest(unittest.IsolatedAsyncioTestCase):
    async def test_dynamic_input_keeps_common_usdt_reference_out_of_vendor_call(self) -> None:
        provider = _DynamicNotionalProvider()
        source = PollingDexQuoteSource(
            provider=provider,  # type: ignore[arg-type] - narrow public-provider fixture
            notionals=(Decimal("100"),),
            notional_supplier=lambda: (
                QuoteRoundInput(
                    amount=Decimal("0.5"),
                    reference_notional_usdt=Decimal("100"),
                ),
            ),
        )
        events = []
        stop_event = asyncio.Event()

        async def publish(event: object) -> None:
            events.append(event)
            stop_event.set()

        await source.run(publish, stop_event)

        self.assertEqual(provider.calls, [(Decimal("0.5"),)])
        quote = events[0].value  # type: ignore[attr-defined]
        self.assertEqual(quote.requested_notional_quote, Decimal("0.5"))
        self.assertEqual(quote.reference_notional_usdt, Decimal("100"))
        self.assertEqual(source.status()["last_round_inputs"][0]["amount"], "0.5")

    async def test_rate_limit_breaker_honours_long_configured_cooldown(self) -> None:
        source = PollingDexQuoteSource(
            provider=_RateLimitedProvider(),  # type: ignore[arg-type] - narrow public-provider fixture
            notionals=(Decimal("100"),),
            rate_limit_circuit_breaker_events=1,
            rate_limit_circuit_breaker_seconds=24.0 * 60.0 * 60.0,
        )
        stop_event = asyncio.Event()

        async def publish(event: object) -> None:
            stop_event.set()

        await source.run(publish, stop_event)

        status = source.status()
        self.assertTrue(status["rate_limit_circuit_breaker_active"])
        self.assertGreater(status["rate_limit_circuit_breaker_seconds_remaining"], 86_390.0)

    async def test_t24_polling_observation_seeds_shared_broker_without_remote_call(self) -> None:
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        source = PollingDexQuoteSource(
            provider=_BrokerProvider(),  # type: ignore[arg-type] - narrow provider fixture
            notionals=(Decimal("100"),),
            quota_domain="vendor:test-broker",
        )
        quote = source._normalise(  # noqa: SLF001 - adapter normalization fixture
            {
                "provider": "TEST_BROKER",
                "chain": "solana",
                "protocol": "test-v1",
                "source_kind": "fixture",
                "pair": "BASE/USDC",
                "direction": "buy_base",
                "round_id": 7,
                "requested_notional_quote": "100",
                "base_amount": "2",
                "quote_amount": "100",
                "input_symbol": "USDC",
                "output_symbol": "BASE",
                "input_amount_raw": "100000000",
                "output_amount_raw": "2000000000",
                "average_price_quote_per_base": "50",
                "status": "ok",
                "response_received_realtime_ns": now_realtime_ns,
                "response_received_monotonic_ns": now_monotonic_ns,
            },
        )
        result = source.broker_result(replace(quote, source_epoch=3))
        self.assertEqual(
            quote.input_asset_id,
            "solana:mainnet:quoteMint11111111111111111111111111111111:6",
        )
        self.assertEqual(
            quote.output_asset_id,
            "solana:mainnet:baseMint111111111111111111111111111111111:9",
        )
        self.assertIsNotNone(result)
        assert result is not None
        broker = QuoteBroker(
            backends={},
            budgets=SharedQuoteBudgetManager({}),
        )
        self.assertTrue(broker.observe_result(result))

        delivered = await broker.get_quote(
            QuoteRequest(
                request_id="consumer",
                reason="candidate_verification",
                priority="candidate",
                deadline_monotonic_ns=time.monotonic_ns() + 1_000_000_000,
                key=result.key,
            ),
        )

        self.assertEqual(delivered.status, "ok")
        self.assertEqual(delivered.served_from, "cache")
        self.assertEqual(delivered.actual_output_raw, 2_000_000_000)
        self.assertIn("source_epoch:3", delivered.key.required_state_fingerprint)
        self.assertNotIn("remote_requests_started", broker.snapshot()["counts"])


if __name__ == "__main__":
    unittest.main()
