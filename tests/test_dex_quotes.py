from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
import urllib.parse
from decimal import Decimal
from pathlib import Path

from market_data_lab.dex_quotes import Asset
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import DexQuoteProvider
from market_data_lab.dex_quotes import EvmMarket
from market_data_lab.dex_quotes import JupiterProvider
from market_data_lab.dex_quotes import OMNISTON_QUOTE_METHOD
from market_data_lab.dex_quotes import OmnistonProvider
from market_data_lab.dex_quotes import RaydiumProvider
from market_data_lab.dex_quotes import StonFiProvider
from market_data_lab.dex_quotes import UniswapV3Provider
from market_data_lab.dex_quotes import _redact_url
from market_data_lab.dex_quotes import encode_quoter_v2_exact_input_single
from market_data_lab.dex_quotes import encode_quoter_v2_exact_output_single
from market_data_lab.dex_quotes import parse_quoter_v2_result
from market_data_lab.dex_quotes import record_dex_quotes


def _quoter_result(*values: int) -> str:
    return "0x" + "".join(f"{value:064x}" for value in values)


class DexQuoteEncodingTest(unittest.TestCase):
    def test_provider_protocol_declares_config_capability(self) -> None:
        self.assertIn("config", DexQuoteProvider.__dict__)

    def test_quoter_v2_static_tuple_encoding(self) -> None:
        value = encode_quoter_v2_exact_input_single(
            "0x4200000000000000000000000000000000000006",
            "0x833589fCD6eDb6E08f4C7C32D4f71b54bdA02913",
            10**18,
            500,
        )

        self.assertTrue(value.startswith("0xc6a5026a"))
        self.assertEqual(len(value), 2 + 8 + 5 * 64)
        words = [value[10 + index * 64 : 10 + (index + 1) * 64] for index in range(5)]
        self.assertEqual(words[0][-40:], "4200000000000000000000000000000000000006")
        self.assertEqual(words[1][-40:], "833589fcd6edb6e08f4c7c32d4f71b54bda02913")
        self.assertEqual(int(words[2], 16), 10**18)
        self.assertEqual(int(words[3], 16), 500)
        self.assertEqual(int(words[4], 16), 0)

    def test_quoter_v2_exact_output_uses_v2_tuple_selector(self) -> None:
        value = encode_quoter_v2_exact_output_single(
            "0x4200000000000000000000000000000000000006",
            "0x833589fCD6eDb6E08f4C7C32D4f71b54bdA02913",
            10**18,
            500,
        )

        self.assertTrue(value.startswith("0xbd21704a"))
        self.assertEqual(len(value), 2 + 8 + 5 * 64)
        self.assertEqual(int(value[10 + 2 * 64 : 10 + 3 * 64], 16), 10**18)

    def test_quoter_v2_result_decoding(self) -> None:
        self.assertEqual(
            parse_quoter_v2_result(_quoter_result(11, 22, 33, 44)),
            {
                "amount_out": 11,
                "sqrt_price_x96_after": 22,
                "initialized_ticks_crossed": 33,
                "gas_estimate": 44,
            },
        )

    def test_rpc_url_redaction_removes_path_query_and_credentials(self) -> None:
        self.assertEqual(
            _redact_url("https://user:secret@example.test:8443/v2/private-key?token=hidden"),
            "https://example.test:8443",
        )


class ProviderNormalizationTest(unittest.TestCase):
    def test_uniswap_paces_each_buy_and_sell_rpc_batch(self) -> None:
        starts: list[int] = []

        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            starts.append(time.monotonic_ns())
            calls = json.loads(body or b"[]")
            response: list[dict[str, object]] = [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0x64", "timestamp": "0xc8", "hash": "0xabc"},
                },
            ]
            for call in calls:
                if call["method"] == "eth_call":
                    response.append(
                        {
                            "jsonrpc": "2.0",
                            "id": call["id"],
                            "result": _quoter_result(10**18, 2, 3, 4),
                        },
                    )
            return response

        market = EvmMarket(
            provider="TEST_UNISWAP",
            chain="test",
            chain_id=1,
            protocol="Uniswap v3",
            rpc_url="https://rpc.example.test/key",
            block_tag="latest",
            quoter_address="0x1111111111111111111111111111111111111111",
            base=Asset("WETH", "0x2222222222222222222222222222222222222222", 18),
            quote=Asset("USDC", "0x3333333333333333333333333333333333333333", 6),
            fee_tiers=(500,),
        )
        provider = UniswapV3Provider(
            market,
            proxy_url=None,
            timeout_seconds=1,
            request_pacer=AsyncRequestPacer(0.02),
            fetch_json=fetch,
        )

        asyncio.run(provider.quote_round(7, [Decimal("100")]))

        self.assertEqual(len(starts), 2)
        self.assertGreaterEqual(starts[1] - starts[0], 15_000_000)

    def test_uniswap_records_both_sides_from_batch_eth_call(self) -> None:
        invocations = 0

        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            nonlocal invocations
            invocations += 1
            calls = json.loads(body or b"[]")
            response: list[dict[str, object]] = [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0x64", "timestamp": "0xc8", "hash": "0xabc"},
                },
            ]
            for call in calls:
                if call["method"] == "eth_call":
                    amount_out = 10**18 if invocations == 1 else 99_000_000
                    response.append(
                        {
                            "jsonrpc": "2.0",
                            "id": call["id"],
                            "result": _quoter_result(amount_out, 2, 3, 4),
                        },
                    )
            return response

        market = EvmMarket(
            provider="TEST_UNISWAP",
            chain="test",
            chain_id=1,
            protocol="Uniswap v3",
            rpc_url="https://rpc.example.test/key",
            block_tag="latest",
            quoter_address="0x1111111111111111111111111111111111111111",
            base=Asset("WETH", "0x2222222222222222222222222222222222222222", 18),
            quote=Asset("USDC", "0x3333333333333333333333333333333333333333", 6),
            fee_tiers=(500,),
        )
        provider = UniswapV3Provider(
            market,
            proxy_url=None,
            timeout_seconds=1,
            fetch_json=fetch,
        )

        records = asyncio.run(provider.quote_round(7, [Decimal("100")]))

        self.assertEqual(invocations, 2)
        self.assertEqual([record["direction"] for record in records], ["buy_base", "sell_base"])
        self.assertEqual([record["status"] for record in records], ["ok", "ok"])
        self.assertEqual(records[0]["average_price_quote_per_base"], "100")
        self.assertEqual(records[1]["average_price_quote_per_base"], "99")
        self.assertEqual(records[0]["chain_context"]["block_number"], 100)

    def test_uniswap_exact_base_records_are_aligned_for_a_perp_hedge(self) -> None:
        invocations = 0

        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            nonlocal invocations
            invocations += 1
            calls = json.loads(body or b"[]")
            response: list[dict[str, object]] = [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0x64", "timestamp": "0xc8", "hash": "0xabc"},
                },
            ]
            for call in calls:
                if call["method"] != "eth_call":
                    continue
                data = call["params"][0]["data"]
                # Exact output (USDC -> WETH) returns input USDC; the reverse
                # exact input (WETH -> USDC) returns output USDC.
                amount = 100_000_000 if data.startswith("0xbd21704a") else 99_000_000
                response.append({"jsonrpc": "2.0", "id": call["id"], "result": _quoter_result(amount, 2, 3, 4)})
            return response

        market = EvmMarket(
            provider="TEST_UNISWAP",
            chain="test",
            chain_id=1,
            protocol="Uniswap v3",
            rpc_url="https://rpc.example.test/key",
            block_tag="latest",
            quoter_address="0x1111111111111111111111111111111111111111",
            base=Asset("WETH", "0x2222222222222222222222222222222222222222", 18),
            quote=Asset("USDC", "0x3333333333333333333333333333333333333333", 6),
            fee_tiers=(500,),
        )
        provider = UniswapV3Provider(market, proxy_url=None, timeout_seconds=1, fetch_json=fetch)

        records = asyncio.run(
            provider.quote_exact_base_round(7, [(Decimal("100"), Decimal("1"))]),
        )

        self.assertEqual(invocations, 2)
        self.assertEqual([record["direction"] for record in records], ["buy_base", "sell_base"])
        self.assertEqual([record["base_amount"] for record in records], ["1", "1"])
        self.assertTrue(all(record["hedge_quantity_exact"] for record in records))
        self.assertEqual(records[0]["quote_semantics"], "exact_output_base_aligned_to_perp_step")
        self.assertEqual(records[0]["quote_amount"], "100")
        self.assertEqual(records[1]["quote_amount"], "99")

    def test_raydium_route_quote_is_normalized(self) -> None:
        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            buying = query["inputMint"][0] == RaydiumProvider.quote.address
            return {
                "id": "quote-id",
                "success": True,
                "version": "V1",
                "data": {
                    "outputAmount": "1000000000" if buying else "99000000",
                    "otherAmountThreshold": "1",
                    "priceImpactPct": 0.001,
                    "routePlan": [{"poolId": "pool"}],
                },
            }

        provider = RaydiumProvider(
            proxy_url=None,
            timeout_seconds=1,
            fetch_json=fetch,
        )

        records = asyncio.run(provider.quote_round(0, [Decimal("100")]))

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["average_price_quote_per_base"], "100")
        self.assertEqual(records[1]["average_price_quote_per_base"], "99")
        self.assertEqual(records[0]["quote_service_metadata"]["route_plan"][0]["poolId"], "pool")

    def test_jupiter_quote_only_order_is_normalized_without_taker(self) -> None:
        requested_urls: list[str] = []

        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            requested_urls.append(url)
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            buying = query["inputMint"][0] == provider.quote.address
            self.assertEqual(method, "GET")
            self.assertIsNone(body)
            self.assertEqual(headers, {})
            self.assertNotIn("taker", query)
            return {
                "requestId": "request-id",
                "inAmount": query["amount"][0],
                "outAmount": "1000000000" if buying else "99000000",
                "router": "metis",
                "feeBps": 2,
                "platformFee": {"amount": "20000", "feeBps": 2},
                "routePlan": [{"swapInfo": {"label": "TestPool"}}],
                "transaction": None,
                "taker": None,
            }

        provider = JupiterProvider(
            proxy_url=None,
            timeout_seconds=1,
            request_pacer=AsyncRequestPacer(0),
            fetch_json=fetch,
        )

        records = asyncio.run(provider.quote_round(0, [Decimal("100")]))

        self.assertEqual(len(requested_urls), 2)
        self.assertEqual([record["status"] for record in records], ["ok", "ok"])
        self.assertEqual(records[0]["average_price_quote_per_base"], "100")
        self.assertEqual(records[1]["average_price_quote_per_base"], "99")
        self.assertEqual(records[0]["quote_service_metadata"]["router"], "metis")
        self.assertTrue(records[0]["quote_includes_aggregator_platform_fee"])
        self.assertFalse(provider.config()["wallet_or_taker_supplied"])

    def test_stonfi_quote_is_normalized(self) -> None:
        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            buying = query["offer_address"][0] == StonFiProvider.quote.address
            return {
                "ask_units": "1000000000" if buying else "99000000",
                "pool_address": "pool",
                "router_address": "router",
                "router": {"major_version": 2, "minor_version": 1},
                "min_ask_units": "1",
                "recommended_min_ask_units": "1",
                "swap_rate": "1",
                "price_impact": "0.001",
                "fee_units": "1",
                "fee_percent": "0.003",
                "gas_params": {"forward_gas": "1"},
            }

        provider = StonFiProvider(
            proxy_url=None,
            timeout_seconds=1,
            fetch_json=fetch,
        )

        records = asyncio.run(provider.quote_round(0, [Decimal("100")]))

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["average_price_quote_per_base"], "100")
        self.assertEqual(records[1]["average_price_quote_per_base"], "99")
        self.assertEqual(records[0]["quote_service_metadata"]["router_major_version"], 2)

    def test_omniston_v1beta8_stream_quote_is_normalized_without_wallet(self) -> None:
        requests: list[dict[str, object]] = []

        class FakeWebSocket:
            def __init__(self) -> None:
                self.messages: list[str] = []

            async def __aenter__(self) -> FakeWebSocket:
                return self

            async def __aexit__(self, *args: object) -> None:
                return None

            async def send(self, raw: str) -> None:
                request = json.loads(raw)
                if request.get("method") != OMNISTON_QUOTE_METHOD:
                    return
                requests.append(request)
                params = request["params"]
                buying = "jetton" in params["input_asset"]["ton"]
                output_units = "1000000000" if buying else "99000000"
                subscription = len(requests)
                self.messages = [
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": request["id"],
                            "result": subscription,
                        },
                    ),
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "method": OMNISTON_QUOTE_METHOD,
                            "params": {
                                "subscription": subscription,
                                "result": {"ack": {"rfq_id": f"rfq-{subscription}"}},
                            },
                        },
                    ),
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "method": OMNISTON_QUOTE_METHOD,
                            "params": {
                                "subscription": subscription,
                                "result": {
                                    "quote_updated": {
                                        "rfq_id": f"rfq-{subscription}",
                                        "quote_id": f"quote-{subscription}",
                                        "resolver_id": "resolver-id",
                                        "resolver_name": "resolver",
                                        "input_units": params["input_units"],
                                        "output_units": output_units,
                                        "integrator_fee_units": "0",
                                        "protocol_fee_units": "1",
                                        "quote_timestamp": 1,
                                        "gas_budget": "70000000",
                                        "swap": {"routes": [{"chunks": []}]},
                                    },
                                },
                            },
                        },
                    ),
                ]

            async def recv(self) -> str:
                return self.messages.pop(0)

        def connect(*args: object, **kwargs: object) -> FakeWebSocket:
            self.assertIsNone(kwargs["proxy"])
            return FakeWebSocket()

        provider = OmnistonProvider(
            proxy_url=None,
            timeout_seconds=1,
            quote_selection_window_seconds=0,
            connect_websocket=connect,
        )

        records = asyncio.run(provider.quote_round(0, [Decimal("100")]))

        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0]["method"], OMNISTON_QUOTE_METHOD)
        self.assertNotIn("wallet_address", json.dumps(requests[0]))
        self.assertEqual([record["status"] for record in records], ["ok", "ok"])
        self.assertEqual(records[0]["average_price_quote_per_base"], "100")
        self.assertEqual(records[1]["average_price_quote_per_base"], "99")
        self.assertEqual(records[0]["quote_service_metadata"]["resolver_name"], "resolver")
        self.assertEqual(records[0]["quote_service_metadata"]["api_version"], "v1beta8")

    def test_stonfi_custom_asset_uses_configured_pair_and_addresses(self) -> None:
        base = Asset("NOT", "EQ_NOT", 9)
        quote = Asset("USDT", "EQ_USDT", 6)
        requested_pairs: list[tuple[str, str]] = []

        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            requested_pairs.append((query["offer_address"][0], query["ask_address"][0]))
            buying = query["offer_address"][0] == quote.address
            return {"ask_units": "1000000000" if buying else "99000000"}

        provider = StonFiProvider(
            name="STONFI_NOT",
            base=base,
            quote=quote,
            proxy_url=None,
            timeout_seconds=1,
            fetch_json=fetch,
        )

        records = asyncio.run(provider.quote_round(0, [Decimal("100")]))

        self.assertEqual(provider.config()["pair"], "NOT/USDT")
        self.assertEqual(records[0]["provider"], "STONFI_NOT")
        self.assertEqual(records[0]["pair"], "NOT/USDT")
        self.assertEqual(requested_pairs, [("EQ_USDT", "EQ_NOT"), ("EQ_NOT", "EQ_USDT")])


class RecorderTest(unittest.TestCase):
    def test_recorder_writes_jsonl_and_manifest_without_credentials(self) -> None:
        class FakeProvider:
            name = "FAKE"

            async def quote_round(
                self,
                round_id: int,
                notionals: list[Decimal],
            ) -> list[dict[str, object]]:
                return [
                    {
                        "schema_version": 1,
                        "round_id": round_id,
                        "provider": self.name,
                        "status": "ok",
                        "request_rtt_ms": 1.25,
                        "response_received_realtime_ns": 10,
                    },
                ]

            def config(self) -> dict[str, object]:
                return {"provider": self.name}

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            manifest = asyncio.run(
                record_dex_quotes(
                    [FakeProvider()],
                    notionals=[Decimal("100")],
                    duration_seconds=0.001,
                    interval_seconds=1,
                    output_directory=output,
                    proxy_url=None,
                ),
            )

            rows = [json.loads(line) for line in (output / "quotes.jsonl").read_text().splitlines()]
            persisted = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "ok")
            self.assertEqual(rows[0]["provider"], "FAKE")
            self.assertFalse(persisted["api_credentials_used"])
            self.assertFalse(persisted["wallet_or_private_key_used"])
            self.assertFalse(persisted["transactions_submitted"])


if __name__ == "__main__":
    unittest.main()
