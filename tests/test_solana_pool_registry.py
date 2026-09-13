from __future__ import annotations

import asyncio
import unittest
from decimal import Decimal

from market_data_lab.dex_quotes import SOLANA_USDC
from market_data_lab.dex_quotes import SOL_WRAPPED_MINT
from market_data_lab.solana_pool_registry import PoolSelectionPolicy
from market_data_lab.solana_pool_registry import discover_raydium_pools
from market_data_lab.solana_pool_registry import parse_meteora_dlmm_pools
from market_data_lab.solana_pool_registry import parse_orca_whirlpool_pools
from market_data_lab.solana_pool_registry import parse_raydium_pools
from market_data_lab.solana_pool_registry import refresh_registry
from market_data_lab.solana_pool_registry import select_pools


def _raydium_payload() -> dict[str, object]:
    return {
        "success": True,
        "data": {
            "data": [
                {
                    "id": "ray-clmm-sol-usdc",
                    "type": "Concentrated",
                    "mint1": {
                        "address": SOL_WRAPPED_MINT,
                        "symbol": "SOL",
                        "decimals": 9,
                        "isVerified": True,
                    },
                    "mint2": {
                        "address": SOLANA_USDC.address,
                        "symbol": "USDC",
                        "decimals": 6,
                        "isVerified": True,
                    },
                    "tvl": "5000000",
                    "day": {"volume": "12500000"},
                    "config": {"tradeFeeRate": 2500, "tickSpacing": 60},
                    "tags": [],
                },
                {
                    "id": "ray-scam",
                    "type": "Concentrated",
                    "mintA": {"address": SOL_WRAPPED_MINT, "isVerified": True},
                    "mintB": {"address": SOLANA_USDC.address, "isVerified": True},
                    "tvl": "99999999",
                    "day": {"volume": "99999999"},
                    "tags": ["scam"],
                },
            ],
        },
    }


def _meteora_payload() -> dict[str, object]:
    return {
        "current_page": 1,
        "data": [
            {
                "address": "meteor-sol-usdc",
                "name": "SOL-USDC",
                "tvl": 4_000_000,
                "volume": {"24h": 9_000_000},
                "token_x": {
                    "address": SOL_WRAPPED_MINT,
                    "symbol": "SOL",
                    "decimals": 9,
                    "is_verified": True,
                },
                "token_y": {
                    "address": SOLANA_USDC.address,
                    "symbol": "USDC",
                    "decimals": 6,
                    "is_verified": True,
                },
                "is_blacklisted": False,
                "pool_config": {"base_fee_pct": 0.04, "bin_step": 4},
            },
            {
                "address": "meteor-unverified",
                "tvl": 4_000_000,
                "volume": {"24h": 9_000_000},
                "token_x": {"address": SOL_WRAPPED_MINT, "is_verified": True},
                "token_y": {"address": SOLANA_USDC.address, "is_verified": False},
                "is_blacklisted": False,
            },
        ],
    }


def _orca_payload() -> dict[str, object]:
    return {
        "data": [
            {
                "address": "orca-sol-usdc",
                "poolType": "whirlpool",
                "tickSpacing": 64,
                "feeRate": 2000,
                "protocolFeeRate": 300,
                "adaptiveFeeEnabled": False,
                "tvlUsdc": "3000000",
                "tokenA": {
                    "address": SOL_WRAPPED_MINT,
                    "symbol": "SOL",
                    "decimals": 9,
                },
                "tokenB": {
                    "address": SOLANA_USDC.address,
                    "symbol": "USDC",
                    "decimals": 6,
                },
                "stats": {"24h": {"volume": "7000000"}},
                "hasWarning": False,
            },
        ],
        "meta": {"next": None},
    }


class SolanaPoolRegistryParsingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = PoolSelectionPolicy(
            allowed_mints=frozenset({SOL_WRAPPED_MINT, SOLANA_USDC.address}),
            minimum_tvl_usd=Decimal("100000"),
            minimum_volume_24h_usd=Decimal("25000"),
            max_pools_per_protocol=3,
        )

    def test_raydium_payload_is_normalized_and_blacklist_is_filtered(self) -> None:
        parsed = parse_raydium_pools(_raydium_payload())

        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].protocol, "raydium_clmm")
        self.assertEqual(parsed[0].volume_24h_usd, Decimal("12500000"))
        self.assertEqual(parsed[0].fee_hint["tradeFeeRate"], 2500)
        selected = select_pools(parsed, policy=self.policy)
        self.assertEqual([pool.pool_id for pool in selected], ["ray-clmm-sol-usdc"])

    def test_meteora_payload_uses_documented_24h_and_verified_fields(self) -> None:
        parsed = parse_meteora_dlmm_pools(_meteora_payload())

        self.assertEqual(parsed[0].protocol, "meteora_dlmm")
        self.assertEqual(parsed[0].volume_24h_usd, Decimal("9000000"))
        self.assertEqual(parsed[0].fee_hint["base_fee_pct"], 0.04)
        selected = select_pools(parsed, policy=self.policy)
        self.assertEqual([pool.pool_id for pool in selected], ["meteor-sol-usdc"])

    def test_unverified_token_can_be_allowed_explicitly(self) -> None:
        parsed = parse_meteora_dlmm_pools(_meteora_payload())
        permissive = PoolSelectionPolicy(
            allowed_mints=self.policy.allowed_mints,
            minimum_tvl_usd=self.policy.minimum_tvl_usd,
            minimum_volume_24h_usd=self.policy.minimum_volume_24h_usd,
            max_pools_per_protocol=3,
            require_verified_tokens=False,
        )

        selected = select_pools(parsed, policy=permissive)
        self.assertEqual([pool.pool_id for pool in selected], ["meteor-sol-usdc", "meteor-unverified"])

    def test_orca_payload_is_normalized_without_trusting_price(self) -> None:
        parsed = parse_orca_whirlpool_pools(_orca_payload())

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].protocol, "orca_whirlpool")
        self.assertEqual(parsed[0].volume_24h_usd, Decimal("7000000"))
        self.assertEqual(parsed[0].fee_hint["adaptiveFeeEnabled"], False)
        self.assertEqual([pool.pool_id for pool in select_pools(parsed, policy=self.policy)], ["orca-sol-usdc"])


class SolanaPoolRegistryRefreshTest(unittest.TestCase):
    def test_refresh_keeps_source_error_but_selects_other_protocol(self) -> None:
        def fake_fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            self.assertEqual(method, "GET")
            self.assertIsNone(body)
            self.assertEqual(headers, {})
            if "raydium" in url:
                raise RuntimeError("HTTP 500: temporary upstream failure")
            return _meteora_payload()

        registry = asyncio.run(
            refresh_registry(
                policy=PoolSelectionPolicy(
                    allowed_mints=frozenset({SOL_WRAPPED_MINT, SOLANA_USDC.address}),
                ),
                sources=("raydium", "meteora"),
                proxy_url=None,
                timeout_seconds=1,
                fetch_json=fake_fetch,
            ),
        )

        self.assertTrue(registry["not_a_price_feed"])
        self.assertFalse(registry["raw_catalogue_rows_persisted"])
        self.assertEqual(registry["selected_pool_count"], 1)
        sources = registry["sources"]
        self.assertIsInstance(sources, list)
        self.assertIn("HTTP 500", sources[0]["error"])
        self.assertIsNone(sources[1]["error"])

    def test_raydium_discovery_does_not_retry_500(self) -> None:
        calls = 0

        def fake_fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            nonlocal calls
            calls += 1
            raise RuntimeError("HTTP 500")

        result = asyncio.run(
            discover_raydium_pools(
                proxy_url=None,
                timeout_seconds=1,
                fetch_json=fake_fetch,
            ),
        )
        self.assertEqual(calls, 1)
        self.assertEqual(result.pools, ())
        self.assertIn("HTTP 500", result.error or "")


if __name__ == "__main__":
    unittest.main()
