from __future__ import annotations

import base64
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from market_data_lab.raydium_clmm_prefilter import CLMM_MINIMUM_ACCOUNT_SIZE
from market_data_lab.raydium_clmm_prefilter import CLMM_SQRT_PRICE_X64_OFFSET
from market_data_lab.raydium_clmm_prefilter import CLMM_TICK_CURRENT_OFFSET
from market_data_lab.raydium_clmm_prefilter import RAYDIUM_CLMM_PROGRAM_ID
from market_data_lab.raydium_clmm_prefilter import SolanaClmmPoolStream
from market_data_lab.solana_realtime_scanner import load_solana_scanner_config


class SolanaRealtimeScannerTest(unittest.IsolatedAsyncioTestCase):
    async def test_account_subscription_places_decoded_state_on_queue(self) -> None:
        stream = SolanaClmmPoolStream(("pool",), endpoint="wss://example.invalid", proxy_url=None, timeout_seconds=1)
        data = bytearray(CLMM_MINIMUM_ACCOUNT_SIZE)
        data[CLMM_SQRT_PRICE_X64_OFFSET : CLMM_SQRT_PRICE_X64_OFFSET + 16] = (2**64).to_bytes(16, "little")
        data[CLMM_TICK_CURRENT_OFFSET : CLMM_TICK_CURRENT_OFFSET + 4] = (17).to_bytes(4, "little", signed=True)
        stream._subscription_to_pool[7] = "pool"
        stream._process_message(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "accountNotification",
                    "params": {
                        "subscription": 7,
                        "result": {
                            "context": {"slot": 123},
                            "value": {
                                "owner": RAYDIUM_CLMM_PROGRAM_ID,
                                "data": [base64.b64encode(bytes(data)).decode(), "base64"],
                            },
                        },
                    },
                },
            ),
        )

        state = await stream.next_update()
        self.assertEqual(state.pool_id, "pool")
        self.assertEqual(state.slot, 123)
        self.assertEqual(state.tick_current, 17)
        self.assertEqual(stream.latest["pool"], state)


class SolanaScannerConfigTest(unittest.TestCase):
    def test_amm_simulation_section_is_disabled_by_default_shadow_profile(self) -> None:
        content = """
[scanner]
retention_seconds = 120

[solana]
rpc_http_url = "https://rpc.example"
rpc_ws_url = "wss://rpc.example"

[[solana.raydium_clmm_pool]]
pool_id = "pool-one"
label = "BASE/BRIDGE"
"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "scanner.toml"
            path.write_text(content, encoding="utf-8")
            config = load_solana_scanner_config(path)

        amm = config.amm_simulation
        self.assertFalse(amm.enabled)
        self.assertEqual(amm.mode, "shadow")
        self.assertEqual(list(amm.allowed_protocols), [])
        self.assertFalse(amm.safe_descriptor()["wallet_or_private_key_used"])

    def test_amm_simulation_section_parses_explicit_limits_and_allowlist(self) -> None:
        content = """
[scanner]
retention_seconds = 120

[solana]
rpc_http_url = "https://rpc.example"
rpc_ws_url = "wss://rpc.example"

[[solana.raydium_clmm_pool]]
pool_id = "pool-one"
label = "BASE/BRIDGE"

[amm_simulation]
enabled = true
mode = "model_candidates"
allowed_protocols = ["raydium_cpmm", "synthetic_cpmm_v1"]
max_path_legs = 2
max_evidence_bundle_bytes = 4096
"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "scanner.toml"
            path.write_text(content, encoding="utf-8")
            config = load_solana_scanner_config(path)

        amm = config.amm_simulation
        self.assertTrue(amm.enabled)
        self.assertEqual(amm.mode, "model_candidates")
        self.assertEqual(list(amm.allowed_protocols), ["raydium_cpmm", "synthetic_cpmm_v1"])
        self.assertEqual(amm.limits()["max_path_legs"], 2)
        self.assertEqual(amm.limits()["max_evidence_bundle_bytes"], 4096)

    def test_amm_simulation_rejects_unknown_protocol_in_allowlist(self) -> None:
        content = """
[solana]
rpc_http_url = "https://rpc.example"
rpc_ws_url = "wss://rpc.example"

[[solana.raydium_clmm_pool]]
pool_id = "pool-one"
label = "BASE/BRIDGE"

[amm_simulation]
allowed_protocols = ["unchecked_curve_v9"]
"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "scanner.toml"
            path.write_text(content, encoding="utf-8")
            with self.assertRaises(ValueError):
                load_solana_scanner_config(path)


    def test_loads_named_pools_and_cex_streams(self) -> None:
        content = """
[scanner]
retention_seconds = 120
max_events_per_key = 64

[solana]
rpc_http_url = "https://rpc.example"
rpc_ws_url = "wss://rpc.example"

[[solana.raydium_clmm_pool]]
pool_id = "pool-one"
label = "PUMP/SOL"

[[cex_stream]]
venue = "MEXC"
symbols = ["PUMPUSDT", "SOLUSDT"]
"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "scanner.toml"
            path.write_text(content, encoding="utf-8")
            config = load_solana_scanner_config(path)

        self.assertEqual(config.retention_seconds, 120)
        self.assertEqual(config.pools[0].label, "PUMP/SOL")
        self.assertEqual(config.cex_streams[0].symbols, ("PUMPUSDT", "SOLUSDT"))

    def test_reads_managed_rpc_urls_from_named_environment_variables(self) -> None:
        content = """
[solana]
rpc_http_url_env = "TEST_SOLANA_HTTP"
rpc_ws_url_env = "TEST_SOLANA_WS"

[[solana.raydium_clmm_pool]]
pool_id = "pool-one"
label = "pool"
"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "scanner.toml"
            path.write_text(content, encoding="utf-8")
            with patch.dict(
                "os.environ",
                {
                    "TEST_SOLANA_HTTP": "https://provider.example/key-is-not-persisted",
                    "TEST_SOLANA_WS": "wss://provider.example/key-is-not-persisted",
                },
                clear=False,
            ):
                config = load_solana_scanner_config(path)

        self.assertEqual(config.rpc_http_url, "https://provider.example/key-is-not-persisted")
        self.assertEqual(config.rpc_ws_url, "wss://provider.example/key-is-not-persisted")

    def test_reads_local_jupiter_key_without_exposing_it_in_safe_descriptor(self) -> None:
        content = """
[solana]
rpc_http_url = "https://rpc.example"
rpc_ws_url = "wss://rpc.example"

jupiter_api_key = "test-jupiter-key"

[[solana.raydium_clmm_pool]]
pool_id = "pool-one"
label = "pool"
"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "scanner.toml"
            path.write_text(content, encoding="utf-8")
            config = load_solana_scanner_config(path)

        self.assertEqual(config.jupiter.api_key, "test-jupiter-key")
        self.assertEqual(config.jupiter.minimum_request_interval_seconds, 1.05)
        self.assertTrue(config.jupiter.safe_descriptor()["api_key_configured"])
        self.assertNotIn("test-jupiter-key", str(config.jupiter.safe_descriptor()))

    def test_local_route_requires_configured_pool_and_both_cex_legs(self) -> None:
        content = """
[solana]
rpc_http_url = "https://rpc.example"
rpc_ws_url = "wss://rpc.example"

[[solana.raydium_clmm_pool]]
pool_id = "pool-one"
label = "BASE/BRIDGE"

[[cex_stream]]
venue = "MEXC"
symbols = ["BASEUSDT", "BRIDGEUSDT"]

[local_route_evaluator]
enabled = true
minimum_quote_interval_ms = 125

[[local_route_evaluator.route]]
route_id = "base-bridge"
pool_id = "pool-one"
base_mint = "base-mint"
bridge_mint = "bridge-mint"
base_decimals = 6
bridge_decimals = 9
base_symbol = "BASE"
bridge_symbol = "BRIDGE"
settlement_symbol = "USDT"
cex_venue = "MEXC"
base_cex_symbol = "BASEUSDT"
bridge_cex_symbol = "BRIDGEUSDT"
notional_settlement = "100"
network_cost_floor_settlement = "0.01"
asset_equivalence = "test only"
"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "scanner.toml"
            path.write_text(content, encoding="utf-8")
            config = load_solana_scanner_config(path)

        self.assertTrue(config.local_route_evaluator.enabled)
        self.assertEqual(config.local_route_evaluator.minimum_quote_interval_ms, 125)
        self.assertEqual(config.local_route_evaluator.routes[0].base_cex_symbol, "BASEUSDT")


if __name__ == "__main__":
    unittest.main()
