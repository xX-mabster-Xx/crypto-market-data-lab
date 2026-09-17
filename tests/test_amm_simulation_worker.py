from __future__ import annotations

import json
import unittest

from market_data_lab.amm_simulation import (
    AmmPathRequest,
    AmmSimulationLimits,
    AmmSnapshot,
    PathSimulator,
    SnapshotRequest,
    SwapLeg,
)


class AmmSimulationWorkerContractsTest(unittest.TestCase):
    def test_stage_a_exports_public_snapshot_and_evidence_contracts(self) -> None:
        self.assertIn("sdk_versions", AmmSnapshot.__dataclass_fields__)
        from market_data_lab.amm_simulation import SwapLegResult
        self.assertIn("reason", SwapLegResult.__dataclass_fields__)

    def test_worker_emitted_raydium_snapshot_shape_decodes_in_python(self) -> None:
        """The TS ``snapshots.ts`` bundle must be a valid Python snapshot."""

        bundle = {
            "schema_version": 1,
            "snapshot_id": "snapshot-req-1",
            "worker_generation": 3,
            "source_epoch": 0,
            "boot_id": "boot-1",
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
            "sdk_versions": [["@raydium-io/raydium-sdk-v2", "0.2.63-alpha"]],
        }
        from market_data_lab.amm_simulation.replay import _decode_snapshot

        snapshot = _decode_snapshot(bundle)
        self.assertEqual(snapshot.model_version, "raydium_cpmm_v1")
        self.assertEqual(snapshot.pools[0].pool_ref.pool_id, "solana:mainnet:cpmm-pool")
        self.assertEqual(snapshot.pools[0].vault_a_raw, 1_000_000_000)

    def test_worker_snapshot_feeds_a_python_path_execution(self) -> None:
        from market_data_lab.amm_simulation.replay import _decode_snapshot

        bundle = _worker_bundle()
        snapshot = _decode_snapshot(bundle)
        pool_ref = snapshot.pools[0].pool_ref
        request = AmmPathRequest(
            schema_version=1,
            request_id="worker-path",
            reason="shadow_verification",
            priority="candidate",
            snapshot=snapshot,
            legs=(
                SwapLeg(
                    leg_id="leg-1",
                    pool_ref=pool_ref,
                    input_asset_id="solana:mainnet:mint-a:6",
                    output_asset_id="solana:mainnet:mint-b:9",
                    mode="exact_in",
                    amount_source="literal",
                    amount_raw=1_000_000,
                    previous_leg_id=None,
                ),
            ),
            initial_balances=(("solana:mainnet:mint-a:6", 1_000_000_000),),
            scenario_id="worker-capture",
            limits=AmmSimulationLimits(),
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        self.assertTrue(result.complete, result.reason)
        self.assertEqual(result.leg_results[0].actual_net_output_raw, 996_386)


def _worker_bundle() -> dict[str, object]:
    return {
        "schema_version": 1,
        "snapshot_id": "snapshot-req-1",
        "worker_generation": 3,
        "source_epoch": 0,
        "boot_id": "boot-1",
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
        "sdk_versions": [["@raydium-io/raydium-sdk-v2", "0.2.63-alpha"]],
    }


if __name__ == "__main__":
    unittest.main()
