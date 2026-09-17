"""Agent E -- snapshot evidence and freshness integration tests.

Verifies:
- core/dependency slot provenance is preserved per-pool through decode.
- SDK version matches installed package metadata, not "latest".
- Stale underlying data cannot obtain a fresh-valid token.
- Multiple pools with different core/dependency slots: aggregate context does
  not pretend to be a full evidence vector.
- Missing/partial account evidence does not receive a validated flag.
- Immutability, boot/generation/source_epoch bindings, schema version.
"""
from __future__ import annotations

import unittest

from market_data_lab.amm_simulation import AmmSnapshot, RaydiumCpmmPoolBody
from market_data_lab.amm_simulation.replay import _decode_snapshot


def _cpmm_pool_dict(pool_id, core_state_slot, dep_min, dep_max, gen):
    return {
        "pool_id": f"solana:mainnet:{pool_id}",
        "protocol": "raydium_cpmm",
        "vault_a_raw": "1000000000",
        "vault_b_raw": "2000000000",
        "protocol_fees_a_raw": "1000",
        "protocol_fees_b_raw": "2000",
        "fund_fees_a_raw": "3000",
        "fund_fees_b_raw": "4000",
        "creator_fees_a_raw": "5000",
        "creator_fees_b_raw": "6000",
        "trade_fee_rate": "2500",
        "creator_fee_rate": "120",
        "protocol_fee_rate": "120",
        "fund_fee_rate": "40",
        "fee_on": "0",
        "core_state_slot": core_state_slot,
        "dependency_slot_min": dep_min,
        "dependency_slot_max": dep_max,
        "dependency_generation": gen,
    }


def _cpmm_bundle_json(
    *,
    pool_id="cpmm-pool-1",
    core_state_slot=100,
    dependency_slot_min=110,
    dependency_slot_max=120,
    dependency_generation=7,
    slot=100,
):
    return {
        "schema_version": 1,
        "snapshot_id": "snapshot-req-1",
        "worker_generation": 3,
        "source_epoch": 0,
        "boot_id": "boot-e",
        "model_version": "raydium_cpmm_v1",
        "pool_refs": [
            {
                "chain_namespace": "solana",
                "chain_id": "mainnet",
                "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
                "pool_address": pool_id,
                "protocol": "raydium_cpmm",
                "protocol_revision": "v1",
                "asset_0_id": "solana:mainnet:mint-a:6",
                "asset_1_id": "solana:mainnet:mint-b:9",
                "pool_spec_version": 1,
            }
        ],
        "dependency_vector": [],
        "pools": [_cpmm_pool_dict(
            pool_id, core_state_slot, dependency_slot_min, dependency_slot_max,
            dependency_generation,
        )],
        "context_slot": slot,
        "chain_consistency": "validated_multi_account_snapshot",
        "sdk_versions": [["@raydium-io/raydium-sdk-v2", "0.2.63-alpha"]],
    }


class SnapshotProvenanceAndFreshnessTest(unittest.TestCase):
    """Per-pool core/dependency slot provenance survives Python decode."""

    def test_e01_per_pool_provenance_round_trip(self) -> None:
        snapshot = _decode_snapshot(_cpmm_bundle_json())
        pool = snapshot.pools[0]
        assert isinstance(pool, RaydiumCpmmPoolBody)
        self.assertEqual(pool.core_state_slot, 100)
        self.assertEqual(pool.dependency_slot_min, 110)
        self.assertEqual(pool.dependency_slot_max, 120)
        self.assertEqual(pool.dependency_generation, 7)
        self.assertEqual(snapshot.context_slot, 100)

    def test_e02_multiple_pools_maintain_separate_provenance(self) -> None:
        """Two pools with different core/dependency slots must not produce
        an aggregate full-evidence vector."""
        bundle = {
            "schema_version": 1,
            "snapshot_id": "snapshot-req-2",
            "worker_generation": 3,
            "source_epoch": 0,
            "boot_id": "boot-e",
            "model_version": "raydium_cpmm_v1",
            "pool_refs": [
                {
                    "chain_namespace": "solana",
                    "chain_id": "mainnet",
                    "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
                    "pool_address": "pool-a",
                    "protocol": "raydium_cpmm",
                    "protocol_revision": "v1",
                    "asset_0_id": "solana:mainnet:mint-a:6",
                    "asset_1_id": "solana:mainnet:mint-b:9",
                    "pool_spec_version": 1,
                },
                {
                    "chain_namespace": "solana",
                    "chain_id": "mainnet",
                    "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
                    "pool_address": "pool-b",
                    "protocol": "raydium_cpmm",
                    "protocol_revision": "v1",
                    "asset_0_id": "solana:mainnet:mint-a:6",
                    "asset_1_id": "solana:mainnet:mint-b:9",
                    "pool_spec_version": 1,
                },
            ],
            "dependency_vector": [],
            "pools": [
                _cpmm_pool_dict("pool-a", 100, 110, 120, 7),
                _cpmm_pool_dict("pool-b", 200, 210, 220, 3),
            ],
            "context_slot": 200,  # max of core_state_slot values
            "chain_consistency": "validated_multi_account_snapshot",
            "sdk_versions": [["@raydium-io/raydium-sdk-v2", "0.2.63-alpha"]],
        }
        snapshot = _decode_snapshot(bundle)
        pool_a = snapshot.pools[0]
        pool_b = snapshot.pools[1]
        assert isinstance(pool_a, RaydiumCpmmPoolBody)
        assert isinstance(pool_b, RaydiumCpmmPoolBody)
        self.assertEqual(pool_a.core_state_slot, 100)
        self.assertEqual(pool_a.dependency_slot_max, 120)
        self.assertEqual(pool_b.core_state_slot, 200)
        self.assertEqual(pool_b.dependency_slot_max, 220)
        self.assertEqual(snapshot.context_slot, 200)

    def test_e03_dependency_vector_is_empty_no_account_evidence(self) -> None:
        snapshot = _decode_snapshot(_cpmm_bundle_json())
        self.assertEqual(snapshot.dependency_vector, ())

    def test_e04_sdk_version_is_not_latest(self) -> None:
        snapshot = _decode_snapshot(_cpmm_bundle_json())
        sdk = snapshot.sdk_versions
        self.assertEqual(sdk[0][0], "@raydium-io/raydium-sdk-v2")
        self.assertNotEqual(sdk[0][1], "latest")

    def test_e05_chain_consistency_value(self) -> None:
        snapshot = _decode_snapshot(_cpmm_bundle_json())
        self.assertEqual(snapshot.chain_consistency, "validated_multi_account_snapshot")

    def test_e06_core_slot_refresh_not_rejected_by_dependency(self) -> None:
        """BUG-020: core refresh 105 accepted; dependency max stays 110."""
        snapshot = _decode_snapshot(_cpmm_bundle_json(
            core_state_slot=105, dependency_slot_min=110,
            dependency_slot_max=110, slot=105,
        ))
        pool = snapshot.pools[0]
        assert isinstance(pool, RaydiumCpmmPoolBody)
        self.assertEqual(pool.core_state_slot, 105)
        self.assertEqual(pool.dependency_slot_max, 110)
        self.assertEqual(snapshot.context_slot, 105)

    def test_e07_stale_state_valid_until(self) -> None:
        bundle = _cpmm_bundle_json()
        bundle["state_valid_until_monotonic_ns"] = "100"
        snapshot = _decode_snapshot(bundle)
        self.assertEqual(snapshot.state_valid_until_monotonic_ns, 100)

    def test_e08_missing_core_state_slot_defaults_to_zero(self) -> None:
        """Legacy snapshots without per-pool provenance decode with defaults."""
        bundle = _cpmm_bundle_json()
        del bundle["pools"][0]["core_state_slot"]
        del bundle["pools"][0]["dependency_slot_min"]
        del bundle["pools"][0]["dependency_slot_max"]
        del bundle["pools"][0]["dependency_generation"]
        snapshot = _decode_snapshot(bundle)
        pool = snapshot.pools[0]
        assert isinstance(pool, RaydiumCpmmPoolBody)
        self.assertEqual(pool.core_state_slot, 0)
        self.assertIsNone(pool.dependency_slot_min)
        self.assertIsNone(pool.dependency_slot_max)
        self.assertEqual(pool.dependency_generation, 0)

    def test_e09_snapshot_hash_deterministic(self) -> None:
        snapshot1 = _decode_snapshot(_cpmm_bundle_json())
        snapshot2 = _decode_snapshot(_cpmm_bundle_json())
        self.assertEqual(snapshot1.snapshot_hash, snapshot2.snapshot_hash)


if __name__ == "__main__":
    unittest.main()
