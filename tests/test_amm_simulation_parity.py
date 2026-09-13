"""Canonical hash parity tests (AS37).

Verifies that the Python canonical_hash produces the same SHA-256 over the
amm_snapshot domain as the TypeScript codec in
workers/solana-quote-worker/src/ammCodec.ts.

The golden hashes in tests/fixtures/amm_simulation/snapshot-parity-hashes.json
were generated from the Python AmmSnapshot.economic_projection.  The TS test
loads the same projection JSON files and must produce the identical hash.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from market_data_lab.amm_simulation import canonical_hash

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "amm_simulation"


class CanonicalHashParityTest(unittest.TestCase):
    """Verify Python canonical hash matches golden fixtures (AS37)."""

    def test_python_hash_matches_golden_for_all_fixtures(self) -> None:
        hashes_file = FIXTURES_DIR / "snapshot-parity-hashes.json"
        with open(hashes_file, encoding="utf-8") as fh:
            golden = json.load(fh)

        self.assertEqual(golden["domain"], "amm_snapshot")

        for protocol, entry in golden["fixtures"].items():
            with self.subTest(protocol=protocol):
                fixture_file = FIXTURES_DIR / entry["file"]
                with open(fixture_file, encoding="utf-8") as fh:
                    projection = json.load(fh)

                computed = canonical_hash(projection, domain="amm_snapshot")
                self.assertEqual(
                    computed,
                    entry["hash"],
                    f"canonical hash mismatch for {protocol}",
                )

    def test_python_hash_is_deterministic_across_runs(self) -> None:
        fixture_file = FIXTURES_DIR / "snapshot-parity-cpmm.json"
        with open(fixture_file, encoding="utf-8") as fh:
            projection = json.load(fh)

        first = canonical_hash(projection, domain="amm_snapshot")
        second = canonical_hash(projection, domain="amm_snapshot")
        self.assertEqual(first, second)

    def test_snapshot_hash_property_matches_golden(self) -> None:
        """The AmmSnapshot.snapshot_hash property must match the golden hash."""
        from market_data_lab.amm_simulation import (
            AmmSnapshot, AssetRef, CpmmPoolBody, PoolRef,
        )

        common = {
            "chain_namespace": "synthetic",
            "chain_id": "fixture",
            "decimals": 0,
            "token_program": "synthetic-token",
            "token_extensions_fingerprint": "none",
            "spec_version": 1,
        }
        asset_a = AssetRef(
            asset_id="synthetic:fixture:cpmm:A",
            address_or_native_id="cpmm-A", **common,
        )
        asset_b = AssetRef(
            asset_id="synthetic:fixture:cpmm:B",
            address_or_native_id="cpmm-B", **common,
        )
        pool_ref = PoolRef(
            chain_namespace="synthetic", chain_id="fixture",
            program_id="synthetic_cpmm_v1-program", pool_address="pool-cpmm",
            protocol="synthetic_cpmm_v1", protocol_revision="v1",
            asset_0_id=asset_a.asset_id, asset_1_id=asset_b.asset_id,
            pool_spec_version=1,
        )
        snapshot = AmmSnapshot(
            schema_version=1,
            snapshot_id="snapshot-parity-cpmm",
            worker_generation=1,
            source_epoch=0,
            boot_id="boot-parity",
            model_version="synthetic_cpmm_v1",
            pool_refs=(pool_ref,),
            dependency_vector=(),
            pools=(
                CpmmPoolBody(
                    pool_ref=pool_ref, reserve_0_raw=1000, reserve_1_raw=900,
                    fee_numerator=1, fee_denominator=100,
                ),
            ),
            context_slot=1,
            chain_consistency="validated_multi_account_snapshot",
            state_valid_until_monotonic_ns=1,
        )

        self.assertEqual(
            snapshot.snapshot_hash,
            "55f7dd1836b87fa5aded11717ee40cbdf9a9db9effececc97be16f36afef3634",
        )


if __name__ == "__main__":
    unittest.main()
