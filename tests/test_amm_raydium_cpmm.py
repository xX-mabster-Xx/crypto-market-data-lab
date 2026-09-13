from __future__ import annotations

import json
import unittest
from pathlib import Path

from market_data_lab.amm_simulation.adapters import RaydiumCpmmAdapter, UnsupportedVariant
from market_data_lab.amm_simulation.contracts import PoolRef, RaydiumCpmmPoolBody


INPUT_FIXTURE = Path(__file__).parent / "fixtures" / "raydium-cpmm-sdk-swap-base-input.json"
OUTPUT_FIXTURE = Path(__file__).parent / "fixtures" / "raydium-cpmm-sdk-swap-base-output.json"


def _pool(payload: dict[str, object], case: dict[str, object]) -> RaydiumCpmmPoolBody:
    return RaydiumCpmmPoolBody(
        pool_ref=PoolRef(
            chain_namespace="solana",
            chain_id="mainnet",
            program_id="CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
            pool_address="fixture-pool",
            protocol="raydium_cpmm",
            protocol_revision="fixture",
            asset_0_id="asset-a",
            asset_1_id="asset-b",
            pool_spec_version=1,
        ),
        vault_a_raw=int(case["reserve_a"]),
        vault_b_raw=int(case["reserve_b"]),
        protocol_fees_a_raw=0,
        protocol_fees_b_raw=0,
        fund_fees_a_raw=0,
        fund_fees_b_raw=0,
        creator_fees_a_raw=0,
        creator_fees_b_raw=0,
        trade_fee_rate=int(payload["trade_fee_rate"]),
        creator_fee_rate=int(payload["creator_fee_rate"]),
        protocol_fee_rate=int(payload["protocol_fee_rate"]),
        fund_fee_rate=int(payload["fund_fee_rate"]),
        fee_on=int(case["fee_on"]),
    )


class RaydiumCpmmSdkParityTest(unittest.TestCase):
    """Locks the Python adapter against the pinned Raydium SDK fixture."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.payload = json.loads(INPUT_FIXTURE.read_text(encoding="utf-8"))
        cls.output_payload = json.loads(OUTPUT_FIXTURE.read_text(encoding="utf-8"))
        cls.adapter = RaydiumCpmmAdapter()

    def test_exact_in_matches_pinned_sdk_amounts_and_fee_split(self) -> None:
        checked = 0
        for case in self.payload["cases"]:
            pool = _pool(self.payload, case)
            sdk_output = int(case["output_amount_raw"])
            if sdk_output <= 0:
                with self.assertRaises(UnsupportedVariant):
                    self.adapter.exact_in(pool, int(case["input_amount_raw"]), bool(case["a_to_b"]))
                continue
            transition = self.adapter.exact_in(
                pool,
                int(case["input_amount_raw"]),
                bool(case["a_to_b"]),
            )
            self.assertEqual(transition.net_output, sdk_output, case)
            self.assertEqual(transition.fees[0].amount_raw, int(case["protocol_fee_raw"]), case)
            self.assertEqual(transition.fees[1].amount_raw, int(case["fund_fee_raw"]), case)
            self.assertEqual(transition.fees[2].amount_raw, int(case["creator_fee_raw"]), case)
            checked += 1
        self.assertGreaterEqual(checked, 24)

    def test_creator_fee_asset_tracks_fee_on_and_direction(self) -> None:
        for case in self.payload["cases"]:
            if int(case["output_amount_raw"]) <= 0:
                continue
            pool = _pool(self.payload, case)
            transition = self.adapter.exact_in(
                pool,
                int(case["input_amount_raw"]),
                bool(case["a_to_b"]),
            )
            creator_fee = transition.fees[2]
            if creator_fee.amount_raw == 0:
                continue
            expected_asset = "asset-a" if (case["creator_fee_on_input"] and case["a_to_b"]) or (
                not case["creator_fee_on_input"] and not case["a_to_b"]
            ) else "asset-b"
            self.assertEqual(creator_fee.asset_id, expected_asset, case)

    def test_exact_in_post_state_tracks_contract_transfers_in_all_fee_modes(self) -> None:
        checked = 0
        for case in self.payload["cases"]:
            if int(case["output_amount_raw"]) <= 0:
                continue
            pool = _pool(self.payload, case)
            transition = self.adapter.exact_in(
                pool,
                int(case["input_amount_raw"]),
                bool(case["a_to_b"]),
            )
            self._assert_contract_post_state(pool, transition, case)
            checked += 1
        self.assertGreaterEqual(checked, 24)

    def test_vault_and_counter_post_state_conserve_and_rebuild_effective_reserves(self) -> None:
        case = self.payload["cases"][0]
        pool = _pool(self.payload, case)
        transition = self.adapter.exact_in(pool, int(case["input_amount_raw"]), bool(case["a_to_b"]))
        after = transition.body_after

        self.assertEqual(after.vault_a_raw, pool.vault_a_raw + transition.gross_input)
        self.assertEqual(after.vault_b_raw, pool.vault_b_raw - transition.gross_pool_output)
        self.assertGreater(after.protocol_fees_a_raw + after.fund_fees_a_raw + after.creator_fees_a_raw, 0)
        self.assertEqual(
            after.vault_a_raw,
            after.effective_reserves()[0]
            + after.protocol_fees_a_raw
            + after.fund_fees_a_raw
            + after.creator_fees_a_raw,
        )
        effective_a, effective_b = after.effective_reserves()
        self.assertEqual(effective_b, after.vault_b_raw - (
            after.protocol_fees_b_raw + after.fund_fees_b_raw + after.creator_fees_b_raw
        ))
        self.assertGreater(effective_a, 0)
        self.assertGreater(effective_b, 0)

    def test_exact_output_matches_pinned_sdk_input_and_fee_split(self) -> None:
        checked = 0
        for case in self.output_payload["cases"]:
            pool = _pool(self.output_payload, case)
            transition = self.adapter.exact_out(
                pool,
                int(case["output_amount_raw"]),
                bool(case["a_to_b"]),
            )
            self.assertEqual(transition.gross_input, int(case["input_amount_raw"]), case)
            self.assertEqual(transition.net_output, int(case["output_amount_raw"]), case)
            self.assertEqual(transition.fees[0].amount_raw, int(case["protocol_fee_raw"]), case)
            self.assertEqual(transition.fees[1].amount_raw, int(case["fund_fee_raw"]), case)
            self.assertEqual(transition.fees[2].amount_raw, int(case["creator_fee_raw"]), case)
            self._assert_contract_post_state(pool, transition, case)
            checked += 1
        self.assertGreaterEqual(checked, 24)

    def _assert_contract_post_state(self, pool, transition, case) -> None:
        after = transition.body_after
        a_to_b = bool(case["a_to_b"])
        creator_on_input = bool(case["creator_fee_on_input"])
        creator_fee = int(case["creator_fee_raw"])
        protocol_fee = int(case["protocol_fee_raw"])
        fund_fee = int(case["fund_fee_raw"])

        # The program transfers the full input into its vault and only the net
        # user output out.  An output-side creator fee stays in that vault.
        self.assertEqual(
            after.vault_a_raw,
            pool.vault_a_raw + (transition.gross_input if a_to_b else -transition.net_output),
            case,
        )
        self.assertEqual(
            after.vault_b_raw,
            pool.vault_b_raw + (transition.gross_input if not a_to_b else -transition.net_output),
            case,
        )
        self.assertEqual(after.protocol_fees_a_raw, protocol_fee if a_to_b else 0, case)
        self.assertEqual(after.protocol_fees_b_raw, protocol_fee if not a_to_b else 0, case)
        self.assertEqual(after.fund_fees_a_raw, fund_fee if a_to_b else 0, case)
        self.assertEqual(after.fund_fees_b_raw, fund_fee if not a_to_b else 0, case)
        creator_on_a = creator_on_input == a_to_b
        self.assertEqual(after.creator_fees_a_raw, creator_fee if creator_on_a else 0, case)
        self.assertEqual(after.creator_fees_b_raw, creator_fee if not creator_on_a else 0, case)


if __name__ == "__main__":
    unittest.main()
