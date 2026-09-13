from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.raydium_clmm_prefilter import CLMM_MINIMUM_ACCOUNT_SIZE
from market_data_lab.raydium_clmm_prefilter import CLMM_SQRT_PRICE_X64_OFFSET
from market_data_lab.raydium_clmm_prefilter import CLMM_TICK_CURRENT_OFFSET
from market_data_lab.raydium_clmm_prefilter import CLMM_TOKEN_0_DECIMALS_OFFSET
from market_data_lab.raydium_clmm_prefilter import CLMM_TOKEN_0_MINT_OFFSET
from market_data_lab.raydium_clmm_prefilter import CLMM_TOKEN_1_DECIMALS_OFFSET
from market_data_lab.raydium_clmm_prefilter import CLMM_TOKEN_1_MINT_OFFSET
from market_data_lab.raydium_clmm_prefilter import ClmmPoolState
from market_data_lab.raydium_clmm_prefilter import base58_encode
from market_data_lab.raydium_clmm_prefilter import decode_clmm_pool_state
from market_data_lab.raydium_clmm_prefilter import ui_price


class RaydiumClmmPrefilterTest(unittest.TestCase):
    def test_decodes_packed_leading_clmm_pool_state_fields(self) -> None:
        data = bytearray(CLMM_MINIMUM_ACCOUNT_SIZE)
        token_0 = bytes(range(32))
        token_1 = bytes(range(32, 64))
        data[CLMM_TOKEN_0_MINT_OFFSET : CLMM_TOKEN_0_MINT_OFFSET + 32] = token_0
        data[CLMM_TOKEN_1_MINT_OFFSET : CLMM_TOKEN_1_MINT_OFFSET + 32] = token_1
        data[CLMM_TOKEN_0_DECIMALS_OFFSET] = 9
        data[CLMM_TOKEN_1_DECIMALS_OFFSET] = 6
        data[CLMM_SQRT_PRICE_X64_OFFSET : CLMM_SQRT_PRICE_X64_OFFSET + 16] = (
            2**64
        ).to_bytes(16, "little")
        data[CLMM_TICK_CURRENT_OFFSET : CLMM_TICK_CURRENT_OFFSET + 4] = (-42).to_bytes(
            4,
            "little",
            signed=True,
        )

        pool = decode_clmm_pool_state(
            bytes(data),
            pool_id="pool",
            slot=123,
            received_realtime_ns=1_000,
            received_monotonic_ns=2_000,
            source="test",
        )

        self.assertEqual(pool.token_0_mint, base58_encode(token_0))
        self.assertEqual(pool.token_1_mint, base58_encode(token_1))
        self.assertEqual(pool.token_0_decimals, 9)
        self.assertEqual(pool.token_1_decimals, 6)
        self.assertEqual(pool.sqrt_price_x64, 2**64)
        self.assertEqual(pool.tick_current, -42)
        self.assertEqual(pool.slot, 123)

    def test_ui_price_applies_decimals_and_supports_inverse_direction(self) -> None:
        pool = ClmmPoolState(
            pool_id="pool",
            token_0_mint="SOL",
            token_1_mint="USDT",
            token_0_decimals=9,
            token_1_decimals=6,
            sqrt_price_x64=2**64,
            tick_current=0,
            slot=1,
            received_realtime_ns=1,
            received_monotonic_ns=1,
            source="test",
        )

        self.assertEqual(ui_price(pool, base_mint="SOL", quote_mint="USDT"), Decimal("1000"))
        self.assertEqual(ui_price(pool, base_mint="USDT", quote_mint="SOL"), Decimal("0.001"))

    def test_decode_rejects_short_or_zero_price_data(self) -> None:
        with self.assertRaises(ValueError):
            decode_clmm_pool_state(
                b"\x00" * (CLMM_MINIMUM_ACCOUNT_SIZE - 1),
                pool_id="pool",
                slot=None,
                received_realtime_ns=1,
                received_monotonic_ns=1,
                source="test",
            )
        with self.assertRaises(ValueError):
            decode_clmm_pool_state(
                b"\x00" * CLMM_MINIMUM_ACCOUNT_SIZE,
                pool_id="pool",
                slot=None,
                received_realtime_ns=1,
                received_monotonic_ns=1,
                source="test",
            )


if __name__ == "__main__":
    unittest.main()
