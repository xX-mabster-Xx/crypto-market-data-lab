from __future__ import annotations

import time
import unittest
from decimal import Decimal

from market_data_lab.exact_quote_pair_cache import ExactQuotePairCache
from market_data_lab.polling_quote_sources import ExactInputQuote


def _quote(
    *,
    direction: str,
    round_id: int | None,
    base_amount: str,
    quote_amount: str,
    input_symbol: str,
    output_symbol: str,
    input_amount_raw: int,
    output_amount_raw: int,
    received_ns: int,
    block_number: int | None = None,
    source_epoch: int = 0,
) -> ExactInputQuote:
    return ExactInputQuote(
        provider="TEST_PROVIDER",
        chain="test-chain",
        protocol="test",
        source_kind="test",
        pair="BASE/STABLE",
        direction=direction,
        round_id=round_id,
        requested_notional_quote=Decimal("100"),
        reference_notional_usdt=Decimal("100"),
        base_amount=Decimal(base_amount),
        quote_amount=Decimal(quote_amount),
        input_symbol=input_symbol,
        output_symbol=output_symbol,
        input_amount_raw=input_amount_raw,
        output_amount_raw=output_amount_raw,
        average_price_quote_per_base=Decimal(quote_amount) / Decimal(base_amount),
        fee_bps=Decimal("10"),
        request_rtt_ms=2,
        status="ok",
        error=None,
        response_received_realtime_ns=received_ns,
        response_received_monotonic_ns=received_ns,
        block_number=block_number,
        source_epoch=source_epoch,
    )


class ExactQuotePairCacheTest(unittest.TestCase):
    def test_keeps_matching_reverse_after_later_other_size_quote(self) -> None:
        now = time.monotonic_ns()
        cache = ExactQuotePairCache(max_buckets=8, max_records_per_bucket=2)
        buy = _quote(
            direction="buy_base",
            round_id=4,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="BASE",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            received_ns=now,
        )
        matching_sell = _quote(
            direction="sell_base",
            round_id=4,
            base_amount="1",
            quote_amount="99",
            input_symbol="BASE",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=99_000_000,
            received_ns=now,
        )
        later_other_size_sell = _quote(
            direction="sell_base",
            round_id=5,
            base_amount="0.999",
            quote_amount="98",
            input_symbol="BASE",
            output_symbol="USDC",
            input_amount_raw=999_000_000,
            output_amount_raw=98_000_000,
            received_ns=now,
        )
        cache.put(buy)
        cache.put(matching_sell)
        cache.put(later_other_size_sell)

        pair, reason = cache.best_reverse_for(
            buy,
            now_monotonic_ns=now,
            max_age_ns=1_000_000_000,
        )

        self.assertIsNotNone(pair)
        assert pair is not None
        self.assertIs(pair.exit, matching_sell)
        self.assertEqual(reason, "same_round_unpinned_state")
        self.assertTrue(pair.same_source_round)

    def test_cross_round_pair_is_explicitly_lower_quality_not_rejected(self) -> None:
        now = time.monotonic_ns()
        cache = ExactQuotePairCache()
        buy = _quote(
            direction="buy_base",
            round_id=4,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="BASE",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            received_ns=now,
        )
        sell = _quote(
            direction="sell_base",
            round_id=5,
            base_amount="1",
            quote_amount="99",
            input_symbol="BASE",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=99_000_000,
            received_ns=now,
        )
        cache.put(buy)
        cache.put(sell)

        pair, reason = cache.best_reverse_for(
            buy,
            now_monotonic_ns=now,
            max_age_ns=1_000_000_000,
        )

        self.assertIsNotNone(pair)
        assert pair is not None
        self.assertEqual(reason, "cross_round_unpinned_state")
        self.assertFalse(pair.same_source_round)
        self.assertIsNone(pair.same_block_number)

    def test_same_round_and_amount_from_prior_source_epoch_do_not_pair(self) -> None:
        now = time.monotonic_ns()
        cache = ExactQuotePairCache()
        buy = _quote(
            direction="buy_base",
            round_id=1,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="BASE",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            received_ns=now,
            source_epoch=2,
        )
        prior_epoch_sell = _quote(
            direction="sell_base",
            round_id=1,
            base_amount="1",
            quote_amount="99",
            input_symbol="BASE",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=99_000_000,
            received_ns=now,
            source_epoch=1,
        )
        cache.put(buy)
        cache.put(prior_epoch_sell)

        pair, reason = cache.best_reverse_for(
            buy,
            now_monotonic_ns=now,
            max_age_ns=1_000_000_000,
        )

        self.assertIsNone(pair)
        self.assertEqual(reason, "source_epoch_mismatch")

    def test_rejects_stale_and_block_mismatched_reverse_quotes(self) -> None:
        now = time.monotonic_ns()
        cache = ExactQuotePairCache()
        buy = _quote(
            direction="buy_base",
            round_id=4,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="BASE",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            received_ns=now - 2_000_000_000,
            block_number=100,
        )
        stale_sell = _quote(
            direction="sell_base",
            round_id=4,
            base_amount="1",
            quote_amount="99",
            input_symbol="BASE",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=99_000_000,
            received_ns=now - 2_000_000_000,
            block_number=100,
        )
        cache.put(buy)
        cache.put(stale_sell)
        pair, reason = cache.best_reverse_for(
            buy,
            now_monotonic_ns=now,
            max_age_ns=1_000_000_000,
        )
        self.assertIsNone(pair)
        self.assertEqual(reason, "entry_stale")

        fresh_buy = _quote(
            direction="buy_base",
            round_id=5,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="BASE",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            received_ns=now,
            block_number=101,
        )
        mismatched_block_sell = _quote(
            direction="sell_base",
            round_id=5,
            base_amount="1",
            quote_amount="99",
            input_symbol="BASE",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=99_000_000,
            received_ns=now,
            block_number=102,
        )
        cache.put(fresh_buy)
        cache.put(mismatched_block_sell)
        pair, reason = cache.best_reverse_for(
            fresh_buy,
            now_monotonic_ns=now,
            max_age_ns=1_000_000_000,
        )
        self.assertIsNone(pair)
        self.assertEqual(reason, "block_number_mismatch")

    def test_lru_capacity_is_bounded(self) -> None:
        now = time.monotonic_ns()
        cache = ExactQuotePairCache(max_buckets=1, max_records_per_bucket=1)
        first = _quote(
            direction="buy_base",
            round_id=1,
            base_amount="1",
            quote_amount="100",
            input_symbol="USDC",
            output_symbol="BASE",
            input_amount_raw=100_000_000,
            output_amount_raw=1_000_000_000,
            received_ns=now,
        )
        second = _quote(
            direction="buy_base",
            round_id=2,
            base_amount="2",
            quote_amount="200",
            input_symbol="USDC",
            output_symbol="BASE",
            input_amount_raw=200_000_000,
            output_amount_raw=2_000_000_000,
            received_ns=now,
        )
        cache.put(first)
        cache.put(second)

        snapshot = cache.snapshot()
        self.assertEqual(snapshot["stored_buckets"], 1)
        self.assertEqual(snapshot["stored_records"], 1)
        self.assertEqual(snapshot["counts"]["buckets_evicted_lru"], 1)

    def test_delayed_older_response_cannot_regress_a_bucket(self) -> None:
        now = time.monotonic_ns()
        cache = ExactQuotePairCache()
        fresh = _quote(
            direction="sell_base",
            round_id=5,
            base_amount="1",
            quote_amount="99",
            input_symbol="BASE",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=99_000_000,
            received_ns=now,
        )
        delayed = _quote(
            direction="sell_base",
            round_id=4,
            base_amount="1",
            quote_amount="100",
            input_symbol="BASE",
            output_symbol="USDC",
            input_amount_raw=1_000_000_000,
            output_amount_raw=100_000_000,
            received_ns=now - 1_000_000,
        )

        self.assertTrue(cache.put(fresh))
        self.assertFalse(cache.put(delayed))
        snapshot = cache.snapshot()
        self.assertEqual(snapshot["stored_records"], 1)
        self.assertEqual(snapshot["counts"]["out_of_order_records_ignored"], 1)


if __name__ == "__main__":
    unittest.main()
