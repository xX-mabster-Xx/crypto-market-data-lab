from __future__ import annotations

import unittest

from market_data_lab.strategy_quality import derive_candidate_quality


class StrategyQualityTest(unittest.TestCase):
    def test_entry_basis_is_not_upgraded_to_pnl_or_execution(self) -> None:
        quality = derive_candidate_quality(
            analysis_kind="dex_perp_entry_hedge",
            timing_valid=True,
            pnl_model_complete=False,
            full_exit_model=False,
            funding_quality="none_required",
            has_exact_dex_quote=True,
            top_of_book_only=True,
            dex_post_trade_pool_state_simulated=None,
        )

        self.assertEqual(quality.market_evidence, "exact_quote_and_bbo")
        self.assertEqual(quality.economics, "entry_basis_only")
        self.assertEqual(quality.validation, "quote_checked")
        self.assertEqual(quality.resource_context, "hypothetical")
        self.assertFalse(quality.execution_enabled)

    def test_stale_pair_is_explicitly_not_quote_checked(self) -> None:
        quality = derive_candidate_quality(
            analysis_kind="dex_perp_paired_exact_quote_flat_model",
            timing_valid=False,
            pnl_model_complete=True,
            full_exit_model=True,
            funding_quality="none_required",
            has_exact_dex_quote=True,
            top_of_book_only=True,
            dex_post_trade_pool_state_simulated=False,
        )

        self.assertEqual(quality.economics, "current_flat_model")
        self.assertEqual(quality.validation, "stale_or_unsynchronised")
        self.assertEqual(quality.funding_quality, "none_required")
        self.assertFalse(quality.execution_enabled)


if __name__ == "__main__":
    unittest.main()
