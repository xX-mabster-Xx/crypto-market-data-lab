from __future__ import annotations

import unittest

from market_data_lab.contract_models import resolve_perp_contract_model


class PerpContractModelTest(unittest.TestCase):
    def test_linear_perpetual_is_the_only_currently_supported_shape(self) -> None:
        model = resolve_perp_contract_model("LinearPerpetual")

        self.assertTrue(model.supported)
        self.assertEqual(model.model_id, "linear_base_quantity_perpetual_v1")
        self.assertEqual(model.quantity_semantics, "base_asset_quantity")
        self.assertIsNone(model.reason)

    def test_inverse_and_missing_types_are_explicitly_not_modelled(self) -> None:
        inverse = resolve_perp_contract_model("inverse_perpetual")
        missing = resolve_perp_contract_model(None)

        self.assertFalse(inverse.supported)
        self.assertEqual(inverse.reason, "unsupported_contract_model")
        self.assertFalse(missing.supported)
        self.assertEqual(missing.reason, "contract_type_unavailable")


if __name__ == "__main__":
    unittest.main()
