from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.quantity_lattice import common_quantity_step
from market_data_lab.quantity_lattice import round_down_to_common_quantity_lattice


class QuantityLatticeTest(unittest.TestCase):
    def test_common_lattice_prevents_invalid_sequential_rounding(self) -> None:
        steps = (Decimal("0.03"), Decimal("0.02"))

        self.assertEqual(common_quantity_step(steps), Decimal("0.06"))
        self.assertIsNone(round_down_to_common_quantity_lattice(Decimal("0.04"), steps))
        self.assertEqual(
            round_down_to_common_quantity_lattice(Decimal("0.119"), steps),
            Decimal("0.06"),
        )

    def test_unknown_leg_step_does_not_create_a_fake_constraint(self) -> None:
        self.assertEqual(
            round_down_to_common_quantity_lattice(
                Decimal("1.234"),
                (None, Decimal("0.01")),
            ),
            Decimal("1.23"),
        )

    def test_invalid_or_excessively_precise_step_is_not_unrestricted(self) -> None:
        self.assertIsNone(
            round_down_to_common_quantity_lattice(
                Decimal("1"),
                (Decimal("0"),),
            )
        )
        self.assertIsNone(
            round_down_to_common_quantity_lattice(
                Decimal("1"),
                (Decimal("1e-19"),),
            )
        )


if __name__ == "__main__":
    unittest.main()
