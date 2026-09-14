from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.solana_realtime_scanner import _positive_float
from market_data_lab.solana_realtime_scanner import _positive_int
from market_data_lab.solana_realtime_scanner import _non_negative_int


class PositiveFloatValidationTest(unittest.TestCase):
    """BUG-011: _positive_float must reject NaN, inf, and non-positive values."""

    def test_rejects_nan(self) -> None:
        with self.assertRaises(ValueError):
            _positive_float({"x": float("nan")}, "x", 1.0)

    def test_rejects_positive_infinity(self) -> None:
        with self.assertRaises(ValueError):
            _positive_float({"x": float("inf")}, "x", 1.0)

    def test_rejects_negative_infinity(self) -> None:
        with self.assertRaises(ValueError):
            _positive_float({"x": float("-inf")}, "x", 1.0)

    def test_rejects_zero(self) -> None:
        with self.assertRaises(ValueError):
            _positive_float({"x": 0.0}, "x", 1.0)

    def test_rejects_negative(self) -> None:
        with self.assertRaises(ValueError):
            _positive_float({"x": -1.0}, "x", 1.0)

    def test_rejects_bool(self) -> None:
        with self.assertRaises(ValueError):
            _positive_float({"x": True}, "x", 1.0)

    def test_accepts_positive(self) -> None:
        self.assertEqual(_positive_float({"x": 3.5}, "x", 1.0), 3.5)

    def test_accepts_string_numeric(self) -> None:
        self.assertEqual(_positive_float({"x": "2.5"}, "x", 1.0), 2.5)


class PositiveIntValidationTest(unittest.TestCase):
    """BUG-011: _positive_int must reject NaN, inf, fractional floats, and bools."""

    def test_rejects_nan_decimal(self) -> None:
        with self.assertRaises(ValueError):
            _positive_int({"x": Decimal("nan")}, "x", 1)

    def test_rejects_inf_decimal(self) -> None:
        with self.assertRaises(ValueError):
            _positive_int({"x": Decimal("inf")}, "x", 1)

    def test_rejects_negative_inf_decimal(self) -> None:
        with self.assertRaises(ValueError):
            _positive_int({"x": Decimal("-inf")}, "x", 1)

    def test_rejects_fractional_float(self) -> None:
        with self.assertRaises(ValueError):
            _positive_int({"x": 1.9}, "x", 1)

    def test_rejects_fractional_string(self) -> None:
        with self.assertRaises(ValueError):
            _positive_int({"x": "1.9"}, "x", 1)

    def test_rejects_bool(self) -> None:
        with self.assertRaises(ValueError):
            _positive_int({"x": True}, "x", 1)

    def test_rejects_zero(self) -> None:
        with self.assertRaises(ValueError):
            _positive_int({"x": 0}, "x", 1)

    def test_rejects_negative(self) -> None:
        with self.assertRaises(ValueError):
            _positive_int({"x": -1}, "x", 1)

    def test_accepts_integer(self) -> None:
        self.assertEqual(_positive_int({"x": 2}, "x", 1), 2)

    def test_accepts_float_integer_value(self) -> None:
        self.assertEqual(_positive_int({"x": 2.0}, "x", 1), 2)

    def test_accepts_string_integer(self) -> None:
        self.assertEqual(_positive_int({"x": "2"}, "x", 1), 2)


class NonNegativeIntValidationTest(unittest.TestCase):
    """BUG-011: _non_negative_int must also reject NaN/inf."""

    def test_rejects_nan(self) -> None:
        with self.assertRaises(ValueError):
            _non_negative_int({"x": float("nan")}, "x", 0)

    def test_rejects_inf(self) -> None:
        with self.assertRaises(ValueError):
            _non_negative_int({"x": float("inf")}, "x", 0)

    def test_rejects_negative_inf(self) -> None:
        with self.assertRaises(ValueError):
            _non_negative_int({"x": float("-inf")}, "x", 0)

    def test_rejects_fractional_float(self) -> None:
        with self.assertRaises(ValueError):
            _non_negative_int({"x": 1.9}, "x", 0)

    def test_rejects_bool(self) -> None:
        with self.assertRaises(ValueError):
            _non_negative_int({"x": True}, "x", 0)

    def test_accepts_zero(self) -> None:
        self.assertEqual(_non_negative_int({"x": 0}, "x", 1), 0)

    def test_accepts_positive_integer(self) -> None:
        self.assertEqual(_non_negative_int({"x": 5}, "x", 1), 5)

    def test_accepts_float_integer_value(self) -> None:
        self.assertEqual(_non_negative_int({"x": 5.0}, "x", 1), 5)


if __name__ == "__main__":
    unittest.main()
