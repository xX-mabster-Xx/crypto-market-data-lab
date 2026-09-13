"""Currency conversion and capital charge estimation.

Section 8.5: FX conversion per side of market.
Section 13.1: Capital cost as separate analytical metric.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

from ..cost_breakdown.calculator import calc_capital_charge
from ..cost_breakdown.contracts import CapitalCharge, CostRecord


@dataclass(frozen=True, slots=True)
class FXRate:
    """A verified FX rate."""

    from_asset: str
    to_asset: str
    rate: Decimal
    source: str
    quality: str = "verified"
    effective_time_ns: int = 0


@dataclass
class FXConversion:
    """FX converter with explicit rates."""

    _rates: dict[tuple[str, str], FXRate] = None

    def __post_init__(self) -> None:
        if self._rates is None:
            self._rates = {}

    def add_rate(self, rate: FXRate) -> None:
        self._rates[(rate.from_asset, rate.to_asset)] = rate

    def convert(
        self,
        amount: Decimal,
        from_asset: str,
        to_asset: str,
    ) -> tuple[Decimal, FXRate | None]:
        """Convert amount with explicit rate.

        Section 13: T13 USDC/USDT=0.98 — conversion on market side.
        No implicit parity assumption.
        """
        if from_asset == to_asset:
            return amount, None

        rate = self._rates.get((from_asset, to_asset))
        if rate is None:
            # Try reverse
            reverse = self._rates.get((to_asset, from_asset))
            if reverse:
                rate = FXRate(
                    from_asset=to_asset,
                    to_asset=from_asset,
                    rate=Decimal("1") / reverse.rate,
                    source=reverse.source,
                    quality=reverse.quality,
                    effective_time_ns=reverse.effective_time_ns,
                )
                return amount * (Decimal("1") / rate.rate), rate

        if rate is None:
            return amount, None

        return amount * rate.rate, rate


def convert_currency(
    amount: Decimal,
    from_asset: str,
    to_asset: str,
    rates: Mapping[tuple[str, str], FXRate],
) -> tuple[Decimal, FXRate | None]:
    """Standalone conversion using a rate map."""
    converter = FXConversion(dict(rates))
    return converter.convert(amount, from_asset, to_asset)


def estimate_capital_charge(
    capital_amount: Decimal,
    annual_rate_bps: Decimal,
    horizon_seconds: Decimal,
    currency: str = "USDT",
) -> CostRecord:
    """Estimate the cost of capital as a separate analytical metric.

    Section 13.1: Capital cost is separate from transaction fees.
    """
    charge = calc_capital_charge(annual_rate_bps, horizon_seconds, capital_amount, currency)
    return CostRecord(
        record_id="capital_charge",
        category="gas",  # closest category for reporting
        native_amount=charge.charge_amount,
        native_currency=currency,
        price_conversion=Decimal("1"),
        quote_currency=currency,
        source="estimated",
        quality="scenario",
        capital_charge=charge,
        metadata={"annual_rate_bps": str(annual_rate_bps)},
    )
