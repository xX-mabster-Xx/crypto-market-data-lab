from __future__ import annotations

from decimal import Decimal
from typing import Mapping, Sequence

from .contracts import (
    CapitalCharge,
    CostBreakdown,
    CostCategory,
    CostRecord,
    FeeTier,
    GasEstimate,
    ReturnableDeposit,
)


def _default_fee_tier(public: bool = True) -> FeeTier:
    if public:
        return FeeTier(name="public_taker", taker_bps=Decimal("10"), source="public")
    return FeeTier(name="conservative_limit", taker_bps=Decimal("10"), source="unverified")


def calc_trading_fee(
    record_id: str,
    quantity: Decimal,
    price: Decimal,
    fee_tier: FeeTier | None = None,
    source: str = "unknown",
    included_in_quote: bool = False,
) -> CostRecord:
    tier = fee_tier or _default_fee_tier()
    fee_amount = quantity * price * (tier.taker_bps / Decimal("10000"))
    return CostRecord(
        record_id=record_id,
        category="trading_fee",
        native_amount=fee_amount,
        native_currency="USDT",
        price_conversion=Decimal("1"),
        quote_currency="USDT",
        source=source,
        included_in_quote=included_in_quote,
        quality="verified" if tier.source == "public" else "estimated",
        fee_tier=tier,
    )


def calc_gas(
    record_id: str,
    gas: GasEstimate,
    price_conversion: Decimal = Decimal("1"),
) -> CostRecord:
    return CostRecord(
        record_id=record_id,
        category="gas",
        native_amount=gas.total,
        native_currency=gas.currency,
        price_conversion=price_conversion,
        quote_currency="USDT",
        source=gas.source,
        quality="estimated",
        gas_estimate=gas,
    )


def calc_failed_tx_cost(
    record_id: str,
    gas: GasEstimate,
    price_conversion: Decimal = Decimal("1"),
) -> CostRecord:
    return CostRecord(
        record_id=record_id,
        category="failed_tx_cost",
        native_amount=gas.failed_tx_cost,
        native_currency=gas.currency,
        price_conversion=price_conversion,
        quote_currency="USDT",
        source="scenario",
        quality="scenario",
        gas_estimate=gas,
        metadata={"failed_tx_probability": str(gas.failed_tx_probability)},
    )


def calc_returnable_deposit(
    record_id: str,
    deposit: ReturnableDeposit,
    price_conversion: Decimal = Decimal("1"),
) -> CostRecord:
    return CostRecord(
        record_id=record_id,
        category="token_account_setup",
        native_amount=deposit.amount,
        native_currency=deposit.currency,
        price_conversion=price_conversion,
        quote_currency="USDT",
        source="protocol",
        quality="verified",
        returnable_deposit=deposit,
    )


def calc_capital_charge(
    annual_rate_bps: Decimal,
    horizon_seconds: Decimal,
    capital_amount: Decimal,
    currency: str = "USDT",
) -> CapitalCharge:
    return CapitalCharge(
        annual_rate_bps=annual_rate_bps,
        horizon_seconds=horizon_seconds,
        capital_amount=capital_amount,
        currency=currency,
    )


def aggregate_costs(
    leg_id: str,
    records: Sequence[CostRecord],
    capital_charges: Sequence[CapitalCharge] | None = None,
) -> CostBreakdown:
    return CostBreakdown(
        leg_id=leg_id,
        records=list(records),
        capital_charges=list(capital_charges) if capital_charges else [],
    )


def calculate_total_cost(breakdown: CostBreakdown) -> Decimal:
    """Calculate total non-returnable cost in quote currency."""
    return breakdown.total_non_returnable_quote + breakdown.total_capital_charge


def calculate_break_even(
    target_quantity: Decimal,
    target_price: Decimal,
    total_cost_quote: Decimal,
) -> Decimal:
    """Calculate break-even price including all costs."""
    if target_quantity == 0:
        return Decimal("0")
    return target_price + (total_cost_quote / target_quantity)


class CostCalculator:
    """Convenience calculator for common cost scenarios."""

    def __init__(
        self,
        fee_tier: FeeTier | None = None,
        gas_estimate: GasEstimate | None = None,
    ) -> None:
        self._fee_tier = fee_tier or _default_fee_tier()
        self._gas = gas_estimate

    def round_trip_cost(
        self,
        quantity: Decimal,
        price: Decimal,
        record_prefix: str = "leg",
    ) -> tuple[CostRecord, CostRecord]:
        """Calculate entry + exit trading fees."""
        entry = calc_trading_fee(
            f"{record_prefix}_entry",
            quantity,
            price,
            self._fee_tier,
        )
        exit_ = calc_trading_fee(
            f"{record_prefix}_exit",
            quantity,
            price,
            self._fee_tier,
        )
        return entry, exit_

    def total_round_trip_cost_quote(
        self,
        quantity: Decimal,
        price: Decimal,
    ) -> Decimal:
        entry, exit_ = self.round_trip_cost(quantity, price)
        return (entry.amount_in_quote or Decimal("0")) + (exit_.amount_in_quote or Decimal("0"))

    def with_gas(
        self,
        quantity: Decimal,
        price: Decimal,
    ) -> tuple[CostRecord, CostRecord, CostRecord | None]:
        entry, exit_ = self.round_trip_cost(quantity, price)
        gas = None
        if self._gas:
            gas = calc_gas("gas_entry", self._gas)
        return entry, exit_, gas
