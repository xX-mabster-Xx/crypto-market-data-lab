from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Mapping, Sequence

CostCategory = Literal[
    "trading_fee",
    "dex_pool_fee",
    "dex_aggregator_fee",
    "protocol_fee",
    "token_transfer_fee",
    "gas",
    "priority_fee",
    "failed_tx_cost",
    "token_account_setup",
    "wrap_unwrap",
    "approval_setup",
    "funding",
    "borrow",
    "settlement",
    "withdraw",
    "bridge_redeem",
    "rebalance",
    "residual_unwind",
    "fx_conversion",
    "scenario_execution_deterioration",
]

DepositKind = Literal[
    "none",
    "returnable_rent",
    "returnable_deposit",
    "non_returnable_fee",
]


@dataclass(frozen=True, slots=True)
class FeeTier:
    """Fee tier for trading fees."""

    name: str
    taker_bps: Decimal
    maker_bps: Decimal | None = None
    source: str = "public"
    account_context: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "taker_bps": str(self.taker_bps),
            "maker_bps": str(self.maker_bps) if self.maker_bps is not None else None,
            "source": self.source,
            "account_context": self.account_context,
        }


@dataclass(frozen=True, slots=True)
class ReturnableDeposit:
    """Returnable deposit/rent — occupies capital but may be returned."""

    deposit_kind: DepositKind
    amount: Decimal
    currency: str
    return_probability: Decimal = Decimal("1")
    return_conditions: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "deposit_kind": self.deposit_kind,
            "amount": str(self.amount),
            "currency": self.currency,
            "return_probability": str(self.return_probability),
            "return_conditions": self.return_conditions,
        }


@dataclass(frozen=True, slots=True)
class GasEstimate:
    """Gas/network fee estimate with transaction shape."""

    base_fee: Decimal
    priority_fee: Decimal
    currency: str
    tx_count: int = 1
    failed_tx_probability: Decimal = Decimal("0")
    failed_tx_cost: Decimal = Decimal("0")
    source: str = "estimated"

    @property
    def total(self) -> Decimal:
        return self.base_fee + self.priority_fee

    def as_dict(self) -> dict[str, object]:
        return {
            "base_fee": str(self.base_fee),
            "priority_fee": str(self.priority_fee),
            "total": str(self.total),
            "currency": self.currency,
            "tx_count": self.tx_count,
            "failed_tx_probability": str(self.failed_tx_probability),
            "failed_tx_cost": str(self.failed_tx_cost),
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class CapitalCharge:
    """Cost of capital as separate analytical metric."""

    annual_rate_bps: Decimal
    horizon_seconds: Decimal
    capital_amount: Decimal
    currency: str

    @property
    def charge_amount(self) -> Decimal:
        return self.capital_amount * (self.annual_rate_bps / Decimal("10000")) * (
            self.horizon_seconds / Decimal("31536000")
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "annual_rate_bps": str(self.annual_rate_bps),
            "horizon_seconds": str(self.horizon_seconds),
            "capital_amount": str(self.capital_amount),
            "charge_amount": str(self.charge_amount),
            "currency": self.currency,
        }


@dataclass
class CostRecord:
    """Complete cost breakdown for one leg/operation."""

    record_id: str
    category: CostCategory
    native_amount: Decimal
    native_currency: str
    price_conversion: Decimal | None = None
    quote_currency: str | None = None
    source: str = "unknown"
    effective_time_offset_ns: int = 0
    included_in_quote: bool = False
    quality: str = "estimated"
    fee_tier: FeeTier | None = None
    gas_estimate: GasEstimate | None = None
    returnable_deposit: ReturnableDeposit | None = None
    capital_charge: CapitalCharge | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def amount_in_quote(self) -> Decimal | None:
        if self.price_conversion is None:
            return None
        return self.native_amount * self.price_conversion

    @property
    def is_returnable(self) -> bool:
        return self.returnable_deposit is not None

    def as_dict(self) -> dict[str, object]:
        return {
            "record_id": self.record_id,
            "category": self.category,
            "native_amount": str(self.native_amount),
            "native_currency": self.native_currency,
            "price_conversion": str(self.price_conversion) if self.price_conversion else None,
            "quote_currency": self.quote_currency,
            "amount_in_quote": str(self.amount_in_quote) if self.amount_in_quote else None,
            "source": self.source,
            "effective_time_offset_ns": self.effective_time_offset_ns,
            "included_in_quote": self.included_in_quote,
            "quality": self.quality,
            "is_returnable": self.is_returnable,
            "fee_tier": self.fee_tier.as_dict() if self.fee_tier else None,
            "gas_estimate": self.gas_estimate.as_dict() if self.gas_estimate else None,
            "returnable_deposit": self.returnable_deposit.as_dict() if self.returnable_deposit else None,
            "capital_charge": self.capital_charge.as_dict() if self.capital_charge else None,
            "metadata": self.metadata,
        }


@dataclass
class CostBreakdown:
    """Aggregated cost breakdown for a complete position or candidate."""

    leg_id: str
    records: list[CostRecord] = field(default_factory=list)
    capital_charges: list[CapitalCharge] = field(default_factory=list)

    @property
    def total_fees_quote(self) -> Decimal:
        return sum(
            r.amount_in_quote or Decimal("0")
            for r in self.records
            if r.category in ("trading_fee", "dex_pool_fee", "dex_aggregator_fee", "protocol_fee")
        )

    @property
    def total_gas_quote(self) -> Decimal:
        return sum(
            r.amount_in_quote or Decimal("0")
            for r in self.records
            if r.category == "gas"
        )

    @property
    def total_returnable_quote(self) -> Decimal:
        return sum(
            r.amount_in_quote or Decimal("0")
            for r in self.records
            if r.is_returnable
        )

    @property
    def total_non_returnable_quote(self) -> Decimal:
        return sum(
            r.amount_in_quote or Decimal("0")
            for r in self.records
            if not r.is_returnable
        )

    @property
    def total_capital_charge(self) -> Decimal:
        return sum(c.charge_amount for c in self.capital_charges)

    def by_category(self) -> Mapping[str, Decimal]:
        result: dict[str, Decimal] = {}
        for record in self.records:
            amount = record.amount_in_quote or Decimal("0")
            result[record.category] = result.get(record.category, Decimal("0")) + amount
        return result

    def as_dict(self) -> dict[str, object]:
        return {
            "leg_id": self.leg_id,
            "records": [r.as_dict() for r in self.records],
            "capital_charges": [c.as_dict() for c in self.capital_charges],
            "total_fees_quote": str(self.total_fees_quote),
            "total_gas_quote": str(self.total_gas_quote),
            "total_returnable_quote": str(self.total_returnable_quote),
            "total_non_returnable_quote": str(self.total_non_returnable_quote),
            "total_capital_charge": str(self.total_capital_charge),
            "by_category": {k: str(v) for k, v in self.by_category().items()},
        }
