"""Instrument contracts and capabilities.

Section 6.2: Instrument Registry holds contract specs and capabilities.
Section 8: ContractModel determines how price/qty/ multiplier interact.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Sequence

ContractModel = Literal["linear", "inverse", "spot"]
SettlementType = Literal["cash", "deliverable"]
MarginDomain = Literal["cross", "isolated"]


@dataclass(frozen=True, slots=True)
class FeeSpec:
    """Fee specification for an instrument/venue pair."""

    taker_bps: Decimal
    maker_bps: Decimal | None = None
    source: str = "public"
    account_context: str | None = None
    verified: bool = True


@dataclass(frozen=True, slots=True)
class FundingSpec:
    """Funding specification for perpetual instruments."""

    venue_id: str
    interval_hours: int
    reference_price_source: Literal["oracle", "mark", "index"]
    settlement_asset_id: str
    max_rate_bps: Decimal | None = None
    verified: bool = True


@dataclass(frozen=True, slots=True)
class InstrumentCapability:
    """Capabilities of an instrument for routing/filtering."""

    contract_model: ContractModel
    multiplier: Decimal
    quote_asset_id: str
    base_asset_id: str
    settlement_type: SettlementType = "cash"
    margin_domain: MarginDomain = "cross"
    min_order_size: Decimal | None = None
    max_order_size: Decimal | None = None
    price_tick: Decimal | None = None
    quantity_step: Decimal | None = None
    has_funding: bool = False
    has_borrow: bool = False
    fee_spec: FeeSpec | None = None
    funding_spec: FundingSpec | None = None
    chain: str | None = None
    exchange_domain: str | None = None

    @property
    def is_linear(self) -> bool:
        return self.contract_model == "linear"

    @property
    def is_inverse(self) -> bool:
        return self.contract_model == "inverse"


@dataclass(frozen=True, slots=True)
class AssetCompatibility:
    """Explicit compatibility result between two assets on different venues.

    Never inferred from ticker text alone (Section 6.2).
    """

    base_asset_id: str
    quote_asset_id: str
    venue_a_id: str
    venue_b_id: str
    verified: bool
    notes: str = ""


@dataclass
class Instrument:
    """Full instrument definition."""

    instrument_id: str
    symbol: str
    venue_id: str
    asset_id: str
    quote_asset_id: str
    contract_model: ContractModel
    multiplier: Decimal = Decimal("1")
    decimals: int = 8
    capabilities: InstrumentCapability | None = None

    @property
    def display_name(self) -> str:
        return f"{self.venue_id}:{self.symbol}"
