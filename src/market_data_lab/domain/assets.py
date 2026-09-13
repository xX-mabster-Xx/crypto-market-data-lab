"""Asset identity and venue-level mapping.

Section 6.2: Asset/Instrument Registry is responsible for identity, contract
model, fee/funding specs and capabilities — but never infers compatibility
from a bare ticker symbol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

AssetType = Literal["crypto", "fiat", "stablecoin"]


@dataclass(frozen=True, slots=True)
class Asset:
    """Canonical asset identity."""

    asset_id: str
    symbol: str
    asset_type: AssetType
    decimals: int
    mint_or_isin: str | None = None
    chain: str | None = None


@dataclass(frozen=True, slots=True)
class VenueAsset:
    """An asset as it appears on a specific venue/contract."""

    asset_id: str
    venue_id: str
    venue_symbol: str
    multiplier: int = 1
    contract_address: str | None = None


@dataclass
class AssetMapping:
    """Maps canonical asset IDs to per-venue representations.

    Every mapping must be explicitly verified; the registry never infers
    compatibility from a bare ticker symbol.
    """

    _assets: dict[str, Asset] = field(default_factory=dict)
    _venue_assets: dict[tuple[str, str], VenueAsset] = field(default_factory=dict)
    _verified_pairs: set[tuple[str, str]] = field(default_factory=set)

    def register_asset(self, asset: Asset) -> None:
        self._assets[asset.asset_id] = asset

    def get_asset(self, asset_id: str) -> Asset | None:
        return self._assets.get(asset_id)

    def register_venue_asset(self, venue_asset: VenueAsset, verified: bool = False) -> None:
        key = (venue_asset.venue_id, venue_asset.venue_symbol)
        self._venue_assets[key] = venue_asset
        if verified:
            self._verified_pairs.add((venue_asset.venue_id, venue_asset.asset_id))

    def get_venue_asset(self, venue_id: str, venue_symbol: str) -> VenueAsset | None:
        return self._venue_assets.get((venue_id, venue_symbol))

    def is_verified_compatible(self, venue_id: str, asset_id: str) -> bool:
        """True only when the venue explicitly supports this canonical asset."""
        return (venue_id, asset_id) in self._verified_pairs

    def venue_symbol_for(self, venue_id: str, asset_id: str) -> str | None:
        """Return venue symbol for a verified mapping."""
        if not self.is_verified_compatible(venue_id, asset_id):
            return None
        asset = self.get_asset(asset_id)
        if asset is None:
            return None
        return asset.symbol
