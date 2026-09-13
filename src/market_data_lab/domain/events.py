"""Versioned state and market events.

Section 7: Events are immutable versioned envelopes.
Section 11: TIME-01 age measured from local receive monotonic, not calc time.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .assets import Asset

StateQuality = Literal[
    "pinned_consistent_snapshot",
    "validated_multi_account_snapshot",
    "slot_window_estimate",
    "response_time_only",
    "unknown",
]


@dataclass(frozen=True, slots=True)
class StateVersion:
    """Version identifier for a market state snapshot."""

    version: int
    published_at_ns: int
    source_ids: tuple[str, ...] = ()

    def is_superseded_by(self, other: StateVersion) -> bool:
        return other.version > self.version


@dataclass
class VersionedState:
    """A versioned snapshot of state for a given market key."""

    key: str
    version: StateVersion
    data: dict
    quality: StateQuality = "response_time_only"
    receive_time_ns: int = 0

    @property
    def age_ms(self) -> float:
        """Age from local receive monotonic (TIME-01)."""
        import time
        now_ns = time.monotonic_ns()
        # Use a fallback if receive_time_ns is wall-clock (0 means unset)
        if self.receive_time_ns == 0:
            return 0.0
        return (now_ns - self.receive_time_ns) / 1_000_000


@dataclass
class MarketUpdate:
    """A single market data update (delta or snapshot)."""

    venue_id: str
    instrument_id: str
    update_type: Literal["snapshot", "delta"]
    payload: dict
    receive_time_ns: int
    exchange_time_ns: int | None = None
    sequence: int | None = None
    checksum: str | None = None

    def is_sequential_to(self, prev_sequence: int | None) -> bool:
        """Check sequence continuity (Section 8.2, 11)."""
        if self.sequence is None or prev_sequence is None:
            return True  # Cannot verify, assume valid
        return self.sequence == prev_sequence + 1

    def is_crossed(self) -> bool:
        """Check if book has crossed prices (bids >= asks)."""
        bids = self.payload.get("bids", [])
        asks = self.payload.get("asks", [])
        if not bids or not asks:
            return False
        best_bid = bids[0]["price"] if bids else 0
        best_ask = asks[0]["price"] if asks else 0
        return best_bid >= best_ask


@dataclass
class FundingEvent:
    """A funding event for a perpetual instrument."""

    venue_id: str
    instrument_id: str
    event_time_ns: int
    rate: Decimal  # signed rate, e.g. 0.001 = 10bps
    rate_unit: Literal["fraction", "bps", "period_rate", "per_second", "cumulative_index"]
    mark_price: Decimal | None = None
    oracle_price: Decimal | None = None
    estimated_clamp: Decimal | None = None
    receive_time_ns: int = 0
    event_id: str = ""


@dataclass
class SpecEvent:
    """Instrument specification change event."""

    instrument_id: str
    field_name: str
    old_value: str | None
    new_value: str | None
    effective_time_ns: int
    reason: str = ""
