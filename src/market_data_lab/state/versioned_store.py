"""Versioned Market State Store.

Section 6.1, ARCH-01: One process owns normalized versioned state.
Section 6.3: Fast stage — local data → index updates → cheap screening.
Section 11: TIME-01 age from local receive monotonic.

Section 8.2: Book recovery follows venue protocol — snapshot + deltas,
sequence/checksum validation, resnapshot after gap.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from ..domain.events import (
    MarketUpdate,
    StateVersion,
    VersionedState,
    StateQuality,
    FundingEvent,
    SpecEvent,
)


@dataclass
class StateEntry:
    """A single versioned state entry."""

    key: str
    version: StateVersion
    data: dict
    quality: StateQuality
    receive_time_ns: int
    is_valid: bool = True
    invalidated_reason: str | None = None


@dataclass
class VersionedStateStore:
    """Single-owner versioned market state.

    Per Section 6.1: one process owns normalized state.
    Per ARCH-01: local data update → index → cheap screening, no remote calls.
    """

    def __init__(self, source_epoch_ns: int | None = None) -> None:
        self._entries: dict[str, StateEntry] = {}
        self._source_epoch_ns: int = source_epoch_ns or int(time.monotonic_ns())
        self._sequence_counters: dict[str, int] = {}

    @property
    def source_epoch_ns(self) -> int:
        return self._source_epoch_ns

    def publish(self, key: str, version: StateVersion, data: dict, quality: StateQuality = "response_time_only") -> None:
        """Publish a versioned state update."""
        receive_time = int(time.monotonic_ns())
        self._entries[key] = StateEntry(
            key=key,
            version=version,
            data=data,
            quality=quality,
            receive_time_ns=receive_time,
        )

    def get(self, key: str) -> StateEntry | None:
        entry = self._entries.get(key)
        if entry is None or not entry.is_valid:
            return None
        return entry

    def get_versioned(self, key: str) -> VersionedState | None:
        """Return as VersionedState."""
        entry = self.get(key)
        if entry is None:
            return None
        return VersionedState(
            key=entry.key,
            version=entry.version,
            data=entry.data,
            quality=entry.quality,
            receive_time_ns=entry.receive_time_ns,
        )

    def invalidate(self, key: str, reason: str = "sequence_gap") -> None:
        """Mark state as invalid — requires resync."""
        if key in self._entries:
            entry = self._entries[key]
            entry.is_valid = False
            entry.invalidated_reason = reason

    def is_stale(self, key: str, max_age_ms: float) -> bool:
        """Check if state is stale per TIME-01."""
        entry = self._entries.get(key)
        if entry is None or not entry.is_valid:
            return True
        age_ms = (time.monotonic_ns() - entry.receive_time_ns) / 1_000_000
        return age_ms > max_age_ms

    def apply_market_update(self, update: MarketUpdate) -> bool:
        """Apply a market update (snapshot or delta).

        Per Section 8.2: validate sequence/checksum, resync on gap.
        Returns True if applied, False if invalidation needed.
        """
        key = f"book:{update.venue_id}:{update.instrument_id}"
        prev_seq = self._sequence_counters.get(key)

        if not update.is_sequential_to(prev_seq):
            # Sequence gap or out-of-order — invalidate book (Section 8.2, T21)
            self.invalidate(key, "sequence_gap")
            return False

        # Check for crossed book
        if update.is_crossed():
            self.invalidate(key, "crossed_snapshot")
            return False

        self._sequence_counters[key] = update.sequence or 0
        self.publish(
            key=key,
            version=StateVersion(
                version=len(self._entries.get(key, StateEntry(key=key, version=StateVersion(0, 0), data={}, quality="unknown", receive_time_ns=0)).version.version) + 1 if key in self._entries else 1,
                published_at_ns=int(time.monotonic_ns()),
                source_ids=(update.venue_id,),
            ),
            data=update.payload,
            quality="response_time_only",
        )
        return True

    def pin_view(self, dependencies: list[str]) -> dict[str, StateEntry | None]:
        """Pin a consistent view of multiple state keys.

        Per Section 10.7: view = state.pin_view(group.dependencies)
        """
        return {key: self.get(key) for key in dependencies}

    def record_funding_event(self, event: FundingEvent) -> StateEntry | None:
        """Record a funding event for an instrument."""
        key = f"funding:{event.venue_id}:{event.instrument_id}"
        self.publish(
            key=key,
            version=StateVersion(version=1, published_at_ns=event.event_time_ns, source_ids=(event.venue_id,)),
            data={
                "rate": str(event.rate),
                "rate_unit": event.rate_unit,
                "mark_price": str(event.mark_price) if event.mark_price else None,
                "oracle_price": str(event.oracle_price) if event.oracle_price else None,
                "event_time_ns": event.event_time_ns,
            },
            quality="response_time_only",
        )
        return self.get(key)

    def record_spec_event(self, event: SpecEvent) -> StateEntry | None:
        """Record a specification change."""
        key = f"spec:{event.instrument_id}"
        self.publish(
            key=key,
            version=StateVersion(version=event.effective_time_ns, published_at_ns=event.effective_time_ns, source_ids=()),
            data={
                "field": event.field_name,
                "old_value": event.old_value,
                "new_value": event.new_value,
                "effective_time_ns": event.effective_time_ns,
            },
            quality="response_time_only",
        )
        return self.get(key)
