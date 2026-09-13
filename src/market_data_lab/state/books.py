"""Book Store — manages L2 book snapshots and deltas.

Section 6.2: State Builder/Book Store owns correct delta application.
Does NOT lose delta sequence for speed.
Section 8.2: Book recovery per venue protocol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping

from ..domain.events import MarketUpdate
from ..execution_cost.contracts import OrderBookSnapshot, DepthLevel


@dataclass
class BookStore:
    """Manages book snapshots with sequence validation."""

    def __init__(self) -> None:
        self._snapshots: dict[str, OrderBookSnapshot] = {}
        self._sequences: dict[str, int] = {}
        self._invalidated: set[str] = set()

    def apply_update(self, update: MarketUpdate) -> bool:
        """Apply a market update. Returns True if applied, False if invalidated."""

        key = update.instrument_id

        # Check sequence continuity
        if not update.is_sequential_to(self._sequences.get(key)):
            self._invalidated.add(key)
            return False

        if update.is_crossed():
            self._invalidated.add(key)
            return False

        self._sequences[key] = update.sequence or 0

        # Build book snapshot from payload
        bids = self._parse_levels(update.payload.get("bids", []))
        asks = self._parse_levels(update.payload.get("asks", []))

        if update.update_type == "snapshot":
            self._snapshots[key] = OrderBookSnapshot(
                instrument_id=key,
                venue_id=update.venue_id,
                bids=bids,
                asks=asks,
                timestamp_ns=update.receive_time_ns,
                version=len(self._snapshots.get(key, OrderBookSnapshot(key, update.venue_id, [], [], 0, 0)).bids) + 1,
                sequence=update.sequence,
                checksum=update.checksum,
            )
        else:
            # Delta — apply to existing snapshot
            existing = self._snapshots.get(key)
            if existing:
                self._snapshots[key] = OrderBookSnapshot(
                    instrument_id=key,
                    venue_id=update.venue_id,
                    bids=bids if bids else existing.bids,
                    asks=asks if asks else existing.asks,
                    timestamp_ns=update.receive_time_ns,
                    version=existing.version + 1,
                    sequence=update.sequence,
                    checksum=update.checksum,
                )

        return True

    def get_book(self, instrument_id: str) -> OrderBookSnapshot | None:
        if instrument_id in self._invalidated:
            return None
        return self._snapshots.get(instrument_id)

    def is_valid(self, instrument_id: str) -> bool:
        return instrument_id not in self._invalidated

    def needs_resync(self, instrument_id: str) -> bool:
        return instrument_id in self._invalidated

    def _parse_levels(self, levels: list[dict]) -> list[DepthLevel]:
        result = []
        for level in levels:
            result.append(DepthLevel(
                price=Decimal(str(level["price"])),
                size=Decimal(str(level["size"])),
            ))
        return result

    def invalidate(self, instrument_id: str, reason: str = "sequence_gap") -> None:
        self._invalidated.add(instrument_id)
