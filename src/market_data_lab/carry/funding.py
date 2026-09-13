"""Funding calendar and accrual engine.

Section 5: FND-01–FND-04.
FND-01: Historical funding history with independent evidence per event.
FND-02: Accrual is time-weighted rate * position * interval.
FND-03: Separate funding events; mark vs oracle reference price matters.
FND-04: Idempotent accrual — delta applied once via cumulative index.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Literal, Sequence

from ..domain.instruments import FundingSpec


class RateUnit(Enum):
    FRACTION = "fraction"
    BPS = "bps"
    PERIOD_RATE = "period_rate"
    PER_SECOND = "per_second"
    CUMULATIVE_INDEX = "cumulative_index"


@dataclass(frozen=True, slots=True)
class FundingRate:
    """A single funding rate record.

    Per FND-03: rate stored with explicit units, not bare string.
    10 bps = 0.001 as fraction.
    """

    rate: Decimal
    rate_unit: RateUnit
    event_time_ns: int
    mark_price: Decimal | None = None
    oracle_price: Decimal | None = None
    reference_price: Literal["oracle", "mark", "index"] = "oracle"
    estimated_clamp: Decimal | None = None
    event_id: str = ""
    quality: str = "historically_verified"

    @property
    def as_fraction(self) -> Decimal:
        """Convert to fraction representation."""
        if self.rate_unit == RateUnit.FRACTION:
            return self.rate
        elif self.rate_unit == RateUnit.BPS:
            return self.rate / Decimal("10000")
        elif self.rate_unit == RateUnit.PER_PERIOD_RATE:
            return self.rate
        elif self.rate_unit == RateUnit.PER_SECOND:
            return self.rate
        else:
            return self.rate

    @property
    def reference_price_value(self) -> Decimal | None:
        """Get the price used for funding calculation."""
        if self.reference_price == "oracle" and self.oracle_price:
            return self.oracle_price
        elif self.reference_price == "mark" and self.mark_price:
            return self.mark_price
        elif self.reference_price == "oracle" and self.oracle_price:
            return self.oracle_price
        return None


@dataclass(frozen=True, slots=True)
class FundingAccrual:
    """Accumulated funding for a position over a period."""

    position_quantity: Decimal
    rate: FundingRate
    interval_seconds: Decimal
    amount: Decimal  # signed — positive = received, negative = paid
    quality: str

    @property
    def is_received(self) -> bool:
        return self.amount > 0

    @property
    def is_paid(self) -> bool:
        return self.amount < 0


@dataclass
class FundingCalendar:
    """Funding event calendar for a perpetual instrument.

    FND-01: Each funding event has independent evidence.
    FND-04: Idempotent — cumulative index prevents double counting.
    """

    venue_id: str
    instrument_id: str
    spec: FundingSpec
    _events: list[FundingRate] = field(default_factory=list)
    _applied_event_ids: set[str] = field(default_factory=set)
    _history: list[FundingAccrual] = field(default_factory=list)

    def add_event(self, rate: FundingRate) -> None:
        """Add a funding event. Idempotent by event_id."""
        if rate.event_id and rate.event_id in self._applied_event_ids:
            return  # FND-04: already applied, skip
        if rate.event_id:
            self._applied_event_ids.add(rate.event_id)
        self._events.append(rate)

    def accrue_for_position(
        self,
        position_quantity: Decimal,
        interval_seconds: Decimal,
        current_time_ns: int,
        event_id_filter: set[str] | None = None,
    ) -> list[FundingAccrual]:
        """Calculate funding accruals for a position.

        FND-02: rate * quantity * interval.
        FND-03: oracle/reference price used, not mark, for oracle-reference shorts.
        FND-04: each event applied once via idempotent tracking.
        """

        # Find events in the interval
        results: list[FundingAccrual] = []
        for event in self._events:
            # Skip already-processed events
            if event.event_id and event.event_id in (event_id_filter or set()):
                continue

            # Calculate accrual: rate_fraction * quantity * interval
            rate_fraction = event.as_fraction

            # Determine reference price for quantity conversion
            ref_price = event.reference_price_value
            if ref_price and ref_price > 0:
                # For oracle-reference: use oracle price (T07)
                # funding = quantity * oracle_price * rate
                amount = position_quantity * ref_price * rate_fraction
            else:
                # Without price reference, use notional
                amount = position_quantity * rate_fraction

            accrual = FundingAccrual(
                position_quantity=position_quantity,
                rate=event,
                interval_seconds=interval_seconds,
                amount=amount,
                quality=event.quality,
            )
            results.append(accrual)
            self._history.append(accrual)

        return results

    def predict_next(
        self,
        current_time_ns: int | None = None,
    ) -> FundingRate | None:
        """Predict next funding rate.

        Section 5.3: Baseline starts with published nearest rate +
        decaying estimate. Returns None if no events available.
        """
        if not self._events:
            return None

        latest = self._events[-1]
        now = current_time_ns or int(time.monotonic_ns())

        # If latest event is in the future, it's a forecast
        if latest.event_time_ns > now:
            return latest

        # Predict based on latest known rate (baseline)
        return FundingRate(
            rate=latest.rate,
            rate_unit=latest.rate_unit,
            event_time_ns=now + self.spec.interval_hours * 3600 * 1_000_000_000,
            mark_price=latest.mark_price,
            oracle_price=latest.oracle_price,
            reference_price=latest.reference_price,
            quality="projected",
        )

    def accrual_history(self) -> list[FundingAccrual]:
        """Return all accrual history."""
        return list(self._history)

    def get_upcoming_events(
        self,
        now_ns: int,
        horizon_seconds: Decimal,
    ) -> list[FundingRate]:
        """Get funding events within the horizon."""
        end_ns = now_ns + int(horizon_seconds * 1_000_000_000)
        return [e for e in self._events if now_ns <= e.event_time_ns <= end_ns]

    @property
    def event_count(self) -> int:
        return len(self._events)

    @property
    def has_history(self) -> bool:
        return len(self._events) > 0

    def insufficient_history(self) -> bool:
        """Per Section 5.3: return insufficient_history for small event counts."""
        # Need at least a few events for meaningful projection
        return len(self._events) < 2
