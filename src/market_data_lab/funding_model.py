"""Pure, conservative funding-event projections for linear perpetual research.

Public tickers often expose a rate without enough calendar information to say
that a new position will receive a payment during a chosen holding horizon.
This module deliberately separates a display-normalised hourly number from a
cashflow that is actually scheduled inside that horizon.  It contains no
network, account, order, or persistence behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR


_MILLISECONDS_PER_HOUR = Decimal(3_600_000)
_MILLISECONDS_PER_MINUTE = 60_000
_MAX_SCHEDULED_EVENTS = 1_024


def _positive(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def _prospective_rate_kind(value: str | None) -> bool:
    """Whether a source explicitly labels the observed rate as future-facing."""

    normalized = value.lower() if isinstance(value, str) else ""
    return any(token in normalized for token in ("next", "future", "estimated"))


@dataclass(frozen=True, slots=True)
class FundingProjection:
    """A display rate plus an optional discrete-event horizon calculation."""

    normalized_cashflow_per_hour: Decimal | None
    scheduled_cashflow_for_horizon: Decimal | None
    scheduled_event_times_ms: tuple[int, ...]
    quality: str
    reason: str | None
    reference_price: Decimal | None
    reference_price_kind: str | None

    @property
    def horizon_model_complete(self) -> bool:
        return self.scheduled_cashflow_for_horizon is not None


def project_common_rate_discrete_funding(
    *,
    rate: Decimal | None,
    rate_kind: str | None,
    interval_minutes: int | None,
    next_event_time_ms: int | None,
    reference_price: Decimal | None,
    reference_price_kind: str | None,
    quantity: Decimal,
    side: str,
    now_realtime_ns: int,
    horizon_hours: Decimal,
) -> FundingProjection:
    """Project a signed common funding rate only across known future events.

    Convention: a positive common rate means the long pays the short.  The
    caller supplies a venue reference price and must label its provenance.  A
    missing event time, unknown rate orientation, or invalid interval returns
    a useful normalised display rate where possible, but never invents a
    horizon cashflow.
    """

    if side not in {"long", "short"}:
        raise ValueError("side must be long or short")
    if not _positive(quantity):
        raise ValueError("quantity must be finite and positive")
    if not _positive(horizon_hours):
        raise ValueError("horizon_hours must be finite and positive")
    if now_realtime_ns < 0:
        raise ValueError("now_realtime_ns must be non-negative")

    if rate is None or not rate.is_finite():
        return FundingProjection(
            normalized_cashflow_per_hour=None,
            scheduled_cashflow_for_horizon=None,
            scheduled_event_times_ms=(),
            quality="unknown",
            reason="funding_rate_unavailable",
            reference_price=reference_price,
            reference_price_kind=reference_price_kind,
        )
    if not _positive(reference_price):
        return FundingProjection(
            normalized_cashflow_per_hour=None,
            scheduled_cashflow_for_horizon=None,
            scheduled_event_times_ms=(),
            quality="unknown",
            reason="funding_reference_price_unavailable",
            reference_price=reference_price,
            reference_price_kind=reference_price_kind,
        )

    sign = Decimal("1") if side == "short" else Decimal("-1")
    event_cashflow = quantity * reference_price * rate * sign
    normalized_per_hour: Decimal | None = None
    interval_ms: int | None = None
    if interval_minutes is not None and interval_minutes > 0:
        normalized_per_hour = event_cashflow * Decimal(60) / Decimal(interval_minutes)
        interval_ms = interval_minutes * _MILLISECONDS_PER_MINUTE

    if not _prospective_rate_kind(rate_kind):
        return FundingProjection(
            normalized_cashflow_per_hour=normalized_per_hour,
            scheduled_cashflow_for_horizon=None,
            scheduled_event_times_ms=(),
            quality="unknown",
            reason="funding_rate_not_explicitly_for_next_event",
            reference_price=reference_price,
            reference_price_kind=reference_price_kind,
        )
    if next_event_time_ms is None:
        return FundingProjection(
            normalized_cashflow_per_hour=normalized_per_hour,
            scheduled_cashflow_for_horizon=None,
            scheduled_event_times_ms=(),
            quality="unknown",
            reason="next_funding_event_unknown",
            reference_price=reference_price,
            reference_price_kind=reference_price_kind,
        )

    now_ms = now_realtime_ns // 1_000_000
    if next_event_time_ms <= now_ms:
        return FundingProjection(
            normalized_cashflow_per_hour=normalized_per_hour,
            scheduled_cashflow_for_horizon=None,
            scheduled_event_times_ms=(),
            quality="unknown",
            reason="next_funding_event_not_future",
            reference_price=reference_price,
            reference_price_kind=reference_price_kind,
        )
    horizon_ms = int((horizon_hours * _MILLISECONDS_PER_HOUR).to_integral_value(rounding=ROUND_FLOOR))
    end_ms = now_ms + horizon_ms
    event_times: list[int] = []
    event_time = next_event_time_ms
    if interval_ms is None:
        if event_time <= end_ms:
            event_times.append(event_time)
    else:
        while event_time <= end_ms:
            event_times.append(event_time)
            if len(event_times) >= _MAX_SCHEDULED_EVENTS:
                return FundingProjection(
                    normalized_cashflow_per_hour=normalized_per_hour,
                    scheduled_cashflow_for_horizon=None,
                    scheduled_event_times_ms=tuple(event_times),
                    quality="unknown",
                    reason="funding_event_schedule_exceeds_safety_cap",
                    reference_price=reference_price,
                    reference_price_kind=reference_price_kind,
                )
            event_time += interval_ms
    return FundingProjection(
        normalized_cashflow_per_hour=normalized_per_hour,
        scheduled_cashflow_for_horizon=event_cashflow * len(event_times),
        scheduled_event_times_ms=tuple(event_times),
        quality="projected",
        reason=None,
        reference_price=reference_price,
        reference_price_kind=reference_price_kind,
    )
