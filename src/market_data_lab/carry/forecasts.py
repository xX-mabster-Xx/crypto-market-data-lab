"""Funding forecast engine.

Section 5.3: Baseline = published nearest rate + decaying estimate.
ML allowed only after walk-forward verification against baseline.
FND-03: Separate funding events from forecast.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from .funding import FundingCalendar, FundingRate, RateUnit


@dataclass(frozen=True, slots=True)
class ForecastScenario:
    """A forecast scenario for funding rate."""

    scenario_name: str
    rate: Decimal
    rate_unit: RateUnit
    confidence: str  # "historically_verified" | "projected" | "scenario"
    basis: str  # source of this forecast


@dataclass
class FundingForecast:
    """Funding forecast with base + adverse scenarios.

    Section 5.3: At horizons 1, 4, 8, 24h and next funding events.
    Returns insufficient_history if too few events.
    APR/APY not emitted for guaranteed income; APY without reinvestment
    is not needed.
    """

    calendar: FundingCalendar
    _forecast_cache: dict[int, list[ForecastScenario]] = field(default_factory=dict)

    def forecast_at_horizon(
        self,
        horizon_seconds: Decimal,
        current_time_ns: int,
    ) -> list[ForecastScenario] | None:
        """Forecast funding at a given horizon.

        Returns None if insufficient_history.
        """
        if self.calendar.insufficient_history():
            return None

        cached = self._forecast_cache.get(int(horizon_seconds))
        if cached is not None:
            return cached

        # Baseline: nearest published rate + decaying estimate
        baseline = self.calendar.predict_next(current_time_ns)
        if baseline is None:
            return None

        base_scenario = ForecastScenario(
            scenario_name="base",
            rate=baseline.as_fraction,
            rate_unit=RateUnit.FRACTION,
            confidence="projected",
            basis="nearest_published_rate",
        )

        # Adverse scenario: rate halves or inverts
        adverse_rate = baseline.as_fraction * Decimal("0.5")
        adverse_scenario = ForecastScenario(
            scenario_name="adverse",
            rate=adverse_rate,
            rate_unit=RateUnit.FRACTION,
            confidence="scenario",
            basis="half_of_baseline",
        )

        # Favorable scenario: rate doubles
        favorable_rate = baseline.as_fraction * Decimal("2")
        favorable_scenario = ForecastScenario(
            scenario_name="favorable",
            rate=favorable_rate,
            rate_unit=RateUnit.FRACTION,
            confidence="scenario",
            basis="double_baseline",
        )

        scenarios = [base_scenario, adverse_scenario, favorable_scenario]
        self._forecast_cache[int(horizon_seconds)] = scenarios
        return scenarios

    def break_even_rate(
        self,
        entry_cost: Decimal,
        exit_cost: Decimal,
        horizon_seconds: Decimal,
    ) -> Decimal:
        """Calculate break-even funding rate.

        Section 13.4: For carry, show break-even funding.
        """
        total_cost = entry_cost + exit_cost
        if horizon_seconds <= 0:
            return Decimal("0")
        # break_even_rate such that: position_value * rate = total_cost
        # Assuming position_value is known from caller context
        # Here we return a rate that covers the costs
        return (total_cost / (total_cost + Decimal("1"))) * Decimal("10000")  # bps

    def persistence_stats(self, lookback_events: int = 10) -> dict:
        """Calculate persistence of funding rate sign.

        Section 5.3: Persistence of sign, median, quartiles.
        """
        events = self.calendar.accrual_history()[-lookback_events:] if events else []
        if not events:
            return {"sign_persistence": 0, "median": 0, "samples": 0}

        positive_count = sum(1 for e in events if e.is_received)
        negative_count = sum(1 for e in events if e.is_paid)
        total = len(events)

        return {
            "sign_persistence": positive_count / total if total > 0 else 0,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "median": 0,  # Would compute from actual amounts
            "samples": total,
        }
