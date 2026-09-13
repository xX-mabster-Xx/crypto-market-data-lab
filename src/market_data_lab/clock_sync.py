"""Clock diagnostics shared by the live market-data recorders.

Exchange HTTP time endpoints are useful diagnostics, but they are not NTP: a
single server timestamp cannot separate outbound delay from inbound delay.  The
helpers below therefore keep the raw measurements, prefer low-delay samples,
and report uncertainty instead of silently rewriting market-data timestamps.
"""

from __future__ import annotations

import math
import statistics
import time
from collections import deque
from dataclasses import dataclass, field

from market_data_lab.live_stats import BoundedDistribution


NANOSECONDS_PER_MILLISECOND = 1_000_000


def _milliseconds(value: int | float | None) -> float | None:
    if value is None:
        return None
    return round(value / NANOSECONDS_PER_MILLISECOND, 6)


@dataclass(frozen=True)
class LocalClockReading:
    """A near-simultaneous CLOCK_REALTIME/CLOCK_MONOTONIC reading."""

    realtime_ns: int
    monotonic_ns: int
    capture_span_ns: int = 0


def read_local_clocks() -> LocalClockReading:
    """Read realtime between two monotonic reads and use their midpoint."""
    monotonic_before_ns = time.monotonic_ns()
    realtime_ns = time.time_ns()
    monotonic_after_ns = time.monotonic_ns()
    return LocalClockReading(
        realtime_ns=realtime_ns,
        monotonic_ns=monotonic_before_ns
        + (monotonic_after_ns - monotonic_before_ns) // 2,
        capture_span_ns=monotonic_after_ns - monotonic_before_ns,
    )


def calculate_clock_sample(
    request: LocalClockReading,
    receive: LocalClockReading,
    *,
    server_time_ns: int,
    server_time_resolution_ns: int,
    venue: str,
    endpoint: str,
) -> dict[str, object]:
    """Estimate exchange-minus-local offset with an HTTP midpoint.

    RTT is measured with the monotonic clock.  The local midpoint is projected
    from the request-side realtime reading, so a realtime correction during the
    HTTP request is visible as ``wall_monotonic_divergence_ns`` instead of being
    hidden inside the RTT.
    """
    rtt_ns = receive.monotonic_ns - request.monotonic_ns
    if rtt_ns < 0:
        raise ValueError("receive monotonic timestamp precedes request")
    if server_time_resolution_ns <= 0:
        raise ValueError("server_time_resolution_ns must be positive")

    wall_elapsed_ns = receive.realtime_ns - request.realtime_ns
    wall_monotonic_divergence_ns = wall_elapsed_ns - rtt_ns
    local_midpoint_ns = request.realtime_ns + rtt_ns // 2
    offset_ns = server_time_ns - local_midpoint_ns
    network_uncertainty_ns = rtt_ns // 2
    total_uncertainty_ns = network_uncertainty_ns + server_time_resolution_ns

    return {
        "venue": venue,
        "endpoint": endpoint,
        "ts_request_ns": request.realtime_ns,
        "ts_receive_ns": receive.realtime_ns,
        "monotonic_request_ns": request.monotonic_ns,
        "monotonic_receive_ns": receive.monotonic_ns,
        "request_clock_capture_span_ns": request.capture_span_ns,
        "receive_clock_capture_span_ns": receive.capture_span_ns,
        "server_time_ns": server_time_ns,
        "server_time_resolution_ns": server_time_resolution_ns,
        "local_midpoint_ns": local_midpoint_ns,
        "rtt_ns": rtt_ns,
        "rtt_ms": _milliseconds(rtt_ns),
        "wall_elapsed_ns": wall_elapsed_ns,
        "wall_monotonic_divergence_ns": wall_monotonic_divergence_ns,
        "wall_monotonic_divergence_ms": _milliseconds(wall_monotonic_divergence_ns),
        "offset_ns": offset_ns,
        "offset_ms": _milliseconds(offset_ns),
        "network_uncertainty_bound_ns": network_uncertainty_ns,
        "network_uncertainty_bound_ms": _milliseconds(network_uncertainty_ns),
        "total_uncertainty_bound_ns": total_uncertainty_ns,
        "total_uncertainty_bound_ms": _milliseconds(total_uncertainty_ns),
    }


@dataclass
class ClockOffsetEstimator:
    """Bounded, minimum-delay filter for HTTP midpoint clock samples."""

    window_capacity: int = 256
    count: int = 0
    rtt: BoundedDistribution = field(default_factory=BoundedDistribution)
    offset: BoundedDistribution = field(default_factory=BoundedDistribution)
    wall_monotonic_divergence: BoundedDistribution = field(
        default_factory=BoundedDistribution,
    )
    _recent: deque[dict[str, object]] = field(init=False, repr=False)
    _minimum_rtt_sample: dict[str, object] | None = field(default=None, init=False, repr=False)
    _latest_sample: dict[str, object] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.window_capacity <= 0:
            raise ValueError("window_capacity must be positive")
        self._recent = deque(maxlen=self.window_capacity)

    def add(self, sample: dict[str, object]) -> None:
        rtt_ns = int(sample["rtt_ns"])
        offset_ns = int(sample["offset_ns"])
        divergence_ns = int(sample["wall_monotonic_divergence_ns"])
        self.count += 1
        self.rtt.add(rtt_ns)
        self.offset.add(offset_ns)
        self.wall_monotonic_divergence.add(divergence_ns)
        stored = dict(sample)
        self._recent.append(stored)
        self._latest_sample = stored
        if (
            self._minimum_rtt_sample is None
            or rtt_ns < int(self._minimum_rtt_sample["rtt_ns"])
        ):
            self._minimum_rtt_sample = stored

    @staticmethod
    def _brief(sample: dict[str, object] | None) -> dict[str, object] | None:
        if sample is None:
            return None
        return {
            "ts_receive_ns": int(sample["ts_receive_ns"]),
            "rtt_ms": _milliseconds(int(sample["rtt_ns"])),
            "offset_ms": _milliseconds(int(sample["offset_ns"])),
            "total_uncertainty_bound_ms": _milliseconds(
                int(sample["total_uncertainty_bound_ns"]),
            ),
        }

    def to_dict(self) -> dict[str, object]:
        base: dict[str, object] = {
            "samples": self.count,
            "recent_window_samples": len(self._recent),
            "recent_window_capacity": self.window_capacity,
            "rtt": self.rtt.as_milliseconds(),
            "exchange_minus_local_offset": self.offset.as_milliseconds(),
            "wall_monotonic_divergence": self.wall_monotonic_divergence.as_milliseconds(),
            "minimum_rtt_sample": self._brief(self._minimum_rtt_sample),
            "latest_sample": self._brief(self._latest_sample),
        }
        if not self._recent:
            return {
                **base,
                "estimate_method": None,
                "offset_estimate_ns": None,
                "offset_estimate_ms": None,
            }

        ordered = sorted(self._recent, key=lambda sample: int(sample["rtt_ns"]))
        if len(ordered) < 3:
            selected = ordered[:1]
            method = "minimum RTT sample"
        else:
            selected_count = max(3, math.ceil(len(ordered) * 0.20))
            selected = ordered[:selected_count]
            method = "median offset among the lowest-RTT 20% (minimum 3 samples)"

        selected_offsets = [int(sample["offset_ns"]) for sample in selected]
        estimate_ns = int(round(statistics.median(selected_offsets)))
        absolute_deviations = [abs(value - estimate_ns) for value in selected_offsets]
        mad_ns = int(round(statistics.median(absolute_deviations)))
        interval_lower_ns = max(
            int(sample["offset_ns"]) - int(sample["total_uncertainty_bound_ns"])
            for sample in selected
        )
        interval_upper_ns = min(
            int(sample["offset_ns"]) + int(sample["total_uncertainty_bound_ns"])
            for sample in selected
        )

        return {
            **base,
            "estimate_method": method,
            "offset_estimate_ns": estimate_ns,
            "offset_estimate_ms": _milliseconds(estimate_ns),
            "selected_low_rtt_samples": len(selected),
            "low_rtt_cutoff_ms": _milliseconds(int(selected[-1]["rtt_ns"])),
            "selected_offset_mad_ms": _milliseconds(mad_ns),
            "selected_offset_range_ms": {
                "min": _milliseconds(min(selected_offsets)),
                "max": _milliseconds(max(selected_offsets)),
            },
            "selected_uncertainty_intersection_ms": {
                "valid": interval_lower_ns <= interval_upper_ns,
                "lower": _milliseconds(interval_lower_ns),
                "upper": _milliseconds(interval_upper_ns),
            },
        }


@dataclass
class LocalClockContinuity:
    """Detect steps by comparing elapsed realtime with elapsed monotonic time."""

    step_threshold_ns: int = 5_000_000
    max_events: int = 32
    readings: int = 0
    suspected_discontinuities: int = 0
    elapsed_divergence: BoundedDistribution = field(default_factory=BoundedDistribution)
    capture_span: BoundedDistribution = field(default_factory=BoundedDistribution)
    _first: LocalClockReading | None = field(default=None, init=False, repr=False)
    _previous: LocalClockReading | None = field(default=None, init=False, repr=False)
    _maximum_absolute_divergence_ns: int = field(default=0, init=False, repr=False)
    _events: list[dict[str, object]] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.step_threshold_ns <= 0:
            raise ValueError("step_threshold_ns must be positive")
        if self.max_events < 0:
            raise ValueError("max_events cannot be negative")

    def add(self, reading: LocalClockReading) -> None:
        self.readings += 1
        self.capture_span.add(reading.capture_span_ns)
        if self._first is None:
            self._first = reading

        previous = self._previous
        if previous is not None:
            monotonic_elapsed_ns = reading.monotonic_ns - previous.monotonic_ns
            if monotonic_elapsed_ns < 0:
                raise ValueError("monotonic clock regressed")
            realtime_elapsed_ns = reading.realtime_ns - previous.realtime_ns
            divergence_ns = realtime_elapsed_ns - monotonic_elapsed_ns
            self.elapsed_divergence.add(divergence_ns)
            self._maximum_absolute_divergence_ns = max(
                self._maximum_absolute_divergence_ns,
                abs(divergence_ns),
            )
            if abs(divergence_ns) >= self.step_threshold_ns:
                self.suspected_discontinuities += 1
                if len(self._events) < self.max_events:
                    self._events.append(
                        {
                            "ts_realtime_ns": reading.realtime_ns,
                            "ts_monotonic_ns": reading.monotonic_ns,
                            "realtime_elapsed_ns": realtime_elapsed_ns,
                            "monotonic_elapsed_ns": monotonic_elapsed_ns,
                            "elapsed_divergence_ns": divergence_ns,
                            "elapsed_divergence_ms": _milliseconds(divergence_ns),
                        },
                    )
        self._previous = reading

    def to_dict(self) -> dict[str, object]:
        if self._first is None or self._previous is None:
            observed_span_seconds = 0.0
        else:
            observed_span_seconds = round(
                (self._previous.monotonic_ns - self._first.monotonic_ns) / 1_000_000_000,
                6,
            )
        return {
            "method": "CLOCK_REALTIME elapsed minus CLOCK_MONOTONIC elapsed",
            "readings": self.readings,
            "observed_span_seconds": observed_span_seconds,
            "step_threshold_ms": _milliseconds(self.step_threshold_ns),
            "suspected_discontinuities": self.suspected_discontinuities,
            "maximum_absolute_elapsed_divergence_ms": _milliseconds(
                self._maximum_absolute_divergence_ns,
            ),
            "elapsed_divergence": self.elapsed_divergence.as_milliseconds(),
            "clock_read_capture_span": self.capture_span.as_milliseconds(),
            "events": self._events,
        }
