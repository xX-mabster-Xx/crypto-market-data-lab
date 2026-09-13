"""Small, bounded statistics helpers for live market-data recording."""

from __future__ import annotations

import random
from dataclasses import dataclass, field


def _percentile(sorted_values: list[int], percentile: float) -> int | None:
    if not sorted_values:
        return None
    index = round((len(sorted_values) - 1) * percentile)
    return sorted_values[index]


def _milliseconds(value: int | float | None) -> float | None:
    if value is None:
        return None
    return round(value / 1_000_000, 6)


@dataclass
class BoundedDistribution:
    """Streaming summary with a deterministic fixed-size reservoir."""

    capacity: int = 8_192
    count: int = 0
    total: int = 0
    minimum: int | None = None
    maximum: int | None = None
    _samples: list[int] = field(default_factory=list, repr=False)
    _random: random.Random = field(default_factory=lambda: random.Random(0), repr=False)

    def add(self, value: int) -> None:
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)

        if len(self._samples) < self.capacity:
            self._samples.append(value)
            return
        replacement = self._random.randrange(self.count)
        if replacement < self.capacity:
            self._samples[replacement] = value

    def as_milliseconds(self) -> dict[str, int | float | None]:
        samples = sorted(self._samples)
        mean = self.total / self.count if self.count else None
        return {
            "count": self.count,
            "sample_count": len(samples),
            "min_ms": _milliseconds(self.minimum),
            "mean_ms": _milliseconds(mean),
            "p50_ms": _milliseconds(_percentile(samples, 0.50)),
            "p95_ms": _milliseconds(_percentile(samples, 0.95)),
            "p99_ms": _milliseconds(_percentile(samples, 0.99)),
            "max_ms": _milliseconds(self.maximum),
        }


@dataclass
class StreamMetric:
    messages: int = 0
    records: int = 0
    first_event_ns: int | None = None
    last_event_ns: int | None = None
    first_init_ns: int | None = None
    last_init_ns: int | None = None
    max_interarrival_ns: int = 0
    negative_clock_deltas: int = 0
    clock_delta: BoundedDistribution = field(default_factory=BoundedDistribution)

    def add(self, *, ts_event: int, ts_init: int, records: int = 1) -> None:
        self.messages += 1
        self.records += records
        if self.first_event_ns is None:
            self.first_event_ns = ts_event
            self.first_init_ns = ts_init
        if self.last_init_ns is not None:
            self.max_interarrival_ns = max(self.max_interarrival_ns, ts_init - self.last_init_ns)
        self.last_event_ns = ts_event
        self.last_init_ns = ts_init

        delta = ts_init - ts_event
        if delta < 0:
            self.negative_clock_deltas += 1
        self.clock_delta.add(delta)

    def to_dict(self) -> dict[str, object]:
        return {
            "messages": self.messages,
            "records": self.records,
            "first_event_ns": self.first_event_ns,
            "last_event_ns": self.last_event_ns,
            "first_init_ns": self.first_init_ns,
            "last_init_ns": self.last_init_ns,
            "max_interarrival_ms": _milliseconds(self.max_interarrival_ns),
            "negative_clock_deltas": self.negative_clock_deltas,
            "observed_clock_delta": self.clock_delta.as_milliseconds(),
        }


@dataclass
class BookSequenceMetric:
    snapshots: int = 0
    update_id_duplicates: int = 0
    update_id_regressions: int = 0
    update_id_gap_events: int = 0
    missing_update_ids: int = 0
    cross_sequence_regressions: int = 0
    last_update_id: int | None = None
    last_cross_sequence: int | None = None

    def add(self, *, update_id: int | None, cross_sequence: int, snapshot: bool) -> None:
        if snapshot:
            self.snapshots += 1
        if self.last_cross_sequence is not None and cross_sequence < self.last_cross_sequence:
            self.cross_sequence_regressions += 1
        self.last_cross_sequence = cross_sequence

        if update_id is None:
            return
        if snapshot or self.last_update_id is None:
            self.last_update_id = update_id
            return
        if update_id == self.last_update_id:
            self.update_id_duplicates += 1
        elif update_id < self.last_update_id:
            self.update_id_regressions += 1
        elif update_id > self.last_update_id + 1:
            self.update_id_gap_events += 1
            self.missing_update_ids += update_id - self.last_update_id - 1
        self.last_update_id = update_id

    def to_dict(self) -> dict[str, int | None]:
        return {
            "snapshots": self.snapshots,
            "update_id_duplicates": self.update_id_duplicates,
            "update_id_regressions": self.update_id_regressions,
            "update_id_gap_events": self.update_id_gap_events,
            "missing_update_ids": self.missing_update_ids,
            "cross_sequence_regressions": self.cross_sequence_regressions,
            "last_update_id": self.last_update_id,
            "last_cross_sequence": self.last_cross_sequence,
        }

