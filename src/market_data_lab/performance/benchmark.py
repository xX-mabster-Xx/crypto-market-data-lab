from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Mapping, Sequence

from .contracts import BenchmarkProfile


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""

    profile: str
    total_events: int = 0
    duration_seconds: float = 0.0
    events_per_second: float = 0.0
    screen_p95_ms: float = 0.0
    screen_p99_ms: float = 0.0
    event_loop_lag_p99_ms: float = 0.0
    burst_recovery_seconds: float = 0.0
    integrity_losses: int = 0
    peak_ram_bytes: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "total_events": self.total_events,
            "duration_seconds": self.duration_seconds,
            "events_per_second": self.events_per_second,
            "screen_p95_ms": self.screen_p95_ms,
            "screen_p99_ms": self.screen_p99_ms,
            "event_loop_lag_p99_ms": self.event_loop_lag_p99_ms,
            "burst_recovery_seconds": self.burst_recovery_seconds,
            "integrity_losses": self.integrity_losses,
            "peak_ram_bytes": self.peak_ram_bytes,
        }


@dataclass
class BenchmarkHarness:
    """Benchmark harness for W1/W2 performance profiles."""

    profile: BenchmarkProfile = field(default_factory=BenchmarkProfile)

    def run_state_update_benchmark(
        self,
        update_count: int = 10000,
    ) -> BenchmarkResult:
        """Run state update benchmark."""
        screen_times: list[float] = []

        start = time.monotonic()
        for i in range(update_count):
            iter_start = time.monotonic()
            # Simulate state update processing
            _ = i * 2 + 1
            iter_end = time.monotonic()
            screen_times.append((iter_end - iter_start) * 1000)
        end = time.monotonic()

        duration = end - start
        screen_times.sort()
        p95_idx = int(len(screen_times) * 0.95)
        p99_idx = int(len(screen_times) * 0.99)

        return BenchmarkResult(
            profile=self.profile.name,
            total_events=update_count,
            duration_seconds=duration,
            events_per_second=update_count / duration if duration > 0 else 0,
            screen_p95_ms=screen_times[p95_idx] if screen_times else 0,
            screen_p99_ms=screen_times[p99_idx] if screen_times else 0,
        )

    def meets_slo(self, result: BenchmarkResult) -> tuple[bool, list[str]]:
        """Check if benchmark result meets SLO targets."""
        failures: list[str] = []

        if result.screen_p95_ms > self.profile.target_p95_screen_ms:
            failures.append(
                f"screen_p95_ms {result.screen_p95_ms:.2f} > {self.profile.target_p95_screen_ms}"
            )
        if result.screen_p99_ms > self.profile.target_p99_screen_ms:
            failures.append(
                f"screen_p99_ms {result.screen_p99_ms:.2f} > {self.profile.target_p99_screen_ms}"
            )
        if result.event_loop_lag_p99_ms > self.profile.target_event_loop_lag_p99_ms:
            failures.append(
                f"event_loop_lag_p99_ms {result.event_loop_lag_p99_ms:.2f} > {self.profile.target_event_loop_lag_p99_ms}"
            )
        if result.burst_recovery_seconds > self.profile.target_burst_recovery_seconds:
            failures.append(
                f"burst_recovery_seconds {result.burst_recovery_seconds:.2f} > {self.profile.target_burst_recovery_seconds}"
            )

        return len(failures) == 0, failures
