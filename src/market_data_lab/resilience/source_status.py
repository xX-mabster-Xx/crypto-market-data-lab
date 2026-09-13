from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Mapping, Sequence

from .contracts import (
    ErrorClass,
    SourceHealth,
    SourceStatus,
    SourceTransport,
    SourceUsability,
)


@dataclass
class SourceStatusTracker:
    """Track health status of multiple sources."""

    _sources: dict[str, SourceStatus] = field(default_factory=dict)
    _counts: dict[str, int] = field(default_factory=dict)

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    def register(self, source_id: str) -> None:
        if source_id not in self._sources:
            self._sources[source_id] = SourceStatus(source_id=source_id)

    def get(self, source_id: str) -> SourceStatus | None:
        return self._sources.get(source_id)

    def record_success(self, source_id: str) -> None:
        status = self._sources.get(source_id)
        if status is None:
            return
        status.transport = "live"
        status.usability = "valid"
        status.last_valid_data_ns = time.monotonic_ns()
        status.consecutive_errors = 0
        status.last_error = None
        self._counts["source_success"] = self._counts.get("source_success", 0) + 1

    def record_error(
        self,
        source_id: str,
        error_class: ErrorClass,
        message: str,
    ) -> None:
        status = self._sources.get(source_id)
        if status is None:
            return
        status.last_error = message
        status.last_error_ns = time.monotonic_ns()
        status.consecutive_errors += 1
        self._counts[f"error_{error_class}"] = self._counts.get(f"error_{error_class}", 0) + 1

        if error_class == "transport_error":
            status.transport = "backoff"
            status.usability = "stale"
        elif error_class == "rate_limit":
            status.transport = "backoff"
            status.usability = "stale"
        elif error_class == "auth_error":
            status.transport = "disabled"
            status.usability = "unsupported"
        elif error_class == "negative_cache":
            status.usability = "unsupported"
        elif error_class == "liquidity_error":
            status.usability = "gapped"
        elif error_class == "sequence_gap":
            status.usability = "stale"
        elif error_class == "protocol_mismatch":
            status.transport = "disabled"
            status.usability = "unsupported"

    def get_health(self, source_id: str) -> SourceHealth | None:
        status = self._sources.get(source_id)
        if status is None:
            return None
        is_healthy = status.transport == "live" and status.usability == "valid"
        return SourceHealth(
            source_id=source_id,
            status=status,
            is_healthy=is_healthy,
        )

    def get_all_healthy(self) -> list[str]:
        return [
            sid for sid, status in self._sources.items()
            if status.transport == "live" and status.usability == "valid"
        ]

    def get_all_unhealthy(self) -> list[str]:
        return [
            sid for sid, status in self._sources.items()
            if status.transport != "live" or status.usability != "valid"
        ]
