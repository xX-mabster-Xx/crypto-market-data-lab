from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Mapping

from .contracts import RecoveryProfile


@dataclass
class BackoffManager:
    """Manage exponential backoff with bounded jitter."""

    profile: RecoveryProfile = field(default_factory=RecoveryProfile)
    _attempts: dict[str, int] = field(default_factory=dict)
    _next_retry: dict[str, int] = field(default_factory=dict)

    def record_attempt(self, source_id: str) -> None:
        self._attempts[source_id] = self._attempts.get(source_id, 0) + 1

    def get_delay_ns(self, source_id: str) -> int:
        attempt = self._attempts.get(source_id, 0)
        delay = int(self.profile.base_delay_ns * (self.profile.multiplier ** attempt))
        return min(delay, self.profile.max_delay_ns)

    def get_next_retry_time(self, source_id: str) -> int:
        now = time.monotonic_ns()
        return now + self.get_delay_ns(source_id)

    def can_retry(self, source_id: str) -> bool:
        next_retry = self._next_retry.get(source_id, 0)
        return time.monotonic_ns() >= next_retry

    def schedule_retry(self, source_id: str) -> int:
        next_time = self.get_next_retry_time(source_id)
        self._next_retry[source_id] = next_time
        return next_time

    def reset(self, source_id: str) -> None:
        self._attempts.pop(source_id, None)
        self._next_retry.pop(source_id, None)

    def is_backoff_exhausted(self, source_id: str, max_attempts: int = 10) -> bool:
        return self._attempts.get(source_id, 0) >= max_attempts
