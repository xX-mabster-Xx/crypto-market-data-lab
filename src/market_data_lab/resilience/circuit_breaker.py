from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Mapping

from .contracts import (
    CircuitBreakerConfig,
    CircuitBreakerState,
    ErrorClass,
)


@dataclass
class CircuitBreaker:
    """Circuit breaker for source error handling."""

    config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    _state: CircuitBreakerState = "closed"
    _failure_count: int = 0
    _last_failure_ns: int = 0
    _half_open_calls: int = 0

    @property
    def state(self) -> CircuitBreakerState:
        if self._state == "open":
            now = time.monotonic_ns()
            if now - self._last_failure_ns >= self.config.recovery_timeout_ns:
                self._state = "half_open"
                self._half_open_calls = 0
        return self._state

    def record_success(self) -> None:
        current_state = self.state
        if current_state == "half_open":
            self._half_open_calls += 1
            if self._half_open_calls >= self.config.half_open_max_calls:
                self._state = "closed"
                self._failure_count = 0
        else:
            self._failure_count = 0

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_ns = time.monotonic_ns()
        if self._failure_count >= self.config.failure_threshold:
            self._state = "open"

    def can_execute(self) -> bool:
        current_state = self.state
        if current_state == "closed":
            return True
        if current_state == "half_open" and self._half_open_calls < self.config.half_open_max_calls:
            return True
        return False
