from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Mapping, Sequence

SourceTransport = Literal["connecting", "live", "backoff", "disabled"]
SourceUsability = Literal["valid", "stale", "gapped", "unsupported"]
CircuitBreakerState = Literal["closed", "open", "half_open"]
ErrorClass = Literal[
    "transport_error",
    "rate_limit",
    "auth_error",
    "negative_cache",
    "liquidity_error",
    "sequence_gap",
    "protocol_mismatch",
]


@dataclass
class SourceStatus:
    source_id: str
    transport: SourceTransport = "connecting"
    usability: SourceUsability = "unsupported"
    last_valid_data_ns: int | None = None
    last_error: str | None = None
    last_error_ns: int | None = None
    affected_routes: list[str] = field(default_factory=list)
    next_retry_ns: int | None = None
    consecutive_errors: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "transport": self.transport,
            "usability": self.usability,
            "last_valid_data_ns": self.last_valid_data_ns,
            "last_error": self.last_error,
            "last_error_ns": self.last_error_ns,
            "affected_routes": self.affected_routes,
            "next_retry_ns": self.next_retry_ns,
            "consecutive_errors": self.consecutive_errors,
        }


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout_ns: int = 30_000_000_000
    half_open_max_calls: int = 1


@dataclass
class RecoveryProfile:
    """Backoff profile: 30 → 60 → 120 → 240 → 300 seconds with jitter."""

    base_delay_ns: int = 30_000_000_000
    max_delay_ns: int = 300_000_000_000
    multiplier: float = 2.0
    jitter_fraction: float = 0.1


@dataclass
class AuthPolicy:
    stop_on_auth_error: bool = True
    cooldown_on_quota: bool = True


@dataclass
class SourceHealth:
    source_id: str
    status: SourceStatus
    circuit_breaker_state: CircuitBreakerState = "closed"
    is_healthy: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "status": self.status.as_dict(),
            "circuit_breaker_state": self.circuit_breaker_state,
            "is_healthy": self.is_healthy,
        }
