"""Provider quota budgets and domains.

Section 9.5: QTE-03 — shared quota domains, priority shares.
Section 18: T28–T29 — vendor cooldown, error-based backoff scopes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Mapping

from .contracts import QuoteRequest


@dataclass(frozen=True, slots=True)
class QuotaDomain:
    """A quota domain for a provider/endpoint."""

    name: str
    provider: str
    max_requests_per_window: int
    window_seconds: int
    cooldown_seconds: int = 0  # 429 cooldown
    auth_required: bool = False


@dataclass
class ProviderBudget:
    """Tracks usage against a quota domain."""

    quota_domain: QuotaDomain
    _used: int = 0
    _window_start_ns: int = 0
    _in_cooldown_until_ns: int = 0
    _error_backoff_ns: int = 0
    _consecutive_errors: int = 0

    def can_request(self, now_ns: int) -> bool:
        """Check if a request can be made."""
        import time
        # Check cooldown (429)
        if now_ns < self._in_cooldown_until_ns:
            return False

        # Check error backoff
        if now_ns < self._error_backoff_ns:
            return False

        # Check quota window
        if self._used >= self.quota_domain.max_requests_per_window:
            window_elapsed = now_ns - self._window_start_ns
            window_ns = self.quota_domain.window_seconds * 1_000_000_000
            if window_elapsed < window_ns:
                return False
            else:
                # Reset window
                self._used = 0
                self._window_start_ns = now_ns

        return True

    def record_request(self, now_ns: int) -> None:
        """Record a successful request."""
        if self._window_start_ns == 0:
            self._window_start_ns = now_ns
        self._used += 1
        self._consecutive_errors = 0

    def record_error(
        self,
        error_type: Literal["rate_limit", "transport", "auth", "liquidity"],
        now_ns: int,
    ) -> None:
        """Record an error and apply appropriate backoff scope."""
        self._consecutive_errors += 1

        if error_type == "rate_limit":
            # 429: vendor cooldown
            self._in_cooldown_until_ns = now_ns + self.quota_domain.cooldown_seconds * 1_000_000_000
        elif error_type == "auth":
            # Stop requests until config change
            self._in_cooldown_until_ns = now_ns + 3600 * 1_000_000_000  # 1 hour
        elif error_type == "liquidity":
            # Only blocks this size/route; shorter backoff
            self._error_backoff_ns = now_ns + 5_000_000_000  # 5 seconds
        elif error_type == "transport":
            # 5xx: exponential backoff
            import time
            delay = min(
                30 * (2 ** self._consecutive_errors) * 1_000_000_000,
                300 * 1_000_000_000,  # max 300s
            )
            self._error_backoff_ns = now_ns + delay

    def record_error_recovered(self) -> None:
        """Reset error count after valid data received."""
        self._consecutive_errors = 0
        self._error_backoff_ns = 0

    def remaining_in_window(self, now_ns: int) -> int:
        """Remaining requests in current window."""
        if self._used >= self.quota_domain.max_requests_per_window:
            window_elapsed = now_ns - self._window_start_ns
            window_ns = self.quota_domain.window_seconds * 1_000_000_000
            if window_elapsed < window_ns:
                return 0
            self._used = 0
            self._window_start_ns = now_ns
        return self.quota_domain.max_requests_per_window - self._used

    @property
    def usage_fraction(self) -> float:
        return self._used / self.quota_domain.max_requests_per_window


@dataclass
class BudgetManager:
    """Manages all quota domains and request scheduling.

    QTE-01: Strategies do not make direct provider calls.
    QTE-03: Shared quota domains ensure fair distribution.
    """

    _domains: dict[str, ProviderBudget] = field(default_factory=dict)
    _priority_shares: dict[str, float] = field(default_factory=lambda: {
        "exit": 0.30, "verification": 0.50, "exploration": 0.10, "maintenance": 0.10
    })

    def register_domain(self, domain: QuotaDomain) -> None:
        """Register a quota domain."""
        self._domains[domain.provider] = ProviderBudget(quota_domain=domain)

    def get_budget(self, provider: str) -> ProviderBudget | None:
        return self._domains.get(provider)

    def can_request(self, provider: str, now_ns: int) -> bool:
        budget = self._domains.get(provider)
        if budget is None:
            return True  # No budget = no limit
        return budget.can_request(now_ns)

    def record_request(self, provider: str, now_ns: int) -> None:
        budget = self._domains.get(provider)
        if budget:
            budget.record_request(now_ns)

    def record_error(
        self,
        provider: str,
        error_type: str,
        now_ns: int,
    ) -> None:
        """Record an error. Per OPS-01: cause determines backoff scope."""
        budget = self._domains.get(provider)
        if budget:
            budget.record_error(error_type, now_ns)

    def distribute_budget(
        self,
        total_requests: int,
        priority: str,
    ) -> int:
        """Distribute requests based on priority shares (QTE-03)."""
        share = self._priority_shares.get(priority, 0.0)
        return int(total_requests * share)

    def stats(self) -> dict[str, dict]:
        """Return stats for all domains."""
        result = {}
        for name, budget in self._domains.items():
            result[name] = {
                "used": budget._used,
                "max": budget.quota_domain.max_requests_per_window,
                "usage_fraction": budget.usage_fraction,
                "in_cooldown": budget._in_cooldown_until_ns,
                "consecutive_errors": budget._consecutive_errors,
            }
        return result
