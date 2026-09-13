"""Shared, read-only exact-quote broker contracts and orchestration.

The broker owns no provider connection.  Callers inject already configured
read-only backends, while this module supplies the cross-strategy behaviour:
exact cache keys, one in-flight request per key, monotonic deadlines, bounded
cache retention and shared quota-domain cooldowns.  It never builds, signs or
submits a transaction.

This is the local M2 core.  Existing polling adapters can feed results into
``observe_result``; later dynamic-size strategy verification can use
``get_quote`` without opening strategy-specific connections.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter, OrderedDict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Literal

from market_data_lab.amm_simulation.contracts import AmmPathRequest, AmmPathResult


QuoteMode = Literal["exact_in", "exact_out"]
QuoteStatus = Literal[
    "ok",
    "no_liquidity",
    "rate_limited",
    "invalid_auth",
    "unsupported",
    "exact_size_unavailable",
    "provider_error",
    "provider_budget_exhausted",
    "provider_budget_unconfigured",
    "verification_deadline_missed",
]
QuoteBackend = Callable[["QuoteRequest"], Awaitable["QuoteResult"]]


def _require_text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class QuoteKey:
    provider: str
    endpoint_generation: str
    quota_domain: str
    chain: str
    input_asset_id: str
    output_asset_id: str
    amount_raw: int
    mode: QuoteMode
    route_constraints_fingerprint: str
    fee_policy_fingerprint: str
    slippage_policy_fingerprint: str
    required_state_fingerprint: str
    minimum_state_quality: str
    context_fingerprint: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "provider",
            "endpoint_generation",
            "quota_domain",
            "chain",
            "input_asset_id",
            "output_asset_id",
            "route_constraints_fingerprint",
            "fee_policy_fingerprint",
            "slippage_policy_fingerprint",
            "required_state_fingerprint",
            "minimum_state_quality",
        ):
            _require_text(getattr(self, name), name)
        if self.amount_raw <= 0:
            raise ValueError("amount_raw must be positive")
        if self.mode not in {"exact_in", "exact_out"}:
            raise ValueError("mode must be exact_in or exact_out")
        if self.context_fingerprint is not None:
            _require_text(self.context_fingerprint, "context_fingerprint")


@dataclass(frozen=True, slots=True)
class QuoteRequest:
    request_id: str
    reason: str
    priority: str
    deadline_monotonic_ns: int
    key: QuoteKey
    dependent_candidate_ids: tuple[str, ...] = ()
    position_ids: tuple[str, ...] = ()
    request_weight: int = 1

    def __post_init__(self) -> None:
        for name in ("request_id", "reason", "priority"):
            _require_text(getattr(self, name), name)
        if self.deadline_monotonic_ns <= 0:
            raise ValueError("deadline_monotonic_ns must be positive")
        if self.request_weight <= 0:
            raise ValueError("request_weight must be positive")


@dataclass(frozen=True, slots=True)
class QuoteResult:
    request_id: str
    key: QuoteKey
    status: QuoteStatus
    reason: str | None
    requested_input_raw: int | None
    requested_output_raw: int | None
    actual_input_raw: int | None
    actual_output_raw: int | None
    expected_output_raw: int | None
    minimum_accepted_output_raw: int | None
    maximum_accepted_input_raw: int | None
    request_started_monotonic_ns: int
    response_received_monotonic_ns: int
    response_received_realtime_ns: int
    published_monotonic_ns: int
    ttl_ns: int
    exactness: str
    firmness: str
    consistency: str
    state_after_capability: str
    route_ids: tuple[str, ...] = ()
    pool_ids: tuple[str, ...] = ()
    fee_breakdown: tuple[Mapping[str, object], ...] = ()
    block_number: int | None = None
    slot: int | None = None
    retry_after_ns: int | None = None
    served_from: str = "remote"

    def __post_init__(self) -> None:
        _require_text(self.request_id, "request_id")
        for name in ("exactness", "firmness", "consistency", "state_after_capability"):
            _require_text(getattr(self, name), name)
        if self.status not in {
            "ok",
            "no_liquidity",
            "rate_limited",
            "invalid_auth",
            "unsupported",
            "exact_size_unavailable",
            "provider_error",
            "provider_budget_exhausted",
            "provider_budget_unconfigured",
            "verification_deadline_missed",
        }:
            raise ValueError("unsupported quote status")
        if (
            self.request_started_monotonic_ns <= 0
            or self.response_received_monotonic_ns <= 0
            or self.response_received_realtime_ns <= 0
            or self.published_monotonic_ns <= 0
        ):
            raise ValueError("quote timestamps must be positive")
        if self.response_received_monotonic_ns < self.request_started_monotonic_ns:
            raise ValueError("response cannot precede request start")
        if self.published_monotonic_ns < self.response_received_monotonic_ns:
            raise ValueError("publication cannot precede response receipt")
        if self.ttl_ns < 0:
            raise ValueError("ttl_ns must be non-negative")
        if self.retry_after_ns is not None and self.retry_after_ns <= 0:
            raise ValueError("retry_after_ns must be positive when present")
        for name in (
            "requested_input_raw",
            "requested_output_raw",
            "actual_input_raw",
            "actual_output_raw",
            "expected_output_raw",
            "minimum_accepted_output_raw",
            "maximum_accepted_input_raw",
            "block_number",
            "slot",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative when present")
        if self.status == "ok":
            if self.actual_input_raw is None or self.actual_output_raw is None:
                raise ValueError("successful quote needs actual raw input and output")
            if self.ttl_ns <= 0:
                raise ValueError("successful quote needs a positive TTL")
            if self.key.mode == "exact_in" and self.actual_input_raw != self.key.amount_raw:
                raise ValueError("exact-input result must consume the keyed raw input")
            if self.key.mode == "exact_out" and self.actual_output_raw != self.key.amount_raw:
                raise ValueError("exact-output result must deliver the keyed raw output")

    def fresh_at(
        self,
        now_monotonic_ns: int,
        now_realtime_ns: int | None = None,
    ) -> bool:
        monotonic_fresh = (
            self.status == "ok"
            and now_monotonic_ns >= self.response_received_monotonic_ns
            and now_monotonic_ns < self.response_received_monotonic_ns + self.ttl_ns
        )
        if not monotonic_fresh or now_realtime_ns is None:
            return monotonic_fresh
        # A wall-clock discontinuity or a long suspend must not make an old
        # response eligible merely because the process monotonic clock shows
        # a small age.  Disagreement is resolved conservatively as stale.
        return (
            now_realtime_ns >= self.response_received_realtime_ns
            and now_realtime_ns < self.response_received_realtime_ns + self.ttl_ns
        )

    def delivered(self, request_id: str, *, served_from: str, now_ns: int) -> "QuoteResult":
        return replace(
            self,
            request_id=request_id,
            served_from=served_from,
            # Publication is a separate time from response receipt.  Serving
            # from cache must never rewrite the receipt time or extend TTL.
            published_monotonic_ns=max(now_ns, self.response_received_monotonic_ns),
        )


@dataclass(frozen=True, slots=True)
class TwoSidedQuoteResult:
    """Exact-output buy plus exact-input sell for one native base quantity."""

    base_asset_id: str
    base_amount_raw: int
    buy: QuoteResult
    sell: QuoteResult
    compatible: bool
    consistency: str
    reason: str | None

    def __post_init__(self) -> None:
        _require_text(self.base_asset_id, "base_asset_id")
        if self.base_amount_raw <= 0:
            raise ValueError("base_amount_raw must be positive")
        _require_text(self.consistency, "consistency")


@dataclass(frozen=True, slots=True)
class QuoteBudgetPolicy:
    max_concurrency: int
    minimum_interval_ns: int = 0
    max_requests: int | None = None
    max_weight: int | None = None

    def __post_init__(self) -> None:
        if self.max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        if self.minimum_interval_ns < 0:
            raise ValueError("minimum_interval_ns must be non-negative")
        if self.max_requests is not None and self.max_requests <= 0:
            raise ValueError("max_requests must be positive when present")
        if self.max_weight is not None and self.max_weight <= 0:
            raise ValueError("max_weight must be positive when present")


@dataclass(slots=True)
class _BudgetState:
    active: int = 0
    active_weight: int = 0
    total_started: int = 0
    total_weight_started: int = 0
    last_started_ns: int | None = None
    cooldown_until_ns: int = 0
    denied: int = 0


class SharedQuoteBudgetManager:
    """One non-blocking budget ledger per real vendor/quota domain."""

    def __init__(self, policies: Mapping[str, QuoteBudgetPolicy]) -> None:
        if any(not isinstance(name, str) or not name for name in policies):
            raise ValueError("quota domain names must be non-empty")
        self._policies = dict(policies)
        self._states = {name: _BudgetState() for name in policies}

    def try_acquire(
        self,
        quota_domain: str,
        *,
        now_ns: int,
        deadline_ns: int,
        weight: int = 1,
    ) -> str | None:
        if weight <= 0:
            raise ValueError("request weight must be positive")
        policy = self._policies.get(quota_domain)
        state = self._states.get(quota_domain)
        if policy is None or state is None:
            return "provider_budget_unconfigured"
        next_interval_ns = (
            state.last_started_ns + policy.minimum_interval_ns
            if state.last_started_ns is not None
            else now_ns
        )
        next_allowed_ns = max(state.cooldown_until_ns, next_interval_ns)
        if next_allowed_ns > now_ns:
            state.denied += 1
            return (
                "verification_deadline_missed"
                if next_allowed_ns >= deadline_ns
                else "provider_budget_exhausted"
            )
        if state.active >= policy.max_concurrency:
            state.denied += 1
            return "provider_budget_exhausted"
        if policy.max_requests is not None and state.total_started >= policy.max_requests:
            state.denied += 1
            return "provider_budget_exhausted"
        if (
            policy.max_weight is not None
            and state.total_weight_started + weight > policy.max_weight
        ):
            state.denied += 1
            return "provider_budget_exhausted"
        state.active += 1
        state.active_weight += weight
        state.total_started += 1
        state.total_weight_started += weight
        state.last_started_ns = now_ns
        return None

    def preview_bundle(
        self,
        requests: Sequence[QuoteRequest],
        *,
        now_ns: int,
    ) -> str | None:
        """Conservatively check whether every uncached request can start.

        This does not reserve capacity.  It prevents an obviously incomplete
        two-sided bundle from spending its first remote call; each real start
        still goes through :meth:`try_acquire` so concurrent consumers cannot
        exceed the ledger.
        """

        grouped: dict[str, list[QuoteRequest]] = {}
        for request in requests:
            grouped.setdefault(request.key.quota_domain, []).append(request)
        for quota_domain, domain_requests in grouped.items():
            policy = self._policies.get(quota_domain)
            state = self._states.get(quota_domain)
            if policy is None or state is None:
                return "provider_budget_unconfigured"
            if state.active >= policy.max_concurrency:
                state.denied += 1
                return "provider_budget_exhausted"
            if (
                policy.max_requests is not None
                and state.total_started + len(domain_requests) > policy.max_requests
            ):
                state.denied += 1
                return "provider_budget_exhausted"
            total_weight = sum(request.request_weight for request in domain_requests)
            if (
                policy.max_weight is not None
                and state.total_weight_started + total_weight > policy.max_weight
            ):
                state.denied += 1
                return "provider_budget_exhausted"
            first_start_ns = max(
                now_ns,
                state.cooldown_until_ns,
                (
                    state.last_started_ns + policy.minimum_interval_ns
                    if state.last_started_ns is not None
                    else now_ns
                ),
            )
            for index, request in enumerate(
                sorted(domain_requests, key=lambda item: item.deadline_monotonic_ns),
            ):
                estimated_start_ns = first_start_ns + index * policy.minimum_interval_ns
                if estimated_start_ns >= request.deadline_monotonic_ns:
                    state.denied += 1
                    return "verification_deadline_missed"
        return None

    def release(self, quota_domain: str, *, weight: int = 1) -> None:
        if weight <= 0:
            raise ValueError("request weight must be positive")
        state = self._states.get(quota_domain)
        if state is None:
            return
        state.active = max(0, state.active - 1)
        state.active_weight = max(0, state.active_weight - weight)

    def set_cooldown(self, quota_domain: str, *, now_ns: int, duration_ns: int) -> None:
        if duration_ns <= 0:
            raise ValueError("cooldown duration must be positive")
        state = self._states.get(quota_domain)
        if state is not None:
            state.cooldown_until_ns = max(state.cooldown_until_ns, now_ns + duration_ns)

    def snapshot(self, *, now_ns: int) -> dict[str, object]:
        return {
            "domains": {
                name: {
                    "max_concurrency": self._policies[name].max_concurrency,
                    "minimum_interval_ns": self._policies[name].minimum_interval_ns,
                    "max_requests": self._policies[name].max_requests,
                    "max_weight": self._policies[name].max_weight,
                    "active": state.active,
                    "active_weight": state.active_weight,
                    "total_started": state.total_started,
                    "total_weight_started": state.total_weight_started,
                    "denied": state.denied,
                    "cooldown_remaining_ns": max(0, state.cooldown_until_ns - now_ns),
                }
                for name, state in sorted(self._states.items())
            }
        }


@dataclass(frozen=True, slots=True)
class QuoteFailurePolicy:
    """Bounded retry suppression with failure-specific scope.

    A liquidity miss belongs to one exact key (including amount and route
    constraints), a provider 5xx/transport storm belongs to one endpoint
    generation, and invalid credentials belong to the provider endpoint.
    Rate-limit cooldown remains owned by the real quota domain above.
    """

    no_liquidity_backoff_ns: int = 1_000_000_000
    exact_size_backoff_ns: int = 1_000_000_000
    unsupported_backoff_ns: int = 60_000_000_000
    provider_error_initial_backoff_ns: int = 250_000_000
    provider_error_max_backoff_ns: int = 15_000_000_000
    invalid_auth_backoff_ns: int = 60_000_000_000

    def __post_init__(self) -> None:
        values = (
            self.no_liquidity_backoff_ns,
            self.exact_size_backoff_ns,
            self.unsupported_backoff_ns,
            self.provider_error_initial_backoff_ns,
            self.provider_error_max_backoff_ns,
            self.invalid_auth_backoff_ns,
        )
        if any(value <= 0 for value in values):
            raise ValueError("quote failure backoffs must be positive")
        if self.provider_error_max_backoff_ns < self.provider_error_initial_backoff_ns:
            raise ValueError("provider-error maximum backoff cannot be below initial backoff")


@dataclass(slots=True)
class _FailureGate:
    status: QuoteStatus
    reason: str
    until_ns: int
    consecutive_failures: int = 1


class QuoteBroker:
    """Bounded exact-quote cache and in-flight request coordinator."""

    def __init__(
        self,
        *,
        backends: Mapping[str, QuoteBackend],
        budgets: SharedQuoteBudgetManager,
        failure_policy: QuoteFailurePolicy = QuoteFailurePolicy(),
        max_cache_items: int = 2_048,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        realtime_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        if max_cache_items <= 0:
            raise ValueError("max_cache_items must be positive")
        if any(not isinstance(name, str) or not name for name in backends):
            raise ValueError("backend provider names must be non-empty")
        self._backends = dict(backends)
        self._budgets = budgets
        self._failure_policy = failure_policy
        self.max_cache_items = max_cache_items
        self._monotonic_ns = monotonic_ns
        self._realtime_ns = realtime_ns
        self._cache: OrderedDict[QuoteKey, QuoteResult] = OrderedDict()
        self._key_failures: OrderedDict[QuoteKey, _FailureGate] = OrderedDict()
        self._endpoint_failures: OrderedDict[tuple[str, str], _FailureGate] = OrderedDict()
        self._auth_failures: OrderedDict[tuple[str, str], _FailureGate] = OrderedDict()
        self._inflight: dict[QuoteKey, asyncio.Task[QuoteResult]] = {}
        self._local_backends: dict[str, QuoteBackend] = {}
        self._local_inflight: dict[QuoteKey, asyncio.Task[QuoteResult]] = {}
        self._counts: Counter[str] = Counter()

    @staticmethod
    def _endpoint_key(key: QuoteKey) -> tuple[str, str]:
        # Endpoint failures and credential failures follow the real shared
        # vendor/account domain, not a market-specific Python provider name.
        return key.quota_domain, key.endpoint_generation

    def _bounded_gate_set(
        self,
        store: OrderedDict[object, _FailureGate],
        key: object,
        gate: _FailureGate,
    ) -> None:
        store[key] = gate
        store.move_to_end(key)
        while len(store) > self.max_cache_items:
            store.popitem(last=False)
            self._counts["failure_gate_evictions"] += 1

    def _record_failure(self, result: QuoteResult) -> None:
        now_ns = max(self._monotonic_ns(), result.response_received_monotonic_ns)
        endpoint_key = self._endpoint_key(result.key)
        if result.status == "rate_limited":
            if result.retry_after_ns is not None:
                self._budgets.set_cooldown(
                    result.key.quota_domain,
                    now_ns=now_ns,
                    duration_ns=result.retry_after_ns,
                )
            return
        if result.status == "provider_error":
            previous = self._endpoint_failures.get(endpoint_key)
            consecutive = (previous.consecutive_failures + 1) if previous is not None else 1
            exponent = min(consecutive - 1, 30)
            duration = min(
                self._failure_policy.provider_error_max_backoff_ns,
                self._failure_policy.provider_error_initial_backoff_ns * (2**exponent),
            )
            self._bounded_gate_set(
                self._endpoint_failures,
                endpoint_key,
                _FailureGate(
                    status="provider_error",
                    reason="provider_error_backoff_active",
                    until_ns=now_ns + duration,
                    consecutive_failures=consecutive,
                ),
            )
            return
        if result.status == "invalid_auth":
            previous = self._auth_failures.get(endpoint_key)
            consecutive = (previous.consecutive_failures + 1) if previous is not None else 1
            self._bounded_gate_set(
                self._auth_failures,
                endpoint_key,
                _FailureGate(
                    status="invalid_auth",
                    reason="invalid_auth_backoff_active",
                    until_ns=now_ns + self._failure_policy.invalid_auth_backoff_ns,
                    consecutive_failures=consecutive,
                ),
            )
            return
        key_backoffs = {
            "no_liquidity": self._failure_policy.no_liquidity_backoff_ns,
            "exact_size_unavailable": self._failure_policy.exact_size_backoff_ns,
            "unsupported": self._failure_policy.unsupported_backoff_ns,
        }
        duration = key_backoffs.get(result.status)
        if duration is not None:
            previous = self._key_failures.get(result.key)
            consecutive = (previous.consecutive_failures + 1) if previous is not None else 1
            self._bounded_gate_set(
                self._key_failures,
                result.key,
                _FailureGate(
                    status=result.status,
                    reason=f"{result.status}_backoff_active",
                    until_ns=now_ns + duration,
                    consecutive_failures=consecutive,
                ),
            )

    @staticmethod
    def _active_gate(
        store: OrderedDict[object, _FailureGate],
        key: object,
        *,
        now_ns: int,
    ) -> _FailureGate | None:
        gate = store.get(key)
        if gate is None or gate.until_ns <= now_ns:
            return None
        store.move_to_end(key)
        return gate

    def _blocked_by_failure(self, key: QuoteKey, *, now_ns: int) -> _FailureGate | None:
        endpoint_key = self._endpoint_key(key)
        # Invalid credentials are more specific evidence than a transient
        # endpoint failure, so report them first.
        return (
            self._active_gate(self._auth_failures, endpoint_key, now_ns=now_ns)
            or self._active_gate(self._endpoint_failures, endpoint_key, now_ns=now_ns)
            or self._active_gate(self._key_failures, key, now_ns=now_ns)
        )

    def observe_result(self, result: QuoteResult) -> bool:
        """Offer a normalized result from a shared polling/local adapter."""

        if result.status != "ok":
            self._counts[f"observed_{result.status}"] += 1
            self._record_failure(result)
            return False
        previous = self._cache.get(result.key)
        if (
            previous is not None
            and result.response_received_monotonic_ns
            <= previous.response_received_monotonic_ns
        ):
            self._counts["out_of_order_results_ignored"] += 1
            return False
        endpoint_key = self._endpoint_key(result.key)
        self._key_failures.pop(result.key, None)
        self._endpoint_failures.pop(endpoint_key, None)
        self._auth_failures.pop(endpoint_key, None)
        self._cache[result.key] = result
        self._cache.move_to_end(result.key)
        while len(self._cache) > self.max_cache_items:
            self._cache.popitem(last=False)
            self._counts["cache_evictions"] += 1
        self._counts["results_cached"] += 1
        return True

    def _failure(self, request: QuoteRequest, status: QuoteStatus, reason: str) -> QuoteResult:
        now_mono = max(1, self._monotonic_ns())
        now_real = max(1, self._realtime_ns())
        return QuoteResult(
            request_id=request.request_id,
            key=request.key,
            status=status,
            reason=reason,
            requested_input_raw=(request.key.amount_raw if request.key.mode == "exact_in" else None),
            requested_output_raw=(request.key.amount_raw if request.key.mode == "exact_out" else None),
            actual_input_raw=None,
            actual_output_raw=None,
            expected_output_raw=None,
            minimum_accepted_output_raw=None,
            maximum_accepted_input_raw=None,
            request_started_monotonic_ns=now_mono,
            response_received_monotonic_ns=now_mono,
            response_received_realtime_ns=now_real,
            published_monotonic_ns=now_mono,
            ttl_ns=0,
            exactness="unknown",
            firmness="unknown",
            consistency="unknown",
            state_after_capability="unknown",
            served_from="broker",
        )

    async def _execute(self, request: QuoteRequest) -> QuoteResult:
        backend = self._backends.get(request.key.provider)
        if backend is None:
            return self._failure(request, "unsupported", "quote_provider_backend_unavailable")
        try:
            result = await backend(request)
            if result.key != request.key:
                return self._failure(request, "provider_error", "quote_backend_key_mismatch")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result = self._failure(
                request,
                "provider_error",
                f"{type(exc).__name__}: {exc}"[:512],
            )
        finally:
            self._budgets.release(
                request.key.quota_domain,
                weight=request.request_weight,
            )
        self.observe_result(result)
        return result

    def _remove_inflight(self, key: QuoteKey, task: asyncio.Task[QuoteResult]) -> None:
        if self._inflight.get(key) is task:
            self._inflight.pop(key, None)

    async def _deliver(
        self,
        task: asyncio.Task[QuoteResult],
        request: QuoteRequest,
        *,
        served_from: str,
    ) -> QuoteResult:
        remaining_ns = request.deadline_monotonic_ns - self._monotonic_ns()
        if remaining_ns <= 0:
            self._counts["deadline_missed"] += 1
            return self._failure(
                request,
                "verification_deadline_missed",
                "verification_deadline_missed",
            )
        try:
            result = await asyncio.wait_for(
                asyncio.shield(task),
                timeout=remaining_ns / 1_000_000_000,
            )
        except TimeoutError:
            self._counts["deadline_missed"] += 1
            return self._failure(
                request,
                "verification_deadline_missed",
                "verification_deadline_missed",
            )
        if self._monotonic_ns() >= request.deadline_monotonic_ns:
            self._counts["deadline_missed"] += 1
            return self._failure(
                request,
                "verification_deadline_missed",
                "verification_deadline_missed",
            )
        return result.delivered(
            request.request_id,
            served_from=served_from,
            now_ns=self._monotonic_ns(),
        )

    async def get_quote(self, request: QuoteRequest) -> QuoteResult:
        now_ns = self._monotonic_ns()
        now_realtime_ns = self._realtime_ns()
        if request.deadline_monotonic_ns <= now_ns:
            self._counts["deadline_missed"] += 1
            return self._failure(
                request,
                "verification_deadline_missed",
                "verification_deadline_missed",
            )
        cached = self._cache.get(request.key)
        if cached is not None and cached.fresh_at(now_ns, now_realtime_ns):
            self._cache.move_to_end(request.key)
            self._counts["cache_hits"] += 1
            return cached.delivered(
                request.request_id,
                served_from="cache",
                now_ns=now_ns,
            )
        if cached is not None:
            self._counts["cache_expired"] += 1

        existing = self._inflight.get(request.key)
        if existing is not None:
            self._counts["inflight_joins"] += 1
            return await self._deliver(existing, request, served_from="inflight_shared")

        if request.key.provider not in self._backends:
            self._counts["unsupported"] += 1
            return self._failure(
                request,
                "unsupported",
                "quote_provider_backend_unavailable",
            )

        failure_gate = self._blocked_by_failure(request.key, now_ns=now_ns)
        if failure_gate is not None:
            self._counts[f"blocked_{failure_gate.status}"] += 1
            return self._failure(
                request,
                failure_gate.status,
                failure_gate.reason,
            )

        denied = self._budgets.try_acquire(
            request.key.quota_domain,
            now_ns=now_ns,
            deadline_ns=request.deadline_monotonic_ns,
            weight=request.request_weight,
        )
        if denied is not None:
            self._counts[denied] += 1
            status: QuoteStatus = (
                "provider_budget_unconfigured"
                if denied == "provider_budget_unconfigured"
                else (
                    "verification_deadline_missed"
                    if denied == "verification_deadline_missed"
                    else "provider_budget_exhausted"
                )
            )
            return self._failure(request, status, denied)

        task = asyncio.create_task(self._execute(request))
        self._inflight[request.key] = task
        task.add_done_callback(
            lambda completed, key=request.key: self._remove_inflight(key, completed),
        )
        self._counts["remote_requests_started"] += 1
        return await self._deliver(task, request, served_from="remote")

    def peek_fresh(self, key: QuoteKey) -> QuoteResult | None:
        """Return reusable evidence without starting a backend call."""

        now_ns = self._monotonic_ns()
        result = self._cache.get(key)
        if result is None or not result.fresh_at(now_ns, self._realtime_ns()):
            self._counts["cache_peek_misses"] += 1
            return None
        self._cache.move_to_end(key)
        self._counts["cache_peek_hits"] += 1
        return result

    async def get_exact_input(self, request: QuoteRequest) -> QuoteResult:
        if request.key.mode != "exact_in":
            raise ValueError("get_exact_input requires an exact_in quote key")
        return await self.get_quote(request)

    async def get_exact_output(self, request: QuoteRequest) -> QuoteResult:
        if request.key.mode != "exact_out":
            raise ValueError("get_exact_output requires an exact_out quote key")
        return await self.get_quote(request)

    async def get_two_sided_for_quantity(
        self,
        *,
        buy_exact_output: QuoteRequest,
        sell_exact_input: QuoteRequest,
    ) -> TwoSidedQuoteResult:
        """Get a buy and sell simulation for one identical raw base quantity."""

        buy_key = buy_exact_output.key
        sell_key = sell_exact_input.key
        if buy_key.mode != "exact_out" or sell_key.mode != "exact_in":
            raise ValueError("two-sided quantity needs exact-output buy and exact-input sell")
        if (
            buy_key.chain != sell_key.chain
            or buy_key.output_asset_id != sell_key.input_asset_id
            or buy_key.input_asset_id != sell_key.output_asset_id
            or buy_key.amount_raw != sell_key.amount_raw
        ):
            raise ValueError("two-sided quote requests do not describe one reversible quantity")

        cached_buy = self.peek_fresh(buy_key)
        cached_sell = self.peek_fresh(sell_key)
        missing = tuple(
            request
            for request, cached in (
                (buy_exact_output, cached_buy),
                (sell_exact_input, cached_sell),
            )
            if cached is None
        )
        denied = self._budgets.preview_bundle(
            missing,
            now_ns=self._monotonic_ns(),
        )
        if denied is not None:
            status: QuoteStatus = (
                "provider_budget_unconfigured"
                if denied == "provider_budget_unconfigured"
                else (
                    "verification_deadline_missed"
                    if denied == "verification_deadline_missed"
                    else "provider_budget_exhausted"
                )
            )
            self._counts["two_sided_preflight_denied"] += 1
            return TwoSidedQuoteResult(
                base_asset_id=buy_key.output_asset_id,
                base_amount_raw=buy_key.amount_raw,
                buy=cached_buy or self._failure(buy_exact_output, status, denied),
                sell=cached_sell or self._failure(sell_exact_input, status, denied),
                compatible=False,
                consistency="unavailable",
                reason=denied,
            )

        buy = (
            cached_buy.delivered(
                buy_exact_output.request_id,
                served_from="cache",
                now_ns=self._monotonic_ns(),
            )
            if cached_buy is not None
            else await self.get_exact_output(buy_exact_output)
        )
        sell = (
            cached_sell.delivered(
                sell_exact_input.request_id,
                served_from="cache",
                now_ns=self._monotonic_ns(),
            )
            if cached_sell is not None
            else await self.get_exact_input(sell_exact_input)
        )
        if buy.status != "ok":
            reason = buy.reason or buy.status
            compatible = False
        elif sell.status != "ok":
            reason = sell.reason or sell.status
            compatible = False
        elif (
            buy.block_number is not None
            and sell.block_number is not None
            and buy.block_number != sell.block_number
        ):
            reason = "state_mismatch"
            compatible = False
        else:
            reason = None
            compatible = True
        consistency = (
            "same_block"
            if (
                compatible
                and buy.block_number is not None
                and buy.block_number == sell.block_number
            )
            else ("unpinned_or_cross_state" if compatible else "unavailable")
        )
        self._counts[
            "two_sided_compatible" if compatible else "two_sided_incompatible"
        ] += 1
        return TwoSidedQuoteResult(
            base_asset_id=buy_key.output_asset_id,
            base_amount_raw=buy_key.amount_raw,
            buy=buy,
            sell=sell,
            compatible=compatible,
            consistency=consistency,
            reason=reason,
        )

    async def revalue_position_exit(self, request: QuoteRequest) -> QuoteResult:
        if not request.position_ids:
            raise ValueError("revalue_position_exit requires at least one position_id")
        return await self.get_exact_input(request)

    async def estimate_local_path(self, request: QuoteRequest) -> QuoteResult:
        """Use a registered local backend through the same cache and budget path."""

        return await self._get_local_quote(request)

    async def simulate_local_path(self, request: QuoteRequest) -> QuoteResult:
        """Use a local after-state backend with explicit simulation accounting."""

        self._counts["simulation_requests"] += 1
        return await self._get_local_quote(request)

    async def simulate_amm_path(
        self,
        request: AmmPathRequest,
        *,
        deadline_monotonic_ns: int | None = None,
    ) -> AmmPathResult:
        """Execute a stateful exact AMM path simulation on an immutable snapshot."""
        from market_data_lab.amm_simulation.engine import PathSimulator

        simulator = PathSimulator(monotonic_ns=self._monotonic_ns)
        # An explicit call-site deadline is an immutable override.  ``replace``
        # keeps the caller's request object untouched and makes the semantics
        # deterministic even when it already carries a longer deadline.
        if deadline_monotonic_ns is not None:
            request = replace(request, deadline_monotonic_ns=deadline_monotonic_ns)
        result = simulator.simulate(request)
        return result

    def register_local_backend(self, provider: str, backend: QuoteBackend) -> None:
        if not isinstance(provider, str) or not provider:
            raise ValueError("local backend provider must be non-empty")
        self._local_backends[provider] = backend

    async def _get_local_quote(self, request: QuoteRequest) -> QuoteResult:
        now_ns = self._monotonic_ns()
        if request.deadline_monotonic_ns <= now_ns:
            self._counts["deadline_missed"] += 1
            return self._failure(
                request,
                "verification_deadline_missed",
                "verification_deadline_missed",
            )
        cached = self._cache.get(request.key)
        if cached is not None and cached.fresh_at(now_ns, self._realtime_ns()):
            self._cache.move_to_end(request.key)
            self._counts["simulation_cache_hits"] += 1
            return cached.delivered(
                request.request_id,
                served_from="local_cache",
                now_ns=now_ns,
            )
        existing = self._local_inflight.get(request.key)
        if existing is not None:
            self._counts["simulation_inflight_joins"] += 1
            return await self._deliver(existing, request, served_from="local_inflight_shared")
        backend = self._local_backends.get(request.key.provider)
        if backend is None:
            self._counts["simulation_unsupported"] += 1
            return self._failure(request, "unsupported", "local_simulation_backend_unavailable")
        task = asyncio.create_task(self._execute_local(request, backend))
        self._local_inflight[request.key] = task
        task.add_done_callback(
            lambda completed, key=request.key: self._remove_local_inflight(key, completed),
        )
        self._counts["simulation_requests_started"] += 1
        return await self._deliver(task, request, served_from="local_backend")

    async def _execute_local(self, request: QuoteRequest, backend: QuoteBackend) -> QuoteResult:
        try:
            result = await backend(request)
            if result.key != request.key:
                return self._failure(request, "provider_error", "local_backend_key_mismatch")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result = self._failure(
                request,
                "provider_error",
                f"{type(exc).__name__}: {exc}"[:512],
            )
        self.observe_result(result)
        return result

    def _remove_local_inflight(self, key: QuoteKey, task: asyncio.Task[QuoteResult]) -> None:
        if self._local_inflight.get(key) is task:
            self._local_inflight.pop(key, None)

    async def close(self) -> None:
        tasks = tuple(self._inflight.values())
        self._inflight.clear()
        local_tasks = tuple(self._local_inflight.values())
        self._local_inflight.clear()
        for task in tasks:
            task.cancel()
        for task in local_tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if local_tasks:
            await asyncio.gather(*local_tasks, return_exceptions=True)

    def snapshot(self) -> dict[str, object]:
        now_ns = self._monotonic_ns()
        now_realtime_ns = self._realtime_ns()
        return {
            "schema_version": 1,
            "mode": "read_only_shared_exact_quote_broker",
            "execution_enabled": False,
            "transactions_submitted": False,
            "remote_backend_providers": sorted(self._backends),
            "remote_backend_count": len(self._backends),
            "max_cache_items": self.max_cache_items,
            "cache_items": len(self._cache),
            "fresh_cache_items": sum(
                result.fresh_at(now_ns, now_realtime_ns)
                for result in self._cache.values()
            ),
            "failure_gates": {
                "key_scoped": len(self._key_failures),
                "endpoint_scoped": len(self._endpoint_failures),
                "auth_scoped": len(self._auth_failures),
                "active_key_scoped": sum(
                    gate.until_ns > now_ns for gate in self._key_failures.values()
                ),
                "active_endpoint_scoped": sum(
                    gate.until_ns > now_ns for gate in self._endpoint_failures.values()
                ),
                "active_auth_scoped": sum(
                    gate.until_ns > now_ns for gate in self._auth_failures.values()
                ),
            },
            "inflight_requests": len(self._inflight),
            "inflight_simulations": len(self._local_inflight),
            "counts": dict(sorted(self._counts.items())),
            "budgets": self._budgets.snapshot(now_ns=now_ns),
        }
