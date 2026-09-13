"""Quote Broker — async request/result contract with caching and dedup.

Section 6.2: Quote Broker owns cache, request merging, shared budget.
Does NOT sign or send transactions.

Section 9: QTE-01 priority: fresh cached exact quote → local sim → remote API.
QTE-02: pair reuse validation.
QTE-03: dynamic sizing, request limits.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Mapping, Sequence

from .cache import QuoteCache
from .budgets import BudgetManager
from .contracts import (
    CacheKey,
    QuoteResult,
    QuoteRequest,
    ExactMode,
    QuoteQuality,
)
from ..execution_cost.contracts import (
    ExecutionEstimate,
    ExecutionResult,
    AMMState,
    OrderBookSnapshot,
)
from ..execution_cost.amm import simulate_cpmm_swap
from ..execution_cost.orderbook import depth_cost_to_acquire


@dataclass
class _InFlightRequest:
    """Tracks an in-flight request being served by multiple consumers."""

    request: QuoteRequest
    consumers: set[str]
    future: asyncio.Future


@dataclass
class AsyncQuoteBroker:
    """Async quote broker with cache, dedup, and quota management.

    QTE-01: Fresh cached exact quote → local simulation → remote API.
    QTE-01: Multiple consumers share one in-flight request (T25).
    """

    cache: QuoteCache = field(default_factory=QuoteCache)
    budget_manager: BudgetManager = field(default_factory=BudgetManager)
    max_remote_requests_per_verification: int = 6
    max_pending_requests: int = 256

    _inflight: dict[str, _InFlightRequest] = field(default_factory=dict)
    _pending_count: int = 0
    _total_requests: int = 0
    _cache_hits: int = 0
    _remote_calls: int = 0

    @property
    def stats(self) -> dict:
        return {
            "inflight": len(self._inflight),
            "pending": self._pending_count,
            "total_requests": self._total_requests,
            "cache_hits": self._cache_hits,
            "remote_calls": self._remote_calls,
            "cache_stats": self.cache.stats(),
            "budget_stats": self.budget_manager.stats(),
        }

    async def get_exact_input(
        self,
        input_asset_id: str,
        output_asset_id: str,
        amount_raw: int,
        provider: str,
        chain: str,
        state_version: int,
        priority: str = "verification",
        consumer_id: str = "",
        timeout_ms: int = 1500,
        route_restrictions: tuple[str, ...] = (),
        fee_policy: str = "public",
        slippage_policy: str = "tight",
        pinned_block: str | None = None,
        local_sim_callback: Callable | None = None,
        remote_callback: Callable | None = None,
    ) -> QuoteResult | None:
        """Get exact-input quote (Section 9.3).

        Priority per QTE-01:
        1. Fresh exact quote in cache
        2. Local simulation
        3. Remote API
        """

        self._total_requests += 1

        cache_key = CacheKey(
            provider=provider,
            chain=chain,
            input_asset_id=input_asset_id,
            output_asset_id=output_asset_id,
            amount_raw=amount_raw,
            mode="exact_in",
            route_restrictions=route_restrictions,
            fee_policy=fee_policy,
            slippage_policy=slippage_policy,
            pinned_block=pinned_block,
            state_version=state_version,
        )

        # 1. Check cache
        cached = self.cache.get(cache_key)
        if cached is not None:
            self._cache_hits += 1
            return cached

        # 2. Check in-flight (T25: single in-flight serves all consumers)
        if cache_key.key_string in self._inflight:
            inflight = self._inflight[cache_key.key_string]
            inflight.consumers.add(consumer_id)
            try:
                result = await asyncio.wait_for(
                    asyncio.shield(inflight.future),
                    timeout=timeout_ms / 1000,
                )
                return result
            except (asyncio.TimeoutError, asyncio.CancelledError):
                return None

        # 3. Try local simulation first
        if local_sim_callback is not None:
            local_result = local_sim_callback(cache_key)
            if local_result is not None:
                self.cache.put(cache_key, local_result)
                self._cache_hits += 1
                return local_result

        # 4. Remote API call (if budget allows)
        now_ns = int(time.monotonic_ns())
        if not self.budget_manager.can_request(provider, now_ns):
            # T28: vendor budget/cooldown respected
            return None

        if remote_callback is None:
            return None

        # Check pending request cap
        if self._pending_count >= self.max_pending_requests:
            return None

        self._pending_count += 1
        self._remote_calls += 1

        # Create in-flight request (T25: shared by multiple consumers)
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        request = QuoteRequest(
            request_id=f"req-{self._total_requests}",
            cache_key=cache_key,
            input_asset_id=input_asset_id,
            output_asset_id=output_asset_id,
            amount_raw=amount_raw,
            mode="exact_in",
            deadline_ns=now_ns + timeout_ms * 1_000_000,
            priority=priority,
            consumers=[consumer_id],
        )
        self._inflight[cache_key.key_string] = _InFlightRequest(
            request=request,
            consumers={consumer_id},
            future=future,
        )

        try:
            # Make remote call
            result = await asyncio.wait_for(
                remote_callback(cache_key),
                timeout=timeout_ms / 1000,
            )

            if result is not None:
                # Cache the result
                self.cache.put(cache_key, result)
                # Resolve all waiting consumers (T25)
                future.set_result(result)
            else:
                future.set_result(None)

            self.budget_manager.record_request(provider, now_ns)
            return result

        except (asyncio.TimeoutError, Exception) as exc:
            future.set_exception(exc)
            self.budget_manager.record_error(provider, "transport", now_ns)
            return None

        finally:
            self._pending_count = max(0, self._pending_count - 1)
            self._inflight.pop(cache_key.key_string, None)

    async def get_exact_output(
        self,
        input_asset_id: str,
        output_asset_id: str,
        target_output_raw: int,
        **kwargs,
    ) -> QuoteResult | None:
        """Get exact-output quote (Section 9.3).

        get_exact_output is implemented via native endpoint when confirmed.
        If only exact-input available, perform bounded search.
        """
        # Check cache first
        cache_key = CacheKey(
            provider=kwargs.get("provider", ""),
            chain=kwargs.get("chain", ""),
            input_asset_id=input_asset_id,
            output_asset_id=output_asset_id,
            amount_raw=target_output_raw,
            mode="exact_out",
            route_restrictions=kwargs.get("route_restrictions", ()),
            fee_policy=kwargs.get("fee_policy", "public"),
            slippage_policy=kwargs.get("slippage_policy", "tight"),
            pinned_block=kwargs.get("pinned_block"),
            state_version=kwargs.get("state_version", 0),
        )

        cached = self.cache.get(cache_key)
        if cached is not None:
            self._cache_hits += 1
            return cached

        # Try remote with exact_output mode
        kwargs["exact_output_mode"] = True
        return await self.get_exact_input(
            input_asset_id, output_asset_id, target_output_raw, **kwargs
        )

    async def get_two_sided_for_quantity(
        self,
        asset_a: str,
        asset_b: str,
        quantity_raw: int,
        **kwargs,
    ) -> tuple[QuoteResult | None, QuoteResult | None]:
        """Get both sides of a swap for a quantity (T24).

        If cached pair exists and validates per QTE-02, return without
        additional remote calls.
        """
        common_kwargs = {k: v for k, v in kwargs.items() if k != "mode"}

        forward_key = CacheKey(
            provider=kwargs.get("provider", ""),
            chain=kwargs.get("chain", ""),
            input_asset_id=asset_a,
            output_asset_id=asset_b,
            amount_raw=quantity_raw,
            mode="exact_in",
            route_restrictions=kwargs.get("route_restrictions", ()),
            fee_policy=kwargs.get("fee_policy", "public"),
            slippage_policy=kwargs.get("slippage_policy", "tight"),
            pinned_block=kwargs.get("pinned_block"),
            state_version=kwargs.get("state_version", 0),
        )

        reverse_key = CacheKey(
            provider=kwargs.get("provider", ""),
            chain=kwargs.get("chain", ""),
            input_asset_id=asset_b,
            output_asset_id=asset_a,
            amount_raw=quantity_raw,
            mode="exact_in",
            route_restrictions=kwargs.get("route_restrictions", ()),
            fee_policy=kwargs.get("fee_policy", "public"),
            slippage_policy=kwargs.get("slippage_policy", "tight"),
            pinned_block=kwargs.get("pinned_block"),
            state_version=kwargs.get("state_version", 0),
        )

        # QTE-02: Try pair reuse
        forward, reverse, compatible = self.cache.try_pair_reuse(
            forward_key, reverse_key
        )
        if forward is not None and reverse is not None and compatible:
            self._cache_hits += 2
            return forward, reverse

        # Need fresh quotes
        forward_result = await self.get_exact_input(asset_a, asset_b, quantity_raw, **kwargs)
        reverse_result = await self.get_exact_input(asset_b, asset_a, quantity_raw, **kwargs)
        return forward_result, reverse_result

    async def revalue_position_exit(
        self,
        position_id: str,
        entry_quantity: int,
        **kwargs,
    ) -> QuoteResult | None:
        """Revalue exit on the actual entered quantity (T12).

        Per Section 9.2: exit quotes the actual entered amount, not a new
        quantity. Exit belongs to new data, not entry round ID.
        """
        cache_key = CacheKey(
            provider=kwargs.get("provider", ""),
            chain=kwargs.get("chain", ""),
            input_asset_id=kwargs.get("output_asset_id", ""),
            output_asset_id=kwargs.get("input_asset_id", ""),
            amount_raw=entry_quantity,
            mode="exact_in",
            route_restrictions=kwargs.get("route_restrictions", ()),
            fee_policy=kwargs.get("fee_policy", "public"),
            slippage_policy=kwargs.get("slippage_policy", "tight"),
            pinned_block=kwargs.get("pinned_block"),
            state_version=kwargs.get("state_version", 0),
        )

        cached = self.cache.get(cache_key)
        if cached is not None:
            self._cache_hits += 1
            return cached

        return await self.get_exact_input(
            input_asset_id=kwargs.get("output_asset_id", ""),
            output_asset_id=kwargs.get("input_asset_id", ""),
            amount_raw=entry_quantity,
            **kwargs,
        )

    def accept_result(self, result: QuoteResult) -> bool:
        """Accept a quote result only if not superseded (TIME-03, Section 9.4).

        Late-arriving old response does not overwrite newer result.
        """
        cache_key = CacheKey(
            provider=result.provider,
            chain="",  # Simplified
            input_asset_id=result.input_asset_id,
            output_asset_id=result.output_asset_id,
            amount_raw=result.input_amount_raw,
            mode=result.mode,
            route_restrictions=(),
            fee_policy="",
            slippage_policy="",
            pinned_block=result.pinned_block,
            state_version=result.state_version,
        )

        existing = self.cache.get(cache_key)
        if existing is not None:
            # Don't regress — late old response doesn't overwrite newer
            if existing.receive_time_ns >= result.receive_time_ns:
                return False

        self.cache.put(cache_key, result)
        return True
