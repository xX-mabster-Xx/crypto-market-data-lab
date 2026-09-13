from __future__ import annotations

import asyncio
import unittest

from market_data_lab.quote_broker import QuoteBroker
from market_data_lab.quote_broker import QuoteBudgetPolicy
from market_data_lab.quote_broker import QuoteFailurePolicy
from market_data_lab.quote_broker import QuoteKey
from market_data_lab.quote_broker import QuoteRequest
from market_data_lab.quote_broker import QuoteResult
from market_data_lab.quote_broker import SharedQuoteBudgetManager


class _Clock:
    def __init__(self) -> None:
        self.monotonic = 1_000_000_000
        self.realtime = 2_000_000_000

    def monotonic_ns(self) -> int:
        return self.monotonic

    def realtime_ns(self) -> int:
        return self.realtime

    def advance(self, nanoseconds: int) -> None:
        self.monotonic += nanoseconds
        self.realtime += nanoseconds


def _key(
    *,
    amount_raw: int = 100,
    provider: str = "TEST",
    quota_domain: str = "vendor:test",
    mode: str = "exact_in",
    input_asset_id: str = "test-chain:USDC",
    output_asset_id: str = "test-chain:BASE",
) -> QuoteKey:
    return QuoteKey(
        provider=provider,
        endpoint_generation="v1",
        quota_domain=quota_domain,
        chain="test-chain",
        input_asset_id=input_asset_id,
        output_asset_id=output_asset_id,
        amount_raw=amount_raw,
        mode=mode,  # type: ignore[arg-type]
        route_constraints_fingerprint="any-route",
        fee_policy_fingerprint="fees-included",
        slippage_policy_fingerprint="50bps",
        required_state_fingerprint="latest",
        minimum_state_quality="fresh",
    )


def _request(clock: _Clock, request_id: str, key: QuoteKey) -> QuoteRequest:
    return QuoteRequest(
        request_id=request_id,
        reason="candidate_verification",
        priority="candidate",
        deadline_monotonic_ns=clock.monotonic + 1_000_000_000,
        key=key,
        dependent_candidate_ids=(request_id,),
    )


def _ok_result(
    clock: _Clock,
    request: QuoteRequest,
    *,
    received_ns: int | None = None,
    ttl_ns: int = 100_000_000,
) -> QuoteResult:
    received = received_ns if received_ns is not None else clock.monotonic
    exact_input = request.key.mode == "exact_in"
    return QuoteResult(
        request_id=request.request_id,
        key=request.key,
        status="ok",
        reason=None,
        requested_input_raw=(request.key.amount_raw if exact_input else None),
        requested_output_raw=(request.key.amount_raw if not exact_input else None),
        actual_input_raw=(request.key.amount_raw if exact_input else request.key.amount_raw * 2),
        actual_output_raw=(request.key.amount_raw * 2 if exact_input else request.key.amount_raw),
        expected_output_raw=(request.key.amount_raw * 2 if exact_input else request.key.amount_raw),
        minimum_accepted_output_raw=request.key.amount_raw,
        maximum_accepted_input_raw=None,
        request_started_monotonic_ns=received,
        response_received_monotonic_ns=received,
        response_received_realtime_ns=clock.realtime,
        published_monotonic_ns=received,
        ttl_ns=ttl_ns,
        exactness=("exact_input" if exact_input else "exact_output"),
        firmness="indicative",
        consistency="unpinned",
        state_after_capability="unsupported",
        route_ids=("route:test",),
        pool_ids=("pool:test",),
    )


def _failure_result(
    clock: _Clock,
    request: QuoteRequest,
    status: str,
    *,
    reason: str = "fixture_failure",
    retry_after_ns: int | None = None,
) -> QuoteResult:
    return QuoteResult(
        request_id=request.request_id,
        key=request.key,
        status=status,  # type: ignore[arg-type]
        reason=reason,
        requested_input_raw=(request.key.amount_raw if request.key.mode == "exact_in" else None),
        requested_output_raw=(request.key.amount_raw if request.key.mode == "exact_out" else None),
        actual_input_raw=None,
        actual_output_raw=None,
        expected_output_raw=None,
        minimum_accepted_output_raw=None,
        maximum_accepted_input_raw=None,
        request_started_monotonic_ns=clock.monotonic,
        response_received_monotonic_ns=clock.monotonic,
        response_received_realtime_ns=clock.realtime,
        published_monotonic_ns=clock.monotonic,
        ttl_ns=0,
        exactness="unknown",
        firmness="unknown",
        consistency="unknown",
        state_after_capability="unknown",
        retry_after_ns=retry_after_ns,
    )


def _budgets(*domains: str) -> SharedQuoteBudgetManager:
    return SharedQuoteBudgetManager(
        {
            domain: QuoteBudgetPolicy(max_concurrency=1)
            for domain in domains
        },
    )


class QuoteBrokerTest(unittest.IsolatedAsyncioTestCase):
    async def test_t24_fresh_exact_cache_causes_zero_new_remote_calls(self) -> None:
        clock = _Clock()
        calls = 0

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=_budgets("vendor:test"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        key = _key()
        first = await broker.get_quote(_request(clock, "one", key))
        second = await broker.get_quote(_request(clock, "two", key))

        self.assertEqual(calls, 1)
        self.assertEqual(first.served_from, "remote")
        self.assertEqual(second.served_from, "cache")
        self.assertEqual(second.request_id, "two")
        self.assertEqual(second.response_received_monotonic_ns, clock.monotonic)

    async def test_t25_ten_consumers_share_one_inflight_request(self) -> None:
        clock = _Clock()
        calls = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=_budgets("vendor:test"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )

    async def test_simulation_backend_is_deduped_without_remote_quota(self) -> None:
        clock = _Clock()
        calls = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={},
            budgets=_budgets("vendor:test"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        broker.register_local_backend("LOCAL", backend)
        key = _key(provider="LOCAL")
        tasks = [
            asyncio.create_task(
                broker.simulate_local_path(_request(clock, f"local-{index}", key)),
            )
            for index in range(10)
        ]
        await started.wait()
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(*tasks)
        third = await broker.simulate_local_path(_request(clock, "local-cache", key))

        self.assertEqual(calls, 1)
        self.assertEqual(
            {result.served_from for result in results},
            {"local_backend", "local_inflight_shared"},
        )
        self.assertEqual(third.served_from, "local_cache")
        self.assertEqual(
            sum(result.served_from == "local_backend" for result in results),
            1,
        )

    async def test_t26_late_old_or_new_error_does_not_replace_success(self) -> None:
        clock = _Clock()
        key = _key()
        request = _request(clock, "seed", key)

        async def backend(unused: QuoteRequest) -> QuoteResult:
            raise AssertionError("fresh observed cache must avoid backend")

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=_budgets("vendor:test"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        newest = _ok_result(clock, request, received_ns=clock.monotonic)
        older = _ok_result(clock, request, received_ns=clock.monotonic - 1)
        self.assertTrue(broker.observe_result(newest))
        self.assertFalse(broker.observe_result(older))

        error = QuoteResult(
            request_id="error",
            key=key,
            status="provider_error",
            reason="timeout",
            requested_input_raw=key.amount_raw,
            requested_output_raw=None,
            actual_input_raw=None,
            actual_output_raw=None,
            expected_output_raw=None,
            minimum_accepted_output_raw=None,
            maximum_accepted_input_raw=None,
            request_started_monotonic_ns=clock.monotonic,
            response_received_monotonic_ns=clock.monotonic,
            response_received_realtime_ns=clock.realtime,
            published_monotonic_ns=clock.monotonic,
            ttl_ns=0,
            exactness="unknown",
            firmness="unknown",
            consistency="unknown",
            state_after_capability="unknown",
        )
        self.assertFalse(broker.observe_result(error))
        delivered = await broker.get_quote(_request(clock, "consumer", key))
        self.assertEqual(delivered.status, "ok")
        self.assertEqual(delivered.served_from, "cache")

    async def test_t28_rate_limit_cooldown_is_shared_by_quota_domain(self) -> None:
        clock = _Clock()
        vendor_calls = 0
        other_calls = 0

        async def limited_backend(request: QuoteRequest) -> QuoteResult:
            nonlocal vendor_calls
            vendor_calls += 1
            return _failure_result(
                clock,
                request,
                "rate_limited",
                reason="429",
                retry_after_ns=500_000_000,
            )

        async def other_backend(request: QuoteRequest) -> QuoteResult:
            nonlocal other_calls
            other_calls += 1
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={"TEST": limited_backend, "OTHER": other_backend},
            budgets=_budgets("vendor:test", "vendor:other"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        limited = await broker.get_quote(_request(clock, "one", _key()))
        blocked = await broker.get_quote(
            _request(clock, "two", _key(amount_raw=101)),
        )
        independent = await broker.get_quote(
            _request(
                clock,
                "three",
                _key(provider="OTHER", quota_domain="vendor:other"),
            ),
        )

        self.assertEqual(limited.status, "rate_limited")
        self.assertEqual(blocked.status, "provider_budget_exhausted")
        self.assertEqual(vendor_calls, 1)
        self.assertEqual(independent.status, "ok")
        self.assertEqual(other_calls, 1)

    async def test_no_liquidity_does_not_create_vendor_wide_cooldown(self) -> None:
        clock = _Clock()
        calls = 0

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            if calls == 1:
                return _failure_result(
                    clock,
                    request,
                    "no_liquidity",
                    reason="size_unavailable",
                )
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=_budgets("vendor:test"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        large = await broker.get_quote(_request(clock, "large", _key(amount_raw=1_000)))
        small = await broker.get_quote(_request(clock, "small", _key(amount_raw=10)))

        self.assertEqual(large.status, "no_liquidity")
        self.assertEqual(small.status, "ok")
        self.assertEqual(calls, 2)

    async def test_expired_cache_refetches_without_extending_old_receipt(self) -> None:
        clock = _Clock()
        calls = 0

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            return _ok_result(clock, request, ttl_ns=10)

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=_budgets("vendor:test"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        key = _key()
        first = await broker.get_quote(_request(clock, "one", key))
        clock.advance(11)
        second = await broker.get_quote(_request(clock, "two", key))

        self.assertEqual(calls, 2)
        self.assertGreater(
            second.response_received_monotonic_ns,
            first.response_received_monotonic_ns,
        )

    async def test_dual_clock_disagreement_invalidates_cache(self) -> None:
        clock = _Clock()
        calls = 0

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            return _ok_result(clock, request, ttl_ns=100)

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=_budgets("vendor:test"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        key = _key()
        await broker.get_quote(_request(clock, "one", key))
        # Model a suspend/clock discontinuity: process monotonic age remains
        # small while UTC has advanced beyond the response TTL.
        clock.monotonic += 10
        clock.realtime += 101
        await broker.get_quote(_request(clock, "two", key))

        self.assertEqual(calls, 2)
        self.assertEqual(broker.snapshot()["counts"]["cache_expired"], 1)

    async def test_cache_and_failure_gate_memory_are_bounded(self) -> None:
        clock = _Clock()

        async def backend(request: QuoteRequest) -> QuoteResult:
            if request.key.amount_raw % 2:
                return _failure_result(clock, request, "no_liquidity")
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=SharedQuoteBudgetManager(
                {"vendor:test": QuoteBudgetPolicy(max_concurrency=1, max_requests=20)},
            ),
            max_cache_items=2,
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        for amount in range(100, 106):
            await broker.get_quote(_request(clock, str(amount), _key(amount_raw=amount)))

        snapshot = broker.snapshot()
        self.assertEqual(snapshot["cache_items"], 2)
        self.assertLessEqual(snapshot["failure_gates"]["key_scoped"], 2)
        self.assertGreater(snapshot["counts"]["cache_evictions"], 0)

    async def test_t29_failure_backoff_scopes_are_distinct(self) -> None:
        clock = _Clock()
        calls: list[tuple[str, int]] = []

        async def backend(request: QuoteRequest) -> QuoteResult:
            calls.append((request.key.provider, request.key.amount_raw))
            if request.key.provider in {"FAILS", "FAILS_SIBLING"}:
                return _failure_result(clock, request, "provider_error", reason="HTTP 503")
            if request.key.provider in {"AUTH", "AUTH_SIBLING"}:
                return _failure_result(clock, request, "invalid_auth", reason="HTTP 401")
            if request.key.amount_raw == 1_000:
                return _failure_result(clock, request, "no_liquidity")
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={
                "TEST": backend,
                "FAILS": backend,
                "FAILS_SIBLING": backend,
                "AUTH": backend,
                "AUTH_SIBLING": backend,
                "OTHER": backend,
            },
            budgets=_budgets("vendor:test", "vendor:fails", "vendor:auth", "vendor:other"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )

        large_key = _key(amount_raw=1_000)
        self.assertEqual(
            (await broker.get_quote(_request(clock, "large", large_key))).status,
            "no_liquidity",
        )
        repeated_large = await broker.get_quote(_request(clock, "large-again", large_key))
        small = await broker.get_quote(_request(clock, "small", _key(amount_raw=10)))

        failed_key = _key(provider="FAILS", quota_domain="vendor:fails")
        await broker.get_quote(_request(clock, "fails", failed_key))
        failed_sibling = await broker.get_quote(
            _request(
                clock,
                "fails-sibling",
                _key(
                    amount_raw=101,
                    provider="FAILS_SIBLING",
                    quota_domain="vendor:fails",
                ),
            ),
        )

        auth_key = _key(provider="AUTH", quota_domain="vendor:auth")
        await broker.get_quote(_request(clock, "auth", auth_key))
        auth_sibling = await broker.get_quote(
            _request(
                clock,
                "auth-sibling",
                _key(
                    amount_raw=102,
                    provider="AUTH_SIBLING",
                    quota_domain="vendor:auth",
                ),
            ),
        )
        other = await broker.get_quote(
            _request(clock, "other", _key(provider="OTHER", quota_domain="vendor:other")),
        )

        self.assertEqual(repeated_large.reason, "no_liquidity_backoff_active")
        self.assertEqual(small.status, "ok")
        self.assertEqual(failed_sibling.reason, "provider_error_backoff_active")
        self.assertEqual(auth_sibling.reason, "invalid_auth_backoff_active")
        self.assertEqual(other.status, "ok")
        self.assertNotIn(("TEST", 1_000), calls[1:])
        self.assertNotIn(("FAILS_SIBLING", 101), calls)
        self.assertNotIn(("AUTH_SIBLING", 102), calls)

    async def test_t34_unchanged_failed_evaluation_does_not_feedback_loop(self) -> None:
        clock = _Clock()
        calls = 0

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            return _failure_result(clock, request, "no_liquidity")

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=_budgets("vendor:test"),
            failure_policy=QuoteFailurePolicy(no_liquidity_backoff_ns=10_000),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        key = _key()
        first = await broker.get_quote(_request(clock, "first", key))
        second = await broker.get_quote(_request(clock, "second", key))

        self.assertEqual(first.status, "no_liquidity")
        self.assertEqual(second.reason, "no_liquidity_backoff_active")
        self.assertEqual(calls, 1)

    async def test_two_sided_cached_quantity_uses_zero_remote_calls(self) -> None:
        clock = _Clock()
        calls = 0

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=_budgets("vendor:test"),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        buy_key = _key(
            mode="exact_out",
            input_asset_id="test-chain:USDC",
            output_asset_id="test-chain:BASE",
        )
        sell_key = _key(
            mode="exact_in",
            input_asset_id="test-chain:BASE",
            output_asset_id="test-chain:USDC",
        )
        buy_request = _request(clock, "buy", buy_key)
        sell_request = _request(clock, "sell", sell_key)
        self.assertTrue(broker.observe_result(_ok_result(clock, buy_request)))
        self.assertTrue(broker.observe_result(_ok_result(clock, sell_request)))

        pair = await broker.get_two_sided_for_quantity(
            buy_exact_output=buy_request,
            sell_exact_input=sell_request,
        )

        self.assertTrue(pair.compatible)
        self.assertEqual(pair.consistency, "unpinned_or_cross_state")
        self.assertEqual(calls, 0)

    async def test_two_sided_preflight_avoids_partial_bundle_spend(self) -> None:
        clock = _Clock()
        calls = 0

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            return _ok_result(clock, request)

        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=SharedQuoteBudgetManager(
                {"vendor:test": QuoteBudgetPolicy(max_concurrency=1, max_requests=1)},
            ),
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        pair = await broker.get_two_sided_for_quantity(
            buy_exact_output=_request(
                clock,
                "buy",
                _key(
                    mode="exact_out",
                    input_asset_id="test-chain:USDC",
                    output_asset_id="test-chain:BASE",
                ),
            ),
            sell_exact_input=_request(
                clock,
                "sell",
                _key(
                    mode="exact_in",
                    input_asset_id="test-chain:BASE",
                    output_asset_id="test-chain:USDC",
                ),
            ),
        )

        self.assertFalse(pair.compatible)
        self.assertEqual(pair.reason, "provider_budget_exhausted")
        self.assertEqual(calls, 0)

    async def test_weighted_budget_counts_real_call_cost(self) -> None:
        clock = _Clock()
        calls = 0

        async def backend(request: QuoteRequest) -> QuoteResult:
            nonlocal calls
            calls += 1
            return _ok_result(clock, request)

        budgets = SharedQuoteBudgetManager(
            {"vendor:test": QuoteBudgetPolicy(max_concurrency=1, max_weight=3)},
        )
        broker = QuoteBroker(
            backends={"TEST": backend},
            budgets=budgets,
            monotonic_ns=clock.monotonic_ns,
            realtime_ns=clock.realtime_ns,
        )
        heavy = QuoteRequest(
            request_id="heavy",
            reason="candidate_verification",
            priority="candidate",
            deadline_monotonic_ns=clock.monotonic + 1_000_000_000,
            key=_key(amount_raw=100),
            request_weight=2,
        )
        too_heavy = QuoteRequest(
            request_id="too-heavy",
            reason="candidate_verification",
            priority="candidate",
            deadline_monotonic_ns=clock.monotonic + 1_000_000_000,
            key=_key(amount_raw=101),
            request_weight=2,
        )
        self.assertEqual((await broker.get_quote(heavy)).status, "ok")
        self.assertEqual(
            (await broker.get_quote(too_heavy)).status,
            "provider_budget_exhausted",
        )
        domain = budgets.snapshot(now_ns=clock.monotonic)["domains"]["vendor:test"]
        self.assertEqual(calls, 1)
        self.assertEqual(domain["total_started"], 1)
        self.assertEqual(domain["total_weight_started"], 2)


if __name__ == "__main__":
    unittest.main()
