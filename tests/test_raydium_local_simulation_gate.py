from __future__ import annotations

import asyncio
import unittest

from market_data_lab.solana_realtime_scanner import RaydiumLocalQuoteStateSource


class _StubClient:
    def __init__(self) -> None:
        self.capture_calls = 0
        self.simulate_calls = 0
        self.evidence_calls = 0

    async def capture_snapshot(self, **kwargs: object) -> dict[str, object]:
        self.capture_calls += 1
        return {"status": "ok"}

    async def simulate_path(self, **kwargs: object) -> dict[str, object]:
        self.simulate_calls += 1
        self.last_simulate_kwargs = kwargs
        return {"status": "complete"}

    async def export_evidence(self, **kwargs: object) -> dict[str, object]:
        self.evidence_calls += 1
        return {"status": "ok"}


class RaydiumLocalSimulationGateTest(unittest.IsolatedAsyncioTestCase):
    def _source(self, *, enabled: bool = False) -> RaydiumLocalQuoteStateSource:
        return RaydiumLocalQuoteStateSource(
            pools=(),
            raydium_standard_pools=(),
            meteora_pools=(),
            orca_pools=(),
            http_url="https://rpc.example",
            ws_url="wss://rpc.example",
            timeout_seconds=5.0,
            tick_cache_max_age_ms=300_000,
            state_snapshot_refresh_interval_ms=15_000,
            rpc_http_min_request_interval_ms=200,
            amm_simulation_enabled=enabled,
            allowed_protocols=("raydium_cpmm",),
        )

    async def test_simulation_is_refused_when_disabled(self) -> None:
        source = self._source(enabled=False)
        source._client = _StubClient()  # type: ignore[attr-defined]
        for method in (
            source.capture_snapshot(request_id="sim", pool_ids=("pool-a",)),
            source.simulate_path(
                request_id="sim",
                snapshot_token="t",
                legs=(),
                initial_balances=({"asset_id": "asset-a", "amount_raw": "1"},),
            ),
            source.export_simulation_evidence(request_id="sim", snapshot_token="t"),
        ):
            with self.assertRaisesRegex(RuntimeError, "disabled"):
                await method

    async def test_simulation_runs_when_enabled(self) -> None:
        source = self._source(enabled=True)
        client = _StubClient()
        source._client = client  # type: ignore[attr-defined]
        self.assertEqual((await source.capture_snapshot(request_id="sim", pool_ids=("p",)))["status"], "ok")
        self.assertEqual((await source.simulate_path(
            request_id="sim",
            snapshot_token="t",
            legs=(),
            initial_balances=({"asset_id": "asset-a", "amount_raw": "1"},),
            deadline_monotonic_ns="9007199254740993123",
        ))["status"], "complete")
        self.assertEqual((await source.export_simulation_evidence(request_id="sim", snapshot_token="t"))["status"], "ok")
        self.assertEqual(client.capture_calls, 1)
        self.assertEqual(client.simulate_calls, 1)
        self.assertEqual(client.last_simulate_kwargs["initial_balances"], ({"asset_id": "asset-a", "amount_raw": "1"},))
        self.assertEqual(client.last_simulate_kwargs["deadline_monotonic_ns"], "9007199254740993123")
        self.assertEqual(client.evidence_calls, 1)

    def test_describe_includes_gate_fields(self) -> None:
        descriptor = self._source(enabled=True).describe()
        self.assertIs(descriptor["amm_simulation_enabled"], True)
        self.assertEqual(descriptor["amm_simulation_allowed_protocols"], ["raydium_cpmm"])


if __name__ == "__main__":
    unittest.main()
