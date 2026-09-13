from __future__ import annotations

import unittest

from market_data_lab.position_engine import (
    ExitPolicy,
    Leg,
    PortfolioMode,
    PositionEngine,
    PositionId,
    PositionIntent,
    PositionState,
    VirtualFill,
)
from market_data_lab.position_engine.scenarios import (
    scenario_both_legs_filled,
    scenario_cex_filled_dex_stale,
    scenario_dex_filled_perp_unavailable,
    scenario_exit_more_expensive,
    scenario_funding_event_missed,
    scenario_partial_fill,
    scenario_pool_route_changed,
    scenario_source_lost_during_hold,
    scenario_withdraw_borrow_temporarily_closed,
)


def _make_intent(
    position_id: PositionId,
    mode: PortfolioMode = "isolated_case",
    exit_policy: ExitPolicy | None = None,
) -> PositionIntent:
    return PositionIntent(
        position_id=position_id,
        scenario_kind="frozen_market",
        legs=(
            Leg(
                leg_id="leg-spot",
                kind="spot",
                direction="buy",
                input_asset_id="USDT",
                output_asset_id="SOL",
                requested_raw=100_000_000,
            ),
            Leg(
                leg_id="leg-perp",
                kind="perp",
                direction="sell",
                input_asset_id="SOL",
                output_asset_id="USDT",
                requested_raw=100_000_000,
            ),
        ),
        initial_balances=(("USDT", 100_000_000),),
        exit_policy=exit_policy or ExitPolicy(),
    )


def _fill(leg_id: str, filled_raw: int = 100_000_000) -> VirtualFill:
    return VirtualFill(
        leg_id=leg_id,
        requested_raw=100_000_000,
        filled_raw=filled_raw,
        price_raw=100_000_000,
        fee_raw=100_000,
        received_at_offset_ns=0,
    )


class PositionEngineStateMachineTest(unittest.TestCase):
    def test_propose_isolated_case(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        record = engine.propose(intent)
        self.assertEqual(record.position.state, "Proposed")

    def test_propose_portfolio_replay_reserves(self) -> None:
        engine = PositionEngine(mode="portfolio_replay")
        engine.set_portfolio_balance("USDT", 1_000_000_000)
        intent = _make_intent(PositionId("test", "pos-1"))
        record = engine.propose(intent)
        self.assertEqual(record.position.state, "Reserved")

    def test_reject_duplicate_position(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        with self.assertRaises(ValueError):
            engine.propose(intent)

    def test_open_leg_transitions_to_opening(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        position = engine.position(PositionId("test", "pos-1"))
        self.assertIn(position.state, {"Opening", "Hedged"})

    def test_all_legs_filled_transitions_to_hedged(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "Hedged")

    def test_partial_fill_transitions_correctly(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(
            PositionId("test", "pos-1"),
            "leg-spot",
            _fill("leg-spot", filled_raw=50_000_000),
        )
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "Partial")

    def test_mark_exit_transitions_to_closing(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        engine.mark_exit(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "Closing")

    def test_all_exits_filled_closes_position(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        engine.mark_exit(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.mark_exit(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "Closed")

    def test_invalid_transition_raises(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        with self.assertRaises(ValueError):
            engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))

    def test_unwind_from_hedged(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        engine.unwind(PositionId("test", "pos-1"), "test_unwind")
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "Closed")

    def test_data_impaired_and_restore(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        engine.mark_data_impaired(PositionId("test", "pos-1"), "source_lost")
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "DataImpaired")
        engine.restore_data(PositionId("test", "pos-1"))
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "Hedged")

    def test_apply_funding_only_in_hedged_or_partial(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.apply_funding(PositionId("test", "pos-1"), 1000)
        self.assertEqual(engine.position(PositionId("test", "pos-1")).accumulated_funding_raw, 0)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        engine.apply_funding(PositionId("test", "pos-1"), 1000)
        self.assertEqual(engine.position(PositionId("test", "pos-1")).accumulated_funding_raw, 1000)

    def test_exit_policy_max_holding_horizon(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(
            PositionId("test", "pos-1"),
            exit_policy=ExitPolicy(max_holding_horizon_ns=1000),
        )
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        reason = engine.evaluate_exit_policy(PositionId("test", "pos-1"))
        self.assertEqual(reason, "max_holding_horizon_exceeded")

    def test_record_tracks_scenario_log(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        record = engine.record(PositionId("test", "pos-1"))
        self.assertGreater(len(record.scenario_log), 0)


class PositionEngineScenariosTest(unittest.TestCase):
    def test_scenario_both_legs_filled(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        result = scenario_both_legs_filled(
            engine,
            PositionId("test", "pos-1"),
            {"leg-spot": _fill("leg-spot"), "leg-perp": _fill("leg-perp")},
        )
        self.assertTrue(result.success)
        self.assertEqual(result.final_state, "Hedged")

    def test_scenario_cex_filled_dex_stale(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        result = scenario_cex_filled_dex_stale(
            engine,
            PositionId("test", "pos-1"),
            _fill("leg-spot"),
        )
        self.assertIn(result.final_state, {"Opening", "Hedged"})

    def test_scenario_dex_filled_perp_unavailable(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        result = scenario_dex_filled_perp_unavailable(
            engine,
            PositionId("test", "pos-1"),
            _fill("leg-perp"),
        )
        self.assertIn(result.final_state, {"Opening", "Hedged"})

    def test_scenario_partial_fill(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        result = scenario_partial_fill(
            engine,
            PositionId("test", "pos-1"),
            _fill("leg-spot"),
            fill_ratio=0.5,
        )
        self.assertTrue(result.success)
        self.assertEqual(result.final_state, "Partial")

    def test_scenario_pool_route_changed(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        result = scenario_pool_route_changed(
            engine,
            PositionId("test", "pos-1"),
        )
        self.assertTrue(result.success)
        self.assertEqual(result.final_state, "Closed")

    def test_scenario_funding_event_missed(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        result = scenario_funding_event_missed(
            engine,
            PositionId("test", "pos-1"),
            expected_funding_raw=500,
        )
        self.assertTrue(result.success)
        self.assertEqual(result.final_state, "Hedged")

    def test_scenario_exit_more_expensive(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        result = scenario_exit_more_expensive(
            engine,
            PositionId("test", "pos-1"),
            {"leg-spot": _fill("leg-spot"), "leg-perp": _fill("leg-perp")},
        )
        self.assertTrue(result.success)
        self.assertEqual(result.final_state, "Closed")

    def test_scenario_withdraw_borrow_temporarily_closed(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        result = scenario_withdraw_borrow_temporarily_closed(
            engine,
            PositionId("test", "pos-1"),
        )
        self.assertTrue(result.success)
        self.assertEqual(result.final_state, "DataImpaired")

    def test_scenario_source_lost_during_hold(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        result = scenario_source_lost_during_hold(
            engine,
            PositionId("test", "pos-1"),
        )
        self.assertTrue(result.success)
        self.assertEqual(result.final_state, "Closed")


class PositionEnginePortfolioReplayTest(unittest.TestCase):
    def test_portfolio_replay_reserves_balances(self) -> None:
        engine = PositionEngine(mode="portfolio_replay")
        engine.set_portfolio_balance("USDT", 1_000_000_000)
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "Reserved")

    def test_portfolio_replay_releases_on_close(self) -> None:
        engine = PositionEngine(mode="portfolio_replay")
        engine.set_portfolio_balance("USDT", 1_000_000_000)
        intent = _make_intent(PositionId("test", "pos-1"))
        engine.propose(intent)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        engine.mark_exit(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.mark_exit(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        self.assertEqual(engine.position(PositionId("test", "pos-1")).state, "Closed")

    def test_active_and_terminal_positions(self) -> None:
        engine = PositionEngine(mode="isolated_case")
        intent1 = _make_intent(PositionId("test", "pos-1"))
        intent2 = _make_intent(PositionId("test", "pos-2"))
        engine.propose(intent1)
        engine.propose(intent2)
        self.assertEqual(len(engine.active_positions()), 2)
        engine.open_leg(PositionId("test", "pos-1"), "leg-spot", _fill("leg-spot"))
        engine.open_leg(PositionId("test", "pos-1"), "leg-perp", _fill("leg-perp"))
        engine.unwind(PositionId("test", "pos-1"), "manual_close")
        self.assertEqual(len(engine.active_positions()), 1)
        self.assertEqual(len(engine.terminal_positions()), 1)


if __name__ == "__main__":
    unittest.main()
