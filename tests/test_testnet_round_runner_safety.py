import argparse
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from phoenix_testnet_round_runner import (
    Candidate,
    MAINNET_LIVE,
    TESTNET_LIVE,
    apply_trade_cost_attribution,
    build_parallel_dynamic_exit_simulation,
    build_round_config_snapshot,
    build_testnet_trade_telemetry_schema,
    build_testnet_learning_row,
    choose_experiment_for_runner_candidate,
    dynamic_concurrency_throttle,
    parse_args,
    run_trade,
    summarize_order_fills_many,
    validate_runtime_mode_args,
)


class FakeFutures:
    def __init__(self) -> None:
        self.margin_type_calls: list[tuple[str, str]] = []
        self.orders: list[dict[str, object]] = []

    async def change_margin_type(self, symbol: str, margin_type: str) -> dict[str, object]:
        self.margin_type_calls.append((symbol, margin_type))
        return {}

    async def change_initial_leverage(self, symbol: str, leverage: int) -> dict[str, object]:
        return {"symbol": symbol, "leverage": leverage}

    async def new_order(self, payload: dict[str, object]) -> dict[str, object]:
        self.orders.append(payload)
        return {
            "orderId": len(self.orders),
            "avgPrice": "100",
            "executedQty": str(payload.get("quantity", "0")),
        }

    async def user_trades(self, symbol: str, limit: int = 50) -> list[dict[str, object]]:
        return [
            {
                "orderId": 1,
                "qty": "1",
                "quoteQty": "100",
                "commission": "0.02",
                "commissionAsset": "USDT",
                "realizedPnl": "0",
                "maker": False,
                "time": 1_700_000_000_050,
            },
            {
                "orderId": 2,
                "qty": "1",
                "quoteQty": "100.5",
                "commission": "0.02",
                "commissionAsset": "USDT",
                "realizedPnl": "0.5",
                "maker": False,
                "time": 1_700_000_003_250,
            },
        ]


class FakeExecutor:
    async def get_symbol_rules(self, symbol: str) -> SimpleNamespace:
        return SimpleNamespace(min_notional=5.0, tick_size=0.1, step_size=0.001, min_qty=0.001)

    async def build_trade_intent(
        self,
        symbol: str,
        *,
        side: str,
        quote_allocation_usdt: float,
        leverage: int,
        available_balance_usdt: float,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            symbol=symbol,
            side=side,
            quote_allocation_usdt=quote_allocation_usdt,
            leverage=leverage,
            entry_price=100.0,
            quantity=1.0,
            initial_stop_price=99.0,
            take_profit_price=101.0,
            margin_type="ISOLATED",
        )


def make_candidate() -> Candidate:
    return Candidate(
        symbol="BTCUSDT",
        setup="plain_long",
        side="BUY",
        score=50.0,
        mark_price=100.0,
        quote_volume_24h=1_000_000_000.0,
        avg_quote_turnover_1m=1_000_000.0,
        current_quote_turnover_1m=1_000_000.0,
        price_change_24h_pct=1.0,
        ret_1m_pct=0.1,
        ret_3m_pct=0.1,
        ret_5m_pct=0.1,
        ret_15m_pct=0.1,
        ret_30m_pct=0.1,
        ret_60m_pct=0.1,
        volume_ratio=1.0,
        range_position=0.5,
        trend_gap_pct=0.1,
        higher_tf_trend_gap_pct=0.1,
        pullback_pct=0.0,
        bounce_pct=0.0,
        tf5_ret_3bar_pct=0.1,
        tf5_ret_6bar_pct=0.1,
        tf5_volume_ratio=1.0,
        tf5_range_position=0.5,
        tf5_trend_gap_pct=0.1,
        tf5_pullback_pct=0.0,
        tf15_ret_3bar_pct=0.1,
        tf15_ret_6bar_pct=0.1,
        tf15_volume_ratio=1.0,
        tf15_range_position=0.5,
        tf15_trend_gap_pct=0.1,
        tf15_pullback_pct=0.0,
    )


class TestnetRoundRunnerSafetyTests(unittest.TestCase):
    def test_parse_args_defaults_are_testnet_safe(self) -> None:
        args = parse_args([])

        self.assertEqual(args.runtime_mode, TESTNET_LIVE)
        self.assertEqual(args.env, "testnet")
        self.assertEqual(args.margin_type, "ISOLATED")
        self.assertEqual(args.max_open_positions, 10)
        self.assertFalse(args.run_forever)
        self.assertFalse(args.disable_learning_store)
        self.assertEqual(args.experiments_dir.name, "strategy_experiments")
        self.assertAlmostEqual(args.candidate_allocation_pct, 0.1)

    def test_runtime_mode_rejects_mainnet_live(self) -> None:
        args = argparse.Namespace(runtime_mode=MAINNET_LIVE, env="prod")

        with self.assertRaisesRegex(ValueError, "MAINNET_LIVE"):
            validate_runtime_mode_args(args)

    def test_throttle_caps_slots_by_max_open_positions_and_reject_rate(self) -> None:
        throttle = dynamic_concurrency_throttle(
            requested_slots=8,
            max_open_positions=3,
            current_open_positions=1,
            api_error_rate=0.0,
            order_reject_rate=0.25,
            base_cooldown_sec=1.0,
        )

        self.assertEqual(throttle["max_slots"], 1)
        self.assertGreaterEqual(throttle["cooldown_sec"], 8.0)
        self.assertIn("order_rejects", throttle["reasons"])

    def test_run_trade_passes_selected_margin_type_to_futures(self) -> None:
        async def fake_manage_open_trade(*_args: object, **_kwargs: object) -> dict[str, object]:
            return {
                "exit_reason": "time_stop",
                "close_order_id": 0,
                "close_order_ids": [],
                "close_avg_price": 100.0,
                "best_mark_price": 100.0,
                "worst_mark_price": 100.0,
                "hold_seconds": 1.0,
            }

        args = parse_args(["--margin-type", "CROSSED"])
        futures = FakeFutures()
        with patch("phoenix_testnet_round_runner.manage_open_trade", fake_manage_open_trade):
            trade = asyncio.run(
                run_trade(
                    futures,
                    FakeExecutor(),
                    candidate=make_candidate(),
                    available_balance=1_000.0,
                    args=args,
                    open_trade_state={},
                )
            )

        self.assertEqual(futures.margin_type_calls, [("BTCUSDT", "CROSSED")])
        self.assertEqual(trade["margin_type"], "CROSSED")
        self.assertEqual(trade["runtime_mode"], TESTNET_LIVE)

    def test_order_fill_summary_keeps_cost_role_and_time_metadata(self) -> None:
        summary = summarize_order_fills_many(
            [
                {
                    "orderId": 10,
                    "qty": "1",
                    "quoteQty": "100",
                    "commission": "0.02",
                    "commissionAsset": "USDT",
                    "realizedPnl": "0",
                    "maker": True,
                    "time": 1_700_000_000_100,
                },
                {
                    "orderId": 11,
                    "qty": "2",
                    "quoteQty": "201",
                    "commission": "0.04",
                    "commissionAsset": "USDT",
                    "realizedPnl": "1.2",
                    "maker": False,
                    "time": 1_700_000_000_900,
                },
            ],
            [10, 11],
        )

        self.assertAlmostEqual(summary["avg_price"], 100.333333, places=6)
        self.assertAlmostEqual(summary["commission"], 0.06, places=6)
        self.assertEqual(summary["maker_or_taker"], "mixed")
        self.assertEqual(summary["maker_fill_count"], 1)
        self.assertEqual(summary["taker_fill_count"], 1)
        self.assertEqual(summary["first_time_ms"], 1_700_000_000_100)
        self.assertEqual(summary["last_time_ms"], 1_700_000_000_900)
        self.assertEqual(summary["commission_asset"], "USDT")

    def test_trade_telemetry_schema_lists_required_execution_cost_fields(self) -> None:
        schema = build_testnet_trade_telemetry_schema()

        self.assertEqual(schema["schema_version"], "testnet_trade_telemetry_v1")
        for field in (
            "maker_or_taker",
            "fee_bps_effective",
            "exchange_fee",
            "realized_slippage_proxy",
            "funding_paid_or_received",
            "mfe",
            "mae",
            "runtime_mode_resolved",
            "credentials_environment_resolved",
            "round_config_snapshot_id",
        ):
            self.assertIn(field, schema["fields"])

    def test_round_config_snapshot_is_stable_and_secret_free(self) -> None:
        args = parse_args(["--runtime-mode", TESTNET_LIVE, "--env", "testnet", "--trades-per-round", "2"])
        settings = SimpleNamespace(
            quote_allocation_usdt=200.0,
            execution_mode="MANUAL_CONFIRM",
            max_open_positions=1,
            api_secret="must-not-leak",
        )

        first = build_round_config_snapshot(
            args,
            runtime_mode=TESTNET_LIVE,
            credentials_environment="testnet",
            settings=settings,
            round_no=7,
        )
        second = build_round_config_snapshot(
            args,
            runtime_mode=TESTNET_LIVE,
            credentials_environment="testnet",
            settings=settings,
            round_no=7,
        )

        self.assertEqual(first["round_config_snapshot_id"], second["round_config_snapshot_id"])
        encoded = str(first)
        self.assertNotIn("must-not-leak", encoded)
        self.assertEqual(first["runtime_mode_resolved"], TESTNET_LIVE)
        self.assertEqual(first["credentials_environment_resolved"], "testnet")

    def test_run_trade_emits_execution_cost_telemetry_without_changing_trade_flow(self) -> None:
        async def fake_manage_open_trade(*_args: object, **_kwargs: object) -> dict[str, object]:
            return {
                "exit_reason": "time_stop",
                "close_order_id": 2,
                "close_order_ids": [2],
                "close_avg_price": 100.5,
                "last_mark_price": 100.4,
                "best_mark_price": 101.0,
                "worst_mark_price": 99.5,
                "hold_seconds": 3.0,
                "exit_signal_timestamp": 1_700_000_003_000,
                "close_order_submit_timestamp": 1_700_000_003_010,
            }

        args = parse_args(["--env", "testnet"])
        snapshot = build_round_config_snapshot(
            args,
            runtime_mode=TESTNET_LIVE,
            credentials_environment="testnet",
            settings=SimpleNamespace(execution_mode="MANUAL_CONFIRM"),
            round_no=1,
        )
        futures = FakeFutures()
        with (
            patch("phoenix_testnet_round_runner.manage_open_trade", fake_manage_open_trade),
            patch("phoenix_testnet_round_runner.time.time", return_value=1_700_000_000.0),
        ):
            trade = asyncio.run(
                run_trade(
                    futures,
                    FakeExecutor(),
                    candidate=make_candidate(),
                    available_balance=1_000.0,
                    args=args,
                    open_trade_state={},
                    round_config_snapshot=snapshot,
                    entry_signal_timestamp=1_700_000_000_000,
                )
            )

        self.assertEqual([order["type"] for order in futures.orders], ["MARKET"])
        self.assertEqual(trade["runtime_mode_resolved"], TESTNET_LIVE)
        self.assertEqual(trade["credentials_environment_resolved"], "testnet")
        self.assertEqual(trade["execution_mode_resolved"], "TESTNET_LIVE_ORDER_SUBMIT")
        self.assertEqual(trade["round_config_snapshot_id"], snapshot["round_config_snapshot_id"])
        self.assertEqual(trade["maker_or_taker"], "taker")
        self.assertEqual(trade["entry_maker_or_taker"], "taker")
        self.assertEqual(trade["exit_maker_or_taker"], "taker")
        self.assertAlmostEqual(trade["exchange_fee"]["total_usdt"], 0.04, places=6)
        self.assertAlmostEqual(trade["fee_bps_effective"], 1.995, places=3)
        self.assertEqual(trade["actual_entry_timestamp"], 1_700_000_000_050)
        self.assertEqual(trade["entry_to_fill_latency_ms"], 50)
        self.assertEqual(trade["actual_exit_timestamp"], 1_700_000_003_250)
        self.assertEqual(trade["exit_to_fill_latency_ms"], 250)
        self.assertEqual(trade["mfe"], 1.0)
        self.assertEqual(trade["mae"], 0.5025)
        self.assertIn("realized_slippage_proxy", trade)
        self.assertFalse(trade["income_funding_event_linkage"]["available"])
        self.assertEqual(trade["funding_paid_or_received"], None)

    def test_learning_row_captures_testnet_trade_path_metrics(self) -> None:
        row = build_testnet_learning_row(
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "setup": "trend_pullback_long",
                "entry_order_id": 123,
                "entry_price": 100.0,
                "close_price": 100.4,
                "best_mark_price": 101.0,
                "worst_mark_price": 99.7,
                "net_pnl_usdt": 0.2,
                "commission_usdt": 0.01,
                "exit_reason": "time_stop",
                "latency_ms": 12.5,
                "estimated_slippage_bps": 2.0,
            }
        )

        self.assertEqual(row["mode"], "testnet")
        self.assertEqual(row["strategy_id"], "trend_pullback_long")
        self.assertEqual(row["mfe_pct"], 1.0)
        self.assertAlmostEqual(row["mae_pct"], 0.3009, places=4)
        self.assertEqual(row["profit_giveback_pct"], 0.6)
        self.assertEqual(row["loss_reason"], "not_a_loss")
        self.assertEqual(row["setup"], "trend_pullback_long")
        self.assertIn("not_a_loss", row["all_loss_reasons"])
        self.assertEqual(row["baseline_id"], "testnet_runner_v1:trend_pullback_long")

    def test_runner_selects_bounded_experiment_without_touching_live_flags(self) -> None:
        experiment = {
            "experiment_id": "exp_aiot",
            "strategy_id": "volatility_long",
            "version": "volatility_long_v2",
            "status": "running_experiment",
            "mode": "testnet_candidate",
            "setup": "plain_long",
            "symbol": "BTCUSDT",
            "candidate_allocation_pct": 1.0,
            "live_trading_enabled": False,
            "promotion_allowed": False,
            "direct_live_change_allowed": False,
        }

        selected = choose_experiment_for_runner_candidate(
            make_candidate(),
            [experiment],
            allocation_pct=1.0,
            seed="always-select",
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["experiment_id"], "exp_aiot")
        self.assertFalse(selected["live_trading_enabled"])
        self.assertFalse(selected["promotion_allowed"])

    def test_parallel_dynamic_exit_simulation_marks_improvement(self) -> None:
        simulation = build_parallel_dynamic_exit_simulation(
            candidate=make_candidate(),
            entry_price=100.0,
            management={
                "exit_reason": "time_stop",
                "close_avg_price": 99.9,
                "price_points": [
                    {"timestamp_ms": 0, "mark_price": 100.0},
                    {"timestamp_ms": 60_000, "mark_price": 100.9},
                    {"timestamp_ms": 120_000, "mark_price": 100.1},
                    {"timestamp_ms": 181_000, "mark_price": 99.9},
                ],
            },
            baseline_return_pct=-0.1,
        )

        self.assertIn("dynamic_exit_simulated_result", simulation)
        self.assertTrue(simulation["would_dynamic_exit_improve"])
        self.assertIn("baseline_exit_result", simulation)

    def test_apply_trade_cost_attribution_marks_take_profit_net_loss(self) -> None:
        trade = apply_trade_cost_attribution(
            {
                "symbol": "BTCUSDT",
                "setup": "micro_breakout_long",
                "side": "BUY",
                "exit_reason": "take_profit",
                "entry_price": 100.0,
                "intended_take_profit_price": 100.08,
                "close_price": 100.01,
                "realized_pnl_usdt": 0.01,
                "commission_usdt": 0.03,
                "net_pnl_usdt": -0.02,
                "estimated_slippage_bps": 5.0,
            }
        )

        self.assertEqual(trade["exit_reason"], "fee_slippage_tp_fail")
        self.assertEqual(trade["loss_reason"], "tp_net_loss")
        self.assertEqual(trade["gross_pnl_usdt"], 0.01)


if __name__ == "__main__":
    unittest.main()
