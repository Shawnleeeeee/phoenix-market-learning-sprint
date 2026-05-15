import unittest

from phoenix_learning_analyzer import build_learning_analysis_report
from phoenix_strategy_proposals import generate_strategy_proposals


class MechanicalLearningRepairTests(unittest.TestCase):
    def test_trade_attribution_classifies_tp_net_loss_and_fee_drag(self) -> None:
        from phoenix_trade_attribution import classify_execution_record

        attribution = classify_execution_record(
            {
                "symbol": "BTCUSDT",
                "setup": "micro_breakout_long",
                "side": "BUY",
                "exit_reason": "take_profit",
                "entry_price": 100.0,
                "intended_take_profit_price": 100.08,
                "close_price": 100.02,
                "actual_close_avg_price": 100.02,
                "realized_pnl_usdt": 0.01,
                "commission_usdt": 0.03,
                "net_pnl_usdt": -0.02,
                "estimated_slippage_bps": 3.0,
            }
        )

        self.assertEqual(attribution["loss_reason"], "tp_net_loss")
        self.assertIn("fee_drag_exit", attribution["secondary_loss_reasons"])
        self.assertEqual(attribution["is_loss"], True)
        self.assertNotIn("unknown", attribution["all_loss_reasons"])

    def test_learning_analyzer_reduces_unknown_with_shared_attribution(self) -> None:
        report = build_learning_analysis_report(
            trade_rows=[
                {
                    "symbol": "BTCUSDT",
                    "setup": "micro_breakout_long",
                    "side": "BUY",
                    "exit_reason": "take_profit",
                    "entry_price": 100.0,
                    "close_price": 100.02,
                    "realized_pnl_usdt": 0.01,
                    "commission_usdt": 0.03,
                    "net_pnl_usdt": -0.02,
                    "estimated_slippage_bps": 3.0,
                },
                {
                    "symbol": "ETHUSDT",
                    "setup": "trend_pullback_long",
                    "side": "BUY",
                    "exit_reason": "time_stop",
                    "entry_price": 100.0,
                    "close_price": 99.97,
                    "best_mark_price": 100.02,
                    "worst_mark_price": 99.7,
                    "net_pnl_usdt": -0.05,
                },
            ]
        )

        self.assertEqual(report["problem_counts"]["tp_net_loss"], 1)
        self.assertEqual(report["problem_counts"]["no_follow_through"], 1)
        self.assertNotIn("unknown", report["problem_counts"])
        self.assertEqual(report["records"][0]["loss_reason"], "tp_net_loss")

    def test_fee_aware_take_profit_raises_micro_targets_above_cost_floor(self) -> None:
        from phoenix_testnet_round_runner import compute_fee_aware_take_profit_pct

        result = compute_fee_aware_take_profit_pct(
            raw_take_pct=0.06,
            estimated_entry_slippage_bps=8.0,
            round_trip_fee_bps=8.0,
            slippage_buffer_bps=6.0,
            safety_buffer_bps=5.0,
        )

        self.assertGreaterEqual(result["effective_take_pct"], 0.27)
        self.assertFalse(result["raw_take_profit_fee_safe"])
        self.assertEqual(result["fee_aware_adjustment_reason"], "raised_to_cost_floor")

    def test_learning_gate_quarantines_known_bad_symbol_setup(self) -> None:
        from phoenix_learning_gate import LearningGateConfig, build_learning_gate_decision

        decision = build_learning_gate_decision(
            {
                "symbol": "AIOTUSDT",
                "setup": "volatility_long",
                "estimated_slippage_bps": 2.0,
            },
            recent_records=[],
            config=LearningGateConfig(),
        )

        self.assertEqual(decision["decision"], "block")
        self.assertEqual(decision["reason"], "quarantined_symbol_setup")
        self.assertFalse(decision["live_trading_enabled"])

    def test_learning_gate_blocks_recent_bad_combo_and_high_slippage(self) -> None:
        from phoenix_learning_gate import LearningGateConfig, build_learning_gate_decision

        records = [
            {"symbol": "TESTUSDT", "setup": "volatility_long", "net_pnl_usdt": -0.2, "estimated_slippage_bps": 30.0},
            {"symbol": "TESTUSDT", "setup": "volatility_long", "net_pnl_usdt": -0.1, "estimated_slippage_bps": 28.0},
            {"symbol": "TESTUSDT", "setup": "volatility_long", "net_pnl_usdt": -0.05, "estimated_slippage_bps": 26.0},
        ]
        decision = build_learning_gate_decision(
            {"symbol": "TESTUSDT", "setup": "volatility_long"},
            recent_records=records,
            config=LearningGateConfig(recent_trade_window=3, min_recent_trades=3),
        )

        self.assertEqual(decision["decision"], "block")
        self.assertIn(decision["reason"], {"rolling_bad_combo", "rolling_high_slippage"})
        self.assertLess(decision["score_multiplier"], 1.0)

    def test_proposals_skip_unknown_targets_and_generate_bad_combo_experiment(self) -> None:
        report = {
            "report_type": "learning_analysis_report",
            "generated_at": "2026-05-01T00:00:00Z",
            "input_counts": {"records": 4},
            "problem_counts": {"bad_symbol_setup_combo": 3},
            "problem_examples": {"bad_symbol_setup_combo": []},
            "by_symbol_setup": [
                {
                    "symbol": "UNKNOWN",
                    "setup": "unknown",
                    "sample_count": 3,
                    "loss_count": 3,
                    "avg_return": -1.0,
                    "avg_mfe_pct": 0.0,
                    "problem_counts": {"bad_symbol_setup_combo": 3},
                },
                {
                    "symbol": "XRPUSDT",
                    "setup": "trend_pullback_long",
                    "sample_count": 3,
                    "loss_count": 3,
                    "avg_return": -0.2,
                    "avg_mfe_pct": 0.02,
                    "problem_counts": {"bad_symbol_setup_combo": 3},
                },
            ],
        }

        proposals = generate_strategy_proposals(report)

        self.assertTrue(proposals)
        self.assertTrue(all(proposal.get("setup") != "unknown" for proposal in proposals if proposal.get("setup")))
        self.assertIn("bad_symbol_setup_combo", {proposal["problem_type"] for proposal in proposals})
        self.assertIn("XRPUSDT", {proposal.get("symbol") for proposal in proposals})


if __name__ == "__main__":
    unittest.main()
