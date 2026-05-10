import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_learning_analyzer import build_learning_analysis_report
from phoenix_strategy_proposals import generate_strategy_proposals
from phoenix_strategy_registry import register_experiment_candidate, shadow_safe_defaults


class MechanicalEvolutionTests(unittest.TestCase):
    def test_learning_analyzer_detects_take_profit_net_loss(self) -> None:
        report = build_learning_analysis_report(
            trade_rows=[
                {
                    "symbol": "BTCUSDT",
                    "setup": "volatility_long",
                    "side": "BUY",
                    "exit_reason": "take_profit",
                    "entry_price": 100.0,
                    "close_price": 100.05,
                    "best_mark_price": 100.08,
                    "worst_mark_price": 99.95,
                    "realized_pnl_usdt": 0.02,
                    "commission_usdt": 0.04,
                    "net_pnl_usdt": -0.02,
                }
            ]
        )

        self.assertEqual(report["problem_counts"]["take_profit_net_loss"], 1)
        self.assertFalse(report["policy"]["agent_enabled"])
        self.assertFalse(report["policy"]["direct_live_change_allowed"])

    def test_repeated_aiot_volatility_losses_generate_symbol_setup_proposal(self) -> None:
        rows = [
            {
                "symbol": "AIOTUSDT",
                "setup": "volatility_long",
                "side": "BUY",
                "exit_reason": "time_stop",
                "entry_price": 1.0,
                "close_price": 0.999,
                "best_mark_price": 1.0005,
                "worst_mark_price": 0.997,
                "net_pnl_usdt": -0.05,
            }
            for _ in range(3)
        ]
        report = build_learning_analysis_report(trade_rows=rows)
        proposals = generate_strategy_proposals(report)

        repeated = [
            proposal
            for proposal in proposals
            if proposal["problem_type"] == "repeated_symbol_setup_loss"
            and proposal["symbol"] == "AIOTUSDT"
            and proposal["setup"] == "volatility_long"
        ]
        self.assertEqual(len(repeated), 1)
        self.assertFalse(repeated[0]["agent_generated"])
        self.assertFalse(repeated[0]["direct_live_change_allowed"])

    def test_high_slippage_and_fee_drag_generate_safe_proposals(self) -> None:
        report = build_learning_analysis_report(
            trade_rows=[
                {
                    "symbol": "ETHUSDT",
                    "setup": "momentum_long",
                    "side": "BUY",
                    "entry_price": 100.0,
                    "close_price": 100.03,
                    "best_mark_price": 100.06,
                    "worst_mark_price": 99.98,
                    "realized_pnl_usdt": 0.03,
                    "commission_usdt": 0.05,
                    "net_pnl_usdt": -0.02,
                    "estimated_slippage_bps": 7.0,
                }
            ]
        )
        proposal_types = {proposal["problem_type"] for proposal in generate_strategy_proposals(report)}

        self.assertIn("cost_slippage_edge_destroyed", proposal_types)
        self.assertIn("high_slippage_failure", proposal_types)

    def test_experiment_generation_writes_registry_with_shadow_safe_defaults(self) -> None:
        from phoenix_strategy_experiments import generate_experiment_from_proposal

        proposal = {
            "proposal_id": "proposal_abc",
            "status": "auto_experiment_allowed",
            "target_strategy": "volatility_long",
            "setup": "volatility_long",
            "symbol": "AIOTUSDT",
            "problem_type": "repeated_symbol_setup_loss",
            "candidate_version": "volatility_long_v2_confirmed_entry",
            "proposed_change": {"score_threshold_increase": 6},
            "validation_plan": {"minimum_sample_count": 50},
        }
        experiment = generate_experiment_from_proposal(proposal)
        registry = register_experiment_candidate(shadow_safe_defaults(), experiment)

        self.assertEqual(experiment["status"], "running_experiment")
        self.assertEqual(experiment["mode"], "testnet_candidate")
        self.assertEqual(experiment["candidate_allocation_pct"], 0.1)
        self.assertFalse(experiment["live_trading_enabled"])
        self.assertFalse(experiment["promotion_allowed"])
        self.assertFalse(experiment["direct_live_change_allowed"])
        item = registry["strategies"][experiment["strategy_id"]]
        self.assertEqual(item["rollback_target"], experiment["rollback_target"])
        self.assertFalse(item["live_trading_enabled"])
        self.assertIn(experiment["experiment_id"], registry["experiments"])
        self.assertIn(experiment["version"], item["versions"])

    def test_experiment_evaluator_deduplicates_independent_events(self) -> None:
        from phoenix_strategy_experiments import evaluate_experiment_records

        experiment = {
            "experiment_id": "exp_1",
            "strategy_id": "volatility_long",
            "version": "volatility_long_v2",
            "parent_version": "testnet_runner_v1",
        }
        records = [
            {"source_event_id": "a", "strategy_id": "volatility_long", "strategy_version": "testnet_runner_v1", "net_pnl_usdt": 1.0},
            {"source_event_id": "b", "strategy_id": "volatility_long", "strategy_version": "testnet_runner_v1", "net_pnl_usdt": -0.5},
            {"source_event_id": "c", "experiment_id": "exp_1", "strategy_id": "volatility_long", "strategy_version": "volatility_long_v2", "net_pnl_usdt": 1.5},
            {"source_event_id": "c", "experiment_id": "exp_1", "strategy_id": "volatility_long", "strategy_version": "volatility_long_v2", "net_pnl_usdt": 1.5},
        ]

        result = evaluate_experiment_records(experiment, records)

        self.assertEqual(result["baseline"]["independent_event_count"], 2)
        self.assertEqual(result["candidate"]["independent_event_count"], 1)
        self.assertGreater(result["candidate"]["avg_net_pnl"], result["baseline"]["avg_net_pnl"])

    def test_promotion_gate_never_allows_mainnet_live(self) -> None:
        from phoenix_promotion_gate import evaluate_promotion_gate

        result = evaluate_promotion_gate(
            {
                "experiment_id": "exp_good",
                "strategy_id": "volatility_long",
                "version": "v2",
                "mode": "testnet_candidate",
                "evidence_level": 4,
                "candidate": {
                    "sample_count": 60,
                    "independent_event_count": 40,
                    "net_pnl": 12.0,
                    "avg_net_pnl": 0.2,
                    "after_cost_edge": 1.2,
                    "expected_net_edge_bps": 4.0,
                    "expected_gross_move_bps": 15.0,
                    "expected_total_cost_bps": 4.0,
                    "profit_factor": 1.5,
                    "max_drawdown": -2.0,
                    "max_symbol_concentration_pct": 25.0,
                    "parity_passed": True,
                    "fee_drag_ratio": 0.2,
                    "take_profit_net_loss_count": 0,
                    "monte_carlo_probability_total_return_negative": 0.2,
                },
                "baseline": {
                    "fee_drag_ratio": 0.4,
                    "take_profit_net_loss_count": 1,
                },
            }
        )

        self.assertEqual(result["status"], "manual_review_required")
        self.assertFalse(result["auto_promotion_allowed"])
        self.assertFalse(result["mainnet_live_promotion_allowed"])
        self.assertFalse(result["live_trading_enabled"])

    def test_learning_report_pipeline_generates_outputs(self) -> None:
        from phoenix_learning_report import run_learning_evolution_pipeline

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "learning_store.jsonl").write_text(
                json.dumps(
                    {
                        "source_event_id": "tp_loss",
                        "symbol": "BTCUSDT",
                        "strategy_id": "volatility_long",
                        "strategy_version": "testnet_runner_v1",
                        "side": "BUY",
                        "exit_reason": "take_profit",
                        "return_pct": 0.01,
                        "net_pnl_usdt": -0.01,
                        "realized_pnl_usdt": 0.01,
                        "commission_usdt": 0.02,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            summary = run_learning_evolution_pipeline(input_dir=root)

            self.assertTrue((root / "learning_analysis_report.json").exists())
            self.assertTrue((root / "strategy_registry.json").exists())
            self.assertTrue(list((root / "strategy_proposals").glob("*.json")))
            self.assertTrue(list((root / "strategy_experiments").glob("*.json")))
            self.assertTrue((root / "promotion_gate_learning_report.json").exists())
            self.assertGreaterEqual(summary["proposal_count"], 1)


if __name__ == "__main__":
    unittest.main()
