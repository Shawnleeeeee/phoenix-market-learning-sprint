from __future__ import annotations

import unittest

from phoenix_promotion_gate import evaluate_promotion_gate


def safe_candidate(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "sample_count": 80,
        "independent_event_count": 50,
        "net_pnl": 12.0,
        "after_cost_edge": 1.2,
        "expected_net_edge_bps": 4.0,
        "expected_gross_move_bps": 15.0,
        "expected_total_cost_bps": 4.0,
        "profit_factor": 1.4,
        "max_drawdown": -2.0,
        "max_symbol_concentration_pct": 25.0,
        "parity_passed": True,
        "fee_drag_ratio": 0.2,
        "take_profit_net_loss_count": 0,
        "monte_carlo_probability_total_return_negative": 0.2,
    }
    payload.update(overrides)
    return payload


class PromotionGateSafetyTests(unittest.TestCase):
    def test_avg_return_and_gross_pnl_cannot_promote_without_net_edge_fields(self) -> None:
        result = evaluate_promotion_gate(
            {
                "experiment_id": "exp_gross_only",
                "strategy_id": "gross_only",
                "mode": "testnet_candidate",
                "evidence_level": 4,
                "candidate": {
                    "sample_count": 100,
                    "independent_event_count": 80,
                    "avg_return_pct": 9.0,
                    "gross_pnl_usdt": 100.0,
                    "profit_factor": 2.0,
                    "max_drawdown": -1.0,
                    "parity_passed": True,
                    "max_symbol_concentration_pct": 20.0,
                },
            }
        )

        self.assertEqual(result["status"], "blocked")
        self.assertIn("missing_net_pnl", result["blockers"])
        self.assertIn("missing_expected_net_edge_bps", result["blockers"])
        self.assertFalse(result["auto_promotion_allowed"])

    def test_positive_avg_gross_and_backtest_cannot_promote_when_net_pnl_is_negative(self) -> None:
        result = evaluate_promotion_gate(
            {
                "experiment_id": "exp_positive_gross_negative_net",
                "strategy_id": "gross_positive_net_negative",
                "mode": "testnet_candidate",
                "evidence_level": 4,
                "candidate": {
                    "sample_count": 100,
                    "independent_event_count": 80,
                    "avg_return": 0.08,
                    "avg_return_pct": 8.0,
                    "gross_pnl": 250.0,
                    "gross_pnl_usdt": 250.0,
                    "backtest_pnl": 500.0,
                    "net_pnl": -1.0,
                    "after_cost_edge": 1.0,
                    "expected_net_edge_bps": 2.0,
                    "expected_gross_move_bps": 9.0,
                    "expected_total_cost_bps": 3.0,
                    "profit_factor": 1.5,
                    "max_drawdown": -2.0,
                    "parity_passed": True,
                    "max_symbol_concentration_pct": 20.0,
                    "fee_drag_ratio": 0.1,
                    "take_profit_net_loss_count": 0,
                },
                "baseline": {"fee_drag_ratio": 0.1, "take_profit_net_loss_count": 0},
            }
        )

        self.assertEqual(result["status"], "blocked")
        self.assertIn("net_pnl_not_positive", result["blockers"])
        self.assertNotEqual(result["status"], "promoted_testnet_candidate")
        self.assertFalse(result["auto_promotion_allowed"])
        self.assertFalse(result["mainnet_live_promotion_allowed"])

    def test_missing_expected_net_edge_blocks_even_with_positive_net_pnl(self) -> None:
        candidate = safe_candidate()
        candidate.pop("expected_net_edge_bps")
        result = evaluate_promotion_gate(
            {
                "experiment_id": "exp_missing_edge",
                "strategy_id": "missing_edge",
                "mode": "testnet_candidate",
                "evidence_level": 4,
                "candidate": candidate,
            }
        )

        self.assertEqual(result["status"], "blocked")
        self.assertIn("missing_expected_net_edge_bps", result["blockers"])

    def test_level_zero_one_two_can_never_pass_for_testnet(self) -> None:
        for level in (0, 1, 2):
            result = evaluate_promotion_gate(
                {
                    "experiment_id": f"exp_level_{level}",
                    "strategy_id": "low_level",
                    "mode": "testnet_candidate",
                    "evidence_level": level,
                    "candidate": safe_candidate(),
                }
            )
            self.assertEqual(result["status"], "blocked")
            self.assertIn("evidence_level_too_low_for_testnet", result["blockers"])

    def test_all_required_fields_only_create_manual_review_not_auto_promotion(self) -> None:
        result = evaluate_promotion_gate(
            {
                "experiment_id": "exp_ready",
                "strategy_id": "ready",
                "mode": "testnet_candidate",
                "evidence_level": 4,
                "candidate": safe_candidate(),
            }
        )

        self.assertEqual(result["status"], "manual_review_required")
        self.assertFalse(result["auto_promotion_allowed"])
        self.assertFalse(result["mainnet_live_promotion_allowed"])
        self.assertFalse(result["live_trading_enabled"])

    def test_parity_and_symbol_concentration_are_required(self) -> None:
        parity_failed = evaluate_promotion_gate(
            {
                "experiment_id": "exp_parity",
                "strategy_id": "parity",
                "mode": "testnet_candidate",
                "evidence_level": 4,
                "candidate": safe_candidate(parity_passed=False),
            }
        )
        concentrated = evaluate_promotion_gate(
            {
                "experiment_id": "exp_concentration",
                "strategy_id": "concentration",
                "mode": "testnet_candidate",
                "evidence_level": 4,
                "candidate": safe_candidate(max_symbol_concentration_pct=80.0),
            }
        )

        self.assertIn("entry_parity_failed", parity_failed["blockers"])
        self.assertIn("symbol_concentration_too_high", concentrated["blockers"])


if __name__ == "__main__":
    unittest.main()
