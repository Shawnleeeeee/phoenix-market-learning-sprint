from __future__ import annotations

import unittest

from phoenix_strategy_optimizer import (
    CandidateRule,
    TradeRow,
    build_shadow_strategy_configs,
    compute_metrics,
    evaluate_rule,
    row_from_payload,
    rule_id,
)


def row(
    *,
    year: int,
    ret: float,
    oi5: float = 2.0,
    oi15: float = 5.0,
    score: float = 70.0,
    symbol: str = "BTCUSDT",
    quarter: str | None = None,
    entry_date: str | None = None,
    funding_rate_at_entry: float | None = 0.0001,
    funding_paid_during_hold: float | None = 0.0,
    spread_bps_at_entry: float | None = 1.5,
    order_latency_ms: float | None = 120.0,
    estimated_slippage_bps: float | None = 2.0,
    maker_or_taker: str = "paper_taker",
    liquidity_bucket: str = "large",
) -> TradeRow:
    return TradeRow(
        symbol=symbol,
        year=year,
        quarter=quarter or f"{year}-Q1",
        playbook="oi_build_breakout",
        side="BUY",
        session="Asia",
        oi_change_5m_pct=oi5,
        oi_change_15m_pct=oi15,
        trigger_score=score,
        after_fee_return_pct=ret,
        notional_usdt=100.0,
        entry_time=f"{entry_date or f'{year}-01-15'}T00:00:00+00:00",
        entry_date=entry_date or f"{year}-01-15",
        funding_rate_at_entry=funding_rate_at_entry,
        funding_paid_during_hold=funding_paid_during_hold,
        spread_bps_at_entry=spread_bps_at_entry,
        order_latency_ms=order_latency_ms,
        estimated_slippage_bps=estimated_slippage_bps,
        maker_or_taker=maker_or_taker,
        liquidity_bucket=liquidity_bucket,
    )


class StrategyOptimizerTests(unittest.TestCase):
    def test_compute_metrics_includes_extra_cost_stress(self) -> None:
        metrics = compute_metrics([row(year=2023, ret=1.0), row(year=2023, ret=-0.5)])
        stressed = compute_metrics([row(year=2023, ret=1.0), row(year=2023, ret=-0.5)], extra_round_trip_cost_pct=0.25)

        self.assertEqual(metrics["trade_count"], 2)
        self.assertGreater(metrics["profit_factor"], stressed["profit_factor"])
        self.assertAlmostEqual(stressed["avg_after_cost_return_pct"], 0.0)

    def test_candidate_rule_only_uses_entry_time_features(self) -> None:
        candidate = CandidateRule(rule_id="test", oi5_min=1.0, oi15_min=3.0, score_min=60.0)

        self.assertTrue(candidate.matches(row(year=2023, ret=0.3)))
        self.assertFalse(candidate.matches(row(year=2023, ret=3.0, oi15=1.0)))
        self.assertFalse(candidate.matches(row(year=2023, ret=3.0, score=50.0)))

    def test_walk_forward_requires_train_gate_before_oos_selection(self) -> None:
        rows = []
        for year in (2022, 2023, 2024, 2025, 2026):
            rows.extend(row(year=year, ret=0.3, symbol=f"A{i}USDT") for i in range(1000))
            rows.extend(row(year=year, ret=-0.1, symbol=f"B{i}USDT") for i in range(100))
        report = evaluate_rule(
            rows,
            CandidateRule(rule_id="test", oi5_min=1.0, oi15_min=3.0, score_min=60.0),
        )

        self.assertEqual(report["selected_fold_count"], 4)
        self.assertTrue(report["gates"]["positive_oos_years_all"])
        self.assertTrue(report["stress_tests"]["extra_10bps_round_trip"]["profit_factor"] > 1.0)

    def test_rule_id_is_stable_and_readable(self) -> None:
        self.assertEqual(
            rule_id(1.0, 60.0, oi15_min=5.0),
            "OPT_OI_BUILD_OI5_GE_1_OI15_GE_5_SCORE_GE_60",
        )

    def test_shadow_strategy_configs_are_shadow_only_with_requested_thresholds(self) -> None:
        configs = build_shadow_strategy_configs()

        self.assertEqual([item.strategy_id for item in configs], ["strategy_a_quality", "strategy_b_balanced"])
        strategy_a, strategy_b = configs
        self.assertFalse(strategy_a.live_trading_enabled)
        self.assertFalse(strategy_a.promotion_allowed)
        self.assertEqual(strategy_a.mode, "shadow_paper")
        self.assertEqual(strategy_a.rule.oi5_min, 1.0)
        self.assertEqual(strategy_a.rule.oi15_min, 5.0)
        self.assertEqual(strategy_a.rule.score_min, 60.0)
        self.assertFalse(strategy_b.live_trading_enabled)
        self.assertFalse(strategy_b.promotion_allowed)
        self.assertEqual(strategy_b.mode, "shadow_paper")
        self.assertEqual(strategy_b.rule.oi5_min, 2.0)
        self.assertIsNone(strategy_b.rule.oi15_min)
        self.assertEqual(strategy_b.rule.score_min, 60.0)

    def test_row_from_payload_captures_shadow_cost_realism_fields(self) -> None:
        parsed = row_from_payload(
            {
                "entry_time": "2026-04-30T12:34:56+00:00",
                "symbol": "ETHUSDT",
                "playbook": "oi_build_breakout",
                "side": "SELL",
                "trading_session": "Europe",
                "oi_change_5m_pct": 2.3,
                "oi_change_15m_pct": 5.5,
                "trigger_score": 66,
                "after_fee_return_pct": 0.42,
                "notional_usdt": 250.0,
                "funding_rate": -0.0002,
                "funding_paid_during_hold": -0.01,
                "spread_bps": 1.8,
                "order_latency_ms": 175,
                "estimated_slippage_bps": 3.4,
                "maker_or_taker": "paper_taker",
                "liquidity_bucket": "major",
            }
        )

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.entry_date, "2026-04-30")
        self.assertAlmostEqual(parsed.funding_rate_at_entry or 0.0, -0.0002)
        self.assertAlmostEqual(parsed.funding_paid_during_hold or 0.0, -0.01)
        self.assertAlmostEqual(parsed.spread_bps_at_entry or 0.0, 1.8)
        self.assertAlmostEqual(parsed.order_latency_ms or 0.0, 175.0)
        self.assertAlmostEqual(parsed.estimated_slippage_bps or 0.0, 3.4)
        self.assertEqual(parsed.maker_or_taker, "paper_taker")
        self.assertEqual(parsed.liquidity_bucket, "major")

    def test_evaluate_rule_reports_required_shadow_acceptance_fields(self) -> None:
        rows: list[TradeRow] = []
        for year in (2022, 2023):
            for index in range(260):
                rows.append(
                    row(
                        year=year,
                        ret=0.35,
                        symbol=f"S{index % 10}USDT",
                        quarter=f"{year}-Q{(index % 4) + 1}",
                        entry_date=f"{year}-01-{(index % 28) + 1:02d}",
                    )
                )
            for index in range(60):
                rows.append(
                    row(
                        year=year,
                        ret=-0.15,
                        symbol=f"L{index % 10}USDT",
                        quarter=f"{year}-Q{(index % 4) + 1}",
                        entry_date=f"{year}-02-{(index % 28) + 1:02d}",
                    )
                )
        report = evaluate_rule(
            rows,
            CandidateRule(rule_id="test", oi5_min=1.0, oi15_min=5.0, score_min=60.0),
            folds=(((2022,), (2023,)),),
            min_train_trades=100,
        )

        summary = report["oos_summary"]
        self.assertIn("trade_count", summary)
        self.assertIn("win_rate", summary)
        self.assertIn("avg_after_fee_return", summary)
        self.assertIn("profit_factor", summary)
        self.assertIn("extra_10bps_round_trip", report["stress_tests"])
        self.assertIn("extra_20bps_round_trip", report["stress_tests"])
        self.assertIn("extra_25bps_round_trip", report["stress_tests"])
        self.assertIn("2023", report["per_year"])
        self.assertIn("2023-Q1", report["per_quarter"])
        self.assertIn("top_symbol_pnl_share", report["stability"])
        self.assertEqual(report["field_coverage"]["required_field_count"], 7)
        self.assertTrue(report["acceptance_gates"]["shadow_trade_count_ge_300"])
        self.assertTrue(report["acceptance_gates"]["avg_after_fee_return_gt_0"])
        self.assertTrue(report["acceptance_gates"]["stress_20bps_profit_factor_gt_1_2"])
        self.assertTrue(report["acceptance_gates"]["daily_loss_kill_switch_effective"])

    def test_acceptance_gate_rejects_single_symbol_profit_concentration(self) -> None:
        rows = [
            row(year=2022, ret=0.4, symbol="BTCUSDT", entry_date=f"2022-01-{(index % 28) + 1:02d}")
            for index in range(120)
        ]
        rows.extend(
            row(year=2023, ret=0.4, symbol="BTCUSDT", entry_date=f"2023-01-{(index % 28) + 1:02d}")
            for index in range(320)
        )
        report = evaluate_rule(
            rows,
            CandidateRule(rule_id="test", oi5_min=1.0, oi15_min=5.0, score_min=60.0),
            folds=(((2022,), (2023,)),),
            min_train_trades=100,
        )

        self.assertAlmostEqual(report["stability"]["top_symbol_pnl_share"], 1.0)
        self.assertFalse(report["acceptance_gates"]["top_symbol_pnl_share_le_20pct"])


if __name__ == "__main__":
    unittest.main()
