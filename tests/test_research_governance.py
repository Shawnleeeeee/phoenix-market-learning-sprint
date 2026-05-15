from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_research_governance import (
    audit_lookahead_recursive_bias,
    append_experiment_ledger_entry,
    build_binance_public_kline_url,
    build_execution_realism_report,
    extract_backtest_gate_metrics,
    normalize_kline_csv_row,
    promotion_gate_decision,
)


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


class ResearchGovernanceTests(unittest.TestCase):
    def test_bias_audit_flags_timestamp_inversion_and_label_like_factor(self) -> None:
        report = audit_lookahead_recursive_bias(
            [
                {
                    "event_id": "evt-1",
                    "observed_at_ms": 1000,
                    "sample": {"anchor_close_time_ms": 2000},
                    "factors": {"future_return_pct": 1.2},
                }
            ]
        )

        self.assertTrue(report["lookahead_bias_risk"])
        self.assertEqual(report["severity_counts"]["high"], 2)

    def test_public_zip_url_and_kline_parser_are_official_layout_compatible(self) -> None:
        url = build_binance_public_kline_url(
            symbol="btcusdt",
            interval="1m",
            day=date(2026, 4, 28),
        )
        self.assertEqual(
            url,
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-04-28.zip",
        )
        row = normalize_kline_csv_row(
            [
                "1714262400000",
                "100",
                "110",
                "90",
                "105",
                "12.5",
                "1714262459999",
                "1300",
                "42",
                "6.0",
                "630",
                "0",
            ],
            symbol="BTCUSDT",
            interval="1m",
            source_url=url,
        )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["symbol"], "BTCUSDT")
        self.assertEqual(row["open_time_ms"], 1714262400000)
        self.assertAlmostEqual(row["quote_asset_volume"], 1300.0)

    def test_execution_realism_penalizes_raw_returns(self) -> None:
        report = build_execution_realism_report(
            [
                {"side": "BUY", "after_fee_and_slippage_return_pct": 1.0},
                {"side": "BUY", "after_fee_and_slippage_return_pct": -0.5},
            ],
            latency_ms=1000,
            latency_drift_pct_per_sec=0.1,
            extra_slippage_bps=10,
            partial_fill_ratio=0.8,
        )

        self.assertEqual(report["rows_checked"], 2)
        self.assertLess(
            report["execution_realistic"]["avg_return_pct"],
            report["raw"]["avg_return_pct"],
        )

    def test_experiment_ledger_writes_jsonl_and_sqlite(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            jsonl_path = base / "experiments.jsonl"
            sqlite_path = base / "experiments.sqlite"
            entry = append_experiment_ledger_entry(
                jsonl_path,
                experiment_name="factor_factory_smoke",
                params={"min_samples": 3},
                metrics={"profit_factor": 1.4},
                sqlite_path=sqlite_path,
            )

            self.assertTrue(jsonl_path.exists())
            self.assertTrue(sqlite_path.exists())
            self.assertEqual(json.loads(jsonl_path.read_text(encoding="utf-8"))["experiment_id"], entry["experiment_id"])
            connection = sqlite3.connect(sqlite_path)
            try:
                count = connection.execute("SELECT COUNT(*) FROM experiment_ledger").fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(count, 1)

    def test_promotion_gate_never_enables_live_and_reports_blockers(self) -> None:
        report = promotion_gate_decision(
            factor_control={"rules": {"factor_trend_alignment": {"status": "focus_shadow"}}},
            shadow_readiness={"live_unlock_candidate": True},
            execution_realism={"execution_realistic": {"avg_return_pct": 0.2, "profit_factor": 1.4}},
            bias_audit={"lookahead_bias_risk": False},
            backtest_report={
                "report_type": "phoenix_promotion_backtest",
                "trade_count": 1200,
                "avg_return_pct": 0.05,
                "profit_factor": 1.3,
                "win_rate_pct": 53.0,
                "sharpe_ratio": 1.1,
            },
        )

        self.assertTrue(report["promotion_candidate"])
        self.assertFalse(report["promotion_allowed"])
        self.assertFalse(report["live_trading_enabled"])
        self.assertEqual(report["backtest_metrics"]["trade_count"], 1200)

    def test_promotion_gate_blocks_missing_or_negative_backtest(self) -> None:
        missing = promotion_gate_decision(
            factor_control={"rules": {"factor_trend_alignment": {"status": "focus_shadow"}}},
            shadow_readiness={"live_unlock_candidate": True},
            execution_realism={"execution_realistic": {"avg_return_pct": 0.2, "profit_factor": 1.4}},
            bias_audit={"lookahead_bias_risk": False},
        )
        self.assertIn("missing_backtest_report", missing["blockers"])

        negative = promotion_gate_decision(
            factor_control={"rules": {"factor_trend_alignment": {"status": "focus_shadow"}}},
            shadow_readiness={"live_unlock_candidate": True},
            execution_realism={"execution_realistic": {"avg_return_pct": 0.2, "profit_factor": 1.4}},
            bias_audit={"lookahead_bias_risk": False},
            backtest_report={"trade_count": 1200, "avg_return_pct": -0.01, "profit_factor": 0.9},
        )
        self.assertIn("backtest_avg_return_not_positive", negative["blockers"])
        self.assertIn("backtest_profit_factor_below_1_2", negative["blockers"])

    def test_backtest_metrics_accept_playbook_report_shape(self) -> None:
        metrics = extract_backtest_gate_metrics(
            {
                "portfolio_summary": {
                    "accepted_trade_count": 1001,
                    "avg_after_fee_return_pct": 0.08,
                    "profit_factor": 1.25,
                }
            }
        )

        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertEqual(metrics["trade_count"], 1001)
        self.assertEqual(metrics["avg_return_pct"], 0.08)

    def test_cli_execution_realism_writes_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            outcomes_file = base / "outcomes.jsonl"
            output_file = base / "execution_realism.json"
            write_jsonl(
                outcomes_file,
                [{"event": "signal_bridge_shadow_horizon_result", "after_fee_and_slippage_return_pct": 0.7}],
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_research_governance.py",
                    "execution-realism",
                    "--outcomes-file",
                    str(outcomes_file),
                    "--output-json",
                    str(output_file),
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("execution_realism", result.stdout)
            self.assertTrue(output_file.exists())


if __name__ == "__main__":
    unittest.main()
