from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_testnet_execution_report import build_testnet_execution_report


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


class TestnetExecutionReportTests(unittest.TestCase):
    def test_build_report_metrics_and_breakdowns(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            write_jsonl(
                base / "round_001_trades.jsonl",
                [
                    {
                        "symbol": "BTCUSDT",
                        "setup": "breakout_long",
                        "side": "BUY",
                        "exit_reason": "take_profit",
                        "realized_pnl_usdt": 12.0,
                        "commission_usdt": 2.0,
                        "net_pnl_usdt": 10.0,
                        "latency_ms": 100,
                        "estimated_slippage_bps": 1.5,
                    },
                    {
                        "symbol": "ETHUSDT",
                        "setup": "reclaim_long",
                        "side": "BUY",
                        "exit_reason": "stop_loss",
                        "realized_pnl_usdt": -5.0,
                        "commission_usdt": 1.0,
                        "net_pnl_usdt": -6.0,
                        "entry_latency_ms": 300,
                        "entry_slippage_bps": 4.5,
                        "partial_fill": True,
                    },
                    {
                        "symbol": "BTCUSDT",
                        "setup": "breakout_long",
                        "side": "BUY",
                        "exit_reason": "time_stop",
                        "realized_pnl_usdt": 2.0,
                        "commission_usdt": 1.0,
                        "net_pnl_usdt": 1.0,
                        "order_status": "REJECTED",
                        "reject_reason": "post only",
                    },
                ],
            )
            (base / "round_001_report.json").write_text(json.dumps({"round": 1, "trade_count": 3}), encoding="utf-8")

            report = build_testnet_execution_report(base)

        metrics = report["metrics"]
        self.assertEqual(metrics["trade_count"], 3)
        self.assertEqual(metrics["win_count"], 2)
        self.assertAlmostEqual(metrics["win_rate_pct"], 66.666667, places=6)
        self.assertAlmostEqual(metrics["gross_pnl_usdt"], 9.0, places=6)
        self.assertAlmostEqual(metrics["commission_usdt"], 4.0, places=6)
        self.assertAlmostEqual(metrics["net_pnl_usdt"], 5.0, places=6)
        self.assertAlmostEqual(metrics["realized_pnl_usdt"], 9.0, places=6)
        self.assertEqual(metrics["partial_fill_count"], 1)
        self.assertAlmostEqual(metrics["partial_fill_rate_pct"], 33.333333, places=6)
        self.assertEqual(metrics["reject_count"], 1)
        self.assertAlmostEqual(metrics["reject_rate_pct"], 33.333333, places=6)
        self.assertAlmostEqual(metrics["max_drawdown_usdt"], 6.0, places=6)
        self.assertEqual(report["latency_stats_ms"]["count"], 2)
        self.assertAlmostEqual(report["latency_stats_ms"]["avg"], 200.0, places=6)
        self.assertEqual(report["slippage_estimate_stats_bps"]["count"], 2)
        symbol_rows = {row["key"]: row for row in report["breakdowns"]["symbol"]}
        self.assertEqual(symbol_rows["BTCUSDT"]["trade_count"], 2)
        self.assertAlmostEqual(symbol_rows["BTCUSDT"]["net_pnl_usdt"], 11.0, places=6)
        setup_rows = {row["key"]: row for row in report["breakdowns"]["setup"]}
        self.assertEqual(setup_rows["breakout_long"]["trade_count"], 2)
        exit_rows = {row["key"]: row for row in report["breakdowns"]["exit"]}
        self.assertEqual(exit_rows["stop_loss"]["loss_count"], 1)

    def test_missing_fields_are_reported_without_crashing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            write_jsonl(
                base / "round_002_trades.jsonl",
                [
                    {"symbol": "SOLUSDT", "realized_pnl_usdt": 3.0},
                    {"side": "SELL", "commission_usdt": 0.5},
                ],
            )

            report = build_testnet_execution_report(base)

        self.assertEqual(report["metrics"]["trade_count"], 2)
        self.assertEqual(report["sample_status"]["status"], "ready")
        self.assertEqual(report["sample_status"]["missing_field_counts"]["net_pnl_usdt"], 2)
        self.assertEqual(report["sample_status"]["missing_field_counts"]["exit_reason"], 2)
        self.assertEqual(report["latency_stats_ms"]["count"], 0)
        self.assertEqual(report["breakdowns"]["exit"][0]["key"], "unknown")
        self.assertEqual(report["promotion_cross_validation"]["status"], "not_provided")

    def test_cli_writes_json_and_markdown(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            shadow_report = base / "shadow.json"
            shadow_report.write_text(json.dumps({"version": "shadow-v1", "buckets": [{"symbol": "BTCUSDT"}]}), encoding="utf-8")
            write_jsonl(
                base / "round_001_trades.jsonl",
                [
                    {
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "setup": "breakout_long",
                        "exit_reason": "take_profit",
                        "realized_pnl_usdt": 1.2,
                        "commission_usdt": 0.2,
                        "net_pnl_usdt": 1.0,
                    }
                ],
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_testnet_execution_report.py",
                    "--input-dir",
                    str(base),
                    "--shadow-report",
                    str(shadow_report),
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("trade_count=1", result.stdout)
            json_report = base / "testnet_execution_report.json"
            md_report = base / "testnet_execution_report.md"
            self.assertTrue(json_report.exists())
            self.assertTrue(md_report.exists())
            payload = json.loads(json_report.read_text(encoding="utf-8"))
            self.assertEqual(payload["metrics"]["trade_count"], 1)
            self.assertEqual(payload["promotion_cross_validation"]["status"], "loaded")
            self.assertEqual(payload["promotion_cross_validation"]["shadow_bucket_count"], 1)
            self.assertIn("Phoenix Testnet Execution Report", md_report.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
