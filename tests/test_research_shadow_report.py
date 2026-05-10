from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_research_shadow_report import build_research_shadow_report


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


class ResearchShadowReportTests(unittest.TestCase):
    def test_build_report_groups_research_pool_outcomes_and_metrics(self) -> None:
        signals = [
            {
                "event": "signal_bridge_shadow_logged",
                "event_id": "evt-1",
                "symbol": "BTCUSDT",
                "trigger_types": ["volume_burst", "range_expansion"],
                "bar_interval": "5m",
                "trading_session": "US",
                "playbook": "oi_build_breakout",
                "research_direction": "oi_breakout_v2",
                "research_only": True,
                "branch_type": "research_pool",
            },
            {
                "event": "signal_bridge_shadow_logged",
                "event_id": "evt-2",
                "symbol": "BTCUSDT",
                "trigger_types": ["range_expansion", "volume_burst"],
                "bar_interval": "5m",
                "trading_session": "US",
                "playbook": "oi_build_breakout",
                "research_direction": "oi_breakout_v2",
                "research_only": True,
                "branch_type": "research_pool",
            },
            {
                "event": "signal_bridge_shadow_logged",
                "event_id": "evt-live",
                "symbol": "ETHUSDT",
                "trigger_types": ["volume_burst"],
                "bar_interval": "1m",
                "trading_session": "Asia",
                "playbook": "live_control",
                "research_only": False,
                "branch_type": "stable_core",
            },
        ]
        outcomes = [
            {
                "event": "signal_bridge_shadow_horizon_result",
                "event_id": "evt-1",
                "symbol": "BTCUSDT",
                "shadow_branch_id": "AUTO",
                "branch_type": "research_pool",
                "research_only": True,
                "horizon_sec": 900,
                "after_fee_return_pct": 2.0,
                "effective_quote_allocation_usdt": 1000.0,
            },
            {
                "event": "signal_bridge_shadow_horizon_result",
                "event_id": "evt-2",
                "symbol": "BTCUSDT",
                "shadow_branch_id": "AUTO",
                "branch_type": "research_pool",
                "research_only": True,
                "horizon_sec": 900,
                "after_fee_return_pct": -1.0,
                "effective_quote_allocation_usdt": 1000.0,
            },
            {
                "event": "signal_bridge_shadow_horizon_result",
                "event_id": "evt-live",
                "symbol": "ETHUSDT",
                "shadow_branch_id": "AUTO",
                "branch_type": "stable_core",
                "research_only": False,
                "horizon_sec": 900,
                "after_fee_return_pct": 20.0,
                "effective_quote_allocation_usdt": 1000.0,
            },
            {
                "event": "signal_bridge_shadow_horizon_result",
                "event_id": "evt-1",
                "symbol": "BTCUSDT",
                "shadow_branch_id": "AUTO",
                "branch_type": "research_pool",
                "research_only": True,
                "horizon_sec": 1800,
                "after_fee_return_pct": 0.5,
                "effective_quote_allocation_usdt": 1000.0,
            },
        ]

        report = build_research_shadow_report(signals, outcomes, min_samples=2)

        self.assertEqual(report["input_counts"]["signal_count"], 3)
        self.assertEqual(report["input_counts"]["research_signal_count"], 2)
        self.assertEqual(report["input_counts"]["outcome_count"], 4)
        self.assertEqual(report["input_counts"]["research_outcome_count"], 3)
        self.assertEqual(report["input_counts"]["reported_bucket_count"], 1)
        bucket = report["buckets"][0]
        self.assertEqual(bucket["symbol"], "BTCUSDT")
        self.assertEqual(bucket["trigger_type"], "range_expansion+volume_burst")
        self.assertEqual(bucket["bar_interval"], "5m")
        self.assertEqual(bucket["trading_session"], "US")
        self.assertEqual(bucket["playbook"], "oi_build_breakout")
        self.assertEqual(bucket["research_direction"], "oi_breakout_v2")
        self.assertEqual(bucket["branch"], "AUTO")
        self.assertEqual(bucket["horizon_sec"], 900)
        self.assertEqual(bucket["sample_count"], 2)
        self.assertEqual(bucket["win_count"], 1)
        self.assertEqual(bucket["loss_count"], 1)
        self.assertAlmostEqual(bucket["win_rate_pct"], 50.0, places=6)
        self.assertAlmostEqual(bucket["profit_factor"], 2.0, places=6)
        self.assertAlmostEqual(bucket["sharpe"], 0.23570226039551587, places=6)
        self.assertAlmostEqual(bucket["mdd_pct"], 1.0, places=6)
        self.assertAlmostEqual(bucket["total_pnl_usdt"], 10.0, places=6)
        self.assertAlmostEqual(bucket["avg_return_pct"], 0.5, places=6)

    def test_cli_writes_json_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            signals_file = base / "signals.jsonl"
            outcomes_file = base / "outcomes.jsonl"
            output_file = base / "report.json"
            write_jsonl(
                signals_file,
                [
                    {
                        "event": "signal_bridge_shadow_logged",
                        "event_id": "evt-1",
                        "symbol": "SOLUSDT",
                        "trigger_type": "liquidation_flush",
                        "bar_interval": "1m",
                        "trading_session": "Asia",
                        "playbook": "liquidation_flush",
                        "research_only": "true",
                        "branch_type": "research_pool",
                    }
                ],
            )
            write_jsonl(
                outcomes_file,
                [
                    {
                        "event": "signal_bridge_shadow_horizon_result",
                        "event_id": "evt-1",
                        "shadow_branch_id": "R1",
                        "branch_type": "research_pool",
                        "horizon_sec": 300,
                        "after_fee_and_slippage_return_pct": 1.5,
                        "effective_quote_allocation_usdt": 200.0,
                    }
                ],
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_research_shadow_report.py",
                    "--signals-file",
                    str(signals_file),
                    "--outcomes-file",
                    str(outcomes_file),
                    "--output-json",
                    str(output_file),
                    "--min-samples",
                    "1",
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("reported_bucket_count=1", result.stdout)
            report = json.loads(output_file.read_text(encoding="utf-8"))
            self.assertEqual(report["buckets"][0]["symbol"], "SOLUSDT")
            self.assertEqual(report["buckets"][0]["branch"], "R1")
            self.assertAlmostEqual(report["buckets"][0]["total_pnl_usdt"], 3.0, places=6)


if __name__ == "__main__":
    unittest.main()
