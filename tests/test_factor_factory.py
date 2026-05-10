from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_factor_factory import (
    build_factor_factory_control,
    build_factor_factory_report,
)


def snapshot(event_id: str, *, ret_1bar_pct: float, side_bias: str = "up") -> dict[str, object]:
    return {
        "event": "market_event_created",
        "event_id": event_id,
        "symbol": "BTCUSDT",
        "bar_interval": "1m",
        "trading_session": "US",
        "sample": {
            "trigger_candle_direction": side_bias,
            "candle_direction": side_bias,
            "volume_burst_ratio": 3.0,
            "range_to_atr": 2.2,
            "body_to_atr": 1.4,
            "ret_1bar_pct": ret_1bar_pct,
            "ret_5bar_pct": ret_1bar_pct * 1.5,
            "ret_15bar_pct": ret_1bar_pct * 2.0,
            "quote_volume_24h": 200_000_000.0,
        },
        "enrichments": {
            "oi_change_5m_pct": 1.4,
            "oi_change_15m_pct": 2.2,
            "depth_imbalance": 0.4 if ret_1bar_pct > 0 else -0.4,
            "spread_bps": 2.0,
            "estimated_slippage_bps": 3.0,
            "bid_depth_notional_5": 900_000.0,
            "ask_depth_notional_5": 900_000.0,
            "btcusdt_ret_5m_pct": 0.2 if ret_1bar_pct > 0 else -0.2,
            "btcusdt_ret_60m_pct": 0.4 if ret_1bar_pct > 0 else -0.4,
            "ethusdt_ret_5m_pct": 0.1 if ret_1bar_pct > 0 else -0.1,
            "taker_buy_ratio_1m": 1.6 if ret_1bar_pct > 0 else 0.6,
            "aggressive_flow_delta": 0.35 if ret_1bar_pct > 0 else -0.35,
        },
    }


def outcome(source_event_id: str, *, side: str, return_pct: float) -> dict[str, object]:
    return {
        "event": "signal_bridge_shadow_horizon_result",
        "event_id": f"{source_event_id}::TREND_SCALP_050_150",
        "shadow_instance_id": f"{source_event_id}::TREND_SCALP_050_150",
        "source_event_id": source_event_id,
        "shadow_branch_id": "TREND_SCALP_050_150",
        "branch_type": "research_pool",
        "research_only": True,
        "symbol": "BTCUSDT",
        "side": side,
        "horizon_sec": 180,
        "after_fee_and_slippage_return_pct": return_pct,
        "effective_quote_allocation_usdt": 10.0,
    }


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


class FactorFactoryTests(unittest.TestCase):
    def test_report_ranks_positive_factor_rule_and_control_focuses_it(self) -> None:
        snapshots = []
        outcomes = []
        for index in range(8):
            event_id = f"evt-{index}"
            snapshots.append(snapshot(event_id, ret_1bar_pct=1.2))
            outcomes.append(outcome(event_id, side="BUY", return_pct=0.7))
        for index in range(8, 12):
            event_id = f"evt-{index}"
            snapshots.append(snapshot(event_id, ret_1bar_pct=-1.2, side_bias="down"))
            outcomes.append(outcome(event_id, side="SELL", return_pct=-0.2))

        report = build_factor_factory_report(
            snapshots=snapshots,
            outcomes=outcomes,
            min_samples=3,
            pair_min_samples=3,
            top_n=10,
        )

        self.assertEqual(report["input_counts"]["rule_evaluation_rows"], 12)
        self.assertTrue(report["ranked_candidate_rules"])
        self.assertEqual(report["ranked_candidate_rules"][0]["rule"], "factor_trend_alignment")
        self.assertTrue(report["factor_attribution"]["top_single_factor_slices"])

        control = build_factor_factory_control(report, min_outcomes=3, focus_profit_factor=1.1)
        self.assertEqual(control["mode"], "shadow_research_only")
        self.assertIn(control["rules"]["factor_trend_alignment"]["status"], {"focus_shadow", "observing"})
        self.assertFalse(control["live_trading_enabled"])

    def test_cli_writes_report_and_control(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            snapshots_file = base / "snapshots.jsonl"
            outcomes_file = base / "outcomes.jsonl"
            report_file = base / "factor_factory_report.json"
            control_file = base / "factor_factory_control.json"
            write_jsonl(snapshots_file, [snapshot("evt-1", ret_1bar_pct=1.2), snapshot("evt-2", ret_1bar_pct=1.3)])
            write_jsonl(
                outcomes_file,
                [
                    outcome("evt-1", side="BUY", return_pct=0.5),
                    outcome("evt-2", side="BUY", return_pct=0.6),
                ],
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_factor_factory.py",
                    "--snapshots-file",
                    str(snapshots_file),
                    "--outcomes-file",
                    str(outcomes_file),
                    "--report-file",
                    str(report_file),
                    "--control-file",
                    str(control_file),
                    "--min-samples",
                    "1",
                    "--pair-min-samples",
                    "1",
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("factor_factory_report", result.stdout)
            self.assertTrue(report_file.exists())
            self.assertTrue(control_file.exists())


if __name__ == "__main__":
    unittest.main()
