from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_hmm_state_report import build_hmm_state_report
from phoenix_reports.hmm_state import build_hmm_state_report as wrapped_build_hmm_state_report


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def signal(
    event_id: str,
    *,
    symbol: str = "BTCUSDT",
    signal_time: str = "2026-05-01T00:00:00+00:00",
    playbook: str = "oi_build_breakout",
    ret_1bar_pct: float = 0.5,
    volume_burst_ratio: float = 1.8,
    range_to_atr: float = 1.2,
    oi_change_5m_pct: float = 0.6,
    spread_bps: float = 2.0,
    estimated_slippage_bps: float = 3.0,
) -> dict[str, object]:
    return {
        "event": "signal_bridge_shadow_logged",
        "event_id": event_id,
        "symbol": symbol,
        "signal_time": signal_time,
        "playbook": playbook,
        "sample": {
            "ret_1bar_pct": ret_1bar_pct,
            "volume_burst_ratio": volume_burst_ratio,
            "range_to_atr": range_to_atr,
        },
        "enrichments": {
            "oi_change_5m_pct": oi_change_5m_pct,
            "spread_bps": spread_bps,
            "estimated_slippage_bps": estimated_slippage_bps,
        },
    }


def outcome(
    source_event_id: str,
    *,
    return_pct: float = 0.3,
    strategy: str = "momentum_scalp",
    exit_reason: str = "take_profit",
    max_drawdown_pct: float = -0.2,
) -> dict[str, object]:
    return {
        "event": "signal_bridge_shadow_horizon_result",
        "event_id": f"{source_event_id}::AUTO:900",
        "source_event_id": source_event_id,
        "symbol": "BTCUSDT",
        "strategy_id": strategy,
        "shadow_branch_id": "AUTO",
        "horizon_sec": 900,
        "playbook": "oi_build_breakout",
        "after_fee_and_slippage_return_pct": return_pct,
        "effective_quote_allocation_usdt": 100.0,
        "exit_reason": exit_reason,
        "max_drawdown_pct": max_drawdown_pct,
    }


class HmmStateReportTests(unittest.TestCase):
    def test_report_is_offline_only_and_exposes_required_sections(self) -> None:
        report = build_hmm_state_report(
            [
                signal("evt-1"),
                signal("evt-2", oi_change_5m_pct=-0.8, ret_1bar_pct=-0.4, playbook="oi_unwind"),
            ],
            [
                outcome("evt-1", return_pct=0.4),
                outcome("evt-2", return_pct=-0.5, exit_reason="stop_loss_hit", max_drawdown_pct=-0.7),
            ],
        )
        self.assertFalse(report["trading_gate_enabled"])
        self.assertFalse(report["position_manager_enabled"])
        self.assertFalse(report["policy"]["trading_gate_enabled"])
        self.assertFalse(report["policy"]["position_manager_enabled"])
        self.assertIn("hidden_regime_probabilities", report)
        self.assertIn("state_performance", report)
        self.assertIn("strategy_by_hidden_state", report)
        self.assertIn("exit_risk_by_hidden_state", report)
        self.assertEqual(report["input_counts"]["independent_outcomes"], 2)

    def test_lightweight_behavior_is_graceful_without_required_hmm_dependency(self) -> None:
        report = build_hmm_state_report([signal("evt-1")], [outcome("evt-1")])
        self.assertTrue(report["lightweight_model"])
        self.assertIn("hmm_available", report)
        self.assertTrue(report["hmm_available"] is False or report["lightweight_model"] is True)
        probabilities = report["hidden_regime_probabilities"][0]["probabilities"]
        self.assertAlmostEqual(sum(probabilities.values()), 1.0, places=5)

    def test_wrapper_imports_root_report_builder(self) -> None:
        report = wrapped_build_hmm_state_report([signal("evt-1")], [outcome("evt-1")])
        self.assertEqual(report["report_type"], "hmm_state_report")

    def test_cli_writes_default_json_and_markdown(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            write_jsonl(base / "signal_bridge_shadow_signals.jsonl", [signal("evt-1"), signal("evt-2", spread_bps=10.0)])
            write_jsonl(
                base / "signal_bridge_shadow_outcomes.jsonl",
                [outcome("evt-1", return_pct=0.4), outcome("evt-2", return_pct=-0.3, exit_reason="stop_loss_hit")],
            )
            repo = Path(__file__).resolve().parents[1]
            subprocess.run(
                [sys.executable, "phoenix_hmm_state_report.py", "--input-dir", str(base)],
                cwd=repo,
                check=True,
                capture_output=True,
                text=True,
            )
            output_json = base / "hmm_state_report.json"
            output_md = base / "hmm_state_report.md"
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertFalse(payload["policy"]["trading_gate_enabled"])
            self.assertEqual(payload["input_counts"]["hidden_state_events"], 2)


if __name__ == "__main__":
    unittest.main()
