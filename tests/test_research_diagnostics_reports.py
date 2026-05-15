from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_loss_attribution_report import build_loss_attribution_report, classify_loss_reason
from phoenix_markov_state_report import build_markov_state_report
from phoenix_monte_carlo_report import build_monte_carlo_report, run_monte_carlo
from phoenix_research_diagnostics import dedupe_independent_events, longest_losing_streak, max_drawdown_pct, return_pct


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def signal(
    event_id: str,
    *,
    symbol: str = "BTCUSDT",
    signal_time: str = "2026-05-01T00:00:00+00:00",
    playbook: str = "oi_build_breakout",
    side: str = "BUY",
    ret_1bar_pct: float = 0.1,
    volume_burst_ratio: float = 1.0,
    range_to_atr: float = 0.7,
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
        "side": side,
        "sample": {
            "ret_1bar_pct": ret_1bar_pct,
            "volume_burst_ratio": volume_burst_ratio,
            "range_to_atr": range_to_atr,
        },
        "enrichments": {
            "oi_change_5m_pct": oi_change_5m_pct,
            "spread_bps": spread_bps,
            "estimated_slippage_bps": estimated_slippage_bps,
            "btcusdt_ret_5m_pct": 0.2,
        },
    }


def outcome(
    source_event_id: str,
    *,
    event_id: str | None = None,
    symbol: str = "BTCUSDT",
    branch: str = "AUTO",
    horizon_sec: int = 900,
    return_pct: float = -1.0,
    side: str = "BUY",
    exit_reason: str = "horizon_close",
    max_runup_pct: float = 0.1,
    max_drawdown_pct_value: float = -0.5,
    close_return_pct: float | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "event": "signal_bridge_shadow_horizon_result",
        "event_id": event_id or f"{source_event_id}::{branch}:{horizon_sec}",
        "source_event_id": source_event_id,
        "symbol": symbol,
        "shadow_branch_id": branch,
        "horizon_sec": horizon_sec,
        "side": side,
        "playbook": "oi_build_breakout",
        "after_fee_and_slippage_return_pct": return_pct,
        "after_fee_return_pct": return_pct + 0.05,
        "effective_quote_allocation_usdt": 100.0,
        "exit_reason": exit_reason,
        "max_runup_pct": max_runup_pct,
        "max_drawdown_pct": max_drawdown_pct_value,
    }
    if close_return_pct is not None:
        row["close_return_pct"] = close_return_pct
    return row


class ResearchDiagnosticsReportTests(unittest.TestCase):
    def test_loss_reason_classification_core_cases(self) -> None:
        reason, secondary = classify_loss_reason(
            outcome("evt-1", exit_reason="stop_loss_hit", return_pct=-1.2, max_runup_pct=0.1),
            signal("evt-1"),
        )
        self.assertEqual(reason, "stop_loss_hit")
        self.assertIn("immediate_adverse_move", secondary)

        reason, _ = classify_loss_reason(
            outcome(
                "evt-2",
                exit_reason="horizon_close",
                return_pct=-0.02,
                close_return_pct=0.01,
                max_runup_pct=0.02,
            ),
            signal("evt-2"),
        )
        self.assertEqual(reason, "cost_slippage_edge_destroyed")

        reason, _ = classify_loss_reason(
            outcome("evt-3", exit_reason="horizon_close", return_pct=-0.4, max_runup_pct=0.9),
            signal("evt-3"),
        )
        self.assertEqual(reason, "gave_back_profit")

    def test_independent_event_dedup_keeps_worst_source_event(self) -> None:
        rows = [
            outcome("evt-1", branch="A", horizon_sec=300, return_pct=-0.2),
            outcome("evt-1", branch="B", horizon_sec=900, return_pct=-1.5),
            outcome("evt-2", branch="A", horizon_sec=300, return_pct=0.4),
        ]
        deduped = dedupe_independent_events(rows)
        self.assertEqual(len(deduped), 2)
        evt_1 = [row for row in deduped if row["source_event_id"] == "evt-1"][0]
        self.assertEqual(evt_1["shadow_branch_id"], "B")

    def test_loss_report_separates_raw_branch_and_independent_scope(self) -> None:
        report = build_loss_attribution_report(
            [
                outcome("evt-1", branch="A", return_pct=-0.2),
                outcome("evt-1", branch="B", return_pct=-1.5),
                outcome("evt-2", branch="A", return_pct=-0.4),
            ],
            [signal("evt-1"), signal("evt-2")],
        )
        self.assertEqual(report["deduplication"]["raw_branch_losses"], 3)
        self.assertEqual(report["deduplication"]["independent_source_event_losses"], 2)
        self.assertTrue(report["strategy_priority_fix_list"])

    def test_monte_carlo_deterministic_seed_and_path_metrics(self) -> None:
        values = [1.0, -2.0, -3.0, 4.0, -1.0]
        self.assertAlmostEqual(max_drawdown_pct(values), 5.0)
        self.assertEqual(longest_losing_streak(values), 2)
        first = run_monte_carlo(values, simulations=50, seed=123, min_independent_events=1)
        second = run_monte_carlo(values, simulations=50, seed=123, min_independent_events=1)
        self.assertEqual(first["modes"]["bootstrap"], second["modes"]["bootstrap"])
        self.assertFalse(first["insufficient_sample"])

    def test_monte_carlo_report_marks_insufficient_sample(self) -> None:
        report = build_monte_carlo_report(
            [outcome("evt-1", return_pct=0.2), outcome("evt-2", return_pct=-0.3)],
            simulations=20,
            seed=7,
            min_independent_events=30,
        )
        row = report["groups"]["playbook"][0]
        self.assertTrue(row["monte_carlo"]["insufficient_sample"])
        self.assertEqual(report["input_counts"]["independent_source_events"], 2)

    def test_return_pct_falls_back_to_net_pnl_usdt(self) -> None:
        self.assertAlmostEqual(
            return_pct({"net_pnl_usdt": 2.5, "effective_quote_allocation_usdt": 100.0}),
            2.5,
            places=6,
        )

    def test_markov_transition_matrix_collapses_same_state_per_symbol(self) -> None:
        signals = [
            signal(
                "evt-1",
                signal_time="2026-05-01T00:00:00+00:00",
                playbook="",
                oi_change_5m_pct=0.0,
                volume_burst_ratio=0.7,
                range_to_atr=0.6,
            ),
            signal("evt-2", signal_time="2026-05-01T00:01:00+00:00", playbook="oi_build_breakout", oi_change_5m_pct=1.0),
            signal("evt-3", signal_time="2026-05-01T00:02:00+00:00", playbook="oi_build_breakout", oi_change_5m_pct=1.1),
            signal("evt-4", signal_time="2026-05-01T00:03:00+00:00", playbook="", oi_change_5m_pct=-0.8),
        ]
        report = build_markov_state_report(
            signals,
            [outcome("evt-2", return_pct=0.4), outcome("evt-4", return_pct=-0.5)],
            min_transition_denominator=1,
        )
        transitions = {(row["from_state"], row["to_state"]): row for row in report["transition_matrix"]}
        self.assertIn(("compression", "oi_build_breakout"), transitions)
        self.assertIn(("oi_build_breakout", "oi_unwind"), transitions)
        self.assertEqual(report["input_counts"]["transitions"], 2)

    def test_cli_writes_three_reports(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            write_jsonl(base / "signal_bridge_shadow_signals.jsonl", [signal("evt-1"), signal("evt-2", oi_change_5m_pct=-0.8)])
            write_jsonl(
                base / "signal_bridge_shadow_outcomes.jsonl",
                [outcome("evt-1", return_pct=-0.4), outcome("evt-2", return_pct=0.6)],
            )
            repo = Path(__file__).resolve().parents[1]
            commands = [
                [sys.executable, "phoenix_loss_attribution_report.py", "--input-dir", str(base)],
                [sys.executable, "phoenix_monte_carlo_report.py", "--input-dir", str(base), "--simulations", "20"],
                [sys.executable, "phoenix_markov_state_report.py", "--input-dir", str(base)],
            ]
            for command in commands:
                subprocess.run(command, cwd=repo, check=True, capture_output=True, text=True)
            self.assertTrue((base / "loss_attribution_report.json").exists())
            self.assertTrue((base / "monte_carlo_report.json").exists())
            self.assertTrue((base / "markov_state_report.json").exists())


if __name__ == "__main__":
    unittest.main()
