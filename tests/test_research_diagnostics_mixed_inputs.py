from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_loss_attribution_report import build_loss_attribution_report
from phoenix_research_diagnostics import (
    dedupe_independent_events,
    load_mixed_research_outcomes,
    load_testnet_research_outcomes,
)


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def shadow_outcome(
    source_event_id: str,
    *,
    branch: str = "AUTO",
    strategy: str = "breakout",
    return_pct: float = 0.2,
) -> dict[str, object]:
    return {
        "event": "signal_bridge_shadow_horizon_result",
        "event_id": f"{source_event_id}::{branch}:900",
        "source_event_id": source_event_id,
        "shadow_branch_id": branch,
        "strategy_id": strategy,
        "playbook": strategy,
        "symbol": "BTCUSDT",
        "side": "BUY",
        "after_fee_and_slippage_return_pct": return_pct,
        "effective_quote_allocation_usdt": 100.0,
    }


def testnet_trade(
    trade_id: str,
    *,
    strategy: str = "breakout",
    net_pnl_usdt: float = 1.0,
) -> dict[str, object]:
    return {
        "trade_id": trade_id,
        "symbol": "BTCUSDT",
        "side": "BUY",
        "setup": strategy,
        "playbook": strategy,
        "entry_order_id": f"entry-{trade_id}",
        "close_order_id": f"close-{trade_id}",
        "quote_allocation_usdt": 100.0,
        "net_pnl_usdt": net_pnl_usdt,
        "exit_reason": "take_profit" if net_pnl_usdt > 0 else "stop_loss",
    }


class MixedResearchInputTests(unittest.TestCase):
    def test_mixed_loader_counts_shadow_and_testnet_without_collapsing_raw_branches(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            shadow = base / "mainnet_shadow"
            testnet = base / "testnet_live"
            write_jsonl(
                shadow / "signal_bridge_shadow_outcomes.jsonl",
                [
                    shadow_outcome("evt-1", branch="A", return_pct=0.4),
                    shadow_outcome("evt-1", branch="B", return_pct=-0.1),
                ],
            )
            write_jsonl(
                testnet / "round_001_trades.jsonl",
                [testnet_trade("t-1", net_pnl_usdt=2.0), testnet_trade("t-2", net_pnl_usdt=-3.0)],
            )

            mixed = load_mixed_research_outcomes(shadow, testnet_dir=testnet)
            self.assertEqual(mixed["counts"]["mainnet_shadow_outcomes"], 2)
            self.assertEqual(mixed["counts"]["testnet_live_outcomes"], 2)
            self.assertEqual(mixed["counts"]["combined_outcomes"], 4)

            report = build_loss_attribution_report(mixed["outcomes"])
            self.assertEqual(report["deduplication"]["raw_branch_outcomes"], 4)
            self.assertEqual(report["input_counts"]["by_research_source"]["MAINNET_SHADOW"], 2)
            self.assertEqual(report["input_counts"]["by_research_source"]["TESTNET_LIVE"], 2)

    def test_testnet_independent_event_dedup_uses_stable_trade_identity(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trades_file = Path(temp_dir) / "round_001_trades.jsonl"
            duplicate = testnet_trade("same-trade", net_pnl_usdt=1.5)
            write_jsonl(
                trades_file,
                [
                    duplicate,
                    {**duplicate, "net_pnl_usdt": -2.5},
                    testnet_trade("other-trade", net_pnl_usdt=0.8),
                ],
            )

            outcomes = load_testnet_research_outcomes(trades_file=trades_file)
            deduped = dedupe_independent_events(outcomes)
            self.assertEqual(len(outcomes), 3)
            self.assertEqual(len(deduped), 2)
            same = [row for row in deduped if row["source_event_id"].endswith("same-trade")][0]
            self.assertAlmostEqual(same["net_pnl_usdt"], -2.5)

    def test_cli_accepts_testnet_dir_and_reports_cross_validation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            shadow = base / "mainnet_shadow"
            testnet = base / "testnet_live"
            write_jsonl(shadow / "signal_bridge_shadow_signals.jsonl", [])
            write_jsonl(shadow / "signal_bridge_shadow_outcomes.jsonl", [shadow_outcome("evt-1", strategy="breakout", return_pct=0.7)])
            write_jsonl(testnet / "round_001_trades.jsonl", [testnet_trade("t-1", strategy="breakout", net_pnl_usdt=-2.0)])

            repo = Path(__file__).resolve().parents[1]
            commands = [
                [sys.executable, "phoenix_loss_attribution_report.py", "--input-dir", str(shadow), "--testnet-dir", str(testnet)],
                [
                    sys.executable,
                    "phoenix_monte_carlo_report.py",
                    "--input-dir",
                    str(shadow),
                    "--testnet-dir",
                    str(testnet),
                    "--simulations",
                    "10",
                    "--min-independent-events",
                    "1",
                ],
                [sys.executable, "phoenix_markov_state_report.py", "--input-dir", str(shadow), "--testnet-dir", str(testnet)],
            ]
            for command in commands:
                subprocess.run(command, cwd=repo, check=True, capture_output=True, text=True)
            payload = json.loads((shadow / "monte_carlo_report.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["input_counts"]["raw_branch_outcomes"], 2)
            row = payload["metadata"]["strategy_promotion_cross_validation"]["strategies"][0]
            self.assertEqual(row["strategy"], "breakout")
            self.assertEqual(row["classification"], "execution_issue")


if __name__ == "__main__":
    unittest.main()
