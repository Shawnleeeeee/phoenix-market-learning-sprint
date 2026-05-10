from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_strategy_discovery import build_strategy_discovery_report


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def snapshot(
    event_id: str,
    *,
    oi_change_5m_pct: float,
    btcusdt_ret_5m_pct: float = 0.05,
    depth_imbalance: float = 0.42,
    trading_session: str = "US",
) -> dict[str, object]:
    return {
        "event": "market_event_created",
        "event_id": event_id,
        "symbol": "ALTUSDT",
        "sample_type": "trigger",
        "bar_interval": "1m",
        "trading_session": trading_session,
        "trigger_types": ["volume_burst", "range_expansion"],
        "sample": {
            "sample_type": "trigger",
            "price": 100.0,
            "candle_direction": "down",
            "trigger_candle_direction": "down",
            "volume_burst_ratio": 2.4,
            "range_to_atr": 2.2,
        },
        "enrichments": {
            "oi_change_5m_pct": oi_change_5m_pct,
            "oi_change_15m_pct": -1.2,
            "btcusdt_ret_5m_pct": btcusdt_ret_5m_pct,
            "depth_imbalance": depth_imbalance,
            "liquidation_long_usd_15m": 80_000.0,
            "liquidation_short_usd_15m": 2_000.0,
            "liquidation_event_count_15m": 5,
        },
        "context_3m": {
            "volume_burst_ratio": 2.2,
            "range_to_atr": 1.7,
            "ret_1bar_pct": -0.25,
        },
    }


def outcome(
    event_id: str,
    *,
    return_pct: float,
    branch_id: str = "REVERSAL_SCALP_050_100",
    side: str = "BUY",
    horizon_sec: int = 300,
) -> dict[str, object]:
    return {
        "event": "signal_bridge_shadow_horizon_result",
        "event_id": f"{event_id}::research_pool",
        "source_event_id": event_id,
        "symbol": "ALTUSDT",
        "branch_type": "research_pool",
        "research_only": True,
        "shadow_branch_id": branch_id,
        "direction_variant": "reversal",
        "side": side,
        "horizon_sec": horizon_sec,
        "after_fee_and_slippage_return_pct": return_pct,
        "effective_quote_allocation_usdt": 10.0,
    }


class StrategyDiscoveryTests(unittest.TestCase):
    def test_discovers_validated_positive_condition_combo(self) -> None:
        snapshots = []
        outcomes = []
        for index, return_pct in enumerate([0.8, 0.6, 0.7, 0.5, 0.4, 0.3], start=1):
            event_id = f"winner-{index}"
            snapshots.append(snapshot(event_id, oi_change_5m_pct=-1.0, btcusdt_ret_5m_pct=0.05))
            outcomes.append(outcome(event_id, return_pct=return_pct))
        for index, return_pct in enumerate([-0.5, -0.4, 0.1], start=1):
            event_id = f"loser-{index}"
            snapshots.append(snapshot(event_id, oi_change_5m_pct=1.2, btcusdt_ret_5m_pct=-1.2))
            outcomes.append(outcome(event_id, return_pct=return_pct))

        report = build_strategy_discovery_report(
            snapshots,
            outcomes,
            branch_type="research_pool",
            min_samples=4,
            min_validation_samples=2,
            max_degree=2,
            min_profit_factor=1.3,
            validation_min_profit_factor=1.1,
            top_n=10,
        )

        self.assertGreaterEqual(report["input_counts"]["records_built"], 9)
        self.assertTrue(report["qualified_candidates"])
        top = report["qualified_candidates"][0]
        self.assertEqual(top["branch_id"], "REVERSAL_SCALP_050_100")
        self.assertEqual(top["horizon_sec"], 300)
        self.assertGreaterEqual(top["sample_count"], 6)
        condition_pairs = {(item["feature"], item["bucket"]) for item in top["conditions"]}
        self.assertIn(("oi_5m_regime", "oi_unwind_drop_-2..-0.5"), condition_pairs)
        self.assertTrue(top["passes"]["validation_profit_factor_ok"])

    def test_rejects_candidate_when_validation_tail_is_negative(self) -> None:
        snapshots = []
        outcomes = []
        for index, return_pct in enumerate([1.0, 0.9, 0.8, 0.7, -1.4, -1.3], start=1):
            event_id = f"decay-{index}"
            snapshots.append(snapshot(event_id, oi_change_5m_pct=-1.0, btcusdt_ret_5m_pct=0.05))
            outcomes.append(outcome(event_id, return_pct=return_pct))

        report = build_strategy_discovery_report(
            snapshots,
            outcomes,
            branch_type="research_pool",
            min_samples=4,
            min_validation_samples=2,
            max_degree=1,
            min_profit_factor=1.1,
            validation_min_profit_factor=1.1,
            top_n=10,
        )

        self.assertFalse(report["qualified_candidates"])
        self.assertTrue(report["near_miss_candidates"])
        self.assertFalse(report["near_miss_candidates"][0]["passes"]["validation_avg_return_ok"])

    def test_cli_writes_discovery_json(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            snapshots_file = base / "snapshots.jsonl"
            outcomes_file = base / "outcomes.jsonl"
            output_file = base / "strategy_discovery.json"
            snapshots = [
                snapshot(f"evt-{index}", oi_change_5m_pct=-1.0, btcusdt_ret_5m_pct=0.05)
                for index in range(1, 6)
            ]
            outcomes = [
                outcome(f"evt-{index}", return_pct=value)
                for index, value in enumerate([0.8, 0.7, 0.6, 0.5, 0.4], start=1)
            ]
            write_jsonl(snapshots_file, snapshots)
            write_jsonl(outcomes_file, outcomes)

            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_strategy_discovery.py",
                    "--snapshots-file",
                    str(snapshots_file),
                    "--outcomes-file",
                    str(outcomes_file),
                    "--output-json",
                    str(output_file),
                    "--min-samples",
                    "3",
                    "--min-validation-samples",
                    "1",
                    "--max-degree",
                    "2",
                    "--top",
                    "5",
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("qualified=", result.stdout)
            report = json.loads(output_file.read_text(encoding="utf-8"))
            self.assertTrue(report["qualified_candidates"])


if __name__ == "__main__":
    unittest.main()
