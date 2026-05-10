from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_feature_slice_report import build_feature_slice_report


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def snapshot(event_id: str, *, bar_interval: str, oi_change_5m_pct: float) -> dict[str, object]:
    return {
        "event": "market_event_created",
        "event_id": event_id,
        "symbol": "BTCUSDT",
        "sample_type": "trigger",
        "bar_interval": bar_interval,
        "trading_session": "US",
        "trigger_types": ["volume_burst", "range_expansion"],
        "trigger_score": 2.5,
        "sample": {
            "sample_type": "trigger",
            "price": 100.0,
            "quote_volume_24h": 150_000_000.0,
            "price_change_24h_pct": -1.2,
            "candle_direction": "down",
            "trigger_candle_direction": "down",
            "confirmation_candle_direction": "up",
            "volume_burst_ratio": 3.4,
            "range_to_atr": 2.2,
            "body_to_atr": 1.6,
            "ret_1bar_pct": -0.7,
            "ret_5bar_pct": -1.1,
        },
        "enrichments": {
            "derivatives_data_source": "ws",
            "oi_change_5m_pct": oi_change_5m_pct,
            "oi_change_15m_pct": -1.1,
            "funding_rate": 0.0001,
            "depth_imbalance": -0.42,
            "btcusdt_ret_5m_pct": -0.2,
            "liquidation_long_usd_15m": 80_000.0,
            "liquidation_short_usd_15m": 0.0,
            "liquidation_event_count_15m": 4,
        },
        "research_contexts": {
            "3m": {
                "volume_burst_ratio": 2.1,
                "range_to_atr": 1.7,
                "ret_1bar_pct": -0.4,
            }
        },
    }


def outcome(
    source_event_id: str,
    *,
    branch_id: str,
    horizon_sec: int,
    return_pct: float,
    direction_variant: str = "reversal",
    side: str = "BUY",
) -> dict[str, object]:
    return {
        "event": "signal_bridge_shadow_horizon_result",
        "event_id": f"{source_event_id}::research_pool",
        "source_event_id": source_event_id,
        "symbol": "BTCUSDT",
        "shadow_branch_id": branch_id,
        "branch_type": "research_pool",
        "research_only": True,
        "side": side,
        "direction_variant": direction_variant,
        "horizon_sec": horizon_sec,
        "after_fee_and_slippage_return_pct": return_pct,
        "effective_quote_allocation_usdt": 100.0,
    }


class FeatureSliceReportTests(unittest.TestCase):
    def test_build_report_joins_snapshots_and_finds_pair_slice(self) -> None:
        snapshots = [
            snapshot("evt-1", bar_interval="5m", oi_change_5m_pct=-1.1),
            snapshot("evt-2", bar_interval="5m", oi_change_5m_pct=-1.6),
            snapshot("evt-3", bar_interval="1m", oi_change_5m_pct=1.2),
        ]
        outcomes = [
            outcome("evt-1", branch_id="REVERSAL_SCALP_050_100", horizon_sec=180, return_pct=0.8),
            outcome("evt-2", branch_id="REVERSAL_SCALP_050_100", horizon_sec=180, return_pct=0.4),
            outcome("evt-3", branch_id="REVERSAL_SCALP_050_100", horizon_sec=180, return_pct=-0.7),
            outcome("evt-1", branch_id="LEGACY", horizon_sec=180, return_pct=99.0),
            outcome("evt-1", branch_id="REVERSAL_SCALP_050_100", horizon_sec=180, return_pct=99.0, side=""),
        ]

        report = build_feature_slice_report(
            snapshots,
            outcomes,
            min_samples=2,
            pair_min_samples=2,
            top_n=100,
        )

        self.assertEqual(report["input_counts"]["snapshots_indexed"], 3)
        self.assertEqual(report["input_counts"]["outcomes_seen"], 5)
        self.assertEqual(report["input_counts"]["outcomes_in_scope"], 3)
        self.assertEqual(report["input_counts"]["records_built"], 3)

        matching_pairs = [
            row
            for row in report["top_pair_feature_slices"]
            if {
                (row["feature_a"], row["bucket_a"]),
                (row["feature_b"], row["bucket_b"]),
            }
            == {("bar_interval", "5m"), ("oi_change_5m_pct", "-2..-1")}
        ]
        self.assertEqual(len(matching_pairs), 1)
        top_pair = matching_pairs[0]
        self.assertEqual(top_pair["branch_id"], "REVERSAL_SCALP_050_100")
        self.assertEqual(top_pair["horizon_sec"], 180)
        self.assertEqual(top_pair["sample_count"], 2)
        self.assertAlmostEqual(top_pair["avg_return_pct"], 0.6, places=6)
        self.assertAlmostEqual(top_pair["win_rate_pct"], 100.0, places=6)
        regime_pairs = [
            row
            for row in report["top_pair_feature_slices"]
            if {
                (row["feature_a"], row["bucket_a"]),
                (row["feature_b"], row["bucket_b"]),
            }
            == {("bar_interval", "5m"), ("oi_5m_regime", "oi_unwind_drop_-2..-0.5")}
        ]
        self.assertEqual(len(regime_pairs), 1)
        factor_slices = [
            row
            for row in report["top_single_feature_slices"]
            if row.get("feature") == "mean_reversion_bucket" and row.get("bucket", "").startswith("reversion_long")
        ]
        self.assertTrue(factor_slices)

    def test_cli_writes_feature_slice_json(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            snapshots_file = base / "snapshots.jsonl"
            outcomes_file = base / "outcomes.jsonl"
            output_file = base / "feature_report.json"
            write_jsonl(
                snapshots_file,
                [
                    snapshot("evt-1", bar_interval="5m", oi_change_5m_pct=-1.0),
                    snapshot("evt-2", bar_interval="5m", oi_change_5m_pct=-1.5),
                ],
            )
            write_jsonl(
                outcomes_file,
                [
                    outcome("evt-1", branch_id="REVERSAL_SCALP_050_100", horizon_sec=300, return_pct=1.2),
                    outcome("evt-2", branch_id="REVERSAL_SCALP_050_100", horizon_sec=300, return_pct=-0.2),
                ],
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_feature_slice_report.py",
                    "--snapshots-file",
                    str(snapshots_file),
                    "--outcomes-file",
                    str(outcomes_file),
                    "--output-json",
                    str(output_file),
                    "--min-samples",
                    "2",
                    "--pair-min-samples",
                    "2",
                    "--top",
                    "5",
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("records_built=2", result.stdout)
            report = json.loads(output_file.read_text(encoding="utf-8"))
            self.assertEqual(report["input_counts"]["records_built"], 2)
            self.assertTrue(report["top_pair_feature_slices"])


if __name__ == "__main__":
    unittest.main()
