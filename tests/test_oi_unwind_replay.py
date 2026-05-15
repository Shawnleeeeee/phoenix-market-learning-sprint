from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_oi_unwind_replay import (
    build_oi_unwind_replay_report,
    build_replay_outcomes,
    build_replay_signal,
    find_oi_unwind_confirmation_from_klines,
)


def snapshot(event_id: str = "evt-oi-unwind") -> dict[str, object]:
    anchor_close_time_ms = 1_777_000_000_000
    return {
        "event": "market_event_created",
        "event_id": event_id,
        "symbol": "ALTUSDT",
        "sample_type": "trigger",
        "bar_interval": "5m",
        "trigger_types": ["volume_burst", "range_expansion"],
        "trading_session": "US",
        "sample": {
            "price": 100.0,
            "anchor_close_time_ms": anchor_close_time_ms,
            "candle_direction": "down",
        },
        "enrichments": {
            "oi_change_5m_pct": -1.1,
            "oi_change_15m_pct": -1.4,
            "depth_imbalance": 0.41,
            "btcusdt_ret_5m_pct": 0.05,
        },
    }


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


class OiUnwindReplayTests(unittest.TestCase):
    def test_find_confirmation_uses_next_1m_opposite_close(self) -> None:
        record = snapshot()
        anchor_close_time_ms = int(record["sample"]["anchor_close_time_ms"])  # type: ignore[index]
        rows = [
            [
                anchor_close_time_ms + 1,
                "100.0",
                "100.8",
                "99.7",
                "100.6",
                "10",
                anchor_close_time_ms + 60_000,
            ],
        ]

        confirmation = find_oi_unwind_confirmation_from_klines(record, rows)

        self.assertIsNotNone(confirmation)
        assert confirmation is not None
        self.assertEqual(confirmation["side"], "BUY")
        self.assertEqual(confirmation["confirmation_source"], "offline_1m_kline")
        self.assertEqual(confirmation["confirmation_candle_direction"], "up")
        self.assertAlmostEqual(float(confirmation["entry_price"]), 100.6, places=6)
        self.assertEqual(confirmation["confirmation_wait_ms"], 60_000)

    def test_build_replay_outcomes_runs_three_shadow_branches(self) -> None:
        record = snapshot()
        confirmation = {
            "side": "BUY",
            "trigger_direction": "down",
            "confirmation_source": "offline_1m_kline",
            "confirmation_candle_direction": "up",
            "confirmation_close_time_ms": 1_777_000_060_000,
            "confirmation_wait_ms": 60_000,
            "entry_price": 100.0,
        }
        path_rows = [
            [1_777_000_060_001, "100.0", "101.1", "99.9", "100.7", "10", 1_777_000_120_000],
        ]

        outcomes = build_replay_outcomes(
            record=record,
            confirmation=confirmation,
            path_candles_by_horizon={180: path_rows},
            horizons_sec=[180],
            round_trip_fee_bps=8.0,
            extra_slippage_penalty_pct=0.0,
            quote_allocation_usdt=10.0,
        )

        self.assertEqual(len(outcomes), 3)
        by_branch = {row["shadow_branch_id"]: row for row in outcomes}
        self.assertEqual(by_branch["OI_UNWIND_REV_050_100"]["exit_reason"], "take_profit_hit")
        self.assertAlmostEqual(
            float(by_branch["OI_UNWIND_REV_050_100"]["after_fee_return_pct"]),
            0.92,
            places=6,
        )
        self.assertEqual(by_branch["OI_UNWIND_REV_050_100"]["branch_type"], "candidate_oi_unwind_reversal")
        self.assertFalse(by_branch["OI_UNWIND_REV_050_100"]["live_unlock_eligible"])

    def test_build_report_keeps_candidate_separate_from_other_shadow_rows(self) -> None:
        record = snapshot()
        signal = build_replay_signal(
            record=record,
            confirmation={
                "side": "BUY",
                "trigger_direction": "down",
                "confirmation_source": "offline_1m_kline",
                "confirmation_candle_direction": "up",
                "confirmation_close_time_ms": 1_777_000_060_000,
                "confirmation_wait_ms": 60_000,
                "entry_price": 100.0,
            },
            horizons_sec=[180],
            quote_allocation_usdt=10.0,
        )
        outcomes = [
            {
                "event": "signal_bridge_shadow_horizon_result",
                "event_id": "evt-oi-unwind::candidate_oi_unwind_reversal",
                "source_event_id": "evt-oi-unwind",
                "symbol": "ALTUSDT",
                "shadow_branch_id": "OI_UNWIND_REV_050_100",
                "branch_type": "candidate_oi_unwind_reversal",
                "research_only": True,
                "side": "BUY",
                "direction_variant": "reversal",
                "horizon_sec": 180,
                "after_fee_and_slippage_return_pct": 0.8,
                "effective_quote_allocation_usdt": 10.0,
            },
            {
                "event": "signal_bridge_shadow_horizon_result",
                "event_id": "evt-oi-unwind::stable_core",
                "source_event_id": "evt-oi-unwind",
                "symbol": "ALTUSDT",
                "shadow_branch_id": "AUTO",
                "branch_type": "stable_core",
                "research_only": False,
                "side": "BUY",
                "horizon_sec": 180,
                "after_fee_and_slippage_return_pct": -99.0,
            },
        ]

        report = build_oi_unwind_replay_report(
            snapshots=[record],
            signals=[signal],
            outcomes=outcomes,
            min_samples=1,
            pair_min_samples=1,
            top_n=5,
        )

        self.assertEqual(report["input_counts"]["candidate_signals"], 1)
        self.assertEqual(report["feature_slice_report"]["input_counts"]["records_built"], 1)
        self.assertEqual(
            report["feature_slice_report"]["overall_by_branch_horizon"][0]["branch_id"],
            "OI_UNWIND_REV_050_100",
        )
        self.assertAlmostEqual(
            report["feature_slice_report"]["overall_by_branch_horizon"][0]["avg_return_pct"],
            0.8,
            places=6,
        )

    def test_cli_can_report_existing_candidate_outcomes_without_network(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            snapshots_file = base / "snapshots.jsonl"
            signals_file = base / "signals.jsonl"
            outcomes_file = base / "outcomes.jsonl"
            output_file = base / "report.json"
            record = snapshot()
            signal = build_replay_signal(
                record=record,
                confirmation={
                    "side": "BUY",
                    "trigger_direction": "down",
                    "confirmation_source": "offline_1m_kline",
                    "confirmation_candle_direction": "up",
                    "confirmation_close_time_ms": 1_777_000_060_000,
                    "confirmation_wait_ms": 60_000,
                    "entry_price": 100.0,
                },
                horizons_sec=[180],
                quote_allocation_usdt=10.0,
            )
            write_jsonl(snapshots_file, [record])
            write_jsonl(signals_file, [signal])
            write_jsonl(
                outcomes_file,
                [
                    {
                        "event": "signal_bridge_shadow_horizon_result",
                        "event_id": "evt-oi-unwind::candidate_oi_unwind_reversal",
                        "source_event_id": "evt-oi-unwind",
                        "symbol": "ALTUSDT",
                        "shadow_branch_id": "OI_UNWIND_REV_035_070",
                        "branch_type": "candidate_oi_unwind_reversal",
                        "research_only": True,
                        "side": "BUY",
                        "direction_variant": "reversal",
                        "horizon_sec": 180,
                        "after_fee_and_slippage_return_pct": 0.5,
                        "effective_quote_allocation_usdt": 10.0,
                    }
                ],
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_oi_unwind_replay.py",
                    "--snapshots-file",
                    str(snapshots_file),
                    "--existing-signals-file",
                    str(signals_file),
                    "--existing-outcomes-file",
                    str(outcomes_file),
                    "--output-json",
                    str(output_file),
                    "--report-only",
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

            self.assertIn("candidate_signals=1", result.stdout)
            report = json.loads(output_file.read_text(encoding="utf-8"))
            self.assertEqual(report["input_counts"]["candidate_outcomes"], 1)


if __name__ == "__main__":
    unittest.main()
