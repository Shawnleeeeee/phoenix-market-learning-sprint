from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_factor_factory import build_factor_factory_report
from phoenix_historical_research_replay import (
    build_historical_research_rows,
    build_survivor_rules_report,
    build_walk_forward_oos_report,
    derive_attribution_candidate_rules,
    evaluate_candidate_rules,
    parse_horizon_seconds,
)


def candle(index: int, *, close_price: float | None = None, volume: float = 10.0) -> list[object]:
    open_time_ms = index * 60_000
    open_price = 100.0 + (index * 0.01)
    close = close_price if close_price is not None else open_price + 0.01
    high = max(open_price, close) + 0.02
    low = min(open_price, close) - 0.02
    return [
        open_time_ms,
        str(open_price),
        str(high),
        str(low),
        str(close),
        str(volume),
        open_time_ms + 59_999,
        str(close * volume),
        "10",
        "5",
        str(close * volume * 0.5),
        "0",
    ]


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


class HistoricalResearchReplayTests(unittest.TestCase):
    def test_parse_horizon_seconds_supports_mixed_units(self) -> None:
        self.assertEqual(parse_horizon_seconds("1m,5m,15m,1h"), [60, 300, 900, 3600])

    def test_build_rows_outputs_factor_factory_compatible_records(self) -> None:
        candles = [candle(index) for index in range(72)]
        candles[55] = candle(55, close_price=103.0, volume=80.0)
        for index in range(56, 72):
            candles[index] = candle(index, close_price=103.0 + ((index - 55) * 0.08), volume=18.0)
        oi_rows = [
            {"timestamp": index * 60_000, "sumOpenInterest": str(1000 + index)}
            for index in range(0, 72, 5)
        ]

        rows = build_historical_research_rows(
            symbol="BTCUSDT",
            candles=candles,
            oi_rows=oi_rows,
            interval="1m",
            horizons_sec=[60, 300, 900],
            round_trip_fee_bps=8.0,
            quote_allocation_usdt=10.0,
            min_quote_volume=0.0,
            rolling_24h_bars=30,
        )

        self.assertTrue(rows.event_samples)
        self.assertEqual(len(rows.factor_vectors), len(rows.event_samples))
        self.assertEqual(len(rows.labeled_outcomes), len(rows.event_samples) * 3 * 2)

        sample = rows.event_samples[0]
        self.assertEqual(sample["event"], "market_event_created")
        self.assertTrue(sample["research_only"])
        self.assertEqual(sample["branch_type"], "research_pool")
        self.assertIn("factors", sample)
        self.assertIn("trend_score", sample["factors"])

        outcome = rows.labeled_outcomes[0]
        self.assertEqual(outcome["event"], "signal_bridge_shadow_horizon_result")
        self.assertEqual(outcome["source_event_id"], sample["event_id"])
        self.assertIn(outcome["side"], {"BUY", "SELL"})
        self.assertIn(outcome["horizon_sec"], {60, 300, 900})
        self.assertIn("after_fee_and_slippage_return_pct", outcome)

        report = build_factor_factory_report(
            snapshots=rows.event_samples,
            outcomes=rows.labeled_outcomes,
            min_samples=1,
            pair_min_samples=1,
            top_n=5,
        )
        self.assertGreater(report["input_counts"]["rule_evaluation_rows"], 0)

    def test_walk_forward_uses_prior_events_for_train_and_later_events_for_test(self) -> None:
        snapshots = []
        outcomes = []
        for index in range(12):
            event_id = f"evt-{index}"
            snapshots.append(
                {
                    "event": "market_event_created",
                    "event_id": event_id,
                    "symbol": "BTCUSDT",
                    "observed_at_ms": index * 60_000,
                    "bar_interval": "1m",
                    "trading_session": "US",
                    "sample": {
                        "trigger_candle_direction": "up",
                        "candle_direction": "up",
                        "volume_burst_ratio": 3.0,
                        "range_to_atr": 2.0,
                        "body_to_atr": 1.4,
                        "ret_1bar_pct": 1.2,
                        "ret_5bar_pct": 1.5,
                        "ret_15bar_pct": 2.0,
                    },
                    "enrichments": {
                        "oi_change_5m_pct": 1.0,
                        "oi_change_15m_pct": 2.0,
                        "depth_imbalance": 0.4,
                        "spread_bps": 2.0,
                        "estimated_slippage_bps": 3.0,
                        "btcusdt_ret_5m_pct": 0.2,
                    },
                }
            )
            outcomes.append(
                {
                    "event": "signal_bridge_shadow_horizon_result",
                    "event_id": f"{event_id}::HIST_BUY_1M",
                    "shadow_instance_id": f"{event_id}::HIST_BUY_1M",
                    "source_event_id": event_id,
                    "shadow_branch_id": "HIST_BUY_1M",
                    "branch_type": "research_pool",
                    "research_only": True,
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "horizon_sec": 60,
                    "after_fee_and_slippage_return_pct": 0.4,
                    "effective_quote_allocation_usdt": 10.0,
                }
            )

        report = build_walk_forward_oos_report(
            snapshots,
            outcomes,
            fold_count=3,
            min_train_events=4,
            min_samples=1,
            pair_min_samples=1,
            top_n=3,
        )

        self.assertGreaterEqual(report["fold_count"], 1)
        first_fold = report["folds"][0]
        self.assertLess(first_fold["train_end_ms"], first_fold["test_start_ms"])
        self.assertTrue(first_fold["selected_rules"])
        self.assertTrue(first_fold["selected_attribution_rules"])
        self.assertGreater(first_fold["test_summary"]["sample_count"], 0)
        self.assertGreater(first_fold["attribution_candidate_summary"]["sample_count"], 0)

    def test_attribution_slices_become_candidate_rules_and_match_oos_rows(self) -> None:
        attribution = {
            "factor_attribution": {
                "top_pair_factor_slices": [
                    {
                        "branch_id": "HIST_SELL_5M",
                        "horizon_sec": 300,
                        "feature_a": "trading_session",
                        "bucket_a": "offhours",
                        "feature_b": "trend_bucket",
                        "bucket_b": "trend_long_mild",
                        "sample_count": 42,
                        "avg_return_pct": 0.24,
                        "profit_factor": 2.1,
                    }
                ],
                "top_single_factor_slices": [
                    {
                        "branch_id": "HIST_SELL_5M",
                        "horizon_sec": 300,
                        "feature": "momentum_bucket",
                        "bucket": "momentum_long_strong",
                        "sample_count": 35,
                        "avg_return_pct": 0.18,
                        "profit_factor": 1.7,
                    }
                ],
            }
        }

        candidates = derive_attribution_candidate_rules(
            attribution,
            min_samples=30,
            min_profit_factor=1.2,
            min_avg_return_pct=0.0,
            top_n=5,
        )

        self.assertEqual(candidates[0]["rule_type"], "pair_slice")
        self.assertEqual(candidates[0]["branch_id"], "HIST_SELL_5M")
        self.assertEqual(len(candidates[0]["conditions"]), 2)

        snapshots = [
            {
                "event": "market_event_created",
                "event_id": "evt-oos",
                "symbol": "BTCUSDT",
                "observed_at_ms": 1,
                "bar_interval": "1m",
                "trading_session": "offhours",
                "sample": {
                    "trigger_candle_direction": "up",
                    "candle_direction": "up",
                    "volume_burst_ratio": 1.5,
                    "range_to_atr": 1.4,
                    "body_to_atr": 1.2,
                    "ret_1bar_pct": 0.2,
                    "ret_5bar_pct": 0.3,
                    "ret_15bar_pct": 0.3,
                },
                "enrichments": {
                    "oi_change_5m_pct": 0.2,
                    "oi_change_15m_pct": 0.2,
                    "depth_imbalance": 0.0,
                    "spread_bps": 2.0,
                    "estimated_slippage_bps": 3.0,
                    "btcusdt_ret_5m_pct": 0.0,
                },
                "factors": {
                    "trend_bucket": "trend_long_mild",
                    "momentum_bucket": "momentum_long_strong",
                },
            }
        ]
        outcomes = [
            {
                "event": "signal_bridge_shadow_horizon_result",
                "event_id": "evt-oos::HIST_SELL_5M",
                "source_event_id": "evt-oos",
                "shadow_branch_id": "HIST_SELL_5M",
                "branch_type": "research_pool",
                "research_only": True,
                "symbol": "BTCUSDT",
                "side": "SELL",
                "horizon_sec": 300,
                "after_fee_and_slippage_return_pct": 0.31,
                "effective_quote_allocation_usdt": 10.0,
            }
        ]

        result = evaluate_candidate_rules(snapshots, outcomes, candidates)

        self.assertEqual(result["summary"]["sample_count"], 2)
        self.assertEqual(result["rules"][0]["test_summary"]["sample_count"], 1)

    def test_survivor_report_splits_passed_and_rejected_candidate_rules(self) -> None:
        good_rule = {
            "rule_id": "ATTR_GOOD",
            "rule_type": "pair_slice",
            "branch_id": "HIST_BUY_5M",
            "horizon_sec": 300,
            "conditions": [{"feature": "trading_session", "bucket": "US"}],
            "test_summary": {
                "sample_count": 60,
                "avg_return_pct": 0.08,
                "profit_factor": 1.4,
                "win_rate_pct": 55.0,
            },
            "concentration": {
                "max_symbol_share_pct": 40.0,
                "max_session_share_pct": 60.0,
                "max_day_share_pct": 25.0,
            },
        }
        weak_rule = {
            "rule_id": "ATTR_WEAK",
            "rule_type": "pair_slice",
            "branch_id": "HIST_SELL_5M",
            "horizon_sec": 300,
            "conditions": [{"feature": "trading_session", "bucket": "offhours"}],
            "test_summary": {
                "sample_count": 60,
                "avg_return_pct": -0.03,
                "profit_factor": 0.8,
                "win_rate_pct": 45.0,
            },
            "concentration": {
                "max_symbol_share_pct": 95.0,
                "max_session_share_pct": 100.0,
                "max_day_share_pct": 90.0,
            },
        }
        walk_forward = {
            "folds": [
                {
                    "fold_index": 1,
                    "attribution_candidate_rule_results": [
                        good_rule,
                        weak_rule,
                    ],
                },
                {
                    "fold_index": 2,
                    "attribution_candidate_rule_results": [
                        {
                            **good_rule,
                            "test_summary": {
                                **good_rule["test_summary"],
                                "sample_count": 50,
                                "avg_return_pct": 0.04,
                                "profit_factor": 1.25,
                            },
                        },
                        {
                            **weak_rule,
                            "test_summary": {
                                **weak_rule["test_summary"],
                                "sample_count": 50,
                                "avg_return_pct": 0.02,
                                "profit_factor": 1.1,
                            },
                        },
                    ],
                },
            ]
        }

        report = build_survivor_rules_report(
            walk_forward,
            min_oos_samples=100,
            min_profit_factor=1.2,
            min_avg_return_pct=0.0,
            min_positive_folds=2,
            min_tested_folds=2,
            max_symbol_share_pct=70.0,
            max_session_share_pct=80.0,
            max_day_share_pct=60.0,
        )

        self.assertFalse(report["live_trading_enabled"])
        self.assertEqual(report["survivor_count"], 1)
        self.assertEqual(report["rejected_count"], 1)
        self.assertEqual(report["survivor_rules"][0]["rule_id"], "ATTR_GOOD")
        rejected = report["rejected_rules"][0]
        self.assertEqual(rejected["rule_id"], "ATTR_WEAK")
        self.assertIn("oos_profit_factor_below_gate", rejected["rejection_reasons"])
        self.assertIn("symbol_concentration_too_high", rejected["rejection_reasons"])

    def test_cli_writes_survivor_gate_from_existing_walk_forward_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            walk_forward_path = base / "walk_forward_oos_report.json"
            output_dir = base / "gate"
            walk_forward_path.write_text(
                json.dumps(
                    {
                        "folds": [
                            {
                                "fold_index": 1,
                                "attribution_candidate_rule_results": [
                                    {
                                        "rule_id": "ATTR_GOOD",
                                        "rule_type": "single_slice",
                                        "branch_id": "HIST_BUY_5M",
                                        "horizon_sec": 300,
                                        "conditions": [{"feature": "trading_session", "bucket": "US"}],
                                        "test_summary": {
                                            "sample_count": 70,
                                            "win_count": 42,
                                            "loss_count": 28,
                                            "avg_return_pct": 0.09,
                                            "total_return_pct": 6.3,
                                            "profit_factor": 1.45,
                                        },
                                        "concentration": {
                                            "max_symbol_share_pct": 45.0,
                                            "max_session_share_pct": 65.0,
                                            "max_day_share_pct": 30.0,
                                        },
                                    }
                                ],
                            },
                            {
                                "fold_index": 2,
                                "attribution_candidate_rule_results": [
                                    {
                                        "rule_id": "ATTR_GOOD",
                                        "rule_type": "single_slice",
                                        "branch_id": "HIST_BUY_5M",
                                        "horizon_sec": 300,
                                        "conditions": [{"feature": "trading_session", "bucket": "US"}],
                                        "test_summary": {
                                            "sample_count": 70,
                                            "win_count": 40,
                                            "loss_count": 30,
                                            "avg_return_pct": 0.07,
                                            "total_return_pct": 4.9,
                                            "profit_factor": 1.3,
                                        },
                                        "concentration": {
                                            "max_symbol_share_pct": 50.0,
                                            "max_session_share_pct": 65.0,
                                            "max_day_share_pct": 35.0,
                                        },
                                    }
                                ],
                            },
                        ]
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_historical_research_replay.py",
                    "--input-walk-forward-json",
                    str(walk_forward_path),
                    "--output-dir",
                    str(output_dir),
                    "--survivor-min-oos-samples",
                    "100",
                    "--survivor-min-positive-folds",
                    "2",
                    "--survivor-min-tested-folds",
                    "2",
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("survivor_gate_completed", result.stdout)
            survivor_report = json.loads((output_dir / "survivor_rules_report.json").read_text(encoding="utf-8"))
            rejected_report = json.loads((output_dir / "rejected_rules_report.json").read_text(encoding="utf-8"))
            self.assertEqual(survivor_report["survivor_count"], 1)
            self.assertEqual(rejected_report["rejected_count"], 0)
            self.assertTrue((output_dir / "survivor_gate_manifest.json").exists())

    def test_cli_writes_all_research_artifacts_from_existing_json_inputs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            candles_path = base / "candles.jsonl"
            oi_path = base / "oi.jsonl"
            output_dir = base / "out"
            candles = [candle(index) for index in range(72)]
            candles[55] = candle(55, close_price=103.0, volume=80.0)
            for index in range(56, 72):
                candles[index] = candle(index, close_price=103.0 + ((index - 55) * 0.08), volume=18.0)
            write_jsonl(
                candles_path,
                [
                    {
                        "open_time": row[0],
                        "open": row[1],
                        "high": row[2],
                        "low": row[3],
                        "close": row[4],
                        "volume": row[5],
                        "close_time": row[6],
                        "quote_asset_volume": row[7],
                        "number_of_trades": row[8],
                        "taker_buy_base_volume": row[9],
                        "taker_buy_quote_volume": row[10],
                        "ignore": row[11],
                    }
                    for row in candles
                ],
            )
            write_jsonl(
                oi_path,
                [{"timestamp": index * 60_000, "sumOpenInterest": str(1000 + index)} for index in range(0, 72, 5)],
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "phoenix_historical_research_replay.py",
                    "--symbol",
                    "BTCUSDT",
                    "--input-klines-jsonl",
                    str(candles_path),
                    "--input-oi-jsonl",
                    str(oi_path),
                    "--output-dir",
                    str(output_dir),
                    "--horizons",
                    "1m,5m,15m",
                    "--min-quote-volume",
                    "0",
                    "--rolling-24h-bars",
                    "30",
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

            self.assertIn("historical_research_replay_completed", result.stdout)
            for name in (
                "historical_event_samples.jsonl",
                "historical_factor_vectors.jsonl",
                "historical_labeled_outcomes.jsonl",
                "historical_attribution_report.json",
                "attribution_candidate_rules_report.json",
                "walk_forward_oos_report.json",
                "survivor_rules_report.json",
                "rejected_rules_report.json",
            ):
                self.assertTrue((output_dir / name).exists(), name)

            derived_dir = base / "derived"
            derived = subprocess.run(
                [
                    sys.executable,
                    "phoenix_historical_research_replay.py",
                    "--input-event-samples-jsonl",
                    str(output_dir / "historical_event_samples.jsonl"),
                    "--input-labeled-outcomes-jsonl",
                    str(output_dir / "historical_labeled_outcomes.jsonl"),
                    "--output-dir",
                    str(derived_dir),
                    "--min-samples",
                    "1",
                    "--pair-min-samples",
                    "1",
                    "--min-train-events",
                    "1",
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertIn("historical_research_replay_completed", derived.stdout)
            self.assertTrue((derived_dir / "attribution_candidate_rules_report.json").exists())
            self.assertTrue((derived_dir / "walk_forward_oos_report.json").exists())
            self.assertTrue((derived_dir / "survivor_rules_report.json").exists())
            self.assertTrue((derived_dir / "rejected_rules_report.json").exists())


if __name__ == "__main__":
    unittest.main()
