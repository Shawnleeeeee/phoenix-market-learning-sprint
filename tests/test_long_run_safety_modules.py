from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_agent_research import assert_research_agent_read_only, build_agent_research_config
from phoenix_learning_store import append_learning_row, read_learning_jsonl, write_daily_learning_report
from phoenix_position_manager import EXIT_MODULES, TELEMETRY_FIELDS, build_dynamic_exit_report
from phoenix_strategy_registry import register_strategy, shadow_safe_defaults


class LongRunSafetyModuleTests(unittest.TestCase):
    def test_dynamic_exit_report_covers_all_modules_and_records_excursion_telemetry(self) -> None:
        report = build_dynamic_exit_report(
            {"side": "LONG", "entry_price": 100.0, "opened_at_ms": 0},
            [
                {"timestamp_ms": 1_000, "price": 100.2},
                {"timestamp_ms": 60_000, "price": 101.0, "momentum_score": 0.1},
                {"timestamp_ms": 120_000, "price": 100.7},
                {"timestamp_ms": 180_000, "price": 100.01, "danger_score": 0.9},
                {"timestamp_ms": 901_000, "price": 100.0, "danger_score": 0.9},
            ],
            config={
                "breakeven_trigger_pct": 0.4,
                "breakeven_buffer_pct": 0.02,
                "partial_take_profit_pct": 0.8,
                "trailing_activation_pct": 0.7,
                "trailing_giveback_pct": 0.25,
                "momentum_decay_min_profit_pct": 0.2,
                "momentum_decay_threshold": 0.25,
                "no_follow_through_after_ms": 180_000,
                "no_follow_through_min_mfe_pct": 1.2,
                "danger_score_threshold": 0.8,
                "stop_loss_pct": 0.6,
                "time_decay_after_ms": 900_000,
                "time_decay_min_profit_pct": 0.1,
            },
        )

        self.assertEqual(set(report["covered_exit_modules"]), set(EXIT_MODULES))
        self.assertTrue(set(TELEMETRY_FIELDS).issubset(report.keys()))
        self.assertEqual(report["mfe_pct"], 1.0)
        self.assertEqual(report["mae_pct"], 0.0)
        self.assertEqual(report["time_to_first_profit_ms"], 1_000)
        self.assertEqual(report["time_to_mfe_ms"], 60_000)
        self.assertEqual(report["giveback_pct"], 1.0)
        self.assertEqual(set(report["early_exit_signal_names"]), set(EXIT_MODULES))
        self.assertFalse(report["live_trading_enabled"])

    def test_learning_store_writes_jsonl_and_daily_reports_support_shadow_and_testnet(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            jsonl_path = base / "learning.jsonl"
            append_learning_row(
                jsonl_path,
                {
                    "mode": "shadow",
                    "return_pct": 0.4,
                    "mfe_pct": 0.8,
                    "mae_pct": -0.1,
                    "strategy_proposals": [{"strategy_id": "S1", "change": "raise threshold"}],
                },
            )
            append_learning_row(
                jsonl_path,
                {
                    "mode": "testnet",
                    "return_pct": -0.2,
                    "mfe_pct": 0.3,
                    "mae_pct": -0.5,
                    "strategy_proposals": [{"strategy_id": "S2", "write_config": True}],
                },
            )

            rows = read_learning_jsonl(jsonl_path)
            report = write_daily_learning_report(
                rows=rows,
                output_json=base / "daily_learning_report.json",
                output_md=base / "daily_learning_report.md",
            )
            persisted = json.loads((base / "daily_learning_report.json").read_text(encoding="utf-8"))
            md_text = (base / "daily_learning_report.md").read_text(encoding="utf-8")

            self.assertEqual(len(jsonl_path.read_text(encoding="utf-8").splitlines()), 2)
            self.assertEqual(report["mode_counts"], {"shadow": 1, "testnet": 1})
            self.assertEqual(persisted["mode_counts"], {"shadow": 1, "testnet": 1})
            self.assertFalse(report["direct_config_change_allowed"])
            self.assertFalse(report["live_trading_enabled"])
            self.assertIn("testnet_rows: 1", md_text)
            self.assertTrue(report["strategy_proposals"])
            self.assertTrue(all(item["suggestion_only"] for item in report["strategy_proposals"]))
            self.assertTrue(all(item["direct_config_change_allowed"] is False for item in report["strategy_proposals"]))
            self.assertTrue(all("write_config" not in item for item in report["strategy_proposals"]))

    def test_strategy_registry_defaults_are_shadow_safe(self) -> None:
        defaults = shadow_safe_defaults()
        registered = register_strategy(
            {"live_trading_enabled": True, "promotion_allowed": True},
            strategy_id="REVERSAL_SCALP",
            metadata={"promotion_allowed": True},
        )

        self.assertEqual(defaults["mode"], "shadow")
        self.assertFalse(defaults["live_trading_enabled"])
        self.assertFalse(defaults["promotion_allowed"])
        self.assertFalse(registered["live_trading_enabled"])
        self.assertFalse(registered["promotion_allowed"])
        self.assertFalse(registered["strategies"]["REVERSAL_SCALP"]["promotion_allowed"])
        self.assertFalse(registered["strategies"]["REVERSAL_SCALP"]["live_trading_enabled"])

    def test_agent_research_config_is_disabled_read_only_and_redacts_api_keys(self) -> None:
        config = build_agent_research_config(
            {
                "enabled": True,
                "api_key": "sk-live-secret-value",
                "nested": {"secret": "abcdef123456"},
                "order_writes_allowed": True,
                "trading_setting_writes_allowed": True,
                "fallback_to_code_reports": False,
            }
        )

        self.assertTrue(assert_research_agent_read_only(config))
        self.assertFalse(config["enabled"])
        self.assertTrue(config["read_only"])
        self.assertTrue(config["fallback_to_code_reports"])
        self.assertFalse(config["order_writes_allowed"])
        self.assertFalse(config["trading_setting_writes_allowed"])
        self.assertFalse(config["config_writes_allowed"])
        self.assertIn("REDACTED", config["api_key"])
        self.assertIn("REDACTED", config["nested"]["secret"])


if __name__ == "__main__":
    unittest.main()
