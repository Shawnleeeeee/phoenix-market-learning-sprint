from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_entry_parity_checker import evaluate_entry_parity
from phoenix_experiment_tracking import build_experiment_manifest, write_experiment_scaffold
from phoenix_oi_strategy_framework import OI_REQUIRED_FEATURES, build_oi_shadow_candidate
from phoenix_strategy_registry import OLD_STRATEGY_IDS, build_strategy_gate_decision, validate_strategy_manifest


def manifest_payload(strategy_id: str, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "strategy_id": strategy_id,
        "version": "v1",
        "family": "unit_test",
        "status": "limited_testnet",
        "market_hypothesis": "A specific forced-flow structure can continue after confirmation.",
        "edge_source": "forced_flow_follow_through",
        "who_loses_money": "late forced buyers or sellers crossing the spread",
        "trigger_event": "unit_test_trigger",
        "entry_conditions": ["entry confirmation is present"],
        "invalidation_conditions": ["follow through fails"],
        "exit_logic": ["time stop"],
        "allowed_regimes": ["trend"],
        "blocked_regimes": ["chop"],
        "allowed_liquidity_buckets": ["top"],
        "blocked_symbols": [],
        "expected_holding_horizon_sec": [60, 300],
        "expected_gross_move_bps": 15.0,
        "max_expected_total_cost_bps": 4.0,
        "min_expected_net_edge_bps": 3.0,
        "min_cost_to_edge_ratio": 3.0,
        "evidence_level": 3,
        "shadow_evidence": {
            "outcomes": 80,
            "avg_net_edge_bps": 2.5,
            "profit_factor": 1.4,
            "max_symbol_concentration_pct": 25.0,
        },
        "replay_evidence": {"validated": True},
        "testnet_evidence": {},
        "old_strategy_similarity": {"is_similar": False, "similar_to": [], "change_type": "new_hypothesis"},
        "created_at": "2026-05-08T00:00:00+00:00",
        "updated_at": "2026-05-08T00:00:00+00:00",
    }
    payload.update(overrides)
    return payload


def write_manifest(root: Path, payload: dict[str, object]) -> None:
    manifest_dir = root / "strategy_manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / f"{payload['strategy_id']}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


class StrategyEvidenceFrameworkTests(unittest.TestCase):
    def test_without_manifest_strategy_is_rejected_for_testnet(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)

            decision = build_strategy_gate_decision(
                {"setup": "new_alpha", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "new_alpha", "strategy_id": "new_alpha"},
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(decision["decision"], "block")
        self.assertIn("blocked_missing_strategy_manifest", decision["block_reasons"])

    def test_old_strategy_freeze_blocks_testnet_candidate(self) -> None:
        self.assertIn("volatility_long", OLD_STRATEGY_IDS)
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(root, manifest_payload("volatility_long"))

            decision = build_strategy_gate_decision(
                {"setup": "volatility_long", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "volatility_long", "strategy_id": "volatility_long"},
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(decision["decision"], "block")
        self.assertIn("blocked_by_old_strategy_freeze", decision["block_reasons"])

    def test_old_strategy_threshold_tweak_cannot_validate_as_new_strategy(self) -> None:
        report = validate_strategy_manifest(
            manifest_payload(
                "fake_new_volatility",
                old_strategy_similarity={
                    "is_similar": True,
                    "similar_to": ["volatility_long"],
                    "change_type": "threshold_tweak",
                },
            )
        )

        self.assertFalse(report["valid"])
        self.assertIn("old_strategy_similarity_not_new_strategy", report["errors"])

    def test_negative_expected_net_edge_is_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(
                root,
                manifest_payload(
                    "new_alpha",
                    expected_gross_move_bps=5.0,
                    max_expected_total_cost_bps=6.0,
                ),
            )

            decision = build_strategy_gate_decision(
                {"setup": "new_alpha", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "new_alpha", "strategy_id": "new_alpha"},
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(decision["decision"], "block")
        self.assertIn("blocked_negative_expected_net_edge", decision["block_reasons"])

    def test_candidate_strategy_manifest_id_is_used_before_experiment_for_testnet(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(
                root,
                manifest_payload(
                    "new_alpha",
                    status="shadow_only",
                    evidence_level=0,
                    expected_gross_move_bps=None,
                    max_expected_total_cost_bps=None,
                    min_expected_net_edge_bps=None,
                ),
            )

            decision = build_strategy_gate_decision(
                {
                    "setup": "new_alpha",
                    "strategy_manifest_id": "new_alpha",
                    "strategy_family": "unit_test",
                    "experiment_id": "exp_candidate_manifest",
                    "evidence_level": 0,
                    "symbol": "BTCUSDT",
                    "expected_gross_move_bps": 10.0,
                    "expected_total_cost_bps": 3.0,
                    "expected_net_edge_bps": 7.0,
                },
                experiment=None,
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(decision["strategy_manifest_id"], "new_alpha")
        self.assertNotIn("blocked_missing_strategy_manifest_id", decision["block_reasons"])
        self.assertIn("blocked_manifest_status_not_testnet", decision["block_reasons"])
        self.assertIn("blocked_evidence_level_too_low", decision["block_reasons"])

    def test_candidate_setup_can_map_to_manifest_when_explicit_manifest_id_is_absent(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(root, manifest_payload("new_alpha"))

            decision = build_strategy_gate_decision(
                {
                    "setup": "new_alpha",
                    "strategy_family": "unit_test",
                    "experiment_id": "exp_setup_mapping",
                    "evidence_level": 3,
                    "symbol": "BTCUSDT",
                    "liquidity_bucket": "top",
                    "expected_gross_move_bps": 15.0,
                    "expected_total_cost_bps": 4.0,
                    "expected_net_edge_bps": 11.0,
                },
                experiment=None,
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(decision["strategy_manifest_id"], "new_alpha")
        self.assertNotIn("blocked_missing_strategy_manifest_id", decision["block_reasons"])

    def test_negative_candidate_expected_net_edge_is_rejected_after_manifest_resolution(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(root, manifest_payload("new_alpha"))

            decision = build_strategy_gate_decision(
                {
                    "setup": "new_alpha",
                    "strategy_manifest_id": "new_alpha",
                    "strategy_family": "unit_test",
                    "experiment_id": "exp_negative_net_edge",
                    "evidence_level": 3,
                    "symbol": "BTCUSDT",
                    "liquidity_bucket": "top",
                    "expected_gross_move_bps": 10.0,
                    "expected_total_cost_bps": 3.0,
                    "expected_net_edge_bps": 0.0,
                },
                experiment=None,
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(decision["strategy_manifest_id"], "new_alpha")
        self.assertNotIn("blocked_missing_strategy_manifest_id", decision["block_reasons"])
        self.assertIn("blocked_negative_expected_net_edge", decision["block_reasons"])

    def test_cost_to_edge_under_three_is_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(
                root,
                manifest_payload(
                    "new_alpha",
                    expected_gross_move_bps=10.0,
                    max_expected_total_cost_bps=4.0,
                ),
            )

            decision = build_strategy_gate_decision(
                {"setup": "new_alpha", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "new_alpha", "strategy_id": "new_alpha"},
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(decision["decision"], "block")
        self.assertIn("blocked_cost_to_edge_too_low", decision["block_reasons"])

    def test_evidence_level_below_shadow_positive_is_rejected_for_testnet(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(root, manifest_payload("new_alpha", evidence_level=2))

            decision = build_strategy_gate_decision(
                {"setup": "new_alpha", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "new_alpha", "strategy_id": "new_alpha"},
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(decision["decision"], "block")
        self.assertIn("blocked_evidence_level_too_low", decision["block_reasons"])

    def test_parity_failure_caps_evidence_and_rejects_testnet(self) -> None:
        parity = evaluate_entry_parity(
            {
                "feature_timestamp_ms": 1_700_000_010_000,
                "entry_timestamp_ms": 1_700_000_000_000,
                "backtest_uses_close_as_entry": True,
                "backtest_has_spread": False,
                "backtest_has_slippage": False,
                "backtest_has_fee": True,
                "backtest_has_funding": True,
                "signal_delay_matches": True,
                "strategy_conditions_match": True,
                "symbol_universe_matches": True,
                "top200_survivor_bias_checked": True,
                "replay_shadow_entry_match": True,
                "shadow_testnet_entry_match": True,
            }
        )

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(root, manifest_payload("new_alpha", evidence_level=4))

            decision = build_strategy_gate_decision(
                {"setup": "new_alpha", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "new_alpha", "strategy_id": "new_alpha"},
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
                parity_report=parity,
            )

        self.assertFalse(parity["passed"])
        self.assertEqual(decision["decision"], "block")
        self.assertIn("blocked_entry_parity_failed", decision["block_reasons"])

    def test_oi_strategy_without_evidence_is_shadow_only_not_testnet(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(
                root,
                manifest_payload(
                    "oi_build_breakout_continuation",
                    status="shadow_only",
                    evidence_level=0,
                    expected_gross_move_bps=None,
                    max_expected_total_cost_bps=None,
                    min_expected_net_edge_bps=None,
                ),
            )

            shadow_decision = build_strategy_gate_decision(
                {"setup": "oi_build_breakout_continuation", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "oi_build_breakout_continuation"},
                manifest_dir=root / "strategy_manifests",
                target_stage="shadow",
            )
            testnet_decision = build_strategy_gate_decision(
                {"setup": "oi_build_breakout_continuation", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "oi_build_breakout_continuation"},
                manifest_dir=root / "strategy_manifests",
                target_stage="testnet",
            )

        self.assertEqual(shadow_decision["decision"], "allow")
        self.assertEqual(testnet_decision["decision"], "block")
        self.assertIn("blocked_manifest_status_not_testnet", testnet_decision["block_reasons"])

    def test_mainnet_live_stage_is_always_blocked(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_manifest(root, manifest_payload("new_alpha", evidence_level=5, status="mainnet_blocked"))

            decision = build_strategy_gate_decision(
                {"setup": "new_alpha", "symbol": "BTCUSDT"},
                experiment={"strategy_manifest_id": "new_alpha", "strategy_id": "new_alpha"},
                manifest_dir=root / "strategy_manifests",
                target_stage="mainnet",
            )

        self.assertEqual(decision["decision"], "block")
        self.assertIn("blocked_mainnet_live_manual_approval_required", decision["block_reasons"])

    def test_experiment_tracking_writes_required_scaffold_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = build_experiment_manifest(
                experiment_id="exp_new_alpha",
                strategy_manifest_id="new_alpha",
                strategy_family="new_discovery",
                evidence_level=3,
                cost_model_version="cost_v1",
                regime_model_version="regime_v1",
                data_snapshot_id="snapshot_v1",
                config={"threshold": 1.0},
                code_git_sha=None,
            )
            outputs = write_experiment_scaffold(experiments_root=root / "experiments", manifest=manifest)

            self.assertEqual(manifest["experiment_id"], "exp_new_alpha")
            self.assertFalse(manifest["live_trading_enabled"])
            self.assertIn("config_hash", manifest)
            for path in outputs.values():
                self.assertTrue(Path(path).exists())

    def test_oi_framework_records_required_features_and_stays_shadow_only(self) -> None:
        features = {field: 1.0 for field in OI_REQUIRED_FEATURES}
        payload = build_oi_shadow_candidate(
            strategy_id="oi_build_breakout_continuation",
            features=features,
            manifest={"strategy_id": "oi_build_breakout_continuation", "family": "oi_build", "status": "shadow_only", "evidence_level": 0},
        )

        self.assertFalse(payload["promotion_allowed"])
        self.assertFalse(payload["testnet_allowed"])
        self.assertTrue(payload["shadow_only"])
        self.assertFalse(payload["missing_features"])


if __name__ == "__main__":
    unittest.main()
