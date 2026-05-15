import json
import unittest

from phoenix.hermes_decision import validate_hermes_decision
from pathlib import Path

from phoenix.hermes_trader_mode import (
    main as hermes_trader_mode_main,
    risk_config_from_trial_config,
    run_trader_cycle,
    validate_micro_notional,
    validate_stage2_micro_config,
)
from phoenix.review_reporter import build_review_report
from phoenix.risk_governor import RiskGovernorConfig, evaluate_risk
from phoenix.trader_snapshot import build_trader_snapshot


def fresh_snapshot(**overrides):
    base = build_trader_snapshot(
        market_data={
            "regime": "TREND_UP",
            "btc_trend_1m": 0.2,
            "btc_trend_5m": 0.4,
            "eth_trend_1m": 0.1,
            "eth_trend_5m": 0.3,
            "volatility": 1.0,
            "direction_lock": "BOTH_ALLOWED",
        },
        account_state={
            "trading_allowed": True,
            "daily_pnl_pct": 0.0,
            "daily_loss_remaining_pct": 3.0,
            "open_positions_count": 0,
            "max_open_positions": 1,
            "loss_streak": 0,
            "cooldown_active": False,
        },
        positions=[],
        candidates=[
            {
                "symbol": "BTCUSDT",
                "bias": "LONG",
                "setup_type": "QUICK_TRADE",
                "score": 90.0,
                "current_price": 100.0,
                "spread_bps": 1.0,
                "estimated_slippage_bps": 1.0,
                "liquidity_ok": True,
                "suggested_stop_pct": 0.6,
                "suggested_tp_pct": 1.2,
                "max_holding_time_sec": 900,
            }
        ],
        system_status={
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
            "protective_stop_path_available": True,
            "take_profit_path_available": True,
            "take_profit_order_supported": True,
            "emergency_close_available": True,
            "position_state": "known",
            "stop_protection_status": "healthy",
            "candidate_state": "known",
        },
    )
    for key, value in overrides.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key].update(value)
        else:
            base[key] = value
    base["system_status"].setdefault("protective_stop_path_available", True)
    base["system_status"].setdefault("take_profit_path_available", True)
    base["system_status"].setdefault("take_profit_order_supported", True)
    base["system_status"].setdefault("emergency_close_available", True)
    base["system_status"].setdefault("position_state", "known")
    base["system_status"].setdefault("stop_protection_status", "healthy")
    base["system_status"].setdefault("candidate_state", "known")
    return base


def valid_enter_long():
    return {
        "action": "ENTER_LONG",
        "symbol": "BTCUSDT",
        "trade_type": "QUICK_TRADE",
        "confidence": 0.8,
        "reason": "BTC breakout with tight spread",
        "stop_loss_pct": 0.6,
        "take_profit_pct": 1.2,
        "max_holding_time_sec": 900,
        "invalidation_condition": "breaks below continuation base",
        "source": "HERMES",
    }


class HermesTraderModeTests(unittest.TestCase):
    def test_invalid_hermes_action_should_reject(self) -> None:
        result = validate_hermes_decision({"action": "YOLO", "trade_type": "NONE", "confidence": 1, "source": "HERMES"})

        self.assertFalse(result.valid)
        self.assertIn("invalid_action", result.reasons)

    def test_enter_without_stop_loss_should_reject(self) -> None:
        decision = valid_enter_long()
        decision.pop("stop_loss_pct")

        result = validate_hermes_decision(decision)

        self.assertFalse(result.valid)
        self.assertIn("missing_stop_loss_for_entry", result.reasons)

    def test_stale_snapshot_should_reject(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        decision = validation.decision or {}
        snapshot = fresh_snapshot(system_status={"data_fresh": False})

        risk = evaluate_risk(decision, snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertFalse(risk.approved)
        self.assertIn("stale_snapshot", risk.blocked_by)

    def test_no_trade_does_not_fail_only_because_snapshot_is_stale(self) -> None:
        validation = validate_hermes_decision(
            {
                "action": "NO_TRADE",
                "symbol": None,
                "trade_type": "NONE",
                "confidence": 1.0,
                "reason": "nothing clean",
                "source": "HERMES",
            }
        )
        snapshot = fresh_snapshot(system_status={"data_fresh": False})

        risk = evaluate_risk(validation.decision or {}, snapshot, environment={"runtime_mode": "DRY_RUN", "env": "testnet"})

        self.assertTrue(risk.approved)

    def test_mainnet_env_should_reject(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())

        risk = evaluate_risk(
            validation.decision or {},
            fresh_snapshot(),
            environment={"runtime_mode": "MAINNET_LIVE", "env": "prod", "mainnet_live": True},
        )

        self.assertFalse(risk.approved)
        self.assertIn("mainnet_live_blocked", risk.blocked_by)
        self.assertIn("mainnet_env_blocked", risk.blocked_by)

    def test_mainnet_shadow_prod_rejects_order_sensitive_action(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())

        shadow = evaluate_risk(
            validation.decision or {},
            fresh_snapshot(),
            environment={"runtime_mode": "MAINNET_SHADOW", "env": "prod"},
        )
        generic_shadow = evaluate_risk(
            validation.decision or {},
            fresh_snapshot(),
            environment={"runtime_mode": "SHADOW", "env": "prod"},
        )

        self.assertFalse(shadow.approved)
        self.assertIn("mainnet_shadow_order_endpoint_blocked", shadow.blocked_by)
        self.assertFalse(generic_shadow.approved)
        self.assertIn("mainnet_env_blocked", generic_shadow.blocked_by)

    def test_phoenix_mainnet_live_enabled_should_reject(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())

        risk = evaluate_risk(
            validation.decision or {},
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "PHOENIX_MAINNET_LIVE_ENABLED": "true"},
        )

        self.assertFalse(risk.approved)
        self.assertIn("mainnet_live_blocked", risk.blocked_by)

    def test_auto_confirm_when_rules_pass_should_reject_and_be_ignored(self) -> None:
        decision = valid_enter_long()
        decision["AUTO_CONFIRM_WHEN_RULES_PASS"] = True
        validation = validate_hermes_decision(decision)

        risk = evaluate_risk(
            validation.decision or {},
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "AUTO_CONFIRM_WHEN_RULES_PASS": "true"},
        )

        self.assertFalse(risk.approved)
        self.assertIn("auto_confirm_rejected", risk.blocked_by)
        self.assertFalse(risk.sanitized_action.get("AUTO_CONFIRM_WHEN_RULES_PASS"))

    def test_unhealthy_transport_or_stale_snapshot_time_should_reject(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        old_snapshot = fresh_snapshot(
            system_status={
                "snapshot_time": "2026-01-01T00:00:00+00:00",
                "websocket_status": "healthy",
                "exchange_status": "healthy",
            }
        )
        unhealthy_snapshot = fresh_snapshot(system_status={"websocket_status": "ok", "exchange_status": "degraded"})

        old_risk = evaluate_risk(validation.decision or {}, old_snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})
        unhealthy_risk = evaluate_risk(
            validation.decision or {}, unhealthy_snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"}
        )

        self.assertFalse(old_risk.approved)
        self.assertIn("stale_snapshot", old_risk.blocked_by)
        self.assertFalse(unhealthy_risk.approved)
        self.assertIn("snapshot_transport_unhealthy", unhealthy_risk.blocked_by)

    def test_daily_loss_hit_should_reject(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        snapshot = fresh_snapshot(account_risk={"daily_loss_remaining_pct": 0.0})

        risk = evaluate_risk(
            validation.decision or {},
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertFalse(risk.approved)
        self.assertIn("daily_loss_limit_hit", risk.blocked_by)

    def test_direction_lock_conflict_should_reject(self) -> None:
        decision = valid_enter_long()
        decision["action"] = "ENTER_SHORT"
        validation = validate_hermes_decision(decision)
        snapshot = fresh_snapshot(market_regime={"direction_lock": "LONG_ONLY_OR_NO_TRADE"})

        risk = evaluate_risk(validation.decision or {}, snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertFalse(risk.approved)
        self.assertIn("direction_lock_conflict", risk.blocked_by)
        self.assertIn("direction_regime_mismatch", risk.blocked_by)
        self.assertEqual(risk.sanitized_action["allowed_direction"], "LONG")

    def test_trend_down_long_should_reject_without_direction_lock(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        snapshot = fresh_snapshot(market_regime={"regime": "TREND_DOWN", "direction_lock": "BOTH_ALLOWED"})

        risk = evaluate_risk(
            validation.decision or {},
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertFalse(risk.approved)
        self.assertIn("direction_regime_mismatch", risk.blocked_by)
        self.assertEqual(risk.sanitized_action["candidate_direction"], "LONG")
        self.assertEqual(risk.sanitized_action["allowed_direction"], "SHORT")

    def test_unknown_market_regime_should_reject_enter(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        snapshot = fresh_snapshot(market_regime={"regime": "UNKNOWN", "direction_lock": "BOTH_ALLOWED"})

        risk = evaluate_risk(
            validation.decision or {},
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertFalse(risk.approved)
        self.assertIn("market_regime_unknown", risk.blocked_by)

    def test_high_vol_no_trade_blocks_entry_without_micro_scalp_exception(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        snapshot = fresh_snapshot(market_regime={"regime": "HIGH_VOL", "direction_lock": "NO_TRADE"})

        risk = evaluate_risk(validation.decision or {}, snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertFalse(risk.approved)
        self.assertIn("direction_regime_high_vol", risk.blocked_by)
        self.assertIn("direction_regime_mismatch", risk.blocked_by)

    def test_high_vol_no_trade_rejects_reduced_micro_scalp_exception(self) -> None:
        decision = valid_enter_long()
        decision["size_reduced"] = True
        decision["micro_scalp_exception"] = True
        decision["max_holding_time_sec"] = 180
        validation = validate_hermes_decision(decision)
        snapshot = fresh_snapshot(market_regime={"regime": "HIGH_VOL", "direction_lock": "NO_TRADE"})
        snapshot["top_candidates"][0]["micro_scalp_exception"] = True

        risk = evaluate_risk(validation.decision or {}, snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertFalse(risk.approved)
        self.assertIn("direction_regime_high_vol", risk.blocked_by)
        self.assertIn("direction_regime_mismatch", risk.blocked_by)

    def test_chop_non_edge_should_reject(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        snapshot = fresh_snapshot(market_regime={"regime": "CHOP", "direction_lock": "BOTH_ALLOWED"})

        risk = evaluate_risk(
            validation.decision or {},
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertFalse(risk.approved)
        self.assertIn("chop_requires_range_edge", risk.blocked_by)
        self.assertFalse(risk.sanitized_action["direction_regime_allowed"])

    def test_protective_stop_path_or_emergency_close_unavailable_should_reject(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        stop_unavailable = fresh_snapshot(system_status={"protective_stop_path_available": False})
        emergency_unavailable = fresh_snapshot(system_status={"emergency_close_available": False})

        stop_risk = evaluate_risk(validation.decision or {}, stop_unavailable, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})
        emergency_risk = evaluate_risk(
            validation.decision or {}, emergency_unavailable, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"}
        )

        self.assertFalse(stop_risk.approved)
        self.assertIn("protective_stop_path_unavailable", stop_risk.blocked_by)
        self.assertFalse(emergency_risk.approved)
        self.assertIn("emergency_close_unavailable", emergency_risk.blocked_by)

    def test_position_state_unknown_should_reject_reduce_action(self) -> None:
        decision = {
            "action": "EXIT",
            "symbol": "BTCUSDT",
            "trade_type": "NONE",
            "confidence": 0.8,
            "reason": "exit stale position",
            "reduce_only": True,
            "source": "HERMES",
        }
        validation = validate_hermes_decision(decision)

        risk = evaluate_risk(validation.decision or {}, fresh_snapshot(), environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertFalse(risk.approved)
        self.assertIn("position_state_unknown", risk.blocked_by)

    def test_stop_order_unknown_should_reject_reduce_action(self) -> None:
        decision = {
            "action": "EXIT",
            "symbol": "BTCUSDT",
            "trade_type": "NONE",
            "confidence": 0.8,
            "reason": "exit position",
            "reduce_only": True,
            "source": "HERMES",
        }
        validation = validate_hermes_decision(decision)
        snapshot = fresh_snapshot(
            current_positions=[
                {
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "protection_status": "unknown",
                }
            ],
            account_risk={"open_positions_count": 1},
        )

        risk = evaluate_risk(validation.decision or {}, snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertFalse(risk.approved)
        self.assertIn("stop_order_status_unknown", risk.blocked_by)

    def test_spread_too_wide_should_reject(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        snapshot = fresh_snapshot()
        snapshot["top_candidates"][0]["spread_bps"] = 50.0

        risk = evaluate_risk(
            validation.decision or {},
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            config=RiskGovernorConfig(max_spread_bps=10.0),
        )

        self.assertFalse(risk.approved)
        self.assertIn("spread_too_wide", risk.blocked_by)

    def test_valid_testnet_decision_should_pass(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())

        risk = evaluate_risk(validation.decision or {}, fresh_snapshot(), environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertTrue(risk.approved)
        self.assertEqual(risk.reason, "approved")
        self.assertEqual(risk.required_protective_orders[0]["type"], "exchange_side_stop_loss")

    def test_stage2_config_is_explicit_and_micro_sized(self) -> None:
        config_path = Path(__file__).resolve().parents[1] / "configs" / "hermes_trader_stage2_micro.testnet.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))

        self.assertEqual(validate_stage2_micro_config(config), [])
        self.assertEqual(config["mode"], "testnet")
        self.assertEqual(config["provider"], "file")
        self.assertEqual(config["decision_policy"], "calibration")
        self.assertEqual(config["max_open_positions"], 1)
        self.assertLessEqual(config["leverage"], 2)
        self.assertLessEqual(config["quote_allocation_usdt"], 10.0)
        self.assertTrue(config["require_known_market_regime"])
        self.assertTrue(config["lifecycle_monitor_enabled"])
        self.assertEqual(config["lifecycle_poll_sec"], 5)
        self.assertFalse(config["mainnet_live"])

    def test_stage2_config_rejects_missing_micro_notional(self) -> None:
        config = json.loads((Path(__file__).resolve().parents[1] / "configs" / "hermes_trader_stage2_micro.testnet.json").read_text(encoding="utf-8"))
        config.pop("quote_allocation_usdt")

        errors = validate_stage2_micro_config(config)

        self.assertIn("quote_allocation_usdt_missing", errors)
        self.assertIn("quote_allocation_usdt_missing", validate_micro_notional(None))

    def test_stage2_risk_config_blocks_missing_exit_plan_fields(self) -> None:
        config = json.loads((Path(__file__).resolve().parents[1] / "configs" / "hermes_trader_stage2_micro.testnet.json").read_text(encoding="utf-8"))
        risk_config = risk_config_from_trial_config(config)
        decision = valid_enter_long()
        decision.pop("take_profit_pct")
        decision.pop("max_holding_time_sec")
        decision.pop("invalidation_condition")
        validation = validate_hermes_decision(decision)

        risk = evaluate_risk(
            validation.decision or decision,
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "quote_allocation_usdt": 5.0},
            config=risk_config,
        )

        self.assertFalse(risk.approved)
        self.assertIn("missing_take_profit", risk.blocked_by)
        self.assertIn("missing_max_holding_time", risk.blocked_by)
        self.assertIn("missing_invalidation_condition", risk.blocked_by)

    def test_stage2_risk_config_blocks_missing_or_large_micro_notional(self) -> None:
        config = json.loads((Path(__file__).resolve().parents[1] / "configs" / "hermes_trader_stage2_micro.testnet.json").read_text(encoding="utf-8"))
        risk_config = risk_config_from_trial_config(config)
        validation = validate_hermes_decision(valid_enter_long())

        missing = evaluate_risk(
            validation.decision or {},
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            config=risk_config,
        )
        large = evaluate_risk(
            validation.decision or {},
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "quote_allocation_usdt": 200.0},
            config=risk_config,
        )

        self.assertFalse(missing.approved)
        self.assertIn("micro_notional_required", missing.blocked_by)
        self.assertFalse(large.approved)
        self.assertIn("micro_notional_exceeded", large.blocked_by)

    def test_stage2_risk_config_rejects_micro_notional_infeasible_candidate(self) -> None:
        config = json.loads((Path(__file__).resolve().parents[1] / "configs" / "hermes_trader_stage2_micro.testnet.json").read_text(encoding="utf-8"))
        risk_config = risk_config_from_trial_config(config)
        validation = validate_hermes_decision(valid_enter_long())
        snapshot = fresh_snapshot()
        snapshot["top_candidates"][0].update(
            {
                "exchange_filter_checked": True,
                "symbol_tradeable": True,
                "micro_notional_feasible": False,
                "rounded_qty": 0.0,
                "min_qty": 1.0,
                "step_size": 1.0,
                "min_notional": 5.0,
                "required_quote_allocation_usdt": 12.0,
                "configured_quote_allocation_usdt": 5.0,
                "configured_leverage": 2,
                "max_quote_allocation_usdt": 10.0,
                "infeasible_reason": "min_qty_not_met,required_quote_allocation_exceeds_max",
            }
        )

        risk = evaluate_risk(
            validation.decision or {},
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "quote_allocation_usdt": 5.0, "stage2_micro_order": True},
            config=risk_config,
        )

        self.assertFalse(risk.approved)
        self.assertIn("exchange_filter_reject", risk.blocked_by)
        self.assertIn("micro_notional_infeasible", risk.blocked_by)
        self.assertEqual(risk.sanitized_action["configured_quote_allocation_usdt"], 5.0)
        self.assertEqual(risk.sanitized_action["configured_leverage"], 2)

    def test_stage2_risk_config_rejects_unchecked_exchange_filter(self) -> None:
        config = json.loads((Path(__file__).resolve().parents[1] / "configs" / "hermes_trader_stage2_micro.testnet.json").read_text(encoding="utf-8"))
        risk_config = risk_config_from_trial_config(config)
        validation = validate_hermes_decision(valid_enter_long())

        risk = evaluate_risk(
            validation.decision or {},
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "quote_allocation_usdt": 5.0, "stage2_micro_order": True},
            config=risk_config,
        )

        self.assertFalse(risk.approved)
        self.assertIn("exchange_filter_reject", risk.blocked_by)
        self.assertIn("exchange_filter_unchecked", risk.blocked_by)

    def test_review_reporter_should_output_readable_cantonese_text(self) -> None:
        report = build_review_report(
            "RISK_REJECT",
            {
                "action": "ENTER_LONG",
                "symbol": "BTCUSDT",
                "blocked_by": ["spread_too_wide"],
            },
        )

        self.assertIn("【Risk Governor 拒绝】", report.text)
        self.assertIn("Phoenix 唔会执行", report.text)
        self.assertIn("BTCUSDT", report.text)

    def test_orchestrator_dry_run_writes_replay_logs_without_order(self, tmp_path=None) -> None:
        # unittest compatibility: use tempfile instead of pytest fixtures.
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_payload=fresh_snapshot(),
                decision_payload=valid_enter_long(),
                output_dir=tmp,
                dry_run=True,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            )

            self.assertTrue(result["risk_governor_result"]["approved"])
            self.assertTrue(result["execution_intent"]["dry_run"])
            self.assertFalse(result["execution_result"]["order_submitted"])
            for name in (
                "snapshot.jsonl",
                "hermes_decision.jsonl",
                "risk_governor_result.jsonl",
                "execution_intent.jsonl",
                "execution_result.jsonl",
                "review_report.jsonl",
            ):
                with open(f"{tmp}/{name}", encoding="utf-8") as fh:
                    self.assertTrue(json.loads(fh.readline()))


def test_single_cycle_config_file_provider_writes_snapshot_to_config_inbox(tmp_path, monkeypatch, capsys):
    from phoenix import hermes_trader_mode as module

    config_path = tmp_path / "stage2_preflight.dry_run.json"
    inbox = tmp_path / "hermes_inbox"
    outbox = tmp_path / "hermes_outbox"
    archive = tmp_path / "hermes_archive"
    logs = tmp_path / "hermes_logs"
    output = tmp_path / "run_logs"
    config_path.write_text(
        json.dumps(
            {
                "mode": "dry-run",
                "provider": "file",
                "dashboard_snapshot_url": "http://127.0.0.1:18765/snapshot",
                "hermes_inbox": str(inbox),
                "hermes_outbox": str(outbox),
                "hermes_archive": str(archive),
                "hermes_logs": str(logs),
                "output_dir": str(output),
                "decision_timeout_sec": 0.0,
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        module,
        "verify_testnet_executor_capabilities",
        lambda: {
            "protective_stop_path_available": True,
            "take_profit_path_available": True,
            "emergency_close_available": True,
            "protective_stop_capability_source": "verified",
            "take_profit_capability_source": "verified",
            "emergency_close_capability_source": "verified",
        },
    )
    monkeypatch.setattr(module, "build_dashboard_api_trader_snapshot", lambda **_: fresh_snapshot())

    exit_code = hermes_trader_mode_main(["--config", str(config_path), "--single-cycle"])
    captured = capsys.readouterr()

    assert exit_code == 0, captured.out
    snapshots = list(inbox.glob("snapshot_*.json"))
    assert len(snapshots) == 1
    payload = json.loads(snapshots[0].read_text(encoding="utf-8"))
    assert payload["trace_id"]
    assert payload["system_status"]["trace_id"] == payload["trace_id"]
    replay_events = [
        json.loads(line)
        for line in (output / "replay_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    event_names = {event["event"] for event in replay_events}
    assert "hermes_decision_raw" in event_names
    assert "hermes_decision_normalized" in event_names
    assert "hermes_decision_fallback" in event_names


if __name__ == "__main__":
    unittest.main()
