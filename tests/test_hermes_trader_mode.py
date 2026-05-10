import json
import unittest

from phoenix.hermes_decision import validate_hermes_decision
from phoenix.hermes_trader_mode import run_trader_cycle
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

        risk = evaluate_risk(validation.decision or {}, snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

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

    def test_high_vol_no_trade_blocks_entry_without_micro_scalp_exception(self) -> None:
        validation = validate_hermes_decision(valid_enter_long())
        snapshot = fresh_snapshot(market_regime={"regime": "HIGH_VOL", "direction_lock": "NO_TRADE"})

        risk = evaluate_risk(validation.decision or {}, snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertFalse(risk.approved)
        self.assertIn("direction_lock_no_trade", risk.blocked_by)

    def test_high_vol_no_trade_allows_reduced_micro_scalp_exception(self) -> None:
        decision = valid_enter_long()
        decision["size_reduced"] = True
        decision["micro_scalp_exception"] = True
        decision["max_holding_time_sec"] = 180
        validation = validate_hermes_decision(decision)
        snapshot = fresh_snapshot(market_regime={"regime": "HIGH_VOL", "direction_lock": "NO_TRADE"})
        snapshot["top_candidates"][0]["micro_scalp_exception"] = True

        risk = evaluate_risk(validation.decision or {}, snapshot, environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"})

        self.assertTrue(risk.approved)
        self.assertIn("micro_scalp_size_multiplier=0.35", risk.risk_notes)

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


if __name__ == "__main__":
    unittest.main()
