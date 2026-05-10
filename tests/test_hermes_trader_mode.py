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
            "websocket_status": "ok",
            "exchange_status": "ok",
        },
    )
    for key, value in overrides.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key].update(value)
        else:
            base[key] = value
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
