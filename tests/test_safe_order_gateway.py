import json
import tempfile
import unittest

from phoenix.safe_order_gateway import submit_order_intent
from phoenix.trader_snapshot import build_trader_snapshot


def fresh_snapshot(**overrides):
    base = build_trader_snapshot(
        market_data={
            "regime": "TREND_UP",
            "direction_lock": "BOTH_ALLOWED",
            "btc_trend_1m": 0.2,
            "btc_trend_5m": 0.4,
            "eth_trend_1m": 0.1,
            "eth_trend_5m": 0.3,
            "volatility": 1.0,
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
    base["system_status"].update(
        {
            "protective_stop_path_available": True,
            "emergency_close_available": True,
            "position_state": "known",
            "stop_protection_status": "healthy",
            "candidate_state": "known",
        }
    )
    for key, value in overrides.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key].update(value)
        else:
            base[key] = value
    return base


def valid_intent():
    return {
        "action": "ENTER_LONG",
        "symbol": "btcusdt",
        "trade_type": "quick_trade",
        "confidence": 0.8,
        "reason": "Hermes saw a clean continuation setup",
        "stop_loss_pct": 0.6,
        "take_profit_pct": 1.2,
        "max_holding_time_sec": 900,
    }


class SafeOrderGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_risk_reject_does_not_call_executor_callback(self) -> None:
        calls = []

        with tempfile.TemporaryDirectory() as tmp:
            gateway = await submit_order_intent(
                valid_intent(),
                fresh_snapshot(system_status={"data_fresh": False}),
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                source="HERMES",
                dry_run=False,
                log_dir=f"{tmp}/logs",
                audit_log_path=f"{tmp}/gateway.jsonl",
                executor_callback=lambda payload: calls.append(payload),
            )
            result = gateway.to_dict()

            self.assertEqual(calls, [])
            self.assertFalse(result["risk_governor_result"]["approved"])
            self.assertFalse(result["execution_result"]["order_submitted"])
            self.assertFalse(result["execution_result"]["dry_run"])
            self.assertEqual(result["execution_result"]["status"], "blocked_before_execution")
            with open(f"{tmp}/gateway.jsonl", encoding="utf-8") as fh:
                rows = [json.loads(line) for line in fh]
            self.assertEqual(rows[-1]["event"], "safe_order_rejected")
            self.assertIn("stale_snapshot", rows[-1]["risk_governor_result"]["blocked_by"])

    async def test_approved_dry_run_only_generates_execution_intent(self) -> None:
        calls = []

        with tempfile.TemporaryDirectory() as tmp:
            gateway = await submit_order_intent(
                valid_intent(),
                fresh_snapshot(),
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                source="HERMES",
                log_dir=f"{tmp}/logs",
                audit_log_path=f"{tmp}/gateway.jsonl",
                executor_callback=lambda payload: calls.append(payload),
            )
            result = gateway.to_dict()

            self.assertEqual(calls, [])
            self.assertTrue(result["risk_governor_result"]["approved"])
            self.assertTrue(result["execution_intent"]["dry_run"])
            self.assertTrue(result["execution_intent"]["approved_for_execution"])
            self.assertEqual(result["execution_intent"]["symbol"], "BTCUSDT")
            self.assertFalse(result["execution_result"]["order_submitted"])
            self.assertEqual(result["execution_result"]["status"], "dry_run_intent_only")
            with open(f"{tmp}/gateway.jsonl", encoding="utf-8") as fh:
                rows = [json.loads(line) for line in fh]
            self.assertEqual(rows[-1]["event"], "safe_order_approved")
            self.assertEqual(rows[-1]["execution_intent"]["side"], "BUY")

    async def test_mainnet_shadow_order_endpoint_rejects_before_callback(self) -> None:
        calls = []

        gateway = await submit_order_intent(
            valid_intent(),
            fresh_snapshot(),
            environment={"runtime_mode": "MAINNET_SHADOW", "env": "prod"},
            source="HERMES",
            dry_run=False,
            executor_callback=lambda payload: calls.append(payload),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertIn("mainnet_shadow_order_endpoint_blocked", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["order_submitted"])

    async def test_legacy_enable_mainnet_live_flag_rejects_before_callback(self) -> None:
        calls = []

        gateway = await submit_order_intent(
            valid_intent(),
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "PHOENIX_ENABLE_MAINNET_LIVE": "true"},
            source="HERMES",
            dry_run=False,
            executor_callback=lambda payload: calls.append(payload),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertIn("mainnet_live_blocked", gateway.blocked_by)

    async def test_auto_confirm_execution_mode_rejects_before_callback(self) -> None:
        calls = []

        gateway = await submit_order_intent(
            valid_intent(),
            fresh_snapshot(),
            environment={
                "runtime_mode": "TESTNET_LIVE",
                "env": "testnet",
                "PHOENIX_EXECUTION_MODE": "AUTO_CONFIRM_WHEN_RULES_PASS",
            },
            source="HERMES",
            dry_run=False,
            executor_callback=lambda payload: calls.append(payload),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertIn("auto_confirm_rejected", gateway.blocked_by)


if __name__ == "__main__":
    unittest.main()
