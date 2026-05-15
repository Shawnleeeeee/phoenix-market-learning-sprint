import json
import tempfile
import unittest
from pathlib import Path

from phoenix.hermes_decision import validate_hermes_decision
from phoenix.hermes_decision_loop import build_calibration_decision
from phoenix.safe_order_gateway import _action_from_payload, submit_order_intent
from phoenix.trader_snapshot import build_trader_snapshot
from phoenix.risk_governor import RiskGovernorConfig


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
            "take_profit_path_available": True,
            "take_profit_order_supported": True,
            "emergency_close_available": True,
            "position_state": "known",
            "stop_protection_status": "healthy",
            "candidate_state": "known",
        },
    )
    base["system_status"].update(
        {
            "protective_stop_path_available": True,
            "take_profit_path_available": True,
            "take_profit_order_supported": True,
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
        "invalidation_condition": "breaks below continuation base",
    }


def no_trade_intent():
    return {
        "action": "NO_TRADE",
        "symbol": None,
        "trade_type": "NONE",
        "confidence": 0.9,
        "reason": "Hermes decided no trade",
        "source": "HERMES",
    }


def trusted_runtime_snapshot(**system_overrides):
    snapshot = fresh_snapshot(
        system_status={
            "source": "runtime",
            "snapshot_source": "runtime",
            "trusted_runtime_snapshot": True,
            "account_state_source": "signed_account",
            "position_state_source": "signed_positions",
            "protective_stop_path_available": True,
            "protective_stop_capability_source": "verified_code_path:test",
            "take_profit_path_available": True,
            "take_profit_capability_source": "verified_code_path:test",
            "take_profit_order_supported": True,
            "emergency_close_available": True,
            "emergency_close_capability_source": "verified_code_path:test",
            "position_state": "known",
            "stop_protection_status": "healthy",
            "candidate_state": "known",
            **system_overrides,
        }
    )
    return snapshot


class SafeOrderGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_take_profit_purpose_normalizes_as_reduce_only_action(self) -> None:
        action = _action_from_payload(
            side="SELL",
            reduce_only=False,
            order_type="TAKE_PROFIT_MARKET",
            purpose="take_profit",
        )

        self.assertEqual(action, "TAKE_PROFIT")

    async def test_forced_valid_enter_long_passes_dry_run_without_executor(self) -> None:
        calls = []

        gateway = await submit_order_intent(
            valid_intent(),
            trusted_runtime_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertEqual(calls, [])
        self.assertTrue(gateway.approved)
        self.assertEqual(gateway.result_type, "approved_dry_run")
        self.assertEqual(gateway.risk_governor_result["reason"], "approved")
        self.assertGreater(len(gateway.execution_intent["required_protective_orders"]), 0)
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["testnet_order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])

    async def test_forced_valid_enter_short_passes_dry_run_without_executor(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["top_candidates"][0].update({"bias": "SHORT", "setup_type": "QUICK_TRADE"})
        snapshot["market_regime"] = {"regime": "TREND_DOWN", "direction_lock": "SHORT_ONLY_OR_NO_TRADE"}
        intent = {
            **valid_intent(),
            "action": "ENTER_SHORT",
            "reason": "Hermes saw a clean short continuation setup",
            "invalidation_condition": "reclaims short trigger level",
        }

        gateway = await submit_order_intent(
            intent,
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertEqual(calls, [])
        self.assertTrue(gateway.approved)
        self.assertEqual(gateway.result_type, "approved_dry_run")
        self.assertEqual(gateway.execution_intent["side"], "SELL")
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["testnet_order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])

    async def test_forced_enter_without_stop_loss_rejects_before_executor(self) -> None:
        calls = []
        intent = valid_intent()
        intent.pop("stop_loss_pct")

        gateway = await submit_order_intent(
            intent,
            trusted_runtime_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertIn("missing_stop_loss_for_entry", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])

    async def test_forced_enter_with_stale_snapshot_hard_freezes(self) -> None:
        calls = []

        gateway = await submit_order_intent(
            valid_intent(),
            trusted_runtime_snapshot(data_fresh=False),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "hard_freeze")
        self.assertIn("stale_snapshot", gateway.blocked_by)
        self.assertTrue(gateway.execution_result["frozen"])
        self.assertFalse(gateway.execution_result["can_continue"])
        self.assertFalse(gateway.execution_result["executor_called"])

    async def test_forced_enter_direction_lock_conflict_soft_rejects(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["market_regime"]["direction_lock"] = "LONG_ONLY_OR_NO_TRADE"
        intent = {**valid_intent(), "action": "ENTER_SHORT"}

        gateway = await submit_order_intent(
            intent,
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("direction_lock_conflict", gateway.blocked_by)
        self.assertIn("direction_regime_mismatch", gateway.blocked_by)
        self.assertFalse(gateway.execution_intent["direction_regime_allowed"])
        self.assertEqual(gateway.execution_intent["allowed_direction"], "LONG")
        self.assertFalse(gateway.execution_result["frozen"])
        self.assertTrue(gateway.execution_result["can_continue"])
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["testnet_order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])

    async def test_forced_enter_trend_down_long_soft_rejects_without_executor(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["market_regime"] = {"regime": "TREND_DOWN", "direction_lock": "BOTH_ALLOWED"}

        gateway = await submit_order_intent(
            valid_intent(),
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("direction_regime_mismatch", gateway.blocked_by)
        self.assertIn("direction_regime_mismatch", gateway.risk_governor_result["blocked_by"])
        self.assertIn("direction_regime_reason", gateway.risk_governor_result["sanitized_action"])
        self.assertIn("direction_regime_reason", gateway.review_report["payload"])
        self.assertFalse(gateway.execution_intent["direction_regime_allowed"])
        self.assertEqual(gateway.execution_intent["allowed_direction"], "SHORT")
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["testnet_order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])

    async def test_no_trade_direction_regime_metadata_survives_gateway_audit(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["market_regime"] = {"regime": "TREND_DOWN", "direction_lock": "BOTH_ALLOWED"}
        snapshot["top_candidates"][0].update({"symbol": "BTCUSDT", "bias": "LONG", "invalidation_hint": "trend-down invalidates long bias"})
        decision = build_calibration_decision(snapshot, trace_id="audit_meta_trend_down_long")

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertEqual(decision["market_regime"], "TREND_DOWN")
        self.assertEqual(decision["candidate_direction"], "LONG")
        self.assertEqual(decision["allowed_direction"], "SHORT")
        self.assertFalse(decision["direction_regime_allowed"])
        self.assertIn("direction_regime_mismatch", decision["blocked_by"])
        self.assertIn("direction_regime_reason", decision)

        validation = validate_hermes_decision(decision)
        self.assertTrue(validation.valid)
        validated_decision = validation.decision or {}
        self.assertEqual(validated_decision["normalized_decision"], "NO_TRADE")
        self.assertEqual(validated_decision["no_trade_reason"], decision["reason"])
        self.assertEqual(validated_decision["market_regime"], "TREND_DOWN")
        self.assertEqual(validated_decision["candidate_direction"], "LONG")
        self.assertEqual(validated_decision["allowed_direction"], "SHORT")
        self.assertFalse(validated_decision["direction_regime_allowed"])
        self.assertIn("direction_regime_mismatch", validated_decision["blocked_by"])
        self.assertEqual(validated_decision["candidate_symbol"], "BTCUSDT")

        with tempfile.TemporaryDirectory() as tmp:
            gateway = await submit_order_intent(
                decision,
                snapshot,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
                source="HERMES",
                dry_run=False,
                executor_callback=lambda payload: calls.append(payload),
                risk_config=RiskGovernorConfig(require_known_market_regime=True),
                log_dir=f"{tmp}/logs",
                audit_log_path=f"{tmp}/gateway.jsonl",
            )

            self.assertEqual(calls, [])
            self.assertFalse(gateway.approved)
            self.assertEqual(gateway.result_type, "soft_reject")
            self.assertIn("no_trade_from_hermes", gateway.blocked_by)
            self.assertIn("direction_regime_mismatch", gateway.blocked_by)
            self.assertEqual(gateway.normalized_decision["market_regime"], "TREND_DOWN")
            self.assertEqual(gateway.risk_governor_result["sanitized_action"]["direction_regime_reason"], decision["direction_regime_reason"])
            self.assertFalse(gateway.risk_governor_result["sanitized_action"]["direction_regime_allowed"])
            self.assertEqual(gateway.execution_intent["direction_regime_reason"], decision["direction_regime_reason"])
            self.assertEqual(gateway.execution_intent["candidate_direction"], "LONG")
            self.assertEqual(gateway.execution_intent["allowed_direction"], "SHORT")
            self.assertFalse(gateway.execution_intent["direction_regime_allowed"])
            self.assertEqual(gateway.review_report["payload"]["direction_regime_reason"], decision["direction_regime_reason"])
            self.assertIn("TREND_DOWN", gateway.review_report["text"])
            self.assertIn("SHORT", gateway.review_report["text"])
            self.assertIn("LONG", gateway.review_report["text"])
            self.assertFalse(gateway.execution_result["executor_called"])
            self.assertFalse(gateway.execution_result["order_submitted"])
            self.assertFalse(gateway.execution_result["testnet_order_submitted"])
            self.assertFalse(gateway.execution_result["mainnet_order_submitted"])

            replay_path = Path(tmp) / "logs" / "safe_order_gateway_replay.jsonl"
            with replay_path.open(encoding="utf-8") as fh:
                replay = [json.loads(line) for line in fh][-1]
            gateway_row = replay["safe_order_gateway_result"]
            self.assertEqual(gateway_row["normalized_decision"]["direction_regime_reason"], decision["direction_regime_reason"])
            self.assertEqual(replay["risk_governor_result"]["sanitized_action"]["direction_regime_reason"], decision["direction_regime_reason"])
            self.assertEqual(gateway_row["execution_intent"]["direction_regime_reason"], decision["direction_regime_reason"])
            self.assertFalse(replay["execution_result"]["executor_called"])
            self.assertFalse(replay["execution_result"]["testnet_order_submitted"])
            self.assertFalse(replay["execution_result"]["mainnet_order_submitted"])

    async def test_high_vol_blocks_entry_without_executor(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["market_regime"] = {"regime": "HIGH_VOL", "direction_lock": "BOTH_ALLOWED"}

        gateway = await submit_order_intent(
            valid_intent(),
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("direction_regime_high_vol", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["testnet_order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])

    async def test_chop_non_edge_blocks_entry_without_executor(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["market_regime"] = {"regime": "CHOP", "direction_lock": "BOTH_ALLOWED"}

        gateway = await submit_order_intent(
            valid_intent(),
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("chop_requires_range_edge", gateway.blocked_by)
        self.assertFalse(gateway.execution_intent["direction_regime_allowed"])
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["testnet_order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])

    async def test_forced_enter_unknown_market_regime_soft_rejects_without_executor(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["market_regime"] = {"regime": "UNKNOWN", "direction_lock": "BOTH_ALLOWED"}

        gateway = await submit_order_intent(
            valid_intent(),
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(require_known_market_regime=True),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("market_regime_unknown", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["frozen"])
        self.assertTrue(gateway.execution_result["can_continue"])
        self.assertFalse(gateway.execution_result["executor_called"])

    async def test_forced_enter_spread_too_wide_soft_rejects(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["top_candidates"][0]["spread_bps"] = 99.0

        gateway = await submit_order_intent(
            valid_intent(),
            snapshot,
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("spread_too_wide", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["frozen"])
        self.assertTrue(gateway.execution_result["can_continue"])
        self.assertFalse(gateway.execution_result["executor_called"])

    async def test_stage2_take_profit_order_unsupported_rejects_before_executor(self) -> None:
        calls = []

        gateway = await submit_order_intent(
            valid_intent(),
            trusted_runtime_snapshot(take_profit_order_supported=False),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=True,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(require_take_profit=True),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertIn("take_profit_order_unsupported", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["order_submitted"])

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

    async def test_approved_no_executor_only_for_approved_open_action_without_callback(self) -> None:
        gateway = await submit_order_intent(
            valid_intent(),
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            source="HERMES",
            dry_run=False,
            executor_callback=None,
        )

        self.assertTrue(gateway.approved)
        self.assertEqual(gateway.result_type, "approved_no_executor")
        self.assertEqual(gateway.execution_result["status"], "no_executor_callback")
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["frozen"])
        self.assertTrue(gateway.execution_result["can_continue"])

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

    async def test_testnet_order_requires_trusted_runtime_snapshot_when_flagged(self) -> None:
        calls = []

        gateway = await submit_order_intent(
            valid_intent(),
            fresh_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=False,
            executor_callback=lambda payload: calls.append(payload),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertIn("manual_snapshot_not_allowed_for_testnet_order_mode", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["order_submitted"])

    async def test_micro_notional_infeasible_rejects_before_executor_as_exchange_filter_reject(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
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

        gateway = await submit_order_intent(
            valid_intent(),
            snapshot,
            environment={
                "runtime_mode": "TESTNET_LIVE",
                "env": "testnet",
                "require_trusted_runtime_snapshot": True,
                "quote_allocation_usdt": 5.0,
                "stage2_micro_order": True,
            },
            source="HERMES",
            dry_run=False,
            executor_callback=lambda payload: calls.append(payload),
            risk_config=RiskGovernorConfig(
                require_known_market_regime=True,
                require_take_profit=True,
                require_max_holding_time=True,
                require_invalidation_condition=True,
                require_explicit_quote_allocation=True,
                max_quote_allocation_usdt=10.0,
            ),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("exchange_filter_reject", gateway.blocked_by)
        self.assertIn("micro_notional_infeasible", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["testnet_order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])
        self.assertFalse(gateway.execution_result["frozen"])
        self.assertEqual(gateway.execution_intent["rounded_qty"], 0.0)
        self.assertEqual(gateway.execution_intent["required_quote_allocation_usdt"], 12.0)

    async def test_entry_quality_false_soft_rejects_before_executor_and_preserves_audit_fields(self) -> None:
        calls = []
        intent = {
            **valid_intent(),
            "entry_quality_filter": "stage2_v0.4_entry_quality",
            "entry_quality_version": "stage2_v0.4",
            "entry_quality_checked": True,
            "entry_quality_allowed": False,
            "entry_quality_score": 0.46,
            "entry_quality_min_score": 0.75,
            "entry_quality_reason": "entry quality block: late chase short",
            "entry_quality_reasons": ["late chase short"],
            "entry_quality_components": {"late_chase": True},
            "blocked_by": ["entry_quality_filter_failed", "late_chase_short"],
        }

        with tempfile.TemporaryDirectory() as tmp:
            gateway = await submit_order_intent(
                intent,
                trusted_runtime_snapshot(),
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
                source="HERMES",
                dry_run=False,
                executor_callback=lambda payload: calls.append(payload),
                risk_config=RiskGovernorConfig(require_known_market_regime=True),
                log_dir=f"{tmp}/logs",
            )

            replay_path = Path(tmp) / "logs" / "safe_order_gateway_replay.jsonl"
            replay = [json.loads(line) for line in replay_path.read_text(encoding="utf-8").splitlines()][-1]

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("entry_quality_filter_failed", gateway.blocked_by)
        self.assertEqual(gateway.execution_intent["entry_quality_allowed"], False)
        self.assertEqual(gateway.execution_intent["entry_quality_reason"], "entry quality block: late chase short")
        self.assertEqual(gateway.review_report["payload"]["entry_quality_reason"], "entry quality block: late chase short")
        self.assertIn("Entry quality", gateway.review_report["text"])
        self.assertFalse(gateway.execution_result["executor_called"])
        self.assertFalse(gateway.execution_result["testnet_order_submitted"])
        self.assertFalse(gateway.execution_result["mainnet_order_submitted"])
        self.assertEqual(
            replay["safe_order_gateway_result"]["execution_intent"]["entry_quality_reason"],
            "entry quality block: late chase short",
        )

    async def test_executor_callback_exception_freezes_closed(self) -> None:
        def broken_executor(_payload):
            raise RuntimeError("simulated executor failure")

        gateway = await submit_order_intent(
            valid_intent(),
            trusted_runtime_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=False,
            executor_callback=broken_executor,
        )

        self.assertTrue(gateway.approved)
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertTrue(gateway.execution_result["frozen"])
        self.assertEqual(gateway.execution_result["result_type"], "hard_freeze")
        self.assertFalse(gateway.execution_result["can_continue"])
        self.assertEqual(gateway.execution_result["freeze_reason"], "executor_callback_exception")
        self.assertFalse(gateway.execution_result["can_continue"])

    async def test_no_trade_is_soft_reject_not_approved_no_executor(self) -> None:
        calls = []

        gateway = await submit_order_intent(
            no_trade_intent(),
            trusted_runtime_snapshot(),
            environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet", "require_trusted_runtime_snapshot": True},
            source="HERMES",
            dry_run=False,
            executor_callback=lambda payload: calls.append(payload),
        )

        self.assertEqual(calls, [])
        self.assertFalse(gateway.approved)
        self.assertEqual(gateway.result_type, "soft_reject")
        self.assertIn("no_trade_from_hermes", gateway.blocked_by)
        self.assertFalse(gateway.execution_result["order_submitted"])
        self.assertFalse(gateway.execution_result["frozen"])
        self.assertTrue(gateway.execution_result["can_continue"])
        self.assertNotEqual(gateway.execution_result["result_type"], "approved_no_executor")


if __name__ == "__main__":
    unittest.main()
