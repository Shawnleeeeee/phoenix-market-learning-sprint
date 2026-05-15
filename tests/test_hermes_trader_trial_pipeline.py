import asyncio
import inspect
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from phoenix.hermes_decision import validate_hermes_decision
from phoenix.hermes_decision_provider import FileHermesDecisionProvider, HttpHermesDecisionProvider, MockHermesDecisionProvider
from phoenix.hermes_trader_mode import main as trader_main
from phoenix.hermes_trader_mode import run_trader_cycle
from phoenix.review_reporter import build_review_report
from phoenix.risk_governor import evaluate_risk
from phoenix.testnet_executor_callback import TestnetExecutorCallback
import phoenix.testnet_executor_callback as testnet_executor_callback
from phoenix.trader_snapshot import build_trader_snapshot
from phoenix.trader_snapshot_runtime import build_runtime_trader_snapshot


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
            "source": "unit_test",
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
        "reason": "Hermes saw a clean continuation setup",
        "stop_loss_pct": 0.6,
        "take_profit_pct": 1.2,
        "max_holding_time_sec": 900,
        "invalidation_condition": "breaks below continuation base",
        "source": "HERMES",
    }


def no_trade_decision():
    return {
        "action": "NO_TRADE",
        "symbol": None,
        "trade_type": "NONE",
        "confidence": 0.9,
        "reason": "Hermes judged no clean setup",
        "source": "HERMES",
    }


def wait_for_trigger_decision():
    return {
        "action": "WAIT_FOR_TRIGGER",
        "symbol": "BTCUSDT",
        "trade_type": "NONE",
        "confidence": 0.9,
        "reason": "Hermes wants a trigger but Phoenix has no active pending trigger",
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
            "protective_stop_capability_source": "testnet_order_plan_preflight",
            "take_profit_path_available": True,
            "take_profit_capability_source": "testnet_order_plan_preflight",
            "take_profit_order_supported": True,
            "emergency_close_available": True,
            "emergency_close_capability_source": "implemented_reduce_only_preflight",
            "position_state": "known",
            "stop_protection_status": "healthy",
            "candidate_state": "known",
            **system_overrides,
        }
    )
    return snapshot


class FakeTrialFutures:
    def __init__(self, *, fail_stop: bool = False, fail_take_profit: bool = False, fail_emergency: bool = False) -> None:
        self.fail_stop = fail_stop
        self.fail_take_profit = fail_take_profit
        self.fail_emergency = fail_emergency
        self.new_order_calls = []
        self.conditional_calls = []
        self.account_api_mode = "classic"
        self.positions = []

    async def account_overview(self):
        return [{"asset": "USDT", "availableBalance": "1000", "balance": "1000"}]

    async def position_information_v3(self, symbol=None):
        if symbol:
            return [item for item in self.positions if item.get("symbol") == symbol]
        return list(self.positions)

    async def get_account_api_mode(self):
        return self.account_api_mode

    def planned_account_api_mode(self):
        return self.account_api_mode

    async def exchange_info(self):
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "triggerProtect": "0.05",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

    async def mark_price(self, symbol):
        return {"symbol": symbol, "markPrice": "100"}

    async def new_order(self, payload):
        if payload.get("reduceOnly") == "true" and self.fail_emergency:
            raise RuntimeError("emergency close failed")
        self.new_order_calls.append(dict(payload))
        if payload.get("reduceOnly") == "true":
            self.positions = [item for item in self.positions if item.get("symbol") != payload.get("symbol")]
        else:
            quantity = str(payload.get("quantity") or "0.001")
            amount = quantity if payload.get("side") == "BUY" else f"-{quantity}"
            self.positions = [
                {
                    "symbol": payload.get("symbol"),
                    "positionAmt": amount,
                    "entryPrice": "100",
                    "markPrice": "100",
                    "unRealizedProfit": "0",
                    "protection_status": "healthy",
                }
            ]
        return {"orderId": len(self.new_order_calls), **payload}

    async def new_conditional_order(self, payload):
        self.conditional_calls.append(dict(payload))
        order_type = str(payload.get("type") or payload.get("strategyType") or "").upper()
        if self.fail_stop and "STOP" in order_type:
            raise RuntimeError("protective stop failed")
        if self.fail_take_profit and "TAKE_PROFIT" in order_type:
            raise RuntimeError("take profit failed")
        return {"algoId": len(self.conditional_calls), **payload}


class FakeNoEmergencyPositionFutures(FakeTrialFutures):
    async def position_information_v3(self, symbol=None):
        del symbol
        return []


class HermesTraderTrialPipelineTests(unittest.TestCase):
    def test_runtime_snapshot_unavailable_marks_no_trade_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = build_runtime_trader_snapshot(root=tmp)
            risk = evaluate_risk(
                validate_hermes_decision(valid_enter_long()).decision or {},
                snapshot,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            )

            self.assertFalse(snapshot["system_status"]["data_fresh"])
            self.assertEqual(snapshot["system_status"]["websocket_status"], "unavailable")
            self.assertFalse(risk.approved)
            self.assertIn("stale_snapshot", risk.blocked_by)

    def test_runtime_snapshot_uses_compact_market_stream_and_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "btc_data" / "state"
            state.mkdir(parents=True)
            now = datetime.now(timezone.utc).isoformat()
            (state / "market_stream_state.json").write_text(json.dumps({"status": "connected", "last_event_at": now}), encoding="utf-8")
            (state / "market_stream_snapshot.json").write_text(
                json.dumps(
                    {
                        "generated_at": now,
                        "source": "ws",
                        "per_symbol": {
                            "BTCUSDT": {
                                "price_change_5m_pct": 0.3,
                                "price_change_15m_pct": 0.5,
                                "mid_price": 100.0,
                                "depth": {
                                    "spread_bps": 1.0,
                                    "depth_bid_5": 100000,
                                    "depth_ask_5": 90000,
                                    "depth_imbalance": 0.05,
                                    "estimated_slippage_bps_buy": 1.1,
                                },
                            },
                            "ETHUSDT": {
                                "price_change_5m_pct": 0.2,
                                "price_change_15m_pct": 0.4,
                                "mid_price": 50.0,
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            (root / "phoenix_candidates.json").write_text(
                json.dumps({"top_candidates": [{"symbol": "BTCUSDT", "score": 88.0, "directional_bias": "LONG"}]}),
                encoding="utf-8",
            )

            snapshot = build_runtime_trader_snapshot(root=root, protective_stop_path_available=True, emergency_close_available=True)

            self.assertTrue(snapshot["system_status"]["data_fresh"])
            self.assertEqual(snapshot["system_status"]["websocket_status"], "healthy")
            self.assertEqual(snapshot["top_candidates"][0]["symbol"], "BTCUSDT")
            self.assertEqual(snapshot["top_candidates"][0]["spread_bps"], 1.0)

    def test_invalid_file_provider_json_falls_back_to_no_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "decision.json"
            path.write_text(json.dumps({"action": "ENTER_LONG", "symbol": "BTCUSDT", "source": "HERMES"}), encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(path).decide(fresh_snapshot()))

            self.assertFalse(result.ok)
            self.assertEqual(result.decision["action"], "NO_TRADE")
            self.assertIn("hermes_decision_invalid", result.fallback_reason or "")

    def test_file_provider_valid_decision_passes_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "decision.json"
            path.write_text(json.dumps(valid_enter_long()), encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(path).decide(fresh_snapshot()))

            self.assertTrue(result.ok)
            self.assertEqual(result.decision["action"], "ENTER_LONG")

    def test_http_provider_timeout_falls_back_to_no_trade(self) -> None:
        provider = HttpHermesDecisionProvider("http://127.0.0.1:1", timeout_sec=0.01)

        async def slow(_snapshot):
            await asyncio.sleep(0.1)
            return valid_enter_long()

        provider._post_snapshot = slow  # type: ignore[method-assign]
        result = asyncio.run(provider.decide(fresh_snapshot()))

        self.assertFalse(result.ok)
        self.assertEqual(result.decision["action"], "NO_TRADE")
        self.assertEqual(result.fallback_reason, "http_provider_timeout")

    def test_testnet_mode_manual_snapshot_rejects_before_executor_callback(self) -> None:
        calls = []

        async def callback(intent):
            calls.append(intent)
            return {"order_submitted": True, "status": "fake_testnet_order"}

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_payload=fresh_snapshot(),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=callback,
            )

            self.assertEqual(calls, [])
            self.assertTrue(result["frozen"])
            self.assertEqual(result["freeze_reason"], "manual_snapshot_not_allowed_for_testnet_order_mode")
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertFalse(result["can_continue"])
            self.assertFalse(result["execution_result"]["order_submitted"])

    def test_dry_run_mode_manual_snapshot_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_payload=fresh_snapshot(),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=True,
                environment={"runtime_mode": "DRY_RUN", "env": "testnet"},
            )

            self.assertFalse(result.get("frozen", False))
            self.assertEqual(result["result_type"], "approved_dry_run")
            self.assertTrue(result["risk_governor_result"]["approved"])
            self.assertFalse(result["execution_result"]["order_submitted"])

    def test_testnet_mode_snapshot_file_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot_file = Path(tmp) / "snapshot.json"
            snapshot_file.write_text(json.dumps(fresh_snapshot()), encoding="utf-8")

            exit_code = trader_main(
                [
                    "--single-cycle",
                    "--mode",
                    "testnet",
                    "--snapshot-file",
                    str(snapshot_file),
                    "--output-dir",
                    str(Path(tmp) / "logs"),
                ]
            )

            self.assertEqual(exit_code, 2)

    def test_testnet_mode_runtime_snapshot_can_continue_to_risk_governor(self) -> None:
        calls = []

        async def callback(intent):
            calls.append(intent)
            return {"order_submitted": True, "status": "fake_testnet_order"}

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=trusted_runtime_snapshot,
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=callback,
            )

            self.assertEqual(len(calls), 1)
            self.assertFalse(result.get("frozen", False))
            self.assertEqual(result["result_type"], "completed")
            self.assertTrue(result["risk_governor_result"]["approved"])
            self.assertTrue(result["execution_result"]["order_submitted"])

    def test_risk_reject_prevents_executor_callback(self) -> None:
        calls = []

        async def callback(intent):
            calls.append(intent)
            return {"order_submitted": True}

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_payload=fresh_snapshot(system_status={"data_fresh": False}),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=callback,
            )

            self.assertEqual(calls, [])
            self.assertFalse(result["risk_governor_result"]["approved"])
            self.assertFalse(result["execution_result"]["order_submitted"])

    def test_spread_too_wide_is_soft_reject_and_trial_can_continue(self) -> None:
        calls = []
        snapshot = trusted_runtime_snapshot()
        snapshot["top_candidates"][0]["spread_bps"] = 50.0

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=lambda: snapshot,
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: calls.append(intent),
            )

            self.assertEqual(calls, [])
            self.assertEqual(result["result_type"], "soft_reject")
            self.assertFalse(result["frozen"])
            self.assertTrue(result["can_continue"])
            self.assertIsNone(result["freeze_reason"])

    def test_direction_lock_conflict_is_soft_reject_and_trial_can_continue(self) -> None:
        snapshot = trusted_runtime_snapshot()
        snapshot["market_regime"]["regime"] = "TREND_DOWN"
        snapshot["market_regime"]["direction_lock"] = "SHORT_ONLY_OR_NO_TRADE"

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=lambda: snapshot,
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: {"order_submitted": True},
            )

            self.assertEqual(result["result_type"], "soft_reject")
            self.assertFalse(result["frozen"])
            self.assertTrue(result["can_continue"])

    def test_cooldown_active_is_soft_reject_and_trial_can_continue(self) -> None:
        snapshot = trusted_runtime_snapshot()
        snapshot["account_risk"]["cooldown_active"] = True

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=lambda: snapshot,
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: {"order_submitted": True},
            )

            self.assertEqual(result["result_type"], "soft_reject")
            self.assertFalse(result["frozen"])
            self.assertTrue(result["can_continue"])

    def test_hermes_no_trade_is_soft_reject_without_executor_callback(self) -> None:
        calls = []

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=trusted_runtime_snapshot,
                decision_provider=MockHermesDecisionProvider(no_trade_decision()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: calls.append(intent),
            )

            self.assertEqual(calls, [])
            self.assertFalse(result["risk_governor_result"]["approved"])
            self.assertEqual(result["result_type"], "soft_reject")
            self.assertIn("no_trade_from_hermes", result["safe_order_gateway_result"]["blocked_by"])
            self.assertFalse(result["frozen"])
            self.assertTrue(result["can_continue"])
            self.assertIsNone(result["freeze_reason"])
            self.assertFalse(result["execution_result"]["order_submitted"])
            self.assertNotEqual(result["execution_result"]["result_type"], "approved_no_executor")

    def test_wait_for_trigger_without_active_trigger_is_soft_reject(self) -> None:
        calls = []

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=trusted_runtime_snapshot,
                decision_provider=MockHermesDecisionProvider(wait_for_trigger_decision()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: calls.append(intent),
            )

            self.assertEqual(calls, [])
            self.assertFalse(result["risk_governor_result"]["approved"])
            self.assertEqual(result["result_type"], "soft_reject")
            self.assertIn("wait_for_trigger", result["safe_order_gateway_result"]["blocked_by"])
            self.assertFalse(result["frozen"])
            self.assertTrue(result["can_continue"])
            self.assertFalse(result["execution_result"]["order_submitted"])
            self.assertNotEqual(result["execution_result"]["result_type"], "approved_no_executor")

    def test_stale_snapshot_is_hard_freeze_and_trial_stops(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=lambda: trusted_runtime_snapshot(data_fresh=False, trusted_runtime_snapshot=False),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: {"order_submitted": True},
            )

            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertTrue(result["frozen"])
            self.assertFalse(result["can_continue"])
            self.assertIn("stale_snapshot", result["safe_order_gateway_result"]["blocked_by"])

    def test_exchange_unknown_is_hard_freeze_and_trial_stops(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=lambda: trusted_runtime_snapshot(exchange_status="unknown", trusted_runtime_snapshot=False),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: {"order_submitted": True},
            )

            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertTrue(result["frozen"])
            self.assertFalse(result["can_continue"])

    def test_protective_stop_unavailable_rejects_entry(self) -> None:
        calls = []

        async def callback(intent):
            calls.append(intent)
            return {"order_submitted": True}

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=lambda: trusted_runtime_snapshot(
                    protective_stop_path_available=False,
                    protective_stop_capability_source="unverified",
                    trusted_runtime_snapshot=False,
                ),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=callback,
            )

            self.assertEqual(calls, [])
            self.assertTrue(result["frozen"])
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertFalse(result["can_continue"])
            self.assertIn("protective_stop", result["freeze_reason"])

    def test_emergency_close_unavailable_rejects_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_builder=lambda: trusted_runtime_snapshot(
                    emergency_close_available=False,
                    emergency_close_capability_source="unverified",
                    trusted_runtime_snapshot=False,
                ),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: {"order_submitted": True},
            )

            self.assertTrue(result["frozen"])
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertFalse(result["can_continue"])
            self.assertIn("emergency_close", result["freeze_reason"])

    def test_executor_callback_cannot_fabricate_healthy_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            callback = TestnetExecutorCallback(
                snapshot=fresh_snapshot(),
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                client_factory=lambda _session: FakeTrialFutures(),
            )

            result = asyncio.run(callback({"action": "ENTER_LONG", "symbol": "BTCUSDT", "side": "BUY", "required_protective_orders": [{}]}))

            self.assertFalse(result["order_submitted"])
            self.assertTrue(result["frozen"])
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertEqual(result["freeze_reason"], "manual_snapshot_not_allowed_for_testnet_order_mode")

    def test_testnet_callback_requires_explicit_micro_notional(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            callback = TestnetExecutorCallback(
                snapshot=trusted_runtime_snapshot(),
                output_dir=tmp,
                client_factory=lambda _session: FakeTrialFutures(),
            )

            result = asyncio.run(
                callback(
                    {
                        "action": "ENTER_LONG",
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "raw_intent": valid_enter_long(),
                        "required_protective_orders": [{}],
                    }
                )
            )

            self.assertFalse(result["order_submitted"])
            self.assertTrue(result["frozen"])
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertEqual(result["freeze_reason"], "micro_notional_required")

    def test_testnet_callback_submits_entry_stop_and_take_profit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeTrialFutures()
            callback = TestnetExecutorCallback(
                snapshot=trusted_runtime_snapshot(),
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                client_factory=lambda _session: fake,
            )

            result = asyncio.run(
                callback(
                    {
                        "action": "ENTER_LONG",
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "raw_intent": valid_enter_long(),
                        "required_protective_orders": [{}, {}],
                    }
                )
            )

            self.assertTrue(result["order_submitted"])
            self.assertFalse(result["frozen"])
            self.assertEqual(result["status"], "testnet_order_submitted_with_protective_stop_and_take_profit")
            self.assertEqual(len(fake.new_order_calls), 1)
            self.assertEqual(len(fake.conditional_calls), 2)
            self.assertEqual(fake.conditional_calls[0]["type"], "STOP_MARKET")
            self.assertEqual(fake.conditional_calls[1]["type"], "TAKE_PROFIT_MARKET")
            self.assertIn("protective_stop_response", result)
            self.assertIn("take_profit_response", result)

    def test_testnet_callback_uses_explicit_stop_and_take_profit_prices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeTrialFutures()
            decision = valid_enter_long()
            decision.pop("stop_loss_pct")
            decision.pop("take_profit_pct")
            decision["stop_loss_price"] = 98.5
            decision["take_profit_price"] = 103.5
            callback = TestnetExecutorCallback(
                snapshot=trusted_runtime_snapshot(),
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                client_factory=lambda _session: fake,
            )

            result = asyncio.run(
                callback(
                    {
                        "action": "ENTER_LONG",
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "raw_intent": decision,
                        "required_protective_orders": [{}, {}],
                    }
                )
            )

            self.assertTrue(result["order_submitted"])
            self.assertEqual(fake.conditional_calls[0]["triggerPrice"], 98.5)
            self.assertEqual(fake.conditional_calls[1]["triggerPrice"], 103.5)

    def test_take_profit_failed_emergency_close_success_freezes_trial(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeTrialFutures(fail_take_profit=True)
            callback = TestnetExecutorCallback(
                snapshot=trusted_runtime_snapshot(),
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                client_factory=lambda _session: fake,
            )

            result = asyncio.run(
                callback(
                    {
                        "action": "ENTER_LONG",
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "raw_intent": valid_enter_long(),
                        "required_protective_orders": [{}, {}],
                    }
                )
            )

            self.assertTrue(result["order_submitted"])
            self.assertTrue(result["frozen"])
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertEqual(result["freeze_reason"], "take_profit_failed")
            self.assertTrue(result["emergency_close"]["ok"])
            self.assertFalse(result["can_continue"])

    def test_protective_stop_failed_emergency_close_success_freezes_trial(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeTrialFutures(fail_stop=True)
            callback = TestnetExecutorCallback(
                snapshot=trusted_runtime_snapshot(),
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                client_factory=lambda _session: fake,
            )

            result = asyncio.run(
                callback(
                    {
                        "action": "ENTER_LONG",
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "raw_intent": valid_enter_long(),
                        "required_protective_orders": [{}],
                    }
                )
            )

            self.assertTrue(result["order_submitted"])
            self.assertTrue(result["frozen"])
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertEqual(result["freeze_reason"], "protective_stop_failed")
            self.assertTrue(result["emergency_close"]["ok"])
            self.assertFalse(result["can_continue"])

    def test_protective_stop_failed_emergency_close_failed_freezes_trial(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeTrialFutures(fail_stop=True, fail_emergency=True)
            callback = TestnetExecutorCallback(
                snapshot=trusted_runtime_snapshot(),
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                client_factory=lambda _session: fake,
            )

            result = asyncio.run(
                callback(
                    {
                        "action": "ENTER_LONG",
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "raw_intent": valid_enter_long(),
                        "required_protective_orders": [{}],
                    }
                )
            )

            self.assertTrue(result["frozen"])
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertEqual(result["freeze_reason"], "emergency_close_failed")
            self.assertFalse(result["emergency_close"]["ok"])
            self.assertFalse(result["can_continue"])

    def test_emergency_close_without_real_position_state_hard_freezes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeNoEmergencyPositionFutures(fail_stop=True)
            callback = TestnetExecutorCallback(
                snapshot=trusted_runtime_snapshot(),
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                client_factory=lambda _session: fake,
            )

            result = asyncio.run(
                callback(
                    {
                        "action": "ENTER_LONG",
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "raw_intent": valid_enter_long(),
                        "required_protective_orders": [{}],
                    }
                )
            )

            self.assertTrue(result["frozen"])
            self.assertEqual(result["result_type"], "hard_freeze")
            self.assertEqual(result["freeze_reason"], "emergency_close_failed")
            self.assertFalse(result["emergency_close"]["ok"])
            self.assertIn("emergency_close_runtime_state_unavailable", result["emergency_close"]["error"])

    def test_emergency_path_does_not_fabricate_healthy_snapshot(self) -> None:
        source = inspect.getsource(testnet_executor_callback._attempt_emergency_close)

        self.assertNotIn("_emergency_snapshot", source)
        self.assertNotIn('"trusted_runtime_snapshot": True', source)
        self.assertNotIn('"data_fresh": True', source)
        self.assertNotIn('"websocket_status": "healthy"', source)
        self.assertNotIn('"protective_stop_path_available": True', source)
        self.assertNotIn('"emergency_close_available": True', source)

    def test_no_contradictory_freeze_continue_state(self) -> None:
        scenarios = [
            run_trader_cycle(
                snapshot_builder=lambda: trusted_runtime_snapshot(),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tempfile.mkdtemp(),
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: {"order_submitted": True},
            ),
            run_trader_cycle(
                snapshot_builder=lambda: trusted_runtime_snapshot(data_fresh=False, trusted_runtime_snapshot=False),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tempfile.mkdtemp(),
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=lambda intent: {"order_submitted": True},
            ),
        ]

        for result in scenarios:
            self.assertFalse(result["frozen"] and result["can_continue"])
            if not result["frozen"] and result["can_continue"] is False:
                self.assertEqual(result["result_type"], "completed")

    def test_replay_log_contains_full_cycle_events_with_trace_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_payload=fresh_snapshot(),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=True,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            )
            rows = [json.loads(line) for line in (Path(tmp) / "replay_events.jsonl").read_text(encoding="utf-8").splitlines()]
            events = {row["event"] for row in rows}

            self.assertIn("snapshot", events)
            self.assertIn("hermes_decision_raw", events)
            self.assertIn("hermes_decision_validated", events)
            self.assertIn("risk_governor_result", events)
            self.assertIn("safe_order_gateway_result", events)
            self.assertIn("execution_intent", events)
            self.assertIn("execution_result", events)
            self.assertIn("review_report", events)
            self.assertTrue(all(row.get("trace_id") for row in rows))
            self.assertEqual(result["execution_result"]["status"], "dry_run_intent_only")
            execution_rows = [row for row in rows if row.get("event") == "execution_result"]
            self.assertEqual(len(execution_rows), 1)
            self.assertFalse(execution_rows[0]["executor_called"])
            self.assertFalse(execution_rows[0]["order_submitted"])
            self.assertFalse(execution_rows[0]["testnet_order_submitted"])
            self.assertFalse(execution_rows[0]["mainnet_order_submitted"])

    def test_replay_and_review_preserve_testnet_exploration_enter_reason(self) -> None:
        reason = "Stage 2 testnet exploration: candidate ready for Binance testnet sandbox."
        decision = {**valid_enter_long(), "reason": reason}
        with tempfile.TemporaryDirectory() as tmp:
            run_trader_cycle(
                snapshot_payload=fresh_snapshot(),
                decision_provider=MockHermesDecisionProvider(decision),
                output_dir=tmp,
                dry_run=True,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            )
            rows = [json.loads(line) for line in (Path(tmp) / "replay_events.jsonl").read_text(encoding="utf-8").splitlines()]
            raw = next(row for row in rows if row["event"] == "hermes_decision_raw")
            normalized = next(row for row in rows if row["event"] == "hermes_decision_normalized")
            review = next(row for row in rows if row["event"] == "review_report")

            self.assertEqual(raw["decision"]["reason"], reason)
            self.assertEqual(normalized["normalized_decision"]["reason"], reason)
            self.assertEqual(review["payload"]["reason"], reason)
            self.assertIn(reason, review["text"])
            self.assertNotIn("dry-run", json.dumps(raw, ensure_ascii=False))
            self.assertNotIn("dry-run", json.dumps(normalized, ensure_ascii=False))
            self.assertNotIn("dry-run", json.dumps(review, ensure_ascii=False))

    def test_review_reporter_outputs_readable_cantonese(self) -> None:
        report = build_review_report("RISK_REJECT", {"action": "ENTER_LONG", "symbol": "BTCUSDT", "blocked_by": ["spread_too_wide"]})

        self.assertIn("【Risk Governor 拒绝】", report.text)
        self.assertIn("Phoenix 唔会执行", report.text)
        self.assertIn("BTCUSDT", report.text)


if __name__ == "__main__":
    unittest.main()
