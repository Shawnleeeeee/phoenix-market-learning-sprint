import asyncio
import tempfile
import unittest
from pathlib import Path

from phoenix.review_reporter import build_review_report
from phoenix.risk_governor import RiskGovernorConfig
from phoenix.stage2_lifecycle_monitor import (
    Stage2LifecycleMonitor,
    Stage2ProtectedPosition,
    protection_status_for_orders,
)
from phoenix.testnet_executor_callback import TestnetExecutorCallback
from phoenix.trader_snapshot import build_trader_snapshot


def trusted_runtime_snapshot():
    snapshot = build_trader_snapshot(
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
            "open_positions_count": 1,
            "max_open_positions": 1,
            "loss_streak": 0,
            "cooldown_active": False,
        },
        positions=[
            {
                "symbol": "ZECUSDT",
                "side": "LONG",
                "entry_price": 531.12,
                "current_price": 536.62,
                "protection_status": "healthy",
            }
        ],
        candidates=[
            {
                "symbol": "ZECUSDT",
                "bias": "LONG",
                "setup_type": "QUICK_TRADE",
                "score": 90.0,
                "current_price": 536.62,
                "spread_bps": 1.0,
                "estimated_slippage_bps": 1.0,
                "liquidity_ok": True,
                "suggested_stop_pct": 0.6,
                "suggested_tp_pct": 1.2,
                "max_holding_time_sec": 300,
            }
        ],
        system_status={
            "source": "runtime",
            "snapshot_source": "runtime",
            "trusted_runtime_snapshot": True,
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
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
        },
    )
    return snapshot


def valid_enter_long():
    return {
        "action": "ENTER_LONG",
        "symbol": "ZECUSDT",
        "trade_type": "QUICK_TRADE",
        "confidence": 0.8,
        "reason": "Hermes saw a clean continuation setup",
        "entry_price_hint": 531.12,
        "stop_loss_price": 524.57,
        "take_profit_price": 539.17,
        "max_holding_time_sec": 300,
        "invalidation_condition": "momentum fades",
        "source": "HERMES",
    }


def protected_position(entry_filled_time_ms=1_000_000):
    return Stage2ProtectedPosition(
        symbol="ZECUSDT",
        direction="ENTER_LONG",
        entry_side="BUY",
        close_side="SELL",
        entry_order_id=123,
        entry_price=531.12,
        position_size=0.018,
        entry_filled_time_ms=entry_filled_time_ms,
        stop_loss_order_id=1001,
        stop_loss_client_id="stop-client",
        take_profit_order_id=1002,
        take_profit_client_id="tp-client",
        stop_loss_price=524.57,
        take_profit_price=539.17,
        max_holding_time_sec=300,
        invalidation_condition="momentum fades",
        order_intent={
            "symbol": "ZECUSDT",
            "side": "BUY",
            "entry_price": 531.12,
            "quantity": 0.018,
            "leverage": 2,
            "quote_allocation_usdt": 5.0,
            "notional_usdt": 9.56,
            "margin_type": "ISOLATED",
            "take_profit_price": 539.17,
        },
    )


class FakeLifecycleFutures:
    def __init__(self):
        self.position_open = True
        self.new_order_calls = []
        self.cancel_conditional_calls = []
        self.conditional_orders = [
            {
                "algoId": 1001,
                "clientAlgoId": "stop-client",
                "orderType": "STOP_MARKET",
                "symbol": "ZECUSDT",
                "side": "SELL",
                "algoStatus": "NEW",
                "reduceOnly": True,
                "closePosition": True,
                "triggerPrice": "524.57",
                "stopPrice": "524.57",
            },
            {
                "algoId": 1002,
                "clientAlgoId": "tp-client",
                "orderType": "TAKE_PROFIT_MARKET",
                "symbol": "ZECUSDT",
                "side": "SELL",
                "algoStatus": "NEW",
                "reduceOnly": True,
                "closePosition": True,
                "triggerPrice": "539.17",
                "stopPrice": "539.17",
            },
        ]
        self.session = None

    async def account_overview(self):
        return [{"asset": "USDT", "availableBalance": "1000", "balance": "1000"}]

    async def exchange_info(self):
        return {"symbols": [{"symbol": "ZECUSDT"}]}

    async def position_information_v3(self, symbol=None):
        row = {
            "symbol": "ZECUSDT",
            "positionAmt": "0.018",
            "entryPrice": "531.12",
            "markPrice": "536.62",
            "unRealizedProfit": "0.099",
            "protection_status": "healthy",
        }
        if not self.position_open:
            return []
        if symbol in (None, "ZECUSDT"):
            return [row]
        return []

    async def open_orders(self, symbol=None):
        return []

    async def open_conditional_orders(self, symbol=None):
        if symbol not in (None, "ZECUSDT"):
            return []
        return list(self.conditional_orders)

    async def mark_price(self, symbol):
        return {"symbol": symbol, "markPrice": "536.62"}

    async def user_trades(self, symbol, **kwargs):
        return [{"side": "SELL", "qty": "0.018", "quoteQty": "9.65916", "realizedPnl": "0.099", "price": "536.62"}]

    async def income_history(self, symbol=None, **kwargs):
        return [{"incomeType": "REALIZED_PNL", "income": "0.099", "symbol": symbol or "ZECUSDT"}]

    async def new_order(self, payload):
        self.new_order_calls.append(dict(payload))
        if payload.get("reduceOnly") == "true" and payload.get("side") == "SELL":
            self.position_open = False
        return {"orderId": 456, "status": "FILLED", "avgPrice": "536.62", **payload}

    async def new_conditional_order(self, payload):
        raise AssertionError("lifecycle monitor must not create new protection orders")

    async def cancel_conditional_order(self, symbol, *, strategy_id=None, client_strategy_id=None, **kwargs):
        self.cancel_conditional_calls.append(
            {"symbol": symbol, "strategy_id": strategy_id, "client_strategy_id": client_strategy_id}
        )
        self.conditional_orders = [
            order for order in self.conditional_orders
            if str(order.get("algoId")) != str(strategy_id)
            and str(order.get("clientAlgoId")) != str(client_strategy_id)
        ]
        return {"cancelled": True, "strategyId": strategy_id, "clientAlgoId": client_strategy_id}


class FakeNoFollowThroughFutures(FakeLifecycleFutures):
    async def mark_price(self, symbol):
        return {"symbol": symbol, "markPrice": "531.12"}


class FakeEntryFutures(FakeLifecycleFutures):
    def __init__(self):
        super().__init__()
        self.position_open = False
        self.conditional_orders = []
        self.account_api_mode = "classic"
        self.conditional_calls = []

    async def get_account_api_mode(self):
        return self.account_api_mode

    def planned_account_api_mode(self):
        return self.account_api_mode

    async def exchange_info(self):
        return {
            "symbols": [
                {
                    "symbol": "ZECUSDT",
                    "triggerProtect": "0.05",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

    async def new_order(self, payload):
        self.new_order_calls.append(dict(payload))
        if payload.get("reduceOnly") == "true":
            self.position_open = False
            return {"orderId": 789, "status": "FILLED", "avgPrice": "536.62", "updateTime": 1_303_000, **payload}
        self.position_open = True
        return {
            "orderId": 123,
            "status": "FILLED",
            "avgPrice": "531.12",
            "executedQty": str(payload.get("quantity")),
            "updateTime": 1_000_000,
            **payload,
        }

    async def new_conditional_order(self, payload):
        self.conditional_calls.append(dict(payload))
        order_id = 1000 + len(self.conditional_calls)
        order = {
            "algoId": order_id,
            "clientAlgoId": payload.get("newClientStrategyId") or payload.get("clientAlgoId"),
            "orderType": payload.get("type"),
            "symbol": payload.get("symbol"),
            "side": payload.get("side"),
            "algoStatus": "NEW",
            "reduceOnly": bool(payload.get("reduceOnly", True)),
            "closePosition": bool(payload.get("closePosition", True)),
            "triggerPrice": payload.get("triggerPrice") or payload.get("stopPrice"),
            "stopPrice": payload.get("stopPrice") or payload.get("triggerPrice"),
        }
        self.conditional_orders.append(order)
        return {**order, **payload}


class FakeClosedSession:
    closed = True


class SessionAwareFakeEntryFutures(FakeEntryFutures):
    def __init__(self, session):
        super().__init__()
        self.session = session


class Stage2LifecycleMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_close_uses_entry_filled_time_and_reconciles_protection_orders(self):
        futures = FakeLifecycleFutures()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            monitor = Stage2LifecycleMonitor(
                futures=futures,
                protected_position=protected_position(entry_filled_time_ms=1_000_000),
                source_snapshot=trusted_runtime_snapshot(),
                output_dir=tmp_path,
                risk_config=RiskGovernorConfig(),
                now_ms=lambda: 1_301_000,
                poll_sec=0.01,
                post_close_wait_sec=1,
            )

            result = await monitor.monitor_until_closed()
            gateway_rows = [
                line for line in (tmp_path / "lifecycle_safe_order_gateway.jsonl").read_text(encoding="utf-8").splitlines()
                if '"approved": true' in line
            ]
            review_report_exists = (tmp_path / "stage2_lifecycle_review_report.jsonl").exists()

        self.assertEqual(result["final_close_reason"], "CLOSED_BY_TIMEOUT")
        self.assertEqual(result["holding_time_sec"], 301.0)
        self.assertEqual(futures.new_order_calls, [
            {
                "symbol": "ZECUSDT",
                "side": "SELL",
                "type": "MARKET",
                "quantity": "0.018",
                "reduceOnly": "true",
                "newOrderRespType": "RESULT",
            }
        ])
        self.assertEqual(len(futures.cancel_conditional_calls), 2)
        self.assertEqual(result["ending_positions_count"], 0)
        self.assertEqual(result["ending_open_orders_count"], 0)
        self.assertEqual(result["ending_conditional_orders_count"], 0)
        self.assertFalse(result["orphan_protective_order"])
        self.assertEqual(result["close_snapshot_status"]["stop_protection_status"], "healthy")
        self.assertTrue(result["close_snapshot_status"]["trusted_runtime_snapshot"])
        self.assertFalse(result["mainnet_order_submitted"])
        self.assertTrue(gateway_rows)
        self.assertEqual(result["stop_loss_price"], 524.57)
        self.assertEqual(result["take_profit_price"], 539.17)
        self.assertEqual(result["entry_fill_price"], 531.12)
        self.assertEqual(result["close_fill_price"], 536.62)
        self.assertEqual(result["realized_pnl_usdt"], 0.099)
        self.assertEqual(result["fees"], 0.0)
        self.assertEqual(result["max_holding_sec"], 300)
        self.assertEqual(result["close_reason"], "CLOSED_BY_TIMEOUT")
        self.assertTrue(result["timeout_due_at"].endswith("+00:00"))
        self.assertGreater(result["roi_pct"], 0)
        self.assertEqual(result["close_snapshot_status"]["stop_order_id"], 1001)
        self.assertEqual(result["close_snapshot_status"]["stop_client_order_id"], "stop-client")
        self.assertEqual(result["close_snapshot_status"]["stop_status"], "NEW")
        self.assertTrue(result["close_snapshot_status"]["stop_reduce_only"])
        self.assertTrue(result["close_snapshot_status"]["stop_close_position"])
        self.assertEqual(result["close_snapshot_status"]["stop_type"], "STOP_MARKET")
        self.assertEqual(result["close_snapshot_status"]["take_profit_order_id"], 1002)
        self.assertEqual(result["close_snapshot_status"]["take_profit_client_order_id"], "tp-client")
        self.assertEqual(result["close_snapshot_status"]["take_profit_status"], "NEW")
        self.assertTrue(result["close_snapshot_status"]["take_profit_reduce_only"])
        self.assertTrue(result["close_snapshot_status"]["take_profit_close_position"])
        self.assertEqual(result["close_snapshot_status"]["take_profit_type"], "TAKE_PROFIT_MARKET")
        self.assertEqual(result["close_snapshot_status"]["protection_verification_source"], "open_conditional_orders")
        self.assertTrue(result["close_snapshot_status"]["lifecycle_exit_snapshot"])
        review_text = result["review_report"]["text"]
        self.assertIn("ZECUSDT", review_text)
        self.assertIn("做多", review_text)
        self.assertIn("531.12", review_text)
        self.assertIn("536.62", review_text)
        self.assertIn("0.099", review_text)
        self.assertIn("524.57", review_text)
        self.assertIn("539.17", review_text)
        self.assertIn("300", review_text)
        self.assertNotIn("方向未知", review_text)
        self.assertNotIn("入场价：n/a", review_text)
        self.assertNotIn("止损价：None", review_text)
        self.assertNotIn("止盈价：None", review_text)
        self.assertNotIn("PnL 未回报", review_text)
        self.assertTrue(review_report_exists)

    async def test_no_follow_through_close_uses_reduce_only_safe_order_gateway(self):
        futures = FakeNoFollowThroughFutures()
        no_follow_position = protected_position(entry_filled_time_ms=1_000_000)
        no_follow_position.order_intent.update(
            {
                "no_follow_through_exit_sec": 120,
                "no_follow_through_min_mfe_pct": 0.0,
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            monitor = Stage2LifecycleMonitor(
                futures=futures,
                protected_position=no_follow_position,
                source_snapshot=trusted_runtime_snapshot(),
                output_dir=tmp_path,
                risk_config=RiskGovernorConfig(),
                now_ms=lambda: 1_120_000,
                poll_sec=0.01,
                post_close_wait_sec=1,
            )

            result = await monitor.monitor_until_closed()
            gateway_text = (tmp_path / "lifecycle_safe_order_gateway.jsonl").read_text(encoding="utf-8")

        self.assertEqual(result["final_close_reason"], "CLOSED_BY_NO_FOLLOW_THROUGH")
        self.assertTrue(result["no_follow_through_exit"])
        self.assertFalse(result["hard_freeze"])
        self.assertEqual(len(futures.new_order_calls), 1)
        self.assertEqual(futures.new_order_calls[0]["side"], "SELL")
        self.assertEqual(futures.new_order_calls[0]["reduceOnly"], "true")
        self.assertIn("stage2_lifecycle_monitor:no_follow_through", gateway_text)
        self.assertEqual(len(futures.cancel_conditional_calls), 2)
        self.assertEqual(result["ending_positions_count"], 0)
        self.assertEqual(result["ending_open_orders_count"], 0)
        self.assertEqual(result["ending_conditional_orders_count"], 0)
        self.assertFalse(result["mainnet_order_submitted"])

    async def test_lifecycle_monitor_fail_closed_when_client_session_already_closed(self):
        futures = FakeLifecycleFutures()
        futures.session = FakeClosedSession()
        with tempfile.TemporaryDirectory() as tmp:
            monitor = Stage2LifecycleMonitor(
                futures=futures,
                protected_position=protected_position(entry_filled_time_ms=1_000_000),
                source_snapshot=trusted_runtime_snapshot(),
                output_dir=tmp,
                risk_config=RiskGovernorConfig(),
                now_ms=lambda: 1_301_000,
                poll_sec=0.01,
                post_close_wait_sec=1,
            )

            result = await monitor.monitor_until_closed()

        self.assertTrue(result["hard_freeze"])
        self.assertEqual(result["final_close_reason"], "HARD_FREEZE")
        self.assertEqual(result["freeze_reason"], "lifecycle_client_session_closed")
        self.assertEqual(futures.new_order_calls, [])

    async def test_emergency_close_for_missing_protection_still_goes_through_gateway(self):
        futures = FakeLifecycleFutures()
        futures.conditional_orders = [futures.conditional_orders[0]]
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            monitor = Stage2LifecycleMonitor(
                futures=futures,
                protected_position=protected_position(entry_filled_time_ms=1_000_000),
                source_snapshot=trusted_runtime_snapshot(),
                output_dir=tmp_path,
                risk_config=RiskGovernorConfig(),
                now_ms=lambda: 1_001_000,
                poll_sec=0.01,
                post_close_wait_sec=1,
            )

            result = await monitor.monitor_until_closed()
            gateway_rows = [
                line for line in (tmp_path / "lifecycle_safe_order_gateway.jsonl").read_text(encoding="utf-8").splitlines()
                if '"approved": true' in line
            ]

        self.assertEqual(result["final_close_reason"], "EMERGENCY_CLOSED")
        self.assertFalse(result["hard_freeze"])
        self.assertEqual(len(futures.new_order_calls), 1)
        self.assertEqual(futures.new_order_calls[0]["reduceOnly"], "true")
        self.assertTrue(gateway_rows)
        self.assertEqual(result["close_snapshot_status"]["take_profit_status"], "MISSING")
        self.assertEqual(result["close_snapshot_status"]["protection_verification_source"], "open_conditional_orders")

    async def test_protection_status_requires_matching_reduce_only_stop_and_tp(self):
        futures = FakeLifecycleFutures()

        status = protection_status_for_orders(
            futures.conditional_orders,
            protected_position(),
        )

        self.assertTrue(status["stop_loss_valid"])
        self.assertTrue(status["take_profit_valid"])
        self.assertEqual(status["stop_loss_order"]["algoId"], 1001)
        self.assertEqual(status["take_profit_order"]["algoId"], 1002)

    async def test_executor_callback_delegates_to_lifecycle_monitor_after_entry_is_protected(self):
        futures = FakeEntryFutures()
        lifecycle_contexts = []

        async def lifecycle_factory(context):
            lifecycle_contexts.append(context)
            return {
                "final_close_reason": "CLOSED_BY_TIMEOUT",
                "ending_positions_count": 0,
                "ending_open_orders_count": 0,
                "ending_conditional_orders_count": 0,
                "mainnet_order_submitted": False,
            }

        with tempfile.TemporaryDirectory() as tmp:
            callback = TestnetExecutorCallback(
                snapshot={
                    **trusted_runtime_snapshot(),
                    "current_positions": [],
                    "account_risk": {
                        **trusted_runtime_snapshot()["account_risk"],
                        "trading_allowed": True,
                        "open_positions_count": 0,
                        "reason_if_blocked": None,
                    },
                },
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                leverage=2,
                client_factory=lambda _session: futures,
                lifecycle_monitor_factory=lifecycle_factory,
            )

            result = await callback(
                {
                    "action": "ENTER_LONG",
                    "symbol": "ZECUSDT",
                    "side": "BUY",
                    "raw_intent": valid_enter_long(),
                    "required_protective_orders": [{}, {}],
                }
            )

        self.assertTrue(result["order_submitted"])
        self.assertFalse(result["frozen"])
        self.assertEqual(result["lifecycle_result"]["final_close_reason"], "CLOSED_BY_TIMEOUT")
        self.assertEqual(len(lifecycle_contexts), 1)
        protected = lifecycle_contexts[0]["protected_position"]
        self.assertEqual(protected.entry_filled_time_ms, 1_000_000)
        self.assertEqual(protected.max_holding_time_sec, 300)
        self.assertEqual(protected.stop_loss_order_id, 1001)
        self.assertEqual(protected.take_profit_order_id, 1002)

    async def test_executor_callback_runs_lifecycle_monitor_before_aiohttp_session_closes(self):
        lifecycle_session_closed_values = []
        captured = {}

        async def lifecycle_factory(context):
            futures = context["futures"]
            lifecycle_session_closed_values.append(futures.session.closed)
            captured["futures"] = futures
            return {
                "final_close_reason": "CLOSED_BY_TIMEOUT",
                "ending_positions_count": 0,
                "ending_open_orders_count": 0,
                "ending_conditional_orders_count": 0,
                "mainnet_order_submitted": False,
            }

        with tempfile.TemporaryDirectory() as tmp:
            callback = TestnetExecutorCallback(
                snapshot={
                    **trusted_runtime_snapshot(),
                    "current_positions": [],
                    "account_risk": {
                        **trusted_runtime_snapshot()["account_risk"],
                        "trading_allowed": True,
                        "open_positions_count": 0,
                        "reason_if_blocked": None,
                    },
                },
                output_dir=tmp,
                quote_allocation_usdt=5.0,
                leverage=2,
                client_factory=lambda session: SessionAwareFakeEntryFutures(session),
                lifecycle_monitor_factory=lifecycle_factory,
            )

            result = await callback(
                {
                    "action": "ENTER_LONG",
                    "symbol": "ZECUSDT",
                    "side": "BUY",
                    "raw_intent": valid_enter_long(),
                    "required_protective_orders": [{}, {}],
                }
            )

        self.assertTrue(result["order_submitted"])
        self.assertEqual(lifecycle_session_closed_values, [False])
        self.assertTrue(captured["futures"].session.closed)

    async def test_review_reporter_uses_action_and_price_fallbacks_without_bad_placeholders(self):
        opened = build_review_report(
            "OPENED",
            {
                "symbol": "ZECUSDT",
                "action": "ENTER_LONG",
                "entry_price_hint": 531.12,
                "stop_loss_price": 524.57,
                "take_profit_price": 539.17,
                "position_size": 0.018,
                "leverage": 2,
                "reason": "Stage 2 testnet exploration: candidate ready.",
            },
        )
        closed = build_review_report(
            "CLOSED",
            {
                "symbol": "ZECUSDT",
                "direction": "ENTER_LONG",
                "entry_price": 531.12,
                "final_close_price": 536.62,
                "final_close_reason": "CLOSED_BY_TIMEOUT",
                "holding_time_sec": 301.0,
            },
        )

        combined = opened.text + "\n" + closed.text
        self.assertIn("做多", combined)
        self.assertIn("531.12", combined)
        self.assertIn("536.62", combined)
        self.assertIn("Stage 2 testnet exploration: candidate ready.", combined)
        self.assertNotIn("方向未知", combined)
        self.assertNotIn("入场价：n/a", combined)
        self.assertNotIn("止损价：None", combined)
        self.assertNotIn("止盈价：None", combined)


if __name__ == "__main__":
    unittest.main()
