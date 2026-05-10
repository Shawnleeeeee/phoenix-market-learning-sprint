import unittest

from phoenix.models import OrderInstruction, SymbolRules, TradeIntent
from phoenix_post_fill_worker import (
    build_real_trade_outcome_record,
    ensure_unique_protection_order,
    fetch_actual_settlement,
)


class FakeConditionalClient:
    def __init__(self, orders: list[dict[str, object]]) -> None:
        self.orders = [dict(order) for order in orders]
        self.calls: list[tuple[str, object]] = []

    async def open_conditional_orders(self, symbol: str | None = None) -> list[dict[str, object]]:
        self.calls.append(("open", symbol))
        if symbol is None:
            return [dict(order) for order in self.orders]
        return [dict(order) for order in self.orders if str(order.get("symbol") or "").upper() == symbol.upper()]

    async def cancel_conditional_order(
        self,
        symbol: str,
        *,
        strategy_id: int | None = None,
        client_strategy_id: str | None = None,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            (
                "cancel",
                {
                    "symbol": symbol,
                    "strategy_id": strategy_id,
                    "client_strategy_id": client_strategy_id,
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                },
            )
        )
        remaining: list[dict[str, object]] = []
        cancelled: dict[str, object] | None = None
        for order in self.orders:
            matches_strategy = strategy_id is not None and order.get("strategyId") == strategy_id
            matches_client = client_strategy_id is not None and order.get("newClientStrategyId") == client_strategy_id
            matches_order = order_id is not None and order.get("algoId") == order_id
            matches_client_order = client_order_id is not None and order.get("clientAlgoId") == client_order_id
            if cancelled is None and (matches_strategy or matches_client or matches_order or matches_client_order):
                cancelled = order
                continue
            remaining.append(order)
        self.orders = remaining
        return {"symbol": symbol, "cancelled": cancelled}

    async def new_conditional_order(self, payload: dict[str, object]) -> dict[str, object]:
        self.calls.append(("new", dict(payload)))
        order = {
            "symbol": payload["symbol"],
            "side": payload["side"],
            "type": payload.get("type") or payload.get("strategyType"),
            "strategyType": payload.get("strategyType") or payload.get("type"),
            "positionSide": payload.get("positionSide", "BOTH"),
            "triggerPrice": payload.get("triggerPrice") or payload.get("stopPrice"),
            "stopPrice": payload.get("stopPrice") or payload.get("triggerPrice"),
            "activationPrice": payload.get("activationPrice") or payload.get("activatePrice"),
            "callbackRate": payload.get("callbackRate") or payload.get("priceRate"),
            "workingType": payload.get("workingType"),
            "priceProtect": payload.get("priceProtect"),
            "closePosition": payload.get("closePosition"),
            "reduceOnly": payload.get("reduceOnly"),
            "quantity": payload.get("quantity"),
            "strategyId": 9001,
            "newClientStrategyId": str(payload.get("clientAlgoId") or payload.get("newClientStrategyId") or "generated"),
            "algoId": 9001,
            "clientAlgoId": str(payload.get("clientAlgoId") or payload.get("newClientStrategyId") or "generated"),
        }
        self.orders.append(order)
        return order


class FakeSettlementClient:
    def __init__(self, trades: list[dict[str, object]], incomes: list[dict[str, object]] | object) -> None:
        self.trades = [dict(item) for item in trades]
        self.incomes = incomes

    async def user_trades(
        self,
        symbol: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        return [dict(item) for item in self.trades if str(item.get("symbol") or "").upper() == symbol.upper()][:limit]

    async def income_history(
        self,
        symbol: str | None = None,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        income_type: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, object]] | object:
        if not isinstance(self.incomes, list):
            return self.incomes
        if symbol is None:
            return [dict(item) for item in self.incomes][:limit]
        return [dict(item) for item in self.incomes if str(item.get("symbol") or "").upper() == symbol.upper()][:limit]


def classic_stop_instruction(stop_price: float) -> OrderInstruction:
    return OrderInstruction(
        name="breakeven_stop_replacement",
        endpoint="/fapi/v1/algoOrder",
        description="breakeven stop",
        payload={
            "symbol": "BTCUSDT",
            "side": "SELL",
            "algoType": "CONDITIONAL",
            "type": "STOP_MARKET",
            "clientAlgoId": "phoenix-be",
            "triggerPrice": stop_price,
            "stopPrice": stop_price,
            "closePosition": "true",
            "workingType": "MARK_PRICE",
            "priceProtect": "TRUE",
        },
    )


def classic_trailing_instruction(activation_price: float, callback_rate: float) -> OrderInstruction:
    return OrderInstruction(
        name="post_trigger_trailing_stop",
        endpoint="/fapi/v1/algoOrder",
        description="trailing stop",
        payload={
            "symbol": "BTCUSDT",
            "side": "SELL",
            "algoType": "CONDITIONAL",
            "type": "TRAILING_STOP_MARKET",
            "clientAlgoId": "phoenix-trail",
            "quantity": 0.1,
            "reduceOnly": "true",
            "activatePrice": activation_price,
            "activationPrice": activation_price,
            "priceRate": callback_rate,
            "callbackRate": callback_rate,
            "workingType": "MARK_PRICE",
        },
    )


class EnsureUniqueProtectionOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_reuses_identical_close_position_stop(self) -> None:
        client = FakeConditionalClient(
            [
                {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "STOP_MARKET",
                    "strategyType": "STOP_MARKET",
                    "positionSide": "BOTH",
                    "triggerPrice": 100.1,
                    "stopPrice": 100.1,
                    "workingType": "MARK_PRICE",
                    "priceProtect": "TRUE",
                    "closePosition": "true",
                    "strategyId": 101,
                    "newClientStrategyId": "phoenix-be",
                    "algoId": 101,
                    "clientAlgoId": "phoenix-be",
                }
            ]
        )

        result = await ensure_unique_protection_order(client, classic_stop_instruction(100.1))

        self.assertEqual(result["action"], "reused")
        self.assertEqual(result["order"]["strategyId"], 101)
        self.assertEqual(client.calls, [("open", "BTCUSDT")])

    async def test_cancels_old_stop_before_placing_replacement(self) -> None:
        client = FakeConditionalClient(
            [
                {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "STOP_MARKET",
                    "strategyType": "STOP_MARKET",
                    "positionSide": "BOTH",
                    "triggerPrice": 99.0,
                    "stopPrice": 99.0,
                    "workingType": "MARK_PRICE",
                    "priceProtect": "TRUE",
                    "closePosition": "true",
                    "strategyId": 102,
                    "newClientStrategyId": "phoenix-stop",
                    "algoId": 102,
                    "clientAlgoId": "phoenix-stop",
                }
            ]
        )

        result = await ensure_unique_protection_order(client, classic_stop_instruction(100.1))

        self.assertEqual(result["action"], "replaced")
        self.assertEqual([call[0] for call in client.calls], ["open", "cancel", "new"])
        self.assertEqual(result["cancelled"][0]["strategyId"], 102)
        self.assertEqual(result["order"]["stopPrice"], 100.1)

    async def test_cancels_duplicate_stops_but_keeps_matching_one(self) -> None:
        client = FakeConditionalClient(
            [
                {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "STOP_MARKET",
                    "strategyType": "STOP_MARKET",
                    "positionSide": "BOTH",
                    "triggerPrice": 100.1,
                    "stopPrice": 100.1,
                    "workingType": "MARK_PRICE",
                    "priceProtect": "TRUE",
                    "closePosition": "true",
                    "strategyId": 103,
                    "newClientStrategyId": "phoenix-be",
                    "algoId": 103,
                    "clientAlgoId": "phoenix-be",
                },
                {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "STOP_MARKET",
                    "strategyType": "STOP_MARKET",
                    "positionSide": "BOTH",
                    "triggerPrice": 99.0,
                    "stopPrice": 99.0,
                    "workingType": "MARK_PRICE",
                    "priceProtect": "TRUE",
                    "closePosition": "true",
                    "strategyId": 104,
                    "newClientStrategyId": "phoenix-old-stop",
                    "algoId": 104,
                    "clientAlgoId": "phoenix-old-stop",
                },
            ]
        )

        result = await ensure_unique_protection_order(client, classic_stop_instruction(100.1))

        self.assertEqual(result["action"], "reused")
        self.assertEqual([call[0] for call in client.calls], ["open", "cancel"])
        self.assertEqual(result["order"]["strategyId"], 103)
        self.assertEqual(result["cancelled"][0]["strategyId"], 104)

    async def test_reuses_identical_trailing_stop(self) -> None:
        client = FakeConditionalClient(
            [
                {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "TRAILING_STOP_MARKET",
                    "strategyType": "TRAILING_STOP_MARKET",
                    "positionSide": "BOTH",
                    "activationPrice": 101.2,
                    "activatePrice": 101.2,
                    "callbackRate": 0.5,
                    "priceRate": 0.5,
                    "workingType": "MARK_PRICE",
                    "reduceOnly": "true",
                    "quantity": 0.1,
                    "strategyId": 105,
                    "newClientStrategyId": "phoenix-trail",
                    "algoId": 105,
                    "clientAlgoId": "phoenix-trail",
                }
            ]
        )

        result = await ensure_unique_protection_order(client, classic_trailing_instruction(101.2, 0.5))

        self.assertEqual(result["action"], "reused")
        self.assertEqual(result["order"]["strategyId"], 105)
        self.assertEqual(client.calls, [("open", "BTCUSDT")])


class FetchActualSettlementTests(unittest.IsolatedAsyncioTestCase):
    def build_intent(self, *, side: str = "SELL") -> TradeIntent:
        return TradeIntent(
            symbol="SIRENUSDT",
            side=side,
            entry_price=0.68253,
            quantity=146.0,
            leverage=10,
            quote_allocation_usdt=10.0,
            notional_usdt=99.64938,
            margin_type="ISOLATED",
            working_type="MARK_PRICE",
            initial_stop_price=0.6893,
            take_profit_price=0.6586,
            breakeven_trigger_price=0.6771,
            breakeven_stop_price=0.6824,
            trailing_callback_rate=0.5,
            allocation_mode="FIXED",
            allocation_cap_usdt=10.0,
            risk_budget_usdt=0.8,
            rules=SymbolRules(
                symbol="SIRENUSDT",
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
                trigger_protect=0.15,
            ),
        )

    def build_job(self) -> dict[str, object]:
        return {
            "job_id": "SIRENUSDT-1",
            "created_at": "2026-04-27T19:56:35+00:00",
            "position_closed_at": "2026-04-27T19:58:01.758773+00:00",
            "position_mode": "ONE_WAY",
            "entry_market": {
                "orderId": 94641489,
                "updateTime": 1777319795494,
                "avgPrice": "0.6825300",
            },
            "phase": "CLOSED_PRE_BREAKEVEN",
            "dispatch_reason": "bridge_signal",
            "candidate": {"playbook": "oi_build_breakout"},
        }

    async def test_fetch_actual_settlement_accepts_one_way_both_position_side(self) -> None:
        client = FakeSettlementClient(
            trades=[
                {
                    "symbol": "SIRENUSDT",
                    "id": 38443777,
                    "orderId": 94641489,
                    "side": "SELL",
                    "price": "0.6825300",
                    "qty": "146",
                    "realizedPnl": "0",
                    "quoteQty": "99.6493800",
                    "commission": "0.04982469",
                    "time": 1777319795494,
                    "positionSide": "BOTH",
                },
                {
                    "symbol": "SIRENUSDT",
                    "id": 38444997,
                    "orderId": 94643809,
                    "side": "BUY",
                    "price": "0.6902200",
                    "qty": "146",
                    "realizedPnl": "-1.12274000",
                    "quoteQty": "100.7721200",
                    "commission": "0.05038606",
                    "time": 1777319880045,
                    "positionSide": "BOTH",
                },
            ],
            incomes=[
                {
                    "symbol": "SIRENUSDT",
                    "incomeType": "REALIZED_PNL",
                    "income": "-1.12274000",
                    "asset": "USDT",
                    "time": 1777319880000,
                    "info": "38444997",
                    "tradeId": "38444997",
                },
                {
                    "symbol": "SIRENUSDT",
                    "incomeType": "COMMISSION",
                    "income": "-0.04982469",
                    "asset": "USDT",
                    "time": 1777319795000,
                    "info": "38443777",
                    "tradeId": "38443777",
                },
                {
                    "symbol": "SIRENUSDT",
                    "incomeType": "COMMISSION",
                    "income": "-0.05038606",
                    "asset": "USDT",
                    "time": 1777319880000,
                    "info": "38444997",
                    "tradeId": "38444997",
                },
            ],
        )

        settlement = await fetch_actual_settlement(client, intent=self.build_intent(), job=self.build_job())

        assert settlement is not None
        self.assertAlmostEqual(float(settlement["entry_avg_price"]), 0.68253, places=6)
        self.assertAlmostEqual(float(settlement["exit_avg_price"]), 0.69022, places=6)
        self.assertAlmostEqual(float(settlement["realized_pnl_usdt"]), -1.12274, places=6)
        self.assertAlmostEqual(float(settlement["commission_usdt"]), -0.10021075, places=8)
        self.assertAlmostEqual(float(settlement["net_pnl_usdt"]), -1.22295075, places=8)
        self.assertEqual(settlement["position_mode"], "ONE_WAY")

    async def test_fetch_actual_settlement_can_complete_without_income_history(self) -> None:
        client = FakeSettlementClient(
            trades=[
                {
                    "symbol": "SIRENUSDT",
                    "id": 38443777,
                    "orderId": 94641489,
                    "side": "SELL",
                    "price": "0.6825300",
                    "qty": "146",
                    "realizedPnl": "0",
                    "quoteQty": "99.6493800",
                    "commission": "0.04982469",
                    "time": 1777319795494,
                    "positionSide": "BOTH",
                },
                {
                    "symbol": "SIRENUSDT",
                    "id": 38444997,
                    "orderId": 94643809,
                    "side": "BUY",
                    "price": "0.6902200",
                    "qty": "146",
                    "realizedPnl": "-1.12274000",
                    "quoteQty": "100.7721200",
                    "commission": "0.05038606",
                    "time": 1777319880045,
                    "positionSide": "BOTH",
                },
            ],
            incomes={"code": -1},
        )

        settlement = await fetch_actual_settlement(client, intent=self.build_intent(), job=self.build_job())

        assert settlement is not None
        self.assertAlmostEqual(float(settlement["net_pnl_usdt"]), -1.22295075, places=8)
        self.assertEqual(settlement["income_types"], [])

    def test_build_real_trade_outcome_marks_stop_loss_losses(self) -> None:
        job = self.build_job()
        job["actual_settlement"] = {
            "net_pnl_usdt": -1.22295075,
            "realized_pnl_usdt": -1.12274,
            "commission_usdt": -0.10021075,
            "funding_fee_usdt": 0.0,
            "entry_avg_price": 0.68253,
            "exit_avg_price": 0.69022,
        }
        job["journal_files"] = {"journal_json": "/tmp/one.json"}

        outcome = build_real_trade_outcome_record(job, self.build_intent())

        self.assertEqual(outcome["close_reason"], "stop_loss_hit")
        self.assertTrue(outcome["is_stop_loss_loss"])
        self.assertEqual(outcome["settlement_status"], "complete")
        self.assertEqual(outcome["playbook"], "oi_build_breakout")


if __name__ == "__main__":
    unittest.main()
