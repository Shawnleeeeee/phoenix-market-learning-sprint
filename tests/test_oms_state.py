import unittest

from phoenix.models import OrderInstruction, SymbolRules, TradeIntent
from phoenix.oms_state import (
    UserDataOMSState,
    build_reconciled_intent,
    choose_reconciled_entry_state,
    classify_entry_reconciliation,
    extract_order_response_state,
    extract_order_update_state,
    sync_plan_quantities_to_fill,
)


def build_intent() -> TradeIntent:
    return TradeIntent(
        symbol="DOGEUSDT",
        side="BUY",
        entry_price=0.1,
        quantity=1000.0,
        leverage=10,
        quote_allocation_usdt=10.0,
        notional_usdt=100.0,
        margin_type="ISOLATED",
        working_type="MARK_PRICE",
        initial_stop_price=0.099,
        take_profit_price=0.103,
        breakeven_trigger_price=0.101,
        breakeven_stop_price=0.1002,
        trailing_callback_rate=0.5,
        allocation_mode="FIXED",
        allocation_cap_usdt=10.0,
        risk_budget_usdt=0.8,
        rules=SymbolRules(
            symbol="DOGEUSDT",
            tick_size=0.00001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            trigger_protect=0.05,
        ),
    )


class OMSStateTests(unittest.IsolatedAsyncioTestCase):
    def test_extract_order_update_state_from_user_stream_partial_fill(self) -> None:
        state = extract_order_update_state(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1777369000000,
                "o": {
                    "s": "DOGEUSDT",
                    "c": "entry-1",
                    "S": "BUY",
                    "o": "MARKET",
                    "q": "1000",
                    "z": "250",
                    "l": "250",
                    "L": "0.1002",
                    "ap": "0.1002",
                    "X": "PARTIALLY_FILLED",
                    "x": "TRADE",
                    "i": 12345,
                    "R": False,
                    "cp": False,
                    "ps": "BOTH",
                },
            }
        )

        assert state is not None
        self.assertEqual(state.symbol, "DOGEUSDT")
        self.assertTrue(state.is_partially_filled)
        self.assertEqual(state.executed_qty_text, "250")
        self.assertAlmostEqual(state.avg_price or 0.0, 0.1002, places=6)

    def test_choose_reconciled_entry_state_prefers_stream_with_more_fill(self) -> None:
        response_state = extract_order_response_state(
            {
                "symbol": "DOGEUSDT",
                "side": "BUY",
                "orderId": 12345,
                "clientOrderId": "entry-1",
                "status": "PARTIALLY_FILLED",
                "origQty": "1000",
                "executedQty": "250",
                "avgPrice": "0.1002",
            }
        )
        stream_state = extract_order_update_state(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1777369000001,
                "o": {
                    "s": "DOGEUSDT",
                    "c": "entry-1",
                    "S": "BUY",
                    "o": "MARKET",
                    "q": "1000",
                    "z": "600",
                    "l": "350",
                    "L": "0.1003",
                    "ap": "0.10025",
                    "X": "PARTIALLY_FILLED",
                    "x": "TRADE",
                    "i": 12345,
                },
            }
        )

        selected = choose_reconciled_entry_state(response_state, stream_state)

        assert selected is not None
        self.assertAlmostEqual(selected.executed_qty, 600.0, places=6)
        self.assertEqual(classify_entry_reconciliation(selected), "partial_fill_cancel_residual_sync_protection")

    def test_build_reconciled_intent_uses_actual_executed_quantity(self) -> None:
        state = extract_order_response_state(
            {
                "symbol": "DOGEUSDT",
                "side": "BUY",
                "orderId": 12345,
                "clientOrderId": "entry-1",
                "status": "PARTIALLY_FILLED",
                "origQty": "1000",
                "executedQty": "333",
                "avgPrice": "0.1005",
            }
        )

        assert state is not None
        reconciled = build_reconciled_intent(build_intent(), state)

        self.assertAlmostEqual(reconciled.quantity, 333.0, places=6)
        self.assertAlmostEqual(reconciled.entry_price, 0.1005, places=6)
        self.assertAlmostEqual(reconciled.notional_usdt, 33.4665, places=6)

    def test_sync_plan_quantities_to_fill_adjusts_protective_quantity_fields(self) -> None:
        state = extract_order_response_state(
            {
                "symbol": "DOGEUSDT",
                "side": "BUY",
                "orderId": 12345,
                "clientOrderId": "entry-1",
                "status": "PARTIALLY_FILLED",
                "origQty": "1000",
                "executedQty": "333",
                "avgPrice": "0.1005",
            }
        )
        assert state is not None
        plan = [
            OrderInstruction("entry_market", "/fapi/v1/order", {"quantity": 1000.0}, "entry"),
            OrderInstruction(
                "initial_protective_stop",
                "/papi/v1/um/conditional/order",
                {"quantity": 1000.0, "stopPrice": 0.099},
                "stop",
            ),
            OrderInstruction(
                "post_trigger_trailing_stop",
                "/fapi/v1/algoOrder",
                {"quantity": 1000.0, "activationPrice": 0.101},
                "trail",
            ),
        ]

        synced = sync_plan_quantities_to_fill(plan, state)

        self.assertEqual(synced[0].payload["quantity"], 1000.0)
        self.assertEqual(synced[1].payload["quantity"], "333")
        self.assertEqual(synced[2].payload["quantity"], "333")

    async def test_user_data_oms_state_waits_for_matching_update(self) -> None:
        oms_state = UserDataOMSState()
        oms_state.apply_event(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1777369000000,
                "o": {
                    "s": "DOGEUSDT",
                    "c": "entry-1",
                    "S": "BUY",
                    "o": "MARKET",
                    "q": "1000",
                    "z": "1000",
                    "l": "1000",
                    "L": "0.1002",
                    "ap": "0.1002",
                    "X": "FILLED",
                    "x": "TRADE",
                    "i": 12345,
                },
            }
        )

        result = await oms_state.wait_for_update(order_id=12345, timeout_sec=0.01)

        assert result is not None
        self.assertTrue(result.is_filled)
        self.assertEqual(oms_state.summary()["tracked_order_count"], 1)


if __name__ == "__main__":
    unittest.main()
