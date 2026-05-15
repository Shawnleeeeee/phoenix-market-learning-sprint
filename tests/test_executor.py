import unittest
from dataclasses import replace

from phoenix.config import ExecutionSettings
from phoenix.executor import PhoenixExecutor


class FakeFuturesClient:
    def __init__(self, *, account_api_mode: str = "classic") -> None:
        self._account_api_mode = account_api_mode

    async def exchange_info(self) -> dict[str, object]:
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

    async def mark_price(self, symbol: str) -> dict[str, str]:
        return {"symbol": symbol, "markPrice": "100"}

    def planned_account_api_mode(self) -> str:
        return self._account_api_mode


class PhoenixExecutorTakeProfitTests(unittest.IsolatedAsyncioTestCase):
    async def test_build_trade_intent_sets_take_profit_for_long(self) -> None:
        settings = replace(
            ExecutionSettings(),
            quote_allocation_usdt=500.0,
            leverage=10,
            initial_stop_loss_pct=1.0,
            take_profit_pct=2.5,
        )
        executor = PhoenixExecutor(FakeFuturesClient(), settings)

        intent = await executor.build_trade_intent(
            "BTCUSDT",
            side="BUY",
            quote_allocation_usdt=500.0,
            leverage=10,
            entry_price=100.0,
        )

        self.assertEqual(intent.initial_stop_price, 99.0)
        self.assertEqual(intent.take_profit_price, 102.5)

    async def test_build_order_plan_includes_initial_take_profit_for_classic(self) -> None:
        settings = replace(ExecutionSettings(), take_profit_pct=2.5)
        executor = PhoenixExecutor(FakeFuturesClient(account_api_mode="classic"), settings)
        intent = await executor.build_trade_intent(
            "BTCUSDT",
            side="BUY",
            quote_allocation_usdt=500.0,
            leverage=10,
            entry_price=100.0,
        )

        plan = executor.build_order_plan(intent, account_api_mode="classic", position_mode="ONE_WAY")
        names = [item.name for item in plan]

        self.assertEqual(
            names,
            [
                "entry_market",
                "initial_protective_stop",
                "initial_take_profit",
                "breakeven_stop_replacement",
                "post_trigger_trailing_stop",
            ],
        )
        take_profit = next(item for item in plan if item.name == "initial_take_profit")
        self.assertEqual(take_profit.payload["type"], "TAKE_PROFIT_MARKET")
        self.assertEqual(take_profit.payload["triggerPrice"], 102.5)

    async def test_build_order_plan_includes_initial_take_profit_for_portfolio_margin(self) -> None:
        settings = replace(ExecutionSettings(), take_profit_pct=2.5)
        executor = PhoenixExecutor(FakeFuturesClient(account_api_mode="portfolio_margin"), settings)
        intent = await executor.build_trade_intent(
            "BTCUSDT",
            side="SELL",
            quote_allocation_usdt=500.0,
            leverage=10,
            entry_price=100.0,
        )

        plan = executor.build_order_plan(intent, account_api_mode="portfolio_margin", position_mode="ONE_WAY")
        take_profit = next(item for item in plan if item.name == "initial_take_profit")

        self.assertEqual(take_profit.payload["strategyType"], "TAKE_PROFIT_MARKET")
        self.assertEqual(take_profit.payload["stopPrice"], 97.5)
        self.assertEqual(take_profit.payload["reduceOnly"], "true")


if __name__ == "__main__":
    unittest.main()
