from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any
from uuid import uuid4

from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import ExecutionSettings
from phoenix.models import OrderInstruction, SymbolRules, TradeIntent


@dataclass(slots=True)
class ProtectionState:
    phase: str
    extreme_price: float
    current_stop_price: float


class PhoenixExecutor:
    def __init__(self, futures_client: BinanceFuturesClient, settings: ExecutionSettings) -> None:
        self.futures_client = futures_client
        self.settings = settings
        self._exchange_info_cache: dict[str, Any] | None = None

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        if self._exchange_info_cache is None:
            self._exchange_info_cache = await self.futures_client.exchange_info()

        for item in self._exchange_info_cache["symbols"]:
            if item["symbol"] != symbol:
                continue

            price_filter = next(filter for filter in item["filters"] if filter["filterType"] == "PRICE_FILTER")
            lot_filter = next(filter for filter in item["filters"] if filter["filterType"] == "LOT_SIZE")
            min_notional_filter = next(filter for filter in item["filters"] if filter["filterType"] == "MIN_NOTIONAL")
            return SymbolRules(
                symbol=symbol,
                tick_size=float(price_filter["tickSize"]),
                step_size=float(lot_filter["stepSize"]),
                min_qty=float(lot_filter["minQty"]),
                min_notional=float(min_notional_filter["notional"]),
                trigger_protect=float(item.get("triggerProtect") or 0.0),
            )

        raise ValueError(f"Symbol {symbol} not found in futures exchange info.")

    async def build_trade_intent(
        self,
        symbol: str,
        *,
        side: str = "BUY",
        quote_allocation_usdt: float | None = None,
        entry_price: float | None = None,
        leverage: int | None = None,
        available_balance_usdt: float | None = None,
        margin_type_override: str | None = None,
    ) -> TradeIntent:
        rules = await self.get_symbol_rules(symbol)
        leverage = leverage or self.settings.leverage

        if entry_price is None:
            quote = await self.futures_client.mark_price(symbol)
            entry_price = float(quote["markPrice"])

        quote_allocation, resolved_allocation_mode, allocation_cap, risk_budget = self._resolve_quote_allocation(
            leverage=leverage,
            quote_allocation_usdt=quote_allocation_usdt,
            available_balance_usdt=available_balance_usdt,
        )

        notional_usdt = quote_allocation * leverage
        raw_quantity = notional_usdt / entry_price
        quantity = self._round_down(raw_quantity, rules.step_size)
        if quantity < rules.min_qty:
            raise ValueError(
                f"Calculated quantity {quantity} is smaller than exchange minimum {rules.min_qty} for {symbol}. "
                f"allocation_mode={resolved_allocation_mode} quote_allocation={quote_allocation:.4f} "
                f"risk_budget={risk_budget if risk_budget is not None else 'n/a'}"
            )
        if quantity * entry_price < rules.min_notional:
            raise ValueError(
                f"Calculated notional {quantity * entry_price:.4f} is smaller than minimum {rules.min_notional}."
            )

        if side.upper() == "BUY":
            initial_stop_reference = self._apply_pct(entry_price, self.settings.initial_stop_loss_pct, subtract=True)
            take_profit_reference = (
                self._apply_pct(entry_price, self.settings.take_profit_pct, subtract=False)
                if self.settings.take_profit_pct > 0
                else None
            )
            breakeven_trigger_reference = self._apply_pct(entry_price, self.settings.breakeven_trigger_pct, subtract=False)
            breakeven_stop_reference = self._apply_pct(entry_price, self.settings.breakeven_lock_pct, subtract=False)
        else:
            initial_stop_reference = self._apply_pct(entry_price, self.settings.initial_stop_loss_pct, subtract=False)
            take_profit_reference = (
                self._apply_pct(entry_price, self.settings.take_profit_pct, subtract=True)
                if self.settings.take_profit_pct > 0
                else None
            )
            breakeven_trigger_reference = self._apply_pct(entry_price, self.settings.breakeven_trigger_pct, subtract=True)
            breakeven_stop_reference = self._apply_pct(entry_price, self.settings.breakeven_lock_pct, subtract=True)

        initial_stop_price = self._round_price(initial_stop_reference, rules.tick_size)
        take_profit_price = (
            self._round_price(take_profit_reference, rules.tick_size)
            if take_profit_reference is not None
            else None
        )
        breakeven_trigger = self._round_price(breakeven_trigger_reference, rules.tick_size)
        breakeven_stop = self._round_price(breakeven_stop_reference, rules.tick_size)

        return TradeIntent(
            symbol=symbol,
            side=side.upper(),
            entry_price=entry_price,
            quantity=quantity,
            leverage=leverage,
            quote_allocation_usdt=quote_allocation,
            notional_usdt=quantity * entry_price,
            margin_type=(margin_type_override or self.settings.margin_type).upper(),
            working_type=self.settings.working_type,
            initial_stop_price=initial_stop_price,
            take_profit_price=take_profit_price,
            breakeven_trigger_price=breakeven_trigger,
            breakeven_stop_price=breakeven_stop,
            trailing_callback_rate=self.settings.trailing_callback_pct,
            allocation_mode=resolved_allocation_mode,
            allocation_cap_usdt=allocation_cap,
            risk_budget_usdt=risk_budget,
            rules=rules,
        )

    def build_order_plan(
        self,
        intent: TradeIntent,
        *,
        account_api_mode: str | None = None,
        position_mode: str | None = None,
    ) -> list[OrderInstruction]:
        account_api_mode = account_api_mode or self.futures_client.planned_account_api_mode()
        exit_side = "SELL" if intent.side == "BUY" else "BUY"
        if account_api_mode == "portfolio_margin":
            hedge_mode = (position_mode or self.settings.position_mode).upper() == "HEDGE"
            entry_position_side = self._entry_position_side(intent.side, hedge_mode=hedge_mode)
            exit_position_side = self._exit_position_side(intent.side, hedge_mode=hedge_mode)
            instructions = [
                OrderInstruction(
                    name="entry_market",
                    endpoint="/papi/v1/um/order",
                    description="Immediate market entry on the selected unified-account UM perpetual symbol.",
                    payload={
                        "symbol": intent.symbol,
                        "side": intent.side,
                        **({"positionSide": entry_position_side} if entry_position_side else {}),
                        "type": "MARKET",
                        "quantity": intent.quantity,
                        "newOrderRespType": "RESULT",
                    },
                ),
                OrderInstruction(
                    name="initial_protective_stop",
                    endpoint="/papi/v1/um/conditional/order",
                    description="Initial unified-account stop-market strategy to cap downside immediately after fill.",
                    payload={
                        "symbol": intent.symbol,
                        "side": exit_side,
                        **({"positionSide": exit_position_side} if exit_position_side else {}),
                        "strategyType": "STOP_MARKET",
                        "quantity": intent.quantity,
                        **({} if hedge_mode else {"reduceOnly": "true"}),
                        "stopPrice": intent.initial_stop_price,
                        "workingType": intent.working_type,
                        "priceProtect": "TRUE",
                    },
                ),
            ]
            if intent.take_profit_price is not None:
                instructions.append(
                    OrderInstruction(
                        name="initial_take_profit",
                        endpoint="/papi/v1/um/conditional/order",
                        description="Initial unified-account take-profit strategy to capture the first fixed target.",
                        payload={
                            "symbol": intent.symbol,
                            "side": exit_side,
                            **({"positionSide": exit_position_side} if exit_position_side else {}),
                            "strategyType": "TAKE_PROFIT_MARKET",
                            "quantity": intent.quantity,
                            **({} if hedge_mode else {"reduceOnly": "true"}),
                            "stopPrice": intent.take_profit_price,
                            "workingType": intent.working_type,
                            "priceProtect": "TRUE",
                        },
                    )
                )
            instructions.extend(
                [
                    OrderInstruction(
                    name="breakeven_stop_replacement",
                    endpoint="/papi/v1/um/conditional/order",
                    description="Replacement unified-account stop once price reaches the breakeven trigger.",
                    payload={
                        "symbol": intent.symbol,
                        "side": exit_side,
                        **({"positionSide": exit_position_side} if exit_position_side else {}),
                        "strategyType": "STOP_MARKET",
                        "quantity": intent.quantity,
                        **({} if hedge_mode else {"reduceOnly": "true"}),
                        "stopPrice": intent.breakeven_stop_price,
                        "workingType": intent.working_type,
                        "priceProtect": "TRUE",
                    },
                ),
                OrderInstruction(
                    name="post_trigger_trailing_stop",
                    endpoint="/papi/v1/um/conditional/order",
                    description="Unified-account trailing-stop strategy armed after the breakeven trigger.",
                    payload={
                        "symbol": intent.symbol,
                        "side": exit_side,
                        **({"positionSide": exit_position_side} if exit_position_side else {}),
                        "strategyType": "TRAILING_STOP_MARKET",
                        "quantity": intent.quantity,
                        **({} if hedge_mode else {"reduceOnly": "true"}),
                        "activationPrice": self._round_price(
                            intent.breakeven_trigger_price * (1.001 if exit_side == "SELL" else 0.999),
                            intent.rules.tick_size,
                        ),
                        "callbackRate": intent.trailing_callback_rate,
                        "workingType": intent.working_type,
                    },
                ),
                ]
            )
            return instructions

        instructions = [
            OrderInstruction(
                name="entry_market",
                endpoint="/fapi/v1/order",
                description="Immediate market entry on the selected perpetual symbol.",
                payload={
                    "symbol": intent.symbol,
                    "side": intent.side,
                    "type": "MARKET",
                    "quantity": intent.quantity,
                    "newOrderRespType": "RESULT",
                },
            ),
            OrderInstruction(
                name="initial_protective_stop",
                endpoint="/fapi/v1/algoOrder",
                description="Initial mark-price stop submitted through the Binance USD-M algo-order endpoint.",
                payload={
                    "symbol": intent.symbol,
                    "side": exit_side,
                    "algoType": "CONDITIONAL",
                    "type": "STOP_MARKET",
                    "clientAlgoId": self._client_algo_id("stop"),
                    "triggerPrice": intent.initial_stop_price,
                    "stopPrice": intent.initial_stop_price,
                    "closePosition": "true",
                    "workingType": intent.working_type,
                    "priceProtect": "TRUE",
                },
            ),
        ]
        if intent.take_profit_price is not None:
            instructions.append(
                OrderInstruction(
                    name="initial_take_profit",
                    endpoint="/fapi/v1/algoOrder",
                    description="Initial take-profit submitted through the Binance USD-M algo-order endpoint.",
                    payload={
                        "symbol": intent.symbol,
                        "side": exit_side,
                        "algoType": "CONDITIONAL",
                        "type": "TAKE_PROFIT_MARKET",
                        "clientAlgoId": self._client_algo_id("tp"),
                        "triggerPrice": intent.take_profit_price,
                        "stopPrice": intent.take_profit_price,
                        "closePosition": "true",
                        "workingType": intent.working_type,
                        "priceProtect": "TRUE",
                    },
                )
            )
        instructions.extend(
            [
                OrderInstruction(
                name="breakeven_stop_replacement",
                endpoint="/fapi/v1/algoOrder",
                description="Replacement stop submitted through the Binance USD-M algo-order endpoint.",
                payload={
                    "symbol": intent.symbol,
                    "side": exit_side,
                    "algoType": "CONDITIONAL",
                    "type": "STOP_MARKET",
                    "clientAlgoId": self._client_algo_id("be"),
                    "triggerPrice": intent.breakeven_stop_price,
                    "stopPrice": intent.breakeven_stop_price,
                    "closePosition": "true",
                    "workingType": intent.working_type,
                    "priceProtect": "TRUE",
                },
            ),
            OrderInstruction(
                name="post_trigger_trailing_stop",
                endpoint="/fapi/v1/algoOrder",
                description="Trailing stop armed after breakeven trigger through the Binance USD-M algo-order endpoint.",
                payload={
                    "symbol": intent.symbol,
                    "side": exit_side,
                    "algoType": "CONDITIONAL",
                    "type": "TRAILING_STOP_MARKET",
                    "clientAlgoId": self._client_algo_id("trail"),
                    "quantity": intent.quantity,
                    "reduceOnly": "true",
                    "activatePrice": self._round_price(
                        intent.breakeven_trigger_price * (1.001 if exit_side == "SELL" else 0.999),
                        intent.rules.tick_size,
                    ),
                    "activationPrice": self._round_price(
                        intent.breakeven_trigger_price * (1.001 if exit_side == "SELL" else 0.999),
                        intent.rules.tick_size,
                    ),
                    "priceRate": intent.trailing_callback_rate,
                    "callbackRate": intent.trailing_callback_rate,
                    "workingType": intent.working_type,
                },
            ),
            ]
        )
        return instructions

    def build_protection_state(self, intent: TradeIntent) -> ProtectionState:
        return ProtectionState(
            phase="INITIAL_STOP",
            extreme_price=intent.entry_price,
            current_stop_price=intent.initial_stop_price,
        )

    def on_price_tick(self, intent: TradeIntent, state: ProtectionState, mark_price: float) -> dict[str, Any] | None:
        if intent.side == "BUY":
            state.extreme_price = max(state.extreme_price, mark_price)
            trigger_hit = mark_price >= intent.breakeven_trigger_price
            action_name = "move_stop_to_breakeven_long"
            cancel_hint_key = "cancel_stop_below"
        else:
            state.extreme_price = min(state.extreme_price, mark_price)
            trigger_hit = mark_price <= intent.breakeven_trigger_price
            action_name = "move_stop_to_breakeven_short"
            cancel_hint_key = "cancel_stop_above"

        if state.phase == "INITIAL_STOP" and trigger_hit:
            state.phase = "BREAKEVEN_LOCKED"
            state.current_stop_price = intent.breakeven_stop_price
            return {
                "action": action_name,
                cancel_hint_key: intent.initial_stop_price,
                "new_stop_price": intent.breakeven_stop_price,
                "arm_trailing_callback_rate": intent.trailing_callback_rate,
            }
        return None

    @staticmethod
    def _entry_position_side(side: str, *, hedge_mode: bool) -> str | None:
        if not hedge_mode:
            return None
        return "LONG" if side.upper() == "BUY" else "SHORT"

    @staticmethod
    def _exit_position_side(side: str, *, hedge_mode: bool) -> str | None:
        if not hedge_mode:
            return None
        return "LONG" if side.upper() == "BUY" else "SHORT"

    async def validate_test_plan(
        self,
        intent: TradeIntent,
        *,
        include_account_setup: bool = False,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        try:
            account_api_mode = await self.futures_client.get_account_api_mode()
        except (BinanceAPIError, RuntimeError) as exc:
            results.append({"name": "account_api_resolution", "ok": False, "error": str(exc)})
            account_api_mode = self.futures_client.planned_account_api_mode()

        if include_account_setup:
            if account_api_mode == "portfolio_margin":
                results.append(
                    self._skipped_call(
                        "change_margin_type",
                        "Portfolio Margin UM accounts do not expose margin-type switching via PAPI.",
                    )
                )
                results.append(await self._safe_call("um_account_config", self.futures_client.um_account_config()))
                symbol_config_result = await self._safe_call("um_symbol_config", self.futures_client.um_symbol_config(intent.symbol))
                results.append(symbol_config_result)
                results.append(self._evaluate_portfolio_margin_leverage(intent, symbol_config_result))
                results.append(
                    self._skipped_call(
                        "change_initial_leverage",
                        "Portfolio Margin preflight verifies the current symbol leverage via symbolConfig instead of mutating leverage.",
                    )
                )
            else:
                results.append(
                    await self._safe_call(
                        "change_margin_type",
                        self.futures_client.change_margin_type(intent.symbol, intent.margin_type),
                    )
                )
                results.append(
                    await self._safe_call(
                        "change_initial_leverage",
                        self.futures_client.change_initial_leverage(intent.symbol, intent.leverage),
                    )
                )

        if account_api_mode == "portfolio_margin":
            position_mode = self._position_mode_from_validation_results(results)
            results.append(await self._safe_call("account_overview", self.futures_client.account_overview()))
            results.append(await self._safe_call("position_information_v3", self.futures_client.position_information_v3(intent.symbol)))
            for instruction in self.build_order_plan(
                intent,
                account_api_mode=account_api_mode,
                position_mode=position_mode,
            ):
                results.append(
                    self._skipped_call(
                        instruction.name,
                        "Binance Portfolio Margin UM APIs do not provide an order/test endpoint. Payload preview only.",
                    )
                )
            return results

        for instruction in self.build_order_plan(intent, account_api_mode=account_api_mode):
            results.append(
                await self._safe_call(
                    instruction.name,
                    self.futures_client.test_order(
                        instruction.payload,
                        endpoint=instruction.endpoint,
                    ),
                )
            )
        return results

    @staticmethod
    def _client_algo_id(prefix: str) -> str:
        return f"phx_{prefix}_{uuid4().hex[:20]}"

    async def _safe_call(self, name: str, awaitable: Any) -> dict[str, Any]:
        try:
            payload = await awaitable
            return {"name": name, "ok": True, "payload": payload}
        except BinanceAPIError as exc:
            return {"name": name, "ok": False, "error": str(exc)}
        except RuntimeError as exc:
            return {"name": name, "ok": False, "error": str(exc)}

    @staticmethod
    def _skipped_call(name: str, reason: str) -> dict[str, Any]:
        return {"name": name, "ok": None, "skipped": True, "reason": reason}

    @staticmethod
    def _evaluate_portfolio_margin_leverage(intent: TradeIntent, symbol_config_result: dict[str, Any]) -> dict[str, Any]:
        if not symbol_config_result.get("ok"):
            return {
                "name": "portfolio_margin_leverage_check",
                "ok": False,
                "error": "Unable to verify current Portfolio Margin leverage because symbolConfig failed.",
            }

        payload = symbol_config_result.get("payload")
        if not isinstance(payload, list):
            return {
                "name": "portfolio_margin_leverage_check",
                "ok": False,
                "error": f"Unexpected symbolConfig payload: {payload!r}",
            }

        current = next((item for item in payload if item.get("symbol") == intent.symbol), None)
        if current is None:
            return {
                "name": "portfolio_margin_leverage_check",
                "ok": False,
                "error": f"Portfolio Margin symbolConfig did not include {intent.symbol}.",
            }

        try:
            current_leverage = int(current.get("leverage"))
        except (TypeError, ValueError):
            return {
                "name": "portfolio_margin_leverage_check",
                "ok": False,
                "error": f"Portfolio Margin symbolConfig returned an invalid leverage: {current.get('leverage')!r}",
            }

        margin_type = str(current.get("marginType") or "")
        if current_leverage != intent.leverage:
            return {
                "name": "portfolio_margin_leverage_check",
                "ok": False,
                "error": (
                    f"Current Portfolio Margin leverage for {intent.symbol} is {current_leverage}x, "
                    f"but the plan expects {intent.leverage}x."
                ),
                "payload": current,
            }

        return {
            "name": "portfolio_margin_leverage_check",
            "ok": True,
            "payload": {
                "symbol": intent.symbol,
                "current_leverage": current_leverage,
                "expected_leverage": intent.leverage,
                "margin_type": margin_type,
            },
        }

    def _resolve_quote_allocation(
        self,
        *,
        leverage: int,
        quote_allocation_usdt: float | None,
        available_balance_usdt: float | None,
    ) -> tuple[float, str, float | None, float | None]:
        allocation_cap = float(quote_allocation_usdt or self.settings.quote_allocation_usdt)
        if allocation_cap <= 0:
            raise ValueError("Quote allocation cap must be positive.")

        if available_balance_usdt is not None:
            spendable_balance = max(available_balance_usdt - self.settings.reserve_balance_usdt, 0.0)
            if spendable_balance <= 0:
                raise ValueError(
                    f"Available balance {available_balance_usdt:.4f} is not enough after reserve "
                    f"{self.settings.reserve_balance_usdt:.4f}."
                )
            allocation_cap = min(allocation_cap, spendable_balance)

        stop_loss_fraction = self.settings.initial_stop_loss_pct / 100.0
        if stop_loss_fraction <= 0:
            raise ValueError("Initial stop loss percentage must be positive.")

        requested_mode = self.settings.allocation_mode.upper()
        if requested_mode == "RISK_BUDGET":
            if available_balance_usdt is None:
                risk_budget = allocation_cap * leverage * stop_loss_fraction
                return allocation_cap, "FIXED_FALLBACK", allocation_cap, risk_budget
            risk_budget = available_balance_usdt * (self.settings.risk_budget_pct_of_balance / 100.0)
            if risk_budget <= 0:
                raise ValueError("Risk budget must be positive.")
            target_quote_allocation = risk_budget / (leverage * stop_loss_fraction)
            return min(target_quote_allocation, allocation_cap), "RISK_BUDGET", allocation_cap, risk_budget

        risk_budget = allocation_cap * leverage * stop_loss_fraction
        return allocation_cap, "FIXED", allocation_cap, risk_budget

    @staticmethod
    def _position_mode_from_validation_results(results: list[dict[str, Any]]) -> str:
        current = next((item for item in results if item.get("name") == "um_account_config" and item.get("ok")), None)
        if not isinstance(current, dict):
            return "ONE_WAY"
        payload = current.get("payload")
        if not isinstance(payload, dict):
            return "ONE_WAY"
        return "HEDGE" if bool(payload.get("dualSidePosition")) else "ONE_WAY"

    @staticmethod
    def _round_down(value: float, step: float) -> float:
        decimal_value = Decimal(str(value))
        decimal_step = Decimal(str(step))
        return float((decimal_value / decimal_step).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_step)

    @staticmethod
    def _round_price(value: float, tick_size: float) -> float:
        decimal_value = Decimal(str(value))
        decimal_tick = Decimal(str(tick_size))
        return float((decimal_value / decimal_tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_tick)

    @staticmethod
    def _apply_pct(price: float, pct: float, *, subtract: bool) -> float:
        decimal_price = Decimal(str(price))
        decimal_pct = Decimal(str(pct)) / Decimal("100")
        multiplier = Decimal("1") - decimal_pct if subtract else Decimal("1") + decimal_pct
        return float(decimal_price * multiplier)
