"""Protective orders for Leiting BTC testnet."""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Any
from uuid import uuid4

from btc_engine.execution.binance_signed import BinanceSignedFuturesClient
from btc_engine.execution.safe_gateway import submit_btc_client_algo_order
from btc_engine.types import ExecutionPlan


def _exit_side(entry_side: str) -> str:
    return "SELL" if entry_side.upper() == "BUY" else "BUY"


def _round_price(value: float, tick_size: float) -> float:
    decimal_value = Decimal(str(value))
    decimal_tick = Decimal(str(tick_size))
    return float((decimal_value / decimal_tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_tick)


def _client_algo_id(prefix: str) -> str:
    return f"lt_{prefix}_{uuid4().hex[:20]}"


def place_stop_order(
    client: BinanceSignedFuturesClient,
    *,
    plan: ExecutionPlan,
    trigger_price: float,
) -> dict[str, Any]:
    payload = {
        "symbol": plan.symbol,
        "side": _exit_side(plan.side),
        "algoType": "CONDITIONAL",
        "type": "STOP_MARKET",
        "clientAlgoId": _client_algo_id("stop"),
        "quantity": plan.quantity,
        "triggerPrice": _round_price(trigger_price, plan.rules.tick_size),
        "reduceOnly": "true",
        "workingType": "MARK_PRICE",
        "priceProtect": "TRUE",
    }
    return submit_btc_client_algo_order(
        client,
        payload,
        source="btc_engine.execution.protective_orders:place_stop_order",
        purpose="protection",
        plan=plan,
    )


def place_initial_stop(client: BinanceSignedFuturesClient, plan: ExecutionPlan) -> dict[str, Any]:
    return place_stop_order(client, plan=plan, trigger_price=plan.initial_stop_price)


def place_breakeven_stop(client: BinanceSignedFuturesClient, plan: ExecutionPlan) -> dict[str, Any]:
    return place_stop_order(client, plan=plan, trigger_price=plan.breakeven_stop_price)


def place_trailing_stop(client: BinanceSignedFuturesClient, *, plan: ExecutionPlan, trigger_price: float) -> dict[str, Any]:
    return place_stop_order(client, plan=plan, trigger_price=trigger_price)


def list_protection_orders(client: BinanceSignedFuturesClient, *, symbol: str) -> list[dict[str, Any]]:
    return [item for item in client.open_algo_orders(symbol) if item.get("symbol") == symbol]


def cancel_protection_orders(client: BinanceSignedFuturesClient, *, symbol: str) -> list[Any]:
    return client.cancel_all_open_algo_orders(symbol)


def current_stop_trigger_price(client: BinanceSignedFuturesClient, *, symbol: str) -> float | None:
    orders = list_protection_orders(client, symbol=symbol)
    if not orders:
        return None
    latest = max(orders, key=lambda item: int(item.get("updateTime", 0) or 0))
    trigger = latest.get("triggerPrice")
    return float(trigger) if trigger is not None else None


def sync_protection(client: BinanceSignedFuturesClient, plan: ExecutionPlan) -> dict[str, Any]:
    """Create the initial protection chain for a newly opened position."""
    client.assert_one_way_mode()
    initial_stop = place_initial_stop(client, plan)
    return {
        "current_phase": "INITIAL_STOP",
        "current_trigger_price": plan.initial_stop_price,
        "active_algo_order": initial_stop,
        "initial_stop": initial_stop,
        "breakeven_trigger_price": plan.breakeven_trigger_price,
        "breakeven_stop_price": plan.breakeven_stop_price,
        "trailing_callback_rate": plan.trailing_callback_rate,
    }
