"""Order manager for Leiting BTC testnet."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
import time
from uuid import uuid4
from typing import Any

from btc_engine.config import get_symbol, load_risk_config
from btc_engine.execution.binance_signed import BinanceSignedFuturesClient
from btc_engine.execution.safe_gateway import submit_btc_client_order
from btc_engine.types import ExecutionPlan, Side, SymbolRules


def _client_id(prefix: str) -> str:
    return f"lt_{prefix}_{uuid4().hex[:20]}"


def _round_down(value: float, step: float) -> float:
    decimal_value = Decimal(str(value))
    decimal_step = Decimal(str(step))
    return float((decimal_value / decimal_step).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_step)


def _round_price(value: float, tick_size: float) -> float:
    decimal_value = Decimal(str(value))
    decimal_tick = Decimal(str(tick_size))
    return float((decimal_value / decimal_tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_tick)


def _best_passive_entry_price(
    client: BinanceSignedFuturesClient,
    *,
    symbol: str,
    side: Side,
    rules: SymbolRules,
    tick_offset: int = 0,
) -> float:
    payload = client.depth(symbol, limit=5)
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    if not bids or not asks:
        raise RuntimeError(f"Cannot derive maker-first entry price for {symbol}: empty depth snapshot")
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    tick_size = float(rules.tick_size)
    offset = max(0, int(tick_offset)) * tick_size
    if side == "BUY":
        return _round_price(best_bid - offset, tick_size)
    return _round_price(best_ask + offset, tick_size)


def _apply_filled_order_to_plan(plan: ExecutionPlan, order: dict[str, Any]) -> None:
    executed_qty = float(order.get("executedQty") or plan.quantity)
    avg_price = float(order.get("avgPrice") or order.get("price") or plan.reference_price)
    if executed_qty > 0:
        plan.quantity = _round_down(executed_qty, plan.rules.step_size)
        plan.notional_usdt = round(plan.quantity * avg_price, 8)
    if avg_price > 0:
        plan.reference_price = avg_price


def get_symbol_rules(client: BinanceSignedFuturesClient, symbol: str) -> SymbolRules:
    info = client.exchange_info()
    item = next(symbol_info for symbol_info in info["symbols"] if symbol_info["symbol"] == symbol)
    price_filter = next(filter_item for filter_item in item["filters"] if filter_item["filterType"] == "PRICE_FILTER")
    lot_filter = next(filter_item for filter_item in item["filters"] if filter_item["filterType"] == "LOT_SIZE")
    min_notional_filter = next(filter_item for filter_item in item["filters"] if filter_item["filterType"] == "MIN_NOTIONAL")
    return SymbolRules(
        symbol=symbol,
        tick_size=float(price_filter["tickSize"]),
        step_size=float(lot_filter["stepSize"]),
        min_qty=float(lot_filter["minQty"]),
        min_notional=float(min_notional_filter["notional"]),
    )


def build_execution_plan(
    *,
    client: BinanceSignedFuturesClient,
    side: Side,
    quote_allocation_usdt: float,
    leverage: int,
    reason: str,
    symbol: str | None = None,
    stop_loss_pct: float | None = None,
    stop_loss_net_roi_pct: float | None = None,
    breakeven_trigger_pct: float | None = None,
    breakeven_lock_pct: float | None = None,
    trailing_callback_rate: float | None = None,
    take_profit_net_roi_pct: float | None = None,
    estimated_fee_bps_per_side: float = 5.0,
    max_hold_minutes: float | None = None,
    enable_breakeven: bool = True,
    enable_trailing: bool = True,
    maker_first: bool = False,
    maker_entry_timeout_sec: float | None = None,
    maker_price_ticks: int = 0,
) -> ExecutionPlan:
    risk = load_risk_config()
    symbol = symbol or get_symbol()
    mark = client._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol}, signed=False)
    rules = get_symbol_rules(client, symbol)
    reference_price = float(mark["markPrice"])
    entry_order_type = "MARKET"
    entry_limit_price: float | None = None
    entry_time_in_force: str | None = None
    if maker_first:
        entry_order_type = "LIMIT"
        entry_time_in_force = "GTC"
        entry_limit_price = _best_passive_entry_price(
            client,
            symbol=symbol,
            side=side,
            rules=rules,
            tick_offset=maker_price_ticks,
        )
    entry_reference_price = entry_limit_price or reference_price
    quantity = _round_down((quote_allocation_usdt * leverage) / entry_reference_price, rules.step_size)
    if quantity < rules.min_qty:
        raise ValueError(f"Calculated quantity {quantity} is smaller than minimum {rules.min_qty}")
    notional = quantity * entry_reference_price
    if notional < rules.min_notional:
        raise ValueError(f"Calculated notional {notional} is smaller than minimum {rules.min_notional}")

    if stop_loss_pct is None and stop_loss_net_roi_pct is not None:
        # Convert desired net ROI stop into an underlying price move that accounts
        # for estimated round-trip fees on leveraged notional.
        fee_roi_pct = 2.0 * float(estimated_fee_bps_per_side or 0.0) * float(leverage) / 100.0
        gross_stop_roi_pct = max(0.25, float(stop_loss_net_roi_pct) - fee_roi_pct)
        stop_loss_pct = gross_stop_roi_pct / max(float(leverage), 1.0)
    initial_stop_pct = 0.6 if stop_loss_pct is None else stop_loss_pct
    breakeven_trigger_pct = 0.35 if breakeven_trigger_pct is None else breakeven_trigger_pct
    breakeven_lock_pct = 0.05 if breakeven_lock_pct is None else breakeven_lock_pct
    trailing_callback_rate = 0.3 if trailing_callback_rate is None else trailing_callback_rate

    if side == "BUY":
        initial_stop = _round_price(reference_price * (1.0 - initial_stop_pct / 100.0), rules.tick_size)
        breakeven_trigger = _round_price(reference_price * (1.0 + breakeven_trigger_pct / 100.0), rules.tick_size)
        breakeven_stop = _round_price(reference_price * (1.0 + breakeven_lock_pct / 100.0), rules.tick_size)
    else:
        initial_stop = _round_price(reference_price * (1.0 + initial_stop_pct / 100.0), rules.tick_size)
        breakeven_trigger = _round_price(reference_price * (1.0 - breakeven_trigger_pct / 100.0), rules.tick_size)
        breakeven_stop = _round_price(reference_price * (1.0 - breakeven_lock_pct / 100.0), rules.tick_size)

    return ExecutionPlan(
        symbol=symbol,
        side=side,
        leverage=leverage,
        quote_allocation_usdt=quote_allocation_usdt,
        reference_price=entry_reference_price,
        quantity=quantity,
        notional_usdt=notional,
        initial_stop_price=initial_stop,
        breakeven_trigger_price=breakeven_trigger,
        breakeven_stop_price=breakeven_stop,
        trailing_callback_rate=trailing_callback_rate,
        rules=rules,
        reason=reason,
        entry_order_type=entry_order_type,
        entry_limit_price=entry_limit_price,
        entry_time_in_force=entry_time_in_force,
        entry_timeout_sec=maker_entry_timeout_sec or 6.0 if maker_first else None,
        maker_first=maker_first,
        take_profit_net_roi_pct=take_profit_net_roi_pct,
        stop_loss_net_roi_pct=stop_loss_net_roi_pct,
        estimated_fee_bps_per_side=estimated_fee_bps_per_side,
        max_hold_minutes=max_hold_minutes,
        enable_breakeven=enable_breakeven,
        enable_trailing=enable_trailing,
    )


def preflight_test_order(client: BinanceSignedFuturesClient, plan: ExecutionPlan) -> dict[str, Any]:
    client.assert_one_way_mode()
    client.change_leverage(plan.symbol, plan.leverage)
    payload = {
        "symbol": plan.symbol,
        "side": plan.side,
        "type": plan.entry_order_type,
        "quantity": plan.quantity,
        "newClientOrderId": _client_id("test"),
    }
    if plan.entry_order_type == "LIMIT":
        payload["price"] = plan.entry_limit_price
        payload["timeInForce"] = plan.entry_time_in_force or "GTC"
    client.test_order(payload)
    return {"ok": True, "plan": asdict(plan)}


def place_entry_order(client: BinanceSignedFuturesClient, plan: ExecutionPlan) -> dict[str, Any]:
    """Place entry orders and record fills."""
    client.assert_one_way_mode()
    client.change_leverage(plan.symbol, plan.leverage)
    payload = {
        "symbol": plan.symbol,
        "side": plan.side,
        "type": plan.entry_order_type,
        "quantity": plan.quantity,
        "newClientOrderId": _client_id("entry"),
        "newOrderRespType": "RESULT",
    }
    if plan.entry_order_type == "LIMIT":
        payload["price"] = plan.entry_limit_price
        payload["timeInForce"] = plan.entry_time_in_force or "GTC"
    result = submit_btc_client_order(
        client,
        payload,
        source="btc_engine.execution.order_manager:place_entry_order",
        purpose="entry",
        plan=plan,
    )
    if plan.entry_order_type == "LIMIT":
        client_order_id = str(result.get("clientOrderId") or payload["newClientOrderId"])
        deadline = time.time() + float(plan.entry_timeout_sec or 6.0)
        final_order = result
        while time.time() < deadline:
            status = str(final_order.get("status") or "").upper()
            if status == "FILLED":
                break
            if status in {"CANCELED", "EXPIRED", "REJECTED"} and float(final_order.get("executedQty") or 0.0) <= 0:
                raise RuntimeError(f"maker_first_entry_unfilled:{plan.symbol}:{plan.side}:{status}")
            time.sleep(0.35)
            final_order = client.get_order(plan.symbol, client_order_id=client_order_id)
        status = str(final_order.get("status") or "").upper()
        if status != "FILLED":
            try:
                client.cancel_order(plan.symbol, client_order_id=client_order_id)
            except Exception:  # noqa: BLE001
                pass
            final_order = client.get_order(plan.symbol, client_order_id=client_order_id)
            if float(final_order.get("executedQty") or 0.0) <= 0:
                raise RuntimeError(f"maker_first_entry_timeout:{plan.symbol}:{plan.side}")
            final_order["canceled_remainder"] = True
        _apply_filled_order_to_plan(plan, final_order)
        result = final_order
    else:
        _apply_filled_order_to_plan(plan, result)
    result["placed_at"] = datetime.now(timezone.utc).isoformat()
    result["plan"] = asdict(plan)
    return result
