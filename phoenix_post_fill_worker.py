#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import load_credentials, load_proxy_settings
from phoenix.executor import PhoenixExecutor, ProtectionState
from phoenix.hermes_notify import humanize_side, send_telegram_message_async, telegram_settings
from phoenix.models import OrderInstruction, TradeIntent

DEFAULT_SETTLEMENT_RETRY_DELAYS_SEC = (0.0, 1.0, 2.0, 4.0, 8.0)
REAL_TRADE_OUTCOMES_FILENAME = "phoenix_real_trade_outcomes.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detached Guardian worker for Phoenix post-fill protection management.")
    parser.add_argument("--job-file", required=True)
    return parser.parse_args()


def read_job(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected Guardian job payload: {path}")
    return payload


def write_job(path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def append_event(payload: dict[str, Any], message: str, **details: Any) -> None:
    events = payload.setdefault("events", [])
    if isinstance(events, list):
        events.append(
            {
                "at": datetime.now(timezone.utc).isoformat(),
                "message": message,
                "details": details,
            }
        )


def hermes_home() -> Path:
    return Path(str(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes")))


def humanize_bool(value: Any) -> str:
    return "通过" if bool(value) else "未通过"


def humanize_outcome(value: str) -> str:
    outcome = str(value or "")
    mapping = {
        "protected_exit_after_breakeven": "保本后受保护退出",
        "exit_before_breakeven": "保本触发前退出",
    }
    return mapping.get(outcome, outcome or "未知")


def journals_dir() -> Path:
    path = hermes_home() / "memories" / "phoenix_trade_journals"
    path.mkdir(parents=True, exist_ok=True)
    return path


def real_trade_outcomes_file() -> Path:
    path = hermes_home() / "memories" / REAL_TRADE_OUTCOMES_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def extract_symbol_config(payload: object, symbol: str) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("symbol") or "").upper() == symbol.upper():
                return item
    return None


def has_open_position(position_payload: object, symbol: str) -> bool:
    if not isinstance(position_payload, list):
        return False
    for item in position_payload:
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol") or "").upper() != symbol.upper():
            continue
        for key in ("positionAmt", "positionQty", "quantity"):
            value = item.get(key)
            if value in (None, "", "0", "0.0", "0.00000000"):
                continue
            try:
                if abs(float(value)) > 0:
                    return True
            except (TypeError, ValueError):
                continue
    return False


def find_instruction(items: list[OrderInstruction], name: str) -> OrderInstruction:
    for item in items:
        if item.name == name:
            return item
    raise RuntimeError(f"Missing required post-fill instruction: {name}")


async def place_instruction(futures: BinanceFuturesClient, instruction: OrderInstruction) -> dict[str, Any]:
    if "/conditional/" in instruction.endpoint or instruction.endpoint.endswith("/algoOrder"):
        return await futures.new_conditional_order(instruction.payload)
    return await futures.new_order(instruction.payload)


def build_runtime_trailing_instruction(
    instruction: OrderInstruction,
    *,
    intent: TradeIntent,
    mark_price: float,
) -> OrderInstruction:
    payload = dict(instruction.payload)
    if intent.side == "BUY":
        activation_reference = mark_price * 1.001
    else:
        activation_reference = mark_price * 0.999
    activation_price = executor_round_price(activation_reference, intent.rules.tick_size)
    if instruction.endpoint.endswith("/algoOrder"):
        payload["activatePrice"] = activation_price
    payload["activationPrice"] = activation_price
    return OrderInstruction(
        name=instruction.name,
        endpoint=instruction.endpoint,
        payload=payload,
        description=instruction.description,
    )


def executor_round_price(value: float, tick_size: float) -> float:
    from decimal import Decimal, ROUND_DOWN

    decimal_value = Decimal(str(value))
    decimal_tick = Decimal(str(tick_size))
    return float((decimal_value / decimal_tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_tick)


def truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def normalize_order_type(payload: dict[str, Any]) -> str:
    return str(
        payload.get("strategyType")
        or payload.get("origType")
        or payload.get("type")
        or ""
    ).upper()


def normalize_position_side(value: Any) -> str:
    normalized = str(value or "").upper()
    return normalized or "BOTH"


def normalized_price_protect(value: Any) -> str:
    normalized = str(value or "").upper()
    return normalized or "FALSE"


def maybe_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def same_number(left: Any, right: Any, *, tolerance: float = 1e-9) -> bool:
    left_value = maybe_float(left)
    right_value = maybe_float(right)
    if left_value is None or right_value is None:
        return left_value is right_value
    return abs(left_value - right_value) <= tolerance


def conditional_trigger_price(payload: dict[str, Any]) -> float | None:
    return maybe_float(payload.get("triggerPrice") if payload.get("triggerPrice") not in (None, "") else payload.get("stopPrice"))


def conditional_activation_price(payload: dict[str, Any]) -> float | None:
    return maybe_float(payload.get("activationPrice") if payload.get("activationPrice") not in (None, "") else payload.get("activatePrice"))


def conditional_callback_rate(payload: dict[str, Any]) -> float | None:
    return maybe_float(payload.get("callbackRate") if payload.get("callbackRate") not in (None, "") else payload.get("priceRate"))


def protection_order_candidates(
    orders: list[dict[str, Any]],
    instruction: OrderInstruction,
) -> list[dict[str, Any]]:
    payload = instruction.payload
    desired_symbol = str(payload.get("symbol") or "").upper()
    desired_side = str(payload.get("side") or "").upper()
    desired_type = normalize_order_type(payload)
    desired_position_side = normalize_position_side(payload.get("positionSide"))
    matches: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("symbol") or "").upper() != desired_symbol:
            continue
        if str(order.get("side") or "").upper() != desired_side:
            continue
        if normalize_order_type(order) != desired_type:
            continue
        if normalize_position_side(order.get("positionSide")) != desired_position_side:
            continue
        matches.append(order)
    return matches


def protection_order_matches_instruction(order: dict[str, Any], instruction: OrderInstruction) -> bool:
    if order not in protection_order_candidates([order], instruction):
        return False

    payload = instruction.payload
    order_type = normalize_order_type(order)
    if order_type == "TRAILING_STOP_MARKET":
        return (
            same_number(conditional_activation_price(order), conditional_activation_price(payload))
            and same_number(conditional_callback_rate(order), conditional_callback_rate(payload))
            and same_number(order.get("quantity"), payload.get("quantity"))
            and truthy(order.get("reduceOnly")) == truthy(payload.get("reduceOnly"))
            and str(order.get("workingType") or "").upper() == str(payload.get("workingType") or "").upper()
        )

    return (
        same_number(conditional_trigger_price(order), conditional_trigger_price(payload))
        and same_number(order.get("quantity"), payload.get("quantity"))
        and truthy(order.get("closePosition")) == truthy(payload.get("closePosition"))
        and truthy(order.get("reduceOnly")) == truthy(payload.get("reduceOnly"))
        and normalized_price_protect(order.get("priceProtect")) == normalized_price_protect(payload.get("priceProtect"))
        and str(order.get("workingType") or "").upper() == str(payload.get("workingType") or "").upper()
    )


async def cancel_conditional_order_payload(
    futures: BinanceFuturesClient,
    *,
    symbol: str,
    order_payload: dict[str, Any],
) -> dict[str, Any]:
    strategy_id_value = order_payload.get("strategyId")
    if strategy_id_value in (None, ""):
        strategy_id_value = order_payload.get("algoId")
    strategy_id = None
    if strategy_id_value not in (None, ""):
        try:
            strategy_id = int(strategy_id_value)
        except (TypeError, ValueError):
            strategy_id = None
    client_strategy_id = order_payload.get("newClientStrategyId")
    if client_strategy_id in (None, ""):
        client_strategy_id = order_payload.get("clientAlgoId")
    return await futures.cancel_conditional_order(
        symbol,
        strategy_id=strategy_id,
        client_strategy_id=(
            str(client_strategy_id)
            if client_strategy_id not in (None, "")
            else None
        ),
    )


async def ensure_unique_protection_order(
    futures: BinanceFuturesClient,
    instruction: OrderInstruction,
) -> dict[str, Any]:
    symbol = str(instruction.payload.get("symbol") or "").upper()
    existing_orders = await futures.open_conditional_orders(symbol)
    open_orders = existing_orders if isinstance(existing_orders, list) else []
    matching_orders = protection_order_candidates(open_orders, instruction)
    exact_matches = [order for order in matching_orders if protection_order_matches_instruction(order, instruction)]

    kept_order = exact_matches[0] if exact_matches else None
    orders_to_cancel = [order for order in matching_orders if order is not kept_order]
    cancel_results: list[dict[str, Any]] = []
    for order in orders_to_cancel:
        cancel_results.append(
            await cancel_conditional_order_payload(
                futures,
                symbol=symbol,
                order_payload=order,
            )
        )

    if kept_order is not None:
        return {
            "action": "reused",
            "order": kept_order,
            "cancelled": orders_to_cancel,
            "cancel_results": cancel_results,
        }

    placed_order = await place_instruction(futures, instruction)
    return {
        "action": "replaced" if matching_orders else "placed",
        "order": placed_order,
        "cancelled": orders_to_cancel,
        "cancel_results": cancel_results,
    }


async def cleanup_residual_protection(futures: BinanceFuturesClient, symbol: str) -> dict[str, Any] | list[dict[str, Any]] | None:
    try:
        return await futures.cancel_all_open_conditional_orders(symbol)
    except Exception:
        return None


def restore_protection_state(job: dict[str, Any], state: ProtectionState, intent: TradeIntent) -> None:
    stored_phase = str(job.get("phase") or "").upper()
    if stored_phase == "BREAKEVEN_ONLY":
        state.phase = "BREAKEVEN_ONLY"
        state.current_stop_price = intent.breakeven_stop_price
    elif job.get("breakeven_lock"):
        trailing_payload = job.get("breakeven_lock") if isinstance(job.get("breakeven_lock"), dict) else {}
        state.phase = "BREAKEVEN_AND_TRAILING_ARMED" if trailing_payload.get("trailing_stop_response") else "BREAKEVEN_ONLY"
        state.current_stop_price = intent.breakeven_stop_price
    extreme_seen = job.get("extreme_mark_price")
    if extreme_seen in (None, ""):
        extreme_seen = job.get("highest_mark_price") if intent.side == "BUY" else job.get("lowest_mark_price")
    if extreme_seen not in (None, ""):
        try:
            seen = float(extreme_seen)
            if intent.side == "BUY":
                state.extreme_price = max(state.extreme_price, seen)
            else:
                state.extreme_price = min(state.extreme_price, seen)
        except (TypeError, ValueError):
            pass


def estimate_pnl_usdt(intent: TradeIntent, reference_price: float | None) -> float | None:
    if reference_price is None:
        return None
    if intent.side == "BUY":
        return (reference_price - intent.entry_price) * intent.quantity
    return (intent.entry_price - reference_price) * intent.quantity


def close_phase_for_job(job: dict[str, Any]) -> str:
    return "CLOSED_AFTER_BREAKEVEN" if job.get("breakeven_lock") else "CLOSED_PRE_BREAKEVEN"


def iso_to_epoch_ms(value: str | None) -> int | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def safe_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_trade_position_side(
    *,
    trade_position_side: Any,
    job_position_mode: str,
    intent_side: str,
) -> bool:
    normalized_trade_side = str(trade_position_side or "").upper()
    normalized_position_mode = str(job_position_mode or "").upper()
    normalized_intent_side = str(intent_side or "").upper()
    if normalized_position_mode == "ONE_WAY":
        return normalized_trade_side in {"", "BOTH"}
    desired_side = "LONG" if normalized_intent_side == "BUY" else "SHORT"
    return normalized_trade_side in {"", desired_side}


def signed_trade_commission_usdt(trade: dict[str, Any]) -> float:
    commission = safe_float(trade.get("commission")) or 0.0
    return -abs(commission)


def weighted_average_price(trades: list[dict[str, Any]]) -> float | None:
    qty_total = 0.0
    quote_total = 0.0
    for trade in trades:
        qty = safe_float(trade.get("qty"))
        quote_qty = safe_float(trade.get("quoteQty"))
        if qty is None or quote_qty is None or qty == 0:
            continue
        qty_total += qty
        quote_total += quote_qty
    if qty_total <= 0:
        return None
    return quote_total / qty_total


async def fetch_actual_settlement(
    futures: BinanceFuturesClient,
    *,
    intent: TradeIntent,
    job: dict[str, Any],
) -> dict[str, Any] | None:
    entry_payload = job.get("entry_market") if isinstance(job.get("entry_market"), dict) else {}
    entry_order_id = entry_payload.get("orderId")
    entry_time_ms = safe_float(entry_payload.get("updateTime"))
    opened_at_ms = iso_to_epoch_ms(str(job.get("created_at") or "")) or (int(entry_time_ms) if entry_time_ms is not None else None)
    closed_at_ms = iso_to_epoch_ms(str(job.get("position_closed_at") or ""))
    if opened_at_ms is None or closed_at_ms is None:
        return None

    anchor_opened_at_ms = int(entry_time_ms) if entry_time_ms is not None else opened_at_ms
    start_time_ms = max(0, anchor_opened_at_ms - 60000)
    end_time_ms = closed_at_ms + 300000
    trades_payload = await futures.user_trades(
        intent.symbol,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=200,
    )
    income_payload = await futures.income_history(
        intent.symbol,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=200,
    )
    if not isinstance(trades_payload, list):
        return None
    if not isinstance(income_payload, list):
        income_payload = []

    open_side = intent.side.upper()
    close_side = "SELL" if open_side == "BUY" else "BUY"
    entry_order_id_str = str(entry_order_id) if entry_order_id not in (None, "") else None

    filtered_trades: list[dict[str, Any]] = []
    for trade in trades_payload:
        if not isinstance(trade, dict):
            continue
        if str(trade.get("symbol") or "").upper() != intent.symbol.upper():
            continue
        trade_time = safe_float(trade.get("time"))
        if trade_time is None or not (start_time_ms <= trade_time <= end_time_ms):
            continue
        if not normalize_trade_position_side(
            trade_position_side=trade.get("positionSide"),
            job_position_mode=str(job.get("position_mode") or ""),
            intent_side=intent.side,
        ):
            continue
        filtered_trades.append(trade)

    if entry_order_id_str is not None:
        entry_trades = [
            trade
            for trade in filtered_trades
            if str(trade.get("orderId")) == entry_order_id_str
        ]
    else:
        entry_trades = [
            trade
            for trade in filtered_trades
            if str(trade.get("side") or "").upper() == open_side
        ]
    exit_trades = [
        trade
        for trade in filtered_trades
        if (
            str(trade.get("side") or "").upper() == close_side
            and (entry_order_id_str is None or str(trade.get("orderId")) != entry_order_id_str)
            and (safe_float(trade.get("time")) or 0.0) >= max(0, anchor_opened_at_ms - 1000)
        )
    ]
    if not entry_trades or not exit_trades:
        return None

    relevant_trade_ids = {
        str(trade.get("id"))
        for trade in entry_trades + exit_trades
        if trade.get("id") not in (None, "")
    }
    realized_pnl = sum(safe_float(trade.get("realizedPnl")) or 0.0 for trade in exit_trades)
    commission_total = sum(signed_trade_commission_usdt(trade) for trade in entry_trades + exit_trades)
    funding_fee = 0.0
    other_income = 0.0
    income_types: set[str] = set()
    for income in income_payload:
        if not isinstance(income, dict):
            continue
        if str(income.get("symbol") or "").upper() != intent.symbol.upper():
            continue
        income_time = safe_float(income.get("time"))
        if income_time is None or not (start_time_ms <= income_time <= end_time_ms):
            continue
        income_type = str(income.get("incomeType") or "")
        amount = safe_float(income.get("income"))
        if amount is None:
            continue
        trade_id = str(income.get("tradeId") or "")
        info = str(income.get("info") or "")
        if income_type != "FUNDING_FEE" and trade_id and trade_id not in relevant_trade_ids:
            continue
        if income_type == "FUNDING_FEE" and info not in {"", "FUNDING_FEE"} and trade_id and trade_id not in relevant_trade_ids:
            continue
        income_types.add(income_type)
        if income_type == "FUNDING_FEE":
            funding_fee += amount
        elif income_type in {"REALIZED_PNL", "COMMISSION"}:
            continue
        else:
            other_income += amount

    exit_closed_at_ms = max(safe_float(trade.get("time")) or 0.0 for trade in exit_trades)
    net_income = realized_pnl + commission_total + funding_fee + other_income
    return {
        "entry_order_id": entry_order_id,
        "entry_trade_ids": [trade.get("id") for trade in entry_trades if trade.get("id") is not None],
        "exit_trade_ids": [trade.get("id") for trade in exit_trades if trade.get("id") is not None],
        "entry_avg_price": weighted_average_price(entry_trades),
        "exit_avg_price": weighted_average_price(exit_trades),
        "entry_qty": sum(safe_float(trade.get("qty")) or 0.0 for trade in entry_trades),
        "exit_qty": sum(safe_float(trade.get("qty")) or 0.0 for trade in exit_trades),
        "entry_quote_qty": sum(safe_float(trade.get("quoteQty")) or 0.0 for trade in entry_trades),
        "exit_quote_qty": sum(safe_float(trade.get("quoteQty")) or 0.0 for trade in exit_trades),
        "realized_pnl_usdt": realized_pnl,
        "commission_usdt": commission_total,
        "funding_fee_usdt": funding_fee,
        "other_income_usdt": other_income,
        "net_pnl_usdt": net_income,
        "closed_at": datetime.fromtimestamp(exit_closed_at_ms / 1000, tz=timezone.utc).isoformat() if exit_closed_at_ms else None,
        "income_types": sorted(income_types),
        "settlement_source": "user_trades_plus_income",
        "position_mode": str(job.get("position_mode") or ""),
    }


async def resolve_actual_settlement(
    futures: BinanceFuturesClient,
    *,
    intent: TradeIntent,
    job: dict[str, Any],
) -> dict[str, Any] | None:
    retry_delays = list(DEFAULT_SETTLEMENT_RETRY_DELAYS_SEC)
    for attempt_index, delay_sec in enumerate(retry_delays, start=1):
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)
        settlement = await fetch_actual_settlement(
            futures,
            intent=intent,
            job=job,
        )
        if settlement is not None:
            if attempt_index > 1:
                append_event(job, "settlement_fetch_recovered", attempt=attempt_index, delay_sec=delay_sec)
            return settlement
        if attempt_index < len(retry_delays):
            append_event(
                job,
                "settlement_pending_retry",
                attempt=attempt_index,
                next_delay_sec=retry_delays[attempt_index],
            )
    append_event(job, "settlement_unavailable_after_retries", attempts=len(retry_delays))
    return None


def classify_real_trade_close_reason(job: dict[str, Any], intent: TradeIntent) -> str:
    actual_settlement = job.get("actual_settlement") if isinstance(job.get("actual_settlement"), dict) else None
    if not isinstance(actual_settlement, dict):
        return "settlement_missing"
    net_pnl_usdt = safe_float(actual_settlement.get("net_pnl_usdt"))
    if net_pnl_usdt is None:
        return "settlement_incomplete"
    if job.get("breakeven_lock"):
        return "breakeven_or_trailing_exit" if net_pnl_usdt >= 0 else "post_breakeven_loss_exit"
    if net_pnl_usdt < 0:
        return "stop_loss_hit"
    if intent.take_profit_price is not None:
        return "take_profit_hit"
    return "positive_exit_without_take_profit"


def build_real_trade_outcome_record(job: dict[str, Any], intent: TradeIntent) -> dict[str, Any]:
    actual_settlement = job.get("actual_settlement") if isinstance(job.get("actual_settlement"), dict) else None
    candidate = job.get("candidate") if isinstance(job.get("candidate"), dict) else {}
    close_reason = classify_real_trade_close_reason(job, intent)
    return {
        "event": "real_trade_outcome",
        "at": datetime.now(timezone.utc).isoformat(),
        "job_id": job.get("job_id"),
        "symbol": intent.symbol,
        "side": intent.side,
        "playbook": candidate.get("playbook") or candidate.get("setup"),
        "dispatch_reason": job.get("dispatch_reason"),
        "account_api_mode": job.get("account_api_mode"),
        "position_mode": job.get("position_mode"),
        "phase": job.get("phase"),
        "breakeven_lock": bool(job.get("breakeven_lock")),
        "opened_at": job.get("created_at"),
        "closed_at": job.get("position_closed_at"),
        "close_reason": close_reason,
        "is_stop_loss_loss": close_reason == "stop_loss_hit",
        "settlement_status": "complete" if actual_settlement is not None else "missing",
        "net_pnl_usdt": safe_float(actual_settlement.get("net_pnl_usdt")) if actual_settlement is not None else None,
        "realized_pnl_usdt": safe_float(actual_settlement.get("realized_pnl_usdt")) if actual_settlement is not None else None,
        "commission_usdt": safe_float(actual_settlement.get("commission_usdt")) if actual_settlement is not None else None,
        "funding_fee_usdt": safe_float(actual_settlement.get("funding_fee_usdt")) if actual_settlement is not None else None,
        "entry_avg_price": safe_float(actual_settlement.get("entry_avg_price")) if actual_settlement is not None else None,
        "exit_avg_price": safe_float(actual_settlement.get("exit_avg_price")) if actual_settlement is not None else None,
        "journal_files": job.get("journal_files"),
        "actual_settlement": actual_settlement,
    }


def build_close_journal(job: dict[str, Any], intent: TradeIntent) -> tuple[dict[str, Any], str]:
    entry_price = float(job.get("entry_market", {}).get("avgPrice") or intent.entry_price)
    last_mark_price = job.get("last_mark_price")
    extreme_mark_price = job.get("extreme_mark_price")
    highest_mark_price = job.get("highest_mark_price")
    lowest_mark_price = job.get("lowest_mark_price")
    try:
        last_mark_float = float(last_mark_price) if last_mark_price not in (None, "") else None
    except (TypeError, ValueError):
        last_mark_float = None
    try:
        extreme_mark_float = float(extreme_mark_price) if extreme_mark_price not in (None, "") else None
    except (TypeError, ValueError):
        extreme_mark_float = None
    try:
        highest_mark_float = float(highest_mark_price) if highest_mark_price not in (None, "") else None
    except (TypeError, ValueError):
        highest_mark_float = None
    try:
        lowest_mark_float = float(lowest_mark_price) if lowest_mark_price not in (None, "") else None
    except (TypeError, ValueError):
        lowest_mark_float = None

    estimated_pnl = estimate_pnl_usdt(intent, last_mark_float)
    actual_settlement = job.get("actual_settlement") if isinstance(job.get("actual_settlement"), dict) else None
    outcome = "protected_exit_after_breakeven" if job.get("breakeven_lock") else "exit_before_breakeven"
    journal = {
        "job_id": job.get("job_id"),
        "symbol": intent.symbol,
        "side": intent.side,
        "account_api_mode": job.get("account_api_mode"),
        "position_mode": job.get("position_mode"),
        "margin_type": intent.margin_type,
        "candidate": job.get("candidate"),
        "strategy_gate": job.get("strategy_gate"),
        "dispatch_reason": job.get("dispatch_reason"),
        "status": job.get("status"),
        "phase": job.get("phase"),
        "opened_at": job.get("created_at"),
        "closed_at": job.get("position_closed_at"),
        "entry_price": entry_price,
        "quantity": intent.quantity,
        "leverage": intent.leverage,
        "initial_stop_price": intent.initial_stop_price,
        "take_profit_price": intent.take_profit_price,
        "breakeven_trigger_price": intent.breakeven_trigger_price,
        "breakeven_stop_price": intent.breakeven_stop_price,
        "trailing_activation_price": find_instruction(
            [OrderInstruction.from_dict(item) for item in job.get("remaining_plan", [])],
            "post_trigger_trailing_stop",
        ).payload.get("activationPrice"),
        "extreme_mark_price": extreme_mark_float,
        "highest_mark_price": highest_mark_float,
        "lowest_mark_price": lowest_mark_float,
        "last_observed_mark_price": last_mark_float,
        "estimated_pnl_usdt_at_last_mark": estimated_pnl,
        "actual_settlement": actual_settlement,
        "outcome": outcome,
        "breakeven_lock": job.get("breakeven_lock"),
        "final_cleanup": job.get("final_cleanup"),
    }
    account_mode = f"{job.get('account_api_mode')} / {job.get('position_mode')} / {intent.margin_type}"
    lines = [
        "# 凤凰协议交易复盘",
        "",
        f"- 作业编号：`{journal['job_id']}`",
        f"- 标的：`{intent.symbol}`",
        f"- 方向：`{humanize_side(intent.side)}`",
        f"- 账户模式：`{account_mode}`",
        f"- 入场：`{entry_price}` x `{intent.quantity}` @ `{intent.leverage}x`",
        f"- 初始止损：`{intent.initial_stop_price}`",
        f"- 固定止盈：`{intent.take_profit_price}`" if intent.take_profit_price is not None else "- 固定止盈：`disabled`",
        f"- 保本触发：`{intent.breakeven_trigger_price}`",
        f"- 保本止损：`{intent.breakeven_stop_price}`",
        f"- 结果：`{humanize_outcome(outcome)}`",
    ]
    candidate = job.get("candidate")
    if isinstance(candidate, dict):
        lines.append(f"- 候选分数：`{candidate.get('score')}`")
        blocked = candidate.get("blocked_reasons") or []
        if blocked:
            lines.append(f"- 入场时阻断原因：{', '.join(str(item) for item in blocked)}")
        breakdown = candidate.get("score_breakdown")
        if isinstance(breakdown, dict) and breakdown:
            top_components = sorted(
                breakdown.items(),
                key=lambda item: abs(float(item[1])) if isinstance(item[1], (int, float)) else 0.0,
                reverse=True,
            )[:4]
            lines.append(
                "- 候选核心驱动："
                + ", ".join(f"{key}:{value}" for key, value in top_components)
            )
    strategy_gate = job.get("strategy_gate")
    if isinstance(strategy_gate, dict):
        lines.append(
            f"- 入场策略闸门：`{humanize_bool(strategy_gate.get('ok'))}` "
            f"(分数 `{strategy_gate.get('score')}` / 最低要求 `{strategy_gate.get('minimum_score')}`)"
        )
    if job.get("dispatch_reason"):
        lines.append(f"- 触发原因：`{job.get('dispatch_reason')}`")
    if intent.side == "BUY" and highest_mark_float is not None:
        lines.append(f"- 最高观察到的标记价：`{highest_mark_float}`")
    elif intent.side == "SELL" and lowest_mark_float is not None:
        lines.append(f"- 最低观察到的标记价：`{lowest_mark_float}`")
    elif extreme_mark_float is not None:
        lines.append(f"- 极值标记价：`{extreme_mark_float}`")
    if last_mark_float is not None:
        lines.append(f"- 平仓确认前最后观察到的标记价：`{last_mark_float}`")
    if isinstance(actual_settlement, dict):
        if actual_settlement.get("entry_avg_price") is not None:
            lines.append(f"- 实际入场均价：`{actual_settlement.get('entry_avg_price')}`")
        if actual_settlement.get("exit_avg_price") is not None:
            lines.append(f"- 实际平仓均价：`{actual_settlement.get('exit_avg_price')}`")
        if actual_settlement.get("realized_pnl_usdt") is not None:
            lines.append(f"- 实际已实现盈亏：`{actual_settlement.get('realized_pnl_usdt')}` USDT")
        if actual_settlement.get("commission_usdt") is not None:
            lines.append(f"- 实际手续费：`{actual_settlement.get('commission_usdt')}` USDT")
        if actual_settlement.get("funding_fee_usdt") is not None:
            lines.append(f"- 实际资金费：`{actual_settlement.get('funding_fee_usdt')}` USDT")
        if actual_settlement.get("other_income_usdt") not in (None, 0, 0.0):
            lines.append(f"- 其他已实现收益：`{actual_settlement.get('other_income_usdt')}` USDT")
        if actual_settlement.get("net_pnl_usdt") is not None:
            lines.append(f"- 实际净利润：`{actual_settlement.get('net_pnl_usdt')}` USDT")
    if estimated_pnl is not None:
        lines.append(f"- 按最后观察标记价估算利润：`{estimated_pnl:.4f}` USDT")
    if journal["closed_at"]:
        lines.append(f"- 平仓时间：`{journal['closed_at']}`")
    return journal, "\n".join(lines) + "\n"


def write_close_artifacts(job: dict[str, Any], intent: TradeIntent) -> dict[str, str]:
    journal, markdown = build_close_journal(job, intent)
    base_dir = journals_dir()
    job_id = str(job.get("job_id") or intent.symbol)
    journal_json = base_dir / f"{job_id}.json"
    journal_md = base_dir / f"{job_id}.md"
    journal_json.write_text(json.dumps(journal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    journal_md.write_text(markdown, encoding="utf-8")

    memories_dir = hermes_home() / "memories"
    memories_dir.mkdir(parents=True, exist_ok=True)
    (memories_dir / "phoenix_last_exit.json").write_text(
        json.dumps(journal, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (memories_dir / "phoenix_last_exit.md").write_text(markdown, encoding="utf-8")
    return {
        "journal_json": str(journal_json),
        "journal_md": str(journal_md),
        "last_exit_json": str(memories_dir / "phoenix_last_exit.json"),
        "last_exit_md": str(memories_dir / "phoenix_last_exit.md"),
    }


async def async_main() -> int:
    args = parse_args()
    job_path = Path(args.job_file)
    job = read_job(job_path)
    credentials = load_credentials(required=True)
    proxy_settings = load_proxy_settings()
    intent = TradeIntent.from_dict(job["intent"])
    remaining_plan = [OrderInstruction.from_dict(item) for item in job.get("remaining_plan", [])]
    if not isinstance(job.get("initial_protective_stop"), dict):
        raise RuntimeError("Guardian job is missing initial protective stop response payload.")

    poll_interval_sec = float(job.get("poll_interval_sec") or 2.0)
    max_runtime_sec = int(job.get("max_runtime_sec") or 43200)
    started_at = datetime.now(timezone.utc)

    timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        futures = BinanceFuturesClient(
            session=session,
            environment=credentials.environment,
            credentials=credentials,
            proxy_settings=proxy_settings,
        )
        executor = PhoenixExecutor(
            futures_client=futures,
            settings=executor_settings_from_intent(intent, position_mode=str(job.get("position_mode") or "ONE_WAY")),
        )
        state = executor.build_protection_state(intent)
        notification_settings = telegram_settings()
        account_api_mode = str(job.get("account_api_mode") or futures.planned_account_api_mode())

        if account_api_mode == "portfolio_margin":
            try:
                symbol_config_payload = await futures.um_symbol_config(intent.symbol)
                symbol_config = extract_symbol_config(symbol_config_payload, intent.symbol)
                if isinstance(symbol_config, dict):
                    margin_type = str(symbol_config.get("marginType") or "").upper()
                    if margin_type:
                        intent.margin_type = margin_type
                        if isinstance(job.get("intent"), dict):
                            job["intent"]["margin_type"] = margin_type
            except Exception as exc:
                append_event(job, "guardian_margin_type_normalize_failed", error=str(exc))

        restore_protection_state(job, state, intent)

        job["status"] = "running"
        job["phase"] = state.phase
        job.pop("error", None)
        append_event(job, "guardian_worker_started", symbol=intent.symbol)
        write_job(job_path, job)

        try:
            while True:
                elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
                if elapsed >= max_runtime_sec:
                    job["status"] = "timed_out"
                    append_event(job, "guardian_worker_timed_out", elapsed_sec=elapsed)
                    write_job(job_path, job)
                    return 1

                positions = await futures.position_information_v3(intent.symbol)
                if not has_open_position(positions, intent.symbol):
                    cleanup = await cleanup_residual_protection(futures, intent.symbol)
                    job["status"] = "closed"
                    job["phase"] = close_phase_for_job(job)
                    job["position_closed_at"] = datetime.now(timezone.utc).isoformat()
                    if cleanup is not None:
                        job["final_cleanup"] = cleanup
                    try:
                        settlement = await resolve_actual_settlement(
                            futures,
                            intent=intent,
                            job=job,
                        )
                        if settlement is not None:
                            job["actual_settlement"] = settlement
                    except Exception as exc:
                        append_event(job, "settlement_fetch_failed", error=str(exc))
                    artifacts = write_close_artifacts(job, intent)
                    job["journal_files"] = artifacts
                    real_trade_outcome = build_real_trade_outcome_record(job, intent)
                    job["real_trade_outcome"] = real_trade_outcome
                    append_jsonl(real_trade_outcomes_file(), real_trade_outcome)
                    append_event(
                        job,
                        "real_trade_outcome_written",
                        outcome_file=str(real_trade_outcomes_file()),
                        close_reason=real_trade_outcome.get("close_reason"),
                        settlement_status=real_trade_outcome.get("settlement_status"),
                        net_pnl_usdt=real_trade_outcome.get("net_pnl_usdt"),
                    )
                    job.pop("error", None)
                    append_event(job, "position_closed", symbol=intent.symbol)
                    estimated_pnl = estimate_pnl_usdt(intent, float(job["last_mark_price"])) if job.get("last_mark_price") not in (None, "") else None
                    actual_settlement = job.get("actual_settlement") if isinstance(job.get("actual_settlement"), dict) else None
                    close_text = (
                        f"📘 凤凰协议已平仓：{intent.symbol} {humanize_side(intent.side)}\n"
                        f"阶段：{job['phase']}\n"
                        f"入场价：{intent.entry_price}\n"
                        f"平仓前最后标记价：{job.get('last_mark_price')}\n"
                        + (
                            f"实际净利润：{float(actual_settlement['net_pnl_usdt']):.4f} USDT\n"
                            if actual_settlement and actual_settlement.get("net_pnl_usdt") is not None
                            else ""
                        )
                        + (
                            f"估算利润：{estimated_pnl:.4f} USDT\n"
                            if estimated_pnl is not None
                            else ""
                        )
                        + f"复盘文件：{artifacts['journal_md']}"
                    )
                    try:
                        await send_telegram_message_async(
                            session,
                            text=close_text,
                            notification_settings=notification_settings,
                        )
                    except Exception as exc:
                        append_event(job, "guardian_close_notification_failed", error=str(exc))
                    write_job(job_path, job)
                    return 0

                mark = await futures.mark_price(intent.symbol)
                mark_price = float(mark["markPrice"])
                if intent.side == "BUY":
                    state.extreme_price = max(state.extreme_price, mark_price)
                    job["highest_mark_price"] = state.extreme_price
                else:
                    state.extreme_price = min(state.extreme_price, mark_price)
                    job["lowest_mark_price"] = state.extreme_price
                job["last_mark_price"] = mark_price
                job["extreme_mark_price"] = state.extreme_price
                job["phase"] = state.phase
                write_job(job_path, job)

                action = executor.on_price_tick(intent, state, mark_price)
                if action is not None and state.phase == "BREAKEVEN_LOCKED":
                    breakeven_instruction = find_instruction(remaining_plan, "breakeven_stop_replacement")
                    trailing_instruction = find_instruction(remaining_plan, "post_trigger_trailing_stop")
                    breakeven_reconcile = await ensure_unique_protection_order(futures, breakeven_instruction)
                    breakeven_response = breakeven_reconcile["order"]
                    runtime_trailing_instruction = build_runtime_trailing_instruction(
                        trailing_instruction,
                        intent=intent,
                        mark_price=mark_price,
                    )

                    state.phase = "BREAKEVEN_ONLY"
                    state.current_stop_price = intent.breakeven_stop_price
                    job["phase"] = state.phase
                    job["breakeven_lock"] = {
                        "triggered_at": datetime.now(timezone.utc).isoformat(),
                        "mark_price": mark_price,
                        "action": action,
                        "breakeven_stop_response": breakeven_response,
                        "breakeven_stop_reconcile": breakeven_reconcile,
                    }
                    append_event(
                        job,
                        "breakeven_stop_armed",
                        mark_price=mark_price,
                        new_stop_price=action.get("new_stop_price"),
                        reconcile_action=breakeven_reconcile["action"],
                    )
                    job.pop("error", None)
                    write_job(job_path, job)

                    try:
                        trailing_reconcile = await ensure_unique_protection_order(futures, runtime_trailing_instruction)
                    except (BinanceAPIError, RuntimeError, ValueError) as exc:
                        append_event(
                            job,
                            "trailing_arm_deferred",
                            error=str(exc),
                            activation_price=runtime_trailing_instruction.payload.get("activationPrice"),
                        )
                        write_job(job_path, job)
                    else:
                        trailing_response = trailing_reconcile["order"]
                        state.phase = "BREAKEVEN_AND_TRAILING_ARMED"
                        job["phase"] = state.phase
                        if isinstance(job.get("breakeven_lock"), dict):
                            job["breakeven_lock"]["trailing_stop_response"] = trailing_response
                            job["breakeven_lock"]["trailing_stop_reconcile"] = trailing_reconcile
                        append_event(
                            job,
                            "breakeven_and_trailing_armed",
                            mark_price=mark_price,
                            activation_price=runtime_trailing_instruction.payload.get("activationPrice"),
                            new_stop_price=action.get("new_stop_price"),
                            trailing_reconcile_action=trailing_reconcile["action"],
                        )
                        be_text = (
                            f"🛡️ 凤凰协议已切换到保本 + 追踪：{intent.symbol} {humanize_side(intent.side)}\n"
                            f"触发标记价：{mark_price}\n"
                            f"保本止损：{intent.breakeven_stop_price}\n"
                            f"追踪激活价：{runtime_trailing_instruction.payload.get('activationPrice')}\n"
                            f"追踪回调：{runtime_trailing_instruction.payload.get('callbackRate')}%"
                        )
                        try:
                            await send_telegram_message_async(
                                session,
                                text=be_text,
                                notification_settings=notification_settings,
                            )
                        except Exception as exc:
                            append_event(job, "guardian_breakeven_notification_failed", error=str(exc))
                        write_job(job_path, job)

                elif state.phase == "BREAKEVEN_ONLY":
                    trailing_instruction = find_instruction(remaining_plan, "post_trigger_trailing_stop")
                    runtime_trailing_instruction = build_runtime_trailing_instruction(
                        trailing_instruction,
                        intent=intent,
                        mark_price=mark_price,
                    )
                    try:
                        trailing_reconcile = await ensure_unique_protection_order(futures, runtime_trailing_instruction)
                    except (BinanceAPIError, RuntimeError, ValueError) as exc:
                        append_event(
                            job,
                            "trailing_rearm_retry_failed",
                            error=str(exc),
                            activation_price=runtime_trailing_instruction.payload.get("activationPrice"),
                        )
                        write_job(job_path, job)
                    else:
                        trailing_response = trailing_reconcile["order"]
                        state.phase = "BREAKEVEN_AND_TRAILING_ARMED"
                        job["phase"] = state.phase
                        if isinstance(job.get("breakeven_lock"), dict):
                            job["breakeven_lock"]["trailing_stop_response"] = trailing_response
                            job["breakeven_lock"]["trailing_stop_reconcile"] = trailing_reconcile
                        append_event(
                            job,
                            "trailing_rearmed_after_retry",
                            mark_price=mark_price,
                            activation_price=runtime_trailing_instruction.payload.get("activationPrice"),
                            trailing_reconcile_action=trailing_reconcile["action"],
                        )
                        write_job(job_path, job)

                await asyncio.sleep(poll_interval_sec)
        except (BinanceAPIError, RuntimeError, ValueError) as exc:
            job["status"] = "failed"
            job["error"] = str(exc)
            append_event(job, "guardian_worker_failed", error=str(exc))
            try:
                await send_telegram_message_async(
                    session,
                    text=(
                        f"⚠️ 凤凰协议 Guardian 管仓任务失败：{intent.symbol} {humanize_side(intent.side)}\n"
                        f"当前阶段：{job.get('phase')}\n"
                        f"错误信息：{exc}"
                    ),
                    notification_settings=notification_settings,
                )
            except Exception as notify_exc:
                append_event(job, "guardian_failure_notification_failed", error=str(notify_exc))
            write_job(job_path, job)
            return 1


def executor_settings_from_intent(intent: TradeIntent, *, position_mode: str):
    from phoenix.config import ExecutionSettings

    return ExecutionSettings(
        quote_allocation_usdt=intent.quote_allocation_usdt,
        leverage=intent.leverage,
        margin_type=intent.margin_type,
        working_type=intent.working_type,
        initial_stop_loss_pct=1.2,
        breakeven_trigger_pct=0.8,
        breakeven_lock_pct=0.1,
        trailing_callback_pct=intent.trailing_callback_rate,
        position_mode=position_mode.upper(),
    )


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
