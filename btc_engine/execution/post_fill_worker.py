"""Post-fill worker for Leiting BTC testnet positions.

This worker manages:
- initial stop
- breakeven stop replacement
- software trailing stop replacement
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from btc_engine.config import STATE_DIR, get_journal_mode_for_execution
from btc_engine.execution.binance_signed import BinanceSignedFuturesClient
from btc_engine.execution.protective_orders import (
    cancel_protection_orders,
    current_stop_trigger_price,
    place_initial_stop,
    place_breakeven_stop,
    place_trailing_stop,
)
from btc_engine.execution.safe_gateway import submit_btc_client_order
from btc_engine.review.demo_v2_monitor import maybe_export_first_fill_review
from btc_engine.review.export_review import maybe_export_formal_review
from btc_engine.review.journal import build_close_journal, write_trade_journal
from btc_engine.runtime.control_events import emit_control_event
from btc_engine.runtime.control_plane import sync_control_plane
from btc_engine.runtime.demo_account import update_demo_account_after_close
from btc_engine.runtime.service_health import heartbeat
from btc_engine.runtime.state_store import delete_runtime_state, read_runtime_state, update_runtime_state
from btc_engine.types import ExecutionPlan, SymbolRules


def _print(data: dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def _plan_from_state(active_trade: dict[str, Any]) -> ExecutionPlan:
    plan_data = dict(active_trade["plan"])
    rules_data = dict(plan_data["rules"])
    plan_data["rules"] = SymbolRules(**rules_data)
    return ExecutionPlan(**plan_data)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


USER_STREAM_EVENTS_PATH = STATE_DIR / "user_stream_events.jsonl"
PENDING_CLOSE_REQUEST_PATH = STATE_DIR / "pending_close_request.json"


def _round_price(value: float, tick_size: float) -> float:
    from decimal import Decimal, ROUND_DOWN

    decimal_value = Decimal(str(value))
    decimal_tick = Decimal(str(tick_size))
    return float((decimal_value / decimal_tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_tick)


def _position_for_symbol(client: BinanceSignedFuturesClient, symbol: str) -> dict[str, Any] | None:
    positions = client.position_risk(symbol)
    return next((item for item in positions if abs(float(item.get("positionAmt", 0.0))) > 0.0), None)


def _desired_trailing_trigger(plan: ExecutionPlan, active_trade: dict[str, Any], mark_price: float) -> float | None:
    callback_pct = plan.trailing_callback_rate / 100.0
    tick = plan.rules.tick_size
    if plan.side == "BUY":
        high_watermark = float(active_trade.get("high_watermark") or mark_price)
        raw = high_watermark * (1.0 - callback_pct)
        desired = max(plan.breakeven_stop_price, raw)
        if desired >= mark_price:
            return None
        return _round_price(desired, tick)
    low_watermark = float(active_trade.get("low_watermark") or mark_price)
    raw = low_watermark * (1.0 + callback_pct)
    desired = min(plan.breakeven_stop_price, raw)
    if desired <= mark_price:
        return None
    return _round_price(desired, tick)


def _rearm_missing_protection(
    client: BinanceSignedFuturesClient,
    *,
    active_trade: dict[str, Any],
    plan: ExecutionPlan,
    phase: str,
    mark_price: float,
) -> tuple[dict[str, Any] | None, float | None]:
    if phase == "BREAKEVEN_LOCKED":
        order = place_breakeven_stop(client, plan)
        trigger = float(order.get("triggerPrice") or plan.breakeven_stop_price)
        return order, trigger
    if phase == "TRAILING_ACTIVE":
        trigger_price = _desired_trailing_trigger(plan, active_trade, mark_price)
        if trigger_price is None:
            trigger_price = float(active_trade.get("current_stop_trigger_price") or plan.breakeven_stop_price)
        order = place_trailing_stop(client, plan=plan, trigger_price=trigger_price)
        trigger = float(order.get("triggerPrice") or trigger_price)
        return order, trigger
    order = place_initial_stop(client, plan)
    trigger = float(order.get("triggerPrice") or plan.initial_stop_price)
    return order, trigger


def _estimated_net_roi_pct(plan: ExecutionPlan, *, entry_price: float, current_price: float, quantity: float) -> float:
    if quantity <= 0 or plan.quote_allocation_usdt <= 0:
        return 0.0
    if plan.side == "BUY":
        gross_pnl = (current_price - entry_price) * quantity
    else:
        gross_pnl = (entry_price - current_price) * quantity
    fee_rate = max(0.0, float(plan.estimated_fee_bps_per_side or 0.0) / 10_000.0)
    estimated_fees = ((entry_price * quantity) + (current_price * quantity)) * fee_rate
    net_pnl = gross_pnl - estimated_fees
    return round((net_pnl / plan.quote_allocation_usdt) * 100.0, 4)


def _to_timestamp_ms(value: str | None) -> int:
    if not value:
        return 0
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def _load_user_stream_events(
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    path: Path = USER_STREAM_EVENTS_PATH
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            if not raw_line.strip():
                continue
            try:
                payload = json.loads(raw_line)
            except Exception:
                continue
            if str(payload.get("symbol") or "") != symbol:
                continue
            event_ms = int(payload.get("event_time_ms") or payload.get("transaction_time_ms") or _to_timestamp_ms(payload.get("received_at")))
            if start_ms <= event_ms <= end_ms:
                events.append(payload)
    except Exception:
        return []
    return events


def _load_pending_close_request() -> dict[str, Any] | None:
    payload = read_runtime_state("pending_close_request")
    return payload if isinstance(payload, dict) else None


def _write_pending_close_request(
    *,
    symbol: str,
    side: str,
    reason: str,
    active_trade: dict[str, Any],
    client_order_id: str | None = None,
) -> None:
    update_runtime_state(
        "pending_close_request",
        {
            "symbol": symbol,
            "side": side,
            "reason": reason,
            "client_order_id": client_order_id,
            "requested_at": _utcnow(),
            "opened_at": active_trade.get("opened_at"),
            "entry_order_id": active_trade.get("entry_order_id"),
            "candidate_name": active_trade.get("candidate_name"),
        },
    )


def _clear_pending_close_request() -> None:
    delete_runtime_state("pending_close_request")


def _close_active_trade(
    client: BinanceSignedFuturesClient,
    active_trade: dict[str, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    plan = _plan_from_state(active_trade)
    position = _position_for_symbol(client, plan.symbol)
    if position is None:
        closed = _mark_closed_without_position(client, active_trade, reason=reason)
        return {"ok": True, "managed": True, "reason": reason, "active_trade": closed}
    amount = abs(float(position["positionAmt"]))
    close_side = "SELL" if float(position["positionAmt"]) > 0 else "BUY"
    close_client_order_id = f"lt_close_{uuid4().hex[:20]}"
    _write_pending_close_request(
        symbol=plan.symbol,
        side=close_side,
        reason=reason,
        active_trade=active_trade,
        client_order_id=close_client_order_id,
    )
    cancel_protection_orders(client, symbol=plan.symbol)
    close_result = submit_btc_client_order(
        client,
        {
            "symbol": plan.symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": amount,
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
            "newClientOrderId": close_client_order_id,
        },
        source="btc_engine.execution.post_fill_worker:close_active_trade",
        purpose="emergency",
        plan=plan,
    )
    now = datetime.now(timezone.utc)
    opened_at_str = active_trade.get("opened_at")
    entry_order_id = active_trade.get("entry_order_id")
    close_order_id = close_result.get("orderId")
    if opened_at_str:
        opened_at = datetime.fromisoformat(str(opened_at_str).replace("Z", "+00:00"))
    else:
        opened_at = now
    start_ms = int((opened_at.timestamp() - 120) * 1000)
    end_ms = int((now.timestamp() + 120) * 1000)
    trades_all = client.user_trades(plan.symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000)
    target_order_ids = {item for item in (entry_order_id, close_order_id) if item is not None}
    trades = [item for item in trades_all if item.get("orderId") in target_order_ids] if target_order_ids else trades_all
    incomes = client.income_history(plan.symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=200)
    journal = build_close_journal(
        symbol=plan.symbol,
        side=plan.side,
        quantity=amount,
        trades=trades,
        incomes=incomes,
        reason=reason,
        opened_at=opened_at,
        entry_order_id=entry_order_id,
        close_order_id=close_order_id,
        mode=get_journal_mode_for_execution(),
    )
    journal_id = f"{plan.symbol}_{int(now.timestamp())}"
    journal.update(
        {
            "candidate_name": active_trade.get("candidate_name"),
            "regime": active_trade.get("regime"),
            "entry_snapshot_id": active_trade.get("entry_snapshot_id"),
            "entry_signal": active_trade.get("entry_signal") or {},
            "entry_market": active_trade.get("entry_market") or {},
            "plan": active_trade.get("plan") or {},
        }
    )
    journal_path = write_trade_journal(journal_id, journal)
    if journal.get("mode") == "demo_auto":
        update_demo_account_after_close(journal_id, journal)
    _clear_pending_close_request()
    closed = {
        **active_trade,
        "status": "closed",
        "closed_at": _utcnow(),
        "close_reason": reason,
        "close_order": close_result,
        "journal_path": str(journal_path),
        "journal": journal,
    }
    update_runtime_state("active_trade", closed)
    update_runtime_state("last_close", journal)
    maybe_export_formal_review()
    first_fill_review = maybe_export_first_fill_review(
        active_trade=active_trade,
        journal=journal,
        journal_id=journal_id,
        journal_path=journal_path,
    )
    closed["first_fill_review"] = first_fill_review
    emit_control_event(
        kind="trade_closed",
        title=f"雷霆已平仓：{plan.symbol} {plan.side}",
        severity="info",
        dedupe_key=f"trade_closed:{plan.symbol}:{close_order_id}",
        details={
            "symbol": plan.symbol,
            "side": plan.side,
            "reason": reason,
            "net_pnl": journal.get("net_pnl"),
            "hold_minutes": journal.get("hold_minutes"),
            "entry_avg_price": journal.get("entry_avg_price"),
            "close_avg_price": journal.get("close_avg_price"),
        },
    )
    return {"ok": True, "managed": True, "reason": reason, "active_trade": closed, "journal": journal}


def _infer_missing_position_reason(
    active_trade: dict[str, Any],
    *,
    fallback_reason: str,
    close_order_id: int | None,
) -> str:
    plan = _plan_from_state(active_trade)
    phase = str(active_trade.get("phase") or "INITIAL_STOP").upper()
    opened_at = _to_timestamp_ms(active_trade.get("opened_at"))
    closed_at = _to_timestamp_ms(active_trade.get("closed_at")) or int(datetime.now(timezone.utc).timestamp() * 1000)
    event_window_start_ms = max(0, opened_at - 120_000)
    event_window_end_ms = closed_at + 120_000
    pending_close = _load_pending_close_request() or {}
    if (
        str(pending_close.get("symbol") or "") == plan.symbol
        and str(pending_close.get("opened_at") or "") == str(active_trade.get("opened_at") or "")
    ):
        pending_reason = str(pending_close.get("reason") or "").strip()
        if pending_reason:
            return pending_reason
    events = _load_user_stream_events(
        symbol=plan.symbol,
        start_ms=event_window_start_ms,
        end_ms=event_window_end_ms,
    )
    stop_order_ids: set[str] = set()
    stop_seen = False
    for event in events:
        raw = event.get("raw") or {}
        event_type = str(event.get("event_type") or "")
        if event_type == "TRADE_LITE":
            client_order_id = str(event.get("client_order_id") or raw.get("c") or "")
            order_id = str(event.get("order_id") or raw.get("i") or "")
            if client_order_id.startswith("lt_stop_"):
                stop_seen = True
                if order_id:
                    stop_order_ids.add(order_id)
        elif event_type == "ALGO_UPDATE":
            order_payload = raw.get("o") or {}
            client_algo_id = str(event.get("client_algo_id") or order_payload.get("caid") or "")
            algo_status = str(event.get("algo_status") or order_payload.get("X") or "").upper()
            triggered_order_id = str(event.get("triggered_order_id") or order_payload.get("ai") or "")
            if client_algo_id.startswith("lt_stop_") and algo_status in {"TRIGGERING", "TRIGGERED", "FINISHED"}:
                stop_seen = True
                if triggered_order_id:
                    stop_order_ids.add(triggered_order_id)

    if stop_seen:
        if close_order_id is None or str(close_order_id) in stop_order_ids or not stop_order_ids:
            if phase == "INITIAL_STOP":
                return "initial_stop_triggered"
            if phase == "BREAKEVEN_LOCKED":
                return "breakeven_stop_triggered"
            if phase == "TRAILING_ACTIVE":
                return "trailing_stop_triggered"
            return "protection_stop_triggered"

    last_trade_lite = read_runtime_state("last_trade_lite") or {}
    last_algo_update = read_runtime_state("last_algo_update") or {}

    if str(last_trade_lite.get("symbol") or "") == plan.symbol:
        last_trade_order_id = last_trade_lite.get("order_id")
        if close_order_id is None or str(last_trade_order_id) == str(close_order_id):
            client_order_id = str(last_trade_lite.get("client_order_id") or "")
            pending_close_client_id = str((pending_close or {}).get("client_order_id") or "")
            if client_order_id.startswith("lt_close_") or (pending_close_client_id and client_order_id == pending_close_client_id):
                pending_reason = str((pending_close or {}).get("reason") or "").strip()
                if pending_reason:
                    return pending_reason
                return "system_close_order"
            if client_order_id.startswith("lt_stop_"):
                if phase == "INITIAL_STOP":
                    return "initial_stop_triggered"
                if phase == "BREAKEVEN_LOCKED":
                    return "breakeven_stop_triggered"
                if phase == "TRAILING_ACTIVE":
                    return "trailing_stop_triggered"
                return "protection_stop_triggered"

    if str(last_algo_update.get("symbol") or "") == plan.symbol:
        triggered_order_id = last_algo_update.get("triggered_order_id")
        algo_status = str(last_algo_update.get("algo_status") or "").upper()
        if algo_status == "TRIGGERED" and (close_order_id is None or str(triggered_order_id) == str(close_order_id)):
            if phase == "INITIAL_STOP":
                return "initial_stop_triggered"
            if phase == "BREAKEVEN_LOCKED":
                return "breakeven_stop_triggered"
            if phase == "TRAILING_ACTIVE":
                return "trailing_stop_triggered"
            return "protection_stop_triggered"

    if close_order_id is not None:
        return "manual_or_untracked_close"

    return fallback_reason


def _mark_closed_without_position(
    client: BinanceSignedFuturesClient,
    active_trade: dict[str, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    plan = _plan_from_state(active_trade)
    now = datetime.now(timezone.utc)
    opened_at_str = active_trade.get("opened_at")
    entry_order_id = active_trade.get("entry_order_id")
    if opened_at_str:
        opened_at = datetime.fromisoformat(str(opened_at_str).replace("Z", "+00:00"))
    else:
        opened_at = now
    start_ms = int((opened_at.timestamp() - 120) * 1000)
    end_ms = int((now.timestamp() + 120) * 1000)
    trades_all = client.user_trades(plan.symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000)
    close_trades = [item for item in trades_all if item.get("orderId") != entry_order_id]
    close_order_id = close_trades[-1]["orderId"] if close_trades else None
    inferred_reason = _infer_missing_position_reason(
        active_trade,
        fallback_reason=reason,
        close_order_id=close_order_id,
    )
    target_order_ids = {item for item in (entry_order_id, close_order_id) if item is not None}
    trades = [item for item in trades_all if item.get("orderId") in target_order_ids] if target_order_ids else trades_all
    incomes = client.income_history(plan.symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=200)
    journal = build_close_journal(
        symbol=plan.symbol,
        side=plan.side,
        quantity=float(active_trade.get("position_amt") or plan.quantity),
        trades=trades,
        incomes=incomes,
        reason=inferred_reason,
        opened_at=opened_at,
        entry_order_id=entry_order_id,
        close_order_id=close_order_id,
        mode=get_journal_mode_for_execution(),
    )
    journal_id = f"{plan.symbol}_{int(now.timestamp())}"
    journal.update(
        {
            "candidate_name": active_trade.get("candidate_name"),
            "regime": active_trade.get("regime"),
            "entry_snapshot_id": active_trade.get("entry_snapshot_id"),
            "entry_signal": active_trade.get("entry_signal") or {},
            "entry_market": active_trade.get("entry_market") or {},
            "plan": active_trade.get("plan") or {},
        }
    )
    journal_path = write_trade_journal(journal_id, journal)
    if journal.get("mode") == "demo_auto":
        update_demo_account_after_close(journal_id, journal)
    _clear_pending_close_request()
    closed = {
        **active_trade,
        "status": "closed_without_position",
        "closed_at": _utcnow(),
        "close_reason": inferred_reason,
        "journal_path": str(journal_path),
        "journal": journal,
    }
    update_runtime_state("active_trade", closed)
    update_runtime_state("last_close", journal)
    maybe_export_formal_review()
    first_fill_review = maybe_export_first_fill_review(
        active_trade=active_trade,
        journal=journal,
        journal_id=journal_id,
        journal_path=journal_path,
    )
    closed["first_fill_review"] = first_fill_review
    event_title = f"雷霆仓位已丢失：{plan.symbol} {plan.side}"
    event_severity = "warning"
    if inferred_reason.endswith("_stop_triggered"):
        event_title = f"雷霆保护止损已触发：{plan.symbol} {plan.side}"
        event_severity = "info"
    emit_control_event(
        kind="trade_closed_without_position",
        title=event_title,
        severity=event_severity,
        dedupe_key=f"trade_closed_without_position:{plan.symbol}:{close_order_id or journal_id}",
        details={
            "symbol": plan.symbol,
            "side": plan.side,
            "reason": inferred_reason,
            "fallback_reason": reason,
            "net_pnl": journal.get("net_pnl"),
            "hold_minutes": journal.get("hold_minutes"),
        },
    )
    return closed


def manage_active_trade_once(client: BinanceSignedFuturesClient) -> dict[str, Any]:
    active_trade = read_runtime_state("active_trade")
    if not active_trade or active_trade.get("status") not in {None, "open"}:
        heartbeat("post_fill", status="idle", details={"reason": "no_open_active_trade"})
        return {"ok": True, "managed": False, "reason": "no_open_active_trade"}

    plan = _plan_from_state(active_trade)
    position = _position_for_symbol(client, plan.symbol)
    if position is None:
        closed = _mark_closed_without_position(client, active_trade, reason="position_missing_on_exchange")
        return {"ok": True, "managed": False, "reason": "position_missing_on_exchange", "active_trade": closed}

    mark_price = float(position["markPrice"])
    amount = abs(float(position["positionAmt"]))
    if abs(amount - float(plan.quantity)) > max(plan.rules.step_size / 2.0, 1e-9):
        plan = replace(plan, quantity=amount, notional_usdt=round(mark_price * amount, 8))
    phase = active_trade.get("phase", "INITIAL_STOP")
    events = list(active_trade.get("events") or [])
    active_trade["status"] = "open"
    active_trade["phase"] = phase
    active_trade["mark_price"] = mark_price
    active_trade["position_amt"] = amount
    active_trade["updated_at"] = _utcnow()

    if plan.side == "BUY":
        active_trade["high_watermark"] = max(float(active_trade.get("high_watermark") or mark_price), mark_price)
        active_trade["low_watermark"] = min(float(active_trade.get("low_watermark") or mark_price), mark_price)
    else:
        active_trade["high_watermark"] = max(float(active_trade.get("high_watermark") or mark_price), mark_price)
        active_trade["low_watermark"] = min(float(active_trade.get("low_watermark") or mark_price), mark_price)

    current_trigger = current_stop_trigger_price(client, symbol=plan.symbol)
    active_trade["current_stop_trigger_price"] = current_trigger
    entry_avg_price = float(active_trade.get("entry_avg_price") or plan.reference_price)
    net_roi_pct = _estimated_net_roi_pct(plan, entry_price=entry_avg_price, current_price=mark_price, quantity=amount)
    active_trade["estimated_net_roi_pct"] = net_roi_pct

    if current_trigger is None:
        try:
            new_stop, new_trigger = _rearm_missing_protection(
                client,
                active_trade=active_trade,
                plan=plan,
                phase=str(phase).upper(),
                mark_price=mark_price,
            )
        except Exception as exc:  # noqa: BLE001
            events.append(
                {
                    "event": "protection_missing_recovery_failed",
                    "at": _utcnow(),
                    "mark_price": mark_price,
                    "phase": phase,
                    "error": str(exc),
                }
            )
            active_trade["events"] = events
            update_runtime_state("active_trade", active_trade)
            emit_control_event(
                kind="protection_missing_recovery_failed",
                title=f"雷霆保护单补挂失败：{plan.symbol}",
                severity="warning",
                dedupe_key=f"protection_missing_recovery_failed:{plan.symbol}:{active_trade.get('opened_at')}",
                details={
                    "symbol": plan.symbol,
                    "side": plan.side,
                    "phase": phase,
                    "mark_price": mark_price,
                    "error": str(exc),
                },
            )
            return _close_active_trade(client, active_trade, reason="missing_protection_recovery_flatten")

        active_trade["current_stop_trigger_price"] = new_trigger
        protection = dict(active_trade.get("protection") or {})
        protection.update(
            {
                "current_phase": phase,
                "current_trigger_price": new_trigger,
                "active_algo_order": new_stop,
            }
        )
        if str(phase).upper() == "INITIAL_STOP":
            protection["initial_stop"] = new_stop
        elif str(phase).upper() == "BREAKEVEN_LOCKED":
            protection["breakeven_stop"] = new_stop
        elif str(phase).upper() == "TRAILING_ACTIVE":
            protection["trailing_stop"] = new_stop
        active_trade["protection"] = protection
        events.append(
            {
                "event": "protection_rearmed",
                "at": _utcnow(),
                "mark_price": mark_price,
                "phase": phase,
                "trigger_price": new_trigger,
                "client_algo_id": (new_stop or {}).get("clientAlgoId"),
            }
        )
        emit_control_event(
            kind="protection_rearmed",
            title=f"雷霆保护单已补挂：{plan.symbol}",
            severity="warning",
            dedupe_key=f"protection_rearmed:{plan.symbol}:{active_trade.get('opened_at')}:{new_trigger}",
            details={
                "symbol": plan.symbol,
                "side": plan.side,
                "phase": phase,
                "mark_price": mark_price,
                "trigger_price": new_trigger,
            },
        )

    take_profit_target = plan.take_profit_net_roi_pct
    if take_profit_target is not None and net_roi_pct >= float(take_profit_target):
        events.append(
            {
                "event": "take_profit_target_hit",
                "at": _utcnow(),
                "mark_price": mark_price,
                "net_roi_pct": net_roi_pct,
                "target_net_roi_pct": take_profit_target,
            }
        )
        active_trade["events"] = events
        update_runtime_state("active_trade", active_trade)
        return _close_active_trade(client, active_trade, reason=f"net_roi_take_profit_{float(take_profit_target):.2f}pct")

    breakeven_reached = (
        mark_price >= plan.breakeven_trigger_price if plan.side == "BUY" else mark_price <= plan.breakeven_trigger_price
    )

    if plan.enable_breakeven and phase == "INITIAL_STOP" and breakeven_reached:
        cancel_protection_orders(client, symbol=plan.symbol)
        new_stop = place_breakeven_stop(client, plan)
        active_trade["phase"] = "BREAKEVEN_LOCKED"
        active_trade["current_stop_trigger_price"] = plan.breakeven_stop_price
        active_trade["protection"] = {
            **dict(active_trade.get("protection") or {}),
            "current_phase": "BREAKEVEN_LOCKED",
            "current_trigger_price": plan.breakeven_stop_price,
            "active_algo_order": new_stop,
            "breakeven_stop": new_stop,
        }
        events.append(
            {
                "event": "breakeven_locked",
                "at": _utcnow(),
                "mark_price": mark_price,
                "trigger_price": plan.breakeven_stop_price,
            }
        )
        emit_control_event(
            kind="breakeven_locked",
            title=f"雷霆保本已锁定：{plan.symbol}",
            severity="info",
            dedupe_key=f"breakeven_locked:{plan.symbol}:{plan.breakeven_stop_price}",
            details={
                "symbol": plan.symbol,
                "side": plan.side,
                "trigger_price": plan.breakeven_stop_price,
                "mark_price": mark_price,
            },
        )

    current_trigger = active_trade.get("current_stop_trigger_price")
    desired_trailing = _desired_trailing_trigger(plan, active_trade, mark_price)
    replaced = False
    if plan.enable_trailing and desired_trailing is not None and current_trigger is not None:
        if plan.side == "BUY" and float(desired_trailing) > float(current_trigger):
            cancel_protection_orders(client, symbol=plan.symbol)
            new_stop = place_trailing_stop(client, plan=plan, trigger_price=float(desired_trailing))
            active_trade["phase"] = "TRAILING_ACTIVE"
            active_trade["current_stop_trigger_price"] = float(desired_trailing)
            active_trade["protection"] = {
                **dict(active_trade.get("protection") or {}),
                "current_phase": "TRAILING_ACTIVE",
                "current_trigger_price": float(desired_trailing),
                "active_algo_order": new_stop,
                "trailing_stop": new_stop,
            }
            events.append(
                {
                    "event": "trailing_stop_replaced",
                    "at": _utcnow(),
                    "mark_price": mark_price,
                    "trigger_price": float(desired_trailing),
                }
            )
            replaced = True
            emit_control_event(
                kind="trailing_stop_replaced",
                title=f"雷霆追踪止损已抬高：{plan.symbol}",
                severity="info",
                dedupe_key=f"trailing_stop_replaced:{plan.symbol}:{float(desired_trailing)}",
                details={
                    "symbol": plan.symbol,
                    "side": plan.side,
                    "trigger_price": float(desired_trailing),
                    "mark_price": mark_price,
                },
            )
        if plan.side == "SELL" and float(desired_trailing) < float(current_trigger):
            cancel_protection_orders(client, symbol=plan.symbol)
            new_stop = place_trailing_stop(client, plan=plan, trigger_price=float(desired_trailing))
            active_trade["phase"] = "TRAILING_ACTIVE"
            active_trade["current_stop_trigger_price"] = float(desired_trailing)
            active_trade["protection"] = {
                **dict(active_trade.get("protection") or {}),
                "current_phase": "TRAILING_ACTIVE",
                "current_trigger_price": float(desired_trailing),
                "active_algo_order": new_stop,
                "trailing_stop": new_stop,
            }
            events.append(
                {
                    "event": "trailing_stop_replaced",
                    "at": _utcnow(),
                    "mark_price": mark_price,
                    "trigger_price": float(desired_trailing),
                }
            )
            replaced = True
            emit_control_event(
                kind="trailing_stop_replaced",
                title=f"雷霆追踪止损已压低：{plan.symbol}",
                severity="info",
                dedupe_key=f"trailing_stop_replaced:{plan.symbol}:{float(desired_trailing)}",
                details={
                    "symbol": plan.symbol,
                    "side": plan.side,
                    "trigger_price": float(desired_trailing),
                    "mark_price": mark_price,
                },
            )

    active_trade["events"] = events
    update_runtime_state("active_trade", active_trade)
    sync_control_plane(reason="post_fill_manage")
    heartbeat(
        "post_fill",
        status="running",
        details={
            "symbol": plan.symbol,
            "phase": active_trade.get("phase"),
            "mark_price": mark_price,
            "current_stop_trigger_price": active_trade.get("current_stop_trigger_price"),
        },
    )
    return {
        "ok": True,
        "managed": True,
        "phase": active_trade.get("phase"),
        "mark_price": mark_price,
        "current_stop_trigger_price": active_trade.get("current_stop_trigger_price"),
        "replaced": replaced,
        "active_trade": active_trade,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Leiting BTC post-fill worker")
    parser.add_argument("--once", action="store_true", help="Run one management pass and exit")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval-sec", type=int, default=5)
    args = parser.parse_args()

    try:
        client = BinanceSignedFuturesClient()
        client.assert_one_way_mode()
    except Exception as exc:  # noqa: BLE001
        details = {"ok": False, "reason": "one_way_mode_required", "error": str(exc)}
        heartbeat("post_fill", status="error", details=details)
        _print(details)
        raise

    if args.loop:
        while True:
            result = manage_active_trade_once(client)
            if result.get("managed") or result.get("reason") not in {"no_open_active_trade"}:
                _print(result)
            time.sleep(args.interval_sec)
        return

    _print(manage_active_trade_once(client))


if __name__ == "__main__":
    main()
