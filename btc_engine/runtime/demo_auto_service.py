"""Demo/testnet auto-execution service for 雷霆.

This uses the real demo matching engine on Binance Futures demo trading.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import (
    STATE_DIR,
    ensure_runtime_dirs,
    get_demo_candidate_config_path,
    get_demo_leverage,
    get_execution_mode,
    get_journal_mode_for_execution,
)
from btc_engine.execution.binance_signed import BinanceSignedFuturesClient
from btc_engine.execution.order_manager import build_execution_plan, get_symbol_rules, place_entry_order, preflight_test_order
from btc_engine.execution.protective_orders import list_protection_orders, sync_protection
from btc_engine.review.demo_v2_monitor import record_rejection
from btc_engine.runtime.demo_account import demo_allocatable_equity, load_demo_account_state
from btc_engine.runtime.control_events import emit_control_event
from btc_engine.runtime.control_plane import sync_control_plane
from btc_engine.runtime.service_health import heartbeat
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state
from btc_engine.runtime.strategy_version import refresh_strategy_version
from btc_engine.types import ExecutionPlan


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_candidate_config() -> dict[str, Any]:
    path = get_demo_candidate_config_path()
    return json.loads(path.read_text(encoding="utf-8"))


def _allowed_regime(snapshot: dict[str, Any], candidate: dict[str, Any]) -> bool:
    allowed = {str(item).strip().lower() for item in candidate.get("allowed_regimes", [])}
    regime = str(((snapshot.get("signal") or {}).get("regime") or "")).strip().lower()
    return not allowed or regime in allowed


def _candidate_float(candidate: dict[str, Any], key: str, default: float | None = None) -> float | None:
    value = candidate.get(key, default)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _check_min(
    reasons: list[str],
    *,
    label: str,
    value: Any,
    threshold: float | None,
) -> None:
    if threshold is None:
        return
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        numeric = 0.0
    if numeric < threshold:
        reasons.append(f"{label}低于候选门槛 ({numeric:.4f} < {threshold:.4f})")


def _check_max(
    reasons: list[str],
    *,
    label: str,
    value: Any,
    threshold: float | None,
) -> None:
    if threshold is None:
        return
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        numeric = 0.0
    if numeric > threshold:
        reasons.append(f"{label}高于候选门槛 ({numeric:.4f} > {threshold:.4f})")


def _signal_ok(snapshot: dict[str, Any], candidate: dict[str, Any]) -> tuple[bool, list[str]]:
    signal = snapshot.get("signal") or {}
    market = snapshot.get("market") or {}
    reasons: list[str] = []
    engine_mode = str(candidate.get("engine_mode") or "").strip().lower()
    if engine_mode in {"v3_simple", "v4_microstructure", "v6_router"}:
        bias = str(signal.get("bias") or "").upper()
        threshold = _candidate_float(candidate, "threshold", 0.0) or 0.0
        if bias == "NONE":
            reasons.append("没有明确多空信号")
        if bias == "LONG" and not bool(candidate.get("allow_long", True)):
            reasons.append("当前策略未启用做多")
        if bias == "SHORT" and not bool(candidate.get("allow_short", True)):
            reasons.append("当前策略未启用做空")
        if float(signal.get("directional_confidence") or 0.0) < threshold:
            reasons.append("候选阈值未通过")
        if not signal.get("gates_passed", False):
            reasons.extend(list(signal.get("gate_reasons") or ["雷霆风控闸门未通过"]))
        _check_min(
            reasons,
            label="microprice偏移(bps)",
            value=market.get("microprice_bias_bps"),
            threshold=_candidate_float(candidate, "min_microprice_bias_bps_long") if bias == "LONG" else None,
        )
        _check_max(
            reasons,
            label="microprice偏移(bps)",
            value=market.get("microprice_bias_bps"),
            threshold=_candidate_float(candidate, "max_microprice_bias_bps_short") if bias == "SHORT" else None,
        )
        _check_min(
            reasons,
            label="OFI窗口和",
            value=market.get("ofi_window_sum"),
            threshold=_candidate_float(candidate, "min_ofi_window_sum_long") if bias == "LONG" else None,
        )
        _check_max(
            reasons,
            label="OFI窗口和",
            value=market.get("ofi_window_sum"),
            threshold=_candidate_float(candidate, "max_ofi_window_sum_short") if bias == "SHORT" else None,
        )
        deduped: list[str] = []
        seen: set[str] = set()
        for reason in reasons:
            text = str(reason).strip()
            if text and text not in seen:
                deduped.append(text)
                seen.add(text)
        return (not deduped), deduped
    if (signal.get("bias") or "") != "LONG":
        reasons.append("不是做多信号")
    if not signal.get("gates_passed", False):
        reasons.append("雷霆风控闸门未通过")
    if not _allowed_regime(snapshot, candidate):
        reasons.append("当前 regime 不在候选范围内")
    threshold = _candidate_float(candidate, "threshold", 0.0) or 0.0
    if float(signal.get("directional_confidence") or 0.0) < threshold:
        reasons.append("候选阈值未通过")
    _check_min(
        reasons,
        label="动量分",
        value=signal.get("momentum_score"),
        threshold=_candidate_float(candidate, "min_momentum_score"),
    )
    _check_min(
        reasons,
        label="微观结构分",
        value=signal.get("microstructure_score"),
        threshold=_candidate_float(candidate, "min_microstructure_score"),
    )
    _check_min(
        reasons,
        label="执行质量分",
        value=signal.get("execution_quality_score"),
        threshold=_candidate_float(candidate, "min_execution_quality_score"),
    )
    _check_max(
        reasons,
        label="事件风险分",
        value=signal.get("event_risk_score"),
        threshold=_candidate_float(candidate, "max_event_risk_score"),
    )
    _check_max(
        reasons,
        label="点差(bps)",
        value=market.get("spread_bps"),
        threshold=_candidate_float(candidate, "max_spread_bps"),
    )
    _check_max(
        reasons,
        label="预计滑点(bps)",
        value=min(
            float(market.get("estimated_slippage_bps_buy") or 0.0),
            float(market.get("estimated_slippage_bps_sell") or 0.0),
        ),
        threshold=_candidate_float(candidate, "max_estimated_slippage_bps"),
    )
    _check_min(
        reasons,
        label="盘口失衡",
        value=market.get("depth_imbalance"),
        threshold=_candidate_float(candidate, "min_depth_imbalance"),
    )
    _check_min(
        reasons,
        label="5m主动买入占比",
        value=market.get("taker_buy_ratio_5m"),
        threshold=_candidate_float(candidate, "min_taker_buy_ratio_5m"),
    )
    _check_min(
        reasons,
        label="5m价格变化",
        value=market.get("price_change_5m_pct"),
        threshold=_candidate_float(candidate, "min_price_change_5m_pct"),
    )
    _check_min(
        reasons,
        label="1h价格变化",
        value=market.get("price_change_1h_pct"),
        threshold=_candidate_float(candidate, "min_price_change_1h_pct"),
    )
    _check_min(
        reasons,
        label="主动流向差",
        value=market.get("aggressive_flow_delta"),
        threshold=_candidate_float(candidate, "min_aggressive_flow_delta"),
    )
    _check_max(
        reasons,
        label="全市场多空账户比(5m)",
        value=market.get("global_long_short_ratio_5m"),
        threshold=_candidate_float(candidate, "max_global_long_short_ratio_5m"),
    )
    return (not reasons), reasons


def _write_status(snapshot: dict[str, Any], payload: dict[str, Any]) -> None:
    account_state = load_demo_account_state()
    active_trade = payload.get("active_trade")
    if not isinstance(active_trade, dict):
        active_trade = read_runtime_state("active_trade") or {}
    has_open_trade = bool(active_trade) and active_trade.get("status") in {None, "open"}
    selected_symbol = snapshot.get("environment", {}).get("symbol")
    signal_payload = dict(snapshot.get("signal") or {})
    market_payload = {
        key: (snapshot.get("market") or {}).get(key)
        for key in (
            "mark_price",
            "funding_rate",
            "spread_bps",
            "depth_imbalance",
            "microprice_bias_bps",
            "ofi_window_sum",
            "taker_buy_ratio_5m",
            "aggressive_flow_delta",
            "price_change_5m_pct",
            "price_change_1h_pct",
        )
    }
    if has_open_trade:
        selected_symbol = active_trade.get("symbol") or selected_symbol
        signal_payload = dict(active_trade.get("entry_signal") or signal_payload)
        market_payload = {
            key: (active_trade.get("entry_market") or {}).get(key)
            for key in (
                "mark_price",
                "funding_rate",
                "spread_bps",
                "depth_imbalance",
                "taker_buy_ratio_5m",
                "aggressive_flow_delta",
                "price_change_5m_pct",
                "price_change_1h_pct",
            )
        }
    status_payload = {
        "generated_at": _utc_now(),
        "ok": payload.get("ok"),
        "reason": payload.get("reason", payload.get("status")),
        "execution_mode": get_execution_mode(),
        "candidate_name": payload.get("candidate_name") or "n/a",
        "virtual_equity_usdt": account_state.get("equity_usdt"),
        "starting_equity_usdt": account_state.get("starting_equity_usdt"),
        "realized_pnl_usdt": account_state.get("realized_pnl_usdt"),
        "closed_trades": account_state.get("closed_trades"),
        "position_fraction": account_state.get("position_fraction"),
        "effective_leverage": account_state.get("leverage"),
        "signal": signal_payload,
        "selected_symbol": selected_symbol,
        "scan_symbol": snapshot.get("environment", {}).get("symbol"),
        "market": market_payload,
        "reasons": list(payload.get("reasons") or []),
    }
    for key in (
        "order_id",
        "quote_allocation_usdt",
        "available_balance_usdt",
        "effective_equity_usdt",
        "preflight",
        "symbol",
        "side",
    ):
        if key in payload:
            status_payload[key] = payload.get(key)
    update_runtime_state("demo_status", status_payload)
    markdown = "\n".join(
        [
            "# 雷霆 Demo 自动交易状态",
            "",
            f"- 生成时间：{_utc_now()}",
            f"- 模式：{get_execution_mode()}",
            f"- 展示标的：{selected_symbol}",
            f"- 扫描标的：{snapshot.get('environment', {}).get('symbol')}",
            f"- 当前 Bias：{signal_payload.get('bias')}",
            f"- 当前 Regime：{signal_payload.get('regime')}",
            f"- 方向置信度：{signal_payload.get('directional_confidence')}",
            f"- 风控放行：{'是' if signal_payload.get('gates_passed') else '否'}",
            f"- 原因：{'; '.join(signal_payload.get('gate_reasons') or [])}",
            f"- 虚拟本金起始：{account_state.get('starting_equity_usdt')}",
            f"- 虚拟本金当前：{account_state.get('equity_usdt')}",
            f"- 已平仓笔数：{account_state.get('closed_trades')}",
            f"- 单笔仓位比例：{account_state.get('position_fraction')}",
            f"- 候选策略：{payload.get('candidate_name', 'n/a')}",
            f"- 结果：{payload.get('reason', payload.get('status', ''))}",
        ]
    )
    (STATE_DIR / "demo_status.md").write_text(markdown, encoding="utf-8")


def _iso_from_ms(timestamp_ms: int | float | str | None) -> str:
    try:
        value = int(float(timestamp_ms or 0))
    except (TypeError, ValueError):
        value = 0
    if value <= 0:
        return _utc_now()
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()


def _rebuild_active_trade_from_exchange(
    client: BinanceSignedFuturesClient,
    snapshot: dict[str, Any],
    candidate: dict[str, Any],
    open_position: dict[str, Any],
) -> dict[str, Any]:
    symbol = str(open_position.get("symbol") or snapshot.get("environment", {}).get("symbol") or "BTCUSDT").upper()
    position_amt = float(open_position.get("positionAmt") or 0.0)
    side = "BUY" if position_amt > 0 else "SELL"
    quantity = abs(position_amt)
    leverage = int(float(open_position.get("leverage") or candidate.get("leverage") or get_demo_leverage()))
    entry_price = float(
        open_position.get("entryPrice")
        or open_position.get("breakEvenPrice")
        or open_position.get("markPrice")
        or 0.0
    )
    mark_price = float(open_position.get("markPrice") or entry_price)
    notional_usdt = abs(float(open_position.get("notional") or (mark_price * quantity)))
    quote_allocation_usdt = abs(float(open_position.get("initialMargin") or 0.0))
    if quote_allocation_usdt <= 0 and leverage > 0:
        quote_allocation_usdt = notional_usdt / leverage

    stop_loss_net_roi_pct = _candidate_float(candidate, "hard_stop_net_roi_pct")
    stop_loss_pct = _candidate_float(candidate, "stop_loss_pct")
    if stop_loss_pct is None and stop_loss_net_roi_pct is not None:
        stop_loss_pct = float(stop_loss_net_roi_pct) / max(float(leverage), 1.0)
    initial_stop_pct = 0.6 if stop_loss_pct is None else stop_loss_pct
    breakeven_trigger_pct = 0.35 if candidate.get("breakeven_trigger_pct") in (None, "") else float(candidate["breakeven_trigger_pct"])
    breakeven_lock_pct = 0.05 if candidate.get("breakeven_lock_pct") in (None, "") else float(candidate["breakeven_lock_pct"])
    trailing_callback_rate = 0.3 if candidate.get("trailing_callback_pct") in (None, "") else float(candidate["trailing_callback_pct"])
    rules = get_symbol_rules(client, symbol)

    if side == "BUY":
        computed_initial_stop = entry_price * (1.0 - initial_stop_pct / 100.0)
        breakeven_trigger = entry_price * (1.0 + breakeven_trigger_pct / 100.0)
        breakeven_stop = entry_price * (1.0 + breakeven_lock_pct / 100.0)
    else:
        computed_initial_stop = entry_price * (1.0 + initial_stop_pct / 100.0)
        breakeven_trigger = entry_price * (1.0 - breakeven_trigger_pct / 100.0)
        breakeven_stop = entry_price * (1.0 - breakeven_lock_pct / 100.0)

    protection_orders = list_protection_orders(client, symbol=symbol)
    active_algo_order = max(protection_orders, key=lambda item: int(item.get("updateTime") or 0)) if protection_orders else None
    current_stop_trigger_price = (
        float(active_algo_order.get("triggerPrice")) if active_algo_order and active_algo_order.get("triggerPrice") is not None else computed_initial_stop
    )

    plan = ExecutionPlan(
        symbol=symbol,
        side=side,
        leverage=leverage,
        quote_allocation_usdt=round(quote_allocation_usdt, 8),
        reference_price=entry_price,
        quantity=quantity,
        notional_usdt=round(notional_usdt, 8),
        initial_stop_price=round(current_stop_trigger_price, 8),
        breakeven_trigger_price=round(breakeven_trigger, 8),
        breakeven_stop_price=round(breakeven_stop, 8),
        trailing_callback_rate=trailing_callback_rate,
        rules=rules,
        reason=str(candidate.get("name") or "recovered_exchange_position"),
        take_profit_net_roi_pct=_candidate_float(candidate, "take_profit_net_roi_pct"),
        stop_loss_net_roi_pct=stop_loss_net_roi_pct,
        estimated_fee_bps_per_side=float(candidate.get("estimated_fee_bps_per_side") or 5.0),
        max_hold_minutes=_candidate_float(candidate, "max_hold_minutes"),
        enable_breakeven=bool(candidate.get("enable_breakeven", True)),
        enable_trailing=bool(candidate.get("enable_trailing", True)),
    )

    recent_trades = client.user_trades(symbol, limit=50)
    entry_trade = next(
        (
            item
            for item in sorted(recent_trades, key=lambda it: int(it.get("time") or 0), reverse=True)
            if str(item.get("side") or "").upper() == side and float(item.get("realizedPnl") or 0.0) == 0.0
        ),
        None,
    )
    opened_at = _iso_from_ms((entry_trade or {}).get("time") or open_position.get("updateTime"))
    current_signal = dict(snapshot.get("signal") or {})
    current_market = dict(snapshot.get("market") or {})
    if str(snapshot.get("environment", {}).get("symbol") or "").upper() != symbol:
        current_signal = {
            "bias": "LONG" if side == "BUY" else "SHORT",
            "entry_side": side,
            "directional_confidence": 0.0,
            "directional_reasons": ["交易所持仓恢复"],
            "regime": "recovered",
            "execution_quality_score": 0.0,
            "event_risk_score": 0.0,
            "gates_passed": True,
            "gate_reasons": [],
        }
        current_market = {
            "mark_price": mark_price,
            "funding_rate": None,
            "mark_index_basis_pct": None,
            "spread_bps": None,
            "depth_imbalance": None,
            "taker_buy_ratio_5m": None,
            "aggressive_flow_delta": None,
            "price_change_5m_pct": None,
            "price_change_1h_pct": None,
        }
    else:
        current_market = {
            key: current_market.get(key)
            for key in (
                "mark_price",
                "funding_rate",
                "mark_index_basis_pct",
                "spread_bps",
                "depth_imbalance",
                "microprice_bias_bps",
                "ofi_window_sum",
                "taker_buy_ratio_5m",
                "aggressive_flow_delta",
                "price_change_5m_pct",
                "price_change_1h_pct",
            )
        }

    recovered = {
        "mode": get_journal_mode_for_execution(),
        "status": "open",
        "phase": "INITIAL_STOP",
        "opened_at": opened_at,
        "symbol": symbol,
        "side": side,
        "regime": current_signal.get("regime"),
        "candidate_name": candidate.get("name"),
        "entry_snapshot_id": snapshot.get("generated_at"),
        "entry_signal": current_signal,
        "entry_market": current_market,
        "plan": asdict(plan),
        "entry_order": entry_trade or {"recovered_from_exchange": True},
        "entry_order_id": (entry_trade or {}).get("orderId"),
        "entry_avg_price": entry_price,
        "entry_update_time_ms": int((entry_trade or {}).get("time") or open_position.get("updateTime") or 0),
        "protection": {
            "current_phase": "INITIAL_STOP",
            "current_trigger_price": round(current_stop_trigger_price, 8),
            "active_algo_order": active_algo_order,
            "initial_stop": active_algo_order,
            "breakeven_trigger_price": round(breakeven_trigger, 8),
            "breakeven_stop_price": round(breakeven_stop, 8),
            "trailing_callback_rate": trailing_callback_rate,
        },
        "current_stop_trigger_price": round(current_stop_trigger_price, 8),
        "high_watermark": mark_price,
        "low_watermark": mark_price,
        "position_amt": quantity,
        "mark_price": mark_price,
        "estimated_net_roi_pct": 0.0,
        "recovered_from_exchange": True,
        "events": [
            {
                "event": "exchange_position_recovered",
                "at": _utc_now(),
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "entry_avg_price": entry_price,
            }
        ],
    }
    update_runtime_state("active_trade", recovered)
    emit_control_event(
        kind="trade_state_recovered",
        title=f"雷霆已恢复持仓镜像：{symbol} {side}",
        severity="warning",
        dedupe_key=f"trade_state_recovered:{symbol}:{side}:{opened_at}",
        details={
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "entry_avg_price": entry_price,
            "current_stop_price": round(current_stop_trigger_price, 8),
        },
    )
    return recovered


def run_once() -> dict[str, Any]:
    ensure_runtime_dirs()
    refresh_strategy_version(reason="demo_auto_run")
    if get_execution_mode() not in {"demo_auto", "testnet_auto"}:
        result = {"ok": False, "reason": "demo_auto_disabled"}
        heartbeat("demo_auto", status="idle", details=result)
        return result

    snapshot = read_runtime_state("engine_snapshot")
    if not snapshot:
        result = {"ok": False, "reason": "engine_snapshot_missing"}
        heartbeat("demo_auto", status="waiting", details=result)
        return result

    last_processed = read_runtime_state("demo_last_processed") or {}
    snapshot_id = snapshot.get("generated_at")
    if snapshot_id and last_processed.get("snapshot_id") == snapshot_id:
        result = {"ok": True, "reason": "snapshot_already_processed", "snapshot_id": snapshot_id}
        heartbeat("demo_auto", status="running", details=result)
        return result

    client = BinanceSignedFuturesClient()
    candidate = _load_candidate_config()
    active_trade = read_runtime_state("active_trade")
    if active_trade and active_trade.get("status") in {None, "open"}:
        result = {
            "ok": True,
            "reason": "active_trade_in_progress",
            "candidate_name": active_trade.get("candidate_name"),
            "symbol": active_trade.get("symbol"),
            "side": active_trade.get("side"),
        }
        update_runtime_state("demo_last_processed", {"snapshot_id": snapshot_id, "updated_at": _utc_now()})
        _write_status(snapshot, result)
        heartbeat("demo_auto", status="running", details=result)
        sync_control_plane(reason="active_trade_in_progress")
        return result

    positions = client.position_risk(snapshot.get("environment", {}).get("symbol", "BTCUSDT"))
    candidate_symbols = [
        str(item).strip().upper()
        for item in (candidate.get("symbols") or [snapshot.get("environment", {}).get("symbol", "BTCUSDT")])
        if str(item).strip()
    ]
    open_position = None
    for symbol in candidate_symbols:
        positions = client.position_risk(symbol)
        open_position = next((item for item in positions if abs(float(item.get("positionAmt", 0.0))) > 0.0), None)
        if open_position is not None:
            break
    if open_position is not None:
        recovered_trade = _rebuild_active_trade_from_exchange(client, snapshot, candidate, open_position)
        result = {
            "ok": True,
            "reason": "exchange_position_recovered",
            "candidate_name": recovered_trade.get("candidate_name"),
            "symbol": recovered_trade.get("symbol"),
            "side": recovered_trade.get("side"),
        }
        update_runtime_state("demo_last_processed", {"snapshot_id": snapshot_id, "updated_at": _utc_now()})
        _write_status(snapshot, result)
        heartbeat("demo_auto", status="running", details=result)
        sync_control_plane(force=True, reason="exchange_position_recovered")
        return result

    ok, reasons = _signal_ok(snapshot, candidate)
    if not ok:
        if candidate.get("name"):
            record_rejection(snapshot, candidate, reasons)
        result = {
            "ok": True,
            "reason": "entry_conditions_not_met",
            "candidate_name": candidate.get("name"),
            "reasons": reasons,
        }
        update_runtime_state("demo_last_processed", {"snapshot_id": snapshot_id, "updated_at": _utc_now()})
        _write_status(snapshot, result)
        heartbeat("demo_auto", status="running", details=result)
        sync_control_plane(reason="entry_conditions_not_met")
        return result

    account = client.account()
    available_balance = float(account.get("availableBalance") or account.get("totalWalletBalance") or 0.0)
    capital = demo_allocatable_equity(available_balance)
    quote_allocation = float(capital["quote_allocation_usdt"])
    leverage = int(candidate.get("leverage") or get_demo_leverage())
    signal = dict(snapshot.get("signal") or {})
    bias = str(signal.get("bias") or "").upper()
    side = "BUY" if bias == "LONG" else "SELL" if bias == "SHORT" else "BUY"
    hard_stop_net_roi_pct = signal.get("hard_stop_net_roi_pct_override")
    if hard_stop_net_roi_pct in (None, ""):
        hard_stop_net_roi_pct = _candidate_float(candidate, "hard_stop_net_roi_pct")
    take_profit_net_roi_pct = signal.get("take_profit_net_roi_pct_override")
    if take_profit_net_roi_pct in (None, ""):
        take_profit_net_roi_pct = _candidate_float(candidate, "take_profit_net_roi_pct")
    maker_first = signal.get("maker_first_override")
    if maker_first is None:
        maker_first = bool(candidate.get("maker_first", False))
    maker_entry_timeout_sec = signal.get("entry_timeout_sec_override")
    if maker_entry_timeout_sec in (None, ""):
        maker_entry_timeout_sec = _candidate_float(candidate, "maker_entry_timeout_sec")
    maker_price_ticks = signal.get("maker_price_ticks_override")
    if maker_price_ticks in (None, ""):
        maker_price_ticks = int(float(candidate.get("maker_price_ticks") or 0))
    plan_reason = str(candidate.get("name") or "demo_auto_candidate")
    sub_strategy = str(signal.get("sub_strategy") or "").strip()
    if sub_strategy:
        plan_reason = f"{plan_reason}:{sub_strategy}"
    plan = build_execution_plan(
        client=client,
        side=side,
        quote_allocation_usdt=quote_allocation,
        leverage=leverage,
        reason=plan_reason,
        symbol=str(snapshot.get("environment", {}).get("symbol") or "BTCUSDT"),
        stop_loss_pct=float(candidate.get("stop_loss_pct") or 0.25) if candidate.get("stop_loss_pct") not in (None, "") else None,
        stop_loss_net_roi_pct=hard_stop_net_roi_pct,
        breakeven_trigger_pct=float(candidate.get("breakeven_trigger_pct") or 0.3),
        breakeven_lock_pct=float(candidate.get("breakeven_lock_pct") or 0.05),
        trailing_callback_rate=float(candidate.get("trailing_callback_pct") or 0.25),
        take_profit_net_roi_pct=take_profit_net_roi_pct,
        estimated_fee_bps_per_side=float(candidate.get("estimated_fee_bps_per_side") or 5.0),
        max_hold_minutes=_candidate_float(candidate, "max_hold_minutes"),
        enable_breakeven=bool(candidate.get("enable_breakeven", True)),
        enable_trailing=bool(candidate.get("enable_trailing", True)),
        maker_first=bool(maker_first),
        maker_entry_timeout_sec=float(maker_entry_timeout_sec) if maker_entry_timeout_sec not in (None, "") else None,
        maker_price_ticks=int(float(maker_price_ticks or 0)),
    )
    try:
        preflight = preflight_test_order(client, plan)
        order = place_entry_order(client, plan)
        protection = sync_protection(client, plan)
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
        failure_reason = "entry_submission_failed"
        if "maker_first_entry_" in error_text:
            failure_reason = "maker_entry_not_filled"
        reasons = [error_text]
        if candidate.get("name"):
            record_rejection(snapshot, candidate, reasons)
        result = {
            "ok": True,
            "reason": failure_reason,
            "candidate_name": candidate.get("name"),
            "reasons": reasons,
            "symbol": plan.symbol,
            "side": plan.side,
        }
        update_runtime_state("demo_last_processed", {"snapshot_id": snapshot_id, "updated_at": _utc_now()})
        _write_status(snapshot, result)
        heartbeat("demo_auto", status="running", details=result)
        sync_control_plane(reason=failure_reason)
        return result
    entry_avg_price = float(order.get("avgPrice") or plan.reference_price)
    opened_at = _utc_now()
    active_trade = {
        "mode": get_journal_mode_for_execution(),
        "status": "open",
        "phase": "INITIAL_STOP",
        "opened_at": opened_at,
        "symbol": plan.symbol,
        "side": plan.side,
        "regime": snapshot.get("signal", {}).get("regime"),
        "candidate_name": candidate.get("name"),
        "sub_strategy": signal.get("sub_strategy"),
        "entry_snapshot_id": snapshot_id,
        "entry_signal": signal,
        "entry_market": {
            key: (snapshot.get("market") or {}).get(key)
            for key in (
                "mark_price",
                "funding_rate",
                "mark_index_basis_pct",
                "spread_bps",
                "depth_imbalance",
                "microprice_bias_bps",
                "ofi_window_sum",
                "taker_buy_ratio_5m",
                "aggressive_flow_delta",
                "price_change_5m_pct",
                "price_change_1h_pct",
            )
        },
        "plan": asdict(plan),
        "entry_order": order,
        "entry_order_id": order.get("orderId"),
        "entry_avg_price": entry_avg_price,
        "entry_update_time_ms": int(order.get("updateTime") or order.get("transactTime") or 0),
        "protection": protection,
        "current_stop_trigger_price": plan.initial_stop_price,
        "high_watermark": entry_avg_price,
        "low_watermark": entry_avg_price,
        "events": [
            {
                "event": "entry_filled",
                "at": opened_at,
                "avg_price": entry_avg_price,
                "quantity": plan.quantity,
            },
            {
                "event": "initial_stop_armed",
                "at": opened_at,
                "trigger_price": plan.initial_stop_price,
            },
        ],
    }
    update_runtime_state("active_trade", active_trade)
    result = {
        "ok": True,
        "reason": "entered_demo_position",
        "candidate_name": candidate.get("name"),
        "preflight": preflight,
        "order_id": order.get("orderId"),
        "quote_allocation_usdt": quote_allocation,
        "available_balance_usdt": available_balance,
        "virtual_equity_usdt": capital["virtual_equity_usdt"],
        "effective_equity_usdt": capital["effective_equity_usdt"],
    }
    update_runtime_state("demo_last_processed", {"snapshot_id": snapshot_id, "updated_at": _utc_now(), "last_entry_order_id": order.get("orderId")})
    _write_status(snapshot, result)
    heartbeat("demo_auto", status="running", details=result)
    emit_control_event(
        kind="trade_opened",
        title=f"雷霆已开仓：{plan.symbol} {plan.side}",
        severity="info",
        dedupe_key=f"trade_opened:{plan.symbol}:{order.get('orderId')}",
        details={
            "symbol": plan.symbol,
            "side": plan.side,
            "candidate_name": candidate.get("name"),
            "entry_avg_price": entry_avg_price,
            "quantity": plan.quantity,
            "quote_allocation_usdt": quote_allocation,
            "leverage": plan.leverage,
            "initial_stop_price": plan.initial_stop_price,
            "take_profit_net_roi_pct": plan.take_profit_net_roi_pct,
            "sub_strategy": signal.get("sub_strategy"),
            "execution_style": signal.get("execution_style"),
        },
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Leiting demo auto execution service.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-sec", type=int, default=10)
    args = parser.parse_args()
    try:
        client = BinanceSignedFuturesClient()
        client.assert_one_way_mode()
    except Exception as exc:  # noqa: BLE001
        details = {"ok": False, "reason": "one_way_mode_required", "error": str(exc)}
        heartbeat("demo_auto", status="error", details=details)
        print(json.dumps(details, ensure_ascii=False, indent=2))
        raise
    while True:
        result = run_once()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if args.once:
            break
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    main()
