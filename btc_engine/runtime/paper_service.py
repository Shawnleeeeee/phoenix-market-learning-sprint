"""Paper trading service for 雷霆.

Uses live public-market snapshots but never touches any trading API.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import (
    STATE_DIR,
    ensure_runtime_dirs,
    get_execution_mode,
    get_paper_candidate_config_path,
    get_paper_leverage,
    get_paper_max_open_positions,
    get_paper_position_fraction,
    get_paper_start_equity_usdt,
    get_review_journal_mode,
)
from btc_engine.review.export_review import maybe_export_formal_review
from btc_engine.review.journal import write_trade_journal
from btc_engine.runtime.service_health import heartbeat
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_candidate_config() -> dict[str, Any]:
    path = get_paper_candidate_config_path()
    return json.loads(path.read_text(encoding="utf-8"))


def _load_account_state() -> dict[str, Any]:
    state = read_runtime_state("paper_account")
    if state:
        return state
    starting_equity = get_paper_start_equity_usdt()
    account = {
        "mode": "paper",
        "started_at": _utc_now(),
        "starting_equity_usdt": starting_equity,
        "equity_usdt": starting_equity,
        "available_equity_usdt": starting_equity,
        "position_fraction": get_paper_position_fraction(),
        "leverage": get_paper_leverage(),
        "closed_trade_count": 0,
        "open_trade_count": 0,
        "last_update": _utc_now(),
    }
    update_runtime_state("paper_account", account)
    return account


def _round_qty(value: float) -> float:
    return round(value, 6)


def _paper_fee(notional: float, fee_bps: float) -> float:
    return notional * (fee_bps / 10_000.0)


def _mark_to_entry(mark_price: float, slippage_bps: float) -> float:
    return mark_price * (1.0 + slippage_bps / 10_000.0)


def _mark_to_exit(mark_price: float, slippage_bps: float) -> float:
    return mark_price * (1.0 - slippage_bps / 10_000.0)


def _allowed_regime(snapshot: dict[str, Any], candidate: dict[str, Any]) -> bool:
    allowed = {str(item).strip().lower() for item in candidate.get("allowed_regimes", [])}
    regime = str(((snapshot.get("signal") or {}).get("regime") or "")).strip().lower()
    return not allowed or regime in allowed


def _signal_ok(snapshot: dict[str, Any], candidate: dict[str, Any]) -> tuple[bool, list[str]]:
    signal = snapshot.get("signal") or {}
    reasons: list[str] = []
    if (signal.get("bias") or "") != "LONG":
        reasons.append("不是做多信号")
    if not signal.get("gates_passed", False):
        reasons.append("雷霆风控闸门未通过")
    if not _allowed_regime(snapshot, candidate):
        reasons.append("当前 regime 不在候选范围内")
    if float(signal.get("directional_confidence") or 0.0) < float(candidate.get("threshold") or 0.0):
        reasons.append("候选阈值未通过")
    return (not reasons), reasons


def _enter_trade(snapshot: dict[str, Any], account: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    mark_price = float((snapshot.get("market") or {}).get("mark_price") or 0.0)
    fee_bps = float(candidate.get("fee_bps_per_side") or 0.0)
    slippage_bps = float(candidate.get("slippage_bps_per_side") or 0.0)
    position_fraction = float(account["position_fraction"])
    equity_before = float(account["equity_usdt"])
    quote_allocation = round(equity_before * position_fraction, 4)
    leverage = int(account["leverage"])
    entry_price = _mark_to_entry(mark_price, slippage_bps)
    quantity = _round_qty((quote_allocation * leverage) / max(entry_price, 1e-12))
    notional = round(quantity * entry_price, 8)
    stop_loss_pct = float(candidate.get("stop_loss_pct") or 0.25)
    breakeven_trigger_pct = float(candidate.get("breakeven_trigger_pct") or 0.3)
    breakeven_lock_pct = float(candidate.get("breakeven_lock_pct") or 0.05)
    trailing_callback_pct = float(candidate.get("trailing_callback_pct") or 0.25)

    active_trade = {
        "mode": "paper",
        "status": "open",
        "phase": "INITIAL_STOP",
        "opened_at": _utc_now(),
        "symbol": snapshot.get("environment", {}).get("symbol", "BTCUSDT"),
        "side": "BUY",
        "bias": "LONG",
        "regime": snapshot.get("signal", {}).get("regime"),
        "candidate_name": candidate.get("name"),
        "quote_allocation_usdt": quote_allocation,
        "leverage": leverage,
        "quantity": quantity,
        "entry_price": round(entry_price, 8),
        "mark_price_at_entry": mark_price,
        "notional_usdt": notional,
        "fee_bps_per_side": fee_bps,
        "slippage_bps_per_side": slippage_bps,
        "stop_loss_pct": stop_loss_pct,
        "breakeven_trigger_pct": breakeven_trigger_pct,
        "breakeven_lock_pct": breakeven_lock_pct,
        "trailing_callback_pct": trailing_callback_pct,
        "initial_stop_price": round(entry_price * (1.0 - stop_loss_pct / 100.0), 8),
        "breakeven_trigger_price": round(entry_price * (1.0 + breakeven_trigger_pct / 100.0), 8),
        "breakeven_stop_price": round(entry_price * (1.0 + breakeven_lock_pct / 100.0), 8),
        "high_watermark": round(entry_price, 8),
        "events": [
            {
                "event": "paper_entry_opened",
                "at": _utc_now(),
                "entry_price": round(entry_price, 8),
                "quantity": quantity,
                "quote_allocation_usdt": quote_allocation,
            }
        ],
    }
    account["available_equity_usdt"] = round(max(0.0, equity_before - quote_allocation), 8)
    account["open_trade_count"] = 1
    account["last_update"] = _utc_now()
    update_runtime_state("paper_active_trade", active_trade)
    update_runtime_state("paper_account", account)
    return active_trade


def _build_effective_stop(active_trade: dict[str, Any]) -> float:
    base_stop = float(active_trade.get("initial_stop_price") or 0.0)
    if active_trade.get("phase") != "BREAKEVEN_TRAILING":
        return base_stop
    breakeven_stop = float(active_trade.get("breakeven_stop_price") or 0.0)
    high_watermark = float(active_trade.get("high_watermark") or 0.0)
    trailing_pct = float(active_trade.get("trailing_callback_pct") or 0.25)
    trailing_stop = high_watermark * (1.0 - trailing_pct / 100.0)
    return max(breakeven_stop, trailing_stop)


def _close_trade(
    *,
    snapshot: dict[str, Any],
    account: dict[str, Any],
    active_trade: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    mark_price = float((snapshot.get("market") or {}).get("mark_price") or 0.0)
    exit_price = _mark_to_exit(mark_price, float(active_trade.get("slippage_bps_per_side") or 0.0))
    entry_price = float(active_trade.get("entry_price") or 0.0)
    quantity = float(active_trade.get("quantity") or 0.0)
    notional_entry = float(active_trade.get("notional_usdt") or 0.0)
    notional_exit = quantity * exit_price
    fee_bps = float(active_trade.get("fee_bps_per_side") or 0.0)
    gross_pnl = (exit_price - entry_price) * quantity
    fees = _paper_fee(notional_entry, fee_bps) + _paper_fee(notional_exit, fee_bps)
    funding = 0.0
    net_pnl = gross_pnl - fees + funding
    equity_before = float(account["equity_usdt"])
    equity_after = round(equity_before + net_pnl, 8)

    closed_at = datetime.now(timezone.utc)
    opened_at = datetime.fromisoformat(str(active_trade["opened_at"]).replace("Z", "+00:00"))
    hold_minutes = round((closed_at - opened_at).total_seconds() / 60.0, 4)
    journal = {
        "mode": "paper",
        "opened_at": active_trade["opened_at"],
        "closed_at": closed_at.isoformat(),
        "symbol": active_trade["symbol"],
        "side": active_trade["side"],
        "quantity": quantity,
        "entry_avg_price": round(entry_price, 8),
        "close_avg_price": round(exit_price, 8),
        "gross_pnl": round(gross_pnl, 8),
        "fees": round(fees, 8),
        "funding": round(funding, 8),
        "net_pnl": round(net_pnl, 8),
        "slippage_bps": float(active_trade.get("slippage_bps_per_side") or 0.0),
        "hold_minutes": hold_minutes,
        "reason": reason,
        "trade_count": 2,
        "income_count": 0,
        "quote_allocation_usdt": float(active_trade.get("quote_allocation_usdt") or 0.0),
        "leverage": int(active_trade.get("leverage") or 1),
        "regime": active_trade.get("regime"),
        "candidate_name": active_trade.get("candidate_name"),
        "account_equity_before": round(equity_before, 8),
        "account_equity_after": round(equity_after, 8),
    }
    journal_id = f"paper_{active_trade['symbol']}_{int(closed_at.timestamp())}"
    journal_path = write_trade_journal(journal_id, journal)

    account["equity_usdt"] = equity_after
    account["available_equity_usdt"] = equity_after
    account["closed_trade_count"] = int(account.get("closed_trade_count", 0)) + 1
    account["open_trade_count"] = 0
    account["last_update"] = _utc_now()
    update_runtime_state("paper_account", account)
    update_runtime_state("paper_last_close", journal)
    update_runtime_state("last_close", journal)
    update_runtime_state(
        "paper_active_trade",
        {
            **active_trade,
            "status": "closed",
            "closed_at": journal["closed_at"],
            "close_reason": reason,
            "close_price": journal["close_avg_price"],
            "net_pnl": journal["net_pnl"],
        },
    )
    formal_review = maybe_export_formal_review()
    return {
        "closed": True,
        "journal": journal,
        "journal_path": str(journal_path),
        "formal_review": formal_review,
    }


def _manage_trade(snapshot: dict[str, Any], account: dict[str, Any], active_trade: dict[str, Any]) -> dict[str, Any]:
    mark_price = float((snapshot.get("market") or {}).get("mark_price") or 0.0)
    high_watermark = max(float(active_trade.get("high_watermark") or 0.0), mark_price)
    active_trade["high_watermark"] = round(high_watermark, 8)
    active_trade["last_mark_price"] = mark_price
    phase = active_trade.get("phase", "INITIAL_STOP")

    if phase == "INITIAL_STOP" and mark_price >= float(active_trade.get("breakeven_trigger_price") or 0.0):
        active_trade["phase"] = "BREAKEVEN_TRAILING"
        active_trade.setdefault("events", []).append(
            {
                "event": "paper_breakeven_trailing_armed",
                "at": _utc_now(),
                "mark_price": mark_price,
                "breakeven_stop_price": active_trade.get("breakeven_stop_price"),
            }
        )

    effective_stop = _build_effective_stop(active_trade)
    if mark_price <= effective_stop:
        reason = "paper_initial_stop" if active_trade.get("phase") == "INITIAL_STOP" else "paper_trailing_or_breakeven_stop"
        return _close_trade(snapshot=snapshot, account=account, active_trade=active_trade, reason=reason)

    update_runtime_state("paper_active_trade", active_trade)
    update_runtime_state("paper_account", account)
    return {"closed": False, "active_trade": active_trade, "effective_stop": round(effective_stop, 8)}


def _write_status(snapshot: dict[str, Any], account: dict[str, Any], active_trade: dict[str, Any] | None, candidate: dict[str, Any]) -> None:
    status = {
        "generated_at": _utc_now(),
        "mode": "paper",
        "candidate": candidate,
        "snapshot": {
            "symbol": snapshot.get("environment", {}).get("symbol"),
            "bias": snapshot.get("signal", {}).get("bias"),
            "regime": snapshot.get("signal", {}).get("regime"),
            "directional_confidence": snapshot.get("signal", {}).get("directional_confidence"),
            "gates_passed": snapshot.get("signal", {}).get("gates_passed"),
            "gate_reasons": snapshot.get("signal", {}).get("gate_reasons"),
            "mark_price": snapshot.get("market", {}).get("mark_price"),
        },
        "account": account,
        "active_trade": active_trade,
    }
    update_runtime_state("paper_status", status)
    markdown = "\n".join(
        [
            "# 雷霆 Paper 模拟状态",
            "",
            f"- 生成时间：{status['generated_at']}",
            f"- 模式：paper",
            f"- 当前权益：{account['equity_usdt']:.8f} USDT",
            f"- 可用权益：{account['available_equity_usdt']:.8f} USDT",
            f"- 已平仓数：{account['closed_trade_count']}",
            f"- 开仓数：{account['open_trade_count']}",
            f"- 候选配置：{candidate.get('name')}",
            f"- 当前 Bias：{status['snapshot']['bias']}",
            f"- 当前 Regime：{status['snapshot']['regime']}",
            f"- 当前标记价：{status['snapshot']['mark_price']}",
            f"- 风控放行：{'是' if status['snapshot']['gates_passed'] else '否'}",
            f"- 原因：{'; '.join(status['snapshot']['gate_reasons'] or [])}",
            f"- 活跃仓位：{'有' if active_trade and active_trade.get('status') == 'open' else '无'}",
        ]
    )
    (STATE_DIR / "paper_status.md").write_text(markdown, encoding="utf-8")


def run_once() -> dict[str, Any]:
    ensure_runtime_dirs()
    if get_execution_mode() not in {"paper", "paper_auto"}:
        result = {"ok": False, "reason": "paper_mode_disabled"}
        heartbeat("paper", status="idle", details=result)
        return result

    snapshot = read_runtime_state("engine_snapshot")
    if not snapshot:
        result = {"ok": False, "reason": "engine_snapshot_missing"}
        heartbeat("paper", status="waiting", details=result)
        return result

    last_processed = read_runtime_state("paper_last_processed") or {}
    snapshot_id = snapshot.get("generated_at")
    if snapshot_id and last_processed.get("snapshot_id") == snapshot_id:
        result = {"ok": True, "skipped": True, "reason": "snapshot_already_processed", "snapshot_id": snapshot_id}
        heartbeat("paper", status="running", details=result)
        return result

    candidate = _load_candidate_config()
    account = _load_account_state()
    active_trade = read_runtime_state("paper_active_trade")

    result: dict[str, Any]
    if active_trade and active_trade.get("status") == "open":
        result = _manage_trade(snapshot, account, active_trade)
    else:
        can_enter, reasons = _signal_ok(snapshot, candidate)
        if can_enter and get_paper_max_open_positions() >= 1:
            trade = _enter_trade(snapshot, account, candidate)
            result = {"ok": True, "entered": True, "trade": trade}
        else:
            result = {"ok": True, "entered": False, "reasons": reasons}

    update_runtime_state("paper_last_processed", {"snapshot_id": snapshot_id, "updated_at": _utc_now()})
    _write_status(snapshot, _load_account_state(), read_runtime_state("paper_active_trade"), candidate)
    heartbeat("paper", status="running", details={"snapshot_id": snapshot_id, **{k: v for k, v in result.items() if k in {'entered', 'closed', 'reason', 'reasons'}}})
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Leiting paper trading service.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-sec", type=int, default=10)
    args = parser.parse_args()

    while True:
        result = run_once()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if args.once:
            break
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    main()
