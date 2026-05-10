"""Virtual capital ledger for demo-auto execution."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from btc_engine.config import (
    get_demo_leverage,
    get_demo_position_fraction,
    get_demo_start_equity_usdt,
)
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_demo_account_state() -> dict[str, Any]:
    state = read_runtime_state("demo_account")
    if state:
        desired_fraction = get_demo_position_fraction()
        desired_leverage = get_demo_leverage()
        if (
            float(state.get("position_fraction") or 0.0) != desired_fraction
            or int(state.get("leverage") or 0) != desired_leverage
        ):
            state.update(
                {
                    "position_fraction": desired_fraction,
                    "leverage": desired_leverage,
                    "updated_at": _utc_now(),
                }
            )
            update_runtime_state("demo_account", state)
        return state
    starting_equity = round(get_demo_start_equity_usdt(), 8)
    state = {
        "mode": "demo_auto",
        "starting_equity_usdt": starting_equity,
        "equity_usdt": starting_equity,
        "realized_pnl_usdt": 0.0,
        "closed_trades": 0,
        "position_fraction": get_demo_position_fraction(),
        "leverage": get_demo_leverage(),
        "applied_journal_ids": [],
        "updated_at": _utc_now(),
    }
    update_runtime_state("demo_account", state)
    return state


def update_demo_account_after_close(journal_id: str, journal: dict[str, Any]) -> dict[str, Any]:
    state = load_demo_account_state()
    applied = list(state.get("applied_journal_ids") or [])
    if journal_id in applied:
        return state
    net_pnl = float(journal.get("net_pnl") or 0.0)
    next_equity = max(0.0, float(state.get("equity_usdt") or 0.0) + net_pnl)
    state.update(
        {
            "equity_usdt": round(next_equity, 8),
            "realized_pnl_usdt": round(float(state.get("realized_pnl_usdt") or 0.0) + net_pnl, 8),
            "closed_trades": int(state.get("closed_trades") or 0) + 1,
            "position_fraction": get_demo_position_fraction(),
            "leverage": get_demo_leverage(),
            "updated_at": _utc_now(),
        }
    )
    applied.append(journal_id)
    state["applied_journal_ids"] = applied[-200:]
    update_runtime_state("demo_account", state)
    return state


def demo_allocatable_equity(exchange_available_balance: float) -> dict[str, float]:
    state = load_demo_account_state()
    virtual_equity = float(state.get("equity_usdt") or 0.0)
    effective_equity = max(0.0, min(virtual_equity, float(exchange_available_balance or 0.0)))
    position_fraction = get_demo_position_fraction()
    quote_allocation = round(effective_equity * position_fraction, 4)
    return {
        "virtual_equity_usdt": round(virtual_equity, 8),
        "exchange_available_balance_usdt": round(float(exchange_available_balance or 0.0), 8),
        "effective_equity_usdt": round(effective_equity, 8),
        "quote_allocation_usdt": quote_allocation,
        "position_fraction": position_fraction,
    }
