"""Trade journal writer for Leiting."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import JOURNALS_DIR, ensure_runtime_dirs


def write_trade_journal(job_id: str, payload: dict[str, Any]) -> Path:
    """Write a structured close journal for a BTC trade."""
    ensure_runtime_dirs()
    path = JOURNALS_DIR / f"{job_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_close_journal(
    *,
    symbol: str,
    side: str,
    quantity: float,
    trades: list[dict],
    incomes: list[dict],
    reason: str,
    opened_at: datetime | None = None,
    entry_order_id: int | None = None,
    close_order_id: int | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    funding_entries = [item for item in incomes if str(item.get("incomeType") or "").upper() == "FUNDING_FEE"]
    gross_pnl = sum(float(item.get("realizedPnl", 0.0)) for item in trades if "realizedPnl" in item)
    fees = sum(float(item.get("commission", 0.0)) for item in trades if "commission" in item)
    funding = sum(float(item.get("income", 0.0)) for item in funding_entries)
    net_pnl = gross_pnl - fees + funding
    first_trade = trades[0] if trades else {}
    last_trade = trades[-1] if trades else {}
    closed_at_dt = datetime.now(timezone.utc)
    entry_avg_price = (
        sum(float(item.get("price", 0.0)) * float(item.get("qty", 0.0)) for item in trades if item.get("orderId") == entry_order_id)
        / max(1e-12, sum(float(item.get("qty", 0.0)) for item in trades if item.get("orderId") == entry_order_id))
        if entry_order_id is not None
        else float(first_trade.get("price", 0.0) or 0.0)
    )
    close_avg_price = (
        sum(float(item.get("price", 0.0)) * float(item.get("qty", 0.0)) for item in trades if item.get("orderId") == close_order_id)
        / max(1e-12, sum(float(item.get("qty", 0.0)) for item in trades if item.get("orderId") == close_order_id))
        if close_order_id is not None
        else float(last_trade.get("price", 0.0) or 0.0)
    )
    hold_minutes = round(((closed_at_dt - opened_at).total_seconds() / 60.0), 4) if opened_at else 0.0
    payload = {
        "opened_at": opened_at.isoformat() if opened_at else None,
        "closed_at": closed_at_dt.isoformat(),
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "entry_order_id": entry_order_id,
        "close_order_id": close_order_id,
        "entry_avg_price": round(entry_avg_price, 8) if entry_avg_price else 0.0,
        "close_avg_price": round(close_avg_price, 8) if close_avg_price else 0.0,
        "gross_pnl": round(gross_pnl, 8),
        "fees": round(fees, 8),
        "funding": round(funding, 8),
        "net_pnl": round(net_pnl, 8),
        "slippage_bps": 0.0,
        "hold_minutes": hold_minutes,
        "reason": reason,
        "first_trade": first_trade,
        "last_trade": last_trade,
        "trade_count": len(trades),
        "income_count": len(funding_entries),
        "raw_income_count": len(incomes),
    }
    if mode:
        payload["mode"] = mode
    return payload
