"""Orphan position recovery for Leiting."""

from __future__ import annotations

from datetime import datetime, timezone

from btc_engine.execution.binance_signed import BinanceSignedFuturesClient
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


def recover_orphan_positions(client: BinanceSignedFuturesClient, *, symbol: str) -> dict:
    """Rebuild worker state when real positions exist without an active worker."""
    positions = client.position_risk(symbol)
    active_position = next((item for item in positions if abs(float(item.get("positionAmt", 0.0))) > 0.0), None)
    active_trade = read_runtime_state("active_trade")
    if active_position and not active_trade:
        recovered = {
            "recovered_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "position_amt": float(active_position["positionAmt"]),
            "entry_price": float(active_position["entryPrice"]),
            "mark_price": float(active_position["markPrice"]),
            "unrealized_pnl": float(active_position["unRealizedProfit"]),
            "source": "orphan_recovery",
        }
        update_runtime_state("active_trade", recovered)
        return {"recovered": True, "active_trade": recovered}
    if not active_position and active_trade:
        update_runtime_state("active_trade", {"status": "cleared", "cleared_at": datetime.now(timezone.utc).isoformat()})
        return {"recovered": False, "cleared_stale_state": True}
    return {"recovered": False, "cleared_stale_state": False}
