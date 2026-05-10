"""Metrics for Leiting review packets."""

from __future__ import annotations

from statistics import mean
from typing import Any


def compute_trade_metrics(trades: list[dict[str, Any]]) -> dict:
    """Return trade-window metrics for the last closed batch."""
    if not trades:
        return {
            "window_trade_count": 0,
            "net_pnl": 0.0,
            "gross_pnl": 0.0,
            "fees": 0.0,
            "funding": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "avg_hold_minutes": 0.0,
            "max_drawdown": 0.0,
            "slippage_mean_bps": 0.0,
            "slippage_p95_bps": 0.0,
        }

    pnls = [float(t.get("net_pnl", 0.0)) for t in trades]
    gross = [float(t.get("gross_pnl", 0.0)) for t in trades]
    fees = [float(t.get("fees", 0.0)) for t in trades]
    funding = [float(t.get("funding", 0.0)) for t in trades]
    hold = [float(t.get("hold_minutes", 0.0)) for t in trades]
    slippage = sorted(float(t.get("slippage_bps", 0.0)) for t in trades)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else (999.0 if wins else 0.0)
    p95_index = min(len(slippage) - 1, max(0, int(round(len(slippage) * 0.95)) - 1))
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    return {
        "window_trade_count": len(trades),
        "net_pnl": round(sum(pnls), 8),
        "gross_pnl": round(sum(gross), 8),
        "fees": round(sum(fees), 8),
        "funding": round(sum(funding), 8),
        "win_rate": round(len(wins) / len(trades), 4),
        "profit_factor": round(profit_factor, 4),
        "avg_win": round(mean(wins), 8) if wins else 0.0,
        "avg_loss": round(mean(losses), 8) if losses else 0.0,
        "avg_hold_minutes": round(mean(hold), 4) if hold else 0.0,
        "max_drawdown": round(max_drawdown, 8),
        "slippage_mean_bps": round(mean(slippage), 4) if slippage else 0.0,
        "slippage_p95_bps": round(slippage[p95_index], 4) if slippage else 0.0,
    }
