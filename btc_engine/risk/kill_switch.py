"""Kill-switch logic."""

from __future__ import annotations


def kill_switch_active(*, max_daily_loss_pct: float, daily_pnl_pct: float | None) -> tuple[bool, str | None]:
    """Return True when the system must stop opening new BTC positions."""
    if daily_pnl_pct is None:
        return False, None
    if daily_pnl_pct <= -abs(max_daily_loss_pct):
        return True, f"日内回撤达到 {daily_pnl_pct:.2f}%"
    return False, None
