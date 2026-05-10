"""Simple sizing for the BTC engine."""

from __future__ import annotations


def compute_position_size(*, account_equity_usdt: float | None, risk_budget_pct: float, fixed_quote_allocation_usdt: float) -> float:
    """Compute quote allocation from risk budget and volatility."""
    if account_equity_usdt is None or account_equity_usdt <= 0:
        return round(fixed_quote_allocation_usdt, 4)
    budget = account_equity_usdt * (risk_budget_pct / 100.0) * 10.0
    return round(max(20.0, min(fixed_quote_allocation_usdt, budget)), 4)
