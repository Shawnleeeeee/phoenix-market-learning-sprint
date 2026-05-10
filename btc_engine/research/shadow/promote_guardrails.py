from __future__ import annotations

from typing import Any


def evaluate_shadow_promotion_guardrails(summary: dict[str, Any]) -> dict[str, Any]:
    """Judge whether a shadow model is even eligible for manual review."""

    net_return = float(summary.get("net_return_pct") or 0.0)
    trade_count = int(summary.get("trade_count") or 0)
    max_drawdown = abs(float(summary.get("max_drawdown_pct") or 0.0))
    eligible = trade_count >= 20 and net_return > 0 and max_drawdown <= 5.0
    return {
        "eligible_for_manual_review": eligible,
        "reasons": {
            "enough_trades": trade_count >= 20,
            "positive_return": net_return > 0,
            "drawdown_within_limit": max_drawdown <= 5.0,
        },
    }
