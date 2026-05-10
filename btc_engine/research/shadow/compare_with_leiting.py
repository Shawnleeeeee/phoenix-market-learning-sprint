from __future__ import annotations

from typing import Any


def compare_shadow_with_leiting(
    shadow_summary: dict[str, Any],
    leiting_summary: dict[str, Any],
) -> dict[str, Any]:
    shadow_return = float(shadow_summary.get("net_return_pct") or 0.0)
    leiting_return = float(leiting_summary.get("net_return_pct") or 0.0)
    gap = shadow_return - leiting_return
    return {
        "shadow_net_return_pct": shadow_return,
        "leiting_net_return_pct": leiting_return,
        "net_return_gap_pct": gap,
        "shadow_outperforming": gap > 0,
    }
