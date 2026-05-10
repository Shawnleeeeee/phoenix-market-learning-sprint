from __future__ import annotations

from typing import Any


def compute_metaorder_features(snapshot: dict[str, Any]) -> dict[str, float]:
    """Heuristic metaorder-style features until a richer flowpylib bridge exists."""

    buy_ratio = float(snapshot.get("taker_buy_ratio_5m") or 0.0)
    aggressive_delta = float(snapshot.get("aggressive_flow_delta") or 0.0)
    persistence = 1.0 if buy_ratio > 0.6 and aggressive_delta > 0 else 0.0
    unwind_risk = 1.0 if buy_ratio < 0.4 and aggressive_delta < 0 else 0.0
    return {
        "flow_metaorder_buy_ratio_5m": buy_ratio,
        "flow_metaorder_aggressive_delta": aggressive_delta,
        "flow_metaorder_persistence": persistence,
        "flow_metaorder_unwind_risk": unwind_risk,
    }
