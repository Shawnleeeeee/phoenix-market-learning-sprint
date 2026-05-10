from __future__ import annotations


def compute_changepoint_features(snapshot: dict[str, object]) -> dict[str, float]:
    """Placeholder for future change-point detection integration."""

    flow_delta = float(snapshot.get("aggressive_flow_delta") or 0.0)
    depth_imbalance = float(snapshot.get("depth_imbalance") or 0.0)
    spread_bps = float(snapshot.get("spread_bps") or 0.0)
    regime_shift_hint = 1.0 if abs(flow_delta) > 0.15 and abs(depth_imbalance) > 0.2 else 0.0
    liquidity_break_hint = 1.0 if spread_bps > 2.5 else 0.0
    return {
        "flow_regime_shift_hint": regime_shift_hint,
        "flow_liquidity_break_hint": liquidity_break_hint,
    }
