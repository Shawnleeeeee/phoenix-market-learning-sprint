from __future__ import annotations


def compute_tca_features(snapshot: dict[str, object]) -> dict[str, float]:
    spread_bps = float(snapshot.get("spread_bps") or 0.0)
    slippage_bps = float(snapshot.get("estimated_slippage_bps") or 0.0)
    depth_imbalance = float(snapshot.get("depth_imbalance") or 0.0)
    effective_cost_bps = spread_bps + slippage_bps
    instability = abs(depth_imbalance) * slippage_bps
    return {
        "flow_tca_effective_cost_bps": effective_cost_bps,
        "flow_tca_instability_score": instability,
    }
