"""Microstructure confirmation scoring."""

from __future__ import annotations


def compute_microstructure_score(
    *,
    spread_bps: float,
    depth_imbalance: float,
    taker_buy_ratio_5m: float | None,
    aggressive_flow_delta: float | None,
    estimated_slippage_bps_buy: float,
    estimated_slippage_bps_sell: float,
) -> float:
    """Return a score from depth, taker flow, and slippage inputs."""
    score = 0.0
    score += max(-1.0, min(1.0, depth_imbalance * 2.5))
    if taker_buy_ratio_5m is not None:
        score += max(-1.0, min(1.0, (taker_buy_ratio_5m - 1.0) * 2.0))
    if aggressive_flow_delta is not None:
        score += max(-1.0, min(1.0, aggressive_flow_delta / 1000.0))
    avg_slippage = (estimated_slippage_bps_buy + estimated_slippage_bps_sell) / 2.0
    score -= max(0.0, min(1.0, avg_slippage / 10.0))
    score -= max(0.0, min(1.0, spread_bps / 8.0))
    return round(score / 3.0, 4)
