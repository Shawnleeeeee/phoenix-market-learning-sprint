"""Market regime classification."""

from __future__ import annotations


def classify_regime(*, momentum_score: float, microstructure_score: float, funding_rate: float) -> str:
    """Return the current BTC market regime."""
    if abs(momentum_score) >= 0.55 and abs(microstructure_score) >= 0.2:
        return "trend"
    if abs(momentum_score) <= 0.15 and abs(microstructure_score) <= 0.15:
        return "range"
    if abs(funding_rate) >= 0.0015:
        return "crowded"
    return "mixed"
