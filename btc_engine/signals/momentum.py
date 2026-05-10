"""Momentum signal computation."""

from __future__ import annotations


def _score(change_pct: float | None, scale: float) -> float:
    if change_pct is None:
        return 0.0
    return max(-1.0, min(1.0, change_pct / scale))


def compute_momentum_signal(*, price_change_5m_pct: float | None, price_change_15m_pct: float | None, price_change_1h_pct: float | None) -> float:
    """Return a normalized momentum score."""
    parts = [
        _score(price_change_5m_pct, 0.5),
        _score(price_change_15m_pct, 1.2),
        _score(price_change_1h_pct, 2.5),
    ]
    return round((parts[0] * 0.4) + (parts[1] * 0.35) + (parts[2] * 0.25), 4)
