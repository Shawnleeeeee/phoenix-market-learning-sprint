"""Directional bias for the BTC engine."""

from __future__ import annotations

from btc_engine.types import Bias


def determine_bias(
    *,
    momentum_score: float,
    microstructure_score: float,
    funding_rate: float,
    allow_auto_short: bool,
    directional_confidence_min: float,
) -> tuple[Bias, float, list[str]]:
    """Return LONG / SHORT / NONE for BTC."""
    long_score = 0.0
    short_score = 0.0
    reasons: list[str] = []

    if momentum_score > 0:
        long_score += momentum_score
        reasons.append(f"动量偏多 {momentum_score:+.2f}")
    elif momentum_score < 0:
        short_score += abs(momentum_score)
        reasons.append(f"动量偏空 {momentum_score:+.2f}")

    if microstructure_score > 0:
        long_score += microstructure_score
        reasons.append(f"微观结构偏多 {microstructure_score:+.2f}")
    elif microstructure_score < 0:
        short_score += abs(microstructure_score)
        reasons.append(f"微观结构偏空 {microstructure_score:+.2f}")

    if funding_rate <= -0.0005:
        long_score += 0.15
        reasons.append(f"资金费偏空挤压多头机会 {funding_rate:+.5f}")
    elif funding_rate >= 0.0005:
        short_score += 0.15
        reasons.append(f"资金费偏多拥挤空头机会 {funding_rate:+.5f}")

    if long_score >= directional_confidence_min and long_score > short_score:
        return "LONG", round(long_score, 4), reasons
    if allow_auto_short and short_score >= directional_confidence_min and short_score > long_score:
        return "SHORT", round(short_score, 4), reasons
    return "NONE", round(max(long_score, short_score), 4), reasons
