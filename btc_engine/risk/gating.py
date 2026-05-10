"""Risk gates for Leiting."""

from __future__ import annotations

from btc_engine.types import RiskConfig, StrategyConfig


def passes_risk_gates(
    *,
    strategy: StrategyConfig,
    risk: RiskConfig,
    bias: str,
    directional_confidence: float,
    execution_quality_score: float,
    event_risk_score: float,
    spread_bps: float,
    estimated_slippage_bps: float,
    funding_rate: float,
) -> tuple[bool, list[str]]:
    """Return True only if execution quality and event risk permit a trade."""
    reasons: list[str] = []
    if bias == "NONE":
        reasons.append("方向不明确")
    if directional_confidence < strategy.directional_confidence_min:
        reasons.append("方向置信度不足")
    if execution_quality_score < strategy.execution_quality_min:
        reasons.append("执行质量不足")
    if event_risk_score > strategy.event_risk_max:
        reasons.append("事件风险过高")
    if spread_bps > risk.max_spread_bps:
        reasons.append("点差过大")
    if estimated_slippage_bps > risk.max_slippage_bps:
        reasons.append("预计滑点过高")
    funding_bps = abs(funding_rate) * 10_000
    if bias == "LONG" and funding_bps > risk.funding_hard_cap_bps_long:
        reasons.append("多头资金费过热")
    if bias == "SHORT" and funding_bps > risk.funding_hard_cap_bps_short:
        reasons.append("空头资金费过热")
    return not reasons, reasons
