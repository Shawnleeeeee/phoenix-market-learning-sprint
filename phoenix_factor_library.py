from __future__ import annotations

import math
from typing import Any


FACTOR_VERSION = "v1.0"
FACTOR_DEFINITIONS = [
    {
        "name": "momentum_score",
        "category": "momentum",
        "expression": "0.55*ret_1bar/1.5 + 0.30*ret_5bar/3 + 0.15*ret_15bar/6",
    },
    {
        "name": "trend_score",
        "category": "trend",
        "expression": "signed momentum * expansion + directional OI build + market/depth alignment",
    },
    {
        "name": "mean_reversion_score",
        "category": "mean_reversion",
        "expression": "-sign(momentum) * exhaustion(range, volume, OI unwind, BTC flat, liquidation)",
    },
    {
        "name": "volatility_regime_score",
        "category": "volatility",
        "expression": "0.40*range_to_atr + 0.25*body_to_atr + 0.35*volume_burst_ratio, normalized",
    },
    {
        "name": "liquidity_score",
        "category": "liquidity",
        "expression": "spread quality + slippage quality + top-5 depth quality",
    },
    {
        "name": "flow_score",
        "category": "order_flow",
        "expression": "taker_buy_ratio/aggressive_flow_delta centered to [-1,1]",
    },
    {
        "name": "oi_build_score",
        "category": "derivatives",
        "expression": "positive OI change strength, max(oi_5m/2, oi_15m/4)",
    },
    {
        "name": "oi_unwind_score",
        "category": "derivatives",
        "expression": "negative OI change strength, max(-oi_5m/2, -oi_15m/4)",
    },
    {
        "name": "crowding_score",
        "category": "derivatives",
        "expression": "funding_rate and mark/index basis, positive=crowded long, negative=crowded short",
    },
]


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def clamp(value: float, lower: float = -1.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def round4(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 4)


def sign(value: float | None, *, deadband: float = 1e-9) -> int:
    if value is None or abs(value) <= deadband:
        return 0
    return 1 if value > 0 else -1


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def sample_from_record(record: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(record.get("sample"))


def enrichments_from_record(record: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(record.get("enrichments"))


def context_from_record(record: dict[str, Any]) -> dict[str, Any]:
    interval = str(record.get("bar_interval") or sample_from_record(record).get("bar_interval") or "").strip()
    contexts = _as_dict(record.get("research_contexts"))
    if interval and isinstance(contexts.get(interval), dict):
        return dict(contexts[interval])
    if isinstance(record.get("context_3m"), dict):
        return dict(record["context_3m"])
    enrichments = enrichments_from_record(record)
    if isinstance(enrichments.get("context_3m"), dict):
        return dict(enrichments["context_3m"])
    return {}


def numeric_feature(record: dict[str, Any], name: str) -> float | None:
    for container in (
        sample_from_record(record),
        context_from_record(record),
        enrichments_from_record(record),
        record,
        _as_dict(record.get("factors")),
    ):
        if name in container:
            number = safe_float(container.get(name))
            if number is not None:
                return number
    return None


def text_feature(record: dict[str, Any], name: str) -> str:
    for container in (sample_from_record(record), context_from_record(record), enrichments_from_record(record), record):
        if name in container:
            text = str(container.get(name) or "").strip()
            if text:
                return text
    return ""


def _scaled(value: float | None, scale: float) -> float:
    if value is None or scale <= 0:
        return 0.0
    return clamp(float(value) / scale)


def _positive_scaled(value: float | None, scale: float) -> float:
    if value is None or scale <= 0:
        return 0.0
    return clamp(float(value) / scale, 0.0, 1.0)


def _volume_strength(record: dict[str, Any]) -> float:
    ratio = numeric_feature(record, "volume_burst_ratio")
    if ratio is None:
        return 0.0
    return clamp((ratio - 1.0) / 3.0, 0.0, 1.0)


def _range_strength(record: dict[str, Any]) -> float:
    ratio = numeric_feature(record, "range_to_atr")
    if ratio is None:
        return 0.0
    return clamp((ratio - 1.0) / 3.0, 0.0, 1.0)


def _body_strength(record: dict[str, Any]) -> float:
    ratio = numeric_feature(record, "body_to_atr")
    if ratio is None:
        return 0.0
    return clamp((ratio - 1.0) / 3.0, 0.0, 1.0)


def compute_momentum_score(record: dict[str, Any]) -> float:
    ret_1bar = _scaled(numeric_feature(record, "ret_1bar_pct"), 1.5)
    ret_5bar = _scaled(numeric_feature(record, "ret_5bar_pct"), 3.0)
    ret_15bar = _scaled(numeric_feature(record, "ret_15bar_pct"), 6.0)
    if ret_1bar == 0 and ret_5bar == 0 and ret_15bar == 0:
        direction = text_feature(record, "trigger_candle_direction") or text_feature(record, "candle_direction")
        if direction.lower() == "up":
            return 0.25
        if direction.lower() == "down":
            return -0.25
    return clamp((0.55 * ret_1bar) + (0.3 * ret_5bar) + (0.15 * ret_15bar))


def compute_oi_pressure_score(record: dict[str, Any]) -> float:
    oi_5m = _scaled(numeric_feature(record, "oi_change_5m_pct"), 2.0)
    oi_15m = _scaled(numeric_feature(record, "oi_change_15m_pct"), 4.0)
    return clamp((0.7 * oi_5m) + (0.3 * oi_15m))


def compute_oi_build_score(record: dict[str, Any]) -> float:
    oi_5m = _positive_scaled(numeric_feature(record, "oi_change_5m_pct"), 2.0)
    oi_15m = _positive_scaled(numeric_feature(record, "oi_change_15m_pct"), 4.0)
    return clamp(max(oi_5m, oi_15m), 0.0, 1.0)


def compute_oi_unwind_score(record: dict[str, Any]) -> float:
    oi_5m = _positive_scaled(-(numeric_feature(record, "oi_change_5m_pct") or 0.0), 2.0)
    oi_15m = _positive_scaled(-(numeric_feature(record, "oi_change_15m_pct") or 0.0), 4.0)
    return clamp(max(oi_5m, oi_15m), 0.0, 1.0)


def compute_flow_score(record: dict[str, Any]) -> float:
    ratio_1m = numeric_feature(record, "taker_buy_ratio_1m")
    ratio_5m = numeric_feature(record, "taker_buy_ratio_5m")
    aggressive_flow = numeric_feature(record, "aggressive_flow_delta")
    ratio_parts = []
    for ratio in (ratio_1m, ratio_5m):
        if ratio is not None and ratio > 0:
            ratio_parts.append(clamp(math.log(ratio) / math.log(2.0)))
    ratio_score = sum(ratio_parts) / len(ratio_parts) if ratio_parts else 0.0
    aggressive_score = clamp(aggressive_flow or 0.0)
    if not ratio_parts and aggressive_flow is None:
        return 0.0
    return clamp((0.65 * ratio_score) + (0.35 * aggressive_score))


def compute_crowding_score(record: dict[str, Any]) -> float:
    funding_rate = numeric_feature(record, "funding_rate")
    basis_pct = numeric_feature(record, "mark_index_basis_pct")
    funding_component = _scaled(funding_rate, 0.001)
    basis_component = _scaled(basis_pct, 0.5)
    return clamp((0.6 * funding_component) + (0.4 * basis_component))


def compute_liquidation_pressure_score(record: dict[str, Any]) -> float:
    long_usd = max(0.0, numeric_feature(record, "liquidation_long_usd_15m") or numeric_feature(record, "liquidation_long_usd") or 0.0)
    short_usd = max(0.0, numeric_feature(record, "liquidation_short_usd_15m") or numeric_feature(record, "liquidation_short_usd") or 0.0)
    total = long_usd + short_usd
    if total <= 0:
        return 0.0
    dominance = (short_usd - long_usd) / total
    intensity = clamp(math.log10(total + 1.0) / 6.0, 0.0, 1.0)
    return clamp(dominance * intensity)


def compute_market_regime_score(record: dict[str, Any]) -> float:
    btc_5m = _scaled(numeric_feature(record, "btcusdt_ret_5m_pct"), 1.0)
    btc_60m = _scaled(
        numeric_feature(record, "btcusdt_ret_60m_pct")
        if numeric_feature(record, "btcusdt_ret_60m_pct") is not None
        else numeric_feature(record, "btcusdt_ret_1h_pct"),
        3.0,
    )
    eth_5m = _scaled(numeric_feature(record, "ethusdt_ret_5m_pct"), 1.0)
    eth_60m = _scaled(
        numeric_feature(record, "ethusdt_ret_60m_pct")
        if numeric_feature(record, "ethusdt_ret_60m_pct") is not None
        else numeric_feature(record, "ethusdt_ret_1h_pct"),
        3.0,
    )
    return clamp((0.55 * btc_5m) + (0.2 * btc_60m) + (0.2 * eth_5m) + (0.05 * eth_60m))


def compute_microstructure_score(record: dict[str, Any]) -> float:
    imbalance = clamp(numeric_feature(record, "depth_imbalance") or 0.0)
    spread_bps = numeric_feature(record, "spread_bps")
    slippage_bps = numeric_feature(record, "estimated_slippage_bps")
    spread_quality = 1.0 - clamp((spread_bps or 0.0) / 30.0, 0.0, 1.0)
    slippage_quality = 1.0 - clamp((slippage_bps or 0.0) / 50.0, 0.0, 1.0)
    quality = (0.55 * spread_quality) + (0.45 * slippage_quality)
    return clamp(imbalance * quality)


def compute_liquidity_score(record: dict[str, Any]) -> float:
    spread_bps = numeric_feature(record, "spread_bps")
    slippage_bps = numeric_feature(record, "estimated_slippage_bps")
    bid_depth = max(0.0, numeric_feature(record, "bid_depth_notional_5") or 0.0)
    ask_depth = max(0.0, numeric_feature(record, "ask_depth_notional_5") or 0.0)
    total_depth = bid_depth + ask_depth
    spread_quality = 1.0 - clamp((spread_bps if spread_bps is not None else 15.0) / 30.0, 0.0, 1.0)
    slippage_quality = 1.0 - clamp((slippage_bps if slippage_bps is not None else 25.0) / 50.0, 0.0, 1.0)
    depth_quality = clamp(math.log10(total_depth + 1.0) / 7.0, 0.0, 1.0) if total_depth > 0 else 0.35
    return clamp((0.35 * spread_quality) + (0.35 * slippage_quality) + (0.3 * depth_quality), 0.0, 1.0)


def compute_volatility_regime_score(record: dict[str, Any]) -> float:
    return clamp(
        (0.4 * _range_strength(record)) + (0.25 * _body_strength(record)) + (0.35 * _volume_strength(record)),
        0.0,
        1.0,
    )


def compute_trend_score(record: dict[str, Any]) -> float:
    momentum = compute_momentum_score(record)
    oi_build_strength = compute_oi_build_score(record)
    market_regime = compute_market_regime_score(record)
    microstructure = compute_microstructure_score(record)
    flow_score = compute_flow_score(record)
    expansion = (0.45 * _volume_strength(record)) + (0.35 * _range_strength(record)) + (0.2 * _body_strength(record))
    expansion_multiplier = 0.55 + (0.45 * expansion)
    aligned_market = market_regime if sign(market_regime) == sign(momentum) else 0.0
    aligned_micro = microstructure if sign(microstructure) == sign(momentum) else 0.0
    aligned_flow = flow_score if sign(flow_score) == sign(momentum) else 0.0
    directional_oi_build = sign(momentum) * oi_build_strength
    return clamp(
        (momentum * expansion_multiplier)
        + (0.18 * directional_oi_build)
        + (0.15 * aligned_market)
        + (0.1 * aligned_micro)
        + (0.12 * aligned_flow)
    )


def compute_mean_reversion_score(record: dict[str, Any]) -> float:
    momentum = compute_momentum_score(record)
    direction = -sign(momentum, deadband=0.08)
    if direction == 0:
        return 0.0
    oi_unwind = clamp(-(numeric_feature(record, "oi_change_5m_pct") or 0.0) / 2.0, 0.0, 1.0)
    btc_flat = 1.0 - clamp(abs(numeric_feature(record, "btcusdt_ret_5m_pct") or 0.0) / 1.0, 0.0, 1.0)
    liquidation_support = abs(compute_liquidation_pressure_score(record))
    exhaustion = (
        0.3 * abs(momentum)
        + 0.25 * _range_strength(record)
        + 0.15 * _volume_strength(record)
        + 0.2 * oi_unwind
        + 0.1 * max(btc_flat, liquidation_support)
    )
    return clamp(direction * exhaustion)


def signed_bucket(value: float | None, *, prefix: str) -> str:
    number = safe_float(value)
    if number is None:
        return f"{prefix}_missing"
    if number <= -0.7:
        return f"{prefix}_short_extreme"
    if number <= -0.35:
        return f"{prefix}_short_strong"
    if number < -0.1:
        return f"{prefix}_short_mild"
    if number <= 0.1:
        return f"{prefix}_flat"
    if number < 0.35:
        return f"{prefix}_long_mild"
    if number < 0.7:
        return f"{prefix}_long_strong"
    return f"{prefix}_long_extreme"


def unsigned_bucket(value: float | None, *, prefix: str) -> str:
    number = safe_float(value)
    if number is None:
        return f"{prefix}_missing"
    if number < 0.25:
        return f"{prefix}_low"
    if number < 0.55:
        return f"{prefix}_medium"
    if number < 0.8:
        return f"{prefix}_high"
    return f"{prefix}_extreme"


def build_factor_vector(record: dict[str, Any]) -> dict[str, Any]:
    momentum_score = compute_momentum_score(record)
    trend_score = compute_trend_score(record)
    mean_reversion_score = compute_mean_reversion_score(record)
    volatility_regime_score = compute_volatility_regime_score(record)
    liquidity_score = compute_liquidity_score(record)
    oi_pressure_score = compute_oi_pressure_score(record)
    oi_build_score = compute_oi_build_score(record)
    oi_unwind_score = compute_oi_unwind_score(record)
    liquidation_pressure_score = compute_liquidation_pressure_score(record)
    market_regime_score = compute_market_regime_score(record)
    microstructure_score = compute_microstructure_score(record)
    flow_score = compute_flow_score(record)
    crowding_score = compute_crowding_score(record)
    return {
        "factor_version": FACTOR_VERSION,
        "factor_definition_count": len(FACTOR_DEFINITIONS),
        "momentum_score": round4(momentum_score),
        "momentum_bucket": signed_bucket(momentum_score, prefix="momentum"),
        "trend_score": round4(trend_score),
        "trend_bucket": signed_bucket(trend_score, prefix="trend"),
        "mean_reversion_score": round4(mean_reversion_score),
        "mean_reversion_bucket": signed_bucket(mean_reversion_score, prefix="reversion"),
        "volatility_regime_score": round4(volatility_regime_score),
        "volatility_regime_bucket": unsigned_bucket(volatility_regime_score, prefix="volatility"),
        "liquidity_score": round4(liquidity_score),
        "liquidity_bucket": unsigned_bucket(liquidity_score, prefix="liquidity"),
        "oi_pressure_score": round4(oi_pressure_score),
        "oi_pressure_bucket": signed_bucket(oi_pressure_score, prefix="oi_pressure"),
        "oi_build_score": round4(oi_build_score),
        "oi_build_bucket": unsigned_bucket(oi_build_score, prefix="oi_build"),
        "oi_unwind_score": round4(oi_unwind_score),
        "oi_unwind_bucket": unsigned_bucket(oi_unwind_score, prefix="oi_unwind"),
        "liquidation_pressure_score": round4(liquidation_pressure_score),
        "liquidation_pressure_bucket": signed_bucket(liquidation_pressure_score, prefix="liquidation_pressure"),
        "market_regime_score": round4(market_regime_score),
        "market_regime_bucket": signed_bucket(market_regime_score, prefix="market"),
        "microstructure_score": round4(microstructure_score),
        "microstructure_bucket": signed_bucket(microstructure_score, prefix="microstructure"),
        "flow_score": round4(flow_score),
        "flow_bucket": signed_bucket(flow_score, prefix="flow"),
        "crowding_score": round4(crowding_score),
        "crowding_bucket": signed_bucket(crowding_score, prefix="crowding"),
    }
