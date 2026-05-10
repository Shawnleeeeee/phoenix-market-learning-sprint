"""Simplified dual-symbol engine for Leiting demo auto trading."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from btc_engine.config import (
    RESEARCH_RUNTIME_DIR,
    ensure_runtime_dirs,
    environment_summary,
    get_demo_leverage,
    get_demo_position_fraction,
    get_demo_start_equity_usdt,
    get_execution_mode,
    get_market_stream_freshness_sec,
    load_runtime_candidate_config,
)
from btc_engine.market_data.depth import analyze_depth_levels, sample_depth
from btc_engine.market_data.funding import fetch_current_funding_context
from btc_engine.market_data.oi import fetch_current_oi_context
from btc_engine.market_data.public_client import BinancePublicFuturesClient
from btc_engine.runtime.service_health import heartbeat
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ema(values: list[float], period: int) -> float | None:
    if period <= 0 or len(values) < period:
        return None
    multiplier = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for value in values[period:]:
        ema = (value - ema) * multiplier + ema
    return ema


def _parse_dt(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _candidate_symbols(candidate: dict[str, Any]) -> list[str]:
    values = candidate.get("symbols")
    if isinstance(values, list):
        symbols = [str(item).strip().upper() for item in values if str(item).strip()]
        if symbols:
            return symbols
    return ["BTCUSDT", "ETHUSDT"]


def _load_symbol_market_stream(symbol: str, order_notional_usdt: float) -> dict[str, Any] | None:
    payload = read_runtime_state("market_stream_snapshot")
    if not payload:
        return None
    generated_at = _parse_dt(payload.get("generated_at"))
    if generated_at is None:
        return None
    age_sec = (datetime.now(timezone.utc) - generated_at).total_seconds()
    if age_sec > get_market_stream_freshness_sec():
        return None
    per_symbol = payload.get("per_symbol") or {}
    symbol_payload = per_symbol.get(symbol)
    if not isinstance(symbol_payload, dict):
        if str(payload.get("symbol") or "").upper() == symbol.upper():
            symbol_payload = payload
        else:
            return None
    depth_levels = symbol_payload.get("depth_levels") or {}
    bids = depth_levels.get("bids") or []
    asks = depth_levels.get("asks") or []
    if not bids or not asks:
        return None
    depth = analyze_depth_levels(bids=bids, asks=asks, order_notional_usdt=order_notional_usdt)
    return {
        "source": "ws",
        "generated_at": payload.get("generated_at"),
        "symbol": symbol,
        "last_event_type": symbol_payload.get("last_event_type"),
        "last_event_at": symbol_payload.get("last_event_at"),
        "last_trade_at": symbol_payload.get("last_trade_at"),
        "last_kline_at": symbol_payload.get("last_kline_at"),
        "last_depth_at": symbol_payload.get("last_depth_at"),
        "depth": depth,
        "trade": symbol_payload.get("trade") or {},
        "best_bid_price": symbol_payload.get("best_bid_price"),
        "best_ask_price": symbol_payload.get("best_ask_price"),
        "mid_price": symbol_payload.get("mid_price"),
        "microprice": symbol_payload.get("microprice"),
        "microprice_bias_bps": symbol_payload.get("microprice_bias_bps"),
        "ofi_latest": symbol_payload.get("ofi_latest"),
        "ofi_window_sum": symbol_payload.get("ofi_window_sum"),
        "ofi_window_mean": symbol_payload.get("ofi_window_mean"),
        "kline_cache": list(symbol_payload.get("kline_cache") or []),
        "price_change_5m_pct": symbol_payload.get("price_change_5m_pct"),
        "price_change_15m_pct": symbol_payload.get("price_change_15m_pct"),
        "price_change_1h_pct": symbol_payload.get("price_change_1h_pct"),
    }


def _fetch_recent_klines(client: BinancePublicFuturesClient, symbol: str, interval: str, limit: int = 60) -> list[dict[str, Any]]:
    rows = client.get_json("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "open_time_ms": int(row[0]),
                "close_time_ms": int(row[6]),
                "open_price": float(row[1]),
                "high_price": float(row[2]),
                "low_price": float(row[3]),
                "close_price": float(row[4]),
                "volume": float(row[5]),
                "is_closed": True,
            }
        )
    return items[-limit:]


def _price_changes_from_cache(kline_cache: list[dict[str, Any]]) -> tuple[float | None, float | None, float | None]:
    if len(kline_cache) < 13:
        return None, None, None
    closes = [float(item["close_price"]) for item in kline_cache]
    latest = closes[-1]

    def pct(back: int) -> float:
        base = closes[-1 - back]
        return ((latest - base) / base) * 100 if base else 0.0

    return round(pct(1), 6), round(pct(3), 6), round(pct(12), 6)


def _execution_quality(depth: dict[str, Any], *, max_spread_bps: float, max_slippage_bps: float) -> float:
    avg_slippage = (
        float(depth.get("estimated_slippage_bps_buy") or 0.0) + float(depth.get("estimated_slippage_bps_sell") or 0.0)
    ) / 2.0
    return round(
        max(
            0.0,
            min(
                10.0,
                10.0
                - (float(depth.get("spread_bps") or 0.0) / max(max_spread_bps, 0.1)) * 2.0
                - (avg_slippage / max(max_slippage_bps, 0.1)) * 3.0
                + min(1.5, abs(float(depth.get("depth_imbalance") or 0.0)) * 1.2),
            ),
        ),
        4,
    )


def _simple_signal(
    *,
    symbol: str,
    candidate: dict[str, Any],
    funding: dict[str, Any],
    oi: dict[str, Any],
    market: dict[str, Any],
) -> dict[str, Any]:
    engine_mode = str(candidate.get("engine_mode") or "v3_simple").strip().lower()
    kline_cache = list(market.get("kline_cache") or [])
    closes = [float(item.get("close_price") or 0.0) for item in kline_cache]
    highs = [float(item.get("high_price") or 0.0) for item in kline_cache]
    lows = [float(item.get("low_price") or 0.0) for item in kline_cache]
    latest_close = closes[-1] if closes else float(funding.get("mark_price") or 0.0)
    latest_open = float(kline_cache[-1].get("open_price") or latest_close) if kline_cache else latest_close
    latest_high = highs[-1] if highs else latest_close
    latest_low = lows[-1] if lows else latest_close
    prev_close = closes[-2] if len(closes) >= 2 else latest_close
    ema_fast_bars = int(candidate.get("ema_fast_bars") or 9)
    ema_slow_bars = int(candidate.get("ema_slow_bars") or 21)
    breakout_lookback = int(candidate.get("breakout_lookback_bars") or 12)
    entry_style = str(candidate.get("entry_style") or ("router" if engine_mode == "v6_router" else "breakout_flow")).strip().lower()
    ema_fast = _ema(closes, ema_fast_bars) or latest_close
    ema_slow = _ema(closes, ema_slow_bars) or latest_close
    prev_high = max(highs[-1 - breakout_lookback : -1]) if len(highs) > breakout_lookback else latest_close
    prev_low = min(lows[-1 - breakout_lookback : -1]) if len(lows) > breakout_lookback else latest_close
    taker_ratio = float(oi.get("taker_buy_ratio_5m") or 1.0)
    aggressive_flow_delta = float(oi.get("aggressive_flow_delta") or 0.0)
    depth_imbalance = float(market["depth"].get("depth_imbalance") or 0.0)
    microprice_bias_bps = float(market.get("microprice_bias_bps") or 0.0)
    ofi_latest = float(market.get("ofi_latest") or 0.0)
    ofi_window_sum = float(market.get("ofi_window_sum") or 0.0)
    ofi_window_mean = float(market.get("ofi_window_mean") or 0.0)
    execution_quality_score = _execution_quality(
        market["depth"],
        max_spread_bps=float(candidate.get("max_spread_bps") or 1.2),
        max_slippage_bps=float(candidate.get("max_estimated_slippage_bps") or 2.0),
    )
    ema_gap_pct = ((ema_fast - ema_slow) / latest_close * 100.0) if latest_close else 0.0
    trend_up = ema_fast > ema_slow
    trend_down = ema_fast < ema_slow
    breakout_up = latest_close > prev_high and trend_up
    breakout_down = latest_close < prev_low and trend_down
    taker_long_ok = taker_ratio >= float(candidate.get("min_taker_buy_ratio_long") or 1.02)
    taker_short_ok = taker_ratio <= float(candidate.get("max_taker_buy_ratio_short") or 0.98)
    aggressive_long_ok = aggressive_flow_delta >= float(candidate.get("min_aggressive_flow_delta_long") or 0.0)
    aggressive_short_ok = aggressive_flow_delta <= float(candidate.get("max_aggressive_flow_delta_short") or 0.0)
    min_ema_gap_pct = float(candidate.get("min_ema_gap_pct") or 0.0)
    min_depth_imbalance_long = float(candidate.get("min_depth_imbalance_long") or 0.0)
    max_depth_imbalance_short = float(candidate.get("max_depth_imbalance_short") or 0.0)
    min_microprice_bias_bps_long = float(candidate.get("min_microprice_bias_bps_long") or 0.0)
    max_microprice_bias_bps_short = float(candidate.get("max_microprice_bias_bps_short") or 0.0)
    min_ofi_window_sum_long = float(candidate.get("min_ofi_window_sum_long") or 0.0)
    max_ofi_window_sum_short = float(candidate.get("max_ofi_window_sum_short") or 0.0)
    require_breakout_confirmation = bool(candidate.get("require_breakout_confirmation", False))
    require_flow_confirmation = bool(candidate.get("require_flow_confirmation", False))
    pullback_lookback_bars = int(candidate.get("pullback_lookback_bars") or 3)
    reclaim_tolerance_pct = float(candidate.get("reclaim_tolerance_pct") or 0.03)
    max_extension_from_ema_pct = float(candidate.get("max_extension_from_ema_pct") or 0.08)
    microprice_long_ok = microprice_bias_bps >= min_microprice_bias_bps_long
    microprice_short_ok = microprice_bias_bps <= max_microprice_bias_bps_short
    ofi_long_ok = ofi_window_sum >= min_ofi_window_sum_long
    ofi_short_ok = ofi_window_sum <= max_ofi_window_sum_short
    flow_long_setup = (
        trend_up
        and taker_long_ok
        and aggressive_long_ok
        and depth_imbalance >= min_depth_imbalance_long
        and microprice_long_ok
        and ofi_long_ok
    )
    flow_short_setup = (
        trend_down
        and taker_short_ok
        and aggressive_short_ok
        and depth_imbalance <= max_depth_imbalance_short
        and microprice_short_ok
        and ofi_short_ok
    )

    recent_low = min(lows[-pullback_lookback_bars:]) if len(lows) >= pullback_lookback_bars else latest_low
    recent_high = max(highs[-pullback_lookback_bars:]) if len(highs) >= pullback_lookback_bars else latest_high
    extension_from_ema_pct = abs((latest_close - ema_fast) / latest_close * 100.0) if latest_close else 0.0
    pullback_touched_fast_long = recent_low <= ema_fast * (1.0 + reclaim_tolerance_pct / 100.0)
    pullback_touched_fast_short = recent_high >= ema_fast * (1.0 - reclaim_tolerance_pct / 100.0)
    reclaimed_long = latest_close >= ema_fast and latest_close >= latest_open and latest_close >= prev_close
    reclaimed_short = latest_close <= ema_fast and latest_close <= latest_open and latest_close <= prev_close
    not_extended_long = latest_close >= ema_fast and extension_from_ema_pct <= max_extension_from_ema_pct
    not_extended_short = latest_close <= ema_fast and extension_from_ema_pct <= max_extension_from_ema_pct
    pullback_reclaim_long = (
        trend_up
        and ema_gap_pct >= min_ema_gap_pct
        and pullback_touched_fast_long
        and reclaimed_long
        and not_extended_long
    )
    pullback_reclaim_short = (
        trend_down
        and (-ema_gap_pct) >= min_ema_gap_pct
        and pullback_touched_fast_short
        and reclaimed_short
        and not_extended_short
    )

    sub_strategy: str | None = None
    execution_style = "maker" if bool(candidate.get("maker_first", False)) else "taker"
    maker_first_override: bool | None = None
    take_profit_override: float | None = None
    hard_stop_override: float | None = None
    entry_timeout_override: float | None = None
    maker_price_ticks_override: int | None = None
    long_setup = False
    short_setup = False
    impulse_long_setup = False
    impulse_short_setup = False
    mean_reversion_long_setup = False
    mean_reversion_short_setup = False

    if engine_mode == "v6_router":
        price_vs_ema_fast_pct = ((latest_close - ema_fast) / latest_close * 100.0) if latest_close else 0.0
        impulse_breakout_lookback = int(candidate.get("impulse_breakout_lookback_bars") or breakout_lookback)
        impulse_high = max(highs[-1 - impulse_breakout_lookback : -1]) if len(highs) > impulse_breakout_lookback else latest_close
        impulse_low = min(lows[-1 - impulse_breakout_lookback : -1]) if len(lows) > impulse_breakout_lookback else latest_close
        impulse_breakout_up = latest_close > impulse_high and trend_up
        impulse_breakout_down = latest_close < impulse_low and trend_down
        impulse_min_ema_gap_pct = float(candidate.get("impulse_min_ema_gap_pct") or min_ema_gap_pct)
        impulse_long_setup = (
            trend_up
            and ema_gap_pct >= impulse_min_ema_gap_pct
            and impulse_breakout_up
            and taker_ratio >= float(candidate.get("impulse_min_taker_buy_ratio_long") or 1.06)
            and aggressive_flow_delta >= float(candidate.get("impulse_min_aggressive_flow_delta_long") or 60.0)
            and depth_imbalance >= float(candidate.get("impulse_min_depth_imbalance_long") or 0.08)
            and microprice_bias_bps >= float(candidate.get("impulse_min_microprice_bias_bps_long") or 0.04)
            and ofi_window_sum >= float(candidate.get("impulse_min_ofi_window_sum_long") or 25.0)
        )
        impulse_short_setup = (
            trend_down
            and (-ema_gap_pct) >= impulse_min_ema_gap_pct
            and impulse_breakout_down
            and taker_ratio <= float(candidate.get("impulse_max_taker_buy_ratio_short") or 0.94)
            and aggressive_flow_delta <= float(candidate.get("impulse_max_aggressive_flow_delta_short") or -60.0)
            and depth_imbalance <= float(candidate.get("impulse_max_depth_imbalance_short") or -0.08)
            and microprice_bias_bps <= float(candidate.get("impulse_max_microprice_bias_bps_short") or -0.04)
            and ofi_window_sum <= float(candidate.get("impulse_max_ofi_window_sum_short") or -25.0)
        )

        mr_max_counter_trend_gap_pct = float(candidate.get("mr_max_counter_trend_gap_pct") or 0.05)
        mr_extension_pct = float(candidate.get("mr_extension_pct") or 0.05)
        mean_reversion_long_setup = (
            ema_gap_pct >= -mr_max_counter_trend_gap_pct
            and price_vs_ema_fast_pct <= -mr_extension_pct
            and taker_ratio <= float(candidate.get("mr_max_taker_buy_ratio_long") or 0.98)
            and aggressive_flow_delta <= float(candidate.get("mr_max_aggressive_flow_delta_long") or -20.0)
            and depth_imbalance <= float(candidate.get("mr_max_depth_imbalance_long") or -0.05)
            and ofi_window_sum <= float(candidate.get("mr_max_ofi_window_sum_long") or -20.0)
            and microprice_bias_bps >= float(candidate.get("mr_min_microprice_reversal_bps_long") or 0.02)
            and ofi_latest >= float(candidate.get("mr_min_ofi_latest_long") or 1.0)
            and execution_quality_score >= float(candidate.get("mr_min_execution_quality_score") or 8.9)
        )
        mean_reversion_short_setup = (
            ema_gap_pct <= mr_max_counter_trend_gap_pct
            and price_vs_ema_fast_pct >= mr_extension_pct
            and taker_ratio >= float(candidate.get("mr_min_taker_buy_ratio_short") or 1.02)
            and aggressive_flow_delta >= float(candidate.get("mr_min_aggressive_flow_delta_short") or 20.0)
            and depth_imbalance >= float(candidate.get("mr_min_depth_imbalance_short") or 0.05)
            and ofi_window_sum >= float(candidate.get("mr_min_ofi_window_sum_short") or 20.0)
            and microprice_bias_bps <= float(candidate.get("mr_max_microprice_reversal_bps_short") or -0.02)
            and ofi_latest <= float(candidate.get("mr_max_ofi_latest_short") or -1.0)
            and execution_quality_score >= float(candidate.get("mr_min_execution_quality_score") or 8.9)
        )
        long_setup = impulse_long_setup or mean_reversion_long_setup
        short_setup = impulse_short_setup or mean_reversion_short_setup
    elif entry_style == "pullback_reclaim":
        long_setup = pullback_reclaim_long
        short_setup = pullback_reclaim_short
        if require_flow_confirmation:
            long_setup = long_setup and flow_long_setup
            short_setup = short_setup and flow_short_setup
    else:
        long_setup = trend_up and ema_gap_pct >= min_ema_gap_pct
        short_setup = trend_down and (-ema_gap_pct) >= min_ema_gap_pct
        if require_breakout_confirmation:
            long_setup = long_setup and breakout_up
            short_setup = short_setup and breakout_down
        else:
            long_setup = long_setup and (breakout_up or flow_long_setup)
            short_setup = short_setup and (breakout_down or flow_short_setup)
        if require_flow_confirmation:
            long_setup = long_setup and flow_long_setup
            short_setup = short_setup and flow_short_setup
    long_score = 0.0
    short_score = 0.0
    if impulse_long_setup:
        long_score += 0.26
    if impulse_short_setup:
        short_score += 0.26
    if mean_reversion_long_setup:
        long_score += 0.22
    if mean_reversion_short_setup:
        short_score += 0.22
    if long_setup:
        long_score += 0.12 + min(0.13, max(0.0, ema_gap_pct) * 8.0)
    if short_setup:
        short_score += 0.12 + min(0.13, max(0.0, -ema_gap_pct) * 8.0)
    if pullback_reclaim_long:
        long_score += 0.12
    if pullback_reclaim_short:
        short_score += 0.12
    if breakout_up:
        long_score += 0.18
    if breakout_down:
        short_score += 0.18
    if flow_long_setup:
        long_score += 0.12 + min(0.13, max(0.0, taker_ratio - 1.0) * 4.0 + max(0.0, aggressive_flow_delta) / 2000.0)
    if flow_short_setup:
        short_score += 0.12 + min(0.13, max(0.0, 1.0 - taker_ratio) * 4.0 + max(0.0, -aggressive_flow_delta) / 2000.0)
    if long_setup and depth_imbalance > 0:
        long_score += min(0.12, depth_imbalance * 0.5)
    if short_setup and depth_imbalance < 0:
        short_score += min(0.12, abs(depth_imbalance) * 0.5)
    if long_setup and microprice_bias_bps > 0:
        long_score += min(0.10, microprice_bias_bps / 12.0)
    if short_setup and microprice_bias_bps < 0:
        short_score += min(0.10, abs(microprice_bias_bps) / 12.0)
    if long_setup and ofi_window_sum > 0:
        long_score += min(0.10, ofi_window_sum / 1500.0)
    if short_setup and ofi_window_sum < 0:
        short_score += min(0.10, abs(ofi_window_sum) / 1500.0)
    flow_bias_score = max(
        -1.0,
        min(
            1.0,
            (taker_ratio - 1.0) * 2.5
            + (aggressive_flow_delta / 4000.0)
            + depth_imbalance,
        ),
    )
    threshold = float(candidate.get("threshold") or 0.25)
    allow_long = bool(candidate.get("allow_long", True))
    allow_short = bool(candidate.get("allow_short", True))
    bias = "NONE"
    confidence = 0.0
    reasons: list[str] = []
    if allow_long and long_score >= threshold and long_score > short_score:
        bias = "LONG"
        confidence = round(long_score, 4)
        reasons = []
        if engine_mode == "v6_router" and impulse_long_setup:
            sub_strategy = "taker_impulse"
            execution_style = "taker"
            maker_first_override = False
            take_profit_override = float(candidate.get("impulse_take_profit_net_roi_pct") or candidate.get("take_profit_net_roi_pct") or 55.0)
            hard_stop_override = float(candidate.get("impulse_hard_stop_net_roi_pct") or candidate.get("hard_stop_net_roi_pct") or 18.0)
            reasons.extend(["顺势冲击", "突破确认", "订单流同向"])
        elif engine_mode == "v6_router" and mean_reversion_long_setup:
            sub_strategy = "maker_mean_reversion"
            execution_style = "maker"
            maker_first_override = True
            take_profit_override = float(candidate.get("mr_take_profit_net_roi_pct") or candidate.get("take_profit_net_roi_pct") or 30.0)
            hard_stop_override = float(candidate.get("mr_hard_stop_net_roi_pct") or candidate.get("hard_stop_net_roi_pct") or 14.0)
            entry_timeout_override = float(candidate.get("mr_maker_entry_timeout_sec") or candidate.get("maker_entry_timeout_sec") or 8.0)
            maker_price_ticks_override = int(float(candidate.get("mr_maker_price_ticks") or candidate.get("maker_price_ticks") or 0))
            reasons.extend(["失衡回归", "microprice翻转", "被动吃回归"])
        elif trend_up:
            reasons.append(f"EMA{ema_fast_bars}>{ema_slow_bars}")
        if breakout_up:
            reasons.append(f"{breakout_lookback}根突破确认")
        if flow_long_setup:
            reasons.append("主动买盘确认")
        if long_setup and depth_imbalance > 0:
            reasons.append("盘口偏多")
        if not reasons:
            reasons.append("多头结构确认")
    elif allow_short and short_score >= threshold and short_score > long_score:
        bias = "SHORT"
        confidence = round(short_score, 4)
        reasons = []
        if engine_mode == "v6_router" and impulse_short_setup:
            sub_strategy = "taker_impulse"
            execution_style = "taker"
            maker_first_override = False
            take_profit_override = float(candidate.get("impulse_take_profit_net_roi_pct") or candidate.get("take_profit_net_roi_pct") or 55.0)
            hard_stop_override = float(candidate.get("impulse_hard_stop_net_roi_pct") or candidate.get("hard_stop_net_roi_pct") or 18.0)
            reasons.extend(["顺势冲击", "跌破确认", "订单流同向"])
        elif engine_mode == "v6_router" and mean_reversion_short_setup:
            sub_strategy = "maker_mean_reversion"
            execution_style = "maker"
            maker_first_override = True
            take_profit_override = float(candidate.get("mr_take_profit_net_roi_pct") or candidate.get("take_profit_net_roi_pct") or 30.0)
            hard_stop_override = float(candidate.get("mr_hard_stop_net_roi_pct") or candidate.get("hard_stop_net_roi_pct") or 14.0)
            entry_timeout_override = float(candidate.get("mr_maker_entry_timeout_sec") or candidate.get("maker_entry_timeout_sec") or 8.0)
            maker_price_ticks_override = int(float(candidate.get("mr_maker_price_ticks") or candidate.get("maker_price_ticks") or 0))
            reasons.extend(["失衡回归", "microprice翻转", "被动吃回归"])
        elif trend_down:
            reasons.append(f"EMA{ema_fast_bars}<EMA{ema_slow_bars}")
        if breakout_down:
            reasons.append(f"{breakout_lookback}根跌破确认")
        if flow_short_setup:
            reasons.append("主动卖盘确认")
        if short_setup and depth_imbalance < 0:
            reasons.append("盘口偏空")
        if not reasons:
            reasons.append("空头结构确认")
    gate_reasons: list[str] = []
    spread_bps = float(market["depth"].get("spread_bps") or 0.0)
    estimated_slippage_bps = (
        float(market["depth"].get("estimated_slippage_bps_buy") or 0.0)
        if bias != "SHORT"
        else float(market["depth"].get("estimated_slippage_bps_sell") or 0.0)
    )
    if bias == "NONE":
        gate_reasons.append("没有明确多空信号")
    if confidence < threshold:
        gate_reasons.append("候选阈值未通过")
    if execution_quality_score < float(candidate.get("min_execution_quality_score") or 7.0):
        gate_reasons.append("执行质量不足")
    if spread_bps > float(candidate.get("max_spread_bps") or 1.2):
        gate_reasons.append("点差过大")
    if estimated_slippage_bps > float(candidate.get("max_estimated_slippage_bps") or 2.0):
        gate_reasons.append("预计滑点过高")
    regime = "trend" if (long_setup or short_setup) else "mixed"
    if engine_mode == "v6_router":
        if impulse_long_setup or impulse_short_setup:
            regime = "impulse"
        elif mean_reversion_long_setup or mean_reversion_short_setup:
            regime = "mean_reversion"
        else:
            regime = "mixed"
    return {
        "symbol": symbol,
        "bias": bias,
        "entry_side": "BUY" if bias == "LONG" else "SELL" if bias == "SHORT" else None,
        "directional_confidence": round(confidence, 4),
        "directional_reasons": reasons,
        "regime": regime,
        "execution_quality_score": execution_quality_score,
        "event_risk_score": 0.0,
        "gates_passed": not gate_reasons,
        "gate_reasons": gate_reasons,
        "ema_fast": round(ema_fast, 6),
        "ema_slow": round(ema_slow, 6),
        "ema_gap_pct": round(ema_gap_pct, 6),
        "flow_bias_score": round(flow_bias_score, 6),
        "breakout_high": round(prev_high, 6),
        "breakout_low": round(prev_low, 6),
        "trend_up": trend_up,
        "trend_down": trend_down,
        "breakout_up": breakout_up,
        "breakout_down": breakout_down,
        "entry_style": entry_style,
        "flow_long_setup": flow_long_setup,
        "flow_short_setup": flow_short_setup,
        "pullback_reclaim_long": pullback_reclaim_long,
        "pullback_reclaim_short": pullback_reclaim_short,
        "extension_from_ema_pct": round(extension_from_ema_pct, 6),
        "price_vs_ema_fast_pct": round(((latest_close - ema_fast) / latest_close * 100.0) if latest_close else 0.0, 6),
        "microprice_bias_bps": round(microprice_bias_bps, 6),
        "ofi_latest": round(ofi_latest, 6),
        "ofi_window_sum": round(ofi_window_sum, 6),
        "ofi_window_mean": round(ofi_window_mean, 6),
        "impulse_long_setup": impulse_long_setup,
        "impulse_short_setup": impulse_short_setup,
        "mean_reversion_long_setup": mean_reversion_long_setup,
        "mean_reversion_short_setup": mean_reversion_short_setup,
        "sub_strategy": sub_strategy,
        "execution_style": execution_style,
        "maker_first_override": maker_first_override,
        "take_profit_net_roi_pct_override": take_profit_override,
        "hard_stop_net_roi_pct_override": hard_stop_override,
        "entry_timeout_sec_override": entry_timeout_override,
        "maker_price_ticks_override": maker_price_ticks_override,
        "long_score": round(long_score, 4),
        "short_score": round(short_score, 4),
    }


def _candidate_market_payload(
    *,
    funding: dict[str, Any],
    oi: dict[str, Any],
    stream_market: dict[str, Any] | None,
    client: BinancePublicFuturesClient,
    symbol: str,
    interval: str,
    order_notional_usdt: float,
) -> dict[str, Any]:
    if stream_market:
        depth = stream_market["depth"]
        price_change_5m_pct = stream_market.get("price_change_5m_pct")
        price_change_15m_pct = stream_market.get("price_change_15m_pct")
        price_change_1h_pct = stream_market.get("price_change_1h_pct")
        kline_cache = list(stream_market.get("kline_cache") or [])
        market_data_source = "ws"
    else:
        depth = sample_depth(client, symbol=symbol, order_notional_usdt=order_notional_usdt)
        kline_cache = _fetch_recent_klines(client, symbol, interval, limit=60)
        price_change_5m_pct, price_change_15m_pct, price_change_1h_pct = _price_changes_from_cache(kline_cache)
        market_data_source = "rest"
    return {
        **funding,
        **oi,
        **depth,
        "depth": depth,
        "market_data_source": market_data_source,
        "market_stream_generated_at": (stream_market or {}).get("generated_at"),
        "market_stream_last_event_at": (stream_market or {}).get("last_event_at"),
        "last_trade_price": ((stream_market or {}).get("trade") or {}).get("price"),
        "last_trade_quantity": ((stream_market or {}).get("trade") or {}).get("quantity"),
        "last_trade_is_buyer_maker": ((stream_market or {}).get("trade") or {}).get("is_buyer_maker"),
        "best_bid_price": (stream_market or {}).get("best_bid_price"),
        "best_ask_price": (stream_market or {}).get("best_ask_price"),
        "mid_price": (stream_market or {}).get("mid_price"),
        "microprice": (stream_market or {}).get("microprice"),
        "microprice_bias_bps": (stream_market or {}).get("microprice_bias_bps"),
        "ofi_latest": (stream_market or {}).get("ofi_latest"),
        "ofi_window_sum": (stream_market or {}).get("ofi_window_sum"),
        "ofi_window_mean": (stream_market or {}).get("ofi_window_mean"),
        "price_change_5m_pct": price_change_5m_pct,
        "price_change_15m_pct": price_change_15m_pct,
        "price_change_1h_pct": price_change_1h_pct,
        "kline_cache": kline_cache,
    }


def _build_snapshot() -> dict[str, Any]:
    ensure_runtime_dirs()
    candidate = load_runtime_candidate_config()
    client = BinancePublicFuturesClient()
    execution_mode = get_execution_mode()
    quote_allocation_usdt = 100.0
    order_notional_usdt = quote_allocation_usdt
    if execution_mode in {"demo_auto", "testnet_auto"}:
        demo_account = read_runtime_state("demo_account") or {}
        virtual_equity = float(demo_account.get("equity_usdt") or get_demo_start_equity_usdt())
        quote_allocation_usdt = round(virtual_equity * get_demo_position_fraction(), 4)
        order_notional_usdt = quote_allocation_usdt * get_demo_leverage()
    symbols = _candidate_symbols(candidate)
    interval = str(candidate.get("interval") or "5m")
    candidates: list[dict[str, Any]] = []
    for symbol in symbols:
        funding = fetch_current_funding_context(client, symbol=symbol)
        oi = fetch_current_oi_context(client, symbol=symbol, period=interval)
        stream_market = _load_symbol_market_stream(symbol, order_notional_usdt)
        market = _candidate_market_payload(
            funding=funding,
            oi=oi,
            stream_market=stream_market,
            client=client,
            symbol=symbol,
            interval=interval,
            order_notional_usdt=order_notional_usdt,
        )
        signal = _simple_signal(symbol=symbol, candidate=candidate, funding=funding, oi=oi, market=market)
        candidates.append(
            {
                "symbol": symbol,
                "market": market,
                "signal": signal,
            }
        )
    candidates.sort(
        key=lambda item: (
            1 if item["signal"].get("gates_passed") else 0,
            float(item["signal"].get("directional_confidence") or 0.0),
            float(item["signal"].get("execution_quality_score") or 0.0),
        ),
        reverse=True,
    )
    selected = candidates[0]
    summary_environment = environment_summary()
    summary_environment["symbol"] = selected["symbol"]
    summary_environment["symbols"] = symbols
    snapshot = {
        "generated_at": _utc_now(),
        "environment": summary_environment,
        "strategy": {
            "engine_mode": candidate.get("engine_mode") or "v3_simple",
            "entry_style": candidate.get("entry_style") or ("router" if str(candidate.get("engine_mode") or "").strip().lower() == "v6_router" else "breakout_flow"),
            "interval": interval,
            "symbols": symbols,
            "threshold": candidate.get("threshold"),
            "maker_first": bool(candidate.get("maker_first", False)),
            "allow_long": candidate.get("allow_long", True),
            "allow_short": candidate.get("allow_short", True),
            "ema_fast_bars": candidate.get("ema_fast_bars"),
            "ema_slow_bars": candidate.get("ema_slow_bars"),
            "breakout_lookback_bars": candidate.get("breakout_lookback_bars"),
        },
        "risk": {
            "default_leverage": get_demo_leverage() if execution_mode in {"demo_auto", "testnet_auto"} else candidate.get("leverage"),
            "fixed_quote_allocation_usdt": quote_allocation_usdt,
            "position_fraction": get_demo_position_fraction() if execution_mode in {"demo_auto", "testnet_auto"} else candidate.get("position_fraction"),
        },
        "market": {
            key: value
            for key, value in selected["market"].items()
            if key != "kline_cache"
        },
        "signal": {
            **{k: v for k, v in selected["signal"].items() if k != "symbol"},
            "recommended_quote_allocation_usdt": quote_allocation_usdt,
        },
        "candidates": [
            {
                "symbol": item["symbol"],
                "bias": item["signal"].get("bias"),
                "entry_side": item["signal"].get("entry_side"),
                "directional_confidence": item["signal"].get("directional_confidence"),
                "execution_quality_score": item["signal"].get("execution_quality_score"),
                "gates_passed": item["signal"].get("gates_passed"),
                "gate_reasons": item["signal"].get("gate_reasons"),
                "spread_bps": item["market"].get("spread_bps"),
                "estimated_slippage_bps": (
                    item["market"].get("estimated_slippage_bps_buy")
                    if item["signal"].get("bias") != "SHORT"
                    else item["market"].get("estimated_slippage_bps_sell")
                ),
                "price_change_5m_pct": item["market"].get("price_change_5m_pct"),
                "price_change_1h_pct": item["market"].get("price_change_1h_pct"),
                "taker_buy_ratio_5m": item["market"].get("taker_buy_ratio_5m"),
                "aggressive_flow_delta": item["market"].get("aggressive_flow_delta"),
                "depth_imbalance": item["market"].get("depth_imbalance"),
                "microprice_bias_bps": item["market"].get("microprice_bias_bps"),
                "ofi_latest": item["market"].get("ofi_latest"),
                "ofi_window_sum": item["market"].get("ofi_window_sum"),
                "pullback_reclaim_long": item["signal"].get("pullback_reclaim_long"),
                "pullback_reclaim_short": item["signal"].get("pullback_reclaim_short"),
                "impulse_long_setup": item["signal"].get("impulse_long_setup"),
                "impulse_short_setup": item["signal"].get("impulse_short_setup"),
                "mean_reversion_long_setup": item["signal"].get("mean_reversion_long_setup"),
                "mean_reversion_short_setup": item["signal"].get("mean_reversion_short_setup"),
                "sub_strategy": item["signal"].get("sub_strategy"),
                "execution_style": item["signal"].get("execution_style"),
                "ema_fast": item["signal"].get("ema_fast"),
                "ema_slow": item["signal"].get("ema_slow"),
                "long_score": item["signal"].get("long_score"),
                "short_score": item["signal"].get("short_score"),
            }
            for item in candidates
        ],
    }
    return snapshot


def run_once() -> dict[str, Any]:
    snapshot = _build_snapshot()
    update_runtime_state("engine_snapshot", snapshot)
    RESEARCH_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    micro_path = RESEARCH_RUNTIME_DIR / "market_micro_snapshots.jsonl"
    micro_packet = {
        "generated_at": snapshot["generated_at"],
        "symbol": snapshot["environment"]["symbol"],
        "candidates": snapshot.get("candidates") or [],
        "bias": snapshot["signal"]["bias"],
        "regime": snapshot["signal"]["regime"],
        "momentum_score": snapshot["signal"].get("ema_gap_pct"),
        "microstructure_score": snapshot["signal"].get("flow_bias_score"),
        "spread_bps": snapshot["market"]["spread_bps"],
        "depth_imbalance": snapshot["market"]["depth_imbalance"],
        "taker_buy_ratio_5m": snapshot["market"]["taker_buy_ratio_5m"],
        "aggressive_flow_delta": snapshot["market"]["aggressive_flow_delta"],
        "microprice_bias_bps": snapshot["market"].get("microprice_bias_bps"),
        "ofi_window_sum": snapshot["market"].get("ofi_window_sum"),
        "price_change_5m_pct": snapshot["market"]["price_change_5m_pct"],
        "price_change_1h_pct": snapshot["market"]["price_change_1h_pct"],
    }
    with micro_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(micro_packet, ensure_ascii=False) + "\n")
    heartbeat(
        "engine",
        status="running",
        details={
            "symbol": snapshot["environment"]["symbol"],
            "candidates": len(snapshot.get("candidates") or []),
            "bias": snapshot["signal"]["bias"],
            "confidence": snapshot["signal"]["directional_confidence"],
            "gates_passed": snapshot["signal"]["gates_passed"],
        },
    )
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Leiting v3-simple engine service.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-sec", type=int, default=30)
    args = parser.parse_args()
    while True:
        snapshot = run_once()
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        if args.once:
            break
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    main()
