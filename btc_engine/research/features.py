"""Feature engineering for BTC 5m training datasets."""

from __future__ import annotations
from statistics import mean, pstdev
from typing import Any


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct_change(current: float | None, previous: float | None) -> float | None:
    if current in (None, 0.0) or previous in (None, 0.0):
        return None
    return ((current - previous) / previous) * 100.0


def _rolling_mean(values: list[float], window: int) -> float | None:
    if len(values) < window or window <= 0:
        return None
    return mean(values[-window:])


def _rolling_std(values: list[float], window: int) -> float | None:
    if len(values) < window or window <= 1:
        return None
    return pstdev(values[-window:])


def _clip(value: float | None, low: float, high: float) -> float | None:
    if value is None:
        return None
    return max(low, min(high, value))


def _update_ema(current: float, previous: float | None, period: int) -> float:
    alpha = 2.0 / (period + 1.0)
    if previous is None:
        return current
    return previous + alpha * (current - previous)


def engineer_feature_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return per-bar research feature rows using only information available up to that bar."""
    feature_rows: list[dict[str, Any]] = []
    closes: list[float] = []
    quote_volumes: list[float] = []
    oi_values: list[float] = []
    premium_values: list[float] = []
    true_ranges: list[float] = []
    gains: list[float] = []
    losses: list[float] = []
    ema_12: float | None = None
    ema_24: float | None = None
    ema_26: float | None = None
    ema_48: float | None = None
    macd_signal: float | None = None
    current_session: str | None = None
    session_pv = 0.0
    session_volume = 0.0

    for idx, row in enumerate(rows):
        close = _safe_float(row.get("close"))
        open_ = _safe_float(row.get("open"))
        high = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        volume = _safe_float(row.get("volume"))
        quote_volume = _safe_float(row.get("quote_asset_volume"))
        taker_buy_quote = _safe_float(row.get("taker_buy_quote_volume"))
        premium_close = _safe_float(row.get("premium_close"))
        funding_rate = _safe_float(row.get("funding_rate"))
        oi = _safe_float(row.get("sum_open_interest"))
        oi_value = _safe_float(row.get("sum_open_interest_value"))
        oi_age_bars = _safe_float(row.get("oi_age_bars"))
        has_oi_context = _safe_float(row.get("has_oi_context"))
        taker_buy_sell_ratio = _safe_float(row.get("buy_sell_ratio"))
        taker_buy_vol = _safe_float(row.get("buy_vol"))
        taker_sell_vol = _safe_float(row.get("sell_vol"))
        taker_age_bars = _safe_float(row.get("taker_age_bars"))
        has_taker_context = _safe_float(row.get("has_taker_context"))
        has_micro_context = _safe_float(row.get("has_micro_context"))
        micro_last_return_1m_pct = _safe_float(row.get("micro_last_return_1m_pct"))
        micro_mean_range_1m_pct = _safe_float(row.get("micro_mean_range_1m_pct"))
        micro_last_imbalance_1m = _safe_float(row.get("micro_last_imbalance_1m"))
        micro_mean_imbalance_1m = _safe_float(row.get("micro_mean_imbalance_1m"))
        micro_std_imbalance_1m = _safe_float(row.get("micro_std_imbalance_1m"))
        micro_buy_share_5m = _safe_float(row.get("micro_buy_share_5m"))
        micro_window_imbalance_5m = _safe_float(row.get("micro_window_imbalance_5m"))
        global_long_short_ratio = _safe_float(row.get("global_long_short_ratio"))
        global_long_account = _safe_float(row.get("global_long_account"))
        global_short_account = _safe_float(row.get("global_short_account"))
        global_ratio_age_bars = _safe_float(row.get("global_ratio_age_bars"))
        has_global_ratio_context = _safe_float(row.get("has_global_ratio_context"))
        runtime_snapshot_count = _safe_float(row.get("runtime_snapshot_count"))
        runtime_spread_bps_mean = _safe_float(row.get("runtime_spread_bps_mean"))
        runtime_spread_bps_max = _safe_float(row.get("runtime_spread_bps_max"))
        runtime_depth_imbalance_last = _safe_float(row.get("runtime_depth_imbalance_last"))
        runtime_depth_imbalance_mean = _safe_float(row.get("runtime_depth_imbalance_mean"))
        runtime_depth_imbalance_std = _safe_float(row.get("runtime_depth_imbalance_std"))
        runtime_slippage_bps_buy_mean = _safe_float(row.get("runtime_slippage_bps_buy_mean"))
        runtime_slippage_bps_sell_mean = _safe_float(row.get("runtime_slippage_bps_sell_mean"))
        runtime_execution_quality_mean = _safe_float(row.get("runtime_execution_quality_mean"))
        runtime_microstructure_score_mean = _safe_float(row.get("runtime_microstructure_score_mean"))
        runtime_taker_buy_ratio_mean = _safe_float(row.get("runtime_taker_buy_ratio_mean"))
        has_runtime_micro_context = _safe_float(row.get("has_runtime_micro_context"))

        if close is None or open_ is None or high is None or low is None or quote_volume is None:
            continue

        closes.append(close)
        quote_volumes.append(quote_volume)
        oi_values.append(oi_value or 0.0)
        premium_values.append(premium_close or 0.0)

        prev_close = closes[-2] if len(closes) >= 2 else None
        true_range = max(
            high - low,
            abs(high - prev_close) if prev_close is not None else high - low,
            abs(low - prev_close) if prev_close is not None else high - low,
        )
        true_ranges.append(true_range)
        change = close - prev_close if prev_close is not None else 0.0
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))

        raw_return_1 = _pct_change(close, closes[-2] if len(closes) >= 2 else None)
        raw_return_3 = _pct_change(close, closes[-4] if len(closes) >= 4 else None)
        raw_return_6 = _pct_change(close, closes[-7] if len(closes) >= 7 else None)
        raw_return_12 = _pct_change(close, closes[-13] if len(closes) >= 13 else None)

        volume_ma_12 = _rolling_mean(quote_volumes, 12)
        volume_ma_36 = _rolling_mean(quote_volumes, 36)
        close_std_12 = _rolling_std(closes, 12)
        close_std_36 = _rolling_std(closes, 36)
        oi_ma_12 = _rolling_mean(oi_values, 12)
        premium_ma_12 = _rolling_mean(premium_values, 12)

        candle_range_pct = ((high - low) / close) * 100.0 if close else 0.0
        candle_body_pct = ((close - open_) / open_) * 100.0 if open_ else 0.0
        upper_wick_pct = ((high - max(open_, close)) / close) * 100.0 if close else 0.0
        lower_wick_pct = ((min(open_, close) - low) / close) * 100.0 if close else 0.0

        ema_12 = _update_ema(close, ema_12, 12)
        ema_24 = _update_ema(close, ema_24, 24)
        ema_26 = _update_ema(close, ema_26, 26)
        ema_48 = _update_ema(close, ema_48, 48)
        macd_line = (ema_12 - ema_26) if ema_12 is not None and ema_26 is not None else None
        if macd_line is not None:
            macd_signal = _update_ema(macd_line, macd_signal, 9)
        macd_hist = (macd_line - macd_signal) if macd_line is not None and macd_signal is not None else None

        atr_14 = _rolling_mean(true_ranges, 14)
        avg_gain_14 = _rolling_mean(gains, 14)
        avg_loss_14 = _rolling_mean(losses, 14)
        if avg_gain_14 is None or avg_loss_14 is None:
            rsi_14 = None
        elif avg_loss_14 == 0:
            rsi_14 = 100.0
        else:
            rs = avg_gain_14 / avg_loss_14
            rsi_14 = 100.0 - (100.0 / (1.0 + rs))

        bb_mid_20 = _rolling_mean(closes, 20)
        bb_std_20 = _rolling_std(closes, 20)
        if bb_mid_20 is not None and bb_std_20 is not None and bb_mid_20 != 0:
            bb_upper_20 = bb_mid_20 + (2.0 * bb_std_20)
            bb_lower_20 = bb_mid_20 - (2.0 * bb_std_20)
            bollinger_width_20_pct = ((bb_upper_20 - bb_lower_20) / bb_mid_20) * 100.0
            bollinger_zscore_20 = (close - bb_mid_20) / bb_std_20 if bb_std_20 else 0.0
        else:
            bollinger_width_20_pct = None
            bollinger_zscore_20 = None

        session_key = str(row.get("open_time_iso", ""))[:10]
        if session_key != current_session:
            current_session = session_key
            session_pv = 0.0
            session_volume = 0.0
        typical_price = (high + low + close) / 3.0
        session_pv += typical_price * (volume or 0.0)
        session_volume += volume or 0.0
        session_vwap = (session_pv / session_volume) if session_volume else None
        vwap_session_deviation_pct = ((close - session_vwap) / session_vwap) * 100.0 if session_vwap not in (None, 0.0) else None

        taker_flow_imbalance = None
        if taker_buy_vol is not None and taker_sell_vol is not None:
            total_flow = taker_buy_vol + taker_sell_vol
            if total_flow:
                taker_flow_imbalance = (taker_buy_vol - taker_sell_vol) / total_flow

        feature_rows.append(
            {
                "row_index": idx,
                "open_time": row["open_time"],
                "open_time_iso": row["open_time_iso"],
                "close_time": row["close_time"],
                "close_time_iso": row["close_time_iso"],
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume or 0.0,
                "quote_asset_volume": quote_volume,
                "taker_buy_quote_volume": taker_buy_quote or 0.0,
                "return_1_pct": raw_return_1,
                "return_3_pct": raw_return_3,
                "return_6_pct": raw_return_6,
                "return_12_pct": raw_return_12,
                "candle_range_pct": candle_range_pct,
                "candle_body_pct": candle_body_pct,
                "upper_wick_pct": upper_wick_pct,
                "lower_wick_pct": lower_wick_pct,
                "volume_ratio_12": (quote_volume / volume_ma_12) if volume_ma_12 else None,
                "volume_ratio_36": (quote_volume / volume_ma_36) if volume_ma_36 else None,
                "realized_vol_12": ((close_std_12 / close) * 100.0) if close and close_std_12 is not None else None,
                "realized_vol_36": ((close_std_36 / close) * 100.0) if close and close_std_36 is not None else None,
                "ema_12_gap_pct": ((close - ema_12) / ema_12 * 100.0) if ema_12 not in (None, 0.0) else None,
                "ema_24_gap_pct": ((close - ema_24) / ema_24 * 100.0) if ema_24 not in (None, 0.0) else None,
                "ema_48_gap_pct": ((close - ema_48) / ema_48 * 100.0) if ema_48 not in (None, 0.0) else None,
                "ema_cross_12_24_pct": ((ema_12 - ema_24) / ema_24 * 100.0) if ema_12 is not None and ema_24 not in (None, 0.0) else None,
                "ema_cross_24_48_pct": ((ema_24 - ema_48) / ema_48 * 100.0) if ema_24 is not None and ema_48 not in (None, 0.0) else None,
                "atr_14_pct": ((atr_14 / close) * 100.0) if close and atr_14 is not None else None,
                "rsi_14": rsi_14,
                "bollinger_width_20_pct": bollinger_width_20_pct,
                "bollinger_zscore_20": bollinger_zscore_20,
                "vwap_session_deviation_pct": vwap_session_deviation_pct,
                "macd_line_pct": ((macd_line / close) * 100.0) if close and macd_line is not None else None,
                "macd_signal_pct": ((macd_signal / close) * 100.0) if close and macd_signal is not None else None,
                "macd_hist_pct": ((macd_hist / close) * 100.0) if close and macd_hist is not None else None,
                "funding_rate": funding_rate,
                "funding_rate_bps": (funding_rate * 10_000.0) if funding_rate is not None else None,
                "premium_close": premium_close,
                "premium_change_3_pct": _pct_change(premium_close, premium_values[-4] if len(premium_values) >= 4 else None),
                "premium_deviation_pct": ((premium_close - premium_ma_12) / abs(premium_ma_12) * 100.0) if premium_close is not None and premium_ma_12 not in (None, 0.0) else None,
                "sum_open_interest": oi,
                "sum_open_interest_value": oi_value,
                "oi_age_bars": oi_age_bars,
                "has_oi_context": has_oi_context,
                "oi_change_1_pct": _pct_change(oi_value, oi_values[-2] if len(oi_values) >= 2 else None),
                "oi_change_3_pct": _pct_change(oi_value, oi_values[-4] if len(oi_values) >= 4 else None),
                "oi_change_12_pct": _pct_change(oi_value, oi_values[-13] if len(oi_values) >= 13 else None),
                "oi_deviation_pct": ((oi_value - oi_ma_12) / oi_ma_12 * 100.0) if oi_value is not None and oi_ma_12 not in (None, 0.0) else None,
                "buy_sell_ratio": taker_buy_sell_ratio,
                "buy_sell_pressure": ((taker_buy_sell_ratio - 1.0) if taker_buy_sell_ratio is not None else None),
                "taker_buy_vol": taker_buy_vol,
                "taker_sell_vol": taker_sell_vol,
                "taker_age_bars": taker_age_bars,
                "has_taker_context": has_taker_context,
                "taker_flow_imbalance": taker_flow_imbalance,
                "has_micro_context": has_micro_context,
                "micro_last_return_1m_pct": micro_last_return_1m_pct,
                "micro_mean_range_1m_pct": micro_mean_range_1m_pct,
                "micro_last_imbalance_1m": micro_last_imbalance_1m,
                "micro_mean_imbalance_1m": micro_mean_imbalance_1m,
                "micro_std_imbalance_1m": micro_std_imbalance_1m,
                "micro_buy_share_5m": micro_buy_share_5m,
                "micro_window_imbalance_5m": micro_window_imbalance_5m,
                "global_long_short_ratio": global_long_short_ratio,
                "global_long_account": global_long_account,
                "global_short_account": global_short_account,
                "global_ratio_age_bars": global_ratio_age_bars,
                "has_global_ratio_context": has_global_ratio_context,
                "global_account_skew": (
                    (global_long_account - global_short_account)
                    if global_long_account is not None and global_short_account is not None
                    else None
                ),
                "global_buy_crowding": ((global_long_short_ratio - 1.0) if global_long_short_ratio is not None else None),
                "runtime_snapshot_count": runtime_snapshot_count,
                "runtime_spread_bps_mean": runtime_spread_bps_mean,
                "runtime_spread_bps_max": runtime_spread_bps_max,
                "runtime_depth_imbalance_last": runtime_depth_imbalance_last,
                "runtime_depth_imbalance_mean": runtime_depth_imbalance_mean,
                "runtime_depth_imbalance_std": runtime_depth_imbalance_std,
                "runtime_slippage_bps_buy_mean": runtime_slippage_bps_buy_mean,
                "runtime_slippage_bps_sell_mean": runtime_slippage_bps_sell_mean,
                "runtime_execution_quality_mean": runtime_execution_quality_mean,
                "runtime_microstructure_score_mean": runtime_microstructure_score_mean,
                "runtime_taker_buy_ratio_mean": runtime_taker_buy_ratio_mean,
                "has_runtime_micro_context": has_runtime_micro_context,
            }
        )

    return feature_rows


def baseline_signal_score(row: dict[str, Any]) -> float:
    """A pure rules baseline score used before model training is available."""
    volume_ratio_12 = (_clip(_safe_float(row.get("volume_ratio_12")), 0.0, 3.0) or 0.0) - 1.0
    rsi_centered = ((_safe_float(row.get("rsi_14")) or 50.0) - 50.0) / 25.0
    oi_age_penalty = (_clip(_safe_float(row.get("oi_age_bars")), 0.0, 48.0) or 48.0) / 48.0
    taker_age_penalty = (_clip(_safe_float(row.get("taker_age_bars")), 0.0, 48.0) or 48.0) / 48.0
    global_ratio_age_penalty = (_clip(_safe_float(row.get("global_ratio_age_bars")), 0.0, 48.0) or 48.0) / 48.0
    components = [
        (_clip(_safe_float(row.get("return_3_pct")), -1.5, 1.5) or 0.0) * 0.24,
        (_clip(_safe_float(row.get("return_12_pct")), -3.0, 3.0) or 0.0) * 0.18,
        (_clip(_safe_float(row.get("oi_change_3_pct")), -6.0, 6.0) or 0.0) * 0.16,
        (_clip(_safe_float(row.get("buy_sell_pressure")), -1.0, 1.0) or 0.0) * 0.14,
        (_clip(_safe_float(row.get("taker_flow_imbalance")), -1.0, 1.0) or 0.0) * 0.12,
        (_clip(_safe_float(row.get("premium_deviation_pct")), -0.8, 0.8) or 0.0) * 0.08,
        (_clip(_safe_float(row.get("ema_cross_12_24_pct")), -1.0, 1.0) or 0.0) * 0.08,
        (_clip(_safe_float(row.get("macd_hist_pct")), -0.25, 0.25) or 0.0) * 0.18,
        (_clip(_safe_float(row.get("vwap_session_deviation_pct")), -1.0, 1.0) or 0.0) * 0.07,
        (_clip(_safe_float(row.get("bollinger_zscore_20")), -2.0, 2.0) or 0.0) * 0.04,
        (_clip(_safe_float(row.get("micro_last_return_1m_pct")), -0.4, 0.4) or 0.0) * 0.14,
        (_clip(_safe_float(row.get("micro_mean_imbalance_1m")), -1.0, 1.0) or 0.0) * 0.1,
        (_clip(_safe_float(row.get("micro_window_imbalance_5m")), -1.0, 1.0) or 0.0) * 0.1,
        (_clip(_safe_float(row.get("global_buy_crowding")), -0.5, 0.5) or 0.0) * -0.05,
        (_clip(_safe_float(row.get("global_account_skew")), -0.2, 0.2) or 0.0) * -0.04,
        _clip(rsi_centered, -1.0, 1.0) * 0.06,
        (_clip(_safe_float(row.get("funding_rate_bps")), -8.0, 8.0) or 0.0) * -0.04,
        (_clip(_safe_float(row.get("atr_14_pct")), 0.0, 1.5) or 0.0) * -0.03,
        (_clip(_safe_float(row.get("candle_body_pct")), -1.0, 1.0) or 0.0) * 0.04,
        volume_ratio_12 * 0.04,
        ((_safe_float(row.get("has_oi_context")) or 0.0) - 0.5) * 0.04,
        ((_safe_float(row.get("has_taker_context")) or 0.0) - 0.5) * 0.04,
        ((_safe_float(row.get("has_micro_context")) or 0.0) - 0.5) * 0.05,
        ((_safe_float(row.get("has_global_ratio_context")) or 0.0) - 0.5) * 0.03,
        ((_safe_float(row.get("has_runtime_micro_context")) or 0.0) - 0.5) * 0.05,
        oi_age_penalty * -0.03,
        taker_age_penalty * -0.03,
        global_ratio_age_penalty * -0.02,
        (_clip(_safe_float(row.get("runtime_depth_imbalance_mean")), -1.0, 1.0) or 0.0) * 0.08,
        (_clip(_safe_float(row.get("runtime_microstructure_score_mean")), -2.0, 2.0) or 0.0) * 0.08,
        (((_clip(_safe_float(row.get("runtime_taker_buy_ratio_mean")), 0.0, 1.0) or 0.5) - 0.5) * 0.04),
        (_clip(_safe_float(row.get("runtime_execution_quality_mean")), 0.0, 10.0) or 0.0) * 0.03,
        (_clip(_safe_float(row.get("runtime_spread_bps_mean")), 0.0, 10.0) or 0.0) * -0.02,
        (_clip(_safe_float(row.get("runtime_slippage_bps_buy_mean")), 0.0, 20.0) or 0.0) * -0.02,
    ]
    return round(sum(components), 6)
