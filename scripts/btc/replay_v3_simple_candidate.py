#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay v3_simple candidate over historical 5m data.")
    parser.add_argument("--candidate", required=True, help="Path to candidate JSON")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--max-hold-bars", type=int, default=12)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _ema(values: list[float], period: int) -> float | None:
    if period <= 0 or len(values) < period:
        return None
    multiplier = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for value in values[period:]:
        ema = (value - ema) * multiplier + ema
    return ema


def _round6(value: float) -> float:
    return round(value, 6)


def _signal(
    rows: list[dict[str, Any]],
    idx: int,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    closes = [float(item["close"]) for item in rows[: idx + 1]]
    highs = [float(item["high"]) for item in rows[: idx + 1]]
    lows = [float(item["low"]) for item in rows[: idx + 1]]
    row = rows[idx]
    latest_close = closes[-1]
    ema_fast_bars = int(candidate.get("ema_fast_bars") or 9)
    ema_slow_bars = int(candidate.get("ema_slow_bars") or 21)
    lookback = int(candidate.get("breakout_lookback_bars") or 12)
    ema_fast = _ema(closes, ema_fast_bars) or latest_close
    ema_slow = _ema(closes, ema_slow_bars) or latest_close
    prev_high = max(highs[-1 - lookback : -1]) if len(highs) > lookback else latest_close
    prev_low = min(lows[-1 - lookback : -1]) if len(lows) > lookback else latest_close
    taker_ratio = float(row.get("buySellRatio") or 1.0)
    buy_vol = float(row.get("buyVol") or 0.0)
    sell_vol = float(row.get("sellVol") or 0.0)
    aggressive_flow_delta = buy_vol - sell_vol
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
    require_breakout_confirmation = bool(candidate.get("require_breakout_confirmation", False))
    require_flow_confirmation = bool(candidate.get("require_flow_confirmation", False))

    flow_long_setup = trend_up and taker_long_ok and aggressive_long_ok
    flow_short_setup = trend_down and taker_short_ok and aggressive_short_ok
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
    if long_setup:
        long_score += 0.12 + min(0.13, max(0.0, ema_gap_pct) * 8.0)
        if breakout_up:
            long_score += 0.18
        if flow_long_setup:
            long_score += 0.12 + min(0.13, max(0.0, taker_ratio - 1.0) * 4.0 + max(0.0, aggressive_flow_delta) / 2000.0)
    if short_setup:
        short_score += 0.12 + min(0.13, max(0.0, -ema_gap_pct) * 8.0)
        if breakout_down:
            short_score += 0.18
        if flow_short_setup:
            short_score += 0.12 + min(0.13, max(0.0, 1.0 - taker_ratio) * 4.0 + max(0.0, -aggressive_flow_delta) / 2000.0)

    threshold = float(candidate.get("threshold") or 0.25)
    bias = "NONE"
    confidence = 0.0
    if candidate.get("allow_long", True) and long_score >= threshold and long_score > short_score:
        bias = "LONG"
        confidence = long_score
    elif candidate.get("allow_short", True) and short_score >= threshold and short_score > long_score:
        bias = "SHORT"
        confidence = short_score
    return {
        "bias": bias,
        "confidence": _round6(confidence),
        "ema_gap_pct": _round6(ema_gap_pct),
        "breakout_up": breakout_up,
        "breakout_down": breakout_down,
        "flow_long_setup": flow_long_setup,
        "flow_short_setup": flow_short_setup,
        "taker_buy_ratio_5m": _round6(taker_ratio),
        "aggressive_flow_delta": _round6(aggressive_flow_delta),
    }


def _simulate_trade(
    rows: list[dict[str, Any]],
    start_idx: int,
    side: str,
    candidate: dict[str, Any],
    max_hold_bars: int,
) -> tuple[dict[str, Any], int] | None:
    if start_idx + 1 >= len(rows):
        return None
    entry_bar = rows[start_idx + 1]
    entry_price = float(entry_bar["open"])
    leverage = float(candidate.get("leverage") or 100.0)
    fee_bps_per_side = float(candidate.get("estimated_fee_bps_per_side") or 5.0)
    fee_roi_pct = 2.0 * fee_bps_per_side * leverage / 100.0
    take_profit_net = float(candidate.get("take_profit_net_roi_pct") or 26.0)
    stop_net = float(candidate.get("hard_stop_net_roi_pct") or 12.0)
    gross_take_roi_pct = take_profit_net + fee_roi_pct
    gross_stop_roi_pct = max(0.25, stop_net - fee_roi_pct)
    take_profit_pct = gross_take_roi_pct / leverage
    stop_loss_pct = gross_stop_roi_pct / leverage

    if side == "LONG":
        take_price = entry_price * (1.0 + take_profit_pct / 100.0)
        stop_price = entry_price * (1.0 - stop_loss_pct / 100.0)
    else:
        take_price = entry_price * (1.0 - take_profit_pct / 100.0)
        stop_price = entry_price * (1.0 + stop_loss_pct / 100.0)

    end_idx = min(len(rows) - 1, start_idx + max_hold_bars)
    exit_idx = end_idx
    exit_price = float(rows[end_idx]["close"])
    exit_reason = "timeout"
    for idx in range(start_idx + 1, end_idx + 1):
        high = float(rows[idx]["high"])
        low = float(rows[idx]["low"])
        close = float(rows[idx]["close"])
        if side == "LONG":
            if low <= stop_price:
                exit_price = stop_price
                exit_reason = "stop"
                exit_idx = idx
                break
            if high >= take_price:
                exit_price = take_price
                exit_reason = "take_profit"
                exit_idx = idx
                break
        else:
            if high >= stop_price:
                exit_price = stop_price
                exit_reason = "stop"
                exit_idx = idx
                break
            if low <= take_price:
                exit_price = take_price
                exit_reason = "take_profit"
                exit_idx = idx
                break
        if idx == end_idx:
            exit_price = close

    gross_roi_pct = ((exit_price - entry_price) / entry_price) * leverage * 100.0 if side == "LONG" else ((entry_price - exit_price) / entry_price) * leverage * 100.0
    net_roi_pct = gross_roi_pct - fee_roi_pct
    return ({
        "entry_time": entry_bar["close_time_iso"],
        "exit_time": rows[exit_idx]["close_time_iso"],
        "side": side,
        "entry_price": _round6(entry_price),
        "exit_price": _round6(exit_price),
        "gross_roi_pct": _round6(gross_roi_pct),
        "net_roi_pct": _round6(net_roi_pct),
        "exit_reason": exit_reason,
        "hold_bars": exit_idx - (start_idx + 1) + 1,
    }, exit_idx)


def main() -> None:
    args = parse_args()
    candidate_path = Path(args.candidate)
    candidate = _load_json(candidate_path)
    raw_dir = Path("/opt/leiting-btc/btc_data/raw") / args.symbol / "5m"
    klines = _read_csv(raw_dir / "klines.csv")
    taker = _read_csv(raw_dir / "taker_buy_sell_ratio_5m.csv")
    taker_by_ts = {str(row["timestamp"]): row for row in taker}
    merged: list[dict[str, Any]] = []
    for row in klines:
        close_ms = str(row.get("close_time_ms") or row.get("close_time") or "")
        ts = close_ms if close_ms in taker_by_ts else str(row.get("open_time_ms") or row.get("open_time") or "")
        extra = taker_by_ts.get(ts, {})
        merged.append({**row, **extra})
    if args.days > 0:
        bars = args.days * 24 * 12
        merged = merged[-bars:]

    trades: list[dict[str, Any]] = []
    idx = max(int(candidate.get("ema_slow_bars") or 21), int(candidate.get("breakout_lookback_bars") or 20)) + 1
    while idx < len(merged) - 1:
        signal = _signal(merged, idx, candidate)
        if signal["bias"] == "NONE":
            idx += 1
            continue
        trade = _simulate_trade(merged, idx, "LONG" if signal["bias"] == "LONG" else "SHORT", candidate, args.max_hold_bars)
        if trade is None:
            break
        payload, exit_idx = trade
        payload["signal"] = signal
        trades.append(payload)
        idx = exit_idx + 1

    net = sum(float(item["net_roi_pct"]) for item in trades)
    wins = sum(1 for item in trades if float(item["net_roi_pct"]) > 0)
    loss_reasons = Counter(item["exit_reason"] for item in trades)
    side_counter = Counter(item["side"] for item in trades)
    result = {
        "candidate": candidate.get("name"),
        "symbol": args.symbol,
        "days": args.days,
        "trade_count": len(trades),
        "win_rate": round((wins / len(trades)), 4) if trades else 0.0,
        "avg_net_roi_pct": round(mean([float(item["net_roi_pct"]) for item in trades]), 6) if trades else 0.0,
        "total_net_roi_pct": round(net, 6),
        "by_exit_reason": dict(loss_reasons),
        "by_side": dict(side_counter),
        "first_trades": trades[:10],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
