"""First-version BTC backtest framework for Leiting."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

from btc_engine.config import BACKTESTS_DIR, DATASETS_DIR, ensure_runtime_dirs


@dataclass(slots=True)
class BacktestConfig:
    score_field: str = "baseline_signal_score"
    long_threshold: float = 0.35
    short_threshold: float = -0.35
    allow_long: bool = True
    allow_short: bool = True
    regime_field: str = "label_regime"
    regime_thresholds: dict[str, float | dict[str, float]] | None = None
    allowed_regimes: list[str] | None = None
    max_hold_bars: int = 12
    stop_loss_pct: float = 0.35
    breakeven_trigger_pct: float = 0.30
    breakeven_lock_pct: float = 0.05
    trailing_callback_pct: float = 0.25
    fee_bps_per_side: float = 2.0
    slippage_bps_per_side: float = 1.0
    cooldown_bars: int = 6


def _read_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_thresholds(row: dict[str, Any], cfg: BacktestConfig) -> tuple[float, float]:
    long_threshold = cfg.long_threshold
    short_threshold = cfg.short_threshold
    regime_value = str(row.get(cfg.regime_field) or "").strip().lower()
    regime_thresholds = cfg.regime_thresholds or {}
    override = regime_thresholds.get(regime_value)
    if override is None:
        return long_threshold, short_threshold
    if isinstance(override, dict):
        long_threshold = float(override.get("long", long_threshold))
        short_value = float(override.get("short", abs(short_threshold)))
        short_threshold = -abs(short_value)
    else:
        long_threshold = float(override)
        short_threshold = -abs(float(override))
    return long_threshold, short_threshold


def _signal_side(score: float, row: dict[str, Any], cfg: BacktestConfig) -> str | None:
    regime_value = str(row.get(cfg.regime_field) or "").strip().lower()
    if cfg.allowed_regimes:
        normalized = {item.strip().lower() for item in cfg.allowed_regimes if str(item).strip()}
        if regime_value not in normalized:
            return None
    long_threshold, short_threshold = _resolve_thresholds(row, cfg)
    if cfg.allow_long and score >= long_threshold:
        return "LONG"
    if cfg.allow_short and score <= short_threshold:
        return "SHORT"
    return None


def _simulate_trade(rows: list[dict[str, Any]], start_idx: int, side: str, cfg: BacktestConfig) -> tuple[dict[str, Any], int] | None:
    if start_idx + 1 >= len(rows):
        return None
    entry_bar = rows[start_idx + 1]
    entry_price = _to_float(entry_bar["open"]) * (1 + cfg.slippage_bps_per_side / 10_000) if side == "LONG" else _to_float(entry_bar["open"]) * (1 - cfg.slippage_bps_per_side / 10_000)
    stop_price = entry_price * (1 - cfg.stop_loss_pct / 100) if side == "LONG" else entry_price * (1 + cfg.stop_loss_pct / 100)
    breakeven_trigger = entry_price * (1 + cfg.breakeven_trigger_pct / 100) if side == "LONG" else entry_price * (1 - cfg.breakeven_trigger_pct / 100)
    locked_stop = entry_price * (1 + cfg.breakeven_lock_pct / 100) if side == "LONG" else entry_price * (1 - cfg.breakeven_lock_pct / 100)
    phase = "INITIAL_STOP"
    high_watermark = entry_price
    low_watermark = entry_price

    end_idx = min(len(rows) - 1, start_idx + cfg.max_hold_bars)
    exit_price = entry_price
    exit_reason = "timeout"
    exit_idx = end_idx

    for idx in range(start_idx + 1, end_idx + 1):
        bar = rows[idx]
        high = _to_float(bar["high"])
        low = _to_float(bar["low"])
        close = _to_float(bar["close"])

        if side == "LONG":
            if phase == "INITIAL_STOP" and low <= stop_price:
                exit_price = stop_price
                exit_reason = "initial_stop"
                exit_idx = idx
                break
            if phase == "INITIAL_STOP" and high >= breakeven_trigger:
                stop_price = max(stop_price, locked_stop)
                phase = "BREAKEVEN_LOCKED"
                high_watermark = max(high_watermark, high)
            if phase != "INITIAL_STOP":
                high_watermark = max(high_watermark, high)
                trailing_stop = max(stop_price, high_watermark * (1 - cfg.trailing_callback_pct / 100))
                stop_price = trailing_stop
                phase = "TRAILING_ACTIVE"
                if low <= stop_price:
                    exit_price = stop_price
                    exit_reason = "trailing_stop"
                    exit_idx = idx
                    break
        else:
            if phase == "INITIAL_STOP" and high >= stop_price:
                exit_price = stop_price
                exit_reason = "initial_stop"
                exit_idx = idx
                break
            if phase == "INITIAL_STOP" and low <= breakeven_trigger:
                stop_price = min(stop_price, locked_stop)
                phase = "BREAKEVEN_LOCKED"
                low_watermark = min(low_watermark, low)
            if phase != "INITIAL_STOP":
                low_watermark = min(low_watermark, low)
                trailing_stop = min(stop_price, low_watermark * (1 + cfg.trailing_callback_pct / 100))
                stop_price = trailing_stop
                phase = "TRAILING_ACTIVE"
                if high >= stop_price:
                    exit_price = stop_price
                    exit_reason = "trailing_stop"
                    exit_idx = idx
                    break

        if idx == end_idx:
            exit_price = close

    gross_return_pct = ((exit_price - entry_price) / entry_price) * 100.0 if side == "LONG" else ((entry_price - exit_price) / entry_price) * 100.0
    fees_pct = (cfg.fee_bps_per_side * 2) / 100.0
    slippage_pct = (cfg.slippage_bps_per_side * 2) / 100.0
    net_return_pct = gross_return_pct - fees_pct - slippage_pct

    trade = {
        "entry_index": start_idx + 1,
        "exit_index": exit_idx,
        "entry_time": entry_bar["open_time_iso"],
        "exit_time": rows[exit_idx]["close_time_iso"],
        "side": side,
        "entry_regime": row_regime(rows[start_idx], cfg),
        "entry_price": round(entry_price, 6),
        "exit_price": round(exit_price, 6),
        "gross_return_pct": round(gross_return_pct, 6),
        "net_return_pct": round(net_return_pct, 6),
        "exit_reason": exit_reason,
        "hold_bars": exit_idx - (start_idx + 1) + 1,
    }
    return trade, exit_idx


def summarize_backtest_trades(
    trades: list[dict[str, Any]],
    cfg: BacktestConfig | None = None,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    net_returns = [trade["net_return_pct"] for trade in trades]
    wins = [value for value in net_returns if value > 0]
    losses = [value for value in net_returns if value < 0]
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in net_returns:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)

    return {
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "config": asdict(cfg) if cfg else None,
        "trade_count": len(trades),
        "net_return_pct": round(sum(net_returns), 6),
        "avg_trade_pct": round(mean(net_returns), 6) if net_returns else 0.0,
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
        "profit_factor": round((sum(wins) / abs(sum(losses))) if losses else (999.0 if wins else 0.0), 4),
        "max_drawdown_pct": round(max_drawdown, 6),
        "trades": trades,
    }


def summarize_filtered_trades(
    trades: list[dict[str, Any]],
    predicate,
    cfg: BacktestConfig | None = None,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    filtered = [trade for trade in trades if predicate(trade)]
    return summarize_backtest_trades(filtered, cfg, generated_at=generated_at)


def summarize_side_breakdown(
    trades: list[dict[str, Any]],
    cfg: BacktestConfig | None = None,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "long": summarize_filtered_trades(trades, lambda trade: trade.get("side") == "LONG", cfg, generated_at=generated_at),
        "short": summarize_filtered_trades(trades, lambda trade: trade.get("side") == "SHORT", cfg, generated_at=generated_at),
    }


def summarize_regime_breakdown(
    trades: list[dict[str, Any]],
    cfg: BacktestConfig | None = None,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    regimes = sorted({str(trade.get("entry_regime") or "").strip().lower() for trade in trades if trade.get("entry_regime")})
    return {
        regime: summarize_filtered_trades(
            trades,
            lambda trade, regime_value=regime: str(trade.get("entry_regime") or "").strip().lower() == regime_value,
            cfg,
            generated_at=generated_at,
        )
        for regime in regimes
    }


def summarize_trade_views(
    trades: list[dict[str, Any]],
    cfg: BacktestConfig | None = None,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "overall": summarize_backtest_trades(trades, cfg, generated_at=generated_at),
        "by_side": summarize_side_breakdown(trades, cfg, generated_at=generated_at),
        "by_regime": summarize_regime_breakdown(trades, cfg, generated_at=generated_at),
    }


def row_regime(row: dict[str, Any], cfg: BacktestConfig) -> str | None:
    regime = row.get(cfg.regime_field)
    if regime in (None, ""):
        return None
    return str(regime)


def clone_backtest_config(cfg: BacktestConfig, **overrides: Any) -> BacktestConfig:
    return replace(cfg, **overrides)


def run_backtest(rows: list[dict[str, Any]], cfg: BacktestConfig) -> dict[str, Any]:
    trades: list[dict[str, Any]] = []
    idx = 0
    cooldown_until = -1
    while idx < len(rows) - 2:
        if idx <= cooldown_until:
            idx += 1
            continue
        score = _to_float(rows[idx].get(cfg.score_field))
        side = _signal_side(score, rows[idx], cfg)
        if not side:
            idx += 1
            continue
        result = _simulate_trade(rows, idx, side, cfg)
        if not result:
            break
        trade, exit_idx = result
        trades.append(trade)
        cooldown_until = exit_idx + cfg.cooldown_bars
        idx = exit_idx + 1
    summaries = summarize_trade_views(trades, cfg)
    overall = summaries["overall"]
    overall["by_side"] = summaries["by_side"]
    overall["by_regime"] = summaries["by_regime"]
    return overall


def write_backtest_report(result: dict[str, Any], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = output_dir / f"backtest_{stamp}.json"
    md_path = output_dir / f"backtest_{stamp}.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# 雷霆 BTC 回测结果",
        "",
        f"- 生成时间：{result['generated_at']}",
        f"- 交易数：{result['trade_count']}",
        f"- 净收益：{result['net_return_pct']}%",
        f"- 平均单笔：{result['avg_trade_pct']}%",
        f"- 胜率：{result['win_rate']}",
        f"- 盈亏比：{result['profit_factor']}",
        f"- 最大回撤：{result['max_drawdown_pct']}%",
    ]
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the first-version Leiting BTC backtest.")
    parser.add_argument("--dataset", default=str(DATASETS_DIR / "BTCUSDT_5m" / "dataset.csv"))
    parser.add_argument("--score-field", default="baseline_signal_score")
    parser.add_argument("--long-threshold", type=float, default=0.35)
    parser.add_argument("--short-threshold", type=float, default=-0.35)
    parser.add_argument("--no-short", action="store_true")
    parser.add_argument("--allowed-regimes", default="")
    parser.add_argument("--max-hold-bars", type=int, default=12)
    parser.add_argument("--stop-loss-pct", type=float, default=0.35)
    parser.add_argument("--breakeven-trigger-pct", type=float, default=0.30)
    parser.add_argument("--breakeven-lock-pct", type=float, default=0.05)
    parser.add_argument("--trailing-callback-pct", type=float, default=0.25)
    parser.add_argument("--fee-bps-per-side", type=float, default=2.0)
    parser.add_argument("--slippage-bps-per-side", type=float, default=1.0)
    parser.add_argument("--cooldown-bars", type=int, default=6)
    parser.add_argument("--output-dir", default=str(BACKTESTS_DIR))
    args = parser.parse_args()

    ensure_runtime_dirs()
    rows = _read_rows(Path(args.dataset))
    cfg = BacktestConfig(
        score_field=args.score_field,
        long_threshold=args.long_threshold,
        short_threshold=args.short_threshold,
        allow_short=not args.no_short,
        allowed_regimes=[item.strip().lower() for item in args.allowed_regimes.split(",") if item.strip()] or None,
        max_hold_bars=args.max_hold_bars,
        stop_loss_pct=args.stop_loss_pct,
        breakeven_trigger_pct=args.breakeven_trigger_pct,
        breakeven_lock_pct=args.breakeven_lock_pct,
        trailing_callback_pct=args.trailing_callback_pct,
        fee_bps_per_side=args.fee_bps_per_side,
        slippage_bps_per_side=args.slippage_bps_per_side,
        cooldown_bars=args.cooldown_bars,
    )
    result = run_backtest(rows, cfg)
    reports = write_backtest_report(result, Path(args.output_dir))
    print(json.dumps({"summary": {k: v for k, v in result.items() if k != "trades"}, "reports": reports}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
