"""Walk-forward backtest for Leiting BTC research."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

from btc_engine.config import BACKTESTS_DIR, DATASETS_DIR, ensure_runtime_dirs

from .backtest import BacktestConfig, clone_backtest_config, run_backtest, summarize_backtest_trades
from .train import (
    DEFAULT_FEATURE_COLUMNS,
    _direction_metrics,
    _run_side_backtests,
    attach_model_scores,
    build_linear_model,
    select_regime_thresholds,
    select_threshold,
)


@dataclass(slots=True)
class WalkForwardConfig:
    train_bars: int = 8_640
    validation_bars: int = 4_320
    test_bars: int = 4_320
    step_bars: int = 4_320
    target_column: str = "future_entry_edge_horizon_pct"
    score_field: str = "model_score"
    threshold_grid_start: float = 0.1
    threshold_grid_end: float = 1.5
    threshold_grid_step: float = 0.05
    allow_short: bool = True
    research_profile: str = "general"
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


def _threshold_grid(cfg: WalkForwardConfig) -> list[float]:
    steps = int(round((cfg.threshold_grid_end - cfg.threshold_grid_start) / cfg.threshold_grid_step))
    return [round(cfg.threshold_grid_start + idx * cfg.threshold_grid_step, 2) for idx in range(steps + 1)]


def _iterate_fold_ranges(total_rows: int, cfg: WalkForwardConfig) -> list[tuple[int, int, int, int, int, int]]:
    ranges: list[tuple[int, int, int, int, int, int]] = []
    train = cfg.train_bars
    validation = cfg.validation_bars
    test = cfg.test_bars
    step = cfg.step_bars
    start = 0
    while start + train + validation + test <= total_rows:
        train_start = start
        train_end = train_start + train
        val_start = train_end
        val_end = val_start + validation
        test_start = val_end
        test_end = test_start + test
        ranges.append((train_start, train_end, val_start, val_end, test_start, test_end))
        start += step
    return ranges


def run_walk_forward(
    *,
    dataset_path: Path,
    output_dir: Path | None = None,
    feature_columns: list[str] | None = None,
    cfg: WalkForwardConfig | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    output_dir = output_dir or BACKTESTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = _read_rows(dataset_path)
    feature_columns = feature_columns or DEFAULT_FEATURE_COLUMNS
    cfg = cfg or WalkForwardConfig()
    fold_ranges = _iterate_fold_ranges(len(rows), cfg)
    if not fold_ranges:
        raise RuntimeError("Not enough rows for the configured walk-forward windows.")

    folds: list[dict[str, Any]] = []
    combined_test_trades: list[dict[str, Any]] = []
    threshold_grid = _threshold_grid(cfg)

    for fold_index, (train_start, train_end, val_start, val_end, test_start, test_end) in enumerate(fold_ranges, start=1):
        train_rows = rows[train_start:train_end]
        val_rows = rows[val_start:val_end]
        test_rows = rows[test_start:test_end]

        model_bundle = build_linear_model(train_rows, feature_columns=feature_columns, target_column=cfg.target_column)
        model = model_bundle["model"]
        scored_train = attach_model_scores(train_rows, model, cfg.score_field)
        scored_val = attach_model_scores(val_rows, model, cfg.score_field)
        scored_test = attach_model_scores(test_rows, model, cfg.score_field)

        selected_threshold, val_result_global = select_threshold(
            scored_val,
            score_field=cfg.score_field,
            threshold_grid=threshold_grid,
            allow_short=cfg.allow_short,
            allowed_regimes=cfg.allowed_regimes,
        )
        global_cfg = BacktestConfig(
            score_field=cfg.score_field,
            long_threshold=selected_threshold,
            short_threshold=-selected_threshold,
            allow_long=True,
            allow_short=cfg.allow_short,
            allowed_regimes=cfg.allowed_regimes,
            max_hold_bars=cfg.max_hold_bars,
            stop_loss_pct=cfg.stop_loss_pct,
            breakeven_trigger_pct=cfg.breakeven_trigger_pct,
            breakeven_lock_pct=cfg.breakeven_lock_pct,
            trailing_callback_pct=cfg.trailing_callback_pct,
            fee_bps_per_side=cfg.fee_bps_per_side,
            slippage_bps_per_side=cfg.slippage_bps_per_side,
            cooldown_bars=cfg.cooldown_bars,
        )
        regime_thresholds, val_result_regime = select_regime_thresholds(scored_val, base_cfg=global_cfg, threshold_grid=threshold_grid)
        test_cfg = clone_backtest_config(global_cfg, regime_thresholds=regime_thresholds)
        test_result = run_backtest(scored_test, test_cfg)
        train_metrics = _direction_metrics(scored_train, cfg.score_field, test_cfg)
        val_metrics = _direction_metrics(scored_val, cfg.score_field, test_cfg)
        test_metrics = _direction_metrics(scored_test, cfg.score_field, test_cfg)
        validation_side_backtests = _run_side_backtests(scored_val, test_cfg)
        test_side_backtests = _run_side_backtests(scored_test, test_cfg)

        fold_trades = []
        for trade in test_result["trades"]:
            trade_copy = dict(trade)
            trade_copy["fold_index"] = fold_index
            combined_test_trades.append(trade_copy)
            fold_trades.append(trade_copy)

        folds.append(
            {
                "fold_index": fold_index,
                "ranges": {
                    "train": {"start": train_start, "end": train_end, "start_time": train_rows[0]["open_time_iso"], "end_time": train_rows[-1]["close_time_iso"]},
                    "validation": {"start": val_start, "end": val_end, "start_time": val_rows[0]["open_time_iso"], "end_time": val_rows[-1]["close_time_iso"]},
                    "test": {"start": test_start, "end": test_end, "start_time": test_rows[0]["open_time_iso"], "end_time": test_rows[-1]["close_time_iso"]},
                },
                "selected_threshold": selected_threshold,
                "selected_regime_thresholds": regime_thresholds,
                "direction_metrics": {"train": train_metrics, "validation": val_metrics, "test": test_metrics},
                "validation_backtest_global": {k: v for k, v in val_result_global.items() if k != "trades"},
                "validation_backtest_regime": {k: v for k, v in val_result_regime.items() if k != "trades"},
                "validation_backtest_by_side": validation_side_backtests,
                "test_backtest": {k: v for k, v in test_result.items() if k != "trades"},
                "test_backtest_by_side": test_side_backtests,
                "test_trades": fold_trades,
            }
        )

    aggregate_cfg = BacktestConfig(
        score_field=cfg.score_field,
        long_threshold=mean(fold["selected_threshold"] for fold in folds),
        short_threshold=-mean(fold["selected_threshold"] for fold in folds),
        allow_short=cfg.allow_short,
        max_hold_bars=cfg.max_hold_bars,
        stop_loss_pct=cfg.stop_loss_pct,
        breakeven_trigger_pct=cfg.breakeven_trigger_pct,
        breakeven_lock_pct=cfg.breakeven_lock_pct,
        trailing_callback_pct=cfg.trailing_callback_pct,
        fee_bps_per_side=cfg.fee_bps_per_side,
        slippage_bps_per_side=cfg.slippage_bps_per_side,
        cooldown_bars=cfg.cooldown_bars,
    )
    aggregate_test = summarize_backtest_trades(combined_test_trades, aggregate_cfg)
    aggregate = {
        "fold_count": len(folds),
        "selected_threshold_mean": round(mean(fold["selected_threshold"] for fold in folds), 4),
        "direction_accuracy_mean": {
            "train": round(mean(fold["direction_metrics"]["train"]["direction_accuracy"] for fold in folds), 4),
            "validation": round(mean(fold["direction_metrics"]["validation"]["direction_accuracy"] for fold in folds), 4),
            "test": round(mean(fold["direction_metrics"]["test"]["direction_accuracy"] for fold in folds), 4),
        },
        "active_signal_ratio_mean": {
            "train": round(mean(fold["direction_metrics"]["train"]["active_signal_ratio"] for fold in folds), 4),
            "validation": round(mean(fold["direction_metrics"]["validation"]["active_signal_ratio"] for fold in folds), 4),
            "test": round(mean(fold["direction_metrics"]["test"]["active_signal_ratio"] for fold in folds), 4),
        },
        "validation_net_return_mean": round(mean(fold["validation_backtest_regime"]["net_return_pct"] for fold in folds), 6),
        "test_net_return_mean": round(mean(fold["test_backtest"]["net_return_pct"] for fold in folds), 6),
        "test_long_only_net_return_mean": round(mean(fold["test_backtest_by_side"]["long_only"]["net_return_pct"] for fold in folds), 6),
        "test_short_only_net_return_mean": round(mean(fold["test_backtest_by_side"]["short_only"]["net_return_pct"] for fold in folds), 6),
        "test_summary": aggregate_test,
    }
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "feature_columns": feature_columns,
        "config": asdict(cfg),
        "folds": folds,
        "aggregate": aggregate,
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = output_dir / f"walk_forward_{stamp}.json"
    md_path = output_dir / f"walk_forward_{stamp}.md"
    latest_json = output_dir / "latest_walk_forward.json"
    latest_md = output_dir / "latest_walk_forward.md"
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    summary_lines = [
        "# 雷霆 BTC Walk-Forward 回测",
        "",
        f"- 生成时间：{result['generated_at']}",
        f"- 数据集：{dataset_path}",
        f"- 研究档位：{cfg.research_profile}",
        f"- 折数：{aggregate['fold_count']}",
        f"- 平均阈值：{aggregate['selected_threshold_mean']}",
        f"- 验证集平均净收益：{aggregate['validation_net_return_mean']}%",
        f"- 测试集平均净收益：{aggregate['test_net_return_mean']}%",
        f"- 测试集平均净收益(long-only)：{aggregate['test_long_only_net_return_mean']}%",
        f"- 测试集平均净收益(short-only)：{aggregate['test_short_only_net_return_mean']}%",
        f"- 聚合测试净收益：{aggregate['test_summary']['net_return_pct']}%",
        f"- 聚合测试交易数：{aggregate['test_summary']['trade_count']}",
        f"- 聚合测试最大回撤：{aggregate['test_summary']['max_drawdown_pct']}%",
    ]
    markdown = "\n".join(summary_lines)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")
    latest_md.write_text(markdown, encoding="utf-8")
    return {"result": result, "paths": {"json": str(json_path), "md": str(md_path), "latest_json": str(latest_json), "latest_md": str(latest_md)}}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Leiting BTC walk-forward backtest.")
    parser.add_argument("--dataset", default=str(DATASETS_DIR / "BTCUSDT_5m" / "dataset.csv"))
    parser.add_argument("--output-dir", default=str(BACKTESTS_DIR))
    parser.add_argument("--train-bars", type=int, default=8_640)
    parser.add_argument("--validation-bars", type=int, default=4_320)
    parser.add_argument("--test-bars", type=int, default=4_320)
    parser.add_argument("--step-bars", type=int, default=4_320)
    parser.add_argument("--target-column", default="future_entry_edge_horizon_pct")
    parser.add_argument("--no-short", action="store_true")
    parser.add_argument("--research-profile", default="general")
    parser.add_argument("--allowed-regimes", default="")
    args = parser.parse_args()
    cfg = WalkForwardConfig(
        train_bars=args.train_bars,
        validation_bars=args.validation_bars,
        test_bars=args.test_bars,
        step_bars=args.step_bars,
        target_column=args.target_column,
        allow_short=not args.no_short,
        research_profile=args.research_profile,
        allowed_regimes=[item.strip().lower() for item in args.allowed_regimes.split(",") if item.strip()] or None,
    )
    print(json.dumps(run_walk_forward(dataset_path=Path(args.dataset), output_dir=Path(args.output_dir), cfg=cfg), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
