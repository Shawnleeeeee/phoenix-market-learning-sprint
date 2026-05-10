"""First-version model training scaffold for Leiting BTC."""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from btc_engine.config import BACKTESTS_DIR, DATASETS_DIR, MODELS_DIR, ensure_runtime_dirs

from .backtest import BacktestConfig, clone_backtest_config, run_backtest


DEFAULT_FEATURE_COLUMNS = [
    "return_1_pct",
    "return_3_pct",
    "return_6_pct",
    "return_12_pct",
    "candle_range_pct",
    "candle_body_pct",
    "volume_ratio_12",
    "volume_ratio_36",
    "realized_vol_12",
    "realized_vol_36",
    "ema_12_gap_pct",
    "ema_24_gap_pct",
    "ema_48_gap_pct",
    "ema_cross_12_24_pct",
    "ema_cross_24_48_pct",
    "atr_14_pct",
    "rsi_14",
    "bollinger_width_20_pct",
    "bollinger_zscore_20",
    "vwap_session_deviation_pct",
    "macd_line_pct",
    "macd_signal_pct",
    "macd_hist_pct",
    "funding_rate_bps",
    "premium_deviation_pct",
    "has_oi_context",
    "oi_age_bars",
    "oi_change_1_pct",
    "oi_change_3_pct",
    "oi_change_12_pct",
    "has_taker_context",
    "taker_age_bars",
    "buy_sell_pressure",
    "taker_flow_imbalance",
    "has_micro_context",
    "micro_last_return_1m_pct",
    "micro_mean_range_1m_pct",
    "micro_last_imbalance_1m",
    "micro_mean_imbalance_1m",
    "micro_std_imbalance_1m",
    "micro_buy_share_5m",
    "micro_window_imbalance_5m",
    "has_global_ratio_context",
    "global_long_short_ratio",
    "global_account_skew",
    "global_buy_crowding",
    "global_ratio_age_bars",
    "has_runtime_micro_context",
    "runtime_snapshot_count",
    "runtime_spread_bps_mean",
    "runtime_spread_bps_max",
    "runtime_depth_imbalance_last",
    "runtime_depth_imbalance_mean",
    "runtime_depth_imbalance_std",
    "runtime_slippage_bps_buy_mean",
    "runtime_slippage_bps_sell_mean",
    "runtime_execution_quality_mean",
    "runtime_microstructure_score_mean",
    "runtime_taker_buy_ratio_mean",
]


def _read_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _split_rows(rows: list[dict[str, Any]], train_ratio: float = 0.7, val_ratio: float = 0.15) -> tuple[list[dict], list[dict], list[dict]]:
    total = len(rows)
    train_end = int(total * train_ratio)
    val_end = train_end + int(total * val_ratio)
    return rows[:train_end], rows[train_end:val_end], rows[val_end:]


def _prepare_matrix(rows: list[dict[str, Any]], feature_columns: list[str], target_column: str) -> tuple[list[list[float]], list[float], list[dict[str, float]]]:
    raw_rows: list[dict[str, Any]] = []
    targets: list[float] = []
    feature_samples: dict[str, list[float]] = {col: [] for col in feature_columns}
    for row in rows:
        target = _to_float(row.get(target_column))
        if target is None:
            continue
        for col in feature_columns:
            val = _to_float(row.get(col))
            if val is not None:
                feature_samples[col].append(val)
        raw_rows.append(row)
        targets.append(target)

    feature_stats: list[dict[str, float]] = []
    if not raw_rows:
        return [], targets, feature_stats
    for column in feature_columns:
        series = feature_samples[column]
        mu = mean(series) if series else 0.0
        sigma = pstdev(series) if len(series) > 1 else 1.0
        sigma = sigma or 1.0
        feature_stats.append({"feature": column, "mean": mu, "std": sigma})

    stat_by_feature = {item["feature"]: item for item in feature_stats}
    valid_rows: list[list[float]] = []
    for row in raw_rows:
        values: list[float] = []
        for col in feature_columns:
            val = _to_float(row.get(col))
            if val is None:
                val = stat_by_feature[col]["mean"]
            values.append(val)
        valid_rows.append(values)
    return valid_rows, targets, feature_stats


def _standardize_row(row: list[float], feature_stats: list[dict[str, float]]) -> list[float]:
    standardized: list[float] = []
    for value, stats in zip(row, feature_stats, strict=False):
        std = stats["std"] or 1.0
        standardized.append((value - stats["mean"]) / std)
    return standardized


def _fit_linear_score(
    matrix: list[list[float]],
    targets: list[float],
    feature_stats: list[dict[str, float]],
) -> dict[str, Any]:
    if not matrix or not targets:
        raise RuntimeError("Not enough rows to fit the Leiting baseline model.")
    target_mean = mean(targets)
    target_std = pstdev(targets) or 1.0
    standardized_rows = [_standardize_row(row, feature_stats) for row in matrix]
    weights: dict[str, float] = {}
    for idx, stats in enumerate(feature_stats):
        series = [row[idx] for row in standardized_rows]
        covariance = mean((x) * ((y - target_mean) / target_std) for x, y in zip(series, targets, strict=False))
        weights[stats["feature"]] = round(covariance, 8)
    return {
        "intercept": round(target_mean, 8),
        "target_mean": round(target_mean, 8),
        "target_std": round(target_std, 8),
        "feature_stats": feature_stats,
        "weights": weights,
    }


def score_row(row: dict[str, Any], model: dict[str, Any]) -> float | None:
    total = model["intercept"]
    for stats in model["feature_stats"]:
        value = _to_float(row.get(stats["feature"]))
        if value is None:
            value = stats["mean"]
        std = stats["std"] or 1.0
        z = (value - stats["mean"]) / std
        total += z * model["weights"][stats["feature"]]
    return round(total, 8)


def attach_model_scores(rows: list[dict[str, Any]], model: dict[str, Any], score_field: str = "model_score") -> list[dict[str, Any]]:
    scored_rows: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        enriched[score_field] = score_row(enriched, model)
        scored_rows.append(enriched)
    return scored_rows


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


def _direction_metrics(rows: list[dict[str, Any]], score_field: str, cfg: BacktestConfig) -> dict[str, Any]:
    total = 0
    correct = 0
    active = 0
    for row in rows:
        score = _to_float(row.get(score_field))
        label = row.get("label_direction")
        if score is None or label not in {"LONG", "SHORT", "NONE"}:
            continue
        total += 1
        long_threshold, short_threshold = _resolve_thresholds(row, cfg)
        predicted = "NONE"
        if cfg.allow_long and score >= long_threshold:
            predicted = "LONG"
            active += 1
        elif cfg.allow_short and score <= short_threshold:
            predicted = "SHORT"
            active += 1
        if predicted == label:
            correct += 1
    return {
        "direction_accuracy": round(correct / total, 4) if total else 0.0,
        "active_signal_ratio": round(active / total, 4) if total else 0.0,
        "evaluated_rows": total,
    }


def build_linear_model(
    train_rows: list[dict[str, Any]],
    feature_columns: list[str] | None = None,
    target_column: str = "future_entry_edge_horizon_pct",
) -> dict[str, Any]:
    feature_columns = feature_columns or DEFAULT_FEATURE_COLUMNS
    train_matrix, train_targets, feature_stats = _prepare_matrix(train_rows, feature_columns, target_column)
    model = _fit_linear_score(train_matrix, train_targets, feature_stats)
    return {
        "feature_columns": feature_columns,
        "target_column": target_column,
        "model": model,
    }


def select_threshold(
    rows: list[dict[str, Any]],
    *,
    score_field: str = "model_score",
    threshold_grid: list[float] | None = None,
    allow_short: bool = True,
    allowed_regimes: list[str] | None = None,
) -> tuple[float, dict[str, Any]]:
    threshold_grid = threshold_grid or [round(x * 0.05, 2) for x in range(2, 31)]
    best_threshold = 0.35
    best_result: dict[str, Any] | None = None
    for threshold in threshold_grid:
        cfg = BacktestConfig(
            score_field=score_field,
            long_threshold=threshold,
            short_threshold=-threshold,
            allow_long=True,
            allow_short=allow_short,
            allowed_regimes=allowed_regimes,
        )
        result = run_backtest(rows, cfg)
        summary = result["net_return_pct"]
        if best_result is None or summary > best_result["net_return_pct"]:
            best_result = result
            best_threshold = threshold
    if best_result is None:
        raise RuntimeError("Threshold search failed; no validation backtest results were produced.")
    return best_threshold, best_result


def _regimes_in_rows(rows: list[dict[str, Any]], regime_field: str = "label_regime") -> list[str]:
    return sorted(
        {
            str(row.get(regime_field) or "").strip().lower()
            for row in rows
            if str(row.get(regime_field) or "").strip().lower()
        }
    )


def select_regime_thresholds(
    rows: list[dict[str, Any]],
    *,
    base_cfg: BacktestConfig,
    threshold_grid: list[float] | None = None,
) -> tuple[dict[str, float], dict[str, Any]]:
    threshold_grid = threshold_grid or [round(x * 0.05, 2) for x in range(2, 31)]
    regime_thresholds = {
        regime: abs(base_cfg.long_threshold)
        for regime in _regimes_in_rows(rows, base_cfg.regime_field)
    }
    best_result = run_backtest(rows, clone_backtest_config(base_cfg, regime_thresholds=regime_thresholds))
    for regime in regime_thresholds:
        best_threshold = regime_thresholds[regime]
        for threshold in threshold_grid:
            candidate_map = dict(regime_thresholds)
            candidate_map[regime] = threshold
            candidate_cfg = clone_backtest_config(base_cfg, regime_thresholds=candidate_map)
            candidate_result = run_backtest(rows, candidate_cfg)
            if candidate_result["net_return_pct"] > best_result["net_return_pct"]:
                regime_thresholds = candidate_map
                best_threshold = threshold
                best_result = candidate_result
        regime_thresholds[regime] = best_threshold
    return regime_thresholds, best_result


def _run_side_backtests(rows: list[dict[str, Any]], cfg: BacktestConfig) -> dict[str, Any]:
    both = run_backtest(rows, cfg)
    long_only = run_backtest(rows, clone_backtest_config(cfg, allow_long=True, allow_short=False))
    short_only = run_backtest(rows, clone_backtest_config(cfg, allow_long=False, allow_short=True))
    return {
        "both": {k: v for k, v in both.items() if k != "trades"},
        "long_only": {k: v for k, v in long_only.items() if k != "trades"},
        "short_only": {k: v for k, v in short_only.items() if k != "trades"},
    }


def train_model(
    *,
    dataset_path: Path,
    output_dir: Path | None = None,
    feature_columns: list[str] | None = None,
    target_column: str = "future_entry_edge_horizon_pct",
    threshold_grid: list[float] | None = None,
    allow_short: bool = True,
    research_profile: str = "general",
    allowed_regimes: list[str] | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    output_dir = output_dir or MODELS_DIR / "BTCUSDT_5m"
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = _read_rows(dataset_path)
    feature_columns = feature_columns or DEFAULT_FEATURE_COLUMNS
    threshold_grid = threshold_grid or [round(x * 0.05, 2) for x in range(2, 31)]

    train_rows, val_rows, test_rows = _split_rows(rows)
    model_bundle = build_linear_model(train_rows, feature_columns=feature_columns, target_column=target_column)
    model = model_bundle["model"]
    train_rows = attach_model_scores(train_rows, model)
    val_rows = attach_model_scores(val_rows, model)
    test_rows = attach_model_scores(test_rows, model)

    best_threshold, val_backtest_global = select_threshold(
        val_rows,
        score_field="model_score",
        threshold_grid=threshold_grid,
        allow_short=allow_short,
        allowed_regimes=allowed_regimes,
    )
    global_cfg = BacktestConfig(
        score_field="model_score",
        long_threshold=best_threshold,
        short_threshold=-best_threshold,
        allow_long=True,
        allow_short=allow_short,
        allowed_regimes=allowed_regimes,
    )
    regime_thresholds, val_backtest_regime = select_regime_thresholds(val_rows, base_cfg=global_cfg, threshold_grid=threshold_grid)
    final_cfg = clone_backtest_config(global_cfg, regime_thresholds=regime_thresholds)
    test_backtest = run_backtest(test_rows, final_cfg)
    train_metrics = _direction_metrics(train_rows, "model_score", final_cfg)
    val_metrics = _direction_metrics(val_rows, "model_score", final_cfg)
    test_metrics = _direction_metrics(test_rows, "model_score", final_cfg)
    validation_side_backtests = _run_side_backtests(val_rows, final_cfg)
    test_side_backtests = _run_side_backtests(test_rows, final_cfg)

    artifact = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "feature_columns": feature_columns,
        "target_column": target_column,
        "research_profile": research_profile,
        "allow_short": allow_short,
        "allowed_regimes": allowed_regimes,
        "split": {
            "train_rows": len(train_rows),
            "val_rows": len(val_rows),
            "test_rows": len(test_rows),
        },
        "model": model,
        "selected_threshold": best_threshold,
        "selected_regime_thresholds": regime_thresholds,
        "direction_metrics": {
            "train": train_metrics,
            "validation": val_metrics,
            "test": test_metrics,
        },
        "backtest": {
            "validation_global": {k: v for k, v in val_backtest_global.items() if k != "trades"},
            "validation_regime": {k: v for k, v in val_backtest_regime.items() if k != "trades"},
            "validation_by_side": validation_side_backtests,
            "test": {k: v for k, v in test_backtest.items() if k != "trades"},
            "test_by_side": test_side_backtests,
        },
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    model_path = output_dir / f"linear_score_model_{stamp}.json"
    latest_model_path = output_dir / "latest_model.json"
    report_path = output_dir / f"training_report_{stamp}.md"
    artifact_text = json.dumps(artifact, ensure_ascii=False, indent=2)
    model_path.write_text(artifact_text, encoding="utf-8")
    latest_model_path.write_text(artifact_text, encoding="utf-8")
    report_path.write_text(
        "\n".join(
            [
                "# 雷霆 BTC 第一版训练报告",
                "",
                f"- 生成时间：{artifact['generated_at']}",
                f"- 数据集：{dataset_path}",
                f"- 研究档位：{research_profile}",
                f"- 特征数：{len(feature_columns)}",
                f"- 全局阈值：{best_threshold}",
                f"- Regime 阈值：{json.dumps(regime_thresholds, ensure_ascii=False)}",
                f"- 训练方向准确率：{train_metrics['direction_accuracy']}",
                f"- 验证方向准确率：{val_metrics['direction_accuracy']}",
                f"- 测试方向准确率：{test_metrics['direction_accuracy']}",
                f"- 验证净收益(全局)：{artifact['backtest']['validation_global']['net_return_pct']}%",
                f"- 验证净收益(Regime)：{artifact['backtest']['validation_regime']['net_return_pct']}%",
                f"- 测试净收益：{artifact['backtest']['test']['net_return_pct']}%",
                f"- 测试净收益(long-only)：{artifact['backtest']['test_by_side']['long_only']['net_return_pct']}%",
                f"- 测试净收益(short-only)：{artifact['backtest']['test_by_side']['short_only']['net_return_pct']}%",
            ]
        ),
        encoding="utf-8",
    )

    val_reports = {
        "json": str((BACKTESTS_DIR / f"train_val_backtest_{stamp}.json")),
        "md": str((BACKTESTS_DIR / f"train_val_backtest_{stamp}.md")),
    }
    (BACKTESTS_DIR / f"train_val_backtest_{stamp}.json").write_text(
        json.dumps(
            {
                "global": val_backtest_global,
                "regime": val_backtest_regime,
                "regime_thresholds": regime_thresholds,
                "by_side": validation_side_backtests,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (BACKTESTS_DIR / f"train_val_backtest_{stamp}.md").write_text(
        "\n".join(
            [
                "# 雷霆验证集回测",
                "",
                f"- 全局阈值净收益：{val_backtest_global['net_return_pct']}%",
                f"- Regime 阈值净收益：{val_backtest_regime['net_return_pct']}%",
                f"- Regime 阈值：{json.dumps(regime_thresholds, ensure_ascii=False)}",
                f"- Long-only 净收益：{validation_side_backtests['long_only']['net_return_pct']}%",
                f"- Short-only 净收益：{validation_side_backtests['short_only']['net_return_pct']}%",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "artifact": artifact,
        "paths": {
            "model": str(model_path),
            "latest_model": str(latest_model_path),
            "report": str(report_path),
            "validation_backtest": val_reports,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the first-version Leiting BTC linear score model.")
    parser.add_argument("--dataset", default=str(DATASETS_DIR / "BTCUSDT_5m" / "dataset.csv"))
    parser.add_argument("--output-dir", default=str(MODELS_DIR / "BTCUSDT_5m"))
    parser.add_argument("--target-column", default="future_entry_edge_horizon_pct")
    parser.add_argument("--no-short", action="store_true")
    parser.add_argument("--research-profile", default="general")
    parser.add_argument("--allowed-regimes", default="")
    args = parser.parse_args()
    result = train_model(
        dataset_path=Path(args.dataset),
        output_dir=Path(args.output_dir),
        target_column=args.target_column,
        allow_short=not args.no_short,
        research_profile=args.research_profile,
        allowed_regimes=[item.strip().lower() for item in args.allowed_regimes.split(",") if item.strip()] or None,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
