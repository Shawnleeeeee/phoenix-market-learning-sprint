"""Threshold and execution-cost sensitivity scan for Leiting BTC research."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import BACKTESTS_DIR, DATASETS_DIR, MODELS_DIR, ensure_runtime_dirs

from .backtest import BacktestConfig, clone_backtest_config, run_backtest
from .train import _read_rows, _run_side_backtests, _split_rows, attach_model_scores, select_regime_thresholds


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _threshold_grid(start: float, end: float, step: float) -> list[float]:
    values: list[float] = []
    current = start
    while current <= end + 1e-9:
        values.append(round(current, 4))
        current += step
    return values


def _matrix_scan(
    rows: list[dict[str, Any]],
    *,
    thresholds: list[float],
    fee_bps_values: list[float],
    slippage_bps_values: list[float],
    stop_loss_values: list[float],
    allow_short: bool,
    allowed_regimes: list[str] | None,
) -> dict[str, Any]:
    threshold_scan: list[dict[str, Any]] = []
    cost_matrix: list[dict[str, Any]] = []
    threshold_best: dict[str, Any] | None = None
    cost_best: dict[str, Any] | None = None

    for threshold in thresholds:
        result = run_backtest(
            rows,
            BacktestConfig(
                score_field="model_score",
                long_threshold=threshold,
                short_threshold=-threshold,
                allow_long=True,
                allow_short=allow_short,
                allowed_regimes=allowed_regimes,
            ),
        )
        summary = {k: v for k, v in result.items() if k != "trades"}
        summary["threshold"] = threshold
        threshold_scan.append(summary)
        if threshold_best is None or summary["net_return_pct"] > threshold_best["net_return_pct"]:
            threshold_best = summary

    anchor_threshold = (threshold_best or {"threshold": thresholds[0]})["threshold"]
    base_cfg = BacktestConfig(
        score_field="model_score",
        long_threshold=anchor_threshold,
        short_threshold=-anchor_threshold,
        allow_long=True,
        allow_short=allow_short,
        allowed_regimes=allowed_regimes,
    )
    best_regime_thresholds, best_regime_result = select_regime_thresholds(rows, base_cfg=base_cfg, threshold_grid=thresholds)
    regime_cfg = clone_backtest_config(base_cfg, regime_thresholds=best_regime_thresholds)
    regime_side_backtests = _run_side_backtests(rows, regime_cfg)
    for fee_bps in fee_bps_values:
        for slippage_bps in slippage_bps_values:
            for stop_loss_pct in stop_loss_values:
                result = run_backtest(
                    rows,
                    clone_backtest_config(
                        regime_cfg,
                        fee_bps_per_side=fee_bps,
                        slippage_bps_per_side=slippage_bps,
                        stop_loss_pct=stop_loss_pct,
                    ),
                )
                summary = {k: v for k, v in result.items() if k != "trades"}
                summary.update(
                    {
                        "threshold": anchor_threshold,
                        "regime_thresholds": best_regime_thresholds,
                        "fee_bps_per_side": fee_bps,
                        "slippage_bps_per_side": slippage_bps,
                        "stop_loss_pct": stop_loss_pct,
                    }
                )
                cost_matrix.append(summary)
                if cost_best is None or summary["net_return_pct"] > cost_best["net_return_pct"]:
                    cost_best = summary

    return {
        "threshold_scan": threshold_scan,
        "best_threshold": threshold_best,
        "best_regime_thresholds": best_regime_thresholds,
        "best_regime_backtest": {k: v for k, v in best_regime_result.items() if k != "trades"},
        "regime_backtest_by_side": regime_side_backtests,
        "cost_matrix": cost_matrix,
        "best_cost_scenario": cost_best,
    }


def run_sensitivity_scan(
    *,
    dataset_path: Path,
    model_path: Path,
    output_dir: Path | None = None,
    threshold_start: float = 0.1,
    threshold_end: float = 1.5,
    threshold_step: float = 0.05,
    allow_short: bool = True,
    research_profile: str = "general",
    allowed_regimes: list[str] | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    output_dir = output_dir or BACKTESTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = _read_rows(dataset_path)
    _, _, test_rows = _split_rows(rows)
    model_payload = _read_json(model_path)
    scored_test = attach_model_scores(test_rows, model_payload["model"], "model_score")

    thresholds = _threshold_grid(threshold_start, threshold_end, threshold_step)
    fee_bps_values = [1.0, 2.0, 3.0]
    slippage_bps_values = [0.5, 1.0, 2.0, 3.0]
    stop_loss_values = [0.25, 0.35, 0.45]
    scan = _matrix_scan(
        scored_test,
        thresholds=thresholds,
        fee_bps_values=fee_bps_values,
        slippage_bps_values=slippage_bps_values,
        stop_loss_values=stop_loss_values,
        allow_short=allow_short,
        allowed_regimes=allowed_regimes,
    )
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "model_path": str(model_path),
        "research_profile": research_profile,
        "allow_short": allow_short,
        "allowed_regimes": allowed_regimes,
        "threshold_grid": thresholds,
        "scenario_grid": {
            "fee_bps_values": fee_bps_values,
            "slippage_bps_values": slippage_bps_values,
            "stop_loss_values": stop_loss_values,
        },
        **scan,
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = output_dir / f"sensitivity_{stamp}.json"
    md_path = output_dir / f"sensitivity_{stamp}.md"
    latest_json = output_dir / "latest_sensitivity.json"
    latest_md = output_dir / "latest_sensitivity.md"
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    threshold_best = result["best_threshold"] or {}
    cost_best = result["best_cost_scenario"] or {}
    markdown = "\n".join(
        [
            "# 雷霆 BTC 敏感性扫描",
            "",
            f"- 生成时间：{result['generated_at']}",
            f"- 研究档位：{research_profile}",
            f"- 最佳阈值：{threshold_best.get('threshold', 0)}",
            f"- 阈值扫描最佳净收益：{threshold_best.get('net_return_pct', 0)}%",
            f"- Regime 阈值：{json.dumps(result.get('best_regime_thresholds') or {}, ensure_ascii=False)}",
            f"- Regime 阈值净收益：{(result.get('best_regime_backtest') or {}).get('net_return_pct', 0)}%",
            f"- Regime long-only：{((result.get('regime_backtest_by_side') or {}).get('long_only') or {}).get('net_return_pct', 0)}%",
            f"- Regime short-only：{((result.get('regime_backtest_by_side') or {}).get('short_only') or {}).get('net_return_pct', 0)}%",
            f"- 最佳成本场景：fee={cost_best.get('fee_bps_per_side', 0)}bps, slippage={cost_best.get('slippage_bps_per_side', 0)}bps, stop={cost_best.get('stop_loss_pct', 0)}%",
            f"- 最佳成本场景净收益：{cost_best.get('net_return_pct', 0)}%",
        ]
    )
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")
    latest_md.write_text(markdown, encoding="utf-8")
    return {
        "result": result,
        "paths": {
            "json": str(json_path),
            "md": str(md_path),
            "latest_json": str(latest_json),
            "latest_md": str(latest_md),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Leiting BTC threshold/cost sensitivity scan.")
    parser.add_argument("--dataset", default=str(DATASETS_DIR / "BTCUSDT_5m" / "dataset.csv"))
    parser.add_argument("--model", default=str(MODELS_DIR / "BTCUSDT_5m" / "latest_model.json"))
    parser.add_argument("--output-dir", default=str(BACKTESTS_DIR))
    parser.add_argument("--no-short", action="store_true")
    parser.add_argument("--research-profile", default="general")
    parser.add_argument("--allowed-regimes", default="")
    args = parser.parse_args()
    print(
        json.dumps(
            run_sensitivity_scan(
                dataset_path=Path(args.dataset),
                model_path=Path(args.model),
                output_dir=Path(args.output_dir),
                allow_short=not args.no_short,
                research_profile=args.research_profile,
                allowed_regimes=[item.strip().lower() for item in args.allowed_regimes.split(",") if item.strip()] or None,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
