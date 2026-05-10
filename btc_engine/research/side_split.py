"""BTC side-split research summary for Leiting."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import BACKTESTS_DIR, DATASETS_DIR, MODELS_DIR, ensure_runtime_dirs

from .sensitivity import run_sensitivity_scan
from .train import train_model
from .walk_forward import WalkForwardConfig, run_walk_forward


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _summary_from_training(training: dict[str, Any], side: str) -> dict[str, Any]:
    artifact = training["artifact"]
    if side == "long":
        payload = artifact["backtest"]["test"]
    else:
        payload = artifact["backtest"]["test_by_side"]["short_only"]
    return {
        "net_return_pct": payload.get("net_return_pct"),
        "trade_count": payload.get("trade_count"),
        "win_rate": payload.get("win_rate"),
        "profit_factor": payload.get("profit_factor"),
    }


def _summary_from_walk_forward(walk_forward: dict[str, Any], side: str) -> dict[str, Any]:
    aggregate = walk_forward["result"]["aggregate"]
    key = "test_long_only_net_return_mean" if side == "long" else "test_short_only_net_return_mean"
    return {
        "mean_net_return_pct": aggregate.get(key),
        "fold_count": aggregate.get("fold_count"),
        "selected_threshold_mean": aggregate.get("selected_threshold_mean"),
    }


def _summary_from_sensitivity(sensitivity: dict[str, Any], side: str) -> dict[str, Any]:
    result = sensitivity["result"]
    side_key = "long_only" if side == "long" else "short_only"
    payload = (result.get("regime_backtest_by_side") or {}).get(side_key) or {}
    return {
        "regime_net_return_pct": payload.get("net_return_pct"),
        "trade_count": payload.get("trade_count"),
        "best_threshold": (result.get("best_threshold") or {}).get("threshold"),
        "best_threshold_net_return_pct": (result.get("best_threshold") or {}).get("net_return_pct"),
    }


def run_side_split_research(
    *,
    dataset_path: Path,
    output_dir: Path | None = None,
    model_dir: Path | None = None,
    allowed_regimes: list[str] | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    output_dir = output_dir or BACKTESTS_DIR / "side_split"
    model_dir = model_dir or MODELS_DIR / "BTCUSDT_5m_side_split"
    output_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)

    dual_training = train_model(
        dataset_path=dataset_path,
        output_dir=model_dir / "dual",
        target_column="future_entry_edge_horizon_pct",
        allow_short=True,
        research_profile="btc_side_split_dual",
        allowed_regimes=allowed_regimes,
    )
    long_training = train_model(
        dataset_path=dataset_path,
        output_dir=model_dir / "long",
        target_column="future_entry_edge_long_horizon_pct",
        allow_short=False,
        research_profile="btc_side_split_long",
        allowed_regimes=allowed_regimes,
    )
    dual_walk_forward = run_walk_forward(
        dataset_path=dataset_path,
        output_dir=output_dir / "walk_forward_dual",
        cfg=WalkForwardConfig(
            train_bars=2880,
            validation_bars=1440,
            test_bars=1440,
            step_bars=1440,
            allow_short=True,
            research_profile="btc_side_split_dual",
            allowed_regimes=allowed_regimes,
        ),
    )
    dual_sensitivity = run_sensitivity_scan(
        dataset_path=dataset_path,
        model_path=Path(dual_training["paths"]["latest_model"]),
        output_dir=output_dir / "sensitivity_dual",
        allow_short=True,
        research_profile="btc_side_split_dual",
        allowed_regimes=allowed_regimes,
    )
    result = {
        "generated_at": _utc_now(),
        "dataset_path": str(dataset_path),
        "allowed_regimes": allowed_regimes or [],
        "long": {
            "training": _summary_from_training(long_training, "long"),
            "walk_forward": _summary_from_walk_forward(dual_walk_forward, "long"),
            "sensitivity": _summary_from_sensitivity(dual_sensitivity, "long"),
        },
        "short": {
            "training": _summary_from_training(dual_training, "short"),
            "walk_forward": _summary_from_walk_forward(dual_walk_forward, "short"),
            "sensitivity": _summary_from_sensitivity(dual_sensitivity, "short"),
        },
        "artifacts": {
            "dual_training": dual_training["paths"],
            "long_training": long_training["paths"],
            "dual_walk_forward": dual_walk_forward["paths"],
            "dual_sensitivity": dual_sensitivity["paths"],
        },
    }
    markdown = "\n".join(
        [
            "# 雷霆 BTC 多空分拆研究",
            "",
            f"- 生成时间：{result['generated_at']}",
            f"- 数据集：{dataset_path}",
            f"- 允许 Regime：{', '.join(result['allowed_regimes']) or 'all'}",
            "",
            "## Long",
            f"- 训练测试净收益：{result['long']['training']['net_return_pct']}%",
            f"- Walk-forward 平均净收益：{result['long']['walk_forward']['mean_net_return_pct']}%",
            f"- Sensitivity Regime 净收益：{result['long']['sensitivity']['regime_net_return_pct']}%",
            "",
            "## Short",
            f"- 训练测试净收益：{result['short']['training']['net_return_pct']}%",
            f"- Walk-forward 平均净收益：{result['short']['walk_forward']['mean_net_return_pct']}%",
            f"- Sensitivity Regime 净收益：{result['short']['sensitivity']['regime_net_return_pct']}%",
        ]
    )
    json_path = output_dir / "latest.json"
    md_path = output_dir / "latest.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")
    return {"result": result, "paths": {"latest_json": str(json_path), "latest_md": str(md_path)}}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run BTC long/short side-split research.")
    parser.add_argument("--dataset", default=str(DATASETS_DIR / "BTCUSDT_5m" / "dataset.csv"))
    parser.add_argument("--output-dir", default=str(BACKTESTS_DIR / "side_split"))
    parser.add_argument("--model-dir", default=str(MODELS_DIR / "BTCUSDT_5m_side_split"))
    parser.add_argument("--allowed-regimes", default="")
    args = parser.parse_args()
    print(
        json.dumps(
            run_side_split_research(
                dataset_path=Path(args.dataset),
                output_dir=Path(args.output_dir),
                model_dir=Path(args.model_dir),
                allowed_regimes=[item.strip().lower() for item in args.allowed_regimes.split(",") if item.strip()] or None,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
