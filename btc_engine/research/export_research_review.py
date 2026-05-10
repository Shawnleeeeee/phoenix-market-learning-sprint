"""Export Leiting research summary for Hermes consumption."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import BACKTESTS_DIR, DATASETS_DIR, MODELS_DIR, ensure_runtime_dirs, get_env, get_hermes_research_target
from btc_engine.runtime.hermes_sync import run_remote_command, sync_outputs
from btc_engine.runtime.state_store import update_runtime_state


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def _latest_baseline_backtest() -> dict[str, Any] | None:
    candidates = sorted(
        [
            path
            for path in BACKTESTS_DIR.glob("backtest_*.json")
            if not path.name.startswith("train_val_") and "walk_forward" not in path.name
        ]
    )
    if not candidates:
        return None
    payload = _read_json(candidates[-1])
    if payload is None:
        return None
    return {"path": str(candidates[-1]), "summary": {k: v for k, v in payload.items() if k != "trades"}}


def _recommendations(summary: dict[str, Any]) -> list[str]:
    recommendations: list[str] = []
    baseline = summary.get("baseline_backtest") or {}
    training = summary.get("training") or {}
    walk_forward = summary.get("walk_forward") or {}
    sensitivity = summary.get("sensitivity") or {}
    baseline_net = (((baseline.get("summary") or {}).get("net_return_pct")) if baseline else None)
    walk_test = ((((walk_forward.get("aggregate") or {}).get("test_summary")) or {}).get("net_return_pct")) if walk_forward else None
    walk_folds = ((walk_forward.get("aggregate") or {}).get("fold_count")) if walk_forward else 0
    active_ratio = ((((training.get("direction_metrics") or {}).get("test")) or {}).get("active_signal_ratio")) if training else None
    best_cost = sensitivity.get("best_cost_scenario") or {}
    best_threshold = sensitivity.get("best_threshold") or {}
    training_side = ((training.get("backtest") or {}).get("test_by_side")) or {}
    walk_side = ((walk_forward.get("aggregate") or {})) if walk_forward else {}
    training_cfg = ((training.get("backtest") or {}).get("test") or {}).get("config") or {}
    allow_short = bool(training_cfg.get("allow_short", True))
    allowed_regimes = training.get("allowed_regimes") or training_cfg.get("allowed_regimes") or []

    if baseline_net is not None and baseline_net <= 0:
        recommendations.append("Baseline 规则策略仍然为负，不能直接拿规则分数上线。")
    if walk_test is None:
        recommendations.append("还没有 walk-forward 结果，先补齐滚动验证。")
    elif walk_test <= 0:
        recommendations.append("Walk-forward 聚合测试仍未转正，先继续做标签和特征迭代。")
    if walk_folds and walk_folds < 3:
        recommendations.append("Walk-forward 折数偏少，统计稳定性还不够。")
    if active_ratio is not None and active_ratio < 0.01:
        recommendations.append("模型当前太保守，测试集触发次数过少，先优化标签和阈值设计。")
    if best_threshold and (best_threshold.get("net_return_pct") or 0) <= 0:
        recommendations.append("阈值扫描没有找到正收益区间，先继续重做标签和执行特征。")
    if best_cost and (best_cost.get("net_return_pct") or 0) > 0:
        recommendations.append("策略对成本和止损参数较敏感，后续应把 execution cost 作为一等超参数。")
    if not allow_short:
        recommendations.append("当前研究档位已经切到 long-only，优先继续细化多头标签、阈值和执行成本。")
    if allowed_regimes:
        recommendations.append(f"当前交易过滤已收窄到 {', '.join(allowed_regimes)}，后续优先在这些 regime 内继续打磨多头入场质量。")
    long_test = ((training_side.get("long_only") or {}).get("net_return_pct"))
    short_test = ((training_side.get("short_only") or {}).get("net_return_pct"))
    long_count = ((training_side.get("long_only") or {}).get("trade_count")) or 0
    short_count = ((training_side.get("short_only") or {}).get("trade_count")) or 0
    if allow_short and long_test is not None and short_test is not None:
        if long_count > 0 and short_count > 0 and long_test > short_test:
            recommendations.append("当前样本上 long-only 明显优于 short-only，下一轮优先做多头专门化。")
        elif long_count > 0 and short_count > 0 and short_test > long_test:
            recommendations.append("当前样本上 short-only 明显优于 long-only，下一轮优先做空头专门化。")
        elif long_count > 0 and short_count == 0:
            recommendations.append("当前样本上 short-only 尚未形成有效成交，先聚焦 long-only 可行区间。")
        elif short_count > 0 and long_count == 0:
            recommendations.append("当前样本上 long-only 尚未形成有效成交，先聚焦 short-only 可行区间。")
    walk_long = walk_side.get("test_long_only_net_return_mean")
    walk_short = walk_side.get("test_short_only_net_return_mean")
    if walk_long is not None and walk_short is not None and walk_long > walk_short:
        recommendations.append("Walk-forward 结果更偏向 long-only，后续可先把多头作为主策略、空头作为次策略。")
    elif walk_long is not None and walk_short is not None and walk_short > walk_long:
        recommendations.append("Walk-forward 结果更偏向 short-only，后续可先拆分做空专用标签与阈值。")
    if not recommendations:
        recommendations.append("研究结果暂无明显结构性问题，可以继续扩展样本和执行特征。")
    return recommendations


def _render_markdown(review: dict[str, Any]) -> str:
    baseline = review.get("baseline_backtest") or {}
    training = review.get("training") or {}
    walk_forward = review.get("walk_forward") or {}
    sensitivity = review.get("sensitivity") or {}
    dataset_rows = review.get("dataset", {}).get("rows") or review.get("dataset", {}).get("row_count", 0)
    training_cfg = (((review.get("training") or {}).get("backtest") or {}).get("test") or {}).get("config") or {}
    research_profile = (review.get("training") or {}).get("research_profile") or ("long_only" if not training_cfg.get("allow_short", True) else "general")
    allowed_regimes = (review.get("training") or {}).get("allowed_regimes") or training_cfg.get("allowed_regimes") or []
    lines = [
        "# 雷霆 BTC 研究摘要",
        "",
        f"- 生成时间：{review['generated_at']}",
        f"- 数据集行数：{dataset_rows}",
        f"- 特征数量：{review['training'].get('feature_count', 0)}",
        f"- 研究档位：{research_profile}",
        f"- 允许交易 Regime：{', '.join(allowed_regimes) if allowed_regimes else '全部'}",
    ]
    if baseline:
        lines.extend(
            [
                "",
                "## Baseline 回测",
                f"- 净收益：{baseline['summary'].get('net_return_pct', 0)}%",
                f"- 交易数：{baseline['summary'].get('trade_count', 0)}",
                f"- 最大回撤：{baseline['summary'].get('max_drawdown_pct', 0)}%",
            ]
        )
    if training:
        direction_metrics = training.get("direction_metrics") or {}
        test_metrics = direction_metrics.get("test") or {}
        test_by_side = (training.get("backtest") or {}).get("test_by_side") or {}
        lines.extend(
            [
                "",
                "## 最新训练",
                f"- 全局阈值：{training.get('selected_threshold', 0)}",
                f"- Regime 阈值：{json.dumps(training.get('selected_regime_thresholds') or {}, ensure_ascii=False)}",
                f"- 测试方向准确率：{test_metrics.get('direction_accuracy', 0)}",
                f"- 测试信号触发率：{test_metrics.get('active_signal_ratio', 0)}",
                f"- 测试回测净收益：{((training.get('backtest') or {}).get('test') or {}).get('net_return_pct', 0)}%",
                f"- 测试回测净收益(long-only)：{(test_by_side.get('long_only') or {}).get('net_return_pct', 0)}%",
                f"- 测试回测净收益(short-only)：{(test_by_side.get('short_only') or {}).get('net_return_pct', 0)}%",
            ]
        )
    if walk_forward:
        aggregate = walk_forward.get("aggregate") or {}
        test_summary = aggregate.get("test_summary") or {}
        lines.extend(
            [
                "",
                "## Walk-Forward",
                f"- 折数：{aggregate.get('fold_count', 0)}",
                f"- 平均阈值：{aggregate.get('selected_threshold_mean', 0)}",
                f"- 平均测试净收益：{aggregate.get('test_net_return_mean', 0)}%",
                f"- 平均测试净收益(long-only)：{aggregate.get('test_long_only_net_return_mean', 0)}%",
                f"- 平均测试净收益(short-only)：{aggregate.get('test_short_only_net_return_mean', 0)}%",
                f"- 聚合测试净收益：{test_summary.get('net_return_pct', 0)}%",
                f"- 聚合测试交易数：{test_summary.get('trade_count', 0)}",
            ]
        )
    if sensitivity:
        best_threshold = sensitivity.get("best_threshold") or {}
        best_cost = sensitivity.get("best_cost_scenario") or {}
        lines.extend(
            [
                "",
                "## 敏感性扫描",
                f"- 最佳阈值：{best_threshold.get('threshold', 0)}",
                f"- 阈值扫描最佳净收益：{best_threshold.get('net_return_pct', 0)}%",
                f"- Regime 阈值：{json.dumps(sensitivity.get('best_regime_thresholds') or {}, ensure_ascii=False)}",
                f"- Regime 阈值净收益：{(sensitivity.get('best_regime_backtest') or {}).get('net_return_pct', 0)}%",
                f"- Regime long-only：{((sensitivity.get('regime_backtest_by_side') or {}).get('long_only') or {}).get('net_return_pct', 0)}%",
                f"- Regime short-only：{((sensitivity.get('regime_backtest_by_side') or {}).get('short_only') or {}).get('net_return_pct', 0)}%",
                f"- 最佳成本场景净收益：{best_cost.get('net_return_pct', 0)}%",
                f"- 最佳成本场景：fee={best_cost.get('fee_bps_per_side', 0)}bps / slippage={best_cost.get('slippage_bps_per_side', 0)}bps / stop={best_cost.get('stop_loss_pct', 0)}%",
            ]
        )
    lines.extend(["", "## 建议"])
    lines.extend(f"- {item}" for item in review.get("recommendations") or [])
    return "\n".join(lines)


def export_research_review() -> dict[str, Any]:
    ensure_runtime_dirs()
    dataset_manifest = _read_json(DATASETS_DIR / "BTCUSDT_5m" / "dataset_manifest.json") or {}
    latest_model = _read_json(MODELS_DIR / "BTCUSDT_5m" / "latest_model.json") or {}
    latest_walk_forward = _read_json(BACKTESTS_DIR / "latest_walk_forward.json") or {}
    latest_sensitivity = _read_json(BACKTESTS_DIR / "latest_sensitivity.json") or {}
    baseline_backtest = _latest_baseline_backtest() or {}

    review = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "kind": "research_summary",
        "dataset": dataset_manifest,
        "baseline_backtest": baseline_backtest,
        "training": {
            "feature_count": len(latest_model.get("feature_columns") or []),
            "selected_threshold": latest_model.get("selected_threshold"),
            "selected_regime_thresholds": latest_model.get("selected_regime_thresholds"),
            "direction_metrics": latest_model.get("direction_metrics"),
            "backtest": latest_model.get("backtest"),
            "research_profile": latest_model.get("research_profile"),
            "allowed_regimes": latest_model.get("allowed_regimes"),
            "path": str(MODELS_DIR / "BTCUSDT_5m" / "latest_model.json") if latest_model else None,
        },
        "walk_forward": latest_walk_forward,
        "sensitivity": latest_sensitivity,
    }
    walk_forward_aggregate = latest_walk_forward.get("aggregate") or {}
    walk_forward_test = walk_forward_aggregate.get("test_summary") or {}
    review.update(
        {
            "dataset_rows": dataset_manifest.get("rows") or dataset_manifest.get("row_count"),
            "feature_count": len(latest_model.get("feature_columns") or []),
            "profile": review["training"].get("research_profile"),
            "allowed_regimes": review["training"].get("allowed_regimes") or [],
            "baseline_net_return_pct": ((baseline_backtest.get("summary") or {}).get("net_return_pct")),
            "walk_forward_net_return_pct": walk_forward_test.get("net_return_pct"),
            "walk_forward_trade_count": walk_forward_test.get("trade_count"),
            "selected_threshold": latest_model.get("selected_threshold"),
        }
    )
    review["recommendations"] = _recommendations(review)

    research_dir = BACKTESTS_DIR / "research_reviews"
    research_dir.mkdir(parents=True, exist_ok=True)
    latest_json = research_dir / "latest.json"
    latest_md = research_dir / "latest.md"
    payload = json.dumps(review, ensure_ascii=False, indent=2)
    markdown = _render_markdown(review)
    latest_json.write_text(payload, encoding="utf-8")
    latest_md.write_text(markdown, encoding="utf-8")
    update_runtime_state("latest_research_review", review)

    target = get_hermes_research_target()
    remote_hook_error = None
    if target:
        sync_outputs({"latest.json": latest_json, "latest.md": latest_md}, target)
        remote_cmd = (
            get_env(
                "BTC_HERMES_RESEARCH_REMOTE_CMD",
                'python3 /opt/phoenix/deploy/hermes/leiting_research_digest.py --hermes-home /root/.hermes --notify-telegram',
            )
            or ""
        ).strip()
        if remote_cmd:
            remote_hook_error = run_remote_command(target, remote_cmd)

    return {
        "review": review,
        "paths": {"json": str(latest_json), "md": str(latest_md)},
        "remote_hook_error": remote_hook_error,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Leiting BTC research summary for Hermes.")
    parser.parse_args()
    print(json.dumps(export_research_review(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
