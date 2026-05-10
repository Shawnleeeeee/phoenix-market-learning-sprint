"""10-trade review export for Leiting."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from btc_engine.config import JOURNALS_DIR, REVIEWS_DIR, ensure_runtime_dirs, get_env, get_hermes_review_target, get_review_journal_mode
from btc_engine.review.metrics import compute_trade_metrics
from btc_engine.runtime.hermes_sync import run_remote_command, sync_outputs
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


def _load_trade_entries(mode_filter: str | None = None) -> list[tuple[Path, dict]]:
    trades: list[tuple[Path, dict]] = []
    if not JOURNALS_DIR.exists():
        return trades
    for path in sorted(JOURNALS_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if mode_filter and str(payload.get("mode", "")).strip().lower() != mode_filter:
                continue
            trades.append((path, payload))
        except Exception:  # noqa: BLE001
            continue
    return trades


def _load_trades(limit: int = 10) -> list[dict]:
    return [payload for _, payload in _load_trade_entries(get_review_journal_mode())[-limit:]]


def _build_recommendations(metrics: dict, trades: list[dict]) -> list[str]:
    if not trades:
        return ["当前无已平仓 BTC 交易，先积累交易样本。"]
    recommendations: list[str] = []
    if metrics["net_pnl"] < 0:
        recommendations.append("最近一批净亏损为负，先不要放大仓位或杠杆。")
    if metrics["win_rate"] < 0.4:
        recommendations.append("最近一批胜率偏低，优先回看方向门槛和入场过滤条件。")
    if metrics["fees"] > abs(metrics["gross_pnl"]):
        recommendations.append("手续费已经超过毛盈亏，说明成交方式偏激进，应继续压 maker/taker 成本。")
    if not recommendations:
        recommendations.append("最近一批交易没有明显结构性问题，可继续积累样本后再做参数调整。")
    return recommendations


def _render_markdown(title: str, review: dict) -> str:
    metrics = review["metrics"]
    recommendations = review.get("recommendations") or []
    lines = [
        f"# {title}",
        "",
        f"- 生成时间：{review['generated_at']}",
        f"- 样本交易数：{metrics['window_trade_count']}",
        f"- 净盈亏：{metrics['net_pnl']}",
        f"- 胜率：{metrics['win_rate']}",
        f"- 盈亏比：{metrics['profit_factor']}",
    ]
    if "batch_index" in review:
        lines.append(f"- 批次编号：{review['batch_index']}")
        lines.append(f"- 批次大小：{review['batch_size']}")
    lines.extend(["", "## 建议"])
    lines.extend(f"- {item}" for item in recommendations)
    return "\n".join(lines)


def _run_remote_formal_hook(target_spec: str) -> str | None:
    remote_cmd = (
        get_env(
            "BTC_HERMES_REVIEW_REMOTE_CMD",
            'python3 /opt/phoenix/deploy/hermes/leiting_formal_review_digest.py --hermes-home /root/.hermes --notify-telegram',
        )
        or ""
    ).strip()
    return run_remote_command(target_spec, remote_cmd)


def export_review_packet(limit: int = 10) -> dict:
    """Export the structured 10-trade review packet for Hermes."""
    ensure_runtime_dirs()
    trades = _load_trades(limit)
    metrics = compute_trade_metrics(trades)
    snapshot = read_runtime_state("engine_snapshot") or {}
    review = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_kind": "snapshot",
        "journal_mode": get_review_journal_mode() or "all",
        "window_trade_count": metrics["window_trade_count"],
        "metrics": metrics,
        "latest_engine_snapshot": snapshot,
        "recommendations": _build_recommendations(metrics, trades),
    }
    REVIEWS_DIR.mkdir(parents=True, exist_ok=True)
    latest_json = REVIEWS_DIR / "latest.json"
    latest_md = REVIEWS_DIR / "latest.md"
    latest_json.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_md.write_text(_render_markdown("雷霆状态复盘", review), encoding="utf-8")
    update_runtime_state("latest_review", review)
    hermes_target = get_hermes_review_target()
    if hermes_target:
        sync_outputs({"latest.json": latest_json, "latest.md": latest_md}, hermes_target)
    return review


def maybe_export_formal_review(batch_size: int | None = None) -> dict:
    ensure_runtime_dirs()
    batch_size = batch_size or int((get_env("BTC_REVIEW_BATCH_TRADES", "10") or "10").strip())
    journal_mode = get_review_journal_mode()
    entries = _load_trade_entries(journal_mode)
    completed_batch_count = len(entries) // batch_size
    state_name = "review_state" if not journal_mode else f"review_state_{journal_mode}"
    review_state = read_runtime_state(state_name) or {}
    last_exported_batch = int(review_state.get("last_exported_batch", 0) or 0)
    if completed_batch_count <= last_exported_batch:
        return {
            "triggered": False,
            "completed_batch_count": completed_batch_count,
            "last_exported_batch": last_exported_batch,
            "reason": "no_new_completed_batch",
        }

    batch_index = last_exported_batch + 1
    start = (batch_index - 1) * batch_size
    end = batch_index * batch_size
    batch_entries = entries[start:end]
    batch_paths = [str(path) for path, _ in batch_entries]
    batch_trades = [payload for _, payload in batch_entries]
    metrics = compute_trade_metrics(batch_trades)
    snapshot = read_runtime_state("engine_snapshot") or {}
    review = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_kind": "formal_batch",
        "journal_mode": journal_mode or "all",
        "batch_index": batch_index,
        "batch_size": batch_size,
        "window_trade_count": metrics["window_trade_count"],
        "metrics": metrics,
        "latest_engine_snapshot": snapshot,
        "source_journals": batch_paths,
        "recommendations": _build_recommendations(metrics, batch_trades),
    }
    formal_json = REVIEWS_DIR / f"formal_batch_{batch_index:04d}.json"
    formal_md = REVIEWS_DIR / f"formal_batch_{batch_index:04d}.md"
    formal_latest_json = REVIEWS_DIR / "formal_latest.json"
    formal_latest_md = REVIEWS_DIR / "formal_latest.md"
    payload_text = json.dumps(review, ensure_ascii=False, indent=2)
    markdown_text = _render_markdown("雷霆正式 10 单复盘", review)
    formal_json.write_text(payload_text, encoding="utf-8")
    formal_md.write_text(markdown_text, encoding="utf-8")
    formal_latest_json.write_text(payload_text, encoding="utf-8")
    formal_latest_md.write_text(markdown_text, encoding="utf-8")
    update_runtime_state(state_name, {"last_exported_batch": batch_index, "updated_at": review["generated_at"]})
    latest_formal_name = "latest_formal_review" if not journal_mode else f"latest_formal_review_{journal_mode}"
    update_runtime_state(latest_formal_name, review)

    hermes_target = get_hermes_review_target()
    remote_hook_error = None
    if hermes_target:
        sync_outputs(
            {
                formal_json.name: formal_json,
                formal_md.name: formal_md,
                "formal_latest.json": formal_latest_json,
                "formal_latest.md": formal_latest_md,
            },
            hermes_target,
        )
        remote_hook_error = _run_remote_formal_hook(hermes_target)
    return {"triggered": True, "review": review, "remote_hook_error": remote_hook_error}


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Leiting 10-trade review packet.")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--formal", action="store_true", help="Emit a completed 10-trade formal batch review if due.")
    args = parser.parse_args()
    if args.formal:
        print(json.dumps(maybe_export_formal_review(args.limit), ensure_ascii=False, indent=2))
        return
    print(json.dumps(export_review_packet(args.limit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
