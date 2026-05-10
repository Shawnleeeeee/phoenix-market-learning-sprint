#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from deploy.hermes.binance_skill_context import load_skill_digest, markdown_lines_from_digest, telegram_lines_from_digest
from deploy.hermes.hermes_telegram import send_telegram_message
from phoenix.guardian_workers import latest_running_worker as latest_running_worker_from_exchange
from phoenix.hermes_notify import humanize_side


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review the last two hours of Phoenix operations.")
    parser.add_argument("--window-hours", type=float, default=2.0)
    parser.add_argument("--hermes-home", default=os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes"))
    parser.add_argument("--notify-telegram", action="store_true")
    return parser.parse_args()


def load_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    items: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            items.append(payload)
    return items


def parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def in_window(timestamp: datetime | None, start: datetime, end: datetime) -> bool:
    if timestamp is None:
        return False
    return start <= timestamp <= end


def read_guardian_jobs(workers_dir: Path) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    if not workers_dir.exists():
        return jobs
    for path in sorted(workers_dir.glob("*.json")):
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            continue
        payload["_path"] = str(path)
        jobs.append(payload)
    return jobs


def read_trade_journals(journals_dir: Path) -> list[dict[str, Any]]:
    journals: list[dict[str, Any]] = []
    if not journals_dir.exists():
        return journals
    for path in sorted(journals_dir.glob("*.json")):
        payload = load_json_file(path)
        if not isinstance(payload, dict):
            continue
        payload["_path"] = str(path)
        journals.append(payload)
    return journals


def latest_running_worker(workers_dir: Path) -> dict[str, Any] | None:
    payload = latest_running_worker_from_exchange(workers_dir)
    if not isinstance(payload, dict):
        return None
    worker = dict(payload)
    intent = worker.get("intent") if isinstance(worker.get("intent"), dict) else {}
    if worker.get("symbol") in (None, ""):
        worker["symbol"] = intent.get("symbol")
    if worker.get("side") in (None, ""):
        worker["side"] = intent.get("side")
    if worker.get("entry_price") in (None, ""):
        worker["entry_price"] = intent.get("entry_price")
    if worker.get("quantity") in (None, ""):
        worker["quantity"] = intent.get("quantity")
    return worker


def humanize_reason(value: Any) -> str:
    reason = str(value or "")
    mapping = {
        "active_position_in_progress": "有活跃仓位在管仓中",
        "candidate_not_significantly_changed": "候选变化不显著",
        "cycle_failed": "扫描或评分失败",
        "dispatch_triggered": "已进入调度链路",
        "max_open_positions_blocked": "已达到最大持仓数",
        "no_supported_directional_bias": "方向不足，不支持开仓",
        "candidate_bias_none": "候选方向为 NONE",
        "candidate_bias_long": "候选方向做多",
        "candidate_bias_short": "候选方向做空",
    }
    return mapping.get(reason, reason)


def format_float(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def build_recommendations(
    *,
    cycles: list[dict[str, Any]],
    journals: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    active_worker: dict[str, Any] | None,
) -> list[str]:
    recommendations: list[str] = []

    none_bias_high = 0
    network_issues = 0
    blocked_top = 0
    reasons = Counter()
    top_symbols = Counter()
    for cycle in cycles:
        actionable = cycle.get("actionable_top_candidate")
        raw = cycle.get("raw_top_candidate")
        if isinstance(actionable, dict):
            top_symbols[str(actionable.get("symbol") or "")] += 1
            try:
                score = float(actionable.get("score") or 0.0)
            except (TypeError, ValueError):
                score = 0.0
            if score >= 10 and str(actionable.get("directional_bias") or "") == "NONE":
                none_bias_high += 1
        if isinstance(raw, dict) and raw.get("blocked_reasons"):
            blocked_top += 1
        reason = str(cycle.get("reason") or "").strip()
        if reason:
            reasons[reason] += 1
        tail = str(cycle.get("scan_stdout_tail") or "")
        network_issues += tail.count("oi_poll_error")

    if network_issues > 0:
        recommendations.append(
            f"本窗口内 OI 轮询出现 {network_issues} 次瞬时网络错误；继续观察代理稳定性和 Binance 公共接口连通性。"
        )
    if none_bias_high > 0:
        recommendations.append(
            f"仍有 {none_bias_high} 个高分候选被判为 directional_bias=NONE；在扩大自动暴露前，继续加强价格/OI/成交量的方向一致性判断。"
        )
    if blocked_top > 0:
        recommendations.append(
            f"有 {blocked_top} 轮原始第一名被策略闸门拦住；继续坚持 actionable-top 路由，并考虑再加强旧热点的新鲜度衰减。"
        )
    if reasons.get("active_position_in_progress"):
        recommendations.append(
            "单仓纪律运行正常：当 Guardian 管仓任务仍在运行时，自动循环会主动跳过新开仓。"
        )
    if top_symbols:
        symbol, count = top_symbols.most_common(1)[0]
        if count >= max(3, len(cycles) // 2):
            recommendations.append(
                f"{symbol} 在 {len(cycles)} 轮可执行候选中占了 {count} 轮；继续观察是否需要再加一档“连续霸榜降权”。"
            )
    if journals:
        pnl = 0.0
        counted = 0
        for journal in journals:
            settlement = journal.get("actual_settlement") if isinstance(journal.get("actual_settlement"), dict) else None
            if settlement and settlement.get("net_pnl_usdt") is not None:
                try:
                    pnl += float(settlement["net_pnl_usdt"])
                    counted += 1
                except (TypeError, ValueError):
                    pass
        if counted:
            recommendations.append(
                f"本窗口已平仓 {counted} 笔，累计净利润 {pnl:.4f} USDT。继续保持 recommendations-only，等再积累几笔策略驱动平仓后再考虑自动调参。"
            )
    if active_worker is not None:
        recommendations.append(
            f"当前活跃仓位仍为 {active_worker.get('symbol')} {humanize_side(active_worker.get('side'))}，阶段 {active_worker.get('phase')}；在保护链未结束前不应改动参数。"
        )
    if not recommendations:
        recommendations.append("本窗口暂无建议改动，继续收集更多 cycle 样本。")
    return recommendations


def build_review(
    *,
    start: datetime,
    end: datetime,
    state: dict[str, Any] | None,
    cycles: list[dict[str, Any]],
    journals: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    active_worker: dict[str, Any] | None,
) -> dict[str, Any]:
    skip_reasons = Counter(str(item.get("reason") or "unknown") for item in cycles)
    actionable_symbols = Counter(
        str(item.get("actionable_top_candidate", {}).get("symbol") or "")
        for item in cycles
        if isinstance(item.get("actionable_top_candidate"), dict)
    )
    actionable_symbols.pop("", None)
    bias_counter = Counter(
        str(item.get("actionable_top_candidate", {}).get("directional_bias") or "")
        for item in cycles
        if isinstance(item.get("actionable_top_candidate"), dict)
    )
    bias_counter.pop("", None)

    dispatch_attempted = sum(1 for item in cycles if bool(item.get("dispatch_attempted")))
    significant_changes = sum(
        1
        for item in cycles
        if isinstance(item.get("change_gate"), dict) and bool(item["change_gate"].get("significant"))
    )
    closed_trades = []
    total_net_pnl = 0.0
    for journal in journals:
        settlement = journal.get("actual_settlement") if isinstance(journal.get("actual_settlement"), dict) else None
        pnl = None
        if settlement and settlement.get("net_pnl_usdt") is not None:
            try:
                pnl = float(settlement["net_pnl_usdt"])
                total_net_pnl += pnl
            except (TypeError, ValueError):
                pnl = None
        closed_trades.append(
            {
                "symbol": journal.get("symbol"),
                "side": journal.get("side"),
                "closed_at": journal.get("closed_at"),
                "outcome": journal.get("outcome"),
                "net_pnl_usdt": pnl,
            }
        )

    recommendations = build_recommendations(
        cycles=cycles,
        journals=journals,
        jobs=jobs,
        active_worker=active_worker,
    )

    return {
        "generated_at": end.isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "mode": "recommendations_only",
        "state_snapshot": state if isinstance(state, dict) else None,
        "counts": {
            "cycles": len(cycles),
            "significant_changes": significant_changes,
            "dispatch_attempted": dispatch_attempted,
            "closed_trades": len(closed_trades),
            "guardian_jobs_observed": len(jobs),
        },
        "skip_reasons": dict(skip_reasons),
        "actionable_symbols": dict(actionable_symbols),
        "directional_bias_counts": dict(bias_counter),
        "closed_trades": closed_trades,
        "net_pnl_usdt": round(total_net_pnl, 8),
        "active_worker": active_worker,
        "recommendations": recommendations,
    }


def build_markdown(review: dict[str, Any]) -> str:
    counts = review.get("counts") or {}
    lines = [
        "# Phoenix 2h Review",
        "",
        f"- Generated: `{review.get('generated_at')}`",
        f"- Window: `{review.get('window_start')}` → `{review.get('window_end')}`",
        f"- Mode: `{review.get('mode')}`",
        f"- Cycles: `{counts.get('cycles')}`",
        f"- Significant changes: `{counts.get('significant_changes')}`",
        f"- Dispatch attempted: `{counts.get('dispatch_attempted')}`",
        f"- Closed trades: `{counts.get('closed_trades')}`",
        f"- Net PnL in window: `{review.get('net_pnl_usdt')}` USDT",
    ]

    skip_reasons = review.get("skip_reasons") or {}
    if skip_reasons:
        ordered = ", ".join(f"{humanize_reason(reason)}:{count}" for reason, count in sorted(skip_reasons.items()))
        lines.append(f"- Skip reasons: {ordered}")

    actionable = review.get("actionable_symbols") or {}
    if actionable:
        ordered = ", ".join(f"{symbol}:{count}" for symbol, count in sorted(actionable.items(), key=lambda item: item[1], reverse=True))
        lines.append(f"- Actionable leaders: {ordered}")

    bias_counts = review.get("directional_bias_counts") or {}
    if bias_counts:
        ordered = ", ".join(f"{bias}:{count}" for bias, count in sorted(bias_counts.items()))
        lines.append(f"- Directional bias counts: {ordered}")

    active_worker = review.get("active_worker")
    if isinstance(active_worker, dict):
        lines.extend(
            [
                f"- Active live position: `{active_worker.get('symbol')}` `{humanize_side(active_worker.get('side'))}`",
                f"- Active phase: `{active_worker.get('phase')}`",
                f"- Active entry: `{active_worker.get('entry_price')}` x `{active_worker.get('quantity')}`",
            ]
        )

    closed_trades = review.get("closed_trades") or []
    if closed_trades:
        lines.append("")
        lines.append("## Closed Trades")
        for trade in closed_trades:
            pnl = trade.get("net_pnl_usdt")
            pnl_text = f"{pnl:.4f} USDT" if isinstance(pnl, float) else "n/a"
            lines.append(
                f"- `{trade.get('symbol')}` `{trade.get('side')}` `{trade.get('outcome')}` "
                f"`{trade.get('closed_at')}` net `{pnl_text}`"
            )

    recommendations = review.get("recommendations") or []
    if recommendations:
        lines.append("")
        lines.append("## Recommendations")
        for item in recommendations:
            lines.append(f"- {item}")
    skill_digest = load_skill_digest(Path.home() / ".hermes")
    lines.extend(markdown_lines_from_digest(skill_digest))
    return "\n".join(lines) + "\n"


def build_telegram_text(review: dict[str, Any], review_md_path: Path) -> str:
    counts = review.get("counts") or {}
    skip_reasons = review.get("skip_reasons") or {}
    top_skip = None
    if skip_reasons:
        top_skip = sorted(skip_reasons.items(), key=lambda item: item[1], reverse=True)[0]
    recommendations = review.get("recommendations") or []
    actionable = review.get("actionable_symbols") or {}
    leader = None
    if actionable:
        leader = sorted(actionable.items(), key=lambda item: item[1], reverse=True)[0][0]
    active_worker = review.get("active_worker")
    lines = [
        "🧠 凤凰协议 2 小时复盘",
        f"时间窗口：{review.get('window_start')} → {review.get('window_end')}",
        f"循环次数：{counts.get('cycles')} | 显著变化：{counts.get('significant_changes')} | 发起调度：{counts.get('dispatch_attempted')}",
        f"平仓笔数：{counts.get('closed_trades')} | 窗口净利润：{review.get('net_pnl_usdt')} USDT",
    ]
    if leader:
        lines.append(f"最常出现的可执行候选：{leader}")
    if top_skip:
        lines.append(f"主要跳过原因：{humanize_reason(top_skip[0])}（{top_skip[1]} 次）")
    if isinstance(active_worker, dict):
        lines.append(
            f"当前活跃仓位：{active_worker.get('symbol')} {humanize_side(active_worker.get('side'))}，阶段 {active_worker.get('phase')}"
        )
    if recommendations:
        lines.append("建议：")
        for item in recommendations[:3]:
            lines.append(f"- {item}")
    skill_digest = load_skill_digest(Path.home() / ".hermes")
    lines.extend(telegram_lines_from_digest(skill_digest))
    lines.append(f"复盘文件：{review_md_path}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    hermes_home = Path(args.hermes_home).expanduser()
    memories_dir = hermes_home / "memories"
    reviews_dir = memories_dir / "phoenix_reviews"
    reviews_dir.mkdir(parents=True, exist_ok=True)

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=args.window_hours)

    state = load_json_file(memories_dir / "phoenix_state.json")
    cycles_all = load_jsonl(memories_dir / "phoenix_autocycle_history.jsonl")
    cycles = [
        item
        for item in cycles_all
        if in_window(parse_iso(item.get("updated_at")), start, end)
    ]

    journals_all = read_trade_journals(memories_dir / "phoenix_trade_journals")
    journals = [
        item
        for item in journals_all
        if in_window(parse_iso(item.get("closed_at")), start, end)
    ]

    jobs_all = read_guardian_jobs(memories_dir / "phoenix_guardian_workers")
    jobs = [
        item
        for item in jobs_all
        if in_window(parse_iso(item.get("updated_at") or item.get("created_at")), start, end)
        or in_window(parse_iso(item.get("position_closed_at")), start, end)
    ]
    active_worker = latest_running_worker(memories_dir / "phoenix_guardian_workers")

    review = build_review(
        start=start,
        end=end,
        state=state,
        cycles=cycles,
        journals=journals,
        jobs=jobs,
        active_worker=active_worker,
    )
    timestamp = end.strftime("%Y%m%dT%H%M%SZ")
    review_json = reviews_dir / f"{timestamp}.json"
    review_md = reviews_dir / f"{timestamp}.md"
    latest_json = reviews_dir / "latest.json"
    latest_md = reviews_dir / "latest.md"

    markdown = build_markdown(review)
    review_json.write_text(json.dumps(review, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    review_md.write_text(markdown, encoding="utf-8")
    latest_json.write_text(json.dumps(review, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    latest_md.write_text(markdown, encoding="utf-8")

    telegram_error = None
    if args.notify_telegram:
        telegram_error = send_telegram_message(build_telegram_text(review, review_md), hermes_home)

    output = {
        "ok": True,
        "generated_at": review["generated_at"],
        "window_start": review["window_start"],
        "window_end": review["window_end"],
        "review_json": str(review_json),
        "review_md": str(review_md),
        "latest_json": str(latest_json),
        "latest_md": str(latest_md),
        "telegram_error": telegram_error,
        "counts": review["counts"],
        "net_pnl_usdt": review["net_pnl_usdt"],
        "recommendations": review["recommendations"],
    }
    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
