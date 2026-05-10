#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from deploy.hermes.binance_skill_context import load_skill_digest, markdown_lines_from_digest, telegram_lines_from_digest
from deploy.hermes.hermes_telegram import send_telegram_message
from deploy.hermes.leiting_market_stream_context import (
    filter_recent_ws_rejections,
    market_stream_view,
    market_stream_markdown_lines,
    market_stream_telegram_lines,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hermes digest for Leiting demo candidate monitoring.")
    parser.add_argument("--hermes-home", default=os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes"))
    parser.add_argument("--notify-telegram", action="store_true")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _format(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def _combo_key(reasons: list[Any]) -> str:
    normalized = sorted({str(item).split(" (", 1)[0].strip() for item in reasons if str(item).strip()})
    return " + ".join(item for item in normalized if item)


def _top_ws_combo_counts(
    rejection: dict[str, Any],
    market_stream_state: dict[str, Any] | None,
) -> tuple[int, list[tuple[str, int]], list[tuple[str, int]]]:
    recent_ws = filter_recent_ws_rejections(rejection, market_stream_state)
    if not recent_ws:
        return 0, [], []
    bucketed: dict[str, dict[str, Any]] = {}
    for item in recent_ws:
        bucket_key = str(item.get("decision_bucket_5m") or item.get("snapshot_id") or item.get("timestamp") or "")
        if bucket_key:
            bucketed[bucket_key] = item
    reason_counts: dict[str, int] = {}
    combo_counts: dict[str, int] = {}
    for item in bucketed.values():
        reasons = [str(reason).split(" (", 1)[0].strip() for reason in (item.get("reasons") or []) if str(reason).strip()]
        combo_key = _combo_key(reasons)
        if combo_key:
            combo_counts[combo_key] = combo_counts.get(combo_key, 0) + 1
        for reason in reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    top_reasons = sorted(reason_counts.items(), key=lambda item: item[1], reverse=True)[:5]
    top_combos = sorted(combo_counts.items(), key=lambda item: item[1], reverse=True)[:5]
    return len(bucketed), top_reasons, top_combos


def _top_combo_counts_from_recent(rejection: dict[str, Any]) -> list[tuple[str, int]]:
    recent = rejection.get("recent_rejections")
    if not isinstance(recent, list):
        return []
    bucketed: dict[str, dict[str, Any]] = {}
    for item in recent:
        if not isinstance(item, dict):
            continue
        bucket_key = str(item.get("decision_bucket_5m") or item.get("snapshot_id") or item.get("timestamp") or "")
        if bucket_key:
            bucketed[bucket_key] = item
    combo_counts: dict[str, int] = {}
    for item in bucketed.values():
        combo_key = _combo_key(item.get("reasons") or [])
        if combo_key:
            combo_counts[combo_key] = combo_counts.get(combo_key, 0) + 1
    return sorted(combo_counts.items(), key=lambda item: item[1], reverse=True)[:5]


def build_digest(home: Path) -> tuple[dict[str, Any], str]:
    source_dir = home / "memories" / "btc_demo_v2"
    rejection = load_json(source_dir / "rejection_stats_latest.json") or {}
    first_fill = load_json(source_dir / "first_fill_review.json") or {}
    active_trade = load_json(home / "memories" / "btc_control" / "active_trade.json") or {}
    demo_status = load_json(home / "memories" / "btc_control" / "demo_status.json") or {}
    active_symbol = (
        active_trade.get("symbol")
        or demo_status.get("symbol")
        or demo_status.get("selected_symbol")
        or (first_fill.get("symbol") if isinstance(first_fill, dict) else None)
    )
    market_view = market_stream_view(home, symbol=active_symbol)
    market_stream_state = market_view.get("state") or {}
    market_stream_snapshot = market_view.get("snapshot") or {}
    market_stream_depth = market_view.get("depth") or {}
    ws_bucket_count, ws_top_reasons, ws_top_combos = _top_ws_combo_counts(rejection, market_stream_state)
    review = first_fill.get("journal") or {}
    digest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rejection_present": bool(rejection),
        "first_fill_present": bool(first_fill),
        "first_fill_review_present": bool(first_fill),
        "candidate_name": rejection.get("candidate_name") or first_fill.get("candidate_name"),
        "total_rejections": rejection.get("bucketed_total_rejections", rejection.get("total_rejections", 0)),
        "raw_total_rejections": rejection.get("raw_total_rejections", rejection.get("total_rejections", 0)),
        "counting_mode": rejection.get("counting_mode") or ("decision_bucket_5m" if rejection.get("bucketed_total_rejections") is not None else "raw_snapshots"),
        "top_rejection_reasons": sorted(
            (rejection.get("bucketed_reason_counts") or rejection.get("reason_counts") or {}).items(),
            key=lambda item: item[1],
            reverse=True,
        )[:5],
        "top_rejection_combos": (
            sorted(
                (rejection.get("bucketed_combo_counts") or rejection.get("combo_counts") or {}).items(),
                key=lambda item: item[1],
                reverse=True,
            )[:5]
            or _top_combo_counts_from_recent(rejection)
        ),
        "market_stream": {
            "present": bool(market_stream_state or market_stream_snapshot),
            "symbol": active_symbol,
            "status": market_stream_state.get("status"),
            "started_at": market_stream_state.get("started_at"),
            "last_event_type": market_stream_state.get("last_event_type") or market_stream_snapshot.get("last_event_type"),
            "last_event_at": market_stream_state.get("last_event_at") or market_stream_snapshot.get("last_event_at"),
            "source": market_stream_snapshot.get("source"),
            "spread_bps": market_stream_depth.get("spread_bps"),
            "depth_imbalance": market_stream_depth.get("depth_imbalance"),
            "price_change_5m_pct": market_stream_snapshot.get("price_change_5m_pct"),
            "price_change_1h_pct": market_stream_snapshot.get("price_change_1h_pct"),
        },
        "ws_recent_bucketed_rejections": ws_bucket_count,
        "ws_top_rejection_reasons": ws_top_reasons,
        "ws_top_rejection_combos": ws_top_combos,
        "first_fill": {
            "symbol": first_fill.get("symbol"),
            "side": first_fill.get("side"),
            "regime": first_fill.get("regime"),
            "quote_allocation_usdt": first_fill.get("quote_allocation_usdt"),
            "leverage": first_fill.get("leverage"),
            "net_pnl": review.get("net_pnl"),
            "hold_minutes": review.get("hold_minutes"),
            "close_reason": review.get("reason"),
        },
    }
    lines = [
        "# 雷霆 Demo 策略进化摘要",
        "",
        f"- 生成时间：{digest['generated_at']}",
        f"- 候选策略：`{digest.get('candidate_name') or 'n/a'}`",
        f"- 已接入拒单统计：`{'是' if digest['rejection_present'] else '否'}`",
        f"- 已接入首笔成交复盘：`{'是' if digest['first_fill_present'] else '否'}`",
        f"- 5m 决策级拒单数：`{digest['total_rejections']}`",
        f"- 原始快照拒单数：`{digest['raw_total_rejections']}`",
        f"- 统计口径：`{digest['counting_mode']}`",
        "",
        "## 拒单主因",
    ]
    top_reasons = digest["top_rejection_reasons"]
    if top_reasons:
        lines.extend(f"- {reason}: `{count}`" for reason, count in top_reasons)
    else:
        lines.append("- 暂无")
    lines.extend(["", "## 最卡成交的门槛组合"])
    top_combos = digest["top_rejection_combos"]
    if top_combos:
        lines.extend(f"- {combo}: `{count}`" for combo, count in top_combos)
    else:
        lines.append("- 暂无")
    lines.extend(["", "## WS 实时卡点分析", f"- WS 5m 决策样本：`{digest['ws_recent_bucketed_rejections']}`"])
    ws_reasons = digest["ws_top_rejection_reasons"]
    if ws_reasons:
        lines.extend(f"- WS 主因 {reason}: `{count}`" for reason, count in ws_reasons)
    else:
        lines.append("- 暂无 WS 样本")
    ws_combos = digest["ws_top_rejection_combos"]
    if ws_combos:
        lines.extend(f"- WS 组合 {combo}: `{count}`" for combo, count in ws_combos[:3])
    lines.extend(market_stream_markdown_lines(home, symbol=active_symbol))
    lines.extend(["", "## 首笔成交质量"])
    if digest["first_fill_present"]:
        first = digest["first_fill"]
        lines.extend(
            [
                f"- 标的：`{first.get('symbol')}` / 方向：`{first.get('side')}` / Regime：`{first.get('regime')}`",
                f"- quote allocation：`{first.get('quote_allocation_usdt')}` USDT / 杠杆：`{first.get('leverage')}`x",
                f"- 净盈亏：`{_format(first.get('net_pnl'), 8)}` USDT",
                f"- 持仓分钟：`{_format(first.get('hold_minutes'))}`",
                f"- 平仓原因：`{first.get('close_reason')}`",
            ]
        )
    else:
        lines.append("- 首笔 demo 成交尚未完成平仓。")
    lines.extend(markdown_lines_from_digest(load_skill_digest(home)))
    return digest, "\n".join(lines)


def build_telegram_text(digest: dict[str, Any], markdown_path: Path, home: Path) -> str:
    lines = [
        "雷霆 Demo 策略摘要",
        f"候选策略：{digest.get('candidate_name') or 'n/a'}",
        f"5m决策级拒单：{digest.get('total_rejections', 0)}",
        f"原始快照拒单：{digest.get('raw_total_rejections', 0)}",
    ]
    for reason, count in digest.get("top_rejection_reasons") or []:
        lines.append(f"- {reason}: {count}")
    for combo, count in (digest.get("ws_top_rejection_combos") or [])[:2]:
        lines.append(f"- WS组合 {combo}: {count}")
    first = digest.get("first_fill") or {}
    if first.get("symbol"):
        lines.extend(
            [
                "首笔成交：",
                f"- {first.get('symbol')} {first.get('side')} / {first.get('regime')}",
                f"- 净盈亏：{_format(first.get('net_pnl'), 8)} USDT",
                f"- 持仓：{_format(first.get('hold_minutes'))} 分钟",
                f"- 原因：{first.get('close_reason')}",
            ]
        )
    lines.extend(market_stream_telegram_lines(home, symbol=active_symbol))
    lines.extend(telegram_lines_from_digest(load_skill_digest(home)))
    lines.append(f"摘要文件：{markdown_path}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    home = Path(args.hermes_home)
    out_dir = home / "memories" / "btc_demo_v2"
    out_dir.mkdir(parents=True, exist_ok=True)
    digest, markdown = build_digest(home)
    digest_json = out_dir / "digest_latest.json"
    digest_md = out_dir / "digest_latest.md"
    digest_json.write_text(json.dumps(digest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    digest_md.write_text(markdown, encoding="utf-8")
    telegram_error = None
    if args.notify_telegram:
        telegram_error = send_telegram_message(build_telegram_text(digest, digest_md, home), home)
    print(
        json.dumps(
            {
                "ok": True,
                "digest_json": str(digest_json),
                "digest_md": str(digest_md),
                "telegram_error": telegram_error,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
