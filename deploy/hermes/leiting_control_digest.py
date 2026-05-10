#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from deploy.hermes.hermes_telegram import send_telegram_message
from deploy.hermes.binance_skill_context import load_skill_digest, markdown_lines_from_digest, telegram_lines_from_digest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hermes control digest for Leiting.")
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


def build_digest(home: Path) -> tuple[dict[str, Any], str]:
    root = home / "memories" / "btc_control"
    active_trade = load_json(root / "active_trade.json") or {}
    demo_status = load_json(root / "demo_status.json") or {}
    market_stream_state = load_json(root / "market_stream_state.json") or {}
    user_stream_state = load_json(root / "user_stream_state.json") or {}
    strategy_version = load_json(root / "strategy_version.json") or {}
    last_close = load_json(root / "last_close.json") or {}
    last_event = load_json(root / "last_control_event.json") or {}
    skill_digest = load_skill_digest(home) or {}
    strategy = strategy_version.get("strategy") or {}
    signal = demo_status.get("signal") or {}
    market = demo_status.get("market") or {}
    current_strategy_name = strategy.get("name") or demo_status.get("candidate_name")
    current_execution_mode = strategy_version.get("execution_mode") or demo_status.get("execution_mode")
    current_effective_leverage = strategy_version.get("effective_leverage")
    if current_effective_leverage is None:
        current_effective_leverage = demo_status.get("effective_leverage")
    current_equity = demo_status.get("virtual_equity_usdt")
    if current_equity is None:
        demo_account = demo_status.get("demo_account") or {}
        current_equity = demo_account.get("equity_usdt")
    digest = {
        "generated_at": demo_status.get("generated_at") or active_trade.get("updated_at") or strategy_version.get("generated_at"),
        "strategy_version": strategy_version,
        "active_trade": active_trade,
        "demo_status": demo_status,
        "market_stream_state": market_stream_state,
        "user_stream_state": user_stream_state,
        "last_close": last_close,
        "last_event": last_event,
        "skill_digest": skill_digest,
        "has_open_trade": active_trade.get("status") == "open",
        "strategy_name": current_strategy_name,
        "execution_mode": current_execution_mode,
        "effective_leverage": current_effective_leverage,
        "demo_equity": current_equity,
        "selected_symbol": demo_status.get("selected_symbol"),
        "scan_symbol": demo_status.get("scan_symbol"),
        "active_trade_status": active_trade.get("status"),
        "active_trade_symbol": active_trade.get("symbol"),
    }
    lines = [
        "# 雷霆控制面摘要",
        "",
        f"- 生成时间：{digest.get('generated_at') or 'n/a'}",
        f"- 当前策略：`{current_strategy_name or 'n/a'}`",
        f"- 执行模式：`{current_execution_mode or 'n/a'}` / 当前权益：`{current_equity}` USDT / 杠杆：`{current_effective_leverage}`x",
        f"- 市场流：`{market_stream_state.get('status') or 'n/a'}` / 用户流：`{user_stream_state.get('status') or 'n/a'}`",
        f"- 当前选中：`{demo_status.get('selected_symbol') or 'n/a'}` / 扫描标的：`{demo_status.get('scan_symbol') or 'n/a'}` / Bias：`{signal.get('bias') or 'n/a'}` / 置信度：`{signal.get('directional_confidence')}`",
        f"- 当前点差：`{market.get('spread_bps')}` bps / 盘口失衡：`{market.get('depth_imbalance')}`",
        "",
        "## 当前仓位",
    ]
    if digest["has_open_trade"]:
        plan = active_trade.get("plan") or {}
        lines.extend(
            [
                f"- 标的：`{active_trade.get('symbol')}` / 方向：`{active_trade.get('side')}` / 阶段：`{active_trade.get('phase')}`",
                f"- 开仓：`{active_trade.get('entry_avg_price')}` / 数量：`{plan.get('quantity')}` / 杠杆：`{plan.get('leverage')}`x",
                f"- 初始止损：`{plan.get('initial_stop_price')}` / 当前止损：`{active_trade.get('current_stop_trigger_price')}`",
                f"- 估算净 ROI：`{active_trade.get('estimated_net_roi_pct')}`%",
            ]
        )
    else:
        lines.append("- 当前无活跃仓位。")
    lines.extend(["", "## 最新事件"])
    if last_event:
        lines.extend(
            [
                f"- 事件：`{last_event.get('kind')}` / 等级：`{last_event.get('severity')}`",
                f"- 标题：{last_event.get('title')}",
            ]
        )
    else:
        lines.append("- 暂无。")
    if last_close:
        lines.extend(
            [
                "",
                "## 最近平仓",
                f"- 标的：`{last_close.get('symbol')}` / 方向：`{last_close.get('side')}`",
                f"- 净盈亏：`{last_close.get('net_pnl')}` USDT / 原因：`{last_close.get('reason')}`",
            ]
        )
    lines.extend(markdown_lines_from_digest(skill_digest))
    markdown = "\n".join(lines)
    return digest, markdown


def build_telegram_text(digest: dict[str, Any], markdown_path: Path) -> str:
    last_event = digest.get("last_event") or {}
    active_trade = digest.get("active_trade") or {}
    strategy = (digest.get("strategy_version") or {}).get("strategy") or {}
    lines = [
        "雷霆事件通知",
        f"策略：{strategy.get('name') or digest.get('demo_status', {}).get('candidate_name') or 'n/a'}",
        f"事件：{last_event.get('title') or 'n/a'}",
    ]
    details = last_event.get("details") or {}
    if details.get("symbol"):
        lines.append(f"标的：{details.get('symbol')} {details.get('side') or ''}".strip())
    if active_trade.get("status") == "open":
        lines.append(f"当前仓位：{active_trade.get('symbol')} {active_trade.get('side')} @ {active_trade.get('entry_avg_price')}")
        lines.append(f"当前止损：{active_trade.get('current_stop_trigger_price')}")
    lines.extend(telegram_lines_from_digest(digest.get("skill_digest")))
    lines.append(f"摘要：{markdown_path}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    home = Path(args.hermes_home)
    out_dir = home / "memories" / "btc_control"
    out_dir.mkdir(parents=True, exist_ok=True)
    digest, markdown = build_digest(home)
    digest_json = out_dir / "digest_latest.json"
    digest_md = out_dir / "digest_latest.md"
    digest_state = load_json(out_dir / "digest_state.json") or {}
    latest_event = digest.get("last_event") or {}
    digest_json.write_text(json.dumps(digest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    digest_md.write_text(markdown, encoding="utf-8")
    telegram_error = None
    should_notify = False
    if args.notify_telegram and latest_event.get("event_id") and latest_event.get("event_id") != digest_state.get("last_notified_event_id"):
        should_notify = True
        telegram_error = send_telegram_message(build_telegram_text(digest, digest_md), home)
    out_dir.joinpath("digest_state.json").write_text(
        json.dumps(
            {
                "last_notified_event_id": latest_event.get("event_id") if should_notify and telegram_error is None else digest_state.get("last_notified_event_id"),
                "last_notified_at": latest_event.get("emitted_at") if should_notify and telegram_error is None else digest_state.get("last_notified_at"),
                "telegram_error": telegram_error,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
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
