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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hermes digest for Leiting formal reviews.")
    parser.add_argument("--hermes-home", default=os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes"))
    parser.add_argument("--notify-telegram", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def format_float(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def build_digest(review: dict[str, Any], review_dir: Path) -> tuple[dict[str, Any], str]:
    metrics = review.get("metrics") or {}
    recommendations = review.get("recommendations") or []
    snapshot = review.get("latest_engine_snapshot") or {}
    signal = snapshot.get("signal") or {}
    market = snapshot.get("market") or {}
    digest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_index": review.get("batch_index"),
        "batch_size": review.get("batch_size"),
        "window_trade_count": metrics.get("window_trade_count"),
        "net_pnl": metrics.get("net_pnl"),
        "win_rate": metrics.get("win_rate"),
        "profit_factor": metrics.get("profit_factor"),
        "avg_hold_minutes": metrics.get("avg_hold_minutes"),
        "max_drawdown": metrics.get("max_drawdown"),
        "engine_bias": signal.get("bias"),
        "engine_regime": signal.get("regime"),
        "execution_quality_score": signal.get("execution_quality_score"),
        "event_risk_score": signal.get("event_risk_score"),
        "mark_price": market.get("mark_price"),
        "funding_rate": market.get("funding_rate"),
        "recommendations": recommendations,
        "review_file": str(review_dir / "formal_latest.md"),
    }
    skill_digest = load_skill_digest(review_dir.parent.parent)
    lines = [
        "# 雷霆正式复盘摘要",
        "",
        f"- 生成时间：{digest['generated_at']}",
        f"- 批次：第 `{digest['batch_index']}` 批 / 每批 `{digest['batch_size']}` 单",
        f"- 样本交易数：`{digest['window_trade_count']}`",
        f"- 净盈亏：`{format_float(digest['net_pnl'], 8)}` USDT",
        f"- 胜率：`{format_float(digest['win_rate'], 4)}`",
        f"- 盈亏比：`{format_float(digest['profit_factor'], 4)}`",
        f"- 平均持仓：`{format_float(digest['avg_hold_minutes'], 4)}` 分钟",
        f"- 最大回撤：`{format_float(digest['max_drawdown'], 8)}` USDT",
        f"- 当前引擎方向：`{digest['engine_bias']}` / 阶段：`{digest['engine_regime']}`",
        f"- 当前执行质量分：`{format_float(digest['execution_quality_score'], 4)}`",
        f"- 当前事件风险分：`{format_float(digest['event_risk_score'], 4)}`",
        f"- 当前标记价：`{format_float(digest['mark_price'], 4)}`",
        f"- 当前资金费率：`{format_float(digest['funding_rate'], 8)}`",
        "",
        "## 建议",
    ]
    lines.extend(f"- {item}" for item in recommendations)
    lines.extend(markdown_lines_from_digest(skill_digest))
    markdown = "\n".join(lines)
    return digest, markdown


def build_telegram_text(digest: dict[str, Any], markdown_path: Path) -> str:
    recommendations = digest.get("recommendations") or []
    lines = [
        "雷霆正式复盘已生成",
        f"批次：第 {digest.get('batch_index')} 批 / 每批 {digest.get('batch_size')} 单",
        f"净盈亏：{format_float(digest.get('net_pnl'), 8)} USDT",
        f"胜率：{format_float(digest.get('win_rate'), 4)}",
        f"盈亏比：{format_float(digest.get('profit_factor'), 4)}",
        f"平均持仓：{format_float(digest.get('avg_hold_minutes'), 4)} 分钟",
        f"最大回撤：{format_float(digest.get('max_drawdown'), 8)} USDT",
        f"当前引擎：{digest.get('engine_bias')} / {digest.get('engine_regime')}",
        f"执行质量分：{format_float(digest.get('execution_quality_score'), 4)}",
    ]
    if recommendations:
        lines.append("建议：")
        lines.extend(f"- {item}" for item in recommendations[:3])
    skill_digest = load_skill_digest(markdown_path.parent.parent.parent)
    lines.extend(telegram_lines_from_digest(skill_digest))
    lines.append(f"摘要文件：{markdown_path}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    hermes_home = Path(args.hermes_home)
    reviews_dir = hermes_home / "memories" / "btc_reviews"
    review_path = reviews_dir / "formal_latest.json"
    review = load_json(review_path)
    if not isinstance(review, dict):
        print(json.dumps({"ok": False, "reason": "formal_latest_missing"}, ensure_ascii=False, indent=2))
        return

    state_path = reviews_dir / "formal_digest_state.json"
    state = load_json(state_path) or {}
    batch_index = int(review.get("batch_index", 0) or 0)
    last_notified_batch = int(state.get("last_notified_batch", 0) or 0)

    digest, markdown = build_digest(review, reviews_dir)
    digest_json_path = reviews_dir / "formal_digest_latest.json"
    digest_md_path = reviews_dir / "formal_digest_latest.md"
    digest_json_path.write_text(json.dumps(digest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    digest_md_path.write_text(markdown, encoding="utf-8")

    telegram_error = None
    should_notify = args.notify_telegram and (args.force or batch_index > last_notified_batch)
    if should_notify:
        telegram_error = send_telegram_message(build_telegram_text(digest, digest_md_path), hermes_home)

    state.update(
        {
            "last_seen_batch": batch_index,
            "last_notified_batch": batch_index if should_notify and telegram_error is None else last_notified_batch,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "telegram_error": telegram_error,
        }
    )
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "batch_index": batch_index,
                "notified": should_notify and telegram_error is None,
                "telegram_error": telegram_error,
                "digest_json": str(digest_json_path),
                "digest_md": str(digest_md_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
