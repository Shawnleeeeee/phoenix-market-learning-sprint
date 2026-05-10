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
    parser = argparse.ArgumentParser(description="Build Hermes digest for Leiting research summary.")
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


def build_digest(review: dict[str, Any], research_dir: Path, hermes_home: Path) -> tuple[dict[str, Any], str]:
    recommendations = review.get("recommendations") or []
    digest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_generated_at": review.get("generated_at"),
        "dataset_rows": review.get("dataset_rows") or review.get("dataset", {}).get("rows") or review.get("dataset", {}).get("row_count"),
        "feature_count": review.get("feature_count") or review.get("training", {}).get("feature_count"),
        "profile": review.get("profile") or review.get("training", {}).get("research_profile"),
        "allowed_regimes": review.get("allowed_regimes") or review.get("training", {}).get("allowed_regimes") or [],
        "baseline_net_return_pct": review.get("baseline_net_return_pct"),
        "walk_forward_net_return_pct": review.get("walk_forward_net_return_pct"),
        "walk_forward_trade_count": review.get("walk_forward_trade_count"),
        "selected_threshold": review.get("selected_threshold"),
        "review_file": str(research_dir / "latest.md"),
        "recommendations": recommendations,
    }
    skill_digest = load_skill_digest(hermes_home)
    lines = [
        "# 雷霆研究摘要",
        "",
        f"- 生成时间：{digest['generated_at']}",
        f"- 研究生成时间：`{digest.get('source_generated_at') or 'n/a'}`",
        f"- 数据集行数：`{digest.get('dataset_rows') or 'n/a'}`",
        f"- 特征数量：`{digest.get('feature_count') or 'n/a'}`",
        f"- 研究档位：`{digest.get('profile') or 'n/a'}`",
        f"- 允许交易 Regime：`{', '.join(digest.get('allowed_regimes') or []) or '全部'}`",
        f"- Baseline 净收益：`{format_float(digest.get('baseline_net_return_pct'), 6)}`%",
        f"- Walk-forward 净收益：`{format_float(digest.get('walk_forward_net_return_pct'), 6)}`%",
        f"- Walk-forward 交易数：`{digest.get('walk_forward_trade_count') if digest.get('walk_forward_trade_count') is not None else 'n/a'}`",
        f"- 当前选中阈值：`{format_float(digest.get('selected_threshold'), 4)}`",
        "",
        "## 建议",
    ]
    lines.extend(f"- {item}" for item in recommendations)
    lines.extend(markdown_lines_from_digest(skill_digest))
    return digest, "\n".join(lines)


def build_telegram_text(digest: dict[str, Any], markdown_path: Path, hermes_home: Path) -> str:
    recommendations = digest.get("recommendations") or []
    lines = [
        "雷霆研究摘要已更新",
        f"研究档位：{digest.get('profile') or 'n/a'}",
        f"允许 Regime：{', '.join(digest.get('allowed_regimes') or []) or '全部'}",
        f"Baseline 净收益：{format_float(digest.get('baseline_net_return_pct'), 6)}%",
        f"Walk-forward 净收益：{format_float(digest.get('walk_forward_net_return_pct'), 6)}%",
        f"Walk-forward 交易数：{digest.get('walk_forward_trade_count') if digest.get('walk_forward_trade_count') is not None else 'n/a'}",
        f"当前阈值：{format_float(digest.get('selected_threshold'), 4)}",
    ]
    if recommendations:
        lines.append("建议：")
        lines.extend(f"- {item}" for item in recommendations[:3])
    lines.extend(telegram_lines_from_digest(load_skill_digest(hermes_home)))
    lines.append(f"摘要文件：{markdown_path}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    hermes_home = Path(args.hermes_home)
    research_dir = hermes_home / "memories" / "btc_research"
    review_path = research_dir / "latest.json"
    review = load_json(review_path)
    if not isinstance(review, dict):
        print(json.dumps({"ok": False, "reason": "research_latest_missing"}, ensure_ascii=False, indent=2))
        return

    digest, markdown = build_digest(review, research_dir, hermes_home)
    digest_json_path = research_dir / "digest_latest.json"
    digest_md_path = research_dir / "digest_latest.md"
    digest_json_path.write_text(json.dumps(digest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    digest_md_path.write_text(markdown, encoding="utf-8")

    state_path = research_dir / "digest_state.json"
    state = load_json(state_path) or {}
    source_generated_at = review.get("generated_at")
    last_notified_source_generated_at = state.get("last_notified_source_generated_at")
    telegram_error = None
    should_notify = args.notify_telegram and (args.force or source_generated_at != last_notified_source_generated_at)
    if should_notify:
        telegram_error = send_telegram_message(build_telegram_text(digest, digest_md_path, hermes_home), hermes_home)

    state.update(
        {
            "last_seen_source_generated_at": source_generated_at,
            "last_notified_source_generated_at": source_generated_at if should_notify and telegram_error is None else last_notified_source_generated_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "telegram_error": telegram_error,
        }
    )
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
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
