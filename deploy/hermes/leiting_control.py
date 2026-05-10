#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from deploy.hermes.binance_skill_context import load_skill_digest, markdown_lines_from_digest
from deploy.hermes.hermes_telegram import telegram_settings
from deploy.hermes.leiting_skill_digest import build_digest as build_skill_digest, build_square_preview


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hermes control surface for Leiting.")
    parser.add_argument("--hermes-home", default=str(Path.home() / ".hermes"))
    parser.add_argument("--format", choices=("json", "text"), default="json")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    sub.add_parser("position")
    sub.add_parser("review")
    sub.add_parser("research")
    p_square = sub.add_parser("square-preview")
    p_square.add_argument("--symbol")
    p_flatten = sub.add_parser("flatten")
    p_flatten.add_argument("--symbol")
    sub.add_parser("pause")
    sub.add_parser("resume")
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _json_or_text(payload: dict[str, Any], text: str, fmt: str) -> None:
    if fmt == "text":
        print(text)
        return
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _status_text(home: Path, digest: dict[str, Any]) -> str:
    md_path = home / "memories" / "btc_control" / "digest_latest.md"
    if md_path.exists():
        return md_path.read_text(encoding="utf-8")
    strategy = (digest.get("strategy_version") or {}).get("name") or "unknown"
    market_stream = (digest.get("market_stream") or {}).get("status") or "unknown"
    user_stream = (digest.get("user_stream") or {}).get("status") or "unknown"
    active_trade = digest.get("active_trade") or {}
    if active_trade:
        active_line = f"{active_trade.get('symbol')} {active_trade.get('side')} @ {active_trade.get('entry_avg_price')}"
    else:
        active_line = "无活动仓位"
    return "\n".join(
        [
            "雷霆状态",
            f"- 策略版本：`{strategy}`",
            f"- 市场流：`{market_stream}`",
            f"- 用户流：`{user_stream}`",
            f"- 当前仓位：{active_line}",
        ]
    )


def _position_text(active: dict[str, Any]) -> str:
    if not active:
        return "雷霆当前无活动仓位。"
    plan = active.get("plan") or {}
    quantity = active.get("quantity")
    if quantity is None:
        quantity = active.get("position_amt")
    if quantity is None:
        quantity = plan.get("quantity")
    leverage = active.get("leverage")
    if leverage is None:
        leverage = plan.get("leverage")
    current_stop = active.get("current_stop_price")
    if current_stop is None:
        current_stop = active.get("current_stop_trigger_price")
    lines = [
        "雷霆当前仓位",
        f"- 标的：`{active.get('symbol')}`",
        f"- 方向：`{active.get('side')}`",
        f"- 阶段：`{active.get('phase') or 'unknown'}`",
        f"- 入场均价：`{active.get('entry_avg_price')}`",
        f"- 数量：`{quantity}`",
        f"- 杠杆：`{leverage}`",
    ]
    if current_stop is not None:
        lines.append(f"- 当前止损：`{current_stop}`")
    if active.get("estimated_net_roi_pct") is not None:
        lines.append(f"- 预估净 ROI：`{active.get('estimated_net_roi_pct')}`")
    if active.get("candidate_name"):
        lines.append(f"- 策略：`{active.get('candidate_name')}`")
    return "\n".join(lines)


def _append_external_reference(home: Path, text: str) -> str:
    skill_digest = load_skill_digest(home)
    extra_lines = markdown_lines_from_digest(skill_digest)
    if not extra_lines:
        return text
    if "## 外部市场参考" in text:
        return text
    suffix = "\n".join(extra_lines).strip()
    if not suffix:
        return text
    return f"{text.rstrip()}\n\n{suffix}\n"


def _review_text(home: Path) -> str:
    review_md = home / "memories" / "btc_reviews" / "latest.md"
    if not review_md.exists():
        return "missing"
    return _append_external_reference(home, review_md.read_text(encoding="utf-8"))


def _research_text(home: Path) -> str:
    digest_md = home / "memories" / "btc_research" / "digest_latest.md"
    if digest_md.exists():
        return digest_md.read_text(encoding="utf-8")
    research_md = home / "memories" / "btc_research" / "latest.md"
    if not research_md.exists():
        return "missing"
    return _append_external_reference(home, research_md.read_text(encoding="utf-8"))


def _remote_result_text(action: str, result: dict[str, Any]) -> str:
    if result.get("ok"):
        stdout = (result.get("stdout") or "").strip()
        if stdout:
            return f"雷霆{action}已执行。\n\n{stdout}"
        return f"雷霆{action}已执行。"
    stderr = (result.get("stderr") or result.get("stdout") or "").strip() or "unknown error"
    return f"雷霆{action}失败：`{stderr[:600]}`"


def _square_preview_text(home: Path, *, symbol: str | None = None) -> str:
    digest, digest_markdown = build_skill_digest(home)
    skills_dir = home / "memories" / "binance_skills"
    skills_dir.mkdir(parents=True, exist_ok=True)
    (skills_dir / "digest_latest.json").write_text(json.dumps(digest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (skills_dir / "digest_latest.md").write_text(digest_markdown, encoding="utf-8")

    normalized_symbol = (symbol or "").strip().upper() or None
    preview, preview_markdown = build_square_preview(digest, symbol=normalized_symbol)
    square_dir = home / "memories" / "binance_square"
    square_dir.mkdir(parents=True, exist_ok=True)
    (square_dir / "preview_latest.json").write_text(json.dumps(preview, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (square_dir / "preview_latest.md").write_text(preview_markdown, encoding="utf-8")
    (square_dir / "status.json").write_text(
        json.dumps(
            {
                "enabled": False,
                "mode": "preview_only",
                "generated_at": preview.get("generated_at"),
                "symbol": normalized_symbol,
                "reason": "preview_generated_on_demand",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return "\n".join(
        [
            "雷霆 Binance Square 预览",
            "仅生成草稿，不会自动发布。",
            "",
            preview.get("text") or "n/a",
        ]
    )


def _resolve_remote(home: Path) -> tuple[str, str]:
    settings = telegram_settings(home)
    target = (
        (settings.get("relay_target") or "").strip()
        or "root@43.98.174.32"
    )
    key_path = (settings.get("relay_key_path") or "").strip()
    return target, key_path


def _run_remote(home: Path, remote_cmd: str) -> dict[str, Any]:
    target, key_path = _resolve_remote(home)
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no"]
    if key_path:
        cmd.extend(["-i", key_path])
    cmd.extend([target, remote_cmd])
    completed = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "ok": completed.returncode == 0,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "returncode": completed.returncode,
    }


def main() -> None:
    args = parse_args()
    home = Path(args.hermes_home)
    mem = home / "memories"
    control = mem / "btc_control"
    if args.command == "status":
        digest = _read_json(control / "digest_latest.json") or {}
        _json_or_text(digest, _status_text(home, digest), args.format)
        return
    if args.command == "position":
        active = _read_json(control / "active_trade.json") or {}
        _json_or_text(active, _position_text(active), args.format)
        return
    if args.command == "review":
        print(_review_text(home))
        return
    if args.command == "research":
        print(_research_text(home))
        return
    if args.command == "square-preview":
        print(_square_preview_text(home, symbol=args.symbol))
        return
    if args.command == "flatten":
        active = _read_json(control / "active_trade.json") or {}
        symbol = args.symbol or active.get("symbol") or "BTCUSDT"
        result = _run_remote(home, f"cd /opt/leiting-btc && ./.venv/bin/python -m btc_engine.execution.router flatten --symbol {symbol} --reason hermes_manual_flatten")
        _json_or_text(result, _remote_result_text("平仓", result), args.format)
        return
    if args.command == "pause":
        result = _run_remote(home, "systemctl stop leiting-btc-demo-auto.service")
        _json_or_text(result, _remote_result_text("暂停", result), args.format)
        return
    if args.command == "resume":
        result = _run_remote(home, "systemctl start leiting-btc-demo-auto.service")
        _json_or_text(result, _remote_result_text("恢复", result), args.format)
        return


if __name__ == "__main__":
    main()
