"""Archive the current demo_auto run and reset the virtual demo ledger."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from btc_engine.config import DATA_DIR, get_demo_leverage, get_demo_position_fraction, get_demo_start_equity_usdt
from btc_engine.runtime.control_plane import sync_control_plane
from btc_engine.runtime.state_store import delete_runtime_state, read_runtime_state, update_runtime_state


STATE_DIR = DATA_DIR / "state"
JOURNALS_DIR = DATA_DIR / "journals"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _archive_demo_journals(tag: str) -> dict[str, object]:
    archive_dir = JOURNALS_DIR / "archive" / tag
    archive_dir.mkdir(parents=True, exist_ok=True)
    moved: list[str] = []
    for path in sorted(JOURNALS_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("mode") != "demo_auto":
            continue
        target = archive_dir / path.name
        shutil.move(str(path), str(target))
        moved.append(path.name)
    return {
        "archive_dir": str(archive_dir),
        "count": len(moved),
        "files": moved,
    }


def _reset_demo_account(start_equity: float) -> dict[str, object]:
    payload = {
        "mode": "demo_auto",
        "starting_equity_usdt": round(start_equity, 8),
        "equity_usdt": round(start_equity, 8),
        "realized_pnl_usdt": 0.0,
        "closed_trades": 0,
        "position_fraction": get_demo_position_fraction(),
        "leverage": get_demo_leverage(),
        "applied_journal_ids": [],
        "updated_at": _utc_now(),
    }
    update_runtime_state("demo_account", payload)
    return payload


def _reset_runtime_views(start_equity: float, candidate_name: str | None) -> None:
    delete_runtime_state("active_trade")
    delete_runtime_state("last_close")
    delete_runtime_state("demo_last_processed")
    delete_runtime_state("latest_formal_review_demo_auto")
    delete_runtime_state("review_state_demo_auto")
    update_runtime_state(
        "demo_status",
        {
            "generated_at": _utc_now(),
            "ok": True,
            "reason": "demo_ledger_reset",
            "execution_mode": "demo_auto",
            "candidate_name": candidate_name,
            "virtual_equity_usdt": round(start_equity, 8),
            "starting_equity_usdt": round(start_equity, 8),
            "realized_pnl_usdt": 0.0,
            "closed_trades": 0,
            "position_fraction": get_demo_position_fraction(),
            "effective_leverage": get_demo_leverage(),
            "signal": {},
            "market": {},
            "reasons": ["已重置 demo 账本到新的 1000U 起点"],
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset Leiting demo_auto ledger to a fresh virtual balance.")
    parser.add_argument("--start-equity", type=float, default=get_demo_start_equity_usdt())
    parser.add_argument("--tag", default="")
    args = parser.parse_args()

    tag = args.tag.strip() or f"demo_reset_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    strategy_version = read_runtime_state("strategy_version") or {}
    candidate_name = ((strategy_version.get("strategy") or {}).get("name")) or None

    archived = _archive_demo_journals(tag)
    demo_account = _reset_demo_account(args.start_equity)
    _reset_runtime_views(args.start_equity, candidate_name)
    sync_control_plane(force=True, notify_remote=False, reason="demo_reset")

    result = {
        "generated_at": _utc_now(),
        "tag": tag,
        "archived": archived,
        "demo_account": demo_account,
        "candidate_name": candidate_name,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
