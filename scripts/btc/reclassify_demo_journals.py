#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from btc_engine.execution.binance_signed import BinanceSignedFuturesClient

TARGET_REASONS = {"manual_or_untracked_close", "position_missing_on_exchange"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reclassify historical demo_auto journal close reasons.")
    parser.add_argument("--journals-dir", default="/opt/leiting-btc/btc_data/journals")
    parser.add_argument("--events-path", default="/opt/leiting-btc/btc_data/state/user_stream_events.jsonl")
    parser.add_argument("--write", action="store_true")
    return parser.parse_args()


def load_events(events_path: Path) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    by_order: dict[str, list[dict[str, Any]]] = {}
    by_trigger: dict[str, list[dict[str, Any]]] = {}
    if not events_path.exists():
        return by_order, by_trigger
    for line in events_path.read_text(encoding="utf-8").splitlines():
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        order_id = str(payload.get("order_id") or "").strip()
        if order_id:
            by_order.setdefault(order_id, []).append(payload)
        triggered_order_id = str(payload.get("triggered_order_id") or "").strip()
        if triggered_order_id:
            by_trigger.setdefault(triggered_order_id, []).append(payload)
    return by_order, by_trigger


def infer_from_events(close_order_id: str, by_order: dict[str, list[dict[str, Any]]], by_trigger: dict[str, list[dict[str, Any]]]) -> tuple[str | None, str | None]:
    matched = (by_order.get(close_order_id) or []) + (by_trigger.get(close_order_id) or [])
    for event in matched:
        client_algo_id = str(event.get("client_algo_id") or "")
        client_order_id = str(event.get("client_order_id") or "")
        if client_algo_id.startswith("lt_stop_") or client_order_id.startswith("lt_stop_"):
            return "initial_stop_triggered", client_algo_id or client_order_id
        if client_order_id.startswith("lt_close_"):
            return "system_close_order", client_order_id
    return None, None


def infer_from_order_query(client: BinanceSignedFuturesClient, symbol: str, close_order_id: int) -> tuple[str | None, str | None]:
    try:
        payload = client.get_order(symbol, order_id=close_order_id)
    except Exception:
        return None, None
    if not isinstance(payload, dict):
        return None, None
    client_order_id = str(payload.get("clientOrderId") or "")
    if client_order_id.startswith("lt_stop_"):
        return "initial_stop_triggered", client_order_id
    if client_order_id.startswith("lt_close_"):
        return "system_close_order", client_order_id
    return None, client_order_id or None


def main() -> None:
    args = parse_args()
    journals_dir = Path(args.journals_dir)
    events_path = Path(args.events_path)
    by_order, by_trigger = load_events(events_path)
    client = BinanceSignedFuturesClient()
    summary: Counter[str] = Counter()
    changed: list[tuple[str, str, str, str | None]] = []

    for path in sorted(journals_dir.glob("*.json")):
        try:
            journal = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if journal.get("mode") != "demo_auto":
            continue
        old_reason = str(journal.get("reason") or "")
        if old_reason not in TARGET_REASONS:
            continue
        symbol = str(journal.get("symbol") or "")
        close_order_id = journal.get("close_order_id")
        if not symbol or not close_order_id:
            summary["skipped_missing_ids"] += 1
            continue
        new_reason, source_id = infer_from_events(str(close_order_id), by_order, by_trigger)
        source = "events"
        if new_reason is None:
            new_reason, source_id = infer_from_order_query(client, symbol, int(close_order_id))
            source = "order_query"
        if new_reason is None or new_reason == old_reason:
            summary["unchanged"] += 1
            continue
        summary[f"reclassified_to_{new_reason}"] += 1
        changed.append((path.name, old_reason, new_reason, source_id))
        journal["reason"] = new_reason
        journal["reason_reclassified_from"] = old_reason
        journal["reason_reclassified_at"] = source
        if source_id:
            journal["close_client_order_id"] = source_id
        if args.write:
            path.write_text(json.dumps(journal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({"summary": dict(summary), "changed": changed[:20], "write": args.write}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
