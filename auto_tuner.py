#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_PLAYBOOKS = (
    "oi_build_breakout",
    "liquidation_flush",
)
DEFAULT_INTERVAL_SEC = 2 * 60 * 60
DEFAULT_WINDOW_LINES = 2000
DEFAULT_MIN_SAMPLES_PER_PLAYBOOK = 12
DEFAULT_PAUSE_WIN_RATE_PCT = 40.0
DEFAULT_STOP_LOSS_PCT = 1.3
DEFAULT_TAKE_PROFIT_PCT = 3.5
DEFAULT_SLEEP_CHUNK_SEC = 30.0


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_event(event: str, **payload: Any) -> None:
    print(json.dumps({"event": event, "at": now_iso(), **payload}, ensure_ascii=False), flush=True)


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rolling auto-tuner for Phoenix bridge playbooks.")
    parser.add_argument("--signals-file", type=Path, required=True)
    parser.add_argument("--outcomes-file", type=Path, required=True)
    parser.add_argument("--strategy-file", type=Path, required=True)
    parser.add_argument("--interval-sec", type=float, default=DEFAULT_INTERVAL_SEC)
    parser.add_argument("--window-lines", type=int, default=DEFAULT_WINDOW_LINES)
    parser.add_argument("--min-samples-per-playbook", type=int, default=DEFAULT_MIN_SAMPLES_PER_PLAYBOOK)
    parser.add_argument("--pause-win-rate-pct", type=float, default=DEFAULT_PAUSE_WIN_RATE_PCT)
    parser.add_argument("--default-stop-loss-pct", type=float, default=DEFAULT_STOP_LOSS_PCT)
    parser.add_argument("--default-take-profit-pct", type=float, default=DEFAULT_TAKE_PROFIT_PCT)
    parser.add_argument("--playbooks", default=",".join(DEFAULT_PLAYBOOKS))
    parser.add_argument("--emergency-trigger-file", type=Path, default=None)
    parser.add_argument("--sleep-chunk-sec", type=float, default=DEFAULT_SLEEP_CHUNK_SEC)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def parse_playbooks(raw_value: str) -> list[str]:
    values = [token.strip() for token in str(raw_value or "").split(",") if token.strip()]
    return values or list(DEFAULT_PLAYBOOKS)


def tail_jsonl_records(path: Path, limit: int) -> list[dict[str, Any]]:
    if limit <= 0 or not path.exists():
        return []
    with path.open("rb") as handle:
        handle.seek(0, 2)
        end = handle.tell()
        data = b""
        while end > 0 and data.count(b"\n") <= limit:
            step = min(64 * 1024, end)
            end -= step
            handle.seek(end)
            data = handle.read(step) + data
    rows: list[dict[str, Any]] = []
    for raw_line in data.splitlines()[-limit:]:
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def load_previous_strategy(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_default_playbook_payload(
    playbooks: list[str],
    *,
    stop_loss_pct: float,
    take_profit_pct: float,
) -> dict[str, dict[str, Any]]:
    defaults: dict[str, dict[str, Any]] = {}
    for playbook in playbooks:
        defaults[playbook] = {
            "status": "active",
            "sl": max(0.01, float(stop_loss_pct)),
            "tp": max(0.0, float(take_profit_pct)),
            "sample_count": 0,
            "win_rate_pct": None,
            "avg_after_fee_return_pct": None,
            "selected_branch_id": None,
            "status_reason": "default_bootstrap",
        }
    return defaults


def build_shadow_instance_id(record: dict[str, Any]) -> str:
    event_id = str(record.get("event_id") or "")
    branch_id = str(record.get("shadow_branch_id") or "LEGACY").upper()
    explicit = str(record.get("shadow_instance_id") or "")
    return explicit or f"{event_id}:{branch_id}"


def reduce_latest_outcomes(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_by_instance: dict[str, dict[str, Any]] = {}
    for record in records:
        if str(record.get("event") or "") != "signal_bridge_shadow_horizon_result":
            continue
        instance_id = build_shadow_instance_id(record)
        if not instance_id:
            continue
        horizon_sec = int(record.get("horizon_sec") or 0)
        previous = latest_by_instance.get(instance_id)
        if previous is None or horizon_sec >= int(previous.get("horizon_sec") or 0):
            latest_by_instance[instance_id] = record
    return list(latest_by_instance.values())


def summarize_signal_counts(signal_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in signal_rows:
        if str(row.get("event") or "") != "signal_bridge_shadow_logged":
            continue
        playbook = str(row.get("playbook") or "").strip()
        if playbook:
            counts[playbook] += 1
    return dict(counts)


def summarize_branch_metrics(outcome_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[tuple[str, float, float], dict[str, Any]]] = defaultdict(dict)
    for row in reduce_latest_outcomes(outcome_rows):
        playbook = str(row.get("playbook") or "").strip()
        branch_id = str(row.get("shadow_branch_id") or "LEGACY").upper()
        sl = safe_float(row.get("branch_stop_loss_pct"))
        tp = safe_float(row.get("branch_take_profit_pct"))
        after_fee_return_pct = safe_float(row.get("after_fee_return_pct"))
        if branch_id == "LEGACY":
            continue
        if not playbook or sl is None or tp is None or after_fee_return_pct is None:
            continue
        if float(sl) < 0.1 or float(tp) <= 0.0:
            continue
        key = (branch_id, round(float(sl), 4), round(float(tp), 4))
        bucket = grouped[playbook].setdefault(
            key,
            {
                "branch_id": branch_id,
                "sl": max(0.01, float(sl)),
                "tp": max(0.0, float(tp)),
                "sample_count": 0,
                "win_count": 0,
                "return_total": 0.0,
            },
        )
        bucket["sample_count"] += 1
        bucket["return_total"] += float(after_fee_return_pct)
        if float(after_fee_return_pct) > 0:
            bucket["win_count"] += 1

    summary: dict[str, list[dict[str, Any]]] = {}
    for playbook, branch_map in grouped.items():
        rows: list[dict[str, Any]] = []
        for payload in branch_map.values():
            sample_count = int(payload["sample_count"])
            win_count = int(payload["win_count"])
            avg_return = (float(payload["return_total"]) / sample_count) if sample_count else 0.0
            rows.append(
                {
                    "branch_id": payload["branch_id"],
                    "sl": payload["sl"],
                    "tp": payload["tp"],
                    "sample_count": sample_count,
                    "win_rate_pct": ((win_count / sample_count) * 100.0) if sample_count else 0.0,
                    "avg_after_fee_return_pct": avg_return,
                }
            )
        rows.sort(
            key=lambda item: (
                float(item["avg_after_fee_return_pct"]),
                float(item["win_rate_pct"]),
                int(item["sample_count"]),
            ),
            reverse=True,
        )
        summary[playbook] = rows
    return summary


def select_playbook_payload(
    playbook: str,
    *,
    defaults: dict[str, Any],
    previous_payload: dict[str, Any] | None,
    branch_rows: list[dict[str, Any]],
    recent_signal_count: int,
    min_samples_per_playbook: int,
    pause_win_rate_pct: float,
) -> dict[str, Any]:
    fallback = dict(previous_payload or defaults)
    fallback.setdefault("status", "active")
    fallback.setdefault("sl", defaults["sl"])
    fallback.setdefault("tp", defaults["tp"])
    if not branch_rows:
        fallback.update(
            {
                "sample_count": 0,
                "recent_signal_count": int(recent_signal_count),
                "status_reason": "no_recent_outcomes",
            }
        )
        return fallback
    best = branch_rows[0]
    sample_count = int(best["sample_count"])
    win_rate_pct = float(best["win_rate_pct"])
    status = "active"
    status_reason = "win_rate_ok"
    if sample_count < max(1, int(min_samples_per_playbook)):
        carried = dict(fallback)
        carried.update(
            {
                "sample_count": sample_count,
                "recent_signal_count": int(recent_signal_count),
                "win_rate_pct": win_rate_pct,
                "avg_after_fee_return_pct": float(best["avg_after_fee_return_pct"]),
                "candidate_branch_id": best["branch_id"],
                "status_reason": "insufficient_samples_carry_forward",
            }
        )
        return carried
    elif win_rate_pct < float(pause_win_rate_pct):
        status = "paused"
        status_reason = "win_rate_below_threshold"
    return {
        "status": status,
        "sl": float(best["sl"]),
        "tp": float(best["tp"]),
        "sample_count": sample_count,
        "recent_signal_count": int(recent_signal_count),
        "win_rate_pct": win_rate_pct,
        "avg_after_fee_return_pct": float(best["avg_after_fee_return_pct"]),
        "selected_branch_id": best["branch_id"],
        "status_reason": status_reason,
    }


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


def consume_emergency_trigger(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        raw_text = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        return {"source": str(path), "read_error": str(exc)}
    try:
        path.unlink()
    except OSError as exc:
        emit_event("auto_tuner_emergency_trigger_cleanup_failed", trigger_file=str(path), error=str(exc))
    if not raw_text:
        return {"source": str(path), "reason": "empty_trigger_file"}
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        return {"source": str(path), "raw": raw_text}
    if isinstance(payload, dict):
        payload.setdefault("source", str(path))
        return payload
    return {"source": str(path), "payload": payload}


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    playbooks = parse_playbooks(args.playbooks)
    defaults = build_default_playbook_payload(
        playbooks,
        stop_loss_pct=float(args.default_stop_loss_pct),
        take_profit_pct=float(args.default_take_profit_pct),
    )
    previous_payload = load_previous_strategy(args.strategy_file)
    previous_playbooks = previous_payload.get("playbooks") if isinstance(previous_payload.get("playbooks"), dict) else {}
    signal_rows = tail_jsonl_records(args.signals_file, int(args.window_lines))
    outcome_rows = tail_jsonl_records(args.outcomes_file, int(args.window_lines))
    signal_counts = summarize_signal_counts(signal_rows)
    branch_summary = summarize_branch_metrics(outcome_rows)

    playbook_payloads: dict[str, Any] = {}
    for playbook in playbooks:
        playbook_payloads[playbook] = select_playbook_payload(
            playbook,
            defaults=defaults[playbook],
            previous_payload=(previous_playbooks.get(playbook) if isinstance(previous_playbooks, dict) else None),
            branch_rows=branch_summary.get(playbook, []),
            recent_signal_count=int(signal_counts.get(playbook, 0)),
            min_samples_per_playbook=int(args.min_samples_per_playbook),
            pause_win_rate_pct=float(args.pause_win_rate_pct),
        )

    payload = {
        "generated_at": now_iso(),
        "sample_window": {
            "signals_file": str(args.signals_file),
            "outcomes_file": str(args.outcomes_file),
            "window_lines": int(args.window_lines),
            "min_samples_per_playbook": int(args.min_samples_per_playbook),
            "pause_win_rate_pct": float(args.pause_win_rate_pct),
        },
        "playbooks": playbook_payloads,
    }
    atomic_write_json(args.strategy_file, payload)
    emit_event(
        "auto_tuner_strategy_written",
        strategy_file=str(args.strategy_file),
        playbooks=playbook_payloads,
        window_lines=int(args.window_lines),
    )
    gc.collect()
    return payload


def main() -> int:
    args = parse_args()
    interval_sec = max(60.0, float(args.interval_sec))
    sleep_chunk_sec = max(1.0, float(args.sleep_chunk_sec))
    while True:
        try:
            run_once(args)
        except Exception as exc:  # noqa: BLE001
            emit_event("auto_tuner_iteration_failed", error=str(exc), strategy_file=str(args.strategy_file))
        if args.once:
            break
        wake_deadline = time.monotonic() + interval_sec
        while True:
            trigger_payload = consume_emergency_trigger(args.emergency_trigger_file)
            if trigger_payload is not None:
                emit_event(
                    "auto_tuner_emergency_trigger_consumed",
                    strategy_file=str(args.strategy_file),
                    trigger_file=str(args.emergency_trigger_file),
                    trigger=trigger_payload,
                )
                break
            remaining_sec = wake_deadline - time.monotonic()
            if remaining_sec <= 0:
                break
            time.sleep(min(sleep_chunk_sec, remaining_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
