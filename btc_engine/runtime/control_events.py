"""Key control-plane events for Leiting -> Hermes notifications."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from btc_engine.config import STATE_DIR
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


EVENTS_JSONL = STATE_DIR / "control_events.jsonl"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_control_event(
    *,
    kind: str,
    title: str,
    details: dict[str, Any] | None = None,
    severity: str = "info",
    dedupe_key: str | None = None,
    notify_remote: bool = True,
) -> dict[str, Any]:
    state = read_runtime_state("control_event_state") or {}
    if dedupe_key and state.get("last_dedupe_key") == dedupe_key:
        return {"emitted": False, "reason": "deduped", "dedupe_key": dedupe_key}
    payload = {
        "event_id": uuid4().hex,
        "emitted_at": _utc_now(),
        "kind": kind,
        "title": title,
        "severity": severity,
        "details": details or {},
        "dedupe_key": dedupe_key,
    }
    EVENTS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS_JSONL.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    update_runtime_state("last_control_event", payload)
    update_runtime_state(
        "control_event_state",
        {
            "last_event_id": payload["event_id"],
            "last_dedupe_key": dedupe_key,
            "last_event_at": payload["emitted_at"],
            "kind": kind,
        },
    )
    from btc_engine.runtime.control_plane import sync_control_plane

    remote_hook_error = sync_control_plane(force=True, notify_remote=notify_remote, reason=f"event:{kind}")
    payload["remote_hook_error"] = remote_hook_error
    update_runtime_state("last_control_event", payload)
    return {"emitted": True, "event": payload}

