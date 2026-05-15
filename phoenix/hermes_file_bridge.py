from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix.risk_governor import append_jsonl

DEFAULT_HERMES_INBOX = Path("/opt/phoenix-testnet/hermes_inbox")
DEFAULT_HERMES_OUTBOX = Path("/opt/phoenix-testnet/hermes_outbox")
DEFAULT_HERMES_ARCHIVE = Path("/opt/phoenix-testnet/hermes_archive")
DEFAULT_HERMES_LOGS = Path("/opt/phoenix-testnet/hermes_logs")


@dataclass(frozen=True, slots=True)
class HermesFileBridge:
    inbox_dir: Path = DEFAULT_HERMES_INBOX
    outbox_dir: Path = DEFAULT_HERMES_OUTBOX
    archive_dir: Path = DEFAULT_HERMES_ARCHIVE
    log_dir: Path = DEFAULT_HERMES_LOGS

    def __init__(
        self,
        *,
        inbox_dir: str | Path = DEFAULT_HERMES_INBOX,
        outbox_dir: str | Path = DEFAULT_HERMES_OUTBOX,
        archive_dir: str | Path = DEFAULT_HERMES_ARCHIVE,
        log_dir: str | Path = DEFAULT_HERMES_LOGS,
    ) -> None:
        object.__setattr__(self, "inbox_dir", Path(inbox_dir))
        object.__setattr__(self, "outbox_dir", Path(outbox_dir))
        object.__setattr__(self, "archive_dir", Path(archive_dir))
        object.__setattr__(self, "log_dir", Path(log_dir))

    def ensure_dirs(self) -> None:
        for directory in (self.inbox_dir, self.outbox_dir, self.archive_dir, self.log_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def snapshot_path(self, trace_id: str) -> Path:
        return self.inbox_dir / f"snapshot_{trace_id}.json"

    def decision_path(self, trace_id: str) -> Path:
        return self.outbox_dir / f"decision_{trace_id}.json"

    def write_snapshot(self, trace_id: str, snapshot: dict[str, Any]) -> Path:
        self.ensure_dirs()
        payload = _snapshot_file_payload(trace_id, snapshot)
        path = self.snapshot_path(trace_id)
        path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        self.log_event(
            "snapshot_written",
            trace_id,
            {
                "snapshot_path": str(path),
                **snapshot_metadata(snapshot),
            },
        )
        return path

    def archive_trace(self, trace_id: str) -> dict[str, str | None]:
        self.ensure_dirs()
        archived: dict[str, str | None] = {"snapshot_archive_path": None, "decision_archive_path": None}
        for kind, source in (("snapshot", self.snapshot_path(trace_id)), ("decision", self.decision_path(trace_id))):
            if not source.exists():
                continue
            target = self.archive_dir / f"{kind}_{trace_id}.json"
            shutil.copy2(source, target)
            archived[f"{kind}_archive_path"] = str(target)
        self.log_event("trace_archived", trace_id, archived)
        return archived

    def log_event(self, event: str, trace_id: str, payload: dict[str, Any] | None = None) -> None:
        self.ensure_dirs()
        append_jsonl(
            self.log_dir / "hermes_file_bridge_events.jsonl",
            {
                "event": event,
                "trace_id": trace_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                **(payload or {}),
            },
        )


def snapshot_metadata(snapshot: dict[str, Any]) -> dict[str, Any]:
    status = snapshot.get("system_status") if isinstance(snapshot, dict) else {}
    status = status if isinstance(status, dict) else {}
    return {
        "snapshot_source": status.get("snapshot_source") or status.get("source"),
        "trusted_runtime_snapshot": bool(status.get("trusted_runtime_snapshot", False)),
        "data_fresh": bool(status.get("data_fresh", False)),
        "websocket_status": status.get("websocket_status"),
        "exchange_status": status.get("exchange_status"),
        "account_state_source": status.get("account_state_source") or status.get("account_source"),
        "position_state_source": status.get("position_state_source"),
        "protective_stop_capability_source": status.get("protective_stop_capability_source"),
        "emergency_close_capability_source": status.get("emergency_close_capability_source"),
        "protective_stop_path_available": status.get("protective_stop_path_available"),
        "emergency_close_available": status.get("emergency_close_available"),
        "freeze_reason": status.get("freeze_reason"),
    }


def attach_trace_id(snapshot: dict[str, Any], trace_id: str) -> None:
    snapshot["trace_id"] = trace_id
    status = snapshot.setdefault("system_status", {})
    if isinstance(status, dict):
        status["trace_id"] = trace_id


def _snapshot_file_payload(trace_id: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    payload = {"trace_id": trace_id, **snapshot}
    status = payload.setdefault("system_status", {})
    if isinstance(status, dict):
        status["trace_id"] = trace_id
    return payload
