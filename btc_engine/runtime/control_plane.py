"""Leiting control-plane sync helpers."""

from __future__ import annotations

import time
from pathlib import Path

from btc_engine.config import STATE_DIR, get_env, get_hermes_control_target
from btc_engine.runtime.hermes_sync import run_remote_command, sync_outputs
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state
from btc_engine.runtime.strategy_version import STRATEGY_SWITCH_HISTORY_JSON, STRATEGY_VERSION_JSON, STRATEGY_VERSION_MD


CONTROL_SYNC_INTERVAL_SEC = int((get_env("BTC_CONTROL_SYNC_INTERVAL_SEC", "30") or "30").strip())


def _existing_outputs() -> dict[str, Path]:
    outputs: dict[str, Path] = {}
    file_names = [
        "active_trade.json",
        "demo_status.json",
        "engine_snapshot.json",
        "market_stream_state.json",
        "market_stream_snapshot.json",
        "user_stream_state.json",
        "last_close.json",
        "engine_heartbeat.json",
        "demo_auto_heartbeat.json",
        "post_fill_heartbeat.json",
        "market_stream_heartbeat.json",
        "user_stream_heartbeat.json",
        "last_control_event.json",
    ]
    for name in file_names:
        path = STATE_DIR / name
        if path.exists():
            outputs[name] = path
    if STRATEGY_VERSION_JSON.exists():
        outputs["strategy_version.json"] = STRATEGY_VERSION_JSON
    if STRATEGY_VERSION_MD.exists():
        outputs["strategy_version.md"] = STRATEGY_VERSION_MD
    if STRATEGY_SWITCH_HISTORY_JSON.exists():
        outputs["strategy_switch_history.json"] = STRATEGY_SWITCH_HISTORY_JSON
    return outputs


def sync_control_plane(*, force: bool = False, notify_remote: bool = False, reason: str = "control_sync") -> str | None:
    target = get_hermes_control_target()
    if not target:
        return None
    now = time.time()
    sync_state = read_runtime_state("control_sync_state") or {}
    last_synced_ts = float(sync_state.get("last_synced_ts") or 0.0)
    if not force and (now - last_synced_ts) < CONTROL_SYNC_INTERVAL_SEC:
        return None
    outputs = _existing_outputs()
    if not outputs:
        return None
    sync_outputs(outputs, target)
    remote_cmd = (
        get_env(
            "BTC_HERMES_CONTROL_REMOTE_CMD",
            "python3 /opt/phoenix/deploy/hermes/leiting_control_digest.py --hermes-home /root/.hermes",
        )
        or ""
    ).strip()
    if notify_remote and remote_cmd and "--notify-telegram" not in remote_cmd:
        remote_cmd = f"{remote_cmd} --notify-telegram"
    remote_hook_error = run_remote_command(target, remote_cmd)
    update_runtime_state(
        "control_sync_state",
        {
            "last_synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
            "last_synced_ts": now,
            "reason": reason,
            "remote_hook_error": remote_hook_error,
        },
    )
    return remote_hook_error
