"""Shared Hermes sync helpers for Leiting review and research outputs."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from btc_engine.config import get_env


def _ssh_timeout_sec() -> int:
    raw = (get_env("BTC_HERMES_SYNC_TIMEOUT_SEC", "12") or "12").strip()
    try:
        return max(3, int(raw))
    except ValueError:
        return 12


def _ssh_base_args() -> list[str]:
    timeout = str(_ssh_timeout_sec())
    return [
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={timeout}",
        "-o",
        "ServerAliveInterval=5",
        "-o",
        "ServerAliveCountMax=2",
    ]


def sync_outputs(outputs: dict[str, Path], target_spec: str) -> None:
    if not target_spec:
        return
    if ":" in target_spec and not target_spec.startswith("/"):
        remote_host, remote_dir = target_spec.split(":", 1)
        key_path = (get_env("BTC_HERMES_REVIEW_SSH_KEY_PATH", "") or "").strip()
        timeout = _ssh_timeout_sec()
        ssh_base = ["ssh", *_ssh_base_args()]
        scp_base = ["scp", *_ssh_base_args()]
        if key_path:
            ssh_base.extend(["-i", key_path])
            scp_base.extend(["-i", key_path])
        subprocess.run([*ssh_base, remote_host, f"mkdir -p {remote_dir}"], check=True, timeout=timeout + 3)
        for name, path in outputs.items():
            subprocess.run(
                [*scp_base, str(path), f"{remote_host}:{remote_dir}/{name}"],
                check=True,
                timeout=timeout + 3,
            )
        return
    target = Path(target_spec)
    target.mkdir(parents=True, exist_ok=True)
    for name, path in outputs.items():
        shutil.copy2(path, target / name)


def run_remote_command(target_spec: str, remote_cmd: str) -> str | None:
    if ":" not in target_spec or target_spec.startswith("/") or not remote_cmd.strip():
        return None
    remote_host, _remote_dir = target_spec.split(":", 1)
    key_path = (get_env("BTC_HERMES_REVIEW_SSH_KEY_PATH", "") or "").strip()
    timeout = _ssh_timeout_sec()
    ssh_base = ["ssh", *_ssh_base_args()]
    if key_path:
        ssh_base.extend(["-i", key_path])
    try:
        completed = subprocess.run(
            [*ssh_base, remote_host, remote_cmd],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout + 3,
        )
        return None if completed.returncode == 0 else f"remote_hook_rc_{completed.returncode}"
    except subprocess.TimeoutExpired:
        return "remote_hook_failed:timeout"
    except subprocess.CalledProcessError as exc:
        details = (exc.stderr or exc.stdout or str(exc)).strip()
        return f"remote_hook_failed:{details[:240]}"
