from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def spawn_guardian_worker(job_path: Path, log_path: Path) -> dict[str, Any]:
    unit_name = f"phoenix-guardian-{job_path.stem.lower()}"
    if _systemd_run_available():
        started = _spawn_with_systemd_run(unit_name=unit_name, job_path=job_path, log_path=log_path)
        started["job_file"] = str(job_path)
        started["log_file"] = str(log_path)
        return started
    started = _spawn_with_popen(job_path=job_path, log_path=log_path)
    started["unit_name"] = None
    return started


def _systemd_run_available() -> bool:
    return shutil.which("systemd-run") is not None


def _spawn_with_systemd_run(*, unit_name: str, job_path: Path, log_path: Path) -> dict[str, Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    shell_command = " ".join(
        [
            "exec",
            shlex.quote(str(sys.executable)),
            shlex.quote(str(PROJECT_ROOT / "phoenix_post_fill_worker.py")),
            "--job-file",
            shlex.quote(str(job_path)),
            ">>",
            shlex.quote(str(log_path)),
            "2>&1",
        ]
    )
    hermes_home = str(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))
    cmd = [
        "systemd-run",
        "--quiet",
        "--collect",
        "--unit",
        unit_name,
        "--property",
        f"WorkingDirectory={PROJECT_ROOT}",
        "--property",
        f"Environment=HERMES_HOME={hermes_home}",
        "--property",
        "Restart=no",
        "/bin/sh",
        "-lc",
        shell_command,
    ]
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        detail = stderr or stdout or f"systemd-run exited {completed.returncode}"
        raise RuntimeError(f"Failed to launch Guardian worker via systemd-run: {detail}")
    return {"pid": None, "unit_name": unit_name, "launcher": "systemd-run"}


def _spawn_with_popen(*, job_path: Path, log_path: Path) -> dict[str, Any]:
    cmd = [sys.executable, str(PROJECT_ROOT / "phoenix_post_fill_worker.py"), "--job-file", str(job_path)]
    env = os.environ.copy()
    log_handle = log_path.open("a", encoding="utf-8", buffering=1)
    process = subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    log_handle.close()
    return {"pid": process.pid, "launcher": "popen"}
