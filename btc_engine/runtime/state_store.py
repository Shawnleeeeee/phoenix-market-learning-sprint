"""Runtime state store for Leiting."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from btc_engine.config import STATE_DIR, ensure_runtime_dirs


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    with tmp_path.open("w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, path)
    return path


def update_runtime_state(name: str, payload: dict[str, Any]) -> Path:
    """Persist BTC engine state for Hermes and local diagnostics."""
    ensure_runtime_dirs()
    path = STATE_DIR / f"{name}.json"
    return _atomic_write_json(path, payload)


def read_runtime_state(name: str) -> dict[str, Any] | None:
    path = STATE_DIR / f"{name}.json"
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    decoder = json.JSONDecoder()
    try:
        payload, end = decoder.raw_decode(text)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        trailing = text[end:].strip()
        if trailing:
            # Recover a valid leading JSON object from a previously torn write.
            _atomic_write_json(path, payload)
        return payload
    return None


def delete_runtime_state(name: str) -> None:
    path = STATE_DIR / f"{name}.json"
    if path.exists():
        path.unlink()
