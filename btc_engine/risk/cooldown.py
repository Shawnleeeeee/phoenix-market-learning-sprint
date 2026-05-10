"""Cooldown management."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def cooldown_active(*, last_close_time_iso: str | None, cooldown_minutes: int) -> tuple[bool, str | None]:
    """Return True if the system is still in post-trade cooldown."""
    if not last_close_time_iso:
        return False, None
    try:
        last_close = datetime.fromisoformat(last_close_time_iso)
    except ValueError:
        return False, None
    if last_close.tzinfo is None:
        last_close = last_close.replace(tzinfo=timezone.utc)
    next_allowed = last_close + timedelta(minutes=cooldown_minutes)
    if datetime.now(timezone.utc) < next_allowed:
        return True, next_allowed.isoformat()
    return False, None
