"""Service health for Leiting."""

from __future__ import annotations

from datetime import datetime, timezone

from .state_store import update_runtime_state


def heartbeat(service_name: str, *, status: str, details: dict | None = None) -> None:
    """Emit health information for systemd, dashboards, and Hermes."""
    payload = {
        "service": service_name,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "details": details or {},
    }
    update_runtime_state(f"{service_name}_heartbeat", payload)
