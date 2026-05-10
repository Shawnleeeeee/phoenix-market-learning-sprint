from __future__ import annotations

from typing import Any

EXECUTION_MODE_TESTNET_LIVE = "TESTNET_LIVE"
EXECUTION_MODE_MAINNET_SHADOW = "MAINNET_SHADOW"


def normalize_execution_mode(raw_value: str | None) -> str:
    value = str(raw_value or EXECUTION_MODE_TESTNET_LIVE).strip().upper()
    aliases = {
        "TESTNET": EXECUTION_MODE_TESTNET_LIVE,
        "TESTNET_LIVE": EXECUTION_MODE_TESTNET_LIVE,
        "MAINNET_SHADOW": EXECUTION_MODE_MAINNET_SHADOW,
        "SHADOW": EXECUTION_MODE_MAINNET_SHADOW,
    }
    if value not in aliases:
        allowed = ", ".join(sorted({EXECUTION_MODE_TESTNET_LIVE, EXECUTION_MODE_MAINNET_SHADOW}))
        raise ValueError(f"Unsupported execution mode '{raw_value}'. Expected one of: {allowed}")
    return aliases[value]


def is_mainnet_shadow_mode(execution_mode: str | None) -> bool:
    return normalize_execution_mode(execution_mode) == EXECUTION_MODE_MAINNET_SHADOW


def validate_execution_mode_args(args: Any) -> str:
    execution_mode = normalize_execution_mode(getattr(args, "execution_mode", None))
    env = str(getattr(args, "env", "") or "").strip().lower()
    if execution_mode == EXECUTION_MODE_MAINNET_SHADOW and env != "prod":
        raise ValueError("MAINNET_SHADOW must run with --env prod so testnet data and mainnet shadow state cannot mix.")
    return execution_mode


def user_data_oms_enabled_for_mode(execution_mode: str | None, *, disable_user_data_oms: bool) -> bool:
    if is_mainnet_shadow_mode(execution_mode):
        return False
    return not bool(disable_user_data_oms)


def candidate_shadow_writes_enabled(args: Any) -> bool:
    return not bool(getattr(args, "disable_discovery_candidates", False))


def candidate_parking_lot_enabled(args: Any) -> bool:
    return bool(getattr(args, "candidate_parking_lot_only", False))
