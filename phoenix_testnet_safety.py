from __future__ import annotations

import itertools
import os
import shutil
from pathlib import Path
from typing import Any

from phoenix_runtime_modes import (
    MAINNET_LIVE,
    MAINNET_SHADOW,
    TESTNET_LIVE,
    assert_order_endpoint_allowed,
    evaluate_runtime_mode,
    normalize_runtime_mode,
)

PROJECT_ROOT = Path(__file__).resolve().parent
ORDER_SUBMIT_PATH = "/fapi/v1/order"


def validate_runtime_mode_args(args: Any) -> str:
    runtime_mode = normalize_runtime_mode(getattr(args, "runtime_mode", None))
    env = str(getattr(args, "env", "") or "").strip().lower()
    evaluate_runtime_mode(runtime_mode=runtime_mode, env=env)
    if runtime_mode == TESTNET_LIVE and env not in {"testnet", "demo"}:
        raise ValueError("TESTNET_LIVE may only run with --env testnet or --env demo.")
    if runtime_mode == MAINNET_LIVE:
        raise ValueError("MAINNET_LIVE is rejected by phoenix_testnet_round_runner.")
    return runtime_mode


def assert_live_order_submission_allowed(args: Any, *, endpoint: str = ORDER_SUBMIT_PATH) -> None:
    runtime_mode = validate_runtime_mode_args(args)
    env = str(getattr(args, "env", "") or "").strip().lower()
    if runtime_mode == MAINNET_SHADOW:
        raise RuntimeError("MAINNET_SHADOW must not submit live orders from phoenix_testnet_round_runner.")
    assert_order_endpoint_allowed(runtime_mode=runtime_mode, env=env, endpoint=endpoint)


def round_numbers(args: Any):
    if bool(getattr(args, "run_forever", False)) or int(getattr(args, "rounds", 0) or 0) <= 0:
        return itertools.count(1)
    return range(1, max(1, int(args.rounds)) + 1)


def active_position_count(position_payload: object) -> int:
    if not isinstance(position_payload, list):
        return 0
    return sum(
        1
        for item in position_payload
        if isinstance(item, dict) and abs(_safe_float(item.get("positionAmt"))) > 1e-12
    )


def dynamic_concurrency_throttle(
    *,
    requested_slots: int,
    max_open_positions: int,
    current_open_positions: int = 0,
    api_error_rate: float = 0.0,
    order_reject_rate: float = 0.0,
    base_cooldown_sec: float = 1.0,
) -> dict[str, Any]:
    requested = max(1, int(requested_slots or 1))
    position_capacity = max(0, int(max_open_positions or 0) - max(0, int(current_open_positions or 0)))
    slots = min(requested, max(1, int(max_open_positions or 1)), max(1, position_capacity))
    cooldown = max(0.05, float(base_cooldown_sec or 0.05))
    reasons: list[str] = []

    try:
        load_avg = os.getloadavg()[0]
        cpu_count = max(1, os.cpu_count() or 1)
        if load_avg / cpu_count >= 0.85:
            slots = max(1, slots // 2)
            cooldown = max(cooldown, 2.0)
            reasons.append("cpu")
    except (AttributeError, OSError):
        pass

    if hasattr(os, "sysconf"):
        try:
            pages = int(os.sysconf("SC_PHYS_PAGES"))
            available_pages = int(os.sysconf("SC_AVPHYS_PAGES"))
            page_size = int(os.sysconf("SC_PAGE_SIZE"))
            if pages > 0 and page_size > 0:
                memory_available_ratio = (available_pages * page_size) / (pages * page_size)
                if memory_available_ratio <= 0.12:
                    slots = max(1, slots // 2)
                    cooldown = max(cooldown, 2.0)
                    reasons.append("memory")
        except (ValueError, OSError):
            pass

    try:
        disk = shutil.disk_usage(str(PROJECT_ROOT))
        if disk.total > 0 and (disk.free / disk.total) <= 0.08:
            slots = max(1, slots // 2)
            cooldown = max(cooldown, 3.0)
            reasons.append("disk")
    except OSError:
        pass

    if api_error_rate >= 0.25:
        slots = 1
        cooldown = max(cooldown, 10.0)
        reasons.append("api_errors")
    elif api_error_rate >= 0.10:
        slots = max(1, slots // 2)
        cooldown = max(cooldown, 4.0)
        reasons.append("api_errors")

    if order_reject_rate >= 0.20:
        slots = 1
        cooldown = max(cooldown, 8.0)
        reasons.append("order_rejects")
    elif order_reject_rate >= 0.08:
        slots = max(1, slots // 2)
        cooldown = max(cooldown, 3.0)
        reasons.append("order_rejects")

    if position_capacity <= 0:
        slots = 0
        cooldown = max(cooldown, 5.0)
        reasons.append("max_open_positions")

    return {
        "max_slots": slots,
        "cooldown_sec": _round4(cooldown),
        "reasons": sorted(set(reasons)),
    }


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _round4(value: float) -> float:
    return round(float(value), 4)
