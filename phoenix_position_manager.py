from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable


POSITION_MANAGER_VERSION = "v1.0"

EXIT_MODULES = (
    "breakeven_guard",
    "partial_take_profit",
    "trailing_stop",
    "momentum_decay_exit",
    "no_follow_through_exit",
    "danger_exit_before_stop_loss",
    "time_decay_exit",
)

TELEMETRY_FIELDS = (
    "mfe_pct",
    "mae_pct",
    "time_to_first_profit_ms",
    "time_to_mfe_ms",
    "giveback_pct",
    "early_exit_signals",
)

DEFAULT_EXIT_CONFIG = {
    "breakeven_trigger_pct": 0.4,
    "breakeven_buffer_pct": 0.02,
    "partial_take_profit_pct": 0.8,
    "trailing_activation_pct": 0.7,
    "trailing_giveback_pct": 0.25,
    "momentum_decay_min_profit_pct": 0.2,
    "momentum_decay_threshold": 0.25,
    "no_follow_through_after_ms": 180_000,
    "no_follow_through_min_mfe_pct": 0.15,
    "danger_score_threshold": 0.8,
    "stop_loss_pct": 0.6,
    "time_decay_after_ms": 900_000,
    "time_decay_min_profit_pct": 0.1,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def side_multiplier(side: str | None) -> int:
    normalized = str(side or "LONG").strip().upper()
    if normalized in {"SHORT", "SELL"}:
        return -1
    return 1


def profit_pct(*, entry_price: float, mark_price: float, side: str | None) -> float:
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")
    return side_multiplier(side) * ((mark_price - entry_price) / entry_price) * 100.0


def merge_exit_config(config: dict[str, Any] | None = None) -> dict[str, float]:
    merged = dict(DEFAULT_EXIT_CONFIG)
    if config:
        for key, value in config.items():
            if key in merged:
                parsed = safe_float(value)
                if parsed is not None:
                    merged[key] = parsed
    return {key: float(value) for key, value in merged.items()}


def _timestamp_ms(row: dict[str, Any], fallback: int) -> int:
    parsed = safe_float(row.get("timestamp_ms") or row.get("ts_ms") or row.get("time_ms"))
    return int(parsed) if parsed is not None else fallback


def build_dynamic_exit_report(
    position: dict[str, Any],
    price_points: Iterable[dict[str, Any]],
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entry_price = safe_float(position.get("entry_price"))
    if entry_price is None or entry_price <= 0:
        raise ValueError("position.entry_price must be positive")
    side = str(position.get("side") or "LONG")
    opened_at_ms = int(safe_float(position.get("opened_at_ms"), 0) or 0)
    exit_config = merge_exit_config(config)

    mfe_pct = 0.0
    mae_pct = 0.0
    time_to_first_profit_ms: int | None = None
    time_to_mfe_ms: int | None = None
    signals: list[dict[str, Any]] = []
    signal_names: set[str] = set()
    last_profit_pct = 0.0
    last_elapsed_ms = 0

    for index, point in enumerate(price_points):
        mark_price = safe_float(point.get("price") or point.get("mark_price") or point.get("close"))
        if mark_price is None or mark_price <= 0:
            continue
        timestamp_ms = _timestamp_ms(point, opened_at_ms + index)
        elapsed_ms = max(0, timestamp_ms - opened_at_ms)
        current_profit_pct = profit_pct(entry_price=entry_price, mark_price=mark_price, side=side)
        last_profit_pct = current_profit_pct
        last_elapsed_ms = elapsed_ms
        if current_profit_pct > mfe_pct:
            mfe_pct = current_profit_pct
            time_to_mfe_ms = elapsed_ms
        if current_profit_pct < mae_pct:
            mae_pct = current_profit_pct
        if time_to_first_profit_ms is None and current_profit_pct > 0:
            time_to_first_profit_ms = elapsed_ms
        giveback_pct = max(0.0, mfe_pct - current_profit_pct)

        def add_signal(name: str, reason: str) -> None:
            if name in signal_names:
                return
            signal_names.add(name)
            signals.append(
                {
                    "module": name,
                    "reason": reason,
                    "timestamp_ms": timestamp_ms,
                    "elapsed_ms": elapsed_ms,
                    "profit_pct": round(current_profit_pct, 6),
                    "mfe_pct": round(mfe_pct, 6),
                    "mae_pct": round(mae_pct, 6),
                    "giveback_pct": round(giveback_pct, 6),
                }
            )

        if (
            mfe_pct >= exit_config["breakeven_trigger_pct"]
            and current_profit_pct <= exit_config["breakeven_buffer_pct"]
        ):
            add_signal("breakeven_guard", "profit gave back to breakeven after favorable excursion")
        if current_profit_pct >= exit_config["partial_take_profit_pct"]:
            add_signal("partial_take_profit", "profit reached partial take-profit threshold")
        if (
            mfe_pct >= exit_config["trailing_activation_pct"]
            and giveback_pct >= exit_config["trailing_giveback_pct"]
        ):
            add_signal("trailing_stop", "activated trailing stop gave back enough profit")
        momentum_score = safe_float(point.get("momentum_score"))
        if (
            momentum_score is not None
            and current_profit_pct >= exit_config["momentum_decay_min_profit_pct"]
            and momentum_score <= exit_config["momentum_decay_threshold"]
        ):
            add_signal("momentum_decay_exit", "momentum score decayed while trade was profitable")
        if (
            elapsed_ms >= int(exit_config["no_follow_through_after_ms"])
            and mfe_pct < exit_config["no_follow_through_min_mfe_pct"]
        ):
            add_signal("no_follow_through_exit", "trade failed to make enough favorable progress")
        danger_score = safe_float(point.get("danger_score"))
        if (
            danger_score is not None
            and danger_score >= exit_config["danger_score_threshold"]
            and current_profit_pct > -exit_config["stop_loss_pct"]
        ):
            add_signal("danger_exit_before_stop_loss", "danger score fired before hard stop loss")
        if (
            elapsed_ms >= int(exit_config["time_decay_after_ms"])
            and current_profit_pct < exit_config["time_decay_min_profit_pct"]
        ):
            add_signal("time_decay_exit", "trade exceeded maximum hold time without enough profit")

    final_giveback_pct = max(0.0, mfe_pct - last_profit_pct)
    return {
        "generated_at": utc_now_iso(),
        "position_manager_version": POSITION_MANAGER_VERSION,
        "exit_modules": list(EXIT_MODULES),
        "covered_exit_modules": list(EXIT_MODULES),
        "telemetry_fields": list(TELEMETRY_FIELDS),
        "side": side.upper(),
        "entry_price": entry_price,
        "elapsed_ms": last_elapsed_ms,
        "current_profit_pct": round(last_profit_pct, 6),
        "mfe_pct": round(mfe_pct, 6),
        "mae_pct": round(mae_pct, 6),
        "time_to_first_profit_ms": time_to_first_profit_ms,
        "time_to_mfe_ms": time_to_mfe_ms,
        "giveback_pct": round(final_giveback_pct, 6),
        "early_exit_signals": signals,
        "early_exit_signal_names": [signal["module"] for signal in signals],
        "live_trading_enabled": False,
    }


def evaluate_dynamic_exit(
    position: dict[str, Any],
    point: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_dynamic_exit_report(position, [point], config=config)
