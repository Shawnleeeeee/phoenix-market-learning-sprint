from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

REGIMES = {"TREND_UP", "TREND_DOWN", "CHOP", "HIGH_VOL", "UNKNOWN"}
BIAS_VALUES = {"LONG", "SHORT", "NEUTRAL"}
SETUP_TYPES = {"QUICK_TRADE", "SWING_TRADE", "TREND_HOLD_CANDIDATE"}


def build_trader_snapshot(
    *,
    market_data: dict[str, Any] | None = None,
    account_state: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
    candidates: list[dict[str, Any]] | None = None,
    system_status: dict[str, Any] | None = None,
    max_candidates: int = 10,
    stale_after_sec: int = 30,
) -> dict[str, Any]:
    market_data = market_data or {}
    account_state = account_state or {}
    positions = positions or []
    candidates = candidates or []
    system_status = system_status or {}

    snapshot_time = str(system_status.get("snapshot_time") or datetime.now(timezone.utc).isoformat())
    data_fresh = _data_fresh(system_status, stale_after_sec=stale_after_sec)
    market_regime = _build_market_regime(market_data)
    current_positions = [_normalize_position(item) for item in positions]
    account_risk = _build_account_risk(account_state, current_positions)
    top_candidates = [_normalize_candidate(item) for item in _top_candidates(candidates, max_candidates=max_candidates)]

    normalized_status = {
        "snapshot_time": snapshot_time,
        "data_fresh": bool(data_fresh),
        "websocket_status": str(system_status.get("websocket_status") or "unavailable"),
        "exchange_status": str(system_status.get("exchange_status") or "unavailable"),
        "source": str(system_status.get("source") or "manual_payload"),
        "testnet_only": True,
    }
    for key in (
        "stale_after_sec",
        "data_age_sec",
        "position_state",
        "stop_protection_status",
        "candidate_state",
        "protective_stop_path_available",
        "emergency_close_available",
        "exchange_error",
        "websocket_error",
        "candidate_source",
        "account_source",
        "snapshot_source",
        "trusted_runtime_snapshot",
        "account_state_source",
        "position_state_source",
        "protective_stop_capability_source",
        "emergency_close_capability_source",
        "freeze_reason",
        "can_continue",
    ):
        if key in system_status:
            normalized_status[key] = system_status[key]

    return {
        "market_regime": market_regime,
        "account_risk": account_risk,
        "current_positions": current_positions,
        "top_candidates": top_candidates,
        "system_status": normalized_status,
    }


def _build_market_regime(market_data: dict[str, Any]) -> dict[str, Any]:
    btc_1m = _safe_float(market_data.get("btc_trend_1m"))
    btc_5m = _safe_float(market_data.get("btc_trend_5m"))
    eth_1m = _safe_float(market_data.get("eth_trend_1m"))
    eth_5m = _safe_float(market_data.get("eth_trend_5m"))
    volatility = _safe_float(market_data.get("volatility"))
    raw_regime = str(market_data.get("regime") or "").strip().upper()

    if raw_regime in REGIMES:
        regime = raw_regime
    elif volatility is not None and volatility >= 2.5:
        regime = "HIGH_VOL"
    elif _both_positive(btc_1m, btc_5m, eth_1m, eth_5m):
        regime = "TREND_UP"
    elif _both_negative(btc_1m, btc_5m, eth_1m, eth_5m):
        regime = "TREND_DOWN"
    elif any(value is not None for value in (btc_1m, btc_5m, eth_1m, eth_5m)):
        regime = "CHOP"
    else:
        regime = "UNKNOWN"

    direction_lock = str(market_data.get("direction_lock") or "").strip().upper()
    if direction_lock not in {
        "LONG_ONLY_OR_NO_TRADE",
        "SHORT_ONLY_OR_NO_TRADE",
        "BOTH_ALLOWED",
        "NO_TRADE",
    }:
        if regime == "TREND_UP":
            direction_lock = "LONG_ONLY_OR_NO_TRADE"
        elif regime == "TREND_DOWN":
            direction_lock = "SHORT_ONLY_OR_NO_TRADE"
        elif regime == "HIGH_VOL":
            direction_lock = "NO_TRADE"
        else:
            direction_lock = "BOTH_ALLOWED"

    return {
        "regime": regime,
        "btc_trend_1m": _value_or_unavailable(btc_1m),
        "btc_trend_5m": _value_or_unavailable(btc_5m),
        "eth_trend_1m": _value_or_unavailable(eth_1m),
        "eth_trend_5m": _value_or_unavailable(eth_5m),
        "volatility": _value_or_unavailable(volatility),
        "direction_lock": direction_lock,
    }


def _build_account_risk(account_state: dict[str, Any], positions: list[dict[str, Any]]) -> dict[str, Any]:
    max_open = int(_safe_float(account_state.get("max_open_positions"), default=1) or 1)
    open_count = int(_safe_float(account_state.get("open_positions_count"), default=len(positions)) or len(positions))
    daily_pnl = _safe_float(account_state.get("daily_pnl_pct"), default=0.0) or 0.0
    max_daily_loss = abs(_safe_float(account_state.get("max_daily_loss_pct"), default=3.0) or 3.0)
    daily_loss_remaining = max(0.0, max_daily_loss + daily_pnl)
    loss_streak = int(_safe_float(account_state.get("loss_streak"), default=0) or 0)
    cooldown_active = bool(account_state.get("cooldown_active", False))
    reason_if_blocked = str(account_state.get("reason_if_blocked") or "").strip()
    trading_allowed = bool(account_state.get("trading_allowed", True))

    if open_count >= max_open:
        trading_allowed = False
        reason_if_blocked = reason_if_blocked or "max_open_positions_reached"
    if daily_loss_remaining <= 0:
        trading_allowed = False
        reason_if_blocked = reason_if_blocked or "daily_loss_limit_hit"
    if cooldown_active:
        trading_allowed = False
        reason_if_blocked = reason_if_blocked or "cooldown_active"

    return {
        "trading_allowed": trading_allowed,
        "daily_pnl_pct": round(daily_pnl, 6),
        "daily_loss_remaining_pct": round(daily_loss_remaining, 6),
        "open_positions_count": open_count,
        "max_open_positions": max_open,
        "loss_streak": loss_streak,
        "cooldown_active": cooldown_active,
        "reason_if_blocked": reason_if_blocked or None,
    }


def _normalize_position(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": str(item.get("symbol") or "UNKNOWN").upper(),
        "side": str(item.get("side") or "UNKNOWN").upper(),
        "entry_price": _value_or_unavailable(_safe_float(item.get("entry_price"))),
        "current_price": _value_or_unavailable(_safe_float(item.get("current_price"))),
        "unrealized_pnl_pct": _value_or_unavailable(_safe_float(item.get("unrealized_pnl_pct"))),
        "unrealized_pnl_usdt": _value_or_unavailable(_safe_float(item.get("unrealized_pnl_usdt"))),
        "stop_loss_price": _value_or_unavailable(_safe_float(item.get("stop_loss_price"))),
        "take_profit_price": _value_or_unavailable(_safe_float(item.get("take_profit_price"))),
        "time_in_trade_sec": int(_safe_float(item.get("time_in_trade_sec"), default=0) or 0),
        "protection_status": str(item.get("protection_status") or "unknown"),
    }


def _normalize_candidate(item: dict[str, Any]) -> dict[str, Any]:
    bias = str(item.get("bias") or item.get("directional_bias") or "NEUTRAL").upper()
    setup_type = str(item.get("setup_type") or "QUICK_TRADE").upper()
    return {
        "symbol": str(item.get("symbol") or "UNKNOWN").upper(),
        "bias": bias if bias in BIAS_VALUES else "NEUTRAL",
        "setup_type": setup_type if setup_type in SETUP_TYPES else "QUICK_TRADE",
        "score": _safe_float(item.get("score"), default=0.0) or 0.0,
        "current_price": _value_or_unavailable(_safe_float(item.get("current_price") or item.get("mark_price"))),
        "price_action_summary": str(item.get("price_action_summary") or "unavailable"),
        "order_flow_summary": str(item.get("order_flow_summary") or "unavailable"),
        "depth_summary": str(item.get("depth_summary") or "unavailable"),
        "derivatives_summary": str(item.get("derivatives_summary") or "unavailable"),
        "spread_bps": _value_or_unavailable(_safe_float(item.get("spread_bps"))),
        "estimated_slippage_bps": _value_or_unavailable(_safe_float(item.get("estimated_slippage_bps"))),
        "liquidity_ok": bool(item.get("liquidity_ok", True)),
        "suggested_stop_pct": _value_or_unavailable(_safe_float(item.get("suggested_stop_pct"))),
        "suggested_tp_pct": _value_or_unavailable(_safe_float(item.get("suggested_tp_pct"))),
        "max_holding_time_sec": int(_safe_float(item.get("max_holding_time_sec"), default=0) or 0),
        "invalidation_hint": str(item.get("invalidation_hint") or "unavailable"),
    }


def _top_candidates(candidates: list[dict[str, Any]], *, max_candidates: int) -> list[dict[str, Any]]:
    return sorted(candidates, key=lambda item: _safe_float(item.get("score"), default=0.0) or 0.0, reverse=True)[
        : max(1, min(10, int(max_candidates or 10)))
    ]


def _data_fresh(system_status: dict[str, Any], *, stale_after_sec: int) -> bool:
    if "data_fresh" in system_status:
        return bool(system_status.get("data_fresh"))
    age = _safe_float(system_status.get("data_age_sec"))
    if age is None:
        return False
    return age <= stale_after_sec


def _both_positive(*values: float | None) -> bool:
    present = [value for value in values if value is not None]
    return bool(present) and sum(1 for value in present if value > 0) >= max(2, len(present) - 1)


def _both_negative(*values: float | None) -> bool:
    present = [value for value in values if value is not None]
    return bool(present) and sum(1 for value in present if value < 0) >= max(2, len(present) - 1)


def _value_or_unavailable(value: float | None) -> float | str:
    return value if value is not None else "unavailable"


def _safe_float(value: Any, *, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
