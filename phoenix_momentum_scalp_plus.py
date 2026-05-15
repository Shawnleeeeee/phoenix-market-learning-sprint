#!/usr/bin/env python3
"""Shadow-only 1m momentum scalp experiment for Phoenix.

This module is intentionally side-effect free. It builds paper/shadow signals,
simulates paper exits from candle paths, and renders aggregate shadow reports.
It does not import live execution code and never submits orders.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


STRATEGY_FAMILY = "ONE_MIN_MOMENTUM_SCALP_PLUS"

ENTRY_PROFILE_FAST = "SCALP_FAST_ENTRY"
ENTRY_PROFILE_CONFIRM = "SCALP_CONFIRM_ENTRY"

EXIT_FIXED_QUICK_TAKE = "EXIT_FIXED_QUICK_TAKE"
EXIT_HALF_TAKE_THEN_TRAIL = "EXIT_HALF_TAKE_THEN_TRAIL"
EXIT_MOMENTUM_DECAY = "EXIT_MOMENTUM_DECAY"

STATUS_SHADOW_ONLY = "shadow_only"
STATUS_ACTIVE_SHADOW = "active_shadow"
STATUS_WATCHLIST = "watchlist"
STATUS_PAUSED = "paused"
STATUS_PROMOTION_CANDIDATE_SHADOW_ONLY = "promotion_candidate_shadow_only"


@dataclass(frozen=True)
class MomentumScalpConfig:
    min_one_min_return_pct: float = 0.5
    min_one_min_range_to_atr: float = 1.4
    min_one_min_volume_burst_ratio: float = 2.0
    min_one_min_oi_change_pct: float = 0.2
    allow_missing_one_min_oi_for_shadow: bool = True
    min_five_min_oi_fallback_pct: float = 0.2
    require_one_min_oi_hard_gate: bool = False
    max_spread_bps: float = 6.0
    max_slippage_bps: float = 8.0
    symbol_cooldown_sec: int = 90
    confirmation_min_delay_sec: int = 60
    confirmation_max_delay_sec: int = 120
    confirmation_min_volume_burst_ratio: float = 1.5
    confirmation_min_oi_change_pct: float = 0.0
    confirmation_max_retrace_pct: float = 0.15
    default_round_trip_fee_bps: float = 8.0
    default_stop_loss_price_pct: float = 0.6
    decay_volume_burst_floor: float = 1.2
    decay_oi_drop_floor_pct: float = -0.05
    promotion_min_trades: int = 300
    promotion_min_profit_factor: float = 1.2
    promotion_min_first_tp_hit_rate: float = 0.55
    promotion_max_top_symbol_pnl_share: float = 20.0
    promotion_max_drawdown_pct: float = 5.0
    paused_negative_avg_return_pct: float = -0.05
    paused_fake_spike_rate: float = 0.4
    paused_max_losing_streak: int = 8


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _optional_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _iso_from_ms(value: int | float | None) -> str:
    ms = _safe_int(value, 0)
    if ms <= 0:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat()


def _first_float(payload: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        value = _optional_float(payload.get(key))
        if value is not None:
            return value
    return default


def _first_optional_float(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _optional_float(payload.get(key))
        if value is not None:
            return value
    return None


def _first_optional_float_from_sources(sources: Iterable[dict[str, Any]], *keys: str) -> float | None:
    for source in sources:
        value = _first_optional_float(source, *keys)
        if value is not None:
            return value
    return None


def _nested_dict(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def _normalized_direction(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"up", "buy", "long", "bull", "bullish", "green", "rise", "rising"}:
        return "up"
    if raw in {"down", "sell", "short", "bear", "bearish", "red", "fall", "falling"}:
        return "down"
    return ""


def _side_from_direction(direction: str) -> str:
    if direction == "up":
        return "BUY"
    if direction == "down":
        return "SELL"
    return ""


def _side_aware_return_pct(entry_price: float, exit_price: float, side: str) -> float:
    if entry_price <= 0 or exit_price <= 0:
        return 0.0
    if str(side).upper() == "SELL":
        return (entry_price - exit_price) / entry_price * 100.0
    return (exit_price - entry_price) / entry_price * 100.0


def _favorable_return_pct(entry_price: float, candle: dict[str, Any], side: str) -> float:
    if str(side).upper() == "SELL":
        return _side_aware_return_pct(entry_price, _safe_float(candle.get("low")), side)
    return _side_aware_return_pct(entry_price, _safe_float(candle.get("high")), side)


def _adverse_return_pct(entry_price: float, candle: dict[str, Any], side: str) -> float:
    if str(side).upper() == "SELL":
        return _side_aware_return_pct(entry_price, _safe_float(candle.get("high")), side)
    return _side_aware_return_pct(entry_price, _safe_float(candle.get("low")), side)


def _price_for_return(entry_price: float, return_pct: float, side: str) -> float:
    if entry_price <= 0:
        return 0.0
    if str(side).upper() == "SELL":
        return entry_price * (1.0 - return_pct / 100.0)
    return entry_price * (1.0 + return_pct / 100.0)


def _liquidity_bucket(quote_volume_24h: float | None) -> str:
    if quote_volume_24h is None or quote_volume_24h <= 0:
        return "unknown"
    if quote_volume_24h >= 1_000_000_000:
        return "mega"
    if quote_volume_24h >= 100_000_000:
        return "major"
    if quote_volume_24h >= 25_000_000:
        return "large"
    if quote_volume_24h >= 5_000_000:
        return "mid"
    return "thin"


def oi5_direction_bucket(side: str, five_min_oi_change_pct: float | None, *, threshold_pct: float = 0.2) -> str:
    side = str(side or "").upper()
    if five_min_oi_change_pct is None:
        return "oi5_flat_or_tiny"
    threshold = abs(float(threshold_pct))
    if side == "BUY" and five_min_oi_change_pct > threshold:
        return "oi5_aligned"
    if side == "SELL" and five_min_oi_change_pct < -threshold:
        return "oi5_aligned"
    if side == "BUY" and five_min_oi_change_pct < -threshold:
        return "oi5_unwind_opposite"
    if side == "SELL" and five_min_oi_change_pct > threshold:
        return "oi5_unwind_opposite"
    return "oi5_flat_or_tiny"


def _base_strategy_id(entry_profile: str, suffix: str) -> str:
    prefix = "OMSP_FAST" if entry_profile == ENTRY_PROFILE_FAST else "OMSP_CONFIRM"
    return f"{prefix}_{suffix}"


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def default_exit_branch_specs(entry_profile: str) -> list[dict[str, Any]]:
    """Return the shadow-only parameter grid for one entry profile."""

    specs: list[dict[str, Any]] = []
    normalized_entry = entry_profile if entry_profile in {ENTRY_PROFILE_FAST, ENTRY_PROFILE_CONFIRM} else ENTRY_PROFILE_FAST

    for take_profit in (0.8, 1.0):
        for stop_loss in (0.4, 0.6):
            for hold_minutes in (3, 5):
                suffix = f"FIXED_TP{int(take_profit * 10):02d}_SL{int(stop_loss * 10):02d}_H{hold_minutes}"
                specs.append(
                    {
                        "strategy_family": STRATEGY_FAMILY,
                        "strategy_id": _base_strategy_id(normalized_entry, suffix),
                        "branch_id": _base_strategy_id(normalized_entry, suffix),
                        "entry_profile": normalized_entry,
                        "exit_profile": EXIT_FIXED_QUICK_TAKE,
                        "take_profit_price_pct": take_profit,
                        "stop_loss_price_pct": stop_loss,
                        "max_hold_minutes": hold_minutes,
                        "status": STATUS_SHADOW_ONLY,
                        "shadow_only": True,
                        "paper_record_only": True,
                        "live_trading_enabled": False,
                        "promotion_allowed": False,
                    }
                )

    for first_take_profit in (0.8, 1.0):
        for trailing_callback in (0.3, 0.5):
            for hold_minutes in (5, 8, 10):
                suffix = (
                    f"HALF_TP{int(first_take_profit * 10):02d}_"
                    f"TRAIL{int(trailing_callback * 10):02d}_H{hold_minutes}"
                )
                specs.append(
                    {
                        "strategy_family": STRATEGY_FAMILY,
                        "strategy_id": _base_strategy_id(normalized_entry, suffix),
                        "branch_id": _base_strategy_id(normalized_entry, suffix),
                        "entry_profile": normalized_entry,
                        "exit_profile": EXIT_HALF_TAKE_THEN_TRAIL,
                        "first_take_profit_price_pct": first_take_profit,
                        "close_fraction_at_first_tp": 0.5,
                        "move_stop_to_breakeven_after_first_tp": True,
                        "trailing_callback_pct": trailing_callback,
                        "stop_loss_price_pct": 0.6,
                        "max_hold_minutes": hold_minutes,
                        "status": STATUS_SHADOW_ONLY,
                        "shadow_only": True,
                        "paper_record_only": True,
                        "live_trading_enabled": False,
                        "promotion_allowed": False,
                    }
                )

    specs.append(
        {
            "strategy_family": STRATEGY_FAMILY,
            "strategy_id": _base_strategy_id(normalized_entry, "MOMENTUM_DECAY_H10"),
            "branch_id": _base_strategy_id(normalized_entry, "MOMENTUM_DECAY_H10"),
            "entry_profile": normalized_entry,
            "exit_profile": EXIT_MOMENTUM_DECAY,
            "max_hold_minutes": 10,
            "status": STATUS_SHADOW_ONLY,
            "shadow_only": True,
            "paper_record_only": True,
            "live_trading_enabled": False,
            "promotion_allowed": False,
        }
    )

    return specs


def detect_momentum_scalp_radar_event(
    record: dict[str, Any],
    *,
    config: MomentumScalpConfig,
) -> dict[str, Any] | None:
    """Build a radar event from one collector record when thresholds pass."""

    if str(record.get("bar_interval") or "").strip().lower() not in {"1m", "1min", "60s"}:
        return None

    sample = _nested_dict(record, "sample")
    enrichments = _nested_dict(record, "enrichments")
    direction = _normalized_direction(
        sample.get("trigger_candle_direction")
        or sample.get("candle_direction")
        or record.get("trigger_candle_direction")
        or record.get("candle_direction")
    )
    side = _side_from_direction(direction)
    if not side:
        return None

    one_min_return_pct = _first_float(sample, "ret_1bar_pct", "one_min_return_pct", "return_pct")
    if direction == "down" and one_min_return_pct > 0:
        one_min_return_pct = -one_min_return_pct
    if abs(one_min_return_pct) < float(config.min_one_min_return_pct):
        return None

    if direction == "up" and one_min_return_pct < 0:
        return None
    if direction == "down" and one_min_return_pct > 0:
        return None

    one_min_range_to_atr = _first_float(sample, "range_to_atr", "one_min_range_to_atr")
    one_min_volume_burst_ratio = _first_float(sample, "volume_burst_ratio", "one_min_volume_burst_ratio")
    one_min_oi_change_pct = _first_optional_float_from_sources(
        (enrichments, sample, record),
        "oi_change_1m_pct",
        "one_min_oi_change_pct",
        "oi_change_pct",
    )
    five_min_oi_change_pct = _first_optional_float_from_sources(
        (enrichments, sample, record),
        "oi_change_5m_pct",
        "five_min_oi_change_pct",
    )
    fifteen_min_oi_change_pct = _first_optional_float_from_sources(
        (enrichments, sample, record),
        "oi_change_15m_pct",
        "fifteen_min_oi_change_pct",
    )
    spread_bps = _first_float(enrichments, "spread_bps_at_entry", "spread_bps")
    slippage_bps = _first_float(enrichments, "estimated_slippage_bps", "slippage_bps")

    if one_min_range_to_atr < float(config.min_one_min_range_to_atr):
        return None
    if one_min_volume_burst_ratio < float(config.min_one_min_volume_burst_ratio):
        return None
    if spread_bps > float(config.max_spread_bps):
        return None
    if slippage_bps > float(config.max_slippage_bps):
        return None

    if one_min_oi_change_pct is not None:
        oi_confirmation_source = "one_min"
        if bool(config.require_one_min_oi_hard_gate) and abs(one_min_oi_change_pct) < float(config.min_one_min_oi_change_pct):
            return None
    elif bool(config.require_one_min_oi_hard_gate):
        return None
    elif five_min_oi_change_pct is not None:
        oi_confirmation_source = "five_min_fallback"
    elif bool(config.allow_missing_one_min_oi_for_shadow):
        oi_confirmation_source = "missing"
    else:
        return None

    oi5_bucket = oi5_direction_bucket(
        side,
        five_min_oi_change_pct,
        threshold_pct=float(config.min_five_min_oi_fallback_pct),
    )

    price = _first_float(sample, "price", "close", "trigger_price", default=_safe_float(record.get("price")))
    if price <= 0:
        return None

    signal_time_ms = _safe_int(
        sample.get("anchor_close_time_ms")
        or record.get("anchor_close_time_ms")
        or record.get("observed_at_ms")
    )
    quote_volume_24h = _optional_float(sample.get("quote_volume_24h"))
    fake_spike_flag = bool(sample.get("fake_spike_flag") or record.get("fake_spike_flag"))
    funding_rate = _first_float(enrichments, "funding_rate_at_entry", "funding_rate")

    trigger_features = {
        "min_one_min_return_pct": config.min_one_min_return_pct,
        "min_one_min_range_to_atr": config.min_one_min_range_to_atr,
        "min_one_min_volume_burst_ratio": config.min_one_min_volume_burst_ratio,
        "min_one_min_oi_change_pct": config.min_one_min_oi_change_pct,
        "allow_missing_one_min_oi_for_shadow": config.allow_missing_one_min_oi_for_shadow,
        "min_five_min_oi_fallback_pct": config.min_five_min_oi_fallback_pct,
        "require_one_min_oi_hard_gate": config.require_one_min_oi_hard_gate,
        "direction": direction,
        "oi_confirmation_source": oi_confirmation_source,
        "oi5_direction_bucket": oi5_bucket,
    }
    market_snapshot = {
        "sample": dict(sample),
        "enrichments": dict(enrichments),
        "event_id": record.get("event_id"),
        "observed_at_ms": record.get("observed_at_ms"),
    }

    return {
        "event": "one_min_momentum_scalp_radar_event",
        "strategy_family": STRATEGY_FAMILY,
        "symbol": str(record.get("symbol") or "").upper(),
        "side": side,
        "signal_time_ms": signal_time_ms,
        "signal_time": _iso_from_ms(signal_time_ms),
        "trigger_price": price,
        "trigger_features": trigger_features,
        "market_snapshot": market_snapshot,
        "one_min_return_pct": one_min_return_pct,
        "one_min_range_to_atr": one_min_range_to_atr,
        "one_min_volume_burst_ratio": one_min_volume_burst_ratio,
        "one_min_oi_change_pct": one_min_oi_change_pct,
        "five_min_oi_change_pct": five_min_oi_change_pct,
        "fifteen_min_oi_change_pct": fifteen_min_oi_change_pct,
        "oi_confirmation_source": oi_confirmation_source,
        "oi5_direction_bucket": oi5_bucket,
        "one_min_candle_direction": direction,
        "spread_bps_at_entry": spread_bps,
        "estimated_slippage_bps": slippage_bps,
        "funding_rate_at_entry": funding_rate,
        "estimated_fee_bps": config.default_round_trip_fee_bps,
        "order_latency_ms": 0,
        "maker_or_taker": "paper_taker",
        "liquidity_bucket": _liquidity_bucket(quote_volume_24h),
        "fake_spike_flag": fake_spike_flag,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "shadow_only": True,
        "paper_record_only": True,
        "live_order_submission_blocked": True,
    }


def _signal_from_radar_and_branch(
    record: dict[str, Any],
    radar: dict[str, Any],
    branch: dict[str, Any],
    *,
    entry_profile: str,
    entry_time_ms: int,
    entry_price: float,
    confirmation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    confirmation = confirmation or {}
    signal_time_ms = _safe_int(radar.get("signal_time_ms"))
    latency_ms = max(0, entry_time_ms - signal_time_ms) if entry_time_ms and signal_time_ms else 0
    signal_id = f"{record.get('event_id') or radar.get('symbol')}-{branch.get('strategy_id')}"
    max_hold_minutes = _safe_float(branch.get("max_hold_minutes"), 5.0)
    base = {
        "event": "one_min_momentum_scalp_shadow_signal",
        "event_id": signal_id,
        "source_event_id": record.get("event_id"),
        "strategy_family": STRATEGY_FAMILY,
        "strategy_id": branch.get("strategy_id"),
        "entry_profile": entry_profile,
        "exit_profile": branch.get("exit_profile"),
        "status": STATUS_SHADOW_ONLY,
        "symbol": radar.get("symbol"),
        "side": radar.get("side"),
        "signal_time": radar.get("signal_time"),
        "signal_time_ms": signal_time_ms,
        "simulated_entry_time": _iso_from_ms(entry_time_ms),
        "simulated_entry_time_ms": entry_time_ms,
        "simulated_entry_price": entry_price,
        "entry_price": entry_price,
        "trigger_price": radar.get("trigger_price"),
        "trigger_features": radar.get("trigger_features"),
        "market_snapshot": radar.get("market_snapshot"),
        "one_min_return_pct": radar.get("one_min_return_pct"),
        "one_min_range_to_atr": radar.get("one_min_range_to_atr"),
        "one_min_volume_burst_ratio": radar.get("one_min_volume_burst_ratio"),
        "one_min_oi_change_pct": radar.get("one_min_oi_change_pct"),
        "five_min_oi_change_pct": radar.get("five_min_oi_change_pct"),
        "fifteen_min_oi_change_pct": radar.get("fifteen_min_oi_change_pct"),
        "oi_confirmation_source": radar.get("oi_confirmation_source"),
        "oi5_direction_bucket": radar.get("oi5_direction_bucket"),
        "one_min_candle_direction": radar.get("one_min_candle_direction"),
        "confirmation_delay_sec": int(confirmation.get("confirmation_delay_sec") or 0),
        "confirmed_price": confirmation.get("confirmed_price"),
        "confirmed_oi_change_pct": confirmation.get("confirmed_oi_change_pct"),
        "confirmed_volume_burst_ratio": confirmation.get("confirmed_volume_burst_ratio"),
        "fake_spike_flag": bool(confirmation.get("fake_spike_flag", radar.get("fake_spike_flag", False))),
        "spread_bps_at_entry": radar.get("spread_bps_at_entry"),
        "estimated_slippage_bps": radar.get("estimated_slippage_bps"),
        "funding_rate_at_entry": radar.get("funding_rate_at_entry"),
        "estimated_fee_bps": radar.get("estimated_fee_bps"),
        "order_latency_ms": latency_ms,
        "maker_or_taker": radar.get("maker_or_taker") or "paper_taker",
        "liquidity_bucket": radar.get("liquidity_bucket") or "unknown",
        "shadow_only": True,
        "paper_record_only": True,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "live_order_submission_blocked": True,
        "live_order_queue_write": False,
        "order_endpoint_called": False,
        "shadow_target_horizons_sec": [max(60, int(max_hold_minutes * 60))],
    }
    base.update(branch)
    base["shadow_branches"] = [dict(branch)]
    base["status"] = STATUS_SHADOW_ONLY
    base["live_trading_enabled"] = False
    base["promotion_allowed"] = False
    return base


def build_momentum_scalp_shadow_signals(
    record: dict[str, Any],
    *,
    config: MomentumScalpConfig,
    now_ms: int,
    last_signal_ms_by_symbol: dict[str, int],
) -> list[dict[str, Any]]:
    """Build fast-entry shadow signals for all exit branches."""

    radar = detect_momentum_scalp_radar_event(record, config=config)
    if radar is None:
        return []
    symbol = str(radar.get("symbol") or "")
    last_ms = _safe_int(last_signal_ms_by_symbol.get(symbol))
    if last_ms > 0 and int(now_ms) - last_ms < int(config.symbol_cooldown_sec) * 1000:
        return []

    entry_time_ms = _safe_int(radar.get("signal_time_ms")) or int(now_ms)
    entry_price = _safe_float(radar.get("trigger_price"))
    signals = [
        _signal_from_radar_and_branch(
            record,
            radar,
            branch,
            entry_profile=ENTRY_PROFILE_FAST,
            entry_time_ms=entry_time_ms,
            entry_price=entry_price,
        )
        for branch in default_exit_branch_specs(ENTRY_PROFILE_FAST)
    ]
    if signals:
        last_signal_ms_by_symbol[symbol] = int(now_ms)
    return signals


def resolve_momentum_scalp_confirmation(
    radar: dict[str, Any],
    candles: Iterable[dict[str, Any]],
    *,
    config: MomentumScalpConfig,
) -> dict[str, Any] | None:
    """Confirm a radar event after one or two 1m candles."""

    if bool(radar.get("fake_spike_flag")):
        return None
    side = str(radar.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return None
    trigger_price = _safe_float(radar.get("trigger_price"))
    signal_time_ms = _safe_int(radar.get("signal_time_ms"))
    if trigger_price <= 0 or signal_time_ms <= 0:
        return None

    sorted_candles = sorted(candles, key=lambda item: _safe_int(item.get("close_time_ms")))
    eligible: list[dict[str, Any]] = []
    for candle in sorted_candles:
        close_time_ms = _safe_int(candle.get("close_time_ms"))
        delay_sec = int((close_time_ms - signal_time_ms) / 1000)
        if delay_sec < int(config.confirmation_min_delay_sec):
            continue
        if delay_sec > int(config.confirmation_max_delay_sec):
            continue
        eligible.append(candle)
    if not eligible:
        return None

    latest = eligible[-1]
    close_price = _safe_float(latest.get("close"))
    if close_price <= 0:
        return None
    confirmed_volume = _first_float(latest, "volume_burst_ratio", "one_min_volume_burst_ratio")
    confirmed_oi = _first_float(latest, "oi_change_pct", "one_min_oi_change_pct")
    if confirmed_volume < float(config.confirmation_min_volume_burst_ratio):
        return None
    if confirmed_oi < float(config.confirmation_min_oi_change_pct):
        return None

    if side == "BUY":
        retrace_floor = trigger_price * (1.0 - float(config.confirmation_max_retrace_pct) / 100.0)
        if close_price < trigger_price or _safe_float(latest.get("low")) < retrace_floor:
            return None
    else:
        retrace_ceiling = trigger_price * (1.0 + float(config.confirmation_max_retrace_pct) / 100.0)
        if close_price > trigger_price or _safe_float(latest.get("high")) > retrace_ceiling:
            return None

    delay_sec = int((_safe_int(latest.get("close_time_ms")) - signal_time_ms) / 1000)
    return {
        "confirmation_delay_sec": delay_sec,
        "confirmed_price": close_price,
        "confirmed_oi_change_pct": confirmed_oi,
        "confirmed_volume_burst_ratio": confirmed_volume,
        "fake_spike_flag": False,
        "confirmed_at": _iso_from_ms(_safe_int(latest.get("close_time_ms"))),
        "confirmed_at_ms": _safe_int(latest.get("close_time_ms")),
    }


def build_momentum_scalp_signal_from_confirmation(
    record: dict[str, Any],
    radar: dict[str, Any],
    confirmation: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Build the primary confirm-entry branch from a confirmed radar event."""

    if confirmation is None:
        return None
    branch = next(
        item
        for item in default_exit_branch_specs(ENTRY_PROFILE_CONFIRM)
        if item.get("exit_profile") == EXIT_HALF_TAKE_THEN_TRAIL
    )
    return _signal_from_radar_and_branch(
        record,
        radar,
        branch,
        entry_profile=ENTRY_PROFILE_CONFIRM,
        entry_time_ms=_safe_int(confirmation.get("confirmed_at_ms")) or _safe_int(radar.get("signal_time_ms")),
        entry_price=_safe_float(confirmation.get("confirmed_price"), _safe_float(radar.get("trigger_price"))),
        confirmation=confirmation,
    )


def build_momentum_scalp_signals_from_confirmation(
    record: dict[str, Any],
    radar: dict[str, Any],
    confirmation: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Build confirm-entry shadow signals for all exit branches."""

    if confirmation is None:
        return []
    entry_time_ms = _safe_int(confirmation.get("confirmed_at_ms")) or _safe_int(radar.get("signal_time_ms"))
    entry_price = _safe_float(confirmation.get("confirmed_price"), _safe_float(radar.get("trigger_price")))
    return [
        _signal_from_radar_and_branch(
            record,
            radar,
            branch,
            entry_profile=ENTRY_PROFILE_CONFIRM,
            entry_time_ms=entry_time_ms,
            entry_price=entry_price,
            confirmation=confirmation,
        )
        for branch in default_exit_branch_specs(ENTRY_PROFILE_CONFIRM)
    ]


def _base_outcome(
    signal: dict[str, Any],
    branch: dict[str, Any],
    *,
    round_trip_fee_bps: float,
    final_return_pct: float,
    final_exit_reason: str,
    final_exit_time_ms: int,
    final_exit_price: float,
    first_tp_hit: bool,
    time_to_first_tp_sec: int | None,
    first_tp_return_pct: float | None,
    max_runup_pct: float,
    max_drawdown_pct: float,
    close_fraction_at_first_tp: float | None = None,
    move_stop_to_breakeven_after_first_tp: bool | None = None,
) -> dict[str, Any]:
    entry_time_ms = _safe_int(signal.get("simulated_entry_time_ms") or signal.get("signal_time_ms"))
    holding_minutes = max(0.0, (final_exit_time_ms - entry_time_ms) / 60_000.0) if final_exit_time_ms else 0.0
    spread_bps = _safe_float(signal.get("spread_bps_at_entry"))
    slippage_bps = _safe_float(signal.get("estimated_slippage_bps"))
    fee_bps = float(round_trip_fee_bps)
    funding_rate = _safe_float(signal.get("funding_rate_at_entry"))
    funding_paid_during_hold = abs(funding_rate) * 100.0 * (holding_minutes / (8.0 * 60.0))
    after_fee_return_pct = final_return_pct - fee_bps / 100.0
    after_fee_and_slippage_return_pct = after_fee_return_pct - slippage_bps / 100.0
    after_real_cost_return_pct = after_fee_and_slippage_return_pct - spread_bps / 100.0 - funding_paid_during_hold

    payload = {
        "event": "one_min_momentum_scalp_shadow_outcome",
        "strategy_family": STRATEGY_FAMILY,
        "strategy_id": branch.get("strategy_id") or signal.get("strategy_id"),
        "entry_profile": signal.get("entry_profile"),
        "exit_profile": branch.get("exit_profile") or signal.get("exit_profile"),
        "status": STATUS_SHADOW_ONLY,
        "symbol": signal.get("symbol"),
        "side": signal.get("side"),
        "signal_time": signal.get("signal_time"),
        "signal_time_ms": signal.get("signal_time_ms"),
        "simulated_entry_time": signal.get("simulated_entry_time"),
        "simulated_entry_time_ms": signal.get("simulated_entry_time_ms"),
        "simulated_entry_price": signal.get("simulated_entry_price"),
        "trigger_price": signal.get("trigger_price"),
        "trigger_features": signal.get("trigger_features"),
        "market_snapshot": signal.get("market_snapshot"),
        "one_min_return_pct": signal.get("one_min_return_pct"),
        "one_min_range_to_atr": signal.get("one_min_range_to_atr"),
        "one_min_volume_burst_ratio": signal.get("one_min_volume_burst_ratio"),
        "one_min_oi_change_pct": signal.get("one_min_oi_change_pct"),
        "five_min_oi_change_pct": signal.get("five_min_oi_change_pct"),
        "fifteen_min_oi_change_pct": signal.get("fifteen_min_oi_change_pct"),
        "oi_confirmation_source": signal.get("oi_confirmation_source"),
        "oi5_direction_bucket": signal.get("oi5_direction_bucket"),
        "one_min_candle_direction": signal.get("one_min_candle_direction"),
        "confirmation_delay_sec": signal.get("confirmation_delay_sec"),
        "confirmed_price": signal.get("confirmed_price"),
        "confirmed_oi_change_pct": signal.get("confirmed_oi_change_pct"),
        "confirmed_volume_burst_ratio": signal.get("confirmed_volume_burst_ratio"),
        "fake_spike_flag": bool(signal.get("fake_spike_flag")),
        "spread_bps_at_entry": spread_bps,
        "estimated_slippage_bps": slippage_bps,
        "funding_rate_at_entry": funding_rate,
        "funding_paid_during_hold": round(funding_paid_during_hold, 8),
        "estimated_fee_bps": fee_bps,
        "order_latency_ms": signal.get("order_latency_ms"),
        "maker_or_taker": signal.get("maker_or_taker") or "paper_taker",
        "liquidity_bucket": signal.get("liquidity_bucket") or "unknown",
        "after_fee_return_pct": round(after_fee_return_pct, 8),
        "after_fee_and_slippage_return_pct": round(after_fee_and_slippage_return_pct, 8),
        "after_real_cost_return_pct": round(after_real_cost_return_pct, 8),
        "first_tp_hit": first_tp_hit,
        "time_to_first_tp_sec": time_to_first_tp_sec,
        "first_tp_return_pct": first_tp_return_pct,
        "final_exit_reason": final_exit_reason,
        "final_exit_time": _iso_from_ms(final_exit_time_ms),
        "final_exit_time_ms": final_exit_time_ms,
        "final_exit_price": final_exit_price,
        "final_return_pct": round(final_return_pct, 8),
        "max_runup_pct": round(max_runup_pct, 8),
        "max_drawdown_pct": round(max_drawdown_pct, 8),
        "holding_minutes": round(holding_minutes, 6),
        "shadow_only": True,
        "paper_record_only": True,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "live_order_submission_blocked": True,
        "live_order_queue_write": False,
        "order_endpoint_called": False,
    }
    if close_fraction_at_first_tp is not None:
        payload["close_fraction_at_first_tp"] = close_fraction_at_first_tp
    if move_stop_to_breakeven_after_first_tp is not None:
        payload["move_stop_to_breakeven_after_first_tp"] = move_stop_to_breakeven_after_first_tp
    return payload


def simulate_momentum_scalp_exit(
    signal: dict[str, Any],
    branch: dict[str, Any],
    candles: Iterable[dict[str, Any]],
    *,
    round_trip_fee_bps: float,
) -> dict[str, Any]:
    """Simulate one shadow exit branch from 1m candles."""

    exit_profile = str(branch.get("exit_profile") or signal.get("exit_profile") or "")
    side = str(signal.get("side") or "").upper()
    entry_price = _safe_float(signal.get("simulated_entry_price") or signal.get("entry_price"))
    entry_time_ms = _safe_int(signal.get("simulated_entry_time_ms") or signal.get("signal_time_ms"))
    sorted_candles = sorted(candles, key=lambda item: _safe_int(item.get("close_time_ms")))
    if entry_price <= 0 or side not in {"BUY", "SELL"}:
        return _base_outcome(
            signal,
            branch,
            round_trip_fee_bps=round_trip_fee_bps,
            final_return_pct=0.0,
            final_exit_reason="invalid_signal",
            final_exit_time_ms=entry_time_ms,
            final_exit_price=entry_price,
            first_tp_hit=False,
            time_to_first_tp_sec=None,
            first_tp_return_pct=None,
            max_runup_pct=0.0,
            max_drawdown_pct=0.0,
        )
    if not sorted_candles:
        return _base_outcome(
            signal,
            branch,
            round_trip_fee_bps=round_trip_fee_bps,
            final_return_pct=0.0,
            final_exit_reason="no_path_data",
            final_exit_time_ms=entry_time_ms,
            final_exit_price=entry_price,
            first_tp_hit=False,
            time_to_first_tp_sec=None,
            first_tp_return_pct=None,
            max_runup_pct=0.0,
            max_drawdown_pct=0.0,
        )

    max_hold_ms = int(_safe_float(branch.get("max_hold_minutes"), 5.0) * 60_000)
    stop_loss_pct = _safe_float(branch.get("stop_loss_price_pct"), 0.6)
    max_runup = 0.0
    max_drawdown = 0.0

    if exit_profile == EXIT_FIXED_QUICK_TAKE:
        take_profit_pct = _safe_float(branch.get("take_profit_price_pct"), 0.8)
        for candle in sorted_candles:
            close_time_ms = _safe_int(candle.get("close_time_ms"))
            favorable = _favorable_return_pct(entry_price, candle, side)
            adverse = _adverse_return_pct(entry_price, candle, side)
            max_runup = max(max_runup, favorable)
            max_drawdown = min(max_drawdown, adverse)
            elapsed_ms = close_time_ms - entry_time_ms
            if favorable >= take_profit_pct:
                return _base_outcome(
                    signal,
                    branch,
                    round_trip_fee_bps=round_trip_fee_bps,
                    final_return_pct=take_profit_pct,
                    final_exit_reason="take_profit_hit",
                    final_exit_time_ms=close_time_ms,
                    final_exit_price=_price_for_return(entry_price, take_profit_pct, side),
                    first_tp_hit=True,
                    time_to_first_tp_sec=max(0, int(elapsed_ms / 1000)),
                    first_tp_return_pct=take_profit_pct,
                    max_runup_pct=max_runup,
                    max_drawdown_pct=max_drawdown,
                )
            if adverse <= -abs(stop_loss_pct):
                return _base_outcome(
                    signal,
                    branch,
                    round_trip_fee_bps=round_trip_fee_bps,
                    final_return_pct=-abs(stop_loss_pct),
                    final_exit_reason="stop_loss_hit",
                    final_exit_time_ms=close_time_ms,
                    final_exit_price=_price_for_return(entry_price, -abs(stop_loss_pct), side),
                    first_tp_hit=False,
                    time_to_first_tp_sec=None,
                    first_tp_return_pct=None,
                    max_runup_pct=max_runup,
                    max_drawdown_pct=max_drawdown,
                )
            if elapsed_ms >= max_hold_ms:
                close_return = _side_aware_return_pct(entry_price, _safe_float(candle.get("close")), side)
                return _base_outcome(
                    signal,
                    branch,
                    round_trip_fee_bps=round_trip_fee_bps,
                    final_return_pct=close_return,
                    final_exit_reason="max_hold_timeout",
                    final_exit_time_ms=close_time_ms,
                    final_exit_price=_safe_float(candle.get("close")),
                    first_tp_hit=False,
                    time_to_first_tp_sec=None,
                    first_tp_return_pct=None,
                    max_runup_pct=max_runup,
                    max_drawdown_pct=max_drawdown,
                )

    elif exit_profile == EXIT_HALF_TAKE_THEN_TRAIL:
        first_take_profit_pct = _safe_float(branch.get("first_take_profit_price_pct"), 0.8)
        close_fraction = min(1.0, max(0.0, _safe_float(branch.get("close_fraction_at_first_tp"), 0.5)))
        trailing_callback_pct = _safe_float(branch.get("trailing_callback_pct"), 0.3)
        move_stop_to_breakeven = bool(branch.get("move_stop_to_breakeven_after_first_tp", True))
        first_tp_hit = False
        first_tp_time_sec: int | None = None
        high_water_return = 0.0

        for candle in sorted_candles:
            close_time_ms = _safe_int(candle.get("close_time_ms"))
            favorable = _favorable_return_pct(entry_price, candle, side)
            adverse = _adverse_return_pct(entry_price, candle, side)
            max_runup = max(max_runup, favorable)
            max_drawdown = min(max_drawdown, adverse)
            high_water_return = max(high_water_return, favorable)
            elapsed_ms = close_time_ms - entry_time_ms

            if not first_tp_hit and adverse <= -abs(stop_loss_pct):
                return _base_outcome(
                    signal,
                    branch,
                    round_trip_fee_bps=round_trip_fee_bps,
                    final_return_pct=-abs(stop_loss_pct),
                    final_exit_reason="stop_loss_hit",
                    final_exit_time_ms=close_time_ms,
                    final_exit_price=_price_for_return(entry_price, -abs(stop_loss_pct), side),
                    first_tp_hit=False,
                    time_to_first_tp_sec=None,
                    first_tp_return_pct=None,
                    max_runup_pct=max_runup,
                    max_drawdown_pct=max_drawdown,
                    close_fraction_at_first_tp=close_fraction,
                    move_stop_to_breakeven_after_first_tp=move_stop_to_breakeven,
                )

            first_tp_hit_this_candle = False
            if not first_tp_hit and favorable >= first_take_profit_pct:
                first_tp_hit = True
                first_tp_hit_this_candle = True
                first_tp_time_sec = max(0, int(elapsed_ms / 1000))
                high_water_return = max(high_water_return, first_take_profit_pct)

            if first_tp_hit and not first_tp_hit_this_candle:
                trail_return = high_water_return - trailing_callback_pct
                if move_stop_to_breakeven and adverse <= 0.0:
                    second_leg_return = 0.0
                    blended = close_fraction * first_take_profit_pct + (1.0 - close_fraction) * second_leg_return
                    return _base_outcome(
                        signal,
                        branch,
                        round_trip_fee_bps=round_trip_fee_bps,
                        final_return_pct=blended,
                        final_exit_reason="breakeven_stop_after_first_tp",
                        final_exit_time_ms=close_time_ms,
                        final_exit_price=entry_price,
                        first_tp_hit=True,
                        time_to_first_tp_sec=first_tp_time_sec,
                        first_tp_return_pct=first_take_profit_pct,
                        max_runup_pct=max_runup,
                        max_drawdown_pct=max_drawdown,
                        close_fraction_at_first_tp=close_fraction,
                        move_stop_to_breakeven_after_first_tp=move_stop_to_breakeven,
                    )
                if adverse <= trail_return:
                    second_leg_return = max(0.0, trail_return)
                    blended = close_fraction * first_take_profit_pct + (1.0 - close_fraction) * second_leg_return
                    return _base_outcome(
                        signal,
                        branch,
                        round_trip_fee_bps=round_trip_fee_bps,
                        final_return_pct=blended,
                        final_exit_reason="trailing_exit",
                        final_exit_time_ms=close_time_ms,
                        final_exit_price=_price_for_return(entry_price, second_leg_return, side),
                        first_tp_hit=True,
                        time_to_first_tp_sec=first_tp_time_sec,
                        first_tp_return_pct=first_take_profit_pct,
                        max_runup_pct=max_runup,
                        max_drawdown_pct=max_drawdown,
                        close_fraction_at_first_tp=close_fraction,
                        move_stop_to_breakeven_after_first_tp=move_stop_to_breakeven,
                    )

            if elapsed_ms >= max_hold_ms:
                close_return = _side_aware_return_pct(entry_price, _safe_float(candle.get("close")), side)
                if first_tp_hit:
                    close_return = close_fraction * first_take_profit_pct + (1.0 - close_fraction) * close_return
                return _base_outcome(
                    signal,
                    branch,
                    round_trip_fee_bps=round_trip_fee_bps,
                    final_return_pct=close_return,
                    final_exit_reason="max_hold_timeout",
                    final_exit_time_ms=close_time_ms,
                    final_exit_price=_safe_float(candle.get("close")),
                    first_tp_hit=first_tp_hit,
                    time_to_first_tp_sec=first_tp_time_sec,
                    first_tp_return_pct=first_take_profit_pct if first_tp_hit else None,
                    max_runup_pct=max_runup,
                    max_drawdown_pct=max_drawdown,
                    close_fraction_at_first_tp=close_fraction,
                    move_stop_to_breakeven_after_first_tp=move_stop_to_breakeven,
                )

    elif exit_profile == EXIT_MOMENTUM_DECAY:
        no_extension_count = 0
        best_extreme = entry_price
        for candle in sorted_candles:
            close_time_ms = _safe_int(candle.get("close_time_ms"))
            close_price = _safe_float(candle.get("close"))
            high_price = _safe_float(candle.get("high"))
            low_price = _safe_float(candle.get("low"))
            favorable = _favorable_return_pct(entry_price, candle, side)
            adverse = _adverse_return_pct(entry_price, candle, side)
            max_runup = max(max_runup, favorable)
            max_drawdown = min(max_drawdown, adverse)
            elapsed_ms = close_time_ms - entry_time_ms
            if side == "BUY":
                extended = high_price > best_extreme
                if extended:
                    best_extreme = high_price
            else:
                extended = best_extreme <= 0 or low_price < best_extreme
                if extended:
                    best_extreme = low_price
            no_extension_count = 0 if extended else no_extension_count + 1
            volume_burst = _first_float(candle, "volume_burst_ratio", "one_min_volume_burst_ratio")
            oi_change = _first_float(candle, "oi_change_pct", "one_min_oi_change_pct")
            returned_to_trigger = close_price <= entry_price if side == "BUY" else close_price >= entry_price
            momentum_faded = (
                no_extension_count >= 2
                or (elapsed_ms > 0 and volume_burst < MomentumScalpConfig.decay_volume_burst_floor)
                or oi_change < MomentumScalpConfig.decay_oi_drop_floor_pct
                or returned_to_trigger
            )
            if momentum_faded and elapsed_ms > 0:
                return _base_outcome(
                    signal,
                    branch,
                    round_trip_fee_bps=round_trip_fee_bps,
                    final_return_pct=_side_aware_return_pct(entry_price, close_price, side),
                    final_exit_reason="momentum_decay",
                    final_exit_time_ms=close_time_ms,
                    final_exit_price=close_price,
                    first_tp_hit=False,
                    time_to_first_tp_sec=None,
                    first_tp_return_pct=None,
                    max_runup_pct=max_runup,
                    max_drawdown_pct=max_drawdown,
                )
            if elapsed_ms >= max_hold_ms:
                return _base_outcome(
                    signal,
                    branch,
                    round_trip_fee_bps=round_trip_fee_bps,
                    final_return_pct=_side_aware_return_pct(entry_price, close_price, side),
                    final_exit_reason="max_hold_timeout",
                    final_exit_time_ms=close_time_ms,
                    final_exit_price=close_price,
                    first_tp_hit=False,
                    time_to_first_tp_sec=None,
                    first_tp_return_pct=None,
                    max_runup_pct=max_runup,
                    max_drawdown_pct=max_drawdown,
                )

    last = sorted_candles[-1]
    last_close = _safe_float(last.get("close"))
    return _base_outcome(
        signal,
        branch,
        round_trip_fee_bps=round_trip_fee_bps,
        final_return_pct=_side_aware_return_pct(entry_price, last_close, side),
        final_exit_reason="path_exhausted",
        final_exit_time_ms=_safe_int(last.get("close_time_ms")),
        final_exit_price=last_close,
        first_tp_hit=False,
        time_to_first_tp_sec=None,
        first_tp_return_pct=None,
        max_runup_pct=max_runup,
        max_drawdown_pct=max_drawdown,
    )


def _max_losing_streak(returns: Iterable[float]) -> int:
    worst = 0
    current = 0
    for value in returns:
        if value < 0:
            current += 1
            worst = max(worst, current)
        else:
            current = 0
    return worst


def _max_drawdown_from_returns(returns: Iterable[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return abs(worst)


def _profit_factor(returns: Iterable[float]) -> float | None:
    gains = sum(value for value in returns if value > 0)
    losses = abs(sum(value for value in returns if value < 0))
    if losses <= 0:
        return 999.0 if gains > 0 else None
    return gains / losses


def _ranked_aggregate(rows: list[dict[str, Any]], key: str, *, reverse: bool) -> list[dict[str, Any]]:
    buckets: dict[str, list[float]] = {}
    for row in rows:
        bucket = str(row.get(key) or "unknown")
        buckets.setdefault(bucket, []).append(_safe_float(row.get("after_real_cost_return_pct")))
    ranked = []
    for name, values in buckets.items():
        ranked.append(
            {
                key: name,
                "trade_count": len(values),
                "total_after_real_cost_return": round(sum(values), 6),
                "avg_after_real_cost_return": round(sum(values) / len(values), 6) if values else 0.0,
            }
        )
    return sorted(ranked, key=lambda item: item["total_after_real_cost_return"], reverse=reverse)[:5]


def _top_symbol_pnl_share(rows: list[dict[str, Any]]) -> float:
    pnl_by_symbol: dict[str, float] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "unknown")
        pnl_by_symbol[symbol] = pnl_by_symbol.get(symbol, 0.0) + _safe_float(row.get("after_real_cost_return_pct"))
    positive_total = sum(value for value in pnl_by_symbol.values() if value > 0)
    if positive_total <= 0:
        return 0.0
    largest = max((value for value in pnl_by_symbol.values() if value > 0), default=0.0)
    return largest / positive_total * 100.0


def _value_distribution(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    distribution: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        distribution[value] = distribution.get(value, 0) + 1
    return dict(sorted(distribution.items()))


def _status_for_group(rows: list[dict[str, Any]], stats: dict[str, Any], config: MomentumScalpConfig) -> str:
    count = int(stats.get("shadow_trade_count") or 0)
    avg_return = _safe_float(stats.get("avg_after_real_cost_return"))
    profit_factor = _safe_float(stats.get("profit_factor_after_real_cost"), 0.0)
    first_tp_hit_rate = _safe_float(stats.get("first_tp_hit_rate"))
    top_symbol_share = _safe_float(stats.get("top_symbol_pnl_share"))
    max_drawdown = _safe_float(stats.get("max_drawdown"))
    fake_spike_rate = _safe_float(stats.get("fake_spike_rate"))
    losing_streak = int(stats.get("max_losing_streak") or 0)

    if (
        count >= config.promotion_min_trades
        and avg_return > 0
        and profit_factor >= config.promotion_min_profit_factor
        and first_tp_hit_rate >= config.promotion_min_first_tp_hit_rate
        and top_symbol_share <= config.promotion_max_top_symbol_pnl_share
        and max_drawdown <= config.promotion_max_drawdown_pct
    ):
        return STATUS_PROMOTION_CANDIDATE_SHADOW_ONLY
    if (
        avg_return <= config.paused_negative_avg_return_pct
        or fake_spike_rate >= config.paused_fake_spike_rate
        or losing_streak >= config.paused_max_losing_streak
    ):
        return STATUS_PAUSED
    if count >= max(30, config.promotion_min_trades // 3) and (avg_return <= 0 or profit_factor < 1.0):
        return STATUS_WATCHLIST
    return STATUS_ACTIVE_SHADOW


def build_strategy_shadow_league_report(
    *,
    generated_at: str,
    shadow_signals: list[dict[str, Any]],
    shadow_outcomes: list[dict[str, Any]],
    config: MomentumScalpConfig | None = None,
) -> dict[str, Any]:
    """Aggregate strategy_id + entry_profile + exit_profile performance."""

    config = config or MomentumScalpConfig()
    family_signals = [
        row for row in shadow_signals if row.get("strategy_family") == STRATEGY_FAMILY
    ]
    family_outcomes = [
        row for row in shadow_outcomes if row.get("strategy_family") == STRATEGY_FAMILY
    ]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in family_outcomes:
        key = (
            str(row.get("strategy_id") or "unknown"),
            str(row.get("entry_profile") or "unknown"),
            str(row.get("exit_profile") or "unknown"),
        )
        grouped.setdefault(key, []).append(row)

    rows: list[dict[str, Any]] = []
    for (strategy_id, entry_profile, exit_profile), items in sorted(grouped.items()):
        returns = [_safe_float(item.get("after_real_cost_return_pct")) for item in items]
        wins = [value for value in returns if value > 0]
        losses = [value for value in returns if value < 0]
        time_to_tp = [
            _safe_float(item.get("time_to_first_tp_sec"))
            for item in items
            if item.get("time_to_first_tp_sec") is not None
        ]
        holding_minutes = [_safe_float(item.get("holding_minutes")) for item in items]
        first_tp_rate = sum(1 for item in items if bool(item.get("first_tp_hit"))) / len(items) if items else 0.0
        fake_spike_rate = sum(1 for item in items if bool(item.get("fake_spike_flag"))) / len(items) if items else 0.0
        profit_factor = _profit_factor(returns)
        stats = {
            "strategy_family": STRATEGY_FAMILY,
            "strategy_id": strategy_id,
            "entry_profile": entry_profile,
            "exit_profile": exit_profile,
            "shadow_trade_count": len(items),
            "first_tp_hit_rate": round(first_tp_rate, 6),
            "fake_spike_rate": round(fake_spike_rate, 6),
            "win_rate": round(len(wins) / len(items), 6) if items else 0.0,
            "avg_after_real_cost_return": round(sum(returns) / len(returns), 8) if returns else 0.0,
            "median_after_real_cost_return": round(statistics.median(returns), 8) if returns else 0.0,
            "profit_factor_after_real_cost": round(profit_factor, 6) if profit_factor is not None else None,
            "avg_time_to_first_tp_sec": round(sum(time_to_tp) / len(time_to_tp), 6) if time_to_tp else None,
            "avg_holding_minutes": round(sum(holding_minutes) / len(holding_minutes), 6) if holding_minutes else 0.0,
            "max_losing_streak": _max_losing_streak(returns),
            "max_drawdown": round(_max_drawdown_from_returns(returns), 8),
            "best_symbols": _ranked_aggregate(items, "symbol", reverse=True),
            "worst_symbols": _ranked_aggregate(items, "symbol", reverse=False),
            "best_sessions": _ranked_aggregate(items, "trading_session", reverse=True),
            "worst_sessions": _ranked_aggregate(items, "trading_session", reverse=False),
            "top_symbol_pnl_share": round(_top_symbol_pnl_share(items), 6),
            "oi_confirmation_source_counts": _value_distribution(items, "oi_confirmation_source"),
            "oi5_direction_bucket_counts": _value_distribution(items, "oi5_direction_bucket"),
            "live_trading_enabled": False,
            "promotion_allowed": False,
        }
        stats["status"] = _status_for_group(items, stats, config)
        rows.append(stats)

    configured_strategy_ids = sorted({str(item.get("strategy_id")) for item in default_exit_branch_specs(ENTRY_PROFILE_FAST)})
    configured_strategy_ids.extend(
        strategy_id
        for strategy_id in sorted({str(item.get("strategy_id")) for item in default_exit_branch_specs(ENTRY_PROFILE_CONFIRM)})
        if strategy_id not in configured_strategy_ids
    )
    return {
        "report_type": "strategy_shadow_league_report",
        "generated_at": generated_at,
        "strategy_family": STRATEGY_FAMILY,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "paper_record_only": True,
        "input_counts": {
            "shadow_signals": len(family_signals),
            "shadow_outcomes": len(family_outcomes),
        },
        "configured_strategy_ids": configured_strategy_ids,
        "strategies": rows,
    }


def build_strategy_shadow_league_markdown(report: dict[str, Any]) -> str:
    """Render the shadow league as a compact Markdown report."""

    generated_at = str(report.get("generated_at") or "")
    family = str(report.get("strategy_family") or STRATEGY_FAMILY)
    lines = [
        f"# Strategy Shadow League Report - {family}",
        "",
        f"- generated_at: {generated_at}",
        "- live_trading_enabled: false",
        "- promotion_allowed: false",
        "- mode: shadow_only / paper_record_only",
        "",
        "| strategy_id | entry | exit | trades | win_rate | avg_real_cost_return | profit_factor | first_tp | top_symbol_share | status |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    strategies = report.get("strategies") if isinstance(report.get("strategies"), list) else []
    if not strategies:
        lines.append("| n/a | n/a | n/a | 0 | 0 | 0 | n/a | 0 | 0 | active_shadow |")
    for row in strategies:
        lines.append(
            "| {strategy_id} | {entry_profile} | {exit_profile} | {shadow_trade_count} | "
            "{win_rate} | {avg_after_real_cost_return} | {profit_factor_after_real_cost} | "
            "{first_tp_hit_rate} | {top_symbol_pnl_share} | {status} |".format(
                strategy_id=row.get("strategy_id"),
                entry_profile=row.get("entry_profile"),
                exit_profile=row.get("exit_profile"),
                shadow_trade_count=row.get("shadow_trade_count"),
                win_rate=row.get("win_rate"),
                avg_after_real_cost_return=row.get("avg_after_real_cost_return"),
                profit_factor_after_real_cost=row.get("profit_factor_after_real_cost"),
                first_tp_hit_rate=row.get("first_tp_hit_rate"),
                top_symbol_pnl_share=row.get("top_symbol_pnl_share"),
                status=row.get("status"),
            )
        )
    lines.append("")
    lines.append("No row can promote itself to live; promotion_candidate_shadow_only remains paper-only.")
    return "\n".join(lines) + "\n"
