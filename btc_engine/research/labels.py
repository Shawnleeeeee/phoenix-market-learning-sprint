"""Direction labels for BTC research datasets."""

from __future__ import annotations

from typing import Any


def _horizon_metrics(rows: list[dict[str, Any]], idx: int, horizon_bars: int) -> tuple[float, float, float] | None:
    total = len(rows)
    if idx + horizon_bars >= total:
        return None
    entry = float(rows[idx]["close"])
    future_slice = rows[idx + 1 : idx + 1 + horizon_bars]
    future_close = float(future_slice[-1]["close"])
    future_high = max(float(item["high"]) for item in future_slice)
    future_low = min(float(item["low"]) for item in future_slice)
    future_return = ((future_close - entry) / entry) * 100.0
    future_max_up = ((future_high - entry) / entry) * 100.0
    future_max_down = ((future_low - entry) / entry) * 100.0
    return future_return, future_max_up, future_max_down


def _direction_label(
    future_return: float,
    future_max_up: float,
    future_max_down: float,
    *,
    direction_threshold_pct: float,
    adverse_move_limit_pct: float,
) -> str:
    if future_return >= direction_threshold_pct and future_max_down > -adverse_move_limit_pct:
        return "LONG"
    if future_return <= -direction_threshold_pct and future_max_up < adverse_move_limit_pct:
        return "SHORT"
    return "NONE"


def _opportunity_labels(
    future_max_up: float,
    future_max_down: float,
    *,
    opportunity_target_pct: float,
    adverse_move_limit_pct: float,
) -> tuple[int, int]:
    return (
        int(future_max_up >= opportunity_target_pct and future_max_down > -adverse_move_limit_pct),
        int(abs(future_max_down) >= opportunity_target_pct and future_max_up < adverse_move_limit_pct),
    )


def _entry_edges(
    future_max_up: float,
    future_max_down: float,
    *,
    opportunity_target_pct: float,
    adverse_move_limit_pct: float,
    cost_buffer_pct: float,
) -> tuple[float, float]:
    long_edge = future_max_up - max(0.0, abs(min(future_max_down, 0.0))) - cost_buffer_pct
    short_edge = abs(min(future_max_down, 0.0)) - max(0.0, future_max_up) - cost_buffer_pct
    if future_max_down <= -adverse_move_limit_pct:
        long_edge -= abs(future_max_down) - adverse_move_limit_pct
    if future_max_up >= adverse_move_limit_pct:
        short_edge -= future_max_up - adverse_move_limit_pct
    if future_max_up < opportunity_target_pct:
        long_edge -= (opportunity_target_pct - future_max_up) * 0.5
    if abs(min(future_max_down, 0.0)) < opportunity_target_pct:
        short_edge -= (opportunity_target_pct - abs(min(future_max_down, 0.0))) * 0.5
    return round(long_edge, 6), round(short_edge, 6)


def _entry_quality_label(best_side: str | None, best_edge: float, *, entry_edge_high_pct: float, entry_edge_min_pct: float) -> str:
    if not best_side or best_edge < entry_edge_min_pct:
        return "NONE"
    if best_edge >= entry_edge_high_pct:
        return f"HIGH_{best_side}"
    return f"MEDIUM_{best_side}"


def _single_side_quality_label(edge: float, *, side: str, entry_edge_high_pct: float, entry_edge_min_pct: float) -> str:
    if edge < entry_edge_min_pct:
        return "NONE"
    if edge >= entry_edge_high_pct:
        return f"HIGH_{side}"
    return f"MEDIUM_{side}"


def _detect_regime(row: dict[str, Any]) -> str:
    realized_vol_12 = float(row.get("realized_vol_12") or 0.0)
    realized_vol_36 = float(row.get("realized_vol_36") or 0.0)
    ema_cross = abs(float(row.get("ema_cross_12_24_pct") or 0.0))
    return_12 = abs(float(row.get("return_12_pct") or 0.0))
    bb_width = float(row.get("bollinger_width_20_pct") or 0.0)
    if ema_cross >= 0.18 and return_12 >= 0.35:
        return "trend"
    if realized_vol_12 >= max(0.45, realized_vol_36 * 1.2) or bb_width >= 1.25:
        return "expansion"
    if realized_vol_12 <= max(0.12, realized_vol_36 * 0.75) and bb_width <= 0.75:
        return "compression"
    return "range"


def _primary_horizon_for_regime(regime: str, *, base: int, fast: int, slow: int) -> int:
    if regime == "expansion":
        return fast
    if regime in {"trend", "compression"}:
        return slow
    return base


def attach_direction_labels(
    rows: list[dict[str, Any]],
    *,
    horizon_bars: int = 12,
    horizon_fast_bars: int = 6,
    horizon_slow_bars: int = 24,
    direction_threshold_pct: float = 0.25,
    opportunity_target_pct: float = 0.35,
    adverse_move_limit_pct: float = 0.35,
    entry_edge_min_pct: float = 0.10,
    entry_edge_high_pct: float = 0.25,
    cost_buffer_pct: float = 0.06,
) -> list[dict[str, Any]]:
    """Attach forward-looking labels without mutating the input list."""
    labeled: list[dict[str, Any]] = []
    total = len(rows)
    for idx, row in enumerate(rows):
        enriched = dict(row)
        fast_metrics = _horizon_metrics(rows, idx, horizon_fast_bars)
        base_metrics = _horizon_metrics(rows, idx, horizon_bars)
        slow_metrics = _horizon_metrics(rows, idx, horizon_slow_bars)
        if fast_metrics is None or base_metrics is None or slow_metrics is None:
            enriched.update(
                {
                    "future_return_horizon_pct": None,
                    "future_max_up_horizon_pct": None,
                    "future_max_down_horizon_pct": None,
                    "future_return_fast_pct": None,
                    "future_max_up_fast_pct": None,
                    "future_max_down_fast_pct": None,
                    "future_return_slow_pct": None,
                    "future_max_up_slow_pct": None,
                    "future_max_down_slow_pct": None,
                    "label_regime": None,
                    "label_horizon_bars": None,
                    "label_direction_fast": None,
                    "label_direction_base": None,
                    "label_direction_slow": None,
                    "label_direction": None,
                    "label_direction_regime": None,
                    "label_long_opportunity": None,
                    "label_short_opportunity": None,
                    "label_long_opportunity_fast": None,
                    "label_short_opportunity_fast": None,
                    "label_long_opportunity_slow": None,
                    "label_short_opportunity_slow": None,
                    "future_long_edge_fast_pct": None,
                    "future_short_edge_fast_pct": None,
                    "future_long_edge_horizon_pct": None,
                    "future_short_edge_horizon_pct": None,
                    "future_long_edge_slow_pct": None,
                    "future_short_edge_slow_pct": None,
                    "future_entry_edge_horizon_pct": None,
                    "label_entry_quality_fast": None,
                    "label_entry_quality_base": None,
                    "label_entry_quality_slow": None,
                    "label_entry_quality": None,
                }
            )
            labeled.append(enriched)
            continue

        future_return_fast, future_max_up_fast, future_max_down_fast = fast_metrics
        future_return, future_max_up, future_max_down = base_metrics
        future_return_slow, future_max_up_slow, future_max_down_slow = slow_metrics

        label_direction_fast = _direction_label(
            future_return_fast,
            future_max_up_fast,
            future_max_down_fast,
            direction_threshold_pct=direction_threshold_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
        )
        label_direction_base = _direction_label(
            future_return,
            future_max_up,
            future_max_down,
            direction_threshold_pct=direction_threshold_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
        )
        label_direction_slow = _direction_label(
            future_return_slow,
            future_max_up_slow,
            future_max_down_slow,
            direction_threshold_pct=direction_threshold_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
        )
        label_long_opportunity_fast, label_short_opportunity_fast = _opportunity_labels(
            future_max_up_fast,
            future_max_down_fast,
            opportunity_target_pct=opportunity_target_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
        )
        label_long_opportunity, label_short_opportunity = _opportunity_labels(
            future_max_up,
            future_max_down,
            opportunity_target_pct=opportunity_target_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
        )
        label_long_opportunity_slow, label_short_opportunity_slow = _opportunity_labels(
            future_max_up_slow,
            future_max_down_slow,
            opportunity_target_pct=opportunity_target_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
        )
        long_edge_fast, short_edge_fast = _entry_edges(
            future_max_up_fast,
            future_max_down_fast,
            opportunity_target_pct=opportunity_target_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
            cost_buffer_pct=cost_buffer_pct,
        )
        long_edge_base, short_edge_base = _entry_edges(
            future_max_up,
            future_max_down,
            opportunity_target_pct=opportunity_target_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
            cost_buffer_pct=cost_buffer_pct,
        )
        long_edge_slow, short_edge_slow = _entry_edges(
            future_max_up_slow,
            future_max_down_slow,
            opportunity_target_pct=opportunity_target_pct,
            adverse_move_limit_pct=adverse_move_limit_pct,
            cost_buffer_pct=cost_buffer_pct,
        )
        quality_fast = _entry_quality_label(
            "LONG" if long_edge_fast >= short_edge_fast else "SHORT",
            max(long_edge_fast, short_edge_fast),
            entry_edge_high_pct=entry_edge_high_pct,
            entry_edge_min_pct=entry_edge_min_pct,
        )
        quality_long_fast = _single_side_quality_label(
            long_edge_fast,
            side="LONG",
            entry_edge_high_pct=entry_edge_high_pct,
            entry_edge_min_pct=entry_edge_min_pct,
        )
        quality_base = _entry_quality_label(
            "LONG" if long_edge_base >= short_edge_base else "SHORT",
            max(long_edge_base, short_edge_base),
            entry_edge_high_pct=entry_edge_high_pct,
            entry_edge_min_pct=entry_edge_min_pct,
        )
        quality_long_base = _single_side_quality_label(
            long_edge_base,
            side="LONG",
            entry_edge_high_pct=entry_edge_high_pct,
            entry_edge_min_pct=entry_edge_min_pct,
        )
        quality_slow = _entry_quality_label(
            "LONG" if long_edge_slow >= short_edge_slow else "SHORT",
            max(long_edge_slow, short_edge_slow),
            entry_edge_high_pct=entry_edge_high_pct,
            entry_edge_min_pct=entry_edge_min_pct,
        )
        quality_long_slow = _single_side_quality_label(
            long_edge_slow,
            side="LONG",
            entry_edge_high_pct=entry_edge_high_pct,
            entry_edge_min_pct=entry_edge_min_pct,
        )
        label_regime = _detect_regime(row)
        label_horizon_bars = _primary_horizon_for_regime(
            label_regime,
            base=horizon_bars,
            fast=horizon_fast_bars,
            slow=horizon_slow_bars,
        )
        if label_horizon_bars == horizon_fast_bars:
            label_direction_regime = label_direction_fast
            future_return_horizon = future_return_fast
            future_max_up_horizon = future_max_up_fast
            future_max_down_horizon = future_max_down_fast
            future_long_edge_horizon = long_edge_fast
            future_short_edge_horizon = short_edge_fast
            label_entry_quality = quality_fast
            label_entry_quality_long = quality_long_fast
        elif label_horizon_bars == horizon_slow_bars:
            label_direction_regime = label_direction_slow
            future_return_horizon = future_return_slow
            future_max_up_horizon = future_max_up_slow
            future_max_down_horizon = future_max_down_slow
            future_long_edge_horizon = long_edge_slow
            future_short_edge_horizon = short_edge_slow
            label_entry_quality = quality_slow
            label_entry_quality_long = quality_long_slow
        else:
            label_direction_regime = label_direction_base
            future_return_horizon = future_return
            future_max_up_horizon = future_max_up
            future_max_down_horizon = future_max_down
            future_long_edge_horizon = long_edge_base
            future_short_edge_horizon = short_edge_base
            label_entry_quality = quality_base
            label_entry_quality_long = quality_long_base

        if future_long_edge_horizon >= future_short_edge_horizon and future_long_edge_horizon >= entry_edge_min_pct:
            label_direction_regime = "LONG"
            future_entry_edge_horizon = future_long_edge_horizon
        elif future_short_edge_horizon > future_long_edge_horizon and future_short_edge_horizon >= entry_edge_min_pct:
            label_direction_regime = "SHORT"
            future_entry_edge_horizon = -future_short_edge_horizon
        else:
            label_direction_regime = "NONE"
            future_entry_edge_horizon = 0.0

        enriched.update(
            {
                "future_return_fast_pct": round(future_return_fast, 6),
                "future_max_up_fast_pct": round(future_max_up_fast, 6),
                "future_max_down_fast_pct": round(future_max_down_fast, 6),
                "future_return_horizon_pct": round(future_return_horizon, 6),
                "future_max_up_horizon_pct": round(future_max_up_horizon, 6),
                "future_max_down_horizon_pct": round(future_max_down_horizon, 6),
                "future_return_slow_pct": round(future_return_slow, 6),
                "future_max_up_slow_pct": round(future_max_up_slow, 6),
                "future_max_down_slow_pct": round(future_max_down_slow, 6),
                "label_regime": label_regime,
                "label_horizon_bars": label_horizon_bars,
                "label_direction_fast": label_direction_fast,
                "label_direction_base": label_direction_base,
                "label_direction_slow": label_direction_slow,
                "label_direction": label_direction_regime,
                "label_direction_regime": label_direction_regime,
                "label_entry_quality_fast": quality_fast,
                "label_entry_quality_base": quality_base,
                "label_entry_quality_slow": quality_slow,
                "label_entry_quality": label_entry_quality,
                "label_entry_quality_long_fast": quality_long_fast,
                "label_entry_quality_long_base": quality_long_base,
                "label_entry_quality_long_slow": quality_long_slow,
                "label_entry_quality_long": label_entry_quality_long,
                "label_direction_long_fast": "LONG" if long_edge_fast >= entry_edge_min_pct else "NONE",
                "label_direction_long_base": "LONG" if long_edge_base >= entry_edge_min_pct else "NONE",
                "label_direction_long_slow": "LONG" if long_edge_slow >= entry_edge_min_pct else "NONE",
                "label_direction_long": "LONG" if future_long_edge_horizon >= entry_edge_min_pct else "NONE",
                "label_long_opportunity": label_long_opportunity,
                "label_short_opportunity": label_short_opportunity,
                "label_long_opportunity_fast": label_long_opportunity_fast,
                "label_short_opportunity_fast": label_short_opportunity_fast,
                "label_long_opportunity_slow": label_long_opportunity_slow,
                "label_short_opportunity_slow": label_short_opportunity_slow,
                "future_long_edge_fast_pct": long_edge_fast,
                "future_short_edge_fast_pct": short_edge_fast,
                "future_long_edge_horizon_pct": future_long_edge_horizon,
                "future_short_edge_horizon_pct": future_short_edge_horizon,
                "future_long_edge_slow_pct": long_edge_slow,
                "future_short_edge_slow_pct": short_edge_slow,
                "future_entry_edge_horizon_pct": round(future_entry_edge_horizon, 6),
                "future_entry_edge_long_horizon_pct": round(future_long_edge_horizon, 6),
            }
        )
        labeled.append(enriched)
    return labeled
