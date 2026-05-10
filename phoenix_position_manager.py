from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class PricePoint:
    seconds_since_entry: float
    price: float
    volume_burst_ratio: float | None = None
    oi_change_pct: float | None = None
    momentum_score: float | None = None


@dataclass(frozen=True, slots=True)
class DynamicExitConfig:
    hard_stop_loss_pct: float = 1.2
    breakeven_guard_trigger_pct: float = 0.45
    breakeven_lock_pct: float = 0.02
    partial_take_profit_pct: float = 0.8
    partial_close_fraction: float = 0.5
    trailing_activation_pct: float = 1.0
    trailing_callback_pct: float = 0.35
    momentum_decay_bars: int = 2
    no_follow_through_sec: float = 90.0
    no_follow_through_min_profit_pct: float = 0.18
    danger_exit_adverse_pct: float = 0.75
    time_decay_sec: float = 300.0


@dataclass(slots=True)
class DynamicExitResult:
    exit_reason: str
    exit_price: float
    exit_seconds: float
    final_return_pct: float
    mfe_pct: float
    mae_pct: float
    time_to_first_profit_sec: float | None
    time_to_mfe_sec: float | None
    profit_giveback_pct: float
    triggered_early_exit: bool
    partial_take_profit_hit: bool = False
    breakeven_guard_armed: bool = False
    trailing_stop_armed: bool = False
    early_exit_signals: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def side_return_pct(side: str, entry_price: float, price: float) -> float:
    if entry_price <= 0 or price <= 0:
        return 0.0
    if str(side).upper() == "SELL":
        return (entry_price / price - 1.0) * 100.0
    return (price / entry_price - 1.0) * 100.0


def adverse_return_pct(side: str, entry_price: float, price: float) -> float:
    return min(0.0, side_return_pct(side, entry_price, price))


def _price_from_return(side: str, entry_price: float, return_pct: float) -> float:
    if str(side).upper() == "SELL":
        return entry_price / (1.0 + return_pct / 100.0)
    return entry_price * (1.0 + return_pct / 100.0)


def _momentum_decayed(points: list[PricePoint], side: str, bars: int) -> bool:
    if len(points) < bars + 1:
        return False
    recent = points[-(bars + 1):]
    prices = [point.price for point in recent]
    if str(side).upper() == "SELL":
        return all(prices[index] >= prices[index - 1] for index in range(1, len(prices)))
    return all(prices[index] <= prices[index - 1] for index in range(1, len(prices)))


def evaluate_dynamic_exit(
    *,
    side: str,
    entry_price: float,
    price_points: Iterable[PricePoint | dict[str, Any]],
    config: DynamicExitConfig | None = None,
) -> DynamicExitResult:
    cfg = config or DynamicExitConfig()
    points = [
        point if isinstance(point, PricePoint) else PricePoint(
            seconds_since_entry=float(point.get("seconds_since_entry", point.get("t", 0.0)) or 0.0),
            price=float(point.get("price", 0.0) or 0.0),
            volume_burst_ratio=point.get("volume_burst_ratio"),
            oi_change_pct=point.get("oi_change_pct"),
            momentum_score=point.get("momentum_score"),
        )
        for point in price_points
    ]
    points = [point for point in points if point.price > 0]
    if not points:
        return DynamicExitResult(
            exit_reason="no_price_points",
            exit_price=entry_price,
            exit_seconds=0.0,
            final_return_pct=0.0,
            mfe_pct=0.0,
            mae_pct=0.0,
            time_to_first_profit_sec=None,
            time_to_mfe_sec=None,
            profit_giveback_pct=0.0,
            triggered_early_exit=False,
        )

    best_return = 0.0
    worst_return = 0.0
    time_to_mfe: float | None = None
    time_to_first_profit: float | None = None
    stop_return = -abs(cfg.hard_stop_loss_pct)
    trailing_stop_return: float | None = None
    partial_take_profit_hit = False
    breakeven_armed = False
    trailing_armed = False
    early_signals: list[str] = []
    seen: list[PricePoint] = []

    exit_point = points[-1]
    exit_reason = "time_decay_exit"
    for point in points:
        current_return = side_return_pct(side, entry_price, point.price)
        best_return = max(best_return, current_return)
        worst_return = min(worst_return, current_return)
        if best_return == current_return:
            time_to_mfe = point.seconds_since_entry
        if time_to_first_profit is None and current_return > 0:
            time_to_first_profit = point.seconds_since_entry

        if current_return <= stop_return:
            exit_reason = "hard_stop_loss"
            exit_point = point
            break

        if current_return >= cfg.partial_take_profit_pct and not partial_take_profit_hit:
            partial_take_profit_hit = True
            early_signals.append("partial_take_profit")

        if current_return >= cfg.breakeven_guard_trigger_pct:
            breakeven_armed = True
            stop_return = max(stop_return, cfg.breakeven_lock_pct)
            early_signals.append("breakeven_guard")

        if current_return >= cfg.trailing_activation_pct:
            trailing_armed = True
            trailing_stop_return = max(
                trailing_stop_return if trailing_stop_return is not None else stop_return,
                best_return - cfg.trailing_callback_pct,
            )
            stop_return = max(stop_return, trailing_stop_return)
            early_signals.append("trailing_stop")

        if current_return <= -abs(cfg.danger_exit_adverse_pct) and best_return < cfg.no_follow_through_min_profit_pct:
            exit_reason = "danger_exit_before_stop_loss"
            exit_point = point
            early_signals.append("danger_exit_before_stop_loss")
            break

        if point.seconds_since_entry >= cfg.no_follow_through_sec and best_return < cfg.no_follow_through_min_profit_pct:
            exit_reason = "no_follow_through_exit"
            exit_point = point
            early_signals.append("no_follow_through_exit")
            break

        seen.append(point)
        if _momentum_decayed(seen, side, cfg.momentum_decay_bars) and best_return >= cfg.breakeven_guard_trigger_pct:
            exit_reason = "momentum_decay_exit"
            exit_point = point
            early_signals.append("momentum_decay_exit")
            break

        if point.seconds_since_entry >= cfg.time_decay_sec:
            exit_reason = "time_decay_exit"
            exit_point = point
            early_signals.append("time_decay_exit")
            break

    final_return = side_return_pct(side, entry_price, exit_point.price)
    giveback = max(0.0, best_return - final_return)
    unique_signals = list(dict.fromkeys(early_signals))
    return DynamicExitResult(
        exit_reason=exit_reason,
        exit_price=exit_point.price,
        exit_seconds=exit_point.seconds_since_entry,
        final_return_pct=round(final_return, 6),
        mfe_pct=round(best_return, 6),
        mae_pct=round(abs(worst_return), 6),
        time_to_first_profit_sec=time_to_first_profit,
        time_to_mfe_sec=time_to_mfe,
        profit_giveback_pct=round(giveback, 6),
        triggered_early_exit=exit_reason != "hard_stop_loss" and exit_reason != "time_decay_exit",
        partial_take_profit_hit=partial_take_profit_hit,
        breakeven_guard_armed=breakeven_armed,
        trailing_stop_armed=trailing_armed,
        early_exit_signals=unique_signals,
    )


def summarize_trade_path_from_record(record: dict[str, Any]) -> dict[str, Any]:
    entry = float(record.get("entry_price") or 0.0)
    best = float(record.get("best_mark_price") or entry or 0.0)
    worst = float(record.get("worst_mark_price") or entry or 0.0)
    side = str(record.get("side") or "BUY").upper()
    mfe = max(0.0, side_return_pct(side, entry, best if side == "BUY" else worst)) if entry > 0 else 0.0
    mae = abs(min(0.0, side_return_pct(side, entry, worst if side == "BUY" else best))) if entry > 0 else 0.0
    close_price = float(record.get("close_price") or entry or 0.0)
    final_return = side_return_pct(side, entry, close_price) if entry > 0 and close_price > 0 else 0.0
    return {
        "mfe_pct": round(mfe, 6),
        "mae_pct": round(mae, 6),
        "profit_giveback_pct": round(max(0.0, mfe - final_return), 6),
        "final_return_pct": round(final_return, 6),
    }
