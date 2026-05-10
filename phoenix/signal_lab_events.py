from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Sequence


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def pct_change(current: float, past: float) -> float:
    if current <= 0 or past <= 0:
        return 0.0
    return ((current / past) - 1.0) * 100.0


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


@dataclass(slots=True)
class EventTriggerConfig:
    atr_period: int = 20
    atr_multiplier: float = 2.0
    volume_lookback: int = 20
    volume_multiplier: float = 3.0
    atr_multiplier_1m: float | None = 1.5
    volume_multiplier_1m: float | None = 2.0
    atr_multiplier_5m: float | None = 1.5
    volume_multiplier_5m: float | None = 2.25
    min_price: float = 0.01
    min_quote_volume_24h: float = 1_000_000.0
    min_avg_quote_turnover_1m: float = 50_000.0
    min_current_quote_turnover_1m: float = 5_000.0


@dataclass(slots=True)
class MarketEventContext:
    symbol: str
    bar_interval: str
    anchor_open_time_ms: int
    anchor_close_time_ms: int
    price: float
    quote_volume_24h: float
    price_change_24h_pct: float
    current_volume: float
    avg_volume_20: float
    volume_burst_ratio: float
    current_quote_turnover: float
    avg_quote_turnover_20: float
    candle_body_pct: float
    candle_range_pct: float
    atr_20_pct: float
    body_to_atr: float
    range_to_atr: float
    ret_1bar_pct: float
    ret_5bar_pct: float
    ret_15bar_pct: float
    ret_60bar_pct: float
    candle_direction: str

    def to_event_sample(
        self,
        *,
        sample_type: str,
        trigger_types: Sequence[str],
        trigger_score: float,
    ) -> "MarketEventSample":
        return MarketEventSample(
            symbol=self.symbol,
            sample_type=str(sample_type),
            trigger_types=tuple(str(token) for token in trigger_types),
            trigger_score=float(trigger_score),
            bar_interval=self.bar_interval,
            anchor_open_time_ms=self.anchor_open_time_ms,
            anchor_close_time_ms=self.anchor_close_time_ms,
            price=self.price,
            quote_volume_24h=self.quote_volume_24h,
            price_change_24h_pct=self.price_change_24h_pct,
            current_volume=self.current_volume,
            avg_volume_20=self.avg_volume_20,
            volume_burst_ratio=self.volume_burst_ratio,
            current_quote_turnover=self.current_quote_turnover,
            avg_quote_turnover_20=self.avg_quote_turnover_20,
            candle_body_pct=self.candle_body_pct,
            candle_range_pct=self.candle_range_pct,
            atr_20_pct=self.atr_20_pct,
            body_to_atr=self.body_to_atr,
            range_to_atr=self.range_to_atr,
            ret_1bar_pct=self.ret_1bar_pct,
            ret_5bar_pct=self.ret_5bar_pct,
            ret_15bar_pct=self.ret_15bar_pct,
            ret_60bar_pct=self.ret_60bar_pct,
            candle_direction=self.candle_direction,
        )


@dataclass(slots=True)
class MarketEventSample:
    symbol: str
    sample_type: str
    trigger_types: tuple[str, ...]
    trigger_score: float
    bar_interval: str
    anchor_open_time_ms: int
    anchor_close_time_ms: int
    price: float
    quote_volume_24h: float
    price_change_24h_pct: float
    current_volume: float
    avg_volume_20: float
    volume_burst_ratio: float
    current_quote_turnover: float
    avg_quote_turnover_20: float
    candle_body_pct: float
    candle_range_pct: float
    atr_20_pct: float
    body_to_atr: float
    range_to_atr: float
    ret_1bar_pct: float
    ret_5bar_pct: float
    ret_15bar_pct: float
    ret_60bar_pct: float
    candle_direction: str
    trigger_anchor_open_time_ms: int | None = None
    trigger_anchor_close_time_ms: int | None = None
    trigger_candle_direction: str | None = None
    confirmation_candle_open_price: float | None = None
    confirmation_candle_close_price: float | None = None
    confirmation_candle_direction: str | None = None
    reversal_confirmation_passed: bool = False
    reversal_confirmation_bar_interval: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OrderBookMetrics:
    spread_bps: float | None
    bid_depth_notional_5: float | None
    ask_depth_notional_5: float | None
    depth_imbalance: float | None
    estimated_slippage_bps: float | None
    estimated_slippage_for_order_usdt: float | None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FuturePathLabel:
    horizon_sec: int
    candle_count: int
    close_price: float
    close_return_pct: float
    after_fee_return_pct: float
    max_runup_pct: float
    max_drawdown_pct: float

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _extract_quote_volume_24h(ticker_item: dict[str, Any]) -> float:
    return safe_float(
        ticker_item.get("quoteVolume")
        if "quoteVolume" in ticker_item
        else ticker_item.get("quote_volume_24h")
    )


def _extract_price_change_24h_pct(ticker_item: dict[str, Any]) -> float:
    return safe_float(
        ticker_item.get("priceChangePercent")
        if "priceChangePercent" in ticker_item
        else ticker_item.get("price_change_24h_pct")
    )


def _true_range(*, high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def _candle_direction_from_prices(*, open_price: float, close_price: float) -> str:
    if close_price > open_price:
        return "up"
    if close_price < open_price:
        return "down"
    return "flat"


def is_reversal_confirmation_passed(*, trigger_direction: str, confirmation_direction: str) -> bool:
    normalized_trigger = str(trigger_direction or "").strip().lower()
    normalized_confirmation = str(confirmation_direction or "").strip().lower()
    return (
        (normalized_trigger == "down" and normalized_confirmation == "up")
        or (normalized_trigger == "up" and normalized_confirmation == "down")
    )


def build_market_event_context(
    ticker_item: dict[str, Any],
    candles_1m: list[list[Any]],
    *,
    config: EventTriggerConfig,
    bar_interval: str = "1m",
    anchor_offset: int = 1,
) -> MarketEventContext | None:
    anchor_offset = max(1, int(anchor_offset))
    required_lookback = max(config.atr_period, config.volume_lookback)
    min_history = required_lookback + anchor_offset + 1
    if len(candles_1m) < min_history:
        return None
    current_index = len(candles_1m) - 1 - anchor_offset
    if current_index < required_lookback:
        return None

    opens = [safe_float(row[1]) for row in candles_1m]
    highs = [safe_float(row[2]) for row in candles_1m]
    lows = [safe_float(row[3]) for row in candles_1m]
    closes = [safe_float(row[4]) for row in candles_1m]
    volumes = [safe_float(row[5]) for row in candles_1m]
    anchor_open_time_ms = int(candles_1m[current_index][0])
    anchor_close_time_ms = int(candles_1m[current_index][6])
    price = closes[current_index]
    if price < config.min_price:
        return None

    quote_volume_24h = _extract_quote_volume_24h(ticker_item)
    if quote_volume_24h < config.min_quote_volume_24h:
        return None

    lookback_start = current_index - config.volume_lookback
    prev_volumes = volumes[lookback_start:current_index]
    avg_volume_20 = _mean(prev_volumes)
    current_volume = volumes[current_index]
    if avg_volume_20 <= 0 or current_volume <= 0:
        return None

    current_quote_turnover = price * current_volume
    avg_quote_turnover_20 = _mean(
        [closes[index] * volumes[index] for index in range(lookback_start, current_index)]
    )
    if avg_quote_turnover_20 < config.min_avg_quote_turnover_1m:
        return None
    if current_quote_turnover < config.min_current_quote_turnover_1m:
        return None

    atr_start = current_index - config.atr_period + 1
    true_ranges = [
        _true_range(
            high=highs[index],
            low=lows[index],
            prev_close=closes[index - 1],
        )
        for index in range(atr_start, current_index + 1)
    ]
    atr = _mean(true_ranges)
    if atr <= 0:
        return None

    candle_body = abs(closes[current_index] - opens[current_index])
    candle_range = highs[current_index] - lows[current_index]
    body_to_atr = candle_body / atr
    range_to_atr = candle_range / atr
    candle_direction = _candle_direction_from_prices(
        open_price=opens[current_index],
        close_price=closes[current_index],
    )
    return MarketEventContext(
        symbol=str(ticker_item.get("symbol") or "").upper(),
        bar_interval=str(bar_interval),
        anchor_open_time_ms=anchor_open_time_ms,
        anchor_close_time_ms=anchor_close_time_ms,
        price=price,
        quote_volume_24h=quote_volume_24h,
        price_change_24h_pct=_extract_price_change_24h_pct(ticker_item),
        current_volume=current_volume,
        avg_volume_20=avg_volume_20,
        volume_burst_ratio=current_volume / max(avg_volume_20, 1e-9),
        current_quote_turnover=current_quote_turnover,
        avg_quote_turnover_20=avg_quote_turnover_20,
        candle_body_pct=(candle_body / price) * 100.0,
        candle_range_pct=(candle_range / price) * 100.0,
        atr_20_pct=(atr / price) * 100.0,
        body_to_atr=body_to_atr,
        range_to_atr=range_to_atr,
        ret_1bar_pct=pct_change(price, closes[current_index - 1]),
        ret_5bar_pct=pct_change(price, closes[current_index - 5]),
        ret_15bar_pct=pct_change(price, closes[current_index - 15]) if current_index >= 15 else 0.0,
        ret_60bar_pct=pct_change(price, closes[current_index - 60]) if current_index >= 60 else 0.0,
        candle_direction=candle_direction,
    )


def evaluate_event_triggers(
    context: MarketEventContext,
    *,
    config: EventTriggerConfig,
) -> tuple[str, ...]:
    atr_multiplier, body_multiplier, volume_multiplier = resolve_event_trigger_thresholds(
        context.bar_interval,
        config=config,
    )
    trigger_types: list[str] = []
    if context.range_to_atr >= atr_multiplier:
        trigger_types.append("range_expansion")
    if context.body_to_atr >= body_multiplier:
        trigger_types.append("body_expansion")
    if context.volume_burst_ratio >= volume_multiplier:
        trigger_types.append("volume_burst")
    if context.bar_interval == "1m" and (
        "range_expansion" not in trigger_types or "volume_burst" not in trigger_types
    ):
        return ()
    return tuple(trigger_types)


def event_trigger_ratios(
    context: MarketEventContext,
    *,
    config: EventTriggerConfig,
) -> dict[str, float]:
    atr_multiplier, body_multiplier, volume_multiplier = resolve_event_trigger_thresholds(
        context.bar_interval,
        config=config,
    )
    atr_base = max(atr_multiplier, 1e-9)
    body_base = max(body_multiplier, 1e-9)
    volume_base = max(volume_multiplier, 1e-9)
    return {
        "range_expansion": context.range_to_atr / atr_base,
        "body_expansion": context.body_to_atr / body_base,
        "volume_burst": context.volume_burst_ratio / volume_base,
    }


def score_event_triggers(
    context: MarketEventContext,
    trigger_types: Sequence[str],
    *,
    config: EventTriggerConfig,
) -> float:
    if not trigger_types:
        return 0.0
    atr_multiplier, body_multiplier, volume_multiplier = resolve_event_trigger_thresholds(
        context.bar_interval,
        config=config,
    )
    score = 0.0
    if "range_expansion" in trigger_types:
        score += max(0.0, (context.range_to_atr - atr_multiplier) * 12.0) + 18.0
    if "body_expansion" in trigger_types:
        score += max(0.0, (context.body_to_atr - body_multiplier) * 10.0) + 14.0
    if "volume_burst" in trigger_types:
        score += max(0.0, (context.volume_burst_ratio - volume_multiplier) * 8.0) + 12.0
    return score


def resolve_event_trigger_thresholds(
    bar_interval: str,
    *,
    config: EventTriggerConfig,
) -> tuple[float, float, float]:
    atr_multiplier = max(0.1, float(config.atr_multiplier or 0.1))
    volume_multiplier = max(0.1, float(config.volume_multiplier or 0.1))
    if str(bar_interval) == "1m":
        atr_multiplier = max(0.1, float(config.atr_multiplier_1m or atr_multiplier))
        volume_multiplier = max(0.1, float(config.volume_multiplier_1m or volume_multiplier))
    elif str(bar_interval) == "5m":
        atr_multiplier = max(0.1, float(config.atr_multiplier_5m or atr_multiplier))
        volume_multiplier = max(0.1, float(config.volume_multiplier_5m or volume_multiplier))
    return atr_multiplier, atr_multiplier, volume_multiplier


def build_triggered_market_event(
    context: MarketEventContext,
    *,
    config: EventTriggerConfig,
) -> MarketEventSample | None:
    trigger_types = evaluate_event_triggers(context, config=config)
    if not trigger_types:
        return None
    return context.to_event_sample(
        sample_type="trigger",
        trigger_types=trigger_types,
        trigger_score=score_event_triggers(context, trigger_types, config=config),
    )


def build_confirmed_reversal_market_event(
    ticker_item: dict[str, Any],
    candles_1m: list[list[Any]],
    *,
    config: EventTriggerConfig,
) -> MarketEventSample | None:
    if len(candles_1m) < 4:
        return None
    trigger_context = build_market_event_context(
        ticker_item,
        candles_1m,
        config=config,
        bar_interval="1m",
        anchor_offset=2,
    )
    confirmation_context = build_market_event_context(
        ticker_item,
        candles_1m,
        config=config,
        bar_interval="1m",
    )
    if trigger_context is None or confirmation_context is None:
        return None

    trigger_types = evaluate_event_triggers(trigger_context, config=config)
    if not trigger_types:
        return None
    has_expansion_shape = "body_expansion" in trigger_types or "range_expansion" in trigger_types
    if "volume_burst" not in trigger_types or not has_expansion_shape:
        return None

    confirmation_row = candles_1m[-2]
    confirmation_open_price = safe_float(confirmation_row[1])
    confirmation_close_price = safe_float(confirmation_row[4])
    if confirmation_open_price <= 0 or confirmation_close_price <= 0:
        return None

    confirmation_direction = _candle_direction_from_prices(
        open_price=confirmation_open_price,
        close_price=confirmation_close_price,
    )
    if not is_reversal_confirmation_passed(
        trigger_direction=trigger_context.candle_direction,
        confirmation_direction=confirmation_direction,
    ):
        return None

    enriched_trigger_types = tuple(
        list(trigger_types)
        + ([] if "right_side_confirmation_1m" in trigger_types else ["right_side_confirmation_1m"])
    )
    sample = trigger_context.to_event_sample(
        sample_type="trigger",
        trigger_types=enriched_trigger_types,
        trigger_score=score_event_triggers(trigger_context, trigger_types, config=config),
    )
    sample.anchor_open_time_ms = confirmation_context.anchor_open_time_ms
    sample.anchor_close_time_ms = confirmation_context.anchor_close_time_ms
    sample.price = confirmation_context.price
    sample.quote_volume_24h = confirmation_context.quote_volume_24h
    sample.price_change_24h_pct = confirmation_context.price_change_24h_pct
    sample.trigger_anchor_open_time_ms = trigger_context.anchor_open_time_ms
    sample.trigger_anchor_close_time_ms = trigger_context.anchor_close_time_ms
    sample.trigger_candle_direction = trigger_context.candle_direction
    sample.confirmation_candle_open_price = confirmation_open_price
    sample.confirmation_candle_close_price = confirmation_close_price
    sample.confirmation_candle_direction = confirmation_direction
    sample.reversal_confirmation_passed = True
    sample.reversal_confirmation_bar_interval = "1m"
    return sample


def build_ranked_market_event(
    context: MarketEventContext,
    *,
    config: EventTriggerConfig,
    min_trigger_ratio: float = 0.75,
    relative_floor: float = 0.85,
) -> MarketEventSample | None:
    if evaluate_event_triggers(context, config=config):
        return None
    ratios = event_trigger_ratios(context, config=config)
    best_ratio = max(ratios.values(), default=0.0)
    threshold = max(0.0, float(min_trigger_ratio or 0.0))
    if best_ratio < threshold:
        return None
    floor = max(threshold, best_ratio * max(0.0, min(1.0, float(relative_floor or 0.0))))
    trigger_types = tuple(sorted(name for name, value in ratios.items() if value >= floor))
    if not trigger_types:
        return None
    trigger_score = (sum(ratios[name] for name in trigger_types) * 12.0) + (best_ratio * 8.0)
    return context.to_event_sample(
        sample_type="ranked",
        trigger_types=trigger_types,
        trigger_score=trigger_score,
    )


def build_baseline_market_event(context: MarketEventContext) -> MarketEventSample:
    return context.to_event_sample(
        sample_type="baseline",
        trigger_types=("baseline_control",),
        trigger_score=0.0,
    )


def compute_macro_context(candles_1m: list[list[Any]]) -> dict[str, float] | None:
    if len(candles_1m) < 62:
        return None
    current_index = len(candles_1m) - 2
    closes = [safe_float(row[4]) for row in candles_1m]
    price = closes[current_index]
    if price <= 0:
        return None
    return {
        "ret_5m_pct": pct_change(price, closes[current_index - 5]),
        "ret_60m_pct": pct_change(price, closes[current_index - 60]),
    }


def _sum_depth_notional(levels: Any, *, max_levels: int = 5) -> float | None:
    if not isinstance(levels, list):
        return None
    total = 0.0
    count = 0
    for level in levels:
        if count >= max_levels:
            break
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price = safe_float(level[0])
        quantity = safe_float(level[1])
        if price <= 0 or quantity <= 0:
            continue
        total += price * quantity
        count += 1
    return total if total > 0 else None


def _estimate_side_slippage_bps(levels: Any, *, notional_usdt: float, reference_price: float) -> float | None:
    if not isinstance(levels, list) or notional_usdt <= 0 or reference_price <= 0:
        return None
    remaining_quote = float(notional_usdt)
    filled_quote = 0.0
    filled_base = 0.0
    for level in levels:
        if remaining_quote <= 0:
            break
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price = safe_float(level[0])
        quantity = safe_float(level[1])
        if price <= 0 or quantity <= 0:
            continue
        level_quote = price * quantity
        take_quote = min(remaining_quote, level_quote)
        filled_quote += take_quote
        filled_base += take_quote / price
        remaining_quote -= take_quote
    if filled_quote <= 0 or filled_base <= 0 or remaining_quote > 1e-6:
        return None
    vwap = filled_quote / filled_base
    return abs(vwap - reference_price) / reference_price * 10_000.0


def compute_orderbook_metrics(
    depth_payload: Any,
    *,
    reference_price: float,
    slippage_notional_usdt: float = 0.0,
) -> OrderBookMetrics:
    if not isinstance(depth_payload, dict):
        return OrderBookMetrics(None, None, None, None, None, None)
    bids = depth_payload.get("bids")
    asks = depth_payload.get("asks")
    best_bid = safe_float(bids[0][0]) if isinstance(bids, list) and bids and isinstance(bids[0], (list, tuple)) else 0.0
    best_ask = safe_float(asks[0][0]) if isinstance(asks, list) and asks and isinstance(asks[0], (list, tuple)) else 0.0
    spread_bps = None
    if best_bid > 0 and best_ask > 0:
        mid = (best_bid + best_ask) / 2.0
        spread_bps = ((best_ask - best_bid) / mid) * 10_000.0 if mid > 0 else None
    bid_depth = _sum_depth_notional(bids)
    ask_depth = _sum_depth_notional(asks)
    depth_imbalance = None
    if bid_depth is not None and ask_depth is not None:
        total_depth = bid_depth + ask_depth
        depth_imbalance = ((bid_depth - ask_depth) / total_depth) if total_depth > 0 else None
    buy_slippage = _estimate_side_slippage_bps(asks, notional_usdt=slippage_notional_usdt, reference_price=reference_price)
    sell_slippage = _estimate_side_slippage_bps(bids, notional_usdt=slippage_notional_usdt, reference_price=reference_price)
    slippages = [value for value in (buy_slippage, sell_slippage) if value is not None]
    estimated_slippage_bps = max(slippages) if slippages else None
    estimated_slippage_for_order_usdt = (
        slippage_notional_usdt * (estimated_slippage_bps / 10_000.0)
        if estimated_slippage_bps is not None and slippage_notional_usdt > 0
        else None
    )
    return OrderBookMetrics(
        spread_bps=spread_bps,
        bid_depth_notional_5=bid_depth,
        ask_depth_notional_5=ask_depth,
        depth_imbalance=depth_imbalance,
        estimated_slippage_bps=estimated_slippage_bps,
        estimated_slippage_for_order_usdt=estimated_slippage_for_order_usdt,
    )


def _extract_open_interest_value(row: Any) -> float:
    if not isinstance(row, dict):
        return 0.0
    for key in ("openInterest", "sumOpenInterest", "sumOpenInterestValue"):
        value = safe_float(row.get(key))
        if value > 0:
            return value
    return 0.0


def compute_open_interest_context(current_open_interest: float, history_rows: list[dict[str, Any]]) -> dict[str, float | None]:
    current_value = safe_float(current_open_interest)
    history_values = [value for value in (_extract_open_interest_value(row) for row in history_rows) if value > 0]
    if current_value <= 0 and history_values:
        current_value = history_values[-1]
    base_5m = history_values[-2] if len(history_values) >= 2 else None
    base_15m = history_values[-4] if len(history_values) >= 4 else None
    return {
        "open_interest": current_value if current_value > 0 else None,
        "oi_change_5m_pct": pct_change(current_value, base_5m) if current_value > 0 and base_5m else None,
        "oi_change_15m_pct": pct_change(current_value, base_15m) if current_value > 0 and base_15m else None,
    }


def compute_future_path_label(
    *,
    entry_price: float,
    candles_1m: list[list[Any]],
    horizon_sec: int,
    round_trip_fee_bps: float = 0.0,
) -> FuturePathLabel | None:
    if entry_price <= 0 or not candles_1m:
        return None
    highs = [safe_float(row[2]) for row in candles_1m]
    lows = [safe_float(row[3]) for row in candles_1m]
    closes = [safe_float(row[4]) for row in candles_1m]
    close_price = closes[-1]
    if close_price <= 0:
        return None
    close_return_pct = pct_change(close_price, entry_price)
    max_runup_price = max(highs)
    max_drawdown_price = min(lows)
    fee_pct = max(0.0, float(round_trip_fee_bps or 0.0)) / 100.0
    return FuturePathLabel(
        horizon_sec=int(horizon_sec),
        candle_count=len(candles_1m),
        close_price=close_price,
        close_return_pct=close_return_pct,
        after_fee_return_pct=close_return_pct - fee_pct,
        max_runup_pct=pct_change(max_runup_price, entry_price),
        max_drawdown_pct=pct_change(max_drawdown_price, entry_price),
    )
