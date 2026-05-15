#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable

import aiohttp

from phoenix_factor_library import build_factor_vector
from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import load_proxy_settings, resolve_environment
from phoenix.signal_lab_events import (
    EventTriggerConfig,
    FuturePathLabel,
    MarketEventContext,
    MarketEventSample,
    build_baseline_market_event,
    build_confirmed_reversal_market_event,
    build_market_event_context,
    build_ranked_market_event,
    build_triggered_market_event,
    compute_future_path_label,
    compute_macro_context,
    compute_open_interest_context,
    compute_orderbook_metrics,
)
from phoenix_testnet_round_runner import (
    build_timeframe_snapshot,
    Candidate,
    discover_universe,
    is_tradeable_symbol,
    safe_float,
    score_candidate,
    setup_execution_floor,
    setup_priority,
)

# Binance USD-M futures official streams used by the collector:
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
MARK_PRICE_STREAM_NAME = "!markPrice@arr@1s"
LIQUIDATION_STREAM_NAME = "!forceOrder@arr"
DEFAULT_OBSERVE_HORIZONS_SEC = (20, 45, 90, 180)
FRESH_PRICE_MAX_AGE_MS = 5_000
DEFAULT_MARK_PRICE_WS_TIMEOUT_SEC = 35.0
DEFAULT_LIQUIDATION_WS_TIMEOUT_SEC = 90.0
DEFAULT_MARK_PRICE_WS_BACKOFF_SEC = 3.0
DEFAULT_DERIVATIVES_WS_BACKOFF_SEC = 3.0
DEFAULT_FINALIZER_INTERVAL_SEC = 1.0
DEFAULT_CYCLE_TIMEOUT_SEC = 75.0
DEFAULT_KLINE_LIMIT = 70
DEFAULT_UNIVERSE_CACHE_SEC = 45.0
DEFAULT_KLINE_CACHE_TTL_1M_SEC = 10.0
DEFAULT_KLINE_CACHE_TTL_3M_SEC = 30.0
DEFAULT_KLINE_CACHE_TTL_5M_SEC = 60.0
DEFAULT_KLINE_CACHE_TTL_15M_SEC = 180.0
DEFAULT_FEATURE_MIN_QUOTE_VOLUME = 150_000_000.0
DEFAULT_FEATURE_MIN_AVG_QUOTE_TURNOVER = 200_000.0
DEFAULT_FEATURE_MIN_CURRENT_QUOTE_TURNOVER = 5_000.0
DEFAULT_COLLECT_HORIZONS_SEC = (60, 180, 300, 900, 1800, 3600)
DEFAULT_EVENT_QUEUE_SIZE = 512
DEFAULT_EVENT_WORKER_COUNT = 4
DEFAULT_EVENT_SYMBOL_COOLDOWN_SEC = 60
DEFAULT_EVENT_DEPTH_LIMIT = 5
DEFAULT_EVENT_PUBLIC_CACHE_TTL_SEC = 8.0
DEFAULT_EVENT_DEPTH_CACHE_TTL_SEC = 5.0
DEFAULT_EVENT_OI_HIST_CACHE_TTL_SEC = 30.0
DEFAULT_LIQUIDATION_WINDOW_MS = 15 * 60 * 1000
DEFAULT_EVENT_WORKER_MAX_RETRIES = 4
DEFAULT_EVENT_RATE_LIMIT_BACKOFF_SEC = 4.0
DEFAULT_COLLECT_DURATION_SEC = 0
LIGHTWEIGHT_ENRICHMENT_SAMPLE_TYPES = frozenset({"baseline"})
BTC_REVERSAL_REGIME_BLOCK_PCT = 0.35

SCOUT_SNAPSHOT_KEYS = (
    "funding_rate",
    "next_funding_time_ms",
    "mark_index_basis_pct",
    "taker_buy_ratio_1m",
    "taker_buy_ratio_5m",
    "aggressive_flow_delta",
    "liquidation_long_usd",
    "liquidation_short_usd",
    "liquidation_event_count",
    "spread_bps",
    "depth_imbalance",
    "estimated_slippage_bps",
    "estimated_slippage_for_order_usdt",
    "volume_5m_ratio_short",
    "volume_5m_ratio_medium",
    "volume_5m_ratio_long",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def now_ms() -> int:
    return int(time.time() * 1000)


def trading_session_label(timestamp_ms: int) -> str:
    hour = datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc).hour
    if 0 <= hour < 8:
        return "Asia"
    if 8 <= hour < 14:
        return "Europe"
    if 14 <= hour < 22:
        return "US"
    return "offhours"


def record_trading_session(record: dict[str, Any]) -> str:
    trading_session = str(record.get("trading_session") or "").strip()
    if trading_session:
        return trading_session
    sample_payload = record.get("sample")
    if isinstance(sample_payload, dict):
        sample_session = str(sample_payload.get("trading_session") or "").strip()
        if sample_session:
            return sample_session
        anchor_close_time_ms = sample_payload.get("anchor_close_time_ms")
        if anchor_close_time_ms not in (None, ""):
            return trading_session_label(int(anchor_close_time_ms))
    observed_at_ms = record.get("observed_at_ms")
    if observed_at_ms not in (None, ""):
        return trading_session_label(int(observed_at_ms))
    return "unknown"


def futures_ws_stream_urls(futures: BinanceFuturesClient, stream_name: str) -> list[str]:
    primary_base = futures.environment.futures_ws_base.rstrip("/")
    urls = [f"{primary_base}/ws/{stream_name}"]
    if primary_base == "wss://fstream.binance.com":
        urls.append(f"wss://fstream.binancefuture.com/ws/{stream_name}")
    unique_urls: list[str] = []
    for url in urls:
        if url not in unique_urls:
            unique_urls.append(url)
    return unique_urls


def round4(value: float) -> float:
    return round(float(value), 4)


def pct_change(current: float, past: float) -> float:
    if current <= 0 or past <= 0:
        return 0.0
    return ((current / past) - 1.0) * 100.0


def side_aware_return_pct(*, side: str, entry_price: float, current_price: float) -> float:
    if side == "BUY":
        return pct_change(current_price, entry_price)
    return pct_change(entry_price, current_price)


def parse_horizons(raw_value: str) -> list[int]:
    horizons: list[int] = []
    for part in str(raw_value or "").split(","):
        token = part.strip()
        if not token:
            continue
        value = int(token)
        if value <= 0:
            raise ValueError(f"Observation horizon must be positive, got {value}.")
        horizons.append(value)
    if not horizons:
        raise ValueError("At least one positive observation horizon is required.")
    return sorted(set(horizons))


def parse_setup_filter(raw_value: str | None) -> set[str] | None:
    if raw_value is None:
        return None
    setups = {token.strip() for token in str(raw_value).split(",") if token.strip()}
    return setups or None


def parse_allowed_event_patterns(raw_value: str | None) -> set[tuple[str, str, str]] | None:
    if raw_value is None:
        return None
    patterns: set[tuple[str, str, str]] = set()
    for token in str(raw_value).split(","):
        raw_token = token.strip()
        if not raw_token:
            continue
        parts = [part.strip() for part in raw_token.split(":")]
        if len(parts) != 3:
            raise ValueError(
                "Event pattern must look like 'sample_type:bar_interval:trigger_signature', "
                f"got {raw_token!r}."
            )
        sample_type, bar_interval, trigger_signature = parts
        patterns.add(
            (
                str(sample_type or "*").lower(),
                str(bar_interval or "*").lower(),
                str(trigger_signature or "*").lower(),
            )
        )
    return patterns or None


def setup_allowed(setup: str, *, include_setups: set[str] | None) -> bool:
    return include_setups is None or setup in include_setups


def sample_event_signature(sample: MarketEventSample) -> str:
    if sample.sample_type == "baseline":
        return "baseline_control"
    if not sample.trigger_types:
        return "unclassified"
    return "+".join(sorted(str(token) for token in sample.trigger_types if str(token)))


def sample_allowed(
    sample: MarketEventSample,
    *,
    allowed_patterns: set[tuple[str, str, str]] | None,
) -> bool:
    if allowed_patterns is None:
        return True
    sample_type = str(sample.sample_type or "").lower()
    bar_interval = str(sample.bar_interval or "").lower()
    trigger_signature = sample_event_signature(sample).lower()
    for allowed_sample_type, allowed_bar_interval, allowed_signature in allowed_patterns:
        if allowed_sample_type not in {"*", sample_type}:
            continue
        if allowed_bar_interval not in {"*", bar_interval}:
            continue
        if allowed_signature not in {"*", trigger_signature}:
            continue
        return True
    return False


@dataclass(slots=True)
class MarketFeatures:
    symbol: str
    last_close: float
    quote_volume_24h: float
    price_change_24h_pct: float
    avg_quote_turnover_1m: float
    current_quote_turnover_1m: float
    volume_ratio: float
    range_position: float
    trend_gap_pct: float
    higher_tf_trend_gap_pct: float
    pullback_pct: float
    bounce_pct: float
    ret_1m_pct: float
    ret_3m_pct: float
    ret_5m_pct: float
    ret_15m_pct: float
    ret_30m_pct: float
    ret_60m_pct: float
    tf5_ret_3bar_pct: float
    tf5_ret_6bar_pct: float
    tf5_volume_ratio: float
    tf5_range_position: float
    tf5_trend_gap_pct: float
    tf5_pullback_pct: float
    tf15_ret_3bar_pct: float
    tf15_ret_6bar_pct: float
    tf15_volume_ratio: float
    tf15_range_position: float
    tf15_trend_gap_pct: float
    tf15_pullback_pct: float


@dataclass(frozen=True, slots=True)
class DiagnosticRuleSpec:
    metric: str
    min_value: float | None = None
    max_value: float | None = None


@dataclass(slots=True)
class DiagnosticRuleResult:
    metric: str
    actual_value: float
    min_value: float | None
    max_value: float | None
    passed: bool
    gap: float
    clue: str


@dataclass(slots=True)
class SetupDiagnosticResult:
    setup: str
    symbol: str
    price: float
    live_score: float
    passed_rules: int
    total_rules: int
    normalized_gap: float
    quote_volume_24h: float
    avg_quote_turnover_1m: float
    rule_results: list[DiagnosticRuleResult]


BROAD_DIAGNOSTIC_RULES: dict[str, tuple[DiagnosticRuleSpec, ...]] = {
    "broad_hot_momentum_long": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=8.0, max_value=60.0),
        DiagnosticRuleSpec("ret_15m_pct", min_value=0.8),
        DiagnosticRuleSpec("ret_30m_pct", min_value=1.2),
        DiagnosticRuleSpec("ret_1m_pct", min_value=-0.10, max_value=0.45),
        DiagnosticRuleSpec("ret_5m_pct", min_value=0.20),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", min_value=0.35),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.45, max_value=0.95),
        DiagnosticRuleSpec("tf5_pullback_pct", max_value=0.0),
        DiagnosticRuleSpec("volume_ratio", min_value=0.35, max_value=4.0),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", min_value=0.20),
    ),
    "broad_hot_pullback_long": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=8.0, max_value=60.0),
        DiagnosticRuleSpec("ret_15m_pct", min_value=0.8),
        DiagnosticRuleSpec("ret_30m_pct", min_value=1.2),
        DiagnosticRuleSpec("ret_1m_pct", min_value=-0.45, max_value=0.35),
        DiagnosticRuleSpec("ret_5m_pct", min_value=-0.35, max_value=0.35),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", min_value=0.50),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.60, max_value=1.02),
        DiagnosticRuleSpec("tf5_pullback_pct", max_value=0.12),
        DiagnosticRuleSpec("volume_ratio", min_value=0.35, max_value=4.0),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", min_value=0.35),
    ),
    "broad_warm_momentum_long": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=3.0, max_value=35.0),
        DiagnosticRuleSpec("ret_15m_pct", min_value=0.45),
        DiagnosticRuleSpec("ret_30m_pct", min_value=0.65),
        DiagnosticRuleSpec("ret_1m_pct", min_value=-0.12, max_value=0.35),
        DiagnosticRuleSpec("ret_5m_pct", min_value=-0.15),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", min_value=0.25),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.45, max_value=0.98),
        DiagnosticRuleSpec("tf5_pullback_pct", max_value=0.05),
        DiagnosticRuleSpec("volume_ratio", min_value=0.18, max_value=4.5),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", min_value=0.20),
    ),
    "broad_momentum_long": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=-1.0, max_value=12.0),
        DiagnosticRuleSpec("ret_15m_pct", min_value=0.08),
        DiagnosticRuleSpec("ret_30m_pct", min_value=0.10),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", min_value=-0.08),
        DiagnosticRuleSpec("volume_ratio", min_value=0.45),
        DiagnosticRuleSpec("tf5_volume_ratio", min_value=0.20),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", min_value=0.0),
    ),
    "broad_mean_reversion_long": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=-2.5, max_value=2.5),
        DiagnosticRuleSpec("ret_15m_pct", min_value=-0.45, max_value=0.12),
        DiagnosticRuleSpec("tf5_ret_3bar_pct", min_value=-0.30, max_value=0.08),
        DiagnosticRuleSpec("range_position", min_value=0.0, max_value=0.26),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.0, max_value=0.30),
        DiagnosticRuleSpec("volume_ratio", min_value=0.25, max_value=2.4),
    ),
    "broad_momentum_short": (
        DiagnosticRuleSpec("price_change_24h_pct", max_value=0.15),
        DiagnosticRuleSpec("ret_15m_pct", max_value=-0.08),
        DiagnosticRuleSpec("ret_30m_pct", max_value=-0.10),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", max_value=0.08),
        DiagnosticRuleSpec("volume_ratio", min_value=0.45),
        DiagnosticRuleSpec("tf5_volume_ratio", min_value=0.20),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", max_value=0.05),
    ),
    "broad_trend_momentum_short": (
        DiagnosticRuleSpec("price_change_24h_pct", max_value=-3.0),
        DiagnosticRuleSpec("ret_15m_pct", max_value=-0.35),
        DiagnosticRuleSpec("ret_30m_pct", max_value=-1.8),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", max_value=-0.15),
        DiagnosticRuleSpec("volume_ratio", min_value=0.45, max_value=5.5),
        DiagnosticRuleSpec("tf5_volume_ratio", min_value=0.45, max_value=6.0),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", max_value=-0.15),
        DiagnosticRuleSpec("avg_quote_turnover_1m", max_value=1_000_000.0),
    ),
    "broad_pullback_short": (
        DiagnosticRuleSpec("price_change_24h_pct", max_value=0.35),
        DiagnosticRuleSpec("ret_30m_pct", max_value=-0.05),
        DiagnosticRuleSpec("ret_5m_pct", min_value=-0.08, max_value=0.35),
        DiagnosticRuleSpec("bounce_pct", min_value=0.02),
        DiagnosticRuleSpec("tf15_trend_gap_pct", max_value=0.05),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", max_value=0.08),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.22, max_value=0.88),
        DiagnosticRuleSpec("volume_ratio", min_value=0.35),
    ),
    "broad_mean_reversion_short": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=-2.5, max_value=2.5),
        DiagnosticRuleSpec("ret_15m_pct", min_value=-0.12, max_value=0.45),
        DiagnosticRuleSpec("tf5_ret_3bar_pct", min_value=-0.08, max_value=0.30),
        DiagnosticRuleSpec("range_position", min_value=0.74, max_value=1.10),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.70, max_value=1.10),
        DiagnosticRuleSpec("volume_ratio", min_value=0.25, max_value=2.4),
    ),
    "broad_hot_reversal_short": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=18.0, max_value=90.0),
        DiagnosticRuleSpec("ret_15m_pct", min_value=0.8),
        DiagnosticRuleSpec("ret_30m_pct", min_value=1.0),
        DiagnosticRuleSpec("ret_1m_pct", max_value=-0.12),
        DiagnosticRuleSpec("ret_5m_pct", min_value=-2.5, max_value=4.5),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", min_value=2.0),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.90, max_value=1.35),
        DiagnosticRuleSpec("tf5_pullback_pct", min_value=-0.60),
        DiagnosticRuleSpec("volume_ratio", min_value=0.45, max_value=4.5),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", min_value=0.45),
    ),
    "broad_hot_reversal_swing_short": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=10.0, max_value=90.0),
        DiagnosticRuleSpec("ret_15m_pct", min_value=0.6),
        DiagnosticRuleSpec("ret_30m_pct", min_value=0.8),
        DiagnosticRuleSpec("ret_1m_pct", max_value=-0.08),
        DiagnosticRuleSpec("ret_5m_pct", min_value=-2.8, max_value=5.0),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", min_value=1.4),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.85, max_value=1.35),
        DiagnosticRuleSpec("tf5_pullback_pct", min_value=-0.75),
        DiagnosticRuleSpec("volume_ratio", min_value=0.35, max_value=6.0),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", min_value=0.35),
    ),
    "broad_hot_flush_short": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=12.0, max_value=80.0),
        DiagnosticRuleSpec("ret_15m_pct", min_value=0.4),
        DiagnosticRuleSpec("ret_30m_pct", min_value=0.8),
        DiagnosticRuleSpec("ret_1m_pct", max_value=-0.05),
        DiagnosticRuleSpec("ret_5m_pct", min_value=-3.0, max_value=0.4),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", min_value=1.0),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.25, max_value=0.95),
        DiagnosticRuleSpec("tf5_pullback_pct", max_value=-0.80),
        DiagnosticRuleSpec("volume_ratio", min_value=0.40, max_value=3.0),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", min_value=0.35),
    ),
    "broad_ultra_hot_reversal_short": (
        DiagnosticRuleSpec("price_change_24h_pct", min_value=25.0, max_value=120.0),
        DiagnosticRuleSpec("ret_15m_pct", min_value=3.0),
        DiagnosticRuleSpec("ret_30m_pct", min_value=6.0),
        DiagnosticRuleSpec("ret_1m_pct", max_value=-0.25),
        DiagnosticRuleSpec("ret_5m_pct", min_value=-2.0, max_value=5.0),
        DiagnosticRuleSpec("tf5_ret_6bar_pct", min_value=4.0),
        DiagnosticRuleSpec("tf5_range_position", min_value=0.95, max_value=1.25),
        DiagnosticRuleSpec("tf5_pullback_pct", min_value=-0.25),
        DiagnosticRuleSpec("volume_ratio", min_value=0.35, max_value=4.0),
        DiagnosticRuleSpec("higher_tf_trend_gap_pct", min_value=0.80),
    ),
}


def diagnostic_supported_setups() -> list[str]:
    return sorted(BROAD_DIAGNOSTIC_RULES)


def rule_gap_scale(rule: DiagnosticRuleSpec) -> float:
    if rule.min_value is not None and rule.max_value is not None:
        return max(abs(rule.max_value - rule.min_value), 1e-9)
    anchor = rule.min_value if rule.min_value is not None else rule.max_value
    return max(abs(safe_float(anchor)), 1.0)


def evaluate_diagnostic_rule(features: MarketFeatures, rule: DiagnosticRuleSpec) -> DiagnosticRuleResult:
    actual_value = safe_float(getattr(features, rule.metric))
    if rule.min_value is not None and actual_value < rule.min_value:
        gap = rule.min_value - actual_value
        return DiagnosticRuleResult(
            metric=rule.metric,
            actual_value=actual_value,
            min_value=rule.min_value,
            max_value=rule.max_value,
            passed=False,
            gap=gap,
            clue=f"{rule.metric}={actual_value:.4f} < min {rule.min_value:.4f} (short by {gap:.4f})",
        )
    if rule.max_value is not None and actual_value > rule.max_value:
        gap = actual_value - rule.max_value
        return DiagnosticRuleResult(
            metric=rule.metric,
            actual_value=actual_value,
            min_value=rule.min_value,
            max_value=rule.max_value,
            passed=False,
            gap=gap,
            clue=f"{rule.metric}={actual_value:.4f} > max {rule.max_value:.4f} (over by {gap:.4f})",
        )
    if rule.min_value is not None and rule.max_value is not None:
        clue = f"{rule.metric}={actual_value:.4f} within [{rule.min_value:.4f}, {rule.max_value:.4f}]"
    elif rule.min_value is not None:
        clue = f"{rule.metric}={actual_value:.4f} >= {rule.min_value:.4f}"
    elif rule.max_value is not None:
        clue = f"{rule.metric}={actual_value:.4f} <= {rule.max_value:.4f}"
    else:
        clue = f"{rule.metric}={actual_value:.4f}"
    return DiagnosticRuleResult(
        metric=rule.metric,
        actual_value=actual_value,
        min_value=rule.min_value,
        max_value=rule.max_value,
        passed=True,
        gap=0.0,
        clue=clue,
    )


def evaluate_setup_diagnostic(features: MarketFeatures, *, setup: str) -> SetupDiagnosticResult:
    rules = BROAD_DIAGNOSTIC_RULES[setup]
    rule_results = [evaluate_diagnostic_rule(features, rule) for rule in rules]
    passed_rules = sum(1 for item in rule_results if item.passed)
    live_candidate = score_broad_candidate(features, allow_short=True, include_setups={setup})
    normalized_gap = sum(
        result.gap / rule_gap_scale(rule)
        for rule, result in zip(rules, rule_results)
        if not result.passed
    )
    return SetupDiagnosticResult(
        setup=setup,
        symbol=features.symbol,
        price=features.last_close,
        live_score=round4(live_candidate.score) if live_candidate is not None else 0.0,
        passed_rules=passed_rules,
        total_rules=len(rule_results),
        normalized_gap=round4(normalized_gap),
        quote_volume_24h=features.quote_volume_24h,
        avg_quote_turnover_1m=features.avg_quote_turnover_1m,
        rule_results=rule_results,
    )


def bounded_score(value: float, *, center: float, half_width: float, max_score: float) -> float:
    if half_width <= 0:
        return 0.0
    distance = abs(value - center)
    if distance >= half_width:
        return 0.0
    return max_score * (1.0 - distance / half_width)


def mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def compute_market_features(
    item: dict[str, Any],
    candles_1m: list[list[Any]],
    candles_5m: list[list[Any]],
    candles_15m: list[list[Any]],
    *,
    feature_min_quote_volume: float = DEFAULT_FEATURE_MIN_QUOTE_VOLUME,
) -> MarketFeatures | None:
    if len(candles_1m) < 65:
        return None
    frame5 = build_timeframe_snapshot(candles_5m)
    frame15 = build_timeframe_snapshot(candles_15m)
    if frame5 is None or frame15 is None:
        return None

    closes = [safe_float(row[4]) for row in candles_1m]
    highs = [safe_float(row[2]) for row in candles_1m]
    lows = [safe_float(row[3]) for row in candles_1m]
    volumes = [safe_float(row[5]) for row in candles_1m]
    if min(closes[-20:]) <= 0:
        return None

    current_index = len(candles_1m) - 2
    if current_index < 21:
        return None

    last_close = closes[current_index]
    recent_high = max(highs[current_index - 9 : current_index])
    recent_low = min(lows[current_index - 9 : current_index])
    current_quote_turnover = last_close * volumes[current_index]
    avg_quote_turnover = mean(
        [closes[index] * volumes[index] for index in range(current_index - 9, current_index + 1)]
    )
    volume_ratio = volumes[current_index] / max(mean(volumes[current_index - 20 : current_index]), 1e-9)
    ret_1m_pct = pct_change(last_close, closes[current_index - 1])
    ret_3m_pct = pct_change(last_close, closes[current_index - 3])
    ret_5m_pct = pct_change(last_close, closes[current_index - 5])
    ret_15m_pct = pct_change(last_close, closes[current_index - 15])
    ret_30m_pct = pct_change(last_close, closes[current_index - 30]) if current_index >= 30 else 0.0
    ret_60m_pct = pct_change(last_close, closes[current_index - 60]) if current_index >= 60 else 0.0
    range_span = max(recent_high - recent_low, 1e-9)
    range_position = (last_close - recent_low) / range_span
    trend_gap_pct = frame5.trend_gap_pct
    higher_tf_trend_gap_pct = frame15.higher_trend_gap_pct
    pullback_pct = pct_change(last_close, recent_high)
    bounce_pct = pct_change(last_close, recent_low)
    quote_volume_24h = safe_float(item["quote_volume_24h"])
    price_change_24h_pct = safe_float(item["price_change_24h_pct"])
    if last_close < 0.01 or quote_volume_24h < max(0.0, feature_min_quote_volume):
        return None
    if (
        avg_quote_turnover < DEFAULT_FEATURE_MIN_AVG_QUOTE_TURNOVER
        or current_quote_turnover < DEFAULT_FEATURE_MIN_CURRENT_QUOTE_TURNOVER
    ):
        return None
    return MarketFeatures(
        symbol=str(item["symbol"]),
        last_close=last_close,
        quote_volume_24h=quote_volume_24h,
        price_change_24h_pct=price_change_24h_pct,
        avg_quote_turnover_1m=avg_quote_turnover,
        current_quote_turnover_1m=current_quote_turnover,
        volume_ratio=volume_ratio,
        range_position=range_position,
        trend_gap_pct=trend_gap_pct,
        higher_tf_trend_gap_pct=higher_tf_trend_gap_pct,
        pullback_pct=pullback_pct,
        bounce_pct=bounce_pct,
        ret_1m_pct=ret_1m_pct,
        ret_3m_pct=ret_3m_pct,
        ret_5m_pct=ret_5m_pct,
        ret_15m_pct=ret_15m_pct,
        ret_30m_pct=ret_30m_pct,
        ret_60m_pct=ret_60m_pct,
        tf5_ret_3bar_pct=frame5.ret_3bar_pct,
        tf5_ret_6bar_pct=frame5.ret_6bar_pct,
        tf5_volume_ratio=frame5.volume_ratio,
        tf5_range_position=frame5.range_position,
        tf5_trend_gap_pct=frame5.trend_gap_pct,
        tf5_pullback_pct=frame5.pullback_pct,
        tf15_ret_3bar_pct=frame15.ret_3bar_pct,
        tf15_ret_6bar_pct=frame15.ret_6bar_pct,
        tf15_volume_ratio=frame15.volume_ratio,
        tf15_range_position=frame15.range_position,
        tf15_trend_gap_pct=frame15.trend_gap_pct,
        tf15_pullback_pct=frame15.pullback_pct,
    )


def candidate_from_features(features: MarketFeatures, *, setup: str, side: str, score: float) -> Candidate:
    return Candidate(
        symbol=features.symbol,
        setup=setup,
        side=side,
        score=score,
        mark_price=features.last_close,
        quote_volume_24h=features.quote_volume_24h,
        avg_quote_turnover_1m=features.avg_quote_turnover_1m,
        current_quote_turnover_1m=features.current_quote_turnover_1m,
        price_change_24h_pct=features.price_change_24h_pct,
        ret_1m_pct=features.ret_1m_pct,
        ret_3m_pct=features.ret_3m_pct,
        ret_5m_pct=features.ret_5m_pct,
        ret_15m_pct=features.ret_15m_pct,
        ret_30m_pct=features.ret_30m_pct,
        ret_60m_pct=features.ret_60m_pct,
        volume_ratio=features.volume_ratio,
        range_position=features.range_position,
        trend_gap_pct=features.trend_gap_pct,
        higher_tf_trend_gap_pct=features.higher_tf_trend_gap_pct,
        pullback_pct=features.pullback_pct,
        bounce_pct=features.bounce_pct,
        tf5_ret_3bar_pct=features.tf5_ret_3bar_pct,
        tf5_ret_6bar_pct=features.tf5_ret_6bar_pct,
        tf5_volume_ratio=features.tf5_volume_ratio,
        tf5_range_position=features.tf5_range_position,
        tf5_trend_gap_pct=features.tf5_trend_gap_pct,
        tf5_pullback_pct=features.tf5_pullback_pct,
        tf15_ret_3bar_pct=features.tf15_ret_3bar_pct,
        tf15_ret_6bar_pct=features.tf15_ret_6bar_pct,
        tf15_volume_ratio=features.tf15_volume_ratio,
        tf15_range_position=features.tf15_range_position,
        tf15_trend_gap_pct=features.tf15_trend_gap_pct,
        tf15_pullback_pct=features.tf15_pullback_pct,
    )


def score_broad_candidate(
    features: MarketFeatures,
    *,
    allow_short: bool,
    include_setups: set[str] | None = None,
) -> Candidate | None:
    options: list[tuple[str, str, float]] = []

    broad_hot_momentum_long = 0.0
    if (
        8.0 <= features.price_change_24h_pct <= 60.0
        and features.ret_15m_pct >= 0.8
        and features.ret_30m_pct >= 1.2
        and -0.10 <= features.ret_1m_pct <= 0.45
        and features.ret_5m_pct >= 0.20
        and features.tf5_ret_6bar_pct >= 0.35
        and 0.45 <= features.tf5_range_position <= 0.95
        and features.tf5_pullback_pct <= 0.0
        and 0.35 <= features.volume_ratio <= 4.0
        and features.higher_tf_trend_gap_pct >= 0.20
    ):
        broad_hot_momentum_long += 18.0
        broad_hot_momentum_long += bounded_score(features.ret_15m_pct, center=1.8, half_width=4.0, max_score=14.0)
        broad_hot_momentum_long += bounded_score(features.ret_30m_pct, center=3.0, half_width=6.5, max_score=14.0)
        broad_hot_momentum_long += bounded_score(features.tf5_ret_3bar_pct, center=0.50, half_width=1.8, max_score=10.0)
        broad_hot_momentum_long += bounded_score(features.volume_ratio, center=1.1, half_width=1.6, max_score=8.0)
        broad_hot_momentum_long += bounded_score(features.tf5_range_position, center=0.82, half_width=0.28, max_score=8.0)
    if setup_allowed("broad_hot_momentum_long", include_setups=include_setups):
        options.append(("broad_hot_momentum_long", "BUY", broad_hot_momentum_long))

    broad_hot_pullback_long = 0.0
    if (
        8.0 <= features.price_change_24h_pct <= 60.0
        and features.ret_15m_pct >= 0.8
        and features.ret_30m_pct >= 1.2
        and -0.45 <= features.ret_1m_pct <= 0.35
        and -0.35 <= features.ret_5m_pct <= 0.35
        and features.tf5_ret_6bar_pct >= 0.50
        and 0.60 <= features.tf5_range_position <= 1.02
        and features.tf5_pullback_pct <= 0.12
        and 0.35 <= features.volume_ratio <= 4.0
        and features.higher_tf_trend_gap_pct >= 0.35
    ):
        broad_hot_pullback_long += 18.0
        broad_hot_pullback_long += bounded_score(features.ret_15m_pct, center=1.8, half_width=4.0, max_score=12.0)
        broad_hot_pullback_long += bounded_score(features.ret_30m_pct, center=2.8, half_width=6.0, max_score=12.0)
        broad_hot_pullback_long += bounded_score(-features.ret_1m_pct, center=0.16, half_width=0.45, max_score=10.0)
        broad_hot_pullback_long += bounded_score(features.ret_5m_pct, center=0.02, half_width=0.45, max_score=8.0)
        broad_hot_pullback_long += bounded_score(features.tf5_ret_6bar_pct, center=1.1, half_width=3.0, max_score=8.0)
        broad_hot_pullback_long += bounded_score(features.tf5_range_position, center=0.84, half_width=0.24, max_score=8.0)
        broad_hot_pullback_long += bounded_score(features.volume_ratio, center=1.0, half_width=1.4, max_score=6.0)
    if setup_allowed("broad_hot_pullback_long", include_setups=include_setups):
        options.append(("broad_hot_pullback_long", "BUY", broad_hot_pullback_long))

    broad_warm_momentum_long = 0.0
    if (
        3.0 <= features.price_change_24h_pct <= 35.0
        and features.ret_15m_pct >= 0.45
        and features.ret_30m_pct >= 0.65
        and -0.12 <= features.ret_1m_pct <= 0.35
        and features.ret_5m_pct >= -0.15
        and features.tf5_ret_6bar_pct >= 0.25
        and 0.45 <= features.tf5_range_position <= 0.98
        and features.tf5_pullback_pct <= 0.05
        and 0.18 <= features.volume_ratio <= 4.5
        and features.higher_tf_trend_gap_pct >= 0.20
    ):
        broad_warm_momentum_long += 18.0
        broad_warm_momentum_long += bounded_score(features.ret_15m_pct, center=1.1, half_width=3.2, max_score=14.0)
        broad_warm_momentum_long += bounded_score(features.ret_30m_pct, center=1.8, half_width=4.8, max_score=14.0)
        broad_warm_momentum_long += bounded_score(features.tf5_ret_3bar_pct, center=0.25, half_width=1.4, max_score=10.0)
        broad_warm_momentum_long += bounded_score(features.volume_ratio, center=0.75, half_width=1.9, max_score=8.0)
        broad_warm_momentum_long += bounded_score(features.tf5_range_position, center=0.78, half_width=0.32, max_score=8.0)
    if setup_allowed("broad_warm_momentum_long", include_setups=include_setups):
        options.append(("broad_warm_momentum_long", "BUY", broad_warm_momentum_long))

    broad_momentum_long = 0.0
    if (
        -1.0 <= features.price_change_24h_pct <= 12.0
        and features.ret_15m_pct >= 0.08
        and features.ret_30m_pct >= 0.10
        and features.tf5_ret_6bar_pct >= -0.08
        and features.volume_ratio >= 0.45
        and features.tf5_volume_ratio >= 0.20
        and features.higher_tf_trend_gap_pct >= 0.0
    ):
        broad_momentum_long += 16.0
        broad_momentum_long += bounded_score(features.ret_15m_pct, center=0.35, half_width=1.4, max_score=12.0)
        broad_momentum_long += bounded_score(features.ret_30m_pct, center=0.55, half_width=2.4, max_score=12.0)
        broad_momentum_long += bounded_score(features.tf5_ret_3bar_pct, center=0.04, half_width=0.40, max_score=8.0)
        broad_momentum_long += bounded_score(features.volume_ratio, center=1.20, half_width=1.2, max_score=8.0)
        broad_momentum_long += bounded_score(features.pullback_pct, center=-0.10, half_width=0.25, max_score=8.0)
        broad_momentum_long += bounded_score(features.tf5_range_position, center=0.55, half_width=0.35, max_score=8.0)
    if setup_allowed("broad_momentum_long", include_setups=include_setups):
        options.append(("broad_momentum_long", "BUY", broad_momentum_long))

    broad_mean_reversion_long = 0.0
    if (
        -2.5 <= features.price_change_24h_pct <= 2.5
        and -0.45 <= features.ret_15m_pct <= 0.12
        and -0.30 <= features.tf5_ret_3bar_pct <= 0.08
        and 0.0 <= features.range_position <= 0.26
        and 0.0 <= features.tf5_range_position <= 0.30
        and 0.25 <= features.volume_ratio <= 2.4
    ):
        broad_mean_reversion_long += 14.0
        broad_mean_reversion_long += bounded_score(features.range_position, center=0.08, half_width=0.12, max_score=12.0)
        broad_mean_reversion_long += bounded_score(features.ret_5m_pct, center=-0.08, half_width=0.18, max_score=10.0)
        broad_mean_reversion_long += bounded_score(features.pullback_pct, center=-0.12, half_width=0.25, max_score=8.0)
        broad_mean_reversion_long += bounded_score(features.volume_ratio, center=1.0, half_width=1.0, max_score=6.0)
    if setup_allowed("broad_mean_reversion_long", include_setups=include_setups):
        options.append(("broad_mean_reversion_long", "BUY", broad_mean_reversion_long))

    if allow_short:
        broad_momentum_short = 0.0
        if (
            features.price_change_24h_pct <= 0.15
            and features.ret_15m_pct <= -0.08
            and features.ret_30m_pct <= -0.10
            and features.tf5_ret_6bar_pct <= 0.08
            and features.volume_ratio >= 0.45
            and features.tf5_volume_ratio >= 0.20
            and features.higher_tf_trend_gap_pct <= 0.05
        ):
            broad_momentum_short += 16.0
            broad_momentum_short += bounded_score(-features.ret_15m_pct, center=0.35, half_width=1.4, max_score=12.0)
            broad_momentum_short += bounded_score(-features.ret_30m_pct, center=0.55, half_width=2.4, max_score=12.0)
            broad_momentum_short += bounded_score(-features.tf5_ret_3bar_pct, center=0.04, half_width=0.40, max_score=8.0)
            broad_momentum_short += bounded_score(features.volume_ratio, center=1.20, half_width=1.2, max_score=8.0)
            broad_momentum_short += bounded_score(features.bounce_pct, center=0.10, half_width=0.25, max_score=8.0)
            broad_momentum_short += bounded_score(1.0 - features.tf5_range_position, center=0.45, half_width=0.35, max_score=8.0)
        if setup_allowed("broad_momentum_short", include_setups=include_setups):
            options.append(("broad_momentum_short", "SELL", broad_momentum_short))

        broad_trend_momentum_short = 0.0
        if (
            features.price_change_24h_pct <= -3.0
            and features.ret_15m_pct <= -0.35
            and features.ret_30m_pct <= -1.8
            and features.tf5_ret_6bar_pct <= -0.15
            and 0.45 <= features.volume_ratio <= 5.5
            and 0.45 <= features.tf5_volume_ratio <= 6.0
            and features.higher_tf_trend_gap_pct <= -0.15
            and features.avg_quote_turnover_1m <= 1_000_000.0
        ):
            broad_trend_momentum_short += 20.0
            broad_trend_momentum_short += bounded_score(-features.ret_15m_pct, center=1.10, half_width=2.2, max_score=12.0)
            broad_trend_momentum_short += bounded_score(-features.ret_30m_pct, center=2.20, half_width=4.6, max_score=14.0)
            broad_trend_momentum_short += bounded_score(-features.higher_tf_trend_gap_pct, center=0.55, half_width=1.00, max_score=12.0)
            broad_trend_momentum_short += bounded_score(1.0 - features.tf5_range_position, center=0.82, half_width=0.50, max_score=8.0)
            broad_trend_momentum_short += bounded_score(features.volume_ratio, center=1.10, half_width=1.60, max_score=6.0)
        if setup_allowed("broad_trend_momentum_short", include_setups=include_setups):
            options.append(("broad_trend_momentum_short", "SELL", broad_trend_momentum_short))

        broad_pullback_short = 0.0
        if (
            features.price_change_24h_pct <= 0.35
            and features.ret_30m_pct <= -0.05
            and -0.08 <= features.ret_5m_pct <= 0.35
            and features.bounce_pct >= 0.02
            and features.tf15_trend_gap_pct <= 0.05
            and features.higher_tf_trend_gap_pct <= 0.08
            and 0.22 <= features.tf5_range_position <= 0.88
            and features.volume_ratio >= 0.35
        ):
            broad_pullback_short += 16.0
            broad_pullback_short += bounded_score(features.bounce_pct, center=0.08, half_width=0.16, max_score=12.0)
            broad_pullback_short += bounded_score(features.ret_5m_pct, center=0.04, half_width=0.16, max_score=10.0)
            broad_pullback_short += bounded_score(-features.ret_15m_pct, center=0.12, half_width=0.60, max_score=10.0)
            broad_pullback_short += bounded_score(1.0 - features.tf5_range_position, center=0.42, half_width=0.28, max_score=8.0)
            broad_pullback_short += bounded_score(features.volume_ratio, center=1.0, half_width=1.0, max_score=6.0)
        if setup_allowed("broad_pullback_short", include_setups=include_setups):
            options.append(("broad_pullback_short", "SELL", broad_pullback_short))

        broad_mean_reversion_short = 0.0
        if (
            -2.5 <= features.price_change_24h_pct <= 2.5
            and -0.12 <= features.ret_15m_pct <= 0.45
            and -0.08 <= features.tf5_ret_3bar_pct <= 0.30
            and 0.74 <= features.range_position <= 1.10
            and 0.70 <= features.tf5_range_position <= 1.10
            and 0.25 <= features.volume_ratio <= 2.4
        ):
            broad_mean_reversion_short += 14.0
            broad_mean_reversion_short += bounded_score(1.0 - features.range_position, center=0.08, half_width=0.12, max_score=12.0)
            broad_mean_reversion_short += bounded_score(features.ret_5m_pct, center=0.08, half_width=0.18, max_score=10.0)
            broad_mean_reversion_short += bounded_score(features.bounce_pct, center=0.12, half_width=0.25, max_score=8.0)
            broad_mean_reversion_short += bounded_score(features.volume_ratio, center=1.0, half_width=1.0, max_score=6.0)
        if setup_allowed("broad_mean_reversion_short", include_setups=include_setups):
            options.append(("broad_mean_reversion_short", "SELL", broad_mean_reversion_short))

        broad_hot_reversal_short = 0.0
        if (
            18.0 <= features.price_change_24h_pct <= 90.0
            and features.ret_15m_pct >= 0.8
            and features.ret_30m_pct >= 1.0
            and features.ret_1m_pct <= -0.12
            and -2.5 <= features.ret_5m_pct <= 4.5
            and features.tf5_ret_6bar_pct >= 2.0
            and 0.90 <= features.tf5_range_position <= 1.35
            and features.tf5_pullback_pct >= -0.60
            and 0.45 <= features.volume_ratio <= 4.5
            and features.higher_tf_trend_gap_pct >= 0.45
        ):
            broad_hot_reversal_short += 18.0
            broad_hot_reversal_short += bounded_score(
                features.ret_15m_pct,
                center=3.0,
                half_width=4.0,
                max_score=12.0,
            )
            broad_hot_reversal_short += bounded_score(
                features.ret_30m_pct,
                center=4.5,
                half_width=6.0,
                max_score=12.0,
            )
            broad_hot_reversal_short += bounded_score(
                -features.ret_1m_pct,
                center=0.45,
                half_width=0.70,
                max_score=12.0,
            )
            broad_hot_reversal_short += bounded_score(
                features.tf5_ret_6bar_pct,
                center=3.0,
                half_width=4.0,
                max_score=10.0,
            )
            broad_hot_reversal_short += bounded_score(
                features.tf5_range_position,
                center=1.03,
                half_width=0.25,
                max_score=8.0,
            )
            broad_hot_reversal_short += bounded_score(
                features.volume_ratio,
                center=1.1,
                half_width=1.4,
                max_score=6.0,
            )
            broad_hot_reversal_short += bounded_score(
                features.higher_tf_trend_gap_pct,
                center=0.9,
                half_width=1.4,
                max_score=6.0,
            )
        if setup_allowed("broad_hot_reversal_short", include_setups=include_setups):
            options.append(("broad_hot_reversal_short", "SELL", broad_hot_reversal_short))

        broad_hot_reversal_swing_short = 0.0
        if (
            10.0 <= features.price_change_24h_pct <= 90.0
            and features.ret_15m_pct >= 0.6
            and features.ret_30m_pct >= 0.8
            and features.ret_1m_pct <= -0.08
            and -2.8 <= features.ret_5m_pct <= 5.0
            and features.tf5_ret_6bar_pct >= 1.4
            and 0.85 <= features.tf5_range_position <= 1.35
            and features.tf5_pullback_pct >= -0.75
            and 0.35 <= features.volume_ratio <= 6.0
            and features.higher_tf_trend_gap_pct >= 0.35
        ):
            broad_hot_reversal_swing_short += 18.0
            broad_hot_reversal_swing_short += bounded_score(
                features.ret_15m_pct,
                center=2.2,
                half_width=4.0,
                max_score=12.0,
            )
            broad_hot_reversal_swing_short += bounded_score(
                features.ret_30m_pct,
                center=3.4,
                half_width=5.5,
                max_score=12.0,
            )
            broad_hot_reversal_swing_short += bounded_score(
                -features.ret_1m_pct,
                center=0.28,
                half_width=0.55,
                max_score=12.0,
            )
            broad_hot_reversal_swing_short += bounded_score(
                features.tf5_ret_6bar_pct,
                center=2.2,
                half_width=3.4,
                max_score=10.0,
            )
            broad_hot_reversal_swing_short += bounded_score(
                features.tf5_range_position,
                center=1.00,
                half_width=0.30,
                max_score=8.0,
            )
            broad_hot_reversal_swing_short += bounded_score(
                features.volume_ratio,
                center=0.95,
                half_width=1.8,
                max_score=6.0,
            )
            broad_hot_reversal_swing_short += bounded_score(
                features.higher_tf_trend_gap_pct,
                center=0.70,
                half_width=1.2,
                max_score=6.0,
            )
        if setup_allowed("broad_hot_reversal_swing_short", include_setups=include_setups):
            options.append(("broad_hot_reversal_swing_short", "SELL", broad_hot_reversal_swing_short))

        broad_hot_flush_short = 0.0
        if (
            12.0 <= features.price_change_24h_pct <= 80.0
            and features.ret_15m_pct >= 0.4
            and features.ret_30m_pct >= 0.8
            and features.ret_1m_pct <= -0.05
            and -3.0 <= features.ret_5m_pct <= 0.4
            and features.tf5_ret_6bar_pct >= 1.0
            and 0.25 <= features.tf5_range_position <= 0.95
            and features.tf5_pullback_pct <= -0.80
            and 0.40 <= features.volume_ratio <= 3.0
            and features.higher_tf_trend_gap_pct >= 0.35
        ):
            broad_hot_flush_short += 18.0
            broad_hot_flush_short += bounded_score(
                features.ret_15m_pct,
                center=1.2,
                half_width=2.2,
                max_score=10.0,
            )
            broad_hot_flush_short += bounded_score(
                features.ret_30m_pct,
                center=2.0,
                half_width=3.5,
                max_score=10.0,
            )
            broad_hot_flush_short += bounded_score(
                -features.ret_1m_pct,
                center=0.35,
                half_width=0.55,
                max_score=10.0,
            )
            broad_hot_flush_short += bounded_score(
                -features.ret_5m_pct,
                center=0.9,
                half_width=1.8,
                max_score=10.0,
            )
            broad_hot_flush_short += bounded_score(
                features.tf5_ret_6bar_pct,
                center=2.2,
                half_width=2.8,
                max_score=8.0,
            )
            broad_hot_flush_short += bounded_score(
                features.tf5_range_position,
                center=0.60,
                half_width=0.35,
                max_score=8.0,
            )
            broad_hot_flush_short += bounded_score(
                -features.tf5_pullback_pct,
                center=1.8,
                half_width=2.2,
                max_score=8.0,
            )
            broad_hot_flush_short += bounded_score(
                features.volume_ratio,
                center=0.9,
                half_width=1.1,
                max_score=6.0,
            )
        if setup_allowed("broad_hot_flush_short", include_setups=include_setups):
            options.append(("broad_hot_flush_short", "SELL", broad_hot_flush_short))

        broad_ultra_hot_reversal_short = 0.0
        if (
            25.0 <= features.price_change_24h_pct <= 120.0
            and features.ret_15m_pct >= 3.0
            and features.ret_30m_pct >= 6.0
            and features.ret_1m_pct <= -0.25
            and -2.0 <= features.ret_5m_pct <= 5.0
            and features.tf5_ret_6bar_pct >= 4.0
            and 0.95 <= features.tf5_range_position <= 1.25
            and features.tf5_pullback_pct >= -0.25
            and 0.35 <= features.volume_ratio <= 4.0
            and features.higher_tf_trend_gap_pct >= 0.80
        ):
            broad_ultra_hot_reversal_short += 18.0
            broad_ultra_hot_reversal_short += bounded_score(
                features.ret_15m_pct,
                center=5.0,
                half_width=6.0,
                max_score=12.0,
            )
            broad_ultra_hot_reversal_short += bounded_score(
                features.ret_30m_pct,
                center=10.0,
                half_width=12.0,
                max_score=14.0,
            )
            broad_ultra_hot_reversal_short += bounded_score(
                -features.ret_1m_pct,
                center=0.55,
                half_width=0.80,
                max_score=12.0,
            )
            broad_ultra_hot_reversal_short += bounded_score(
                features.tf5_range_position,
                center=1.05,
                half_width=0.20,
                max_score=10.0,
            )
            broad_ultra_hot_reversal_short += bounded_score(
                features.volume_ratio,
                center=1.0,
                half_width=1.4,
                max_score=6.0,
            )
        if setup_allowed("broad_ultra_hot_reversal_short", include_setups=include_setups):
            options.append(("broad_ultra_hot_reversal_short", "SELL", broad_ultra_hot_reversal_short))

    if not options:
        return None
    setup, side, score = max(options, key=lambda item: item[2])
    if score < 12.0:
        return None
    return candidate_from_features(features, setup=setup, side=side, score=round4(score))


def candidate_to_payload(candidate: Candidate) -> dict[str, Any]:
    payload = asdict(candidate)
    for key, value in list(payload.items()):
        if isinstance(value, float):
            payload[key] = round4(value)
    return payload


def resolve_default_snapshot_file() -> Path | None:
    candidates = [
        Path("phoenix_snapshot.openclaw.json"),
        Path("phoenix_snapshot.json"),
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def resolve_output_dir(raw_output_dir: Path | None) -> Path:
    if raw_output_dir is not None:
        path = raw_output_dir
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = Path("signal_lab_runs") / stamp
    path.mkdir(parents=True, exist_ok=True)
    return path


@dataclass(slots=True)
class MarketPriceSample:
    price: float
    observed_at_ms: int


@dataclass(slots=True)
class DerivativeSymbolState:
    mark_price: float | None = None
    index_price: float | None = None
    funding_rate: float | None = None
    next_funding_time_ms: int | None = None
    mark_index_basis_pct: float | None = None
    mark_price_ts_ms: int | None = None
    funding_rate_ts_ms: int | None = None
    liquidation_ts_ms: int | None = None
    liquidation_history: deque[tuple[int, float, float]] = field(default_factory=deque)


class DerivativeDataCache:
    """Small in-memory cache that aligns derivative WS data with event snapshots."""

    def __init__(
        self,
        *,
        liquidation_window_ms: int = DEFAULT_LIQUIDATION_WINDOW_MS,
        max_liquidation_events_per_symbol: int = 512,
    ) -> None:
        self.liquidation_window_ms = max(1_000, int(liquidation_window_ms))
        self.max_liquidation_events_per_symbol = max(8, int(max_liquidation_events_per_symbol))
        self._states: dict[str, DerivativeSymbolState] = {}

    def update_mark_payload(self, item: dict[str, Any]) -> bool:
        if not isinstance(item, dict):
            return False
        symbol = str(item.get("s") or item.get("symbol") or "").upper()
        mark_price = safe_float(item.get("p") or item.get("markPrice"))
        observed_at_ms = self._coerce_int(item.get("E") or item.get("eventTime") or now_ms())
        if not symbol or mark_price <= 0 or observed_at_ms is None:
            return False
        state = self._state_for(symbol)
        state.mark_price = mark_price
        state.mark_price_ts_ms = observed_at_ms
        index_price = safe_float(item.get("i") or item.get("indexPrice"))
        if index_price > 0:
            state.index_price = index_price
            state.mark_index_basis_pct = ((mark_price - index_price) / index_price) * 100.0
        funding_rate_raw = item.get("r") if "r" in item else item.get("lastFundingRate")
        if funding_rate_raw not in (None, ""):
            state.funding_rate = safe_float(funding_rate_raw)
            state.funding_rate_ts_ms = observed_at_ms
        next_funding_time_ms = self._coerce_int(item.get("T") or item.get("nextFundingTime"))
        if next_funding_time_ms is not None:
            state.next_funding_time_ms = next_funding_time_ms
        return True

    def update_liquidation_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        order = payload.get("o")
        if not isinstance(order, dict):
            return None
        symbol = str(order.get("s") or "").upper()
        side = str(order.get("S") or "").upper()
        average_price = safe_float(order.get("ap") or order.get("p"))
        filled_qty = safe_float(order.get("z") or order.get("l") or order.get("q"))
        event_ts_ms = self._coerce_int(order.get("T") or payload.get("E") or now_ms())
        if not symbol or average_price <= 0 or filled_qty <= 0 or event_ts_ms is None:
            return None
        notional = average_price * filled_qty
        long_liq_usd = notional if side == "SELL" else 0.0
        short_liq_usd = notional if side == "BUY" else 0.0
        state = self._state_for(symbol)
        state.liquidation_ts_ms = event_ts_ms
        history = state.liquidation_history
        history.append((event_ts_ms, long_liq_usd, short_liq_usd))
        while len(history) > self.max_liquidation_events_per_symbol:
            history.popleft()
        self._prune_liquidations(state, event_ts_ms)
        snapshot = self.snapshot(symbol, observed_at_ms=event_ts_ms)
        return {
            "symbol": symbol,
            "side": side,
            "notional_usd": notional,
            "event_ts_ms": event_ts_ms,
            "liquidation_long_usd_15m": snapshot.get("liquidation_long_usd_15m"),
            "liquidation_short_usd_15m": snapshot.get("liquidation_short_usd_15m"),
            "liquidation_event_count_15m": snapshot.get("liquidation_event_count_15m"),
        }

    def snapshot(self, symbol: str, *, observed_at_ms: int | None = None) -> dict[str, Any]:
        token = str(symbol or "").upper()
        state = self._states.get(token)
        if state is None:
            return self._empty_snapshot()
        timestamp_ms = now_ms() if observed_at_ms is None else int(observed_at_ms)
        self._prune_liquidations(state, timestamp_ms)
        long_liq_usd = sum(item[1] for item in state.liquidation_history)
        short_liq_usd = sum(item[2] for item in state.liquidation_history)
        source = "ws" if any(
            value is not None
            for value in (state.mark_price_ts_ms, state.funding_rate_ts_ms, state.liquidation_ts_ms)
        ) else "missing"
        return {
            "derivatives_data_source": source,
            "derivatives_mark_price_ts_ms": state.mark_price_ts_ms,
            "derivatives_funding_rate_ts_ms": state.funding_rate_ts_ms,
            "derivatives_liquidation_ts_ms": state.liquidation_ts_ms,
            "derivatives_mark_price_age_ms": (
                max(0, timestamp_ms - state.mark_price_ts_ms) if state.mark_price_ts_ms is not None else None
            ),
            "derivatives_funding_rate_age_ms": (
                max(0, timestamp_ms - state.funding_rate_ts_ms) if state.funding_rate_ts_ms is not None else None
            ),
            "derivatives_liquidation_age_ms": (
                max(0, timestamp_ms - state.liquidation_ts_ms) if state.liquidation_ts_ms is not None else None
            ),
            "mark_price": state.mark_price,
            "index_price": state.index_price,
            "funding_rate": state.funding_rate,
            "next_funding_time_ms": state.next_funding_time_ms,
            "mark_index_basis_pct": state.mark_index_basis_pct,
            "liquidation_long_usd_15m": long_liq_usd,
            "liquidation_short_usd_15m": short_liq_usd,
            "liquidation_event_count_15m": len(state.liquidation_history),
            "liquidation_long_usd": long_liq_usd,
            "liquidation_short_usd": short_liq_usd,
            "liquidation_event_count": len(state.liquidation_history),
        }

    def _state_for(self, symbol: str) -> DerivativeSymbolState:
        token = str(symbol or "").upper()
        state = self._states.get(token)
        if state is None:
            state = DerivativeSymbolState()
            self._states[token] = state
        return state

    def _prune_liquidations(self, state: DerivativeSymbolState, timestamp_ms: int) -> None:
        lower_bound = int(timestamp_ms) - self.liquidation_window_ms
        history = state.liquidation_history
        while history and history[0][0] < lower_bound:
            history.popleft()

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _empty_snapshot() -> dict[str, Any]:
        return {
            "derivatives_data_source": "missing",
            "derivatives_mark_price_ts_ms": None,
            "derivatives_funding_rate_ts_ms": None,
            "derivatives_liquidation_ts_ms": None,
            "derivatives_mark_price_age_ms": None,
            "derivatives_funding_rate_age_ms": None,
            "derivatives_liquidation_age_ms": None,
            "mark_price": None,
            "index_price": None,
            "funding_rate": None,
            "next_funding_time_ms": None,
            "mark_index_basis_pct": None,
            "liquidation_long_usd_15m": None,
            "liquidation_short_usd_15m": None,
            "liquidation_event_count_15m": None,
            "liquidation_long_usd": None,
            "liquidation_short_usd": None,
            "liquidation_event_count": None,
        }


@dataclass(slots=True)
class HorizonObservation:
    horizon_sec: int
    deadline_ms: int
    last_return_pct: float = 0.0
    final_return_pct: float | None = None
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    finalized_at_ms: int | None = None
    finalized_at_iso: str | None = None
    final_price: float | None = None
    final_price_source: str | None = None


@dataclass(slots=True)
class CandidateObservation:
    observation_id: str
    cycle_no: int
    rank: int
    observed_at_ms: int
    observed_at_iso: str
    symbol: str
    setup: str
    side: str
    score: float
    entry_mark_price: float
    candidate: dict[str, Any]
    scout: dict[str, Any]
    horizons: dict[int, HorizonObservation] = field(default_factory=dict)


class ScoutSnapshotIndex:
    def __init__(self, snapshot_file: Path | None) -> None:
        self.snapshot_file = snapshot_file
        self._last_mtime_ns = -1
        self._symbols: dict[str, dict[str, Any]] = {}

    def current(self) -> dict[str, dict[str, Any]]:
        if self.snapshot_file is None or not self.snapshot_file.exists():
            return {}
        stat = self.snapshot_file.stat()
        if stat.st_mtime_ns == self._last_mtime_ns:
            return self._symbols
        try:
            payload = json.loads(self.snapshot_file.read_text(encoding="utf-8"))
        except Exception:
            return self._symbols
        generated_at_ms = int(payload.get("generated_at_ms") or 0)
        snapshot_now_ms = now_ms()
        loaded: dict[str, dict[str, Any]] = {}
        for item in payload.get("symbols", []):
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").upper()
            if not symbol:
                continue
            scout_record: dict[str, Any] = {}
            for key in SCOUT_SNAPSHOT_KEYS:
                if key in item and item.get(key) is not None:
                    scout_record[key] = item.get(key)
            scout_record["snapshot_generated_at_ms"] = generated_at_ms or None
            scout_record["snapshot_age_ms"] = (
                max(0, snapshot_now_ms - generated_at_ms) if generated_at_ms > 0 else None
            )
            loaded[symbol] = scout_record
        self._symbols = loaded
        self._last_mtime_ns = stat.st_mtime_ns
        return self._symbols


class SharedSymbolLeases:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock_path = path.with_name(f"{path.name}.lock")

    def try_claim(self, symbol: str, *, observed_at_ms: int, cooldown_ms: int) -> bool:
        if cooldown_ms <= 0:
            return True
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd: int | None = None
        deadline = time.time() + 2.0
        while lock_fd is None:
            try:
                lock_fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except FileExistsError:
                if time.time() >= deadline:
                    return False
                time.sleep(0.05)
        try:
            state = self._read_state()
            last_seen_ms = int(state.get(symbol) or 0)
            if observed_at_ms - last_seen_ms < cooldown_ms:
                return False
            state[symbol] = observed_at_ms
            temp_path = self.path.with_name(f"{self.path.name}.tmp")
            temp_path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
            temp_path.replace(self.path)
            return True
        finally:
            os.close(lock_fd)
            with contextlib.suppress(FileNotFoundError):
                os.remove(self.lock_path)

    def _read_state(self) -> dict[str, int]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        state: dict[str, int] = {}
        for symbol, value in payload.items():
            token = str(symbol or "").upper()
            if not token:
                continue
            try:
                state[token] = int(value)
            except Exception:
                continue
        return state


class ObservationStore:
    def __init__(
        self,
        *,
        output_dir: Path,
        horizons_sec: list[int],
        dedupe_sec: int,
        max_observations_per_symbol: int,
        symbol_cooldown_sec: int,
        max_active_per_symbol: int,
        shared_symbol_leases: SharedSymbolLeases | None,
    ) -> None:
        self.output_dir = output_dir
        self.horizons_sec = horizons_sec
        self.dedupe_ms = max(0, dedupe_sec) * 1000
        self.symbol_cooldown_ms = max(0, int(symbol_cooldown_sec)) * 1000
        self.max_observations_per_symbol = max(0, int(max_observations_per_symbol))
        self.max_active_per_symbol = max(0, int(max_active_per_symbol))
        self.shared_symbol_leases = shared_symbol_leases
        self.observations_file = output_dir / "candidate_observations.jsonl"
        self.labels_file = output_dir / "candidate_labels.jsonl"
        self.active: dict[str, CandidateObservation] = {}
        self.active_by_symbol: dict[str, set[str]] = defaultdict(set)
        self.last_seen_by_key_ms: dict[str, int] = {}
        self.last_created_by_symbol_ms: dict[str, int] = {}
        self.created_by_symbol: dict[str, int] = defaultdict(int)
        self.created_count = 0
        self.labeled_count = 0
        self._finalize_lock = asyncio.Lock()

    def can_accept_symbol(self, symbol: str, *, observed_at_ms: int | None = None) -> bool:
        timestamp_ms = now_ms() if observed_at_ms is None else int(observed_at_ms)
        if self.max_observations_per_symbol > 0 and self.created_by_symbol.get(symbol, 0) >= self.max_observations_per_symbol:
            return False
        if self.max_active_per_symbol > 0 and len(self.active_by_symbol.get(symbol, set())) >= self.max_active_per_symbol:
            return False
        last_created_ms = int(self.last_created_by_symbol_ms.get(symbol) or 0)
        if self.symbol_cooldown_ms > 0 and timestamp_ms - last_created_ms < self.symbol_cooldown_ms:
            return False
        return True

    def can_accept_candidate(self, candidate: Candidate, *, observed_at_ms: int | None = None) -> bool:
        timestamp_ms = now_ms() if observed_at_ms is None else int(observed_at_ms)
        if not self.can_accept_symbol(candidate.symbol, observed_at_ms=timestamp_ms):
            return False
        dedupe_key = f"{candidate.symbol}|{candidate.setup}|{candidate.side}"
        last_seen_ms = int(self.last_seen_by_key_ms.get(dedupe_key) or 0)
        if self.dedupe_ms > 0 and timestamp_ms - last_seen_ms < self.dedupe_ms:
            return False
        return True

    def create(
        self,
        *,
        cycle_no: int,
        rank: int,
        candidate: Candidate,
        entry_mark_price: float,
        scout: dict[str, Any] | None,
    ) -> CandidateObservation | None:
        observed_at_ms = now_ms()
        if not self.can_accept_candidate(candidate, observed_at_ms=observed_at_ms):
            return None
        if self.shared_symbol_leases is not None and not self.shared_symbol_leases.try_claim(
            candidate.symbol,
            observed_at_ms=observed_at_ms,
            cooldown_ms=self.symbol_cooldown_ms,
        ):
            return None
        dedupe_key = f"{candidate.symbol}|{candidate.setup}|{candidate.side}"
        self.last_seen_by_key_ms[dedupe_key] = observed_at_ms
        self.last_created_by_symbol_ms[candidate.symbol] = observed_at_ms
        observation_id = (
            f"{observed_at_ms}-{cycle_no}-{rank}-"
            f"{candidate.symbol}-{candidate.setup}-{candidate.side}".replace(" ", "_")
        )
        observation = CandidateObservation(
            observation_id=observation_id,
            cycle_no=cycle_no,
            rank=rank,
            observed_at_ms=observed_at_ms,
            observed_at_iso=utc_now_iso(),
            symbol=candidate.symbol,
            setup=candidate.setup,
            side=candidate.side,
            score=round4(candidate.score),
            entry_mark_price=round4(entry_mark_price),
            candidate=candidate_to_payload(candidate),
            scout=dict(scout or {}),
            horizons={
                horizon_sec: HorizonObservation(
                    horizon_sec=horizon_sec,
                    deadline_ms=observed_at_ms + (horizon_sec * 1000),
                )
                for horizon_sec in self.horizons_sec
            },
        )
        self.active[observation_id] = observation
        self.active_by_symbol[candidate.symbol].add(observation_id)
        self.created_by_symbol[candidate.symbol] += 1
        self.created_count += 1
        self._append_jsonl(
            self.observations_file,
            {
                "event": "observation_created",
                "observation_id": observation.observation_id,
                "observed_at": observation.observed_at_iso,
                "observed_at_ms": observation.observed_at_ms,
                "cycle_no": observation.cycle_no,
                "rank": observation.rank,
                "symbol": observation.symbol,
                "setup": observation.setup,
                "side": observation.side,
                "score": observation.score,
                "entry_mark_price": observation.entry_mark_price,
                "candidate": observation.candidate,
                "scout": observation.scout,
                "horizons_sec": self.horizons_sec,
            },
        )
        return observation

    def update_mark_price(self, symbol: str, price: float, observed_at_ms: int) -> None:
        if price <= 0:
            return
        for observation_id in list(self.active_by_symbol.get(symbol, set())):
            observation = self.active.get(observation_id)
            if observation is None:
                continue
            if observed_at_ms < observation.observed_at_ms:
                continue
            current_return_pct = side_aware_return_pct(
                side=observation.side,
                entry_price=observation.entry_mark_price,
                current_price=price,
            )
            for horizon in observation.horizons.values():
                if horizon.final_return_pct is not None:
                    continue
                horizon.last_return_pct = round4(current_return_pct)
                horizon.mfe_pct = round4(max(horizon.mfe_pct, current_return_pct))
                horizon.mae_pct = round4(min(horizon.mae_pct, current_return_pct))

    async def finalize_due(
        self,
        *,
        futures: BinanceFuturesClient,
        latest_mark_prices: dict[str, MarketPriceSample],
        force_all: bool = False,
    ) -> None:
        async with self._finalize_lock:
            if not self.active:
                return
            timestamp_ms = now_ms()
            fallback_prices: dict[str, float] = {}
            for observation in list(self.active.values()):
                for horizon in observation.horizons.values():
                    if horizon.final_return_pct is not None:
                        continue
                    if not force_all and timestamp_ms < horizon.deadline_ms:
                        continue
                    price_sample = latest_mark_prices.get(observation.symbol)
                    price = 0.0
                    price_source = "stream"
                    if price_sample is not None and timestamp_ms - price_sample.observed_at_ms <= FRESH_PRICE_MAX_AGE_MS:
                        price = price_sample.price
                    else:
                        if observation.symbol not in fallback_prices:
                            payload = await futures.mark_price(observation.symbol)
                            fallback_prices[observation.symbol] = safe_float(payload.get("markPrice"))
                        price = fallback_prices[observation.symbol]
                        price_source = "rest"
                    if price <= 0:
                        continue
                    current_return_pct = side_aware_return_pct(
                        side=observation.side,
                        entry_price=observation.entry_mark_price,
                        current_price=price,
                    )
                    horizon.last_return_pct = round4(current_return_pct)
                    horizon.mfe_pct = round4(max(horizon.mfe_pct, current_return_pct))
                    horizon.mae_pct = round4(min(horizon.mae_pct, current_return_pct))
                    horizon.final_return_pct = round4(current_return_pct)
                    horizon.finalized_at_ms = timestamp_ms
                    horizon.finalized_at_iso = utc_now_iso()
                    horizon.final_price = round4(price)
                    horizon.final_price_source = price_source
                if self._is_complete(observation):
                    self._finalize_observation(observation)

    def has_active(self) -> bool:
        return bool(self.active)

    def summary(self) -> dict[str, Any]:
        return {
            "created": self.created_count,
            "labeled": self.labeled_count,
            "active": len(self.active),
        }

    def _is_complete(self, observation: CandidateObservation) -> bool:
        return all(horizon.final_return_pct is not None for horizon in observation.horizons.values())

    def _finalize_observation(self, observation: CandidateObservation) -> None:
        self.labeled_count += 1
        horizons_payload = [
            {
                "horizon_sec": horizon.horizon_sec,
                "deadline_ms": horizon.deadline_ms,
                "final_return_pct": horizon.final_return_pct,
                "mfe_pct": horizon.mfe_pct,
                "mae_pct": horizon.mae_pct,
                "finalized_at_ms": horizon.finalized_at_ms,
                "finalized_at": horizon.finalized_at_iso,
                "final_price": horizon.final_price,
                "final_price_source": horizon.final_price_source,
            }
            for _, horizon in sorted(observation.horizons.items())
        ]
        self._append_jsonl(
            self.labels_file,
            {
                "event": "observation_labeled",
                "observation_id": observation.observation_id,
                "observed_at": observation.observed_at_iso,
                "observed_at_ms": observation.observed_at_ms,
                "cycle_no": observation.cycle_no,
                "rank": observation.rank,
                "symbol": observation.symbol,
                "setup": observation.setup,
                "side": observation.side,
                "score": observation.score,
                "entry_mark_price": observation.entry_mark_price,
                "candidate": observation.candidate,
                "scout": observation.scout,
                "horizons": horizons_payload,
            },
        )
        emit_event(
            "signal_lab_observation_labeled",
            observation_id=observation.observation_id,
            symbol=observation.symbol,
            setup=observation.setup,
            side=observation.side,
            score=observation.score,
            horizons=[
                {
                    "horizon_sec": item["horizon_sec"],
                    "final_return_pct": item["final_return_pct"],
                    "mfe_pct": item["mfe_pct"],
                    "mae_pct": item["mae_pct"],
                    "final_price_source": item["final_price_source"],
                }
                for item in horizons_payload
            ],
        )
        self.active.pop(observation.observation_id, None)
        symbol_bucket = self.active_by_symbol.get(observation.symbol)
        if symbol_bucket is not None:
            symbol_bucket.discard(observation.observation_id)
            if not symbol_bucket:
                self.active_by_symbol.pop(observation.symbol, None)

    @staticmethod
    def _append_jsonl(path: Path, payload: dict[str, Any], *, compact: bool = False) -> None:
        with path.open("a", encoding="utf-8") as handle:
            if compact:
                handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
            else:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


@dataclass(slots=True)
class RankResult:
    candidates: list[Candidate]
    strict_candidates: int = 0
    broad_candidates: int = 0
    effective_scan_offset: int = 0


@dataclass(slots=True)
class EventLabelHorizon:
    horizon_sec: int
    deadline_ms: int
    close_return_pct: float | None = None
    after_fee_return_pct: float | None = None
    max_runup_pct: float | None = None
    max_drawdown_pct: float | None = None
    finalized_at_ms: int | None = None
    finalized_at_iso: str | None = None
    close_price: float | None = None
    candle_count: int = 0


@dataclass(slots=True)
class EventObservation:
    event_id: str
    cycle_no: int
    rank: int
    observed_at_ms: int
    observed_at_iso: str
    trading_session: str
    symbol: str
    sample_type: str
    bar_interval: str
    trigger_types: list[str]
    trigger_score: float
    sample: dict[str, Any]
    enrichments: dict[str, Any]
    anchor_close_time_ms: int
    research_contexts: dict[str, Any] = field(default_factory=dict)
    horizons: dict[int, EventLabelHorizon] = field(default_factory=dict)


@dataclass(slots=True)
class EventScanResult:
    triggered_samples: list[MarketEventSample]
    eligible_contexts: list[MarketEventContext]
    effective_scan_offset: int
    shortlist_count: int
    research_contexts_by_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)


def build_market_event_research_context(
    context: MarketEventContext,
    *,
    config: EventTriggerConfig,
) -> dict[str, Any]:
    confirmation_sample = build_triggered_market_event(context, config=config)
    trigger_types = list(confirmation_sample.trigger_types) if confirmation_sample is not None else []
    return {
        "bar_interval": context.bar_interval,
        "anchor_open_time_ms": context.anchor_open_time_ms,
        "anchor_close_time_ms": context.anchor_close_time_ms,
        "price": round4(context.price),
        "candle_direction": context.candle_direction,
        "current_volume": round4(context.current_volume),
        "avg_volume_20": round4(context.avg_volume_20),
        "volume_burst_ratio": round4(context.volume_burst_ratio),
        "current_quote_turnover": round4(context.current_quote_turnover),
        "avg_quote_turnover_20": round4(context.avg_quote_turnover_20),
        "candle_body_pct": round4(context.candle_body_pct),
        "candle_range_pct": round4(context.candle_range_pct),
        "atr_20_pct": round4(context.atr_20_pct),
        "body_to_atr": round4(context.body_to_atr),
        "range_to_atr": round4(context.range_to_atr),
        "ret_1bar_pct": round4(context.ret_1bar_pct),
        "ret_5bar_pct": round4(context.ret_5bar_pct),
        "ret_15bar_pct": round4(context.ret_15bar_pct),
        "ret_60bar_pct": round4(context.ret_60bar_pct),
        "trigger_types": trigger_types,
        "trigger_signature": sample_event_signature(confirmation_sample) if confirmation_sample is not None else "none",
        "trigger_score": round4(confirmation_sample.trigger_score) if confirmation_sample is not None else 0.0,
    }


def normalize_research_contexts(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for interval, payload in value.items():
        if isinstance(payload, dict):
            normalized[str(interval)] = dict(payload)
    return normalized


def attach_research_contexts(
    enrichments: dict[str, Any],
    research_contexts: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized = normalize_research_contexts(research_contexts)
    if not normalized:
        return enrichments
    enriched = dict(enrichments)
    enriched["research_contexts"] = normalized
    if "3m" in normalized:
        enriched["context_3m"] = normalized["3m"]
    return enriched


class EventCollectionStore:
    def __init__(
        self,
        *,
        output_dir: Path,
        horizons_sec: list[int],
        dedupe_sec: int,
        symbol_cooldown_sec: int,
        max_active_per_symbol: int,
        round_trip_fee_bps: float,
    ) -> None:
        self.output_dir = output_dir
        self.horizons_sec = horizons_sec
        self.dedupe_ms = max(0, dedupe_sec) * 1000
        self.symbol_cooldown_ms = max(0, int(symbol_cooldown_sec)) * 1000
        self.max_active_per_symbol = max(0, int(max_active_per_symbol))
        self.round_trip_fee_bps = max(0.0, float(round_trip_fee_bps or 0.0))
        self.snapshots_file = output_dir / "event_snapshots.jsonl"
        self.bridge_feed_file = output_dir / "bridge_event_feed.jsonl"
        self.horizon_labels_file = output_dir / "event_horizon_labels.jsonl"
        self.labels_file = output_dir / "event_labels.jsonl"
        self.active: dict[str, EventObservation] = {}
        self.active_by_symbol: dict[str, set[str]] = defaultdict(set)
        self.last_seen_by_key_ms: dict[str, int] = {}
        self.last_created_by_symbol_ms: dict[str, int] = {}
        self.created_count = 0
        self.labeled_count = 0
        self.label_tasks: set[asyncio.Task[None]] = set()
        self._state_lock = asyncio.Lock()

    def can_accept_symbol(self, symbol: str, *, observed_at_ms: int | None = None) -> bool:
        timestamp_ms = now_ms() if observed_at_ms is None else int(observed_at_ms)
        if self.max_active_per_symbol > 0 and len(self.active_by_symbol.get(symbol, set())) >= self.max_active_per_symbol:
            return False
        last_created_ms = int(self.last_created_by_symbol_ms.get(symbol) or 0)
        if self.symbol_cooldown_ms > 0 and timestamp_ms - last_created_ms < self.symbol_cooldown_ms:
            return False
        return True

    def can_accept_sample(self, sample: MarketEventSample, *, observed_at_ms: int | None = None) -> bool:
        timestamp_ms = now_ms() if observed_at_ms is None else int(observed_at_ms)
        if not self.can_accept_symbol(sample.symbol, observed_at_ms=timestamp_ms):
            return False
        dedupe_key = (
            f"{sample.symbol}|{sample.sample_type}|{sample.bar_interval}|"
            f"{','.join(sample.trigger_types)}"
        )
        last_seen_ms = int(self.last_seen_by_key_ms.get(dedupe_key) or 0)
        if self.dedupe_ms > 0 and timestamp_ms - last_seen_ms < self.dedupe_ms:
            return False
        return True

    def create(
        self,
        *,
        cycle_no: int,
        rank: int,
        sample: MarketEventSample,
        enrichments: dict[str, Any],
    ) -> EventObservation | None:
        observed_at_ms = now_ms()
        if not self.can_accept_sample(sample, observed_at_ms=observed_at_ms):
            return None
        research_contexts = normalize_research_contexts(enrichments.get("research_contexts"))
        if "3m" not in research_contexts and isinstance(enrichments.get("context_3m"), dict):
            research_contexts["3m"] = dict(enrichments["context_3m"])
        dedupe_key = (
            f"{sample.symbol}|{sample.sample_type}|{sample.bar_interval}|"
            f"{','.join(sample.trigger_types)}"
        )
        self.last_seen_by_key_ms[dedupe_key] = observed_at_ms
        self.last_created_by_symbol_ms[sample.symbol] = observed_at_ms
        event_id = (
            f"{observed_at_ms}-{cycle_no}-{rank}-"
            f"{sample.symbol}-{sample.sample_type}-{sample.bar_interval}"
        ).replace(" ", "_")
        trading_session = trading_session_label(int(sample.anchor_close_time_ms))
        observation = EventObservation(
            event_id=event_id,
            cycle_no=cycle_no,
            rank=rank,
            observed_at_ms=observed_at_ms,
            observed_at_iso=utc_now_iso(),
            trading_session=trading_session,
            symbol=sample.symbol,
            sample_type=sample.sample_type,
            bar_interval=sample.bar_interval,
            trigger_types=list(sample.trigger_types),
            trigger_score=round4(sample.trigger_score),
            sample=sample.to_payload(),
            enrichments=dict(enrichments),
            anchor_close_time_ms=int(sample.anchor_close_time_ms),
            research_contexts=research_contexts,
            horizons={
                horizon_sec: EventLabelHorizon(
                    horizon_sec=horizon_sec,
                    deadline_ms=int(sample.anchor_close_time_ms) + (horizon_sec * 1000),
                )
                for horizon_sec in self.horizons_sec
            },
        )
        self.active[event_id] = observation
        self.active_by_symbol[sample.symbol].add(event_id)
        self.created_count += 1
        self._append_jsonl(self.snapshots_file, self._build_snapshot_payload(observation))
        try:
            self._append_jsonl(
                self.bridge_feed_file,
                self._build_bridge_feed_payload(observation),
                compact=True,
            )
        except Exception as exc:  # noqa: BLE001
            emit_event(
                "signal_lab_bridge_feed_write_failed",
                event_id=observation.event_id,
                symbol=observation.symbol,
                error=str(exc),
            )
        emit_event(
            "signal_lab_event_created",
            event_id=observation.event_id,
            symbol=observation.symbol,
            sample_type=observation.sample_type,
            bar_interval=observation.bar_interval,
            trading_session=observation.trading_session,
            trigger_types=observation.trigger_types,
            trigger_score=observation.trigger_score,
        )
        return observation

    def _build_snapshot_payload(self, observation: EventObservation) -> dict[str, Any]:
        payload = {
            "event": "market_event_created",
            "event_id": observation.event_id,
            "observed_at": observation.observed_at_iso,
            "observed_at_ms": observation.observed_at_ms,
            "trading_session": observation.trading_session,
            "cycle_no": observation.cycle_no,
            "rank": observation.rank,
            "symbol": observation.symbol,
            "sample_type": observation.sample_type,
            "bar_interval": observation.bar_interval,
            "trigger_types": observation.trigger_types,
            "trigger_score": observation.trigger_score,
            "sample": observation.sample,
            "enrichments": observation.enrichments,
            "research_contexts": observation.research_contexts,
            "context_3m": observation.research_contexts.get("3m"),
            "horizons_sec": self.horizons_sec,
        }
        payload["factors"] = build_factor_vector(payload)
        return payload

    def _build_bridge_feed_payload(self, observation: EventObservation) -> dict[str, Any]:
        sample_payload = observation.sample if isinstance(observation.sample, dict) else {}
        enrichments = observation.enrichments if isinstance(observation.enrichments, dict) else {}
        record = {
            "event": "market_event_created",
            "event_id": observation.event_id,
            "observed_at_ms": observation.observed_at_ms,
            "trading_session": observation.trading_session,
            "symbol": observation.symbol,
            "sample_type": observation.sample_type,
            "bar_interval": observation.bar_interval,
            "trigger_types": observation.trigger_types,
            "trigger_score": observation.trigger_score,
            "sample": {
                "price": sample_payload.get("price"),
                "anchor_close_time_ms": sample_payload.get("anchor_close_time_ms"),
                "trigger_anchor_close_time_ms": sample_payload.get("trigger_anchor_close_time_ms"),
                "quote_volume_24h": sample_payload.get("quote_volume_24h"),
                "price_change_24h_pct": sample_payload.get("price_change_24h_pct"),
                "candle_direction": sample_payload.get("candle_direction"),
                "trigger_candle_direction": sample_payload.get("trigger_candle_direction"),
                "confirmation_candle_direction": sample_payload.get("confirmation_candle_direction"),
                "reversal_confirmation_passed": sample_payload.get("reversal_confirmation_passed"),
                "reversal_confirmation_bar_interval": sample_payload.get("reversal_confirmation_bar_interval"),
                "volume_burst_ratio": sample_payload.get("volume_burst_ratio"),
                "range_to_atr": sample_payload.get("range_to_atr"),
                "body_to_atr": sample_payload.get("body_to_atr"),
                "ret_1bar_pct": sample_payload.get("ret_1bar_pct"),
                "ret_5bar_pct": sample_payload.get("ret_5bar_pct"),
                "ret_15bar_pct": sample_payload.get("ret_15bar_pct"),
                "ret_60bar_pct": sample_payload.get("ret_60bar_pct"),
            },
            "enrichments": {
                "derivatives_data_source": enrichments.get("derivatives_data_source"),
                "mark_price": enrichments.get("mark_price"),
                "index_price": enrichments.get("index_price"),
                "funding_rate": enrichments.get("funding_rate"),
                "next_funding_time_ms": enrichments.get("next_funding_time_ms"),
                "mark_index_basis_pct": enrichments.get("mark_index_basis_pct"),
                "btcusdt_ret_5m_pct": enrichments.get("btcusdt_ret_5m_pct"),
                "btcusdt_ret_60m_pct": enrichments.get("btcusdt_ret_60m_pct"),
                "btcusdt_ret_1h_pct": enrichments.get("btcusdt_ret_1h_pct"),
                "ethusdt_ret_5m_pct": enrichments.get("ethusdt_ret_5m_pct"),
                "ethusdt_ret_60m_pct": enrichments.get("ethusdt_ret_60m_pct"),
                "ethusdt_ret_1h_pct": enrichments.get("ethusdt_ret_1h_pct"),
                "oi_change_5m_pct": enrichments.get("oi_change_5m_pct"),
                "oi_change_15m_pct": enrichments.get("oi_change_15m_pct"),
                "spread_bps": enrichments.get("spread_bps"),
                "bid_depth_notional_5": enrichments.get("bid_depth_notional_5"),
                "ask_depth_notional_5": enrichments.get("ask_depth_notional_5"),
                "depth_imbalance": enrichments.get("depth_imbalance"),
                "estimated_slippage_bps": enrichments.get("estimated_slippage_bps"),
                "estimated_slippage_for_order_usdt": enrichments.get("estimated_slippage_for_order_usdt"),
                "taker_buy_ratio_1m": enrichments.get("taker_buy_ratio_1m"),
                "taker_buy_ratio_5m": enrichments.get("taker_buy_ratio_5m"),
                "aggressive_flow_delta": enrichments.get("aggressive_flow_delta"),
                "volume_5m_ratio_short": enrichments.get("volume_5m_ratio_short"),
                "volume_5m_ratio_medium": enrichments.get("volume_5m_ratio_medium"),
                "volume_5m_ratio_long": enrichments.get("volume_5m_ratio_long"),
                "liquidation_long_usd_15m": enrichments.get("liquidation_long_usd_15m"),
                "liquidation_short_usd_15m": enrichments.get("liquidation_short_usd_15m"),
                "liquidation_event_count_15m": enrichments.get("liquidation_event_count_15m"),
                "liquidation_long_usd": enrichments.get("liquidation_long_usd"),
                "liquidation_short_usd": enrichments.get("liquidation_short_usd"),
                "liquidation_event_count": enrichments.get("liquidation_event_count"),
            },
            "context_3m": observation.research_contexts.get("3m"),
        }
        record["factors"] = build_factor_vector(record)
        record["playbook"] = event_context_label(record, split_by="playbook")
        return record

    def schedule_labeling(self, observation: EventObservation, *, futures: BinanceFuturesClient) -> None:
        task = asyncio.create_task(
            self._label_observation(observation, futures=futures),
            name=f"signal-lab-label-{observation.event_id}",
        )
        self.label_tasks.add(task)
        task.add_done_callback(self.label_tasks.discard)

    async def _label_observation(
        self,
        observation: EventObservation,
        *,
        futures: BinanceFuturesClient,
    ) -> None:
        try:
            for _, horizon in sorted(observation.horizons.items()):
                sleep_sec = max(0.0, (horizon.deadline_ms - now_ms()) / 1000.0)
                if sleep_sec > 0:
                    await asyncio.sleep(sleep_sec)
                for attempt in range(3):
                    if await self._finalize_horizon(observation, horizon, futures=futures):
                        break
                    await asyncio.sleep(float(attempt + 1))
                else:
                    emit_event(
                        "signal_lab_event_label_incomplete",
                        event_id=observation.event_id,
                        symbol=observation.symbol,
                        horizon_sec=horizon.horizon_sec,
                    )
                    await self._release_observation(
                        observation,
                        status="label_incomplete",
                        horizon_sec=horizon.horizon_sec,
                    )
                    return
            await self._finalize_observation(observation)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            emit_event(
                "signal_lab_event_label_error",
                event_id=observation.event_id,
                symbol=observation.symbol,
                error=str(exc),
            )
            await self._release_observation(
                observation,
                status="label_error",
                error=str(exc),
            )

    async def _finalize_horizon(
        self,
        observation: EventObservation,
        horizon: EventLabelHorizon,
        *,
        futures: BinanceFuturesClient,
    ) -> bool:
        if horizon.close_return_pct is not None:
            return True
        start_time_ms = observation.anchor_close_time_ms + 1
        limit = max(8, min(1000, int(horizon.horizon_sec / 60) + 8))
        payload = await futures.klines(
            observation.symbol,
            interval="1m",
            start_time_ms=start_time_ms,
            end_time_ms=horizon.deadline_ms + 60_000,
            limit=limit,
        )
        candles = [
            row
            for row in payload
            if isinstance(row, list)
            and len(row) >= 7
            and int(row[6]) <= horizon.deadline_ms
        ]
        if not candles:
            mark_payload = await futures.mark_price(observation.symbol)
            close_price = safe_float(mark_payload.get("markPrice"))
            if close_price <= 0:
                return False
            candles = [
                [
                    horizon.deadline_ms - 60_000,
                    observation.sample["price"],
                    close_price,
                    close_price,
                    close_price,
                    0.0,
                    horizon.deadline_ms,
                ]
            ]
        label = compute_future_path_label(
            entry_price=safe_float(observation.sample.get("price")),
            candles_1m=candles,
            horizon_sec=horizon.horizon_sec,
            round_trip_fee_bps=self.round_trip_fee_bps,
        )
        if label is None:
            return False
        horizon.close_return_pct = round4(label.close_return_pct)
        horizon.after_fee_return_pct = round4(label.after_fee_return_pct)
        horizon.max_runup_pct = round4(label.max_runup_pct)
        horizon.max_drawdown_pct = round4(label.max_drawdown_pct)
        horizon.finalized_at_ms = now_ms()
        horizon.finalized_at_iso = utc_now_iso()
        horizon.close_price = round4(label.close_price)
        horizon.candle_count = int(label.candle_count)
        self._append_jsonl(
            self.horizon_labels_file,
            {
                "event": "market_event_horizon_labeled",
                "event_id": observation.event_id,
                "observed_at": observation.observed_at_iso,
                "observed_at_ms": observation.observed_at_ms,
                "trading_session": observation.trading_session,
                "cycle_no": observation.cycle_no,
                "rank": observation.rank,
                "symbol": observation.symbol,
                "sample_type": observation.sample_type,
                "bar_interval": observation.bar_interval,
                "trigger_types": observation.trigger_types,
                "trigger_score": observation.trigger_score,
                "sample": observation.sample,
                "enrichments": observation.enrichments,
                "research_contexts": observation.research_contexts,
                "context_3m": observation.research_contexts.get("3m"),
                "horizon": {
                    "horizon_sec": horizon.horizon_sec,
                    "deadline_ms": horizon.deadline_ms,
                    "close_return_pct": horizon.close_return_pct,
                    "after_fee_return_pct": horizon.after_fee_return_pct,
                    "max_runup_pct": horizon.max_runup_pct,
                    "max_drawdown_pct": horizon.max_drawdown_pct,
                    "finalized_at_ms": horizon.finalized_at_ms,
                    "finalized_at": horizon.finalized_at_iso,
                    "close_price": horizon.close_price,
                    "candle_count": horizon.candle_count,
                },
            },
        )
        emit_event(
            "signal_lab_event_horizon_labeled",
            event_id=observation.event_id,
            symbol=observation.symbol,
            sample_type=observation.sample_type,
            bar_interval=observation.bar_interval,
            trading_session=observation.trading_session,
            trigger_types=observation.trigger_types,
            horizon_sec=horizon.horizon_sec,
            close_return_pct=horizon.close_return_pct,
            after_fee_return_pct=horizon.after_fee_return_pct,
            max_drawdown_pct=horizon.max_drawdown_pct,
        )
        return True

    async def _finalize_observation(self, observation: EventObservation) -> None:
        async with self._state_lock:
            if observation.event_id not in self.active:
                return
            self.labeled_count += 1
            horizons_payload = [
                {
                    "horizon_sec": horizon.horizon_sec,
                    "deadline_ms": horizon.deadline_ms,
                    "close_return_pct": horizon.close_return_pct,
                    "after_fee_return_pct": horizon.after_fee_return_pct,
                    "max_runup_pct": horizon.max_runup_pct,
                    "max_drawdown_pct": horizon.max_drawdown_pct,
                    "finalized_at_ms": horizon.finalized_at_ms,
                    "finalized_at": horizon.finalized_at_iso,
                    "close_price": horizon.close_price,
                    "candle_count": horizon.candle_count,
                }
                for _, horizon in sorted(observation.horizons.items())
            ]
            self._append_jsonl(
                self.labels_file,
                {
                    "event": "market_event_labeled",
                    "event_id": observation.event_id,
                    "observed_at": observation.observed_at_iso,
                    "observed_at_ms": observation.observed_at_ms,
                    "trading_session": observation.trading_session,
                    "cycle_no": observation.cycle_no,
                    "rank": observation.rank,
                    "symbol": observation.symbol,
                    "sample_type": observation.sample_type,
                    "bar_interval": observation.bar_interval,
                    "trigger_types": observation.trigger_types,
                    "trigger_score": observation.trigger_score,
                    "sample": observation.sample,
                    "enrichments": observation.enrichments,
                    "research_contexts": observation.research_contexts,
                    "context_3m": observation.research_contexts.get("3m"),
                    "horizons": horizons_payload,
                },
            )
            emit_event(
                "signal_lab_event_labeled",
                event_id=observation.event_id,
                symbol=observation.symbol,
                sample_type=observation.sample_type,
                bar_interval=observation.bar_interval,
                trading_session=observation.trading_session,
                trigger_types=observation.trigger_types,
                horizons=[
                    {
                        "horizon_sec": item["horizon_sec"],
                        "close_return_pct": item["close_return_pct"],
                        "after_fee_return_pct": item["after_fee_return_pct"],
                        "max_drawdown_pct": item["max_drawdown_pct"],
                    }
                    for item in horizons_payload
                ],
            )
            self.active.pop(observation.event_id, None)
            symbol_bucket = self.active_by_symbol.get(observation.symbol)
            if symbol_bucket is not None:
                symbol_bucket.discard(observation.event_id)
                if not symbol_bucket:
                    self.active_by_symbol.pop(observation.symbol, None)

    async def _release_observation(
        self,
        observation: EventObservation,
        *,
        status: str,
        horizon_sec: int | None = None,
        error: str | None = None,
    ) -> None:
        async with self._state_lock:
            if observation.event_id not in self.active:
                return
            self.active.pop(observation.event_id, None)
            symbol_bucket = self.active_by_symbol.get(observation.symbol)
            if symbol_bucket is not None:
                symbol_bucket.discard(observation.event_id)
                if not symbol_bucket:
                    self.active_by_symbol.pop(observation.symbol, None)
        emit_event(
            "signal_lab_event_released",
            event_id=observation.event_id,
            symbol=observation.symbol,
            sample_type=observation.sample_type,
            bar_interval=observation.bar_interval,
            status=status,
            horizon_sec=horizon_sec,
            error=error,
        )

    async def wait_for_active(self, *, timeout_sec: float | None = None) -> None:
        pending = [task for task in self.label_tasks if not task.done()]
        if not pending:
            return
        await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=timeout_sec)

    def summary(self) -> dict[str, Any]:
        return {
            "created": self.created_count,
            "labeled": self.labeled_count,
            "active": len(self.active),
            "label_tasks": len([task for task in self.label_tasks if not task.done()]),
        }

    @staticmethod
    def _append_jsonl(path: Path, payload: dict[str, Any], *, compact: bool = False) -> None:
        with path.open("a", encoding="utf-8") as handle:
            if compact:
                handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
            else:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def render_diagnostic_report(
    *,
    setup: str,
    scanned_count: int,
    eligible_count: int,
    live_matches: list[SetupDiagnosticResult],
    near_misses: list[SetupDiagnosticResult],
    limit: int,
) -> str:
    lines = [
        f"setup={setup} scanned={scanned_count} eligible={eligible_count} "
        f"live_matches={len(live_matches)} near_misses_shown={min(limit, len(near_misses))}/{len(near_misses)}"
    ]
    if live_matches:
        lines.append(
            "live_matches: "
            + ", ".join(f"{item.symbol}({item.live_score:.1f})" for item in live_matches[: min(5, len(live_matches))])
        )
    if not near_misses:
        lines.append("no near-miss candidates in the current scan.")
        return "\n".join(lines)
    for index, item in enumerate(near_misses[:limit], start=1):
        lines.append(
            f"{index}. {item.symbol} price={item.price:.8g} rules={item.passed_rules}/{item.total_rules} "
            f"gap={item.normalized_gap:.4f} vol24h={item.quote_volume_24h:.0f} "
            f"turnover1m={item.avg_quote_turnover_1m:.0f}"
        )
        for rule in item.rule_results:
            status = "PASS" if rule.passed else "FAIL"
            lines.append(f"   {status} {rule.clue}")
    return "\n".join(lines)


def emit_event(event: str, **payload: Any) -> None:
    print(json.dumps({"ts": utc_now_iso(), "event": event, **payload}, ensure_ascii=False), flush=True)


def enrichment_route_for_sample(sample: MarketEventSample) -> str:
    return "baseline_light" if sample.sample_type in LIGHTWEIGHT_ENRICHMENT_SAMPLE_TYPES else "full"


def select_cycle_baseline_samples(
    *,
    eligible_contexts: list[MarketEventContext],
    cycle_no: int,
    requested_scan_offset: int,
    triggered_keys: set[tuple[str, str]],
    ranked_keys: set[tuple[str, str]],
    requested_baseline_count: int,
) -> list[MarketEventSample]:
    if requested_baseline_count <= 0 or not eligible_contexts:
        return []
    blocked_symbols = {symbol for symbol, _ in triggered_keys.union(ranked_keys)}
    baseline_pool = [item for item in eligible_contexts if item.symbol not in blocked_symbols]
    if not baseline_pool:
        baseline_pool = [
            item
            for item in eligible_contexts
            if (item.symbol, item.bar_interval) not in triggered_keys
            and (item.symbol, item.bar_interval) not in ranked_keys
        ]
    if not baseline_pool:
        return []
    ordered_pool = sorted(
        baseline_pool,
        key=lambda item: (item.bar_interval, item.symbol, item.anchor_close_time_ms),
    )
    preferred_interval = "1m" if max(1, int(cycle_no)) % 2 == 1 else "5m"
    interval_order = (preferred_interval, "5m" if preferred_interval == "1m" else "1m")
    selected_pool = ordered_pool
    for interval in interval_order:
        bucket = [item for item in ordered_pool if item.bar_interval == interval]
        if bucket:
            selected_pool = bucket
            break
    # Keep the control arm hard-capped to a single lightweight sample per cycle.
    sample_index = (max(0, int(requested_scan_offset)) + max(0, int(cycle_no)) - 1) % len(selected_pool)
    return [build_baseline_market_event(selected_pool[sample_index])]


def combine_cycle_event_samples(
    *,
    priority_samples: list[MarketEventSample],
    baseline_samples: list[MarketEventSample],
    max_events_per_cycle: int,
) -> list[MarketEventSample]:
    capped_max = max(1, int(max_events_per_cycle or 1))
    ordered_priority = sorted(priority_samples, key=lambda item: item.trigger_score, reverse=True)
    reserved_baseline = min(len(baseline_samples), capped_max)
    priority_budget = max(0, capped_max - reserved_baseline)
    return ordered_priority[:priority_budget] + list(baseline_samples[:reserved_baseline])


def build_scan_shortlist(
    universe: list[dict[str, Any]],
    *,
    scan_top: int,
    scan_offset: int,
) -> tuple[list[dict[str, Any]], int]:
    if not universe:
        return [], 0
    width = max(1, min(int(scan_top), len(universe)))
    offset = max(0, int(scan_offset)) % len(universe)
    end = offset + width
    if end <= len(universe):
        return universe[offset:end], offset
    overflow = end - len(universe)
    return universe[offset:] + universe[:overflow], offset


async def load_cached_klines(
    futures: BinanceFuturesClient,
    *,
    symbol: str,
    interval: str,
    limit: int,
    market_cache: dict[str, Any],
    ttl_sec: float,
) -> list[list[Any]]:
    ttl_ms = max(0, int(float(ttl_sec) * 1000))
    cache_key = f"kline::{symbol}::{interval}::{limit}"
    current_ms = now_ms()
    cached_payload = market_cache.get(cache_key)
    expires_at_ms = int(market_cache.get(f"{cache_key}::expires_at_ms") or 0)
    if ttl_ms > 0 and cached_payload is not None and current_ms < expires_at_ms:
        return cached_payload
    payload = await futures.klines(symbol, interval=interval, limit=limit)
    if ttl_ms > 0:
        market_cache[cache_key] = payload
        market_cache[f"{cache_key}::expires_at_ms"] = current_ms + ttl_ms
    return payload


async def load_cached_public_payload(
    *,
    market_cache: dict[str, Any],
    cache_key: str,
    ttl_sec: float,
    loader: Callable[[], Any],
) -> Any:
    ttl_ms = max(0, int(float(ttl_sec) * 1000))
    current_ms = now_ms()
    cached_payload = market_cache.get(cache_key)
    expires_at_ms = int(market_cache.get(f"{cache_key}::expires_at_ms") or 0)
    if ttl_ms > 0 and cached_payload is not None and current_ms < expires_at_ms:
        return cached_payload
    payload = await loader()
    if ttl_ms > 0:
        market_cache[cache_key] = payload
        market_cache[f"{cache_key}::expires_at_ms"] = current_ms + ttl_ms
    return payload


async def load_symbol_klines(
    futures: BinanceFuturesClient,
    *,
    symbol: str,
    market_cache: dict[str, Any],
    ttl_1m_sec: float,
    ttl_5m_sec: float,
    ttl_15m_sec: float,
) -> tuple[list[list[Any]], list[list[Any]], list[list[Any]]]:
    async def one(interval: str, ttl_sec: float) -> list[list[Any]]:
        return await load_cached_klines(
            futures,
            symbol=symbol,
            interval=interval,
            limit=DEFAULT_KLINE_LIMIT,
            market_cache=market_cache,
            ttl_sec=ttl_sec,
        )

    return await asyncio.gather(
        one("1m", ttl_1m_sec),
        one("5m", ttl_5m_sec),
        one("15m", ttl_15m_sec),
    )


async def rank_candidates(
    futures: BinanceFuturesClient,
    *,
    universe_top: int,
    scan_top: int,
    scan_offset: int,
    min_quote_volume: float,
    min_score: float,
    allow_short: bool,
    execution_floor_offset: float,
    market_cache: dict[str, Any],
    sampling_mode: str,
    include_setups: set[str] | None,
    universe_sort: str,
    universe_cache_sec: float,
    kline_cache_ttl_1m_sec: float,
    kline_cache_ttl_5m_sec: float,
    kline_cache_ttl_15m_sec: float,
    feature_min_quote_volume: float,
    kline_concurrency: int,
    symbol_admission_check: Callable[[str], bool] | None = None,
) -> RankResult:
    observed_at_ms = now_ms()
    cache_key = f"universe::{universe_sort}::{min_quote_volume:.0f}::{universe_top}"
    cached_universe = market_cache.get(cache_key)
    cached_expires_at_ms = int(market_cache.get(f"{cache_key}::expires_at_ms") or 0)
    universe_cache_ms = max(0, int(float(universe_cache_sec) * 1000))
    if cached_universe is not None and observed_at_ms < cached_expires_at_ms:
        universe = cached_universe
    else:
        universe = discover_universe(
            await futures.ticker_24hr(),
            top_limit=universe_top,
            min_quote_volume=min_quote_volume,
            sort_by=universe_sort,
        )
        market_cache[cache_key] = universe
        market_cache[f"{cache_key}::expires_at_ms"] = observed_at_ms + universe_cache_ms
    shortlist, effective_scan_offset = build_scan_shortlist(
        universe,
        scan_top=scan_top,
        scan_offset=scan_offset,
    )
    if symbol_admission_check is not None:
        shortlist = [item for item in shortlist if symbol_admission_check(str(item.get("symbol") or ""))]
    semaphore = asyncio.Semaphore(max(1, int(kline_concurrency)))

    strict_candidates = 0
    broad_candidates = 0

    async def one(item: dict[str, Any]) -> tuple[Candidate | None, bool, bool]:
        try:
            async with semaphore:
                candles_1m, candles_5m, candles_15m = await load_symbol_klines(
                    futures,
                    symbol=item["symbol"],
                    market_cache=market_cache,
                    ttl_1m_sec=kline_cache_ttl_1m_sec,
                    ttl_5m_sec=kline_cache_ttl_5m_sec,
                    ttl_15m_sec=kline_cache_ttl_15m_sec,
                )
        except Exception:
            return None, False, False
        strict_candidate = score_candidate(
            item,
            candles_1m,
            candles_5m,
            candles_15m,
            allow_short=allow_short,
        )
        strict_selected = False
        if strict_candidate is not None and not setup_allowed(strict_candidate.setup, include_setups=include_setups):
            strict_candidate = None
        if strict_candidate is not None:
            execution_floor = max(
                min_score,
                max(24.0, setup_execution_floor(strict_candidate.setup) + execution_floor_offset),
            )
            if strict_candidate.score >= execution_floor:
                strict_selected = True
        features = compute_market_features(
            item,
            candles_1m,
            candles_5m,
            candles_15m,
            feature_min_quote_volume=feature_min_quote_volume,
        )
        broad_candidate = (
            score_broad_candidate(features, allow_short=allow_short, include_setups=include_setups)
            if features is not None
            else None
        )

        if sampling_mode == "strict":
            return (strict_candidate if strict_selected else None), strict_selected, broad_candidate is not None
        if sampling_mode == "broad":
            return broad_candidate, strict_selected, broad_candidate is not None
        if strict_selected:
            return strict_candidate, True, broad_candidate is not None
        return broad_candidate, False, broad_candidate is not None

    candidates: list[Candidate] = []
    for candidate, had_strict, had_broad in await asyncio.gather(*(one(item) for item in shortlist)):
        if had_strict:
            strict_candidates += 1
        if had_broad:
            broad_candidates += 1
        if candidate is not None:
            candidates.append(candidate)
    candidates.sort(
        key=lambda item: (
            item.score,
            setup_priority(item.setup),
            item.avg_quote_turnover_1m,
            item.quote_volume_24h,
        ),
        reverse=True,
    )
    return RankResult(
        candidates=candidates,
        strict_candidates=strict_candidates,
        broad_candidates=broad_candidates,
        effective_scan_offset=effective_scan_offset,
    )


async def mark_price_stream_loop(
    *,
    session: aiohttp.ClientSession,
    futures: BinanceFuturesClient,
    latest_mark_prices: dict[str, MarketPriceSample],
    store: ObservationStore,
    shutdown: asyncio.Event,
) -> None:
    url = f"{futures.environment.futures_ws_base.rstrip('/')}/ws/{MARK_PRICE_STREAM_NAME}"
    proxy = futures.proxy_settings.proxy_for_url(url)
    while not shutdown.is_set():
        try:
            async with session.ws_connect(
                url,
                heartbeat=30,
                receive_timeout=DEFAULT_MARK_PRICE_WS_TIMEOUT_SEC,
                proxy=proxy,
            ) as websocket:
                emit_event("signal_lab_mark_stream_connected", url=url)
                async for message in websocket:
                    if shutdown.is_set():
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        payload = json.loads(message.data)
                        if not isinstance(payload, list):
                            continue
                        for item in payload:
                            if not isinstance(item, dict):
                                continue
                            symbol = str(item.get("s") or "").upper()
                            mark_price = safe_float(item.get("p") or item.get("markPrice"))
                            observed_at_ms = int(item.get("E") or item.get("eventTime") or now_ms())
                            if not symbol or mark_price <= 0:
                                continue
                            latest_mark_prices[symbol] = MarketPriceSample(
                                price=mark_price,
                                observed_at_ms=observed_at_ms,
                            )
                            store.update_mark_price(symbol, mark_price, observed_at_ms)
                    elif message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING}:
                        break
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError("Binance mark price websocket reported an error frame.")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            emit_event("signal_lab_mark_stream_error", error=str(exc))
            await asyncio.sleep(DEFAULT_MARK_PRICE_WS_BACKOFF_SEC)


async def derivatives_mark_price_stream_loop(
    *,
    session: aiohttp.ClientSession,
    futures: BinanceFuturesClient,
    derivatives_cache: DerivativeDataCache,
    shutdown: asyncio.Event,
) -> None:
    urls = futures_ws_stream_urls(futures, MARK_PRICE_STREAM_NAME)
    url_index = 0
    while not shutdown.is_set():
        url = urls[url_index % len(urls)]
        proxy = futures.proxy_settings.proxy_for_url(url)
        try:
            async with session.ws_connect(
                url,
                heartbeat=30,
                receive_timeout=DEFAULT_MARK_PRICE_WS_TIMEOUT_SEC,
                proxy=proxy,
            ) as websocket:
                emit_event("signal_lab_derivatives_mark_stream_connected", url=url)
                while not shutdown.is_set():
                    message = await asyncio.wait_for(
                        websocket.receive(),
                        timeout=DEFAULT_MARK_PRICE_WS_TIMEOUT_SEC,
                    )
                    if shutdown.is_set():
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        payload = json.loads(message.data)
                        items = payload if isinstance(payload, list) else [payload]
                        for item in items:
                            if isinstance(item, dict):
                                derivatives_cache.update_mark_payload(item)
                    elif message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING}:
                        break
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError("Binance derivatives mark websocket reported an error frame.")
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            next_url = urls[(url_index + 1) % len(urls)] if len(urls) > 1 else url
            emit_event(
                "signal_lab_derivatives_mark_stream_fallback",
                url=url,
                next_url=next_url,
                reason="receive_timeout",
            )
            if len(urls) > 1:
                url_index += 1
            await asyncio.sleep(DEFAULT_DERIVATIVES_WS_BACKOFF_SEC)
        except Exception as exc:
            emit_event("signal_lab_derivatives_mark_stream_error", url=url, error=str(exc))
            if len(urls) > 1:
                url_index += 1
            await asyncio.sleep(DEFAULT_DERIVATIVES_WS_BACKOFF_SEC)


async def liquidation_stream_loop(
    *,
    session: aiohttp.ClientSession,
    futures: BinanceFuturesClient,
    derivatives_cache: DerivativeDataCache,
    shutdown: asyncio.Event,
) -> None:
    urls = futures_ws_stream_urls(futures, LIQUIDATION_STREAM_NAME)
    url_index = 0
    while not shutdown.is_set():
        url = urls[url_index % len(urls)]
        proxy = futures.proxy_settings.proxy_for_url(url)
        try:
            async with session.ws_connect(
                url,
                heartbeat=30,
                receive_timeout=DEFAULT_LIQUIDATION_WS_TIMEOUT_SEC,
                proxy=proxy,
            ) as websocket:
                emit_event("signal_lab_liquidation_stream_connected", url=url)
                while not shutdown.is_set():
                    message = await asyncio.wait_for(
                        websocket.receive(),
                        timeout=DEFAULT_LIQUIDATION_WS_TIMEOUT_SEC,
                    )
                    if shutdown.is_set():
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        payload = json.loads(message.data)
                        items = payload if isinstance(payload, list) else [payload]
                        for item in items:
                            if not isinstance(item, dict):
                                continue
                            summary = derivatives_cache.update_liquidation_payload(item)
                            if summary is None:
                                continue
                            emit_event(
                                "signal_lab_liquidation_snapshot",
                                symbol=summary["symbol"],
                                side=summary["side"],
                                notional_usd=round4(summary["notional_usd"]),
                                liquidation_long_usd_15m=round4(summary["liquidation_long_usd_15m"] or 0.0),
                                liquidation_short_usd_15m=round4(summary["liquidation_short_usd_15m"] or 0.0),
                                liquidation_event_count_15m=summary["liquidation_event_count_15m"],
                            )
                    elif message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING}:
                        break
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError("Binance liquidation websocket reported an error frame.")
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            next_url = urls[(url_index + 1) % len(urls)] if len(urls) > 1 else url
            emit_event(
                "signal_lab_liquidation_stream_fallback",
                url=url,
                next_url=next_url,
                reason="receive_timeout",
            )
            if len(urls) > 1:
                url_index += 1
            await asyncio.sleep(DEFAULT_DERIVATIVES_WS_BACKOFF_SEC)
        except Exception as exc:
            emit_event("signal_lab_liquidation_stream_error", url=url, error=str(exc))
            if len(urls) > 1:
                url_index += 1
            await asyncio.sleep(DEFAULT_DERIVATIVES_WS_BACKOFF_SEC)


async def finalize_loop(
    *,
    futures: BinanceFuturesClient,
    latest_mark_prices: dict[str, MarketPriceSample],
    store: ObservationStore,
    shutdown: asyncio.Event,
    interval_sec: float = DEFAULT_FINALIZER_INTERVAL_SEC,
) -> None:
    while not shutdown.is_set():
        try:
            await store.finalize_due(
                futures=futures,
                latest_mark_prices=latest_mark_prices,
                force_all=False,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            emit_event(
                "signal_lab_finalize_error",
                error=str(exc),
                active_observations=store.summary()["active"],
            )
        await asyncio.sleep(max(0.25, float(interval_sec)))


async def scan_market_events(
    futures: BinanceFuturesClient,
    *,
    universe_top: int,
    scan_top: int,
    scan_offset: int,
    min_quote_volume: float,
    market_cache: dict[str, Any],
    universe_sort: str,
    universe_cache_sec: float,
    kline_cache_ttl_1m_sec: float,
    kline_cache_ttl_5m_sec: float,
    kline_concurrency: int,
    trigger_config: EventTriggerConfig,
    kline_cache_ttl_3m_sec: float | None = None,
) -> EventScanResult:
    observed_at_ms = now_ms()
    cache_key = f"event-universe::{universe_sort}::{min_quote_volume:.0f}::{universe_top}"
    cached_universe = market_cache.get(cache_key)
    cached_expires_at_ms = int(market_cache.get(f"{cache_key}::expires_at_ms") or 0)
    universe_cache_ms = max(0, int(float(universe_cache_sec) * 1000))
    if cached_universe is not None and observed_at_ms < cached_expires_at_ms:
        universe = cached_universe
    else:
        universe = discover_universe(
            await futures.ticker_24hr(),
            top_limit=universe_top,
            min_quote_volume=min_quote_volume,
            sort_by=universe_sort,
        )
        market_cache[cache_key] = universe
        market_cache[f"{cache_key}::expires_at_ms"] = observed_at_ms + universe_cache_ms
    shortlist, effective_scan_offset = build_scan_shortlist(
        universe,
        scan_top=scan_top,
        scan_offset=scan_offset,
    )
    shortlist = [item for item in shortlist if is_tradeable_symbol(str(item.get("symbol") or ""))]
    semaphore = asyncio.Semaphore(max(1, int(kline_concurrency)))

    async def one(
        item: dict[str, Any],
    ) -> tuple[list[MarketEventSample], list[MarketEventContext], dict[str, dict[str, Any]]]:
        symbol = str(item["symbol"])
        try:
            async with semaphore:
                kline_results = await asyncio.gather(
                    load_cached_klines(
                        futures,
                        symbol=symbol,
                        interval="1m",
                        limit=DEFAULT_KLINE_LIMIT,
                        market_cache=market_cache,
                        ttl_sec=kline_cache_ttl_1m_sec,
                    ),
                    load_cached_klines(
                        futures,
                        symbol=symbol,
                        interval="5m",
                        limit=DEFAULT_KLINE_LIMIT,
                        market_cache=market_cache,
                        ttl_sec=kline_cache_ttl_5m_sec,
                    ),
                    load_cached_klines(
                        futures,
                        symbol=symbol,
                        interval="3m",
                        limit=DEFAULT_KLINE_LIMIT,
                        market_cache=market_cache,
                        ttl_sec=(
                            kline_cache_ttl_5m_sec
                            if kline_cache_ttl_3m_sec is None
                            else kline_cache_ttl_3m_sec
                        ),
                    ),
                    return_exceptions=True,
                )
        except Exception:
            return [], [], {}
        if isinstance(kline_results[0], BaseException) or isinstance(kline_results[1], BaseException):
            return [], [], {}
        candles_1m = kline_results[0]
        candles_5m = kline_results[1]
        candles_3m = [] if isinstance(kline_results[2], BaseException) else kline_results[2]
        contexts: list[MarketEventContext] = []
        triggered: list[MarketEventSample] = []
        research_contexts_by_symbol: dict[str, dict[str, Any]] = {}
        context_1m = build_market_event_context(
            item,
            candles_1m,
            config=trigger_config,
            bar_interval="1m",
        )
        if context_1m is not None:
            contexts.append(context_1m)
            sample_1m = build_triggered_market_event(context_1m, config=trigger_config)
            if sample_1m is not None:
                triggered.append(sample_1m)
            confirmed_reversal_1m = build_confirmed_reversal_market_event(
                item,
                candles_1m,
                config=trigger_config,
            )
            if confirmed_reversal_1m is not None:
                triggered.append(confirmed_reversal_1m)
        context_3m = build_market_event_context(
            item,
            candles_3m,
            config=trigger_config,
            bar_interval="3m",
        )
        if context_3m is not None:
            research_contexts_by_symbol[symbol] = {
                "3m": build_market_event_research_context(context_3m, config=trigger_config),
            }
        context_5m = build_market_event_context(
            item,
            candles_5m,
            config=trigger_config,
            bar_interval="5m",
        )
        if context_5m is not None:
            contexts.append(context_5m)
            sample_5m = build_triggered_market_event(context_5m, config=trigger_config)
            if sample_5m is not None:
                triggered.append(sample_5m)
        return triggered, contexts, research_contexts_by_symbol

    triggered_samples: list[MarketEventSample] = []
    eligible_contexts: list[MarketEventContext] = []
    research_contexts_by_symbol: dict[str, dict[str, Any]] = {}
    for sample_rows, context_rows, research_context_rows in await asyncio.gather(*(one(item) for item in shortlist)):
        triggered_samples.extend(sample_rows)
        eligible_contexts.extend(context_rows)
        research_contexts_by_symbol.update(research_context_rows)
    triggered_samples.sort(key=lambda item: item.trigger_score, reverse=True)
    return EventScanResult(
        triggered_samples=triggered_samples,
        eligible_contexts=eligible_contexts,
        effective_scan_offset=effective_scan_offset,
        shortlist_count=len(shortlist),
        research_contexts_by_symbol=research_contexts_by_symbol,
    )


async def load_macro_symbol_context(
    futures: BinanceFuturesClient,
    *,
    symbol: str,
    market_cache: dict[str, Any],
    ttl_sec: float,
) -> dict[str, Any]:
    candles = await load_cached_klines(
        futures,
        symbol=symbol,
        interval="1m",
        limit=DEFAULT_KLINE_LIMIT,
        market_cache=market_cache,
        ttl_sec=ttl_sec,
    )
    context = compute_macro_context(candles)
    if context is None:
        return {}
    return {
        f"{symbol.lower()}_ret_5m_pct": round4(context["ret_5m_pct"]),
        f"{symbol.lower()}_ret_60m_pct": round4(context["ret_60m_pct"]),
    }


def round_enrichment_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: (round4(value) if isinstance(value, float) and value is not None else value)
        for key, value in payload.items()
    }


def round_enrichment_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return round4(safe_float(value))


def optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_market_event_common_enrichments(
    *,
    premium_payload: dict[str, Any],
    btc_macro: dict[str, Any],
    eth_macro: dict[str, Any],
    derivatives_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    derivatives_snapshot = derivatives_snapshot if isinstance(derivatives_snapshot, dict) else {}
    ws_mark_price = safe_float(derivatives_snapshot.get("mark_price"))
    ws_index_price = safe_float(derivatives_snapshot.get("index_price"))
    rest_mark_price = safe_float(premium_payload.get("markPrice"))
    rest_index_price = safe_float(premium_payload.get("indexPrice"))
    mark_price = ws_mark_price if ws_mark_price > 0 else rest_mark_price
    index_price = ws_index_price if ws_index_price > 0 else rest_index_price
    basis_pct = 0.0
    if mark_price > 0 and index_price > 0:
        basis_pct = ((mark_price - index_price) / index_price) * 100.0
    funding_rate = (
        safe_float(derivatives_snapshot.get("funding_rate"))
        if derivatives_snapshot.get("funding_rate") not in (None, "")
        else safe_float(premium_payload.get("lastFundingRate"))
    )
    next_funding_time_ms = optional_int(derivatives_snapshot.get("next_funding_time_ms"))
    if next_funding_time_ms is None:
        next_funding_time_ms = optional_int(premium_payload.get("nextFundingTime"))
    data_source = str(derivatives_snapshot.get("derivatives_data_source") or "").strip() or (
        "rest" if premium_payload else "missing"
    )
    return {
        "derivatives_data_source": data_source,
        "derivatives_mark_price_ts_ms": derivatives_snapshot.get("derivatives_mark_price_ts_ms"),
        "derivatives_funding_rate_ts_ms": derivatives_snapshot.get("derivatives_funding_rate_ts_ms"),
        "derivatives_liquidation_ts_ms": derivatives_snapshot.get("derivatives_liquidation_ts_ms"),
        "derivatives_mark_price_age_ms": derivatives_snapshot.get("derivatives_mark_price_age_ms"),
        "derivatives_funding_rate_age_ms": derivatives_snapshot.get("derivatives_funding_rate_age_ms"),
        "derivatives_liquidation_age_ms": derivatives_snapshot.get("derivatives_liquidation_age_ms"),
        "mark_price": round4(mark_price) if mark_price > 0 else None,
        "index_price": round4(index_price) if index_price > 0 else None,
        "funding_rate": round4(funding_rate),
        "next_funding_time_ms": next_funding_time_ms,
        "mark_index_basis_pct": round4(basis_pct) if mark_price > 0 and index_price > 0 else None,
        "liquidation_long_usd_15m": round_enrichment_value(derivatives_snapshot.get("liquidation_long_usd_15m")),
        "liquidation_short_usd_15m": round_enrichment_value(derivatives_snapshot.get("liquidation_short_usd_15m")),
        "liquidation_event_count_15m": derivatives_snapshot.get("liquidation_event_count_15m"),
        "liquidation_long_usd": round_enrichment_value(derivatives_snapshot.get("liquidation_long_usd")),
        "liquidation_short_usd": round_enrichment_value(derivatives_snapshot.get("liquidation_short_usd")),
        "liquidation_event_count": derivatives_snapshot.get("liquidation_event_count"),
        **btc_macro,
        **eth_macro,
    }


def empty_enrichment_extensions() -> dict[str, Any]:
    return {
        "open_interest": None,
        "oi_change_5m_pct": None,
        "oi_change_15m_pct": None,
        "spread_bps": None,
        "bid_depth_notional_5": None,
        "ask_depth_notional_5": None,
        "depth_imbalance": None,
        "estimated_slippage_bps": None,
        "estimated_slippage_for_order_usdt": None,
    }


async def enrich_market_event_lightweight(
    futures: BinanceFuturesClient,
    *,
    sample: MarketEventSample,
    market_cache: dict[str, Any],
    macro_ttl_sec: float,
    derivatives_cache: DerivativeDataCache | None,
    public_cache_ttl_sec: float,
) -> dict[str, Any]:
    derivatives_snapshot = (
        derivatives_cache.snapshot(sample.symbol, observed_at_ms=now_ms())
        if derivatives_cache is not None
        else None
    )
    premium_payload, btc_macro, eth_macro = await asyncio.gather(
        load_cached_public_payload(
            market_cache=market_cache,
            cache_key=f"public::premium::{sample.symbol}",
            ttl_sec=public_cache_ttl_sec,
            loader=lambda: futures.mark_price(sample.symbol),
        ),
        load_macro_symbol_context(
            futures,
            symbol="BTCUSDT",
            market_cache=market_cache,
            ttl_sec=macro_ttl_sec,
        ),
        load_macro_symbol_context(
            futures,
            symbol="ETHUSDT",
            market_cache=market_cache,
            ttl_sec=macro_ttl_sec,
        ),
    )
    return {
        **build_market_event_common_enrichments(
            premium_payload=premium_payload if isinstance(premium_payload, dict) else {},
            btc_macro=btc_macro,
            eth_macro=eth_macro,
            derivatives_snapshot=derivatives_snapshot,
        ),
        **empty_enrichment_extensions(),
    }


async def enrich_market_event_full(
    futures: BinanceFuturesClient,
    *,
    sample: MarketEventSample,
    market_cache: dict[str, Any],
    macro_ttl_sec: float,
    depth_limit: int,
    depth_slippage_notional_usdt: float,
    derivatives_cache: DerivativeDataCache | None,
    public_cache_ttl_sec: float,
    oi_hist_cache_ttl_sec: float,
) -> dict[str, Any]:
    derivatives_snapshot = (
        derivatives_cache.snapshot(sample.symbol, observed_at_ms=now_ms())
        if derivatives_cache is not None
        else None
    )
    premium_payload, open_interest_payload, open_interest_hist_payload, depth_payload, btc_macro, eth_macro = await asyncio.gather(
        load_cached_public_payload(
            market_cache=market_cache,
            cache_key=f"public::premium::{sample.symbol}",
            ttl_sec=public_cache_ttl_sec,
            loader=lambda: futures.mark_price(sample.symbol),
        ),
        load_cached_public_payload(
            market_cache=market_cache,
            cache_key=f"public::oi::{sample.symbol}",
            ttl_sec=public_cache_ttl_sec,
            loader=lambda: futures.open_interest(sample.symbol),
        ),
        load_cached_public_payload(
            market_cache=market_cache,
            cache_key=f"public::oihist::{sample.symbol}",
            ttl_sec=oi_hist_cache_ttl_sec,
            loader=lambda: futures.open_interest_hist(sample.symbol, period="5m", limit=4),
        ),
        load_cached_public_payload(
            market_cache=market_cache,
            cache_key=f"public::depth::{sample.symbol}::{max(5, depth_limit)}",
            ttl_sec=DEFAULT_EVENT_DEPTH_CACHE_TTL_SEC,
            loader=lambda: futures.depth(sample.symbol, limit=max(5, depth_limit)),
        ),
        load_macro_symbol_context(
            futures,
            symbol="BTCUSDT",
            market_cache=market_cache,
            ttl_sec=macro_ttl_sec,
        ),
        load_macro_symbol_context(
            futures,
            symbol="ETHUSDT",
            market_cache=market_cache,
            ttl_sec=macro_ttl_sec,
        ),
    )
    premium_payload = premium_payload if isinstance(premium_payload, dict) else {}
    open_interest_payload = open_interest_payload if isinstance(open_interest_payload, dict) else {}
    common_enrichments = build_market_event_common_enrichments(
        premium_payload=premium_payload,
        btc_macro=btc_macro,
        eth_macro=eth_macro,
        derivatives_snapshot=derivatives_snapshot,
    )
    orderbook_metrics = compute_orderbook_metrics(
        depth_payload,
        reference_price=safe_float(common_enrichments.get("mark_price")) or sample.price,
        slippage_notional_usdt=max(0.0, float(depth_slippage_notional_usdt or 0.0)),
    )
    open_interest_context = compute_open_interest_context(
        safe_float(open_interest_payload.get("openInterest")),
        open_interest_hist_payload if isinstance(open_interest_hist_payload, list) else [],
    )
    return {
        **common_enrichments,
        **round_enrichment_payload(open_interest_context),
        **round_enrichment_payload(orderbook_metrics.to_payload()),
    }


async def run_enrich_sample(
    futures: BinanceFuturesClient,
    *,
    sample: MarketEventSample,
    market_cache: dict[str, Any],
    macro_ttl_sec: float,
    depth_limit: int,
    depth_slippage_notional_usdt: float,
    derivatives_cache: DerivativeDataCache | None,
    public_cache_ttl_sec: float,
    oi_hist_cache_ttl_sec: float,
    research_contexts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route = enrichment_route_for_sample(sample)
    if route == "baseline_light":
        enrichments = await enrich_market_event_lightweight(
            futures,
            sample=sample,
            market_cache=market_cache,
            macro_ttl_sec=macro_ttl_sec,
            derivatives_cache=derivatives_cache,
            public_cache_ttl_sec=public_cache_ttl_sec,
        )
    else:
        enrichments = await enrich_market_event_full(
            futures,
            sample=sample,
            market_cache=market_cache,
            macro_ttl_sec=macro_ttl_sec,
            depth_limit=depth_limit,
            depth_slippage_notional_usdt=depth_slippage_notional_usdt,
            derivatives_cache=derivatives_cache,
            public_cache_ttl_sec=public_cache_ttl_sec,
            oi_hist_cache_ttl_sec=oi_hist_cache_ttl_sec,
        )
    return attach_research_contexts(enrichments, research_contexts)


def unpack_event_queue_item(item: Any) -> tuple[int, int, MarketEventSample, dict[str, dict[str, Any]]]:
    if not isinstance(item, (tuple, list)) or len(item) not in {3, 4}:
        raise ValueError("Event queue item must be (cycle_no, rank, sample[, research_contexts]).")
    cycle_no, rank, sample = item[:3]
    if not isinstance(sample, MarketEventSample):
        raise ValueError("Event queue item sample must be a MarketEventSample.")
    research_contexts = normalize_research_contexts(item[3]) if len(item) == 4 else {}
    return int(cycle_no), int(rank), sample, research_contexts


async def event_enrichment_worker(
    *,
    worker_id: int,
    queue: asyncio.Queue[Any],
    futures: BinanceFuturesClient,
    market_cache: dict[str, Any],
    store: EventCollectionStore,
    derivatives_cache: DerivativeDataCache | None,
    macro_ttl_sec: float,
    depth_limit: int,
    depth_slippage_notional_usdt: float,
    public_cache_ttl_sec: float,
    oi_hist_cache_ttl_sec: float,
    max_retries: int,
    retry_backoff_sec: float,
) -> None:
    while True:
        item = await queue.get()
        sample: MarketEventSample | None = None
        enrichment_route = "unknown"
        try:
            if item is None:
                return
            cycle_no, rank, sample, research_contexts = unpack_event_queue_item(item)
            enrichment_route = enrichment_route_for_sample(sample)
            enrichments: dict[str, Any] | None = None
            for attempt in range(max(1, int(max_retries))):
                try:
                    enrichments = await run_enrich_sample(
                        futures,
                        sample=sample,
                        market_cache=market_cache,
                        macro_ttl_sec=macro_ttl_sec,
                        depth_limit=depth_limit,
                        depth_slippage_notional_usdt=depth_slippage_notional_usdt,
                        derivatives_cache=derivatives_cache,
                        public_cache_ttl_sec=public_cache_ttl_sec,
                        oi_hist_cache_ttl_sec=oi_hist_cache_ttl_sec,
                        research_contexts=research_contexts,
                    )
                    break
                except BinanceAPIError as exc:
                    is_rate_limit = exc.status == 429 or exc.code == -1003
                    if not is_rate_limit or attempt + 1 >= max(1, int(max_retries)):
                        raise
                    sleep_sec = max(1.0, float(retry_backoff_sec)) * float(attempt + 1)
                    emit_event(
                        "signal_lab_event_worker_backoff",
                        worker_id=worker_id,
                        symbol=sample.symbol,
                        route=enrichment_route,
                        attempt=attempt + 1,
                        sleep_sec=round4(sleep_sec),
                        status=exc.status,
                        code=exc.code,
                    )
                    await asyncio.sleep(sleep_sec)
            if enrichments is None:
                continue
            observation = store.create(
                cycle_no=int(cycle_no),
                rank=int(rank),
                sample=sample,
                enrichments=enrichments,
            )
            if observation is not None:
                store.schedule_labeling(observation, futures=futures)
        except Exception as exc:
            emit_event(
                "signal_lab_event_worker_error",
                worker_id=worker_id,
                symbol=sample.symbol if sample is not None else None,
                route=enrichment_route,
                error=str(exc),
            )
        finally:
            queue.task_done()


async def collect_main(args: argparse.Namespace) -> int:
    output_dir = resolve_output_dir(args.output_dir)
    timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)
    shutdown = asyncio.Event()
    environment = resolve_environment(args.env)
    proxy_settings = load_proxy_settings()
    allowed_event_patterns = parse_allowed_event_patterns(args.allowed_event_patterns)
    market_cache: dict[str, Any] = {}
    derivatives_cache = DerivativeDataCache()
    event_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(1, int(args.queue_size or 1)))
    trigger_config = EventTriggerConfig(
        atr_period=max(2, int(args.atr_period or 20)),
        atr_multiplier=max(0.1, float(args.atr_multiplier or 2.0)),
        volume_lookback=max(2, int(args.volume_lookback or 20)),
        volume_multiplier=max(0.1, float(args.volume_multiplier or 3.0)),
        atr_multiplier_1m=max(0.1, float(args.atr_multiplier_1m or 1.5)),
        volume_multiplier_1m=max(0.1, float(args.volume_multiplier_1m or 2.0)),
        atr_multiplier_5m=max(0.1, float(args.atr_multiplier_5m or 1.5)),
        volume_multiplier_5m=max(0.1, float(args.volume_multiplier_5m or 2.25)),
        min_price=max(0.0, float(args.trigger_min_price or 0.0)),
        min_quote_volume_24h=max(0.0, float(args.trigger_min_quote_volume or 0.0)),
        min_avg_quote_turnover_1m=max(0.0, float(args.trigger_min_avg_quote_turnover or 0.0)),
        min_current_quote_turnover_1m=max(0.0, float(args.trigger_min_current_quote_turnover or 0.0)),
    )
    store = EventCollectionStore(
        output_dir=output_dir,
        horizons_sec=parse_horizons(args.horizons_sec),
        dedupe_sec=max(0, args.dedupe_sec),
        symbol_cooldown_sec=max(0, args.symbol_cooldown_sec),
        max_active_per_symbol=max(0, args.max_active_per_symbol),
        round_trip_fee_bps=max(0.0, float(args.round_trip_fee_bps or 0.0)),
    )

    def request_stop(*_: object) -> None:
        shutdown.set()

    previous_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, request_stop)
    previous_sigterm = None
    if hasattr(signal, "SIGTERM"):
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, request_stop)

    stream_timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=None)
    async with aiohttp.ClientSession(timeout=timeout) as session, aiohttp.ClientSession(timeout=stream_timeout) as stream_session:
        futures = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=None,
            proxy_settings=proxy_settings,
        )
        stream_futures = BinanceFuturesClient(
            session=stream_session,
            environment=environment,
            credentials=None,
            proxy_settings=proxy_settings,
        )
        derivative_stream_tasks = [
            asyncio.create_task(
                derivatives_mark_price_stream_loop(
                    session=stream_session,
                    futures=stream_futures,
                    derivatives_cache=derivatives_cache,
                    shutdown=shutdown,
                ),
                name="signal-lab-derivatives-mark-stream",
            ),
            asyncio.create_task(
                liquidation_stream_loop(
                    session=stream_session,
                    futures=stream_futures,
                    derivatives_cache=derivatives_cache,
                    shutdown=shutdown,
                ),
                name="signal-lab-liquidation-stream",
            ),
        ]
        workers = [
            asyncio.create_task(
                event_enrichment_worker(
                    worker_id=index + 1,
                    queue=event_queue,
                    futures=futures,
                    market_cache=market_cache,
                    store=store,
                    derivatives_cache=derivatives_cache,
                    macro_ttl_sec=max(0.0, float(args.kline_cache_ttl_1m_sec or 0.0)),
                    depth_limit=max(5, int(args.depth_limit or DEFAULT_EVENT_DEPTH_LIMIT)),
                    depth_slippage_notional_usdt=max(0.0, float(args.depth_slippage_notional_usdt or 0.0)),
                    public_cache_ttl_sec=max(0.0, float(args.public_cache_ttl_sec or 0.0)),
                    oi_hist_cache_ttl_sec=max(0.0, float(args.oi_hist_cache_ttl_sec or 0.0)),
                    max_retries=DEFAULT_EVENT_WORKER_MAX_RETRIES,
                    retry_backoff_sec=DEFAULT_EVENT_RATE_LIMIT_BACKOFF_SEC,
                ),
                name=f"signal-lab-event-worker-{index+1}",
            )
            for index in range(max(1, int(args.worker_count or 1)))
        ]
        cycle_no = 0
        started_at = time.time()
        max_duration_sec = max(0, int(args.duration_sec or 0))
        try:
            while not shutdown.is_set():
                if max_duration_sec > 0 and time.time() - started_at >= max_duration_sec:
                    break
                cycle_no += 1
                started_cycle_at = time.time()
                requested_scan_offset = max(0, int(args.scan_offset or 0)) + (
                    max(0, int(args.scan_step or 0)) * max(0, cycle_no - 1)
                )
                try:
                    scan_result = await asyncio.wait_for(
                        scan_market_events(
                            futures,
                            universe_top=max(args.universe_top, args.scan_top),
                            scan_top=max(2, args.scan_top),
                            scan_offset=requested_scan_offset,
                            min_quote_volume=max(0.0, float(args.min_quote_volume or 0.0)),
                            market_cache=market_cache,
                            universe_sort=str(args.universe_sort),
                            universe_cache_sec=max(0.0, float(args.universe_cache_sec or 0.0)),
                            kline_cache_ttl_1m_sec=max(0.0, float(args.kline_cache_ttl_1m_sec or 0.0)),
                            kline_cache_ttl_3m_sec=max(0.0, float(args.kline_cache_ttl_3m_sec or 0.0)),
                            kline_cache_ttl_5m_sec=max(0.0, float(args.kline_cache_ttl_5m_sec or 0.0)),
                            kline_concurrency=max(1, int(args.kline_concurrency or 1)),
                            trigger_config=trigger_config,
                        ),
                        timeout=max(30.0, float(args.cycle_timeout_sec or DEFAULT_CYCLE_TIMEOUT_SEC)),
                    )
                except BinanceAPIError as exc:
                    is_rate_limit = exc.status == 429 or exc.code == -1003
                    if is_rate_limit:
                        sleep_sec = max(1.0, float(DEFAULT_EVENT_RATE_LIMIT_BACKOFF_SEC))
                        emit_event(
                            "signal_lab_collect_cycle_backoff",
                            cycle_no=cycle_no,
                            sleep_sec=round4(sleep_sec),
                            status=exc.status,
                            code=exc.code,
                            error=str(exc),
                        )
                        await asyncio.sleep(sleep_sec)
                        continue
                    emit_event(
                        "signal_lab_collect_cycle_error",
                        cycle_no=cycle_no,
                        error=str(exc),
                        status=exc.status,
                        code=exc.code,
                    )
                    return 1
                except asyncio.TimeoutError:
                    emit_event(
                        "signal_lab_collect_cycle_timeout",
                        cycle_no=cycle_no,
                        timeout_sec=max(30.0, float(args.cycle_timeout_sec or DEFAULT_CYCLE_TIMEOUT_SEC)),
                    )
                    await asyncio.sleep(1.0)
                    continue

                priority_samples = list(scan_result.triggered_samples)
                triggered_interval_counts = dict(
                    sorted(
                        (
                            interval,
                            len([item for item in scan_result.triggered_samples if item.bar_interval == interval]),
                        )
                        for interval in {item.bar_interval for item in scan_result.triggered_samples}
                    )
                )
                triggered_keys = {(item.symbol, item.bar_interval) for item in scan_result.triggered_samples}
                ranked_count = 0
                ranked_keys: set[tuple[str, str]] = set()
                ranked_per_cycle = max(0, int(args.ranked_samples_per_cycle or 0))
                ranked_min_trigger_ratio = max(0.0, float(args.ranked_min_trigger_ratio or 0.0))
                if ranked_per_cycle > 0 and scan_result.eligible_contexts:
                    ranked_candidates = [
                        ranked_sample
                        for item in scan_result.eligible_contexts
                        if (item.symbol, item.bar_interval) not in triggered_keys
                        for ranked_sample in [
                            build_ranked_market_event(
                                item,
                                config=trigger_config,
                                min_trigger_ratio=ranked_min_trigger_ratio,
                            )
                        ]
                        if ranked_sample is not None
                    ]
                    ranked_candidates.sort(key=lambda item: item.trigger_score, reverse=True)
                    ranked_samples = ranked_candidates[:ranked_per_cycle]
                    ranked_count = len(ranked_samples)
                    ranked_keys = {(item.symbol, item.bar_interval) for item in ranked_samples}
                    priority_samples.extend(ranked_samples)
                baseline_per_cycle = max(0, int(args.baseline_samples_per_cycle or 0))
                baseline_samples = select_cycle_baseline_samples(
                    eligible_contexts=scan_result.eligible_contexts,
                    cycle_no=cycle_no,
                    requested_scan_offset=requested_scan_offset,
                    triggered_keys=triggered_keys,
                    ranked_keys=ranked_keys,
                    requested_baseline_count=baseline_per_cycle,
                )

                filtered_out_count = 0
                if allowed_event_patterns is not None:
                    original_count = len(priority_samples) + len(baseline_samples)
                    priority_samples = [
                        item for item in priority_samples if sample_allowed(item, allowed_patterns=allowed_event_patterns)
                    ]
                    baseline_samples = [
                        item for item in baseline_samples if sample_allowed(item, allowed_patterns=allowed_event_patterns)
                    ]
                    filtered_out_count = max(
                        0,
                        original_count - len(priority_samples) - len(baseline_samples),
                    )
                max_events_per_cycle = max(1, int(args.max_events_per_cycle or 1))
                baseline_count = len(baseline_samples)
                samples = combine_cycle_event_samples(
                    priority_samples=priority_samples,
                    baseline_samples=baseline_samples,
                    max_events_per_cycle=max_events_per_cycle,
                )
                enqueued_interval_counts: dict[str, int] = defaultdict(int)
                enqueued_count = 0
                prefilter_skipped_count = 0
                planned_symbol_counts: dict[str, int] = defaultdict(int)
                planned_sample_keys: set[str] = set()
                planned_symbols_with_cooldown: set[str] = set()
                for rank, sample in enumerate(samples, start=1):
                    sample_key = (
                        f"{sample.symbol}|{sample.sample_type}|{sample.bar_interval}|"
                        f"{','.join(sample.trigger_types)}"
                    )
                    active_count = len(store.active_by_symbol.get(sample.symbol, set())) + planned_symbol_counts[sample.symbol]
                    if store.max_active_per_symbol > 0 and active_count >= store.max_active_per_symbol:
                        prefilter_skipped_count += 1
                        continue
                    if sample_key in planned_sample_keys:
                        prefilter_skipped_count += 1
                        continue
                    if store.symbol_cooldown_ms > 0 and sample.symbol in planned_symbols_with_cooldown:
                        prefilter_skipped_count += 1
                        continue
                    if not store.can_accept_sample(sample):
                        prefilter_skipped_count += 1
                        continue
                    await event_queue.put(
                        (
                            cycle_no,
                            rank,
                            sample,
                            scan_result.research_contexts_by_symbol.get(sample.symbol, {}),
                        )
                    )
                    planned_symbol_counts[sample.symbol] += 1
                    planned_sample_keys.add(sample_key)
                    if store.symbol_cooldown_ms > 0:
                        planned_symbols_with_cooldown.add(sample.symbol)
                    enqueued_interval_counts[sample.bar_interval] += 1
                    enqueued_count += 1

                emit_event(
                    "signal_lab_collect_cycle",
                    cycle_no=cycle_no,
                    requested_scan_offset=requested_scan_offset,
                    effective_scan_offset=scan_result.effective_scan_offset,
                    shortlist_count=scan_result.shortlist_count,
                    eligible_context_count=len(scan_result.eligible_contexts),
                    triggered_count=len(scan_result.triggered_samples),
                    triggered_interval_counts=triggered_interval_counts,
                    ranked_count=ranked_count,
                    baseline_count=baseline_count,
                    filtered_out_count=filtered_out_count,
                    enqueued_count=enqueued_count,
                    enqueued_interval_counts=dict(sorted(enqueued_interval_counts.items())),
                    prefilter_skipped_count=prefilter_skipped_count,
                    queue_size=event_queue.qsize(),
                    active_labels=store.summary()["active"],
                    elapsed_sec=round4(time.time() - started_cycle_at),
                )
                await asyncio.sleep(max(1.0, float(args.cycle_sec)))
        finally:
            shutdown.set()
            for task in derivative_stream_tasks:
                task.cancel()
            await asyncio.gather(*derivative_stream_tasks, return_exceptions=True)
            for _ in workers:
                await event_queue.put(None)
            await event_queue.join()
            await asyncio.gather(*workers, return_exceptions=True)
            drain_timeout_sec = (
                float(args.drain_timeout_sec)
                if args.drain_timeout_sec is not None
                else (max(parse_horizons(args.horizons_sec)) + 90.0)
            )
            try:
                await store.wait_for_active(timeout_sec=drain_timeout_sec)
            except asyncio.TimeoutError:
                emit_event(
                    "signal_lab_event_drain_timeout",
                    active_observations=store.summary()["active"],
                    timeout_sec=drain_timeout_sec,
                )
            emit_event(
                "signal_lab_collect_stopped",
                output_dir=str(output_dir),
                summary=store.summary(),
            )
            signal.signal(signal.SIGINT, previous_sigint)
            if previous_sigterm is not None and hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


async def diagnose_main(args: argparse.Namespace) -> int:
    selected_setups = parse_setup_filter(args.setups) or {"broad_hot_momentum_long"}
    unsupported = sorted(selected_setups.difference(BROAD_DIAGNOSTIC_RULES))
    if unsupported:
        raise ValueError(
            "Unsupported diagnose setup(s): "
            f"{', '.join(unsupported)}. Supported setups: {', '.join(diagnostic_supported_setups())}"
        )

    timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)
    environment = resolve_environment(args.env)
    proxy_settings = load_proxy_settings()
    async with aiohttp.ClientSession(timeout=timeout) as session:
        futures = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=None,
            proxy_settings=proxy_settings,
        )
        try:
            universe = discover_universe(
                await futures.ticker_24hr(),
                top_limit=max(args.universe_top, args.scan_top),
                min_quote_volume=max(1_000_000.0, args.min_quote_volume),
            )
        except BinanceAPIError as exc:
            emit_event(
                "signal_lab_diagnose_error",
                error=str(exc),
                status=exc.status,
                code=exc.code,
                hint=(
                    "Binance public data is blocked from this network. "
                    "Run through a region-appropriate proxy such as PHOENIX_ALL_PROXY."
                    if exc.status == 451
                    else None
                ),
            )
            return 1
        except Exception as exc:
            emit_event(
                "signal_lab_diagnose_error",
                error=str(exc),
                hint="Check Binance connectivity or configure PHOENIX_ALL_PROXY before running diagnose.",
            )
            return 1

        shortlist = universe[: max(1, args.scan_top)]
        semaphore = asyncio.Semaphore(3)

        async def one(item: dict[str, Any]) -> MarketFeatures | None:
            try:
                async with semaphore:
                    candles_1m, candles_5m, candles_15m = await asyncio.gather(
                        futures.klines(item["symbol"], interval="1m", limit=DEFAULT_KLINE_LIMIT),
                        futures.klines(item["symbol"], interval="5m", limit=DEFAULT_KLINE_LIMIT),
                        futures.klines(item["symbol"], interval="15m", limit=DEFAULT_KLINE_LIMIT),
                    )
            except Exception:
                return None
            return compute_market_features(
                item,
                candles_1m,
                candles_5m,
                candles_15m,
                feature_min_quote_volume=max(0.0, float(args.feature_min_quote_volume or 0.0)),
            )

        feature_rows = [features for features in await asyncio.gather(*(one(item) for item in shortlist)) if features is not None]

    sections: list[str] = []
    for setup in sorted(selected_setups):
        diagnostics = [evaluate_setup_diagnostic(features, setup=setup) for features in feature_rows]
        live_matches = sorted(
            [item for item in diagnostics if item.passed_rules == item.total_rules],
            key=lambda item: (
                item.live_score,
                item.avg_quote_turnover_1m,
                item.quote_volume_24h,
            ),
            reverse=True,
        )
        near_misses = sorted(
            [item for item in diagnostics if item.passed_rules < item.total_rules],
            key=lambda item: (
                -item.passed_rules,
                item.normalized_gap,
                -item.avg_quote_turnover_1m,
                -item.quote_volume_24h,
                item.symbol,
            ),
        )
        sections.append(
            render_diagnostic_report(
                setup=setup,
                scanned_count=len(shortlist),
                eligible_count=len(feature_rows),
                live_matches=live_matches,
                near_misses=near_misses,
                limit=max(1, args.limit),
            )
        )
    print("\n\n".join(sections))
    return 0


async def observe_main(args: argparse.Namespace) -> int:
    output_dir = resolve_output_dir(args.output_dir)
    snapshot_file = args.scout_snapshot_file or resolve_default_snapshot_file()
    shared_symbol_leases_file = args.shared_symbol_leases_file or (output_dir.parent / "_shared_symbol_leases.json")
    timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)
    shutdown = asyncio.Event()
    environment = resolve_environment(args.env)
    proxy_settings = load_proxy_settings()
    market_cache: dict[str, Any] = {}
    latest_mark_prices: dict[str, MarketPriceSample] = {}
    snapshot_index = ScoutSnapshotIndex(snapshot_file)
    include_setups = parse_setup_filter(args.include_setups)
    store = ObservationStore(
        output_dir=output_dir,
        horizons_sec=parse_horizons(args.horizons_sec),
        dedupe_sec=max(0, args.dedupe_sec),
        max_observations_per_symbol=max(0, args.max_observations_per_symbol),
        symbol_cooldown_sec=max(0, args.symbol_cooldown_sec),
        max_active_per_symbol=max(0, args.max_active_per_symbol),
        shared_symbol_leases=SharedSymbolLeases(shared_symbol_leases_file),
    )

    def request_stop(*_: object) -> None:
        shutdown.set()

    previous_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, request_stop)
    previous_sigterm = None
    if hasattr(signal, "SIGTERM"):
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, request_stop)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        futures = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=None,
            proxy_settings=proxy_settings,
        )
        stream_task = asyncio.create_task(
            mark_price_stream_loop(
                session=session,
                futures=futures,
                latest_mark_prices=latest_mark_prices,
                store=store,
                shutdown=shutdown,
            ),
            name="signal-lab-mark-stream",
        )
        finalizer_task = asyncio.create_task(
            finalize_loop(
                futures=futures,
                latest_mark_prices=latest_mark_prices,
                store=store,
                shutdown=shutdown,
            ),
            name="signal-lab-finalizer",
        )
        finalized_draining = False
        cycle_no = 0
        observed_started_at = time.time()
        max_duration_sec = max(0, int(args.duration_sec or 0))
        try:
            emit_event(
                "signal_lab_started",
                output_dir=str(output_dir),
                snapshot_file=str(snapshot_file) if snapshot_file is not None else None,
                horizons_sec=parse_horizons(args.horizons_sec),
                cycle_sec=args.cycle_sec,
                top_n=args.top_n,
                scan_top=args.scan_top,
                scan_offset=max(0, int(args.scan_offset or 0)),
                scan_step=max(0, int(args.scan_step or 0)),
                universe_sort=str(args.universe_sort),
                universe_cache_sec=max(0.0, float(args.universe_cache_sec or 0.0)),
                kline_cache_ttl_1m_sec=max(0.0, float(args.kline_cache_ttl_1m_sec or 0.0)),
                kline_cache_ttl_5m_sec=max(0.0, float(args.kline_cache_ttl_5m_sec or 0.0)),
                kline_cache_ttl_15m_sec=max(0.0, float(args.kline_cache_ttl_15m_sec or 0.0)),
                feature_min_quote_volume=max(0.0, float(args.feature_min_quote_volume or 0.0)),
                min_score=args.starting_min_score,
                execution_floor_offset=args.execution_floor_offset,
                sampling_mode=args.sampling_mode,
                symbol_cooldown_sec=max(0, args.symbol_cooldown_sec),
                max_active_per_symbol=max(0, args.max_active_per_symbol),
                kline_concurrency=max(1, int(args.kline_concurrency or 1)),
                shared_symbol_leases_file=str(shared_symbol_leases_file),
            )
            while not shutdown.is_set():
                if max_duration_sec > 0 and time.time() - observed_started_at >= max_duration_sec:
                    shutdown.set()
                    finalized_draining = True
                    break
                cycle_no += 1
                started_cycle_at = time.time()
                requested_scan_offset = max(0, int(args.scan_offset or 0)) + (
                    max(0, int(args.scan_step or 0)) * max(0, cycle_no - 1)
                )
                try:
                    rank_result = await asyncio.wait_for(
                        rank_candidates(
                            futures,
                            universe_top=max(args.universe_top, args.scan_top),
                            scan_top=max(2, args.scan_top),
                            scan_offset=requested_scan_offset,
                            min_quote_volume=max(1_000_000.0, args.min_quote_volume),
                            min_score=max(24.0, args.starting_min_score),
                            allow_short=bool(args.allow_short),
                            execution_floor_offset=float(args.execution_floor_offset or 0.0),
                            market_cache=market_cache,
                            sampling_mode=str(args.sampling_mode),
                            include_setups=include_setups,
                            universe_sort=str(args.universe_sort),
                            universe_cache_sec=max(0.0, float(args.universe_cache_sec or 0.0)),
                            kline_cache_ttl_1m_sec=max(0.0, float(args.kline_cache_ttl_1m_sec or 0.0)),
                            kline_cache_ttl_5m_sec=max(0.0, float(args.kline_cache_ttl_5m_sec or 0.0)),
                            kline_cache_ttl_15m_sec=max(0.0, float(args.kline_cache_ttl_15m_sec or 0.0)),
                            feature_min_quote_volume=max(0.0, float(args.feature_min_quote_volume or 0.0)),
                            kline_concurrency=max(1, int(args.kline_concurrency or 1)),
                            symbol_admission_check=store.can_accept_symbol,
                        ),
                        timeout=max(30.0, float(args.cycle_timeout_sec or DEFAULT_CYCLE_TIMEOUT_SEC)),
                    )
                except BinanceAPIError as exc:
                    emit_event(
                        "signal_lab_cycle_error",
                        cycle_no=cycle_no,
                        error=str(exc),
                        status=exc.status,
                        code=exc.code,
                        hint=(
                            "Binance public data is blocked from this network. "
                            "Run through a region-appropriate proxy such as PHOENIX_ALL_PROXY."
                            if exc.status == 451
                            else None
                        ),
                    )
                    return 1
                except asyncio.TimeoutError:
                    emit_event(
                        "signal_lab_cycle_timeout",
                        cycle_no=cycle_no,
                        timeout_sec=max(30.0, float(args.cycle_timeout_sec or DEFAULT_CYCLE_TIMEOUT_SEC)),
                    )
                    await asyncio.sleep(1.0)
                    continue
                candidates = rank_result.candidates
                scout_state = snapshot_index.current()
                created_count = 0
                top_view: list[dict[str, Any]] = []
                top_limit = max(1, args.top_n)
                for rank, candidate in enumerate(candidates, start=1):
                    if len(top_view) < top_limit:
                        top_view.append(
                            {
                                "rank": rank,
                                "symbol": candidate.symbol,
                                "setup": candidate.setup,
                                "side": candidate.side,
                                "score": round4(candidate.score),
                            }
                        )
                    if created_count >= top_limit:
                        break
                    price_sample = latest_mark_prices.get(candidate.symbol)
                    entry_mark_price = (
                        price_sample.price
                        if price_sample is not None and price_sample.price > 0
                        else candidate.mark_price
                    )
                    observation = store.create(
                        cycle_no=cycle_no,
                        rank=rank,
                        candidate=candidate,
                        entry_mark_price=entry_mark_price,
                        scout=scout_state.get(candidate.symbol),
                    )
                    if observation is not None:
                        created_count += 1
                await store.finalize_due(
                    futures=futures,
                    latest_mark_prices=latest_mark_prices,
                    force_all=False,
                )
                emit_event(
                    "signal_lab_cycle",
                    cycle_no=cycle_no,
                    requested_scan_offset=requested_scan_offset,
                    effective_scan_offset=rank_result.effective_scan_offset,
                    created_observations=created_count,
                    candidate_count=len(candidates),
                    strict_candidate_count=rank_result.strict_candidates,
                    broad_candidate_count=rank_result.broad_candidates,
                    top_candidates=top_view,
                    active_observations=store.summary()["active"],
                    elapsed_sec=round4(time.time() - started_cycle_at),
                )
                await asyncio.sleep(max(1.0, float(args.cycle_sec)))
            if finalized_draining:
                emit_event(
                    "signal_lab_drain_started",
                    active_observations=store.summary()["active"],
                )
                drain_deadline = time.time() + max(parse_horizons(args.horizons_sec)) + 10
                while store.has_active() and time.time() < drain_deadline:
                    await store.finalize_due(
                        futures=futures,
                        latest_mark_prices=latest_mark_prices,
                        force_all=False,
                    )
                    await asyncio.sleep(1.0)
        finally:
            with contextlib.suppress(Exception):
                await store.finalize_due(
                    futures=futures,
                    latest_mark_prices=latest_mark_prices,
                    force_all=True,
                )
            shutdown.set()
            finalizer_task.cancel()
            stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await finalizer_task
            with contextlib.suppress(asyncio.CancelledError):
                await stream_task
            signal.signal(signal.SIGINT, previous_sigint)
            if previous_sigterm is not None:
                signal.signal(signal.SIGTERM, previous_sigterm)
            emit_event("signal_lab_stopped", output_dir=str(output_dir), summary=store.summary())
    return 0


def load_labeled_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw_line = line.strip()
            if not raw_line:
                continue
            payload = json.loads(raw_line)
            if isinstance(payload, dict) and payload.get("event") == "observation_labeled":
                records.append(payload)
    return records


def load_event_labeled_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw_line = line.strip()
            if not raw_line:
                continue
            payload = json.loads(raw_line)
            if isinstance(payload, dict) and payload.get("event") in {
                "market_event_labeled",
                "market_event_horizon_labeled",
            }:
                records.append(payload)
    return records


def mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def scout_context_label(record: dict[str, Any], *, split_by: str) -> str:
    if split_by == "none":
        return "all"
    scout = record.get("scout")
    if not isinstance(scout, dict) or not scout:
        return "missing"

    def scout_float(key: str) -> float | None:
        value = scout.get(key)
        if value in (None, ""):
            return None
        return safe_float(value)

    def flow_bucket() -> str:
        taker_ratio = scout_float("taker_buy_ratio_5m")
        aggressive_flow = scout_float("aggressive_flow_delta")
        if taker_ratio is None and aggressive_flow is None:
            return "missing"
        taker_ratio = taker_ratio if taker_ratio is not None else 1.0
        aggressive_flow = aggressive_flow if aggressive_flow is not None else 0.0
        if taker_ratio >= 1.25 or aggressive_flow >= 0.20:
            return "buyers"
        if taker_ratio <= 0.80 or aggressive_flow <= -0.20:
            return "sellers"
        return "balanced"

    def crowding_bucket() -> str:
        funding_rate = scout_float("funding_rate")
        basis_pct = scout_float("mark_index_basis_pct")
        if funding_rate is None and basis_pct is None:
            return "missing"
        funding_rate = funding_rate if funding_rate is not None else 0.0
        basis_pct = basis_pct if basis_pct is not None else 0.0
        if funding_rate >= 0.0008 or basis_pct >= 0.35:
            return "crowded_long"
        if funding_rate <= -0.0008 or basis_pct <= -0.35:
            return "crowded_short"
        return "neutral"

    def liquidation_bucket() -> str:
        long_liq = scout_float("liquidation_long_usd")
        short_liq = scout_float("liquidation_short_usd")
        if long_liq is None and short_liq is None:
            return "missing"
        long_liq = max(0.0, long_liq or 0.0)
        short_liq = max(0.0, short_liq or 0.0)
        if long_liq < 1.0 and short_liq < 1.0:
            return "quiet"
        if long_liq >= max(25_000.0, short_liq * 1.8):
            return "long_flush"
        if short_liq >= max(25_000.0, long_liq * 1.8):
            return "short_squeeze"
        return "mixed"

    def microstructure_bucket() -> str:
        spread_bps = scout_float("spread_bps")
        slippage_bps = scout_float("estimated_slippage_bps")
        if spread_bps is None and slippage_bps is None:
            return "missing"
        spread_bps = spread_bps if spread_bps is not None else 99.0
        slippage_bps = slippage_bps if slippage_bps is not None else 99.0
        if spread_bps <= 3.0 and slippage_bps <= 6.0:
            return "clean"
        if spread_bps <= 8.0 and slippage_bps <= 12.0:
            return "workable"
        return "thin"

    if split_by == "flow":
        return flow_bucket()
    if split_by == "crowding":
        return crowding_bucket()
    if split_by == "liquidation":
        return liquidation_bucket()
    if split_by == "microstructure":
        return microstructure_bucket()
    if split_by == "combined":
        return "|".join(
            [
                flow_bucket(),
                crowding_bucket(),
                liquidation_bucket(),
                microstructure_bucket(),
            ]
        )
    raise ValueError(f"Unsupported context split: {split_by}")


def event_context_label(
    record: dict[str, Any],
    *,
    split_by: str,
    horizon: dict[str, Any] | None = None,
) -> str:
    if split_by == "none":
        return "all"
    session_label = record_trading_session(record)
    if split_by == "session":
        return session_label
    enrichments = record.get("enrichments")
    if not isinstance(enrichments, dict) or not enrichments:
        if split_by == "playbook_session":
            return f"{session_label}|missing"
        return "missing"
    sample_payload = record.get("sample")
    sample_payload = sample_payload if isinstance(sample_payload, dict) else {}
    trigger_types = {
        str(token)
        for token in (record.get("trigger_types") or sample_payload.get("trigger_types") or [])
        if str(token)
    }
    sample_type = str(record.get("sample_type") or sample_payload.get("sample_type") or "")
    bar_interval = str(record.get("bar_interval") or sample_payload.get("bar_interval") or "")

    def enrich_float(key: str) -> float | None:
        value = enrichments.get(key)
        if value in (None, ""):
            return None
        return safe_float(value)

    def horizon_float(key: str) -> float | None:
        if not isinstance(horizon, dict):
            return None
        value = horizon.get(key)
        if value in (None, ""):
            return None
        return safe_float(value)

    def sample_bool(key: str) -> bool:
        value = sample_payload.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "on"}
        return False

    def has_right_side_reversal_confirmation() -> bool:
        trigger_direction = str(
            sample_payload.get("trigger_candle_direction")
            or sample_payload.get("candle_direction")
            or ""
        ).strip().lower()
        confirmation_direction = str(
            sample_payload.get("confirmation_candle_direction") or ""
        ).strip().lower()
        if not sample_bool("reversal_confirmation_passed"):
            return False
        return (
            (trigger_direction == "down" and confirmation_direction == "up")
            or (trigger_direction == "up" and confirmation_direction == "down")
        )

    def btc_regime_allows_reversal(*, trigger_direction: str) -> bool:
        btc_ret_5m_pct = enrich_float("btcusdt_ret_5m_pct")
        if btc_ret_5m_pct is None:
            return False
        normalized_direction = str(trigger_direction or "").strip().lower()
        if normalized_direction == "down":
            return btc_ret_5m_pct > (-BTC_REVERSAL_REGIME_BLOCK_PCT)
        if normalized_direction == "up":
            return btc_ret_5m_pct < BTC_REVERSAL_REGIME_BLOCK_PCT
        return False

    def oi_bucket() -> str:
        oi_5m = enrich_float("oi_change_5m_pct")
        oi_15m = enrich_float("oi_change_15m_pct")
        if oi_5m is None and oi_15m is None:
            return "missing"
        oi_5m = oi_5m if oi_5m is not None else 0.0
        oi_15m = oi_15m if oi_15m is not None else 0.0
        if oi_5m <= -0.35 or oi_15m <= -0.75:
            return "oi_flush"
        if oi_5m >= 0.50 or oi_15m >= 1.00:
            return "oi_build"
        return "oi_flat"

    def crowding_bucket() -> str:
        funding_rate = enrich_float("funding_rate")
        basis_pct = enrich_float("mark_index_basis_pct")
        if funding_rate is None and basis_pct is None:
            return "missing"
        funding_rate = funding_rate if funding_rate is not None else 0.0
        basis_pct = basis_pct if basis_pct is not None else 0.0
        if funding_rate >= 0.0008 or basis_pct >= 0.35:
            return "crowded_long"
        if funding_rate <= -0.0008 or basis_pct <= -0.35:
            return "crowded_short"
        return "neutral"

    def microstructure_bucket() -> str:
        depth_imbalance = enrich_float("depth_imbalance")
        spread_bps = enrich_float("spread_bps")
        slippage_bps = enrich_float("estimated_slippage_bps")
        if depth_imbalance is None and spread_bps is None and slippage_bps is None:
            return "missing"
        depth_imbalance = depth_imbalance if depth_imbalance is not None else 0.0
        spread_bps = spread_bps if spread_bps is not None else 99.0
        slippage_bps = slippage_bps if slippage_bps is not None else 99.0
        if spread_bps <= 3.0 and slippage_bps <= 6.0:
            liquidity = "clean"
        elif spread_bps <= 8.0 and slippage_bps <= 12.0:
            liquidity = "workable"
        else:
            liquidity = "thin"
        if depth_imbalance >= 0.15:
            side = "bid_lean"
        elif depth_imbalance <= -0.15:
            side = "ask_lean"
        else:
            side = "balanced"
        return f"{liquidity}:{side}"

    def playbook_bucket() -> str:
        explicit_playbook = str(record.get("playbook") or "").strip()
        if explicit_playbook:
            return explicit_playbook
        if sample_type == "baseline":
            return "baseline_control"
        oi_state = oi_bucket()
        depth_imbalance = enrich_float("depth_imbalance")
        max_drawdown_pct = horizon_float("max_drawdown_pct")
        trigger_candle_direction = str(
            sample_payload.get("trigger_candle_direction")
            or sample_payload.get("candle_direction")
            or ""
        ).strip().lower()
        has_expansion_shape = "body_expansion" in trigger_types or "range_expansion" in trigger_types
        has_breakout_shape = "volume_burst" in trigger_types and (
            has_expansion_shape
        )
        if (
            bar_interval == "5m"
            and has_breakout_shape
            and oi_state == "oi_build"
        ):
            return "oi_build_breakout"
        if (
            oi_state == "oi_flush"
            and has_expansion_shape
            and depth_imbalance is not None
            and depth_imbalance >= 0.12
            and trigger_candle_direction == "down"
            and (max_drawdown_pct is None or max_drawdown_pct <= -0.75)
        ):
            return "liquidation_flush"
        return "other_trigger"

    if split_by == "oi":
        return oi_bucket()
    if split_by == "crowding":
        return crowding_bucket()
    if split_by == "microstructure":
        return microstructure_bucket()
    if split_by == "combined":
        return "|".join([oi_bucket(), crowding_bucket(), microstructure_bucket()])
    if split_by == "playbook":
        return playbook_bucket()
    if split_by == "playbook_session":
        return f"{session_label}|{playbook_bucket()}"
    raise ValueError(f"Unsupported event context split: {split_by}")


def analyze_records(
    records: list[dict[str, Any]],
    *,
    round_trip_fee_bps: float,
    context_split: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    fee_pct = round_trip_fee_bps / 100.0
    for record in records:
        setup = str(record.get("setup") or "")
        symbol = str(record.get("symbol") or "").upper()
        context = scout_context_label(record, split_by=context_split)
        for horizon in record.get("horizons", []):
            if not isinstance(horizon, dict):
                continue
            horizon_sec = int(horizon.get("horizon_sec") or 0)
            final_return_pct = safe_float(horizon.get("final_return_pct"))
            mfe_pct = safe_float(horizon.get("mfe_pct"))
            mae_pct = safe_float(horizon.get("mae_pct"))
            grouped[(setup, horizon_sec, context)].append(
                {
                    "symbol": symbol,
                    "score": safe_float(record.get("score")),
                    "final_return_pct": final_return_pct,
                    "mfe_pct": mfe_pct,
                    "mae_pct": mae_pct,
                    "after_fee_pct": final_return_pct - fee_pct,
                }
            )
    rows: list[dict[str, Any]] = []
    for (setup, horizon_sec, context), values in grouped.items():
        final_returns = [item["final_return_pct"] for item in values]
        after_fee_returns = [item["after_fee_pct"] for item in values]
        mfes = [item["mfe_pct"] for item in values]
        maes = [item["mae_pct"] for item in values]
        scores = [item["score"] for item in values]
        symbols = [str(item["symbol"]) for item in values if str(item.get("symbol") or "")]
        unique_symbols = len(set(symbols))
        top_symbol_share_pct = 0.0
        if symbols:
            top_symbol_share_pct = (max(symbols.count(symbol) for symbol in set(symbols)) / len(symbols)) * 100.0
        rows.append(
            {
                "setup": setup,
                "context": context,
                "horizon_sec": horizon_sec,
                "observations": len(values),
                "unique_symbols": unique_symbols,
                "top_symbol_share_pct": round4(top_symbol_share_pct),
                "avg_score": round4(mean(scores)),
                "avg_final_pct": round4(mean(final_returns)),
                "median_final_pct": round4(median(final_returns)),
                "win_rate_pct": round4(
                    (len([item for item in final_returns if item > 0]) / len(final_returns)) * 100.0
                ),
                "avg_mfe_pct": round4(mean(mfes)),
                "avg_mae_pct": round4(mean(maes)),
                "avg_after_fee_pct": round4(mean(after_fee_returns)),
                "after_fee_win_rate_pct": round4(
                    (len([item for item in after_fee_returns if item > 0]) / len(after_fee_returns)) * 100.0
                ),
            }
        )
    rows.sort(
        key=lambda item: (
            item["avg_after_fee_pct"],
            item["win_rate_pct"],
            item["observations"],
        ),
        reverse=True,
    )
    return rows


def event_trigger_signature(record: dict[str, Any]) -> str:
    sample_type = str(record.get("sample_type") or "")
    trigger_types = [str(item) for item in (record.get("trigger_types") or []) if str(item)]
    if sample_type == "baseline":
        return "baseline_control"
    if not trigger_types:
        return "unclassified"
    return "+".join(sorted(trigger_types))


def analyze_event_records(
    records: list[dict[str, Any]],
    *,
    context_split: str = "none",
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        sample_type = str(record.get("sample_type") or "")
        bar_interval = str(record.get("bar_interval") or "")
        trigger_signature = event_trigger_signature(record)
        symbol = str(record.get("symbol") or "").upper()
        trigger_score = safe_float(record.get("trigger_score"))
        horizon_rows: list[dict[str, Any]] = []
        if isinstance(record.get("horizon"), dict):
            horizon_rows = [record["horizon"]]
        else:
            horizon_rows = [item for item in record.get("horizons", []) if isinstance(item, dict)]
        for horizon in horizon_rows:
            context = event_context_label(record, split_by=context_split, horizon=horizon)
            horizon_sec = int(horizon.get("horizon_sec") or 0)
            grouped[(sample_type, bar_interval, trigger_signature, horizon_sec, context)].append(
                {
                    "symbol": symbol,
                    "trigger_score": trigger_score,
                    "close_return_pct": safe_float(horizon.get("close_return_pct")),
                    "after_fee_return_pct": safe_float(horizon.get("after_fee_return_pct")),
                    "max_runup_pct": safe_float(horizon.get("max_runup_pct")),
                    "max_drawdown_pct": safe_float(horizon.get("max_drawdown_pct")),
                }
            )
    rows: list[dict[str, Any]] = []
    for (sample_type, bar_interval, trigger_signature, horizon_sec, context), values in grouped.items():
        symbols = [item["symbol"] for item in values if item.get("symbol")]
        unique_symbols = len(set(symbols))
        top_symbol_share_pct = 0.0
        if symbols:
            top_symbol_share_pct = (max(symbols.count(symbol) for symbol in set(symbols)) / len(symbols)) * 100.0
        after_fee_returns = [item["after_fee_return_pct"] for item in values]
        close_returns = [item["close_return_pct"] for item in values]
        max_runups = [item["max_runup_pct"] for item in values]
        max_drawdowns = [item["max_drawdown_pct"] for item in values]
        trigger_scores = [item["trigger_score"] for item in values]
        rows.append(
            {
                "sample_type": sample_type,
                "bar_interval": bar_interval,
                "trigger_signature": trigger_signature,
                "horizon_sec": horizon_sec,
                "context": context,
                "observations": len(values),
                "unique_symbols": unique_symbols,
                "top_symbol_share_pct": round4(top_symbol_share_pct),
                "avg_trigger_score": round4(mean(trigger_scores)),
                "avg_close_return_pct": round4(mean(close_returns)),
                "median_close_return_pct": round4(median(close_returns)),
                "avg_after_fee_pct": round4(mean(after_fee_returns)),
                "after_fee_win_rate_pct": round4(
                    (len([item for item in after_fee_returns if item > 0]) / len(after_fee_returns)) * 100.0
                ),
                "avg_max_runup_pct": round4(mean(max_runups)),
                "avg_max_drawdown_pct": round4(mean(max_drawdowns)),
            }
        )
    baseline_lookup = {
        (row["bar_interval"], row["horizon_sec"], row["context"]): row
        for row in rows
        if row["sample_type"] == "baseline" and row["trigger_signature"] == "baseline_control"
    }
    baseline_lookup_any = {
        (row["bar_interval"], row["horizon_sec"]): row
        for row in rows
        if row["sample_type"] == "baseline" and row["trigger_signature"] == "baseline_control"
    }
    for row in rows:
        baseline_row = baseline_lookup.get((row["bar_interval"], row["horizon_sec"], row["context"]))
        if baseline_row is None:
            baseline_row = baseline_lookup_any.get((row["bar_interval"], row["horizon_sec"]))
        if baseline_row is None:
            row["baseline_after_fee_pct"] = None
            row["baseline_after_fee_win_rate_pct"] = None
            row["delta_after_fee_pct"] = None
            row["delta_after_fee_win_rate_pct"] = None
            continue
        row["baseline_after_fee_pct"] = baseline_row["avg_after_fee_pct"]
        row["baseline_after_fee_win_rate_pct"] = baseline_row["after_fee_win_rate_pct"]
        row["delta_after_fee_pct"] = round4(row["avg_after_fee_pct"] - baseline_row["avg_after_fee_pct"])
        row["delta_after_fee_win_rate_pct"] = round4(
            row["after_fee_win_rate_pct"] - baseline_row["after_fee_win_rate_pct"]
        )
    rows.sort(
        key=lambda item: (
            item["delta_after_fee_pct"] if item["delta_after_fee_pct"] is not None else item["avg_after_fee_pct"],
            item["avg_after_fee_pct"],
            item["after_fee_win_rate_pct"],
            item["observations"],
        ),
        reverse=True,
    )
    return rows


def render_analysis_table(rows: list[dict[str, Any]], *, limit: int, show_context: bool) -> str:
    header = (
        "setup".ljust(28)
        + (" context".rjust(25) if show_context else "")
        + " horizon".rjust(8)
        + " obs".rjust(6)
        + " syms".rjust(6)
        + " topSym%".rjust(9)
        + " avg%".rjust(10)
        + " med%".rjust(10)
        + " afterFee%".rjust(12)
        + " win%".rjust(8)
        + " feeWin%".rjust(10)
        + " mfe%".rjust(10)
        + " mae%".rjust(10)
    )
    lines = [header, "-" * len(header)]
    for row in rows[:limit]:
        lines.append(
            str(row["setup"])[:28].ljust(28)
            + (str(row.get("context") or "")[:24].rjust(25) if show_context else "")
            + str(row["horizon_sec"]).rjust(8)
            + str(row["observations"]).rjust(6)
            + str(row["unique_symbols"]).rjust(6)
            + f"{row['top_symbol_share_pct']:.1f}".rjust(9)
            + f"{row['avg_final_pct']:.4f}".rjust(10)
            + f"{row['median_final_pct']:.4f}".rjust(10)
            + f"{row['avg_after_fee_pct']:.4f}".rjust(12)
            + f"{row['win_rate_pct']:.1f}".rjust(8)
            + f"{row['after_fee_win_rate_pct']:.1f}".rjust(10)
            + f"{row['avg_mfe_pct']:.4f}".rjust(10)
            + f"{row['avg_mae_pct']:.4f}".rjust(10)
        )
    return "\n".join(lines)


def render_event_analysis_table(rows: list[dict[str, Any]], *, limit: int, show_context: bool) -> str:
    header = (
        "sample".ljust(10)
        + " bar".rjust(5)
        + (" context".rjust(32) if show_context else "")
        + " horizon".rjust(8)
        + " obs".rjust(6)
        + " syms".rjust(6)
        + " top%".rjust(8)
        + " af%".rjust(9)
        + " dAF%".rjust(9)
        + " win%".rjust(8)
        + " dWin%".rjust(9)
        + " dd%".rjust(9)
        + " ru%".rjust(9)
        + " score".rjust(8)
        + " trigger".rjust(28)
    )
    lines = [header, "-" * len(header)]

    def fmt_delta(value: float | None, *, decimals: int) -> str:
        if value is None:
            return "n/a".rjust(9)
        return f"{value:.{decimals}f}".rjust(9)

    for row in rows[: max(1, limit)]:
        lines.append(
            str(row["sample_type"]).ljust(10)
            + str(row["bar_interval"]).rjust(5)
            + (str(row.get("context") or "")[:31].rjust(32) if show_context else "")
            + str(row["horizon_sec"]).rjust(8)
            + str(row["observations"]).rjust(6)
            + str(row["unique_symbols"]).rjust(6)
            + f"{row['top_symbol_share_pct']:.1f}".rjust(8)
            + f"{row['avg_after_fee_pct']:.4f}".rjust(9)
            + fmt_delta(row.get("delta_after_fee_pct"), decimals=4)
            + f"{row['after_fee_win_rate_pct']:.1f}".rjust(8)
            + fmt_delta(row.get("delta_after_fee_win_rate_pct"), decimals=1)
            + f"{row['avg_max_drawdown_pct']:.4f}".rjust(9)
            + f"{row['avg_max_runup_pct']:.4f}".rjust(9)
            + f"{row['avg_trigger_score']:.2f}".rjust(8)
            + str(row["trigger_signature"]).rjust(28)
        )
    return "\n".join(lines)


def analyze_events_main(args: argparse.Namespace) -> int:
    labels_file = args.labels_file
    if labels_file.is_dir():
        horizon_file = labels_file / "event_horizon_labels.jsonl"
        labels_file = horizon_file if horizon_file.exists() else (labels_file / "event_labels.jsonl")
    if not labels_file.exists():
        raise FileNotFoundError(f"Could not find event label file: {labels_file}")
    records = load_event_labeled_records(labels_file)
    rows = analyze_event_records(records, context_split=str(args.context_split))
    report = {
        "labels_file": str(labels_file),
        "record_count": len(records),
        "context_split": str(args.context_split),
        "rows": rows,
    }
    if args.report_file is not None:
        args.report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        render_event_analysis_table(
            rows,
            limit=max(1, args.limit),
            show_context=str(args.context_split) != "none",
        )
    )
    return 0


def analyze_main(args: argparse.Namespace) -> int:
    labels_file = args.labels_file
    if labels_file.is_dir():
        labels_file = labels_file / "candidate_labels.jsonl"
    if not labels_file.exists():
        raise FileNotFoundError(f"Could not find labeled observation file: {labels_file}")
    records = load_labeled_records(labels_file)
    rows = analyze_records(
        records,
        round_trip_fee_bps=max(0.0, float(args.round_trip_fee_bps or 0.0)),
        context_split=str(args.context_split),
    )
    report = {
        "labels_file": str(labels_file),
        "record_count": len(records),
        "round_trip_fee_bps": max(0.0, float(args.round_trip_fee_bps or 0.0)),
        "context_split": str(args.context_split),
        "rows": rows,
    }
    if args.report_file is not None:
        args.report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        render_analysis_table(
            rows,
            limit=max(1, args.limit),
            show_context=str(args.context_split) != "none",
        )
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Observe Phoenix candidates without trading them, then label and analyze forward outcomes."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser(
        "collect",
        help="Broad market event collector with async enrichment and delayed forward labeling.",
    )
    collect.add_argument("--env", default="prod", choices=["prod", "testnet", "demo"])
    collect.add_argument("--output-dir", type=Path, default=None)
    collect.add_argument("--duration-sec", type=int, default=DEFAULT_COLLECT_DURATION_SEC)
    collect.add_argument("--cycle-sec", type=int, default=20)
    collect.add_argument("--horizons-sec", default=",".join(str(item) for item in DEFAULT_COLLECT_HORIZONS_SEC))
    collect.add_argument("--scan-top", type=int, default=200)
    collect.add_argument("--scan-offset", type=int, default=0)
    collect.add_argument("--scan-step", type=int, default=60)
    collect.add_argument("--universe-top", type=int, default=200)
    collect.add_argument(
        "--universe-sort",
        default="quote_volume",
        choices=["quote_volume", "positive_change", "negative_change", "abs_change"],
    )
    collect.add_argument("--universe-cache-sec", type=float, default=DEFAULT_UNIVERSE_CACHE_SEC)
    collect.add_argument("--kline-cache-ttl-1m-sec", type=float, default=8.0)
    collect.add_argument("--kline-cache-ttl-3m-sec", type=float, default=DEFAULT_KLINE_CACHE_TTL_3M_SEC)
    collect.add_argument("--kline-cache-ttl-5m-sec", type=float, default=45.0)
    collect.add_argument("--min-quote-volume", type=float, default=5_000_000.0)
    collect.add_argument("--trigger-min-price", type=float, default=0.01)
    collect.add_argument("--trigger-min-quote-volume", type=float, default=5_000_000.0)
    collect.add_argument("--trigger-min-avg-quote-turnover", type=float, default=100_000.0)
    collect.add_argument("--trigger-min-current-quote-turnover", type=float, default=10_000.0)
    collect.add_argument("--atr-period", type=int, default=20)
    collect.add_argument("--atr-multiplier", type=float, default=2.0)
    collect.add_argument("--atr-multiplier-1m", type=float, default=1.5)
    collect.add_argument("--atr-multiplier-5m", type=float, default=1.5)
    collect.add_argument("--volume-lookback", type=int, default=20)
    collect.add_argument("--volume-multiplier", type=float, default=3.0)
    collect.add_argument("--volume-multiplier-1m", type=float, default=2.0)
    collect.add_argument("--volume-multiplier-5m", type=float, default=2.25)
    collect.add_argument(
        "--allowed-event-patterns",
        default=None,
        help=(
            "Optional comma-separated filters like "
            "'trigger:5m:volume_burst,baseline:5m:baseline_control'. "
            "Use * as a wildcard in any position."
        ),
    )
    collect.add_argument("--ranked-samples-per-cycle", type=int, default=0)
    collect.add_argument("--ranked-min-trigger-ratio", type=float, default=0.75)
    collect.add_argument("--baseline-samples-per-cycle", type=int, default=0)
    collect.add_argument("--max-events-per-cycle", type=int, default=80)
    collect.add_argument("--dedupe-sec", type=int, default=30)
    collect.add_argument("--symbol-cooldown-sec", type=int, default=DEFAULT_EVENT_SYMBOL_COOLDOWN_SEC)
    collect.add_argument("--max-active-per-symbol", type=int, default=4)
    collect.add_argument("--queue-size", type=int, default=DEFAULT_EVENT_QUEUE_SIZE)
    collect.add_argument("--worker-count", type=int, default=6)
    collect.add_argument("--kline-concurrency", type=int, default=12)
    collect.add_argument("--cycle-timeout-sec", type=float, default=DEFAULT_CYCLE_TIMEOUT_SEC)
    collect.add_argument("--depth-limit", type=int, default=DEFAULT_EVENT_DEPTH_LIMIT)
    collect.add_argument("--depth-slippage-notional-usdt", type=float, default=200.0)
    collect.add_argument("--public-cache-ttl-sec", type=float, default=DEFAULT_EVENT_PUBLIC_CACHE_TTL_SEC)
    collect.add_argument("--oi-hist-cache-ttl-sec", type=float, default=DEFAULT_EVENT_OI_HIST_CACHE_TTL_SEC)
    collect.add_argument("--round-trip-fee-bps", type=float, default=0.0)
    collect.add_argument("--drain-timeout-sec", type=float, default=None)

    observe = subparsers.add_parser("observe", help="Collect candidate observations and forward labels.")
    observe.add_argument("--env", default="prod", choices=["prod", "testnet", "demo"])
    observe.add_argument("--output-dir", type=Path, default=None)
    observe.add_argument("--scout-snapshot-file", type=Path, default=None)
    observe.add_argument("--duration-sec", type=int, default=900)
    observe.add_argument("--cycle-sec", type=int, default=20)
    observe.add_argument("--top-n", type=int, default=8)
    observe.add_argument("--dedupe-sec", type=int, default=45)
    observe.add_argument("--horizons-sec", default="20,45,90,180")
    observe.add_argument("--sampling-mode", default="hybrid", choices=["strict", "hybrid", "broad"])
    observe.add_argument("--scan-top", type=int, default=120)
    observe.add_argument("--scan-offset", type=int, default=0)
    observe.add_argument("--scan-step", type=int, default=0)
    observe.add_argument("--universe-top", type=int, default=240)
    observe.add_argument(
        "--universe-sort",
        default="quote_volume",
        choices=["quote_volume", "positive_change", "negative_change", "abs_change"],
    )
    observe.add_argument("--universe-cache-sec", type=float, default=DEFAULT_UNIVERSE_CACHE_SEC)
    observe.add_argument("--kline-cache-ttl-1m-sec", type=float, default=DEFAULT_KLINE_CACHE_TTL_1M_SEC)
    observe.add_argument("--kline-cache-ttl-5m-sec", type=float, default=DEFAULT_KLINE_CACHE_TTL_5M_SEC)
    observe.add_argument("--kline-cache-ttl-15m-sec", type=float, default=DEFAULT_KLINE_CACHE_TTL_15M_SEC)
    observe.add_argument("--min-quote-volume", type=float, default=250_000.0)
    observe.add_argument("--feature-min-quote-volume", type=float, default=DEFAULT_FEATURE_MIN_QUOTE_VOLUME)
    observe.add_argument("--starting-min-score", type=float, default=40.0)
    observe.add_argument("--execution-floor-offset", type=float, default=0.0)
    observe.add_argument("--allow-short", action="store_true")
    observe.add_argument("--cycle-timeout-sec", type=float, default=DEFAULT_CYCLE_TIMEOUT_SEC)
    observe.add_argument("--include-setups", default=None)
    observe.add_argument("--max-observations-per-symbol", type=int, default=0)
    observe.add_argument("--symbol-cooldown-sec", type=int, default=0)
    observe.add_argument("--max-active-per-symbol", type=int, default=0)
    observe.add_argument("--kline-concurrency", type=int, default=3)
    observe.add_argument("--shared-symbol-leases-file", type=Path, default=None)

    analyze = subparsers.add_parser("analyze", help="Aggregate labeled observations into setup summaries.")
    analyze.add_argument("--labels-file", type=Path, required=True)
    analyze.add_argument("--round-trip-fee-bps", type=float, default=0.0)
    analyze.add_argument("--limit", type=int, default=30)
    analyze.add_argument("--report-file", type=Path, default=None)
    analyze.add_argument(
        "--context-split",
        default="none",
        choices=["none", "flow", "crowding", "liquidation", "microstructure", "combined"],
    )

    analyze_events = subparsers.add_parser(
        "analyze-events",
        help="Aggregate event collector labels into trigger/sample summaries.",
    )
    analyze_events.add_argument("--labels-file", type=Path, required=True)
    analyze_events.add_argument("--limit", type=int, default=30)
    analyze_events.add_argument("--report-file", type=Path, default=None)
    analyze_events.add_argument(
        "--context-split",
        default="none",
        choices=["none", "oi", "crowding", "microstructure", "combined", "playbook", "session", "playbook_session"],
    )

    diagnose = subparsers.add_parser("diagnose", help="Scan the current market and print setup near-misses.")
    diagnose.add_argument("--env", default="prod", choices=["prod", "testnet", "demo"])
    diagnose.add_argument("--setups", default="broad_hot_momentum_long")
    diagnose.add_argument("--scan-top", type=int, default=120)
    diagnose.add_argument("--universe-top", type=int, default=240)
    diagnose.add_argument("--min-quote-volume", type=float, default=250_000.0)
    diagnose.add_argument("--feature-min-quote-volume", type=float, default=DEFAULT_FEATURE_MIN_QUOTE_VOLUME)
    diagnose.add_argument("--limit", type=int, default=8)
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    if args.command == "collect":
        return await collect_main(args)
    if args.command == "observe":
        return await observe_main(args)
    if args.command == "analyze":
        return analyze_main(args)
    if args.command == "analyze-events":
        return analyze_events_main(args)
    if args.command == "diagnose":
        return await diagnose_main(args)
    raise RuntimeError(f"Unsupported command: {args.command}")


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
