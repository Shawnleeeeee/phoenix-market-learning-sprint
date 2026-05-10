#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import time
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from phoenix.signal_lab_events import (
    EventTriggerConfig,
    build_market_event_context,
    build_triggered_market_event,
    compute_open_interest_context,
)
from phoenix_factor_factory import (
    FACTOR_SIGNAL_RULES,
    build_factor_factory_control,
    build_factor_factory_report,
    rule_evaluation_rows,
    summarize_returns,
)
from phoenix_feature_slice_report import (
    build_snapshot_index,
    feature_bucket,
    horizon_sec_for_outcome,
    return_pct_for_outcome,
    row_branch_id,
    source_event_id_for_outcome,
)
from phoenix_factor_library import FACTOR_VERSION, build_factor_vector, safe_float
from phoenix_playbook_backtest import (
    BinancePublicBackfillClient,
    BinancePublicZipBackfillClient,
    DEFAULT_PUBLIC_ZIP_CACHE_DIR,
    KLINE_COLUMNS,
    build_historical_ticker_item,
    build_history_chunks,
    build_proxy_candle_metrics,
    classify_playbook_record_with_proxies,
    dedupe_sort_rows,
    discover_universe,
    parse_symbol_list,
    parse_utc_date,
    read_json_payload,
    resolve_backtest_window,
    trading_session_label,
)


SNAPSHOT_EVENT_NAME = "market_event_created"
OUTCOME_EVENT_NAME = "signal_bridge_shadow_horizon_result"
FACTOR_EVENT_NAME = "phoenix_historical_factor_vector"
RESEARCH_BRANCH_TYPE = "research_pool"
DEFAULT_HORIZONS = (60, 300, 900, 3600)
INTERVAL_TO_SEC = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}
HORIZON_LABELS = {
    60: "1M",
    300: "5M",
    900: "15M",
    3600: "1H",
}


@dataclass(slots=True)
class HistoricalResearchRows:
    event_samples: list[dict[str, Any]]
    factor_vectors: list[dict[str, Any]]
    labeled_outcomes: list[dict[str, Any]]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def round6(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def parse_horizon_seconds(raw_value: str | Sequence[int] | None) -> list[int]:
    if raw_value is None or raw_value == "":
        return list(DEFAULT_HORIZONS)
    if isinstance(raw_value, (list, tuple)):
        values = [int(item) for item in raw_value]
        return sorted({value for value in values if value > 0})
    horizons: list[int] = []
    for part in str(raw_value).replace(";", ",").split(","):
        token = part.strip().lower()
        if not token:
            continue
        if token.endswith("ms"):
            seconds = int(float(token[:-2]) / 1000.0)
        elif token.endswith("m"):
            seconds = int(float(token[:-1]) * 60)
        elif token.endswith("h"):
            seconds = int(float(token[:-1]) * 3600)
        elif token.endswith("s"):
            seconds = int(float(token[:-1]))
        else:
            seconds = int(float(token))
        if seconds <= 0:
            raise ValueError(f"Horizon must be positive: {part}")
        horizons.append(seconds)
    return sorted(set(horizons))


def horizon_label(horizon_sec: int) -> str:
    return HORIZON_LABELS.get(int(horizon_sec), f"{int(horizon_sec)}S")


def side_return_pct(*, side: str, entry_price: float, exit_price: float) -> float:
    if entry_price <= 0 or exit_price <= 0:
        return 0.0
    if str(side).upper() == "SELL":
        return ((entry_price / exit_price) - 1.0) * 100.0
    return ((exit_price / entry_price) - 1.0) * 100.0


def normalize_kline_jsonl_row(row: dict[str, Any]) -> list[Any] | None:
    open_time = row.get("open_time") if "open_time" in row else row.get("open_time_ms")
    close_time = row.get("close_time") if "close_time" in row else row.get("close_time_ms")
    if open_time in (None, "") or close_time in (None, ""):
        return None
    return [
        open_time,
        row.get("open"),
        row.get("high"),
        row.get("low"),
        row.get("close"),
        row.get("volume"),
        close_time,
        row.get("quote_asset_volume"),
        row.get("number_of_trades"),
        row.get("taker_buy_base_volume")
        if "taker_buy_base_volume" in row
        else row.get("taker_buy_base_asset_volume"),
        row.get("taker_buy_quote_volume")
        if "taker_buy_quote_volume" in row
        else row.get("taker_buy_quote_asset_volume"),
        row.get("ignore"),
    ]


def read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw_line = line.strip()
            if not raw_line:
                continue
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def read_klines_jsonl(path: Path) -> list[list[Any]]:
    candles = [row for row in (normalize_kline_jsonl_row(item) for item in read_jsonl_records(path)) if row is not None]
    return sorted(candles, key=lambda item: int(item[0]))


def read_oi_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows = read_jsonl_records(path)
    return dedupe_sort_rows(rows, "timestamp")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
    return count


def resolve_proxy() -> str | None:
    for key in ("BTC_ALL_PROXY", "ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        value = os.environ.get(key)
        if value:
            return value
    return None


def extract_open_interest_value(row: dict[str, Any]) -> float:
    for key in ("openInterest", "sumOpenInterest", "sumOpenInterestValue"):
        value = safe_float(row.get(key))
        if value is not None and value > 0:
            return value
    return 0.0


def oi_rows_for_anchor(oi_rows: list[dict[str, Any]], oi_timestamps: list[int], anchor_close_time_ms: int) -> list[dict[str, Any]]:
    if not oi_rows:
        return []
    cursor = bisect_right(oi_timestamps, int(anchor_close_time_ms))
    return oi_rows[max(0, cursor - 4) : cursor]


def current_oi_enrichments(
    oi_rows: list[dict[str, Any]],
    oi_timestamps: list[int],
    anchor_close_time_ms: int,
) -> dict[str, Any]:
    oi_slice = oi_rows_for_anchor(oi_rows, oi_timestamps, anchor_close_time_ms)
    if not oi_slice:
        return {
            "open_interest": None,
            "oi_change_5m_pct": None,
            "oi_change_15m_pct": None,
            "derivatives_data_source": "missing",
        }
    current_oi = extract_open_interest_value(oi_slice[-1])
    enrichments = compute_open_interest_context(current_oi, oi_slice)
    enrichments["derivatives_data_source"] = "binance_public_metrics"
    for key in (
        "countToptraderLongShortRatio",
        "sumToptraderLongShortRatio",
        "countLongShortRatio",
        "sumTakerLongShortVolRatio",
    ):
        if key in oi_slice[-1]:
            enrichments[key] = safe_float(oi_slice[-1].get(key))
    return enrichments


def build_trigger_config(*, min_quote_volume: float) -> EventTriggerConfig:
    return EventTriggerConfig(
        atr_period=20,
        atr_multiplier=2.0,
        volume_lookback=20,
        volume_multiplier=3.0,
        atr_multiplier_1m=1.5,
        volume_multiplier_1m=2.0,
        atr_multiplier_5m=1.5,
        volume_multiplier_5m=2.25,
        min_price=0.01,
        min_quote_volume_24h=max(0.0, float(min_quote_volume or 0.0)),
        min_avg_quote_turnover_1m=0.0,
        min_current_quote_turnover_1m=0.0,
    )


def build_snapshot_record(
    *,
    symbol: str,
    interval: str,
    current_index: int,
    candles: list[list[Any]],
    oi_rows: list[dict[str, Any]],
    oi_timestamps: list[int],
    trigger_config: EventTriggerConfig,
    rolling_24h_bars: int,
) -> dict[str, Any] | None:
    ticker_item = build_historical_ticker_item(
        symbol,
        candles,
        current_index=current_index,
        rolling_bars=max(1, int(rolling_24h_bars)),
    )
    if ticker_item is None:
        return None
    window = candles[max(0, current_index - 80) : current_index + 2]
    context = build_market_event_context(
        ticker_item,
        window,
        config=trigger_config,
        bar_interval=interval,
    )
    if context is None:
        return None
    sample = build_triggered_market_event(context, config=trigger_config)
    if sample is None:
        return None

    sample_payload = sample.to_payload()
    sample_payload.update(build_proxy_candle_metrics(candles[current_index]))
    enrichments = current_oi_enrichments(oi_rows, oi_timestamps, int(context.anchor_close_time_ms))
    record = {
        "event": SNAPSHOT_EVENT_NAME,
        "source": "phoenix_historical_research_replay",
        "event_id": f"hist-{symbol}-{int(context.anchor_close_time_ms)}-{'+'.join(sample.trigger_types)}",
        "symbol": symbol,
        "sample_type": sample.sample_type,
        "bar_interval": interval,
        "trigger_type": "+".join(sorted(sample.trigger_types)),
        "trigger_types": list(sample.trigger_types),
        "trigger_score": round6(sample.trigger_score),
        "sample": sample_payload,
        "enrichments": enrichments,
        "trading_session": trading_session_label(int(context.anchor_close_time_ms)),
        "observed_at_ms": int(context.anchor_close_time_ms),
        "observed_at": iso_from_ms(int(context.anchor_close_time_ms)),
        "anchor_index": int(current_index),
        "research_only": True,
        "branch_type": RESEARCH_BRANCH_TYPE,
    }
    record["playbook"] = classify_playbook_record_with_proxies(record)
    factors = build_factor_vector(record)
    record["factors"] = factors
    return record


def build_factor_vector_record(snapshot: dict[str, Any]) -> dict[str, Any]:
    factors = snapshot.get("factors") if isinstance(snapshot.get("factors"), dict) else build_factor_vector(snapshot)
    return {
        "event": FACTOR_EVENT_NAME,
        "source": "phoenix_historical_research_replay",
        "event_id": f"{snapshot['event_id']}::factors",
        "source_event_id": snapshot["event_id"],
        "symbol": snapshot.get("symbol"),
        "observed_at_ms": snapshot.get("observed_at_ms"),
        "bar_interval": snapshot.get("bar_interval"),
        "factor_version": FACTOR_VERSION,
        "factors": factors,
    }


def build_outcome_record(
    *,
    snapshot: dict[str, Any],
    side: str,
    horizon_sec: int,
    entry_price: float,
    exit_price: float,
    close_return_pct: float,
    after_fee_return_pct: float,
    max_favorable_pct: float,
    max_adverse_pct: float,
    quote_allocation_usdt: float,
    exit_time_ms: int,
    interval_sec: int,
    candle_count: int,
) -> dict[str, Any]:
    side = str(side).upper()
    label = horizon_label(horizon_sec)
    branch_id = f"HIST_{side}_{label}"
    event_id = f"{snapshot['event_id']}::{branch_id}"
    pnl_usdt = float(quote_allocation_usdt) * (after_fee_return_pct / 100.0)
    return {
        "event": OUTCOME_EVENT_NAME,
        "source": "phoenix_historical_research_replay",
        "event_id": event_id,
        "shadow_instance_id": event_id,
        "source_event_id": snapshot["event_id"],
        "horizon_event_id": snapshot["event_id"],
        "shadow_branch_id": branch_id,
        "branch_type": RESEARCH_BRANCH_TYPE,
        "research_only": True,
        "symbol": snapshot.get("symbol"),
        "side": side,
        "direction_variant": "long_forward" if side == "BUY" else "short_forward",
        "research_direction": "historical_forward_label",
        "horizon_sec": int(horizon_sec),
        "requested_horizon_sec": int(horizon_sec),
        "effective_horizon_sec": int(candle_count * interval_sec),
        "bar_interval": snapshot.get("bar_interval"),
        "entry_time_ms": int(snapshot.get("observed_at_ms") or 0) + 1,
        "exit_time_ms": int(exit_time_ms),
        "exit_time": iso_from_ms(int(exit_time_ms)),
        "entry_price": round6(entry_price),
        "exit_price": round6(exit_price),
        "close_return_pct": round6(close_return_pct),
        "after_fee_return_pct": round6(after_fee_return_pct),
        "after_fee_and_slippage_return_pct": round6(after_fee_return_pct),
        "max_favorable_pct": round6(max_favorable_pct),
        "max_adverse_pct": round6(max_adverse_pct),
        "effective_quote_allocation_usdt": round6(float(quote_allocation_usdt)),
        "pnl_usdt": round6(pnl_usdt),
        "label_resolution_warning": int(horizon_sec) < int(interval_sec),
    }


def label_snapshot_outcomes(
    *,
    snapshot: dict[str, Any],
    candles: list[list[Any]],
    horizons_sec: Sequence[int],
    interval_sec: int,
    round_trip_fee_bps: float,
    quote_allocation_usdt: float,
) -> list[dict[str, Any]]:
    current_index = int(snapshot.get("anchor_index") or -1)
    entry_index = current_index + 1
    if entry_index >= len(candles):
        return []
    entry_price = safe_float(candles[entry_index][1])
    if entry_price is None or entry_price <= 0:
        return []
    fee_pct = max(0.0, float(round_trip_fee_bps or 0.0)) / 100.0
    rows: list[dict[str, Any]] = []
    for horizon_sec in sorted({int(value) for value in horizons_sec if int(value) > 0}):
        candle_count = max(1, int(math.ceil(horizon_sec / max(1, interval_sec))))
        exit_index = entry_index + candle_count - 1
        if exit_index >= len(candles):
            continue
        future = candles[entry_index : exit_index + 1]
        exit_price = safe_float(future[-1][4])
        if exit_price is None or exit_price <= 0:
            continue
        highs = [safe_float(row[2]) or 0.0 for row in future]
        lows = [safe_float(row[3]) or 0.0 for row in future]
        exit_time_ms = int(future[-1][6])
        for side in ("BUY", "SELL"):
            close_return_pct = side_return_pct(side=side, entry_price=entry_price, exit_price=exit_price)
            if side == "BUY":
                max_favorable_pct = side_return_pct(side=side, entry_price=entry_price, exit_price=max(highs))
                max_adverse_pct = side_return_pct(side=side, entry_price=entry_price, exit_price=min(lows))
            else:
                max_favorable_pct = side_return_pct(side=side, entry_price=entry_price, exit_price=min(lows))
                max_adverse_pct = side_return_pct(side=side, entry_price=entry_price, exit_price=max(highs))
            rows.append(
                build_outcome_record(
                    snapshot=snapshot,
                    side=side,
                    horizon_sec=horizon_sec,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    close_return_pct=close_return_pct,
                    after_fee_return_pct=close_return_pct - fee_pct,
                    max_favorable_pct=max_favorable_pct,
                    max_adverse_pct=max_adverse_pct,
                    quote_allocation_usdt=quote_allocation_usdt,
                    exit_time_ms=exit_time_ms,
                    interval_sec=interval_sec,
                    candle_count=candle_count,
                )
            )
    return rows


def build_historical_research_rows(
    *,
    symbol: str,
    candles: list[list[Any]],
    oi_rows: list[dict[str, Any]],
    interval: str,
    horizons_sec: Sequence[int],
    round_trip_fee_bps: float,
    quote_allocation_usdt: float,
    min_quote_volume: float,
    rolling_24h_bars: int | None = None,
) -> HistoricalResearchRows:
    interval_sec = INTERVAL_TO_SEC.get(str(interval), 300)
    rolling_bars = int(rolling_24h_bars or max(1, int(86_400 / interval_sec)))
    candles = sorted(candles, key=lambda item: int(item[0]))
    oi_rows = [row for row in oi_rows if int(row.get("timestamp") or 0) > 0]
    oi_rows = dedupe_sort_rows(oi_rows, "timestamp")
    oi_timestamps = [int(row.get("timestamp") or 0) for row in oi_rows]
    trigger_config = build_trigger_config(min_quote_volume=min_quote_volume)
    event_samples: list[dict[str, Any]] = []
    factor_vectors: list[dict[str, Any]] = []
    labeled_outcomes: list[dict[str, Any]] = []
    start_index = max(20, rolling_bars)
    last_horizon = max([int(value) for value in horizons_sec] or [0])
    required_future_bars = max(1, int(math.ceil(last_horizon / max(1, interval_sec))))
    stop_index = max(start_index, len(candles) - required_future_bars - 1)
    for current_index in range(start_index, stop_index):
        snapshot = build_snapshot_record(
            symbol=symbol,
            interval=interval,
            current_index=current_index,
            candles=candles,
            oi_rows=oi_rows,
            oi_timestamps=oi_timestamps,
            trigger_config=trigger_config,
            rolling_24h_bars=rolling_bars,
        )
        if snapshot is None:
            continue
        event_samples.append(snapshot)
        factor_vectors.append(build_factor_vector_record(snapshot))
        labeled_outcomes.extend(
            label_snapshot_outcomes(
                snapshot=snapshot,
                candles=candles,
                horizons_sec=horizons_sec,
                interval_sec=interval_sec,
                round_trip_fee_bps=round_trip_fee_bps,
                quote_allocation_usdt=quote_allocation_usdt,
            )
        )
    return HistoricalResearchRows(
        event_samples=event_samples,
        factor_vectors=factor_vectors,
        labeled_outcomes=labeled_outcomes,
    )


def event_time_ms(snapshot: dict[str, Any]) -> int:
    value = safe_float(snapshot.get("observed_at_ms"))
    if value is not None:
        return int(value)
    sample = snapshot.get("sample") if isinstance(snapshot.get("sample"), dict) else {}
    return int(safe_float(sample.get("anchor_close_time_ms")) or 0)


def matching_rule_returns(rows: list[dict[str, Any]], rule_names: set[str]) -> list[float]:
    values: list[float] = []
    rules = [rule for rule in FACTOR_SIGNAL_RULES if rule.name in rule_names]
    for row in rows:
        factors = row.get("factors") if isinstance(row.get("factors"), dict) else {}
        side = str(row.get("side") or "").upper()
        return_pct = safe_float(row.get("return_pct"))
        if return_pct is None:
            continue
        for rule in rules:
            if rule.side_resolver(factors) == side:
                values.append(float(return_pct))
                break
    return values


def attribution_payload(report: dict[str, Any]) -> dict[str, Any]:
    payload = report.get("factor_attribution") if isinstance(report.get("factor_attribution"), dict) else report
    return payload if isinstance(payload, dict) else {}


def candidate_sort_key(item: dict[str, Any]) -> tuple[float, float, int]:
    return (
        float(item.get("avg_return_pct") or -999999.0),
        float(item.get("profit_factor") or -999999.0),
        int(item.get("sample_count") or 0),
    )


def rule_id_for_candidate(*, rule_type: str, branch_id: str, horizon_sec: int, conditions: list[dict[str, str]]) -> str:
    condition_text = "__".join(f"{item['feature']}={item['bucket']}" for item in conditions)
    safe_text = "".join(ch if ch.isalnum() or ch in {"_", "-", "=", "."} else "_" for ch in condition_text)
    return f"ATTR_{rule_type.upper()}_{branch_id}_{int(horizon_sec)}_{safe_text}"


def build_candidate_from_slice(row: dict[str, Any], *, rule_type: str) -> dict[str, Any] | None:
    branch_id = str(row.get("branch_id") or "").strip().upper()
    horizon_sec = int(safe_float(row.get("horizon_sec")) or 0)
    if not branch_id or horizon_sec <= 0:
        return None
    conditions: list[dict[str, str]] = []
    if rule_type == "single_slice":
        feature = str(row.get("feature") or "").strip()
        bucket = str(row.get("bucket") or "").strip()
        if feature and bucket:
            conditions.append({"feature": feature, "bucket": bucket})
    else:
        feature_a = str(row.get("feature_a") or "").strip()
        bucket_a = str(row.get("bucket_a") or "").strip()
        feature_b = str(row.get("feature_b") or "").strip()
        bucket_b = str(row.get("bucket_b") or "").strip()
        if feature_a and bucket_a:
            conditions.append({"feature": feature_a, "bucket": bucket_a})
        if feature_b and bucket_b:
            conditions.append({"feature": feature_b, "bucket": bucket_b})
    if not conditions:
        return None
    return {
        "rule_id": rule_id_for_candidate(
            rule_type=rule_type,
            branch_id=branch_id,
            horizon_sec=horizon_sec,
            conditions=conditions,
        ),
        "rule_type": rule_type,
        "source": "attribution_slice",
        "branch_id": branch_id,
        "horizon_sec": horizon_sec,
        "conditions": conditions,
        "sample_count": int(row.get("sample_count") or 0),
        "avg_return_pct": safe_float(row.get("avg_return_pct")),
        "win_rate_pct": safe_float(row.get("win_rate_pct")),
        "profit_factor": safe_float(row.get("profit_factor")),
        "sharpe": safe_float(row.get("sharpe")),
        "mdd_pct": safe_float(row.get("mdd_pct")),
        "train_slice": row,
    }


def derive_attribution_candidate_rules(
    report: dict[str, Any],
    *,
    min_samples: int = 30,
    min_profit_factor: float = 1.2,
    min_avg_return_pct: float = 0.0,
    top_n: int = 25,
) -> list[dict[str, Any]]:
    payload = attribution_payload(report)
    source_rows: list[tuple[str, dict[str, Any]]] = []
    for row in payload.get("top_pair_factor_slices") or []:
        if isinstance(row, dict):
            source_rows.append(("pair_slice", row))
    for row in payload.get("all_positive_pair_factor_slices") or []:
        if isinstance(row, dict):
            source_rows.append(("pair_slice", row))
    for row in payload.get("top_single_factor_slices") or []:
        if isinstance(row, dict):
            source_rows.append(("single_slice", row))
    candidates: dict[str, dict[str, Any]] = {}
    for rule_type, row in source_rows:
        if int(row.get("sample_count") or 0) < max(1, int(min_samples)):
            continue
        if (safe_float(row.get("avg_return_pct")) or 0.0) <= float(min_avg_return_pct):
            continue
        if (safe_float(row.get("profit_factor")) or 0.0) < float(min_profit_factor):
            continue
        candidate = build_candidate_from_slice(row, rule_type=rule_type)
        if candidate is None:
            continue
        current = candidates.get(candidate["rule_id"])
        if current is None or candidate_sort_key(candidate) > candidate_sort_key(current):
            candidates[candidate["rule_id"]] = candidate
    ranked = sorted(candidates.values(), key=candidate_sort_key, reverse=True)
    return ranked[: max(1, int(top_n))]


def candidate_matches_outcome(
    candidate: dict[str, Any],
    *,
    snapshot: dict[str, Any] | None,
    outcome: dict[str, Any],
) -> bool:
    if row_branch_id(outcome) != str(candidate.get("branch_id") or "").strip().upper():
        return False
    if horizon_sec_for_outcome(outcome) != int(candidate.get("horizon_sec") or 0):
        return False
    for condition in candidate.get("conditions") or []:
        if not isinstance(condition, dict):
            return False
        feature = str(condition.get("feature") or "").strip()
        bucket = str(condition.get("bucket") or "").strip()
        if not feature or feature_bucket(feature, snapshot=snapshot, outcome=outcome) != bucket:
            return False
    return True


def evaluate_candidate_rules(
    snapshots: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    candidates: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    snapshot_by_event = build_snapshot_index(snapshots)
    candidate_list = list(candidates)
    rule_returns: dict[str, list[float]] = {str(rule.get("rule_id")): [] for rule in candidate_list}
    rule_matches: dict[str, list[dict[str, Any]]] = {str(rule.get("rule_id")): [] for rule in candidate_list}
    all_returns: list[float] = []
    for outcome in outcomes:
        return_pct = return_pct_for_outcome(outcome)
        if return_pct is None:
            continue
        snapshot = snapshot_by_event.get(source_event_id_for_outcome(outcome))
        for candidate in candidate_list:
            if not candidate_matches_outcome(candidate, snapshot=snapshot, outcome=outcome):
                continue
            rule_id = str(candidate.get("rule_id"))
            rule_returns.setdefault(rule_id, []).append(float(return_pct))
            rule_matches.setdefault(rule_id, []).append(
                {
                    "return_pct": float(return_pct),
                    "symbol": str(outcome.get("symbol") or (snapshot or {}).get("symbol") or "unknown").upper(),
                    "trading_session": str(
                        outcome.get("trading_session") or (snapshot or {}).get("trading_session") or "unknown"
                    ),
                    "day": day_key_for_outcome(outcome, snapshot),
                }
            )
            all_returns.append(float(return_pct))
    rules = []
    by_id = {str(rule.get("rule_id")): rule for rule in candidate_list}
    for rule_id, returns in sorted(rule_returns.items()):
        rule = by_id.get(rule_id, {"rule_id": rule_id})
        rules.append(
            {
                **rule,
                "test_summary": summarize_returns(returns),
                "return_components": return_components(returns),
                "concentration": concentration_payload(rule_matches.get(rule_id, [])),
            }
        )
    rules.sort(
        key=lambda item: (
            float((item.get("test_summary") or {}).get("avg_return_pct") or -999999.0),
            float((item.get("test_summary") or {}).get("profit_factor") or -999999.0),
            int((item.get("test_summary") or {}).get("sample_count") or 0),
        ),
        reverse=True,
    )
    return {
        "candidate_count": len(candidate_list),
        "matched_rule_outcome_count": len(all_returns),
        "summary": summarize_returns(all_returns),
        "rules": rules,
        "returns_pct": all_returns,
    }


def day_key_for_outcome(outcome: dict[str, Any], snapshot: dict[str, Any] | None) -> str:
    for source in (outcome, snapshot or {}):
        for key in ("exit_time_ms", "observed_at_ms", "entry_time_ms"):
            value = safe_float(source.get(key))
            if value is not None and value > 0:
                return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc).date().isoformat()
    return "unknown"


def max_share_pct(values: list[str]) -> float:
    if not values:
        return 0.0
    counts = Counter(values)
    return round6((max(counts.values()) / len(values)) * 100.0) or 0.0


def concentration_payload(matches: list[dict[str, Any]]) -> dict[str, Any]:
    symbols = [str(row.get("symbol") or "unknown") for row in matches]
    sessions = [str(row.get("trading_session") or "unknown") for row in matches]
    days = [str(row.get("day") or "unknown") for row in matches]
    return {
        "sample_count": len(matches),
        "unique_symbol_count": len(set(symbols)),
        "unique_session_count": len(set(sessions)),
        "unique_day_count": len(set(days)),
        "max_symbol_share_pct": max_share_pct(symbols),
        "max_session_share_pct": max_share_pct(sessions),
        "max_day_share_pct": max_share_pct(days),
    }


def return_components(values: Iterable[float]) -> dict[str, Any]:
    rows = [float(value) for value in values if math.isfinite(float(value))]
    gross_profit = sum(value for value in rows if value > 0)
    gross_loss_abs = abs(sum(value for value in rows if value < 0))
    return {
        "sample_count": len(rows),
        "gross_profit_pct": round6(gross_profit),
        "gross_loss_abs_pct": round6(gross_loss_abs),
    }


def return_components_from_summary(summary: dict[str, Any]) -> dict[str, Any]:
    sample_count = int(summary.get("sample_count") or 0)
    total_return = safe_float(summary.get("total_return_pct"))
    if total_return is None:
        avg_return = safe_float(summary.get("avg_return_pct"))
        total_return = (avg_return or 0.0) * sample_count
    profit_factor = safe_float(summary.get("profit_factor"))
    win_count = int(summary.get("win_count") or 0)
    loss_count = int(summary.get("loss_count") or 0)
    if sample_count <= 0:
        return {"sample_count": 0, "gross_profit_pct": 0.0, "gross_loss_abs_pct": 0.0}
    if profit_factor is None:
        if loss_count == 0 and total_return > 0:
            return {
                "sample_count": sample_count,
                "gross_profit_pct": round6(total_return),
                "gross_loss_abs_pct": 0.0,
            }
        if win_count == 0 and total_return < 0:
            return {
                "sample_count": sample_count,
                "gross_profit_pct": 0.0,
                "gross_loss_abs_pct": round6(abs(total_return)),
            }
        return {
            "sample_count": sample_count,
            "gross_profit_pct": None,
            "gross_loss_abs_pct": None,
        }
    if profit_factor >= 999999.0:
        return {
            "sample_count": sample_count,
            "gross_profit_pct": round6(max(0.0, total_return)),
            "gross_loss_abs_pct": 0.0,
        }
    if abs(profit_factor - 1.0) <= 1e-12:
        return {
            "sample_count": sample_count,
            "gross_profit_pct": None,
            "gross_loss_abs_pct": None,
        }
    gross_loss_abs = total_return / (profit_factor - 1.0)
    if gross_loss_abs < 0:
        gross_loss_abs = abs(gross_loss_abs)
    gross_profit = profit_factor * gross_loss_abs
    return {
        "sample_count": sample_count,
        "gross_profit_pct": round6(gross_profit),
        "gross_loss_abs_pct": round6(gross_loss_abs),
    }


def summarize_summary_components(
    summaries: Iterable[dict[str, Any]],
    components: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    summary_list = [summary for summary in summaries if isinstance(summary, dict)]
    component_list = [component for component in components if isinstance(component, dict)]
    sample_count = sum(int(summary.get("sample_count") or 0) for summary in summary_list)
    win_count = sum(int(summary.get("win_count") or 0) for summary in summary_list)
    loss_count = sum(int(summary.get("loss_count") or 0) for summary in summary_list)
    total_return = 0.0
    for summary in summary_list:
        summary_sample_count = int(summary.get("sample_count") or 0)
        value = safe_float(summary.get("total_return_pct"))
        if value is None:
            value = (safe_float(summary.get("avg_return_pct")) or 0.0) * summary_sample_count
        total_return += value
    gross_profit_values = [safe_float(component.get("gross_profit_pct")) for component in component_list]
    gross_loss_values = [safe_float(component.get("gross_loss_abs_pct")) for component in component_list]
    has_exact_pf = len(component_list) == len(summary_list) and all(value is not None for value in gross_profit_values + gross_loss_values)
    gross_profit = sum(float(value) for value in gross_profit_values if value is not None)
    gross_loss_abs = sum(float(value) for value in gross_loss_values if value is not None)
    profit_factor = None
    if has_exact_pf:
        if gross_loss_abs > 0:
            profit_factor = gross_profit / gross_loss_abs
        elif gross_profit > 0:
            profit_factor = 999999.0
    return {
        "sample_count": sample_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate_pct": round6((win_count / sample_count) * 100.0 if sample_count else None),
        "avg_return_pct": round6(total_return / sample_count if sample_count else None),
        "total_return_pct": round6(total_return),
        "profit_factor": round6(profit_factor),
        "sharpe_ratio": None,
        "max_drawdown_pct": None,
        "aggregation_quality": "exact_from_components" if has_exact_pf else "summary_only",
    }


def rule_fold_passes(
    summary: dict[str, Any],
    *,
    min_samples: int,
    min_profit_factor: float,
    min_avg_return_pct: float,
) -> bool:
    return (
        int(summary.get("sample_count") or 0) >= max(1, int(min_samples))
        and (safe_float(summary.get("avg_return_pct")) or -999999.0) > float(min_avg_return_pct)
        and (safe_float(summary.get("profit_factor")) or 0.0) >= float(min_profit_factor)
    )


def rejection_reasons_for_rule(
    row: dict[str, Any],
    *,
    min_oos_samples: int,
    min_profit_factor: float,
    min_avg_return_pct: float,
    min_positive_folds: int,
    min_tested_folds: int,
    max_symbol_share_pct: float,
    max_session_share_pct: float,
    max_day_share_pct: float,
) -> list[str]:
    summary = row.get("oos_summary") if isinstance(row.get("oos_summary"), dict) else {}
    concentration = row.get("concentration") if isinstance(row.get("concentration"), dict) else {}
    reasons: list[str] = []
    if int(summary.get("sample_count") or 0) < max(1, int(min_oos_samples)):
        reasons.append("oos_sample_too_small")
    if (safe_float(summary.get("avg_return_pct")) or -999999.0) <= float(min_avg_return_pct):
        reasons.append("oos_avg_return_not_positive")
    if (safe_float(summary.get("profit_factor")) or 0.0) < float(min_profit_factor):
        reasons.append("oos_profit_factor_below_gate")
    if int(row.get("positive_fold_count") or 0) < max(1, int(min_positive_folds)):
        reasons.append("positive_fold_count_below_gate")
    if int(row.get("tested_fold_count") or 0) < max(1, int(min_tested_folds)):
        reasons.append("tested_fold_count_below_gate")
    if (safe_float(concentration.get("max_symbol_share_pct")) or 0.0) > float(max_symbol_share_pct):
        reasons.append("symbol_concentration_too_high")
    if (safe_float(concentration.get("max_session_share_pct")) or 0.0) > float(max_session_share_pct):
        reasons.append("session_concentration_too_high")
    if (safe_float(concentration.get("max_day_share_pct")) or 0.0) > float(max_day_share_pct):
        reasons.append("day_concentration_too_high")
    return reasons


def build_survivor_rules_report(
    walk_forward_report: dict[str, Any],
    *,
    min_oos_samples: int = 100,
    min_profit_factor: float = 1.2,
    min_avg_return_pct: float = 0.0,
    min_positive_folds: int = 2,
    min_tested_folds: int = 2,
    max_symbol_share_pct: float = 70.0,
    max_session_share_pct: float = 80.0,
    max_day_share_pct: float = 60.0,
) -> dict[str, Any]:
    aggregated: dict[str, dict[str, Any]] = {}
    for fold in walk_forward_report.get("folds") or []:
        fold_index = int(fold.get("fold_index") or 0)
        for result in fold.get("attribution_candidate_rule_results") or []:
            if not isinstance(result, dict):
                continue
            rule_id = str(result.get("rule_id") or "").strip()
            if not rule_id:
                continue
            summary = result.get("test_summary") if isinstance(result.get("test_summary"), dict) else {}
            sample_count = int(summary.get("sample_count") or 0)
            if sample_count <= 0:
                continue
            row = aggregated.setdefault(
                rule_id,
                {
                    "rule_id": rule_id,
                    "rule_type": result.get("rule_type"),
                    "branch_id": result.get("branch_id"),
                    "horizon_sec": result.get("horizon_sec"),
                    "conditions": result.get("conditions"),
                    "folds": [],
                    "_summaries": [],
                    "_components": [],
                    "_max_symbol_share_pct": 0.0,
                    "_max_session_share_pct": 0.0,
                    "_max_day_share_pct": 0.0,
                },
            )
            row["folds"].append({"fold_index": fold_index, **summary})
            row["_summaries"].append(summary)
            components = result.get("return_components") if isinstance(result.get("return_components"), dict) else {}
            row["_components"].append(components or return_components_from_summary(summary))
            concentration = result.get("concentration") if isinstance(result.get("concentration"), dict) else {}
            row["_max_symbol_share_pct"] = max(
                float(row.get("_max_symbol_share_pct") or 0.0),
                safe_float(concentration.get("max_symbol_share_pct")) or 0.0,
            )
            row["_max_session_share_pct"] = max(
                float(row.get("_max_session_share_pct") or 0.0),
                safe_float(concentration.get("max_session_share_pct")) or 0.0,
            )
            row["_max_day_share_pct"] = max(
                float(row.get("_max_day_share_pct") or 0.0),
                safe_float(concentration.get("max_day_share_pct")) or 0.0,
            )
    rows: list[dict[str, Any]] = []
    fold_min_samples = max(1, int(min_oos_samples / max(1, int(min_positive_folds))))
    for row in aggregated.values():
        summaries = list(row.pop("_summaries", []))
        components = list(row.pop("_components", []))
        row["oos_summary"] = summarize_summary_components(summaries, components)
        row["tested_fold_count"] = len(row.get("folds") or [])
        row["positive_fold_count"] = sum(
            1
            for fold in row.get("folds") or []
            if rule_fold_passes(
                fold,
                min_samples=fold_min_samples,
                min_profit_factor=min_profit_factor,
                min_avg_return_pct=min_avg_return_pct,
            )
        )
        row["concentration"] = {
            "max_symbol_share_pct": round6(row.pop("_max_symbol_share_pct", 0.0)),
            "max_session_share_pct": round6(row.pop("_max_session_share_pct", 0.0)),
            "max_day_share_pct": round6(row.pop("_max_day_share_pct", 0.0)),
        }
        row["rejection_reasons"] = rejection_reasons_for_rule(
            row,
            min_oos_samples=min_oos_samples,
            min_profit_factor=min_profit_factor,
            min_avg_return_pct=min_avg_return_pct,
            min_positive_folds=min_positive_folds,
            min_tested_folds=min_tested_folds,
            max_symbol_share_pct=max_symbol_share_pct,
            max_session_share_pct=max_session_share_pct,
            max_day_share_pct=max_day_share_pct,
        )
        rows.append(row)
    rows.sort(
        key=lambda item: (
            not item.get("rejection_reasons"),
            safe_float((item.get("oos_summary") or {}).get("profit_factor")) or 0.0,
            safe_float((item.get("oos_summary") or {}).get("avg_return_pct")) or -999999.0,
            int((item.get("oos_summary") or {}).get("sample_count") or 0),
        ),
        reverse=True,
    )
    survivors = [row for row in rows if not row.get("rejection_reasons")]
    rejected = [row for row in rows if row.get("rejection_reasons")]
    return {
        "generated_at": utc_now_iso(),
        "report_type": "phoenix_survivor_rules_report",
        "research_only": True,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "survivor_count": len(survivors),
        "rejected_count": len(rejected),
        "policy": {
            "min_oos_samples": max(1, int(min_oos_samples)),
            "min_profit_factor": float(min_profit_factor),
            "min_avg_return_pct": float(min_avg_return_pct),
            "min_positive_folds": max(1, int(min_positive_folds)),
            "min_tested_folds": max(1, int(min_tested_folds)),
            "max_symbol_share_pct": float(max_symbol_share_pct),
            "max_session_share_pct": float(max_session_share_pct),
            "max_day_share_pct": float(max_day_share_pct),
        },
        "survivor_rules": survivors,
        "rejected_rules": rejected,
    }


def survivor_gate_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "min_oos_samples": max(1, int(args.survivor_min_oos_samples)),
        "min_profit_factor": float(args.survivor_min_profit_factor),
        "min_avg_return_pct": float(args.survivor_min_avg_return_pct),
        "min_positive_folds": max(1, int(args.survivor_min_positive_folds)),
        "min_tested_folds": max(1, int(args.survivor_min_tested_folds)),
        "max_symbol_share_pct": float(args.survivor_max_symbol_share_pct),
        "max_session_share_pct": float(args.survivor_max_session_share_pct),
        "max_day_share_pct": float(args.survivor_max_day_share_pct),
    }


def survivor_rules_payload(gate_report: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in gate_report.items()
        if key not in {"rejected_rules"}
    }


def rejected_rules_payload(gate_report: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": gate_report.get("generated_at"),
        "report_type": "phoenix_rejected_rules_report",
        "research_only": True,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "survivor_count": int(gate_report.get("survivor_count") or 0),
        "rejected_count": int(gate_report.get("rejected_count") or 0),
        "policy": gate_report.get("policy") or {},
        "rejected_rules": gate_report.get("rejected_rules") or [],
    }


def write_survivor_gate_reports(
    output_dir: Path,
    gate_report: dict[str, Any],
    *,
    source_walk_forward_path: Path | None = None,
) -> dict[str, Path]:
    source_files = {}
    if source_walk_forward_path is not None:
        source_files["walk_forward_oos_report"] = str(source_walk_forward_path)
    if source_files:
        gate_report["source_files"] = source_files
    survivor_path = output_dir / "survivor_rules_report.json"
    rejected_path = output_dir / "rejected_rules_report.json"
    survivor_payload = survivor_rules_payload(gate_report)
    rejected_payload = rejected_rules_payload(gate_report)
    if source_files:
        survivor_payload["source_files"] = source_files
        rejected_payload["source_files"] = source_files
    write_json(survivor_path, survivor_payload)
    write_json(rejected_path, rejected_payload)
    return {
        "survivor_rules_report": survivor_path,
        "rejected_rules_report": rejected_path,
    }


def run_survivor_gate_from_walk_forward(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    walk_forward_path = Path(args.input_walk_forward_json)
    walk_forward = json.loads(walk_forward_path.read_text(encoding="utf-8"))
    gate_report = build_survivor_rules_report(walk_forward, **survivor_gate_kwargs(args))
    report_paths = write_survivor_gate_reports(
        output_dir,
        gate_report,
        source_walk_forward_path=walk_forward_path,
    )
    manifest = {
        "generated_at": utc_now_iso(),
        "report_type": "phoenix_survivor_gate",
        "research_only": True,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "input_walk_forward_oos_report": str(walk_forward_path),
        "counts": {
            "survivor_rules": int(gate_report.get("survivor_count") or 0),
            "rejected_rules": int(gate_report.get("rejected_count") or 0),
        },
        "outputs": {key: str(value) for key, value in report_paths.items()},
    }
    write_json(output_dir / "survivor_gate_manifest.json", manifest)
    return manifest


def build_walk_forward_oos_report(
    snapshots: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    *,
    fold_count: int = 4,
    min_train_events: int = 200,
    min_samples: int = 30,
    pair_min_samples: int = 20,
    top_n: int = 10,
    candidate_min_profit_factor: float = 1.2,
    candidate_min_avg_return_pct: float = 0.0,
) -> dict[str, Any]:
    snapshots_list = sorted(list(snapshots), key=event_time_ms)
    outcomes_list = list(outcomes)
    if not snapshots_list:
        return {
            "generated_at": utc_now_iso(),
            "mode": "walk_forward_oos",
            "fold_count": 0,
            "folds": [],
            "aggregate": summarize_returns([]),
            "notes": ["No snapshots were available for walk-forward evaluation."],
        }
    fold_count = max(2, int(fold_count))
    chunk_size = max(1, math.ceil(len(snapshots_list) / fold_count))
    folds: list[dict[str, Any]] = []
    all_oos_returns: list[float] = []
    all_predefined_returns: list[float] = []
    all_attribution_returns: list[float] = []
    for fold_index in range(1, fold_count):
        train = snapshots_list[: fold_index * chunk_size]
        test = snapshots_list[fold_index * chunk_size : (fold_index + 1) * chunk_size]
        if len(train) < max(1, int(min_train_events)) or not test:
            continue
        train_ids = {str(row.get("event_id")) for row in train}
        test_ids = {str(row.get("event_id")) for row in test}
        train_outcomes = [row for row in outcomes_list if str(row.get("source_event_id")) in train_ids]
        test_outcomes = [row for row in outcomes_list if str(row.get("source_event_id")) in test_ids]
        train_report = build_factor_factory_report(
            snapshots=train,
            outcomes=train_outcomes,
            min_samples=max(1, int(min_samples)),
            pair_min_samples=max(1, int(pair_min_samples)),
            top_n=max(1, int(top_n)),
        )
        selected_rules = [
            str(row.get("rule"))
            for row in train_report.get("ranked_candidate_rules", [])
            if int(row.get("sample_count") or 0) >= max(1, int(min_samples))
            and (safe_float(row.get("avg_return_pct")) or 0.0) > 0
            and (safe_float(row.get("profit_factor")) or 0.0) >= 1.0
        ][: max(1, int(top_n))]
        selected_attribution_rules = derive_attribution_candidate_rules(
            train_report,
            min_samples=max(1, int(min_samples)),
            min_profit_factor=float(candidate_min_profit_factor),
            min_avg_return_pct=float(candidate_min_avg_return_pct),
            top_n=max(1, int(top_n)),
        )
        test_rows = rule_evaluation_rows(snapshots=test, outcomes=test_outcomes, branch_type=RESEARCH_BRANCH_TYPE)
        predefined_returns = matching_rule_returns(test_rows, set(selected_rules)) if selected_rules else []
        attribution_eval = evaluate_candidate_rules(test, test_outcomes, selected_attribution_rules)
        attribution_returns = list(attribution_eval.get("returns_pct") or [])
        test_returns = predefined_returns + attribution_returns
        all_predefined_returns.extend(predefined_returns)
        all_attribution_returns.extend(attribution_returns)
        all_oos_returns.extend(test_returns)
        folds.append(
            {
                "fold_index": fold_index,
                "train_event_count": len(train),
                "test_event_count": len(test),
                "train_start_ms": event_time_ms(train[0]),
                "train_end_ms": event_time_ms(train[-1]),
                "test_start_ms": event_time_ms(test[0]),
                "test_end_ms": event_time_ms(test[-1]),
                "selected_rules": selected_rules,
                "selected_attribution_rules": selected_attribution_rules,
                "train_top_rules": train_report.get("ranked_candidate_rules", [])[: max(1, int(top_n))],
                "train_top_attribution_slices": (train_report.get("factor_attribution") or {}).get("top_pair_factor_slices", [])[: max(1, int(top_n))],
                "predefined_rule_summary": summarize_returns(predefined_returns),
                "attribution_candidate_summary": attribution_eval["summary"],
                "attribution_candidate_rule_results": attribution_eval["rules"],
                "test_summary": summarize_returns(test_returns),
            }
        )
    return {
        "generated_at": utc_now_iso(),
        "mode": "walk_forward_oos",
        "policy": {
            "train_before_test_only": True,
            "no_same_fold_discovery_and_validation": True,
            "includes_attribution_derived_candidate_rules": True,
            "live_trading_enabled": False,
        },
        "candidate_policy": {
            "min_samples": max(1, int(min_samples)),
            "min_profit_factor": float(candidate_min_profit_factor),
            "min_avg_return_pct": float(candidate_min_avg_return_pct),
            "top_n": max(1, int(top_n)),
        },
        "input_counts": {
            "snapshots": len(snapshots_list),
            "outcomes": len(outcomes_list),
        },
        "fold_count": len(folds),
        "folds": folds,
        "aggregate": summarize_returns(all_oos_returns),
        "predefined_rule_aggregate": summarize_returns(all_predefined_returns),
        "attribution_candidate_aggregate": summarize_returns(all_attribution_returns),
    }


def fetch_symbol_research_history(
    client: BinancePublicBackfillClient,
    *,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    chunk_hours: int,
    fetch_open_interest: bool = True,
) -> tuple[list[list[Any]], list[dict[str, Any]]]:
    candle_rows: list[dict[str, Any]] = []
    oi_rows: list[dict[str, Any]] = []
    for chunk_start_ms, chunk_end_ms in build_history_chunks(start_ms, end_ms, chunk_hours=max(1, int(chunk_hours))):
        candle_rows.extend(
            client.fetch_klines(
                symbol=symbol,
                interval=interval,
                start_ms=chunk_start_ms,
                end_ms=chunk_end_ms,
            )
        )
        if fetch_open_interest:
            oi_rows.extend(
                client.fetch_open_interest_hist(
                    symbol=symbol,
                    period="5m",
                    start_ms=chunk_start_ms,
                    end_ms=chunk_end_ms,
                )
            )
    candle_rows = dedupe_sort_rows(candle_rows, "open_time")
    oi_rows = dedupe_sort_rows(oi_rows, "timestamp")
    candles = [[row.get(column) for column in KLINE_COLUMNS] for row in candle_rows if isinstance(row, dict)]
    return candles, oi_rows


def selected_symbols_from_args(args: argparse.Namespace, client: BinancePublicBackfillClient) -> list[str]:
    explicit = parse_symbol_list(",".join(part for part in (args.symbol, args.symbols) if part))
    if explicit:
        return explicit
    ticker_payload = read_json_payload(args.ticker_24hr_json)
    if ticker_payload is None:
        ticker_payload = client._get_json("/fapi/v1/ticker/24hr", {})
    universe = discover_universe(
        ticker_payload if isinstance(ticker_payload, list) else [],
        top_limit=max(1, int(args.universe_top)),
        min_quote_volume=max(0.0, float(args.min_quote_volume or 0.0)),
        sort_by="quote_volume",
    )
    return [str(item.get("symbol")) for item in universe if item.get("symbol")]


def build_client(args: argparse.Namespace) -> BinancePublicBackfillClient:
    proxy = resolve_proxy()
    if str(args.data_source) == "public-zip":
        return BinancePublicZipBackfillClient(
            proxy=proxy,
            timeout_sec=45,
            pause_sec=0.35,
            max_retries=8,
            cache_dir=args.public_zip_cache_dir,
            offline_cache_only=bool(args.offline_cache_only),
        )
    return BinancePublicBackfillClient(
        proxy=proxy,
        timeout_sec=20,
        pause_sec=0.15,
        max_retries=4,
    )


def run_replay(args: argparse.Namespace) -> dict[str, Any]:
    horizons_sec = parse_horizon_seconds(args.horizons)
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    all_events: list[dict[str, Any]] = []
    all_factors: list[dict[str, Any]] = []
    all_outcomes: list[dict[str, Any]] = []
    symbols_processed = 0
    symbol_errors: list[dict[str, Any]] = []

    if args.input_event_samples_jsonl is not None and args.input_labeled_outcomes_jsonl is not None:
        all_events.extend(read_jsonl_records(args.input_event_samples_jsonl))
        if args.input_factor_vectors_jsonl is not None and args.input_factor_vectors_jsonl.exists():
            all_factors.extend(read_jsonl_records(args.input_factor_vectors_jsonl))
        else:
            all_factors.extend(build_factor_vector_record(row) for row in all_events)
        all_outcomes.extend(read_jsonl_records(args.input_labeled_outcomes_jsonl))
        symbols_processed = len({str(row.get("symbol") or "") for row in all_events if row.get("symbol")})
    elif args.input_klines_jsonl is not None:
        symbol = str(args.symbol or args.symbols or "UNKNOWN").strip().upper() or "UNKNOWN"
        rows = build_historical_research_rows(
            symbol=symbol,
            candles=read_klines_jsonl(args.input_klines_jsonl),
            oi_rows=read_oi_jsonl(args.input_oi_jsonl),
            interval=str(args.interval),
            horizons_sec=horizons_sec,
            round_trip_fee_bps=float(args.round_trip_fee_bps),
            quote_allocation_usdt=float(args.quote_allocation),
            min_quote_volume=float(args.min_quote_volume),
            rolling_24h_bars=args.rolling_24h_bars,
        )
        all_events.extend(rows.event_samples)
        all_factors.extend(rows.factor_vectors)
        all_outcomes.extend(rows.labeled_outcomes)
        symbols_processed = 1
    else:
        client = build_client(args)
        start_date = parse_utc_date(args.start_date)
        end_date = parse_utc_date(args.end_date)
        start_dt, end_dt = resolve_backtest_window(
            days=max(1, int(args.days)),
            start_date=start_date,
            end_date=end_date,
        )
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        symbols = selected_symbols_from_args(args, client)
        for index, symbol in enumerate(symbols, start=1):
            try:
                candles, oi_rows = fetch_symbol_research_history(
                    client,
                    symbol=symbol,
                    interval=str(args.interval),
                    start_ms=start_ms,
                    end_ms=end_ms,
                    chunk_hours=max(1, int(args.history_chunk_hours)),
                    fetch_open_interest=not bool(args.skip_open_interest),
                )
                rows = build_historical_research_rows(
                    symbol=symbol,
                    candles=candles,
                    oi_rows=oi_rows,
                    interval=str(args.interval),
                    horizons_sec=horizons_sec,
                    round_trip_fee_bps=float(args.round_trip_fee_bps),
                    quote_allocation_usdt=float(args.quote_allocation),
                    min_quote_volume=float(args.min_quote_volume),
                    rolling_24h_bars=args.rolling_24h_bars,
                )
                all_events.extend(rows.event_samples)
                all_factors.extend(rows.factor_vectors)
                all_outcomes.extend(rows.labeled_outcomes)
                symbols_processed += 1
                print(
                    json.dumps(
                        {
                            "ts": utc_now_iso(),
                            "event": "historical_research_symbol_complete",
                            "symbol": symbol,
                            "ordinal": index,
                            "events": len(rows.event_samples),
                            "outcomes": len(rows.labeled_outcomes),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                time.sleep(max(0.0, float(args.symbol_pause_sec or 0.0)))
            except Exception as exc:  # noqa: BLE001
                symbol_errors.append({"symbol": symbol, "error": str(exc), "ordinal": index})
                print(
                    json.dumps(
                        {
                            "ts": utc_now_iso(),
                            "event": "historical_research_symbol_error",
                            "symbol": symbol,
                            "error": str(exc),
                            "ordinal": index,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )

    event_samples_path = output_dir / "historical_event_samples.jsonl"
    factor_vectors_path = output_dir / "historical_factor_vectors.jsonl"
    labeled_outcomes_path = output_dir / "historical_labeled_outcomes.jsonl"
    attribution_path = output_dir / "historical_attribution_report.json"
    candidate_rules_path = output_dir / "attribution_candidate_rules_report.json"
    control_path = output_dir / "historical_factor_control.json"
    walk_forward_path = output_dir / "walk_forward_oos_report.json"

    event_count = write_jsonl(event_samples_path, all_events)
    factor_count = write_jsonl(factor_vectors_path, all_factors)
    outcome_count = write_jsonl(labeled_outcomes_path, all_outcomes)
    attribution = build_factor_factory_report(
        snapshots=all_events,
        outcomes=all_outcomes,
        min_samples=max(1, int(args.min_samples)),
        pair_min_samples=max(1, int(args.pair_min_samples)),
        top_n=max(1, int(args.top)),
    )
    attribution["source_files"] = {
        "snapshots_file": str(event_samples_path),
        "outcomes_file": str(labeled_outcomes_path),
        "factor_vectors_file": str(factor_vectors_path),
    }
    attribution["research_only"] = True
    attribution["live_trading_enabled"] = False
    write_json(attribution_path, attribution)
    candidate_rules = derive_attribution_candidate_rules(
        attribution,
        min_samples=max(1, int(args.min_samples)),
        min_profit_factor=float(args.candidate_min_profit_factor),
        min_avg_return_pct=float(args.candidate_min_avg_return_pct),
        top_n=max(1, int(args.top)),
    )
    write_json(
        candidate_rules_path,
        {
            "generated_at": utc_now_iso(),
            "report_type": "phoenix_attribution_candidate_rules",
            "research_only": True,
            "live_trading_enabled": False,
            "advisory_only": True,
            "advisory_reason": "Rules in this file are derived from the full attribution sample; use walk_forward_oos_report.json for validation.",
            "policy": {
                "min_samples": max(1, int(args.min_samples)),
                "min_profit_factor": float(args.candidate_min_profit_factor),
                "min_avg_return_pct": float(args.candidate_min_avg_return_pct),
                "top_n": max(1, int(args.top)),
            },
            "candidate_rules": candidate_rules,
        },
    )
    factor_control = build_factor_factory_control(
        attribution,
        min_outcomes=max(1, int(args.min_samples)),
    )
    factor_control["source_report"] = str(attribution_path)
    write_json(control_path, factor_control)

    walk_forward = build_walk_forward_oos_report(
        all_events,
        all_outcomes,
        fold_count=max(2, int(args.walk_forward_folds)),
        min_train_events=max(1, int(args.min_train_events)),
        min_samples=max(1, int(args.min_samples)),
        pair_min_samples=max(1, int(args.pair_min_samples)),
        top_n=max(1, int(args.top)),
        candidate_min_profit_factor=float(args.candidate_min_profit_factor),
        candidate_min_avg_return_pct=float(args.candidate_min_avg_return_pct),
    )
    walk_forward["source_files"] = attribution["source_files"]
    write_json(walk_forward_path, walk_forward)
    survivor_gate = build_survivor_rules_report(walk_forward, **survivor_gate_kwargs(args))
    survivor_paths = write_survivor_gate_reports(
        output_dir,
        survivor_gate,
        source_walk_forward_path=walk_forward_path,
    )

    manifest = {
        "generated_at": utc_now_iso(),
        "report_type": "phoenix_historical_research_replay",
        "research_only": True,
        "live_trading_enabled": False,
        "symbols_processed": symbols_processed,
        "symbol_errors": symbol_errors,
        "counts": {
            "event_samples": event_count,
            "factor_vectors": factor_count,
            "labeled_outcomes": outcome_count,
        },
        "config": {
            "interval": str(args.interval),
            "horizons_sec": horizons_sec,
            "round_trip_fee_bps": float(args.round_trip_fee_bps),
            "quote_allocation": float(args.quote_allocation),
            "min_quote_volume": float(args.min_quote_volume),
            "data_source": str(args.data_source),
            "universe_top": int(args.universe_top),
        },
        "outputs": {
            "historical_event_samples": str(event_samples_path),
            "historical_factor_vectors": str(factor_vectors_path),
            "historical_labeled_outcomes": str(labeled_outcomes_path),
            "historical_attribution_report": str(attribution_path),
            "attribution_candidate_rules_report": str(candidate_rules_path),
            "historical_factor_control": str(control_path),
            "walk_forward_oos_report": str(walk_forward_path),
            "survivor_rules_report": str(survivor_paths["survivor_rules_report"]),
            "rejected_rules_report": str(survivor_paths["rejected_rules_report"]),
        },
    }
    write_json(output_dir / "historical_research_replay_manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phoenix historical research replay and labeling engine.")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--universe-top", type=int, default=50)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--interval", default="1m", choices=["1m", "5m"])
    parser.add_argument("--horizons", default="1m,5m,15m,1h")
    parser.add_argument("--min-quote-volume", type=float, default=5_000_000.0)
    parser.add_argument("--quote-allocation", type=float, default=10.0)
    parser.add_argument("--round-trip-fee-bps", type=float, default=8.0)
    parser.add_argument("--data-source", default="public-zip", choices=["public-zip", "rest"])
    parser.add_argument("--public-zip-cache-dir", type=Path, default=DEFAULT_PUBLIC_ZIP_CACHE_DIR)
    parser.add_argument("--offline-cache-only", action="store_true")
    parser.add_argument("--skip-open-interest", action="store_true")
    parser.add_argument("--ticker-24hr-json", type=Path, default=None)
    parser.add_argument("--history-chunk-hours", type=int, default=24)
    parser.add_argument("--symbol-pause-sec", type=float, default=0.0)
    parser.add_argument("--input-klines-jsonl", type=Path, default=None)
    parser.add_argument("--input-oi-jsonl", type=Path, default=None)
    parser.add_argument("--input-event-samples-jsonl", type=Path, default=None)
    parser.add_argument("--input-factor-vectors-jsonl", type=Path, default=None)
    parser.add_argument("--input-labeled-outcomes-jsonl", type=Path, default=None)
    parser.add_argument("--input-walk-forward-json", type=Path, default=None)
    parser.add_argument("--rolling-24h-bars", type=int, default=None)
    parser.add_argument("--min-samples", type=int, default=30)
    parser.add_argument("--pair-min-samples", type=int, default=20)
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--walk-forward-folds", type=int, default=4)
    parser.add_argument("--min-train-events", type=int, default=200)
    parser.add_argument("--candidate-min-profit-factor", type=float, default=1.2)
    parser.add_argument("--candidate-min-avg-return-pct", type=float, default=0.0)
    parser.add_argument("--survivor-min-oos-samples", type=int, default=100)
    parser.add_argument("--survivor-min-profit-factor", type=float, default=1.2)
    parser.add_argument("--survivor-min-avg-return-pct", type=float, default=0.0)
    parser.add_argument("--survivor-min-positive-folds", type=int, default=2)
    parser.add_argument("--survivor-min-tested-folds", type=int, default=2)
    parser.add_argument("--survivor-max-symbol-share-pct", type=float, default=70.0)
    parser.add_argument("--survivor-max-session-share-pct", type=float, default=80.0)
    parser.add_argument("--survivor-max-day-share-pct", type=float, default=60.0)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.input_walk_forward_json is not None:
        manifest = run_survivor_gate_from_walk_forward(args)
        print(
            "survivor_gate_completed "
            f"survivors={manifest['counts']['survivor_rules']} "
            f"rejected={manifest['counts']['rejected_rules']} "
            f"output_dir={manifest['outputs']['survivor_rules_report']}",
            flush=True,
        )
        return 0
    manifest = run_replay(args)
    print(
        "historical_research_replay_completed "
        f"events={manifest['counts']['event_samples']} "
        f"factors={manifest['counts']['factor_vectors']} "
        f"outcomes={manifest['counts']['labeled_outcomes']} "
        f"output_dir={manifest['outputs']['historical_event_samples']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
