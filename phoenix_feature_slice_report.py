from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

from phoenix_factor_library import build_factor_vector


UNKNOWN = "unknown"
DEFAULT_BRANCH_TYPE = "research_pool"
OUTCOME_EVENT_NAME = "signal_bridge_shadow_horizon_result"
SNAPSHOT_EVENT_NAME = "market_event_created"


CATEGORICAL_FEATURES = [
    "bar_interval",
    "trading_session",
    "trigger_signature",
    "sample_type",
    "candle_direction",
    "trigger_candle_direction",
    "confirmation_candle_direction",
    "reversal_confirmation_passed",
    "derivatives_data_source",
    "direction_variant",
    "side",
    "oi_5m_regime",
    "oi_15m_regime",
    "btc_5m_regime",
    "depth_imbalance_regime",
    "liquidation_pressure",
    "liquidation_side_bias",
    "volume_burst_regime",
    "range_expansion_regime",
    "momentum_bucket",
    "trend_bucket",
    "mean_reversion_bucket",
    "volatility_regime_bucket",
    "liquidity_bucket",
    "flow_bucket",
    "oi_build_bucket",
    "oi_unwind_bucket",
    "crowding_bucket",
    "market_regime_bucket",
    "microstructure_bucket",
]

NUMERIC_FEATURES = [
    "trigger_score",
    "quote_volume_24h",
    "price_change_24h_pct",
    "volume_burst_ratio",
    "range_to_atr",
    "body_to_atr",
    "ret_1bar_pct",
    "ret_5bar_pct",
    "ret_15bar_pct",
    "ret_60bar_pct",
    "oi_change_5m_pct",
    "oi_change_15m_pct",
    "funding_rate",
    "depth_imbalance",
    "spread_bps",
    "estimated_slippage_bps",
    "btcusdt_ret_5m_pct",
    "btcusdt_ret_1h_pct",
    "ethusdt_ret_5m_pct",
    "ethusdt_ret_1h_pct",
    "liquidation_long_usd_15m",
    "liquidation_short_usd_15m",
    "liquidation_event_count_15m",
    "context_3m_volume_burst_ratio",
    "context_3m_range_to_atr",
    "context_3m_ret_1bar_pct",
    "context_3m_ret_5bar_pct",
    "context_3m_ret_15bar_pct",
    "momentum_score",
    "trend_score",
    "mean_reversion_score",
    "volatility_regime_score",
    "liquidity_score",
    "flow_score",
    "oi_build_score",
    "oi_unwind_score",
    "crowding_score",
    "market_regime_score",
    "microstructure_score",
]

PAIR_FEATURES = [
    "bar_interval",
    "trading_session",
    "trigger_signature",
    "candle_direction",
    "direction_variant",
    "side",
    "oi_5m_regime",
    "btc_5m_regime",
    "depth_imbalance_regime",
    "liquidation_pressure",
    "liquidation_side_bias",
    "volume_burst_regime",
    "range_expansion_regime",
    "oi_change_5m_pct",
    "oi_change_15m_pct",
    "depth_imbalance",
    "volume_burst_ratio",
    "range_to_atr",
    "btcusdt_ret_5m_pct",
    "liquidation_long_usd_15m",
    "liquidation_short_usd_15m",
    "liquidation_event_count_15m",
    "trend_bucket",
    "mean_reversion_bucket",
    "liquidity_bucket",
    "flow_bucket",
    "oi_build_bucket",
    "oi_unwind_bucket",
]

PCT_BUCKETS = [-math.inf, -5.0, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 5.0, math.inf]
RATIO_BUCKETS = [-math.inf, 0.8, 1.0, 1.5, 2.0, 3.0, 5.0, math.inf]
IMBALANCE_BUCKETS = [-math.inf, -0.55, -0.35, -0.15, 0.0, 0.15, 0.35, 0.55, math.inf]
BPS_BUCKETS = [-math.inf, 2.0, 5.0, 10.0, 20.0, 50.0, math.inf]
FUNDING_BUCKETS = [-math.inf, -0.001, -0.0002, 0.0, 0.0002, 0.001, math.inf]
QUOTE_VOLUME_BUCKETS = [-math.inf, 10_000_000.0, 50_000_000.0, 100_000_000.0, 500_000_000.0, 1_000_000_000.0, math.inf]
LIQUIDATION_USD_BUCKETS = [-math.inf, 1.0, 10_000.0, 50_000.0, 100_000.0, 500_000.0, 1_000_000.0, math.inf]
COUNT_BUCKETS = [-math.inf, 1.0, 3.0, 10.0, 30.0, 100.0, math.inf]
SCORE_BUCKETS = [-math.inf, -0.7, -0.35, -0.1, 0.1, 0.35, 0.7, math.inf]


@dataclass(slots=True)
class SliceRecord:
    event_id: str
    source_event_id: str
    symbol: str
    branch_id: str
    horizon_sec: int
    return_pct: float
    pnl_value: float
    features: dict[str, str]


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def normalized_text(value: Any, *, default: str = UNKNOWN) -> str:
    text = str(value or "").strip()
    return text if text else default


def normalize_branch_type(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_branch_id(value: Any) -> str:
    return normalized_text(value, default="LEGACY").upper()


def event_matches(row: dict[str, Any], expected: str) -> bool:
    event_name = str(row.get("event") or "").strip()
    return not event_name or event_name == expected


def read_jsonl_records(path: Path, *, max_records: int = 0) -> list[dict[str, Any]]:
    records: deque[dict[str, Any]] | list[dict[str, Any]]
    records = deque(maxlen=max_records) if max_records > 0 else []
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
                records.append(payload)
    return list(records)


def get_nested(payload: dict[str, Any] | None, path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def first_present(*values: Any, default: str = UNKNOWN) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return default


def trigger_signature(*rows: dict[str, Any] | None) -> str:
    for row in rows:
        if not row:
            continue
        explicit = str(row.get("trigger_type") or "").strip()
        if explicit:
            return explicit
        values = row.get("trigger_types")
        if not values and isinstance(row.get("sample"), dict):
            values = row["sample"].get("trigger_types")
        if isinstance(values, (list, tuple, set)):
            parts = sorted({str(value).strip() for value in values if str(value or "").strip()})
            if parts:
                return "+".join(parts)
    return UNKNOWN


def parse_shadow_instance_id(instance_id: Any) -> tuple[str, str] | None:
    text = str(instance_id or "").strip()
    if not text:
        return None
    separator = "::" if "::" in text else ":"
    if separator not in text:
        return None
    event_id, branch_id = text.rsplit(separator, 1)
    if not event_id:
        return None
    return event_id, normalize_branch_id(branch_id)


def row_event_id(row: dict[str, Any]) -> str:
    event_id = str(row.get("event_id") or "").strip()
    if event_id:
        return event_id
    parsed = parse_shadow_instance_id(row.get("shadow_instance_id"))
    return parsed[0] if parsed else ""


def row_branch_id(row: dict[str, Any]) -> str:
    for key in ("shadow_branch_id", "branch_id"):
        if row.get(key):
            return normalize_branch_id(row.get(key))
    parsed = parse_shadow_instance_id(row.get("shadow_instance_id"))
    return parsed[1] if parsed else "LEGACY"


def source_event_id_for_outcome(outcome: dict[str, Any]) -> str:
    for key in ("source_event_id", "horizon_event_id"):
        value = str(outcome.get(key) or "").strip()
        if value:
            return value
    event_id = row_event_id(outcome)
    if "::" in event_id:
        return event_id.rsplit("::", 1)[0]
    return event_id


def return_pct_for_outcome(outcome: dict[str, Any]) -> float | None:
    adjusted = safe_float(outcome.get("after_fee_and_slippage_return_pct"))
    if adjusted is not None:
        return adjusted
    after_fee = safe_float(outcome.get("after_fee_return_pct"))
    if after_fee is not None:
        return after_fee
    return safe_float(outcome.get("close_return_pct"))


def horizon_sec_for_outcome(outcome: dict[str, Any]) -> int:
    explicit = safe_float(outcome.get("horizon_sec"))
    if explicit is not None:
        return int(explicit)
    horizon = outcome.get("horizon")
    if isinstance(horizon, dict):
        nested = safe_float(horizon.get("horizon_sec"))
        if nested is not None:
            return int(nested)
    fallback = safe_float(horizon)
    return int(fallback or 0)


def pnl_value_for_outcome(outcome: dict[str, Any], return_pct: float) -> float:
    allocation = safe_float(outcome.get("effective_quote_allocation_usdt"))
    if allocation is None:
        return return_pct
    return allocation * (return_pct / 100.0)


def bucket_label(lower: float, upper: float) -> str:
    def fmt(value: float) -> str:
        if value == -math.inf:
            return "-inf"
        if value == math.inf:
            return "inf"
        if abs(value) >= 1_000_000:
            return f"{value / 1_000_000:g}M"
        if abs(value) >= 1_000:
            return f"{value / 1_000:g}K"
        return f"{value:g}"

    if lower == -math.inf:
        return f"<{fmt(upper)}"
    if upper == math.inf:
        return f">={fmt(lower)}"
    return f"{fmt(lower)}..{fmt(upper)}"


def bucket_numeric_feature(name: str, value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return "missing"
    buckets = buckets_for_feature(name)
    for lower, upper in zip(buckets, buckets[1:]):
        if lower <= number < upper:
            return bucket_label(lower, upper)
    return "missing"


def buckets_for_feature(name: str) -> list[float]:
    if name.endswith("_score"):
        return SCORE_BUCKETS
    if name in {"oi_change_5m_pct", "oi_change_15m_pct", "price_change_24h_pct", "btcusdt_ret_5m_pct", "btcusdt_ret_1h_pct", "ethusdt_ret_5m_pct", "ethusdt_ret_1h_pct", "ret_1bar_pct", "ret_5bar_pct", "ret_15bar_pct", "ret_60bar_pct", "trigger_score"}:
        return PCT_BUCKETS
    if name in {"volume_burst_ratio", "range_to_atr", "body_to_atr", "context_3m_volume_burst_ratio", "context_3m_range_to_atr"}:
        return RATIO_BUCKETS
    if name == "depth_imbalance":
        return IMBALANCE_BUCKETS
    if name in {"spread_bps", "estimated_slippage_bps"}:
        return BPS_BUCKETS
    if name == "funding_rate":
        return FUNDING_BUCKETS
    if name == "quote_volume_24h":
        return QUOTE_VOLUME_BUCKETS
    if name in {"liquidation_long_usd_15m", "liquidation_short_usd_15m"}:
        return LIQUIDATION_USD_BUCKETS
    if name == "liquidation_event_count_15m":
        return COUNT_BUCKETS
    return PCT_BUCKETS


def value_from_snapshot(
    name: str,
    *,
    snapshot: dict[str, Any] | None,
    outcome: dict[str, Any],
) -> Any:
    snapshot = snapshot or {}
    sample = snapshot.get("sample") if isinstance(snapshot.get("sample"), dict) else {}
    enrichments = snapshot.get("enrichments") if isinstance(snapshot.get("enrichments"), dict) else {}
    factors = factors_for_rows(snapshot=snapshot, outcome=outcome)
    interval = str(snapshot.get("bar_interval") or outcome.get("bar_interval") or sample.get("bar_interval") or "").strip()
    context = research_context(snapshot, interval)
    for container in (sample, context, enrichments, factors, snapshot, outcome):
        if isinstance(container, dict) and name in container:
            return container.get(name)
    return None


def oi_regime(value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return "missing"
    if number < -2.0:
        return "oi_flush_extreme_drop_<-2"
    if number < -0.5:
        return "oi_unwind_drop_-2..-0.5"
    if number <= 0.5:
        return "oi_flat_-0.5..0.5"
    if number <= 2.0:
        return "oi_build_0.5..2"
    return "oi_build_extreme_>2"


def btc_regime(value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return "missing"
    if number <= -1.0:
        return "btc_dump_<=-1"
    if number <= -0.3:
        return "btc_soft_down_-1..-0.3"
    if number < 0.3:
        return "btc_flat_-0.3..0.3"
    if number < 1.0:
        return "btc_soft_up_0.3..1"
    return "btc_pump_>=1"


def depth_imbalance_regime(value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return "missing"
    if number <= -0.55:
        return "ask_wall_extreme_<=-0.55"
    if number <= -0.35:
        return "ask_dominant_-0.55..-0.35"
    if number < 0.35:
        return "balanced_-0.35..0.35"
    if number < 0.55:
        return "bid_dominant_0.35..0.55"
    return "bid_wall_extreme_>=0.55"


def ratio_regime(value: Any, *, prefix: str) -> str:
    number = safe_float(value)
    if number is None:
        return "missing"
    if number < 1.0:
        return f"{prefix}_quiet_<1"
    if number < 2.0:
        return f"{prefix}_mild_1..2"
    if number < 3.0:
        return f"{prefix}_strong_2..3"
    return f"{prefix}_extreme_>=3"


def liquidation_pressure_regime(long_value: Any, short_value: Any) -> str:
    long_usd = max(0.0, safe_float(long_value) or 0.0)
    short_usd = max(0.0, safe_float(short_value) or 0.0)
    total = long_usd + short_usd
    if total <= 0:
        return "liq_none"
    if total < 50_000.0:
        return "liq_low_<50K"
    if total < 250_000.0:
        return "liq_medium_50K..250K"
    if total < 1_000_000.0:
        return "liq_high_250K..1M"
    return "liq_extreme_>=1M"


def liquidation_side_bias(long_value: Any, short_value: Any) -> str:
    long_usd = max(0.0, safe_float(long_value) or 0.0)
    short_usd = max(0.0, safe_float(short_value) or 0.0)
    total = long_usd + short_usd
    if total <= 0:
        return "liq_none"
    long_share = long_usd / total
    if long_share >= 0.7:
        return "long_liquidation_dominant"
    if long_share <= 0.3:
        return "short_liquidation_dominant"
    return "mixed_liquidation"


def derived_feature_bucket(
    name: str,
    *,
    snapshot: dict[str, Any] | None,
    outcome: dict[str, Any],
) -> str | None:
    if name == "oi_5m_regime":
        return oi_regime(value_from_snapshot("oi_change_5m_pct", snapshot=snapshot, outcome=outcome))
    if name == "oi_15m_regime":
        return oi_regime(value_from_snapshot("oi_change_15m_pct", snapshot=snapshot, outcome=outcome))
    if name == "btc_5m_regime":
        return btc_regime(value_from_snapshot("btcusdt_ret_5m_pct", snapshot=snapshot, outcome=outcome))
    if name == "depth_imbalance_regime":
        return depth_imbalance_regime(value_from_snapshot("depth_imbalance", snapshot=snapshot, outcome=outcome))
    if name == "volume_burst_regime":
        return ratio_regime(value_from_snapshot("volume_burst_ratio", snapshot=snapshot, outcome=outcome), prefix="volume")
    if name == "range_expansion_regime":
        return ratio_regime(value_from_snapshot("range_to_atr", snapshot=snapshot, outcome=outcome), prefix="range")
    if name in {"liquidation_pressure", "liquidation_side_bias"}:
        long_value = value_from_snapshot("liquidation_long_usd_15m", snapshot=snapshot, outcome=outcome)
        short_value = value_from_snapshot("liquidation_short_usd_15m", snapshot=snapshot, outcome=outcome)
        if name == "liquidation_pressure":
            return liquidation_pressure_regime(long_value, short_value)
        return liquidation_side_bias(long_value, short_value)
    return None


def research_context(snapshot: dict[str, Any], interval: str | None) -> dict[str, Any]:
    contexts = snapshot.get("research_contexts")
    if isinstance(contexts, dict) and interval and isinstance(contexts.get(interval), dict):
        return contexts[interval]
    return {}


def factors_for_rows(*, snapshot: dict[str, Any] | None, outcome: dict[str, Any]) -> dict[str, Any]:
    snapshot = snapshot or {}
    snapshot_factors = snapshot.get("factors")
    if isinstance(snapshot_factors, dict) and snapshot_factors:
        return snapshot_factors
    outcome_factors = outcome.get("factors")
    if isinstance(outcome_factors, dict) and outcome_factors:
        return outcome_factors
    try:
        return build_factor_vector(snapshot if snapshot else outcome)
    except Exception:
        return {}


def feature_raw_value(
    name: str,
    *,
    snapshot: dict[str, Any] | None,
    outcome: dict[str, Any],
) -> Any:
    snapshot = snapshot or {}
    sample = snapshot.get("sample") if isinstance(snapshot.get("sample"), dict) else {}
    enrichments = snapshot.get("enrichments") if isinstance(snapshot.get("enrichments"), dict) else {}
    factors = factors_for_rows(snapshot=snapshot, outcome=outcome)
    interval = str(snapshot.get("bar_interval") or outcome.get("bar_interval") or sample.get("bar_interval") or "").strip()
    context = research_context(snapshot, interval)
    context_3m = snapshot.get("context_3m") if isinstance(snapshot.get("context_3m"), dict) else {}

    if name == "trigger_signature":
        return trigger_signature(outcome, snapshot)
    if name == "bar_interval":
        return first_present(outcome.get("bar_interval"), snapshot.get("bar_interval"), sample.get("bar_interval"))
    if name == "trading_session":
        return first_present(outcome.get("trading_session"), snapshot.get("trading_session"))
    if name == "sample_type":
        return first_present(snapshot.get("sample_type"), sample.get("sample_type"))
    if name == "direction_variant":
        return first_present(outcome.get("direction_variant"))
    if name == "side":
        return first_present(outcome.get("side"))
    if name == "derivatives_data_source":
        return first_present(enrichments.get("derivatives_data_source"))
    if name in {"candle_direction", "trigger_candle_direction", "confirmation_candle_direction", "reversal_confirmation_passed"}:
        return sample.get(name)
    if name.startswith("context_3m_"):
        return context_3m.get(name.removeprefix("context_3m_"))

    for container in (sample, context, enrichments, factors, snapshot, outcome):
        if isinstance(container, dict) and name in container:
            return container.get(name)
    return None


def feature_bucket(
    name: str,
    *,
    snapshot: dict[str, Any] | None,
    outcome: dict[str, Any],
) -> str:
    derived = derived_feature_bucket(name, snapshot=snapshot, outcome=outcome)
    if derived is not None:
        return derived
    value = feature_raw_value(name, snapshot=snapshot, outcome=outcome)
    if name in NUMERIC_FEATURES:
        return bucket_numeric_feature(name, value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return normalized_text(value)


def compute_sharpe(returns_pct: list[float]) -> float | None:
    if len(returns_pct) < 2:
        return None
    mean_return = sum(returns_pct) / len(returns_pct)
    variance = sum((value - mean_return) ** 2 for value in returns_pct) / (len(returns_pct) - 1)
    if variance <= 0:
        return None
    std_dev = variance**0.5
    if std_dev <= 1e-12:
        return None
    return mean_return / std_dev


def compute_profit_factor(values: list[float]) -> float | None:
    gross_profit = sum(value for value in values if value > 0)
    gross_loss = abs(sum(value for value in values if value < 0))
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return 999999.0
    return None


def compute_mdd_pct(returns_pct: list[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns_pct:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return max_drawdown


def round_optional(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def build_metric_payload(records: list[SliceRecord]) -> dict[str, Any]:
    returns_pct = [record.return_pct for record in records]
    pnl_values = [record.pnl_value for record in records]
    wins = [value for value in returns_pct if value > 0]
    losses = [value for value in returns_pct if value < 0]
    sample_count = len(records)
    total_return_pct = sum(returns_pct)
    total_pnl_value = sum(pnl_values)
    return {
        "sample_count": sample_count,
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate_pct": round_optional((len(wins) / sample_count * 100.0) if sample_count else None),
        "avg_return_pct": round_optional((total_return_pct / sample_count) if sample_count else None),
        "median_return_pct": round_optional(sorted(returns_pct)[sample_count // 2] if sample_count else None),
        "total_return_pct": round_optional(total_return_pct),
        "total_pnl_value": round_optional(total_pnl_value),
        "profit_factor": round_optional(compute_profit_factor(pnl_values)),
        "sharpe": round_optional(compute_sharpe(returns_pct)),
        "mdd_pct": round_optional(compute_mdd_pct(returns_pct)),
    }


def positive_sort_key(item: dict[str, Any]) -> tuple[float, float, float, int]:
    return (
        float(item.get("avg_return_pct") or -999999.0),
        float(item.get("profit_factor") or -999999.0),
        float(item.get("sharpe") or -999999.0),
        int(item.get("sample_count") or 0),
    )


def build_snapshot_index(snapshots: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in snapshots:
        if not event_matches(row, SNAPSHOT_EVENT_NAME):
            continue
        event_id = str(row.get("event_id") or "").strip()
        if event_id:
            index[event_id] = row
    return index


def outcome_is_in_scope(outcome: dict[str, Any], *, branch_type: str, include_non_research: bool) -> bool:
    if not event_matches(outcome, OUTCOME_EVENT_NAME):
        return False
    if branch_type and normalize_branch_type(outcome.get("branch_type")) != normalize_branch_type(branch_type):
        return False
    if not include_non_research and not boolish(outcome.get("research_only")) and normalize_branch_type(outcome.get("branch_type")) != DEFAULT_BRANCH_TYPE:
        return False
    if row_branch_id(outcome) == "LEGACY":
        return False
    side = str(outcome.get("side") or "").upper()
    return side in {"BUY", "SELL"}


def build_slice_records(
    *,
    snapshots: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    branch_type: str = DEFAULT_BRANCH_TYPE,
    include_non_research: bool = False,
) -> tuple[list[SliceRecord], dict[str, int]]:
    snapshot_by_event_id = build_snapshot_index(snapshots)
    records: list[SliceRecord] = []
    counts = {
        "snapshots_indexed": len(snapshot_by_event_id),
        "outcomes_seen": 0,
        "outcomes_in_scope": 0,
        "outcomes_missing_snapshot": 0,
        "outcomes_missing_return": 0,
        "records_built": 0,
    }
    all_features = CATEGORICAL_FEATURES + NUMERIC_FEATURES
    for outcome in outcomes:
        if not event_matches(outcome, OUTCOME_EVENT_NAME):
            continue
        counts["outcomes_seen"] += 1
        if not outcome_is_in_scope(outcome, branch_type=branch_type, include_non_research=include_non_research):
            continue
        counts["outcomes_in_scope"] += 1
        source_event_id = source_event_id_for_outcome(outcome)
        snapshot = snapshot_by_event_id.get(source_event_id)
        if snapshot is None:
            counts["outcomes_missing_snapshot"] += 1
        return_pct = return_pct_for_outcome(outcome)
        if return_pct is None:
            counts["outcomes_missing_return"] += 1
            continue
        features = {
            name: feature_bucket(name, snapshot=snapshot, outcome=outcome)
            for name in all_features
        }
        records.append(
            SliceRecord(
                event_id=row_event_id(outcome),
                source_event_id=source_event_id,
                symbol=first_present(outcome.get("symbol"), snapshot.get("symbol") if snapshot else None).upper(),
                branch_id=row_branch_id(outcome),
                horizon_sec=horizon_sec_for_outcome(outcome),
                return_pct=return_pct,
                pnl_value=pnl_value_for_outcome(outcome, return_pct),
                features=features,
            )
        )
    counts["records_built"] = len(records)
    return records, counts


def build_group_payload(
    *,
    records: list[SliceRecord],
    dimensions: dict[str, Any],
) -> dict[str, Any]:
    return {**dimensions, **build_metric_payload(records)}


def build_overall_slices(records: list[SliceRecord], *, min_samples: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[SliceRecord]] = defaultdict(list)
    for record in records:
        grouped[(record.branch_id, record.horizon_sec)].append(record)
    rows = [
        build_group_payload(
            records=items,
            dimensions={"branch_id": branch_id, "horizon_sec": horizon_sec},
        )
        for (branch_id, horizon_sec), items in grouped.items()
        if len(items) >= min_samples
    ]
    rows.sort(key=positive_sort_key, reverse=True)
    return rows


def build_single_feature_slices(records: list[SliceRecord], *, min_samples: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str, str], list[SliceRecord]] = defaultdict(list)
    feature_names = CATEGORICAL_FEATURES + NUMERIC_FEATURES
    for record in records:
        for feature_name in feature_names:
            bucket = record.features.get(feature_name) or "missing"
            if bucket == "missing":
                continue
            grouped[(record.branch_id, record.horizon_sec, feature_name, bucket)].append(record)
    rows = [
        build_group_payload(
            records=items,
            dimensions={
                "branch_id": branch_id,
                "horizon_sec": horizon_sec,
                "feature": feature_name,
                "bucket": bucket,
            },
        )
        for (branch_id, horizon_sec, feature_name, bucket), items in grouped.items()
        if len(items) >= min_samples
    ]
    rows.sort(key=positive_sort_key, reverse=True)
    return rows


def build_pair_feature_slices(records: list[SliceRecord], *, min_samples: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str, str, str, str], list[SliceRecord]] = defaultdict(list)
    feature_pairs = list(combinations(PAIR_FEATURES, 2))
    for record in records:
        for feature_a, feature_b in feature_pairs:
            bucket_a = record.features.get(feature_a) or "missing"
            bucket_b = record.features.get(feature_b) or "missing"
            if bucket_a == "missing" or bucket_b == "missing":
                continue
            grouped[(record.branch_id, record.horizon_sec, feature_a, bucket_a, feature_b, bucket_b)].append(record)
    rows = [
        build_group_payload(
            records=items,
            dimensions={
                "branch_id": branch_id,
                "horizon_sec": horizon_sec,
                "feature_a": feature_a,
                "bucket_a": bucket_a,
                "feature_b": feature_b,
                "bucket_b": bucket_b,
            },
        )
        for (branch_id, horizon_sec, feature_a, bucket_a, feature_b, bucket_b), items in grouped.items()
        if len(items) >= min_samples
    ]
    rows.sort(key=positive_sort_key, reverse=True)
    return rows


def build_feature_slice_report(
    snapshots: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    *,
    branch_type: str = DEFAULT_BRANCH_TYPE,
    min_samples: int = 40,
    pair_min_samples: int = 35,
    top_n: int = 25,
    include_non_research: bool = False,
) -> dict[str, Any]:
    records, counts = build_slice_records(
        snapshots=snapshots,
        outcomes=outcomes,
        branch_type=branch_type,
        include_non_research=include_non_research,
    )
    min_samples = max(1, int(min_samples))
    pair_min_samples = max(1, int(pair_min_samples))
    top_n = max(1, int(top_n))
    overall = build_overall_slices(records, min_samples=min_samples)
    single_feature = build_single_feature_slices(records, min_samples=min_samples)
    pair_feature = build_pair_feature_slices(records, min_samples=pair_min_samples)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "filters": {
            "branch_type": branch_type,
            "include_non_research": include_non_research,
            "skip_legacy_branch": True,
            "skip_missing_side": True,
        },
        "min_samples": min_samples,
        "pair_min_samples": pair_min_samples,
        "input_counts": counts,
        "top_n": top_n,
        "overall_by_branch_horizon": overall[:top_n],
        "top_single_feature_slices": single_feature[:top_n],
        "top_pair_feature_slices": pair_feature[:top_n],
        "all_positive_pair_feature_slices": [
            row for row in pair_feature if float(row.get("avg_return_pct") or 0.0) > 0
        ][: max(top_n, 100)],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Slice Phoenix shadow outcomes by event features.")
    parser.add_argument("--snapshots-file", type=Path, required=True)
    parser.add_argument("--outcomes-file", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--branch-type", default=DEFAULT_BRANCH_TYPE)
    parser.add_argument("--min-samples", type=int, default=40)
    parser.add_argument("--pair-min-samples", type=int, default=35)
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--max-snapshots", type=int, default=0)
    parser.add_argument("--max-outcomes", type=int, default=0)
    parser.add_argument("--include-non-research", action="store_true")
    return parser.parse_args(argv)


def print_console_summary(report: dict[str, Any], *, output_json: Path) -> None:
    counts = report["input_counts"]
    print(
        "feature_slice_report "
        f"records_built={counts['records_built']} "
        f"missing_snapshot={counts['outcomes_missing_snapshot']} "
        f"output_json={output_json}",
        flush=True,
    )
    for section_name, title in (
        ("overall_by_branch_horizon", "top_overall"),
        ("top_single_feature_slices", "top_single"),
        ("top_pair_feature_slices", "top_pair"),
    ):
        rows = report.get(section_name) or []
        if not rows:
            print(f"{title}: none", flush=True)
            continue
        for index, row in enumerate(rows[:5], start=1):
            descriptor = row.get("branch_id", "")
            if row.get("feature"):
                descriptor += f" {row['feature']}={row['bucket']}"
            if row.get("feature_a"):
                descriptor += f" {row['feature_a']}={row['bucket_a']} {row['feature_b']}={row['bucket_b']}"
            print(
                f"{title}[{index}] {descriptor} "
                f"h={row.get('horizon_sec')} n={row.get('sample_count')} "
                f"avg={row.get('avg_return_pct')} win={row.get('win_rate_pct')} "
                f"pf={row.get('profit_factor')} sharpe={row.get('sharpe')}",
                flush=True,
            )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    snapshots = read_jsonl_records(args.snapshots_file, max_records=max(0, int(args.max_snapshots or 0)))
    outcomes = read_jsonl_records(args.outcomes_file, max_records=max(0, int(args.max_outcomes or 0)))
    report = build_feature_slice_report(
        snapshots,
        outcomes,
        branch_type=args.branch_type,
        min_samples=args.min_samples,
        pair_min_samples=args.pair_min_samples,
        top_n=args.top,
        include_non_research=bool(args.include_non_research),
    )
    report.update(
        {
            "snapshots_file": str(args.snapshots_file),
            "outcomes_file": str(args.outcomes_file),
        }
    )
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print_console_summary(report, output_json=args.output_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
