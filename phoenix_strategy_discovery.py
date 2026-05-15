#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

from phoenix_feature_slice_report import (
    DEFAULT_BRANCH_TYPE,
    SliceRecord,
    build_slice_records,
    compute_mdd_pct,
    compute_profit_factor,
    compute_sharpe,
    read_jsonl_records,
    round_optional,
)


DEFAULT_DISCOVERY_FEATURES = [
    "bar_interval",
    "trading_session",
    "trigger_signature",
    "candle_direction",
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
    "volume_burst_ratio",
    "range_to_atr",
    "oi_change_5m_pct",
    "oi_change_15m_pct",
    "depth_imbalance",
    "btcusdt_ret_5m_pct",
    "liquidation_long_usd_15m",
    "liquidation_short_usd_15m",
    "liquidation_event_count_15m",
    "context_3m_volume_burst_ratio",
    "context_3m_range_to_atr",
    "context_3m_ret_1bar_pct",
    "context_3m_ret_5bar_pct",
    "context_3m_ret_15bar_pct",
]

IGNORED_BUCKETS = {"", "missing", "unknown"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_csv(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.split(",")
    else:
        raw_items = list(value)
    return [item.strip() for item in raw_items if item and item.strip()]


def normalize_branch_type(value: str | None) -> str:
    text = str(value or DEFAULT_BRANCH_TYPE).strip()
    if text.lower() in {"all", "*", "any"}:
        return ""
    return text


def metric_payload(records: list[SliceRecord]) -> dict[str, Any]:
    returns_pct = [record.return_pct for record in records]
    pnl_values = [record.pnl_value for record in records]
    wins = [value for value in returns_pct if value > 0]
    losses = [value for value in returns_pct if value < 0]
    sample_count = len(records)
    total_return_pct = sum(returns_pct)
    return {
        "sample_count": sample_count,
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate_pct": round_optional((len(wins) / sample_count * 100.0) if sample_count else None),
        "avg_return_pct": round_optional((total_return_pct / sample_count) if sample_count else None),
        "total_return_pct": round_optional(total_return_pct),
        "total_pnl_value": round_optional(sum(pnl_values)),
        "profit_factor": round_optional(compute_profit_factor(pnl_values)),
        "sharpe": round_optional(compute_sharpe(returns_pct)),
        "mdd_pct": round_optional(compute_mdd_pct(returns_pct)),
    }


def split_validation_records(
    records: list[SliceRecord],
    *,
    validation_pct: float,
    min_validation_samples: int,
) -> list[SliceRecord]:
    if not records:
        return []
    requested = int(math.ceil(len(records) * max(0.0, min(0.9, validation_pct))))
    validation_count = max(max(1, int(min_validation_samples)), requested)
    validation_count = min(len(records), validation_count)
    return records[-validation_count:]


def condition_candidates(record: SliceRecord, features: list[str]) -> list[tuple[str, str]]:
    conditions: list[tuple[str, str]] = []
    for feature_name in features:
        bucket = str(record.features.get(feature_name) or "").strip()
        if bucket.lower() in IGNORED_BUCKETS:
            continue
        conditions.append((feature_name, bucket))
    return conditions


def condition_key_to_payload(condition_key: tuple[tuple[str, str], ...]) -> list[dict[str, str]]:
    return [
        {"feature": feature_name, "bucket": bucket}
        for feature_name, bucket in condition_key
    ]


def format_condition_key(condition_key: tuple[tuple[str, str], ...]) -> str:
    return " & ".join(f"{feature}={bucket}" for feature, bucket in condition_key)


def stable_candidate_id(branch_id: str, horizon_sec: int, condition_key: tuple[tuple[str, str], ...]) -> str:
    raw_value = json.dumps(
        {
            "branch_id": branch_id,
            "horizon_sec": horizon_sec,
            "conditions": list(condition_key),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return "CAND_" + hashlib.sha1(raw_value.encode("utf-8")).hexdigest()[:12].upper()


def discovery_score(metrics: dict[str, Any], validation_metrics: dict[str, Any]) -> float:
    avg_return = float(metrics.get("avg_return_pct") or 0.0)
    sample_count = max(1, int(metrics.get("sample_count") or 0))
    profit_factor = min(10.0, float(metrics.get("profit_factor") or 0.0))
    sharpe = float(metrics.get("sharpe") or 0.0)
    mdd_pct = float(metrics.get("mdd_pct") or 0.0)
    validation_avg = float(validation_metrics.get("avg_return_pct") or 0.0)
    validation_pf = min(10.0, float(validation_metrics.get("profit_factor") or 0.0))
    return round(
        avg_return * math.log10(sample_count + 1)
        + validation_avg * 1.5
        + profit_factor * 0.05
        + validation_pf * 0.05
        + sharpe * 0.15
        - mdd_pct * 0.02,
        6,
    )


def build_candidate_payload(
    *,
    branch_id: str,
    horizon_sec: int,
    condition_key: tuple[tuple[str, str], ...],
    records: list[SliceRecord],
    validation_pct: float,
    min_validation_samples: int,
    min_avg_return_pct: float,
    min_profit_factor: float,
    validation_min_profit_factor: float,
    max_mdd_pct: float,
) -> dict[str, Any]:
    metrics = metric_payload(records)
    validation_records = split_validation_records(
        records,
        validation_pct=validation_pct,
        min_validation_samples=min_validation_samples,
    )
    validation_metrics = metric_payload(validation_records)
    profit_factor = float(metrics.get("profit_factor") or 0.0)
    validation_profit_factor = float(validation_metrics.get("profit_factor") or 0.0)
    avg_return_pct = float(metrics.get("avg_return_pct") or 0.0)
    validation_avg_return_pct = float(validation_metrics.get("avg_return_pct") or 0.0)
    mdd_pct = float(metrics.get("mdd_pct") or 0.0)
    passes = {
        "avg_return_ok": avg_return_pct >= min_avg_return_pct,
        "profit_factor_ok": profit_factor >= min_profit_factor,
        "validation_sample_ok": int(validation_metrics.get("sample_count") or 0) >= min_validation_samples,
        "validation_avg_return_ok": validation_avg_return_pct > 0.0,
        "validation_profit_factor_ok": validation_profit_factor >= validation_min_profit_factor,
        "max_drawdown_ok": mdd_pct <= max_mdd_pct,
    }
    return {
        "strategy_candidate_id": stable_candidate_id(branch_id, horizon_sec, condition_key),
        "branch_id": branch_id,
        "horizon_sec": horizon_sec,
        "degree": len(condition_key),
        "condition_key": format_condition_key(condition_key),
        "conditions": condition_key_to_payload(condition_key),
        **metrics,
        "validation": validation_metrics,
        "passes": passes,
        "qualified": all(passes.values()),
        "discovery_score": discovery_score(metrics, validation_metrics),
        "source_event_ids_sample": [record.source_event_id for record in records[:5]],
    }


def candidate_sort_key(item: dict[str, Any]) -> tuple[float, float, float, int]:
    return (
        float(item.get("discovery_score") or -999999.0),
        float(item.get("avg_return_pct") or -999999.0),
        float(item.get("profit_factor") or -999999.0),
        int(item.get("sample_count") or 0),
    )


def mine_group_candidates(
    *,
    branch_id: str,
    horizon_sec: int,
    records: list[SliceRecord],
    features: list[str],
    max_degree: int,
    min_samples: int,
    validation_pct: float,
    min_validation_samples: int,
    min_avg_return_pct: float,
    min_profit_factor: float,
    validation_min_profit_factor: float,
    max_mdd_pct: float,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[tuple[str, str], ...], list[SliceRecord]] = defaultdict(list)
    max_degree = max(1, min(3, int(max_degree)))
    for record in records:
        conditions = condition_candidates(record, features)
        if not conditions:
            continue
        for degree in range(1, max_degree + 1):
            if len(conditions) < degree:
                continue
            for combo in combinations(conditions, degree):
                grouped[combo].append(record)
    candidates: list[dict[str, Any]] = []
    for condition_key, items in grouped.items():
        if len(items) < min_samples:
            continue
        candidates.append(
            build_candidate_payload(
                branch_id=branch_id,
                horizon_sec=horizon_sec,
                condition_key=condition_key,
                records=items,
                validation_pct=validation_pct,
                min_validation_samples=min_validation_samples,
                min_avg_return_pct=min_avg_return_pct,
                min_profit_factor=min_profit_factor,
                validation_min_profit_factor=validation_min_profit_factor,
                max_mdd_pct=max_mdd_pct,
            )
        )
    return candidates


def build_strategy_discovery_report(
    snapshots: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    *,
    branch_type: str = DEFAULT_BRANCH_TYPE,
    include_non_research: bool = False,
    features: list[str] | None = None,
    min_samples: int = 50,
    min_validation_samples: int = 15,
    validation_pct: float = 0.3,
    max_degree: int = 3,
    min_avg_return_pct: float = 0.0,
    min_profit_factor: float = 1.3,
    validation_min_profit_factor: float = 1.05,
    max_mdd_pct: float = 20.0,
    top_n: int = 50,
) -> dict[str, Any]:
    records, counts = build_slice_records(
        snapshots=snapshots,
        outcomes=outcomes,
        branch_type=normalize_branch_type(branch_type),
        include_non_research=include_non_research,
    )
    selected_features = features or DEFAULT_DISCOVERY_FEATURES
    records_by_group: dict[tuple[str, int], list[SliceRecord]] = defaultdict(list)
    for record in records:
        records_by_group[(record.branch_id, record.horizon_sec)].append(record)

    baseline = [
        {
            "branch_id": branch_id,
            "horizon_sec": horizon_sec,
            **metric_payload(items),
        }
        for (branch_id, horizon_sec), items in records_by_group.items()
    ]
    baseline.sort(key=candidate_sort_key, reverse=True)

    all_candidates: list[dict[str, Any]] = []
    for (branch_id, horizon_sec), items in sorted(records_by_group.items()):
        all_candidates.extend(
            mine_group_candidates(
                branch_id=branch_id,
                horizon_sec=horizon_sec,
                records=items,
                features=selected_features,
                max_degree=max_degree,
                min_samples=max(1, int(min_samples)),
                validation_pct=validation_pct,
                min_validation_samples=max(1, int(min_validation_samples)),
                min_avg_return_pct=min_avg_return_pct,
                min_profit_factor=min_profit_factor,
                validation_min_profit_factor=validation_min_profit_factor,
                max_mdd_pct=max_mdd_pct,
            )
        )
    qualified = [item for item in all_candidates if bool(item.get("qualified"))]
    near_misses = [
        item
        for item in all_candidates
        if not bool(item.get("qualified")) and float(item.get("avg_return_pct") or 0.0) > 0.0
    ]
    qualified.sort(key=candidate_sort_key, reverse=True)
    near_misses.sort(key=candidate_sort_key, reverse=True)
    top_n = max(1, int(top_n))
    return {
        "generated_at": utc_now_iso(),
        "scope": {
            "branch_type": normalize_branch_type(branch_type) or "all",
            "include_non_research": include_non_research,
            "features": selected_features,
        },
        "thresholds": {
            "min_samples": max(1, int(min_samples)),
            "min_validation_samples": max(1, int(min_validation_samples)),
            "validation_pct": validation_pct,
            "max_degree": max(1, min(3, int(max_degree))),
            "min_avg_return_pct": min_avg_return_pct,
            "min_profit_factor": min_profit_factor,
            "validation_min_profit_factor": validation_min_profit_factor,
            "max_mdd_pct": max_mdd_pct,
        },
        "input_counts": {
            **counts,
            "branch_horizon_groups": len(records_by_group),
            "candidate_groups_tested": len(all_candidates),
            "qualified_count": len(qualified),
            "near_miss_count": len(near_misses),
        },
        "baseline_by_branch_horizon": baseline[:top_n],
        "qualified_candidates": qualified[:top_n],
        "near_miss_candidates": near_misses[:top_n],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mine validated, explainable strategy candidates from Phoenix shadow outcomes."
    )
    parser.add_argument("--snapshots-file", type=Path, required=True)
    parser.add_argument("--outcomes-file", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--branch-type", default=DEFAULT_BRANCH_TYPE)
    parser.add_argument("--include-non-research", action="store_true")
    parser.add_argument("--features", default=",".join(DEFAULT_DISCOVERY_FEATURES))
    parser.add_argument("--min-samples", type=int, default=50)
    parser.add_argument("--min-validation-samples", type=int, default=15)
    parser.add_argument("--validation-pct", type=float, default=0.3)
    parser.add_argument("--max-degree", type=int, default=3)
    parser.add_argument("--min-avg-return-pct", type=float, default=0.0)
    parser.add_argument("--min-profit-factor", type=float, default=1.3)
    parser.add_argument("--validation-min-profit-factor", type=float, default=1.05)
    parser.add_argument("--max-mdd-pct", type=float, default=20.0)
    parser.add_argument("--top", type=int, default=50)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    snapshots = read_jsonl_records(args.snapshots_file)
    outcomes = read_jsonl_records(args.outcomes_file)
    report = build_strategy_discovery_report(
        snapshots,
        outcomes,
        branch_type=args.branch_type,
        include_non_research=bool(args.include_non_research),
        features=parse_csv(args.features) or DEFAULT_DISCOVERY_FEATURES,
        min_samples=max(1, int(args.min_samples)),
        min_validation_samples=max(1, int(args.min_validation_samples)),
        validation_pct=max(0.05, min(0.9, float(args.validation_pct))),
        max_degree=max(1, min(3, int(args.max_degree))),
        min_avg_return_pct=float(args.min_avg_return_pct),
        min_profit_factor=float(args.min_profit_factor),
        validation_min_profit_factor=float(args.validation_min_profit_factor),
        max_mdd_pct=float(args.max_mdd_pct),
        top_n=max(1, int(args.top)),
    )
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    counts = report["input_counts"]
    print(
        "strategy_discovery "
        f"records_built={counts['records_built']} "
        f"tested={counts['candidate_groups_tested']} "
        f"qualified={counts['qualified_count']} "
        f"near_miss={counts['near_miss_count']} "
        f"output={args.output_json}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
