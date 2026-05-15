from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from phoenix_factor_library import FACTOR_DEFINITIONS, FACTOR_VERSION, build_factor_vector, safe_float
from phoenix_feature_slice_report import (
    build_feature_slice_report,
    build_snapshot_index,
    event_matches,
    horizon_sec_for_outcome,
    return_pct_for_outcome,
    row_branch_id,
    row_event_id,
    source_event_id_for_outcome,
)


FACTOR_FACTORY_VERSION = "v1.0"
OUTCOME_EVENT_NAME = "signal_bridge_shadow_horizon_result"
FACTOR_BUCKET_FEATURES = {
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
}
FACTOR_SCORE_FEATURES = {
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
}
DEFAULT_MIN_OUTCOMES = 30
DEFAULT_FOCUS_PROFIT_FACTOR = 1.25
DEFAULT_PAUSE_PROFIT_FACTOR = 0.8
DEFAULT_MIN_AVG_RETURN_PCT = 0.0


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def round_optional(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def compute_sharpe_ratio(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / (len(values) - 1)
    if variance <= 0:
        return None
    std_dev = variance**0.5
    if std_dev <= 1e-12:
        return None
    return mean_value / std_dev


def compute_profit_factor(values: list[float]) -> float | None:
    gross_profit = sum(value for value in values if value > 0)
    gross_loss = abs(sum(value for value in values if value < 0))
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return 999999.0
    return None


def compute_drawdown_pct(values: list[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return max_drawdown


def summarize_returns(values: Iterable[float]) -> dict[str, Any]:
    rows = [float(value) for value in values if math.isfinite(float(value))]
    wins = [value for value in rows if value > 0]
    losses = [value for value in rows if value < 0]
    sample_count = len(rows)
    return {
        "sample_count": sample_count,
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate_pct": round_optional((len(wins) / sample_count) * 100.0 if sample_count else None),
        "avg_return_pct": round_optional((sum(rows) / sample_count) if sample_count else None),
        "total_return_pct": round_optional(sum(rows)),
        "profit_factor": round_optional(compute_profit_factor(rows)),
        "sharpe_ratio": round_optional(compute_sharpe_ratio(rows)),
        "max_drawdown_pct": round_optional(compute_drawdown_pct(rows)),
    }


def read_recent_jsonl_records(path: Path, *, max_records: int = 0) -> list[dict[str, Any]]:
    records: deque[dict[str, Any]] | list[dict[str, Any]]
    records = deque(maxlen=max_records) if max_records > 0 else []
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError:
        return []
    with handle:
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


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    return side if side in {"BUY", "SELL"} else ""


def factor_value(factors: dict[str, Any], name: str) -> float:
    return safe_float(factors.get(name)) or 0.0


def opposite_side(side: str) -> str:
    return "SELL" if side == "BUY" else "BUY" if side == "SELL" else ""


@dataclass(frozen=True)
class FactorSignalRule:
    name: str
    category: str
    expression: str
    side_resolver: Callable[[dict[str, Any]], str]


def trend_alignment_side(factors: dict[str, Any]) -> str:
    trend = factor_value(factors, "trend_score")
    liquidity = factor_value(factors, "liquidity_score")
    volatility = factor_value(factors, "volatility_regime_score")
    flow = factor_value(factors, "flow_score")
    market = factor_value(factors, "market_regime_score")
    oi_build = factor_value(factors, "oi_build_score")
    if trend >= 0.35 and liquidity >= 0.35 and volatility >= 0.3 and oi_build >= 0.2 and flow >= -0.15 and market >= -0.25:
        return "BUY"
    if trend <= -0.35 and liquidity >= 0.35 and volatility >= 0.3 and oi_build >= 0.2 and flow <= 0.15 and market <= 0.25:
        return "SELL"
    return ""


def liquidation_reversion_side(factors: dict[str, Any]) -> str:
    reversion = factor_value(factors, "mean_reversion_score")
    unwind = factor_value(factors, "oi_unwind_score")
    liquidation = factor_value(factors, "liquidation_pressure_score")
    liquidity = factor_value(factors, "liquidity_score")
    market = factor_value(factors, "market_regime_score")
    if reversion >= 0.35 and unwind >= 0.25 and liquidation <= -0.05 and liquidity >= 0.25 and market >= -0.35:
        return "BUY"
    if reversion <= -0.35 and unwind >= 0.25 and liquidation >= 0.05 and liquidity >= 0.25 and market <= 0.35:
        return "SELL"
    return ""


def flow_squeeze_side(factors: dict[str, Any]) -> str:
    flow = factor_value(factors, "flow_score")
    trend = factor_value(factors, "trend_score")
    oi_build = factor_value(factors, "oi_build_score")
    micro = factor_value(factors, "microstructure_score")
    liquidity = factor_value(factors, "liquidity_score")
    if flow >= 0.35 and trend >= 0.2 and oi_build >= 0.25 and micro >= -0.1 and liquidity >= 0.35:
        return "BUY"
    if flow <= -0.35 and trend <= -0.2 and oi_build >= 0.25 and micro <= 0.1 and liquidity >= 0.35:
        return "SELL"
    return ""


def crowded_unwind_side(factors: dict[str, Any]) -> str:
    crowding = factor_value(factors, "crowding_score")
    unwind = factor_value(factors, "oi_unwind_score")
    reversion = factor_value(factors, "mean_reversion_score")
    market = factor_value(factors, "market_regime_score")
    if crowding >= 0.35 and unwind >= 0.3 and reversion <= -0.2 and market <= 0.35:
        return "SELL"
    if crowding <= -0.35 and unwind >= 0.3 and reversion >= 0.2 and market >= -0.35:
        return "BUY"
    return ""


FACTOR_SIGNAL_RULES = (
    FactorSignalRule(
        name="factor_trend_alignment",
        category="trend",
        expression="trend_score extreme + OI build + liquidity ok + flow/market not fighting the side",
        side_resolver=trend_alignment_side,
    ),
    FactorSignalRule(
        name="factor_liquidation_reversion",
        category="reversion",
        expression="mean_reversion + OI unwind + liquidation side bias + no BTC storm",
        side_resolver=liquidation_reversion_side,
    ),
    FactorSignalRule(
        name="factor_flow_squeeze",
        category="order_flow",
        expression="aggressive flow + OI build + trend alignment + tolerable microstructure",
        side_resolver=flow_squeeze_side,
    ),
    FactorSignalRule(
        name="factor_crowded_unwind",
        category="derivatives",
        expression="funding/basis crowding + OI unwind + reversion pressure",
        side_resolver=crowded_unwind_side,
    ),
)


def latest_outcomes_by_instance(outcomes: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    latest_horizon: dict[str, int] = {}
    for outcome in outcomes:
        if not event_matches(outcome, OUTCOME_EVENT_NAME):
            continue
        instance_id = str(outcome.get("shadow_instance_id") or row_event_id(outcome) or "").strip()
        if not instance_id:
            continue
        horizon_sec = horizon_sec_for_outcome(outcome)
        if instance_id not in latest or horizon_sec >= latest_horizon.get(instance_id, -1):
            latest[instance_id] = outcome
            latest_horizon[instance_id] = horizon_sec
    return [latest[key] for key in sorted(latest)]


def row_factors(snapshot: dict[str, Any] | None, outcome: dict[str, Any]) -> dict[str, Any]:
    snapshot = snapshot or {}
    for source in (outcome.get("factors"), snapshot.get("factors")):
        if isinstance(source, dict) and source:
            return source
    try:
        return build_factor_vector(snapshot if snapshot else outcome)
    except Exception:
        return {}


def rule_evaluation_rows(
    *,
    snapshots: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    branch_type: str = "",
) -> list[dict[str, Any]]:
    snapshot_by_event = build_snapshot_index(snapshots)
    rows: list[dict[str, Any]] = []
    for outcome in latest_outcomes_by_instance(outcomes):
        if branch_type and str(outcome.get("branch_type") or "").strip().lower() != branch_type.strip().lower():
            continue
        return_pct = return_pct_for_outcome(outcome)
        if return_pct is None:
            continue
        side = normalize_side(outcome.get("side"))
        if not side:
            continue
        snapshot = snapshot_by_event.get(source_event_id_for_outcome(outcome))
        factors = row_factors(snapshot, outcome)
        if not factors:
            continue
        rows.append(
            {
                "outcome": outcome,
                "snapshot": snapshot,
                "factors": factors,
                "side": side,
                "return_pct": float(return_pct),
                "horizon_sec": horizon_sec_for_outcome(outcome),
                "branch_id": row_branch_id(outcome),
            }
        )
    return rows


def build_rule_report(rows: list[dict[str, Any]], *, min_samples: int) -> dict[str, Any]:
    by_rule: dict[str, list[float]] = defaultdict(list)
    by_rule_horizon: dict[tuple[str, int], list[float]] = defaultdict(list)
    trigger_counts: dict[str, int] = defaultdict(int)
    side_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"BUY": 0, "SELL": 0})
    for row in rows:
        factors = row["factors"]
        actual_side = row["side"]
        return_pct = float(row["return_pct"])
        for rule in FACTOR_SIGNAL_RULES:
            resolved_side = rule.side_resolver(factors)
            if not resolved_side:
                continue
            trigger_counts[rule.name] += 1
            side_counts[rule.name][resolved_side] += 1
            if resolved_side != actual_side:
                continue
            by_rule[rule.name].append(return_pct)
            by_rule_horizon[(rule.name, int(row["horizon_sec"]))].append(return_pct)

    rule_payloads: dict[str, dict[str, Any]] = {}
    min_samples = max(1, int(min_samples))
    for rule in FACTOR_SIGNAL_RULES:
        horizon_rows = []
        for (rule_name, horizon_sec), values in sorted(by_rule_horizon.items()):
            if rule_name != rule.name or len(values) < min_samples:
                continue
            horizon_rows.append({"horizon_sec": horizon_sec, **summarize_returns(values)})
        horizon_rows.sort(
            key=lambda item: (
                float(item.get("avg_return_pct") or -999999.0),
                float(item.get("profit_factor") or -999999.0),
                int(item.get("sample_count") or 0),
            ),
            reverse=True,
        )
        rule_payloads[rule.name] = {
            "category": rule.category,
            "expression": rule.expression,
            "factor_version": FACTOR_VERSION,
            "trigger_count": int(trigger_counts.get(rule.name, 0)),
            "matched_outcome_side_count": len(by_rule.get(rule.name, [])),
            "side_counts": dict(side_counts.get(rule.name, {})),
            **summarize_returns(by_rule.get(rule.name, [])),
            "best_horizon": horizon_rows[0] if horizon_rows else None,
            "horizon_breakdown": horizon_rows,
        }
    return rule_payloads


def filter_factor_slices(rows: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
    selected = []
    for row in rows:
        feature = str(row.get("feature") or "")
        feature_a = str(row.get("feature_a") or "")
        feature_b = str(row.get("feature_b") or "")
        if (
            feature in FACTOR_BUCKET_FEATURES
            or feature in FACTOR_SCORE_FEATURES
            or feature_a in FACTOR_BUCKET_FEATURES
            or feature_a in FACTOR_SCORE_FEATURES
            or feature_b in FACTOR_BUCKET_FEATURES
            or feature_b in FACTOR_SCORE_FEATURES
        ):
            selected.append(row)
    return selected[: max(1, int(top_n))]


def build_factor_factory_report(
    *,
    snapshots: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    generated_at: str | None = None,
    branch_type: str = "research_pool",
    min_samples: int = DEFAULT_MIN_OUTCOMES,
    pair_min_samples: int = 20,
    top_n: int = 25,
    include_non_research: bool = False,
) -> dict[str, Any]:
    snapshots_list = list(snapshots)
    outcomes_list = list(outcomes)
    generated_at = generated_at or now_iso()
    attribution = build_feature_slice_report(
        snapshots_list,
        outcomes_list,
        branch_type=branch_type,
        min_samples=max(1, int(min_samples)),
        pair_min_samples=max(1, int(pair_min_samples)),
        top_n=max(1, int(top_n)),
        include_non_research=include_non_research,
    )
    rows = rule_evaluation_rows(
        snapshots=snapshots_list,
        outcomes=outcomes_list,
        branch_type=branch_type,
    )
    rule_report = build_rule_report(rows, min_samples=max(1, int(min_samples)))
    ranked_rules = sorted(
        (
            {"rule": name, **payload}
            for name, payload in rule_report.items()
            if int(payload.get("sample_count") or 0) > 0
        ),
        key=lambda item: (
            float(item.get("avg_return_pct") or -999999.0),
            float(item.get("profit_factor") or -999999.0),
            float(item.get("sharpe_ratio") or -999999.0),
            int(item.get("sample_count") or 0),
        ),
        reverse=True,
    )
    return {
        "generated_at": generated_at,
        "factor_factory_version": FACTOR_FACTORY_VERSION,
        "factor_version": FACTOR_VERSION,
        "factor_definition_count": len(FACTOR_DEFINITIONS),
        "filters": {
            "branch_type": branch_type,
            "include_non_research": include_non_research,
            "min_samples": max(1, int(min_samples)),
            "pair_min_samples": max(1, int(pair_min_samples)),
            "top_n": max(1, int(top_n)),
        },
        "input_counts": {
            "snapshots_read": len(snapshots_list),
            "outcomes_read": len(outcomes_list),
            "rule_evaluation_rows": len(rows),
            **(attribution.get("input_counts") or {}),
        },
        "factor_registry": FACTOR_DEFINITIONS,
        "rule_definitions": [
            {"name": rule.name, "category": rule.category, "expression": rule.expression}
            for rule in FACTOR_SIGNAL_RULES
        ],
        "candidate_rule_performance": rule_report,
        "ranked_candidate_rules": ranked_rules[: max(1, int(top_n))],
        "factor_attribution": {
            "top_single_factor_slices": filter_factor_slices(
                list(attribution.get("top_single_feature_slices") or []),
                top_n=max(1, int(top_n)),
            ),
            "top_pair_factor_slices": filter_factor_slices(
                list(attribution.get("top_pair_feature_slices") or []),
                top_n=max(1, int(top_n)),
            ),
            "all_positive_pair_factor_slices": filter_factor_slices(
                list(attribution.get("all_positive_pair_feature_slices") or []),
                top_n=max(max(1, int(top_n)), 100),
            ),
        },
    }


def build_factor_factory_control(
    report: dict[str, Any],
    *,
    min_outcomes: int = DEFAULT_MIN_OUTCOMES,
    focus_profit_factor: float = DEFAULT_FOCUS_PROFIT_FACTOR,
    pause_profit_factor: float = DEFAULT_PAUSE_PROFIT_FACTOR,
    min_avg_return_pct: float = DEFAULT_MIN_AVG_RETURN_PCT,
) -> dict[str, Any]:
    generated_at = str(report.get("generated_at") or now_iso())
    rules = report.get("candidate_rule_performance") if isinstance(report.get("candidate_rule_performance"), dict) else {}
    payload: dict[str, Any] = {
        "generated_at": generated_at,
        "mode": "shadow_research_only",
        "live_trading_enabled": False,
        "policy": {
            "min_outcomes": max(1, int(min_outcomes)),
            "focus_profit_factor_at_least": float(focus_profit_factor),
            "pause_profit_factor_below": float(pause_profit_factor),
            "pause_avg_return_pct_below": float(min_avg_return_pct),
        },
        "rules": {},
    }
    for rule_name, row in sorted(rules.items()):
        if not isinstance(row, dict):
            continue
        sample_count = int(row.get("sample_count") or 0)
        avg_return = safe_float(row.get("avg_return_pct"))
        profit_factor = safe_float(row.get("profit_factor"))
        status = "observing"
        reason = "insufficient_outcomes"
        if sample_count >= max(1, int(min_outcomes)):
            if profit_factor is not None and profit_factor >= float(focus_profit_factor) and (avg_return or 0.0) >= 0:
                status = "focus_shadow"
                reason = "factor_rule_positive_expectancy_candidate"
            elif profit_factor is not None and (
                profit_factor < float(pause_profit_factor) or (avg_return is not None and avg_return < float(min_avg_return_pct))
            ):
                status = "paused_shadow"
                reason = "factor_rule_negative_expectancy"
            else:
                status = "observing"
                reason = "factor_rule_mixed_evidence"
        payload["rules"][rule_name] = {
            "status": status,
            "status_reason": reason,
            "sample_count": sample_count,
            "trigger_count": int(row.get("trigger_count") or 0),
            "matched_outcome_side_count": int(row.get("matched_outcome_side_count") or 0),
            "avg_return_pct": avg_return,
            "win_rate_pct": safe_float(row.get("win_rate_pct")),
            "profit_factor": profit_factor,
            "sharpe_ratio": safe_float(row.get("sharpe_ratio")),
            "best_horizon": row.get("best_horizon"),
            "updated_at": generated_at,
        }
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build lightweight Phoenix factor factory and attribution reports.")
    parser.add_argument("--snapshots-file", type=Path, required=True)
    parser.add_argument("--outcomes-file", type=Path, required=True)
    parser.add_argument("--report-file", type=Path, required=True)
    parser.add_argument("--control-file", type=Path, default=None)
    parser.add_argument("--branch-type", default="research_pool")
    parser.add_argument("--max-snapshots", type=int, default=5000)
    parser.add_argument("--max-outcomes", type=int, default=5000)
    parser.add_argument("--min-samples", type=int, default=DEFAULT_MIN_OUTCOMES)
    parser.add_argument("--pair-min-samples", type=int, default=20)
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--include-non-research", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    snapshots = read_recent_jsonl_records(args.snapshots_file, max_records=max(0, int(args.max_snapshots or 0)))
    outcomes = read_recent_jsonl_records(args.outcomes_file, max_records=max(0, int(args.max_outcomes or 0)))
    report = build_factor_factory_report(
        snapshots=snapshots,
        outcomes=outcomes,
        branch_type=str(args.branch_type or "research_pool"),
        min_samples=max(1, int(args.min_samples)),
        pair_min_samples=max(1, int(args.pair_min_samples)),
        top_n=max(1, int(args.top)),
        include_non_research=bool(args.include_non_research),
    )
    args.report_file.parent.mkdir(parents=True, exist_ok=True)
    args.report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.control_file is not None:
        control = build_factor_factory_control(report, min_outcomes=max(1, int(args.min_samples)))
        args.control_file.parent.mkdir(parents=True, exist_ok=True)
        args.control_file.write_text(json.dumps(control, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    top_rule = (report.get("ranked_candidate_rules") or [{}])[0]
    print(
        "factor_factory_report "
        f"rows={report['input_counts']['rule_evaluation_rows']} "
        f"top_rule={top_rule.get('rule', 'none')} "
        f"top_avg={top_rule.get('avg_return_pct')} "
        f"top_pf={top_rule.get('profit_factor')} "
        f"report_file={args.report_file}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
