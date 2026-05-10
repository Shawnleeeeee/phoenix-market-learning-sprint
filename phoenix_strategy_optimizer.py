#!/usr/bin/env python3
"""Research-only optimizer for Phoenix oi_build_breakout candidates.

The optimizer reads historical playbook trades, evaluates entry-time filters
with a train-before-test walk-forward policy, and writes JSON/Markdown reports.
It never places orders and never changes live execution configuration.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


DEFAULT_FOLDS = (
    ((2022,), (2023,)),
    ((2022, 2023), (2024,)),
    ((2022, 2023, 2024), (2025,)),
    ((2022, 2023, 2024, 2025), (2026,)),
)
DEFAULT_SHADOW_ACCEPTANCE_MIN_TRADES = 300
DEFAULT_DAILY_LOSS_LIMIT_PCT = 2.0
REQUIRED_SHADOW_COST_FIELDS = (
    "funding_rate_at_entry",
    "funding_paid_during_hold",
    "spread_bps_at_entry",
    "order_latency_ms",
    "estimated_slippage_bps",
    "maker_or_taker",
    "liquidity_bucket",
)


@dataclass(frozen=True)
class TradeRow:
    symbol: str
    year: int
    quarter: str
    playbook: str
    side: str
    session: str
    oi_change_5m_pct: float
    oi_change_15m_pct: float
    trigger_score: float
    after_fee_return_pct: float
    notional_usdt: float
    entry_time: str = ""
    entry_date: str = ""
    funding_rate_at_entry: float | None = None
    funding_paid_during_hold: float | None = None
    spread_bps_at_entry: float | None = None
    order_latency_ms: float | None = None
    estimated_slippage_bps: float | None = None
    maker_or_taker: str = "unknown"
    liquidity_bucket: str = "unknown"


@dataclass(frozen=True)
class CandidateRule:
    rule_id: str
    oi5_min: float
    score_min: float
    oi15_min: float | None = None
    sessions: tuple[str, ...] | None = None
    sides: tuple[str, ...] | None = None

    def matches(self, row: TradeRow) -> bool:
        if row.playbook != "oi_build_breakout":
            return False
        if row.oi_change_5m_pct < self.oi5_min:
            return False
        if self.oi15_min is not None and row.oi_change_15m_pct < self.oi15_min:
            return False
        if row.trigger_score < self.score_min:
            return False
        if self.sessions is not None and row.session not in self.sessions:
            return False
        if self.sides is not None and row.side not in self.sides:
            return False
        return True

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "base_playbook": "oi_build_breakout",
            "conditions": [
                {"field": "playbook", "operator": "==", "value": "oi_build_breakout"},
                {"field": "oi_change_5m_pct", "operator": ">=", "value": self.oi5_min},
                {"field": "trigger_score", "operator": ">=", "value": self.score_min},
                *(
                    []
                    if self.oi15_min is None
                    else [{"field": "oi_change_15m_pct", "operator": ">=", "value": self.oi15_min}]
                ),
                *(
                    []
                    if self.sessions is None
                    else [{"field": "trading_session", "operator": "in", "value": list(self.sessions)}]
                ),
                *([] if self.sides is None else [{"field": "side", "operator": "in", "value": list(self.sides)}]),
            ],
        }


@dataclass(frozen=True)
class ShadowStrategyConfig:
    strategy_id: str
    label: str
    rule: CandidateRule
    mode: str = "shadow_paper"
    live_trading_enabled: bool = False
    promotion_allowed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "label": self.label,
            "mode": self.mode,
            "live_trading_enabled": self.live_trading_enabled,
            "promotion_allowed": self.promotion_allowed,
            "paper_record_only": True,
            "rule": self.rule.as_dict(),
        }


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = -999.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def optional_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def first_optional_float(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = optional_float(payload.get(key))
        if value is not None:
            return value
    return None


def liquidity_bucket_from_quote_volume(quote_volume_24h: float | None) -> str:
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


def row_from_payload(payload: dict[str, Any]) -> TradeRow | None:
    entry_time = str(payload.get("entry_time") or payload.get("signal_time") or "")
    if len(entry_time) < 7:
        return None
    try:
        year = int(entry_time[:4])
        month = int(entry_time[5:7])
    except ValueError:
        return None
    quarter = f"{year}-Q{((month - 1) // 3) + 1}"
    quote = safe_float(payload.get("quote_allocation_usdt"), 10.0)
    leverage = safe_float(payload.get("leverage"), 10.0)
    notional = safe_float(payload.get("notional_usdt"), quote * leverage)
    entry_date = str(payload.get("entry_date") or (entry_time[:10] if len(entry_time) >= 10 else ""))
    funding_rate_at_entry = first_optional_float(payload, "funding_rate_at_entry", "funding_rate")
    spread_bps_at_entry = first_optional_float(payload, "spread_bps_at_entry", "spread_bps")
    order_latency_ms = first_optional_float(payload, "order_latency_ms")
    estimated_slippage_bps = first_optional_float(payload, "estimated_slippage_bps", "slippage_bps")
    funding_paid_during_hold = first_optional_float(
        payload,
        "funding_paid_during_hold",
        "funding_paid_during_hold_usdt",
        "funding_fee_usdt",
    )
    quote_volume_24h = first_optional_float(payload, "quote_volume_24h", "live_universe_quote_volume_24h")
    maker_or_taker = str(payload.get("maker_or_taker") or "unknown").strip().lower() or "unknown"
    liquidity_bucket = str(payload.get("liquidity_bucket") or "").strip().lower()
    if not liquidity_bucket:
        liquidity_bucket = liquidity_bucket_from_quote_volume(quote_volume_24h)
    return TradeRow(
        symbol=str(payload.get("symbol") or ""),
        year=year,
        quarter=quarter,
        playbook=str(payload.get("playbook") or ""),
        side=str(payload.get("side") or ""),
        session=str(payload.get("trading_session") or ""),
        oi_change_5m_pct=safe_float(payload.get("oi_change_5m_pct")),
        oi_change_15m_pct=safe_float(payload.get("oi_change_15m_pct")),
        trigger_score=safe_float(payload.get("trigger_score")),
        after_fee_return_pct=safe_float(payload.get("after_fee_return_pct"), 0.0),
        notional_usdt=notional,
        entry_time=entry_time,
        entry_date=entry_date,
        funding_rate_at_entry=funding_rate_at_entry,
        funding_paid_during_hold=funding_paid_during_hold,
        spread_bps_at_entry=spread_bps_at_entry,
        order_latency_ms=order_latency_ms,
        estimated_slippage_bps=estimated_slippage_bps,
        maker_or_taker=maker_or_taker,
        liquidity_bucket=liquidity_bucket,
    )


def read_trade_rows(path: Path) -> list[TradeRow]:
    rows: list[TradeRow] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = row_from_payload(json.loads(line))
            if row is not None:
                rows.append(row)
    return rows


def compute_metrics(rows: Iterable[TradeRow], *, extra_round_trip_cost_pct: float = 0.0) -> dict[str, Any]:
    materialized = list(rows)
    if not materialized:
        return {
            "trade_count": 0,
            "win_rate": 0.0,
            "win_rate_pct": 0.0,
            "avg_after_fee_return": 0.0,
            "avg_after_fee_return_pct": 0.0,
            "avg_after_cost_return_pct": 0.0,
            "median_after_cost_return_pct": 0.0,
            "profit_factor": 0.0,
            "total_pnl_usdt": 0.0,
        }
    returns = []
    for row in materialized:
        funding_paid = float(row.funding_paid_during_hold or 0.0)
        funding_cost_pct = (funding_paid / row.notional_usdt) * 100.0 if row.notional_usdt > 0 else 0.0
        returns.append(row.after_fee_return_pct - funding_cost_pct - extra_round_trip_cost_pct)
    pnl = [row.notional_usdt * ret / 100.0 for row, ret in zip(materialized, returns, strict=True)]
    gains = sum(item for item in pnl if item > 0)
    losses = -sum(item for item in pnl if item < 0)
    profit_factor = gains / losses if losses else 999.0
    win_rate = sum(1 for item in returns if item > 0) / len(returns)
    avg_return = sum(returns) / len(returns)
    return {
        "trade_count": len(materialized),
        "win_rate": round(win_rate, 6),
        "win_rate_pct": round(100.0 * win_rate, 6),
        "avg_after_fee_return": round(avg_return, 6),
        "avg_after_fee_return_pct": round(avg_return, 6),
        "avg_after_cost_return_pct": round(avg_return, 6),
        "median_after_cost_return_pct": round(statistics.median(returns), 6),
        "profit_factor": round(profit_factor, 6),
        "total_pnl_usdt": round(sum(pnl), 6),
    }


def group_by(rows: Iterable[TradeRow], field: str) -> dict[str, list[TradeRow]]:
    grouped: dict[str, list[TradeRow]] = {}
    for row in rows:
        key = str(getattr(row, field))
        grouped.setdefault(key, []).append(row)
    return grouped


def metrics_by_group(rows: Iterable[TradeRow], field: str) -> dict[str, dict[str, Any]]:
    grouped = group_by(rows, field)
    return {
        key: compute_metrics(group_rows)
        for key, group_rows in sorted(grouped.items())
        if key and key != "None"
    }


def top_symbol_share(rows: list[TradeRow]) -> float:
    total = compute_metrics(rows)["total_pnl_usdt"]
    if total <= 0:
        return 0.0
    symbol_pnls = [compute_metrics(symbol_rows)["total_pnl_usdt"] for symbol_rows in group_by(rows, "symbol").values()]
    return round(max(symbol_pnls, default=0.0) / total, 6)


def required_shadow_field_coverage(rows: Iterable[TradeRow]) -> dict[str, Any]:
    materialized = list(rows)

    def has_value(row: TradeRow, field: str) -> bool:
        value = getattr(row, field)
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip()) and value.strip().lower() != "unknown"
        return True

    field_counts = {
        field: sum(1 for row in materialized if has_value(row, field))
        for field in REQUIRED_SHADOW_COST_FIELDS
    }
    complete_count = sum(
        1
        for row in materialized
        if all(has_value(row, field) for field in REQUIRED_SHADOW_COST_FIELDS)
    )
    total = len(materialized)
    return {
        "required_field_count": len(REQUIRED_SHADOW_COST_FIELDS),
        "required_fields": list(REQUIRED_SHADOW_COST_FIELDS),
        "trade_count": total,
        "field_counts": field_counts,
        "complete_trade_count": complete_count,
        "complete_coverage": round(complete_count / total, 6) if total else 0.0,
    }


def evaluate_daily_loss_kill_switch(
    rows: Iterable[TradeRow],
    *,
    daily_loss_limit_pct: float = DEFAULT_DAILY_LOSS_LIMIT_PCT,
) -> dict[str, Any]:
    materialized = list(rows)
    dated_rows = [row for row in materialized if row.entry_date]
    daily_rows = []
    for entry_date, group_rows in sorted(group_by(dated_rows, "entry_date").items()):
        total_notional = sum(max(0.0, row.notional_usdt) for row in group_rows)
        total_pnl = compute_metrics(group_rows)["total_pnl_usdt"]
        daily_return_pct = (total_pnl / total_notional) * 100.0 if total_notional > 0 else 0.0
        breached = daily_return_pct <= -abs(float(daily_loss_limit_pct or 0.0))
        daily_rows.append(
            {
                "entry_date": entry_date,
                "trade_count": len(group_rows),
                "total_pnl_usdt": round(total_pnl, 6),
                "daily_return_pct": round(daily_return_pct, 6),
                "breached": breached,
            }
        )
    enabled = daily_loss_limit_pct > 0
    coverage = len(dated_rows) / len(materialized) if materialized else 0.0
    return {
        "enabled": enabled,
        "threshold_pct": abs(float(daily_loss_limit_pct or 0.0)),
        "evaluated_trade_count": len(dated_rows),
        "evaluated_day_count": len(daily_rows),
        "coverage": round(coverage, 6),
        "breach_count": sum(1 for row in daily_rows if row["breached"]),
        "worst_daily_return_pct": min((row["daily_return_pct"] for row in daily_rows), default=None),
        "kill_switch_effective": bool(enabled and materialized and len(dated_rows) == len(materialized)),
        "daily": daily_rows,
    }


def positive_group_share(rows: list[TradeRow], field: str) -> float:
    groups = group_by(rows, field)
    if not groups:
        return 0.0
    positive = sum(1 for group_rows in groups.values() if compute_metrics(group_rows)["avg_after_cost_return_pct"] > 0)
    return round(positive / len(groups), 6)


def all_groups_positive(rows: list[TradeRow], field: str) -> bool:
    groups = group_by(rows, field)
    return bool(groups) and all(compute_metrics(group_rows)["avg_after_cost_return_pct"] > 0 for group_rows in groups.values())


def filter_rows(rows: Iterable[TradeRow], rule: CandidateRule, years: Iterable[int] | None = None) -> list[TradeRow]:
    year_set = set(years) if years is not None else None
    return [row for row in rows if (year_set is None or row.year in year_set) and rule.matches(row)]


def evaluate_rule(
    rows: list[TradeRow],
    rule: CandidateRule,
    *,
    folds: tuple[tuple[tuple[int, ...], tuple[int, ...]], ...] = DEFAULT_FOLDS,
    min_train_trades: int = 1000,
    min_train_profit_factor: float = 1.2,
) -> dict[str, Any]:
    fold_reports: list[dict[str, Any]] = []
    selected_oos_rows: list[TradeRow] = []
    for train_years, test_years in folds:
        train_rows = filter_rows(rows, rule, train_years)
        test_rows = filter_rows(rows, rule, test_years)
        train_metrics = compute_metrics(train_rows)
        oos_metrics = compute_metrics(test_rows)
        selected = (
            train_metrics["trade_count"] >= min_train_trades
            and train_metrics["profit_factor"] >= min_train_profit_factor
            and train_metrics["avg_after_cost_return_pct"] > 0
        )
        if selected:
            selected_oos_rows.extend(test_rows)
        fold_reports.append(
            {
                "fold": f"train_{train_years[0]}_{train_years[-1]}_test_{test_years[0]}",
                "train_years": list(train_years),
                "test_years": list(test_years),
                "selected_by_train_gate": selected,
                "train": train_metrics,
                "oos": oos_metrics,
            }
        )
    oos_summary = compute_metrics(selected_oos_rows)
    stress_10bps = compute_metrics(selected_oos_rows, extra_round_trip_cost_pct=0.10)
    stress_20bps = compute_metrics(selected_oos_rows, extra_round_trip_cost_pct=0.20)
    stress_25bps = compute_metrics(selected_oos_rows, extra_round_trip_cost_pct=0.25)
    top1_pnl_share = top_symbol_share(selected_oos_rows)
    daily_loss_report = evaluate_daily_loss_kill_switch(selected_oos_rows)
    selected_count = sum(1 for item in fold_reports if item["selected_by_train_gate"])
    positive_oos_count = sum(
        1
        for item in fold_reports
        if item["selected_by_train_gate"] and item["oos"]["avg_after_cost_return_pct"] > 0
    )
    gates = {
        "selected_in_at_least_3_folds": selected_count >= 3,
        "positive_oos_folds_ge_3": positive_oos_count >= 3,
        "oos_sample_ge_300": oos_summary["trade_count"] >= DEFAULT_SHADOW_ACCEPTANCE_MIN_TRADES,
        "oos_pf_ge_1_2": oos_summary["profit_factor"] >= 1.2,
        "oos_avg_positive": oos_summary["avg_after_cost_return_pct"] > 0,
        "positive_oos_years_all": all_groups_positive(selected_oos_rows, "year"),
        "positive_oos_quarter_share_ge_65pct": positive_group_share(selected_oos_rows, "quarter") >= 0.65,
        "top1_pnl_share_le_20pct": top1_pnl_share <= 0.20,
        "stress_10bps_pf_ge_1_15": stress_10bps["profit_factor"] >= 1.15,
        "stress_20bps_pf_ge_1_0": stress_20bps["profit_factor"] >= 1.0,
    }
    acceptance_gates = {
        "shadow_trade_count_ge_300": oos_summary["trade_count"] >= DEFAULT_SHADOW_ACCEPTANCE_MIN_TRADES,
        "avg_after_fee_return_gt_0": oos_summary["avg_after_fee_return"] > 0,
        "stress_20bps_profit_factor_gt_1_2": stress_20bps["profit_factor"] > 1.2,
        "top_symbol_pnl_share_le_20pct": top1_pnl_share <= 0.20,
        "daily_loss_kill_switch_effective": bool(daily_loss_report["kill_switch_effective"]),
    }
    return {
        **rule.as_dict(),
        "selected_fold_count": selected_count,
        "positive_oos_fold_count": positive_oos_count,
        "folds": fold_reports,
        "oos_summary": oos_summary,
        "per_year": metrics_by_group(selected_oos_rows, "year"),
        "per_quarter": metrics_by_group(selected_oos_rows, "quarter"),
        "stress_tests": {
            "extra_10bps_round_trip": stress_10bps,
            "extra_20bps_round_trip": stress_20bps,
            "extra_25bps_round_trip": stress_25bps,
        },
        "stability": {
            "positive_years_all": gates["positive_oos_years_all"],
            "positive_quarter_share": positive_group_share(selected_oos_rows, "quarter"),
            "top_symbol_pnl_share": top1_pnl_share,
        },
        "field_coverage": required_shadow_field_coverage(selected_oos_rows),
        "daily_loss_kill_switch": daily_loss_report,
        "acceptance_gates": acceptance_gates,
        "passes_shadow_acceptance_gate": all(acceptance_gates.values()),
        "gates": gates,
        "survives_optimization_gate": all(gates.values()),
        "rank_score": rank_score(
            oos_summary=oos_summary,
            stress_10bps=stress_10bps,
            stress_20bps=stress_20bps,
            positive_quarter_share=positive_group_share(selected_oos_rows, "quarter"),
            positive_years_all=gates["positive_oos_years_all"],
        ),
    }


def rank_score(
    *,
    oos_summary: dict[str, Any],
    stress_10bps: dict[str, Any],
    stress_20bps: dict[str, Any],
    positive_quarter_share: float,
    positive_years_all: bool,
) -> float:
    trade_count = max(1, int(oos_summary["trade_count"]))
    score = 0.0
    score += (min(float(oos_summary["profit_factor"]), 2.2) - 1.0) * 4.0
    score += min(float(oos_summary["avg_after_cost_return_pct"]), 0.6) * 3.0
    score += math.log10(trade_count) * 0.25
    score += max(0.0, float(stress_10bps["profit_factor"]) - 1.0) * 2.0
    score += max(0.0, float(stress_20bps["profit_factor"]) - 1.0)
    score += positive_quarter_share
    score += 1.0 if positive_years_all else -2.0
    return round(score, 6)


def build_shadow_strategy_configs() -> list[ShadowStrategyConfig]:
    return [
        ShadowStrategyConfig(
            strategy_id="strategy_a_quality",
            label="Strategy A - high quality, fewer trades",
            rule=CandidateRule(
                rule_id=rule_id(1.0, 60.0, oi15_min=5.0),
                oi5_min=1.0,
                oi15_min=5.0,
                score_min=60.0,
            ),
        ),
        ShadowStrategyConfig(
            strategy_id="strategy_b_balanced",
            label="Strategy B - balanced",
            rule=CandidateRule(
                rule_id=rule_id(2.0, 60.0),
                oi5_min=2.0,
                score_min=60.0,
            ),
        ),
    ]


def build_candidate_rules() -> list[CandidateRule]:
    rules: list[CandidateRule] = []
    for oi5 in (0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0):
        for score in (0.0, 50.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0):
            rules.append(CandidateRule(rule_id=rule_id(oi5, score), oi5_min=oi5, score_min=score))
    for oi5 in (1.0, 1.5, 2.0, 2.5, 3.0):
        for oi15 in (0.0, 1.0, 2.0, 3.0, 5.0):
            for score in (50.0, 60.0, 70.0, 80.0):
                rules.append(
                    CandidateRule(rule_id=rule_id(oi5, score, oi15_min=oi15), oi5_min=oi5, oi15_min=oi15, score_min=score)
                )
    # Keep side/session variants limited to the existing primary and high-conviction families.
    for oi5, score in ((1.0, 60.0), (2.0, 60.0), (3.0, 60.0), (1.0, 70.0)):
        for sessions in (("Asia", "Europe", "US"), ("Asia", "Europe"), ("US",), ("Asia",), ("Europe",)):
            rules.append(
                CandidateRule(
                    rule_id=rule_id(oi5, score, sessions=sessions),
                    oi5_min=oi5,
                    score_min=score,
                    sessions=sessions,
                )
            )
        for sides in (("BUY",), ("SELL",)):
            rules.append(
                CandidateRule(rule_id=rule_id(oi5, score, sides=sides), oi5_min=oi5, score_min=score, sides=sides)
            )
    unique: dict[str, CandidateRule] = {}
    for item in rules:
        unique[item.rule_id] = item
    return list(unique.values())


def clean_number(value: float) -> str:
    text = f"{value:g}".replace(".", "_")
    return text.replace("-", "neg_")


def rule_id(
    oi5_min: float,
    score_min: float,
    *,
    oi15_min: float | None = None,
    sessions: tuple[str, ...] | None = None,
    sides: tuple[str, ...] | None = None,
) -> str:
    pieces = ["OPT_OI_BUILD", f"OI5_GE_{clean_number(oi5_min)}"]
    if oi15_min is not None:
        pieces.append(f"OI15_GE_{clean_number(oi15_min)}")
    pieces.append(f"SCORE_GE_{clean_number(score_min)}")
    if sessions:
        pieces.append("SESS_" + "_".join(sessions).upper())
    if sides:
        pieces.append("SIDE_" + "_".join(sides).upper())
    return "_".join(pieces)


def candidate_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "rule_id": report["rule_id"],
        "conditions": report["conditions"],
        "selected_fold_count": report["selected_fold_count"],
        "positive_oos_fold_count": report["positive_oos_fold_count"],
        "oos_summary": report["oos_summary"],
        "per_year": report["per_year"],
        "per_quarter": report["per_quarter"],
        "stress_tests": report["stress_tests"],
        "stability": report["stability"],
        "field_coverage": report["field_coverage"],
        "daily_loss_kill_switch": report["daily_loss_kill_switch"],
        "acceptance_gates": report["acceptance_gates"],
        "passes_shadow_acceptance_gate": report["passes_shadow_acceptance_gate"],
        "gates": report["gates"],
        "survives_optimization_gate": report["survives_optimization_gate"],
        "rank_score": report["rank_score"],
    }


def build_optimization_report(rows: list[TradeRow], *, top_n: int) -> dict[str, Any]:
    reports = [evaluate_rule(rows, rule) for rule in build_candidate_rules()]
    reports_by_rule_id = {item["rule_id"]: item for item in reports}
    shadow_strategy_configs = build_shadow_strategy_configs()
    eligible = [
        item
        for item in reports
        if item["selected_fold_count"] >= 3
        and item["positive_oos_fold_count"] >= 3
        and item["oos_summary"]["trade_count"] >= DEFAULT_SHADOW_ACCEPTANCE_MIN_TRADES
        and item["oos_summary"]["profit_factor"] >= 1.15
        and item["oos_summary"]["avg_after_cost_return_pct"] > 0
    ]
    ranked = sorted(eligible, key=lambda item: item["rank_score"], reverse=True)
    survivors = [item for item in ranked if item["survives_optimization_gate"]]
    recommended_full = survivors[0] if survivors else (ranked[0] if ranked else reports[0])
    return {
        "generated_at": now_iso(),
        "report_type": "phoenix_strategy_optimization",
        "research_only": True,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "input_trade_count": len(rows),
        "candidate_count": len(reports),
        "eligible_candidate_count": len(eligible),
        "survivor_count": len(survivors),
        "recommended_candidate": candidate_summary(recommended_full),
        "recommended_candidate_full": recommended_full,
        "top_survivors": [candidate_summary(item) for item in survivors[:top_n]],
        "top_ranked_candidates": [candidate_summary(item) for item in ranked[:top_n]],
        "shadow_strategy_configs": [item.as_dict() for item in shadow_strategy_configs],
        "shadow_strategy_candidates": [
            candidate_summary(reports_by_rule_id[item.rule.rule_id])
            for item in shadow_strategy_configs
            if item.rule.rule_id in reports_by_rule_id
        ],
        "known_baseline_candidates": [
            candidate_summary(item)
            for wanted in (
                "OPT_OI_BUILD_OI5_GE_1_OI15_GE_5_SCORE_GE_60",
                "OPT_OI_BUILD_OI5_GE_1_SCORE_GE_60",
                "OPT_OI_BUILD_OI5_GE_2_SCORE_GE_60",
                "OPT_OI_BUILD_OI5_GE_3_SCORE_GE_60",
                "OPT_OI_BUILD_OI5_GE_1_SCORE_GE_70",
            )
            for item in reports
            if item["rule_id"] == wanted
        ],
        "policy": {
            "train_before_test_only": True,
            "folds": [
                {"train_years": list(train_years), "test_years": list(test_years)}
                for train_years, test_years in DEFAULT_FOLDS
            ],
            "train_gate": {
                "min_train_trades": 1000,
                "min_train_profit_factor": 1.2,
                "requires_positive_train_avg": True,
            },
            "optimization_gate": {
                "selected_in_at_least_3_folds": True,
                "positive_oos_folds_ge_3": True,
                "oos_sample_ge_300": True,
                "oos_pf_ge_1_2": True,
                "oos_avg_positive": True,
                "positive_oos_years_all": True,
                "positive_oos_quarter_share_ge_65pct": True,
                "top1_pnl_share_le_20pct": True,
                "stress_10bps_pf_ge_1_15": True,
                "stress_20bps_pf_ge_1_0": True,
            },
            "shadow_acceptance_gate": {
                "shadow_trade_count_ge_300": True,
                "avg_after_fee_return_gt_0": True,
                "stress_20bps_profit_factor_gt_1_2": True,
                "top_symbol_pnl_share_le_20pct": True,
                "daily_loss_kill_switch_effective": True,
            },
        },
        "promotion_blockers": [
            "research_only_optimizer",
            "requires_forward_shadow_validation",
            "requires_exchange_latency_slippage_realism_check",
            "requires_daily_loss_cap_and_kill_switch_review",
            "requires_binance_restricted_location_solution_before_mainnet_shadow",
        ],
    }


def format_metric(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def render_markdown(report: dict[str, Any]) -> str:
    recommended = report["recommended_candidate"]
    recommended_full = report["recommended_candidate_full"]
    lines = [
        "# Phoenix Strategy Optimization Report",
        "",
        f"- Generated: `{report['generated_at']}`",
        "- Mode: `research_only`",
        "- Live trading: `false`",
        "- Promotion allowed: `false`",
        f"- Input trades: `{report['input_trade_count']}`",
        f"- Candidates scanned: `{report['candidate_count']}`",
        f"- Survivors: `{report['survivor_count']}`",
        f"- Recommended shadow candidate: `{recommended['rule_id']}`",
        "",
        "## Shadow Strategy Configs",
        "",
        "| strategy | rule | live | promotion | mode |",
        "|---|---|---|---|---|",
    ]
    for config in report["shadow_strategy_configs"]:
        lines.append(
            "| {strategy} | {rule} | {live} | {promotion} | {mode} |".format(
                strategy=config["strategy_id"],
                rule=config["rule"]["rule_id"],
                live=str(config["live_trading_enabled"]).lower(),
                promotion=str(config["promotion_allowed"]).lower(),
                mode=config["mode"],
            )
        )
    lines.extend(
        [
        "",
        "## Recommendation",
        "",
        "| rule | selected folds | trade_count | win_rate | avg_after_fee_return | profit_factor | PF +10bps | PF +20bps | PF +25bps | top_symbol_pnl_share |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for item in report["top_survivors"][:10]:
        summary = item["oos_summary"]
        stress = item["stress_tests"]
        stability = item["stability"]
        lines.append(
            "| {rule} | {folds}/4 | {trades} | {win_rate} | {avg}% | {pf} | {pf10} | {pf20} | {pf25} | {topshare} |".format(
                rule=item["rule_id"],
                folds=item["selected_fold_count"],
                trades=summary["trade_count"],
                win_rate=format_metric(summary["win_rate"]),
                pf=format_metric(summary["profit_factor"]),
                avg=format_metric(summary["avg_after_fee_return"]),
                pf10=format_metric(stress["extra_10bps_round_trip"]["profit_factor"]),
                pf20=format_metric(stress["extra_20bps_round_trip"]["profit_factor"]),
                pf25=format_metric(stress["extra_25bps_round_trip"]["profit_factor"]),
                topshare=format_metric(stability["top_symbol_pnl_share"]),
            )
        )
    lines.extend(
        [
            "",
            "## Recommended Fold Detail",
            "",
            "| fold | selected | train trades | train PF | OOS trades | OOS PF | OOS avg |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for fold in recommended_full["folds"]:
        train = fold["train"]
        oos = fold["oos"]
        lines.append(
            "| {fold} | {selected} | {train_trades} | {train_pf} | {oos_trades} | {oos_pf} | {oos_avg}% |".format(
                fold=fold["fold"],
                selected=str(fold["selected_by_train_gate"]).lower(),
                train_trades=train["trade_count"],
                train_pf=format_metric(train["profit_factor"]),
                oos_trades=oos["trade_count"],
                oos_pf=format_metric(oos["profit_factor"]),
                oos_avg=format_metric(oos["avg_after_cost_return_pct"]),
            )
        )
    lines.extend(
        [
            "",
            "## Baseline Comparison",
            "",
            "| rule | selected folds | trade_count | win_rate | avg_after_fee_return | profit_factor | PF +10bps | PF +20bps | PF +25bps | top_symbol_pnl_share | acceptance |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for item in report["known_baseline_candidates"]:
        summary = item["oos_summary"]
        stress = item["stress_tests"]
        stability = item["stability"]
        lines.append(
            "| {rule} | {folds}/4 | {trades} | {win_rate} | {avg}% | {pf} | {pf10} | {pf20} | {pf25} | {topshare} | {acceptance} |".format(
                rule=item["rule_id"],
                folds=item["selected_fold_count"],
                trades=summary["trade_count"],
                win_rate=format_metric(summary["win_rate"]),
                pf=format_metric(summary["profit_factor"]),
                avg=format_metric(summary["avg_after_fee_return"]),
                pf10=format_metric(stress["extra_10bps_round_trip"]["profit_factor"]),
                pf20=format_metric(stress["extra_20bps_round_trip"]["profit_factor"]),
                pf25=format_metric(stress["extra_25bps_round_trip"]["profit_factor"]),
                topshare=format_metric(stability["top_symbol_pnl_share"]),
                acceptance=str(item["passes_shadow_acceptance_gate"]).lower(),
            )
        )
    lines.extend(
        [
            "",
            "## Recommended Per-Year",
            "",
            "| year | trade_count | win_rate | avg_after_fee_return | profit_factor |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for year, metrics in recommended_full["per_year"].items():
        lines.append(
            "| {year} | {trades} | {win_rate} | {avg}% | {pf} |".format(
                year=year,
                trades=metrics["trade_count"],
                win_rate=format_metric(metrics["win_rate"]),
                avg=format_metric(metrics["avg_after_fee_return"]),
                pf=format_metric(metrics["profit_factor"]),
            )
        )
    lines.extend(
        [
            "",
            "## Recommended Per-Quarter",
            "",
            "| quarter | trade_count | win_rate | avg_after_fee_return | profit_factor |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for quarter, metrics in recommended_full["per_quarter"].items():
        lines.append(
            "| {quarter} | {trades} | {win_rate} | {avg}% | {pf} |".format(
                quarter=quarter,
                trades=metrics["trade_count"],
                win_rate=format_metric(metrics["win_rate"]),
                avg=format_metric(metrics["avg_after_fee_return"]),
                pf=format_metric(metrics["profit_factor"]),
            )
        )
    lines.extend(
        [
            "",
            "## Operator Notes",
            "",
            "- Keep live trading locked; this report only selects shadow candidates.",
            "- Treat the recommended rule as a cost-robust shadow candidate, not a production strategy.",
            "- Run forward shadow validation before promotion; the minimum useful gate is 300 paper trades, PF >= 1.2, and positive after-cost average.",
            "- Re-check actual user commission, funding, latency, and slippage before any live sizing decision.",
            "",
            "## Promotion Blockers",
            "",
        ]
    )
    lines.extend(f"- `{item}`" for item in report["promotion_blockers"])
    return "\n".join(lines) + "\n"


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(text, encoding="utf-8")
    temp.replace(path)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trades-file", required=True, type=Path)
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--output-md", required=True, type=Path)
    parser.add_argument("--top-n", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = read_trade_rows(args.trades_file)
    report = build_optimization_report(rows, top_n=max(1, int(args.top_n)))
    atomic_write_json(args.output_json, report)
    atomic_write_text(args.output_md, render_markdown(report))
    print(
        json.dumps(
            {
                "event": "strategy_optimization_complete",
                "output_json": str(args.output_json),
                "output_md": str(args.output_md),
                "recommended_candidate": report["recommended_candidate"]["rule_id"],
                "survivor_count": report["survivor_count"],
                "live_trading_enabled": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
