from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SIGNAL_EVENT_NAME = "signal_bridge_shadow_logged"
OUTCOME_EVENT_NAME = "signal_bridge_shadow_horizon_result"
RESEARCH_BRANCH_TYPE = "research_pool"
UNKNOWN = "unknown"


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def parse_shadow_instance_id(instance_id: Any) -> tuple[str, str] | None:
    text = str(instance_id or "").strip()
    if not text:
        return None
    if "::" in text:
        event_id, branch_id = text.rsplit("::", 1)
    elif ":" in text:
        event_id, branch_id = text.rsplit(":", 1)
    else:
        return None
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


def event_matches(row: dict[str, Any], expected: str) -> bool:
    event_name = str(row.get("event") or "").strip()
    return not event_name or event_name == expected


def find_branch_meta(signal: dict[str, Any] | None, branch_id: str) -> dict[str, Any]:
    if not signal:
        return {}
    branches = signal.get("shadow_branches")
    if not isinstance(branches, list):
        return {}
    target = normalize_branch_id(branch_id)
    for item in branches:
        if isinstance(item, dict) and normalize_branch_id(item.get("branch_id")) == target:
            return item
    return {}


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


def pnl_usdt_for_outcome(outcome: dict[str, Any], return_pct: float) -> float:
    allocation = safe_float(outcome.get("effective_quote_allocation_usdt"))
    if allocation is None:
        return return_pct
    return allocation * (return_pct / 100.0)


def compute_sharpe(returns_pct: list[float]) -> float | None:
    if len(returns_pct) < 2:
        return None
    mean_return = sum(returns_pct) / len(returns_pct)
    variance = sum((value - mean_return) ** 2 for value in returns_pct) / (len(returns_pct) - 1)
    if variance <= 0:
        return None
    std_dev = variance ** 0.5
    if std_dev <= 1e-12:
        return None
    return mean_return / std_dev


def compute_profit_factor(pnls_usdt: list[float]) -> float | None:
    gross_profit = sum(value for value in pnls_usdt if value > 0)
    gross_loss = abs(sum(value for value in pnls_usdt if value < 0))
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


def is_research_outcome(
    outcome: dict[str, Any],
    signal: dict[str, Any] | None,
    branch_meta: dict[str, Any],
) -> bool:
    research_only = (
        boolish(outcome.get("research_only"))
        or boolish(branch_meta.get("research_only"))
        or boolish(signal.get("research_only") if signal else None)
    )
    branch_type = first_present(
        outcome.get("branch_type"),
        branch_meta.get("branch_type"),
        signal.get("branch_type") if signal else None,
        default="",
    )
    return research_only and normalize_branch_type(branch_type) == RESEARCH_BRANCH_TYPE


def build_bucket_key(
    *,
    signal: dict[str, Any] | None,
    outcome: dict[str, Any],
    branch_id: str,
    horizon_sec: int,
) -> tuple[str, str, str, str, str, str, str, int]:
    sample = signal.get("sample") if signal and isinstance(signal.get("sample"), dict) else {}
    symbol = first_present(outcome.get("symbol"), signal.get("symbol") if signal else None).upper()
    playbook = first_present(outcome.get("playbook"), signal.get("playbook") if signal else None)
    research_direction = first_present(
        outcome.get("research_direction"),
        signal.get("research_direction") if signal else None,
        playbook,
    )
    return (
        symbol,
        trigger_signature(outcome, signal),
        first_present(outcome.get("bar_interval"), signal.get("bar_interval") if signal else None, sample.get("bar_interval")),
        first_present(outcome.get("trading_session"), signal.get("trading_session") if signal else None),
        playbook,
        research_direction,
        branch_id,
        horizon_sec,
    )


def build_bucket_payload(
    key: tuple[str, str, str, str, str, str, str, int],
    values: list[dict[str, float]],
) -> dict[str, Any]:
    symbol, trigger_type, bar_interval, trading_session, playbook, research_direction, branch_id, horizon_sec = key
    returns_pct = [value["return_pct"] for value in values]
    pnls_usdt = [value["pnl_usdt"] for value in values]
    sample_count = len(returns_pct)
    wins = [value for value in returns_pct if value > 0]
    losses = [value for value in returns_pct if value < 0]
    total_pnl = sum(pnls_usdt)
    win_rate_pct = (len(wins) / sample_count * 100.0) if sample_count else None
    mdd_pct = compute_mdd_pct(returns_pct)
    avg_return_pct = (sum(returns_pct) / sample_count) if sample_count else None
    return {
        "symbol": symbol,
        "trigger_type": trigger_type,
        "bar_interval": bar_interval,
        "trading_session": trading_session,
        "playbook": playbook,
        "research_direction": research_direction,
        "branch": branch_id,
        "branch_id": branch_id,
        "horizon": horizon_sec,
        "horizon_sec": horizon_sec,
        "samples": sample_count,
        "sample_count": sample_count,
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": win_rate_pct,
        "win_rate_pct": win_rate_pct,
        "profit_factor": compute_profit_factor(pnls_usdt),
        "sharpe": compute_sharpe(returns_pct),
        "mdd": mdd_pct,
        "mdd_pct": mdd_pct,
        "total_pnl": total_pnl,
        "total_pnl_usdt": total_pnl,
        "avg_return": avg_return_pct,
        "avg_return_pct": avg_return_pct,
    }


def build_research_shadow_report(
    signals: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    *,
    min_samples: int = 1,
) -> dict[str, Any]:
    signal_rows = [row for row in signals if event_matches(row, SIGNAL_EVENT_NAME)]
    outcome_rows = [row for row in outcomes if event_matches(row, OUTCOME_EVENT_NAME)]
    signal_by_event_id = {
        event_id: row
        for row in signal_rows
        if (event_id := row_event_id(row))
    }
    research_signal_count = sum(
        1
        for row in signal_rows
        if boolish(row.get("research_only")) and normalize_branch_type(row.get("branch_type")) == RESEARCH_BRANCH_TYPE
    )
    grouped: dict[tuple[str, str, str, str, str, str, str, int], list[dict[str, float]]] = defaultdict(list)
    research_outcome_count = 0
    bucketed_outcome_count = 0

    for outcome in outcome_rows:
        event_id = row_event_id(outcome)
        signal = signal_by_event_id.get(event_id)
        branch_id = row_branch_id(outcome)
        branch_meta = find_branch_meta(signal, branch_id)
        if not is_research_outcome(outcome, signal, branch_meta):
            continue
        research_outcome_count += 1
        return_pct = return_pct_for_outcome(outcome)
        if return_pct is None:
            continue
        horizon_sec = horizon_sec_for_outcome(outcome)
        key = build_bucket_key(
            signal=signal,
            outcome=outcome,
            branch_id=branch_id,
            horizon_sec=horizon_sec,
        )
        grouped[key].append(
            {
                "return_pct": return_pct,
                "pnl_usdt": pnl_usdt_for_outcome(outcome, return_pct),
            }
        )
        bucketed_outcome_count += 1

    threshold = max(1, int(min_samples))
    buckets = [
        build_bucket_payload(key, values)
        for key, values in sorted(grouped.items())
        if len(values) >= threshold
    ]
    buckets.sort(
        key=lambda item: (
            -int(item["sample_count"]),
            str(item["symbol"]),
            str(item["trigger_type"]),
            str(item["branch"]),
            int(item["horizon_sec"]),
        )
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "filters": {
            "research_only": True,
            "branch_type": RESEARCH_BRANCH_TYPE,
        },
        "min_samples": threshold,
        "bucket_dimensions": [
            "symbol",
            "trigger_type",
            "bar_interval",
            "trading_session",
            "playbook",
            "research_direction",
            "branch",
            "horizon_sec",
        ],
        "input_counts": {
            "signal_count": len(signal_rows),
            "research_signal_count": research_signal_count,
            "outcome_count": len(outcome_rows),
            "research_outcome_count": research_outcome_count,
            "bucketed_outcome_count": bucketed_outcome_count,
            "reported_bucket_count": len(buckets),
        },
        "buckets": buckets,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a research shadow performance report from JSONL files.")
    parser.add_argument("--signals-file", type=Path, required=True)
    parser.add_argument("--outcomes-file", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--min-samples", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    signals = read_jsonl_records(args.signals_file)
    outcomes = read_jsonl_records(args.outcomes_file)
    report = build_research_shadow_report(signals, outcomes, min_samples=args.min_samples)
    report.update(
        {
            "signals_file": str(args.signals_file),
            "outcomes_file": str(args.outcomes_file),
        }
    )
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    counts = report["input_counts"]
    print(
        "research_shadow_report "
        f"research_outcome_count={counts['research_outcome_count']} "
        f"reported_bucket_count={counts['reported_bucket_count']} "
        f"output_json={args.output_json}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
