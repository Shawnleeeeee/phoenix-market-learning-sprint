from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable


UNKNOWN = "unknown"
OUTCOME_EVENT_NAME = "signal_bridge_shadow_horizon_result"
SIGNAL_EVENT_NAME = "signal_bridge_shadow_logged"
RESEARCH_SOURCE_MAINNET_SHADOW = "MAINNET_SHADOW"
RESEARCH_SOURCE_TESTNET_LIVE = "TESTNET_LIVE"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def safe_int(value: Any) -> int | None:
    number = safe_float(value)
    if number is None:
        return None
    return int(number)


def round_optional(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def normalized_text(value: Any, *, default: str = UNKNOWN) -> str:
    text = str(value or "").strip()
    return text if text else default


def normalize_branch_id(value: Any) -> str:
    return normalized_text(value, default="LEGACY")


def event_matches(row: dict[str, Any], expected: str) -> bool:
    event_name = str(row.get("event") or "").strip()
    return not event_name or event_name == expected


def read_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def read_jsonl_records(path: Path, *, max_records: int = 0) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        handle = path.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return rows
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
                rows.append(payload)
                if max_records > 0 and len(rows) >= max_records:
                    break
    return rows


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


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


def source_event_id(row: dict[str, Any]) -> str:
    for key in ("source_event_id", "horizon_event_id", "sourceEventId"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    event_id = row_event_id(row)
    if "::" in event_id:
        return event_id.rsplit("::", 1)[0]
    return event_id


def research_source(row: dict[str, Any]) -> str:
    value = normalized_text(
        row.get("research_source") or row.get("_research_source") or row.get("execution_mode"),
        default=RESEARCH_SOURCE_MAINNET_SHADOW,
    )
    return value.upper()


def independent_event_key(row: dict[str, Any]) -> str:
    source_id = source_event_id(row) or row_event_id(row)
    if not source_id:
        return ""
    return f"{research_source(row)}:{source_id}"


def branch_id(row: dict[str, Any]) -> str:
    if research_source(row) == RESEARCH_SOURCE_TESTNET_LIVE:
        return RESEARCH_SOURCE_TESTNET_LIVE
    for key in ("shadow_branch_id", "branch_id", "strategy_id", "exit_profile"):
        if row.get(key):
            return normalize_branch_id(row.get(key))
    parsed = parse_shadow_instance_id(row.get("shadow_instance_id"))
    return parsed[1] if parsed else "LEGACY"


def strategy_id(row: dict[str, Any]) -> str:
    for key in ("strategy_id", "strategy_family", "research_direction", "direction_variant", "setup", "candidate_setup"):
        value = normalized_text(row.get(key), default="")
        if value:
            return value
    return normalized_text(row.get("playbook"))


def playbook(row: dict[str, Any]) -> str:
    return normalized_text(row.get("playbook"))


def return_pct(row: dict[str, Any]) -> float | None:
    for key in (
        "after_real_cost_return_pct",
        "after_fee_and_slippage_return_pct",
        "after_fee_return_pct",
        "final_return_pct",
        "close_return_pct",
        "return_pct",
    ):
        number = safe_float(row.get(key))
        if number is not None:
            return number
    net_pnl = safe_float(row.get("net_pnl_usdt"))
    if net_pnl is not None:
        allocation = safe_float(row.get("effective_quote_allocation_usdt"))
        if allocation is None:
            allocation = safe_float(row.get("quote_allocation_usdt"))
        if allocation is not None and abs(allocation) > 1e-12:
            return net_pnl / allocation * 100.0
        return net_pnl
    return None


def pnl_value(row: dict[str, Any], pct: float | None = None) -> float | None:
    for key in ("net_pnl_usdt", "pnl_usdt", "estimated_pnl_usdt"):
        number = safe_float(row.get(key))
        if number is not None:
            return number
    if pct is None:
        pct = return_pct(row)
    if pct is None:
        return None
    allocation = safe_float(row.get("effective_quote_allocation_usdt"))
    if allocation is None:
        allocation = safe_float(row.get("quote_allocation_usdt"))
    if allocation is None:
        return pct
    return allocation * pct / 100.0


def time_value(row: dict[str, Any]) -> str:
    for key in (
        "recorded_at",
        "final_exit_time",
        "signal_time",
        "simulated_entry_time",
        "logged_at",
        "strategy_generated_at",
        "updated_at",
    ):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    for key in ("signal_time_ms", "observed_at_ms", "anchor_close_time_ms"):
        number = safe_float(row.get(key))
        if number is not None:
            return f"{number:020.0f}"
    return ""


def nested_dicts(row: dict[str, Any]) -> list[dict[str, Any]]:
    dicts = [row]
    for key in ("factors", "sample", "enrichments", "trigger_features", "market_snapshot"):
        value = row.get(key)
        if isinstance(value, dict):
            dicts.append(value)
    market = row.get("market_snapshot")
    if isinstance(market, dict):
        for key in ("sample", "enrichments", "trigger_features"):
            value = market.get(key)
            if isinstance(value, dict):
                dicts.append(value)
    return dicts


def field_value(row: dict[str, Any] | None, signal: dict[str, Any] | None, *names: str) -> Any:
    for source in (row, signal):
        if not isinstance(source, dict):
            continue
        for payload in nested_dicts(source):
            for name in names:
                if name in payload and payload.get(name) not in (None, ""):
                    return payload.get(name)
    return None


def build_signal_index(signals: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in signals:
        if not event_matches(row, SIGNAL_EVENT_NAME):
            continue
        event_id = source_event_id(row) or row_event_id(row)
        if event_id:
            index[event_id] = row
    return index


def load_shadow_inputs(input_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signals = read_jsonl_records(input_dir / "signal_bridge_shadow_signals.jsonl")
    outcomes = read_jsonl_records(input_dir / "signal_bridge_shadow_outcomes.jsonl")
    return signals, annotate_research_source(outcomes, RESEARCH_SOURCE_MAINNET_SHADOW)


def annotate_research_source(records: Iterable[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    rows = []
    for record in records:
        row = dict(record)
        row.setdefault("research_source", source)
        row.setdefault("_research_source", source)
        rows.append(row)
    return rows


def _testnet_trade_identity(row: dict[str, Any], fallback_index: int) -> str:
    for key in ("source_event_id", "trade_id", "id", "client_order_id", "entry_order_id", "order_id", "job_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    parts = [
        row.get("symbol"),
        row.get("side"),
        row.get("setup") or row.get("playbook") or row.get("strategy_id"),
        row.get("entry_price"),
        row.get("close_price"),
        row.get("net_pnl_usdt"),
        row.get("closed_at") or row.get("exit_time") or row.get("hold_seconds"),
    ]
    text = "|".join(str(part or "") for part in parts).strip("|")
    return text or f"row-{fallback_index}"


def _normalize_testnet_trade(row: dict[str, Any], *, source_name: str, index: int) -> dict[str, Any]:
    source_id = str(row.get("source_event_id") or "").strip() or f"{source_name}:{_testnet_trade_identity(row, index)}"
    strategy = row.get("strategy_id") or row.get("playbook") or row.get("setup") or row.get("candidate_setup")
    normalized = dict(row)
    normalized.update(
        {
            "event": OUTCOME_EVENT_NAME,
            "research_source": RESEARCH_SOURCE_TESTNET_LIVE,
            "_research_source": RESEARCH_SOURCE_TESTNET_LIVE,
            "execution_mode": RESEARCH_SOURCE_TESTNET_LIVE,
            "event_id": str(row.get("event_id") or f"{source_id}:TESTNET_LIVE"),
            "source_event_id": source_id,
            "shadow_branch_id": RESEARCH_SOURCE_TESTNET_LIVE,
            "branch_id": RESEARCH_SOURCE_TESTNET_LIVE,
            "strategy_id": normalized_text(strategy),
            "playbook": normalized_text(row.get("playbook") or row.get("setup") or strategy),
            "effective_quote_allocation_usdt": row.get("effective_quote_allocation_usdt") or row.get("quote_allocation_usdt"),
            "final_exit_reason": row.get("final_exit_reason") or row.get("exit_reason") or row.get("close_reason"),
        }
    )
    return normalized


def _discover_testnet_trade_files(testnet_dir: Path | None) -> list[Path]:
    if testnet_dir is None:
        return []
    if not testnet_dir.exists():
        return []
    if testnet_dir.is_file():
        return [testnet_dir]
    files = sorted(testnet_dir.glob("round_*_trades.jsonl"))
    if not files:
        files = sorted(testnet_dir.glob("*trades*.jsonl"))
    return [path for path in files if path.is_file()]


def _fallback_load_testnet_research_outcomes(
    *,
    testnet_dir: Path | None = None,
    trades_file: Path | None = None,
) -> list[dict[str, Any]]:
    files = [trades_file] if trades_file is not None else _discover_testnet_trade_files(testnet_dir)
    outcomes: list[dict[str, Any]] = []
    for path in files:
        if path is None:
            continue
        source_name = path.stem
        for index, row in enumerate(read_jsonl_records(path), start=1):
            if not isinstance(row, dict):
                continue
            if str(row.get("event") or "").strip() == "trade_skipped":
                continue
            outcomes.append(_normalize_testnet_trade(row, source_name=source_name, index=index))
    return outcomes


def load_testnet_research_outcomes(
    *,
    testnet_dir: Path | None = None,
    trades_file: Path | None = None,
) -> list[dict[str, Any]]:
    try:
        from phoenix_execution_records import load_testnet_research_outcomes as external_loader
    except (ImportError, AttributeError):
        return _fallback_load_testnet_research_outcomes(testnet_dir=testnet_dir, trades_file=trades_file)
    try:
        rows = external_loader(testnet_dir=testnet_dir, trades_file=trades_file)
    except TypeError:
        rows = external_loader(trades_file or testnet_dir)
    return [_normalize_testnet_trade(row, source_name="external_testnet", index=index) for index, row in enumerate(rows, start=1)]


def load_mixed_research_outcomes(
    input_dir: Path,
    *,
    outcomes_file: Path | None = None,
    testnet_dir: Path | None = None,
    testnet_trades_file: Path | None = None,
) -> dict[str, Any]:
    shadow_file = outcomes_file or (input_dir / "signal_bridge_shadow_outcomes.jsonl")
    shadow_outcomes = annotate_research_source(read_jsonl_records(shadow_file), RESEARCH_SOURCE_MAINNET_SHADOW)
    testnet_outcomes = load_testnet_research_outcomes(testnet_dir=testnet_dir, trades_file=testnet_trades_file)
    combined = shadow_outcomes + testnet_outcomes
    return {
        "outcomes": combined,
        "shadow_outcomes_file": shadow_file,
        "testnet_dir": testnet_dir,
        "testnet_trades_file": testnet_trades_file,
        "counts": {
            "mainnet_shadow_outcomes": len(shadow_outcomes),
            "testnet_live_outcomes": len(testnet_outcomes),
            "combined_outcomes": len(combined),
        },
    }


def dedupe_independent_events(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_source: dict[str, dict[str, Any]] = {}
    for record in records:
        source_id = independent_event_key(record)
        if not source_id:
            continue
        current = by_source.get(source_id)
        if current is None:
            by_source[source_id] = record
            continue
        current_return = return_pct(current)
        candidate_return = return_pct(record)
        if candidate_return is None:
            continue
        if current_return is None or candidate_return < current_return:
            by_source[source_id] = record
    return sorted(by_source.values(), key=time_value)


def dedupe_by_group_source(
    records: Iterable[dict[str, Any]],
    key_func: Callable[[dict[str, Any]], Any],
) -> list[dict[str, Any]]:
    by_key: dict[tuple[Any, str], dict[str, Any]] = {}
    for record in records:
        source_id = independent_event_key(record)
        if not source_id:
            continue
        key = (key_func(record), source_id)
        current = by_key.get(key)
        if current is None:
            by_key[key] = record
            continue
        current_return = return_pct(current)
        candidate_return = return_pct(record)
        if candidate_return is not None and (current_return is None or candidate_return < current_return):
            by_key[key] = record
    return list(by_key.values())


def compute_profit_factor(values: Iterable[float]) -> float | None:
    rows = [float(value) for value in values if math.isfinite(float(value))]
    gross_profit = sum(value for value in rows if value > 0)
    gross_loss = abs(sum(value for value in rows if value < 0))
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return 999999.0
    return None


def max_drawdown_pct(values: Iterable[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += float(value)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return max_drawdown


def longest_losing_streak(values: Iterable[float]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def worst_window_return(values: list[float], window: int = 20) -> float:
    if not values:
        return 0.0
    actual_window = min(window, len(values))
    return min(sum(values[index : index + actual_window]) for index in range(0, len(values) - actual_window + 1))


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


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
        "avg_return_pct": round_optional(sum(rows) / sample_count if sample_count else None),
        "total_return_pct": round_optional(sum(rows)),
        "profit_factor": round_optional(compute_profit_factor(rows)),
        "max_drawdown_pct": round_optional(max_drawdown_pct(rows)),
        "longest_losing_streak": longest_losing_streak(rows),
    }


def input_count_by_research_source(records: Iterable[dict[str, Any]]) -> dict[str, int]:
    counter = Counter(research_source(record) for record in records)
    return dict(sorted(counter.items()))


def strategy_promotion_cross_validation(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in records:
        value = return_pct(row)
        if value is None:
            continue
        grouped[strategy_id(row)][research_source(row)].append(value)

    rows = []
    counts = Counter()
    for strategy, by_source in grouped.items():
        shadow = summarize_returns(by_source.get(RESEARCH_SOURCE_MAINNET_SHADOW, []))
        testnet = summarize_returns(by_source.get(RESEARCH_SOURCE_TESTNET_LIVE, []))
        shadow_good = (shadow.get("sample_count") or 0) > 0 and (shadow.get("avg_return_pct") or 0.0) > 0.0
        testnet_good = (testnet.get("sample_count") or 0) > 0 and (testnet.get("avg_return_pct") or 0.0) > 0.0
        if shadow_good and testnet_good:
            classification = "candidate"
        elif shadow_good and not testnet_good:
            classification = "execution_issue"
        elif not shadow_good and testnet_good:
            classification = "testnet_artifact"
        else:
            classification = "pause"
        counts[classification] += 1
        rows.append(
            {
                "strategy": strategy,
                "classification": classification,
                "mainnet_shadow": shadow,
                "testnet_live": testnet,
            }
        )
    rows.sort(key=lambda item: (str(item["classification"]), str(item["strategy"])))
    return {
        "rule": "mainnet shadow good + testnet good => candidate; shadow good + testnet bad => execution_issue; shadow bad + testnet good => testnet_artifact; both bad => pause",
        "classification_counts": dict(sorted(counts.items())),
        "strategies": rows,
    }


def count_by(records: Iterable[dict[str, Any]], key_func, *, top_n: int = 20) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "loss_count": 0, "return_sum": 0.0, "pnl_sum": 0.0})
    for record in records:
        key = normalized_text(key_func(record))
        pct = return_pct(record)
        pnl = pnl_value(record, pct)
        stats[key]["count"] += 1
        if pct is not None:
            stats[key]["return_sum"] += pct
            if pct < 0:
                stats[key]["loss_count"] += 1
        if pnl is not None:
            stats[key]["pnl_sum"] += pnl
    rows = [
        {
            "key": key,
            "count": value["count"],
            "loss_count": value["loss_count"],
            "return_sum_pct": round_optional(value["return_sum"]),
            "pnl_sum": round_optional(value["pnl_sum"]),
        }
        for key, value in stats.items()
    ]
    rows.sort(key=lambda item: (item["pnl_sum"] or 0.0, item["return_sum_pct"] or 0.0, -int(item["count"])))
    return rows[:top_n]


def markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    def cell(value: Any) -> str:
        if value is None:
            return ""
        text = str(value)
        return text.replace("|", "\\|").replace("\n", " ")

    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(cell(value) for value in row) + " |")
    return "\n".join(lines)


def counter_rows(counter: Counter, *, top_n: int = 20) -> list[dict[str, Any]]:
    return [{"key": key, "count": count} for key, count in counter.most_common(top_n)]
