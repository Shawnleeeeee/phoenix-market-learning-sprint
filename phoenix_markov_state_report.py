from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from phoenix_research_diagnostics import (
    OUTCOME_EVENT_NAME,
    SIGNAL_EVENT_NAME,
    build_signal_index,
    dedupe_independent_events,
    event_matches,
    field_value,
    input_count_by_research_source,
    load_mixed_research_outcomes,
    markdown_table,
    normalized_text,
    now_iso,
    playbook,
    read_jsonl_records,
    return_pct,
    round_optional,
    safe_float,
    source_event_id,
    strategy_promotion_cross_validation,
    strategy_id,
    summarize_returns,
    time_value,
    write_json,
    write_text,
)


MARKOV_VERSION = "v1.0"
DEFAULT_MIN_STATE_COUNT = 10
DEFAULT_MIN_TRANSITION_DENOMINATOR = 30


def num(row: dict[str, Any], *names: str) -> float | None:
    value = field_value(row, None, *names)
    return safe_float(value)


def build_market_state(row: dict[str, Any]) -> str:
    playbook_value = normalized_text(field_value(row, None, "playbook"), default="").lower()
    liquidity_bucket = normalized_text(field_value(row, None, "liquidity_bucket"), default="").lower()
    oi_bucket = normalized_text(field_value(row, None, "oi5_direction_bucket", "oi_5m_regime"), default="").lower()
    volume = num(row, "volume_burst_ratio", "one_min_volume_burst_ratio")
    range_atr = num(row, "range_to_atr", "one_min_range_to_atr")
    ret_1 = num(row, "ret_1bar_pct", "one_min_return_pct")
    ret_5 = num(row, "ret_5bar_pct")
    oi5 = num(row, "oi_change_5m_pct", "five_min_oi_change_pct")
    oi15 = num(row, "oi_change_15m_pct", "fifteen_min_oi_change_pct")
    spread = num(row, "spread_bps_at_entry", "spread_bps")
    slippage = num(row, "estimated_slippage_bps")
    depth_imbalance = num(row, "depth_imbalance")
    vol_score = num(row, "volatility_regime_score")
    trend_score = num(row, "trend_strength_score", "trend_score", "market_regime_score")
    long_liq = num(row, "liquidation_long_usd_15m", "liquidation_long_usd")
    short_liq = num(row, "liquidation_short_usd_15m", "liquidation_short_usd")
    liq_events = num(row, "liquidation_event_count_15m", "liquidation_event_count")

    if "liquidation" in playbook_value or max(long_liq or 0.0, short_liq or 0.0) >= 100_000.0 or (liq_events or 0.0) >= 10.0:
        return "liquidation_flush"
    if (spread is not None and spread >= 8.0) or (slippage is not None and slippage >= 12.0) or "low" in liquidity_bucket or "thin" in liquidity_bucket:
        return "liquidity_thin"
    if (oi5 is not None and oi5 <= -0.5) or (oi15 is not None and oi15 <= -1.0) or "unwind" in oi_bucket:
        return "oi_unwind"
    if (
        "oi_build" in playbook_value
        or ((oi5 is not None and oi5 >= 0.5) or (oi15 is not None and oi15 >= 1.0))
        and (volume is None or volume >= 1.5)
        and (range_atr is None or range_atr >= 1.0)
    ):
        return "oi_build_breakout"
    if range_atr is not None and range_atr >= 1.5 and (volume is None or volume < 1.2):
        return "false_breakout_risk"
    if vol_score is not None and vol_score >= 0.6 and (trend_score is None or abs(trend_score) < 0.25):
        return "high_vol_chop"
    if depth_imbalance is not None and abs(depth_imbalance) >= 0.6 and (range_atr is not None and range_atr >= 1.0):
        return "liquidity_thin"
    move = ret_1 if ret_1 is not None else ret_5
    if move is not None and move >= 0.5 and (range_atr is None or range_atr >= 1.0) and (volume is None or volume >= 1.2):
        return "trend_expansion_up"
    if move is not None and move <= -0.5 and (range_atr is None or range_atr >= 1.0) and (volume is None or volume >= 1.2):
        return "trend_expansion_down"
    if (range_atr is not None and range_atr < 0.8) and (volume is not None and volume < 1.0):
        return "compression"
    return "quiet_range"


def state_sample_status(count: int, min_ok: int = DEFAULT_MIN_TRANSITION_DENOMINATOR) -> str:
    if count < min(10, min_ok):
        return "insufficient_sample"
    if count < min_ok:
        return "low_confidence"
    return "ok"


def build_signal_states(signals: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in signals:
        if not event_matches(row, SIGNAL_EVENT_NAME):
            continue
        source_id = source_event_id(row) or str(row.get("event_id") or "")
        if not source_id:
            continue
        rows.append(
            {
                "source_event_id": source_id,
                "symbol": normalized_text(row.get("symbol")).upper(),
                "time": time_value(row),
                "state": build_market_state(row),
                "raw": row,
            }
        )
    rows.sort(key=lambda item: (item["symbol"], item["time"], item["source_event_id"]))
    return rows


def transition_key(prev: dict[str, Any], current: dict[str, Any]) -> str:
    return f"{prev['state']}->{current['state']}"


def build_transitions(states: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in states:
        by_symbol[item["symbol"]].append(item)
    transitions: list[dict[str, Any]] = []
    event_transition: dict[str, str] = {}
    for symbol, rows in by_symbol.items():
        previous: dict[str, Any] | None = None
        for row in sorted(rows, key=lambda item: (item["time"], item["source_event_id"])):
            if previous is not None and previous["state"] != row["state"]:
                key = transition_key(previous, row)
                transitions.append(
                    {
                        "symbol": symbol,
                        "from_state": previous["state"],
                        "to_state": row["state"],
                        "transition": key,
                        "from_source_event_id": previous["source_event_id"],
                        "to_source_event_id": row["source_event_id"],
                        "time": row["time"],
                    }
                )
                event_transition[row["source_event_id"]] = key
            previous = row
    return transitions, event_transition


def transition_matrix(transitions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str]] = Counter((row["from_state"], row["to_state"]) for row in transitions)
    denominators: Counter[str] = Counter(row["from_state"] for row in transitions)
    matrix = []
    for (from_state, to_state), count in sorted(counts.items()):
        denominator = denominators[from_state]
        matrix.append(
            {
                "from_state": from_state,
                "to_state": to_state,
                "count": count,
                "denominator": denominator,
                "probability": round_optional(count / denominator if denominator else None),
                "sample_status": state_sample_status(denominator),
            }
        )
    return matrix


def performance_by_key(records: Iterable[dict[str, Any]], key_func) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    strategy_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in records:
        value = return_pct(row)
        if value is None:
            continue
        key = normalized_text(key_func(row))
        grouped[key].append(value)
        strategy_counts[key][strategy_id(row)] += 1
    rows = []
    for key, values in grouped.items():
        summary = summarize_returns(values)
        summary.update(
            {
                "key": key,
                "top_strategies": [
                    {"strategy": strategy, "count": count}
                    for strategy, count in strategy_counts[key].most_common(5)
                ],
                "sample_status": state_sample_status(summary["sample_count"]),
            }
        )
        rows.append(summary)
    rows.sort(key=lambda row: (row.get("avg_return_pct") or 0.0), reverse=True)
    return rows


def recommendation_rows(per_state: list[dict[str, Any]], per_transition: list[dict[str, Any]]) -> dict[str, Any]:
    good_states = [row for row in per_state if (row.get("sample_count") or 0) >= 10 and (row.get("avg_return_pct") or 0.0) > 0]
    bad_states = [row for row in per_state if (row.get("sample_count") or 0) >= 10 and (row.get("avg_return_pct") or 0.0) < 0]
    good_transitions = [row for row in per_transition if (row.get("sample_count") or 0) >= 5 and (row.get("avg_return_pct") or 0.0) > 0]
    bad_transitions = [row for row in per_transition if (row.get("sample_count") or 0) >= 5 and (row.get("avg_return_pct") or 0.0) < 0]
    state_names = {row["key"] for row in good_states}
    return {
        "trend_strategy_enable_states": [state for state in ("trend_expansion_up", "trend_expansion_down", "oi_build_breakout") if state in state_names],
        "reversal_disable_states": [row["key"] for row in bad_states if row["key"] in {"trend_expansion_up", "trend_expansion_down", "liquidity_thin", "high_vol_chop"}],
        "one_min_momentum_scalp_states": [state for state in ("trend_expansion_up", "trend_expansion_down", "oi_build_breakout", "liquidation_flush") if state in state_names],
        "dynamic_take_profit_hold_transitions": [row["key"] for row in good_transitions[:10]],
        "early_exit_transitions": [row["key"] for row in sorted(bad_transitions, key=lambda item: item.get("avg_return_pct") or 0.0)[:10]],
    }


def build_markov_state_report(
    signals: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    *,
    min_transition_denominator: int = DEFAULT_MIN_TRANSITION_DENOMINATOR,
) -> dict[str, Any]:
    signal_rows = [row for row in signals if event_matches(row, SIGNAL_EVENT_NAME)]
    outcome_rows = [row for row in outcomes if event_matches(row, OUTCOME_EVENT_NAME)]
    states = build_signal_states(signal_rows)
    transitions, event_transition = build_transitions(states)
    source_state = {row["source_event_id"]: row["state"] for row in states}
    independent_outcomes = dedupe_independent_events(outcome_rows)
    enriched_outcomes = []
    for row in independent_outcomes:
        source_id = source_event_id(row)
        enriched = dict(row)
        enriched["market_state"] = source_state.get(source_id, "unknown")
        enriched["market_transition"] = event_transition.get(source_id, "unknown")
        enriched_outcomes.append(enriched)
    state_counts = Counter(row["state"] for row in states)
    matrix = transition_matrix(transitions)
    for row in matrix:
        row["sample_status"] = state_sample_status(int(row["denominator"]), min_transition_denominator)
    per_state = performance_by_key(enriched_outcomes, lambda row: row.get("market_state"))
    per_transition = performance_by_key(enriched_outcomes, lambda row: row.get("market_transition"))
    return {
        "report_type": "markov_state_report",
        "algorithm_version": MARKOV_VERSION,
        "generated_at": now_iso(),
        "policy": {
            "model_type": "simple_first_order_markov_chain",
            "sequence_scope": "per_symbol",
            "same_state_repeats": "collapsed",
            "min_transition_denominator": min_transition_denominator,
        },
        "input_counts": {
            "signals": len(signal_rows),
            "outcomes": len(outcome_rows),
            "state_events": len(states),
            "transitions": len(transitions),
            "independent_outcomes": len(independent_outcomes),
            "by_research_source": input_count_by_research_source(outcome_rows),
        },
        "metadata": {
            "strategy_promotion_cross_validation": strategy_promotion_cross_validation(outcome_rows),
        },
        "state_counts": [{"state": state, "count": count, "sample_status": state_sample_status(count, DEFAULT_MIN_STATE_COUNT)} for state, count in state_counts.most_common()],
        "transition_matrix": matrix,
        "per_state_strategy_performance": per_state,
        "per_transition_strategy_performance": per_transition,
        "recommendations": recommendation_rows(per_state, per_transition),
    }


def render_markov_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix Markov State Report",
        "",
        f"- Generated: `{report['generated_at']}`",
        f"- State events: `{report['input_counts']['state_events']}`",
        f"- Transitions: `{report['input_counts']['transitions']}`",
        f"- Independent outcomes: `{report['input_counts']['independent_outcomes']}`",
        "",
        "## State Counts",
        markdown_table(
            ["state", "count", "status"],
            [[row["state"], row["count"], row["sample_status"]] for row in report["state_counts"][:20]],
        ),
        "",
        "## Transition Matrix",
        markdown_table(
            ["from", "to", "count", "denom", "prob", "status"],
            [
                [row["from_state"], row["to_state"], row["count"], row["denominator"], row["probability"], row["sample_status"]]
                for row in report["transition_matrix"][:40]
            ],
        ),
        "",
        "## Per State Performance",
        markdown_table(
            ["state", "samples", "avg_return", "pf", "mdd", "status"],
            [
                [row["key"], row["sample_count"], row["avg_return_pct"], row["profit_factor"], row["max_drawdown_pct"], row["sample_status"]]
                for row in report["per_state_strategy_performance"][:20]
            ],
        ),
        "",
        "## Recommendations",
        "```json",
        __import__("json").dumps(report["recommendations"], ensure_ascii=False, indent=2),
        "```",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phoenix simple Markov market-state diagnostics.")
    parser.add_argument("--input-dir", type=Path, required=True, help="mainnet_shadow directory")
    parser.add_argument("--signals-file", type=Path, default=None)
    parser.add_argument("--outcomes-file", type=Path, default=None)
    parser.add_argument("--testnet-dir", type=Path, default=None, help="directory containing TESTNET_LIVE trade jsonl files")
    parser.add_argument("--testnet-trades-file", type=Path, default=None, help="single TESTNET_LIVE trade jsonl file")
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=None)
    parser.add_argument("--min-transition-denominator", type=int, default=DEFAULT_MIN_TRANSITION_DENOMINATOR)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_dir = args.input_dir
    signals_file = args.signals_file or (input_dir / "signal_bridge_shadow_signals.jsonl")
    signals = read_jsonl_records(signals_file)
    mixed = load_mixed_research_outcomes(
        input_dir,
        outcomes_file=args.outcomes_file,
        testnet_dir=args.testnet_dir,
        testnet_trades_file=args.testnet_trades_file,
    )
    outcomes = mixed["outcomes"]
    report = build_markov_state_report(
        signals,
        outcomes,
        min_transition_denominator=max(1, int(args.min_transition_denominator)),
    )
    report.update(
        {
            "input_dir": str(input_dir),
            "signals_file": str(signals_file),
            "outcomes_file": str(mixed["shadow_outcomes_file"]),
            "testnet_dir": str(args.testnet_dir) if args.testnet_dir else None,
            "testnet_trades_file": str(args.testnet_trades_file) if args.testnet_trades_file else None,
            "mixed_input_counts": mixed["counts"],
        }
    )
    output_json = args.output_json or (input_dir / "markov_state_report.json")
    output_md = args.output_md or (input_dir / "markov_state_report.md")
    write_json(output_json, report)
    write_text(output_md, render_markov_markdown(report))
    print(
        "markov_state_report "
        f"state_events={report['input_counts']['state_events']} "
        f"transitions={report['input_counts']['transitions']} output_json={output_json}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
