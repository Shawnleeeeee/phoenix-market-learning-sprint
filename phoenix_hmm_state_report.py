from __future__ import annotations

import argparse
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from phoenix_research_diagnostics import (
    OUTCOME_EVENT_NAME,
    SIGNAL_EVENT_NAME,
    dedupe_independent_events,
    event_matches,
    field_value,
    markdown_table,
    normalized_text,
    now_iso,
    read_jsonl_records,
    return_pct,
    round_optional,
    safe_float,
    source_event_id,
    strategy_id,
    summarize_returns,
    time_value,
    write_json,
    write_text,
)


HMM_STATE_VERSION = "v1.0"
HIDDEN_STATES = (
    "quiet_range",
    "trend_expansion",
    "oi_unwind",
    "liquidity_stress",
    "volatile_chop",
)
DEFAULT_MIN_STATE_EVENTS = 10


def dependency_status() -> dict[str, Any]:
    try:
        __import__("hmmlearn")
    except Exception as exc:  # pragma: no cover - environment dependent
        return {
            "hmm_available": False,
            "lightweight_model": True,
            "dependency": "hmmlearn",
            "reason": exc.__class__.__name__,
        }
    return {
        "hmm_available": True,
        "lightweight_model": True,
        "dependency": "hmmlearn",
        "reason": "available_but_not_used_for_offline_lightweight_report",
    }


def num(row: dict[str, Any], *names: str) -> float | None:
    return safe_float(field_value(row, None, *names))


def state_sample_status(count: int, min_ok: int = DEFAULT_MIN_STATE_EVENTS) -> str:
    if count < min(5, min_ok):
        return "insufficient_sample"
    if count < min_ok:
        return "low_confidence"
    return "ok"


def softmax(scores: dict[str, float]) -> dict[str, float]:
    if not scores:
        return {}
    peak = max(scores.values())
    exp_values = {key: math.exp(value - peak) for key, value in scores.items()}
    denom = sum(exp_values.values())
    if denom <= 0:
        return {key: round_optional(1.0 / len(exp_values)) for key in exp_values}
    return {key: round_optional(value / denom) for key, value in exp_values.items()}


def emission_scores(row: dict[str, Any]) -> dict[str, float]:
    playbook_value = normalized_text(field_value(row, None, "playbook"), default="").lower()
    liquidity_bucket = normalized_text(field_value(row, None, "liquidity_bucket"), default="").lower()
    oi_bucket = normalized_text(field_value(row, None, "oi5_direction_bucket", "oi_5m_regime"), default="").lower()
    ret_1 = num(row, "ret_1bar_pct", "one_min_return_pct")
    ret_5 = num(row, "ret_5bar_pct", "five_min_return_pct")
    volume = num(row, "volume_burst_ratio", "one_min_volume_burst_ratio")
    range_atr = num(row, "range_to_atr", "one_min_range_to_atr")
    oi5 = num(row, "oi_change_5m_pct", "five_min_oi_change_pct")
    spread = num(row, "spread_bps_at_entry", "spread_bps")
    slippage = num(row, "estimated_slippage_bps")
    vol_score = num(row, "volatility_regime_score")
    trend_score = num(row, "trend_strength_score", "trend_score", "market_regime_score")

    move = ret_1 if ret_1 is not None else ret_5
    abs_move = abs(move or 0.0)
    abs_oi = abs(oi5 or 0.0)
    cost = max(spread or 0.0, slippage or 0.0)

    scores = {state: 0.0 for state in HIDDEN_STATES}
    scores["quiet_range"] += 0.8
    if volume is not None and volume < 1.0:
        scores["quiet_range"] += 0.5
    if range_atr is not None and range_atr < 0.9:
        scores["quiet_range"] += 0.5

    if "breakout" in playbook_value or abs_move >= 0.4:
        scores["trend_expansion"] += 1.2
    if volume is not None and volume >= 1.4:
        scores["trend_expansion"] += 0.7
    if range_atr is not None and range_atr >= 1.0:
        scores["trend_expansion"] += 0.5
    if trend_score is not None and abs(trend_score) >= 0.35:
        scores["trend_expansion"] += 0.5

    if (oi5 is not None and oi5 <= -0.35) or "unwind" in oi_bucket:
        scores["oi_unwind"] += 1.6
    if "unwind" in playbook_value:
        scores["oi_unwind"] += 0.8

    if cost >= 8.0 or "low" in liquidity_bucket or "thin" in liquidity_bucket:
        scores["liquidity_stress"] += 1.6
    if range_atr is not None and range_atr >= 1.5 and (volume is None or volume < 1.2):
        scores["liquidity_stress"] += 0.6

    if vol_score is not None and vol_score >= 0.6:
        scores["volatile_chop"] += 1.0
    if range_atr is not None and range_atr >= 1.2 and volume is not None and volume < 1.2:
        scores["volatile_chop"] += 0.8
    if abs_move >= 0.25 and abs_oi < 0.2:
        scores["volatile_chop"] += 0.4
    return scores


def infer_hidden_states(signals: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    previous_by_symbol: dict[str, str] = {}
    for signal in sorted(signals, key=lambda row: (normalized_text(row.get("symbol")).upper(), time_value(row), source_event_id(row) or "")):
        if not event_matches(signal, SIGNAL_EVENT_NAME):
            continue
        event_id = source_event_id(signal)
        if not event_id:
            continue
        symbol = normalized_text(signal.get("symbol")).upper()
        scores = emission_scores(signal)
        previous = previous_by_symbol.get(symbol)
        if previous in scores:
            scores[previous] += 0.25
        probabilities = softmax(scores)
        state = max(probabilities, key=lambda key: probabilities[key]) if probabilities else "unknown"
        previous_by_symbol[symbol] = state
        rows.append(
            {
                "source_event_id": event_id,
                "symbol": symbol,
                "time": time_value(signal),
                "most_likely_hidden_state": state,
                "probabilities": probabilities,
                "lightweight_model": True,
            }
        )
    return rows


def performance_rows(records: Iterable[dict[str, Any]], key_func, *, key_name: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in records:
        value = return_pct(row)
        if value is None:
            continue
        grouped[normalized_text(key_func(row))].append(value)
    rows = []
    for key, values in grouped.items():
        summary = summarize_returns(values)
        summary.update({key_name: key, "sample_status": state_sample_status(summary["sample_count"])})
        rows.append(summary)
    rows.sort(key=lambda row: (row.get("avg_return_pct") or 0.0), reverse=True)
    return rows


def strategy_rows(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        grouped[(normalized_text(row.get("hidden_state")), strategy_id(row))].append(row)
    rows = []
    for (state, strategy), values in grouped.items():
        returns = [return_pct(row) for row in values]
        clean_returns = [value for value in returns if value is not None]
        summary = summarize_returns(clean_returns)
        summary.update(
            {
                "hidden_state": state,
                "strategy": strategy,
                "sample_status": state_sample_status(summary["sample_count"]),
            }
        )
        rows.append(summary)
    rows.sort(key=lambda row: (row["hidden_state"], -(row.get("sample_count") or 0), row["strategy"]))
    return rows


def exit_risk_rows(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        grouped[normalized_text(row.get("hidden_state"))].append(row)
    rows = []
    for state, values in grouped.items():
        returns = [return_pct(row) for row in values]
        clean_returns = [value for value in returns if value is not None]
        losses = [value for value in clean_returns if value < 0]
        exit_counts = Counter(normalized_text(row.get("exit_reason") or row.get("final_exit_reason"), default="unknown") for row in values)
        adverse_values = [safe_float(row.get("max_adverse_return_pct") or row.get("max_drawdown_pct")) for row in values]
        adverse_values = [abs(value) for value in adverse_values if value is not None]
        rows.append(
            {
                "hidden_state": state,
                "sample_count": len(values),
                "loss_count": len(losses),
                "loss_rate_pct": round_optional((len(losses) / len(clean_returns)) * 100.0 if clean_returns else None),
                "avg_loss_return_pct": round_optional(sum(losses) / len(losses) if losses else None),
                "avg_adverse_excursion_pct": round_optional(sum(adverse_values) / len(adverse_values) if adverse_values else None),
                "top_exit_reasons": [{"exit_reason": reason, "count": count} for reason, count in exit_counts.most_common(5)],
                "sample_status": state_sample_status(len(values)),
            }
        )
    rows.sort(key=lambda row: (row.get("loss_rate_pct") or 0.0, row.get("avg_adverse_excursion_pct") or 0.0), reverse=True)
    return rows


def enrich_outcomes_with_hidden_state(
    outcomes: Iterable[dict[str, Any]],
    hidden_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    state_by_source = {row["source_event_id"]: row["most_likely_hidden_state"] for row in hidden_rows}
    enriched = []
    for row in dedupe_independent_events([item for item in outcomes if event_matches(item, OUTCOME_EVENT_NAME)]):
        copy = dict(row)
        copy["hidden_state"] = state_by_source.get(source_event_id(row), "unknown")
        enriched.append(copy)
    return enriched


def build_hmm_state_report(
    signals: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    *,
    testnet_signals: Iterable[dict[str, Any]] | None = None,
    testnet_outcomes: Iterable[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    mainnet_signals = [row for row in signals if event_matches(row, SIGNAL_EVENT_NAME)]
    mainnet_outcomes = [row for row in outcomes if event_matches(row, OUTCOME_EVENT_NAME)]
    test_signals = [row for row in (testnet_signals or []) if event_matches(row, SIGNAL_EVENT_NAME)]
    test_outcomes = [row for row in (testnet_outcomes or []) if event_matches(row, OUTCOME_EVENT_NAME)]
    all_signals = mainnet_signals + test_signals
    all_outcomes = mainnet_outcomes + test_outcomes
    hidden_rows = infer_hidden_states(all_signals)
    enriched_outcomes = enrich_outcomes_with_hidden_state(all_outcomes, hidden_rows)
    state_counts = Counter(row["most_likely_hidden_state"] for row in hidden_rows)
    dependency = dependency_status()
    return {
        "report_type": "hmm_state_report",
        "algorithm_version": HMM_STATE_VERSION,
        "generated_at": now_iso(),
        "hmm_available": dependency["hmm_available"],
        "lightweight_model": dependency["lightweight_model"],
        "trading_gate_enabled": False,
        "position_manager_enabled": False,
        "policy": {
            "model_type": "offline_lightweight_hmm_style_regime_inference",
            "dependency_status": dependency,
            "hidden_states": list(HIDDEN_STATES),
            "sequence_scope": "per_symbol",
            "trading_gate_enabled": False,
            "position_manager_enabled": False,
            "decision_use": "research_only_never_trade_execution",
        },
        "input_counts": {
            "mainnet_signals": len(mainnet_signals),
            "mainnet_outcomes": len(mainnet_outcomes),
            "testnet_signals": len(test_signals),
            "testnet_outcomes": len(test_outcomes),
            "hidden_state_events": len(hidden_rows),
            "independent_outcomes": len(enriched_outcomes),
        },
        "hidden_state_distribution": [
            {
                "hidden_state": state,
                "count": count,
                "probability": round_optional(count / len(hidden_rows) if hidden_rows else None),
                "sample_status": state_sample_status(count),
            }
            for state, count in state_counts.most_common()
        ],
        "hidden_regime_probabilities": hidden_rows,
        "state_performance": performance_rows(enriched_outcomes, lambda row: row.get("hidden_state"), key_name="hidden_state"),
        "strategy_by_hidden_state": strategy_rows(enriched_outcomes),
        "exit_risk_by_hidden_state": exit_risk_rows(enriched_outcomes),
    }


def render_hmm_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix HMM-Style Hidden Regime Report",
        "",
        f"- Generated: `{report['generated_at']}`",
        f"- HMM dependency available: `{report['hmm_available']}`",
        f"- Lightweight model: `{report['lightweight_model']}`",
        f"- Trading gate enabled: `{report['trading_gate_enabled']}`",
        f"- Position manager enabled: `{report['position_manager_enabled']}`",
        f"- Hidden state events: `{report['input_counts']['hidden_state_events']}`",
        f"- Independent outcomes: `{report['input_counts']['independent_outcomes']}`",
        "",
        "## Hidden State Distribution",
        markdown_table(
            ["state", "count", "probability", "status"],
            [[row["hidden_state"], row["count"], row["probability"], row["sample_status"]] for row in report["hidden_state_distribution"]],
        ),
        "",
        "## State Performance",
        markdown_table(
            ["state", "samples", "win_rate", "avg_return", "pf", "mdd", "status"],
            [
                [
                    row["hidden_state"],
                    row["sample_count"],
                    row["win_rate_pct"],
                    row["avg_return_pct"],
                    row["profit_factor"],
                    row["max_drawdown_pct"],
                    row["sample_status"],
                ]
                for row in report["state_performance"][:20]
            ],
        ),
        "",
        "## Strategy By Hidden State",
        markdown_table(
            ["state", "strategy", "samples", "avg_return", "pf", "status"],
            [
                [row["hidden_state"], row["strategy"], row["sample_count"], row["avg_return_pct"], row["profit_factor"], row["sample_status"]]
                for row in report["strategy_by_hidden_state"][:30]
            ],
        ),
        "",
        "## Exit Risk By Hidden State",
        markdown_table(
            ["state", "samples", "loss_rate", "avg_loss", "avg_adverse", "status"],
            [
                [
                    row["hidden_state"],
                    row["sample_count"],
                    row["loss_rate_pct"],
                    row["avg_loss_return_pct"],
                    row["avg_adverse_excursion_pct"],
                    row["sample_status"],
                ]
                for row in report["exit_risk_by_hidden_state"][:20]
            ],
        ),
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phoenix offline HMM-style hidden regime diagnostics.")
    parser.add_argument("--input-dir", type=Path, required=True, help="mainnet_shadow directory")
    parser.add_argument("--testnet-dir", type=Path, default=None, help="optional testnet shadow directory")
    parser.add_argument("--signals-file", type=Path, default=None)
    parser.add_argument("--outcomes-file", type=Path, default=None)
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_dir = args.input_dir
    signals_file = args.signals_file or (input_dir / "signal_bridge_shadow_signals.jsonl")
    outcomes_file = args.outcomes_file or (input_dir / "signal_bridge_shadow_outcomes.jsonl")
    signals = read_jsonl_records(signals_file)
    outcomes = read_jsonl_records(outcomes_file)
    testnet_signals: list[dict[str, Any]] = []
    testnet_outcomes: list[dict[str, Any]] = []
    if args.testnet_dir is not None:
        testnet_signals = read_jsonl_records(args.testnet_dir / "signal_bridge_shadow_signals.jsonl")
        testnet_outcomes = read_jsonl_records(args.testnet_dir / "signal_bridge_shadow_outcomes.jsonl")
    report = build_hmm_state_report(
        signals,
        outcomes,
        testnet_signals=testnet_signals,
        testnet_outcomes=testnet_outcomes,
    )
    report.update(
        {
            "input_dir": str(input_dir),
            "testnet_dir": str(args.testnet_dir) if args.testnet_dir is not None else None,
            "signals_file": str(signals_file),
            "outcomes_file": str(outcomes_file),
        }
    )
    output_json = args.output_json or (input_dir / "hmm_state_report.json")
    output_md = args.output_md or (input_dir / "hmm_state_report.md")
    write_json(output_json, report)
    write_text(output_md, render_hmm_markdown(report))
    print(
        "hmm_state_report "
        f"hidden_state_events={report['input_counts']['hidden_state_events']} "
        f"independent_outcomes={report['input_counts']['independent_outcomes']} "
        f"lightweight_model={report['lightweight_model']} output_json={output_json}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
