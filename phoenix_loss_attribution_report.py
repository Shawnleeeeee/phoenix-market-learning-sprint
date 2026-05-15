from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Iterable

from phoenix_research_diagnostics import (
    OUTCOME_EVENT_NAME,
    SIGNAL_EVENT_NAME,
    UNKNOWN,
    branch_id,
    build_signal_index,
    count_by,
    dedupe_independent_events,
    event_matches,
    field_value,
    input_count_by_research_source,
    load_mixed_research_outcomes,
    markdown_table,
    normalized_text,
    now_iso,
    playbook,
    pnl_value,
    read_json,
    read_jsonl_records,
    return_pct,
    round_optional,
    safe_float,
    source_event_id,
    strategy_promotion_cross_validation,
    strategy_id,
    time_value,
    write_json,
    write_text,
)
from phoenix_trade_attribution import classify_execution_record


LOSS_ATTRIBUTION_VERSION = "v1.0"
DEFAULT_CLUSTER_LOOKBACK = 5
DEFAULT_CLUSTER_MIN_LOSSES = 3


def adverse_abs(row: dict[str, Any]) -> float | None:
    for key in ("max_adverse_return_pct", "max_drawdown_pct", "max_drawdown"):
        value = safe_float(row.get(key))
        if value is not None:
            return abs(value)
    return None


def runup_pct(row: dict[str, Any]) -> float | None:
    for key in ("max_favorable_return_pct", "max_runup_pct", "max_runup"):
        value = safe_float(row.get(key))
        if value is not None:
            return abs(value)
    return None


def is_timeout_exit(exit_reason: str) -> bool:
    normalized = exit_reason.lower()
    return any(part in normalized for part in ("horizon_close", "timeout", "max_hold", "momentum_decay"))


def side_against_btc_or_market(row: dict[str, Any], signal: dict[str, Any] | None) -> bool:
    side = normalized_text(field_value(row, signal, "side"), default="").upper()
    btc_5m = safe_float(field_value(row, signal, "btcusdt_ret_5m_pct", "btc_regime_ret_5m_pct"))
    market_score = safe_float(field_value(row, signal, "market_regime_score"))
    btc_bucket = normalized_text(field_value(row, signal, "btc_5m_regime", "btc_regime_bucket"), default="").lower()
    if side == "BUY":
        return (btc_5m is not None and btc_5m <= -0.1) or (market_score is not None and market_score <= -0.25) or "down" in btc_bucket
    if side == "SELL":
        return (btc_5m is not None and btc_5m >= 0.1) or (market_score is not None and market_score >= 0.25) or "up" in btc_bucket
    return False


def oi_not_confirmed(row: dict[str, Any], signal: dict[str, Any] | None) -> bool:
    side = normalized_text(field_value(row, signal, "side"), default="").upper()
    source = normalized_text(field_value(row, signal, "oi_confirmation_source"), default="").lower()
    bucket = normalized_text(field_value(row, signal, "oi5_direction_bucket"), default="").lower()
    oi5 = safe_float(field_value(row, signal, "oi_change_5m_pct", "five_min_oi_change_pct"))
    if source in {"missing", "unknown"} or "flat" in bucket or "unwind_opposite" in bucket:
        return True
    if oi5 is None:
        return True
    if side == "BUY" and oi5 <= 0.2:
        return True
    if side == "SELL" and oi5 >= -0.2:
        return True
    return False


def low_liquidity_or_spread(row: dict[str, Any], signal: dict[str, Any] | None) -> bool:
    spread = safe_float(field_value(row, signal, "spread_bps_at_entry", "spread_bps"))
    slippage = safe_float(field_value(row, signal, "estimated_slippage_bps"))
    liquidity_bucket = normalized_text(field_value(row, signal, "liquidity_bucket"), default="").lower()
    bid_depth = safe_float(field_value(row, signal, "bid_depth_notional_5"))
    ask_depth = safe_float(field_value(row, signal, "ask_depth_notional_5"))
    if spread is not None and spread >= 8.0:
        return True
    if slippage is not None and slippage >= 12.0:
        return True
    if "low" in liquidity_bucket or "thin" in liquidity_bucket:
        return True
    if bid_depth is not None and ask_depth is not None and min(bid_depth, ask_depth) < 1000.0:
        return True
    return False


def cost_destroyed_edge(row: dict[str, Any]) -> bool:
    net = return_pct(row)
    if net is None or net >= 0:
        return False
    close_return = safe_float(row.get("close_return_pct"))
    after_fee = safe_float(row.get("after_fee_return_pct"))
    final_return = safe_float(row.get("final_return_pct"))
    gross = close_return if close_return is not None else final_return
    if gross is not None and gross >= -0.03:
        return True
    if after_fee is not None and after_fee >= -0.03 and net < 0:
        return True
    return False


def classify_loss_reason(
    row: dict[str, Any],
    signal: dict[str, Any] | None = None,
    *,
    symbol_recent_losses: int = 0,
) -> tuple[str, list[str]]:
    net = return_pct(row)
    if net is None or net >= 0:
        return "not_a_loss", []
    exit_reason = normalized_text(row.get("exit_reason") or row.get("final_exit_reason"), default="").lower()
    runup = runup_pct(row)
    adverse = adverse_abs(row)

    reasons: list[str] = []
    shared = classify_execution_record(row)
    for reason in shared.get("all_loss_reasons", []):
        if reason not in {"not_a_loss", "unknown"}:
            reasons.append(reason)
    if "stop" in exit_reason:
        reasons.append("stop_loss_hit")
    if is_timeout_exit(exit_reason) and (runup is None or runup < 0.25):
        reasons.append("no_follow_through")
    if runup is not None and runup >= 0.5:
        reasons.append("gave_back_profit")
    if adverse is not None and (runup is None or adverse >= max(0.5, runup * 2.0)):
        reasons.append("immediate_adverse_move")
    if cost_destroyed_edge(row):
        reasons.append("cost_slippage_edge_destroyed")
    if side_against_btc_or_market(row, signal):
        reasons.append("btc_regime_against_side")
    if oi_not_confirmed(row, signal):
        reasons.append("oi_not_confirmed")
    if low_liquidity_or_spread(row, signal):
        reasons.append("low_liquidity_or_spread")
    if symbol_recent_losses >= DEFAULT_CLUSTER_MIN_LOSSES:
        reasons.append("repeated_symbol_cluster")

    priority = [
        "stop_loss_hit",
        "order_reject_microstructure",
        "tp_net_loss",
        "fee_drag_exit",
        "high_slippage",
        "cost_slippage_edge_destroyed",
        "bad_symbol_setup_combo",
        "gave_back_profit",
        "immediate_adverse_move",
        "no_follow_through",
        "btc_regime_against_side",
        "oi_not_confirmed",
        "low_liquidity_or_spread",
        "repeated_symbol_cluster",
    ]
    primary = next((reason for reason in priority if reason in reasons), "unknown")
    secondary = [reason for reason in reasons if reason != primary]
    return primary, secondary


def symbol_cluster_counts(losses: list[dict[str, Any]]) -> dict[str, int]:
    ordered = sorted(losses, key=time_value)
    recent: dict[str, deque[str]] = defaultdict(lambda: deque(maxlen=DEFAULT_CLUSTER_LOOKBACK))
    counts: dict[str, int] = {}
    for row in ordered:
        symbol = normalized_text(row.get("symbol")).upper()
        counts[row.get("_diagnostic_id", source_event_id(row) or str(id(row)))] = len(recent[symbol])
        recent[symbol].append(source_event_id(row) or row.get("event_id") or "")
    return counts


def scope_metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
    values = [return_pct(row) for row in records]
    values = [value for value in values if value is not None]
    pnls = [pnl_value(row, return_pct(row)) for row in records]
    pnls = [value for value in pnls if value is not None]
    losses = [value for value in values if value < 0]
    wins = [value for value in values if value > 0]
    return {
        "outcome_count": len(records),
        "loss_count": len(losses),
        "win_count": len(wins),
        "avg_return_pct": round_optional(sum(values) / len(values) if values else None),
        "total_return_pct": round_optional(sum(values) if values else None),
        "total_pnl": round_optional(sum(pnls) if pnls else None),
    }


def build_group_rows(records: list[dict[str, Any]], key_func, *, top_n: int = 30) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        grouped[normalized_text(key_func(row))].append(row)
    rows = []
    for key, values in grouped.items():
        metrics = scope_metrics(values)
        reason_counts = Counter(row.get("loss_reason", UNKNOWN) for row in values if row.get("loss_reason"))
        metrics.update(
            {
                "key": key,
                "top_loss_reasons": [{"reason": reason, "count": count} for reason, count in reason_counts.most_common(5)],
            }
        )
        rows.append(metrics)
    rows.sort(key=lambda item: (item.get("total_pnl") or 0.0, -int(item.get("loss_count") or 0)))
    return rows[:top_n]


def build_strategy_priorities(losses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in losses:
        grouped[strategy_id(row)].append(row)
    priorities = []
    for strategy, rows in grouped.items():
        reasons = Counter(row.get("loss_reason", UNKNOWN) for row in rows)
        symbols = Counter(normalized_text(row.get("symbol")).upper() for row in rows)
        primary, count = reasons.most_common(1)[0]
        action = {
            "stop_loss_hit": "review stop width and entry quality before widening stops",
            "no_follow_through": "tighten entry filters or shorten timeout exits",
            "gave_back_profit": "test faster partial take-profit or trailing stop",
            "immediate_adverse_move": "add adverse move / regime prefilter before entry",
            "cost_slippage_edge_destroyed": "raise minimum edge or cap slippage/spread harder",
            "btc_regime_against_side": "gate entries against BTC 5m/market regime",
            "oi_not_confirmed": "separate OI-missing/fallback cohorts and require alignment for live candidates",
            "low_liquidity_or_spread": "exclude thin books or raise liquidity bucket threshold",
            "repeated_symbol_cluster": "add symbol-level cooldown after clustered losses",
            "unknown": "add missing feature capture before changing strategy",
        }.get(primary, "review samples")
        priorities.append(
            {
                "strategy": strategy,
                "loss_count": len(rows),
                "primary_issue": primary,
                "primary_issue_count": count,
                "top_symbol": symbols.most_common(1)[0][0] if symbols else UNKNOWN,
                "recommended_next_step": action,
            }
        )
    priorities.sort(key=lambda item: (-int(item["loss_count"]), str(item["strategy"])))
    return priorities


def annotate_losses(outcomes: Iterable[dict[str, Any]], signals: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signal_index = build_signal_index(signals)
    losses: list[dict[str, Any]] = []
    for index, row in enumerate(outcomes):
        if not event_matches(row, OUTCOME_EVENT_NAME):
            continue
        pct = return_pct(row)
        if pct is None or pct >= 0:
            continue
        enriched = dict(row)
        enriched["_diagnostic_id"] = f"loss-{index}"
        losses.append(enriched)

    cluster_counts = symbol_cluster_counts(losses)
    annotated: list[dict[str, Any]] = []
    for row in losses:
        signal = signal_index.get(source_event_id(row))
        primary, secondary = classify_loss_reason(
            row,
            signal,
            symbol_recent_losses=cluster_counts.get(row["_diagnostic_id"], 0),
        )
        annotated_row = dict(row)
        annotated_row["loss_reason"] = primary
        annotated_row["secondary_loss_reasons"] = secondary
        annotated_row["source_event_id"] = source_event_id(row)
        annotated_row["branch_id"] = branch_id(row)
        annotated_row["strategy_id"] = strategy_id(row)
        annotated_row["return_pct_used"] = return_pct(row)
        annotated_row["pnl_value"] = pnl_value(row, return_pct(row))
        annotated.append(annotated_row)
    independent = dedupe_independent_events(annotated)
    return annotated, independent


def build_loss_attribution_report(
    outcomes: Iterable[dict[str, Any]],
    signals: Iterable[dict[str, Any]] = (),
    *,
    supplemental_reports: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcome_rows = [row for row in outcomes if event_matches(row, OUTCOME_EVENT_NAME)]
    signal_rows = [row for row in signals if event_matches(row, SIGNAL_EVENT_NAME)]
    raw_losses, independent_losses = annotate_losses(outcome_rows, signal_rows)
    reason_combo_counter = Counter(
        "+".join([row.get("loss_reason", UNKNOWN), *row.get("secondary_loss_reasons", [])]) for row in raw_losses
    )
    return {
        "report_type": "loss_attribution_report",
        "algorithm_version": LOSS_ATTRIBUTION_VERSION,
        "generated_at": now_iso(),
        "deduplication": {
            "raw_branch_outcomes": len(outcome_rows),
            "raw_branch_losses": len(raw_losses),
            "independent_source_event_losses": len(independent_losses),
            "dedupe_rule": "source_event_id; worst return kept per source event",
        },
        "input_counts": {
            "signals": len(signal_rows),
            "outcomes": len(outcome_rows),
            "losses": len(raw_losses),
            "by_research_source": input_count_by_research_source(outcome_rows),
        },
        "metadata": {
            "strategy_promotion_cross_validation": strategy_promotion_cross_validation(outcome_rows),
        },
        "raw_branch_scope": {
            "summary": scope_metrics(raw_losses),
            "by_playbook": build_group_rows(raw_losses, playbook),
            "by_strategy": build_group_rows(raw_losses, strategy_id),
            "by_branch": build_group_rows(raw_losses, branch_id),
            "by_symbol": build_group_rows(raw_losses, lambda row: normalized_text(row.get("symbol")).upper()),
            "by_exit_reason": build_group_rows(raw_losses, lambda row: row.get("exit_reason") or row.get("final_exit_reason")),
            "by_loss_reason": build_group_rows(raw_losses, lambda row: row.get("loss_reason")),
            "by_side": build_group_rows(raw_losses, lambda row: row.get("side")),
            "top_loss_reason_combinations": [
                {"reasons": reason, "count": count} for reason, count in reason_combo_counter.most_common(20)
            ],
        },
        "independent_event_scope": {
            "summary": scope_metrics(independent_losses),
            "by_source_event_id": build_group_rows(independent_losses, source_event_id, top_n=50),
            "by_symbol": build_group_rows(independent_losses, lambda row: normalized_text(row.get("symbol")).upper()),
            "by_loss_reason": build_group_rows(independent_losses, lambda row: row.get("loss_reason")),
        },
        "top_losing_symbols": count_by(raw_losses, lambda row: normalized_text(row.get("symbol")).upper(), top_n=20),
        "strategy_priority_fix_list": build_strategy_priorities(raw_losses),
        "supplemental_inputs": supplemental_reports or {},
    }


def render_loss_attribution_markdown(report: dict[str, Any]) -> str:
    dedupe = report["deduplication"]
    raw_summary = report["raw_branch_scope"]["summary"]
    independent_summary = report["independent_event_scope"]["summary"]
    priorities = report["strategy_priority_fix_list"][:15]
    lines = [
        "# Phoenix Loss Attribution Report",
        "",
        f"- Generated: `{report['generated_at']}`",
        f"- Raw branch losses: `{dedupe['raw_branch_losses']}` / outcomes `{dedupe['raw_branch_outcomes']}`",
        f"- Independent source-event losses: `{dedupe['independent_source_event_losses']}`",
        f"- Raw loss PnL: `{raw_summary.get('total_pnl')}`; independent loss PnL: `{independent_summary.get('total_pnl')}`",
        "",
        "## Strategy Priority Fix List",
        markdown_table(
            ["strategy", "losses", "primary_issue", "count", "top_symbol", "next_step"],
            [
                [
                    row["strategy"],
                    row["loss_count"],
                    row["primary_issue"],
                    row["primary_issue_count"],
                    row["top_symbol"],
                    row["recommended_next_step"],
                ]
                for row in priorities
            ],
        ),
        "",
        "## Loss Reasons",
        markdown_table(
            ["reason", "losses", "total_pnl", "top_secondary"],
            [
                [
                    row["key"],
                    row["loss_count"],
                    row["total_pnl"],
                    ", ".join(f"{item['reason']}={item['count']}" for item in row["top_loss_reasons"][:3]),
                ]
                for row in report["raw_branch_scope"]["by_loss_reason"][:20]
            ],
        ),
        "",
        "## Top Losing Symbols",
        markdown_table(
            ["symbol", "count", "loss_count", "pnl_sum", "return_sum_pct"],
            [
                [row["key"], row["count"], row["loss_count"], row["pnl_sum"], row["return_sum_pct"]]
                for row in report["top_losing_symbols"][:20]
            ],
        ),
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phoenix shadow loss attribution diagnostics.")
    parser.add_argument("--input-dir", type=Path, required=True, help="mainnet_shadow directory")
    parser.add_argument("--signals-file", type=Path, default=None)
    parser.add_argument("--outcomes-file", type=Path, default=None)
    parser.add_argument("--testnet-dir", type=Path, default=None, help="directory containing TESTNET_LIVE trade jsonl files")
    parser.add_argument("--testnet-trades-file", type=Path, default=None, help="single TESTNET_LIVE trade jsonl file")
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=None)
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
    supplemental = {}
    for name in ("candidate_strategy_report.json", "execution_realism_report.json"):
        payload = read_json(input_dir / name)
        if isinstance(payload, dict):
            supplemental[name] = payload
    report = build_loss_attribution_report(outcomes, signals, supplemental_reports=supplemental)
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
    output_json = args.output_json or (input_dir / "loss_attribution_report.json")
    output_md = args.output_md or (input_dir / "loss_attribution_report.md")
    write_json(output_json, report)
    write_text(output_md, render_loss_attribution_markdown(report))
    print(
        "loss_attribution_report "
        f"raw_branch_losses={report['deduplication']['raw_branch_losses']} "
        f"independent_losses={report['deduplication']['independent_source_event_losses']} "
        f"output_json={output_json}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
