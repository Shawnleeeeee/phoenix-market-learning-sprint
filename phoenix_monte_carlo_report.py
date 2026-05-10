from __future__ import annotations

import argparse
import random
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from phoenix_research_diagnostics import (
    OUTCOME_EVENT_NAME,
    branch_id,
    compute_profit_factor,
    dedupe_by_group_source,
    event_matches,
    input_count_by_research_source,
    load_mixed_research_outcomes,
    longest_losing_streak,
    markdown_table,
    max_drawdown_pct,
    normalized_text,
    now_iso,
    percentile,
    playbook,
    read_jsonl_records,
    return_pct,
    round_optional,
    source_event_id,
    strategy_promotion_cross_validation,
    strategy_id,
    summarize_returns,
    worst_window_return,
    write_json,
    write_text,
)


MONTE_CARLO_VERSION = "v1.0"
DEFAULT_MIN_INDEPENDENT_EVENTS = 30
DEFAULT_SIMULATIONS = 5000


def group_key(row: dict[str, Any], dimension: str) -> str:
    if dimension == "playbook":
        return playbook(row)
    if dimension == "strategy":
        return strategy_id(row)
    if dimension == "branch":
        return branch_id(row)
    if dimension == "playbook_strategy_branch":
        return "|".join([playbook(row), strategy_id(row), branch_id(row)])
    return normalized_text(row.get(dimension))


def simulate_path(
    returns: list[float],
    rng: random.Random,
    *,
    mode: str,
) -> list[float]:
    if mode == "bootstrap":
        return [rng.choice(returns) for _ in returns]
    if mode == "shuffle":
        path = list(returns)
        rng.shuffle(path)
        return path
    raise ValueError(f"unsupported mode: {mode}")


def risk_of_ruin_probability(
    paths: list[list[float]],
    *,
    risk_per_trade_pct: float,
    ruin_threshold_pct: float = 30.0,
) -> float:
    ruin_count = 0
    for path in paths:
        equity_drawdown = 0.0
        peak = 0.0
        cumulative = 0.0
        scale = risk_per_trade_pct
        for value in path:
            cumulative += value * scale
            peak = max(peak, cumulative)
            equity_drawdown = max(equity_drawdown, peak - cumulative)
            if equity_drawdown >= ruin_threshold_pct:
                ruin_count += 1
                break
    return ruin_count / len(paths) if paths else 0.0


def path_metrics(path: list[float]) -> dict[str, float | int]:
    return {
        "total_return_pct": sum(path),
        "max_drawdown_pct": max_drawdown_pct(path),
        "longest_losing_streak": longest_losing_streak(path),
        "worst_20_trade_return": worst_window_return(path, 20),
    }


def summarize_metric_distribution(values: list[float]) -> dict[str, float | None]:
    return {
        "p5": round_optional(percentile(values, 5)),
        "p50": round_optional(percentile(values, 50)),
        "p95": round_optional(percentile(values, 95)),
    }


def run_monte_carlo(
    returns: list[float],
    *,
    simulations: int = DEFAULT_SIMULATIONS,
    seed: int = 42,
    min_independent_events: int = DEFAULT_MIN_INDEPENDENT_EVENTS,
) -> dict[str, Any]:
    clean_returns = [float(value) for value in returns]
    sample_count = len(clean_returns)
    insufficient = sample_count < min_independent_events
    rng = random.Random(seed)
    modes: dict[str, dict[str, Any]] = {}
    for mode in ("shuffle", "bootstrap"):
        paths = [simulate_path(clean_returns, rng, mode=mode) for _ in range(max(0, simulations))] if clean_returns else []
        metrics = [path_metrics(path) for path in paths]
        total_returns = [float(row["total_return_pct"]) for row in metrics]
        drawdowns = [float(row["max_drawdown_pct"]) for row in metrics]
        losing_streaks = [float(row["longest_losing_streak"]) for row in metrics]
        worst_20 = [float(row["worst_20_trade_return"]) for row in metrics]
        modes[mode] = {
            "simulation_count": len(paths),
            "sample_status": "insufficient_sample" if insufficient else "ok",
            "total_return_pct": summarize_metric_distribution(total_returns),
            "max_drawdown_pct": summarize_metric_distribution(drawdowns),
            "longest_losing_streak": summarize_metric_distribution(losing_streaks),
            "worst_20_trade_return": summarize_metric_distribution(worst_20),
            "probability_total_return_negative": round_optional(
                sum(1 for value in total_returns if value < 0) / len(total_returns) if total_returns else None
            ),
            "probability_drawdown_gt_10pct": round_optional(
                sum(1 for value in drawdowns if value > 10.0) / len(drawdowns) if drawdowns else None
            ),
            "probability_drawdown_gt_20pct": round_optional(
                sum(1 for value in drawdowns if value > 20.0) / len(drawdowns) if drawdowns else None
            ),
            "probability_drawdown_gt_30pct": round_optional(
                sum(1 for value in drawdowns if value > 30.0) / len(drawdowns) if drawdowns else None
            ),
            "risk_of_ruin": {
                "risk_per_trade_0_25pct": round_optional(risk_of_ruin_probability(paths, risk_per_trade_pct=0.25)),
                "risk_per_trade_0_5pct": round_optional(risk_of_ruin_probability(paths, risk_per_trade_pct=0.5)),
                "risk_per_trade_1pct": round_optional(risk_of_ruin_probability(paths, risk_per_trade_pct=1.0)),
                "ruin_threshold_drawdown_pct": 30.0,
            },
        }
    return {
        "sample_count": sample_count,
        "min_independent_events": min_independent_events,
        "insufficient_sample": insufficient,
        "seed": seed,
        "input_summary": summarize_returns(clean_returns),
        "modes": modes,
    }


def build_group_reports(
    outcomes: Iterable[dict[str, Any]],
    *,
    simulations: int,
    seed: int,
    min_independent_events: int,
) -> dict[str, list[dict[str, Any]]]:
    outcome_rows = [row for row in outcomes if event_matches(row, OUTCOME_EVENT_NAME) and return_pct(row) is not None]
    groups: dict[str, list[dict[str, Any]]] = {}
    for dimension in ("playbook", "strategy", "branch", "playbook_strategy_branch"):
        deduped = dedupe_by_group_source(outcome_rows, lambda row, dim=dimension: group_key(row, dim))
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in deduped:
            buckets[group_key(row, dimension)].append(row)
        rows = []
        for key, records in buckets.items():
            values = [return_pct(row) for row in sorted(records, key=lambda item: source_event_id(item))]
            returns = [value for value in values if value is not None]
            mc = run_monte_carlo(
                returns,
                simulations=simulations,
                seed=seed,
                min_independent_events=min_independent_events,
            )
            rows.append(
                {
                    "key": key,
                    "dimension": dimension,
                    "independent_event_count": len({source_event_id(row) for row in records}),
                    "raw_branch_outcome_count": sum(1 for row in outcome_rows if group_key(row, dimension) == key),
                    "win_rate": mc["input_summary"]["win_rate_pct"],
                    "avg_return": mc["input_summary"]["avg_return_pct"],
                    "profit_factor": round_optional(compute_profit_factor(returns)),
                    "monte_carlo": mc,
                }
            )
        rows.sort(
            key=lambda row: (
                bool(row["monte_carlo"]["insufficient_sample"]),
                -(row["independent_event_count"]),
                str(row["key"]),
            )
        )
        groups[dimension] = rows
    return groups


def build_monte_carlo_report(
    outcomes: Iterable[dict[str, Any]],
    *,
    simulations: int = DEFAULT_SIMULATIONS,
    seed: int = 42,
    min_independent_events: int = DEFAULT_MIN_INDEPENDENT_EVENTS,
) -> dict[str, Any]:
    outcome_rows = [row for row in outcomes if event_matches(row, OUTCOME_EVENT_NAME)]
    independent = dedupe_independent_events_for_report(outcome_rows)
    groups = build_group_reports(
        outcome_rows,
        simulations=simulations,
        seed=seed,
        min_independent_events=min_independent_events,
    )
    return {
        "report_type": "monte_carlo_report",
        "algorithm_version": MONTE_CARLO_VERSION,
        "generated_at": now_iso(),
        "policy": {
            "return_field_priority": [
                "after_real_cost_return_pct",
                "after_fee_and_slippage_return_pct",
                "after_fee_return_pct",
                "final_return_pct",
                "close_return_pct",
                "net_pnl_usdt",
            ],
            "simulation_modes": ["shuffle_without_replacement", "bootstrap_with_replacement"],
            "meaning": "survival path stress test; not a next-trade forecast",
            "min_independent_events": min_independent_events,
        },
        "input_counts": {
            "raw_branch_outcomes": len(outcome_rows),
            "independent_source_events": len(independent),
            "by_research_source": input_count_by_research_source(outcome_rows),
        },
        "metadata": {
            "strategy_promotion_cross_validation": strategy_promotion_cross_validation(outcome_rows),
        },
        "overall_independent_event_summary": summarize_returns([return_pct(row) for row in independent if return_pct(row) is not None]),
        "groups": groups,
    }


def dedupe_independent_events_for_report(outcomes: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    from phoenix_research_diagnostics import dedupe_independent_events

    return dedupe_independent_events(outcomes)


def render_monte_carlo_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix Monte Carlo Survival Report",
        "",
        f"- Generated: `{report['generated_at']}`",
        f"- Raw branch outcomes: `{report['input_counts']['raw_branch_outcomes']}`",
        f"- Independent source events: `{report['input_counts']['independent_source_events']}`",
        "- Interpretation: black-path survivability stress test, not a next-trade forecast.",
        "",
    ]
    for dimension in ("playbook", "strategy", "branch"):
        rows = report["groups"].get(dimension, [])[:20]
        lines.extend(
            [
                f"## {dimension}",
                markdown_table(
                    ["key", "events", "raw", "win_rate", "avg_return", "pf", "status", "p5_total", "p95_dd", "ruin_1pct"],
                    [
                        [
                            row["key"],
                            row["independent_event_count"],
                            row["raw_branch_outcome_count"],
                            row["win_rate"],
                            row["avg_return"],
                            row["profit_factor"],
                            "insufficient" if row["monte_carlo"]["insufficient_sample"] else "ok",
                            row["monte_carlo"]["modes"]["bootstrap"]["total_return_pct"]["p5"],
                            row["monte_carlo"]["modes"]["bootstrap"]["max_drawdown_pct"]["p95"],
                            row["monte_carlo"]["modes"]["bootstrap"]["risk_of_ruin"]["risk_per_trade_1pct"],
                        ]
                        for row in rows
                    ],
                ),
                "",
            ]
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phoenix Monte Carlo survival diagnostics.")
    parser.add_argument("--input-dir", type=Path, required=True, help="mainnet_shadow directory")
    parser.add_argument("--outcomes-file", type=Path, default=None)
    parser.add_argument("--testnet-dir", type=Path, default=None, help="directory containing TESTNET_LIVE trade jsonl files")
    parser.add_argument("--testnet-trades-file", type=Path, default=None, help="single TESTNET_LIVE trade jsonl file")
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=None)
    parser.add_argument("--simulations", type=int, default=DEFAULT_SIMULATIONS)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-independent-events", type=int, default=DEFAULT_MIN_INDEPENDENT_EVENTS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_dir = args.input_dir
    mixed = load_mixed_research_outcomes(
        input_dir,
        outcomes_file=args.outcomes_file,
        testnet_dir=args.testnet_dir,
        testnet_trades_file=args.testnet_trades_file,
    )
    outcomes = mixed["outcomes"]
    report = build_monte_carlo_report(
        outcomes,
        simulations=max(1, int(args.simulations)),
        seed=int(args.seed),
        min_independent_events=max(1, int(args.min_independent_events)),
    )
    report.update(
        {
            "input_dir": str(input_dir),
            "outcomes_file": str(mixed["shadow_outcomes_file"]),
            "testnet_dir": str(args.testnet_dir) if args.testnet_dir else None,
            "testnet_trades_file": str(args.testnet_trades_file) if args.testnet_trades_file else None,
            "mixed_input_counts": mixed["counts"],
        }
    )
    output_json = args.output_json or (input_dir / "monte_carlo_report.json")
    output_md = args.output_md or (input_dir / "monte_carlo_report.md")
    write_json(output_json, report)
    write_text(output_md, render_monte_carlo_markdown(report))
    print(
        "monte_carlo_report "
        f"raw_branch_outcomes={report['input_counts']['raw_branch_outcomes']} "
        f"independent_source_events={report['input_counts']['independent_source_events']} "
        f"simulations={args.simulations} output_json={output_json}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
