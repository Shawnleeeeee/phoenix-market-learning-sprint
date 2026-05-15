from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from phoenix_learning_analyzer import infer_return, read_json, read_jsonl, read_round_trade_rows, safe_float
from phoenix_strategy_registry import register_experiment_candidate, shadow_safe_defaults, write_strategy_registry


EXPERIMENT_VERSION = "v1.0"
ACTIVE_EXPERIMENT_STATUSES = {
    "running_experiment",
    "promoted_shadow_candidate",
    "promoted_testnet_candidate",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part) for part in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def clamp_pct(value: Any, default: float = 0.10) -> float:
    parsed = safe_float(value, default)
    return max(0.0, min(1.0, parsed))


def force_experiment_safety(payload: dict[str, Any]) -> dict[str, Any]:
    safe = dict(payload)
    safe["live_trading_enabled"] = False
    safe["promotion_allowed"] = False
    safe["direct_live_change_allowed"] = False
    safe["mainnet_live_promotion_allowed"] = False
    safe["agent_enabled"] = False
    return safe


def generate_experiment_from_proposal(
    proposal: dict[str, Any],
    *,
    default_allocation_pct: float = 0.10,
) -> dict[str, Any]:
    proposal_id = str(proposal.get("proposal_id") or "").strip()
    if not proposal_id:
        raise ValueError("proposal_id is required")
    target_strategy = str(proposal.get("target_strategy") or proposal.get("setup") or "mechanical_candidate")
    if target_strategy == "global":
        target_strategy = str(proposal.get("setup") or "global_mechanical")
    problem_type = str(proposal.get("problem_type") or "unknown")
    version = str(proposal.get("candidate_version") or f"{target_strategy}_v2_{problem_type}")
    experiment_id = stable_id("experiment", proposal_id, target_strategy, version)
    auto_allowed = proposal.get("status") == "auto_experiment_allowed"
    validation = proposal.get("validation_plan") if isinstance(proposal.get("validation_plan"), dict) else {}
    experiment = {
        "experiment_id": experiment_id,
        "experiment_version": EXPERIMENT_VERSION,
        "generated_at": utc_now_iso(),
        "strategy_id": target_strategy,
        "version": version,
        "parent_version": proposal.get("parent_version") or f"{target_strategy}_v1",
        "status": "running_experiment" if auto_allowed else "pending_review",
        "mode": "testnet_candidate" if auto_allowed else "research_only",
        "experiment_type": proposal.get("experiment_type") or _infer_experiment_type(problem_type),
        "created_from_proposal": proposal_id,
        "problem_type": problem_type,
        "setup": proposal.get("setup"),
        "symbol": proposal.get("symbol"),
        "branch": proposal.get("branch"),
        "proposed_change": proposal.get("proposed_change") or {},
        "expected_effect": proposal.get("expected_effect"),
        "risk": proposal.get("risk"),
        "validation_requirements": {
            "minimum_sample_count": validation.get("minimum_sample_count", 50),
            "minimum_independent_event_count": validation.get("minimum_independent_event_count", 30),
            "layers": validation.get("layers", ["mainnet_shadow", "futures_testnet"]),
            "compare_against": validation.get("compare_against", "parent_version"),
        },
        "rollback_target": proposal.get("rollback_target") or f"{target_strategy}_v1",
        "candidate_allocation_pct": clamp_pct(proposal.get("candidate_allocation_pct"), default_allocation_pct),
        "hard_safety_fail_count": int(proposal.get("hard_safety_fail_count") or 0),
        "hard_safety_fail_limit": int(proposal.get("hard_safety_fail_limit") or 3),
    }
    return force_experiment_safety(experiment)


def _infer_experiment_type(problem_type: str) -> str:
    if problem_type in {"take_profit_net_loss", "tp_net_loss", "gave_back_profit", "stale_or_time_decay", "no_follow_through"}:
        return "dynamic_position_manager_experiment"
    if problem_type in {"high_slippage_failure", "high_slippage", "cost_slippage_edge_destroyed", "fee_drag_exit"}:
        return "slippage_filter_experiment"
    if problem_type in {"repeated_symbol_setup_loss", "bad_symbol_setup_combo"}:
        return "symbol_setup_filter_experiment"
    return "entry_filter_experiment"


def read_proposals(proposal_dir: Path) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    if not proposal_dir.exists():
        return proposals
    for path in sorted(proposal_dir.glob("*.json")):
        payload = read_json(path)
        if isinstance(payload, dict):
            proposals.append(payload)
    return proposals


def write_strategy_experiments(
    *,
    proposals: Iterable[dict[str, Any]],
    output_dir: Path,
    registry_path: Path | None = None,
    default_allocation_pct: float = 0.10,
) -> list[dict[str, Any]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    experiments: list[dict[str, Any]] = []
    registry = shadow_safe_defaults() if registry_path is not None else None
    valid_names: set[str] = set()
    for proposal in proposals:
        experiment = generate_experiment_from_proposal(proposal, default_allocation_pct=default_allocation_pct)
        experiments.append(experiment)
        path = output_dir / f"{experiment['experiment_id']}.json"
        valid_names.add(path.name)
        path.write_text(json.dumps(experiment, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if registry_path is not None:
            registry = register_experiment_candidate(registry, experiment)
    archive_dir = output_dir / "_stale"
    for old_path in sorted(output_dir.glob("*.json")):
        if old_path.name in valid_names:
            continue
        archive_dir.mkdir(parents=True, exist_ok=True)
        old_path.replace(archive_dir / old_path.name)
    if registry_path is not None and registry is not None:
        write_strategy_registry(registry_path, registry)
    return experiments


def load_experiment_records(input_dir: Path, *, learning_store_file: Path | None = None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    learning_path = learning_store_file or input_dir / "learning_store.jsonl"
    for row in read_jsonl(learning_path):
        row = dict(row)
        row.setdefault("analysis_source", "learning_store")
        records.append(row)
    for row in read_round_trade_rows(input_dir):
        row = dict(row)
        row.setdefault("analysis_source", "testnet_trades")
        records.append(row)
    return records


def event_key(row: dict[str, Any], index: int) -> str:
    return str(
        row.get("source_event_id")
        or row.get("entry_order_id")
        or row.get("signal_id")
        or row.get("trade_id")
        or f"row:{index}"
    )


def dedupe_independent_events(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for index, row in enumerate(records):
        key = event_key(row, index)
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _row_strategy_id(row: dict[str, Any]) -> str:
    return str(row.get("strategy_id") or row.get("setup") or "unknown")


def _row_version(row: dict[str, Any]) -> str:
    return str(row.get("strategy_version") or row.get("version") or row.get("config_version") or "")


def split_baseline_candidate_records(
    experiment: dict[str, Any],
    records: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    strategy_id = str(experiment.get("strategy_id") or "")
    parent_version = str(experiment.get("parent_version") or "")
    version = str(experiment.get("version") or "")
    experiment_id = str(experiment.get("experiment_id") or "")
    baseline: list[dict[str, Any]] = []
    candidate: list[dict[str, Any]] = []
    for row in records:
        row_strategy = _row_strategy_id(row)
        row_version = _row_version(row)
        if str(row.get("experiment_id") or "") == experiment_id or (version and row_version == version):
            candidate.append(dict(row))
            continue
        if row_strategy == strategy_id and (
            not row.get("experiment_id")
            and (not parent_version or row_version in {"", parent_version, "testnet_runner_v1"})
        ):
            baseline.append(dict(row))
    return baseline, candidate


def _max_drawdown(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return round(worst, 6)


def _longest_losing_streak(values: list[float]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value <= 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _take_profit_net_loss_count(records: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in records
        if "take_profit" in str(row.get("exit_reason") or "").lower() and infer_return(row) <= 0
    )


def _problem_count(records: list[dict[str, Any]], problem: str) -> int:
    count = 0
    for row in records:
        problems = row.get("problem_types") or row.get("strategy_proposals") or []
        if isinstance(problems, str):
            problems = [problems]
        if problem in problems:
            count += 1
    return count


def _problem_list(row: dict[str, Any]) -> list[str]:
    problems = row.get("problem_types") or []
    if isinstance(problems, str):
        return [problems]
    if isinstance(problems, list):
        return [str(problem) for problem in problems]
    return []


def _state_performance(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in records:
        grouped[str(row.get("market_state") or row.get("hidden_state") or "unknown")].append(infer_return(row))
    out = []
    for state, values in grouped.items():
        wins = [value for value in values if value > 0]
        losses = [value for value in values if value < 0]
        out.append(
            {
                "state": state,
                "sample_count": len(values),
                "avg_net_pnl": round(sum(values) / len(values), 6) if values else 0.0,
                "win_rate": round(len(wins) / len(values) * 100.0, 6) if values else 0.0,
                "profit_factor": round(sum(wins) / abs(sum(losses)), 6) if losses else (999.0 if wins else 0.0),
            }
        )
    return sorted(out, key=lambda item: item["sample_count"], reverse=True)


def summarize_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [infer_return(row) for row in records]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    gross = sum(abs(safe_float(row.get("realized_pnl_usdt"), 0.0)) for row in records)
    commission = sum(abs(safe_float(row.get("commission_usdt"), 0.0)) for row in records)
    independent = dedupe_independent_events(records)
    return {
        "sample_count": len(records),
        "independent_event_count": len(independent),
        "win_rate": round(len(wins) / len(records) * 100.0, 6) if records else 0.0,
        "avg_net_pnl": round(sum(returns) / len(returns), 6) if returns else 0.0,
        "avg_return_pct": round(
            sum(safe_float(row.get("return_pct"), infer_return(row)) for row in records) / len(records),
            6,
        )
        if records
        else 0.0,
        "profit_factor": round(sum(wins) / abs(sum(losses)), 6) if losses else (999.0 if wins else 0.0),
        "max_drawdown": _max_drawdown(returns),
        "longest_losing_streak": _longest_losing_streak(returns),
        "fee_drag_ratio": round(commission / gross, 6) if gross > 0 else (999.0 if commission > 0 else 0.0),
        "take_profit_net_loss_count": _take_profit_net_loss_count(records),
        "no_follow_through_count": _problem_count(records, "no_follow_through"),
        "gave_back_profit_count": _problem_count(records, "gave_back_profit"),
        "problem_counts": dict(Counter(problem for row in records for problem in _problem_list(row))),
        "state_performance": _state_performance(records),
        "monte_carlo_probability_total_return_negative": None,
    }


def evaluate_experiment_records(
    experiment: dict[str, Any],
    records: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    baseline_rows, candidate_rows = split_baseline_candidate_records(experiment, records)
    result = {
        "report_type": "experiment_result",
        "version": EXPERIMENT_VERSION,
        "generated_at": utc_now_iso(),
        "experiment_id": experiment.get("experiment_id"),
        "strategy_id": experiment.get("strategy_id"),
        "candidate_version": experiment.get("version"),
        "parent_version": experiment.get("parent_version"),
        "mode": experiment.get("mode"),
        "status": experiment.get("status"),
        "baseline": summarize_records(baseline_rows),
        "candidate": summarize_records(candidate_rows),
    }
    return force_experiment_safety(result)


def format_experiment_result_markdown(result: dict[str, Any]) -> str:
    baseline = result.get("baseline") or {}
    candidate = result.get("candidate") or {}
    lines = [
        "# Phoenix Experiment Result",
        "",
        f"- experiment_id: {result.get('experiment_id')}",
        f"- strategy_id: {result.get('strategy_id')}",
        f"- candidate_version: {result.get('candidate_version')}",
        f"- parent_version: {result.get('parent_version')}",
        f"- live_trading_enabled: {str(result.get('live_trading_enabled')).lower()}",
        "",
        "## Baseline",
        f"- sample_count: {baseline.get('sample_count', 0)}",
        f"- independent_event_count: {baseline.get('independent_event_count', 0)}",
        f"- avg_net_pnl: {baseline.get('avg_net_pnl', 0)}",
        f"- profit_factor: {baseline.get('profit_factor', 0)}",
        "",
        "## Candidate",
        f"- sample_count: {candidate.get('sample_count', 0)}",
        f"- independent_event_count: {candidate.get('independent_event_count', 0)}",
        f"- avg_net_pnl: {candidate.get('avg_net_pnl', 0)}",
        f"- profit_factor: {candidate.get('profit_factor', 0)}",
        "",
    ]
    return "\n".join(lines)


def write_experiment_results(
    *,
    experiments: Iterable[dict[str, Any]],
    records: Iterable[dict[str, Any]],
    output_dir: Path,
) -> list[dict[str, Any]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    record_list = list(records)
    valid_names: set[str] = set()
    for experiment in experiments:
        result = evaluate_experiment_records(experiment, record_list)
        results.append(result)
        stem = str(result.get("experiment_id") or stable_id("experiment_result", len(results)))
        valid_names.add(f"{stem}.json")
        valid_names.add(f"{stem}.md")
        (output_dir / f"{stem}.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (output_dir / f"{stem}.md").write_text(format_experiment_result_markdown(result), encoding="utf-8")
    archive_dir = output_dir / "_stale"
    for old_path in sorted(output_dir.glob("*.json")) + sorted(output_dir.glob("*.md")):
        if old_path.name in valid_names:
            continue
        archive_dir.mkdir(parents=True, exist_ok=True)
        old_path.replace(archive_dir / old_path.name)
    return results


def load_active_experiments(experiments_dir: Path) -> list[dict[str, Any]]:
    experiments: list[dict[str, Any]] = []
    if not experiments_dir.exists():
        return experiments
    for path in sorted(experiments_dir.glob("*.json")):
        payload = read_json(path)
        if not isinstance(payload, dict):
            continue
        if payload.get("status") not in ACTIVE_EXPERIMENT_STATUSES:
            continue
        if payload.get("mode") not in {"testnet_candidate", "shadow_candidate"}:
            continue
        if payload.get("live_trading_enabled") or payload.get("promotion_allowed") or payload.get("direct_live_change_allowed"):
            continue
        fail_count = int(payload.get("hard_safety_fail_count") or 0)
        fail_limit = int(payload.get("hard_safety_fail_limit") or 3)
        if fail_count >= fail_limit:
            continue
        experiments.append(force_experiment_safety(payload))
    return experiments


def record_experiment_hard_safety_fail(
    experiments_dir: Path,
    experiment_id: str,
    *,
    reason: str = "",
) -> dict[str, Any] | None:
    if not experiment_id:
        return None
    for path in sorted(experiments_dir.glob("*.json")):
        payload = read_json(path)
        if not isinstance(payload, dict) or str(payload.get("experiment_id") or "") != str(experiment_id):
            continue
        fail_count = int(payload.get("hard_safety_fail_count") or 0) + 1
        fail_limit = int(payload.get("hard_safety_fail_limit") or 3)
        payload["hard_safety_fail_count"] = fail_count
        payload["last_hard_safety_fail_at"] = utc_now_iso()
        payload["last_hard_safety_fail_reason"] = reason
        if fail_count >= fail_limit:
            payload["status"] = "paused"
        payload = force_experiment_safety(payload)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return payload
    return None


def _candidate_value(candidate: Any, key: str) -> Any:
    if isinstance(candidate, dict):
        return candidate.get(key)
    return getattr(candidate, key, None)


def experiment_matches_candidate(experiment: dict[str, Any], candidate: Any) -> bool:
    if experiment.get("live_trading_enabled") or experiment.get("promotion_allowed"):
        return False
    symbol = experiment.get("symbol")
    setup = experiment.get("setup")
    if symbol and str(symbol) != str(_candidate_value(candidate, "symbol")):
        return False
    if setup and str(setup) != str(_candidate_value(candidate, "setup")):
        return False
    return True


def deterministic_fraction(*parts: Any) -> float:
    digest = hashlib.sha1("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()[:12]
    return int(digest, 16) / float(0xFFFFFFFFFFFF)


def choose_experiment_for_candidate(
    candidate: Any,
    experiments: Iterable[dict[str, Any]],
    *,
    allocation_pct: float = 0.10,
    seed: str = "",
) -> dict[str, Any] | None:
    bounded_allocation = clamp_pct(allocation_pct, 0.10)
    for experiment in experiments:
        if not experiment_matches_candidate(experiment, candidate):
            continue
        experiment_allocation = min(bounded_allocation, clamp_pct(experiment.get("candidate_allocation_pct"), 0.10))
        if experiment_allocation <= 0:
            continue
        draw = deterministic_fraction(seed, experiment.get("experiment_id"), _candidate_value(candidate, "symbol"), _candidate_value(candidate, "setup"))
        if draw <= experiment_allocation:
            return force_experiment_safety(experiment)
    return None


def experiment_trade_metadata(experiment: dict[str, Any] | None) -> dict[str, Any]:
    if not experiment:
        return {
            "experiment_id": None,
            "baseline_id": "testnet_runner_v1",
            "strategy_version": "testnet_runner_v1",
            "parent_version": None,
            "candidate_allocation_pct": 0.0,
            "candidate_experiment_active": False,
        }
    return {
        "experiment_id": experiment.get("experiment_id"),
        "baseline_id": None,
        "strategy_id": experiment.get("strategy_id"),
        "strategy_version": experiment.get("version"),
        "parent_version": experiment.get("parent_version"),
        "candidate_allocation_pct": clamp_pct(experiment.get("candidate_allocation_pct"), 0.0),
        "candidate_experiment_active": True,
        "experiment_type": experiment.get("experiment_type"),
        "created_from_proposal": experiment.get("created_from_proposal"),
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "direct_live_change_allowed": False,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and evaluate Phoenix strategy experiments.")
    parser.add_argument("--proposal-dir", type=Path, required=True)
    parser.add_argument("--input-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--registry-path", type=Path, default=None)
    parser.add_argument("--learning-store-file", type=Path, default=None)
    parser.add_argument("--default-allocation-pct", type=float, default=0.10)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    proposals = read_proposals(args.proposal_dir)
    experiments = write_strategy_experiments(
        proposals=proposals,
        output_dir=args.output_dir,
        registry_path=args.registry_path,
        default_allocation_pct=args.default_allocation_pct,
    )
    if args.input_dir is not None:
        records = load_experiment_records(args.input_dir, learning_store_file=args.learning_store_file)
        write_experiment_results(experiments=experiments, records=records, output_dir=args.output_dir.parent / "experiment_results")
    print(f"strategy_experiments count={len(experiments)} output_dir={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
