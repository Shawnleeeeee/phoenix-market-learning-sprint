from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix_learning_analyzer import read_json


PROPOSAL_VERSION = "v1.0"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part) for part in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _evidence(problem_type: str, report: dict[str, Any], target: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "problem_count": (report.get("problem_counts") or {}).get(problem_type, 0),
        "examples": (report.get("problem_examples") or {}).get(problem_type, [])[:5],
        "target": target or {},
    }


def valid_target(symbol: str | None, setup: str | None) -> bool:
    bad = {"", "unknown", "UNKNOWN", "global", "global_mechanical", "None"}
    return str(symbol or "") not in bad and str(setup or "") not in bad


def proposal_for_problem(
    *,
    report: dict[str, Any],
    problem_type: str,
    target_strategy: str = "global",
    setup: str | None = None,
    symbol: str | None = None,
    branch: str | None = None,
    evidence_target: dict[str, Any] | None = None,
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    target_label = "|".join(item for item in [target_strategy, setup or "", symbol or "", branch or "", problem_type] if item)
    proposal_id = stable_id("proposal", target_label, generated_at[:10])
    candidate_version = f"{target_strategy or setup or 'strategy'}_v2_{problem_type}"
    change: dict[str, Any]
    experiment_type: str
    expected_effect: str
    risk: str

    if problem_type in {"take_profit_net_loss", "tp_net_loss"}:
        experiment_type = "exit_profile_experiment"
        change = {
            "min_take_profit_pct_rule": "round_trip_fee_bps + estimated_slippage_bps + safety_buffer_bps",
            "safety_buffer_bps": 5,
            "fee_aware_min_profit_enabled": True,
            "alternative_exit_profile": "partial_take_profit_plus_breakeven",
        }
        expected_effect = "avoid nominal take-profit exits that are net losers after fees and slippage"
        risk = "higher target can reduce fill rate"
    elif problem_type == "repeated_symbol_setup_loss":
        experiment_type = "symbol_setup_filter_experiment"
        change = {
            "symbol_setup_cooldown_after_losses": 3,
            "score_threshold_increase": 6,
            "candidate_filter": "setup_v2_confirmed_entry",
            "no_follow_through_filter_enabled": True,
        }
        expected_effect = "reduce repeated losses from weak symbol/setup combinations"
        risk = "can skip recovery trades after a bad streak"
    elif problem_type == "gave_back_profit":
        experiment_type = "dynamic_position_manager_experiment"
        change = {
            "breakeven_guard": True,
            "partial_take_profit": True,
            "trailing_stop": True,
            "candidate_exit_profile": "setup_v2_dynamic_exit",
        }
        expected_effect = "reduce profitable trades turning into losers"
        risk = "early exits can cap large winners"
    elif problem_type in {"cost_slippage_edge_destroyed", "fee_drag_exit"}:
        experiment_type = "slippage_filter_experiment"
        change = {
            "min_expected_edge_bps": 12,
            "max_estimated_slippage_bps": 5,
            "skip_low_margin_trades": True,
        }
        expected_effect = "avoid trades where fees/slippage consume the edge"
        risk = "lower sample rate"
    elif problem_type == "hot_chase_failure":
        experiment_type = "entry_filter_experiment"
        change = {
            "avoid_long_when_price_change_24h_pct_gte": 10,
            "and_ret_1m_pct_lte": 0,
            "secondary_confirmation_required": True,
        }
        expected_effect = "reduce chasing overheated symbols after short-term momentum rolls over"
        risk = "can miss continuation breakouts"
    elif problem_type == "stale_or_time_decay":
        experiment_type = "dynamic_position_manager_experiment"
        change = {
            "no_follow_through_exit_after_sec": 30,
            "min_mfe_pct_before_hold": 0.10,
            "raise_entry_quality_threshold": True,
        }
        expected_effect = "cut dead trades earlier and reduce time decay"
        risk = "may exit just before delayed follow-through"
    elif problem_type in {"high_slippage_failure", "high_slippage"}:
        experiment_type = "slippage_filter_experiment"
        change = {
            "max_estimated_slippage_bps": 5,
            "liquidity_bucket_required": "medium_or_better",
            "reduce_quote_allocation_multiplier": 0.5,
        }
        expected_effect = "reduce losses from thin or expensive execution"
        risk = "filters out fast-moving low-liquidity opportunities"
    elif problem_type == "bad_symbol_setup_combo":
        experiment_type = "symbol_setup_filter_experiment"
        change = {
            "pause_symbol_setup": True,
            "quarantine_minutes": 240,
            "score_multiplier": 0.0,
            "resume_requires_positive_shadow_or_testnet": True,
        }
        expected_effect = "stop repeating symbol/setup combinations with strongly negative recent expectancy"
        risk = "can temporarily block a combo before it mean-reverts"
    elif problem_type == "poor_state_fit":
        experiment_type = "entry_filter_experiment"
        change = {
            "disable_in_negative_market_state": True,
            "state_gate_source": "markov_or_hmm_offline_report",
        }
        expected_effect = "avoid strategies in regimes with negative expectancy"
        risk = "state labels can be stale or noisy"
    else:
        experiment_type = "entry_filter_experiment"
        change = {
            "increase_confirmation": True,
            "raise_score_threshold": 3,
        }
        expected_effect = "reduce weak entries"
        risk = "lower sample rate"

    status = "auto_experiment_allowed" if problem_type in {
        "take_profit_net_loss",
        "tp_net_loss",
        "repeated_symbol_setup_loss",
        "bad_symbol_setup_combo",
        "gave_back_profit",
        "cost_slippage_edge_destroyed",
        "fee_drag_exit",
        "high_slippage_failure",
        "high_slippage",
        "stale_or_time_decay",
    } else "pending_review"
    return {
        "proposal_id": proposal_id,
        "proposal_version": PROPOSAL_VERSION,
        "generated_at": generated_at,
        "source_data": {
            "report_type": report.get("report_type"),
            "report_generated_at": report.get("generated_at"),
            "input_counts": report.get("input_counts"),
        },
        "target_strategy": target_strategy,
        "setup": setup,
        "symbol": symbol,
        "branch": branch,
        "problem_type": problem_type,
        "evidence": _evidence(problem_type, report, evidence_target),
        "proposed_change": change,
        "candidate_version": candidate_version,
        "expected_effect": expected_effect,
        "risk": risk,
        "validation_plan": {
            "layers": ["mainnet_shadow", "futures_testnet"],
            "minimum_sample_count": 50,
            "minimum_independent_event_count": 30,
            "compare_against": "parent_version",
            "required_reports": ["experiment_result.json", "promotion_gate_learning_report.json"],
        },
        "status": status,
        "direct_live_change_allowed": False,
        "agent_generated": False,
    }


def generate_strategy_proposals(report: dict[str, Any]) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    counts = report.get("problem_counts") if isinstance(report.get("problem_counts"), dict) else {}
    symbol_setup_rows = [item for item in (report.get("by_symbol_setup") or []) if isinstance(item, dict)]
    global_problem_order = [
        "tp_net_loss",
        "take_profit_net_loss",
        "fee_drag_exit",
        "gave_back_profit",
        "cost_slippage_edge_destroyed",
        "high_slippage",
        "high_slippage_failure",
        "hot_chase_failure",
        "stale_or_time_decay",
        "poor_state_fit",
    ]
    for problem_type in global_problem_order:
        has_targeted_rows = any(
            valid_target(str(item.get("symbol") or ""), str(item.get("setup") or ""))
            and int((item.get("problem_counts") if isinstance(item.get("problem_counts"), dict) else {}).get(problem_type) or 0) > 0
            for item in symbol_setup_rows
        )
        if int(counts.get(problem_type) or 0) > 0 and not has_targeted_rows:
            proposals.append(proposal_for_problem(report=report, problem_type=problem_type))

    targeted_problem_types = [
        "tp_net_loss",
        "fee_drag_exit",
        "high_slippage",
        "high_slippage_failure",
        "cost_slippage_edge_destroyed",
        "bad_symbol_setup_combo",
    ]
    for item in symbol_setup_rows:
        if not isinstance(item, dict):
            continue
        problem_counts = item.get("problem_counts") if isinstance(item.get("problem_counts"), dict) else {}
        symbol = str(item.get("symbol") or "")
        setup = str(item.get("setup") or "")
        if not valid_target(symbol, setup):
            continue
        for targeted_problem in targeted_problem_types:
            if int(problem_counts.get(targeted_problem) or 0) > 0:
                proposals.append(
                    proposal_for_problem(
                        report=report,
                        problem_type=targeted_problem,
                        target_strategy=setup,
                        setup=setup,
                        symbol=symbol,
                        evidence_target=item,
                    )
                )
        if int(problem_counts.get("bad_symbol_setup_combo") or 0) > 0:
            proposals.append(
                proposal_for_problem(
                    report=report,
                    problem_type="bad_symbol_setup_combo",
                    target_strategy=setup,
                    setup=setup,
                    symbol=symbol,
                    evidence_target=item,
                )
            )
        if int(problem_counts.get("repeated_symbol_setup_loss") or 0) > 0:
            proposals.append(
                proposal_for_problem(
                    report=report,
                    problem_type="repeated_symbol_setup_loss",
                    target_strategy=setup,
                    setup=setup,
                    symbol=symbol,
                    evidence_target=item,
                )
            )
        if int(problem_counts.get("no_follow_through") or 0) >= 2 and float(item.get("avg_mfe_pct") or 0.0) < 0.10:
            proposals.append(
                proposal_for_problem(
                    report=report,
                    problem_type="no_follow_through",
                    target_strategy=setup,
                    setup=setup,
                    symbol=symbol,
                    evidence_target=item,
                )
            )
        if int(problem_counts.get("hot_chase_failure") or 0) > 0:
            proposals.append(
                proposal_for_problem(
                    report=report,
                    problem_type="hot_chase_failure",
                    target_strategy=setup,
                    setup=setup,
                    symbol=symbol,
                    evidence_target=item,
                )
            )

    deduped: dict[str, dict[str, Any]] = {}
    for proposal in proposals:
        key = "|".join(
            str(proposal.get(part) or "")
            for part in ("target_strategy", "setup", "symbol", "branch", "problem_type")
        )
        deduped.setdefault(key, proposal)
    return list(deduped.values())


def write_strategy_proposals(
    *,
    report: dict[str, Any],
    output_dir: Path,
) -> list[dict[str, Any]]:
    proposals = generate_strategy_proposals(report)
    output_dir.mkdir(parents=True, exist_ok=True)
    valid_names = set()
    for proposal in proposals:
        path = output_dir / f"{proposal['proposal_id']}.json"
        valid_names.add(path.name)
        path.write_text(json.dumps(proposal, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    archive_dir = output_dir / "_stale"
    for old_path in sorted(output_dir.glob("*.json")):
        if old_path.name in valid_names:
            continue
        archive_dir.mkdir(parents=True, exist_ok=True)
        old_path.replace(archive_dir / old_path.name)
    return proposals


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate mechanical Phoenix strategy proposals.")
    parser.add_argument("--analysis-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = read_json(args.analysis_report)
    if not isinstance(report, dict):
        raise SystemExit(f"could not read analysis report: {args.analysis_report}")
    proposals = write_strategy_proposals(report=report, output_dir=args.output_dir)
    print(f"strategy_proposals count={len(proposals)} output_dir={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
