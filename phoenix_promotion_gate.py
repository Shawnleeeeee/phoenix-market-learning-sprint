from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from phoenix_learning_analyzer import read_json


PROMOTION_GATE_VERSION = "v1.0"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def first_metric(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = optional_float(payload.get(key))
        if value is not None:
            return value
    return None


def parity_passed(experiment_result: dict[str, Any], candidate: dict[str, Any]) -> bool | None:
    for payload in (candidate, experiment_result):
        for key in ("parity_passed", "entry_parity_passed"):
            if key in payload:
                return bool(payload.get(key))
    parity = experiment_result.get("parity_report")
    if isinstance(parity, dict) and "passed" in parity:
        return bool(parity.get("passed"))
    parity = candidate.get("parity_report")
    if isinstance(parity, dict) and "passed" in parity:
        return bool(parity.get("passed"))
    return None


def force_gate_safety(payload: dict[str, Any]) -> dict[str, Any]:
    safe = dict(payload)
    safe["live_trading_enabled"] = False
    safe["mainnet_live_promotion_allowed"] = False
    safe["direct_live_change_allowed"] = False
    safe["agent_enabled"] = False
    safe["auto_promotion_allowed"] = False
    return safe


def evaluate_promotion_gate(
    experiment_result: dict[str, Any],
    *,
    thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    limits = {
        "minimum_sample_count": 50,
        "minimum_independent_event_count": 30,
        "minimum_profit_factor": 1.2,
        "maximum_mc_negative_probability": 0.35,
        "maximum_drawdown_abs": 10.0,
        "minimum_evidence_level_for_testnet": 3,
        "maximum_symbol_concentration_pct": 50.0,
        "minimum_cost_to_edge_ratio": 3.0,
    }
    if thresholds:
        limits.update(thresholds)
    candidate = experiment_result.get("candidate") if isinstance(experiment_result.get("candidate"), dict) else {}
    baseline = experiment_result.get("baseline") if isinstance(experiment_result.get("baseline"), dict) else {}
    sample_count = int(candidate.get("sample_count") or 0)
    independent_count = int(candidate.get("independent_event_count") or 0)
    net_pnl = first_metric(candidate, "net_pnl", "net_pnl_usdt", "total_net_pnl", "total_net_pnl_usdt", "avg_net_pnl")
    after_cost_edge = first_metric(
        candidate,
        "after_cost_edge",
        "after_cost_edge_bps",
        "after_cost_net_edge_bps",
        "avg_after_cost_return_pct",
    )
    expected_net_edge_bps = first_metric(candidate, "expected_net_edge_bps")
    expected_gross_move_bps = first_metric(candidate, "expected_gross_move_bps", "expected_follow_through_bps")
    expected_total_cost_bps = first_metric(candidate, "expected_total_cost_bps", "max_expected_total_cost_bps")
    profit_factor = safe_float(candidate.get("profit_factor"))
    max_drawdown = safe_float(candidate.get("max_drawdown"))
    evidence_level = int(safe_float(experiment_result.get("evidence_level", candidate.get("evidence_level")), 0.0))
    symbol_concentration = first_metric(candidate, "max_symbol_concentration_pct", "symbol_concentration_pct")
    parity_ok = parity_passed(experiment_result, candidate)
    fee_drag_ratio = safe_float(candidate.get("fee_drag_ratio"))
    baseline_fee_drag_ratio = safe_float(baseline.get("fee_drag_ratio"), fee_drag_ratio)
    tp_net_loss_count = int(candidate.get("take_profit_net_loss_count") or 0)
    baseline_tp_net_loss_count = int(baseline.get("take_profit_net_loss_count") or tp_net_loss_count)
    mc_negative = candidate.get("monte_carlo_probability_total_return_negative")
    mc_negative_value = safe_float(mc_negative, 0.0) if mc_negative is not None else None

    min_sample = int(limits["minimum_sample_count"])
    min_independent = int(limits["minimum_independent_event_count"])
    min_evidence = int(limits["minimum_evidence_level_for_testnet"])
    min_cost_ratio = float(limits["minimum_cost_to_edge_ratio"])
    blockers: list[str] = []
    checks = {
        "sample_enough": sample_count >= min_sample and independent_count >= min_independent,
        "net_pnl_positive": net_pnl is not None and net_pnl > 0,
        "after_cost_edge_positive": after_cost_edge is not None and after_cost_edge > 0,
        "expected_net_edge_positive": expected_net_edge_bps is not None and expected_net_edge_bps > 0,
        "gross_move_covers_cost": (
            expected_gross_move_bps is not None
            and expected_total_cost_bps is not None
            and expected_total_cost_bps > 0
            and expected_gross_move_bps >= min_cost_ratio * expected_total_cost_bps
        ),
        "evidence_level_ok": evidence_level >= min_evidence,
        "entry_parity_passed": parity_ok is True,
        "symbol_concentration_ok": symbol_concentration is not None
        and symbol_concentration <= float(limits["maximum_symbol_concentration_pct"]),
        "profit_factor_ok": profit_factor >= float(limits["minimum_profit_factor"]),
        "monte_carlo_ok": mc_negative_value is None or mc_negative_value < float(limits["maximum_mc_negative_probability"]),
        "drawdown_ok": abs(max_drawdown) <= float(limits["maximum_drawdown_abs"]),
        "fee_drag_not_worse": fee_drag_ratio <= baseline_fee_drag_ratio,
        "take_profit_net_loss_not_worse": tp_net_loss_count <= baseline_tp_net_loss_count,
    }

    if sample_count < min_sample:
        blockers.append("sample_count_too_small")
    if independent_count < min_independent:
        blockers.append("independent_event_count_too_small")
    if net_pnl is None:
        blockers.append("missing_net_pnl")
    elif net_pnl <= 0:
        blockers.append("net_pnl_not_positive")
    if after_cost_edge is None:
        blockers.append("missing_after_cost_edge")
    elif after_cost_edge <= 0:
        blockers.append("after_cost_edge_not_positive")
    if expected_net_edge_bps is None:
        blockers.append("missing_expected_net_edge_bps")
    elif expected_net_edge_bps <= 0:
        blockers.append("expected_net_edge_bps_not_positive")
    if expected_gross_move_bps is None:
        blockers.append("missing_expected_gross_move_bps")
    if expected_total_cost_bps is None:
        blockers.append("missing_expected_total_cost_bps")
    elif expected_total_cost_bps <= 0:
        blockers.append("expected_total_cost_bps_not_positive")
    if (
        expected_gross_move_bps is not None
        and expected_total_cost_bps is not None
        and expected_total_cost_bps > 0
        and expected_gross_move_bps < min_cost_ratio * expected_total_cost_bps
    ):
        blockers.append("gross_move_less_than_3x_total_cost")
    if evidence_level < min_evidence:
        blockers.append("evidence_level_too_low_for_testnet")
    if parity_ok is None:
        blockers.append("missing_entry_parity_pass")
    elif parity_ok is not True:
        blockers.append("entry_parity_failed")
    if symbol_concentration is None:
        blockers.append("missing_symbol_concentration")
    elif symbol_concentration > float(limits["maximum_symbol_concentration_pct"]):
        blockers.append("symbol_concentration_too_high")
    if not checks["profit_factor_ok"]:
        blockers.append("profit_factor_too_low")
    if not checks["monte_carlo_ok"]:
        blockers.append("monte_carlo_negative_probability_too_high")
    if not checks["drawdown_ok"]:
        blockers.append("drawdown_too_large")
    if not checks["fee_drag_not_worse"]:
        blockers.append("fee_drag_worse_than_baseline")
    if not checks["take_profit_net_loss_not_worse"]:
        blockers.append("take_profit_net_loss_worse_than_baseline")

    status = "manual_review_required" if not blockers else "blocked"

    if int(experiment_result.get("hard_safety_fail_count") or 0) >= int(experiment_result.get("hard_safety_fail_limit") or 3):
        status = "rollback_required"
        blockers.append("hard_safety_fail_limit_reached")

    return force_gate_safety(
        {
            "report_type": "promotion_gate_decision",
            "version": PROMOTION_GATE_VERSION,
            "generated_at": utc_now_iso(),
            "experiment_id": experiment_result.get("experiment_id"),
            "strategy_id": experiment_result.get("strategy_id"),
            "candidate_version": experiment_result.get("candidate_version") or experiment_result.get("version"),
            "parent_version": experiment_result.get("parent_version"),
            "status": status,
            "blockers": list(dict.fromkeys(blockers)),
            "checks": checks,
            "metrics_used_for_promotion_review": {
                "net_pnl": net_pnl,
                "after_cost_edge": after_cost_edge,
                "expected_net_edge_bps": expected_net_edge_bps,
                "expected_gross_move_bps": expected_gross_move_bps,
                "expected_total_cost_bps": expected_total_cost_bps,
                "evidence_level": evidence_level,
                "parity_passed": parity_ok,
                "symbol_concentration_pct": symbol_concentration,
            },
            "ignored_for_promotion": {
                "avg_return": candidate.get("avg_return"),
                "avg_return_pct": candidate.get("avg_return_pct"),
                "gross_pnl": candidate.get("gross_pnl"),
                "gross_pnl_usdt": candidate.get("gross_pnl_usdt"),
                "backtest_pnl": candidate.get("backtest_pnl"),
            },
            "thresholds": limits,
            "candidate": candidate,
            "baseline": baseline,
        }
    )


def read_experiment_results(results_dir: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not results_dir.exists():
        return results
    for path in sorted(results_dir.glob("*.json")):
        payload = read_json(path)
        if isinstance(payload, dict):
            results.append(payload)
    return results


def build_promotion_gate_report(results: Iterable[dict[str, Any]]) -> dict[str, Any]:
    decisions = [evaluate_promotion_gate(result) for result in results]
    counts: dict[str, int] = {}
    for decision in decisions:
        counts[str(decision.get("status"))] = counts.get(str(decision.get("status")), 0) + 1
    return force_gate_safety(
        {
            "report_type": "promotion_gate_learning_report",
            "version": PROMOTION_GATE_VERSION,
            "generated_at": utc_now_iso(),
            "decision_count": len(decisions),
            "status_counts": counts,
            "decisions": decisions,
        }
    )


def format_promotion_gate_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix Promotion Gate Learning Report",
        "",
        f"- generated_at: {report.get('generated_at')}",
        f"- decision_count: {report.get('decision_count', 0)}",
        f"- live_trading_enabled: {str(report.get('live_trading_enabled')).lower()}",
        f"- mainnet_live_promotion_allowed: {str(report.get('mainnet_live_promotion_allowed')).lower()}",
        "",
        "## Decisions",
    ]
    for decision in report.get("decisions") or []:
        lines.append(
            f"- {decision.get('experiment_id')}: {decision.get('status')} "
            f"pf={decision.get('candidate', {}).get('profit_factor')} "
            f"avg={decision.get('candidate', {}).get('avg_net_pnl')}"
        )
    lines.append("")
    return "\n".join(lines)


def write_promotion_gate_report(
    *,
    results: Iterable[dict[str, Any]],
    output_json: Path,
    output_md: Path,
) -> dict[str, Any]:
    report = build_promotion_gate_report(results)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(format_promotion_gate_markdown(report), encoding="utf-8")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Phoenix candidate experiments against promotion gates.")
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_json = args.output_json or args.results_dir.parent / "promotion_gate_learning_report.json"
    output_md = args.output_md or args.results_dir.parent / "promotion_gate_learning_report.md"
    report = write_promotion_gate_report(
        results=read_experiment_results(args.results_dir),
        output_json=output_json,
        output_md=output_md,
    )
    print(f"promotion_gate decisions={report['decision_count']} output_json={output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
