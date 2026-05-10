#!/usr/bin/env python3
"""Reporting-only factor factory for candidate shadow outcomes.

This does not add strategies and does not touch the original research_pool
factor_factory_report/control files. It filters candidate shadow outcomes by a
small branch_type allowlist and writes separate candidate-only reports.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path("/opt/phoenix-testnet")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from phoenix_factor_factory import (  # noqa: E402
    FACTOR_SIGNAL_RULES,
    build_factor_factory_control,
    build_factor_factory_report,
    read_recent_jsonl_records,
)


RUNS_DIR = ROOT / "signal_lab_runs"
CANDIDATE_DIR = RUNS_DIR / "vps_forward_shadow_candidate_validation_20260430_072436"
SNAPSHOTS_FILE = RUNS_DIR / "event_collect_v6_speed_boost" / "bridge_event_feed.jsonl"
OUTCOMES_FILE = CANDIDATE_DIR / "mainnet_shadow" / "signal_bridge_shadow_outcomes.jsonl"
PROMOTION_FILE = CANDIDATE_DIR / "mainnet_shadow" / "promotion_gate_report.json"
READINESS_FILE = CANDIDATE_DIR / "mainnet_shadow" / "mainnet_shadow_readiness.json"

REPORT_FILE = RUNS_DIR / "candidate_factor_factory_report.json"
CONTROL_FILE = RUNS_DIR / "candidate_factor_factory_control.json"
MD_FILE = RUNS_DIR / "candidate_factor_factory_report.md"

ALLOWED_BRANCH_TYPES = ("candidate_strategy_discovery", "candidate_oi_unwind_reversal")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    return payload if isinstance(payload, dict) else {}


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


def normalize_branch_type(value: Any) -> str:
    return str(value or "").strip().lower()


def filter_candidate_outcomes(
    outcomes: list[dict[str, Any]],
    *,
    allowed_branch_types: set[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    branch_type_counts: Counter[str] = Counter()
    playbook_counts: Counter[str] = Counter()
    shadow_branch_counts: Counter[str] = Counter()
    excluded_counts: Counter[str] = Counter()
    for outcome in outcomes:
        branch_type = normalize_branch_type(outcome.get("branch_type"))
        branch_type_counts[branch_type or "<missing>"] += 1
        playbook_counts[str(outcome.get("playbook") or "<missing>")] += 1
        shadow_branch_counts[str(outcome.get("shadow_branch_id") or "<missing>")] += 1
        if branch_type not in allowed_branch_types:
            excluded_counts[branch_type or "<missing>"] += 1
            continue
        kept.append(outcome)
    profile = {
        "allowed_branch_types": sorted(allowed_branch_types),
        "input_outcome_count": len(outcomes),
        "filtered_outcome_count": len(kept),
        "excluded_outcome_count": len(outcomes) - len(kept),
        "input_branch_type_counts": dict(branch_type_counts.most_common()),
        "excluded_branch_type_counts": dict(excluded_counts.most_common()),
        "playbook_counts": dict(playbook_counts.most_common()),
        "shadow_branch_id_counts_top20": dict(shadow_branch_counts.most_common(20)),
    }
    return kept, profile


def add_reporting_safety(
    report: dict[str, Any],
    control: dict[str, Any],
    *,
    candidate_dir: Path,
    outcome_filter_profile: dict[str, Any],
) -> None:
    promotion = read_json(PROMOTION_FILE)
    readiness = read_json(READINESS_FILE)
    safety = {
        "reporting_only": True,
        "candidate_factor_factory_affects_live": False,
        "candidate_factor_factory_affects_promotion": False,
        "live_trading_enabled": bool(
            promotion.get("live_trading_enabled", readiness.get("live_trading_enabled", False))
        ),
        "promotion_allowed": bool(promotion.get("promotion_allowed", False)),
        "source_candidate_dir": str(candidate_dir),
        "source_promotion_gate_report": str(PROMOTION_FILE),
        "source_readiness_report": str(READINESS_FILE),
    }
    report["candidate_factor_factory"] = {
        "mode": "candidate_shadow_reporting_only",
        "branch_type_allowlist": list(ALLOWED_BRANCH_TYPES),
        "outcome_filter_profile": outcome_filter_profile,
        "safety": safety,
    }
    control["mode"] = "candidate_shadow_reporting_only"
    control["live_trading_enabled"] = False
    control["promotion_allowed"] = False
    control["reporting_only"] = True
    control["branch_type_allowlist"] = list(ALLOWED_BRANCH_TYPES)
    control["outcome_filter_profile"] = outcome_filter_profile
    control["safety"] = safety


def top_rule_payload(report: dict[str, Any]) -> dict[str, Any]:
    ranked = report.get("ranked_candidate_rules")
    if isinstance(ranked, list) and ranked:
        top = ranked[0]
        return top if isinstance(top, dict) else {}
    return {}


def render_markdown(report: dict[str, Any], control: dict[str, Any]) -> str:
    input_counts = report.get("input_counts") or {}
    filters = report.get("filters") or {}
    candidate_meta = report.get("candidate_factor_factory") or {}
    safety = candidate_meta.get("safety") or {}
    top_rule = top_rule_payload(report)
    lines = [
        "# Phoenix Candidate Factor Factory Report",
        "",
        f"- Generated at UTC: `{report.get('generated_at')}`",
        "- Mode: `candidate_shadow_reporting_only`",
        f"- Branch type allowlist: `{candidate_meta.get('branch_type_allowlist')}`",
        f"- Rule evaluation rows: `{input_counts.get('rule_evaluation_rows')}`",
        f"- Snapshots / outcomes read: `{input_counts.get('snapshots_read')}` / `{input_counts.get('outcomes_read')}`",
        f"- Original branch filter passed to factory: `{filters.get('branch_type')}`",
        f"- Top rule: `{top_rule.get('rule')}`",
        f"- Top PF / avg_return: `{top_rule.get('profit_factor')}` / `{top_rule.get('avg_return_pct')}`",
        f"- live_trading_enabled: `{safety.get('live_trading_enabled')}`",
        f"- promotion_allowed: `{safety.get('promotion_allowed')}`",
        "",
        "## Rule Performance",
        "",
        "| rule | sample_count | trigger_count | matched_side | avg_return_pct | PF | win_rate_pct | status |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    rules = report.get("candidate_rule_performance")
    control_rules = control.get("rules") if isinstance(control.get("rules"), dict) else {}
    if isinstance(rules, dict):
        for rule in FACTOR_SIGNAL_RULES:
            row = rules.get(rule.name) if isinstance(rules.get(rule.name), dict) else {}
            control_row = control_rules.get(rule.name) if isinstance(control_rules.get(rule.name), dict) else {}
            lines.append(
                "| {rule} | {sample} | {trigger} | {matched} | {avg} | {pf} | {win} | {status} |".format(
                    rule=rule.name,
                    sample=row.get("sample_count"),
                    trigger=row.get("trigger_count"),
                    matched=row.get("matched_outcome_side_count"),
                    avg=row.get("avg_return_pct"),
                    pf=row.get("profit_factor"),
                    win=row.get("win_rate_pct"),
                    status=control_row.get("status"),
                )
            )
    lines.extend(
        [
            "",
            "## Outcome Filter",
            "",
            "```json",
            json.dumps(candidate_meta.get("outcome_filter_profile") or {}, ensure_ascii=False, indent=2, sort_keys=True),
            "```",
        ]
    )
    return "\n".join(lines) + "\n"


def build_reports(args: argparse.Namespace) -> dict[str, Any]:
    allowed = {normalize_branch_type(item) for item in args.branch_type_allowlist.split(",") if item.strip()}
    snapshots = read_recent_jsonl_records(args.snapshots_file, max_records=max(0, int(args.max_snapshots)))
    outcomes = read_recent_jsonl_records(args.outcomes_file, max_records=max(0, int(args.max_outcomes)))
    candidate_outcomes, filter_profile = filter_candidate_outcomes(outcomes, allowed_branch_types=allowed)
    generated_at = now_iso()
    report = build_factor_factory_report(
        snapshots=snapshots,
        outcomes=candidate_outcomes,
        generated_at=generated_at,
        branch_type="",
        min_samples=max(1, int(args.min_samples)),
        pair_min_samples=max(1, int(args.pair_min_samples)),
        top_n=max(1, int(args.top_n)),
        include_non_research=False,
    )
    control = build_factor_factory_control(report, min_outcomes=max(1, int(args.min_samples)))
    add_reporting_safety(
        report,
        control,
        candidate_dir=args.candidate_dir,
        outcome_filter_profile=filter_profile,
    )
    atomic_write_json(args.report_file, report)
    atomic_write_json(args.control_file, control)
    args.markdown_file.write_text(render_markdown(report, control), encoding="utf-8")
    top_rule = top_rule_payload(report)
    return {
        "report_file": str(args.report_file),
        "control_file": str(args.control_file),
        "markdown_file": str(args.markdown_file),
        "rule_evaluation_rows": (report.get("input_counts") or {}).get("rule_evaluation_rows"),
        "top_rule": top_rule.get("rule"),
        "top_rule_profit_factor": top_rule.get("profit_factor"),
        "top_rule_avg_return_pct": top_rule.get("avg_return_pct"),
        "live_trading_enabled": (report.get("candidate_factor_factory") or {}).get("safety", {}).get("live_trading_enabled"),
        "promotion_allowed": (report.get("candidate_factor_factory") or {}).get("safety", {}).get("promotion_allowed"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build reporting-only factor factory report for candidate shadow outcomes.")
    parser.add_argument("--candidate-dir", type=Path, default=CANDIDATE_DIR)
    parser.add_argument("--snapshots-file", type=Path, default=SNAPSHOTS_FILE)
    parser.add_argument("--outcomes-file", type=Path, default=OUTCOMES_FILE)
    parser.add_argument("--report-file", type=Path, default=REPORT_FILE)
    parser.add_argument("--control-file", type=Path, default=CONTROL_FILE)
    parser.add_argument("--markdown-file", type=Path, default=MD_FILE)
    parser.add_argument("--branch-type-allowlist", default=",".join(ALLOWED_BRANCH_TYPES))
    parser.add_argument("--max-snapshots", type=int, default=5000)
    parser.add_argument("--max-outcomes", type=int, default=5000)
    parser.add_argument("--min-samples", type=int, default=30)
    parser.add_argument("--pair-min-samples", type=int, default=20)
    parser.add_argument("--top-n", type=int, default=25)
    return parser.parse_args()


def main() -> int:
    summary = build_reports(parse_args())
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
