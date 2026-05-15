from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


COST_GATE_VERSION = "cost_gate_v1"
TINY_COST_BPS = 1e-9


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_number(*values: Any) -> float | None:
    for value in values:
        parsed = safe_float(value, None)
        if parsed is not None:
            return parsed
    return None


def _lookup(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping.get(key)
    return None


def _nested_lookup(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        direct = _lookup(mapping, key)
        if direct is not None:
            return direct
    for nested_key in ("cost_model", "expected_cost_model", "cost"):
        nested = mapping.get(nested_key)
        if isinstance(nested, dict):
            value = _lookup(nested, *keys)
            if value is not None:
                return value
    return None


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def build_expected_cost_debug(
    *,
    candidate: dict[str, Any] | None = None,
    manifest: dict[str, Any] | None = None,
    experiment: dict[str, Any] | None = None,
    cost_model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate = _as_dict(candidate)
    manifest = _as_dict(manifest)
    experiment = _as_dict(experiment)
    cost_model = _as_dict(cost_model)
    sources = [candidate, cost_model, experiment, manifest]

    def pick(*keys: str) -> float | None:
        return _first_number(*[_nested_lookup(source, *keys) for source in sources])

    expected_gross_move_bps = pick("expected_gross_move_bps", "expected_follow_through_bps")
    expected_fee_bps = pick("expected_fee_bps", "fee_bps")
    expected_spread_bps = pick("expected_spread_bps", "spread_bps", "max_expected_spread_bps")
    expected_slippage_bps = pick("expected_slippage_bps", "slippage_bps", "estimated_slippage_bps")
    expected_funding_bps = pick("expected_funding_bps", "funding_bps", "expected_funding_cost_bps")
    explicit_total_cost_bps = pick("expected_total_cost_bps", "max_expected_total_cost_bps")
    explicit_net_edge_bps = _first_number(
        _nested_lookup(candidate, "expected_net_edge_bps"),
        _nested_lookup(cost_model, "expected_net_edge_bps"),
        _nested_lookup(experiment, "expected_net_edge_bps"),
    )

    component_values = [expected_fee_bps, expected_spread_bps, expected_slippage_bps, expected_funding_bps]
    if explicit_total_cost_bps is not None:
        expected_total_cost_bps = explicit_total_cost_bps
    elif all(value is not None for value in component_values):
        expected_total_cost_bps = sum(float(value or 0.0) for value in component_values)
    else:
        expected_total_cost_bps = None

    computed_net_edge_bps = (
        expected_gross_move_bps - expected_total_cost_bps
        if expected_gross_move_bps is not None and expected_total_cost_bps is not None
        else None
    )
    expected_net_edge_bps = explicit_net_edge_bps if explicit_net_edge_bps is not None else computed_net_edge_bps
    cost_to_edge_ratio = (
        expected_gross_move_bps / max(float(expected_total_cost_bps), TINY_COST_BPS)
        if expected_gross_move_bps is not None and expected_total_cost_bps is not None
        else None
    )
    return {
        "expected_gross_move_bps": expected_gross_move_bps,
        "expected_fee_bps": expected_fee_bps,
        "expected_spread_bps": expected_spread_bps,
        "expected_slippage_bps": expected_slippage_bps,
        "expected_funding_bps": expected_funding_bps,
        "expected_total_cost_bps": expected_total_cost_bps,
        "expected_net_edge_bps": expected_net_edge_bps,
        "computed_net_edge_bps": computed_net_edge_bps,
        "explicit_net_edge_bps": explicit_net_edge_bps,
        "cost_to_edge_ratio": cost_to_edge_ratio,
        "liquidity_bucket": _lookup(candidate, "liquidity_bucket") or _lookup(experiment, "liquidity_bucket"),
        "symbol": _lookup(candidate, "symbol") or _lookup(experiment, "symbol"),
        "strategy_id": _lookup(manifest, "strategy_id") or _lookup(experiment, "strategy_id") or _lookup(candidate, "setup"),
    }


def evaluate_cost_gate(
    *,
    candidate: dict[str, Any] | None = None,
    manifest: dict[str, Any] | None = None,
    experiment: dict[str, Any] | None = None,
    cost_model: dict[str, Any] | None = None,
    thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = _as_dict(manifest)
    thresholds = _as_dict(thresholds)
    debug = build_expected_cost_debug(candidate=candidate, manifest=manifest, experiment=experiment, cost_model=cost_model)
    block_reasons: list[str] = []
    warnings: list[str] = []

    gross = debug["expected_gross_move_bps"]
    total = debug["expected_total_cost_bps"]
    net = debug["expected_net_edge_bps"]
    ratio = debug["cost_to_edge_ratio"]
    if gross is None or total is None:
        block_reasons.append("blocked_missing_cost_model")
        if net is not None and net <= 0:
            block_reasons.append("blocked_negative_expected_net_edge")
    else:
        if net is None or net <= 0:
            block_reasons.append("blocked_negative_expected_net_edge")
        min_ratio = safe_float(manifest.get("min_cost_to_edge_ratio"), None)
        if min_ratio is None:
            min_ratio = safe_float(thresholds.get("min_cost_to_edge_ratio"), 3.0) or 3.0
        if gross < float(min_ratio) * total:
            block_reasons.append("blocked_cost_to_edge_too_low")

    slippage = debug["expected_slippage_bps"]
    max_slippage = _first_number(
        thresholds.get("max_slippage_bps"),
        manifest.get("max_expected_slippage_bps"),
        manifest.get("max_slippage_bps"),
    )
    symbol_median_slippage = _first_number(_as_dict(candidate or {}).get("symbol_median_slippage_bps"), _as_dict(experiment or {}).get("symbol_median_slippage_bps"))
    if slippage is not None and max_slippage is not None and slippage > max_slippage:
        block_reasons.append("blocked_high_slippage")
    if slippage is not None and symbol_median_slippage is not None:
        multiplier = safe_float(thresholds.get("max_symbol_median_slippage_multiple"), 2.0) or 2.0
        if slippage > symbol_median_slippage * multiplier:
            block_reasons.append("blocked_high_slippage")
    if bool(_as_dict(candidate or {}).get("high_slippage_symbol")) or bool(_as_dict(experiment or {}).get("high_slippage_symbol")):
        block_reasons.append("blocked_high_slippage")

    spread = debug["expected_spread_bps"]
    max_spread = _first_number(thresholds.get("max_spread_bps"), manifest.get("max_expected_spread_bps"), manifest.get("max_spread_bps"))
    if spread is not None and max_spread is not None and spread > max_spread:
        block_reasons.append("blocked_spread_too_wide")
    if bool(_as_dict(candidate or {}).get("spread_abnormally_wide")):
        block_reasons.append("blocked_spread_too_wide")

    allowed_liquidity = manifest.get("allowed_liquidity_buckets") or []
    if allowed_liquidity and debug.get("liquidity_bucket") and str(debug["liquidity_bucket"]) not in {str(item) for item in allowed_liquidity}:
        block_reasons.append("blocked_liquidity_bucket")

    funding = debug["expected_funding_bps"]
    max_funding = _first_number(thresholds.get("max_abs_funding_bps"), manifest.get("max_expected_funding_bps"), manifest.get("max_funding_bps"))
    if funding is not None and max_funding is not None and abs(funding) > max_funding:
        block_reasons.append("blocked_funding_cost")

    unique_reasons = list(dict.fromkeys(block_reasons))
    return {
        "report_type": "cost_gate_decision",
        "version": COST_GATE_VERSION,
        "generated_at": utc_now_iso(),
        "decision": "block" if unique_reasons else "allow",
        "block_reasons": unique_reasons,
        "warnings": warnings,
        "debug": debug,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }


def build_cost_gate_report(decisions: Iterable[dict[str, Any]]) -> dict[str, Any]:
    decision_list = list(decisions)
    reason_counts: dict[str, int] = {}
    for decision in decision_list:
        for reason in decision.get("block_reasons") or []:
            reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + 1
    return {
        "report_type": "cost_gate_report",
        "version": COST_GATE_VERSION,
        "generated_at": utc_now_iso(),
        "candidate_count": len(decision_list),
        "blocked_count": sum(1 for decision in decision_list if decision.get("decision") == "block"),
        "allowed_count": sum(1 for decision in decision_list if decision.get("decision") == "allow"),
        "block_reason_counts": reason_counts,
        "decisions": decision_list,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }


def format_daily_cost_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix Daily Cost Summary",
        "",
        f"- generated_at: {report.get('generated_at')}",
        f"- candidates: {report.get('candidate_count', 0)}",
        f"- allowed: {report.get('allowed_count', 0)}",
        f"- blocked: {report.get('blocked_count', 0)}",
        "",
        "## Block Reasons",
    ]
    for reason, count in sorted((report.get("block_reason_counts") or {}).items()):
        lines.append(f"- {reason}: {count}")
    lines.append("")
    return "\n".join(lines)


def write_cost_gate_outputs(decisions: Iterable[dict[str, Any]], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    decision_list = list(decisions)
    report = build_cost_gate_report(decision_list)
    (output_dir / "cost_gate_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "per_candidate_cost_debug.jsonl").open("w", encoding="utf-8") as handle:
        for decision in decision_list:
            handle.write(json.dumps(decision, ensure_ascii=False, sort_keys=True) + "\n")
    (output_dir / "daily_cost_summary.md").write_text(format_daily_cost_summary(report), encoding="utf-8")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Phoenix cost-aware gate decisions.")
    parser.add_argument("--candidate-json", type=Path, required=True)
    parser.add_argument("--manifest-json", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    candidate = json.loads(args.candidate_json.read_text(encoding="utf-8"))
    manifest = json.loads(args.manifest_json.read_text(encoding="utf-8"))
    decision = evaluate_cost_gate(candidate=candidate, manifest=manifest)
    write_cost_gate_outputs([decision], args.output_dir)
    print(f"cost_gate decision={decision['decision']} reasons={','.join(decision['block_reasons'])}")
    return 0 if decision["decision"] == "allow" else 2


if __name__ == "__main__":
    raise SystemExit(main())
