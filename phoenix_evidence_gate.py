from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


EVIDENCE_GATE_VERSION = "evidence_gate_v1"
LEVEL_DESCRIPTIONS = {
    0: "backtest_only",
    1: "replay_validated",
    2: "forward_shadow_active",
    3: "shadow_cost_adjusted_positive",
    4: "testnet_net_positive",
    5: "mainnet_eligible_manual_approval_required",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _effective_evidence_level(manifest: dict[str, Any], parity_report: dict[str, Any] | None) -> int:
    level = int(safe_float(manifest.get("evidence_level"), 0.0))
    if isinstance(parity_report, dict) and not bool(parity_report.get("passed", True)):
        cap = int(safe_float(parity_report.get("evidence_level_cap"), 1.0))
        level = min(level, cap)
    return level


def evaluate_evidence_gate(
    manifest: dict[str, Any],
    *,
    target_stage: str = "testnet",
    parity_report: dict[str, Any] | None = None,
    thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    thresholds = {
        "min_shadow_outcomes": 50,
        "min_shadow_profit_factor": 1.2,
        "max_shadow_symbol_concentration_pct": 50.0,
        "min_testnet_trades": 50,
        "min_testnet_profit_factor": 1.1,
        **_as_dict(thresholds),
    }
    status = str(manifest.get("status") or "")
    raw_level = int(safe_float(manifest.get("evidence_level"), 0.0))
    effective_level = _effective_evidence_level(manifest, parity_report)
    shadow = _as_dict(manifest.get("shadow_evidence"))
    testnet = _as_dict(manifest.get("testnet_evidence"))
    block_reasons: list[str] = []

    if target_stage == "mainnet":
        block_reasons.append("blocked_mainnet_live_manual_approval_required")
    if isinstance(parity_report, dict) and not bool(parity_report.get("passed", True)):
        block_reasons.append("blocked_entry_parity_failed")
    if target_stage == "shadow":
        if status == "disabled":
            block_reasons.append("blocked_manifest_disabled")
    elif target_stage == "testnet":
        if status not in {"limited_testnet", "promoted_testnet"}:
            block_reasons.append("blocked_manifest_status_not_testnet")
        if effective_level < 3:
            block_reasons.append("blocked_evidence_level_too_low")
        if raw_level != effective_level:
            block_reasons.append("blocked_evidence_capped_by_parity")
        shadow_outcomes = int(safe_float(shadow.get("outcomes"), 0.0))
        shadow_avg_net = safe_float(shadow.get("avg_net_edge_bps"), 0.0)
        shadow_pf = safe_float(shadow.get("profit_factor"), 0.0)
        symbol_concentration = safe_float(shadow.get("max_symbol_concentration_pct"), 100.0)
        if effective_level >= 3:
            if shadow_outcomes < int(thresholds["min_shadow_outcomes"]):
                block_reasons.append("blocked_shadow_sample_too_small")
            if shadow_avg_net <= 0:
                block_reasons.append("blocked_shadow_net_edge_not_positive")
            if shadow_pf < float(thresholds["min_shadow_profit_factor"]):
                block_reasons.append("blocked_shadow_profit_factor_too_low")
            if symbol_concentration > float(thresholds["max_shadow_symbol_concentration_pct"]):
                block_reasons.append("blocked_shadow_single_symbol_dominated")
        if status == "promoted_testnet" or effective_level >= 4:
            testnet_trades = int(safe_float(testnet.get("trades"), 0.0))
            testnet_net_pnl = safe_float(testnet.get("net_pnl"), safe_float(testnet.get("net_pnl_usdt"), 0.0))
            testnet_pf = safe_float(testnet.get("profit_factor"), 0.0)
            if testnet_trades < int(thresholds["min_testnet_trades"]):
                block_reasons.append("blocked_testnet_sample_too_small")
            if testnet_net_pnl <= 0:
                block_reasons.append("blocked_testnet_net_pnl_not_positive")
            if testnet_pf < float(thresholds["min_testnet_profit_factor"]):
                block_reasons.append("blocked_testnet_profit_factor_too_low")
    else:
        block_reasons.append("blocked_unknown_target_stage")

    unique_reasons = list(dict.fromkeys(block_reasons))
    return {
        "report_type": "evidence_gate_decision",
        "version": EVIDENCE_GATE_VERSION,
        "generated_at": utc_now_iso(),
        "strategy_id": manifest.get("strategy_id"),
        "target_stage": target_stage,
        "decision": "block" if unique_reasons else "allow",
        "block_reasons": unique_reasons,
        "manifest_status": status,
        "raw_evidence_level": raw_level,
        "effective_evidence_level": effective_level,
        "evidence_level_description": LEVEL_DESCRIPTIONS.get(effective_level, "unknown"),
        "thresholds": thresholds,
        "shadow_evidence": shadow,
        "testnet_evidence": testnet,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }


def build_evidence_gate_report(decisions: Iterable[dict[str, Any]]) -> dict[str, Any]:
    decision_list = list(decisions)
    reason_counts: dict[str, int] = {}
    for decision in decision_list:
        for reason in decision.get("block_reasons") or []:
            reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + 1
    return {
        "report_type": "evidence_gate_report",
        "version": EVIDENCE_GATE_VERSION,
        "generated_at": utc_now_iso(),
        "decision_count": len(decision_list),
        "blocked_count": sum(1 for decision in decision_list if decision.get("decision") == "block"),
        "allowed_count": sum(1 for decision in decision_list if decision.get("decision") == "allow"),
        "block_reason_counts": reason_counts,
        "decisions": decision_list,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }


def write_evidence_gate_outputs(decisions: Iterable[dict[str, Any]], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = build_evidence_gate_report(decisions)
    for name in ("evidence_gate_report.json", "promotion_gate_report.json", "strategy_evidence_registry.json"):
        (output_dir / name).write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Phoenix strategy evidence gates.")
    parser.add_argument("--manifest-json", type=Path, required=True)
    parser.add_argument("--target-stage", default="testnet", choices=["shadow", "testnet", "mainnet"])
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = json.loads(args.manifest_json.read_text(encoding="utf-8"))
    decision = evaluate_evidence_gate(manifest, target_stage=args.target_stage)
    write_evidence_gate_outputs([decision], args.output_dir)
    print(f"evidence_gate decision={decision['decision']} reasons={','.join(decision['block_reasons'])}")
    return 0 if decision["decision"] == "allow" else 2


if __name__ == "__main__":
    raise SystemExit(main())
