from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix.hermes_decision import validate_hermes_decision
from phoenix.review_reporter import append_review_log, build_review_report
from phoenix.risk_governor import RiskGovernorConfig, append_jsonl, evaluate_risk
from phoenix.trader_snapshot import build_trader_snapshot


def run_trader_cycle(
    *,
    snapshot_payload: dict[str, Any] | None = None,
    decision_payload: dict[str, Any] | None = None,
    output_dir: str | Path = "hermes_trader_mode_logs",
    dry_run: bool = True,
    environment: dict[str, Any] | None = None,
    risk_config: RiskGovernorConfig | None = None,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    snapshot = snapshot_payload or build_trader_snapshot(
        system_status={"data_fresh": False, "exchange_status": "unavailable", "websocket_status": "unavailable"}
    )
    decision_payload = decision_payload or _mock_no_trade_decision()
    environment = {
        "runtime_mode": "DRY_RUN" if dry_run else "TESTNET_LIVE",
        "env": "testnet",
        **(environment or {}),
    }

    append_jsonl(output / "snapshot.jsonl", {"event": "snapshot", "payload": snapshot})
    validation = validate_hermes_decision(decision_payload)
    append_jsonl(output / "hermes_decision.jsonl", {"event": "hermes_decision", **validation.to_dict()})

    if not validation.valid:
        risk_result = {
            "approved": False,
            "reason": "invalid_hermes_decision",
            "blocked_by": validation.reasons,
            "sanitized_action": validation.decision,
            "risk_notes": [],
            "max_allowed_size": 0.0,
            "required_protective_orders": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    else:
        risk = evaluate_risk(
            validation.decision or {},
            snapshot,
            environment=environment,
            config=risk_config,
            log_path=output / "risk_governor_result.jsonl",
        )
        risk_result = risk.to_dict()
        if risk.approved:
            append_jsonl(output / "risk_governor_result.jsonl", {"event": "risk_approved", **risk_result})

    execution_intent = _build_execution_intent(validation.decision, risk_result, dry_run=dry_run)
    append_jsonl(output / "execution_intent.jsonl", {"event": "execution_intent", **execution_intent})

    execution_result = _execution_result_placeholder(execution_intent, risk_result)
    append_jsonl(output / "execution_result.jsonl", {"event": "execution_result", **execution_result})

    report_type = "PRE_ENTER" if risk_result.get("approved") and execution_intent.get("action") in {"ENTER_LONG", "ENTER_SHORT"} else "RISK_REJECT"
    report_payload = {
        **(validation.decision or {}),
        "blocked_by": risk_result.get("blocked_by"),
        "reason": risk_result.get("reason"),
    }
    review = build_review_report(report_type, report_payload)
    append_review_log(output / "review_report.jsonl", review)

    return {
        "snapshot": snapshot,
        "hermes_validation": validation.to_dict(),
        "risk_governor_result": risk_result,
        "execution_intent": execution_intent,
        "execution_result": execution_result,
        "review_report": review.to_dict(),
        "output_dir": str(output),
    }


def load_json_file(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Hermes Trader Mode MVP pipeline without bypassing risk controls.")
    parser.add_argument("--snapshot-file", type=Path)
    parser.add_argument("--decision-file", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("hermes_trader_mode_logs"))
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--testnet", action="store_true", help="Use testnet environment metadata, still no mainnet.")
    args = parser.parse_args(argv)

    snapshot = load_json_file(args.snapshot_file) if args.snapshot_file else None
    decision = load_json_file(args.decision_file) if args.decision_file else None
    environment = {"runtime_mode": "TESTNET_LIVE" if args.testnet else "DRY_RUN", "env": "testnet"}
    result = run_trader_cycle(
        snapshot_payload=snapshot,
        decision_payload=decision,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
        environment=environment,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _mock_no_trade_decision() -> dict[str, Any]:
    return {
        "action": "NO_TRADE",
        "symbol": None,
        "trade_type": "NONE",
        "confidence": 1.0,
        "reason": "mock Hermes provider returned no actionable setup",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": "HERMES",
    }


def _build_execution_intent(decision: dict[str, Any] | None, risk_result: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    decision = decision or {}
    approved = bool(risk_result.get("approved", False))
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "approved_for_execution": approved,
        "dry_run": dry_run,
        "testnet_only": True,
        "action": decision.get("action"),
        "symbol": decision.get("symbol"),
        "side": _side_for_action(decision.get("action")),
        "reduce_only": bool(decision.get("reduce_only", False)),
        "entry_price_hint": decision.get("entry_price_hint"),
        "stop_loss_pct": decision.get("stop_loss_pct"),
        "stop_loss_price": decision.get("stop_loss_price"),
        "take_profit_pct": decision.get("take_profit_pct"),
        "take_profit_price": decision.get("take_profit_price"),
        "max_holding_time_sec": decision.get("max_holding_time_sec"),
        "required_protective_orders": risk_result.get("required_protective_orders") or [],
        "reason_if_not_approved": None if approved else risk_result.get("reason"),
    }


def _execution_result_placeholder(execution_intent: dict[str, Any], risk_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "order_submitted": False,
        "mainnet_order_submitted": False,
        "dry_run": bool(execution_intent.get("dry_run", True)),
        "status": "dry_run_intent_only" if risk_result.get("approved") else "blocked_before_execution",
        "reason": risk_result.get("reason"),
    }


def _side_for_action(action: Any) -> str | None:
    text = str(action or "").upper()
    if text == "ENTER_LONG":
        return "BUY"
    if text == "ENTER_SHORT":
        return "SELL"
    return None


if __name__ == "__main__":
    raise SystemExit(main())
