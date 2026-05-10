from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from phoenix.hermes_decision import OPEN_ACTIONS
from phoenix.hermes_decision_provider import (
    HermesDecisionProvider,
    MockHermesDecisionProvider,
    append_provider_log,
    no_trade_decision,
    provider_from_name,
)
from phoenix.review_reporter import append_review_log, build_review_report
from phoenix.risk_governor import RiskGovernorConfig, append_jsonl
from phoenix.safe_order_gateway import ExecutorCallback, submit_order_intent
from phoenix.testnet_executor_callback import (
    TestnetExecutorCallback,
    validate_trusted_runtime_snapshot,
    verify_testnet_executor_capabilities,
)
from phoenix.trader_snapshot import build_trader_snapshot
from phoenix.trader_snapshot_runtime import build_runtime_trader_snapshot, fetch_signed_testnet_account_state

SnapshotBuilder = Callable[[], dict[str, Any]]


def run_trader_cycle(
    *,
    snapshot_payload: dict[str, Any] | None = None,
    decision_payload: dict[str, Any] | None = None,
    decision_provider: HermesDecisionProvider | None = None,
    snapshot_builder: SnapshotBuilder | None = None,
    output_dir: str | Path = "hermes_trader_mode_logs",
    dry_run: bool = True,
    environment: dict[str, Any] | None = None,
    risk_config: RiskGovernorConfig | None = None,
    executor_callback: ExecutorCallback | None = None,
    trace_id: str | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        run_trader_cycle_async(
            snapshot_payload=snapshot_payload,
            decision_payload=decision_payload,
            decision_provider=decision_provider,
            snapshot_builder=snapshot_builder,
            output_dir=output_dir,
            dry_run=dry_run,
            environment=environment,
            risk_config=risk_config,
            executor_callback=executor_callback,
            trace_id=trace_id,
        )
    )


async def run_trader_cycle_async(
    *,
    snapshot_payload: dict[str, Any] | None = None,
    decision_payload: dict[str, Any] | None = None,
    decision_provider: HermesDecisionProvider | None = None,
    snapshot_builder: SnapshotBuilder | None = None,
    output_dir: str | Path = "hermes_trader_mode_logs",
    dry_run: bool = True,
    environment: dict[str, Any] | None = None,
    risk_config: RiskGovernorConfig | None = None,
    executor_callback: ExecutorCallback | None = None,
    trace_id: str | None = None,
) -> dict[str, Any]:
    trace = trace_id or _trace_id()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    environment_payload = {
        "runtime_mode": "DRY_RUN" if dry_run else "TESTNET_LIVE",
        "env": "testnet",
        **(environment or {}),
    }
    testnet_order_mode = _is_testnet_order_mode(dry_run=dry_run, environment=environment_payload)
    snapshot_origin = "manual_payload" if snapshot_payload is not None else ("runtime_builder" if snapshot_builder is not None else "unavailable")
    snapshot = snapshot_payload or (snapshot_builder() if snapshot_builder is not None else _unavailable_snapshot())
    _annotate_snapshot_source(snapshot, snapshot_origin=snapshot_origin, testnet_order_mode=testnet_order_mode)

    if testnet_order_mode:
        trust_reasons = validate_trusted_runtime_snapshot(snapshot)
        if snapshot_origin != "runtime_builder" and "manual_snapshot_not_allowed_for_testnet_order_mode" not in trust_reasons:
            trust_reasons.insert(0, "manual_snapshot_not_allowed_for_testnet_order_mode")
        if trust_reasons:
            return _frozen_cycle_result(
                output=output,
                trace_id=trace,
                snapshot=snapshot,
                environment=environment_payload,
                freeze_reason=_prioritized_freeze_reason(trust_reasons),
                blocked_by=trust_reasons,
                raw_decision=decision_payload or no_trade_decision("testnet order mode frozen before Hermes decision"),
            )

    provider = decision_provider or MockHermesDecisionProvider(decision_payload or no_trade_decision("mock Hermes provider returned no actionable setup"))
    provider_result = await provider.decide(snapshot)
    decision = provider_result.decision
    append_provider_log(output / "hermes_decision_provider.jsonl", provider_result)
    _append_event(output, "snapshot", trace, {"payload": snapshot})
    _append_event(output, "hermes_decision_raw", trace, provider_result.to_dict())
    append_jsonl(output / "snapshot.jsonl", {"trace_id": trace, "event": "snapshot", "payload": snapshot})

    callback = executor_callback if _should_allow_executor_callback(decision, dry_run=dry_run, callback=executor_callback) else None
    gateway = await submit_order_intent(
        decision,
        snapshot,
        environment_payload,
        "HERMES",
        dry_run=dry_run,
        executor_callback=callback,
        risk_config=risk_config,
        log_dir=output,
        audit_log_path=output / "safe_order_gateway.jsonl",
        extra_context={"trace_id": trace},
    )
    validation_result = gateway.validation_result
    risk_result = gateway.risk_governor_result
    execution_intent = gateway.execution_intent
    execution_result = gateway.execution_result or _execution_result_placeholder(execution_intent, risk_result)
    normalized = gateway.normalized_decision or decision

    append_jsonl(output / "hermes_decision.jsonl", {"trace_id": trace, "event": "hermes_decision", **validation_result})
    append_jsonl(
        output / "risk_governor_result.jsonl",
        {"trace_id": trace, "event": "risk_approved" if gateway.approved else "risk_reject", **risk_result},
    )
    append_jsonl(output / "execution_intent.jsonl", {"trace_id": trace, "event": "execution_intent", **execution_intent})
    append_jsonl(output / "execution_result.jsonl", {"trace_id": trace, "event": "execution_result", **execution_result})

    review = build_review_report(_review_type(gateway.approved, normalized, execution_result), _review_payload(normalized, risk_result, snapshot))
    append_review_log(output / "review_report.jsonl", review)
    _append_event(output, "hermes_decision_validated", trace, validation_result)
    _append_event(output, "risk_governor_result", trace, risk_result)
    _append_event(output, "safe_order_gateway_result", trace, gateway.to_dict())
    _append_event(output, "execution_intent", trace, execution_intent)
    _append_event(output, "execution_result", trace, execution_result)
    _append_event(output, "review_report", trace, review.to_dict())

    return {
        "trace_id": trace,
        "snapshot": snapshot,
        "hermes_provider_result": provider_result.to_dict(),
        "hermes_validation": validation_result,
        "risk_governor_result": risk_result,
        "safe_order_gateway_result": gateway.to_dict(),
        "execution_intent": execution_intent,
        "execution_result": execution_result,
        "review_report": review.to_dict(),
        "output_dir": str(output),
        "frozen": bool(execution_result.get("frozen", False)) or (testnet_order_mode and not risk_result.get("approved", False)),
        "freeze_reason": execution_result.get("freeze_reason") or (risk_result.get("reason") if testnet_order_mode and not risk_result.get("approved", False) else None),
        "can_continue": bool(execution_result.get("can_continue", not bool(execution_result.get("frozen", False)))),
    }


async def run_trader_trial_async(
    *,
    mode: str = "dry-run",
    provider: HermesDecisionProvider | None = None,
    interval_sec: float = 5.0,
    duration_min: float = 60.0,
    output_dir: str | Path = "hermes_trader_mode_logs",
    root: str | Path = ".",
    max_open_positions: int = 1,
    leverage: int = 2,
    quote_allocation_usdt: float | None = None,
    risk_config: RiskGovernorConfig | None = None,
) -> dict[str, Any]:
    normalized_mode = str(mode or "dry-run").strip().lower()
    if normalized_mode not in {"dry-run", "testnet"}:
        raise ValueError("mode must be dry-run or testnet")
    run_id = _run_id()
    run_dir = Path(output_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    deadline = started + max(0.0, float(duration_min)) * 60.0
    cycles: list[dict[str, Any]] = []
    selected_provider = provider or MockHermesDecisionProvider()
    try:
        while True:
            signed_state = (
                await fetch_signed_testnet_account_state(environment_name="testnet")
                if normalized_mode == "testnet"
                else {"account_state": None, "positions": None}
            )
            capabilities = verify_testnet_executor_capabilities() if normalized_mode == "testnet" else {}
            snapshot = build_runtime_trader_snapshot(
                root=root,
                max_candidates=10,
                max_open_positions=max_open_positions,
                signed_account_state=signed_state.get("account_state") if isinstance(signed_state, dict) else None,
                signed_positions=signed_state.get("positions") if isinstance(signed_state, dict) else None,
                protective_stop_path_available=bool(capabilities.get("protective_stop_path_available", False)),
                emergency_close_available=bool(capabilities.get("emergency_close_available", False)),
                protective_stop_capability_source=str(capabilities.get("protective_stop_capability_source") or "unverified"),
                emergency_close_capability_source=str(capabilities.get("emergency_close_capability_source") or "unverified"),
            )
            callback = None
            if normalized_mode == "testnet":
                callback = TestnetExecutorCallback(
                    snapshot=snapshot,
                    output_dir=run_dir,
                    environment_name="testnet",
                    quote_allocation_usdt=quote_allocation_usdt,
                    leverage=leverage,
                )
            result = await run_trader_cycle_async(
                snapshot_builder=lambda snapshot=snapshot: snapshot,
                decision_provider=selected_provider,
                output_dir=run_dir,
                dry_run=normalized_mode != "testnet",
                environment={
                    "runtime_mode": "DRY_RUN" if normalized_mode == "dry-run" else "TESTNET_LIVE",
                    "env": "testnet",
                    "PHOENIX_MAINNET_LIVE_ENABLED": "false",
                    "PHOENIX_LIVE_TRADING_ENABLED": "false",
                    "PHOENIX_PROMOTION_ALLOWED": "false",
                    "require_trusted_runtime_snapshot": normalized_mode == "testnet",
                },
                risk_config=risk_config,
                executor_callback=callback,
            )
            cycles.append(_cycle_summary(result))
            if result.get("frozen") and result.get("can_continue") is False:
                break
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(max(1.0, float(interval_sec)))
    except KeyboardInterrupt:
        _append_event(run_dir, "trial_shutdown", _trace_id(), {"reason": "keyboard_interrupt"})
    summary = {
        "run_id": run_id,
        "mode": normalized_mode,
        "provider": getattr(selected_provider, "name", "unknown"),
        "cycles": len(cycles),
        "cycle_summaries": cycles,
        "output_dir": str(run_dir),
        "mainnet_live": False,
        "auto_promotion": False,
    }
    append_jsonl(run_dir / "trial_summary.jsonl", {"event": "trial_summary", **summary})
    return summary


def load_json_file(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_trial_config(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = load_json_file(path)
    return payload if isinstance(payload, dict) else {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Hermes Trader Mode through Phoenix safe_order_gateway.")
    parser.add_argument("--snapshot-file", type=Path)
    parser.add_argument("--decision-file", type=Path)
    parser.add_argument("--provider", choices=["mock", "file", "http"], default="mock")
    parser.add_argument("--http-endpoint")
    parser.add_argument("--http-timeout-sec", type=float, default=10.0)
    parser.add_argument("--mode", choices=["dry-run", "testnet"], default="dry-run")
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-min", type=float, default=0.0)
    parser.add_argument("--max-open-positions", type=int, default=1)
    parser.add_argument("--leverage", type=int, default=2)
    parser.add_argument("--quote-allocation", type=float)
    parser.add_argument("--output-dir", type=Path, default=Path("hermes_trader_mode_logs"))
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path)
    parser.add_argument("--single-cycle", action="store_true")
    args = parser.parse_args(argv)

    config = load_trial_config(args.config)
    mode = str(config.get("mode") or args.mode)
    provider = provider_from_name(
        str(config.get("provider") or args.provider),
        decision_file=args.decision_file or config.get("decision_file"),
        http_endpoint=args.http_endpoint or config.get("http_endpoint"),
        http_timeout_sec=float(config.get("http_timeout_sec") or args.http_timeout_sec),
    )
    if args.single_cycle or args.snapshot_file or args.decision_file:
        if mode == "testnet" and args.snapshot_file:
            print(
                json.dumps(
                    {
                        "error": "manual snapshot not allowed for testnet order mode",
                        "freeze_reason": "manual_snapshot_not_allowed_for_testnet_order_mode",
                        "frozen": True,
                        "can_continue": False,
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 2
        if args.snapshot_file:
            snapshot = load_json_file(args.snapshot_file)
        else:
            signed_state = (
                asyncio.run(fetch_signed_testnet_account_state(environment_name="testnet"))
                if mode == "testnet"
                else {"account_state": None, "positions": None}
            )
            capabilities = verify_testnet_executor_capabilities() if mode == "testnet" else {}
            snapshot = build_runtime_trader_snapshot(
                root=args.root,
                signed_account_state=signed_state.get("account_state") if isinstance(signed_state, dict) else None,
                signed_positions=signed_state.get("positions") if isinstance(signed_state, dict) else None,
                protective_stop_path_available=bool(capabilities.get("protective_stop_path_available", False)),
                emergency_close_available=bool(capabilities.get("emergency_close_available", False)),
                protective_stop_capability_source=str(capabilities.get("protective_stop_capability_source") or "unverified"),
                emergency_close_capability_source=str(capabilities.get("emergency_close_capability_source") or "unverified"),
            )
        decision_payload = load_json_file(args.decision_file) if args.decision_file and args.provider != "file" else None
        if decision_payload is not None:
            provider = MockHermesDecisionProvider(decision_payload)
        callback = None
        if mode == "testnet":
            callback = TestnetExecutorCallback(
                snapshot=snapshot,
                output_dir=args.output_dir,
                quote_allocation_usdt=args.quote_allocation,
                leverage=args.leverage,
            )
        result = run_trader_cycle(
            snapshot_builder=(lambda snapshot=snapshot: snapshot) if mode == "testnet" and not args.snapshot_file else None,
            snapshot_payload=None if mode == "testnet" and not args.snapshot_file else snapshot,
            decision_provider=provider,
            output_dir=args.output_dir,
            dry_run=mode != "testnet",
            environment={
                "runtime_mode": "DRY_RUN" if mode == "dry-run" else "TESTNET_LIVE",
                "env": "testnet",
                "PHOENIX_MAINNET_LIVE_ENABLED": "false",
                "PHOENIX_LIVE_TRADING_ENABLED": "false",
                "PHOENIX_PROMOTION_ALLOWED": "false",
                "require_trusted_runtime_snapshot": mode == "testnet",
            },
            executor_callback=callback,
        )
    else:
        result = asyncio.run(
            run_trader_trial_async(
                mode=mode,
                provider=provider,
                interval_sec=float(config.get("interval_sec") or args.interval_sec),
                duration_min=float(config.get("duration_min") or args.duration_min),
                output_dir=config.get("output_dir") or args.output_dir,
                root=args.root,
                max_open_positions=int(config.get("max_open_positions") or args.max_open_positions),
                leverage=int(config.get("leverage") or args.leverage),
                quote_allocation_usdt=args.quote_allocation or config.get("quote_allocation_usdt"),
            )
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _unavailable_snapshot() -> dict[str, Any]:
    return build_trader_snapshot(
        system_status={
            "data_fresh": False,
            "exchange_status": "unavailable",
            "websocket_status": "unavailable",
            "source": "unavailable",
            "snapshot_source": "unavailable",
            "trusted_runtime_snapshot": False,
            "position_state": "unknown",
            "stop_protection_status": "unknown",
            "candidate_state": "unknown",
            "protective_stop_path_available": False,
            "emergency_close_available": False,
            "protective_stop_capability_source": "unverified",
            "emergency_close_capability_source": "unverified",
        }
    )


def _execution_result_placeholder(execution_intent: dict[str, Any], risk_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "order_submitted": False,
        "mainnet_order_submitted": False,
        "dry_run": bool(execution_intent.get("dry_run", True)),
        "status": "dry_run_intent_only" if risk_result.get("approved") else "blocked_before_execution",
        "reason": risk_result.get("reason"),
    }


def _is_testnet_order_mode(*, dry_run: bool, environment: dict[str, Any]) -> bool:
    runtime_mode = str(environment.get("runtime_mode") or environment.get("PHOENIX_RUNTIME_MODE") or "").upper()
    return (not dry_run) and runtime_mode in {"TESTNET", "TESTNET_LIVE"}


def _annotate_snapshot_source(snapshot: dict[str, Any], *, snapshot_origin: str, testnet_order_mode: bool) -> None:
    status = snapshot.setdefault("system_status", {})
    if not isinstance(status, dict):
        snapshot["system_status"] = status = {}
    if "snapshot_source" not in status:
        if snapshot_origin == "manual_payload":
            status["snapshot_source"] = "manual_payload"
        elif snapshot_origin == "runtime_builder":
            status["snapshot_source"] = status.get("source") or "runtime"
        else:
            status["snapshot_source"] = snapshot_origin
    if snapshot_origin == "manual_payload":
        status["source"] = status.get("source") or "manual_payload"
        status["trusted_runtime_snapshot"] = False
    elif testnet_order_mode:
        status.setdefault("trusted_runtime_snapshot", False)


def _prioritized_freeze_reason(reasons: list[str]) -> str:
    ordered = [
        "manual_snapshot_not_allowed_for_testnet_order_mode",
        "protective_stop_path_unavailable",
        "protective_stop_path_unverified",
        "emergency_close_unavailable",
        "emergency_close_unverified",
        "missing_account_state",
        "missing_position_state",
        "position_state_unknown",
        "exchange_status_unknown",
        "websocket_status_unhealthy",
        "stale_snapshot",
        "untrusted_runtime_snapshot",
    ]
    reason_set = set(reasons)
    for reason in ordered:
        if reason in reason_set:
            return reason
    return reasons[0] if reasons else "testnet_order_mode_safety_freeze"


def _frozen_cycle_result(
    *,
    output: Path,
    trace_id: str,
    snapshot: dict[str, Any],
    environment: dict[str, Any],
    freeze_reason: str,
    blocked_by: list[str],
    raw_decision: dict[str, Any],
) -> dict[str, Any]:
    created_at = datetime.now(timezone.utc).isoformat()
    risk_result = {
        "approved": False,
        "reason": freeze_reason,
        "blocked_by": list(dict.fromkeys(blocked_by)),
        "sanitized_action": raw_decision,
        "risk_notes": ["testnet_order_mode_fail_closed"],
        "max_allowed_size": 0.0,
        "required_protective_orders": [],
        "created_at": created_at,
    }
    execution_intent = {
        "created_at": created_at,
        "source": "HERMES",
        "dry_run": False,
        "testnet_only": True,
        "action": raw_decision.get("action"),
        "symbol": raw_decision.get("symbol"),
        "side": None,
        "reduce_only": False,
        "trace_id": trace_id,
        "required_protective_orders": [],
        "approved_for_execution": False,
        "reason_if_not_approved": freeze_reason,
        "raw_intent": raw_decision,
    }
    execution_result = {
        "created_at": created_at,
        "order_submitted": False,
        "mainnet_order_submitted": False,
        "dry_run": False,
        "status": "frozen_before_testnet_order",
        "reason": freeze_reason,
        "frozen": True,
        "freeze_reason": freeze_reason,
        "can_continue": False,
        "executor_called": False,
    }
    gateway_result = {
        "approved": False,
        "rejected": True,
        "blocked_by": list(dict.fromkeys(blocked_by)),
        "source": "HERMES",
        "reason": freeze_reason,
        "normalized_decision": raw_decision,
        "validation_result": {"valid": False, "reasons": list(dict.fromkeys(blocked_by)), "decision": raw_decision, "rejectable": True},
        "risk_governor_result": risk_result,
        "execution_intent": execution_intent,
        "execution_result": execution_result,
        "review_report": None,
        "created_at": created_at,
    }
    review = build_review_report("RISK_REJECT", {"action": raw_decision.get("action"), "symbol": raw_decision.get("symbol"), "blocked_by": blocked_by, "reason": freeze_reason})
    _append_event(output, "snapshot", trace_id, {"payload": snapshot})
    _append_event(output, "hermes_decision_raw", trace_id, {"decision": raw_decision, "fallback_reason": freeze_reason})
    _append_event(output, "hermes_decision_validated", trace_id, gateway_result["validation_result"])
    _append_event(output, "risk_governor_result", trace_id, risk_result)
    _append_event(output, "safe_order_gateway_result", trace_id, gateway_result)
    _append_event(output, "execution_intent", trace_id, execution_intent)
    _append_event(output, "execution_result", trace_id, execution_result)
    _append_event(output, "review_report", trace_id, review.to_dict())
    append_jsonl(output / "snapshot.jsonl", {"trace_id": trace_id, "event": "snapshot", "payload": snapshot})
    append_jsonl(output / "risk_governor_result.jsonl", {"trace_id": trace_id, "event": "risk_reject", **risk_result})
    append_jsonl(output / "execution_intent.jsonl", {"trace_id": trace_id, "event": "execution_intent", **execution_intent})
    append_jsonl(output / "execution_result.jsonl", {"trace_id": trace_id, "event": "execution_result", **execution_result})
    append_review_log(output / "review_report.jsonl", review)
    return {
        "trace_id": trace_id,
        "snapshot": snapshot,
        "hermes_provider_result": {"ok": False, "provider": "safety_freeze", "decision": raw_decision, "fallback_reason": freeze_reason, "raw_response": raw_decision, "created_at": created_at},
        "hermes_validation": gateway_result["validation_result"],
        "risk_governor_result": risk_result,
        "safe_order_gateway_result": gateway_result,
        "execution_intent": execution_intent,
        "execution_result": execution_result,
        "review_report": review.to_dict(),
        "output_dir": str(output),
        "environment": environment,
        "frozen": True,
        "freeze_reason": freeze_reason,
        "can_continue": False,
    }


def _should_allow_executor_callback(
    decision: dict[str, Any],
    *,
    dry_run: bool,
    callback: ExecutorCallback | None,
) -> bool:
    if dry_run or callback is None:
        return False
    return str(decision.get("action") or "").upper() in OPEN_ACTIONS


def _review_type(approved: bool, decision: dict[str, Any], execution_result: dict[str, Any]) -> str:
    action = str(decision.get("action") or "").upper()
    if action == "NO_TRADE":
        return "NO_TRADE"
    if not approved:
        return "RISK_REJECT"
    if action in OPEN_ACTIONS and execution_result.get("order_submitted"):
        return "OPENED"
    if action in OPEN_ACTIONS:
        return "PRE_ENTER"
    return "POSITION_UPDATE"


def _review_payload(decision: dict[str, Any], risk_result: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    market = snapshot.get("market_regime") or {}
    return {
        **decision,
        "blocked_by": risk_result.get("blocked_by"),
        "reason": decision.get("reason") or risk_result.get("reason"),
        "regime": market.get("regime"),
        "market_regime": market.get("regime"),
        "risk_status": risk_result.get("reason"),
    }


def _append_event(output: Path, event: str, trace_id: str, payload: dict[str, Any]) -> None:
    append_jsonl(
        output / "replay_events.jsonl",
        {
            "event": event,
            "trace_id": trace_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **payload,
        },
    )


def _cycle_summary(result: dict[str, Any]) -> dict[str, Any]:
    gateway = result.get("safe_order_gateway_result") or {}
    execution = result.get("execution_result") or {}
    provider = result.get("hermes_provider_result") or {}
    decision = provider.get("decision") if isinstance(provider.get("decision"), dict) else {}
    return {
        "trace_id": result.get("trace_id"),
        "action": decision.get("action"),
        "symbol": decision.get("symbol"),
        "approved": gateway.get("approved"),
        "blocked_by": gateway.get("blocked_by"),
        "order_submitted": execution.get("order_submitted"),
        "execution_status": execution.get("status"),
        "frozen": bool(result.get("frozen", False) or execution.get("frozen", False)),
        "freeze_reason": result.get("freeze_reason") or execution.get("freeze_reason"),
        "can_continue": result.get("can_continue"),
    }


def _trace_id() -> str:
    return f"htm_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"


def _run_id() -> str:
    return f"hermes_trader_trial_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"


if __name__ == "__main__":
    raise SystemExit(main())
