from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import aiohttp

from phoenix.binance_futures import BinanceFuturesClient
from phoenix.config import load_proxy_settings, resolve_environment
from phoenix.exchange_filter_feasibility import enrich_snapshot_with_exchange_filters
from phoenix.hermes_file_bridge import HermesFileBridge, attach_trace_id, snapshot_metadata
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
from phoenix.trader_snapshot_runtime import (
    DEFAULT_DASHBOARD_SNAPSHOT_URL,
    build_dashboard_api_trader_snapshot,
    build_runtime_trader_snapshot,
    fetch_signed_testnet_account_state,
)

SnapshotBuilder = Callable[[], dict[str, Any]]
STAGE2_MICRO_PROFILES = {"stage2_micro", "stage2_micro_order"}
MAX_STAGE2_QUOTE_ALLOCATION_USDT = 10.0


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
    file_bridge: HermesFileBridge | None = None,
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
            file_bridge=file_bridge,
        )
    )


async def _enrich_stage2_snapshot_exchange_filters(
    snapshot: dict[str, Any],
    *,
    quote_allocation_usdt: Any,
    leverage: Any,
    max_quote_allocation_usdt: Any,
    environment_name: str = "testnet",
) -> dict[str, Any]:
    exchange_info: dict[str, Any] | None = None
    error: str | None = None
    try:
        timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            client = BinanceFuturesClient(
                session=session,
                environment=resolve_environment(environment_name),
                credentials=None,
                proxy_settings=load_proxy_settings(),
            )
            exchange_info = await client.exchange_info()
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    enriched = enrich_snapshot_with_exchange_filters(
        snapshot,
        exchange_info,
        configured_quote_allocation_usdt=quote_allocation_usdt,
        configured_leverage=leverage,
        max_quote_allocation_usdt=max_quote_allocation_usdt,
    )
    if error:
        status = dict(enriched.get("system_status") or {})
        status["exchange_filter_error"] = error
        enriched["system_status"] = status
    return enriched


def _risk_max_quote_allocation(risk_config: RiskGovernorConfig | None) -> float:
    if risk_config is not None and risk_config.max_quote_allocation_usdt is not None:
        return float(risk_config.max_quote_allocation_usdt)
    return MAX_STAGE2_QUOTE_ALLOCATION_USDT


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
    file_bridge: HermesFileBridge | None = None,
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
    attach_trace_id(snapshot, trace)
    snapshot_path: Path | None = None
    if file_bridge is not None:
        snapshot_path = file_bridge.write_snapshot(trace, snapshot)

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
    _append_event(output, "snapshot", trace, _snapshot_event_payload(snapshot, snapshot_path=snapshot_path))
    provider_payload = provider_result.to_dict()
    _append_event(output, "hermes_decision_raw", trace, provider_payload)
    _append_event(
        output,
        "hermes_decision_normalized",
        trace,
        {
            "normalized_decision": provider_payload.get("normalized_decision"),
            "decision_origin": provider_payload.get("decision_origin"),
            "fallback_used": provider_payload.get("fallback_used"),
            "contract_warnings": provider_payload.get("contract_warnings"),
            "contract_errors": provider_payload.get("contract_errors"),
        },
    )
    if provider_payload.get("fallback_used"):
        _append_event(
            output,
            "hermes_decision_fallback",
            trace,
            {
                "fallback_decision": provider_payload.get("fallback_decision"),
                "fallback_reason": provider_payload.get("fallback_reason"),
                "decision_origin": provider_payload.get("decision_origin"),
            },
        )
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
    archive_paths: dict[str, str | None] = {}
    if file_bridge is not None:
        archive_paths = file_bridge.archive_trace(trace)
    _append_event(output, "hermes_decision_validated", trace, validation_result)
    _append_event(output, "risk_governor_result", trace, risk_result)
    _append_event(output, "safe_order_gateway_result", trace, gateway.to_dict())
    _append_event(output, "execution_intent", trace, execution_intent)
    _append_event(output, "execution_result", trace, execution_result)
    _append_event(output, "review_report", trace, review.to_dict())
    cycle_state = _cycle_state_from_gateway(gateway.to_dict(), execution_result)

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
        "snapshot_path": str(snapshot_path) if snapshot_path else None,
        "bridge_archive": archive_paths,
        **cycle_state,
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
    file_bridge: HermesFileBridge | None = None,
    dashboard_snapshot_url: str | None = None,
    dashboard_bearer_token: str | None = None,
    dashboard_timeout_sec: float = 5.0,
    lifecycle_monitor_enabled: bool = False,
    lifecycle_poll_sec: float = 5.0,
    lifecycle_post_close_wait_sec: float = 60.0,
) -> dict[str, Any]:
    normalized_mode = str(mode or "dry-run").strip().lower()
    if normalized_mode not in {"dry-run", "testnet"}:
        raise ValueError("mode must be dry-run or testnet")
    if normalized_mode == "testnet":
        micro_errors = validate_micro_notional(quote_allocation_usdt)
        if micro_errors:
            raise ValueError("stage2_micro_notional_invalid:" + ",".join(micro_errors))
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
            capabilities = verify_testnet_executor_capabilities()
            if dashboard_snapshot_url:
                snapshot = build_dashboard_api_trader_snapshot(
                    root=root,
                    dashboard_snapshot_url=dashboard_snapshot_url,
                    dashboard_bearer_token=dashboard_bearer_token,
                    dashboard_timeout_sec=dashboard_timeout_sec,
                    max_candidates=10,
                    max_open_positions=max_open_positions,
                    protective_stop_path_available=bool(capabilities.get("protective_stop_path_available", False)),
                    take_profit_path_available=bool(capabilities.get("take_profit_path_available", False)),
                    emergency_close_available=bool(capabilities.get("emergency_close_available", False)),
                    protective_stop_capability_source=str(capabilities.get("protective_stop_capability_source") or "unverified"),
                    take_profit_capability_source=str(capabilities.get("take_profit_capability_source") or "unverified"),
                    emergency_close_capability_source=str(capabilities.get("emergency_close_capability_source") or "unverified"),
                )
            else:
                snapshot = build_runtime_trader_snapshot(
                    root=root,
                    max_candidates=10,
                    max_open_positions=max_open_positions,
                    signed_account_state=signed_state.get("account_state") if isinstance(signed_state, dict) else None,
                    signed_positions=signed_state.get("positions") if isinstance(signed_state, dict) else None,
                    protective_stop_path_available=bool(capabilities.get("protective_stop_path_available", False)),
                    take_profit_path_available=bool(capabilities.get("take_profit_path_available", False)),
                    emergency_close_available=bool(capabilities.get("emergency_close_available", False)),
                    protective_stop_capability_source=str(capabilities.get("protective_stop_capability_source") or "unverified"),
                    take_profit_capability_source=str(capabilities.get("take_profit_capability_source") or "unverified"),
                    emergency_close_capability_source=str(capabilities.get("emergency_close_capability_source") or "unverified"),
                )
            if normalized_mode == "testnet":
                snapshot = await _enrich_stage2_snapshot_exchange_filters(
                    snapshot,
                    quote_allocation_usdt=quote_allocation_usdt,
                    leverage=leverage,
                    max_quote_allocation_usdt=_risk_max_quote_allocation(risk_config),
                )
            callback = None
            if normalized_mode == "testnet":
                callback = TestnetExecutorCallback(
                    snapshot=snapshot,
                    output_dir=run_dir,
                    environment_name="testnet",
                    quote_allocation_usdt=quote_allocation_usdt,
                    leverage=leverage,
                    lifecycle_monitor_enabled=lifecycle_monitor_enabled,
                    lifecycle_poll_sec=lifecycle_poll_sec,
                    lifecycle_post_close_wait_sec=lifecycle_post_close_wait_sec,
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
                    "quote_allocation_usdt": quote_allocation_usdt,
                    "stage2_micro_order": normalized_mode == "testnet",
                },
                risk_config=risk_config,
                executor_callback=callback,
                file_bridge=file_bridge,
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


async def run_bridge_smoke_test_async(
    *,
    provider: HermesDecisionProvider,
    bridge: HermesFileBridge,
    output_dir: str | Path,
    root: str | Path = ".",
    max_open_positions: int = 1,
    dashboard_snapshot_url: str = DEFAULT_DASHBOARD_SNAPSHOT_URL,
    dashboard_bearer_token: str | None = None,
    dashboard_timeout_sec: float = 5.0,
    decision_timeout_sec: float = 0.5,
) -> dict[str, Any]:
    del provider
    run_dir = Path(output_dir) / f"hermes_file_bridge_smoke_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    bridge.ensure_dirs()
    capabilities = verify_testnet_executor_capabilities()

    async def _cycle(name: str, decision: dict[str, Any] | None) -> dict[str, Any]:
        trace = f"bridge_smoke_{name}_{uuid4().hex[:8]}"
        if decision is not None:
            bridge.decision_path(trace).write_text(json.dumps(decision, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            bridge.log_event("decision_seeded_for_smoke", trace, {"decision_path": str(bridge.decision_path(trace)), "action": decision.get("action")})
        snapshot = build_dashboard_api_trader_snapshot(
            root=root,
            dashboard_snapshot_url=dashboard_snapshot_url,
            dashboard_bearer_token=dashboard_bearer_token,
            dashboard_timeout_sec=dashboard_timeout_sec,
            max_open_positions=max_open_positions,
            protective_stop_path_available=bool(capabilities.get("protective_stop_path_available", False)),
            take_profit_path_available=bool(capabilities.get("take_profit_path_available", False)),
            emergency_close_available=bool(capabilities.get("emergency_close_available", False)),
            protective_stop_capability_source=str(capabilities.get("protective_stop_capability_source") or "unverified"),
            take_profit_capability_source=str(capabilities.get("take_profit_capability_source") or "unverified"),
            emergency_close_capability_source=str(capabilities.get("emergency_close_capability_source") or "unverified"),
        )
        file_provider = provider_from_name(
            "file",
            decision_file=bridge.outbox_dir,
            decision_timeout_sec=decision_timeout_sec,
            decision_poll_interval_sec=0.1,
        )
        result = await run_trader_cycle_async(
            snapshot_builder=lambda snapshot=snapshot: snapshot,
            decision_provider=file_provider,
            output_dir=run_dir,
            dry_run=True,
            environment={
                "runtime_mode": "DRY_RUN",
                "env": "testnet",
                "PHOENIX_MAINNET_LIVE_ENABLED": "false",
                "PHOENIX_LIVE_TRADING_ENABLED": "false",
                "PHOENIX_PROMOTION_ALLOWED": "false",
                "require_trusted_runtime_snapshot": False,
            },
            trace_id=trace,
            file_bridge=bridge,
        )
        return _bridge_smoke_cycle_summary(name, result)

    cycles = [
        await _cycle("01_timeout", None),
        await _cycle("02_no_trade", no_trade_decision("Hermes decided NO_TRADE in bridge smoke")),
        await _cycle(
            "03_enter_long_risk_reject",
            {
                "action": "ENTER_LONG",
                "symbol": "BTCUSDT",
                "trade_type": "QUICK_TRADE",
                "confidence": 0.8,
                "reason": "Bridge smoke ENTER_LONG should still pass Risk Governor before any executor",
                "stop_loss_pct": 0.6,
                "take_profit_pct": 1.2,
                "max_holding_time_sec": 900,
                "source": "HERMES",
            },
        ),
    ]
    summary = {
        "event": "bridge_smoke_summary",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(run_dir),
        "hermes_inbox": str(bridge.inbox_dir),
        "hermes_outbox": str(bridge.outbox_dir),
        "hermes_archive": str(bridge.archive_dir),
        "hermes_logs": str(bridge.log_dir),
        "cycles": cycles,
        "orders_submitted": sum(1 for item in cycles if item.get("order_submitted") is True),
        "mainnet_order_submitted": sum(1 for item in cycles if item.get("mainnet_order_submitted") is True),
        "can_enter_stage1_long_dry_run": all(item.get("order_submitted") is False for item in cycles)
        and all(item.get("mainnet_order_submitted") is False for item in cycles)
        and cycles[0].get("result_type") == "soft_reject"
        and cycles[1].get("result_type") == "soft_reject",
    }
    append_jsonl(run_dir / "bridge_smoke_summary.jsonl", summary)
    return summary


def load_json_file(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_trial_config(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = load_json_file(path)
    return payload if isinstance(payload, dict) else {}


def is_stage2_micro_config(config: dict[str, Any]) -> bool:
    profile = str(config.get("profile") or config.get("stage") or "").strip().lower()
    return profile in STAGE2_MICRO_PROFILES or bool(config.get("stage2_micro_order") or config.get("micro_notional"))


def validate_micro_notional(value: Any) -> list[str]:
    try:
        quote = float(value)
    except (TypeError, ValueError):
        return ["quote_allocation_usdt_missing"]
    if quote <= 0:
        return ["quote_allocation_usdt_missing"]
    if quote > MAX_STAGE2_QUOTE_ALLOCATION_USDT:
        return ["quote_allocation_usdt_exceeds_micro_limit"]
    return []


def validate_stage2_micro_config(config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if str(config.get("mode") or "").strip().lower() != "testnet":
        errors.append("mode_must_be_testnet")
    if str(config.get("provider") or "").strip().lower() != "file":
        errors.append("provider_must_be_file")
    if str(config.get("decision_policy") or "").strip().lower() not in {"calibration", "stage2_micro"}:
        errors.append("decision_policy_must_be_calibration_or_stage2_micro")
    if config.get("testnet_only") is not True:
        errors.append("testnet_only_required")
    if bool(config.get("mainnet_live", False)):
        errors.append("mainnet_live_must_be_false")
    if int(config.get("max_open_positions") or 0) != 1:
        errors.append("max_open_positions_must_be_1")
    try:
        leverage = int(config.get("leverage") or 0)
    except (TypeError, ValueError):
        leverage = 0
    if leverage < 1 or leverage > 2:
        errors.append("leverage_must_be_1_or_2")
    errors.extend(validate_micro_notional(config.get("quote_allocation_usdt")))
    for key in (
        "stop_loss_required",
        "take_profit_required",
        "max_holding_time_required",
        "invalidation_required",
        "no_averaging_down",
        "no_martingale",
        "loss_streak_lock",
        "daily_loss_limit",
        "cooldown_after_loss",
    ):
        if config.get(key) is not True:
            errors.append(f"{key}_required")
    return errors


def risk_config_from_trial_config(config: dict[str, Any]) -> RiskGovernorConfig:
    micro_required = bool(config.get("micro_notional") or config.get("stage2_micro_order"))
    return RiskGovernorConfig(
        max_open_positions=int(config.get("max_open_positions") or 1),
        max_daily_loss_pct=float(config.get("max_daily_loss_pct") or 3.0),
        require_take_profit=bool(config.get("take_profit_required") or config.get("stage2_micro_order")),
        require_max_holding_time=bool(config.get("max_holding_time_required") or config.get("stage2_micro_order")),
        require_invalidation_condition=bool(config.get("invalidation_required") or config.get("invalidation_condition_required") or config.get("stage2_micro_order")),
        require_explicit_quote_allocation=micro_required,
        require_known_market_regime=bool(config.get("require_known_market_regime") or config.get("stage2_micro_order")),
        max_quote_allocation_usdt=(float(config.get("max_quote_allocation_usdt") or MAX_STAGE2_QUOTE_ALLOCATION_USDT) if micro_required else None),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Hermes Trader Mode through Phoenix safe_order_gateway.")
    parser.add_argument("--snapshot-file", type=Path)
    parser.add_argument("--decision-file", type=Path)
    parser.add_argument("--provider", choices=["mock", "file", "http"], default="mock")
    parser.add_argument("--http-endpoint")
    parser.add_argument("--http-timeout-sec", type=float, default=10.0)
    parser.add_argument("--decision-timeout-sec", type=float, default=0.0)
    parser.add_argument("--decision-poll-interval-sec", type=float, default=0.25)
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
    parser.add_argument("--dashboard-snapshot-url", default=None)
    parser.add_argument("--dashboard-token-env", default="PHOENIX_DASHBOARD_READONLY_TOKEN")
    parser.add_argument("--dashboard-token-file", type=Path)
    parser.add_argument("--dashboard-timeout-sec", type=float, default=5.0)
    parser.add_argument("--hermes-inbox", type=Path, default=Path("/opt/phoenix-testnet/hermes_inbox"))
    parser.add_argument("--hermes-outbox", type=Path, default=Path("/opt/phoenix-testnet/hermes_outbox"))
    parser.add_argument("--hermes-archive", type=Path, default=Path("/opt/phoenix-testnet/hermes_archive"))
    parser.add_argument("--hermes-logs", type=Path, default=Path("/opt/phoenix-testnet/hermes_logs"))
    parser.add_argument("--bridge-smoke-test", action="store_true")
    args = parser.parse_args(argv)

    config = load_trial_config(args.config)
    mode = str(config.get("mode") or args.mode)
    stage2_config = is_stage2_micro_config(config)
    if stage2_config:
        stage2_errors = validate_stage2_micro_config(config)
        if stage2_errors:
            print(
                json.dumps(
                    {
                        "error": "stage2_micro_config_invalid",
                        "errors": stage2_errors,
                        "frozen": True,
                        "can_continue": False,
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 2
    risk_config = risk_config_from_trial_config(config) if config else None
    bridge = HermesFileBridge(
        inbox_dir=config.get("hermes_inbox") or args.hermes_inbox,
        outbox_dir=config.get("hermes_outbox") or args.hermes_outbox,
        archive_dir=config.get("hermes_archive") or args.hermes_archive,
        log_dir=config.get("hermes_logs") or args.hermes_logs,
    )
    bridge.ensure_dirs()
    decision_file = args.decision_file or config.get("decision_file")
    provider_name = str(config.get("provider") or args.provider)
    if provider_name == "file" and decision_file is None:
        decision_file = bridge.outbox_dir
    provider = provider_from_name(
        provider_name,
        decision_file=decision_file,
        http_endpoint=args.http_endpoint or config.get("http_endpoint"),
        http_timeout_sec=float(config.get("http_timeout_sec") or args.http_timeout_sec),
        decision_timeout_sec=float(config.get("decision_timeout_sec") or args.decision_timeout_sec),
        decision_poll_interval_sec=float(config.get("decision_poll_interval_sec") or args.decision_poll_interval_sec),
    )
    dashboard_url = config.get("dashboard_snapshot_url") or args.dashboard_snapshot_url
    dashboard_token = None
    token_env = str(config.get("dashboard_token_env") or args.dashboard_token_env or "")
    if token_env:
        import os

        dashboard_token = os.environ.get(token_env) or None
    token_file = args.dashboard_token_file or config.get("dashboard_token_file")
    if not dashboard_token and token_file:
        try:
            dashboard_token = Path(token_file).read_text(encoding="utf-8").strip() or None
        except OSError:
            dashboard_token = None
    dashboard_timeout_sec = float(config.get("dashboard_timeout_sec") or args.dashboard_timeout_sec)

    if args.bridge_smoke_test:
        result = asyncio.run(
            run_bridge_smoke_test_async(
                provider=provider,
                bridge=bridge,
                output_dir=config.get("output_dir") or args.output_dir,
                root=args.root,
                max_open_positions=int(config.get("max_open_positions") or args.max_open_positions),
                dashboard_snapshot_url=dashboard_url or DEFAULT_DASHBOARD_SNAPSHOT_URL,
                dashboard_bearer_token=dashboard_token,
                dashboard_timeout_sec=dashboard_timeout_sec,
                decision_timeout_sec=float(config.get("decision_timeout_sec") or args.decision_timeout_sec),
            )
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

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
            capabilities = verify_testnet_executor_capabilities()
            if dashboard_url:
                snapshot = build_dashboard_api_trader_snapshot(
                    root=args.root,
                    dashboard_snapshot_url=dashboard_url,
                    dashboard_bearer_token=dashboard_token,
                    dashboard_timeout_sec=dashboard_timeout_sec,
                    protective_stop_path_available=bool(capabilities.get("protective_stop_path_available", False)),
                    take_profit_path_available=bool(capabilities.get("take_profit_path_available", False)),
                    emergency_close_available=bool(capabilities.get("emergency_close_available", False)),
                    protective_stop_capability_source=str(capabilities.get("protective_stop_capability_source") or "unverified"),
                    take_profit_capability_source=str(capabilities.get("take_profit_capability_source") or "unverified"),
                    emergency_close_capability_source=str(capabilities.get("emergency_close_capability_source") or "unverified"),
                )
            else:
                snapshot = build_runtime_trader_snapshot(
                    root=args.root,
                    signed_account_state=signed_state.get("account_state") if isinstance(signed_state, dict) else None,
                    signed_positions=signed_state.get("positions") if isinstance(signed_state, dict) else None,
                    protective_stop_path_available=bool(capabilities.get("protective_stop_path_available", False)),
                    take_profit_path_available=bool(capabilities.get("take_profit_path_available", False)),
                    emergency_close_available=bool(capabilities.get("emergency_close_available", False)),
                    protective_stop_capability_source=str(capabilities.get("protective_stop_capability_source") or "unverified"),
                    take_profit_capability_source=str(capabilities.get("take_profit_capability_source") or "unverified"),
                    emergency_close_capability_source=str(capabilities.get("emergency_close_capability_source") or "unverified"),
                )
        cycle_output_dir = config.get("output_dir") or args.output_dir
        cycle_quote_allocation = args.quote_allocation if args.quote_allocation is not None else config.get("quote_allocation_usdt")
        cycle_leverage = int(config.get("leverage") or args.leverage)
        if mode == "testnet" or stage2_config:
            snapshot = asyncio.run(
                _enrich_stage2_snapshot_exchange_filters(
                    snapshot,
                    quote_allocation_usdt=cycle_quote_allocation,
                    leverage=cycle_leverage,
                    max_quote_allocation_usdt=float(config.get("max_quote_allocation_usdt") or MAX_STAGE2_QUOTE_ALLOCATION_USDT),
                )
            )
        decision_payload = load_json_file(args.decision_file) if args.decision_file and provider_name != "file" else None
        if decision_payload is not None:
            provider = MockHermesDecisionProvider(decision_payload)
        callback = None
        if mode == "testnet":
            callback = TestnetExecutorCallback(
                snapshot=snapshot,
                output_dir=cycle_output_dir,
                quote_allocation_usdt=cycle_quote_allocation,
                leverage=cycle_leverage,
                lifecycle_monitor_enabled=bool(config.get("lifecycle_monitor_enabled", False)),
                lifecycle_poll_sec=float(config.get("lifecycle_poll_sec") or 5.0),
                lifecycle_post_close_wait_sec=float(config.get("lifecycle_post_close_wait_sec") or 60.0),
            )
        result = run_trader_cycle(
            snapshot_builder=(lambda snapshot=snapshot: snapshot) if mode == "testnet" and not args.snapshot_file else None,
            snapshot_payload=None if mode == "testnet" and not args.snapshot_file else snapshot,
            decision_provider=provider,
            output_dir=cycle_output_dir,
            dry_run=mode != "testnet",
            environment={
                "runtime_mode": "DRY_RUN" if mode == "dry-run" else "TESTNET_LIVE",
                "env": "testnet",
                "PHOENIX_MAINNET_LIVE_ENABLED": "false",
                "PHOENIX_LIVE_TRADING_ENABLED": "false",
                "PHOENIX_PROMOTION_ALLOWED": "false",
                "require_trusted_runtime_snapshot": mode == "testnet",
                "quote_allocation_usdt": cycle_quote_allocation,
                "stage2_micro_order": stage2_config,
            },
            risk_config=risk_config,
            executor_callback=callback,
            file_bridge=bridge if provider_name == "file" else None,
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
                risk_config=risk_config,
                file_bridge=bridge if provider_name == "file" else None,
                dashboard_snapshot_url=dashboard_url,
                dashboard_bearer_token=dashboard_token,
                dashboard_timeout_sec=dashboard_timeout_sec,
                lifecycle_monitor_enabled=bool(config.get("lifecycle_monitor_enabled", False)),
                lifecycle_poll_sec=float(config.get("lifecycle_poll_sec") or 5.0),
                lifecycle_post_close_wait_sec=float(config.get("lifecycle_post_close_wait_sec") or 60.0),
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
        "result_type": "hard_freeze",
        "executor_called": False,
    }
    gateway_result = {
        "approved": False,
        "rejected": True,
        "blocked_by": list(dict.fromkeys(blocked_by)),
        "source": "HERMES",
        "reason": freeze_reason,
        "result_type": "hard_freeze",
        "normalized_decision": raw_decision,
        "validation_result": {"valid": False, "reasons": list(dict.fromkeys(blocked_by)), "decision": raw_decision, "rejectable": True},
        "risk_governor_result": risk_result,
        "execution_intent": execution_intent,
        "execution_result": execution_result,
        "review_report": None,
        "created_at": created_at,
    }
    review = build_review_report(
        "HARD_FREEZE",
        {"action": raw_decision.get("action"), "symbol": raw_decision.get("symbol"), "blocked_by": blocked_by, "reason": freeze_reason, "freeze_reason": freeze_reason},
    )
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
        "result_type": "hard_freeze",
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


def _cycle_state_from_gateway(gateway: dict[str, Any], execution_result: dict[str, Any]) -> dict[str, Any]:
    result_type = str(execution_result.get("result_type") or gateway.get("result_type") or "completed")
    frozen = bool(execution_result.get("frozen", result_type == "hard_freeze"))
    can_continue = bool(execution_result.get("can_continue", not frozen))
    if frozen and can_continue:
        can_continue = False
    if not frozen and not can_continue and result_type != "completed":
        can_continue = True
    freeze_reason = execution_result.get("freeze_reason") if frozen else None
    if frozen and not freeze_reason:
        freeze_reason = gateway.get("reason") or "hard_freeze"
    return {
        "result_type": result_type,
        "frozen": frozen,
        "freeze_reason": freeze_reason,
        "can_continue": can_continue,
    }


def _review_type(approved: bool, decision: dict[str, Any], execution_result: dict[str, Any]) -> str:
    action = str(decision.get("action") or "").upper()
    result_type = str(execution_result.get("result_type") or "").lower()
    if result_type == "hard_freeze":
        return "HARD_FREEZE"
    if result_type == "soft_reject":
        return "SOFT_REJECT"
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


def _snapshot_event_payload(snapshot: dict[str, Any], *, snapshot_path: Path | None) -> dict[str, Any]:
    return {
        "payload": snapshot,
        "snapshot_path": str(snapshot_path) if snapshot_path else None,
        **snapshot_metadata(snapshot),
    }


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
        "result_type": result.get("result_type") or execution.get("result_type") or gateway.get("result_type"),
        "frozen": bool(result.get("frozen", False) or execution.get("frozen", False)),
        "freeze_reason": result.get("freeze_reason") or execution.get("freeze_reason"),
        "can_continue": result.get("can_continue"),
    }


def _bridge_smoke_cycle_summary(name: str, result: dict[str, Any]) -> dict[str, Any]:
    provider = result.get("hermes_provider_result") or {}
    decision = provider.get("decision") if isinstance(provider.get("decision"), dict) else {}
    gateway = result.get("safe_order_gateway_result") or {}
    execution = result.get("execution_result") or {}
    snapshot = result.get("snapshot") if isinstance(result.get("snapshot"), dict) else {}
    return {
        "cycle": name,
        "trace_id": result.get("trace_id"),
        "snapshot_path": result.get("snapshot_path"),
        **snapshot_metadata(snapshot),
        "provider": provider.get("provider"),
        "provider_ok": provider.get("ok"),
        "fallback_used": provider.get("fallback_used"),
        "decision_origin": provider.get("decision_origin"),
        "provider_fallback_reason": provider.get("fallback_reason"),
        "action": decision.get("action"),
        "symbol": decision.get("symbol"),
        "approved": gateway.get("approved"),
        "blocked_by": gateway.get("blocked_by"),
        "reason": gateway.get("reason"),
        "result_type": result.get("result_type") or execution.get("result_type") or gateway.get("result_type"),
        "order_submitted": bool(execution.get("order_submitted", False)),
        "mainnet_order_submitted": bool(execution.get("mainnet_order_submitted", False)),
        "executor_called": bool(execution.get("executor_called", False)),
        "frozen": bool(result.get("frozen", False) or execution.get("frozen", False)),
        "freeze_reason": result.get("freeze_reason") or execution.get("freeze_reason"),
        "can_continue": result.get("can_continue"),
        "review_report": (result.get("review_report") or {}).get("text"),
    }


def _trace_id() -> str:
    return f"htm_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"


def _run_id() -> str:
    return f"hermes_trader_trial_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"


if __name__ == "__main__":
    raise SystemExit(main())
