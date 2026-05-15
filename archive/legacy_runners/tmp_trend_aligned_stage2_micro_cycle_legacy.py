from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

from phoenix.binance_futures import BinanceFuturesClient
from phoenix.config import load_credentials, load_proxy_settings, resolve_environment
from phoenix.direction_regime_matrix import direction_regime_fields, evaluate_direction_regime
from phoenix.hermes_decision_provider import provider_from_name
from phoenix.hermes_file_bridge import HermesFileBridge
from phoenix.hermes_trader_mode import (
    _enrich_stage2_snapshot_exchange_filters,
    risk_config_from_trial_config,
    run_trader_cycle_async,
    validate_stage2_micro_config,
)
from phoenix.testnet_executor_callback import TestnetExecutorCallback, verify_testnet_executor_capabilities
from phoenix.trader_snapshot_runtime import build_dashboard_api_trader_snapshot


ROOT = Path("/opt/phoenix-testnet")
CONFIG_PATH = ROOT / "configs/hermes_trader_stage2_micro.testnet.json"
BASE_OUTPUT = ROOT / "hermes_logs/stage2_trend_aligned_single_micro"
STAGE2_PATTERNS = ("stage2_micro", "stage2_preflight", "phoenix.hermes_trader_mode")


def _now_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _disk_memory_status() -> dict[str, Any]:
    usage = shutil.disk_usage(ROOT)
    meminfo: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            key, value = line.split(":", 1)
            meminfo[key] = int(value.strip().split()[0])
    except Exception:
        pass
    return {
        "disk": {
            "path": str(ROOT),
            "total_gb": round(usage.total / 1024**3, 3),
            "used_gb": round(usage.used / 1024**3, 3),
            "free_gb": round(usage.free / 1024**3, 3),
            "used_pct": round((usage.used / usage.total) * 100.0, 3) if usage.total else None,
        },
        "memory": {
            "mem_total_mb": round(meminfo.get("MemTotal", 0) / 1024, 3),
            "mem_available_mb": round(meminfo.get("MemAvailable", 0) / 1024, 3),
        },
        "swap": {
            "swap_total_mb": round(meminfo.get("SwapTotal", 0) / 1024, 3),
            "swap_free_mb": round(meminfo.get("SwapFree", 0) / 1024, 3),
        },
    }


def _service_state(name: str) -> str:
    try:
        proc = subprocess.run(
            ["systemctl", "is-active", name],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
        )
    except Exception as exc:  # noqa: BLE001
        return f"error:{exc}"
    return proc.stdout.strip() or proc.stderr.strip() or "unknown"


def _recent_oom_events() -> list[str]:
    patterns = ("out of memory", "oom-kill", "oom_reaper", "killed process")
    events: list[str] = []
    for command in (
        ["journalctl", "-k", "--since", "-24 hours", "--no-pager"],
        ["journalctl", "--since", "-24 hours", "--no-pager"],
    ):
        try:
            proc = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        except Exception:
            continue
        for line in proc.stdout.splitlines():
            lower = line.lower()
            if any(pattern in lower for pattern in patterns):
                events.append(line[:1000])
        if events:
            break
    return events[:20]


def _stage2_residual_processes() -> list[dict[str, Any]]:
    ignore_pids = {os.getpid(), os.getppid()}
    ignore_tokens = (
        ".tmp_cycle2_gate3_testnet_runner.py",
        ".tmp_authorized_stage2_micro_cycle_next.py",
        ".tmp_trend_aligned_stage2_micro_cycle.py",
        "cycle2_gate3_testnet_runner",
        "trend_aligned_stage2_micro_cycle",
        "_stage2_residual_processes",
    )
    rows: list[dict[str, Any]] = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit() or int(pid) in ignore_pids:
            continue
        try:
            cmdline = (
                Path(f"/proc/{pid}/cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode("utf-8", errors="replace")
                .strip()
            )
        except Exception:
            continue
        if not cmdline or any(token in cmdline for token in ignore_tokens):
            continue
        if any(pattern in cmdline for pattern in STAGE2_PATTERNS):
            rows.append({"pid": int(pid), "cmdline": cmdline[:500]})
    return rows


def _open_position_rows(rows: Any) -> list[dict[str, Any]]:
    open_rows: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        try:
            amount = abs(float(row.get("positionAmt") or row.get("position_amount") or 0.0))
        except (TypeError, ValueError):
            amount = 0.0
        if amount > 0:
            open_rows.append(row)
    return open_rows


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _direction_regime_payload(candidate: dict[str, Any], regime: dict[str, Any]) -> dict[str, Any]:
    if not candidate:
        return {
            "market_regime": str(regime.get("regime") or "UNKNOWN").upper(),
            "candidate_direction": None,
            "allowed_direction": "NONE",
            "direction_regime_allowed": False,
            "direction_regime_reason": "selected candidate missing; Stage 2 must fail closed.",
            "blocked_by": ["selected_candidate_missing", "direction_regime_mismatch"],
        }
    result = evaluate_direction_regime(
        candidate_direction=candidate.get("bias") or candidate.get("side") or candidate.get("direction"),
        market_regime=regime,
        candidate=candidate,
    )
    return {**direction_regime_fields(result), "blocked_by": result.blocked_by}


async def _fetch_testnet_state(symbol: str | None = None) -> dict[str, Any]:
    environment = resolve_environment("testnet")
    credentials = load_credentials(required=True)
    timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=credentials,
            proxy_settings=load_proxy_settings(),
        )
        positions = await client.position_information_v3(symbol)
        open_orders = await client.open_orders(symbol)
        conditional_orders = await client.open_conditional_orders(symbol)
    open_positions = _open_position_rows(positions)
    return {
        "environment": environment.name,
        "positions_count": len(open_positions),
        "positions": open_positions,
        "open_orders_count": len(open_orders if isinstance(open_orders, list) else []),
        "open_orders": open_orders if isinstance(open_orders, list) else open_orders,
        "conditional_orders_count": len(conditional_orders if isinstance(conditional_orders, list) else []),
        "conditional_orders": conditional_orders if isinstance(conditional_orders, list) else conditional_orders,
    }


def _gate_errors(
    *,
    config: dict[str, Any],
    snapshot: dict[str, Any],
    resources: dict[str, Any],
    testnet_state: dict[str, Any],
    residual: list[dict[str, Any]],
    service_states: dict[str, str],
    oom_events: list[str],
    config_errors: list[str],
) -> list[str]:
    status = snapshot.get("system_status") if isinstance(snapshot.get("system_status"), dict) else {}
    regime = snapshot.get("market_regime") if isinstance(snapshot.get("market_regime"), dict) else {}
    candidates = snapshot.get("top_candidates") if isinstance(snapshot.get("top_candidates"), list) else []
    selected_candidate = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
    errors: list[str] = []
    if config_errors:
        errors.extend(f"config:{item}" for item in config_errors)
    if status.get("trusted_runtime_snapshot") is not True:
        errors.append("trusted_runtime_snapshot_not_true")
    if status.get("data_fresh") is not True:
        errors.append("data_fresh_not_true")
    websocket_status = str(status.get("websocket_status") or "").strip().lower()
    if websocket_status not in {"healthy", "degraded"}:
        errors.append("websocket_status_not_healthy_or_degraded")
    if str(status.get("exchange_status") or "").strip().lower() != "healthy":
        errors.append("exchange_status_not_healthy")
    if str(status.get("candidate_state") or "").strip().lower() != "known":
        errors.append("candidate_state_not_known")
    if int(status.get("top_candidates_count") or len(snapshot.get("top_candidates") or [])) <= 0:
        errors.append("top_candidates_empty")
    candidate_age = status.get("candidate_latest_age_sec")
    if candidate_age is None or float(candidate_age) > 60.0:
        errors.append("candidate_latest_age_sec_over_60")
    regime_name = str(regime.get("regime") or "UNKNOWN").upper()
    if regime_name == "UNKNOWN":
        errors.append("market_regime_unknown")
    if regime_name not in {"TREND_UP", "TREND_DOWN"}:
        errors.append("market_regime_not_trend_aligned")
    if not (regime.get("market_regime_source") or regime.get("source")):
        errors.append("market_regime_source_missing")
    if not (regime.get("market_regime_reason") or regime.get("reason")):
        errors.append("market_regime_reason_missing")
    if (regime.get("market_regime_confidence") if regime.get("market_regime_confidence") is not None else regime.get("confidence")) is None:
        errors.append("market_regime_confidence_missing")
    if selected_candidate:
        direction_payload = _direction_regime_payload(selected_candidate, regime)
        regime_name = str(regime.get("regime") or "UNKNOWN").upper()
        candidate_direction = str(direction_payload.get("candidate_direction") or "").upper()
        expected_direction = {"TREND_UP": "LONG", "TREND_DOWN": "SHORT"}.get(regime_name)
        if expected_direction is None or candidate_direction != expected_direction:
            errors.append("candidate_direction_not_trend_aligned")
        if direction_payload.get("direction_regime_allowed") is not True:
            errors.append("direction_regime_not_allowed")
        if not direction_payload.get("direction_regime_reason"):
            errors.append("direction_regime_reason_missing")
        if selected_candidate.get("exchange_filter_checked") is not True:
            errors.append("selected_exchange_filter_not_checked")
        if selected_candidate.get("symbol_tradeable") is not True:
            errors.append("selected_symbol_not_tradeable")
        if selected_candidate.get("micro_notional_feasible") is not True:
            errors.append("selected_micro_notional_infeasible")
        required_quote = _safe_float(selected_candidate.get("required_quote_allocation_usdt"))
        max_quote = _safe_float(selected_candidate.get("max_quote_allocation_usdt") or config.get("max_quote_allocation_usdt"))
        configured_quote = _safe_float(selected_candidate.get("configured_quote_allocation_usdt"))
        configured_leverage = _safe_float(selected_candidate.get("configured_leverage"))
        if required_quote is None or max_quote is None or required_quote > max_quote:
            errors.append("required_quote_allocation_exceeds_max")
        if configured_quote != _safe_float(config.get("quote_allocation_usdt")):
            errors.append("quote_allocation_changed")
        if configured_leverage != _safe_float(config.get("leverage")):
            errors.append("leverage_changed")
    else:
        errors.append("selected_candidate_missing")
    if testnet_state.get("positions_count") != 0:
        errors.append("testnet_open_positions_not_zero")
    if testnet_state.get("open_orders_count") != 0:
        errors.append("testnet_open_orders_not_zero")
    if testnet_state.get("conditional_orders_count") != 0:
        errors.append("testnet_open_conditional_orders_not_zero")
    if residual:
        errors.append("stage2_runner_residual_process")
    if float((resources.get("memory") or {}).get("mem_available_mb") or 0.0) < 100.0:
        errors.append("available_memory_below_100mib")
    if oom_events:
        errors.append("recent_24h_oom_detected")
    if config.get("mainnet_live") is not False:
        errors.append("mainnet_live_not_false")
    if bool(config.get("cleanup", False)):
        errors.append("cleanup_not_false")
    if bool(config.get("auto_promotion", False)):
        errors.append("auto_promotion_not_false")
    for service in ("phoenix-dashboard-snapshot-api.service", "phoenix-candidate-producer.service", "hermes-decision-loop.service"):
        if service_states.get(service) != "active":
            errors.append(f"{service}_not_active")
    return list(dict.fromkeys(errors))


async def _run_gate(
    *,
    gate_index: int,
    config: dict[str, Any],
    config_errors: list[str],
    capabilities: dict[str, Any],
    dashboard_token: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    started = time.monotonic()
    resources = _disk_memory_status()
    service_states = {
        "phoenix-dashboard-snapshot-api.service": _service_state("phoenix-dashboard-snapshot-api.service"),
        "phoenix-candidate-producer.service": _service_state("phoenix-candidate-producer.service"),
        "hermes-decision-loop.service": _service_state("hermes-decision-loop.service"),
    }
    testnet_state = await _fetch_testnet_state()
    residual = _stage2_residual_processes()
    oom_events = _recent_oom_events()
    snapshot = build_dashboard_api_trader_snapshot(
        root=ROOT,
        dashboard_snapshot_url=str(config.get("dashboard_snapshot_url") or "http://127.0.0.1:18765/api/phoenix-dashboard-snapshot"),
        dashboard_bearer_token=dashboard_token,
        dashboard_timeout_sec=float(config.get("dashboard_timeout_sec") or 30),
        max_candidates=10,
        stale_after_sec=60,
        max_open_positions=int(config.get("max_open_positions") or 1),
        protective_stop_path_available=bool(capabilities.get("protective_stop_path_available")),
        take_profit_path_available=bool(capabilities.get("take_profit_path_available")),
        emergency_close_available=bool(capabilities.get("emergency_close_available")),
        protective_stop_capability_source=str(capabilities.get("protective_stop_capability_source") or "unverified"),
        take_profit_capability_source=str(capabilities.get("take_profit_capability_source") or "unverified"),
        emergency_close_capability_source=str(capabilities.get("emergency_close_capability_source") or "unverified"),
    )
    snapshot = await _enrich_stage2_snapshot_exchange_filters(
        snapshot,
        quote_allocation_usdt=config.get("quote_allocation_usdt"),
        leverage=config.get("leverage"),
        max_quote_allocation_usdt=config.get("max_quote_allocation_usdt"),
    )
    status = snapshot.get("system_status") if isinstance(snapshot.get("system_status"), dict) else {}
    regime = snapshot.get("market_regime") if isinstance(snapshot.get("market_regime"), dict) else {}
    candidates = snapshot.get("top_candidates") if isinstance(snapshot.get("top_candidates"), list) else []
    selected_candidate = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
    direction_payload = _direction_regime_payload(selected_candidate, regime)
    errors = _gate_errors(
        config=config,
        snapshot=snapshot,
        resources=resources,
        testnet_state=testnet_state,
        residual=residual,
        service_states=service_states,
        oom_events=oom_events,
        config_errors=config_errors,
    )
    summary = {
        "gate_index": gate_index,
        "passed": not errors,
        "errors": errors,
        "latency_sec": round(time.monotonic() - started, 3),
        "resources": resources,
        "service_states": service_states,
        "testnet_state": testnet_state,
        "stage2_runner_residual_processes": residual,
        "recent_24h_oom_events": oom_events,
        "trusted_runtime_snapshot": status.get("trusted_runtime_snapshot"),
        "data_fresh": status.get("data_fresh"),
        "websocket_status": status.get("websocket_status"),
        "exchange_status": status.get("exchange_status"),
        "candidate_state": status.get("candidate_state"),
        "candidate_latest_age_sec": status.get("candidate_latest_age_sec"),
        "top_candidates_count": int(status.get("top_candidates_count") or len(snapshot.get("top_candidates") or [])),
        "market_regime": regime.get("regime"),
        "market_regime_source": regime.get("market_regime_source") or regime.get("source"),
        "market_regime_reason": regime.get("market_regime_reason") or regime.get("reason"),
        "market_regime_confidence": (
            regime.get("market_regime_confidence")
            if regime.get("market_regime_confidence") is not None
            else regime.get("confidence")
        ),
        "direction_regime": direction_payload,
        "allowed_direction": direction_payload.get("allowed_direction"),
        "direction_regime_allowed": direction_payload.get("direction_regime_allowed"),
        "direction_regime_reason": direction_payload.get("direction_regime_reason"),
        "freshness_reason": status.get("freshness_reason"),
        "exchange_filter_checked": status.get("exchange_filter_checked"),
        "exchange_filter_checked_count": status.get("exchange_filter_checked_count"),
        "micro_notional_feasible_count": status.get("micro_notional_feasible_count"),
        "micro_notional_infeasible_count": status.get("micro_notional_infeasible_count"),
        "selected_candidate": selected_candidate,
    }
    return summary, snapshot


def _extract_order_id(response: Any) -> Any:
    if not isinstance(response, dict):
        return None
    for key in ("orderId", "order_id", "algoId", "strategyId", "clientOrderId", "clientAlgoId"):
        if response.get(key) not in (None, ""):
            return response.get(key)
    payload = response.get("payload")
    if isinstance(payload, dict):
        return _extract_order_id(payload)
    safe_gateway = response.get("safe_order_gateway")
    if isinstance(safe_gateway, dict):
        execution = safe_gateway.get("execution_result")
        if isinstance(execution, dict):
            return _extract_order_id(execution.get("payload"))
    return None


def _extract_payload(response: Any) -> dict[str, Any]:
    if isinstance(response, dict) and isinstance(response.get("payload"), dict):
        return response["payload"]
    return response if isinstance(response, dict) else {}


def _position_for_symbol(state: dict[str, Any], symbol: str | None) -> dict[str, Any] | None:
    if not symbol:
        return None
    for row in state.get("positions") or []:
        if str(row.get("symbol") or "").upper() == symbol.upper():
            return row
    return None


def _final_state_from_execution(decision: dict[str, Any], execution_payload: dict[str, Any], lifecycle: dict[str, Any]) -> str:
    action = str(decision.get("action") or "").upper()
    if lifecycle.get("hard_freeze") or execution_payload.get("frozen"):
        return "HARD_FREEZE"
    if lifecycle.get("final_close_reason"):
        return str(lifecycle.get("final_close_reason"))
    if action == "NO_TRADE":
        return "NO_TRADE"
    if execution_payload.get("status") == "testnet_order_submitted_with_protective_stop_and_take_profit":
        return "OPENED_AND_PROTECTED"
    return "NO_TRADE"


async def main() -> int:
    config = _read_json(CONFIG_PATH)
    run_dir = BASE_OUTPUT / f"cycle_{_now_id()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "stage2_micro_cycle_report.json"
    config_errors = validate_stage2_micro_config(config)
    token_file = Path(config.get("dashboard_token_file") or ROOT / ".phoenix_dashboard_readonly_token")
    dashboard_token = token_file.read_text(encoding="utf-8").strip() if token_file.exists() else None
    capabilities = verify_testnet_executor_capabilities()
    gate_results: list[dict[str, Any]] = []
    snapshot_for_cycle: dict[str, Any] | None = None

    for index in range(1, 4):
        gate, snapshot = await _run_gate(
            gate_index=index,
            config=config,
            config_errors=config_errors,
            capabilities=capabilities,
            dashboard_token=dashboard_token,
        )
        gate_results.append(gate)
        snapshot_for_cycle = snapshot
        if not gate["passed"]:
            report = {
                "cycle_final_state": "CANCELLED_BY_GATE",
                "run_dir": str(run_dir),
                "report_path": str(report_path),
                "replay_path": None,
                "review_report_path": None,
                "readiness_gates": gate_results,
                "start_resources": gate_results[0]["resources"] if gate_results else None,
                "end_resources": _disk_memory_status(),
                "starting_testnet_state": gate.get("testnet_state"),
                "ending_testnet_state": await _fetch_testnet_state(),
                "config_path": str(CONFIG_PATH),
                "executor_called": False,
                "entry_order_submitted": False,
                "stop_loss_submitted": False,
                "take_profit_submitted": False,
                "testnet_order_submitted": False,
                "mainnet_order_submitted": False,
                "cleanup": False,
                "auto_promotion": False,
                "hard_freeze": False,
            }
            _write_json(report_path, report)
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
            return 3
        if index < 3:
            await asyncio.sleep(10.0)

    assert snapshot_for_cycle is not None
    bridge = HermesFileBridge(
        inbox_dir=config.get("hermes_inbox") or ROOT / "hermes_inbox",
        outbox_dir=config.get("hermes_outbox") or ROOT / "hermes_outbox",
        archive_dir=config.get("hermes_archive") or ROOT / "hermes_archive",
        log_dir=config.get("hermes_logs") or ROOT / "hermes_logs",
    )
    bridge.ensure_dirs()
    provider = provider_from_name(
        "file",
        decision_file=bridge.outbox_dir,
        decision_timeout_sec=float(config.get("decision_timeout_sec") or 60),
        decision_poll_interval_sec=float(config.get("decision_poll_interval_sec") or 0.5),
    )
    callback = TestnetExecutorCallback(
        snapshot=snapshot_for_cycle,
        output_dir=run_dir,
        environment_name="testnet",
        quote_allocation_usdt=float(config.get("quote_allocation_usdt") or 5.0),
        leverage=int(config.get("leverage") or 2),
        lifecycle_monitor_enabled=bool(config.get("lifecycle_monitor_enabled")),
        lifecycle_poll_sec=float(config.get("lifecycle_poll_sec") or 5.0),
        lifecycle_post_close_wait_sec=float(config.get("lifecycle_post_close_wait_sec") or 60.0),
    )
    result = await run_trader_cycle_async(
        snapshot_builder=lambda: snapshot_for_cycle,
        decision_provider=provider,
        output_dir=run_dir,
        dry_run=False,
        environment={
            "runtime_mode": "TESTNET_LIVE",
            "env": "testnet",
            "PHOENIX_MAINNET_LIVE_ENABLED": "false",
            "PHOENIX_LIVE_TRADING_ENABLED": "false",
            "PHOENIX_PROMOTION_ALLOWED": "false",
            "require_trusted_runtime_snapshot": True,
            "quote_allocation_usdt": float(config.get("quote_allocation_usdt") or 5.0),
            "stage2_micro_order": True,
            "cleanup": False,
            "auto_promotion": False,
        },
        risk_config=risk_config_from_trial_config(config),
        executor_callback=callback,
        file_bridge=bridge,
    )
    provider_result = result.get("hermes_provider_result") if isinstance(result.get("hermes_provider_result"), dict) else {}
    decision = provider_result.get("decision") if isinstance(provider_result.get("decision"), dict) else {}
    execution = result.get("execution_result") if isinstance(result.get("execution_result"), dict) else {}
    execution_payload = execution.get("payload") if isinstance(execution.get("payload"), dict) else {}
    lifecycle = execution_payload.get("lifecycle_result") if isinstance(execution_payload.get("lifecycle_result"), dict) else {}
    intent = execution_payload.get("intent") if isinstance(execution_payload.get("intent"), dict) else {}
    entry_response = execution_payload.get("entry_response")
    stop_response = execution_payload.get("protective_stop_response")
    take_profit_response = execution_payload.get("take_profit_response")
    symbol = decision.get("symbol") or intent.get("symbol")
    ending_state = await _fetch_testnet_state(str(symbol) if symbol else None)
    end_resources = _disk_memory_status()
    status = snapshot_for_cycle.get("system_status") if isinstance(snapshot_for_cycle.get("system_status"), dict) else {}
    regime = snapshot_for_cycle.get("market_regime") if isinstance(snapshot_for_cycle.get("market_regime"), dict) else {}
    final_state = _final_state_from_execution(decision, execution_payload, lifecycle)
    final_close_order = lifecycle.get("timeout_close_response") or lifecycle.get("emergency_close_response") or {}
    selected_candidate = (
        (snapshot_for_cycle.get("top_candidates") or [{}])[0]
        if isinstance(snapshot_for_cycle.get("top_candidates"), list) and snapshot_for_cycle.get("top_candidates")
        else {}
    )
    direction_payload = _direction_regime_payload(selected_candidate, regime)
    raw_response = provider_result.get("raw_response") if isinstance(provider_result.get("raw_response"), dict) else {}
    archive = result.get("bridge_archive") if isinstance(result.get("bridge_archive"), dict) else {}
    report = {
        "cycle_final_state": final_state,
        "run_dir": str(run_dir),
        "report_path": str(report_path),
        "replay_path": str(run_dir / "replay_events.jsonl"),
        "review_report_path": str(run_dir / "review_report.jsonl"),
        "lifecycle_report_path": lifecycle.get("report_path") or str(run_dir / "stage2_lifecycle_final_report.json"),
        "lifecycle_review_report_path": lifecycle.get("review_report_path") or str(run_dir / "stage2_lifecycle_review_report.jsonl"),
        "snapshot_path": result.get("snapshot_path"),
        "decision_file_path": raw_response.get("_decision_path"),
        "decision_archive_path": archive.get("decision_archive_path"),
        "readiness_gates": gate_results,
        "start_resources": gate_results[0]["resources"],
        "end_resources": end_resources,
        "starting_testnet_state": gate_results[-1]["testnet_state"],
        "ending_testnet_state": ending_state,
        "config_path": str(CONFIG_PATH),
        "config": {
            "mode": config.get("mode"),
            "provider": config.get("provider"),
            "decision_policy": config.get("decision_policy"),
            "testnet_only": config.get("testnet_only"),
            "mainnet_live": config.get("mainnet_live"),
            "max_open_positions": config.get("max_open_positions"),
            "leverage": config.get("leverage"),
            "quote_allocation_usdt": config.get("quote_allocation_usdt"),
            "max_quote_allocation_usdt": config.get("max_quote_allocation_usdt"),
            "lifecycle_monitor_enabled": config.get("lifecycle_monitor_enabled"),
        },
        "market_regime": regime.get("regime"),
        "market_regime_source": regime.get("market_regime_source") or regime.get("source"),
        "market_regime_confidence": regime.get("market_regime_confidence") if regime.get("market_regime_confidence") is not None else regime.get("confidence"),
        "market_regime_reason": regime.get("market_regime_reason") or regime.get("reason"),
        "candidate_latest_age_sec": status.get("candidate_latest_age_sec"),
        "top_candidates_count": int(status.get("top_candidates_count") or len(snapshot_for_cycle.get("top_candidates") or [])),
        "direction_regime_result": direction_payload,
        "allowed_direction": direction_payload.get("allowed_direction"),
        "direction_regime_allowed": direction_payload.get("direction_regime_allowed"),
        "direction_regime_reason": direction_payload.get("direction_regime_reason"),
        "exchange_filter_result": {
            "symbol": selected_candidate.get("symbol"),
            "exchange_filter_checked": selected_candidate.get("exchange_filter_checked"),
            "symbol_tradeable": selected_candidate.get("symbol_tradeable"),
            "micro_notional_feasible": selected_candidate.get("micro_notional_feasible"),
            "rounded_qty": selected_candidate.get("rounded_qty"),
            "min_qty": selected_candidate.get("min_qty"),
            "step_size": selected_candidate.get("step_size"),
            "min_notional": selected_candidate.get("min_notional"),
            "required_quote_allocation_usdt": selected_candidate.get("required_quote_allocation_usdt"),
            "configured_quote_allocation_usdt": selected_candidate.get("configured_quote_allocation_usdt"),
            "configured_leverage": selected_candidate.get("configured_leverage"),
            "max_quote_allocation_usdt": selected_candidate.get("max_quote_allocation_usdt"),
            "infeasible_reason": selected_candidate.get("infeasible_reason"),
        },
        "hermes_decision": decision.get("action"),
        "symbol": symbol,
        "direction": decision.get("action"),
        "source": decision.get("source"),
        "writer": decision.get("writer"),
        "decision_origin": provider_result.get("decision_origin"),
        "fallback_used": provider_result.get("fallback_used"),
        "risk_governor_result": result.get("risk_governor_result"),
        "order_intent": intent,
        "executor_called": bool(execution.get("executor_called")),
        "entry_order_submitted": bool(entry_response),
        "entry_order_id": _extract_order_id(entry_response),
        "entry_fill_price": lifecycle.get("entry_fill_price") or intent.get("entry_price") or _extract_payload(entry_response).get("avgPrice"),
        "position_size": lifecycle.get("position_size") or intent.get("quantity") or _extract_payload(entry_response).get("executedQty"),
        "notional": intent.get("notional_usdt") or intent.get("quote_allocation_usdt"),
        "leverage": config.get("leverage"),
        "stop_loss_submitted": bool(stop_response),
        "stop_loss_order_id": _extract_order_id(stop_response),
        "stop_loss_reduce_only": _extract_payload(stop_response).get("reduceOnly"),
        "stop_loss_close_position": _extract_payload(stop_response).get("closePosition"),
        "take_profit_submitted": bool(take_profit_response),
        "take_profit_order_id": _extract_order_id(take_profit_response),
        "take_profit_reduce_only": _extract_payload(take_profit_response).get("reduceOnly"),
        "take_profit_close_position": _extract_payload(take_profit_response).get("closePosition"),
        "lifecycle_monitor_auto_handoff": bool(lifecycle),
        "timeout_due_at": lifecycle.get("timeout_due_at"),
        "final_close_reason": lifecycle.get("final_close_reason") or final_state,
        "final_close_order_id": _extract_order_id(final_close_order),
        "final_close_price": lifecycle.get("close_fill_price"),
        "realized_pnl": lifecycle.get("realized_pnl_usdt"),
        "roi": lifecycle.get("roi_pct"),
        "fees": lifecycle.get("fees"),
        "holding_time_sec": lifecycle.get("holding_time_sec"),
        "emergency_close": bool(lifecycle.get("emergency_close")),
        "hard_freeze": bool(result.get("frozen") or execution.get("frozen") or lifecycle.get("hard_freeze")),
        "testnet_order_submitted": bool(execution.get("testnet_order_submitted")),
        "mainnet_order_submitted": bool(execution.get("mainnet_order_submitted")),
        "cleanup": False,
        "auto_promotion": False,
        "orphan_protective_order": bool((ending_state.get("conditional_orders_count") or 0) > 0),
        "execution_result": execution,
        "lifecycle_result": lifecycle,
    }
    _write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
