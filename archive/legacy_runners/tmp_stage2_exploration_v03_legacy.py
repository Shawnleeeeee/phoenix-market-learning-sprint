from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import aiohttp

from phoenix.binance_futures import BinanceFuturesClient
from phoenix.config import load_credentials, load_proxy_settings, resolve_environment
from phoenix.direction_regime_matrix import direction_regime_fields, evaluate_direction_regime
from phoenix.exchange_filter_feasibility import enrich_snapshot_with_exchange_filters
from phoenix.hermes_decision import OPEN_ACTIONS
from phoenix.hermes_decision_provider import provider_from_name
from phoenix.hermes_file_bridge import HermesFileBridge
from phoenix.hermes_trader_mode import (
    risk_config_from_trial_config,
    run_trader_cycle_async,
    validate_stage2_micro_config,
)
from phoenix.testnet_executor_callback import TestnetExecutorCallback, verify_testnet_executor_capabilities
from phoenix.trader_snapshot_runtime import build_dashboard_api_trader_snapshot


ROOT = Path("/opt/phoenix-testnet")
CONFIG_PATH = ROOT / "configs/hermes_trader_stage2_micro.testnet.json"
RUN_ROOT = ROOT / "hermes_logs/stage2_testnet_exploration_v03"
SESSION_DURATION_SEC = 60 * 60
MAX_REAL_TESTNET_TRADES = 3
GATE_INTERVAL_SEC = 5.0
OBSERVATION_INTERVAL_SEC = 5.0
GATES_REQUIRED = 3
CANDIDATE_MAX_AGE_SEC = 30.0
MIN_MEMORY_AVAILABLE_MB = 100.0
NO_NEW_CYCLE_WITHIN_SEC = 8 * 60
MAX_CONSECUTIVE_SYSTEM_ANOMALIES = 3
TARGET_OBSERVATION_SAMPLES = 50
TARGET_HERMES_DECISION_SAMPLES = 30
STAGE2_PATTERNS = ("stage2_micro", "stage2_preflight", "phoenix.hermes_trader_mode")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("session_%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _ms_between(start: Any, end: Any) -> float | None:
    a = _parse_iso(start)
    b = _parse_iso(end)
    if a is None or b is None:
        return None
    return round((b - a).total_seconds() * 1000.0, 3)


def _candidate_ms_between(start: Any, end: Any) -> float | None:
    value = _ms_between(start, end)
    if value is None or value < 0:
        return None
    return value


def _candidate_latency_reliability(*, candidate_to_decision: float | None, candidate_to_order: float | None) -> dict[str, Any]:
    unavailable: list[str] = []
    if candidate_to_decision is None:
        unavailable.append("candidate_to_decision_latency_ms")
    if candidate_to_order is None:
        unavailable.append("candidate_to_order_latency_ms")
    return {
        "reliable_latency_fields": ["snapshot_to_decision_latency_ms", "decision_to_gateway_latency_ms"],
        "unreliable_latency_fields": ["candidate_to_decision_latency_ms", "candidate_to_order_latency_ms"],
        "candidate_latency_unavailable_fields": unavailable,
        "candidate_latency_note": (
            "candidate timestamps can come from feed age or upstream clocks; unavailable means missing, "
            "unparseable, or negative and is excluded from SLO."
        ),
    }


def _stats(values: list[float | None]) -> dict[str, Any]:
    clean = sorted(float(item) for item in values if item is not None)
    if not clean:
        return {"count": 0, "min": None, "max": None, "avg": None, "p95": None}
    p95_index = min(len(clean) - 1, int(round((len(clean) - 1) * 0.95)))
    return {
        "count": len(clean),
        "min": round(clean[0], 3),
        "max": round(clean[-1], 3),
        "avg": round(mean(clean), 3),
        "p95": round(clean[p95_index], 3),
    }


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
            if any(pattern in line.lower() for pattern in patterns):
                events.append(line[:1000])
        if events:
            break
    return events[:20]


def _stage2_residual_processes() -> list[dict[str, Any]]:
    ignore_pids = {os.getpid(), os.getppid()}
    ignore_tokens = (
        ".tmp_stage2_exploration_v02.py",
        ".tmp_stage2_exploration_v03.py",
        "stage2_exploration_v02",
        "stage2_exploration_v03",
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
        if not isinstance(row, dict):
            continue
        try:
            amount = abs(float(row.get("positionAmt") or row.get("position_amount") or 0.0))
        except (TypeError, ValueError):
            amount = 0.0
        if amount > 0:
            open_rows.append(row)
    return open_rows


async def _fetch_exchange_info() -> dict[str, Any]:
    environment = resolve_environment("testnet")
    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=None,
            proxy_settings=load_proxy_settings(),
        )
        return await client.exchange_info()


async def _fetch_testnet_state(symbol: str | None = None) -> dict[str, Any]:
    environment = resolve_environment("testnet")
    credentials = load_credentials(required=True)
    if credentials.environment.name not in {"testnet", "demo"}:
        raise RuntimeError(f"credentials_not_testnet:{credentials.environment.name}")
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


def _direction_regime_payload(candidate: dict[str, Any], regime: dict[str, Any]) -> dict[str, Any]:
    if not candidate:
        return {
            "market_regime": str(regime.get("regime") or "UNKNOWN").upper(),
            "candidate_direction": None,
            "allowed_direction": "NONE",
            "direction_regime_allowed": False,
            "direction_regime_reason": "selected candidate missing; fail closed",
            "blocked_by": ["selected_candidate_missing", "direction_regime_mismatch"],
        }
    result = evaluate_direction_regime(
        candidate_direction=candidate.get("bias") or candidate.get("side") or candidate.get("direction"),
        market_regime=regime,
        candidate=candidate,
    )
    return {**direction_regime_fields(result), "blocked_by": result.blocked_by}


def _selected_candidate(snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = snapshot.get("top_candidates") if isinstance(snapshot.get("top_candidates"), list) else []
    return rows[0] if rows and isinstance(rows[0], dict) else {}


def _candidate_generated_at(snapshot: dict[str, Any]) -> str | None:
    candidate = _selected_candidate(snapshot)
    status = snapshot.get("system_status") if isinstance(snapshot.get("system_status"), dict) else {}
    for source in (candidate, status, snapshot):
        for key in (
            "candidate_generated_at",
            "generated_at",
            "emitted_at",
            "created_at",
            "updated_at",
            "candidate_latest_generated_at",
        ):
            value = source.get(key) if isinstance(source, dict) else None
            if value:
                return str(value)
    age = _safe_float(status.get("candidate_latest_age_sec") if isinstance(status, dict) else None)
    if age is None:
        return None
    dt = datetime.now(timezone.utc)
    return (dt.timestamp() - age) and datetime.fromtimestamp(dt.timestamp() - age, tz=timezone.utc).isoformat()


def _snapshot_created_at(snapshot: dict[str, Any]) -> str:
    status = snapshot.get("system_status") if isinstance(snapshot.get("system_status"), dict) else {}
    for source in (status, snapshot):
        for key in ("snapshot_created_at", "created_at", "snapshot_time", "updated_at"):
            value = source.get(key) if isinstance(source, dict) else None
            if value:
                return str(value)
    return _now_iso()


def _gate_errors(
    *,
    config: dict[str, Any],
    config_errors: list[str],
    snapshot: dict[str, Any],
    resources: dict[str, Any],
    testnet_state: dict[str, Any] | None,
    testnet_state_error: str | None,
    residual: list[dict[str, Any]],
    service_states: dict[str, str],
    oom_events: list[str],
) -> list[str]:
    status = snapshot.get("system_status") if isinstance(snapshot.get("system_status"), dict) else {}
    regime = snapshot.get("market_regime") if isinstance(snapshot.get("market_regime"), dict) else {}
    candidates = snapshot.get("top_candidates") if isinstance(snapshot.get("top_candidates"), list) else []
    selected = _selected_candidate(snapshot)
    direction_payload = _direction_regime_payload(selected, regime)
    errors: list[str] = []
    if config_errors:
        errors.extend(f"config:{item}" for item in config_errors)
    if config.get("mode") != "testnet":
        errors.append("mode_not_testnet")
    if config.get("provider") != "file":
        errors.append("provider_not_file")
    if config.get("testnet_only") is not True:
        errors.append("testnet_only_not_true")
    if config.get("mainnet_live") is not False:
        errors.append("mainnet_live_not_false")
    if bool(config.get("cleanup", False)):
        errors.append("cleanup_not_false")
    if bool(config.get("auto_promotion", False)):
        errors.append("auto_promotion_not_false")
    if _safe_float(config.get("quote_allocation_usdt")) != 5.0:
        errors.append("quote_allocation_not_5")
    if _safe_float(config.get("leverage")) != 2.0:
        errors.append("leverage_not_2")
    if _safe_float(config.get("max_quote_allocation_usdt")) != 10.0:
        errors.append("max_quote_allocation_not_10")
    for service in ("hermes-decision-loop.service", "phoenix-candidate-producer.service", "phoenix-dashboard-snapshot-api.service"):
        if service_states.get(service) != "active":
            errors.append(f"{service}_not_active")
    if status.get("trusted_runtime_snapshot") is not True:
        errors.append("trusted_runtime_snapshot_not_true")
    if status.get("data_fresh") is not True:
        errors.append("data_fresh_not_true")
    websocket_status = str(status.get("websocket_status") or "").strip().lower()
    if websocket_status not in {"healthy", "degraded"}:
        errors.append("websocket_status_unavailable")
    if str(status.get("exchange_status") or "").strip().lower() != "healthy":
        errors.append("exchange_status_not_healthy")
    if str(status.get("candidate_state") or "").strip().lower() != "known":
        errors.append("candidate_state_not_known")
    if int(status.get("top_candidates_count") or len(candidates)) <= 0:
        errors.append("top_candidates_empty")
    candidate_age = _safe_float(status.get("candidate_latest_age_sec"))
    if candidate_age is None or candidate_age > CANDIDATE_MAX_AGE_SEC:
        errors.append("candidate_latest_age_sec_over_30")
    regime_name = str(regime.get("regime") or "UNKNOWN").upper()
    if regime_name not in {"TREND_UP", "TREND_DOWN"}:
        errors.append("market_regime_not_trend_up_or_down")
    if not (regime.get("market_regime_source") or regime.get("source")):
        errors.append("market_regime_source_missing")
    if not (regime.get("market_regime_reason") or regime.get("reason")):
        errors.append("market_regime_reason_missing")
    confidence = regime.get("market_regime_confidence") if regime.get("market_regime_confidence") is not None else regime.get("confidence")
    if confidence is None:
        errors.append("market_regime_confidence_missing")
    expected_direction = {"TREND_UP": "LONG", "TREND_DOWN": "SHORT"}.get(regime_name)
    candidate_direction = str(direction_payload.get("candidate_direction") or "").upper()
    if expected_direction is None or candidate_direction != expected_direction:
        errors.append("candidate_direction_not_trend_aligned")
    if direction_payload.get("direction_regime_allowed") is not True:
        errors.append("direction_regime_not_allowed")
    if not direction_payload.get("direction_regime_reason"):
        errors.append("direction_regime_reason_missing")
    if selected.get("exchange_filter_checked") is not True:
        errors.append("selected_exchange_filter_not_checked")
    if selected.get("symbol_tradeable") is not True:
        errors.append("selected_symbol_not_tradeable")
    if selected.get("micro_notional_feasible") is not True:
        errors.append("selected_micro_notional_infeasible")
    required_quote = _safe_float(selected.get("required_quote_allocation_usdt"))
    max_quote = _safe_float(selected.get("max_quote_allocation_usdt") or config.get("max_quote_allocation_usdt"))
    if required_quote is None or max_quote is None or required_quote > max_quote:
        errors.append("required_quote_allocation_exceeds_max")
    configured_quote = _safe_float(selected.get("configured_quote_allocation_usdt"))
    configured_leverage = _safe_float(selected.get("configured_leverage"))
    if configured_quote != 5.0:
        errors.append("configured_quote_allocation_changed")
    if configured_leverage != 2.0:
        errors.append("configured_leverage_changed")
    if testnet_state_error:
        errors.append("testnet_state_fetch_failed")
    state = testnet_state or {}
    if state.get("positions_count") != 0:
        errors.append("testnet_open_positions_not_zero")
    if state.get("open_orders_count") != 0:
        errors.append("testnet_open_orders_not_zero")
    if state.get("conditional_orders_count") != 0:
        errors.append("testnet_open_conditional_orders_not_zero")
    if residual:
        errors.append("stage2_runner_residual_process")
    if float((resources.get("memory") or {}).get("mem_available_mb") or 0.0) < MIN_MEMORY_AVAILABLE_MB:
        errors.append("available_memory_below_100mib")
    if oom_events:
        errors.append("recent_24h_oom_detected")
    return list(dict.fromkeys(errors))


def _is_system_anomaly(errors: list[str]) -> bool:
    non_system_prefixes = {
        "market_regime_not_trend_up_or_down",
        "candidate_direction_not_trend_aligned",
        "direction_regime_not_allowed",
        "selected_exchange_filter_not_checked",
        "selected_symbol_not_tradeable",
        "selected_micro_notional_infeasible",
        "required_quote_allocation_exceeds_max",
        "top_candidates_empty",
    }
    if not errors:
        return False
    return any(error not in non_system_prefixes for error in errors)


async def _collect_gate_sample(
    *,
    session_dir: Path,
    attempt_index: int,
    gate_index: int,
    config: dict[str, Any],
    config_errors: list[str],
    capabilities: dict[str, Any],
    dashboard_token: str | None,
    exchange_info: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    started = time.monotonic()
    resources = _disk_memory_status()
    service_states = {
        "hermes-decision-loop.service": _service_state("hermes-decision-loop.service"),
        "phoenix-candidate-producer.service": _service_state("phoenix-candidate-producer.service"),
        "phoenix-dashboard-snapshot-api.service": _service_state("phoenix-dashboard-snapshot-api.service"),
    }
    testnet_state: dict[str, Any] | None = None
    testnet_state_error: str | None = None
    try:
        testnet_state = await _fetch_testnet_state()
    except Exception as exc:  # noqa: BLE001
        testnet_state_error = str(exc)[:500]
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
    snapshot = enrich_snapshot_with_exchange_filters(
        snapshot,
        exchange_info,
        configured_quote_allocation_usdt=config.get("quote_allocation_usdt"),
        configured_leverage=config.get("leverage"),
        max_quote_allocation_usdt=config.get("max_quote_allocation_usdt"),
    )
    status = snapshot.get("system_status") if isinstance(snapshot.get("system_status"), dict) else {}
    regime = snapshot.get("market_regime") if isinstance(snapshot.get("market_regime"), dict) else {}
    selected = _selected_candidate(snapshot)
    direction_payload = _direction_regime_payload(selected, regime)
    errors = _gate_errors(
        config=config,
        config_errors=config_errors,
        snapshot=snapshot,
        resources=resources,
        testnet_state=testnet_state,
        testnet_state_error=testnet_state_error,
        residual=residual,
        service_states=service_states,
        oom_events=oom_events,
    )
    sample = {
        "timestamp": _now_iso(),
        "attempt_index": attempt_index,
        "gate_index": gate_index,
        "passed": not errors,
        "errors": errors,
        "system_anomaly": _is_system_anomaly(errors),
        "latency_sec": round(time.monotonic() - started, 3),
        "trusted_runtime_snapshot": status.get("trusted_runtime_snapshot"),
        "data_fresh": status.get("data_fresh"),
        "websocket_status": status.get("websocket_status"),
        "exchange_status": status.get("exchange_status"),
        "candidate_state": status.get("candidate_state"),
        "candidate_latest_age_sec": status.get("candidate_latest_age_sec"),
        "top_candidates_count": int(status.get("top_candidates_count") or len(snapshot.get("top_candidates") or [])),
        "market_regime": regime.get("regime"),
        "market_regime_source": regime.get("market_regime_source") or regime.get("source"),
        "market_regime_confidence": regime.get("market_regime_confidence") if regime.get("market_regime_confidence") is not None else regime.get("confidence"),
        "market_regime_reason": regime.get("market_regime_reason") or regime.get("reason"),
        "selected_symbol": selected.get("symbol"),
        "candidate_direction": direction_payload.get("candidate_direction"),
        "allowed_direction": direction_payload.get("allowed_direction"),
        "direction_regime_allowed": direction_payload.get("direction_regime_allowed"),
        "direction_regime_reason": direction_payload.get("direction_regime_reason"),
        "exchange_filter_checked": selected.get("exchange_filter_checked"),
        "symbol_tradeable": selected.get("symbol_tradeable"),
        "micro_notional_feasible": selected.get("micro_notional_feasible"),
        "required_quote_allocation_usdt": selected.get("required_quote_allocation_usdt"),
        "configured_quote_allocation_usdt": selected.get("configured_quote_allocation_usdt"),
        "configured_leverage": selected.get("configured_leverage"),
        "max_quote_allocation_usdt": selected.get("max_quote_allocation_usdt"),
        "rounded_qty": selected.get("rounded_qty"),
        "min_qty": selected.get("min_qty"),
        "step_size": selected.get("step_size"),
        "min_notional": selected.get("min_notional"),
        "infeasible_reason": selected.get("infeasible_reason"),
        "positions_count": (testnet_state or {}).get("positions_count"),
        "open_orders_count": (testnet_state or {}).get("open_orders_count"),
        "conditional_orders_count": (testnet_state or {}).get("conditional_orders_count"),
        "testnet_state_error": testnet_state_error,
        "stage2_runner_residual_processes": residual,
        "resources": resources,
        "service_states": service_states,
        "recent_24h_oom_events": oom_events,
    }
    _append_jsonl(session_dir / "readiness_samples.jsonl", sample)
    return sample, snapshot


def _latest_event(log_path: Path, *, trace_id: str, event: str) -> dict[str, Any] | None:
    if not log_path.exists():
        return None
    try:
        lines = log_path.read_text(encoding="utf-8").splitlines()[-20000:]
    except Exception:
        return None
    for line in reversed(lines):
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("trace_id") == trace_id and row.get("event") == event:
            return row
    return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _extract_order_id(response: Any) -> Any:
    if not isinstance(response, dict):
        return None
    for key in ("orderId", "order_id", "algoId", "strategyId", "clientOrderId", "clientAlgoId"):
        if response.get(key) not in (None, ""):
            return response.get(key)
    payload = response.get("payload")
    if isinstance(payload, dict):
        return _extract_order_id(payload)
    return None


def _payload(response: Any) -> dict[str, Any]:
    if isinstance(response, dict) and isinstance(response.get("payload"), dict):
        return response["payload"]
    return response if isinstance(response, dict) else {}


def _safe_gateway_submit_times(cycle_dir: Path) -> dict[str, Any]:
    rows = _read_jsonl(cycle_dir / "testnet_executor_safe_gateway.jsonl")
    out: dict[str, Any] = {}
    for row in rows:
        intent = row.get("execution_intent") if isinstance(row.get("execution_intent"), dict) else {}
        execution = row.get("execution_result") if isinstance(row.get("execution_result"), dict) else {}
        purpose = str(intent.get("purpose") or "").lower()
        timestamp = execution.get("created_at") or row.get("created_at")
        if purpose == "entry":
            out["entry_order_submitted_at"] = timestamp
        elif purpose == "protection":
            out["stop_submitted_at"] = timestamp
        elif purpose == "take_profit":
            out["take_profit_submitted_at"] = timestamp
        elif purpose == "emergency":
            out["emergency_close_submitted_at"] = timestamp
    if out.get("stop_submitted_at") and out.get("take_profit_submitted_at"):
        out["stop_tp_submitted_at"] = max(str(out["stop_submitted_at"]), str(out["take_profit_submitted_at"]))
    return out


def _final_close_reason(result: dict[str, Any]) -> str:
    execution = result.get("execution_result") if isinstance(result.get("execution_result"), dict) else {}
    payload = execution.get("payload") if isinstance(execution.get("payload"), dict) else {}
    lifecycle = payload.get("lifecycle_result") if isinstance(payload.get("lifecycle_result"), dict) else {}
    decision = (result.get("hermes_provider_result") or {}).get("decision") if isinstance(result.get("hermes_provider_result"), dict) else {}
    action = str((decision or {}).get("action") or "").upper()
    if lifecycle.get("hard_freeze") or result.get("frozen") or execution.get("frozen"):
        return "HARD_FREEZE"
    if lifecycle.get("final_close_reason"):
        return str(lifecycle.get("final_close_reason"))
    if payload.get("emergency_close"):
        return "EMERGENCY_CLOSED"
    if action == "NO_TRADE":
        return "NO_TRADE"
    if execution.get("order_submitted"):
        return "OPENED_AND_PROTECTED"
    return "NO_TRADE"


def _cycle_report(
    *,
    session_dir: Path,
    cycle_dir: Path,
    result: dict[str, Any],
    snapshot: dict[str, Any],
    gate_samples: list[dict[str, Any]],
    ending_state: dict[str, Any],
) -> dict[str, Any]:
    trace_id = str(result.get("trace_id") or "")
    provider_result = result.get("hermes_provider_result") if isinstance(result.get("hermes_provider_result"), dict) else {}
    decision = provider_result.get("decision") if isinstance(provider_result.get("decision"), dict) else {}
    raw_response = provider_result.get("raw_response") if isinstance(provider_result.get("raw_response"), dict) else {}
    gateway = result.get("safe_order_gateway_result") if isinstance(result.get("safe_order_gateway_result"), dict) else {}
    risk = result.get("risk_governor_result") if isinstance(result.get("risk_governor_result"), dict) else {}
    execution = result.get("execution_result") if isinstance(result.get("execution_result"), dict) else {}
    execution_payload = execution.get("payload") if isinstance(execution.get("payload"), dict) else {}
    lifecycle = execution_payload.get("lifecycle_result") if isinstance(execution_payload.get("lifecycle_result"), dict) else {}
    intent = execution_payload.get("intent") if isinstance(execution_payload.get("intent"), dict) else gateway.get("execution_intent") or {}
    archive = result.get("bridge_archive") if isinstance(result.get("bridge_archive"), dict) else {}
    selected = _selected_candidate(snapshot)
    regime = snapshot.get("market_regime") if isinstance(snapshot.get("market_regime"), dict) else {}
    direction_payload = _direction_regime_payload(selected, regime)
    bridge_event = _latest_event(ROOT / "hermes_logs/hermes_file_bridge_events.jsonl", trace_id=trace_id, event="snapshot_written")
    loop_event = _latest_event(ROOT / "hermes_logs/hermes_decision_loop_events.jsonl", trace_id=trace_id, event="decision_written")
    submit_times = _safe_gateway_submit_times(cycle_dir)
    candidate_generated_at = _candidate_generated_at(snapshot)
    snapshot_created_at = _snapshot_created_at(snapshot)
    snapshot_written_at = (bridge_event or {}).get("created_at")
    hermes_decision_written_at = (loop_event or {}).get("created_at") or decision.get("created_at") or raw_response.get("created_at")
    phoenix_decision_read_at = provider_result.get("created_at")
    risk_governor_at = risk.get("created_at")
    safe_order_gateway_at = gateway.get("created_at")
    executor_called_at = execution.get("created_at") if execution.get("executor_called") else None
    simulated_or_entry_ready_at = submit_times.get("entry_order_submitted_at") or execution.get("created_at")
    candidate_to_decision_latency_ms = _candidate_ms_between(candidate_generated_at, hermes_decision_written_at)
    candidate_to_order_latency_ms = _candidate_ms_between(candidate_generated_at, simulated_or_entry_ready_at)
    report = {
        "trace_id": trace_id,
        "cycle_dir": str(cycle_dir),
        "snapshot_path": result.get("snapshot_path"),
        "decision_path": raw_response.get("_decision_path"),
        "decision_archive_path": archive.get("decision_archive_path"),
        "replay_path": str(cycle_dir / "replay_events.jsonl"),
        "review_report_path": str(cycle_dir / "review_report.jsonl"),
        "readiness_gate_samples": gate_samples,
        "candidate_generated_at": candidate_generated_at,
        "snapshot_created_at": snapshot_created_at,
        "snapshot_written_at": snapshot_written_at,
        "hermes_decision_written_at": hermes_decision_written_at,
        "phoenix_decision_read_at": phoenix_decision_read_at,
        "risk_governor_at": risk_governor_at,
        "safe_order_gateway_at": safe_order_gateway_at,
        "executor_called_at": executor_called_at,
        "entry_order_submitted_at": submit_times.get("entry_order_submitted_at"),
        "stop_tp_submitted_at": submit_times.get("stop_tp_submitted_at"),
        "candidate_to_decision_latency_ms": candidate_to_decision_latency_ms,
        "snapshot_to_decision_latency_ms": _ms_between(snapshot_created_at, hermes_decision_written_at),
        "decision_to_gateway_latency_ms": _ms_between(phoenix_decision_read_at, safe_order_gateway_at),
        "candidate_to_order_latency_ms": candidate_to_order_latency_ms,
        "latency_reliability": _candidate_latency_reliability(
            candidate_to_decision=candidate_to_decision_latency_ms,
            candidate_to_order=candidate_to_order_latency_ms,
        ),
        "provider": provider_result.get("provider"),
        "decision_origin": provider_result.get("decision_origin"),
        "fallback_used": provider_result.get("fallback_used"),
        "source": decision.get("source"),
        "writer": decision.get("writer"),
        "hermes_decision": decision.get("action"),
        "no_trade_reason": decision.get("no_trade_reason") or decision.get("reason") if decision.get("action") == "NO_TRADE" else None,
        "enter_reason": decision.get("reason") if decision.get("action") in OPEN_ACTIONS else None,
        "symbol": decision.get("symbol") or intent.get("symbol"),
        "direction": decision.get("action"),
        "market_regime": regime.get("regime"),
        "market_regime_source": regime.get("market_regime_source") or regime.get("source"),
        "market_regime_confidence": regime.get("market_regime_confidence") if regime.get("market_regime_confidence") is not None else regime.get("confidence"),
        "market_regime_reason": regime.get("market_regime_reason") or regime.get("reason"),
        "candidate_direction": direction_payload.get("candidate_direction"),
        "allowed_direction": direction_payload.get("allowed_direction"),
        "direction_regime_allowed": direction_payload.get("direction_regime_allowed"),
        "direction_regime_reason": direction_payload.get("direction_regime_reason"),
        "entry_quality_filter": decision.get("entry_quality_filter") or selected.get("entry_quality_filter"),
        "entry_quality_allowed": decision.get("entry_quality_allowed")
        if decision.get("entry_quality_allowed") is not None
        else selected.get("entry_quality_allowed"),
        "entry_quality_reason": decision.get("entry_quality_reason") or selected.get("entry_quality_reason"),
        "entry_quality_score": decision.get("entry_quality_score") or selected.get("entry_quality_score"),
        "entry_quality_components": decision.get("entry_quality_components") or selected.get("entry_quality_components"),
        "exchange_filter": {
            "exchange_filter_checked": selected.get("exchange_filter_checked"),
            "symbol_tradeable": selected.get("symbol_tradeable"),
            "micro_notional_feasible": selected.get("micro_notional_feasible"),
            "required_quote_allocation_usdt": selected.get("required_quote_allocation_usdt"),
            "rounded_qty": selected.get("rounded_qty"),
            "min_qty": selected.get("min_qty"),
            "step_size": selected.get("step_size"),
            "min_notional": selected.get("min_notional"),
        },
        "risk_governor_result": risk,
        "safe_order_gateway_result": gateway,
        "execution_result": execution,
        "executor_called": bool(execution.get("executor_called")),
        "entry_order_submitted": bool(execution_payload.get("entry_response")),
        "entry_order_id": _extract_order_id(execution_payload.get("entry_response")),
        "entry_fill_price": lifecycle.get("entry_fill_price") or _payload(execution_payload.get("entry_response")).get("avgPrice"),
        "position_size": lifecycle.get("position_size") or intent.get("quantity") or _payload(execution_payload.get("entry_response")).get("executedQty"),
        "notional": intent.get("notional_usdt") or intent.get("quote_allocation_usdt"),
        "leverage": 2,
        "stop_loss_submitted": bool(execution_payload.get("protective_stop_response")),
        "stop_loss_order_id": _extract_order_id(execution_payload.get("protective_stop_response")),
        "take_profit_submitted": bool(execution_payload.get("take_profit_response")),
        "take_profit_order_id": _extract_order_id(execution_payload.get("take_profit_response")),
        "lifecycle_monitor_auto_handoff": bool(lifecycle),
        "timeout_due_at": lifecycle.get("timeout_due_at"),
        "final_close_reason": lifecycle.get("final_close_reason") or _final_close_reason(result),
        "final_close_order_id": _extract_order_id(lifecycle.get("timeout_close_response") or lifecycle.get("emergency_close_response") or {}),
        "final_close_price": lifecycle.get("close_fill_price"),
        "realized_pnl_usdt": lifecycle.get("realized_pnl_usdt"),
        "roi_pct": lifecycle.get("roi_pct"),
        "fees": lifecycle.get("fees"),
        "holding_time_sec": lifecycle.get("holding_time_sec"),
        "emergency_close": bool(lifecycle.get("emergency_close") or execution_payload.get("emergency_close")),
        "hard_freeze": bool(result.get("frozen") or execution.get("frozen") or lifecycle.get("hard_freeze")),
        "testnet_order_submitted": bool(execution.get("testnet_order_submitted")),
        "mainnet_order_submitted": bool(execution.get("mainnet_order_submitted")),
        "ending_positions": ending_state.get("positions_count"),
        "ending_open_orders": ending_state.get("open_orders_count"),
        "ending_conditional_orders": ending_state.get("conditional_orders_count"),
        "orphan_protective_order": bool((ending_state.get("conditional_orders_count") or 0) > 0),
    }
    _write_json(cycle_dir / "cycle_report.json", report)
    _append_jsonl(session_dir / "cycle_reports.jsonl", report)
    return report


def _minimal_cycle_report_after_generation_error(
    *,
    session_dir: Path,
    cycle_dir: Path,
    result: dict[str, Any],
    report_error: Exception,
    ending_state: dict[str, Any],
) -> dict[str, Any]:
    provider_result = result.get("hermes_provider_result") if isinstance(result.get("hermes_provider_result"), dict) else {}
    decision = provider_result.get("decision") if isinstance(provider_result.get("decision"), dict) else {}
    gateway = result.get("safe_order_gateway_result") if isinstance(result.get("safe_order_gateway_result"), dict) else {}
    risk = result.get("risk_governor_result") if isinstance(result.get("risk_governor_result"), dict) else {}
    execution = result.get("execution_result") if isinstance(result.get("execution_result"), dict) else {}
    execution_payload = execution.get("payload") if isinstance(execution.get("payload"), dict) else {}
    lifecycle = execution_payload.get("lifecycle_result") if isinstance(execution_payload.get("lifecycle_result"), dict) else {}
    intent = execution_payload.get("intent") if isinstance(execution_payload.get("intent"), dict) else gateway.get("execution_intent") or {}
    report = {
        "trace_id": str(result.get("trace_id") or decision.get("trace_id") or ""),
        "cycle_dir": str(cycle_dir),
        "replay_path": str(cycle_dir / "replay_events.jsonl"),
        "review_report_path": str(cycle_dir / "review_report.jsonl"),
        "report_generation_error": f"{type(report_error).__name__}: {report_error}",
        "report_reconstructed_from_execution_result": True,
        "provider": provider_result.get("provider"),
        "decision_origin": provider_result.get("decision_origin"),
        "fallback_used": provider_result.get("fallback_used"),
        "source": decision.get("source"),
        "writer": decision.get("writer"),
        "hermes_decision": decision.get("action"),
        "enter_reason": decision.get("reason") if decision.get("action") in OPEN_ACTIONS else None,
        "symbol": decision.get("symbol") or intent.get("symbol") or lifecycle.get("symbol"),
        "direction": decision.get("action") or lifecycle.get("direction"),
        "risk_governor_result": risk,
        "safe_order_gateway_result": gateway,
        "execution_result": execution,
        "executor_called": bool(execution.get("executor_called")),
        "entry_order_submitted": bool(execution_payload.get("entry_response")),
        "entry_order_id": _extract_order_id(execution_payload.get("entry_response")),
        "entry_fill_price": lifecycle.get("entry_fill_price") or _payload(execution_payload.get("entry_response")).get("avgPrice"),
        "position_size": lifecycle.get("position_size") or intent.get("quantity") or _payload(execution_payload.get("entry_response")).get("executedQty"),
        "notional": intent.get("notional_usdt") or intent.get("quote_allocation_usdt") or _payload(execution_payload.get("entry_response")).get("cumQuote"),
        "leverage": intent.get("leverage") or 2,
        "stop_loss_submitted": bool(execution_payload.get("protective_stop_response")),
        "stop_loss_order_id": _extract_order_id(execution_payload.get("protective_stop_response")),
        "take_profit_submitted": bool(execution_payload.get("take_profit_response")),
        "take_profit_order_id": _extract_order_id(execution_payload.get("take_profit_response")),
        "lifecycle_monitor_auto_handoff": bool(lifecycle),
        "timeout_due_at": lifecycle.get("timeout_due_at"),
        "final_close_reason": lifecycle.get("final_close_reason") or _final_close_reason(result),
        "final_close_order_id": _extract_order_id(lifecycle.get("timeout_close_response") or lifecycle.get("emergency_close_response") or {}),
        "final_close_price": lifecycle.get("close_fill_price"),
        "realized_pnl_usdt": lifecycle.get("realized_pnl_usdt"),
        "roi_pct": lifecycle.get("roi_pct"),
        "fees": lifecycle.get("fees"),
        "holding_time_sec": lifecycle.get("holding_time_sec"),
        "emergency_close": bool(lifecycle.get("emergency_close") or execution_payload.get("emergency_close")),
        "hard_freeze": bool(result.get("frozen") or execution.get("frozen") or lifecycle.get("hard_freeze")),
        "testnet_order_submitted": bool(execution.get("testnet_order_submitted")),
        "mainnet_order_submitted": bool(execution.get("mainnet_order_submitted")),
        "ending_positions": ending_state.get("positions_count"),
        "ending_open_orders": ending_state.get("open_orders_count"),
        "ending_conditional_orders": ending_state.get("conditional_orders_count"),
        "orphan_protective_order": bool((ending_state.get("conditional_orders_count") or 0) > 0),
    }
    _write_json(cycle_dir / "cycle_report.json", report)
    _write_json(cycle_dir / "cycle_report_generation_error.json", {"created_at": _now_iso(), "error": report["report_generation_error"]})
    _append_jsonl(session_dir / "cycle_reports.jsonl", report)
    return report


def _cycle_participation_failures(cycle: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if cycle.get("decision_origin") != "outbox_file":
        failures.append("decision_origin_not_outbox_file")
    if cycle.get("fallback_used") is not False:
        failures.append("fallback_used")
    if cycle.get("source") != "HERMES":
        failures.append("source_not_hermes")
    if cycle.get("writer") != "Hermes Trader Brain":
        failures.append("writer_not_hermes_trader_brain")
    if not cycle.get("decision_path"):
        failures.append("decision_path_missing")
    if cycle.get("hermes_decision") in OPEN_ACTIONS:
        decision = (cycle.get("safe_order_gateway_result") or {}).get("normalized_decision") or {}
        required = {
            "stop_loss": decision.get("stop_loss_pct") is not None or decision.get("stop_loss_price") is not None,
            "take_profit": decision.get("take_profit_pct") is not None or decision.get("take_profit_price") is not None,
            "max_holding": decision.get("max_holding_time_sec") is not None,
            "invalidation": bool(decision.get("invalidation_condition")),
        }
        failures.extend(name + "_missing_from_hermes_enter" for name, ok in required.items() if not ok)
    return failures


def _decision_file_exists(report: dict[str, Any]) -> bool:
    for key in ("decision_path", "decision_archive_path"):
        value = report.get(key)
        if value and Path(str(value)).exists():
            return True
    return False


def _runtime_participation_failures(report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if report.get("decision_origin") != "outbox_file":
        failures.append("decision_origin_not_outbox_file")
    if report.get("fallback_used") is not False:
        failures.append("fallback_used")
    if report.get("source") != "HERMES":
        failures.append("source_not_hermes")
    if report.get("writer") != "Hermes Trader Brain":
        failures.append("writer_not_hermes_trader_brain")
    if not report.get("decision_path") and not report.get("decision_archive_path"):
        failures.append("decision_file_missing")
    if (report.get("decision_path") or report.get("decision_archive_path")) and not _decision_file_exists(report):
        failures.append("decision_file_not_found")
    if report.get("trace_id") and report.get("decision_trace_id") and report.get("trace_id") != report.get("decision_trace_id"):
        failures.append("trace_id_mismatch")
    return failures


def _is_system_not_ready(errors: list[str]) -> bool:
    system_error_names = {
        "trusted_runtime_snapshot_not_true",
        "data_fresh_not_true",
        "websocket_status_unavailable",
        "exchange_status_not_healthy",
        "candidate_state_not_known",
        "candidate_latest_age_sec_over_30",
        "testnet_state_fetch_failed",
        "stage2_runner_residual_process",
        "available_memory_below_100mib",
        "recent_24h_oom_detected",
    }
    return any(error in system_error_names or error.endswith("_not_active") for error in errors)


def _observation_label(sample: dict[str, Any], decision: dict[str, Any], risk: dict[str, Any], gateway: dict[str, Any]) -> str:
    errors = [str(item) for item in sample.get("errors") or []]
    regime = str(sample.get("market_regime") or "").upper()
    action = str(decision.get("action") or "").upper()
    selected_slippage = _safe_float(sample.get("estimated_slippage_bps"))
    reason_text = " ".join(
        str(item or "")
        for item in (
            decision.get("reason"),
            decision.get("no_trade_reason"),
            risk.get("reason"),
            gateway.get("reason"),
        )
    ).lower()
    if _is_system_not_ready(errors):
        return "SYSTEM_NOT_READY"
    if regime == "CHOP":
        return "CHOP_NO_EDGE"
    if "selected_exchange_filter_not_checked" in errors or "selected_symbol_not_tradeable" in errors or "selected_micro_notional_infeasible" in errors:
        return "EXCHANGE_FILTER_REJECT"
    if "candidate_direction_not_trend_aligned" in errors or "direction_regime_not_allowed" in errors:
        return "DIRECTION_MISMATCH"
    if selected_slippage is not None and selected_slippage > 8.0:
        return "HIGH_SLIPPAGE"
    if "slippage" in reason_text or "spread" in reason_text:
        if action == "NO_TRADE":
            return "HIGH_SLIPPAGE"
    if action in OPEN_ACTIONS:
        return "POTENTIAL_ENTER"
    if action == "NO_TRADE":
        return "CORRECT_NO_TRADE"
    return "OTHER"


def _bridge_latency_fields(*, trace_id: str, snapshot: dict[str, Any], provider_result: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    decision = provider_result.get("decision") if isinstance(provider_result.get("decision"), dict) else {}
    raw_response = provider_result.get("raw_response") if isinstance(provider_result.get("raw_response"), dict) else {}
    bridge_event = _latest_event(ROOT / "hermes_logs/hermes_file_bridge_events.jsonl", trace_id=trace_id, event="snapshot_written")
    loop_event = _latest_event(ROOT / "hermes_logs/hermes_decision_loop_events.jsonl", trace_id=trace_id, event="decision_written")
    archive_event = _latest_event(ROOT / "hermes_logs/hermes_file_bridge_events.jsonl", trace_id=trace_id, event="trace_archived")
    risk_rows = _read_jsonl(output_dir / "risk_governor_result.jsonl")
    gateway_rows = _read_jsonl(output_dir / "safe_order_gateway.jsonl")
    execution_rows = _read_jsonl(output_dir / "execution_result.jsonl")
    safe_row = gateway_rows[-1] if gateway_rows else {}
    execution_row = execution_rows[-1] if execution_rows else {}
    candidate_generated_at = _candidate_generated_at(snapshot)
    snapshot_created_at = _snapshot_created_at(snapshot)
    snapshot_written_at = (bridge_event or {}).get("created_at")
    hermes_decision_written_at = (loop_event or {}).get("created_at") or decision.get("created_at") or raw_response.get("created_at")
    phoenix_decision_read_at = provider_result.get("created_at")
    risk_governor_at = (risk_rows[-1] if risk_rows else {}).get("created_at")
    safe_order_gateway_at = safe_row.get("created_at")
    executor_called_at = execution_row.get("created_at") if execution_row.get("executor_called") else None
    simulated_ready_at = execution_row.get("created_at") or safe_order_gateway_at
    candidate_to_decision_latency_ms = _candidate_ms_between(candidate_generated_at, hermes_decision_written_at)
    candidate_to_order_latency_ms = _candidate_ms_between(candidate_generated_at, simulated_ready_at)
    return {
        "candidate_generated_at": candidate_generated_at,
        "snapshot_created_at": snapshot_created_at,
        "snapshot_written_at": snapshot_written_at,
        "hermes_read_at": "unavailable",
        "hermes_decision_started_at": "unavailable",
        "hermes_decision_written_at": hermes_decision_written_at,
        "phoenix_decision_read_at": phoenix_decision_read_at,
        "risk_governor_at": risk_governor_at,
        "safe_order_gateway_at": safe_order_gateway_at,
        "executor_called_at": executor_called_at,
        "entry_order_submitted_at": None,
        "stop_tp_submitted_at": None,
        "candidate_to_decision_latency_ms": candidate_to_decision_latency_ms,
        "snapshot_to_decision_latency_ms": _ms_between(snapshot_created_at, hermes_decision_written_at),
        "decision_to_gateway_latency_ms": _ms_between(phoenix_decision_read_at, safe_order_gateway_at),
        "candidate_to_order_latency_ms": candidate_to_order_latency_ms,
        "latency_reliability": _candidate_latency_reliability(
            candidate_to_decision=candidate_to_decision_latency_ms,
            candidate_to_order=candidate_to_order_latency_ms,
        ),
        "snapshot_path": (bridge_event or {}).get("snapshot_path"),
        "decision_path": (loop_event or {}).get("decision_path") or raw_response.get("_decision_path"),
        "decision_archive_path": (archive_event or {}).get("decision_archive_path"),
    }


def _observation_report(
    *,
    session_dir: Path,
    observation_dir: Path,
    observation_index: int,
    result: dict[str, Any],
    snapshot: dict[str, Any],
    gate_sample: dict[str, Any],
) -> dict[str, Any]:
    trace_id = str(result.get("trace_id") or "")
    provider_result = result.get("hermes_provider_result") if isinstance(result.get("hermes_provider_result"), dict) else {}
    decision = provider_result.get("decision") if isinstance(provider_result.get("decision"), dict) else {}
    raw_response = provider_result.get("raw_response") if isinstance(provider_result.get("raw_response"), dict) else {}
    gateway = result.get("safe_order_gateway_result") if isinstance(result.get("safe_order_gateway_result"), dict) else {}
    risk = result.get("risk_governor_result") if isinstance(result.get("risk_governor_result"), dict) else {}
    execution = result.get("execution_result") if isinstance(result.get("execution_result"), dict) else {}
    selected = _selected_candidate(snapshot)
    regime = snapshot.get("market_regime") if isinstance(snapshot.get("market_regime"), dict) else {}
    direction_payload = _direction_regime_payload(selected, regime)
    latency = _bridge_latency_fields(trace_id=trace_id, snapshot=snapshot, provider_result=provider_result, output_dir=observation_dir)
    label = _observation_label(gate_sample, decision, risk, gateway)
    report = {
        "lane": "observation",
        "observation_index": observation_index,
        "trace_id": trace_id,
        "decision_trace_id": decision.get("trace_id"),
        "observation_dir": str(observation_dir),
        "replay_path": str(observation_dir / "replay_events.jsonl"),
        "review_report_path": str(observation_dir / "review_report.jsonl"),
        **latency,
        "latency_warning": any(
            value is not None and value > limit
            for value, limit in (
                (latency.get("snapshot_to_decision_latency_ms"), 3000.0),
                (latency.get("candidate_to_order_latency_ms"), 20000.0),
            )
        ),
        "provider": provider_result.get("provider"),
        "decision_origin": provider_result.get("decision_origin"),
        "fallback_used": provider_result.get("fallback_used"),
        "decision_file_exists": _decision_file_exists(latency),
        "source": decision.get("source"),
        "writer": decision.get("writer"),
        "hermes_raw_decision": raw_response,
        "hermes_normalized_decision": provider_result.get("normalized_decision"),
        "hermes_decision": decision.get("action"),
        "no_trade_reason": decision.get("no_trade_reason") or (decision.get("reason") if decision.get("action") == "NO_TRADE" else None),
        "enter_reason": decision.get("reason") if decision.get("action") in OPEN_ACTIONS else None,
        "market_regime": regime.get("regime"),
        "market_regime_source": regime.get("market_regime_source") or regime.get("source"),
        "market_regime_confidence": regime.get("market_regime_confidence") if regime.get("market_regime_confidence") is not None else regime.get("confidence"),
        "market_regime_reason": regime.get("market_regime_reason") or regime.get("reason"),
        "candidate_symbol": selected.get("symbol"),
        "candidate_direction": direction_payload.get("candidate_direction"),
        "allowed_direction": direction_payload.get("allowed_direction"),
        "direction_regime_allowed": direction_payload.get("direction_regime_allowed"),
        "direction_regime_reason": direction_payload.get("direction_regime_reason"),
        "entry_quality_filter": decision.get("entry_quality_filter") or selected.get("entry_quality_filter"),
        "entry_quality_allowed": decision.get("entry_quality_allowed")
        if decision.get("entry_quality_allowed") is not None
        else selected.get("entry_quality_allowed"),
        "entry_quality_reason": decision.get("entry_quality_reason") or selected.get("entry_quality_reason"),
        "entry_quality_score": decision.get("entry_quality_score") or selected.get("entry_quality_score"),
        "entry_quality_components": decision.get("entry_quality_components") or selected.get("entry_quality_components"),
        "exchange_filter_checked": selected.get("exchange_filter_checked"),
        "symbol_tradeable": selected.get("symbol_tradeable"),
        "micro_notional_feasible": selected.get("micro_notional_feasible"),
        "required_quote_allocation_usdt": selected.get("required_quote_allocation_usdt"),
        "spread_bps": selected.get("spread_bps"),
        "estimated_slippage_bps": selected.get("estimated_slippage_bps"),
        "risk_governor_result": risk,
        "safe_order_gateway_result": gateway,
        "execution_result": execution,
        "observation_label": label,
        "execution_gate_passed": bool(gate_sample.get("passed")),
        "execution_gate_errors": gate_sample.get("errors") or [],
        "executor_called": bool(execution.get("executor_called")),
        "order_submitted": bool(execution.get("order_submitted")),
        "testnet_order_submitted": bool(execution.get("testnet_order_submitted")),
        "mainnet_order_submitted": bool(execution.get("mainnet_order_submitted")),
    }
    _write_json(observation_dir / "observation_report.json", report)
    _append_jsonl(session_dir / "observation_samples.jsonl", report)
    return report


def _all_decision_samples(observation_reports: list[dict[str, Any]], cycle_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in observation_reports:
        rows.append(
            {
                "lane": "observation",
                "trace_id": item.get("trace_id"),
                "symbol": item.get("candidate_symbol"),
                "market_regime": item.get("market_regime"),
                "decision": item.get("hermes_decision"),
                "reason": item.get("no_trade_reason") or item.get("enter_reason"),
                "decision_origin": item.get("decision_origin"),
                "fallback_used": item.get("fallback_used"),
                "source": item.get("source"),
                "writer": item.get("writer"),
                "observation_label": item.get("observation_label"),
                "entry_quality_allowed": item.get("entry_quality_allowed"),
                "entry_quality_reason": item.get("entry_quality_reason"),
            }
        )
    for item in cycle_reports:
        rows.append(
            {
                "lane": "execution",
                "trace_id": item.get("trace_id"),
                "symbol": item.get("symbol"),
                "market_regime": item.get("market_regime"),
                "decision": item.get("hermes_decision"),
                "reason": item.get("no_trade_reason") or item.get("enter_reason"),
                "decision_origin": item.get("decision_origin"),
                "fallback_used": item.get("fallback_used"),
                "source": item.get("source"),
                "writer": item.get("writer"),
                "final_close_reason": item.get("final_close_reason"),
                "testnet_order_submitted": item.get("testnet_order_submitted"),
                "entry_quality_allowed": item.get("entry_quality_allowed"),
                "entry_quality_reason": item.get("entry_quality_reason"),
            }
        )
    return rows


def _write_jsonl_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _reason_summary(rows: list[dict[str, Any]], *, decision: str | None = None, labels: set[str] | None = None) -> dict[str, Any]:
    filtered = [
        row
        for row in rows
        if (decision is None or row.get("decision") == decision)
        and (labels is None or row.get("observation_label") in labels)
    ]
    counts = Counter(str(row.get("reason") or "unknown")[:500] for row in filtered)
    return {"total": len(filtered), "top_reasons": counts.most_common(20)}


def _latency_summary_for_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_to_decision = [item.get("candidate_to_decision_latency_ms") for item in reports]
    candidate_to_order = [item.get("candidate_to_order_latency_ms") for item in reports]
    snapshot_to_decision = [item.get("snapshot_to_decision_latency_ms") for item in reports]
    decision_to_gateway = [item.get("decision_to_gateway_latency_ms") for item in reports]
    return {
        "candidate_to_decision_latency_ms": _stats(candidate_to_decision),
        "snapshot_to_decision_latency_ms": _stats(snapshot_to_decision),
        "decision_to_gateway_latency_ms": _stats(decision_to_gateway),
        "candidate_to_order_latency_ms": _stats(candidate_to_order),
        "reliable_latency_summary": {
            "snapshot_to_decision_latency_ms": _stats(snapshot_to_decision),
            "decision_to_gateway_latency_ms": _stats(decision_to_gateway),
        },
        "unreliable_candidate_latency_summary": {
            "candidate_to_decision_latency_ms": _stats(candidate_to_decision),
            "candidate_to_order_latency_ms": _stats(candidate_to_order),
            "unavailable_candidate_to_decision_count": sum(1 for item in candidate_to_decision if item is None),
            "unavailable_candidate_to_order_count": sum(1 for item in candidate_to_order if item is None),
            "note": "candidate-derived latency is excluded from SLO when timestamps are missing or negative.",
        },
        "latency_warning_count": sum(1 for item in reports if item.get("latency_warning")),
    }


def _write_analyst_package(
    *,
    session_dir: Path,
    observation_reports: list[dict[str, Any]],
    cycle_reports: list[dict[str, Any]],
) -> str:
    package_dir = session_dir / "analyst_review_package"
    package_dir.mkdir(parents=True, exist_ok=True)
    decision_rows = _all_decision_samples(observation_reports, cycle_reports)
    executed = [item for item in cycle_reports if item.get("testnet_order_submitted")]
    _write_jsonl_rows(package_dir / "all_observation_samples.jsonl", observation_reports)
    _write_jsonl_rows(package_dir / "all_hermes_decisions.jsonl", decision_rows)
    _write_jsonl_rows(package_dir / "all_executed_trades.jsonl", executed)
    _write_json(package_dir / "no_trade_reasons_summary.json", _reason_summary(decision_rows, decision="NO_TRADE"))
    _write_json(
        package_dir / "rejected_enter_reasons_summary.json",
        _reason_summary(decision_rows, labels={"DIRECTION_MISMATCH", "EXCHANGE_FILTER_REJECT", "HIGH_SLIPPAGE", "SYSTEM_NOT_READY"}),
    )
    _write_json(package_dir / "latency_summary.json", _latency_summary_for_reports([*observation_reports, *cycle_reports]))
    _write_json(
        package_dir / "top_symbols.json",
        {
            "observed": Counter(str(item.get("candidate_symbol") or "UNKNOWN") for item in observation_reports).most_common(20),
            "rejected": Counter(
                str(item.get("candidate_symbol") or "UNKNOWN")
                for item in observation_reports
                if item.get("observation_label") not in {"POTENTIAL_ENTER", "CORRECT_NO_TRADE"}
            ).most_common(20),
            "traded": Counter(str(item.get("symbol") or "UNKNOWN") for item in executed).most_common(20),
        },
    )
    candidates = [
        {
            "trace_id": item.get("trace_id"),
            "label": item.get("observation_label"),
            "symbol": item.get("candidate_symbol"),
            "market_regime": item.get("market_regime"),
            "decision": item.get("hermes_decision"),
            "reason": item.get("no_trade_reason") or item.get("enter_reason"),
            "replay_path": item.get("replay_path"),
        }
        for item in observation_reports[:200]
    ]
    _write_json(package_dir / "candidate_examples_for_manual_review.json", {"examples": candidates})
    _write_json(
        package_dir / "package_index.json",
        {
            "created_at": _now_iso(),
            "session_dir": str(session_dir),
            "observation_samples_path": str(package_dir / "all_observation_samples.jsonl"),
            "hermes_decisions_path": str(package_dir / "all_hermes_decisions.jsonl"),
            "executed_trades_path": str(package_dir / "all_executed_trades.jsonl"),
            "no_trade_reasons_summary_path": str(package_dir / "no_trade_reasons_summary.json"),
            "rejected_enter_reasons_summary_path": str(package_dir / "rejected_enter_reasons_summary.json"),
            "latency_summary_path": str(package_dir / "latency_summary.json"),
            "top_symbols_path": str(package_dir / "top_symbols.json"),
            "candidate_examples_path": str(package_dir / "candidate_examples_for_manual_review.json"),
        },
    )
    return str(package_dir)


def _summarize_session(
    *,
    session_dir: Path,
    started_at: str,
    ended_at: str,
    stop_reason: str,
    readiness_samples: int,
    gate_attempts: int,
    cancelled_by_gate: int,
    cycle_reports: list[dict[str, Any]],
    ending_state: dict[str, Any],
) -> dict[str, Any]:
    real_trades = [c for c in cycle_reports if c.get("testnet_order_submitted")]
    realized = [_safe_float(c.get("realized_pnl_usdt")) for c in real_trades]
    roi = [_safe_float(c.get("roi_pct")) for c in real_trades]
    fees = [_safe_float(c.get("fees")) for c in real_trades]
    holding = [_safe_float(c.get("holding_time_sec")) for c in real_trades]
    pnl_clean = [v for v in realized if v is not None]
    roi_clean = [v for v in roi if v is not None]
    fees_clean = [v for v in fees if v is not None]
    holding_clean = [v for v in holding if v is not None]
    wins = [c for c in real_trades if (_safe_float(c.get("realized_pnl_usdt")) or 0.0) > 0]
    best_trade = max(real_trades, key=lambda c: _safe_float(c.get("realized_pnl_usdt")) or 0.0, default=None)
    worst_trade = min(real_trades, key=lambda c: _safe_float(c.get("realized_pnl_usdt")) or 0.0, default=None)
    latency = {
        "candidate_to_decision_latency_ms": _stats([c.get("candidate_to_decision_latency_ms") for c in cycle_reports]),
        "snapshot_to_decision_latency_ms": _stats([c.get("snapshot_to_decision_latency_ms") for c in cycle_reports]),
        "decision_to_gateway_latency_ms": _stats([c.get("decision_to_gateway_latency_ms") for c in cycle_reports]),
        "candidate_to_order_latency_ms": _stats([c.get("candidate_to_order_latency_ms") for c in cycle_reports]),
    }
    decision_counts = {
        "NO_TRADE": sum(1 for c in cycle_reports if c.get("hermes_decision") == "NO_TRADE"),
        "ENTER_LONG": sum(1 for c in cycle_reports if c.get("hermes_decision") == "ENTER_LONG"),
        "ENTER_SHORT": sum(1 for c in cycle_reports if c.get("hermes_decision") == "ENTER_SHORT"),
    }
    close_counts = {
        "CLOSED_BY_TP": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_TP"),
        "CLOSED_BY_STOP": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_STOP"),
        "CLOSED_BY_TIMEOUT": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_TIMEOUT"),
        "CLOSED_BY_NO_FOLLOW_THROUGH": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_NO_FOLLOW_THROUGH"),
        "CLOSED_BY_INVALIDATION": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_INVALIDATION"),
        "EMERGENCY_CLOSED": sum(1 for c in real_trades if c.get("final_close_reason") == "EMERGENCY_CLOSED" or c.get("emergency_close")),
        "HARD_FREEZE": sum(1 for c in cycle_reports if c.get("hard_freeze")),
    }
    no_trade_examples = [
        {"trace_id": c.get("trace_id"), "symbol": c.get("symbol"), "reason": c.get("no_trade_reason")}
        for c in cycle_reports
        if c.get("hermes_decision") == "NO_TRADE"
    ][:5]
    bad_enter_examples = [
        {
            "trace_id": c.get("trace_id"),
            "symbol": c.get("symbol"),
            "reason": (c.get("risk_governor_result") or {}).get("reason"),
            "final_close_reason": c.get("final_close_reason"),
            "realized_pnl_usdt": c.get("realized_pnl_usdt"),
        }
        for c in cycle_reports
        if c.get("hermes_decision") in OPEN_ACTIONS
        and ((not c.get("testnet_order_submitted")) or c.get("hard_freeze") or ((_safe_float(c.get("realized_pnl_usdt")) or 0.0) < 0))
    ][:5]
    good_enter_examples = [
        {
            "trace_id": c.get("trace_id"),
            "symbol": c.get("symbol"),
            "enter_reason": c.get("enter_reason"),
            "final_close_reason": c.get("final_close_reason"),
            "realized_pnl_usdt": c.get("realized_pnl_usdt"),
            "roi_pct": c.get("roi_pct"),
        }
        for c in cycle_reports
        if c.get("testnet_order_submitted") and not c.get("hard_freeze") and ((_safe_float(c.get("realized_pnl_usdt")) or 0.0) >= 0)
    ][:5]
    if not cycle_reports:
        one_sentence = "Fast gate never produced a trend-aligned ready window, so Hermes was not invoked and no trainable trade signal was collected."
    elif len(real_trades) < 2:
        one_sentence = "Hermes produced runtime decisions through the real outbox path; training value is limited until more ENTER samples close cleanly."
    else:
        one_sentence = "Hermes has begun producing trainable testnet trade outcomes through the real runtime path."
    duration = _ms_between(started_at, ended_at)
    report = {
        "session_run_dir": str(session_dir),
        "start_time": started_at,
        "end_time": ended_at,
        "total_duration_sec": round((duration or 0.0) / 1000.0, 3),
        "stop_reason": stop_reason,
        "total_readiness_samples": readiness_samples,
        "total_gate_attempts": gate_attempts,
        "cancelled_by_gate_count": cancelled_by_gate,
        "hermes_decision_cycles": len(cycle_reports),
        **decision_counts,
        "real_testnet_trades": len(real_trades),
        **close_counts,
        "total_realized_pnl": round(sum(pnl_clean), 8) if pnl_clean else 0.0,
        "win_rate": round(len(wins) / len(real_trades), 4) if real_trades else None,
        "avg_roi": round(mean(roi_clean), 8) if roi_clean else None,
        "fees": round(sum(fees_clean), 8) if fees_clean else None,
        "avg_holding_time": round(mean(holding_clean), 3) if holding_clean else None,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "symbols_traded": sorted({str(c.get("symbol")) for c in real_trades if c.get("symbol")}),
        "market_regimes_traded": sorted({str(c.get("market_regime")) for c in real_trades if c.get("market_regime")}),
        "good_no_trade_examples": no_trade_examples,
        "bad_enter_examples": bad_enter_examples,
        "good_enter_examples": good_enter_examples,
        "latency_summary": latency,
        "ending_positions": ending_state.get("positions_count"),
        "ending_open_orders": ending_state.get("open_orders_count"),
        "ending_conditional_orders": ending_state.get("conditional_orders_count"),
        "mainnet_order_submitted": any(bool(c.get("mainnet_order_submitted")) for c in cycle_reports),
        "cleanup": False,
        "auto_promotion": False,
        "recommend_analyst_review": True,
        "one_sentence_conclusion": one_sentence,
        "cycle_report_paths": [str(Path(c["cycle_dir"]) / "cycle_report.json") for c in cycle_reports],
    }
    _write_json(session_dir / "stage2_testnet_exploration_session_report.json", report)
    return report


def _summarize_session_v03(
    *,
    session_dir: Path,
    started_at: str,
    ended_at: str,
    stop_reason: str,
    readiness_samples: int,
    gate_attempts: int,
    cancelled_by_gate: int,
    observation_reports: list[dict[str, Any]],
    cycle_reports: list[dict[str, Any]],
    ending_state: dict[str, Any],
) -> dict[str, Any]:
    decision_rows = _all_decision_samples(observation_reports, cycle_reports)
    real_trades = [c for c in cycle_reports if c.get("testnet_order_submitted")]
    all_reports = [*observation_reports, *cycle_reports]
    realized = [_safe_float(c.get("realized_pnl_usdt")) for c in real_trades]
    roi = [_safe_float(c.get("roi_pct")) for c in real_trades]
    fees = [_safe_float(c.get("fees")) for c in real_trades]
    holding = [_safe_float(c.get("holding_time_sec")) for c in real_trades]
    pnl_clean = [v for v in realized if v is not None]
    roi_clean = [v for v in roi if v is not None]
    fees_clean = [v for v in fees if v is not None]
    holding_clean = [v for v in holding if v is not None]
    wins = [c for c in real_trades if (_safe_float(c.get("realized_pnl_usdt")) or 0.0) > 0]
    best_trade = max(real_trades, key=lambda c: _safe_float(c.get("realized_pnl_usdt")) or 0.0, default=None)
    worst_trade = min(real_trades, key=lambda c: _safe_float(c.get("realized_pnl_usdt")) or 0.0, default=None)
    correct_no_trade = [
        {
            "trace_id": item.get("trace_id"),
            "symbol": item.get("candidate_symbol"),
            "regime": item.get("market_regime"),
            "reason": item.get("no_trade_reason"),
            "label": item.get("observation_label"),
        }
        for item in observation_reports
        if item.get("hermes_decision") == "NO_TRADE" and item.get("observation_label") in {"CORRECT_NO_TRADE", "CHOP_NO_EDGE", "DIRECTION_MISMATCH", "EXCHANGE_FILTER_REJECT", "SYSTEM_NOT_READY"}
    ][:8]
    questionable_no_trade = [
        {
            "trace_id": item.get("trace_id"),
            "symbol": item.get("candidate_symbol"),
            "regime": item.get("market_regime"),
            "reason": item.get("no_trade_reason"),
        }
        for item in observation_reports
        if item.get("hermes_decision") == "NO_TRADE"
        and item.get("execution_gate_passed")
        and item.get("direction_regime_allowed") is True
        and item.get("micro_notional_feasible") is True
    ][:8]
    good_enter_examples = [
        {
            "lane": "execution" if item in cycle_reports else "observation",
            "trace_id": item.get("trace_id"),
            "symbol": item.get("symbol") or item.get("candidate_symbol"),
            "regime": item.get("market_regime"),
            "reason": item.get("enter_reason"),
            "final_close_reason": item.get("final_close_reason"),
            "realized_pnl_usdt": item.get("realized_pnl_usdt"),
            "roi_pct": item.get("roi_pct"),
        }
        for item in [*cycle_reports, *observation_reports]
        if item.get("hermes_decision") in OPEN_ACTIONS
        and (item.get("observation_label") == "POTENTIAL_ENTER" or item.get("testnet_order_submitted"))
        and not item.get("hard_freeze")
    ][:8]
    bad_enter_examples = [
        {
            "lane": "execution" if item in cycle_reports else "observation",
            "trace_id": item.get("trace_id"),
            "symbol": item.get("symbol") or item.get("candidate_symbol"),
            "regime": item.get("market_regime"),
            "reason": item.get("enter_reason"),
            "label": item.get("observation_label"),
            "final_close_reason": item.get("final_close_reason"),
            "realized_pnl_usdt": item.get("realized_pnl_usdt"),
        }
        for item in [*observation_reports, *cycle_reports]
        if item.get("hermes_decision") in OPEN_ACTIONS
        and (
            item.get("observation_label") in {"DIRECTION_MISMATCH", "EXCHANGE_FILTER_REJECT", "HIGH_SLIPPAGE", "SYSTEM_NOT_READY"}
            or item.get("hard_freeze")
            or ((_safe_float(item.get("realized_pnl_usdt")) or 0.0) < 0)
        )
    ][:8]
    gate_cancel_examples = [
        {
            "trace_id": item.get("trace_id"),
            "symbol": item.get("candidate_symbol"),
            "regime": item.get("market_regime"),
            "errors": item.get("execution_gate_errors"),
            "label": item.get("observation_label"),
        }
        for item in observation_reports
        if not item.get("execution_gate_passed")
    ][:8]
    participation_failures = [
        {"trace_id": item.get("trace_id"), "failures": _runtime_participation_failures(item)}
        for item in [*observation_reports, *cycle_reports]
        if _runtime_participation_failures(item)
    ]
    analyst_package_path = _write_analyst_package(
        session_dir=session_dir,
        observation_reports=observation_reports,
        cycle_reports=cycle_reports,
    )
    duration = _ms_between(started_at, ended_at)
    report = {
        "session_run_dir": str(session_dir),
        "start_time": started_at,
        "end_time": ended_at,
        "total_duration_sec": round((duration or 0.0) / 1000.0, 3),
        "stop_reason": stop_reason,
        "total_observation_samples": len(observation_reports),
        "total_hermes_decision_samples": len(decision_rows),
        "total_readiness_samples": readiness_samples,
        "total_gate_attempts": gate_attempts,
        "cancelled_by_gate_count": cancelled_by_gate,
        "NO_TRADE": sum(1 for item in decision_rows if item.get("decision") == "NO_TRADE"),
        "ENTER_LONG_observation": sum(1 for item in observation_reports if item.get("hermes_decision") == "ENTER_LONG"),
        "ENTER_SHORT_observation": sum(1 for item in observation_reports if item.get("hermes_decision") == "ENTER_SHORT"),
        "real_testnet_trades": len(real_trades),
        "ENTER_LONG_executed": sum(1 for item in real_trades if item.get("hermes_decision") == "ENTER_LONG"),
        "ENTER_SHORT_executed": sum(1 for item in real_trades if item.get("hermes_decision") == "ENTER_SHORT"),
        "CLOSED_BY_TP": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_TP"),
        "CLOSED_BY_STOP": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_STOP"),
        "CLOSED_BY_TIMEOUT": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_TIMEOUT"),
        "CLOSED_BY_NO_FOLLOW_THROUGH": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_NO_FOLLOW_THROUGH"),
        "CLOSED_BY_INVALIDATION": sum(1 for c in real_trades if c.get("final_close_reason") == "CLOSED_BY_INVALIDATION"),
        "EMERGENCY_CLOSED": sum(1 for c in real_trades if c.get("final_close_reason") == "EMERGENCY_CLOSED" or c.get("emergency_close")),
        "HARD_FREEZE": sum(1 for c in cycle_reports if c.get("hard_freeze")),
        "total_realized_pnl": round(sum(pnl_clean), 8) if pnl_clean else 0.0,
        "win_rate": round(len(wins) / len(real_trades), 4) if real_trades else None,
        "avg_roi": round(mean(roi_clean), 8) if roi_clean else None,
        "total_fees": round(sum(fees_clean), 8) if fees_clean else None,
        "avg_holding_time": round(mean(holding_clean), 3) if holding_clean else None,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "symbols_observed": sorted({str(item.get("candidate_symbol")) for item in observation_reports if item.get("candidate_symbol")}),
        "symbols_traded": sorted({str(item.get("symbol")) for item in real_trades if item.get("symbol")}),
        "market_regimes_observed": dict(Counter(str(item.get("market_regime") or "UNKNOWN") for item in observation_reports)),
        "market_regimes_traded": sorted({str(item.get("market_regime")) for item in real_trades if item.get("market_regime")}),
        "correct_no_trade_examples": correct_no_trade,
        "questionable_no_trade_examples": questionable_no_trade,
        "good_enter_examples": good_enter_examples,
        "bad_enter_examples": bad_enter_examples,
        "gate_cancel_examples": gate_cancel_examples,
        "latency_summary": _latency_summary_for_reports(all_reports),
        "hermes_participation_summary": {
            "total_samples": len(all_reports),
            "failures": participation_failures,
            "decision_origin_outbox_file": sum(1 for item in all_reports if item.get("decision_origin") == "outbox_file"),
            "fallback_used_count": sum(1 for item in all_reports if item.get("fallback_used") is not False),
            "source_hermes_count": sum(1 for item in all_reports if item.get("source") == "HERMES"),
            "writer_hermes_trader_brain_count": sum(1 for item in all_reports if item.get("writer") == "Hermes Trader Brain"),
        },
        "telegram_hermes_runtime_participation": False,
        "ending_positions": ending_state.get("positions_count"),
        "ending_open_orders": ending_state.get("open_orders_count"),
        "ending_conditional_orders": ending_state.get("conditional_orders_count"),
        "mainnet_order_submitted": any(bool(item.get("mainnet_order_submitted")) for item in all_reports),
        "cleanup": False,
        "auto_promotion": False,
        "analyst_review_package_path": analyst_package_path,
        "one_sentence_conclusion": (
            "Hermes produced reviewable testnet exploration samples through the real outbox runtime path; "
            "Analyst should judge edge from the observation and executed-trade package, not from a single trade."
        ),
    }
    _write_json(session_dir / "stage2_testnet_exploration_v03_session_report.json", report)
    return report


def _real_trade_count(cycle_reports: list[dict[str, Any]]) -> int:
    return sum(1 for item in cycle_reports if item.get("testnet_order_submitted"))


def _target_samples_reached(observation_reports: list[dict[str, Any]], cycle_reports: list[dict[str, Any]]) -> bool:
    return len(observation_reports) >= TARGET_OBSERVATION_SAMPLES and len(_all_decision_samples(observation_reports, cycle_reports)) >= TARGET_HERMES_DECISION_SAMPLES


def _observation_no_order_failures(observation: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if observation.get("executor_called"):
        failures.append("observation_executor_called")
    if observation.get("order_submitted"):
        failures.append("observation_order_submitted")
    if observation.get("testnet_order_submitted"):
        failures.append("observation_testnet_order_submitted")
    if observation.get("mainnet_order_submitted"):
        failures.append("observation_mainnet_order_submitted")
    execution = observation.get("execution_result") if isinstance(observation.get("execution_result"), dict) else {}
    if execution.get("dry_run") is not True:
        failures.append("observation_not_dry_run")
    return failures


def _append_gate_attempt(
    session_dir: Path,
    *,
    attempt_index: int,
    status: str,
    gate_samples: list[dict[str, Any]],
    failed_gate_index: int | None = None,
    errors: list[str] | None = None,
) -> None:
    _append_jsonl(
        session_dir / "gate_attempts.jsonl",
        {
            "attempt_index": attempt_index,
            "status": status,
            "failed_gate_index": failed_gate_index,
            "errors": errors or [],
            "sample_count": len(gate_samples),
            "gate_samples": gate_samples,
            "timestamp": _now_iso(),
        },
    )


async def main() -> int:
    session_dir = RUN_ROOT / _run_id()
    session_dir.mkdir(parents=True, exist_ok=True)
    pid_path = session_dir / "runner.pid"
    pid_path.write_text(str(os.getpid()) + "\n", encoding="utf-8")
    started_at = _now_iso()
    deadline = time.monotonic() + SESSION_DURATION_SEC
    config = _read_json(CONFIG_PATH)
    config_errors = validate_stage2_micro_config(config)
    if config.get("quote_allocation_usdt") != 5.0:
        config_errors.append("quote_allocation_usdt_must_equal_5")
    if config.get("leverage") != 2:
        config_errors.append("leverage_must_equal_2")
    if config.get("max_quote_allocation_usdt") != 10.0:
        config_errors.append("max_quote_allocation_usdt_must_equal_10")
    token_file = Path(config.get("dashboard_token_file") or ROOT / ".phoenix_dashboard_readonly_token")
    dashboard_token = token_file.read_text(encoding="utf-8").strip() if token_file.exists() else None
    capabilities = verify_testnet_executor_capabilities()
    exchange_info = await _fetch_exchange_info()
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
    readiness_samples = 0
    gate_attempts = 0
    cancelled_by_gate = 0
    consecutive_system_anomalies = 0
    stop_reason = "session_duration_elapsed"
    stop_details: list[str] = []
    observation_reports: list[dict[str, Any]] = []
    cycle_reports: list[dict[str, Any]] = []

    _write_json(
        session_dir / "session_config_snapshot.json",
        {
            "started_at": started_at,
            "config_path": str(CONFIG_PATH),
            "config": config,
            "config_errors": config_errors,
            "gate_interval_sec": GATE_INTERVAL_SEC,
            "observation_interval_sec": OBSERVATION_INTERVAL_SEC,
            "gates_required": GATES_REQUIRED,
            "session_duration_sec": SESSION_DURATION_SEC,
            "max_real_testnet_trades": MAX_REAL_TESTNET_TRADES,
            "target_observation_samples": TARGET_OBSERVATION_SAMPLES,
            "target_hermes_decision_samples": TARGET_HERMES_DECISION_SAMPLES,
            "capabilities": capabilities,
        },
    )

    while time.monotonic() < deadline and _real_trade_count(cycle_reports) < MAX_REAL_TESTNET_TRADES:
        if _target_samples_reached(observation_reports, cycle_reports):
            stop_reason = "sample_targets_reached"
            break
        gate_attempts += 1
        gate_samples: list[dict[str, Any]] = []
        snapshot_for_cycle: dict[str, Any] | None = None
        attempt_failed = False
        stop_now = False
        for gate_index in range(1, GATES_REQUIRED + 1):
            if time.monotonic() >= deadline:
                stop_reason = "session_duration_elapsed"
                stop_now = True
                break
            sample, snapshot = await _collect_gate_sample(
                session_dir=session_dir,
                attempt_index=gate_attempts,
                gate_index=gate_index,
                config=config,
                config_errors=config_errors,
                capabilities=capabilities,
                dashboard_token=dashboard_token,
                exchange_info=exchange_info,
            )
            readiness_samples += 1
            gate_samples.append(sample)
            snapshot_for_cycle = snapshot

            observation_index = len(observation_reports) + 1
            observation_dir = session_dir / "observations" / f"obs_{observation_index:04d}"
            observation_dir.mkdir(parents=True, exist_ok=True)
            observation_result = await run_trader_cycle_async(
                snapshot_builder=lambda snapshot=snapshot: snapshot,
                decision_provider=provider,
                output_dir=observation_dir,
                dry_run=True,
                environment={
                    "runtime_mode": "DRY_RUN",
                    "env": "testnet",
                    "PHOENIX_MAINNET_LIVE_ENABLED": "false",
                    "PHOENIX_LIVE_TRADING_ENABLED": "false",
                    "PHOENIX_PROMOTION_ALLOWED": "false",
                    "require_trusted_runtime_snapshot": False,
                    "quote_allocation_usdt": 5.0,
                    "stage2_observation_lane": True,
                    "stage2_micro_order": False,
                    "cleanup": False,
                    "auto_promotion": False,
                },
                risk_config=risk_config_from_trial_config(config),
                executor_callback=None,
                file_bridge=bridge,
            )
            observation = _observation_report(
                session_dir=session_dir,
                observation_dir=observation_dir,
                observation_index=observation_index,
                result=observation_result,
                snapshot=snapshot,
                gate_sample=sample,
            )
            observation_reports.append(observation)

            participation_failures = _runtime_participation_failures(observation)
            no_order_failures = _observation_no_order_failures(observation)
            if participation_failures or no_order_failures:
                stop_reason = "hermes_runtime_participation_failed" if participation_failures else "observation_no_order_invariant_failed"
                stop_details = [*participation_failures, *no_order_failures]
                stop_now = True
                break

            if not sample["passed"]:
                cancelled_by_gate += 1
                attempt_failed = True
                consecutive_system_anomalies = consecutive_system_anomalies + 1 if sample.get("system_anomaly") else 0
                _append_gate_attempt(
                    session_dir,
                    attempt_index=gate_attempts,
                    status="CANCELLED_BY_GATE",
                    gate_samples=gate_samples,
                    failed_gate_index=gate_index,
                    errors=sample.get("errors") or [],
                )
                break
            consecutive_system_anomalies = 0
            if gate_index < GATES_REQUIRED:
                await asyncio.sleep(GATE_INTERVAL_SEC)
        if stop_now:
            break
        if consecutive_system_anomalies >= MAX_CONSECUTIVE_SYSTEM_ANOMALIES:
            stop_reason = "consecutive_system_anomalies"
            break
        if attempt_failed:
            await asyncio.sleep(OBSERVATION_INTERVAL_SEC)
            continue
        if snapshot_for_cycle is None:
            stop_reason = "snapshot_missing_after_gate"
            break
        if len(gate_samples) < GATES_REQUIRED:
            stop_reason = "incomplete_gate_sequence"
            break
        if deadline - time.monotonic() < NO_NEW_CYCLE_WITHIN_SEC:
            cancelled_by_gate += 1
            _append_gate_attempt(
                session_dir,
                attempt_index=gate_attempts,
                status="CANCELLED_BY_DEADLINE_GUARD",
                gate_samples=gate_samples,
                errors=["deadline_guard_no_new_execution"],
            )
            await asyncio.sleep(OBSERVATION_INTERVAL_SEC)
            continue

        _append_gate_attempt(
            session_dir,
            attempt_index=gate_attempts,
            status="PASSED_EXECUTION_READY",
            gate_samples=gate_samples,
        )

        cycle_index = len(cycle_reports) + 1
        cycle_dir = session_dir / "cycles" / f"cycle_{cycle_index:02d}"
        cycle_dir.mkdir(parents=True, exist_ok=True)
        callback = TestnetExecutorCallback(
            snapshot=snapshot_for_cycle,
            output_dir=cycle_dir,
            environment_name="testnet",
            quote_allocation_usdt=5.0,
            leverage=2,
            lifecycle_monitor_enabled=bool(config.get("lifecycle_monitor_enabled")),
            lifecycle_poll_sec=float(config.get("lifecycle_poll_sec") or 5.0),
            lifecycle_post_close_wait_sec=float(config.get("lifecycle_post_close_wait_sec") or 60.0),
        )
        result = await run_trader_cycle_async(
            snapshot_builder=lambda snapshot=snapshot_for_cycle: snapshot,
            decision_provider=provider,
            output_dir=cycle_dir,
            dry_run=False,
            environment={
                "runtime_mode": "TESTNET_LIVE",
                "env": "testnet",
                "PHOENIX_MAINNET_LIVE_ENABLED": "false",
                "PHOENIX_LIVE_TRADING_ENABLED": "false",
                "PHOENIX_PROMOTION_ALLOWED": "false",
                "require_trusted_runtime_snapshot": True,
                "quote_allocation_usdt": 5.0,
                "stage2_micro_order": True,
                "cleanup": False,
                "auto_promotion": False,
            },
            risk_config=risk_config_from_trial_config(config),
            executor_callback=callback,
            file_bridge=bridge,
        )
        symbol = (((result.get("hermes_provider_result") or {}).get("decision") or {}).get("symbol")) if isinstance(result.get("hermes_provider_result"), dict) else None
        ending_state = await _fetch_testnet_state(str(symbol) if symbol else None)
        report_generation_failed = False
        try:
            cycle = _cycle_report(
                session_dir=session_dir,
                cycle_dir=cycle_dir,
                result=result,
                snapshot=snapshot_for_cycle,
                gate_samples=gate_samples,
                ending_state=ending_state,
            )
        except Exception as exc:  # noqa: BLE001
            report_generation_failed = True
            cycle = _minimal_cycle_report_after_generation_error(
                session_dir=session_dir,
                cycle_dir=cycle_dir,
                result=result,
                report_error=exc,
                ending_state=ending_state,
            )
        cycle_reports.append(cycle)
        if report_generation_failed:
            stop_reason = "cycle_report_generation_failed"
            break
        participation_failures = [*_runtime_participation_failures(cycle), *_cycle_participation_failures(cycle)]
        if participation_failures:
            stop_reason = "hermes_runtime_participation_failed:" + ",".join(participation_failures)
            break
        if cycle.get("mainnet_order_submitted"):
            stop_reason = "mainnet_order_submitted"
            break
        if cycle.get("hard_freeze"):
            stop_reason = "hard_freeze"
            break
        if cycle.get("final_close_reason") == "EMERGENCY_CLOSED" and (ending_state.get("positions_count") or 0) > 0:
            stop_reason = "emergency_close_failed"
            break
        if (ending_state.get("positions_count") or 0) != 0 or (ending_state.get("open_orders_count") or 0) != 0 or (ending_state.get("conditional_orders_count") or 0) != 0:
            stop_reason = "post_cycle_reconciliation_not_flat"
            break
        await asyncio.sleep(OBSERVATION_INTERVAL_SEC)
    else:
        if _real_trade_count(cycle_reports) >= MAX_REAL_TESTNET_TRADES:
            stop_reason = "max_real_testnet_trades_reached"

    ending_state = await _fetch_testnet_state()
    report = _summarize_session_v03(
        session_dir=session_dir,
        started_at=started_at,
        ended_at=_now_iso(),
        stop_reason=stop_reason,
        readiness_samples=readiness_samples,
        gate_attempts=gate_attempts,
        cancelled_by_gate=cancelled_by_gate,
        observation_reports=observation_reports,
        cycle_reports=cycle_reports,
        ending_state=ending_state,
    )
    if stop_details:
        report["stop_details"] = stop_details
        _write_json(session_dir / "stage2_testnet_exploration_v03_session_report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not report.get("mainnet_order_submitted") else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
