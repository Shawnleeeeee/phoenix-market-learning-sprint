from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

from phoenix.binance_futures import BinanceFuturesClient
from phoenix.config import load_credentials, load_proxy_settings, resolve_environment
from phoenix.exchange_filter_feasibility import evaluate_micro_notional_feasibility, find_symbol_info
from phoenix.hermes_decision_provider import provider_from_name
from phoenix.hermes_file_bridge import HermesFileBridge
from phoenix.hermes_trader_mode import (
    _enrich_stage2_snapshot_exchange_filters,
    risk_config_from_trial_config,
    run_trader_cycle_async,
    validate_stage2_micro_config,
)
from phoenix.safe_order_gateway import submit_order_intent
from phoenix.testnet_executor_callback import verify_testnet_executor_capabilities
from phoenix.trader_snapshot_runtime import build_dashboard_api_trader_snapshot


ROOT = Path("/opt/phoenix-testnet")
CONFIG_PATH = ROOT / "configs/hermes_trader_stage2_micro.testnet.json"
RUN_ROOT = ROOT / "hermes_logs/exchange_filter_micro_no_order_smoke"
STAGE2_PATTERNS = ("stage2_micro", "stage2_preflight", "phoenix.hermes_trader_mode")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def now_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def service_state(name: str) -> str:
    proc = subprocess.run(["systemctl", "is-active", name], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc.stdout.strip() or proc.stderr.strip()


def disk_memory_status() -> dict[str, Any]:
    usage = shutil.disk_usage(ROOT)
    meminfo: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, value = line.split(":", 1)
        meminfo[key] = int(value.strip().split()[0])
    return {
        "disk": {
            "free_gb": round(usage.free / 1024**3, 3),
            "used_pct": round((usage.used / usage.total) * 100.0, 3) if usage.total else None,
        },
        "memory": {
            "mem_available_mb": round(meminfo.get("MemAvailable", 0) / 1024, 3),
            "mem_total_mb": round(meminfo.get("MemTotal", 0) / 1024, 3),
        },
        "swap": {
            "swap_free_mb": round(meminfo.get("SwapFree", 0) / 1024, 3),
            "swap_total_mb": round(meminfo.get("SwapTotal", 0) / 1024, 3),
        },
    }


def residual_stage2_processes() -> list[dict[str, Any]]:
    ignore_pids = {os.getpid(), os.getppid()}
    matches: list[dict[str, Any]] = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit() or int(pid) in ignore_pids:
            continue
        try:
            raw = (
                Path(f"/proc/{pid}/cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode("utf-8", errors="replace")
                .strip()
            )
        except Exception:
            continue
        if not raw or "tmp_exchange_filter_no_order_smoke" in raw:
            continue
        if any(pattern in raw for pattern in STAGE2_PATTERNS):
            matches.append({"pid": int(pid), "cmdline": raw[:500]})
    return matches


def open_position_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    open_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            amount = abs(float(row.get("positionAmt") or row.get("position_amount") or 0.0))
        except (TypeError, ValueError):
            amount = 0.0
        if amount > 0:
            open_rows.append(row)
    return open_rows


async def fetch_testnet_state() -> dict[str, Any]:
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
        positions = await client.position_information_v3()
        open_orders = await client.open_orders()
        conditional_orders = await client.open_conditional_orders()
        exchange_info = await client.exchange_info()
    position_rows = positions if isinstance(positions, list) else []
    open_orders_rows = open_orders if isinstance(open_orders, list) else []
    conditional_rows = conditional_orders if isinstance(conditional_orders, list) else []
    return {
        "positions_count": len(open_position_rows(position_rows)),
        "open_orders_count": len(open_orders_rows),
        "conditional_orders_count": len(conditional_rows),
        "exchange_info": exchange_info,
    }


def candidate_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = snapshot.get("top_candidates") if isinstance(snapshot.get("top_candidates"), list) else []
    feasible = [row for row in rows if row.get("micro_notional_feasible") is True]
    infeasible = [row for row in rows if row.get("micro_notional_feasible") is False]
    return {
        "top_candidates_count": len(rows),
        "feasible_symbols": [row.get("symbol") for row in feasible],
        "infeasible_symbols": [
            {
                "symbol": row.get("symbol"),
                "rounded_qty": row.get("rounded_qty"),
                "min_qty": row.get("min_qty"),
                "min_notional": row.get("min_notional"),
                "required_quote_allocation_usdt": row.get("required_quote_allocation_usdt"),
                "infeasible_reason": row.get("infeasible_reason"),
            }
            for row in infeasible
        ],
    }


def avax_like_probe(exchange_info: dict[str, Any]) -> dict[str, Any]:
    symbol_info = find_symbol_info(exchange_info, "AVAXUSDT")
    result = evaluate_micro_notional_feasibility(
        symbol_info,
        current_price=24.0,
        configured_quote_allocation_usdt=5.0,
        configured_leverage=2,
        max_quote_allocation_usdt=10.0,
        symbol="AVAXUSDT",
    )
    return result.to_dict()


async def forced_avax_risk_probe(snapshot: dict[str, Any], exchange_info: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    calls: list[dict[str, Any]] = []
    feasibility = avax_like_probe(exchange_info)
    probe_snapshot = dict(snapshot)
    probe_snapshot["top_candidates"] = [
        {
            "symbol": "AVAXUSDT",
            "bias": "LONG",
            "setup_type": "QUICK_TRADE",
            "score": 90.0,
            "current_price": 24.0,
            "spread_bps": 0.5,
            "estimated_slippage_bps": 1.0,
            "liquidity_ok": True,
            "suggested_stop_pct": 0.6,
            "suggested_tp_pct": 1.2,
            "max_holding_time_sec": 300,
            "invalidation_hint": "forced risk probe only",
            **feasibility,
        }
    ]
    gateway = await submit_order_intent(
        {
            "action": "ENTER_LONG",
            "symbol": "AVAXUSDT",
            "trade_type": "QUICK_TRADE",
            "confidence": 0.8,
            "reason": "forced AVAX exchange filter risk probe; no order allowed",
            "entry_price_hint": 24.0,
            "stop_loss_pct": 0.6,
            "take_profit_pct": 1.2,
            "max_holding_time_sec": 300,
            "invalidation_condition": "forced risk probe only",
            "source": "HERMES",
        },
        probe_snapshot,
        environment={
            "runtime_mode": "TESTNET_LIVE",
            "env": "testnet",
            "PHOENIX_MAINNET_LIVE_ENABLED": "false",
            "PHOENIX_LIVE_TRADING_ENABLED": "false",
            "PHOENIX_PROMOTION_ALLOWED": "false",
            "require_trusted_runtime_snapshot": True,
            "quote_allocation_usdt": config.get("quote_allocation_usdt"),
            "stage2_micro_order": True,
        },
        source="HERMES",
        dry_run=False,
        executor_callback=lambda payload: calls.append(payload) or {"order_submitted": True},
        risk_config=risk_config_from_trial_config(config),
    )
    result = gateway.to_dict()
    execution = result.get("execution_result") or {}
    return {
        "approved": result.get("approved"),
        "result_type": result.get("result_type"),
        "blocked_by": result.get("blocked_by"),
        "risk_reason": (result.get("risk_governor_result") or {}).get("reason"),
        "executor_callback_calls": len(calls),
        "executor_called": execution.get("executor_called"),
        "order_submitted": execution.get("order_submitted"),
        "testnet_order_submitted": execution.get("testnet_order_submitted"),
        "mainnet_order_submitted": execution.get("mainnet_order_submitted"),
        "frozen": execution.get("frozen"),
        "feasibility": feasibility,
    }


async def main() -> int:
    config = load_json(CONFIG_PATH)
    config_errors = validate_stage2_micro_config(config)
    run_dir = RUN_ROOT / f"exchange_filter_micro_no_order_{now_id()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    capabilities = verify_testnet_executor_capabilities()
    token = Path(config["dashboard_token_file"]).read_text(encoding="utf-8").strip()
    snapshot = build_dashboard_api_trader_snapshot(
        root=ROOT,
        dashboard_snapshot_url=config["dashboard_snapshot_url"],
        dashboard_bearer_token=token,
        dashboard_timeout_sec=float(config.get("dashboard_timeout_sec") or 30),
        max_candidates=10,
        max_open_positions=int(config.get("max_open_positions") or 1),
        protective_stop_path_available=bool(capabilities.get("protective_stop_path_available", False)),
        take_profit_path_available=bool(capabilities.get("take_profit_path_available", False)),
        emergency_close_available=bool(capabilities.get("emergency_close_available", False)),
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
    state = await fetch_testnet_state()
    bridge = HermesFileBridge(
        inbox_dir=config["hermes_inbox"],
        outbox_dir=config["hermes_outbox"],
        archive_dir=config["hermes_archive"],
        log_dir=config["hermes_logs"],
    )
    provider = provider_from_name(
        "file",
        decision_file=bridge.outbox_dir,
        decision_timeout_sec=float(config.get("decision_timeout_sec") or 60),
        decision_poll_interval_sec=float(config.get("decision_poll_interval_sec") or 0.5),
    )
    result = await run_trader_cycle_async(
        snapshot_builder=lambda snapshot=snapshot: snapshot,
        decision_provider=provider,
        output_dir=run_dir,
        dry_run=True,
        environment={
            "runtime_mode": "TESTNET_LIVE",
            "env": "testnet",
            "PHOENIX_MAINNET_LIVE_ENABLED": "false",
            "PHOENIX_LIVE_TRADING_ENABLED": "false",
            "PHOENIX_PROMOTION_ALLOWED": "false",
            "require_trusted_runtime_snapshot": True,
            "quote_allocation_usdt": config.get("quote_allocation_usdt"),
            "stage2_micro_order": True,
        },
        risk_config=risk_config_from_trial_config(config),
        executor_callback=None,
        file_bridge=bridge,
    )
    status = snapshot.get("system_status") if isinstance(snapshot.get("system_status"), dict) else {}
    market = snapshot.get("market_regime") if isinstance(snapshot.get("market_regime"), dict) else {}
    execution = result.get("execution_result") if isinstance(result.get("execution_result"), dict) else {}
    report = {
        "run_dir": str(run_dir),
        "report_path": str(run_dir / "exchange_filter_micro_no_order_smoke.json"),
        "replay_path": str(run_dir / "replay_events.jsonl"),
        "review_report_path": str(run_dir / "review_report.jsonl"),
        "config_path": str(CONFIG_PATH),
        "config_errors": config_errors,
        "config": {
            "mode": config.get("mode"),
            "provider": config.get("provider"),
            "decision_policy": config.get("decision_policy"),
            "mainnet_live": config.get("mainnet_live"),
            "quote_allocation_usdt": config.get("quote_allocation_usdt"),
            "max_quote_allocation_usdt": config.get("max_quote_allocation_usdt"),
            "leverage": config.get("leverage"),
            "max_open_positions": config.get("max_open_positions"),
        },
        "services": {
            "hermes-decision-loop.service": service_state("hermes-decision-loop.service"),
            "phoenix-candidate-producer.service": service_state("phoenix-candidate-producer.service"),
            "phoenix-dashboard-snapshot-api.service": service_state("phoenix-dashboard-snapshot-api.service"),
        },
        "resources": disk_memory_status(),
        "runtime_snapshot": {
            "trusted_runtime_snapshot": status.get("trusted_runtime_snapshot"),
            "data_fresh": status.get("data_fresh"),
            "websocket_status": status.get("websocket_status"),
            "exchange_status": status.get("exchange_status"),
            "candidate_state": status.get("candidate_state"),
            "candidate_latest_age_sec": status.get("candidate_latest_age_sec"),
            "exchange_filter_checked": status.get("exchange_filter_checked"),
            "exchange_filter_checked_count": status.get("exchange_filter_checked_count"),
            "micro_notional_feasible_count": status.get("micro_notional_feasible_count"),
            "micro_notional_infeasible_count": status.get("micro_notional_infeasible_count"),
            "market_regime": market.get("regime"),
            "market_regime_source": market.get("market_regime_source") or market.get("source"),
            "market_regime_confidence": market.get("market_regime_confidence") or market.get("confidence"),
            "market_regime_reason": market.get("market_regime_reason") or market.get("reason"),
        },
        "candidate_summary": candidate_summary(snapshot),
        "avax_like_probe": avax_like_probe(state["exchange_info"]),
        "forced_avax_risk_probe": await forced_avax_risk_probe(snapshot, state["exchange_info"], config),
        "hermes_decision": (result.get("hermes_provider_result") or {}).get("decision"),
        "provider_result": {
            "decision_origin": (result.get("hermes_provider_result") or {}).get("decision_origin"),
            "fallback_used": (result.get("hermes_provider_result") or {}).get("fallback_used"),
            "source": ((result.get("hermes_provider_result") or {}).get("decision") or {}).get("source"),
            "writer": ((result.get("hermes_provider_result") or {}).get("decision") or {}).get("writer"),
        },
        "risk_governor_result": result.get("risk_governor_result"),
        "safe_order_gateway_result_type": (result.get("safe_order_gateway_result") or {}).get("result_type"),
        "executor_called": execution.get("executor_called"),
        "testnet_order_submitted": execution.get("testnet_order_submitted"),
        "mainnet_order_submitted": execution.get("mainnet_order_submitted"),
        "order_submitted": execution.get("order_submitted"),
        "hard_freeze": bool(execution.get("frozen")),
        "cleanup": False,
        "auto_promotion": False,
        "positions": state["positions_count"],
        "open_orders": state["open_orders_count"],
        "conditional_orders": state["conditional_orders_count"],
        "stage2_runner_residual_processes": residual_stage2_processes(),
    }
    (run_dir / "exchange_filter_micro_no_order_smoke.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
