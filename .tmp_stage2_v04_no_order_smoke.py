from __future__ import annotations

import asyncio
import importlib.util
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

from phoenix.binance_futures import BinanceFuturesClient
from phoenix.config import load_credentials
from phoenix.hermes_decision_loop import build_calibration_decision
from phoenix.risk_governor import RiskGovernorConfig
from phoenix.safe_order_gateway import submit_order_intent


ROOT = Path("/opt/phoenix-testnet")
RUN_ROOT = ROOT / "hermes_logs/stage2_v04_entry_quality_no_order_smoke"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_id() -> str:
    return datetime.now(timezone.utc).strftime("smoke_%Y%m%dT%H%M%SZ")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def open_positions_count(rows: Any) -> int:
    if not isinstance(rows, list):
        return 0
    count = 0
    for row in rows:
        try:
            if abs(float(row.get("positionAmt") or row.get("position_amt") or 0.0)) > 0:
                count += 1
        except (TypeError, ValueError):
            continue
    return count


def residual_stage2_runner_count() -> int:
    try:
        output = subprocess.check_output(["ps", "-eo", "pid,args"], text=True)
    except Exception:
        return -1
    patterns = ("stage2_micro_cycle", "stage2_testnet_exploration", ".tmp_stage2_exploration", "hermes_trader_mode")
    count = 0
    for line in output.splitlines():
        if "stage2_v04_no_order_smoke" in line:
            continue
        if any(pattern in line for pattern in patterns):
            count += 1
    return count


def latency_semantics_probe() -> dict[str, Any]:
    module_path = ROOT / ".tmp_stage2_exploration_v03.py"
    spec = importlib.util.spec_from_file_location("stage2_exploration_v03_latency_probe", module_path)
    if spec is None or spec.loader is None:
        return {"available": False, "reason": "stage2_exploration_v03_loader_unavailable"}
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    candidate_to_decision = module._candidate_ms_between(
        "2026-05-15T10:56:40+00:00",
        "2026-05-15T10:56:30+00:00",
    )
    candidate_to_order = module._candidate_ms_between(
        "2026-05-15T10:56:40+00:00",
        "2026-05-15T10:56:33+00:00",
    )
    reliability = module._candidate_latency_reliability(
        candidate_to_decision=candidate_to_decision,
        candidate_to_order=candidate_to_order,
    )
    return {
        "available": True,
        "candidate_to_decision_latency_ms": candidate_to_decision,
        "candidate_to_order_latency_ms": candidate_to_order,
        "snapshot_to_decision_latency_ms": module._ms_between(
            "2026-05-15T10:56:29+00:00",
            "2026-05-15T10:56:30+00:00",
        ),
        "decision_to_gateway_latency_ms": module._ms_between(
            "2026-05-15T10:56:31+00:00",
            "2026-05-15T10:56:33+00:00",
        ),
        "latency_reliability": reliability,
    }


async def fetch_testnet_state() -> dict[str, Any]:
    credentials = load_credentials(required=True)
    if credentials.environment.name not in {"testnet", "demo"}:
        raise RuntimeError(f"refusing_non_testnet_credentials:{credentials.environment.name}")
    async with aiohttp.ClientSession() as session:
        client = BinanceFuturesClient(session, credentials.environment, credentials)
        await client.sync_server_time()
        positions = await client.position_information_v3()
        open_orders = await client.open_orders()
        conditional_orders = await client.open_conditional_orders()
    return {
        "positions_count": open_positions_count(positions),
        "open_orders_count": len(open_orders if isinstance(open_orders, list) else []),
        "conditional_orders_count": len(conditional_orders if isinstance(conditional_orders, list) else []),
    }


def smoke_snapshot(trace_id: str) -> dict[str, Any]:
    created_at = now_iso()
    return {
        "trace_id": trace_id,
        "created_at": created_at,
        "system_status": {
            "trace_id": trace_id,
            "source": "runtime",
            "snapshot_source": "runtime",
            "snapshot_time": created_at,
            "trusted_runtime_snapshot": True,
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
            "candidate_state": "known",
            "position_state": "known",
            "stop_protection_status": "healthy",
            "protective_stop_path_available": True,
            "take_profit_path_available": True,
            "take_profit_order_supported": True,
            "emergency_close_available": True,
            "account_state_source": "signed_account",
            "position_state_source": "signed_positions",
            "protective_stop_capability_source": "verified_code_path:stage2_v04_smoke",
            "take_profit_capability_source": "verified_code_path:stage2_v04_smoke",
            "emergency_close_capability_source": "verified_code_path:stage2_v04_smoke",
        },
        "account_risk": {
            "trading_allowed": True,
            "daily_pnl_pct": 0.0,
            "daily_loss_remaining_pct": 3.0,
            "open_positions_count": 0,
            "max_open_positions": 1,
            "loss_streak": 0,
            "cooldown_active": False,
        },
        "current_positions": [],
        "market_regime": {
            "regime": "TREND_DOWN",
            "direction_lock": "SHORT_ONLY_OR_NO_TRADE",
            "source": "stage2_v04_no_order_smoke",
            "confidence": 0.82,
            "reason": "synthetic no-order smoke trend-down fixture",
            "btc_trend_1m": -0.14,
            "btc_trend_5m": -0.31,
            "eth_trend_1m": 0.01,
            "eth_trend_5m": -0.07,
        },
        "top_candidates": [
            {
                "symbol": "SOLUSDT",
                "bias": "SHORT",
                "setup_type": "QUICK_TRADE",
                "score": 18.0,
                "current_price": 142.42,
                "spread_bps": 0.7,
                "estimated_slippage_bps": 1.1,
                "estimated_fee_bps": 4.0,
                "liquidity_ok": True,
                "price_change_1m_pct": -1.20,
                "price_change_5m_pct": -2.10,
                "momentum_follow_through": True,
                "suggested_stop_pct": 0.6,
                "suggested_tp_pct": 1.2,
                "max_holding_time_sec": 300,
                "invalidation_hint": "failed rebound invalidates short",
                "invalidation_distance_bps": 60.0,
                "exchange_filter_checked": True,
                "symbol_tradeable": True,
                "micro_notional_feasible": True,
                "required_quote_allocation_usdt": 5.0,
                "configured_quote_allocation_usdt": 5.0,
                "configured_leverage": 2,
                "max_quote_allocation_usdt": 10.0,
                "rounded_qty": 0.07,
                "min_qty": 0.01,
                "step_size": 0.01,
                "min_notional": 5.0,
            }
        ],
    }


async def main() -> int:
    run_dir = RUN_ROOT / run_id()
    run_dir.mkdir(parents=True, exist_ok=True)
    trace_id = run_dir.name
    state_before = await fetch_testnet_state()
    snapshot = smoke_snapshot(trace_id)
    snapshot["account_risk"]["open_positions_count"] = state_before["positions_count"]
    write_json(run_dir / "snapshot.json", snapshot)
    decision = build_calibration_decision(snapshot, trace_id=trace_id)
    write_json(run_dir / f"decision_{trace_id}.json", decision)
    append_jsonl(run_dir / "replay_events.jsonl", {"event": "snapshot", "snapshot": snapshot, "created_at": now_iso()})
    append_jsonl(run_dir / "replay_events.jsonl", {"event": "hermes_decision_normalized", "decision": decision, "created_at": now_iso()})

    executor_called = False

    async def unexpected_executor(payload: dict[str, Any]) -> dict[str, Any]:
        nonlocal executor_called
        executor_called = True
        return {
            "order_submitted": False,
            "testnet_order_submitted": False,
            "mainnet_order_submitted": False,
            "status": "unexpected_executor_called",
            "frozen": True,
            "can_continue": False,
        }

    gateway = await submit_order_intent(
        decision,
        snapshot,
        {
            "runtime_mode": "TESTNET_LIVE",
            "env": "testnet",
            "require_trusted_runtime_snapshot": True,
            "quote_allocation_usdt": 5.0,
            "stage2_micro_order": True,
            "stage2_entry_quality_required": True,
            "mainnet_live": False,
            "PHOENIX_MAINNET_LIVE_ENABLED": "false",
            "PHOENIX_LIVE_TRADING_ENABLED": "false",
            "PHOENIX_PROMOTION_ALLOWED": "false",
        },
        source="HERMES",
        dry_run=False,
        executor_callback=unexpected_executor,
        risk_config=RiskGovernorConfig(
            require_known_market_regime=True,
            require_take_profit=True,
            require_max_holding_time=True,
            require_invalidation_condition=True,
            require_explicit_quote_allocation=True,
            require_entry_quality_filter=True,
            max_quote_allocation_usdt=10.0,
        ),
        log_dir=run_dir / "safe_order_gateway",
        audit_log_path=run_dir / "safe_order_gateway.jsonl",
    )
    gateway_payload = gateway.to_dict()
    append_jsonl(run_dir / "replay_events.jsonl", {"event": "risk_governor_result", "payload": gateway_payload["risk_governor_result"], "created_at": now_iso()})
    append_jsonl(run_dir / "replay_events.jsonl", {"event": "safe_order_gateway_result", "payload": gateway_payload, "created_at": now_iso()})
    append_jsonl(run_dir / "replay_events.jsonl", {"event": "execution_result", "payload": gateway_payload["execution_result"], "created_at": now_iso()})
    append_jsonl(run_dir / "review_report.jsonl", gateway_payload.get("review_report") or {})
    append_jsonl(run_dir / "replay_events.jsonl", {"event": "review_report", "payload": gateway_payload.get("review_report"), "created_at": now_iso()})
    state_after = await fetch_testnet_state()
    latency_probe = latency_semantics_probe()
    append_jsonl(run_dir / "replay_events.jsonl", {"event": "latency_semantics_probe", "payload": latency_probe, "created_at": now_iso()})
    artifact_text = json.dumps({"snapshot": snapshot, "decision": decision, "gateway": gateway_payload}, ensure_ascii=False)
    bad_wording_markers = ["dry-run 校准", "dry-run calibration", "dry-run 只"]
    report = {
        "run_dir": str(run_dir),
        "report_path": str(run_dir / "stage2_v04_no_order_smoke_report.json"),
        "replay_path": str(run_dir / "replay_events.jsonl"),
        "review_report_path": str(run_dir / "review_report.jsonl"),
        "bad_wording_count": sum(artifact_text.count(marker) for marker in bad_wording_markers),
        "bad_wording_markers": [marker for marker in bad_wording_markers if marker in artifact_text],
        "hermes_decision": decision.get("action"),
        "entry_quality_filter": decision.get("entry_quality_filter"),
        "entry_quality_allowed": decision.get("entry_quality_allowed"),
        "entry_quality_reason": decision.get("entry_quality_reason"),
        "entry_quality_components": decision.get("entry_quality_components"),
        "latency_semantics": latency_probe,
        "result_type": gateway.result_type,
        "frozen": gateway_payload["execution_result"].get("frozen"),
        "can_continue": gateway_payload["execution_result"].get("can_continue"),
        "executor_called": executor_called or bool(gateway_payload["execution_result"].get("executor_called")),
        "testnet_order_submitted": bool(gateway_payload["execution_result"].get("testnet_order_submitted")),
        "mainnet_order_submitted": bool(gateway_payload["execution_result"].get("mainnet_order_submitted")),
        "cleanup": False,
        "auto_promotion": False,
        "positions_before": state_before["positions_count"],
        "open_orders_before": state_before["open_orders_count"],
        "conditional_orders_before": state_before["conditional_orders_count"],
        "positions_after": state_after["positions_count"],
        "open_orders_after": state_after["open_orders_count"],
        "conditional_orders_after": state_after["conditional_orders_count"],
        "stage2_runner_residual_process": residual_stage2_runner_count(),
    }
    write_json(Path(report["report_path"]), report)
    failures = []
    for key in ("executor_called", "testnet_order_submitted", "mainnet_order_submitted", "cleanup", "auto_promotion"):
        if report.get(key):
            failures.append(key)
    if report["positions_after"] != 0 or report["open_orders_after"] != 0 or report["conditional_orders_after"] != 0:
        failures.append("ending_state_not_flat")
    if report["bad_wording_count"] != 0:
        failures.append("bad_wording_present")
    if report["result_type"] != "soft_reject":
        failures.append("expected_soft_reject")
    if report["entry_quality_allowed"] is not False:
        failures.append("expected_entry_quality_reject")
    if latency_probe.get("available") is not True:
        failures.append("latency_probe_unavailable")
    if latency_probe.get("candidate_to_decision_latency_ms") is not None:
        failures.append("candidate_to_decision_negative_not_unavailable")
    if latency_probe.get("candidate_to_order_latency_ms") is not None:
        failures.append("candidate_to_order_negative_not_unavailable")
    if latency_probe.get("snapshot_to_decision_latency_ms") != 1000.0:
        failures.append("snapshot_to_decision_reliable_latency_missing")
    if latency_probe.get("decision_to_gateway_latency_ms") != 2000.0:
        failures.append("decision_to_gateway_reliable_latency_missing")
    report["failures"] = failures
    write_json(Path(report["report_path"]), report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
