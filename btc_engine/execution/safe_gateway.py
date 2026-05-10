"""Safe order gateway adapter for legacy BTC engine execution paths."""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any, Callable

from btc_engine.config import get_env, get_rest_base_url
from btc_engine.types import ExecutionPlan
from phoenix.safe_order_gateway import (
    SafeOrderGatewayBlocked,
    build_gateway_snapshot,
    submit_sync_order_intent,
)


def btc_runtime_environment() -> dict[str, Any]:
    explicit_env = str(get_env("BTC_BINANCE_ENV", "") or os.environ.get("PHOENIX_BINANCE_ENV", "") or "").strip().lower()
    base_url = get_rest_base_url().lower()
    env = explicit_env or ("testnet" if "testnet" in base_url else "prod")
    return {
        "runtime_mode": os.environ.get("PHOENIX_RUNTIME_MODE") or "TESTNET_LIVE",
        "env": env,
        "PHOENIX_BINANCE_ENV": env,
        "PHOENIX_LIVE_TRADING_ENABLED": os.environ.get("PHOENIX_LIVE_TRADING_ENABLED", "false"),
        "PHOENIX_MAINNET_LIVE_ENABLED": os.environ.get("PHOENIX_MAINNET_LIVE_ENABLED", "false"),
        "PHOENIX_ENABLE_MAINNET_LIVE": os.environ.get("PHOENIX_ENABLE_MAINNET_LIVE", "false"),
        "PHOENIX_PROMOTION_ALLOWED": os.environ.get("PHOENIX_PROMOTION_ALLOWED", "false"),
        "PHOENIX_EXECUTION_MODE": os.environ.get("PHOENIX_EXECUTION_MODE", ""),
    }


def submit_btc_client_order(
    client: Any,
    payload: dict[str, Any],
    *,
    source: str,
    purpose: str,
    plan: ExecutionPlan | None = None,
    executor: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = dict(payload)
    purpose_name = str(purpose or "order").lower()
    symbol = str(payload.get("symbol") or (plan.symbol if plan is not None else "") or "BTCUSDT").upper()
    side = str(payload.get("side") or (plan.side if plan is not None else "") or "").upper()
    reduce_like = str(payload.get("reduceOnly") or payload.get("closePosition") or "").lower() == "true"
    positions = []
    if reduce_like or purpose_name in {"exit", "emergency", "protection", "take_profit", "reattach"}:
        positions.append({"symbol": symbol, "side": "LONG" if side == "SELL" else "SHORT", "protection_status": "healthy"})

    # Legacy BTC engine entry does not have an audited emergency close handoff.
    # Keep the path hard-blocked until the full entry/protection chain is migrated.
    entry_blocked = purpose_name == "entry"
    snapshot = build_gateway_snapshot(
        symbol=symbol,
        side=side,
        positions=positions,
        data_fresh=True,
        websocket_status="healthy",
        exchange_status="healthy",
        position_state="known",
        stop_protection_status="healthy",
        candidate_state="known",
        protective_stop_path_available=not entry_blocked,
        emergency_close_available=not entry_blocked,
    )
    order_intent = asdict(plan) if plan is not None else {"symbol": symbol, "side": side, "margin_type": "ISOLATED"}

    def _execute(_execution_intent: dict[str, Any]) -> dict[str, Any]:
        if executor is not None:
            return executor(payload)
        return client.new_order(payload)

    gateway = submit_sync_order_intent(
        payload,
        snapshot,
        btc_runtime_environment(),
        source,
        dry_run=False,
        executor_callback=_execute,
        extra_context={
            "purpose": purpose_name,
            "order_intent": order_intent,
            "protective_stop_path_available": not entry_blocked,
            "emergency_close_path_available": not entry_blocked,
            "reason": "legacy_btc_engine_order_path_requires_safe_gateway",
        },
    )
    if not gateway.approved:
        raise SafeOrderGatewayBlocked(gateway)
    result = (gateway.execution_result or {}).get("payload")
    if not isinstance(result, dict):
        raise RuntimeError("BTC safe order gateway approved but returned no payload.")
    return result


def submit_btc_client_algo_order(
    client: Any,
    payload: dict[str, Any],
    *,
    source: str,
    purpose: str,
    plan: ExecutionPlan | None = None,
) -> dict[str, Any]:
    return submit_btc_client_order(
        client,
        payload,
        source=source,
        purpose=purpose,
        plan=plan,
        executor=lambda gated_payload: client.new_algo_order(gated_payload),
    )
