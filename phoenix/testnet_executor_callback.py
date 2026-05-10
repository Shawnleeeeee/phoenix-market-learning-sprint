from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

import aiohttp

from phoenix.binance_futures import BinanceFuturesClient
from phoenix.config import load_credentials, load_execution_settings, load_proxy_settings, resolve_environment
from phoenix.executor import PhoenixExecutor
from phoenix.models import OrderInstruction, TradeIntent
from phoenix.safe_order_gateway import submit_binance_order_intent
from phoenix.trader_snapshot_runtime import _compact_account_state  # lightweight internal normalizer


class TestnetExecutorCallback:
    """Phoenix testnet executor callback used after safe_order_gateway approval."""

    def __init__(
        self,
        *,
        snapshot: dict[str, Any],
        output_dir: str | Path,
        environment_name: str = "testnet",
        quote_allocation_usdt: float | None = None,
        leverage: int = 2,
        client_factory: Callable[[aiohttp.ClientSession], BinanceFuturesClient] | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.output_dir = Path(output_dir)
        self.environment_name = environment_name
        self.quote_allocation_usdt = quote_allocation_usdt
        self.leverage = min(max(1, int(leverage or 1)), 2)
        self.client_factory = client_factory

    async def __call__(self, execution_intent: dict[str, Any]) -> dict[str, Any]:
        action = str(execution_intent.get("action") or "").upper()
        if action not in {"ENTER_LONG", "ENTER_SHORT"}:
            return {
                "order_submitted": False,
                "status": "no_order_action",
                "reason": f"{action or 'UNKNOWN'} does not require testnet entry execution",
            }
        if not execution_intent.get("required_protective_orders"):
            return {
                "order_submitted": False,
                "status": "blocked_before_executor",
                "reason": "required_protective_orders_missing",
            }
        trust_reasons = validate_trusted_runtime_snapshot(self.snapshot)
        if trust_reasons:
            return _frozen_executor_result(
                "manual_snapshot_not_allowed_for_testnet_order_mode"
                if "manual_snapshot_not_allowed_for_testnet_order_mode" in trust_reasons
                else ",".join(trust_reasons),
                executor_stage="snapshot_trust_check",
            )

        environment = resolve_environment(self.environment_name)
        if environment.name not in {"testnet", "demo"}:
            raise RuntimeError("Hermes Trader Mode executor callback is testnet/demo only.")

        credentials = None if self.client_factory is not None else load_credentials(required=True)
        settings = load_execution_settings()
        raw_decision = execution_intent.get("raw_intent") if isinstance(execution_intent.get("raw_intent"), dict) else {}
        settings = replace(
            settings,
            leverage=self.leverage,
            max_open_positions=1,
            initial_stop_loss_pct=float(raw_decision.get("stop_loss_pct") or settings.initial_stop_loss_pct),
            take_profit_pct=float(raw_decision.get("take_profit_pct") or settings.take_profit_pct or 0.0),
        )
        timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            futures = self.client_factory(session) if self.client_factory else BinanceFuturesClient(
                session=session,
                environment=environment,
                credentials=credentials,
                proxy_settings=load_proxy_settings(),
            )
            try:
                account_payload, positions_payload, exchange_payload = await _fetch_required_account_position_exchange_state(futures)
            except Exception as exc:  # noqa: BLE001
                return _frozen_executor_result(str(exc), executor_stage="signed_state_preflight")
            executor = PhoenixExecutor(futures_client=futures, settings=settings)
            intent = await executor.build_trade_intent(
                symbol=str(execution_intent.get("symbol") or raw_decision.get("symbol") or "").upper(),
                side=str(execution_intent.get("side") or "").upper(),
                quote_allocation_usdt=self.quote_allocation_usdt,
                entry_price=_optional_float(raw_decision.get("entry_price_hint")),
                leverage=self.leverage,
            )
            account_api_mode = await futures.get_account_api_mode()
            plan = executor.build_order_plan(intent, account_api_mode=account_api_mode)
            entry = _find_instruction(plan, "entry_market")
            protective_stop = _find_instruction(plan, "initial_protective_stop")
            if entry is None:
                return _frozen_executor_result("entry_order_path_unavailable", executor_stage="order_plan_preflight")
            if protective_stop is None:
                return _frozen_executor_result("protective_stop_path_unavailable", executor_stage="order_plan_preflight")
            initial_status = self.snapshot.get("system_status") if isinstance(self.snapshot, dict) else {}
            initial_status = initial_status if isinstance(initial_status, dict) else {}
            protective_stop_verified = protective_stop is not None
            emergency_close_verified = initial_status.get("emergency_close_available") is True and _verified_capability(
                initial_status.get("emergency_close_capability_source")
            )
            risk_log_path = self.output_dir / "testnet_executor_safe_gateway.jsonl"
            entry_snapshot = _trusted_snapshot_with_signed_state(
                self.snapshot,
                account_payload=account_payload,
                positions_payload=positions_payload,
                exchange_payload=exchange_payload,
                protective_stop_path_available=protective_stop_verified,
                emergency_close_available=emergency_close_verified,
                protective_stop_capability_source="testnet_executor_order_plan_preflight",
                emergency_close_capability_source=str(initial_status.get("emergency_close_capability_source") or "unverified"),
            )

            entry_response = await submit_binance_order_intent(
                futures,
                entry.payload,
                snapshot=entry_snapshot,
                environment=_testnet_gateway_environment(environment.name),
                source="hermes_trader_mode:testnet_entry",
                purpose="entry",
                endpoint=entry.endpoint,
                order_intent=intent,
                dry_run=False,
                audit_log_path=risk_log_path,
                extra_context={
                    "protective_stop_path_available": protective_stop_verified,
                    "emergency_close_path_available": emergency_close_verified,
                },
            )
            try:
                post_entry_positions = await futures.position_information_v3(intent.symbol)
                protection_snapshot = _trusted_snapshot_with_signed_state(
                    self.snapshot,
                    account_payload=account_payload,
                    positions_payload=post_entry_positions,
                    exchange_payload=exchange_payload,
                    protective_stop_path_available=protective_stop_verified,
                    emergency_close_available=emergency_close_verified,
                    protective_stop_capability_source="testnet_executor_order_plan_preflight",
                    emergency_close_capability_source=str(initial_status.get("emergency_close_capability_source") or "unverified"),
                    require_symbol=intent.symbol,
                )
                stop_response = await submit_binance_order_intent(
                    futures,
                    protective_stop.payload,
                    snapshot=protection_snapshot,
                    environment=_testnet_gateway_environment(environment.name),
                    source="hermes_trader_mode:testnet_protective_stop",
                    purpose="protection",
                    endpoint=protective_stop.endpoint,
                    order_intent=intent,
                    dry_run=False,
                    audit_log_path=risk_log_path,
                    extra_context={
                        "protective_stop_path_available": protective_stop_verified,
                        "emergency_close_path_available": emergency_close_verified,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                emergency = await _attempt_emergency_close(
                    futures=futures,
                    intent=intent,
                    account_api_mode=account_api_mode,
                    executor=executor,
                    environment_name=environment.name,
                    risk_log_path=risk_log_path,
                )
                freeze_reason = "protective_stop_failed" if emergency.get("ok") else "emergency_close_failed"
                return {
                    "order_submitted": True,
                    "frozen": True,
                    "can_continue": False,
                    "freeze_reason": freeze_reason,
                    "status": "protective_stop_failed_emergency_close_attempted",
                    "entry_response": entry_response,
                    "protective_stop_error": str(exc),
                    "emergency_close": emergency,
                }

        return {
            "order_submitted": True,
            "frozen": False,
            "can_continue": True,
            "status": "testnet_order_submitted_with_protective_stop",
            "entry_response": entry_response,
            "protective_stop_response": stop_response,
            "intent": intent.to_dict(),
        }


def validate_trusted_runtime_snapshot(snapshot: dict[str, Any]) -> list[str]:
    status = snapshot.get("system_status") if isinstance(snapshot, dict) else {}
    status = status if isinstance(status, dict) else {}
    reasons: list[str] = []
    source = str(status.get("snapshot_source") or status.get("source") or "").strip().lower()
    if source != "runtime":
        reasons.append("manual_snapshot_not_allowed_for_testnet_order_mode")
    if status.get("trusted_runtime_snapshot") is not True:
        reasons.append("untrusted_runtime_snapshot")
    if str(status.get("account_state_source") or status.get("account_source") or "").strip().lower() != "signed_account":
        reasons.append("missing_account_state")
    if str(status.get("position_state_source") or "").strip().lower() != "signed_positions":
        reasons.append("missing_position_state")
    if status.get("data_fresh") is not True:
        reasons.append("stale_snapshot")
    if str(status.get("websocket_status") or "").strip().lower() != "healthy":
        reasons.append("websocket_status_unhealthy")
    if str(status.get("exchange_status") or "").strip().lower() != "healthy":
        reasons.append("exchange_status_unknown")
    if str(status.get("position_state") or "").strip().lower() != "known":
        reasons.append("position_state_unknown")
    if str(status.get("candidate_state") or "").strip().lower() != "known":
        reasons.append("candidate_state_unknown")
    if status.get("protective_stop_path_available") is not True:
        reasons.append("protective_stop_path_unavailable")
    if not _verified_capability(status.get("protective_stop_capability_source")):
        reasons.append("protective_stop_path_unverified")
    if status.get("emergency_close_available") is not True:
        reasons.append("emergency_close_unavailable")
    if not _verified_capability(status.get("emergency_close_capability_source")):
        reasons.append("emergency_close_unverified")
    return list(dict.fromkeys(reasons))


def verify_testnet_executor_capabilities() -> dict[str, Any]:
    """Verify local testnet executor safety paths exist before marking snapshots trusted.

    This is deliberately a code-path capability check, not a prompt/config flag:
    symbol-specific order-plan verification still happens inside the callback
    immediately before any entry order.
    """
    protective_ok = callable(getattr(PhoenixExecutor, "build_order_plan", None)) and callable(
        getattr(BinanceFuturesClient, "new_conditional_order", None)
    )
    emergency_ok = callable(getattr(BinanceFuturesClient, "new_order", None)) and callable(
        getattr(PhoenixExecutor, "_exit_position_side", None)
    )
    return {
        "protective_stop_path_available": bool(protective_ok),
        "protective_stop_capability_source": (
            "verified_code_path:PhoenixExecutor.build_order_plan+BinanceFuturesClient.new_conditional_order"
            if protective_ok
            else "unverified"
        ),
        "emergency_close_available": bool(emergency_ok),
        "emergency_close_capability_source": (
            "verified_code_path:BinanceFuturesClient.new_order_reduce_only+PhoenixExecutor._exit_position_side"
            if emergency_ok
            else "unverified"
        ),
    }


def _find_instruction(plan: list[OrderInstruction], name: str) -> OrderInstruction | None:
    for item in plan:
        if item.name == name:
            return item
    return None


async def _fetch_required_account_position_exchange_state(futures: BinanceFuturesClient) -> tuple[Any, list[dict[str, Any]], dict[str, Any]]:
    try:
        account_payload = await futures.account_overview()
        positions_payload = await futures.position_information_v3()
        exchange_payload = await futures.exchange_info()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"testnet_signed_account_position_exchange_fetch_failed:{exc}") from exc
    if account_payload in (None, ""):
        raise RuntimeError("missing_account_state")
    if not isinstance(positions_payload, list):
        raise RuntimeError("position_state_unknown")
    if not isinstance(exchange_payload, dict) or not exchange_payload.get("symbols"):
        raise RuntimeError("exchange_status_unknown")
    return account_payload, positions_payload, exchange_payload


def _trusted_snapshot_with_signed_state(
    source_snapshot: dict[str, Any],
    *,
    account_payload: Any,
    positions_payload: list[dict[str, Any]],
    exchange_payload: dict[str, Any],
    protective_stop_path_available: bool,
    emergency_close_available: bool,
    protective_stop_capability_source: str,
    emergency_close_capability_source: str,
    require_symbol: str | None = None,
) -> dict[str, Any]:
    snapshot = {
        **source_snapshot,
        "account_risk": dict(source_snapshot.get("account_risk") or {}),
        "system_status": dict(source_snapshot.get("system_status") or {}),
        "current_positions": [_position_from_exchange(item) for item in positions_payload if _is_open_position(item)],
    }
    if require_symbol and not any(str(item.get("symbol") or "").upper() == require_symbol.upper() for item in snapshot["current_positions"]):
        raise RuntimeError("position_state_unknown_after_entry")
    snapshot["account_risk"]["open_positions_count"] = len(snapshot["current_positions"])
    snapshot["account_risk"]["trading_allowed"] = snapshot["account_risk"].get("trading_allowed", True)
    compact = _compact_account_state(account_payload)
    exchange_status = "healthy" if isinstance(exchange_payload, dict) and exchange_payload.get("symbols") else "unknown"
    snapshot["account_risk"]["available_balance_usdt"] = compact.get("available_balance_usdt")
    snapshot["system_status"].update(
        {
            "source": "runtime",
            "snapshot_source": "runtime",
            "trusted_runtime_snapshot": True,
            "account_state_source": "signed_account",
            "account_source": "signed_account",
            "position_state_source": "signed_positions",
            "position_state": "known",
            "exchange_status": exchange_status,
            "protective_stop_path_available": bool(protective_stop_path_available),
            "protective_stop_capability_source": protective_stop_capability_source,
            "emergency_close_available": bool(emergency_close_available),
            "emergency_close_capability_source": emergency_close_capability_source,
            "can_continue": True,
            "freeze_reason": None,
        }
    )
    return snapshot


async def _attempt_emergency_close(
    *,
    futures: BinanceFuturesClient,
    intent: TradeIntent,
    account_api_mode: str,
    executor: PhoenixExecutor,
    environment_name: str,
    risk_log_path: Path,
) -> dict[str, Any]:
    exit_side = "SELL" if intent.side == "BUY" else "BUY"
    hedge_mode = False
    exit_position_side = executor._exit_position_side(intent.side, hedge_mode=hedge_mode)
    payload = {
        "symbol": intent.symbol,
        "side": exit_side,
        **({"positionSide": exit_position_side} if exit_position_side else {}),
        "type": "MARKET",
        "quantity": intent.quantity,
        **({} if hedge_mode and account_api_mode == "portfolio_margin" else {"reduceOnly": "true"}),
        "newOrderRespType": "RESULT",
    }
    try:
        response = await submit_binance_order_intent(
            futures,
            payload,
            snapshot=_emergency_snapshot(intent),
            environment=_testnet_gateway_environment(environment_name),
            source="hermes_trader_mode:emergency_close",
            purpose="emergency",
            order_intent=intent,
            dry_run=False,
            audit_log_path=risk_log_path,
            extra_context={
                "protective_stop_path_available": True,
                "emergency_close_path_available": True,
            },
        )
        return {"ok": True, "payload": response}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}


def _emergency_snapshot(intent: TradeIntent) -> dict[str, Any]:
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    return {
        "market_regime": {"regime": "UNKNOWN", "direction_lock": "BOTH_ALLOWED"},
        "account_risk": {
            "trading_allowed": True,
            "daily_pnl_pct": 0.0,
            "daily_loss_remaining_pct": 3.0,
            "open_positions_count": 1,
            "max_open_positions": 1,
            "loss_streak": 0,
            "cooldown_active": False,
            "reason_if_blocked": None,
        },
        "current_positions": [{"symbol": intent.symbol, "side": "LONG" if intent.side == "BUY" else "SHORT", "protection_status": "healthy"}],
        "top_candidates": [],
        "system_status": {
            "snapshot_time": now,
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
            "testnet_only": True,
            "source": "runtime",
            "snapshot_source": "runtime",
            "trusted_runtime_snapshot": True,
            "account_state_source": "signed_account",
            "position_state_source": "signed_positions",
            "position_state": "known",
            "stop_protection_status": "healthy",
            "candidate_state": "known",
            "protective_stop_path_available": True,
            "protective_stop_capability_source": "emergency_reduce_only_close",
            "emergency_close_available": True,
            "emergency_close_capability_source": "emergency_reduce_only_close",
        },
    }


def _testnet_gateway_environment(environment_name: str) -> dict[str, Any]:
    return {
        "runtime_mode": "TESTNET_LIVE",
        "env": environment_name,
        "require_trusted_runtime_snapshot": True,
    }


def _frozen_executor_result(reason: str, *, executor_stage: str) -> dict[str, Any]:
    return {
        "order_submitted": False,
        "mainnet_order_submitted": False,
        "frozen": True,
        "can_continue": False,
        "freeze_reason": reason,
        "status": "frozen_before_testnet_order",
        "executor_stage": executor_stage,
    }


def _is_open_position(item: dict[str, Any]) -> bool:
    return abs(_position_amount(item)) > 1e-12


def _position_amount(item: dict[str, Any]) -> float:
    for key in ("positionAmt", "positionQty", "quantity"):
        value = _optional_float(item.get(key))
        if value is not None:
            return value
    return 0.0


def _position_from_exchange(item: dict[str, Any]) -> dict[str, Any]:
    amount = _position_amount(item)
    return {
        "symbol": str(item.get("symbol") or "UNKNOWN").upper(),
        "side": "LONG" if amount >= 0 else "SHORT",
        "entry_price": _optional_float(item.get("entryPrice") or item.get("entry_price")),
        "current_price": _optional_float(item.get("markPrice") or item.get("mark_price")),
        "unrealized_pnl_pct": item.get("unrealized_pnl_pct", "unavailable"),
        "unrealized_pnl_usdt": _optional_float(item.get("unRealizedProfit") or item.get("unrealizedProfit")),
        "stop_loss_price": item.get("stop_loss_price", "unavailable"),
        "take_profit_price": item.get("take_profit_price", "unavailable"),
        "time_in_trade_sec": int(_optional_float(item.get("time_in_trade_sec")) or 0),
        "protection_status": str(item.get("protection_status") or "healthy"),
    }


def _verified_capability(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text not in {"", "unverified", "manual", "manual_payload", "mock", "file", "unknown"}


def _candidate_for_symbol(snapshot: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    for item in snapshot.get("top_candidates") or []:
        if str(item.get("symbol") or "").upper() == symbol.upper():
            return dict(item)
    return None


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
