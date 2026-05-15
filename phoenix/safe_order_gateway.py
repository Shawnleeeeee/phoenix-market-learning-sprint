from __future__ import annotations

import asyncio
import inspect
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from phoenix.exchange_filter_feasibility import exchange_filter_fields
from phoenix.hermes_decision import OPEN_ACTIONS, REDUCE_ONLY_ACTIONS, validate_hermes_decision
from phoenix.models import OrderInstruction, TradeIntent
from phoenix.review_reporter import build_review_report
from phoenix.risk_governor import RiskGovernorConfig, append_jsonl, evaluate_risk

ExecutorCallback = Callable[[dict[str, Any]], Awaitable[dict[str, Any]] | dict[str, Any]]

DEFAULT_SAFE_ORDER_LOG_DIR = Path(str(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))) / "memories" / "phoenix_safe_order_gateway_logs"


class SafeOrderGatewayBlocked(RuntimeError):
    def __init__(self, result: SafeOrderGatewayResult | dict[str, Any]) -> None:
        payload = result.to_dict() if hasattr(result, "to_dict") else dict(result)
        self.result = payload
        self.blocked_by = list(payload.get("blocked_by") or [])
        super().__init__(
            "safe_order_gateway blocked order submission: "
            f"source={payload.get('source')} reason={payload.get('reason')} blocked_by={self.blocked_by}"
        )


@dataclass(frozen=True, slots=True)
class SafeOrderGatewayResult:
    approved: bool
    rejected: bool
    blocked_by: list[str]
    source: str
    reason: str
    result_type: str
    normalized_decision: dict[str, Any] | None
    validation_result: dict[str, Any]
    risk_governor_result: dict[str, Any]
    execution_intent: dict[str, Any]
    execution_result: dict[str, Any] | None
    review_report: dict[str, Any] | None
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def submit_order_intent(
    intent: Any,
    snapshot: dict[str, Any] | None,
    environment: dict[str, Any] | None,
    source: str,
    *,
    dry_run: bool = True,
    executor_callback: ExecutorCallback | None = None,
    risk_config: RiskGovernorConfig | None = None,
    log_dir: str | Path | None = None,
    audit_log_path: str | Path | None = None,
    extra_context: dict[str, Any] | None = None,
) -> SafeOrderGatewayResult:
    """Single gate for every Phoenix path that could become an exchange order."""
    created_at = _now_iso()
    source_name = str(source or "UNKNOWN").strip() or "UNKNOWN"
    normalized = normalize_order_intent(intent, source=source_name, extra_context=extra_context)
    snapshot_payload = snapshot or _stale_missing_snapshot(normalized.get("symbol"))
    environment_payload = _merged_environment(environment)
    validation = validate_hermes_decision(normalized)

    risk_result: dict[str, Any]
    execution_intent = _build_execution_intent(
        raw_intent=intent,
        normalized_decision=validation.decision or normalized,
        source=source_name,
        dry_run=dry_run,
        extra_context=extra_context,
    )
    execution_result: dict[str, Any] | None = None

    if not validation.valid:
        environment_risk = evaluate_risk(
            validation.decision or normalized,
            snapshot_payload,
            environment=environment_payload,
            config=risk_config,
        ).to_dict()
        blocked = list(dict.fromkeys([*validation.reasons, *(environment_risk.get("blocked_by") or [])]))
        risk_result = {
            "approved": False,
            "reason": "invalid_order_decision",
            "blocked_by": blocked,
            "sanitized_action": validation.decision or normalized,
            "risk_notes": environment_risk.get("risk_notes") or [],
            "max_allowed_size": 0.0,
            "required_protective_orders": [],
            "created_at": _now_iso(),
        }
    else:
        risk = evaluate_risk(
            validation.decision or normalized,
            snapshot_payload,
            environment=environment_payload,
            config=risk_config,
        )
        risk_result = risk.to_dict()
        _apply_non_trade_soft_reject(risk_result, validation.decision or normalized)

    approved = bool(risk_result.get("approved")) and validation.valid
    execution_intent.update(exchange_filter_fields(risk_result.get("sanitized_action")))
    execution_intent.update(_direction_regime_fields_from_payload(risk_result.get("sanitized_action")))
    execution_intent.update(_entry_quality_fields_from_payload(risk_result.get("sanitized_action")))
    execution_intent["approved_for_execution"] = approved
    execution_intent["required_protective_orders"] = risk_result.get("required_protective_orders") or []
    execution_intent["reason_if_not_approved"] = None if approved else risk_result.get("reason")
    execution_intent["blocked_by"] = list(risk_result.get("blocked_by") or [])
    rejected_state = _classify_reject(risk_result.get("blocked_by") or validation.reasons or [])
    if approved and not dry_run and executor_callback is not None:
        try:
            callback_result = executor_callback(execution_intent)
            if inspect.isawaitable(callback_result):
                callback_result = await callback_result
        except Exception as exc:  # noqa: BLE001
            callback_result = {
                "order_submitted": False,
                "testnet_order_submitted": False,
                "mainnet_order_submitted": False,
                "frozen": True,
                "can_continue": False,
                "freeze_reason": "executor_callback_exception",
                "status": "executor_callback_exception",
                "error": str(exc),
            }
        callback_payload = callback_result if isinstance(callback_result, dict) else {"payload": callback_result}
        order_submitted = bool(callback_payload.get("order_submitted", True))
        mainnet_order_submitted = bool(callback_payload.get("mainnet_order_submitted", False))
        execution_result = {
            "created_at": _now_iso(),
            "order_submitted": order_submitted,
            "testnet_order_submitted": bool(callback_payload.get("testnet_order_submitted", order_submitted and not mainnet_order_submitted)),
            "mainnet_order_submitted": mainnet_order_submitted,
            "dry_run": False,
            "status": str(callback_payload.get("status") or "submitted_after_safe_order_gateway"),
            "executor_called": True,
            "frozen": bool(callback_payload.get("frozen", False)),
            "can_continue": bool(callback_payload.get("can_continue", not bool(callback_payload.get("frozen", False)))),
            "freeze_reason": callback_payload.get("freeze_reason"),
            "result_type": _result_type_from_callback(callback_payload),
            "payload": callback_payload,
        }
    else:
        status = "dry_run_intent_only" if approved and dry_run else "blocked_before_execution"
        if approved and not dry_run:
            status = "no_executor_callback"
        execution_result = {
            "created_at": _now_iso(),
            "order_submitted": False,
            "testnet_order_submitted": False,
            "mainnet_order_submitted": False,
            "dry_run": bool(dry_run),
            "status": status,
            "reason": None if approved else risk_result.get("reason"),
            "executor_called": False,
            "frozen": False if approved else rejected_state["frozen"],
            "can_continue": True if approved else rejected_state["can_continue"],
            "freeze_reason": None if approved else rejected_state["freeze_reason"],
            "result_type": "approved_dry_run" if approved and dry_run else ("approved_no_executor" if approved else rejected_state["result_type"]),
        }

    result_type = str((execution_result or {}).get("result_type") or ("approved" if approved else rejected_state["result_type"]))
    review_type = _review_type(approved, normalized, result_type=result_type)
    review_context = {
        **_direction_regime_fields_from_payload(risk_result.get("sanitized_action")),
        **_entry_quality_fields_from_payload(risk_result.get("sanitized_action")),
    }
    review = build_review_report(
        review_type,
        {
            **normalized,
            **review_context,
            "blocked_by": risk_result.get("blocked_by"),
            "reason": risk_result.get("reason"),
            "result_type": result_type,
            "freeze_reason": (execution_result or {}).get("freeze_reason"),
        },
    )

    result = SafeOrderGatewayResult(
        approved=approved,
        rejected=not approved,
        blocked_by=list(risk_result.get("blocked_by") or validation.reasons or []),
        source=source_name,
        reason="approved" if approved else str(risk_result.get("reason") or "blocked_before_execution"),
        result_type=result_type,
        normalized_decision=validation.decision or normalized,
        validation_result=validation.to_dict(),
        risk_governor_result=risk_result,
        execution_intent=execution_intent,
        execution_result=execution_result,
        review_report=review.to_dict(),
        created_at=created_at,
    )
    _write_replay_log(
        log_dir=log_dir,
        snapshot=snapshot_payload,
        raw_intent=intent,
        validation_result=validation.to_dict(),
        risk_result=risk_result,
        gateway_result=result.to_dict(),
        execution_result=execution_result,
        review_report=review.to_dict(),
    )
    _write_audit_log(audit_log_path, result.to_dict())
    return result


async def submit_binance_order_intent(
    futures: Any,
    payload: dict[str, Any],
    *,
    snapshot: dict[str, Any] | None,
    environment: dict[str, Any] | None,
    source: str,
    purpose: str,
    endpoint: str | None = None,
    order_intent: TradeIntent | dict[str, Any] | None = None,
    dry_run: bool = False,
    risk_config: RiskGovernorConfig | None = None,
    log_dir: str | Path | None = None,
    audit_log_path: str | Path | None = None,
    extra_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Gate and then submit a Binance order payload.

    Business code should call this helper instead of calling Binance client
    order methods directly. The only direct Binance order calls live here.
    """
    payload = dict(payload)
    purpose_name = str(purpose or "order").lower()
    context = {
        "purpose": purpose_name,
        "endpoint": endpoint,
        "order_intent": _to_plain_dict(order_intent),
        **(extra_context or {}),
    }

    async def _execute(_execution_intent: dict[str, Any]) -> dict[str, Any]:
        if _is_conditional_endpoint(endpoint, payload):
            return await futures.new_conditional_order(payload)
        return await futures.new_order(payload)

    gateway = await submit_order_intent(
        payload,
        snapshot,
        environment,
        source,
        dry_run=dry_run,
        executor_callback=None if dry_run else _execute,
        risk_config=risk_config,
        log_dir=log_dir,
        audit_log_path=audit_log_path,
        extra_context=context,
    )
    if not gateway.approved:
        raise SafeOrderGatewayBlocked(gateway)
    if dry_run:
        return {
            "safe_order_gateway": gateway.to_dict(),
            "dry_run": True,
            "order_submitted": False,
        }
    execution_result = gateway.execution_result or {}
    if execution_result.get("frozen") or execution_result.get("order_submitted") is not True:
        raise RuntimeError(str(execution_result.get("freeze_reason") or execution_result.get("status") or "safe_order_gateway_executor_failed"))
    payload_result = execution_result.get("payload")
    if not isinstance(payload_result, dict):
        raise RuntimeError("safe_order_gateway approved but executor returned no order payload.")
    return payload_result


def submit_sync_order_intent(
    intent: Any,
    snapshot: dict[str, Any] | None,
    environment: dict[str, Any] | None,
    source: str,
    *,
    executor_callback: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    dry_run: bool = False,
    risk_config: RiskGovernorConfig | None = None,
    log_dir: str | Path | None = None,
    audit_log_path: str | Path | None = None,
    extra_context: dict[str, Any] | None = None,
) -> SafeOrderGatewayResult:
    """Synchronous wrapper for legacy CLI paths.

    It intentionally refuses to run inside an active event loop. Async callers
    must use submit_order_intent directly so execution cannot hide behind a
    nested loop workaround.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            submit_order_intent(
                intent,
                snapshot,
                environment,
                source,
                dry_run=dry_run,
                executor_callback=executor_callback,
                risk_config=risk_config,
                log_dir=log_dir,
                audit_log_path=audit_log_path,
                extra_context=extra_context,
            )
        )
    raise RuntimeError("submit_sync_order_intent cannot be used inside a running event loop.")


def submit_sync_client_order_intent(
    client: Any,
    payload: dict[str, Any],
    *,
    snapshot: dict[str, Any] | None,
    environment: dict[str, Any] | None,
    source: str,
    purpose: str,
    order_intent: Any | None = None,
    dry_run: bool = False,
    risk_config: RiskGovernorConfig | None = None,
    log_dir: str | Path | None = None,
    audit_log_path: str | Path | None = None,
    extra_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Gate and then submit a synchronous legacy client order payload."""
    payload = dict(payload)
    context = {
        "purpose": str(purpose or "order").lower(),
        "order_intent": _to_plain_dict(order_intent),
        **(extra_context or {}),
    }

    def _execute(_execution_intent: dict[str, Any]) -> dict[str, Any]:
        return client.new_order(payload)

    gateway = submit_sync_order_intent(
        payload,
        snapshot,
        environment,
        source,
        dry_run=dry_run,
        executor_callback=None if dry_run else _execute,
        risk_config=risk_config,
        log_dir=log_dir,
        audit_log_path=audit_log_path,
        extra_context=context,
    )
    if not gateway.approved:
        raise SafeOrderGatewayBlocked(gateway)
    if dry_run:
        return {
            "safe_order_gateway": gateway.to_dict(),
            "dry_run": True,
            "order_submitted": False,
        }
    execution_result = gateway.execution_result or {}
    if execution_result.get("frozen") or execution_result.get("order_submitted") is not True:
        raise RuntimeError(str(execution_result.get("freeze_reason") or execution_result.get("status") or "safe_order_gateway_executor_failed"))
    payload_result = execution_result.get("payload")
    if not isinstance(payload_result, dict):
        raise RuntimeError("safe_order_gateway approved but sync executor returned no order payload.")
    return payload_result


def normalize_order_intent(
    intent: Any,
    *,
    source: str,
    extra_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = extra_context or {}
    if isinstance(intent, dict) and str(intent.get("action") or "").strip():
        decision = dict(intent)
        decision.setdefault("source", "HERMES")
        return decision

    if isinstance(intent, OrderInstruction):
        return _decision_from_order_payload(intent.payload, instruction=intent, source=source, context=context)

    if isinstance(intent, TradeIntent):
        return _decision_from_trade_intent(intent, source=source, context=context)

    if isinstance(intent, dict):
        return _decision_from_order_payload(intent, source=source, context=context)

    if hasattr(intent, "to_dict"):
        return normalize_order_intent(intent.to_dict(), source=source, extra_context=context)

    return {
        "action": "NO_TRADE",
        "symbol": None,
        "trade_type": "NONE",
        "confidence": 0.0,
        "reason": f"unsupported_order_intent_from_{source}",
        "source": "HERMES",
    }


def build_gateway_snapshot(
    *,
    symbol: str | None = None,
    side: str | None = None,
    candidate: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
    data_fresh: bool = True,
    websocket_status: str = "healthy",
    exchange_status: str = "healthy",
    direction_lock: str = "BOTH_ALLOWED",
    regime: str = "UNKNOWN",
    trading_allowed: bool = True,
    open_positions_count: int | None = None,
    max_open_positions: int = 10,
    stale_after_sec: int = 30,
    stop_protection_status: str = "healthy",
    position_state: str = "known",
    candidate_state: str = "known",
    protective_stop_path_available: bool = True,
    emergency_close_available: bool = True,
) -> dict[str, Any]:
    symbol_text = str(symbol or (candidate or {}).get("symbol") or "UNKNOWN").upper()
    candidate_payload = dict(candidate or {})
    if not candidate_payload and symbol_text != "UNKNOWN":
        candidate_payload = {
            "symbol": symbol_text,
            "bias": "LONG" if str(side or "").upper() == "BUY" else ("SHORT" if str(side or "").upper() == "SELL" else "NEUTRAL"),
            "setup_type": "QUICK_TRADE",
            "score": 0.0,
            "current_price": "unavailable",
            "spread_bps": 0.0,
            "estimated_slippage_bps": 0.0,
            "liquidity_ok": True,
        }
    now = _now_iso()
    position_list = list(positions or [])
    return {
        "market_regime": {
            "regime": regime,
            "btc_trend_1m": "unavailable",
            "btc_trend_5m": "unavailable",
            "eth_trend_1m": "unavailable",
            "eth_trend_5m": "unavailable",
            "volatility": "unavailable",
            "direction_lock": direction_lock,
        },
        "account_risk": {
            "trading_allowed": trading_allowed,
            "daily_pnl_pct": 0.0,
            "daily_loss_remaining_pct": 3.0,
            "open_positions_count": len(position_list) if open_positions_count is None else open_positions_count,
            "max_open_positions": max_open_positions,
            "loss_streak": 0,
            "cooldown_active": False,
            "reason_if_blocked": None,
        },
        "current_positions": position_list,
        "top_candidates": [candidate_payload] if candidate_payload else [],
        "system_status": {
            "snapshot_time": now,
            "data_fresh": bool(data_fresh),
            "websocket_status": websocket_status,
            "exchange_status": exchange_status,
            "testnet_only": True,
            "stale_after_sec": stale_after_sec,
            "position_state": position_state,
            "stop_protection_status": stop_protection_status,
            "candidate_state": candidate_state,
            "protective_stop_path_available": bool(protective_stop_path_available),
            "emergency_close_available": bool(emergency_close_available),
        },
    }


def _decision_from_trade_intent(intent: TradeIntent, *, source: str, context: dict[str, Any]) -> dict[str, Any]:
    side = str(intent.side or "").upper()
    action = "ENTER_LONG" if side == "BUY" else "ENTER_SHORT"
    return {
        "action": action,
        "symbol": intent.symbol,
        "trade_type": str(context.get("trade_type") or "QUICK_TRADE").upper(),
        "confidence": _safe_float(context.get("confidence"), default=1.0),
        "reason": str(context.get("reason") or f"normalized_trade_intent_from_{source}"),
        "entry_price_hint": intent.entry_price,
        "stop_loss_price": intent.initial_stop_price,
        "take_profit_price": intent.take_profit_price,
        "max_holding_time_sec": _optional_int(context.get("max_holding_time_sec")),
        "invalidation_condition": context.get("invalidation_condition"),
        "reduce_only": False,
        "margin_type": intent.margin_type,
        "protective_stop_path_available": bool(context.get("protective_stop_path_available", False)),
        "emergency_close_path_available": bool(context.get("emergency_close_path_available", False)),
        "source": "HERMES",
    }


def _decision_from_order_payload(
    payload: dict[str, Any],
    *,
    source: str,
    context: dict[str, Any],
    instruction: OrderInstruction | None = None,
) -> dict[str, Any]:
    payload = dict(payload)
    order_intent = context.get("order_intent") if isinstance(context.get("order_intent"), dict) else {}
    side = str(payload.get("side") or order_intent.get("side") or "").upper()
    reduce_only = _truthy(payload.get("reduceOnly")) or _truthy(payload.get("closePosition"))
    order_type = str(
        payload.get("strategyType")
        or payload.get("origType")
        or payload.get("type")
        or ""
    ).upper()
    purpose = str(context.get("purpose") or "").lower()
    action = _action_from_payload(side=side, reduce_only=reduce_only, order_type=order_type, purpose=purpose)
    symbol = str(payload.get("symbol") or order_intent.get("symbol") or "").upper() or None
    stop_loss_price = (
        payload.get("stop_loss_price")
        or payload.get("stopPrice")
        or payload.get("triggerPrice")
        or order_intent.get("initial_stop_price")
    )
    if action in {"TAKE_PROFIT"}:
        stop_loss_price = order_intent.get("initial_stop_price")
    decision = {
        "action": action,
        "symbol": symbol,
        "trade_type": "NONE" if action in REDUCE_ONLY_ACTIONS or action in {"NO_TRADE"} else str(context.get("trade_type") or "QUICK_TRADE").upper(),
        "confidence": _safe_float(context.get("confidence"), default=1.0),
        "reason": str(context.get("reason") or f"normalized_order_payload_from_{source}:{purpose or order_type.lower()}"),
        "entry_price_hint": order_intent.get("entry_price"),
        "stop_loss_price": _optional_float(stop_loss_price) if action in OPEN_ACTIONS else None,
        "take_profit_price": _optional_float(order_intent.get("take_profit_price")),
        "max_holding_time_sec": _optional_int(context.get("max_holding_time_sec")),
        "invalidation_condition": context.get("invalidation_condition"),
        "reduce_only": action in REDUCE_ONLY_ACTIONS,
        "margin_type": str(order_intent.get("margin_type") or context.get("margin_type") or "ISOLATED").upper(),
        "protective_stop_path_available": bool(context.get("protective_stop_path_available", False)),
        "emergency_close_path_available": bool(context.get("emergency_close_path_available", False)),
        "size_reduced": bool(context.get("size_reduced", False)),
        "source": "HERMES",
    }
    if instruction is not None:
        decision["instruction_name"] = instruction.name
    return decision


def _action_from_payload(*, side: str, reduce_only: bool, order_type: str, purpose: str) -> str:
    if reduce_only or purpose in {"exit", "emergency", "protection", "reattach", "take_profit"}:
        if "TAKE_PROFIT" in order_type or "take_profit" in purpose:
            return "TAKE_PROFIT"
        if "STOP" in order_type or purpose in {"protection", "reattach"}:
            return "MOVE_STOP"
        return "EXIT"
    if side == "BUY":
        return "ENTER_LONG"
    if side == "SELL":
        return "ENTER_SHORT"
    return "NO_TRADE"


def _build_execution_intent(
    *,
    raw_intent: Any,
    normalized_decision: dict[str, Any],
    source: str,
    dry_run: bool,
    extra_context: dict[str, Any] | None,
) -> dict[str, Any]:
    context = extra_context or {}
    return {
        "created_at": _now_iso(),
        "source": source,
        "dry_run": dry_run,
        "testnet_only": True,
        "action": normalized_decision.get("action"),
        "symbol": normalized_decision.get("symbol"),
        "side": _side_for_action(normalized_decision.get("action")),
        "reduce_only": bool(normalized_decision.get("reduce_only", False)),
        "trace_id": context.get("trace_id"),
        "entry_price_hint": normalized_decision.get("entry_price_hint"),
        "stop_loss_pct": normalized_decision.get("stop_loss_pct"),
        "stop_loss_price": normalized_decision.get("stop_loss_price"),
        "take_profit_pct": normalized_decision.get("take_profit_pct"),
        "take_profit_price": normalized_decision.get("take_profit_price"),
        "max_holding_time_sec": normalized_decision.get("max_holding_time_sec"),
        "no_follow_through_exit_enabled": normalized_decision.get("no_follow_through_exit_enabled"),
        "no_follow_through_exit_sec": normalized_decision.get("no_follow_through_exit_sec"),
        "no_follow_through_min_mfe_pct": normalized_decision.get("no_follow_through_min_mfe_pct"),
        "required_protective_orders": [],
        "raw_intent": _to_plain_dict(raw_intent),
        "purpose": context.get("purpose"),
        "endpoint": context.get("endpoint"),
    }


def _write_replay_log(
    *,
    log_dir: str | Path | None,
    snapshot: dict[str, Any],
    raw_intent: Any,
    validation_result: dict[str, Any],
    risk_result: dict[str, Any],
    gateway_result: dict[str, Any],
    execution_result: dict[str, Any] | None,
    review_report: dict[str, Any] | None,
) -> None:
    directory = Path(log_dir) if log_dir is not None else DEFAULT_SAFE_ORDER_LOG_DIR
    row = {
        "event": "safe_order_gateway_replay",
        "created_at": _now_iso(),
        **_snapshot_replay_metadata(snapshot),
        "snapshot": snapshot,
        "raw_decision_or_order_intent": _to_plain_dict(raw_intent),
        "validation_result": validation_result,
        "risk_governor_result": risk_result,
        "safe_order_gateway_result": gateway_result,
        "execution_result": execution_result,
        "review_report": review_report,
    }
    append_jsonl(directory / "safe_order_gateway_replay.jsonl", row)
    event_name = "safe_order_gateway_approved" if gateway_result.get("approved") else "safe_order_gateway_rejected"
    append_jsonl(directory / "safe_order_gateway_decisions.jsonl", {"event": event_name, **gateway_result})


def _snapshot_replay_metadata(snapshot: dict[str, Any]) -> dict[str, Any]:
    status = snapshot.get("system_status") if isinstance(snapshot, dict) else {}
    status = status if isinstance(status, dict) else {}
    return {
        "snapshot_source": status.get("snapshot_source") or status.get("source"),
        "trusted_runtime_snapshot": bool(status.get("trusted_runtime_snapshot", False)),
        "account_state_source": status.get("account_state_source") or status.get("account_source"),
        "position_state_source": status.get("position_state_source"),
        "protective_stop_capability_source": status.get("protective_stop_capability_source"),
        "emergency_close_capability_source": status.get("emergency_close_capability_source"),
        "freeze_reason": status.get("freeze_reason"),
    }


def _write_audit_log(audit_log_path: str | Path | None, gateway_result: dict[str, Any]) -> None:
    if audit_log_path is None:
        return
    event_name = "safe_order_approved" if gateway_result.get("approved") else "safe_order_rejected"
    append_jsonl(Path(audit_log_path), {"event": event_name, **gateway_result})


def _merged_environment(environment: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key in (
        "PHOENIX_RUNTIME_MODE",
        "PHOENIX_BINANCE_ENV",
        "PHOENIX_LIVE_TRADING_ENABLED",
        "PHOENIX_MAINNET_LIVE_ENABLED",
        "PHOENIX_ENABLE_MAINNET_LIVE",
        "PHOENIX_PROMOTION_ALLOWED",
        "PHOENIX_EXECUTION_MODE",
        "AUTO_CONFIRM_WHEN_RULES_PASS",
    ):
        value = os.getenv(key)
        if value not in (None, ""):
            merged[key] = value
    merged.update(environment or {})
    return merged


def _stale_missing_snapshot(symbol: Any) -> dict[str, Any]:
    return build_gateway_snapshot(
        symbol=str(symbol or "UNKNOWN"),
        data_fresh=False,
        websocket_status="unavailable",
        exchange_status="unavailable",
        stop_protection_status="unknown",
        position_state="unknown",
    )


def _is_conditional_endpoint(endpoint: str | None, payload: dict[str, Any]) -> bool:
    endpoint_text = str(endpoint or "")
    return "/conditional/" in endpoint_text or endpoint_text.endswith("/algoOrder") or bool(payload.get("algoType"))


SOFT_REJECT_BLOCKERS = {
    "spread_too_wide",
    "slippage_too_high",
    "liquidity_too_poor",
    "direction_lock_conflict",
    "direction_lock_no_trade",
    "direction_regime_mismatch",
    "direction_regime_high_vol",
    "chop_requires_range_edge",
    "direction_unknown",
    "market_regime_unknown",
    "cooldown_after_loss_active",
    "loss_streak_lock",
    "max_open_positions",
    "duplicate_position",
    "confidence_below_threshold",
    "account_trading_blocked",
    "exchange_filter_reject",
    "exchange_filter_unchecked",
    "micro_notional_infeasible",
    "symbol_not_tradeable",
    "entry_quality_filter_failed",
    "late_chase_short",
    "late_chase_long",
    "btc_eth_alignment_missing",
    "momentum_follow_through_missing",
    "fee_slippage_drag_high",
    "fee_slippage_cost_unavailable",
    "entry_far_from_invalidation",
    "entry_invalidation_distance_unavailable",
    "entry_quality_symbol_observe_only",
    "entry_quality_score_below_threshold",
}


def _direction_regime_fields_from_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    keys = (
        "market_regime",
        "candidate_direction",
        "allowed_direction",
        "direction_regime_allowed",
        "direction_regime_reason",
        "blocked_by",
        "direction_regime_source",
        "matrix_source",
        "candidate_symbol",
        "normalized_decision",
        "no_trade_reason",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def _entry_quality_fields_from_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    keys = (
        "entry_quality_filter",
        "entry_quality_version",
        "entry_quality_checked",
        "entry_quality_allowed",
        "entry_quality_score",
        "entry_quality_min_score",
        "entry_quality_reason",
        "entry_quality_reasons",
        "entry_quality_components",
    )
    return {key: payload.get(key) for key in keys if key in payload}

HARD_FREEZE_BLOCKERS = {
    "mainnet_live_blocked",
    "live_trading_enabled_blocked",
    "mainnet_env_blocked",
    "mainnet_shadow_order_endpoint_blocked",
    "manual_snapshot_not_allowed_for_testnet_order_mode",
    "untrusted_runtime_snapshot",
    "stale_snapshot",
    "snapshot_transport_unhealthy",
    "websocket_status_unhealthy",
    "exchange_status_unknown",
    "missing_account_state",
    "missing_position_state",
    "position_state_unknown",
    "stop_order_status_unknown",
    "protective_stop_path_unavailable",
    "protective_stop_path_unverified",
    "emergency_close_unavailable",
    "emergency_close_unverified",
    "micro_notional_required",
    "micro_notional_exceeded",
    "take_profit_path_unavailable",
    "take_profit_path_unverified",
    "executor_callback_exception",
    "daily_loss_limit_hit",
}


def _classify_reject(blocked_by: list[Any]) -> dict[str, Any]:
    reasons = [str(item) for item in blocked_by if item]
    hard = [reason for reason in reasons if reason in HARD_FREEZE_BLOCKERS]
    if hard:
        return {
            "result_type": "hard_freeze",
            "frozen": True,
            "can_continue": False,
            "freeze_reason": ",".join(sorted(set(hard))),
        }
    if reasons:
        return {
            "result_type": "soft_reject",
            "frozen": False,
            "can_continue": True,
            "freeze_reason": None,
        }
    return {
        "result_type": "approved",
        "frozen": False,
        "can_continue": True,
        "freeze_reason": None,
    }


def _apply_non_trade_soft_reject(risk_result: dict[str, Any], decision: dict[str, Any]) -> None:
    action = str(decision.get("action") or "").upper()
    if action == "NO_TRADE":
        _mark_soft_reject(
            risk_result,
            blocker="no_trade_from_hermes",
            reason=str(decision.get("reason") or "Hermes decided NO_TRADE"),
            decision=decision,
        )
    elif action == "WAIT_FOR_TRIGGER":
        _mark_soft_reject(
            risk_result,
            blocker="wait_for_trigger",
            reason=str(decision.get("reason") or "Hermes waiting for trigger"),
            decision=decision,
        )


def _mark_soft_reject(risk_result: dict[str, Any], *, blocker: str, reason: str, decision: dict[str, Any]) -> None:
    existing = list(risk_result.get("blocked_by") or [])
    if any(str(item) in HARD_FREEZE_BLOCKERS for item in existing):
        return
    decision_blockers = decision.get("blocked_by") if isinstance(decision.get("blocked_by"), list) else []
    blocked_by = list(dict.fromkeys([*existing, *decision_blockers, blocker]))
    risk_result.update(
        {
            "approved": False,
            "reason": reason,
            "blocked_by": blocked_by,
            "sanitized_action": decision,
            "max_allowed_size": 0.0,
            "required_protective_orders": [],
        }
    )


def _result_type_from_callback(callback_payload: dict[str, Any]) -> str:
    if callback_payload.get("result_type"):
        return str(callback_payload.get("result_type"))
    if callback_payload.get("frozen"):
        return "hard_freeze"
    if callback_payload.get("order_submitted"):
        return "completed"
    return "soft_reject"


def _review_type(approved: bool, normalized: dict[str, Any], *, result_type: str = "") -> str:
    if result_type == "hard_freeze":
        return "HARD_FREEZE"
    if result_type == "soft_reject":
        return "SOFT_REJECT"
    if str(normalized.get("action") or "").upper() == "NO_TRADE":
        return "NO_TRADE"
    if not approved:
        return "RISK_REJECT"
    if normalized.get("action") in OPEN_ACTIONS:
        return "PRE_ENTER"
    return "POSITION_UPDATE"


def _side_for_action(action: Any) -> str | None:
    text = str(action or "").upper()
    if text == "ENTER_LONG":
        return "BUY"
    if text == "ENTER_SHORT":
        return "SELL"
    return None


def _to_plain_dict(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): _to_plain_dict(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain_dict(item) for item in value]
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "to_dict"):
        return _to_plain_dict(value.to_dict())
    if hasattr(value, "__dict__"):
        return _to_plain_dict(vars(value))
    return repr(value)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
