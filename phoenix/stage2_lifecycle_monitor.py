from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Awaitable, Callable

from phoenix.review_reporter import append_review_log, build_review_report
from phoenix.risk_governor import RiskGovernorConfig
from phoenix.safe_order_gateway import submit_binance_order_intent
from phoenix.testnet_executor_callback import (
    _fetch_required_account_position_exchange_state,
    _trusted_snapshot_with_signed_state,
)


Clock = Callable[[], int]
Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class Stage2ProtectedPosition:
    symbol: str
    direction: str
    entry_side: str
    close_side: str
    entry_order_id: int | str | None
    entry_price: float | None
    position_size: float
    entry_filled_time_ms: int
    stop_loss_order_id: int | str | None
    stop_loss_client_id: str | None
    take_profit_order_id: int | str | None
    take_profit_client_id: str | None
    stop_loss_price: float | None
    take_profit_price: float | None
    max_holding_time_sec: int
    invalidation_condition: str
    order_intent: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_executor_result(
        cls,
        *,
        execution_intent: dict[str, Any],
        executor_result: dict[str, Any],
        fallback_entry_time_ms: int | None = None,
    ) -> "Stage2ProtectedPosition":
        payload = executor_result.get("payload") if isinstance(executor_result.get("payload"), dict) else executor_result
        payload = payload if isinstance(payload, dict) else {}
        raw_intent = execution_intent.get("raw_intent") if isinstance(execution_intent.get("raw_intent"), dict) else {}
        intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
        order_intent = {**intent, **(raw_intent if isinstance(raw_intent, dict) else {})}
        entry = payload.get("entry_response") if isinstance(payload.get("entry_response"), dict) else {}
        stop = payload.get("protective_stop_response") if isinstance(payload.get("protective_stop_response"), dict) else {}
        take_profit = payload.get("take_profit_response") if isinstance(payload.get("take_profit_response"), dict) else {}
        entry_side = str(entry.get("side") or intent.get("side") or execution_intent.get("side") or "BUY").upper()
        close_side = "SELL" if entry_side == "BUY" else "BUY"
        symbol = str(entry.get("symbol") or intent.get("symbol") or execution_intent.get("symbol") or raw_intent.get("symbol") or "").upper()
        entry_time = _optional_int(entry.get("updateTime") or entry.get("transactTime"))
        if entry_time is None:
            entry_time = int(fallback_entry_time_ms or time.time() * 1000)
        return cls(
            symbol=symbol,
            direction=str(execution_intent.get("action") or raw_intent.get("action") or "").upper(),
            entry_side=entry_side,
            close_side=close_side,
            entry_order_id=entry.get("orderId"),
            entry_price=_first_float(entry, "avgPrice", "price") or _optional_float(intent.get("entry_price") or raw_intent.get("entry_price_hint")),
            position_size=float(
                _decimal(entry.get("executedQty") or entry.get("cumQty") or intent.get("quantity") or raw_intent.get("quantity"))
            ),
            entry_filled_time_ms=entry_time,
            stop_loss_order_id=stop.get("algoId") or stop.get("strategyId") or stop.get("orderId"),
            stop_loss_client_id=_optional_text(stop.get("clientAlgoId") or stop.get("newClientStrategyId") or stop.get("clientOrderId")),
            take_profit_order_id=take_profit.get("algoId") or take_profit.get("strategyId") or take_profit.get("orderId"),
            take_profit_client_id=_optional_text(
                take_profit.get("clientAlgoId") or take_profit.get("newClientStrategyId") or take_profit.get("clientOrderId")
            ),
            stop_loss_price=_first_float(stop, "triggerPrice", "stopPrice") or _optional_float(intent.get("initial_stop_price") or raw_intent.get("stop_loss_price")),
            take_profit_price=_first_float(take_profit, "triggerPrice", "stopPrice")
            or _optional_float(intent.get("take_profit_price") or raw_intent.get("take_profit_price")),
            max_holding_time_sec=int(_decimal(raw_intent.get("max_holding_time_sec") or execution_intent.get("max_holding_time_sec") or 300)),
            invalidation_condition=str(raw_intent.get("invalidation_condition") or execution_intent.get("invalidation_condition") or "").strip(),
            order_intent=order_intent,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def protection_status_for_orders(
    conditional_orders: list[dict[str, Any]],
    protected_position: Stage2ProtectedPosition,
) -> dict[str, Any]:
    stop = None
    take_profit = None
    for order in conditional_orders:
        if _matches_known_order(
            order,
            order_id=protected_position.stop_loss_order_id,
            client_id=protected_position.stop_loss_client_id,
        ):
            stop = order
        if _matches_known_order(
            order,
            order_id=protected_position.take_profit_order_id,
            client_id=protected_position.take_profit_client_id,
        ):
            take_profit = order
    return {
        "stop_loss_present": stop is not None,
        "take_profit_present": take_profit is not None,
        "stop_loss_valid": _valid_protective_order(stop, expected_type="STOP", close_side=protected_position.close_side),
        "take_profit_valid": _valid_protective_order(take_profit, expected_type="TAKE_PROFIT", close_side=protected_position.close_side),
        "stop_loss_order": stop,
        "take_profit_order": take_profit,
    }


class Stage2LifecycleMonitor:
    def __init__(
        self,
        *,
        futures: Any,
        protected_position: Stage2ProtectedPosition,
        source_snapshot: dict[str, Any],
        output_dir: str | Path,
        risk_config: RiskGovernorConfig | None = None,
        environment_name: str = "testnet",
        now_ms: Clock | None = None,
        poll_sec: float = 5.0,
        post_close_wait_sec: float = 60.0,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.futures = futures
        self.protected_position = protected_position
        self.source_snapshot = source_snapshot
        self.output_dir = Path(output_dir)
        self.risk_config = risk_config or RiskGovernorConfig()
        self.environment_name = environment_name
        self.now_ms = now_ms or (lambda: int(time.time() * 1000))
        self.poll_sec = max(0.0, float(poll_sec))
        self.post_close_wait_sec = max(0.0, float(post_close_wait_sec))
        self.sleep = sleep
        self.events_path = self.output_dir / "stage2_lifecycle_events.jsonl"
        self.report_path = self.output_dir / "stage2_lifecycle_final_report.json"
        self.review_report_path = self.output_dir / "stage2_lifecycle_review_report.jsonl"
        self.last_close_snapshot_status: dict[str, Any] | None = None

    async def monitor_until_closed(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._append_event({"event": "lifecycle_monitor_start", "protected_position": self.protected_position.to_dict()})
        if self._client_session_closed():
            self._append_event({"event": "lifecycle_client_session_closed_fail_closed"})
            result = self._build_final_report(
                final_close_reason="HARD_FREEZE",
                final_state={},
                close_response=None,
                emergency_close=False,
                hard_freeze=True,
                cancellations=[],
            )
            result["freeze_reason"] = "lifecycle_client_session_closed"
            return self._write_final_result(result)
        close_response: dict[str, Any] | None = None
        emergency_close = False
        hard_freeze = False
        final_close_reason = ""
        final_state: dict[str, Any] = {}
        cancellations: list[dict[str, Any]] = []

        while True:
            try:
                state = await self._fetch_state()
            except Exception as exc:  # noqa: BLE001
                hard_freeze = True
                final_close_reason = "HARD_FREEZE"
                final_state = {}
                self._append_event({"event": "lifecycle_state_fetch_failed", "error": str(exc)})
                break
            position = _position_for_symbol(state["positions"], self.protected_position.symbol)
            protection_status = protection_status_for_orders(state["conditional_orders"], self.protected_position)
            elapsed_sec = self._elapsed_sec()
            self._append_event(
                {
                    "event": "lifecycle_poll",
                    "elapsed_sec": elapsed_sec,
                    "position_amt": position.get("positionAmt") if position else "0",
                    "conditional_orders_count": len(state["conditional_orders"]),
                    "protection_status": protection_status,
                }
            )
            if position is None:
                final_close_reason = self._classify_exchange_close(state)
                final_state = state
                break
            quantity = abs(_decimal(position.get("positionAmt")))
            if not protection_status["stop_loss_valid"] or not protection_status["take_profit_valid"]:
                emergency_close = True
                try:
                    close_response = await self._submit_reduce_only_close(
                        quantity=quantity,
                        reason="protection_missing",
                        protection_status=protection_status,
                    )
                    final_close_reason = "EMERGENCY_CLOSED"
                except Exception as exc:  # noqa: BLE001
                    hard_freeze = True
                    final_close_reason = "HARD_FREEZE"
                    self._append_event({"event": "emergency_close_failed", "error": str(exc)})
                    final_state = await self._safe_fetch_state()
                    break
                final_state = await self._wait_position_flat()
                if _position_for_symbol(final_state["positions"], self.protected_position.symbol) is not None:
                    hard_freeze = True
                    final_close_reason = "HARD_FREEZE"
                break
            if self._no_follow_through_exit_due(elapsed_sec, state):
                self._append_event({"event": "no_follow_through_exit_triggered", "elapsed_sec": elapsed_sec})
                try:
                    close_response = await self._submit_reduce_only_close(
                        quantity=quantity,
                        reason="no_follow_through",
                        protection_status=protection_status,
                    )
                    final_close_reason = "CLOSED_BY_NO_FOLLOW_THROUGH"
                except Exception as exc:  # noqa: BLE001
                    hard_freeze = True
                    final_close_reason = "HARD_FREEZE"
                    self._append_event({"event": "no_follow_through_close_failed", "error": str(exc)})
                    final_state = await self._safe_fetch_state()
                    break
                final_state = await self._wait_position_flat()
                if _position_for_symbol(final_state["positions"], self.protected_position.symbol) is not None:
                    hard_freeze = True
                    final_close_reason = "HARD_FREEZE"
                break
            if elapsed_sec >= self.protected_position.max_holding_time_sec:
                try:
                    close_response = await self._submit_reduce_only_close(
                        quantity=quantity,
                        reason="timeout",
                        protection_status=protection_status,
                    )
                    final_close_reason = "CLOSED_BY_TIMEOUT"
                except Exception as exc:  # noqa: BLE001
                    hard_freeze = True
                    final_close_reason = "HARD_FREEZE"
                    self._append_event({"event": "timeout_close_failed", "error": str(exc)})
                    final_state = await self._safe_fetch_state()
                    break
                final_state = await self._wait_position_flat()
                if _position_for_symbol(final_state["positions"], self.protected_position.symbol) is not None:
                    hard_freeze = True
                    final_close_reason = "HARD_FREEZE"
                break
            await self.sleep(self.poll_sec)

        if not hard_freeze:
            final_state = await self._fetch_state()
            if _position_for_symbol(final_state["positions"], self.protected_position.symbol) is None:
                cancellations = await self._cancel_known_conditionals()
                final_state = await self._fetch_state()

        result = self._build_final_report(
            final_close_reason=final_close_reason,
            final_state=final_state,
            close_response=close_response,
            emergency_close=emergency_close,
            hard_freeze=hard_freeze,
            cancellations=cancellations,
        )
        return self._write_final_result(result)

    def _write_final_result(self, result: dict[str, Any]) -> dict[str, Any]:
        review = self._build_review_report(result)
        append_review_log(self.review_report_path, review)
        result["review_report"] = review.to_dict()
        result["review_report_path"] = str(self.review_report_path)
        self.report_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        self._append_event({"event": "lifecycle_monitor_completed", "final_report": result})
        return result

    async def _submit_reduce_only_close(
        self,
        *,
        quantity: Decimal,
        reason: str,
        protection_status: dict[str, Any],
    ) -> dict[str, Any]:
        self._raise_if_client_session_closed()
        if quantity <= Decimal("0"):
            raise RuntimeError("no_open_quantity_to_close")
        close_snapshot = await self._build_lifecycle_close_snapshot(protection_status)
        self.last_close_snapshot_status = dict(close_snapshot.get("system_status") or {})
        payload = {
            "symbol": self.protected_position.symbol,
            "side": self.protected_position.close_side,
            "type": "MARKET",
            "quantity": format(quantity, "f"),
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        }
        environment = {
            "runtime_mode": "TESTNET_LIVE",
            "env": self.environment_name,
            "testnet_only": True,
            "mainnet_live": False,
            "PHOENIX_MAINNET_LIVE_ENABLED": "false",
            "PHOENIX_LIVE_TRADING_ENABLED": "false",
            "PHOENIX_PROMOTION_ALLOWED": "false",
            "require_trusted_runtime_snapshot": True,
        }
        self._append_event({"event": "reduce_only_close_submit_start", "reason": reason, "payload": payload})
        self._raise_if_client_session_closed()
        response = await submit_binance_order_intent(
            self.futures,
            payload,
            snapshot=close_snapshot,
            environment=environment,
            source=f"stage2_lifecycle_monitor:{reason}",
            purpose="exit" if reason in {"timeout", "no_follow_through"} else "emergency",
            order_intent={
                **self.protected_position.order_intent,
                "symbol": self.protected_position.symbol,
                "side": self.protected_position.entry_side,
                "quantity": self.protected_position.position_size,
                "take_profit_price": self.protected_position.take_profit_price,
                "margin_type": self.protected_position.order_intent.get("margin_type") or "ISOLATED",
            },
            dry_run=False,
            risk_config=self.risk_config,
            log_dir=self.output_dir / "lifecycle_safe_order_gateway",
            audit_log_path=self.output_dir / "lifecycle_safe_order_gateway.jsonl",
            extra_context={
                "reason": f"stage2_micro_cycle_{reason}_reduce_only_close",
                "protective_stop_path_available": True,
                "emergency_close_path_available": True,
                "max_holding_time_sec": self.protected_position.max_holding_time_sec,
                "invalidation_condition": self.protected_position.invalidation_condition,
            },
        )
        self._append_event({"event": "reduce_only_close_submit_result", "reason": reason, "response": response})
        return response

    def _no_follow_through_exit_due(self, elapsed_sec: float, state: dict[str, Any]) -> bool:
        intent = self.protected_position.order_intent if isinstance(self.protected_position.order_intent, dict) else {}
        enabled = _optional_bool(
            intent.get("no_follow_through_exit_enabled")
            if "no_follow_through_exit_enabled" in intent
            else intent.get("no_follow_through_enabled")
        )
        if enabled is False:
            return False
        window = _optional_float(
            intent.get("no_follow_through_exit_sec")
            or intent.get("no_follow_through_window_sec")
            or intent.get("no_follow_through_after_sec")
        )
        window_sec = max(90.0, min(120.0, float(window if window is not None else 120.0)))
        if elapsed_sec < window_sec:
            return False
        entry_price = _optional_float(self.protected_position.entry_price)
        mark_price = _optional_float((state.get("mark_price") or {}).get("markPrice"))
        if entry_price is None or entry_price <= 0 or mark_price is None or mark_price <= 0:
            self._append_event({"event": "no_follow_through_unavailable", "entry_price": entry_price, "mark_price": mark_price})
            return False
        threshold = _optional_float(intent.get("no_follow_through_min_mfe_pct"))
        min_mfe_pct = max(0.0, float(threshold if threshold is not None else 0.0))
        if self.protected_position.direction == "ENTER_SHORT" or self.protected_position.entry_side == "SELL":
            favorable_pct = ((entry_price - mark_price) / entry_price) * 100.0
        else:
            favorable_pct = ((mark_price - entry_price) / entry_price) * 100.0
        due = favorable_pct <= min_mfe_pct
        self._append_event(
            {
                "event": "no_follow_through_check",
                "elapsed_sec": elapsed_sec,
                "window_sec": window_sec,
                "entry_price": entry_price,
                "mark_price": mark_price,
                "favorable_pct": round(favorable_pct, 6),
                "min_mfe_pct": min_mfe_pct,
                "due": due,
            }
        )
        return due

    async def _build_lifecycle_close_snapshot(self, protection_status: dict[str, Any]) -> dict[str, Any]:
        self._raise_if_client_session_closed()
        account_payload, positions_payload, exchange_payload = await _fetch_required_account_position_exchange_state(self.futures)
        source_snapshot = {
            **self.source_snapshot,
            "system_status": dict(self.source_snapshot.get("system_status") or {}),
        }
        source_status = source_snapshot["system_status"]
        protection_healthy = bool(protection_status.get("stop_loss_valid") and protection_status.get("take_profit_valid"))
        verified_at = _now_iso()
        protection_contract = _protection_contract(
            protection_status,
            self.protected_position,
            verified_at=verified_at,
        )
        source_status.update(
            {
                "source": "runtime",
                "snapshot_source": "runtime",
                "snapshot_time": verified_at,
                "trusted_runtime_snapshot": True,
                "data_fresh": True,
                "websocket_status": source_status.get("websocket_status") or "healthy",
                "exchange_status": "healthy" if isinstance(exchange_payload, dict) and exchange_payload.get("symbols") else "unknown",
                "account_state_source": "signed_account",
                "account_source": "signed_account",
                "position_state_source": "signed_positions",
                "position_state": "known",
                "stop_protection_status": "healthy" if protection_healthy else "unhealthy",
                "candidate_state": source_status.get("candidate_state") or "known",
                "lifecycle_exit_snapshot": True,
                "lifecycle_exit_snapshot_source": "signed_account+signed_positions+exchange_info+open_conditional_orders",
                **protection_contract,
            }
        )
        snapshot = _trusted_snapshot_with_signed_state(
            source_snapshot,
            account_payload=account_payload,
            positions_payload=positions_payload,
            exchange_payload=exchange_payload,
            protective_stop_path_available=source_status.get("protective_stop_path_available") is True,
            emergency_close_available=source_status.get("emergency_close_available") is True,
            protective_stop_capability_source=str(source_status.get("protective_stop_capability_source") or "unverified"),
            emergency_close_capability_source=str(source_status.get("emergency_close_capability_source") or "unverified"),
            require_symbol=self.protected_position.symbol,
        )
        snapshot["system_status"].update(
            {
                "stop_protection_status": "healthy" if protection_healthy else "unhealthy",
                "data_fresh": True,
                "snapshot_time": verified_at,
                "trusted_runtime_snapshot": True,
                "can_continue": True,
                "freeze_reason": None,
                **protection_contract,
            }
        )
        return snapshot

    async def _fetch_state(self) -> dict[str, Any]:
        self._raise_if_client_session_closed()
        symbol = self.protected_position.symbol
        positions = await self.futures.position_information_v3(symbol)
        all_positions = await self.futures.position_information_v3()
        open_orders = await self.futures.open_orders(symbol)
        all_open_orders = await self.futures.open_orders()
        conditional_orders = await self.futures.open_conditional_orders(symbol)
        all_conditional_orders = await self.futures.open_conditional_orders()
        mark = await self.futures.mark_price(symbol)
        user_trades = await _optional_user_trades(self.futures, symbol, start_time_ms=self.protected_position.entry_filled_time_ms)
        income_history = await _optional_income_history(self.futures, symbol, start_time_ms=self.protected_position.entry_filled_time_ms)
        return {
            "positions": positions if isinstance(positions, list) else [],
            "all_positions": all_positions if isinstance(all_positions, list) else [],
            "open_orders": open_orders if isinstance(open_orders, list) else [],
            "all_open_orders": all_open_orders if isinstance(all_open_orders, list) else [],
            "conditional_orders": conditional_orders if isinstance(conditional_orders, list) else [],
            "all_conditional_orders": all_conditional_orders if isinstance(all_conditional_orders, list) else [],
            "mark_price": mark if isinstance(mark, dict) else {},
            "user_trades": user_trades,
            "income_history": income_history,
        }

    async def _safe_fetch_state(self) -> dict[str, Any]:
        try:
            return await self._fetch_state()
        except Exception as exc:  # noqa: BLE001
            self._append_event({"event": "lifecycle_state_fetch_after_failure_failed", "error": str(exc)})
            return {}

    async def _wait_position_flat(self) -> dict[str, Any]:
        deadline = time.monotonic() + self.post_close_wait_sec
        state = await self._fetch_state()
        while time.monotonic() <= deadline:
            if _position_for_symbol(state["positions"], self.protected_position.symbol) is None:
                return state
            await self.sleep(min(1.0, self.poll_sec or 1.0))
            state = await self._fetch_state()
        return state

    async def _cancel_known_conditionals(self) -> list[dict[str, Any]]:
        self._raise_if_client_session_closed()
        cancellations: list[dict[str, Any]] = []
        orders = await self.futures.open_conditional_orders(self.protected_position.symbol)
        if not isinstance(orders, list):
            return cancellations
        for label, order_id, client_id in (
            ("stop_loss", self.protected_position.stop_loss_order_id, self.protected_position.stop_loss_client_id),
            ("take_profit", self.protected_position.take_profit_order_id, self.protected_position.take_profit_client_id),
        ):
            for order in orders:
                if not _matches_known_order(order, order_id=order_id, client_id=client_id):
                    continue
                response = await self.futures.cancel_conditional_order(
                    self.protected_position.symbol,
                    strategy_id=int(order_id) if order_id is not None and str(order_id).isdigit() else None,
                    client_strategy_id=client_id,
                )
                cancellation = {"label": label, "ok": True, "response": response}
                cancellations.append(cancellation)
                self._append_event({"event": "orphan_protective_order_cancelled", **cancellation})
        return cancellations

    def _build_final_report(
        self,
        *,
        final_close_reason: str,
        final_state: dict[str, Any],
        close_response: dict[str, Any] | None,
        emergency_close: bool,
        hard_freeze: bool,
        cancellations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        ending_positions = _open_positions(final_state.get("all_positions"))
        ending_open_orders = final_state.get("all_open_orders") if isinstance(final_state.get("all_open_orders"), list) else []
        ending_conditionals = final_state.get("all_conditional_orders") if isinstance(final_state.get("all_conditional_orders"), list) else []
        known_remaining = [
            order for order in (final_state.get("conditional_orders") or [])
            if _matches_known_order(order, order_id=self.protected_position.stop_loss_order_id, client_id=self.protected_position.stop_loss_client_id)
            or _matches_known_order(order, order_id=self.protected_position.take_profit_order_id, client_id=self.protected_position.take_profit_client_id)
        ]
        close_fill_price = _first_float(close_response or {}, "avgPrice", "price") or _latest_trade_price(
            final_state.get("user_trades"),
            side=self.protected_position.close_side,
        ) or _optional_float((final_state.get("mark_price") or {}).get("markPrice"))
        realized_pnl = _realized_pnl_usdt(final_state.get("user_trades"), final_state.get("income_history"))
        if realized_pnl is None:
            realized_pnl = _estimated_pnl(
                direction=self.protected_position.direction,
                entry_price=self.protected_position.entry_price,
                close_price=close_fill_price,
                quantity=self.protected_position.position_size,
            )
        fees = _fees_usdt(final_state.get("user_trades"))
        roi = _roi_pct(
            realized_pnl=realized_pnl,
            entry_price=self.protected_position.entry_price,
            quantity=self.protected_position.position_size,
            leverage=_optional_float(self.protected_position.order_intent.get("leverage")) or 1.0,
        )
        timeout_due_at = _iso_from_ms(self.protected_position.entry_filled_time_ms + self.protected_position.max_holding_time_sec * 1000)
        return {
            "run_dir": str(self.output_dir),
            "entry_order_id": self.protected_position.entry_order_id,
            "symbol": self.protected_position.symbol,
            "direction": self.protected_position.direction,
            "entry_price": self.protected_position.entry_price,
            "entry_fill_price": self.protected_position.entry_price,
            "position_size": self.protected_position.position_size,
            "stop_loss_order_id": self.protected_position.stop_loss_order_id,
            "take_profit_order_id": self.protected_position.take_profit_order_id,
            "stop_loss_price": self.protected_position.stop_loss_price,
            "take_profit_price": self.protected_position.take_profit_price,
            "final_close_reason": final_close_reason,
            "close_reason": final_close_reason,
            "final_close_price": close_fill_price,
            "close_fill_price": close_fill_price,
            "realized_pnl_usdt": realized_pnl,
            "roi_pct": roi,
            "fees": fees,
            "holding_time_sec": self._elapsed_sec(),
            "max_holding_sec": self.protected_position.max_holding_time_sec,
            "timeout_due_at": timeout_due_at,
            "emergency_close_triggered": emergency_close,
            "hard_freeze": hard_freeze,
            "timeout_close_response": close_response if final_close_reason == "CLOSED_BY_TIMEOUT" else None,
            "no_follow_through_close_response": close_response if final_close_reason == "CLOSED_BY_NO_FOLLOW_THROUGH" else None,
            "no_follow_through_exit": final_close_reason == "CLOSED_BY_NO_FOLLOW_THROUGH",
            "emergency_close_response": close_response if emergency_close else None,
            "protective_order_cancellations": cancellations,
            "ending_positions_count": len(ending_positions),
            "ending_positions": ending_positions,
            "ending_open_orders_count": len(ending_open_orders),
            "ending_open_orders": ending_open_orders,
            "ending_conditional_orders_count": len(ending_conditionals),
            "ending_conditional_orders": ending_conditionals,
            "orphan_protective_order": bool(known_remaining),
            "known_protective_orders_remaining": known_remaining,
            "mainnet_order_submitted": False,
            "cleanup": False,
            "auto_promotion": False,
            "close_snapshot_status": self.last_close_snapshot_status or {},
            "lifecycle_events_path": str(self.events_path),
            "safe_order_gateway_log_path": str(self.output_dir / "lifecycle_safe_order_gateway.jsonl"),
            "report_path": str(self.report_path),
        }

    def _build_review_report(self, result: dict[str, Any]):
        if result.get("hard_freeze"):
            return build_review_report(
                "HARD_FREEZE",
                {
                    "symbol": self.protected_position.symbol,
                    "action": self.protected_position.direction,
                    "reason": result.get("freeze_reason") or result.get("final_close_reason") or "lifecycle_monitor_hard_freeze",
                    "freeze_reason": result.get("freeze_reason") or result.get("final_close_reason") or "lifecycle_monitor_hard_freeze",
                },
            )
        return build_review_report(
            "CLOSED",
            {
                "symbol": self.protected_position.symbol,
                "action": self.protected_position.direction,
                "side": self.protected_position.entry_side,
                "entry_price": result.get("entry_fill_price"),
                "exit_price": result.get("close_fill_price"),
                "pnl_usdt": result.get("realized_pnl_usdt"),
                "roi_pct": result.get("roi_pct"),
                "fees": result.get("fees"),
                "holding_time_sec": result.get("holding_time_sec"),
                "exit_reason": result.get("final_close_reason"),
                "stop_loss_price": self.protected_position.stop_loss_price,
                "take_profit_price": self.protected_position.take_profit_price,
                "max_holding_time_sec": self.protected_position.max_holding_time_sec,
                "timeout_due_at": result.get("timeout_due_at"),
                "review": "Stage 2 testnet micro cycle 已结束，保护单 reconciliation 已记录。",
            },
        )

    def _client_session_closed(self) -> bool:
        session = getattr(self.futures, "session", None)
        return bool(getattr(session, "closed", False))

    def _raise_if_client_session_closed(self) -> None:
        if self._client_session_closed():
            raise RuntimeError("lifecycle_client_session_closed")

    def _classify_exchange_close(self, state: dict[str, Any]) -> str:
        mark = _optional_float((state.get("mark_price") or {}).get("markPrice"))
        if mark is not None and self.protected_position.take_profit_price is not None and mark >= self.protected_position.take_profit_price:
            return "CLOSED_BY_TP"
        if mark is not None and self.protected_position.stop_loss_price is not None and mark <= self.protected_position.stop_loss_price:
            return "CLOSED_BY_STOP"
        return "CLOSED_BY_INVALIDATION"

    def _elapsed_sec(self) -> float:
        return round(max(0.0, (self.now_ms() - self.protected_position.entry_filled_time_ms) / 1000.0), 3)

    def _append_event(self, event: dict[str, Any]) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        row = {"created_at": _now_iso(), **event}
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n")


def _matches_known_order(order: dict[str, Any], *, order_id: Any, client_id: Any) -> bool:
    ids = {str(order.get("algoId") or ""), str(order.get("strategyId") or ""), str(order.get("orderId") or "")}
    clients = {
        str(order.get("clientAlgoId") or ""),
        str(order.get("newClientStrategyId") or ""),
        str(order.get("clientOrderId") or ""),
    }
    return (order_id is not None and str(order_id) in ids) or (client_id is not None and str(client_id) in clients)


def _valid_protective_order(order: dict[str, Any] | None, *, expected_type: str, close_side: str) -> bool:
    if not order:
        return False
    return (
        str(order.get("algoStatus") or "").upper() in {"NEW", "PARTIALLY_FILLED"}
        and str(order.get("side") or "").upper() == close_side.upper()
        and _truthy(order.get("reduceOnly"))
        and _truthy(order.get("closePosition"))
        and expected_type in str(order.get("orderType") or order.get("type") or "").upper()
    )


def _protection_contract(
    protection_status: dict[str, Any],
    protected_position: Stage2ProtectedPosition,
    *,
    verified_at: str,
) -> dict[str, Any]:
    stop = protection_status.get("stop_loss_order") if isinstance(protection_status.get("stop_loss_order"), dict) else {}
    take_profit = protection_status.get("take_profit_order") if isinstance(protection_status.get("take_profit_order"), dict) else {}
    return {
        "stop_order_id": _first_present(stop, "algoId", "strategyId", "orderId") or protected_position.stop_loss_order_id,
        "stop_client_order_id": _first_present(stop, "clientAlgoId", "newClientStrategyId", "clientOrderId") or protected_position.stop_loss_client_id,
        "stop_status": _first_present(stop, "algoStatus", "status") or "MISSING",
        "stop_reduce_only": _truthy(stop.get("reduceOnly")),
        "stop_close_position": _truthy(stop.get("closePosition")),
        "stop_type": _first_present(stop, "orderType", "type") or "MISSING",
        "take_profit_order_id": _first_present(take_profit, "algoId", "strategyId", "orderId")
        or protected_position.take_profit_order_id,
        "take_profit_client_order_id": _first_present(take_profit, "clientAlgoId", "newClientStrategyId", "clientOrderId")
        or protected_position.take_profit_client_id,
        "take_profit_status": _first_present(take_profit, "algoStatus", "status") or "MISSING",
        "take_profit_reduce_only": _truthy(take_profit.get("reduceOnly")),
        "take_profit_close_position": _truthy(take_profit.get("closePosition")),
        "take_profit_type": _first_present(take_profit, "orderType", "type") or "MISSING",
        "protection_verified_at": verified_at,
        "protection_verification_source": "open_conditional_orders",
        "lifecycle_exit_snapshot": True,
    }


def _position_for_symbol(rows: Any, symbol: str) -> dict[str, Any] | None:
    for row in _open_positions(rows):
        if str(row.get("symbol") or "").upper() == symbol.upper():
            return row
    return None


def _open_positions(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    return [row for row in rows if abs(_decimal(row.get("positionAmt") or row.get("quantity"))) > Decimal("0")]


async def _optional_user_trades(futures: Any, symbol: str, *, start_time_ms: int) -> list[dict[str, Any]]:
    method = getattr(futures, "user_trades", None)
    if not callable(method):
        return []
    try:
        value = await method(symbol, start_time_ms=start_time_ms, limit=100)
    except Exception:  # noqa: BLE001
        return []
    return value if isinstance(value, list) else []


async def _optional_income_history(futures: Any, symbol: str, *, start_time_ms: int) -> list[dict[str, Any]]:
    method = getattr(futures, "income_history", None)
    if not callable(method):
        return []
    try:
        value = await method(symbol, start_time_ms=start_time_ms, limit=100)
    except Exception:  # noqa: BLE001
        return []
    return value if isinstance(value, list) else []


def _latest_trade_price(rows: Any, *, side: str) -> float | None:
    if not isinstance(rows, list):
        return None
    for row in reversed(rows):
        if str(row.get("side") or "").upper() == side.upper():
            value = _optional_float(row.get("price") or row.get("avgPrice"))
            if value is not None:
                return value
    return None


def _realized_pnl_usdt(trades: Any, income_rows: Any) -> float | None:
    values: list[float] = []
    if isinstance(trades, list):
        for row in trades:
            value = _optional_float(row.get("realizedPnl") or row.get("realized_pnl"))
            if value is not None:
                values.append(value)
    if values:
        return round(sum(values), 8)
    if isinstance(income_rows, list):
        for row in income_rows:
            if str(row.get("incomeType") or row.get("income_type") or "").upper() == "REALIZED_PNL":
                value = _optional_float(row.get("income"))
                if value is not None:
                    values.append(value)
    if values:
        return round(sum(values), 8)
    return None


def _fees_usdt(trades: Any) -> float:
    values: list[float] = []
    if isinstance(trades, list):
        for row in trades:
            value = _optional_float(row.get("commission") or row.get("fee"))
            if value is not None:
                values.append(abs(value))
    return round(sum(values), 8)


def _estimated_pnl(*, direction: str, entry_price: float | None, close_price: float | None, quantity: float) -> float | None:
    if entry_price is None or close_price is None:
        return None
    sign = Decimal("1") if str(direction or "").upper() == "ENTER_LONG" else Decimal("-1")
    pnl = (Decimal(str(close_price)) - Decimal(str(entry_price))) * Decimal(str(quantity)) * sign
    return round(float(pnl), 8)


def _roi_pct(*, realized_pnl: float | None, entry_price: float | None, quantity: float, leverage: float) -> float | None:
    if realized_pnl is None or entry_price is None or quantity <= 0:
        return None
    notional = Decimal(str(entry_price)) * Decimal(str(quantity))
    if notional <= 0:
        return None
    margin = notional / Decimal(str(max(leverage, 1.0)))
    if margin <= 0:
        return None
    return round(float((Decimal(str(realized_pnl)) / margin) * Decimal("100")), 6)


def _iso_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _first_float(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _optional_float(payload.get(key))
        if value is not None and value > 0:
            return value
    return None


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value
