from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, replace
from typing import Any

from phoenix.models import OrderInstruction, TradeIntent


TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def _quantity_text(value: Any) -> str | None:
    raw = str(value or "").strip()
    quantity = _safe_float(raw)
    if quantity is None or quantity <= 0:
        return None
    if raw and raw not in {"0", "0.0", "0.00", "0.00000000"}:
        return raw
    return f"{quantity:.12f}".rstrip("0").rstrip(".")


@dataclass(slots=True)
class OrderFillState:
    symbol: str
    side: str | None
    order_id: int | None
    client_order_id: str | None
    status: str
    execution_type: str | None
    order_type: str | None
    original_qty: float | None
    executed_qty: float
    executed_qty_text: str | None
    last_filled_qty: float | None
    avg_price: float | None
    last_filled_price: float | None
    reduce_only: bool
    close_position: bool
    position_side: str | None
    event_time_ms: int | None
    raw: dict[str, Any]

    @property
    def is_terminal(self) -> bool:
        return self.status.upper() in TERMINAL_ORDER_STATUSES

    @property
    def is_filled(self) -> bool:
        return self.status.upper() == "FILLED"

    @property
    def is_partially_filled(self) -> bool:
        return self.executed_qty > 0 and not self.is_filled


def extract_order_update_state(payload: dict[str, Any]) -> OrderFillState | None:
    if not isinstance(payload, dict) or str(payload.get("e") or "") != "ORDER_TRADE_UPDATE":
        return None
    order = payload.get("o")
    if not isinstance(order, dict):
        return None
    executed_qty_text = _quantity_text(order.get("z"))
    avg_price = _safe_float(order.get("ap"))
    if avg_price is not None and avg_price <= 0:
        avg_price = None
    last_filled_price = _safe_float(order.get("L"))
    if avg_price is None and last_filled_price is not None and last_filled_price > 0:
        avg_price = last_filled_price
    return OrderFillState(
        symbol=str(order.get("s") or "").upper(),
        side=str(order.get("S") or "").upper() or None,
        order_id=_safe_int(order.get("i")),
        client_order_id=str(order.get("c") or "").strip() or None,
        status=str(order.get("X") or "").upper(),
        execution_type=str(order.get("x") or "").upper() or None,
        order_type=str(order.get("o") or "").upper() or None,
        original_qty=_safe_float(order.get("q")),
        executed_qty=_safe_float(order.get("z")) or 0.0,
        executed_qty_text=executed_qty_text,
        last_filled_qty=_safe_float(order.get("l")),
        avg_price=avg_price,
        last_filled_price=last_filled_price,
        reduce_only=_truthy(order.get("R")),
        close_position=_truthy(order.get("cp")),
        position_side=str(order.get("ps") or "").upper() or None,
        event_time_ms=_safe_int(payload.get("E")) or _safe_int(order.get("T")) or _safe_int(payload.get("T")),
        raw=payload,
    )


def extract_order_response_state(payload: dict[str, Any]) -> OrderFillState | None:
    if not isinstance(payload, dict):
        return None
    symbol = str(payload.get("symbol") or "").upper()
    if not symbol:
        return None
    executed_qty_text = _quantity_text(payload.get("executedQty"))
    avg_price = _safe_float(payload.get("avgPrice"))
    if avg_price is not None and avg_price <= 0:
        avg_price = None
    return OrderFillState(
        symbol=symbol,
        side=str(payload.get("side") or "").upper() or None,
        order_id=_safe_int(payload.get("orderId")),
        client_order_id=str(payload.get("clientOrderId") or "").strip() or None,
        status=str(payload.get("status") or "").upper(),
        execution_type=None,
        order_type=str(payload.get("type") or payload.get("origType") or "").upper() or None,
        original_qty=_safe_float(payload.get("origQty")),
        executed_qty=_safe_float(payload.get("executedQty")) or 0.0,
        executed_qty_text=executed_qty_text,
        last_filled_qty=None,
        avg_price=avg_price,
        last_filled_price=None,
        reduce_only=_truthy(payload.get("reduceOnly")),
        close_position=_truthy(payload.get("closePosition")),
        position_side=str(payload.get("positionSide") or "").upper() or None,
        event_time_ms=_safe_int(payload.get("updateTime")) or _safe_int(payload.get("time")),
        raw=payload,
    )


def choose_reconciled_entry_state(
    response_state: OrderFillState | None,
    stream_state: OrderFillState | None,
) -> OrderFillState | None:
    if response_state is None:
        return stream_state
    if stream_state is None:
        return response_state
    if stream_state.executed_qty > response_state.executed_qty:
        return stream_state
    if stream_state.executed_qty == response_state.executed_qty and stream_state.is_terminal:
        return stream_state
    return response_state


def classify_entry_reconciliation(fill_state: OrderFillState | None) -> str:
    if fill_state is None:
        return "missing_entry_state"
    if fill_state.executed_qty <= 0:
        return "no_fill_cancel_residual"
    if fill_state.is_filled:
        return "full_fill"
    return "partial_fill_cancel_residual_sync_protection"


def build_reconciled_intent(intent: TradeIntent, fill_state: OrderFillState) -> TradeIntent:
    if fill_state.executed_qty <= 0:
        return intent
    avg_price = fill_state.avg_price if fill_state.avg_price is not None and fill_state.avg_price > 0 else intent.entry_price
    return replace(
        intent,
        quantity=fill_state.executed_qty,
        entry_price=avg_price,
        notional_usdt=fill_state.executed_qty * avg_price,
    )


def sync_instruction_quantity_to_fill(
    instruction: OrderInstruction,
    fill_state: OrderFillState,
) -> OrderInstruction:
    quantity_text = fill_state.executed_qty_text or _quantity_text(fill_state.executed_qty)
    if quantity_text is None or instruction.name == "entry_market":
        return instruction
    payload = dict(instruction.payload)
    if "quantity" in payload:
        payload["quantity"] = quantity_text
    return replace(instruction, payload=payload)


def sync_plan_quantities_to_fill(
    plan: list[OrderInstruction],
    fill_state: OrderFillState,
) -> list[OrderInstruction]:
    return [sync_instruction_quantity_to_fill(item, fill_state) for item in plan]


class UserDataOMSState:
    def __init__(self, *, max_events: int = 512) -> None:
        self.max_events = max(32, int(max_events))
        self.recent: deque[OrderFillState] = deque(maxlen=self.max_events)
        self.by_order_id: dict[int, OrderFillState] = {}
        self.by_client_order_id: dict[str, OrderFillState] = {}
        self._event = asyncio.Event()

    def apply_event(self, payload: dict[str, Any]) -> OrderFillState | None:
        update = extract_order_update_state(payload)
        if update is None:
            return None
        self.recent.append(update)
        if update.order_id is not None:
            self.by_order_id[update.order_id] = update
        if update.client_order_id:
            self.by_client_order_id[update.client_order_id] = update
        self._event.set()
        self._event = asyncio.Event()
        return update

    def latest(
        self,
        *,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> OrderFillState | None:
        if order_id is not None and order_id in self.by_order_id:
            return self.by_order_id[order_id]
        if client_order_id and client_order_id in self.by_client_order_id:
            return self.by_client_order_id[client_order_id]
        return None

    async def wait_for_update(
        self,
        *,
        order_id: int | None = None,
        client_order_id: str | None = None,
        timeout_sec: float = 1.0,
    ) -> OrderFillState | None:
        existing = self.latest(order_id=order_id, client_order_id=client_order_id)
        if existing is not None:
            return existing
        deadline = asyncio.get_running_loop().time() + max(0.0, float(timeout_sec))
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return self.latest(order_id=order_id, client_order_id=client_order_id)
            try:
                await asyncio.wait_for(self._event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return self.latest(order_id=order_id, client_order_id=client_order_id)
            current = self.latest(order_id=order_id, client_order_id=client_order_id)
            if current is not None:
                return current

    def summary(self) -> dict[str, Any]:
        latest = self.recent[-1] if self.recent else None
        return {
            "tracked_order_count": len(self.by_order_id),
            "tracked_client_order_count": len(self.by_client_order_id),
            "recent_event_count": len(self.recent),
            "latest": (
                {
                    "symbol": latest.symbol,
                    "side": latest.side,
                    "order_id": latest.order_id,
                    "client_order_id": latest.client_order_id,
                    "status": latest.status,
                    "execution_type": latest.execution_type,
                    "executed_qty": latest.executed_qty,
                    "event_time_ms": latest.event_time_ms,
                }
                if latest is not None
                else None
            ),
        }
