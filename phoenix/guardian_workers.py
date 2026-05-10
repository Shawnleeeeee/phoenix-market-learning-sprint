from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix.binance_futures import BinanceFuturesClient
from phoenix.config import load_credentials, load_execution_settings, load_proxy_settings
from phoenix.executor import PhoenixExecutor
from phoenix.guardian_launch import spawn_guardian_worker
from phoenix.models import OrderInstruction, TradeIntent
from phoenix.safe_order_gateway import build_gateway_snapshot, submit_binance_order_intent


def load_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def save_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_position_side(side: Any) -> str:
    normalized = str(side or "").upper()
    if normalized in {"LONG", "SHORT", "BOTH"}:
        return normalized
    return "BOTH"


def _extract_nonzero_positions(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    active: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        amount = (
            _as_float(item.get("positionAmt"))
            or _as_float(item.get("position_amount"))
            or _as_float(item.get("quantity"))
            or 0.0
        )
        if abs(amount) < 1e-12:
            continue
        active.append(item)
    return active


async def _fetch_active_positions_async() -> list[dict[str, Any]] | None:
    import aiohttp

    credentials = load_credentials(required=False)
    if credentials is None:
        return None
    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceFuturesClient(
            session=session,
            environment=credentials.environment,
            credentials=credentials,
            proxy_settings=load_proxy_settings(),
        )
        payload = await client.position_information_v3()
        return _extract_nonzero_positions(payload)


def fetch_active_positions() -> list[dict[str, Any]] | None:
    try:
        return asyncio.run(_fetch_active_positions_async())
    except Exception:
        return None


def worker_matches_positions(worker: dict[str, Any], positions: list[dict[str, Any]]) -> bool:
    intent = worker.get("intent") if isinstance(worker.get("intent"), dict) else {}
    symbol = str(intent.get("symbol") or "").upper()
    side = str(intent.get("side") or "").upper()
    if not symbol or side not in {"BUY", "SELL"}:
        return False

    for position in positions:
        if str(position.get("symbol") or "").upper() != symbol:
            continue
        quantity = (
            _as_float(position.get("positionAmt"))
            or _as_float(position.get("position_amount"))
            or _as_float(position.get("quantity"))
            or 0.0
        )
        position_side = _normalize_position_side(position.get("positionSide"))
        if side == "BUY":
            if position_side == "LONG" and quantity > 0:
                return True
            if position_side == "BOTH" and quantity > 0:
                return True
        if side == "SELL":
            if position_side == "SHORT" and quantity < 0:
                return True
            if position_side == "BOTH" and quantity < 0:
                return True
    return False


def position_identity(position: dict[str, Any]) -> tuple[str, str]:
    symbol = str(position.get("symbol") or "").upper()
    quantity = (
        _as_float(position.get("positionAmt"))
        or _as_float(position.get("position_amount"))
        or _as_float(position.get("quantity"))
        or 0.0
    )
    side = "BUY" if quantity > 0 else "SELL"
    return symbol, side


def worker_identity(worker: dict[str, Any]) -> tuple[str, str] | None:
    intent = worker.get("intent") if isinstance(worker.get("intent"), dict) else {}
    symbol = str(intent.get("symbol") or "").upper()
    side = str(intent.get("side") or "").upper()
    if not symbol or side not in {"BUY", "SELL"}:
        return None
    return symbol, side


def mark_worker_stale(
    worker_path: Path,
    worker: dict[str, Any],
    *,
    reason: str,
    positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    payload = dict(worker)
    payload["status"] = "stale"
    payload["phase"] = "STALE_NO_POSITION"
    payload["updated_at"] = now
    payload["closed_at"] = payload.get("closed_at") or now
    payload["stale_reason"] = reason
    if positions is not None:
        payload["exchange_position_snapshot"] = positions
    save_json_file(worker_path, payload)
    return payload


def _round_price(value: float, tick_size: float) -> float:
    from decimal import Decimal, ROUND_DOWN

    decimal_value = Decimal(str(value))
    decimal_tick = Decimal(str(tick_size))
    return float((decimal_value / decimal_tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_tick)


def _apply_pct(price: float, pct: float, *, subtract: bool) -> float:
    from decimal import Decimal

    decimal_price = Decimal(str(price))
    decimal_pct = Decimal(str(pct)) / Decimal("100")
    multiplier = Decimal("1") - decimal_pct if subtract else Decimal("1") + decimal_pct
    return float(decimal_price * multiplier)


def _find_conditional_order(
    orders: list[dict[str, Any]],
    *,
    strategy_type: str,
    symbol: str,
    side: str,
    position_side: str | None,
) -> dict[str, Any] | None:
    for order in orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("symbol") or "").upper() != symbol.upper():
            continue
        order_type = str(
            order.get("strategyType")
            or order.get("origType")
            or order.get("type")
            or ""
        ).upper()
        if order_type != strategy_type.upper():
            continue
        if str(order.get("side") or "").upper() != side.upper():
            continue
        if position_side and str(order.get("positionSide") or "").upper() not in {"", position_side.upper()}:
            continue
        return order
    return None


def _spawn_worker(job_path: Path, log_path: Path) -> dict[str, Any]:
    started = spawn_guardian_worker(job_path, log_path)
    return {
        "pid": started.get("pid"),
        "unit_name": started.get("unit_name"),
        "launcher": started.get("launcher"),
        "log_file": str(log_path),
        "job_file": str(job_path),
    }


async def _reattach_orphan_position_async(
    *,
    workers_dir: Path,
    client: BinanceFuturesClient,
    position: dict[str, Any],
) -> dict[str, Any] | None:
    settings = load_execution_settings()
    symbol, side = position_identity(position)
    quantity = abs(
        _as_float(position.get("positionAmt"))
        or _as_float(position.get("position_amount"))
        or _as_float(position.get("quantity"))
        or 0.0
    )
    if not symbol or quantity <= 0:
        return None

    entry_price = _as_float(position.get("entryPrice")) or _as_float(position.get("entry_price"))
    if entry_price in (None, 0.0):
        return None

    leverage = int(_as_float(position.get("leverage")) or settings.leverage)
    account_api_mode = await client.get_account_api_mode()
    position_mode = settings.position_mode
    margin_type = settings.margin_type
    if account_api_mode == "portfolio_margin":
        account_config = await client.um_account_config()
        position_mode = "HEDGE" if bool(account_config.get("dualSidePosition")) else "ONE_WAY"
        symbol_config_payload = await client.um_symbol_config(symbol)
        if isinstance(symbol_config_payload, list):
            for item in symbol_config_payload:
                if str(item.get("symbol") or "").upper() == symbol.upper():
                    value = str(item.get("marginType") or "").upper()
                    if value:
                        margin_type = value
                    break

    executor = PhoenixExecutor(client, settings)
    rules = await executor.get_symbol_rules(symbol)
    if side == "BUY":
        initial_stop_reference = _apply_pct(entry_price, settings.initial_stop_loss_pct, subtract=True)
        take_profit_reference = (
            _apply_pct(entry_price, settings.take_profit_pct, subtract=False)
            if settings.take_profit_pct > 0
            else None
        )
        breakeven_trigger_reference = _apply_pct(entry_price, settings.breakeven_trigger_pct, subtract=False)
        breakeven_stop_reference = _apply_pct(entry_price, settings.breakeven_lock_pct, subtract=False)
    else:
        initial_stop_reference = _apply_pct(entry_price, settings.initial_stop_loss_pct, subtract=False)
        take_profit_reference = (
            _apply_pct(entry_price, settings.take_profit_pct, subtract=True)
            if settings.take_profit_pct > 0
            else None
        )
        breakeven_trigger_reference = _apply_pct(entry_price, settings.breakeven_trigger_pct, subtract=True)
        breakeven_stop_reference = _apply_pct(entry_price, settings.breakeven_lock_pct, subtract=True)

    intent = TradeIntent(
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        quantity=quantity,
        leverage=leverage,
        quote_allocation_usdt=(quantity * entry_price / leverage) if leverage else quantity * entry_price,
        notional_usdt=quantity * entry_price,
        margin_type=margin_type,
        working_type=settings.working_type,
        initial_stop_price=_round_price(initial_stop_reference, rules.tick_size),
        take_profit_price=(
            _round_price(take_profit_reference, rules.tick_size)
            if take_profit_reference is not None
            else None
        ),
        breakeven_trigger_price=_round_price(breakeven_trigger_reference, rules.tick_size),
        breakeven_stop_price=_round_price(breakeven_stop_reference, rules.tick_size),
        trailing_callback_rate=settings.trailing_callback_pct,
        allocation_mode="REATTACH",
        allocation_cap_usdt=None,
        risk_budget_usdt=None,
        rules=rules,
    )
    plan = executor.build_order_plan(intent, account_api_mode=account_api_mode, position_mode=position_mode)
    exit_side = "SELL" if side == "BUY" else "BUY"
    exit_position_side = executor._exit_position_side(intent.side, hedge_mode=position_mode.upper() == "HEDGE")
    conditional_orders = await client.open_conditional_orders(symbol)
    stop_order = _find_conditional_order(
        conditional_orders if isinstance(conditional_orders, list) else [],
        strategy_type="STOP_MARKET",
        symbol=symbol,
        side=exit_side,
        position_side=exit_position_side,
    )
    trailing_order = _find_conditional_order(
        conditional_orders if isinstance(conditional_orders, list) else [],
        strategy_type="TRAILING_STOP_MARKET",
        symbol=symbol,
        side=exit_side,
        position_side=exit_position_side,
    )
    if stop_order is None:
        stop_instruction = next(item for item in plan if item.name == "initial_protective_stop")
        stop_order = await submit_binance_order_intent(
            client,
            stop_instruction.payload,
            snapshot=build_gateway_snapshot(
                symbol=symbol,
                side=exit_side,
                positions=[{"symbol": symbol, "side": "LONG" if side == "BUY" else "SHORT", "protection_status": "healthy"}],
                data_fresh=True,
                websocket_status="healthy",
                exchange_status="healthy",
                position_state="known",
                stop_protection_status="healthy",
                protective_stop_path_available=True,
                emergency_close_available=True,
            ),
            environment={
                "runtime_mode": "TESTNET_LIVE",
                "env": getattr(getattr(client, "environment", None), "name", None) or "testnet",
            },
            source="phoenix_guardian_workers:reattach_stop",
            purpose="reattach",
            endpoint=stop_instruction.endpoint,
            order_intent=intent,
            dry_run=False,
            extra_context={
                "protective_stop_path_available": True,
                "emergency_close_path_available": True,
            },
        )

    now = datetime.now(timezone.utc)
    job_id = f"REATTACH-{symbol}-{int(now.timestamp())}"
    job_path = workers_dir / f"{job_id}.json"
    logs_dir = workers_dir.parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"phoenix_guardian_{job_id}.log"
    phase = "INITIAL_STOP"
    breakeven_lock: dict[str, Any] | None = None
    stop_price = _as_float(stop_order.get("stopPrice"))
    if trailing_order is not None or (stop_price is not None and abs(stop_price - intent.breakeven_stop_price) <= max(intent.rules.tick_size * 3, intent.entry_price * 0.0005)):
        phase = "BREAKEVEN_AND_TRAILING_ARMED" if trailing_order is not None else "BREAKEVEN_ONLY"
        breakeven_lock = {
            "triggered_at": now.isoformat(),
            "mark_price": _as_float(position.get("markPrice")) or _as_float(position.get("mark_price")),
            "action": "reattach_detected",
            "breakeven_stop_response": stop_order,
        }
        if trailing_order is not None:
            breakeven_lock["trailing_stop_response"] = trailing_order

    payload: dict[str, Any] = {
        "job_id": job_id,
        "confirmation_token": None,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "status": "running",
        "phase": phase,
        "account_api_mode": account_api_mode,
        "position_mode": position_mode,
        "intent": intent.to_dict(),
        "candidate": None,
        "strategy_gate": {
            "applied": False,
            "ok": True,
            "reason": "orphan_position_reattach",
        },
        "dispatch_reason": "orphan_position_reattach",
        "entry_market": {
            "avgPrice": entry_price,
            "orderId": position.get("orderId"),
            "updateTime": now.isoformat(),
        },
        "initial_protective_stop": stop_order,
        "remaining_plan": [
            item.to_dict() for item in plan if item.name not in {"entry_market", "initial_protective_stop"}
        ],
        "poll_interval_sec": settings.guardian_poll_interval_sec,
        "max_runtime_sec": settings.guardian_max_runtime_sec,
        "reattached_from_position": position,
    }
    if breakeven_lock is not None:
        payload["breakeven_lock"] = breakeven_lock
    save_json_file(job_path, payload)
    spawn_result = _spawn_worker(job_path, log_path)
    payload["spawn"] = spawn_result
    save_json_file(job_path, payload)
    return payload


async def _latest_running_worker_async(workers_dir: Path) -> dict[str, Any] | None:
    running_records: list[tuple[float, Path, dict[str, Any]]] = []
    for record_path in workers_dir.glob("*.json"):
        payload = load_json_file(record_path)
        if not isinstance(payload, dict):
            continue
        if str(payload.get("status") or "").lower() != "running":
            continue
        try:
            mtime = record_path.stat().st_mtime
        except OSError:
            continue
        running_records.append((mtime, record_path, payload))

    import aiohttp

    credentials = load_credentials(required=False)
    if credentials is None:
        if not running_records:
            return None
        newest = max(running_records, key=lambda item: item[0])
        return newest[2]

    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceFuturesClient(
            session=session,
            environment=credentials.environment,
            credentials=credentials,
            proxy_settings=load_proxy_settings(),
        )
        positions_payload = await client.position_information_v3()
        positions = _extract_nonzero_positions(positions_payload)

        valid: list[tuple[float, Path, dict[str, Any]]] = []
        matched_identities: set[tuple[str, str]] = set()
        for mtime, record_path, payload in running_records:
            if worker_matches_positions(payload, positions):
                valid.append((mtime, record_path, payload))
                identity = worker_identity(payload)
                if identity is not None:
                    matched_identities.add(identity)
                continue
            mark_worker_stale(
                record_path,
                payload,
                reason="no_matching_exchange_position",
                positions=positions,
            )

        for position in positions:
            identity = position_identity(position)
            if identity in matched_identities:
                continue
            reattached = await _reattach_orphan_position_async(
                workers_dir=workers_dir,
                client=client,
                position=position,
            )
            if reattached is not None:
                valid.append((datetime.now(timezone.utc).timestamp(), workers_dir / f"{reattached['job_id']}.json", reattached))
                matched_identities.add(identity)

        if not valid:
            return None
        newest = max(valid, key=lambda item: item[0])
        return newest[2]


def latest_running_worker(workers_dir: Path) -> dict[str, Any] | None:
    try:
        return asyncio.run(_latest_running_worker_async(workers_dir))
    except Exception:
        positions = fetch_active_positions()
        if positions is None:
            return None
        for position in positions:
            symbol, side = position_identity(position)
            return {
                "symbol": symbol,
                "side": side,
                "phase": "ORPHAN_POSITION",
                "status": "running",
                "reattach_failed": True,
            }
        return None
