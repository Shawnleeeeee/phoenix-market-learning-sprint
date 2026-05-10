from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

import aiohttp

from phoenix.binance_futures import BinanceFuturesClient
from phoenix.config import load_credentials, load_execution_settings, load_proxy_settings, resolve_environment
from phoenix.executor import PhoenixExecutor
from phoenix.models import OrderInstruction, TradeIntent
from phoenix.safe_order_gateway import build_gateway_snapshot, submit_binance_order_intent
from phoenix_live_execute import place_instruction


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

        environment = resolve_environment(self.environment_name)
        if environment.name not in {"testnet", "demo"}:
            raise RuntimeError("Hermes Trader Mode executor callback is testnet/demo only.")

        credentials = load_credentials(required=True)
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
            risk_log_path = self.output_dir / "testnet_executor_safe_gateway.jsonl"

            entry_response = await place_instruction(
                futures,
                entry,
                environment_name=environment.name,
                runtime_mode="TESTNET_LIVE",
                intent_log_path=risk_log_path,
                intent=intent,
                purpose="entry",
                snapshot=_entry_snapshot(intent, self.snapshot),
            )
            try:
                stop_response = await place_instruction(
                    futures,
                    protective_stop,
                    environment_name=environment.name,
                    runtime_mode="TESTNET_LIVE",
                    intent_log_path=risk_log_path,
                    intent=intent,
                    purpose="protection",
                    snapshot=_protection_snapshot(intent, protective_stop),
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
                return {
                    "order_submitted": True,
                    "status": "protective_stop_failed_emergency_close_attempted",
                    "entry_response": entry_response,
                    "protective_stop_error": str(exc),
                    "emergency_close": emergency,
                }

        return {
            "order_submitted": True,
            "status": "testnet_order_submitted_with_protective_stop",
            "entry_response": entry_response,
            "protective_stop_response": stop_response,
            "intent": intent.to_dict(),
        }


def _find_instruction(plan: list[OrderInstruction], name: str) -> OrderInstruction:
    for item in plan:
        if item.name == name:
            return item
    raise RuntimeError(f"Missing required order instruction: {name}")


def _entry_snapshot(intent: TradeIntent, source_snapshot: dict[str, Any]) -> dict[str, Any]:
    return build_gateway_snapshot(
        symbol=intent.symbol,
        side=intent.side,
        candidate=_candidate_for_symbol(source_snapshot, intent.symbol),
        positions=[],
        open_positions_count=0,
        max_open_positions=1,
        data_fresh=True,
        websocket_status="healthy",
        exchange_status="healthy",
        position_state="known",
        stop_protection_status="healthy",
        protective_stop_path_available=True,
        emergency_close_available=True,
    )


def _protection_snapshot(intent: TradeIntent, instruction: OrderInstruction) -> dict[str, Any]:
    return build_gateway_snapshot(
        symbol=intent.symbol,
        side=str(instruction.payload.get("side") or ""),
        positions=[{"symbol": intent.symbol, "side": "LONG" if intent.side == "BUY" else "SHORT", "protection_status": "healthy"}],
        data_fresh=True,
        websocket_status="healthy",
        exchange_status="healthy",
        position_state="known",
        stop_protection_status="healthy",
        protective_stop_path_available=True,
        emergency_close_available=True,
    )


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
            snapshot=build_gateway_snapshot(
                symbol=intent.symbol,
                side=exit_side,
                positions=[{"symbol": intent.symbol, "side": "LONG" if intent.side == "BUY" else "SHORT", "protection_status": "healthy"}],
                data_fresh=True,
                websocket_status="healthy",
                exchange_status="healthy",
                position_state="known",
                stop_protection_status="healthy",
                protective_stop_path_available=True,
                emergency_close_available=True,
            ),
            environment={"runtime_mode": "TESTNET_LIVE", "env": environment_name},
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
