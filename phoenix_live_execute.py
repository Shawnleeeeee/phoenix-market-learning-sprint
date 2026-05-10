#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import (
    ExecutionSettings,
    load_credentials,
    load_execution_settings,
    load_proxy_settings,
    resolve_environment,
)
from phoenix.executor import PhoenixExecutor
from phoenix.guardian_launch import spawn_guardian_worker
from phoenix.hermes_notify import humanize_side, send_telegram_message_async, telegram_settings
from phoenix.models import OrderInstruction, TradeIntent
from phoenix.runtime_state import evaluate_cooldown_gate, summarize_candidate
from phoenix.safe_order_gateway import build_gateway_snapshot, submit_binance_order_intent, submit_order_intent


PROJECT_ROOT = Path(__file__).resolve().parent
GUARDIAN_WORKERS_DIRNAME = "phoenix_guardian_workers"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Place a guarded live Phoenix entry plus the initial protection order.")
    parser.add_argument("--symbol")
    parser.add_argument("--quote-allocation", type=float)
    parser.add_argument("--leverage", type=int)
    parser.add_argument("--entry-price", type=float)
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"])
    parser.add_argument("--env", default=None, choices=["prod", "testnet", "demo"])
    parser.add_argument("--confirmation-record")
    parser.add_argument("--confirmation-token")
    return parser.parse_args()


def hermes_home() -> Path:
    return Path(str(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes")))


def guardian_workers_dir() -> Path:
    path = hermes_home() / "memories" / GUARDIAN_WORKERS_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def guardian_logs_dir() -> Path:
    path = hermes_home() / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_confirmation_record(path: str | None) -> dict[str, object] | None:
    if not path:
        return None
    record_path = Path(path)
    if not record_path.exists():
        raise RuntimeError(f"Confirmation record was not found: {record_path}")
    try:
        payload = json.loads(record_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Confirmation record is not valid JSON: {record_path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Confirmation record has an unexpected shape: {record_path}")
    return payload


def extract_available_balance(payload: object) -> float | None:
    if isinstance(payload, dict):
        for key in ("totalAvailableBalance", "virtualMaxWithdrawAmount", "accountEquity"):
            value = payload.get(key)
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("asset") or "").upper() != "USDT":
                continue
            for key in ("availableBalance", "balance", "crossWalletBalance"):
                value = item.get(key)
                if value not in (None, ""):
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        return None
    return None


def has_open_position(payload: object, symbol: str) -> bool:
    if not isinstance(payload, list):
        return False
    for item in payload:
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol") or "").upper() != symbol.upper():
            continue
        for key in ("positionAmt", "positionQty", "quantity"):
            value = item.get(key)
            if value in (None, "", "0", "0.0", "0.00000000"):
                continue
            try:
                if abs(float(value)) > 0:
                    return True
            except (TypeError, ValueError):
                continue
    return False


def open_position_symbols(payload: object) -> list[str]:
    symbols: list[str] = []
    if not isinstance(payload, list):
        return symbols
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        for key in ("positionAmt", "positionQty", "quantity"):
            value = item.get(key)
            if value in (None, "", "0", "0.0", "0.00000000"):
                continue
            try:
                if abs(float(value)) > 0:
                    symbols.append(symbol)
                    break
            except (TypeError, ValueError):
                continue
    return sorted(set(symbols))


def extract_symbol_config(payload: object, symbol: str) -> dict[str, object] | None:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("symbol") or "").upper() == symbol.upper():
                return item
    return None


def find_instruction(plan: list[OrderInstruction], name: str) -> OrderInstruction:
    for item in plan:
        if item.name == name:
            return item
    raise RuntimeError(f"Missing required order instruction: {name}")


def find_instruction_optional(plan: list[OrderInstruction], name: str) -> OrderInstruction | None:
    for item in plan:
        if item.name == name:
            return item
    return None


async def place_instruction(
    futures: BinanceFuturesClient,
    instruction: OrderInstruction,
    *,
    environment_name: str | None = None,
    runtime_mode: str | None = None,
    intent_log_path: str | Path | None = None,
    intent: TradeIntent | None = None,
    purpose: str | None = None,
    snapshot: dict[str, object] | None = None,
) -> dict[str, object]:
    env_name = _gateway_env_name(futures, environment_name)
    resolved_purpose = purpose or _purpose_from_instruction(instruction)
    payload = dict(instruction.payload)
    gateway_snapshot = snapshot or _snapshot_for_instruction(
        payload,
        purpose=resolved_purpose,
        intent=intent,
    )
    return await submit_binance_order_intent(
        futures,
        payload,
        snapshot=gateway_snapshot,
        environment=_gateway_environment(env_name, runtime_mode),
        source="phoenix_live_execute",
        purpose=resolved_purpose,
        endpoint=instruction.endpoint,
        order_intent=intent,
        dry_run=False,
        audit_log_path=intent_log_path,
        extra_context={
            "instruction_name": instruction.name,
            "protective_stop_path_available": True,
            "emergency_close_path_available": True,
        },
    )


def _gateway_env_name(futures: BinanceFuturesClient, environment_name: str | None = None) -> str:
    if environment_name:
        return str(environment_name).strip().lower()
    environment = getattr(futures, "environment", None)
    return str(getattr(environment, "name", None) or os.environ.get("PHOENIX_BINANCE_ENV") or "testnet").strip().lower()


def _gateway_environment(environment_name: str, runtime_mode: str | None = None) -> dict[str, object]:
    return {
        "runtime_mode": runtime_mode or os.environ.get("PHOENIX_RUNTIME_MODE") or "TESTNET_LIVE",
        "env": environment_name,
    }


def _purpose_from_instruction(instruction: OrderInstruction) -> str:
    name = instruction.name.lower()
    if "entry" in name:
        return "entry"
    if "take_profit" in name:
        return "take_profit"
    if "stop" in name or "protection" in name:
        return "protection"
    return "order"


def _snapshot_for_instruction(
    payload: dict[str, object],
    *,
    purpose: str,
    intent: TradeIntent | None,
) -> dict[str, object]:
    symbol = str(payload.get("symbol") or (intent.symbol if intent is not None else "") or "UNKNOWN").upper()
    side = str(payload.get("side") or (intent.side if intent is not None else "") or "").upper()
    reduce_like = str(payload.get("reduceOnly") or "").lower() == "true" or str(payload.get("closePosition") or "").lower() == "true"
    positions = []
    if reduce_like or purpose in {"protection", "take_profit", "exit", "emergency"}:
        positions.append(
            {
                "symbol": symbol,
                "side": "LONG" if side == "SELL" else "SHORT",
                "protection_status": "healthy",
            }
        )
    return build_gateway_snapshot(
        symbol=symbol,
        side=side,
        positions=positions,
        data_fresh=True,
        websocket_status="healthy",
        exchange_status="healthy",
        position_state="known",
        stop_protection_status="healthy",
        protective_stop_path_available=True,
        emergency_close_available=True,
    )


def spawn_post_fill_worker(
    *,
    confirmation_token: str | None,
    account_api_mode: str,
    position_mode: str,
    intent: TradeIntent,
    candidate: dict[str, object] | None,
    strategy_gate: dict[str, object] | None,
    dispatch_reason: str | None,
    entry_response: dict[str, object],
    initial_stop_response: dict[str, object],
    initial_take_profit_response: dict[str, object] | None = None,
    remaining_plan: list[OrderInstruction],
    settings: ExecutionSettings,
) -> dict[str, object]:
    job_id = confirmation_token or f"{intent.symbol}-{int(datetime.now(timezone.utc).timestamp())}"
    job_path = guardian_workers_dir() / f"{job_id}.json"
    log_path = guardian_logs_dir() / f"phoenix_guardian_{job_id}.log"
    now = datetime.now(timezone.utc).isoformat()
    job_payload = {
        "job_id": job_id,
        "confirmation_token": confirmation_token,
        "created_at": now,
        "updated_at": now,
        "status": "running",
        "phase": "INITIAL_STOP",
        "account_api_mode": account_api_mode,
        "position_mode": position_mode,
        "intent": intent.to_dict(),
        "candidate": candidate,
        "strategy_gate": strategy_gate,
        "dispatch_reason": dispatch_reason,
        "entry_market": entry_response,
        "initial_protective_stop": initial_stop_response,
        "initial_take_profit": initial_take_profit_response,
        "remaining_plan": [item.to_dict() for item in remaining_plan],
        "poll_interval_sec": settings.guardian_poll_interval_sec,
        "max_runtime_sec": settings.guardian_max_runtime_sec,
    }
    job_path.write_text(json.dumps(job_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    started = spawn_guardian_worker(job_path, log_path)
    return {
        "ok": True,
        "job_id": job_id,
        "pid": started.get("pid"),
        "unit_name": started.get("unit_name"),
        "launcher": started.get("launcher"),
        "job_file": str(job_path),
        "log_file": str(log_path),
        "poll_interval_sec": settings.guardian_poll_interval_sec,
        "max_runtime_sec": settings.guardian_max_runtime_sec,
    }


def intent_and_plan_from_record(record: dict[str, object]) -> tuple[TradeIntent, list[OrderInstruction], str | None]:
    intent_payload = record.get("intent")
    if not isinstance(intent_payload, dict):
        raise RuntimeError("Confirmation record is missing a valid intent payload.")
    plan_payload = record.get("plan")
    if not isinstance(plan_payload, list):
        raise RuntimeError("Confirmation record is missing a valid order plan.")
    account_api = record.get("account_api")
    planned_mode = account_api.get("resolved") if isinstance(account_api, dict) else None
    return (
        TradeIntent.from_dict(intent_payload),
        [OrderInstruction.from_dict(item) for item in plan_payload],
        planned_mode if isinstance(planned_mode, str) else None,
    )


async def async_main() -> int:
    args = parse_args()
    settings = load_execution_settings()
    credentials = load_credentials(required=True)
    environment = resolve_environment(args.env) if args.env else credentials.environment
    proxy_settings = load_proxy_settings()
    confirmation_record = load_confirmation_record(args.confirmation_record)

    timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        futures = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=credentials,
            proxy_settings=proxy_settings,
        )
        executor = PhoenixExecutor(futures_client=futures, settings=settings)
        notification_settings = telegram_settings()
        result: dict[str, object] = {
            "environment": environment.name,
            "action": "live_execute",
            "ok": False,
        }
        if args.confirmation_token:
            result["confirmation_token"] = args.confirmation_token

        try:
            if confirmation_record is not None:
                intent, plan, planned_account_api_mode = intent_and_plan_from_record(confirmation_record)
            else:
                intent = None
                plan = []
                planned_account_api_mode = None
            candidate = (
                confirmation_record.get("candidate")
                if isinstance(confirmation_record, dict) and isinstance(confirmation_record.get("candidate"), dict)
                else None
            )
            strategy_gate = (
                confirmation_record.get("strategy_gate")
                if isinstance(confirmation_record, dict) and isinstance(confirmation_record.get("strategy_gate"), dict)
                else None
            )
            dispatch_reason = (
                str(confirmation_record.get("dispatch_reason"))
                if isinstance(confirmation_record, dict) and confirmation_record.get("dispatch_reason") not in (None, "")
                else None
            )

            account_api_mode = await futures.get_account_api_mode()
            result["account_api"] = {
                "requested": futures.requested_account_api_mode(),
                "resolved": futures.resolved_account_api_mode() or account_api_mode,
            }
            position_mode = settings.position_mode
            margin_type_override = None
            if account_api_mode == "portfolio_margin":
                account_config = await futures.um_account_config()
                position_mode = "HEDGE" if bool(account_config.get("dualSidePosition")) else "ONE_WAY"
                result["um_account_config"] = account_config
                config_symbol = args.symbol or (intent.symbol if intent is not None else "")
                raw_symbol_config = await futures.um_symbol_config(config_symbol)
                result["um_symbol_config"] = raw_symbol_config
                symbol_config = extract_symbol_config(raw_symbol_config, config_symbol)
                margin_type_value = symbol_config.get("marginType") if symbol_config is not None else None
                if margin_type_value not in (None, ""):
                    margin_type_override = str(margin_type_value).upper()
            result["position_mode"] = position_mode
            if planned_account_api_mode and planned_account_api_mode != account_api_mode:
                raise RuntimeError(
                    f"Account API mode changed between arm and confirm: planned={planned_account_api_mode}, current={account_api_mode}."
                )

            account_overview = await futures.account_overview()
            available_balance = extract_available_balance(account_overview)
            result["account_overview"] = {
                "available_balance": available_balance,
            }
            cooldown_gate = evaluate_cooldown_gate(settings.cooldown_minutes_after_close)
            result["cooldown_gate"] = cooldown_gate
            if not cooldown_gate.get("ok"):
                raise RuntimeError(
                    "Phoenix cooldown gate blocked a new live trade: "
                    f"last_symbol={cooldown_gate.get('last_symbol')} "
                    f"minutes_remaining={cooldown_gate.get('minutes_remaining')}"
                )

            if intent is None:
                intent = await executor.build_trade_intent(
                    symbol=args.symbol or "",
                    side=args.side,
                    quote_allocation_usdt=args.quote_allocation,
                    entry_price=args.entry_price,
                    leverage=args.leverage,
                    available_balance_usdt=available_balance,
                    margin_type_override=margin_type_override,
                )
            elif margin_type_override:
                intent.margin_type = margin_type_override

            if not plan:
                plan = executor.build_order_plan(
                    intent,
                    account_api_mode=account_api_mode,
                    position_mode=position_mode,
                )
            result["intent"] = intent.to_dict()
            result["sizing"] = {
                "allocation_mode": intent.allocation_mode,
                "quote_allocation_usdt": intent.quote_allocation_usdt,
                "allocation_cap_usdt": intent.allocation_cap_usdt,
                "risk_budget_usdt": intent.risk_budget_usdt,
                "available_balance_usdt": available_balance,
            }
            if available_balance is not None and available_balance < intent.quote_allocation_usdt:
                raise RuntimeError(
                    f"Available balance {available_balance:.4f} is smaller than requested allocation {intent.quote_allocation_usdt:.4f}."
                )

            positions = await futures.position_information_v3()
            result["position_information_v3"] = positions
            open_symbols = open_position_symbols(positions)
            result["open_positions"] = {
                "symbols": open_symbols,
                "count": len(open_symbols),
                "max_allowed": settings.max_open_positions,
            }
            if settings.max_open_positions > 0 and len(open_symbols) >= settings.max_open_positions:
                raise RuntimeError(
                    f"Phoenix max-open-positions gate blocked a new live trade: currently open={open_symbols}, "
                    f"max_allowed={settings.max_open_positions}"
                )
            if has_open_position(positions, intent.symbol):
                raise RuntimeError(f"{intent.symbol} already has a non-zero open position; refusing overlapping live entry.")

            if account_api_mode == "portfolio_margin":
                symbol_config = await futures.um_symbol_config(intent.symbol)
                leverage_check = executor._evaluate_portfolio_margin_leverage(
                    intent,
                    {"ok": True, "payload": symbol_config},
                )
                result["portfolio_margin_leverage_check"] = leverage_check
                if not leverage_check.get("ok"):
                    raise RuntimeError(str(leverage_check.get("error") or "Portfolio Margin leverage check failed."))

            result["plan"] = [item.to_dict() for item in plan]

            entry_instruction = find_instruction(plan, "entry_market")
            protective_stop = find_instruction_optional(plan, "initial_protective_stop")
            risk_log_path = guardian_workers_dir() / "phoenix_order_intents.jsonl"
            entry_snapshot = build_gateway_snapshot(
                symbol=intent.symbol,
                side=intent.side,
                candidate=summarize_candidate(candidate) if candidate is not None else None,
                positions=[],
                open_positions_count=len(open_symbols),
                max_open_positions=settings.max_open_positions,
                data_fresh=True,
                websocket_status="healthy",
                exchange_status="healthy",
                position_state="known",
                stop_protection_status="healthy",
                protective_stop_path_available=protective_stop is not None,
                emergency_close_available=True,
            )
            entry_risk_gateway = await submit_order_intent(
                intent,
                entry_snapshot,
                _gateway_environment(environment.name),
                "phoenix_live_execute:entry_preflight",
                dry_run=True,
                audit_log_path=risk_log_path,
                extra_context={
                    "protective_stop_path_available": protective_stop is not None,
                    "emergency_close_path_available": True,
                },
            )
            result["risk_governor_result"] = entry_risk_gateway.risk_governor_result
            if not entry_risk_gateway.approved:
                raise RuntimeError(
                    "Risk Governor blocked entry before exchange call: "
                    f"{entry_risk_gateway.reason}"
                )
            remaining = [
                item
                for item in plan
                if item.name not in {"entry_market", "initial_protective_stop"}
            ]

            entry_response = await place_instruction(
                futures,
                entry_instruction,
                environment_name=environment.name,
                intent_log_path=risk_log_path,
                intent=intent,
                purpose="entry",
                snapshot=entry_snapshot,
            )
            result["entry_market"] = {"ok": True, "payload": entry_response}

            try:
                stop_response = await place_instruction(
                    futures,
                    protective_stop,
                    environment_name=environment.name,
                    intent_log_path=risk_log_path,
                    intent=intent,
                    purpose="protection",
                    snapshot=_snapshot_for_instruction(protective_stop.payload, purpose="protection", intent=intent),
                )
                result["initial_protective_stop"] = {"ok": True, "payload": stop_response}
            except Exception as exc:  # noqa: BLE001
                exit_side = "SELL" if intent.side == "BUY" else "BUY"
                hedge_mode = position_mode.upper() == "HEDGE"
                exit_position_side = executor._exit_position_side(intent.side, hedge_mode=hedge_mode)
                emergency_payload = {
                    "symbol": intent.symbol,
                    "side": exit_side,
                    **({"positionSide": exit_position_side} if exit_position_side else {}),
                    "type": "MARKET",
                    "quantity": intent.quantity,
                    **({} if hedge_mode and account_api_mode == "portfolio_margin" else {"reduceOnly": "true"}),
                    "newOrderRespType": "RESULT",
                }
                emergency_close = None
                emergency_error = None
                try:
                    emergency_close = await submit_binance_order_intent(
                        futures,
                        emergency_payload,
                        snapshot=_snapshot_for_instruction(emergency_payload, purpose="emergency", intent=intent),
                        environment=_gateway_environment(environment.name),
                        source="phoenix_live_execute:emergency_close",
                        purpose="emergency",
                        order_intent=intent,
                        dry_run=False,
                        audit_log_path=risk_log_path,
                        extra_context={
                            "protective_stop_path_available": True,
                            "emergency_close_path_available": True,
                        },
                    )
                except Exception as close_exc:  # noqa: BLE001
                    emergency_error = str(close_exc)
                result["initial_protective_stop"] = {"ok": False, "error": str(exc)}
                result["emergency_reduce_only_close"] = {
                    "ok": emergency_close is not None,
                    "payload": emergency_close,
                    "error": emergency_error,
                }
                raise RuntimeError(
                    "Entry was filled but initial protective stop failed; attempted an emergency reduce-only market close."
                ) from exc

            try:
                worker = spawn_post_fill_worker(
                    confirmation_token=args.confirmation_token,
                    account_api_mode=account_api_mode,
                    position_mode=position_mode,
                    intent=intent,
                    candidate=summarize_candidate(candidate),
                    strategy_gate=strategy_gate,
                    dispatch_reason=dispatch_reason,
                    entry_response=entry_response,
                    initial_stop_response=stop_response,
                    remaining_plan=remaining,
                    settings=settings,
                )
            except Exception as exc:  # noqa: BLE001
                worker = {"ok": False, "error": str(exc)}

            result["post_fill_worker"] = worker
            if not worker.get("ok"):
                result["warning"] = (
                    "Live entry and initial protective stop succeeded, but the post-fill Guardian worker did not start."
                )
            if candidate is not None:
                result["candidate"] = candidate
            if strategy_gate is not None:
                result["strategy_gate"] = strategy_gate
            if dispatch_reason:
                result["dispatch_reason"] = dispatch_reason
            result["post_entry_management"] = (
                "A detached Guardian worker now monitors the position and will replace the stop at breakeven, "
                "arm the trailing stop, and clean up residual protection orders after the position closes."
            )
            rationale_bits: list[str] = []
            if isinstance(candidate, dict):
                score = candidate.get("score")
                if score is not None:
                    rationale_bits.append(f"candidate score={score}")
                breakdown = candidate.get("score_breakdown")
                if isinstance(breakdown, dict) and breakdown:
                    top_components = sorted(
                        breakdown.items(),
                        key=lambda item: abs(float(item[1])) if isinstance(item[1], (int, float)) else 0.0,
                        reverse=True,
                    )[:3]
                    rationale_bits.append(
                        "drivers="
                        + ", ".join(
                            f"{key}:{value}"
                            for key, value in top_components
                        )
                    )
                sentiment = candidate.get("social_sentiment")
                if sentiment:
                    rationale_bits.append(f"sentiment={sentiment}")
                directional_bias = candidate.get("directional_bias")
                if directional_bias:
                    rationale_bits.append(f"directional_bias={directional_bias}")
                directional_score = candidate.get("directional_score")
                if directional_score not in (None, ""):
                    rationale_bits.append(f"directional_score={directional_score}")
                smart_money_traders = candidate.get("smart_money_traders")
                if smart_money_traders not in (None, ""):
                    rationale_bits.append(f"smart_money_traders={smart_money_traders}")
                audit_flags = candidate.get("audit_flags") or []
                if audit_flags:
                    rationale_bits.append("audit_flags=" + ",".join(str(item) for item in audit_flags[:2]))
            if dispatch_reason:
                rationale_bits.append(f"dispatch={dispatch_reason}")
            open_text = (
                f"🚀 凤凰协议已开仓：{intent.symbol} {humanize_side(intent.side)}\n"
                f"参考入场价：{intent.entry_price}\n"
                f"数量：{intent.quantity}\n"
                f"杠杆：{intent.leverage}x\n"
                f"保证金占用：{intent.quote_allocation_usdt} USDT（{intent.allocation_mode}）\n"
                f"初始止损：{intent.initial_stop_price}\n"
                f"保本触发：{intent.breakeven_trigger_price}\n"
                f"追踪回调：{intent.trailing_callback_rate}%\n"
                + ("开仓依据：" + " | ".join(rationale_bits) if rationale_bits else "开仓依据：满足实盘执行条件")
            )
            try:
                await send_telegram_message_async(
                    session,
                    text=open_text,
                    notification_settings=notification_settings,
                )
            except Exception as exc:
                result["entry_notification_error"] = str(exc)
            result["ok"] = True
            print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
            return 0

        except (BinanceAPIError, RuntimeError, ValueError) as exc:
            result["error"] = str(exc)
            print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
            return 1


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
