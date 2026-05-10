#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import aiohttp

from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import load_credentials, load_execution_settings, load_proxy_settings, resolve_environment
from phoenix.executor import PhoenixExecutor
from phoenix.runtime_state import evaluate_cooldown_gate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview or validate Phoenix Futures execution plan.")
    parser.add_argument("--symbol", help="Perpetual futures symbol, e.g. BTCUSDT")
    parser.add_argument("--candidate-file", type=Path, default=Path("phoenix_candidates.json"))
    parser.add_argument("--candidate-rank", type=int, default=1)
    parser.add_argument("--quote-allocation", type=float)
    parser.add_argument("--leverage", type=int)
    parser.add_argument("--entry-price", type=float)
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"])
    parser.add_argument("--env", default=None, choices=["prod", "testnet", "demo"])
    parser.add_argument(
        "--test-order",
        action="store_true",
        help="Validate order payloads. Classic entry orders use /fapi/v1/order/test; classic algo protection orders fall back to payload preview because Binance exposes no algo /test endpoint.",
    )
    parser.add_argument(
        "--apply-account-setup",
        action="store_true",
        help="Also call change margin type and leverage before test orders.",
    )
    return parser.parse_args()


def load_symbol_from_candidates(path: Path, rank: int) -> str:
    candidate = load_candidate_from_file(path, rank)
    return str(candidate["symbol"]).upper()


def load_candidate_from_file(path: Path, rank: int) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    candidates = payload.get("top_candidates") or []
    if not candidates:
        raise RuntimeError(f"No candidates found in {path}")
    index = max(rank - 1, 0)
    if index >= len(candidates):
        raise RuntimeError(f"Requested candidate rank {rank}, but file only contains {len(candidates)} candidates.")
    candidate = candidates[index]
    if not isinstance(candidate, dict):
        raise RuntimeError(f"Candidate payload at rank {rank} is not an object in {path}")
    return dict(candidate)


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


def evaluate_strategy_gate(candidate: dict[str, object] | None, settings, *, requested_side: str = "BUY") -> dict[str, object]:
    if candidate is None:
        return {
            "applied": False,
            "ok": True,
            "reason": "explicit_symbol_bypass",
        }

    blocked = [str(item) for item in (candidate.get("blocked_reasons") or []) if str(item)]
    try:
        score = float(candidate.get("score") or 0.0)
    except (TypeError, ValueError):
        score = 0.0

    reasons: list[str] = []
    if settings.reject_blocked_candidates and blocked:
        reasons.append(f"candidate_blocked:{','.join(blocked)}")
    if score < settings.strategy_min_score:
        reasons.append(
            f"candidate_score_below_threshold:{score:.4f}<{settings.strategy_min_score:.4f}"
        )
    directional_bias = str(candidate.get("directional_bias") or "NONE").upper()
    expected_side = {"LONG": "BUY", "SHORT": "SELL"}.get(directional_bias)
    normalized_requested_side = str(requested_side or "BUY").upper()
    try:
        execution_quality_score = float(candidate.get("execution_quality_score") or 0.0)
    except (TypeError, ValueError):
        execution_quality_score = 0.0
    try:
        event_risk_score = float(candidate.get("event_risk_score") or 0.0)
    except (TypeError, ValueError):
        event_risk_score = 0.0
    try:
        estimated_slippage_bps = (
            float(candidate.get("estimated_slippage_bps"))
            if candidate.get("estimated_slippage_bps") not in (None, "")
            else None
        )
    except (TypeError, ValueError):
        estimated_slippage_bps = None
    try:
        spread_bps = float(candidate.get("spread_bps")) if candidate.get("spread_bps") not in (None, "") else None
    except (TypeError, ValueError):
        spread_bps = None

    if directional_bias == "NONE":
        reasons.append("candidate_direction_undefined")
    if expected_side and normalized_requested_side != expected_side:
        reasons.append(f"candidate_direction_mismatch:{directional_bias}->{expected_side}")
    if directional_bias == "SHORT" and not settings.allow_auto_short:
        reasons.append("auto_short_disabled")
    if execution_quality_score < settings.min_execution_quality_score:
        reasons.append(
            f"execution_quality_below_threshold:{execution_quality_score:.2f}<{settings.min_execution_quality_score:.2f}"
        )
    if event_risk_score > settings.max_event_risk_score:
        reasons.append(f"event_risk_above_threshold:{event_risk_score:.2f}>{settings.max_event_risk_score:.2f}")
    if estimated_slippage_bps is not None and estimated_slippage_bps > settings.max_estimated_slippage_bps:
        reasons.append(
            f"estimated_slippage_above_threshold:{estimated_slippage_bps:.2f}>{settings.max_estimated_slippage_bps:.2f}"
        )
    if spread_bps is not None and spread_bps > settings.max_spread_bps:
        reasons.append(f"spread_above_threshold:{spread_bps:.2f}>{settings.max_spread_bps:.2f}")

    return {
        "applied": True,
        "ok": not reasons,
        "score": score,
        "minimum_score": settings.strategy_min_score,
        "directional_bias": directional_bias,
        "requested_side": normalized_requested_side,
        "expected_side": expected_side,
        "blocked_reasons": blocked,
        "execution_quality_score": execution_quality_score,
        "event_risk_score": event_risk_score,
        "estimated_slippage_bps": estimated_slippage_bps,
        "spread_bps": spread_bps,
        "reasons": reasons,
        "symbol": str(candidate.get("symbol") or ""),
        "confidence": candidate.get("confidence"),
    }


async def async_main() -> int:
    result: dict[str, object] = {}
    try:
        args = parse_args()
        candidate = None if args.symbol else load_candidate_from_file(args.candidate_file, args.candidate_rank)
        symbol = str(args.symbol).upper() if args.symbol else str(candidate["symbol"]).upper()
        settings = load_execution_settings()
        credentials = load_credentials(required=args.test_order or args.apply_account_setup)
        environment = credentials.environment if credentials is not None else resolve_environment(args.env or "prod")
        proxy_settings = load_proxy_settings()
        strategy_gate = evaluate_strategy_gate(candidate, settings, requested_side=args.side)
        if strategy_gate.get("applied") and not strategy_gate.get("ok"):
            raise RuntimeError(
                "Phoenix strategy gate rejected the candidate: "
                + "; ".join(str(item) for item in strategy_gate.get("reasons") or [])
            )
        cooldown_gate = evaluate_cooldown_gate(settings.cooldown_minutes_after_close)
        if not cooldown_gate.get("ok"):
            raise RuntimeError(
                "Phoenix cooldown gate blocked a new trade: "
                f"last_symbol={cooldown_gate.get('last_symbol')} "
                f"minutes_remaining={cooldown_gate.get('minutes_remaining')}"
            )

        timeout = aiohttp.ClientTimeout(total=45, sock_connect=15, sock_read=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            futures = BinanceFuturesClient(
                session=session,
                environment=environment,
                credentials=credentials,
                proxy_settings=proxy_settings,
            )
            executor = PhoenixExecutor(futures_client=futures, settings=settings)
            account_api_error: str | None = None
            if credentials is not None:
                try:
                    resolved_account_api = await futures.get_account_api_mode()
                except (BinanceAPIError, RuntimeError) as exc:
                    resolved_account_api = futures.planned_account_api_mode()
                    account_api_error = str(exc)
            else:
                resolved_account_api = futures.planned_account_api_mode()
            available_balance = None
            positions = None
            if credentials is not None:
                try:
                    account_overview = await futures.account_overview()
                    available_balance = extract_available_balance(account_overview)
                except (BinanceAPIError, RuntimeError) as exc:
                    if account_api_error is None:
                        account_api_error = str(exc)
                try:
                    positions = await futures.position_information_v3()
                except (BinanceAPIError, RuntimeError) as exc:
                    if account_api_error is None:
                        account_api_error = str(exc)
            position_mode = settings.position_mode
            if credentials is not None and resolved_account_api == "portfolio_margin":
                try:
                    account_config = await futures.um_account_config()
                    position_mode = "HEDGE" if bool(account_config.get("dualSidePosition")) else "ONE_WAY"
                except (BinanceAPIError, RuntimeError) as exc:
                    if account_api_error is None:
                        account_api_error = str(exc)
            margin_type_override = None
            if credentials is not None and resolved_account_api == "portfolio_margin":
                try:
                    symbol_config = extract_symbol_config(await futures.um_symbol_config(symbol), symbol)
                    margin_type_value = symbol_config.get("marginType") if symbol_config is not None else None
                    if margin_type_value not in (None, ""):
                        margin_type_override = str(margin_type_value).upper()
                except (BinanceAPIError, RuntimeError) as exc:
                    if account_api_error is None:
                        account_api_error = str(exc)
            intent = await executor.build_trade_intent(
                symbol=symbol,
                side=args.side,
                quote_allocation_usdt=args.quote_allocation,
                entry_price=args.entry_price,
                leverage=args.leverage,
                available_balance_usdt=available_balance,
                margin_type_override=margin_type_override,
            )
            plan = [
                item.to_dict()
                for item in executor.build_order_plan(
                    intent,
                    account_api_mode=resolved_account_api,
                    position_mode=position_mode,
                )
            ]
            result = {
                "environment": environment.name,
                "account_api": {
                    "requested": futures.requested_account_api_mode(),
                    "resolved": futures.resolved_account_api_mode() or resolved_account_api,
                },
                "candidate": candidate,
                "strategy_gate": strategy_gate,
                "cooldown_gate": cooldown_gate,
                "position_mode": position_mode,
                "sizing": {
                    "allocation_mode": intent.allocation_mode,
                    "quote_allocation_usdt": intent.quote_allocation_usdt,
                    "allocation_cap_usdt": intent.allocation_cap_usdt,
                    "risk_budget_usdt": intent.risk_budget_usdt,
                    "available_balance_usdt": available_balance,
                },
                "intent": intent.to_dict(),
                "plan": plan,
            }
            if positions is not None:
                open_symbols = open_position_symbols(positions)
                result["open_positions"] = {
                    "symbols": open_symbols,
                    "count": len(open_symbols),
                    "max_allowed": settings.max_open_positions,
                }
                if settings.max_open_positions > 0 and len(open_symbols) >= settings.max_open_positions:
                    raise RuntimeError(
                        f"Phoenix max-open-positions gate blocked a new trade: currently open={open_symbols}, "
                        f"max_allowed={settings.max_open_positions}"
                    )
            if account_api_error is not None:
                result["account_api"]["error"] = account_api_error

            if args.test_order:
                result["validation"] = await executor.validate_test_plan(
                    intent,
                    include_account_setup=args.apply_account_setup,
                )

        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 0
    except Exception as exc:  # noqa: BLE001
        result["ok"] = False
        result["error"] = str(exc)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        return 1


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
