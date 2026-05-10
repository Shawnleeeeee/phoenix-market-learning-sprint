from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix.trader_snapshot import build_trader_snapshot


DEFAULT_CANDIDATE_FILES = (
    Path("phoenix_candidates.openclaw.json"),
    Path("phoenix_candidates.json"),
)
DEFAULT_STATE_DIR = Path("btc_data") / "state"


def build_runtime_trader_snapshot(
    *,
    root: str | Path = ".",
    state_dir: str | Path | None = None,
    candidate_files: list[str | Path] | tuple[str | Path, ...] | None = None,
    signed_account_state: dict[str, Any] | None = None,
    signed_positions: list[dict[str, Any]] | None = None,
    max_candidates: int = 10,
    stale_after_sec: int = 60,
    max_open_positions: int = 1,
    protective_stop_path_available: bool = False,
    emergency_close_available: bool = False,
) -> dict[str, Any]:
    """Build a lightweight Trader Mode snapshot from existing Phoenix runtime files.

    This intentionally reads compact state files only. It does not start scanners,
    does not open WebSockets, and does not load large JSONL datasets.
    """
    root_path = Path(root)
    resolved_state_dir = Path(state_dir) if state_dir is not None else root_path / DEFAULT_STATE_DIR
    market_stream = _read_json(resolved_state_dir / "market_stream_snapshot.json")
    market_state = _read_json(resolved_state_dir / "market_stream_state.json")
    engine_snapshot = _read_json(resolved_state_dir / "engine_snapshot.json")
    demo_account = _read_json(resolved_state_dir / "demo_account.json")
    active_trade = _read_json(resolved_state_dir / "active_trade.json")
    paper_active_trade = _read_json(resolved_state_dir / "paper_active_trade.json")
    last_close = _read_json(resolved_state_dir / "last_close.json")

    candidates_payload, candidate_source = _load_candidates(root_path, candidate_files)
    merged_candidates = _merge_candidates_with_market(
        _candidate_rows(candidates_payload),
        market_stream,
        limit=max_candidates,
    )
    market_data = _build_market_data(market_stream, engine_snapshot)
    position_state_known = signed_positions is not None or isinstance(active_trade, dict) or isinstance(paper_active_trade, dict)
    current_positions = _positions_from_sources(
        signed_positions=signed_positions,
        active_trade=active_trade,
        paper_active_trade=paper_active_trade,
        market_stream=market_stream,
    )
    account_state = _build_account_state(
        signed_account_state=signed_account_state,
        demo_account=demo_account,
        current_positions=current_positions,
        last_close=last_close,
        max_open_positions=max_open_positions,
    )
    system_status = _build_system_status(
        market_stream=market_stream,
        market_state=market_state,
        engine_snapshot=engine_snapshot,
        candidate_source=candidate_source,
        account_source="signed_account" if signed_account_state is not None or signed_positions is not None else "runtime_state",
        stale_after_sec=stale_after_sec,
        candidates=merged_candidates,
        positions=current_positions,
        position_state_known=position_state_known,
        protective_stop_path_available=protective_stop_path_available,
        emergency_close_available=emergency_close_available,
    )
    return build_trader_snapshot(
        market_data=market_data,
        account_state=account_state,
        positions=current_positions,
        candidates=merged_candidates,
        system_status=system_status,
        max_candidates=max_candidates,
        stale_after_sec=stale_after_sec,
    )


async def fetch_signed_testnet_account_state(*, environment_name: str = "testnet") -> dict[str, Any]:
    """Fetch compact signed account state for testnet trial risk checks."""
    from phoenix.binance_futures import BinanceFuturesClient
    from phoenix.config import load_credentials, load_proxy_settings, resolve_environment

    environment = resolve_environment(environment_name)
    if environment.name not in {"testnet", "demo"}:
        return {"ok": False, "error": "signed_account_fetch_testnet_only", "account_state": None, "positions": None}
    credentials = load_credentials(required=False)
    if credentials is None:
        return {"ok": False, "error": "signed_account_credentials_missing", "account_state": None, "positions": None}
    if credentials.environment.name not in {"testnet", "demo"}:
        return {"ok": False, "error": "signed_account_credentials_not_testnet", "account_state": None, "positions": None}
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=credentials,
            proxy_settings=load_proxy_settings(),
        )
        try:
            account = await client.account_overview()
            positions = await client.position_information_v3()
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc), "account_state": None, "positions": None}
    return {
        "ok": True,
        "error": None,
        "account_state": _compact_account_state(account),
        "positions": positions if isinstance(positions, list) else [],
    }


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _load_candidates(
    root: Path,
    candidate_files: list[str | Path] | tuple[str | Path, ...] | None,
) -> tuple[dict[str, Any] | None, str]:
    paths = tuple(Path(item) for item in candidate_files) if candidate_files is not None else DEFAULT_CANDIDATE_FILES
    for path in paths:
        resolved = path if path.is_absolute() else root / path
        payload = _read_json(resolved)
        if payload is not None:
            return payload, str(resolved)
    return None, "unavailable"


def _candidate_rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("top_candidates")
    if not isinstance(rows, list):
        return []
    return [dict(item) for item in rows if isinstance(item, dict)]


def _build_market_data(
    market_stream: dict[str, Any] | None,
    engine_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    btc = _symbol_market(market_stream, "BTCUSDT")
    eth = _symbol_market(market_stream, "ETHUSDT")
    btc_1m = _safe_float(btc.get("price_change_1m_pct")) or _safe_float(btc.get("price_change_5m_pct"))
    btc_5m = _safe_float(btc.get("price_change_5m_pct")) or _safe_float(btc.get("price_change_15m_pct"))
    eth_1m = _safe_float(eth.get("price_change_1m_pct")) or _safe_float(eth.get("price_change_5m_pct"))
    eth_5m = _safe_float(eth.get("price_change_5m_pct")) or _safe_float(eth.get("price_change_15m_pct"))
    volatility = _runtime_value(engine_snapshot, "volatility_15m") or _estimate_volatility([btc, eth])
    regime = _runtime_value(engine_snapshot, "regime") or _infer_regime(
        btc_1m=btc_1m,
        btc_5m=btc_5m,
        eth_1m=eth_1m,
        eth_5m=eth_5m,
        volatility=volatility,
    )
    return {
        "regime": regime,
        "btc_trend_1m": btc_1m,
        "btc_trend_5m": btc_5m,
        "eth_trend_1m": eth_1m,
        "eth_trend_5m": eth_5m,
        "volatility": volatility,
    }


def _symbol_market(market_stream: dict[str, Any] | None, symbol: str) -> dict[str, Any]:
    if not isinstance(market_stream, dict):
        return {}
    per_symbol = market_stream.get("per_symbol")
    if isinstance(per_symbol, dict) and isinstance(per_symbol.get(symbol), dict):
        return dict(per_symbol[symbol])
    if str(market_stream.get("symbol") or "").upper() == symbol:
        return dict(market_stream)
    return {}


def _merge_candidates_with_market(
    candidates: list[dict[str, Any]],
    market_stream: dict[str, Any] | None,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for item in candidates[: max(1, min(10, int(limit or 10)))]:
        symbol = str(item.get("symbol") or "").upper()
        market = _symbol_market(market_stream, symbol)
        depth = market.get("depth") if isinstance(market.get("depth"), dict) else {}
        directional_bias = str(item.get("directional_bias") or item.get("bias") or "NEUTRAL").upper()
        bias = {"LONG": "LONG", "SHORT": "SHORT", "NONE": "NEUTRAL"}.get(directional_bias, directional_bias)
        spread = _first_float(item, "spread_bps", "spread_bps_top") or _safe_float(depth.get("spread_bps"))
        slippage = _first_float(item, "estimated_slippage_bps")
        if slippage is None:
            if bias == "SHORT":
                slippage = _safe_float(depth.get("estimated_slippage_bps_sell"))
            else:
                slippage = _safe_float(depth.get("estimated_slippage_bps_buy"))
        merged.append(
            {
                **item,
                "symbol": symbol or "UNKNOWN",
                "bias": bias if bias in {"LONG", "SHORT", "NEUTRAL"} else "NEUTRAL",
                "setup_type": str(item.get("setup_type") or "QUICK_TRADE").upper(),
                "current_price": _first_float(item, "current_price", "mark_price") or _safe_float(market.get("mid_price")),
                "spread_bps": spread,
                "estimated_slippage_bps": slippage,
                "liquidity_ok": _liquidity_ok(depth, spread, slippage, item),
                "price_action_summary": _price_action_summary(item, market),
                "order_flow_summary": _order_flow_summary(item, market),
                "depth_summary": _depth_summary(depth),
                "derivatives_summary": _derivatives_summary(item),
                "suggested_stop_pct": _first_float(item, "suggested_stop_pct", "stop_loss_pct") or 0.6,
                "suggested_tp_pct": _first_float(item, "suggested_tp_pct", "take_profit_pct") or 1.2,
                "max_holding_time_sec": int(_first_float(item, "max_holding_time_sec") or 900),
                "invalidation_hint": str(item.get("invalidation_hint") or "breakout fails or spread/slippage widens"),
            }
        )
    return merged


def _positions_from_sources(
    *,
    signed_positions: list[dict[str, Any]] | None,
    active_trade: dict[str, Any] | None,
    paper_active_trade: dict[str, Any] | None,
    market_stream: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if signed_positions is not None:
        return [_position_from_exchange(item, market_stream) for item in signed_positions if _is_open_position(item)]
    rows: list[dict[str, Any]] = []
    for item in (active_trade, paper_active_trade):
        if isinstance(item, dict) and str(item.get("status") or "").lower() not in {"", "closed", "cleared"}:
            rows.append(_position_from_active_trade(item, market_stream))
    return rows


def _build_account_state(
    *,
    signed_account_state: dict[str, Any] | None,
    demo_account: dict[str, Any] | None,
    current_positions: list[dict[str, Any]],
    last_close: dict[str, Any] | None,
    max_open_positions: int,
) -> dict[str, Any]:
    daily_pnl_pct = _safe_float((demo_account or {}).get("daily_pnl_pct"))
    if daily_pnl_pct is None:
        daily_pnl_pct = _safe_float((signed_account_state or {}).get("daily_pnl_pct")) or 0.0
    loss_streak = int(_safe_float((demo_account or {}).get("loss_streak")) or _loss_streak_from_last_close(last_close))
    cooldown_active = bool((demo_account or {}).get("cooldown_active", False))
    return {
        "trading_allowed": len(current_positions) < max(1, int(max_open_positions or 1)) and not cooldown_active,
        "daily_pnl_pct": daily_pnl_pct,
        "max_daily_loss_pct": abs(_safe_float((signed_account_state or {}).get("max_daily_loss_pct")) or 3.0),
        "open_positions_count": len(current_positions),
        "max_open_positions": max(1, int(max_open_positions or 1)),
        "loss_streak": loss_streak,
        "cooldown_active": cooldown_active,
        "reason_if_blocked": "cooldown_active" if cooldown_active else None,
    }


def _build_system_status(
    *,
    market_stream: dict[str, Any] | None,
    market_state: dict[str, Any] | None,
    engine_snapshot: dict[str, Any] | None,
    candidate_source: str,
    account_source: str,
    stale_after_sec: int,
    candidates: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    position_state_known: bool,
    protective_stop_path_available: bool,
    emergency_close_available: bool,
) -> dict[str, Any]:
    generated_at = _latest_timestamp(
        _runtime_value(market_stream, "generated_at"),
        _runtime_value(market_stream, "last_event_at"),
        _runtime_value(market_state, "last_event_at"),
        _runtime_value(engine_snapshot, "generated_at"),
    )
    age = _age_sec(generated_at)
    stream_status = str((market_state or {}).get("status") or ("connected" if market_stream else "unavailable")).lower()
    websocket_status = "healthy" if stream_status in {"connected", "running", "healthy"} else "unavailable"
    data_fresh = age is not None and age <= max(1, int(stale_after_sec or 60)) and bool(candidates)
    return {
        "snapshot_time": generated_at or datetime.now(timezone.utc).isoformat(),
        "data_fresh": bool(data_fresh),
        "data_age_sec": age,
        "websocket_status": websocket_status,
        "exchange_status": "healthy" if market_stream else "unavailable",
        "source": "phoenix_runtime_state",
        "candidate_source": candidate_source,
        "account_source": account_source,
        "stale_after_sec": stale_after_sec,
        "position_state": "known" if position_state_known else "unknown",
        "stop_protection_status": _protection_status(positions),
        "candidate_state": "known" if candidates else "unavailable",
        "protective_stop_path_available": bool(protective_stop_path_available),
        "emergency_close_available": bool(emergency_close_available),
    }


def _position_from_exchange(item: dict[str, Any], market_stream: dict[str, Any] | None) -> dict[str, Any]:
    symbol = str(item.get("symbol") or "UNKNOWN").upper()
    amount = _position_amount(item)
    side = "LONG" if amount >= 0 else "SHORT"
    entry_price = _first_float(item, "entryPrice", "entry_price")
    current_price = _first_float(item, "markPrice", "mark_price") or _safe_float(_symbol_market(market_stream, symbol).get("mid_price"))
    unrealized = _first_float(item, "unRealizedProfit", "unrealizedProfit", "unrealized_pnl_usdt")
    pnl_pct = None
    if entry_price and current_price:
        sign = 1.0 if side == "LONG" else -1.0
        pnl_pct = ((current_price - entry_price) / entry_price) * 100.0 * sign
    return {
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "current_price": current_price,
        "unrealized_pnl_pct": pnl_pct,
        "unrealized_pnl_usdt": unrealized,
        "stop_loss_price": _first_float(item, "stop_loss_price", "initial_stop_price"),
        "take_profit_price": _first_float(item, "take_profit_price"),
        "time_in_trade_sec": int(_first_float(item, "time_in_trade_sec") or 0),
        "protection_status": str(item.get("protection_status") or "unknown"),
    }


def _position_from_active_trade(item: dict[str, Any], market_stream: dict[str, Any] | None) -> dict[str, Any]:
    intent = item.get("intent") if isinstance(item.get("intent"), dict) else item
    symbol = str(intent.get("symbol") or item.get("symbol") or "UNKNOWN").upper()
    side = "LONG" if str(intent.get("side") or item.get("side") or "BUY").upper() == "BUY" else "SHORT"
    current_price = _safe_float(_symbol_market(market_stream, symbol).get("mid_price"))
    return {
        "symbol": symbol,
        "side": side,
        "entry_price": _first_float(intent, "entry_price"),
        "current_price": current_price,
        "unrealized_pnl_pct": "unavailable",
        "unrealized_pnl_usdt": "unavailable",
        "stop_loss_price": _first_float(intent, "initial_stop_price", "stop_loss_price"),
        "take_profit_price": _first_float(intent, "take_profit_price"),
        "time_in_trade_sec": int(_age_sec(item.get("created_at")) or 0),
        "protection_status": str(item.get("phase") or item.get("protection_status") or "unknown"),
    }


def _is_open_position(item: dict[str, Any]) -> bool:
    return abs(_position_amount(item)) > 1e-12


def _position_amount(item: dict[str, Any]) -> float:
    return _first_float(item, "positionAmt", "positionQty", "quantity") or 0.0


def _runtime_value(payload: dict[str, Any] | None, key: str) -> Any:
    return payload.get(key) if isinstance(payload, dict) else None


def _latest_timestamp(*values: Any) -> str | None:
    parsed = [(_parse_dt(item), str(item)) for item in values if item not in (None, "")]
    parsed = [(dt, raw) for dt, raw in parsed if dt is not None]
    if not parsed:
        return None
    return max(parsed, key=lambda item: item[0])[1]


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_sec(value: Any) -> float | None:
    parsed = _parse_dt(value)
    if parsed is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def _compact_account_state(payload: Any) -> dict[str, Any]:
    available = None
    wallet = None
    if isinstance(payload, dict):
        available = _safe_float(payload.get("totalAvailableBalance") or payload.get("virtualMaxWithdrawAmount"))
        wallet = _safe_float(payload.get("totalWalletBalance") or payload.get("accountEquity"))
    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict) or str(item.get("asset") or "").upper() != "USDT":
                continue
            available = _safe_float(item.get("availableBalance") or item.get("balance"))
            wallet = _safe_float(item.get("balance") or item.get("crossWalletBalance"))
            break
    return {
        "available_balance_usdt": available,
        "wallet_balance_usdt": wallet,
        "daily_pnl_pct": 0.0,
        "max_daily_loss_pct": 3.0,
    }


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _safe_float(payload.get(key))
        if value is not None:
            return value
    return None


def _infer_regime(
    *,
    btc_1m: float | None,
    btc_5m: float | None,
    eth_1m: float | None,
    eth_5m: float | None,
    volatility: float | None,
) -> str:
    if volatility is not None and volatility >= 2.5:
        return "HIGH_VOL"
    values = [item for item in (btc_1m, btc_5m, eth_1m, eth_5m) if item is not None]
    if len(values) < 2:
        return "UNKNOWN"
    if sum(1 for item in values if item > 0) >= max(2, len(values) - 1):
        return "TREND_UP"
    if sum(1 for item in values if item < 0) >= max(2, len(values) - 1):
        return "TREND_DOWN"
    return "CHOP"


def _estimate_volatility(symbols: list[dict[str, Any]]) -> float | None:
    changes = []
    for item in symbols:
        for key in ("price_change_5m_pct", "price_change_15m_pct", "price_change_1h_pct"):
            value = _safe_float(item.get(key))
            if value is not None:
                changes.append(abs(value))
    if not changes:
        return None
    return round(max(changes), 6)


def _liquidity_ok(depth: dict[str, Any], spread: float | None, slippage: float | None, item: dict[str, Any]) -> bool:
    if item.get("liquidity_ok") is False:
        return False
    if spread is not None and spread > 12.0:
        return False
    if slippage is not None and slippage > 8.0:
        return False
    bid = _safe_float(depth.get("depth_bid_5")) or 0.0
    ask = _safe_float(depth.get("depth_ask_5")) or 0.0
    return (bid + ask) > 0 if depth else True


def _price_action_summary(item: dict[str, Any], market: dict[str, Any]) -> str:
    change_5m = _safe_float(item.get("price_change_5m_pct")) or _safe_float(market.get("price_change_5m_pct"))
    change_15m = _safe_float(item.get("price_change_15m_pct")) or _safe_float(market.get("price_change_15m_pct"))
    return f"5m={_fmt(change_5m)}%, 15m={_fmt(change_15m)}%"


def _order_flow_summary(item: dict[str, Any], market: dict[str, Any]) -> str:
    flow = _safe_float(item.get("aggressive_flow_delta")) or _safe_float(market.get("ofi_window_sum"))
    buy_ratio = _safe_float(item.get("taker_buy_ratio_5m"))
    return f"flow={_fmt(flow)}, taker_buy_ratio_5m={_fmt(buy_ratio)}"


def _depth_summary(depth: dict[str, Any]) -> str:
    if not depth:
        return "unavailable"
    return (
        f"spread={_fmt(depth.get('spread_bps'))}bps, "
        f"imbalance={_fmt(depth.get('depth_imbalance'))}, "
        f"bid5={_fmt(depth.get('depth_bid_5'))}, ask5={_fmt(depth.get('depth_ask_5'))}"
    )


def _derivatives_summary(item: dict[str, Any]) -> str:
    return (
        f"oi_1m={_fmt(item.get('oi_delta_1m_pct'))}%, "
        f"oi_5m={_fmt(item.get('oi_delta_5m_pct'))}%, "
        f"funding={_fmt(item.get('funding_rate'))}"
    )


def _fmt(value: Any) -> str:
    number = _safe_float(value)
    return "unavailable" if number is None else f"{number:.4f}"


def _loss_streak_from_last_close(last_close: dict[str, Any] | None) -> int:
    if not isinstance(last_close, dict):
        return 0
    outcome = str(last_close.get("outcome") or last_close.get("exit_reason") or "").lower()
    return 1 if "loss" in outcome or "stop" in outcome else 0


def _protection_status(positions: list[dict[str, Any]]) -> str:
    if not positions:
        return "healthy"
    statuses = {str(item.get("protection_status") or "unknown").lower() for item in positions}
    if statuses <= {"healthy", "initial_stop", "breakeven_only", "breakeven_and_trailing_armed"}:
        return "healthy"
    return "unknown"
