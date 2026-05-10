"""Multi-symbol public market WebSocket service for Leiting v3-simple."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import (
    STATE_DIR,
    get_demo_leverage,
    get_demo_position_fraction,
    get_demo_start_equity_usdt,
    get_env,
    get_execution_mode,
    get_hermes_demo_v2_target,
    get_market_stream_freshness_sec,
    get_market_ws_base_url,
    load_runtime_candidate_config,
)
from btc_engine.market_data.depth import analyze_depth_levels
from btc_engine.market_data.public_client import BinancePublicFuturesClient
from btc_engine.runtime.control_events import emit_control_event
from btc_engine.runtime.control_plane import sync_control_plane
from btc_engine.runtime.hermes_sync import run_remote_command, sync_outputs
from btc_engine.runtime.service_health import heartbeat
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _candidate_symbols(candidate: dict[str, Any]) -> list[str]:
    values = candidate.get("symbols")
    if isinstance(values, list):
        symbols = [str(item).strip().upper() for item in values if str(item).strip()]
        if symbols:
            return symbols
    return ["BTCUSDT", "ETHUSDT"]


def _build_ws_url(*, symbols: list[str], interval: str) -> str:
    base = get_market_ws_base_url().rstrip("/")
    streams: list[str] = []
    for symbol in symbols:
        stream_symbol = symbol.lower()
        streams.extend(
            [
                f"{stream_symbol}@depth20@100ms",
                f"{stream_symbol}@aggTrade",
                f"{stream_symbol}@kline_{interval}",
            ]
        )
    return f"{base}/stream?streams={'/'.join(streams)}"


def _bootstrap_klines(client: BinancePublicFuturesClient, *, symbol: str, interval: str, limit: int = 60) -> list[dict[str, Any]]:
    rows = client.get_json("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    cache: list[dict[str, Any]] = []
    for row in rows:
        cache.append(
            {
                "open_time_ms": int(row[0]),
                "close_time_ms": int(row[6]),
                "open_price": float(row[1]),
                "high_price": float(row[2]),
                "low_price": float(row[3]),
                "close_price": float(row[4]),
                "volume": float(row[5]),
                "is_closed": True,
            }
        )
    return cache[-limit:]


def _bootstrap_depth(client: BinancePublicFuturesClient, *, symbol: str, limit: int = 20) -> tuple[list[list[str]], list[list[str]]]:
    rows = client.get_json("/fapi/v1/depth", {"symbol": symbol, "limit": limit})
    bids = rows.get("bids") or []
    asks = rows.get("asks") or []
    return bids, asks


def _price_changes_from_cache(kline_cache: list[dict[str, Any]]) -> tuple[float | None, float | None, float | None]:
    if len(kline_cache) < 13:
        return None, None, None
    closes = [float(item["close_price"]) for item in kline_cache]
    latest = closes[-1]

    def pct(back: int) -> float:
        base = closes[-1 - back]
        return ((latest - base) / base) * 100 if base else 0.0

    return round(pct(1), 6), round(pct(3), 6), round(pct(12), 6)


def _top_of_book(bids: list[list[str]], asks: list[list[str]]) -> dict[str, float] | None:
    if not bids or not asks:
        return None
    try:
        best_bid_price = float(bids[0][0])
        best_bid_qty = float(bids[0][1])
        best_ask_price = float(asks[0][0])
        best_ask_qty = float(asks[0][1])
    except (TypeError, ValueError, IndexError):
        return None
    mid = (best_bid_price + best_ask_price) / 2.0 if (best_bid_price and best_ask_price) else 0.0
    spread_bps = ((best_ask_price - best_bid_price) / mid * 10_000.0) if mid else 0.0
    microprice = (
        (best_ask_price * best_bid_qty + best_bid_price * best_ask_qty) / (best_bid_qty + best_ask_qty)
        if (best_bid_qty + best_ask_qty) > 0
        else mid
    )
    microprice_bias_bps = ((microprice - mid) / mid * 10_000.0) if mid else 0.0
    return {
        "best_bid_price": best_bid_price,
        "best_bid_qty": best_bid_qty,
        "best_ask_price": best_ask_price,
        "best_ask_qty": best_ask_qty,
        "mid_price": mid,
        "microprice": microprice,
        "microprice_bias_bps": microprice_bias_bps,
        "spread_bps_top": spread_bps,
    }


def _ofi_delta(previous_top: dict[str, float] | None, current_top: dict[str, float] | None) -> float:
    if not previous_top or not current_top:
        return 0.0
    prev_bid_price = float(previous_top.get("best_bid_price") or 0.0)
    prev_bid_qty = float(previous_top.get("best_bid_qty") or 0.0)
    prev_ask_price = float(previous_top.get("best_ask_price") or 0.0)
    prev_ask_qty = float(previous_top.get("best_ask_qty") or 0.0)
    bid_price = float(current_top.get("best_bid_price") or 0.0)
    bid_qty = float(current_top.get("best_bid_qty") or 0.0)
    ask_price = float(current_top.get("best_ask_price") or 0.0)
    ask_qty = float(current_top.get("best_ask_qty") or 0.0)
    bid_flow = (bid_qty if bid_price >= prev_bid_price else 0.0) - (prev_bid_qty if bid_price <= prev_bid_price else 0.0)
    ask_flow = (ask_qty if ask_price <= prev_ask_price else 0.0) - (prev_ask_qty if ask_price >= prev_ask_price else 0.0)
    return round(bid_flow - ask_flow, 6)


def _default_order_notional() -> float:
    execution_mode = get_execution_mode()
    quote_allocation_usdt = 100.0
    leverage = 1
    if execution_mode in {"demo_auto", "testnet_auto"}:
        demo_account = read_runtime_state("demo_account") or {}
        virtual_equity = float(demo_account.get("equity_usdt") or get_demo_start_equity_usdt())
        quote_allocation_usdt = round(virtual_equity * get_demo_position_fraction(), 4)
        leverage = get_demo_leverage()
    return quote_allocation_usdt * leverage


def _sync_interval_sec() -> int:
    return int((get_env("BTC_MARKET_STREAM_SYNC_INTERVAL_SEC", "60") or "60").strip())


def _sync_outputs_to_hermes(*, force: bool = False) -> str | None:
    target = get_hermes_demo_v2_target()
    if not target:
        return None
    now = time.time()
    sync_state = read_runtime_state("market_stream_sync_state") or {}
    last_synced_ts = float(sync_state.get("last_synced_ts") or 0.0)
    if not force and (now - last_synced_ts) < _sync_interval_sec():
        return None
    outputs: dict[str, Path] = {
        "market_stream_state.json": STATE_DIR / "market_stream_state.json",
        "market_stream_snapshot.json": STATE_DIR / "market_stream_snapshot.json",
    }
    try:
        sync_outputs(outputs, target)
        remote_error = run_remote_command(
            target,
            "python3 /opt/phoenix/deploy/hermes/leiting_demo_v2_digest.py --hermes-home /root/.hermes",
        )
    except Exception as exc:  # noqa: BLE001
        remote_error = f"market_stream_sync_failed:{str(exc)[:240]}"
    update_runtime_state(
        "market_stream_sync_state",
        {
            "last_synced_at": _utc_now(),
            "last_synced_ts": now,
            "remote_hook_error": remote_error,
        },
    )
    return remote_error


def _apply_kline_event(cache: list[dict[str, Any]], payload: dict[str, Any]) -> list[dict[str, Any]]:
    kline = payload.get("k") or {}
    item = {
        "open_time_ms": int(kline.get("t") or 0),
        "close_time_ms": int(kline.get("T") or 0),
        "open_price": float(kline.get("o") or 0.0),
        "high_price": float(kline.get("h") or 0.0),
        "low_price": float(kline.get("l") or 0.0),
        "close_price": float(kline.get("c") or 0.0),
        "volume": float(kline.get("v") or 0.0),
        "is_closed": bool(kline.get("x")),
    }
    open_time_ms = item["open_time_ms"]
    updated = False
    for idx, existing in enumerate(cache):
        if int(existing.get("open_time_ms") or 0) == open_time_ms:
            cache[idx] = item
            updated = True
            break
    if not updated:
        cache.append(item)
    cache.sort(key=lambda entry: int(entry.get("open_time_ms") or 0))
    return cache[-60:]


def _build_symbol_snapshot(symbol: str, payload: dict[str, Any], order_notional_usdt: float) -> dict[str, Any]:
    bids = payload.get("depth_bids") or []
    asks = payload.get("depth_asks") or []
    depth = analyze_depth_levels(bids=bids, asks=asks, order_notional_usdt=order_notional_usdt)
    kline_cache = list(payload.get("kline_cache") or [])
    price_change_5m_pct, price_change_15m_pct, price_change_1h_pct = _price_changes_from_cache(kline_cache)
    top = _top_of_book(bids, asks) or {}
    ofi_recent = [float(item) for item in list(payload.get("ofi_recent") or [])[-120:]]
    return {
        "symbol": symbol,
        "last_event_type": payload.get("last_event_type"),
        "last_event_at": payload.get("last_event_at"),
        "last_depth_at": payload.get("last_depth_at"),
        "last_trade_at": payload.get("last_trade_at"),
        "last_kline_at": payload.get("last_kline_at"),
        "depth_levels": {"bids": bids, "asks": asks},
        "depth": depth,
        "kline_cache": kline_cache,
        "trade": payload.get("last_trade") or {},
        "price_change_5m_pct": price_change_5m_pct,
        "price_change_15m_pct": price_change_15m_pct,
        "price_change_1h_pct": price_change_1h_pct,
        "best_bid_price": top.get("best_bid_price"),
        "best_ask_price": top.get("best_ask_price"),
        "mid_price": top.get("mid_price"),
        "microprice": top.get("microprice"),
        "microprice_bias_bps": round(float(top.get("microprice_bias_bps") or 0.0), 6),
        "ofi_latest": ofi_recent[-1] if ofi_recent else 0.0,
        "ofi_window_sum": round(sum(ofi_recent), 6),
        "ofi_window_mean": round((sum(ofi_recent) / len(ofi_recent)), 6) if ofi_recent else 0.0,
    }


def _build_combined_snapshot(*, symbols: list[str], interval: str, state: dict[str, Any]) -> dict[str, Any]:
    order_notional_usdt = _default_order_notional()
    per_symbol: dict[str, Any] = {}
    for symbol in symbols:
        per_symbol[symbol] = _build_symbol_snapshot(symbol, state["symbols"][symbol], order_notional_usdt)
    primary_symbol = symbols[0]
    primary = per_symbol[primary_symbol]
    return {
        "generated_at": _utc_now(),
        "source": "ws",
        "symbol": primary_symbol,
        "interval": interval,
        "symbols": symbols,
        "per_symbol": per_symbol,
        "last_event_type": primary.get("last_event_type"),
        "last_event_at": primary.get("last_event_at"),
        "last_depth_at": primary.get("last_depth_at"),
        "last_trade_at": primary.get("last_trade_at"),
        "last_kline_at": primary.get("last_kline_at"),
        "depth_levels": primary.get("depth_levels"),
        "depth": primary.get("depth"),
        "kline_cache": primary.get("kline_cache"),
        "trade": primary.get("trade"),
        "price_change_5m_pct": primary.get("price_change_5m_pct"),
        "price_change_15m_pct": primary.get("price_change_15m_pct"),
        "price_change_1h_pct": primary.get("price_change_1h_pct"),
    }


def _symbol_from_stream(stream: str) -> str:
    return stream.split("@", 1)[0].upper()


def _consume_background_result(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except Exception:  # noqa: BLE001
        return


async def _run_stream(*, reconnect_delay_sec: int = 3, flush_interval_sec: float = 1.0) -> None:
    try:
        import websockets
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("websockets package is required for market stream service") from exc

    candidate = load_runtime_candidate_config()
    symbols = _candidate_symbols(candidate)
    interval = str(candidate.get("interval") or "5m")
    ws_url = _build_ws_url(symbols=symbols, interval=interval)
    client = BinancePublicFuturesClient()
    symbol_states: dict[str, Any] = {}
    for symbol in symbols:
        depth_bids, depth_asks = _bootstrap_depth(client, symbol=symbol)
        symbol_states[symbol] = {
            "last_event_type": None,
            "last_event_at": None,
            "last_depth_at": None,
            "last_trade_at": None,
            "last_kline_at": None,
            "depth_bids": depth_bids,
            "depth_asks": depth_asks,
            "kline_cache": _bootstrap_klines(client, symbol=symbol, interval=interval),
            "last_trade": {},
            "last_top": _top_of_book(depth_bids, depth_asks),
            "ofi_recent": [],
        }
    base_state: dict[str, Any] = {
        "status": "starting",
        "symbols": symbol_states,
    }
    update_runtime_state(
        "market_stream_state",
        {
            "status": "starting",
            "symbols": symbols,
            "interval": interval,
            "ws_url": ws_url,
            "started_at": _utc_now(),
        },
    )
    update_runtime_state("market_stream_snapshot", _build_combined_snapshot(symbols=symbols, interval=interval, state=base_state))
    pending_remote_sync: asyncio.Task[Any] | None = None
    pending_control_sync: asyncio.Task[Any] | None = None
    while True:
        started_at = _utc_now()
        state = json.loads(json.dumps(base_state))
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
                update_runtime_state(
                    "market_stream_state",
                    {
                        "status": "connected",
                        "symbols": symbols,
                        "interval": interval,
                        "ws_url": ws_url,
                        "started_at": started_at,
                    },
                )
                heartbeat("market_stream", status="running", details={"symbols": symbols, "interval": interval})
                emit_control_event(
                    kind="market_stream_connected",
                    title="雷霆市场流已连接",
                    severity="info",
                    dedupe_key="market_stream_status:connected",
                    details={"symbols": symbols, "interval": interval},
                )
                last_flush = 0.0
                while True:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=max(5, get_market_stream_freshness_sec()))
                    payload = json.loads(raw)
                    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                    stream = str(payload.get("stream") or "")
                    event_type = str(data.get("e") or "")
                    symbol = _symbol_from_stream(stream)
                    if symbol not in state["symbols"] or not event_type:
                        continue
                    now_iso = _utc_now()
                    symbol_state = state["symbols"][symbol]
                    symbol_state["last_event_type"] = event_type
                    symbol_state["last_event_at"] = now_iso
                    if event_type == "depthUpdate":
                        symbol_state["depth_bids"] = data.get("b") or []
                        symbol_state["depth_asks"] = data.get("a") or []
                        current_top = _top_of_book(symbol_state["depth_bids"], symbol_state["depth_asks"])
                        ofi_delta = _ofi_delta(symbol_state.get("last_top"), current_top)
                        ofi_recent = list(symbol_state.get("ofi_recent") or [])
                        ofi_recent.append(ofi_delta)
                        symbol_state["ofi_recent"] = ofi_recent[-120:]
                        symbol_state["last_top"] = current_top
                        symbol_state["last_depth_at"] = now_iso
                    elif event_type == "aggTrade":
                        symbol_state["last_trade_at"] = now_iso
                        symbol_state["last_trade"] = {
                            "trade_id": data.get("a"),
                            "price": float(data.get("p") or 0.0),
                            "quantity": float(data.get("q") or 0.0),
                            "trade_time_ms": data.get("T"),
                            "is_buyer_maker": bool(data.get("m")),
                        }
                    elif event_type == "kline":
                        symbol_state["last_kline_at"] = now_iso
                        symbol_state["kline_cache"] = _apply_kline_event(list(symbol_state.get("kline_cache") or []), data)
                    now = time.time()
                    if now - last_flush >= flush_interval_sec:
                        snapshot = _build_combined_snapshot(symbols=symbols, interval=interval, state=state)
                        update_runtime_state("market_stream_snapshot", snapshot)
                        update_runtime_state(
                            "market_stream_state",
                            {
                                "status": "connected",
                                "symbols": symbols,
                                "interval": interval,
                                "ws_url": ws_url,
                                "started_at": started_at,
                                "last_event_type": event_type,
                                "last_event_symbol": symbol,
                                "last_event_at": now_iso,
                            },
                        )
                        if pending_remote_sync is None or pending_remote_sync.done():
                            pending_remote_sync = asyncio.create_task(asyncio.to_thread(_sync_outputs_to_hermes))
                            pending_remote_sync.add_done_callback(_consume_background_result)
                        if pending_control_sync is None or pending_control_sync.done():
                            pending_control_sync = asyncio.create_task(
                                asyncio.to_thread(sync_control_plane, reason="market_stream_flush")
                            )
                            pending_control_sync.add_done_callback(_consume_background_result)
                        last_flush = now
        except Exception as exc:  # noqa: BLE001
            update_runtime_state(
                "market_stream_state",
                {
                    "status": "reconnecting",
                    "symbols": symbols,
                    "interval": interval,
                    "ws_url": ws_url,
                    "started_at": started_at,
                    "error": str(exc),
                    "error_at": _utc_now(),
                },
            )
            heartbeat("market_stream", status="warning", details={"symbols": symbols, "error": str(exc)})
            emit_control_event(
                kind="market_stream_reconnecting",
                title="雷霆市场流重连中",
                severity="warning",
                dedupe_key=f"market_stream_status:reconnecting:{str(exc)[:80]}",
                details={"symbols": symbols, "error": str(exc)},
            )
            await asyncio.sleep(reconnect_delay_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Leiting v3-simple market stream service.")
    parser.add_argument("--reconnect-delay-sec", type=int, default=3)
    parser.add_argument("--flush-interval-sec", type=float, default=1.0)
    args = parser.parse_args()
    asyncio.run(
        _run_stream(
            reconnect_delay_sec=args.reconnect_delay_sec,
            flush_interval_sec=args.flush_interval_sec,
        )
    )


if __name__ == "__main__":
    main()
