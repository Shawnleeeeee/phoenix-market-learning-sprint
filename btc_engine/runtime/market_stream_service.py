"""Public market WebSocket service for 雷霆."""

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
    get_runtime_engine_mode,
    get_symbol,
    load_strategy_config,
    market_stream_enabled,
)
from btc_engine.market_data.depth import analyze_depth_levels
from btc_engine.market_data.public_client import BinancePublicFuturesClient
from btc_engine.runtime.hermes_sync import run_remote_command, sync_outputs
from btc_engine.runtime.service_health import heartbeat
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_ws_url(*, symbol: str, interval: str) -> str:
    base = get_market_ws_base_url().rstrip("/")
    symbol_stream = symbol.lower()
    streams = [
        f"{symbol_stream}@depth20@100ms",
        f"{symbol_stream}@aggTrade",
        f"{symbol_stream}@kline_{interval}",
    ]
    return f"{base}/stream?streams={'/'.join(streams)}"


def _bootstrap_klines(client: BinancePublicFuturesClient, *, symbol: str, interval: str, limit: int = 20) -> list[dict[str, Any]]:
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
    return cache[-20:]


def _price_changes_from_cache(kline_cache: list[dict[str, Any]]) -> tuple[float | None, float | None, float | None]:
    if len(kline_cache) < 13:
        return None, None, None
    closes = [float(item["close_price"]) for item in kline_cache]
    latest = closes[-1]

    def pct(back: int) -> float:
        base = closes[-1 - back]
        return ((latest - base) / base) * 100 if base else 0.0

    return round(pct(1), 6), round(pct(3), 6), round(pct(12), 6)


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


def _sync_market_stream_outputs(*, force: bool = False) -> str | None:
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


def _normalize_market_message(payload: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    event_type = str(data.get("e") or "")
    return (event_type or None), data


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
    return cache[-20:]


def _build_snapshot(*, symbol: str, interval: str, state: dict[str, Any]) -> dict[str, Any]:
    bids = state.get("depth_bids") or []
    asks = state.get("depth_asks") or []
    order_notional_usdt = _default_order_notional()
    depth = analyze_depth_levels(bids=bids, asks=asks, order_notional_usdt=order_notional_usdt)
    kline_cache = list(state.get("kline_cache") or [])
    price_change_5m_pct, price_change_15m_pct, price_change_1h_pct = _price_changes_from_cache(kline_cache)
    return {
        "generated_at": _utc_now(),
        "symbol": symbol,
        "interval": interval,
        "source": "ws",
        "last_event_type": state.get("last_event_type"),
        "last_event_at": state.get("last_event_at"),
        "last_depth_at": state.get("last_depth_at"),
        "last_trade_at": state.get("last_trade_at"),
        "last_kline_at": state.get("last_kline_at"),
        "depth_levels": {"bids": bids, "asks": asks},
        "depth": depth,
        "kline_cache": kline_cache,
        "trade": state.get("last_trade") or {},
        "price_change_5m_pct": price_change_5m_pct,
        "price_change_15m_pct": price_change_15m_pct,
        "price_change_1h_pct": price_change_1h_pct,
    }


async def _run_stream(*, reconnect_delay_sec: int = 3, flush_interval_sec: float = 1.0) -> None:
    try:
        import websockets
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("websockets package is required for market stream service") from exc

    strategy = load_strategy_config()
    symbol = get_symbol()
    interval = strategy.interval
    ws_url = _build_ws_url(symbol=symbol, interval=interval)
    client = BinancePublicFuturesClient()
    base_state: dict[str, Any] = {
        "status": "starting",
        "symbol": symbol,
        "interval": interval,
        "ws_url": ws_url,
        "started_at": _utc_now(),
        "last_event_type": None,
        "last_event_at": None,
        "last_depth_at": None,
        "last_trade_at": None,
        "last_kline_at": None,
        "depth_bids": [],
        "depth_asks": [],
        "kline_cache": _bootstrap_klines(client, symbol=symbol, interval=interval),
        "last_trade": {},
    }
    update_runtime_state(
        "market_stream_state",
        {
            "status": "starting",
            "symbol": symbol,
            "interval": interval,
            "ws_url": ws_url,
            "started_at": base_state["started_at"],
        },
    )
    update_runtime_state("market_stream_snapshot", _build_snapshot(symbol=symbol, interval=interval, state=base_state))

    while True:
        started_at = _utc_now()
        state = dict(base_state)
        state["started_at"] = started_at
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
                state["status"] = "connected"
                update_runtime_state(
                    "market_stream_state",
                    {
                        "status": "connected",
                        "symbol": symbol,
                        "interval": interval,
                        "ws_url": ws_url,
                        "started_at": started_at,
                        "last_event_type": None,
                        "last_event_at": None,
                    },
                )
                heartbeat("market_stream", status="running", details={"symbol": symbol, "interval": interval})
                last_flush = 0.0
                while True:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=max(5, get_market_stream_freshness_sec()))
                    payload = json.loads(raw)
                    event_type, data = _normalize_market_message(payload)
                    if not event_type:
                        continue
                    now_iso = _utc_now()
                    state["last_event_type"] = event_type
                    state["last_event_at"] = now_iso
                    if event_type == "depthUpdate":
                        state["depth_bids"] = data.get("b") or []
                        state["depth_asks"] = data.get("a") or []
                        state["last_depth_at"] = now_iso
                    elif event_type == "aggTrade":
                        state["last_trade_at"] = now_iso
                        state["last_trade"] = {
                            "trade_id": data.get("a"),
                            "price": float(data.get("p") or 0.0),
                            "quantity": float(data.get("q") or 0.0),
                            "trade_time_ms": data.get("T"),
                            "is_buyer_maker": bool(data.get("m")),
                        }
                    elif event_type == "kline":
                        state["last_kline_at"] = now_iso
                        state["kline_cache"] = _apply_kline_event(list(state.get("kline_cache") or []), data)
                    now = time.time()
                    if now - last_flush >= flush_interval_sec:
                        snapshot = _build_snapshot(symbol=symbol, interval=interval, state=state)
                        update_runtime_state("market_stream_snapshot", snapshot)
                        update_runtime_state(
                            "market_stream_state",
                            {
                                "status": "connected",
                                "symbol": symbol,
                                "interval": interval,
                                "ws_url": ws_url,
                                "started_at": started_at,
                                "last_event_type": state.get("last_event_type"),
                                "last_event_at": state.get("last_event_at"),
                                "last_depth_at": state.get("last_depth_at"),
                                "last_trade_at": state.get("last_trade_at"),
                                "last_kline_at": state.get("last_kline_at"),
                            },
                        )
                        remote_error = _sync_market_stream_outputs()
                        if remote_error:
                            update_runtime_state(
                                "market_stream_state",
                                {
                                    "status": "connected",
                                    "symbol": symbol,
                                    "interval": interval,
                                    "ws_url": ws_url,
                                    "started_at": started_at,
                                    "last_event_type": state.get("last_event_type"),
                                    "last_event_at": state.get("last_event_at"),
                                    "last_depth_at": state.get("last_depth_at"),
                                    "last_trade_at": state.get("last_trade_at"),
                                    "last_kline_at": state.get("last_kline_at"),
                                    "last_sync_error": remote_error,
                                    "last_sync_error_at": _utc_now(),
                                },
                            )
                        last_flush = now
        except Exception as exc:  # noqa: BLE001
            update_runtime_state(
                "market_stream_state",
                {
                    "status": "reconnecting",
                    "symbol": symbol,
                    "interval": interval,
                    "ws_url": ws_url,
                    "started_at": started_at,
                    "last_error": str(exc),
                    "last_error_at": _utc_now(),
                },
            )
            heartbeat("market_stream", status="degraded", details={"error": str(exc)})
            await asyncio.sleep(reconnect_delay_sec)


def main() -> None:
    if get_runtime_engine_mode() in {"v3_simple", "v4_microstructure"}:
        from btc_engine.runtime.market_stream_service_v3_simple import main as main_v3

        main_v3()
        return
    parser = argparse.ArgumentParser(description="Run Leiting Binance futures market stream service.")
    parser.add_argument("--reconnect-delay-sec", type=int, default=3)
    parser.add_argument("--flush-interval-sec", type=float, default=1.0)
    args = parser.parse_args()
    if not market_stream_enabled():
        heartbeat("market_stream", status="idle", details={"reason": "market_stream_disabled"})
        print(json.dumps({"ok": False, "reason": "market_stream_disabled"}, ensure_ascii=False, indent=2))
        return
    asyncio.run(_run_stream(reconnect_delay_sec=args.reconnect_delay_sec, flush_interval_sec=args.flush_interval_sec))


if __name__ == "__main__":
    main()
