"""User data stream WebSocket service for 雷霆."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import DATA_DIR, get_execution_mode, get_symbol, get_ws_base_url
from btc_engine.execution.binance_signed import BinanceSignedFuturesClient
from btc_engine.market_data.user_stream import close_user_stream, keepalive_user_stream, run_user_stream
from btc_engine.runtime.control_events import emit_control_event
from btc_engine.runtime.control_plane import sync_control_plane
from btc_engine.runtime.service_health import heartbeat
from btc_engine.runtime.state_store import update_runtime_state


EVENTS_PATH = DATA_DIR / "state" / "user_stream_events.jsonl"
USER_STREAM_STATE_PATH = DATA_DIR / "state" / "user_stream_state.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_event(payload: dict[str, Any]) -> dict[str, Any] | None:
    event_type = str(payload.get("e") or "")
    if event_type not in {"TRADE_LITE", "ALGO_UPDATE"}:
        return None
    normalized = {
        "received_at": _utc_now(),
        "event_type": event_type,
        "event_time_ms": payload.get("E"),
        "transaction_time_ms": payload.get("T"),
        "symbol": payload.get("s") or payload.get("o", {}).get("s"),
        "raw": payload,
    }
    if event_type == "TRADE_LITE":
        normalized.update(
            {
                "order_id": payload.get("i"),
                "client_order_id": payload.get("c"),
                "side": payload.get("S"),
                "last_filled_qty": payload.get("l"),
                "last_filled_price": payload.get("L"),
                "position_side": payload.get("ps"),
            }
        )
    elif event_type == "ALGO_UPDATE":
        algo = payload.get("o") or {}
        normalized.update(
            {
                "algo_id": algo.get("aid") or algo.get("ai"),
                "triggered_order_id": algo.get("ai"),
                "client_algo_id": algo.get("caid") or algo.get("c"),
                "algo_status": algo.get("X"),
                "algo_type": algo.get("at") or algo.get("ot"),
                "order_type": algo.get("o"),
                "side": algo.get("S"),
                "trigger_price": algo.get("tp") or algo.get("sp"),
                "working_type": algo.get("wt"),
            }
        )
    return normalized


def _record_event(normalized: dict[str, Any]) -> None:
    EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(normalized, ensure_ascii=False) + "\n")
    update_runtime_state("last_user_stream_event", normalized)
    if normalized["event_type"] == "TRADE_LITE":
        update_runtime_state("last_trade_lite", normalized)
    elif normalized["event_type"] == "ALGO_UPDATE":
        update_runtime_state("last_algo_update", normalized)
    current = _read_existing_user_stream_state()
    state = {
        "status": "connected" if current.get("status") not in {"reconnecting", "starting"} else current.get("status"),
        "listen_key": current.get("listen_key"),
        "ws_url": current.get("ws_url"),
        "started_at": current.get("started_at"),
        "last_keepalive_at": current.get("last_keepalive_at"),
        "last_event_type": normalized["event_type"],
        "last_event_at": normalized["received_at"],
        "symbol": normalized.get("symbol") or current.get("symbol") or get_symbol(),
    }
    update_runtime_state("user_stream_state", state)
    sync_control_plane(reason="user_stream_event")


def _read_existing_user_stream_state() -> dict[str, Any]:
    if not USER_STREAM_STATE_PATH.exists():
        return {}
    try:
        return json.loads(USER_STREAM_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _last_known_event_fields() -> dict[str, Any]:
    for state_name in ("last_user_stream_event", "last_trade_lite", "last_algo_update"):
        try:
            existing = DATA_DIR / "state" / f"{state_name}.json"
            if not existing.exists():
                continue
            payload = json.loads(existing.read_text(encoding="utf-8"))
        except Exception:
            continue
        event_type = payload.get("event_type")
        event_at = payload.get("received_at")
        symbol = payload.get("symbol")
        if event_type or event_at:
            return {
                "last_event_type": event_type,
                "last_event_at": event_at,
                "symbol": symbol,
            }
    return {}


async def _consume_forever(*, interval_sec: int = 1200) -> None:
    try:
        import websockets
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("websockets package is required for user stream service") from exc

    client = BinanceSignedFuturesClient()
    client.assert_one_way_mode()
    while True:
        listen = run_user_stream(client)
        listen_key = listen["listenKey"]
        ws_url = f"{get_ws_base_url().rstrip('/')}/ws/{listen_key}"
        update_runtime_state(
            "user_stream_state",
            {
                "status": "starting",
                "listen_key": listen_key,
                "ws_url": ws_url,
                "started_at": _utc_now(),
            },
        )
        heartbeat("user_stream", status="running", details={"listen_key": listen_key})
        last_keepalive = time.time()
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
                current = _read_existing_user_stream_state()
                fallback_event = _last_known_event_fields()
                update_runtime_state(
                    "user_stream_state",
                    {
                        "status": "connected",
                        "listen_key": listen_key,
                        "ws_url": ws_url,
                        "started_at": current.get("started_at") or _utc_now(),
                        "last_keepalive_at": current.get("last_keepalive_at"),
                        "last_event_type": current.get("last_event_type") or fallback_event.get("last_event_type"),
                        "last_event_at": current.get("last_event_at") or fallback_event.get("last_event_at"),
                        "symbol": current.get("symbol") or fallback_event.get("symbol") or get_symbol(),
                    },
                )
                emit_control_event(
                    kind="user_stream_connected",
                    title="雷霆用户流已连接",
                    severity="info",
                    dedupe_key="user_stream_status:connected",
                    details={"symbol": get_symbol()},
                )
                while True:
                    timeout = 30
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        if time.time() - last_keepalive >= interval_sec:
                            keepalive_user_stream(client, listen_key)
                            last_keepalive = time.time()
                            state = {
                                "status": "connected",
                                "listen_key": listen_key,
                                "ws_url": ws_url,
                                "started_at": _utc_now(),
                                "last_keepalive_at": _utc_now(),
                                "symbol": get_symbol(),
                            }
                            current = _read_existing_user_stream_state()
                            state["started_at"] = current.get("started_at") or state["started_at"]
                            state["last_event_type"] = current.get("last_event_type")
                            state["last_event_at"] = current.get("last_event_at")
                            update_runtime_state("user_stream_state", state)
                            sync_control_plane(reason="user_stream_keepalive")
                        continue
                    payload = json.loads(raw)
                    normalized = _normalize_event(payload)
                    if normalized:
                        _record_event(normalized)
        except Exception as exc:  # noqa: BLE001
            update_runtime_state(
                "user_stream_state",
                {
                    "status": "reconnecting",
                    "listen_key": listen_key,
                    "ws_url": ws_url,
                    "error": str(exc),
                    "error_at": _utc_now(),
                    "symbol": get_symbol(),
                },
            )
            heartbeat("user_stream", status="warning", details={"error": str(exc)})
            emit_control_event(
                kind="user_stream_reconnecting",
                title="雷霆用户流重连中",
                severity="warning",
                dedupe_key=f"user_stream_status:reconnecting:{str(exc)[:80]}",
                details={"symbol": get_symbol(), "error": str(exc)},
            )
            await asyncio.sleep(3)
        finally:
            try:
                close_user_stream(client, listen_key)
            except Exception:  # noqa: BLE001
                pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Leiting Binance user data stream service.")
    parser.add_argument("--keepalive-sec", type=int, default=1200)
    args = parser.parse_args()
    if get_execution_mode() not in {"demo_auto", "testnet_auto"}:
        heartbeat("user_stream", status="idle", details={"reason": "mode_disabled"})
        print(json.dumps({"ok": False, "reason": "mode_disabled"}, ensure_ascii=False, indent=2))
        return
    asyncio.run(_consume_forever(interval_sec=args.keepalive_sec))


if __name__ == "__main__":
    main()
