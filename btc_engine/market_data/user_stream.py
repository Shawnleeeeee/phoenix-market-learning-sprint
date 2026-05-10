"""User stream helpers for Leiting testnet."""

from __future__ import annotations

import time

from btc_engine.execution.binance_signed import BinanceSignedFuturesClient


def run_user_stream(client: BinanceSignedFuturesClient) -> dict:
    """Track fills, position changes, and order updates."""
    listen = client.start_user_data_stream()
    return {"listenKey": listen["listenKey"], "started_at_ms": int(time.time() * 1000)}


def keepalive_user_stream(client: BinanceSignedFuturesClient, listen_key: str | None = None) -> dict:
    result = client.keepalive_user_data_stream(listen_key)
    return {"listenKey": listen_key, "result": result, "keepalive_at_ms": int(time.time() * 1000)}


def close_user_stream(client: BinanceSignedFuturesClient, listen_key: str | None = None) -> dict:
    result = client.close_user_data_stream(listen_key)
    return {"listenKey": listen_key, "result": result, "closed_at_ms": int(time.time() * 1000)}
