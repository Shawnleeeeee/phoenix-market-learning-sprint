#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import aiohttp

from phoenix.config import load_proxy_settings

DEFAULT_SYMBOLS = ("SOLUSDT", "BTCUSDT")
DEFAULT_SYMBOLS_FILE = Path("alpha_symbols.txt")
USER_AGENT = "project-phoenix-sentry/0.1"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phoenix MVP data sentry: subscribe to Binance USD-M ticker streams."
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Explicit symbol list, e.g. SOLUSDT BTCUSDT",
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        default=DEFAULT_SYMBOLS_FILE,
        help="Text file containing one symbol per line.",
    )
    parser.add_argument(
        "--heartbeat-sec",
        type=int,
        default=20,
        help="WebSocket heartbeat interval.",
    )
    return parser.parse_args()


def load_symbols(args: argparse.Namespace) -> list[str]:
    if args.symbols:
        return normalize_symbols(args.symbols)
    if args.symbols_file.exists():
        return normalize_symbols(args.symbols_file.read_text(encoding="utf-8").splitlines())
    return list(DEFAULT_SYMBOLS)


def normalize_symbols(raw_symbols: Iterable[str]) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for raw in raw_symbols:
        symbol = raw.strip().upper()
        if not symbol or symbol.startswith("#"):
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def build_stream_url(symbols: list[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@ticker" for symbol in symbols)
    return f"wss://fstream.binance.com/stream?streams={streams}"


def render_ticker(data: dict[str, object]) -> str:
    symbol = str(data.get("s") or "?")
    price = str(data.get("c") or "?")
    volume = str(data.get("v") or "?")
    quote_volume = str(data.get("q") or "?")
    price_change_pct = str(data.get("P") or "?")
    return (
        f"[{utc_now()}] 📡 [哨兵监控] {symbol} | 价格: {price} | "
        f"24h涨跌: {price_change_pct}% | 成交量: {volume} | 成交额: {quote_volume}"
    )


async def run_sentry(symbols: list[str], heartbeat_sec: int) -> None:
    url = build_stream_url(symbols)
    proxy_settings = load_proxy_settings()
    print("🚀 凤凰协议：数据哨兵已启动，正在监听 Alpha 信号...")
    print(f"🎯 监控标的: {', '.join(symbols)}")
    print(f"🔗 WebSocket: {url}")

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=90)
    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": USER_AGENT}) as session:
                async with session.ws_connect(
                    url,
                    heartbeat=heartbeat_sec,
                    proxy=proxy_settings.proxy_for_url(url),
                ) as websocket:
                    async for message in websocket:
                        if message.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = json.loads(message.data)
                        data = payload.get("data", payload)
                        if isinstance(data, dict) and "s" in data:
                            print(render_ticker(data), flush=True)
        except asyncio.CancelledError:
            raise
        except KeyboardInterrupt:
            return
        except Exception as exc:  # noqa: BLE001
            print(f"[{utc_now()}] ⚠️ WebSocket 断开，3秒后重连: {exc}", flush=True)
            await asyncio.sleep(3)


def main() -> int:
    args = parse_args()
    symbols = load_symbols(args)
    if not symbols:
        raise SystemExit("No symbols configured. Provide --symbols or populate alpha_symbols.txt")
    try:
        asyncio.run(run_sentry(symbols, args.heartbeat_sec))
    except KeyboardInterrupt:
        print("\n🛑 凤凰协议：数据哨兵已停止。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
