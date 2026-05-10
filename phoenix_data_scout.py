#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import statistics
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

from phoenix.config import ProxySettings, load_execution_settings, load_proxy_settings

ALPHA_RANK_URL = (
    "https://web3.binance.com/bapi/defi/v1/public/"
    "wallet-direct/buw/wallet/market/token/pulse/unified/rank/list/ai"
)
FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
FUTURES_OPEN_INTEREST_URL = "https://fapi.binance.com/fapi/v1/openInterest"
FUTURES_DEPTH_URL = "https://fapi.binance.com/fapi/v1/depth"
FUTURES_TAKER_FLOW_URL = "https://fapi.binance.com/futures/data/takerlongshortRatio"
FUTURES_MARK_PRICE_WS_URL = "wss://fstream.binance.com/ws/!markPrice@arr@1s"
FUTURES_LIQUIDATION_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
USER_AGENT = "project-phoenix-scout/0.1"
OI_CONFIRMATION_WINDOWS_MS = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
}
VOLUME_CONFIRMATION_WINDOWS_MS = {
    "short": 3 * 60_000,
    "medium": 8 * 60_000,
    "long": 15 * 60_000,
}
LIQUIDATION_WINDOW_MS = 15 * 60_000


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def emit_event(event: str, **payload: Any) -> None:
    record = {"ts": utc_now_iso(), "event": event, **payload}
    print(json.dumps(record, ensure_ascii=False, separators=(",", ":")), flush=True)


@dataclass(slots=True)
class ScoutConfig:
    alpha_refresh_sec: int
    oi_refresh_sec: int
    taker_refresh_sec: int
    oi_concurrency: int
    taker_concurrency: int
    volume_baseline_points: int
    volume_spike_ratio: float
    oi_jump_pct: float
    top_n: int
    max_symbols: int
    snapshot_file: Path


@dataclass(slots=True)
class SymbolMeta:
    alpha_symbol: str
    futures_symbol: str
    chain_id: str
    contract_address: str
    alpha_price: float | None
    volume_5m: float | None
    volume_24h: float | None
    market_cap: float | None
    onboard_date_ms: int | None = None


@dataclass(slots=True)
class SymbolState:
    meta: SymbolMeta
    mark_price: float | None = None
    mark_price_ts_ms: int | None = None
    open_interest: float | None = None
    open_interest_ts_ms: int | None = None
    oi_delta_pct: float | None = None
    oi_delta_1m_pct: float | None = None
    oi_delta_5m_pct: float | None = None
    oi_delta_15m_pct: float | None = None
    price_change_1m_pct: float | None = None
    price_change_5m_pct: float | None = None
    price_change_15m_pct: float | None = None
    index_price: float | None = None
    funding_rate: float | None = None
    next_funding_time_ms: int | None = None
    premium_index: float | None = None
    mark_index_basis_pct: float | None = None
    taker_buy_ratio_1m: float | None = None
    taker_buy_ratio_5m: float | None = None
    aggressive_flow_delta: float | None = None
    taker_buy_volume_5m: float | None = None
    taker_sell_volume_5m: float | None = None
    liquidation_long_usd: float | None = None
    liquidation_short_usd: float | None = None
    liquidation_event_count: int | None = None
    spread_bps: float | None = None
    depth_bid_5: float | None = None
    depth_ask_5: float | None = None
    depth_imbalance: float | None = None
    estimated_slippage_bps: float | None = None
    estimated_slippage_for_order_usdt: float | None = None
    last_volume_ratio: float | None = None
    volume_5m_ratio_short: float | None = None
    volume_5m_ratio_medium: float | None = None
    volume_5m_ratio_long: float | None = None
    volume_5m_history: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=24))
    open_interest_history: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=120))
    mark_price_history: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=120))
    liquidation_history: deque[tuple[int, float, float]] = field(default_factory=lambda: deque(maxlen=256))

    @property
    def oi_notional_usd(self) -> float | None:
        if self.open_interest is None or self.mark_price is None:
            return None
        return self.open_interest * self.mark_price


class PhoenixScout:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: ScoutConfig,
        proxy_settings: ProxySettings,
    ) -> None:
        self.session = session
        self.config = config
        self.proxy_settings = proxy_settings
        self.shutdown = asyncio.Event()
        self.state: dict[str, SymbolState] = {}
        self.execution_settings = load_execution_settings()
        self._load_state_from_snapshot()

    async def run(self) -> None:
        await self.refresh_universe()
        tasks = [
            asyncio.create_task(self.alpha_refresh_loop(), name="alpha-refresh"),
            asyncio.create_task(self.mark_price_loop(), name="mark-price"),
            asyncio.create_task(self.oi_poll_loop(), name="oi-poll"),
            asyncio.create_task(self.taker_flow_poll_loop(), name="taker-flow"),
            asyncio.create_task(self.liquidation_stream_loop(), name="liquidation-stream"),
        ]
        try:
            await self.shutdown.wait()
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            self.write_snapshot()

    def _load_state_from_snapshot(self) -> None:
        snapshot_path = self.config.snapshot_file
        if not snapshot_path or not snapshot_path.exists():
            return
        try:
            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return

        restored: dict[str, SymbolState] = {}
        for item in payload.get("symbols", []):
            symbol = str(item.get("symbol") or "").upper()
            if not symbol:
                continue
            meta = SymbolMeta(
                alpha_symbol=str(item.get("alpha_symbol") or ""),
                futures_symbol=symbol,
                chain_id=str(item.get("chain_id") or ""),
                contract_address=str(item.get("contract_address") or ""),
                alpha_price=parse_float(item.get("alpha_price")),
                volume_5m=parse_float(item.get("volume_5m")),
                volume_24h=parse_float(item.get("volume_24h")),
                market_cap=parse_float(item.get("market_cap")),
                onboard_date_ms=(
                    int(item.get("onboard_date_ms"))
                    if item.get("onboard_date_ms") not in (None, "")
                    else None
                ),
            )
            state = SymbolState(
                meta=meta,
                mark_price=parse_float(item.get("mark_price")),
                mark_price_ts_ms=item.get("mark_price_ts_ms"),
                open_interest=parse_float(item.get("open_interest")),
                open_interest_ts_ms=item.get("open_interest_ts_ms"),
                oi_delta_pct=parse_float(item.get("oi_delta_pct")),
                oi_delta_1m_pct=parse_float(item.get("oi_delta_1m_pct")),
                oi_delta_5m_pct=parse_float(item.get("oi_delta_5m_pct")),
                oi_delta_15m_pct=parse_float(item.get("oi_delta_15m_pct")),
                price_change_1m_pct=parse_float(item.get("price_change_1m_pct")),
                price_change_5m_pct=parse_float(item.get("price_change_5m_pct")),
                price_change_15m_pct=parse_float(item.get("price_change_15m_pct")),
                index_price=parse_float(item.get("index_price")),
                funding_rate=parse_float(item.get("funding_rate")),
                next_funding_time_ms=item.get("next_funding_time_ms"),
                premium_index=parse_float(item.get("premium_index")),
                mark_index_basis_pct=parse_float(item.get("mark_index_basis_pct")),
                taker_buy_ratio_1m=parse_float(item.get("taker_buy_ratio_1m")),
                taker_buy_ratio_5m=parse_float(item.get("taker_buy_ratio_5m")),
                aggressive_flow_delta=parse_float(item.get("aggressive_flow_delta")),
                taker_buy_volume_5m=parse_float(item.get("taker_buy_volume_5m")),
                taker_sell_volume_5m=parse_float(item.get("taker_sell_volume_5m")),
                liquidation_long_usd=parse_float(item.get("liquidation_long_usd")),
                liquidation_short_usd=parse_float(item.get("liquidation_short_usd")),
                liquidation_event_count=(
                    int(item.get("liquidation_event_count"))
                    if item.get("liquidation_event_count") not in (None, "")
                    else None
                ),
                spread_bps=parse_float(item.get("spread_bps")),
                depth_bid_5=parse_float(item.get("depth_bid_5")),
                depth_ask_5=parse_float(item.get("depth_ask_5")),
                depth_imbalance=parse_float(item.get("depth_imbalance")),
                estimated_slippage_bps=parse_float(item.get("estimated_slippage_bps")),
                estimated_slippage_for_order_usdt=parse_float(item.get("estimated_slippage_for_order_usdt")),
                last_volume_ratio=parse_float(item.get("volume_5m_ratio")),
                volume_5m_ratio_short=parse_float(item.get("volume_5m_ratio_short")),
                volume_5m_ratio_medium=parse_float(item.get("volume_5m_ratio_medium")),
                volume_5m_ratio_long=parse_float(item.get("volume_5m_ratio_long")),
            )
            state.volume_5m_history = self._decode_history(item.get("volume_5m_history"), maxlen=24)
            state.open_interest_history = self._decode_history(item.get("open_interest_history"), maxlen=120)
            state.mark_price_history = self._decode_history(item.get("mark_price_history"), maxlen=120)
            state.liquidation_history = self._decode_liquidation_history(item.get("liquidation_history"), maxlen=256)
            if not state.volume_5m_history and meta.volume_5m is not None:
                observed_at_ms = int(item.get("generated_at_ms") or time.time() * 1000)
                state.volume_5m_history.append((observed_at_ms, meta.volume_5m))
            if not state.open_interest_history and state.open_interest is not None and state.open_interest_ts_ms is not None:
                state.open_interest_history.append((int(state.open_interest_ts_ms), float(state.open_interest)))
            if not state.mark_price_history and state.mark_price is not None and state.mark_price_ts_ms is not None:
                state.mark_price_history.append((int(state.mark_price_ts_ms), float(state.mark_price)))
            restored[symbol] = state
        self.state = restored

    async def alpha_refresh_loop(self) -> None:
        while not self.shutdown.is_set():
            await asyncio.sleep(self.config.alpha_refresh_sec)
            await self.refresh_universe()

    async def refresh_universe(self) -> None:
        alpha_tokens = await self.fetch_all_alpha_tokens()
        futures_symbols = await self.fetch_futures_symbols()
        observed_at_ms = int(time.time() * 1000)
        next_state: dict[str, SymbolState] = {}

        for token in alpha_tokens:
            symbol = (token.get("symbol") or "").upper()
            futures_info = futures_symbols.get(symbol)
            if not futures_info:
                continue
            futures_symbol = str(futures_info.get("symbol") or "").upper()
            if not futures_symbol:
                continue

            meta = SymbolMeta(
                alpha_symbol=symbol,
                futures_symbol=futures_symbol,
                chain_id=str(token.get("chainId") or ""),
                contract_address=str(token.get("contractAddress") or ""),
                alpha_price=parse_float(token.get("price")),
                volume_5m=parse_float(token.get("volume5m")),
                volume_24h=parse_float(token.get("volume24h")),
                market_cap=parse_float(token.get("marketCap")),
                onboard_date_ms=(
                    int(futures_info.get("onboardDate"))
                    if futures_info.get("onboardDate") not in (None, "")
                    else None
                ),
            )

            existing = self.state.get(futures_symbol)
            if existing is not None:
                existing.meta = meta
                self._update_volume_history(existing, meta.volume_5m, observed_at_ms)
                next_state[futures_symbol] = existing
                continue

            next_state[futures_symbol] = SymbolState(meta=meta)
            self._update_volume_history(next_state[futures_symbol], meta.volume_5m, observed_at_ms)

        sorted_symbols = sorted(
            next_state.values(),
            key=lambda item: item.meta.volume_24h or 0.0,
            reverse=True,
        )

        if self.config.max_symbols > 0:
            sorted_symbols = sorted_symbols[: self.config.max_symbols]

        self.state = {item.meta.futures_symbol: item for item in sorted_symbols}
        emit_event(
            "universe_refreshed",
            overlap_count=len(self.state),
            sample_symbols=[item.meta.futures_symbol for item in sorted_symbols[:15]],
        )
        self.write_snapshot()

    async def fetch_all_alpha_tokens(self) -> list[dict[str, Any]]:
        page = 1
        items: list[dict[str, Any]] = []
        total = None

        while total is None or len(items) < total:
            body = {
                "rankType": 20,
                "period": 50,
                "page": page,
                "size": 200,
            }
            async with self.session.post(
                ALPHA_RANK_URL,
                json=body,
                proxy=self.proxy_settings.proxy_for_url(ALPHA_RANK_URL),
            ) as response:
                response.raise_for_status()
                payload = await response.json()

            data = payload["data"]
            total = int(data["total"])
            items.extend(data["tokens"])
            page += 1

        deduped: dict[str, dict[str, Any]] = {}
        for token in items:
            symbol = (token.get("symbol") or "").upper()
            existing = deduped.get(symbol)
            current_volume = parse_float(token.get("volume24h")) or 0.0
            existing_volume = parse_float(existing.get("volume24h")) if existing else -1.0
            if existing is None or current_volume > (existing_volume or 0.0):
                deduped[symbol] = token

        return list(deduped.values())

    async def fetch_futures_symbols(self) -> dict[str, dict[str, Any]]:
        async with self.session.get(
            FUTURES_EXCHANGE_INFO_URL,
            proxy=self.proxy_settings.proxy_for_url(FUTURES_EXCHANGE_INFO_URL),
        ) as response:
            response.raise_for_status()
            payload = await response.json()

        mapping: dict[str, dict[str, Any]] = {}
        for item in payload["symbols"]:
            if item.get("status") != "TRADING":
                continue
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
            base_asset = str(item.get("baseAsset") or "").upper()
            symbol = str(item.get("symbol") or "").upper()
            if base_asset:
                mapping[base_asset] = {
                    "symbol": symbol,
                    "onboardDate": item.get("onboardDate"),
                }
        return mapping

    async def mark_price_loop(self) -> None:
        while not self.shutdown.is_set():
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=60)
                async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": USER_AGENT}) as ws_session:
                    async with ws_session.ws_connect(
                        FUTURES_MARK_PRICE_WS_URL,
                        heartbeat=20,
                        proxy=self.proxy_settings.proxy_for_url(FUTURES_MARK_PRICE_WS_URL),
                    ) as websocket:
                        emit_event("mark_price_stream_connected", url=FUTURES_MARK_PRICE_WS_URL)
                        async for message in websocket:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            payload = json.loads(message.data)
                            if not isinstance(payload, list):
                                continue
                            for item in payload:
                                symbol = item.get("s")
                                if symbol not in self.state:
                                    continue
                                current = self.state[symbol]
                                current.mark_price = parse_float(item.get("p"))
                                current.mark_price_ts_ms = int(item.get("E"))
                                self._update_mark_price_history(
                                    current,
                                    current.mark_price,
                                    current.mark_price_ts_ms,
                                )
                                current.index_price = parse_float(item.get("i"))
                                current.funding_rate = parse_float(item.get("r"))
                                current.next_funding_time_ms = (
                                    int(item.get("T")) if item.get("T") not in (None, "") else None
                                )
                                self._update_basis_fields(current)
                            if self.shutdown.is_set():
                                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                emit_event("mark_price_stream_error", error=str(exc))
                await asyncio.sleep(3)

    async def oi_poll_loop(self) -> None:
        while not self.shutdown.is_set():
            started = time.perf_counter()
            symbols = list(self.state.keys())
            if not symbols:
                await asyncio.sleep(self.config.oi_refresh_sec)
                continue

            semaphore = asyncio.Semaphore(self.config.oi_concurrency)

            async def poll_one(symbol: str) -> None:
                try:
                    async with semaphore:
                        params = {"symbol": symbol}
                        async with self.session.get(
                            FUTURES_OPEN_INTEREST_URL,
                            params=params,
                            proxy=self.proxy_settings.proxy_for_url(FUTURES_OPEN_INTEREST_URL),
                        ) as response:
                            response.raise_for_status()
                            payload = await response.json()
                        async with self.session.get(
                            FUTURES_DEPTH_URL,
                            params={"symbol": symbol, "limit": 5},
                            proxy=self.proxy_settings.proxy_for_url(FUTURES_DEPTH_URL),
                        ) as depth_response:
                            depth_response.raise_for_status()
                            depth_payload = await depth_response.json()
                except Exception as exc:  # noqa: BLE001
                    emit_event("oi_poll_error", symbol=symbol, error=str(exc))
                    return

                current_state = self.state.get(symbol)
                if current_state is None:
                    return

                new_oi = parse_float(payload.get("openInterest"))
                current_state.open_interest = new_oi
                current_state.open_interest_ts_ms = int(payload.get("time"))
                self._update_open_interest_history(current_state, new_oi, current_state.open_interest_ts_ms)
                self._update_depth_fields(current_state, depth_payload)

                oi_confirmation = max(
                    (
                        abs(delta)
                        for delta in (
                            current_state.oi_delta_1m_pct,
                            current_state.oi_delta_5m_pct,
                            current_state.oi_delta_15m_pct,
                        )
                        if delta is not None
                    ),
                    default=0.0,
                )
                if oi_confirmation >= self.config.oi_jump_pct:
                    strongest_delta = current_state.oi_delta_5m_pct
                    if strongest_delta is None:
                        strongest_delta = current_state.oi_delta_1m_pct
                    if strongest_delta is None:
                        strongest_delta = current_state.oi_delta_15m_pct
                    if strongest_delta is not None:
                        emit_event(
                            "oi_jump",
                            symbol=symbol,
                            chain_id=current_state.meta.chain_id,
                            mark_price=current_state.mark_price,
                            open_interest=new_oi,
                            oi_notional_usd=current_state.oi_notional_usd,
                            oi_delta_pct=round(strongest_delta, 4),
                            oi_delta_1m_pct=(
                                round(current_state.oi_delta_1m_pct, 4)
                                if current_state.oi_delta_1m_pct is not None
                                else None
                            ),
                            oi_delta_5m_pct=(
                                round(current_state.oi_delta_5m_pct, 4)
                                if current_state.oi_delta_5m_pct is not None
                                else None
                            ),
                            oi_delta_15m_pct=(
                                round(current_state.oi_delta_15m_pct, 4)
                                if current_state.oi_delta_15m_pct is not None
                                else None
                            ),
                            volume_5m_ratio=current_state.last_volume_ratio,
                        )

            await asyncio.gather(*(poll_one(symbol) for symbol in symbols), return_exceptions=False)

            ordered = sorted(
                self.state.values(),
                key=lambda item: abs(item.oi_delta_pct or 0.0),
                reverse=True,
            )
            top_items = ordered[: self.config.top_n]
            emit_event(
                "oi_cycle_summary",
                universe_size=len(symbols),
                latency_ms=round((time.perf_counter() - started) * 1000, 2),
                top=[
                    {
                        "symbol": item.meta.futures_symbol,
                        "chain_id": item.meta.chain_id,
                        "mark_price": item.mark_price,
                        "open_interest": item.open_interest,
                        "oi_notional_usd": item.oi_notional_usd,
                        "oi_delta_pct": round(item.oi_delta_pct, 4) if item.oi_delta_pct is not None else None,
                        "oi_delta_1m_pct": (
                            round(item.oi_delta_1m_pct, 4) if item.oi_delta_1m_pct is not None else None
                        ),
                        "oi_delta_5m_pct": (
                            round(item.oi_delta_5m_pct, 4) if item.oi_delta_5m_pct is not None else None
                        ),
                        "oi_delta_15m_pct": (
                            round(item.oi_delta_15m_pct, 4) if item.oi_delta_15m_pct is not None else None
                        ),
                        "price_change_1m_pct": (
                            round(item.price_change_1m_pct, 4)
                            if item.price_change_1m_pct is not None
                            else None
                        ),
                        "price_change_5m_pct": (
                            round(item.price_change_5m_pct, 4)
                            if item.price_change_5m_pct is not None
                            else None
                        ),
                        "price_change_15m_pct": (
                            round(item.price_change_15m_pct, 4)
                            if item.price_change_15m_pct is not None
                            else None
                        ),
                        "volume_5m_ratio": round(item.last_volume_ratio, 4)
                        if item.last_volume_ratio is not None
                        else None,
                        "volume_5m_ratio_short": (
                            round(item.volume_5m_ratio_short, 4)
                            if item.volume_5m_ratio_short is not None
                            else None
                        ),
                        "volume_5m_ratio_medium": (
                            round(item.volume_5m_ratio_medium, 4)
                            if item.volume_5m_ratio_medium is not None
                            else None
                        ),
                        "volume_5m_ratio_long": (
                            round(item.volume_5m_ratio_long, 4)
                            if item.volume_5m_ratio_long is not None
                            else None
                        ),
                    }
                    for item in top_items
                ],
            )
            self.write_snapshot()
            await asyncio.sleep(self.config.oi_refresh_sec)

    async def taker_flow_poll_loop(self) -> None:
        while not self.shutdown.is_set():
            started = time.perf_counter()
            symbols = list(self.state.keys())
            if not symbols:
                await asyncio.sleep(self.config.taker_refresh_sec)
                continue

            semaphore = asyncio.Semaphore(self.config.taker_concurrency)

            async def poll_one(symbol: str) -> None:
                try:
                    async with semaphore:
                        async with self.session.get(
                            FUTURES_TAKER_FLOW_URL,
                            params={"symbol": symbol, "period": "5m", "limit": 1},
                            proxy=self.proxy_settings.proxy_for_url(FUTURES_TAKER_FLOW_URL),
                        ) as response:
                            response.raise_for_status()
                            payload = await response.json()
                except Exception as exc:  # noqa: BLE001
                    emit_event("taker_flow_poll_error", symbol=symbol, error=str(exc))
                    return

                current_state = self.state.get(symbol)
                if current_state is None:
                    return
                latest = payload[0] if isinstance(payload, list) and payload else None
                if not isinstance(latest, dict):
                    return
                buy_vol = parse_float(latest.get("buyVol"))
                sell_vol = parse_float(latest.get("sellVol"))
                ratio = parse_float(latest.get("buySellRatio"))
                current_state.taker_buy_volume_5m = buy_vol
                current_state.taker_sell_volume_5m = sell_vol
                current_state.taker_buy_ratio_5m = ratio
                if buy_vol is not None and sell_vol is not None:
                    total = buy_vol + sell_vol
                    current_state.aggressive_flow_delta = ((buy_vol - sell_vol) / total) if total > 0 else None

            await asyncio.gather(*(poll_one(symbol) for symbol in symbols), return_exceptions=False)
            emit_event(
                "taker_flow_cycle_summary",
                universe_size=len(symbols),
                latency_ms=round((time.perf_counter() - started) * 1000, 2),
                top=[
                    {
                        "symbol": state.meta.futures_symbol,
                        "taker_buy_ratio_5m": (
                            round(state.taker_buy_ratio_5m, 4) if state.taker_buy_ratio_5m is not None else None
                        ),
                        "aggressive_flow_delta": (
                            round(state.aggressive_flow_delta, 4)
                            if state.aggressive_flow_delta is not None
                            else None
                        ),
                    }
                    for state in sorted(
                        self.state.values(),
                        key=lambda item: abs(item.aggressive_flow_delta or 0.0),
                        reverse=True,
                    )[: self.config.top_n]
                ],
            )
            self.write_snapshot()
            await asyncio.sleep(self.config.taker_refresh_sec)

    async def liquidation_stream_loop(self) -> None:
        while not self.shutdown.is_set():
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=60)
                async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": USER_AGENT}) as ws_session:
                    async with ws_session.ws_connect(
                        FUTURES_LIQUIDATION_WS_URL,
                        heartbeat=20,
                        proxy=self.proxy_settings.proxy_for_url(FUTURES_LIQUIDATION_WS_URL),
                    ) as websocket:
                        emit_event("liquidation_stream_connected", url=FUTURES_LIQUIDATION_WS_URL)
                        async for message in websocket:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            payload = json.loads(message.data)
                            if not isinstance(payload, dict):
                                continue
                            order = payload.get("o")
                            if not isinstance(order, dict):
                                continue
                            symbol = str(order.get("s") or "").upper()
                            current_state = self.state.get(symbol)
                            if current_state is None:
                                continue
                            side = str(order.get("S") or "").upper()
                            average_price = parse_float(order.get("ap")) or parse_float(order.get("p"))
                            filled_qty = parse_float(order.get("z")) or parse_float(order.get("q"))
                            event_ts_ms = (
                                int(order.get("T"))
                                if order.get("T") not in (None, "")
                                else int(payload.get("E") or int(time.time() * 1000))
                            )
                            if average_price is None or filled_qty is None:
                                continue
                            notional = average_price * filled_qty
                            long_liq = notional if side == "SELL" else 0.0
                            short_liq = notional if side == "BUY" else 0.0
                            self._update_liquidation_history(current_state, event_ts_ms, long_liq, short_liq)
                            emit_event(
                                "liquidation_snapshot",
                                symbol=symbol,
                                side=side,
                                notional_usd=round(notional, 4),
                                liquidation_long_usd=(
                                    round(current_state.liquidation_long_usd, 4)
                                    if current_state.liquidation_long_usd is not None
                                    else None
                                ),
                                liquidation_short_usd=(
                                    round(current_state.liquidation_short_usd, 4)
                                    if current_state.liquidation_short_usd is not None
                                    else None
                                ),
                                liquidation_event_count=current_state.liquidation_event_count,
                            )
                            if self.shutdown.is_set():
                                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                emit_event("liquidation_stream_error", error=str(exc))
                await asyncio.sleep(3)

    def _update_volume_history(
        self,
        current_state: SymbolState,
        current_volume_5m: float | None,
        observed_at_ms: int,
    ) -> None:
        if current_volume_5m is None:
            return

        history = current_state.volume_5m_history
        current_state.volume_5m_ratio_short = self._volume_ratio_for_window(
            history, observed_at_ms, current_volume_5m, VOLUME_CONFIRMATION_WINDOWS_MS["short"]
        )
        current_state.volume_5m_ratio_medium = self._volume_ratio_for_window(
            history, observed_at_ms, current_volume_5m, VOLUME_CONFIRMATION_WINDOWS_MS["medium"]
        )
        current_state.volume_5m_ratio_long = self._volume_ratio_for_window(
            history, observed_at_ms, current_volume_5m, VOLUME_CONFIRMATION_WINDOWS_MS["long"]
        )
        current_state.last_volume_ratio = (
            current_state.volume_5m_ratio_medium
            or current_state.volume_5m_ratio_short
            or current_state.volume_5m_ratio_long
        )
        if current_state.last_volume_ratio is not None and current_state.last_volume_ratio >= self.config.volume_spike_ratio:
            baseline = self._window_average(
                history,
                observed_at_ms,
                VOLUME_CONFIRMATION_WINDOWS_MS["medium"],
            )
            emit_event(
                "volume_spike",
                symbol=current_state.meta.futures_symbol,
                chain_id=current_state.meta.chain_id,
                mark_price=current_state.mark_price or current_state.meta.alpha_price,
                volume_5m=current_volume_5m,
                volume_5m_baseline=baseline,
                volume_ratio=round(current_state.last_volume_ratio, 4),
                volume_ratio_short=(
                    round(current_state.volume_5m_ratio_short, 4)
                    if current_state.volume_5m_ratio_short is not None
                    else None
                ),
                volume_ratio_medium=(
                    round(current_state.volume_5m_ratio_medium, 4)
                    if current_state.volume_5m_ratio_medium is not None
                    else None
                ),
                volume_ratio_long=(
                    round(current_state.volume_5m_ratio_long, 4)
                    if current_state.volume_5m_ratio_long is not None
                    else None
                ),
            )
        history.append((observed_at_ms, current_volume_5m))

    def _update_open_interest_history(
        self,
        current_state: SymbolState,
        new_oi: float | None,
        observed_at_ms: int | None,
    ) -> None:
        if new_oi is None or observed_at_ms is None:
            current_state.oi_delta_1m_pct = None
            current_state.oi_delta_5m_pct = None
            current_state.oi_delta_15m_pct = None
            current_state.oi_delta_pct = None
            return

        history = current_state.open_interest_history
        current_state.oi_delta_1m_pct = self._pct_change_from_history(
            history,
            observed_at_ms,
            new_oi,
            OI_CONFIRMATION_WINDOWS_MS["1m"],
        )
        current_state.oi_delta_5m_pct = self._pct_change_from_history(
            history,
            observed_at_ms,
            new_oi,
            OI_CONFIRMATION_WINDOWS_MS["5m"],
        )
        current_state.oi_delta_15m_pct = self._pct_change_from_history(
            history,
            observed_at_ms,
            new_oi,
            OI_CONFIRMATION_WINDOWS_MS["15m"],
        )
        current_state.oi_delta_pct = (
            current_state.oi_delta_5m_pct
            if current_state.oi_delta_5m_pct is not None
            else current_state.oi_delta_1m_pct
        )
        history.append((observed_at_ms, new_oi))

    @staticmethod
    def _pct_change_from_history(
        history: deque[tuple[int, float]],
        current_ts_ms: int,
        current_value: float,
        window_ms: int,
    ) -> float | None:
        reference = None
        threshold_ts = current_ts_ms - window_ms
        for observed_ts, value in history:
            if observed_ts <= threshold_ts:
                reference = value
            else:
                break
        if reference is None or reference <= 0:
            return None
        return ((current_value - reference) / reference) * 100.0

    @staticmethod
    def _window_average(
        history: deque[tuple[int, float]],
        current_ts_ms: int,
        window_ms: int,
    ) -> float | None:
        lower_bound = current_ts_ms - window_ms
        has_full_window = any(observed_ts <= lower_bound for observed_ts, _ in history)
        if not has_full_window:
            return None
        samples = [value for observed_ts, value in history if lower_bound <= observed_ts < current_ts_ms]
        if not samples:
            return None
        return statistics.fmean(samples)

    def _volume_ratio_for_window(
        self,
        history: deque[tuple[int, float]],
        current_ts_ms: int,
        current_volume_5m: float,
        window_ms: int,
    ) -> float | None:
        baseline = self._window_average(history, current_ts_ms, window_ms)
        if baseline is None or baseline <= 0:
            return None
        return current_volume_5m / baseline

    def _update_liquidation_history(
        self,
        current_state: SymbolState,
        observed_at_ms: int,
        long_liq_usd: float,
        short_liq_usd: float,
    ) -> None:
        history = current_state.liquidation_history
        history.append((observed_at_ms, long_liq_usd, short_liq_usd))
        lower_bound = observed_at_ms - LIQUIDATION_WINDOW_MS
        while history and history[0][0] < lower_bound:
            history.popleft()
        current_state.liquidation_long_usd = round(sum(item[1] for item in history), 4)
        current_state.liquidation_short_usd = round(sum(item[2] for item in history), 4)
        current_state.liquidation_event_count = len(history)

    def write_snapshot(self) -> None:
        if not self.config.snapshot_file:
            return

        payload = {
            "generated_at": utc_now_iso(),
            "universe_size": len(self.state),
            "symbols": [
                {
                    "symbol": state.meta.futures_symbol,
                    "alpha_symbol": state.meta.alpha_symbol,
                    "chain_id": state.meta.chain_id,
                    "contract_address": state.meta.contract_address,
                    "alpha_price": state.meta.alpha_price,
                    "mark_price": state.mark_price,
                    "mark_price_ts_ms": state.mark_price_ts_ms,
                    "open_interest": state.open_interest,
                    "open_interest_ts_ms": state.open_interest_ts_ms,
                    "oi_notional_usd": state.oi_notional_usd,
                    "oi_delta_pct": state.oi_delta_pct,
                    "oi_delta_1m_pct": state.oi_delta_1m_pct,
                    "oi_delta_5m_pct": state.oi_delta_5m_pct,
                    "oi_delta_15m_pct": state.oi_delta_15m_pct,
                    "price_change_1m_pct": state.price_change_1m_pct,
                    "price_change_5m_pct": state.price_change_5m_pct,
                    "price_change_15m_pct": state.price_change_15m_pct,
                    "index_price": state.index_price,
                    "funding_rate": state.funding_rate,
                    "next_funding_time_ms": state.next_funding_time_ms,
                    "premium_index": state.premium_index,
                    "mark_index_basis_pct": state.mark_index_basis_pct,
                    "taker_buy_ratio_1m": state.taker_buy_ratio_1m,
                    "taker_buy_ratio_5m": state.taker_buy_ratio_5m,
                    "aggressive_flow_delta": state.aggressive_flow_delta,
                    "taker_buy_volume_5m": state.taker_buy_volume_5m,
                    "taker_sell_volume_5m": state.taker_sell_volume_5m,
                    "liquidation_long_usd": state.liquidation_long_usd,
                    "liquidation_short_usd": state.liquidation_short_usd,
                    "liquidation_event_count": state.liquidation_event_count,
                    "spread_bps": state.spread_bps,
                    "depth_bid_5": state.depth_bid_5,
                    "depth_ask_5": state.depth_ask_5,
                    "depth_imbalance": state.depth_imbalance,
                    "estimated_slippage_bps": state.estimated_slippage_bps,
                    "estimated_slippage_for_order_usdt": state.estimated_slippage_for_order_usdt,
                    "volume_5m": state.meta.volume_5m,
                    "volume_24h": state.meta.volume_24h,
                    "market_cap": state.meta.market_cap,
                    "onboard_date_ms": state.meta.onboard_date_ms,
                    "volume_5m_ratio": state.last_volume_ratio,
                    "volume_5m_ratio_short": state.volume_5m_ratio_short,
                    "volume_5m_ratio_medium": state.volume_5m_ratio_medium,
                    "volume_5m_ratio_long": state.volume_5m_ratio_long,
                    "volume_5m_history": list(state.volume_5m_history),
                    "open_interest_history": list(state.open_interest_history),
                    "mark_price_history": list(state.mark_price_history),
                    "liquidation_history": list(state.liquidation_history),
                    "generated_at_ms": int(time.time() * 1000),
                }
                for state in sorted(self.state.values(), key=lambda item: item.meta.futures_symbol)
            ],
        }

        tmp_path = self.config.snapshot_file.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.config.snapshot_file)

    @staticmethod
    def _decode_history(raw_history: Any, *, maxlen: int) -> deque[tuple[int, float]]:
        history: deque[tuple[int, float]] = deque(maxlen=maxlen)
        if not isinstance(raw_history, list):
            return history
        for item in raw_history:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                continue
            try:
                history.append((int(item[0]), float(item[1])))
            except (TypeError, ValueError):
                continue
        return history

    @staticmethod
    def _decode_liquidation_history(raw_history: Any, *, maxlen: int) -> deque[tuple[int, float, float]]:
        history: deque[tuple[int, float, float]] = deque(maxlen=maxlen)
        if not isinstance(raw_history, list):
            return history
        for item in raw_history:
            if not isinstance(item, (list, tuple)) or len(item) != 3:
                continue
            try:
                history.append((int(item[0]), float(item[1]), float(item[2])))
            except (TypeError, ValueError):
                continue
        return history

    def _update_mark_price_history(
        self,
        current_state: SymbolState,
        mark_price: float | None,
        observed_at_ms: int | None,
    ) -> None:
        if mark_price is None or observed_at_ms is None:
            current_state.price_change_1m_pct = None
            current_state.price_change_5m_pct = None
            current_state.price_change_15m_pct = None
            return

        history = current_state.mark_price_history
        current_state.price_change_1m_pct = self._pct_change_from_history(
            history,
            observed_at_ms,
            mark_price,
            OI_CONFIRMATION_WINDOWS_MS["1m"],
        )
        current_state.price_change_5m_pct = self._pct_change_from_history(
            history,
            observed_at_ms,
            mark_price,
            OI_CONFIRMATION_WINDOWS_MS["5m"],
        )
        current_state.price_change_15m_pct = self._pct_change_from_history(
            history,
            observed_at_ms,
            mark_price,
            OI_CONFIRMATION_WINDOWS_MS["15m"],
        )
        history.append((observed_at_ms, mark_price))

    @staticmethod
    def _sum_depth_notional(levels: Any) -> float | None:
        if not isinstance(levels, list):
            return None
        total = 0.0
        has_any = False
        for item in levels[:5]:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            price = parse_float(item[0])
            qty = parse_float(item[1])
            if price is None or qty is None:
                continue
            total += price * qty
            has_any = True
        return total if has_any else None

    def _estimate_side_slippage_bps(self, levels: Any, notional_usdt: float, mark_price: float | None) -> float | None:
        if mark_price is None or mark_price <= 0 or not isinstance(levels, list):
            return None
        remaining = float(notional_usdt)
        acquired_qty = 0.0
        spent_notional = 0.0
        for item in levels:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            price = parse_float(item[0])
            qty = parse_float(item[1])
            if price is None or qty is None or price <= 0 or qty <= 0:
                continue
            level_notional = price * qty
            take_notional = min(level_notional, remaining)
            take_qty = take_notional / price
            spent_notional += take_notional
            acquired_qty += take_qty
            remaining -= take_notional
            if remaining <= 1e-9:
                break
        if acquired_qty <= 0:
            return None
        average_price = spent_notional / acquired_qty
        return abs((average_price - mark_price) / mark_price) * 10000.0

    def _update_basis_fields(self, current_state: SymbolState) -> None:
        if current_state.mark_price is None or current_state.index_price in (None, 0):
            current_state.premium_index = None
            current_state.mark_index_basis_pct = None
            return
        premium = current_state.mark_price - current_state.index_price
        current_state.premium_index = premium
        current_state.mark_index_basis_pct = (premium / current_state.index_price) * 100.0

    def _update_depth_fields(self, current_state: SymbolState, payload: Any) -> None:
        if not isinstance(payload, dict):
            current_state.spread_bps = None
            current_state.depth_bid_5 = None
            current_state.depth_ask_5 = None
            current_state.depth_imbalance = None
            current_state.estimated_slippage_bps = None
            current_state.estimated_slippage_for_order_usdt = None
            return
        bids = payload.get("bids")
        asks = payload.get("asks")
        best_bid = parse_float(bids[0][0]) if isinstance(bids, list) and bids and isinstance(bids[0], (list, tuple)) else None
        best_ask = parse_float(asks[0][0]) if isinstance(asks, list) and asks and isinstance(asks[0], (list, tuple)) else None
        if best_bid is not None and best_ask is not None and best_bid > 0:
            mid = (best_bid + best_ask) / 2.0
            current_state.spread_bps = ((best_ask - best_bid) / mid) * 10000.0 if mid > 0 else None
        else:
            current_state.spread_bps = None
        current_state.depth_bid_5 = self._sum_depth_notional(bids)
        current_state.depth_ask_5 = self._sum_depth_notional(asks)
        if current_state.depth_bid_5 and current_state.depth_ask_5:
            total = current_state.depth_bid_5 + current_state.depth_ask_5
            current_state.depth_imbalance = ((current_state.depth_bid_5 - current_state.depth_ask_5) / total) if total > 0 else None
        else:
            current_state.depth_imbalance = None
        notional = float(self.execution_settings.quote_allocation_usdt or 0.0)
        buy_slippage = self._estimate_side_slippage_bps(asks, notional, current_state.mark_price)
        sell_slippage = self._estimate_side_slippage_bps(bids, notional, current_state.mark_price)
        slippages = [value for value in (buy_slippage, sell_slippage) if value is not None]
        current_state.estimated_slippage_bps = max(slippages) if slippages else None
        if current_state.estimated_slippage_bps is not None:
            current_state.estimated_slippage_for_order_usdt = notional * (current_state.estimated_slippage_bps / 10000.0)
        else:
            current_state.estimated_slippage_for_order_usdt = None


def parse_args() -> ScoutConfig:
    parser = argparse.ArgumentParser(
        description="Project Phoenix read-only data scout for Binance Alpha x Futures overlap."
    )
    parser.add_argument("--alpha-refresh-sec", type=int, default=60)
    parser.add_argument("--oi-refresh-sec", type=int, default=15)
    parser.add_argument("--taker-refresh-sec", type=int, default=60)
    parser.add_argument("--oi-concurrency", type=int, default=20)
    parser.add_argument("--taker-concurrency", type=int, default=8)
    parser.add_argument("--volume-baseline-points", type=int, default=5)
    parser.add_argument("--volume-spike-ratio", type=float, default=3.0)
    parser.add_argument("--oi-jump-pct", type=float, default=1.0)
    parser.add_argument("--top-n", type=int, default=15)
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument(
        "--snapshot-file",
        type=Path,
        default=Path("phoenix_snapshot.json"),
    )
    args = parser.parse_args()

    return ScoutConfig(
        alpha_refresh_sec=args.alpha_refresh_sec,
        oi_refresh_sec=args.oi_refresh_sec,
        taker_refresh_sec=args.taker_refresh_sec,
        oi_concurrency=args.oi_concurrency,
        taker_concurrency=args.taker_concurrency,
        volume_baseline_points=args.volume_baseline_points,
        volume_spike_ratio=args.volume_spike_ratio,
        oi_jump_pct=args.oi_jump_pct,
        top_n=args.top_n,
        max_symbols=args.max_symbols,
        snapshot_file=args.snapshot_file,
    )


async def async_main() -> int:
    config = parse_args()
    proxy_settings = load_proxy_settings()
    timeout = aiohttp.ClientTimeout(total=45, sock_connect=15, sock_read=30)
    headers = {
        "Accept-Encoding": "identity",
        "User-Agent": USER_AGENT,
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        scout = PhoenixScout(session=session, config=config, proxy_settings=proxy_settings)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, scout.shutdown.set)

        emit_event(
            "scout_boot",
            alpha_refresh_sec=config.alpha_refresh_sec,
            oi_refresh_sec=config.oi_refresh_sec,
            taker_refresh_sec=config.taker_refresh_sec,
            volume_spike_ratio=config.volume_spike_ratio,
            oi_jump_pct=config.oi_jump_pct,
            snapshot_file=str(config.snapshot_file),
        )
        await scout.run()
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        emit_event("scout_stopped", reason="keyboard_interrupt")
        return 130
    except Exception as exc:  # noqa: BLE001
        emit_event("scout_error", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
