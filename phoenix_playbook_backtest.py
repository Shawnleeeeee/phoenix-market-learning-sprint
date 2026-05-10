#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gc
import json
import math
import os
import re
import time
import zipfile
from bisect import bisect_right
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from io import BytesIO, TextIOWrapper
from pathlib import Path
from statistics import fmean, stdev
from typing import Any, Callable, Sequence
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener

from phoenix.signal_lab_events import (
    EventTriggerConfig,
    MarketEventContext,
    build_market_event_context,
    build_triggered_market_event,
    compute_open_interest_context,
)
from phoenix.config import load_execution_settings
try:
    from phoenix_testnet_round_runner import discover_universe, safe_float
except ModuleNotFoundError:
    ASCII_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,12}USDT$")
    LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
    STABLE_ASSETS = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDP", "EUR", "DAI"}

    def safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0


    def is_tradeable_symbol(symbol: str) -> bool:
        normalized = str(symbol or "").upper().strip()
        if not ASCII_SYMBOL_RE.match(normalized):
            return False
        if not normalized.endswith("USDT"):
            return False
        base = normalized[:-4]
        if len(base) < 2 or base in STABLE_ASSETS:
            return False
        if any(base.endswith(suffix) for suffix in LEVERAGED_SUFFIXES):
            return False
        return True


    def discover_universe(
        payload: list[dict[str, Any]],
        *,
        top_limit: int,
        min_quote_volume: float,
        sort_by: str = "quote_volume",
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for item in payload:
            symbol = str(item.get("symbol") or "").upper()
            if not is_tradeable_symbol(symbol):
                continue
            quote_volume = safe_float(item.get("quoteVolume"))
            if quote_volume < min_quote_volume:
                continue
            items.append(
                {
                    "symbol": symbol,
                    "quote_volume_24h": quote_volume,
                    "price_change_24h_pct": safe_float(item.get("priceChangePercent")),
                    "last_price": safe_float(item.get("lastPrice")),
                }
            )
        if sort_by == "positive_change":
            items.sort(key=lambda entry: (entry["price_change_24h_pct"], entry["quote_volume_24h"]), reverse=True)
        elif sort_by == "negative_change":
            items.sort(key=lambda entry: (entry["price_change_24h_pct"], -entry["quote_volume_24h"]))
        elif sort_by == "abs_change":
            items.sort(key=lambda entry: (abs(entry["price_change_24h_pct"]), entry["quote_volume_24h"]), reverse=True)
        else:
            items.sort(key=lambda entry: entry["quote_volume_24h"], reverse=True)
        return items[:top_limit]

try:
    from phoenix_signal_lab import event_context_label, trading_session_label
except ModuleNotFoundError:
    def trading_session_label(timestamp_ms: int) -> str:
        hour = datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc).hour
        if 0 <= hour < 8:
            return "Asia"
        if 8 <= hour < 14:
            return "Europe"
        if 14 <= hour < 22:
            return "US"
        return "offhours"


    def event_context_label(
        record: dict[str, Any],
        *,
        split_by: str,
        horizon: dict[str, Any] | None = None,
    ) -> str:
        if split_by == "none":
            return "all"
        trading_session = str(record.get("trading_session") or "").strip()
        sample_payload = record.get("sample")
        sample_payload = sample_payload if isinstance(sample_payload, dict) else {}
        if not trading_session:
            sample_session = str(sample_payload.get("trading_session") or "").strip()
            if sample_session:
                trading_session = sample_session
            else:
                anchor_close_time_ms = sample_payload.get("anchor_close_time_ms")
                if anchor_close_time_ms not in (None, ""):
                    trading_session = trading_session_label(int(anchor_close_time_ms))
        trading_session = trading_session or "unknown"
        if split_by == "session":
            return trading_session
        enrichments = record.get("enrichments")
        if not isinstance(enrichments, dict) or not enrichments:
            if split_by == "playbook_session":
                return f"{trading_session}|missing"
            return "missing"
        trigger_types = {
            str(token)
            for token in (record.get("trigger_types") or sample_payload.get("trigger_types") or [])
            if str(token)
        }
        sample_type = str(record.get("sample_type") or sample_payload.get("sample_type") or "")
        bar_interval = str(record.get("bar_interval") or sample_payload.get("bar_interval") or "")

        def enrich_float(key: str) -> float | None:
            value = enrichments.get(key)
            if value in (None, ""):
                return None
            return safe_float(value)

        def horizon_float(key: str) -> float | None:
            if not isinstance(horizon, dict):
                return None
            value = horizon.get(key)
            if value in (None, ""):
                return None
            return safe_float(value)

        def sample_bool(key: str) -> bool:
            value = sample_payload.get(key)
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return value != 0
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "y", "on"}
            return False

        def has_right_side_reversal_confirmation() -> bool:
            trigger_direction = str(
                sample_payload.get("trigger_candle_direction")
                or sample_payload.get("candle_direction")
                or ""
            ).strip().lower()
            confirmation_direction = str(
                sample_payload.get("confirmation_candle_direction") or ""
            ).strip().lower()
            if not sample_bool("reversal_confirmation_passed"):
                return False
            return (
                (trigger_direction == "down" and confirmation_direction == "up")
                or (trigger_direction == "up" and confirmation_direction == "down")
            )

        def btc_regime_allows_reversal(*, trigger_direction: str) -> bool:
            btc_ret_5m_pct = enrich_float("btcusdt_ret_5m_pct")
            if btc_ret_5m_pct is None:
                return False
            normalized_direction = str(trigger_direction or "").strip().lower()
            if normalized_direction == "down":
                return btc_ret_5m_pct > (-BTC_REVERSAL_REGIME_BLOCK_PCT)
            if normalized_direction == "up":
                return btc_ret_5m_pct < BTC_REVERSAL_REGIME_BLOCK_PCT
            return False

        def oi_bucket() -> str:
            oi_5m = enrich_float("oi_change_5m_pct")
            oi_15m = enrich_float("oi_change_15m_pct")
            if oi_5m is None and oi_15m is None:
                return "missing"
            oi_5m = oi_5m if oi_5m is not None else 0.0
            oi_15m = oi_15m if oi_15m is not None else 0.0
            if oi_5m <= -0.35 or oi_15m <= -0.75:
                return "oi_flush"
            if oi_5m >= 0.50 or oi_15m >= 1.00:
                return "oi_build"
            return "oi_flat"

        def playbook_bucket() -> str:
            if sample_type == "baseline":
                return "baseline_control"
            oi_state = oi_bucket()
            depth_imbalance = enrich_float("depth_imbalance")
            max_drawdown_pct = horizon_float("max_drawdown_pct")
            trigger_candle_direction = str(
                sample_payload.get("trigger_candle_direction")
                or sample_payload.get("candle_direction")
                or ""
            ).strip().lower()
            has_expansion_shape = "body_expansion" in trigger_types or "range_expansion" in trigger_types
            has_breakout_shape = "volume_burst" in trigger_types and has_expansion_shape
            if bar_interval == "5m" and has_breakout_shape and oi_state == "oi_build":
                return "oi_build_breakout"
            if (
                oi_state == "oi_flush"
                and has_expansion_shape
                and depth_imbalance is not None
                and depth_imbalance >= 0.12
                and trigger_candle_direction == "down"
                and (max_drawdown_pct is None or max_drawdown_pct <= -0.75)
            ):
                return "liquidation_flush"
            return "other_trigger"

        if split_by == "oi":
            return oi_bucket()
        if split_by == "playbook":
            return playbook_bucket()
        if split_by == "playbook_session":
            return f"{trading_session}|{playbook_bucket()}"
        raise ValueError(f"Unsupported event context split: {split_by}")

try:
    from phoenix_signal_bridge import derive_signal_side
except ModuleNotFoundError:
    def derive_signal_side(record: dict[str, Any], *, playbook: str | None = None) -> str | None:
        sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
        candle_direction = str(sample.get("candle_direction") or "").lower()
        playbook_name = str(playbook or record.get("playbook") or "")
        if playbook_name == "liquidation_flush":
            if candle_direction == "down":
                return "BUY"
            if candle_direction == "up":
                return "SELL"
            return None
        if candle_direction == "up":
            return "BUY"
        if candle_direction == "down":
            return "SELL"
        return None

BAR_MS_5M = 300_000
FAPI_BASE_URL = "https://fapi.binance.com"
BINANCE_PUBLIC_DATA_BASE_URL = "https://data.binance.vision"
USER_AGENT = "Phoenix-Playbook-Backtest/1.0"
BTC_REVERSAL_REGIME_BLOCK_PCT = 0.35
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ALLOWED_PLAYBOOKS = (
    "oi_build_breakout",
    "liquidation_flush",
)
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "signal_lab_replay" / "playbook_backtest_14d"
DEFAULT_PUBLIC_ZIP_CACHE_DIR = PROJECT_ROOT / "signal_lab_replay" / "binance_public_zip_cache"
DEFAULT_HISTORY_CHUNK_HOURS = 6
DEFAULT_PLAYBOOK_CLASSIFIER_MODE = "proxy"
DEFAULT_MAX_LOAD_AVERAGE = 1.5
DEFAULT_LOAD_CHECK_SLEEP_SEC = 15.0
DEFAULT_PROCESS_NICE_VALUE = 19
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]
INTERVAL_TO_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}

try:
    from btc_engine.market_data.backfill import BinancePublicBackfillClient, _resolve_proxy
except ModuleNotFoundError:
    def _resolve_proxy() -> str | None:
        for key in ("BTC_ALL_PROXY", "ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
            value = os.environ.get(key)
            if value:
                return value
        return None


    def _dedupe_sort(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
        deduped: dict[Any, dict[str, Any]] = {}
        for row in rows:
            deduped[row[key]] = row
        return [deduped[item] for item in sorted(deduped)]


    class BinancePublicBackfillClient:
        def __init__(self, *, proxy: str | None, timeout_sec: int, pause_sec: float, max_retries: int) -> None:
            self.timeout_sec = timeout_sec
            self.pause_sec = pause_sec
            self.max_retries = max_retries
            proxies = {"http": proxy, "https": proxy} if proxy else {}
            self.opener = build_opener(ProxyHandler(proxies))
            self.opener.addheaders = [("User-Agent", USER_AGENT)]

        def _get_json(self, path: str, params: dict[str, Any]) -> Any:
            url = f"{FAPI_BASE_URL}{path}?{urlencode(params)}"
            last_error: Exception | None = None
            for attempt in range(1, self.max_retries + 1):
                try:
                    with self.opener.open(url, timeout=self.timeout_sec) as response:
                        return json.loads(response.read().decode("utf-8"))
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    if attempt >= self.max_retries:
                        raise
                    time.sleep(min(1.0 * attempt, 3.0))
            if last_error is not None:
                raise last_error
            raise RuntimeError(f"Unexpected request failure: {url}")

        def fetch_klines(self, *, symbol: str, interval: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
            rows: list[dict[str, Any]] = []
            cursor = start_ms
            interval_ms = INTERVAL_TO_MS[interval]
            while cursor < end_ms:
                payload = self._get_json(
                    "/fapi/v1/klines",
                    {
                        "symbol": symbol,
                        "interval": interval,
                        "startTime": cursor,
                        "endTime": end_ms,
                        "limit": 1500,
                    },
                )
                if not payload:
                    break
                for raw in payload:
                    row = dict(zip(KLINE_COLUMNS, raw, strict=False))
                    rows.append(row)
                cursor = int(payload[-1][0]) + interval_ms
                time.sleep(self.pause_sec)
            return _dedupe_sort(rows, "open_time")

        def fetch_open_interest_hist(self, *, symbol: str, period: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
            rows: list[dict[str, Any]] = []
            cursor = start_ms
            period_ms = INTERVAL_TO_MS[period]
            chunk_ms = period_ms * 500
            while cursor < end_ms:
                chunk_end_ms = min(cursor + chunk_ms - 1, end_ms)
                payload = self._get_json(
                    "/futures/data/openInterestHist",
                    {
                        "symbol": symbol,
                        "period": period,
                        "startTime": cursor,
                        "endTime": chunk_end_ms,
                        "limit": 500,
                    },
                )
                if payload:
                    rows.extend(payload)
                cursor = chunk_end_ms + 1
                time.sleep(self.pause_sec)
            return _dedupe_sort(rows, "timestamp")


@dataclass(slots=True)
class SymbolRules:
    symbol: str
    tick_size: float
    step_size: float
    min_qty: float


@dataclass(slots=True)
class TradeLevels:
    entry_price: float
    quantity: float
    notional_usdt: float
    margin_usdt: float
    stop_price: float
    take_profit_price: float | None
    breakeven_trigger_price: float
    breakeven_stop_price: float
    trailing_callback_pct: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest current v7 playbook logic over Binance 14-day 5m historical futures data."
    )
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--start-date", default="", help="UTC start date YYYY-MM-DD. Overrides --days when set.")
    parser.add_argument("--end-date", default="", help="UTC inclusive end date YYYY-MM-DD for date-window backtests.")
    parser.add_argument("--interval", default="5m", choices=["5m"])
    parser.add_argument("--symbols", default="", help="Comma-separated symbols. When set, skips universe discovery.")
    parser.add_argument("--universe-top", type=int, default=150)
    parser.add_argument("--min-quote-volume", type=float, default=5_000_000.0)
    parser.add_argument("--exchange-info-json", type=Path, default=None)
    parser.add_argument("--ticker-24hr-json", type=Path, default=None)
    parser.add_argument("--quote-allocation", type=float, default=500.0)
    parser.add_argument("--leverage", type=int, default=10)
    parser.add_argument("--stop-loss-pct", type=float, default=1.0)
    parser.add_argument("--take-profit-pct", type=float, default=2.5)
    parser.add_argument("--breakeven-trigger-pct", type=float, default=0.8)
    parser.add_argument("--breakeven-lock-pct", type=float, default=0.1)
    parser.add_argument("--trailing-callback-pct", type=float, default=0.5)
    parser.add_argument("--round-trip-fee-bps", type=float, default=8.0)
    parser.add_argument("--allowed-playbooks", default=",".join(DEFAULT_ALLOWED_PLAYBOOKS))
    parser.add_argument(
        "--playbook-classifier",
        default=DEFAULT_PLAYBOOK_CLASSIFIER_MODE,
        choices=["strict", "proxy"],
    )
    parser.add_argument("--starting-equity", type=float, default=10_000.0)
    parser.add_argument("--portfolio-max-open-positions", type=int, default=10)
    parser.add_argument("--workers-note", default="sequential_fetch_with_public_backfill_client")
    parser.add_argument("--scan-stop-losses", default="")
    parser.add_argument("--scan-take-profits", default="")
    parser.add_argument("--data-source", default="public-zip", choices=["public-zip", "rest"])
    parser.add_argument("--public-zip-cache-dir", type=Path, default=DEFAULT_PUBLIC_ZIP_CACHE_DIR)
    parser.add_argument("--offline-cache-only", action="store_true")
    parser.add_argument("--skip-open-interest", action="store_true", help="Smoke-test mode only; OI-dependent playbooks will not trigger.")
    parser.add_argument(
        "--backtest-report-json",
        type=Path,
        default=None,
        help="Promotion-gate compatible report path. Defaults to output-dir/backtest_report.json.",
    )
    parser.add_argument("--history-chunk-hours", type=int, default=DEFAULT_HISTORY_CHUNK_HOURS)
    parser.add_argument("--max-load-average", type=float, default=DEFAULT_MAX_LOAD_AVERAGE)
    parser.add_argument("--load-check-sleep-sec", type=float, default=DEFAULT_LOAD_CHECK_SLEEP_SEC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return now_utc().isoformat(timespec="milliseconds")


def round4(value: float) -> float:
    return round(float(value), 4)


def emit_event(name: str, **payload: Any) -> None:
    print(json.dumps({"ts": utc_now_iso(), "event": name, **payload}, ensure_ascii=False), flush=True)


def dedupe_sort_rows(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    deduped: dict[Any, dict[str, Any]] = {}
    for row in rows:
        if key in row:
            deduped[row[key]] = row
    return [deduped[item] for item in sorted(deduped)]


def parse_allowed_playbooks(raw_value: str) -> set[str]:
    allowed = {token.strip() for token in str(raw_value or "").split(",") if token.strip()}
    return allowed or set(DEFAULT_ALLOWED_PLAYBOOKS)


def parse_float_grid(raw_value: str) -> list[float]:
    values: list[float] = []
    for part in str(raw_value or "").split(","):
        token = part.strip()
        if not token:
            continue
        number = float(token)
        if number <= 0:
            raise ValueError(f"Grid values must be positive: {token}")
        values.append(round(float(number), 6))
    return sorted(set(values))


def parse_symbol_list(raw_value: str) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for part in str(raw_value or "").replace("\n", ",").split(","):
        token = part.strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        symbols.append(token)
    return symbols


def parse_utc_date(raw_value: str) -> date | None:
    token = str(raw_value or "").strip()
    if not token:
        return None
    return date.fromisoformat(token)


def read_json_payload(path: Path | None) -> Any | None:
    if path is None:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def utc_day_start_ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)


def utc_dates_overlapping_ms(start_ms: int, end_ms: int) -> list[date]:
    if end_ms <= start_ms:
        return []
    cursor_dt = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).date()
    last_dt = datetime.fromtimestamp((end_ms - 1) / 1000.0, tz=timezone.utc).date()
    days: list[date] = []
    while cursor_dt <= last_dt:
        days.append(cursor_dt)
        cursor_dt += timedelta(days=1)
    return days


def build_public_kline_zip_url(
    *,
    symbol: str,
    interval: str,
    day: date,
    base_url: str = BINANCE_PUBLIC_DATA_BASE_URL,
) -> str:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_interval = str(interval or "").strip()
    if not normalized_symbol or not normalized_interval:
        raise ValueError("symbol and interval are required")
    suffix = day.isoformat()
    return (
        f"{str(base_url).rstrip('/')}/data/futures/um/daily/klines/"
        f"{normalized_symbol}/{normalized_interval}/{normalized_symbol}-{normalized_interval}-{suffix}.zip"
    )


def normalize_public_kline_row(row: list[str], *, symbol: str) -> dict[str, Any] | None:
    if len(row) < len(KLINE_COLUMNS):
        return None
    first = str(row[0]).strip().lower()
    if first in {"open_time", "open time"}:
        return None
    values = dict(zip(KLINE_COLUMNS, row[: len(KLINE_COLUMNS)], strict=False))
    try:
        int(values["open_time"])
        int(values["close_time"])
    except (TypeError, ValueError):
        return None
    return values


def iter_public_kline_rows_from_zip_bytes(payload: bytes, *, symbol: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with zipfile.ZipFile(BytesIO(payload)) as archive:
        for name in archive.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with archive.open(name, "r") as raw_handle:
                text_handle = TextIOWrapper(raw_handle, encoding="utf-8")
                for csv_row in csv.reader(text_handle):
                    row = normalize_public_kline_row(csv_row, symbol=symbol)
                    if row is not None:
                        rows.append(row)
    return rows


class BinancePublicZipBackfillClient:
    """Use Binance public zip files for local kline backtests, with REST only for metadata/OI."""

    def __init__(
        self,
        *,
        proxy: str | None,
        timeout_sec: int,
        pause_sec: float,
        max_retries: int,
        cache_dir: Path,
        offline_cache_only: bool = False,
        public_data_base_url: str = BINANCE_PUBLIC_DATA_BASE_URL,
    ) -> None:
        self.timeout_sec = timeout_sec
        self.pause_sec = pause_sec
        self.max_retries = max(1, int(max_retries))
        self.cache_dir = Path(cache_dir)
        self.offline_cache_only = bool(offline_cache_only)
        self.public_data_base_url = str(public_data_base_url).rstrip("/")
        self.rest_client = BinancePublicBackfillClient(
            proxy=proxy,
            timeout_sec=timeout_sec,
            pause_sec=pause_sec,
            max_retries=max_retries,
        )
        proxies = {"http": proxy, "https": proxy} if proxy else {}
        self.opener = build_opener(ProxyHandler(proxies))
        self.opener.addheaders = [("User-Agent", USER_AGENT)]

    def _get_json(self, path: str, params: dict[str, Any]) -> Any:
        return self.rest_client._get_json(path, params)

    def _cache_path(self, *, symbol: str, interval: str, day: date) -> Path:
        return (
            self.cache_dir
            / "futures"
            / "um"
            / "daily"
            / "klines"
            / str(symbol).upper()
            / str(interval)
            / f"{str(symbol).upper()}-{interval}-{day.isoformat()}.zip"
        )

    def _download_zip_if_needed(self, *, symbol: str, interval: str, day: date) -> Path | None:
        cache_path = self._cache_path(symbol=symbol, interval=interval, day=day)
        if cache_path.exists() and cache_path.stat().st_size > 0:
            return cache_path
        if self.offline_cache_only:
            return None
        url = build_public_kline_zip_url(
            symbol=symbol,
            interval=interval,
            day=day,
            base_url=self.public_data_base_url,
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with self.opener.open(url, timeout=self.timeout_sec) as response:
                    cache_path.write_bytes(response.read())
                time.sleep(self.pause_sec)
                return cache_path
            except HTTPError as exc:
                if exc.code == 404:
                    return None
                last_error = exc
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            if attempt < self.max_retries:
                time.sleep(min(1.0 * attempt, 3.0))
        if last_error is not None:
            raise last_error
        return None

    def fetch_klines(self, *, symbol: str, interval: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for day in utc_dates_overlapping_ms(start_ms, end_ms):
            zip_path = self._download_zip_if_needed(symbol=symbol, interval=interval, day=day)
            if zip_path is None:
                continue
            for row in iter_public_kline_rows_from_zip_bytes(zip_path.read_bytes(), symbol=symbol):
                open_time = int(row["open_time"])
                if int(start_ms) <= open_time < int(end_ms):
                    rows.append(row)
        return dedupe_sort_rows(rows, "open_time")

    def fetch_open_interest_hist(self, *, symbol: str, period: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        return self.rest_client.fetch_open_interest_hist(
            symbol=symbol,
            period=period,
            start_ms=start_ms,
            end_ms=end_ms,
        )


def choose_playbook_classifier(mode: str) -> Callable[[dict[str, Any]], str]:
    normalized = str(mode or DEFAULT_PLAYBOOK_CLASSIFIER_MODE).strip().lower()
    if normalized == "strict":
        return classify_playbook_record
    return classify_playbook_record_with_proxies


def set_process_nice_value(
    target_value: int = DEFAULT_PROCESS_NICE_VALUE,
    *,
    set_priority_func: Callable[[Any, int, int], Any] | None = None,
    nice_func: Callable[[int], Any] | None = None,
) -> bool:
    target = max(0, int(target_value))
    if target <= 0:
        return False
    if set_priority_func is None and hasattr(os, "setpriority") and hasattr(os, "PRIO_PROCESS"):
        set_priority_func = os.setpriority
    if set_priority_func is not None:
        try:
            set_priority_func(os.PRIO_PROCESS, 0, target)
            return True
        except (AttributeError, OSError, PermissionError):
            pass
    if nice_func is None:
        nice_func = getattr(os, "nice", None)
    if nice_func is None:
        return False
    native_os_nice = getattr(os, "nice", None)
    try:
        if native_os_nice is not None and nice_func is native_os_nice:
            current_nice = int(native_os_nice(0))
            increment = max(0, target - current_nice)
            if increment > 0:
                native_os_nice(increment)
            return True
        nice_func(target)
        return True
    except (AttributeError, OSError, PermissionError, TypeError, ValueError):
        return False


def wait_for_safe_system_load(
    *,
    max_load_average: float,
    sleep_seconds: float,
    get_load_average: Callable[[], float] | None = None,
    sleep_func: Callable[[float], Any] = time.sleep,
) -> int:
    threshold = float(max_load_average or 0.0)
    if threshold <= 0:
        return 0
    if get_load_average is None:
        if not hasattr(os, "getloadavg"):
            return 0
        get_load_average = lambda: float(os.getloadavg()[0])
    delay = max(0.01, float(sleep_seconds or 0.0))
    pause_count = 0
    while True:
        try:
            load_average = float(get_load_average())
        except (AttributeError, OSError, ValueError):
            return pause_count
        if load_average <= threshold:
            return pause_count
        pause_count += 1
        emit_event(
            "playbook_backtest_load_pause",
            load_average=round4(load_average),
            max_load_average=round4(threshold),
            sleep_seconds=round4(delay),
        )
        sleep_func(delay)


def build_history_chunks(start_ms: int, end_ms: int, *, chunk_hours: int) -> list[tuple[int, int]]:
    chunk_ms = max(1, int(chunk_hours)) * 60 * 60 * 1000
    chunks: list[tuple[int, int]] = []
    cursor = int(start_ms)
    boundary = int(end_ms)
    while cursor < boundary:
        chunk_end = min(cursor + chunk_ms, boundary)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return chunks


def pct_change(current: float, past: float) -> float:
    if current <= 0 or past <= 0:
        return 0.0
    return ((current / past) - 1.0) * 100.0


def side_return_pct(*, side: str, entry_price: float, exit_price: float) -> float:
    if str(side).upper() == "BUY":
        return pct_change(exit_price, entry_price)
    return pct_change(entry_price, exit_price)


def iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def round_down_to_step(value: float, step: float) -> float:
    if value <= 0 or step <= 0:
        return 0.0
    decimal_step = Decimal(str(step))
    # Nudge by a tiny fraction of the step to absorb binary float underflow
    # like 102.49999999999999 when the intended value is exactly 102.5.
    decimal_value = Decimal(str(value)) + (decimal_step / Decimal("1000000"))
    return float((decimal_value / decimal_step).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_step)


def round_price(value: float, tick_size: float) -> float:
    return round_down_to_step(value, tick_size)


def build_historical_ticker_item(
    symbol: str,
    candles_5m: Sequence[Sequence[Any]],
    *,
    current_index: int,
    rolling_bars: int = 288,
) -> dict[str, Any] | None:
    if current_index < rolling_bars:
        return None
    current_close = safe_float(candles_5m[current_index][4])
    prior_close = safe_float(candles_5m[current_index - rolling_bars][4])
    if current_close <= 0 or prior_close <= 0:
        return None
    quote_volume_24h = sum(max(0.0, safe_float(row[7])) for row in candles_5m[current_index - rolling_bars + 1 : current_index + 1])
    if quote_volume_24h <= 0:
        return None
    return {
        "symbol": symbol,
        "quoteVolume": quote_volume_24h,
        "priceChangePercent": pct_change(current_close, prior_close),
        "lastPrice": current_close,
    }


def classify_playbook_record(record: dict[str, Any]) -> str:
    return event_context_label(record, split_by="playbook")


def _record_oi_bucket(record: dict[str, Any]) -> str:
    enrichments = record.get("enrichments") if isinstance(record.get("enrichments"), dict) else {}
    oi_5m = safe_float(enrichments.get("oi_change_5m_pct"))
    oi_15m = safe_float(enrichments.get("oi_change_15m_pct"))
    if oi_5m is None and oi_15m is None:
        return "missing"
    oi_5m = oi_5m if oi_5m is not None else 0.0
    oi_15m = oi_15m if oi_15m is not None else 0.0
    if oi_5m <= -0.35 or oi_15m <= -0.75:
        return "oi_flush"
    if oi_5m >= 0.50 or oi_15m >= 1.00:
        return "oi_build"
    return "oi_flat"


def build_proxy_candle_metrics(candle: Sequence[Any]) -> dict[str, float]:
    open_price = safe_float(candle[1])
    high_price = safe_float(candle[2])
    low_price = safe_float(candle[3])
    close_price = safe_float(candle[4])
    candle_range = max(0.0, high_price - low_price)
    if candle_range <= 0:
        return {
            "lower_wick_ratio": 0.0,
            "upper_wick_ratio": 0.0,
        }
    upper_wick = max(0.0, high_price - max(open_price, close_price))
    lower_wick = max(0.0, min(open_price, close_price) - low_price)
    return {
        "lower_wick_ratio": lower_wick / candle_range,
        "upper_wick_ratio": upper_wick / candle_range,
    }


def classify_playbook_record_with_proxies(record: dict[str, Any]) -> str:
    strict_playbook = classify_playbook_record(record)
    if strict_playbook != "other_trigger":
        return strict_playbook

    sample_payload = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    trigger_types = {
        str(token)
        for token in (record.get("trigger_types") or sample_payload.get("trigger_types") or [])
        if str(token)
    }
    candle_direction = str(sample_payload.get("candle_direction") or "").lower()
    bar_interval = str(record.get("bar_interval") or sample_payload.get("bar_interval") or "")
    oi_state = _record_oi_bucket(record)
    lower_wick_ratio = safe_float(sample_payload.get("lower_wick_ratio")) or 0.0
    upper_wick_ratio = safe_float(sample_payload.get("upper_wick_ratio")) or 0.0
    volume_burst_ratio = safe_float(sample_payload.get("volume_burst_ratio")) or 0.0
    range_to_atr = safe_float(sample_payload.get("range_to_atr")) or 0.0
    has_expansion_shape = "body_expansion" in trigger_types or "range_expansion" in trigger_types

    if (
        bar_interval == "5m"
        and "volume_burst" in trigger_types
        and has_expansion_shape
        and oi_state == "oi_build"
    ):
        return "oi_build_breakout"
    if (
        oi_state == "oi_flush"
        and has_expansion_shape
        and candle_direction == "down"
        and lower_wick_ratio >= 0.35
        and volume_burst_ratio >= 2.5
        and range_to_atr >= 1.8
    ):
        return "liquidation_flush"
    return strict_playbook


def build_symbol_rules_lookup(exchange_info_payload: dict[str, Any]) -> dict[str, SymbolRules]:
    lookup: dict[str, SymbolRules] = {}
    for item in exchange_info_payload.get("symbols", []):
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        filters = item.get("filters")
        if not symbol or not isinstance(filters, list):
            continue
        price_filter = next((row for row in filters if row.get("filterType") == "PRICE_FILTER"), None)
        lot_filter = next((row for row in filters if row.get("filterType") == "LOT_SIZE"), None)
        if not isinstance(price_filter, dict) or not isinstance(lot_filter, dict):
            continue
        lookup[symbol] = SymbolRules(
            symbol=symbol,
            tick_size=safe_float(price_filter.get("tickSize")),
            step_size=safe_float(lot_filter.get("stepSize")),
            min_qty=safe_float(lot_filter.get("minQty")),
        )
    return lookup


def build_trade_levels(
    *,
    entry_price: float,
    side: str,
    rules: SymbolRules,
    quote_allocation_usdt: float,
    leverage: int,
    stop_loss_pct: float,
    take_profit_pct: float,
    breakeven_trigger_pct: float,
    breakeven_lock_pct: float,
    trailing_callback_pct: float,
) -> TradeLevels | None:
    if entry_price <= 0 or leverage <= 0 or rules.tick_size <= 0 or rules.step_size <= 0:
        return None
    raw_quantity = (quote_allocation_usdt * leverage) / entry_price
    quantity = round_down_to_step(raw_quantity, rules.step_size)
    if quantity <= 0 or quantity < rules.min_qty:
        return None
    notional_usdt = quantity * entry_price
    margin_usdt = notional_usdt / max(1, leverage)
    side = str(side).upper()
    if side == "BUY":
        stop_reference = entry_price * (1.0 - stop_loss_pct / 100.0)
        take_reference = entry_price * (1.0 + take_profit_pct / 100.0) if take_profit_pct > 0 else None
        breakeven_trigger_reference = entry_price * (1.0 + breakeven_trigger_pct / 100.0)
        breakeven_stop_reference = entry_price * (1.0 + breakeven_lock_pct / 100.0)
    else:
        stop_reference = entry_price * (1.0 + stop_loss_pct / 100.0)
        take_reference = entry_price * (1.0 - take_profit_pct / 100.0) if take_profit_pct > 0 else None
        breakeven_trigger_reference = entry_price * (1.0 - breakeven_trigger_pct / 100.0)
        breakeven_stop_reference = entry_price * (1.0 - breakeven_lock_pct / 100.0)
    return TradeLevels(
        entry_price=entry_price,
        quantity=quantity,
        notional_usdt=notional_usdt,
        margin_usdt=margin_usdt,
        stop_price=round_price(stop_reference, rules.tick_size),
        take_profit_price=round_price(take_reference, rules.tick_size) if take_reference is not None else None,
        breakeven_trigger_price=round_price(breakeven_trigger_reference, rules.tick_size),
        breakeven_stop_price=round_price(breakeven_stop_reference, rules.tick_size),
        trailing_callback_pct=max(0.0, float(trailing_callback_pct or 0.0)),
    )


def _path_points_for_candle(candle: Sequence[Any]) -> list[float]:
    open_price = safe_float(candle[1])
    high_price = safe_float(candle[2])
    low_price = safe_float(candle[3])
    close_price = safe_float(candle[4])
    if close_price >= open_price:
        return [open_price, low_price, high_price, close_price]
    return [open_price, high_price, low_price, close_price]


def _long_trailing_stop(extreme_price: float, callback_pct: float) -> float:
    return extreme_price * (1.0 - callback_pct / 100.0)


def _short_trailing_stop(extreme_price: float, callback_pct: float) -> float:
    return extreme_price * (1.0 + callback_pct / 100.0)


def simulate_trade_path(
    *,
    side: str,
    entry_price: float,
    future_candles: Sequence[Sequence[Any]],
    stop_loss_pct: float,
    take_profit_pct: float,
    breakeven_trigger_pct: float,
    breakeven_lock_pct: float,
    trailing_callback_pct: float,
    round_trip_fee_bps: float,
    notional_usdt: float,
    margin_usdt: float | None = None,
    tick_size: float = 0.0001,
) -> dict[str, Any]:
    side = str(side).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported side: {side}")
    if not future_candles:
        raise ValueError("future_candles must not be empty")

    if side == "BUY":
        initial_stop = round_price(entry_price * (1.0 - stop_loss_pct / 100.0), tick_size)
        take_profit = round_price(entry_price * (1.0 + take_profit_pct / 100.0), tick_size) if take_profit_pct > 0 else None
        breakeven_trigger = round_price(entry_price * (1.0 + breakeven_trigger_pct / 100.0), tick_size)
        breakeven_stop = round_price(entry_price * (1.0 + breakeven_lock_pct / 100.0), tick_size)
    else:
        initial_stop = round_price(entry_price * (1.0 + stop_loss_pct / 100.0), tick_size)
        take_profit = round_price(entry_price * (1.0 - take_profit_pct / 100.0), tick_size) if take_profit_pct > 0 else None
        breakeven_trigger = round_price(entry_price * (1.0 - breakeven_trigger_pct / 100.0), tick_size)
        breakeven_stop = round_price(entry_price * (1.0 - breakeven_lock_pct / 100.0), tick_size)

    phase = "INITIAL"
    extreme_price = entry_price
    current_stop = initial_stop
    trailing_activation_price: float | None = None
    observed_high = entry_price
    observed_low = entry_price
    hit_breakeven = False
    hit_trailing = False
    effective_margin_usdt = (
        float(margin_usdt)
        if margin_usdt is not None and float(margin_usdt) > 0
        else max(1e-9, float(notional_usdt))
    )

    def build_result(*, exit_reason: str, exit_price: float, exit_time_ms: int, holding_bars: int) -> dict[str, Any]:
        gross_return_pct = side_return_pct(side=side, entry_price=entry_price, exit_price=exit_price)
        fee_pct = max(0.0, float(round_trip_fee_bps or 0.0)) / 100.0
        after_fee_return_pct = gross_return_pct - fee_pct
        pnl_usdt = notional_usdt * (after_fee_return_pct / 100.0)
        margin_return_pct = (pnl_usdt / effective_margin_usdt) * 100.0
        return {
            "exit_reason": exit_reason,
            "exit_price": round4(exit_price),
            "exit_time_ms": int(exit_time_ms),
            "exit_time": iso_from_ms(int(exit_time_ms)),
            "holding_bars": int(holding_bars),
            "gross_return_pct": round4(gross_return_pct),
            "after_fee_return_pct": round4(after_fee_return_pct),
            "pnl_usdt": round4(pnl_usdt),
            "margin_return_pct": round4(margin_return_pct),
            "max_runup_pct": round4(side_return_pct(side=side, entry_price=entry_price, exit_price=observed_high if side == "BUY" else observed_low)),
            "max_drawdown_pct": round4(side_return_pct(side=side, entry_price=entry_price, exit_price=observed_low if side == "BUY" else observed_high)),
            "hit_breakeven": hit_breakeven,
            "hit_trailing": hit_trailing,
        }

    for candle_index, candle in enumerate(future_candles, start=1):
        candle_open = safe_float(candle[1])
        candle_high = safe_float(candle[2])
        candle_low = safe_float(candle[3])
        candle_close = safe_float(candle[4])
        candle_close_ms = int(candle[6])
        observed_high = max(observed_high, candle_high)
        observed_low = min(observed_low, candle_low)

        if phase == "TRAILING":
            if side == "BUY":
                current_stop = max(current_stop, _long_trailing_stop(extreme_price, trailing_callback_pct))
            else:
                current_stop = min(current_stop, _short_trailing_stop(extreme_price, trailing_callback_pct))

        if side == "BUY":
            if candle_open <= current_stop:
                reason = "trailing_stop" if phase == "TRAILING" and current_stop > breakeven_stop else ("breakeven_stop" if phase != "INITIAL" else "stop_loss")
                return build_result(exit_reason=reason, exit_price=candle_open, exit_time_ms=candle_close_ms, holding_bars=candle_index)
            if take_profit is not None and candle_open >= take_profit:
                return build_result(exit_reason="take_profit", exit_price=candle_open, exit_time_ms=candle_close_ms, holding_bars=candle_index)
            if phase == "INITIAL" and candle_open >= breakeven_trigger:
                hit_breakeven = True
                phase = "BREAKEVEN_ONLY"
                current_stop = breakeven_stop
                trigger_price = candle_open
                trailing_activation_price = round_price(trigger_price * 1.001, tick_size) if trailing_callback_pct > 0 else None
            points = _path_points_for_candle(candle)
            for start_price, end_price in zip(points, points[1:]):
                if end_price > start_price:
                    cursor_price = start_price
                    if phase == "INITIAL" and start_price < breakeven_trigger <= end_price:
                        hit_breakeven = True
                        phase = "BREAKEVEN_ONLY"
                        current_stop = breakeven_stop
                        trigger_price = breakeven_trigger
                        trailing_activation_price = round_price(trigger_price * 1.001, tick_size) if trailing_callback_pct > 0 else None
                        cursor_price = trigger_price
                    extreme_price = max(extreme_price, end_price)
                    if phase == "BREAKEVEN_ONLY" and trailing_activation_price is not None:
                        if cursor_price >= trailing_activation_price or (cursor_price < trailing_activation_price <= end_price):
                            phase = "TRAILING"
                            hit_trailing = True
                            cursor_price = max(cursor_price, trailing_activation_price)
                            current_stop = max(current_stop, _long_trailing_stop(extreme_price, trailing_callback_pct))
                    if take_profit is not None and cursor_price <= take_profit <= end_price:
                        return build_result(exit_reason="take_profit", exit_price=take_profit, exit_time_ms=candle_close_ms, holding_bars=candle_index)
                elif end_price < start_price:
                    effective_stop = current_stop
                    if phase == "TRAILING":
                        effective_stop = max(effective_stop, _long_trailing_stop(extreme_price, trailing_callback_pct))
                    if end_price <= effective_stop <= start_price:
                        reason = "trailing_stop" if phase == "TRAILING" and effective_stop > breakeven_stop else ("breakeven_stop" if phase != "INITIAL" else "stop_loss")
                        return build_result(exit_reason=reason, exit_price=effective_stop, exit_time_ms=candle_close_ms, holding_bars=candle_index)
        else:
            if candle_open >= current_stop:
                reason = "trailing_stop" if phase == "TRAILING" and current_stop < breakeven_stop else ("breakeven_stop" if phase != "INITIAL" else "stop_loss")
                return build_result(exit_reason=reason, exit_price=candle_open, exit_time_ms=candle_close_ms, holding_bars=candle_index)
            if take_profit is not None and candle_open <= take_profit:
                return build_result(exit_reason="take_profit", exit_price=candle_open, exit_time_ms=candle_close_ms, holding_bars=candle_index)
            if phase == "INITIAL" and candle_open <= breakeven_trigger:
                hit_breakeven = True
                phase = "BREAKEVEN_ONLY"
                current_stop = breakeven_stop
                trigger_price = candle_open
                trailing_activation_price = round_price(trigger_price * 0.999, tick_size) if trailing_callback_pct > 0 else None
            points = _path_points_for_candle(candle)
            for start_price, end_price in zip(points, points[1:]):
                if end_price < start_price:
                    cursor_price = start_price
                    if phase == "INITIAL" and end_price <= breakeven_trigger <= start_price:
                        hit_breakeven = True
                        phase = "BREAKEVEN_ONLY"
                        current_stop = breakeven_stop
                        trigger_price = breakeven_trigger
                        trailing_activation_price = round_price(trigger_price * 0.999, tick_size) if trailing_callback_pct > 0 else None
                        cursor_price = trigger_price
                    extreme_price = min(extreme_price, end_price)
                    if phase == "BREAKEVEN_ONLY" and trailing_activation_price is not None:
                        if cursor_price <= trailing_activation_price or (end_price <= trailing_activation_price < cursor_price):
                            phase = "TRAILING"
                            hit_trailing = True
                            cursor_price = min(cursor_price, trailing_activation_price)
                            current_stop = min(current_stop, _short_trailing_stop(extreme_price, trailing_callback_pct))
                    if take_profit is not None and end_price <= take_profit <= cursor_price:
                        return build_result(exit_reason="take_profit", exit_price=take_profit, exit_time_ms=candle_close_ms, holding_bars=candle_index)
                elif end_price > start_price:
                    effective_stop = current_stop
                    if phase == "TRAILING":
                        effective_stop = min(effective_stop, _short_trailing_stop(extreme_price, trailing_callback_pct))
                    if start_price <= effective_stop <= end_price:
                        reason = "trailing_stop" if phase == "TRAILING" and effective_stop < breakeven_stop else ("breakeven_stop" if phase != "INITIAL" else "stop_loss")
                        return build_result(exit_reason=reason, exit_price=effective_stop, exit_time_ms=candle_close_ms, holding_bars=candle_index)

        if candle_close > 0:
            extreme_price = max(extreme_price, candle_close) if side == "BUY" else min(extreme_price, candle_close)

    final_close = safe_float(future_candles[-1][4])
    final_close_ms = int(future_candles[-1][6])
    return build_result(
        exit_reason="mark_to_market",
        exit_price=final_close if final_close > 0 else entry_price,
        exit_time_ms=final_close_ms,
        holding_bars=len(future_candles),
    )


def compute_profit_factor_from_pnl(pnl_values: Sequence[float]) -> float:
    gross_profit = sum(value for value in pnl_values if value > 0)
    gross_loss = abs(sum(value for value in pnl_values if value < 0))
    if gross_loss <= 1e-12:
        return round4(gross_profit) if gross_profit > 0 else 0.0
    return round4(gross_profit / gross_loss)


def compute_max_drawdown_pct_from_pnl(pnl_values: Sequence[float], *, starting_equity: float | None) -> float:
    if not pnl_values or starting_equity is None or starting_equity <= 0:
        return 0.0
    equity = float(starting_equity)
    peak = equity
    max_drawdown = 0.0
    for pnl in pnl_values:
        equity += float(pnl)
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, ((peak - equity) / peak) * 100.0)
    return round4(max_drawdown)


def summarize_trades(trades: list[dict[str, Any]], *, starting_equity: float | None = None, ending_equity: float | None = None) -> dict[str, Any]:
    if not trades:
        return {
            "trade_count": 0,
            "win_rate_pct": 0.0,
            "avg_after_fee_return_pct": 0.0,
            "avg_pnl_usdt": 0.0,
            "total_pnl_usdt": 0.0,
            "total_return_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "playbooks": {},
        }
    pnl_values = [safe_float(trade.get("pnl_usdt")) for trade in trades]
    after_fee_values = [safe_float(trade.get("after_fee_return_pct")) for trade in trades]
    wins = sum(1 for value in pnl_values if value > 0)
    playbook_groups: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        playbook_groups.setdefault(str(trade.get("playbook") or "unknown"), []).append(trade)
    playbook_summary = {
        playbook: {
            "trade_count": len(rows),
            "win_rate_pct": round4((sum(1 for row in rows if safe_float(row.get("pnl_usdt")) > 0) / len(rows)) * 100.0),
            "avg_after_fee_return_pct": round4(fmean(safe_float(row.get("after_fee_return_pct")) for row in rows)),
            "total_pnl_usdt": round4(sum(safe_float(row.get("pnl_usdt")) for row in rows)),
            "profit_factor": compute_profit_factor_from_pnl([safe_float(row.get("pnl_usdt")) for row in rows]),
        }
        for playbook, rows in sorted(playbook_groups.items())
    }
    total_pnl = sum(pnl_values)
    total_return_pct = 0.0
    if starting_equity is not None and ending_equity is not None and starting_equity > 0:
        total_return_pct = ((ending_equity / starting_equity) - 1.0) * 100.0
    return {
        "trade_count": len(trades),
        "win_rate_pct": round4((wins / len(trades)) * 100.0),
        "avg_after_fee_return_pct": round4(fmean(after_fee_values)),
        "avg_pnl_usdt": round4(fmean(pnl_values)),
        "total_pnl_usdt": round4(total_pnl),
        "total_return_pct": round4(total_return_pct),
        "profit_factor": compute_profit_factor_from_pnl(pnl_values),
        "max_drawdown_pct": compute_max_drawdown_pct_from_pnl(pnl_values, starting_equity=starting_equity),
        "playbooks": playbook_summary,
    }


def compute_daily_sharpe_ratio(trades: list[dict[str, Any]], *, starting_equity: float) -> float:
    if len(trades) < 2 or starting_equity <= 0:
        return 0.0
    pnl_by_day: dict[str, float] = {}
    for trade in trades:
        exit_time_ms = int(trade.get("exit_time_ms") or 0)
        if exit_time_ms <= 0:
            continue
        day_key = datetime.fromtimestamp(exit_time_ms / 1000.0, tz=timezone.utc).date().isoformat()
        pnl_by_day[day_key] = pnl_by_day.get(day_key, 0.0) + safe_float(trade.get("pnl_usdt"))
    if len(pnl_by_day) < 2:
        return 0.0
    equity = float(starting_equity)
    daily_returns: list[float] = []
    for day_key in sorted(pnl_by_day):
        base_equity = equity if equity > 0 else starting_equity
        day_pnl = pnl_by_day[day_key]
        daily_returns.append(day_pnl / max(1e-9, base_equity))
        equity += day_pnl
    if len(daily_returns) < 2:
        return 0.0
    volatility = stdev(daily_returns)
    if volatility <= 1e-12:
        return 0.0
    return round4((fmean(daily_returns) / volatility) * math.sqrt(365.0))


def select_portfolio_trades(
    trades: list[dict[str, Any]],
    *,
    starting_equity: float,
    margin_per_trade: float,
    max_open_positions: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    open_trades: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []
    skipped_capacity = 0
    skipped_symbol_overlap = 0
    skipped_balance = 0
    cash = float(starting_equity)

    def settle_until(timestamp_ms: int) -> None:
        nonlocal cash, open_trades
        remaining: list[dict[str, Any]] = []
        for trade in sorted(open_trades, key=lambda item: int(item["exit_time_ms"])):
            if int(trade["exit_time_ms"]) <= timestamp_ms:
                cash += margin_per_trade + safe_float(trade["pnl_usdt"])
            else:
                remaining.append(trade)
        open_trades = remaining

    for trade in sorted(trades, key=lambda item: int(item["signal_time_ms"])):
        settle_until(int(trade["signal_time_ms"]))
        if max_open_positions > 0 and len(open_trades) >= max_open_positions:
            skipped_capacity += 1
            continue
        if any(str(item["symbol"]).upper() == str(trade["symbol"]).upper() for item in open_trades):
            skipped_symbol_overlap += 1
            continue
        if cash < margin_per_trade:
            skipped_balance += 1
            continue
        cash -= margin_per_trade
        open_trades.append(trade)
        accepted.append(trade)

    settle_until(2**63 - 1)
    metrics = {
        "starting_equity": round4(starting_equity),
        "ending_equity": round4(cash),
        "accepted_trade_count": len(accepted),
        "skipped_capacity": skipped_capacity,
        "skipped_symbol_overlap": skipped_symbol_overlap,
        "skipped_balance": skipped_balance,
        "max_open_positions": max_open_positions,
        "margin_per_trade": round4(margin_per_trade),
    }
    return accepted, metrics


def simulate_portfolio(
    trades: list[dict[str, Any]],
    *,
    starting_equity: float,
    margin_per_trade: float,
    max_open_positions: int,
) -> dict[str, Any]:
    accepted, metrics = select_portfolio_trades(
        trades,
        starting_equity=starting_equity,
        margin_per_trade=margin_per_trade,
        max_open_positions=max_open_positions,
    )
    summary = summarize_trades(
        accepted,
        starting_equity=starting_equity,
        ending_equity=safe_float(metrics.get("ending_equity")),
    )
    summary["sharpe_ratio"] = compute_daily_sharpe_ratio(accepted, starting_equity=starting_equity)
    summary.update(metrics)
    return summary


def collect_symbol_signal_candidates(
    *,
    symbol: str,
    rules: SymbolRules,
    candles_5m: list[list[Any]],
    oi_rows: list[dict[str, Any]],
    trigger_config: EventTriggerConfig,
    allowed_playbooks: set[str],
    playbook_classifier: Callable[[dict[str, Any]], str] = classify_playbook_record,
    quote_allocation_usdt: float,
    leverage: int,
    stop_loss_pct: float,
    take_profit_pct: float,
    breakeven_trigger_pct: float,
    breakeven_lock_pct: float,
    trailing_callback_pct: float,
    round_trip_fee_bps: float,
) -> list[dict[str, Any]]:
    candles = candles_5m
    if len(candles) < 350:
        return []
    oi_timestamps = [int(row.get("timestamp") or 0) for row in oi_rows if int(row.get("timestamp") or 0) > 0]
    filtered_oi_rows = [row for row in oi_rows if int(row.get("timestamp") or 0) > 0]
    candidates: list[dict[str, Any]] = []

    for current_index in range(288, len(candles) - 1):
        ticker_item = build_historical_ticker_item(symbol, candles, current_index=current_index)
        if ticker_item is None:
            continue
        window = candles[max(0, current_index - 80) : current_index + 2]
        context = build_market_event_context(
            ticker_item,
            window,
            config=trigger_config,
            bar_interval="5m",
        )
        if context is None:
            continue
        sample = build_triggered_market_event(context, config=trigger_config)
        if sample is None:
            continue
        oi_end = bisect_right(oi_timestamps, int(context.anchor_close_time_ms))
        oi_slice = filtered_oi_rows[max(0, oi_end - 4) : oi_end]
        if not oi_slice:
            continue
        current_oi = safe_float(
            oi_slice[-1].get("openInterest")
            if oi_slice[-1].get("openInterest") not in (None, "")
            else oi_slice[-1].get("sumOpenInterest")
        )
        enrichments = compute_open_interest_context(current_oi, oi_slice)
        sample_payload = sample.to_payload()
        sample_payload.update(build_proxy_candle_metrics(candles[current_index]))
        record = {
            "symbol": symbol,
            "sample_type": sample.sample_type,
            "bar_interval": sample.bar_interval,
            "trigger_types": list(sample.trigger_types),
            "sample": sample_payload,
            "enrichments": enrichments,
            "trading_session": trading_session_label(int(context.anchor_close_time_ms)),
        }
        playbook = playbook_classifier(record)
        if playbook not in allowed_playbooks:
            continue
        side = derive_signal_side(record, playbook=playbook)
        if side not in {"BUY", "SELL"}:
            continue
        entry_index = current_index + 1
        entry_price = safe_float(candles[entry_index][1])
        if entry_price <= 0:
            continue
        levels = build_trade_levels(
            entry_price=entry_price,
            side=side,
            rules=rules,
            quote_allocation_usdt=quote_allocation_usdt,
            leverage=leverage,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            breakeven_trigger_pct=breakeven_trigger_pct,
            breakeven_lock_pct=breakeven_lock_pct,
            trailing_callback_pct=trailing_callback_pct,
        )
        if levels is None:
            continue
        candidates.append(
            {
                "signal_id": f"{symbol}-{int(context.anchor_close_time_ms)}-{playbook}-{side}".replace(" ", "_"),
                "symbol": symbol,
                "playbook": playbook,
                "side": side,
                "signal_time_ms": int(context.anchor_close_time_ms),
                "signal_time": iso_from_ms(int(context.anchor_close_time_ms)),
                "entry_index": entry_index,
                "entry_time_ms": int(candles[entry_index][0]),
                "entry_time": iso_from_ms(int(candles[entry_index][0])),
                "entry_price": round4(levels.entry_price),
                "trigger_score": round4(sample.trigger_score),
                "trigger_types": list(sample.trigger_types),
                "trading_session": trading_session_label(int(context.anchor_close_time_ms)),
                "open_interest": round4(enrichments.get("open_interest") or 0.0) if enrichments.get("open_interest") is not None else None,
                "oi_change_5m_pct": round4(enrichments.get("oi_change_5m_pct") or 0.0) if enrichments.get("oi_change_5m_pct") is not None else None,
                "oi_change_15m_pct": round4(enrichments.get("oi_change_15m_pct") or 0.0) if enrichments.get("oi_change_15m_pct") is not None else None,
                "notional_usdt": round4(levels.notional_usdt),
                "margin_usdt": round4(levels.margin_usdt),
                "quote_allocation_usdt": round4(quote_allocation_usdt),
                "leverage": leverage,
                "tick_size": round4(rules.tick_size),
                "step_size": round4(rules.step_size),
            }
        )
    return candidates


def simulate_signal_candidate(
    candidate: dict[str, Any],
    *,
    candles_5m: list[list[Any]],
    rules: SymbolRules,
    quote_allocation_usdt: float,
    leverage: int,
    stop_loss_pct: float,
    take_profit_pct: float,
    breakeven_trigger_pct: float,
    breakeven_lock_pct: float,
    trailing_callback_pct: float,
    round_trip_fee_bps: float,
) -> dict[str, Any] | None:
    entry_index = int(candidate["entry_index"])
    entry_price = safe_float(candidate.get("entry_price"))
    levels = build_trade_levels(
        entry_price=entry_price,
        side=str(candidate["side"]),
        rules=rules,
        quote_allocation_usdt=quote_allocation_usdt,
        leverage=leverage,
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        breakeven_trigger_pct=breakeven_trigger_pct,
        breakeven_lock_pct=breakeven_lock_pct,
        trailing_callback_pct=trailing_callback_pct,
    )
    if levels is None:
        return None
    outcome = simulate_trade_path(
        side=str(candidate["side"]),
        entry_price=levels.entry_price,
        future_candles=candles_5m[entry_index:],
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        breakeven_trigger_pct=breakeven_trigger_pct,
        breakeven_lock_pct=breakeven_lock_pct,
        trailing_callback_pct=trailing_callback_pct,
        round_trip_fee_bps=round_trip_fee_bps,
        notional_usdt=levels.notional_usdt,
        margin_usdt=levels.margin_usdt,
        tick_size=rules.tick_size,
    )
    return {
        "signal_id": str(candidate["signal_id"]),
        "symbol": str(candidate["symbol"]),
        "playbook": str(candidate["playbook"]),
        "side": str(candidate["side"]),
        "signal_time_ms": int(candidate["signal_time_ms"]),
        "signal_time": str(candidate["signal_time"]),
        "entry_time_ms": int(candidate["entry_time_ms"]),
        "entry_time": str(candidate["entry_time"]),
        "entry_price": round4(levels.entry_price),
        "trigger_score": round4(candidate["trigger_score"]),
        "trigger_types": list(candidate["trigger_types"]),
        "trading_session": str(candidate["trading_session"]),
        "open_interest": candidate.get("open_interest"),
        "oi_change_5m_pct": candidate.get("oi_change_5m_pct"),
        "oi_change_15m_pct": candidate.get("oi_change_15m_pct"),
        "notional_usdt": round4(levels.notional_usdt),
        "quote_allocation_usdt": round4(quote_allocation_usdt),
        "leverage": leverage,
        "stop_price": round4(levels.stop_price),
        "take_profit_price": round4(levels.take_profit_price) if levels.take_profit_price is not None else None,
        "breakeven_trigger_price": round4(levels.breakeven_trigger_price),
        "breakeven_stop_price": round4(levels.breakeven_stop_price),
        "trailing_callback_pct": round4(levels.trailing_callback_pct),
        **outcome,
    }


def simulate_signal_candidates(
    candidates: list[dict[str, Any]],
    *,
    candles_by_symbol: dict[str, list[list[Any]]],
    rules_by_symbol: dict[str, SymbolRules],
    quote_allocation_usdt: float,
    leverage: int,
    stop_loss_pct: float,
    take_profit_pct: float,
    breakeven_trigger_pct: float,
    breakeven_lock_pct: float,
    trailing_callback_pct: float,
    round_trip_fee_bps: float,
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for candidate in candidates:
        symbol = str(candidate["symbol"])
        candles = candles_by_symbol.get(symbol)
        rules = rules_by_symbol.get(symbol)
        if candles is None or rules is None:
            continue
        trade = simulate_signal_candidate(
            candidate,
            candles_5m=candles,
            rules=rules,
            quote_allocation_usdt=quote_allocation_usdt,
            leverage=leverage,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            breakeven_trigger_pct=breakeven_trigger_pct,
            breakeven_lock_pct=breakeven_lock_pct,
            trailing_callback_pct=trailing_callback_pct,
            round_trip_fee_bps=round_trip_fee_bps,
        )
        if trade is not None:
            trades.append(trade)
    return trades


def backtest_symbol(
    *,
    symbol: str,
    rules: SymbolRules,
    candles_5m: list[list[Any]],
    oi_rows: list[dict[str, Any]],
    trigger_config: EventTriggerConfig,
    allowed_playbooks: set[str],
    quote_allocation_usdt: float,
    leverage: int,
    stop_loss_pct: float,
    take_profit_pct: float,
    breakeven_trigger_pct: float,
    breakeven_lock_pct: float,
    trailing_callback_pct: float,
    round_trip_fee_bps: float,
    playbook_classifier: Callable[[dict[str, Any]], str] = classify_playbook_record,
) -> list[dict[str, Any]]:
    candidates = collect_symbol_signal_candidates(
        symbol=symbol,
        rules=rules,
        candles_5m=candles_5m,
        oi_rows=oi_rows,
        trigger_config=trigger_config,
        allowed_playbooks=allowed_playbooks,
        playbook_classifier=playbook_classifier,
        quote_allocation_usdt=quote_allocation_usdt,
        leverage=leverage,
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        breakeven_trigger_pct=breakeven_trigger_pct,
        breakeven_lock_pct=breakeven_lock_pct,
        trailing_callback_pct=trailing_callback_pct,
        round_trip_fee_bps=round_trip_fee_bps,
    )
    return simulate_signal_candidates(
        candidates,
        candles_by_symbol={symbol: candles_5m},
        rules_by_symbol={symbol: rules},
        quote_allocation_usdt=quote_allocation_usdt,
        leverage=leverage,
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        breakeven_trigger_pct=breakeven_trigger_pct,
        breakeven_lock_pct=breakeven_lock_pct,
        trailing_callback_pct=trailing_callback_pct,
        round_trip_fee_bps=round_trip_fee_bps,
    )


def fetch_symbol_history(
    client: BinancePublicBackfillClient,
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    chunk_hours: int = DEFAULT_HISTORY_CHUNK_HOURS,
    fetch_open_interest: bool = True,
    throttle_callback: Callable[[], Any] | None = None,
) -> tuple[list[list[Any]], list[dict[str, Any]]]:
    candle_rows: list[dict[str, Any]] = []
    oi_rows: list[dict[str, Any]] = []
    for chunk_start_ms, chunk_end_ms in build_history_chunks(start_ms, end_ms, chunk_hours=max(1, int(chunk_hours))):
        if throttle_callback is not None:
            throttle_callback()
        candle_rows.extend(
            client.fetch_klines(
                symbol=symbol,
                interval="5m",
                start_ms=chunk_start_ms,
                end_ms=chunk_end_ms,
            )
        )
        if fetch_open_interest:
            oi_rows.extend(
                client.fetch_open_interest_hist(
                    symbol=symbol,
                    period="5m",
                    start_ms=chunk_start_ms,
                    end_ms=chunk_end_ms,
                )
            )
    candle_rows = dedupe_sort_rows(candle_rows, "open_time")
    oi_rows = dedupe_sort_rows(oi_rows, "timestamp")
    candles = [
        [row.get(column) for column in KLINE_COLUMNS]
        for row in candle_rows
        if isinstance(row, dict)
    ]
    return candles, oi_rows


def run_parameter_scan(
    *,
    candidates: list[dict[str, Any]],
    candles_by_symbol: dict[str, list[list[Any]]],
    rules_by_symbol: dict[str, SymbolRules],
    stop_losses: list[float],
    take_profits: list[float],
    quote_allocation_usdt: float,
    leverage: int,
    breakeven_trigger_pct: float,
    breakeven_lock_pct: float,
    trailing_callback_pct: float,
    round_trip_fee_bps: float,
    starting_equity: float,
    portfolio_max_open_positions: int,
) -> dict[str, Any]:
    trade_groups: dict[tuple[float, float], list[dict[str, Any]]] = {}
    for stop_loss_pct in stop_losses:
        for take_profit_pct in take_profits:
            trade_groups[(round4(stop_loss_pct), round4(take_profit_pct))] = simulate_signal_candidates(
                candidates,
                candles_by_symbol=candles_by_symbol,
                rules_by_symbol=rules_by_symbol,
                quote_allocation_usdt=quote_allocation_usdt,
                leverage=leverage,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                breakeven_trigger_pct=breakeven_trigger_pct,
                breakeven_lock_pct=breakeven_lock_pct,
                trailing_callback_pct=trailing_callback_pct,
                round_trip_fee_bps=round_trip_fee_bps,
            )
    return summarize_parameter_scan_trade_groups(
        trade_groups=trade_groups,
        quote_allocation_usdt=quote_allocation_usdt,
        starting_equity=starting_equity,
        portfolio_max_open_positions=portfolio_max_open_positions,
    )


def summarize_parameter_scan_trade_groups(
    *,
    trade_groups: dict[tuple[float, float], list[dict[str, Any]]],
    quote_allocation_usdt: float,
    starting_equity: float,
    portfolio_max_open_positions: int,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for (stop_loss_pct, take_profit_pct), trades in trade_groups.items():
        ordered_trades = sorted(trades, key=lambda item: int(item["signal_time_ms"]))
        accepted, metrics = select_portfolio_trades(
            ordered_trades,
            starting_equity=starting_equity,
            margin_per_trade=quote_allocation_usdt,
            max_open_positions=portfolio_max_open_positions,
        )
        portfolio_summary = summarize_trades(
            accepted,
            starting_equity=starting_equity,
            ending_equity=safe_float(metrics.get("ending_equity")),
        )
        sharpe_ratio = compute_daily_sharpe_ratio(accepted, starting_equity=starting_equity)
        rows.append(
            {
                "stop_loss_pct": round4(stop_loss_pct),
                "take_profit_pct": round4(take_profit_pct),
                "raw_trade_count": len(ordered_trades),
                "accepted_trade_count": metrics["accepted_trade_count"],
                "win_rate_pct": portfolio_summary["win_rate_pct"],
                "avg_after_fee_return_pct": portfolio_summary["avg_after_fee_return_pct"],
                "total_pnl_usdt": portfolio_summary["total_pnl_usdt"],
                "total_return_pct": portfolio_summary["total_return_pct"],
                "ending_equity": metrics["ending_equity"],
                "sharpe_ratio": sharpe_ratio,
                "skipped_capacity": metrics["skipped_capacity"],
                "skipped_symbol_overlap": metrics["skipped_symbol_overlap"],
                "skipped_balance": metrics["skipped_balance"],
            }
        )
    rows.sort(
        key=lambda item: (
            safe_float(item.get("sharpe_ratio")),
            safe_float(item.get("total_return_pct")),
            safe_float(item.get("total_pnl_usdt")),
        ),
        reverse=True,
    )
    best_sharpe = rows[0] if rows else None
    best_return = (
        max(
            rows,
            key=lambda item: (
                safe_float(item.get("total_return_pct")),
                safe_float(item.get("sharpe_ratio")),
                safe_float(item.get("total_pnl_usdt")),
            )
        )
        if rows
        else None
    )
    return {
        "combo_count": len(rows),
        "best_sharpe_combo": best_sharpe,
        "best_total_return_combo": best_return,
        "rows": rows,
    }


def render_console_summary(report: dict[str, Any]) -> str:
    raw_summary = report["raw_signal_summary"]
    portfolio_summary = report["portfolio_summary"]
    portfolio_slots = report["config"]["portfolio_max_open_positions"]
    lines = [
        "Phoenix Playbook Backtest",
        f"Window: {report['config']['days']}d | Universe: {report['config']['universe_top']} | Signals: {raw_summary['trade_count']}",
        f"Raw: win={raw_summary['win_rate_pct']:.2f}% avg_af={raw_summary['avg_after_fee_return_pct']:.4f}% total_pnl={raw_summary['total_pnl_usdt']:.2f}U",
        f"Portfolio({portfolio_slots} slots): win={portfolio_summary['win_rate_pct']:.2f}% total_return={portfolio_summary['total_return_pct']:.4f}% ending={portfolio_summary['ending_equity']:.2f}U accepted={portfolio_summary['accepted_trade_count']}",
    ]
    parameter_scan = report.get("parameter_scan")
    if isinstance(parameter_scan, dict) and isinstance(parameter_scan.get("best_sharpe_combo"), dict):
        best = parameter_scan["best_sharpe_combo"]
        lines.append(
            "Best Scan: "
            + f"SL={safe_float(best.get('stop_loss_pct')):.2f}% "
            + f"TP={safe_float(best.get('take_profit_pct')):.2f}% "
            + f"Sharpe={safe_float(best.get('sharpe_ratio')):.4f} "
            + f"Return={safe_float(best.get('total_return_pct')):.4f}%"
        )
    return "\n".join(lines)


def build_promotion_backtest_report(report: dict[str, Any]) -> dict[str, Any]:
    portfolio = report.get("portfolio_summary") if isinstance(report.get("portfolio_summary"), dict) else {}
    raw = report.get("raw_signal_summary") if isinstance(report.get("raw_signal_summary"), dict) else {}
    config = report.get("config") if isinstance(report.get("config"), dict) else {}
    trade_count = int(portfolio.get("accepted_trade_count") or portfolio.get("trade_count") or 0)
    avg_return_pct = safe_float(portfolio.get("avg_after_fee_return_pct"))
    profit_factor = safe_float(portfolio.get("profit_factor"))
    win_rate_pct = safe_float(portfolio.get("win_rate_pct"))
    sharpe_ratio = safe_float(portfolio.get("sharpe_ratio"))
    max_drawdown_pct = safe_float(portfolio.get("max_drawdown_pct"))
    blockers: list[str] = []
    warnings: list[str] = []
    if trade_count < 1000:
        blockers.append("backtest_sample_size_below_1000")
    if avg_return_pct is None or avg_return_pct <= 0:
        blockers.append("backtest_avg_return_not_positive")
    if profit_factor is None or profit_factor < 1.2:
        blockers.append("backtest_profit_factor_below_1_2")
    if win_rate_pct is not None and win_rate_pct < 50.0:
        warnings.append("backtest_win_rate_below_50")
    if sharpe_ratio is not None and sharpe_ratio < 1.0:
        warnings.append("backtest_sharpe_below_1")
    return {
        "generated_at": utc_now_iso(),
        "report_type": "phoenix_promotion_backtest",
        "source": "phoenix_playbook_backtest",
        "status": "pass" if not blockers else "fail",
        "blockers": blockers,
        "warnings": warnings,
        "trade_count": trade_count,
        "sample_count": trade_count,
        "raw_trade_count": int(raw.get("trade_count") or 0),
        "avg_return_pct": avg_return_pct,
        "avg_after_fee_return_pct": avg_return_pct,
        "profit_factor": profit_factor,
        "win_rate_pct": win_rate_pct,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown_pct": max_drawdown_pct,
        "total_pnl_usdt": safe_float(portfolio.get("total_pnl_usdt")),
        "total_return_pct": safe_float(portfolio.get("total_return_pct")),
        "assumptions": {
            "data_source": config.get("data_source"),
            "interval": config.get("interval"),
            "start_time": config.get("start_time"),
            "end_time": config.get("end_time"),
            "selected_symbol_count": len(config.get("selected_symbols") or []),
            "allowed_playbooks": config.get("allowed_playbooks"),
            "quote_allocation": config.get("quote_allocation"),
            "leverage": config.get("leverage"),
            "stop_loss_pct": config.get("stop_loss_pct"),
            "take_profit_pct": config.get("take_profit_pct"),
            "round_trip_fee_bps": config.get("round_trip_fee_bps"),
            "portfolio_max_open_positions": config.get("portfolio_max_open_positions"),
        },
        "policy": {
            "min_trade_count": 1000,
            "min_profit_factor": 1.2,
            "requires_positive_avg_return": True,
            "parameter_scan_is_advisory_only": True,
        },
    }


def resolve_backtest_window(
    *,
    days: int,
    start_date: date | None,
    end_date: date | None,
    now_func: Callable[[], datetime] = now_utc,
) -> tuple[datetime, datetime]:
    if start_date is not None:
        start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
        if end_date is not None:
            end_dt = datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc) + timedelta(days=1)
        else:
            end_dt = now_func().replace(second=0, microsecond=0)
    else:
        if end_date is not None:
            end_dt = datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc) + timedelta(days=1)
        else:
            end_dt = now_func().replace(second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=max(1, int(days)))
    if end_dt <= start_dt:
        raise ValueError("Backtest end time must be after start time")
    return start_dt, end_dt


def main() -> int:
    args = parse_args()
    allowed_playbooks = parse_allowed_playbooks(args.allowed_playbooks)
    playbook_classifier = choose_playbook_classifier(args.playbook_classifier)
    scan_stop_losses = parse_float_grid(args.scan_stop_losses)
    scan_take_profits = parse_float_grid(args.scan_take_profits)
    scan_combos = [(stop_loss_pct, take_profit_pct) for stop_loss_pct in scan_stop_losses for take_profit_pct in scan_take_profits]
    requested_symbols = parse_symbol_list(args.symbols)
    start_date = parse_utc_date(args.start_date)
    end_date = parse_utc_date(args.end_date)
    config_defaults = load_execution_settings()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    history_chunk_hours = max(1, int(args.history_chunk_hours))
    max_load_average = max(0.0, float(args.max_load_average or 0.0))
    load_check_sleep_sec = max(0.01, float(args.load_check_sleep_sec or 0.0))
    process_nice_applied = set_process_nice_value(DEFAULT_PROCESS_NICE_VALUE)
    load_pause_count = 0

    start_dt, end_dt = resolve_backtest_window(
        days=max(1, int(args.days)),
        start_date=start_date,
        end_date=end_date,
    )
    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = int(start_dt.timestamp() * 1000)
    proxy = _resolve_proxy()
    if str(args.data_source) == "public-zip":
        client = BinancePublicZipBackfillClient(
            proxy=proxy,
            timeout_sec=20,
            pause_sec=0.15,
            max_retries=4,
            cache_dir=args.public_zip_cache_dir,
            offline_cache_only=bool(args.offline_cache_only),
        )
    else:
        client = BinancePublicBackfillClient(
            proxy=proxy,
            timeout_sec=20,
            pause_sec=0.15,
            max_retries=4,
        )

    exchange_info_payload = read_json_payload(args.exchange_info_json)
    if exchange_info_payload is None:
        exchange_info_payload = client._get_json("/fapi/v1/exchangeInfo", {})
    rules_lookup = build_symbol_rules_lookup(exchange_info_payload if isinstance(exchange_info_payload, dict) else {})
    if requested_symbols:
        universe = [
            {
                "symbol": symbol,
                "quote_volume_24h": None,
                "price_change_24h_pct": None,
                "last_price": None,
            }
            for symbol in requested_symbols
        ]
    else:
        ticker_payload = read_json_payload(args.ticker_24hr_json)
        if ticker_payload is None:
            ticker_payload = client._get_json("/fapi/v1/ticker/24hr", {})
        universe = discover_universe(
            ticker_payload if isinstance(ticker_payload, list) else [],
            top_limit=max(1, int(args.universe_top)),
            min_quote_volume=max(0.0, float(args.min_quote_volume or 0.0)),
            sort_by="quote_volume",
        )
    trigger_config = EventTriggerConfig(
        atr_period=20,
        atr_multiplier=2.0,
        volume_lookback=20,
        volume_multiplier=3.0,
        atr_multiplier_1m=1.5,
        volume_multiplier_1m=2.0,
        atr_multiplier_5m=1.5,
        volume_multiplier_5m=2.25,
        min_price=0.01,
        min_quote_volume_24h=max(0.0, float(args.min_quote_volume or 0.0)),
        min_avg_quote_turnover_1m=100_000.0,
        min_current_quote_turnover_1m=10_000.0,
    )

    def protect_backtest_progress() -> None:
        nonlocal load_pause_count
        load_pause_count += wait_for_safe_system_load(
            max_load_average=max_load_average,
            sleep_seconds=load_check_sleep_sec,
        )

    emit_event(
        "playbook_backtest_started",
        days=max(1, int(args.days)),
        start_time=start_dt.isoformat(timespec="milliseconds"),
        end_time=end_dt.isoformat(timespec="milliseconds"),
        universe_top=max(1, int(args.universe_top)),
        requested_symbols=requested_symbols,
        allowed_playbooks=sorted(allowed_playbooks),
        playbook_classifier=str(args.playbook_classifier),
        data_source=str(args.data_source),
        public_zip_cache_dir=str(args.public_zip_cache_dir) if str(args.data_source) == "public-zip" else None,
        offline_cache_only=bool(args.offline_cache_only),
        skip_open_interest=bool(args.skip_open_interest),
        history_chunk_hours=history_chunk_hours,
        process_nice_value=DEFAULT_PROCESS_NICE_VALUE,
        process_nice_applied=process_nice_applied,
        max_load_average=round4(max_load_average),
        start_ms=start_ms,
        end_ms=end_ms,
    )

    selected_symbols = [item["symbol"] for item in universe]
    history_chunk_count = len(build_history_chunks(start_ms, end_ms, chunk_hours=history_chunk_hours))
    all_trades: list[dict[str, Any]] = []
    parameter_scan_trade_groups = {
        (round4(stop_loss_pct), round4(take_profit_pct)): []
        for stop_loss_pct, take_profit_pct in scan_combos
    }
    saw_any_candidates = False
    for index, symbol in enumerate(selected_symbols, start=1):
        rules = rules_lookup.get(symbol)
        if rules is None:
            emit_event("playbook_backtest_symbol_skipped", symbol=symbol, reason="missing_rules")
            continue
        try:
            protect_backtest_progress()
            candles, oi_rows = fetch_symbol_history(
                client,
                symbol=symbol,
                start_ms=start_ms,
                end_ms=end_ms,
                chunk_hours=history_chunk_hours,
                fetch_open_interest=not bool(args.skip_open_interest),
                throttle_callback=protect_backtest_progress,
            )
        except Exception as exc:  # noqa: BLE001
            emit_event("playbook_backtest_symbol_error", symbol=symbol, error=str(exc), ordinal=index)
            continue
        protect_backtest_progress()
        symbol_candidates = collect_symbol_signal_candidates(
            symbol=symbol,
            rules=rules,
            candles_5m=candles,
            oi_rows=oi_rows,
            trigger_config=trigger_config,
            allowed_playbooks=allowed_playbooks,
            playbook_classifier=playbook_classifier,
            quote_allocation_usdt=float(args.quote_allocation),
            leverage=max(1, int(args.leverage)),
            stop_loss_pct=float(args.stop_loss_pct),
            take_profit_pct=float(args.take_profit_pct),
            breakeven_trigger_pct=float(args.breakeven_trigger_pct),
            breakeven_lock_pct=float(args.breakeven_lock_pct),
            trailing_callback_pct=float(args.trailing_callback_pct),
            round_trip_fee_bps=float(args.round_trip_fee_bps),
        )
        if symbol_candidates:
            saw_any_candidates = True
        symbol_trades = simulate_signal_candidates(
            symbol_candidates,
            candles_by_symbol={symbol: candles},
            rules_by_symbol={symbol: rules},
            quote_allocation_usdt=float(args.quote_allocation),
            leverage=max(1, int(args.leverage)),
            stop_loss_pct=float(args.stop_loss_pct),
            take_profit_pct=float(args.take_profit_pct),
            breakeven_trigger_pct=float(args.breakeven_trigger_pct),
            breakeven_lock_pct=float(args.breakeven_lock_pct),
            trailing_callback_pct=float(args.trailing_callback_pct),
            round_trip_fee_bps=float(args.round_trip_fee_bps),
        )
        all_trades.extend(symbol_trades)
        if parameter_scan_trade_groups and symbol_candidates:
            for stop_loss_pct, take_profit_pct in scan_combos:
                protect_backtest_progress()
                combo_trades = simulate_signal_candidates(
                    symbol_candidates,
                    candles_by_symbol={symbol: candles},
                    rules_by_symbol={symbol: rules},
                    quote_allocation_usdt=float(args.quote_allocation),
                    leverage=max(1, int(args.leverage)),
                    stop_loss_pct=float(stop_loss_pct),
                    take_profit_pct=float(take_profit_pct),
                    breakeven_trigger_pct=float(args.breakeven_trigger_pct),
                    breakeven_lock_pct=float(args.breakeven_lock_pct),
                    trailing_callback_pct=float(args.trailing_callback_pct),
                    round_trip_fee_bps=float(args.round_trip_fee_bps),
                )
                parameter_scan_trade_groups[(round4(stop_loss_pct), round4(take_profit_pct))].extend(combo_trades)
        emit_event(
            "playbook_backtest_symbol_complete",
            symbol=symbol,
            ordinal=index,
            universe_size=len(selected_symbols),
            candles=len(candles),
            oi_rows=len(oi_rows),
            candidates=len(symbol_candidates),
            trades=len(symbol_trades),
            history_chunks=history_chunk_count,
            gc_collected=True,
        )
        del candles
        del oi_rows
        del symbol_candidates
        del symbol_trades
        gc.collect()
    all_trades.sort(key=lambda item: int(item["signal_time_ms"]))
    raw_signal_summary = summarize_trades(all_trades)
    portfolio_summary = simulate_portfolio(
        all_trades,
        starting_equity=float(args.starting_equity),
        margin_per_trade=float(args.quote_allocation),
        max_open_positions=max(0, int(args.portfolio_max_open_positions)),
    )
    parameter_scan = None
    if parameter_scan_trade_groups and saw_any_candidates:
        parameter_scan = summarize_parameter_scan_trade_groups(
            trade_groups=parameter_scan_trade_groups,
            quote_allocation_usdt=float(args.quote_allocation),
            starting_equity=float(args.starting_equity),
            portfolio_max_open_positions=max(0, int(args.portfolio_max_open_positions)),
        )
    seen_playbooks = sorted({str(trade.get("playbook") or "") for trade in all_trades if str(trade.get("playbook") or "")})
    missing_allowed_playbooks = sorted(allowed_playbooks.difference(seen_playbooks))

    report = {
        "generated_at": utc_now_iso(),
        "config": {
            "days": max(1, int(args.days)),
            "start_time": start_dt.isoformat(timespec="milliseconds"),
            "end_time": end_dt.isoformat(timespec="milliseconds"),
            "start_date": start_date.isoformat() if start_date is not None else None,
            "end_date": end_date.isoformat() if end_date is not None else None,
            "interval": args.interval,
            "data_source": str(args.data_source),
            "public_zip_cache_dir": str(args.public_zip_cache_dir) if str(args.data_source) == "public-zip" else None,
            "offline_cache_only": bool(args.offline_cache_only),
            "skip_open_interest": bool(args.skip_open_interest),
            "exchange_info_json": str(args.exchange_info_json) if args.exchange_info_json is not None else None,
            "ticker_24hr_json": str(args.ticker_24hr_json) if args.ticker_24hr_json is not None else None,
            "universe_top": max(1, int(args.universe_top)),
            "requested_symbols": requested_symbols,
            "selected_symbols": selected_symbols,
            "allowed_playbooks": sorted(allowed_playbooks),
            "quote_allocation": float(args.quote_allocation),
            "leverage": max(1, int(args.leverage)),
            "stop_loss_pct": float(args.stop_loss_pct),
            "take_profit_pct": float(args.take_profit_pct),
            "breakeven_trigger_pct": float(args.breakeven_trigger_pct),
            "breakeven_lock_pct": float(args.breakeven_lock_pct),
            "trailing_callback_pct": float(args.trailing_callback_pct),
            "round_trip_fee_bps": float(args.round_trip_fee_bps),
            "portfolio_max_open_positions": max(0, int(args.portfolio_max_open_positions)),
            "starting_equity": float(args.starting_equity),
            "workers_note": str(args.workers_note),
            "playbook_classifier": str(args.playbook_classifier),
            "history_chunk_hours": history_chunk_hours,
            "max_load_average": round4(max_load_average),
            "load_check_sleep_sec": round4(load_check_sleep_sec),
            "process_nice_value": DEFAULT_PROCESS_NICE_VALUE,
            "process_nice_applied": process_nice_applied,
            "load_pause_count": load_pause_count,
            "config_defaults_max_open_positions": int(config_defaults.max_open_positions),
        },
        "raw_signal_summary": raw_signal_summary,
        "portfolio_summary": portfolio_summary,
        "parameter_scan": parameter_scan,
        "seen_playbooks": seen_playbooks,
        "missing_allowed_playbooks": missing_allowed_playbooks,
        "notes": [
            "Signals are reconstructed from historical 5m candles plus historical 5m open-interest using the live v7 trigger thresholds and selected playbook classifier mode.",
            "When data_source=public-zip, kline candles are read from Binance public daily futures zip files cached under the project signal_lab_replay directory; OI remains REST-backed because Binance exposes it through futures data endpoints.",
            "Entry price is the next 5m candle open after the signal candle closes to avoid same-bar lookahead.",
            "SL/TP, breakeven, and trailing exits are simulated from 5m OHLC paths with a deterministic intrabar path assumption.",
            "Historical data is fetched sequentially in 6h chunks per symbol and released after processing to reduce peak memory on small servers.",
            "Historical order-book imbalance is not available from Binance public history endpoints, so only playbooks reconstructible from candles + OI will appear in the strict report.",
        ],
    }

    summary_path = output_dir / "playbook_backtest_report.json"
    trades_path = output_dir / "playbook_backtest_trades.jsonl"
    promotion_backtest_path = args.backtest_report_json or (output_dir / "backtest_report.json")
    promotion_backtest_report = build_promotion_backtest_report(report)
    promotion_backtest_report["source_report"] = str(summary_path)
    promotion_backtest_report["trades_file"] = str(trades_path)
    summary_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    promotion_backtest_path.parent.mkdir(parents=True, exist_ok=True)
    promotion_backtest_path.write_text(
        json.dumps(promotion_backtest_report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with trades_path.open("w", encoding="utf-8") as handle:
        for trade in all_trades:
            handle.write(json.dumps(trade, ensure_ascii=False) + "\n")

    print(render_console_summary(report))
    emit_event(
        "playbook_backtest_completed",
        report=str(summary_path),
        backtest_report=str(promotion_backtest_path),
        trades=str(trades_path),
        raw_trade_count=raw_signal_summary["trade_count"],
        portfolio_trade_count=portfolio_summary["accepted_trade_count"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
