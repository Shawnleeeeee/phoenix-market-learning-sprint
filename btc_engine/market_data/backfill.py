"""Runnable first-version BTC market data backfill for research."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener

from btc_engine.config import DATA_DIR

FAPI_BASE_URL = "https://fapi.binance.com"
USER_AGENT = "Phoenix-BTC-Backfill/1.0"
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
GLOBAL_LONG_SHORT_COLUMNS = [
    "symbol",
    "longAccount",
    "longShortRatio",
    "shortAccount",
    "timestamp",
]
PREMIUM_KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "ignore_1",
    "close_time",
    "ignore_2",
    "ignore_3",
    "ignore_4",
    "ignore_5",
    "ignore_6",
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_from_ms(ts_ms: int | str) -> str:
    value = int(ts_ms)
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def _resolve_proxy() -> str | None:
    for key in ("BTC_ALL_PROXY", "ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        value = os.environ.get(key)
        if value:
            return value
    return None


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> int:
    materialized = list(rows)
    _ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in materialized:
            writer.writerow(row)
    return len(materialized)


def _log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


@dataclass(slots=True)
class BackfillConfig:
    symbol: str = "BTCUSDT"
    interval: str = "5m"
    days: int = 90
    output_dir: Path | None = None
    request_timeout_sec: int = 20
    request_pause_sec: float = 0.15
    max_retries: int = 4

    @property
    def interval_ms(self) -> int:
        if self.interval not in INTERVAL_TO_MS:
            raise ValueError(f"Unsupported interval: {self.interval}")
        return INTERVAL_TO_MS[self.interval]

    @property
    def resolved_output_dir(self) -> Path:
        if self.output_dir is not None:
            return self.output_dir
        return DATA_DIR / "raw" / self.symbol / self.interval


class BinancePublicBackfillClient:
    def __init__(self, *, proxy: str | None, timeout_sec: int, pause_sec: float, max_retries: int) -> None:
        self.proxy = proxy
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
                row["open_time_iso"] = _iso_from_ms(row["open_time"])
                row["close_time_iso"] = _iso_from_ms(row["close_time"])
                rows.append(row)
            cursor = int(payload[-1][0]) + interval_ms
            time.sleep(self.pause_sec)
        return _dedupe_sort(rows, "open_time")

    def fetch_premium_index_klines(self, *, symbol: str, interval: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = start_ms
        interval_ms = INTERVAL_TO_MS[interval]
        while cursor < end_ms:
            payload = self._get_json(
                "/fapi/v1/premiumIndexKlines",
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
                row = dict(zip(PREMIUM_KLINE_COLUMNS, raw, strict=False))
                row["open_time_iso"] = _iso_from_ms(row["open_time"])
                row["close_time_iso"] = _iso_from_ms(row["close_time"])
                rows.append(row)
            cursor = int(payload[-1][0]) + interval_ms
            time.sleep(self.pause_sec)
        return _dedupe_sort(rows, "open_time")

    def fetch_funding_rate_history(self, *, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = start_ms
        while cursor < end_ms:
            payload = self._get_json(
                "/fapi/v1/fundingRate",
                {
                    "symbol": symbol,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1000,
                },
            )
            if not payload:
                break
            rows.extend(payload)
            cursor = int(payload[-1]["fundingTime"]) + 1
            time.sleep(self.pause_sec)
        for row in rows:
            row["funding_time_iso"] = _iso_from_ms(row["fundingTime"])
        return _dedupe_sort(rows, "fundingTime")

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
        for row in rows:
            row["timestamp_iso"] = _iso_from_ms(row["timestamp"])
        return _dedupe_sort(rows, "timestamp")

    def fetch_taker_long_short_ratio(self, *, symbol: str, period: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = start_ms
        period_ms = INTERVAL_TO_MS[period]
        chunk_ms = period_ms * 500
        while cursor < end_ms:
            chunk_end_ms = min(cursor + chunk_ms - 1, end_ms)
            payload = self._get_json(
                "/futures/data/takerlongshortRatio",
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
        for row in rows:
            row["timestamp_iso"] = _iso_from_ms(row["timestamp"])
        return _dedupe_sort(rows, "timestamp")

    def fetch_global_long_short_account_ratio(self, *, symbol: str, period: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = start_ms
        period_ms = INTERVAL_TO_MS[period]
        chunk_ms = period_ms * 500
        while cursor < end_ms:
            chunk_end_ms = min(cursor + chunk_ms - 1, end_ms)
            payload = self._get_json(
                "/futures/data/globalLongShortAccountRatio",
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
        for row in rows:
            row["timestamp_iso"] = _iso_from_ms(row["timestamp"])
        return _dedupe_sort(rows, "timestamp")


def _dedupe_sort(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        deduped[str(row[key])] = row
    return [deduped[k] for k in sorted(deduped, key=lambda v: int(v))]


def _build_manifest_entry(path: Path, count: int, *, start_key: str | None = None, end_key: str | None = None, rows: list[dict[str, Any]] | None = None, note: str | None = None) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "path": str(path),
        "rows": count,
    }
    if rows:
        if start_key:
            entry["start"] = rows[0][start_key]
        if end_key:
            entry["end"] = rows[-1][end_key]
    if note:
        entry["note"] = note
    return entry


def _fetch_optional(name: str, fetcher: Any) -> tuple[list[dict[str, Any]], str | None]:
    try:
        _log(f"[backfill] fetching optional dataset: {name}")
        rows = fetcher()
        _log(f"[backfill] fetched {name}: {len(rows)} rows")
        return rows, None
    except Exception as exc:  # noqa: BLE001
        _log(f"[backfill] optional dataset failed: {name}: {exc}")
        return [], f"{name} fetch failed: {type(exc).__name__}: {exc}"


def run_backfill(config: BackfillConfig) -> dict[str, Any]:
    now = _utc_now()
    end_ms = int(now.timestamp() * 1000)
    start_ms = int((now - timedelta(days=config.days)).timestamp() * 1000)
    limited_start_ms = int((now - timedelta(days=min(config.days, 30))).timestamp() * 1000)
    output_dir = config.resolved_output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    proxy = _resolve_proxy()

    client = BinancePublicBackfillClient(
        proxy=proxy,
        timeout_sec=config.request_timeout_sec,
        pause_sec=config.request_pause_sec,
        max_retries=config.max_retries,
    )
    _log(f"[backfill] start symbol={config.symbol} interval={config.interval} days={config.days} output={output_dir}")
    _log("[backfill] fetching klines")
    klines = client.fetch_klines(symbol=config.symbol, interval=config.interval, start_ms=start_ms, end_ms=end_ms)
    _log(f"[backfill] fetched klines: {len(klines)} rows")
    _log("[backfill] fetching premium index klines")
    premium_klines = client.fetch_premium_index_klines(symbol=config.symbol, interval=config.interval, start_ms=start_ms, end_ms=end_ms)
    _log(f"[backfill] fetched premium index klines: {len(premium_klines)} rows")
    _log("[backfill] fetching funding rate history")
    funding_rates = client.fetch_funding_rate_history(symbol=config.symbol, start_ms=start_ms, end_ms=end_ms)
    _log(f"[backfill] fetched funding rate history: {len(funding_rates)} rows")
    oi_hist_5m, oi_failure_note = _fetch_optional(
        "open_interest_hist_5m",
        lambda: client.fetch_open_interest_hist(
            symbol=config.symbol,
            period=config.interval,
            start_ms=limited_start_ms,
            end_ms=end_ms,
        ),
    )
    taker_ratio_5m, taker_failure_note = _fetch_optional(
        "taker_buy_sell_ratio_5m",
        lambda: client.fetch_taker_long_short_ratio(
            symbol=config.symbol,
            period=config.interval,
            start_ms=limited_start_ms,
            end_ms=end_ms,
        ),
    )
    klines_1m_recent, klines_1m_failure_note = _fetch_optional(
        "klines_1m_recent",
        lambda: client.fetch_klines(
            symbol=config.symbol,
            interval="1m",
            start_ms=limited_start_ms,
            end_ms=end_ms,
        ),
    )
    global_ratio_5m, global_ratio_failure_note = _fetch_optional(
        "global_long_short_ratio_5m",
        lambda: client.fetch_global_long_short_account_ratio(
            symbol=config.symbol,
            period=config.interval,
            start_ms=limited_start_ms,
            end_ms=end_ms,
        ),
    )

    klines_path = output_dir / "klines.csv"
    premium_path = output_dir / "premium_index_klines.csv"
    funding_path = output_dir / "funding_rate_history.csv"
    oi_path = output_dir / "open_interest_hist_5m.csv"
    taker_path = output_dir / "taker_buy_sell_ratio_5m.csv"
    klines_1m_path = output_dir / "klines_1m_recent.csv"
    global_ratio_path = output_dir / "global_long_short_ratio_5m.csv"
    manifest_path = output_dir / "manifest.json"

    files: dict[str, Any] = {}
    files["klines"] = _build_manifest_entry(
        klines_path,
        _write_csv(klines_path, klines, KLINE_COLUMNS + ["open_time_iso", "close_time_iso"]),
        start_key="open_time_iso",
        end_key="close_time_iso",
        rows=klines,
    )
    files["premium_index_klines"] = _build_manifest_entry(
        premium_path,
        _write_csv(premium_path, premium_klines, PREMIUM_KLINE_COLUMNS + ["open_time_iso", "close_time_iso"]),
        start_key="open_time_iso",
        end_key="close_time_iso",
        rows=premium_klines,
    )
    files["funding_rate_history"] = _build_manifest_entry(
        funding_path,
        _write_csv(funding_path, funding_rates, ["symbol", "fundingRate", "fundingTime", "markPrice", "funding_time_iso"]),
        start_key="funding_time_iso",
        end_key="funding_time_iso",
        rows=funding_rates,
    )
    files["open_interest_hist_5m"] = _build_manifest_entry(
        oi_path,
        _write_csv(oi_path, oi_hist_5m, ["symbol", "sumOpenInterest", "sumOpenInterestValue", "CMCCirculatingSupply", "timestamp", "timestamp_iso"]),
        start_key="timestamp_iso",
        end_key="timestamp_iso",
        rows=oi_hist_5m,
        note="; ".join(
            [
                note
                for note in (
                    "Binance only exposes the latest 1 month for open interest statistics.",
                    oi_failure_note,
                )
                if note
            ]
        ),
    )
    files["taker_buy_sell_ratio_5m"] = _build_manifest_entry(
        taker_path,
        _write_csv(taker_path, taker_ratio_5m, ["buySellRatio", "buyVol", "sellVol", "timestamp", "timestamp_iso"]),
        start_key="timestamp_iso",
        end_key="timestamp_iso",
        rows=taker_ratio_5m,
        note="; ".join(
            [
                note
                for note in (
                    "Binance only exposes the latest 30 days for taker buy/sell volume.",
                    taker_failure_note,
                )
                if note
            ]
        ),
    )
    files["klines_1m_recent"] = _build_manifest_entry(
        klines_1m_path,
        _write_csv(klines_1m_path, klines_1m_recent, KLINE_COLUMNS + ["open_time_iso", "close_time_iso"]),
        start_key="open_time_iso",
        end_key="close_time_iso",
        rows=klines_1m_recent,
        note="; ".join(
            [
                note
                for note in (
                    "Fine-grained 1m microstructure context retained for the latest 30 days only.",
                    klines_1m_failure_note,
                )
                if note
            ]
        ),
    )
    files["global_long_short_ratio_5m"] = _build_manifest_entry(
        global_ratio_path,
        _write_csv(global_ratio_path, global_ratio_5m, GLOBAL_LONG_SHORT_COLUMNS + ["timestamp_iso"]),
        start_key="timestamp_iso",
        end_key="timestamp_iso",
        rows=global_ratio_5m,
        note="; ".join(
            [
                note
                for note in (
                    "Binance crowding ratio history retained for the latest 30 days.",
                    global_ratio_failure_note,
                )
                if note
            ]
        ),
    )

    manifest = {
        "generated_at": now.isoformat(),
        "symbol": config.symbol,
        "interval": config.interval,
        "days_requested": config.days,
        "proxy": proxy,
        "output_dir": str(output_dir),
        "files": files,
        "sources": {
            "klines": "/fapi/v1/klines",
            "premium_index_klines": "/fapi/v1/premiumIndexKlines",
            "funding_rate_history": "/fapi/v1/fundingRate",
            "open_interest_hist_5m": "/futures/data/openInterestHist",
            "taker_buy_sell_ratio_5m": "/futures/data/takerlongshortRatio",
            "klines_1m_recent": "/fapi/v1/klines?interval=1m",
            "global_long_short_ratio_5m": "/futures/data/globalLongShortAccountRatio",
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"[backfill] complete manifest={manifest_path}")
    return manifest


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill BTCUSDT futures market data into btc_data/raw.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    config = BackfillConfig(
        symbol=args.symbol.upper(),
        interval=args.interval,
        days=args.days,
        output_dir=args.output_dir,
    )
    manifest = run_backfill(config)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
