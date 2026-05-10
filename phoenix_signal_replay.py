#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
from bisect import bisect_right
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, TypeVar

import aiohttp

from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import load_proxy_settings, resolve_environment
from phoenix_signal_lab import (
    analyze_records,
    candidate_to_payload,
    compute_market_features,
    parse_horizons,
    parse_setup_filter,
    render_analysis_table,
    round4,
    score_broad_candidate,
    side_aware_return_pct,
    utc_now_iso,
)
from phoenix_testnet_round_runner import (
    discover_universe,
    safe_float,
    score_candidate,
    setup_execution_floor,
)

DEFAULT_REPLAY_OUTPUT_DIR = Path("signal_lab_replay")
DEFAULT_ONE_MINUTE_HISTORY_LIMIT = 4320
DEFAULT_FIVE_MINUTE_HISTORY_LIMIT = 900
DEFAULT_FIFTEEN_MINUTE_HISTORY_LIMIT = 300
DEFAULT_HISTORY_FEE_BPS = 8.0
KLINE_WINDOW = 70
ONE_DAY_MINUTES = 1440
MAX_KLINE_PAGE_LIMIT = 1500
INTERVAL_TO_MS = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
}
RATE_LIMIT_ERROR_CODES = {-1003}
MAX_RATE_LIMIT_ATTEMPTS = 6
T = TypeVar("T")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk-replay Phoenix signal setups from recent Binance futures kline history."
    )
    parser.add_argument("--env", default="prod", choices=["prod", "testnet", "demo"])
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_REPLAY_OUTPUT_DIR)
    parser.add_argument("--symbol-limit", type=int, default=40)
    parser.add_argument("--symbol-offset", type=int, default=0)
    parser.add_argument("--universe-top", type=int, default=160)
    parser.add_argument("--universe-sort", default="quote_volume", choices=["quote_volume", "positive_change", "negative_change", "abs_change"])
    parser.add_argument("--min-quote-volume", type=float, default=20_000_000.0)
    parser.add_argument("--feature-min-quote-volume", type=float, default=20_000_000.0)
    parser.add_argument("--allow-short", action="store_true")
    parser.add_argument("--include-setups", default=None)
    parser.add_argument("--sampling-mode", default="broad", choices=["strict", "hybrid", "broad"])
    parser.add_argument("--starting-min-score", type=float, default=24.0)
    parser.add_argument("--execution-floor-offset", type=float, default=0.0)
    parser.add_argument("--horizons-sec", default="60,180,300,600")
    parser.add_argument("--sample-step-bars", type=int, default=1)
    parser.add_argument("--max-samples-per-symbol", type=int, default=0)
    parser.add_argument("--history-1m-limit", type=int, default=DEFAULT_ONE_MINUTE_HISTORY_LIMIT)
    parser.add_argument("--history-5m-limit", type=int, default=DEFAULT_FIVE_MINUTE_HISTORY_LIMIT)
    parser.add_argument("--history-15m-limit", type=int, default=DEFAULT_FIFTEEN_MINUTE_HISTORY_LIMIT)
    parser.add_argument("--kline-concurrency", type=int, default=3)
    parser.add_argument("--round-trip-fee-bps", type=float, default=DEFAULT_HISTORY_FEE_BPS)
    parser.add_argument("--analysis-limit", type=int, default=20)
    return parser.parse_args()


def emit_event(event: str, **payload: Any) -> None:
    print(json.dumps({"ts": utc_now_iso(), "event": event, **payload}, ensure_ascii=False), flush=True)


def iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def close_time_ms(candle: list[Any]) -> int:
    return int(candle[6])


def open_time_ms(candle: list[Any]) -> int:
    return int(candle[0])


def build_quote_volume_prefix(candles: list[list[Any]]) -> list[float]:
    prefix = [0.0]
    running_total = 0.0
    for candle in candles:
        running_total += max(0.0, safe_float(candle[7]))
        prefix.append(running_total)
    return prefix


def trailing_window(candles: list[list[Any]], *, closed_index: int, minimum_size: int) -> list[list[Any]] | None:
    end = closed_index + 2
    if end > len(candles):
        return None
    start = max(0, end - KLINE_WINDOW)
    window = candles[start:end]
    if len(window) < minimum_size:
        return None
    return window


async def call_with_backoff(
    operation: Callable[[], Awaitable[T]],
    *,
    label: str,
    max_attempts: int = MAX_RATE_LIMIT_ATTEMPTS,
) -> T:
    delay_sec = 2.0
    for attempt in range(1, max(1, int(max_attempts)) + 1):
        try:
            return await operation()
        except BinanceAPIError as exc:
            rate_limited = exc.status == 429 or exc.code in RATE_LIMIT_ERROR_CODES
            if not rate_limited or attempt >= max_attempts:
                raise
            emit_event(
                "signal_replay_rate_limit",
                label=label,
                attempt=attempt,
                retry_delay_sec=round4(delay_sec),
                status=exc.status,
                code=exc.code,
            )
            await asyncio.sleep(delay_sec)
            delay_sec = min(delay_sec * 2.0, 30.0)
    raise RuntimeError(f"Rate-limit retry loop exhausted for {label}.")


def historical_ticker_item(
    symbol: str,
    candles_1m: list[list[Any]],
    *,
    current_index: int,
    quote_volume_prefix: list[float],
) -> dict[str, Any] | None:
    if current_index < ONE_DAY_MINUTES:
        return None
    current_close = safe_float(candles_1m[current_index][4])
    prior_close = safe_float(candles_1m[current_index - ONE_DAY_MINUTES][4])
    window_start = current_index - (ONE_DAY_MINUTES - 1)
    quote_volume_24h = quote_volume_prefix[current_index + 1] - quote_volume_prefix[window_start]
    if current_close <= 0 or prior_close <= 0 or quote_volume_24h <= 0:
        return None
    return {
        "symbol": symbol,
        "quote_volume_24h": quote_volume_24h,
        "price_change_24h_pct": round4(((current_close / prior_close) - 1.0) * 100.0),
    }


def pick_candidate(
    *,
    item: dict[str, Any],
    candles_1m: list[list[Any]],
    candles_5m: list[list[Any]],
    candles_15m: list[list[Any]],
    allow_short: bool,
    include_setups: set[str] | None,
    sampling_mode: str,
    starting_min_score: float,
    execution_floor_offset: float,
    feature_min_quote_volume: float,
):
    strict_candidate = score_candidate(
        item,
        candles_1m,
        candles_5m,
        candles_15m,
        allow_short=allow_short,
    )
    strict_selected = False
    if strict_candidate is not None and include_setups is not None and strict_candidate.setup not in include_setups:
        strict_candidate = None
    if strict_candidate is not None:
        execution_floor = max(
            max(24.0, float(starting_min_score)),
            max(24.0, setup_execution_floor(strict_candidate.setup) + float(execution_floor_offset)),
        )
        if strict_candidate.score >= execution_floor:
            strict_selected = True
    features = compute_market_features(
        item,
        candles_1m,
        candles_5m,
        candles_15m,
        feature_min_quote_volume=max(0.0, float(feature_min_quote_volume)),
    )
    broad_candidate = (
        score_broad_candidate(features, allow_short=allow_short, include_setups=include_setups)
        if features is not None
        else None
    )
    if sampling_mode == "strict":
        return strict_candidate if strict_selected else None
    if sampling_mode == "broad":
        return broad_candidate
    if strict_selected:
        return strict_candidate
    return broad_candidate


def build_horizons_payload(
    *,
    side: str,
    entry_price: float,
    candles_1m: list[list[Any]],
    current_index: int,
    horizons_sec: list[int],
) -> list[dict[str, Any]] | None:
    payload: list[dict[str, Any]] = []
    for horizon_sec in horizons_sec:
        bars_ahead = max(1, math.ceil(horizon_sec / 60.0))
        target_index = current_index + bars_ahead
        if target_index >= len(candles_1m):
            return None
        entry_close_ms = close_time_ms(candles_1m[current_index])
        target_close_ms = close_time_ms(candles_1m[target_index])
        target_price = safe_float(candles_1m[target_index][4])
        if target_price <= 0:
            return None
        path_returns = [
            side_aware_return_pct(
                side=side,
                entry_price=entry_price,
                current_price=safe_float(candles_1m[path_index][4]),
            )
            for path_index in range(current_index + 1, target_index + 1)
            if safe_float(candles_1m[path_index][4]) > 0
        ]
        if not path_returns:
            return None
        payload.append(
            {
                "horizon_sec": horizon_sec,
                "deadline_ms": entry_close_ms + (horizon_sec * 1000),
                "final_return_pct": round4(path_returns[-1]),
                "mfe_pct": round4(max(path_returns)),
                "mae_pct": round4(min(path_returns)),
                "finalized_at_ms": target_close_ms,
                "finalized_at": iso_from_ms(target_close_ms),
                "final_price": round4(target_price),
                "final_price_source": "replay_1m_close",
            }
        )
    return payload


async def fetch_symbol_history(
    futures: BinanceFuturesClient,
    *,
    symbol: str,
    history_1m_limit: int,
    history_5m_limit: int,
    history_15m_limit: int,
) -> tuple[list[list[Any]], list[list[Any]], list[list[Any]]]:
    async def fetch_interval(interval: str, target_bars: int) -> list[list[Any]]:
        wanted_bars = max(0, int(target_bars))
        if wanted_bars <= 0:
            return []
        interval_ms = INTERVAL_TO_MS[interval]
        batches: list[list[list[Any]]] = []
        remaining = wanted_bars
        end_time_ms: int | None = None
        while remaining > 0:
            batch_limit = min(MAX_KLINE_PAGE_LIMIT, remaining)
            batch = await call_with_backoff(
                lambda: futures.klines(
                    symbol,
                    interval=interval,
                    end_time_ms=end_time_ms,
                    limit=batch_limit,
                ),
                label=f"{symbol}:{interval}",
            )
            if not batch:
                break
            batches.append(batch)
            remaining -= len(batch)
            earliest_open_ms = open_time_ms(batch[0])
            next_end_time_ms = earliest_open_ms - 1
            if len(batch) < batch_limit or next_end_time_ms < interval_ms:
                break
            end_time_ms = next_end_time_ms

        candles: list[list[Any]] = []
        seen_open_ms: set[int] = set()
        for batch in reversed(batches):
            for candle in batch:
                candle_open_ms = open_time_ms(candle)
                if candle_open_ms in seen_open_ms:
                    continue
                seen_open_ms.add(candle_open_ms)
                candles.append(candle)
        if len(candles) > wanted_bars:
            candles = candles[-wanted_bars:]
        return candles

    return await asyncio.gather(
        fetch_interval("1m", history_1m_limit),
        fetch_interval("5m", history_5m_limit),
        fetch_interval("15m", history_15m_limit),
    )


async def replay_symbol(
    futures: BinanceFuturesClient,
    *,
    symbol: str,
    history_1m_limit: int,
    history_5m_limit: int,
    history_15m_limit: int,
    horizons_sec: list[int],
    include_setups: set[str] | None,
    sampling_mode: str,
    allow_short: bool,
    starting_min_score: float,
    execution_floor_offset: float,
    feature_min_quote_volume: float,
    sample_step_bars: int,
    max_samples_per_symbol: int,
) -> list[dict[str, Any]]:
    candles_1m, candles_5m, candles_15m = await fetch_symbol_history(
        futures,
        symbol=symbol,
        history_1m_limit=history_1m_limit,
        history_5m_limit=history_5m_limit,
        history_15m_limit=history_15m_limit,
    )
    if len(candles_1m) < (ONE_DAY_MINUTES + 10) or len(candles_5m) < 80 or len(candles_15m) < 80:
        return []

    five_minute_closes = [close_time_ms(candle) for candle in candles_5m]
    fifteen_minute_closes = [close_time_ms(candle) for candle in candles_15m]
    quote_volume_prefix = build_quote_volume_prefix(candles_1m)
    max_future_bars = max(max(1, math.ceil(horizon / 60.0)) for horizon in horizons_sec)
    records: list[dict[str, Any]] = []
    step_bars = max(1, int(sample_step_bars))
    end_index = len(candles_1m) - max_future_bars - 1
    for current_index in range(ONE_DAY_MINUTES, end_index + 1, step_bars):
        ticker_item = historical_ticker_item(
            symbol,
            candles_1m,
            current_index=current_index,
            quote_volume_prefix=quote_volume_prefix,
        )
        if ticker_item is None:
            continue
        sample_close_ms = close_time_ms(candles_1m[current_index])
        tf5_index = bisect_right(five_minute_closes, sample_close_ms) - 1
        tf15_index = bisect_right(fifteen_minute_closes, sample_close_ms) - 1
        if tf5_index < 20 or tf15_index < 20:
            continue
        one_minute_window = trailing_window(candles_1m, closed_index=current_index, minimum_size=65)
        five_minute_window = trailing_window(candles_5m, closed_index=tf5_index, minimum_size=20)
        fifteen_minute_window = trailing_window(candles_15m, closed_index=tf15_index, minimum_size=20)
        if one_minute_window is None or five_minute_window is None or fifteen_minute_window is None:
            continue
        candidate = pick_candidate(
            item=ticker_item,
            candles_1m=one_minute_window,
            candles_5m=five_minute_window,
            candles_15m=fifteen_minute_window,
            allow_short=allow_short,
            include_setups=include_setups,
            sampling_mode=sampling_mode,
            starting_min_score=starting_min_score,
            execution_floor_offset=execution_floor_offset,
            feature_min_quote_volume=feature_min_quote_volume,
        )
        if candidate is None or candidate.mark_price <= 0:
            continue
        horizons_payload = build_horizons_payload(
            side=candidate.side,
            entry_price=candidate.mark_price,
            candles_1m=candles_1m,
            current_index=current_index,
            horizons_sec=horizons_sec,
        )
        if horizons_payload is None:
            continue
        observation_id = (
            f"replay-{symbol}-{open_time_ms(candles_1m[current_index])}-{candidate.setup}-{candidate.side}"
            .replace(" ", "_")
        )
        records.append(
            {
                "event": "observation_labeled",
                "observation_id": observation_id,
                "observed_at": iso_from_ms(sample_close_ms),
                "observed_at_ms": sample_close_ms,
                "cycle_no": len(records) + 1,
                "rank": 1,
                "symbol": candidate.symbol,
                "setup": candidate.setup,
                "side": candidate.side,
                "score": round4(candidate.score),
                "entry_mark_price": round4(candidate.mark_price),
                "candidate": candidate_to_payload(candidate),
                "scout": {
                    "replay_mode": "price_only",
                    "resolution": "1m",
                },
                "horizons": horizons_payload,
            }
        )
        if max_samples_per_symbol > 0 and len(records) >= max_samples_per_symbol:
            break
    return records


async def async_main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    labels_path = output_dir / "candidate_labels.jsonl"
    if labels_path.exists():
        labels_path.unlink()
    horizons_sec = parse_horizons(args.horizons_sec)
    include_setups = parse_setup_filter(args.include_setups)
    timeout = aiohttp.ClientTimeout(total=90, sock_connect=15, sock_read=60)
    environment = resolve_environment(args.env)
    proxy_settings = load_proxy_settings()

    async with aiohttp.ClientSession(timeout=timeout) as session:
        futures = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=None,
            proxy_settings=proxy_settings,
        )
        try:
            universe = discover_universe(
                await call_with_backoff(
                    futures.ticker_24hr,
                    label="ticker_24hr",
                ),
                top_limit=max(1, int(args.universe_top)),
                min_quote_volume=max(1_000_000.0, float(args.min_quote_volume)),
                sort_by=str(args.universe_sort),
            )
        except BinanceAPIError as exc:
            emit_event(
                "signal_replay_universe_error",
                error=str(exc),
                status=exc.status,
                code=exc.code,
            )
            return 1

        symbol_limit = max(1, int(args.symbol_limit))
        symbol_offset = max(0, int(args.symbol_offset))
        selected_symbols = [
            item["symbol"]
            for item in universe[symbol_offset : symbol_offset + symbol_limit]
        ]
        emit_event(
            "signal_replay_started",
            output_dir=str(output_dir),
            symbol_count=len(selected_symbols),
            symbol_offset=symbol_offset,
            sampling_mode=str(args.sampling_mode),
            include_setups=sorted(include_setups) if include_setups is not None else None,
            horizons_sec=horizons_sec,
        )
        semaphore = asyncio.Semaphore(max(1, int(args.kline_concurrency)))

        async def one(symbol: str) -> list[dict[str, Any]]:
            async with semaphore:
                return await replay_symbol(
                    futures,
                    symbol=symbol,
                    history_1m_limit=max(1450, int(args.history_1m_limit)),
                    history_5m_limit=max(120, int(args.history_5m_limit)),
                    history_15m_limit=max(90, int(args.history_15m_limit)),
                    horizons_sec=horizons_sec,
                    include_setups=include_setups,
                    sampling_mode=str(args.sampling_mode),
                    allow_short=bool(args.allow_short),
                    starting_min_score=float(args.starting_min_score),
                    execution_floor_offset=float(args.execution_floor_offset or 0.0),
                    feature_min_quote_volume=float(args.feature_min_quote_volume),
                    sample_step_bars=max(1, int(args.sample_step_bars)),
                    max_samples_per_symbol=max(0, int(args.max_samples_per_symbol)),
                )

        all_records: list[dict[str, Any]] = []
        for symbol, records in zip(selected_symbols, await asyncio.gather(*(one(symbol) for symbol in selected_symbols))):
            all_records.extend(records)
            emit_event(
                "signal_replay_symbol_complete",
                symbol=symbol,
                samples=len(records),
            )

    with labels_path.open("a", encoding="utf-8") as handle:
        for record in all_records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    rows = analyze_records(
        all_records,
        round_trip_fee_bps=max(0.0, float(args.round_trip_fee_bps or 0.0)),
        context_split="none",
    )
    report = {
        "generated_at": utc_now_iso(),
        "labels_file": str(labels_path),
        "record_count": len(all_records),
        "unique_symbols": len({str(record.get("symbol") or "") for record in all_records}),
        "round_trip_fee_bps": max(0.0, float(args.round_trip_fee_bps or 0.0)),
        "rows": rows,
    }
    (output_dir / "replay_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(render_analysis_table(rows, limit=max(1, int(args.analysis_limit)), show_context=False))
    emit_event(
        "signal_replay_completed",
        record_count=len(all_records),
        unique_symbols=report["unique_symbols"],
        output_dir=str(output_dir),
    )
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
