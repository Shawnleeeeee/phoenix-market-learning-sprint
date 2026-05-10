#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, TypeVar

import aiohttp

from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import load_proxy_settings, resolve_environment
from phoenix_feature_slice_report import build_feature_slice_report, read_jsonl_records
from phoenix_signal_bridge import (
    BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
    DEFAULT_OI_UNWIND_REVERSAL_TARGET_HORIZONS_SEC,
    DEFAULT_SHADOW_ROUND_TRIP_FEE_BPS,
    PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
    SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE,
    build_oi_unwind_reversal_branch_specs,
    candle_direction_from_kline,
    fetch_shadow_path_candles,
    is_oi_unwind_reversal_base,
    safe_float,
    side_from_reversal_trigger_direction,
    simulate_shadow_branch_outcome,
    trigger_candle_direction,
)


OUTCOME_EVENT_NAME = "signal_bridge_shadow_horizon_result"
SIGNAL_EVENT_NAME = "signal_bridge_shadow_logged"
SNAPSHOT_EVENT_NAME = "market_event_created"
MAX_RATE_LIMIT_ATTEMPTS = 6
RATE_LIMIT_ERROR_CODES = {-1003}
T = TypeVar("T")


@dataclass(slots=True)
class ReplayRunCounts:
    snapshots_seen: int = 0
    base_candidates: int = 0
    confirmed_candidates: int = 0
    rejected_no_confirmation: int = 0
    fetch_errors: int = 0
    outcomes_written: int = 0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_event(event: str, **payload: Any) -> None:
    print(json.dumps({"ts": utc_now_iso(), "event": event, **payload}, ensure_ascii=False), flush=True)


def event_matches(row: dict[str, Any], expected: str) -> bool:
    event_name = str(row.get("event") or "").strip()
    return not event_name or event_name == expected


def parse_horizons(raw_value: str | Iterable[int]) -> list[int]:
    if isinstance(raw_value, str):
        tokens = [item.strip() for item in raw_value.split(",")]
    else:
        tokens = [str(item).strip() for item in raw_value]
    horizons = sorted({int(token) for token in tokens if token and int(token) > 0})
    if not horizons:
        raise ValueError("At least one positive horizon is required.")
    return horizons


def iter_jsonl_records(path: Path, *, max_records: int = 0) -> Iterable[dict[str, Any]]:
    seen = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw_line = line.strip()
            if not raw_line:
                continue
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            yield payload
            seen += 1
            if max_records > 0 and seen >= max_records:
                break


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


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
                "oi_unwind_replay_rate_limit",
                label=label,
                attempt=attempt,
                retry_delay_sec=round(delay_sec, 4),
                status=exc.status,
                code=exc.code,
            )
            await asyncio.sleep(delay_sec)
            delay_sec = min(delay_sec * 2.0, 30.0)
    raise RuntimeError(f"Rate-limit retry loop exhausted for {label}.")


def find_oi_unwind_confirmation_from_klines(
    record: dict[str, Any],
    candles_1m: Iterable[list[Any]],
    *,
    max_confirmation_wait_ms: int = 4 * 60_000,
) -> dict[str, Any] | None:
    if not is_oi_unwind_reversal_base(record):
        return None
    trigger_direction = trigger_candle_direction(record)
    side = side_from_reversal_trigger_direction(trigger_direction)
    if side not in {"BUY", "SELL"}:
        return None
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    anchor_close_time_ms = int(safe_float(sample.get("anchor_close_time_ms")) or 0)
    if anchor_close_time_ms <= 0:
        return None
    max_close_time_ms = anchor_close_time_ms + max(60_000, int(max_confirmation_wait_ms))
    for row in candles_1m:
        if not isinstance(row, list) or len(row) < 7:
            continue
        close_time_ms = int(safe_float(row[6]) or 0)
        if close_time_ms <= anchor_close_time_ms:
            continue
        if close_time_ms > max_close_time_ms:
            return None
        confirmation_direction = candle_direction_from_kline(row)
        if (
            (trigger_direction == "down" and confirmation_direction == "up")
            or (trigger_direction == "up" and confirmation_direction == "down")
        ):
            return {
                "side": side,
                "trigger_direction": trigger_direction,
                "confirmation_source": "offline_1m_kline",
                "confirmation_candle_direction": confirmation_direction,
                "confirmation_close_time_ms": close_time_ms,
                "confirmation_wait_ms": max(0, close_time_ms - anchor_close_time_ms),
                "entry_price": safe_float(row[4]),
            }
        if confirmation_direction in {"up", "down"}:
            return None
    return None


async def fetch_oi_unwind_confirmation(
    *,
    futures: BinanceFuturesClient,
    record: dict[str, Any],
    max_confirmation_wait_ms: int,
) -> dict[str, Any] | None:
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    anchor_close_time_ms = int(safe_float(sample.get("anchor_close_time_ms")) or 0)
    if anchor_close_time_ms <= 0:
        return None
    candles = await call_with_backoff(
        lambda: futures.klines(
            str(record.get("symbol") or "").upper(),
            interval="1m",
            start_time_ms=anchor_close_time_ms + 1,
            end_time_ms=anchor_close_time_ms + max(60_000, int(max_confirmation_wait_ms)),
            limit=8,
        ),
        label=f"confirmation_1m:{record.get('symbol')}",
    )
    return find_oi_unwind_confirmation_from_klines(
        record,
        candles,
        max_confirmation_wait_ms=max_confirmation_wait_ms,
    )


def replay_event_id(record: dict[str, Any]) -> str:
    source_event_id = str(record.get("event_id") or "").strip()
    if source_event_id:
        return f"{source_event_id}::{BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE}"
    symbol = str(record.get("symbol") or "UNKNOWN").upper()
    return f"{symbol}::{BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE}"


def build_replay_signal(
    *,
    record: dict[str, Any],
    confirmation: dict[str, Any],
    horizons_sec: Iterable[int],
    quote_allocation_usdt: float,
) -> dict[str, Any]:
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    enrichments = record.get("enrichments") if isinstance(record.get("enrichments"), dict) else {}
    side = str(confirmation.get("side") or "").upper()
    source_event_id = str(record.get("event_id") or "").strip()
    return {
        "event": SIGNAL_EVENT_NAME,
        "recorded_at": utc_now_iso(),
        "event_id": replay_event_id(record),
        "source_event_id": source_event_id,
        "symbol": str(record.get("symbol") or "").upper(),
        "playbook": PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
        "side": side,
        "branch_type": BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
        "research_only": True,
        "shadow_only": True,
        "live_unlock_eligible": False,
        "exchange_preflight_requested": False,
        "shadow_reason": SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE,
        "candidate_rule": PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
        "candidate_source": "offline_feature_slice_5m_oi_unwind_1m_confirmation",
        "sample_price": confirmation.get("entry_price"),
        "trigger_sample_price": sample.get("price"),
        "entry_time_ms": confirmation.get("confirmation_close_time_ms"),
        "bar_interval": record.get("bar_interval") or sample.get("bar_interval"),
        "trigger_types": record.get("trigger_types") or sample.get("trigger_types"),
        "trading_session": record.get("trading_session") or sample.get("trading_session"),
        "effective_quote_allocation_usdt": max(0.0, float(quote_allocation_usdt or 0.0)),
        "shadow_target_horizons_sec": list(parse_horizons(horizons_sec)),
        "shadow_branches": build_oi_unwind_reversal_branch_specs(side),
        "oi_change_5m_pct": enrichments.get("oi_change_5m_pct"),
        "oi_change_15m_pct": enrichments.get("oi_change_15m_pct"),
        "confirmation_source": confirmation.get("confirmation_source"),
        "confirmation_candle_direction": confirmation.get("confirmation_candle_direction"),
        "confirmation_close_time_ms": confirmation.get("confirmation_close_time_ms"),
        "confirmation_wait_ms": confirmation.get("confirmation_wait_ms"),
    }


def fallback_close_price(candles: list[list[Any]]) -> float | None:
    for row in reversed(candles):
        if isinstance(row, list) and len(row) >= 5:
            value = safe_float(row[4])
            if value is not None:
                return value
    return None


def build_replay_outcomes(
    *,
    record: dict[str, Any],
    confirmation: dict[str, Any],
    path_candles_by_horizon: dict[int, list[list[Any]]],
    horizons_sec: Iterable[int],
    round_trip_fee_bps: float,
    extra_slippage_penalty_pct: float,
    quote_allocation_usdt: float,
) -> list[dict[str, Any]]:
    side = str(confirmation.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return []
    entry_price = safe_float(confirmation.get("entry_price")) or 0.0
    entry_time_ms = int(safe_float(confirmation.get("confirmation_close_time_ms")) or 0)
    if entry_price <= 0 or entry_time_ms <= 0:
        return []
    signal_event_id = replay_event_id(record)
    source_event_id = str(record.get("event_id") or "").strip()
    rows: list[dict[str, Any]] = []
    for horizon_sec in parse_horizons(horizons_sec):
        candles = path_candles_by_horizon.get(horizon_sec) or []
        if not candles:
            continue
        for branch in build_oi_unwind_reversal_branch_specs(side):
            simulated = simulate_shadow_branch_outcome(
                event_id=signal_event_id,
                branch=branch,
                side=side,
                entry_price=entry_price,
                entry_time_ms=entry_time_ms,
                horizon_sec=horizon_sec,
                candles_1m=candles,
                fallback_close_price=fallback_close_price(candles),
                fallback_max_drawdown_pct=None,
                fallback_max_runup_pct=None,
                round_trip_fee_bps=round_trip_fee_bps,
                extra_slippage_penalty_pct=extra_slippage_penalty_pct,
            )
            rows.append(
                {
                    "event": OUTCOME_EVENT_NAME,
                    "recorded_at": utc_now_iso(),
                    "event_id": signal_event_id,
                    "source_event_id": source_event_id,
                    "symbol": str(record.get("symbol") or "").upper(),
                    "playbook": PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
                    "side": side,
                    "base_side": side,
                    "direction_variant": branch.get("direction_variant"),
                    "branch_type": BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
                    "research_only": True,
                    "shadow_only": True,
                    "live_unlock_eligible": False,
                    "shadow_reason": SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE,
                    "effective_quote_allocation_usdt": max(0.0, float(quote_allocation_usdt or 0.0)),
                    "horizon_sec": horizon_sec,
                    **simulated,
                }
            )
    return rows


async def replay_candidate(
    *,
    futures: BinanceFuturesClient,
    record: dict[str, Any],
    horizons_sec: Iterable[int],
    max_confirmation_wait_ms: int,
    round_trip_fee_bps: float,
    extra_slippage_penalty_pct: float,
    quote_allocation_usdt: float,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    confirmation = await fetch_oi_unwind_confirmation(
        futures=futures,
        record=record,
        max_confirmation_wait_ms=max_confirmation_wait_ms,
    )
    if confirmation is None:
        return None, []
    entry_time_ms = int(safe_float(confirmation.get("confirmation_close_time_ms")) or 0)
    path_candles_by_horizon: dict[int, list[list[Any]]] = {}
    parsed_horizons = parse_horizons(horizons_sec)
    max_deadline_ms = entry_time_ms + max(parsed_horizons) * 1000
    max_horizon_candles = await call_with_backoff(
        lambda: fetch_shadow_path_candles(
            futures=futures,
            symbol=str(record.get("symbol") or "").upper(),
            entry_time_ms=entry_time_ms,
            deadline_ms=max_deadline_ms,
        ),
        label=f"path_1m:{record.get('symbol')}:max",
    )
    for horizon_sec in parsed_horizons:
        deadline_ms = entry_time_ms + horizon_sec * 1000
        path_candles_by_horizon[horizon_sec] = [
            row
            for row in max_horizon_candles
            if isinstance(row, list) and len(row) >= 7 and int(safe_float(row[6]) or 0) <= deadline_ms
        ]
    signal = build_replay_signal(
        record=record,
        confirmation=confirmation,
        horizons_sec=horizons_sec,
        quote_allocation_usdt=quote_allocation_usdt,
    )
    outcomes = build_replay_outcomes(
        record=record,
        confirmation=confirmation,
        path_candles_by_horizon=path_candles_by_horizon,
        horizons_sec=horizons_sec,
        round_trip_fee_bps=round_trip_fee_bps,
        extra_slippage_penalty_pct=extra_slippage_penalty_pct,
        quote_allocation_usdt=quote_allocation_usdt,
    )
    return signal, outcomes


def is_candidate_signal(row: dict[str, Any]) -> bool:
    return (
        event_matches(row, SIGNAL_EVENT_NAME)
        and str(row.get("branch_type") or "").strip().lower() == BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE
    )


def is_candidate_outcome(row: dict[str, Any]) -> bool:
    return (
        event_matches(row, OUTCOME_EVENT_NAME)
        and str(row.get("branch_type") or "").strip().lower() == BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE
    )


def build_oi_unwind_replay_report(
    *,
    snapshots: Iterable[dict[str, Any]],
    signals: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    min_samples: int = 3,
    pair_min_samples: int = 3,
    top_n: int = 25,
) -> dict[str, Any]:
    snapshot_rows = list(snapshots)
    signal_rows = [row for row in signals if is_candidate_signal(row)]
    outcome_rows = [row for row in outcomes if is_candidate_outcome(row)]
    branch_counts = Counter(str(row.get("shadow_branch_id") or "UNKNOWN").upper() for row in outcome_rows)
    horizon_counts = Counter(str(int(safe_float(row.get("horizon_sec")) or 0)) for row in outcome_rows)
    feature_report = build_feature_slice_report(
        snapshot_rows,
        outcome_rows,
        branch_type=BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
        min_samples=min_samples,
        pair_min_samples=pair_min_samples,
        top_n=top_n,
        include_non_research=True,
    )
    return {
        "generated_at": utc_now_iso(),
        "candidate": PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
        "branch_type": BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
        "input_counts": {
            "snapshots": len(snapshot_rows),
            "candidate_signals": len(signal_rows),
            "candidate_outcomes": len(outcome_rows),
            "candidate_outcomes_by_branch": dict(sorted(branch_counts.items())),
            "candidate_outcomes_by_horizon": dict(sorted(horizon_counts.items())),
        },
        "feature_slice_report": feature_report,
    }


async def run_replay(args: argparse.Namespace) -> ReplayRunCounts:
    horizons_sec = parse_horizons(args.horizons_sec)
    counts = ReplayRunCounts()
    if args.output_signals:
        args.output_signals.parent.mkdir(parents=True, exist_ok=True)
        args.output_signals.write_text("", encoding="utf-8")
    if args.output_outcomes:
        args.output_outcomes.parent.mkdir(parents=True, exist_ok=True)
        args.output_outcomes.write_text("", encoding="utf-8")
    timeout = aiohttp.ClientTimeout(total=90, sock_connect=15, sock_read=60)
    environment = resolve_environment(args.env)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        futures = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=None,
            proxy_settings=load_proxy_settings(),
        )
        for record in iter_jsonl_records(args.snapshots_file, max_records=max(0, int(args.max_snapshots))):
            if not event_matches(record, SNAPSHOT_EVENT_NAME):
                continue
            counts.snapshots_seen += 1
            if not is_oi_unwind_reversal_base(record):
                continue
            counts.base_candidates += 1
            if args.max_candidates > 0 and counts.base_candidates > args.max_candidates:
                break
            try:
                signal, outcomes = await replay_candidate(
                    futures=futures,
                    record=record,
                    horizons_sec=horizons_sec,
                    max_confirmation_wait_ms=max(60_000, int(args.confirmation_max_wait_ms)),
                    round_trip_fee_bps=max(0.0, float(args.round_trip_fee_bps)),
                    extra_slippage_penalty_pct=max(0.0, float(args.extra_slippage_penalty_pct)),
                    quote_allocation_usdt=max(0.0, float(args.quote_allocation_usdt)),
                )
            except Exception as exc:  # noqa: BLE001
                counts.fetch_errors += 1
                emit_event(
                    "oi_unwind_replay_candidate_error",
                    event_id=record.get("event_id"),
                    symbol=record.get("symbol"),
                    error=str(exc),
                )
                continue
            if signal is None:
                counts.rejected_no_confirmation += 1
                continue
            counts.confirmed_candidates += 1
            if args.output_signals:
                append_jsonl(args.output_signals, signal)
            for outcome in outcomes:
                if args.output_outcomes:
                    append_jsonl(args.output_outcomes, outcome)
            counts.outcomes_written += len(outcomes)
    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay and report the oi_unwind_reversal_confirmed candidate using 5m OI unwind plus 1m confirmation."
    )
    parser.add_argument("--snapshots-file", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-signals", type=Path, default=None)
    parser.add_argument("--output-outcomes", type=Path, default=None)
    parser.add_argument("--existing-signals-file", type=Path, default=None)
    parser.add_argument("--existing-outcomes-file", type=Path, default=None)
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--env", default="prod", choices=["prod", "testnet", "demo"])
    parser.add_argument(
        "--horizons-sec",
        default=",".join(str(item) for item in DEFAULT_OI_UNWIND_REVERSAL_TARGET_HORIZONS_SEC),
    )
    parser.add_argument("--confirmation-max-wait-ms", type=int, default=4 * 60_000)
    parser.add_argument("--round-trip-fee-bps", type=float, default=DEFAULT_SHADOW_ROUND_TRIP_FEE_BPS)
    parser.add_argument("--extra-slippage-penalty-pct", type=float, default=0.0)
    parser.add_argument("--quote-allocation-usdt", type=float, default=10.0)
    parser.add_argument("--max-snapshots", type=int, default=0)
    parser.add_argument("--max-candidates", type=int, default=0)
    parser.add_argument("--min-samples", type=int, default=3)
    parser.add_argument("--pair-min-samples", type=int, default=3)
    parser.add_argument("--top", type=int, default=25)
    return parser.parse_args(argv)


def load_optional_records(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    return read_jsonl_records(path)


async def async_main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.report_only:
        if args.existing_outcomes_file is None:
            raise SystemExit("--report-only requires --existing-outcomes-file")
    elif args.output_signals is None or args.output_outcomes is None:
        raise SystemExit("Replay mode requires --output-signals and --output-outcomes")
    run_counts: ReplayRunCounts | None = None
    if not args.report_only:
        run_counts = await run_replay(args)

    snapshots = read_jsonl_records(args.snapshots_file)
    signals = load_optional_records(args.existing_signals_file)
    outcomes = load_optional_records(args.existing_outcomes_file)
    if args.output_signals:
        signals.extend(load_optional_records(args.output_signals))
    if args.output_outcomes:
        outcomes.extend(load_optional_records(args.output_outcomes))
    report = build_oi_unwind_replay_report(
        snapshots=snapshots,
        signals=signals,
        outcomes=outcomes,
        min_samples=max(1, int(args.min_samples)),
        pair_min_samples=max(1, int(args.pair_min_samples)),
        top_n=max(1, int(args.top)),
    )
    if run_counts is not None:
        report["replay_run_counts"] = {
            "snapshots_seen": run_counts.snapshots_seen,
            "base_candidates": run_counts.base_candidates,
            "confirmed_candidates": run_counts.confirmed_candidates,
            "rejected_no_confirmation": run_counts.rejected_no_confirmation,
            "fetch_errors": run_counts.fetch_errors,
            "outcomes_written": run_counts.outcomes_written,
        }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    counts = report["input_counts"]
    print(
        "oi_unwind_replay_report "
        f"candidate_signals={counts['candidate_signals']} "
        f"candidate_outcomes={counts['candidate_outcomes']} "
        f"records_built={report['feature_slice_report']['input_counts']['records_built']} "
        f"output={args.output_json}",
        flush=True,
    )
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
