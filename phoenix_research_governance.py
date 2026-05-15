from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
import time
import urllib.request
import zipfile
from collections import deque
from datetime import date, datetime, timezone
from io import TextIOWrapper
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable

from phoenix_factor_library import build_factor_vector, safe_float
from phoenix_factor_factory import (
    build_factor_factory_control,
    build_factor_factory_report,
    compute_profit_factor,
    compute_sharpe_ratio,
    read_recent_jsonl_records,
    summarize_returns,
)


GOVERNANCE_VERSION = "v1.0"
BINANCE_PUBLIC_DATA_BASE_URL = "https://data.binance.vision"
FUTURES_UM_MARKET = "futures/um"
KLINE_COLUMNS = (
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time_ms",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
)
FUTURE_LEAKY_TOKENS = (
    "future",
    "horizon",
    "label",
    "outcome",
    "after_fee",
    "close_return",
    "max_favorable",
    "max_adverse",
    "pnl",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def round_optional(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_timestamp_ms(value: Any) -> int | None:
    number = safe_float(value)
    if number is None:
        return None
    if number > 10_000_000_000_000:
        number = number / 1000.0
    return int(number)


def nested_keys(payload: dict[str, Any], *, prefix: str = "") -> Iterable[str]:
    for key, value in payload.items():
        full_key = f"{prefix}.{key}" if prefix else str(key)
        yield full_key
        if isinstance(value, dict):
            yield from nested_keys(value, prefix=full_key)


def record_event_time_ms(record: dict[str, Any]) -> int | None:
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    for key in ("observed_at_ms", "created_at_ms", "timestamp_ms", "anchor_close_time_ms"):
        value = record.get(key) if key in record else sample.get(key)
        parsed = parse_timestamp_ms(value)
        if parsed is not None:
            return parsed
    return None


def audit_lookahead_recursive_bias(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in records if isinstance(row, dict)]
    findings: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        event_id = str(row.get("event_id") or row.get("source_event_id") or f"row-{index}")
        sample = row.get("sample") if isinstance(row.get("sample"), dict) else {}
        anchor_close = parse_timestamp_ms(sample.get("anchor_close_time_ms") or row.get("anchor_close_time_ms"))
        observed_at = parse_timestamp_ms(row.get("observed_at_ms") or sample.get("observed_at_ms"))
        if anchor_close is not None and observed_at is not None and anchor_close > observed_at:
            findings.append(
                {
                    "severity": "high",
                    "event_id": event_id,
                    "type": "timestamp_inversion",
                    "detail": "anchor_close_time_ms is later than observed_at_ms",
                    "anchor_close_time_ms": anchor_close,
                    "observed_at_ms": observed_at,
                }
            )
        if isinstance(row.get("factors"), dict):
            factor_keys = list(nested_keys(row["factors"]))
            leaky_keys = [
                key
                for key in factor_keys
                if any(token in key.lower() for token in FUTURE_LEAKY_TOKENS)
            ]
            if leaky_keys:
                findings.append(
                    {
                        "severity": "high",
                        "event_id": event_id,
                        "type": "factor_contains_label_like_field",
                        "detail": "factor payload contains fields that look like labels or future outcomes",
                        "keys": leaky_keys[:20],
                    }
                )
        try:
            computed = build_factor_vector(row)
        except Exception as exc:  # noqa: BLE001
            findings.append(
                {
                    "severity": "medium",
                    "event_id": event_id,
                    "type": "factor_recompute_failed",
                    "detail": str(exc),
                }
            )
            continue
        existing = row.get("factors") if isinstance(row.get("factors"), dict) else {}
        drift_keys = []
        for key, value in computed.items():
            existing_value = existing.get(key)
            if safe_float(value) is not None and safe_float(existing_value) is not None:
                if abs((safe_float(value) or 0.0) - (safe_float(existing_value) or 0.0)) > 1e-6:
                    drift_keys.append(key)
        if drift_keys:
            findings.append(
                {
                    "severity": "low",
                    "event_id": event_id,
                    "type": "factor_recompute_drift",
                    "detail": "stored factor values differ from deterministic recomputation",
                    "keys": drift_keys[:20],
                }
            )
    severity_counts: dict[str, int] = {}
    for finding in findings:
        severity = str(finding.get("severity") or "unknown")
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
    return {
        "generated_at": utc_now_iso(),
        "governance_version": GOVERNANCE_VERSION,
        "records_checked": len(rows),
        "finding_count": len(findings),
        "severity_counts": dict(sorted(severity_counts.items())),
        "lookahead_bias_risk": any(item.get("severity") == "high" for item in findings),
        "findings": findings[:200],
    }


def build_binance_public_kline_url(
    *,
    symbol: str,
    interval: str,
    day: date | None = None,
    year_month: str | None = None,
    market: str = FUTURES_UM_MARKET,
) -> str:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_interval = str(interval or "").strip()
    if not normalized_symbol or not normalized_interval:
        raise ValueError("symbol and interval are required")
    if (day is None) == (year_month is None):
        raise ValueError("provide exactly one of day or year_month")
    frequency = "daily" if day is not None else "monthly"
    suffix = day.isoformat() if day is not None else str(year_month)
    filename = f"{normalized_symbol}-{normalized_interval}-{suffix}.zip"
    return (
        f"{BINANCE_PUBLIC_DATA_BASE_URL}/data/{market}/{frequency}/klines/"
        f"{normalized_symbol}/{normalized_interval}/{filename}"
    )


def normalize_kline_csv_row(row: list[str], *, symbol: str, interval: str, source_url: str) -> dict[str, Any] | None:
    if len(row) < 11:
        return None
    if row[0].strip().lower() in {"open_time", "open time"}:
        return None
    values = dict(zip(KLINE_COLUMNS, row[: len(KLINE_COLUMNS)]))
    return {
        "event": "binance_public_kline_backfill",
        "source": "binance_public_data_zip",
        "source_url": source_url,
        "symbol": symbol.upper(),
        "interval": interval,
        "open_time_ms": parse_timestamp_ms(values.get("open_time_ms")),
        "open": safe_float(values.get("open")),
        "high": safe_float(values.get("high")),
        "low": safe_float(values.get("low")),
        "close": safe_float(values.get("close")),
        "volume": safe_float(values.get("volume")),
        "close_time_ms": parse_timestamp_ms(values.get("close_time_ms")),
        "quote_asset_volume": safe_float(values.get("quote_asset_volume")),
        "number_of_trades": int(safe_float(values.get("number_of_trades")) or 0),
        "taker_buy_base_asset_volume": safe_float(values.get("taker_buy_base_asset_volume")),
        "taker_buy_quote_asset_volume": safe_float(values.get("taker_buy_quote_asset_volume")),
    }


def iter_kline_rows_from_zip(zip_path: Path, *, symbol: str, interval: str, source_url: str) -> Iterable[dict[str, Any]]:
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with archive.open(name, "r") as raw_handle:
                text_handle = TextIOWrapper(raw_handle, encoding="utf-8")
                for csv_row in csv.reader(text_handle):
                    normalized = normalize_kline_csv_row(
                        csv_row,
                        symbol=symbol,
                        interval=interval,
                        source_url=source_url,
                    )
                    if normalized is not None:
                        yield normalized


def download_public_zip_to_jsonl(
    *,
    symbol: str,
    interval: str,
    output_jsonl: Path,
    day: date | None = None,
    year_month: str | None = None,
    timeout_sec: float = 60.0,
) -> dict[str, Any]:
    source_url = build_binance_public_kline_url(
        symbol=symbol,
        interval=interval,
        day=day,
        year_month=year_month,
    )
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / source_url.rsplit("/", 1)[-1]
        with urllib.request.urlopen(source_url, timeout=max(1.0, float(timeout_sec))) as response:
            zip_path.write_bytes(response.read())
        row_count = 0
        with output_jsonl.open("a", encoding="utf-8") as handle:
            for row in iter_kline_rows_from_zip(
                zip_path,
                symbol=symbol,
                interval=interval,
                source_url=source_url,
            ):
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                row_count += 1
    return {
        "source_url": source_url,
        "output_jsonl": str(output_jsonl),
        "row_count": row_count,
    }


def execution_realism_adjusted_return_pct(
    raw_return_pct: float,
    *,
    side: str,
    latency_ms: float,
    latency_drift_pct_per_sec: float,
    taker_fee_bps: float,
    maker_fee_bps: float,
    maker_fill_ratio: float,
    partial_fill_ratio: float,
    extra_slippage_bps: float,
) -> float:
    maker_ratio = max(0.0, min(1.0, float(maker_fill_ratio)))
    fill_ratio = max(0.0, min(1.0, float(partial_fill_ratio)))
    blended_fee_bps = (maker_ratio * float(maker_fee_bps)) + ((1.0 - maker_ratio) * float(taker_fee_bps))
    round_trip_fee_pct = (blended_fee_bps * 2.0) / 100.0
    latency_penalty_pct = max(0.0, float(latency_ms) / 1000.0) * max(0.0, float(latency_drift_pct_per_sec))
    slippage_penalty_pct = max(0.0, float(extra_slippage_bps)) / 100.0
    unfilled_penalty_pct = abs(float(raw_return_pct)) * (1.0 - fill_ratio)
    return float(raw_return_pct) - round_trip_fee_pct - latency_penalty_pct - slippage_penalty_pct - unfilled_penalty_pct


def build_execution_realism_report(
    outcomes: Iterable[dict[str, Any]],
    *,
    latency_ms: float = 750.0,
    latency_drift_pct_per_sec: float = 0.05,
    taker_fee_bps: float = 4.0,
    maker_fee_bps: float = 2.0,
    maker_fill_ratio: float = 0.0,
    partial_fill_ratio: float = 1.0,
    extra_slippage_bps: float = 5.0,
) -> dict[str, Any]:
    adjusted_returns: list[float] = []
    raw_returns: list[float] = []
    rows_checked = 0
    for row in outcomes:
        raw_return = (
            safe_float(row.get("after_fee_and_slippage_return_pct"))
            if row.get("after_fee_and_slippage_return_pct") is not None
            else safe_float(row.get("after_fee_return_pct"))
        )
        if raw_return is None:
            raw_return = safe_float(row.get("close_return_pct"))
        if raw_return is None:
            continue
        rows_checked += 1
        raw_returns.append(raw_return)
        adjusted_returns.append(
            execution_realism_adjusted_return_pct(
                raw_return,
                side=str(row.get("side") or ""),
                latency_ms=latency_ms,
                latency_drift_pct_per_sec=latency_drift_pct_per_sec,
                taker_fee_bps=taker_fee_bps,
                maker_fee_bps=maker_fee_bps,
                maker_fill_ratio=maker_fill_ratio,
                partial_fill_ratio=partial_fill_ratio,
                extra_slippage_bps=extra_slippage_bps,
            )
        )
    return {
        "generated_at": utc_now_iso(),
        "governance_version": GOVERNANCE_VERSION,
        "assumptions": {
            "latency_ms": float(latency_ms),
            "latency_drift_pct_per_sec": float(latency_drift_pct_per_sec),
            "taker_fee_bps": float(taker_fee_bps),
            "maker_fee_bps": float(maker_fee_bps),
            "maker_fill_ratio": max(0.0, min(1.0, float(maker_fill_ratio))),
            "partial_fill_ratio": max(0.0, min(1.0, float(partial_fill_ratio))),
            "extra_slippage_bps": float(extra_slippage_bps),
        },
        "rows_checked": rows_checked,
        "raw": summarize_returns(raw_returns),
        "execution_realistic": summarize_returns(adjusted_returns),
        "delta_avg_return_pct": round_optional(
            (sum(adjusted_returns) / len(adjusted_returns) - sum(raw_returns) / len(raw_returns))
            if adjusted_returns and raw_returns
            else None
        ),
    }


def append_experiment_ledger_entry(
    jsonl_path: Path,
    *,
    experiment_name: str,
    params: dict[str, Any],
    metrics: dict[str, Any],
    data_window: dict[str, Any] | None = None,
    artifacts: dict[str, str] | None = None,
    sqlite_path: Path | None = None,
) -> dict[str, Any]:
    entry = {
        "event": "phoenix_experiment_recorded",
        "recorded_at": utc_now_iso(),
        "governance_version": GOVERNANCE_VERSION,
        "experiment_name": experiment_name,
        "params": params,
        "metrics": metrics,
        "data_window": data_window or {},
        "artifacts": artifacts or {},
    }
    entry["experiment_id"] = sha256_text(json.dumps(entry, ensure_ascii=False, sort_keys=True))[:16]
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    if sqlite_path is not None:
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(sqlite_path)
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS experiment_ledger (
                    experiment_id TEXT PRIMARY KEY,
                    recorded_at TEXT NOT NULL,
                    experiment_name TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT OR REPLACE INTO experiment_ledger VALUES (?, ?, ?, ?)",
                (
                    entry["experiment_id"],
                    entry["recorded_at"],
                    experiment_name,
                    json.dumps(entry, ensure_ascii=False, sort_keys=True),
                ),
            )
            connection.commit()
        finally:
            connection.close()
    return entry


def extract_backtest_gate_metrics(backtest_report: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(backtest_report, dict):
        return None
    if isinstance(backtest_report.get("promotion_metrics"), dict):
        source = backtest_report["promotion_metrics"]
    elif isinstance(backtest_report.get("portfolio_summary"), dict):
        source = backtest_report["portfolio_summary"]
    else:
        source = backtest_report
    trade_count = safe_float(
        source.get("trade_count")
        if source.get("trade_count") is not None
        else source.get("accepted_trade_count")
        if source.get("accepted_trade_count") is not None
        else source.get("sample_count")
    )
    avg_return = safe_float(
        source.get("avg_return_pct")
        if source.get("avg_return_pct") is not None
        else source.get("avg_after_fee_return_pct")
    )
    metrics = {
        "status": backtest_report.get("status"),
        "trade_count": int(trade_count or 0),
        "sample_count": int(trade_count or 0),
        "avg_return_pct": round_optional(avg_return),
        "profit_factor": round_optional(safe_float(source.get("profit_factor"))),
        "win_rate_pct": round_optional(safe_float(source.get("win_rate_pct"))),
        "sharpe_ratio": round_optional(safe_float(source.get("sharpe_ratio"))),
        "max_drawdown_pct": round_optional(safe_float(source.get("max_drawdown_pct"))),
        "total_return_pct": round_optional(safe_float(source.get("total_return_pct"))),
        "total_pnl_usdt": round_optional(safe_float(source.get("total_pnl_usdt"))),
        "source": backtest_report.get("source") or backtest_report.get("report_type"),
        "generated_at": backtest_report.get("generated_at"),
    }
    return metrics


def promotion_gate_decision(
    *,
    factor_control: dict[str, Any] | None,
    shadow_readiness: dict[str, Any] | None,
    execution_realism: dict[str, Any] | None,
    bias_audit: dict[str, Any] | None,
    backtest_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    focus_rules = []
    if factor_control and isinstance(factor_control.get("rules"), dict):
        focus_rules = [
            name
            for name, row in factor_control["rules"].items()
            if isinstance(row, dict) and row.get("status") == "focus_shadow"
        ]
    if not focus_rules:
        blockers.append("no_factor_rule_in_focus_shadow")
    if shadow_readiness:
        if not shadow_readiness.get("live_unlock_candidate"):
            blockers.append("shadow_readiness_not_met")
    else:
        blockers.append("missing_shadow_readiness")
    if bias_audit and bias_audit.get("lookahead_bias_risk"):
        blockers.append("lookahead_bias_risk_detected")
    if execution_realism:
        realistic = execution_realism.get("execution_realistic") if isinstance(execution_realism.get("execution_realistic"), dict) else {}
        if (safe_float(realistic.get("avg_return_pct")) or 0.0) <= 0:
            blockers.append("execution_realistic_expectancy_not_positive")
        if (safe_float(realistic.get("profit_factor")) or 0.0) < 1.2:
            warnings.append("execution_realistic_profit_factor_below_1_2")
    else:
        warnings.append("missing_execution_realism_report")
    backtest_metrics = extract_backtest_gate_metrics(backtest_report)
    if backtest_metrics is None:
        blockers.append("missing_backtest_report")
    else:
        if int(backtest_metrics.get("trade_count") or 0) < 1000:
            blockers.append("backtest_sample_size_below_1000")
        profit_factor = safe_float(backtest_metrics.get("profit_factor"))
        avg_return = safe_float(backtest_metrics.get("avg_return_pct"))
        if profit_factor is None:
            blockers.append("backtest_profit_factor_missing")
        elif profit_factor < 1.2:
            blockers.append("backtest_profit_factor_below_1_2")
        if avg_return is None:
            blockers.append("backtest_avg_return_missing")
        elif avg_return <= 0:
            blockers.append("backtest_avg_return_not_positive")
        sharpe_ratio = safe_float(backtest_metrics.get("sharpe_ratio"))
        if sharpe_ratio is not None and sharpe_ratio < 1.0:
            warnings.append("backtest_sharpe_below_1")
        win_rate = safe_float(backtest_metrics.get("win_rate_pct"))
        if win_rate is not None and win_rate < 50.0:
            warnings.append("backtest_win_rate_below_50")
    return {
        "generated_at": utc_now_iso(),
        "governance_version": GOVERNANCE_VERSION,
        "mode": "MAINNET_SHADOW",
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "promotion_candidate": len(blockers) == 0,
        "focus_factor_rules": focus_rules,
        "backtest_metrics": backtest_metrics,
        "blockers": blockers,
        "warnings": warnings,
        "policy": "never enable real mainnet trading automatically; this gate only recommends manual review",
    }


def read_json_file(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phoenix-native lightweight research governance utilities.")
    sub = parser.add_subparsers(dest="command", required=True)
    audit = sub.add_parser("audit-bias")
    audit.add_argument("--input-jsonl", type=Path, required=True)
    audit.add_argument("--output-json", type=Path, required=True)
    audit.add_argument("--max-records", type=int, default=5000)

    backfill = sub.add_parser("backfill-klines")
    backfill.add_argument("--symbol", required=True)
    backfill.add_argument("--interval", required=True)
    backfill.add_argument("--day", default="")
    backfill.add_argument("--year-month", default="")
    backfill.add_argument("--output-jsonl", type=Path, required=True)

    realism = sub.add_parser("execution-realism")
    realism.add_argument("--outcomes-file", type=Path, required=True)
    realism.add_argument("--output-json", type=Path, required=True)
    realism.add_argument("--max-outcomes", type=int, default=5000)
    realism.add_argument("--latency-ms", type=float, default=750.0)
    realism.add_argument("--extra-slippage-bps", type=float, default=5.0)
    realism.add_argument("--partial-fill-ratio", type=float, default=1.0)
    realism.add_argument("--maker-fill-ratio", type=float, default=0.0)

    gate = sub.add_parser("promotion-gate")
    gate.add_argument("--factor-control-json", type=Path, default=None)
    gate.add_argument("--shadow-readiness-json", type=Path, default=None)
    gate.add_argument("--execution-realism-json", type=Path, default=None)
    gate.add_argument("--bias-audit-json", type=Path, default=None)
    gate.add_argument("--backtest-json", type=Path, default=None)
    gate.add_argument("--output-json", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "audit-bias":
        report = audit_lookahead_recursive_bias(
            read_recent_jsonl_records(args.input_jsonl, max_records=max(0, int(args.max_records))),
        )
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"bias_audit findings={report['finding_count']} output_json={args.output_json}", flush=True)
        return 0
    if args.command == "backfill-klines":
        parsed_day = date.fromisoformat(args.day) if args.day else None
        year_month = args.year_month or None
        result = download_public_zip_to_jsonl(
            symbol=args.symbol,
            interval=args.interval,
            output_jsonl=args.output_jsonl,
            day=parsed_day,
            year_month=year_month,
        )
        print(f"backfill rows={result['row_count']} output_jsonl={args.output_jsonl}", flush=True)
        return 0
    if args.command == "execution-realism":
        report = build_execution_realism_report(
            read_recent_jsonl_records(args.outcomes_file, max_records=max(0, int(args.max_outcomes))),
            latency_ms=float(args.latency_ms),
            extra_slippage_bps=float(args.extra_slippage_bps),
            partial_fill_ratio=float(args.partial_fill_ratio),
            maker_fill_ratio=float(args.maker_fill_ratio),
        )
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"execution_realism rows={report['rows_checked']} output_json={args.output_json}", flush=True)
        return 0
    if args.command == "promotion-gate":
        report = promotion_gate_decision(
            factor_control=read_json_file(args.factor_control_json),
            shadow_readiness=read_json_file(args.shadow_readiness_json),
            execution_realism=read_json_file(args.execution_realism_json),
            bias_audit=read_json_file(args.bias_audit_json),
            backtest_report=read_json_file(args.backtest_json),
        )
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"promotion_gate candidate={report['promotion_candidate']} output_json={args.output_json}", flush=True)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
