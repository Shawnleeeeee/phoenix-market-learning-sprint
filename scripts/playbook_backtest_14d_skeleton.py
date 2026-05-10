#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from phoenix_signal_bridge import derive_playbook, derive_signal_side
from phoenix_signal_lab import safe_float


DEFAULT_DAYS = 14
DEFAULT_ALLOWED_PLAYBOOKS = (
    "oi_build_breakout",
    "volume_burst_reversal",
    "liquidation_flush",
)


@dataclass(slots=True)
class BacktestSignal:
    observed_at: str
    observed_at_ms: int
    symbol: str
    playbook: str
    side: str
    trigger_score: float
    price: float
    trading_session: str
    bar_interval: str
    trigger_types: list[str]
    horizon_sec: int
    expected_return_pct: float | None
    max_drawdown_pct: float | None
    source: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal 14-day historical playbook backtest skeleton using current v7 playbook classification."
    )
    parser.add_argument("--snapshots-file", type=Path, required=True)
    parser.add_argument("--output-file", type=Path, default=None)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--allowed-playbooks", default=",".join(DEFAULT_ALLOWED_PLAYBOOKS))
    parser.add_argument("--min-trigger-score", type=float, default=0.0)
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_allowed_playbooks(raw_value: str) -> set[str]:
    allowed = {token.strip() for token in str(raw_value or "").split(",") if token.strip()}
    return allowed or set(DEFAULT_ALLOWED_PLAYBOOKS)


def resolve_observed_at(record: dict[str, Any]) -> tuple[str | None, int | None]:
    candidates = (
        record.get("observed_at"),
        record.get("ts"),
        record.get("at"),
        (record.get("sample") or {}).get("observed_at") if isinstance(record.get("sample"), dict) else None,
    )
    observed_at = next((str(value) for value in candidates if value), None)

    ms_candidates = (
        record.get("observed_at_ms"),
        record.get("event_time_ms"),
        (record.get("sample") or {}).get("observed_at_ms") if isinstance(record.get("sample"), dict) else None,
    )
    observed_at_ms = next((int(value) for value in ms_candidates if value not in (None, "")), None)

    if observed_at_ms is None and observed_at is not None:
        normalized = observed_at.replace("Z", "+00:00")
        try:
            observed_at_ms = int(datetime.fromisoformat(normalized).timestamp() * 1000)
        except ValueError:
            observed_at_ms = None
    if observed_at is None and observed_at_ms is not None:
        observed_at = datetime.fromtimestamp(observed_at_ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")
    return observed_at, observed_at_ms


def choose_horizon_sec(record: dict[str, Any], *, playbook: str) -> int:
    horizons = record.get("horizons")
    if isinstance(horizons, list) and horizons:
        values = sorted(
            {
                int(item.get("horizon_sec") or 0)
                for item in horizons
                if isinstance(item, dict) and int(item.get("horizon_sec") or 0) > 0
            }
        )
        if values:
            return values[0]
    if playbook == "oi_build_breakout":
        return 300
    if playbook == "liquidation_flush":
        return 180
    return 120


def expected_return_pct(record: dict[str, Any]) -> float | None:
    if isinstance(record.get("backtest"), dict):
        value = safe_float((record.get("backtest") or {}).get("expected_return_pct"))
        if value != 0.0 or (record.get("backtest") or {}).get("expected_return_pct") in (0, 0.0):
            return value
    horizons = record.get("horizons")
    if isinstance(horizons, list):
        for horizon in horizons:
            if not isinstance(horizon, dict):
                continue
            if horizon.get("final_return_pct") not in (None, ""):
                return safe_float(horizon.get("final_return_pct"))
    return None


def max_drawdown_pct(record: dict[str, Any]) -> float | None:
    horizons = record.get("horizons")
    if isinstance(horizons, list):
        drawdowns = [
            safe_float(horizon.get("mae_pct"))
            for horizon in horizons
            if isinstance(horizon, dict) and horizon.get("mae_pct") not in (None, "")
        ]
        if drawdowns:
            return min(drawdowns)
    return None


def normalize_signal(record: dict[str, Any]) -> BacktestSignal | None:
    playbook = derive_playbook(record)
    side = derive_signal_side(record, playbook=playbook)
    if not side:
        return None

    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    observed_at, observed_at_ms = resolve_observed_at(record)
    if not observed_at or observed_at_ms is None:
        return None

    symbol = str(record.get("symbol") or sample.get("symbol") or "").upper()
    if not symbol:
        return None

    trigger_types = list(record.get("trigger_types") or sample.get("trigger_types") or [])
    price = safe_float(sample.get("price") or record.get("price"))

    return BacktestSignal(
        observed_at=observed_at,
        observed_at_ms=observed_at_ms,
        symbol=symbol,
        playbook=playbook,
        side=side,
        trigger_score=safe_float(record.get("trigger_score")),
        price=price,
        trading_session=str(record.get("trading_session") or sample.get("trading_session") or ""),
        bar_interval=str(record.get("bar_interval") or sample.get("bar_interval") or ""),
        trigger_types=trigger_types,
        horizon_sec=choose_horizon_sec(record, playbook=playbook),
        expected_return_pct=expected_return_pct(record),
        max_drawdown_pct=max_drawdown_pct(record),
        source={
            "sample_type": record.get("sample_type") or sample.get("sample_type"),
            "enrichments": record.get("enrichments") if isinstance(record.get("enrichments"), dict) else {},
        },
    )


def load_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            payload = json.loads(raw)
            if isinstance(payload, dict):
                records.append(payload)
    return records


def summarize(signals: list[BacktestSignal], *, started_at_ms: int) -> dict[str, Any]:
    playbook_counts = Counter(signal.playbook for signal in signals)
    side_counts = Counter(signal.side for signal in signals)
    symbol_counts = Counter(signal.symbol for signal in signals)
    by_playbook_symbol: dict[str, Counter[str]] = defaultdict(Counter)
    returns_by_playbook: dict[str, list[float]] = defaultdict(list)

    for signal in signals:
        by_playbook_symbol[signal.playbook][signal.symbol] += 1
        if signal.expected_return_pct is not None:
            returns_by_playbook[signal.playbook].append(signal.expected_return_pct)

    playbooks: list[dict[str, Any]] = []
    for playbook, count in sorted(playbook_counts.items()):
        returns = returns_by_playbook.get(playbook, [])
        playbooks.append(
            {
                "playbook": playbook,
                "signals": count,
                "avg_expected_return_pct": round(sum(returns) / len(returns), 4) if returns else None,
                "top_symbols": by_playbook_symbol[playbook].most_common(5),
            }
        )

    return {
        "generated_at": utc_now().isoformat(timespec="milliseconds"),
        "window_start_ms": started_at_ms,
        "signal_count": len(signals),
        "unique_symbols": len(symbol_counts),
        "playbook_counts": dict(playbook_counts),
        "side_counts": dict(side_counts),
        "top_symbols": symbol_counts.most_common(10),
        "playbooks": playbooks,
    }


def main() -> int:
    args = parse_args()
    allowed_playbooks = parse_allowed_playbooks(args.allowed_playbooks)
    window_start = utc_now() - timedelta(days=max(1, int(args.days)))
    window_start_ms = int(window_start.timestamp() * 1000)

    records = load_records(args.snapshots_file)
    signals: list[BacktestSignal] = []
    for record in records:
        signal = normalize_signal(record)
        if signal is None:
            continue
        if signal.observed_at_ms < window_start_ms:
            continue
        if signal.playbook not in allowed_playbooks:
            continue
        if signal.trigger_score < float(args.min_trigger_score):
            continue
        signals.append(signal)
        if int(args.limit) > 0 and len(signals) >= int(args.limit):
            break

    payload = {
        "config": {
            "snapshots_file": str(args.snapshots_file),
            "days": max(1, int(args.days)),
            "allowed_playbooks": sorted(allowed_playbooks),
            "min_trigger_score": float(args.min_trigger_score),
            "limit": max(0, int(args.limit)),
        },
        "summary": summarize(signals, started_at_ms=window_start_ms),
        "signals": [asdict(signal) for signal in signals],
        "notes": [
            "Classification uses phoenix_signal_bridge.derive_playbook/derive_signal_side, which currently delegates to the v7 playbook logic in phoenix_signal_lab.event_context_label(split_by='playbook').",
            "This skeleton intentionally does not model fills, slippage, or exit rules yet.",
            "Parent agent can plug the normalized signals array into a fuller execution or PnL simulator without re-parsing raw snapshot records.",
        ],
    }

    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output_file is not None:
        args.output_file.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
