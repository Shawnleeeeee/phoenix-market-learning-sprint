from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


LEARNING_GATE_VERSION = "v1.0"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class LearningGateConfig:
    recent_trade_window: int = 8
    min_recent_trades: int = 3
    min_recent_win_rate_pct: float = 20.0
    max_median_slippage_bps: float = 25.0
    max_recent_reject_rate_pct: float = 35.0
    quarantine_score_multiplier: float = 0.0
    downweight_score_multiplier: float = 0.65
    quarantine_symbol_setups: set[tuple[str, str]] = field(
        default_factory=lambda: {
            ("XRPUSDT", "trend_pullback_long"),
            ("UBUSDT", "volatility_long"),
            ("AIOTUSDT", "volatility_long"),
        }
    )
    quarantine_symbols: set[str] = field(default_factory=lambda: {"BTCDOMUSDT", "FXSUSDT"})


def normalized_symbol_setup(row: Any) -> tuple[str, str]:
    if isinstance(row, dict):
        symbol = row.get("symbol")
        setup = row.get("setup") or row.get("strategy_id")
    else:
        symbol = getattr(row, "symbol", None)
        setup = getattr(row, "setup", None) or getattr(row, "strategy_id", None)
    return str(symbol or "UNKNOWN").upper(), str(setup or "unknown")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def load_recent_learning_records(output_dir: Path, learning_store_file: Path | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if learning_store_file is not None:
        rows.extend(read_jsonl(learning_store_file))
    else:
        rows.extend(read_jsonl(output_dir / "learning_store.jsonl"))
    for path in sorted(output_dir.glob("round_*_trades.jsonl")):
        rows.extend(read_jsonl(path))
    return rows


def _matching_recent_records(
    candidate: Any,
    recent_records: Iterable[dict[str, Any]],
    *,
    window: int,
) -> list[dict[str, Any]]:
    symbol, setup = normalized_symbol_setup(candidate)
    matches = [
        row
        for row in recent_records
        if normalized_symbol_setup(row) == (symbol, setup)
    ]
    return matches[-max(1, window):]


def _pct(count: int, total: int) -> float:
    return (count / total) * 100.0 if total else 0.0


def build_learning_gate_decision(
    candidate: Any,
    *,
    recent_records: Iterable[dict[str, Any]],
    config: LearningGateConfig | None = None,
) -> dict[str, Any]:
    cfg = config or LearningGateConfig()
    symbol, setup = normalized_symbol_setup(candidate)
    direct_quarantine = (symbol, setup) in cfg.quarantine_symbol_setups
    symbol_quarantine = symbol in cfg.quarantine_symbols
    if direct_quarantine or symbol_quarantine:
        return {
            "learning_gate_version": LEARNING_GATE_VERSION,
            "decision": "block",
            "reason": "quarantined_symbol_setup" if direct_quarantine else "quarantined_high_slippage_symbol",
            "symbol": symbol,
            "setup": setup,
            "score_multiplier": cfg.quarantine_score_multiplier,
            "recent_trade_count": 0,
            "win_rate_pct": 0.0,
            "avg_net_pnl_usdt": 0.0,
            "median_slippage_bps": 0.0,
            "reject_rate_pct": 0.0,
            "live_trading_enabled": False,
            "mainnet_live_promotion_allowed": False,
        }

    recent = _matching_recent_records(candidate, recent_records, window=cfg.recent_trade_window)
    pnls = [safe_float(row.get("net_pnl_usdt")) for row in recent]
    win_rate = _pct(sum(1 for pnl in pnls if pnl > 0), len(pnls))
    avg_net = sum(pnls) / len(pnls) if pnls else 0.0
    slippages = [safe_float(row.get("estimated_slippage_bps")) for row in recent if row.get("estimated_slippage_bps") is not None]
    median_slippage = statistics.median(slippages) if slippages else 0.0
    reject_count = sum(1 for row in recent if row.get("reject") or "reject" in str(row.get("exit_reason") or "").lower())
    reject_rate = _pct(reject_count, len(recent))

    decision = "allow"
    reason = "passes_learning_gate"
    multiplier = 1.0
    if len(recent) >= cfg.min_recent_trades and median_slippage > cfg.max_median_slippage_bps:
        decision = "block"
        reason = "rolling_high_slippage"
        multiplier = cfg.quarantine_score_multiplier
    elif len(recent) >= cfg.min_recent_trades and reject_rate > cfg.max_recent_reject_rate_pct:
        decision = "block"
        reason = "rolling_order_rejects"
        multiplier = cfg.quarantine_score_multiplier
    elif len(recent) >= cfg.min_recent_trades and win_rate < cfg.min_recent_win_rate_pct and avg_net < 0:
        decision = "block"
        reason = "rolling_bad_combo"
        multiplier = cfg.quarantine_score_multiplier
    elif len(recent) >= cfg.min_recent_trades and avg_net < 0:
        decision = "downweight"
        reason = "rolling_negative_expectancy"
        multiplier = cfg.downweight_score_multiplier

    return {
        "learning_gate_version": LEARNING_GATE_VERSION,
        "decision": decision,
        "reason": reason,
        "symbol": symbol,
        "setup": setup,
        "score_multiplier": multiplier,
        "recent_trade_count": len(recent),
        "win_rate_pct": round(win_rate, 6),
        "avg_net_pnl_usdt": round(avg_net, 6),
        "median_slippage_bps": round(median_slippage, 6),
        "reject_rate_pct": round(reject_rate, 6),
        "live_trading_enabled": False,
        "mainnet_live_promotion_allowed": False,
    }
