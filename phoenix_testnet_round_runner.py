#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import signal
import statistics
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

import aiohttp

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import BinanceCredentials, load_credentials, load_execution_settings, load_proxy_settings, resolve_environment
from phoenix.executor import PhoenixExecutor
from phoenix.safe_order_gateway import build_gateway_snapshot, submit_binance_order_intent
from phoenix_learning_gate import LearningGateConfig, build_learning_gate_decision, load_recent_learning_records
from phoenix_learning_store import append_learning_row
from phoenix_position_manager import build_dynamic_exit_report
from phoenix_strategy_experiments import (
    choose_experiment_for_candidate,
    experiment_trade_metadata,
    load_active_experiments,
    record_experiment_hard_safety_fail,
)
from phoenix_strategy_registry import build_strategy_gate_decision, load_strategy_manifest
from phoenix_trade_attribution import classify_execution_record
from phoenix_testnet_safety import (
    MAINNET_LIVE,
    MAINNET_SHADOW,
    ORDER_SUBMIT_PATH,
    TESTNET_LIVE,
    active_position_count,
    assert_live_order_submission_allowed,
    dynamic_concurrency_throttle,
    normalize_runtime_mode,
    round_numbers,
    validate_runtime_mode_args,
)

LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
STABLE_ASSETS = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "USDP",
    "USDE",
    "USD1",
    "BFUSD",
    "AEUR",
    "EURI",
    "RLUSD",
}
LOWER_BETA_SYMBOLS = {"BTCUSDT", "ETHUSDT", "XAUUSDT", "XAGUSDT", "CLUSDT"}
SLOW_SYMBOLS = LOWER_BETA_SYMBOLS | {"XRPUSDT", "BCHUSDT", "SOLUSDT", "LTCUSDT"}
RANGE_REVERSION_SYMBOLS = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT"}
TREND_ELIGIBLE_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "BCHUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "TRXUSDT",
    "LTCUSDT",
    "SUIUSDT",
}
ASCII_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,24}USDT$")
SYMBOL_LOSS_COOLDOWN_SEC = 75.0
SYMBOL_ERROR_COOLDOWN_SEC = 180.0
TESTNET_TRADE_TELEMETRY_SCHEMA_VERSION = "testnet_trade_telemetry_v1"
SENSITIVE_NAME_RE = re.compile(r"(secret|token|password|private|api[_-]?key)", re.IGNORECASE)


@dataclass(slots=True)
class Candidate:
    symbol: str
    setup: str
    side: str
    score: float
    mark_price: float
    quote_volume_24h: float
    avg_quote_turnover_1m: float
    current_quote_turnover_1m: float
    price_change_24h_pct: float
    ret_1m_pct: float
    ret_3m_pct: float
    ret_5m_pct: float
    ret_15m_pct: float
    ret_30m_pct: float
    ret_60m_pct: float
    volume_ratio: float
    range_position: float
    trend_gap_pct: float
    higher_tf_trend_gap_pct: float
    pullback_pct: float
    bounce_pct: float
    tf5_ret_3bar_pct: float
    tf5_ret_6bar_pct: float
    tf5_volume_ratio: float
    tf5_range_position: float
    tf5_trend_gap_pct: float
    tf5_pullback_pct: float
    tf15_ret_3bar_pct: float
    tf15_ret_6bar_pct: float
    tf15_volume_ratio: float
    tf15_range_position: float
    tf15_trend_gap_pct: float
    tf15_pullback_pct: float
    strategy_id: str | None = None
    strategy_manifest_id: str | None = None
    manifest_id: str | None = None
    strategy_family: str | None = None
    evidence_level: int | None = None
    experiment_id: str | None = None
    expected_net_edge_bps: float | None = None
    expected_total_cost_bps: float | None = None
    expected_gross_move_bps: float | None = None


@dataclass(slots=True)
class TradeProfile:
    stop_pct: float
    take_pct: float
    breakeven_trigger_pct: float
    trail_trigger_pct: float
    trail_gap_pct: float
    stale_after_sec: int
    min_progress_pct: float


@dataclass(slots=True)
class TimeframeSnapshot:
    close: float
    quote_turnover: float
    avg_quote_turnover: float
    volume_ratio: float
    range_position: float
    trend_gap_pct: float
    higher_trend_gap_pct: float
    ret_3bar_pct: float
    ret_6bar_pct: float
    pullback_pct: float
    bounce_pct: float


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run repeated real Binance futures testnet trades and review each round.")
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--run-forever", action="store_true")
    parser.add_argument("--trades-per-round", type=int, default=12)
    parser.add_argument("--max-concurrent-trades", "--slots", type=int, default=1)
    parser.add_argument("--max-open-positions", type=int, default=10)
    parser.add_argument("--runtime-mode", default=TESTNET_LIVE, choices=[TESTNET_LIVE, MAINNET_LIVE, MAINNET_SHADOW])
    parser.add_argument("--env", default="testnet", choices=["testnet", "demo", "prod"])
    parser.add_argument("--margin-type", default="ISOLATED", choices=["ISOLATED", "CROSSED"])
    parser.add_argument("--scan-top", type=int, default=60)
    parser.add_argument("--universe-top", type=int, default=180)
    parser.add_argument("--min-quote-volume", type=float, default=250_000.0)
    parser.add_argument("--base-quote", type=float, default=45.0)
    parser.add_argument("--max-long-leverage", type=int, default=2)
    parser.add_argument("--max-short-leverage", type=int, default=2)
    parser.add_argument("--force-long-leverage", type=int, default=0)
    parser.add_argument("--force-short-leverage", type=int, default=0)
    parser.add_argument("--max-hold-sec", type=int, default=120)
    parser.add_argument("--poll-sec", type=float, default=2.0)
    parser.add_argument("--cooldown-sec", type=float, default=1.0)
    parser.add_argument("--idle-sleep-sec", type=float, default=6.0)
    parser.add_argument("--starting-min-score", type=float, default=42.0)
    parser.add_argument("--execution-floor-offset", type=float, default=0.0)
    parser.add_argument("--allow-short", action="store_true")
    parser.add_argument("--max-round-sec", type=int, default=0)
    parser.add_argument("--flatten-after-round", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("round_runner_reports"))
    parser.add_argument("--learning-store-file", type=Path, default=None)
    parser.add_argument("--disable-learning-store", action="store_true")
    parser.add_argument("--experiments-dir", type=Path, default=Path("strategy_experiments"))
    parser.add_argument("--candidate-allocation-pct", type=float, default=0.10)
    parser.add_argument("--disable-experiment-candidates", action="store_true")
    parser.add_argument("--strategy-manifest-dir", type=Path, default=Path("strategy_manifests"))
    parser.add_argument("--old-strategy-freeze-config", type=Path, default=Path("old_strategy_freeze.json"))
    parser.add_argument("--blocked-candidate-report-file", type=Path, default=None)
    parser.add_argument("--round-trip-fee-bps", type=float, default=8.0)
    parser.add_argument("--tp-slippage-buffer-bps", type=float, default=6.0)
    parser.add_argument("--tp-safety-buffer-bps", type=float, default=5.0)
    parser.add_argument("--disable-learning-gate", action="store_true")
    return parser.parse_args(argv)


def _trade_path_metrics(trade: dict[str, Any]) -> dict[str, float]:
    side = str(trade.get("side") or "BUY").upper()
    entry = safe_float(trade.get("entry_price"))
    best = safe_float(trade.get("best_mark_price")) or entry
    worst = safe_float(trade.get("worst_mark_price")) or entry
    close = safe_float(trade.get("close_price")) or entry
    if entry <= 0:
        return {"mfe_pct": 0.0, "mae_pct": 0.0, "return_pct": 0.0, "profit_giveback_pct": 0.0}
    if side == "SELL":
        mfe = max(0.0, (entry / max(worst, 1e-12) - 1.0) * 100.0)
        mae = max(0.0, (best / entry - 1.0) * 100.0)
        realized_return = (entry / max(close, 1e-12) - 1.0) * 100.0
    else:
        mfe = max(0.0, (best / entry - 1.0) * 100.0)
        mae = max(0.0, (entry / max(worst, 1e-12) - 1.0) * 100.0)
        realized_return = (close / entry - 1.0) * 100.0
    return {
        "mfe_pct": round4(mfe),
        "mae_pct": round4(mae),
        "return_pct": round4(realized_return),
        "profit_giveback_pct": round4(max(0.0, mfe - realized_return)),
    }


def build_testnet_learning_row(trade: dict[str, Any]) -> dict[str, Any]:
    metrics = _trade_path_metrics(trade)
    attribution = classify_execution_record({**trade, **metrics})
    entry_features = {
        key: trade.get(key)
        for key in (
            "score",
            "quote_volume_24h",
            "avg_quote_turnover_1m",
            "current_quote_turnover_1m",
            "price_change_24h_pct",
            "ret_1m_pct",
            "ret_3m_pct",
            "ret_5m_pct",
            "ret_15m_pct",
            "ret_30m_pct",
            "ret_60m_pct",
            "volume_ratio",
            "range_position",
            "trend_gap_pct",
            "higher_tf_trend_gap_pct",
            "pullback_pct",
            "bounce_pct",
        )
        if key in trade
    }
    return {
        "mode": "testnet",
        "source_event_id": trade.get("source_event_id") or f"testnet:{trade.get('entry_order_id')}:{trade.get('symbol')}",
        "symbol": trade.get("symbol"),
        "side": trade.get("side"),
        "setup": trade.get("setup") or trade.get("entry_reason") or trade.get("strategy_id") or "unknown",
        "strategy_id": trade.get("strategy_id") or trade.get("setup") or "phoenix_testnet_round_runner",
        "strategy_manifest_id": trade.get("strategy_manifest_id"),
        "strategy_family": trade.get("strategy_family"),
        "strategy_version": trade.get("strategy_version") or "testnet_runner_v1",
        "experiment_id": trade.get("experiment_id"),
        "evidence_level": trade.get("evidence_level"),
        "cost_model_version": trade.get("cost_model_version"),
        "regime_model_version": trade.get("regime_model_version"),
        "data_snapshot_id": trade.get("data_snapshot_id"),
        "code_git_sha": trade.get("code_git_sha"),
        "config_hash": trade.get("config_hash"),
        "baseline_id": trade.get("baseline_id") or f"testnet_runner_v1:{trade.get('setup') or trade.get('strategy_id') or 'unknown'}",
        "config_version": trade.get("config_version") or "testnet_runner_v1",
        "entry_reason": trade.get("setup"),
        "entry_features": entry_features,
        "market_state": trade.get("market_state") or "unknown",
        "exit_reason": trade.get("exit_reason"),
        "loss_reason": trade.get("loss_reason") or attribution["loss_reason"],
        "secondary_loss_reasons": trade.get("secondary_loss_reasons") or attribution["secondary_loss_reasons"],
        "all_loss_reasons": trade.get("all_loss_reasons") or attribution["all_loss_reasons"],
        "return_pct": metrics["return_pct"],
        "mfe_pct": metrics["mfe_pct"],
        "mae_pct": metrics["mae_pct"],
        "profit_giveback_pct": metrics["profit_giveback_pct"],
        "gross_pnl_usdt": trade.get("gross_pnl_usdt") or trade.get("realized_pnl_usdt"),
        "net_pnl_usdt": trade.get("net_pnl_usdt"),
        "commission_usdt": trade.get("commission_usdt"),
        "estimated_slippage_bps": trade.get("estimated_slippage_bps"),
        "latency_ms": trade.get("latency_ms"),
        "intended_take_profit_price": trade.get("intended_take_profit_price"),
        "actual_close_avg_price": trade.get("actual_close_avg_price") or trade.get("close_price"),
        "learning_gate_decision": trade.get("learning_gate_decision") or {},
        "dynamic_exit_simulation": trade.get("dynamic_exit_simulation") or {},
        "strategy_proposals": [],
    }


def compute_fee_aware_take_profit_pct(
    *,
    raw_take_pct: float,
    estimated_entry_slippage_bps: float = 0.0,
    round_trip_fee_bps: float = 8.0,
    slippage_buffer_bps: float = 6.0,
    safety_buffer_bps: float = 5.0,
) -> dict[str, Any]:
    cost_floor_pct = max(0.0, (round_trip_fee_bps + estimated_entry_slippage_bps + slippage_buffer_bps + safety_buffer_bps) / 100.0)
    effective = max(float(raw_take_pct or 0.0), cost_floor_pct)
    raw_safe = float(raw_take_pct or 0.0) > cost_floor_pct
    return {
        "raw_take_pct": round(float(raw_take_pct or 0.0), 6),
        "effective_take_pct": round(effective, 6),
        "cost_floor_pct": round(cost_floor_pct, 6),
        "round_trip_fee_bps": round(float(round_trip_fee_bps or 0.0), 6),
        "estimated_entry_slippage_bps": round(float(estimated_entry_slippage_bps or 0.0), 6),
        "slippage_buffer_bps": round(float(slippage_buffer_bps or 0.0), 6),
        "safety_buffer_bps": round(float(safety_buffer_bps or 0.0), 6),
        "raw_take_profit_fee_safe": raw_safe,
        "fee_aware_adjustment_reason": "raw_take_profit_fee_safe" if raw_safe else "raised_to_cost_floor",
    }


def apply_trade_cost_attribution(trade: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(trade)
    enriched.setdefault("gross_pnl_usdt", enriched.get("realized_pnl_usdt"))
    enriched.setdefault("actual_close_avg_price", enriched.get("close_price"))
    attribution = classify_execution_record(enriched)
    enriched["loss_reason"] = attribution["loss_reason"]
    enriched["secondary_loss_reasons"] = attribution["secondary_loss_reasons"]
    enriched["gross_pnl_usdt"] = attribution["gross_pnl_usdt"]
    if attribution["loss_reason"] == "tp_net_loss" and "take_profit" in str(enriched.get("exit_reason") or "").lower():
        enriched["original_exit_reason"] = enriched.get("exit_reason")
        enriched["exit_reason"] = "fee_slippage_tp_fail"
    return enriched


def choose_experiment_for_runner_candidate(
    candidate: Candidate,
    experiments: list[dict[str, Any]],
    *,
    allocation_pct: float,
    seed: str,
) -> dict[str, Any] | None:
    return choose_experiment_for_candidate(candidate, experiments, allocation_pct=allocation_pct, seed=seed)


def build_parallel_dynamic_exit_simulation(
    *,
    candidate: Candidate,
    entry_price: float,
    management: dict[str, Any],
    baseline_return_pct: float,
) -> dict[str, Any]:
    price_points = list(management.get("price_points") or [])
    if not price_points:
        fallback_price = safe_float(management.get("last_mark_price")) or safe_float(management.get("close_avg_price")) or entry_price
        price_points = [{"timestamp_ms": 0, "mark_price": fallback_price}]
    opened_at_ms = int(safe_float(price_points[0].get("timestamp_ms")) if isinstance(price_points[0], dict) else 0)
    position = {
        "entry_price": entry_price,
        "side": "LONG" if candidate.side == "BUY" else "SHORT",
        "opened_at_ms": opened_at_ms,
    }
    baseline_exit_result = {
        "exit_reason": management.get("exit_reason"),
        "baseline_return_pct": round4(baseline_return_pct),
        "hold_seconds": management.get("hold_seconds"),
    }
    try:
        dynamic_report = build_dynamic_exit_report(position, price_points)
    except Exception as exc:
        return {
            "baseline_exit_result": baseline_exit_result,
            "dynamic_exit_simulated_result": {
                "simulation_error": str(exc),
                "live_trading_enabled": False,
            },
            "would_dynamic_exit_improve": False,
        }
    signals = dynamic_report.get("early_exit_signals") or []
    if signals:
        simulated_exit_return_pct = safe_float(signals[0].get("profit_pct"))
        simulated_exit_reason = signals[0].get("module")
    else:
        simulated_exit_return_pct = safe_float(dynamic_report.get("current_profit_pct"))
        simulated_exit_reason = "baseline_hold"
    dynamic_report["simulated_exit_return_pct"] = round4(simulated_exit_return_pct)
    dynamic_report["simulated_exit_reason"] = simulated_exit_reason
    dynamic_report["would_dynamic_exit_improve"] = simulated_exit_return_pct > baseline_return_pct
    dynamic_report["live_trading_enabled"] = False
    return {
        "baseline_exit_result": baseline_exit_result,
        "dynamic_exit_simulated_result": dynamic_report,
        "would_dynamic_exit_improve": bool(dynamic_report["would_dynamic_exit_improve"]),
    }


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def round4(value: float) -> float:
    return round(float(value), 4)


def round_down_to_step(value: float, step: float) -> float:
    if value <= 0 or step <= 0:
        return 0.0
    decimal_value = Decimal(str(value))
    decimal_step = Decimal(str(step))
    return float((decimal_value / decimal_step).quantize(Decimal("1"), rounding=ROUND_DOWN) * decimal_step)


def round_up_to_step(value: float, step: float) -> float:
    if value <= 0 or step <= 0:
        return 0.0
    decimal_value = Decimal(str(value))
    decimal_step = Decimal(str(step))
    return float((decimal_value / decimal_step).quantize(Decimal("1"), rounding=ROUND_UP) * decimal_step)


def pct_change(current: float, past: float) -> float:
    if current <= 0 or past <= 0:
        return 0.0
    return ((current / past) - 1.0) * 100.0


def safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def latency_ms_between(start_ms: Any, end_ms: Any) -> int | None:
    start = safe_int(start_ms)
    end = safe_int(end_ms)
    if start is None or end is None or end < start:
        return None
    return end - start


def is_sensitive_name(name: str) -> bool:
    return bool(SENSITIVE_NAME_RE.search(name or ""))


def json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple, set)):
        return [json_ready(item) for item in value]
    return str(value)


def build_testnet_trade_telemetry_schema() -> dict[str, Any]:
    fields = {
        "maker_or_taker": {"type": "string", "source": "Binance user_trades maker flag; maker/taker/mixed/unknown"},
        "entry_maker_or_taker": {"type": "string", "source": "Entry order fill summary"},
        "exit_maker_or_taker": {"type": "string", "source": "Exit order fill summary"},
        "fee_bps_effective": {"type": "number|null", "source": "commission / traded notional * 10000"},
        "exchange_fee": {"type": "object", "source": "Entry and exit commission from user_trades"},
        "estimated_slippage": {"type": "object", "source": "Entry average fill versus trade intent"},
        "estimated_slippage_bps": {"type": "number", "source": "Existing entry slippage estimate"},
        "realized_slippage_proxy": {"type": "object", "source": "Entry/exit price versus available references"},
        "realized_slippage_proxy_bps": {"type": "number|null", "source": "Positive means worse than reference; negative means price improvement"},
        "spread_proxy_at_entry": {"type": "object", "source": "Book ticker captured only when runner already reads it for maker entry"},
        "spread_proxy_at_entry_bps": {"type": "number|null", "source": "Entry book ask-bid spread over mid"},
        "spread_proxy_at_exit": {"type": "object", "source": "Not collected by this runner unless explicitly available"},
        "spread_proxy_at_exit_bps": {"type": "number|null", "source": "Exit book spread proxy"},
        "funding_paid_or_received": {"type": "number|null", "source": "Funding income linkage; null when not collected"},
        "funding_paid_or_received_usdt": {"type": "number|null", "source": "Funding income linkage; null when not collected"},
        "income_funding_event_linkage": {"type": "object", "source": "Funding event availability and linkage status"},
        "mfe": {"type": "number", "source": "Best path excursion percent from mark price path"},
        "mae": {"type": "number", "source": "Worst path excursion percent from mark price path"},
        "mfe_pct": {"type": "number", "source": "Alias retained for learning/report compatibility"},
        "mae_pct": {"type": "number", "source": "Alias retained for learning/report compatibility"},
        "entry_signal_timestamp": {"type": "integer|null", "source": "Candidate launch timestamp when provided by runner"},
        "actual_entry_timestamp": {"type": "integer|null", "source": "First entry fill time from user_trades"},
        "entry_to_fill_latency_ms": {"type": "integer|null", "source": "Signal-to-fill latency when signal timestamp is available; otherwise submit-to-fill"},
        "exit_signal_timestamp": {"type": "integer|null", "source": "Time runner decided or detected exit condition"},
        "actual_exit_timestamp": {"type": "integer|null", "source": "Last exit fill time from user_trades"},
        "exit_to_fill_latency_ms": {"type": "integer|null", "source": "Exit signal to fill latency"},
        "runtime_mode_resolved": {"type": "string", "source": "validate_runtime_mode_args/normalize_runtime_mode"},
        "credentials_environment_resolved": {"type": "string", "source": "Resolved Binance credential environment name"},
        "execution_mode_resolved": {"type": "string", "source": "Runner order-submit mode"},
        "round_config_snapshot_id": {"type": "string|null", "source": "Per-round non-secret config snapshot hash"},
        "strategy_id": {"type": "string", "source": "Experiment metadata or setup"},
        "experiment_id": {"type": "string|null", "source": "Experiment metadata"},
        "setup": {"type": "string", "source": "Candidate setup"},
        "symbol": {"type": "string", "source": "Candidate symbol"},
        "side": {"type": "string", "source": "Candidate side"},
        "leverage": {"type": "number", "source": "Resolved trade intent leverage"},
        "quote_allocation": {"type": "number", "source": "Alias of quote_allocation_usdt"},
        "btc_regime": {"type": "string|null", "source": "Not available in this runner unless upstream provides it"},
        "oi_regime": {"type": "string|null", "source": "Not available in this runner unless upstream provides it"},
        "liquidity_bucket": {"type": "string", "source": "Telemetry-only bucket derived from avg_quote_turnover_1m"},
        "hmm_state_if_available": {"type": "string|null", "source": "Not available in this runner unless upstream provides it"},
        "markov_state_if_available": {"type": "string|null", "source": "Not available in this runner unless upstream provides it"},
    }
    return {
        "schema_version": TESTNET_TRADE_TELEMETRY_SCHEMA_VERSION,
        "scope": "phoenix_testnet_round_runner trade-level telemetry",
        "fields": fields,
    }


def _public_args_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in sorted(vars(args).items()):
        payload[key] = "***MASKED***" if is_sensitive_name(key) else json_ready(value)
    return payload


def _public_settings_snapshot(settings: Any) -> dict[str, Any]:
    allowed = (
        "quote_allocation_usdt",
        "allocation_mode",
        "risk_budget_pct_of_balance",
        "reserve_balance_usdt",
        "execution_mode",
        "allow_explicit_symbol_live",
        "allow_auto_short",
        "max_open_positions",
        "strategy_min_score",
        "reject_blocked_candidates",
        "min_execution_quality_score",
        "max_event_risk_score",
        "max_estimated_slippage_bps",
        "max_spread_bps",
        "strategy_manifest_dir",
        "old_strategy_freeze_config",
        "blocked_candidate_report_file",
        "cooldown_minutes_after_close",
        "significant_score_delta",
        "significant_score_delta_pct",
        "leverage",
        "margin_type",
        "working_type",
        "initial_stop_loss_pct",
        "take_profit_pct",
        "breakeven_trigger_pct",
        "breakeven_lock_pct",
        "trailing_callback_pct",
        "position_mode",
        "guardian_poll_interval_sec",
        "guardian_max_runtime_sec",
    )
    payload: dict[str, Any] = {}
    for key in allowed:
        if hasattr(settings, key):
            payload[key] = json_ready(getattr(settings, key))
    return payload


def stable_payload_id(payload: dict[str, Any], *, prefix: str) -> str:
    canonical = json.dumps(json_ready(payload), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def build_round_config_snapshot(
    args: argparse.Namespace,
    *,
    runtime_mode: str,
    credentials_environment: str,
    settings: Any,
    round_no: int,
) -> dict[str, Any]:
    payload = {
        "schema_version": "round_config_snapshot_v1",
        "telemetry_schema_version": TESTNET_TRADE_TELEMETRY_SCHEMA_VERSION,
        "runner_entrypoint": str(Path(__file__).resolve()),
        "round": int(round_no),
        "runtime_mode_resolved": normalize_runtime_mode(runtime_mode),
        "credentials_environment_resolved": str(credentials_environment or "").lower(),
        "execution_mode_resolved": f"{normalize_runtime_mode(runtime_mode)}_ORDER_SUBMIT",
        "order_submit_endpoint": ORDER_SUBMIT_PATH,
        "args": _public_args_snapshot(args),
        "execution_settings": _public_settings_snapshot(settings),
    }
    payload["round_config_snapshot_id"] = stable_payload_id(payload, prefix="roundcfg")
    return payload


def write_json_artifact(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl_artifact(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def liquidity_bucket_from_turnover(avg_quote_turnover_1m: float) -> str:
    turnover = safe_float(avg_quote_turnover_1m)
    if turnover >= 50_000_000:
        return "ultra_liquid"
    if turnover >= 12_000_000:
        return "high_liquidity"
    if turnover >= 3_000_000:
        return "medium_liquidity"
    if turnover >= 1_000_000:
        return "low_liquidity"
    return "thin_liquidity"


def optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _nested_metadata_value(payload: dict[str, Any] | None, *keys: str) -> Any:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    for nested_key in ("cost_model", "expected_cost_model", "cost"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            value = _nested_metadata_value(nested, *keys)
            if value is not None:
                return value
    return None


def _first_number_from_payloads(payloads: list[dict[str, Any]], *keys: str) -> float | None:
    for payload in payloads:
        value = optional_float(_nested_metadata_value(payload, *keys))
        if value is not None:
            return value
    return None


def build_testnet_candidate_gate_payload(
    candidate: Candidate,
    experiment: dict[str, Any] | None,
    args: argparse.Namespace,
) -> dict[str, Any]:
    candidate_payload = asdict(candidate)
    manifest_dir = getattr(args, "strategy_manifest_dir", Path("strategy_manifests"))
    experiment_metadata = experiment_trade_metadata(experiment)
    candidate_setup_manifest = candidate.setup if load_strategy_manifest(manifest_dir, candidate.setup) is not None else None
    strategy_manifest_id = _first_present(
        candidate.strategy_manifest_id,
        candidate.manifest_id,
        candidate.strategy_id,
        candidate_setup_manifest,
        experiment_metadata.get("strategy_manifest_id"),
        experiment_metadata.get("strategy_id"),
    )
    manifest = load_strategy_manifest(manifest_dir, str(strategy_manifest_id)) if strategy_manifest_id else None
    manifest_payload = manifest if isinstance(manifest, dict) else {}
    payload_sources = [candidate_payload, experiment or {}, manifest_payload]
    expected_gross_move_bps = _first_number_from_payloads(payload_sources, "expected_gross_move_bps", "expected_follow_through_bps")
    expected_total_cost_bps = _first_number_from_payloads(payload_sources, "expected_total_cost_bps", "max_expected_total_cost_bps")
    expected_net_edge_bps = _first_number_from_payloads(payload_sources, "expected_net_edge_bps")
    if expected_net_edge_bps is None and expected_gross_move_bps is not None and expected_total_cost_bps is not None:
        expected_net_edge_bps = expected_gross_move_bps - expected_total_cost_bps

    candidate_payload.update(
        {
            "strategy_id": _first_present(candidate.strategy_id, experiment_metadata.get("strategy_id"), strategy_manifest_id, candidate.setup),
            "strategy_manifest_id": strategy_manifest_id,
            "strategy_family": _first_present(candidate.strategy_family, experiment_metadata.get("strategy_family"), manifest_payload.get("family")),
            "evidence_level": _first_present(candidate.evidence_level, experiment_metadata.get("evidence_level"), manifest_payload.get("evidence_level")),
            "experiment_id": _first_present(experiment_metadata.get("experiment_id"), candidate.experiment_id, f"testnet_runner_v1:{candidate.setup}"),
            "expected_gross_move_bps": expected_gross_move_bps,
            "expected_total_cost_bps": expected_total_cost_bps,
            "expected_net_edge_bps": expected_net_edge_bps,
            "liquidity_bucket": liquidity_bucket_from_turnover(candidate.avg_quote_turnover_1m),
        }
    )
    return candidate_payload


def evaluate_testnet_candidate_gate(
    candidate: Candidate,
    experiment: dict[str, Any] | None,
    args: argparse.Namespace,
    *,
    parity_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_payload = build_testnet_candidate_gate_payload(candidate, experiment, args)
    return build_strategy_gate_decision(
        candidate_payload,
        experiment=experiment,
        manifest_dir=getattr(args, "strategy_manifest_dir", Path("strategy_manifests")),
        old_strategy_freeze_path=getattr(args, "old_strategy_freeze_config", Path("old_strategy_freeze.json")),
        target_stage="testnet",
        parity_report=parity_report,
    )


def spread_proxy_from_book(book_ticker: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(book_ticker, dict):
        return {"available": False, "spread_bps": None, "source": "not_collected"}
    bid = safe_float(book_ticker.get("bidPrice"))
    ask = safe_float(book_ticker.get("askPrice"))
    if bid <= 0 or ask <= 0 or ask < bid:
        return {"available": False, "spread_bps": None, "source": "book_ticker_unavailable"}
    mid = (bid + ask) / 2.0
    spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0 else 0.0
    return {
        "available": True,
        "bid": round4(bid),
        "ask": round4(ask),
        "spread_bps": round4(spread_bps),
        "source": "futures.book_ticker",
    }


def execution_slippage_proxy_bps(*, trade_side: str, phase: str, reference_price: float, actual_price: float) -> float | None:
    reference = safe_float(reference_price)
    actual = safe_float(actual_price)
    if reference <= 0 or actual <= 0:
        return None
    side = str(trade_side or "BUY").upper()
    if phase == "entry":
        value = ((actual / reference) - 1.0) * 10_000.0 if side == "BUY" else ((reference / actual) - 1.0) * 10_000.0
    else:
        value = ((reference / actual) - 1.0) * 10_000.0 if side == "BUY" else ((actual / reference) - 1.0) * 10_000.0
    return round4(value)


def combine_liquidity_roles(*roles: str) -> str:
    normalized = {role for role in roles if role in {"maker", "taker", "mixed"}}
    if not normalized:
        return "unknown"
    if len(normalized) == 1:
        return normalized.pop()
    return "mixed"


def bounded_score(value: float, *, center: float, half_width: float, max_score: float) -> float:
    if half_width <= 0:
        return 0.0
    distance = abs(value - center)
    if distance >= half_width:
        return 0.0
    return max_score * (1.0 - distance / half_width)


def penalty_if(condition: bool, amount: float) -> float:
    return amount if condition else 0.0


def uses_resting_take_profit(setup: str) -> bool:
    return (
        setup.startswith("impulse_")
        or setup.startswith("micro_")
        or setup.startswith("pressure_")
        or setup.startswith("flush_")
        or setup.startswith("volatility_")
        or setup.startswith("trend_")
        or setup.startswith("major_")
        or setup.startswith("explore_")
        or setup in {"failed_bounce_short", "breakdown_continuation_short"}
    )


def prefers_maker_entry(setup: str) -> bool:
    return (
        setup.startswith("impulse_")
        or setup.startswith("micro_")
        or setup.startswith("pressure_")
        or setup.startswith("flush_")
        or setup.startswith("explore_")
        or setup.startswith("major_")
        or setup.startswith("range_reversion")
        or setup == "trend_pullback_long"
        or setup == "failed_bounce_short"
    )


def parse_rate_limit_backoff_seconds(exc: BinanceAPIError) -> float | None:
    if exc.code != -1003:
        return None
    payload_msg = ""
    if isinstance(exc.payload, dict):
        payload_msg = str(exc.payload.get("msg") or "")
    message = payload_msg or str(exc)
    match = re.search(r"banned until (\d+)", message)
    if match:
        retry_at_ms = int(match.group(1))
        return max(5.0, ((retry_at_ms - int(time.time() * 1000)) / 1000.0) + 3.0)
    if "Too many requests" in message:
        return 30.0
    return 15.0


async def call_with_rate_limit_backoff(
    operation: Any,
    *,
    label: str,
    round_no: int | None = None,
) -> Any:
    while True:
        try:
            return await operation()
        except BinanceAPIError as exc:
            backoff_sec = parse_rate_limit_backoff_seconds(exc)
            if backoff_sec is None:
                raise
            payload = {
                "event": "rate_limit_backoff",
                "label": label,
                "sleep_seconds": round4(backoff_sec),
                "error": str(exc),
            }
            if round_no is not None:
                payload["round"] = round_no
            print(json.dumps(payload, ensure_ascii=False), flush=True)
            await asyncio.sleep(min(max(5.0, backoff_sec), 900.0))


def setup_priority(setup: str) -> int:
    if setup.startswith("impulse_"):
        return 5
    if setup.startswith("micro_"):
        return 4
    if setup.startswith("pressure_"):
        return 4
    if setup.startswith("volatility_"):
        return 4
    if setup.startswith("flush_"):
        return 3
    if setup.startswith("explore_"):
        return 3
    if setup.startswith("swing_"):
        return 4
    if setup.startswith("major_"):
        return 3
    if setup in {"failed_bounce_short", "trend_pullback_long"}:
        return 2
    if setup in {"breakdown_continuation_short", "range_reversion_long", "range_reversion_short"}:
        return 1
    if setup.startswith("range_reversion"):
        return 0
    return -1


def setup_execution_floor(setup: str) -> float:
    if setup.startswith("impulse_"):
        return 44.0
    if setup.startswith("micro_"):
        return 24.0
    if setup.startswith("pressure_"):
        return 48.0
    if setup.startswith("flush_"):
        return 42.0
    if setup.startswith("swing_"):
        return 54.0
    if setup.startswith("volatility_"):
        return 40.0
    if setup == "explore_reclaim_long":
        return 32.0
    if setup == "explore_reject_short":
        return 34.0
    if setup.startswith("major_"):
        return 44.0
    if setup == "trend_pullback_long":
        return 50.0
    if setup == "trend_breakout_long":
        return 999.0
    if setup == "failed_bounce_short":
        return 48.0
    if setup == "breakdown_continuation_short":
        return 46.0
    if setup.startswith("range_reversion"):
        return 30.0
    return 42.0


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    multiplier = 2.0 / (period + 1.0)
    current = values[0]
    output: list[float] = []
    for value in values:
        current = (value * multiplier) + (current * (1.0 - multiplier))
        output.append(current)
    return output


def build_timeframe_snapshot(candles: list[list[Any]]) -> TimeframeSnapshot | None:
    if len(candles) < 65:
        return None

    closes = [safe_float(row[4]) for row in candles]
    highs = [safe_float(row[2]) for row in candles]
    lows = [safe_float(row[3]) for row in candles]
    volumes = [safe_float(row[5]) for row in candles]
    current_index = len(candles) - 2
    if current_index < 21 or min(closes[-20:]) <= 0:
        return None

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema34 = ema(closes, 34)
    ema55 = ema(closes, 55)
    close = closes[current_index]
    recent_high = max(highs[current_index - 9 : current_index])
    recent_low = min(lows[current_index - 9 : current_index])
    quote_turnover = close * volumes[current_index]
    avg_quote_turnover = mean([closes[index] * volumes[index] for index in range(current_index - 9, current_index + 1)])
    volume_ratio = volumes[current_index] / max(mean(volumes[current_index - 20 : current_index]), 1e-9)
    range_span = max(recent_high - recent_low, 1e-9)

    return TimeframeSnapshot(
        close=close,
        quote_turnover=quote_turnover,
        avg_quote_turnover=avg_quote_turnover,
        volume_ratio=volume_ratio,
        range_position=(close - recent_low) / range_span,
        trend_gap_pct=pct_change(ema9[current_index], ema21[current_index]) if ema21[current_index] > 0 else 0.0,
        higher_trend_gap_pct=pct_change(ema34[current_index], ema55[current_index]) if ema55[current_index] > 0 else 0.0,
        ret_3bar_pct=pct_change(close, closes[current_index - 3]),
        ret_6bar_pct=pct_change(close, closes[current_index - 6]),
        pullback_pct=pct_change(close, recent_high),
        bounce_pct=pct_change(close, recent_low),
    )


def is_tradeable_symbol(symbol: str) -> bool:
    normalized = str(symbol or "").upper().strip()
    if not normalized.endswith("USDT"):
        return False
    base = normalized[:-4]
    if not base:
        return False
    if any(ch.isspace() or ch in {"/", "\\", ":", "*", "?", '"', "<", ">", "|"} for ch in normalized):
        return False
    ascii_base = base.upper() if base.isascii() else base
    if len(base) < 2 or ascii_base in STABLE_ASSETS:
        return False
    if base.isascii() and any(ascii_base.endswith(suffix) for suffix in LEVERAGED_SUFFIXES):
        return False
    return True


def trend_setup_allowed(
    *,
    symbol: str,
    last_close: float,
    quote_volume_24h: float,
    avg_quote_turnover: float,
    current_quote_turnover: float,
    frame5: TimeframeSnapshot,
    frame15: TimeframeSnapshot,
) -> bool:
    major_relaxed_liquidity = (
        avg_quote_turnover >= 450_000.0
        and current_quote_turnover >= 10_000.0
        and frame5.avg_quote_turnover >= 750_000.0
        and frame15.avg_quote_turnover >= 1_250_000.0
    )
    super_liquid_liquidity = (
        last_close >= 0.05
        and quote_volume_24h >= 1_000_000_000.0
        and avg_quote_turnover >= 2_000_000.0
        and current_quote_turnover >= 40_000.0
        and frame5.avg_quote_turnover >= 3_000_000.0
        and frame15.avg_quote_turnover >= 5_000_000.0
    )
    return major_relaxed_liquidity or super_liquid_liquidity


def extract_usdt_available_balance(payload: object) -> float:
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("asset") or "").upper() != "USDT":
                continue
            for key in ("availableBalance", "balance", "crossWalletBalance"):
                value = safe_float(item.get(key))
                if value > 0:
                    return value
    if isinstance(payload, dict):
        for key in ("totalAvailableBalance", "virtualMaxWithdrawAmount", "accountEquity"):
            value = safe_float(payload.get(key))
            if value > 0:
                return value
    return 0.0


def discover_universe(
    payload: list[dict[str, Any]],
    *,
    top_limit: int,
    min_quote_volume: float,
    sort_by: str = "quote_volume",
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in payload:
        symbol = str(item.get("symbol") or "").strip()
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
        items.sort(
            key=lambda entry: (entry["price_change_24h_pct"], entry["quote_volume_24h"]),
            reverse=True,
        )
    elif sort_by == "negative_change":
        items.sort(
            key=lambda entry: (entry["price_change_24h_pct"], -entry["quote_volume_24h"]),
        )
    elif sort_by == "abs_change":
        items.sort(
            key=lambda entry: (abs(entry["price_change_24h_pct"]), entry["quote_volume_24h"]),
            reverse=True,
        )
    else:
        items.sort(key=lambda entry: entry["quote_volume_24h"], reverse=True)
    return items[:top_limit]


def score_candidate(
    item: dict[str, Any],
    candles_1m: list[list[Any]],
    candles_5m: list[list[Any]],
    candles_15m: list[list[Any]],
    *,
    allow_short: bool,
) -> Candidate | None:
    if len(candles_1m) < 65:
        return None

    symbol = str(item["symbol"])
    if not is_tradeable_symbol(symbol):
        return None
    frame5 = build_timeframe_snapshot(candles_5m)
    frame15 = build_timeframe_snapshot(candles_15m)
    if frame5 is None or frame15 is None:
        return None

    closes = [safe_float(row[4]) for row in candles_1m]
    highs = [safe_float(row[2]) for row in candles_1m]
    lows = [safe_float(row[3]) for row in candles_1m]
    volumes = [safe_float(row[5]) for row in candles_1m]
    if min(closes[-20:]) <= 0:
        return None

    current_index = len(candles_1m) - 2
    if current_index < 21:
        return None

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema34 = ema(closes, 34)
    ema55 = ema(closes, 55)
    last_close = closes[current_index]
    recent_high = max(highs[current_index - 9 : current_index])
    recent_low = min(lows[current_index - 9 : current_index])
    current_quote_turnover = last_close * volumes[current_index]
    avg_quote_turnover = mean([closes[index] * volumes[index] for index in range(current_index - 9, current_index + 1)])
    volume_ratio = volumes[current_index] / max(mean(volumes[current_index - 20 : current_index]), 1e-9)
    ret_1m_pct = pct_change(last_close, closes[current_index - 1])
    ret_3m_pct = pct_change(last_close, closes[current_index - 3])
    ret_5m_pct = pct_change(last_close, closes[current_index - 5])
    ret_15m_pct = pct_change(last_close, closes[current_index - 15])
    ret_30m_pct = pct_change(last_close, closes[current_index - 30]) if current_index >= 30 else 0.0
    ret_60m_pct = pct_change(last_close, closes[current_index - 60]) if current_index >= 60 else 0.0
    range_span = max(recent_high - recent_low, 1e-9)
    range_position = (last_close - recent_low) / range_span
    trend_gap_pct = pct_change(ema9[current_index], ema21[current_index]) if ema21[current_index] > 0 else 0.0
    higher_tf_trend_gap_pct = pct_change(ema34[current_index], ema55[current_index]) if ema55[current_index] > 0 else 0.0
    pullback_pct = pct_change(last_close, recent_high)
    bounce_pct = pct_change(last_close, recent_low)
    quote_volume_24h = safe_float(item["quote_volume_24h"])
    price_change_24h_pct = safe_float(item["price_change_24h_pct"])
    min_avg_quote_turnover = 200_000.0
    min_current_quote_turnover = 5_000.0
    if last_close < 0.01:
        return None
    if abs(price_change_24h_pct) >= 120.0:
        return None
    if avg_quote_turnover < min_avg_quote_turnover or current_quote_turnover < min_current_quote_turnover:
        return None
    if quote_volume_24h < 150_000_000.0:
        return None
    if last_close < 0.03 and avg_quote_turnover < 1_250_000.0:
        return None
    if volume_ratio > 8.0 or frame5.volume_ratio > 7.5 or frame15.volume_ratio > 5.5:
        return None
    if frame5.volume_ratio < 0.02 or frame15.volume_ratio < 0.02:
        return None
    if (
        range_position < -0.35
        or range_position > 1.7
        or frame5.range_position < -0.35
        or frame5.range_position > 1.7
        or frame15.range_position < -0.35
        or frame15.range_position > 1.7
    ):
        return None
    major_liquidity = (
        avg_quote_turnover >= 750_000.0
        and frame5.avg_quote_turnover >= 1_200_000.0
        and frame15.avg_quote_turnover >= 2_000_000.0
    )
    trend_liquidity_ok = trend_setup_allowed(
        symbol=symbol,
        last_close=last_close,
        quote_volume_24h=quote_volume_24h,
        avg_quote_turnover=avg_quote_turnover,
        current_quote_turnover=current_quote_turnover,
        frame5=frame5,
        frame15=frame15,
    )
    flat_regime = (
        major_liquidity
        and abs(ret_15m_pct) <= 0.35
        and abs(frame5.ret_6bar_pct) <= 0.55
        and abs(frame15.ret_3bar_pct) <= 0.9
        and abs(trend_gap_pct) <= 0.10
        and abs(frame5.trend_gap_pct) <= 0.08
        and abs(frame15.trend_gap_pct) <= 0.14
        and abs(price_change_24h_pct) <= 2.2
    )

    long_baseline = (
        trend_liquidity_ok
        and last_close >= 0.10
        and
        ema9[current_index] > ema21[current_index]
        and trend_gap_pct > 0.005
        and frame5.trend_gap_pct > -0.03
        and frame15.trend_gap_pct >= -0.02
        and higher_tf_trend_gap_pct >= -0.02
        and -1.8 <= price_change_24h_pct <= 4.8
        and ret_15m_pct >= -0.12
        and frame5.ret_3bar_pct >= -0.03
        and frame15.ret_3bar_pct >= -0.20
        and volume_ratio >= 0.55
        and frame5.volume_ratio >= 0.15
        and frame15.volume_ratio >= 0.10
        and 0.10 <= range_position <= 0.78
        and 0.10 <= frame5.range_position <= 0.82
        and 0.12 <= frame15.range_position <= 0.88
        and frame15.ret_6bar_pct <= 1.8
        and ret_1m_pct <= 0.30
        and ret_5m_pct >= -0.03
        and pullback_pct <= -0.04
        and frame5.pullback_pct <= -0.04
        and (frame5.pullback_pct <= -0.05 or ret_15m_pct >= 0.25)
        and volume_ratio >= 0.90
        and frame5.volume_ratio >= 0.50
    )
    long_score = 0.0
    if long_baseline:
        long_score += 16.0
        long_score += bounded_score(max(price_change_24h_pct, 0.0), center=1.8, half_width=3.8, max_score=8.0)
        long_score += bounded_score(frame15.ret_3bar_pct, center=0.35, half_width=1.4, max_score=14.0)
        long_score += bounded_score(max(frame5.ret_6bar_pct, 0.0), center=0.35, half_width=1.1, max_score=14.0)
        long_score += bounded_score(max(ret_15m_pct, 0.0), center=0.18, half_width=0.7, max_score=8.0)
        long_score += bounded_score(ret_5m_pct, center=-0.02, half_width=0.42, max_score=8.0)
        long_score += bounded_score(volume_ratio, center=1.2, half_width=0.9, max_score=8.0)
        long_score += bounded_score(frame5.volume_ratio, center=1.1, half_width=0.8, max_score=6.0)
        long_score += bounded_score(min(frame15.range_position, 1.0), center=0.62, half_width=0.45, max_score=9.0)
        long_score += bounded_score(min(frame5.range_position, 1.0), center=0.74, half_width=0.44, max_score=9.0)
        long_score += bounded_score(frame5.pullback_pct, center=-0.14, half_width=0.22, max_score=10.0)
        long_score += bounded_score(frame15.trend_gap_pct, center=0.03, half_width=0.20, max_score=8.0)
        long_score -= penalty_if(ret_1m_pct > 0.25, 12.0)
        long_score -= penalty_if(ret_5m_pct > 0.45, 10.0)
        long_score -= penalty_if(range_position > 0.74, 16.0)
        long_score -= penalty_if(frame5.range_position > 0.82, 18.0)
        long_score -= penalty_if(frame15.range_position > 0.88, 18.0)
        long_score -= penalty_if(frame5.pullback_pct > 0.18, 8.0)
        long_score -= penalty_if(frame15.ret_3bar_pct < -0.05, 8.0)
        long_score -= penalty_if(frame5.trend_gap_pct < 0.0, 8.0)
        long_score -= penalty_if(frame15.trend_gap_pct < -0.08, 8.0)
        long_score -= penalty_if(price_change_24h_pct < -1.2, 8.0)
        long_score -= penalty_if(symbol in SLOW_SYMBOLS, 4.0)
        if flat_regime:
            long_score -= 15.0
        if pullback_pct > -0.03 and (
            frame5.pullback_pct > -0.05
            or frame15.ret_3bar_pct < 0.35
            or frame5.ret_6bar_pct < 0.18
            or frame15.trend_gap_pct < 0.06
        ):
            long_score -= 18.0
        if pullback_pct <= -0.04 and (frame5.trend_gap_pct < 0.03 or frame15.trend_gap_pct < 0.04):
            long_score -= 8.0
        if (
            last_close < 0.20
            and quote_volume_24h < 2_500_000_000.0
            and (frame15.trend_gap_pct < 0.02 or ret_15m_pct < 0.05 or volume_ratio < 0.75)
        ):
            long_score -= 18.0
        if last_close < 0.12 and quote_volume_24h < 5_000_000_000.0:
            long_score -= 12.0

    short_baseline = (
        trend_liquidity_ok
        and allow_short
        and ema9[current_index] < ema21[current_index]
        and trend_gap_pct < -0.005
        and frame5.trend_gap_pct < 0.03
        and frame15.trend_gap_pct <= 0.02
        and higher_tf_trend_gap_pct <= 0.04
        and -4.8 <= price_change_24h_pct <= 1.8
        and ret_15m_pct <= 0.12
        and frame5.ret_3bar_pct <= 0.03
        and frame15.ret_3bar_pct <= 0.20
        and volume_ratio >= 0.55
        and -0.15 <= range_position <= 0.90
        and -0.15 <= frame5.range_position <= 0.90
        and -0.15 <= frame15.range_position <= 0.88
        and ret_1m_pct >= -0.45
        and bounce_pct >= -0.05
        and frame5.bounce_pct >= -0.04
        and (frame5.bounce_pct >= 0.02 or frame15.trend_gap_pct <= 0.02 or ret_15m_pct <= -0.25)
    )
    short_score = 0.0
    if short_baseline:
        short_score += 16.0
        short_score += bounded_score(max(-price_change_24h_pct, 0.0), center=1.8, half_width=3.8, max_score=8.0)
        short_score += bounded_score(-frame15.ret_3bar_pct, center=0.35, half_width=1.4, max_score=14.0)
        short_score += bounded_score(max(-frame5.ret_6bar_pct, 0.0), center=0.35, half_width=1.1, max_score=14.0)
        short_score += bounded_score(max(-ret_15m_pct, 0.0), center=0.18, half_width=0.7, max_score=8.0)
        short_score += bounded_score(-ret_5m_pct, center=-0.02, half_width=0.42, max_score=8.0)
        short_score += bounded_score(volume_ratio, center=1.2, half_width=0.9, max_score=8.0)
        short_score += bounded_score(frame5.volume_ratio, center=1.1, half_width=0.8, max_score=6.0)
        short_score += bounded_score(max(0.0, 1.0 - frame15.range_position), center=0.62, half_width=0.45, max_score=9.0)
        short_score += bounded_score(max(0.0, 1.0 - frame5.range_position), center=0.74, half_width=0.44, max_score=9.0)
        short_score += bounded_score(frame5.bounce_pct, center=0.14, half_width=0.22, max_score=10.0)
        short_score += bounded_score(-frame15.trend_gap_pct, center=0.03, half_width=0.20, max_score=8.0)
        short_score -= penalty_if(ret_1m_pct < -0.38, 8.0)
        short_score -= penalty_if(ret_5m_pct < -0.55, 6.0)
        short_score -= penalty_if(frame5.range_position < -0.10, 10.0)
        short_score -= penalty_if(frame15.range_position < -0.10, 12.0)
        short_score -= penalty_if(frame5.bounce_pct < 0.01, 8.0)
        short_score -= penalty_if(frame15.ret_3bar_pct > 0.05, 8.0)
        short_score -= penalty_if(frame5.trend_gap_pct > 0.0, 8.0)
        short_score -= penalty_if(frame15.trend_gap_pct > 0.08, 8.0)
        short_score -= penalty_if(price_change_24h_pct > 1.2, 8.0)
        short_score -= penalty_if(ret_1m_pct < -0.18, 14.0)
        short_score -= penalty_if(range_position < 0.0, 12.0)
        short_score -= penalty_if(symbol in SLOW_SYMBOLS, 4.0)
        if flat_regime:
            short_score -= 15.0
        if bounce_pct < 0.03 and (
            frame5.bounce_pct < 0.05
            or frame15.ret_3bar_pct > -0.35
            or frame5.ret_6bar_pct > -0.18
            or frame15.trend_gap_pct > -0.06
        ):
            short_score -= 18.0
        if bounce_pct >= 0.03 and (frame5.trend_gap_pct > -0.03 or frame15.trend_gap_pct > -0.04):
            short_score -= 8.0

    impulse_liquidity_ok = (
        (
            avg_quote_turnover >= 350_000.0
            and frame5.avg_quote_turnover >= 600_000.0
            and frame15.avg_quote_turnover >= 900_000.0
        )
        or (
            quote_volume_24h >= 300_000_000.0
            and avg_quote_turnover >= 1_000_000.0
            and frame5.avg_quote_turnover >= 1_500_000.0
            and frame15.avg_quote_turnover >= 2_250_000.0
        )
    )

    impulse_pullback_long_score = 0.0
    impulse_pullback_long_ok = (
        impulse_liquidity_ok
        and (last_close >= 0.10 or quote_volume_24h >= 12_000_000_000.0)
        and -1.8 <= price_change_24h_pct <= 7.0
        and -0.05 <= ret_30m_pct <= 2.2
        and -0.25 <= ret_15m_pct <= 0.65
        and -0.05 <= ret_60m_pct <= 3.5
        and -0.05 <= frame15.ret_6bar_pct <= 1.2
        and 0.0 <= frame5.ret_6bar_pct <= 1.2
        and -0.30 <= frame5.ret_3bar_pct <= 0.22
        and -0.03 <= frame15.trend_gap_pct <= 0.35
        and -0.04 <= frame5.trend_gap_pct <= 0.25
        and higher_tf_trend_gap_pct >= -0.06
        and 0.45 <= volume_ratio <= 4.0
        and 0.20 <= frame5.volume_ratio <= 3.5
        and 0.15 <= frame15.volume_ratio <= 3.5
        and 0.18 <= range_position <= 0.72
        and 0.12 <= frame5.range_position <= 0.62
        and 0.10 <= frame15.range_position <= 0.66
        and ret_1m_pct >= 0.02
        and ret_5m_pct >= 0.08
        and volume_ratio >= 0.90
        and pullback_pct <= -0.04
        and frame5.pullback_pct <= -0.04
    )
    if impulse_pullback_long_ok:
        impulse_pullback_long_score += 22.0
        impulse_pullback_long_score += bounded_score(ret_30m_pct, center=0.30, half_width=1.2, max_score=12.0)
        impulse_pullback_long_score += bounded_score(frame5.ret_6bar_pct, center=0.22, half_width=0.60, max_score=10.0)
        impulse_pullback_long_score += bounded_score(frame15.ret_6bar_pct, center=0.28, half_width=0.80, max_score=10.0)
        impulse_pullback_long_score += bounded_score(frame5.pullback_pct, center=-0.08, half_width=0.14, max_score=12.0)
        impulse_pullback_long_score += bounded_score(frame5.range_position, center=0.44, half_width=0.26, max_score=10.0)
        impulse_pullback_long_score += bounded_score(frame15.range_position, center=0.40, half_width=0.24, max_score=8.0)
        impulse_pullback_long_score += bounded_score(volume_ratio, center=1.15, half_width=0.90, max_score=6.0)
        impulse_pullback_long_score += bounded_score(frame15.trend_gap_pct, center=0.05, half_width=0.18, max_score=8.0)
        impulse_pullback_long_score -= penalty_if(ret_1m_pct > 0.16, 10.0)
        impulse_pullback_long_score -= penalty_if(range_position > 0.70, 18.0)
        impulse_pullback_long_score -= penalty_if(frame5.range_position > 0.62, 16.0)
        impulse_pullback_long_score -= penalty_if(frame5.ret_3bar_pct > 0.14, 8.0)
        impulse_pullback_long_score -= penalty_if(price_change_24h_pct > 4.5, 6.0)
        impulse_pullback_long_score -= penalty_if(symbol in SLOW_SYMBOLS, 3.0)

    impulse_bounce_short_score = 0.0
    impulse_bounce_short_ok = (
        allow_short
        and impulse_liquidity_ok
        and -7.0 <= price_change_24h_pct <= 1.8
        and -2.2 <= ret_30m_pct <= 0.05
        and -0.65 <= ret_15m_pct <= 0.25
        and -3.5 <= ret_60m_pct <= 0.05
        and -2.0 <= frame15.ret_6bar_pct <= 0.05
        and -1.2 <= frame5.ret_6bar_pct <= 0.0
        and -0.22 <= frame5.ret_3bar_pct <= 0.30
        and -0.35 <= frame15.trend_gap_pct <= 0.03
        and -0.25 <= frame5.trend_gap_pct <= 0.04
        and higher_tf_trend_gap_pct <= 0.06
        and 0.45 <= volume_ratio <= 4.0
        and 0.45 <= frame5.volume_ratio <= 3.5
        and 0.02 <= range_position <= 0.82
        and 0.12 <= frame5.range_position <= 0.90
        and 0.18 <= frame15.range_position <= 0.90
        and bounce_pct >= -0.02
        and frame5.bounce_pct >= 0.0
    )
    if impulse_bounce_short_ok:
        impulse_bounce_short_score += 22.0
        impulse_bounce_short_score += bounded_score(-ret_30m_pct, center=0.30, half_width=1.2, max_score=12.0)
        impulse_bounce_short_score += bounded_score(-frame5.ret_6bar_pct, center=0.22, half_width=0.60, max_score=10.0)
        impulse_bounce_short_score += bounded_score(-frame15.ret_6bar_pct, center=0.28, half_width=0.80, max_score=10.0)
        impulse_bounce_short_score += bounded_score(frame5.bounce_pct, center=0.08, half_width=0.14, max_score=12.0)
        impulse_bounce_short_score += bounded_score(frame5.range_position, center=0.58, half_width=0.26, max_score=10.0)
        impulse_bounce_short_score += bounded_score(frame15.range_position, center=0.60, half_width=0.24, max_score=8.0)
        impulse_bounce_short_score += bounded_score(volume_ratio, center=1.15, half_width=0.90, max_score=6.0)
        impulse_bounce_short_score += bounded_score(-frame15.trend_gap_pct, center=0.05, half_width=0.18, max_score=8.0)
        impulse_bounce_short_score -= penalty_if(ret_1m_pct < -0.18, 8.0)
        impulse_bounce_short_score -= penalty_if(range_position < 0.08, 8.0)
        impulse_bounce_short_score -= penalty_if(frame5.ret_3bar_pct < -0.14, 8.0)
        impulse_bounce_short_score -= penalty_if(price_change_24h_pct < -4.5, 6.0)
        impulse_bounce_short_score -= penalty_if(symbol in SLOW_SYMBOLS, 3.0)

    flush_reversion_long_score = 0.0
    flush_reversion_long_ok = (
        impulse_liquidity_ok
        and -6.5 <= price_change_24h_pct <= 1.0
        and -1.0 <= ret_30m_pct <= -0.10
        and -1.5 <= ret_60m_pct <= -0.10
        and -0.60 <= ret_15m_pct <= 0.05
        and -0.60 <= frame5.ret_6bar_pct <= -0.08
        and -0.80 <= frame15.ret_6bar_pct <= -0.05
        and -0.15 <= ret_1m_pct <= 0.12
        and -0.12 <= frame5.ret_3bar_pct <= 0.10
        and -0.70 <= frame15.range_position <= 0.28
        and 0.0 <= range_position <= 0.22
        and -0.02 <= frame5.range_position <= 0.24
        and 0.50 <= volume_ratio <= 3.5
        and 0.45 <= frame5.volume_ratio <= 3.5
        and frame5.bounce_pct >= 0.0
        and higher_tf_trend_gap_pct >= -0.12
    )
    if flush_reversion_long_ok:
        flush_reversion_long_score += 18.0
        flush_reversion_long_score += bounded_score(-ret_30m_pct, center=0.30, half_width=0.90, max_score=10.0)
        flush_reversion_long_score += bounded_score(-frame5.ret_6bar_pct, center=0.20, half_width=0.45, max_score=8.0)
        flush_reversion_long_score += bounded_score(-frame15.ret_6bar_pct, center=0.24, half_width=0.55, max_score=8.0)
        flush_reversion_long_score += bounded_score(frame5.range_position, center=0.08, half_width=0.14, max_score=10.0)
        flush_reversion_long_score += bounded_score(frame15.range_position, center=0.02, half_width=0.30, max_score=8.0)
        flush_reversion_long_score += bounded_score(frame5.bounce_pct, center=0.05, half_width=0.12, max_score=8.0)
        flush_reversion_long_score += bounded_score(volume_ratio, center=1.20, half_width=1.0, max_score=6.0)
        flush_reversion_long_score -= penalty_if(ret_1m_pct < -0.12, 8.0)
        flush_reversion_long_score -= penalty_if(price_change_24h_pct < -4.5, 6.0)
        flush_reversion_long_score -= penalty_if(frame5.range_position < 0.0, 4.0)

    pressure_breakdown_short_score = 0.0
    pressure_breakdown_short_ok = (
        allow_short
        and impulse_liquidity_ok
        and -7.0 <= price_change_24h_pct <= 1.0
        and -1.4 <= ret_30m_pct <= -0.08
        and -2.5 <= ret_60m_pct <= -0.08
        and -0.45 <= ret_15m_pct <= 0.05
        and -0.70 <= frame5.ret_6bar_pct <= -0.05
        and -0.80 <= frame15.ret_6bar_pct <= 0.08
        and -0.10 <= ret_1m_pct <= 0.12
        and -0.10 <= frame5.ret_3bar_pct <= 0.18
        and -0.70 <= frame15.range_position <= 0.40
        and 0.0 <= range_position <= 0.40
        and -0.05 <= frame5.range_position <= 0.30
        and 0.45 <= volume_ratio <= 3.5
        and 0.45 <= frame5.volume_ratio <= 3.5
        and bounce_pct >= -0.03
        and higher_tf_trend_gap_pct <= 0.10
    )
    if pressure_breakdown_short_ok:
        pressure_breakdown_short_score += 20.0
        pressure_breakdown_short_score += bounded_score(-ret_30m_pct, center=0.30, half_width=1.0, max_score=10.0)
        pressure_breakdown_short_score += bounded_score(-frame5.ret_6bar_pct, center=0.18, half_width=0.50, max_score=10.0)
        pressure_breakdown_short_score += bounded_score(-frame15.ret_6bar_pct, center=0.18, half_width=0.55, max_score=8.0)
        pressure_breakdown_short_score += bounded_score(frame5.range_position, center=0.10, half_width=0.18, max_score=10.0)
        pressure_breakdown_short_score += bounded_score(max(0.0, -frame15.range_position), center=0.10, half_width=0.28, max_score=8.0)
        pressure_breakdown_short_score += bounded_score(volume_ratio, center=1.20, half_width=1.0, max_score=6.0)
        pressure_breakdown_short_score += bounded_score(-trend_gap_pct, center=0.05, half_width=0.18, max_score=6.0)
        pressure_breakdown_short_score -= penalty_if(ret_1m_pct < -0.12, 8.0)
        pressure_breakdown_short_score -= penalty_if(price_change_24h_pct < -4.8, 6.0)
        pressure_breakdown_short_score -= penalty_if(frame5.range_position < 0.0, 4.0)

    reversion_long_score = 0.0
    if (
        flat_regime
        and 0.0 <= range_position <= 0.20
        and 0.0 <= frame5.range_position <= 0.24
        and 0.15 <= frame15.range_position <= 0.48
        and -0.14 <= ret_1m_pct <= 0.10
        and -0.22 <= ret_5m_pct <= 0.08
        and -0.18 <= frame5.ret_3bar_pct <= 0.04
        and -0.35 <= frame15.ret_3bar_pct <= 0.10
        and volume_ratio <= 2.1
        and 0.8 <= frame5.volume_ratio <= 1.8
        and frame5.pullback_pct <= -0.06
    ):
        reversion_long_score += 12.0
        reversion_long_score += bounded_score(range_position, center=0.06, half_width=0.10, max_score=10.0)
        reversion_long_score += bounded_score(frame5.range_position, center=0.10, half_width=0.12, max_score=8.0)
        reversion_long_score += bounded_score(-ret_1m_pct, center=0.05, half_width=0.16, max_score=8.0)
        reversion_long_score += bounded_score(-ret_5m_pct, center=0.08, half_width=0.18, max_score=6.0)
        reversion_long_score += bounded_score(frame5.volume_ratio, center=1.1, half_width=0.45, max_score=6.0)
        reversion_long_score += bounded_score(bounce_pct, center=0.03, half_width=0.08, max_score=6.0)
        reversion_long_score -= penalty_if(last_close < 0.2, 12.0)

    reversion_short_score = 0.0
    if (
        flat_regime
        and 0.80 <= range_position <= 1.0
        and 0.76 <= frame5.range_position <= 1.0
        and 0.52 <= frame15.range_position <= 0.85
        and -0.10 <= ret_1m_pct <= 0.14
        and -0.08 <= ret_5m_pct <= 0.22
        and -0.04 <= frame5.ret_3bar_pct <= 0.18
        and -0.10 <= frame15.ret_3bar_pct <= 0.35
        and volume_ratio <= 2.1
        and 0.8 <= frame5.volume_ratio <= 1.8
        and frame5.bounce_pct >= 0.06
        and allow_short
    ):
        reversion_short_score += 12.0
        reversion_short_score += bounded_score(1.0 - range_position, center=0.06, half_width=0.10, max_score=10.0)
        reversion_short_score += bounded_score(1.0 - frame5.range_position, center=0.10, half_width=0.12, max_score=8.0)
        reversion_short_score += bounded_score(ret_1m_pct, center=0.05, half_width=0.16, max_score=8.0)
        reversion_short_score += bounded_score(ret_5m_pct, center=0.08, half_width=0.18, max_score=6.0)
        reversion_short_score += bounded_score(frame5.volume_ratio, center=1.1, half_width=0.45, max_score=6.0)
        reversion_short_score += bounded_score(abs(pullback_pct), center=0.03, half_width=0.08, max_score=6.0)
        reversion_short_score -= penalty_if(last_close < 0.2, 12.0)

    micro_liquidity = (
        quote_volume_24h >= 20_000_000_000.0
        and avg_quote_turnover >= 12_000_000.0
        and frame5.avg_quote_turnover >= 12_000_000.0
        and frame15.avg_quote_turnover >= 15_000_000.0
        and last_close >= 0.10
    )
    micro_range_regime = (
        micro_liquidity
        and abs(ret_30m_pct) <= 0.45
        and abs(ret_15m_pct) <= 0.22
        and abs(frame15.ret_3bar_pct) <= 0.35
        and abs(frame5.ret_3bar_pct) <= 0.18
        and abs(trend_gap_pct) <= 0.05
        and abs(frame5.trend_gap_pct) <= 0.05
        and abs(frame15.trend_gap_pct) <= 0.06
        and 0.55 <= volume_ratio <= 2.8
        and 0.45 <= frame5.volume_ratio <= 2.5
        and 0.25 <= frame15.volume_ratio <= 2.5
    )

    micro_reclaim_long_score = 0.0
    micro_reclaim_long_ok = (
        micro_range_regime
        and 0.0 <= range_position <= 0.24
        and 0.0 <= frame5.range_position <= 0.34
        and 0.12 <= frame15.range_position <= 0.55
        and -0.16 <= ret_1m_pct <= 0.03
        and -0.28 <= ret_5m_pct <= 0.04
        and -0.18 <= frame5.ret_3bar_pct <= 0.05
        and frame5.pullback_pct <= -0.05
        and pullback_pct <= -0.04
        and higher_tf_trend_gap_pct >= -0.04
    )
    if micro_reclaim_long_ok:
        micro_reclaim_long_score += 18.0
        micro_reclaim_long_score += bounded_score(range_position, center=0.06, half_width=0.12, max_score=10.0)
        micro_reclaim_long_score += bounded_score(frame5.range_position, center=0.10, half_width=0.16, max_score=10.0)
        micro_reclaim_long_score += bounded_score(-ret_1m_pct, center=0.05, half_width=0.15, max_score=8.0)
        micro_reclaim_long_score += bounded_score(-ret_5m_pct, center=0.10, half_width=0.22, max_score=8.0)
        micro_reclaim_long_score += bounded_score(frame5.volume_ratio, center=1.05, half_width=0.60, max_score=6.0)
        micro_reclaim_long_score += bounded_score(bounce_pct, center=0.03, half_width=0.08, max_score=6.0)
        micro_reclaim_long_score -= penalty_if(ret_1m_pct > 0.02, 8.0)
        micro_reclaim_long_score -= penalty_if(range_position > 0.20, 6.0)

    micro_reject_short_score = 0.0
    micro_reject_short_ok = (
        allow_short
        and micro_range_regime
        and 0.76 <= range_position <= 1.0
        and 0.66 <= frame5.range_position <= 1.0
        and 0.45 <= frame15.range_position <= 0.88
        and -0.03 <= ret_1m_pct <= 0.16
        and -0.04 <= ret_5m_pct <= 0.28
        and -0.05 <= frame5.ret_3bar_pct <= 0.18
        and frame5.bounce_pct >= 0.05
        and bounce_pct >= 0.04
        and higher_tf_trend_gap_pct <= 0.04
    )
    if micro_reject_short_ok:
        micro_reject_short_score += 18.0
        micro_reject_short_score += bounded_score(1.0 - range_position, center=0.06, half_width=0.12, max_score=10.0)
        micro_reject_short_score += bounded_score(1.0 - frame5.range_position, center=0.10, half_width=0.16, max_score=10.0)
        micro_reject_short_score += bounded_score(ret_1m_pct, center=0.05, half_width=0.15, max_score=8.0)
        micro_reject_short_score += bounded_score(ret_5m_pct, center=0.10, half_width=0.22, max_score=8.0)
        micro_reject_short_score += bounded_score(frame5.volume_ratio, center=1.05, half_width=0.60, max_score=6.0)
        micro_reject_short_score += bounded_score(abs(pullback_pct), center=0.03, half_width=0.08, max_score=6.0)
        micro_reject_short_score -= penalty_if(ret_1m_pct < -0.02, 8.0)
        micro_reject_short_score -= penalty_if(range_position < 0.80, 6.0)

    exploration_liquidity = (
        quote_volume_24h >= 5_000_000_000.0
    )
    exploration_liquidity = (
        exploration_liquidity
        and avg_quote_turnover >= 8_000_000.0
        and frame5.avg_quote_turnover >= 10_000_000.0
        and frame15.avg_quote_turnover >= 12_000_000.0
    )

    explore_reclaim_long_score = 0.0
    explore_reclaim_long_ok = (
        exploration_liquidity
        and -1.2 <= price_change_24h_pct <= 3.5
        and -0.16 <= ret_15m_pct <= 0.55
        and -0.30 <= ret_30m_pct <= 0.75
        and -0.20 <= frame15.ret_3bar_pct <= 0.42
        and -0.14 <= frame5.ret_3bar_pct <= 0.16
        and -0.30 <= frame5.ret_6bar_pct <= 0.32
        and -0.06 <= frame15.trend_gap_pct <= 0.06
        and -0.06 <= frame5.trend_gap_pct <= 0.06
        and higher_tf_trend_gap_pct >= -0.06
        and 0.95 <= volume_ratio <= 4.0
        and 0.70 <= frame5.volume_ratio <= 3.2
        and 0.18 <= range_position <= 0.78
        and 0.18 <= frame5.range_position <= 0.68
        and 0.12 <= frame15.range_position <= 0.62
        and pullback_pct <= -0.035
        and frame5.pullback_pct <= -0.045
        and ret_1m_pct >= -0.02
        and ret_5m_pct >= -0.05
    )
    if explore_reclaim_long_ok:
        explore_reclaim_long_score += 14.0
        explore_reclaim_long_score += bounded_score(frame5.pullback_pct, center=-0.06, half_width=0.10, max_score=12.0)
        explore_reclaim_long_score += bounded_score(frame15.range_position, center=0.34, half_width=0.28, max_score=10.0)
        explore_reclaim_long_score += bounded_score(frame5.range_position, center=0.42, half_width=0.30, max_score=10.0)
        explore_reclaim_long_score += bounded_score(ret_15m_pct, center=0.05, half_width=0.35, max_score=8.0)
        explore_reclaim_long_score += bounded_score(frame5.ret_3bar_pct, center=-0.01, half_width=0.16, max_score=8.0)
        explore_reclaim_long_score += bounded_score(volume_ratio, center=1.5, half_width=2.0, max_score=8.0)
        explore_reclaim_long_score += bounded_score(frame15.trend_gap_pct, center=-0.01, half_width=0.08, max_score=6.0)
        explore_reclaim_long_score -= penalty_if(range_position > 0.72, 16.0)
        explore_reclaim_long_score -= penalty_if(frame5.range_position > 0.66, 14.0)
        explore_reclaim_long_score -= penalty_if(ret_1m_pct > 0.12, 8.0)
        explore_reclaim_long_score -= penalty_if(ret_1m_pct < -0.03, 10.0)
        explore_reclaim_long_score -= penalty_if(frame5.ret_3bar_pct > 0.10, 8.0)
        explore_reclaim_long_score -= penalty_if(volume_ratio < 1.05, 8.0)

    explore_reject_short_score = 0.0
    explore_reject_short_ok = (
        allow_short
        and exploration_liquidity
        and -3.5 <= price_change_24h_pct <= 0.6
        and -0.60 <= ret_15m_pct <= 0.06
        and -0.80 <= ret_30m_pct <= 0.10
        and -0.45 <= frame15.ret_3bar_pct <= 0.12
        and -0.18 <= frame5.ret_3bar_pct <= 0.22
        and -0.35 <= frame5.ret_6bar_pct <= 0.35
        and -0.08 <= frame15.trend_gap_pct <= 0.02
        and -0.08 <= frame5.trend_gap_pct <= 0.08
        and higher_tf_trend_gap_pct <= 0.03
        and 0.65 <= volume_ratio <= 6.0
        and 0.35 <= frame5.volume_ratio <= 4.5
        and -0.08 <= range_position <= 0.82
        and 0.18 <= frame5.range_position <= 0.88
        and 0.18 <= frame15.range_position <= 0.82
        and bounce_pct >= 0.01
        and frame5.bounce_pct >= 0.02
    )
    if explore_reject_short_ok:
        explore_reject_short_score += 14.0
        explore_reject_short_score += bounded_score(frame5.bounce_pct, center=0.06, half_width=0.10, max_score=12.0)
        explore_reject_short_score += bounded_score(frame15.range_position, center=0.62, half_width=0.28, max_score=10.0)
        explore_reject_short_score += bounded_score(frame5.range_position, center=0.58, half_width=0.30, max_score=10.0)
        explore_reject_short_score += bounded_score(-ret_15m_pct, center=0.05, half_width=0.35, max_score=8.0)
        explore_reject_short_score += bounded_score(-frame5.ret_3bar_pct, center=-0.01, half_width=0.16, max_score=8.0)
        explore_reject_short_score += bounded_score(volume_ratio, center=1.5, half_width=2.0, max_score=8.0)
        explore_reject_short_score += bounded_score(-frame15.trend_gap_pct, center=-0.01, half_width=0.08, max_score=6.0)
        explore_reject_short_score -= penalty_if(range_position < -0.02, 8.0)
        explore_reject_short_score -= penalty_if(ret_1m_pct < -0.14, 6.0)
        explore_reject_short_score -= penalty_if(frame5.ret_3bar_pct < -0.12, 6.0)
        explore_reject_short_score -= penalty_if(ret_15m_pct > 0.02, 10.0)
        explore_reject_short_score -= penalty_if(ret_30m_pct > 0.05, 12.0)

    major_reclaim_long_score = 0.0
    major_reclaim_long_ok = (
        quote_volume_24h >= 5_000_000_000.0
        and avg_quote_turnover >= 1_200_000.0
        and frame5.avg_quote_turnover >= 2_000_000.0
        and frame15.avg_quote_turnover >= 3_000_000.0
        and -0.4 <= price_change_24h_pct <= 1.8
        and -0.18 <= ret_15m_pct <= 0.18
        and -0.22 <= ret_30m_pct <= 0.18
        and -0.08 <= trend_gap_pct <= 0.10
        and -0.06 <= frame5.trend_gap_pct <= 0.03
        and -0.06 <= frame15.trend_gap_pct <= 0.03
        and higher_tf_trend_gap_pct >= -0.05
        and 0.65 <= volume_ratio <= 4.2
        and 0.12 <= range_position <= 0.78
        and 0.14 <= frame5.range_position <= 0.50
        and 0.12 <= frame15.range_position <= 0.58
        and frame5.pullback_pct <= -0.05
        and frame15.pullback_pct <= -0.08
        and -0.18 <= ret_1m_pct <= 0.08
        and -0.25 <= frame5.ret_3bar_pct <= 0.05
        and -0.30 <= frame5.ret_6bar_pct <= 0.12
    )
    if major_reclaim_long_ok:
        major_reclaim_long_score += 18.0
        major_reclaim_long_score += bounded_score(frame5.pullback_pct, center=-0.09, half_width=0.12, max_score=12.0)
        major_reclaim_long_score += bounded_score(frame15.pullback_pct, center=-0.14, half_width=0.18, max_score=10.0)
        major_reclaim_long_score += bounded_score(frame5.range_position, center=0.30, half_width=0.24, max_score=10.0)
        major_reclaim_long_score += bounded_score(frame15.range_position, center=0.28, half_width=0.24, max_score=10.0)
        major_reclaim_long_score += bounded_score(ret_15m_pct, center=0.08, half_width=0.28, max_score=8.0)
        major_reclaim_long_score += bounded_score(volume_ratio, center=1.5, half_width=1.5, max_score=6.0)
        major_reclaim_long_score += bounded_score(frame5.trend_gap_pct, center=-0.02, half_width=0.05, max_score=8.0)
        major_reclaim_long_score -= penalty_if(range_position > 0.72, 10.0)
        major_reclaim_long_score -= penalty_if(ret_15m_pct > 0.12, 8.0)
        major_reclaim_long_score -= penalty_if(ret_1m_pct > 0.06, 8.0)
        major_reclaim_long_score -= penalty_if(frame5.ret_3bar_pct > 0.04, 10.0)

    swing_long_score = 0.0
    swing_long_ok = (
        quote_volume_24h >= 5_000_000_000.0
        and frame15.trend_gap_pct >= 0.05
        and frame15.higher_trend_gap_pct >= 0.03
        and 0.22 <= frame15.ret_6bar_pct <= 3.8
        and 0.08 <= frame5.ret_6bar_pct <= 1.8
        and -0.55 <= frame5.ret_3bar_pct <= 0.45
        and -0.45 <= ret_15m_pct <= 0.35
        and 0.18 <= frame15.range_position <= 0.82
        and 0.18 <= frame5.range_position <= 0.72
        and 0.60 <= volume_ratio <= 2.4
        and 0.55 <= frame5.volume_ratio <= 2.3
        and pullback_pct <= -0.02
        and frame5.pullback_pct <= -0.03
    )
    if swing_long_ok:
        swing_long_score += 30.0
        swing_long_score += bounded_score(frame15.ret_6bar_pct, center=1.0, half_width=2.2, max_score=16.0)
        swing_long_score += bounded_score(frame5.ret_6bar_pct, center=0.35, half_width=1.1, max_score=12.0)
        swing_long_score += bounded_score(frame5.ret_3bar_pct, center=0.02, half_width=0.6, max_score=8.0)
        swing_long_score += bounded_score(frame15.range_position, center=0.46, half_width=0.28, max_score=10.0)
        swing_long_score += bounded_score(frame5.range_position, center=0.38, half_width=0.24, max_score=10.0)
        swing_long_score += bounded_score(frame15.trend_gap_pct, center=0.11, half_width=0.20, max_score=12.0)
        swing_long_score += bounded_score(frame5.pullback_pct, center=-0.12, half_width=0.22, max_score=10.0)
        swing_long_score -= penalty_if(frame5.pullback_pct > -0.02, 8.0)
        swing_long_score -= penalty_if(frame15.range_position > 0.80, 12.0)

    swing_short_score = 0.0
    swing_short_ok = (
        allow_short
        and quote_volume_24h >= 5_000_000_000.0
        and frame15.trend_gap_pct <= -0.05
        and frame15.higher_trend_gap_pct <= -0.03
        and -3.8 <= frame15.ret_6bar_pct <= -0.22
        and -1.8 <= frame5.ret_6bar_pct <= -0.08
        and -0.45 <= frame5.ret_3bar_pct <= 0.55
        and -0.35 <= ret_15m_pct <= 0.45
        and 0.18 <= frame15.range_position <= 0.82
        and 0.28 <= frame5.range_position <= 0.82
        and 0.60 <= volume_ratio <= 2.4
        and 0.55 <= frame5.volume_ratio <= 2.3
        and bounce_pct >= 0.02
        and frame5.bounce_pct >= 0.03
    )
    if swing_short_ok:
        swing_short_score += 30.0
        swing_short_score += bounded_score(-frame15.ret_6bar_pct, center=1.0, half_width=2.2, max_score=16.0)
        swing_short_score += bounded_score(-frame5.ret_6bar_pct, center=0.35, half_width=1.1, max_score=12.0)
        swing_short_score += bounded_score(-frame5.ret_3bar_pct, center=0.02, half_width=0.6, max_score=8.0)
        swing_short_score += bounded_score(frame15.range_position, center=0.54, half_width=0.28, max_score=10.0)
        swing_short_score += bounded_score(frame5.range_position, center=0.62, half_width=0.24, max_score=10.0)
        swing_short_score += bounded_score(-frame15.trend_gap_pct, center=0.11, half_width=0.20, max_score=12.0)
        swing_short_score += bounded_score(frame5.bounce_pct, center=0.12, half_width=0.22, max_score=10.0)
        swing_short_score -= penalty_if(frame5.bounce_pct < 0.02, 8.0)
        swing_short_score -= penalty_if(frame15.range_position < 0.20, 12.0)

    volatility_long_score = 0.0
    volatility_long_ok = (
        quote_volume_24h >= 8_000_000_000.0
        and avg_quote_turnover >= 2_000_000.0
        and frame5.avg_quote_turnover >= 2_500_000.0
        and frame15.avg_quote_turnover >= 3_500_000.0
        and 5.0 <= price_change_24h_pct <= 80.0
        and -0.80 <= ret_15m_pct <= 12.0
        and -0.80 <= ret_30m_pct <= 14.0
        and -0.80 <= ret_60m_pct <= 20.0
        and -0.80 <= frame5.ret_3bar_pct <= 12.0
        and -0.30 <= frame5.ret_6bar_pct <= 10.0
        and -1.20 <= frame15.ret_3bar_pct <= 10.0
        and -1.20 <= frame15.ret_6bar_pct <= 16.0
        and frame5.trend_gap_pct >= -0.15
        and frame15.trend_gap_pct >= -1.25
        and 0.20 <= volume_ratio <= 7.5
        and 0.0 <= frame5.range_position <= 2.5
        and 0.0 <= frame15.range_position <= 2.4
        and frame5.volume_ratio <= 7.5
        and frame15.volume_ratio <= 5.5
        and frame5.pullback_pct <= 0.35
        and frame15.pullback_pct <= 3.0
    )
    if volatility_long_ok:
        volatility_long_score += 32.0
        volatility_long_score += bounded_score(ret_15m_pct, center=1.2, half_width=3.0, max_score=16.0)
        volatility_long_score += bounded_score(frame5.ret_3bar_pct, center=0.9, half_width=2.4, max_score=12.0)
        volatility_long_score += bounded_score(frame15.ret_3bar_pct, center=0.6, half_width=2.0, max_score=10.0)
        volatility_long_score += bounded_score(volume_ratio, center=1.4, half_width=1.6, max_score=8.0)
        volatility_long_score += bounded_score(frame5.range_position, center=0.78, half_width=0.7, max_score=8.0)
        volatility_long_score -= penalty_if(price_change_24h_pct > 18.0, 8.0)
        volatility_long_score -= penalty_if(frame5.volume_ratio > 5.5, 6.0)
        volatility_long_score -= penalty_if(frame15.volume_ratio > 4.0, 8.0)

    volatility_short_score = 0.0
    volatility_short_ok = (
        allow_short
        and quote_volume_24h >= 8_000_000_000.0
        and avg_quote_turnover >= 2_000_000.0
        and frame5.avg_quote_turnover >= 2_500_000.0
        and frame15.avg_quote_turnover >= 3_500_000.0
        and -80.0 <= price_change_24h_pct <= -5.0
        and -12.0 <= ret_15m_pct <= 0.80
        and -14.0 <= ret_30m_pct <= 0.80
        and -20.0 <= ret_60m_pct <= 0.80
        and -12.0 <= frame5.ret_3bar_pct <= 0.80
        and -10.0 <= frame5.ret_6bar_pct <= 0.30
        and -10.0 <= frame15.ret_3bar_pct <= 1.20
        and -16.0 <= frame15.ret_6bar_pct <= 1.20
        and frame5.trend_gap_pct <= 0.15
        and frame15.trend_gap_pct <= 1.25
        and 0.20 <= volume_ratio <= 7.5
        and -0.25 <= frame5.range_position <= 2.0
        and -0.25 <= frame15.range_position <= 2.0
        and frame5.volume_ratio <= 7.5
        and frame15.volume_ratio <= 5.5
        and frame5.bounce_pct <= 0.35
        and abs(frame15.pullback_pct) <= 3.0
    )
    if volatility_short_ok:
        volatility_short_score += 32.0
        volatility_short_score += bounded_score(-ret_15m_pct, center=1.2, half_width=3.0, max_score=16.0)
        volatility_short_score += bounded_score(-frame5.ret_3bar_pct, center=0.9, half_width=2.4, max_score=12.0)
        volatility_short_score += bounded_score(-frame15.ret_3bar_pct, center=0.6, half_width=2.0, max_score=10.0)
        volatility_short_score += bounded_score(volume_ratio, center=1.4, half_width=1.6, max_score=8.0)
        volatility_short_score += bounded_score(1.0 - frame5.range_position, center=0.78, half_width=0.7, max_score=8.0)
        volatility_short_score -= penalty_if(price_change_24h_pct < -18.0, 8.0)
        volatility_short_score -= penalty_if(frame5.volume_ratio > 5.5, 6.0)
        volatility_short_score -= penalty_if(frame15.volume_ratio > 4.0, 8.0)

    scored_setups = [
        ("impulse_pullback_long", "BUY", impulse_pullback_long_score),
        ("impulse_bounce_short", "SELL", impulse_bounce_short_score),
        ("micro_reclaim_long", "BUY", micro_reclaim_long_score),
        ("micro_reject_short", "SELL", micro_reject_short_score),
        ("flush_reversion_long", "BUY", flush_reversion_long_score),
        ("pressure_breakdown_short", "SELL", pressure_breakdown_short_score),
        ("volatility_long", "BUY", volatility_long_score),
        ("volatility_short", "SELL", volatility_short_score),
        ("explore_reclaim_long", "BUY", explore_reclaim_long_score),
        ("explore_reject_short", "SELL", explore_reject_short_score),
        (
            "trend_pullback_long",
            "BUY",
            max(
                0.0,
                (long_score if frame5.pullback_pct <= -0.03 or pullback_pct <= -0.02 else 0.0)
                - (
                    18.0
                    if (
                        avg_quote_turnover < 2_500_000.0
                        or current_quote_turnover < 250_000.0
                        or frame5.avg_quote_turnover < 3_000_000.0
                    )
                    else 0.0
                ),
            ),
        ),
        (
            "failed_bounce_short",
            "SELL",
            max(
                0.0,
                (short_score if frame5.bounce_pct >= 0.04 or bounce_pct >= 0.03 else 0.0)
                - (
                    18.0
                    if (
                        avg_quote_turnover < 2_500_000.0
                        or current_quote_turnover < 250_000.0
                        or frame5.avg_quote_turnover < 3_000_000.0
                    )
                    else 0.0
                ),
            ),
        ),
        (
            "breakdown_continuation_short",
            "SELL",
            max(
                0.0,
                (max(0.0, short_score - 6.0) if frame5.bounce_pct < 0.05 and bounce_pct < 0.04 else 0.0)
                - (
                    16.0
                    if (
                        avg_quote_turnover < 2_000_000.0
                        or current_quote_turnover < 200_000.0
                        or frame5.avg_quote_turnover < 2_500_000.0
                    )
                    else 0.0
                ),
            ),
        ),
        ("range_reversion_long", "BUY", reversion_long_score),
        ("range_reversion_short", "SELL", reversion_short_score),
        ("major_reclaim_long", "BUY", major_reclaim_long_score),
        ("swing_pullback_long", "BUY", swing_long_score),
        ("swing_bounce_short", "SELL", swing_short_score),
    ]
    setup, side, score = max(scored_setups, key=lambda item: item[2])
    if score <= 0:
        return None
    if score < 18.0:
        return None

    return Candidate(
        symbol=symbol,
        setup=setup,
        side=side,
        score=score,
        mark_price=last_close,
        quote_volume_24h=quote_volume_24h,
        avg_quote_turnover_1m=avg_quote_turnover,
        current_quote_turnover_1m=current_quote_turnover,
        price_change_24h_pct=price_change_24h_pct,
        ret_1m_pct=ret_1m_pct,
        ret_3m_pct=ret_3m_pct,
        ret_5m_pct=ret_5m_pct,
        ret_15m_pct=ret_15m_pct,
        ret_30m_pct=ret_30m_pct,
        ret_60m_pct=ret_60m_pct,
        volume_ratio=volume_ratio,
        range_position=range_position,
        trend_gap_pct=trend_gap_pct,
        higher_tf_trend_gap_pct=higher_tf_trend_gap_pct,
        pullback_pct=pullback_pct,
        bounce_pct=bounce_pct,
        tf5_ret_3bar_pct=frame5.ret_3bar_pct,
        tf5_ret_6bar_pct=frame5.ret_6bar_pct,
        tf5_volume_ratio=frame5.volume_ratio,
        tf5_range_position=frame5.range_position,
        tf5_trend_gap_pct=frame5.trend_gap_pct,
        tf5_pullback_pct=frame5.pullback_pct,
        tf15_ret_3bar_pct=frame15.ret_3bar_pct,
        tf15_ret_6bar_pct=frame15.ret_6bar_pct,
        tf15_volume_ratio=frame15.volume_ratio,
        tf15_range_position=frame15.range_position,
        tf15_trend_gap_pct=frame15.trend_gap_pct,
        tf15_pullback_pct=frame15.pullback_pct,
    )


def select_leverage(candidate: Candidate, *, max_long_leverage: int, max_short_leverage: int) -> int:
    if candidate.setup.startswith("impulse_"):
        leverage = 3
        if candidate.score >= 72.0 and candidate.avg_quote_turnover_1m >= 12_000_000.0:
            leverage = 4
        if (
            candidate.score >= 80.0
            and candidate.symbol not in SLOW_SYMBOLS
            and candidate.avg_quote_turnover_1m >= 25_000_000.0
        ):
            leverage = 5
        return max(1, min(leverage, max_long_leverage if candidate.side == "BUY" else max_short_leverage))
    if candidate.setup.startswith("micro_"):
        leverage = 2
        if candidate.score >= 46.0 and candidate.avg_quote_turnover_1m >= 20_000_000.0:
            leverage = 3
        return max(1, min(leverage, max_long_leverage if candidate.side == "BUY" else max_short_leverage))
    if candidate.setup == "pressure_breakdown_short":
        leverage = 2
        if candidate.score >= 68.0 and candidate.avg_quote_turnover_1m >= 12_000_000.0 and candidate.mark_price >= 1.0:
            leverage = 3
        return max(1, min(leverage, max_short_leverage))
    if candidate.setup == "flush_reversion_long":
        leverage = 2
        if candidate.score >= 62.0 and candidate.avg_quote_turnover_1m >= 10_000_000.0:
            leverage = 3
        return max(1, min(leverage, max_long_leverage))
    if candidate.setup.startswith("swing_"):
        leverage = 1
        if candidate.score >= 78.0 and candidate.avg_quote_turnover_1m >= 40_000_000.0:
            leverage = 2
        if candidate.side == "BUY":
            return max(1, min(leverage, max_long_leverage))
        return max(1, min(leverage, max_short_leverage))
    if candidate.setup == "explore_reclaim_long":
        leverage = 3
        if candidate.score >= 42.0 and candidate.avg_quote_turnover_1m >= 15_000_000.0:
            leverage = 4
        if candidate.score >= 52.0 and candidate.avg_quote_turnover_1m >= 25_000_000.0:
            leverage = 5
        return max(1, min(leverage, max_long_leverage))
    if candidate.setup == "explore_reject_short":
        leverage = 2
        if candidate.score >= 56.0 and candidate.avg_quote_turnover_1m >= 25_000_000.0:
            leverage = 3
        return max(1, min(leverage, max_short_leverage))
    if candidate.setup.startswith("major_"):
        leverage = 2
        if candidate.score >= 54.0 and candidate.avg_quote_turnover_1m >= 25_000_000.0:
            leverage = 3
        if candidate.score >= 62.0 and candidate.avg_quote_turnover_1m >= 40_000_000.0:
            leverage = 4
        return max(1, min(leverage, max_long_leverage if candidate.side == "BUY" else max_short_leverage))
    if candidate.setup.startswith("range_reversion"):
        return 1
    if candidate.side == "BUY":
        leverage = 2
        if (
            candidate.score >= 60.0
            and candidate.symbol not in SLOW_SYMBOLS
            and candidate.avg_quote_turnover_1m >= 10_000_000.0
            and abs(candidate.price_change_24h_pct) <= 6.0
        ):
            leverage = 3
        if (
            candidate.score >= 72.0
            and candidate.symbol not in SLOW_SYMBOLS
            and candidate.avg_quote_turnover_1m >= 24_000_000.0
        ):
            leverage = 4
        return max(1, min(leverage, max_long_leverage))
    leverage = 2
    if (
        candidate.score >= 58.0
        and candidate.symbol not in SLOW_SYMBOLS
        and candidate.avg_quote_turnover_1m >= 10_000_000.0
        and abs(candidate.price_change_24h_pct) <= 6.0
    ):
        leverage = 3
    if (
        candidate.score >= 72.0
        and candidate.symbol not in SLOW_SYMBOLS
        and candidate.avg_quote_turnover_1m >= 24_000_000.0
        and candidate.mark_price >= 1.0
    ):
        leverage = 4
    return max(1, min(leverage, max_short_leverage))


def select_quote_allocation(candidate: Candidate, *, base_quote: float, leverage: int) -> float:
    allocation = base_quote
    if candidate.setup.startswith("impulse_"):
        allocation *= 1.45
    if candidate.setup.startswith("micro_"):
        allocation *= 1.05
    if candidate.setup in {"pressure_breakdown_short", "flush_reversion_long"}:
        allocation *= 1.25
    if candidate.setup.startswith("explore_"):
        allocation *= 1.35
    if candidate.setup == "trend_pullback_long" or candidate.setup == "failed_bounce_short":
        allocation *= 1.20
    if candidate.score >= 46.0:
        allocation *= 1.1
    if candidate.score >= 70.0:
        allocation *= 1.15
    if candidate.score >= 58.0:
        allocation *= 1.15
    if candidate.setup.startswith("swing_"):
        allocation *= 1.25
    if candidate.setup.startswith("range_reversion"):
        allocation *= 0.7
    if candidate.avg_quote_turnover_1m < 5_000_000.0:
        allocation *= 0.8
    if candidate.mark_price < 0.12:
        allocation *= 0.75
    if candidate.symbol in SLOW_SYMBOLS and not candidate.setup.startswith("range_reversion"):
        allocation *= 0.8
    if leverage >= 150:
        allocation *= 0.18
    elif leverage >= 100:
        allocation *= 0.28
    elif leverage >= 50:
        allocation *= 0.45
    elif leverage >= 30:
        allocation *= 0.65
    elif leverage >= 20:
        allocation *= 0.90
    elif leverage <= 10:
        allocation *= 1.10
    return round(allocation, 2)


def adjust_trade_profile_for_leverage(profile: TradeProfile, *, candidate: Candidate, leverage: int) -> TradeProfile:
    if leverage >= 150:
        return TradeProfile(
            stop_pct=profile.stop_pct * 0.42,
            take_pct=profile.take_pct * 0.45,
            breakeven_trigger_pct=max(0.03, profile.breakeven_trigger_pct * 0.45),
            trail_trigger_pct=max(0.05, profile.trail_trigger_pct * 0.50),
            trail_gap_pct=max(0.02, profile.trail_gap_pct * 0.55),
            stale_after_sec=max(8, int(profile.stale_after_sec * 0.22)),
            min_progress_pct=max(0.02, profile.min_progress_pct * 0.50),
        )
    if leverage >= 100:
        return TradeProfile(
            stop_pct=profile.stop_pct * 0.50,
            take_pct=profile.take_pct * 0.58,
            breakeven_trigger_pct=max(0.03, profile.breakeven_trigger_pct * 0.55),
            trail_trigger_pct=max(0.05, profile.trail_trigger_pct * 0.60),
            trail_gap_pct=max(0.02, profile.trail_gap_pct * 0.62),
            stale_after_sec=max(10, int(profile.stale_after_sec * 0.30)),
            min_progress_pct=max(0.03, profile.min_progress_pct * 0.60),
        )
    if leverage >= 50:
        return TradeProfile(
            stop_pct=profile.stop_pct * 0.68,
            take_pct=profile.take_pct * 0.75,
            breakeven_trigger_pct=max(0.04, profile.breakeven_trigger_pct * 0.72),
            trail_trigger_pct=max(0.06, profile.trail_trigger_pct * 0.75),
            trail_gap_pct=max(0.03, profile.trail_gap_pct * 0.72),
            stale_after_sec=max(14, int(profile.stale_after_sec * 0.45)),
            min_progress_pct=max(0.04, profile.min_progress_pct * 0.75),
        )
    if leverage >= 30:
        return TradeProfile(
            stop_pct=profile.stop_pct * 0.82,
            take_pct=profile.take_pct * 0.90,
            breakeven_trigger_pct=max(0.05, profile.breakeven_trigger_pct * 0.88),
            trail_trigger_pct=max(0.08, profile.trail_trigger_pct * 0.88),
            trail_gap_pct=max(0.04, profile.trail_gap_pct * 0.85),
            stale_after_sec=max(18, int(profile.stale_after_sec * 0.70)),
            min_progress_pct=max(0.05, profile.min_progress_pct * 0.85),
        )
    if leverage >= 20:
        hold_scale = 1.70 if candidate.setup.startswith(("impulse_", "trend_", "swing_", "major_")) else 1.20
        return TradeProfile(
            stop_pct=profile.stop_pct * 1.08,
            take_pct=profile.take_pct * 1.30,
            breakeven_trigger_pct=max(0.05, profile.breakeven_trigger_pct * 1.05),
            trail_trigger_pct=max(0.08, profile.trail_trigger_pct * 1.12),
            trail_gap_pct=max(0.04, profile.trail_gap_pct * 1.10),
            stale_after_sec=max(20, int(profile.stale_after_sec * hold_scale)),
            min_progress_pct=max(0.04, profile.min_progress_pct * 0.90),
        )
    return profile


def effective_max_hold_sec(base_max_hold_sec: int, *, candidate: Candidate, leverage: int) -> int:
    if leverage >= 150:
        return max(8, int(base_max_hold_sec * 0.18))
    if leverage >= 100:
        return max(10, int(base_max_hold_sec * 0.25))
    if leverage >= 50:
        return max(14, int(base_max_hold_sec * 0.45))
    if leverage >= 30:
        return max(18, int(base_max_hold_sec * 0.70))
    if leverage >= 20:
        hold_scale = 2.40 if candidate.setup.startswith(("impulse_", "trend_", "swing_", "major_")) else 1.30
        return max(24, int(base_max_hold_sec * hold_scale))
    return max(5, base_max_hold_sec)


def trade_profile(candidate: Candidate, *, leverage: int) -> TradeProfile:
    if candidate.setup.startswith("impulse_"):
        is_slow_symbol = candidate.symbol in SLOW_SYMBOLS
        profile = TradeProfile(
            stop_pct=0.22 if candidate.score >= 74.0 else (0.26 if is_slow_symbol else 0.24),
            take_pct=0.26 if is_slow_symbol else (0.34 if candidate.score >= 74.0 else 0.30),
            breakeven_trigger_pct=0.10,
            trail_trigger_pct=0.18,
            trail_gap_pct=0.06 if is_slow_symbol else 0.07,
            stale_after_sec=70 if candidate.score >= 74.0 else 60,
            min_progress_pct=0.10,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup.startswith("micro_"):
        profile = TradeProfile(
            stop_pct=0.12 if candidate.score >= 42.0 else 0.14,
            take_pct=0.18 if candidate.score >= 42.0 else 0.15,
            breakeven_trigger_pct=0.06,
            trail_trigger_pct=0.10,
            trail_gap_pct=0.035,
            stale_after_sec=18,
            min_progress_pct=0.03,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup == "pressure_breakdown_short":
        profile = TradeProfile(
            stop_pct=0.20 if candidate.score >= 66.0 else 0.24,
            take_pct=0.30 if candidate.score >= 66.0 else 0.26,
            breakeven_trigger_pct=0.10,
            trail_trigger_pct=0.18,
            trail_gap_pct=0.07,
            stale_after_sec=56,
            min_progress_pct=0.10,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup == "flush_reversion_long":
        profile = TradeProfile(
            stop_pct=0.26 if candidate.score >= 60.0 else 0.30,
            take_pct=0.34 if candidate.score >= 60.0 else 0.30,
            breakeven_trigger_pct=0.10,
            trail_trigger_pct=0.20,
            trail_gap_pct=0.08,
            stale_after_sec=64,
            min_progress_pct=0.08,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup.startswith("swing_"):
        profile = TradeProfile(
            stop_pct=0.44 if candidate.score >= 76.0 else 0.54,
            take_pct=1.6 if candidate.score >= 78.0 else 1.35,
            breakeven_trigger_pct=0.22,
            trail_trigger_pct=0.85,
            trail_gap_pct=0.30,
            stale_after_sec=150,
            min_progress_pct=0.18,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup == "explore_reclaim_long":
        profile = TradeProfile(
            stop_pct=0.10 if candidate.score >= 42.0 else 0.12,
            take_pct=0.08 if candidate.score >= 40.0 else 0.06,
            breakeven_trigger_pct=0.04,
            trail_trigger_pct=0.06,
            trail_gap_pct=0.025,
            stale_after_sec=16 if candidate.score >= 42.0 else 14,
            min_progress_pct=0.02,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup == "explore_reject_short":
        profile = TradeProfile(
            stop_pct=0.10 if candidate.score >= 56.0 else 0.12,
            take_pct=0.08 if candidate.score >= 54.0 else 0.06,
            breakeven_trigger_pct=0.04,
            trail_trigger_pct=0.06,
            trail_gap_pct=0.025,
            stale_after_sec=14,
            min_progress_pct=0.02,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup.startswith("major_"):
        profile = TradeProfile(
            stop_pct=0.18 if candidate.score >= 54.0 else 0.22,
            take_pct=0.20 if candidate.score >= 54.0 else 0.16,
            breakeven_trigger_pct=0.07,
            trail_trigger_pct=0.12,
            trail_gap_pct=0.04,
            stale_after_sec=44,
            min_progress_pct=0.06,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup.startswith("range_reversion"):
        profile = TradeProfile(
            stop_pct=0.24 if candidate.score >= 54.0 else 0.28,
            take_pct=0.42 if candidate.score >= 56.0 else 0.36,
            breakeven_trigger_pct=0.12,
            trail_trigger_pct=0.28,
            trail_gap_pct=0.16,
            stale_after_sec=18,
            min_progress_pct=0.08,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup.startswith("volatility_"):
        profile = TradeProfile(
            stop_pct=0.42 if candidate.score >= 56.0 else 0.52,
            take_pct=0.24 if candidate.score >= 56.0 else 0.18,
            breakeven_trigger_pct=0.12,
            trail_trigger_pct=0.18,
            trail_gap_pct=0.06,
            stale_after_sec=45,
            min_progress_pct=0.14,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup in {"trend_pullback_long", "failed_bounce_short"}:
        is_slow_symbol = candidate.symbol in SLOW_SYMBOLS
        profile = TradeProfile(
            stop_pct=0.24 if candidate.score >= 68.0 else 0.30,
            take_pct=0.22 if is_slow_symbol else (0.30 if candidate.score >= 68.0 else 0.26),
            breakeven_trigger_pct=0.10,
            trail_trigger_pct=0.18 if is_slow_symbol else 0.24,
            trail_gap_pct=0.05 if is_slow_symbol else 0.07,
            stale_after_sec=50 if is_slow_symbol else (62 if candidate.score >= 68.0 else 56),
            min_progress_pct=0.08 if is_slow_symbol else 0.10,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    if candidate.setup in {"trend_breakout_long", "breakdown_continuation_short"}:
        profile = TradeProfile(
            stop_pct=0.20 if candidate.score >= 62.0 else 0.24,
            take_pct=0.18 if candidate.score >= 62.0 else 0.22,
            breakeven_trigger_pct=0.10,
            trail_trigger_pct=0.14,
            trail_gap_pct=0.04,
            stale_after_sec=58,
            min_progress_pct=0.10,
        )
        return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)
    profile = TradeProfile(
        stop_pct=0.34 if candidate.score >= 50.0 else 0.42,
        take_pct=1.0 if candidate.score >= 52.0 else 0.88,
        breakeven_trigger_pct=0.15,
        trail_trigger_pct=0.6,
        trail_gap_pct=0.22,
        stale_after_sec=40,
        min_progress_pct=0.16,
    )
    return adjust_trade_profile_for_leverage(profile, candidate=candidate, leverage=leverage)


def extract_position_amt(payload: object, symbol: str) -> float:
    if not isinstance(payload, list):
        return 0.0
    for item in payload:
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol") or "").upper() != symbol.upper():
            continue
        return safe_float(item.get("positionAmt"))
    return 0.0


def extract_position_entry_price(payload: object, symbol: str) -> float:
    if not isinstance(payload, list):
        return 0.0
    for item in payload:
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol") or "").upper() != symbol.upper():
            continue
        return safe_float(item.get("entryPrice"))
    return 0.0


def _empty_fill_summary(order_ids: list[int] | None = None) -> dict[str, Any]:
    return {
        "avg_price": 0.0,
        "qty": 0.0,
        "quote_qty": 0.0,
        "commission": 0.0,
        "realized_pnl": 0.0,
        "order_ids": list(order_ids or []),
        "trade_count": 0,
        "maker_fill_count": 0,
        "taker_fill_count": 0,
        "maker_or_taker": "unknown",
        "first_time_ms": None,
        "last_time_ms": None,
        "commission_asset": None,
    }


def _fill_role(item: dict[str, Any]) -> str | None:
    raw = item.get("maker")
    if raw is None:
        raw = item.get("isMaker")
    if isinstance(raw, bool):
        return "maker" if raw else "taker"
    text = str(raw or "").strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return "maker"
    if text in {"false", "0", "no", "n"}:
        return "taker"
    return None


def _normalize_order_ids(order_ids: list[int]) -> list[int]:
    normalized: list[int] = []
    for item in order_ids:
        parsed = safe_int(item)
        if parsed is not None:
            normalized.append(parsed)
    return sorted(set(normalized))


def _summarize_matched_fills(matched: list[dict[str, Any]], order_ids: list[int]) -> dict[str, Any]:
    if not matched:
        return _empty_fill_summary(order_ids)
    qty = sum(safe_float(item.get("qty")) for item in matched)
    quote_qty = sum(safe_float(item.get("quoteQty")) for item in matched)
    commission = sum(safe_float(item.get("commission")) for item in matched)
    realized_pnl = sum(safe_float(item.get("realizedPnl")) for item in matched)
    avg_price = (quote_qty / qty) if qty > 0 else 0.0
    roles = [_fill_role(item) for item in matched]
    maker_count = len([role for role in roles if role == "maker"])
    taker_count = len([role for role in roles if role == "taker"])
    maker_or_taker = combine_liquidity_roles(*(role for role in roles if role is not None))
    times = [item for item in (safe_int(fill.get("time")) for fill in matched) if item is not None]
    commission_assets = sorted(
        {
            str(item.get("commissionAsset") or "").strip().upper()
            for item in matched
            if str(item.get("commissionAsset") or "").strip()
        }
    )
    return {
        "avg_price": avg_price,
        "qty": qty,
        "quote_qty": quote_qty,
        "commission": commission,
        "realized_pnl": realized_pnl,
        "order_ids": order_ids,
        "trade_count": len(matched),
        "maker_fill_count": maker_count,
        "taker_fill_count": taker_count,
        "maker_or_taker": maker_or_taker,
        "first_time_ms": min(times) if times else None,
        "last_time_ms": max(times) if times else None,
        "commission_asset": commission_assets[0] if len(commission_assets) == 1 else ("mixed" if commission_assets else None),
    }


def summarize_order_fills(trades: list[dict[str, Any]], order_id: int) -> dict[str, Any]:
    normalized = _normalize_order_ids([order_id])
    matched = [item for item in trades if safe_int(item.get("orderId")) in normalized]
    return _summarize_matched_fills(matched, normalized)


def summarize_order_fills_many(trades: list[dict[str, Any]], order_ids: list[int]) -> dict[str, Any]:
    normalized = _normalize_order_ids(order_ids)
    matched = [item for item in trades if safe_int(item.get("orderId")) in normalized]
    return _summarize_matched_fills(matched, normalized)


async def pick_candidate(
    futures: BinanceFuturesClient,
    *,
    universe_top: int,
    scan_top: int,
    min_quote_volume: float,
    min_score: float,
    last_symbol: str | None,
    blocked_symbols: dict[str, float] | None,
    excluded_symbols: set[str] | None,
    market_cache: dict[str, Any] | None,
    allow_short: bool,
    execution_floor_offset: float = 0.0,
) -> Candidate | None:
    now = time.time()
    cached_universe = market_cache.get("universe") if market_cache else None
    cached_expiry = safe_float(market_cache.get("expires_at")) if market_cache else 0.0
    if cached_universe and now < cached_expiry:
        universe = cached_universe
    else:
        universe = discover_universe(await futures.ticker_24hr(), top_limit=universe_top, min_quote_volume=min_quote_volume)
        if market_cache is not None:
            market_cache["universe"] = universe
            market_cache["expires_at"] = now + 20.0
    shortlist = universe[:scan_top]
    semaphore = asyncio.Semaphore(3)

    async def one(item: dict[str, Any]) -> Candidate | None:
        try:
            async with semaphore:
                candles_1m, candles_5m, candles_15m = await asyncio.gather(
                    futures.klines(item["symbol"], interval="1m", limit=90),
                    futures.klines(item["symbol"], interval="5m", limit=90),
                    futures.klines(item["symbol"], interval="15m", limit=90),
                )
        except Exception:
            return None
        candidate = score_candidate(
            item,
            candles_1m,
            candles_5m,
            candles_15m,
            allow_short=allow_short,
        )
        if candidate is None:
            return None
        execution_floor = max(min_score, max(24.0, setup_execution_floor(candidate.setup) + execution_floor_offset))
        if candidate.score < execution_floor:
            return None
        if blocked_symbols:
            cooldown_expires_at = safe_float(blocked_symbols.get(candidate.symbol))
            if cooldown_expires_at > now:
                return None
        if excluded_symbols and candidate.symbol in excluded_symbols:
            return None
        if last_symbol and candidate.symbol == last_symbol:
            candidate.score -= 4.0
            if candidate.score < min_score:
                return None
        return candidate

    candidates = [item for item in await asyncio.gather(*(one(item) for item in shortlist)) if item is not None]
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            item.score,
            setup_priority(item.setup),
            item.avg_quote_turnover_1m,
            item.quote_volume_24h,
        ),
        reverse=True,
    )
    return candidates[0]


def apply_symbol_cooldown(cooldowns: dict[str, float], symbol: str, *, seconds: float) -> None:
    if not symbol or seconds <= 0:
        return
    cooldowns[symbol] = max(cooldowns.get(symbol, 0.0), time.time() + seconds)


async def submit_runner_order(
    futures: BinanceFuturesClient,
    payload: dict[str, Any],
    *,
    source: str,
    purpose: str,
    candidate: Candidate | None = None,
    intent: Any | None = None,
    args: argparse.Namespace | None = None,
) -> dict[str, Any]:
    symbol = str(payload.get("symbol") or getattr(intent, "symbol", "") or (candidate.symbol if candidate else "") or "UNKNOWN").upper()
    side = str(payload.get("side") or getattr(intent, "side", "") or (candidate.side if candidate else "") or "").upper()
    reduce_like = str(payload.get("reduceOnly") or "").lower() == "true" or str(payload.get("closePosition") or "").lower() == "true"
    positions: list[dict[str, Any]] = []
    if reduce_like or purpose in {"exit", "emergency", "take_profit", "protection"}:
        positions.append({"symbol": symbol, "side": "LONG" if side == "SELL" else "SHORT", "protection_status": "healthy"})
    candidate_payload = asdict(candidate) if candidate is not None else None
    if candidate_payload is not None:
        candidate_payload.setdefault("setup_type", "QUICK_TRADE")
        candidate_payload.setdefault("spread_bps", 0.0)
        candidate_payload.setdefault("estimated_slippage_bps", 0.0)
        candidate_payload.setdefault("liquidity_ok", True)
    snapshot = build_gateway_snapshot(
        symbol=symbol,
        side=side,
        candidate=candidate_payload,
        positions=positions,
        data_fresh=True,
        websocket_status="healthy",
        exchange_status="healthy",
        position_state="known",
        stop_protection_status="healthy",
        protective_stop_path_available=True,
        emergency_close_available=True,
        max_open_positions=int(getattr(args, "max_open_positions", 10) or 10),
    )
    return await submit_binance_order_intent(
        futures,
        payload,
        snapshot=snapshot,
        environment={
            "runtime_mode": getattr(args, "runtime_mode", TESTNET_LIVE) if args is not None else TESTNET_LIVE,
            "env": getattr(args, "env", None) if args is not None else (getattr(getattr(futures, "environment", None), "name", None) or "testnet"),
        },
        source=source,
        purpose=purpose,
        endpoint=ORDER_SUBMIT_PATH,
        order_intent=_intent_to_gateway_payload(intent),
        dry_run=False,
        extra_context={
            "protective_stop_path_available": True,
            "emergency_close_path_available": True,
        },
    )


def _intent_to_gateway_payload(intent: Any | None) -> dict[str, Any] | None:
    if intent is None:
        return None
    if hasattr(intent, "to_dict"):
        return intent.to_dict()
    payload: dict[str, Any] = {}
    for key in (
        "symbol",
        "side",
        "entry_price",
        "initial_stop_price",
        "take_profit_price",
        "margin_type",
    ):
        if hasattr(intent, key):
            payload[key] = getattr(intent, key)
    return payload or None


async def try_passive_reduce_only_close(
    futures: BinanceFuturesClient,
    *,
    symbol: str,
    close_side: str,
    quantity: float,
    tick_size: float,
    wait_sec: float = 3.0,
) -> dict[str, float | int] | None:
    if quantity <= 0:
        return None
    try:
        book = await futures.book_ticker(symbol)
    except Exception:
        return None

    bid_price = safe_float(book.get("bidPrice"))
    ask_price = safe_float(book.get("askPrice"))
    if close_side == "SELL":
        reference_price = ask_price or (bid_price + tick_size)
        passive_price = round_up_to_step(reference_price, tick_size)
    else:
        reference_price = bid_price or max(0.0, ask_price - tick_size)
        passive_price = round_down_to_step(reference_price, tick_size)
    if passive_price <= 0:
        return None

    close_order = await submit_runner_order(
        futures,
        {
            "symbol": symbol,
            "side": close_side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": quantity,
            "price": passive_price,
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        },
        source="phoenix_testnet_round_runner:passive_reduce_only_close",
        purpose="exit",
    )
    order_id = int(close_order.get("orderId") or 0)
    if order_id <= 0:
        return None

    await asyncio.sleep(max(1.0, wait_sec))
    position_payload = await futures.position_information_v3(symbol)
    remaining_position_amt = extract_position_amt(position_payload, symbol)
    if abs(remaining_position_amt) <= 1e-12:
        return {
            "order_id": order_id,
            "avg_price": passive_price,
            "remaining_position_amt": 0.0,
        }

    try:
        await futures.cancel_order(symbol, order_id=order_id)
    except BinanceAPIError:
        pass
    return {
        "order_id": order_id,
        "avg_price": passive_price,
        "remaining_position_amt": remaining_position_amt,
    }


async def manage_open_trade(
    futures: BinanceFuturesClient,
    *,
    candidate: Candidate,
    leverage: int,
    entry_price: float,
    quantity: float,
    tick_size: float,
    max_hold_sec: int,
    poll_sec: float,
    estimated_entry_slippage_bps: float = 0.0,
    round_trip_fee_bps: float = 8.0,
    tp_slippage_buffer_bps: float = 6.0,
    tp_safety_buffer_bps: float = 5.0,
) -> dict[str, Any]:
    profile = trade_profile(candidate, leverage=leverage)
    fee_aware_take = compute_fee_aware_take_profit_pct(
        raw_take_pct=profile.take_pct,
        estimated_entry_slippage_bps=estimated_entry_slippage_bps,
        round_trip_fee_bps=round_trip_fee_bps,
        slippage_buffer_bps=tp_slippage_buffer_bps,
        safety_buffer_bps=tp_safety_buffer_bps,
    )
    effective_take_pct = float(fee_aware_take["effective_take_pct"])
    if candidate.side == "BUY":
        stop_price = entry_price * (1.0 - profile.stop_pct / 100.0)
        take_price = entry_price * (1.0 + effective_take_pct / 100.0)
    else:
        stop_price = entry_price * (1.0 + profile.stop_pct / 100.0)
        take_price = entry_price * (1.0 - effective_take_pct / 100.0)

    started = time.time()
    best_price = entry_price
    worst_price = entry_price
    exit_reason = "time_stop"
    last_price = entry_price
    started_ms = int(started * 1000)
    price_points: list[dict[str, Any]] = [{"timestamp_ms": started_ms, "mark_price": entry_price}]
    exit_signal_timestamp_ms: int | None = None
    close_order_submit_timestamp_ms: int | None = None
    close_order_response_timestamp_ms: int | None = None
    stop_hit_count = 0
    breakeven_guard_active = False
    trailing_stop_active = False
    take_profit_order_id = 0
    take_profit_order_price = 0.0
    should_use_resting_take_profit = (
        uses_resting_take_profit(candidate.setup)
        and candidate.mark_price >= 0.10
        and candidate.avg_quote_turnover_1m >= 12_000_000.0
    )

    if should_use_resting_take_profit:
        take_profit_order_price = round_down_to_step(take_price, tick_size)
        if take_profit_order_price > 0 and abs(take_profit_order_price - entry_price) >= max(tick_size, entry_price * 0.0001):
            take_profit_order = await submit_runner_order(
                futures,
                {
                    "symbol": candidate.symbol,
                    "side": "SELL" if candidate.side == "BUY" else "BUY",
                    "type": "LIMIT",
                    "timeInForce": "GTC",
                    "quantity": quantity,
                    "price": take_profit_order_price,
                    "reduceOnly": "true",
                    "newOrderRespType": "RESULT",
                },
                source="phoenix_testnet_round_runner:resting_take_profit",
                purpose="take_profit",
                candidate=candidate,
            )
            take_profit_order_id = int(take_profit_order.get("orderId") or 0)

    while time.time() - started < max_hold_sec:
        await asyncio.sleep(poll_sec)
        quote = await futures.mark_price(candidate.symbol)
        mark_price = safe_float(quote.get("markPrice"))
        if mark_price <= 0:
            continue
        price_points.append({"timestamp_ms": int(time.time() * 1000), "mark_price": mark_price})
        if take_profit_order_id:
            position_payload = await futures.position_information_v3(candidate.symbol)
            position_amt = extract_position_amt(position_payload, candidate.symbol)
            if abs(position_amt) <= 1e-12:
                exit_reason = "take_profit_limit"
                break
        last_price = mark_price
        best_price = max(best_price, mark_price)
        worst_price = min(worst_price, mark_price)
        if candidate.side == "BUY":
            favorable_move_pct = pct_change(best_price, entry_price)
            if favorable_move_pct >= profile.breakeven_trigger_pct:
                stop_price = max(stop_price, entry_price * 1.0009)
                breakeven_guard_active = True
            if favorable_move_pct >= profile.trail_trigger_pct:
                stop_price = max(stop_price, best_price * (1.0 - profile.trail_gap_pct / 100.0))
                trailing_stop_active = True
            if (
                time.time() - started >= profile.stale_after_sec
                and favorable_move_pct < profile.min_progress_pct
                and mark_price <= entry_price * 1.0004
            ):
                exit_reason = "no_follow_through_exit"
                break
        else:
            favorable_move_pct = pct_change(entry_price, worst_price)
            if favorable_move_pct >= profile.breakeven_trigger_pct:
                stop_price = min(stop_price, entry_price * 0.9991)
                breakeven_guard_active = True
            if favorable_move_pct >= profile.trail_trigger_pct:
                stop_price = min(stop_price, worst_price * (1.0 + profile.trail_gap_pct / 100.0))
                trailing_stop_active = True
            if (
                time.time() - started >= profile.stale_after_sec
                and favorable_move_pct < profile.min_progress_pct
                and mark_price >= entry_price * 0.9996
            ):
                exit_reason = "no_follow_through_exit"
                break

        if candidate.side == "BUY":
            if mark_price <= stop_price:
                stop_hit_count += 1
                if stop_hit_count >= 2:
                    exit_reason = "trailing_profit_exit" if trailing_stop_active else ("dynamic_breakeven_exit" if breakeven_guard_active else "stop_loss")
                    break
            else:
                stop_hit_count = 0
            if not take_profit_order_id and mark_price >= take_price:
                exit_reason = "take_profit"
                break
        else:
            if mark_price >= stop_price:
                stop_hit_count += 1
                if stop_hit_count >= 2:
                    exit_reason = "trailing_profit_exit" if trailing_stop_active else ("dynamic_breakeven_exit" if breakeven_guard_active else "stop_loss")
                    break
            else:
                stop_hit_count = 0
            if not take_profit_order_id and mark_price <= take_price:
                exit_reason = "take_profit"
                break

    close_order_id = take_profit_order_id
    close_order_ids: list[int] = [take_profit_order_id] if take_profit_order_id > 0 else []
    close_avg_price = take_profit_order_price if exit_reason == "take_profit_limit" else 0.0
    if exit_signal_timestamp_ms is None:
        exit_signal_timestamp_ms = int(time.time() * 1000)
    if exit_reason != "take_profit_limit":
        if take_profit_order_id:
            try:
                await futures.cancel_order(candidate.symbol, order_id=take_profit_order_id)
            except BinanceAPIError:
                pass
        position_payload = await futures.position_information_v3(candidate.symbol)
        position_amt = extract_position_amt(position_payload, candidate.symbol)
        if abs(position_amt) <= 1e-12:
            exit_reason = "take_profit_limit"
            close_order_id = take_profit_order_id
            close_avg_price = take_profit_order_price
        else:
            close_side = "SELL" if position_amt > 0 else "BUY"
            if (
                exit_reason in {"stale_exit", "time_stop", "no_follow_through_exit"}
                and candidate.mark_price >= 0.10
                and candidate.avg_quote_turnover_1m >= 12_000_000.0
            ):
                close_order_submit_timestamp_ms = int(time.time() * 1000)
                passive_close = await try_passive_reduce_only_close(
                    futures,
                    symbol=candidate.symbol,
                    close_side=close_side,
                    quantity=abs(position_amt),
                    tick_size=tick_size,
                    wait_sec=3.0,
                )
                close_order_response_timestamp_ms = int(time.time() * 1000)
                if passive_close is not None:
                    passive_order_id = int(passive_close["order_id"] or 0)
                    if passive_order_id > 0:
                        close_order_ids.append(passive_order_id)
                        close_order_id = passive_order_id
                    close_avg_price = safe_float(passive_close["avg_price"])
                    position_amt = safe_float(passive_close["remaining_position_amt"])
                    if abs(position_amt) <= 1e-12:
                        return {
                            "exit_reason": exit_reason,
                            "close_order_id": close_order_id,
                            "close_order_ids": close_order_ids,
                            "close_avg_price": close_avg_price,
                            "last_mark_price": last_price,
                            "best_mark_price": best_price,
                            "worst_mark_price": worst_price,
                            "hold_seconds": round4(time.time() - started),
                            "price_points": price_points,
                            "exit_signal_timestamp": exit_signal_timestamp_ms,
                            "close_order_submit_timestamp": close_order_submit_timestamp_ms,
                            "close_order_response_timestamp": close_order_response_timestamp_ms,
                            "intended_take_profit_price": take_price,
                            "raw_take_profit_pct": profile.take_pct,
                            "effective_take_profit_pct": effective_take_pct,
                            "fee_aware_take_profit": fee_aware_take,
                        }
            close_order_submit_timestamp_ms = int(time.time() * 1000)
            close_order = await submit_runner_order(
                futures,
                {
                    "symbol": candidate.symbol,
                    "side": close_side,
                    "type": "MARKET",
                    "quantity": abs(position_amt),
                    "reduceOnly": "true",
                    "newOrderRespType": "RESULT",
                },
                source="phoenix_testnet_round_runner:market_reduce_only_close",
                purpose="exit",
                candidate=candidate,
            )
            close_order_response_timestamp_ms = int(time.time() * 1000)
            close_order_id = int(close_order.get("orderId") or 0)
            if close_order_id > 0:
                close_order_ids.append(close_order_id)
            close_avg_price = safe_float(close_order.get("avgPrice"))
    return {
        "exit_reason": exit_reason,
        "close_order_id": close_order_id,
        "close_order_ids": close_order_ids,
        "close_avg_price": close_avg_price,
        "last_mark_price": last_price,
        "best_mark_price": best_price,
        "worst_mark_price": worst_price,
        "hold_seconds": round4(time.time() - started),
        "price_points": price_points,
        "exit_signal_timestamp": exit_signal_timestamp_ms,
        "close_order_submit_timestamp": close_order_submit_timestamp_ms,
        "close_order_response_timestamp": close_order_response_timestamp_ms,
        "intended_take_profit_price": take_price,
        "raw_take_profit_pct": profile.take_pct,
        "effective_take_profit_pct": effective_take_pct,
        "fee_aware_take_profit": fee_aware_take,
    }


def profit_factor(values: list[float]) -> float:
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if losses <= 0:
        return 999.0 if gains > 0 else 0.0
    return gains / losses


def build_round_review(round_no: int, trades: list[dict[str, Any]], start_balance: float, end_balance: float) -> dict[str, Any]:
    net_pnls = [safe_float(item.get("net_pnl_usdt")) for item in trades]
    commissions = [safe_float(item.get("commission_usdt")) for item in trades]
    leverages = [safe_float(item.get("leverage")) for item in trades]
    hold_seconds = [safe_float(item.get("hold_seconds")) for item in trades]
    exit_counts = Counter(str(item.get("exit_reason") or "") for item in trades)
    side_stats: dict[str, list[float]] = defaultdict(list)
    symbol_stats: dict[str, list[float]] = defaultdict(list)
    for item in trades:
        side_stats[str(item.get("side") or "")].append(safe_float(item.get("net_pnl_usdt")))
        symbol_stats[str(item.get("symbol") or "")].append(safe_float(item.get("net_pnl_usdt")))

    top_symbols = sorted(
        (
            {
                "symbol": symbol,
                "trade_count": len(values),
                "avg_net_pnl_usdt": round4(mean(values)),
                "median_net_pnl_usdt": round4(median(values)),
            }
            for symbol, values in symbol_stats.items()
        ),
        key=lambda item: (item["avg_net_pnl_usdt"], item["trade_count"]),
        reverse=True,
    )[:10]

    side_breakdown = {
        side: {
            "trade_count": len(values),
            "avg_net_pnl_usdt": round4(mean(values)),
            "median_net_pnl_usdt": round4(median(values)),
            "win_rate_pct": round4((len([value for value in values if value > 0]) / len(values)) * 100.0 if values else 0.0),
        }
        for side, values in side_stats.items()
    }

    gross_realized = round4(sum(safe_float(item.get("realized_pnl_usdt")) for item in trades))
    total_commission = round4(sum(commissions))
    net_pnl = round4(sum(net_pnls))
    balance_delta = round4(end_balance - start_balance)
    fee_drag_ratio = round4((total_commission / abs(gross_realized)) if gross_realized != 0 else 0.0)
    overheated = [item for item in trades if abs(safe_float(item.get("price_change_24h_pct"))) > 10.0]
    low_price = [item for item in trades if safe_float(item.get("entry_price")) < 0.1]
    high_leverage = [item for item in trades if safe_float(item.get("leverage")) >= 4.0]

    findings: list[str] = []
    if not trades:
        findings.append("No trade passed the current edge filter during this round. Capital was preserved instead of forcing low-quality entries.")
    buy_count = side_breakdown.get("BUY", {}).get("trade_count", 0)
    sell_count = side_breakdown.get("SELL", {}).get("trade_count", 0)
    if buy_count > 0 and sell_count > 0 and side_breakdown.get("BUY", {}).get("avg_net_pnl_usdt", 0.0) > side_breakdown.get("SELL", {}).get("avg_net_pnl_usdt", 0.0) + 0.01:
        findings.append("Long side is outperforming short side; keep a long bias until short expectancy improves.")
    if buy_count > 0 and sell_count > 0 and side_breakdown.get("SELL", {}).get("avg_net_pnl_usdt", 0.0) > side_breakdown.get("BUY", {}).get("avg_net_pnl_usdt", 0.0) + 0.01:
        findings.append("Short side is outperforming long side; the market currently rewards faster downside follow-through.")
    if exit_counts.get("time_stop", 0) >= max(1, len(trades) // 2):
        findings.append("Too many trades are ending by time stop; current targets may be too ambitious for this turnover speed.")
    if exit_counts.get("stale_exit", 0) >= max(1, len(trades) // 3):
        findings.append("Many trades failed to show early follow-through; keep favoring pullback entries over straight breakout chasing.")
    if fee_drag_ratio >= 0.45:
        findings.append("Fee drag is heavy relative to realized gross PnL; increase selectivity or widen profit targets.")
    if overheated and mean([safe_float(item.get("net_pnl_usdt")) for item in overheated]) < mean(net_pnls) - 0.1:
        findings.append("Overheated 24h movers are underperforming the round baseline; keep fading late-stage heat.")
    if low_price and mean([safe_float(item.get("net_pnl_usdt")) for item in low_price]) < mean(net_pnls) - 0.1:
        findings.append("Sub-10-cent symbols are dragging expectancy; the strategy should stay biased toward cleaner, higher-priced contracts.")
    if high_leverage and mean([safe_float(item.get("net_pnl_usdt")) for item in high_leverage]) < mean(net_pnls) - 0.1:
        findings.append("High leverage is amplifying fee drag and weak entries; keep leverage capped until gross edge turns positive.")
    if not findings:
        findings.append("Current setup is tradable on testnet, but it still needs more samples before promoting any parameter change as durable.")

    return {
        "round": round_no,
        "trade_count": len(trades),
        "start_balance_usdt": round4(start_balance),
        "end_balance_usdt": round4(end_balance),
        "balance_delta_usdt": balance_delta,
        "gross_realized_pnl_usdt": gross_realized,
        "commission_usdt": total_commission,
        "net_pnl_usdt": net_pnl,
        "win_rate_pct": round4((len([value for value in net_pnls if value > 0]) / len(net_pnls)) * 100.0 if net_pnls else 0.0),
        "avg_net_pnl_usdt": round4(mean(net_pnls)),
        "median_net_pnl_usdt": round4(median(net_pnls)),
        "profit_factor": round4(profit_factor(net_pnls)),
        "avg_leverage": round4(mean(leverages)),
        "avg_hold_seconds": round4(mean(hold_seconds)),
        "exit_breakdown": dict(exit_counts),
        "side_breakdown": side_breakdown,
        "top_symbols": top_symbols,
        "review": findings,
    }


async def flatten_active_positions(futures: BinanceFuturesClient) -> list[dict[str, Any]]:
    positions = await futures.position_information_v3()
    flattened: list[dict[str, Any]] = []
    for item in positions:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        position_amt = safe_float(item.get("positionAmt"))
        if not symbol or abs(position_amt) <= 1e-12:
            continue
        close_side = "SELL" if position_amt > 0 else "BUY"
        close_order = await submit_runner_order(
            futures,
            {
                "symbol": symbol,
                "side": close_side,
                "type": "MARKET",
                "quantity": abs(position_amt),
                "reduceOnly": "true",
                "newOrderRespType": "RESULT",
            },
            source="phoenix_testnet_round_runner:flatten_active_positions",
            purpose="emergency",
        )
        flattened.append(
            {
                "symbol": symbol,
                "closed_position_amt": round4(position_amt),
                "close_side": close_side,
                "close_order_id": int(close_order.get("orderId") or 0),
                "close_avg_price": round4(safe_float(close_order.get("avgPrice"))),
            }
        )
    return flattened


async def cancel_orphan_orders(futures: BinanceFuturesClient) -> list[dict[str, Any]]:
    positions = await futures.position_information_v3()
    live_symbols = {
        str(item.get("symbol") or "").upper()
        for item in positions
        if isinstance(item, dict) and abs(safe_float(item.get("positionAmt"))) > 1e-12
    }
    cancelled: list[dict[str, Any]] = []

    open_orders = await futures.open_orders()
    orphan_order_symbols = sorted(
        {
            str(item.get("symbol") or "").upper()
            for item in open_orders
            if isinstance(item, dict)
            and str(item.get("symbol") or "").upper()
            and str(item.get("symbol") or "").upper() not in live_symbols
        }
    )
    for symbol in orphan_order_symbols:
        try:
            await futures.cancel_all_open_orders(symbol)
            cancelled.append({"symbol": symbol, "kind": "open_orders"})
        except BinanceAPIError as exc:
            if exc.code != -2011:
                raise

    open_conditional_orders = await futures.open_conditional_orders()
    orphan_conditional_symbols = sorted(
        {
            str(item.get("symbol") or "").upper()
            for item in open_conditional_orders
            if isinstance(item, dict)
            and str(item.get("symbol") or "").upper()
            and str(item.get("symbol") or "").upper() not in live_symbols
        }
    )
    for symbol in orphan_conditional_symbols:
        try:
            await futures.cancel_all_open_conditional_orders(symbol)
            cancelled.append({"symbol": symbol, "kind": "conditional_orders"})
        except BinanceAPIError as exc:
            if exc.code != -2011:
                raise

    return cancelled


async def flatten_symbol_position(futures: BinanceFuturesClient, symbol: str) -> dict[str, Any] | None:
    positions = await futures.position_information_v3(symbol)
    position_amt = extract_position_amt(positions, symbol)
    if abs(position_amt) <= 1e-12:
        return None
    close_side = "SELL" if position_amt > 0 else "BUY"
    close_order = await submit_runner_order(
        futures,
        {
            "symbol": symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": abs(position_amt),
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        },
        source="phoenix_testnet_round_runner:flatten_symbol_position",
        purpose="emergency",
    )
    return {
        "symbol": symbol,
        "closed_position_amt": round4(position_amt),
        "close_side": close_side,
        "close_order_id": int(close_order.get("orderId") or 0),
        "close_avg_price": round4(safe_float(close_order.get("avgPrice"))),
    }


async def run_trade(
    futures: BinanceFuturesClient,
    executor: PhoenixExecutor,
    *,
    candidate: Candidate,
    available_balance: float,
    args: argparse.Namespace,
    open_trade_state: dict[str, Any],
    experiment: dict[str, Any] | None = None,
    learning_gate_decision: dict[str, Any] | None = None,
    strategy_gate_decision: dict[str, Any] | None = None,
    round_config_snapshot: dict[str, Any] | None = None,
    entry_signal_timestamp: int | None = None,
) -> dict[str, Any]:
    assert_live_order_submission_allowed(args)
    leverage = select_leverage(
        candidate,
        max_long_leverage=max(1, args.max_long_leverage),
        max_short_leverage=max(1, args.max_short_leverage),
    )
    if candidate.side == "BUY" and int(args.force_long_leverage or 0) > 0:
        leverage = max(1, min(int(args.force_long_leverage), int(args.max_long_leverage)))
    if candidate.side == "SELL" and int(args.force_short_leverage or 0) > 0:
        leverage = max(1, min(int(args.force_short_leverage), int(args.max_short_leverage)))
    rules = await executor.get_symbol_rules(candidate.symbol)
    quote_allocation = select_quote_allocation(candidate, base_quote=max(1.0, args.base_quote), leverage=leverage)
    min_quote_for_exchange = round((rules.min_notional / max(1, leverage)) * 1.05, 2)
    quote_allocation = max(quote_allocation, min_quote_for_exchange)
    intent = await executor.build_trade_intent(
        candidate.symbol,
        side=candidate.side,
        quote_allocation_usdt=quote_allocation,
        leverage=leverage,
        available_balance_usdt=available_balance,
    )
    profile = trade_profile(candidate, leverage=intent.leverage)
    tick_size_pct = pct_change(intent.entry_price, intent.entry_price + rules.tick_size)
    if intent.leverage >= 50 and tick_size_pct >= max(0.10, profile.take_pct * 0.60):
        raise ValueError(
            f"Tick size too coarse for {candidate.symbol} at {intent.leverage}x: "
            f"tick_pct={round4(tick_size_pct)} target_pct={round4(profile.take_pct)}"
        )
    margin_type = str(getattr(args, "margin_type", "ISOLATED") or "ISOLATED").upper()
    try:
        await futures.change_margin_type(candidate.symbol, margin_type)
    except BinanceAPIError as exc:
        if exc.code != -4046:
            raise
    try:
        await futures.change_initial_leverage(candidate.symbol, intent.leverage)
    except BinanceAPIError as exc:
        if exc.code == -4028:
            raise ValueError(f"Leverage {intent.leverage} is not valid for {candidate.symbol}") from exc
        raise

    entry_order: dict[str, Any] | None = None
    entry_order_ids: list[int] = []
    entry_price = intent.entry_price
    entry_quantity = intent.quantity
    entry_book_ticker: dict[str, Any] = {}
    entry_submit_timestamp_ms = int(time.time() * 1000)
    entry_order_response_timestamp_ms: int | None = None
    entry_submit_started = time.perf_counter()
    if prefers_maker_entry(candidate.setup):
        try:
            book_ticker = await futures.book_ticker(intent.symbol)
            entry_book_ticker = dict(book_ticker or {})
        except Exception:
            book_ticker = {}
        bid_price = safe_float(book_ticker.get("bidPrice"))
        ask_price = safe_float(book_ticker.get("askPrice"))
        maker_entry_price = 0.0
        if intent.side == "BUY":
            maker_entry_price = round_down_to_step(bid_price or intent.entry_price, rules.tick_size)
        else:
            maker_entry_price = round_up_to_step(ask_price or intent.entry_price, rules.tick_size)
        if maker_entry_price > 0:
            maker_filled = False
            entry_order = await submit_runner_order(
                futures,
                {
                    "symbol": intent.symbol,
                    "side": intent.side,
                    "type": "LIMIT",
                    "timeInForce": "GTC",
                    "quantity": intent.quantity,
                    "price": maker_entry_price,
                    "newOrderRespType": "RESULT",
                },
                source="phoenix_testnet_round_runner:maker_entry",
                purpose="entry",
                candidate=candidate,
                intent=intent,
                args=args,
            )
            entry_order_id = int(entry_order.get("orderId") or 0)
            if entry_order_id > 0:
                entry_order_ids.append(entry_order_id)
            for _ in range(8):
                await asyncio.sleep(1.0)
                position_payload = await futures.position_information_v3(intent.symbol)
                position_amt = extract_position_amt(position_payload, intent.symbol)
                if abs(position_amt) > 1e-12:
                    entry_quantity = abs(position_amt)
                    entry_price = extract_position_entry_price(position_payload, intent.symbol) or maker_entry_price
                    try:
                        await futures.cancel_order(intent.symbol, order_id=entry_order_id)
                    except BinanceAPIError:
                        pass
                    if entry_quantity < intent.quantity * 0.6:
                        remaining_quantity = round_down_to_step(intent.quantity - entry_quantity, rules.step_size)
                        if remaining_quantity >= rules.min_qty:
                            top_up_order = await submit_runner_order(
                                futures,
                                {
                                    "symbol": intent.symbol,
                                    "side": intent.side,
                                    "type": "MARKET",
                                    "quantity": remaining_quantity,
                                    "newOrderRespType": "RESULT",
                                },
                                source="phoenix_testnet_round_runner:entry_top_up",
                                purpose="entry",
                                candidate=candidate,
                                intent=intent,
                                args=args,
                            )
                            top_up_order_id = int(top_up_order.get("orderId") or 0)
                            if top_up_order_id > 0:
                                entry_order_ids.append(top_up_order_id)
                            await asyncio.sleep(0.5)
                            refreshed_position_payload = await futures.position_information_v3(intent.symbol)
                            refreshed_position_amt = extract_position_amt(refreshed_position_payload, intent.symbol)
                            if abs(refreshed_position_amt) > 1e-12:
                                entry_quantity = abs(refreshed_position_amt)
                                entry_price = extract_position_entry_price(refreshed_position_payload, intent.symbol) or entry_price
                    maker_filled = True
                    break
            if entry_order_id > 0 and not maker_filled:
                try:
                    await futures.cancel_order(intent.symbol, order_id=entry_order_id)
                except BinanceAPIError:
                    pass
                entry_order = None
            if entry_order_id <= 0:
                entry_order = None

    if entry_order is None:
        entry_order = await submit_runner_order(
            futures,
            {
                "symbol": intent.symbol,
                "side": intent.side,
                "type": "MARKET",
                "quantity": intent.quantity,
                "newOrderRespType": "RESULT",
            },
            source="phoenix_testnet_round_runner:fallback_market_entry",
            purpose="entry",
            candidate=candidate,
            intent=intent,
            args=args,
        )
        fallback_entry_order_id = int(entry_order.get("orderId") or 0)
        if fallback_entry_order_id > 0:
            entry_order_ids.append(fallback_entry_order_id)
        entry_price = safe_float(entry_order.get("avgPrice")) or intent.entry_price
        entry_quantity = intent.quantity
    entry_order_response_timestamp_ms = int(time.time() * 1000)
    latency_ms = (time.perf_counter() - entry_submit_started) * 1000.0
    pre_management_entry_slippage_bps = 0.0
    if intent.entry_price > 0 and entry_price > 0:
        pre_management_entry_slippage_bps = abs((entry_price / intent.entry_price) - 1.0) * 10_000.0

    open_trade_state.clear()
    open_trade_state.update({"symbol": intent.symbol, "quantity": entry_quantity, "side": intent.side})
    try:
        management = await manage_open_trade(
            futures,
            candidate=candidate,
            leverage=int(intent.leverage),
            entry_price=entry_price,
            quantity=entry_quantity,
            tick_size=rules.tick_size,
            max_hold_sec=effective_max_hold_sec(max(5, args.max_hold_sec), candidate=candidate, leverage=int(intent.leverage)),
            poll_sec=max(0.5, args.poll_sec),
            estimated_entry_slippage_bps=pre_management_entry_slippage_bps,
            round_trip_fee_bps=float(getattr(args, "round_trip_fee_bps", 8.0) or 8.0),
            tp_slippage_buffer_bps=float(getattr(args, "tp_slippage_buffer_bps", 6.0) or 6.0),
            tp_safety_buffer_bps=float(getattr(args, "tp_safety_buffer_bps", 5.0) or 5.0),
        )
    except Exception:
        await flatten_symbol_position(futures, intent.symbol)
        open_trade_state.clear()
        raise
    open_trade_state.clear()
    await asyncio.sleep(1.2)
    user_trades = await futures.user_trades(intent.symbol, limit=50)
    entry_fill = summarize_order_fills_many(user_trades, entry_order_ids)
    close_order_ids = (
        [int(item) for item in management.get("close_order_ids", []) if int(item) > 0]
        or [int(management["close_order_id"])]
    )
    close_fill = summarize_order_fills_many(
        user_trades,
        close_order_ids,
    )
    net_pnl = close_fill["realized_pnl"] - entry_fill["commission"] - close_fill["commission"]
    filled_entry_qty = entry_fill["qty"] or safe_float(entry_order.get("executedQty")) or entry_quantity
    reference_entry_price = entry_fill["avg_price"] or entry_price
    estimated_slippage_bps = 0.0
    if intent.entry_price > 0 and reference_entry_price > 0:
        estimated_slippage_bps = abs((reference_entry_price / intent.entry_price) - 1.0) * 10_000.0
    partial_fill = bool(intent.quantity > 0 and filled_entry_qty < intent.quantity * 0.999)
    close_price = round4(close_fill["avg_price"] or safe_float(management["close_avg_price"]))
    entry_price_for_record = round4(entry_fill["avg_price"] or entry_price)
    baseline_metrics = _trade_path_metrics(
        {
            "side": candidate.side,
            "entry_price": entry_price_for_record,
            "close_price": close_price,
            "best_mark_price": safe_float(management["best_mark_price"]),
            "worst_mark_price": safe_float(management["worst_mark_price"]),
        }
    )
    dynamic_exit_payload = build_parallel_dynamic_exit_simulation(
        candidate=candidate,
        entry_price=entry_price_for_record,
        management=management,
        baseline_return_pct=baseline_metrics["return_pct"],
    )
    experiment_metadata = experiment_trade_metadata(experiment)
    runtime_mode_resolved = normalize_runtime_mode(getattr(args, "runtime_mode", TESTNET_LIVE))
    credentials_environment_resolved = resolve_environment(str(getattr(args, "env", "testnet") or "testnet")).name
    execution_mode_resolved = f"{runtime_mode_resolved}_ORDER_SUBMIT"
    round_config_snapshot = round_config_snapshot or {}
    round_config_snapshot_id = round_config_snapshot.get("round_config_snapshot_id")
    round_config_snapshot_path = round_config_snapshot.get("round_config_snapshot_path")
    telemetry_schema_path = round_config_snapshot.get("telemetry_schema_path")
    entry_notional = safe_float(entry_fill.get("quote_qty")) or (safe_float(entry_fill.get("avg_price")) * safe_float(entry_fill.get("qty")))
    exit_notional = safe_float(close_fill.get("quote_qty")) or (safe_float(close_fill.get("avg_price")) * safe_float(close_fill.get("qty")))
    total_notional = entry_notional + exit_notional
    total_commission = safe_float(entry_fill.get("commission")) + safe_float(close_fill.get("commission"))
    fee_bps_effective = round4((total_commission / total_notional) * 10_000.0) if total_notional > 0 else None
    entry_maker_or_taker = str(entry_fill.get("maker_or_taker") or "unknown")
    exit_maker_or_taker = str(close_fill.get("maker_or_taker") or "unknown")
    maker_or_taker = combine_liquidity_roles(entry_maker_or_taker, exit_maker_or_taker)
    entry_timestamp_ms = safe_int(entry_fill.get("first_time_ms"))
    exit_timestamp_ms = safe_int(close_fill.get("last_time_ms"))
    signal_timestamp_ms = safe_int(entry_signal_timestamp)
    entry_latency_reference_ms = signal_timestamp_ms if signal_timestamp_ms is not None else entry_submit_timestamp_ms
    entry_to_fill_latency_ms = latency_ms_between(entry_latency_reference_ms, entry_timestamp_ms)
    exit_signal_timestamp_ms = safe_int(management.get("exit_signal_timestamp"))
    exit_to_fill_latency_ms = latency_ms_between(exit_signal_timestamp_ms, exit_timestamp_ms)
    entry_slippage_proxy_bps = execution_slippage_proxy_bps(
        trade_side=candidate.side,
        phase="entry",
        reference_price=intent.entry_price,
        actual_price=entry_price_for_record,
    )
    exit_slippage_proxy_bps = execution_slippage_proxy_bps(
        trade_side=candidate.side,
        phase="exit",
        reference_price=safe_float(management.get("last_mark_price")),
        actual_price=close_price,
    )
    slippage_parts = [item for item in (entry_slippage_proxy_bps, exit_slippage_proxy_bps) if item is not None]
    realized_slippage_proxy_bps = round4(sum(slippage_parts)) if slippage_parts else None
    entry_spread_proxy = spread_proxy_from_book(entry_book_ticker)
    exit_spread_proxy = {"available": False, "spread_bps": None, "source": "not_collected_by_testnet_runner"}
    funding_event_linkage = {
        "available": False,
        "source": "not_collected_by_testnet_runner",
        "matched_income_event_count": 0,
        "notes": "Runner does not collect income/funding history in the trade path.",
    }
    regime_context = {
        "btc_regime": None,
        "oi_regime": None,
        "liquidity_bucket": liquidity_bucket_from_turnover(candidate.avg_quote_turnover_1m),
        "hmm_state_if_available": None,
        "markov_state_if_available": None,
        "source": "testnet_runner_trade_context",
    }
    strategy_gate_payload = strategy_gate_decision or {}
    strategy_gate_reports = strategy_gate_payload.get("reports") if isinstance(strategy_gate_payload.get("reports"), dict) else {}
    cost_gate_payload = strategy_gate_reports.get("cost_gate") if isinstance(strategy_gate_reports.get("cost_gate"), dict) else {}
    cost_debug = cost_gate_payload.get("debug") if isinstance(cost_gate_payload.get("debug"), dict) else {}
    resolved_experiment_id = (
        experiment_metadata.get("experiment_id")
        or strategy_gate_payload.get("experiment_id")
        or f"testnet_runner_v1:{candidate.setup}"
    )
    resolved_strategy_manifest_id = (
        experiment_metadata.get("strategy_manifest_id")
        or strategy_gate_payload.get("strategy_manifest_id")
        or candidate.strategy_manifest_id
    )
    resolved_strategy_family = (
        experiment_metadata.get("strategy_family")
        or strategy_gate_payload.get("strategy_family")
        or candidate.strategy_family
    )
    resolved_evidence_level = (
        experiment_metadata.get("evidence_level")
        or strategy_gate_payload.get("candidate_evidence_level")
        or strategy_gate_payload.get("evidence_level")
        or candidate.evidence_level
    )

    trade_record = {
        "telemetry_schema_version": TESTNET_TRADE_TELEMETRY_SCHEMA_VERSION,
        "runtime_mode": runtime_mode_resolved,
        "runtime_mode_resolved": runtime_mode_resolved,
        "credentials_environment_resolved": credentials_environment_resolved,
        "execution_mode_resolved": execution_mode_resolved,
        "order_submit_endpoint": ORDER_SUBMIT_PATH,
        "round_config_snapshot_id": round_config_snapshot_id,
        "round_config_snapshot_path": round_config_snapshot_path,
        "telemetry_schema_path": telemetry_schema_path,
        "margin_type": margin_type,
        "symbol": candidate.symbol,
        "setup": candidate.setup,
        "strategy_id": experiment_metadata.get("strategy_id") or candidate.setup,
        "strategy_manifest_id": resolved_strategy_manifest_id,
        "strategy_family": resolved_strategy_family,
        "strategy_version": experiment_metadata.get("strategy_version") or "testnet_runner_v1",
        "parent_version": experiment_metadata.get("parent_version"),
        "experiment_id": resolved_experiment_id,
        "evidence_level": resolved_evidence_level,
        "cost_model_version": experiment_metadata.get("cost_model_version"),
        "regime_model_version": experiment_metadata.get("regime_model_version"),
        "data_snapshot_id": experiment_metadata.get("data_snapshot_id"),
        "code_git_sha": experiment_metadata.get("code_git_sha"),
        "config_hash": experiment_metadata.get("config_hash"),
        "baseline_id": experiment_metadata.get("baseline_id") or (None if experiment_metadata.get("experiment_id") else f"testnet_runner_v1:{candidate.setup}"),
        "candidate_experiment_active": experiment_metadata.get("candidate_experiment_active"),
        "candidate_allocation_pct": experiment_metadata.get("candidate_allocation_pct"),
        "experiment_type": experiment_metadata.get("experiment_type"),
        "created_from_proposal": experiment_metadata.get("created_from_proposal"),
        "expected_gross_move_bps": cost_debug.get("expected_gross_move_bps"),
        "expected_total_cost_bps": cost_debug.get("expected_total_cost_bps"),
        "expected_net_edge_bps": cost_debug.get("expected_net_edge_bps"),
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "direct_live_change_allowed": False,
        "side": candidate.side,
        "score": round4(candidate.score),
        "quote_volume_24h": round4(candidate.quote_volume_24h),
        "avg_quote_turnover_1m": round4(candidate.avg_quote_turnover_1m),
        "current_quote_turnover_1m": round4(candidate.current_quote_turnover_1m),
        "price_change_24h_pct": round4(candidate.price_change_24h_pct),
        "ret_1m_pct": round4(candidate.ret_1m_pct),
        "ret_3m_pct": round4(candidate.ret_3m_pct),
        "ret_5m_pct": round4(candidate.ret_5m_pct),
        "ret_15m_pct": round4(candidate.ret_15m_pct),
        "ret_30m_pct": round4(candidate.ret_30m_pct),
        "ret_60m_pct": round4(candidate.ret_60m_pct),
        "volume_ratio": round4(candidate.volume_ratio),
        "range_position": round4(candidate.range_position),
        "trend_gap_pct": round4(candidate.trend_gap_pct),
        "higher_tf_trend_gap_pct": round4(candidate.higher_tf_trend_gap_pct),
        "pullback_pct": round4(candidate.pullback_pct),
        "bounce_pct": round4(candidate.bounce_pct),
        "tf5_ret_3bar_pct": round4(candidate.tf5_ret_3bar_pct),
        "tf5_ret_6bar_pct": round4(candidate.tf5_ret_6bar_pct),
        "tf5_volume_ratio": round4(candidate.tf5_volume_ratio),
        "tf5_range_position": round4(candidate.tf5_range_position),
        "tf5_trend_gap_pct": round4(candidate.tf5_trend_gap_pct),
        "tf5_pullback_pct": round4(candidate.tf5_pullback_pct),
        "tf15_ret_3bar_pct": round4(candidate.tf15_ret_3bar_pct),
        "tf15_ret_6bar_pct": round4(candidate.tf15_ret_6bar_pct),
        "tf15_volume_ratio": round4(candidate.tf15_volume_ratio),
        "tf15_range_position": round4(candidate.tf15_range_position),
        "tf15_trend_gap_pct": round4(candidate.tf15_trend_gap_pct),
        "tf15_pullback_pct": round4(candidate.tf15_pullback_pct),
        "leverage": intent.leverage,
        "quote_allocation_usdt": round4(intent.quote_allocation_usdt),
        "quote_allocation": round4(intent.quote_allocation_usdt),
        "quantity": round4(entry_quantity),
        "latency_ms": round4(latency_ms),
        "entry_submit_timestamp": entry_submit_timestamp_ms,
        "entry_order_response_timestamp": entry_order_response_timestamp_ms,
        "entry_signal_timestamp": signal_timestamp_ms,
        "actual_entry_timestamp": entry_timestamp_ms,
        "entry_to_fill_latency_ms": entry_to_fill_latency_ms,
        "entry_submit_to_fill_latency_ms": latency_ms_between(entry_submit_timestamp_ms, entry_timestamp_ms),
        "exit_signal_timestamp": exit_signal_timestamp_ms,
        "close_order_submit_timestamp": safe_int(management.get("close_order_submit_timestamp")),
        "close_order_response_timestamp": safe_int(management.get("close_order_response_timestamp")),
        "actual_exit_timestamp": exit_timestamp_ms,
        "exit_to_fill_latency_ms": exit_to_fill_latency_ms,
        "exit_submit_to_fill_latency_ms": latency_ms_between(management.get("close_order_submit_timestamp"), exit_timestamp_ms),
        "estimated_slippage_bps": round4(estimated_slippage_bps),
        "estimated_slippage": {
            "entry_bps": round4(estimated_slippage_bps),
            "exit_bps": None,
            "total_bps": round4(estimated_slippage_bps),
            "source": "entry_avg_fill_vs_trade_intent",
        },
        "realized_slippage_proxy_bps": realized_slippage_proxy_bps,
        "realized_slippage_proxy": {
            "entry_bps": entry_slippage_proxy_bps,
            "exit_bps": exit_slippage_proxy_bps,
            "total_bps": realized_slippage_proxy_bps,
            "source": "entry_fill_vs_intent_and_exit_fill_vs_last_mark",
            "positive_bps_means": "worse_than_reference",
        },
        "spread_proxy_at_entry": entry_spread_proxy,
        "spread_proxy_at_entry_bps": entry_spread_proxy.get("spread_bps"),
        "spread_proxy_at_exit": exit_spread_proxy,
        "spread_proxy_at_exit_bps": exit_spread_proxy.get("spread_bps"),
        "maker_or_taker": maker_or_taker,
        "entry_maker_or_taker": entry_maker_or_taker,
        "exit_maker_or_taker": exit_maker_or_taker,
        "entry_fill_summary": entry_fill,
        "exit_fill_summary": close_fill,
        "entry_order_ids": entry_order_ids,
        "close_order_ids": close_order_ids,
        "partial_fill": partial_fill,
        "reject": False,
        "reject_reason": "",
        "entry_order_id": int(entry_order.get("orderId") or 0),
        "entry_price": entry_price_for_record,
        "close_order_id": int(management["close_order_id"]),
        "close_price": close_price,
        "actual_close_avg_price": close_price,
        "intended_take_profit_price": round4(safe_float(management.get("intended_take_profit_price"))),
        "raw_take_profit_pct": management.get("raw_take_profit_pct"),
        "effective_take_profit_pct": management.get("effective_take_profit_pct"),
        "fee_aware_take_profit": management.get("fee_aware_take_profit") or {},
        "trigger_price": round4(safe_float(management.get("intended_take_profit_price"))),
        "last_mark_price": round4(safe_float(management.get("last_mark_price"))),
        "exit_reason": str(management["exit_reason"]),
        "hold_seconds": management["hold_seconds"],
        "best_mark_price": round4(safe_float(management["best_mark_price"])),
        "worst_mark_price": round4(safe_float(management["worst_mark_price"])),
        "mfe": baseline_metrics["mfe_pct"],
        "mae": baseline_metrics["mae_pct"],
        "mfe_pct": baseline_metrics["mfe_pct"],
        "mae_pct": baseline_metrics["mae_pct"],
        "return_pct": baseline_metrics["return_pct"],
        "profit_giveback_pct": baseline_metrics["profit_giveback_pct"],
        "realized_pnl_usdt": round4(close_fill["realized_pnl"]),
        "gross_pnl_usdt": round4(close_fill["realized_pnl"]),
        "commission_usdt": round4(entry_fill["commission"] + close_fill["commission"]),
        "exchange_fee": {
            "total_usdt": round4(total_commission),
            "entry_usdt": round4(safe_float(entry_fill.get("commission"))),
            "exit_usdt": round4(safe_float(close_fill.get("commission"))),
            "entry_commission_asset": entry_fill.get("commission_asset"),
            "exit_commission_asset": close_fill.get("commission_asset"),
            "source": "futures.user_trades",
        },
        "fee_bps_effective": fee_bps_effective,
        "funding_paid_or_received": None,
        "funding_paid_or_received_usdt": None,
        "income_funding_event_linkage": funding_event_linkage,
        "funding_event_linkage": funding_event_linkage,
        "net_pnl_usdt": round4(net_pnl),
        "btc_regime": regime_context["btc_regime"],
        "oi_regime": regime_context["oi_regime"],
        "liquidity_bucket": regime_context["liquidity_bucket"],
        "hmm_state_if_available": regime_context["hmm_state_if_available"],
        "markov_state_if_available": regime_context["markov_state_if_available"],
        "regime_context": regime_context,
        "learning_gate_decision": learning_gate_decision or {},
        "strategy_gate_decision": strategy_gate_decision or {},
        "baseline_exit_result": dynamic_exit_payload["baseline_exit_result"],
        "dynamic_exit_simulation": dynamic_exit_payload["dynamic_exit_simulated_result"],
        "dynamic_exit_simulated_result": dynamic_exit_payload["dynamic_exit_simulated_result"],
        "would_dynamic_exit_improve": dynamic_exit_payload["would_dynamic_exit_improve"],
    }
    return apply_trade_cost_attribution(trade_record)


async def main_async(args: argparse.Namespace) -> int:
    runtime_mode = validate_runtime_mode_args(args)
    if runtime_mode == MAINNET_SHADOW:
        print(
            json.dumps(
                {
                    "event": "runtime_mode_shadow_noop",
                    "runtime_mode": runtime_mode,
                    "reason": "MAINNET_SHADOW must not submit orders from phoenix_testnet_round_runner.",
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 0
    credentials = load_credentials(required=True)
    credentials = BinanceCredentials(
        api_key=credentials.api_key,
        api_secret=credentials.api_secret,
        environment=resolve_environment(str(args.env)),
        account_api_preference=credentials.account_api_preference,
        recv_window_ms=credentials.recv_window_ms,
    )
    proxy_settings = load_proxy_settings()
    settings = load_execution_settings()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    telemetry_schema_path = args.output_dir / "testnet_trade_telemetry_schema.json"
    write_json_artifact(telemetry_schema_path, build_testnet_trade_telemetry_schema())
    learning_store_file = None if bool(getattr(args, "disable_learning_store", False)) else (args.learning_store_file or args.output_dir / "learning_store.jsonl")
    active_experiments: list[dict[str, Any]] = []
    learning_gate_config = LearningGateConfig()
    learning_memory_records = load_recent_learning_records(args.output_dir, learning_store_file)
    timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        futures = BinanceFuturesClient(
            session=session,
            environment=credentials.environment,
            credentials=credentials,
            proxy_settings=proxy_settings,
        )
        await call_with_rate_limit_backoff(futures.sync_server_time, label="sync_server_time")
        executor = PhoenixExecutor(futures_client=futures, settings=settings)
        stop_requested = False
        pending_trade_tasks: dict[asyncio.Task[dict[str, Any]], dict[str, Any]] = {}
        api_error_events = 0
        order_reject_events = 0
        trade_attempt_events = 0

        def _request_stop(signum: int, _frame: object) -> None:
            nonlocal stop_requested
            stop_requested = True

        previous_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, _request_stop)
        previous_sigterm = None
        if hasattr(signal, "SIGTERM"):
            previous_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, _request_stop)

        try:
            for round_no in round_numbers(args):
                if stop_requested:
                    break
                if not bool(getattr(args, "disable_experiment_candidates", False)):
                    active_experiments = load_active_experiments(args.experiments_dir)
                if not bool(getattr(args, "disable_learning_gate", False)):
                    learning_memory_records = load_recent_learning_records(args.output_dir, learning_store_file)
                flattened_positions = await call_with_rate_limit_backoff(
                    lambda: flatten_active_positions(futures),
                    label="startup_flatten",
                    round_no=round_no,
                )
                if flattened_positions:
                    print(json.dumps({"event": "startup_flatten", "round": round_no, "flattened": flattened_positions}, ensure_ascii=False), flush=True)
                orphan_orders = await call_with_rate_limit_backoff(
                    lambda: cancel_orphan_orders(futures),
                    label="startup_orphan_cleanup",
                    round_no=round_no,
                )
                if orphan_orders:
                    print(json.dumps({"event": "startup_orphan_cleanup", "round": round_no, "cancelled": orphan_orders}, ensure_ascii=False), flush=True)
                balance_before = extract_usdt_available_balance(
                    await call_with_rate_limit_backoff(
                        futures.account_overview,
                        label="balance_before",
                        round_no=round_no,
                    )
                )
                trades: list[dict[str, Any]] = []
                trades_path = args.output_dir / f"round_{round_no:03d}_trades.jsonl"
                round_config_snapshot_path = args.output_dir / f"round_{round_no:03d}_config_snapshot.json"
                round_config_snapshot = build_round_config_snapshot(
                    args,
                    runtime_mode=runtime_mode,
                    credentials_environment=credentials.environment.name,
                    settings=settings,
                    round_no=round_no,
                )
                round_config_snapshot["round_config_snapshot_path"] = str(round_config_snapshot_path)
                round_config_snapshot["telemetry_schema_path"] = str(telemetry_schema_path)
                write_json_artifact(round_config_snapshot_path, round_config_snapshot)
                min_score = max(24.0, float(args.starting_min_score))
                idle_cycles = 0
                last_symbol: str | None = None
                blocked_symbols: dict[str, float] = {}
                active_symbols: set[str] = set()
                market_cache: dict[str, Any] = {}
                round_started = time.time()
                target_trade_count = max(1, args.trades_per_round)
                max_concurrent_trades = min(
                    max(1, int(args.max_concurrent_trades or 1)),
                    max(1, int(args.max_open_positions or 1)),
                )
                pending_trade_tasks.clear()

                while len(trades) < target_trade_count or pending_trade_tasks:
                    completed_tasks: set[asyncio.Task[dict[str, Any]]] = set()
                    if pending_trade_tasks:
                        completed_tasks, _ = await asyncio.wait(
                            list(pending_trade_tasks),
                            timeout=0.2,
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                    post_task_sleep_sec = 0.0
                    for task in completed_tasks:
                        meta = pending_trade_tasks.pop(task, {})
                        candidate_meta = meta.get("candidate")
                        experiment_meta = meta.get("experiment")
                        if isinstance(candidate_meta, Candidate):
                            active_symbols.discard(candidate_meta.symbol)
                        try:
                            trade_record = task.result()
                        except asyncio.CancelledError:
                            continue
                        except (BinanceAPIError, ValueError) as exc:
                            if isinstance(exc, BinanceAPIError):
                                api_error_events += 1
                                order_reject_events += 1
                                backoff_sec = parse_rate_limit_backoff_seconds(exc)
                                if backoff_sec is not None:
                                    market_cache.clear()
                                    print(
                                        json.dumps(
                                            {
                                                "event": "rate_limit_backoff",
                                                "round": round_no,
                                                "sleep_seconds": round4(backoff_sec),
                                                "error": str(exc),
                                            },
                                            ensure_ascii=False,
                                        ),
                                        flush=True,
                                    )
                                    post_task_sleep_sec = max(post_task_sleep_sec, min(max(5.0, backoff_sec), 900.0))
                            if isinstance(candidate_meta, Candidate):
                                if isinstance(experiment_meta, dict):
                                    record_experiment_hard_safety_fail(
                                        args.experiments_dir,
                                        str(experiment_meta.get("experiment_id") or ""),
                                        reason=str(exc),
                                    )
                                print(
                                    json.dumps(
                                        {
                                            "event": "trade_skipped",
                                            "round": round_no,
                                            "symbol": candidate_meta.symbol,
                                            "setup": candidate_meta.setup,
                                            "side": candidate_meta.side,
                                            "error": str(exc),
                                        },
                                        ensure_ascii=False,
                                    ),
                                    flush=True,
                                )
                                last_symbol = candidate_meta.symbol
                                apply_symbol_cooldown(
                                    blocked_symbols,
                                    candidate_meta.symbol,
                                    seconds=SYMBOL_ERROR_COOLDOWN_SEC,
                                )
                            continue
                        except Exception as exc:
                            if isinstance(candidate_meta, Candidate):
                                if isinstance(experiment_meta, dict):
                                    record_experiment_hard_safety_fail(
                                        args.experiments_dir,
                                        str(experiment_meta.get("experiment_id") or ""),
                                        reason=str(exc),
                                    )
                                print(
                                    json.dumps(
                                        {
                                            "event": "trade_skipped",
                                            "round": round_no,
                                            "symbol": candidate_meta.symbol,
                                            "setup": candidate_meta.setup,
                                            "side": candidate_meta.side,
                                            "error": str(exc),
                                        },
                                        ensure_ascii=False,
                                    ),
                                    flush=True,
                                )
                                last_symbol = candidate_meta.symbol
                                apply_symbol_cooldown(
                                    blocked_symbols,
                                    candidate_meta.symbol,
                                    seconds=SYMBOL_ERROR_COOLDOWN_SEC,
                                )
                            continue

                        trades.append(trade_record)
                        learning_memory_records.append(trade_record)
                        last_symbol = str(trade_record["symbol"])
                        if safe_float(trade_record.get("net_pnl_usdt")) <= 0.0:
                            apply_symbol_cooldown(
                                blocked_symbols,
                                last_symbol,
                                seconds=SYMBOL_LOSS_COOLDOWN_SEC,
                            )
                        with trades_path.open("a", encoding="utf-8") as handle:
                            handle.write(json.dumps(trade_record, ensure_ascii=False) + "\n")
                        if learning_store_file is not None:
                            try:
                                append_learning_row(learning_store_file, build_testnet_learning_row(trade_record))
                            except Exception as exc:
                                print(
                                    json.dumps(
                                        {
                                            "event": "learning_store_append_failed",
                                            "round": round_no,
                                            "symbol": trade_record.get("symbol"),
                                            "error": str(exc),
                                        },
                                        ensure_ascii=False,
                                    ),
                                    flush=True,
                                )
                        print(
                            json.dumps(
                                {
                                    "event": "trade_closed",
                                    "round": round_no,
                                    "trade_index": len(trades),
                                    "symbol": trade_record["symbol"],
                                    "side": trade_record["side"],
                                    "net_pnl_usdt": trade_record["net_pnl_usdt"],
                                    "exit_reason": trade_record["exit_reason"],
                                },
                                ensure_ascii=False,
                            ),
                            flush=True,
                        )

                    if post_task_sleep_sec > 0:
                        await asyncio.sleep(post_task_sleep_sec)
                        continue

                    if len(trades) >= target_trade_count and not pending_trade_tasks:
                        break

                    round_expired = args.max_round_sec > 0 and (time.time() - round_started) >= args.max_round_sec
                    if stop_requested or round_expired:
                        if not pending_trade_tasks:
                            break
                        await asyncio.sleep(0.25)
                        continue

                    if len(trades) + len(pending_trade_tasks) >= target_trade_count:
                        await asyncio.sleep(0.1)
                        continue

                    attempts = max(1, trade_attempt_events)
                    throttle = dynamic_concurrency_throttle(
                        requested_slots=max_concurrent_trades,
                        max_open_positions=max(1, int(args.max_open_positions or 1)),
                        current_open_positions=len(pending_trade_tasks),
                        api_error_rate=api_error_events / attempts,
                        order_reject_rate=order_reject_events / attempts,
                        base_cooldown_sec=float(args.cooldown_sec),
                    )
                    effective_max_slots = int(throttle["max_slots"])
                    if effective_max_slots <= 0 or len(pending_trade_tasks) >= effective_max_slots:
                        await asyncio.sleep(0.1)
                        continue

                    try:
                        candidate = await pick_candidate(
                            futures,
                            universe_top=max(args.scan_top, args.universe_top),
                            scan_top=max(2, args.scan_top),
                            min_quote_volume=max(1_000_000.0, args.min_quote_volume),
                            min_score=max(24.0, min_score),
                            last_symbol=last_symbol,
                            blocked_symbols=blocked_symbols,
                            excluded_symbols=active_symbols,
                            market_cache=market_cache,
                            allow_short=bool(args.allow_short),
                            execution_floor_offset=float(args.execution_floor_offset or 0.0),
                        )
                    except BinanceAPIError as exc:
                        backoff_sec = parse_rate_limit_backoff_seconds(exc)
                        if backoff_sec is None:
                            raise
                        market_cache.clear()
                        print(
                            json.dumps(
                                {
                                    "event": "rate_limit_backoff",
                                    "round": round_no,
                                    "sleep_seconds": round4(backoff_sec),
                                    "error": str(exc),
                                },
                                ensure_ascii=False,
                            ),
                            flush=True,
                        )
                        await asyncio.sleep(min(max(5.0, backoff_sec), 900.0))
                        continue
                    if candidate is None:
                        idle_cycles += 1
                        if idle_cycles >= 3:
                            min_score = max(24.0, min_score - 3.0)
                        await asyncio.sleep(max(0.25, min(float(args.idle_sleep_sec), 1.5)))
                        continue

                    idle_cycles = 0
                    min_score = max(26.0, float(args.starting_min_score))
                    learning_gate_decision = {}
                    if not bool(getattr(args, "disable_learning_gate", False)):
                        learning_gate_decision = build_learning_gate_decision(
                            candidate,
                            recent_records=learning_memory_records,
                            config=learning_gate_config,
                        )
                        print(
                            json.dumps(
                                {
                                    "event": "learning_gate_decision",
                                    "round": round_no,
                                    "symbol": candidate.symbol,
                                    "setup": candidate.setup,
                                    "decision": learning_gate_decision.get("decision"),
                                    "reason": learning_gate_decision.get("reason"),
                                    "recent_trade_count": learning_gate_decision.get("recent_trade_count"),
                                    "win_rate_pct": learning_gate_decision.get("win_rate_pct"),
                                    "avg_net_pnl_usdt": learning_gate_decision.get("avg_net_pnl_usdt"),
                                    "median_slippage_bps": learning_gate_decision.get("median_slippage_bps"),
                                },
                                ensure_ascii=False,
                            ),
                            flush=True,
                        )
                        if learning_gate_decision.get("decision") == "block":
                            apply_symbol_cooldown(
                                blocked_symbols,
                                candidate.symbol,
                                seconds=SYMBOL_ERROR_COOLDOWN_SEC,
                            )
                            last_symbol = candidate.symbol
                            await asyncio.sleep(max(0.05, min(float(args.cooldown_sec), 2.0)))
                            continue
                        if learning_gate_decision.get("decision") == "downweight":
                            candidate.score = max(0.0, candidate.score * safe_float(learning_gate_decision.get("score_multiplier")))
                    experiment = None
                    if active_experiments:
                        experiment = choose_experiment_for_runner_candidate(
                            candidate,
                            active_experiments,
                            allocation_pct=float(args.candidate_allocation_pct),
                            seed=f"{round_no}:{trade_attempt_events}:{candidate.symbol}:{candidate.setup}",
                        )
                    strategy_gate_decision = evaluate_testnet_candidate_gate(candidate, experiment, args)
                    print(
                        json.dumps(
                            {
                                "event": "strategy_candidate_gate_decision",
                                "round": round_no,
                                "symbol": candidate.symbol,
                                "setup": candidate.setup,
                                "decision": strategy_gate_decision.get("decision"),
                                "block_reasons": strategy_gate_decision.get("block_reasons"),
                                "strategy_manifest_id": strategy_gate_decision.get("strategy_manifest_id"),
                                "experiment_id": strategy_gate_decision.get("experiment_id"),
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
                    if strategy_gate_decision.get("decision") == "block":
                        blocked_report_file = args.blocked_candidate_report_file or args.output_dir / "blocked_candidates.jsonl"
                        append_jsonl_artifact(
                            blocked_report_file,
                            {
                                "event": "blocked_candidate",
                                "round": round_no,
                                "symbol": candidate.symbol,
                                "setup": candidate.setup,
                                "side": candidate.side,
                                "score": round4(candidate.score),
                                "decision": strategy_gate_decision,
                            },
                        )
                        apply_symbol_cooldown(
                            blocked_symbols,
                            candidate.symbol,
                            seconds=SYMBOL_ERROR_COOLDOWN_SEC,
                        )
                        last_symbol = candidate.symbol
                        await asyncio.sleep(max(0.05, min(float(args.cooldown_sec), 2.0)))
                        continue
                    available_balance = extract_usdt_available_balance(
                        await call_with_rate_limit_backoff(
                            futures.account_overview,
                            label="available_balance",
                            round_no=round_no,
                        )
                    )
                    entry_signal_timestamp_ms = int(time.time() * 1000)
                    trade_task = asyncio.create_task(
                        run_trade(
                            futures,
                            executor,
                            candidate=candidate,
                            available_balance=available_balance,
                            args=args,
                            open_trade_state={},
                            experiment=experiment,
                            learning_gate_decision=learning_gate_decision,
                            strategy_gate_decision=strategy_gate_decision,
                            round_config_snapshot=round_config_snapshot,
                            entry_signal_timestamp=entry_signal_timestamp_ms,
                        ),
                        name=f"trade:{candidate.symbol}:{candidate.setup}",
                    )
                    trade_attempt_events += 1
                    pending_trade_tasks[trade_task] = {"candidate": candidate, "experiment": experiment, "launched_at": time.time()}
                    active_symbols.add(candidate.symbol)
                    last_symbol = candidate.symbol
                    print(
                        json.dumps(
                            {
                                "event": "trade_launched",
                                "round": round_no,
                                "symbol": candidate.symbol,
                                "setup": candidate.setup,
                                "side": candidate.side,
                                "score": round4(candidate.score),
                                "active_slots": len(pending_trade_tasks),
                                "max_slots": effective_max_slots,
                                "throttle_reasons": throttle["reasons"],
                                "experiment_id": experiment.get("experiment_id") if isinstance(experiment, dict) else None,
                                "candidate_experiment_active": isinstance(experiment, dict),
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
                    await asyncio.sleep(max(0.05, min(float(throttle["cooldown_sec"]), 5.0)))

                balance_after = extract_usdt_available_balance(
                    await call_with_rate_limit_backoff(
                        futures.account_overview,
                        label="balance_after",
                        round_no=round_no,
                    )
                )
                report = build_round_review(round_no, trades, balance_before, balance_after)
                report["trades_file"] = str(trades_path)
                report["round_config_snapshot_id"] = round_config_snapshot.get("round_config_snapshot_id")
                report["round_config_snapshot_file"] = str(round_config_snapshot_path)
                report["telemetry_schema_file"] = str(telemetry_schema_path)
                report_path = args.output_dir / f"round_{round_no:03d}_report.json"
                report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                print(json.dumps({"event": "round_complete", **report}, ensure_ascii=False), flush=True)
                positions_after_round = await call_with_rate_limit_backoff(
                    futures.position_information_v3,
                    label="after_round_position_check",
                    round_no=round_no,
                )
                abnormal_open_positions = active_position_count(positions_after_round) > 0
                if abnormal_open_positions or bool(getattr(args, "flatten_after_round", False)):
                    flattened_positions = await call_with_rate_limit_backoff(
                        lambda: flatten_active_positions(futures),
                        label="after_round_flatten",
                        round_no=round_no,
                    )
                    if flattened_positions:
                        print(
                            json.dumps(
                                {
                                    "event": "after_round_flatten",
                                    "round": round_no,
                                    "abnormal": abnormal_open_positions,
                                    "flattened": flattened_positions,
                                },
                                ensure_ascii=False,
                            ),
                            flush=True,
                        )
        finally:
            if pending_trade_tasks:
                for task in list(pending_trade_tasks):
                    task.cancel()
                await asyncio.gather(*list(pending_trade_tasks), return_exceptions=True)
                pending_trade_tasks.clear()
            flattened_positions = await flatten_active_positions(futures)
            if flattened_positions:
                print(json.dumps({"event": "shutdown_flatten", "flattened": flattened_positions}, ensure_ascii=False), flush=True)
            orphan_orders = await cancel_orphan_orders(futures)
            if orphan_orders:
                print(json.dumps({"event": "shutdown_orphan_cleanup", "cancelled": orphan_orders}, ensure_ascii=False), flush=True)
            signal.signal(signal.SIGINT, previous_sigint)
            if hasattr(signal, "SIGTERM") and previous_sigterm is not None:
                signal.signal(signal.SIGTERM, previous_sigterm)

    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
