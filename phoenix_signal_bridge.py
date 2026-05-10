#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import contextlib
import gc
import json
import ctypes
import os
import signal
import time
from collections import Counter, deque
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import aiohttp

from phoenix_factor_library import build_factor_vector
from phoenix_factor_factory import (
    build_factor_factory_control,
    build_factor_factory_report,
    read_recent_jsonl_records as read_recent_factor_jsonl_records,
)
from phoenix_research_governance import (
    audit_lookahead_recursive_bias,
    build_execution_realism_report,
    promotion_gate_decision,
)
from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import load_credentials, load_execution_settings, load_proxy_settings, resolve_environment
from phoenix.executor import PhoenixExecutor
from phoenix.oms_state import (
    UserDataOMSState,
    build_reconciled_intent,
    choose_reconciled_entry_state,
    classify_entry_reconciliation,
    extract_order_response_state,
    sync_plan_quantities_to_fill,
)
from phoenix_live_execute import (
    extract_available_balance,
    extract_symbol_config,
    find_instruction,
    has_open_position,
    open_position_symbols,
    place_instruction,
    spawn_post_fill_worker,
)
from phoenix_momentum_scalp_plus import (
    STRATEGY_FAMILY as MOMENTUM_SCALP_STRATEGY_FAMILY,
    MomentumScalpConfig,
    build_momentum_scalp_shadow_signals,
    build_momentum_scalp_signals_from_confirmation,
    build_strategy_shadow_league_markdown,
    build_strategy_shadow_league_report,
    detect_momentum_scalp_radar_event,
    resolve_momentum_scalp_confirmation,
    simulate_momentum_scalp_exit,
)
from phoenix_signal_lab import event_context_label
from phoenix_signal_bridge_modes import (
    EXECUTION_MODE_MAINNET_SHADOW,
    EXECUTION_MODE_TESTNET_LIVE,
    candidate_parking_lot_enabled,
    candidate_shadow_writes_enabled,
    is_mainnet_shadow_mode,
    normalize_execution_mode,
    user_data_oms_enabled_for_mode,
    validate_execution_mode_args,
)


DEFAULT_ALLOWED_PLAYBOOKS = (
    "oi_build_breakout",
    "liquidation_flush",
)
PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED = "oi_unwind_reversal_confirmed"
PLAYBOOK_DISCOVERY_TREND_MILD_CONTINUATION = "discovery_trend_mild_volume_continuation"
PLAYBOOK_DISCOVERY_OI_UNWIND_REVERSAL_SCALP = "discovery_oi_unwind_reversal_scalp"
PLAYBOOK_DISCOVERY_LOW_LIQ_DEPTH_REVERSAL = "discovery_low_liq_depth_reversal"
PLAYBOOK_DISCOVERY_OI_UNWIND_5M_RANGE_REVERSAL = "discovery_oi_unwind_5m_range_reversal"
PLAYBOOK_DISCOVERY_BIDWALL_OI_BUILD_CONTINUATION = "discovery_bidwall_oi_build_continuation"
PLAYBOOK_DISCOVERY_FACTOR_TREND_ALIGNMENT = "discovery_factor_trend_alignment"
PLAYBOOK_DISCOVERY_FACTOR_LIQUIDATION_REVERSION = "discovery_factor_liquidation_reversion"
PLAYBOOK_SHADOW_OI_BUILD_QUALITY = "shadow_oi_build_breakout_quality"
PLAYBOOK_SHADOW_OI_BUILD_BALANCED = "shadow_oi_build_breakout_balanced"
PLAYBOOK_SHADOW_OI_BUILD_V2 = PLAYBOOK_SHADOW_OI_BUILD_BALANCED
PLAYBOOK_DISCOVERY_HIST_BUY_1H_TREND_REVERSION_EXTREME = "discovery_hist_buy_1h_trend_reversion_extreme"
PLAYBOOK_DISCOVERY_HIST_BUY_1H_REVERSION_OI_UNWIND_LOW = "discovery_hist_buy_1h_reversion_oi_unwind_low"
DEFAULT_PERMANENT_BLOCKED_SYMBOLS = (
    "CHIPUSDT",
    "CLUSDT",
    "ENSOUSDT",
    "OPGUSDT",
    "PRLUSDT",
    "SNDKUSDT",
    "XAUUSDT",
)
DEFAULT_PROCESSED_EVENT_CAP = 4096
DEFAULT_SHADOW_TARGET_HORIZONS_SEC = (900, 1800, 3600)
DEFAULT_RESEARCH_SHADOW_TARGET_HORIZONS_SEC = (60, 180, 300, 900)
DEFAULT_LIVE_UNIVERSE_REFRESH_SEC = 300.0
DEFAULT_LIVE_REPORT_INTERVAL_SEC = 3600.0
DEFAULT_CANDIDATE_AUTO_PAUSE_MIN_OUTCOMES = 30
DEFAULT_CANDIDATE_AUTO_PAUSE_PROFIT_FACTOR = 0.8
DEFAULT_CANDIDATE_AUTO_PAUSE_MIN_AVG_RETURN_PCT = 0.0
DEFAULT_CANDIDATE_FOCUS_MIN_OUTCOMES = 80
DEFAULT_CANDIDATE_FOCUS_PROFIT_FACTOR = 1.3
DEFAULT_FACTOR_FACTORY_MAX_SNAPSHOTS = 5000
DEFAULT_FACTOR_FACTORY_MAX_OUTCOMES = 5000
DEFAULT_FACTOR_FACTORY_MIN_SAMPLES = 30
DEFAULT_FACTOR_FACTORY_PAIR_MIN_SAMPLES = 20
DEFAULT_GOVERNANCE_MAX_RECORDS = 5000
DEFAULT_MIN_24H_VOLATILITY_PCT = 1.0
DEFAULT_MIN_24H_QUOTE_VOLUME = 50_000_000.0
DEFAULT_MIN_1H_QUOTE_VOLUME = 2_000_000.0
DEFAULT_SYMBOL_1H_VOLUME_REFRESH_SEC = 60.0
DEFAULT_MEMORY_CLEANUP_INTERVAL_SEC = 3600.0
DEFAULT_EMERGENCY_DRAWDOWN_WINDOW_SEC = 900.0
DEFAULT_EMERGENCY_DRAWDOWN_THRESHOLD_PCT = 5.0
DEFAULT_EMERGENCY_DRAWDOWN_CHECK_SEC = 60.0
DEFAULT_EMERGENCY_TUNE_COOLDOWN_SEC = 900.0
DEFAULT_REAL_STOP_LOSS_STREAK_LENGTH = 3
DEFAULT_REAL_TRADE_OUTCOME_TAIL_LIMIT = 32
DEFAULT_HARD_KILL_DRAWDOWN_THRESHOLD_PCT = 3.0
DEFAULT_BTC_REGIME_DROP_THRESHOLD_PCT = 1.0
DEFAULT_BTC_REGIME_REFRESH_SEC = 15.0
DEFAULT_USER_DATA_OMS_RECONCILE_TIMEOUT_SEC = 2.0
DEFAULT_USER_DATA_OMS_KEEPALIVE_SEC = 30.0 * 60.0
DEFAULT_USER_DATA_OMS_RECONNECT_SEC = 5.0
DEFAULT_USER_DATA_OMS_WS_TIMEOUT_SEC = 45.0
DEFAULT_DEGEN_SHADOW_MIN_24H_QUOTE_VOLUME = 0.0
DEFAULT_DEGEN_SHADOW_EXTRA_SLIPPAGE_PCT = 1.0
DEFAULT_STATE_OFFSET_FLUSH_INTERVAL_SEC = 1.0
DEFAULT_STATE_OFFSET_FLUSH_LINE_BATCH = 25
DEFAULT_MAINNET_SHADOW_MIN_CLOSED_TRADES = 200
DEFAULT_MAINNET_SHADOW_MIN_WIN_RATE_PCT = 55.0
DEFAULT_MAINNET_SHADOW_MIN_PROFIT_FACTOR = 1.5
DEFAULT_MAINNET_SHADOW_MIN_SHARPE_RATIO = 1.5
DEFAULT_MAINNET_SHADOW_MAX_DRAWDOWN_PCT = 5.0
SHADOW_REASON_LEVERAGE_LIMIT_REJECTED = "leverage_limit_rejected_converted_to_shadow"
SHADOW_REASON_STRATEGY_PAUSED = "strategy_paused"
SHADOW_REASON_GLOBAL_RISK_LOCK = "global_risk_lock_active"
SHADOW_REASON_BTC_REGIME_BLOCKED = "btc_regime_blocked"
SHADOW_REASON_MAINNET_SHADOW_LOCKED = "mainnet_shadow_live_locked"
SHADOW_REASON_RESEARCH_POOL = "research_shadow_all_trigger"
SHADOW_REASON_RESEARCH_OBSERVATION = "research_shadow_observation_missing_direction"
SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE = "oi_unwind_reversal_confirmed_shadow_only"
SHADOW_REASON_STRATEGY_DISCOVERY_CANDIDATE = "strategy_discovery_shadow_only"
BTC_REGIME_MAJOR_SYMBOLS = {"BTCUSDT", "ETHUSDT"}
BRANCH_TYPE_STABLE_CORE = "stable_core"
BRANCH_TYPE_DEGEN_HIGH_YIELD = "degen_high_yield"
BRANCH_TYPE_RESEARCH_POOL = "research_pool"
BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE = "candidate_oi_unwind_reversal"
BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE = "candidate_strategy_discovery"
DEFAULT_SHADOW_BRANCH_SPECS = (
    "A:1.3:3.5",
    "B:1.5:5.0",
    "C:0.8:2.0",
)
DEFAULT_RESEARCH_SHADOW_SCALPING_PROFILES = (
    (0.35, 0.7),
    (0.5, 1.0),
    (0.8, 2.0),
)
DEFAULT_OI_UNWIND_REVERSAL_BRANCH_PROFILES = (
    (0.35, 0.7),
    (0.5, 1.0),
    (0.8, 2.0),
)
DEFAULT_OI_UNWIND_REVERSAL_TARGET_HORIZONS_SEC = (180, 300, 900)
DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC = (60, 180, 300, 900)
DEFAULT_STRATEGY_DISCOVERY_1H_TARGET_HORIZONS_SEC = (3600,)
DEFAULT_SHADOW_ROUND_TRIP_FEE_BPS = 8.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bridge collector event snapshots into guarded Binance Futures testnet execution."
    )
    parser.add_argument("--snapshots-file", type=Path, required=True)
    parser.add_argument("--state-file", type=Path, default=None)
    parser.add_argument("--env", default="testnet", choices=["prod", "testnet", "demo"])
    parser.add_argument(
        "--execution-mode",
        default=EXECUTION_MODE_TESTNET_LIVE,
        choices=[EXECUTION_MODE_TESTNET_LIVE, EXECUTION_MODE_MAINNET_SHADOW],
        help="TESTNET_LIVE preserves the existing testnet execution path. MAINNET_SHADOW reads prod data/rules and only writes shadow orders plus /order/test preflight.",
    )
    parser.add_argument("--mainnet-shadow-dir", type=Path, default=None)
    parser.add_argument("--allowed-playbooks", default=",".join(DEFAULT_ALLOWED_PLAYBOOKS))
    parser.add_argument("--quote-allocation", type=float, default=200.0)
    parser.add_argument("--leverage", type=int, default=10)
    parser.add_argument("--stop-loss-pct", type=float, default=1.3)
    parser.add_argument("--take-profit-pct", type=float, default=3.5)
    parser.add_argument("--symbol-cooldown-sec", type=float, default=180.0)
    parser.add_argument("--max-open-positions", type=int, default=30)
    parser.add_argument("--low-balance-threshold-usdt", type=float, default=2000.0)
    parser.add_argument("--low-balance-quote-allocation", type=float, default=200.0)
    parser.add_argument("--max-signal-age-sec", type=float, default=60.0)
    parser.add_argument("--poll-interval-sec", type=float, default=0.25)
    parser.add_argument("--live-universe-top", type=int, default=0)
    parser.add_argument("--live-universe-refresh-sec", type=float, default=DEFAULT_LIVE_UNIVERSE_REFRESH_SEC)
    parser.add_argument("--min-24h-quote-volume", type=float, default=DEFAULT_MIN_24H_QUOTE_VOLUME)
    parser.add_argument("--min-1h-quote-volume", type=float, default=DEFAULT_MIN_1H_QUOTE_VOLUME)
    parser.add_argument("--symbol-1h-volume-refresh-sec", type=float, default=DEFAULT_SYMBOL_1H_VOLUME_REFRESH_SEC)
    parser.add_argument("--blocked-symbols", default=",".join(DEFAULT_PERMANENT_BLOCKED_SYMBOLS))
    parser.add_argument("--min-24h-volatility-pct", type=float, default=DEFAULT_MIN_24H_VOLATILITY_PCT)
    parser.add_argument("--baseline-equity-usdt", type=float, default=5000.0)
    parser.add_argument("--live-report-file", type=Path, default=None)
    parser.add_argument("--total-sample-file", type=Path, default=None)
    parser.add_argument("--shadow-readiness-report-file", type=Path, default=None)
    parser.add_argument("--shadow-readiness-min-closed-trades", type=int, default=DEFAULT_MAINNET_SHADOW_MIN_CLOSED_TRADES)
    parser.add_argument("--shadow-readiness-min-win-rate-pct", type=float, default=DEFAULT_MAINNET_SHADOW_MIN_WIN_RATE_PCT)
    parser.add_argument("--shadow-readiness-min-profit-factor", type=float, default=DEFAULT_MAINNET_SHADOW_MIN_PROFIT_FACTOR)
    parser.add_argument("--shadow-readiness-min-sharpe-ratio", type=float, default=DEFAULT_MAINNET_SHADOW_MIN_SHARPE_RATIO)
    parser.add_argument("--shadow-readiness-max-drawdown-pct", type=float, default=DEFAULT_MAINNET_SHADOW_MAX_DRAWDOWN_PCT)
    parser.add_argument("--active-strategy-file", type=Path, default=None)
    parser.add_argument("--candidate-strategy-report-file", type=Path, default=None)
    parser.add_argument("--candidate-strategy-control-file", type=Path, default=None)
    parser.add_argument("--candidate-auto-pause-min-outcomes", type=int, default=DEFAULT_CANDIDATE_AUTO_PAUSE_MIN_OUTCOMES)
    parser.add_argument("--candidate-auto-pause-profit-factor", type=float, default=DEFAULT_CANDIDATE_AUTO_PAUSE_PROFIT_FACTOR)
    parser.add_argument(
        "--candidate-auto-pause-min-avg-return-pct",
        type=float,
        default=DEFAULT_CANDIDATE_AUTO_PAUSE_MIN_AVG_RETURN_PCT,
    )
    parser.add_argument("--candidate-focus-min-outcomes", type=int, default=DEFAULT_CANDIDATE_FOCUS_MIN_OUTCOMES)
    parser.add_argument("--candidate-focus-profit-factor", type=float, default=DEFAULT_CANDIDATE_FOCUS_PROFIT_FACTOR)
    parser.add_argument("--factor-factory-report-file", type=Path, default=None)
    parser.add_argument("--factor-factory-control-file", type=Path, default=None)
    parser.add_argument("--factor-factory-max-snapshots", type=int, default=DEFAULT_FACTOR_FACTORY_MAX_SNAPSHOTS)
    parser.add_argument("--factor-factory-max-outcomes", type=int, default=DEFAULT_FACTOR_FACTORY_MAX_OUTCOMES)
    parser.add_argument("--factor-factory-min-samples", type=int, default=DEFAULT_FACTOR_FACTORY_MIN_SAMPLES)
    parser.add_argument("--factor-factory-pair-min-samples", type=int, default=DEFAULT_FACTOR_FACTORY_PAIR_MIN_SAMPLES)
    parser.add_argument("--bias-audit-report-file", type=Path, default=None)
    parser.add_argument("--execution-realism-report-file", type=Path, default=None)
    parser.add_argument("--promotion-gate-report-file", type=Path, default=None)
    parser.add_argument(
        "--backtest-report-file",
        type=Path,
        default=None,
        help="Latest local historical backtest_report.json copied into this runtime dir. Read-only input for promotion gate.",
    )
    parser.add_argument("--governance-max-records", type=int, default=DEFAULT_GOVERNANCE_MAX_RECORDS)
    parser.add_argument("--emergency-tune-trigger-file", type=Path, default=None)
    parser.add_argument("--global-risk-lock-file", type=Path, default=None)
    parser.add_argument("--real-trade-outcomes-file", type=Path, default=None)
    parser.add_argument("--live-report-interval-sec", type=float, default=DEFAULT_LIVE_REPORT_INTERVAL_SEC)
    parser.add_argument("--memory-cleanup-interval-sec", type=float, default=DEFAULT_MEMORY_CLEANUP_INTERVAL_SEC)
    parser.add_argument("--emergency-drawdown-window-sec", type=float, default=DEFAULT_EMERGENCY_DRAWDOWN_WINDOW_SEC)
    parser.add_argument("--emergency-drawdown-threshold-pct", type=float, default=DEFAULT_EMERGENCY_DRAWDOWN_THRESHOLD_PCT)
    parser.add_argument("--emergency-drawdown-check-sec", type=float, default=DEFAULT_EMERGENCY_DRAWDOWN_CHECK_SEC)
    parser.add_argument("--emergency-tune-cooldown-sec", type=float, default=DEFAULT_EMERGENCY_TUNE_COOLDOWN_SEC)
    parser.add_argument("--real-stop-loss-streak-length", type=int, default=DEFAULT_REAL_STOP_LOSS_STREAK_LENGTH)
    parser.add_argument("--real-trade-outcome-tail-limit", type=int, default=DEFAULT_REAL_TRADE_OUTCOME_TAIL_LIMIT)
    parser.add_argument("--hard-kill-drawdown-threshold-pct", type=float, default=DEFAULT_HARD_KILL_DRAWDOWN_THRESHOLD_PCT)
    parser.add_argument("--btc-regime-drop-threshold-pct", type=float, default=DEFAULT_BTC_REGIME_DROP_THRESHOLD_PCT)
    parser.add_argument("--btc-regime-refresh-sec", type=float, default=DEFAULT_BTC_REGIME_REFRESH_SEC)
    parser.add_argument("--disable-user-data-oms", action="store_true")
    parser.add_argument("--user-data-oms-status-file", type=Path, default=None)
    parser.add_argument(
        "--user-data-oms-reconcile-timeout-sec",
        type=float,
        default=DEFAULT_USER_DATA_OMS_RECONCILE_TIMEOUT_SEC,
    )
    parser.add_argument("--user-data-oms-keepalive-sec", type=float, default=DEFAULT_USER_DATA_OMS_KEEPALIVE_SEC)
    parser.add_argument("--user-data-oms-reconnect-sec", type=float, default=DEFAULT_USER_DATA_OMS_RECONNECT_SEC)
    parser.add_argument("--degen-shadow-min-24h-quote-volume", type=float, default=DEFAULT_DEGEN_SHADOW_MIN_24H_QUOTE_VOLUME)
    parser.add_argument("--degen-shadow-extra-slippage-pct", type=float, default=DEFAULT_DEGEN_SHADOW_EXTRA_SLIPPAGE_PCT)
    parser.add_argument("--shadow-log-file", type=Path, default=None)
    parser.add_argument("--shadow-branch-specs", default=",".join(DEFAULT_SHADOW_BRANCH_SPECS))
    parser.add_argument("--shadow-round-trip-fee-bps", type=float, default=DEFAULT_SHADOW_ROUND_TRIP_FEE_BPS)
    parser.add_argument("--strategy-shadow-league-report-file", type=Path, default=None)
    parser.add_argument("--strategy-shadow-league-report-md-file", type=Path, default=None)
    parser.add_argument(
        "--disable-momentum-scalp-plus",
        action="store_true",
        help="Disable the 1m momentum scalp plus shadow-only experiment.",
    )
    parser.add_argument("--momentum-scalp-min-return-pct", type=float, default=MomentumScalpConfig.min_one_min_return_pct)
    parser.add_argument("--momentum-scalp-min-range-to-atr", type=float, default=MomentumScalpConfig.min_one_min_range_to_atr)
    parser.add_argument(
        "--momentum-scalp-min-volume-burst-ratio",
        type=float,
        default=MomentumScalpConfig.min_one_min_volume_burst_ratio,
    )
    parser.add_argument("--momentum-scalp-min-oi-change-pct", type=float, default=MomentumScalpConfig.min_one_min_oi_change_pct)
    parser.add_argument(
        "--momentum-scalp-min-five-min-oi-fallback-pct",
        type=float,
        default=MomentumScalpConfig.min_five_min_oi_fallback_pct,
    )
    parser.add_argument(
        "--disable-momentum-scalp-missing-one-min-oi-fallback",
        action="store_true",
        help="Require some OI source for 1m momentum scalp shadow signals instead of allowing missing OI as a tag.",
    )
    parser.add_argument(
        "--require-momentum-scalp-one-min-oi-hard-gate",
        action="store_true",
        help="Restore the strict 1m OI hard gate for diagnostics. Default remains shadow collection friendly.",
    )
    parser.add_argument("--momentum-scalp-max-spread-bps", type=float, default=MomentumScalpConfig.max_spread_bps)
    parser.add_argument("--momentum-scalp-max-slippage-bps", type=float, default=MomentumScalpConfig.max_slippage_bps)
    parser.add_argument("--momentum-scalp-symbol-cooldown-sec", type=int, default=MomentumScalpConfig.symbol_cooldown_sec)
    parser.add_argument(
        "--disable-discovery-candidates",
        action="store_true",
        help=(
            "Disable pre-filter shadow-only candidate writes, including strategy discovery "
            "and OI-unwind confirmation candidates. Use for clean baseline shadow runs."
        ),
    )
    parser.add_argument(
        "--candidate-parking-lot-only",
        action="store_true",
        help=(
            "Redirect discovery candidates into a parking-lot JSONL instead of writing "
            "new shadow signals. Existing shadow outcomes can still be reconciled."
        ),
    )
    parser.add_argument("--candidate-parking-lot-file", type=Path, default=None)
    parser.add_argument(
        "--research-shadow-all-triggers",
        action="store_true",
        help="Log every collector market_event_created into a research-only shadow pool without live or order/test.",
    )
    parser.add_argument(
        "--research-shadow-target-horizons-sec",
        default=",".join(str(item) for item in DEFAULT_RESEARCH_SHADOW_TARGET_HORIZONS_SEC),
        help="Comma-separated horizon seconds for research-only all-trigger shadow outcomes.",
    )
    parser.add_argument("--from-start", action="store_true")
    return parser.parse_args()


def build_momentum_scalp_config_from_args(args: argparse.Namespace) -> MomentumScalpConfig:
    return MomentumScalpConfig(
        min_one_min_return_pct=max(0.0, float(getattr(args, "momentum_scalp_min_return_pct", 0.5))),
        min_one_min_range_to_atr=max(0.0, float(getattr(args, "momentum_scalp_min_range_to_atr", 1.4))),
        min_one_min_volume_burst_ratio=max(
            0.0,
            float(getattr(args, "momentum_scalp_min_volume_burst_ratio", 2.0)),
        ),
        min_one_min_oi_change_pct=max(0.0, float(getattr(args, "momentum_scalp_min_oi_change_pct", 0.2))),
        allow_missing_one_min_oi_for_shadow=not bool(
            getattr(args, "disable_momentum_scalp_missing_one_min_oi_fallback", False)
        ),
        min_five_min_oi_fallback_pct=max(
            0.0,
            float(getattr(args, "momentum_scalp_min_five_min_oi_fallback_pct", 0.2)),
        ),
        require_one_min_oi_hard_gate=bool(getattr(args, "require_momentum_scalp_one_min_oi_hard_gate", False)),
        max_spread_bps=max(0.0, float(getattr(args, "momentum_scalp_max_spread_bps", 6.0))),
        max_slippage_bps=max(0.0, float(getattr(args, "momentum_scalp_max_slippage_bps", 8.0))),
        symbol_cooldown_sec=max(0, int(getattr(args, "momentum_scalp_symbol_cooldown_sec", 90))),
        default_round_trip_fee_bps=max(0.0, float(getattr(args, "shadow_round_trip_fee_bps", DEFAULT_SHADOW_ROUND_TRIP_FEE_BPS))),
    )


def build_candidate_parking_lot_payload(
    *,
    reason: str,
    record: dict[str, Any],
    playbook: str,
    side: str,
    branch_type: str,
    candidate_event_id: str,
    source_event_id: str,
    shadow_branch_specs: list[dict[str, Any]] | None = None,
    target_horizons_sec: Iterable[int] | None = None,
    candidate: dict[str, Any] | None = None,
    confirmation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "event": "candidate_discovery_parking_lot",
        "at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "parking_lot_only": True,
        "shadow_signal_written": False,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "event_id": candidate_event_id,
        "source_event_id": source_event_id,
        "symbol": record.get("symbol"),
        "playbook": playbook,
        "side": side,
        "branch_type": branch_type,
        "shadow_branch_specs": shadow_branch_specs or [],
        "target_horizons_sec": list(target_horizons_sec or []),
        "candidate": candidate or {},
        "confirmation": confirmation or {},
        "sample": record_sample(record),
        "enrichments": record.get("enrichments") if isinstance(record.get("enrichments"), dict) else {},
        "trigger_types": record.get("trigger_types") or record_sample(record).get("trigger_types"),
    }


def resolve_snapshots_file(raw_path: Path) -> Path:
    return raw_path / "event_snapshots.jsonl" if raw_path.is_dir() else raw_path


def resolve_mainnet_shadow_dir(snapshots_file: Path, explicit_mainnet_shadow_dir: Path | None) -> Path:
    if explicit_mainnet_shadow_dir is not None:
        return explicit_mainnet_shadow_dir
    return snapshots_file.parent / "mainnet_shadow"


def resolve_runtime_file(runtime_dir: Path | None, explicit_path: Path | None, default_name: str) -> Path | None:
    if explicit_path is not None:
        return explicit_path
    if runtime_dir is None:
        return None
    return runtime_dir / default_name


def resolve_state_file(snapshots_file: Path, explicit_state_file: Path | None) -> Path:
    if explicit_state_file is not None:
        return explicit_state_file
    return snapshots_file.with_suffix(".bridge-state.json")


def parse_allowed_playbooks(raw_value: str) -> set[str]:
    allowed = {token.strip() for token in str(raw_value or "").split(",") if token.strip()}
    return allowed or set(DEFAULT_ALLOWED_PLAYBOOKS)


def parse_symbol_set(raw_value: str) -> set[str]:
    return {token.strip().upper() for token in str(raw_value or "").split(",") if token.strip()}


def parse_shadow_branch_specs(raw_value: str) -> list[dict[str, Any]]:
    branches: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, raw_token in enumerate(str(raw_value or "").split(","), start=1):
        token = raw_token.strip()
        if not token:
            continue
        parts = [part.strip() for part in token.split(":")]
        if len(parts) != 3:
            raise ValueError(
                "Invalid shadow branch spec. Expected format NAME:STOP_LOSS_PCT:TAKE_PROFIT_PCT"
            )
        label, stop_loss_raw, take_profit_raw = parts
        branch_id = (label or f"branch_{index}").upper()
        if branch_id in seen_ids:
            raise ValueError(f"Duplicate shadow branch id: {branch_id}")
        seen_ids.add(branch_id)
        stop_loss_pct = max(0.01, float(stop_loss_raw))
        take_profit_pct = max(0.0, float(take_profit_raw))
        branches.append(
            {
                "branch_id": branch_id,
                "branch_label": label or branch_id,
                "stop_loss_pct": stop_loss_pct,
                "take_profit_pct": take_profit_pct,
            }
        )
    if not branches:
        return parse_shadow_branch_specs(",".join(DEFAULT_SHADOW_BRANCH_SPECS))
    return branches


def parse_positive_int_csv(raw_value: str, *, default: Iterable[int]) -> list[int]:
    parsed: list[int] = []
    for raw_item in str(raw_value or "").split(","):
        item = raw_item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError as exc:
            raise ValueError(f"Invalid positive integer value: {item}") from exc
        if value <= 0:
            raise ValueError(f"Expected positive integer value, got: {value}")
        if value not in parsed:
            parsed.append(value)
    if parsed:
        return parsed
    return list(default)


def normalize_shadow_target_horizons(value: Any, *, default: Iterable[int]) -> list[int]:
    if isinstance(value, str):
        try:
            return parse_positive_int_csv(value, default=default)
        except ValueError:
            return list(default)
    if isinstance(value, Iterable):
        parsed: list[int] = []
        for item in value:
            try:
                horizon_sec = int(item)
            except (TypeError, ValueError):
                continue
            if horizon_sec > 0 and horizon_sec not in parsed:
                parsed.append(horizon_sec)
        if parsed:
            return parsed
    return list(default)


def shadow_target_horizons(payload: dict[str, Any]) -> list[int]:
    return normalize_shadow_target_horizons(
        payload.get("shadow_target_horizons_sec"),
        default=DEFAULT_SHADOW_TARGET_HORIZONS_SEC,
    )


def build_eligible_perpetual_symbols(exchange_info_payload: object) -> set[str]:
    eligible_symbols: set[str] = set()
    if not isinstance(exchange_info_payload, dict):
        return eligible_symbols
    for item in exchange_info_payload.get("symbols", []):
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        if str(item.get("status") or "").upper() != "TRADING":
            continue
        contract_type = str(item.get("contractType") or "").upper()
        if contract_type and contract_type != "PERPETUAL":
            continue
        eligible_symbols.add(symbol)
    return eligible_symbols


def resolve_effective_quote_allocation_usdt(
    *,
    base_quote_allocation_usdt: float,
    available_balance_usdt: float | None,
    low_balance_threshold_usdt: float,
    low_balance_quote_allocation_usdt: float,
) -> float:
    base_allocation = max(0.0, float(base_quote_allocation_usdt or 0.0))
    if available_balance_usdt is None:
        return base_allocation
    if available_balance_usdt < max(0.0, float(low_balance_threshold_usdt or 0.0)):
        low_balance_allocation = max(0.0, float(low_balance_quote_allocation_usdt or 0.0))
        if low_balance_allocation > 0:
            return min(base_allocation, low_balance_allocation)
    return base_allocation


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_branch_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or BRANCH_TYPE_STABLE_CORE


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_event(name: str, **details: Any) -> None:
    payload = {"event": name, "at": now_iso(), **details}
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def best_effort_memory_cleanup() -> dict[str, Any]:
    gc_collected = 0
    malloc_trim_ok = False
    malloc_trim_error = None
    try:
        gc_collected = int(gc.collect())
    except Exception as exc:  # noqa: BLE001
        malloc_trim_error = str(exc)
    try:
        libc = ctypes.CDLL("libc.so.6")
        malloc_trim = getattr(libc, "malloc_trim", None)
        if malloc_trim is not None:
            malloc_trim.argtypes = [ctypes.c_size_t]
            malloc_trim.restype = ctypes.c_int
            malloc_trim_ok = bool(malloc_trim(0))
    except Exception as exc:  # noqa: BLE001
        if malloc_trim_error is None:
            malloc_trim_error = str(exc)
    return {
        "gc_collected": gc_collected,
        "malloc_trim_ok": malloc_trim_ok,
        "malloc_trim_error": malloc_trim_error,
    }


def record_sample(record: dict[str, Any]) -> dict[str, Any]:
    sample = record.get("sample")
    return sample if isinstance(sample, dict) else {}


def record_enrichments(record: dict[str, Any]) -> dict[str, Any]:
    enrichments = record.get("enrichments")
    return enrichments if isinstance(enrichments, dict) else {}


def record_factors(record: dict[str, Any]) -> dict[str, Any]:
    factors = record.get("factors")
    if isinstance(factors, dict) and factors:
        return factors
    computed = build_factor_vector(record)
    record["factors"] = computed
    return computed


def record_context_3m(record: dict[str, Any]) -> dict[str, Any]:
    context_3m = record.get("context_3m")
    if isinstance(context_3m, dict):
        return context_3m
    contexts = record.get("research_contexts")
    if isinstance(contexts, dict) and isinstance(contexts.get("3m"), dict):
        return contexts["3m"]
    return {}


def record_numeric_feature(record: dict[str, Any], name: str) -> float | None:
    sample = record_sample(record)
    context_3m = record_context_3m(record)
    enrichments = record_enrichments(record)
    factors = record_factors(record)
    for container in (sample, context_3m, enrichments, factors, record):
        if isinstance(container, dict) and name in container:
            value = safe_float(container.get(name))
            if value is not None:
                return value
    return None


def liquidity_bucket_from_quote_volume(quote_volume_24h: float | None) -> str:
    if quote_volume_24h is None or quote_volume_24h <= 0:
        return "unknown"
    if quote_volume_24h >= 1_000_000_000:
        return "mega"
    if quote_volume_24h >= 100_000_000:
        return "major"
    if quote_volume_24h >= 25_000_000:
        return "large"
    if quote_volume_24h >= 5_000_000:
        return "mid"
    return "thin"


def estimate_shadow_order_latency_ms(record: dict[str, Any]) -> int | None:
    sample = record_sample(record)
    signal_time_ms = (
        safe_float(record.get("signal_time_ms"))
        or safe_float(sample.get("signal_time_ms"))
        or safe_float(sample.get("anchor_close_time_ms"))
    )
    paper_order_time_ms = (
        safe_float(record.get("entry_time_ms"))
        or safe_float(record.get("observed_at_ms"))
        or safe_float(sample.get("observed_at_ms"))
    )
    if signal_time_ms is None or paper_order_time_ms is None:
        return None
    return max(0, int(round(paper_order_time_ms - signal_time_ms)))


def numeric_in_range(value: float | None, lower: float, upper: float, *, inclusive_upper: bool = False) -> bool:
    if value is None:
        return False
    if inclusive_upper:
        return lower <= value <= upper
    return lower <= value < upper


def trigger_candle_direction(record: dict[str, Any]) -> str:
    sample = record_sample(record)
    return str(sample.get("trigger_candle_direction") or sample.get("candle_direction") or "").strip().lower()


def has_expansion_trigger(record: dict[str, Any]) -> bool:
    sample = record_sample(record)
    trigger_types = {
        str(token)
        for token in (record.get("trigger_types") or sample.get("trigger_types") or [])
        if str(token)
    }
    return bool(trigger_types.intersection({"volume_burst", "range_expansion", "body_expansion"}))


def trigger_type_set(record: dict[str, Any]) -> set[str]:
    sample = record_sample(record)
    return {
        str(token).strip()
        for token in (record.get("trigger_types") or sample.get("trigger_types") or [])
        if str(token).strip()
    }


def side_from_trend_trigger_direction(direction: str | None) -> str | None:
    normalized = str(direction or "").strip().lower()
    if normalized == "up":
        return "BUY"
    if normalized == "down":
        return "SELL"
    return None


def side_from_reversal_trigger_direction(direction: str | None) -> str | None:
    normalized = str(direction or "").strip().lower()
    if normalized == "down":
        return "BUY"
    if normalized == "up":
        return "SELL"
    return None


def record_has_right_side_reversal_confirmation(record: dict[str, Any]) -> bool:
    sample = record_sample(record)
    trigger_direction = trigger_candle_direction(record)
    confirmation_direction = str(sample.get("confirmation_candle_direction") or "").strip().lower()
    if not sample_bool(sample, "reversal_confirmation_passed"):
        return False
    return (
        (trigger_direction == "down" and confirmation_direction == "up")
        or (trigger_direction == "up" and confirmation_direction == "down")
    )


def is_oi_unwind_reversal_base(record: dict[str, Any]) -> bool:
    sample = record_sample(record)
    enrichments = record_enrichments(record)
    bar_interval = str(record.get("bar_interval") or sample.get("bar_interval") or "").strip()
    oi_change_5m_pct = safe_float(enrichments.get("oi_change_5m_pct"))
    if bar_interval != "5m" or oi_change_5m_pct is None:
        return False
    return (
        -2.0 <= oi_change_5m_pct <= -0.5
        and trigger_candle_direction(record) in {"up", "down"}
        and has_expansion_trigger(record)
    )


def derive_preconfirmed_oi_unwind_reversal_side(record: dict[str, Any]) -> str | None:
    if not is_oi_unwind_reversal_base(record):
        return None
    if not record_has_right_side_reversal_confirmation(record):
        return None
    return side_from_reversal_trigger_direction(trigger_candle_direction(record))


def derive_playbook(record: dict[str, Any]) -> str:
    if derive_preconfirmed_oi_unwind_reversal_side(record) is not None:
        return PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED
    precomputed_playbook = str(record.get("playbook") or "").strip()
    if precomputed_playbook:
        return precomputed_playbook
    return event_context_label(record, split_by="playbook")


def derive_signal_side(record: dict[str, Any], *, playbook: str | None = None) -> str | None:
    precomputed_side = str(record.get("signal_side") or "").strip().upper()
    if precomputed_side in {"BUY", "SELL"}:
        return precomputed_side
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    candle_direction = str(sample.get("candle_direction") or "").lower()
    playbook = playbook or derive_playbook(record)
    if playbook == PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED:
        return derive_preconfirmed_oi_unwind_reversal_side(record) or side_from_reversal_trigger_direction(
            trigger_candle_direction(record)
        )
    if playbook == "liquidation_flush":
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


def sample_bool(sample: dict[str, Any], key: str) -> bool:
    value = sample.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def opposite_signal_side(side: str | None) -> str | None:
    normalized = str(side or "").strip().upper()
    if normalized == "BUY":
        return "SELL"
    if normalized == "SELL":
        return "BUY"
    return None


def candle_direction_from_kline(row: list[Any]) -> str:
    if not isinstance(row, list) or len(row) < 5:
        return ""
    open_price = safe_float(row[1])
    close_price = safe_float(row[4])
    if open_price is None or close_price is None:
        return ""
    if close_price > open_price:
        return "up"
    if close_price < open_price:
        return "down"
    return "flat"


def momentum_scalp_candle_from_kline(row: list[Any], *, radar: dict[str, Any] | None = None) -> dict[str, Any]:
    radar = radar or {}
    return {
        "open_time_ms": int(row[0]) if isinstance(row, list) and len(row) > 0 else 0,
        "open": safe_float(row[1]) if isinstance(row, list) and len(row) > 1 else None,
        "high": safe_float(row[2]) if isinstance(row, list) and len(row) > 2 else None,
        "low": safe_float(row[3]) if isinstance(row, list) and len(row) > 3 else None,
        "close": safe_float(row[4]) if isinstance(row, list) and len(row) > 4 else None,
        "close_time_ms": int(row[6]) if isinstance(row, list) and len(row) > 6 else 0,
        "volume_burst_ratio": safe_float(radar.get("one_min_volume_burst_ratio")) or 0.0,
        "oi_change_pct": safe_float(radar.get("one_min_oi_change_pct")) or 0.0,
    }


def momentum_scalp_candles_from_klines(
    rows: Iterable[list[Any]],
    *,
    radar: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 7:
            continue
        candles.append(momentum_scalp_candle_from_kline(row, radar=radar))
    return candles


async def resolve_oi_unwind_reversal_confirmation(
    *,
    futures: BinanceFuturesClient,
    record: dict[str, Any],
    max_confirmation_wait_ms: int = 4 * 60_000,
) -> dict[str, Any] | None:
    if not is_oi_unwind_reversal_base(record):
        return None
    preconfirmed_side = derive_preconfirmed_oi_unwind_reversal_side(record)
    trigger_direction = trigger_candle_direction(record)
    side = preconfirmed_side or side_from_reversal_trigger_direction(trigger_direction)
    if side not in {"BUY", "SELL"}:
        return None
    sample = record_sample(record)
    anchor_close_time_ms = int(safe_float(sample.get("anchor_close_time_ms")) or 0)
    if anchor_close_time_ms <= 0:
        return None
    if preconfirmed_side is not None:
        return {
            "side": preconfirmed_side,
            "trigger_direction": trigger_direction,
            "confirmation_source": "snapshot",
            "confirmation_candle_direction": str(sample.get("confirmation_candle_direction") or "").strip().lower(),
            "confirmation_close_time_ms": safe_float(sample.get("confirmation_anchor_close_time_ms")),
            "confirmation_wait_ms": 0,
            "entry_price": safe_float(sample.get("confirmation_close_price")) or safe_float(sample.get("price")),
        }
    now_timestamp_ms = int(time.time() * 1000)
    if now_timestamp_ms <= anchor_close_time_ms:
        return None
    end_time_ms = min(now_timestamp_ms, anchor_close_time_ms + max(60_000, int(max_confirmation_wait_ms)))
    try:
        candles = await futures.klines(
            str(record.get("symbol") or "").upper(),
            interval="1m",
            start_time_ms=anchor_close_time_ms + 1,
            end_time_ms=end_time_ms,
            limit=8,
        )
    except Exception as exc:  # noqa: BLE001
        emit_event(
            "signal_bridge_oi_unwind_confirmation_fetch_failed",
            event_id=record.get("event_id"),
            symbol=record.get("symbol"),
            error=str(exc),
        )
        return None
    for row in candles:
        if not isinstance(row, list) or len(row) < 7:
            continue
        close_time_ms = int(safe_float(row[6]) or 0)
        if close_time_ms <= anchor_close_time_ms or close_time_ms > now_timestamp_ms:
            continue
        confirmation_direction = candle_direction_from_kline(row)
        if (
            (trigger_direction == "down" and confirmation_direction == "up")
            or (trigger_direction == "up" and confirmation_direction == "down")
        ):
            return {
                "side": side,
                "trigger_direction": trigger_direction,
                "confirmation_source": "rest_1m_kline",
                "confirmation_candle_direction": confirmation_direction,
                "confirmation_close_time_ms": close_time_ms,
                "confirmation_wait_ms": max(0, close_time_ms - anchor_close_time_ms),
                "entry_price": safe_float(row[4]),
            }
        if confirmation_direction in {"up", "down"}:
            return None
    return None


def build_oi_unwind_reversal_branch_specs(side: str) -> list[dict[str, Any]]:
    normalized_side = str(side or "").upper()
    if normalized_side not in {"BUY", "SELL"}:
        return []
    branches: list[dict[str, Any]] = []
    for stop_loss_pct, take_profit_pct in DEFAULT_OI_UNWIND_REVERSAL_BRANCH_PROFILES:
        profile_token = research_shadow_profile_token(stop_loss_pct, take_profit_pct)
        branches.append(
            {
                "branch_id": f"OI_UNWIND_REV_{profile_token}",
                "branch_label": f"oi_unwind_reversal_{profile_token}",
                "direction_variant": "reversal",
                "side": normalized_side,
                "stop_loss_pct": float(stop_loss_pct),
                "take_profit_pct": float(take_profit_pct),
                "research_only": True,
            }
        )
    return branches


def build_strategy_discovery_branch_specs(playbook: str, side: str) -> list[dict[str, Any]]:
    normalized_side = str(side or "").upper()
    if normalized_side not in {"BUY", "SELL"}:
        return []
    normalized_playbook = str(playbook or "").strip()
    if normalized_playbook == PLAYBOOK_DISCOVERY_TREND_MILD_CONTINUATION:
        profiles = [("DISC_TREND_MILD_080_200", "discovery_trend_mild_080_200", "trend", 0.8, 2.0)]
    elif normalized_playbook == PLAYBOOK_DISCOVERY_OI_UNWIND_REVERSAL_SCALP:
        profiles = [
            ("DISC_OI_UNWIND_REV_050_100", "discovery_oi_unwind_reversal_050_100", "reversal", 0.5, 1.0),
            ("DISC_OI_UNWIND_REV_080_200", "discovery_oi_unwind_reversal_080_200", "reversal", 0.8, 2.0),
        ]
    elif normalized_playbook == PLAYBOOK_DISCOVERY_LOW_LIQ_DEPTH_REVERSAL:
        profiles = [("DISC_LOW_LIQ_DEPTH_REV_080_200", "discovery_low_liq_depth_reversal_080_200", "reversal", 0.8, 2.0)]
    elif normalized_playbook == PLAYBOOK_DISCOVERY_OI_UNWIND_5M_RANGE_REVERSAL:
        profiles = [
            ("DISC_OI_UNWIND_5M_RANGE_REV_050_100", "discovery_oi_unwind_5m_range_reversal_050_100", "reversal", 0.5, 1.0),
            ("DISC_OI_UNWIND_5M_RANGE_REV_080_200", "discovery_oi_unwind_5m_range_reversal_080_200", "reversal", 0.8, 2.0),
        ]
    elif normalized_playbook == PLAYBOOK_DISCOVERY_BIDWALL_OI_BUILD_CONTINUATION:
        profiles = [("DISC_BIDWALL_OI_BUILD_080_200", "discovery_bidwall_oi_build_080_200", "trend", 0.8, 2.0)]
    elif normalized_playbook == PLAYBOOK_DISCOVERY_FACTOR_TREND_ALIGNMENT:
        profiles = [
            ("DISC_FACTOR_TREND_050_150", "discovery_factor_trend_alignment_050_150", "trend", 0.5, 1.5),
            ("DISC_FACTOR_TREND_080_200", "discovery_factor_trend_alignment_080_200", "trend", 0.8, 2.0),
        ]
    elif normalized_playbook == PLAYBOOK_DISCOVERY_FACTOR_LIQUIDATION_REVERSION:
        profiles = [
            ("DISC_FACTOR_LIQ_REV_050_100", "discovery_factor_liquidation_reversion_050_100", "reversal", 0.5, 1.0),
            ("DISC_FACTOR_LIQ_REV_080_200", "discovery_factor_liquidation_reversion_080_200", "reversal", 0.8, 2.0),
        ]
    elif normalized_playbook == PLAYBOOK_SHADOW_OI_BUILD_QUALITY:
        profiles = [
            ("OI_BUILD_QUALITY_130_350", "shadow_oi_build_quality_130_350", "trend", 1.3, 3.5),
        ]
    elif normalized_playbook in {PLAYBOOK_SHADOW_OI_BUILD_BALANCED, PLAYBOOK_SHADOW_OI_BUILD_V2}:
        profiles = [
            ("OI_BUILD_BALANCED_130_350", "shadow_oi_build_balanced_130_350", "trend", 1.3, 3.5),
        ]
    elif normalized_playbook in {
        PLAYBOOK_DISCOVERY_HIST_BUY_1H_TREND_REVERSION_EXTREME,
        PLAYBOOK_DISCOVERY_HIST_BUY_1H_REVERSION_OI_UNWIND_LOW,
    }:
        profiles = [
            ("HIST_BUY_1H", "historical_buy_1h_forward_return", "historical_forward", 99.0, 0.0),
        ]
    else:
        return []
    return [
        {
            "branch_id": branch_id,
            "branch_label": branch_label,
            "direction_variant": direction_variant,
            "side": normalized_side,
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
            "research_only": True,
        }
        for branch_id, branch_label, direction_variant, stop_loss_pct, take_profit_pct in profiles
    ]


def derive_strategy_discovery_candidates(record: dict[str, Any]) -> list[dict[str, Any]]:
    trigger_direction = trigger_candle_direction(record)
    if trigger_direction not in {"up", "down"}:
        return []
    candidates: list[dict[str, Any]] = []
    sample = record_sample(record)
    bar_interval = str(record.get("bar_interval") or sample.get("bar_interval") or "").strip()
    trading_session = str(record.get("trading_session") or "").strip()
    volume_burst_ratio = record_numeric_feature(record, "volume_burst_ratio")
    range_to_atr = record_numeric_feature(record, "range_to_atr")
    oi_change_5m_pct = record_numeric_feature(record, "oi_change_5m_pct")
    oi_change_15m_pct = record_numeric_feature(record, "oi_change_15m_pct")
    trigger_score = record_numeric_feature(record, "trigger_score")
    btc_ret_5m_pct = record_numeric_feature(record, "btcusdt_ret_5m_pct")
    depth_imbalance = record_numeric_feature(record, "depth_imbalance")
    liquidation_long_usd_15m = record_numeric_feature(record, "liquidation_long_usd_15m")
    triggers = trigger_type_set(record)
    factors = record_factors(record)
    trend_score = safe_float(factors.get("trend_score"))
    mean_reversion_score = safe_float(factors.get("mean_reversion_score"))
    volatility_regime_score = safe_float(factors.get("volatility_regime_score"))
    liquidity_score = safe_float(factors.get("liquidity_score"))
    flow_score = safe_float(factors.get("flow_score"))
    oi_build_score = safe_float(factors.get("oi_build_score"))
    oi_unwind_score = safe_float(factors.get("oi_unwind_score"))
    liquidation_pressure_score = safe_float(factors.get("liquidation_pressure_score"))
    market_regime_score = safe_float(factors.get("market_regime_score"))
    trend_bucket = str(factors.get("trend_bucket") or "").strip()
    mean_reversion_bucket = str(factors.get("mean_reversion_bucket") or "").strip()
    oi_unwind_bucket = str(factors.get("oi_unwind_bucket") or "").strip()

    trend_side = side_from_trend_trigger_direction(trigger_direction)
    base_playbook = derive_playbook(record)
    base_side = derive_signal_side(record, playbook=base_playbook)
    if base_playbook == "oi_build_breakout" and base_side in {"BUY", "SELL"} and trigger_score is not None and trigger_score >= 60.0:
        if oi_change_5m_pct is not None and oi_change_5m_pct >= 1.0 and oi_change_15m_pct is not None and oi_change_15m_pct >= 5.0:
            candidates.append(
                {
                    "playbook": PLAYBOOK_SHADOW_OI_BUILD_QUALITY,
                    "side": base_side,
                    "candidate_source": "phoenix_shadow_strategy_a_20260430",
                    "candidate_conditions": {
                        "rule_id": "OI_BUILD_STRATEGY_A_QUALITY",
                        "base_playbook": "oi_build_breakout",
                        "oi_change_5m_pct": ">=1.0",
                        "oi_change_15m_pct": ">=5.0",
                        "trigger_score": ">=60",
                        "research_only": True,
                        "shadow_only": True,
                        "live_trading_enabled": False,
                        "promotion_allowed": False,
                    },
                    "shadow_branch_specs": build_strategy_discovery_branch_specs(
                        PLAYBOOK_SHADOW_OI_BUILD_QUALITY,
                        base_side,
                    ),
                    "target_horizons_sec": DEFAULT_SHADOW_TARGET_HORIZONS_SEC,
                    "research_only": True,
                    "shadow_only": True,
                    "live_unlock_eligible": False,
                    "readiness_eligible": False,
                    "live_trading_enabled": False,
                    "promotion_allowed": False,
                }
            )
        if oi_change_5m_pct is not None and oi_change_5m_pct >= 2.0:
            candidates.append(
                {
                    "playbook": PLAYBOOK_SHADOW_OI_BUILD_BALANCED,
                    "side": base_side,
                    "candidate_source": "phoenix_shadow_strategy_b_20260430",
                    "candidate_conditions": {
                        "rule_id": "OI_BUILD_STRATEGY_B_BALANCED",
                        "base_playbook": "oi_build_breakout",
                        "oi_change_5m_pct": ">=2.0",
                        "trigger_score": ">=60",
                        "research_only": True,
                        "shadow_only": True,
                        "live_trading_enabled": False,
                        "promotion_allowed": False,
                    },
                    "shadow_branch_specs": build_strategy_discovery_branch_specs(
                        PLAYBOOK_SHADOW_OI_BUILD_BALANCED,
                        base_side,
                    ),
                    "target_horizons_sec": DEFAULT_SHADOW_TARGET_HORIZONS_SEC,
                    "research_only": True,
                    "shadow_only": True,
                    "live_unlock_eligible": False,
                    "readiness_eligible": False,
                    "live_trading_enabled": False,
                    "promotion_allowed": False,
                }
            )
    if trend_bucket == "trend_long_extreme" and mean_reversion_bucket == "reversion_short_extreme":
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_HIST_BUY_1H_TREND_REVERSION_EXTREME,
                "side": "BUY",
                "candidate_source": "historical_research_cross_shard_20260430",
                "candidate_conditions": {
                    "rule_id": "ATTR_PAIR_SLICE_HIST_BUY_1H_3600_trend_bucket=trend_long_extreme__mean_reversion_bucket=reversion_short_extreme",
                    "branch_id": "HIST_BUY_1H",
                    "horizon_sec": 3600,
                    "trend_bucket": "trend_long_extreme",
                    "mean_reversion_bucket": "reversion_short_extreme",
                    "cross_shard_oos_samples": 381,
                    "cross_shard_oos_profit_factor": 1.564489,
                    "cross_shard_oos_avg_return_pct": 1.387353,
                    "research_only": True,
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_HIST_BUY_1H_TREND_REVERSION_EXTREME,
                    "BUY",
                ),
                "target_horizons_sec": DEFAULT_STRATEGY_DISCOVERY_1H_TARGET_HORIZONS_SEC,
                "research_only": True,
                "shadow_only": True,
                "live_unlock_eligible": False,
                "readiness_eligible": False,
            }
        )

    if mean_reversion_bucket == "reversion_short_extreme" and oi_unwind_bucket == "oi_unwind_low":
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_HIST_BUY_1H_REVERSION_OI_UNWIND_LOW,
                "side": "BUY",
                "candidate_source": "historical_research_cross_shard_20260430",
                "candidate_conditions": {
                    "rule_id": "ATTR_PAIR_SLICE_HIST_BUY_1H_3600_mean_reversion_bucket=reversion_short_extreme__oi_unwind_bucket=oi_unwind_low",
                    "branch_id": "HIST_BUY_1H",
                    "horizon_sec": 3600,
                    "mean_reversion_bucket": "reversion_short_extreme",
                    "oi_unwind_bucket": "oi_unwind_low",
                    "cross_shard_oos_samples": 382,
                    "cross_shard_oos_profit_factor": 1.247747,
                    "cross_shard_oos_avg_return_pct": 0.568444,
                    "research_only": True,
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_HIST_BUY_1H_REVERSION_OI_UNWIND_LOW,
                    "BUY",
                ),
                "target_horizons_sec": DEFAULT_STRATEGY_DISCOVERY_1H_TARGET_HORIZONS_SEC,
                "research_only": True,
                "shadow_only": True,
                "live_unlock_eligible": False,
                "readiness_eligible": False,
            }
        )

    if (
        trend_side
        and has_expansion_trigger(record)
        and numeric_in_range(volume_burst_ratio, 1.5, 2.0)
        and numeric_in_range(range_to_atr, 1.5, 2.0)
    ):
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_TREND_MILD_CONTINUATION,
                "side": trend_side,
                "candidate_source": "strategy_discovery_conservative_v1",
                "candidate_conditions": {
                    "volume_burst_ratio": "1.5..2",
                    "range_to_atr": "1.5..2",
                    "direction_variant": "trend",
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_TREND_MILD_CONTINUATION,
                    trend_side,
                ),
                "target_horizons_sec": DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC,
            }
        )

    factor_trend_side = None
    if trend_score is not None:
        if trend_score >= 0.55:
            factor_trend_side = "BUY"
        elif trend_score <= -0.55:
            factor_trend_side = "SELL"
    if (
        factor_trend_side
        and trend_side == factor_trend_side
        and has_expansion_trigger(record)
        and (volatility_regime_score or 0.0) >= 0.35
        and (liquidity_score or 0.0) >= 0.45
        and (oi_build_score or 0.0) >= 0.25
        and (
            (factor_trend_side == "BUY" and (market_regime_score is None or market_regime_score >= -0.25))
            or (factor_trend_side == "SELL" and (market_regime_score is None or market_regime_score <= 0.25))
        )
        and (
            flow_score is None
            or (factor_trend_side == "BUY" and flow_score >= -0.2)
            or (factor_trend_side == "SELL" and flow_score <= 0.2)
        )
    ):
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_FACTOR_TREND_ALIGNMENT,
                "side": factor_trend_side,
                "candidate_source": "hyper_alpha_arena_factor_lite_v1",
                "candidate_conditions": {
                    "factor_model": "HAA-inspired factor registry lite",
                    "trend_score": "abs>=0.55 and matches trigger direction",
                    "volatility_regime_score": ">=0.35",
                    "liquidity_score": ">=0.45",
                    "oi_build_score": ">=0.25",
                    "market_regime_score": "not strongly opposite",
                    "flow_score": "not strongly opposite",
                    "direction_variant": "trend",
                    "research_only": True,
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_FACTOR_TREND_ALIGNMENT,
                    factor_trend_side,
                ),
                "target_horizons_sec": DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC,
            }
        )

    reversal_side = side_from_reversal_trigger_direction(trigger_direction)
    if (
        reversal_side
        and numeric_in_range(oi_change_5m_pct, -2.0, -0.5, inclusive_upper=True)
        and numeric_in_range(btc_ret_5m_pct, -0.3, 0.3)
    ):
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_OI_UNWIND_REVERSAL_SCALP,
                "side": reversal_side,
                "candidate_source": "strategy_discovery_conservative_v1",
                "candidate_conditions": {
                    "oi_5m_regime": "oi_unwind_drop_-2..-0.5",
                    "btc_5m_regime": "btc_flat_-0.3..0.3",
                    "direction_variant": "reversal",
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_OI_UNWIND_REVERSAL_SCALP,
                    reversal_side,
                ),
                "target_horizons_sec": DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC,
            }
        )

    factor_reversal_side = None
    if mean_reversion_score is not None:
        if mean_reversion_score >= 0.45:
            factor_reversal_side = "BUY"
        elif mean_reversion_score <= -0.45:
            factor_reversal_side = "SELL"
    if (
        factor_reversal_side
        and reversal_side == factor_reversal_side
        and (oi_unwind_score or 0.0) >= 0.25
        and (liquidity_score or 0.0) >= 0.35
        and abs(liquidation_pressure_score or 0.0) >= 0.20
        and (
            (factor_reversal_side == "BUY" and (market_regime_score is None or market_regime_score >= -0.35))
            or (factor_reversal_side == "SELL" and (market_regime_score is None or market_regime_score <= 0.35))
        )
    ):
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_FACTOR_LIQUIDATION_REVERSION,
                "side": factor_reversal_side,
                "candidate_source": "hyper_alpha_arena_factor_lite_v1",
                "candidate_conditions": {
                    "factor_model": "HAA-inspired factor registry lite",
                    "mean_reversion_score": "abs>=0.45 and matches reversal direction",
                    "oi_unwind_score": ">=0.25",
                    "liquidity_score": ">=0.35",
                    "liquidation_pressure_score": "abs>=0.20",
                    "market_regime_score": "not strongly opposite",
                    "direction_variant": "reversal",
                    "research_only": True,
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_FACTOR_LIQUIDATION_REVERSION,
                    factor_reversal_side,
                ),
                "target_horizons_sec": DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC,
            }
        )

    if (
        reversal_side
        and bar_interval == "5m"
        and triggers == {"range_expansion"}
        and numeric_in_range(oi_change_5m_pct, -2.0, -0.5, inclusive_upper=True)
        and numeric_in_range(btc_ret_5m_pct, -0.3, 0.3)
    ):
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_OI_UNWIND_5M_RANGE_REVERSAL,
                "side": reversal_side,
                "candidate_source": "loss_driver_deep_dive_v1",
                "candidate_conditions": {
                    "bar_interval": "5m",
                    "trigger_signature": "range_expansion_only",
                    "oi_5m_regime": "oi_unwind_drop_-2..-0.5",
                    "btc_5m_regime": "btc_flat_-0.3..0.3",
                    "direction_variant": "reversal",
                    "research_evidence": "n=134 avg=+0.156% pf=1.79 on REVERSAL_SCALP_050_100 slice",
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_OI_UNWIND_5M_RANGE_REVERSAL,
                    reversal_side,
                ),
                "target_horizons_sec": DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC,
            }
        )

    if (
        reversal_side
        and {"range_expansion", "volume_burst"}.issubset(triggers)
        and numeric_in_range(depth_imbalance, 0.15, 0.35)
        and liquidation_long_usd_15m is not None
        and 0.0 <= liquidation_long_usd_15m < 1.0
    ):
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_LOW_LIQ_DEPTH_REVERSAL,
                "side": reversal_side,
                "candidate_source": "strategy_discovery_conservative_v1",
                "candidate_conditions": {
                    "depth_imbalance": "0.15..0.35",
                    "liquidation_long_usd_15m": "<1",
                    "trigger_signature": "range_expansion+volume_burst",
                    "direction_variant": "reversal",
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_LOW_LIQ_DEPTH_REVERSAL,
                    reversal_side,
                ),
                "target_horizons_sec": (300, 900),
            }
        )
    if (
        trend_side == "BUY"
        and bar_interval == "5m"
        and trading_session == "US"
        and {"body_expansion", "range_expansion", "volume_burst"}.issubset(triggers)
        and oi_change_5m_pct is not None
        and oi_change_5m_pct >= 0.5
        and depth_imbalance is not None
        and depth_imbalance >= 0.35
        and numeric_in_range(btc_ret_5m_pct, -0.3, 0.3)
    ):
        candidates.append(
            {
                "playbook": PLAYBOOK_DISCOVERY_BIDWALL_OI_BUILD_CONTINUATION,
                "side": trend_side,
                "candidate_source": "loss_driver_deep_dive_v1",
                "candidate_conditions": {
                    "bar_interval": "5m",
                    "trading_session": "US",
                    "trigger_signature": "body_expansion+range_expansion+volume_burst",
                    "oi_5m_regime": "oi_build_or_surge_>=0.5",
                    "depth_imbalance": "bid_extreme_>=0.35",
                    "btc_5m_regime": "btc_flat_-0.3..0.3",
                    "direction_variant": "trend_long",
                    "research_evidence": "n=66 avg=+0.479% pf=2.99 on oi_build_breakout bid-extreme slice",
                },
                "shadow_branch_specs": build_strategy_discovery_branch_specs(
                    PLAYBOOK_DISCOVERY_BIDWALL_OI_BUILD_CONTINUATION,
                    trend_side,
                ),
                "target_horizons_sec": DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC,
            }
        )
    return [
        candidate
        for candidate in candidates
        if candidate.get("side") in {"BUY", "SELL"} and candidate.get("shadow_branch_specs")
    ]


def build_research_shadow_event_id(record: dict[str, Any]) -> str:
    source_event_id = str(record.get("event_id") or "").strip()
    if source_event_id:
        return f"{source_event_id}::{BRANCH_TYPE_RESEARCH_POOL}"
    symbol = str(record.get("symbol") or "UNKNOWN").upper()
    observed_at_ms = str(record.get("observed_at_ms") or "").strip()
    if observed_at_ms:
        return f"{BRANCH_TYPE_RESEARCH_POOL}::{symbol}::{observed_at_ms}"
    return f"{BRANCH_TYPE_RESEARCH_POOL}::{symbol}::{int(time.time() * 1000)}"


def research_shadow_profile_token(stop_loss_pct: float, take_profit_pct: float) -> str:
    return f"{int(round(float(stop_loss_pct) * 100)):03d}_{int(round(float(take_profit_pct) * 100)):03d}"


def build_research_shadow_branch_specs(side: str | None) -> list[dict[str, Any]]:
    trend_side = str(side or "").strip().upper()
    reversal_side = opposite_signal_side(trend_side)
    if trend_side not in {"BUY", "SELL"} or reversal_side is None:
        return []
    branches: list[dict[str, Any]] = []
    for direction_variant, branch_side in (("trend", trend_side), ("reversal", reversal_side)):
        for stop_loss_pct, take_profit_pct in DEFAULT_RESEARCH_SHADOW_SCALPING_PROFILES:
            profile_token = research_shadow_profile_token(stop_loss_pct, take_profit_pct)
            branch_id = f"{direction_variant.upper()}_SCALP_{profile_token}"
            branches.append(
                {
                    "branch_id": branch_id,
                    "branch_label": f"{direction_variant}_scalp_{profile_token}",
                    "direction_variant": direction_variant,
                    "side": branch_side,
                    "stop_loss_pct": float(stop_loss_pct),
                    "take_profit_pct": float(take_profit_pct),
                    "research_only": True,
                }
            )
    return branches


def build_research_shadow_signal_payload(
    record: dict[str, Any],
    *,
    playbook: str | None = None,
    side: str | None = None,
    target_horizons_sec: Iterable[int] = DEFAULT_RESEARCH_SHADOW_TARGET_HORIZONS_SEC,
) -> dict[str, Any]:
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    factors = record_factors(record)
    resolved_playbook = str(playbook or derive_playbook(record) or "unknown").strip() or "unknown"
    resolved_side = str(side or derive_signal_side(record, playbook=resolved_playbook) or "").strip().upper()
    if resolved_side not in {"BUY", "SELL"}:
        resolved_side = ""
    source_event_id = str(record.get("event_id") or "").strip()
    shadow_branches = build_research_shadow_branch_specs(resolved_side)
    research_observation = not bool(shadow_branches)
    return {
        "event": "signal_bridge_shadow_logged",
        "logged_at": now_iso(),
        "event_id": build_research_shadow_event_id(record),
        "source_event_id": source_event_id,
        "horizon_event_id": source_event_id,
        "symbol": str(record.get("symbol") or sample.get("symbol") or "").upper(),
        "playbook": resolved_playbook,
        "side": resolved_side or None,
        "shadow_reason": SHADOW_REASON_RESEARCH_OBSERVATION if research_observation else SHADOW_REASON_RESEARCH_POOL,
        "shadow_target_horizons_sec": normalize_shadow_target_horizons(
            list(target_horizons_sec),
            default=DEFAULT_RESEARCH_SHADOW_TARGET_HORIZONS_SEC,
        ),
        "trading_session": record.get("trading_session"),
        "bar_interval": record.get("bar_interval"),
        "trigger_types": list(record.get("trigger_types") or sample.get("trigger_types") or []),
        "trigger_score": safe_float(record.get("trigger_score")),
        "sample_price": safe_float(sample.get("price")),
        "anchor_close_time_ms": int(sample.get("anchor_close_time_ms") or 0),
        "observed_at_ms": int(record.get("observed_at_ms") or 0),
        "quote_volume_24h": safe_float(sample.get("quote_volume_24h")),
        "price_change_24h_pct": safe_float(sample.get("price_change_24h_pct")),
        "factors": factors,
        "shadow_branches": [
            {
                **branch,
                "completed_horizons_sec": [],
            }
            for branch in shadow_branches
        ],
        "branch_type": BRANCH_TYPE_RESEARCH_POOL,
        "research_only": True,
        "research_observation": research_observation,
        "research_direction_status": "missing_direction" if research_observation else "derived",
        "research_direction_variants": ["trend", "reversal"] if shadow_branches else [],
        "shadow_only": True,
        "live_unlock_eligible": False,
        "readiness_eligible": False,
        "exchange_preflight_requested": False,
        "live_order_submission_blocked": True,
        "live_order_block_reason": "research_only",
        "extra_slippage_penalty_pct": 0.0,
    }


def trade_log_event_name(*, playbook: str, outcome: str) -> str:
    if playbook == "oi_build_breakout":
        return f"signal_bridge_trade_{outcome}"
    return f"signal_bridge_expanded_playbook_trade_{outcome}"


def classify_trade_error_as_shadow_reason(exc: Exception) -> str | None:
    if isinstance(exc, BinanceAPIError):
        if exc.code in {-4028, -2027, -2028, -2019}:
            return SHADOW_REASON_LEVERAGE_LIMIT_REJECTED
        error_text = " ".join(
            [
                str(exc),
                str(exc.payload.get("msg") or "") if isinstance(exc.payload, dict) else "",
            ]
        ).lower()
    else:
        error_text = str(exc).lower()
    if "leverage" in error_text:
        return SHADOW_REASON_LEVERAGE_LIMIT_REJECTED
    if "margin" in error_text and any(token in error_text for token in ("invalid", "insufficient", "reject", "limit")):
        return SHADOW_REASON_LEVERAGE_LIMIT_REJECTED
    return None


def clone_executor_with_settings(executor: PhoenixExecutor, settings: Any) -> PhoenixExecutor:
    cloned = PhoenixExecutor(futures_client=executor.futures_client, settings=settings)
    cloned._exchange_info_cache = executor._exchange_info_cache
    return cloned


def default_playbook_strategy(playbook: str, settings: Any) -> dict[str, Any]:
    return {
        "playbook": playbook,
        "status": "active",
        "sl": max(0.01, float(settings.initial_stop_loss_pct or 0.01)),
        "tp": max(0.0, float(settings.take_profit_pct or 0.0)),
    }


def load_active_strategy_config(
    path: Path,
    *,
    cached_payload: dict[str, Any] | None,
    cached_mtime_ns: int | None,
) -> tuple[dict[str, Any], int | None]:
    try:
        stat_result = path.stat()
    except OSError:
        return cached_payload or {}, cached_mtime_ns
    current_mtime_ns = int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000)))
    if cached_payload is not None and cached_mtime_ns == current_mtime_ns:
        return cached_payload, cached_mtime_ns
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        emit_event("signal_bridge_active_strategy_load_failed", strategy_file=str(path), error=str(exc))
        return cached_payload or {}, cached_mtime_ns
    if not isinstance(payload, dict):
        emit_event(
            "signal_bridge_active_strategy_load_failed",
            strategy_file=str(path),
            error="active strategy payload is not a JSON object",
        )
        return cached_payload or {}, cached_mtime_ns
    return payload, current_mtime_ns


def resolve_playbook_strategy_config(
    active_strategy_payload: dict[str, Any],
    *,
    playbook: str,
    settings: Any,
) -> dict[str, Any]:
    strategy = default_playbook_strategy(playbook, settings)
    raw_bucket = active_strategy_payload.get("playbooks") if isinstance(active_strategy_payload.get("playbooks"), dict) else active_strategy_payload
    if not isinstance(raw_bucket, dict):
        return strategy
    raw_playbook = raw_bucket.get(playbook)
    if not isinstance(raw_playbook, dict):
        return strategy
    raw_status = str(raw_playbook.get("status") or strategy["status"]).strip().lower()
    if raw_status not in {"active", "paused"}:
        raw_status = strategy["status"]
    sl_value = safe_float(raw_playbook.get("sl"))
    tp_value = safe_float(raw_playbook.get("tp"))
    strategy.update(
        {
            "status": raw_status,
            "sl": max(0.01, sl_value if sl_value is not None else float(strategy["sl"])),
            "tp": max(0.0, tp_value if tp_value is not None else float(strategy["tp"])),
            "sample_count": raw_playbook.get("sample_count"),
            "win_rate_pct": raw_playbook.get("win_rate_pct"),
            "avg_after_fee_return_pct": raw_playbook.get("avg_after_fee_return_pct"),
            "selected_branch_id": raw_playbook.get("selected_branch_id"),
            "generated_at": active_strategy_payload.get("generated_at"),
            "status_reason": raw_playbook.get("status_reason"),
        }
    )
    return strategy


def build_strategy_settings(base_settings: Any, playbook_strategy: dict[str, Any]) -> Any:
    sl_value = safe_float(playbook_strategy.get("sl"))
    tp_value = safe_float(playbook_strategy.get("tp"))
    return replace(
        base_settings,
        initial_stop_loss_pct=max(0.01, sl_value if sl_value is not None else float(base_settings.initial_stop_loss_pct)),
        take_profit_pct=max(0.0, tp_value if tp_value is not None else float(base_settings.take_profit_pct)),
    )


def merge_shadow_branch_specs(
    base_specs: list[dict[str, Any]],
    *,
    playbook_strategy: dict[str, Any],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_pairs: set[tuple[float, float]] = set()

    def _append(branch_id: str, branch_label: str, stop_loss_pct: float, take_profit_pct: float) -> None:
        key = (round(float(stop_loss_pct), 4), round(float(take_profit_pct), 4))
        if key in seen_pairs:
            return
        seen_pairs.add(key)
        merged.append(
            {
                "branch_id": branch_id,
                "branch_label": branch_label,
                "stop_loss_pct": max(0.01, float(stop_loss_pct)),
                "take_profit_pct": max(0.0, float(take_profit_pct)),
            }
        )

    _append(
        "AUTO",
        f"{playbook_strategy.get('playbook') or 'auto'}_auto",
        float(playbook_strategy.get("sl") or 0.01),
        float(playbook_strategy.get("tp") or 0.0),
    )
    for item in base_specs:
        _append(
            str(item.get("branch_id") or "LEGACY").upper(),
            str(item.get("branch_label") or item.get("branch_id") or "LEGACY"),
            float(item.get("stop_loss_pct") or 0.01),
            float(item.get("take_profit_pct") or 0.0),
        )
    return merged


def summarize_signal(record: dict[str, Any], *, playbook: str, side: str) -> dict[str, Any]:
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    enrichments = record.get("enrichments") if isinstance(record.get("enrichments"), dict) else {}
    factors = record_factors(record)
    return {
        "symbol": str(record.get("symbol") or sample.get("symbol") or "").upper(),
        "score": safe_float(record.get("trigger_score")),
        "setup": playbook,
        "side": side,
        "playbook": playbook,
        "trading_session": record.get("trading_session"),
        "bar_interval": record.get("bar_interval"),
        "trigger_types": list(record.get("trigger_types") or sample.get("trigger_types") or []),
        "price": safe_float(sample.get("price")),
        "quote_volume_24h": safe_float(sample.get("quote_volume_24h")),
        "price_change_24h_pct": safe_float(sample.get("price_change_24h_pct")),
        "funding_rate": safe_float(enrichments.get("funding_rate")),
        "oi_change_5m_pct": safe_float(enrichments.get("oi_change_5m_pct")),
        "oi_change_15m_pct": safe_float(enrichments.get("oi_change_15m_pct")),
        "depth_imbalance": safe_float(enrichments.get("depth_imbalance")),
        "factors": factors,
        "blocked_reasons": [],
        "score_breakdown": {
            "trigger_score": safe_float(record.get("trigger_score")) or 0.0,
            "trend_score": safe_float(factors.get("trend_score")) or 0.0,
            "mean_reversion_score": safe_float(factors.get("mean_reversion_score")) or 0.0,
            "flow_score": safe_float(factors.get("flow_score")) or 0.0,
            "liquidity_score": safe_float(factors.get("liquidity_score")) or 0.0,
        },
    }


def load_state_offset(state_file: Path, snapshots_file: Path, *, from_start: bool) -> int:
    if state_file.exists():
        try:
            payload = json.loads(state_file.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict) and str(payload.get("snapshots_file") or "") == str(snapshots_file):
            offset = int(payload.get("offset") or 0)
            if offset >= 0:
                return offset
    if from_start:
        return 0
    if snapshots_file.exists():
        try:
            return snapshots_file.stat().st_size
        except OSError:
            return 0
    return 0


def save_state_offset(state_file: Path, snapshots_file: Path, offset: int) -> None:
    payload = {
        "snapshots_file": str(snapshots_file),
        "offset": max(0, int(offset)),
        "updated_at": now_iso(),
    }
    state_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def resolve_shadow_log_file(snapshots_file: Path, explicit_shadow_log_file: Path | None) -> Path:
    if explicit_shadow_log_file is not None:
        return explicit_shadow_log_file
    return snapshots_file.with_name("signal_bridge_shadow_signals.jsonl")


def resolve_shadow_outcomes_file(shadow_log_file: Path) -> Path:
    return shadow_log_file.with_name("signal_bridge_shadow_outcomes.jsonl")


def resolve_shadow_state_file(state_file: Path) -> Path:
    return state_file.with_name(f"{state_file.stem}.shadow.json")


def resolve_horizon_labels_file(snapshots_file: Path) -> Path:
    return snapshots_file.with_name("event_horizon_labels.jsonl")


def resolve_live_report_file(snapshots_file: Path, explicit_live_report_file: Path | None) -> Path:
    if explicit_live_report_file is not None:
        return explicit_live_report_file
    return snapshots_file.with_name("Live_Performance_Report.txt")


def resolve_total_sample_file(snapshots_file: Path, explicit_total_sample_file: Path | None) -> Path:
    if explicit_total_sample_file is not None:
        return explicit_total_sample_file
    return snapshots_file.with_name("Total_Sample_V9_Final.json")


def resolve_active_strategy_file(snapshots_file: Path, explicit_active_strategy_file: Path | None) -> Path:
    if explicit_active_strategy_file is not None:
        return explicit_active_strategy_file
    return snapshots_file.with_name("active_strategy.json")


def resolve_candidate_strategy_report_file(snapshots_file: Path, explicit_report_file: Path | None) -> Path:
    if explicit_report_file is not None:
        return explicit_report_file
    return snapshots_file.with_name("candidate_strategy_report.json")


def resolve_candidate_strategy_control_file(snapshots_file: Path, explicit_control_file: Path | None) -> Path:
    if explicit_control_file is not None:
        return explicit_control_file
    return snapshots_file.with_name("candidate_strategy_control.json")


def resolve_strategy_shadow_league_report_file(snapshots_file: Path, explicit_report_file: Path | None) -> Path:
    if explicit_report_file is not None:
        return explicit_report_file
    return snapshots_file.with_name("strategy_shadow_league_report.json")


def resolve_strategy_shadow_league_report_md_file(snapshots_file: Path, explicit_report_file: Path | None) -> Path:
    if explicit_report_file is not None:
        return explicit_report_file
    return snapshots_file.with_name("strategy_shadow_league_report.md")


def resolve_factor_factory_report_file(snapshots_file: Path, explicit_report_file: Path | None) -> Path:
    if explicit_report_file is not None:
        return explicit_report_file
    return snapshots_file.with_name("factor_factory_report.json")


def resolve_factor_factory_control_file(snapshots_file: Path, explicit_control_file: Path | None) -> Path:
    if explicit_control_file is not None:
        return explicit_control_file
    return snapshots_file.with_name("factor_factory_control.json")


def resolve_bias_audit_report_file(snapshots_file: Path, explicit_report_file: Path | None) -> Path:
    if explicit_report_file is not None:
        return explicit_report_file
    return snapshots_file.with_name("bias_audit_report.json")


def resolve_execution_realism_report_file(snapshots_file: Path, explicit_report_file: Path | None) -> Path:
    if explicit_report_file is not None:
        return explicit_report_file
    return snapshots_file.with_name("execution_realism_report.json")


def resolve_promotion_gate_report_file(snapshots_file: Path, explicit_report_file: Path | None) -> Path:
    if explicit_report_file is not None:
        return explicit_report_file
    return snapshots_file.with_name("promotion_gate_report.json")


def resolve_backtest_report_file(runtime_dir: Path | None, explicit_report_file: Path | None) -> Path | None:
    if explicit_report_file is not None:
        return explicit_report_file
    if runtime_dir is None:
        return None
    return runtime_dir / "backtest_report.json"


def resolve_emergency_tune_trigger_file(snapshots_file: Path, explicit_trigger_file: Path | None) -> Path:
    if explicit_trigger_file is not None:
        return explicit_trigger_file
    return snapshots_file.with_name("emergency_tune.trigger")


def resolve_global_risk_lock_file(snapshots_file: Path, explicit_lock_file: Path | None) -> Path:
    if explicit_lock_file is not None:
        return explicit_lock_file
    return snapshots_file.with_name("global_risk_lock.json")


def resolve_user_data_oms_status_file(snapshots_file: Path, explicit_status_file: Path | None) -> Path:
    if explicit_status_file is not None:
        return explicit_status_file
    return snapshots_file.with_name("user_data_oms_state.json")


def resolve_real_trade_outcomes_file(explicit_real_trade_outcomes_file: Path | None) -> Path:
    if explicit_real_trade_outcomes_file is not None:
        return explicit_real_trade_outcomes_file
    return Path.home() / ".hermes" / "memories" / "phoenix_real_trade_outcomes.jsonl"


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


def read_json_object_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    return payload if isinstance(payload, dict) else None


class BridgeInstanceLockError(RuntimeError):
    pass


class BridgeInstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: Any | None = None

    def acquire(self) -> "BridgeInstanceLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            import fcntl  # type: ignore[import-not-found]

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise BridgeInstanceLockError(f"another signal bridge process already holds {self.path}") from exc
        except ModuleNotFoundError:
            # Windows fallback for local tests/dev; Linux prod uses fcntl above.
            fallback_path = self.path.with_suffix(self.path.suffix + ".owner")
            try:
                fd = os.open(str(fallback_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError as exc:
                handle.close()
                raise BridgeInstanceLockError(f"another signal bridge process already owns {fallback_path}") from exc
            os.close(fd)
        handle.seek(0)
        handle.truncate()
        handle.write(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "acquired_at": now_iso(),
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        handle.flush()
        self._handle = handle
        return self

    def release(self) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        with contextlib.suppress(Exception):
            try:
                import fcntl  # type: ignore[import-not-found]

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except ModuleNotFoundError:
                fallback_path = self.path.with_suffix(self.path.suffix + ".owner")
                with contextlib.suppress(OSError):
                    fallback_path.unlink()
        with contextlib.suppress(Exception):
            handle.close()

    def __enter__(self) -> "BridgeInstanceLock":
        return self.acquire()

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.release()


def acquire_bridge_instance_lock(path: Path) -> BridgeInstanceLock:
    return BridgeInstanceLock(path).acquire()


def read_global_risk_lock(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {
            "active": True,
            "reason": "global_risk_lock_unreadable",
            "lock_file": str(path),
            "error": str(exc),
        }
    if not isinstance(payload, dict):
        return {
            "active": True,
            "reason": "global_risk_lock_invalid_payload",
            "lock_file": str(path),
        }
    payload.setdefault("active", True)
    return payload


def is_global_risk_lock_active(payload: dict[str, Any] | None) -> bool:
    if payload is None:
        return False
    return bool(payload.get("active", True))


def write_global_risk_lock(path: Path, *, payload: dict[str, Any]) -> dict[str, Any]:
    lock_payload = {
        "active": True,
        "triggered_at": now_iso(),
        **payload,
    }
    atomic_write_json(path, lock_payload)
    return lock_payload


def iter_jsonl_records(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw_line = line.strip()
            if not raw_line:
                continue
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def tail_jsonl_records(path: Path, limit: int) -> list[dict[str, Any]]:
    if limit <= 0 or not path.exists():
        return []
    with path.open("rb") as handle:
        handle.seek(0, 2)
        end = handle.tell()
        data = b""
        while end > 0 and data.count(b"\n") <= limit:
            step = min(64 * 1024, end)
            end -= step
            handle.seek(end)
            data = handle.read(step) + data
    rows: list[dict[str, Any]] = []
    for raw_line in data.splitlines()[-limit:]:
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def build_shadow_instance_id(event_id: str, branch_id: str) -> str:
    return f"{event_id}::{branch_id}"


def normalize_shadow_branches(payload: dict[str, Any]) -> list[dict[str, Any]]:
    branches_payload = payload.get("shadow_branches")
    normalized_branches: list[dict[str, Any]] = []
    if isinstance(branches_payload, list):
        for index, item in enumerate(branches_payload, start=1):
            if not isinstance(item, dict):
                continue
            branch_id = str(item.get("branch_id") or f"BRANCH_{index}").upper()
            normalized_branch = {
                "branch_id": branch_id,
                "branch_label": str(item.get("branch_label") or branch_id),
                "stop_loss_pct": max(0.01, float(item.get("stop_loss_pct") or 0.01)),
                "take_profit_pct": max(0.0, float(item.get("take_profit_pct") or 0.0)),
                "completed_horizons_sec": sorted(
                    {
                        int(value)
                        for value in (item.get("completed_horizons_sec") or [])
                        if int(value) > 0
                    }
                ),
            }
            branch_side = str(item.get("side") or "").strip().upper()
            if branch_side in {"BUY", "SELL"}:
                normalized_branch["side"] = branch_side
            direction_variant = str(item.get("direction_variant") or "").strip().lower()
            if direction_variant:
                normalized_branch["direction_variant"] = direction_variant
            if bool(item.get("research_only")):
                normalized_branch["research_only"] = True
            for extra_key in (
                "strategy_family",
                "strategy_id",
                "entry_profile",
                "exit_profile",
                "take_profit_price_pct",
                "first_take_profit_price_pct",
                "close_fraction_at_first_tp",
                "move_stop_to_breakeven_after_first_tp",
                "trailing_callback_pct",
                "stop_loss_price_pct",
                "max_hold_minutes",
                "status",
                "shadow_only",
                "paper_record_only",
                "live_trading_enabled",
                "promotion_allowed",
            ):
                if extra_key in item:
                    normalized_branch[extra_key] = item.get(extra_key)
            normalized_branches.append(normalized_branch)
    if normalized_branches:
        return normalized_branches
    legacy_branch_id = str(payload.get("shadow_branch_id") or "LEGACY").upper()
    return [
        {
            "branch_id": legacy_branch_id,
            "branch_label": str(payload.get("shadow_branch_label") or legacy_branch_id),
            "stop_loss_pct": max(0.01, float(payload.get("branch_stop_loss_pct") or payload.get("stop_loss_pct") or 0.01)),
            "take_profit_pct": max(
                0.0,
                float(payload.get("branch_take_profit_pct") or payload.get("take_profit_pct") or 0.0),
            ),
            "completed_horizons_sec": sorted(
                {
                    int(value)
                    for value in (payload.get("completed_horizons_sec") or [])
                    if int(value) > 0
                }
            ),
        }
    ]


async def capture_account_equity_snapshot(futures: BinanceFuturesClient) -> dict[str, Any]:
    account_overview = await futures.account_overview()
    positions_payload = await futures.position_information_v3()
    open_positions = summarize_open_positions(positions_payload)
    total_unrealized_pnl_usdt = sum(
        safe_float(item.get("unrealized_pnl_usdt")) or 0.0
        for item in open_positions
    )
    wallet_balance_usdt = extract_wallet_balance(account_overview)
    available_balance_usdt = extract_available_balance(account_overview)
    equity_estimate_usdt = (
        (wallet_balance_usdt or 0.0) + total_unrealized_pnl_usdt
        if wallet_balance_usdt is not None
        else None
    )
    return {
        "captured_at": now_iso(),
        "available_balance_usdt": available_balance_usdt,
        "wallet_balance_usdt": wallet_balance_usdt,
        "total_unrealized_pnl_usdt": total_unrealized_pnl_usdt,
        "equity_estimate_usdt": equity_estimate_usdt,
        "open_position_count": len(open_positions),
    }


def prune_equity_history(
    history: deque[dict[str, Any]],
    *,
    now_monotonic: float,
    window_sec: float,
) -> None:
    while history:
        observed_monotonic = safe_float(history[0].get("observed_monotonic"))
        if observed_monotonic is None or (now_monotonic - observed_monotonic) <= max(1.0, float(window_sec)):
            break
        history.popleft()


def evaluate_emergency_drawdown(
    history: deque[dict[str, Any]],
    *,
    threshold_pct: float,
    window_sec: float,
) -> dict[str, Any] | None:
    valid_history = [
        item
        for item in history
        if safe_float(item.get("equity_estimate_usdt")) is not None
    ]
    if len(valid_history) < 2:
        return None
    current = valid_history[-1]
    peak = max(valid_history, key=lambda item: float(item.get("equity_estimate_usdt") or 0.0))
    peak_equity_usdt = safe_float(peak.get("equity_estimate_usdt"))
    current_equity_usdt = safe_float(current.get("equity_estimate_usdt"))
    if peak_equity_usdt is None or current_equity_usdt is None or peak_equity_usdt <= 0:
        return None
    if current.get("captured_at") == peak.get("captured_at"):
        return None
    drawdown_pct = max(0.0, ((peak_equity_usdt - current_equity_usdt) / peak_equity_usdt) * 100.0)
    if drawdown_pct < max(0.0, float(threshold_pct)):
        return None
    return {
        "type": "equity_drawdown",
        "window_sec": max(1.0, float(window_sec)),
        "threshold_pct": max(0.0, float(threshold_pct)),
        "drawdown_pct": round(drawdown_pct, 6),
        "peak_equity_usdt": peak_equity_usdt,
        "current_equity_usdt": current_equity_usdt,
        "peak_captured_at": peak.get("captured_at"),
        "current_captured_at": current.get("captured_at"),
        "available_balance_usdt": current.get("available_balance_usdt"),
        "wallet_balance_usdt": current.get("wallet_balance_usdt"),
        "total_unrealized_pnl_usdt": current.get("total_unrealized_pnl_usdt"),
        "open_position_count": int(current.get("open_position_count") or 0),
    }


def build_real_trade_outcome_key(record: dict[str, Any]) -> str | None:
    job_id = str(record.get("job_id") or "").strip()
    closed_at = str(record.get("closed_at") or record.get("at") or "").strip()
    if not job_id and not closed_at:
        return None
    return f"{job_id}:{closed_at}"


def load_recent_real_trade_outcomes(path: Path, *, tail_limit: int) -> list[dict[str, Any]]:
    rows = tail_jsonl_records(path, max(1, int(tail_limit)))
    filtered = [
        row
        for row in rows
        if str(row.get("event") or "") == "real_trade_outcome"
    ]
    filtered.sort(key=lambda item: str(item.get("closed_at") or item.get("at") or ""))
    return filtered


def evaluate_real_stop_loss_streak(
    records: list[dict[str, Any]],
    *,
    streak_length: int,
) -> dict[str, Any] | None:
    effective_records = [
        row
        for row in records
        if str(row.get("settlement_status") or "").lower() == "complete"
    ]
    if len(effective_records) < max(1, int(streak_length)):
        return None
    latest = effective_records[-max(1, int(streak_length)) :]
    if not all(bool(item.get("is_stop_loss_loss")) for item in latest):
        return None
    total_net_pnl_usdt = sum(safe_float(item.get("net_pnl_usdt")) or 0.0 for item in latest)
    latest_key = build_real_trade_outcome_key(latest[-1])
    return {
        "type": "real_stop_loss_streak",
        "streak_length": len(latest),
        "latest_outcome_key": latest_key,
        "total_net_pnl_usdt": round(total_net_pnl_usdt, 6),
        "outcomes": [
            {
                "job_id": item.get("job_id"),
                "symbol": item.get("symbol"),
                "side": item.get("side"),
                "playbook": item.get("playbook"),
                "closed_at": item.get("closed_at"),
                "net_pnl_usdt": safe_float(item.get("net_pnl_usdt")),
                "close_reason": item.get("close_reason"),
            }
            for item in latest
        ],
    }


def write_emergency_tune_trigger(
    trigger_file: Path,
    *,
    payload: dict[str, Any],
) -> None:
    atomic_write_json(
        trigger_file,
        {
            "triggered_at": now_iso(),
            **payload,
        },
    )


def shadow_branch_meta_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("branch_id") or "").upper(): item
        for item in normalize_shadow_branches(payload)
        if str(item.get("branch_id") or "").upper()
    }


def load_shadow_state(shadow_state_file: Path, *, horizon_labels_file: Path) -> tuple[int, dict[str, dict[str, Any]]]:
    if not shadow_state_file.exists():
        return (0, {})
    try:
        payload = json.loads(shadow_state_file.read_text(encoding="utf-8"))
    except Exception:
        return (0, {})
    if not isinstance(payload, dict):
        return (0, {})
    pending_payload = payload.get("pending_by_event_id")
    pending_by_event_id: dict[str, dict[str, Any]] = {}
    if isinstance(pending_payload, dict):
        for event_id, item in pending_payload.items():
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            if bool(normalized.get("research_observation")):
                continue
            normalized["shadow_branches"] = normalize_shadow_branches(normalized)
            normalized.pop("completed_horizons_sec", None)
            pending_by_event_id[str(event_id)] = normalized
    offset = int(payload.get("horizon_labels_offset") or 0)
    if horizon_labels_file.exists():
        try:
            current_size = horizon_labels_file.stat().st_size
            if current_size < offset:
                offset = 0
        except OSError:
            offset = 0
    return (max(0, offset), pending_by_event_id)


def save_shadow_state(
    shadow_state_file: Path,
    *,
    horizon_labels_file: Path,
    horizon_labels_offset: int,
    pending_by_event_id: dict[str, dict[str, Any]],
) -> None:
    payload = {
        "horizon_labels_file": str(horizon_labels_file),
        "horizon_labels_offset": max(0, int(horizon_labels_offset)),
        "pending_by_event_id": pending_by_event_id,
        "updated_at": now_iso(),
    }
    shadow_state_file.parent.mkdir(parents=True, exist_ok=True)
    shadow_state_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def find_optional_instruction(plan: list[Any], name: str) -> Any | None:
    for item in plan:
        if getattr(item, "name", None) == name:
            return item
    return None


def extract_wallet_balance(payload: object) -> float | None:
    if isinstance(payload, dict):
        for key in ("totalWalletBalance", "walletBalance", "balance", "totalMarginBalance"):
            value = payload.get(key)
            if value not in (None, ""):
                return safe_float(value)
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("asset") or "").upper() != "USDT":
                continue
            for key in ("walletBalance", "balance", "crossWalletBalance"):
                value = item.get(key)
                if value not in (None, ""):
                    return safe_float(value)
    return None


def summarize_open_positions(payload: object) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    if not isinstance(payload, list):
        return positions
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        quantity = safe_float(item.get("positionAmt"))
        if quantity is None or abs(quantity) <= 1e-12:
            continue
        positions.append(
            {
                "symbol": symbol,
                "side": "LONG" if quantity > 0 else "SHORT",
                "quantity": quantity,
                "entry_price": safe_float(item.get("entryPrice")),
                "mark_price": safe_float(item.get("markPrice")),
                "unrealized_pnl_usdt": safe_float(item.get("unRealizedProfit")) or 0.0,
            }
        )
    positions.sort(key=lambda row: safe_float(row.get("unrealized_pnl_usdt")) or 0.0, reverse=True)
    return positions


def build_live_tradable_universe(
    ticker_payload: object,
    exchange_info_payload: object,
    *,
    top_limit: int,
    min_quote_volume_24h: float = 0.0,
) -> tuple[set[str], dict[str, int], dict[str, float]]:
    eligible_symbols = build_eligible_perpetual_symbols(exchange_info_payload)
    ranked_rows: list[tuple[str, float]] = []
    if isinstance(ticker_payload, list):
        for item in ticker_payload:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").upper()
            quote_volume = safe_float(item.get("quoteVolume"))
            if not symbol or quote_volume is None or quote_volume <= 0:
                continue
            if eligible_symbols and symbol not in eligible_symbols:
                continue
            ranked_rows.append((symbol, quote_volume))
    ranked_rows.sort(key=lambda row: row[1], reverse=True)
    if top_limit > 0:
        ranked_rows = ranked_rows[:top_limit]
    else:
        threshold = max(0.0, float(min_quote_volume_24h or 0.0))
        if threshold > 0:
            ranked_rows = [row for row in ranked_rows if row[1] >= threshold]
    symbols = {symbol for symbol, _ in ranked_rows}
    rank_lookup = {symbol: index for index, (symbol, _) in enumerate(ranked_rows, start=1)}
    quote_volume_lookup = {symbol: quote_volume for symbol, quote_volume in ranked_rows}
    return symbols, rank_lookup, quote_volume_lookup


def compute_24h_volatility_pct(ticker_item: dict[str, Any]) -> float | None:
    high_price = safe_float(ticker_item.get("highPrice"))
    low_price = safe_float(ticker_item.get("lowPrice"))
    reference_price = safe_float(ticker_item.get("openPrice"))
    if reference_price is None or reference_price <= 0:
        reference_price = safe_float(ticker_item.get("lastPrice"))
    if (
        high_price is not None
        and low_price is not None
        and reference_price is not None
        and reference_price > 0
        and high_price >= low_price
    ):
        return max(0.0, ((high_price - low_price) / reference_price) * 100.0)
    price_change_pct = safe_float(ticker_item.get("priceChangePercent"))
    if price_change_pct is None:
        return None
    return abs(price_change_pct)


def build_live_market_stats_by_symbol(
    ticker_payload: object,
    *,
    selected_symbols: set[str] | None = None,
    eligible_symbols: set[str] | None = None,
) -> dict[str, dict[str, float | None]]:
    stats_by_symbol: dict[str, dict[str, float | None]] = {}
    if not isinstance(ticker_payload, list):
        return stats_by_symbol
    for item in ticker_payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if eligible_symbols and symbol not in eligible_symbols:
            continue
        if selected_symbols is not None and symbol not in selected_symbols:
            continue
        stats_by_symbol[symbol] = {
            "quote_volume_24h": safe_float(item.get("quoteVolume")),
            "price_change_24h_pct": safe_float(item.get("priceChangePercent")),
            "volatility_24h_pct": compute_24h_volatility_pct(item),
        }
    return stats_by_symbol


def evaluate_live_universe_gate(
    *,
    symbol: str,
    live_universe_top: int,
    min_quote_volume_24h: float,
    live_universe_ready: bool,
    tradable_symbols: set[str],
    tradable_rank_by_symbol: dict[str, int],
    tradable_quote_volume_by_symbol: dict[str, float],
) -> tuple[bool, str | None, dict[str, Any]]:
    top_limit = max(0, int(live_universe_top))
    min_24h_threshold = max(0.0, float(min_quote_volume_24h or 0.0))
    extra_fields = {
        "live_universe_top": top_limit,
        "live_universe_rank": tradable_rank_by_symbol.get(symbol),
        "live_universe_quote_volume_24h": tradable_quote_volume_by_symbol.get(symbol),
        "min_quote_volume_24h": min_24h_threshold,
    }
    if top_limit <= 0 and min_24h_threshold <= 0:
        return True, None, extra_fields
    if not live_universe_ready:
        if top_limit > 0:
            return False, f"live_top{top_limit}_universe_unavailable", extra_fields
        return False, "live_liquidity_universe_unavailable", extra_fields
    if not tradable_symbols:
        if top_limit > 0:
            return False, f"live_top{top_limit}_universe_empty", extra_fields
        return False, "live_liquidity_universe_empty", extra_fields
    if symbol not in tradable_symbols:
        if top_limit > 0:
            return False, f"outside_live_top{top_limit}_liquidity_universe", extra_fields
        return False, "below_min_24h_quote_volume", extra_fields
    return True, None, extra_fields


def evaluate_symbol_risk_gate(
    *,
    symbol: str,
    record: dict[str, Any],
    blocked_symbols: set[str],
    min_24h_volatility_pct: float,
    live_market_stats_by_symbol: dict[str, dict[str, float | None]],
) -> tuple[bool, str | None, dict[str, Any]]:
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    market_stats = live_market_stats_by_symbol.get(symbol) if isinstance(live_market_stats_by_symbol, dict) else None
    quote_volume_24h = safe_float((market_stats or {}).get("quote_volume_24h"))
    if quote_volume_24h is None:
        quote_volume_24h = safe_float(sample.get("quote_volume_24h"))
    price_change_24h_pct = safe_float((market_stats or {}).get("price_change_24h_pct"))
    if price_change_24h_pct is None:
        price_change_24h_pct = safe_float(sample.get("price_change_24h_pct"))
    volatility_24h_pct = safe_float((market_stats or {}).get("volatility_24h_pct"))
    threshold = max(0.0, float(min_24h_volatility_pct or 0.0))
    extra_fields = {
        "quote_volume_24h": quote_volume_24h,
        "price_change_24h_pct": price_change_24h_pct,
        "volatility_24h_pct": volatility_24h_pct,
        "min_24h_volatility_pct": threshold,
        "permanent_blocked_symbol": symbol if symbol in blocked_symbols else None,
    }
    if symbol in blocked_symbols:
        return False, "permanent_symbol_blacklist", extra_fields
    if threshold <= 0:
        return True, None, extra_fields
    if volatility_24h_pct is None:
        return False, "missing_24h_volatility", extra_fields
    if volatility_24h_pct < threshold:
        return False, "low_24h_volatility", extra_fields
    return True, None, extra_fields


def extract_btc_regime_return_5m_pct(record: dict[str, Any]) -> float | None:
    containers: list[dict[str, Any]] = []
    for key in ("enrichments", "sample"):
        value = record.get(key)
        if isinstance(value, dict):
            containers.append(value)
    containers.append(record)
    for container in containers:
        for key in ("btcusdt_ret_5m_pct", "btc_ret_5m_pct", "btc_5m_return_pct"):
            value = safe_float(container.get(key))
            if value is not None:
                return value
    return None


def compute_btc_regime_return_from_klines(candles: list[list[Any]]) -> float | None:
    usable = [row for row in candles if isinstance(row, list) and len(row) >= 5]
    if len(usable) < 2:
        return None
    open_price = safe_float(usable[0][1])
    close_price = safe_float(usable[-1][4])
    if open_price is None or close_price is None or open_price <= 0:
        return None
    return ((close_price - open_price) / open_price) * 100.0


async def resolve_btc_regime_return_5m_pct(
    *,
    futures: BinanceFuturesClient,
    record: dict[str, Any],
    cache: dict[str, Any],
    refresh_sec: float,
) -> tuple[float | None, str, str | None]:
    from_record = extract_btc_regime_return_5m_pct(record)
    if from_record is not None:
        cache["value"] = from_record
        cache["observed_monotonic"] = time.monotonic()
        return from_record, "record", None

    now_monotonic = time.monotonic()
    cached_value = safe_float(cache.get("value"))
    cached_at = safe_float(cache.get("observed_monotonic")) or 0.0
    if cached_value is not None and (now_monotonic - cached_at) < max(1.0, float(refresh_sec or 0.0)):
        return cached_value, "cache", None

    try:
        candles = await futures.klines("BTCUSDT", interval="1m", limit=6)
        value = compute_btc_regime_return_from_klines(candles)
    except Exception as exc:  # noqa: BLE001
        return cached_value, "cache_after_fetch_error" if cached_value is not None else "fetch_error", str(exc)

    if value is None:
        return cached_value, "cache_after_empty_fetch" if cached_value is not None else "missing", None
    cache["value"] = value
    cache["observed_monotonic"] = now_monotonic
    return value, "rest_klines", None


def evaluate_btc_regime_gate(
    *,
    symbol: str,
    side: str,
    btc_ret_5m_pct: float | None,
    drop_threshold_pct: float,
) -> tuple[bool, str | None, dict[str, Any]]:
    threshold = max(0.0, float(drop_threshold_pct or 0.0))
    normalized_symbol = str(symbol or "").upper()
    normalized_side = str(side or "").upper()
    extra_fields = {
        "btc_regime_ret_5m_pct": btc_ret_5m_pct,
        "btc_regime_drop_threshold_pct": threshold,
        "btc_regime_major_symbol": normalized_symbol in BTC_REGIME_MAJOR_SYMBOLS,
    }
    if threshold <= 0:
        return True, None, extra_fields
    if normalized_symbol in BTC_REGIME_MAJOR_SYMBOLS:
        return True, None, extra_fields
    if normalized_side != "BUY":
        return True, None, extra_fields
    if btc_ret_5m_pct is None:
        return False, "missing_btc_regime", extra_fields
    if btc_ret_5m_pct <= -threshold:
        return False, "btc_regime_alt_long_blocked", extra_fields
    return True, None, extra_fields


def extract_kline_quote_volume(row: Any) -> float | None:
    if not isinstance(row, list) or len(row) < 8:
        return None
    quote_volume = safe_float(row[7])
    if quote_volume is not None and quote_volume > 0:
        return quote_volume
    close_price = safe_float(row[4])
    base_volume = safe_float(row[5])
    if close_price is None or close_price <= 0 or base_volume is None or base_volume <= 0:
        return None
    return close_price * base_volume


async def resolve_symbol_quote_volume_1h(
    *,
    futures: BinanceFuturesClient,
    symbol: str,
    cache_by_symbol: dict[str, float],
    refreshed_at_by_symbol: dict[str, float],
    refresh_sec: float,
) -> float | None:
    now_monotonic = time.monotonic()
    cached_value = cache_by_symbol.get(symbol)
    last_refresh = float(refreshed_at_by_symbol.get(symbol) or 0.0)
    if cached_value is not None and (now_monotonic - last_refresh) < max(1.0, float(refresh_sec or 0.0)):
        return cached_value
    payload = await futures.klines(symbol, interval="5m", limit=12)
    quote_volume_1h = sum(
        value
        for value in (extract_kline_quote_volume(row) for row in payload if isinstance(row, list))
        if value is not None and value > 0
    )
    if quote_volume_1h <= 0:
        return None
    cache_by_symbol[symbol] = quote_volume_1h
    refreshed_at_by_symbol[symbol] = now_monotonic
    return quote_volume_1h


def evaluate_intraday_liquidity_gate(
    *,
    symbol: str,
    quote_volume_24h: float | None,
    min_quote_volume_24h: float,
    quote_volume_1h: float | None,
    min_quote_volume_1h: float,
) -> tuple[bool, str | None, dict[str, Any]]:
    min_24h_threshold = max(0.0, float(min_quote_volume_24h or 0.0))
    min_1h_threshold = max(0.0, float(min_quote_volume_1h or 0.0))
    extra_fields = {
        "quote_volume_24h": quote_volume_24h,
        "min_quote_volume_24h": min_24h_threshold,
        "quote_volume_1h": quote_volume_1h,
        "min_quote_volume_1h": min_1h_threshold,
    }
    if min_24h_threshold > 0 and (quote_volume_24h is None or quote_volume_24h < min_24h_threshold):
        return False, "below_min_24h_quote_volume", extra_fields
    if min_1h_threshold <= 0:
        return True, None, extra_fields
    if quote_volume_1h is None:
        return False, "missing_1h_quote_volume", extra_fields
    if quote_volume_1h < min_1h_threshold:
        return False, "below_min_1h_quote_volume", extra_fields
    return True, None, extra_fields


def evaluate_degen_shadow_gate(
    *,
    symbol: str,
    quote_volume_24h: float | None,
    min_quote_volume_24h: float,
) -> tuple[bool, str | None, dict[str, Any]]:
    threshold = max(0.0, float(min_quote_volume_24h or 0.0))
    extra_fields = {
        "branch_type": BRANCH_TYPE_DEGEN_HIGH_YIELD,
        "degen_shadow_min_quote_volume_24h": threshold,
        "degen_shadow_quote_volume_24h": quote_volume_24h,
        "degen_shadow_enabled": threshold > 0,
    }
    if threshold <= 0:
        return False, "degen_shadow_disabled", extra_fields
    if quote_volume_24h is None:
        return False, "missing_degen_24h_quote_volume", extra_fields
    if quote_volume_24h < threshold:
        return False, "below_degen_min_24h_quote_volume", extra_fields
    return True, None, extra_fields


def read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    return list(iter_jsonl_records(path))


def summarize_shadow_performance(
    signals: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    signal_rows = list(signals)
    reason_counter = Counter(str(item.get("shadow_reason") or "unknown") for item in signal_rows)
    playbook_counter = Counter(str(item.get("playbook") or "unknown") for item in signal_rows)
    branch_type_counter = Counter(normalize_branch_type(item.get("branch_type")) for item in signal_rows)
    completed_by_instance: Counter[str] = Counter()
    horizon_counts: Counter[int] = Counter()
    horizon_total_returns: dict[int, float] = {}
    horizon_total_pnl: dict[int, float] = {}
    branch_signal_counter: Counter[str] = Counter()
    branch_outcome_counter: Counter[str] = Counter()
    branch_exit_reason_counter: dict[str, Counter[str]] = {}
    branch_horizon_counts: dict[str, Counter[int]] = {}
    branch_horizon_total_returns: dict[str, dict[int, float]] = {}
    branch_horizon_total_pnl: dict[str, dict[int, float]] = {}
    branch_holding_total_sec: dict[str, float] = {}
    branch_holding_count: Counter[str] = Counter()
    branch_meta: dict[str, dict[str, Any]] = {}

    for item in signal_rows:
        for branch in normalize_shadow_branches(item):
            branch_id = str(branch.get("branch_id") or "LEGACY").upper()
            branch_signal_counter[branch_id] += 1
            branch_meta.setdefault(
                branch_id,
                {
                    "branch_label": branch.get("branch_label") or branch_id,
                    "stop_loss_pct": safe_float(branch.get("stop_loss_pct")),
                    "take_profit_pct": safe_float(branch.get("take_profit_pct")),
                },
            )

    outcome_count = 0
    for item in outcomes:
        outcome_count += 1
        event_id = str(item.get("event_id") or "")
        branch_id = str(item.get("shadow_branch_id") or "LEGACY").upper()
        shadow_instance_id = str(item.get("shadow_instance_id") or build_shadow_instance_id(event_id, branch_id))
        horizon_sec = int(item.get("horizon_sec") or 0)
        after_fee_return_pct = safe_float(item.get("after_fee_return_pct"))
        effective_quote_allocation_usdt = safe_float(item.get("effective_quote_allocation_usdt"))
        if shadow_instance_id:
            completed_by_instance[shadow_instance_id] += 1
        branch_outcome_counter[branch_id] += 1
        branch_meta.setdefault(
            branch_id,
            {
                "branch_label": item.get("shadow_branch_label") or branch_id,
                "stop_loss_pct": safe_float(item.get("branch_stop_loss_pct")),
                "take_profit_pct": safe_float(item.get("branch_take_profit_pct")),
            },
        )
        exit_reason = str(item.get("exit_reason") or "unknown")
        branch_exit_reason_counter.setdefault(branch_id, Counter())[exit_reason] += 1
        holding_duration_sec = safe_float(item.get("holding_duration_sec"))
        if holding_duration_sec is not None and holding_duration_sec > 0:
            branch_holding_total_sec[branch_id] = branch_holding_total_sec.get(branch_id, 0.0) + holding_duration_sec
            branch_holding_count[branch_id] += 1
        if horizon_sec <= 0 or after_fee_return_pct is None:
            continue
        horizon_counts[horizon_sec] += 1
        horizon_total_returns[horizon_sec] = horizon_total_returns.get(horizon_sec, 0.0) + after_fee_return_pct
        if effective_quote_allocation_usdt is not None:
            horizon_total_pnl[horizon_sec] = horizon_total_pnl.get(horizon_sec, 0.0) + (
                effective_quote_allocation_usdt * (after_fee_return_pct / 100.0)
            )
        branch_horizon_counts.setdefault(branch_id, Counter())[horizon_sec] += 1
        branch_horizon_total_returns.setdefault(branch_id, {})
        branch_horizon_total_returns[branch_id][horizon_sec] = (
            branch_horizon_total_returns[branch_id].get(horizon_sec, 0.0) + after_fee_return_pct
        )
        if effective_quote_allocation_usdt is not None:
            branch_horizon_total_pnl.setdefault(branch_id, {})
            branch_horizon_total_pnl[branch_id][horizon_sec] = (
                branch_horizon_total_pnl[branch_id].get(horizon_sec, 0.0)
                + (effective_quote_allocation_usdt * (after_fee_return_pct / 100.0))
            )

    pending_signals = 0
    pending_branches = 0
    for item in signal_rows:
        event_id = str(item.get("event_id") or "")
        branches = normalize_shadow_branches(item)
        target_horizons = shadow_target_horizons(item)
        branch_pending = False
        for branch in branches:
            branch_id = str(branch.get("branch_id") or "LEGACY").upper()
            if completed_by_instance.get(build_shadow_instance_id(event_id, branch_id), 0) < len(
                target_horizons
            ):
                pending_branches += 1
                branch_pending = True
        if branch_pending:
            pending_signals += 1

    horizon_summary: dict[int, dict[str, float | int]] = {}
    for horizon_sec in sorted(horizon_counts):
        count = horizon_counts[horizon_sec]
        avg_after_fee_return_pct = (horizon_total_returns.get(horizon_sec, 0.0) / count) if count else 0.0
        horizon_summary[horizon_sec] = {
            "count": count,
            "avg_after_fee_return_pct": avg_after_fee_return_pct,
            "estimated_total_pnl_usdt": horizon_total_pnl.get(horizon_sec, 0.0),
        }

    branch_summary: dict[str, dict[str, Any]] = {}
    for branch_id in sorted(branch_meta):
        branch_horizon_summary: dict[int, dict[str, float | int]] = {}
        for horizon_sec, count in sorted(branch_horizon_counts.get(branch_id, Counter()).items()):
            branch_horizon_summary[horizon_sec] = {
                "count": count,
                "avg_after_fee_return_pct": branch_horizon_total_returns.get(branch_id, {}).get(horizon_sec, 0.0) / count,
                "estimated_total_pnl_usdt": branch_horizon_total_pnl.get(branch_id, {}).get(horizon_sec, 0.0),
            }
        branch_summary[branch_id] = {
            "branch_label": branch_meta[branch_id].get("branch_label") or branch_id,
            "stop_loss_pct": branch_meta[branch_id].get("stop_loss_pct"),
            "take_profit_pct": branch_meta[branch_id].get("take_profit_pct"),
            "signal_count": int(branch_signal_counter.get(branch_id, 0)),
            "outcome_count": int(branch_outcome_counter.get(branch_id, 0)),
            "avg_holding_duration_sec": (
                branch_holding_total_sec.get(branch_id, 0.0) / branch_holding_count.get(branch_id, 0)
                if branch_holding_count.get(branch_id, 0)
                else None
            ),
            "exit_reason_counts": dict(sorted(branch_exit_reason_counter.get(branch_id, Counter()).items())),
            "horizon_summary": branch_horizon_summary,
        }

    return {
        "signal_count": len(signal_rows),
        "branch_signal_count": sum(branch_signal_counter.values()),
        "pending_signal_count": pending_signals,
        "pending_branch_count": pending_branches,
        "outcome_count": outcome_count,
        "reason_counts": dict(sorted(reason_counter.items())),
        "playbook_counts": dict(sorted(playbook_counter.items())),
        "branch_type_counts": dict(sorted(branch_type_counter.items())),
        "horizon_summary": horizon_summary,
        "branch_summary": branch_summary,
    }


def filter_shadow_rows_by_quote_allocation(
    rows: Iterable[dict[str, Any]],
    *,
    max_effective_quote_allocation_usdt: float,
) -> list[dict[str, Any]]:
    threshold = float(max_effective_quote_allocation_usdt)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        effective_quote_allocation = safe_float(row.get("effective_quote_allocation_usdt"))
        if effective_quote_allocation is None:
            continue
        if effective_quote_allocation <= threshold + 1e-9:
            filtered.append(row)
    return filtered


def summarize_shadow_drawdowns(outcomes: Iterable[dict[str, Any]]) -> dict[str, Any]:
    overall_drawdowns: list[float] = []
    branch_drawdowns: dict[str, list[float]] = {}
    for item in outcomes:
        max_drawdown_pct = safe_float(item.get("max_drawdown_pct"))
        if max_drawdown_pct is None:
            continue
        overall_drawdowns.append(max_drawdown_pct)
        branch_id = str(item.get("shadow_branch_id") or "LEGACY").upper()
        branch_drawdowns.setdefault(branch_id, []).append(max_drawdown_pct)

    def _stats(values: list[float]) -> dict[str, Any]:
        if not values:
            return {
                "count": 0,
                "avg_max_drawdown_pct": None,
                "worst_max_drawdown_pct": None,
                "least_negative_max_drawdown_pct": None,
            }
        return {
            "count": len(values),
            "avg_max_drawdown_pct": sum(values) / len(values),
            "worst_max_drawdown_pct": min(values),
            "least_negative_max_drawdown_pct": max(values),
        }

    return {
        "overall": _stats(overall_drawdowns),
        "by_branch": {
            branch_id: _stats(values)
            for branch_id, values in sorted(branch_drawdowns.items())
        },
    }


def filter_shadow_rows_by_branch_type(
    rows: Iterable[dict[str, Any]],
    *,
    branch_type: str,
) -> list[dict[str, Any]]:
    target = normalize_branch_type(branch_type)
    return [
        dict(row)
        for row in rows
        if normalize_branch_type(row.get("branch_type")) == target
    ]


def filter_shadow_rows_by_shadow_branch_id(
    rows: Iterable[dict[str, Any]],
    *,
    shadow_branch_id: str,
) -> list[dict[str, Any]]:
    target = str(shadow_branch_id or "").upper()
    return [
        dict(row)
        for row in rows
        if str(row.get("shadow_branch_id") or "").upper() == target
    ]


def is_research_shadow_row(row: dict[str, Any]) -> bool:
    return bool(row.get("research_only")) or normalize_branch_type(row.get("branch_type")) == BRANCH_TYPE_RESEARCH_POOL


def compute_sharpe_ratio(returns_pct: list[float]) -> float | None:
    if len(returns_pct) < 2:
        return None
    mean_return = sum(returns_pct) / len(returns_pct)
    variance = sum((value - mean_return) ** 2 for value in returns_pct) / (len(returns_pct) - 1)
    if variance <= 0:
        return None
    std_dev = variance ** 0.5
    if std_dev <= 1e-12:
        return None
    return mean_return / std_dev


def summarize_exchange_preflight_results(results: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(results)
    failed = [item for item in rows if item.get("ok") is False]
    preview_only = [item for item in rows if item.get("skipped") or item.get("ok") is None]
    order_test_count = sum(1 for item in rows if item.get("ok") is True)
    return {
        "ok": len(failed) == 0,
        "result_count": len(rows),
        "order_test_count": order_test_count,
        "preview_only_count": len(preview_only),
        "failed_count": len(failed),
        "failed_names": [item.get("name") for item in failed],
    }


def latest_shadow_outcomes_by_instance(outcomes: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_by_instance: dict[str, dict[str, Any]] = {}
    latest_horizon_by_instance: dict[str, int] = {}
    for item in outcomes:
        instance_id = str(item.get("shadow_instance_id") or "")
        if not instance_id:
            event_id = str(item.get("event_id") or "")
            branch_id = str(item.get("shadow_branch_id") or "LEGACY").upper()
            instance_id = build_shadow_instance_id(event_id, branch_id) if event_id else ""
        if not instance_id:
            continue
        horizon_sec = int(item.get("horizon_sec") or 0)
        if instance_id not in latest_by_instance or horizon_sec >= latest_horizon_by_instance.get(instance_id, -1):
            latest_by_instance[instance_id] = dict(item)
            latest_horizon_by_instance[instance_id] = horizon_sec
    return [latest_by_instance[key] for key in sorted(latest_by_instance)]


def is_candidate_strategy_row(row: dict[str, Any]) -> bool:
    return normalize_branch_type(row.get("branch_type")) in {
        BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
        BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
    }


def shadow_net_return_pct(row: dict[str, Any]) -> float | None:
    for key in ("after_fee_and_slippage_return_pct", "after_fee_return_pct", "close_return_pct"):
        value = safe_float(row.get(key))
        if value is not None:
            return value
    return None


def summarize_return_rows(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    values = [value for row in rows if (value := shadow_net_return_pct(row)) is not None]
    if not values:
        return {
            "outcome_count": 0,
            "avg_return_pct": None,
            "total_return_pct": 0.0,
            "win_rate_pct": None,
            "profit_factor": None,
            "sharpe_ratio": None,
            "win_count": 0,
            "loss_count": 0,
        }
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999999.0 if gross_profit > 0 else 0.0)
    return {
        "outcome_count": len(values),
        "avg_return_pct": sum(values) / len(values),
        "total_return_pct": sum(values),
        "win_rate_pct": (len(wins) / len(values)) * 100.0,
        "profit_factor": profit_factor,
        "sharpe_ratio": compute_sharpe_ratio(values),
        "win_count": len(wins),
        "loss_count": len(losses),
    }


def candidate_playbook_status(control_payload: dict[str, Any] | None, playbook: str) -> str:
    if not isinstance(control_payload, dict):
        return "active"
    playbooks = control_payload.get("playbooks")
    if not isinstance(playbooks, dict):
        return "active"
    row = playbooks.get(playbook)
    if not isinstance(row, dict):
        return "active"
    status = str(row.get("status") or "active").strip().lower()
    return status if status in {"active", "paused", "focus"} else "active"


def candidate_playbook_is_paused(control_payload: dict[str, Any] | None, playbook: str) -> bool:
    return candidate_playbook_status(control_payload, playbook) == "paused"


def build_candidate_strategy_report(
    *,
    generated_at: str,
    shadow_signals: list[dict[str, Any]],
    shadow_outcomes: list[dict[str, Any]],
    control_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_signals = [row for row in shadow_signals if is_candidate_strategy_row(row)]
    candidate_outcomes = [row for row in shadow_outcomes if is_candidate_strategy_row(row)]
    latest_outcomes = latest_shadow_outcomes_by_instance(candidate_outcomes)
    signal_counts = Counter(str(row.get("playbook") or "unknown") for row in candidate_signals)
    exit_counts_by_playbook: dict[str, Counter[str]] = {}
    branch_counts_by_playbook: dict[str, Counter[str]] = {}
    rows_by_playbook: dict[str, list[dict[str, Any]]] = {}
    rows_by_branch: dict[str, list[dict[str, Any]]] = {}
    for row in latest_outcomes:
        playbook = str(row.get("playbook") or "unknown")
        branch_id = str(row.get("shadow_branch_id") or "LEGACY").upper()
        rows_by_playbook.setdefault(playbook, []).append(row)
        rows_by_branch.setdefault(f"{playbook}|{branch_id}", []).append(row)
        exit_counts_by_playbook.setdefault(playbook, Counter())[str(row.get("exit_reason") or "unknown")] += 1
        branch_counts_by_playbook.setdefault(playbook, Counter())[branch_id] += 1

    playbooks: dict[str, dict[str, Any]] = {}
    for playbook in sorted(set(signal_counts) | set(rows_by_playbook)):
        summary = summarize_return_rows(rows_by_playbook.get(playbook, []))
        playbooks[playbook] = {
            "status": candidate_playbook_status(control_payload, playbook),
            "signal_count": int(signal_counts.get(playbook, 0)),
            **summary,
            "exit_reason_counts": dict(sorted(exit_counts_by_playbook.get(playbook, Counter()).items())),
            "branch_outcome_counts": dict(sorted(branch_counts_by_playbook.get(playbook, Counter()).items())),
        }

    branches: dict[str, dict[str, Any]] = {}
    for key, rows in sorted(rows_by_branch.items()):
        playbook, branch_id = key.split("|", 1)
        branches[key] = {
            "playbook": playbook,
            "shadow_branch_id": branch_id,
            **summarize_return_rows(rows),
        }

    return {
        "generated_at": generated_at,
        "candidate_signal_count": len(candidate_signals),
        "candidate_outcome_count": len(candidate_outcomes),
        "latest_candidate_instance_count": len(latest_outcomes),
        "playbooks": playbooks,
        "branches": branches,
    }


def update_candidate_strategy_control(
    *,
    generated_at: str,
    report: dict[str, Any],
    existing_payload: dict[str, Any] | None = None,
    min_outcomes: int = DEFAULT_CANDIDATE_AUTO_PAUSE_MIN_OUTCOMES,
    pause_profit_factor: float = DEFAULT_CANDIDATE_AUTO_PAUSE_PROFIT_FACTOR,
    pause_min_avg_return_pct: float = DEFAULT_CANDIDATE_AUTO_PAUSE_MIN_AVG_RETURN_PCT,
    focus_min_outcomes: int = DEFAULT_CANDIDATE_FOCUS_MIN_OUTCOMES,
    focus_profit_factor: float = DEFAULT_CANDIDATE_FOCUS_PROFIT_FACTOR,
) -> dict[str, Any]:
    existing_playbooks = (
        existing_payload.get("playbooks")
        if isinstance(existing_payload, dict) and isinstance(existing_payload.get("playbooks"), dict)
        else {}
    )
    next_playbooks: dict[str, dict[str, Any]] = {}
    report_playbooks = report.get("playbooks") if isinstance(report.get("playbooks"), dict) else {}
    for playbook, row in sorted(report_playbooks.items()):
        if not isinstance(row, dict):
            continue
        existing_row = existing_playbooks.get(playbook) if isinstance(existing_playbooks, dict) else None
        existing_status = str((existing_row or {}).get("status") or "active").strip().lower()
        outcome_count = int(row.get("outcome_count") or 0)
        avg_return_pct = safe_float(row.get("avg_return_pct"))
        profit_factor = safe_float(row.get("profit_factor"))
        status = existing_status if existing_status in {"active", "paused", "focus"} else "active"
        status_reason = (existing_row or {}).get("status_reason") if isinstance(existing_row, dict) else None
        if outcome_count >= max(1, int(focus_min_outcomes)) and profit_factor is not None and avg_return_pct is not None:
            if profit_factor >= float(focus_profit_factor) and avg_return_pct >= 0:
                status = "focus"
                status_reason = "forward_shadow_focus_threshold_met"
        if outcome_count >= max(1, int(min_outcomes)):
            if (
                avg_return_pct is not None
                and profit_factor is not None
                and (avg_return_pct < float(pause_min_avg_return_pct) or profit_factor < float(pause_profit_factor))
            ):
                status = "paused"
                status_reason = "forward_shadow_auto_pause_negative_expectancy"
        next_playbooks[playbook] = {
            "status": status,
            "status_reason": status_reason,
            "outcome_count": outcome_count,
            "signal_count": int(row.get("signal_count") or 0),
            "avg_return_pct": avg_return_pct,
            "profit_factor": profit_factor,
            "win_rate_pct": safe_float(row.get("win_rate_pct")),
            "updated_at": generated_at,
        }
    return {
        "generated_at": generated_at,
        "policy": {
            "min_outcomes": max(1, int(min_outcomes)),
            "pause_profit_factor_below": float(pause_profit_factor),
            "pause_avg_return_pct_below": float(pause_min_avg_return_pct),
            "focus_min_outcomes": max(1, int(focus_min_outcomes)),
            "focus_profit_factor_at_least": float(focus_profit_factor),
        },
        "playbooks": next_playbooks,
    }


def compute_equity_curve_max_drawdown_pct(pnls_usdt: list[float], *, baseline_equity_usdt: float) -> float:
    baseline = max(1.0, float(baseline_equity_usdt or 0.0))
    equity = baseline
    peak = baseline
    max_drawdown = 0.0
    for pnl in pnls_usdt:
        equity += float(pnl)
        peak = max(peak, equity)
        if peak <= 0:
            continue
        max_drawdown = max(max_drawdown, ((peak - equity) / peak) * 100.0)
    return max_drawdown


def build_shadow_readiness_report(
    outcomes: Iterable[dict[str, Any]],
    *,
    generated_at: str,
    baseline_equity_usdt: float,
    min_closed_trades: int = DEFAULT_MAINNET_SHADOW_MIN_CLOSED_TRADES,
    min_win_rate_pct: float = DEFAULT_MAINNET_SHADOW_MIN_WIN_RATE_PCT,
    min_profit_factor: float = DEFAULT_MAINNET_SHADOW_MIN_PROFIT_FACTOR,
    min_sharpe_ratio: float = DEFAULT_MAINNET_SHADOW_MIN_SHARPE_RATIO,
    max_drawdown_pct: float = DEFAULT_MAINNET_SHADOW_MAX_DRAWDOWN_PCT,
) -> dict[str, Any]:
    outcome_rows = list(outcomes)
    readiness_outcomes = [item for item in outcome_rows if not is_research_shadow_row(item)]
    research_only_outcomes_excluded = len(outcome_rows) - len(readiness_outcomes)
    latest_outcomes = latest_shadow_outcomes_by_instance(readiness_outcomes)
    returns_pct: list[float] = []
    pnls_usdt: list[float] = []
    for item in latest_outcomes:
        return_pct = safe_float(item.get("after_fee_and_slippage_return_pct"))
        if return_pct is None:
            return_pct = safe_float(item.get("after_fee_return_pct"))
        if return_pct is None:
            continue
        returns_pct.append(return_pct)
        allocation = safe_float(item.get("effective_quote_allocation_usdt"))
        pnls_usdt.append((allocation if allocation is not None else 1.0) * (return_pct / 100.0))

    closed_count = len(returns_pct)
    wins = [value for value in returns_pct if value > 0]
    losses = [value for value in returns_pct if value < 0]
    win_rate_pct = (len(wins) / closed_count * 100.0) if closed_count else None
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = 999999.0
    else:
        profit_factor = None
    sharpe_ratio = compute_sharpe_ratio(returns_pct)
    observed_max_drawdown_pct = compute_equity_curve_max_drawdown_pct(
        pnls_usdt,
        baseline_equity_usdt=baseline_equity_usdt,
    )
    sample_size_ok = closed_count >= max(1, int(min_closed_trades))
    expectancy_ok = (
        (
            win_rate_pct is not None
            and profit_factor is not None
            and win_rate_pct > float(min_win_rate_pct)
            and profit_factor > float(min_profit_factor)
        )
        or (sharpe_ratio is not None and sharpe_ratio > float(min_sharpe_ratio))
    )
    drawdown_ok = observed_max_drawdown_pct < float(max_drawdown_pct)
    live_unlock_candidate = bool(sample_size_ok and expectancy_ok and drawdown_ok)
    return {
        "generated_at": generated_at,
        "execution_mode": EXECUTION_MODE_MAINNET_SHADOW,
        "live_trading_enabled": False,
        "live_unlock_candidate": live_unlock_candidate,
        "closed_shadow_trades": closed_count,
        "research_only_outcomes_excluded": research_only_outcomes_excluded,
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate_pct": win_rate_pct,
        "profit_factor": profit_factor,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown_pct": observed_max_drawdown_pct,
        "estimated_total_pnl_usdt": sum(pnls_usdt),
        "thresholds": {
            "min_closed_trades": max(1, int(min_closed_trades)),
            "min_win_rate_pct": float(min_win_rate_pct),
            "min_profit_factor": float(min_profit_factor),
            "min_sharpe_ratio": float(min_sharpe_ratio),
            "max_drawdown_pct": float(max_drawdown_pct),
        },
        "criteria": {
            "sample_size_ok": sample_size_ok,
            "expectancy_ok": expectancy_ok,
            "drawdown_ok": drawdown_ok,
        },
    }


def summarize_shadow_branch_type_auto_performance(
    signals: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
    *,
    branch_type: str,
) -> dict[str, Any]:
    normalized_branch_type = normalize_branch_type(branch_type)
    filtered_signals = filter_shadow_rows_by_branch_type(signals, branch_type=normalized_branch_type)
    filtered_outcomes = filter_shadow_rows_by_shadow_branch_id(
        filter_shadow_rows_by_branch_type(outcomes, branch_type=normalized_branch_type),
        shadow_branch_id="AUTO",
    )
    playbook_counter = Counter(str(item.get("playbook") or "unknown") for item in filtered_signals)
    regular_returns = [
        value
        for value in (safe_float(item.get("after_fee_return_pct")) for item in filtered_outcomes)
        if value is not None
    ]
    slippage_adjusted_returns = [
        value
        for value in (safe_float(item.get("after_fee_and_slippage_return_pct")) for item in filtered_outcomes)
        if value is not None
    ]
    total_pnl_usdt = 0.0
    slippage_adjusted_total_pnl_usdt = 0.0
    for item in filtered_outcomes:
        effective_quote_allocation_usdt = safe_float(item.get("effective_quote_allocation_usdt"))
        regular_return_pct = safe_float(item.get("after_fee_return_pct"))
        adjusted_return_pct = safe_float(item.get("after_fee_and_slippage_return_pct"))
        if effective_quote_allocation_usdt is not None and regular_return_pct is not None:
            total_pnl_usdt += effective_quote_allocation_usdt * (regular_return_pct / 100.0)
        if effective_quote_allocation_usdt is not None and adjusted_return_pct is not None:
            slippage_adjusted_total_pnl_usdt += effective_quote_allocation_usdt * (adjusted_return_pct / 100.0)
    extra_slippage_penalty_pct = max(
        [safe_float(item.get("extra_slippage_penalty_pct")) or 0.0 for item in filtered_outcomes] or [0.0]
    )
    return {
        "branch_type": normalized_branch_type,
        "signal_count": len(filtered_signals),
        "auto_outcome_count": len(filtered_outcomes),
        "playbook_counts": dict(sorted(playbook_counter.items())),
        "avg_after_fee_return_pct": (sum(regular_returns) / len(regular_returns)) if regular_returns else None,
        "avg_after_fee_and_slippage_return_pct": (
            (sum(slippage_adjusted_returns) / len(slippage_adjusted_returns))
            if slippage_adjusted_returns
            else None
        ),
        "estimated_total_pnl_usdt": total_pnl_usdt,
        "slippage_adjusted_total_pnl_usdt": slippage_adjusted_total_pnl_usdt,
        "sharpe_ratio": compute_sharpe_ratio(regular_returns),
        "slippage_adjusted_sharpe_ratio": compute_sharpe_ratio(slippage_adjusted_returns),
        "extra_slippage_penalty_pct": extra_slippage_penalty_pct,
    }


def compare_shadow_branch_type_auto_performance(
    stable_summary: dict[str, Any],
    degen_summary: dict[str, Any],
) -> dict[str, Any]:
    stable_pnl = safe_float(stable_summary.get("estimated_total_pnl_usdt")) or 0.0
    degen_pnl = safe_float(degen_summary.get("estimated_total_pnl_usdt")) or 0.0
    stable_slippage_pnl = safe_float(stable_summary.get("slippage_adjusted_total_pnl_usdt")) or 0.0
    degen_slippage_pnl = safe_float(degen_summary.get("slippage_adjusted_total_pnl_usdt")) or 0.0
    return {
        "stable_branch_type": stable_summary.get("branch_type"),
        "degen_branch_type": degen_summary.get("branch_type"),
        "stable_auto_pnl_usdt": stable_pnl,
        "degen_auto_pnl_usdt": degen_pnl,
        "pnl_delta_degen_minus_stable_usdt": degen_pnl - stable_pnl,
        "stable_auto_slippage_adjusted_pnl_usdt": stable_slippage_pnl,
        "degen_auto_slippage_adjusted_pnl_usdt": degen_slippage_pnl,
        "slippage_adjusted_pnl_delta_degen_minus_stable_usdt": degen_slippage_pnl - stable_slippage_pnl,
        "stable_auto_sharpe_ratio": stable_summary.get("sharpe_ratio"),
        "degen_auto_sharpe_ratio": degen_summary.get("sharpe_ratio"),
        "stable_auto_slippage_adjusted_sharpe_ratio": stable_summary.get("slippage_adjusted_sharpe_ratio"),
        "degen_auto_slippage_adjusted_sharpe_ratio": degen_summary.get("slippage_adjusted_sharpe_ratio"),
        "stable_signal_count": int(stable_summary.get("signal_count") or 0),
        "degen_signal_count": int(degen_summary.get("signal_count") or 0),
        "stable_auto_outcome_count": int(stable_summary.get("auto_outcome_count") or 0),
        "degen_auto_outcome_count": int(degen_summary.get("auto_outcome_count") or 0),
    }


def build_total_sample_report(
    *,
    generated_at: str,
    baseline_equity_usdt: float,
    available_balance_usdt: float | None,
    wallet_balance_usdt: float | None,
    total_unrealized_pnl_usdt: float,
    open_positions: list[dict[str, Any]],
    shadow_signals: list[dict[str, Any]],
    shadow_outcomes: list[dict[str, Any]],
    filtered_quote_allocation_cap_usdt: float,
    shadow_readiness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    filtered_signals = filter_shadow_rows_by_quote_allocation(
        shadow_signals,
        max_effective_quote_allocation_usdt=filtered_quote_allocation_cap_usdt,
    )
    filtered_outcomes = filter_shadow_rows_by_quote_allocation(
        shadow_outcomes,
        max_effective_quote_allocation_usdt=filtered_quote_allocation_cap_usdt,
    )
    stable_auto_summary = summarize_shadow_branch_type_auto_performance(
        filtered_signals,
        filtered_outcomes,
        branch_type=BRANCH_TYPE_STABLE_CORE,
    )
    degen_auto_summary = summarize_shadow_branch_type_auto_performance(
        shadow_signals,
        shadow_outcomes,
        branch_type=BRANCH_TYPE_DEGEN_HIGH_YIELD,
    )
    report = {
        "generated_at": generated_at,
        "sample_filter": {
            "effective_quote_allocation_usdt_lte": float(filtered_quote_allocation_cap_usdt),
        },
        "real_account": {
            "baseline_equity_usdt": max(0.0, float(baseline_equity_usdt or 0.0)),
            "wallet_balance_usdt": wallet_balance_usdt,
            "available_balance_usdt": available_balance_usdt,
            "total_unrealized_pnl_usdt": total_unrealized_pnl_usdt,
            "equity_delta_usdt": (
                ((wallet_balance_usdt or 0.0) + total_unrealized_pnl_usdt - baseline_equity_usdt)
                if wallet_balance_usdt is not None
                else None
            ),
            "open_position_count": len(open_positions),
            "open_positions": open_positions,
        },
        "shadow_all": summarize_shadow_performance(shadow_signals, shadow_outcomes),
        "shadow_10u_mode": summarize_shadow_performance(filtered_signals, filtered_outcomes),
        "shadow_10u_drawdown": summarize_shadow_drawdowns(filtered_outcomes),
        "shadow_branch_type_auto_summary": {
            BRANCH_TYPE_STABLE_CORE: stable_auto_summary,
            BRANCH_TYPE_DEGEN_HIGH_YIELD: degen_auto_summary,
        },
        "shadow_branch_type_auto_comparison": compare_shadow_branch_type_auto_performance(
            stable_auto_summary,
            degen_auto_summary,
        ),
    }
    if shadow_readiness is not None:
        report["mainnet_shadow_readiness"] = shadow_readiness
    return report


def format_live_performance_report(
    *,
    generated_at: str,
    baseline_equity_usdt: float,
    available_balance_usdt: float | None,
    wallet_balance_usdt: float | None,
    total_unrealized_pnl_usdt: float,
    open_positions: list[dict[str, Any]],
    shadow_summary: dict[str, Any],
) -> str:
    equity_estimate = (wallet_balance_usdt or 0.0) + total_unrealized_pnl_usdt if wallet_balance_usdt is not None else None
    equity_delta = (equity_estimate - baseline_equity_usdt) if equity_estimate is not None else None
    lines = [
        "Phoenix Live Performance Report",
        f"Generated: {generated_at}",
        "",
        "[Real Account]",
        f"Baseline Equity USDT: {baseline_equity_usdt:.2f}",
        f"Wallet Balance USDT: {wallet_balance_usdt:.4f}" if wallet_balance_usdt is not None else "Wallet Balance USDT: unavailable",
        f"Available Balance USDT: {available_balance_usdt:.4f}" if available_balance_usdt is not None else "Available Balance USDT: unavailable",
        f"Open Position Count: {len(open_positions)}",
        f"Total Unrealized PnL USDT: {total_unrealized_pnl_usdt:.4f}",
        f"Estimated Equity Delta USDT: {equity_delta:.4f}" if equity_delta is not None else "Estimated Equity Delta USDT: unavailable",
        "",
        "[Open Positions]",
    ]
    if open_positions:
        for row in open_positions:
            lines.append(
                f"{row['symbol']} {row['side']} qty={row['quantity']:.6f} pnl={float(row['unrealized_pnl_usdt']):.4f}"
            )
    else:
        lines.append("none")

    lines.extend(
        [
            "",
            "[Shadow Research]",
            f"Signal Events Logged: {int(shadow_summary.get('signal_count') or 0)}",
            f"Shadow Branches Logged: {int(shadow_summary.get('branch_signal_count') or 0)}",
            f"Pending Signals: {int(shadow_summary.get('pending_signal_count') or 0)}",
            f"Pending Branches: {int(shadow_summary.get('pending_branch_count') or 0)}",
            f"Completed Outcomes: {int(shadow_summary.get('outcome_count') or 0)}",
            f"Playbook Counts: {json.dumps(shadow_summary.get('playbook_counts') or {}, ensure_ascii=False, sort_keys=True)}",
            f"Reason Counts: {json.dumps(shadow_summary.get('reason_counts') or {}, ensure_ascii=False, sort_keys=True)}",
            "",
            "[Shadow Branches]",
        ]
    )
    branch_summary = shadow_summary.get("branch_summary") if isinstance(shadow_summary, dict) else {}
    if isinstance(branch_summary, dict) and branch_summary:
        for branch_id in sorted(branch_summary):
            row = branch_summary.get(branch_id) or {}
            lines.append(
                f"{branch_id} "
                + f"sl={float(row.get('stop_loss_pct') or 0.0):.2f}% "
                + f"tp={float(row.get('take_profit_pct') or 0.0):.2f}% "
                + f"signals={int(row.get('signal_count') or 0)} "
                + f"outcomes={int(row.get('outcome_count') or 0)} "
                + (
                    f"avg_holding_sec={float(row.get('avg_holding_duration_sec') or 0.0):.1f} "
                    if row.get("avg_holding_duration_sec") is not None
                    else ""
                )
                + f"exits={json.dumps(row.get('exit_reason_counts') or {}, ensure_ascii=False, sort_keys=True)}"
            )
            branch_horizons = row.get("horizon_summary") if isinstance(row, dict) else {}
            if isinstance(branch_horizons, dict):
                for horizon_sec in sorted(int(key) for key in branch_horizons):
                    horizon_row = branch_horizons.get(horizon_sec) or {}
                    lines.append(
                        f"{branch_id} {horizon_sec}s count={int(horizon_row.get('count') or 0)} "
                        + f"avg_after_fee_return_pct={float(horizon_row.get('avg_after_fee_return_pct') or 0.0):.4f} "
                        + f"estimated_total_pnl_usdt={float(horizon_row.get('estimated_total_pnl_usdt') or 0.0):.4f}"
                    )
    else:
        lines.append("none")

    lines.extend(
        [
            "",
            "[Shadow Horizons]",
        ]
    )
    horizon_summary = shadow_summary.get("horizon_summary") if isinstance(shadow_summary, dict) else {}
    if isinstance(horizon_summary, dict) and horizon_summary:
        for horizon_sec in sorted(int(key) for key in horizon_summary):
            row = horizon_summary.get(horizon_sec) or {}
            lines.append(
                f"{horizon_sec}s count={int(row.get('count') or 0)} "
                + f"avg_after_fee_return_pct={float(row.get('avg_after_fee_return_pct') or 0.0):.4f} "
                + f"estimated_total_pnl_usdt={float(row.get('estimated_total_pnl_usdt') or 0.0):.4f}"
            )
    else:
        lines.append("none")
    lines.append("")
    return "\n".join(lines)


async def refresh_live_tradable_universe(
    *,
    futures: BinanceFuturesClient,
    top_limit: int,
    min_quote_volume_24h: float,
    refresh_sec: float,
    cached_symbols: set[str],
    cached_ranks: dict[str, int],
    cached_quote_volumes: dict[str, float],
    cached_market_stats_by_symbol: dict[str, dict[str, float | None]],
    exchange_info_payload: dict[str, Any] | None,
    last_refresh_monotonic: float,
) -> tuple[set[str], dict[str, int], dict[str, float], dict[str, dict[str, float | None]], dict[str, Any] | None, float]:
    min_quote_threshold = max(0.0, float(min_quote_volume_24h or 0.0))
    if top_limit <= 0 and min_quote_threshold <= 0:
        return (
            cached_symbols,
            cached_ranks,
            cached_quote_volumes,
            cached_market_stats_by_symbol,
            exchange_info_payload,
            last_refresh_monotonic,
        )
    now_monotonic = time.monotonic()
    if cached_symbols and (now_monotonic - last_refresh_monotonic) < max(1.0, float(refresh_sec or 0.0)):
        return (
            cached_symbols,
            cached_ranks,
            cached_quote_volumes,
            cached_market_stats_by_symbol,
            exchange_info_payload,
            last_refresh_monotonic,
        )
    if exchange_info_payload is None:
        exchange_info_payload = await futures.exchange_info()
    ticker_payload = await futures.ticker_24hr()
    eligible_symbols = build_eligible_perpetual_symbols(exchange_info_payload)
    symbols, ranks, quote_volumes = build_live_tradable_universe(
        ticker_payload,
        exchange_info_payload,
        top_limit=max(0, int(top_limit)),
        min_quote_volume_24h=min_quote_threshold,
    )
    market_stats_by_symbol = build_live_market_stats_by_symbol(
        ticker_payload,
        selected_symbols=None if min_quote_threshold > 0 and top_limit <= 0 else symbols,
        eligible_symbols=eligible_symbols,
    )
    emit_event(
        "signal_bridge_live_universe_refreshed",
        top_limit=max(0, int(top_limit)),
        min_quote_volume_24h=min_quote_threshold,
        tradable_symbol_count=len(symbols),
        sample_symbols=sorted(symbols)[:10],
    )
    return symbols, ranks, quote_volumes, market_stats_by_symbol, exchange_info_payload, now_monotonic


async def write_live_performance_report(
    *,
    futures: BinanceFuturesClient,
    report_file: Path,
    total_sample_file: Path,
    readiness_report_file: Path | None = None,
    candidate_strategy_report_file: Path | None = None,
    candidate_strategy_control_file: Path | None = None,
    strategy_shadow_league_report_file: Path | None = None,
    strategy_shadow_league_report_md_file: Path | None = None,
    factor_factory_report_file: Path | None = None,
    factor_factory_control_file: Path | None = None,
    bias_audit_report_file: Path | None = None,
    execution_realism_report_file: Path | None = None,
    promotion_gate_report_file: Path | None = None,
    backtest_report_file: Path | None = None,
    shadow_log_file: Path,
    shadow_outcomes_file: Path,
    snapshots_file: Path | None = None,
    baseline_equity_usdt: float,
    readiness_min_closed_trades: int = DEFAULT_MAINNET_SHADOW_MIN_CLOSED_TRADES,
    readiness_min_win_rate_pct: float = DEFAULT_MAINNET_SHADOW_MIN_WIN_RATE_PCT,
    readiness_min_profit_factor: float = DEFAULT_MAINNET_SHADOW_MIN_PROFIT_FACTOR,
    readiness_min_sharpe_ratio: float = DEFAULT_MAINNET_SHADOW_MIN_SHARPE_RATIO,
    readiness_max_drawdown_pct: float = DEFAULT_MAINNET_SHADOW_MAX_DRAWDOWN_PCT,
    candidate_auto_pause_min_outcomes: int = DEFAULT_CANDIDATE_AUTO_PAUSE_MIN_OUTCOMES,
    candidate_auto_pause_profit_factor: float = DEFAULT_CANDIDATE_AUTO_PAUSE_PROFIT_FACTOR,
    candidate_auto_pause_min_avg_return_pct: float = DEFAULT_CANDIDATE_AUTO_PAUSE_MIN_AVG_RETURN_PCT,
    candidate_focus_min_outcomes: int = DEFAULT_CANDIDATE_FOCUS_MIN_OUTCOMES,
    candidate_focus_profit_factor: float = DEFAULT_CANDIDATE_FOCUS_PROFIT_FACTOR,
    factor_factory_max_snapshots: int = DEFAULT_FACTOR_FACTORY_MAX_SNAPSHOTS,
    factor_factory_max_outcomes: int = DEFAULT_FACTOR_FACTORY_MAX_OUTCOMES,
    factor_factory_min_samples: int = DEFAULT_FACTOR_FACTORY_MIN_SAMPLES,
    factor_factory_pair_min_samples: int = DEFAULT_FACTOR_FACTORY_PAIR_MIN_SAMPLES,
    governance_max_records: int = DEFAULT_GOVERNANCE_MAX_RECORDS,
) -> dict[str, Any] | None:
    account_overview: dict[str, Any] = {}
    positions_payload: list[dict[str, Any]] = []
    account_overview_error: str | None = None
    try:
        account_overview = await futures.account_overview()
        positions_payload = await futures.position_information_v3()
    except Exception as exc:  # noqa: BLE001
        account_overview_error = str(exc)
        emit_event(
            "signal_bridge_account_overview_unavailable_for_report",
            error=account_overview_error,
            report_file=str(report_file),
        )
    open_positions = summarize_open_positions(positions_payload)
    total_unrealized_pnl_usdt = sum(
        safe_float(item.get("unrealized_pnl_usdt")) or 0.0
        for item in open_positions
    )
    shadow_signals = read_jsonl_records(shadow_log_file)
    shadow_outcomes = read_jsonl_records(shadow_outcomes_file)
    shadow_summary = summarize_shadow_performance(
        shadow_signals,
        shadow_outcomes,
    )
    generated_at = now_iso()
    report_text = format_live_performance_report(
        generated_at=generated_at,
        baseline_equity_usdt=max(0.0, float(baseline_equity_usdt or 0.0)),
        available_balance_usdt=extract_available_balance(account_overview),
        wallet_balance_usdt=extract_wallet_balance(account_overview),
        total_unrealized_pnl_usdt=total_unrealized_pnl_usdt,
        open_positions=open_positions,
        shadow_summary=shadow_summary,
    )
    readiness_payload = build_shadow_readiness_report(
        shadow_outcomes,
        generated_at=generated_at,
        baseline_equity_usdt=max(0.0, float(baseline_equity_usdt or 0.0)),
        min_closed_trades=max(1, int(readiness_min_closed_trades)),
        min_win_rate_pct=float(readiness_min_win_rate_pct),
        min_profit_factor=float(readiness_min_profit_factor),
        min_sharpe_ratio=float(readiness_min_sharpe_ratio),
        max_drawdown_pct=float(readiness_max_drawdown_pct),
    )
    total_sample_payload = build_total_sample_report(
        generated_at=generated_at,
        baseline_equity_usdt=max(0.0, float(baseline_equity_usdt or 0.0)),
        available_balance_usdt=extract_available_balance(account_overview),
        wallet_balance_usdt=extract_wallet_balance(account_overview),
        total_unrealized_pnl_usdt=total_unrealized_pnl_usdt,
        open_positions=open_positions,
        shadow_signals=shadow_signals,
        shadow_outcomes=shadow_outcomes,
        filtered_quote_allocation_cap_usdt=10.0,
        shadow_readiness=readiness_payload,
    )
    total_sample_payload["real_account"]["account_overview_error"] = account_overview_error
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(report_text, encoding="utf-8")
    total_sample_file.parent.mkdir(parents=True, exist_ok=True)
    total_sample_file.write_text(
        json.dumps(total_sample_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if readiness_report_file is not None:
        readiness_report_file.parent.mkdir(parents=True, exist_ok=True)
        readiness_report_file.write_text(
            json.dumps(readiness_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    candidate_control_payload: dict[str, Any] | None = None
    if candidate_strategy_report_file is not None:
        existing_candidate_control = (
            read_json_object_file(candidate_strategy_control_file)
            if candidate_strategy_control_file is not None
            else None
        )
        candidate_report_payload = build_candidate_strategy_report(
            generated_at=generated_at,
            shadow_signals=shadow_signals,
            shadow_outcomes=shadow_outcomes,
            control_payload=existing_candidate_control,
        )
        candidate_strategy_report_file.parent.mkdir(parents=True, exist_ok=True)
        candidate_strategy_report_file.write_text(
            json.dumps(candidate_report_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        if candidate_strategy_control_file is not None:
            candidate_control_payload = update_candidate_strategy_control(
                generated_at=generated_at,
                report=candidate_report_payload,
                existing_payload=existing_candidate_control,
                min_outcomes=max(1, int(candidate_auto_pause_min_outcomes)),
                pause_profit_factor=float(candidate_auto_pause_profit_factor),
                pause_min_avg_return_pct=float(candidate_auto_pause_min_avg_return_pct),
                focus_min_outcomes=max(1, int(candidate_focus_min_outcomes)),
                focus_profit_factor=float(candidate_focus_profit_factor),
            )
            atomic_write_json(candidate_strategy_control_file, candidate_control_payload)
            emit_event(
                "signal_bridge_candidate_strategy_control_written",
                control_file=str(candidate_strategy_control_file),
                report_file=str(candidate_strategy_report_file),
                playbook_count=len(candidate_control_payload.get("playbooks") or {}),
                paused_playbooks=sorted(
                    playbook
                    for playbook, row in (candidate_control_payload.get("playbooks") or {}).items()
                    if isinstance(row, dict) and row.get("status") == "paused"
                ),
                focus_playbooks=sorted(
                    playbook
                    for playbook, row in (candidate_control_payload.get("playbooks") or {}).items()
                    if isinstance(row, dict) and row.get("status") == "focus"
                ),
            )
    if strategy_shadow_league_report_file is not None:
        try:
            strategy_shadow_league_payload = build_strategy_shadow_league_report(
                generated_at=generated_at,
                shadow_signals=shadow_signals,
                shadow_outcomes=shadow_outcomes,
            )
            strategy_shadow_league_report_file.parent.mkdir(parents=True, exist_ok=True)
            strategy_shadow_league_report_file.write_text(
                json.dumps(strategy_shadow_league_payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            if strategy_shadow_league_report_md_file is not None:
                strategy_shadow_league_report_md_file.parent.mkdir(parents=True, exist_ok=True)
                strategy_shadow_league_report_md_file.write_text(
                    build_strategy_shadow_league_markdown(strategy_shadow_league_payload),
                    encoding="utf-8",
                )
            emit_event(
                "signal_bridge_strategy_shadow_league_report_written",
                report_file=str(strategy_shadow_league_report_file),
                markdown_file=(
                    str(strategy_shadow_league_report_md_file)
                    if strategy_shadow_league_report_md_file is not None
                    else None
                ),
                strategy_family=MOMENTUM_SCALP_STRATEGY_FAMILY,
                strategy_count=len(strategy_shadow_league_payload.get("strategies") or []),
                shadow_signal_count=(strategy_shadow_league_payload.get("input_counts") or {}).get("shadow_signals"),
                shadow_outcome_count=(strategy_shadow_league_payload.get("input_counts") or {}).get("shadow_outcomes"),
                live_trading_enabled=False,
                promotion_allowed=False,
            )
        except Exception as exc:  # noqa: BLE001
            emit_event(
                "signal_bridge_strategy_shadow_league_report_failed",
                error=str(exc),
                report_file=str(strategy_shadow_league_report_file),
            )
    factor_control_payload: dict[str, Any] | None = None
    bias_audit_payload: dict[str, Any] | None = None
    execution_realism_payload: dict[str, Any] | None = None
    backtest_report_payload = read_json_object_file(backtest_report_file) if backtest_report_file is not None else None
    if factor_factory_report_file is not None and snapshots_file is not None:
        try:
            factor_snapshots = read_recent_factor_jsonl_records(
                snapshots_file,
                max_records=max(0, int(factor_factory_max_snapshots)),
            )
            factor_outcomes = read_recent_factor_jsonl_records(
                shadow_outcomes_file,
                max_records=max(0, int(factor_factory_max_outcomes)),
            )
            factor_report_payload = build_factor_factory_report(
                snapshots=factor_snapshots,
                outcomes=factor_outcomes,
                generated_at=generated_at,
                branch_type=BRANCH_TYPE_RESEARCH_POOL,
                min_samples=max(1, int(factor_factory_min_samples)),
                pair_min_samples=max(1, int(factor_factory_pair_min_samples)),
                top_n=25,
            )
            factor_factory_report_file.parent.mkdir(parents=True, exist_ok=True)
            factor_factory_report_file.write_text(
                json.dumps(factor_report_payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            if factor_factory_control_file is not None:
                factor_control_payload = build_factor_factory_control(
                    factor_report_payload,
                    min_outcomes=max(1, int(factor_factory_min_samples)),
                )
                atomic_write_json(factor_factory_control_file, factor_control_payload)
            top_factor_rule = (factor_report_payload.get("ranked_candidate_rules") or [{}])[0]
            emit_event(
                "signal_bridge_factor_factory_report_written",
                report_file=str(factor_factory_report_file),
                control_file=str(factor_factory_control_file) if factor_factory_control_file else None,
                snapshots_read=(factor_report_payload.get("input_counts") or {}).get("snapshots_read"),
                outcomes_read=(factor_report_payload.get("input_counts") or {}).get("outcomes_read"),
                rule_evaluation_rows=(factor_report_payload.get("input_counts") or {}).get("rule_evaluation_rows"),
                top_rule=top_factor_rule.get("rule"),
                top_rule_avg_return_pct=top_factor_rule.get("avg_return_pct"),
                top_rule_profit_factor=top_factor_rule.get("profit_factor"),
            )
        except Exception as exc:  # noqa: BLE001
            emit_event(
                "signal_bridge_factor_factory_report_failed",
                error=str(exc),
                report_file=str(factor_factory_report_file),
            )
    if snapshots_file is not None and bias_audit_report_file is not None:
        try:
            bias_records = read_recent_factor_jsonl_records(
                snapshots_file,
                max_records=max(0, int(governance_max_records)),
            )
            bias_audit_payload = audit_lookahead_recursive_bias(bias_records)
            bias_audit_report_file.parent.mkdir(parents=True, exist_ok=True)
            bias_audit_report_file.write_text(
                json.dumps(bias_audit_payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            emit_event(
                "signal_bridge_bias_audit_report_written",
                report_file=str(bias_audit_report_file),
                records_checked=bias_audit_payload.get("records_checked"),
                finding_count=bias_audit_payload.get("finding_count"),
                lookahead_bias_risk=bias_audit_payload.get("lookahead_bias_risk"),
            )
        except Exception as exc:  # noqa: BLE001
            emit_event("signal_bridge_bias_audit_report_failed", error=str(exc), report_file=str(bias_audit_report_file))
    if execution_realism_report_file is not None:
        try:
            execution_realism_payload = build_execution_realism_report(
                read_recent_factor_jsonl_records(
                    shadow_outcomes_file,
                    max_records=max(0, int(governance_max_records)),
                )
            )
            execution_realism_report_file.parent.mkdir(parents=True, exist_ok=True)
            execution_realism_report_file.write_text(
                json.dumps(execution_realism_payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            realistic_summary = execution_realism_payload.get("execution_realistic") or {}
            emit_event(
                "signal_bridge_execution_realism_report_written",
                report_file=str(execution_realism_report_file),
                rows_checked=execution_realism_payload.get("rows_checked"),
                realistic_avg_return_pct=realistic_summary.get("avg_return_pct"),
                realistic_profit_factor=realistic_summary.get("profit_factor"),
            )
        except Exception as exc:  # noqa: BLE001
            emit_event(
                "signal_bridge_execution_realism_report_failed",
                error=str(exc),
                report_file=str(execution_realism_report_file),
            )
    if promotion_gate_report_file is not None:
        try:
            promotion_payload = promotion_gate_decision(
                factor_control=factor_control_payload,
                shadow_readiness=readiness_payload,
                execution_realism=execution_realism_payload,
                bias_audit=bias_audit_payload,
                backtest_report=backtest_report_payload,
            )
            promotion_gate_report_file.parent.mkdir(parents=True, exist_ok=True)
            promotion_gate_report_file.write_text(
                json.dumps(promotion_payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            emit_event(
                "signal_bridge_promotion_gate_report_written",
                report_file=str(promotion_gate_report_file),
                promotion_candidate=promotion_payload.get("promotion_candidate"),
                backtest_report_file=str(backtest_report_file) if backtest_report_file is not None else None,
                backtest_loaded=backtest_report_payload is not None,
                blockers=promotion_payload.get("blockers"),
                warnings=promotion_payload.get("warnings"),
            )
        except Exception as exc:  # noqa: BLE001
            emit_event("signal_bridge_promotion_gate_report_failed", error=str(exc), report_file=str(promotion_gate_report_file))
    emit_event(
        "signal_bridge_live_report_written",
        report_file=str(report_file),
        total_sample_file=str(total_sample_file),
        readiness_report_file=str(readiness_report_file) if readiness_report_file is not None else None,
        candidate_strategy_report_file=(
            str(candidate_strategy_report_file) if candidate_strategy_report_file is not None else None
        ),
        candidate_strategy_control_file=(
            str(candidate_strategy_control_file) if candidate_strategy_control_file is not None else None
        ),
        strategy_shadow_league_report_file=(
            str(strategy_shadow_league_report_file) if strategy_shadow_league_report_file is not None else None
        ),
        strategy_shadow_league_report_md_file=(
            str(strategy_shadow_league_report_md_file) if strategy_shadow_league_report_md_file is not None else None
        ),
        factor_factory_report_file=(
            str(factor_factory_report_file) if factor_factory_report_file is not None else None
        ),
        factor_factory_control_file=(
            str(factor_factory_control_file) if factor_factory_control_file is not None else None
        ),
        bias_audit_report_file=str(bias_audit_report_file) if bias_audit_report_file is not None else None,
        execution_realism_report_file=(
            str(execution_realism_report_file) if execution_realism_report_file is not None else None
        ),
        promotion_gate_report_file=str(promotion_gate_report_file) if promotion_gate_report_file is not None else None,
        backtest_report_file=str(backtest_report_file) if backtest_report_file is not None else None,
        backtest_report_loaded=backtest_report_payload is not None,
        shadow_signal_count=shadow_summary.get("signal_count"),
        shadow_outcome_count=shadow_summary.get("outcome_count"),
        readiness_closed_shadow_trades=readiness_payload.get("closed_shadow_trades"),
        readiness_live_unlock_candidate=readiness_payload.get("live_unlock_candidate"),
        readiness_win_rate_pct=readiness_payload.get("win_rate_pct"),
        readiness_profit_factor=readiness_payload.get("profit_factor"),
        readiness_sharpe_ratio=readiness_payload.get("sharpe_ratio"),
        readiness_max_drawdown_pct=readiness_payload.get("max_drawdown_pct"),
        open_position_count=len(open_positions),
        total_unrealized_pnl_usdt=round(total_unrealized_pnl_usdt, 6),
    )
    return candidate_control_payload


async def apply_symbol_account_setup(
    futures: BinanceFuturesClient,
    *,
    symbol: str,
    leverage: int,
    margin_type: str,
    account_api_mode: str,
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if account_api_mode != "portfolio_margin":
        try:
            payload = await futures.change_margin_type(symbol, margin_type)
            actions.append({"name": "change_margin_type", "ok": True, "payload": payload})
        except BinanceAPIError as exc:
            if exc.code == -4046:
                actions.append({"name": "change_margin_type", "ok": True, "skipped": True, "reason": "already_isolated"})
            else:
                raise
    payload = await futures.change_initial_leverage(symbol, leverage)
    actions.append({"name": "change_initial_leverage", "ok": True, "payload": payload})
    return actions


async def emergency_flatten_after_failure(
    *,
    futures: BinanceFuturesClient,
    executor: PhoenixExecutor,
    intent: Any,
    position_mode: str,
    account_api_mode: str,
) -> dict[str, Any]:
    exit_side = "SELL" if intent.side == "BUY" else "BUY"
    hedge_mode = position_mode.upper() == "HEDGE"
    exit_position_side = executor._exit_position_side(intent.side, hedge_mode=hedge_mode)
    close_payload = {
        "symbol": intent.symbol,
        "side": exit_side,
        **({"positionSide": exit_position_side} if exit_position_side else {}),
        "type": "MARKET",
        "quantity": intent.quantity,
        **({} if hedge_mode and account_api_mode == "portfolio_margin" else {"reduceOnly": "true"}),
        "newOrderRespType": "RESULT",
    }
    close_response = None
    close_error = None
    cleanup_response = None
    cleanup_error = None
    try:
        close_response = await futures.new_order(close_payload)
    except Exception as exc:  # noqa: BLE001
        close_error = str(exc)
    try:
        cleanup_response = await futures.cancel_all_open_conditional_orders(intent.symbol)
    except Exception as exc:  # noqa: BLE001
        cleanup_error = str(exc)
    return {
        "emergency_reduce_only_close": {
            "ok": close_response is not None,
            "payload": close_response,
            "error": close_error,
        },
        "residual_protection_cleanup": {
            "ok": cleanup_error is None,
            "payload": cleanup_response,
            "error": cleanup_error,
        },
    }


def _absolute_position_quantity_payload(raw_position_amt: Any) -> str | None:
    raw_text = str(raw_position_amt or "").strip()
    quantity = safe_float(raw_text)
    if quantity is None or abs(quantity) <= 1e-12:
        return None
    if raw_text.startswith("-"):
        raw_text = raw_text[1:]
    if raw_text and raw_text not in {"0", "0.0"}:
        return raw_text
    return f"{abs(quantity):.12f}".rstrip("0").rstrip(".")


def build_hard_kill_close_payload(
    position: dict[str, Any],
    *,
    account_api_mode: str,
) -> dict[str, Any] | None:
    symbol = str(position.get("symbol") or "").upper()
    quantity_value = safe_float(position.get("positionAmt"))
    quantity_payload = _absolute_position_quantity_payload(position.get("positionAmt"))
    if not symbol or quantity_value is None or abs(quantity_value) <= 1e-12 or not quantity_payload:
        return None
    position_side = str(position.get("positionSide") or "").upper()
    hedge_like = position_side in {"LONG", "SHORT"}
    payload: dict[str, Any] = {
        "symbol": symbol,
        "side": "SELL" if quantity_value > 0 else "BUY",
        "type": "MARKET",
        "quantity": quantity_payload,
        "newOrderRespType": "RESULT",
    }
    if hedge_like:
        payload["positionSide"] = position_side
    if not (hedge_like and account_api_mode == "portfolio_margin"):
        payload["reduceOnly"] = "true"
    return payload


async def hard_kill_flatten_account(futures: BinanceFuturesClient) -> dict[str, Any]:
    account_api_mode = await futures.get_account_api_mode()
    positions_payload = await futures.position_information_v3()
    open_positions = [
        item
        for item in positions_payload
        if isinstance(item, dict)
        and (safe_float(item.get("positionAmt")) is not None)
        and abs(float(safe_float(item.get("positionAmt")) or 0.0)) > 1e-12
    ]
    symbols = {str(item.get("symbol") or "").upper() for item in open_positions if str(item.get("symbol") or "").strip()}
    open_orders_error = None
    open_conditional_orders_error = None
    try:
        for order in await futures.open_orders():
            if isinstance(order, dict) and str(order.get("symbol") or "").strip():
                symbols.add(str(order.get("symbol") or "").upper())
    except Exception as exc:  # noqa: BLE001
        open_orders_error = str(exc)
    try:
        for order in await futures.open_conditional_orders():
            if isinstance(order, dict) and str(order.get("symbol") or "").strip():
                symbols.add(str(order.get("symbol") or "").upper())
    except Exception as exc:  # noqa: BLE001
        open_conditional_orders_error = str(exc)

    cancel_results: list[dict[str, Any]] = []
    for symbol in sorted(symbols):
        symbol_result: dict[str, Any] = {"symbol": symbol}
        try:
            symbol_result["open_orders"] = {"ok": True, "payload": await futures.cancel_all_open_orders(symbol)}
        except Exception as exc:  # noqa: BLE001
            symbol_result["open_orders"] = {"ok": False, "error": str(exc)}
        try:
            symbol_result["conditional_orders"] = {
                "ok": True,
                "payload": await futures.cancel_all_open_conditional_orders(symbol),
            }
        except Exception as exc:  # noqa: BLE001
            symbol_result["conditional_orders"] = {"ok": False, "error": str(exc)}
        cancel_results.append(symbol_result)

    close_results: list[dict[str, Any]] = []
    for position in open_positions:
        payload = build_hard_kill_close_payload(position, account_api_mode=account_api_mode)
        if payload is None:
            continue
        try:
            response = await futures.new_order(payload)
            close_results.append({"symbol": payload["symbol"], "ok": True, "payload": payload, "response": response})
        except Exception as exc:  # noqa: BLE001
            close_results.append({"symbol": payload["symbol"], "ok": False, "payload": payload, "error": str(exc)})

    return {
        "account_api_mode": account_api_mode,
        "open_position_count": len(open_positions),
        "symbol_count": len(symbols),
        "open_orders_error": open_orders_error,
        "open_conditional_orders_error": open_conditional_orders_error,
        "cancel_results": cancel_results,
        "close_results": close_results,
    }


async def user_data_oms_keepalive_loop(
    *,
    futures: BinanceFuturesClient,
    listen_key: str,
    stop_event: asyncio.Event,
    keepalive_sec: float,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(60.0, float(keepalive_sec or 0.0)))
            return
        except asyncio.TimeoutError:
            pass
        try:
            await futures.keepalive_user_data_stream(listen_key)
            emit_event("signal_bridge_user_data_oms_keepalive", listen_key_suffix=listen_key[-6:])
        except Exception as exc:  # noqa: BLE001
            emit_event("signal_bridge_user_data_oms_keepalive_failed", error=str(exc))


def write_user_data_oms_status(
    path: Path,
    *,
    oms_state: UserDataOMSState,
    connected: bool,
    listen_key: str | None = None,
    error: str | None = None,
) -> None:
    payload = {
        "updated_at": now_iso(),
        "connected": connected,
        "listen_key_suffix": listen_key[-6:] if listen_key else None,
        "error": error,
        **oms_state.summary(),
    }
    atomic_write_json(path, payload)


async def user_data_oms_stream_loop(
    *,
    futures: BinanceFuturesClient,
    session: aiohttp.ClientSession,
    oms_state: UserDataOMSState,
    status_file: Path,
    stop_event: asyncio.Event,
    keepalive_sec: float,
    reconnect_sec: float,
) -> None:
    while not stop_event.is_set():
        listen_key = None
        keepalive_task: asyncio.Task[None] | None = None
        try:
            response = await futures.start_user_data_stream()
            listen_key = str(response.get("listenKey") or "").strip()
            if not listen_key:
                raise RuntimeError(f"User data stream did not return listenKey: {response}")
            url = f"{futures.environment.futures_ws_base.rstrip('/')}/private/ws/{listen_key}"
            keepalive_task = asyncio.create_task(
                user_data_oms_keepalive_loop(
                    futures=futures,
                    listen_key=listen_key,
                    stop_event=stop_event,
                    keepalive_sec=keepalive_sec,
                )
            )
            write_user_data_oms_status(status_file, oms_state=oms_state, connected=True, listen_key=listen_key)
            emit_event(
                "signal_bridge_user_data_oms_connected",
                url=f"{futures.environment.futures_ws_base.rstrip('/')}/private/ws/<listenKey>",
            )
            async with session.ws_connect(
                url,
                heartbeat=15.0,
                receive_timeout=DEFAULT_USER_DATA_OMS_WS_TIMEOUT_SEC,
                proxy=futures.proxy_settings.proxy_for_url(url),
            ) as websocket:
                async for message in websocket:
                    if stop_event.is_set():
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(message.data)
                        except json.JSONDecodeError:
                            emit_event("signal_bridge_user_data_oms_bad_json")
                            continue
                        update = oms_state.apply_event(payload) if isinstance(payload, dict) else None
                        if update is None:
                            continue
                        write_user_data_oms_status(
                            status_file,
                            oms_state=oms_state,
                            connected=True,
                            listen_key=listen_key,
                        )
                        if update.status in {"PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                            emit_event(
                                "signal_bridge_user_data_oms_order_update",
                                symbol=update.symbol,
                                side=update.side,
                                order_id=update.order_id,
                                client_order_id=update.client_order_id,
                                status=update.status,
                                execution_type=update.execution_type,
                                executed_qty=update.executed_qty,
                                avg_price=update.avg_price,
                            )
                    elif message.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE}:
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            emit_event("signal_bridge_user_data_oms_disconnected", error=str(exc))
            write_user_data_oms_status(
                status_file,
                oms_state=oms_state,
                connected=False,
                listen_key=listen_key,
                error=str(exc),
            )
        finally:
            if keepalive_task is not None:
                keepalive_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await keepalive_task
        if not stop_event.is_set():
            await asyncio.sleep(max(1.0, float(reconnect_sec or 0.0)))


def side_aware_return_pct(*, side: str, entry_price: float, current_price: float) -> float | None:
    if entry_price <= 0 or current_price <= 0:
        return None
    raw_return = ((current_price - entry_price) / entry_price) * 100.0
    return raw_return if str(side).upper() == "BUY" else (-raw_return)


async def fetch_shadow_path_candles(
    *,
    futures: BinanceFuturesClient,
    symbol: str,
    entry_time_ms: int,
    deadline_ms: int,
) -> list[list[Any]]:
    if entry_time_ms <= 0 or deadline_ms <= entry_time_ms:
        return []
    limit = max(8, min(1000, int((deadline_ms - entry_time_ms) / 60_000) + 8))
    payload = await futures.klines(
        symbol,
        interval="1m",
        start_time_ms=entry_time_ms + 1,
        end_time_ms=deadline_ms + 60_000,
        limit=limit,
    )
    return [
        row
        for row in payload
        if isinstance(row, list)
        and len(row) >= 7
        and int(row[6]) <= deadline_ms
    ]


def simulate_shadow_branch_outcome(
    *,
    event_id: str,
    branch: dict[str, Any],
    side: str,
    entry_price: float,
    entry_time_ms: int,
    horizon_sec: int,
    candles_1m: list[list[Any]],
    fallback_close_price: float | None,
    fallback_max_drawdown_pct: float | None,
    fallback_max_runup_pct: float | None,
    round_trip_fee_bps: float,
    extra_slippage_penalty_pct: float,
) -> dict[str, Any]:
    branch_id = str(branch.get("branch_id") or "LEGACY").upper()
    stop_loss_pct = max(0.01, float(branch.get("stop_loss_pct") or 0.01))
    take_profit_pct = max(0.0, float(branch.get("take_profit_pct") or 0.0))
    fee_pct = max(0.0, float(round_trip_fee_bps or 0.0)) / 100.0
    slippage_penalty_pct = max(0.0, float(extra_slippage_penalty_pct or 0.0))
    best_return_pct = None
    worst_return_pct = None
    realized_return_pct = None
    exit_price = fallback_close_price
    exit_reason = "horizon_close"
    holding_duration_sec = max(0, int(horizon_sec))

    for row in candles_1m:
        high_price = safe_float(row[2])
        low_price = safe_float(row[3])
        close_price = safe_float(row[4])
        close_time_ms = int(row[6]) if len(row) >= 7 else (entry_time_ms + horizon_sec * 1000)
        if high_price is None or low_price is None or close_price is None:
            continue
        favorable_probe = low_price if str(side).upper() == "SELL" else high_price
        adverse_probe = high_price if str(side).upper() == "SELL" else low_price
        favorable_return = side_aware_return_pct(side=side, entry_price=entry_price, current_price=favorable_probe)
        adverse_return = side_aware_return_pct(side=side, entry_price=entry_price, current_price=adverse_probe)
        close_return = side_aware_return_pct(side=side, entry_price=entry_price, current_price=close_price)
        if favorable_return is not None:
            best_return_pct = favorable_return if best_return_pct is None else max(best_return_pct, favorable_return)
        if adverse_return is not None:
            worst_return_pct = adverse_return if worst_return_pct is None else min(worst_return_pct, adverse_return)
        take_profit_hit = take_profit_pct > 0 and favorable_return is not None and favorable_return >= take_profit_pct
        stop_loss_hit = adverse_return is not None and adverse_return <= -stop_loss_pct
        holding_duration_sec = max(60, int(round((close_time_ms - entry_time_ms) / 1000.0)))
        if take_profit_hit and stop_loss_hit:
            exit_reason = "ambiguous_dual_hit_stop_assumed"
            realized_return_pct = -stop_loss_pct
            exit_price = adverse_probe
            break
        if stop_loss_hit:
            exit_reason = "stop_loss_hit"
            realized_return_pct = -stop_loss_pct
            exit_price = adverse_probe
            break
        if take_profit_hit:
            exit_reason = "take_profit_hit"
            realized_return_pct = take_profit_pct
            exit_price = favorable_probe
            break
        realized_return_pct = close_return
        exit_price = close_price

    if realized_return_pct is None and fallback_close_price is not None:
        realized_return_pct = side_aware_return_pct(side=side, entry_price=entry_price, current_price=fallback_close_price)
    if best_return_pct is None and fallback_max_runup_pct is not None:
        best_return_pct = (
            fallback_max_runup_pct if str(side).upper() == "BUY" else (-fallback_max_drawdown_pct if fallback_max_drawdown_pct is not None else None)
        )
    if worst_return_pct is None and fallback_max_drawdown_pct is not None:
        worst_return_pct = (
            fallback_max_drawdown_pct if str(side).upper() == "BUY" else (-fallback_max_runup_pct if fallback_max_runup_pct is not None else None)
        )
    best_return_pct = best_return_pct if best_return_pct is not None else 0.0
    worst_return_pct = worst_return_pct if worst_return_pct is not None else 0.0
    realized_return_pct = realized_return_pct if realized_return_pct is not None else 0.0
    return {
        "shadow_instance_id": build_shadow_instance_id(event_id, branch_id),
        "shadow_branch_id": branch_id,
        "shadow_branch_label": branch.get("branch_label") or branch_id,
        "branch_stop_loss_pct": stop_loss_pct,
        "branch_take_profit_pct": take_profit_pct,
        "round_trip_fee_bps": max(0.0, float(round_trip_fee_bps or 0.0)),
        "extra_slippage_penalty_pct": slippage_penalty_pct,
        "holding_duration_sec": holding_duration_sec,
        "exit_reason": exit_reason,
        "close_price": exit_price,
        "close_return_pct": realized_return_pct,
        "after_fee_return_pct": realized_return_pct - fee_pct,
        "after_fee_and_slippage_return_pct": realized_return_pct - fee_pct - slippage_penalty_pct,
        "max_runup_pct": best_return_pct,
        "max_drawdown_pct": worst_return_pct,
        "max_favorable_return_pct": best_return_pct,
        "max_adverse_return_pct": worst_return_pct,
    }


async def build_shadow_signal_payload(
    *,
    futures: BinanceFuturesClient,
    executor: PhoenixExecutor,
    settings: Any,
    record: dict[str, Any],
    playbook: str,
    side: str,
    failure_reason: str,
    low_balance_threshold_usdt: float,
    low_balance_quote_allocation_usdt: float,
    shadow_branch_specs: list[dict[str, Any]],
    target_horizons_sec: Iterable[int] = DEFAULT_SHADOW_TARGET_HORIZONS_SEC,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective_executor = clone_executor_with_settings(executor, settings)
    symbol = str(record.get("symbol") or "").upper()
    sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
    factors = record_factors(record)
    quote_volume_24h = safe_float(sample.get("quote_volume_24h"))
    funding_rate_at_entry = record_numeric_feature(record, "funding_rate")
    spread_bps_at_entry = record_numeric_feature(record, "spread_bps")
    estimated_slippage_bps = record_numeric_feature(record, "estimated_slippage_bps")
    shadow_branches = []
    for item in shadow_branch_specs:
        branch_payload = {
            "branch_id": str(item.get("branch_id") or "").upper(),
            "branch_label": item.get("branch_label"),
            "stop_loss_pct": max(0.01, float(item.get("stop_loss_pct") or 0.01)),
            "take_profit_pct": max(0.0, float(item.get("take_profit_pct") or 0.0)),
            "completed_horizons_sec": [],
        }
        branch_side = str(item.get("side") or "").strip().upper()
        if branch_side in {"BUY", "SELL"}:
            branch_payload["side"] = branch_side
        direction_variant = str(item.get("direction_variant") or "").strip().lower()
        if direction_variant:
            branch_payload["direction_variant"] = direction_variant
        if bool(item.get("research_only")):
            branch_payload["research_only"] = True
        shadow_branches.append(branch_payload)
    payload: dict[str, Any] = {
        "event": "signal_bridge_shadow_logged",
        "logged_at": now_iso(),
        "event_id": str(record.get("event_id") or ""),
        "symbol": symbol,
        "playbook": playbook,
        "side": side,
        "shadow_reason": failure_reason,
        "shadow_target_horizons_sec": normalize_shadow_target_horizons(
            list(target_horizons_sec),
            default=DEFAULT_SHADOW_TARGET_HORIZONS_SEC,
        ),
        "trading_session": record.get("trading_session"),
        "bar_interval": record.get("bar_interval"),
        "trigger_types": list(record.get("trigger_types") or sample.get("trigger_types") or []),
        "trigger_score": safe_float(record.get("trigger_score")),
        "sample_price": safe_float(sample.get("price")),
        "anchor_close_time_ms": int(sample.get("anchor_close_time_ms") or 0),
        "observed_at_ms": int(record.get("observed_at_ms") or 0),
        "quote_volume_24h": quote_volume_24h,
        "price_change_24h_pct": safe_float(sample.get("price_change_24h_pct")),
        "funding_rate_at_entry": funding_rate_at_entry,
        "spread_bps_at_entry": spread_bps_at_entry,
        "order_latency_ms": estimate_shadow_order_latency_ms(record),
        "estimated_slippage_bps": estimated_slippage_bps,
        "maker_or_taker": "paper_taker",
        "liquidity_bucket": liquidity_bucket_from_quote_volume(quote_volume_24h),
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "factors": factors,
        "shadow_branches": shadow_branches,
    }
    if extra_fields:
        payload.update(extra_fields)
    refreshed_quote_volume_24h = safe_float(payload.get("quote_volume_24h"))
    if str(payload.get("liquidity_bucket") or "unknown").strip().lower() == "unknown":
        payload["liquidity_bucket"] = liquidity_bucket_from_quote_volume(refreshed_quote_volume_24h)
    payload["branch_type"] = normalize_branch_type(payload.get("branch_type"))
    payload["extra_slippage_penalty_pct"] = max(
        0.0,
        safe_float(payload.get("extra_slippage_penalty_pct")) or 0.0,
    )
    try:
        account_api_mode = await futures.get_account_api_mode()
        position_mode = settings.position_mode
        margin_type_override = None
        if account_api_mode == "portfolio_margin":
            account_config = await futures.um_account_config()
            position_mode = "HEDGE" if bool(account_config.get("dualSidePosition")) else "ONE_WAY"
            raw_symbol_config = await futures.um_symbol_config(symbol)
            symbol_config = extract_symbol_config(raw_symbol_config, symbol)
            margin_type_value = symbol_config.get("marginType") if symbol_config is not None else None
            if margin_type_value not in (None, ""):
                margin_type_override = str(margin_type_value).upper()
        account_overview = await futures.account_overview()
        available_balance = extract_available_balance(account_overview)
        effective_quote_allocation = resolve_effective_quote_allocation_usdt(
            base_quote_allocation_usdt=settings.quote_allocation_usdt,
            available_balance_usdt=available_balance,
            low_balance_threshold_usdt=low_balance_threshold_usdt,
            low_balance_quote_allocation_usdt=low_balance_quote_allocation_usdt,
        )
        intent = await effective_executor.build_trade_intent(
            symbol=symbol,
            side=side,
            quote_allocation_usdt=effective_quote_allocation,
            leverage=settings.leverage,
            available_balance_usdt=available_balance,
            margin_type_override=margin_type_override,
        )
        payload.update(
            {
                "available_balance_usdt": available_balance,
                "position_mode": position_mode,
                "account_api_mode": account_api_mode,
                "effective_quote_allocation_usdt": effective_quote_allocation,
                "virtual_intent": intent.to_dict(),
            }
        )
        if bool(payload.get("exchange_preflight_requested")):
            preflight_results = await effective_executor.validate_test_plan(
                intent,
                include_account_setup=False,
            )
            preflight_summary = summarize_exchange_preflight_results(preflight_results)
            payload.update(
                {
                    "exchange_preflight_mode": "binance_usdm_order_test_or_preview",
                    "exchange_preflight_ok": bool(preflight_summary.get("ok")),
                    "exchange_preflight_summary": preflight_summary,
                    "exchange_preflight_results": preflight_results,
                }
            )
    except Exception as exc:  # noqa: BLE001
        payload["shadow_intent_error"] = str(exc)
        if bool(payload.get("exchange_preflight_requested")):
            payload["exchange_preflight_ok"] = False
            payload["exchange_preflight_error"] = str(exc)
    return payload


async def persist_shadow_signal(
    *,
    futures: BinanceFuturesClient,
    executor: PhoenixExecutor,
    settings: Any,
    record: dict[str, Any],
    playbook: str,
    side: str,
    failure_reason: str,
    low_balance_threshold_usdt: float,
    low_balance_quote_allocation_usdt: float,
    shadow_branch_specs: list[dict[str, Any]],
    shadow_log_file: Path,
    shadow_state_file: Path,
    horizon_labels_file: Path,
    horizon_labels_offset: int,
    pending_shadow_by_event_id: dict[str, dict[str, Any]],
    target_horizons_sec: Iterable[int] = DEFAULT_SHADOW_TARGET_HORIZONS_SEC,
    extra_fields: dict[str, Any] | None = None,
) -> None:
    shadow_payload = await build_shadow_signal_payload(
        futures=futures,
        executor=executor,
        settings=settings,
        record=record,
        playbook=playbook,
        side=side,
        failure_reason=failure_reason,
        low_balance_threshold_usdt=low_balance_threshold_usdt,
        low_balance_quote_allocation_usdt=low_balance_quote_allocation_usdt,
        shadow_branch_specs=shadow_branch_specs,
        target_horizons_sec=target_horizons_sec,
        extra_fields=extra_fields,
    )
    append_jsonl(shadow_log_file, shadow_payload)
    shadow_event_id = str(shadow_payload.get("event_id") or "")
    if shadow_event_id:
        pending_shadow_by_event_id[shadow_event_id] = {
            key: value
            for key, value in shadow_payload.items()
            if key != "event"
        }
        save_shadow_state(
            shadow_state_file,
            horizon_labels_file=horizon_labels_file,
            horizon_labels_offset=horizon_labels_offset,
            pending_by_event_id=pending_shadow_by_event_id,
        )


def persist_research_shadow_signal(
    *,
    record: dict[str, Any],
    shadow_log_file: Path,
    shadow_state_file: Path,
    horizon_labels_file: Path,
    horizon_labels_offset: int,
    pending_shadow_by_event_id: dict[str, dict[str, Any]],
    target_horizons_sec: Iterable[int] = DEFAULT_RESEARCH_SHADOW_TARGET_HORIZONS_SEC,
) -> dict[str, Any]:
    shadow_payload = build_research_shadow_signal_payload(
        record,
        target_horizons_sec=target_horizons_sec,
    )
    append_jsonl(shadow_log_file, shadow_payload)
    shadow_event_id = str(shadow_payload.get("event_id") or "")
    raw_shadow_branches = shadow_payload.get("shadow_branches")
    if shadow_event_id and isinstance(raw_shadow_branches, list) and raw_shadow_branches:
        pending_shadow_by_event_id[shadow_event_id] = {
            key: value
            for key, value in shadow_payload.items()
            if key != "event"
        }
        save_shadow_state(
            shadow_state_file,
            horizon_labels_file=horizon_labels_file,
            horizon_labels_offset=horizon_labels_offset,
            pending_by_event_id=pending_shadow_by_event_id,
        )
    return shadow_payload


def persist_momentum_scalp_shadow_signals(
    *,
    signals: Iterable[dict[str, Any]],
    shadow_log_file: Path,
    shadow_state_file: Path,
    horizon_labels_file: Path,
    horizon_labels_offset: int,
    pending_shadow_by_event_id: dict[str, dict[str, Any]],
) -> int:
    written = 0
    for signal in signals:
        if signal.get("strategy_family") != MOMENTUM_SCALP_STRATEGY_FAMILY:
            continue
        shadow_payload = dict(signal)
        shadow_payload["shadow_only"] = True
        shadow_payload["paper_record_only"] = True
        shadow_payload["live_trading_enabled"] = False
        shadow_payload["promotion_allowed"] = False
        shadow_payload["live_order_submission_blocked"] = True
        shadow_payload["live_order_queue_write"] = False
        shadow_payload["order_endpoint_called"] = False
        append_jsonl(shadow_log_file, shadow_payload)
        shadow_event_id = str(shadow_payload.get("event_id") or "")
        if shadow_event_id:
            pending_shadow_by_event_id[shadow_event_id] = {
                key: value
                for key, value in shadow_payload.items()
                if key != "event"
            }
        written += 1
    if written:
        save_shadow_state(
            shadow_state_file,
            horizon_labels_file=horizon_labels_file,
            horizon_labels_offset=horizon_labels_offset,
            pending_by_event_id=pending_shadow_by_event_id,
        )
    return written


def pending_shadow_items_for_horizon_event_id(
    pending_by_event_id: dict[str, dict[str, Any]],
    *,
    horizon_event_id: str,
) -> list[tuple[str, dict[str, Any]]]:
    matches: list[tuple[str, dict[str, Any]]] = []
    direct = pending_by_event_id.get(horizon_event_id)
    if direct is not None:
        matches.append((horizon_event_id, direct))
    for pending_key, payload in sorted(pending_by_event_id.items()):
        if pending_key == horizon_event_id:
            continue
        linked_event_id = str(payload.get("horizon_event_id") or payload.get("source_event_id") or "").strip()
        if linked_event_id == horizon_event_id:
            matches.append((pending_key, payload))
    return matches


def shadow_signal_notional_usdt(shadow_signal: dict[str, Any]) -> float | None:
    virtual_intent = shadow_signal.get("virtual_intent")
    if isinstance(virtual_intent, dict):
        notional = safe_float(virtual_intent.get("notional_usdt"))
        if notional is not None and notional > 0:
            return notional
    quote = safe_float(shadow_signal.get("effective_quote_allocation_usdt"))
    leverage = safe_float(shadow_signal.get("leverage"))
    if quote is not None and quote > 0 and leverage is not None and leverage > 0:
        return quote * leverage
    return None


def estimate_funding_paid_during_hold(
    shadow_signal: dict[str, Any],
    *,
    side: str,
    horizon_sec: int,
) -> float:
    funding_rate = safe_float(shadow_signal.get("funding_rate_at_entry"))
    if funding_rate is None:
        funding_rate = safe_float(shadow_signal.get("funding_rate"))
    notional = shadow_signal_notional_usdt(shadow_signal)
    if funding_rate is None or notional is None or horizon_sec <= 0:
        return 0.0
    side_multiplier = 1.0 if str(side).upper() == "BUY" else -1.0
    funding_period_fraction = max(0.0, float(horizon_sec)) / (8.0 * 60.0 * 60.0)
    return round(notional * funding_rate * funding_period_fraction * side_multiplier, 8)


async def process_shadow_horizon_updates(
    *,
    futures: BinanceFuturesClient,
    horizon_labels_file: Path,
    shadow_outcomes_file: Path,
    shadow_state_file: Path,
    horizon_labels_offset: int,
    pending_by_event_id: dict[str, dict[str, Any]],
    round_trip_fee_bps: float,
) -> int:
    if not horizon_labels_file.exists():
        return horizon_labels_offset
    try:
        current_size = horizon_labels_file.stat().st_size
    except OSError:
        return horizon_labels_offset
    if current_size < horizon_labels_offset:
        horizon_labels_offset = 0
    with horizon_labels_file.open("r", encoding="utf-8") as handle:
        handle.seek(horizon_labels_offset)
        while True:
            line = handle.readline()
            if not line:
                horizon_labels_offset = handle.tell()
                break
            horizon_labels_offset = handle.tell()
            raw_line = line.strip()
            if not raw_line:
                continue
            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict) or str(record.get("event") or "") != "market_event_horizon_labeled":
                continue
            event_id = str(record.get("event_id") or "")
            matching_shadow_items = pending_shadow_items_for_horizon_event_id(
                pending_by_event_id,
                horizon_event_id=event_id,
            )
            if not matching_shadow_items:
                continue
            horizon = record.get("horizon") if isinstance(record.get("horizon"), dict) else {}
            horizon_sec = int(horizon.get("horizon_sec") or 0)
            for pending_event_id, shadow_signal in matching_shadow_items:
                target_horizons = shadow_target_horizons(shadow_signal)
                if horizon_sec not in target_horizons:
                    continue
                shadow_branches = normalize_shadow_branches(shadow_signal)
                if not shadow_branches:
                    continue
                shadow_event_id = str(shadow_signal.get("event_id") or pending_event_id or event_id)
                source_event_id = str(shadow_signal.get("source_event_id") or event_id)
                entry_time_ms = int(
                    shadow_signal.get("entry_time_ms")
                    or shadow_signal.get("anchor_close_time_ms")
                    or shadow_signal.get("observed_at_ms")
                    or record.get("observed_at_ms")
                    or 0
                )
                if shadow_signal.get("entry_time_ms"):
                    deadline_ms = entry_time_ms + horizon_sec * 1000
                else:
                    deadline_ms = int(horizon.get("deadline_ms") or (entry_time_ms + horizon_sec * 1000))
                try:
                    candles_1m = await fetch_shadow_path_candles(
                        futures=futures,
                        symbol=str(shadow_signal.get("symbol") or ""),
                        entry_time_ms=entry_time_ms,
                        deadline_ms=deadline_ms,
                    )
                except Exception as exc:  # noqa: BLE001
                    candles_1m = []
                    emit_event(
                        "signal_bridge_shadow_path_fetch_failed",
                        event_id=shadow_event_id,
                        source_event_id=source_event_id,
                        symbol=shadow_signal.get("symbol"),
                        horizon_sec=horizon_sec,
                        error=str(exc),
                    )
                event_complete = True
                for branch in shadow_branches:
                    completed_horizons = {
                        int(value)
                        for value in (branch.get("completed_horizons_sec") or [])
                        if int(value) > 0
                    }
                    if horizon_sec in completed_horizons:
                        continue
                    branch_side = str(branch.get("side") or shadow_signal.get("side") or "").upper()
                    if branch_side not in {"BUY", "SELL"}:
                        completed_horizons.add(horizon_sec)
                        branch["completed_horizons_sec"] = sorted(completed_horizons)
                        continue
                    branch_research_only = bool(branch.get("research_only")) or is_research_shadow_row(shadow_signal)
                    if shadow_signal.get("strategy_family") == MOMENTUM_SCALP_STRATEGY_FAMILY:
                        momentum_candles = momentum_scalp_candles_from_klines(
                            candles_1m,
                            radar=shadow_signal,
                        )
                        outcome_payload = simulate_momentum_scalp_exit(
                            shadow_signal,
                            branch,
                            momentum_candles,
                            round_trip_fee_bps=round_trip_fee_bps,
                        )
                        outcome_payload.update(
                            {
                                "recorded_at": now_iso(),
                                "event_id": shadow_event_id,
                                "source_event_id": source_event_id,
                                "horizon_event_id": event_id,
                                "horizon_sec": horizon_sec,
                                "shadow_instance_id": build_shadow_instance_id(
                                    shadow_event_id,
                                    str(branch.get("branch_id") or branch.get("strategy_id") or "LEGACY"),
                                ),
                                "shadow_branch_id": branch.get("branch_id") or branch.get("strategy_id"),
                                "branch_type": "one_min_momentum_scalp_plus",
                                "research_only": True,
                                "live_unlock_eligible": False,
                                "readiness_eligible": False,
                                "live_trading_enabled": False,
                                "promotion_allowed": False,
                            }
                        )
                        append_jsonl(shadow_outcomes_file, outcome_payload)
                        emit_event(
                            "signal_bridge_momentum_scalp_shadow_outcome",
                            event_id=shadow_event_id,
                            source_event_id=source_event_id,
                            symbol=shadow_signal.get("symbol"),
                            strategy_id=outcome_payload.get("strategy_id"),
                            entry_profile=outcome_payload.get("entry_profile"),
                            exit_profile=outcome_payload.get("exit_profile"),
                            horizon_sec=horizon_sec,
                            final_exit_reason=outcome_payload.get("final_exit_reason"),
                            after_real_cost_return_pct=outcome_payload.get("after_real_cost_return_pct"),
                            live_trading_enabled=False,
                            promotion_allowed=False,
                        )
                        completed_horizons.add(horizon_sec)
                        branch["completed_horizons_sec"] = sorted(completed_horizons)
                        if not completed_horizons.issuperset(target_horizons):
                            event_complete = False
                        continue
                    outcome_payload = {
                        "event": "signal_bridge_shadow_horizon_result",
                        "recorded_at": now_iso(),
                        "event_id": shadow_event_id,
                        "source_event_id": source_event_id,
                        "horizon_event_id": event_id,
                        "symbol": shadow_signal.get("symbol"),
                        "playbook": shadow_signal.get("playbook"),
                        "side": branch_side,
                        "base_side": shadow_signal.get("side"),
                        "direction_variant": branch.get("direction_variant"),
                        "branch_type": normalize_branch_type(shadow_signal.get("branch_type")),
                        "research_only": branch_research_only,
                        "live_unlock_eligible": False
                        if branch_research_only
                        else bool(shadow_signal.get("live_unlock_eligible", True)),
                        "shadow_reason": shadow_signal.get("shadow_reason"),
                        "effective_quote_allocation_usdt": shadow_signal.get("effective_quote_allocation_usdt"),
                        "virtual_intent": shadow_signal.get("virtual_intent"),
                        "funding_rate_at_entry": shadow_signal.get("funding_rate_at_entry"),
                        "funding_paid_during_hold": estimate_funding_paid_during_hold(
                            shadow_signal,
                            side=branch_side,
                            horizon_sec=horizon_sec,
                        ),
                        "funding_paid_source": "paper_prorated_entry_rate",
                        "spread_bps_at_entry": shadow_signal.get("spread_bps_at_entry"),
                        "order_latency_ms": shadow_signal.get("order_latency_ms"),
                        "estimated_slippage_bps": shadow_signal.get("estimated_slippage_bps"),
                        "maker_or_taker": shadow_signal.get("maker_or_taker") or "paper_taker",
                        "liquidity_bucket": shadow_signal.get("liquidity_bucket") or "unknown",
                        "factors": shadow_signal.get("factors") if isinstance(shadow_signal.get("factors"), dict) else {},
                        "horizon_sec": horizon_sec,
                        **simulate_shadow_branch_outcome(
                            event_id=shadow_event_id,
                            branch=branch,
                            side=branch_side,
                            entry_price=safe_float(shadow_signal.get("entry_price"))
                            or safe_float(shadow_signal.get("sample_price"))
                            or 0.0,
                            entry_time_ms=entry_time_ms,
                            horizon_sec=horizon_sec,
                            candles_1m=candles_1m,
                            fallback_close_price=safe_float(horizon.get("close_price")),
                            fallback_max_drawdown_pct=safe_float(horizon.get("max_drawdown_pct")),
                            fallback_max_runup_pct=safe_float(horizon.get("max_runup_pct")),
                            round_trip_fee_bps=round_trip_fee_bps,
                            extra_slippage_penalty_pct=safe_float(shadow_signal.get("extra_slippage_penalty_pct")) or 0.0,
                        ),
                    }
                    append_jsonl(shadow_outcomes_file, outcome_payload)
                    emit_event(
                        "signal_bridge_shadow_horizon_result",
                        event_id=shadow_event_id,
                        source_event_id=source_event_id,
                        symbol=shadow_signal.get("symbol"),
                        playbook=shadow_signal.get("playbook"),
                        side=branch_side,
                        branch_type=outcome_payload.get("branch_type"),
                        research_only=branch_research_only,
                        horizon_sec=horizon_sec,
                        shadow_branch_id=outcome_payload.get("shadow_branch_id"),
                        after_fee_return_pct=outcome_payload.get("after_fee_return_pct"),
                        after_fee_and_slippage_return_pct=outcome_payload.get("after_fee_and_slippage_return_pct"),
                        close_return_pct=outcome_payload.get("close_return_pct"),
                        max_drawdown_pct=outcome_payload.get("max_drawdown_pct"),
                        exit_reason=outcome_payload.get("exit_reason"),
                    )
                    completed_horizons.add(horizon_sec)
                    branch["completed_horizons_sec"] = sorted(completed_horizons)
                    if not completed_horizons.issuperset(target_horizons):
                        event_complete = False
                shadow_signal["shadow_branches"] = shadow_branches
                if event_complete and all(
                    {
                        int(value)
                        for value in (branch.get("completed_horizons_sec") or [])
                        if int(value) > 0
                    }.issuperset(target_horizons)
                    for branch in shadow_branches
                ):
                    pending_by_event_id.pop(pending_event_id, None)
    save_shadow_state(
        shadow_state_file,
        horizon_labels_file=horizon_labels_file,
        horizon_labels_offset=horizon_labels_offset,
        pending_by_event_id=pending_by_event_id,
    )
    return horizon_labels_offset


async def execute_signal_trade(
    *,
    futures: BinanceFuturesClient,
    executor: PhoenixExecutor,
    settings: Any,
    record: dict[str, Any],
    playbook: str,
    side: str,
    max_open_positions: int,
    low_balance_threshold_usdt: float,
    low_balance_quote_allocation_usdt: float,
    oms_state: UserDataOMSState | None = None,
    entry_reconcile_timeout_sec: float = 0.0,
) -> dict[str, Any]:
    effective_executor = clone_executor_with_settings(executor, settings)
    symbol = str(record.get("symbol") or "").upper()
    account_api_mode = await futures.get_account_api_mode()
    position_mode = settings.position_mode
    margin_type_override = None
    if account_api_mode == "portfolio_margin":
        account_config = await futures.um_account_config()
        position_mode = "HEDGE" if bool(account_config.get("dualSidePosition")) else "ONE_WAY"
        raw_symbol_config = await futures.um_symbol_config(symbol)
        symbol_config = extract_symbol_config(raw_symbol_config, symbol)
        margin_type_value = symbol_config.get("marginType") if symbol_config is not None else None
        if margin_type_value not in (None, ""):
            margin_type_override = str(margin_type_value).upper()

    account_overview = await futures.account_overview()
    available_balance = extract_available_balance(account_overview)
    positions = await futures.position_information_v3()
    open_symbols = open_position_symbols(positions)
    if max_open_positions > 0 and len(open_symbols) >= max_open_positions:
        raise RuntimeError(
            f"max_open_positions gate blocked {symbol}: open={open_symbols} max_allowed={max_open_positions}"
        )
    if has_open_position(positions, symbol):
        raise RuntimeError(f"{symbol} already has an open position; refusing overlapping bridge entry.")

    effective_quote_allocation = resolve_effective_quote_allocation_usdt(
        base_quote_allocation_usdt=settings.quote_allocation_usdt,
        available_balance_usdt=available_balance,
        low_balance_threshold_usdt=low_balance_threshold_usdt,
        low_balance_quote_allocation_usdt=low_balance_quote_allocation_usdt,
    )
    intent = await effective_executor.build_trade_intent(
        symbol=symbol,
        side=side,
        quote_allocation_usdt=effective_quote_allocation,
        leverage=settings.leverage,
        available_balance_usdt=available_balance,
        margin_type_override=margin_type_override,
    )
    if available_balance is not None and available_balance < intent.quote_allocation_usdt:
        raise RuntimeError(
            f"Available balance {available_balance:.4f} is smaller than requested allocation {intent.quote_allocation_usdt:.4f}."
        )

    symbol_setup = await apply_symbol_account_setup(
        futures,
        symbol=symbol,
        leverage=int(intent.leverage),
        margin_type=intent.margin_type,
        account_api_mode=account_api_mode,
    )
    reconciled_intent = intent
    plan = effective_executor.build_order_plan(
        intent,
        account_api_mode=account_api_mode,
        position_mode=position_mode,
    )
    entry_instruction = find_instruction(plan, "entry_market")
    protective_stop = find_instruction(plan, "initial_protective_stop")
    take_profit_instruction = find_optional_instruction(plan, "initial_take_profit")
    remaining = [
        item
        for item in plan
        if item.name not in {"entry_market", "initial_protective_stop", "initial_take_profit"}
    ]

    entry_response = await place_instruction(futures, entry_instruction)
    entry_response_state = extract_order_response_state(entry_response if isinstance(entry_response, dict) else {})
    stream_entry_state = None
    if (
        oms_state is not None
        and entry_response_state is not None
        and not entry_response_state.is_filled
        and float(entry_reconcile_timeout_sec or 0.0) > 0
    ):
        stream_entry_state = await oms_state.wait_for_update(
            order_id=entry_response_state.order_id,
            client_order_id=entry_response_state.client_order_id,
            timeout_sec=float(entry_reconcile_timeout_sec),
        )
    reconciled_entry_state = choose_reconciled_entry_state(entry_response_state, stream_entry_state)
    entry_reconciliation_action = classify_entry_reconciliation(reconciled_entry_state)
    entry_cancel_residual_response = None
    entry_cancel_residual_error = None
    if entry_reconciliation_action == "no_fill_cancel_residual":
        if (
            reconciled_entry_state is not None
            and (reconciled_entry_state.order_id is not None or reconciled_entry_state.client_order_id)
        ):
            try:
                entry_cancel_residual_response = await futures.cancel_order(
                    symbol,
                    order_id=reconciled_entry_state.order_id,
                    client_order_id=reconciled_entry_state.client_order_id,
                )
            except Exception as exc:  # noqa: BLE001
                entry_cancel_residual_error = str(exc)
        raise RuntimeError(
            "Entry market order produced no executed quantity; residual entry was cancelled before arming protection. "
            f"cancel_response={entry_cancel_residual_response} cancel_error={entry_cancel_residual_error}"
        )
    if (
        reconciled_entry_state is not None
        and entry_reconciliation_action == "partial_fill_cancel_residual_sync_protection"
    ):
        if reconciled_entry_state.order_id is not None or reconciled_entry_state.client_order_id:
            try:
                entry_cancel_residual_response = await futures.cancel_order(
                    symbol,
                    order_id=reconciled_entry_state.order_id,
                    client_order_id=reconciled_entry_state.client_order_id,
                )
            except Exception as exc:  # noqa: BLE001
                entry_cancel_residual_error = str(exc)
        reconciled_intent = build_reconciled_intent(intent, reconciled_entry_state)
        plan = effective_executor.build_order_plan(
            reconciled_intent,
            account_api_mode=account_api_mode,
            position_mode=position_mode,
        )
        plan = sync_plan_quantities_to_fill(plan, reconciled_entry_state)
        entry_instruction = find_instruction(plan, "entry_market")
        protective_stop = find_instruction(plan, "initial_protective_stop")
        take_profit_instruction = find_optional_instruction(plan, "initial_take_profit")
        remaining = [
            item
            for item in plan
            if item.name not in {"entry_market", "initial_protective_stop", "initial_take_profit"}
        ]
    elif reconciled_entry_state is not None:
        plan = sync_plan_quantities_to_fill(plan, reconciled_entry_state)
        protective_stop = find_instruction(plan, "initial_protective_stop")
        take_profit_instruction = find_optional_instruction(plan, "initial_take_profit")
        remaining = [
            item
            for item in plan
            if item.name not in {"entry_market", "initial_protective_stop", "initial_take_profit"}
        ]
    try:
        stop_response = await place_instruction(futures, protective_stop)
    except Exception as exc:  # noqa: BLE001
        emergency = await emergency_flatten_after_failure(
            futures=futures,
            executor=executor,
            intent=reconciled_intent,
            position_mode=position_mode,
            account_api_mode=account_api_mode,
        )
        raise RuntimeError(
            f"Entry filled but initial protective stop failed for {symbol}. emergency={json.dumps(emergency, ensure_ascii=False)}"
        ) from exc

    take_profit_response = None
    if take_profit_instruction is not None:
        try:
            take_profit_response = await place_instruction(futures, take_profit_instruction)
        except Exception as exc:  # noqa: BLE001
            emergency = await emergency_flatten_after_failure(
                futures=futures,
                executor=executor,
                intent=reconciled_intent,
                position_mode=position_mode,
                account_api_mode=account_api_mode,
            )
            raise RuntimeError(
                f"Entry and stop succeeded but take profit failed for {symbol}. emergency={json.dumps(emergency, ensure_ascii=False)}"
            ) from exc

    worker = None
    worker_error = None
    try:
        worker = spawn_post_fill_worker(
            confirmation_token=None,
            account_api_mode=account_api_mode,
            position_mode=position_mode,
            intent=reconciled_intent,
            candidate=summarize_signal(record, playbook=playbook, side=side),
            strategy_gate={"applied": False, "ok": True, "reason": "signal_bridge"},
            dispatch_reason=f"signal_bridge:{playbook}",
            entry_response=entry_response,
            initial_stop_response=stop_response,
            initial_take_profit_response=take_profit_response,
            remaining_plan=remaining,
            settings=settings,
        )
    except Exception as exc:  # noqa: BLE001
        worker_error = str(exc)

    return {
        "environment": futures.environment.name,
        "symbol": symbol,
        "side": side,
        "playbook": playbook,
        "event_id": record.get("event_id"),
        "trigger_types": list(record.get("trigger_types") or []),
        "account_api_mode": account_api_mode,
        "position_mode": position_mode,
        "available_balance_usdt": available_balance,
        "effective_quote_allocation_usdt": effective_quote_allocation,
        "open_symbols_before": open_symbols,
        "symbol_setup": symbol_setup,
        "intent": reconciled_intent.to_dict(),
        "oms_entry_reconciliation": {
            "action": entry_reconciliation_action,
            "entry_response_status": entry_response_state.status if entry_response_state is not None else None,
            "stream_status": stream_entry_state.status if stream_entry_state is not None else None,
            "executed_qty": reconciled_entry_state.executed_qty if reconciled_entry_state is not None else None,
            "executed_qty_text": (
                reconciled_entry_state.executed_qty_text if reconciled_entry_state is not None else None
            ),
            "avg_price": reconciled_entry_state.avg_price if reconciled_entry_state is not None else None,
            "cancel_residual_response": entry_cancel_residual_response,
            "cancel_residual_error": entry_cancel_residual_error,
        },
        "entry_market": entry_response,
        "initial_protective_stop": stop_response,
        "initial_take_profit": take_profit_response,
        "remaining_plan": [item.to_dict() for item in remaining],
        "worker": worker,
        "worker_error": worker_error,
    }


async def bridge_loop(args: argparse.Namespace) -> int:
    execution_mode = validate_execution_mode_args(args)
    mainnet_shadow = execution_mode == EXECUTION_MODE_MAINNET_SHADOW
    snapshots_file = resolve_snapshots_file(args.snapshots_file)
    mainnet_shadow_dir = resolve_mainnet_shadow_dir(snapshots_file, args.mainnet_shadow_dir) if mainnet_shadow else None
    if mainnet_shadow_dir is not None:
        mainnet_shadow_dir.mkdir(parents=True, exist_ok=True)
    state_file = resolve_state_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.state_file, "bridge-state.json"),
    )
    state_file.parent.mkdir(parents=True, exist_ok=True)
    shadow_log_file = resolve_shadow_log_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.shadow_log_file, "signal_bridge_shadow_signals.jsonl"),
    )
    shadow_outcomes_file = resolve_shadow_outcomes_file(shadow_log_file)
    shadow_state_file = resolve_shadow_state_file(state_file)
    horizon_labels_file = resolve_horizon_labels_file(snapshots_file)
    live_report_file = resolve_live_report_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.live_report_file, "Live_Performance_Report.txt"),
    )
    total_sample_file = resolve_total_sample_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.total_sample_file, "Total_Sample_V9_Final.json"),
    )
    readiness_report_file = resolve_runtime_file(
        mainnet_shadow_dir,
        args.shadow_readiness_report_file,
        "mainnet_shadow_readiness.json",
    )
    active_strategy_file = resolve_active_strategy_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.active_strategy_file, "active_strategy.json"),
    )
    candidate_strategy_report_file = resolve_candidate_strategy_report_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.candidate_strategy_report_file, "candidate_strategy_report.json"),
    )
    candidate_strategy_control_file = resolve_candidate_strategy_control_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.candidate_strategy_control_file, "candidate_strategy_control.json"),
    )
    strategy_shadow_league_report_file = resolve_strategy_shadow_league_report_file(
        snapshots_file,
        resolve_runtime_file(
            mainnet_shadow_dir,
            args.strategy_shadow_league_report_file,
            "strategy_shadow_league_report.json",
        ),
    )
    strategy_shadow_league_report_md_file = resolve_strategy_shadow_league_report_md_file(
        snapshots_file,
        resolve_runtime_file(
            mainnet_shadow_dir,
            args.strategy_shadow_league_report_md_file,
            "strategy_shadow_league_report.md",
        ),
    )
    candidate_parking_lot_file = resolve_runtime_file(
        mainnet_shadow_dir,
        args.candidate_parking_lot_file,
        "candidate_parking_lot.jsonl",
    )
    factor_factory_report_file = resolve_factor_factory_report_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.factor_factory_report_file, "factor_factory_report.json"),
    )
    factor_factory_control_file = resolve_factor_factory_control_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.factor_factory_control_file, "factor_factory_control.json"),
    )
    bias_audit_report_file = resolve_bias_audit_report_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.bias_audit_report_file, "bias_audit_report.json"),
    )
    execution_realism_report_file = resolve_execution_realism_report_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.execution_realism_report_file, "execution_realism_report.json"),
    )
    promotion_gate_report_file = resolve_promotion_gate_report_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.promotion_gate_report_file, "promotion_gate_report.json"),
    )
    backtest_report_file = resolve_backtest_report_file(
        mainnet_shadow_dir,
        resolve_runtime_file(mainnet_shadow_dir, args.backtest_report_file, "backtest_report.json"),
    )
    emergency_tune_trigger_file = resolve_emergency_tune_trigger_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.emergency_tune_trigger_file, "emergency_tune.trigger"),
    )
    global_risk_lock_file = resolve_global_risk_lock_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.global_risk_lock_file, "global_risk_lock.json"),
    )
    user_data_oms_status_file = resolve_user_data_oms_status_file(
        snapshots_file,
        resolve_runtime_file(mainnet_shadow_dir, args.user_data_oms_status_file, "user_data_oms_state.json"),
    )
    real_trade_outcomes_file = resolve_real_trade_outcomes_file(
        resolve_runtime_file(mainnet_shadow_dir, args.real_trade_outcomes_file, "real_trade_outcomes.jsonl")
    )
    bridge_instance_lock_file = resolve_runtime_file(mainnet_shadow_dir, None, "bridge-instance.lock") or state_file.with_name(
        "bridge-instance.lock"
    )
    bridge_instance_lock = acquire_bridge_instance_lock(bridge_instance_lock_file)
    allowed_playbooks = parse_allowed_playbooks(args.allowed_playbooks)
    candidate_writes_enabled = candidate_shadow_writes_enabled(args)
    candidate_parking_lot_only = candidate_parking_lot_enabled(args)
    momentum_scalp_config = build_momentum_scalp_config_from_args(args)
    blocked_symbols = parse_symbol_set(args.blocked_symbols)
    credentials = load_credentials(required=True)
    environment = resolve_environment(args.env)
    proxy_settings = load_proxy_settings()
    base_settings = load_execution_settings()
    settings = replace(
        base_settings,
        quote_allocation_usdt=float(args.quote_allocation),
        leverage=max(1, int(args.leverage)),
        initial_stop_loss_pct=max(0.01, float(args.stop_loss_pct)),
        take_profit_pct=max(0.0, float(args.take_profit_pct)),
        max_open_positions=max(0, int(args.max_open_positions)),
    )
    shadow_branch_specs = parse_shadow_branch_specs(args.shadow_branch_specs)
    research_shadow_target_horizons_sec = parse_positive_int_csv(
        args.research_shadow_target_horizons_sec,
        default=DEFAULT_RESEARCH_SHADOW_TARGET_HORIZONS_SEC,
    )
    offset = load_state_offset(state_file, snapshots_file, from_start=bool(args.from_start))
    horizon_labels_offset, pending_shadow_by_event_id = load_shadow_state(
        shadow_state_file,
        horizon_labels_file=horizon_labels_file,
    )
    stop_requested = False
    processed_event_ids: set[str] = set()
    processed_queue: deque[str] = deque(maxlen=DEFAULT_PROCESSED_EVENT_CAP)
    last_dispatch_by_symbol: dict[str, float] = {}
    tradable_symbols: set[str] = set()
    tradable_rank_by_symbol: dict[str, int] = {}
    tradable_quote_volume_by_symbol: dict[str, float] = {}
    live_market_stats_by_symbol: dict[str, dict[str, float | None]] = {}
    symbol_quote_volume_1h_by_symbol: dict[str, float] = {}
    symbol_quote_volume_1h_refreshed_at_by_symbol: dict[str, float] = {}
    exchange_info_payload: dict[str, Any] | None = None
    last_live_universe_refresh_monotonic = 0.0
    next_live_report_monotonic = 0.0
    next_memory_cleanup_monotonic = 0.0
    next_emergency_drawdown_check_monotonic = 0.0
    last_state_offset_flush_monotonic = 0.0
    last_emergency_tune_trigger_monotonic = 0.0
    last_real_trade_outcome_key = None
    last_real_stop_loss_trigger_key = None
    lines_since_state_offset_flush = 0
    active_strategy_payload: dict[str, Any] = {}
    active_strategy_mtime_ns: int | None = None
    candidate_strategy_control_payload: dict[str, Any] = read_json_object_file(candidate_strategy_control_file) or {}
    equity_history: deque[dict[str, Any]] = deque(maxlen=64)
    btc_regime_cache: dict[str, Any] = {}
    pending_oi_unwind_reversal_by_event_id: dict[str, dict[str, Any]] = {}
    pending_momentum_scalp_confirmation_by_event_id: dict[str, dict[str, Any]] = {}
    last_momentum_scalp_fast_signal_ms_by_symbol: dict[str, int] = {}
    global_risk_lock_payload = read_global_risk_lock(global_risk_lock_file)
    user_data_oms_state = UserDataOMSState()
    user_data_oms_stop_event: asyncio.Event | None = None
    user_data_oms_task: asyncio.Task[None] | None = None
    live_universe_ready = (
        max(0, int(args.live_universe_top)) <= 0
        and max(0.0, float(args.min_24h_quote_volume)) <= 0
    )

    def flush_state_offset(*, force: bool = False) -> None:
        nonlocal last_state_offset_flush_monotonic, lines_since_state_offset_flush
        if not force and lines_since_state_offset_flush <= 0:
            return
        now_monotonic = time.monotonic()
        if (
            not force
            and lines_since_state_offset_flush < DEFAULT_STATE_OFFSET_FLUSH_LINE_BATCH
            and (now_monotonic - last_state_offset_flush_monotonic) < DEFAULT_STATE_OFFSET_FLUSH_INTERVAL_SEC
        ):
            return
        save_state_offset(state_file, snapshots_file, offset)
        last_state_offset_flush_monotonic = now_monotonic
        lines_since_state_offset_flush = 0

    def _request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True

    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM) if hasattr(signal, "SIGTERM") else None
    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_stop)

    timeout = aiohttp.ClientTimeout(total=60, sock_connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        futures = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=credentials,
            proxy_settings=proxy_settings,
        )
        executor = PhoenixExecutor(futures_client=futures, settings=settings)
        await futures.sync_server_time()

        def write_candidate_parking_lot(payload: dict[str, Any]) -> None:
            if candidate_parking_lot_file is None:
                return
            append_jsonl(candidate_parking_lot_file, payload)
            emit_event(
                "signal_bridge_candidate_parking_lot_written",
                event_id=payload.get("event_id"),
                source_event_id=payload.get("source_event_id"),
                symbol=payload.get("symbol"),
                playbook=payload.get("playbook"),
                side=payload.get("side"),
                branch_type=payload.get("branch_type"),
                reason=payload.get("reason"),
                parking_lot_file=str(candidate_parking_lot_file),
            )

        def queue_pending_momentum_scalp_confirmation(record: dict[str, Any], radar: dict[str, Any]) -> None:
            source_event_id = str(record.get("event_id") or "")
            signal_time_ms = int(safe_float(radar.get("signal_time_ms")) or 0)
            if not source_event_id or signal_time_ms <= 0:
                return
            pending_key = f"{source_event_id}::{MOMENTUM_SCALP_STRATEGY_FAMILY}::confirm"
            ready_at_ms = signal_time_ms + int(momentum_scalp_config.confirmation_min_delay_sec) * 1000
            expire_at_ms = signal_time_ms + int(momentum_scalp_config.confirmation_max_delay_sec) * 1000
            pending_momentum_scalp_confirmation_by_event_id[pending_key] = {
                "record": record,
                "radar": radar,
                "ready_at_ms": ready_at_ms,
                "expire_at_ms": expire_at_ms,
            }
            emit_event(
                "signal_bridge_momentum_scalp_confirmation_pending",
                event_id=pending_key,
                source_event_id=source_event_id,
                symbol=record.get("symbol"),
                ready_at_ms=ready_at_ms,
                expire_at_ms=expire_at_ms,
                live_trading_enabled=False,
                promotion_allowed=False,
            )

        async def process_pending_momentum_scalp_confirmations() -> None:
            if bool(args.disable_momentum_scalp_plus):
                pending_momentum_scalp_confirmation_by_event_id.clear()
                return
            if not pending_momentum_scalp_confirmation_by_event_id:
                return
            now_timestamp_ms = int(time.time() * 1000)
            for pending_key, payload in list(pending_momentum_scalp_confirmation_by_event_id.items()):
                ready_at_ms = int(payload.get("ready_at_ms") or 0)
                expire_at_ms = int(payload.get("expire_at_ms") or 0)
                if now_timestamp_ms < ready_at_ms:
                    continue
                pending_record = payload.get("record") if isinstance(payload.get("record"), dict) else {}
                radar = payload.get("radar") if isinstance(payload.get("radar"), dict) else {}
                signal_time_ms = int(safe_float(radar.get("signal_time_ms")) or 0)
                end_time_ms = min(max(now_timestamp_ms, ready_at_ms), expire_at_ms)
                confirmation = None
                try:
                    rows = await futures.klines(
                        str(radar.get("symbol") or pending_record.get("symbol") or "").upper(),
                        interval="1m",
                        start_time_ms=signal_time_ms + 1,
                        end_time_ms=end_time_ms + 60_000,
                        limit=8,
                    )
                    confirmation = resolve_momentum_scalp_confirmation(
                        radar,
                        momentum_scalp_candles_from_klines(rows, radar=radar),
                        config=momentum_scalp_config,
                    )
                except Exception as exc:  # noqa: BLE001
                    emit_event(
                        "signal_bridge_momentum_scalp_confirmation_failed",
                        event_id=pending_key,
                        symbol=radar.get("symbol") or pending_record.get("symbol"),
                        error=str(exc),
                    )
                if confirmation is not None:
                    confirm_signals = build_momentum_scalp_signals_from_confirmation(
                        pending_record,
                        radar,
                        confirmation,
                    )
                    written = persist_momentum_scalp_shadow_signals(
                        signals=confirm_signals,
                        shadow_log_file=shadow_log_file,
                        shadow_state_file=shadow_state_file,
                        horizon_labels_file=horizon_labels_file,
                        horizon_labels_offset=horizon_labels_offset,
                        pending_shadow_by_event_id=pending_shadow_by_event_id,
                    )
                    emit_event(
                        "signal_bridge_momentum_scalp_confirm_shadow_logged",
                        event_id=pending_key,
                        source_event_id=pending_record.get("event_id"),
                        symbol=radar.get("symbol"),
                        entry_profile="SCALP_CONFIRM_ENTRY",
                        shadow_signal_count=written,
                        live_trading_enabled=False,
                        promotion_allowed=False,
                    )
                    pending_momentum_scalp_confirmation_by_event_id.pop(pending_key, None)
                    continue
                if now_timestamp_ms >= expire_at_ms:
                    pending_momentum_scalp_confirmation_by_event_id.pop(pending_key, None)
                    emit_event(
                        "signal_bridge_momentum_scalp_confirmation_rejected",
                        event_id=pending_key,
                        source_event_id=pending_record.get("event_id"),
                        symbol=radar.get("symbol"),
                        reason="missing_or_failed_confirmation",
                    )

        async def persist_oi_unwind_reversal_candidate(
            *,
            record: dict[str, Any],
            confirmation: dict[str, Any],
        ) -> None:
            candidate_side = str(confirmation.get("side") or "").upper()
            if candidate_side not in {"BUY", "SELL"}:
                return
            candidate_event_id = str(record.get("event_id") or "")
            oi_unwind_extra_fields = {
                "branch_type": BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
                "research_only": True,
                "shadow_only": True,
                "live_trading_enabled": False,
                "promotion_allowed": False,
                "live_unlock_eligible": False,
                "readiness_eligible": False,
                "exchange_preflight_requested": False,
                "live_order_submission_blocked": True,
                "live_order_block_reason": SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE,
                "candidate_rule": PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
                "candidate_source": "feature_slice_5m_oi_unwind_1m_confirmation",
                "oi_unwind_min_pct": -2.0,
                "oi_unwind_max_pct": -0.5,
                "trigger_direction": confirmation.get("trigger_direction"),
                "confirmation_source": confirmation.get("confirmation_source"),
                "confirmation_candle_direction": confirmation.get("confirmation_candle_direction"),
                "confirmation_close_time_ms": confirmation.get("confirmation_close_time_ms"),
                "confirmation_wait_ms": confirmation.get("confirmation_wait_ms"),
                "entry_price": confirmation.get("entry_price"),
                "entry_time_ms": confirmation.get("confirmation_close_time_ms"),
            }
            branch_specs = build_oi_unwind_reversal_branch_specs(candidate_side)
            if candidate_parking_lot_only or candidate_playbook_is_paused(
                candidate_strategy_control_payload,
                PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
            ):
                write_candidate_parking_lot(
                    build_candidate_parking_lot_payload(
                        reason=(
                            "candidate_parking_lot_only"
                            if candidate_parking_lot_only
                            else "candidate_playbook_auto_paused"
                        ),
                        record=record,
                        playbook=PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
                        side=candidate_side,
                        branch_type=BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
                        candidate_event_id=candidate_event_id,
                        source_event_id=candidate_event_id,
                        shadow_branch_specs=branch_specs,
                        target_horizons_sec=DEFAULT_OI_UNWIND_REVERSAL_TARGET_HORIZONS_SEC,
                        confirmation=confirmation,
                    )
                )
                return
            emit_event(
                "signal_bridge_oi_unwind_reversal_shadow_logged",
                event_id=candidate_event_id,
                symbol=record.get("symbol"),
                playbook=PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
                side=candidate_side,
                branch_type=BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
                confirmation_source=confirmation.get("confirmation_source"),
                confirmation_wait_ms=confirmation.get("confirmation_wait_ms"),
            )
            await persist_shadow_signal(
                futures=futures,
                executor=executor,
                settings=settings,
                record=record,
                playbook=PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
                side=candidate_side,
                failure_reason=SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE,
                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                shadow_branch_specs=branch_specs,
                shadow_log_file=shadow_log_file,
                shadow_state_file=shadow_state_file,
                horizon_labels_file=horizon_labels_file,
                horizon_labels_offset=horizon_labels_offset,
                pending_shadow_by_event_id=pending_shadow_by_event_id,
                target_horizons_sec=DEFAULT_OI_UNWIND_REVERSAL_TARGET_HORIZONS_SEC,
                extra_fields=oi_unwind_extra_fields,
            )

        def queue_pending_oi_unwind_reversal(record: dict[str, Any]) -> bool:
            if not is_oi_unwind_reversal_base(record):
                return False
            sample = record_sample(record)
            event_id = str(record.get("event_id") or "")
            anchor_close_time_ms = int(safe_float(sample.get("anchor_close_time_ms")) or 0)
            if not event_id or anchor_close_time_ms <= 0:
                return False
            ready_at_ms = anchor_close_time_ms + 60_000
            expire_at_ms = anchor_close_time_ms + 4 * 60_000
            now_timestamp_ms = int(time.time() * 1000)
            if now_timestamp_ms > expire_at_ms:
                return False
            pending_oi_unwind_reversal_by_event_id[event_id] = {
                "record": record,
                "ready_at_ms": ready_at_ms,
                "expire_at_ms": expire_at_ms,
            }
            emit_event(
                "signal_bridge_oi_unwind_reversal_pending",
                event_id=event_id,
                symbol=record.get("symbol"),
                ready_at_ms=ready_at_ms,
                expire_at_ms=expire_at_ms,
            )
            return True

        async def process_pending_oi_unwind_reversal_candidates() -> None:
            if not candidate_writes_enabled:
                pending_oi_unwind_reversal_by_event_id.clear()
                return
            if not pending_oi_unwind_reversal_by_event_id:
                return
            now_timestamp_ms = int(time.time() * 1000)
            for pending_event_id, payload in list(pending_oi_unwind_reversal_by_event_id.items()):
                if now_timestamp_ms < int(payload.get("ready_at_ms") or 0):
                    continue
                if now_timestamp_ms > int(payload.get("expire_at_ms") or 0):
                    pending_oi_unwind_reversal_by_event_id.pop(pending_event_id, None)
                    emit_event(
                        "signal_bridge_oi_unwind_reversal_expired",
                        event_id=pending_event_id,
                    )
                    continue
                pending_record = payload.get("record")
                if not isinstance(pending_record, dict):
                    pending_oi_unwind_reversal_by_event_id.pop(pending_event_id, None)
                    continue
                confirmation = await resolve_oi_unwind_reversal_confirmation(
                    futures=futures,
                    record=pending_record,
                )
                pending_oi_unwind_reversal_by_event_id.pop(pending_event_id, None)
                if confirmation is None:
                    emit_event(
                        "signal_bridge_oi_unwind_reversal_rejected",
                        event_id=pending_event_id,
                        symbol=pending_record.get("symbol"),
                        reason="missing_or_failed_1m_confirmation",
                    )
                    continue
                await persist_oi_unwind_reversal_candidate(
                    record=pending_record,
                    confirmation=confirmation,
                )

        async def persist_strategy_discovery_candidate(
            *,
            record: dict[str, Any],
            candidate: dict[str, Any],
        ) -> None:
            candidate_side = str(candidate.get("side") or "").upper()
            playbook_name = str(candidate.get("playbook") or "")
            branch_specs = candidate.get("shadow_branch_specs")
            if candidate_side not in {"BUY", "SELL"} or not isinstance(branch_specs, list) or not branch_specs:
                return
            source_event_id = str(record.get("event_id") or "")
            candidate_event_id = f"{source_event_id}::{BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE}::{playbook_name}"
            if candidate_parking_lot_only or candidate_playbook_is_paused(candidate_strategy_control_payload, playbook_name):
                write_candidate_parking_lot(
                    build_candidate_parking_lot_payload(
                        reason=(
                            "candidate_parking_lot_only"
                            if candidate_parking_lot_only
                            else "candidate_playbook_auto_paused"
                        ),
                        record=record,
                        playbook=playbook_name,
                        side=candidate_side,
                        branch_type=BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
                        candidate_event_id=candidate_event_id,
                        source_event_id=source_event_id,
                        shadow_branch_specs=branch_specs,
                        target_horizons_sec=candidate.get("target_horizons_sec")
                        or DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC,
                        candidate=candidate,
                    )
                )
                emit_event(
                    "signal_bridge_strategy_discovery_shadow_skipped",
                    event_id=candidate_event_id,
                    source_event_id=source_event_id,
                    symbol=record.get("symbol"),
                    playbook=playbook_name,
                    side=candidate_side,
                    reason=(
                        "candidate_parking_lot_only"
                        if candidate_parking_lot_only
                        else "candidate_playbook_auto_paused"
                    ),
                )
                return
            discovery_extra_fields = {
                "event_id": candidate_event_id,
                "source_event_id": source_event_id,
                "horizon_event_id": source_event_id,
                "branch_type": BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
                "research_only": True,
                "shadow_only": True,
                "live_unlock_eligible": False,
                "readiness_eligible": False,
                "exchange_preflight_requested": False,
                "live_order_submission_blocked": True,
                "live_order_block_reason": SHADOW_REASON_STRATEGY_DISCOVERY_CANDIDATE,
                "candidate_rule": playbook_name,
                "candidate_source": candidate.get("candidate_source"),
                "candidate_conditions": candidate.get("candidate_conditions"),
            }
            emit_event(
                "signal_bridge_strategy_discovery_shadow_logged",
                event_id=candidate_event_id,
                source_event_id=source_event_id,
                symbol=record.get("symbol"),
                playbook=playbook_name,
                side=candidate_side,
                branch_type=BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
                candidate_conditions=candidate.get("candidate_conditions"),
            )
            await persist_shadow_signal(
                futures=futures,
                executor=executor,
                settings=settings,
                record=record,
                playbook=playbook_name,
                side=candidate_side,
                failure_reason=SHADOW_REASON_STRATEGY_DISCOVERY_CANDIDATE,
                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                shadow_branch_specs=branch_specs,
                shadow_log_file=shadow_log_file,
                shadow_state_file=shadow_state_file,
                horizon_labels_file=horizon_labels_file,
                horizon_labels_offset=horizon_labels_offset,
                pending_shadow_by_event_id=pending_shadow_by_event_id,
                target_horizons_sec=candidate.get("target_horizons_sec")
                or DEFAULT_STRATEGY_DISCOVERY_TARGET_HORIZONS_SEC,
                extra_fields=discovery_extra_fields,
            )

        user_data_oms_enabled = user_data_oms_enabled_for_mode(
            execution_mode,
            disable_user_data_oms=bool(args.disable_user_data_oms),
        )
        emit_event(
            "signal_bridge_started",
            execution_mode=execution_mode,
            mainnet_shadow_live_locked=mainnet_shadow,
            mainnet_shadow_dir=str(mainnet_shadow_dir) if mainnet_shadow_dir is not None else None,
            bridge_instance_lock_file=str(bridge_instance_lock_file),
            snapshots_file=str(snapshots_file),
            state_file=str(state_file),
            shadow_log_file=str(shadow_log_file),
            shadow_outcomes_file=str(shadow_outcomes_file),
            real_trade_outcomes_file=str(real_trade_outcomes_file),
            horizon_labels_file=str(horizon_labels_file),
            live_report_file=str(live_report_file),
            total_sample_file=str(total_sample_file),
            readiness_report_file=str(readiness_report_file) if readiness_report_file is not None else None,
            active_strategy_file=str(active_strategy_file),
            candidate_strategy_report_file=str(candidate_strategy_report_file),
            candidate_strategy_control_file=str(candidate_strategy_control_file),
            strategy_shadow_league_report_file=str(strategy_shadow_league_report_file),
            strategy_shadow_league_report_md_file=str(strategy_shadow_league_report_md_file),
            candidate_parking_lot_only=candidate_parking_lot_only,
            candidate_parking_lot_file=str(candidate_parking_lot_file) if candidate_parking_lot_file else None,
            factor_factory_report_file=str(factor_factory_report_file),
            factor_factory_control_file=str(factor_factory_control_file),
            bias_audit_report_file=str(bias_audit_report_file),
            execution_realism_report_file=str(execution_realism_report_file),
            promotion_gate_report_file=str(promotion_gate_report_file),
            backtest_report_file=str(backtest_report_file) if backtest_report_file is not None else None,
            factor_factory_limits={
                "max_snapshots": max(0, int(args.factor_factory_max_snapshots)),
                "max_outcomes": max(0, int(args.factor_factory_max_outcomes)),
                "min_samples": max(1, int(args.factor_factory_min_samples)),
                "pair_min_samples": max(1, int(args.factor_factory_pair_min_samples)),
            },
            governance_max_records=max(0, int(args.governance_max_records)),
            candidate_auto_pause_thresholds={
                "min_outcomes": max(1, int(args.candidate_auto_pause_min_outcomes)),
                "pause_profit_factor_below": float(args.candidate_auto_pause_profit_factor),
                "pause_avg_return_pct_below": float(args.candidate_auto_pause_min_avg_return_pct),
                "focus_min_outcomes": max(1, int(args.candidate_focus_min_outcomes)),
                "focus_profit_factor_at_least": float(args.candidate_focus_profit_factor),
            },
            emergency_tune_trigger_file=str(emergency_tune_trigger_file),
            global_risk_lock_file=str(global_risk_lock_file),
            global_risk_lock_active=is_global_risk_lock_active(global_risk_lock_payload),
            user_data_oms_enabled=user_data_oms_enabled,
            user_data_oms_status_file=str(user_data_oms_status_file),
            environment=environment.name,
            credentials_environment=credentials.environment.name,
            allowed_playbooks=sorted(allowed_playbooks),
            discovery_candidates_disabled=not candidate_writes_enabled,
            leverage=settings.leverage,
            quote_allocation_usdt=settings.quote_allocation_usdt,
            stop_loss_pct=settings.initial_stop_loss_pct,
            take_profit_pct=settings.take_profit_pct,
            low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
            low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
            live_universe_top=max(0, int(args.live_universe_top)),
            min_24h_quote_volume=float(args.min_24h_quote_volume),
            min_1h_quote_volume=float(args.min_1h_quote_volume),
            symbol_1h_volume_refresh_sec=float(args.symbol_1h_volume_refresh_sec),
            live_universe_refresh_sec=float(args.live_universe_refresh_sec),
            blocked_symbols=sorted(blocked_symbols),
            min_24h_volatility_pct=float(args.min_24h_volatility_pct),
            live_report_interval_sec=float(args.live_report_interval_sec),
            memory_cleanup_interval_sec=float(args.memory_cleanup_interval_sec),
            emergency_drawdown_window_sec=float(args.emergency_drawdown_window_sec),
            emergency_drawdown_threshold_pct=float(args.emergency_drawdown_threshold_pct),
            emergency_drawdown_check_sec=float(args.emergency_drawdown_check_sec),
            emergency_tune_cooldown_sec=float(args.emergency_tune_cooldown_sec),
            real_stop_loss_streak_length=max(1, int(args.real_stop_loss_streak_length)),
            real_trade_outcome_tail_limit=max(1, int(args.real_trade_outcome_tail_limit)),
            hard_kill_drawdown_threshold_pct=float(args.hard_kill_drawdown_threshold_pct),
            btc_regime_drop_threshold_pct=float(args.btc_regime_drop_threshold_pct),
            btc_regime_refresh_sec=float(args.btc_regime_refresh_sec),
            user_data_oms_reconcile_timeout_sec=float(args.user_data_oms_reconcile_timeout_sec),
            user_data_oms_keepalive_sec=float(args.user_data_oms_keepalive_sec),
            user_data_oms_reconnect_sec=float(args.user_data_oms_reconnect_sec),
            degen_shadow_min_24h_quote_volume=float(args.degen_shadow_min_24h_quote_volume),
            degen_shadow_extra_slippage_pct=float(args.degen_shadow_extra_slippage_pct),
            shadow_branch_specs=shadow_branch_specs,
            shadow_round_trip_fee_bps=float(args.shadow_round_trip_fee_bps),
            momentum_scalp_plus_enabled=not bool(args.disable_momentum_scalp_plus),
            momentum_scalp_plus_config={
                "min_return_pct": float(args.momentum_scalp_min_return_pct),
                "min_range_to_atr": float(args.momentum_scalp_min_range_to_atr),
                "min_volume_burst_ratio": float(args.momentum_scalp_min_volume_burst_ratio),
                "min_oi_change_pct": float(args.momentum_scalp_min_oi_change_pct),
                "allow_missing_one_min_oi_for_shadow": not bool(
                    args.disable_momentum_scalp_missing_one_min_oi_fallback
                ),
                "min_five_min_oi_fallback_pct": float(args.momentum_scalp_min_five_min_oi_fallback_pct),
                "require_one_min_oi_hard_gate": bool(args.require_momentum_scalp_one_min_oi_hard_gate),
                "max_spread_bps": float(args.momentum_scalp_max_spread_bps),
                "max_slippage_bps": float(args.momentum_scalp_max_slippage_bps),
                "symbol_cooldown_sec": int(args.momentum_scalp_symbol_cooldown_sec),
                "live_trading_enabled": False,
                "promotion_allowed": False,
            },
            research_shadow_all_triggers=bool(args.research_shadow_all_triggers),
            readiness_thresholds={
                "min_closed_trades": max(1, int(args.shadow_readiness_min_closed_trades)),
                "min_win_rate_pct": float(args.shadow_readiness_min_win_rate_pct),
                "min_profit_factor": float(args.shadow_readiness_min_profit_factor),
                "min_sharpe_ratio": float(args.shadow_readiness_min_sharpe_ratio),
                "max_drawdown_pct": float(args.shadow_readiness_max_drawdown_pct),
            },
            offset=offset,
        )
        if user_data_oms_enabled:
            user_data_oms_stop_event = asyncio.Event()
            user_data_oms_task = asyncio.create_task(
                user_data_oms_stream_loop(
                    futures=futures,
                    session=session,
                    oms_state=user_data_oms_state,
                    status_file=user_data_oms_status_file,
                    stop_event=user_data_oms_stop_event,
                    keepalive_sec=float(args.user_data_oms_keepalive_sec),
                    reconnect_sec=float(args.user_data_oms_reconnect_sec),
                )
            )
        bootstrap_real_outcomes = load_recent_real_trade_outcomes(
            real_trade_outcomes_file,
            tail_limit=max(1, int(args.real_trade_outcome_tail_limit)),
        )
        if bootstrap_real_outcomes:
            last_real_trade_outcome_key = build_real_trade_outcome_key(bootstrap_real_outcomes[-1])
            emit_event(
                "signal_bridge_real_trade_outcomes_bootstrap",
                real_trade_outcomes_file=str(real_trade_outcomes_file),
                bootstrap_record_count=len(bootstrap_real_outcomes),
                latest_outcome_key=last_real_trade_outcome_key,
            )
        try:
            while not stop_requested:
                await process_pending_momentum_scalp_confirmations()
                await process_pending_oi_unwind_reversal_candidates()
                if max(0, int(args.live_universe_top)) > 0 or max(0.0, float(args.min_24h_quote_volume)) > 0:
                    try:
                        (
                            tradable_symbols,
                            tradable_rank_by_symbol,
                            tradable_quote_volume_by_symbol,
                            live_market_stats_by_symbol,
                            exchange_info_payload,
                            last_live_universe_refresh_monotonic,
                        ) = await refresh_live_tradable_universe(
                            futures=futures,
                            top_limit=max(0, int(args.live_universe_top)),
                            min_quote_volume_24h=float(args.min_24h_quote_volume),
                            refresh_sec=float(args.live_universe_refresh_sec),
                            cached_symbols=tradable_symbols,
                            cached_ranks=tradable_rank_by_symbol,
                            cached_quote_volumes=tradable_quote_volume_by_symbol,
                            cached_market_stats_by_symbol=live_market_stats_by_symbol,
                            exchange_info_payload=exchange_info_payload,
                            last_refresh_monotonic=last_live_universe_refresh_monotonic,
                        )
                        live_universe_ready = True
                    except Exception as exc:  # noqa: BLE001
                        emit_event("signal_bridge_live_universe_refresh_failed", error=str(exc))
                if time.monotonic() >= next_live_report_monotonic:
                    try:
                        updated_candidate_control = await write_live_performance_report(
                            futures=futures,
                            report_file=live_report_file,
                            total_sample_file=total_sample_file,
                            readiness_report_file=readiness_report_file,
                            candidate_strategy_report_file=candidate_strategy_report_file,
                            candidate_strategy_control_file=candidate_strategy_control_file,
                            strategy_shadow_league_report_file=strategy_shadow_league_report_file,
                            strategy_shadow_league_report_md_file=strategy_shadow_league_report_md_file,
                            factor_factory_report_file=factor_factory_report_file,
                            factor_factory_control_file=factor_factory_control_file,
                            bias_audit_report_file=bias_audit_report_file,
                            execution_realism_report_file=execution_realism_report_file,
                            promotion_gate_report_file=promotion_gate_report_file,
                            backtest_report_file=backtest_report_file,
                            shadow_log_file=shadow_log_file,
                            shadow_outcomes_file=shadow_outcomes_file,
                            snapshots_file=snapshots_file,
                            baseline_equity_usdt=float(args.baseline_equity_usdt),
                            readiness_min_closed_trades=max(1, int(args.shadow_readiness_min_closed_trades)),
                            readiness_min_win_rate_pct=float(args.shadow_readiness_min_win_rate_pct),
                            readiness_min_profit_factor=float(args.shadow_readiness_min_profit_factor),
                            readiness_min_sharpe_ratio=float(args.shadow_readiness_min_sharpe_ratio),
                            readiness_max_drawdown_pct=float(args.shadow_readiness_max_drawdown_pct),
                            candidate_auto_pause_min_outcomes=max(1, int(args.candidate_auto_pause_min_outcomes)),
                            candidate_auto_pause_profit_factor=float(args.candidate_auto_pause_profit_factor),
                            candidate_auto_pause_min_avg_return_pct=float(args.candidate_auto_pause_min_avg_return_pct),
                            candidate_focus_min_outcomes=max(1, int(args.candidate_focus_min_outcomes)),
                            candidate_focus_profit_factor=float(args.candidate_focus_profit_factor),
                            factor_factory_max_snapshots=max(0, int(args.factor_factory_max_snapshots)),
                            factor_factory_max_outcomes=max(0, int(args.factor_factory_max_outcomes)),
                            factor_factory_min_samples=max(1, int(args.factor_factory_min_samples)),
                            factor_factory_pair_min_samples=max(1, int(args.factor_factory_pair_min_samples)),
                            governance_max_records=max(0, int(args.governance_max_records)),
                        )
                        if isinstance(updated_candidate_control, dict):
                            candidate_strategy_control_payload = updated_candidate_control
                    except Exception as exc:  # noqa: BLE001
                        emit_event("signal_bridge_live_report_failed", error=str(exc), report_file=str(live_report_file))
                    next_live_report_monotonic = time.monotonic() + max(60.0, float(args.live_report_interval_sec))
                if time.monotonic() >= next_memory_cleanup_monotonic:
                    cleanup_result = best_effort_memory_cleanup()
                    emit_event("signal_bridge_memory_cleanup", **cleanup_result)
                    next_memory_cleanup_monotonic = time.monotonic() + max(
                        60.0,
                        float(args.memory_cleanup_interval_sec),
                    )
                if time.monotonic() >= next_emergency_drawdown_check_monotonic:
                    observed_monotonic = time.monotonic()
                    try:
                        snapshot = await capture_account_equity_snapshot(futures)
                        snapshot["observed_monotonic"] = observed_monotonic
                        equity_history.append(snapshot)
                        prune_equity_history(
                            equity_history,
                            now_monotonic=observed_monotonic,
                            window_sec=float(args.emergency_drawdown_window_sec),
                        )
                        hard_kill_payload = evaluate_emergency_drawdown(
                            equity_history,
                            threshold_pct=float(args.hard_kill_drawdown_threshold_pct),
                            window_sec=float(args.emergency_drawdown_window_sec),
                        )
                        global_risk_lock_payload = read_global_risk_lock(global_risk_lock_file)
                        if (
                            hard_kill_payload is not None
                            and float(args.hard_kill_drawdown_threshold_pct) > 0
                            and not mainnet_shadow
                            and not is_global_risk_lock_active(global_risk_lock_payload)
                        ):
                            hard_kill_payload = {
                                **hard_kill_payload,
                                "type": "hard_kill_equity_drawdown",
                                "action": "cancel_orders_flatten_positions_lock_live_trading",
                            }
                            global_risk_lock_payload = write_global_risk_lock(
                                global_risk_lock_file,
                                payload=hard_kill_payload,
                            )
                            emit_event(
                                "signal_bridge_hard_kill_triggered",
                                lock_file=str(global_risk_lock_file),
                                **hard_kill_payload,
                            )
                            flatten_result = await hard_kill_flatten_account(futures)
                            emit_event(
                                "signal_bridge_hard_kill_flatten_attempted",
                                lock_file=str(global_risk_lock_file),
                                **flatten_result,
                            )
                        drawdown_payload = evaluate_emergency_drawdown(
                            equity_history,
                            threshold_pct=float(args.emergency_drawdown_threshold_pct),
                            window_sec=float(args.emergency_drawdown_window_sec),
                        )
                        if drawdown_payload is not None:
                            if (
                                (observed_monotonic - last_emergency_tune_trigger_monotonic)
                                >= max(30.0, float(args.emergency_tune_cooldown_sec))
                            ):
                                write_emergency_tune_trigger(
                                    emergency_tune_trigger_file,
                                    payload=drawdown_payload,
                                )
                                last_emergency_tune_trigger_monotonic = observed_monotonic
                                emit_event(
                                    "signal_bridge_emergency_tune_triggered",
                                    trigger_file=str(emergency_tune_trigger_file),
                                    **drawdown_payload,
                                )
                    except Exception as exc:  # noqa: BLE001
                        emit_event(
                            "signal_bridge_emergency_drawdown_check_failed",
                            error=str(exc),
                            trigger_file=str(emergency_tune_trigger_file),
                        )
                    try:
                        recent_real_outcomes = load_recent_real_trade_outcomes(
                            real_trade_outcomes_file,
                            tail_limit=max(1, int(args.real_trade_outcome_tail_limit)),
                        )
                        newest_real_outcome_key = (
                            build_real_trade_outcome_key(recent_real_outcomes[-1])
                            if recent_real_outcomes
                            else None
                        )
                        if newest_real_outcome_key and newest_real_outcome_key != last_real_trade_outcome_key:
                            streak_payload = evaluate_real_stop_loss_streak(
                                recent_real_outcomes,
                                streak_length=max(1, int(args.real_stop_loss_streak_length)),
                            )
                            last_real_trade_outcome_key = newest_real_outcome_key
                            if (
                                streak_payload is not None
                                and newest_real_outcome_key != last_real_stop_loss_trigger_key
                                and (observed_monotonic - last_emergency_tune_trigger_monotonic)
                                >= max(30.0, float(args.emergency_tune_cooldown_sec))
                            ):
                                write_emergency_tune_trigger(
                                    emergency_tune_trigger_file,
                                    payload=streak_payload,
                                )
                                last_real_stop_loss_trigger_key = newest_real_outcome_key
                                last_emergency_tune_trigger_monotonic = observed_monotonic
                                emit_event(
                                    "signal_bridge_real_stop_loss_streak_triggered",
                                    trigger_file=str(emergency_tune_trigger_file),
                                    **streak_payload,
                                )
                    except Exception as exc:  # noqa: BLE001
                        emit_event(
                            "signal_bridge_real_trade_outcome_check_failed",
                            error=str(exc),
                            real_trade_outcomes_file=str(real_trade_outcomes_file),
                        )
                    next_emergency_drawdown_check_monotonic = time.monotonic() + max(
                        15.0,
                        float(args.emergency_drawdown_check_sec),
                    )
                horizon_labels_offset = await process_shadow_horizon_updates(
                    futures=futures,
                    horizon_labels_file=horizon_labels_file,
                    shadow_outcomes_file=shadow_outcomes_file,
                    shadow_state_file=shadow_state_file,
                    horizon_labels_offset=horizon_labels_offset,
                    pending_by_event_id=pending_shadow_by_event_id,
                    round_trip_fee_bps=float(args.shadow_round_trip_fee_bps),
                )
                if not snapshots_file.exists():
                    await asyncio.sleep(max(0.1, float(args.poll_interval_sec)))
                    continue
                try:
                    current_size = snapshots_file.stat().st_size
                except OSError:
                    await asyncio.sleep(max(0.1, float(args.poll_interval_sec)))
                    continue
                if current_size < offset:
                    emit_event("signal_bridge_offset_reset", previous_offset=offset, current_size=current_size)
                    offset = 0
                    flush_state_offset(force=True)

                with snapshots_file.open("r", encoding="utf-8") as handle:
                    handle.seek(offset)
                    while not stop_requested:
                        line = handle.readline()
                        if not line:
                            offset = handle.tell()
                            flush_state_offset(force=True)
                            break
                        offset = handle.tell()
                        lines_since_state_offset_flush += 1
                        flush_state_offset()
                        raw_line = line.strip()
                        if not raw_line:
                            continue
                        try:
                            record = json.loads(raw_line)
                        except json.JSONDecodeError as exc:
                            emit_event("signal_bridge_bad_json", offset=offset, error=str(exc))
                            continue
                        if not isinstance(record, dict):
                            continue
                        if str(record.get("event") or "") != "market_event_created":
                            continue
                        event_id = str(record.get("event_id") or "")
                        if event_id:
                            if event_id in processed_event_ids:
                                continue
                            if len(processed_queue) == processed_queue.maxlen:
                                expired = processed_queue.popleft()
                                processed_event_ids.discard(expired)
                            processed_queue.append(event_id)
                            processed_event_ids.add(event_id)
                        symbol = str(record.get("symbol") or "").upper()
                        observed_at_ms = safe_float(record.get("observed_at_ms")) or 0.0
                        signal_age_sec = max(0.0, (time.time() * 1000.0 - observed_at_ms) / 1000.0) if observed_at_ms else 0.0
                        if bool(args.research_shadow_all_triggers):
                            research_shadow_payload = persist_research_shadow_signal(
                                record=record,
                                shadow_log_file=shadow_log_file,
                                shadow_state_file=shadow_state_file,
                                horizon_labels_file=horizon_labels_file,
                                horizon_labels_offset=horizon_labels_offset,
                                pending_shadow_by_event_id=pending_shadow_by_event_id,
                                target_horizons_sec=research_shadow_target_horizons_sec,
                            )
                            emit_event(
                                "signal_bridge_research_shadow_logged",
                                event_id=research_shadow_payload.get("event_id"),
                                source_event_id=research_shadow_payload.get("source_event_id"),
                                symbol=research_shadow_payload.get("symbol"),
                                playbook=research_shadow_payload.get("playbook"),
                                side=research_shadow_payload.get("side"),
                                research_only=research_shadow_payload.get("research_only"),
                                research_observation=research_shadow_payload.get("research_observation"),
                                branch_type=research_shadow_payload.get("branch_type"),
                                branch_count=len(normalize_shadow_branches(research_shadow_payload)),
                                target_horizons_sec=research_shadow_payload.get("shadow_target_horizons_sec"),
                            )
                        if signal_age_sec > max(0.0, float(args.max_signal_age_sec)):
                            emit_event(
                                "signal_bridge_signal_skipped",
                                event_id=event_id,
                                symbol=symbol,
                                reason="stale_signal",
                                signal_age_sec=round(signal_age_sec, 3),
                            )
                            continue
                        if not bool(args.disable_momentum_scalp_plus):
                            momentum_scalp_radar = detect_momentum_scalp_radar_event(
                                record,
                                config=momentum_scalp_config,
                            )
                            if momentum_scalp_radar is not None:
                                now_timestamp_ms = int(time.time() * 1000)
                                fast_signals = build_momentum_scalp_shadow_signals(
                                    record,
                                    config=momentum_scalp_config,
                                    now_ms=now_timestamp_ms,
                                    last_signal_ms_by_symbol=last_momentum_scalp_fast_signal_ms_by_symbol,
                                )
                                written = persist_momentum_scalp_shadow_signals(
                                    signals=fast_signals,
                                    shadow_log_file=shadow_log_file,
                                    shadow_state_file=shadow_state_file,
                                    horizon_labels_file=horizon_labels_file,
                                    horizon_labels_offset=horizon_labels_offset,
                                    pending_shadow_by_event_id=pending_shadow_by_event_id,
                                )
                                if written:
                                    emit_event(
                                        "signal_bridge_momentum_scalp_fast_shadow_logged",
                                        event_id=event_id,
                                        symbol=symbol,
                                        strategy_family=MOMENTUM_SCALP_STRATEGY_FAMILY,
                                        entry_profile="SCALP_FAST_ENTRY",
                                        shadow_signal_count=written,
                                        live_trading_enabled=False,
                                        promotion_allowed=False,
                                    )
                                queue_pending_momentum_scalp_confirmation(record, momentum_scalp_radar)
                        if candidate_writes_enabled:
                            for discovery_candidate in derive_strategy_discovery_candidates(record):
                                await persist_strategy_discovery_candidate(
                                    record=record,
                                    candidate=discovery_candidate,
                                )
                        if candidate_writes_enabled and is_oi_unwind_reversal_base(record):
                            sample_payload = record_sample(record)
                            anchor_close_time_ms = int(safe_float(sample_payload.get("anchor_close_time_ms")) or 0)
                            if anchor_close_time_ms > 0 and int(time.time() * 1000) < anchor_close_time_ms + 60_000:
                                if queue_pending_oi_unwind_reversal(record):
                                    continue
                            oi_unwind_confirmation = await resolve_oi_unwind_reversal_confirmation(
                                futures=futures,
                                record=record,
                            )
                            if oi_unwind_confirmation is not None:
                                await persist_oi_unwind_reversal_candidate(
                                    record=record,
                                    confirmation=oi_unwind_confirmation,
                                )
                                continue
                            if queue_pending_oi_unwind_reversal(record):
                                continue
                        playbook = derive_playbook(record)
                        if playbook not in allowed_playbooks:
                            continue
                        active_strategy_payload, active_strategy_mtime_ns = load_active_strategy_config(
                            active_strategy_file,
                            cached_payload=active_strategy_payload,
                            cached_mtime_ns=active_strategy_mtime_ns,
                        )
                        playbook_strategy = resolve_playbook_strategy_config(
                            active_strategy_payload,
                            playbook=playbook,
                            settings=settings,
                        )
                        strategy_settings = build_strategy_settings(settings, playbook_strategy)
                        strategy_executor = clone_executor_with_settings(executor, strategy_settings)
                        effective_shadow_branch_specs = merge_shadow_branch_specs(
                            shadow_branch_specs,
                            playbook_strategy=playbook_strategy,
                        )
                        side = derive_signal_side(record, playbook=playbook)
                        if side is None:
                            emit_event(
                                "signal_bridge_signal_skipped",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                reason="missing_direction",
                            )
                            continue
                        global_risk_lock_payload = read_global_risk_lock(global_risk_lock_file)
                        if is_global_risk_lock_active(global_risk_lock_payload):
                            trade_shadow_extra_fields = {
                                "branch_type": BRANCH_TYPE_STABLE_CORE,
                                "shadow_only": True,
                                "global_risk_lock_file": str(global_risk_lock_file),
                                "global_risk_lock": global_risk_lock_payload,
                                "strategy_status": playbook_strategy.get("status"),
                                "strategy_sl_pct": playbook_strategy.get("sl"),
                                "strategy_tp_pct": playbook_strategy.get("tp"),
                            }
                            emit_event(
                                "signal_bridge_signal_skipped",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                side=side,
                                reason=SHADOW_REASON_GLOBAL_RISK_LOCK,
                                lock_file=str(global_risk_lock_file),
                            )
                            await persist_shadow_signal(
                                futures=futures,
                                executor=strategy_executor,
                                settings=strategy_settings,
                                record=record,
                                playbook=playbook,
                                side=side,
                                failure_reason=SHADOW_REASON_GLOBAL_RISK_LOCK,
                                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                shadow_branch_specs=effective_shadow_branch_specs,
                                shadow_log_file=shadow_log_file,
                                shadow_state_file=shadow_state_file,
                                horizon_labels_file=horizon_labels_file,
                                horizon_labels_offset=horizon_labels_offset,
                                pending_shadow_by_event_id=pending_shadow_by_event_id,
                                extra_fields=trade_shadow_extra_fields,
                            )
                            continue
                        last_dispatch = float(last_dispatch_by_symbol.get(symbol) or 0.0)
                        cooldown_sec = max(0.0, float(args.symbol_cooldown_sec))
                        if cooldown_sec > 0 and (time.time() - last_dispatch) < cooldown_sec:
                            emit_event(
                                "signal_bridge_signal_skipped",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                reason="symbol_cooldown",
                                cooldown_remaining_sec=round(cooldown_sec - (time.time() - last_dispatch), 3),
                            )
                            continue
                        live_gate_ok, failure_reason, live_gate_extra_fields = evaluate_live_universe_gate(
                            symbol=symbol,
                            live_universe_top=max(0, int(args.live_universe_top)),
                            min_quote_volume_24h=float(args.min_24h_quote_volume),
                            live_universe_ready=live_universe_ready,
                            tradable_symbols=tradable_symbols,
                            tradable_rank_by_symbol=tradable_rank_by_symbol,
                            tradable_quote_volume_by_symbol=tradable_quote_volume_by_symbol,
                        )
                        if not live_gate_ok:
                            emit_event(
                                "signal_bridge_signal_skipped",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                reason=failure_reason,
                            )
                            continue
                        trade_shadow_extra_fields = dict(live_gate_extra_fields)
                        trade_shadow_extra_fields.update(
                            {
                                "branch_type": BRANCH_TYPE_STABLE_CORE,
                                "extra_slippage_penalty_pct": 0.0,
                                "strategy_status": playbook_strategy.get("status"),
                                "strategy_sl_pct": playbook_strategy.get("sl"),
                                "strategy_tp_pct": playbook_strategy.get("tp"),
                                "strategy_generated_at": playbook_strategy.get("generated_at"),
                                "strategy_selected_branch_id": playbook_strategy.get("selected_branch_id"),
                                "strategy_win_rate_pct": playbook_strategy.get("win_rate_pct"),
                                "strategy_status_reason": playbook_strategy.get("status_reason"),
                            }
                        )
                        quote_volume_24h = safe_float(trade_shadow_extra_fields.get("live_universe_quote_volume_24h"))
                        if quote_volume_24h is None:
                            quote_volume_24h = safe_float(
                                (live_market_stats_by_symbol.get(symbol) or {}).get("quote_volume_24h")
                            )
                        if quote_volume_24h is None:
                            sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
                            quote_volume_24h = safe_float(sample.get("quote_volume_24h"))
                        try:
                            quote_volume_1h = await resolve_symbol_quote_volume_1h(
                                futures=futures,
                                symbol=symbol,
                                cache_by_symbol=symbol_quote_volume_1h_by_symbol,
                                refreshed_at_by_symbol=symbol_quote_volume_1h_refreshed_at_by_symbol,
                                refresh_sec=float(args.symbol_1h_volume_refresh_sec),
                            )
                        except Exception as exc:  # noqa: BLE001
                            quote_volume_1h = None
                            emit_event(
                                "signal_bridge_symbol_1h_volume_failed",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                error=str(exc),
                            )
                        liquidity_gate_ok, liquidity_failure_reason, liquidity_extra_fields = evaluate_intraday_liquidity_gate(
                            symbol=symbol,
                            quote_volume_24h=quote_volume_24h,
                            min_quote_volume_24h=float(args.min_24h_quote_volume),
                            quote_volume_1h=quote_volume_1h,
                            min_quote_volume_1h=float(args.min_1h_quote_volume),
                        )
                        trade_shadow_extra_fields.update(liquidity_extra_fields)
                        if not liquidity_gate_ok:
                            emit_event(
                                "signal_bridge_signal_skipped",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                reason=liquidity_failure_reason,
                                quote_volume_24h=quote_volume_24h,
                                quote_volume_1h=quote_volume_1h,
                            )
                            degen_gate_ok, degen_failure_reason, degen_extra_fields = evaluate_degen_shadow_gate(
                                symbol=symbol,
                                quote_volume_24h=quote_volume_24h,
                                min_quote_volume_24h=float(args.degen_shadow_min_24h_quote_volume),
                            )
                            if degen_gate_ok:
                                risk_gate_ok, risk_failure_reason, risk_gate_extra_fields = evaluate_symbol_risk_gate(
                                    symbol=symbol,
                                    record=record,
                                    blocked_symbols=blocked_symbols,
                                    min_24h_volatility_pct=float(args.min_24h_volatility_pct),
                                    live_market_stats_by_symbol=live_market_stats_by_symbol,
                                )
                                degen_shadow_extra_fields = dict(trade_shadow_extra_fields)
                                degen_shadow_extra_fields.update(degen_extra_fields)
                                degen_shadow_extra_fields.update(risk_gate_extra_fields)
                                degen_shadow_extra_fields.update(
                                    {
                                        "branch_type": BRANCH_TYPE_DEGEN_HIGH_YIELD,
                                        "extra_slippage_penalty_pct": max(
                                            0.0,
                                            float(args.degen_shadow_extra_slippage_pct),
                                        ),
                                        "shadow_only": True,
                                        "primary_liquidity_failure_reason": liquidity_failure_reason,
                                    }
                                )
                                if risk_gate_ok:
                                    emit_event(
                                        "signal_bridge_degen_shadow_logged",
                                        event_id=event_id,
                                        symbol=symbol,
                                        playbook=playbook,
                                        side=side,
                                        branch_type=BRANCH_TYPE_DEGEN_HIGH_YIELD,
                                        degen_shadow_min_quote_volume_24h=degen_extra_fields.get(
                                            "degen_shadow_min_quote_volume_24h"
                                        ),
                                        extra_slippage_penalty_pct=float(args.degen_shadow_extra_slippage_pct),
                                    )
                                    await persist_shadow_signal(
                                        futures=futures,
                                        executor=strategy_executor,
                                        settings=strategy_settings,
                                        record=record,
                                        playbook=playbook,
                                        side=side,
                                        failure_reason="degen_high_yield_shadow_branch",
                                        low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                        low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                        shadow_branch_specs=effective_shadow_branch_specs,
                                        shadow_log_file=shadow_log_file,
                                        shadow_state_file=shadow_state_file,
                                        horizon_labels_file=horizon_labels_file,
                                        horizon_labels_offset=horizon_labels_offset,
                                        pending_shadow_by_event_id=pending_shadow_by_event_id,
                                        extra_fields=degen_shadow_extra_fields,
                                    )
                                else:
                                    emit_event(
                                        "signal_bridge_degen_shadow_skipped",
                                        event_id=event_id,
                                        symbol=symbol,
                                        playbook=playbook,
                                        side=side,
                                        reason=risk_failure_reason,
                                        branch_type=BRANCH_TYPE_DEGEN_HIGH_YIELD,
                                    )
                            continue
                        if str(playbook_strategy.get("status") or "active").lower() == "paused":
                            emit_event(
                                "signal_bridge_strategy_paused",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                side=side,
                                reason=SHADOW_REASON_STRATEGY_PAUSED,
                                strategy_win_rate_pct=playbook_strategy.get("win_rate_pct"),
                                strategy_sample_count=playbook_strategy.get("sample_count"),
                            )
                            await persist_shadow_signal(
                                futures=futures,
                                executor=strategy_executor,
                                settings=strategy_settings,
                                record=record,
                                playbook=playbook,
                                side=side,
                                failure_reason=SHADOW_REASON_STRATEGY_PAUSED,
                                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                shadow_branch_specs=effective_shadow_branch_specs,
                                shadow_log_file=shadow_log_file,
                                shadow_state_file=shadow_state_file,
                                horizon_labels_file=horizon_labels_file,
                                horizon_labels_offset=horizon_labels_offset,
                                pending_shadow_by_event_id=pending_shadow_by_event_id,
                                extra_fields=trade_shadow_extra_fields,
                            )
                            continue
                        risk_gate_ok, risk_failure_reason, risk_gate_extra_fields = evaluate_symbol_risk_gate(
                            symbol=symbol,
                            record=record,
                            blocked_symbols=blocked_symbols,
                            min_24h_volatility_pct=float(args.min_24h_volatility_pct),
                            live_market_stats_by_symbol=live_market_stats_by_symbol,
                        )
                        trade_shadow_extra_fields.update(risk_gate_extra_fields)
                        if not risk_gate_ok:
                            emit_event(
                                "signal_bridge_signal_skipped",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                reason=risk_failure_reason,
                                volatility_24h_pct=risk_gate_extra_fields.get("volatility_24h_pct"),
                                min_24h_volatility_pct=risk_gate_extra_fields.get("min_24h_volatility_pct"),
                            )
                            await persist_shadow_signal(
                                futures=futures,
                                executor=strategy_executor,
                                settings=strategy_settings,
                                record=record,
                                playbook=playbook,
                                side=side,
                                failure_reason=str(risk_failure_reason),
                                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                shadow_branch_specs=effective_shadow_branch_specs,
                                shadow_log_file=shadow_log_file,
                                shadow_state_file=shadow_state_file,
                                horizon_labels_file=horizon_labels_file,
                                horizon_labels_offset=horizon_labels_offset,
                                pending_shadow_by_event_id=pending_shadow_by_event_id,
                                extra_fields=trade_shadow_extra_fields,
                            )
                            continue
                        btc_ret_5m_pct, btc_regime_source, btc_regime_error = await resolve_btc_regime_return_5m_pct(
                            futures=futures,
                            record=record,
                            cache=btc_regime_cache,
                            refresh_sec=float(args.btc_regime_refresh_sec),
                        )
                        btc_regime_ok, btc_regime_failure_reason, btc_regime_extra_fields = evaluate_btc_regime_gate(
                            symbol=symbol,
                            side=side,
                            btc_ret_5m_pct=btc_ret_5m_pct,
                            drop_threshold_pct=float(args.btc_regime_drop_threshold_pct),
                        )
                        btc_regime_extra_fields["btc_regime_source"] = btc_regime_source
                        if btc_regime_error is not None:
                            btc_regime_extra_fields["btc_regime_error"] = btc_regime_error
                        trade_shadow_extra_fields.update(btc_regime_extra_fields)
                        if not btc_regime_ok:
                            emit_event(
                                "signal_bridge_signal_skipped",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                side=side,
                                reason=btc_regime_failure_reason or SHADOW_REASON_BTC_REGIME_BLOCKED,
                                btc_regime_ret_5m_pct=btc_ret_5m_pct,
                                btc_regime_source=btc_regime_source,
                                btc_regime_error=btc_regime_error,
                            )
                            await persist_shadow_signal(
                                futures=futures,
                                executor=strategy_executor,
                                settings=strategy_settings,
                                record=record,
                                playbook=playbook,
                                side=side,
                                failure_reason=btc_regime_failure_reason or SHADOW_REASON_BTC_REGIME_BLOCKED,
                                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                shadow_branch_specs=effective_shadow_branch_specs,
                                shadow_log_file=shadow_log_file,
                                shadow_state_file=shadow_state_file,
                                horizon_labels_file=horizon_labels_file,
                                horizon_labels_offset=horizon_labels_offset,
                                pending_shadow_by_event_id=pending_shadow_by_event_id,
                                extra_fields=trade_shadow_extra_fields,
                            )
                            continue
                        if mainnet_shadow:
                            shadow_extra_fields = dict(trade_shadow_extra_fields)
                            shadow_extra_fields.update(
                                {
                                    "execution_mode": EXECUTION_MODE_MAINNET_SHADOW,
                                    "shadow_only": True,
                                    "mainnet_shadow_live_locked": True,
                                    "exchange_preflight_requested": True,
                                    "live_order_submission_blocked": True,
                                    "live_order_block_reason": SHADOW_REASON_MAINNET_SHADOW_LOCKED,
                                }
                            )
                            emit_event(
                                "signal_bridge_mainnet_shadow_signal_locked",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                side=side,
                                reason=SHADOW_REASON_MAINNET_SHADOW_LOCKED,
                            )
                            await persist_shadow_signal(
                                futures=futures,
                                executor=strategy_executor,
                                settings=strategy_settings,
                                record=record,
                                playbook=playbook,
                                side=side,
                                failure_reason=SHADOW_REASON_MAINNET_SHADOW_LOCKED,
                                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                shadow_branch_specs=effective_shadow_branch_specs,
                                shadow_log_file=shadow_log_file,
                                shadow_state_file=shadow_state_file,
                                horizon_labels_file=horizon_labels_file,
                                horizon_labels_offset=horizon_labels_offset,
                                pending_shadow_by_event_id=pending_shadow_by_event_id,
                                extra_fields=shadow_extra_fields,
                            )
                            continue
                        try:
                            trade_result = await execute_signal_trade(
                                futures=futures,
                                executor=strategy_executor,
                                settings=strategy_settings,
                                record=record,
                                playbook=playbook,
                                side=side,
                                max_open_positions=strategy_settings.max_open_positions,
                                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                oms_state=user_data_oms_state if user_data_oms_enabled else None,
                                entry_reconcile_timeout_sec=float(args.user_data_oms_reconcile_timeout_sec),
                            )
                        except Exception as exc:  # noqa: BLE001
                            shadow_reason = classify_trade_error_as_shadow_reason(exc)
                            shadow_extra_fields = dict(trade_shadow_extra_fields)
                            shadow_extra_fields["trade_error"] = str(exc)
                            if shadow_reason is not None:
                                emit_event(
                                    "signal_bridge_trade_degraded_to_shadow",
                                    event_id=event_id,
                                    symbol=symbol,
                                    playbook=playbook,
                                    side=side,
                                    reason=shadow_reason,
                                    error=str(exc),
                                )
                            else:
                                emit_event(
                                    trade_log_event_name(playbook=playbook, outcome="failed"),
                                    event_id=event_id,
                                    symbol=symbol,
                                    playbook=playbook,
                                    side=side,
                                    error=str(exc),
                                )
                            await persist_shadow_signal(
                                futures=futures,
                                executor=strategy_executor,
                                settings=strategy_settings,
                                record=record,
                                playbook=playbook,
                                side=side,
                                failure_reason=shadow_reason or str(exc),
                                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                shadow_branch_specs=effective_shadow_branch_specs,
                                shadow_log_file=shadow_log_file,
                                shadow_state_file=shadow_state_file,
                                horizon_labels_file=horizon_labels_file,
                                horizon_labels_offset=horizon_labels_offset,
                                pending_shadow_by_event_id=pending_shadow_by_event_id,
                                extra_fields=shadow_extra_fields,
                            )
                            continue
                        last_dispatch_by_symbol[symbol] = time.time()
                        emit_event(
                            trade_log_event_name(playbook=playbook, outcome="armed"),
                            **trade_result,
                        )
                        try:
                            success_shadow_extra_fields = dict(trade_shadow_extra_fields)
                            success_shadow_extra_fields["real_trade_armed"] = True
                            success_shadow_extra_fields["armed_trade_quote_allocation_usdt"] = trade_result.get(
                                "effective_quote_allocation_usdt"
                            )
                            await persist_shadow_signal(
                                futures=futures,
                                executor=strategy_executor,
                                settings=strategy_settings,
                                record=record,
                                playbook=playbook,
                                side=side,
                                failure_reason="parallel_shadow_branch",
                                low_balance_threshold_usdt=float(args.low_balance_threshold_usdt),
                                low_balance_quote_allocation_usdt=float(args.low_balance_quote_allocation),
                                shadow_branch_specs=effective_shadow_branch_specs,
                                shadow_log_file=shadow_log_file,
                                shadow_state_file=shadow_state_file,
                                horizon_labels_file=horizon_labels_file,
                                horizon_labels_offset=horizon_labels_offset,
                                pending_shadow_by_event_id=pending_shadow_by_event_id,
                                extra_fields=success_shadow_extra_fields,
                            )
                        except Exception as exc:  # noqa: BLE001
                            emit_event(
                                "signal_bridge_parallel_shadow_failed",
                                event_id=event_id,
                                symbol=symbol,
                                playbook=playbook,
                                side=side,
                                error=str(exc),
                            )
                await asyncio.sleep(max(0.1, float(args.poll_interval_sec)))
        finally:
            if user_data_oms_stop_event is not None:
                user_data_oms_stop_event.set()
            if user_data_oms_task is not None:
                user_data_oms_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await user_data_oms_task
            signal.signal(signal.SIGINT, previous_sigint)
            if previous_sigterm is not None and hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, previous_sigterm)
            flush_state_offset(force=True)
            save_shadow_state(
                shadow_state_file,
                horizon_labels_file=horizon_labels_file,
                horizon_labels_offset=horizon_labels_offset,
                pending_by_event_id=pending_shadow_by_event_id,
            )
            bridge_instance_lock.release()
            emit_event("signal_bridge_stopped", snapshots_file=str(snapshots_file), offset=offset)
    return 0


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(bridge_loop(args))
    except BridgeInstanceLockError as exc:
        emit_event("signal_bridge_instance_lock_busy", error=str(exc))
        return 75


if __name__ == "__main__":
    raise SystemExit(main())
