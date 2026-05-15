from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix.direction_regime_matrix import direction_regime_fields, evaluate_direction_regime
from phoenix.entry_quality_filter import entry_quality_fields, evaluate_entry_quality
from phoenix.exchange_filter_feasibility import exchange_filter_fields
from phoenix.hermes_decision import OPEN_ACTIONS, REDUCE_ONLY_ACTIONS


@dataclass(frozen=True, slots=True)
class RiskGovernorConfig:
    max_open_positions: int = 1
    max_risk_per_trade_pct: float = 0.75
    max_daily_loss_pct: float = 3.0
    loss_streak_lock: int = 3
    cooldown_after_loss_sec: int = 900
    max_spread_bps: float = 12.0
    max_slippage_bps: float = 8.0
    min_confidence: float = 0.55
    reduced_size_multiplier: float = 0.35
    default_max_allowed_size: float = 1.0
    stale_snapshot_after_sec: int = 30
    micro_scalp_max_holding_sec: int = 300
    require_take_profit: bool = False
    require_max_holding_time: bool = False
    require_invalidation_condition: bool = False
    require_explicit_quote_allocation: bool = False
    require_known_market_regime: bool = False
    require_entry_quality_filter: bool = False
    max_quote_allocation_usdt: float | None = None


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    reason: str
    blocked_by: list[str] = field(default_factory=list)
    sanitized_action: dict[str, Any] | None = None
    risk_notes: list[str] = field(default_factory=list)
    max_allowed_size: float = 0.0
    required_protective_orders: list[dict[str, Any]] = field(default_factory=list)
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_risk(
    decision: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    environment: dict[str, Any] | None = None,
    config: RiskGovernorConfig | None = None,
    log_path: str | Path | None = None,
) -> RiskDecision:
    config = config or RiskGovernorConfig()
    environment = environment or {}
    blocked_by: list[str] = []
    notes: list[str] = []
    action = str(decision.get("action") or "").upper()
    symbol = str(decision.get("symbol") or "").upper()
    sanitized = dict(decision)

    _check_environment(environment, blocked_by)
    _check_auto_confirm(decision, environment, sanitized, blocked_by)
    _check_order_endpoint_environment(action, environment, blocked_by)

    system_status = snapshot.get("system_status") or {}
    account_risk = snapshot.get("account_risk") or {}
    market_regime = snapshot.get("market_regime") or {}
    positions = list(snapshot.get("current_positions") or [])
    candidate = _candidate_for_symbol(snapshot, symbol)
    trade_sensitive_action = action not in {"NO_TRADE", "STOP_TRADING", "WAIT_FOR_TRIGGER"}

    if trade_sensitive_action:
        _check_trusted_testnet_order_snapshot(system_status, environment, blocked_by)
        _check_snapshot_health(system_status, config, blocked_by)
        _check_state_known(system_status, action, blocked_by)
    if action in OPEN_ACTIONS and not account_risk.get("trading_allowed", False):
        blocked_by.append("account_trading_blocked")
    if account_risk.get("reason_if_blocked"):
        notes.append(f"account_block_reason={account_risk.get('reason_if_blocked')}")

    if action in OPEN_ACTIONS:
        _check_loss_controls(account_risk, config, blocked_by)
        _check_entry_safety_paths(system_status, blocked_by, require_take_profit_path=config.require_take_profit)
        _check_stage2_exit_plan(decision, config, blocked_by)
        _check_micro_notional(environment, config, blocked_by)
        _check_exchange_filter_feasibility(candidate, config, environment, sanitized, blocked_by, notes)
        _check_entry_quality(decision, candidate, market_regime, environment, config, sanitized, blocked_by, notes)
    _check_position_safety(decision, positions, account_risk, config, blocked_by)
    _check_direction(decision, market_regime, candidate, config, blocked_by, notes, sanitized)
    if action in OPEN_ACTIONS:
        _check_stage2_v04_execution_policy(sanitized, environment, blocked_by, notes)
    _check_execution_quality(candidate, action, config, blocked_by)
    _check_stop_protection(decision, action, positions, blocked_by)

    if decision.get("confidence") is not None and float(decision.get("confidence") or 0.0) < config.min_confidence:
        blocked_by.append("confidence_below_threshold")

    if action in REDUCE_ONLY_ACTIONS:
        sanitized["reduce_only"] = True

    required_orders = _required_protective_orders(decision) if action in OPEN_ACTIONS else []
    approved = not blocked_by
    max_allowed_size = config.default_max_allowed_size
    if approved and any(note.startswith("micro_scalp_size_multiplier=") for note in notes):
        max_allowed_size = config.default_max_allowed_size * config.reduced_size_multiplier
    risk_decision = RiskDecision(
        approved=approved,
        reason="approved" if approved else ",".join(sorted(set(blocked_by))),
        blocked_by=sorted(set(blocked_by)),
        sanitized_action=sanitized,
        risk_notes=notes,
        max_allowed_size=(max_allowed_size if approved else 0.0),
        required_protective_orders=required_orders,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    if log_path is not None and not approved:
        append_jsonl(log_path, {"event": "risk_reject", **risk_decision.to_dict()})
    return risk_decision


def append_jsonl(path: str | Path, row: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _check_environment(environment: dict[str, Any], blocked_by: list[str]) -> None:
    runtime_mode = str(environment.get("runtime_mode") or environment.get("PHOENIX_RUNTIME_MODE") or "DRY_RUN").upper()
    env = str(environment.get("env") or environment.get("PHOENIX_BINANCE_ENV") or "testnet").lower()
    mainnet_live = bool(environment.get("mainnet_live", False)) or runtime_mode == "MAINNET_LIVE"
    live_enabled = str(environment.get("PHOENIX_MAINNET_LIVE_ENABLED") or environment.get("mainnet_live_enabled") or "false").lower()
    legacy_enable_mainnet = str(environment.get("PHOENIX_ENABLE_MAINNET_LIVE") or "false").lower()
    legacy_live_enabled = str(environment.get("PHOENIX_LIVE_TRADING_ENABLED") or environment.get("live_trading_enabled") or "false").lower()
    promotion_allowed = str(environment.get("PHOENIX_PROMOTION_ALLOWED") or environment.get("promotion_allowed") or "false").lower()
    if mainnet_live or live_enabled in {"1", "true", "yes", "on"}:
        blocked_by.append("mainnet_live_blocked")
    if legacy_enable_mainnet in {"1", "true", "yes", "on"}:
        blocked_by.append("mainnet_live_blocked")
    if legacy_live_enabled in {"1", "true", "yes", "on"}:
        blocked_by.append("live_trading_enabled_blocked")
    if promotion_allowed in {"1", "true", "yes", "on"}:
        blocked_by.append("promotion_allowed_rejected")
    if runtime_mode not in {"DRY_RUN", "SHADOW", "MAINNET_SHADOW", "TESTNET_LIVE", "TESTNET"}:
        blocked_by.append("unsupported_runtime_mode")
    if runtime_mode in {"TESTNET_LIVE", "TESTNET"} and env not in {"testnet", "demo"}:
        blocked_by.append("testnet_env_required")
    if env in {"prod", "mainnet"} and runtime_mode != "MAINNET_SHADOW":
        blocked_by.append("mainnet_env_blocked")


def _check_order_endpoint_environment(action: str, environment: dict[str, Any], blocked_by: list[str]) -> None:
    if action in {"NO_TRADE", "STOP_TRADING", "WAIT_FOR_TRIGGER"}:
        return
    runtime_mode = str(environment.get("runtime_mode") or environment.get("PHOENIX_RUNTIME_MODE") or "DRY_RUN").upper()
    env = str(environment.get("env") or environment.get("PHOENIX_BINANCE_ENV") or "testnet").lower()
    if runtime_mode == "MAINNET_SHADOW" and env in {"prod", "mainnet"}:
        blocked_by.append("mainnet_shadow_order_endpoint_blocked")


def _check_auto_confirm(
    decision: dict[str, Any],
    environment: dict[str, Any],
    sanitized: dict[str, Any],
    blocked_by: list[str],
) -> None:
    env_auto_confirm = _truthy(environment.get("AUTO_CONFIRM_WHEN_RULES_PASS"))
    decision_auto_confirm = _truthy(decision.get("AUTO_CONFIRM_WHEN_RULES_PASS"))
    execution_mode = str(environment.get("PHOENIX_EXECUTION_MODE") or decision.get("PHOENIX_EXECUTION_MODE") or "").upper()
    if env_auto_confirm or decision_auto_confirm or execution_mode == "AUTO_CONFIRM_WHEN_RULES_PASS":
        blocked_by.append("auto_confirm_rejected")
    sanitized["AUTO_CONFIRM_WHEN_RULES_PASS"] = False


def _check_snapshot_health(
    system_status: dict[str, Any],
    config: RiskGovernorConfig,
    blocked_by: list[str],
) -> None:
    if not system_status.get("data_fresh", False):
        blocked_by.append("stale_snapshot")
    websocket_healthy = _status(system_status.get("websocket_status")) == "healthy"
    exchange_healthy = _status(system_status.get("exchange_status")) == "healthy"
    if not websocket_healthy or not exchange_healthy:
        blocked_by.append("snapshot_transport_unhealthy")
    snapshot_time = _parse_datetime(system_status.get("snapshot_time"))
    if snapshot_time is None:
        blocked_by.append("stale_snapshot")
        return
    age_sec = (datetime.now(timezone.utc) - snapshot_time).total_seconds()
    if age_sec < 0:
        return
    if age_sec > max(1, int(config.stale_snapshot_after_sec or 30)):
        blocked_by.append("stale_snapshot")


def _check_state_known(system_status: dict[str, Any], action: str, blocked_by: list[str]) -> None:
    position_state = _status(system_status.get("position_state") or "unknown")
    stop_status = _status(system_status.get("stop_protection_status") or "unknown")
    candidate_state = _status(system_status.get("candidate_state") or "known")
    if position_state in {"", "unknown", "unavailable", "stale"}:
        blocked_by.append("position_state_unknown")
    if stop_status in {"", "unknown", "unavailable", "failed", "missing"}:
        blocked_by.append("stop_order_status_unknown")
    if action in OPEN_ACTIONS and candidate_state in {"", "unknown", "unavailable", "stale"}:
        blocked_by.append("candidate_state_unknown")


def _check_entry_safety_paths(
    system_status: dict[str, Any],
    blocked_by: list[str],
    *,
    require_take_profit_path: bool = False,
) -> None:
    if system_status.get("protective_stop_path_available") is not True:
        blocked_by.append("protective_stop_path_unavailable")
    if require_take_profit_path and system_status.get("take_profit_path_available") is not True:
        blocked_by.append("take_profit_path_unavailable")
    if require_take_profit_path and system_status.get("take_profit_order_supported") is not True:
        blocked_by.append("take_profit_order_unsupported")
    if system_status.get("emergency_close_available") is not True:
        blocked_by.append("emergency_close_unavailable")


def _check_trusted_testnet_order_snapshot(
    system_status: dict[str, Any],
    environment: dict[str, Any],
    blocked_by: list[str],
) -> None:
    runtime_mode = str(environment.get("runtime_mode") or environment.get("PHOENIX_RUNTIME_MODE") or "DRY_RUN").upper()
    require_trusted = _truthy(environment.get("require_trusted_runtime_snapshot")) or _truthy(
        environment.get("PHOENIX_REQUIRE_TRUSTED_RUNTIME_SNAPSHOT")
    )
    if runtime_mode not in {"TESTNET", "TESTNET_LIVE"} or not require_trusted:
        return
    source = _status(system_status.get("snapshot_source") or system_status.get("source"))
    if source != "runtime":
        blocked_by.append("manual_snapshot_not_allowed_for_testnet_order_mode")
    if system_status.get("trusted_runtime_snapshot") is not True:
        blocked_by.append("untrusted_runtime_snapshot")
    if _status(system_status.get("account_state_source") or system_status.get("account_source")) != "signed_account":
        blocked_by.append("missing_account_state")
    if _status(system_status.get("position_state_source")) != "signed_positions":
        blocked_by.append("missing_position_state")
    if not _verified_capability(system_status.get("protective_stop_capability_source")):
        blocked_by.append("protective_stop_path_unverified")
    if not _verified_capability(system_status.get("take_profit_capability_source")):
        blocked_by.append("take_profit_path_unverified")
    if not _verified_capability(system_status.get("emergency_close_capability_source")):
        blocked_by.append("emergency_close_unverified")


def _check_loss_controls(
    account_risk: dict[str, Any],
    config: RiskGovernorConfig,
    blocked_by: list[str],
) -> None:
    daily_remaining = _safe_float(account_risk.get("daily_loss_remaining_pct"), default=config.max_daily_loss_pct)
    loss_streak = int(_safe_float(account_risk.get("loss_streak"), default=0) or 0)
    if daily_remaining <= 0:
        blocked_by.append("daily_loss_limit_hit")
    if loss_streak >= config.loss_streak_lock:
        blocked_by.append("loss_streak_lock")
    if bool(account_risk.get("cooldown_active", False)):
        blocked_by.append("cooldown_after_loss_active")


def _check_position_safety(
    decision: dict[str, Any],
    positions: list[dict[str, Any]],
    account_risk: dict[str, Any],
    config: RiskGovernorConfig,
    blocked_by: list[str],
) -> None:
    action = str(decision.get("action") or "").upper()
    if action not in OPEN_ACTIONS:
        return
    symbol = str(decision.get("symbol") or "").upper()
    open_count = int(_safe_float(account_risk.get("open_positions_count"), default=len(positions)) or len(positions))
    max_open = int(_safe_float(account_risk.get("max_open_positions"), default=config.max_open_positions) or config.max_open_positions)
    if open_count >= min(max_open, config.max_open_positions):
        blocked_by.append("max_open_positions")
    if any(str(item.get("symbol") or "").upper() == symbol for item in positions):
        blocked_by.append("duplicate_position")
    if bool(decision.get("averaging_down", False)):
        blocked_by.append("averaging_down_blocked")
    if bool(decision.get("martingale", False)):
        blocked_by.append("martingale_blocked")
    if bool(decision.get("increase_size_after_loss", False)):
        blocked_by.append("increase_size_after_loss_blocked")
    margin_type = str(decision.get("margin_type") or "ISOLATED").upper()
    if margin_type != "ISOLATED":
        blocked_by.append("isolated_margin_required")


def _check_direction(
    decision: dict[str, Any],
    market_regime: dict[str, Any],
    candidate: dict[str, Any] | None,
    config: RiskGovernorConfig,
    blocked_by: list[str],
    notes: list[str],
    sanitized: dict[str, Any],
) -> None:
    action = str(decision.get("action") or "").upper()
    if action not in OPEN_ACTIONS:
        return
    raw_regime = str(market_regime.get("regime") or "UNKNOWN").upper()
    raw_lock = str(market_regime.get("direction_lock") or "").upper()
    if raw_regime in {"", "UNKNOWN"} and not config.require_known_market_regime and raw_lock in {"", "BOTH_ALLOWED"}:
        return
    direction_result = evaluate_direction_regime(
        action=action,
        candidate_direction=action,
        market_regime=market_regime,
        candidate=candidate,
    )
    sanitized.update(direction_regime_fields(direction_result))
    notes.append(f"direction_regime_reason={direction_result.direction_regime_reason}")
    if not direction_result.direction_regime_allowed:
        blocked_by.extend(direction_result.blocked_by)
        return

    setup_type = str(decision.get("trade_type") or (candidate or {}).get("setup_type") or "").upper()
    is_micro_scalp = _is_micro_scalp_exception(decision, candidate, config, setup_type)
    regime = raw_regime
    if config.require_known_market_regime and regime in {"", "UNKNOWN"}:
        blocked_by.append("market_regime_unknown")
        return
    if is_micro_scalp:
        notes.append(f"micro_scalp_size_multiplier={config.reduced_size_multiplier}")


def _is_micro_scalp_exception(
    decision: dict[str, Any],
    candidate: dict[str, Any] | None,
    config: RiskGovernorConfig,
    setup_type: str,
) -> bool:
    if setup_type != "QUICK_TRADE":
        return False
    candidate_exception = _truthy((candidate or {}).get("micro_scalp_exception"))
    if not bool(decision.get("size_reduced", False)) and not candidate_exception:
        return False
    if not (_truthy(decision.get("micro_scalp_exception")) or candidate_exception):
        return False
    max_holding = _safe_float(decision.get("max_holding_time_sec") or (candidate or {}).get("max_holding_time_sec"))
    return max_holding is not None and max_holding <= max(1, int(config.micro_scalp_max_holding_sec or 300))


def _check_execution_quality(
    candidate: dict[str, Any] | None,
    action: str,
    config: RiskGovernorConfig,
    blocked_by: list[str],
) -> None:
    if action not in OPEN_ACTIONS:
        return
    if candidate is None:
        blocked_by.append("candidate_state_unknown")
        return
    spread = _safe_float(candidate.get("spread_bps"))
    slippage = _safe_float(candidate.get("estimated_slippage_bps"))
    if candidate.get("liquidity_ok") is False:
        blocked_by.append("liquidity_too_poor")
    if spread is None or spread > config.max_spread_bps:
        blocked_by.append("spread_too_wide")
    if slippage is None or slippage > config.max_slippage_bps:
        blocked_by.append("slippage_too_high")


def _check_entry_quality(
    decision: dict[str, Any],
    candidate: dict[str, Any] | None,
    market_regime: dict[str, Any],
    environment: dict[str, Any],
    config: RiskGovernorConfig,
    sanitized: dict[str, Any],
    blocked_by: list[str],
    notes: list[str],
) -> None:
    checked_payload = decision if decision.get("entry_quality_checked") is not None else (candidate or {})
    checked = _optional_bool(checked_payload.get("entry_quality_checked"))
    required = config.require_entry_quality_filter or _truthy(environment.get("stage2_entry_quality_required"))
    if checked is None and required and candidate is not None:
        result = evaluate_entry_quality(candidate, market_regime, action=decision.get("action"))
        fields = entry_quality_fields(result)
        sanitized.update(fields)
        notes.append(f"entry_quality_reason={result.entry_quality_reason}")
        if not result.entry_quality_allowed:
            blocked_by.extend(result.blocked_by)
        return
    if checked is None:
        return
    fields = entry_quality_fields(checked_payload)
    sanitized.update(fields)
    reason = str(checked_payload.get("entry_quality_reason") or "")
    if reason:
        notes.append(f"entry_quality_reason={reason}")
    if _optional_bool(checked_payload.get("entry_quality_allowed")) is not True:
        existing = checked_payload.get("blocked_by") if isinstance(checked_payload.get("blocked_by"), list) else []
        blocked_by.extend(list(existing) or ["entry_quality_filter_failed"])


def _check_stage2_v04_execution_policy(
    sanitized: dict[str, Any],
    environment: dict[str, Any],
    blocked_by: list[str],
    notes: list[str],
) -> None:
    runtime_mode = str(environment.get("runtime_mode") or environment.get("PHOENIX_RUNTIME_MODE") or "DRY_RUN").upper()
    if runtime_mode != "TESTNET_LIVE" or not _truthy(environment.get("stage2_micro_order")):
        return
    expected_policy = str(environment.get("expected_policy_version") or "v0.4")
    runner_version = str(environment.get("runner_version") or "")
    policy_version = str(environment.get("policy_version") or "")
    entry_quality_policy_version = str(environment.get("entry_quality_policy_version") or "")
    mismatch = (
        runner_version != "stage2_exploration_v04"
        or policy_version != expected_policy
        or entry_quality_policy_version != expected_policy
    )
    if mismatch:
        blocked_by.extend(["policy_gate_reject", "CANCELLED_BY_RUNNER_VERSION_MISMATCH"])
        sanitized["policy_gate_result"] = "CANCELLED_BY_RUNNER_VERSION_MISMATCH"
        notes.append(
            "stage2_policy_version_guard="
            f"runner_version={runner_version or 'missing'};"
            f"policy_version={policy_version or 'missing'};"
            f"entry_quality_policy_version={entry_quality_policy_version or 'missing'};"
            f"expected_policy_version={expected_policy}"
        )
        return

    failures: list[str] = []
    if _optional_bool(sanitized.get("entry_quality_allowed")) is not True:
        failures.append("entry_quality_allowed_not_true")
    if not str(sanitized.get("entry_quality_filter") or "").strip():
        failures.append("entry_quality_filter_missing")
    if not str(sanitized.get("entry_quality_reason") or "").strip():
        failures.append("entry_quality_reason_missing")
    components = sanitized.get("entry_quality_components")
    if not isinstance(components, dict) or not components:
        failures.append("entry_quality_components_missing")
    if _optional_bool(sanitized.get("no_follow_through_exit_enabled")) is not True:
        failures.append("no_follow_through_exit_enabled_not_true")
    if sanitized.get("no_follow_through_exit_sec") is None:
        failures.append("no_follow_through_exit_sec_missing")
    if sanitized.get("no_follow_through_min_mfe_pct") is None:
        failures.append("no_follow_through_min_mfe_pct_missing")
    if _optional_bool(sanitized.get("direction_regime_allowed")) is not True:
        failures.append("direction_regime_allowed_not_true")
    if _optional_bool(sanitized.get("micro_notional_feasible")) is not True:
        failures.append("micro_notional_feasible_not_true")
    if failures:
        blocked_by.extend(["policy_gate_reject", *failures])
        sanitized["policy_gate_result"] = "policy_gate_reject"
        notes.append("stage2_v04_execution_policy_reject=" + ",".join(failures))


def _check_exchange_filter_feasibility(
    candidate: dict[str, Any] | None,
    config: RiskGovernorConfig,
    environment: dict[str, Any],
    sanitized: dict[str, Any],
    blocked_by: list[str],
    notes: list[str],
) -> None:
    stage2_micro = _truthy(environment.get("stage2_micro_order")) or config.max_quote_allocation_usdt is not None
    if not stage2_micro:
        return
    if candidate is None:
        blocked_by.append("exchange_filter_reject")
        blocked_by.append("exchange_filter_unchecked")
        return
    fields = exchange_filter_fields(candidate)
    if fields:
        sanitized.update(fields)
        notes.extend(f"{key}={value}" for key, value in fields.items() if value not in (None, ""))
    if candidate.get("exchange_filter_checked") is not True:
        blocked_by.append("exchange_filter_reject")
        blocked_by.append("exchange_filter_unchecked")
        return
    if candidate.get("symbol_tradeable") is False:
        blocked_by.append("exchange_filter_reject")
        blocked_by.append("symbol_not_tradeable")
    if candidate.get("micro_notional_feasible") is False:
        blocked_by.append("exchange_filter_reject")
        blocked_by.append("micro_notional_infeasible")


def _check_stop_protection(
    decision: dict[str, Any],
    action: str,
    positions: list[dict[str, Any]],
    blocked_by: list[str],
) -> None:
    if action in OPEN_ACTIONS:
        if decision.get("stop_loss_pct") in (None, "") and decision.get("stop_loss_price") in (None, ""):
            blocked_by.append("missing_stop_loss")
        return

    if action in REDUCE_ONLY_ACTIONS:
        symbol = str(decision.get("symbol") or "").upper()
        known = [item for item in positions if str(item.get("symbol") or "").upper() == symbol]
        if not known:
            blocked_by.append("position_state_unknown")
        for item in known:
            if str(item.get("protection_status") or "").lower() in {"unknown", "failed", "missing"}:
                blocked_by.append("stop_order_status_unknown")


def _required_protective_orders(decision: dict[str, Any]) -> list[dict[str, Any]]:
    orders = [
        {
            "type": "exchange_side_stop_loss",
            "symbol": decision.get("symbol"),
            "stop_loss_pct": decision.get("stop_loss_pct"),
            "stop_loss_price": decision.get("stop_loss_price"),
            "reduce_only": True,
        }
    ]
    if decision.get("take_profit_pct") not in (None, "") or decision.get("take_profit_price") not in (None, ""):
        orders.append(
            {
                "type": "exchange_side_take_profit",
                "symbol": decision.get("symbol"),
                "take_profit_pct": decision.get("take_profit_pct"),
                "take_profit_price": decision.get("take_profit_price"),
                "reduce_only": True,
            }
        )
    return orders


def _check_stage2_exit_plan(decision: dict[str, Any], config: RiskGovernorConfig, blocked_by: list[str]) -> None:
    if config.require_take_profit and decision.get("take_profit_pct") in (None, "") and decision.get("take_profit_price") in (None, ""):
        blocked_by.append("missing_take_profit")
    if config.require_max_holding_time:
        max_holding = _safe_float(decision.get("max_holding_time_sec"))
        if max_holding is None or max_holding <= 0:
            blocked_by.append("missing_max_holding_time")
    if config.require_invalidation_condition and not str(decision.get("invalidation_condition") or "").strip():
        blocked_by.append("missing_invalidation_condition")


def _check_micro_notional(environment: dict[str, Any], config: RiskGovernorConfig, blocked_by: list[str]) -> None:
    if not config.require_explicit_quote_allocation and config.max_quote_allocation_usdt is None:
        return
    quote_allocation = _safe_float(
        environment.get("quote_allocation_usdt")
        or environment.get("PHOENIX_QUOTE_ALLOCATION_USDT")
        or environment.get("stage2_quote_allocation_usdt")
    )
    if quote_allocation is None or quote_allocation <= 0:
        if config.require_explicit_quote_allocation:
            blocked_by.append("micro_notional_required")
        return
    if config.max_quote_allocation_usdt is not None and quote_allocation > float(config.max_quote_allocation_usdt):
        blocked_by.append("micro_notional_exceeded")


def _candidate_for_symbol(snapshot: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    for item in snapshot.get("top_candidates") or []:
        if str(item.get("symbol") or "").upper() == symbol:
            return item
    return None


def _safe_float(value: Any, *, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _status(value: Any) -> str:
    return str(value or "").strip().lower()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _verified_capability(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text not in {"", "unverified", "manual", "manual_payload", "mock", "file", "unknown"}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
