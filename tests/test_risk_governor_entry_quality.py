from __future__ import annotations

from datetime import datetime, timezone

from phoenix.risk_governor import RiskGovernorConfig, evaluate_risk


def stage2_config() -> RiskGovernorConfig:
    return RiskGovernorConfig(
        require_known_market_regime=True,
        require_take_profit=True,
        require_max_holding_time=True,
        require_invalidation_condition=True,
        require_explicit_quote_allocation=True,
        require_entry_quality_filter=True,
        max_quote_allocation_usdt=10.0,
    )


def stage2_environment() -> dict:
    return {
        "runtime_mode": "TESTNET_LIVE",
        "env": "testnet",
        "require_trusted_runtime_snapshot": True,
        "stage2_micro_order": True,
        "stage2_entry_quality_required": True,
        "runner_version": "stage2_exploration_v04",
        "policy_version": "v0.4",
        "entry_quality_policy_version": "v0.4",
        "expected_policy_version": "v0.4",
        "quote_allocation_usdt": 5.0,
        "mainnet_live": False,
        "PHOENIX_MAINNET_LIVE_ENABLED": "false",
        "PHOENIX_LIVE_TRADING_ENABLED": "false",
        "PHOENIX_PROMOTION_ALLOWED": "false",
    }


def short_decision(**overrides) -> dict:
    decision = {
        "action": "ENTER_SHORT",
        "symbol": "SOLUSDT",
        "trade_type": "QUICK_TRADE",
        "confidence": 0.82,
        "reason": "Stage 2 testnet exploration: continuation short setup.",
        "stop_loss_pct": 0.6,
        "take_profit_pct": 1.2,
        "max_holding_time_sec": 300,
        "invalidation_condition": "failed rebound invalidates short",
        "source": "HERMES",
        "writer": "Hermes Trader Brain",
    }
    decision.update(overrides)
    return decision


def short_candidate(**overrides) -> dict:
    candidate = {
        "symbol": "SOLUSDT",
        "bias": "SHORT",
        "setup_type": "QUICK_TRADE",
        "score": 18.0,
        "current_price": 142.42,
        "spread_bps": 0.7,
        "estimated_slippage_bps": 1.1,
        "estimated_fee_bps": 4.0,
        "liquidity_ok": True,
        "price_change_1m_pct": -0.18,
        "price_change_5m_pct": -0.55,
        "momentum_follow_through": True,
        "suggested_stop_pct": 0.6,
        "suggested_tp_pct": 1.2,
        "max_holding_time_sec": 300,
        "invalidation_hint": "failed rebound invalidates short",
        "invalidation_distance_bps": 60.0,
        "exchange_filter_checked": True,
        "symbol_tradeable": True,
        "micro_notional_feasible": True,
        "required_quote_allocation_usdt": 5.0,
        "configured_quote_allocation_usdt": 5.0,
        "configured_leverage": 2,
        "max_quote_allocation_usdt": 10.0,
        "rounded_qty": 0.07,
        "min_qty": 0.01,
        "step_size": 0.01,
        "min_notional": 5.0,
    }
    candidate.update(overrides)
    return candidate


def stage2_snapshot(candidate: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "trace_id": "risk_entry_quality",
        "created_at": now,
        "system_status": {
            "source": "runtime",
            "snapshot_source": "runtime",
            "snapshot_time": now,
            "trusted_runtime_snapshot": True,
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
            "candidate_state": "known",
            "position_state": "known",
            "stop_protection_status": "healthy",
            "protective_stop_path_available": True,
            "take_profit_path_available": True,
            "take_profit_order_supported": True,
            "emergency_close_available": True,
            "account_state_source": "signed_account",
            "position_state_source": "signed_positions",
            "protective_stop_capability_source": "verified_code_path:test",
            "take_profit_capability_source": "verified_code_path:test",
            "emergency_close_capability_source": "verified_code_path:test",
        },
        "account_risk": {
            "trading_allowed": True,
            "daily_pnl_pct": 0.0,
            "daily_loss_remaining_pct": 3.0,
            "open_positions_count": 0,
            "max_open_positions": 1,
            "loss_streak": 0,
            "cooldown_active": False,
        },
        "current_positions": [],
        "market_regime": {
            "regime": "TREND_DOWN",
            "direction_lock": "SHORT_ONLY_OR_NO_TRADE",
            "source": "test",
            "confidence": 0.82,
            "reason": "trend down fixture",
            "btc_trend_1m": -0.14,
            "btc_trend_5m": -0.31,
            "eth_trend_1m": 0.01,
            "eth_trend_5m": -0.07,
        },
        "top_candidates": [candidate],
    }


def test_risk_governor_rejects_explicit_entry_quality_failure_and_preserves_fields() -> None:
    decision = short_decision(
        entry_quality_filter="stage2_v0.4_entry_quality",
        entry_quality_version="stage2_v0.4",
        entry_quality_checked=True,
        entry_quality_allowed=False,
        entry_quality_score=0.46,
        entry_quality_min_score=0.75,
        entry_quality_reason="entry quality block: late chase short",
        entry_quality_reasons=["late chase short"],
        entry_quality_components={"late_chase": True},
        blocked_by=["entry_quality_filter_failed", "late_chase_short"],
    )

    risk = evaluate_risk(
        decision,
        stage2_snapshot(short_candidate()),
        environment=stage2_environment(),
        config=stage2_config(),
    )

    assert risk.approved is False
    assert "entry_quality_filter_failed" in risk.blocked_by
    assert "late_chase_short" in risk.blocked_by
    assert risk.sanitized_action["entry_quality_allowed"] is False
    assert risk.sanitized_action["entry_quality_reason"] == "entry quality block: late chase short"
    assert "entry_quality_reason=entry quality block: late chase short" in risk.risk_notes


def test_risk_governor_evaluates_missing_entry_quality_when_required_and_allows_strong_setup() -> None:
    risk = evaluate_risk(
        short_decision(),
        stage2_snapshot(short_candidate()),
        environment=stage2_environment(),
        config=stage2_config(),
    )

    assert risk.approved is True
    assert risk.sanitized_action["entry_quality_checked"] is True
    assert risk.sanitized_action["entry_quality_allowed"] is True
    assert risk.sanitized_action["entry_quality_reason"] == "Entry quality passed for v0.4 testnet exploration."


def test_risk_governor_evaluates_missing_entry_quality_when_required_and_rejects_late_chase() -> None:
    risk = evaluate_risk(
        short_decision(),
        stage2_snapshot(short_candidate(price_change_1m_pct=-1.1, price_change_5m_pct=-2.2)),
        environment=stage2_environment(),
        config=stage2_config(),
    )

    assert risk.approved is False
    assert "entry_quality_filter_failed" in risk.blocked_by
    assert "late_chase_short" in risk.blocked_by
    assert risk.sanitized_action["entry_quality_allowed"] is False
    assert "would chase" in risk.sanitized_action["entry_quality_reason"]
