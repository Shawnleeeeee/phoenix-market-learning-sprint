import asyncio
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from phoenix.hermes_decision_provider import MockHermesDecisionProvider
from phoenix.hermes_trader_mode import run_trader_cycle
from phoenix.risk_governor import RiskGovernorConfig
from phoenix.safe_order_gateway import submit_order_intent


def _snapshot() -> dict:
    snapshot_time = datetime.now(timezone.utc).isoformat()
    return {
        "system_status": {
            "source": "runtime",
            "snapshot_source": "runtime",
            "snapshot_time": snapshot_time,
            "trusted_runtime_snapshot": True,
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
            "account_state_source": "signed_account",
            "account_source": "signed_account",
            "position_state_source": "signed_positions",
            "candidate_state": "known",
            "position_state": "known",
            "stop_protection_status": "healthy",
            "protective_stop_path_available": True,
            "protective_stop_capability_source": "verified_code_path:test",
            "take_profit_path_available": True,
            "take_profit_capability_source": "verified_code_path:test",
            "take_profit_order_supported": True,
            "emergency_close_available": True,
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
            "source": "unit_test",
            "confidence": 0.82,
            "reason": "unit test trend down",
            "btc_trend_1m": -0.12,
            "btc_trend_5m": -0.2,
            "eth_trend_1m": 0.01,
            "eth_trend_5m": -0.08,
        },
        "top_candidates": [
            {
                "symbol": "SOLUSDT",
                "bias": "SHORT",
                "setup_type": "QUICK_TRADE",
                "score": 18.0,
                "current_price": 88.97,
                "spread_bps": 0.7,
                "estimated_slippage_bps": 1.1,
                "estimated_fee_bps": 4.0,
                "liquidity_ok": True,
                "price_change_1m_pct": -0.25,
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
                "rounded_qty": 0.11,
                "min_qty": 0.01,
                "step_size": 0.01,
                "min_notional": 5.0,
            }
        ],
    }


def _decision(**overrides) -> dict:
    payload = {
        "action": "ENTER_SHORT",
        "decision": "ENTER_SHORT",
        "symbol": "SOLUSDT",
        "candidate_symbol": "SOLUSDT",
        "candidate_direction": "SHORT",
        "allowed_direction": "SHORT",
        "direction_regime_allowed": True,
        "direction_regime_reason": "TREND_DOWN permits SHORT only.",
        "trade_type": "QUICK_TRADE",
        "confidence": 0.9,
        "reason": "Stage 2 testnet exploration: candidate ready.",
        "stop_loss_pct": 0.25,
        "take_profit_pct": 0.45,
        "max_holding_time_sec": 300,
        "invalidation_condition": "failed rebound invalidates short",
        "source": "HERMES",
        "writer": "Hermes Trader Brain",
        "entry_quality_filter": "stage2_v0.4_entry_quality",
        "entry_quality_version": "stage2_v0.4",
        "entry_quality_checked": True,
        "entry_quality_allowed": True,
        "entry_quality_score": 0.92,
        "entry_quality_min_score": 0.75,
        "entry_quality_reason": "Entry quality passed for v0.4 testnet exploration.",
        "entry_quality_components": {"late_chase": False, "total_cost_bps": 5.1},
        "no_follow_through_exit_enabled": True,
        "no_follow_through_exit_sec": 120,
        "no_follow_through_min_mfe_pct": 0.0,
    }
    payload.update(overrides)
    return payload


def _environment(**overrides) -> dict:
    payload = {
        "runtime_mode": "TESTNET_LIVE",
        "env": "testnet",
        "stage2_micro_order": True,
        "stage2_entry_quality_required": True,
        "quote_allocation_usdt": 5.0,
        "max_quote_allocation_usdt": 10.0,
        "runner_version": "stage2_exploration_v04",
        "policy_version": "v0.4",
        "entry_quality_policy_version": "v0.4",
        "expected_policy_version": "v0.4",
        "mainnet_live": False,
        "PHOENIX_MAINNET_LIVE_ENABLED": "false",
        "PHOENIX_LIVE_TRADING_ENABLED": "false",
        "PHOENIX_PROMOTION_ALLOWED": "false",
        "cleanup": False,
        "auto_promotion": False,
        "require_trusted_runtime_snapshot": True,
    }
    payload.update(overrides)
    return payload


def _risk_config() -> RiskGovernorConfig:
    return RiskGovernorConfig(
        require_known_market_regime=True,
        require_take_profit=True,
        require_max_holding_time=True,
        require_invalidation_condition=True,
        require_explicit_quote_allocation=True,
        require_entry_quality_filter=True,
        max_quote_allocation_usdt=10.0,
    )


async def _gateway(decision: dict, environment: dict):
    calls = []

    async def fake_executor(intent):
        calls.append(intent)
        return {
            "order_submitted": False,
            "testnet_order_submitted": False,
            "mainnet_order_submitted": False,
            "status": "fake_executor_path_only",
            "result_type": "fake_executor_path_only",
        }

    result = await submit_order_intent(
        decision,
        _snapshot(),
        environment,
        "HERMES",
        dry_run=False,
        executor_callback=fake_executor,
        risk_config=_risk_config(),
    )
    return result.to_dict(), calls


def test_v04_authorization_rejects_v03_runner_before_executor():
    result, calls = asyncio.run(
        _gateway(_decision(), _environment(runner_version="stage2_exploration_v03"))
    )

    assert calls == []
    assert result["result_type"] == "policy_gate_reject"
    assert result["execution_result"]["executor_called"] is False
    assert result["execution_result"]["testnet_order_submitted"] is False
    assert "CANCELLED_BY_RUNNER_VERSION_MISMATCH" in result["blocked_by"]


def test_entry_quality_allowed_null_rejects_before_executor():
    result, calls = asyncio.run(_gateway(_decision(entry_quality_allowed=None), _environment()))

    assert calls == []
    assert result["result_type"] == "policy_gate_reject"
    assert result["execution_result"]["executor_called"] is False
    assert result["execution_result"]["testnet_order_submitted"] is False
    assert "entry_quality_allowed_not_true" in result["blocked_by"]


def test_entry_quality_filter_null_rejects_before_executor():
    result, calls = asyncio.run(_gateway(_decision(entry_quality_filter=None), _environment()))

    assert calls == []
    assert result["result_type"] == "policy_gate_reject"
    assert result["execution_result"]["executor_called"] is False
    assert result["execution_result"]["testnet_order_submitted"] is False
    assert "entry_quality_filter_missing" in result["blocked_by"]


def test_no_follow_through_enabled_null_rejects_before_executor():
    result, calls = asyncio.run(
        _gateway(_decision(no_follow_through_exit_enabled=None), _environment())
    )

    assert calls == []
    assert result["result_type"] == "policy_gate_reject"
    assert result["execution_result"]["executor_called"] is False
    assert result["execution_result"]["testnet_order_submitted"] is False
    assert "no_follow_through_exit_enabled_not_true" in result["blocked_by"]


def test_policy_version_mismatch_rejects_before_executor():
    result, calls = asyncio.run(_gateway(_decision(), _environment(policy_version="v0.3")))

    assert calls == []
    assert result["result_type"] == "policy_gate_reject"
    assert result["execution_result"]["executor_called"] is False
    assert result["execution_result"]["testnet_order_submitted"] is False
    assert "CANCELLED_BY_RUNNER_VERSION_MISMATCH" in result["blocked_by"]


def test_v04_complete_policy_fields_allow_executor_path_without_real_order():
    result, calls = asyncio.run(_gateway(_decision(), _environment()))

    assert len(calls) == 1
    assert result["approved"] is True
    assert result["execution_result"]["executor_called"] is True
    assert result["execution_result"]["testnet_order_submitted"] is False
    assert result["execution_result"]["mainnet_order_submitted"] is False


def test_old_runner_environment_cannot_reach_executor_path(tmp_path):
    calls = []

    async def fake_executor(intent):
        calls.append(intent)
        return {"order_submitted": True, "testnet_order_submitted": True}

    result = run_trader_cycle(
        snapshot_builder=_snapshot,
        decision_provider=MockHermesDecisionProvider(_decision()),
        output_dir=tmp_path,
        dry_run=False,
        environment=_environment(runner_version="stage2_exploration_v03"),
        risk_config=_risk_config(),
        executor_callback=fake_executor,
    )

    assert calls == []
    assert result["result_type"] == "policy_gate_reject"
    assert result["execution_result"]["executor_called"] is False
    assert result["execution_result"]["testnet_order_submitted"] is False


def test_deprecated_v03_runner_script_exits_before_execution():
    script = Path(__file__).resolve().parents[1] / ".tmp_stage2_exploration_v03.py"
    completed = subprocess.run(
        [sys.executable, str(script)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode != 0
    assert "deprecated_runner_do_not_use" in (completed.stdout + completed.stderr)
