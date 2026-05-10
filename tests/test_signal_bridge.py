import json
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from phoenix.config import ExecutionSettings
from phoenix.executor import PhoenixExecutor
from phoenix_testnet_round_runner import discover_universe, is_tradeable_symbol
from phoenix_signal_bridge import (
    BRANCH_TYPE_DEGEN_HIGH_YIELD,
    BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
    BRANCH_TYPE_RESEARCH_POOL,
    BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
    BRANCH_TYPE_STABLE_CORE,
    EXECUTION_MODE_MAINNET_SHADOW,
    PLAYBOOK_DISCOVERY_FACTOR_LIQUIDATION_REVERSION,
    PLAYBOOK_DISCOVERY_FACTOR_TREND_ALIGNMENT,
    PLAYBOOK_DISCOVERY_BIDWALL_OI_BUILD_CONTINUATION,
    PLAYBOOK_DISCOVERY_HIST_BUY_1H_REVERSION_OI_UNWIND_LOW,
    PLAYBOOK_DISCOVERY_HIST_BUY_1H_TREND_REVERSION_EXTREME,
    PLAYBOOK_DISCOVERY_LOW_LIQ_DEPTH_REVERSAL,
    PLAYBOOK_DISCOVERY_OI_UNWIND_5M_RANGE_REVERSAL,
    PLAYBOOK_DISCOVERY_OI_UNWIND_REVERSAL_SCALP,
    PLAYBOOK_DISCOVERY_TREND_MILD_CONTINUATION,
    PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
    PLAYBOOK_SHADOW_OI_BUILD_BALANCED,
    PLAYBOOK_SHADOW_OI_BUILD_QUALITY,
    SHADOW_REASON_MAINNET_SHADOW_LOCKED,
    SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE,
    SHADOW_REASON_RESEARCH_OBSERVATION,
    SHADOW_REASON_RESEARCH_POOL,
    SHADOW_REASON_STRATEGY_DISCOVERY_CANDIDATE,
    BridgeInstanceLockError,
    acquire_bridge_instance_lock,
    build_candidate_strategy_report,
    build_candidate_parking_lot_payload,
    build_hard_kill_close_payload,
    build_oi_unwind_reversal_branch_specs,
    build_research_shadow_signal_payload,
    build_strategy_discovery_branch_specs,
    build_shadow_readiness_report,
    build_shadow_signal_payload,
    build_live_tradable_universe,
    build_real_trade_outcome_key,
    build_total_sample_report,
    candidate_playbook_is_paused,
    candidate_parking_lot_enabled,
    candidate_shadow_writes_enabled,
    compute_24h_volatility_pct,
    compute_btc_regime_return_from_klines,
    derive_playbook,
    derive_signal_side,
    derive_strategy_discovery_candidates,
    evaluate_btc_regime_gate,
    evaluate_degen_shadow_gate,
    evaluate_real_stop_loss_streak,
    evaluate_intraday_liquidity_gate,
    evaluate_live_universe_gate,
    evaluate_symbol_risk_gate,
    is_global_risk_lock_active,
    load_shadow_state,
    parse_args,
    parse_positive_int_csv,
    parse_shadow_branch_specs,
    persist_research_shadow_signal,
    process_shadow_horizon_updates,
    resolve_mainnet_shadow_dir,
    resolve_oi_unwind_reversal_confirmation,
    read_global_risk_lock,
    resolve_effective_quote_allocation_usdt,
    resolve_snapshots_file,
    shadow_target_horizons,
    simulate_shadow_branch_outcome,
    summarize_shadow_performance,
    update_candidate_strategy_control,
    user_data_oms_enabled_for_mode,
    validate_execution_mode_args,
    write_live_performance_report,
    write_global_risk_lock,
)


class FakeMainnetShadowFutures:
    def __init__(self) -> None:
        self.test_orders: list[dict[str, object]] = []
        self.new_orders: list[dict[str, object]] = []
        self.kline_rows: list[list[object]] = []

    async def get_account_api_mode(self) -> str:
        return "classic"

    def planned_account_api_mode(self) -> str:
        return "classic"

    async def account_overview(self) -> list[dict[str, str]]:
        return [{"asset": "USDT", "balance": "1000", "availableBalance": "1000"}]

    async def exchange_info(self) -> dict[str, object]:
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "triggerProtect": "0.05",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

    async def mark_price(self, symbol: str) -> dict[str, str]:
        return {"symbol": symbol, "markPrice": "100"}

    async def test_order(self, payload: dict[str, object], *, endpoint: str | None = None) -> dict[str, object]:
        self.test_orders.append({"endpoint": endpoint, "payload": payload})
        if endpoint == "/fapi/v1/algoOrder":
            return {"previewOnly": True, "endpoint": endpoint, "payload": payload}
        return {"ok": True, "endpoint": endpoint}

    async def klines(
        self,
        symbol: str,
        *,
        interval: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int,
    ) -> list[list[object]]:
        return list(self.kline_rows)

    async def new_order(self, payload: dict[str, object]) -> dict[str, object]:
        self.new_orders.append(payload)
        raise AssertionError("MAINNET_SHADOW must never call new_order")


class FailingAccountOverviewFutures(FakeMainnetShadowFutures):
    async def account_overview(self) -> list[dict[str, str]]:
        raise RuntimeError("account unavailable")

    async def position_information_v3(self) -> list[dict[str, object]]:
        return []


class SignalBridgeHelpersTests(unittest.TestCase):
    def test_resolve_snapshots_file_accepts_directory(self) -> None:
        with TemporaryDirectory() as temp_dir:
            resolved = resolve_snapshots_file(Path(temp_dir))
            self.assertEqual(resolved.name, "event_snapshots.jsonl")

    def test_mainnet_shadow_requires_prod_environment(self) -> None:
        args = Namespace(execution_mode=EXECUTION_MODE_MAINNET_SHADOW, env="testnet")

        with self.assertRaises(ValueError):
            validate_execution_mode_args(args)

    def test_mainnet_shadow_disables_user_data_oms(self) -> None:
        self.assertFalse(
            user_data_oms_enabled_for_mode(
                EXECUTION_MODE_MAINNET_SHADOW,
                disable_user_data_oms=False,
            )
        )

    def test_resolve_mainnet_shadow_dir_defaults_next_to_snapshot_feed(self) -> None:
        snapshots_file = Path("/opt/phoenix-testnet/signal_lab_runs/event_collect_v10/event_snapshots.jsonl")

        self.assertEqual(
            resolve_mainnet_shadow_dir(snapshots_file, None),
            snapshots_file.parent / "mainnet_shadow",
        )

    def test_bridge_instance_lock_rejects_duplicate_process(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lock_file = Path(temp_dir) / "bridge-instance.lock"
            first_lock = acquire_bridge_instance_lock(lock_file)
            try:
                with self.assertRaises(BridgeInstanceLockError):
                    acquire_bridge_instance_lock(lock_file)
            finally:
                first_lock.release()

            second_lock = acquire_bridge_instance_lock(lock_file)
            second_lock.release()

    def test_candidate_strategy_report_uses_latest_outcome_per_instance(self) -> None:
        signals = [
            {
                "event_id": "evt-1",
                "playbook": "candidate_a",
                "branch_type": BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
                "shadow_branches": [{"branch_id": "A", "stop_loss_pct": 0.5, "take_profit_pct": 1.0}],
            },
            {
                "event_id": "evt-2",
                "playbook": "candidate_a",
                "branch_type": BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
                "shadow_branches": [{"branch_id": "A", "stop_loss_pct": 0.5, "take_profit_pct": 1.0}],
            },
        ]
        outcomes = [
            {
                "event_id": "evt-1",
                "shadow_instance_id": "evt-1::A",
                "shadow_branch_id": "A",
                "horizon_sec": 60,
                "playbook": "candidate_a",
                "branch_type": BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
                "after_fee_and_slippage_return_pct": 0.2,
            },
            {
                "event_id": "evt-1",
                "shadow_instance_id": "evt-1::A",
                "shadow_branch_id": "A",
                "horizon_sec": 300,
                "playbook": "candidate_a",
                "branch_type": BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
                "after_fee_and_slippage_return_pct": -0.5,
            },
            {
                "event_id": "evt-2",
                "shadow_instance_id": "evt-2::A",
                "shadow_branch_id": "A",
                "horizon_sec": 300,
                "playbook": "candidate_a",
                "branch_type": BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
                "after_fee_and_slippage_return_pct": 1.0,
            },
        ]

        report = build_candidate_strategy_report(
            generated_at="2026-04-29T00:00:00+00:00",
            shadow_signals=signals,
            shadow_outcomes=outcomes,
        )

        self.assertEqual(report["candidate_outcome_count"], 3)
        self.assertEqual(report["latest_candidate_instance_count"], 2)
        self.assertEqual(report["playbooks"]["candidate_a"]["outcome_count"], 2)
        self.assertAlmostEqual(report["playbooks"]["candidate_a"]["avg_return_pct"], 0.25)

    def test_candidate_strategy_control_pauses_negative_forward_shadow(self) -> None:
        report = {
            "playbooks": {
                "bad_candidate": {
                    "signal_count": 12,
                    "outcome_count": 30,
                    "avg_return_pct": -0.01,
                    "profit_factor": 0.79,
                    "win_rate_pct": 40.0,
                },
                "young_candidate": {
                    "signal_count": 3,
                    "outcome_count": 8,
                    "avg_return_pct": -0.2,
                    "profit_factor": 0.2,
                    "win_rate_pct": 25.0,
                },
                "focus_candidate": {
                    "signal_count": 40,
                    "outcome_count": 80,
                    "avg_return_pct": 0.08,
                    "profit_factor": 1.4,
                    "win_rate_pct": 54.0,
                },
            }
        }

        control = update_candidate_strategy_control(
            generated_at="2026-04-29T00:00:00+00:00",
            report=report,
            min_outcomes=30,
            pause_profit_factor=0.8,
            pause_min_avg_return_pct=0.0,
            focus_min_outcomes=80,
            focus_profit_factor=1.3,
        )

        self.assertTrue(candidate_playbook_is_paused(control, "bad_candidate"))
        self.assertFalse(candidate_playbook_is_paused(control, "young_candidate"))
        self.assertEqual(control["playbooks"]["focus_candidate"]["status"], "focus")

    def test_derive_playbook_matches_oi_build_breakout(self) -> None:
        record = {
            "sample_type": "trigger",
            "bar_interval": "5m",
            "trigger_types": ["volume_burst", "range_expansion"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "5m",
                "candle_direction": "up",
            },
            "enrichments": {
                "oi_change_5m_pct": 0.8,
                "oi_change_15m_pct": 1.2,
                "depth_imbalance": 0.05,
            },
        }

        self.assertEqual(derive_playbook(record), "oi_build_breakout")

    def test_derive_signal_side_uses_candle_direction(self) -> None:
        self.assertEqual(
            derive_signal_side(
                {
                    "sample": {"candle_direction": "up"},
                    "trigger_types": ["range_expansion", "body_expansion", "volume_burst"],
                    "bar_interval": "5m",
                    "sample_type": "trigger",
                    "enrichments": {"oi_change_5m_pct": 0.8, "oi_change_15m_pct": 1.2, "depth_imbalance": 0.05},
                }
            ),
            "BUY",
        )
        self.assertEqual(
            derive_signal_side(
                {
                    "sample": {"candle_direction": "down"},
                    "trigger_types": ["range_expansion"],
                    "bar_interval": "5m",
                    "sample_type": "trigger",
                    "enrichments": {"oi_change_5m_pct": 0.1, "oi_change_15m_pct": 0.0, "depth_imbalance": 0.05},
                }
            ),
            "SELL",
        )
        self.assertIsNone(derive_signal_side({"sample": {"candle_direction": "flat"}}))

    def test_derive_playbook_reclassifies_volume_burst_reversal_shape_as_other_trigger(self) -> None:
        record = {
            "sample_type": "trigger",
            "bar_interval": "1m",
            "trigger_types": ["range_expansion", "body_expansion", "volume_burst"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "1m",
                "candle_direction": "down",
                "trigger_candle_direction": "down",
                "confirmation_candle_direction": "up",
                "reversal_confirmation_passed": True,
                "reversal_confirmation_bar_interval": "1m",
            },
            "enrichments": {
                "btcusdt_ret_5m_pct": 0.12,
                "oi_change_5m_pct": -0.05,
                "oi_change_15m_pct": -0.1,
                "depth_imbalance": 0.42,
            },
        }

        self.assertEqual(derive_playbook(record), "other_trigger")

    def test_derive_playbook_rejects_unconfirmed_volume_burst_reversal(self) -> None:
        record = {
            "sample_type": "trigger",
            "bar_interval": "1m",
            "trigger_types": ["range_expansion", "body_expansion", "volume_burst"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "1m",
                "candle_direction": "down",
                "trigger_candle_direction": "down",
                "confirmation_candle_direction": "down",
                "reversal_confirmation_passed": False,
            },
            "enrichments": {
                "btcusdt_ret_5m_pct": 0.12,
                "oi_change_5m_pct": -0.05,
                "oi_change_15m_pct": -0.1,
                "depth_imbalance": 0.42,
            },
        }

        self.assertEqual(derive_playbook(record), "other_trigger")

    def test_derive_playbook_rejects_reversal_against_btc_regime(self) -> None:
        record = {
            "sample_type": "trigger",
            "bar_interval": "1m",
            "trigger_types": ["range_expansion", "body_expansion", "volume_burst"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "1m",
                "candle_direction": "down",
                "trigger_candle_direction": "down",
                "confirmation_candle_direction": "up",
                "reversal_confirmation_passed": True,
            },
            "enrichments": {
                "btcusdt_ret_5m_pct": -0.52,
                "oi_change_5m_pct": -0.05,
                "oi_change_15m_pct": -0.1,
                "depth_imbalance": 0.42,
            },
        }

        self.assertEqual(derive_playbook(record), "other_trigger")

    def test_derive_playbook_rejects_5m_reversal_even_if_confirmed(self) -> None:
        record = {
            "sample_type": "trigger",
            "bar_interval": "5m",
            "trigger_types": ["range_expansion", "body_expansion", "volume_burst"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "5m",
                "candle_direction": "down",
                "trigger_candle_direction": "down",
                "confirmation_candle_direction": "up",
                "reversal_confirmation_passed": True,
            },
            "enrichments": {
                "btcusdt_ret_5m_pct": 0.04,
                "oi_change_5m_pct": -0.05,
                "oi_change_15m_pct": -0.1,
                "depth_imbalance": 0.42,
            },
        }

        self.assertEqual(derive_playbook(record), "other_trigger")

    def test_derive_playbook_matches_preconfirmed_oi_unwind_reversal_candidate(self) -> None:
        record = {
            "playbook": "other_trigger",
            "sample_type": "trigger",
            "bar_interval": "5m",
            "trigger_types": ["range_expansion", "volume_burst"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "5m",
                "candle_direction": "down",
                "trigger_candle_direction": "down",
                "confirmation_candle_direction": "up",
                "reversal_confirmation_passed": True,
                "reversal_confirmation_bar_interval": "1m",
            },
            "enrichments": {
                "oi_change_5m_pct": -1.1,
                "oi_change_15m_pct": -1.6,
                "depth_imbalance": 0.12,
            },
        }

        self.assertEqual(derive_playbook(record), PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED)
        self.assertEqual(derive_signal_side(record), "BUY")

    def test_derive_playbook_matches_liquidation_flush(self) -> None:
        record = {
            "sample_type": "trigger",
            "bar_interval": "5m",
            "trigger_types": ["range_expansion", "body_expansion"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "5m",
                "candle_direction": "down",
            },
            "enrichments": {
                "oi_change_5m_pct": -0.9,
                "oi_change_15m_pct": -1.4,
                "depth_imbalance": 0.26,
            },
        }

        self.assertEqual(derive_playbook(record), "liquidation_flush")
        self.assertEqual(derive_signal_side(record), "BUY")

    def test_strategy_discovery_derives_mild_trend_candidate(self) -> None:
        record = {
            "event_id": "evt-discovery-trend",
            "symbol": "ALTUSDT",
            "trigger_types": ["range_expansion", "volume_burst"],
            "sample": {
                "price": 100.0,
                "candle_direction": "up",
                "trigger_candle_direction": "up",
                "volume_burst_ratio": 1.7,
                "range_to_atr": 1.8,
            },
            "enrichments": {"btcusdt_ret_5m_pct": 0.05},
        }

        candidates = derive_strategy_discovery_candidates(record)

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["playbook"], PLAYBOOK_DISCOVERY_TREND_MILD_CONTINUATION)
        self.assertEqual(candidate["side"], "BUY")
        self.assertEqual(candidate["target_horizons_sec"], (60, 180, 300, 900))
        self.assertEqual(candidate["shadow_branch_specs"][0]["branch_id"], "DISC_TREND_MILD_080_200")

    def test_strategy_discovery_derives_oi_unwind_reversal_scalp_candidate(self) -> None:
        record = {
            "event_id": "evt-discovery-oi",
            "symbol": "ALTUSDT",
            "trigger_types": ["range_expansion"],
            "sample": {
                "price": 100.0,
                "candle_direction": "down",
                "trigger_candle_direction": "down",
            },
            "enrichments": {
                "oi_change_5m_pct": -1.0,
                "btcusdt_ret_5m_pct": 0.05,
            },
        }

        candidates = derive_strategy_discovery_candidates(record)

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["playbook"], PLAYBOOK_DISCOVERY_OI_UNWIND_REVERSAL_SCALP)
        self.assertEqual(candidate["side"], "BUY")
        self.assertEqual(
            [item["branch_id"] for item in candidate["shadow_branch_specs"]],
            ["DISC_OI_UNWIND_REV_050_100", "DISC_OI_UNWIND_REV_080_200"],
        )

    def test_strategy_discovery_derives_oi_unwind_5m_range_reversal_candidate(self) -> None:
        record = {
            "event_id": "evt-discovery-oi-range",
            "symbol": "ALTUSDT",
            "bar_interval": "5m",
            "trigger_types": ["range_expansion"],
            "sample": {
                "price": 100.0,
                "bar_interval": "5m",
                "candle_direction": "down",
                "trigger_candle_direction": "down",
            },
            "enrichments": {
                "oi_change_5m_pct": -1.0,
                "btcusdt_ret_5m_pct": 0.05,
            },
        }

        candidates = derive_strategy_discovery_candidates(record)
        playbooks = [candidate["playbook"] for candidate in candidates]

        self.assertIn(PLAYBOOK_DISCOVERY_OI_UNWIND_5M_RANGE_REVERSAL, playbooks)
        candidate = next(
            item for item in candidates if item["playbook"] == PLAYBOOK_DISCOVERY_OI_UNWIND_5M_RANGE_REVERSAL
        )
        self.assertEqual(candidate["side"], "BUY")
        self.assertEqual(
            [item["branch_id"] for item in candidate["shadow_branch_specs"]],
            ["DISC_OI_UNWIND_5M_RANGE_REV_050_100", "DISC_OI_UNWIND_5M_RANGE_REV_080_200"],
        )

    def test_strategy_discovery_derives_bidwall_oi_build_continuation_candidate(self) -> None:
        record = {
            "event_id": "evt-discovery-bidwall",
            "symbol": "ALTUSDT",
            "trading_session": "US",
            "bar_interval": "5m",
            "trigger_types": ["body_expansion", "range_expansion", "volume_burst"],
            "sample": {
                "price": 100.0,
                "bar_interval": "5m",
                "candle_direction": "up",
                "trigger_candle_direction": "up",
            },
            "enrichments": {
                "oi_change_5m_pct": 0.8,
                "btcusdt_ret_5m_pct": 0.05,
                "depth_imbalance": 0.42,
            },
        }

        candidates = derive_strategy_discovery_candidates(record)
        playbooks = [candidate["playbook"] for candidate in candidates]

        self.assertIn(PLAYBOOK_DISCOVERY_BIDWALL_OI_BUILD_CONTINUATION, playbooks)
        candidate = next(
            item for item in candidates if item["playbook"] == PLAYBOOK_DISCOVERY_BIDWALL_OI_BUILD_CONTINUATION
        )
        self.assertEqual(candidate["side"], "BUY")
        self.assertEqual(candidate["shadow_branch_specs"][0]["branch_id"], "DISC_BIDWALL_OI_BUILD_080_200")
        self.assertTrue(candidate["shadow_branch_specs"][0]["research_only"])

    def test_strategy_discovery_derives_factor_trend_alignment_candidate(self) -> None:
        record = {
            "event_id": "evt-factor-trend",
            "symbol": "ALTUSDT",
            "bar_interval": "1m",
            "trigger_types": ["body_expansion", "range_expansion", "volume_burst"],
            "sample": {
                "price": 100.0,
                "bar_interval": "1m",
                "candle_direction": "up",
                "trigger_candle_direction": "up",
                "volume_burst_ratio": 3.5,
                "range_to_atr": 2.6,
                "body_to_atr": 1.8,
                "ret_1bar_pct": 1.2,
                "ret_5bar_pct": 2.2,
                "ret_15bar_pct": 2.8,
            },
            "enrichments": {
                "oi_change_5m_pct": 1.1,
                "oi_change_15m_pct": 1.8,
                "btcusdt_ret_5m_pct": 0.1,
                "depth_imbalance": 0.38,
                "spread_bps": 2.0,
                "estimated_slippage_bps": 3.0,
                "bid_depth_notional_5": 500_000.0,
                "ask_depth_notional_5": 400_000.0,
                "taker_buy_ratio_5m": 1.3,
            },
        }

        candidates = derive_strategy_discovery_candidates(record)
        playbooks = [candidate["playbook"] for candidate in candidates]

        self.assertIn(PLAYBOOK_DISCOVERY_FACTOR_TREND_ALIGNMENT, playbooks)
        candidate = next(item for item in candidates if item["playbook"] == PLAYBOOK_DISCOVERY_FACTOR_TREND_ALIGNMENT)
        self.assertEqual(candidate["side"], "BUY")
        self.assertEqual(
            [item["branch_id"] for item in candidate["shadow_branch_specs"]],
            ["DISC_FACTOR_TREND_050_150", "DISC_FACTOR_TREND_080_200"],
        )
        self.assertTrue(candidate["shadow_branch_specs"][0]["research_only"])

    def test_strategy_discovery_derives_factor_liquidation_reversion_candidate(self) -> None:
        record = {
            "event_id": "evt-factor-liq-reversion",
            "symbol": "ALTUSDT",
            "bar_interval": "1m",
            "trigger_types": ["range_expansion", "volume_burst"],
            "sample": {
                "price": 100.0,
                "bar_interval": "1m",
                "candle_direction": "down",
                "trigger_candle_direction": "down",
                "volume_burst_ratio": 3.1,
                "range_to_atr": 2.4,
                "body_to_atr": 1.6,
                "ret_1bar_pct": -1.4,
                "ret_5bar_pct": -2.1,
                "ret_15bar_pct": -2.8,
            },
            "enrichments": {
                "oi_change_5m_pct": -1.0,
                "oi_change_15m_pct": -1.8,
                "btcusdt_ret_5m_pct": 0.02,
                "liquidation_long_usd_15m": 180_000.0,
                "liquidation_short_usd_15m": 0.0,
                "spread_bps": 3.0,
                "estimated_slippage_bps": 4.0,
                "bid_depth_notional_5": 300_000.0,
                "ask_depth_notional_5": 320_000.0,
            },
        }

        candidates = derive_strategy_discovery_candidates(record)
        playbooks = [candidate["playbook"] for candidate in candidates]

        self.assertIn(PLAYBOOK_DISCOVERY_FACTOR_LIQUIDATION_REVERSION, playbooks)
        candidate = next(item for item in candidates if item["playbook"] == PLAYBOOK_DISCOVERY_FACTOR_LIQUIDATION_REVERSION)
        self.assertEqual(candidate["side"], "BUY")
        self.assertEqual(
            [item["branch_id"] for item in candidate["shadow_branch_specs"]],
            ["DISC_FACTOR_LIQ_REV_050_100", "DISC_FACTOR_LIQ_REV_080_200"],
        )

    def test_strategy_discovery_derives_shadow_oi_build_strategy_a_and_b(self) -> None:
        record = {
            "event_id": "evt-oi-build-v2",
            "symbol": "ALTUSDT",
            "playbook": "oi_build_breakout",
            "trigger_score": 65.0,
            "trigger_types": ["body_expansion", "range_expansion", "volume_burst"],
            "sample": {
                "price": 100.0,
                "bar_interval": "1m",
                "candle_direction": "up",
                "trigger_candle_direction": "up",
            },
            "enrichments": {
                "oi_change_5m_pct": 2.25,
                "oi_change_15m_pct": 5.5,
            },
            "factors": {
                "trend_bucket": "trend_flat",
                "mean_reversion_bucket": "reversion_flat",
                "oi_unwind_bucket": "oi_unwind_low",
            },
        }

        candidates = derive_strategy_discovery_candidates(record)
        by_playbook = {item["playbook"]: item for item in candidates}

        self.assertIn(PLAYBOOK_SHADOW_OI_BUILD_QUALITY, by_playbook)
        self.assertIn(PLAYBOOK_SHADOW_OI_BUILD_BALANCED, by_playbook)
        quality = by_playbook[PLAYBOOK_SHADOW_OI_BUILD_QUALITY]
        balanced = by_playbook[PLAYBOOK_SHADOW_OI_BUILD_BALANCED]
        self.assertEqual(quality["side"], "BUY")
        self.assertTrue(quality["research_only"])
        self.assertTrue(quality["shadow_only"])
        self.assertFalse(quality["live_unlock_eligible"])
        self.assertFalse(quality["readiness_eligible"])
        self.assertFalse(quality["live_trading_enabled"])
        self.assertFalse(quality["promotion_allowed"])
        self.assertEqual(quality["target_horizons_sec"], (900, 1800, 3600))
        self.assertEqual(quality["candidate_conditions"]["rule_id"], "OI_BUILD_STRATEGY_A_QUALITY")
        self.assertEqual(quality["candidate_conditions"]["oi_change_5m_pct"], ">=1.0")
        self.assertEqual(quality["candidate_conditions"]["oi_change_15m_pct"], ">=5.0")
        self.assertEqual(quality["candidate_conditions"]["trigger_score"], ">=60")
        self.assertEqual(
            [item["branch_id"] for item in quality["shadow_branch_specs"]],
            ["OI_BUILD_QUALITY_130_350"],
        )
        self.assertTrue(quality["shadow_branch_specs"][0]["research_only"])
        self.assertEqual(balanced["candidate_conditions"]["rule_id"], "OI_BUILD_STRATEGY_B_BALANCED")
        self.assertEqual(balanced["candidate_conditions"]["oi_change_5m_pct"], ">=2.0")
        self.assertNotIn("oi_change_15m_pct", balanced["candidate_conditions"])
        self.assertEqual(
            [item["branch_id"] for item in balanced["shadow_branch_specs"]],
            ["OI_BUILD_BALANCED_130_350"],
        )

    def test_strategy_discovery_applies_distinct_oi_build_shadow_thresholds(self) -> None:
        base_record = {
            "event_id": "evt-oi-build-thresholds",
            "symbol": "ALTUSDT",
            "playbook": "oi_build_breakout",
            "trigger_score": 65.0,
            "trigger_types": ["range_expansion"],
            "sample": {
                "price": 100.0,
                "candle_direction": "up",
                "trigger_candle_direction": "up",
            },
            "factors": {
                "trend_bucket": "trend_flat",
                "mean_reversion_bucket": "reversion_flat",
                "oi_unwind_bucket": "oi_unwind_low",
            },
        }
        quality_only = {
            **base_record,
            "enrichments": {"oi_change_5m_pct": 1.25, "oi_change_15m_pct": 5.5},
        }
        balanced_only = {
            **base_record,
            "enrichments": {"oi_change_5m_pct": 2.25, "oi_change_15m_pct": 1.0},
        }

        quality_playbooks = {item["playbook"] for item in derive_strategy_discovery_candidates(quality_only)}
        balanced_playbooks = {item["playbook"] for item in derive_strategy_discovery_candidates(balanced_only)}

        self.assertIn(PLAYBOOK_SHADOW_OI_BUILD_QUALITY, quality_playbooks)
        self.assertNotIn(PLAYBOOK_SHADOW_OI_BUILD_BALANCED, quality_playbooks)
        self.assertIn(PLAYBOOK_SHADOW_OI_BUILD_BALANCED, balanced_playbooks)
        self.assertNotIn(PLAYBOOK_SHADOW_OI_BUILD_QUALITY, balanced_playbooks)

    def test_strategy_discovery_derives_hist_buy_1h_trend_reversion_survivor(self) -> None:
        record = {
            "event_id": "evt-hist-trend-reversion",
            "symbol": "ALTUSDT",
            "trigger_types": ["body_expansion"],
            "sample": {
                "price": 100.0,
                "candle_direction": "up",
                "trigger_candle_direction": "up",
            },
            "factors": {
                "trend_bucket": "trend_long_extreme",
                "mean_reversion_bucket": "reversion_short_extreme",
                "oi_unwind_bucket": "oi_unwind_low",
            },
        }

        candidates = derive_strategy_discovery_candidates(record)
        candidate = next(
            item
            for item in candidates
            if item["playbook"] == PLAYBOOK_DISCOVERY_HIST_BUY_1H_TREND_REVERSION_EXTREME
        )

        self.assertEqual(candidate["side"], "BUY")
        self.assertEqual(candidate["target_horizons_sec"], (3600,))
        self.assertTrue(candidate["research_only"])
        self.assertFalse(candidate["live_unlock_eligible"])
        self.assertEqual(candidate["candidate_conditions"]["cross_shard_oos_samples"], 381)
        self.assertEqual(candidate["shadow_branch_specs"][0]["branch_id"], "HIST_BUY_1H")
        self.assertEqual(candidate["shadow_branch_specs"][0]["take_profit_pct"], 0.0)
        self.assertTrue(candidate["shadow_branch_specs"][0]["research_only"])

    def test_strategy_discovery_derives_hist_buy_1h_reversion_oi_unwind_survivor(self) -> None:
        record = {
            "event_id": "evt-hist-reversion-oi",
            "symbol": "ALTUSDT",
            "trigger_types": ["range_expansion"],
            "sample": {
                "price": 100.0,
                "candle_direction": "up",
                "trigger_candle_direction": "up",
            },
            "factors": {
                "trend_bucket": "trend_long_mild",
                "mean_reversion_bucket": "reversion_short_extreme",
                "oi_unwind_bucket": "oi_unwind_low",
            },
        }

        candidates = derive_strategy_discovery_candidates(record)
        candidate = next(
            item
            for item in candidates
            if item["playbook"] == PLAYBOOK_DISCOVERY_HIST_BUY_1H_REVERSION_OI_UNWIND_LOW
        )

        self.assertEqual(candidate["side"], "BUY")
        self.assertEqual(candidate["target_horizons_sec"], (3600,))
        self.assertTrue(candidate["shadow_only"])
        self.assertFalse(candidate["readiness_eligible"])
        self.assertEqual(candidate["candidate_conditions"]["cross_shard_oos_samples"], 382)
        self.assertEqual(candidate["shadow_branch_specs"][0]["branch_id"], "HIST_BUY_1H")

    def test_strategy_discovery_derives_low_liq_depth_reversal_candidate(self) -> None:
        record = {
            "event_id": "evt-discovery-low-liq",
            "symbol": "ALTUSDT",
            "trigger_types": ["range_expansion", "volume_burst"],
            "sample": {
                "price": 100.0,
                "candle_direction": "up",
                "trigger_candle_direction": "up",
            },
            "enrichments": {
                "depth_imbalance": 0.22,
                "liquidation_long_usd_15m": 0.0,
            },
        }

        candidates = derive_strategy_discovery_candidates(record)

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["playbook"], PLAYBOOK_DISCOVERY_LOW_LIQ_DEPTH_REVERSAL)
        self.assertEqual(candidate["side"], "SELL")
        self.assertEqual(candidate["target_horizons_sec"], (300, 900))
        self.assertEqual(candidate["shadow_branch_specs"][0]["branch_id"], "DISC_LOW_LIQ_DEPTH_REV_080_200")

    def test_strategy_discovery_rejects_missing_or_out_of_bucket_features(self) -> None:
        self.assertEqual(derive_strategy_discovery_candidates({"sample": {"candle_direction": "flat"}}), [])
        self.assertEqual(
            derive_strategy_discovery_candidates(
                {
                    "trigger_types": ["volume_burst"],
                    "sample": {
                        "candle_direction": "up",
                        "volume_burst_ratio": 3.0,
                        "range_to_atr": 1.8,
                    },
                }
            ),
            [],
        )
        self.assertEqual(build_strategy_discovery_branch_specs("unknown", "BUY"), [])

    def test_resolve_effective_quote_allocation_uses_low_balance_mode(self) -> None:
        self.assertEqual(
            resolve_effective_quote_allocation_usdt(
                base_quote_allocation_usdt=500.0,
                available_balance_usdt=1500.0,
                low_balance_threshold_usdt=2000.0,
                low_balance_quote_allocation_usdt=200.0,
            ),
            200.0,
        )

    def test_resolve_effective_quote_allocation_keeps_base_when_balance_is_healthy(self) -> None:
        self.assertEqual(
            resolve_effective_quote_allocation_usdt(
                base_quote_allocation_usdt=500.0,
                available_balance_usdt=3500.0,
                low_balance_threshold_usdt=2000.0,
                low_balance_quote_allocation_usdt=200.0,
            ),
            500.0,
        )

    def test_parse_args_defaults_align_with_best_runtime_profile(self) -> None:
        with patch("sys.argv", ["phoenix_signal_bridge.py", "--snapshots-file", "snapshots.jsonl"]):
            args = parse_args()

        self.assertEqual(args.execution_mode, "TESTNET_LIVE")
        self.assertEqual(args.quote_allocation, 200.0)
        self.assertEqual(args.stop_loss_pct, 1.3)
        self.assertEqual(args.take_profit_pct, 3.5)
        self.assertEqual(args.max_open_positions, 30)
        self.assertEqual(args.live_universe_top, 0)
        self.assertEqual(args.min_24h_quote_volume, 50_000_000.0)
        self.assertEqual(args.min_1h_quote_volume, 2_000_000.0)
        self.assertEqual(args.min_24h_volatility_pct, 1.0)
        self.assertEqual(args.hard_kill_drawdown_threshold_pct, 3.0)
        self.assertEqual(args.btc_regime_drop_threshold_pct, 1.0)
        self.assertFalse(args.research_shadow_all_triggers)
        self.assertFalse(args.disable_discovery_candidates)
        self.assertFalse(args.candidate_parking_lot_only)
        self.assertTrue(candidate_shadow_writes_enabled(args))
        self.assertFalse(candidate_parking_lot_enabled(args))
        self.assertEqual(args.research_shadow_target_horizons_sec, "60,180,300,900")
        self.assertEqual(args.shadow_round_trip_fee_bps, 8.0)
        self.assertFalse(args.disable_momentum_scalp_missing_one_min_oi_fallback)
        self.assertFalse(args.require_momentum_scalp_one_min_oi_hard_gate)
        self.assertEqual(args.momentum_scalp_min_five_min_oi_fallback_pct, 0.2)
        self.assertIn("ENSOUSDT", args.blocked_symbols)

    def test_parse_args_can_disable_discovery_candidate_writes(self) -> None:
        with patch(
            "sys.argv",
            [
                "phoenix_signal_bridge.py",
                "--snapshots-file",
                "snapshots.jsonl",
                "--disable-discovery-candidates",
            ],
        ):
            args = parse_args()

        self.assertTrue(args.disable_discovery_candidates)
        self.assertFalse(candidate_shadow_writes_enabled(args))

    def test_parse_args_enables_candidate_parking_lot_only(self) -> None:
        with patch(
            "sys.argv",
            [
                "phoenix_signal_bridge.py",
                "--snapshots-file",
                "snapshots.jsonl",
                "--candidate-parking-lot-only",
                "--candidate-parking-lot-file",
                "parking.jsonl",
            ],
        ):
            args = parse_args()

        self.assertTrue(args.candidate_parking_lot_only)
        self.assertTrue(candidate_parking_lot_enabled(args))
        self.assertEqual(args.candidate_parking_lot_file, Path("parking.jsonl"))

    def test_candidate_parking_lot_payload_is_reporting_only(self) -> None:
        payload = build_candidate_parking_lot_payload(
            reason="candidate_parking_lot_only",
            record={
                "event_id": "evt-1",
                "symbol": "BTCUSDT",
                "trigger_types": ["volume_burst"],
                "sample": {"price": 100.0},
                "enrichments": {"oi_change_5m_pct": 1.2},
            },
            playbook="discovery_test",
            side="BUY",
            branch_type=BRANCH_TYPE_STRATEGY_DISCOVERY_CANDIDATE,
            candidate_event_id="evt-1::candidate",
            source_event_id="evt-1",
            shadow_branch_specs=[{"branch_id": "A"}],
            target_horizons_sec=[60, 300],
            candidate={"candidate_source": "unit_test"},
        )

        self.assertEqual(payload["event"], "candidate_discovery_parking_lot")
        self.assertTrue(payload["parking_lot_only"])
        self.assertFalse(payload["shadow_signal_written"])
        self.assertFalse(payload["live_trading_enabled"])
        self.assertFalse(payload["promotion_allowed"])
        self.assertEqual(payload["playbook"], "discovery_test")
        self.assertEqual(payload["target_horizons_sec"], [60, 300])

    def test_parse_args_enables_research_shadow_all_triggers_flag(self) -> None:
        with patch(
            "sys.argv",
            [
                "phoenix_signal_bridge.py",
                "--snapshots-file",
                "snapshots.jsonl",
                "--research-shadow-all-triggers",
            ],
        ):
            args = parse_args()

        self.assertTrue(args.research_shadow_all_triggers)

    def test_parse_positive_int_csv_dedupes_and_rejects_non_positive_values(self) -> None:
        self.assertEqual(parse_positive_int_csv("60,180,60", default=[900]), [60, 180])
        self.assertEqual(parse_positive_int_csv("", default=[900]), [900])

        with self.assertRaises(ValueError):
            parse_positive_int_csv("60,0", default=[900])

    def test_parse_shadow_branch_specs_parses_three_branch_profiles(self) -> None:
        branches = parse_shadow_branch_specs("A:1.3:3.5,B:1.5:5.0,C:0.8:2.0")

        self.assertEqual([item["branch_id"] for item in branches], ["A", "B", "C"])
        self.assertAlmostEqual(branches[0]["stop_loss_pct"], 1.3, places=6)
        self.assertAlmostEqual(branches[1]["take_profit_pct"], 5.0, places=6)
        self.assertAlmostEqual(branches[2]["stop_loss_pct"], 0.8, places=6)

    def test_build_research_shadow_signal_payload_records_all_trigger_pool(self) -> None:
        payload = build_research_shadow_signal_payload(
            {
                "event": "market_event_created",
                "event_id": "evt-research-all",
                "symbol": "ALTUSDT",
                "playbook": "unsupported_collector_trigger",
                "trigger_score": 88,
                "trigger_types": ["collector_only"],
                "sample": {
                    "candle_direction": "up",
                    "price": 12.5,
                    "anchor_close_time_ms": 1_777_000_000_000,
                    "quote_volume_24h": 123_000_000,
                },
                "observed_at_ms": 1_777_000_000_000,
            }
        )

        self.assertEqual(payload["event_id"], "evt-research-all::research_pool")
        self.assertEqual(payload["source_event_id"], "evt-research-all")
        self.assertEqual(payload["playbook"], "unsupported_collector_trigger")
        self.assertEqual(payload["side"], "BUY")
        self.assertEqual(payload["branch_type"], BRANCH_TYPE_RESEARCH_POOL)
        self.assertEqual(payload["shadow_reason"], SHADOW_REASON_RESEARCH_POOL)
        self.assertEqual(payload["shadow_target_horizons_sec"], [60, 180, 300, 900])
        self.assertEqual(shadow_target_horizons(payload), [60, 180, 300, 900])
        self.assertTrue(payload["research_only"])
        self.assertFalse(payload["live_unlock_eligible"])
        self.assertFalse(payload["exchange_preflight_requested"])
        self.assertNotIn("virtual_intent", payload)

        branches = payload["shadow_branches"]
        self.assertEqual(len(branches), 6)
        self.assertEqual(
            {(item["stop_loss_pct"], item["take_profit_pct"]) for item in branches},
            {(0.35, 0.7), (0.5, 1.0), (0.8, 2.0)},
        )
        self.assertEqual(
            {item["side"] for item in branches if item["direction_variant"] == "trend"},
            {"BUY"},
        )
        self.assertEqual(
            {item["side"] for item in branches if item["direction_variant"] == "reversal"},
            {"SELL"},
        )

    def test_build_research_shadow_signal_payload_records_observation_without_direction(self) -> None:
        payload = build_research_shadow_signal_payload(
            {
                "event": "market_event_created",
                "event_id": "evt-research-observation",
                "symbol": "ALTUSDT",
                "sample": {"candle_direction": "flat", "price": 12.5},
            }
        )

        self.assertIsNone(payload["side"])
        self.assertEqual(payload["branch_type"], BRANCH_TYPE_RESEARCH_POOL)
        self.assertEqual(payload["shadow_reason"], SHADOW_REASON_RESEARCH_OBSERVATION)
        self.assertTrue(payload["research_only"])
        self.assertTrue(payload["research_observation"])
        self.assertEqual(payload["shadow_branches"], [])

    def test_persist_research_shadow_signal_writes_without_exchange_order_calls(self) -> None:
        futures = FakeMainnetShadowFutures()
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pending: dict[str, dict[str, object]] = {}

            payload = persist_research_shadow_signal(
                record={
                    "event": "market_event_created",
                    "event_id": "evt-persist-research",
                    "symbol": "ALTUSDT",
                    "sample": {"candle_direction": "down", "price": 12.5},
                },
                shadow_log_file=temp_path / "shadow.jsonl",
                shadow_state_file=temp_path / "shadow-state.json",
                horizon_labels_file=temp_path / "horizon.jsonl",
                horizon_labels_offset=0,
                pending_shadow_by_event_id=pending,
            )

            rows = [
                json.loads(line)
                for line in (temp_path / "shadow.jsonl").read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_id"], "evt-persist-research::research_pool")
        self.assertTrue(rows[0]["research_only"])
        self.assertIn(payload["event_id"], pending)
        self.assertEqual(futures.test_orders, [])
        self.assertEqual(futures.new_orders, [])

    def test_persist_research_shadow_signal_accepts_custom_target_horizons(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pending: dict[str, dict[str, object]] = {}

            payload = persist_research_shadow_signal(
                record={
                    "event": "market_event_created",
                    "event_id": "evt-persist-research-custom-horizon",
                    "symbol": "ALTUSDT",
                    "sample": {"candle_direction": "down", "price": 12.5},
                },
                shadow_log_file=temp_path / "shadow.jsonl",
                shadow_state_file=temp_path / "shadow-state.json",
                horizon_labels_file=temp_path / "horizon.jsonl",
                horizon_labels_offset=0,
                pending_shadow_by_event_id=pending,
                target_horizons_sec=[60, 180],
            )

        self.assertEqual(payload["shadow_target_horizons_sec"], [60, 180])
        self.assertEqual(shadow_target_horizons(pending[payload["event_id"]]), [60, 180])

    def test_research_observation_without_direction_does_not_enter_pending_simulation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pending: dict[str, dict[str, object]] = {}

            payload = persist_research_shadow_signal(
                record={
                    "event": "market_event_created",
                    "event_id": "evt-research-observation-no-direction",
                    "symbol": "ALTUSDT",
                    "sample": {"candle_direction": "flat", "price": 12.5},
                },
                shadow_log_file=temp_path / "shadow.jsonl",
                shadow_state_file=temp_path / "shadow-state.json",
                horizon_labels_file=temp_path / "horizon.jsonl",
                horizon_labels_offset=0,
                pending_shadow_by_event_id=pending,
            )

        self.assertTrue(payload["research_observation"])
        self.assertEqual(payload["shadow_branches"], [])
        self.assertNotIn(payload["event_id"], pending)

    def test_load_shadow_state_drops_legacy_research_observation_pending_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            shadow_state_file = temp_path / "shadow-state.json"
            horizon_file = temp_path / "horizon.jsonl"
            shadow_state_file.write_text(
                json.dumps(
                    {
                        "horizon_labels_file": str(horizon_file),
                        "horizon_labels_offset": 0,
                        "pending_by_event_id": {
                            "evt-observation::research_pool": {
                                "event_id": "evt-observation::research_pool",
                                "branch_type": BRANCH_TYPE_RESEARCH_POOL,
                                "research_only": True,
                                "research_observation": True,
                                "shadow_branches": [
                                    {
                                        "branch_id": "LEGACY",
                                        "stop_loss_pct": 0.01,
                                        "take_profit_pct": 0.0,
                                    }
                                ],
                            },
                            "evt-tradeable::research_pool": {
                                "event_id": "evt-tradeable::research_pool",
                                "branch_type": BRANCH_TYPE_RESEARCH_POOL,
                                "research_only": True,
                                "shadow_branches": [
                                    {
                                        "branch_id": "TREND_SCALP_035_070",
                                        "stop_loss_pct": 0.35,
                                        "take_profit_pct": 0.7,
                                        "side": "BUY",
                                    }
                                ],
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            _, pending = load_shadow_state(shadow_state_file, horizon_labels_file=horizon_file)

        self.assertNotIn("evt-observation::research_pool", pending)
        self.assertIn("evt-tradeable::research_pool", pending)

    def test_tradeable_symbol_allows_official_unicode_usdt_contracts(self) -> None:
        self.assertTrue(is_tradeable_symbol("我踏马来了USDT"))
        self.assertTrue(is_tradeable_symbol("币安人生USDT"))
        self.assertTrue(is_tradeable_symbol("龙虾USDT"))

    def test_discover_universe_keeps_unicode_usdt_symbols(self) -> None:
        universe = discover_universe(
            [
                {"symbol": "我踏马来了USDT", "quoteVolume": "12000000", "priceChangePercent": "8", "lastPrice": "0.1"},
                {"symbol": "BTCUSDT", "quoteVolume": "10000000", "priceChangePercent": "1", "lastPrice": "90000"},
                {"symbol": "ETHUSDC", "quoteVolume": "50000000", "priceChangePercent": "2", "lastPrice": "3000"},
            ],
            top_limit=10,
            min_quote_volume=0,
        )

        self.assertEqual([item["symbol"] for item in universe], ["我踏马来了USDT", "BTCUSDT"])

    def test_build_live_tradable_universe_keeps_top_ranked_eligible_symbols(self) -> None:
        ticker_payload = [
            {"symbol": "BTCUSDT", "quoteVolume": "1000000"},
            {"symbol": "ETHUSDT", "quoteVolume": "900000"},
            {"symbol": "BNBUSDT", "quoteVolume": "800000"},
            {"symbol": "我踏马来了USDT", "quoteVolume": "700000"},
            {"symbol": "SOLUSDT", "quoteVolume": "700000"},
            {"symbol": "BADUSDT", "quoteVolume": "not-a-number"},
        ]
        exchange_info_payload = {
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                {"symbol": "ETHUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                {"symbol": "我踏马来了USDT", "status": "TRADING", "contractType": "PERPETUAL"},
                {"symbol": "BNBUSDT", "status": "BREAK", "contractType": "PERPETUAL"},
                {"symbol": "SOLUSDT", "status": "TRADING", "contractType": "CURRENT_QUARTER"},
            ]
        }

        symbols, rank_lookup, quote_volume_lookup = build_live_tradable_universe(
            ticker_payload,
            exchange_info_payload,
            top_limit=3,
        )

        self.assertEqual(symbols, {"BTCUSDT", "ETHUSDT", "我踏马来了USDT"})
        self.assertEqual(rank_lookup, {"BTCUSDT": 1, "ETHUSDT": 2, "我踏马来了USDT": 3})
        self.assertEqual(
            quote_volume_lookup,
            {"BTCUSDT": 1_000_000.0, "ETHUSDT": 900_000.0, "我踏马来了USDT": 700_000.0},
        )

    def test_build_live_tradable_universe_allows_positive_volume_without_exchange_filter(self) -> None:
        ticker_payload = [
            {"symbol": "DOGEUSDT", "quoteVolume": "1500"},
            {"symbol": "XRPUSDT", "quoteVolume": "2500"},
            {"symbol": "ZEROUSDT", "quoteVolume": "0"},
        ]

        symbols, rank_lookup, quote_volume_lookup = build_live_tradable_universe(
            ticker_payload,
            {},
            top_limit=10,
        )

        self.assertEqual(symbols, {"DOGEUSDT", "XRPUSDT"})
        self.assertEqual(rank_lookup["XRPUSDT"], 1)
        self.assertEqual(rank_lookup["DOGEUSDT"], 2)
        self.assertEqual(quote_volume_lookup["XRPUSDT"], 2500.0)

    def test_build_live_tradable_universe_supports_min_quote_volume_threshold(self) -> None:
        ticker_payload = [
            {"symbol": "DOGEUSDT", "quoteVolume": "60000000"},
            {"symbol": "HYPEUSDT", "quoteVolume": "51000000"},
            {"symbol": "PAXGUSDT", "quoteVolume": "49000000"},
        ]
        exchange_info_payload = {
            "symbols": [
                {"symbol": "DOGEUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                {"symbol": "HYPEUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                {"symbol": "PAXGUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
            ]
        }

        symbols, rank_lookup, quote_volume_lookup = build_live_tradable_universe(
            ticker_payload,
            exchange_info_payload,
            top_limit=0,
            min_quote_volume_24h=50_000_000,
        )

        self.assertEqual(symbols, {"DOGEUSDT", "HYPEUSDT"})
        self.assertEqual(rank_lookup["DOGEUSDT"], 1)
        self.assertEqual(quote_volume_lookup["HYPEUSDT"], 51_000_000.0)

    def test_evaluate_live_universe_gate_fails_closed_until_refresh_succeeds(self) -> None:
        allowed, reason, extra_fields = evaluate_live_universe_gate(
            symbol="BTCUSDT",
            live_universe_top=50,
            min_quote_volume_24h=0.0,
            live_universe_ready=False,
            tradable_symbols=set(),
            tradable_rank_by_symbol={},
            tradable_quote_volume_by_symbol={},
        )

        self.assertFalse(allowed)
        self.assertEqual(reason, "live_top50_universe_unavailable")
        self.assertEqual(extra_fields["live_universe_top"], 50)

    def test_evaluate_live_universe_gate_blocks_symbols_outside_top_list(self) -> None:
        allowed, reason, extra_fields = evaluate_live_universe_gate(
            symbol="DOGEUSDT",
            live_universe_top=50,
            min_quote_volume_24h=0.0,
            live_universe_ready=True,
            tradable_symbols={"BTCUSDT", "ETHUSDT"},
            tradable_rank_by_symbol={"BTCUSDT": 1, "ETHUSDT": 2},
            tradable_quote_volume_by_symbol={"BTCUSDT": 1_000_000.0, "ETHUSDT": 900_000.0},
        )

        self.assertFalse(allowed)
        self.assertEqual(reason, "outside_live_top50_liquidity_universe")
        self.assertIsNone(extra_fields["live_universe_rank"])

    def test_evaluate_live_universe_gate_allows_ranked_symbol(self) -> None:
        allowed, reason, extra_fields = evaluate_live_universe_gate(
            symbol="ETHUSDT",
            live_universe_top=50,
            min_quote_volume_24h=0.0,
            live_universe_ready=True,
            tradable_symbols={"BTCUSDT", "ETHUSDT"},
            tradable_rank_by_symbol={"BTCUSDT": 1, "ETHUSDT": 2},
            tradable_quote_volume_by_symbol={"BTCUSDT": 1_000_000.0, "ETHUSDT": 900_000.0},
        )

        self.assertTrue(allowed)
        self.assertIsNone(reason)
        self.assertEqual(extra_fields["live_universe_rank"], 2)
        self.assertEqual(extra_fields["live_universe_quote_volume_24h"], 900_000.0)

    def test_evaluate_live_universe_gate_supports_dynamic_quote_volume_mode(self) -> None:
        allowed, reason, extra_fields = evaluate_live_universe_gate(
            symbol="HYPEUSDT",
            live_universe_top=0,
            min_quote_volume_24h=50_000_000.0,
            live_universe_ready=True,
            tradable_symbols={"DOGEUSDT", "HYPEUSDT"},
            tradable_rank_by_symbol={"DOGEUSDT": 1, "HYPEUSDT": 2},
            tradable_quote_volume_by_symbol={"DOGEUSDT": 60_000_000.0, "HYPEUSDT": 55_000_000.0},
        )

        self.assertTrue(allowed)
        self.assertEqual(extra_fields["min_quote_volume_24h"], 50_000_000.0)

    def test_evaluate_intraday_liquidity_gate_blocks_low_one_hour_turnover(self) -> None:
        allowed, reason, extra_fields = evaluate_intraday_liquidity_gate(
            symbol="HYPEUSDT",
            quote_volume_24h=55_000_000.0,
            min_quote_volume_24h=50_000_000.0,
            quote_volume_1h=1_500_000.0,
            min_quote_volume_1h=2_000_000.0,
        )

        self.assertFalse(allowed)
        self.assertEqual(reason, "below_min_1h_quote_volume")
        self.assertEqual(extra_fields["quote_volume_1h"], 1_500_000.0)

    def test_evaluate_degen_shadow_gate_allows_relaxed_24h_liquidity(self) -> None:
        allowed, reason, extra_fields = evaluate_degen_shadow_gate(
            symbol="ALTUSDT",
            quote_volume_24h=12_500_000.0,
            min_quote_volume_24h=10_000_000.0,
        )

        self.assertTrue(allowed)
        self.assertIsNone(reason)
        self.assertEqual(extra_fields["branch_type"], BRANCH_TYPE_DEGEN_HIGH_YIELD)
        self.assertAlmostEqual(float(extra_fields["degen_shadow_quote_volume_24h"]), 12_500_000.0, places=6)

    def test_compute_24h_volatility_pct_uses_high_low_range(self) -> None:
        volatility = compute_24h_volatility_pct(
            {
                "openPrice": "100",
                "highPrice": "101.2",
                "lowPrice": "99.4",
            }
        )

        self.assertIsNotNone(volatility)
        assert volatility is not None
        self.assertAlmostEqual(volatility, 1.8, places=6)

    def test_evaluate_symbol_risk_gate_blocks_permanent_blacklist(self) -> None:
        allowed, reason, extra_fields = evaluate_symbol_risk_gate(
            symbol="XAUUSDT",
            record={"sample": {"quote_volume_24h": 10_000_000.0, "price_change_24h_pct": 0.2}},
            blocked_symbols={"XAUUSDT"},
            min_24h_volatility_pct=1.0,
            live_market_stats_by_symbol={
                "XAUUSDT": {
                    "quote_volume_24h": 10_000_000.0,
                    "price_change_24h_pct": 0.2,
                    "volatility_24h_pct": 0.4,
                }
            },
        )

        self.assertFalse(allowed)
        self.assertEqual(reason, "permanent_symbol_blacklist")
        self.assertEqual(extra_fields["permanent_blocked_symbol"], "XAUUSDT")

    def test_evaluate_symbol_risk_gate_routes_low_volatility_to_shadow(self) -> None:
        allowed, reason, extra_fields = evaluate_symbol_risk_gate(
            symbol="PAXGUSDT",
            record={"sample": {"quote_volume_24h": 42_000_000.0, "price_change_24h_pct": 0.3}},
            blocked_symbols=set(),
            min_24h_volatility_pct=1.0,
            live_market_stats_by_symbol={
                "PAXGUSDT": {
                    "quote_volume_24h": 42_000_000.0,
                    "price_change_24h_pct": 0.3,
                    "volatility_24h_pct": 0.62,
                }
            },
        )

        self.assertFalse(allowed)
        self.assertEqual(reason, "low_24h_volatility")
        self.assertAlmostEqual(float(extra_fields["volatility_24h_pct"]), 0.62, places=6)

    def test_evaluate_symbol_risk_gate_allows_high_volatility_symbol(self) -> None:
        allowed, reason, extra_fields = evaluate_symbol_risk_gate(
            symbol="DOGEUSDT",
            record={"sample": {"quote_volume_24h": 300_000_000.0, "price_change_24h_pct": 4.2}},
            blocked_symbols=set(),
            min_24h_volatility_pct=1.0,
            live_market_stats_by_symbol={
                "DOGEUSDT": {
                    "quote_volume_24h": 300_000_000.0,
                    "price_change_24h_pct": 4.2,
                    "volatility_24h_pct": 6.5,
                }
            },
        )

        self.assertTrue(allowed)
        self.assertIsNone(reason)
        self.assertAlmostEqual(float(extra_fields["volatility_24h_pct"]), 6.5, places=6)

    def test_evaluate_btc_regime_gate_blocks_alt_long_when_btc_is_dumping(self) -> None:
        allowed, reason, extra_fields = evaluate_btc_regime_gate(
            symbol="DOGEUSDT",
            side="BUY",
            btc_ret_5m_pct=-1.2,
            drop_threshold_pct=1.0,
        )

        self.assertFalse(allowed)
        self.assertEqual(reason, "btc_regime_alt_long_blocked")
        self.assertAlmostEqual(float(extra_fields["btc_regime_ret_5m_pct"]), -1.2, places=6)

    def test_evaluate_btc_regime_gate_allows_short_and_major_symbols(self) -> None:
        short_allowed, short_reason, _ = evaluate_btc_regime_gate(
            symbol="DOGEUSDT",
            side="SELL",
            btc_ret_5m_pct=-1.4,
            drop_threshold_pct=1.0,
        )
        major_allowed, major_reason, _ = evaluate_btc_regime_gate(
            symbol="BTCUSDT",
            side="BUY",
            btc_ret_5m_pct=-1.4,
            drop_threshold_pct=1.0,
        )

        self.assertTrue(short_allowed)
        self.assertIsNone(short_reason)
        self.assertTrue(major_allowed)
        self.assertIsNone(major_reason)

    def test_evaluate_btc_regime_gate_fails_closed_when_missing_for_alt_long(self) -> None:
        allowed, reason, _ = evaluate_btc_regime_gate(
            symbol="DOGEUSDT",
            side="BUY",
            btc_ret_5m_pct=None,
            drop_threshold_pct=1.0,
        )

        self.assertFalse(allowed)
        self.assertEqual(reason, "missing_btc_regime")

    def test_compute_btc_regime_return_from_klines(self) -> None:
        result = compute_btc_regime_return_from_klines(
            [
                [0, "100.0", "101.0", "99.5", "100.5"],
                [60_000, "100.5", "101.6", "100.2", "101.5"],
            ]
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertAlmostEqual(result, 1.5, places=6)

    def test_global_risk_lock_round_trip_defaults_to_active(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "global_risk_lock.json"
            payload = write_global_risk_lock(path, payload={"type": "hard_kill_equity_drawdown"})

            loaded = read_global_risk_lock(path)

        self.assertTrue(payload["active"])
        self.assertIsNotNone(loaded)
        self.assertTrue(is_global_risk_lock_active(loaded))
        assert loaded is not None
        self.assertEqual(loaded["type"], "hard_kill_equity_drawdown")

    def test_build_hard_kill_close_payload_closes_one_way_short_reduce_only(self) -> None:
        payload = build_hard_kill_close_payload(
            {"symbol": "DOGEUSDT", "positionAmt": "-123.4", "positionSide": "BOTH"},
            account_api_mode="classic",
        )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["side"], "BUY")
        self.assertEqual(payload["quantity"], "123.4")
        self.assertEqual(payload["reduceOnly"], "true")
        self.assertNotIn("positionSide", payload)

    def test_build_hard_kill_close_payload_keeps_hedge_position_side(self) -> None:
        payload = build_hard_kill_close_payload(
            {"symbol": "ETHUSDT", "positionAmt": "0.25", "positionSide": "LONG"},
            account_api_mode="portfolio_margin",
        )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["side"], "SELL")
        self.assertEqual(payload["positionSide"], "LONG")
        self.assertNotIn("reduceOnly", payload)

    def test_summarize_shadow_performance_aggregates_counts_pending_and_horizons(self) -> None:
        signals = [
            {
                "event_id": "evt-1",
                "playbook": "oi_build_breakout",
                "shadow_reason": "margin_insufficient",
                "branch_type": BRANCH_TYPE_STABLE_CORE,
                "shadow_branches": [{"branch_id": "A", "branch_label": "A", "stop_loss_pct": 1.3, "take_profit_pct": 3.5}],
            },
            {
                "event_id": "evt-2",
                "playbook": "liquidation_flush",
                "shadow_reason": "invalid_symbol",
                "branch_type": BRANCH_TYPE_STABLE_CORE,
                "shadow_branches": [{"branch_id": "B", "branch_label": "B", "stop_loss_pct": 1.5, "take_profit_pct": 5.0}],
            },
            {
                "event_id": "evt-3",
                "playbook": "oi_build_breakout",
                "shadow_reason": "margin_insufficient",
                "branch_type": BRANCH_TYPE_DEGEN_HIGH_YIELD,
                "shadow_branches": [{"branch_id": "C", "branch_label": "C", "stop_loss_pct": 0.8, "take_profit_pct": 2.0}],
            },
        ]
        outcomes = [
            {
                "event_id": "evt-1",
                "shadow_instance_id": "evt-1::A",
                "shadow_branch_id": "A",
                "horizon_sec": 900,
                "after_fee_return_pct": 1.0,
                "effective_quote_allocation_usdt": 200.0,
            },
            {
                "event_id": "evt-1",
                "shadow_instance_id": "evt-1::A",
                "shadow_branch_id": "A",
                "horizon_sec": 1800,
                "after_fee_return_pct": 2.0,
                "effective_quote_allocation_usdt": 200.0,
            },
            {
                "event_id": "evt-1",
                "shadow_instance_id": "evt-1::A",
                "shadow_branch_id": "A",
                "horizon_sec": 3600,
                "after_fee_return_pct": -1.0,
                "effective_quote_allocation_usdt": 200.0,
            },
            {
                "event_id": "evt-2",
                "shadow_instance_id": "evt-2::B",
                "shadow_branch_id": "B",
                "horizon_sec": 900,
                "after_fee_return_pct": -0.5,
                "effective_quote_allocation_usdt": 100.0,
            },
        ]

        summary = summarize_shadow_performance(signals, outcomes)

        self.assertEqual(summary["signal_count"], 3)
        self.assertEqual(summary["branch_signal_count"], 3)
        self.assertEqual(summary["outcome_count"], 4)
        self.assertEqual(summary["pending_signal_count"], 2)
        self.assertEqual(summary["pending_branch_count"], 2)
        self.assertEqual(
            summary["playbook_counts"],
            {"liquidation_flush": 1, "oi_build_breakout": 2},
        )
        self.assertEqual(
            summary["reason_counts"],
            {"invalid_symbol": 1, "margin_insufficient": 2},
        )
        self.assertEqual(
            summary["branch_type_counts"],
            {BRANCH_TYPE_DEGEN_HIGH_YIELD: 1, BRANCH_TYPE_STABLE_CORE: 2},
        )
        self.assertEqual(summary["horizon_summary"][900]["count"], 2)
        self.assertAlmostEqual(summary["horizon_summary"][900]["avg_after_fee_return_pct"], 0.25, places=6)
        self.assertAlmostEqual(summary["horizon_summary"][900]["estimated_total_pnl_usdt"], 1.5, places=6)
        self.assertAlmostEqual(summary["horizon_summary"][1800]["estimated_total_pnl_usdt"], 4.0, places=6)
        self.assertAlmostEqual(summary["horizon_summary"][3600]["estimated_total_pnl_usdt"], -2.0, places=6)
        self.assertIn("A", summary["branch_summary"])
        self.assertEqual(summary["branch_summary"]["A"]["signal_count"], 1)

    def test_simulate_shadow_branch_outcome_hits_take_profit_for_short(self) -> None:
        result = simulate_shadow_branch_outcome(
            event_id="evt-short",
            branch={"branch_id": "B", "branch_label": "B", "stop_loss_pct": 1.5, "take_profit_pct": 2.0},
            side="SELL",
            entry_price=100.0,
            entry_time_ms=0,
            horizon_sec=900,
            candles_1m=[
                [0, 100.0, 100.4, 97.7, 98.1, 0.0, 60_000],
            ],
            fallback_close_price=98.1,
            fallback_max_drawdown_pct=-2.3,
            fallback_max_runup_pct=0.4,
            round_trip_fee_bps=0.0,
            extra_slippage_penalty_pct=1.0,
        )

        self.assertEqual(result["shadow_branch_id"], "B")
        self.assertEqual(result["exit_reason"], "take_profit_hit")
        self.assertAlmostEqual(float(result["close_return_pct"]), 2.0, places=6)
        self.assertAlmostEqual(float(result["after_fee_and_slippage_return_pct"]), 1.0, places=6)
        self.assertGreater(float(result["max_runup_pct"]), 2.0)

    def test_simulate_shadow_branch_outcome_prefers_conservative_stop_on_dual_hit(self) -> None:
        result = simulate_shadow_branch_outcome(
            event_id="evt-long",
            branch={"branch_id": "A", "branch_label": "A", "stop_loss_pct": 1.0, "take_profit_pct": 1.5},
            side="BUY",
            entry_price=100.0,
            entry_time_ms=0,
            horizon_sec=900,
            candles_1m=[
                [0, 100.0, 101.8, 98.8, 100.5, 0.0, 60_000],
            ],
            fallback_close_price=100.5,
            fallback_max_drawdown_pct=-1.2,
            fallback_max_runup_pct=1.8,
            round_trip_fee_bps=0.0,
            extra_slippage_penalty_pct=0.0,
        )

        self.assertEqual(result["exit_reason"], "ambiguous_dual_hit_stop_assumed")
        self.assertAlmostEqual(float(result["close_return_pct"]), -1.0, places=6)
        self.assertEqual(result["round_trip_fee_bps"], 0.0)

    def test_build_shadow_readiness_report_excludes_research_pool_from_live_unlock(self) -> None:
        outcomes = [
            {
                "event_id": f"research-{index}",
                "shadow_instance_id": f"research-{index}::TREND_SCALP_035_070",
                "shadow_branch_id": "TREND_SCALP_035_070",
                "branch_type": BRANCH_TYPE_RESEARCH_POOL,
                "research_only": True,
                "live_unlock_eligible": False,
                "horizon_sec": 3600,
                "after_fee_and_slippage_return_pct": 2.0,
                "effective_quote_allocation_usdt": 10.0,
            }
            for index in range(200)
        ]

        report = build_shadow_readiness_report(
            outcomes,
            generated_at="2026-04-28T00:00:00+00:00",
            baseline_equity_usdt=5000.0,
            min_closed_trades=200,
            min_win_rate_pct=55.0,
            min_profit_factor=1.5,
            min_sharpe_ratio=1.5,
            max_drawdown_pct=5.0,
        )

        self.assertFalse(report["live_unlock_candidate"])
        self.assertEqual(report["closed_shadow_trades"], 0)
        self.assertEqual(report["research_only_outcomes_excluded"], 200)

    def test_build_shadow_readiness_report_marks_candidate_without_enabling_live(self) -> None:
        outcomes = []
        for index in range(200):
            return_pct = 2.0 if index < 120 else -1.0
            outcomes.append(
                {
                    "event_id": f"evt-{index}",
                    "shadow_instance_id": f"evt-{index}::AUTO",
                    "shadow_branch_id": "AUTO",
                    "horizon_sec": 3600,
                    "after_fee_and_slippage_return_pct": return_pct,
                    "effective_quote_allocation_usdt": 10.0,
                }
            )

        report = build_shadow_readiness_report(
            outcomes,
            generated_at="2026-04-28T00:00:00+00:00",
            baseline_equity_usdt=5000.0,
            min_closed_trades=200,
            min_win_rate_pct=55.0,
            min_profit_factor=1.5,
            min_sharpe_ratio=1.5,
            max_drawdown_pct=5.0,
        )

        self.assertTrue(report["live_unlock_candidate"])
        self.assertFalse(report["live_trading_enabled"])
        self.assertEqual(report["closed_shadow_trades"], 200)
        self.assertGreater(float(report["win_rate_pct"]), 55.0)
        self.assertGreater(float(report["profit_factor"]), 1.5)
        self.assertLess(float(report["max_drawdown_pct"]), 5.0)

    def test_build_shadow_readiness_report_rejects_small_sample_even_when_profitable(self) -> None:
        outcomes = [
            {
                "event_id": f"evt-{index}",
                "shadow_instance_id": f"evt-{index}::AUTO",
                "shadow_branch_id": "AUTO",
                "horizon_sec": 3600,
                "after_fee_and_slippage_return_pct": 2.0,
                "effective_quote_allocation_usdt": 10.0,
            }
            for index in range(199)
        ]

        report = build_shadow_readiness_report(
            outcomes,
            generated_at="2026-04-28T00:00:00+00:00",
            baseline_equity_usdt=5000.0,
            min_closed_trades=200,
            min_win_rate_pct=55.0,
            min_profit_factor=1.5,
            min_sharpe_ratio=1.5,
            max_drawdown_pct=5.0,
        )

        self.assertFalse(report["live_unlock_candidate"])
        self.assertFalse(report["criteria"]["sample_size_ok"])

    def test_evaluate_real_stop_loss_streak_detects_three_consecutive_losses(self) -> None:
        records = [
            {
                "event": "real_trade_outcome",
                "job_id": "job-1",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "playbook": "oi_build_breakout",
                "closed_at": "2026-04-27T10:00:00+00:00",
                "settlement_status": "complete",
                "is_stop_loss_loss": True,
                "close_reason": "stop_loss_hit",
                "net_pnl_usdt": -1.2,
            },
            {
                "event": "real_trade_outcome",
                "job_id": "job-2",
                "symbol": "ETHUSDT",
                "side": "BUY",
                "playbook": "liquidation_flush",
                "closed_at": "2026-04-27T10:05:00+00:00",
                "settlement_status": "complete",
                "is_stop_loss_loss": True,
                "close_reason": "stop_loss_hit",
                "net_pnl_usdt": -0.9,
            },
            {
                "event": "real_trade_outcome",
                "job_id": "job-3",
                "symbol": "SOLUSDT",
                "side": "SELL",
                "playbook": "oi_build_breakout",
                "closed_at": "2026-04-27T10:10:00+00:00",
                "settlement_status": "complete",
                "is_stop_loss_loss": True,
                "close_reason": "stop_loss_hit",
                "net_pnl_usdt": -1.4,
            },
        ]

        payload = evaluate_real_stop_loss_streak(records, streak_length=3)

        assert payload is not None
        self.assertEqual(payload["streak_length"], 3)
        self.assertEqual(payload["latest_outcome_key"], build_real_trade_outcome_key(records[-1]))
        self.assertAlmostEqual(payload["total_net_pnl_usdt"], -3.5, places=6)

    def test_evaluate_real_stop_loss_streak_requires_consecutive_complete_losses(self) -> None:
        records = [
            {
                "event": "real_trade_outcome",
                "job_id": "job-1",
                "closed_at": "2026-04-27T10:00:00+00:00",
                "settlement_status": "complete",
                "is_stop_loss_loss": True,
                "close_reason": "stop_loss_hit",
                "net_pnl_usdt": -1.2,
            },
            {
                "event": "real_trade_outcome",
                "job_id": "job-2",
                "closed_at": "2026-04-27T10:05:00+00:00",
                "settlement_status": "complete",
                "is_stop_loss_loss": False,
                "close_reason": "take_profit_hit",
                "net_pnl_usdt": 2.1,
            },
            {
                "event": "real_trade_outcome",
                "job_id": "job-3",
                "closed_at": "2026-04-27T10:10:00+00:00",
                "settlement_status": "complete",
                "is_stop_loss_loss": True,
                "close_reason": "stop_loss_hit",
                "net_pnl_usdt": -1.4,
            },
            {
                "event": "real_trade_outcome",
                "job_id": "job-4",
                "closed_at": "2026-04-27T10:15:00+00:00",
                "settlement_status": "missing",
                "is_stop_loss_loss": True,
                "close_reason": "settlement_missing",
                "net_pnl_usdt": None,
            },
        ]

        payload = evaluate_real_stop_loss_streak(records, streak_length=3)

        self.assertIsNone(payload)

    def test_build_total_sample_report_compares_stable_and_degen_auto_branches(self) -> None:
        signals = [
            {
                "event_id": "stable-1",
                "playbook": "oi_build_breakout",
                "branch_type": BRANCH_TYPE_STABLE_CORE,
                "shadow_reason": "parallel_shadow_branch",
                "effective_quote_allocation_usdt": 10.0,
                "shadow_branches": [{"branch_id": "AUTO", "branch_label": "AUTO", "stop_loss_pct": 1.3, "take_profit_pct": 3.5}],
            },
            {
                "event_id": "degen-1",
                "playbook": "liquidation_flush",
                "branch_type": BRANCH_TYPE_DEGEN_HIGH_YIELD,
                "shadow_reason": "degen_high_yield_shadow_branch",
                "effective_quote_allocation_usdt": 10.0,
                "shadow_branches": [{"branch_id": "AUTO", "branch_label": "AUTO", "stop_loss_pct": 0.8, "take_profit_pct": 2.0}],
            },
        ]
        outcomes = [
            {
                "event_id": "stable-1",
                "shadow_instance_id": "stable-1::AUTO",
                "shadow_branch_id": "AUTO",
                "branch_type": BRANCH_TYPE_STABLE_CORE,
                "horizon_sec": 900,
                "after_fee_return_pct": 2.0,
                "after_fee_and_slippage_return_pct": 2.0,
                "effective_quote_allocation_usdt": 10.0,
                "extra_slippage_penalty_pct": 0.0,
            },
            {
                "event_id": "degen-1",
                "shadow_instance_id": "degen-1::AUTO",
                "shadow_branch_id": "AUTO",
                "branch_type": BRANCH_TYPE_DEGEN_HIGH_YIELD,
                "horizon_sec": 900,
                "after_fee_return_pct": 3.0,
                "after_fee_and_slippage_return_pct": 2.0,
                "effective_quote_allocation_usdt": 10.0,
                "extra_slippage_penalty_pct": 1.0,
            },
            {
                "event_id": "degen-2",
                "shadow_instance_id": "degen-2::AUTO",
                "shadow_branch_id": "AUTO",
                "branch_type": BRANCH_TYPE_DEGEN_HIGH_YIELD,
                "horizon_sec": 900,
                "after_fee_return_pct": -1.0,
                "after_fee_and_slippage_return_pct": -2.0,
                "effective_quote_allocation_usdt": 10.0,
                "extra_slippage_penalty_pct": 1.0,
            },
        ]

        report = build_total_sample_report(
            generated_at="2026-04-28T00:00:00+00:00",
            baseline_equity_usdt=5000.0,
            available_balance_usdt=2500.0,
            wallet_balance_usdt=2600.0,
            total_unrealized_pnl_usdt=15.0,
            open_positions=[],
            shadow_signals=signals,
            shadow_outcomes=outcomes,
            filtered_quote_allocation_cap_usdt=10.0,
        )

        stable_summary = report["shadow_branch_type_auto_summary"][BRANCH_TYPE_STABLE_CORE]
        degen_summary = report["shadow_branch_type_auto_summary"][BRANCH_TYPE_DEGEN_HIGH_YIELD]
        comparison = report["shadow_branch_type_auto_comparison"]

        self.assertAlmostEqual(float(stable_summary["estimated_total_pnl_usdt"]), 0.2, places=6)
        self.assertAlmostEqual(float(degen_summary["estimated_total_pnl_usdt"]), 0.2, places=6)
        self.assertAlmostEqual(float(degen_summary["slippage_adjusted_total_pnl_usdt"]), 0.0, places=6)
        self.assertEqual(degen_summary["extra_slippage_penalty_pct"], 1.0)
        self.assertAlmostEqual(float(comparison["pnl_delta_degen_minus_stable_usdt"]), 0.0, places=6)
        self.assertAlmostEqual(float(comparison["slippage_adjusted_pnl_delta_degen_minus_stable_usdt"]), -0.2, places=6)


class SignalBridgeMainnetShadowAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_report_still_writes_shadow_research_when_account_api_is_unavailable(self) -> None:
        futures = FailingAccountOverviewFutures()
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            shadow_log_file = base / "signal_bridge_shadow_signals.jsonl"
            shadow_outcomes_file = base / "signal_bridge_shadow_outcomes.jsonl"
            snapshots_file = base / "bridge_event_feed.jsonl"
            backtest_report_file = base / "backtest_report.json"
            shadow_log_file.write_text("", encoding="utf-8")
            shadow_outcomes_file.write_text("", encoding="utf-8")
            snapshots_file.write_text("", encoding="utf-8")
            backtest_report_file.write_text(
                json.dumps(
                    {
                        "report_type": "phoenix_promotion_backtest",
                        "trade_count": 1200,
                        "avg_return_pct": 0.03,
                        "profit_factor": 1.25,
                    }
                ),
                encoding="utf-8",
            )

            await write_live_performance_report(
                futures=futures,
                report_file=base / "Live_Performance_Report.txt",
                total_sample_file=base / "Total_Sample_V9_Final.json",
                readiness_report_file=base / "mainnet_shadow_readiness.json",
                candidate_strategy_report_file=base / "candidate_strategy_report.json",
                candidate_strategy_control_file=base / "candidate_strategy_control.json",
                factor_factory_report_file=base / "factor_factory_report.json",
                factor_factory_control_file=base / "factor_factory_control.json",
                bias_audit_report_file=base / "bias_audit_report.json",
                execution_realism_report_file=base / "execution_realism_report.json",
                promotion_gate_report_file=base / "promotion_gate_report.json",
                backtest_report_file=backtest_report_file,
                shadow_log_file=shadow_log_file,
                shadow_outcomes_file=shadow_outcomes_file,
                snapshots_file=snapshots_file,
                baseline_equity_usdt=5000.0,
                factor_factory_min_samples=1,
                factor_factory_pair_min_samples=1,
                governance_max_records=100,
            )

            total_sample = json.loads((base / "Total_Sample_V9_Final.json").read_text(encoding="utf-8"))
            self.assertEqual(total_sample["real_account"]["account_overview_error"], "account unavailable")
            self.assertTrue((base / "factor_factory_report.json").exists())
            self.assertTrue((base / "bias_audit_report.json").exists())
            self.assertTrue((base / "execution_realism_report.json").exists())
            self.assertTrue((base / "promotion_gate_report.json").exists())
            promotion_gate = json.loads((base / "promotion_gate_report.json").read_text(encoding="utf-8"))
            self.assertNotIn("missing_backtest_report", promotion_gate["blockers"])
            self.assertEqual(promotion_gate["backtest_metrics"]["trade_count"], 1200)

    async def test_live_report_writes_momentum_scalp_shadow_league_without_live_order(self) -> None:
        futures = FailingAccountOverviewFutures()
        with TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            shadow_log_file = base / "signal_bridge_shadow_signals.jsonl"
            shadow_outcomes_file = base / "signal_bridge_shadow_outcomes.jsonl"
            snapshots_file = base / "bridge_event_feed.jsonl"
            shadow_log_file.write_text(
                json.dumps(
                    {
                        "strategy_family": "ONE_MIN_MOMENTUM_SCALP_PLUS",
                        "strategy_id": "OMSP_FAST_FIXED_TP08_SL04_H3",
                        "entry_profile": "SCALP_FAST_ENTRY",
                        "exit_profile": "EXIT_FIXED_QUICK_TAKE",
                        "symbol": "BTCUSDT",
                        "live_trading_enabled": False,
                        "promotion_allowed": False,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            shadow_outcomes_file.write_text(
                json.dumps(
                    {
                        "strategy_family": "ONE_MIN_MOMENTUM_SCALP_PLUS",
                        "strategy_id": "OMSP_FAST_FIXED_TP08_SL04_H3",
                        "entry_profile": "SCALP_FAST_ENTRY",
                        "exit_profile": "EXIT_FIXED_QUICK_TAKE",
                        "symbol": "BTCUSDT",
                        "trading_session": "Asia",
                        "first_tp_hit": True,
                        "fake_spike_flag": False,
                        "after_real_cost_return_pct": 0.42,
                        "time_to_first_tp_sec": 60,
                        "holding_minutes": 1,
                        "max_drawdown_pct": -0.1,
                        "live_trading_enabled": False,
                        "promotion_allowed": False,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            snapshots_file.write_text("", encoding="utf-8")

            await write_live_performance_report(
                futures=futures,
                report_file=base / "Live_Performance_Report.txt",
                total_sample_file=base / "Total_Sample_V9_Final.json",
                strategy_shadow_league_report_file=base / "strategy_shadow_league_report.json",
                strategy_shadow_league_report_md_file=base / "strategy_shadow_league_report.md",
                shadow_log_file=shadow_log_file,
                shadow_outcomes_file=shadow_outcomes_file,
                snapshots_file=snapshots_file,
                baseline_equity_usdt=5000.0,
            )

            report = json.loads((base / "strategy_shadow_league_report.json").read_text(encoding="utf-8"))
            markdown = (base / "strategy_shadow_league_report.md").read_text(encoding="utf-8")
            self.assertFalse(report["live_trading_enabled"])
            self.assertFalse(report["promotion_allowed"])
            self.assertEqual(report["strategies"][0]["shadow_trade_count"], 1)
            self.assertIn("ONE_MIN_MOMENTUM_SCALP_PLUS", markdown)
            self.assertEqual(futures.new_orders, [])

    async def test_oi_unwind_reversal_candidate_uses_rest_1m_confirmation_and_stays_shadow_only(self) -> None:
        futures = FakeMainnetShadowFutures()
        anchor_close_time_ms = 1_777_000_000_000
        futures.kline_rows = [
            [
                anchor_close_time_ms + 1,
                "100",
                "101",
                "99.8",
                "100.6",
                "10",
                anchor_close_time_ms + 60_000,
            ]
        ]
        record = {
            "event": "market_event_created",
            "event_id": "evt-oi-unwind-candidate",
            "symbol": "ALTUSDT",
            "sample_type": "trigger",
            "bar_interval": "5m",
            "trigger_types": ["range_expansion", "volume_burst"],
            "sample": {
                "price": 100.0,
                "anchor_close_time_ms": anchor_close_time_ms,
                "candle_direction": "down",
            },
            "enrichments": {
                "oi_change_5m_pct": -1.1,
                "oi_change_15m_pct": -1.4,
            },
        }

        confirmation = await resolve_oi_unwind_reversal_confirmation(
            futures=futures,
            record=record,
        )

        self.assertIsNotNone(confirmation)
        assert confirmation is not None
        self.assertEqual(confirmation["side"], "BUY")
        self.assertEqual(confirmation["confirmation_source"], "rest_1m_kline")
        self.assertAlmostEqual(float(confirmation["entry_price"]), 100.6, places=6)
        self.assertEqual(build_oi_unwind_reversal_branch_specs("BUY")[1]["branch_id"], "OI_UNWIND_REV_050_100")

        settings = replace(
            ExecutionSettings(),
            quote_allocation_usdt=10.0,
            leverage=10,
            initial_stop_loss_pct=1.3,
            take_profit_pct=3.5,
        )
        executor = PhoenixExecutor(futures, settings)
        payload = await build_shadow_signal_payload(
            futures=futures,
            executor=executor,
            settings=settings,
            record=record,
            playbook=PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
            side="BUY",
            failure_reason=SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE,
            low_balance_threshold_usdt=2000.0,
            low_balance_quote_allocation_usdt=10.0,
            shadow_branch_specs=build_oi_unwind_reversal_branch_specs("BUY"),
            target_horizons_sec=[180, 300, 900],
            extra_fields={
                "branch_type": BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
                "research_only": True,
                "shadow_only": True,
                "live_unlock_eligible": False,
                "exchange_preflight_requested": False,
            },
        )

        self.assertEqual(payload["playbook"], PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED)
        self.assertEqual(payload["branch_type"], BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE)
        self.assertEqual(payload["shadow_target_horizons_sec"], [180, 300, 900])
        self.assertTrue(payload["research_only"])
        self.assertFalse(payload["live_unlock_eligible"])
        self.assertEqual(futures.new_orders, [])
        self.assertEqual(futures.test_orders, [])

    async def test_shadow_signal_payload_records_entry_cost_realism_fields_without_live_order(self) -> None:
        futures = FakeMainnetShadowFutures()
        settings = replace(
            ExecutionSettings(),
            quote_allocation_usdt=10.0,
            leverage=10,
            initial_stop_loss_pct=1.3,
            take_profit_pct=3.5,
        )
        executor = PhoenixExecutor(futures, settings)
        anchor_close_time_ms = 1_777_000_000_000
        payload = await build_shadow_signal_payload(
            futures=futures,
            executor=executor,
            settings=settings,
            record={
                "event_id": "evt-shadow-cost-fields",
                "symbol": "BTCUSDT",
                "sample": {
                    "price": 100.0,
                    "anchor_close_time_ms": anchor_close_time_ms,
                    "quote_volume_24h": 250_000_000.0,
                },
                "observed_at_ms": anchor_close_time_ms + 175,
                "trigger_types": ["range_expansion"],
                "enrichments": {
                    "funding_rate": 0.0003,
                    "spread_bps": 1.7,
                    "estimated_slippage_bps": 2.8,
                },
            },
            playbook=PLAYBOOK_SHADOW_OI_BUILD_BALANCED,
            side="BUY",
            failure_reason=SHADOW_REASON_STRATEGY_DISCOVERY_CANDIDATE,
            low_balance_threshold_usdt=2000.0,
            low_balance_quote_allocation_usdt=10.0,
            shadow_branch_specs=build_strategy_discovery_branch_specs(PLAYBOOK_SHADOW_OI_BUILD_BALANCED, "BUY"),
            extra_fields={
                "research_only": True,
                "shadow_only": True,
                "live_unlock_eligible": False,
                "exchange_preflight_requested": False,
            },
        )

        self.assertAlmostEqual(float(payload["funding_rate_at_entry"]), 0.0003)
        self.assertAlmostEqual(float(payload["spread_bps_at_entry"]), 1.7)
        self.assertAlmostEqual(float(payload["estimated_slippage_bps"]), 2.8)
        self.assertEqual(payload["order_latency_ms"], 175)
        self.assertEqual(payload["maker_or_taker"], "paper_taker")
        self.assertEqual(payload["liquidity_bucket"], "major")
        self.assertFalse(payload["live_trading_enabled"])
        self.assertFalse(payload["promotion_allowed"])
        self.assertEqual(futures.new_orders, [])
        self.assertEqual(futures.test_orders, [])

    async def test_oi_unwind_horizon_updates_use_confirmation_entry_price_and_time(self) -> None:
        futures = FakeMainnetShadowFutures()
        anchor_close_time_ms = 1_777_000_000_000
        entry_time_ms = anchor_close_time_ms + 60_000
        futures.kline_rows = [
            [
                entry_time_ms + 1,
                "100",
                "101.2",
                "99.9",
                "100.8",
                "10",
                entry_time_ms + 150_000,
            ]
        ]
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pending: dict[str, dict[str, object]] = {
                "evt-confirmed::candidate_oi_unwind_reversal": {
                    "event_id": "evt-confirmed::candidate_oi_unwind_reversal",
                    "source_event_id": "evt-confirmed",
                    "horizon_event_id": "evt-confirmed",
                    "symbol": "ALTUSDT",
                    "playbook": PLAYBOOK_OI_UNWIND_REVERSAL_CONFIRMED,
                    "side": "BUY",
                    "branch_type": BRANCH_TYPE_OI_UNWIND_REVERSAL_CANDIDATE,
                    "research_only": True,
                    "shadow_reason": SHADOW_REASON_OI_UNWIND_REVERSAL_CANDIDATE,
                    "sample_price": 90.0,
                    "entry_price": 100.0,
                    "entry_time_ms": entry_time_ms,
                    "anchor_close_time_ms": anchor_close_time_ms,
                    "funding_rate_at_entry": 0.0008,
                    "spread_bps_at_entry": 1.9,
                    "order_latency_ms": 250,
                    "estimated_slippage_bps": 3.2,
                    "maker_or_taker": "paper_taker",
                    "liquidity_bucket": "major",
                    "virtual_intent": {"notional_usdt": 100.0},
                    "shadow_target_horizons_sec": [180],
                    "effective_quote_allocation_usdt": 10.0,
                    "shadow_branches": [
                        {
                            "branch_id": "OI_UNWIND_REV_TEST",
                            "branch_label": "oi_unwind_reversal_test",
                            "side": "BUY",
                            "direction_variant": "reversal",
                            "stop_loss_pct": 0.5,
                            "take_profit_pct": 1.0,
                            "research_only": True,
                            "completed_horizons_sec": [],
                        }
                    ],
                }
            }
            horizon_file = temp_path / "horizon.jsonl"
            horizon_file.write_text(
                json.dumps(
                    {
                        "event": "market_event_horizon_labeled",
                        "event_id": "evt-confirmed",
                        "observed_at_ms": anchor_close_time_ms,
                        "horizon": {
                            "horizon_sec": 180,
                            "deadline_ms": anchor_close_time_ms + 180_000,
                            "close_price": 90.0,
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch("phoenix_signal_bridge.emit_event"):
                await process_shadow_horizon_updates(
                    futures=futures,
                    horizon_labels_file=horizon_file,
                    shadow_outcomes_file=temp_path / "shadow-outcomes.jsonl",
                    shadow_state_file=temp_path / "shadow-state.json",
                    horizon_labels_offset=0,
                    pending_by_event_id=pending,
                    round_trip_fee_bps=0.0,
                )
            outcomes = [
                json.loads(line)
                for line in (temp_path / "shadow-outcomes.jsonl").read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0]["exit_reason"], "take_profit_hit")
        self.assertAlmostEqual(float(outcomes[0]["after_fee_return_pct"]), 1.0, places=6)
        self.assertAlmostEqual(float(outcomes[0]["funding_rate_at_entry"]), 0.0008, places=8)
        self.assertAlmostEqual(float(outcomes[0]["funding_paid_during_hold"]), 0.0005, places=8)
        self.assertAlmostEqual(float(outcomes[0]["spread_bps_at_entry"]), 1.9, places=6)
        self.assertEqual(outcomes[0]["order_latency_ms"], 250)
        self.assertAlmostEqual(float(outcomes[0]["estimated_slippage_bps"]), 3.2, places=6)
        self.assertEqual(outcomes[0]["maker_or_taker"], "paper_taker")
        self.assertEqual(outcomes[0]["liquidity_bucket"], "major")

    async def test_research_shadow_horizon_updates_simulate_trend_and_reversal_branches(self) -> None:
        futures = FakeMainnetShadowFutures()
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pending: dict[str, dict[str, object]] = {}
            persist_research_shadow_signal(
                record={
                    "event": "market_event_created",
                    "event_id": "evt-research-horizon",
                    "symbol": "ALTUSDT",
                    "sample": {
                        "candle_direction": "up",
                        "price": 100.0,
                        "anchor_close_time_ms": 1_777_000_000_000,
                    },
                    "observed_at_ms": 1_777_000_000_000,
                },
                shadow_log_file=temp_path / "shadow.jsonl",
                shadow_state_file=temp_path / "shadow-state.json",
                horizon_labels_file=temp_path / "horizon.jsonl",
                horizon_labels_offset=0,
                pending_shadow_by_event_id=pending,
            )
            horizon_file = temp_path / "horizon.jsonl"
            horizon_file.write_text(
                json.dumps(
                    {
                        "event": "market_event_horizon_labeled",
                        "event_id": "evt-research-horizon",
                        "observed_at_ms": 1_777_000_000_000,
                        "horizon": {
                            "horizon_sec": 900,
                            "deadline_ms": 1_777_000_900_000,
                            "close_price": 101.0,
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch("phoenix_signal_bridge.emit_event"):
                await process_shadow_horizon_updates(
                    futures=futures,
                    horizon_labels_file=horizon_file,
                    shadow_outcomes_file=temp_path / "shadow-outcomes.jsonl",
                    shadow_state_file=temp_path / "shadow-state.json",
                    horizon_labels_offset=0,
                    pending_by_event_id=pending,
                    round_trip_fee_bps=0.0,
                )
            outcomes = [
                json.loads(line)
                for line in (temp_path / "shadow-outcomes.jsonl").read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(len(outcomes), 6)
        self.assertEqual({item["side"] for item in outcomes}, {"BUY", "SELL"})
        self.assertEqual({item["direction_variant"] for item in outcomes}, {"trend", "reversal"})
        self.assertTrue(all(item["research_only"] for item in outcomes))
        self.assertTrue(all(item["branch_type"] == BRANCH_TYPE_RESEARCH_POOL for item in outcomes))
        self.assertTrue(all(item["live_unlock_eligible"] is False for item in outcomes))
        self.assertEqual(futures.test_orders, [])
        self.assertEqual(futures.new_orders, [])

    async def test_research_shadow_horizon_updates_use_per_signal_target_horizons(self) -> None:
        futures = FakeMainnetShadowFutures()
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pending: dict[str, dict[str, object]] = {}
            payload = persist_research_shadow_signal(
                record={
                    "event": "market_event_created",
                    "event_id": "evt-research-short-horizons",
                    "symbol": "ALTUSDT",
                    "sample": {
                        "candle_direction": "down",
                        "price": 100.0,
                        "anchor_close_time_ms": 1_777_000_000_000,
                    },
                    "observed_at_ms": 1_777_000_000_000,
                },
                shadow_log_file=temp_path / "shadow.jsonl",
                shadow_state_file=temp_path / "shadow-state.json",
                horizon_labels_file=temp_path / "horizon.jsonl",
                horizon_labels_offset=0,
                pending_shadow_by_event_id=pending,
                target_horizons_sec=[60, 180],
            )
            horizon_rows = [
                {
                    "event": "market_event_horizon_labeled",
                    "event_id": "evt-research-short-horizons",
                    "observed_at_ms": 1_777_000_000_000,
                    "horizon": {
                        "horizon_sec": horizon_sec,
                        "deadline_ms": 1_777_000_000_000 + horizon_sec * 1000,
                        "close_price": close_price,
                    },
                }
                for horizon_sec, close_price in ((60, 99.5), (180, 98.5), (900, 97.5))
            ]
            horizon_file = temp_path / "horizon.jsonl"
            horizon_file.write_text(
                "".join(json.dumps(row) + "\n" for row in horizon_rows),
                encoding="utf-8",
            )

            with patch("phoenix_signal_bridge.emit_event"):
                await process_shadow_horizon_updates(
                    futures=futures,
                    horizon_labels_file=horizon_file,
                    shadow_outcomes_file=temp_path / "shadow-outcomes.jsonl",
                    shadow_state_file=temp_path / "shadow-state.json",
                    horizon_labels_offset=0,
                    pending_by_event_id=pending,
                    round_trip_fee_bps=0.0,
                )
            outcomes = [
                json.loads(line)
                for line in (temp_path / "shadow-outcomes.jsonl").read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual({item["horizon_sec"] for item in outcomes}, {60, 180})
        self.assertEqual(len(outcomes), 12)
        self.assertTrue(all(item["event_id"] == payload["event_id"] for item in outcomes))
        self.assertNotIn(payload["event_id"], pending)
        self.assertEqual(futures.test_orders, [])
        self.assertEqual(futures.new_orders, [])

    async def test_shadow_horizon_updates_skip_branches_without_trade_side(self) -> None:
        futures = FakeMainnetShadowFutures()
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pending: dict[str, dict[str, object]] = {
                "evt-no-side::research_pool": {
                    "event_id": "evt-no-side::research_pool",
                    "source_event_id": "evt-no-side",
                    "horizon_event_id": "evt-no-side",
                    "symbol": "ALTUSDT",
                    "branch_type": BRANCH_TYPE_RESEARCH_POOL,
                    "research_only": True,
                    "side": None,
                    "sample_price": 100.0,
                    "anchor_close_time_ms": 1_777_000_000_000,
                    "shadow_target_horizons_sec": [60],
                    "shadow_branches": [
                        {
                            "branch_id": "LEGACY",
                            "stop_loss_pct": 0.01,
                            "take_profit_pct": 0.0,
                            "completed_horizons_sec": [],
                        }
                    ],
                }
            }
            horizon_file = temp_path / "horizon.jsonl"
            horizon_file.write_text(
                json.dumps(
                    {
                        "event": "market_event_horizon_labeled",
                        "event_id": "evt-no-side",
                        "observed_at_ms": 1_777_000_000_000,
                        "horizon": {
                            "horizon_sec": 60,
                            "deadline_ms": 1_777_000_060_000,
                            "close_price": 101.0,
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch("phoenix_signal_bridge.emit_event"):
                await process_shadow_horizon_updates(
                    futures=futures,
                    horizon_labels_file=horizon_file,
                    shadow_outcomes_file=temp_path / "shadow-outcomes.jsonl",
                    shadow_state_file=temp_path / "shadow-state.json",
                    horizon_labels_offset=0,
                    pending_by_event_id=pending,
                    round_trip_fee_bps=8.0,
                )

        self.assertFalse((temp_path / "shadow-outcomes.jsonl").exists())
        self.assertNotIn("evt-no-side::research_pool", pending)

    async def test_mainnet_shadow_preflight_uses_order_test_and_never_new_order(self) -> None:
        futures = FakeMainnetShadowFutures()
        settings = replace(
            ExecutionSettings(),
            quote_allocation_usdt=10.0,
            leverage=10,
            initial_stop_loss_pct=1.3,
            take_profit_pct=3.5,
        )
        executor = PhoenixExecutor(futures, settings)

        payload = await build_shadow_signal_payload(
            futures=futures,
            executor=executor,
            settings=settings,
            record={
                "event_id": "evt-mainnet-shadow",
                "symbol": "BTCUSDT",
                "sample": {"price": 100.0, "anchor_close_time_ms": 1_777_000_000_000},
                "observed_at_ms": 1_777_000_000_000,
                "trigger_types": ["volume_burst"],
            },
            playbook="oi_build_breakout",
            side="BUY",
            failure_reason=SHADOW_REASON_MAINNET_SHADOW_LOCKED,
            low_balance_threshold_usdt=2000.0,
            low_balance_quote_allocation_usdt=10.0,
            shadow_branch_specs=[
                {"branch_id": "AUTO", "branch_label": "AUTO", "stop_loss_pct": 1.3, "take_profit_pct": 3.5}
            ],
            extra_fields={
                "execution_mode": EXECUTION_MODE_MAINNET_SHADOW,
                "exchange_preflight_requested": True,
            },
        )

        self.assertTrue(payload["exchange_preflight_requested"])
        self.assertTrue(payload["exchange_preflight_ok"])
        self.assertEqual(futures.new_orders, [])
        self.assertIn("/fapi/v1/order", {str(item["endpoint"]) for item in futures.test_orders})


if __name__ == "__main__":
    unittest.main()
