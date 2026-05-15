from __future__ import annotations

import unittest

from phoenix_momentum_scalp_plus import (
    ENTRY_PROFILE_CONFIRM,
    ENTRY_PROFILE_FAST,
    EXIT_FIXED_QUICK_TAKE,
    EXIT_HALF_TAKE_THEN_TRAIL,
    EXIT_MOMENTUM_DECAY,
    STRATEGY_FAMILY,
    MomentumScalpConfig,
    build_strategy_shadow_league_report,
    build_strategy_shadow_league_markdown,
    build_momentum_scalp_shadow_signals,
    build_momentum_scalp_signal_from_confirmation,
    default_exit_branch_specs,
    detect_momentum_scalp_radar_event,
    resolve_momentum_scalp_confirmation,
    simulate_momentum_scalp_exit,
)


def base_record(**overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "event_id": "evt-1m-scalp",
        "symbol": "BTCUSDT",
        "bar_interval": "1m",
        "trading_session": "Asia",
        "observed_at_ms": 1_777_000_060_000,
        "sample": {
            "price": 100.0,
            "anchor_close_time_ms": 1_777_000_000_000,
            "candle_direction": "up",
            "trigger_candle_direction": "up",
            "range_to_atr": 1.8,
            "volume_burst_ratio": 3.5,
            "ret_1bar_pct": 0.72,
            "quote_volume_24h": 500_000_000.0,
        },
        "enrichments": {
            "oi_change_1m_pct": 0.45,
            "spread_bps": 2.0,
            "estimated_slippage_bps": 3.0,
            "funding_rate": 0.0001,
        },
    }
    for key, value in overrides.items():
        record[key] = value
    return record


def candle(
    close_time_ms: int,
    *,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume_burst_ratio: float = 2.5,
    oi_change_pct: float = 0.25,
) -> dict[str, object]:
    return {
        "open_time_ms": close_time_ms - 60_000,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "close_time_ms": close_time_ms,
        "volume_burst_ratio": volume_burst_ratio,
        "oi_change_pct": oi_change_pct,
    }


class MomentumScalpPlusTests(unittest.TestCase):
    def test_one_min_radar_event_can_create_shadow_without_live_flags(self) -> None:
        config = MomentumScalpConfig(max_spread_bps=4.0, max_slippage_bps=5.0)
        signals = build_momentum_scalp_shadow_signals(
            base_record(),
            config=config,
            now_ms=1_777_000_061_000,
            last_signal_ms_by_symbol={},
        )

        self.assertGreater(len(signals), 0)
        self.assertTrue(all(item["strategy_family"] == STRATEGY_FAMILY for item in signals))
        self.assertTrue(all(item["shadow_only"] for item in signals))
        self.assertTrue(all(item["live_trading_enabled"] is False for item in signals))
        self.assertTrue(all(item["promotion_allowed"] is False for item in signals))
        self.assertTrue(all(item["live_order_submission_blocked"] for item in signals))

    def test_missing_one_min_oi_can_use_five_min_fallback_for_shadow(self) -> None:
        record = base_record(
            enrichments={
                "oi_change_5m_pct": 0.45,
                "oi_change_15m_pct": 1.2,
                "spread_bps": 2.0,
                "estimated_slippage_bps": 3.0,
                "funding_rate": 0.0001,
            }
        )
        radar = detect_momentum_scalp_radar_event(record, config=MomentumScalpConfig())
        self.assertIsNotNone(radar)
        assert radar is not None
        self.assertIsNone(radar["one_min_oi_change_pct"])
        self.assertEqual(radar["five_min_oi_change_pct"], 0.45)
        self.assertEqual(radar["fifteen_min_oi_change_pct"], 1.2)
        self.assertEqual(radar["oi_confirmation_source"], "five_min_fallback")
        self.assertEqual(radar["oi5_direction_bucket"], "oi5_aligned")

        signals = build_momentum_scalp_shadow_signals(
            record,
            config=MomentumScalpConfig(),
            now_ms=1_777_000_061_000,
            last_signal_ms_by_symbol={},
        )
        self.assertGreater(len(signals), 0)
        self.assertTrue(all(item["oi_confirmation_source"] == "five_min_fallback" for item in signals))
        self.assertTrue(all(item["oi5_direction_bucket"] == "oi5_aligned" for item in signals))
        self.assertTrue(all(item["live_trading_enabled"] is False for item in signals))
        self.assertTrue(all(item["promotion_allowed"] is False for item in signals))

    def test_one_min_oi_source_is_recorded_when_present(self) -> None:
        radar = detect_momentum_scalp_radar_event(base_record(), config=MomentumScalpConfig())
        self.assertIsNotNone(radar)
        assert radar is not None
        self.assertEqual(radar["one_min_oi_change_pct"], 0.45)
        self.assertEqual(radar["oi_confirmation_source"], "one_min")

    def test_missing_all_oi_can_pass_shadow_when_config_allows(self) -> None:
        record = base_record(
            enrichments={
                "spread_bps": 2.0,
                "estimated_slippage_bps": 3.0,
                "funding_rate": 0.0001,
            }
        )
        radar = detect_momentum_scalp_radar_event(record, config=MomentumScalpConfig())
        self.assertIsNotNone(radar)
        assert radar is not None
        self.assertIsNone(radar["one_min_oi_change_pct"])
        self.assertIsNone(radar["five_min_oi_change_pct"])
        self.assertEqual(radar["oi_confirmation_source"], "missing")
        self.assertEqual(radar["oi5_direction_bucket"], "oi5_flat_or_tiny")

    def test_require_one_min_oi_hard_gate_blocks_missing_oi(self) -> None:
        record = base_record(
            enrichments={
                "oi_change_5m_pct": 0.8,
                "spread_bps": 2.0,
                "estimated_slippage_bps": 3.0,
            }
        )
        radar = detect_momentum_scalp_radar_event(
            record,
            config=MomentumScalpConfig(require_one_min_oi_hard_gate=True),
        )
        self.assertIsNone(radar)

    def test_oi5_direction_bucket_classification(self) -> None:
        aligned_buy = detect_momentum_scalp_radar_event(
            base_record(enrichments={"oi_change_5m_pct": 0.3, "spread_bps": 2.0, "estimated_slippage_bps": 3.0}),
            config=MomentumScalpConfig(),
        )
        unwind_buy = detect_momentum_scalp_radar_event(
            base_record(enrichments={"oi_change_5m_pct": -0.3, "spread_bps": 2.0, "estimated_slippage_bps": 3.0}),
            config=MomentumScalpConfig(),
        )
        flat_buy = detect_momentum_scalp_radar_event(
            base_record(enrichments={"oi_change_5m_pct": 0.05, "spread_bps": 2.0, "estimated_slippage_bps": 3.0}),
            config=MomentumScalpConfig(),
        )
        aligned_sell = detect_momentum_scalp_radar_event(
            base_record(
                sample={
                    "price": 100.0,
                    "anchor_close_time_ms": 1_777_000_000_000,
                    "candle_direction": "down",
                    "trigger_candle_direction": "down",
                    "range_to_atr": 1.8,
                    "volume_burst_ratio": 3.5,
                    "ret_1bar_pct": -0.72,
                    "quote_volume_24h": 500_000_000.0,
                },
                enrichments={"oi_change_5m_pct": -0.3, "spread_bps": 2.0, "estimated_slippage_bps": 3.0},
            ),
            config=MomentumScalpConfig(),
        )
        self.assertEqual(aligned_buy["oi5_direction_bucket"], "oi5_aligned")
        self.assertEqual(unwind_buy["oi5_direction_bucket"], "oi5_unwind_opposite")
        self.assertEqual(flat_buy["oi5_direction_bucket"], "oi5_flat_or_tiny")
        self.assertEqual(aligned_sell["oi5_direction_bucket"], "oi5_aligned")

    def test_scalp_fast_entry_creates_shadow_trade_and_symbol_cooldown(self) -> None:
        cooldowns: dict[str, int] = {}
        config = MomentumScalpConfig(symbol_cooldown_sec=120)
        first = build_momentum_scalp_shadow_signals(
            base_record(),
            config=config,
            now_ms=1_777_000_061_000,
            last_signal_ms_by_symbol=cooldowns,
        )
        second = build_momentum_scalp_shadow_signals(
            base_record(event_id="evt-1m-scalp-2"),
            config=config,
            now_ms=1_777_000_090_000,
            last_signal_ms_by_symbol=cooldowns,
        )

        self.assertTrue(any(item["entry_profile"] == ENTRY_PROFILE_FAST for item in first))
        self.assertEqual(second, [])

    def test_scalp_confirm_entry_only_creates_after_confirmation(self) -> None:
        radar = detect_momentum_scalp_radar_event(base_record(), config=MomentumScalpConfig())
        assert radar is not None
        self.assertIsNone(build_momentum_scalp_signal_from_confirmation(base_record(), radar, None))

        confirmation = resolve_momentum_scalp_confirmation(
            radar,
            [
                candle(1_777_000_060_000, open_price=100.0, high_price=100.9, low_price=100.2, close_price=100.7),
                candle(1_777_000_120_000, open_price=100.7, high_price=101.1, low_price=100.5, close_price=100.95),
            ],
            config=MomentumScalpConfig(),
        )
        signal = build_momentum_scalp_signal_from_confirmation(base_record(), radar, confirmation)

        self.assertIsNotNone(confirmation)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal["entry_profile"], ENTRY_PROFILE_CONFIRM)
        self.assertEqual(signal["simulated_entry_price"], confirmation["confirmed_price"])
        self.assertEqual(signal["confirmation_delay_sec"], 120)

    def test_fake_spike_is_marked_and_not_confirmed(self) -> None:
        record = base_record(
            sample={
                "price": 100.0,
                "anchor_close_time_ms": 1_777_000_000_000,
                "candle_direction": "up",
                "trigger_candle_direction": "up",
                "range_to_atr": 2.2,
                "volume_burst_ratio": 4.0,
                "ret_1bar_pct": 0.8,
                "fake_spike_flag": True,
            }
        )
        radar = detect_momentum_scalp_radar_event(record, config=MomentumScalpConfig())
        assert radar is not None

        confirmation = resolve_momentum_scalp_confirmation(
            radar,
            [candle(1_777_000_060_000, open_price=100.0, high_price=100.2, low_price=99.4, close_price=99.7)],
            config=MomentumScalpConfig(),
        )

        self.assertTrue(radar["fake_spike_flag"])
        self.assertIsNone(confirmation)

    def test_exit_fixed_quick_take_handles_tp_stop_and_timeout(self) -> None:
        branch = {
            "exit_profile": EXIT_FIXED_QUICK_TAKE,
            "strategy_id": "fixed-test",
            "take_profit_price_pct": 0.8,
            "stop_loss_price_pct": 0.4,
            "max_hold_minutes": 3,
        }
        signal = build_momentum_scalp_shadow_signals(
            base_record(),
            config=MomentumScalpConfig(),
            now_ms=1_777_000_061_000,
            last_signal_ms_by_symbol={},
        )[0]

        tp = simulate_momentum_scalp_exit(
            signal,
            branch,
            [candle(1_777_000_060_000, open_price=100.0, high_price=101.0, low_price=99.9, close_price=100.9)],
            round_trip_fee_bps=8.0,
        )
        stop = simulate_momentum_scalp_exit(
            signal,
            branch,
            [candle(1_777_000_060_000, open_price=100.0, high_price=100.2, low_price=99.4, close_price=99.5)],
            round_trip_fee_bps=8.0,
        )
        timeout = simulate_momentum_scalp_exit(
            signal,
            branch,
            [candle(1_777_000_180_000, open_price=100.0, high_price=100.2, low_price=99.9, close_price=100.1)],
            round_trip_fee_bps=8.0,
        )

        self.assertEqual(tp["final_exit_reason"], "take_profit_hit")
        self.assertTrue(tp["first_tp_hit"])
        self.assertEqual(stop["final_exit_reason"], "stop_loss_hit")
        self.assertFalse(stop["first_tp_hit"])
        self.assertEqual(timeout["final_exit_reason"], "max_hold_timeout")

    def test_exit_half_take_then_trail_records_first_tp_breakeven_and_trailing_exit(self) -> None:
        branch = {
            "exit_profile": EXIT_HALF_TAKE_THEN_TRAIL,
            "strategy_id": "half-trail-test",
            "first_take_profit_price_pct": 0.8,
            "close_fraction_at_first_tp": 0.5,
            "move_stop_to_breakeven_after_first_tp": True,
            "trailing_callback_pct": 0.3,
            "max_hold_minutes": 8,
        }
        signal = build_momentum_scalp_shadow_signals(
            base_record(),
            config=MomentumScalpConfig(),
            now_ms=1_777_000_061_000,
            last_signal_ms_by_symbol={},
        )[0]

        outcome = simulate_momentum_scalp_exit(
            signal,
            branch,
            [
                candle(1_777_000_060_000, open_price=100.0, high_price=100.9, low_price=100.1, close_price=100.8),
                candle(1_777_000_120_000, open_price=100.8, high_price=101.5, low_price=101.0, close_price=101.4),
                candle(1_777_000_180_000, open_price=101.4, high_price=101.45, low_price=101.1, close_price=101.15),
            ],
            round_trip_fee_bps=8.0,
        )

        self.assertTrue(outcome["first_tp_hit"])
        self.assertEqual(outcome["close_fraction_at_first_tp"], 0.5)
        self.assertTrue(outcome["move_stop_to_breakeven_after_first_tp"])
        self.assertEqual(outcome["final_exit_reason"], "trailing_exit")
        self.assertGreater(outcome["final_return_pct"], 0.8)

    def test_exit_momentum_decay_exits_when_momentum_fades(self) -> None:
        branch = {
            "exit_profile": EXIT_MOMENTUM_DECAY,
            "strategy_id": "decay-test",
            "max_hold_minutes": 10,
        }
        signal = build_momentum_scalp_shadow_signals(
            base_record(),
            config=MomentumScalpConfig(),
            now_ms=1_777_000_061_000,
            last_signal_ms_by_symbol={},
        )[0]

        outcome = simulate_momentum_scalp_exit(
            signal,
            branch,
            [
                candle(1_777_000_060_000, open_price=100.0, high_price=100.9, low_price=100.2, close_price=100.7),
                candle(1_777_000_120_000, open_price=100.7, high_price=100.85, low_price=100.4, close_price=100.55, volume_burst_ratio=1.0),
                candle(1_777_000_180_000, open_price=100.55, high_price=100.8, low_price=100.3, close_price=100.45, volume_burst_ratio=0.9),
            ],
            round_trip_fee_bps=8.0,
        )

        self.assertEqual(outcome["final_exit_reason"], "momentum_decay")
        self.assertGreater(outcome["holding_minutes"], 0)

    def test_exit_specs_and_report_keep_everything_shadow_only(self) -> None:
        specs = default_exit_branch_specs(ENTRY_PROFILE_FAST)
        self.assertTrue(specs)
        self.assertTrue(all(item["live_trading_enabled"] is False for item in specs))
        self.assertTrue(all(item["promotion_allowed"] is False for item in specs))
        self.assertTrue(all(item["status"] == "shadow_only" for item in specs))

        signal = build_momentum_scalp_shadow_signals(
            base_record(),
            config=MomentumScalpConfig(),
            now_ms=1_777_000_061_000,
            last_signal_ms_by_symbol={},
        )[0]
        branch = next(item for item in specs if item["exit_profile"] == EXIT_FIXED_QUICK_TAKE)
        outcome = simulate_momentum_scalp_exit(
            signal,
            branch,
            [candle(1_777_000_060_000, open_price=100.0, high_price=101.0, low_price=99.9, close_price=100.9)],
            round_trip_fee_bps=8.0,
        )
        self.assertEqual(outcome["oi_confirmation_source"], signal["oi_confirmation_source"])
        self.assertEqual(outcome["oi5_direction_bucket"], signal["oi5_direction_bucket"])
        report = build_strategy_shadow_league_report(
            generated_at="2026-04-30T00:00:00+00:00",
            shadow_signals=[signal],
            shadow_outcomes=[outcome],
        )
        markdown = build_strategy_shadow_league_markdown(report)

        self.assertFalse(report["live_trading_enabled"])
        self.assertFalse(report["promotion_allowed"])
        self.assertIn("strategy_shadow_league", report["report_type"])
        self.assertIn("oi_confirmation_source_counts", report["strategies"][0])
        self.assertIn("oi5_direction_bucket_counts", report["strategies"][0])
        self.assertIn("ONE_MIN_MOMENTUM_SCALP_PLUS", markdown)


if __name__ == "__main__":
    unittest.main()
