from __future__ import annotations

import unittest
from datetime import datetime, timezone

from phoenix_signal_lab import analyze_event_records, combine_cycle_event_samples, event_trigger_signature
from phoenix_signal_lab import enrichment_route_for_sample, parse_allowed_event_patterns, render_event_analysis_table
from phoenix_signal_lab import sample_allowed, select_cycle_baseline_samples
from phoenix_signal_lab import trading_session_label
from phoenix.signal_lab_events import MarketEventContext, MarketEventSample


def make_event_sample(
    *,
    symbol: str,
    sample_type: str,
    bar_interval: str,
    trigger_score: float,
    trigger_types: tuple[str, ...] | None = None,
) -> MarketEventSample:
    return MarketEventSample(
        symbol=symbol,
        sample_type=sample_type,
        trigger_types=trigger_types or (("baseline_control",) if sample_type == "baseline" else ("volume_burst",)),
        trigger_score=trigger_score,
        bar_interval=bar_interval,
        anchor_open_time_ms=0,
        anchor_close_time_ms=1,
        price=1.0,
        quote_volume_24h=1.0,
        price_change_24h_pct=0.0,
        current_volume=1.0,
        avg_volume_20=1.0,
        volume_burst_ratio=1.0,
        current_quote_turnover=1.0,
        avg_quote_turnover_20=1.0,
        candle_body_pct=0.0,
        candle_range_pct=0.0,
        atr_20_pct=0.0,
        body_to_atr=0.0,
        range_to_atr=0.0,
        ret_1bar_pct=0.0,
        ret_5bar_pct=0.0,
        ret_15bar_pct=0.0,
        ret_60bar_pct=0.0,
        candle_direction="up",
    )


def make_event_context(*, symbol: str, bar_interval: str) -> MarketEventContext:
    return MarketEventContext(
        symbol=symbol,
        bar_interval=bar_interval,
        anchor_open_time_ms=0,
        anchor_close_time_ms=1,
        price=1.0,
        quote_volume_24h=10_000_000.0,
        price_change_24h_pct=0.0,
        current_volume=1.0,
        avg_volume_20=1.0,
        volume_burst_ratio=1.0,
        current_quote_turnover=1.0,
        avg_quote_turnover_20=1.0,
        candle_body_pct=0.0,
        candle_range_pct=0.0,
        atr_20_pct=0.0,
        body_to_atr=0.0,
        range_to_atr=0.0,
        ret_1bar_pct=0.0,
        ret_5bar_pct=0.0,
        ret_15bar_pct=0.0,
        ret_60bar_pct=0.0,
        candle_direction="flat",
    )


class SignalLabEventAnalysisTests(unittest.TestCase):
    def test_trading_session_label_maps_utc_windows(self) -> None:
        self.assertEqual(
            trading_session_label(int(datetime(2026, 4, 27, 1, 0, tzinfo=timezone.utc).timestamp() * 1000)),
            "Asia",
        )
        self.assertEqual(
            trading_session_label(int(datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc).timestamp() * 1000)),
            "Europe",
        )
        self.assertEqual(
            trading_session_label(int(datetime(2026, 4, 27, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)),
            "US",
        )
        self.assertEqual(
            trading_session_label(int(datetime(2026, 4, 27, 23, 0, tzinfo=timezone.utc).timestamp() * 1000)),
            "offhours",
        )

    def test_event_trigger_signature_handles_trigger_and_baseline(self) -> None:
        self.assertEqual(
            event_trigger_signature(
                {
                    "sample_type": "trigger",
                    "trigger_types": ["volume_burst", "range_expansion"],
                }
            ),
            "range_expansion+volume_burst",
        )
        self.assertEqual(
            event_trigger_signature(
                {
                    "sample_type": "baseline",
                    "trigger_types": ["baseline_control"],
                }
            ),
            "baseline_control",
        )

    def test_analyze_event_records_groups_by_trigger_and_horizon(self) -> None:
        rows = analyze_event_records(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "trigger_types": ["range_expansion", "volume_burst"],
                    "symbol": "AAAUSDT",
                    "trigger_score": 20.0,
                    "horizons": [
                        {
                            "horizon_sec": 300,
                            "close_return_pct": 0.5,
                            "after_fee_return_pct": 0.42,
                            "max_runup_pct": 1.2,
                            "max_drawdown_pct": -0.4,
                        }
                    ],
                },
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "trigger_types": ["range_expansion", "volume_burst"],
                    "symbol": "BBBUSDT",
                    "trigger_score": 30.0,
                    "horizons": [
                        {
                            "horizon_sec": 300,
                            "close_return_pct": -0.1,
                            "after_fee_return_pct": -0.18,
                            "max_runup_pct": 0.9,
                            "max_drawdown_pct": -0.7,
                        }
                    ],
                },
                {
                    "sample_type": "baseline",
                    "bar_interval": "1m",
                    "trigger_types": ["baseline_control"],
                    "symbol": "CCCUSDT",
                    "trigger_score": 0.0,
                    "horizon": {
                        "horizon_sec": 300,
                        "close_return_pct": 0.02,
                        "after_fee_return_pct": -0.06,
                        "max_runup_pct": 0.4,
                        "max_drawdown_pct": -0.2,
                    },
                },
            ]
        )
        self.assertEqual(len(rows), 2)
        top = rows[0]
        self.assertEqual(top["sample_type"], "trigger")
        self.assertEqual(top["bar_interval"], "5m")
        self.assertEqual(top["trigger_signature"], "range_expansion+volume_burst")
        self.assertEqual(top["observations"], 2)
        self.assertEqual(top["unique_symbols"], 2)
        self.assertAlmostEqual(top["avg_after_fee_pct"], 0.12, places=6)
        self.assertAlmostEqual(top["after_fee_win_rate_pct"], 50.0, places=6)

    def test_analyze_event_records_computes_baseline_deltas(self) -> None:
        rows = analyze_event_records(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "trigger_types": ["volume_burst"],
                    "symbol": "AAAUSDT",
                    "trigger_score": 18.0,
                    "horizon": {
                        "horizon_sec": 300,
                        "close_return_pct": 0.7,
                        "after_fee_return_pct": 0.6,
                        "max_runup_pct": 1.0,
                        "max_drawdown_pct": -0.3,
                    },
                },
                {
                    "sample_type": "baseline",
                    "bar_interval": "5m",
                    "trigger_types": ["baseline_control"],
                    "symbol": "BBBUSD",
                    "trigger_score": 0.0,
                    "horizon": {
                        "horizon_sec": 300,
                        "close_return_pct": 0.2,
                        "after_fee_return_pct": 0.1,
                        "max_runup_pct": 0.5,
                        "max_drawdown_pct": -0.2,
                    },
                },
            ]
        )
        self.assertEqual(len(rows), 2)
        trigger_row = next(row for row in rows if row["sample_type"] == "trigger")
        baseline_row = next(row for row in rows if row["sample_type"] == "baseline")
        self.assertAlmostEqual(trigger_row["baseline_after_fee_pct"] or 0.0, 0.1, places=6)
        self.assertAlmostEqual(trigger_row["delta_after_fee_pct"] or 0.0, 0.5, places=6)
        self.assertAlmostEqual(trigger_row["delta_after_fee_win_rate_pct"] or 0.0, 0.0, places=6)
        self.assertAlmostEqual(baseline_row["delta_after_fee_pct"] or 0.0, 0.0, places=6)

    def test_render_event_analysis_table_includes_baseline_delta_columns(self) -> None:
        table = render_event_analysis_table(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "horizon_sec": 300,
                    "observations": 3,
                    "unique_symbols": 2,
                    "top_symbol_share_pct": 66.7,
                    "avg_after_fee_pct": 0.45,
                    "delta_after_fee_pct": 0.12,
                    "after_fee_win_rate_pct": 66.7,
                    "delta_after_fee_win_rate_pct": 16.7,
                    "avg_max_drawdown_pct": -0.3,
                    "avg_max_runup_pct": 0.8,
                    "avg_trigger_score": 22.5,
                    "trigger_signature": "volume_burst",
                }
            ],
            limit=10,
            show_context=False,
        )
        self.assertIn("dAF%", table)
        self.assertIn("dWin%", table)
        self.assertIn("66.7", table)
        self.assertIn("0.1200", table)

    def test_analyze_event_records_supports_oi_context_split(self) -> None:
        rows = analyze_event_records(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "trigger_types": ["volume_burst"],
                    "symbol": "AAAUSDT",
                    "trigger_score": 18.0,
                    "enrichments": {
                        "oi_change_5m_pct": -3.2,
                        "funding_rate": 0.0001,
                        "mark_index_basis_pct": 0.01,
                        "depth_imbalance": -0.2,
                        "spread_bps": 2.0,
                        "estimated_slippage_bps": 4.0,
                    },
                    "horizon": {
                        "horizon_sec": 300,
                        "close_return_pct": 0.7,
                        "after_fee_return_pct": 0.6,
                        "max_runup_pct": 1.0,
                        "max_drawdown_pct": -0.3,
                    },
                },
                {
                    "sample_type": "baseline",
                    "bar_interval": "5m",
                    "trigger_types": ["baseline_control"],
                    "symbol": "BBBUSD",
                    "trigger_score": 0.0,
                    "enrichments": {
                        "oi_change_5m_pct": -2.5,
                        "funding_rate": 0.0002,
                        "mark_index_basis_pct": 0.02,
                        "depth_imbalance": -0.1,
                        "spread_bps": 2.5,
                        "estimated_slippage_bps": 5.0,
                    },
                    "horizon": {
                        "horizon_sec": 300,
                        "close_return_pct": 0.2,
                        "after_fee_return_pct": 0.1,
                        "max_runup_pct": 0.5,
                        "max_drawdown_pct": -0.2,
                    },
                },
            ],
            context_split="oi",
        )
        trigger_row = next(row for row in rows if row["sample_type"] == "trigger")
        self.assertEqual(trigger_row["context"], "oi_flush")
        self.assertAlmostEqual(trigger_row["delta_after_fee_pct"] or 0.0, 0.5, places=6)

    def test_analyze_event_records_supports_session_context_split(self) -> None:
        rows = analyze_event_records(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "trigger_types": ["volume_burst"],
                    "symbol": "AAAUSDT",
                    "trigger_score": 18.0,
                    "trading_session": "US",
                    "horizon": {
                        "horizon_sec": 300,
                        "close_return_pct": 0.7,
                        "after_fee_return_pct": 0.6,
                        "max_runup_pct": 1.0,
                        "max_drawdown_pct": -0.3,
                    },
                },
                {
                    "sample_type": "baseline",
                    "bar_interval": "5m",
                    "trigger_types": ["baseline_control"],
                    "symbol": "BBBUSD",
                    "trigger_score": 0.0,
                    "trading_session": "US",
                    "horizon": {
                        "horizon_sec": 300,
                        "close_return_pct": 0.2,
                        "after_fee_return_pct": 0.1,
                        "max_runup_pct": 0.5,
                        "max_drawdown_pct": -0.2,
                    },
                },
                {
                    "sample_type": "baseline",
                    "bar_interval": "5m",
                    "trigger_types": ["baseline_control"],
                    "symbol": "CCCUSD",
                    "trigger_score": 0.0,
                    "trading_session": "Europe",
                    "horizon": {
                        "horizon_sec": 300,
                        "close_return_pct": 0.8,
                        "after_fee_return_pct": 0.7,
                        "max_runup_pct": 1.1,
                        "max_drawdown_pct": -0.1,
                    },
                },
            ],
            context_split="session",
        )
        trigger_row = next(row for row in rows if row["sample_type"] == "trigger")
        self.assertEqual(trigger_row["context"], "US")
        self.assertAlmostEqual(trigger_row["delta_after_fee_pct"] or 0.0, 0.5, places=6)

    def test_render_event_analysis_table_can_show_context(self) -> None:
        table = render_event_analysis_table(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "context": "oi_flush",
                    "horizon_sec": 300,
                    "observations": 3,
                    "unique_symbols": 2,
                    "top_symbol_share_pct": 66.7,
                    "avg_after_fee_pct": 0.45,
                    "delta_after_fee_pct": 0.12,
                    "after_fee_win_rate_pct": 66.7,
                    "delta_after_fee_win_rate_pct": 16.7,
                    "avg_max_drawdown_pct": -0.3,
                    "avg_max_runup_pct": 0.8,
                    "avg_trigger_score": 22.5,
                    "trigger_signature": "volume_burst",
                }
            ],
            limit=10,
            show_context=True,
        )
        self.assertIn("context", table)
        self.assertIn("oi_flush", table)

    def test_analyze_event_records_supports_playbook_split(self) -> None:
        rows = analyze_event_records(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "trigger_types": ["range_expansion", "body_expansion", "volume_burst"],
                    "symbol": "AAAUSDT",
                    "trigger_score": 28.0,
                    "sample": {
                        "sample_type": "trigger",
                        "bar_interval": "5m",
                        "candle_direction": "up",
                    },
                    "enrichments": {
                        "oi_change_5m_pct": 0.8,
                        "oi_change_15m_pct": 1.2,
                        "funding_rate": 0.0001,
                        "mark_index_basis_pct": 0.02,
                        "depth_imbalance": 0.05,
                        "spread_bps": 2.0,
                        "estimated_slippage_bps": 4.0,
                    },
                    "horizon": {
                        "horizon_sec": 900,
                        "close_return_pct": 1.2,
                        "after_fee_return_pct": 1.1,
                        "max_runup_pct": 1.8,
                        "max_drawdown_pct": -0.2,
                    },
                },
                {
                    "sample_type": "baseline",
                    "bar_interval": "5m",
                    "trigger_types": ["baseline_control"],
                    "symbol": "BBBUSD",
                    "trigger_score": 0.0,
                    "sample": {
                        "sample_type": "baseline",
                        "bar_interval": "5m",
                        "candle_direction": "flat",
                    },
                    "enrichments": {
                        "oi_change_5m_pct": 0.9,
                        "oi_change_15m_pct": 1.1,
                        "funding_rate": 0.0001,
                        "mark_index_basis_pct": 0.01,
                        "depth_imbalance": 0.0,
                        "spread_bps": 2.0,
                        "estimated_slippage_bps": 4.0,
                    },
                    "horizon": {
                        "horizon_sec": 900,
                        "close_return_pct": 0.1,
                        "after_fee_return_pct": 0.0,
                        "max_runup_pct": 0.4,
                        "max_drawdown_pct": -0.1,
                    },
                },
            ],
            context_split="playbook",
        )
        trigger_row = next(row for row in rows if row["sample_type"] == "trigger")
        self.assertEqual(trigger_row["context"], "oi_build_breakout")
        self.assertAlmostEqual(trigger_row["delta_after_fee_pct"] or 0.0, 1.1, places=6)

    def test_analyze_event_records_supports_playbook_session_split(self) -> None:
        rows = analyze_event_records(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "trigger_types": ["range_expansion", "body_expansion", "volume_burst"],
                    "symbol": "AAAUSDT",
                    "trigger_score": 28.0,
                    "trading_session": "US",
                    "sample": {
                        "sample_type": "trigger",
                        "bar_interval": "5m",
                        "candle_direction": "up",
                    },
                    "enrichments": {
                        "oi_change_5m_pct": 0.8,
                        "oi_change_15m_pct": 1.2,
                        "funding_rate": 0.0001,
                        "mark_index_basis_pct": 0.02,
                        "depth_imbalance": 0.05,
                        "spread_bps": 2.0,
                        "estimated_slippage_bps": 4.0,
                    },
                    "horizon": {
                        "horizon_sec": 900,
                        "close_return_pct": 1.2,
                        "after_fee_return_pct": 1.1,
                        "max_runup_pct": 1.8,
                        "max_drawdown_pct": -0.2,
                    },
                },
                {
                    "sample_type": "baseline",
                    "bar_interval": "5m",
                    "trigger_types": ["baseline_control"],
                    "symbol": "BBBUSD",
                    "trigger_score": 0.0,
                    "trading_session": "US",
                    "sample": {
                        "sample_type": "baseline",
                        "bar_interval": "5m",
                        "candle_direction": "flat",
                    },
                    "enrichments": {
                        "oi_change_5m_pct": 0.9,
                        "oi_change_15m_pct": 1.1,
                        "funding_rate": 0.0001,
                        "mark_index_basis_pct": 0.01,
                        "depth_imbalance": 0.0,
                        "spread_bps": 2.0,
                        "estimated_slippage_bps": 4.0,
                    },
                    "horizon": {
                        "horizon_sec": 900,
                        "close_return_pct": 0.1,
                        "after_fee_return_pct": 0.0,
                        "max_runup_pct": 0.4,
                        "max_drawdown_pct": -0.1,
                    },
                },
            ],
            context_split="playbook_session",
        )
        trigger_row = next(row for row in rows if row["sample_type"] == "trigger")
        self.assertEqual(trigger_row["context"], "US|oi_build_breakout")
        self.assertAlmostEqual(trigger_row["delta_after_fee_pct"] or 0.0, 1.1, places=6)

    def test_analyze_event_records_preserves_precomputed_legacy_playbook_labels(self) -> None:
        rows = analyze_event_records(
            [
                {
                    "playbook": "volume_burst_reversal",
                    "sample_type": "trigger",
                    "bar_interval": "1m",
                    "trigger_types": ["range_expansion", "body_expansion", "volume_burst"],
                    "symbol": "REVUSDT",
                    "trigger_score": 31.0,
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
                        "oi_change_5m_pct": -0.04,
                        "oi_change_15m_pct": -0.08,
                        "funding_rate": 0.0001,
                        "mark_index_basis_pct": 0.01,
                        "depth_imbalance": 0.42,
                        "spread_bps": 2.0,
                        "estimated_slippage_bps": 4.0,
                    },
                    "horizon": {
                        "horizon_sec": 900,
                        "close_return_pct": 0.9,
                        "after_fee_return_pct": 0.8,
                        "max_runup_pct": 1.4,
                        "max_drawdown_pct": -0.4,
                    },
                }
            ],
            context_split="playbook",
        )
        self.assertEqual(rows[0]["context"], "volume_burst_reversal")

    def test_analyze_event_records_supports_liquidation_flush_playbook(self) -> None:
        rows = analyze_event_records(
            [
                {
                    "sample_type": "trigger",
                    "bar_interval": "5m",
                    "trigger_types": ["range_expansion", "body_expansion"],
                    "symbol": "FLUSHUSDT",
                    "trigger_score": 26.0,
                    "sample": {
                        "sample_type": "trigger",
                        "bar_interval": "5m",
                        "candle_direction": "down",
                    },
                    "enrichments": {
                        "oi_change_5m_pct": -0.9,
                        "oi_change_15m_pct": -1.1,
                        "funding_rate": -0.0002,
                        "mark_index_basis_pct": -0.08,
                        "depth_imbalance": 0.24,
                        "spread_bps": 3.0,
                        "estimated_slippage_bps": 6.0,
                    },
                    "horizon": {
                        "horizon_sec": 900,
                        "close_return_pct": 1.4,
                        "after_fee_return_pct": 1.3,
                        "max_runup_pct": 2.1,
                        "max_drawdown_pct": -0.9,
                    },
                }
            ],
            context_split="playbook",
        )
        self.assertEqual(rows[0]["context"], "liquidation_flush")

    def test_sample_allowed_matches_exact_event_pattern(self) -> None:
        patterns = parse_allowed_event_patterns("trigger:5m:volume_burst,baseline:5m:baseline_control")
        self.assertIsNotNone(patterns)
        trigger_sample = make_event_sample(
            symbol="AAAUSDT",
            sample_type="trigger",
            bar_interval="5m",
            trigger_score=12.0,
        )
        ranked_sample = MarketEventSample(
            **{
                **trigger_sample.to_payload(),
                "sample_type": "ranked",
            }
        )
        baseline_sample = MarketEventSample(
            **{
                **trigger_sample.to_payload(),
                "sample_type": "baseline",
                "trigger_types": ("baseline_control",),
                "trigger_score": 0.0,
            }
        )
        self.assertTrue(sample_allowed(trigger_sample, allowed_patterns=patterns))
        self.assertFalse(sample_allowed(ranked_sample, allowed_patterns=patterns))
        self.assertTrue(sample_allowed(baseline_sample, allowed_patterns=patterns))

    def test_select_cycle_baseline_samples_hard_caps_to_one_and_rotates_interval(self) -> None:
        contexts = [
            make_event_context(symbol="AAAUSDT", bar_interval="1m"),
            make_event_context(symbol="BBBUSDT", bar_interval="5m"),
        ]
        first_cycle = select_cycle_baseline_samples(
            eligible_contexts=contexts,
            cycle_no=1,
            requested_scan_offset=0,
            triggered_keys=set(),
            ranked_keys=set(),
            requested_baseline_count=4,
        )
        second_cycle = select_cycle_baseline_samples(
            eligible_contexts=contexts,
            cycle_no=2,
            requested_scan_offset=0,
            triggered_keys=set(),
            ranked_keys=set(),
            requested_baseline_count=4,
        )
        self.assertEqual(len(first_cycle), 1)
        self.assertEqual(len(second_cycle), 1)
        self.assertEqual(first_cycle[0].bar_interval, "1m")
        self.assertEqual(second_cycle[0].bar_interval, "5m")

    def test_combine_cycle_event_samples_reserves_baseline_slot(self) -> None:
        priority_samples = [
            make_event_sample(symbol="AAAUSDT", sample_type="trigger", bar_interval="5m", trigger_score=50.0),
            make_event_sample(symbol="BBBUSDT", sample_type="trigger", bar_interval="5m", trigger_score=40.0),
            make_event_sample(symbol="CCCUSDT", sample_type="trigger", bar_interval="5m", trigger_score=30.0),
        ]
        baseline_samples = [
            make_event_sample(symbol="BASEUSDT", sample_type="baseline", bar_interval="5m", trigger_score=0.0),
        ]
        combined = combine_cycle_event_samples(
            priority_samples=priority_samples,
            baseline_samples=baseline_samples,
            max_events_per_cycle=3,
        )
        self.assertEqual(len(combined), 3)
        self.assertEqual(combined[0].symbol, "AAAUSDT")
        self.assertEqual(combined[1].symbol, "BBBUSDT")
        self.assertEqual(combined[2].sample_type, "baseline")

    def test_enrichment_route_for_sample_uses_lightweight_baseline_path(self) -> None:
        trigger_sample = make_event_sample(
            symbol="AAAUSDT",
            sample_type="trigger",
            bar_interval="5m",
            trigger_score=12.0,
        )
        baseline_sample = make_event_sample(
            symbol="BBBUSD",
            sample_type="baseline",
            bar_interval="1m",
            trigger_score=0.0,
        )
        self.assertEqual(enrichment_route_for_sample(trigger_sample), "full")
        self.assertEqual(enrichment_route_for_sample(baseline_sample), "baseline_light")


if __name__ == "__main__":
    unittest.main()
