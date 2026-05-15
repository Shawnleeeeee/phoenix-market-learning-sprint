from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from phoenix.signal_lab_events import (
    EventTriggerConfig,
    build_baseline_market_event,
    build_confirmed_reversal_market_event,
    build_market_event_context,
    build_ranked_market_event,
    build_triggered_market_event,
    compute_future_path_label,
    compute_open_interest_context,
    compute_orderbook_metrics,
)
from phoenix_signal_lab import (
    DEFAULT_COLLECT_HORIZONS_SEC,
    EventCollectionStore,
    build_market_event_research_context,
    scan_market_events,
)


def make_candle(
    index: int,
    *,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume: float,
) -> list[float]:
    open_time = index * 60_000
    close_time = open_time + 59_999
    return [
        open_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        close_time,
        0.0,
        0,
        0.0,
        0.0,
        0.0,
    ]


class SignalLabEventTests(unittest.TestCase):
    def build_trigger_candles(self) -> list[list[float]]:
        candles: list[list[float]] = []
        for index in range(62):
            candles.append(
                make_candle(
                    index,
                    open_price=100.0,
                    high_price=100.5,
                    low_price=99.5,
                    close_price=100.0,
                    volume=10.0,
                )
            )
        candles[60] = make_candle(
            60,
            open_price=100.0,
            high_price=107.0,
            low_price=99.0,
            close_price=106.0,
            volume=40.0,
        )
        return candles

    def build_confirmed_reversal_candles(self) -> list[list[float]]:
        candles: list[list[float]] = []
        for index in range(62):
            candles.append(
                make_candle(
                    index,
                    open_price=100.0,
                    high_price=100.5,
                    low_price=99.5,
                    close_price=100.0,
                    volume=10.0,
                )
            )
        candles[59] = make_candle(
            59,
            open_price=100.0,
            high_price=101.0,
            low_price=94.0,
            close_price=95.0,
            volume=48.0,
        )
        candles[60] = make_candle(
            60,
            open_price=95.0,
            high_price=97.0,
            low_price=94.5,
            close_price=96.0,
            volume=18.0,
        )
        candles[61] = make_candle(
            61,
            open_price=96.0,
            high_price=96.5,
            low_price=95.5,
            close_price=96.0,
            volume=10.0,
        )
        return candles

    def build_flat_candles(self, *, count: int = 70, volume: float = 10.0) -> list[list[float]]:
        return [
            make_candle(
                index,
                open_price=100.0,
                high_price=100.5,
                low_price=99.5,
                close_price=100.0,
                volume=volume,
            )
            for index in range(count)
        ]

    def test_triggered_market_event_detects_range_body_and_volume(self) -> None:
        config = EventTriggerConfig(
            atr_period=20,
            atr_multiplier=2.0,
            volume_lookback=20,
            volume_multiplier=3.0,
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        context = build_market_event_context(
            {
                "symbol": "TESTUSDT",
                "quoteVolume": 50_000_000.0,
                "priceChangePercent": 4.2,
            },
            self.build_trigger_candles(),
            config=config,
        )
        self.assertIsNotNone(context)
        assert context is not None
        event = build_triggered_market_event(context, config=config)
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.sample_type, "trigger")
        self.assertIn("range_expansion", event.trigger_types)
        self.assertIn("body_expansion", event.trigger_types)
        self.assertIn("volume_burst", event.trigger_types)
        self.assertGreater(event.trigger_score, 0.0)
        self.assertGreater(event.volume_burst_ratio, 3.0)

    def test_triggered_market_event_requires_1m_range_and_volume_pair(self) -> None:
        candles = self.build_trigger_candles()
        candles[60] = make_candle(
            60,
            open_price=100.0,
            high_price=107.0,
            low_price=99.0,
            close_price=106.0,
            volume=18.0,
        )
        config = EventTriggerConfig(
            atr_period=20,
            atr_multiplier=2.0,
            volume_lookback=20,
            volume_multiplier=3.0,
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        context = build_market_event_context(
            {
                "symbol": "MICROUSDT",
                "quoteVolume": 25_000_000.0,
                "priceChangePercent": 1.2,
            },
            candles,
            config=config,
            bar_interval="1m",
        )
        self.assertIsNotNone(context)
        assert context is not None
        self.assertIsNone(build_triggered_market_event(context, config=config))

    def test_triggered_market_event_relaxes_5m_secondary_thresholds(self) -> None:
        candles = self.build_trigger_candles()
        candles[60] = make_candle(
            60,
            open_price=100.0,
            high_price=101.6,
            low_price=100.0,
            close_price=101.4,
            volume=24.0,
        )
        config = EventTriggerConfig(
            atr_period=20,
            atr_multiplier=2.0,
            volume_lookback=20,
            volume_multiplier=3.0,
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        context = build_market_event_context(
            {
                "symbol": "SWINGUSDT",
                "quoteVolume": 60_000_000.0,
                "priceChangePercent": 2.8,
            },
            candles,
            config=config,
            bar_interval="5m",
        )
        self.assertIsNotNone(context)
        assert context is not None
        event = build_triggered_market_event(context, config=config)
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.bar_interval, "5m")
        self.assertIn("range_expansion", event.trigger_types)
        self.assertIn("volume_burst", event.trigger_types)

    def test_confirmed_reversal_market_event_waits_for_next_bullish_close(self) -> None:
        config = EventTriggerConfig(
            atr_period=20,
            atr_multiplier=2.0,
            volume_lookback=20,
            volume_multiplier=3.0,
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        event = build_confirmed_reversal_market_event(
            {
                "symbol": "REVUSDT",
                "quoteVolume": 50_000_000.0,
                "priceChangePercent": -3.8,
            },
            self.build_confirmed_reversal_candles(),
            config=config,
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.sample_type, "trigger")
        self.assertEqual(event.bar_interval, "1m")
        self.assertIn("right_side_confirmation_1m", event.trigger_types)
        self.assertEqual(event.candle_direction, "down")
        self.assertEqual(event.trigger_candle_direction, "down")
        self.assertEqual(event.confirmation_candle_direction, "up")
        self.assertTrue(event.reversal_confirmation_passed)
        self.assertEqual(event.trigger_anchor_close_time_ms, (59 * 60_000) + 59_999)
        self.assertEqual(event.anchor_close_time_ms, (60 * 60_000) + 59_999)
        self.assertAlmostEqual(event.price, 96.0, places=6)

    def test_confirmed_reversal_market_event_rejects_same_direction_follow_through(self) -> None:
        candles = self.build_confirmed_reversal_candles()
        candles[60] = make_candle(
            60,
            open_price=95.0,
            high_price=95.5,
            low_price=92.0,
            close_price=93.0,
            volume=18.0,
        )
        config = EventTriggerConfig(
            atr_period=20,
            atr_multiplier=2.0,
            volume_lookback=20,
            volume_multiplier=3.0,
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        event = build_confirmed_reversal_market_event(
            {
                "symbol": "REVFAILUSDT",
                "quoteVolume": 50_000_000.0,
                "priceChangePercent": -4.0,
            },
            candles,
            config=config,
        )
        self.assertIsNone(event)

    def test_baseline_event_preserves_context_fields(self) -> None:
        config = EventTriggerConfig(
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        context = build_market_event_context(
            {
                "symbol": "BASEUSDT",
                "quoteVolume": 20_000_000.0,
                "priceChangePercent": -1.5,
            },
            self.build_trigger_candles(),
            config=config,
        )
        self.assertIsNotNone(context)
        assert context is not None
        baseline = build_baseline_market_event(context)
        self.assertEqual(baseline.sample_type, "baseline")
        self.assertEqual(baseline.trigger_types, ("baseline_control",))
        self.assertEqual(baseline.symbol, "BASEUSDT")

    def test_ranked_event_captures_near_threshold_anomalies(self) -> None:
        candles = self.build_trigger_candles()
        candles[60] = make_candle(
            60,
            open_price=100.0,
            high_price=101.4,
            low_price=100.0,
            close_price=101.3,
            volume=15.8,
        )
        config = EventTriggerConfig(
            atr_period=20,
            atr_multiplier=2.0,
            volume_lookback=20,
            volume_multiplier=3.0,
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        context = build_market_event_context(
            {
                "symbol": "RANKUSDT",
                "quoteVolume": 40_000_000.0,
                "priceChangePercent": 2.4,
            },
            candles,
            config=config,
        )
        self.assertIsNotNone(context)
        assert context is not None
        self.assertIsNone(build_triggered_market_event(context, config=config))
        ranked = build_ranked_market_event(context, config=config, min_trigger_ratio=0.75)
        self.assertIsNotNone(ranked)
        assert ranked is not None
        self.assertEqual(ranked.sample_type, "ranked")
        self.assertIn("range_expansion", ranked.trigger_types)
        self.assertIn("body_expansion", ranked.trigger_types)
        self.assertIn("volume_burst", ranked.trigger_types)
        self.assertGreater(ranked.trigger_score, 0.0)

    def test_orderbook_metrics_calculates_spread_and_imbalance(self) -> None:
        metrics = compute_orderbook_metrics(
            {
                "bids": [["99", "5"], ["98", "4"], ["97", "3"], ["96", "2"], ["95", "1"]],
                "asks": [["101", "2"], ["102", "2"], ["103", "2"], ["104", "2"], ["105", "2"]],
            },
            reference_price=100.0,
            slippage_notional_usdt=200.0,
        )
        self.assertIsNotNone(metrics.spread_bps)
        self.assertAlmostEqual(metrics.spread_bps or 0.0, 200.0, places=6)
        self.assertIsNotNone(metrics.depth_imbalance)
        self.assertGreater(metrics.depth_imbalance or 0.0, 0.0)
        self.assertIsNotNone(metrics.estimated_slippage_bps)

    def test_future_path_label_captures_return_and_drawdown(self) -> None:
        candles = [
            make_candle(0, open_price=100.0, high_price=102.0, low_price=99.0, close_price=101.0, volume=10.0),
            make_candle(1, open_price=101.0, high_price=105.0, low_price=98.0, close_price=103.0, volume=12.0),
        ]
        label = compute_future_path_label(
            entry_price=100.0,
            candles_1m=candles,
            horizon_sec=300,
            round_trip_fee_bps=10.0,
        )
        self.assertIsNotNone(label)
        assert label is not None
        self.assertAlmostEqual(label.close_return_pct, 3.0, places=6)
        self.assertAlmostEqual(label.after_fee_return_pct, 2.9, places=6)
        self.assertAlmostEqual(label.max_runup_pct, 5.0, places=6)
        self.assertAlmostEqual(label.max_drawdown_pct, -2.0, places=6)

    def test_open_interest_context_uses_recent_history_points(self) -> None:
        payload = compute_open_interest_context(
            120.0,
            [
                {"sumOpenInterest": "80"},
                {"sumOpenInterest": "90"},
                {"sumOpenInterest": "100"},
                {"sumOpenInterest": "110"},
            ],
        )
        self.assertEqual(payload["open_interest"], 120.0)
        self.assertAlmostEqual(payload["oi_change_5m_pct"] or 0.0, 20.0, places=6)
        self.assertAlmostEqual(payload["oi_change_15m_pct"] or 0.0, 50.0, places=6)

    def test_collect_default_horizons_include_short_research_windows(self) -> None:
        self.assertEqual(DEFAULT_COLLECT_HORIZONS_SEC, (60, 180, 300, 900, 1800, 3600))

    def test_event_snapshot_preserves_3m_research_context_and_short_horizons(self) -> None:
        config = EventTriggerConfig(
            atr_period=20,
            atr_multiplier=2.0,
            volume_lookback=20,
            volume_multiplier=3.0,
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        context_1m = build_market_event_context(
            {"symbol": "TESTUSDT", "quoteVolume": 50_000_000.0, "priceChangePercent": 4.2},
            self.build_trigger_candles(),
            config=config,
            bar_interval="1m",
        )
        context_3m = build_market_event_context(
            {"symbol": "TESTUSDT", "quoteVolume": 50_000_000.0, "priceChangePercent": 4.2},
            self.build_flat_candles(),
            config=config,
            bar_interval="3m",
        )
        self.assertIsNotNone(context_1m)
        self.assertIsNotNone(context_3m)
        assert context_1m is not None
        assert context_3m is not None
        sample = build_triggered_market_event(context_1m, config=config)
        self.assertIsNotNone(sample)
        assert sample is not None
        research_contexts = {
            "3m": build_market_event_research_context(context_3m, config=config),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            store = EventCollectionStore(
                output_dir=Path(temp_dir),
                horizons_sec=list(DEFAULT_COLLECT_HORIZONS_SEC),
                dedupe_sec=0,
                symbol_cooldown_sec=0,
                max_active_per_symbol=0,
                round_trip_fee_bps=0.0,
            )
            observation = store.create(
                cycle_no=1,
                rank=1,
                sample=sample,
                enrichments={"research_contexts": research_contexts},
            )
            self.assertIsNotNone(observation)
            snapshot = json.loads((Path(temp_dir) / "event_snapshots.jsonl").read_text(encoding="utf-8"))
            bridge_feed = json.loads((Path(temp_dir) / "bridge_event_feed.jsonl").read_text(encoding="utf-8"))

        self.assertEqual(snapshot["bar_interval"], "1m")
        self.assertIn("volume_burst", snapshot["trigger_types"])
        self.assertEqual(snapshot["horizons_sec"], [60, 180, 300, 900, 1800, 3600])
        self.assertEqual(snapshot["research_contexts"]["3m"]["bar_interval"], "3m")
        self.assertEqual(snapshot["context_3m"]["bar_interval"], "3m")
        self.assertEqual(snapshot["factors"]["factor_version"], "v1.0")
        self.assertIn("trend_score", snapshot["factors"])
        self.assertEqual(bridge_feed["factors"]["factor_version"], "v1.0")
        self.assertIn("liquidity_score", bridge_feed["factors"])

    def test_event_horizon_label_preserves_3m_research_context_for_60s(self) -> None:
        class FakeFutures:
            def __init__(self) -> None:
                self.kline_requests: list[dict[str, object]] = []

            async def klines(self, symbol: str, **kwargs: object) -> list[list[float]]:
                self.kline_requests.append({"symbol": symbol, **kwargs})
                return [
                    make_candle(
                        61,
                        open_price=106.0,
                        high_price=108.0,
                        low_price=105.0,
                        close_price=107.0,
                        volume=20.0,
                    )
                ]

            async def mark_price(self, symbol: str) -> dict[str, str]:
                return {"markPrice": "107"}

        config = EventTriggerConfig(
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        context_1m = build_market_event_context(
            {"symbol": "TESTUSDT", "quoteVolume": 50_000_000.0, "priceChangePercent": 4.2},
            self.build_trigger_candles(),
            config=config,
            bar_interval="1m",
        )
        context_3m = build_market_event_context(
            {"symbol": "TESTUSDT", "quoteVolume": 50_000_000.0, "priceChangePercent": 4.2},
            self.build_flat_candles(),
            config=config,
            bar_interval="3m",
        )
        assert context_1m is not None
        assert context_3m is not None
        sample = build_triggered_market_event(context_1m, config=config)
        assert sample is not None

        with tempfile.TemporaryDirectory() as temp_dir:
            store = EventCollectionStore(
                output_dir=Path(temp_dir),
                horizons_sec=[60],
                dedupe_sec=0,
                symbol_cooldown_sec=0,
                max_active_per_symbol=0,
                round_trip_fee_bps=10.0,
            )
            observation = store.create(
                cycle_no=1,
                rank=1,
                sample=sample,
                enrichments={
                    "research_contexts": {
                        "3m": build_market_event_research_context(context_3m, config=config),
                    }
                },
            )
            assert observation is not None
            fake_futures = FakeFutures()
            finalized = asyncio.run(
                store._finalize_horizon(
                    observation,
                    observation.horizons[60],
                    futures=fake_futures,
                )
            )
            self.assertTrue(finalized)
            label = json.loads((Path(temp_dir) / "event_horizon_labels.jsonl").read_text(encoding="utf-8"))

        self.assertEqual(fake_futures.kline_requests[0]["interval"], "1m")
        self.assertEqual(label["bar_interval"], "1m")
        self.assertIn("volume_burst", label["trigger_types"])
        self.assertEqual(label["horizon"]["horizon_sec"], 60)
        self.assertEqual(label["research_contexts"]["3m"]["bar_interval"], "3m")
        self.assertEqual(label["context_3m"]["bar_interval"], "3m")

    def test_scan_market_events_loads_3m_as_research_only(self) -> None:
        class FakeFutures:
            def __init__(self, owner: SignalLabEventTests) -> None:
                self.owner = owner
                self.kline_requests: list[str] = []

            async def ticker_24hr(self) -> list[dict[str, str]]:
                return [
                    {
                        "symbol": "TESTUSDT",
                        "quoteVolume": "50000000",
                        "priceChangePercent": "4.2",
                        "lastPrice": "106",
                    }
                ]

            async def klines(self, symbol: str, *, interval: str, limit: int) -> list[list[float]]:
                self.kline_requests.append(interval)
                if interval == "1m":
                    return self.owner.build_trigger_candles()
                return self.owner.build_flat_candles()

        config = EventTriggerConfig(
            atr_period=20,
            atr_multiplier=2.0,
            volume_lookback=20,
            volume_multiplier=3.0,
            min_quote_volume_24h=1_000_000.0,
            min_avg_quote_turnover_1m=100.0,
            min_current_quote_turnover_1m=100.0,
        )
        fake_futures = FakeFutures(self)
        result = asyncio.run(
            scan_market_events(
                fake_futures,  # type: ignore[arg-type]
                universe_top=1,
                scan_top=1,
                scan_offset=0,
                min_quote_volume=1_000_000.0,
                market_cache={},
                universe_sort="quote_volume",
                universe_cache_sec=0.0,
                kline_cache_ttl_1m_sec=0.0,
                kline_cache_ttl_3m_sec=0.0,
                kline_cache_ttl_5m_sec=0.0,
                kline_concurrency=1,
                trigger_config=config,
            )
        )

        self.assertIn("3m", fake_futures.kline_requests)
        self.assertTrue(result.triggered_samples)
        self.assertTrue(all(sample.bar_interval != "3m" for sample in result.triggered_samples))
        self.assertEqual(result.research_contexts_by_symbol["TESTUSDT"]["3m"]["bar_interval"], "3m")


if __name__ == "__main__":
    unittest.main()
