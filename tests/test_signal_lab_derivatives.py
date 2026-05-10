from __future__ import annotations

import unittest

from phoenix_signal_lab import DerivativeDataCache, build_market_event_common_enrichments, futures_ws_stream_urls


class SignalLabDerivativeDataTests(unittest.TestCase):
    def test_prod_ws_urls_include_binancefuture_fallback(self) -> None:
        class Environment:
            futures_ws_base = "wss://fstream.binance.com"

        class Futures:
            environment = Environment()

        self.assertEqual(
            futures_ws_stream_urls(Futures(), "!markPrice@arr@1s"),
            [
                "wss://fstream.binance.com/ws/!markPrice@arr@1s",
                "wss://fstream.binancefuture.com/ws/!markPrice@arr@1s",
            ],
        )

    def test_mark_price_payload_updates_funding_and_basis(self) -> None:
        cache = DerivativeDataCache()

        updated = cache.update_mark_payload(
            {
                "E": 1_000,
                "s": "BTCUSDT",
                "p": "101.0",
                "i": "100.0",
                "r": "0.0001",
                "T": 3_600_000,
            }
        )

        self.assertTrue(updated)
        snapshot = cache.snapshot("BTCUSDT", observed_at_ms=2_000)
        self.assertEqual(snapshot["derivatives_data_source"], "ws")
        self.assertEqual(snapshot["mark_price"], 101.0)
        self.assertEqual(snapshot["index_price"], 100.0)
        self.assertEqual(snapshot["funding_rate"], 0.0001)
        self.assertEqual(snapshot["next_funding_time_ms"], 3_600_000)
        self.assertEqual(snapshot["mark_index_basis_pct"], 1.0)
        self.assertEqual(snapshot["derivatives_mark_price_ts_ms"], 1_000)

    def test_liquidation_payload_accumulates_long_short_rolling_notional(self) -> None:
        cache = DerivativeDataCache(liquidation_window_ms=1_000)

        cache.update_liquidation_payload(
            {
                "E": 10_000,
                "o": {
                    "s": "ETHUSDT",
                    "S": "SELL",
                    "ap": "2000",
                    "z": "0.5",
                    "T": 10_000,
                },
            }
        )
        cache.update_liquidation_payload(
            {
                "E": 10_500,
                "o": {
                    "s": "ETHUSDT",
                    "S": "BUY",
                    "p": "1000",
                    "q": "1.25",
                    "T": 10_500,
                },
            }
        )

        snapshot = cache.snapshot("ETHUSDT", observed_at_ms=10_800)
        self.assertEqual(snapshot["liquidation_long_usd_15m"], 1000.0)
        self.assertEqual(snapshot["liquidation_short_usd_15m"], 1250.0)
        self.assertEqual(snapshot["liquidation_event_count_15m"], 2)

        cache.update_liquidation_payload(
            {
                "E": 11_600,
                "o": {
                    "s": "ETHUSDT",
                    "S": "SELL",
                    "ap": "10",
                    "z": "3",
                    "T": 11_600,
                },
            }
        )

        snapshot = cache.snapshot("ETHUSDT", observed_at_ms=11_600)
        self.assertEqual(snapshot["liquidation_long_usd_15m"], 30.0)
        self.assertEqual(snapshot["liquidation_short_usd_15m"], 0.0)
        self.assertEqual(snapshot["liquidation_event_count_15m"], 1)

    def test_common_enrichment_prefers_ws_derivatives_snapshot_over_rest_premium(self) -> None:
        cache = DerivativeDataCache()
        cache.update_mark_payload(
            {
                "E": 1_000,
                "s": "SOLUSDT",
                "p": "205.0",
                "i": "200.0",
                "r": "0.0002",
                "T": 9_999,
            }
        )

        enrichments = build_market_event_common_enrichments(
            premium_payload={
                "markPrice": "190.0",
                "indexPrice": "200.0",
                "lastFundingRate": "0.1",
                "nextFundingTime": 123,
            },
            btc_macro={},
            eth_macro={},
            derivatives_snapshot=cache.snapshot("SOLUSDT", observed_at_ms=2_000),
        )

        self.assertEqual(enrichments["mark_price"], 205.0)
        self.assertEqual(enrichments["funding_rate"], 0.0002)
        self.assertEqual(enrichments["next_funding_time_ms"], 9_999)
        self.assertEqual(enrichments["mark_index_basis_pct"], 2.5)
        self.assertEqual(enrichments["derivatives_data_source"], "ws")


if __name__ == "__main__":
    unittest.main()
