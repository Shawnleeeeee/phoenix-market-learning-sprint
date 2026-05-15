from __future__ import annotations

import unittest

from phoenix_factor_library import (
    FACTOR_VERSION,
    build_factor_vector,
    compute_mean_reversion_score,
    compute_trend_score,
)


def base_record() -> dict[str, object]:
    return {
        "bar_interval": "1m",
        "trigger_types": ["volume_burst", "range_expansion"],
        "sample": {
            "price": 100.0,
            "trigger_candle_direction": "up",
            "volume_burst_ratio": 3.5,
            "range_to_atr": 2.4,
            "body_to_atr": 1.7,
            "ret_1bar_pct": 1.2,
            "ret_5bar_pct": 2.1,
            "ret_15bar_pct": 2.8,
        },
        "enrichments": {
            "oi_change_5m_pct": 1.4,
            "oi_change_15m_pct": 2.6,
            "depth_imbalance": 0.42,
            "spread_bps": 3.0,
            "estimated_slippage_bps": 4.0,
            "bid_depth_notional_5": 800_000.0,
            "ask_depth_notional_5": 500_000.0,
            "btcusdt_ret_5m_pct": 0.35,
            "btcusdt_ret_60m_pct": 0.7,
            "ethusdt_ret_5m_pct": 0.2,
            "ethusdt_ret_60m_pct": 0.3,
            "liquidation_long_usd_15m": 0.0,
            "liquidation_short_usd_15m": 80_000.0,
            "taker_buy_ratio_1m": 1.7,
            "taker_buy_ratio_5m": 1.4,
            "aggressive_flow_delta": 0.35,
        },
    }


class FactorLibraryTests(unittest.TestCase):
    def test_trend_alignment_scores_positive_when_momentum_oi_depth_and_market_agree(self) -> None:
        record = base_record()

        factors = build_factor_vector(record)

        self.assertEqual(factors["factor_version"], FACTOR_VERSION)
        self.assertGreater(factors["momentum_score"], 0.5)
        self.assertGreater(factors["trend_score"], 0.6)
        self.assertGreater(factors["oi_pressure_score"], 0.5)
        self.assertGreater(factors["oi_build_score"], 0.5)
        self.assertGreater(factors["flow_score"], 0.3)
        self.assertGreater(factors["microstructure_score"], 0.2)
        self.assertEqual(factors["trend_bucket"], "trend_long_extreme")

    def test_reversion_score_flips_against_exhaustion_move_when_oi_unwinds_and_btc_is_flat(self) -> None:
        record = base_record()
        record["sample"] = {
            **record["sample"],
            "trigger_candle_direction": "down",
            "ret_1bar_pct": -1.4,
            "ret_5bar_pct": -2.5,
            "ret_15bar_pct": -3.0,
        }
        record["enrichments"] = {
            **record["enrichments"],
            "oi_change_5m_pct": -1.4,
            "oi_change_15m_pct": -2.2,
            "btcusdt_ret_5m_pct": 0.05,
            "depth_imbalance": -0.3,
            "liquidation_long_usd_15m": 120_000.0,
            "liquidation_short_usd_15m": 0.0,
        }

        self.assertGreater(compute_mean_reversion_score(record), 0.45)
        self.assertLess(compute_trend_score(record), 0.0)
        self.assertEqual(build_factor_vector(record)["mean_reversion_bucket"], "reversion_long_extreme")

    def test_missing_microstructure_keeps_factor_vector_safe_and_bounded(self) -> None:
        factors = build_factor_vector({"sample": {"trigger_candle_direction": "down"}})

        self.assertLessEqual(abs(factors["momentum_score"]), 1.0)
        self.assertLessEqual(abs(factors["trend_score"]), 1.0)
        self.assertGreaterEqual(factors["liquidity_score"], 0.0)
        self.assertLessEqual(factors["liquidity_score"], 1.0)


if __name__ == "__main__":
    unittest.main()
