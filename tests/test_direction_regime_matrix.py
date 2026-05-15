import unittest

from phoenix.direction_regime_matrix import evaluate_direction_regime


class DirectionRegimeMatrixTests(unittest.TestCase):
    def test_trend_down_allows_short_and_blocks_long(self) -> None:
        allowed = evaluate_direction_regime(candidate_direction="SHORT", market_regime={"regime": "TREND_DOWN"})
        blocked = evaluate_direction_regime(candidate_direction="LONG", market_regime={"regime": "TREND_DOWN"})

        self.assertTrue(allowed.direction_regime_allowed)
        self.assertEqual(allowed.allowed_direction, "SHORT")
        self.assertFalse(blocked.direction_regime_allowed)
        self.assertEqual(blocked.allowed_direction, "SHORT")
        self.assertIn("direction_regime_mismatch", blocked.blocked_by)

    def test_trend_up_allows_long_and_blocks_short(self) -> None:
        allowed = evaluate_direction_regime(candidate_direction="LONG", market_regime={"regime": "TREND_UP"})
        blocked = evaluate_direction_regime(candidate_direction="SHORT", market_regime={"regime": "TREND_UP"})

        self.assertTrue(allowed.direction_regime_allowed)
        self.assertEqual(allowed.allowed_direction, "LONG")
        self.assertFalse(blocked.direction_regime_allowed)
        self.assertEqual(blocked.allowed_direction, "LONG")
        self.assertIn("direction_regime_mismatch", blocked.blocked_by)

    def test_unknown_blocks_all_directions(self) -> None:
        for direction in ("LONG", "SHORT"):
            with self.subTest(direction=direction):
                result = evaluate_direction_regime(candidate_direction=direction, market_regime={"regime": "UNKNOWN"})

                self.assertFalse(result.direction_regime_allowed)
                self.assertEqual(result.allowed_direction, "NONE")
                self.assertIn("market_regime_unknown", result.blocked_by)

    def test_high_vol_defaults_to_no_trade(self) -> None:
        result = evaluate_direction_regime(candidate_direction="LONG", market_regime={"regime": "HIGH_VOL"})

        self.assertFalse(result.direction_regime_allowed)
        self.assertEqual(result.allowed_direction, "NONE")
        self.assertIn("direction_regime_high_vol", result.blocked_by)

    def test_chop_requires_range_edge_evidence(self) -> None:
        blocked = evaluate_direction_regime(
            candidate_direction="LONG",
            market_regime={"regime": "CHOP"},
            candidate={"setup_type": "QUICK_TRADE"},
        )
        allowed = evaluate_direction_regime(
            candidate_direction="LONG",
            market_regime={"regime": "CHOP"},
            candidate={"setup_type": "RANGE_EDGE"},
        )

        self.assertFalse(blocked.direction_regime_allowed)
        self.assertIn("chop_requires_range_edge", blocked.blocked_by)
        self.assertTrue(allowed.direction_regime_allowed)
        self.assertEqual(allowed.allowed_direction, "RANGE_EDGE_ONLY")


if __name__ == "__main__":
    unittest.main()
