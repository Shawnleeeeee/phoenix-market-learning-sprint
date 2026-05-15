from phoenix.entry_quality_filter import evaluate_entry_quality


def trend_down() -> dict:
    return {
        "regime": "TREND_DOWN",
        "btc_trend_1m": -0.12,
        "btc_trend_5m": -0.30,
        "eth_trend_1m": 0.02,
        "eth_trend_5m": -0.10,
    }


def trend_up() -> dict:
    return {
        "regime": "TREND_UP",
        "btc_trend_1m": 0.12,
        "btc_trend_5m": 0.30,
        "eth_trend_1m": -0.02,
        "eth_trend_5m": 0.10,
    }


def short_candidate(**overrides):
    candidate = {
        "symbol": "SOLUSDT",
        "bias": "SHORT",
        "price_change_1m_pct": -0.18,
        "price_change_5m_pct": -0.55,
        "momentum_follow_through": True,
        "spread_bps": 0.8,
        "estimated_slippage_bps": 1.2,
        "estimated_fee_bps": 4.0,
        "suggested_stop_pct": 0.55,
        "invalidation_distance_bps": 55.0,
    }
    candidate.update(overrides)
    return candidate


def long_candidate(**overrides):
    candidate = {
        "symbol": "ETHUSDT",
        "bias": "LONG",
        "price_change_1m_pct": 0.18,
        "price_change_5m_pct": 0.55,
        "momentum_follow_through": True,
        "spread_bps": 0.8,
        "estimated_slippage_bps": 1.2,
        "estimated_fee_bps": 4.0,
        "suggested_stop_pct": 0.55,
        "invalidation_distance_bps": 55.0,
    }
    candidate.update(overrides)
    return candidate


def test_trend_down_short_rejects_late_chase_after_large_drop():
    result = evaluate_entry_quality(
        short_candidate(price_change_1m_pct=-1.10, price_change_5m_pct=-2.20),
        trend_down(),
    )

    assert result.entry_quality_checked is True
    assert result.entry_quality_allowed is False
    assert "entry_quality_filter_failed" in result.blocked_by
    assert "late_chase_short" in result.blocked_by


def test_trend_down_short_rejects_when_btc_eth_not_weak():
    result = evaluate_entry_quality(
        short_candidate(),
        {"regime": "TREND_DOWN", "btc_trend_1m": 0.04, "btc_trend_5m": 0.02, "eth_trend_1m": 0.03},
    )

    assert result.entry_quality_allowed is False
    assert "btc_eth_alignment_missing" in result.blocked_by


def test_trend_down_short_allows_continuation_low_cost_near_invalidation():
    result = evaluate_entry_quality(short_candidate(), trend_down())

    assert result.entry_quality_allowed is True
    assert result.entry_quality_score >= result.entry_quality_min_score
    assert result.blocked_by == []


def test_fee_slippage_drag_high_rejects():
    result = evaluate_entry_quality(
        long_candidate(spread_bps=5.0, estimated_slippage_bps=5.0, estimated_fee_bps=4.0),
        trend_up(),
    )

    assert result.entry_quality_allowed is False
    assert "fee_slippage_drag_high" in result.blocked_by


def test_entry_quality_weak_rejects_far_from_invalidation_and_no_follow_through():
    result = evaluate_entry_quality(
        long_candidate(momentum_follow_through=False, invalidation_distance_bps=140.0),
        trend_up(),
    )

    assert result.entry_quality_allowed is False
    assert "momentum_follow_through_missing" in result.blocked_by
    assert "entry_far_from_invalidation" in result.blocked_by
