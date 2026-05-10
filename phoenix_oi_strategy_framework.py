from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


OI_STRATEGY_IDS = {
    "oi_build_breakout_continuation",
    "bidwall_oi_build_continuation",
    "oi_build_fakeout_reversal",
}

OI_REQUIRED_FEATURES = (
    "oi_state",
    "oi_delta_1m",
    "oi_delta_5m",
    "oi_delta_15m",
    "price_breakout_strength",
    "volume_burst",
    "bidwall_strength",
    "askwall_strength",
    "orderbook_imbalance",
    "depth_support",
    "spread_bps",
    "slippage_bps",
    "btc_regime",
    "eth_regime",
    "hmm_regime",
    "markov_transition_probability",
    "expected_follow_through_bps",
    "invalidation_reason",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_oi_shadow_candidate(
    *,
    strategy_id: str,
    features: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    if strategy_id not in OI_STRATEGY_IDS:
        raise ValueError("strategy_id is not an OI strategy framework id")
    missing = [field for field in OI_REQUIRED_FEATURES if field not in features]
    status = str(manifest.get("status") or "disabled")
    evidence_level = int(manifest.get("evidence_level") or 0)
    return {
        "event": "oi_strategy_shadow_candidate",
        "generated_at": utc_now_iso(),
        "strategy_id": strategy_id,
        "strategy_manifest_id": manifest.get("strategy_id") or strategy_id,
        "strategy_family": manifest.get("family") or "oi_build",
        "manifest_status": status,
        "evidence_level": evidence_level,
        "feature_schema": "oi_strategy_features_v1",
        "missing_features": missing,
        "features": {field: features.get(field) for field in OI_REQUIRED_FEATURES},
        "shadow_only": True,
        "research_only": True,
        "promotion_allowed": False,
        "live_trading_enabled": False,
        "testnet_allowed": False,
        "testnet_block_reason": "oi_strategy_framework_requires_manifest_evidence_cost_and_parity_gates",
    }
