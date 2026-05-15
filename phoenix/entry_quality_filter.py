from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


ENTRY_QUALITY_VERSION = "stage2_v0.4"
ENTRY_QUALITY_FILTER_NAME = "stage2_v0.4_entry_quality"
ENTRY_QUALITY_MIN_SCORE = 0.75

OBSERVE_ONLY_SYMBOLS = {"ZECUSDT", "XMRUSDT"}
PREFERRED_EXECUTION_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"}
LATE_CHASE_1M_PCT = 0.80
LATE_CHASE_5M_PCT = 1.60
MAX_TOTAL_COST_BPS = 10.0
MAX_INVALIDATION_DISTANCE_BPS = 90.0
MIN_MARKET_ALIGNMENT_PCT = 0.05
MIN_FOLLOW_THROUGH_PCT = 0.03
NO_FOLLOW_THROUGH_EXIT_SEC = 120
NO_FOLLOW_THROUGH_MIN_MFE_PCT = 0.0


@dataclass(frozen=True, slots=True)
class EntryQualityResult:
    entry_quality_filter: str = ENTRY_QUALITY_FILTER_NAME
    entry_quality_version: str = ENTRY_QUALITY_VERSION
    entry_quality_checked: bool = True
    entry_quality_allowed: bool = False
    entry_quality_score: float = 0.0
    entry_quality_min_score: float = ENTRY_QUALITY_MIN_SCORE
    entry_quality_reason: str = ""
    entry_quality_reasons: list[str] = field(default_factory=list)
    entry_quality_components: dict[str, Any] = field(default_factory=dict)
    no_follow_through_exit_enabled: bool = True
    no_follow_through_exit_sec: int = NO_FOLLOW_THROUGH_EXIT_SEC
    no_follow_through_min_mfe_pct: float = NO_FOLLOW_THROUGH_MIN_MFE_PCT
    blocked_by: list[str] = field(default_factory=list)

    def to_fields(self) -> dict[str, Any]:
        return asdict(self)


def entry_quality_fields(result: EntryQualityResult | dict[str, Any] | None) -> dict[str, Any]:
    if result is None:
        return {}
    payload = result.to_fields() if isinstance(result, EntryQualityResult) else dict(result)
    keys = (
        "entry_quality_filter",
        "entry_quality_version",
        "entry_quality_checked",
        "entry_quality_allowed",
        "entry_quality_score",
        "entry_quality_min_score",
        "entry_quality_reason",
        "entry_quality_reasons",
        "entry_quality_components",
        "no_follow_through_exit_enabled",
        "no_follow_through_exit_sec",
        "no_follow_through_min_mfe_pct",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def evaluate_entry_quality(
    candidate: dict[str, Any] | None,
    market_regime: dict[str, Any] | None = None,
    *,
    action: str | None = None,
) -> EntryQualityResult:
    candidate = candidate or {}
    market_regime = market_regime or {}
    symbol = str(candidate.get("symbol") or candidate.get("candidate_symbol") or "UNKNOWN").upper()
    direction = _candidate_direction(candidate, action=action)
    reasons: list[str] = []
    blockers: list[str] = []
    components: dict[str, Any] = {
        "symbol": symbol,
        "candidate_direction": direction,
        "market_regime": str(market_regime.get("regime") or "UNKNOWN").upper(),
    }
    components["symbol_policy"] = _symbol_policy(symbol)

    if symbol in OBSERVE_ONLY_SYMBOLS:
        reasons.append(f"{symbol} is observe-only in v0.4 initial symbol policy.")
        blockers.append("entry_quality_symbol_observe_only")

    move_1m = _first_float(candidate, "price_change_1m_pct", "move_1m_pct", "return_1m_pct", "candidate_move_1m_pct")
    if move_1m is None:
        move_1m_bps = _first_float(candidate, "move_1m_bps", "return_1m_bps", "price_change_1m_bps")
        move_1m = None if move_1m_bps is None else move_1m_bps / 100.0
    move_5m = _first_float(candidate, "price_change_5m_pct", "move_5m_pct", "return_5m_pct", "candidate_move_5m_pct")
    if move_5m is None:
        move_5m_bps = _first_float(candidate, "move_5m_bps", "return_5m_bps", "price_change_5m_bps")
        move_5m = None if move_5m_bps is None else move_5m_bps / 100.0
    components["move_1m_pct"] = move_1m
    components["move_5m_pct"] = move_5m

    late_chase = _late_chase(direction=direction, move_1m=move_1m, move_5m=move_5m)
    components["late_chase"] = late_chase
    if late_chase:
        reasons.append(f"{direction} would chase after move_1m={move_1m}%, move_5m={move_5m}%.")
        blockers.append(f"late_chase_{direction.lower()}")

    market_alignment = _btc_eth_alignment(direction=direction, market_regime=market_regime)
    components["btc_eth_alignment"] = market_alignment
    if not market_alignment["aligned"]:
        reasons.append(market_alignment["reason"])
        blockers.append("btc_eth_alignment_missing")

    follow_through = _momentum_follow_through(direction=direction, candidate=candidate, move_1m=move_1m, move_5m=move_5m)
    components["momentum_follow_through"] = follow_through
    if not follow_through["ok"]:
        reasons.append(follow_through["reason"])
        blockers.append("momentum_follow_through_missing")

    cost = _total_cost_bps(candidate)
    components["total_cost_bps"] = cost
    if cost is None:
        reasons.append("fee/slippage cost unavailable.")
        blockers.append("fee_slippage_cost_unavailable")
    elif cost > MAX_TOTAL_COST_BPS:
        reasons.append(f"fee/slippage drag too high: total_cost_bps={cost:g}.")
        blockers.append("fee_slippage_drag_high")

    invalidation_distance = _invalidation_distance_bps(candidate)
    components["invalidation_distance_bps"] = invalidation_distance
    if invalidation_distance is None:
        reasons.append("entry distance to invalidation unavailable.")
        blockers.append("entry_invalidation_distance_unavailable")
    elif invalidation_distance > MAX_INVALIDATION_DISTANCE_BPS:
        reasons.append(f"entry too far from invalidation: {invalidation_distance:g} bps.")
        blockers.append("entry_far_from_invalidation")

    score = _entry_quality_score(components, blockers)
    allowed = not blockers and score >= ENTRY_QUALITY_MIN_SCORE
    if not allowed and not reasons:
        reasons.append("entry quality score below threshold.")
        blockers.append("entry_quality_score_below_threshold")
    reason = "Entry quality passed for v0.4 testnet exploration." if allowed else "; ".join(reasons)
    return EntryQualityResult(
        entry_quality_allowed=allowed,
        entry_quality_score=score,
        entry_quality_reason=reason,
        entry_quality_reasons=reasons,
        entry_quality_components=components,
        blocked_by=["entry_quality_filter_failed", *blockers] if not allowed else [],
    )


def _candidate_direction(candidate: dict[str, Any], *, action: str | None = None) -> str:
    text = str(
        candidate.get("bias")
        or candidate.get("candidate_direction")
        or candidate.get("direction")
        or candidate.get("side")
        or action
        or ""
    ).upper()
    if text in {"ENTER_LONG", "BUY"}:
        return "LONG"
    if text in {"ENTER_SHORT", "SELL"}:
        return "SHORT"
    return text if text in {"LONG", "SHORT"} else "UNKNOWN"


def _late_chase(*, direction: str, move_1m: float | None, move_5m: float | None) -> bool:
    if direction == "SHORT":
        return (move_1m is not None and move_1m <= -LATE_CHASE_1M_PCT) or (
            move_5m is not None and move_5m <= -LATE_CHASE_5M_PCT
        )
    if direction == "LONG":
        return (move_1m is not None and move_1m >= LATE_CHASE_1M_PCT) or (
            move_5m is not None and move_5m >= LATE_CHASE_5M_PCT
        )
    return True


def _symbol_policy(symbol: str) -> dict[str, Any]:
    if symbol in OBSERVE_ONLY_SYMBOLS:
        return {
            "mode": "observe_only",
            "execution_priority": "observe_only",
            "reason": f"{symbol} is downgraded to observation in v0.4 initial symbol policy.",
        }
    if symbol in PREFERRED_EXECUTION_SYMBOLS:
        return {
            "mode": "execution_candidate",
            "execution_priority": "preferred",
            "reason": f"{symbol} is in the v0.4 preferred liquid symbol set.",
        }
    return {
        "mode": "execution_candidate",
        "execution_priority": "standard",
        "reason": f"{symbol} may execute only if liquidity, cost, and micro-notional checks pass.",
    }


def _btc_eth_alignment(*, direction: str, market_regime: dict[str, Any]) -> dict[str, Any]:
    values = [
        _trend_value(market_regime, "btc_trend_1m", "btc_1m_pct", "btc_price_change_1m_pct"),
        _trend_value(market_regime, "btc_trend_5m", "btc_5m_pct", "btc_price_change_5m_pct"),
        _trend_value(market_regime, "eth_trend_1m", "eth_1m_pct", "eth_price_change_1m_pct"),
        _trend_value(market_regime, "eth_trend_5m", "eth_5m_pct", "eth_price_change_5m_pct"),
    ]
    clean = [value for value in values if value is not None]
    if direction == "SHORT":
        aligned = any(value <= -MIN_MARKET_ALIGNMENT_PCT for value in clean)
        return {
            "aligned": aligned,
            "values_pct": clean,
            "reason": (
                "BTC/ETH alignment present for SHORT."
                if aligned
                else "BTC/ETH alignment missing for SHORT: neither BTC nor ETH is weak."
            ),
        }
    if direction == "LONG":
        aligned = any(value >= MIN_MARKET_ALIGNMENT_PCT for value in clean)
        return {
            "aligned": aligned,
            "values_pct": clean,
            "reason": (
                "BTC/ETH alignment present for LONG."
                if aligned
                else "BTC/ETH alignment missing for LONG: neither BTC nor ETH is strong."
            ),
        }
    return {"aligned": False, "values_pct": clean, "reason": "candidate direction unavailable for BTC/ETH alignment."}


def _momentum_follow_through(
    *,
    direction: str,
    candidate: dict[str, Any],
    move_1m: float | None,
    move_5m: float | None,
) -> dict[str, Any]:
    explicit_source = (
        candidate.get("momentum_follow_through")
        if "momentum_follow_through" in candidate
        else candidate.get("momentum_follow_through_ok")
    )
    explicit = _optional_bool(explicit_source)
    if explicit is not None:
        return {
            "ok": explicit,
            "source": "explicit_bool",
            "reason": f"explicit momentum_follow_through={str(explicit).lower()}.",
        }
    score = _first_float(candidate, "momentum_follow_through_score", "follow_through_score", "continuation_score")
    if score is not None:
        return {
            "ok": score >= 0.55,
            "source": "score",
            "score": score,
            "reason": f"momentum follow-through score too low: {score:g}.",
        }
    if direction == "SHORT" and any(value is not None and value <= -MIN_FOLLOW_THROUGH_PCT for value in (move_1m, move_5m)):
        return {"ok": True, "source": "price_change", "reason": "short-side price follow-through present."}
    if direction == "LONG" and any(value is not None and value >= MIN_FOLLOW_THROUGH_PCT for value in (move_1m, move_5m)):
        return {"ok": True, "source": "price_change", "reason": "long-side price follow-through present."}
    return {"ok": False, "source": "unavailable", "reason": "candidate follow-through is stale or unavailable."}


def _total_cost_bps(candidate: dict[str, Any]) -> float | None:
    spread = _first_float(candidate, "spread_bps", "spread_bps_at_entry")
    slippage = _first_float(candidate, "estimated_slippage_bps", "slippage_bps")
    fee = _first_float(candidate, "estimated_fee_bps", "fee_bps", "round_trip_fee_bps")
    if fee is None:
        fee = 4.0
    if spread is None or slippage is None:
        return None
    return round(max(0.0, spread) + max(0.0, slippage) + max(0.0, fee), 6)


def _invalidation_distance_bps(candidate: dict[str, Any]) -> float | None:
    direct = _first_float(candidate, "invalidation_distance_bps", "entry_to_invalidation_bps")
    if direct is not None:
        return abs(direct)
    stop_pct = _first_float(candidate, "suggested_stop_pct", "stop_loss_pct")
    if stop_pct is not None and stop_pct > 0:
        return round(stop_pct * 100.0, 6)
    return None


def _entry_quality_score(components: dict[str, Any], blockers: list[str]) -> float:
    if blockers:
        return round(max(0.0, 1.0 - 0.18 * len(set(blockers))), 4)
    cost = components.get("total_cost_bps")
    invalidation = components.get("invalidation_distance_bps")
    cost_bonus = 0.08 if isinstance(cost, (int, float)) and cost <= 6.0 else 0.0
    invalidation_bonus = 0.07 if isinstance(invalidation, (int, float)) and invalidation <= 70.0 else 0.0
    return round(min(1.0, ENTRY_QUALITY_MIN_SCORE + cost_bonus + invalidation_bonus), 4)


def _trend_value(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = payload.get(key)
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
        text = str(value or "").strip().lower()
        if text in {"weak", "down", "bearish", "short"}:
            return -MIN_MARKET_ALIGNMENT_PCT
        if text in {"strong", "up", "bullish", "long"}:
            return MIN_MARKET_ALIGNMENT_PCT
    return None


def _first_float(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = payload.get(key)
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.strip().lower() in {"unavailable", "unknown", "none", "nan"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None
