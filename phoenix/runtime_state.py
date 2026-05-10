from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DEFAULT_HERMES_HOME = Path.home() / ".hermes"


def hermes_home() -> Path:
    return Path(str(os.environ.get("HERMES_HOME") or DEFAULT_HERMES_HOME))


def memories_dir() -> Path:
    path = hermes_home() / "memories"
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def load_last_exit() -> dict[str, Any] | None:
    return load_json_file(memories_dir() / "phoenix_last_exit.json")


def evaluate_cooldown_gate(cooldown_minutes: int) -> dict[str, Any]:
    gate: dict[str, Any] = {
        "applied": cooldown_minutes > 0,
        "ok": True,
        "cooldown_minutes": cooldown_minutes,
    }
    if cooldown_minutes <= 0:
        gate["reason"] = "cooldown_disabled"
        return gate

    last_exit = load_last_exit()
    if not isinstance(last_exit, dict):
        gate["reason"] = "no_previous_exit"
        return gate

    closed_at = last_exit.get("closed_at")
    if not isinstance(closed_at, str) or not closed_at:
        gate["reason"] = "no_close_timestamp"
        gate["last_exit"] = last_exit
        return gate

    try:
        closed_dt = datetime.fromisoformat(closed_at)
    except ValueError:
        gate["reason"] = "invalid_close_timestamp"
        gate["last_exit"] = last_exit
        return gate

    if closed_dt.tzinfo is None:
        closed_dt = closed_dt.replace(tzinfo=timezone.utc)

    ready_at = closed_dt + timedelta(minutes=cooldown_minutes)
    now = datetime.now(timezone.utc)
    gate.update(
        {
            "last_closed_at": closed_dt.isoformat(),
            "ready_at": ready_at.isoformat(),
            "last_symbol": last_exit.get("symbol"),
            "last_outcome": last_exit.get("outcome"),
            "last_phase": last_exit.get("phase"),
        }
    )
    if now < ready_at:
        gate["ok"] = False
        gate["minutes_remaining"] = round((ready_at - now).total_seconds() / 60.0, 2)
        gate["reason"] = "cooldown_active"
    else:
        gate["reason"] = "cooldown_elapsed"
    return gate


def autocycle_state_path() -> Path:
    return memories_dir() / "phoenix_autocycle_state.json"


def autocycle_history_path() -> Path:
    return memories_dir() / "phoenix_autocycle_history.jsonl"


def load_autocycle_state() -> dict[str, Any] | None:
    return load_json_file(autocycle_state_path())


def save_autocycle_state(payload: dict[str, Any]) -> None:
    write_json_file(autocycle_state_path(), payload)


def append_autocycle_history(payload: dict[str, Any]) -> None:
    append_jsonl(autocycle_history_path(), payload)


def summarize_candidate(candidate: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(candidate, dict):
        return None
    return {
        "symbol": candidate.get("symbol"),
        "score": candidate.get("score"),
        "mark_price": candidate.get("mark_price"),
        "directional_bias": candidate.get("directional_bias"),
        "directional_score": candidate.get("directional_score"),
        "directional_breakdown": candidate.get("directional_breakdown") or {},
        "blocked_reasons": candidate.get("blocked_reasons") or [],
        "confidence": candidate.get("confidence"),
        "score_breakdown": candidate.get("score_breakdown") or {},
        "social_sentiment": candidate.get("social_sentiment"),
        "social_summary": candidate.get("social_summary"),
        "social_rank": candidate.get("social_rank"),
        "social_hype_change_pct": candidate.get("social_hype_change_pct"),
        "social_signal_freshness": candidate.get("social_signal_freshness"),
        "square_trending_rank": candidate.get("square_trending_rank"),
        "square_search_rank": candidate.get("square_search_rank"),
        "square_discovery_score": candidate.get("square_discovery_score"),
        "oi_delta_pct": candidate.get("oi_delta_pct"),
        "oi_delta_1m_pct": candidate.get("oi_delta_1m_pct"),
        "oi_delta_5m_pct": candidate.get("oi_delta_5m_pct"),
        "oi_delta_15m_pct": candidate.get("oi_delta_15m_pct"),
        "price_change_1m_pct": candidate.get("price_change_1m_pct"),
        "price_change_5m_pct": candidate.get("price_change_5m_pct"),
        "price_change_15m_pct": candidate.get("price_change_15m_pct"),
        "volume_5m_ratio": candidate.get("volume_5m_ratio"),
        "volume_5m_ratio_short": candidate.get("volume_5m_ratio_short"),
        "volume_5m_ratio_medium": candidate.get("volume_5m_ratio_medium"),
        "volume_5m_ratio_long": candidate.get("volume_5m_ratio_long"),
        "spread_bps": candidate.get("spread_bps"),
        "depth_bid_5": candidate.get("depth_bid_5"),
        "depth_ask_5": candidate.get("depth_ask_5"),
        "depth_imbalance": candidate.get("depth_imbalance"),
        "estimated_slippage_bps": candidate.get("estimated_slippage_bps"),
        "estimated_slippage_for_order_usdt": candidate.get("estimated_slippage_for_order_usdt"),
        "funding_rate": candidate.get("funding_rate"),
        "next_funding_time_ms": candidate.get("next_funding_time_ms"),
        "premium_index": candidate.get("premium_index"),
        "mark_index_basis_pct": candidate.get("mark_index_basis_pct"),
        "onboard_date_ms": candidate.get("onboard_date_ms"),
        "listing_age_hours": candidate.get("listing_age_hours"),
        "taker_buy_ratio_1m": candidate.get("taker_buy_ratio_1m"),
        "taker_buy_ratio_5m": candidate.get("taker_buy_ratio_5m"),
        "aggressive_flow_delta": candidate.get("aggressive_flow_delta"),
        "taker_buy_volume_5m": candidate.get("taker_buy_volume_5m"),
        "taker_sell_volume_5m": candidate.get("taker_sell_volume_5m"),
        "liquidation_long_usd": candidate.get("liquidation_long_usd"),
        "liquidation_short_usd": candidate.get("liquidation_short_usd"),
        "liquidation_event_count": candidate.get("liquidation_event_count"),
        "squeeze_probability_hint": candidate.get("squeeze_probability_hint"),
        "market_confirmation_score": candidate.get("market_confirmation_score"),
        "execution_quality_score": candidate.get("execution_quality_score"),
        "event_risk_score": candidate.get("event_risk_score"),
        "directional_conflicts": candidate.get("directional_conflicts") or [],
        "event_flags": candidate.get("event_flags") or [],
        "smart_money_buy_signals": candidate.get("smart_money_buy_signals"),
        "smart_money_sell_signals": candidate.get("smart_money_sell_signals"),
        "smart_money_traders": candidate.get("smart_money_traders"),
        "smart_money_tags": candidate.get("smart_money_tags") or [],
        "smart_money_latest_age_minutes": candidate.get("smart_money_latest_age_minutes"),
        "smart_money_weighted_buy_strength": candidate.get("smart_money_weighted_buy_strength"),
        "topic_names": candidate.get("topic_names") or [],
        "topic_net_inflow_usd": candidate.get("topic_net_inflow_usd"),
        "topic_latest_age_minutes": candidate.get("topic_latest_age_minutes"),
        "topic_freshness": candidate.get("topic_freshness"),
        "audit_flags": candidate.get("audit_flags") or [],
    }


def candidate_score(candidate: dict[str, Any] | None) -> float:
    if not isinstance(candidate, dict):
        return 0.0
    try:
        return float(candidate.get("score") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def is_actionable_candidate(
    candidate: dict[str, Any] | None,
    *,
    minimum_score: float,
    reject_blocked: bool,
) -> bool:
    if not isinstance(candidate, dict):
        return False
    blocked = [str(item) for item in (candidate.get("blocked_reasons") or []) if str(item)]
    if reject_blocked and blocked:
        return False
    return candidate_score(candidate) >= minimum_score


def extract_candidate_views(
    payload: dict[str, Any] | None,
    *,
    minimum_score: float,
    reject_blocked: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "raw_top_candidate": None,
        "actionable_top_candidate": None,
        "actionable_rank": None,
    }
    if not isinstance(payload, dict):
        return result
    top_candidates = payload.get("top_candidates") or []
    if top_candidates and isinstance(top_candidates[0], dict):
        result["raw_top_candidate"] = summarize_candidate(dict(top_candidates[0]))
    for index, candidate in enumerate(top_candidates, start=1):
        if not isinstance(candidate, dict):
            continue
        if not is_actionable_candidate(
            candidate,
            minimum_score=minimum_score,
            reject_blocked=reject_blocked,
        ):
            continue
        result["actionable_top_candidate"] = summarize_candidate(dict(candidate))
        result["actionable_rank"] = index
        break
    return result


def evaluate_significant_candidate_change(
    current_candidate: dict[str, Any] | None,
    previous_candidate: dict[str, Any] | None,
    *,
    absolute_delta: float,
    relative_delta_pct: float,
) -> dict[str, Any]:
    if current_candidate is None:
        return {
            "ok": False,
            "significant": False,
            "reason": "no_current_candidate",
            "reasons": ["no_current_candidate"],
        }

    if previous_candidate is None:
        return {
            "ok": True,
            "significant": True,
            "reason": "first_candidate_seen",
            "reasons": ["first_candidate_seen"],
        }

    current_symbol = str(current_candidate.get("symbol") or "")
    previous_symbol = str(previous_candidate.get("symbol") or "")
    try:
        current_score = float(current_candidate.get("score") or 0.0)
    except (TypeError, ValueError):
        current_score = 0.0
    try:
        previous_score = float(previous_candidate.get("score") or 0.0)
    except (TypeError, ValueError):
        previous_score = 0.0

    reasons: list[str] = []
    if current_symbol != previous_symbol:
        reasons.append("top_symbol_changed")

    score_delta = round(current_score - previous_score, 4)
    relative_denominator = abs(previous_score) if abs(previous_score) > 1e-9 else 1.0
    score_delta_pct = abs(score_delta) / relative_denominator

    if abs(score_delta) >= absolute_delta:
        reasons.append(f"score_delta_abs:{score_delta:+.4f}")
    if score_delta_pct >= relative_delta_pct:
        reasons.append(f"score_delta_pct:{score_delta_pct:.4f}")

    current_blocked = [str(item) for item in (current_candidate.get("blocked_reasons") or []) if str(item)]
    previous_blocked = [str(item) for item in (previous_candidate.get("blocked_reasons") or []) if str(item)]
    if bool(current_blocked) != bool(previous_blocked):
        reasons.append("blocked_state_changed")

    return {
        "ok": True,
        "significant": bool(reasons),
        "reason": reasons[0] if reasons else "candidate_unchanged",
        "reasons": reasons or ["candidate_unchanged"],
        "current_symbol": current_symbol,
        "previous_symbol": previous_symbol,
        "current_score": current_score,
        "previous_score": previous_score,
        "score_delta": score_delta,
        "score_delta_pct": round(score_delta_pct, 4),
        "absolute_threshold": absolute_delta,
        "relative_threshold_pct": relative_delta_pct,
    }
