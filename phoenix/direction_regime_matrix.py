from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

MATRIX_SOURCE = "direction_regime_matrix.v1"


@dataclass(frozen=True, slots=True)
class DirectionRegimeResult:
    market_regime: str
    candidate_direction: str | None
    allowed_direction: str
    direction_regime_allowed: bool
    direction_regime_reason: str
    blocked_by: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_direction_regime(
    *,
    action: Any = None,
    candidate_direction: Any = None,
    market_regime: dict[str, Any] | None = None,
    candidate: dict[str, Any] | None = None,
) -> DirectionRegimeResult:
    """Stage 2 direction discipline shared by Hermes and Risk Governor."""

    market = market_regime if isinstance(market_regime, dict) else {}
    regime = _normalize_regime(market.get("regime") or market.get("market_regime"))
    direction = _normalize_direction(candidate_direction or action)

    if regime == "UNKNOWN":
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction="NONE",
            direction_regime_allowed=False,
            direction_regime_reason="market_regime=UNKNOWN; Stage 2 must fail closed.",
            blocked_by=["market_regime_unknown", "direction_regime_mismatch"],
        )

    if regime == "HIGH_VOL":
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction="NONE",
            direction_regime_allowed=False,
            direction_regime_reason="HIGH_VOL defaults to NO_TRADE until a separately approved exception exists.",
            blocked_by=["direction_regime_mismatch", "direction_regime_high_vol"],
        )

    if direction not in {"LONG", "SHORT"}:
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction=_allowed_direction_for_regime(regime),
            direction_regime_allowed=False,
            direction_regime_reason=f"candidate_direction={direction or 'UNKNOWN'} is not tradeable.",
            blocked_by=["direction_regime_mismatch", "direction_unknown"],
        )

    if regime == "TREND_UP":
        allowed = direction == "LONG"
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction="LONG",
            direction_regime_allowed=allowed,
            direction_regime_reason=(
                "TREND_UP permits LONG only."
                if allowed
                else f"direction-regime mismatch: TREND_UP permits LONG only; candidate_direction={direction}."
            ),
            blocked_by=[] if allowed else ["direction_regime_mismatch", "direction_lock_conflict"],
        )

    if regime == "TREND_DOWN":
        allowed = direction == "SHORT"
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction="SHORT",
            direction_regime_allowed=allowed,
            direction_regime_reason=(
                "TREND_DOWN permits SHORT only."
                if allowed
                else f"direction-regime mismatch: TREND_DOWN permits SHORT only; candidate_direction={direction}."
            ),
            blocked_by=[] if allowed else ["direction_regime_mismatch", "direction_lock_conflict"],
        )

    if regime == "CHOP":
        has_edge = _has_range_edge_evidence(candidate or {})
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction="RANGE_EDGE_ONLY",
            direction_regime_allowed=has_edge,
            direction_regime_reason=(
                "CHOP permits range-edge trades only; range-edge evidence is present."
                if has_edge
                else "CHOP permits range-edge trades only; range-edge evidence is missing."
            ),
            blocked_by=[] if has_edge else ["direction_regime_mismatch", "chop_requires_range_edge"],
        )

    lock = str(market.get("direction_lock") or "").strip().upper()
    if lock == "NO_TRADE":
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction="NONE",
            direction_regime_allowed=False,
            direction_regime_reason="direction_lock=NO_TRADE.",
            blocked_by=["direction_regime_mismatch", "direction_lock_no_trade"],
        )
    if lock == "LONG_ONLY_OR_NO_TRADE":
        allowed = direction == "LONG"
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction="LONG",
            direction_regime_allowed=allowed,
            direction_regime_reason=(
                "direction_lock permits LONG only."
                if allowed
                else f"direction-regime mismatch: direction_lock permits LONG only; candidate_direction={direction}."
            ),
            blocked_by=[] if allowed else ["direction_regime_mismatch", "direction_lock_conflict"],
        )
    if lock == "SHORT_ONLY_OR_NO_TRADE":
        allowed = direction == "SHORT"
        return DirectionRegimeResult(
            market_regime=regime,
            candidate_direction=direction,
            allowed_direction="SHORT",
            direction_regime_allowed=allowed,
            direction_regime_reason=(
                "direction_lock permits SHORT only."
                if allowed
                else f"direction-regime mismatch: direction_lock permits SHORT only; candidate_direction={direction}."
            ),
            blocked_by=[] if allowed else ["direction_regime_mismatch", "direction_lock_conflict"],
        )

    return DirectionRegimeResult(
        market_regime=regime,
        candidate_direction=direction,
        allowed_direction="BOTH",
        direction_regime_allowed=True,
        direction_regime_reason="No restrictive direction-regime rule applies.",
        blocked_by=[],
    )


def direction_regime_fields(result: DirectionRegimeResult | None) -> dict[str, Any]:
    if result is None:
        return {}
    return {
        "market_regime": result.market_regime,
        "candidate_direction": result.candidate_direction,
        "allowed_direction": result.allowed_direction,
        "direction_regime_allowed": result.direction_regime_allowed,
        "direction_regime_reason": result.direction_regime_reason,
        "direction_regime_source": MATRIX_SOURCE,
        "matrix_source": MATRIX_SOURCE,
    }


def _normalize_regime(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"", "NONE", "NULL", "UNAVAILABLE"}:
        return "UNKNOWN"
    if text == "HIGH_VOLATILITY":
        return "HIGH_VOL"
    if text == "RANGE":
        return "CHOP"
    return text


def _normalize_direction(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if text in {"ENTER_LONG", "LONG", "BUY"}:
        return "LONG"
    if text in {"ENTER_SHORT", "SHORT", "SELL"}:
        return "SHORT"
    return text or None


def _allowed_direction_for_regime(regime: str) -> str:
    return {
        "TREND_UP": "LONG",
        "TREND_DOWN": "SHORT",
        "HIGH_VOL": "NONE",
        "CHOP": "RANGE_EDGE_ONLY",
        "UNKNOWN": "NONE",
    }.get(regime, "BOTH")


def _has_range_edge_evidence(candidate: dict[str, Any]) -> bool:
    for key in (
        "range_edge",
        "at_range_edge",
        "range_edge_signal",
        "range_edge_confirmed",
        "range_boundary_confirmed",
    ):
        if _truthy(candidate.get(key)):
            return True
    setup_type = str(candidate.get("setup_type") or "").strip().upper()
    return setup_type in {
        "RANGE_EDGE",
        "RANGE_EDGE_LONG",
        "RANGE_EDGE_SHORT",
        "MEAN_REVERSION_EDGE",
    }


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
