from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

ALLOWED_ACTIONS = {
    "NO_TRADE",
    "ENTER_LONG",
    "ENTER_SHORT",
    "HOLD",
    "EXIT",
    "MOVE_STOP",
    "TAKE_PROFIT",
    "STOP_TRADING",
    "WAIT_FOR_TRIGGER",
}

OPEN_ACTIONS = {"ENTER_LONG", "ENTER_SHORT"}
REDUCE_ONLY_ACTIONS = {"EXIT", "TAKE_PROFIT", "MOVE_STOP"}
ALLOWED_TRADE_TYPES = {"QUICK_TRADE", "SWING_TRADE", "TREND_HOLD", "NONE"}


@dataclass(frozen=True, slots=True)
class HermesDecision:
    decision: str
    action: str
    symbol: str | None
    trade_type: str
    confidence: float
    reason: str
    entry_price_hint: float | None = None
    stop_loss_pct: float | None = None
    stop_loss_price: float | None = None
    take_profit_pct: float | None = None
    take_profit_price: float | None = None
    max_holding_time_sec: int | None = None
    invalidation_condition: str | None = None
    reduce_only: bool = False
    created_at: str = ""
    timestamp: str = ""
    source: str = "HERMES"
    writer: str | None = None
    trace_id: str | None = None
    market_regime: str | None = None
    candidate_direction: str | None = None
    allowed_direction: str | None = None
    direction_regime_allowed: bool | None = None
    direction_regime_reason: str | None = None
    blocked_by: list[str] = field(default_factory=list)
    direction_regime_source: str | None = None
    matrix_source: str | None = None
    candidate_symbol: str | None = None
    normalized_decision: str | None = None
    no_trade_reason: str | None = None
    entry_quality_filter: str | None = None
    entry_quality_version: str | None = None
    entry_quality_checked: bool | None = None
    entry_quality_allowed: bool | None = None
    entry_quality_score: float | None = None
    entry_quality_min_score: float | None = None
    entry_quality_reason: str | None = None
    entry_quality_reasons: list[str] = field(default_factory=list)
    entry_quality_components: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "HermesDecision":
        raw_decision = payload.get("decision")
        action = str(payload.get("action") or (raw_decision if isinstance(raw_decision, str) else "") or "").strip().upper()
        decision_alias = str((raw_decision if isinstance(raw_decision, str) else None) or action).strip().upper()
        trade_type = str(payload.get("trade_type") or "NONE").strip().upper()
        created_at = str(payload.get("created_at") or payload.get("timestamp") or datetime.now(timezone.utc).isoformat())
        timestamp = str(payload.get("timestamp") or created_at)
        reason = str(payload.get("reason") or "").strip()
        symbol = _optional_symbol(payload.get("symbol"))
        return cls(
            decision=decision_alias,
            action=action,
            symbol=symbol,
            trade_type=trade_type,
            confidence=_safe_float(payload.get("confidence"), default=0.0),
            reason=reason,
            entry_price_hint=_optional_float(payload.get("entry_price_hint")),
            stop_loss_pct=_optional_float(payload.get("stop_loss_pct")),
            stop_loss_price=_optional_float(payload.get("stop_loss_price")),
            take_profit_pct=_optional_float(payload.get("take_profit_pct")),
            take_profit_price=_optional_float(payload.get("take_profit_price")),
            max_holding_time_sec=_optional_int(payload.get("max_holding_time_sec")),
            invalidation_condition=_optional_text(payload.get("invalidation_condition")),
            reduce_only=bool(payload.get("reduce_only", False)),
            created_at=created_at,
            timestamp=timestamp,
            source=str(payload.get("source") or "HERMES").strip().upper(),
            writer=_optional_text(payload.get("writer")),
            trace_id=_optional_text(payload.get("trace_id")),
            market_regime=_optional_text(payload.get("market_regime")),
            candidate_direction=_optional_text(payload.get("candidate_direction")),
            allowed_direction=_optional_text(payload.get("allowed_direction")),
            direction_regime_allowed=_optional_bool(payload.get("direction_regime_allowed")),
            direction_regime_reason=_optional_text(payload.get("direction_regime_reason")),
            blocked_by=_optional_text_list(payload.get("blocked_by")),
            direction_regime_source=_optional_text(payload.get("direction_regime_source")),
            matrix_source=_optional_text(payload.get("matrix_source")),
            candidate_symbol=_optional_symbol(payload.get("candidate_symbol")) or symbol,
            normalized_decision=_optional_text(payload.get("normalized_decision")) or action,
            no_trade_reason=_optional_text(payload.get("no_trade_reason")) or (reason if action == "NO_TRADE" else None),
            entry_quality_filter=_optional_text(payload.get("entry_quality_filter")),
            entry_quality_version=_optional_text(payload.get("entry_quality_version")),
            entry_quality_checked=_optional_bool(payload.get("entry_quality_checked")),
            entry_quality_allowed=_optional_bool(payload.get("entry_quality_allowed")),
            entry_quality_score=_optional_float(payload.get("entry_quality_score")),
            entry_quality_min_score=_optional_float(payload.get("entry_quality_min_score")),
            entry_quality_reason=_optional_text(payload.get("entry_quality_reason")),
            entry_quality_reasons=_optional_text_list(payload.get("entry_quality_reasons")),
            entry_quality_components=_optional_dict(payload.get("entry_quality_components")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class HermesDecisionValidation:
    valid: bool
    rejectable: bool
    reasons: list[str]
    decision: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_hermes_decision(
    payload: dict[str, Any] | HermesDecision,
    *,
    min_confidence: float = 0.55,
) -> HermesDecisionValidation:
    decision = payload if isinstance(payload, HermesDecision) else HermesDecision.from_payload(payload)
    reasons: list[str] = []
    rejectable_reasons: list[str] = []

    if decision.action not in ALLOWED_ACTIONS:
        reasons.append("invalid_action")
    if decision.decision and decision.action and decision.decision != decision.action:
        reasons.append("decision_action_mismatch")
    if decision.trade_type not in ALLOWED_TRADE_TYPES:
        reasons.append("invalid_trade_type")
    if decision.source != "HERMES":
        reasons.append("invalid_source")
    if not (0.0 <= decision.confidence <= 1.0):
        reasons.append("invalid_confidence")
    elif decision.confidence < min_confidence:
        rejectable_reasons.append("confidence_below_threshold")

    if decision.action in OPEN_ACTIONS:
        if not decision.symbol:
            reasons.append("missing_symbol_for_entry")
        if decision.trade_type == "NONE":
            reasons.append("missing_trade_type_for_entry")
        if decision.stop_loss_pct is None and decision.stop_loss_price is None:
            reasons.append("missing_stop_loss_for_entry")
    elif decision.action in REDUCE_ONLY_ACTIONS:
        if not decision.symbol:
            reasons.append("missing_symbol_for_reduce_action")
        if not decision.reduce_only:
            reasons.append("reduce_action_must_be_reduce_only")
    elif decision.action == "HOLD" and decision.symbol is None:
        reasons.append("missing_symbol_for_hold")

    if decision.action in {"NO_TRADE", "STOP_TRADING", "WAIT_FOR_TRIGGER"} and not decision.trade_type:
        reasons.append("missing_trade_type")

    all_reasons = reasons + rejectable_reasons
    return HermesDecisionValidation(
        valid=not reasons,
        rejectable=bool(rejectable_reasons),
        reasons=all_reasons,
        decision=decision.to_dict(),
    )


def _optional_symbol(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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


def _optional_text_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item or "").strip()]
    return [str(value)]


def _optional_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}
