from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from phoenix.hermes_decision import validate_hermes_decision
from phoenix.risk_governor import append_jsonl


@dataclass(frozen=True, slots=True)
class HermesDecisionProviderResult:
    provider: str
    decision: dict[str, Any]
    raw_response: dict[str, Any] | None
    ok: bool
    fallback_reason: str | None
    created_at: str
    raw_decision: dict[str, Any] | None = None
    normalized_decision: dict[str, Any] | None = None
    fallback_decision: dict[str, Any] | None = None
    contract_warnings: list[str] = field(default_factory=list)
    contract_errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        fallback_used = self.fallback_reason is not None or not self.ok
        payload["fallback_used"] = fallback_used
        payload["decision_origin"] = _decision_origin(self.provider, fallback_used, self.fallback_reason)
        return payload


class HermesDecisionProvider(Protocol):
    name: str

    async def decide(self, snapshot: dict[str, Any]) -> HermesDecisionProviderResult:
        ...


class MockHermesDecisionProvider:
    name = "mock"

    def __init__(self, decision: dict[str, Any] | None = None) -> None:
        self._decision = decision

    async def decide(self, snapshot: dict[str, Any]) -> HermesDecisionProviderResult:
        del snapshot
        decision = self._decision or no_trade_decision("mock provider defaulted to NO_TRADE")
        return _validated_or_fallback("mock", decision, raw_response=decision)


class FileHermesDecisionProvider:
    name = "file"

    def __init__(
        self,
        path: str | Path,
        *,
        timeout_sec: float = 0.0,
        poll_interval_sec: float = 0.25,
    ) -> None:
        self.path = Path(path)
        self.timeout_sec = max(0.0, float(timeout_sec or 0.0))
        self.poll_interval_sec = max(0.05, float(poll_interval_sec or 0.25))

    async def decide(self, snapshot: dict[str, Any]) -> HermesDecisionProviderResult:
        path = self._decision_path(snapshot)
        if not await self._wait_for_path(path):
            return fallback_result(
                "file",
                f"file_provider_timeout_waiting_for_decision:{path.name}",
                raw_response={"path": str(path), "trace_id": _trace_id_from_snapshot(snapshot)},
            )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            return fallback_result("file", f"file_provider_error:{str(exc)[:160]}")
        if not isinstance(payload, dict):
            return fallback_result("file", "file_provider_payload_not_object", raw_response={"payload": payload})
        raw_response = {**payload, "_decision_path": str(path)}
        contract = _normalize_decision_contract(payload)
        raw_response["contract_warnings"] = contract["warnings"]
        raw_response["contract_errors"] = contract["errors"]
        if contract["errors"]:
            return fallback_result(
                "file",
                "hermes_decision_contract_invalid:" + ",".join(contract["errors"]),
                raw_response=raw_response,
                raw_decision=contract["raw_decision"],
                contract_warnings=contract["warnings"],
                contract_errors=contract["errors"],
            )
        return _validated_or_fallback(
            "file",
            contract["normalized_decision"],
            raw_response=raw_response,
            raw_decision=contract["raw_decision"],
            contract_warnings=contract["warnings"],
            contract_errors=contract["errors"],
        )

    def _decision_path(self, snapshot: dict[str, Any]) -> Path:
        trace_id = _trace_id_from_snapshot(snapshot)
        text = str(self.path)
        if "{trace_id}" in text:
            return Path(text.format(trace_id=trace_id))
        if self.path.exists() and self.path.is_dir():
            return self.path / f"decision_{trace_id}.json"
        if not self.path.suffix:
            return self.path / f"decision_{trace_id}.json"
        return self.path

    async def _wait_for_path(self, path: Path) -> bool:
        if path.exists():
            return True
        if self.timeout_sec <= 0:
            return False
        deadline = asyncio.get_running_loop().time() + self.timeout_sec
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(min(self.poll_interval_sec, max(0.05, deadline - asyncio.get_running_loop().time())))
            if path.exists():
                return True
        return path.exists()


class HttpHermesDecisionProvider:
    name = "http"

    def __init__(
        self,
        endpoint: str | None,
        *,
        timeout_sec: float = 10.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.timeout_sec = timeout_sec
        self.headers = headers or {}

    async def decide(self, snapshot: dict[str, Any]) -> HermesDecisionProviderResult:
        if not self.endpoint:
            return fallback_result("http", "http_provider_endpoint_missing")
        try:
            payload = await asyncio.wait_for(self._post_snapshot(snapshot), timeout=max(0.1, self.timeout_sec))
        except asyncio.TimeoutError:
            return fallback_result("http", "http_provider_timeout")
        except Exception as exc:  # noqa: BLE001
            return fallback_result("http", f"http_provider_error:{str(exc)[:160]}")
        if not isinstance(payload, dict):
            return fallback_result("http", "http_provider_response_not_object", raw_response={"payload": payload})
        contract = _normalize_decision_contract(payload)
        payload["contract_warnings"] = contract["warnings"]
        payload["contract_errors"] = contract["errors"]
        if contract["raw_decision"] is None:
            return fallback_result("http", "http_provider_decision_missing", raw_response=payload)
        if contract["errors"]:
            return fallback_result(
                "http",
                "hermes_decision_contract_invalid:" + ",".join(contract["errors"]),
                raw_response=payload,
                raw_decision=contract["raw_decision"],
                contract_warnings=contract["warnings"],
                contract_errors=contract["errors"],
            )
        return _validated_or_fallback(
            "http",
            contract["normalized_decision"],
            raw_response=payload,
            raw_decision=contract["raw_decision"],
            contract_warnings=contract["warnings"],
            contract_errors=contract["errors"],
        )

    async def _post_snapshot(self, snapshot: dict[str, Any]) -> Any:
        import aiohttp

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout_sec)) as session:
            async with session.post(self.endpoint, json={"snapshot": snapshot}, headers=self.headers) as response:
                response.raise_for_status()
                return await response.json()


def provider_from_name(
    name: str,
    *,
    decision_file: str | Path | None = None,
    http_endpoint: str | None = None,
    http_timeout_sec: float = 10.0,
    decision_timeout_sec: float = 0.0,
    decision_poll_interval_sec: float = 0.25,
) -> HermesDecisionProvider:
    normalized = str(name or "mock").strip().lower()
    if normalized == "mock":
        return MockHermesDecisionProvider()
    if normalized == "file":
        if decision_file is None:
            return MockHermesDecisionProvider(no_trade_decision("file provider missing --decision-file"))
        return FileHermesDecisionProvider(
            decision_file,
            timeout_sec=decision_timeout_sec,
            poll_interval_sec=decision_poll_interval_sec,
        )
    if normalized == "http":
        return HttpHermesDecisionProvider(http_endpoint, timeout_sec=http_timeout_sec)
    return MockHermesDecisionProvider(no_trade_decision(f"unknown provider {normalized}; fallback NO_TRADE"))


def append_provider_log(path: str | Path, result: HermesDecisionProviderResult) -> None:
    append_jsonl(path, {"event": "hermes_decision_provider", **result.to_dict()})


def no_trade_decision(reason: str, *, source: str = "HERMES") -> dict[str, Any]:
    created_at = datetime.now(timezone.utc).isoformat()
    return {
        "decision": "NO_TRADE",
        "action": "NO_TRADE",
        "symbol": None,
        "trade_type": "NONE",
        "confidence": 1.0,
        "reason": reason,
        "created_at": created_at,
        "timestamp": created_at,
        "source": source,
    }


def _normalize_decision_contract(payload: dict[str, Any]) -> dict[str, Any]:
    warnings: list[str] = []
    errors: list[str] = []
    raw_decision: dict[str, Any] | None = None

    payload_decision = payload.get("decision")
    outer_action = _optional_action(payload.get("action"))
    decision_action = _optional_action(payload_decision) if isinstance(payload_decision, str) else None

    if isinstance(payload_decision, dict):
        raw_decision = dict(payload_decision)
        nested_action = _optional_action(raw_decision.get("action"))
        nested_decision = _optional_action(raw_decision.get("decision"))
        action = nested_action or nested_decision or outer_action
        _append_action_mismatch(errors, outer_action, action)
        _append_action_mismatch(errors, nested_action, nested_decision)
        for key in ("trace_id", "source", "writer", "timestamp", "created_at", "reason", "confidence"):
            if key not in raw_decision and key in payload:
                raw_decision[key] = payload[key]
    elif isinstance(payload_decision, str):
        action = decision_action or outer_action
        _append_action_mismatch(errors, outer_action, decision_action)
        raw_decision = dict(payload)
    elif outer_action:
        action = outer_action
        raw_decision = dict(payload)
    else:
        return {
            "raw_decision": None,
            "normalized_decision": {},
            "warnings": warnings,
            "errors": ["decision_action_missing"],
        }

    if not action:
        errors.append("decision_action_missing")
        action = ""

    decision = _decision_from_contract(raw_decision or payload, action=action, warnings=warnings)
    return {
        "raw_decision": raw_decision,
        "normalized_decision": decision,
        "warnings": warnings,
        "errors": list(dict.fromkeys(errors)),
    }


def _decision_from_contract(payload: dict[str, Any], *, action: str, warnings: list[str]) -> dict[str, Any]:
    trade_type = "NONE" if action in {"NO_TRADE", "STOP_TRADING", "WAIT_FOR_TRIGGER"} else str(payload.get("trade_type") or "QUICK_TRADE").upper()
    created_at = payload.get("created_at")
    timestamp = payload.get("timestamp")
    if not created_at and not timestamp:
        warnings.append("decision_timestamp_missing")
        created_at = datetime.now(timezone.utc).isoformat()
        timestamp = created_at
    elif not created_at:
        warnings.append("decision_created_at_missing")
        created_at = timestamp
    elif not timestamp:
        warnings.append("decision_timestamp_missing")
        timestamp = created_at
    elif str(created_at) != str(timestamp):
        warnings.append("decision_timestamp_created_at_mismatch")
        timestamp = created_at
    created_at_text = str(created_at)
    timestamp_text = str(timestamp)
    decision = {
        "decision": action,
        "action": action,
        "symbol": payload.get("symbol"),
        "trade_type": trade_type,
        "confidence": payload.get("confidence", 1.0 if action == "NO_TRADE" else 0.0),
        "reason": str(payload.get("reason") or f"file provider shorthand decision={action}"),
        "entry_price_hint": payload.get("entry_price_hint"),
        "stop_loss_pct": payload.get("stop_loss_pct"),
        "stop_loss_price": payload.get("stop_loss_price"),
        "take_profit_pct": payload.get("take_profit_pct"),
        "take_profit_price": payload.get("take_profit_price"),
        "max_holding_time_sec": payload.get("max_holding_time_sec"),
        "invalidation_condition": payload.get("invalidation_condition"),
        "reduce_only": bool(payload.get("reduce_only", False)),
        "created_at": created_at_text,
        "timestamp": timestamp_text,
        "source": str(payload.get("source") or "HERMES").upper(),
        **({"writer": str(payload.get("writer"))} if payload.get("writer") else {}),
        **({"trace_id": str(payload.get("trace_id"))} if payload.get("trace_id") else {}),
    }
    for key in (
        "market_regime",
        "candidate_direction",
        "allowed_direction",
        "direction_regime_allowed",
        "direction_regime_reason",
        "blocked_by",
        "direction_regime_source",
        "matrix_source",
        "candidate_symbol",
        "normalized_decision",
        "no_trade_reason",
        "entry_quality_filter",
        "entry_quality_version",
        "entry_quality_checked",
        "entry_quality_allowed",
        "entry_quality_score",
        "entry_quality_min_score",
        "entry_quality_reason",
        "entry_quality_reasons",
        "entry_quality_components",
    ):
        if key in payload:
            decision[key] = payload.get(key)
    if "candidate_symbol" not in decision and payload.get("symbol"):
        decision["candidate_symbol"] = payload.get("symbol")
    if "normalized_decision" not in decision:
        decision["normalized_decision"] = action
    if action == "NO_TRADE" and "no_trade_reason" not in decision:
        decision["no_trade_reason"] = decision["reason"]
    return decision


def _optional_action(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _append_action_mismatch(errors: list[str], left: str | None, right: str | None) -> None:
    if left and right and left != right:
        errors.append("decision_action_mismatch")


def _trace_id_from_snapshot(snapshot: dict[str, Any]) -> str:
    status = snapshot.get("system_status") if isinstance(snapshot, dict) else {}
    status = status if isinstance(status, dict) else {}
    return str(snapshot.get("trace_id") or status.get("trace_id") or "unknown_trace")


def fallback_result(
    provider: str,
    reason: str,
    *,
    raw_response: dict[str, Any] | None = None,
    raw_decision: dict[str, Any] | None = None,
    contract_warnings: list[str] | None = None,
    contract_errors: list[str] | None = None,
) -> HermesDecisionProviderResult:
    fallback_decision = no_trade_decision(reason, source="PHOENIX_FALLBACK")
    return HermesDecisionProviderResult(
        provider=provider,
        decision=fallback_decision,
        raw_response=raw_response,
        ok=False,
        fallback_reason=reason,
        created_at=datetime.now(timezone.utc).isoformat(),
        raw_decision=raw_decision,
        normalized_decision=None,
        fallback_decision=fallback_decision,
        contract_warnings=list(contract_warnings or []),
        contract_errors=list(contract_errors or []),
    )


def _decision_origin(provider: str, fallback_used: bool, fallback_reason: str | None) -> str:
    if fallback_used:
        if provider == "file" and str(fallback_reason or "").startswith("file_provider_timeout_waiting_for_decision"):
            return "timeout_fallback"
        if provider == "file":
            return "file_provider_fallback"
        return f"{provider}_fallback"
    if provider == "file":
        return "outbox_file"
    return provider


def _validated_or_fallback(
    provider: str,
    decision: dict[str, Any],
    *,
    raw_response: dict[str, Any] | None,
    raw_decision: dict[str, Any] | None = None,
    contract_warnings: list[str] | None = None,
    contract_errors: list[str] | None = None,
) -> HermesDecisionProviderResult:
    validation = validate_hermes_decision(decision)
    if not validation.valid:
        return fallback_result(
            provider,
            "hermes_decision_invalid:" + ",".join(validation.reasons),
            raw_response=raw_response,
            raw_decision=raw_decision,
            contract_warnings=contract_warnings,
            contract_errors=contract_errors,
        )
    normalized = {**decision, **(validation.decision or {})}
    return HermesDecisionProviderResult(
        provider=provider,
        decision=normalized,
        raw_response=raw_response,
        ok=True,
        fallback_reason=None,
        created_at=datetime.now(timezone.utc).isoformat(),
        raw_decision=raw_decision or decision,
        normalized_decision=normalized,
        fallback_decision=None,
        contract_warnings=list(contract_warnings or []),
        contract_errors=list(contract_errors or []),
    )
