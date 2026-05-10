from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def decide(self, snapshot: dict[str, Any]) -> HermesDecisionProviderResult:
        del snapshot
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            return fallback_result("file", f"file_provider_error:{str(exc)[:160]}")
        if not isinstance(payload, dict):
            return fallback_result("file", "file_provider_payload_not_object", raw_response={"payload": payload})
        decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else payload
        return _validated_or_fallback("file", dict(decision), raw_response=payload)


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
        decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else payload
        if not isinstance(decision, dict):
            return fallback_result("http", "http_provider_decision_missing", raw_response=payload)
        return _validated_or_fallback("http", dict(decision), raw_response=payload)

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
) -> HermesDecisionProvider:
    normalized = str(name or "mock").strip().lower()
    if normalized == "mock":
        return MockHermesDecisionProvider()
    if normalized == "file":
        if decision_file is None:
            return MockHermesDecisionProvider(no_trade_decision("file provider missing --decision-file"))
        return FileHermesDecisionProvider(decision_file)
    if normalized == "http":
        return HttpHermesDecisionProvider(http_endpoint, timeout_sec=http_timeout_sec)
    return MockHermesDecisionProvider(no_trade_decision(f"unknown provider {normalized}; fallback NO_TRADE"))


def append_provider_log(path: str | Path, result: HermesDecisionProviderResult) -> None:
    append_jsonl(path, {"event": "hermes_decision_provider", **result.to_dict()})


def no_trade_decision(reason: str) -> dict[str, Any]:
    return {
        "action": "NO_TRADE",
        "symbol": None,
        "trade_type": "NONE",
        "confidence": 1.0,
        "reason": reason,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": "HERMES",
    }


def fallback_result(
    provider: str,
    reason: str,
    *,
    raw_response: dict[str, Any] | None = None,
) -> HermesDecisionProviderResult:
    return HermesDecisionProviderResult(
        provider=provider,
        decision=no_trade_decision(reason),
        raw_response=raw_response,
        ok=False,
        fallback_reason=reason,
        created_at=datetime.now(timezone.utc).isoformat(),
    )


def _validated_or_fallback(
    provider: str,
    decision: dict[str, Any],
    *,
    raw_response: dict[str, Any] | None,
) -> HermesDecisionProviderResult:
    validation = validate_hermes_decision(decision)
    if not validation.valid:
        return fallback_result(
            provider,
            "hermes_decision_invalid:" + ",".join(validation.reasons),
            raw_response=raw_response,
        )
    return HermesDecisionProviderResult(
        provider=provider,
        decision=validation.decision or decision,
        raw_response=raw_response,
        ok=True,
        fallback_reason=None,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
