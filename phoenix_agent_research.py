from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


AGENT_RESEARCH_VERSION = "v1.0"

DEFAULT_AGENT_CONFIG = {
    "agent_name": "phoenix_research_agent",
    "agent_research_version": AGENT_RESEARCH_VERSION,
    "enabled": False,
    "read_only": True,
    "fallback_to_code_reports": True,
    "live_trading_enabled": False,
    "order_writes_allowed": False,
    "trading_setting_writes_allowed": False,
    "config_writes_allowed": False,
    "direct_config_change_allowed": False,
    "allowed_outputs": ["code_report", "research_note", "strategy_suggestion"],
}

SECRET_KEY_TOKENS = ("api_key", "apikey", "secret", "token", "password")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def redact_secret(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    if len(text) <= 8:
        return "***REDACTED***"
    return f"{text[:4]}...REDACTED...{text[-4:]}"


def redact_api_keys(payload: Any) -> Any:
    if isinstance(payload, dict):
        redacted: dict[str, Any] = {}
        for key, value in payload.items():
            if any(token in str(key).lower() for token in SECRET_KEY_TOKENS):
                redacted[key] = redact_secret(value)
            else:
                redacted[key] = redact_api_keys(value)
        return redacted
    if isinstance(payload, list):
        return [redact_api_keys(item) for item in payload]
    return payload


def build_agent_research_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    config = dict(DEFAULT_AGENT_CONFIG)
    if overrides:
        config.update(redact_api_keys(overrides))
    config["enabled"] = False
    config["read_only"] = True
    config["fallback_to_code_reports"] = True
    config["live_trading_enabled"] = False
    config["order_writes_allowed"] = False
    config["trading_setting_writes_allowed"] = False
    config["config_writes_allowed"] = False
    config["direct_config_change_allowed"] = False
    config["generated_at"] = utc_now_iso()
    return config


def assert_research_agent_read_only(config: dict[str, Any]) -> bool:
    forbidden_flags = (
        "live_trading_enabled",
        "order_writes_allowed",
        "trading_setting_writes_allowed",
        "config_writes_allowed",
        "direct_config_change_allowed",
    )
    return (
        config.get("enabled") is False
        and config.get("read_only") is True
        and config.get("fallback_to_code_reports") is True
        and all(config.get(flag) is False for flag in forbidden_flags)
    )
