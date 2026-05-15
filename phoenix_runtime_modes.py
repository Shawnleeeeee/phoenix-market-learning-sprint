from __future__ import annotations

from dataclasses import dataclass
from typing import Any

MAINNET_SHADOW = "MAINNET_SHADOW"
TESTNET_LIVE = "TESTNET_LIVE"
MAINNET_LIVE = "MAINNET_LIVE"

RUNTIME_MODES = (MAINNET_SHADOW, TESTNET_LIVE, MAINNET_LIVE)

_ALIASES = {
    "MAINNET_SHADOW": MAINNET_SHADOW,
    "SHADOW": MAINNET_SHADOW,
    "TESTNET_LIVE": TESTNET_LIVE,
    "TESTNET": TESTNET_LIVE,
    "MAINNET_LIVE": MAINNET_LIVE,
    "MAINNET": MAINNET_LIVE,
}

_MAINNET_ENVS = {"prod", "mainnet"}
_TESTNET_ENVS = {"testnet", "demo"}


class RuntimeModeSafetyError(ValueError):
    """Raised when runtime mode and environment settings are unsafe."""


@dataclass(frozen=True, slots=True)
class RuntimeModeDecision:
    runtime_mode: str
    env: str
    live_trading_enabled: bool
    paper_record_only: bool
    live_order_submission_blocked: bool
    promotion_allowed: bool
    order_endpoint_allowed: bool
    reason: str


def normalize_runtime_mode(raw_value: str | None) -> str:
    value = str(raw_value or TESTNET_LIVE).strip().upper()
    mode = _ALIASES.get(value)
    if mode is None:
        allowed = ", ".join(RUNTIME_MODES)
        raise ValueError(f"Unsupported Phoenix runtime mode '{raw_value}'. Expected one of: {allowed}")
    return mode


def evaluate_runtime_mode(
    runtime_mode: str | None,
    *,
    env: str | None = None,
    binance_env: str | None = None,
    enable_mainnet_live: bool = False,
) -> RuntimeModeDecision:
    resolved_env = str(env or binance_env or default_binance_env_for_mode(runtime_mode)).strip().lower()
    mode = assert_runtime_mode_safe(
        runtime_mode,
        binance_env=resolved_env,
        enable_mainnet_live=enable_mainnet_live,
    )
    if mode == MAINNET_SHADOW:
        return RuntimeModeDecision(
            runtime_mode=mode,
            env=resolved_env,
            live_trading_enabled=False,
            paper_record_only=True,
            live_order_submission_blocked=True,
            promotion_allowed=False,
            order_endpoint_allowed=False,
            reason="mainnet_shadow_paper_only",
        )
    if mode == TESTNET_LIVE:
        return RuntimeModeDecision(
            runtime_mode=mode,
            env=resolved_env,
            live_trading_enabled=True,
            paper_record_only=False,
            live_order_submission_blocked=False,
            promotion_allowed=False,
            order_endpoint_allowed=True,
            reason="futures_testnet_execution_only",
        )
    return RuntimeModeDecision(
        runtime_mode=mode,
        env=resolved_env,
        live_trading_enabled=False,
        paper_record_only=False,
        live_order_submission_blocked=True,
        promotion_allowed=False,
        order_endpoint_allowed=False,
        reason="mainnet_live_blocked_for_order_endpoint",
    )


def assert_order_endpoint_allowed(
    *,
    runtime_mode: str | None,
    env: str | None = None,
    binance_env: str | None = None,
    endpoint: str | None = None,
) -> RuntimeModeDecision:
    decision = evaluate_runtime_mode(runtime_mode, env=env, binance_env=binance_env)
    if not decision.order_endpoint_allowed:
        suffix = f" endpoint={endpoint}" if endpoint else ""
        raise RuntimeError(f"Order submission blocked for {decision.runtime_mode}:{suffix} {decision.reason}")
    return decision


def safety_flags(runtime_mode: str | None, *, env: str | None = None, binance_env: str | None = None) -> dict[str, Any]:
    decision = evaluate_runtime_mode(runtime_mode, env=env, binance_env=binance_env)
    return {
        "execution_mode": decision.runtime_mode,
        "env": decision.env,
        "live_trading_enabled": decision.live_trading_enabled,
        "paper_record_only": decision.paper_record_only,
        "live_order_submission_blocked": decision.live_order_submission_blocked,
        "promotion_allowed": decision.promotion_allowed,
        "order_endpoint_allowed": decision.order_endpoint_allowed,
        "safety_reason": decision.reason,
    }


def default_binance_env_for_mode(runtime_mode: str | None) -> str:
    mode = normalize_runtime_mode(runtime_mode)
    if mode == TESTNET_LIVE:
        return "testnet"
    return "prod"


def permits_real_mainnet_orders(runtime_mode: str | None) -> bool:
    return normalize_runtime_mode(runtime_mode) == MAINNET_LIVE


def assert_runtime_mode_safe(
    runtime_mode: str | None,
    *,
    binance_env: str | None,
    enable_mainnet_live: bool = False,
) -> str:
    mode = normalize_runtime_mode(runtime_mode)
    env = str(binance_env or default_binance_env_for_mode(mode)).strip().lower()

    if mode == TESTNET_LIVE and env not in _TESTNET_ENVS:
        raise RuntimeModeSafetyError("TESTNET_LIVE must run against a testnet/demo Binance environment.")

    if mode == MAINNET_SHADOW and env not in _MAINNET_ENVS:
        raise RuntimeModeSafetyError("MAINNET_SHADOW must read the mainnet/prod environment.")

    if mode == MAINNET_LIVE:
        if env not in _MAINNET_ENVS:
            raise RuntimeModeSafetyError("MAINNET_LIVE must run against a mainnet/prod Binance environment.")
        if not enable_mainnet_live:
            raise RuntimeModeSafetyError(
                "MAINNET_LIVE requires PHOENIX_ENABLE_MAINNET_LIVE=true and is never enabled by default."
            )

    return mode
