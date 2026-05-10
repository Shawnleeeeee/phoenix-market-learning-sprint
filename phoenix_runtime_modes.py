from __future__ import annotations

from dataclasses import dataclass
from typing import Any


MAINNET_SHADOW = "MAINNET_SHADOW"
TESTNET_LIVE = "TESTNET_LIVE"
MAINNET_LIVE = "MAINNET_LIVE"

RUNTIME_MODES = frozenset({MAINNET_SHADOW, TESTNET_LIVE, MAINNET_LIVE})
TESTNET_ENVS = frozenset({"testnet", "demo"})


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


def normalize_runtime_mode(value: Any) -> str:
    text = str(value or TESTNET_LIVE).strip().upper()
    aliases = {
        "SHADOW": MAINNET_SHADOW,
        "PAPER": MAINNET_SHADOW,
        "MAINNET_PAPER": MAINNET_SHADOW,
        "TESTNET": TESTNET_LIVE,
        "TESTNET_EXECUTION": TESTNET_LIVE,
        "LIVE": MAINNET_LIVE,
        "PROD_LIVE": MAINNET_LIVE,
    }
    normalized = aliases.get(text, text)
    if normalized not in RUNTIME_MODES:
        allowed = ", ".join(sorted(RUNTIME_MODES))
        raise ValueError(f"Unsupported Phoenix runtime mode '{text}'. Expected one of: {allowed}")
    return normalized


def normalize_env(value: Any) -> str:
    return str(value or "testnet").strip().lower()


def evaluate_runtime_mode(
    runtime_mode: Any,
    *,
    env: Any,
    allow_mainnet_live: bool = False,
) -> RuntimeModeDecision:
    mode = normalize_runtime_mode(runtime_mode)
    normalized_env = normalize_env(env)

    if mode == MAINNET_SHADOW:
        if normalized_env != "prod":
            raise ValueError("MAINNET_SHADOW must use env=prod so real-market shadow data is not mixed with testnet.")
        return RuntimeModeDecision(
            runtime_mode=mode,
            env=normalized_env,
            live_trading_enabled=False,
            paper_record_only=True,
            live_order_submission_blocked=True,
            promotion_allowed=False,
            order_endpoint_allowed=False,
            reason="mainnet_shadow_paper_only",
        )

    if mode == TESTNET_LIVE:
        if normalized_env not in TESTNET_ENVS:
            raise ValueError("TESTNET_LIVE may only use env=testnet or env=demo.")
        return RuntimeModeDecision(
            runtime_mode=mode,
            env=normalized_env,
            live_trading_enabled=True,
            paper_record_only=False,
            live_order_submission_blocked=False,
            promotion_allowed=False,
            order_endpoint_allowed=True,
            reason="futures_testnet_execution_only",
        )

    if not allow_mainnet_live:
        raise ValueError("MAINNET_LIVE is disabled by Phoenix safety policy.")

    return RuntimeModeDecision(
        runtime_mode=mode,
        env=normalized_env,
        live_trading_enabled=False,
        paper_record_only=False,
        live_order_submission_blocked=True,
        promotion_allowed=False,
        order_endpoint_allowed=False,
        reason="mainnet_live_explicitly_blocked",
    )


def assert_order_endpoint_allowed(runtime_mode: Any, *, env: Any) -> RuntimeModeDecision:
    decision = evaluate_runtime_mode(runtime_mode, env=env)
    if not decision.order_endpoint_allowed:
        raise RuntimeError(f"Order endpoint is blocked for {decision.runtime_mode}: {decision.reason}")
    return decision


def safety_flags(runtime_mode: Any, *, env: Any) -> dict[str, Any]:
    decision = evaluate_runtime_mode(runtime_mode, env=env)
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
