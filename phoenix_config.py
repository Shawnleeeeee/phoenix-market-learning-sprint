from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Mapping

from phoenix_runtime_modes import (
    TESTNET_LIVE,
    assert_runtime_mode_safe,
    default_binance_env_for_mode,
    normalize_runtime_mode,
    permits_real_mainnet_orders,
)

_TRUE_VALUES = {"1", "true", "yes", "on"}
_SECRET_TOKENS = ("API_KEY", "API_SECRET", "SECRET", "TOKEN", "PASSWORD")
_REDACTED = "<redacted>"


@dataclass(frozen=True, slots=True)
class PhoenixConfig:
    runtime_mode: str = TESTNET_LIVE
    binance_env: str = "testnet"
    max_open_positions: int = 10
    margin_type: str = "ISOLATED"
    enable_mainnet_live: bool = False
    openai_api_key: str = ""
    openai_base_url: str = ""
    research_agent_model: str = "gpt-4.1-mini"
    research_agent_enabled: bool = False
    hmm_report_enabled: bool = True
    hmm_trading_gate_enabled: bool = False
    hmm_position_manager_enabled: bool = False
    learning_enabled: bool = False
    learning_state_dir: str = "btc_data/learning"
    testnet_order_validation_enabled: bool = True

    @property
    def mainnet_live_orders_enabled(self) -> bool:
        return permits_real_mainnet_orders(self.runtime_mode) and self.enable_mainnet_live

    def redacted_dict(self) -> dict[str, object]:
        return redact_secrets(asdict(self))


def _env_get(env: Mapping[str, str], name: str, default: str = "") -> str:
    value = env.get(name)
    if value is None or value == "":
        return default
    return value


def _env_bool(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in _TRUE_VALUES


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def load_phoenix_config(env: Mapping[str, str] | None = None) -> PhoenixConfig:
    source: Mapping[str, str] = os.environ if env is None else env

    runtime_mode = normalize_runtime_mode(_env_get(source, "PHOENIX_RUNTIME_MODE", TESTNET_LIVE))
    binance_env = _env_get(
        source,
        "PHOENIX_BINANCE_ENV",
        default_binance_env_for_mode(runtime_mode),
    ).strip().lower()
    enable_mainnet_live = _env_bool(source, "PHOENIX_ENABLE_MAINNET_LIVE", False)

    assert_runtime_mode_safe(
        runtime_mode,
        binance_env=binance_env,
        enable_mainnet_live=enable_mainnet_live,
    )

    return PhoenixConfig(
        runtime_mode=runtime_mode,
        binance_env=binance_env,
        max_open_positions=_env_int(source, "PHOENIX_MAX_OPEN_POSITIONS", 10),
        margin_type=_env_get(source, "PHOENIX_MARGIN_TYPE", "ISOLATED").strip().upper(),
        enable_mainnet_live=enable_mainnet_live,
        openai_api_key=_env_get(source, "OPENAI_API_KEY"),
        openai_base_url=_env_get(source, "OPENAI_BASE_URL"),
        research_agent_model=_env_get(source, "PHOENIX_RESEARCH_AGENT_MODEL", "gpt-4.1-mini"),
        research_agent_enabled=_env_bool(source, "PHOENIX_RESEARCH_AGENT_ENABLED", False),
        hmm_report_enabled=_env_bool(source, "PHOENIX_HMM_REPORT_ENABLED", True),
        hmm_trading_gate_enabled=_env_bool(source, "PHOENIX_HMM_TRADING_GATE_ENABLED", False),
        hmm_position_manager_enabled=_env_bool(source, "PHOENIX_HMM_POSITION_MANAGER_ENABLED", False),
        learning_enabled=_env_bool(source, "PHOENIX_LEARNING_ENABLED", False),
        learning_state_dir=_env_get(source, "PHOENIX_LEARNING_STATE_DIR", "btc_data/learning"),
        testnet_order_validation_enabled=_env_bool(source, "PHOENIX_TESTNET_ORDER_VALIDATION_ENABLED", True),
    )


def redact_secrets(values: Mapping[str, object]) -> dict[str, object]:
    redacted: dict[str, object] = {}
    for key, value in values.items():
        key_upper = key.upper()
        if any(token in key_upper for token in _SECRET_TOKENS):
            redacted[key] = _REDACTED if value not in (None, "") else value
        else:
            redacted[key] = value
    return redacted


def assert_mainnet_live_not_default(config: PhoenixConfig) -> None:
    if config.mainnet_live_orders_enabled:
        raise RuntimeError("Mainnet live orders must not be enabled by default.")
