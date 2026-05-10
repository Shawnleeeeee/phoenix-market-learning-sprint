from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from phoenix.config import _get_env
from phoenix_runtime_modes import TESTNET_LIVE, evaluate_runtime_mode, normalize_runtime_mode


def bool_env(value: Any, *, default: bool = False) -> bool:
    if value in (None, ""):
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def int_env(value: Any, *, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def float_env(value: Any, *, default: float) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True, slots=True)
class PhoenixRuntimeConfig:
    runtime_mode: str = TESTNET_LIVE
    env: str = "testnet"
    mainnet_live_enabled: bool = False

    def safety_decision(self):
        return evaluate_runtime_mode(
            self.runtime_mode,
            env=self.env,
            allow_mainnet_live=self.mainnet_live_enabled,
        )


@dataclass(frozen=True, slots=True)
class PhoenixTestnetRunnerConfig:
    max_open_positions: int = 10
    margin_type: str = "ISOLATED"
    max_concurrent_trades: int = 3
    min_dynamic_concurrency: int = 1
    max_api_error_rate: float = 0.15
    max_order_reject_rate: float = 0.10
    min_disk_free_pct: float = 5.0
    min_memory_available_pct: float = 10.0
    max_cpu_load_pct: float = 85.0
    flatten_on_round_end: bool = True

    def normalized_margin_type(self) -> str:
        margin_type = self.margin_type.strip().upper()
        if margin_type not in {"ISOLATED", "CROSSED"}:
            raise ValueError("Phoenix testnet margin_type must be ISOLATED or CROSSED.")
        return margin_type


@dataclass(frozen=True, slots=True)
class PhoenixAgentConfig:
    enabled: bool = False
    openai_api_key: str | None = None
    openai_base_url: str | None = None
    model: str = "gpt-5.4-mini"
    fallback_to_code_reports: bool = True
    read_only: bool = True
    allow_order_submission: bool = False
    allow_trading_settings_write: bool = False
    allow_live_enable: bool = False

    def redacted_summary(self) -> dict[str, Any]:
        data = asdict(self)
        data["openai_api_key"] = "***present***" if self.openai_api_key else None
        data["has_openai_api_key"] = bool(self.openai_api_key)
        return data


@dataclass(frozen=True, slots=True)
class PhoenixHMMConfig:
    report_enabled: bool = True
    trading_gate_enabled: bool = False
    position_manager_enabled: bool = False
    min_samples: int = 30


@dataclass(frozen=True, slots=True)
class PhoenixLearningConfig:
    enabled: bool = True
    store_path: str = "learning_store.jsonl"
    strategy_proposals_dir: str = "strategy_proposals"


@dataclass(frozen=True, slots=True)
class PhoenixConfig:
    runtime: PhoenixRuntimeConfig
    testnet_runner: PhoenixTestnetRunnerConfig
    agent: PhoenixAgentConfig
    hmm: PhoenixHMMConfig
    learning: PhoenixLearningConfig

    def redacted_summary(self) -> dict[str, Any]:
        return {
            "runtime": asdict(self.runtime),
            "testnet_runner": asdict(self.testnet_runner),
            "agent": self.agent.redacted_summary(),
            "hmm": asdict(self.hmm),
            "learning": asdict(self.learning),
        }


def load_phoenix_config() -> PhoenixConfig:
    runtime = PhoenixRuntimeConfig(
        runtime_mode=normalize_runtime_mode(_get_env("PHOENIX_RUNTIME_MODE", default=TESTNET_LIVE)),
        env=(_get_env("PHOENIX_RUNTIME_ENV", "PHOENIX_BINANCE_ENV", default="testnet") or "testnet").strip().lower(),
        mainnet_live_enabled=bool_env(_get_env("PHOENIX_MAINNET_LIVE_ENABLED", default="false")),
    )
    testnet = PhoenixTestnetRunnerConfig(
        max_open_positions=max(1, int_env(_get_env("PHOENIX_TESTNET_MAX_OPEN_POSITIONS", "PHOENIX_MAX_OPEN_POSITIONS", default="10"), default=10)),
        margin_type=(_get_env("PHOENIX_TESTNET_MARGIN_TYPE", "PHOENIX_MARGIN_TYPE", default="ISOLATED") or "ISOLATED").upper(),
        max_concurrent_trades=max(1, int_env(_get_env("PHOENIX_TESTNET_MAX_CONCURRENT_TRADES", default="3"), default=3)),
        min_dynamic_concurrency=max(1, int_env(_get_env("PHOENIX_TESTNET_MIN_DYNAMIC_CONCURRENCY", default="1"), default=1)),
        max_api_error_rate=float_env(_get_env("PHOENIX_TESTNET_MAX_API_ERROR_RATE", default="0.15"), default=0.15),
        max_order_reject_rate=float_env(_get_env("PHOENIX_TESTNET_MAX_ORDER_REJECT_RATE", default="0.10"), default=0.10),
        min_disk_free_pct=float_env(_get_env("PHOENIX_TESTNET_MIN_DISK_FREE_PCT", default="5.0"), default=5.0),
        min_memory_available_pct=float_env(_get_env("PHOENIX_TESTNET_MIN_MEMORY_AVAILABLE_PCT", default="10.0"), default=10.0),
        max_cpu_load_pct=float_env(_get_env("PHOENIX_TESTNET_MAX_CPU_LOAD_PCT", default="85.0"), default=85.0),
        flatten_on_round_end=bool_env(_get_env("PHOENIX_TESTNET_FLATTEN_ON_ROUND_END", default="true"), default=True),
    )
    agent = PhoenixAgentConfig(
        enabled=bool_env(_get_env("PHOENIX_RESEARCH_AGENT_ENABLED", default="false")),
        openai_api_key=_get_env("OPENAI_API_KEY"),
        openai_base_url=_get_env("OPENAI_BASE_URL"),
        model=_get_env("PHOENIX_RESEARCH_AGENT_MODEL", default="gpt-5.4-mini") or "gpt-5.4-mini",
        fallback_to_code_reports=bool_env(_get_env("PHOENIX_RESEARCH_AGENT_FALLBACK_TO_CODE_REPORTS", default="true"), default=True),
    )
    hmm = PhoenixHMMConfig(
        report_enabled=bool_env(_get_env("PHOENIX_HMM_REPORT_ENABLED", default="true"), default=True),
        trading_gate_enabled=bool_env(_get_env("PHOENIX_HMM_TRADING_GATE_ENABLED", default="false")),
        position_manager_enabled=bool_env(_get_env("PHOENIX_HMM_POSITION_MANAGER_ENABLED", default="false")),
        min_samples=max(1, int_env(_get_env("PHOENIX_HMM_MIN_SAMPLES", default="30"), default=30)),
    )
    learning = PhoenixLearningConfig(
        enabled=bool_env(_get_env("PHOENIX_LEARNING_STORE_ENABLED", default="true"), default=True),
        store_path=_get_env("PHOENIX_LEARNING_STORE_PATH", default="learning_store.jsonl") or "learning_store.jsonl",
        strategy_proposals_dir=_get_env("PHOENIX_STRATEGY_PROPOSALS_DIR", default="strategy_proposals") or "strategy_proposals",
    )
    testnet.normalized_margin_type()
    runtime.safety_decision()
    return PhoenixConfig(
        runtime=runtime,
        testnet_runner=testnet,
        agent=agent,
        hmm=hmm,
        learning=learning,
    )
