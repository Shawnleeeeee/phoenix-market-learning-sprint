"""Configuration loader for the BTC engine."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .types import RiskConfig, StrategyConfig


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "btc_config"
DATA_DIR = PROJECT_ROOT / "btc_data"
STATE_DIR = DATA_DIR / "state"
REVIEWS_DIR = DATA_DIR / "reviews"
JOURNALS_DIR = DATA_DIR / "journals"
RESEARCH_DIR = DATA_DIR / "research"
RESEARCH_RUNTIME_DIR = RESEARCH_DIR / "runtime"
DATASETS_DIR = RESEARCH_DIR / "datasets"
BACKTESTS_DIR = RESEARCH_DIR / "backtests"
MODELS_DIR = RESEARCH_DIR / "models"
LOGS_DIR = PROJECT_ROOT / "logs"


def _strip_matching_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _parse_scalar(raw: str) -> Any:
    value = raw.strip()
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def load_simple_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    parsed: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in stripped:
            continue
        key, raw = stripped.split(":", 1)
        parsed[key.strip()] = _parse_scalar(raw)
    return parsed


def load_json_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_strategy_config(path: Path | None = None) -> StrategyConfig:
    values = load_simple_yaml(path or (CONFIG_DIR / "strategy.yaml"))
    return StrategyConfig(**{k: v for k, v in values.items() if hasattr(StrategyConfig, k)})


def load_risk_config(path: Path | None = None) -> RiskConfig:
    values = load_simple_yaml(path or (CONFIG_DIR / "risk.yaml"))
    return RiskConfig(**{k: v for k, v in values.items() if hasattr(RiskConfig, k)})


def ensure_runtime_dirs() -> None:
    for path in (
        DATA_DIR,
        STATE_DIR,
        REVIEWS_DIR,
        JOURNALS_DIR,
        RESEARCH_DIR,
        RESEARCH_RUNTIME_DIR,
        DATASETS_DIR,
        BACKTESTS_DIR,
        MODELS_DIR,
        LOGS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def environment_summary() -> dict[str, Any]:
    return {
        "account_api": get_account_api(),
        "symbol": get_symbol(),
        "proxy": get_env("BTC_ALL_PROXY", "") or os.environ.get("ALL_PROXY") or "",
        "execution_mode": get_execution_mode(),
    }


def get_env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value not in (None, ""):
        return value
    env_file = CONFIG_DIR / "live.env"
    if env_file.exists():
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, raw_value = line.split("=", 1)
            if key.strip() == name:
                stripped = _strip_matching_quotes(raw_value.strip())
                if stripped not in (None, ""):
                    return stripped
    return default


def get_execution_mode() -> str:
    return (get_env("BTC_EXECUTION_MODE", "paper") or "paper").strip().lower()


def get_symbol() -> str:
    return (get_env("BTC_SYMBOL", "BTCUSDT") or "BTCUSDT").strip().upper()


def get_account_api() -> str:
    return (get_env("BTC_ACCOUNT_API", "classic_um") or "classic_um").strip().lower()


def get_rest_base_url() -> str:
    return (get_env("BTC_FAPI_BASE_URL", "https://testnet.binancefuture.com") or "https://testnet.binancefuture.com").rstrip("/")


def get_ws_base_url() -> str:
    return (get_env("BTC_WS_BASE_URL", "wss://stream.binancefuture.com") or "wss://stream.binancefuture.com").rstrip("/")


def get_market_ws_base_url() -> str:
    explicit = (get_env("BTC_MARKET_WS_BASE_URL", "") or "").strip()
    if explicit:
        return explicit.rstrip("/")
    return get_ws_base_url()


def market_stream_enabled() -> bool:
    value = (get_env("BTC_MARKET_STREAM_ENABLED", "true") or "true").strip().lower()
    return value in {"1", "true", "yes", "on"}


def get_market_stream_freshness_sec() -> int:
    return int((get_env("BTC_MARKET_STREAM_FRESHNESS_SEC", "20") or "20").strip())


def get_api_key() -> str:
    return (get_env("BTC_BINANCE_API_KEY", "") or "").strip()


def get_api_secret() -> str:
    return (get_env("BTC_BINANCE_API_SECRET", "") or "").strip()


def get_hermes_review_target() -> str:
    return (get_env("BTC_HERMES_REVIEW_TARGET", "") or "").strip()


def get_hermes_demo_v2_target() -> str:
    explicit = (get_env("BTC_HERMES_DEMO_V2_TARGET", "") or "").strip()
    if explicit:
        return explicit
    review_target = get_hermes_review_target()
    if not review_target:
        return ""
    return review_target.replace("/btc_reviews", "/btc_demo_v2").replace("btc_reviews", "btc_demo_v2")


def get_hermes_control_target() -> str:
    explicit = (get_env("BTC_HERMES_CONTROL_TARGET", "") or "").strip()
    if explicit:
        return explicit
    demo_target = get_hermes_demo_v2_target()
    if not demo_target:
        return ""
    return demo_target.replace("/btc_demo_v2", "/btc_control").replace("btc_demo_v2", "btc_control")


def get_hermes_research_target() -> str:
    explicit = (get_env("BTC_HERMES_RESEARCH_TARGET", "") or "").strip()
    if explicit:
        return explicit
    review_target = get_hermes_review_target()
    if not review_target:
        return ""
    return review_target.replace("/btc_reviews", "/btc_research").replace("btc_reviews", "btc_research")


def get_review_journal_mode() -> str:
    return (get_env("BTC_REVIEW_JOURNAL_MODE", "") or "").strip().lower()


def get_journal_mode_for_execution() -> str:
    return get_execution_mode()


def get_paper_start_equity_usdt() -> float:
    return float((get_env("BTC_PAPER_START_EQUITY_USDT", "1000") or "1000").strip())


def get_paper_position_fraction() -> float:
    return float((get_env("BTC_PAPER_POSITION_FRACTION", "0.20") or "0.20").strip())


def get_paper_leverage() -> int:
    return int((get_env("BTC_PAPER_LEVERAGE", "1") or "1").strip())


def get_paper_max_open_positions() -> int:
    return int((get_env("BTC_PAPER_MAX_OPEN_POSITIONS", "1") or "1").strip())


def get_paper_candidate_config_path() -> Path:
    raw = (get_env("BTC_PAPER_CANDIDATE_CONFIG", "btc_config/candidate_strategy_v1_trend_only.json") or "").strip()
    candidate = Path(raw)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def get_demo_position_fraction() -> float:
    return float((get_env("BTC_DEMO_POSITION_FRACTION", "0.20") or "0.20").strip())


def get_demo_start_equity_usdt() -> float:
    return float((get_env("BTC_DEMO_START_EQUITY_USDT", "1000") or "1000").strip())


def get_demo_leverage() -> int:
    return int((get_env("BTC_DEMO_LEVERAGE", "1") or "1").strip())


def get_demo_candidate_config_path() -> Path:
    raw = (get_env("BTC_DEMO_CANDIDATE_CONFIG", "btc_config/candidate_strategy_v1_trend_only.json") or "").strip()
    candidate = Path(raw)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def get_runtime_candidate_config_path() -> Path:
    mode = get_execution_mode()
    if mode in {"demo_auto", "testnet_auto"}:
        return get_demo_candidate_config_path()
    return get_paper_candidate_config_path()


def load_runtime_candidate_config() -> dict[str, Any]:
    return load_json_config(get_runtime_candidate_config_path())


def get_runtime_engine_mode() -> str:
    explicit = (get_env("BTC_ENGINE_MODE", "") or "").strip().lower()
    if explicit:
        return explicit
    candidate = load_runtime_candidate_config()
    return str(candidate.get("engine_mode") or "").strip().lower() or "default"
