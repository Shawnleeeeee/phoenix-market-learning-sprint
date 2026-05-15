from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class BinanceEnvironment:
    name: str
    futures_rest_base: str
    futures_ws_base: str
    portfolio_rest_base: str
    web3_base: str = "https://web3.binance.com"


BINANCE_ENVIRONMENTS: dict[str, BinanceEnvironment] = {
    "prod": BinanceEnvironment(
        name="prod",
        futures_rest_base="https://fapi.binance.com",
        futures_ws_base="wss://fstream.binance.com",
        portfolio_rest_base="https://papi.binance.com",
    ),
    "testnet": BinanceEnvironment(
        name="testnet",
        futures_rest_base="https://demo-fapi.binance.com",
        futures_ws_base="wss://fstream.binancefuture.com",
        portfolio_rest_base="https://testnet.binancefuture.com",
    ),
    "demo": BinanceEnvironment(
        name="demo",
        futures_rest_base="https://demo-fapi.binance.com",
        futures_ws_base="wss://fstream.binancefuture.com",
        portfolio_rest_base="https://testnet.binancefuture.com",
    ),
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_ENV_FILES = (PROJECT_ROOT / ".env", PROJECT_ROOT / ".env.local")
GLOBAL_ENV_FILES = tuple(
    Path(item)
    for item in (
        os.getenv("PHOENIX_GLOBAL_ENV_FILE"),
        "/etc/phoenix/phoenix.env",
    )
    if item
)
_LOCAL_ENV_CACHE: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class BinanceCredentials:
    api_key: str
    api_secret: str
    environment: BinanceEnvironment
    account_api_preference: str = "auto"
    recv_window_ms: int = 5000


@dataclass(frozen=True, slots=True)
class ProxySettings:
    http_proxy: str | None = None
    https_proxy: str | None = None
    ws_proxy: str | None = None
    wss_proxy: str | None = None
    all_proxy: str | None = None
    no_proxy: tuple[str, ...] = ()

    def proxy_for_url(self, url: str) -> str | None:
        parsed = urlparse(url)
        host = (parsed.hostname or "").strip().lower()
        if host and self._matches_no_proxy(host):
            return None

        scheme = parsed.scheme.lower()
        if scheme == "http":
            return self.http_proxy or self.all_proxy
        if scheme == "https":
            return self.https_proxy or self.all_proxy
        if scheme == "ws":
            return self.ws_proxy or self.http_proxy or self.all_proxy
        if scheme == "wss":
            return self.wss_proxy or self.https_proxy or self.all_proxy
        return self.all_proxy

    def _matches_no_proxy(self, host: str) -> bool:
        for entry in self.no_proxy:
            token = entry.strip().lower()
            if not token:
                continue
            if token == "*":
                return True
            if token.startswith("."):
                suffix = token[1:]
                if host == suffix or host.endswith(token):
                    return True
                continue
            if host == token or host.endswith(f".{token}"):
                return True
        return False


@dataclass(frozen=True, slots=True)
class ExecutionSettings:
    quote_allocation_usdt: float = 200.0
    allocation_mode: str = "FIXED"
    risk_budget_pct_of_balance: float = 3.0
    reserve_balance_usdt: float = 0.0
    execution_mode: str = "MANUAL_CONFIRM"
    allow_explicit_symbol_live: bool = False
    allow_auto_short: bool = False
    max_open_positions: int = 1
    strategy_min_score: float = 6.0
    reject_blocked_candidates: bool = True
    min_execution_quality_score: float = 5.0
    max_event_risk_score: float = 8.5
    max_estimated_slippage_bps: float = 25.0
    max_spread_bps: float = 20.0
    cooldown_minutes_after_close: int = 30
    significant_score_delta: float = 2.0
    significant_score_delta_pct: float = 0.2
    leverage: int = 5
    margin_type: str = "ISOLATED"
    working_type: str = "MARK_PRICE"
    initial_stop_loss_pct: float = 1.2
    take_profit_pct: float = 0.0
    breakeven_trigger_pct: float = 0.8
    breakeven_lock_pct: float = 0.1
    trailing_callback_pct: float = 0.5
    position_mode: str = "ONE_WAY"
    guardian_poll_interval_sec: float = 2.0
    guardian_max_runtime_sec: int = 43200


def _strip_matching_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_local_env() -> dict[str, str]:
    global _LOCAL_ENV_CACHE
    if _LOCAL_ENV_CACHE is not None:
        return _LOCAL_ENV_CACHE

    values: dict[str, str] = {}
    for env_file in LOCAL_ENV_FILES:
        if not env_file.exists():
            continue

        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            values[key] = _strip_matching_quotes(value.strip())

    _LOCAL_ENV_CACHE = values
    return values


def _load_global_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for env_file in GLOBAL_ENV_FILES:
        if not env_file.exists():
            continue

        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            values[key] = _strip_matching_quotes(value.strip())
    return values


def _get_env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value

    local_env = _load_local_env()
    for name in names:
        value = local_env.get(name)
        if value not in (None, ""):
            return value

    global_env = _load_global_env()
    for name in names:
        value = global_env.get(name)
        if value not in (None, ""):
            return value

    return default


def resolve_environment(name: str | None) -> BinanceEnvironment:
    normalized = (name or "prod").strip().lower()
    environment = BINANCE_ENVIRONMENTS.get(normalized)
    if environment is None:
        allowed = ", ".join(sorted(BINANCE_ENVIRONMENTS))
        raise ValueError(f"Unsupported Binance environment '{normalized}'. Expected one of: {allowed}")
    return environment


def load_credentials(required: bool = False) -> BinanceCredentials | None:
    api_key = _get_env("PHOENIX_BINANCE_API_KEY", "BINANCE_API_KEY")
    api_secret = _get_env("PHOENIX_BINANCE_API_SECRET", "BINANCE_SECRET_KEY")
    environment_name = _get_env("PHOENIX_BINANCE_ENV", "BINANCE_API_ENV", default="prod")
    account_api_preference = (_get_env("PHOENIX_BINANCE_ACCOUNT_API", default="auto") or "auto").strip().lower()
    recv_window = int(_get_env("PHOENIX_RECV_WINDOW_MS", default="5000") or "5000")

    if account_api_preference not in {"auto", "classic", "portfolio_margin"}:
        raise ValueError(
            "Unsupported PHOENIX_BINANCE_ACCOUNT_API value. Expected one of: auto, classic, portfolio_margin"
        )

    if not api_key or not api_secret:
        if required:
            raise RuntimeError(
                "Binance credentials are missing. Set PHOENIX_BINANCE_API_KEY and "
                "PHOENIX_BINANCE_API_SECRET before calling signed endpoints."
            )
        return None

    return BinanceCredentials(
        api_key=api_key,
        api_secret=api_secret,
        environment=resolve_environment(environment_name),
        account_api_preference=account_api_preference,
        recv_window_ms=recv_window,
    )


def load_execution_settings() -> ExecutionSettings:
    return ExecutionSettings(
        quote_allocation_usdt=float(_get_env("PHOENIX_QUOTE_ALLOCATION_USDT", default="200") or "200"),
        allocation_mode=(_get_env("PHOENIX_ALLOCATION_MODE", default="FIXED") or "FIXED").upper(),
        risk_budget_pct_of_balance=float(
            _get_env("PHOENIX_RISK_BUDGET_PCT_OF_BALANCE", default="3.0") or "3.0"
        ),
        reserve_balance_usdt=float(_get_env("PHOENIX_RESERVE_BALANCE_USDT", default="0") or "0"),
        execution_mode=(_get_env("PHOENIX_EXECUTION_MODE", default="MANUAL_CONFIRM") or "MANUAL_CONFIRM").upper(),
        allow_explicit_symbol_live=(
            (_get_env("PHOENIX_ALLOW_EXPLICIT_SYMBOL_LIVE", default="false") or "false").strip().lower()
            in {"1", "true", "yes", "on"}
        ),
        allow_auto_short=(
            (_get_env("PHOENIX_ALLOW_AUTO_SHORT", default="false") or "false").strip().lower()
            in {"1", "true", "yes", "on"}
        ),
        max_open_positions=int(_get_env("PHOENIX_MAX_OPEN_POSITIONS", default="1") or "1"),
        strategy_min_score=float(_get_env("PHOENIX_STRATEGY_MIN_SCORE", default="6.0") or "6.0"),
        reject_blocked_candidates=(
            (_get_env("PHOENIX_REJECT_BLOCKED_CANDIDATES", default="true") or "true").strip().lower()
            not in {"0", "false", "no", "off"}
        ),
        min_execution_quality_score=float(
            _get_env("PHOENIX_MIN_EXECUTION_QUALITY_SCORE", default="5.0") or "5.0"
        ),
        max_event_risk_score=float(_get_env("PHOENIX_MAX_EVENT_RISK_SCORE", default="8.5") or "8.5"),
        max_estimated_slippage_bps=float(
            _get_env("PHOENIX_MAX_ESTIMATED_SLIPPAGE_BPS", default="25.0") or "25.0"
        ),
        max_spread_bps=float(_get_env("PHOENIX_MAX_SPREAD_BPS", default="20.0") or "20.0"),
        cooldown_minutes_after_close=int(
            _get_env("PHOENIX_COOLDOWN_MINUTES_AFTER_CLOSE", default="30") or "30"
        ),
        significant_score_delta=float(_get_env("PHOENIX_SIGNIFICANT_SCORE_DELTA", default="2.0") or "2.0"),
        significant_score_delta_pct=float(
            _get_env("PHOENIX_SIGNIFICANT_SCORE_DELTA_PCT", default="0.2") or "0.2"
        ),
        leverage=int(_get_env("PHOENIX_LEVERAGE", default="5") or "5"),
        margin_type=(_get_env("PHOENIX_MARGIN_TYPE", default="ISOLATED") or "ISOLATED").upper(),
        working_type=(_get_env("PHOENIX_WORKING_TYPE", default="MARK_PRICE") or "MARK_PRICE").upper(),
        initial_stop_loss_pct=float(_get_env("PHOENIX_INITIAL_STOP_LOSS_PCT", default="1.2") or "1.2"),
        take_profit_pct=float(_get_env("PHOENIX_TAKE_PROFIT_PCT", default="0") or "0"),
        breakeven_trigger_pct=float(_get_env("PHOENIX_BREAKEVEN_TRIGGER_PCT", default="0.8") or "0.8"),
        breakeven_lock_pct=float(_get_env("PHOENIX_BREAKEVEN_LOCK_PCT", default="0.1") or "0.1"),
        trailing_callback_pct=float(_get_env("PHOENIX_TRAILING_CALLBACK_PCT", default="0.5") or "0.5"),
        position_mode=(_get_env("PHOENIX_POSITION_MODE", default="ONE_WAY") or "ONE_WAY").upper(),
        guardian_poll_interval_sec=float(_get_env("PHOENIX_GUARDIAN_POLL_INTERVAL_SEC", default="2") or "2"),
        guardian_max_runtime_sec=int(_get_env("PHOENIX_GUARDIAN_MAX_RUNTIME_SEC", default="43200") or "43200"),
    )


def load_proxy_settings() -> ProxySettings:
    no_proxy_raw = _get_env("PHOENIX_NO_PROXY", "NO_PROXY", "no_proxy", default="") or ""
    no_proxy = tuple(item.strip() for item in no_proxy_raw.split(",") if item.strip())
    return ProxySettings(
        http_proxy=_get_env("PHOENIX_HTTP_PROXY", "HTTP_PROXY", "http_proxy"),
        https_proxy=_get_env("PHOENIX_HTTPS_PROXY", "HTTPS_PROXY", "https_proxy"),
        ws_proxy=_get_env("PHOENIX_WS_PROXY", "WS_PROXY", "ws_proxy"),
        wss_proxy=_get_env("PHOENIX_WSS_PROXY", "WSS_PROXY", "wss_proxy"),
        all_proxy=_get_env("PHOENIX_ALL_PROXY", "ALL_PROXY", "all_proxy"),
        no_proxy=no_proxy,
    )
