#!/usr/bin/env python3
"""Read-only Phoenix dashboard snapshot API.

This service intentionally reads Phoenix's on-disk reports and shadow state only.
It does not import or call execution/order code.
"""

from __future__ import annotations

import argparse
import hmac
import json
import os
import platform
import re
import socket
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


DEFAULT_ENDPOINT = "/api/phoenix-dashboard-snapshot"
DEFAULT_PORT = 18765
DEFAULT_CACHE_TTL_SEC = 5.0
DEFAULT_MAX_JSONL_ROWS = 25
DEFAULT_TESTNET_ENV_FILE = Path("/etc/phoenix/phoenix-testnet.env")
TESTNET_FUTURES_REST_BASE = "https://demo-fapi.binance.com"
SERVICE_STARTED_AT = datetime.now(timezone.utc).isoformat()

SENSITIVE_KEY_PARTS = (
    "apikey",
    "apisecret",
    "accesskey",
    "accesstoken",
    "authtoken",
    "bearertoken",
    "clientsecret",
    "privatekey",
    "secretkey",
    "authorization",
    "password",
    "passwordhash",
    "passwd",
    "passphrase",
    "secret",
    "token",
)
SECRET_TEXT_PATTERNS = [
    re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[A-Za-z0-9._~+/=-]+"),
    re.compile(
        r"(?i)((?:--?|/)(?:api[-_]?key|api[-_]?secret|access[-_]?token|"
        r"auth[-_]?token|client[-_]?secret|private[-_]?key|secret[-_]?key|"
        r"secret|password|passwd|passphrase|token)(?:\s+|=))[^\s,;\"']+"
    ),
    re.compile(
        r"(?i)((?:api[_-]?key|secret|password|passwd|passphrase|token)"
        r"\s*[:=]\s*[\"']?)[^\"'\s,;]+"
    ),
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def path_mtime_iso(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
    except OSError:
        return None


def redact_text(value: str, max_len: int = 4000) -> str:
    redacted = value
    for pattern in SECRET_TEXT_PATTERNS:
        redacted = pattern.sub(r"\1[REDACTED]", redacted)
    if len(redacted) > max_len:
        return redacted[:max_len] + "...[truncated]"
    return redacted


def is_secret_key(key: str) -> bool:
    compact = re.sub(r"[^a-z0-9]", "", key.lower())
    return any(part in compact for part in SENSITIVE_KEY_PARTS)


def sanitize(value: Any, *, depth: int = 0, max_depth: int = 8) -> Any:
    if depth > max_depth:
        return "[max_depth]"
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            text_key = str(key)
            if is_secret_key(text_key):
                out[text_key] = "[REDACTED]"
            else:
                out[text_key] = sanitize(item, depth=depth + 1, max_depth=max_depth)
        return out
    if isinstance(value, list):
        return [sanitize(item, depth=depth + 1, max_depth=max_depth) for item in value[:500]]
    if isinstance(value, tuple):
        return [sanitize(item, depth=depth + 1, max_depth=max_depth) for item in value[:500]]
    if isinstance(value, str):
        return redact_text(value)
    return value


def read_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def read_jsonl_tail(path: Path, limit: int = DEFAULT_MAX_JSONL_ROWS) -> list[dict[str, Any]]:
    rows: deque[dict[str, Any]] = deque(maxlen=max(0, limit))
    if limit <= 0:
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except OSError:
        return []
    return [sanitize(row) for row in rows]


def read_text_tail(path: Path, limit: int = 40) -> list[str]:
    lines: deque[str] = deque(maxlen=max(0, limit))
    if limit <= 0:
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                lines.append(redact_text(line.rstrip("\n\r"), max_len=2000))
    except OSError:
        return []
    return list(lines)


def count_lines(path: Path) -> int | None:
    try:
        total = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                total += chunk.count(b"\n")
        return total
    except OSError:
        return None


def file_info(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
    except OSError:
        return {"path": str(path), "exists": False}
    return {
        "path": str(path),
        "exists": True,
        "sizeBytes": stat.st_size,
        "updatedAt": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }


def parse_env_key_presence(path: Path) -> set[str]:
    keys: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[7:].lstrip()
                key = line.split("=", 1)[0].strip()
                if key:
                    keys.add(key)
    except OSError:
        pass
    return keys


def parse_env_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[7:].lstrip()
                key, value = line.split("=", 1)
                key = key.strip()
                if not key:
                    continue
                values[key] = value.strip().strip("\"'")
    except OSError:
        pass
    return values


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def decimal_str(value: Any) -> str:
    if value in (None, ""):
        return "0"
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return "0"


def signed_get_json(
    *,
    base_url: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: dict[str, Any] | None = None,
    timeout_sec: float = 6.0,
) -> Any:
    query_params = dict(params or {})
    query_params["timestamp"] = int(time.time() * 1000)
    query_params["recvWindow"] = 5000
    query = urlencode(query_params)
    signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), sha256).hexdigest()
    request = Request(
        f"{base_url.rstrip('/')}{path}?{query}&signature={signature}",
        headers={"X-MBX-APIKEY": api_key},
        method="GET",
    )
    with urlopen(request, timeout=timeout_sec) as response:  # noqa: S310 - fixed Binance testnet URL.
        return json.loads(response.read().decode("utf-8"))


def build_probe_error(message: str, *, error_type: str = "testnet_snapshot_unavailable") -> dict[str, Any]:
    return {
        "type": error_type,
        "message": redact_text(message, max_len=500),
        "command": None,
        "exitCode": None,
        "stdoutPreview": None,
        "stderrPreview": None,
    }


def env_audit_snapshot(name: str, env_files: list[Path]) -> dict[str, Any]:
    keys: set[str] = set()
    for path in env_files:
        keys.update(parse_env_key_presence(path))
    lowered = {key.lower(): key for key in keys}
    api_keys = [key for key in keys if "api" in key.lower() and "key" in key.lower()]
    secret_keys = [key for key in keys if "secret" in key.lower()]
    password_keys = [key for key in keys if "password" in key.lower() or "passphrase" in key.lower()]
    return {
        "source": "sanitized_env_key_presence",
        "name": name,
        "rawEnvReturned": False,
        "sensitiveValuesReturned": False,
        "files": [file_info(path) for path in env_files],
        "configuredKeyCount": len(keys),
        "apiKeyConfigured": bool(api_keys),
        "apiSecretConfigured": bool(secret_keys),
        "passwordOrPassphraseConfigured": bool(password_keys),
        "knownModeFlags": sorted(
            key
            for key in keys
            if key.lower()
            in {
                "binance_env",
                "binance_testnet",
                "execution_mode",
                "phoenix_execution_mode",
                "phoenix_binance_env",
                "phoenix_runtime_mode",
                "phoenix_live_trading_enabled",
                "phoenix_mainnet_live_enabled",
                "phoenix_promotion_allowed",
            }
        ),
        "safeConfiguredKeys": sorted(
            key
            for key in keys
            if key.lower()
            in {
                "binance_env",
                "execution_mode",
                "phoenix_execution_mode",
                "phoenix_binance_env",
                "phoenix_runtime_mode",
                "phoenix_live_trading_enabled",
                "phoenix_mainnet_live_enabled",
                "phoenix_promotion_allowed",
                "phoenix_research_agent_enabled",
                "phoenix_hmm_trading_gate_enabled",
                "phoenix_hmm_position_manager_enabled",
                "phoenix_max_open_positions",
                "phoenix_margin_type",
            }
        ),
        "hasMainnetHint": any("mainnet" in key for key in lowered),
        "hasTestnetHint": any("testnet" in key for key in lowered),
    }


def newest_file(paths: list[Path]) -> Path | None:
    existing: list[Path] = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda item: item.stat().st_mtime)


def find_shadow_root(project_root: Path, explicit_shadow_root: Path | None = None) -> Path:
    if explicit_shadow_root is not None:
        return explicit_shadow_root
    candidates = list((project_root / "signal_lab_runs").glob("vps_forward_shadow_*/mainnet_shadow"))
    if not candidates:
        return project_root / "signal_lab_runs" / "mainnet_shadow"

    def sort_key(path: Path) -> tuple[int, float]:
        text = str(path)
        priority = 0
        if "mainnet_active" in text or "active" in text:
            priority = 4
        elif "candidate_validation" in text:
            priority = 3
        elif "baseline_clean" in text:
            priority = 2
        elif "baseline_validation" in text:
            priority = 1
        mtimes: list[float] = []
        for candidate_path in (
            path,
            path / "signal_bridge_shadow_signals.jsonl",
            path / "signal_bridge_shadow_outcomes.jsonl",
            path / "mainnet_shadow_readiness.json",
            path / "Total_Sample_V9_Final.json",
        ):
            try:
                mtimes.append(candidate_path.stat().st_mtime)
            except OSError:
                pass
        mtime = max(mtimes) if mtimes else 0.0
        return (priority, mtime)

    return max(candidates, key=sort_key)


def summarize_pending_order(row: dict[str, Any]) -> dict[str, Any]:
    trigger_features = row.get("trigger_features") if isinstance(row.get("trigger_features"), dict) else {}
    return sanitize(
        {
            "eventId": row.get("event_id"),
            "sourceEventId": row.get("source_event_id"),
            "strategyFamily": row.get("strategy_family"),
            "strategyId": row.get("strategy_id"),
            "entryProfile": row.get("entry_profile"),
            "exitProfile": row.get("exit_profile"),
            "status": row.get("status"),
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "signalTime": row.get("signal_time"),
            "simulatedEntryTime": row.get("simulated_entry_time"),
            "simulatedEntryPrice": row.get("simulated_entry_price") or row.get("entry_price"),
            "triggerPrice": row.get("trigger_price"),
            "oiConfirmationSource": trigger_features.get("oi_confirmation_source")
            or row.get("oi_confirmation_source"),
            "oi5DirectionBucket": trigger_features.get("oi5_direction_bucket")
            or row.get("oi5_direction_bucket"),
            "liveTradingEnabled": row.get("live_trading_enabled", False),
            "liveOrderSubmissionBlocked": row.get("live_order_submission_blocked", True),
        }
    )


def summarize_outcome(row: dict[str, Any]) -> dict[str, Any]:
    return sanitize(
        {
            "eventId": row.get("event_id"),
            "sourceEventId": row.get("source_event_id"),
            "strategyFamily": row.get("strategy_family"),
            "strategyId": row.get("strategy_id"),
            "entryProfile": row.get("entry_profile"),
            "exitProfile": row.get("exit_profile"),
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "recordedAt": row.get("recorded_at") or row.get("final_exit_time"),
            "exitReason": row.get("exit_reason") or row.get("final_exit_reason"),
            "afterFeeReturnPct": row.get("after_fee_return_pct"),
            "afterRealCostReturnPct": row.get("after_real_cost_return_pct")
            or row.get("after_fee_and_slippage_return_pct"),
            "holdingDurationSec": row.get("holding_duration_sec"),
            "holdingMinutes": row.get("holding_minutes"),
            "shadowBranchId": row.get("shadow_branch_id"),
            "liveUnlockEligible": row.get("live_unlock_eligible", False),
        }
    )


def load_token_from_file(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "PHOENIX_DASHBOARD_READONLY_TOKEN":
            return value.strip().strip("\"'")
    return text.strip().strip("\"'")


def load_bearer_token(token_file: Path | None = None) -> str:
    token = os.environ.get("PHOENIX_DASHBOARD_READONLY_TOKEN", "").strip()
    if not token and token_file is not None:
        token = load_token_from_file(token_file)
    if len(token) < 32:
        raise SystemExit("PHOENIX_DASHBOARD_READONLY_TOKEN must be at least 32 characters")
    return token


class DashboardSnapshotService:
    def __init__(
        self,
        project_root: Path,
        shadow_root: Path | None = None,
        *,
        host: str = "127.0.0.1",
        port: int = DEFAULT_PORT,
        cache_ttl_sec: float = DEFAULT_CACHE_TTL_SEC,
        testnet_env_file: Path = DEFAULT_TESTNET_ENV_FILE,
        signed_get: Any = signed_get_json,
    ) -> None:
        self.project_root = project_root.resolve()
        self.shadow_root = find_shadow_root(self.project_root, shadow_root.resolve() if shadow_root else None)
        self.host = host
        self.port = port
        self.cache_ttl_sec = max(0.0, cache_ttl_sec)
        self.testnet_env_file = testnet_env_file
        self._signed_get = signed_get
        self._cache_until = 0.0
        self._cache: dict[str, Any] | None = None

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        if self._cache is not None and now < self._cache_until:
            return self._cache
        payload = self._build_snapshot()
        self._cache = payload
        self._cache_until = now + self.cache_ttl_sec
        return payload

    def _build_snapshot(self) -> dict[str, Any]:
        shadow_root = self.shadow_root
        run_root = shadow_root.parent if shadow_root.name == "mainnet_shadow" else shadow_root
        state_path = run_root / "bridge-state.shadow.json"
        bridge_state_path = run_root / "bridge-state.json"
        readiness_path = shadow_root / "mainnet_shadow_readiness.json"
        total_sample_path = shadow_root / "Total_Sample_V9_Final.json"
        league_report_path = shadow_root / "strategy_shadow_league_report.json"
        signals_path = shadow_root / "signal_bridge_shadow_signals.jsonl"
        outcomes_path = shadow_root / "signal_bridge_shadow_outcomes.jsonl"

        readiness = read_json(readiness_path) if readiness_path.exists() else {}
        total_sample = read_json(total_sample_path) if total_sample_path.exists() else {}
        league_report = read_json(league_report_path) if league_report_path.exists() else {}
        state = read_json(state_path) if state_path.exists() else {}
        bridge_state = read_json(bridge_state_path) if bridge_state_path.exists() else {}

        account = total_sample.get("real_account") if isinstance(total_sample, dict) else {}
        account = account if isinstance(account, dict) else {}
        pending_by_event_id = state.get("pending_by_event_id") if isinstance(state, dict) else {}
        pending_by_event_id = pending_by_event_id if isinstance(pending_by_event_id, dict) else {}
        shadow_signal_count = count_lines(signals_path)
        shadow_outcome_count = count_lines(outcomes_path)
        pending_orders = [
            summarize_pending_order(row)
            for row in sorted(
                pending_by_event_id.values(),
                key=lambda item: str(item.get("signal_time") or item.get("simulated_entry_time") or ""),
                reverse=True,
            )
            if isinstance(row, dict)
        ][:50]
        recent_outcomes = [summarize_outcome(row) for row in read_jsonl_tail(outcomes_path, 25)]

        shadow_all = total_sample.get("shadow_all") if isinstance(total_sample, dict) else {}
        shadow_all = shadow_all if isinstance(shadow_all, dict) else {}
        live_enabled = bool(readiness.get("live_trading_enabled", False)) if isinstance(readiness, dict) else False
        execution_mode = (
            readiness.get("execution_mode")
            if isinstance(readiness, dict) and readiness.get("execution_mode")
            else "MAINNET_SHADOW"
        )
        latest_signal = read_jsonl_tail(signals_path, 1)
        latest_signal_row = latest_signal[-1] if latest_signal else {}
        live_blocked = latest_signal_row.get("liveOrderSubmissionBlocked")
        if live_blocked is None:
            live_blocked = latest_signal_row.get("live_order_submission_blocked")
        if live_blocked is None:
            live_blocked = not live_enabled
        readiness_closed = readiness.get("closed_shadow_trades") if isinstance(readiness, dict) else None
        closed_shadow_trades = readiness_closed
        if safe_float(readiness_closed) in (None, 0.0) and shadow_outcome_count is not None:
            closed_shadow_trades = shadow_outcome_count

        payload = {
            "generatedAt": utc_now(),
            "paths": {
                "projectRoot": str(self.project_root),
                "shadowRoot": str(shadow_root),
            },
            "accountProbe": self._account_probe(account, total_sample_path),
            "accountSummary": sanitize(account),
            "openPositions": sanitize(account.get("open_positions") or []),
            "shadowSnapshot": {
                "source": "phoenix_shadow_files",
                "root": str(shadow_root),
                "statePath": str(state_path),
                "updatedAt": (
                    readiness.get("generated_at")
                    if isinstance(readiness, dict)
                    else None
                )
                or (state.get("updated_at") if isinstance(state, dict) else None)
                or path_mtime_iso(state_path),
                "executionMode": execution_mode,
                "liveTradingEnabled": live_enabled,
                "liveOrderSubmissionBlocked": bool(live_blocked),
                "openOrderCount": len(pending_orders),
                "conditionalOrderCount": len(pending_orders),
                "branchCount": shadow_all.get("branch_signal_count", len(pending_orders)),
                "closedShadowTrades": closed_shadow_trades,
                "closedShadowOutcomeRecords": shadow_outcome_count,
                "winRatePct": readiness.get("win_rate_pct") if isinstance(readiness, dict) else None,
                "profitFactor": readiness.get("profit_factor") if isinstance(readiness, dict) else None,
                "maxDrawdownPct": readiness.get("max_drawdown_pct") if isinstance(readiness, dict) else None,
                "estimatedTotalPnlUsdt": (
                    readiness.get("estimated_total_pnl_usdt") if isinstance(readiness, dict) else None
                ),
                "orders": pending_orders,
                "recentOutcomes": recent_outcomes,
            },
            "environmentSnapshots": {
                "mainnet": self._environment_snapshot("mainnet"),
                "testnet": self._environment_snapshot("testnet"),
            },
            "recentLogs": self._recent_logs(),
            "recentTradeJournals": self._recent_trade_journals(outcomes_path),
            "roundRunnerReports": self._round_runner_reports(),
            "sampleCounts": self._sample_counts(
                signals_path=signals_path,
                outcomes_path=outcomes_path,
                shadow_signal_count=shadow_signal_count,
                shadow_outcome_count=shadow_outcome_count,
                league_report=league_report if isinstance(league_report, dict) else {},
                total_sample=total_sample if isinstance(total_sample, dict) else {},
            ),
            "runtime": self._runtime(bridge_state, state),
        }
        return sanitize(payload)

    def _account_probe(self, account: dict[str, Any], source_path: Path) -> dict[str, Any]:
        error = account.get("account_overview_error")
        return {
            "source": str(source_path),
            "updatedAt": path_mtime_iso(source_path),
            "ok": error in (None, "", False),
            "error": sanitize(error),
            "mode": "file_snapshot",
        }

    def _environment_snapshot(self, name: str) -> dict[str, Any]:
        env_files = [
            self.project_root / ".env",
            self.project_root / f".env.{name}",
            self.project_root / f"{name}.env",
        ]
        if name == "testnet" and self.testnet_env_file not in env_files:
            env_files.append(self.testnet_env_file)
        audit = env_audit_snapshot(name, env_files)
        if name != "testnet":
            return audit
        return self._testnet_account_snapshot(audit, env_files)

    def _testnet_account_snapshot(self, audit: dict[str, Any], env_files: list[Path]) -> dict[str, Any]:
        started = time.monotonic()
        executed_at = utc_now()
        values: dict[str, str] = {}
        for path in env_files:
            if path.name == ".env":
                continue
            values.update(parse_env_values(path))
        for key in (
            "PHOENIX_BINANCE_API_KEY",
            "PHOENIX_BINANCE_API_SECRET",
            "BINANCE_API_KEY",
            "BINANCE_SECRET_KEY",
            "PHOENIX_BINANCE_ENV",
            "BINANCE_API_ENV",
            "PHOENIX_BINANCE_ACCOUNT_API",
        ):
            if os.environ.get(key):
                values[key] = os.environ[key]

        api_key = values.get("PHOENIX_BINANCE_API_KEY") or values.get("BINANCE_API_KEY")
        api_secret = values.get("PHOENIX_BINANCE_API_SECRET") or values.get("BINANCE_SECRET_KEY")
        environment = (values.get("PHOENIX_BINANCE_ENV") or values.get("BINANCE_API_ENV") or "testnet").lower()
        account_api_requested = (values.get("PHOENIX_BINANCE_ACCOUNT_API") or "classic").lower()

        base_snapshot: dict[str, Any] = {
            "mode": "testnet",
            "label": "Testnet模式",
            "environment": "testnet",
            "source": "binance_futures_testnet_readonly",
            "configAudit": audit,
            "rawEnvReturned": False,
            "sensitiveValuesReturned": False,
            "accountSummary": None,
            "openPositions": [],
            "accountProbe": {
                "ok": False,
                "source": "python_probe",
                "executedAt": executed_at,
                "durationMs": 0,
                "command": None,
                "pythonExecutable": sys.executable,
                "environment": "testnet",
                "accountApiRequested": account_api_requested,
                "accountApiResolved": "classic",
                "checkNames": [],
                "error": None,
            },
            "shadowSnapshot": None,
        }
        if not api_key or not api_secret:
            base_snapshot["accountProbe"]["error"] = build_probe_error(
                "Testnet credentials are missing.",
                error_type="missing_testnet_credentials",
            )
            base_snapshot["accountProbe"]["durationMs"] = round((time.monotonic() - started) * 1000, 3)
            return base_snapshot
        if environment not in {"testnet", "demo"}:
            base_snapshot["accountProbe"]["error"] = build_probe_error(
                f"Refusing testnet account snapshot because PHOENIX_BINANCE_ENV is {environment!r}.",
                error_type="wrong_testnet_environment",
            )
            base_snapshot["accountProbe"]["durationMs"] = round((time.monotonic() - started) * 1000, 3)
            return base_snapshot

        check_names: list[str] = []
        try:
            balance = self._signed_get(
                base_url=TESTNET_FUTURES_REST_BASE,
                path="/fapi/v3/balance",
                api_key=api_key,
                api_secret=api_secret,
            )
            check_names.append("signed_balance_v3")
            positions_payload = self._signed_get(
                base_url=TESTNET_FUTURES_REST_BASE,
                path="/fapi/v3/positionRisk",
                api_key=api_key,
                api_secret=api_secret,
            )
            check_names.append("signed_position_v3")
            open_orders = self._signed_get(
                base_url=TESTNET_FUTURES_REST_BASE,
                path="/fapi/v1/openOrders",
                api_key=api_key,
                api_secret=api_secret,
            )
            check_names.append("signed_open_orders")
        except Exception as exc:
            base_snapshot["accountProbe"]["checkNames"] = check_names
            base_snapshot["accountProbe"]["error"] = build_probe_error(str(exc))
            base_snapshot["accountProbe"]["durationMs"] = round((time.monotonic() - started) * 1000, 3)
            return base_snapshot

        balances = balance if isinstance(balance, list) else []
        positions = positions_payload if isinstance(positions_payload, list) else []
        open_order_rows = open_orders if isinstance(open_orders, list) else []
        normalized_positions = self._normalize_testnet_positions(positions)
        account_summary = self._testnet_account_summary(
            balances=balances,
            positions=positions,
            open_order_count=len(open_order_rows),
            environment="testnet",
            account_api_requested=account_api_requested,
        )
        base_snapshot["accountSummary"] = account_summary
        base_snapshot["openPositions"] = normalized_positions
        base_snapshot["accountProbe"]["ok"] = True
        base_snapshot["accountProbe"]["checkNames"] = check_names
        base_snapshot["accountProbe"]["durationMs"] = round((time.monotonic() - started) * 1000, 3)
        return base_snapshot

    def _testnet_account_summary(
        self,
        *,
        balances: list[Any],
        positions: list[Any],
        open_order_count: int,
        environment: str,
        account_api_requested: str,
    ) -> dict[str, Any]:
        assets: list[dict[str, Any]] = []
        for row in balances:
            if not isinstance(row, dict):
                continue
            wallet = safe_float(row.get("balance") or row.get("walletBalance"))
            available = safe_float(row.get("availableBalance") or row.get("available_balance"))
            cross_wallet = safe_float(row.get("crossWalletBalance"))
            unrealized = safe_float(row.get("crossUnPnl") or row.get("unrealizedProfit"))
            if all(value in (None, 0.0) for value in (wallet, available, cross_wallet, unrealized)):
                continue
            assets.append(
                {
                    "asset": row.get("asset") or "UNKNOWN",
                    "walletBalance": wallet,
                    "availableBalance": available,
                    "marginBalance": (cross_wallet or wallet or 0.0) + (unrealized or 0.0),
                    "unrealizedProfit": unrealized or 0.0,
                    "maxWithdrawAmount": safe_float(row.get("maxWithdrawAmount")) or available,
                }
            )
        usdt = next((item for item in assets if item.get("asset") == "USDT"), assets[0] if assets else {})
        total_unrealized = sum(safe_float(row.get("unRealizedProfit") or row.get("unrealizedProfit")) or 0.0 for row in positions if isinstance(row, dict))
        total_initial_margin = sum(safe_float(row.get("positionInitialMargin")) or 0.0 for row in positions if isinstance(row, dict))
        return {
            "environment": environment,
            "accountApiRequested": account_api_requested,
            "accountApiResolved": "classic",
            "accountType": "binance_futures_testnet_readonly",
            "totalWalletBalance": usdt.get("walletBalance"),
            "availableBalance": usdt.get("availableBalance"),
            "totalMarginBalance": usdt.get("marginBalance"),
            "totalUnrealizedProfit": total_unrealized,
            "totalInitialMargin": total_initial_margin,
            "totalMaintMargin": None,
            "maxWithdrawAmount": usdt.get("maxWithdrawAmount"),
            "openOrdersCount": open_order_count,
            "openConditionalOrdersCount": None,
            "assetCount": len(assets),
            "nonZeroAssetCount": len(assets),
            "assets": assets,
        }

    def _normalize_testnet_positions(self, positions: list[Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in positions:
            if not isinstance(row, dict):
                continue
            amount = safe_float(row.get("positionAmt"))
            if amount is None or abs(amount) <= 1e-12:
                continue
            mark_price = safe_float(row.get("markPrice"))
            notional = safe_float(row.get("notional"))
            if notional is None and mark_price is not None:
                notional = amount * mark_price
            out.append(
                {
                    "symbol": row.get("symbol") or "UNKNOWN",
                    "side": "SHORT" if amount < 0 else "LONG",
                    "quantity": abs(amount),
                    "entryPrice": safe_float(row.get("entryPrice")),
                    "markPrice": mark_price,
                    "notionalUsd": notional,
                    "unrealizedPnl": safe_float(row.get("unRealizedProfit") or row.get("unrealizedProfit")),
                    "leverage": safe_float(row.get("leverage")),
                    "liquidationPrice": safe_float(row.get("liquidationPrice")),
                    "marginType": row.get("marginType"),
                    "positionSide": row.get("positionSide"),
                }
            )
        return out

    def _recent_logs(self) -> list[dict[str, Any]]:
        paths: list[Path] = []
        collector_log = self.project_root / "signal_lab_runs" / "event_collect_v6_speed_boost" / "collector_stdout.log"
        if collector_log.exists():
            paths.append(collector_log)
        bridge_logs = sorted(
            (self.project_root / "signal_lab_runs" / "bridge_restart_logs").glob("*/*.log"),
            key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
            reverse=True,
        )
        paths.extend(bridge_logs[:5])
        own_log = self.project_root / "signal_lab_runs" / "dashboard_snapshot_api.log"
        if own_log.exists():
            paths.append(own_log)
        out = []
        seen: set[Path] = set()
        for path in paths:
            if path in seen:
                continue
            seen.add(path)
            out.append(
                {
                    "source": str(path),
                    "updatedAt": path_mtime_iso(path),
                    "lines": read_text_tail(path, 30),
                }
            )
        return out

    def _recent_trade_journals(self, outcomes_path: Path) -> list[dict[str, Any]]:
        journals = [
            {
                "source": str(outcomes_path),
                "updatedAt": path_mtime_iso(outcomes_path),
                "kind": "shadow_outcomes",
                "records": read_jsonl_tail(outcomes_path, 20),
            }
        ]
        trade_files = sorted(
            self.project_root.glob("round_runner_reports*/round_*_trades.jsonl"),
            key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
            reverse=True,
        )
        for path in trade_files[:5]:
            journals.append(
                {
                    "source": str(path),
                    "updatedAt": path_mtime_iso(path),
                    "kind": "round_runner_trades",
                    "records": read_jsonl_tail(path, 10),
                }
            )
        return journals

    def _round_runner_reports(self) -> list[dict[str, Any]]:
        reports = sorted(
            self.project_root.glob("round_runner_reports*/round_*_report.json"),
            key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
            reverse=True,
        )
        out: list[dict[str, Any]] = []
        for path in reports[:20]:
            data = read_json(path)
            out.append(
                {
                    "source": str(path),
                    "updatedAt": path_mtime_iso(path),
                    "report": sanitize(data if isinstance(data, dict) else {}),
                }
            )
        return out

    def _sample_counts(
        self,
        *,
        signals_path: Path,
        outcomes_path: Path,
        shadow_signal_count: int | None = None,
        shadow_outcome_count: int | None = None,
        league_report: dict[str, Any],
        total_sample: dict[str, Any],
    ) -> dict[str, Any]:
        collector_root = self.project_root / "signal_lab_runs" / "event_collect_v6_speed_boost"
        input_counts = league_report.get("input_counts") if isinstance(league_report, dict) else {}
        shadow_all = total_sample.get("shadow_all") if isinstance(total_sample, dict) else {}
        shadow_all = shadow_all if isinstance(shadow_all, dict) else {}
        return {
            "shadowSignals": shadow_signal_count if shadow_signal_count is not None else count_lines(signals_path),
            "shadowOutcomes": shadow_outcome_count if shadow_outcome_count is not None else count_lines(outcomes_path),
            "leagueReportInputCounts": sanitize(input_counts if isinstance(input_counts, dict) else {}),
            "totalSampleShadowAll": sanitize(
                {
                    "signal_count": shadow_all.get("signal_count"),
                    "branch_signal_count": shadow_all.get("branch_signal_count"),
                    "pending_signal_count": shadow_all.get("pending_signal_count"),
                    "pending_branch_count": shadow_all.get("pending_branch_count"),
                    "outcome_count": shadow_all.get("outcome_count"),
                }
            ),
            "eventFeed": count_lines(collector_root / "bridge_event_feed.jsonl"),
            "eventSnapshots": count_lines(collector_root / "event_snapshots.jsonl"),
            "eventHorizonLabels": count_lines(collector_root / "event_horizon_labels.jsonl"),
        }

    def _runtime(self, bridge_state: Any, shadow_state: Any) -> dict[str, Any]:
        processes: list[dict[str, Any]] = []
        try:
            result = subprocess.run(
                ["ps", "-eo", "pid,lstart,args"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            if result.stdout:
                for line in result.stdout.splitlines():
                    if "phoenix_signal_bridge.py" in line or "phoenix_signal_lab.py collect" in line:
                        parts = line.split(None, 6)
                        pid = parts[0] if parts else None
                        processes.append({"pid": pid, "command": redact_text(line, max_len=1200)})
        except Exception:
            processes = []
        return {
            "service": "phoenix_dashboard_snapshot_api",
            "serviceStartedAt": SERVICE_STARTED_AT,
            "host": self.host,
            "port": self.port,
            "endpoint": DEFAULT_ENDPOINT,
            "pid": os.getpid(),
            "pythonVersion": sys.version.split()[0],
            "platform": platform.platform(),
            "hostname": socket.gethostname(),
            "cacheTtlSec": self.cache_ttl_sec,
            "projectRootExists": self.project_root.exists(),
            "shadowRootExists": self.shadow_root.exists(),
            "bridgeState": sanitize(bridge_state if isinstance(bridge_state, dict) else {}),
            "shadowStateUpdatedAt": (
                shadow_state.get("updated_at") if isinstance(shadow_state, dict) else None
            ),
            "phoenixProcesses": processes,
        }


class DashboardRequestHandler(BaseHTTPRequestHandler):
    server_version = "PhoenixDashboardSnapshotAPI/1.0"
    service: DashboardSnapshotService
    bearer_token: str
    allowed_origins: set[str]

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_empty(HTTPStatus.NO_CONTENT)

    def do_HEAD(self) -> None:  # noqa: N802
        self._send_empty(HTTPStatus.METHOD_NOT_ALLOWED, extra_headers={"Allow": "GET, OPTIONS"})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != DEFAULT_ENDPOINT:
            self._send_json({"error": {"code": "not_found", "message": "Not found"}}, HTTPStatus.NOT_FOUND)
            return
        if not self._authorized():
            self._send_json(
                {"error": {"code": "unauthorized", "message": "Bearer token required"}},
                HTTPStatus.UNAUTHORIZED,
                extra_headers={"WWW-Authenticate": "Bearer"},
            )
            return
        try:
            payload = self.service.snapshot()
        except Exception as exc:  # Keep caller response generic; detailed trace stays server-side.
            self.log_error("snapshot failed: %s", exc)
            self._send_json(
                {"error": {"code": "snapshot_failed", "message": "Snapshot unavailable"}},
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self._send_json(payload, HTTPStatus.OK)

    def do_POST(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_PUT(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_PATCH(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_DELETE(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def _method_not_allowed(self) -> None:
        self._send_json(
            {"error": {"code": "method_not_allowed", "message": "Only GET is allowed for data access"}},
            HTTPStatus.METHOD_NOT_ALLOWED,
            extra_headers={"Allow": "GET, OPTIONS"},
        )

    def _authorized(self) -> bool:
        auth = self.headers.get("Authorization", "")
        prefix = "Bearer "
        if not auth.startswith(prefix):
            return False
        supplied = auth[len(prefix) :].strip()
        return hmac.compare_digest(supplied, self.bearer_token)

    def _cors_origin(self) -> str | None:
        origin = self.headers.get("Origin")
        if not origin:
            return None
        if "*" in self.allowed_origins or origin in self.allowed_origins:
            return origin
        return None

    def _security_headers(self) -> dict[str, str]:
        headers = {
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
            "Content-Security-Policy": "default-src 'none'",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type",
            "Vary": "Origin, Authorization",
        }
        origin = self._cors_origin()
        if origin:
            headers["Access-Control-Allow-Origin"] = origin
        return headers

    def _send_empty(self, status: HTTPStatus, extra_headers: dict[str, str] | None = None) -> None:
        self.send_response(status.value)
        for key, value in {**self._security_headers(), **(extra_headers or {})}.items():
            self.send_header(key, value)
        self.end_headers()

    def _send_json(
        self,
        payload: dict[str, Any],
        status: HTTPStatus,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status.value)
        for key, value in {**self._security_headers(), **(extra_headers or {})}.items():
            self.send_header(key, value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def make_server(
    service: DashboardSnapshotService,
    bearer_token: str,
    *,
    host: str,
    port: int,
    allowed_origins: set[str] | None = None,
) -> ThreadingHTTPServer:
    class BoundHandler(DashboardRequestHandler):
        pass

    BoundHandler.service = service
    BoundHandler.bearer_token = bearer_token
    BoundHandler.allowed_origins = allowed_origins or set()
    return ThreadingHTTPServer((host, port), BoundHandler)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phoenix read-only dashboard snapshot API")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--shadow-root", type=Path, default=None)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--token-file", type=Path, default=None)
    parser.add_argument("--cache-ttl-sec", type=float, default=DEFAULT_CACHE_TTL_SEC)
    parser.add_argument("--testnet-env-file", type=Path, default=DEFAULT_TESTNET_ENV_FILE)
    parser.add_argument(
        "--allowed-origin",
        action="append",
        default=[],
        help="Allowed browser Origin. Repeat for multiple origins.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    token = load_bearer_token(args.token_file)
    service = DashboardSnapshotService(
        args.project_root,
        args.shadow_root,
        host=args.host,
        port=args.port,
        cache_ttl_sec=args.cache_ttl_sec,
        testnet_env_file=args.testnet_env_file,
    )
    allowed_origins = set(args.allowed_origin or [])
    server = make_server(service, token, host=args.host, port=args.port, allowed_origins=allowed_origins)
    print(
        json.dumps(
            {
                "event": "phoenix_dashboard_snapshot_api_started",
                "generatedAt": utc_now(),
                "endpoint": DEFAULT_ENDPOINT,
                "host": args.host,
                "port": args.port,
                "projectRoot": str(service.project_root),
                "shadowRoot": str(service.shadow_root),
                "allowedOriginCount": len(allowed_origins),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
