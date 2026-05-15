from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from phoenix.trader_snapshot import build_trader_snapshot


DEFAULT_CANDIDATE_FILES = (
    Path("phoenix_candidates.openclaw.json"),
    Path("phoenix_candidates.json"),
)
DEFAULT_STATE_DIR = Path("btc_data") / "state"
DEFAULT_DASHBOARD_SNAPSHOT_URL = "http://127.0.0.1:18765/api/phoenix-dashboard-snapshot"


def build_runtime_trader_snapshot(
    *,
    root: str | Path = ".",
    state_dir: str | Path | None = None,
    candidate_files: list[str | Path] | tuple[str | Path, ...] | None = None,
    signed_account_state: dict[str, Any] | None = None,
    signed_positions: list[dict[str, Any]] | None = None,
    max_candidates: int = 10,
    stale_after_sec: int = 60,
    max_open_positions: int = 1,
    protective_stop_path_available: bool = False,
    take_profit_path_available: bool | None = None,
    emergency_close_available: bool = False,
    protective_stop_capability_source: str = "unverified",
    take_profit_capability_source: str | None = None,
    emergency_close_capability_source: str = "unverified",
) -> dict[str, Any]:
    """Build a lightweight Trader Mode snapshot from existing Phoenix runtime files.

    This intentionally reads compact state files only. It does not start scanners,
    does not open WebSockets, and does not load large JSONL datasets.
    """
    root_path = Path(root)
    resolved_state_dir = Path(state_dir) if state_dir is not None else root_path / DEFAULT_STATE_DIR
    market_stream = _read_json(resolved_state_dir / "market_stream_snapshot.json")
    market_state = _read_json(resolved_state_dir / "market_stream_state.json")
    engine_snapshot = _read_json(resolved_state_dir / "engine_snapshot.json")
    demo_account = _read_json(resolved_state_dir / "demo_account.json")
    active_trade = _read_json(resolved_state_dir / "active_trade.json")
    paper_active_trade = _read_json(resolved_state_dir / "paper_active_trade.json")
    last_close = _read_json(resolved_state_dir / "last_close.json")

    candidates_payload, candidate_source = _load_candidates(root_path, candidate_files)
    merged_candidates = _merge_candidates_with_market(
        _candidate_rows(candidates_payload),
        market_stream,
        limit=max_candidates,
    )
    market_data = _build_market_data(market_stream, engine_snapshot)
    position_state_known = signed_positions is not None or isinstance(active_trade, dict) or isinstance(paper_active_trade, dict)
    current_positions = _positions_from_sources(
        signed_positions=signed_positions,
        active_trade=active_trade,
        paper_active_trade=paper_active_trade,
        market_stream=market_stream,
    )
    account_state = _build_account_state(
        signed_account_state=signed_account_state,
        demo_account=demo_account,
        current_positions=current_positions,
        last_close=last_close,
        max_open_positions=max_open_positions,
    )
    system_status = _build_system_status(
        market_stream=market_stream,
        market_state=market_state,
        engine_snapshot=engine_snapshot,
        candidate_source=candidate_source,
        account_source="signed_account" if signed_account_state is not None else "runtime_state",
        position_state_source="signed_positions" if signed_positions is not None else ("runtime_state" if position_state_known else "unknown"),
        stale_after_sec=stale_after_sec,
        candidates=merged_candidates,
        positions=current_positions,
        position_state_known=position_state_known,
        protective_stop_path_available=protective_stop_path_available,
        take_profit_path_available=(
            protective_stop_path_available if take_profit_path_available is None else take_profit_path_available
        ),
        emergency_close_available=emergency_close_available,
        protective_stop_capability_source=protective_stop_capability_source,
        take_profit_capability_source=take_profit_capability_source or protective_stop_capability_source,
        emergency_close_capability_source=emergency_close_capability_source,
    )
    return build_trader_snapshot(
        market_data=market_data,
        account_state=account_state,
        positions=current_positions,
        candidates=merged_candidates,
        system_status=system_status,
        max_candidates=max_candidates,
        stale_after_sec=stale_after_sec,
    )


async def fetch_signed_testnet_account_state(*, environment_name: str = "testnet") -> dict[str, Any]:
    """Fetch compact signed account state for testnet trial risk checks."""
    from phoenix.binance_futures import BinanceFuturesClient
    from phoenix.config import load_credentials, load_proxy_settings, resolve_environment

    environment = resolve_environment(environment_name)
    if environment.name not in {"testnet", "demo"}:
        return {"ok": False, "error": "signed_account_fetch_testnet_only", "account_state": None, "positions": None}
    credentials = load_credentials(required=False)
    if credentials is None:
        return {"ok": False, "error": "signed_account_credentials_missing", "account_state": None, "positions": None}
    if credentials.environment.name not in {"testnet", "demo"}:
        return {"ok": False, "error": "signed_account_credentials_not_testnet", "account_state": None, "positions": None}
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=credentials,
            proxy_settings=load_proxy_settings(),
        )
        try:
            account = await client.account_overview()
            positions = await client.position_information_v3()
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc), "account_state": None, "positions": None}
    return {
        "ok": True,
        "error": None,
        "account_state": _compact_account_state(account),
        "positions": positions if isinstance(positions, list) else [],
    }


def fetch_dashboard_snapshot_api(
    *,
    url: str = DEFAULT_DASHBOARD_SNAPSHOT_URL,
    bearer_token: str | None = None,
    timeout_sec: float = 5.0,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    request = Request(url, headers=headers, method="GET")
    with urlopen(request, timeout=max(0.5, float(timeout_sec or 5.0))) as response:  # noqa: S310 - caller controls local read-only URL.
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("dashboard_snapshot_api_payload_not_object")
    return payload


def build_dashboard_api_trader_snapshot(
    *,
    root: str | Path = ".",
    dashboard_snapshot_url: str = DEFAULT_DASHBOARD_SNAPSHOT_URL,
    dashboard_bearer_token: str | None = None,
    dashboard_timeout_sec: float = 5.0,
    max_candidates: int = 10,
    stale_after_sec: int = 60,
    max_open_positions: int = 1,
    protective_stop_path_available: bool = False,
    take_profit_path_available: bool | None = None,
    emergency_close_available: bool = False,
    protective_stop_capability_source: str = "unverified",
    take_profit_capability_source: str | None = None,
    emergency_close_capability_source: str = "unverified",
    public_market_heartbeat: bool = True,
) -> dict[str, Any]:
    try:
        payload = fetch_dashboard_snapshot_api(
            url=dashboard_snapshot_url,
            bearer_token=dashboard_bearer_token,
            timeout_sec=dashboard_timeout_sec,
        )
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        return build_trader_snapshot(
            system_status={
                "snapshot_time": datetime.now(timezone.utc).isoformat(),
                "data_fresh": False,
                "websocket_status": "unavailable",
                "exchange_status": "unavailable",
                "source": "runtime",
                "snapshot_source": "runtime",
                "trusted_runtime_snapshot": False,
                "position_state": "unknown",
                "position_state_source": "unknown",
                "candidate_state": "unavailable",
                "protective_stop_path_available": False,
                "take_profit_path_available": False,
                "take_profit_order_supported": False,
                "emergency_close_available": False,
                "protective_stop_capability_source": "unverified",
                "take_profit_capability_source": "unverified",
                "emergency_close_capability_source": "unverified",
                "dashboard_snapshot_error": str(exc)[:300],
                "freeze_reason": "dashboard_snapshot_api_unavailable",
                "can_continue": False,
            },
            stale_after_sec=stale_after_sec,
        )

    return build_trader_snapshot_from_dashboard_payload(
        payload,
        root=root,
        max_candidates=max_candidates,
        stale_after_sec=stale_after_sec,
        max_open_positions=max_open_positions,
        protective_stop_path_available=protective_stop_path_available,
        take_profit_path_available=(
            protective_stop_path_available if take_profit_path_available is None else take_profit_path_available
        ),
        emergency_close_available=emergency_close_available,
        protective_stop_capability_source=protective_stop_capability_source,
        take_profit_capability_source=take_profit_capability_source or protective_stop_capability_source,
        emergency_close_capability_source=emergency_close_capability_source,
        public_market_heartbeat=public_market_heartbeat,
    )


def build_trader_snapshot_from_dashboard_payload(
    payload: dict[str, Any],
    *,
    root: str | Path = ".",
    max_candidates: int = 10,
    stale_after_sec: int = 60,
    max_open_positions: int = 1,
    protective_stop_path_available: bool = False,
    take_profit_path_available: bool | None = None,
    emergency_close_available: bool = False,
    protective_stop_capability_source: str = "unverified",
    take_profit_capability_source: str | None = None,
    emergency_close_capability_source: str = "unverified",
    public_market_heartbeat: bool = False,
) -> dict[str, Any]:
    root_path = Path(root)
    resolved_take_profit_path_available = (
        protective_stop_path_available if take_profit_path_available is None else take_profit_path_available
    )
    resolved_take_profit_capability_source = take_profit_capability_source or protective_stop_capability_source
    take_profit_order_supported = bool(
        resolved_take_profit_path_available and _verified_capability_source(resolved_take_profit_capability_source)
    )
    dashboard_generated_at = str(payload.get("generatedAt") or datetime.now(timezone.utc).isoformat())
    generated_age = _age_sec(dashboard_generated_at)
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    shadow = payload.get("shadowSnapshot") if isinstance(payload.get("shadowSnapshot"), dict) else {}
    environment_snapshots = payload.get("environmentSnapshots") if isinstance(payload.get("environmentSnapshots"), dict) else {}
    paths = payload.get("paths") if isinstance(payload.get("paths"), dict) else {}
    project_root = root_path
    project_root_value = paths.get("projectRoot")
    if project_root_value:
        try:
            project_root = Path(str(project_root_value)).expanduser()
        except Exception:
            project_root = root_path
    shadow_root: Path | None = None
    shadow_root_value = paths.get("shadowRoot")
    if shadow_root_value:
        try:
            shadow_root = Path(str(shadow_root_value)).expanduser()
        except Exception:
            shadow_root = None
    elif root_path.exists():
        shadow_root = root_path
    bridge_state_candidates: list[Path] = []
    shadow_state_candidates: list[Path] = []
    if isinstance(shadow_root, Path):
        bridge_state_candidates.append(shadow_root / "bridge-state.json")
        shadow_state_candidates.append(shadow_root / "bridge-state.shadow.json")
        if shadow_root.parent != shadow_root:
            bridge_state_candidates.append(shadow_root.parent / "bridge-state.json")
            shadow_state_candidates.append(shadow_root.parent / "bridge-state.shadow.json")
    bridge_state_file = _read_first_json(*bridge_state_candidates)
    shadow_state_file = _read_first_json(*shadow_state_candidates)
    testnet = environment_snapshots.get("testnet") if isinstance(environment_snapshots.get("testnet"), dict) else {}
    probe = testnet.get("accountProbe") if isinstance(testnet.get("accountProbe"), dict) else {}
    account_summary = testnet.get("accountSummary") if isinstance(testnet.get("accountSummary"), dict) else {}
    open_positions = testnet.get("openPositions") if isinstance(testnet.get("openPositions"), list) else None

    probe_executed_at = probe.get("executedAt")
    probe_age = _age_sec(probe_executed_at)
    stale_limit = max(1, int(stale_after_sec or 60))
    account_probe_present = probe.get("ok") is True and probe_age is not None
    account_probe_fresh = account_probe_present and probe_age <= stale_limit
    candidates_updated_at = (
        _coerce_timestamp_value(runtime.get("candidatesUpdatedAt"))
        or _coerce_timestamp_value(shadow.get("updatedAt"))
        or _coerce_timestamp_value((shadow_state_file or {}).get("updated_at"))
    )
    bridge_state = runtime.get("bridgeState") if isinstance(runtime.get("bridgeState"), dict) else {}
    bridge_heartbeat_at = (
        _coerce_timestamp_value(runtime.get("bridgeHeartbeatAt"))
        or _coerce_timestamp_value(bridge_state.get("updated_at"))
        or _coerce_timestamp_value(bridge_state.get("updatedAt"))
        or _coerce_timestamp_value((bridge_state_file or {}).get("updated_at"))
    )
    bridge_heartbeat_source_event = "runtime.bridgeHeartbeatAt" if _coerce_timestamp_value(runtime.get("bridgeHeartbeatAt")) else None
    if bridge_heartbeat_source_event is None and (
        _coerce_timestamp_value(bridge_state.get("updated_at"))
        or _coerce_timestamp_value(bridge_state.get("updatedAt"))
        or _coerce_timestamp_value((bridge_state_file or {}).get("updated_at"))
    ):
        bridge_heartbeat_source_event = "bridge_state.updated_at"
    market_data_source_path = None
    if isinstance(bridge_state_file, dict) and bridge_state_file.get("snapshots_file"):
        try:
            market_data_source_path = Path(str(bridge_state_file.get("snapshots_file"))).expanduser()
        except Exception:
            market_data_source_path = None
    if market_data_source_path is None and shadow_root is not None:
        market_data_source_path = shadow_root / "signal_bridge_shadow_signals.jsonl"
    market_data_source_event = "runtime.marketDataUpdatedAt" if _coerce_timestamp_value(runtime.get("marketDataUpdatedAt")) else None
    market_data_updated_at = _coerce_timestamp_value(runtime.get("marketDataUpdatedAt")) or _load_latest_market_data_timestamp(market_data_source_path)
    if market_data_updated_at is not None and market_data_source_event is None:
        market_data_source_event = "market_event_feed"
    bridge_fetch_at, bridge_fetch_path, bridge_fetch_event = _load_latest_bridge_market_fetch(project_root)
    if bridge_fetch_at is not None:
        market_age = _age_sec(market_data_updated_at)
        bridge_fetch_age = _age_sec(bridge_fetch_at)
        if market_data_updated_at is None or (
            bridge_fetch_age is not None and (market_age is None or bridge_fetch_age < market_age)
        ):
            market_data_updated_at = bridge_fetch_at
            market_data_source_path = bridge_fetch_path
            market_data_source_event = bridge_fetch_event
    if public_market_heartbeat:
        public_fetch_at, public_fetch_event = _fetch_public_book_ticker_heartbeat()
        public_fetch_age = _age_sec(public_fetch_at)
        current_market_age = _age_sec(market_data_updated_at)
        if public_fetch_at is not None and (
            market_data_updated_at is None
            or (public_fetch_age is not None and (current_market_age is None or public_fetch_age < current_market_age))
        ):
            market_data_updated_at = public_fetch_at
            market_data_source_path = None
            market_data_source_event = public_fetch_event
        current_bridge_age = _age_sec(bridge_heartbeat_at)
        if public_fetch_at is not None and (
            bridge_heartbeat_at is None
            or (
                public_fetch_age is not None
                and (current_bridge_age is None or current_bridge_age > stale_limit or public_fetch_age < current_bridge_age)
            )
        ):
            bridge_heartbeat_at = public_fetch_at
            bridge_heartbeat_source_event = public_fetch_event
    account_state_updated_at = _coerce_timestamp_value(probe_executed_at)
    position_state_updated_at = account_state_updated_at if open_positions is not None else None
    market_data_age = _age_sec(market_data_updated_at)
    bridge_age = _age_sec(bridge_heartbeat_at)
    account_age = _age_sec(account_state_updated_at)
    position_age = _age_sec(position_state_updated_at)
    shadow_orders = shadow.get("orders") if isinstance(shadow.get("orders"), list) else []
    (
        candidates,
        candidate_source,
        candidate_state,
        candidate_empty_reason,
        candidate_pool_count,
        candidate_ignored_stale_count,
        candidate_latest_signal_at,
        candidate_latest_age_sec,
    ) = _dashboard_candidates(
        shadow,
        bridge_state_file=bridge_state_file,
        shadow_state_file=shadow_state_file,
        project_root=project_root,
        max_candidates=max_candidates,
        stale_after_sec=stale_after_sec,
    )
    market_data = _build_dashboard_market_data(
        runtime=runtime,
        bridge_state_file=bridge_state_file,
        project_root=project_root,
        stale_after_sec=stale_after_sec,
    )
    candidate_producer = _candidate_producer_heartbeat_status(
        bridge_state_file=bridge_state_file,
        project_root=project_root,
        stale_after_sec=stale_after_sec,
    )
    market_fresh = market_data_age is not None and market_data_age <= stale_limit
    bridge_fresh = bridge_age is not None and bridge_age <= stale_limit
    feed_status = "unavailable"
    websocket_status = "unavailable"
    if market_fresh and bridge_fresh:
        feed_status = "healthy" if market_data_age <= max(1.0, stale_limit / 2) else "degraded"
        websocket_status = feed_status
    position_state_known = account_probe_present and open_positions is not None
    exchange_status = "unavailable"
    if account_probe_present and position_state_known:
        if account_probe_fresh:
            exchange_status = "healthy"
        elif probe_age is not None and probe_age <= stale_limit * 2:
            exchange_status = "degraded_but_usable"
    positions = _dashboard_positions(open_positions or [])
    if not positions:
        stop_status = "healthy"
    elif all(str(item.get("protection_status") or "").strip().lower() not in {"", "unknown", "unavailable"} for item in positions):
        stop_status = "healthy"
    else:
        stop_status = "unknown"
    freshness_reasons: list[str] = []
    if market_data_updated_at is None:
        freshness_reasons.append("market_data_missing")
    elif not market_fresh:
        freshness_reasons.append("market_data_stale")
    if bridge_heartbeat_at is None:
        freshness_reasons.append("heartbeat_missing")
    elif not bridge_fresh:
        freshness_reasons.append("heartbeat_stale")
    if not account_probe_present:
        freshness_reasons.append("account_state_unknown")
    if not position_state_known:
        freshness_reasons.append("position_state_unknown")
    if stop_status != "healthy":
        freshness_reasons.append("stop_protection_status_unknown")
    if not protective_stop_path_available or not _verified_capability_source(protective_stop_capability_source):
        freshness_reasons.append("protective_stop_path_unverified")
    if not resolved_take_profit_path_available or not _verified_capability_source(resolved_take_profit_capability_source):
        freshness_reasons.append("take_profit_path_unverified")
    if not emergency_close_available or not _verified_capability_source(emergency_close_capability_source):
        freshness_reasons.append("emergency_close_unverified")
    if websocket_status == "unavailable":
        freshness_reasons.append("websocket_unavailable")
    if exchange_status == "unavailable":
        freshness_reasons.append("exchange_unknown")
    trusted = (
        market_fresh
        and bridge_fresh
        and websocket_status in {"healthy", "degraded"}
        and exchange_status in {"healthy", "degraded_but_usable"}
        and account_probe_present
        and position_state_known
        and stop_status == "healthy"
        and bool(protective_stop_path_available)
        and bool(resolved_take_profit_path_available)
        and bool(emergency_close_available)
        and _verified_capability_source(protective_stop_capability_source)
        and _verified_capability_source(resolved_take_profit_capability_source)
        and _verified_capability_source(emergency_close_capability_source)
    )
    data_fresh = bool(trusted)
    if not data_fresh:
        freshness_reasons.append("runtime_untrusted")
    freshness_reason = ",".join(dict.fromkeys(reason for reason in freshness_reasons if reason)) or None
    account_state = {
        "trading_allowed": account_probe_present,
        "daily_pnl_pct": 0.0,
        "max_daily_loss_pct": 3.0,
        "open_positions_count": len(positions),
        "max_open_positions": max_open_positions,
        "available_balance_usdt": _first_float(account_summary, "availableBalance", "maxWithdrawAmount"),
        "wallet_balance_usdt": _first_float(account_summary, "totalWalletBalance", "totalMarginBalance"),
    }
    snapshot_generated_at = datetime.now(timezone.utc).isoformat()
    candidate_fresh = (
        candidate_state in {"known", "fresh"}
        and candidate_latest_age_sec is not None
        and candidate_latest_age_sec <= stale_limit
    )
    candidate_available = candidate_pool_count > 0
    candidate_tradeable = bool(candidates) and candidate_fresh
    system_status = {
        "snapshot_time": snapshot_generated_at,
        "snapshot_generated_at": snapshot_generated_at,
        "dashboard_generated_at": dashboard_generated_at,
        "data_fresh": bool(data_fresh),
        "data_age_sec": market_data_age if market_data_age is not None else generated_age,
        "market_data_updated_at": market_data_updated_at,
        "bridge_heartbeat_at": bridge_heartbeat_at,
        "bridge_heartbeat_source_event": bridge_heartbeat_source_event,
        "candidates_updated_at": candidates_updated_at,
        "account_state_updated_at": account_state_updated_at,
        "position_state_updated_at": position_state_updated_at,
        "last_market_tick_at": market_data_updated_at,
        "last_bridge_loop_at": bridge_heartbeat_at,
        "last_successful_fetch_at": account_state_updated_at,
        "feed_lag_sec": market_data_age,
        "feed_status": feed_status,
        "feed_health_source": market_data_source_event,
        "market_data_source_path": str(market_data_source_path) if market_data_source_path is not None else None,
        "market_data_source_event": market_data_source_event,
        "websocket_status": websocket_status,
        "exchange_status": exchange_status,
        "source": "runtime",
        "snapshot_source": "runtime",
        "trusted_runtime_snapshot": trusted,
        "candidate_source": candidate_source,
        "producer_alive": bool(candidate_producer["producer_alive"]),
        "candidate_producer_heartbeat_path": candidate_producer["heartbeat_path"],
        "candidate_producer_heartbeat_age_sec": candidate_producer["heartbeat_age_sec"],
        "candidate_producer_running": bool(candidate_producer["running"]),
        "candidate_producer_error_count": candidate_producer["error_count"],
        "candidate_fresh": bool(candidate_fresh),
        "candidate_available": bool(candidate_available),
        "candidate_tradeable": bool(candidate_tradeable),
        "account_source": "signed_account" if account_probe_present else "dashboard_api_unavailable",
        "account_state_source": "signed_account" if account_probe_present else "dashboard_api_unavailable",
        "position_state_source": "signed_positions" if position_state_known else "unknown",
        "stale_after_sec": stale_after_sec,
        "position_state": "known" if position_state_known else "unknown",
        "stop_protection_status": stop_status,
        "candidate_state": candidate_state,
        "candidate_empty_reason": candidate_empty_reason,
        "shadow_orders_count": len(shadow_orders),
        "candidate_pool_count": candidate_pool_count,
        "candidate_ignored_stale_count": candidate_ignored_stale_count,
        "candidate_latest_signal_at": candidate_latest_signal_at,
        "candidate_latest_age_sec": candidate_latest_age_sec,
        "protective_stop_path_available": bool(protective_stop_path_available),
        "take_profit_path_available": bool(resolved_take_profit_path_available),
        "take_profit_order_supported": take_profit_order_supported,
        "emergency_close_available": bool(emergency_close_available),
        "protective_stop_capability_source": protective_stop_capability_source,
        "take_profit_capability_source": resolved_take_profit_capability_source,
        "emergency_close_capability_source": emergency_close_capability_source,
        "dashboard_snapshot_url": DEFAULT_DASHBOARD_SNAPSHOT_URL,
        "dashboard_account_probe_ok": account_probe_fresh,
        "dashboard_account_probe_executed_at": probe_executed_at,
        "dashboard_account_probe_age_sec": probe_age,
        "dashboard_shadow_updated_at": shadow.get("updatedAt"),
        "candidates_state_age_sec": _age_sec(candidates_updated_at),
        "bridge_heartbeat_age_sec": bridge_age,
        "account_state_age_sec": account_age,
        "position_state_age_sec": position_age,
        "freshness_reason": freshness_reason,
        "can_continue": trusted,
        "freeze_reason": None if trusted else "runtime_snapshot_not_trusted_for_testnet_order",
    }
    return build_trader_snapshot(
        market_data=market_data,
        account_state=account_state,
        positions=positions,
        candidates=candidates,
        system_status=system_status,
        max_candidates=max_candidates,
        stale_after_sec=stale_after_sec,
    )


def _coerce_timestamp_value(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, dict):
        for key in ("updated_at", "updatedAt", "timestamp", "generated_at", "executedAt", "observed_at"):
            nested = _coerce_timestamp_value(value.get(key))
            if nested is not None:
                return nested
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric <= 0:
            return None
        if numeric >= 1_000_000_000_000:
            return datetime.fromtimestamp(numeric / 1000.0, tz=timezone.utc).isoformat()
        return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return _coerce_timestamp_value(int(text))
    parsed = _parse_dt(text)
    return parsed.isoformat() if parsed is not None else None


def _read_last_jsonl_row(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            if size <= 0:
                return None
            chunk_size = 4096
            buffer = b""
            position = size
            while position > 0:
                read_size = min(chunk_size, position)
                position -= read_size
                handle.seek(position)
                buffer = handle.read(read_size) + buffer
                if buffer.count(b"\n") >= 2:
                    break
            lines = [line for line in buffer.splitlines() if line.strip()]
            if not lines:
                return None
            row = json.loads(lines[-1].decode("utf-8", errors="replace"))
            return row if isinstance(row, dict) else None
    except OSError:
        return None
    except Exception:
        return None


def _load_latest_market_data_timestamp(shadow_root: Path | None) -> str | None:
    if shadow_root is None:
        return None
    signals_path = shadow_root / "signal_bridge_shadow_signals.jsonl" if shadow_root.is_dir() else shadow_root
    row = _read_last_jsonl_row(signals_path)
    if not row:
        return None
    market_snapshot = row.get("market_snapshot") if isinstance(row.get("market_snapshot"), dict) else {}
    if isinstance(market_snapshot, dict):
        observed_at = _coerce_timestamp_value(
            market_snapshot.get("observed_at_ms") or market_snapshot.get("updated_at") or market_snapshot.get("updatedAt")
        )
        if observed_at is not None:
            return observed_at
    return _coerce_timestamp_value(
        row.get("signal_time_ms")
        or row.get("observed_at_ms")
        or row.get("event_time_ms")
        or row.get("timestamp_ms")
        or row.get("updated_at")
        or row.get("updatedAt")
        or row.get("timestamp")
    )


def _load_latest_bridge_market_fetch(root: Path | None) -> tuple[str | None, Path | None, str | None]:
    if root is None:
        return None, None, None
    log_paths = (
        root / "logs" / "mainnet_shadow_bridge_active.log",
        root / "logs" / "phoenix_signal_bridge.log",
    )
    for path in log_paths:
        event = _find_recent_log_event(path, {"signal_bridge_live_universe_refreshed"})
        if event is None:
            continue
        at = _coerce_timestamp_value(event.get("at"))
        if at is not None:
            return at, path, str(event.get("event") or "signal_bridge_live_universe_refreshed")
    return None, None, None


def _fetch_public_book_ticker_heartbeat(*, symbol: str = "BTCUSDT", timeout_sec: float = 2.0) -> tuple[str | None, str | None]:
    safe_symbol = "".join(ch for ch in str(symbol or "BTCUSDT").upper() if ch.isalnum())
    if not safe_symbol:
        safe_symbol = "BTCUSDT"
    url = f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={safe_symbol}"
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "PhoenixTraderSnapshot/1.0"}, method="GET")
    try:
        with urlopen(request, timeout=max(0.5, float(timeout_sec or 2.0))) as response:  # noqa: S310 - public market-data heartbeat only.
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None, None
    if not isinstance(payload, dict):
        return None, None
    exchange_time = _coerce_timestamp_value(payload.get("time"))
    if exchange_time is not None:
        return exchange_time, f"binance_public_book_ticker:{safe_symbol}"
    return datetime.now(timezone.utc).isoformat(), f"binance_public_book_ticker:{safe_symbol}"


def _find_recent_log_event(path: Path, event_names: set[str]) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            if size <= 0:
                return None
            read_size = min(524_288, size)
            handle.seek(size - read_size)
            raw = handle.read(read_size)
    except OSError:
        return None
    for line in reversed([item for item in raw.splitlines() if item.strip()]):
        try:
            row = json.loads(line.decode("utf-8", errors="replace"))
        except Exception:
            continue
        if isinstance(row, dict) and str(row.get("event") or "") in event_names:
            return row
    return None


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _read_first_json(*paths: Path) -> dict[str, Any] | None:
    for path in paths:
        payload = _read_json(path)
        if payload is not None:
            return payload
    return None


def _dashboard_candidates(
    shadow: dict[str, Any],
    *,
    bridge_state_file: dict[str, Any] | None,
    shadow_state_file: dict[str, Any] | None,
    project_root: Path,
    max_candidates: int,
    stale_after_sec: int,
) -> tuple[list[dict[str, Any]], str, str, str | None, int, int, str | None, float | None]:
    rows = shadow.get("orders") if isinstance(shadow.get("orders"), list) else []
    limit = max(1, min(10, int(max_candidates or 10)))
    # shadowSnapshot.orders are already-created shadow/order records. Hermes needs
    # the upstream opportunity pool, so orders stay diagnostic-only here.

    event_path, event_source = _event_candidate_source_path(bridge_state_file, project_root)
    event_candidates, event_pool_count, event_ignored, event_latest_at, event_latest_age = _candidate_rows_from_event_snapshots(
        event_path,
        limit=limit,
        stale_after_sec=stale_after_sec,
    )
    if event_candidates:
        return (
            event_candidates,
            event_source,
            "known",
            None,
            event_pool_count,
            event_ignored,
            event_latest_at,
            event_latest_age,
        )
    pending = (
        shadow_state_file.get("pending_by_event_id")
        if isinstance(shadow_state_file, dict) and isinstance(shadow_state_file.get("pending_by_event_id"), dict)
        else {}
    )
    pending_rows = [row for row in pending.values() if isinstance(row, dict)] if isinstance(pending, dict) else []
    pending_candidates, pending_ignored, latest_at, latest_age = _candidate_rows_from_pending_signals(
        pending_rows,
        limit=limit,
        stale_after_sec=stale_after_sec,
    )
    if pending_candidates:
        return (
            pending_candidates,
            "bridge_state_shadow.pending_by_event_id",
            "known",
            None,
            len(pending_rows),
            pending_ignored,
            latest_at,
            latest_age,
        )
    if event_pool_count:
        return (
            [],
            event_source,
            "stale",
            "candidate_source_stale",
            event_pool_count,
            event_ignored,
            event_latest_at,
            event_latest_age,
        )
    if pending_rows:
        return (
            [],
            "bridge_state_shadow.pending_by_event_id",
            "stale",
            "candidate_source_stale",
            len(pending_rows),
            pending_ignored,
            latest_at,
            latest_age,
        )

    return (
        [],
        "unavailable",
        "unavailable",
        "candidate_source_missing",
        0,
        0,
        None,
        None,
    )


def _build_dashboard_market_data(
    *,
    runtime: dict[str, Any],
    bridge_state_file: dict[str, Any] | None,
    project_root: Path,
    stale_after_sec: int,
) -> dict[str, Any]:
    explicit = runtime.get("marketRegime") or runtime.get("market_regime")
    if isinstance(explicit, dict):
        explicit_regime = str(explicit.get("regime") or explicit.get("market_regime") or "").strip().upper()
        if explicit_regime:
            return {
                "regime": explicit_regime,
                "btc_trend_1m": _safe_float(explicit.get("btc_trend_1m")),
                "btc_trend_5m": _safe_float(explicit.get("btc_trend_5m")),
                "btc_trend_60m": _safe_float(explicit.get("btc_trend_60m")),
                "eth_trend_1m": _safe_float(explicit.get("eth_trend_1m")),
                "eth_trend_5m": _safe_float(explicit.get("eth_trend_5m")),
                "eth_trend_60m": _safe_float(explicit.get("eth_trend_60m")),
                "volatility": _safe_float(explicit.get("volatility")),
                "direction_lock": explicit.get("direction_lock"),
                "market_regime_source": str(explicit.get("market_regime_source") or "dashboard_runtime.marketRegime"),
                "market_regime_reason": str(explicit.get("market_regime_reason") or "runtime supplied explicit market regime"),
                "market_regime_confidence": _safe_float(explicit.get("market_regime_confidence")) or 0.0,
                "market_regime_updated_at": _coerce_timestamp_value(explicit.get("updated_at") or explicit.get("timestamp")),
            }

    event_path, event_source = _event_candidate_source_path(bridge_state_file, project_root)
    rows = _read_jsonl_tail(event_path, max_lines=96)
    stale_limit = max(1, int(stale_after_sec or 60))
    fresh_rows: list[dict[str, Any]] = []
    latest_at: str | None = None
    latest_age: float | None = None
    for row in rows:
        row_at = _candidate_timestamp(row)
        row_age = _age_sec(row_at)
        if row_age is not None and (latest_age is None or row_age < latest_age):
            latest_at = row_at
            latest_age = row_age
        if row_age is not None and row_age <= stale_limit:
            fresh_rows.append(row)

    source = f"{event_source}.enrichments"
    base = {
        "market_regime_source": source,
        "market_regime_source_path": str(event_path),
        "market_regime_updated_at": latest_at,
        "market_regime_data_points": len(fresh_rows),
    }
    if not fresh_rows:
        return {
            **base,
            "regime": "UNKNOWN",
            "direction_lock": "BOTH_ALLOWED",
            "market_regime_reason": "market_regime_source_stale_or_empty",
            "market_regime_confidence": 0.0,
        }

    btc_5m = _aggregate_float(fresh_rows, "btcusdt_ret_5m_pct")
    btc_60m = _aggregate_float(fresh_rows, "btcusdt_ret_60m_pct")
    eth_5m = _aggregate_float(fresh_rows, "ethusdt_ret_5m_pct")
    eth_60m = _aggregate_float(fresh_rows, "ethusdt_ret_60m_pct")
    atr = _aggregate_float(fresh_rows, "atr_20_pct")
    candle_range = _aggregate_float(fresh_rows, "candle_range_pct")
    spread = _aggregate_float(fresh_rows, "spread_bps", "spread_bps_at_entry")
    regime_score = _aggregate_float(fresh_rows, "market_regime_score")
    regime_bucket = _aggregate_text(fresh_rows, "market_regime_bucket")
    liquidity_bucket = _aggregate_text(fresh_rows, "liquidity_bucket")
    volatility = max(
        [abs(item) for item in (btc_5m, btc_60m, eth_5m, eth_60m, atr, candle_range) if item is not None],
        default=None,
    )
    regime = _classify_dashboard_market_regime(
        btc_5m=btc_5m,
        btc_60m=btc_60m,
        eth_5m=eth_5m,
        eth_60m=eth_60m,
        volatility=volatility,
        spread=spread,
        regime_bucket=regime_bucket,
        liquidity_bucket=liquidity_bucket,
    )
    direction_lock = {
        "TREND_UP": "LONG_ONLY_OR_NO_TRADE",
        "TREND_DOWN": "SHORT_ONLY_OR_NO_TRADE",
        "HIGH_VOL": "NO_TRADE",
        "LOW_LIQUIDITY": "NO_TRADE",
    }.get(regime, "BOTH_ALLOWED")
    return {
        **base,
        "regime": regime,
        "btc_trend_5m": btc_5m,
        "btc_trend_60m": btc_60m,
        "eth_trend_5m": eth_5m,
        "eth_trend_60m": eth_60m,
        "volatility": volatility,
        "direction_lock": direction_lock,
        "market_regime_reason": _dashboard_market_regime_reason(
            btc_5m=btc_5m,
            btc_60m=btc_60m,
            eth_5m=eth_5m,
            eth_60m=eth_60m,
            volatility=volatility,
            spread=spread,
            regime_bucket=regime_bucket,
            liquidity_bucket=liquidity_bucket,
        ),
        "market_regime_confidence": _dashboard_market_regime_confidence(
            regime=regime,
            btc_5m=btc_5m,
            btc_60m=btc_60m,
            eth_5m=eth_5m,
            eth_60m=eth_60m,
            volatility=volatility,
        ),
        "market_regime_bucket": regime_bucket,
        "market_regime_score": regime_score,
    }


def _aggregate_float(rows: list[dict[str, Any]], *keys: str) -> float | None:
    values = [_first_float_deep(row, *keys) for row in rows]
    values = [value for value in values if value is not None]
    if not values:
        return None
    values.sort()
    middle = len(values) // 2
    if len(values) % 2:
        return round(values[middle], 6)
    return round((values[middle - 1] + values[middle]) / 2.0, 6)


def _aggregate_text(rows: list[dict[str, Any]], *keys: str) -> str | None:
    counts: dict[str, int] = {}
    for row in rows:
        value = _first_value_deep(row, *keys)
        if value in (None, ""):
            continue
        text = str(value).strip()
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    if not counts:
        return None
    return sorted(counts.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0]


def _classify_dashboard_market_regime(
    *,
    btc_5m: float | None,
    btc_60m: float | None,
    eth_5m: float | None,
    eth_60m: float | None,
    volatility: float | None,
    spread: float | None,
    regime_bucket: str | None,
    liquidity_bucket: str | None,
) -> str:
    bucket = str(regime_bucket or "").lower()
    liquidity = str(liquidity_bucket or "").lower()
    if spread is not None and spread > 12.0 and not any(token in liquidity for token in ("high", "major", "extreme")):
        return "LOW_LIQUIDITY"
    if volatility is not None and volatility >= 2.5:
        return "HIGH_VOL"
    if not any(value is not None for value in (btc_5m, btc_60m, eth_5m, eth_60m)):
        return "UNKNOWN"
    if "long" in bucket:
        return "TREND_UP"
    if "short" in bucket:
        return "TREND_DOWN"
    if "flat" in bucket or "range" in bucket or "chop" in bucket:
        return "CHOP"
    short_values = [value for value in (btc_5m, eth_5m) if value is not None]
    long_values = [value for value in (btc_60m, eth_60m) if value is not None]
    if len(short_values) >= 2 and all(value >= 0.12 for value in short_values):
        if not long_values or sum(long_values) / len(long_values) >= -0.15:
            return "TREND_UP"
    if len(short_values) >= 2 and all(value <= -0.12 for value in short_values):
        if not long_values or sum(long_values) / len(long_values) <= 0.15:
            return "TREND_DOWN"
    return "CHOP"


def _dashboard_market_regime_reason(
    *,
    btc_5m: float | None,
    btc_60m: float | None,
    eth_5m: float | None,
    eth_60m: float | None,
    volatility: float | None,
    spread: float | None,
    regime_bucket: str | None,
    liquidity_bucket: str | None,
) -> str:
    return (
        "derived_from_fresh_signal_lab_event_snapshots:"
        f" btc_5m={_fmt_optional_float(btc_5m)}"
        f" btc_60m={_fmt_optional_float(btc_60m)}"
        f" eth_5m={_fmt_optional_float(eth_5m)}"
        f" eth_60m={_fmt_optional_float(eth_60m)}"
        f" volatility={_fmt_optional_float(volatility)}"
        f" spread_bps={_fmt_optional_float(spread)}"
        f" bucket={regime_bucket or 'unavailable'}"
        f" liquidity={liquidity_bucket or 'unavailable'}"
    )


def _dashboard_market_regime_confidence(
    *,
    regime: str,
    btc_5m: float | None,
    btc_60m: float | None,
    eth_5m: float | None,
    eth_60m: float | None,
    volatility: float | None,
) -> float:
    if regime == "UNKNOWN":
        return 0.0
    values = [value for value in (btc_5m, btc_60m, eth_5m, eth_60m) if value is not None]
    completeness = len(values) / 4.0
    movement = max([abs(value) for value in values] + ([abs(volatility)] if volatility is not None else [0.0]))
    confidence = 0.45 + completeness * 0.3 + min(0.2, movement / 10.0)
    if regime == "CHOP":
        confidence = max(confidence, 0.58)
    return round(min(0.95, confidence), 3)


def _fmt_optional_float(value: float | None) -> str:
    return "unavailable" if value is None else f"{value:.6g}"


def _event_candidate_source_path(bridge_state_file: dict[str, Any] | None, project_root: Path) -> tuple[Path, str]:
    snapshots_value = bridge_state_file.get("snapshots_file") if isinstance(bridge_state_file, dict) else None
    if snapshots_value:
        try:
            snapshots_path = Path(str(snapshots_value)).expanduser()
            if not snapshots_path.is_absolute():
                snapshots_path = project_root / snapshots_path
            sibling_event_snapshots = snapshots_path.with_name("event_snapshots.jsonl")
            if sibling_event_snapshots.exists():
                return sibling_event_snapshots, "signal_lab.event_snapshots"
            return snapshots_path, f"signal_lab.{snapshots_path.name}"
        except Exception:
            pass
    return project_root / "signal_lab_runs" / "event_collect_v6_speed_boost" / "event_snapshots.jsonl", "signal_lab.event_snapshots"


def _candidate_producer_heartbeat_status(
    *,
    bridge_state_file: dict[str, Any] | None,
    project_root: Path,
    stale_after_sec: int,
) -> dict[str, Any]:
    event_path, _source = _event_candidate_source_path(bridge_state_file, project_root)
    heartbeat_path = event_path.with_name("candidate_producer_heartbeat.json")
    payload = _read_json(heartbeat_path)
    if payload is None:
        return {
            "heartbeat_path": str(heartbeat_path),
            "heartbeat_age_sec": None,
            "running": False,
            "producer_alive": False,
            "error_count": None,
        }
    heartbeat_at = _coerce_timestamp_value(
        payload.get("created_at") or payload.get("last_success_at") or payload.get("timestamp")
    )
    heartbeat_age = _age_sec(heartbeat_at)
    heartbeat_recent = heartbeat_age is not None and heartbeat_age <= max(1, int(stale_after_sec or 60))
    running = bool(payload.get("running") or payload.get("producer_alive"))
    return {
        "heartbeat_path": str(heartbeat_path),
        "heartbeat_age_sec": round(heartbeat_age, 3) if heartbeat_age is not None else None,
        "running": running,
        "producer_alive": bool(running and heartbeat_recent),
        "error_count": payload.get("error_count"),
    }


def _candidate_rows_from_dashboard_orders(rows: list[Any], *, limit: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or row.get("direction") or "").upper()
        candidates.append(_candidate_from_runtime_row(row, symbol=symbol, side=side, source_label="dashboard shadow order"))
    return candidates


def _candidate_rows_from_pending_signals(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    stale_after_sec: int,
) -> tuple[list[dict[str, Any]], int, str | None, float | None]:
    scored: list[tuple[str, dict[str, Any]]] = []
    latest_at: str | None = None
    latest_age: float | None = None
    ignored_stale = 0
    for row in rows:
        candidate_at = _candidate_timestamp(row)
        age = _age_sec(candidate_at)
        if age is not None and (latest_age is None or age < latest_age):
            latest_at = candidate_at
            latest_age = age
        if age is None or age > max(1, int(stale_after_sec or 60)):
            ignored_stale += 1
            continue
        scored.append((candidate_at or "", row))
    scored.sort(key=lambda item: (_candidate_score(item[1]), item[0]), reverse=True)
    candidates: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for _, row in scored:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol or symbol in seen_symbols:
            continue
        side = str(row.get("side") or row.get("direction") or "").upper()
        candidates.append(_candidate_from_runtime_row(row, symbol=symbol, side=side, source_label="pending shadow signal"))
        seen_symbols.add(symbol)
        if len(candidates) >= limit:
            break
    return candidates, ignored_stale, latest_at, latest_age


def _candidate_rows_from_event_snapshots(
    path: Path,
    *,
    limit: int,
    stale_after_sec: int,
) -> tuple[list[dict[str, Any]], int, int, str | None, float | None]:
    rows = _read_jsonl_tail(path, max_lines=max(50, limit * 8))
    latest_at: str | None = None
    latest_age: float | None = None
    ignored_stale = 0
    fresh_rows: list[dict[str, Any]] = []
    for row in rows:
        candidate_at = _candidate_timestamp(row)
        age = _age_sec(candidate_at)
        if age is not None and (latest_age is None or age < latest_age):
            latest_at = candidate_at
            latest_age = age
        if age is None or age > max(1, int(stale_after_sec or 60)):
            ignored_stale += 1
            continue
        fresh_rows.append(row)
    fresh_rows.sort(key=_candidate_score, reverse=True)
    candidates: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for row in fresh_rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol or symbol in seen_symbols:
            continue
        side = str(row.get("side") or row.get("direction") or row.get("bias") or "").upper()
        candidates.append(_candidate_from_runtime_row(row, symbol=symbol, side=side, source_label="signal lab market event"))
        seen_symbols.add(symbol)
        if len(candidates) >= limit:
            break
    return candidates, len(rows), ignored_stale, latest_at, latest_age


def _candidate_containers(row: dict[str, Any]) -> list[dict[str, Any]]:
    containers = [row]
    for key in ("sample", "enrichments", "factors", "market_snapshot", "branch", "shadow_branch", "risk_plan"):
        value = row.get(key)
        if isinstance(value, dict):
            containers.append(value)
            if key == "market_snapshot":
                for nested_key in ("sample", "enrichments", "factors"):
                    nested = value.get(nested_key)
                    if isinstance(nested, dict):
                        containers.append(nested)
    for key in ("shadow_branches", "shadow_branch_specs", "branches"):
        value = row.get(key)
        if isinstance(value, list):
            containers.extend(item for item in value if isinstance(item, dict))
        elif isinstance(value, dict):
            containers.extend(item for item in value.values() if isinstance(item, dict))
    return containers


def _first_value_deep(row: dict[str, Any], *keys: str) -> Any:
    for container in _candidate_containers(row):
        for key in keys:
            value = container.get(key)
            if value not in (None, ""):
                return value
    return None


def _first_float_deep(row: dict[str, Any], *keys: str) -> float | None:
    return _safe_float(_first_value_deep(row, *keys))


def _candidate_bias(row: dict[str, Any], side: str) -> str:
    normalized_side = str(side or "").strip().upper()
    if normalized_side in {"BUY", "LONG"}:
        return "LONG"
    if normalized_side in {"SELL", "SHORT"}:
        return "SHORT"
    raw_bias = str(_first_value_deep(row, "bias") or "").strip().upper()
    if raw_bias in {"LONG", "SHORT", "NEUTRAL"}:
        return raw_bias
    candle_direction = str(_first_value_deep(row, "candle_direction", "trigger_candle_direction") or "").strip().lower()
    if candle_direction == "up":
        return "LONG"
    if candle_direction == "down":
        return "SHORT"
    for key in ("momentum_bucket", "trend_bucket", "oi_pressure_bucket", "liquidation_pressure_bucket"):
        bucket = str(_first_value_deep(row, key) or "").strip().lower()
        if "_long" in bucket or bucket.endswith("long"):
            return "LONG"
        if "_short" in bucket or bucket.endswith("short"):
            return "SHORT"
    return "NEUTRAL"


def _candidate_setup_type(row: dict[str, Any]) -> str:
    raw_setup = str(_first_value_deep(row, "setup_type") or "").strip().upper()
    if raw_setup:
        return raw_setup
    trigger_types = row.get("trigger_types")
    if isinstance(trigger_types, list) and any(str(item).strip() for item in trigger_types):
        return "QUICK_TRADE"
    playbook = str(row.get("playbook") or "").strip().lower()
    if "swing" in playbook:
        return "SWING_TRADE"
    if "trend" in playbook:
        return "TREND_HOLD_CANDIDATE"
    return "QUICK_TRADE"


def _liquidity_ok_from_row(row: dict[str, Any], liquidity_bucket: str) -> bool:
    bucket = str(liquidity_bucket or "").strip().lower()
    if bucket:
        return any(token in bucket for token in ("major", "large", "high", "top", "extreme"))
    bid_depth = _first_float_deep(row, "bid_depth_notional_5", "bid_depth_notional")
    ask_depth = _first_float_deep(row, "ask_depth_notional_5", "ask_depth_notional")
    if bid_depth is not None and ask_depth is not None:
        return min(bid_depth, ask_depth) > 0
    quote_volume = _first_float_deep(row, "quote_volume_24h")
    return bool(quote_volume is not None and quote_volume >= 5_000_000.0)


def _candidate_from_runtime_row(row: dict[str, Any], *, symbol: str, side: str, source_label: str) -> dict[str, Any]:
    score = _candidate_score(row)
    spread = _first_float_deep(row, "spread_bps", "spread_bps_at_entry")
    slippage = _first_float_deep(row, "slippage_bps", "estimated_slippage_bps")
    liquidity_bucket = str(_first_value_deep(row, "liquidity_bucket") or "").strip().lower()
    liquidity_ok = row.get("liquidity_ok")
    if liquidity_ok is None:
        liquidity_ok = _liquidity_ok_from_row(row, liquidity_bucket)
    stop_pct, stop_source = _candidate_stop_pct(row, spread=spread, slippage=slippage)
    tp_pct, tp_source = _candidate_take_profit_pct(row, stop_pct=stop_pct)
    max_hold = _candidate_max_holding_time_sec(row)
    bias = _candidate_bias(row, side)
    return {
        "symbol": symbol or "UNKNOWN",
        "bias": bias,
        "setup_type": _candidate_setup_type(row),
        "score": score,
        "current_price": _first_float_deep(
            row,
            "current_price",
            "simulated_entry_price",
            "entry_price",
            "price",
            "sample_price",
            "confirmed_price",
            "trigger_price",
            "mark_price",
        ),
        "spread_bps": spread,
        "estimated_slippage_bps": slippage,
        "liquidity_ok": bool(liquidity_ok),
        "price_action_summary": _candidate_price_action_summary(row, source_label=source_label),
        "order_flow_summary": _candidate_order_flow_summary(row),
        "depth_summary": _candidate_depth_summary(spread, slippage, liquidity_bucket),
        "derivatives_summary": _candidate_derivatives_summary(row),
        "suggested_stop_pct": stop_pct if stop_pct is not None else "unavailable",
        "suggested_tp_pct": tp_pct if tp_pct is not None else "unavailable",
        "max_holding_time_sec": max_hold,
        "invalidation_hint": _candidate_invalidation_hint(row, bias=bias),
        "risk_plan_source": _candidate_risk_plan_source(stop_source, tp_source),
    }


def _candidate_stop_pct(row: dict[str, Any], *, spread: float | None, slippage: float | None) -> tuple[float | None, str | None]:
    direct = _first_float_deep(row, "suggested_stop_pct", "stop_loss_pct", "stop_loss_price_pct")
    if direct is not None and direct > 0:
        return round(direct, 6), "candidate_direct"

    atr_pct = _first_float_deep(row, "atr_20_pct")
    range_pct = _first_float_deep(row, "candle_range_pct")
    body_pct = abs(_first_float_deep(row, "candle_body_pct") or 0.0)
    volatility_inputs = [value for value in (atr_pct, range_pct * 0.7 if range_pct is not None else None, body_pct * 1.2 if body_pct else None) if value is not None and value > 0]
    if not volatility_inputs:
        return None, None

    execution_floor_pct = max(0.25, ((_positive_or_zero(spread) + _positive_or_zero(slippage)) / 100.0) + 0.10)
    stop_pct = max(execution_floor_pct, max(volatility_inputs) * 1.2)
    return round(min(max(stop_pct, 0.25), 3.0), 6), "derived_from_runtime_volatility"


def _candidate_take_profit_pct(row: dict[str, Any], *, stop_pct: float | None) -> tuple[float | None, str | None]:
    direct = _first_float_deep(row, "suggested_tp_pct", "take_profit_pct", "first_take_profit_price_pct")
    if direct is not None and direct > 0:
        return round(direct, 6), "candidate_direct"
    if stop_pct is None:
        return None, None
    range_pct = _first_float_deep(row, "candle_range_pct")
    volatility_target = range_pct * 1.2 if range_pct is not None and range_pct > 0 else None
    tp_pct = max(stop_pct * 1.8, volatility_target or 0.0, stop_pct + 0.20)
    return round(min(max(tp_pct, stop_pct), 6.0), 6), "derived_from_runtime_volatility"


def _candidate_max_holding_time_sec(row: dict[str, Any]) -> int:
    direct = _first_float_deep(row, "max_holding_time_sec", "holding_horizon_sec", "horizon_sec", "max_hold_sec")
    if direct is not None and direct > 0:
        return int(direct)
    horizons = row.get("horizons_sec")
    values: list[float] = []
    if isinstance(horizons, list):
        values = [_safe_float(item) for item in horizons]
    elif isinstance(horizons, dict):
        values = [_safe_float(item) for item in horizons.values()]
    values = [value for value in values if value is not None and value > 0]
    if values:
        return int(max(values))
    max_hold_minutes = _first_float_deep(row, "max_hold_minutes")
    return int((max_hold_minutes or 15) * 60)


def _candidate_invalidation_hint(row: dict[str, Any], *, bias: str) -> str:
    direct = _first_value_deep(row, "invalidation_hint", "invalidation_condition")
    if direct:
        return str(direct)
    triggers = row.get("trigger_types")
    trigger_text = ", ".join(str(item) for item in triggers if str(item).strip()) if isinstance(triggers, list) else None
    direction_hint = "long momentum fades" if bias == "LONG" else "short momentum fades" if bias == "SHORT" else "direction stays unclear"
    parts = [direction_hint, "source signal becomes stale", "spread/slippage widens", "liquidity deteriorates"]
    if trigger_text:
        parts.insert(1, f"trigger weakens ({trigger_text})")
    return "candidate invalidates if " + "; ".join(parts)


def _candidate_risk_plan_source(stop_source: str | None, tp_source: str | None) -> str:
    sources = {source for source in (stop_source, tp_source) if source}
    if not sources:
        return "unavailable"
    if sources == {"candidate_direct"}:
        return "candidate_direct"
    return "derived_from_runtime_candidate_fields"


def _positive_or_zero(value: float | None) -> float:
    return max(0.0, float(value or 0.0))


def _candidate_timestamp(row: dict[str, Any]) -> str | None:
    market_snapshot = row.get("market_snapshot") if isinstance(row.get("market_snapshot"), dict) else {}
    return _coerce_timestamp_value(
        row.get("signal_time")
        or row.get("simulated_entry_time")
        or row.get("observed_at")
        or row.get("created_at")
        or row.get("generated_at")
        or row.get("signal_time_ms")
        or row.get("observed_at_ms")
        or market_snapshot.get("observed_at_ms")
        or market_snapshot.get("updated_at")
    )


def _candidate_score(row: dict[str, Any]) -> float:
    direct = _first_float_deep(row, "score", "trigger_score", "confidence")
    if direct is not None:
        return direct
    ret = abs(
        _first_float_deep(row, "one_min_return_pct", "ret_1bar_pct", "ret_5bar_pct", "candle_body_pct") or 0.0
    )
    volume = _first_float_deep(row, "one_min_volume_burst_ratio", "volume_burst_ratio") or 0.0
    oi = abs(_first_float_deep(row, "one_min_oi_change_pct", "five_min_oi_change_pct", "oi_change_5m_pct") or 0.0)
    spread = _first_float_deep(row, "spread_bps", "spread_bps_at_entry") or 0.0
    slippage = _first_float_deep(row, "estimated_slippage_bps", "slippage_bps") or 0.0
    return round(max(0.0, ret * 20.0 + volume * 10.0 + oi * 5.0 - spread - slippage), 6)


def _candidate_price_action_summary(row: dict[str, Any], *, source_label: str) -> str:
    ret = _first_float_deep(row, "one_min_return_pct", "ret_1bar_pct", "ret_5bar_pct")
    volume = _first_float_deep(row, "one_min_volume_burst_ratio", "volume_burst_ratio")
    if ret is None and volume is None:
        return f"{source_label}; price action unavailable"
    parts = [source_label]
    if ret is not None:
        parts.append(f"1m return {ret:.4f}%")
    if volume is not None:
        parts.append(f"1m volume burst {volume:.4f}x")
    return "; ".join(parts)


def _candidate_order_flow_summary(row: dict[str, Any]) -> str:
    direction = _first_value_deep(row, "one_min_candle_direction", "candle_direction", "oi5_direction_bucket", "oi_pressure_bucket")
    return str(direction or "unavailable")


def _candidate_depth_summary(spread: float | None, slippage: float | None, liquidity_bucket: str) -> str:
    parts: list[str] = []
    if spread is not None:
        parts.append(f"spread {spread:.4f} bps")
    if slippage is not None:
        parts.append(f"slippage {slippage:.4f} bps")
    if liquidity_bucket:
        parts.append(f"liquidity {liquidity_bucket}")
    return "; ".join(parts) if parts else "unavailable"


def _candidate_derivatives_summary(row: dict[str, Any]) -> str:
    oi_1m = _first_float_deep(row, "one_min_oi_change_pct")
    oi_5m = _first_float_deep(row, "five_min_oi_change_pct", "oi_change_5m_pct")
    funding = _first_float_deep(row, "funding_rate_at_entry", "funding_rate")
    parts: list[str] = []
    if oi_1m is not None:
        parts.append(f"1m OI {oi_1m:.4f}%")
    if oi_5m is not None:
        parts.append(f"5m OI {oi_5m:.4f}%")
    if funding is not None:
        parts.append(f"funding {funding:.8f}")
    return "; ".join(parts) if parts else "unavailable"


def _read_jsonl_tail(path: Path, *, max_lines: int) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            read_size = min(max(8192, max_lines * 4096), size)
            handle.seek(size - read_size)
            raw = handle.read(read_size)
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines()[-max_lines:]:
        if not line.strip():
            continue
        try:
            row = json.loads(line.decode("utf-8", errors="replace"))
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _dashboard_positions(rows: list[Any]) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        positions.append(
            {
                "symbol": str(row.get("symbol") or "UNKNOWN").upper(),
                "side": str(row.get("side") or "UNKNOWN").upper(),
                "entry_price": _first_float(row, "entryPrice", "entry_price"),
                "current_price": _first_float(row, "markPrice", "current_price"),
                "unrealized_pnl_pct": row.get("unrealized_pnl_pct", "unavailable"),
                "unrealized_pnl_usdt": _first_float(row, "unrealizedPnl", "unrealized_pnl_usdt"),
                "stop_loss_price": row.get("stop_loss_price", "unavailable"),
                "take_profit_price": row.get("take_profit_price", "unavailable"),
                "time_in_trade_sec": int(_safe_float(row.get("time_in_trade_sec")) or 0),
                "protection_status": str(row.get("protection_status") or "unknown"),
            }
        )
    return positions


def _load_candidates(
    root: Path,
    candidate_files: list[str | Path] | tuple[str | Path, ...] | None,
) -> tuple[dict[str, Any] | None, str]:
    paths = tuple(Path(item) for item in candidate_files) if candidate_files is not None else DEFAULT_CANDIDATE_FILES
    for path in paths:
        resolved = path if path.is_absolute() else root / path
        payload = _read_json(resolved)
        if payload is not None:
            return payload, str(resolved)
    return None, "unavailable"


def _candidate_rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("top_candidates")
    if not isinstance(rows, list):
        return []
    return [dict(item) for item in rows if isinstance(item, dict)]


def _build_market_data(
    market_stream: dict[str, Any] | None,
    engine_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    btc = _symbol_market(market_stream, "BTCUSDT")
    eth = _symbol_market(market_stream, "ETHUSDT")
    btc_1m = _safe_float(btc.get("price_change_1m_pct")) or _safe_float(btc.get("price_change_5m_pct"))
    btc_5m = _safe_float(btc.get("price_change_5m_pct")) or _safe_float(btc.get("price_change_15m_pct"))
    eth_1m = _safe_float(eth.get("price_change_1m_pct")) or _safe_float(eth.get("price_change_5m_pct"))
    eth_5m = _safe_float(eth.get("price_change_5m_pct")) or _safe_float(eth.get("price_change_15m_pct"))
    volatility = _runtime_value(engine_snapshot, "volatility_15m") or _estimate_volatility([btc, eth])
    regime = _runtime_value(engine_snapshot, "regime") or _infer_regime(
        btc_1m=btc_1m,
        btc_5m=btc_5m,
        eth_1m=eth_1m,
        eth_5m=eth_5m,
        volatility=volatility,
    )
    return {
        "regime": regime,
        "btc_trend_1m": btc_1m,
        "btc_trend_5m": btc_5m,
        "eth_trend_1m": eth_1m,
        "eth_trend_5m": eth_5m,
        "volatility": volatility,
    }


def _symbol_market(market_stream: dict[str, Any] | None, symbol: str) -> dict[str, Any]:
    if not isinstance(market_stream, dict):
        return {}
    per_symbol = market_stream.get("per_symbol")
    if isinstance(per_symbol, dict) and isinstance(per_symbol.get(symbol), dict):
        return dict(per_symbol[symbol])
    if str(market_stream.get("symbol") or "").upper() == symbol:
        return dict(market_stream)
    return {}


def _merge_candidates_with_market(
    candidates: list[dict[str, Any]],
    market_stream: dict[str, Any] | None,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for item in candidates[: max(1, min(10, int(limit or 10)))]:
        symbol = str(item.get("symbol") or "").upper()
        market = _symbol_market(market_stream, symbol)
        depth = market.get("depth") if isinstance(market.get("depth"), dict) else {}
        directional_bias = str(item.get("directional_bias") or item.get("bias") or "NEUTRAL").upper()
        bias = {"LONG": "LONG", "SHORT": "SHORT", "NONE": "NEUTRAL"}.get(directional_bias, directional_bias)
        spread = _first_float(item, "spread_bps", "spread_bps_top") or _safe_float(depth.get("spread_bps"))
        slippage = _first_float(item, "estimated_slippage_bps")
        if slippage is None:
            if bias == "SHORT":
                slippage = _safe_float(depth.get("estimated_slippage_bps_sell"))
            else:
                slippage = _safe_float(depth.get("estimated_slippage_bps_buy"))
        merged.append(
            {
                **item,
                "symbol": symbol or "UNKNOWN",
                "bias": bias if bias in {"LONG", "SHORT", "NEUTRAL"} else "NEUTRAL",
                "setup_type": str(item.get("setup_type") or "QUICK_TRADE").upper(),
                "current_price": _first_float(item, "current_price", "mark_price") or _safe_float(market.get("mid_price")),
                "spread_bps": spread,
                "estimated_slippage_bps": slippage,
                "liquidity_ok": _liquidity_ok(depth, spread, slippage, item),
                "price_action_summary": _price_action_summary(item, market),
                "order_flow_summary": _order_flow_summary(item, market),
                "depth_summary": _depth_summary(depth),
                "derivatives_summary": _derivatives_summary(item),
                "suggested_stop_pct": _first_float(item, "suggested_stop_pct", "stop_loss_pct") or 0.6,
                "suggested_tp_pct": _first_float(item, "suggested_tp_pct", "take_profit_pct") or 1.2,
                "max_holding_time_sec": int(_first_float(item, "max_holding_time_sec") or 900),
                "invalidation_hint": str(item.get("invalidation_hint") or "breakout fails or spread/slippage widens"),
            }
        )
    return merged


def _positions_from_sources(
    *,
    signed_positions: list[dict[str, Any]] | None,
    active_trade: dict[str, Any] | None,
    paper_active_trade: dict[str, Any] | None,
    market_stream: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if signed_positions is not None:
        return [_position_from_exchange(item, market_stream) for item in signed_positions if _is_open_position(item)]
    rows: list[dict[str, Any]] = []
    for item in (active_trade, paper_active_trade):
        if isinstance(item, dict) and str(item.get("status") or "").lower() not in {"", "closed", "cleared"}:
            rows.append(_position_from_active_trade(item, market_stream))
    return rows


def _build_account_state(
    *,
    signed_account_state: dict[str, Any] | None,
    demo_account: dict[str, Any] | None,
    current_positions: list[dict[str, Any]],
    last_close: dict[str, Any] | None,
    max_open_positions: int,
) -> dict[str, Any]:
    daily_pnl_pct = _safe_float((demo_account or {}).get("daily_pnl_pct"))
    if daily_pnl_pct is None:
        daily_pnl_pct = _safe_float((signed_account_state or {}).get("daily_pnl_pct")) or 0.0
    loss_streak = int(_safe_float((demo_account or {}).get("loss_streak")) or _loss_streak_from_last_close(last_close))
    cooldown_active = bool((demo_account or {}).get("cooldown_active", False))
    return {
        "trading_allowed": len(current_positions) < max(1, int(max_open_positions or 1)) and not cooldown_active,
        "daily_pnl_pct": daily_pnl_pct,
        "max_daily_loss_pct": abs(_safe_float((signed_account_state or {}).get("max_daily_loss_pct")) or 3.0),
        "open_positions_count": len(current_positions),
        "max_open_positions": max(1, int(max_open_positions or 1)),
        "loss_streak": loss_streak,
        "cooldown_active": cooldown_active,
        "reason_if_blocked": "cooldown_active" if cooldown_active else None,
    }


def _build_system_status(
    *,
    market_stream: dict[str, Any] | None,
    market_state: dict[str, Any] | None,
    engine_snapshot: dict[str, Any] | None,
    candidate_source: str,
    account_source: str,
    position_state_source: str,
    stale_after_sec: int,
    candidates: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    position_state_known: bool,
    protective_stop_path_available: bool,
    take_profit_path_available: bool,
    emergency_close_available: bool,
    protective_stop_capability_source: str,
    take_profit_capability_source: str,
    emergency_close_capability_source: str,
) -> dict[str, Any]:
    generated_at = _latest_timestamp(
        _runtime_value(market_stream, "generated_at"),
        _runtime_value(market_stream, "last_event_at"),
        _runtime_value(market_state, "last_event_at"),
        _runtime_value(engine_snapshot, "generated_at"),
    )
    age = _age_sec(generated_at)
    stream_status = str((market_state or {}).get("status") or ("connected" if market_stream else "unavailable")).lower()
    websocket_status = "healthy" if stream_status in {"connected", "running", "healthy"} else "unavailable"
    data_fresh = age is not None and age <= max(1, int(stale_after_sec or 60)) and bool(candidates)
    trusted = (
        bool(data_fresh)
        and account_source == "signed_account"
        and position_state_source == "signed_positions"
        and position_state_known
        and bool(protective_stop_path_available)
        and bool(take_profit_path_available)
        and bool(emergency_close_available)
        and _verified_capability_source(protective_stop_capability_source)
        and _verified_capability_source(take_profit_capability_source)
        and _verified_capability_source(emergency_close_capability_source)
    )
    return {
        "snapshot_time": generated_at or datetime.now(timezone.utc).isoformat(),
        "data_fresh": bool(data_fresh),
        "data_age_sec": age,
        "websocket_status": websocket_status,
        "exchange_status": "healthy" if market_stream else "unavailable",
        "source": "runtime",
        "snapshot_source": "runtime",
        "trusted_runtime_snapshot": trusted,
        "candidate_source": candidate_source,
        "account_source": account_source,
        "account_state_source": account_source,
        "position_state_source": position_state_source,
        "stale_after_sec": stale_after_sec,
        "position_state": "known" if position_state_known else "unknown",
        "stop_protection_status": _protection_status(positions),
        "candidate_state": "known" if candidates else "unavailable",
        "protective_stop_path_available": bool(protective_stop_path_available),
        "take_profit_path_available": bool(take_profit_path_available),
        "take_profit_order_supported": bool(
            take_profit_path_available and _verified_capability_source(take_profit_capability_source)
        ),
        "emergency_close_available": bool(emergency_close_available),
        "protective_stop_capability_source": protective_stop_capability_source,
        "take_profit_capability_source": take_profit_capability_source,
        "emergency_close_capability_source": emergency_close_capability_source,
        "can_continue": trusted,
        "freeze_reason": None if trusted else "runtime_snapshot_not_trusted_for_testnet_order",
    }


def _position_from_exchange(item: dict[str, Any], market_stream: dict[str, Any] | None) -> dict[str, Any]:
    symbol = str(item.get("symbol") or "UNKNOWN").upper()
    amount = _position_amount(item)
    side = "LONG" if amount >= 0 else "SHORT"
    entry_price = _first_float(item, "entryPrice", "entry_price")
    current_price = _first_float(item, "markPrice", "mark_price") or _safe_float(_symbol_market(market_stream, symbol).get("mid_price"))
    unrealized = _first_float(item, "unRealizedProfit", "unrealizedProfit", "unrealized_pnl_usdt")
    pnl_pct = None
    if entry_price and current_price:
        sign = 1.0 if side == "LONG" else -1.0
        pnl_pct = ((current_price - entry_price) / entry_price) * 100.0 * sign
    return {
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "current_price": current_price,
        "unrealized_pnl_pct": pnl_pct,
        "unrealized_pnl_usdt": unrealized,
        "stop_loss_price": _first_float(item, "stop_loss_price", "initial_stop_price"),
        "take_profit_price": _first_float(item, "take_profit_price"),
        "time_in_trade_sec": int(_first_float(item, "time_in_trade_sec") or 0),
        "protection_status": str(item.get("protection_status") or "unknown"),
    }


def _position_from_active_trade(item: dict[str, Any], market_stream: dict[str, Any] | None) -> dict[str, Any]:
    intent = item.get("intent") if isinstance(item.get("intent"), dict) else item
    symbol = str(intent.get("symbol") or item.get("symbol") or "UNKNOWN").upper()
    side = "LONG" if str(intent.get("side") or item.get("side") or "BUY").upper() == "BUY" else "SHORT"
    current_price = _safe_float(_symbol_market(market_stream, symbol).get("mid_price"))
    return {
        "symbol": symbol,
        "side": side,
        "entry_price": _first_float(intent, "entry_price"),
        "current_price": current_price,
        "unrealized_pnl_pct": "unavailable",
        "unrealized_pnl_usdt": "unavailable",
        "stop_loss_price": _first_float(intent, "initial_stop_price", "stop_loss_price"),
        "take_profit_price": _first_float(intent, "take_profit_price"),
        "time_in_trade_sec": int(_age_sec(item.get("created_at")) or 0),
        "protection_status": str(item.get("phase") or item.get("protection_status") or "unknown"),
    }


def _is_open_position(item: dict[str, Any]) -> bool:
    return abs(_position_amount(item)) > 1e-12


def _position_amount(item: dict[str, Any]) -> float:
    return _first_float(item, "positionAmt", "positionQty", "quantity") or 0.0


def _runtime_value(payload: dict[str, Any] | None, key: str) -> Any:
    return payload.get(key) if isinstance(payload, dict) else None


def _latest_timestamp(*values: Any) -> str | None:
    parsed = [(_parse_dt(item), str(item)) for item in values if item not in (None, "")]
    parsed = [(dt, raw) for dt, raw in parsed if dt is not None]
    if not parsed:
        return None
    return max(parsed, key=lambda item: item[0])[1]


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_sec(value: Any) -> float | None:
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
        value = _coerce_timestamp_value(value)
    parsed = _parse_dt(value)
    if parsed is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def _compact_account_state(payload: Any) -> dict[str, Any]:
    available = None
    wallet = None
    if isinstance(payload, dict):
        available = _safe_float(payload.get("totalAvailableBalance") or payload.get("virtualMaxWithdrawAmount"))
        wallet = _safe_float(payload.get("totalWalletBalance") or payload.get("accountEquity"))
    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict) or str(item.get("asset") or "").upper() != "USDT":
                continue
            available = _safe_float(item.get("availableBalance") or item.get("balance"))
            wallet = _safe_float(item.get("balance") or item.get("crossWalletBalance"))
            break
    return {
        "available_balance_usdt": available,
        "wallet_balance_usdt": wallet,
        "daily_pnl_pct": 0.0,
        "max_daily_loss_pct": 3.0,
    }


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _safe_float(payload.get(key))
        if value is not None:
            return value
    return None


def _infer_regime(
    *,
    btc_1m: float | None,
    btc_5m: float | None,
    eth_1m: float | None,
    eth_5m: float | None,
    volatility: float | None,
) -> str:
    if volatility is not None and volatility >= 2.5:
        return "HIGH_VOL"
    values = [item for item in (btc_1m, btc_5m, eth_1m, eth_5m) if item is not None]
    if len(values) < 2:
        return "UNKNOWN"
    if sum(1 for item in values if item > 0) >= max(2, len(values) - 1):
        return "TREND_UP"
    if sum(1 for item in values if item < 0) >= max(2, len(values) - 1):
        return "TREND_DOWN"
    return "CHOP"


def _estimate_volatility(symbols: list[dict[str, Any]]) -> float | None:
    changes = []
    for item in symbols:
        for key in ("price_change_5m_pct", "price_change_15m_pct", "price_change_1h_pct"):
            value = _safe_float(item.get(key))
            if value is not None:
                changes.append(abs(value))
    if not changes:
        return None
    return round(max(changes), 6)


def _liquidity_ok(depth: dict[str, Any], spread: float | None, slippage: float | None, item: dict[str, Any]) -> bool:
    if item.get("liquidity_ok") is False:
        return False
    if spread is not None and spread > 12.0:
        return False
    if slippage is not None and slippage > 8.0:
        return False
    bid = _safe_float(depth.get("depth_bid_5")) or 0.0
    ask = _safe_float(depth.get("depth_ask_5")) or 0.0
    return (bid + ask) > 0 if depth else True


def _price_action_summary(item: dict[str, Any], market: dict[str, Any]) -> str:
    change_5m = _safe_float(item.get("price_change_5m_pct")) or _safe_float(market.get("price_change_5m_pct"))
    change_15m = _safe_float(item.get("price_change_15m_pct")) or _safe_float(market.get("price_change_15m_pct"))
    return f"5m={_fmt(change_5m)}%, 15m={_fmt(change_15m)}%"


def _order_flow_summary(item: dict[str, Any], market: dict[str, Any]) -> str:
    flow = _safe_float(item.get("aggressive_flow_delta")) or _safe_float(market.get("ofi_window_sum"))
    buy_ratio = _safe_float(item.get("taker_buy_ratio_5m"))
    return f"flow={_fmt(flow)}, taker_buy_ratio_5m={_fmt(buy_ratio)}"


def _depth_summary(depth: dict[str, Any]) -> str:
    if not depth:
        return "unavailable"
    return (
        f"spread={_fmt(depth.get('spread_bps'))}bps, "
        f"imbalance={_fmt(depth.get('depth_imbalance'))}, "
        f"bid5={_fmt(depth.get('depth_bid_5'))}, ask5={_fmt(depth.get('depth_ask_5'))}"
    )


def _derivatives_summary(item: dict[str, Any]) -> str:
    return (
        f"oi_1m={_fmt(item.get('oi_delta_1m_pct'))}%, "
        f"oi_5m={_fmt(item.get('oi_delta_5m_pct'))}%, "
        f"funding={_fmt(item.get('funding_rate'))}"
    )


def _fmt(value: Any) -> str:
    number = _safe_float(value)
    return "unavailable" if number is None else f"{number:.4f}"


def _loss_streak_from_last_close(last_close: dict[str, Any] | None) -> int:
    if not isinstance(last_close, dict):
        return 0
    outcome = str(last_close.get("outcome") or last_close.get("exit_reason") or "").lower()
    return 1 if "loss" in outcome or "stop" in outcome else 0


def _protection_status(positions: list[dict[str, Any]]) -> str:
    if not positions:
        return "healthy"
    statuses = {str(item.get("protection_status") or "unknown").lower() for item in positions}
    if statuses <= {"healthy", "initial_stop", "breakeven_only", "breakeven_and_trailing_armed"}:
        return "healthy"
    return "unknown"


def _verified_capability_source(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text not in {"", "unverified", "manual", "manual_payload", "mock", "file", "unknown"}
