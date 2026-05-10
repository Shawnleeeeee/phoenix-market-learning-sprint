from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from deploy.hermes.binance_skill_context import load_json


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_market_stream_state(home: Path) -> dict[str, Any] | None:
    return load_json(home / "memories" / "btc_demo_v2" / "market_stream_state.json")


def load_market_stream_snapshot(home: Path) -> dict[str, Any] | None:
    return load_json(home / "memories" / "btc_demo_v2" / "market_stream_snapshot.json")


def market_stream_view(home: Path, *, symbol: str | None = None) -> dict[str, Any]:
    state = load_market_stream_state(home) or {}
    snapshot = load_market_stream_snapshot(home) or {}
    if not state and not snapshot:
        return {}
    effective_snapshot = snapshot
    if symbol:
        per_symbol = snapshot.get("per_symbol") if isinstance(snapshot.get("per_symbol"), dict) else {}
        symbol_snapshot = per_symbol.get(str(symbol).upper()) if isinstance(per_symbol, dict) else None
        if isinstance(symbol_snapshot, dict):
            effective_snapshot = {
                **snapshot,
                **symbol_snapshot,
                "symbol": str(symbol).upper(),
            }
    depth = effective_snapshot.get("depth") if isinstance(effective_snapshot.get("depth"), dict) else {}
    return {
        "state": state,
        "snapshot": effective_snapshot,
        "depth": depth,
    }


def market_stream_markdown_lines(home: Path, *, symbol: str | None = None) -> list[str]:
    view = market_stream_view(home, symbol=symbol)
    if not view:
        return []
    state = view["state"]
    snapshot = view["snapshot"]
    depth = view["depth"]
    lines = [
        "",
        "## 雷霆市场流",
        f"- 状态：`{state.get('status') or 'n/a'}` / 数据源：`{snapshot.get('source') or 'n/a'}`",
        f"- 标的：`{snapshot.get('symbol') or symbol or 'n/a'}`",
        f"- 最近事件：`{state.get('last_event_type') or snapshot.get('last_event_type') or 'n/a'}` @ `{state.get('last_event_at') or snapshot.get('last_event_at') or 'n/a'}`",
        f"- 最新成交：`{state.get('last_trade_at') or snapshot.get('last_trade_at') or 'n/a'}` / 最新 K 线：`{state.get('last_kline_at') or snapshot.get('last_kline_at') or 'n/a'}`",
        f"- 当前点差：`{depth.get('spread_bps') if depth.get('spread_bps') is not None else 'n/a'}` bps / 盘口失衡：`{depth.get('depth_imbalance') if depth.get('depth_imbalance') is not None else 'n/a'}`",
        f"- 5m 涨幅：`{snapshot.get('price_change_5m_pct') if snapshot.get('price_change_5m_pct') is not None else 'n/a'}`% / 1h 涨幅：`{snapshot.get('price_change_1h_pct') if snapshot.get('price_change_1h_pct') is not None else 'n/a'}`%",
    ]
    return lines


def market_stream_telegram_lines(home: Path, *, symbol: str | None = None) -> list[str]:
    view = market_stream_view(home, symbol=symbol)
    if not view:
        return []
    state = view["state"]
    snapshot = view["snapshot"]
    depth = view["depth"]
    return [
        "雷霆市场流：",
        f"- 状态：{state.get('status') or 'n/a'} / 源：{snapshot.get('source') or 'n/a'}",
        f"- 标的：{snapshot.get('symbol') or symbol or 'n/a'}",
        f"- 最近事件：{state.get('last_event_type') or snapshot.get('last_event_type') or 'n/a'}",
        f"- 点差：{depth.get('spread_bps') if depth.get('spread_bps') is not None else 'n/a'} bps / 失衡：{depth.get('depth_imbalance') if depth.get('depth_imbalance') is not None else 'n/a'}",
    ]


def filter_recent_ws_rejections(
    rejection_payload: dict[str, Any] | None,
    market_stream_state: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(rejection_payload, dict):
        return []
    recent = rejection_payload.get("recent_rejections")
    if not isinstance(recent, list):
        return []
    started_at = _parse_dt((market_stream_state or {}).get("started_at"))
    filtered: list[dict[str, Any]] = []
    for item in recent:
        if not isinstance(item, dict):
            continue
        market_data_source = str(item.get("market_data_source") or "").strip().lower()
        if market_data_source == "ws":
            filtered.append(item)
            continue
        timestamp = _parse_dt(item.get("timestamp"))
        if started_at is not None and timestamp is not None and timestamp >= started_at:
            filtered.append(item)
    return filtered
