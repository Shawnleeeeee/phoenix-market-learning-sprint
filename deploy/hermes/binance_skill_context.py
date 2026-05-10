from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def load_skill_digest(home: Path) -> dict[str, Any] | None:
    return load_json(home / "memories" / "binance_skills" / "digest_latest.json")


def _signal_summary(labels: list[str], raw_count: Any) -> str:
    try:
        count = int(raw_count) if raw_count is not None else None
    except (TypeError, ValueError):
        count = None
    if labels:
        if count is not None and count > len(labels):
            return f"{', '.join(labels)}（当前抓取 {count} 条）"
        return ", ".join(labels)
    if count is None:
        return "暂无"
    return f"暂无（当前抓取 {count} 条）"


def markdown_lines_from_digest(digest: dict[str, Any] | None) -> list[str]:
    if not isinstance(digest, dict):
        return []
    assets = digest.get("assets") or {}
    rank = digest.get("market_rank") or {}
    trading_signal = digest.get("trading_signal") or {}
    long_summary = _signal_summary(list(trading_signal.get("top_long") or []), trading_signal.get("raw_long_count"))
    short_summary = _signal_summary(list(trading_signal.get("top_short") or []), trading_signal.get("raw_short_count"))
    return [
        "",
        "## 外部市场参考",
        f"- 交易所 Demo 账户总权益：`{assets.get('equity_usdt') if assets.get('equity_usdt') is not None else 'n/a'}` USDT",
        f"- 交易所 Demo 可用余额：`{assets.get('available_balance_usdt') if assets.get('available_balance_usdt') is not None else 'n/a'}` USDT",
        f"- 交易所 Demo 未实现盈亏：`{assets.get('unrealized_pnl_usdt') if assets.get('unrealized_pnl_usdt') is not None else 'n/a'}` USDT",
        f"- Trending：`{', '.join(rank.get('top_trending') or []) or 'n/a'}`",
        f"- Top Search：`{', '.join(rank.get('top_search') or []) or 'n/a'}`",
        f"- Alpha：`{', '.join(rank.get('top_alpha') or []) or 'n/a'}`",
        f"- Smart Money：`{', '.join(rank.get('top_smart_money') or []) or 'n/a'}`",
        f"- 多头信号：`{long_summary}`",
        f"- 空头信号：`{short_summary}`",
    ]


def telegram_lines_from_digest(digest: dict[str, Any] | None) -> list[str]:
    if not isinstance(digest, dict):
        return []
    assets = digest.get("assets") or {}
    rank = digest.get("market_rank") or {}
    trading_signal = digest.get("trading_signal") or {}
    long_summary = _signal_summary(list(trading_signal.get("top_long") or []), trading_signal.get("raw_long_count"))
    short_summary = _signal_summary(list(trading_signal.get("top_short") or []), trading_signal.get("raw_short_count"))
    return [
        "外部市场参考：",
        f"- 交易所 Demo 总权益：{assets.get('equity_usdt') if assets.get('equity_usdt') is not None else 'n/a'} USDT",
        f"- 交易所 Demo 可用余额：{assets.get('available_balance_usdt') if assets.get('available_balance_usdt') is not None else 'n/a'} USDT",
        f"- Trending：{', '.join(rank.get('top_trending') or []) or 'n/a'}",
        f"- Top Search：{', '.join(rank.get('top_search') or []) or 'n/a'}",
        f"- 多头信号：{long_summary}",
        f"- 空头信号：{short_summary}",
    ]
