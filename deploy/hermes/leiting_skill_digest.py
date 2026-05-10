#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from deploy.hermes.hermes_telegram import send_telegram_message


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hermes digest from Binance skill exports.")
    parser.add_argument("--hermes-home", default=os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes"))
    parser.add_argument("--notify-telegram", action="store_true")
    return parser.parse_args()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _strip_matching_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_env_file(home: Path) -> dict[str, str]:
    env_path = home / ".env"
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _strip_matching_quotes(value.strip())
    return values


def load_config_values(home: Path) -> dict[str, str]:
    config_path = home / "config.yaml"
    values: dict[str, str] = {}
    if not config_path.exists():
        return values
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if key not in {
            "TELEGRAM_ALLOWED_USERS",
            "TELEGRAM_HOME_CHANNEL",
            "TELEGRAM_PROXY",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "TELEGRAM_BOT_TOKEN",
        }:
            continue
        values[key] = value.strip().strip("'").strip('"')
    return values


def get_setting(home: Path, key: str, default: str = "") -> str:
    if os.environ.get(key):
        return str(os.environ[key]).strip()
    env = load_env_file(home)
    if env.get(key):
        return env[key].strip()
    cfg = load_config_values(home)
    if cfg.get(key):
        return cfg[key].strip()
    return default


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _first_number(payload: dict[str, Any] | None, keys: list[str]) -> float | None:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _top_symbols(items: Any, limit: int = 5) -> list[str]:
    if not isinstance(items, list):
        return []
    symbols: list[str] = []
    for item in items[:limit]:
        if isinstance(item, dict):
            symbol = item.get("symbol") or item.get("token") or item.get("asset") or item.get("name")
            if symbol:
                symbols.append(str(symbol))
    return symbols


def _normalize_signal_items(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    def _coerce_items(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        return [item for item in raw if isinstance(item, dict)]

    long_candidates = (
        _coerce_items(payload.get("long"))
        or _coerce_items(payload.get("long_signals"))
        or _coerce_items(payload.get("buy"))
        or _coerce_items(payload.get("buy_signals"))
        or _coerce_items(payload.get("bullish"))
        or _coerce_items(payload.get("top_long"))
        or _coerce_items(payload.get("top_buy"))
    )
    short_candidates = (
        _coerce_items(payload.get("short"))
        or _coerce_items(payload.get("short_signals"))
        or _coerce_items(payload.get("sell"))
        or _coerce_items(payload.get("sell_signals"))
        or _coerce_items(payload.get("bearish"))
        or _coerce_items(payload.get("top_short"))
        or _coerce_items(payload.get("top_sell"))
    )
    if long_candidates or short_candidates:
        return long_candidates, short_candidates

    generic = _coerce_items(payload.get("signals"))
    long_items: list[dict[str, Any]] = []
    short_items: list[dict[str, Any]] = []
    for item in generic:
        direction = str(
            item.get("direction")
            or item.get("signal")
            or item.get("side")
            or item.get("bias")
            or ""
        ).strip().lower()
        if direction in {"long", "buy", "bullish"}:
            long_items.append(item)
        elif direction in {"short", "sell", "bearish"}:
            short_items.append(item)
    return long_items, short_items


def _signal_entry_label(item: dict[str, Any]) -> str | None:
    symbol = item.get("symbol") or item.get("token") or item.get("asset") or item.get("name")
    if not symbol:
        return None
    score = item.get("score")
    confidence = item.get("confidence")
    strength = item.get("strength")
    numeric = None
    for candidate in (score, confidence, strength):
        if candidate in (None, ""):
            continue
        try:
            numeric = float(candidate)
            break
        except (TypeError, ValueError):
            continue
    if numeric is None:
        return str(symbol)
    return f"{symbol}({numeric:.2f})"


def _signal_labels(items: list[dict[str, Any]], limit: int = 5) -> list[str]:
    labels: list[str] = []
    for item in items[:limit]:
        label = _signal_entry_label(item)
        if label:
            labels.append(label)
    return labels


def _render_signal_summary(labels: list[str], raw_count: int | None) -> str:
    if labels:
        if raw_count is not None and raw_count > len(labels):
            return f"{', '.join(labels)}（当前抓取 {raw_count} 条）"
        return ", ".join(labels)
    if raw_count is None:
        return "暂无"
    return f"暂无（当前抓取 {raw_count} 条）"


def build_digest(home: Path) -> tuple[dict[str, Any], str]:
    memory_dir = home / "memories" / "binance_skills"
    assets = load_json(memory_dir / "assets_latest.json") or {}
    market_rank = load_json(memory_dir / "crypto_market_rank_latest.json") or {}
    trading_signal = load_json(memory_dir / "trading_signal_latest.json") or {}

    assets_equity = _first_number(assets, ["equity_usdt", "total_equity_usdt", "wallet_balance_usdt", "total_wallet_balance"])
    assets_available = _first_number(assets, ["available_balance_usdt", "available_usdt", "availableBalance"])
    assets_unrealized = _first_number(assets, ["unrealized_pnl_usdt", "unrealizedProfit"])
    top_trending = _top_symbols(market_rank.get("trending"), limit=5)
    top_search = _top_symbols(market_rank.get("top_search"), limit=5)
    top_alpha = _top_symbols(market_rank.get("alpha"), limit=5)
    top_smart_money = _top_symbols(market_rank.get("smart_money"), limit=5)
    top_long_signal_items, top_short_signal_items = _normalize_signal_items(trading_signal)
    top_long_signals = _signal_labels(top_long_signal_items, limit=5)
    top_short_signals = _signal_labels(top_short_signal_items, limit=5)
    long_signal_summary = _render_signal_summary(top_long_signals, len(top_long_signal_items))
    short_signal_summary = _render_signal_summary(top_short_signals, len(top_short_signal_items))

    digest = {
        "generated_at": _utc_now(),
        "assets_present": bool(assets),
        "market_rank_present": bool(market_rank),
        "trading_signal_present": bool(trading_signal),
        "total_equity_usdt": assets_equity,
        "available_balance_usdt": assets_available,
        "unrealized_pnl_usdt": assets_unrealized,
        "trending_top_symbols": top_trending,
        "top_search_symbols": top_search,
        "alpha_top_symbols": top_alpha,
        "smart_money_top_symbols": top_smart_money,
        "trading_signal_long_top_symbols": top_long_signals,
        "trading_signal_short_top_symbols": top_short_signals,
        "trading_signal_long_summary": long_signal_summary,
        "trading_signal_short_summary": short_signal_summary,
        "assets": {
            "equity_usdt": assets_equity,
            "available_balance_usdt": assets_available,
            "unrealized_pnl_usdt": assets_unrealized,
            "distribution": assets.get("distribution") if isinstance(assets.get("distribution"), list) else [],
        },
        "market_rank": {
            "top_trending": top_trending,
            "top_search": top_search,
            "top_alpha": top_alpha,
            "top_smart_money": top_smart_money,
        },
        "trading_signal": {
            "top_long": top_long_signals,
            "top_short": top_short_signals,
            "raw_long_count": len(top_long_signal_items),
            "raw_short_count": len(top_short_signal_items),
        },
        "input_files": {
            "assets": str(memory_dir / "assets_latest.json"),
            "crypto_market_rank": str(memory_dir / "crypto_market_rank_latest.json"),
            "trading_signal": str(memory_dir / "trading_signal_latest.json"),
        },
    }

    lines = [
        "# 外部市场参考",
        "",
        f"- 生成时间：{digest['generated_at']}",
        f"- Assets 输入：`{'已检测' if digest['assets_present'] else '未检测'}`",
        f"- Crypto Market Rank 输入：`{'已检测' if digest['market_rank_present'] else '未检测'}`",
        f"- Trading Signal 输入：`{'已检测' if digest['trading_signal_present'] else '未检测'}`",
        "",
        "## 资产概览",
        f"- 交易所 Demo 账户总权益：`{assets_equity if assets_equity is not None else 'n/a'}` USDT",
        f"- 交易所 Demo 可用余额：`{assets_available if assets_available is not None else 'n/a'}` USDT",
        f"- 交易所 Demo 未实现盈亏：`{assets_unrealized if assets_unrealized is not None else 'n/a'}` USDT",
        "",
        "## 市场热度",
        f"- Trending Top 5：`{', '.join(top_trending) if top_trending else 'n/a'}`",
        f"- Top Search Top 5：`{', '.join(top_search) if top_search else 'n/a'}`",
        f"- Alpha Top 5：`{', '.join(top_alpha) if top_alpha else 'n/a'}`",
        f"- Smart Money Top 5：`{', '.join(top_smart_money) if top_smart_money else 'n/a'}`",
        "",
        "## Trading Signal",
        f"- 多头信号 Top 5：`{long_signal_summary}`",
        f"- 空头信号 Top 5：`{short_signal_summary}`",
        "",
        "## 用途建议",
        "- `assets` 只用于 Hermes 资金与账户健康摘要，不接交易执行权。",
        "- `crypto-market-rank` 只用于市场热度和研究上下文，不直接驱动雷霆下单。",
        "- `trading-signal` 只用于研究和控制面摘要，不直接替代雷霆执行决策。",
    ]
    if not digest["assets_present"] or not digest["market_rank_present"] or not digest["trading_signal_present"]:
        lines.extend(
            [
                "",
                "## 当前状态",
                "- Binance Skills 输入文件还不完整，Hermes 已接线，但仍在等待外部导入。",
            ]
        )
    return digest, "\n".join(lines)


def build_square_preview(digest: dict[str, Any], *, symbol: str | None = None) -> tuple[dict[str, Any], str]:
    rank = digest.get("market_rank") or {}
    trading_signal = digest.get("trading_signal") or {}
    target_symbol = (symbol or "").strip().upper()

    def _symbol_variants(raw: str) -> set[str]:
        base = raw.strip().upper()
        variants = {base}
        if base:
            variants.add(f"{base}USDT")
            if base.endswith("USDT"):
                variants.add(base[:-4])
        return variants

    variants = _symbol_variants(target_symbol) if target_symbol else set()

    def _filter_symbols(items: list[str]) -> list[str]:
        if not target_symbol:
            return items
        return [item for item in items if str(item).upper() in variants]

    def _filter_signal_labels(items: list[str]) -> list[str]:
        if not target_symbol:
            return items
        return [item for item in items if any(str(item).upper().startswith(candidate) for candidate in variants)]

    trending = list(rank.get("top_trending") or [])
    top_search = list(rank.get("top_search") or [])
    alpha = list(rank.get("top_alpha") or [])
    long_signals = list(trading_signal.get("top_long") or [])
    short_signals = list(trading_signal.get("top_short") or [])

    scoped_trending = _filter_symbols(trending)
    scoped_search = _filter_symbols(top_search)
    scoped_alpha = _filter_symbols(alpha)
    scoped_long = _filter_signal_labels(long_signals)
    scoped_short = _filter_signal_labels(short_signals)
    has_direct_hit = any([scoped_trending, scoped_search, scoped_alpha, scoped_long, scoped_short])

    title = f"雷霆 {target_symbol} 广场预览草稿" if target_symbol else "雷霆广场预览草稿"
    text_lines = [title, f"时间：{digest.get('generated_at')}"]
    if target_symbol:
        text_lines.append("")
        if has_direct_hit:
            text_lines.append(f"今天先看 `{target_symbol}`。这个标的已经在外部市场参考里出现了直接命中。")
            if scoped_trending:
                text_lines.append(f"- 热门榜命中：{', '.join(scoped_trending)}")
            if scoped_search:
                text_lines.append(f"- 搜索热度命中：{', '.join(scoped_search)}")
            if scoped_alpha:
                text_lines.append(f"- Alpha 榜命中：{', '.join(scoped_alpha)}")
            if scoped_long:
                text_lines.append(f"- 链上多头信号：{', '.join(scoped_long)}")
            if scoped_short:
                text_lines.append(f"- 链上空头信号：{', '.join(scoped_short)}")
            text_lines.append("")
            text_lines.append("这说明这个标的至少已经进入了外部热度或链上资金的视野，但它仍然只是辅助参考，不替代雷霆自己的执行信号。")
        else:
            text_lines.append(f"今天先看 `{target_symbol}`。它在当前 Binance Skills 外部参考里没有直接命中。")
            text_lines.append("这并不代表它没有交易机会，更常见的情况是：链上 Smart Money 和热度榜覆盖的是另一组资产宇宙，不一定正好包含 BTC / ETH 合约。")
            text_lines.append("")
            text_lines.append(f"- 当前全局 Trending：{', '.join(trending) or '暂无'}")
            text_lines.append(f"- 当前全局 Top Search：{', '.join(top_search) or '暂无'}")
            text_lines.append(f"- 当前全局 Alpha：{', '.join(alpha) or '暂无'}")
            text_lines.append(f"- 当前链上多头信号：{_render_signal_summary(long_signals, trading_signal.get('raw_long_count'))}")
            text_lines.append(f"- 当前链上空头信号：{_render_signal_summary(short_signals, trading_signal.get('raw_short_count'))}")
            text_lines.append("")
            text_lines.append("所以这条草稿更适合作为外部市场气氛观察，而不是直接当成 BTC / ETH 的开仓理由。")
    else:
        text_lines.extend(
            [
                "",
                f"当前外部市场参考里，Trending 关注在：{', '.join(trending) or '暂无'}。",
                f"Top Search 更活跃的是：{', '.join(top_search) or '暂无'}。",
                f"Alpha 榜里靠前的是：{', '.join(alpha) or '暂无'}。",
                f"链上多头信号主要集中在：{_render_signal_summary(long_signals, trading_signal.get('raw_long_count'))}。",
                f"链上空头信号主要集中在：{_render_signal_summary(short_signals, trading_signal.get('raw_short_count'))}。",
                "",
                "这一组信息适合拿来做市场情绪和外部热度观察，不直接替代雷霆自己的执行判断。",
            ]
        )
    text_lines.extend(["", "说明：本草稿仅作外部市场参考，不构成交易建议。"])
    preview = {
        "generated_at": digest.get("generated_at"),
        "kind": "square_post_preview",
        "channel": "square-post",
        "mode": "preview_only",
        "enabled": True,
        "symbol": target_symbol or None,
        "text": "\n".join(text_lines),
        "source_digest_generated_at": digest.get("generated_at"),
    }
    markdown = "\n".join(
        [
            "# Binance Square 预发布草稿",
            "",
            f"- 生成时间：{preview['generated_at']}",
            f"- 通道：`{preview['channel']}`",
            f"- 模式：`{preview['mode']}`",
            "",
            "```text",
            preview["text"],
            "```",
        ]
    )
    return preview, markdown


def build_telegram_text(digest: dict[str, Any], markdown_path: Path) -> str:
    assets = digest.get("assets") or {}
    rank = digest.get("market_rank") or {}
    trading_signal = digest.get("trading_signal") or {}
    lines = [
        "Hermes 外部市场参考摘要",
        f"生成时间：{digest.get('generated_at')}",
        f"总权益：{assets.get('equity_usdt') if assets.get('equity_usdt') is not None else 'n/a'} USDT",
        f"可用余额：{assets.get('available_balance_usdt') if assets.get('available_balance_usdt') is not None else 'n/a'} USDT",
        f"未实现盈亏：{assets.get('unrealized_pnl_usdt') if assets.get('unrealized_pnl_usdt') is not None else 'n/a'} USDT",
        f"Trending：{', '.join(rank.get('top_trending') or []) or 'n/a'}",
        f"Top Search：{', '.join(rank.get('top_search') or []) or 'n/a'}",
        f"Alpha：{', '.join(rank.get('top_alpha') or []) or 'n/a'}",
        f"Smart Money：{', '.join(rank.get('top_smart_money') or []) or 'n/a'}",
        f"多头信号：{_render_signal_summary(list(trading_signal.get('top_long') or []), trading_signal.get('raw_long_count'))}",
        f"空头信号：{_render_signal_summary(list(trading_signal.get('top_short') or []), trading_signal.get('raw_short_count'))}",
        f"摘要文件：{markdown_path}",
    ]
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    home = Path(args.hermes_home)
    skills_dir = home / "memories" / "binance_skills"
    skills_dir.mkdir(parents=True, exist_ok=True)
    digest, markdown = build_digest(home)
    digest_json_path = skills_dir / "digest_latest.json"
    digest_md_path = skills_dir / "digest_latest.md"
    digest_json_path.write_text(json.dumps(digest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    digest_md_path.write_text(markdown, encoding="utf-8")

    square_enabled = (get_setting(home, "HERMES_BINANCE_SQUARE_ENABLED", "false") or "false").strip().lower() == "true"
    square_mode = (get_setting(home, "HERMES_BINANCE_SQUARE_MODE", "preview_only") or "preview_only").strip().lower()
    square_dir = home / "memories" / "binance_square"
    square_dir.mkdir(parents=True, exist_ok=True)

    if square_enabled:
        preview, preview_md = build_square_preview(digest)
        preview["mode"] = square_mode
        (square_dir / "preview_latest.json").write_text(json.dumps(preview, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (square_dir / "preview_latest.md").write_text(preview_md, encoding="utf-8")
        status = {
            "enabled": True,
            "mode": square_mode,
            "generated_at": digest.get("generated_at"),
            "reason": "square_post_preview_ready",
        }
    else:
        status = {
            "enabled": False,
            "mode": "disabled",
            "generated_at": digest.get("generated_at"),
            "reason": "default_off_to_avoid_publication_feedback_loop",
        }
    (square_dir / "status.json").write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    telegram_error = None
    if args.notify_telegram:
        telegram_error = send_telegram_message(build_telegram_text(digest, digest_md_path), home)
    print(
        json.dumps(
            {
                "ok": True,
                "digest": str(digest_json_path),
                "square_status": status,
                "telegram_error": telegram_error,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
