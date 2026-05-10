"""Strategy version card and switch history for Leiting."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import BACKTESTS_DIR, DATA_DIR, get_demo_leverage, get_demo_position_fraction, get_execution_mode, get_runtime_candidate_config_path, load_runtime_candidate_config
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


STRATEGY_VERSION_JSON = DATA_DIR / "state" / "strategy_version.json"
STRATEGY_VERSION_MD = DATA_DIR / "state" / "strategy_version.md"
STRATEGY_SWITCH_HISTORY_JSON = DATA_DIR / "state" / "strategy_switch_history.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _candidate_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def _read_research_basis() -> dict[str, Any]:
    review = _read_json(BACKTESTS_DIR / "research_reviews" / "latest.json") or {}
    recommendations = review.get("recommendations") or []
    return {
        "dataset_rows": review.get("dataset_rows"),
        "feature_count": review.get("feature_count"),
        "profile": review.get("profile"),
        "allowed_regimes": review.get("allowed_regimes") or [],
        "selected_threshold": review.get("selected_threshold"),
        "walk_forward_net_return_pct": review.get("walk_forward_net_return_pct"),
        "walk_forward_trade_count": review.get("walk_forward_trade_count"),
        "baseline_net_return_pct": review.get("baseline_net_return_pct"),
        "recommendations": recommendations[:3],
    }


def _indicator_summary(candidate: dict[str, Any]) -> list[str]:
    engine_mode = str(candidate.get("engine_mode") or "").strip().lower()
    if engine_mode == "v6_router":
        return [
            "router：按盘口结构在 maker 回归 / taker 冲击间切换",
            "maker mean-reversion：极端失衡 + microprice/OFI 翻转后被动进场",
            "taker impulse：顺势突破 + 多层订单流同向时主动进场",
            "执行质量过滤（点差/滑点/队列风险）",
        ]
    if engine_mode == "v4_microstructure":
        entry_style = str(candidate.get("entry_style") or "breakout_flow").strip().lower()
        if entry_style == "pullback_reclaim":
            return [
                "5m EMA 趋势过滤 + 回踩 EMA 收复确认",
                "OFI / microprice / depth imbalance 订单流确认",
                "maker-first 被动限价进场",
                "执行质量过滤（点差/滑点）",
            ]
        return [
            "5m EMA 趋势过滤 + 20 根突破/跌破",
            "OFI / microprice / depth imbalance 订单流确认",
            "maker-first 被动限价进场",
            "执行质量过滤（点差/滑点）",
        ]
    if engine_mode == "v3_simple":
        return [
            f"5m EMA{candidate.get('ema_fast_bars', 9)}/EMA{candidate.get('ema_slow_bars', 21)} 趋势过滤",
            f"5m {candidate.get('breakout_lookback_bars', 12)} 根突破/跌破",
            "taker/aggressive flow 确认",
            "执行质量过滤（点差/滑点）",
        ]
    return [
        "动量分",
        "微观结构分",
        "执行质量分",
        "候选阈值过滤",
    ]


def _render_markdown(card: dict[str, Any]) -> str:
    strategy = card.get("strategy") or {}
    research_basis = card.get("research_basis") or {}
    history = card.get("recent_switch_history") or []
    lines = [
        "# 雷霆策略版本卡",
        "",
        f"- 生成时间：{card.get('generated_at')}",
        f"- 当前版本：`{strategy.get('name') or 'n/a'}`",
        f"- 配置路径：`{strategy.get('config_path') or 'n/a'}`",
        f"- 配置哈希：`{strategy.get('config_hash') or 'n/a'}`",
        f"- 执行模式：`{card.get('execution_mode') or 'n/a'}`",
        f"- 引擎模式：`{strategy.get('engine_mode') or 'n/a'}`",
        f"- 标的：`{', '.join(strategy.get('symbols') or []) or 'n/a'}`",
        f"- 允许方向：`{'多' if strategy.get('allow_long') else ''}{'/' if strategy.get('allow_long') and strategy.get('allow_short') else ''}{'空' if strategy.get('allow_short') else ''}`",
        f"- 当前杠杆：`{card.get('effective_leverage')}`x / 仓位比例：`{card.get('position_fraction')}`",
        f"- 止盈：`{strategy.get('take_profit_net_roi_pct')}`% / 硬止损：`{strategy.get('hard_stop_net_roi_pct')}`%",
        "",
        "## 核心指标",
    ]
    lines.extend(f"- {item}" for item in strategy.get("indicators") or [])
    lines.extend(
        [
            "",
            "## 研究依据",
            f"- 研究档位：`{research_basis.get('profile') or 'n/a'}`",
            f"- 数据集行数：`{research_basis.get('dataset_rows') or 'n/a'}` / 特征数：`{research_basis.get('feature_count') or 'n/a'}`",
            f"- 允许 Regime：`{', '.join(research_basis.get('allowed_regimes') or []) or 'n/a'}`",
            f"- 当前阈值：`{research_basis.get('selected_threshold')}`",
            f"- Walk-forward：`{research_basis.get('walk_forward_net_return_pct')}`% / 交易数：`{research_basis.get('walk_forward_trade_count')}`",
            f"- Baseline：`{research_basis.get('baseline_net_return_pct')}`%",
            "",
            "## 最近切换",
        ]
    )
    if history:
        lines.extend(
            f"- `{item.get('activated_at')}` | `{item.get('name')}` | 原因：`{item.get('reason') or 'n/a'}`"
            for item in history
        )
    else:
        lines.append("- 暂无")
    if research_basis.get("recommendations"):
        lines.extend(["", "## 当前建议"])
        lines.extend(f"- {item}" for item in research_basis.get("recommendations") or [])
    return "\n".join(lines)


def refresh_strategy_version(*, reason: str = "runtime_refresh") -> dict[str, Any]:
    candidate = load_runtime_candidate_config()
    config_path = get_runtime_candidate_config_path()
    config_hash = _candidate_hash(candidate)
    state = read_runtime_state("strategy_version_state") or {}
    history_payload = _read_json(STRATEGY_SWITCH_HISTORY_JSON) or {"history": []}
    history = list(history_payload.get("history") or [])
    current_signature = {
        "name": candidate.get("name"),
        "config_path": str(config_path),
        "config_hash": config_hash,
    }
    previous_signature = {
        "name": state.get("name"),
        "config_path": state.get("config_path"),
        "config_hash": state.get("config_hash"),
    }
    changed = current_signature != previous_signature
    research_basis = _read_research_basis()
    if changed:
        history.append(
            {
                "activated_at": _utc_now(),
                "name": candidate.get("name"),
                "config_path": str(config_path),
                "config_hash": config_hash,
                "reason": reason,
                "selected_threshold": research_basis.get("selected_threshold"),
                "walk_forward_net_return_pct": research_basis.get("walk_forward_net_return_pct"),
                "walk_forward_trade_count": research_basis.get("walk_forward_trade_count"),
            }
        )
        history = history[-30:]
        STRATEGY_SWITCH_HISTORY_JSON.write_text(
            json.dumps({"history": history}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    card = {
        "generated_at": _utc_now(),
        "execution_mode": get_execution_mode(),
        "effective_leverage": get_demo_leverage() if get_execution_mode() in {"demo_auto", "testnet_auto"} else None,
        "position_fraction": get_demo_position_fraction() if get_execution_mode() in {"demo_auto", "testnet_auto"} else None,
        "strategy": {
            "name": candidate.get("name"),
            "engine_mode": candidate.get("engine_mode"),
            "config_path": str(config_path),
            "config_hash": config_hash,
            "symbols": candidate.get("symbols") or [],
            "allow_long": bool(candidate.get("allow_long", True)),
            "allow_short": bool(candidate.get("allow_short", False)),
            "take_profit_net_roi_pct": candidate.get("take_profit_net_roi_pct"),
            "hard_stop_net_roi_pct": candidate.get("hard_stop_net_roi_pct"),
            "threshold": candidate.get("threshold"),
            "indicators": _indicator_summary(candidate),
            "raw": candidate,
        },
        "research_basis": research_basis,
        "recent_switch_history": history[-10:],
        "last_switch_at": history[-1]["activated_at"] if history else None,
        "current_reason": reason,
    }
    STRATEGY_VERSION_JSON.write_text(json.dumps(card, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    STRATEGY_VERSION_MD.write_text(_render_markdown(card), encoding="utf-8")
    update_runtime_state(
        "strategy_version_state",
        {
            **current_signature,
            "updated_at": card["generated_at"],
            "last_switch_at": card["last_switch_at"],
        },
    )
    update_runtime_state("strategy_version", card)
    return card
