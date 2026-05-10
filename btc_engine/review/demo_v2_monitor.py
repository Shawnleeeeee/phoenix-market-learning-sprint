"""Demo candidate monitoring, rejection stats, and first-fill review export."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import REVIEWS_DIR, ensure_runtime_dirs, get_env, get_hermes_demo_v2_target
from btc_engine.runtime.hermes_sync import run_remote_command, sync_outputs
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


DEMO_V2_DIR = REVIEWS_DIR / "demo_v2"
REJECTION_SYNC_INTERVAL_SEC = 300


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_reason(reason: Any) -> str:
    text = str(reason or "").strip()
    if " (" in text and text.endswith(")"):
        return text.split(" (", 1)[0].strip()
    return text


def _is_supported_candidate(candidate_name: Any) -> bool:
    text = str(candidate_name or "").strip().lower()
    return text.startswith("leiting_") and any(tag in text for tag in ("v2", "v3"))


def _empty_rejection_payload(candidate_name: str | None = None) -> dict[str, Any]:
    return {
        "generated_at": _utc_now(),
        "candidate_name": candidate_name or "unknown",
        "total_rejections": 0,
        "raw_total_rejections": 0,
        "bucketed_total_rejections": 0,
        "counting_mode": "decision_bucket_5m",
        "reason_counts": {},
        "bucketed_reason_counts": {},
        "regime_counts": {},
        "bucketed_regime_counts": {},
        "bias_counts": {},
        "bucketed_bias_counts": {},
        "combo_counts": {},
        "bucketed_combo_counts": {},
        "recent_rejections": [],
        "last_synced_at": None,
        "last_counted_bucket_5m": None,
        "bucketed_started_at": _utc_now(),
        "candidate_switched_at": _utc_now(),
    }


def _combo_key(reasons: list[Any]) -> str:
    normalized = sorted({item for item in (_normalize_reason(reason) for reason in reasons) if item})
    return " + ".join(normalized)


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


def _bucket_5m(snapshot_id: Any) -> str | None:
    parsed = _parse_dt(snapshot_id)
    if parsed is None:
        return None
    bucket_minute = (parsed.minute // 5) * 5
    bucket = parsed.replace(minute=bucket_minute, second=0, microsecond=0)
    return bucket.isoformat()


def _human_rejection_markdown(payload: dict[str, Any]) -> str:
    bucketed_reason_counts = payload.get("bucketed_reason_counts") or {}
    bucketed_regime_counts = payload.get("bucketed_regime_counts") or {}
    bucketed_bias_counts = payload.get("bucketed_bias_counts") or {}
    raw_reason_counts = payload.get("reason_counts") or {}
    raw_regime_counts = payload.get("regime_counts") or {}
    raw_bias_counts = payload.get("bias_counts") or {}
    recent = payload.get("recent_rejections") or []
    lines = [
        "# 雷霆 Demo 候选拒单统计",
        "",
        f"- 生成时间：{payload.get('generated_at')}",
        f"- 候选策略：`{payload.get('candidate_name')}`",
        f"- 5m 决策级拒单数：`{payload.get('bucketed_total_rejections', payload.get('total_rejections', 0))}`",
        f"- 原始快照拒单数：`{payload.get('raw_total_rejections', payload.get('total_rejections', 0))}`",
        f"- 当前统计口径：`{payload.get('counting_mode', 'raw_snapshots')}`",
        f"- 最近同步：`{payload.get('last_synced_at', 'n/a')}`",
        "",
        "## 拒单主因（5m 决策级）",
    ]
    if bucketed_reason_counts:
        lines.extend(f"- {key}: `{value}`" for key, value in sorted(bucketed_reason_counts.items(), key=lambda item: item[1], reverse=True))
    else:
        lines.append("- 暂无")
    lines.extend(
        [
            "",
            "## Regime 分布（5m 决策级）",
        ]
    )
    if bucketed_regime_counts:
        lines.extend(f"- {key}: `{value}`" for key, value in sorted(bucketed_regime_counts.items(), key=lambda item: item[1], reverse=True))
    else:
        lines.append("- 暂无")
    lines.extend(
        [
            "",
            "## Bias 分布（5m 决策级）",
        ]
    )
    if bucketed_bias_counts:
        lines.extend(f"- {key}: `{value}`" for key, value in sorted(bucketed_bias_counts.items(), key=lambda item: item[1], reverse=True))
    else:
        lines.append("- 暂无")
    lines.extend(
        [
            "",
            "## 原始快照主因（调试用）",
        ]
    )
    if raw_reason_counts:
        lines.extend(f"- {key}: `{value}`" for key, value in sorted(raw_reason_counts.items(), key=lambda item: item[1], reverse=True)[:10])
    else:
        lines.append("- 暂无")
    lines.extend(
        [
            "",
            "## 最卡成交的门槛组合（5m 决策级）",
        ]
    )
    combo_counts = payload.get("bucketed_combo_counts") or {}
    if combo_counts:
        lines.extend(f"- {key}: `{value}`" for key, value in sorted(combo_counts.items(), key=lambda item: item[1], reverse=True)[:5])
    else:
        lines.append("- 暂无")
    lines.extend(
        [
            "",
            "## 最近 10 次拒单",
        ]
    )
    for item in recent[-10:]:
        lines.append(
            "- `{timestamp}` | regime=`{regime}` | bias=`{bias}` | conf=`{confidence}` | 原因：{reasons}".format(
                timestamp=item.get("timestamp"),
                regime=item.get("regime"),
                bias=item.get("bias"),
                confidence=item.get("directional_confidence"),
                reasons="; ".join(item.get("reasons") or []),
            )
        )
    if not recent:
        lines.append("- 暂无")
    return "\n".join(lines)


def _first_fill_markdown(payload: dict[str, Any]) -> str:
    journal = payload.get("journal") or {}
    entry_signal = payload.get("entry_signal") or {}
    entry_market = payload.get("entry_market") or {}
    lines = [
        "# 雷霆 Demo 首笔成交质量复盘",
        "",
        f"- 生成时间：{payload.get('generated_at')}",
        f"- 候选策略：`{payload.get('candidate_name')}`",
        f"- 交易模式：`{payload.get('mode')}`",
        f"- 标的：`{payload.get('symbol')}`",
        f"- 方向：`{payload.get('side')}`",
        f"- Regime：`{payload.get('regime')}`",
        f"- 入场快照：`{payload.get('entry_snapshot_id')}`",
        f"- 开仓时间：`{payload.get('opened_at')}`",
        f"- 平仓时间：`{journal.get('closed_at')}`",
        f"- 持仓分钟：`{journal.get('hold_minutes')}`",
        f"- quote allocation：`{payload.get('quote_allocation_usdt')}` USDT",
        f"- 杠杆：`{payload.get('leverage')}`x",
        f"- 入场均价：`{journal.get('entry_avg_price')}`",
        f"- 平仓均价：`{journal.get('close_avg_price')}`",
        f"- 净盈亏：`{journal.get('net_pnl')}` USDT",
        f"- 关闭原因：`{journal.get('reason')}`",
        "",
        "## 入场质量",
        f"- Bias：`{entry_signal.get('bias')}`",
        f"- 方向置信度：`{entry_signal.get('directional_confidence')}`",
        f"- 动量分：`{entry_signal.get('momentum_score')}`",
        f"- 微观结构分：`{entry_signal.get('microstructure_score')}`",
        f"- 执行质量分：`{entry_signal.get('execution_quality_score')}`",
        f"- 事件风险分：`{entry_signal.get('event_risk_score')}`",
        f"- 点差：`{entry_market.get('spread_bps')}` bps",
        f"- 深度失衡：`{entry_market.get('depth_imbalance')}`",
        f"- 5m 主动买入占比：`{entry_market.get('taker_buy_ratio_5m')}`",
        f"- 主动流向差：`{entry_market.get('aggressive_flow_delta')}`",
        f"- 5m 涨幅：`{entry_market.get('price_change_5m_pct')}`%",
        f"- 1h 涨幅：`{entry_market.get('price_change_1h_pct')}`%",
        "",
        "## 保护链事件",
    ]
    for item in payload.get("events") or []:
        lines.append(
            f"- `{item.get('at')}` | `{item.get('event')}` | 价格 `{item.get('mark_price', item.get('avg_price', item.get('trigger_price', 'n/a')))}`"
        )
    if not (payload.get("events") or []):
        lines.append("- 暂无")
    lines.extend(
        [
            "",
            "## 文件",
            f"- journal：`{payload.get('journal_path')}`",
        ]
    )
    return "\n".join(lines)


def _sync_outputs(outputs: dict[str, Path], *, force: bool = False, notify_remote: bool = False) -> str | None:
    target = get_hermes_demo_v2_target()
    if not target:
        return None
    sync_state = read_runtime_state("demo_v2_sync_state") or {}
    last_synced_at = sync_state.get("last_synced_at")
    last_synced_total = int(sync_state.get("last_synced_total_rejections") or 0)
    latest_total = 0
    stats_path = outputs.get("rejection_stats_latest.json")
    if stats_path:
        stats_payload = _read_json(stats_path) or {}
        latest_total = int(stats_payload.get("total_rejections") or 0)
    now_dt = datetime.now(timezone.utc)
    should_sync = force
    if not should_sync:
        if latest_total and latest_total != last_synced_total:
            should_sync = True
        elif not last_synced_at:
            should_sync = True
        else:
            try:
                last_dt = datetime.fromisoformat(str(last_synced_at).replace("Z", "+00:00"))
            except ValueError:
                should_sync = True
            else:
                should_sync = (now_dt - last_dt).total_seconds() >= REJECTION_SYNC_INTERVAL_SEC
    if not should_sync:
        return None
    sync_outputs(outputs, target)
    remote_cmd = (
        get_env(
            "BTC_HERMES_DEMO_V2_REMOTE_CMD",
            "python3 /opt/phoenix/deploy/hermes/leiting_demo_v2_digest.py --hermes-home /root/.hermes",
        )
        or ""
    ).strip()
    if notify_remote and remote_cmd and "--notify-telegram" not in remote_cmd:
        remote_cmd = f"{remote_cmd} --notify-telegram"
    remote_hook_error = run_remote_command(target, remote_cmd)
    update_runtime_state(
        "demo_v2_sync_state",
        {
            "last_synced_at": now_dt.isoformat(),
            "last_synced_total_rejections": latest_total,
        },
    )
    return remote_hook_error


def record_rejection(snapshot: dict[str, Any], candidate: dict[str, Any], reasons: list[str]) -> dict[str, Any]:
    ensure_runtime_dirs()
    DEMO_V2_DIR.mkdir(parents=True, exist_ok=True)
    stats_path = DEMO_V2_DIR / "rejection_stats_latest.json"
    markdown_path = DEMO_V2_DIR / "rejection_stats_latest.md"
    current_candidate_name = candidate.get("name") or "unknown"
    payload = _read_json(stats_path) or _empty_rejection_payload(current_candidate_name)
    if str(payload.get("candidate_name") or "") != str(current_candidate_name):
        payload = _empty_rejection_payload(current_candidate_name)
    if "raw_total_rejections" not in payload:
        payload["raw_total_rejections"] = int(payload.get("total_rejections") or 0)
    if "bucketed_total_rejections" not in payload:
        payload["bucketed_total_rejections"] = 0
    payload["counting_mode"] = "decision_bucket_5m"
    payload.setdefault("bucketed_reason_counts", {})
    payload.setdefault("bucketed_regime_counts", {})
    payload.setdefault("bucketed_bias_counts", {})
    payload.setdefault("combo_counts", {})
    payload.setdefault("bucketed_combo_counts", {})
    payload.setdefault("bucketed_started_at", _utc_now())
    reason_counts = Counter()
    for key, value in (payload.get("reason_counts") or {}).items():
        reason_counts[_normalize_reason(key)] += int(value)
    bucketed_reason_counts = Counter()
    for key, value in (payload.get("bucketed_reason_counts") or {}).items():
        bucketed_reason_counts[_normalize_reason(key)] += int(value)
    regime_counts = Counter(payload.get("regime_counts") or {})
    bucketed_regime_counts = Counter(payload.get("bucketed_regime_counts") or {})
    bias_counts = Counter(payload.get("bias_counts") or {})
    bucketed_bias_counts = Counter(payload.get("bucketed_bias_counts") or {})
    combo_counts = Counter(payload.get("combo_counts") or {})
    bucketed_combo_counts = Counter(payload.get("bucketed_combo_counts") or {})
    signal = snapshot.get("signal") or {}
    market = snapshot.get("market") or {}
    recent = list(payload.get("recent_rejections") or [])
    snapshot_id = snapshot.get("generated_at")
    decision_bucket = _bucket_5m(snapshot_id)
    normalized_reasons = [_normalize_reason(reason) for reason in reasons]
    combo_key = _combo_key(reasons)
    if recent:
        last = recent[-1]
        last_reasons = [_normalize_reason(item) for item in (last.get("reasons") or [])]
        if last.get("snapshot_id") == snapshot_id and last_reasons == normalized_reasons:
            payload["generated_at"] = _utc_now()
            payload["deduped_snapshot_id"] = snapshot_id
            _write_json(stats_path, payload)
            markdown_path.write_text(_human_rejection_markdown(payload), encoding="utf-8")
            update_runtime_state("demo_v2_rejection_stats", payload)
            return payload
    for reason in reasons:
        reason_counts[_normalize_reason(reason)] += 1
    if combo_key:
        combo_counts[combo_key] += 1
    regime_counts[str(signal.get("regime") or "unknown")] += 1
    bias_counts[str(signal.get("bias") or "unknown")] += 1
    payload["raw_total_rejections"] = int(payload.get("raw_total_rejections") or 0) + 1
    count_bucket = decision_bucket and decision_bucket != payload.get("last_counted_bucket_5m")
    if count_bucket:
        for reason in reasons:
            bucketed_reason_counts[_normalize_reason(reason)] += 1
        if combo_key:
            bucketed_combo_counts[combo_key] += 1
        bucketed_regime_counts[str(signal.get("regime") or "unknown")] += 1
        bucketed_bias_counts[str(signal.get("bias") or "unknown")] += 1
        payload["bucketed_total_rejections"] = int(payload.get("bucketed_total_rejections") or 0) + 1
        payload["last_counted_bucket_5m"] = decision_bucket
    recent.append(
        {
            "timestamp": _utc_now(),
            "snapshot_id": snapshot_id,
            "decision_bucket_5m": decision_bucket,
            "candidate_name": candidate.get("name"),
            "regime": signal.get("regime"),
            "bias": signal.get("bias"),
            "directional_confidence": signal.get("directional_confidence"),
            "momentum_score": signal.get("momentum_score"),
            "microstructure_score": signal.get("microstructure_score"),
            "execution_quality_score": signal.get("execution_quality_score"),
            "spread_bps": market.get("spread_bps"),
            "depth_imbalance": market.get("depth_imbalance"),
            "taker_buy_ratio_5m": market.get("taker_buy_ratio_5m"),
            "aggressive_flow_delta": market.get("aggressive_flow_delta"),
            "market_data_source": market.get("market_data_source"),
            "reasons": reasons,
        }
    )
    payload.update(
        {
            "generated_at": _utc_now(),
            "candidate_name": candidate.get("name") or payload.get("candidate_name") or "unknown",
            "total_rejections": int(payload.get("bucketed_total_rejections") or 0),
            "reason_counts": dict(reason_counts),
            "bucketed_reason_counts": dict(bucketed_reason_counts),
            "regime_counts": dict(regime_counts),
            "bucketed_regime_counts": dict(bucketed_regime_counts),
            "bias_counts": dict(bias_counts),
            "bucketed_bias_counts": dict(bucketed_bias_counts),
            "combo_counts": dict(combo_counts),
            "bucketed_combo_counts": dict(bucketed_combo_counts),
            "recent_rejections": recent[-100:],
        }
    )
    sync_state = read_runtime_state("demo_v2_sync_state") or {}
    payload["last_synced_at"] = sync_state.get("last_synced_at")
    _write_json(stats_path, payload)
    markdown_path.write_text(_human_rejection_markdown(payload), encoding="utf-8")
    remote_hook_error = _sync_outputs(
        {
            "rejection_stats_latest.json": stats_path,
            "rejection_stats_latest.md": markdown_path,
        },
        force=False,
        notify_remote=False,
    )
    sync_state = read_runtime_state("demo_v2_sync_state") or {}
    payload["last_synced_at"] = sync_state.get("last_synced_at")
    _write_json(stats_path, payload)
    markdown_path.write_text(_human_rejection_markdown(payload), encoding="utf-8")
    payload["remote_hook_error"] = remote_hook_error
    update_runtime_state("demo_v2_rejection_stats", payload)
    return payload


def maybe_export_first_fill_review(
    *,
    active_trade: dict[str, Any],
    journal: dict[str, Any],
    journal_id: str,
    journal_path: Path,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    if str(journal.get("mode") or "").strip().lower() != "demo_auto":
        return {"triggered": False, "reason": "journal_mode_not_demo_auto"}
    current_candidate_name = str(active_trade.get("candidate_name") or "")
    if not _is_supported_candidate(current_candidate_name):
        return {"triggered": False, "reason": "candidate_not_supported"}
    state = read_runtime_state("demo_v2_first_fill_state") or {}
    if state.get("candidate_name") == current_candidate_name and state.get("first_reviewed_journal_id"):
        return {
            "triggered": False,
            "reason": "first_fill_review_already_exists",
            "journal_id": state.get("first_reviewed_journal_id"),
        }
    DEMO_V2_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": _utc_now(),
        "journal_id": journal_id,
        "journal_path": str(journal_path),
        "mode": journal.get("mode"),
        "candidate_name": active_trade.get("candidate_name"),
        "symbol": active_trade.get("symbol"),
        "side": active_trade.get("side"),
        "regime": active_trade.get("regime"),
        "entry_snapshot_id": active_trade.get("entry_snapshot_id"),
        "opened_at": active_trade.get("opened_at"),
        "quote_allocation_usdt": active_trade.get("plan", {}).get("quote_allocation_usdt"),
        "leverage": active_trade.get("plan", {}).get("leverage"),
        "entry_signal": active_trade.get("entry_signal") or {},
        "entry_market": active_trade.get("entry_market") or {},
        "plan": active_trade.get("plan") or {},
        "protection": active_trade.get("protection") or {},
        "events": active_trade.get("events") or [],
        "journal": journal,
    }
    review_json = DEMO_V2_DIR / "first_fill_review.json"
    review_md = DEMO_V2_DIR / "first_fill_review.md"
    _write_json(review_json, payload)
    review_md.write_text(_first_fill_markdown(payload), encoding="utf-8")
    remote_hook_error = _sync_outputs(
        {
            "first_fill_review.json": review_json,
            "first_fill_review.md": review_md,
        },
        force=True,
        notify_remote=True,
    )
    state = {
        "first_reviewed_journal_id": journal_id,
        "candidate_name": current_candidate_name,
        "generated_at": payload["generated_at"],
        "remote_hook_error": remote_hook_error,
    }
    update_runtime_state("demo_v2_first_fill_state", state)
    return {"triggered": True, "review": payload, "remote_hook_error": remote_hook_error}
