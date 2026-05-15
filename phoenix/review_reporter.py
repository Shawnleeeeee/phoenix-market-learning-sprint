from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ReviewReport:
    report_type: str
    text: str
    payload: dict[str, Any]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_type": self.report_type,
            "text": self.text,
            "payload": self.payload,
            "created_at": self.created_at,
        }


def build_review_report(report_type: str, payload: dict[str, Any]) -> ReviewReport:
    normalized = str(report_type or "").strip().upper()
    builders = {
        "NO_TRADE": _no_trade,
        "PRE_ENTER": _pre_enter,
        "OPENED": _opened,
        "POSITION_UPDATE": _position_update,
        "CLOSED": _closed,
        "RISK_ALERT": _risk_alert,
        "DAILY_SUMMARY": _daily_summary,
        "RISK_REJECT": _risk_reject,
        "SOFT_REJECT": _soft_reject,
        "HARD_FREEZE": _hard_freeze,
    }
    builder = builders.get(normalized, _risk_alert)
    return ReviewReport(
        report_type=normalized or "RISK_ALERT",
        text=builder(payload),
        payload=payload,
        created_at=datetime.now(timezone.utc).isoformat(),
    )


def append_review_log(path: str | Path, report: ReviewReport) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True) + "\n")


def _no_trade(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "【唔交易】",
            "",
            f"原因：{payload.get('reason', '暂时无清晰入场条件')}",
            f"市场状态：{payload.get('regime', payload.get('market_regime', '未知'))}",
            f"风控状态：{payload.get('risk_status', '未触发开仓')}",
            "",
            "状态：",
            "Phoenix 无落单，只继续观察 testnet / shadow 数据。",
        ]
    )


def _pre_enter(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "【准备开仓】",
            "",
            f"币种：{payload.get('symbol', 'UNKNOWN')}",
            f"方向：{_side_text(payload.get('side') or payload.get('action'))}",
            f"交易类型：{_trade_type(payload.get('trade_type'))}",
            f"信心：{payload.get('confidence', 'n/a')}",
            f"止损：{payload.get('stop_loss_price') or payload.get('stop_loss_pct') or 'n/a'}",
            f"止盈：{payload.get('take_profit_price') or payload.get('take_profit_pct') or 'n/a'}",
            f"最长持仓：{payload.get('max_holding_time_sec', 'n/a')} 秒",
            "",
            "当前判断：",
            str(payload.get("reason") or "暂时未有清晰理由。"),
            "",
            "状态：",
            "只系准备动作，真正落单前仍然要过 Risk Governor 同 safe_order_gateway。",
        ]
    )


def _opened(payload: dict[str, Any]) -> str:
    lines = [
        "【已开仓】",
        "",
        f"币种：{payload.get('symbol', 'UNKNOWN')}",
        f"方向：{_side_text(_first_present(payload, 'side', 'action', 'direction'))}",
        f"入场价：{_field_text(_first_present(payload, 'entry_price', 'entry_price_hint'))}",
        f"杠杆：{_field_text(payload.get('leverage'))}x",
        f"仓位大小：{_field_text(_first_present(payload, 'quantity', 'size', 'position_size'))}",
        f"止损价：{_field_text(payload.get('stop_loss_price'))}",
        f"止盈价：{_field_text(payload.get('take_profit_price'))}",
    ]
    if payload.get("reason"):
        lines.extend(["", f"入场理由：{payload['reason']}"])
    lines.extend(
        [
            "",
            "状态：",
            str(payload.get("protection_status") or "止损保护状态未回报。"),
        ]
    )
    return "\n".join(lines)


def _position_update(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "【持仓更新】",
            "",
            f"币种：{payload.get('symbol', 'UNKNOWN')}",
            f"方向：{_side_text(_first_present(payload, 'side', 'action', 'direction'))}",
            f"当前价：{_field_text(_first_present(payload, 'current_price', 'mark_price'))}",
            f"浮盈浮亏：{_field_text(payload.get('unrealized_pnl_pct'))}%",
            f"实际浮盈亏：{_field_text(payload.get('unrealized_pnl_usdt'))} USDT",
            f"已持仓：{_field_text(_first_present(payload, 'time_in_trade_sec', 'holding_time_sec'))} 秒",
            "",
            "状态：",
            str(payload.get("status") or "继续观察，暂时无新动作。"),
        ]
    )


def _closed(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "【已平仓】",
            "",
            f"币种：{payload.get('symbol', 'UNKNOWN')}",
            f"方向：{_side_text(_first_present(payload, 'side', 'action', 'direction'))}",
            f"入场价：{_field_text(_first_present(payload, 'entry_price', 'entry_price_hint'))}",
            f"平仓价：{_field_text(_first_present(payload, 'exit_price', 'final_close_price'))}",
            f"收益率：{_field_text(_first_present(payload, 'pnl_pct', 'roi_pct'))}%",
            f"实际盈亏：{_field_text(_first_present(payload, 'pnl_usdt', 'realized_pnl_usdt'))} USDT",
            f"费用：{_field_text(_first_present(payload, 'fees', 'fees_usdt'))} USDT",
            f"持仓时间：{_field_text(payload.get('holding_time_sec'))} 秒",
            f"止损价：{_field_text(payload.get('stop_loss_price'))}",
            f"止盈价：{_field_text(payload.get('take_profit_price'))}",
            f"最长持仓：{_field_text(_first_present(payload, 'max_holding_time_sec', 'max_holding_sec'))} 秒",
            f"超时到期：{_field_text(payload.get('timeout_due_at'))}",
            f"平仓原因：{_field_text(_first_present(payload, 'exit_reason', 'final_close_reason'))}",
            "",
            "复盘一句：",
            str(payload.get("review") or "单已经结束，之后交俾 Analyst 复盘。"),
        ]
    )


def _risk_alert(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "【风险警报】",
            "",
            f"问题：{payload.get('reason', 'unknown')}",
            f"拦截点：{_join_reasons(payload.get('blocked_by'))}",
            "",
            "状态：",
            "Phoenix 已经停低呢个动作，未有继续执行。",
        ]
    )


def _daily_summary(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "【每日总结】",
            "",
            f"今日交易数：{payload.get('trades', 0)}",
            f"净盈亏：{payload.get('net_pnl_usdt', 'n/a')} USDT",
            f"胜率：{payload.get('win_rate', 'n/a')}",
            f"最大问题：{payload.get('main_issue', '未有特别问题')}",
            "",
            "结论：",
            str(payload.get("verdict") or "继续细样本观察，唔好当成真钱结论。"),
        ]
    )


def _risk_reject(payload: dict[str, Any]) -> str:
    reasons = payload.get("blocked_by") or payload.get("reasons") or [payload.get("reason", "unknown")]
    return "\n".join(
        [
            "【Risk Governor 拒绝】",
            "",
            f"动作：{payload.get('action', 'UNKNOWN')}",
            f"币种：{payload.get('symbol', 'UNKNOWN')}",
            f"拒绝原因：{_join_reasons(reasons)}",
            "",
            "状态：",
            "Hermes 只可以提出建议，Risk Governor 拒绝后 Phoenix 唔会执行。",
        ]
    )


def _soft_reject(payload: dict[str, Any]) -> str:
    reasons = payload.get("blocked_by") or payload.get("reasons") or [payload.get("reason", "unknown")]
    direction_lines = _direction_regime_review_lines(payload)
    entry_quality_lines = _entry_quality_review_lines(payload)
    return "\n".join(
        [
            "【今轮唔交易】",
            "",
            f"原因：{_plain_reject_reason(reasons)}",
            *direction_lines,
            *entry_quality_lines,
            "状态：系统已放弃今轮交易，下一轮继续观察。",
            "",
            "说明：",
            "呢个唔系系统故障，只系今轮条件唔够靓，Phoenix 无落单。",
        ]
    )


def _direction_regime_review_lines(payload: dict[str, Any]) -> list[str]:
    reason = payload.get("direction_regime_reason")
    if not reason:
        return []
    return [
        f"方向纪律：{reason}",
        f"市场状态：{payload.get('market_regime', 'UNKNOWN')}；允许方向：{payload.get('allowed_direction', 'UNKNOWN')}；当前候选：{payload.get('candidate_direction', 'UNKNOWN')}",
        "结论：因方向纪律不交易。",
    ]


def _entry_quality_review_lines(payload: dict[str, Any]) -> list[str]:
    reason = payload.get("entry_quality_reason")
    if not reason:
        return []
    return [
        f"Entry quality：{reason}",
        f"v0.4 过滤：allowed={payload.get('entry_quality_allowed')}；score={payload.get('entry_quality_score')}",
        "结论：因入场质量不足，本轮不交易。",
    ]


def _hard_freeze(payload: dict[str, Any]) -> str:
    reason = payload.get("freeze_reason") or payload.get("reason") or _join_reasons(payload.get("blocked_by"))
    return "\n".join(
        [
            "【风险冻结】",
            "",
            f"原因：{reason}",
            "状态：trial 已暂停，系统唔会继续落单，需要人工检查。",
            "",
            "说明：",
            "呢个系安全状态唔可信，唔可以当普通唔交易处理。",
        ]
    )


def _plain_reject_reason(value: Any) -> str:
    reasons = value if isinstance(value, list) else [value]
    text = ",".join(str(item) for item in reasons if item)
    if "spread_too_wide" in text:
        return "当前 spread 偏大，强行入场容易滑点。"
    if "slippage_too_high" in text:
        return "预计 slippage 偏高，今轮唔值得追。"
    if "liquidity_too_poor" in text:
        return "流动性唔够，落单质量唔稳定。"
    if "entry_quality_filter_failed" in text:
        return "v0.4 入场质量过滤未通过，避免追空/追高或成本拖累。"
    if "direction_lock_conflict" in text:
        return "方向同大盘锁定方向冲突，今轮唔做。"
    if "cooldown_after_loss_active" in text:
        return "冷静期仲未完，系统继续等下一轮。"
    return _join_reasons(value)


def _join_reasons(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "、".join(str(item) for item in value if item)
    return str(value or "unknown")


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _field_text(value: Any, default: str = "未回报") -> str:
    if value in (None, ""):
        return default
    return str(value)


def _side_text(value: Any) -> str:
    text = str(value or "").upper()
    if text in {"LONG", "BUY", "ENTER_LONG"}:
        return "做多"
    if text in {"SHORT", "SELL", "ENTER_SHORT"}:
        return "做空"
    return text or "未知"


def _trade_type(value: Any) -> str:
    text = str(value or "").upper()
    return {
        "QUICK_TRADE": "短线单",
        "SWING_TRADE": "波段单",
        "TREND_HOLD": "趋势持仓",
        "NONE": "无交易",
    }.get(text, text or "未知")
