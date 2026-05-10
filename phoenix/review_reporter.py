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
    return "\n".join(
        [
            "【已开仓】",
            "",
            f"币种：{payload.get('symbol', 'UNKNOWN')}",
            f"方向：{_side_text(payload.get('side'))}",
            f"入场价：{payload.get('entry_price', 'n/a')}",
            f"杠杆：{payload.get('leverage', 'n/a')}x",
            f"仓位大小：{payload.get('quantity', payload.get('size', 'n/a'))}",
            f"止损价：{payload.get('stop_loss_price', 'n/a')}",
            f"止盈价：{payload.get('take_profit_price', 'n/a')}",
            "",
            "状态：",
            str(payload.get("protection_status") or "止损保护状态未回报。"),
        ]
    )


def _position_update(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "【持仓更新】",
            "",
            f"币种：{payload.get('symbol', 'UNKNOWN')}",
            f"方向：{_side_text(payload.get('side'))}",
            f"当前价：{payload.get('current_price', 'n/a')}",
            f"浮盈浮亏：{payload.get('unrealized_pnl_pct', 'n/a')}%",
            f"实际浮盈亏：{payload.get('unrealized_pnl_usdt', 'n/a')} USDT",
            f"已持仓：{payload.get('time_in_trade_sec', 'n/a')} 秒",
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
            f"方向：{_side_text(payload.get('side'))}",
            f"入场价：{payload.get('entry_price', 'n/a')}",
            f"平仓价：{payload.get('exit_price', 'n/a')}",
            f"收益率：{payload.get('pnl_pct', 'n/a')}%",
            f"实际盈亏：{payload.get('pnl_usdt', 'n/a')} USDT",
            f"持仓时间：{payload.get('holding_time_sec', 'n/a')} 秒",
            f"平仓原因：{payload.get('exit_reason', 'n/a')}",
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


def _join_reasons(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "、".join(str(item) for item in value if item)
    return str(value or "unknown")


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
