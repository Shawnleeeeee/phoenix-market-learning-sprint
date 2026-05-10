#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from phoenix.config import load_execution_settings
from phoenix.guardian_workers import latest_running_worker as latest_running_worker_from_exchange
from phoenix.runtime_state import extract_candidate_views
from deploy.hermes.binance_skill_context import load_skill_digest, markdown_lines_from_digest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write compact Hermes-facing Phoenix state.")
    parser.add_argument("--phoenix-root", required=True)
    parser.add_argument("--hermes-home", required=True)
    parser.add_argument("--stdout-file", required=True)
    parser.add_argument("--stderr-file", required=True)
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser.parse_args()


def load_json_file(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def latest_active_worker(memories_dir: Path) -> dict[str, Any] | None:
    workers_dir = memories_dir / "phoenix_guardian_workers"
    if not workers_dir.exists():
        return None
    payload = latest_running_worker_from_exchange(workers_dir)
    if payload is None:
        return None
    intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
    initial_stop = (
        payload.get("initial_protective_stop")
        if isinstance(payload.get("initial_protective_stop"), dict)
        else {}
    )
    breakeven_lock = payload.get("breakeven_lock") if isinstance(payload.get("breakeven_lock"), dict) else {}
    trailing_stop = (
        breakeven_lock.get("trailing_stop_response")
        if isinstance(breakeven_lock.get("trailing_stop_response"), dict)
        else {}
    )

    trailing_activation = trailing_stop.get("activatePrice") or trailing_stop.get("activationPrice")
    if trailing_activation in ("", False):
        trailing_activation = None
    trailing_callback = trailing_stop.get("priceRate") or trailing_stop.get("callbackRate")
    if trailing_callback in ("", False):
        trailing_callback = None

    summary = {
        "job_id": payload.get("job_id"),
        "symbol": intent.get("symbol"),
        "side": intent.get("side"),
        "phase": payload.get("phase"),
        "status": payload.get("status"),
        "position_mode": payload.get("position_mode"),
        "margin_type": intent.get("margin_type"),
        "entry_price": intent.get("entry_price"),
        "quantity": intent.get("quantity"),
        "last_mark_price": payload.get("last_mark_price"),
        "extreme_mark_price": payload.get("extreme_mark_price"),
        "initial_stop_price": initial_stop.get("stopPrice") or intent.get("initial_stop_price"),
        "initial_stop_strategy_id": initial_stop.get("strategyId"),
        "breakeven_trigger_price": intent.get("breakeven_trigger_price"),
        "breakeven_stop_price": intent.get("breakeven_stop_price"),
        "trailing_activation_price": trailing_activation,
        "trailing_callback_rate": trailing_callback,
        "breakeven_armed_at": breakeven_lock.get("triggered_at"),
    }
    return summary


def load_json_text(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        text = path.read_text().strip()
        return json.loads(text) if text else None
    except Exception:
        return None


def active_pending_confirmation(payload: Any | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    expires_at = payload.get("expires_at")
    if not expires_at:
        return payload
    try:
        expires = datetime.fromisoformat(str(expires_at))
    except Exception:
        return payload
    now = datetime.now(timezone.utc)
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    if expires <= now:
        return None
    return payload


def find_named_item(items: list[dict[str, Any]] | None, name: str) -> dict[str, Any] | None:
    if not items:
        return None
    for item in items:
        if item.get("name") == name:
            return item
    return None


def extract_balance(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("totalAvailableBalance", "virtualMaxWithdrawAmount", "accountEquity"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def summarize_result(command_name: str, result: Any) -> dict[str, Any]:
    summary: dict[str, Any] = {"command": command_name}
    if not isinstance(result, dict):
        return summary

    account_api = result.get("account_api")
    if isinstance(account_api, dict):
        summary["account_api"] = account_api.get("resolved")
    if result.get("execution_mode"):
        summary["execution_mode"] = result.get("execution_mode")

    if command_name == "probe":
        checks = result.get("checks") or []
        summary["checks"] = {item.get("name"): item.get("ok") for item in checks if isinstance(item, dict)}
        overview = find_named_item(checks, "signed_account_overview")
        if overview:
            summary["available_balance"] = extract_balance(overview.get("payload"))

    elif command_name in {"preflight", "preview"}:
        validation = result.get("validation") or []
        summary["validation"] = {item.get("name"): item.get("ok") for item in validation if isinstance(item, dict)}
        overview = find_named_item(validation, "account_overview")
        if overview:
            summary["available_balance"] = extract_balance(overview.get("payload"))
        strategy_gate = result.get("strategy_gate")
        if isinstance(strategy_gate, dict):
            summary["strategy_gate"] = strategy_gate
        cooldown_gate = result.get("cooldown_gate")
        if isinstance(cooldown_gate, dict):
            summary["cooldown_gate"] = cooldown_gate
        open_positions = result.get("open_positions")
        if isinstance(open_positions, dict):
            summary["open_positions"] = open_positions
        intent = result.get("intent")
        if isinstance(intent, dict):
            summary["intent"] = {
                "symbol": intent.get("symbol"),
                "side": intent.get("side"),
                "leverage": intent.get("leverage"),
                "quote_allocation_usdt": intent.get("quote_allocation_usdt"),
                "allocation_mode": intent.get("allocation_mode"),
                "risk_budget_usdt": intent.get("risk_budget_usdt"),
                "margin_type": intent.get("margin_type"),
            }

    elif command_name == "cycle":
        summary["generated_at"] = result.get("generated_at")

    elif command_name in {"arm", "dispatch"}:
        summary["armed"] = bool(result.get("armed"))
        if result.get("auto_confirmed") is not None:
            summary["auto_confirmed"] = bool(result.get("auto_confirmed"))
        if result.get("arm_notification_error") is not None:
            summary["arm_notification_error"] = result.get("arm_notification_error")
        strategy_gate = result.get("strategy_gate")
        if isinstance(strategy_gate, dict):
            summary["strategy_gate"] = strategy_gate
        cooldown_gate = result.get("cooldown_gate")
        if isinstance(cooldown_gate, dict):
            summary["cooldown_gate"] = cooldown_gate
        open_positions = result.get("open_positions")
        if isinstance(open_positions, dict):
            summary["open_positions"] = open_positions
        if result.get("token"):
            summary["pending_confirmation"] = {
                "token": result.get("token"),
                "expires_at": result.get("expires_at"),
                "confirm_command": result.get("confirm_command"),
            }
        intent = result.get("intent")
        if isinstance(intent, dict):
            summary["intent"] = {
                "symbol": intent.get("symbol"),
                "side": intent.get("side"),
                "leverage": intent.get("leverage"),
                "quote_allocation_usdt": intent.get("quote_allocation_usdt"),
                "allocation_mode": intent.get("allocation_mode"),
                "risk_budget_usdt": intent.get("risk_budget_usdt"),
            }

    elif command_name == "confirm":
        summary["confirmed"] = bool(result.get("ok"))
        summary["confirmation_token"] = result.get("confirmation_token")
        intent = result.get("intent")
        if isinstance(intent, dict):
            summary["intent"] = {
                "symbol": intent.get("symbol"),
                "side": intent.get("side"),
                "leverage": intent.get("leverage"),
                "quote_allocation_usdt": intent.get("quote_allocation_usdt"),
                "allocation_mode": intent.get("allocation_mode"),
                "risk_budget_usdt": intent.get("risk_budget_usdt"),
            }
        worker = result.get("post_fill_worker")
        if isinstance(worker, dict):
            summary["post_fill_worker"] = {
                "ok": worker.get("ok"),
                "pid": worker.get("pid"),
                "job_file": worker.get("job_file"),
                "log_file": worker.get("log_file"),
            }

    elif command_name == "autocycle":
        change_gate = result.get("change_gate")
        if isinstance(change_gate, dict):
            summary["change_gate"] = change_gate
        if result.get("reason") is not None:
            summary["reason"] = result.get("reason")
        if result.get("dispatch_attempted") is not None:
            summary["dispatch_attempted"] = bool(result.get("dispatch_attempted"))
        if result.get("dispatch_result") is not None:
            summary["dispatch_result"] = result.get("dispatch_result")
        active_worker = result.get("active_worker")
        if isinstance(active_worker, dict):
            summary["active_worker"] = active_worker

    return summary


def humanize_side(value: Any) -> str:
    side = str(value or "").upper()
    if side == "BUY":
        return "做多"
    if side == "SELL":
        return "做空"
    return str(value or "未知")


def humanize_bias(value: Any) -> str:
    bias = str(value or "").upper()
    mapping = {
        "LONG": "做多",
        "SHORT": "做空",
        "NONE": "观望",
    }
    return mapping.get(bias, str(value or "未知"))


def humanize_bool(value: Any) -> str:
    if value is True:
        return "是"
    if value is False:
        return "否"
    return str(value)


def humanize_reason(value: Any) -> str:
    reason = str(value or "")
    mapping = {
        "active_position_in_progress": "有活跃仓位在管仓中",
        "candidate_not_significantly_changed": "候选变化不显著",
        "cycle_failed": "扫描或评分失败",
        "dispatch_triggered": "已进入调度链路",
        "max_open_positions_blocked": "已达到最大持仓数",
        "no_supported_directional_bias": "方向不足，不支持开仓",
        "dispatch_preflight_blocked": "调度预检被拦截",
        "strategy_gate_blocked": "策略闸门阻断",
        "candidate_blocked": "候选被阻断",
        "minimum_quantity_blocked": "下单数量低于交易所最小限制",
        "cooldown_blocked": "仍在冷却期",
        "first_candidate_seen": "首次出现候选",
        "top_symbol_changed": "榜首候选已变更",
        "blocked_state_changed": "阻断状态发生变化",
        "cooldown_active": "冷却中",
        "cooldown_elapsed": "冷却已结束",
        "cooldown_disabled": "冷却未启用",
    }
    return mapping.get(reason, reason)


def format_metric(value: Any, digits: int = 2, suffix: str = "") -> str | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return f"{numeric:.{digits}f}{suffix}"


def build_markdown(state: dict[str, Any]) -> str:
    lines = [
        "# 凤凰协议状态",
        "",
        f"- 更新时间：`{state['updated_at']}`",
        f"- 最近命令：`{state['last_command']}`",
        f"- 退出码：`{state['exit_code']}`",
    ]

    if state.get("raw_top_candidate"):
        top = state["raw_top_candidate"]
        lines.extend(
            [
                f"- 原始榜首候选：`{top.get('symbol')}`",
                f"- 原始榜首分数：`{top.get('score')}`",
                f"- 原始榜首方向：`{humanize_bias(top.get('directional_bias'))}` (`{top.get('directional_score')}`)",
            ]
        )
    if state.get("actionable_top_candidate"):
        actionable = state["actionable_top_candidate"]
        lines.extend(
            [
                f"- 可执行榜首候选：`{actionable.get('symbol')}`",
                f"- 可执行榜首分数：`{actionable.get('score')}`",
                f"- 可执行方向：`{humanize_bias(actionable.get('directional_bias'))}` (`{actionable.get('directional_score')}`)",
            ]
        )
        market_confirmation_score = format_metric(actionable.get("market_confirmation_score"))
        execution_quality_score = format_metric(actionable.get("execution_quality_score"))
        event_risk_score = format_metric(actionable.get("event_risk_score"))
        if market_confirmation_score:
            lines.append(f"- 可执行候选市场确认分：`{market_confirmation_score}`")
        if execution_quality_score:
            lines.append(f"- 可执行候选执行质量分：`{execution_quality_score}`")
        if event_risk_score:
            lines.append(f"- 可执行候选事件风险分：`{event_risk_score}`")
        spread_bps = format_metric(actionable.get("spread_bps"), suffix=" bps")
        slippage_bps = format_metric(actionable.get("estimated_slippage_bps"), suffix=" bps")
        basis_pct = format_metric(actionable.get("mark_index_basis_pct"), digits=3, suffix="%")
        funding_rate = format_metric(actionable.get("funding_rate"), digits=4)
        taker_ratio_5m = format_metric(actionable.get("taker_buy_ratio_5m"), digits=3)
        aggressive_flow = format_metric(actionable.get("aggressive_flow_delta"), digits=3)
        liquidation_long = format_metric(actionable.get("liquidation_long_usd"), digits=0)
        liquidation_short = format_metric(actionable.get("liquidation_short_usd"), digits=0)
        listing_age_hours = format_metric(actionable.get("listing_age_hours"), digits=1, suffix=" 小时")
        if spread_bps:
            lines.append(f"- 可执行候选点差：`{spread_bps}`")
        if slippage_bps:
            lines.append(f"- 可执行候选预计滑点：`{slippage_bps}`")
        if basis_pct:
            lines.append(f"- 可执行候选基差：`{basis_pct}`")
        if funding_rate:
            lines.append(f"- 可执行候选资金费率：`{funding_rate}`")
        if taker_ratio_5m:
            lines.append(f"- 可执行候选主动买入占比(5m)：`{taker_ratio_5m}`")
        if aggressive_flow:
            lines.append(f"- 可执行候选主动流向差值：`{aggressive_flow}`")
        if liquidation_long or liquidation_short:
            parts = []
            if liquidation_long:
                parts.append(f"多头清算 `{liquidation_long}` USD")
            if liquidation_short:
                parts.append(f"空头清算 `{liquidation_short}` USD")
            lines.append("- 可执行候选清算分布：" + " / ".join(parts))
        if listing_age_hours:
            lines.append(f"- 可执行候选上市年龄：`{listing_age_hours}`")
        directional_conflicts = actionable.get("directional_conflicts") or []
        if directional_conflicts:
            lines.append(f"- 可执行候选方向冲突：{', '.join(str(item) for item in directional_conflicts)}")
        event_flags = actionable.get("event_flags") or []
        if event_flags:
            lines.append(f"- 可执行候选事件标记：{', '.join(str(item) for item in event_flags[:4])}")
        if state.get("actionable_rank") is not None:
            lines.append(f"- 在原始榜单中的可执行排名：`{state.get('actionable_rank')}`")
    if state.get("raw_top_candidate") and state.get("actionable_top_candidate"):
        if state["raw_top_candidate"].get("symbol") != state["actionable_top_candidate"].get("symbol"):
            blocked = state["raw_top_candidate"].get("blocked_reasons") or []
            if blocked:
                lines.append(f"- 原始榜首阻断原因：{', '.join(str(item) for item in blocked)}")

    result_summary = state.get("result_summary") or {}
    if result_summary.get("account_api"):
        lines.append(f"- 账户模式：`{result_summary['account_api']}`")
    if result_summary.get("execution_mode"):
        lines.append(f"- 执行模式：`{result_summary['execution_mode']}`")
    if result_summary.get("available_balance"):
        lines.append(f"- 可用余额：`{result_summary['available_balance']}`")
    if result_summary.get("strategy_gate"):
        gate = result_summary["strategy_gate"]
        lines.append(f"- 策略闸门：`{humanize_bool(gate.get('ok'))}`")
        if gate.get("minimum_score") is not None:
            lines.append(
                f"- 策略分数：`{gate.get('score')}` / 最低要求 `{gate.get('minimum_score')}`"
            )
        if gate.get("directional_bias") not in (None, "", "NONE"):
            lines.append(
                f"- 策略方向：`{humanize_bias(gate.get('directional_bias'))}` "
                f"(请求 `{humanize_side(gate.get('requested_side'))}` / 预期 `{humanize_side(gate.get('expected_side'))}`)"
            )
        blocked = gate.get("blocked_reasons") or []
        if blocked:
            lines.append(f"- 候选阻断原因：{', '.join(str(item) for item in blocked)}")
    if result_summary.get("cooldown_gate"):
        cooldown = result_summary["cooldown_gate"]
        lines.append(f"- 冷却闸门：`{humanize_bool(cooldown.get('ok'))}`")
        if cooldown.get("minutes_remaining") is not None:
            lines.append(f"- 剩余冷却时间：`{cooldown.get('minutes_remaining')}` 分钟")
    if result_summary.get("open_positions"):
        positions = result_summary["open_positions"]
        lines.append(
            f"- 当前持仓数：`{positions.get('count')}` / 最大允许 `{positions.get('max_allowed')}`"
        )
        symbols = positions.get("symbols") or []
        if symbols:
            lines.append(f"- 当前持仓标的：{', '.join(str(item) for item in symbols)}")

    if result_summary.get("intent"):
        intent = result_summary["intent"]
        lines.append(
            "- 最近交易意图："
            f"`{intent.get('symbol')}` `{humanize_side(intent.get('side'))}` "
            f"`{intent.get('quote_allocation_usdt')}`USDT `{intent.get('leverage')}x`"
        )
        if intent.get("allocation_mode"):
            lines.append(f"- 资金分配模式：`{intent.get('allocation_mode')}`")
        if intent.get("risk_budget_usdt") is not None:
            lines.append(f"- 风险预算：`{intent.get('risk_budget_usdt')}` USDT")

    pending_confirmation = active_pending_confirmation(state.get("pending_confirmation"))
    if isinstance(pending_confirmation, dict):
        lines.append(f"- 待确认令牌：`{pending_confirmation.get('token')}`")
        lines.append(f"- 过期时间：`{pending_confirmation.get('expires_at')}`")
    if result_summary.get("arm_notification_error") is not None:
        lines.append(f"- 待确认通知错误：`{result_summary.get('arm_notification_error')}`")

    if result_summary.get("change_gate"):
        change = result_summary["change_gate"]
        lines.append(f"- 自动循环显著变化：`{humanize_bool(change.get('significant'))}`")
        reasons = change.get("reasons") or []
        if reasons:
            lines.append(f"- 自动循环原因：{', '.join(humanize_reason(item) for item in reasons)}")
    if result_summary.get("dispatch_attempted") is not None:
        lines.append(f"- 是否已尝试自动调度：`{humanize_bool(result_summary.get('dispatch_attempted'))}`")
    if result_summary.get("reason"):
        lines.append(f"- 自动循环跳过原因：`{humanize_reason(result_summary.get('reason'))}`")
    if isinstance(result_summary.get("active_worker"), dict):
        active_skip = result_summary["active_worker"]
        lines.append(
            f"- 自动循环活跃仓位保护：`{active_skip.get('symbol')}` `{humanize_side(active_skip.get('side'))}` "
            f"`{active_skip.get('phase')}`"
        )

    worker = result_summary.get("post_fill_worker")
    if isinstance(worker, dict):
        lines.append(f"- Guardian 管仓任务已启动：`{humanize_bool(worker.get('ok'))}`")
        if worker.get("pid"):
            lines.append(f"- Guardian 进程 PID：`{worker.get('pid')}`")
        if worker.get("job_file"):
            lines.append(f"- Guardian 任务文件：`{worker.get('job_file')}`")

    active_worker = state.get("active_worker")
    if isinstance(active_worker, dict):
        lines.extend(
            [
                f"- 当前活跃实盘仓位：`{active_worker.get('symbol')}` `{humanize_side(active_worker.get('side'))}`",
                f"- 当前管仓阶段：`{active_worker.get('phase')}`",
                f"- 当前持仓模式：`{active_worker.get('position_mode')}` / 保证金 `{active_worker.get('margin_type')}`",
            ]
        )
        if active_worker.get("entry_price") is not None:
            lines.append(f"- 当前入场价：`{active_worker.get('entry_price')}`")
        if active_worker.get("quantity") is not None:
            lines.append(f"- 当前数量：`{active_worker.get('quantity')}`")
        if active_worker.get("last_mark_price") is not None:
            lines.append(f"- 当前最后标记价：`{active_worker.get('last_mark_price')}`")
        if active_worker.get("extreme_mark_price") is not None:
            lines.append(f"- 当前极值标记价：`{active_worker.get('extreme_mark_price')}`")
        if active_worker.get("initial_stop_price") is not None:
            lines.append(
                f"- 当前初始止损：`{active_worker.get('initial_stop_price')}` "
                f"(策略单 `{active_worker.get('initial_stop_strategy_id')}`)"
            )
        if active_worker.get("breakeven_trigger_price") is not None:
            lines.append(f"- 当前保本触发：`{active_worker.get('breakeven_trigger_price')}`")
        if active_worker.get("breakeven_stop_price") is not None:
            lines.append(f"- 当前保本止损：`{active_worker.get('breakeven_stop_price')}`")
        if active_worker.get("trailing_activation_price") is not None:
            lines.append(f"- 当前追踪激活价：`{active_worker.get('trailing_activation_price')}`")
        if active_worker.get("trailing_callback_rate") is not None:
            lines.append(f"- 当前追踪回调：`{active_worker.get('trailing_callback_rate')}`")
        if active_worker.get("breakeven_armed_at") is not None:
            lines.append(f"- 当前保本已触发时间：`{active_worker.get('breakeven_armed_at')}`")

    last_exit = state.get("last_exit")
    if isinstance(last_exit, dict):
        lines.append(f"- 上一笔平仓标的：`{last_exit.get('symbol')}`")
        lines.append(f"- 上一笔平仓结果：`{last_exit.get('outcome')}`")
        if last_exit.get("closed_at"):
            lines.append(f"- 上一笔平仓时间：`{last_exit.get('closed_at')}`")
        actual_settlement = last_exit.get("actual_settlement")
        if isinstance(actual_settlement, dict) and actual_settlement.get("net_pnl_usdt") is not None:
            lines.append(
                f"- 上一笔实际净利润：`{actual_settlement.get('net_pnl_usdt')}` USDT"
            )
        elif last_exit.get("estimated_pnl_usdt_at_last_mark") is not None:
            lines.append(
                f"- 上一笔按最后标记价估算利润：`{last_exit.get('estimated_pnl_usdt_at_last_mark')}` USDT"
            )

    if state.get("stderr_tail"):
        lines.extend(["", "## 最近 STDERR 输出", "", "```text", state["stderr_tail"], "```"])

    skill_digest = load_skill_digest(Path.home() / ".hermes")
    lines.extend(markdown_lines_from_digest(skill_digest))

    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    command = [part for part in args.command if part != "--"]
    command_name = command[0] if command else "unknown"

    phoenix_root = Path(args.phoenix_root)
    hermes_home = Path(args.hermes_home)
    memories_dir = hermes_home / "memories"
    memories_dir.mkdir(parents=True, exist_ok=True)

    stdout_path = Path(args.stdout_file)
    stderr_path = Path(args.stderr_file)

    raw_result = load_json_text(stdout_path)
    candidates = load_json_file(phoenix_root / "phoenix_candidates.openclaw.json")
    snapshot = load_json_file(phoenix_root / "phoenix_snapshot.openclaw.json")
    pending_confirmation = active_pending_confirmation(
        load_json_file(memories_dir / "phoenix_pending_confirmation.json")
    )
    last_exit = load_json_file(memories_dir / "phoenix_last_exit.json")
    active_worker = latest_active_worker(memories_dir)

    updated_at = datetime.now(timezone.utc).isoformat()
    stderr_tail = ""
    if stderr_path.exists():
        lines = [line.rstrip() for line in stderr_path.read_text().splitlines() if line.strip()]
        stderr_tail = "\n".join(lines[-6:])

    state = {
        "updated_at": updated_at,
        "last_command": " ".join(command) if command else "",
        "command_name": command_name,
        "exit_code": args.exit_code,
        "success": args.exit_code == 0,
        "raw_top_candidate": None,
        "actionable_top_candidate": None,
        "actionable_rank": None,
        "candidate_count": candidates.get("candidate_count") if isinstance(candidates, dict) else None,
        "universe_size": candidates.get("universe_size") if isinstance(candidates, dict) else None,
        "snapshot_generated_at": snapshot.get("generated_at") if isinstance(snapshot, dict) else None,
        "result_summary": summarize_result(command_name, raw_result),
        "pending_confirmation": pending_confirmation if isinstance(pending_confirmation, dict) else None,
        "last_exit": last_exit if isinstance(last_exit, dict) else None,
        "active_worker": active_worker if isinstance(active_worker, dict) else None,
        "stderr_tail": stderr_tail,
    }

    settings = load_execution_settings()
    candidate_views = extract_candidate_views(
        candidates if isinstance(candidates, dict) else None,
        minimum_score=settings.strategy_min_score,
        reject_blocked=settings.reject_blocked_candidates,
    )
    state["raw_top_candidate"] = candidate_views.get("raw_top_candidate")
    state["actionable_top_candidate"] = candidate_views.get("actionable_top_candidate")
    state["actionable_rank"] = candidate_views.get("actionable_rank")

    (memories_dir / "phoenix_state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
    (memories_dir / "phoenix_state.md").write_text(build_markdown(state))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
