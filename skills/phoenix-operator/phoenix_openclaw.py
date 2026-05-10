#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import secrets
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import parse, request


BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from phoenix.config import load_execution_settings
from phoenix.guardian_workers import latest_running_worker as latest_running_worker_from_exchange
from phoenix.hermes_notify import (
    hermes_home,
    humanize_bias,
    humanize_sentiment,
    humanize_side,
    send_telegram_message,
)
from phoenix.runtime_state import (
    append_autocycle_history,
    evaluate_significant_candidate_change,
    extract_candidate_views,
    is_actionable_candidate,
    load_autocycle_state,
    save_autocycle_state,
    summarize_candidate,
)

WINDOWS_VENV_PYTHON = REPO_ROOT / ".venv-win" / "Scripts" / "python.exe"
POSIX_VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
if os.name == "nt" and WINDOWS_VENV_PYTHON.exists():
    PYTHON_BIN = WINDOWS_VENV_PYTHON
elif POSIX_VENV_PYTHON.exists():
    PYTHON_BIN = POSIX_VENV_PYTHON
else:
    PYTHON_BIN = Path(sys.executable)
CONFIRMATIONS_DIRNAME = "phoenix_confirmations"
PENDING_CONFIRMATION_FILE = "phoenix_pending_confirmation.json"
GUARDIAN_WORKERS_DIRNAME = "phoenix_guardian_workers"


def build_cmd(script_name: str, *args: str) -> list[str]:
    return [str(PYTHON_BIN), str(REPO_ROOT / script_name), *args]


def quoted_command(cmd: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def run_once(cmd: list[str]) -> int:
    print(f"[phoenix-openclaw] {quoted_command(cmd)}", file=sys.stderr)
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


def run_capture(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    print(f"[phoenix-openclaw] {quoted_command(cmd)}", file=sys.stderr)
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)


def load_json_file(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def decode_json_stdout(stdout: str) -> dict[str, object] | list[object] | None:
    text = (stdout or "").strip()
    if not text:
        return None
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return None
    candidate = lines[-1]
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        return None


def classify_dispatch_skip(
    dispatch_payload: dict[str, object] | list[object] | None,
) -> tuple[bool, str | None, str | None]:
    if not isinstance(dispatch_payload, dict):
        return False, None, None
    preflight = dispatch_payload.get("preflight")
    if not isinstance(preflight, dict):
        return False, None, None
    if preflight.get("ok") is not False:
        return False, None, None

    preflight_error = str(preflight.get("error") or "").strip()
    lowered = preflight_error.lower()
    reason = "dispatch_preflight_blocked"
    if "max-open-positions gate blocked" in preflight_error:
        reason = "max_open_positions_blocked"
    elif "cooldown gate blocked" in preflight_error:
        reason = "cooldown_blocked"
    elif "strategy gate blocked" in preflight_error:
        reason = "strategy_gate_blocked"
    elif "candidate-file strategy gate blocked" in preflight_error:
        reason = "candidate_blocked"
    elif "calculated quantity" in lowered and "smaller than exchange minimum" in lowered:
        reason = "minimum_quantity_blocked"

    return True, reason, preflight_error or None


def run_bounded_stream(cmd: list[str], duration_sec: int) -> int:
    print(f"[phoenix-openclaw] {quoted_command(cmd)}", file=sys.stderr)
    print(
        f"[phoenix-openclaw] streaming for {duration_sec}s before auto-stop",
        file=sys.stderr,
    )
    proc = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    deadline = time.monotonic() + duration_sec
    try:
        while time.monotonic() < deadline:
            if proc.stdout is None:
                break
            line = proc.stdout.readline()
            if line:
                sys.stdout.write(line)
                sys.stdout.flush()
                continue
            if proc.poll() is not None:
                return proc.returncode
            time.sleep(0.1)
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
    print(f"[phoenix-openclaw] stream ended after {duration_sec}s", file=sys.stderr)
    return 0


def run_bounded_capture(cmd: list[str], duration_sec: int) -> tuple[int, str]:
    print(f"[phoenix-openclaw] {quoted_command(cmd)}", file=sys.stderr)
    print(
        f"[phoenix-openclaw] capturing stream for {duration_sec}s before auto-stop",
        file=sys.stderr,
    )
    proc = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    deadline = time.monotonic() + duration_sec
    captured: list[str] = []
    try:
        while time.monotonic() < deadline:
            if proc.stdout is None:
                break
            line = proc.stdout.readline()
            if line:
                captured.append(line.rstrip("\n"))
                continue
            if proc.poll() is not None:
                return proc.returncode, "\n".join(captured)
            time.sleep(0.1)
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
    print(f"[phoenix-openclaw] capture ended after {duration_sec}s", file=sys.stderr)
    return 0, "\n".join(captured)


def add_env_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--env", choices=["testnet", "demo"], default="testnet")


def add_execution_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbol")
    parser.add_argument("--candidate-file", default="phoenix_candidates.json")
    parser.add_argument("--candidate-rank", type=int, default=1)
    parser.add_argument("--quote-allocation", type=float, default=200.0)
    parser.add_argument("--leverage", type=int, default=5)
    parser.add_argument("--entry-price", type=float)
    parser.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    parser.add_argument("--dispatch-reason")
    add_env_arg(parser)


def append_if_present(cmd: list[str], flag: str, value: object | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def resolve_execution_selector(args: argparse.Namespace) -> list[str]:
    if args.symbol:
        return ["--symbol", args.symbol]
    return ["--candidate-file", args.candidate_file, "--candidate-rank", str(args.candidate_rank)]


def explicit_symbol_live_block(settings, args: argparse.Namespace) -> dict[str, object] | None:
    if not getattr(args, "symbol", None):
        return None
    if str(getattr(args, "env", "prod") or "prod").lower() != "prod":
        return None
    if getattr(settings, "allow_explicit_symbol_live", False):
        return None
    return {
        "ok": False,
        "error": (
            "Explicit --symbol live execution is disabled in production mode. "
            "Use candidate-file/candidate-rank so Phoenix can enforce strategy gating."
        ),
        "symbol": args.symbol,
        "execution_mode": settings.execution_mode,
    }


def format_candidate_reason(candidate: dict[str, object] | None) -> str:
    if not isinstance(candidate, dict):
        return "- 理由：暂无可用解释"
    def as_float(value: object) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        return None

    def fmt_float(value: object, digits: int = 2) -> str | None:
        numeric = as_float(value)
        if numeric is None:
            return None
        return f"{numeric:.{digits}f}"

    breakdown = candidate.get("score_breakdown")
    drivers: list[str] = []
    if isinstance(breakdown, dict):
        ranked = sorted(
            (
                (str(key), float(value))
                for key, value in breakdown.items()
                if isinstance(value, (int, float)) and float(value) != 0.0
            ),
            key=lambda item: abs(item[1]),
            reverse=True,
        )
        for key, value in ranked[:4]:
            drivers.append(f"{key}={value:+.2f}")

    market_bits: list[str] = []
    oi_1m = fmt_float(candidate.get("oi_delta_1m_pct"))
    oi_5m = fmt_float(candidate.get("oi_delta_5m_pct"))
    oi_15m = fmt_float(candidate.get("oi_delta_15m_pct"))
    px_1m = fmt_float(candidate.get("price_change_1m_pct"))
    px_5m = fmt_float(candidate.get("price_change_5m_pct"))
    px_15m = fmt_float(candidate.get("price_change_15m_pct"))
    if px_1m or px_5m or px_15m:
        market_bits.append(
            "price Δ "
            + "/".join(
                part
                for part in [
                    f"1m {px_1m}%" if px_1m else None,
                    f"5m {px_5m}%" if px_5m else None,
                    f"15m {px_15m}%" if px_15m else None,
                ]
                if part
            )
        )
    if oi_1m or oi_5m or oi_15m:
        market_bits.append(
            "oi Δ "
            + "/".join(
                part
                for part in [
                    f"1m {oi_1m}%" if oi_1m else None,
                    f"5m {oi_5m}%" if oi_5m else None,
                    f"15m {oi_15m}%" if oi_15m else None,
                ]
                if part
            )
        )
    vol_short = fmt_float(candidate.get("volume_5m_ratio_short"))
    vol_medium = fmt_float(candidate.get("volume_5m_ratio_medium"))
    vol_long = fmt_float(candidate.get("volume_5m_ratio_long"))
    if vol_short or vol_medium or vol_long:
        market_bits.append(
            "vol ratio "
            + "/".join(
                part
                for part in [
                    f"short {vol_short}x" if vol_short else None,
                    f"medium {vol_medium}x" if vol_medium else None,
                    f"long {vol_long}x" if vol_long else None,
                ]
                if part
            )
        )
    market_confirmation_score = fmt_float(candidate.get("market_confirmation_score"), digits=2)
    if market_confirmation_score:
        market_bits.append(f"市场确认分 {market_confirmation_score}")

    social_bits: list[str] = []
    square_trending_rank = candidate.get("square_trending_rank")
    square_search_rank = candidate.get("square_search_rank")
    square_discovery_score = fmt_float(candidate.get("square_discovery_score"), digits=2)
    if isinstance(square_trending_rank, (int, float)) or isinstance(square_search_rank, (int, float)):
        square_parts = []
        if isinstance(square_trending_rank, (int, float)):
            square_parts.append(f"广场趋势榜 #{int(square_trending_rank)}")
        if isinstance(square_search_rank, (int, float)):
            square_parts.append(f"站内热搜榜 #{int(square_search_rank)}")
        if square_discovery_score:
            square_parts.append(f"发现分 {square_discovery_score}")
        social_bits.append(" | ".join(square_parts))
    social_rank = candidate.get("social_rank")
    if isinstance(social_rank, (int, float)):
        social_bits.append(f"社交排名 #{int(social_rank)}")
    sentiment = candidate.get("social_sentiment")
    if sentiment:
        social_bits.append(f"情绪 {humanize_sentiment(sentiment)}")
    hype_change = fmt_float(candidate.get("social_hype_change_pct"))
    freshness = fmt_float(candidate.get("social_signal_freshness"), digits=3)
    if hype_change:
        social_bits.append(f"热度变化 {hype_change}%")
    if freshness:
        social_bits.append(f"新鲜度 {freshness}")

    smart_money_bits: list[str] = []
    sm_traders = candidate.get("smart_money_traders")
    if isinstance(sm_traders, (int, float)) and float(sm_traders) > 0:
        smart_money_bits.append(f"活跃交易者 {int(sm_traders)}")
    sm_strength = fmt_float(candidate.get("smart_money_weighted_buy_strength"), digits=2)
    if sm_strength:
        smart_money_bits.append(f"买入力度 {sm_strength}")
    sm_age = fmt_float(candidate.get("smart_money_latest_age_minutes"), digits=1)
    if sm_age:
        smart_money_bits.append(f"信号年龄 {sm_age} 分钟")
    sm_tags = candidate.get("smart_money_tags")
    if isinstance(sm_tags, list) and sm_tags:
        smart_money_bits.append("标签 " + ", ".join(str(tag) for tag in sm_tags[:3]))

    topic_bits: list[str] = []
    topic_names = candidate.get("topic_names")
    if isinstance(topic_names, list) and topic_names:
        topic_bits.append("题材 " + ", ".join(str(name) for name in topic_names[:2]))
    topic_inflow = fmt_float(candidate.get("topic_net_inflow_usd"), digits=0)
    if topic_inflow:
        topic_bits.append(f"净流入 {topic_inflow} USD")
    topic_age = fmt_float(candidate.get("topic_latest_age_minutes"), digits=1)
    if topic_age:
        topic_bits.append(f"信号年龄 {topic_age} 分钟")
    topic_freshness = fmt_float(candidate.get("topic_freshness"), digits=3)
    if topic_freshness:
        topic_bits.append(f"新鲜度 {topic_freshness}")

    risk_bits: list[str] = []
    blocked = candidate.get("blocked_reasons")
    if isinstance(blocked, list) and blocked:
        risk_bits.append("阻断原因 " + ", ".join(str(reason) for reason in blocked[:3]))
    audit_flags = candidate.get("audit_flags")
    if isinstance(audit_flags, list) and audit_flags:
        risk_bits.append("审计标记 " + ", ".join(str(flag) for flag in audit_flags[:3]))

    execution_bits: list[str] = []
    execution_quality_score = fmt_float(candidate.get("execution_quality_score"), digits=2)
    if execution_quality_score:
        execution_bits.append(f"执行质量分 {execution_quality_score}")
    spread_bps = fmt_float(candidate.get("spread_bps"), digits=2)
    if spread_bps:
        execution_bits.append(f"点差 {spread_bps} bps")
    depth_imbalance = fmt_float(candidate.get("depth_imbalance"), digits=3)
    if depth_imbalance:
        execution_bits.append(f"深度失衡 {depth_imbalance}")
    slippage_bps = fmt_float(candidate.get("estimated_slippage_bps"), digits=2)
    if slippage_bps:
        execution_bits.append(f"预计滑点 {slippage_bps} bps")
    funding_rate = fmt_float(candidate.get("funding_rate"), digits=4)
    if funding_rate:
        execution_bits.append(f"资金费率 {funding_rate}")
    basis_pct = fmt_float(candidate.get("mark_index_basis_pct"), digits=3)
    if basis_pct:
        execution_bits.append(f"基差 {basis_pct}%")
    taker_1m = fmt_float(candidate.get("taker_buy_ratio_1m"), digits=3)
    taker_5m = fmt_float(candidate.get("taker_buy_ratio_5m"), digits=3)
    if taker_1m or taker_5m:
        execution_bits.append(
            "主动买入占比 "
            + "/".join(
                part
                for part in [
                    f"1m {taker_1m}" if taker_1m else None,
                    f"5m {taker_5m}" if taker_5m else None,
                ]
                if part
            )
        )
    aggressive_flow = fmt_float(candidate.get("aggressive_flow_delta"), digits=3)
    if aggressive_flow:
        execution_bits.append(f"主动流向差值 {aggressive_flow}")
    liq_long = fmt_float(candidate.get("liquidation_long_usd"), digits=0)
    liq_short = fmt_float(candidate.get("liquidation_short_usd"), digits=0)
    liq_count = candidate.get("liquidation_event_count")
    if liq_long or liq_short:
        liquidation_parts = []
        if liq_long:
            liquidation_parts.append(f"多头清算 {liq_long} USD")
        if liq_short:
            liquidation_parts.append(f"空头清算 {liq_short} USD")
        if isinstance(liq_count, (int, float)):
            liquidation_parts.append(f"事件数 {int(liq_count)}")
        execution_bits.append("清算：" + " | ".join(liquidation_parts))
    listing_age = fmt_float(candidate.get("listing_age_hours"), digits=1)
    if listing_age:
        execution_bits.append(f"上市年龄 {listing_age} 小时")

    event_bits: list[str] = []
    event_risk_score = fmt_float(candidate.get("event_risk_score"), digits=2)
    if event_risk_score:
        event_bits.append(f"事件风险分 {event_risk_score}")
    event_flags = candidate.get("event_flags")
    if isinstance(event_flags, list) and event_flags:
        event_bits.append("事件标记 " + ", ".join(str(flag) for flag in event_flags[:3]))
    directional_conflicts = candidate.get("directional_conflicts")
    if isinstance(directional_conflicts, list) and directional_conflicts:
        event_bits.append("方向冲突 " + ", ".join(str(item) for item in directional_conflicts[:3]))

    reasons: list[str] = []
    bias = str(candidate.get("directional_bias") or "NONE").upper()
    direction_score = fmt_float(candidate.get("directional_score"), digits=2)
    direction_breakdown = candidate.get("directional_breakdown")
    if bias != "NONE" or direction_score:
        direction_bits = [f"方向 {humanize_bias(bias)}"]
        if direction_score:
            direction_bits.append(f"分值 {direction_score}")
        if isinstance(direction_breakdown, dict) and direction_breakdown:
            top_direction = sorted(
                (
                    (str(key), float(value))
                    for key, value in direction_breakdown.items()
                    if isinstance(value, (int, float)) and float(value) != 0.0
                ),
                key=lambda item: abs(item[1]),
                reverse=True,
            )[:3]
            if top_direction:
                direction_bits.append(
                    "驱动 " + ", ".join(f"{key}={value:+.2f}" for key, value in top_direction)
                )
        reasons.append("方向判断：" + " | ".join(direction_bits))
    if drivers:
        reasons.append("核心驱动：" + ", ".join(drivers))
    if market_bits:
        reasons.append("市场确认：" + " | ".join(market_bits))
    if social_bits:
        reasons.append("社交信号：" + " | ".join(social_bits))
    if smart_money_bits:
        reasons.append("聪明钱：" + " | ".join(smart_money_bits))
    if topic_bits:
        reasons.append("题材：" + " | ".join(topic_bits))
    if execution_bits:
        reasons.append("执行质量：" + " | ".join(execution_bits))
    if event_bits:
        reasons.append("事件与冲突：" + " | ".join(event_bits))
    if risk_bits:
        reasons.append("风险项：" + " | ".join(risk_bits))
    if not reasons:
        return "- 理由：分数达标的候选"
    return "\n".join(f"- {item}" for item in reasons)


def resolve_dispatch_side(candidate: dict[str, object] | None, settings, *, fallback_side: str) -> tuple[str | None, str]:
    if not isinstance(candidate, dict):
        return None, "no_candidate"
    bias = str(candidate.get("directional_bias") or "NONE").upper()
    if bias == "LONG":
        return "BUY", "candidate_bias_long"
    if bias == "SHORT":
        if settings.allow_auto_short:
            return "SELL", "candidate_bias_short"
        return None, "auto_short_disabled"
    return None, "candidate_bias_none"


def notify_arm_ready(
    *,
    token: str,
    expires_at: datetime,
    candidate: dict[str, object] | None,
    intent: dict[str, object] | None,
    strategy_gate: dict[str, object] | None,
    dispatch_reason: str | None,
) -> str | None:
    symbol = "UNKNOWN"
    score = None
    if isinstance(candidate, dict):
        symbol = str(candidate.get("symbol") or symbol)
        if isinstance(candidate.get("score"), (int, float)):
            score = float(candidate["score"])
    if isinstance(intent, dict):
        symbol = str(intent.get("symbol") or symbol)
    lines = [
        "*凤凰协议待确认开仓*",
        f"- 标的：`{symbol}`",
        f"- 确认码：`{token}`",
        f"- 过期时间：`{expires_at.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}`",
    ]
    if score is not None:
        lines.append(f"- 候选分数：`{score:.4f}`")
    if isinstance(intent, dict):
        qty = intent.get("quantity")
        allocation = intent.get("quote_allocation_usdt")
        leverage = intent.get("leverage")
        side = intent.get("side")
        stop_price = intent.get("initial_stop_price")
        be_trigger = intent.get("breakeven_trigger_price")
        if side is not None:
            lines.append(f"- 方向：`{humanize_side(side)}`")
        if allocation is not None:
            lines.append(f"- 保证金占用：`{float(allocation):.4f} USDT`")
        if leverage is not None:
            lines.append(f"- 杠杆：`{int(leverage)}x`")
        if qty is not None:
            lines.append(f"- 数量：`{qty}`")
        if stop_price is not None:
            lines.append(f"- 初始止损：`{stop_price}`")
        if be_trigger is not None:
            lines.append(f"- 保本触发：`{be_trigger}`")
    if isinstance(strategy_gate, dict):
        lines.append(f"- 策略闸门：`{'通过' if strategy_gate.get('ok') else '拒绝'}`")
    if dispatch_reason:
        lines.append(f"- 触发原因：`{dispatch_reason}`")
    lines.append("")
    lines.append(format_candidate_reason(candidate))
    lines.append("")
    lines.append(f"回复：`confirm {token}`")
    return send_telegram_message("\n".join(lines))


def confirmations_dir() -> Path:
    path = hermes_home() / "memories" / CONFIRMATIONS_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def guardian_workers_dir() -> Path:
    path = hermes_home() / "memories" / GUARDIAN_WORKERS_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def latest_running_worker() -> dict[str, object] | None:
    return latest_running_worker_from_exchange(guardian_workers_dir())


def latest_running_worker_local_only() -> dict[str, object] | None:
    """Return the newest locally recorded running worker without exchange I/O.

    This is intentionally less authoritative than latest_running_worker(), but it
    keeps command/chat status checks responsive when the exchange endpoint is
    slow, blocked, or being accessed through a proxy.
    """
    running_records: list[tuple[float, dict[str, object]]] = []
    for record_path in guardian_workers_dir().glob("*.json"):
        payload = load_json_file(record_path)
        if not isinstance(payload, dict):
            continue
        if str(payload.get("status") or "").lower() != "running":
            continue
        try:
            mtime = record_path.stat().st_mtime
        except OSError:
            continue
        running_records.append((mtime, payload))
    if not running_records:
        return None
    return max(running_records, key=lambda item: item[0])[1]


def summarize_active_worker(worker: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(worker, dict):
        return None
    intent = worker.get("intent") if isinstance(worker.get("intent"), dict) else {}
    initial_stop = worker.get("initial_protective_stop") if isinstance(worker.get("initial_protective_stop"), dict) else {}
    breakeven_lock = worker.get("breakeven_lock") if isinstance(worker.get("breakeven_lock"), dict) else {}
    trailing = (
        breakeven_lock.get("trailing_stop_response")
        if isinstance(breakeven_lock.get("trailing_stop_response"), dict)
        else {}
    )
    return {
        "job_id": worker.get("job_id"),
        "status": worker.get("status"),
        "phase": worker.get("phase"),
        "symbol": intent.get("symbol"),
        "side": intent.get("side"),
        "position_mode": worker.get("position_mode"),
        "margin_type": intent.get("margin_type"),
        "entry_price": intent.get("entry_price"),
        "quantity": intent.get("quantity"),
        "last_mark_price": worker.get("last_mark_price"),
        "extreme_mark_price": worker.get("extreme_mark_price"),
        "initial_stop_price": initial_stop.get("stopPrice") or intent.get("initial_stop_price"),
        "initial_stop_strategy_id": initial_stop.get("strategyId"),
        "breakeven_trigger_price": intent.get("breakeven_trigger_price"),
        "breakeven_stop_price": intent.get("breakeven_stop_price"),
        "trailing_activation_price": (
            trailing.get("activatePrice")
            or trailing.get("activationPrice")
            or None
        ),
        "trailing_callback_rate": (
            trailing.get("priceRate")
            or trailing.get("callbackRate")
            or None
        ),
        "breakeven_armed_at": breakeven_lock.get("triggered_at"),
    }


def pending_confirmation_path() -> Path:
    path = hermes_home() / "memories"
    path.mkdir(parents=True, exist_ok=True)
    return path / PENDING_CONFIRMATION_FILE


def confirmation_record_path(token: str) -> Path:
    return confirmations_dir() / f"{token}.json"


def cleanup_expired_confirmations() -> None:
    now = datetime.now(timezone.utc)
    for record_path in confirmations_dir().glob("*.json"):
        try:
            payload = json.loads(record_path.read_text(encoding="utf-8"))
            expires_at = datetime.fromisoformat(str(payload.get("expires_at")))
        except Exception:
            continue
        if expires_at <= now:
            record_path.unlink(missing_ok=True)
            pending_path = pending_confirmation_path()
            if pending_path.exists():
                try:
                    pending_payload = json.loads(pending_path.read_text(encoding="utf-8"))
                except Exception:
                    pending_payload = {}
                if pending_payload.get("token") == payload.get("token"):
                    pending_path.unlink(missing_ok=True)


def generate_confirmation_token() -> str:
    return secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:10].upper()


def save_confirmation_record(record: dict[str, object]) -> None:
    record_path = confirmation_record_path(str(record["token"]))
    text = json.dumps(record, ensure_ascii=False, indent=2) + "\n"
    record_path.write_text(text, encoding="utf-8")
    pending_confirmation_path().write_text(text, encoding="utf-8")


def load_confirmation_record(token: str) -> dict[str, object] | None:
    record_path = confirmation_record_path(token)
    if not record_path.exists():
        return None
    return json.loads(record_path.read_text(encoding="utf-8"))


def consume_confirmation_record(token: str) -> dict[str, object] | None:
    record = load_confirmation_record(token)
    if record is None:
        return None
    (confirmations_dir() / f"{token}.json").unlink(missing_ok=True)
    pending_path = pending_confirmation_path()
    if pending_path.exists():
        try:
            pending_payload = json.loads(pending_path.read_text(encoding="utf-8"))
        except Exception:
            pending_payload = {}
        if pending_payload.get("token") == token:
            pending_path.unlink(missing_ok=True)
    return record


def validation_failed(validation: object) -> bool:
    if not isinstance(validation, list):
        return True
    return any(isinstance(item, dict) and item.get("ok") is False for item in validation)


def load_candidate_payload(candidate_file: str) -> dict[str, object] | None:
    return load_json_file(REPO_ROOT / candidate_file)


def locate_candidate(
    payload: dict[str, object] | None,
    *,
    symbol: str,
) -> tuple[dict[str, object] | None, int | None]:
    if not isinstance(payload, dict):
        return None, None
    target = symbol.upper()
    for index, item in enumerate(payload.get("top_candidates") or [], start=1):
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol") or "").upper() == target:
            return summarize_candidate(dict(item)), index
    return None, None


def build_candidate_gate(candidate: dict[str, object] | None, settings) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {
            "ok": False,
            "reason": "candidate_not_found",
        }
    score = candidate.get("score")
    blocked_reasons = [str(item) for item in candidate.get("blocked_reasons") or [] if str(item)]
    directional_bias = str(candidate.get("directional_bias") or "NONE").upper()
    gate = {
        "ok": is_actionable_candidate(
            candidate,
            minimum_score=settings.strategy_min_score,
            reject_blocked=settings.reject_blocked_candidates,
        ),
        "score": score,
        "minimum_score": settings.strategy_min_score,
        "blocked_reasons": blocked_reasons,
        "directional_bias": directional_bias,
    }
    if blocked_reasons:
        gate["reason"] = "candidate_blocked"
    elif directional_bias == "NONE":
        gate["reason"] = "no_supported_directional_bias"
    elif isinstance(score, (int, float)) and float(score) < settings.strategy_min_score:
        gate["reason"] = "score_below_threshold"
    else:
        gate["reason"] = "candidate_actionable" if gate["ok"] else "candidate_not_actionable"
    return gate


def build_market_view(candidate: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {"ok": False, "reason": "candidate_not_found"}
    return {
        "ok": True,
        "symbol": candidate.get("symbol"),
        "mark_price": candidate.get("mark_price"),
        "price_change_1m_pct": candidate.get("price_change_1m_pct"),
        "price_change_5m_pct": candidate.get("price_change_5m_pct"),
        "price_change_15m_pct": candidate.get("price_change_15m_pct"),
        "oi_delta_pct": candidate.get("oi_delta_pct"),
        "oi_delta_1m_pct": candidate.get("oi_delta_1m_pct"),
        "oi_delta_5m_pct": candidate.get("oi_delta_5m_pct"),
        "oi_delta_15m_pct": candidate.get("oi_delta_15m_pct"),
        "volume_5m_ratio": candidate.get("volume_5m_ratio"),
        "volume_5m_ratio_short": candidate.get("volume_5m_ratio_short"),
        "volume_5m_ratio_medium": candidate.get("volume_5m_ratio_medium"),
        "volume_5m_ratio_long": candidate.get("volume_5m_ratio_long"),
        "spread_bps": candidate.get("spread_bps"),
        "depth_bid_5": candidate.get("depth_bid_5"),
        "depth_ask_5": candidate.get("depth_ask_5"),
        "depth_imbalance": candidate.get("depth_imbalance"),
        "estimated_slippage_bps": candidate.get("estimated_slippage_bps"),
        "estimated_slippage_for_order_usdt": candidate.get("estimated_slippage_for_order_usdt"),
        "funding_rate": candidate.get("funding_rate"),
        "next_funding_time_ms": candidate.get("next_funding_time_ms"),
        "premium_index": candidate.get("premium_index"),
        "mark_index_basis_pct": candidate.get("mark_index_basis_pct"),
        "onboard_date_ms": candidate.get("onboard_date_ms"),
        "listing_age_hours": candidate.get("listing_age_hours"),
        "taker_buy_ratio_1m": candidate.get("taker_buy_ratio_1m"),
        "taker_buy_ratio_5m": candidate.get("taker_buy_ratio_5m"),
        "aggressive_flow_delta": candidate.get("aggressive_flow_delta"),
        "taker_buy_volume_5m": candidate.get("taker_buy_volume_5m"),
        "taker_sell_volume_5m": candidate.get("taker_sell_volume_5m"),
        "liquidation_long_usd": candidate.get("liquidation_long_usd"),
        "liquidation_short_usd": candidate.get("liquidation_short_usd"),
        "liquidation_event_count": candidate.get("liquidation_event_count"),
        "squeeze_probability_hint": candidate.get("squeeze_probability_hint"),
        "market_confirmation_score": candidate.get("market_confirmation_score"),
        "directional_bias": candidate.get("directional_bias"),
        "directional_score": candidate.get("directional_score"),
    }


def build_risk_view(candidate: dict[str, object] | None, settings) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {"ok": False, "reason": "candidate_not_found"}
    gate = build_candidate_gate(candidate, settings)
    return {
        "ok": True,
        "symbol": candidate.get("symbol"),
        "strategy_gate": gate,
        "confidence": candidate.get("confidence"),
        "blocked_reasons": candidate.get("blocked_reasons") or [],
        "audit_flags": candidate.get("audit_flags") or [],
        "event_flags": candidate.get("event_flags") or [],
        "event_risk_score": candidate.get("event_risk_score"),
        "directional_conflicts": candidate.get("directional_conflicts") or [],
        "listing_age_hours": candidate.get("listing_age_hours"),
        "next_funding_time_ms": candidate.get("next_funding_time_ms"),
        "liquidation_long_usd": candidate.get("liquidation_long_usd"),
        "liquidation_short_usd": candidate.get("liquidation_short_usd"),
        "liquidation_event_count": candidate.get("liquidation_event_count"),
        "squeeze_probability_hint": candidate.get("squeeze_probability_hint"),
        "score_breakdown": candidate.get("score_breakdown") or {},
    }


def build_execution_cmd(script_name: str, args: argparse.Namespace) -> list[str]:
    cmd = build_cmd(
        script_name,
        *resolve_execution_selector(args),
        "--quote-allocation",
        str(args.quote_allocation),
        "--leverage",
        str(args.leverage),
        "--side",
        args.side,
        "--env",
        args.env,
    )
    append_if_present(cmd, "--entry-price", args.entry_price)
    return cmd


def handle_sentry(args: argparse.Namespace) -> int:
    cmd = build_cmd("sentry.py")
    if args.symbols:
        cmd.extend(["--symbols", *args.symbols])
    elif args.symbols_file:
        cmd.extend(["--symbols-file", args.symbols_file])
    append_if_present(cmd, "--heartbeat-sec", args.heartbeat_sec)
    return run_bounded_stream(cmd, args.duration_sec)


def handle_scan(args: argparse.Namespace) -> int:
    cmd = build_cmd("phoenix_data_scout.py")
    append_if_present(cmd, "--alpha-refresh-sec", args.alpha_refresh_sec)
    append_if_present(cmd, "--oi-refresh-sec", args.oi_refresh_sec)
    append_if_present(cmd, "--oi-concurrency", args.oi_concurrency)
    append_if_present(cmd, "--volume-baseline-points", args.volume_baseline_points)
    append_if_present(cmd, "--volume-spike-ratio", args.volume_spike_ratio)
    append_if_present(cmd, "--oi-jump-pct", args.oi_jump_pct)
    append_if_present(cmd, "--top-n", args.top_n)
    append_if_present(cmd, "--max-symbols", args.max_symbols)
    append_if_present(cmd, "--snapshot-file", args.snapshot_file)
    return run_bounded_stream(cmd, args.duration_sec)


def handle_judge(args: argparse.Namespace) -> int:
    cmd = build_cmd(
        "phoenix_judge.py",
        "--snapshot-file",
        args.snapshot_file,
        "--output-file",
        args.output_file,
        "--top-n",
        str(args.top_n),
        "--audit-top-n",
        str(args.audit_top_n),
        "--env",
        args.env,
    )
    return run_once(cmd)


def handle_cycle(args: argparse.Namespace) -> int:
    scan_args = argparse.Namespace(
        alpha_refresh_sec=args.alpha_refresh_sec,
        oi_refresh_sec=args.oi_refresh_sec,
        oi_concurrency=args.oi_concurrency,
        volume_baseline_points=args.volume_baseline_points,
        volume_spike_ratio=args.volume_spike_ratio,
        oi_jump_pct=args.oi_jump_pct,
        top_n=args.top_n,
        max_symbols=args.max_symbols,
        snapshot_file=args.snapshot_file,
        duration_sec=args.scan_duration_sec,
    )
    scan_code = handle_scan(scan_args)
    if scan_code != 0:
        return scan_code
    judge_args = argparse.Namespace(
        snapshot_file=args.snapshot_file,
        output_file=args.output_file,
        top_n=args.top_n,
        audit_top_n=args.audit_top_n,
        env=args.env,
    )
    return handle_judge(judge_args)


def handle_autocycle(args: argparse.Namespace) -> int:
    cleanup_expired_confirmations()
    scan_cmd = build_cmd("phoenix_data_scout.py")
    append_if_present(scan_cmd, "--alpha-refresh-sec", args.alpha_refresh_sec)
    append_if_present(scan_cmd, "--oi-refresh-sec", args.oi_refresh_sec)
    append_if_present(scan_cmd, "--oi-concurrency", args.oi_concurrency)
    append_if_present(scan_cmd, "--volume-baseline-points", args.volume_baseline_points)
    append_if_present(scan_cmd, "--volume-spike-ratio", args.volume_spike_ratio)
    append_if_present(scan_cmd, "--oi-jump-pct", args.oi_jump_pct)
    append_if_present(scan_cmd, "--top-n", args.top_n)
    append_if_present(scan_cmd, "--max-symbols", args.max_symbols)
    append_if_present(scan_cmd, "--snapshot-file", args.snapshot_file)

    cycle_code, scan_stdout = run_bounded_capture(scan_cmd, args.scan_duration_sec)
    if cycle_code == 0:
        judge_cmd = build_cmd(
            "phoenix_judge.py",
            "--snapshot-file",
            args.snapshot_file,
            "--output-file",
            args.output_file,
            "--top-n",
            str(args.top_n),
            "--audit-top-n",
            str(args.audit_top_n),
            "--env",
            args.env,
        )
        judge_completed = run_capture(judge_cmd)
        if judge_completed.stderr:
            sys.stderr.write(judge_completed.stderr)
        cycle_code = judge_completed.returncode
        candidates = decode_json_stdout(judge_completed.stdout)
        if not isinstance(candidates, dict):
            candidates = load_json_file(REPO_ROOT / args.output_file)
    else:
        candidates = load_json_file(REPO_ROOT / args.output_file)
    previous_state = load_autocycle_state() or {}
    settings = load_execution_settings()
    candidate_views = extract_candidate_views(
        candidates if isinstance(candidates, dict) else None,
        minimum_score=settings.strategy_min_score,
        reject_blocked=settings.reject_blocked_candidates,
    )
    current_top = candidate_views.get("raw_top_candidate")
    current_actionable = candidate_views.get("actionable_top_candidate")
    current_actionable_rank = candidate_views.get("actionable_rank")
    previous_top = previous_state.get("raw_top_candidate")
    previous_actionable = previous_state.get("actionable_top_candidate")
    change_gate = evaluate_significant_candidate_change(
        current_actionable if isinstance(current_actionable, dict) else None,
        previous_actionable if isinstance(previous_actionable, dict) else None,
        absolute_delta=settings.significant_score_delta,
        relative_delta_pct=settings.significant_score_delta_pct,
    )

    now = datetime.now(timezone.utc).isoformat()
    autocycle_record: dict[str, object] = {
        "updated_at": now,
        "command": "autocycle",
        "cycle_exit_code": cycle_code,
        "execution_mode": settings.execution_mode,
        "raw_top_candidate": current_top,
        "previous_raw_top_candidate": previous_top if isinstance(previous_top, dict) else None,
        "actionable_top_candidate": current_actionable,
        "previous_actionable_top_candidate": previous_actionable if isinstance(previous_actionable, dict) else None,
        "actionable_rank": current_actionable_rank,
        "change_gate": change_gate,
        "dispatch_attempted": False,
        "scan_stdout_tail": "\n".join(
            [line for line in (scan_stdout or "").splitlines() if line.strip()][-6:]
        ),
    }

    if cycle_code != 0:
        autocycle_record["ok"] = False
        autocycle_record["reason"] = "cycle_failed"
        save_autocycle_state(autocycle_record)
        append_autocycle_history(autocycle_record)
        print(json.dumps(autocycle_record, ensure_ascii=False))
        return cycle_code

    if not isinstance(current_actionable, dict) or current_actionable_rank is None:
        autocycle_record["ok"] = True
        autocycle_record["reason"] = "no_actionable_candidate"
        save_autocycle_state(autocycle_record)
        append_autocycle_history(autocycle_record)
        print(json.dumps(autocycle_record, ensure_ascii=False))
        return 0

    if not change_gate.get("significant"):
        autocycle_record["ok"] = True
        autocycle_record["reason"] = "candidate_not_significantly_changed"
        save_autocycle_state(autocycle_record)
        append_autocycle_history(autocycle_record)
        print(json.dumps(autocycle_record, ensure_ascii=False))
        return 0

    active_worker = latest_running_worker()
    if isinstance(active_worker, dict):
        intent = active_worker.get("intent") if isinstance(active_worker.get("intent"), dict) else {}
        autocycle_record["ok"] = True
        autocycle_record["reason"] = "active_position_in_progress"
        autocycle_record["active_worker"] = {
            "job_id": active_worker.get("job_id"),
            "symbol": intent.get("symbol"),
            "side": intent.get("side"),
            "phase": active_worker.get("phase"),
            "status": active_worker.get("status"),
        }
        save_autocycle_state(autocycle_record)
        append_autocycle_history(autocycle_record)
        print(json.dumps(autocycle_record, ensure_ascii=False))
        return 0

    dispatch_side, dispatch_side_reason = resolve_dispatch_side(
        current_actionable,
        settings,
        fallback_side=args.side,
    )
    autocycle_record["dispatch_side"] = dispatch_side
    autocycle_record["dispatch_side_reason"] = dispatch_side_reason
    if dispatch_side is None:
        autocycle_record["ok"] = True
        autocycle_record["reason"] = "no_supported_directional_bias"
        save_autocycle_state(autocycle_record)
        append_autocycle_history(autocycle_record)
        print(json.dumps(autocycle_record, ensure_ascii=False))
        return 0

    dispatch_reason = "|".join(str(item) for item in change_gate.get("reasons") or [])
    # dispatch itself needs to follow the execution mode, so call the local handler instead of the raw executor
    dispatch_completed = None
    dispatch_exit_code = 0
    try:
        dispatch_cmd = build_cmd(
            "skills/phoenix-operator/phoenix_openclaw.py",
            "dispatch",
            "--candidate-file",
            args.output_file,
            "--candidate-rank",
            str(current_actionable_rank),
            "--quote-allocation",
            str(args.quote_allocation),
            "--leverage",
            str(args.leverage),
            "--side",
            dispatch_side,
            "--env",
            args.env,
            "--ttl-sec",
            str(args.ttl_sec),
            "--dispatch-reason",
            dispatch_reason,
        )
        dispatch_completed = run_capture(dispatch_cmd)
        dispatch_exit_code = dispatch_completed.returncode
        if dispatch_completed.stderr:
            sys.stderr.write(dispatch_completed.stderr)
    except Exception as exc:  # noqa: BLE001
        autocycle_record["ok"] = False
        autocycle_record["dispatch_attempted"] = True
        autocycle_record["reason"] = "dispatch_exception"
        autocycle_record["dispatch_error"] = str(exc)
        save_autocycle_state(autocycle_record)
        append_autocycle_history(autocycle_record)
        print(json.dumps(autocycle_record, ensure_ascii=False))
        return 1

    dispatch_payload = decode_json_stdout(dispatch_completed.stdout) if dispatch_completed is not None else None
    autocycle_record["dispatch_attempted"] = True
    autocycle_record["dispatch_exit_code"] = dispatch_exit_code
    autocycle_record["dispatch_result"] = dispatch_payload
    skip_ok, skip_reason, skip_detail = classify_dispatch_skip(dispatch_payload)
    if dispatch_exit_code == 0:
        autocycle_record["ok"] = True
        autocycle_record["reason"] = "dispatch_triggered"
        save_autocycle_state(autocycle_record)
        append_autocycle_history(autocycle_record)
        print(json.dumps(autocycle_record, ensure_ascii=False))
        return 0

    if skip_ok:
        autocycle_record["ok"] = True
        autocycle_record["reason"] = skip_reason or "dispatch_preflight_blocked"
        if skip_detail:
            autocycle_record["dispatch_skip_detail"] = skip_detail
        save_autocycle_state(autocycle_record)
        append_autocycle_history(autocycle_record)
        print(json.dumps(autocycle_record, ensure_ascii=False))
        return 0

    autocycle_record["ok"] = False
    autocycle_record["reason"] = "dispatch_triggered"
    save_autocycle_state(autocycle_record)
    append_autocycle_history(autocycle_record)
    print(json.dumps(autocycle_record, ensure_ascii=False))
    return 1


def handle_probe(args: argparse.Namespace) -> int:
    cmd = build_cmd("phoenix_probe.py", "--symbol", args.symbol, "--env", args.env)
    return run_once(cmd)


def handle_status(args: argparse.Namespace) -> int:
    cleanup_expired_confirmations()
    settings = load_execution_settings()
    candidates = load_json_file(REPO_ROOT / args.candidate_file)
    candidate_views = extract_candidate_views(
        candidates if isinstance(candidates, dict) else None,
        minimum_score=settings.strategy_min_score,
        reject_blocked=settings.reject_blocked_candidates,
    )
    autocycle_state = load_autocycle_state()
    pending = load_json_file(pending_confirmation_path())
    worker = latest_running_worker_local_only() if getattr(args, "fast", False) else latest_running_worker()
    active_worker = summarize_active_worker(worker)
    last_exit = load_json_file(hermes_home() / "memories" / "phoenix_last_exit.json")

    payload: dict[str, object] = {
        "execution_mode": settings.execution_mode,
        "status_mode": "fast_local" if getattr(args, "fast", False) else "exchange_verified",
        "raw_top_candidate": candidate_views.get("raw_top_candidate"),
        "actionable_top_candidate": candidate_views.get("actionable_top_candidate"),
        "actionable_rank": candidate_views.get("actionable_rank"),
        "autocycle_state": autocycle_state,
        "pending_confirmation": pending,
        "active_worker": active_worker,
        "last_exit": last_exit,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def handle_why(args: argparse.Namespace) -> int:
    payload = load_candidate_payload(args.candidate_file)
    candidate, rank = locate_candidate(payload, symbol=args.symbol)
    settings = load_execution_settings()
    gate = build_candidate_gate(candidate, settings)
    result = {
        "ok": candidate is not None,
        "symbol": args.symbol.upper(),
        "candidate_rank": rank,
        "candidate": candidate,
        "strategy_gate": gate,
        "reason_text": format_candidate_reason(candidate),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if candidate is not None else 1


def handle_why_not(args: argparse.Namespace) -> int:
    payload = load_candidate_payload(args.candidate_file)
    candidate, rank = locate_candidate(payload, symbol=args.symbol)
    settings = load_execution_settings()
    gate = build_candidate_gate(candidate, settings)
    if candidate is None:
        result = {
            "ok": False,
            "symbol": args.symbol.upper(),
            "reason": "candidate_not_found_in_shortlist",
            "candidate_file": args.candidate_file,
        }
        print(json.dumps(result, ensure_ascii=False))
        return 1
    if gate.get("ok"):
        result = {
            "ok": True,
            "symbol": args.symbol.upper(),
            "candidate_rank": rank,
            "strategy_gate": gate,
            "actionable": True,
            "reason": "candidate_is_actionable",
            "message": "该候选当前可执行，没有被策略门槛拦截。请改用 why / market / risk 查看正向理由。",
            "reason_text": format_candidate_reason(candidate),
        }
        print(json.dumps(result, ensure_ascii=False))
        return 0
    result = {
        "ok": True,
        "symbol": args.symbol.upper(),
        "candidate_rank": rank,
        "strategy_gate": gate,
        "why_not": {
            "blocked_reasons": candidate.get("blocked_reasons") or [],
            "directional_bias": candidate.get("directional_bias"),
            "directional_conflicts": candidate.get("directional_conflicts") or [],
            "event_flags": candidate.get("event_flags") or [],
            "event_risk_score": candidate.get("event_risk_score"),
        },
        "reason_text": format_candidate_reason(candidate),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


def handle_market(args: argparse.Namespace) -> int:
    payload = load_candidate_payload(args.candidate_file)
    candidate, rank = locate_candidate(payload, symbol=args.symbol)
    result = build_market_view(candidate)
    result["symbol"] = args.symbol.upper()
    result["candidate_rank"] = rank
    result["reason_text"] = format_candidate_reason(candidate)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if candidate is not None else 1


def handle_risk(args: argparse.Namespace) -> int:
    payload = load_candidate_payload(args.candidate_file)
    candidate, rank = locate_candidate(payload, symbol=args.symbol)
    settings = load_execution_settings()
    result = build_risk_view(candidate, settings)
    result["symbol"] = args.symbol.upper()
    result["candidate_rank"] = rank
    result["reason_text"] = format_candidate_reason(candidate)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if candidate is not None else 1


def handle_preview(args: argparse.Namespace, *, preflight: bool) -> int:
    cmd = build_execution_cmd("phoenix_executor.py", args)
    if preflight:
        cmd.append("--test-order")
    return run_once(cmd)


def handle_arm(args: argparse.Namespace) -> int:
    cleanup_expired_confirmations()
    settings = load_execution_settings()
    explicit_symbol_error = explicit_symbol_live_block(settings, args)
    if explicit_symbol_error is not None:
        print(json.dumps({"armed": False, **explicit_symbol_error}, ensure_ascii=False))
        return 1
    cmd = build_execution_cmd("phoenix_executor.py", args)
    cmd.append("--test-order")
    completed = run_capture(cmd)
    if completed.stderr:
        sys.stderr.write(completed.stderr)

    payload = decode_json_stdout(completed.stdout)
    if not isinstance(payload, dict):
        print(
            json.dumps(
                {
                    "armed": False,
                    "error": "Could not parse Phoenix preflight JSON output.",
                },
                ensure_ascii=False,
            )
        )
        return 1

    if completed.returncode != 0 or validation_failed(payload.get("validation")):
        print(
            json.dumps(
                {
                    "armed": False,
                    "error": "Phoenix preflight failed; no confirmation token issued.",
                    "preflight": payload,
                },
                ensure_ascii=False,
            )
        )
        return 1

    token = generate_confirmation_token()
    created_at = datetime.now(timezone.utc)
    expires_at = created_at + timedelta(seconds=args.ttl_sec)
    intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
    record: dict[str, object] = {
        "token": token,
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "command": "confirm",
        "symbol": intent.get("symbol"),
        "side": intent.get("side", args.side),
        "quote_allocation": intent.get("quote_allocation_usdt", args.quote_allocation),
        "leverage": intent.get("leverage", args.leverage),
        "env": args.env,
        "entry_price": intent.get("entry_price", args.entry_price),
        "candidate": payload.get("candidate"),
        "strategy_gate": payload.get("strategy_gate"),
        "open_positions": payload.get("open_positions"),
        "account_api": payload.get("account_api"),
        "intent": payload.get("intent"),
        "plan": payload.get("plan"),
        "validation": payload.get("validation"),
        "dispatch_reason": args.dispatch_reason,
        "notes": (
            "Confirm executes the reviewed entry_market plus initial_protective_stop, then starts "
            "a detached Guardian worker that watches for the breakeven trigger, arms the trailing stop, "
            "and cleans up residual protection after the position closes."
        ),
    }
    save_confirmation_record(record)
    arm_notification_error = notify_arm_ready(
        token=token,
        expires_at=expires_at,
        candidate=payload.get("candidate") if isinstance(payload.get("candidate"), dict) else None,
        intent=payload.get("intent") if isinstance(payload.get("intent"), dict) else None,
        strategy_gate=payload.get("strategy_gate") if isinstance(payload.get("strategy_gate"), dict) else None,
        dispatch_reason=args.dispatch_reason,
    )
    print(
        json.dumps(
                {
                    "execution_mode": settings.execution_mode,
                    "armed": True,
                "token": token,
                "expires_at": expires_at.isoformat(),
                "confirm_command": f"confirm {token}",
                "candidate": payload.get("candidate"),
                "strategy_gate": payload.get("strategy_gate"),
                "open_positions": payload.get("open_positions"),
                "intent": payload.get("intent"),
                "account_api": payload.get("account_api"),
                "validation": payload.get("validation"),
                "dispatch_reason": args.dispatch_reason,
                "arm_notification_error": arm_notification_error,
                "notes": record["notes"],
            },
            ensure_ascii=False,
        )
    )
    return 0


def handle_confirm(args: argparse.Namespace) -> int:
    cleanup_expired_confirmations()
    record = load_confirmation_record(args.token)
    if record is None:
        print(
            json.dumps(
                {
                    "confirmed": False,
                    "error": f"Confirmation token {args.token} was not found.",
                },
                ensure_ascii=False,
            )
        )
        return 1

    expires_at = datetime.fromisoformat(str(record["expires_at"]))
    if expires_at <= datetime.now(timezone.utc):
        consume_confirmation_record(args.token)
        print(
            json.dumps(
                {
                    "confirmed": False,
                    "error": f"Confirmation token {args.token} has expired.",
                },
                ensure_ascii=False,
            )
        )
        return 1

    record_path = confirmation_record_path(args.token)
    record_env = str(record.get("env") or "testnet").strip().lower()
    if record_env not in {"testnet", "demo"}:
        consume_confirmation_record(args.token)
        print(
            json.dumps(
                {
                    "confirmed": False,
                    "confirmation_token": args.token,
                    "error": "Confirmation record env is not testnet/demo; live execution blocked before signed API access.",
                    "record_env": record_env,
                },
                ensure_ascii=False,
            )
        )
        return 1
    cmd = build_cmd(
        "phoenix_live_execute.py",
        "--env",
        record_env,
        "--confirmation-record",
        str(record_path),
        "--confirmation-token",
        args.token,
    )
    completed = run_capture(cmd)
    if completed.stderr:
        sys.stderr.write(completed.stderr)

    payload = decode_json_stdout(completed.stdout)
    consume_confirmation_record(args.token)

    if isinstance(payload, dict):
        payload["confirmation_token"] = args.token

    if payload is None:
        print(
            json.dumps(
                {
                    "confirmed": False,
                    "confirmation_token": args.token,
                    "error": "Could not parse Phoenix live execution JSON output.",
                },
                ensure_ascii=False,
            )
        )
        return 1

    print(json.dumps(payload, ensure_ascii=False))
    return 0 if completed.returncode == 0 else 1


def handle_dispatch(args: argparse.Namespace) -> int:
    settings = load_execution_settings()
    execution_mode = settings.execution_mode
    explicit_symbol_error = explicit_symbol_live_block(settings, args)
    if explicit_symbol_error is not None:
        print(
            json.dumps(
                {
                    "dispatched": False,
                    "reason": "explicit_symbol_live_disabled",
                    **explicit_symbol_error,
                },
                ensure_ascii=False,
            )
        )
        return 1
    if execution_mode == "DRY_RUN_ONLY":
        cmd = build_execution_cmd("phoenix_executor.py", args)
        cmd.append("--test-order")
        completed = run_capture(cmd)
        if completed.stderr:
            sys.stderr.write(completed.stderr)
        payload = decode_json_stdout(completed.stdout)
        if isinstance(payload, dict):
            payload["execution_mode"] = execution_mode
            payload["dispatched"] = False
            payload["reason"] = "Execution mode is DRY_RUN_ONLY."
            print(json.dumps(payload, ensure_ascii=False))
            return 0 if completed.returncode == 0 else 1
        print(
            json.dumps(
                {
                    "execution_mode": execution_mode,
                    "dispatched": False,
                    "error": "Could not parse Phoenix preflight JSON output.",
                },
                ensure_ascii=False,
            )
        )
        return 1

    if execution_mode == "MANUAL_CONFIRM":
        return handle_arm(args)

    if execution_mode == "AUTO_CONFIRM_WHEN_RULES_PASS":
        print(
            json.dumps(
                {
                    "execution_mode": execution_mode,
                    "dispatched": False,
                    "auto_confirmed": False,
                    "reason": "AUTO_CONFIRM_WHEN_RULES_PASS is hard-blocked by P0 safety patch; use MANUAL_CONFIRM on testnet only.",
                },
                ensure_ascii=False,
            )
        )
        return 1

    print(
        json.dumps(
            {
                "execution_mode": execution_mode,
                "error": f"Unsupported PHOENIX_EXECUTION_MODE value: {execution_mode}",
            },
            ensure_ascii=False,
        )
    )
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hermes/OpenClaw-safe operator for Project Phoenix with token-gated live execution.",
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    sentry_parser = subparsers.add_parser(
        "sentry",
        help="Run Phoenix ticker sentry for a bounded duration.",
    )
    sentry_parser.add_argument("--symbols", nargs="*")
    sentry_parser.add_argument("--symbols-file", default="alpha_symbols.top_candidates.txt")
    sentry_parser.add_argument("--heartbeat-sec", type=int)
    sentry_parser.add_argument("--duration-sec", type=int, default=15)
    sentry_parser.set_defaults(handler=handle_sentry)

    scan_parser = subparsers.add_parser(
        "scan",
        help="Run bounded public scout collection and stop automatically.",
    )
    scan_parser.add_argument("--duration-sec", type=int, default=20)
    scan_parser.add_argument("--alpha-refresh-sec", type=int)
    scan_parser.add_argument("--oi-refresh-sec", type=int)
    scan_parser.add_argument("--oi-concurrency", type=int)
    scan_parser.add_argument("--volume-baseline-points", type=int)
    scan_parser.add_argument("--volume-spike-ratio", type=float)
    scan_parser.add_argument("--oi-jump-pct", type=float)
    scan_parser.add_argument("--top-n", type=int, default=10)
    scan_parser.add_argument("--max-symbols", type=int)
    scan_parser.add_argument("--snapshot-file", default="phoenix_snapshot.openclaw.json")
    scan_parser.set_defaults(handler=handle_scan)

    judge_parser = subparsers.add_parser(
        "judge",
        help="Rank a scout snapshot into Phoenix candidates.",
    )
    judge_parser.add_argument("--snapshot-file", default="phoenix_snapshot.openclaw.json")
    judge_parser.add_argument("--output-file", default="phoenix_candidates.openclaw.json")
    judge_parser.add_argument("--top-n", type=int, default=10)
    judge_parser.add_argument("--audit-top-n", type=int, default=8)
    add_env_arg(judge_parser)
    judge_parser.set_defaults(handler=handle_judge)

    cycle_parser = subparsers.add_parser(
        "cycle",
        help="Run a bounded scout pass and then judge the resulting snapshot.",
    )
    cycle_parser.add_argument("--scan-duration-sec", type=int, default=20)
    cycle_parser.add_argument("--alpha-refresh-sec", type=int)
    cycle_parser.add_argument("--oi-refresh-sec", type=int)
    cycle_parser.add_argument("--oi-concurrency", type=int)
    cycle_parser.add_argument("--volume-baseline-points", type=int)
    cycle_parser.add_argument("--volume-spike-ratio", type=float)
    cycle_parser.add_argument("--oi-jump-pct", type=float)
    cycle_parser.add_argument("--top-n", type=int, default=10)
    cycle_parser.add_argument("--audit-top-n", type=int, default=20)
    cycle_parser.add_argument("--max-symbols", type=int)
    cycle_parser.add_argument("--snapshot-file", default="phoenix_snapshot.openclaw.json")
    cycle_parser.add_argument("--output-file", default="phoenix_candidates.openclaw.json")
    add_env_arg(cycle_parser)
    cycle_parser.set_defaults(handler=handle_cycle)

    autocycle_parser = subparsers.add_parser(
        "autocycle",
        help="Run cycle, compare the top candidate to the previous run, and only dispatch when the change is significant.",
    )
    autocycle_parser.add_argument("--scan-duration-sec", type=int, default=20)
    autocycle_parser.add_argument("--alpha-refresh-sec", type=int)
    autocycle_parser.add_argument("--oi-refresh-sec", type=int)
    autocycle_parser.add_argument("--oi-concurrency", type=int)
    autocycle_parser.add_argument("--volume-baseline-points", type=int)
    autocycle_parser.add_argument("--volume-spike-ratio", type=float)
    autocycle_parser.add_argument("--oi-jump-pct", type=float)
    autocycle_parser.add_argument("--top-n", type=int, default=10)
    autocycle_parser.add_argument("--audit-top-n", type=int, default=20)
    autocycle_parser.add_argument("--max-symbols", type=int)
    autocycle_parser.add_argument("--snapshot-file", default="phoenix_snapshot.openclaw.json")
    autocycle_parser.add_argument("--output-file", default="phoenix_candidates.openclaw.json")
    autocycle_parser.add_argument("--quote-allocation", type=float, default=200.0)
    autocycle_parser.add_argument("--leverage", type=int, default=5)
    autocycle_parser.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    autocycle_parser.add_argument("--ttl-sec", type=int, default=300)
    add_env_arg(autocycle_parser)
    autocycle_parser.set_defaults(handler=handle_autocycle)

    probe_parser = subparsers.add_parser(
        "probe",
        help="Run Phoenix signed-account probe against Binance.",
    )
    probe_parser.add_argument("--symbol", default="BTCUSDT")
    add_env_arg(probe_parser)
    probe_parser.set_defaults(handler=handle_probe)

    status_parser = subparsers.add_parser(
        "status",
        help="Show unified Phoenix/Hermes control-plane state, including actionable candidate and active worker.",
    )
    status_parser.add_argument("--candidate-file", default="phoenix_candidates.openclaw.json")
    status_parser.add_argument(
        "--fast",
        action="store_true",
        help="Skip exchange worker verification and use local runtime state only.",
    )
    status_parser.set_defaults(handler=handle_status)

    why_parser = subparsers.add_parser(
        "why",
        help="Explain why a candidate is ranked and why Phoenix likes it.",
    )
    why_parser.add_argument("symbol")
    why_parser.add_argument("--candidate-file", default="phoenix_candidates.openclaw.json")
    why_parser.set_defaults(handler=handle_why)

    why_not_parser = subparsers.add_parser(
        "why-not",
        help="Explain why a candidate is blocked, skipped, or not actionable.",
    )
    why_not_parser.add_argument("symbol")
    why_not_parser.add_argument("--candidate-file", default="phoenix_candidates.openclaw.json")
    why_not_parser.set_defaults(handler=handle_why_not)

    market_parser = subparsers.add_parser(
        "market",
        help="Show market-structure diagnostics for a candidate.",
    )
    market_parser.add_argument("symbol")
    market_parser.add_argument("--candidate-file", default="phoenix_candidates.openclaw.json")
    market_parser.set_defaults(handler=handle_market)

    risk_parser = subparsers.add_parser(
        "risk",
        help="Show risk and gating diagnostics for a candidate.",
    )
    risk_parser.add_argument("symbol")
    risk_parser.add_argument("--candidate-file", default="phoenix_candidates.openclaw.json")
    risk_parser.set_defaults(handler=handle_risk)

    preview_parser = subparsers.add_parser(
        "preview",
        help="Build a deterministic order plan without authenticated validation.",
    )
    add_execution_args(preview_parser)
    preview_parser.set_defaults(handler=lambda args: handle_preview(args, preflight=False))

    preflight_parser = subparsers.add_parser(
        "preflight",
        help="Run authenticated dry-run validation without live order placement.",
    )
    add_execution_args(preflight_parser)
    preflight_parser.set_defaults(handler=lambda args: handle_preview(args, preflight=True))

    arm_parser = subparsers.add_parser(
        "arm",
        help="Run preflight and issue a short-lived confirmation token for live entry + initial stop.",
    )
    add_execution_args(arm_parser)
    arm_parser.add_argument("--ttl-sec", type=int, default=300)
    arm_parser.set_defaults(handler=handle_arm)

    dispatch_parser = subparsers.add_parser(
        "dispatch",
        help="Follow PHOENIX_EXECUTION_MODE: dry-run only, arm for manual confirm, or auto-confirm after passing rules.",
    )
    add_execution_args(dispatch_parser)
    dispatch_parser.add_argument("--ttl-sec", type=int, default=300)
    dispatch_parser.set_defaults(handler=handle_dispatch)

    confirm_parser = subparsers.add_parser(
        "confirm",
        help="Consume a confirmation token and place a guarded live entry plus initial stop.",
    )
    confirm_parser.add_argument("token")
    confirm_parser.set_defaults(handler=handle_confirm)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
