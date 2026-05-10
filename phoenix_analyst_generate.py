#!/usr/bin/env python3
"""Generate Phoenix Analyst evidence reports.

This script is intentionally read-only with respect to Phoenix inputs. It writes
new report artifacts under analyst_reports/ and does not edit strategy,
runtime, service, config, env, or source files.
"""

from __future__ import annotations

import csv
import datetime as dt
import glob
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
from collections import Counter, defaultdict
from typing import Any, Iterable


OLD_STRATEGIES = [
    "volatility_long",
    "volatility_short",
    "trend_pullback_long",
    "trend_breakout_long",
    "impulse_pullback_long",
    "impulse_bounce_short",
    "flush_reversion_long",
    "pressure_breakdown_short",
    "explore_reclaim_long",
    "explore_reject_short",
    "major_reclaim_long",
    "micro_reclaim_long",
    "micro_reject_short",
    "range_reversion_long",
    "range_reversion_short",
    "swing_pullback_long",
    "swing_bounce_short",
    "failed_bounce_short",
]


FAILURE_MODE_DEFS: dict[str, dict[str, Any]] = {
    "no_follow_through": {
        "zh": "入场后没有延续",
        "rule": "time_stop / MFE 很低 / 动态退出显示没有及时盈利",
    },
    "late_entry": {"zh": "入场太晚", "rule": "延迟较高或入场后 MFE 小、MAE 大"},
    "fake_breakout": {"zh": "假突破", "rule": "breakout 相关策略亏损或很快回撤"},
    "fake_reclaim": {"zh": "假 reclaim，重新站回后又失败", "rule": "reclaim/reject 相关策略亏损"},
    "liquidity_trap": {"zh": "流动性陷阱", "rule": "低流动性或盘口成本高且亏损"},
    "spread_too_wide": {"zh": "买卖价差过宽", "rule": "entry/exit spread proxy 高于 5 bps"},
    "slippage_too_high": {"zh": "滑点过高", "rule": "estimated/realized slippage 高于 5 bps"},
    "cost_exceeds_edge": {
        "zh": "成本吃掉优势",
        "rule": "gross PnL >= 0 但 net PnL < 0，或成本大于毛收益",
    },
    "chop_regime": {"zh": "震荡行情", "rule": "time_stop 且方向没有延续"},
    "wrong_horizon": {"zh": "持仓周期错配", "rule": "MFE 出现但最终 time_stop/回吐"},
    "adverse_btc_regime": {"zh": "BTC 大环境逆风", "rule": "BTC regime 标记为 risk_off / adverse"},
    "adverse_eth_regime": {"zh": "ETH 大环境逆风", "rule": "ETH regime 标记为 adverse"},
    "oi_signal_failed": {"zh": "OI 信号失败", "rule": "OI 相关策略无正 net edge"},
    "bidwall_removed": {"zh": "买墙撤掉", "rule": "需要 orderbook persistence 字段，目前多为 unavailable"},
    "askwall_absorption": {"zh": "卖墙吸收", "rule": "需要 orderbook absorption 字段，目前多为 unavailable"},
    "liquidation_continuation_instead_of_reversal": {
        "zh": "爆仓后继续单边而不是反转",
        "rule": "flush/reversion 做反转但后续继续不利",
    },
    "reversal_instead_of_continuation": {
        "zh": "本应延续却反转",
        "rule": "breakout/trend/impulse 延续策略亏损",
    },
    "time_stop_decay": {"zh": "时间止损损耗", "rule": "exit_reason = time_stop"},
    "stale_signal": {"zh": "信号过期", "rule": "latency/order delay 高于 5 秒"},
    "overfit_backtest_pattern": {
        "zh": "历史回测拟合出来的形态",
        "rule": "backtest 强、forward shadow/testnet 弱",
    },
}

OI_FEATURE_COLUMNS = [
    "source_file",
    "symbol",
    "playbook",
    "side",
    "horizon_sec",
    "liquidity_bucket",
    "oi_change_5m_pct",
    "oi_change_15m_pct",
    "volume_ratio",
    "spread_bps_at_entry",
    "estimated_slippage_bps",
    "round_trip_fee_bps",
    "gross_return_bps",
    "net_return_bps",
    "estimated_total_cost_bps",
    "mfe_pct",
    "mae_pct",
    "exit_reason",
    "research_only",
    "live_unlock_eligible",
]


def now_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().strftime("%Y%m%d_%H%M%S")


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def fmt_num(value: Any, digits: int = 4) -> str:
    num = safe_float(value)
    if num is None:
        return ""
    return f"{num:.{digits}f}"


def avg(values: Iterable[float | None]) -> float | None:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def med(values: Iterable[float | None]) -> float | None:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return statistics.median(nums)


def total(values: Iterable[float | None]) -> float:
    return sum(v for v in values if v is not None)


def profit_factor(values: Iterable[float | None]) -> float | None:
    wins = sum(v for v in values if v is not None and v > 0)
    losses = abs(sum(v for v in values if v is not None and v < 0))
    if losses == 0:
        return None if wins == 0 else 999.0
    return wins / losses


def win_rate(values: Iterable[float | None]) -> float | None:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return 100.0 * sum(1 for v in nums if v > 0) / len(nums)


def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    obj["_source_file"] = str(path)
                    yield obj
    except OSError:
        return


def read_json(path: Path) -> Any | None:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        keys: list[str] = []
        seen = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    keys.append(key)
                    seen.add(key)
        fieldnames = keys
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: scalar_csv(row.get(k)) for k in fieldnames})


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write(text.rstrip() + "\n")


def scalar_csv(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if value is None:
        return ""
    return value


def run_cmd(args: list[str], timeout: int = 20) -> str:
    try:
        out = subprocess.check_output(args, stderr=subprocess.STDOUT, timeout=timeout, text=True)
        return out.strip()
    except Exception as exc:  # noqa: BLE001 - report generator must keep going.
        return f"unavailable: {type(exc).__name__}: {exc}"


def du_bytes(path: Path) -> int | None:
    if not path.exists():
        return None
    out = run_cmd(["du", "-sb", str(path)], timeout=60)
    try:
        return int(out.split()[0])
    except Exception:
        total_size = 0
        try:
            if path.is_file():
                return path.stat().st_size
            for dirpath, dirnames, filenames in os.walk(path):
                dirnames[:] = [
                    d for d in dirnames if d not in {".venv", ".venv-win", ".git", "binance_public_zip_cache"}
                ]
                for filename in filenames:
                    try:
                        total_size += (Path(dirpath) / filename).stat().st_size
                    except OSError:
                        continue
            return total_size
        except OSError:
            return None


def human_bytes(num: int | None) -> str:
    if num is None:
        return "not_found"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{num}B"


def collect_vps_status(root: Path) -> dict[str, Any]:
    disk = shutil.disk_usage(root)
    category_paths = {
        "project_main": [root],
        "collector_event_collect_v6": [root / "signal_lab_runs" / "event_collect_v6_speed_boost"],
        "shadow_active": [root / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"],
        "testnet_active_req2": [root / "round_runner_reports_req2_telemetry_20260506_162834"],
        "replay_backtest_signal_lab_replay": [root / "signal_lab_replay"],
        "logs": [root / "logs"],
        "reports_named_dirs": [root / "phoenix_reports", root / "hermes_reports"],
    }
    for p in root.glob("round_runner_reports*"):
        category_paths.setdefault("testnet_round_runner_reports_all", []).append(p)
        category_paths.setdefault("reports_named_dirs", []).append(p)
    all_shadow = list((root / "signal_lab_runs").glob("**/mainnet_shadow"))
    category_paths["shadow_all_mainnet_shadow_dirs"] = all_shadow
    skip_dirs = {".venv", ".venv-win", ".git", "binance_public_zip_cache", "__pycache__"}
    cache_tmp = []
    for dirpath, dirnames, _filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        for dirname in dirnames:
            lowered = dirname.lower()
            if dirname == "__pycache__" or "cache" in lowered or "tmp" in lowered or "temp" in lowered:
                cache_tmp.append(Path(dirpath) / dirname)
    category_paths["cache_tmp"] = cache_tmp
    sizes = {}
    for key, paths in category_paths.items():
        sizes[key] = sum((du_bytes(p) or 0) for p in paths)
    top_files: list[dict[str, Any]] = []
    recent_by_top: dict[str, dict[str, Any]] = defaultdict(lambda: {"files": 0, "bytes": 0})
    cutoff = dt.datetime.now().timestamp() - 7 * 86400
    for dirpath, dirnames, filenames in os.walk(root):
        path_obj = Path(dirpath)
        if any(part in skip_dirs for part in path_obj.parts):
            dirnames[:] = []
            continue
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        for filename in filenames:
            p = path_obj / filename
            try:
                st = p.stat()
            except OSError:
                continue
            top_files.append({"path": str(p), "bytes": st.st_size, "size": human_bytes(st.st_size)})
            if st.st_mtime >= cutoff:
                rel = p.relative_to(root)
                top = rel.parts[0] if rel.parts else "(root)"
                recent_by_top[top]["files"] += 1
                recent_by_top[top]["bytes"] += st.st_size
    top_files = sorted(top_files, key=lambda x: x["bytes"], reverse=True)[:20]
    recent_rows = [
        {"top_level": k, "files": v["files"], "bytes": v["bytes"], "size": human_bytes(v["bytes"])}
        for k, v in recent_by_top.items()
    ]
    recent_rows.sort(key=lambda x: x["bytes"], reverse=True)
    top_dirs_output = run_cmd(["du", "-B1", "-d", "2", str(root)], timeout=120)
    top_dirs = []
    for line in top_dirs_output.splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) == 2 and parts[0].isdigit():
            top_dirs.append({"path": parts[1], "bytes": int(parts[0]), "size": human_bytes(int(parts[0]))})
    top_dirs.sort(key=lambda x: x["bytes"], reverse=True)
    use_pct = round(disk.used / disk.total * 100, 2)
    risk = "绿" if use_pct < 70 else "黄" if use_pct < 85 else "红"
    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).astimezone().isoformat(),
        "root": str(root),
        "disk_total": disk.total,
        "disk_used": disk.used,
        "disk_free": disk.free,
        "disk_use_pct": use_pct,
        "disk_free_human": human_bytes(disk.free),
        "risk": risk,
        "category_sizes": {
            k: {"bytes": v, "size": human_bytes(v)} for k, v in sorted(sizes.items())
        },
        "recent_7d_by_top_level": recent_rows[:20],
        "top20_files": top_files,
        "top20_dirs": top_dirs[:20],
        "processes": run_cmd(
            [
                "bash",
                "-lc",
                "ps -eo pid,ppid,etime,cmd --sort=pid | grep -Ei 'phoenix|collector|shadow|testnet|dashboard|uvicorn|gunicorn|streamlit|fastapi' | grep -v grep || true",
            ],
            timeout=20,
        ),
        "systemd_running": run_cmd(
            [
                "bash",
                "-lc",
                "systemctl list-units --type=service --state=running --no-pager 2>/dev/null | grep -Ei 'phoenix|collector|shadow|testnet|dashboard' || true",
            ],
            timeout=20,
        ),
        "delete_executed": False,
    }


def strategy_id(row: dict[str, Any]) -> str:
    return str(
        row.get("strategy_id")
        or row.get("setup")
        or row.get("entry_reason")
        or row.get("playbook")
        or row.get("shadow_branch_label")
        or "unknown"
    )


def playbook_id(row: dict[str, Any]) -> str:
    return str(row.get("playbook") or row.get("strategy_id") or row.get("shadow_branch_label") or "unknown")


def row_net_pnl(row: dict[str, Any]) -> float | None:
    for key in ("net_pnl_usdt", "realized_pnl_usdt", "pnl_usdt"):
        val = safe_float(row.get(key))
        if val is not None:
            return val
    return None


def row_gross_pnl(row: dict[str, Any]) -> float | None:
    for key in ("gross_pnl_usdt", "pnl_usdt"):
        val = safe_float(row.get(key))
        if val is not None:
            return val
    return None


def estimated_cost_bps(row: dict[str, Any]) -> float | None:
    fields = [
        "fee_bps_effective",
        "estimated_slippage_bps",
        "realized_slippage_proxy_bps",
        "spread_proxy_at_entry_bps",
        "spread_proxy_at_exit_bps",
    ]
    values = [safe_float(row.get(k)) for k in fields]
    nums = [abs(v) for v in values if v is not None]
    if not nums:
        return None
    fee = safe_float(row.get("fee_bps_effective"))
    if fee is None:
        fee = 8.0
    slip = safe_float(row.get("estimated_slippage_bps")) or safe_float(row.get("realized_slippage_proxy_bps")) or 0.0
    spread = (safe_float(row.get("spread_proxy_at_entry_bps")) or 0.0) + (
        safe_float(row.get("spread_proxy_at_exit_bps")) or 0.0
    )
    funding = abs(safe_float(row.get("funding_paid_or_received")) or 0.0)
    return abs(fee) + abs(slip) + abs(spread) + funding


def dynamic_value(row: dict[str, Any], key: str) -> Any:
    sim = row.get("dynamic_exit_simulation") or row.get("dynamic_exit_simulated_result")
    if isinstance(sim, dict):
        return sim.get(key)
    return None


def classify_failure(row: dict[str, Any]) -> list[str]:
    modes: list[str] = []
    sid = strategy_id(row)
    net = row_net_pnl(row)
    gross = row_gross_pnl(row)
    exit_reason = str(row.get("exit_reason") or "").lower()
    latency = safe_float(row.get("latency_ms")) or safe_float(row.get("entry_to_fill_latency_ms"))
    mae = safe_float(row.get("mae_pct"))
    mfe = safe_float(row.get("mfe_pct"))
    cost_bps = estimated_cost_bps(row)
    spread = max(
        [
            safe_float(row.get("spread_proxy_at_entry_bps")) or 0.0,
            safe_float(row.get("spread_proxy_at_exit_bps")) or 0.0,
            safe_float(row.get("spread_bps_at_entry")) or 0.0,
        ]
    )
    slippage = max(
        [
            safe_float(row.get("estimated_slippage_bps")) or 0.0,
            safe_float(row.get("realized_slippage_proxy_bps")) or 0.0,
            safe_float(row.get("estimated_slippage")) or 0.0,
        ]
    )
    if net is not None and net < 0 and "time_stop" in exit_reason:
        modes += ["no_follow_through", "time_stop_decay", "chop_regime"]
    if net is not None and gross is not None and gross >= 0 and net < 0:
        modes.append("cost_exceeds_edge")
    if spread > 5:
        modes.append("spread_too_wide")
    if slippage > 5:
        modes.append("slippage_too_high")
    if latency is not None and latency > 5000:
        modes.append("stale_signal")
    if any(token in sid for token in ("breakout", "trend", "impulse")) and net is not None and net < 0:
        modes.append("reversal_instead_of_continuation")
    if "breakout" in sid and net is not None and net < 0:
        modes.append("fake_breakout")
    if any(token in sid for token in ("reclaim", "reject")) and net is not None and net < 0:
        modes.append("fake_reclaim")
    if any(token in sid for token in ("flush", "reversion")) and net is not None and net < 0:
        modes.append("liquidation_continuation_instead_of_reversal")
    if "oi" in sid and net is not None and net < 0:
        modes.append("oi_signal_failed")
    if cost_bps is not None and cost_bps > 10 and net is not None and net < 0:
        modes.append("cost_exceeds_edge")
    if mfe is not None and mae is not None and mfe > 0 and abs(mae) > abs(mfe) * 1.5:
        modes.append("late_entry")
    btc_regime = json.dumps(row.get("btc_regime") or row.get("regime_context") or "", ensure_ascii=False).lower()
    if any(token in btc_regime for token in ("risk_off", "bear", "adverse", "down")) and net is not None and net < 0:
        modes.append("adverse_btc_regime")
    if not modes and net is not None and net < 0:
        modes.append("no_follow_through")
    if not modes:
        modes.append("not_a_loss_or_unclassified")
    return sorted(set(modes))


def top_group(rows: list[dict[str, Any]], key: str, reverse: bool) -> str:
    grouped: dict[str, float] = defaultdict(float)
    counts: Counter[str] = Counter()
    for row in rows:
        name = str(row.get(key) or "unknown")
        val = row_net_pnl(row)
        if val is None:
            continue
        grouped[name] += val
        counts[name] += 1
    if not grouped:
        return ""
    items = sorted(grouped.items(), key=lambda kv: kv[1], reverse=reverse)[:5]
    return "; ".join(f"{k}:{v:.2f}({counts[k]})" for k, v in items)


def regime_key(row: dict[str, Any]) -> str:
    for key in ("btc_regime", "eth_regime", "oi_regime", "hmm_state_if_available", "markov_state_if_available"):
        val = row.get(key)
        if val not in (None, "", {}, []):
            return f"{key}={val}"
    ctx = row.get("regime_context")
    if isinstance(ctx, dict) and ctx:
        return json.dumps(ctx, ensure_ascii=False, sort_keys=True)[:120]
    return "unknown"


def follow_through_summary(rows: list[dict[str, Any]], horizon_sec: int) -> str:
    usable = 0
    hits = 0
    for row in rows:
        t = safe_float(dynamic_value(row, "time_to_first_profit_ms"))
        if t is not None:
            usable += 1
            if t <= horizon_sec * 1000:
                hits += 1
            continue
        mfe = safe_float(row.get("mfe_pct"))
        if mfe is not None:
            # This is only a proxy because the trade schema does not record
            # horizon-specific post-entry returns.
            usable += 1
            if mfe > 0:
                hits += 1
    if usable == 0:
        return "unavailable: testnet rows do not record post-entry horizon return"
    return f"proxy_profit_touch={hits}/{usable} ({hits / usable * 100:.1f}%)"


def build_old_autopsy(testnet_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str], list[str], list[dict[str, Any]]]:
    rows_out: list[dict[str, Any]] = []
    kill: list[str] = []
    shadow_only: list[str] = []
    rewrite: list[dict[str, Any]] = []
    by_sid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in testnet_rows:
        by_sid[strategy_id(row)].append(row)

    for sid in OLD_STRATEGIES:
        rows = by_sid.get(sid, [])
        nets = [row_net_pnl(r) for r in rows]
        grosses = [row_gross_pnl(r) for r in rows]
        gross_sum = total(grosses)
        net_sum = total(nets)
        gap = gross_sum - net_sum
        pf = profit_factor(nets)
        wr = win_rate(nets)
        cost_usd = [((row_gross_pnl(r) or 0.0) - (row_net_pnl(r) or 0.0)) for r in rows]
        cost_bps = [estimated_cost_bps(r) for r in rows]
        modes = Counter()
        for r in rows:
            modes.update(classify_failure(r))
        most_common_failure = modes.most_common(1)[0][0] if modes else "no_forward_testnet_sample"
        exit_reason = Counter(str(r.get("exit_reason") or "unknown") for r in rows).most_common(1)
        exit_text = exit_reason[0][0] if exit_reason else "no_forward_testnet_sample"
        post_cost_edge = bool(len(rows) >= 30 and net_sum > 0 and (pf or 0) > 1.1)
        if not rows:
            recommendation = "shadow-only"
            reason = "没有 forward testnet 样本；旧策略冻结，只能作为对照/尸检，不可作为主开发方向。"
            shadow_only.append(sid)
        elif not post_cost_edge:
            recommendation = "kill"
            reason = "没有 post-cost edge：net PnL 为负或 profit factor 不足，不能继续调参包装成新策略。"
            kill.append(sid)
        else:
            recommendation = "keep"
            reason = "有 testnet post-cost edge；但旧策略冻结规则下仍需人工复核。"

        if rows and gross_sum > 0 and net_sum < 0:
            rewrite.append(
                {
                    "strategy_id": sid,
                    "rewrite_basis": "方向或 MFE 可能有信号，但成本/退出把 net PnL 打负；只能重写市场假设，不能旧策略调参。",
                    "gross_pnl_usdt": round(gross_sum, 6),
                    "net_pnl_usdt": round(net_sum, 6),
                }
            )
        if sid in {"flush_reversion_long", "pressure_breakdown_short"}:
            rewrite.append(
                {
                    "strategy_id": sid,
                    "rewrite_basis": "可转入 liquidation absorption / cascade 的新微结构假设研究，不保留旧参数。",
                    "gross_pnl_usdt": round(gross_sum, 6),
                    "net_pnl_usdt": round(net_sum, 6),
                }
            )
        row = {
            "strategy_id": sid,
            "trades": len(rows),
            "gross_pnl_usdt": round(gross_sum, 6),
            "net_pnl_usdt": round(net_sum, 6),
            "gross_to_net_gap_usdt": round(gap, 6),
            "win_rate_pct": round(wr, 4) if wr is not None else "",
            "profit_factor": round(pf, 4) if pf is not None else "",
            "avg_net_per_trade_usdt": round(avg(nets) or 0.0, 6) if rows else "",
            "avg_fee_spread_slippage_cost_usdt": round(avg(cost_usd) or 0.0, 6) if rows else "",
            "avg_fee_spread_slippage_cost_bps_est": round(avg(cost_bps) or 0.0, 6) if rows else "",
            "avg_favorable_excursion_pct": round(avg(safe_float(r.get("mfe_pct")) for r in rows) or 0.0, 6) if rows else "",
            "avg_adverse_excursion_pct": round(avg(safe_float(r.get("mae_pct")) for r in rows) or 0.0, 6) if rows else "",
            "follow_through_30s": follow_through_summary(rows, 30),
            "follow_through_60s": follow_through_summary(rows, 60),
            "follow_through_180s": follow_through_summary(rows, 180),
            "follow_through_300s": follow_through_summary(rows, 300),
            "most_common_exit_reason": exit_text,
            "most_common_failure_mode": most_common_failure,
            "worst_symbols": top_group(rows, "symbol", reverse=False),
            "best_symbols": top_group(rows, "symbol", reverse=True),
            "worst_liquidity_buckets": top_group(rows, "liquidity_bucket", reverse=False),
            "best_liquidity_buckets": top_group(rows, "liquidity_bucket", reverse=True),
            "regime_performance": summarize_group_net(rows, regime_key),
            "has_post_cost_edge": "yes" if post_cost_edge else "no",
            "recommendation": recommendation,
            "reason": reason,
        }
        rows_out.append(row)
    return rows_out, sorted(set(kill)), sorted(set(shadow_only)), dedupe_dicts(rewrite, "strategy_id")


def dedupe_dicts(items: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for item in items:
        marker = item.get(key)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(item)
    return out


def summarize_group_net(rows: list[dict[str, Any]], key_fn) -> str:
    grouped: dict[str, float] = defaultdict(float)
    counts: Counter[str] = Counter()
    for row in rows:
        key = key_fn(row)
        net = row_net_pnl(row)
        if net is None:
            continue
        grouped[key] += net
        counts[key] += 1
    if not grouped:
        return ""
    items = sorted(grouped.items(), key=lambda kv: abs(kv[1]), reverse=True)[:5]
    return "; ".join(f"{k}:{v:.2f}({counts[k]})" for k, v in items)


def build_failure_reports(testnet_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], Counter[str]]:
    mode_total: Counter[str] = Counter()
    aggs: dict[tuple[str, str, str], dict[str, Any]] = {}

    def add(group_type: str, group_value: str, row: dict[str, Any], modes: list[str]) -> None:
        for mode in modes:
            key = (group_type, group_value, mode)
            rec = aggs.setdefault(
                key,
                {
                    "group_type": group_type,
                    "group_value": group_value,
                    "failure_mode": mode,
                    "count": 0,
                    "net_pnl_usdt": 0.0,
                    "gross_pnl_usdt": 0.0,
                },
            )
            rec["count"] += 1
            rec["net_pnl_usdt"] += row_net_pnl(row) or 0.0
            rec["gross_pnl_usdt"] += row_gross_pnl(row) or 0.0

    for row in testnet_rows:
        modes = classify_failure(row)
        mode_total.update(modes)
        add("strategy", strategy_id(row), row, modes)
        add("symbol", str(row.get("symbol") or "unknown"), row, modes)
        add("regime", regime_key(row), row, modes)

    rows = list(aggs.values())
    for row in rows:
        row["net_pnl_usdt"] = round(row["net_pnl_usdt"], 6)
        row["gross_pnl_usdt"] = round(row["gross_pnl_usdt"], 6)
    by_strategy = sorted([r for r in rows if r["group_type"] == "strategy"], key=lambda x: (-x["count"], x["group_value"]))
    by_symbol = sorted([r for r in rows if r["group_type"] == "symbol"], key=lambda x: (-x["count"], x["group_value"]))
    by_regime = sorted([r for r in rows if r["group_type"] == "regime"], key=lambda x: (-x["count"], x["group_value"]))
    return by_strategy, by_symbol, by_regime, mode_total


def aggregate_backtest(backtest_rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    by_playbook: dict[str, dict[str, Any]] = defaultdict(lambda: fresh_agg())
    by_year: dict[tuple[str, str], dict[str, Any]] = defaultdict(lambda: fresh_agg())
    by_symbol: dict[tuple[str, str], dict[str, Any]] = defaultdict(lambda: fresh_agg())
    for row in backtest_rows:
        pb = playbook_id(row)
        ret = safe_float(row.get("after_fee_return_pct"))
        gross = safe_float(row.get("gross_return_pct"))
        pnl = safe_float(row.get("pnl_usdt"))
        update_agg(by_playbook[pb], ret, gross, pnl, row.get("symbol"))
        year = parse_year(row.get("entry_time") or row.get("signal_time"))
        update_agg(by_year[(pb, year)], ret, gross, pnl, row.get("symbol"))
        update_agg(by_symbol[(pb, str(row.get("symbol") or "unknown"))], ret, gross, pnl, row.get("symbol"))
    rows = []
    for (pb, year), agg in by_year.items():
        rows.append({"strategy_id": pb, "bucket_type": "year", "bucket": year, **final_agg(agg)})
    for (pb, sym), agg in by_symbol.items():
        rows.append({"strategy_id": pb, "bucket_type": "symbol", "bucket": sym, **final_agg(agg)})
    suspected = {}
    for pb, agg in by_playbook.items():
        symbol_pnls = [
            final_agg(v)
            for (pb2, _), v in by_symbol.items()
            if pb2 == pb and v["count"] > 0
        ]
        symbol_pnls.sort(key=lambda x: x["pnl_usdt"], reverse=True)
        top_pnl = symbol_pnls[0]["pnl_usdt"] if symbol_pnls else 0.0
        total_pnl = final_agg(agg)["pnl_usdt"]
        suspected[pb] = {
            "top_symbol_contribution_pct": round(100 * top_pnl / total_pnl, 4) if total_pnl else None,
            "top_symbols": symbol_pnls[:5],
            "notes": "If one year/symbol dominates, treat backtest as fragile until forward shadow confirms.",
        }
    return {k: final_agg(v) for k, v in by_playbook.items()}, rows, suspected


def parse_year(value: Any) -> str:
    if not value:
        return "unknown"
    text = str(value)
    if len(text) >= 4 and text[:4].isdigit():
        return text[:4]
    return "unknown"


def fresh_agg() -> dict[str, Any]:
    return {"count": 0, "returns": [], "gross_returns": [], "pnl": [], "symbols": Counter()}


def update_agg(agg: dict[str, Any], ret: float | None, gross: float | None, pnl: float | None, symbol: Any) -> None:
    agg["count"] += 1
    if ret is not None:
        agg["returns"].append(ret)
    if gross is not None:
        agg["gross_returns"].append(gross)
    if pnl is not None:
        agg["pnl"].append(pnl)
    if symbol:
        agg["symbols"][str(symbol)] += 1


def final_agg(agg: dict[str, Any]) -> dict[str, Any]:
    count = agg["count"]
    returns = agg["returns"]
    gross_returns = agg["gross_returns"]
    pnl = agg["pnl"]
    top_symbol_count = agg["symbols"].most_common(1)[0][1] if agg["symbols"] else 0
    return {
        "trades": count,
        "avg_after_cost_return_pct": round(avg(returns) or 0.0, 6) if returns else "",
        "sum_after_cost_return_pct": round(sum(returns), 6) if returns else "",
        "avg_gross_return_pct": round(avg(gross_returns) or 0.0, 6) if gross_returns else "",
        "sum_gross_return_pct": round(sum(gross_returns), 6) if gross_returns else "",
        "pnl_usdt": round(sum(pnl), 6) if pnl else "",
        "win_rate_pct": round(win_rate(returns) or 0.0, 4) if returns else "",
        "profit_factor": round(profit_factor(returns) or 0.0, 4) if returns else "",
        "top_symbol_concentration_pct": round(100 * top_symbol_count / count, 4) if count else "",
        "top_symbols": "; ".join(f"{s}:{c}" for s, c in agg["symbols"].most_common(5)),
    }


def aggregate_shadow(shadow_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    aggs: dict[str, dict[str, Any]] = defaultdict(lambda: fresh_agg())
    for row in shadow_rows:
        pb = playbook_id(row)
        ret = safe_float(row.get("after_fee_and_slippage_return_pct"))
        if ret is None:
            ret = safe_float(row.get("after_fee_return_pct"))
        gross = safe_float(row.get("close_return_pct"))
        update_agg(aggs[pb], ret, gross, None, row.get("symbol"))
    return {k: final_agg(v) for k, v in aggs.items()}


def aggregate_testnet(testnet_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    aggs: dict[str, dict[str, Any]] = defaultdict(lambda: fresh_agg())
    for row in testnet_rows:
        sid = strategy_id(row)
        net = row_net_pnl(row)
        gross = row_gross_pnl(row)
        update_agg(aggs[sid], net, gross, net, row.get("symbol"))
    out = {}
    for sid, agg in aggs.items():
        rec = final_agg(agg)
        rec["net_pnl_usdt"] = rec.pop("pnl_usdt")
        rec["avg_net_pnl_usdt"] = rec.pop("avg_after_cost_return_pct")
        rec["sum_net_pnl_usdt"] = rec.pop("sum_after_cost_return_pct")
        rec["avg_gross_pnl_usdt"] = rec.pop("avg_gross_return_pct")
        rec["sum_gross_pnl_usdt"] = rec.pop("sum_gross_return_pct")
        out[sid] = rec
    return out


def build_gap_matrix(
    backtest: dict[str, dict[str, Any]], shadow: dict[str, dict[str, Any]], testnet: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    keys = sorted(set(backtest) | set(shadow) | set(testnet) | set(OLD_STRATEGIES))
    rows = []
    for key in keys:
        bt = backtest.get(key, {})
        sh = shadow.get(key, {})
        tn = testnet.get(key, {})
        bt_ret = safe_float(bt.get("sum_after_cost_return_pct"))
        sh_ret = safe_float(sh.get("sum_after_cost_return_pct"))
        tn_net = safe_float(tn.get("sum_net_pnl_usdt"))
        gap_note = ""
        if bt_ret is not None and bt_ret > 0 and ((sh_ret is not None and sh_ret < 0) or (tn_net is not None and tn_net < 0)):
            gap_note = "backtest_positive_forward_negative"
        elif bt_ret is not None and bt_ret > 0 and sh_ret is None and tn_net is None:
            gap_note = "backtest_only_no_forward_evidence"
        elif sh_ret is not None and sh_ret > 0 and tn_net is None:
            gap_note = "shadow_positive_no_testnet_evidence"
        rows.append(
            {
                "strategy_id": key,
                "backtest_trades": bt.get("trades", 0),
                "backtest_sum_after_cost_return_pct": bt.get("sum_after_cost_return_pct", ""),
                "backtest_pnl_usdt": bt.get("pnl_usdt", ""),
                "backtest_top_symbols": bt.get("top_symbols", ""),
                "shadow_outcomes": sh.get("trades", 0),
                "shadow_sum_after_cost_return_pct": sh.get("sum_after_cost_return_pct", ""),
                "shadow_avg_after_cost_return_pct": sh.get("avg_after_cost_return_pct", ""),
                "testnet_trades": tn.get("trades", 0),
                "testnet_net_pnl_usdt": tn.get("sum_net_pnl_usdt", ""),
                "testnet_win_rate_pct": tn.get("win_rate_pct", ""),
                "gap_classification": gap_note,
            }
        )
    return rows


def make_new_strategy_templates(cost_baseline_bps: float) -> list[dict[str, Any]]:
    base_cost = round(cost_baseline_bps, 4)
    items = [
        (
            "oi_build_breakout_continuation_v1",
            "OI_BUILD_BREAKOUT",
            "价格突破同时 OI 增加，说明新仓进入；若 volume burst 和 orderbook support 同时存在，突破可能延续。",
            42,
            "追突破太晚的反向方、被迫止损的空头/多头，以及错误 fade 突破的人。",
        ),
        (
            "bidwall_oi_build_continuation_v1",
            "ORDERBOOK_OI_CONTINUATION",
            "OI build + breakout 后 bidwall 未撤，说明有主动支撑或被动承接，延续概率可能提高。",
            38,
            "在支撑未撤时过早做反转的人，以及低估被动承接的人。",
        ),
        (
            "oi_build_fakeout_reversal_v1",
            "OI_BUILD_FAKEOUT",
            "价格突破 + OI build 但 30/60/180 秒无 follow-through，新增仓位可能被困，随后反向止损。",
            32,
            "追假突破的新仓，以及突破失败后被动止损的人。",
        ),
        (
            "liquidation_flush_absorption_v1",
            "LIQUIDATION_ABSORPTION",
            "爆仓长针后若成交量释放但价格不再下破，说明被动承接吸收，短线可能均值回归。",
            34,
            "爆仓后继续追杀但被吸收的人。",
        ),
        (
            "liquidation_cascade_continuation_v1",
            "LIQUIDATION_CASCADE",
            "爆仓不是结束而是级联开始；若 OI unwind、价差扩大、深度撤退同向出现，趋势可能延续。",
            45,
            "过早抄底/摸顶的人，以及被动强平链条里的杠杆仓位。",
        ),
        (
            "oi_unwind_reversal_v1",
            "OI_UNWIND_REVERSAL",
            "价格延续后 OI 快速下降，说明拥挤仓位退出；若价格不再创新高/低，可能进入反转。",
            30,
            "最后一批追趋势但持仓已经开始撤退的人。",
        ),
        (
            "funding_squeeze_continuation_v1",
            "FUNDING_SQUEEZE",
            "资金费率极端且价格继续推进，拥挤方向可能被 squeeze，短线延续来自被迫平仓。",
            36,
            "为了 funding 做反向但承受价格 squeeze 的仓位。",
        ),
        (
            "vol_compression_expansion_v1",
            "VOL_COMPRESSION_EXPANSION",
            "低波动压缩后，盘口和成交量同时扩张，说明被压住的风险释放，可能产生可交易的扩张段。",
            40,
            "压缩末端仍按窄波动做均值回归的人。",
        ),
        (
            "btc_eth_regime_lead_lag_filter_v1",
            "BTC_ETH_LEAD_LAG",
            "BTC/ETH 先行进入 risk-on/risk-off，山寨币滞后跟随；策略只做 regime 顺风方向。",
            28,
            "忽略主导资产 regime 的局部追反向者。",
        ),
        (
            "major_symbol_lead_lag_rotation_v1",
            "MAJOR_ROTATION",
            "BTC/ETH/SOL/BNB 等 major 先动后，相关板块 top liquidity alt 延迟跟随。",
            35,
            "在轮动初期低估相关资产扩散的人。",
        ),
    ]
    out = []
    for sid, family, hypothesis, gross_bps, loser in items:
        net_bps = round(gross_bps - base_cost, 4)
        out.append(
            {
                "strategy_id": sid,
                "strategy_family": family,
                "market_hypothesis": hypothesis,
                "edge_source": "只在微结构事件、流动性足够、成本显著低于预期波动时捕捉短线非均衡。",
                "who_loses_money": loser,
                "trigger_event": "实时 futures event：price/OI/volume/orderbook/regime 同时满足。",
                "entry_conditions": [
                    "expected_net_edge_bps > 0",
                    "expected_gross_move_bps >= 3 * expected_total_cost_bps",
                    "major/mega liquidity bucket only before validation",
                    "spread/slippage below block threshold",
                ],
                "invalidation_conditions": [
                    "30s/60s 无 follow-through",
                    "orderbook support disappears",
                    "BTC/ETH regime turns adverse",
                    "expected_net_edge_bps <= 0",
                ],
                "exit_logic": "time-boxed exit by horizon plus invalidation exit; no slow rescue averaging.",
                "allowed_regimes": ["trend_expansion", "oi_build_trend", "btc_led_risk_on"],
                "blocked_regimes": ["high_vol_chop", "low_liquidity_chop", "adverse_btc_regime"],
                "allowed_liquidity_buckets": ["mega", "major"],
                "expected_holding_horizon": "60s/180s/300s/900s depending on branch",
                "expected_gross_move_bps": gross_bps,
                "expected_total_cost_bps": base_cost,
                "expected_net_edge_bps": net_bps,
                "why_this_is_not_an_old_strategy": "从 OI/orderbook/liquidation/regime 的可解释市场行为出发，不继承旧策略 score/阈值/名称。",
                "required_data_fields": [
                    "strategy_manifest_id",
                    "symbol",
                    "timestamp",
                    "side",
                    "oi_change_5m_pct",
                    "oi_change_15m_pct",
                    "volume_ratio",
                    "spread_bps_at_entry",
                    "estimated_slippage_bps",
                    "bidwall_strength",
                    "bidwall_persistence_sec",
                    "btc_regime",
                    "eth_regime",
                    "hmm_state",
                    "markov_state",
                    "return_30s_bps",
                    "return_60s_bps",
                    "return_180s_bps",
                    "return_300s_bps",
                    "gross_pnl",
                    "net_pnl",
                    "failure_mode",
                ],
                "required_shadow_evidence": {
                    "min_unique_signals": 100,
                    "min_days": 7,
                    "require_cost_adjusted_positive": True,
                    "max_top_symbol_concentration_pct": 35,
                },
                "required_testnet_evidence": {
                    "min_trades_before_promotion": 100,
                    "require_net_pnl_positive": True,
                    "require_net_profit_factor_gt": 1.15,
                    "daily_trade_cap": "5-10 before validation",
                },
                "kill_conditions": [
                    "expected_net_edge_bps <= 0",
                    "gross move < 3x total cost",
                    "shadow cost-adjusted return negative after 100 unique signals",
                    "testnet net PnL negative after 100 trades",
                ],
                "evidence_status": "hypothesis_only_or_shadow_observing",
                "mainnet_live_allowed": False,
            }
        )
    return out


def build_oi_feature_table(shadow_rows: list[dict[str, Any]], max_rows: int = 20000) -> list[dict[str, Any]]:
    rows = []
    for row in shadow_rows:
        pb = playbook_id(row)
        if "oi_build" not in pb and "bidwall_oi" not in pb:
            continue
        factors = row.get("factors") if isinstance(row.get("factors"), dict) else {}
        gross = safe_float(row.get("close_return_pct"))
        net = safe_float(row.get("after_fee_and_slippage_return_pct"))
        cost = None
        if gross is not None and net is not None:
            cost = (gross - net) * 100
        rows.append(
            {
                "source_file": row.get("_source_file"),
                "symbol": row.get("symbol"),
                "playbook": pb,
                "side": row.get("side"),
                "horizon_sec": row.get("horizon_sec"),
                "liquidity_bucket": row.get("liquidity_bucket"),
                "oi_change_5m_pct": factors.get("oi_change_5m_pct") or row.get("oi_change_5m_pct"),
                "oi_change_15m_pct": factors.get("oi_change_15m_pct") or row.get("oi_change_15m_pct"),
                "volume_ratio": factors.get("volume_ratio") or row.get("volume_ratio"),
                "spread_bps_at_entry": row.get("spread_bps_at_entry"),
                "estimated_slippage_bps": row.get("estimated_slippage_bps"),
                "round_trip_fee_bps": row.get("round_trip_fee_bps"),
                "gross_return_bps": round(gross * 100, 6) if gross is not None else "",
                "net_return_bps": round(net * 100, 6) if net is not None else "",
                "estimated_total_cost_bps": round(cost, 6) if cost is not None else "",
                "mfe_pct": row.get("max_favorable_return_pct") or row.get("max_runup_pct"),
                "mae_pct": row.get("max_adverse_return_pct") or row.get("max_drawdown_pct"),
                "exit_reason": row.get("exit_reason"),
                "research_only": row.get("research_only"),
                "live_unlock_eligible": row.get("live_unlock_eligible"),
            }
        )
        if len(rows) >= max_rows:
            break
    return rows


def build_cost_ranking(
    testnet_rows: list[dict[str, Any]], shadow_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rank_rows: list[dict[str, Any]] = []
    by_sid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in testnet_rows:
        by_sid[strategy_id(row)].append(row)
    for sid, rows in by_sid.items():
        returns_bps = []
        gross_bps = []
        costs = []
        for r in rows:
            ret = safe_float(r.get("return_pct"))
            if ret is not None:
                returns_bps.append(ret * 100)
            net = row_net_pnl(r)
            gross = row_gross_pnl(r)
            if gross is not None and net is not None:
                costs.append(gross - net)
            cost_b = estimated_cost_bps(r)
            if cost_b is not None:
                gross_bps.append((ret or 0.0) * 100 + cost_b)
        add_rank_row(rank_rows, sid, "testnet", rows, gross_bps, returns_bps, costs)

    by_pb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in shadow_rows:
        pb = playbook_id(row)
        if pb == "unknown":
            continue
        by_pb[pb].append(row)
    for pb, rows in by_pb.items():
        if not (
            "oi" in pb
            or "liquidation" in pb
            or "bidwall" in pb
            or "shadow_oi_build" in pb
            or "discovery" in pb
        ):
            continue
        gross_bps = []
        net_bps = []
        costs = []
        for r in rows:
            gross = safe_float(r.get("close_return_pct"))
            net = safe_float(r.get("after_fee_and_slippage_return_pct"))
            if gross is not None:
                gross_bps.append(gross * 100)
            if net is not None:
                net_bps.append(net * 100)
            if gross is not None and net is not None:
                costs.append((gross - net) * 100)
        add_rank_row(rank_rows, pb, "shadow", rows, gross_bps, net_bps, costs)
    rank_rows.sort(key=lambda x: safe_float(x.get("expected_net_edge_bps")) or -999999, reverse=True)
    return rank_rows


def add_rank_row(
    out: list[dict[str, Any]],
    sid: str,
    source: str,
    rows: list[dict[str, Any]],
    gross_bps: list[float],
    net_bps: list[float],
    cost_values: list[float],
) -> None:
    sample = len(rows)
    gross = avg(gross_bps)
    net = avg(net_bps)
    cost = avg(cost_values)
    if cost is None and gross is not None and net is not None:
        cost = gross - net
    symbols = Counter(str(r.get("symbol") or "unknown") for r in rows)
    top_conc = 100.0 * symbols.most_common(1)[0][1] / sample if sample and symbols else 0.0
    values = net_bps
    pf = profit_factor(values)
    wr = win_rate(values)
    sufficient = sample >= 100
    allowed = (
        net is not None
        and gross is not None
        and cost is not None
        and net > 0
        and gross >= 3 * max(cost, 0)
        and sufficient
        and top_conc <= 35
        and source != "backtest"
    )
    if allowed and source == "testnet":
        rec = "eligible_for_promoted_testnet_review_not_mainnet"
    elif allowed and source == "shadow":
        rec = "shadow_positive_needs_dedup_and_limited_testnet_gate"
    else:
        rec = "not_recommended"
    out.append(
        {
            "strategy_id": sid,
            "source": source,
            "expected_gross_move_bps": round(gross, 6) if gross is not None else "",
            "expected_fee_bps": "",
            "expected_spread_bps": "",
            "expected_slippage_bps": "",
            "expected_funding_bps": "",
            "expected_total_cost_bps": round(cost, 6) if cost is not None else "",
            "expected_net_edge_bps": round(net, 6) if net is not None else "",
            "cost_to_edge_ratio": round(cost / net, 6) if cost is not None and net and net > 0 else "",
            "net_profit_factor": round(pf, 6) if pf is not None else "",
            "net_win_rate": round(wr, 4) if wr is not None else "",
            "sample_size": sample,
            "confidence_score": confidence_score(sample, net, top_conc, source),
            "symbol_concentration_risk": round(top_conc, 4),
            "regime_concentration_risk": "unavailable_without_stable_regime_join",
            "recommendation": rec,
        }
    )


def confidence_score(sample: int, net: float | None, top_conc: float, source: str) -> float:
    base = min(sample / 200, 1.0) * 40
    edge = 30 if net is not None and net > 0 else 0
    conc = 20 if top_conc <= 35 else 5
    src = 10 if source == "testnet" else 6 if source == "shadow" else 2
    return round(base + edge + conc + src, 2)


def data_value_rows(status: dict[str, Any]) -> list[dict[str, Any]]:
    cats = status["category_sizes"]
    return [
        data_value("collector 原始样本", cats.get("collector_event_collect_v6", {}), "高", "中", "高", "否", "部分旧样本会变旧", "是", "否", "是", "否", "30 日热数据；30 日以上先 summary 再 archive"),
        data_value("shadow outcomes", cats.get("shadow_all_mainnet_shadow_dirs", {}), "高", "高", "高", "否", "部分旧", "是", "否", "是", "否", "关键 outcome 至少 90 日；完整明细可压缩归档"),
        data_value("testnet trades", cats.get("testnet_round_runner_reports_all", {}), "高", "高", "高", "否", "否", "可压缩旧 jsonl", "否", "是", "禁止删除未备份原始 trade", "永久保留关键 trade records；旧明细可压缩"),
        data_value("backtest results", cats.get("replay_backtest_signal_lab_replay", {}), "中", "中", "中", "可能", "可能", "是", "可只保 summary", "是", "否", "保留能复现实验的最小集合"),
        data_value("replay results", cats.get("replay_backtest_signal_lab_replay", {}), "高", "高", "高", "否", "可能", "是", "否", "是", "否", "用于 backtest 到 shadow 的桥接证据"),
        data_value("strategy reports", cats.get("reports_named_dirs", {}), "中", "高", "中", "常见", "可能", "是", "可只保 summary", "是", "否", "重复日报只留 summary 和关键版本"),
        data_value("factor factory reports", {}, "中", "中", "中", "可能", "可能", "是", "可只保 summary", "是", "否", "保留字段定义和获胜/失败样本"),
        data_value("HMM / Markov / Monte Carlo outputs", {}, "中", "中", "中", "否", "可能", "是", "可只保 summary", "是", "否", "只作 regime 解释，不直接当交易信号"),
        data_value("gate reports", {}, "高", "高", "高", "否", "否", "是", "否", "是", "禁止删除当前 gate evidence", "至少保留所有 promotion/evidence gate 结论"),
        data_value("cost reports", {}, "高", "高", "高", "否", "否", "是", "否", "是", "禁止删除当前 cost evidence", "至少保留所有 cost gate 结论"),
        data_value("dashboard snapshots", {}, "中", "中", "低", "可能", "可能", "是", "可只保 latest+daily", "是", "否", "保留最近和关键异常点"),
        data_value("logs", cats.get("logs", {}), "低到中", "中", "低", "常见", "会", "是", "可只保异常摘要", "是", "否", "旧日志先压缩再按批准清理"),
        data_value("temporary / cache files", cats.get("cache_tmp", {}), "低", "低", "低", "是", "是", "否", "否", "是", "必须 dry-run 后才可删除", "短期，按 dry-run 批准"),
    ]


def data_value(
    name: str,
    size_obj: dict[str, Any],
    discovery: str,
    audit: str,
    reproduce: str,
    duplicate: str,
    old: str,
    compress: str,
    summary_only: str,
    archive: str,
    delete: str,
    retention: str,
) -> dict[str, Any]:
    return {
        "data_type": name,
        "current_size": size_obj.get("size", "unknown"),
        "useful_for_strategy_discovery": discovery,
        "useful_for_audit": audit,
        "useful_for_reproduce": reproduce,
        "is_duplicate_report": duplicate,
        "sample_too_old": old,
        "can_compress": compress,
        "can_keep_summary_only": summary_only,
        "can_archive": archive,
        "can_delete_dry_run_only": delete,
        "reasonable_retention": retention,
    }


def build_shadow_candidates(shadow_agg: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for sid, rec in shadow_agg.items():
        if "oi_build" not in sid and "bidwall_oi" not in sid:
            continue
        avg_ret = safe_float(rec.get("avg_after_cost_return_pct"))
        trades = int(rec.get("trades") or 0)
        if avg_ret is None:
            continue
        out.append(
            {
                "strategy_id": sid,
                "shadow_outcomes": trades,
                "avg_after_cost_return_pct": avg_ret,
                "sum_after_cost_return_pct": rec.get("sum_after_cost_return_pct"),
                "top_symbols": rec.get("top_symbols"),
                "candidate_status": "shadow_positive_observing" if avg_ret > 0 and trades >= 30 else "not_ready",
                "limited_testnet_allowed_now": False,
                "reason": "需要 unique signal 去重、成本字段完整、100+ 独立信号后才可 limited testnet。",
            }
        )
    out.sort(key=lambda x: (x["avg_after_cost_return_pct"], x["shadow_outcomes"]), reverse=True)
    return out


def md_table(rows: list[dict[str, Any]], columns: list[str], limit: int | None = None) -> str:
    use_rows = rows[:limit] if limit else rows
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, sep]
    for row in use_rows:
        vals = [str(scalar_csv(row.get(col, ""))).replace("\n", " ") for col in columns]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def load_data(root: Path) -> dict[str, list[dict[str, Any]]]:
    testnet_files = sorted(root.glob("round_runner_reports*/round_*_trades.jsonl"))
    learning_files = sorted(root.glob("round_runner_reports*/learning_store.jsonl"))
    shadow_files = sorted(root.glob("signal_lab_runs/**/signal_bridge_shadow_outcomes.jsonl"))
    signal_files = sorted(root.glob("signal_lab_runs/**/signal_bridge_shadow_signals.jsonl"))
    backtest_files = sorted(root.glob("signal_lab_replay/**/playbook_backtest_trades.jsonl"))
    data = {
        "testnet": [],
        "learning": [],
        "shadow": [],
        "signals": [],
        "backtest": [],
    }
    for path in testnet_files:
        data["testnet"].extend(read_jsonl(path))
    for path in learning_files:
        data["learning"].extend(read_jsonl(path))
    for path in shadow_files:
        data["shadow"].extend(read_jsonl(path))
    for path in signal_files:
        data["signals"].extend(read_jsonl(path))
    for path in backtest_files:
        data["backtest"].extend(read_jsonl(path))
    data["_files"] = [  # type: ignore[assignment]
        {
            "testnet_files": [str(p) for p in testnet_files],
            "learning_files": [str(p) for p in learning_files],
            "shadow_files": [str(p) for p in shadow_files],
            "signal_files": [str(p) for p in signal_files],
            "backtest_files": [str(p) for p in backtest_files],
        }
    ]
    return data


def build_reports(root: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[phoenix-analyst] collecting VPS status for {root}", flush=True)
    status = collect_vps_status(root)
    write_json(out_dir / "vps_status.json", status)
    print("[phoenix-analyst] loading json/jsonl data", flush=True)
    data = load_data(root)
    testnet_rows = data["testnet"]
    shadow_rows = data["shadow"]
    backtest_rows = data["backtest"]
    write_json(
        out_dir / "source_inventory.json",
        {
            "row_counts": {k: len(v) for k, v in data.items() if not k.startswith("_")},
            "files": data["_files"][0],
        },
    )

    print(
        "[phoenix-analyst] rows "
        + json.dumps({k: len(v) for k, v in data.items() if not k.startswith("_")}, sort_keys=True),
        flush=True,
    )
    print("[phoenix-analyst] building old strategy autopsy", flush=True)
    old_rows, kill, shadow_only, rewrite = build_old_autopsy(testnet_rows)
    write_csv(out_dir / "old_strategy_autopsy.csv", old_rows)
    write_json(out_dir / "old_strategy_kill_list.json", {"strategies": kill, "count": len(kill)})
    write_json(out_dir / "old_strategy_shadow_only_list.json", {"strategies": shadow_only, "count": len(shadow_only)})
    write_json(out_dir / "old_strategy_rewrite_candidates.json", {"candidates": rewrite, "count": len(rewrite)})
    write_text(out_dir / "old_strategy_autopsy.md", old_strategy_md(old_rows, kill, shadow_only, rewrite))

    print("[phoenix-analyst] building failure taxonomy", flush=True)
    by_strategy, by_symbol, by_regime, mode_total = build_failure_reports(testnet_rows)
    write_json(out_dir / "failure_mode_taxonomy.json", FAILURE_MODE_DEFS)
    write_csv(out_dir / "failure_mode_by_strategy.csv", by_strategy)
    write_csv(out_dir / "failure_mode_by_symbol.csv", by_symbol)
    write_csv(out_dir / "failure_mode_by_regime.csv", by_regime)
    write_text(out_dir / "failure_mode_report.md", failure_mode_md(mode_total, by_strategy, by_symbol))

    print("[phoenix-analyst] building backtest/shadow/testnet gap", flush=True)
    backtest_agg, backtest_detail_rows, suspected = aggregate_backtest(backtest_rows)
    shadow_agg = aggregate_shadow(shadow_rows)
    testnet_agg = aggregate_testnet(testnet_rows)
    gap_matrix = build_gap_matrix(backtest_agg, shadow_agg, testnet_agg)
    write_csv(out_dir / "backtest_forward_gap_matrix.csv", gap_matrix)
    write_csv(out_dir / "backtest_breakdown_by_year_symbol.csv", backtest_detail_rows)
    write_json(out_dir / "suspected_overfit_patterns.json", suspected)
    write_text(out_dir / "backtest_forward_gap_analysis.md", gap_md(gap_matrix, suspected))

    print("[phoenix-analyst] building data value report", flush=True)
    dv_rows = data_value_rows(status)
    write_csv(out_dir / "data_value_matrix.csv", dv_rows)
    write_json(out_dir / "keep_compress_archive_delete_recommendation.json", data_value_reco(dv_rows, status))
    write_text(out_dir / "data_value_report.md", data_value_md(dv_rows, status))
    write_text(out_dir / "minimal_research_dataset_plan.md", minimal_dataset_md())

    print("[phoenix-analyst] building strategy hypotheses", flush=True)
    cost_baseline = med(estimated_cost_bps(r) for r in testnet_rows) or 12.0
    strategies = make_new_strategy_templates(cost_baseline)
    write_text(out_dir / "new_strategy_hypotheses.md", new_strategy_md(strategies))
    drafts_dir = out_dir / "strategy_manifest_drafts"
    for item in strategies:
        write_json(drafts_dir / f"{item['strategy_id']}.json", item)
    priority = [
        {
            "rank": i + 1,
            "strategy_id": item["strategy_id"],
            "strategy_family": item["strategy_family"],
            "priority_reason": priority_reason(item["strategy_id"]),
            "current_allowed_stage": "shadow research only",
        }
        for i, item in enumerate(strategies)
    ]
    write_json(out_dir / "new_strategy_priority_rank.json", priority)

    print("[phoenix-analyst] building OI research", flush=True)
    oi_feature_rows = build_oi_feature_table(shadow_rows)
    write_csv(out_dir / "oi_build_breakout_feature_table.csv", oi_feature_rows, OI_FEATURE_COLUMNS)
    shadow_candidates = build_shadow_candidates(shadow_agg)
    write_json(out_dir / "oi_build_breakout_shadow_candidates.json", shadow_candidates)
    oi_dir = out_dir / "oi_build_strategy_manifest_drafts"
    for item in strategies[:3]:
        write_json(oi_dir / f"{item['strategy_id']}.json", item)
    write_text(out_dir / "oi_build_breakout_research.md", oi_research_md(shadow_candidates, oi_feature_rows, shadow_agg))

    print("[phoenix-analyst] building regime and ranking reports", flush=True)
    regime_rows = build_regime_rows(testnet_rows, shadow_rows)
    write_csv(out_dir / "strategy_by_regime_matrix.csv", regime_rows)
    write_text(out_dir / "regime_research.md", regime_md(regime_rows))
    write_text(out_dir / "hmm_state_interpretation.md", hmm_md(root))
    write_csv(out_dir / "markov_transition_edge_report.csv", markov_rows(root))

    ranking = build_cost_ranking(testnet_rows, shadow_rows)
    write_csv(out_dir / "cost_aware_strategy_ranking.csv", ranking)
    write_text(out_dir / "cost_aware_strategy_ranking.md", ranking_md(ranking))

    print("[phoenix-analyst] building promotion and experiment reports", flush=True)
    evidence = build_evidence_recommendations(old_rows, shadow_candidates, ranking)
    write_json(out_dir / "strategy_evidence_recommendations.json", evidence)
    write_text(out_dir / "promotion_recommendations.md", promotion_md(evidence))

    exp_dir = out_dir / "experiment_001_strategy_manifests"
    for item in strategies[:3]:
        exp_item = dict(item)
        exp_item["experiment_id"] = "experiment_001_oi_build_breakout_family_cost_aware_forward_validation"
        exp_item["old_strategy_allowed"] = False
        exp_item["limited_testnet_daily_cap"] = 10
        write_json(exp_dir / f"{item['strategy_id']}.json", exp_item)
    write_text(out_dir / "experiment_001_design.md", experiment_design_md())
    write_json(out_dir / "experiment_001_success_criteria.json", experiment_success())
    write_json(out_dir / "experiment_001_failure_criteria.json", experiment_failure())
    write_text(out_dir / "experiment_001_required_builder_tasks.md", builder_tasks_md())
    write_text(out_dir / "experiment_001_required_auditor_checks.md", auditor_checks_md())

    write_text(
        out_dir / "phoenix_analyst_final_report.md",
        final_report_md(
            status,
            old_rows,
            kill,
            shadow_only,
            rewrite,
            mode_total,
            gap_matrix,
            dv_rows,
            shadow_candidates,
            ranking,
            evidence,
            out_dir,
        ),
    )
    write_json(
        out_dir / "artifact_index.json",
        {
            "output_dir": str(out_dir),
            "generated_at": dt.datetime.now(dt.timezone.utc).astimezone().isoformat(),
            "files": sorted(str(p.relative_to(out_dir)) for p in out_dir.rglob("*") if p.is_file()),
            "safety": {
                "deleted_files": False,
                "modified_runtime_config": False,
                "stopped_services": False,
                "mainnet_live_enabled": False,
            },
        },
    )


def old_strategy_md(rows: list[dict[str, Any]], kill: list[str], shadow_only: list[str], rewrite: list[dict[str, Any]]) -> str:
    return f"""# Old Strategy Autopsy

## Shawn 可读结论

旧 testnet 策略没有证明 post-cost edge（扣除成本后仍然有优势）。多数策略不是方向很强但成本吃掉，而是 gross PnL（未完整扣成本前盈亏）本身就弱或接近 0；扣掉 fee、spread、slippage 后，net PnL（净盈亏）全部变成负数。旧策略不能继续调参包装成新策略。

## 死亡名单

{", ".join(kill) if kill else "无"}

## 只能 shadow-only / autopsy 的旧策略

{", ".join(shadow_only) if shadow_only else "无"}

## 可重写方向

这些不是“保留旧策略继续调参”，而是把失败现象拆出来，重新写市场假设：

{md_table(rewrite, ["strategy_id", "rewrite_basis", "gross_pnl_usdt", "net_pnl_usdt"]) if rewrite else "无"}

## 关键尸检表

{md_table(rows, ["strategy_id", "trades", "gross_pnl_usdt", "net_pnl_usdt", "gross_to_net_gap_usdt", "win_rate_pct", "profit_factor", "most_common_failure_mode", "recommendation"], limit=30)}

## 字段说明

- gross PnL = 未完整扣成本前的盈亏。
- net PnL = 扣除手续费、滑点、价差等成本后的净盈亏。
- gross-to-net gap = 成本和执行损耗造成的差距。
- follow-through = 入场后是否继续朝有利方向走；当前 testnet trade schema 没有完整 30s/60s/180s/300s 后收益字段，只能用 dynamic_exit 的 time_to_first_profit 或 MFE 做弱 proxy。
"""


def failure_mode_md(mode_total: Counter[str], by_strategy: list[dict[str, Any]], by_symbol: list[dict[str, Any]]) -> str:
    mode_rows = [
        {"failure_mode": k, "count": v, "zh": FAILURE_MODE_DEFS.get(k, {}).get("zh", "")}
        for k, v in mode_total.most_common()
    ]
    return f"""# Failure Mode Report

## Shawn 可读结论

Phoenix 不能只说“亏了”。本轮把 testnet trade 尽量归因到 failure modes（亏损原因分类）。最常见的问题集中在 time stop 后没有 follow-through、成本超过微弱 gross edge、以及 stale signal / slippage 这类执行问题。

## Failure mode 总览

{md_table(mode_rows, ["failure_mode", "zh", "count"])}

## 按策略 Top

{md_table(by_strategy, ["group_value", "failure_mode", "count", "net_pnl_usdt"], limit=40)}

## 按 symbol Top

{md_table(by_symbol, ["group_value", "failure_mode", "count", "net_pnl_usdt"], limit=40)}

## 审计限制

- bidwall_removed / askwall_absorption 需要 orderbook persistence 字段，目前旧 testnet trade 不足。
- 30s/60s/180s/300s follow-through 需要明确 post-entry horizon return 字段，当前只能弱 proxy。
"""


def gap_md(gap_rows: list[dict[str, Any]], suspected: dict[str, Any]) -> str:
    flagged = [r for r in gap_rows if r.get("gap_classification")]
    return f"""# Backtest vs Shadow/Testnet Gap Analysis

## Shawn 可读结论

2024-2026 top200 backtest 好看不能作为上线理由。现有文件显示，`oi_build_breakout` 在 backtest 里 after-fee return 很强，但 forward shadow（主网行情影子模拟，不下真钱单）里同名 playbook 变成负；同时旧 testnet 策略没有 net positive。这说明 Phoenix 的问题不是“继续调阈值”，而是历史模拟 edge 到 forward realized edge 之间断链。

## Gap 样本

{md_table(flagged, ["strategy_id", "backtest_trades", "backtest_sum_after_cost_return_pct", "shadow_outcomes", "shadow_sum_after_cost_return_pct", "testnet_trades", "testnet_net_pnl_usdt", "gap_classification"], limit=40)}

## 主要怀疑模式

- backtest 可能只捕捉到某段行情或少数 symbol 的结构，forward 后结构消失。
- fee/slippage/spread 一加，gross edge 太薄的高换手策略会被成本杀死。
- entry delay 和 signal staleness 在 backtest 中不充分，testnet 中直接体现为 net PnL 下降。
- shadow outcome 存在多 horizon / multi branch fan-out，不能把 outcome 数直接当独立 trades。

## 过拟合检查

完整 JSON 见 `suspected_overfit_patterns.json`。
"""


def data_value_reco(rows: list[dict[str, Any]], status: dict[str, Any]) -> dict[str, Any]:
    return {
        "delete_now": [],
        "delete_recommendation": "不执行真实删除；如需清理，必须先生成 storage_cleanup_plan.md/json 并 dry-run。",
        "keep": [
            "testnet trades",
            "shadow outcomes",
            "strategy/experiment manifest",
            "gate reports",
            "cost reports",
            "最近 30 日 collector 样本",
        ],
        "compress": [
            "旧 jsonl log",
            "旧 shadow outcome 明细",
            "旧 collector 样本",
            "旧 replay/backtest 明细",
        ],
        "archive": [
            "30 日以上但仍有研究价值的数据",
            "完成实验完整结果",
            "旧 strategy autopsy 数据",
        ],
        "dry_run_delete_only": [
            "tmp/cache",
            "重复 report",
            "空文件",
            "失败任务残留",
            "已压缩且校验成功的重复原始副本",
        ],
        "largest_space_pressure": status.get("top20_files", [])[:5],
        "matrix": rows,
    }


def data_value_md(rows: list[dict[str, Any]], status: dict[str, Any]) -> str:
    biggest = status.get("top20_files", [])[:5]
    return f"""# Data Value Report

## Shawn 可读结论

Phoenix 当前最占空间的是 collector 原始样本，不是 testnet report。最有研究价值的数据是 testnet trades、shadow outcomes、cost/gate evidence、能复现结论的最小 feature table。最容易变成垃圾的是重复日报、旧 log、没有 summary 的大型中间文件。

## 最大文件

{md_table(biggest, ["size", "path", "bytes"])}

## 数据价值矩阵

{md_table(rows, ["data_type", "current_size", "useful_for_strategy_discovery", "useful_for_audit", "useful_for_reproduce", "can_compress", "can_archive", "can_delete_dry_run_only", "reasonable_retention"])}

## 建议

- 不建议直接删除任何 Phoenix 数据。
- 大型 collector 文件先生成 summary / feature table，再压缩或归档。
- 重复 report 可以进入 dry-run 清理候选，但必须先列出 path、大小、风险和回滚方案。
"""


def minimal_dataset_md() -> str:
    return """# Minimal Research Dataset Plan

如果未来只保留最小但足够研究的数据，每一条 strategy event / shadow outcome / testnet trade 至少保留以下字段：

- strategy_id
- strategy_manifest_id
- experiment_id
- symbol
- timestamp
- side
- regime
- btc_regime
- eth_regime
- hmm_state
- markov_state
- liquidity_bucket
- features
- entry
- exit
- gross PnL
- net PnL
- fee
- spread
- slippage
- funding
- expected_gross_move_bps
- expected_total_cost_bps
- expected_net_edge_bps
- cost_to_edge_ratio
- exit reason
- failure mode
- shadow/testnet source
- horizon returns: 30s / 60s / 180s / 300s / 900s
- MFE / MAE

目标：把大 jsonl 明细沉淀成可审计的小型 feature table，同时保留能复现结论的原始文件引用和 hash。
"""


def new_strategy_md(strategies: list[dict[str, Any]]) -> str:
    lines = ["# New Strategy Hypotheses", "", "## Shawn 可读结论", "", "这些是假设，不是已验证策略。真正的新策略必须从 Binance futures market behavior / microstructure / position behavior 出发，并且先 shadow 验证 post-cost edge。", ""]
    for s in strategies:
        lines += [
            f"## {s['strategy_id']}",
            "",
            f"- strategy_family: {s['strategy_family']}",
            f"- market_hypothesis: {s['market_hypothesis']}",
            f"- edge_source: {s['edge_source']}",
            f"- who_loses_money: {s['who_loses_money']}",
            f"- expected_gross_move_bps: {s['expected_gross_move_bps']}",
            f"- expected_total_cost_bps: {s['expected_total_cost_bps']}",
            f"- expected_net_edge_bps: {s['expected_net_edge_bps']}",
            f"- why this is not old strategy: {s['why_this_is_not_an_old_strategy']}",
            "- mainnet live: 不允许",
            "",
        ]
    return "\n".join(lines)


def priority_reason(strategy_id: str) -> str:
    if "oi_build_breakout" in strategy_id:
        return "当前 Phoenix 已有 backtest/shadow 断链证据，最适合先做 cost-aware forward validation。"
    if "bidwall" in strategy_id:
        return "能直接解释 fake breakout 与 continuation 的分叉，需要新增 orderbook persistence 字段。"
    if "fakeout" in strategy_id:
        return "从旧趋势/突破失败中提取反向结构，但必须用 30/60/180s follow-through 证明。"
    return "作为后续研究方向，先保持 shadow-only。"


def oi_research_md(candidates: list[dict[str, Any]], features: list[dict[str, Any]], shadow_agg: dict[str, dict[str, Any]]) -> str:
    agg_rows = [
        {"strategy_id": k, **v}
        for k, v in shadow_agg.items()
        if "oi_build" in k or "bidwall_oi" in k
    ]
    agg_rows.sort(key=lambda x: safe_float(x.get("sum_after_cost_return_pct")) or -999, reverse=True)
    return f"""# OI Build Breakout Research

## Shawn 可读结论

`oi_build_breakout` 这个大方向值得研究，但不能直接 testnet。原因是：泛化的 `oi_build_breakout` 在 forward shadow 里并不稳；只有更窄的 `shadow_oi_build_breakout_balanced`、`shadow_oi_build_breakout_quality`、`discovery_bidwall_oi_build_continuation` 等分支显示正向迹象。这些仍然是 shadow 证据，而且 outcome 可能包含多 horizon / 多 branch，不等于独立交易。

## Shadow 候选

{md_table(candidates, ["strategy_id", "shadow_outcomes", "avg_after_cost_return_pct", "sum_after_cost_return_pct", "candidate_status", "limited_testnet_allowed_now"], limit=20)}

## OI 相关聚合

{md_table(agg_rows, ["strategy_id", "trades", "avg_after_cost_return_pct", "sum_after_cost_return_pct", "win_rate_pct", "top_symbol_concentration_pct", "top_symbols"], limit=30)}

## 当前缺口

- 需要 bidwall_strength、bidwall_persistence、bid_depth_change_after_breakout。
- 需要 post-entry 30s/60s/180s/300s returns，而不是只看最终 PnL。
- 需要 unique signal 去重，避免多 horizon outcome 放大样本数。
- 需要 testnet execution parity：entry delay、spread、slippage、fee 全部纳入 expected_net_edge_bps。

Feature table 行数：{len(features)}
"""


def build_regime_rows(testnet_rows: list[dict[str, Any]], shadow_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in testnet_rows:
        key = (strategy_id(row), "testnet", regime_key(row))
        rec = rows.setdefault(key, {"strategy_id": key[0], "source": key[1], "regime": key[2], "samples": 0, "net_sum": 0.0, "wins": 0})
        net = row_net_pnl(row)
        rec["samples"] += 1
        rec["net_sum"] += net or 0.0
        rec["wins"] += 1 if net is not None and net > 0 else 0
    for row in shadow_rows:
        key = (playbook_id(row), "shadow", str(row.get("liquidity_bucket") or "unknown_liquidity"))
        rec = rows.setdefault(key, {"strategy_id": key[0], "source": key[1], "regime": key[2], "samples": 0, "net_sum": 0.0, "wins": 0})
        ret = safe_float(row.get("after_fee_and_slippage_return_pct")) or safe_float(row.get("after_fee_return_pct"))
        rec["samples"] += 1
        rec["net_sum"] += ret or 0.0
        rec["wins"] += 1 if ret is not None and ret > 0 else 0
    out = []
    for rec in rows.values():
        samples = rec["samples"]
        out.append(
            {
                "strategy_id": rec["strategy_id"],
                "source": rec["source"],
                "regime": rec["regime"],
                "samples": samples,
                "net_sum": round(rec["net_sum"], 6),
                "win_rate_pct": round(100 * rec["wins"] / samples, 4) if samples else "",
                "interpretation": "effective" if rec["net_sum"] > 0 and samples >= 30 else "blocked_or_unproven",
            }
        )
    out.sort(key=lambda x: (x["strategy_id"], x["source"], x["regime"]))
    return out


def regime_md(rows: list[dict[str, Any]]) -> str:
    effective = [r for r in rows if r["interpretation"] == "effective"][:30]
    blocked = [r for r in rows if r["interpretation"] != "effective"][:30]
    return f"""# Regime Research

## Shawn 可读结论

HMM 和 Markov 不能直接当交易信号，只能当 regime / state transition 解释工具。当前 testnet trade 里 HMM/Markov 大多没有可连接的稳定状态字段，因此不能声称某个 HMM state 有 edge。现在可用的 regime 证据主要来自 BTC/ETH/oi/liquidity bucket 的粗粒度字段。

## 当前看起来有效的 regime/source 组合

{md_table(effective, ["strategy_id", "source", "regime", "samples", "net_sum", "win_rate_pct"], limit=30) if effective else "无足够证据"}

## 当前 blocked / unproven 组合

{md_table(blocked, ["strategy_id", "source", "regime", "samples", "net_sum", "win_rate_pct"], limit=30)}

## 建议

- Builder 应把 HMM/Markov state 写入每条 shadow/testnet event。
- Auditor 应检查 state timestamp 是否早于 entry，避免未来函数。
- Regime filter 只能减少明显逆风，不应被当成制造 edge 的魔法。
"""


def hmm_md(root: Path) -> str:
    files = sorted(root.glob("signal_lab_runs/**/hmm_state_report.json"))
    return f"""# HMM State Interpretation

## Shawn 可读结论

当前 HMM report 文件存在 {len(files)} 个，但 testnet trades 没有稳定的 per-trade HMM state join。也就是说，现在只能说“有 HMM 分析产物”，不能说“HMM 已经证明某策略有 edge”。

## 使用边界

- HMM state 只能解释 regime，不直接下单。
- 必须记录 entry 前一刻的 HMM state，不能用事后 state。
- 必须把 state stability、transition probability 和 favorable/adverse outcome 分开审计。

## 已发现文件

{chr(10).join(f'- {p}' for p in files[:20]) if files else '无'}
"""


def markov_rows(root: Path) -> list[dict[str, Any]]:
    files = sorted(root.glob("signal_lab_runs/**/*markov*.json"))
    rows = []
    for p in files:
        rows.append(
            {
                "file": str(p),
                "samples_joined_to_trades": 0,
                "edge_claim_allowed": False,
                "reason": "No stable per-trade Markov state join verified in this Analyst pass.",
            }
        )
    if not rows:
        rows.append(
            {
                "file": "",
                "samples_joined_to_trades": 0,
                "edge_claim_allowed": False,
                "reason": "No Markov report files discovered.",
            }
        )
    return rows


def ranking_md(rows: list[dict[str, Any]]) -> str:
    allowed = [r for r in rows if r.get("recommendation") != "not_recommended"]
    return f"""# Cost-Aware Strategy Ranking

## Shawn 可读结论

按 post-cost edge 排名后，没有任何策略可以直接进入 mainnet。旧 testnet 策略全部没有 net positive。部分 OI shadow 分支显示正向，但仍需要 unique signal 去重和 limited testnet gate，不能直接 promotion。

## 可进一步 shadow / gate 的候选

{md_table(allowed, ["strategy_id", "source", "expected_gross_move_bps", "expected_total_cost_bps", "expected_net_edge_bps", "sample_size", "confidence_score", "recommendation"], limit=30) if allowed else "无"}

## Top ranking raw

{md_table(rows, ["strategy_id", "source", "expected_gross_move_bps", "expected_total_cost_bps", "expected_net_edge_bps", "sample_size", "symbol_concentration_risk", "recommendation"], limit=40)}
"""


def build_evidence_recommendations(
    old_rows: list[dict[str, Any]], shadow_candidates: list[dict[str, Any]], ranking: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    out = []
    for r in old_rows:
        out.append(
            {
                "strategy_id": r["strategy_id"],
                "current_evidence_level": 2 if r["trades"] else 0,
                "failed_testnet": bool(r["trades"] and r["has_post_cost_edge"] == "no"),
                "can_enter_shadow": r["recommendation"] == "shadow-only",
                "can_enter_limited_testnet": False,
                "should_kill": r["recommendation"] == "kill",
                "need_more_data": ["post-entry horizon returns", "execution cost breakdown"] if r["trades"] else ["forward shadow sample"],
                "minimum_sample_requirement": "100 independent trades/signals before any promotion discussion",
                "failure_conditions": ["net PnL <= 0", "expected_net_edge_bps <= 0", "gross move < 3x cost"],
            }
        )
    for c in shadow_candidates:
        out.append(
            {
                "strategy_id": c["strategy_id"],
                "current_evidence_level": 2,
                "failed_testnet": False,
                "can_enter_shadow": True,
                "can_enter_limited_testnet": False,
                "should_kill": c["candidate_status"] == "not_ready",
                "need_more_data": ["unique signal dedup", "30/60/180/300s follow-through", "bidwall persistence", "testnet execution parity"],
                "minimum_sample_requirement": "100 unique shadow signals; then 100 limited testnet trades if shadow remains cost-adjusted positive",
                "failure_conditions": ["shadow net <= 0 after de-dup", "one symbol dominates", "cost exceeds edge"],
            }
        )
    return out


def promotion_md(evidence: list[dict[str, Any]]) -> str:
    kill = [e for e in evidence if e.get("should_kill")]
    shadow = [e for e in evidence if e.get("can_enter_shadow") and not e.get("can_enter_limited_testnet")]
    return f"""# Promotion Recommendations

## Shawn 可读结论

当前没有任何策略可以 mainnet。旧策略没有 post-cost edge，应该 kill 或只留 shadow-only 对照。OI build family 值得继续 shadow research，但目前不允许直接 limited testnet，必须先完成去重、成本、follow-through 和 regime 证据。

## Should kill

{md_table(kill, ["strategy_id", "current_evidence_level", "failed_testnet", "should_kill"], limit=40)}

## Shadow only / observing

{md_table(shadow, ["strategy_id", "current_evidence_level", "can_enter_shadow", "can_enter_limited_testnet", "minimum_sample_requirement"], limit=40)}

## Mainnet

不可以。未来即使 Level 5，也必须 Shawn 人工批准。
"""


def experiment_design_md() -> str:
    return """# Experiment 001 Design

## Experiment 001

OI Build Breakout Family — Cost-Aware Forward Validation

## Shawn 可读结论

这个实验不是救旧策略。目标是把 OI build breakout family 拆成可审计的新市场假设，用 forward shadow 先证明扣成本后仍有 edge，再考虑每天 5-10 单 limited testnet。

## Rules

1. 旧策略全部 shadow-only。
2. 只观察 OI build breakout family。
3. 只允许 major / mega liquidity bucket。
4. 禁止 high slippage symbols。
5. 每日最多 5-10 单 limited testnet，且必须先满足 shadow cost-adjusted positive。
6. 每单必须记录 expected_net_edge_bps。
7. 每单必须记录 strategy_manifest_id。
8. 每单必须记录 HMM regime。
9. 每单必须记录 BTC/ETH regime。
10. 每单必须记录入场后 30s / 60s / 180s follow-through。
11. 100 单前不允许 promotion。
12. 如果 net PnL 仍为负，实验失败，不允许调参硬救。

## Stage Gate

- Stage A: replay sanity check，只验证字段和成本模型。
- Stage B: forward shadow，100 unique signals。
- Stage C: limited testnet，每日 5-10 单。
- Stage D: promoted testnet only if 100 trades net positive。
- Mainnet: no，必须 Shawn 人工批准。
"""


def experiment_success() -> dict[str, Any]:
    return {
        "experiment_id": "experiment_001_oi_build_breakout_family_cost_aware_forward_validation",
        "success_criteria": {
            "shadow_unique_signals_min": 100,
            "shadow_net_return_positive": True,
            "gross_move_min_multiple_of_cost": 3,
            "limited_testnet_trades_min": 100,
            "testnet_net_pnl_positive": True,
            "net_profit_factor_min": 1.15,
            "top_symbol_concentration_max_pct": 35,
            "mainnet_live_allowed": False,
        },
    }


def experiment_failure() -> dict[str, Any]:
    return {
        "experiment_id": "experiment_001_oi_build_breakout_family_cost_aware_forward_validation",
        "failure_criteria": {
            "expected_net_edge_bps_lte_zero": True,
            "gross_move_lt_3x_cost": True,
            "shadow_net_negative_after_100_unique_signals": True,
            "testnet_net_negative_after_100_trades": True,
            "one_symbol_dominates_pct_gt": 35,
            "cost_or_slippage_unavailable": True,
            "missing_strategy_manifest_id": True,
        },
    }


def builder_tasks_md() -> str:
    return """# Experiment 001 Required Builder Tasks

1. Add `strategy_manifest_id` to every shadow/testnet event.
2. Add `expected_gross_move_bps`, `expected_total_cost_bps`, `expected_net_edge_bps`, `cost_to_edge_ratio`.
3. Record post-entry returns at 30s / 60s / 180s / 300s / 900s.
4. Record bidwall_strength, bidwall_persistence_sec, bid_depth_change_after_breakout.
5. Record BTC/ETH/HMM/Markov regime at entry timestamp only.
6. Add unique signal de-dup so multi-horizon outcomes cannot fake sample size.
7. Block limited testnet when expected_net_edge_bps <= 0 or gross < 3x cost.
8. Keep all mainnet live flags disabled.
"""


def auditor_checks_md() -> str:
    return """# Experiment 001 Required Auditor Checks

1. Confirm no mainnet live flag is enabled.
2. Confirm old strategies are shadow-only and not renamed into new candidates.
3. Check every candidate has a market hypothesis and required cost fields.
4. Verify unique signal count separately from horizon outcome count.
5. Verify post-entry follow-through fields are recorded after entry, not inferred from future leakage.
6. Verify net PnL uses fee, spread, slippage, funding where available.
7. Verify promotion is blocked before 100 independent limited testnet trades.
8. Verify no report leaks API key, secret, token, or private key.
"""


def final_report_md(
    status: dict[str, Any],
    old_rows: list[dict[str, Any]],
    kill: list[str],
    shadow_only: list[str],
    rewrite: list[dict[str, Any]],
    mode_total: Counter[str],
    gap_matrix: list[dict[str, Any]],
    data_rows: list[dict[str, Any]],
    shadow_candidates: list[dict[str, Any]],
    ranking: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    out_dir: Path,
) -> str:
    positive_rank = [r for r in ranking if r.get("recommendation") != "not_recommended"]
    top_dir = (status.get("top20_dirs") or [{}])[0]
    recent_top = (status.get("recent_7d_by_top_level") or [{}])[0]
    return f"""# Phoenix Analyst Final Report

【老板判断区】

1. 结论：旧 testnet 策略没有证明正期望；当前最值得研究的是 OI build breakout family，但只能先做 cost-aware forward shadow，不能直接 mainnet。
2. 状态：黄。
3. 是否真进步：证据不足。本次是分析和实验设计，不是策略收益变好。
4. 是否新策略：有新策略假设草案，但还不是已验证新策略。
5. 是否可以 testnet：旧策略不可以；OI family 需要先满足 shadow cost-adjusted positive 和去重样本要求。
6. 是否可以 mainnet：不可以。
7. Shawn 需要做什么：叫 Builder 补字段和 Experiment 001；叫 Auditor 查样本去重、成本、promotion gate。
8. 证据够不够：旧策略死亡证据够；新策略上线证据不够。

【VPS 状态】

- 磁盘使用率：{status['disk_use_pct']}%
- 剩余空间：{status['disk_free_human']}
- 最大目录：{top_dir.get('path', 'unavailable')} ({top_dir.get('size', 'unavailable')})
- 最近增长最快目录：{recent_top.get('top_level', 'unavailable')} ({recent_top.get('size', 'unavailable')})
- 是否有爆盘风险：{status['risk']}
- 是否需要清理：否，暂时不需要真实删除；后续可做 dry-run cleanup plan。
- 是否已经执行删除：否。

## 1. 旧策略死亡名单

{", ".join(kill) if kill else "无"}

## 2. 旧策略 shadow-only 名单

{", ".join(shadow_only) if shadow_only else "无"}

## 3. 可重写策略名单

{md_table(rewrite, ["strategy_id", "rewrite_basis", "gross_pnl_usdt", "net_pnl_usdt"]) if rewrite else "无"}

## 4. 为什么 backtest 不可信

backtest 是 Level 0，只能研究，不可交易。现有 gap matrix 显示 backtest positive forward negative 的情况，尤其 `oi_build_breakout`：历史模拟很强，但 forward shadow 同名 playbook 不稳。原因可能是成本、entry delay、symbol concentration、行情段贡献、multi-branch outcome fan-out。

## 5. 最主要 failure modes

{md_table([{"failure_mode": k, "count": v, "zh": FAILURE_MODE_DEFS.get(k, {}).get("zh", "")} for k, v in mode_total.most_common(12)], ["failure_mode", "zh", "count"])}

## 6. 最值得研究的新策略家族

1. OI build breakout continuation
2. bidwall + OI build continuation
3. OI build fakeout reversal
4. liquidation cascade / absorption family

## 7. OI build breakout 是否值得 limited testnet

现在不值得直接 limited testnet。值得先做 Experiment 001 的 cost-aware forward shadow。部分 shadow 分支为正，但需要 unique signal 去重、bidwall/orderbook 字段、30s/60s/180s/300s follow-through 字段。

## 8. 应该禁用的 symbol / liquidity bucket

- 先禁用 high slippage symbols。
- 只允许 major / mega liquidity bucket。
- 禁用 one-symbol dominated candidate，top symbol concentration 超过 35% 不允许 promotion。

## 9. 应该禁用的 regime

- high_vol_chop
- low_liquidity_chop
- adverse BTC/ETH regime
- HMM/Markov 未稳定连接前，不允许用它们直接开仓。

## 10. Cost-aware ranking

{md_table(positive_rank, ["strategy_id", "source", "expected_net_edge_bps", "sample_size", "recommendation"], limit=20) if positive_rank else "没有可直接推荐 testnet promotion 的候选。"}

## 11. Experiment 001 完整方案

见 `experiment_001_design.md`、`experiment_001_success_criteria.json`、`experiment_001_failure_criteria.json`。

## 12. Builder 需要实现

见 `experiment_001_required_builder_tasks.md`。重点是 expected_net_edge_bps、post-entry horizon returns、bidwall persistence、unique signal de-dup。

## 13. Auditor 需要重点审计

见 `experiment_001_required_auditor_checks.md`。重点是 mainnet 禁止、旧策略不能换名、sample 去重、成本真实扣除。

## 14. 当前是否可以 mainnet

No。不可以。除非未来有独立 Level 5 证据，并且 Shawn 人工批准。

## 15. 最值得保留的数据

testnet trades、shadow outcomes、strategy/experiment manifest、promotion/evidence gate、cost reports、最近 30 日关键 collector 样本、能复现结论的最小 feature table。

## 16. 垃圾 report / 低价值数据

重复日报、旧 log、没有新增信息的 report、没有 summary 的大型中间文件、tmp/cache。

## 17. 建议压缩或归档

旧 jsonl log、旧 shadow outcome 明细、旧 collector 样本、旧 backtest/replay 明细。只建议，不执行真实删除。

## 18. 可复制给 ChatGPT 的 summary 文件

1. `{out_dir}/phoenix_analyst_final_report.md`
2. `{out_dir}/old_strategy_autopsy.md`
3. `{out_dir}/failure_mode_report.md`
4. `{out_dir}/backtest_forward_gap_analysis.md`
5. `{out_dir}/oi_build_breakout_research.md`
6. `{out_dir}/experiment_001_design.md`

【Shawn 交接包】

1. 本次任务名称：Phoenix Analyst old strategy autopsy + new strategy evidence chain
2. 当前 thread：Analyst
3. VPS 状态：磁盘 {status['disk_use_pct']}%，剩余 {status['disk_free_human']}，爆盘风险 {status['risk']}。
4. Phoenix 运行状态：collector/shadow/testnet/dashboard 未停止；mainnet live 风险无。
5. 本次做了什么：生成旧策略尸检、failure taxonomy、backtest-forward gap、data value、new strategy hypotheses、OI research、regime research、cost ranking、promotion recommendations、Experiment 001。
6. 本次没有做什么：没有删数据，没有改交易配置，没有停止服务，没有开启 mainnet。
7. 关键结论：旧策略没有 post-cost edge；OI build family 值得研究但不能直接 testnet/mainnet。
8. 是否真进步：证据不足；这是研究框架和证据包，不是收益改善。
9. 是否新策略：有假设草案，不是已验证策略。
10. 证据：旧策略 testnet trades 见 CSV；shadow/backtest gap 见 matrix；expected net edge 见 ranking。
11. 修改文件：无 Phoenix 源码/配置修改。
12. 新增文件：本目录所有 report artifacts。
13. 删除文件：无。
14. 需要 Shawn 决定的事：是否批准 Builder 做 Experiment 001 字段实现；是否批准 Auditor 做独立 sample de-dup 审计；是否后续生成 storage cleanup dry-run plan。
15. 下一步建议：Builder 补 Experiment 001 字段；Auditor 审计 gate 和 sample 去重；Analyst 等 100 unique shadow signals 后复评。
16. 一句话摘要：旧策略应冻结/死亡，Phoenix 下一步不是调参，而是围绕 OI build breakout family 建立 replay -> shadow -> limited testnet 的成本优先证据链。
"""


def main() -> None:
    root = Path(os.environ.get("PHOENIX_ROOT", "/opt/phoenix-testnet")).resolve()
    out = root / "analyst_reports" / f"phoenix_analyst_{now_stamp()}"
    build_reports(root, out)
    print(out)


if __name__ == "__main__":
    main()
