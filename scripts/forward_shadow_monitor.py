#!/usr/bin/env python3
"""Forward shadow monitor for Phoenix baseline and candidate shadow runs.

This script is reporting-only. It reads shadow outputs, writes JSON/Markdown
summaries, and never changes strategy controls or execution settings.
"""

from __future__ import annotations

import json
import math
import os
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path("/opt/phoenix-testnet")
RUNS_DIR = ROOT / "signal_lab_runs"
JSON_REPORT = RUNS_DIR / "forward_shadow_monitor_report.json"
MD_REPORT = RUNS_DIR / "forward_shadow_monitor_report.md"

EXPERIMENTS = [
    {
        "name": "clean_baseline",
        "kind": "baseline",
        "dir": RUNS_DIR / "vps_forward_shadow_baseline_clean_20260430_100038",
        "allowed_playbooks": {"oi_build_breakout", "liquidation_flush"},
    },
    {
        "name": "candidate_only",
        "kind": "candidate",
        "dir": RUNS_DIR / "vps_forward_shadow_candidate_validation_20260430_072436",
        "allowed_playbooks": None,
    },
]


RETURN_KEYS = (
    "after_fee_and_slippage_return_pct",
    "after_fee_return_pct",
    "return_pct",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {"_error": type(exc).__name__, "_message": str(exc)}
    return payload if isinstance(payload, dict) else {"_value": payload}


def tail_text(path: Path, max_chars: int = 4000) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[-max_chars:]
    except FileNotFoundError:
        return ""


def tail_jsonl(path: Path, limit: int = 10) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]
    except FileNotFoundError:
        return []
    records: list[dict[str, Any]] = []
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(compact_record(payload))
    return records


def iter_jsonl(path: Path):
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    yield payload
    except FileNotFoundError:
        return


def compact_record(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "at",
        "event_id",
        "source_event_id",
        "symbol",
        "playbook",
        "side",
        "branch_type",
        "shadow_branch_id",
        "research_only",
        "after_fee_return_pct",
        "after_fee_and_slippage_return_pct",
        "exit_reason",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def get_playbook(payload: dict[str, Any]) -> str:
    return str(
        payload.get("playbook")
        or payload.get("candidate_playbook")
        or payload.get("strategy")
        or payload.get("playbook_name")
        or "unknown"
    )


def get_candidate_key(payload: dict[str, Any]) -> str:
    playbook = get_playbook(payload)
    branch = (
        payload.get("shadow_branch_id")
        or payload.get("branch_id")
        or payload.get("branch_type")
        or "default"
    )
    return f"{playbook}|{branch}"


def extract_return_pct(payload: dict[str, Any]) -> float | None:
    for key in RETURN_KEYS:
        value = payload.get(key)
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            return float(value)
    return None


def metrics_from_returns(values: list[float]) -> dict[str, Any]:
    count = len(values)
    if count == 0:
        return {
            "numeric_outcome_count": 0,
            "avg_after_fee_return_pct": None,
            "profit_factor": None,
            "win_rate_pct": None,
            "max_drawdown_pct": None,
            "win_count": 0,
            "loss_count": 0,
        }
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    if gross_loss > 0:
        profit_factor: float | None = gross_win / gross_loss
    elif gross_win > 0:
        profit_factor = 999999.0
    else:
        profit_factor = None
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {
        "numeric_outcome_count": count,
        "avg_after_fee_return_pct": sum(values) / count,
        "profit_factor": profit_factor,
        "win_rate_pct": len(wins) / count * 100.0,
        "max_drawdown_pct": max_drawdown,
        "win_count": len(wins),
        "loss_count": len(losses),
    }


def round_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    rounded: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, float):
            rounded[key] = round(value, 6)
        else:
            rounded[key] = value
    return rounded


def decision_for(kind: str, outcome_count: int, metrics: dict[str, Any]) -> str:
    avg_return = metrics.get("avg_after_fee_return_pct")
    profit_factor = metrics.get("profit_factor")
    if outcome_count < 30:
        return "continue_shadow"
    if (
        outcome_count >= 100
        and isinstance(profit_factor, (int, float))
        and profit_factor >= 1.2
        and isinstance(avg_return, (int, float))
        and avg_return > 0
    ):
        return "promote_to_paper_test"
    if (
        kind == "candidate"
        and isinstance(avg_return, (int, float))
        and isinstance(profit_factor, (int, float))
        and (avg_return <= 0 or profit_factor < 1.0)
    ):
        return "pause_candidate"
    return "continue_shadow"


def decision_reason(outcome_count: int, metrics: dict[str, Any], decision: str) -> str:
    if outcome_count < 30:
        return "outcome_count < 30, no conclusion allowed"
    if decision == "promote_to_paper_test":
        return "outcome_count >= 100, PF >= 1.2, and avg_after_fee_return_pct > 0"
    if decision == "pause_candidate":
        return "candidate has enough early samples and aggregate expectancy is not positive"
    return "keep collecting forward shadow samples"


def latest_started_event(stdout_path: Path) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for line in tail_text(stdout_path, max_chars=200000).splitlines():
        if "signal_bridge_started" not in line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            latest = payload
    return latest


def process_rows() -> list[dict[str, Any]]:
    try:
        output = subprocess.check_output(
            ["ps", "-eo", "pid,ppid,comm,rss,etime,args", "--sort=-rss"],
            text=True,
            errors="replace",
        )
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for line in output.splitlines()[1:]:
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        pid, ppid, command, rss, etime, args = parts
        if "phoenix_signal_lab.py collect" not in args and "phoenix_signal_bridge.py" not in args:
            continue
        if "python3 - <<" in args:
            continue
        rows.append(
            {
                "pid": int(pid),
                "ppid": int(ppid),
                "command": command,
                "rss_mb": round(int(rss) / 1024.0, 1),
                "etime": etime,
                "args": args,
            }
        )
    return rows


def matching_process(exp_dir: Path, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    needle = str(exp_dir)
    for row in rows:
        if needle in str(row.get("args") or ""):
            return row
    return None


def summarize_experiment(config: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    exp_dir = Path(config["dir"])
    shadow_dir = exp_dir / "mainnet_shadow"
    signals_file = shadow_dir / "signal_bridge_shadow_signals.jsonl"
    outcomes_file = shadow_dir / "signal_bridge_shadow_outcomes.jsonl"
    stderr_file = exp_dir / "bridge_stderr.log"
    stdout_file = exp_dir / "bridge_stdout.log"

    signal_records = list(iter_jsonl(signals_file))
    outcome_records = list(iter_jsonl(outcomes_file))
    outcome_returns = [value for value in (extract_return_pct(item) for item in outcome_records) if value is not None]

    per_playbook_returns: dict[str, list[float]] = defaultdict(list)
    per_playbook_outcomes: Counter[str] = Counter()
    per_playbook_signals: Counter[str] = Counter(get_playbook(item) for item in signal_records)
    per_candidate_returns: dict[str, list[float]] = defaultdict(list)
    per_candidate_outcomes: Counter[str] = Counter()
    per_candidate_signals: Counter[str] = Counter(get_candidate_key(item) for item in signal_records)

    for item in outcome_records:
        playbook = get_playbook(item)
        candidate = get_candidate_key(item)
        per_playbook_outcomes[playbook] += 1
        per_candidate_outcomes[candidate] += 1
        value = extract_return_pct(item)
        if value is not None:
            per_playbook_returns[playbook].append(value)
            per_candidate_returns[candidate].append(value)

    def grouped_metrics(
        outcomes: Counter[str],
        signals: Counter[str],
        returns_by_key: dict[str, list[float]],
    ) -> dict[str, dict[str, Any]]:
        keys = set(outcomes) | set(signals)
        grouped: dict[str, dict[str, Any]] = {}
        for key in sorted(keys):
            values = returns_by_key.get(key, [])
            metrics = round_metrics(metrics_from_returns(values))
            decision = decision_for(str(config["kind"]), int(outcomes.get(key, 0)), metrics)
            grouped[key] = {
                "signal_count": int(signals.get(key, 0)),
                "outcome_count": int(outcomes.get(key, 0)),
                **metrics,
                "decision": decision,
            }
        return grouped

    started = latest_started_event(stdout_file) or {}
    process = matching_process(exp_dir, rows)
    manifest = read_json(exp_dir / "experiment_manifest.json") or {}
    readiness = read_json(shadow_dir / "mainnet_shadow_readiness.json") or {}
    promotion = read_json(shadow_dir / "promotion_gate_report.json") or {}

    live_trading_enabled = (
        promotion.get("live_trading_enabled")
        if "live_trading_enabled" in promotion
        else readiness.get("live_trading_enabled", manifest.get("live_trading_enabled", False))
    )
    promotion_allowed = promotion.get("promotion_allowed", manifest.get("promotion_allowed", False))
    user_data_oms_enabled = started.get("user_data_oms_enabled")
    if user_data_oms_enabled is None:
        launch = read_json(exp_dir / "launch_command.json") or {}
        argv = launch.get("argv") if isinstance(launch, dict) else None
        launch_args = [str(item) for item in argv] if isinstance(argv, list) else []
        process_args = str((process or {}).get("args") or "")
        if "--disable-user-data-oms" in launch_args or "--disable-user-data-oms" in process_args:
            user_data_oms_enabled = False
        elif started.get("execution_mode") == "MAINNET_SHADOW" or "MAINNET_SHADOW" in process_args:
            user_data_oms_enabled = False
        else:
            user_data_oms_enabled = True

    metrics = round_metrics(metrics_from_returns(outcome_returns))
    outcome_count = len(outcome_records)
    decision = decision_for(str(config["kind"]), outcome_count, metrics)
    allowed = config.get("allowed_playbooks")
    recent = tail_jsonl(signals_file, 10) + tail_jsonl(outcomes_file, 10)
    prohibited_recent_records = []
    if allowed:
        prohibited_recent_records = [
            item for item in recent if str(item.get("playbook") or "") not in allowed
        ]

    stderr_size = stderr_file.stat().st_size if stderr_file.exists() else None
    return {
        "name": config["name"],
        "kind": config["kind"],
        "dir": str(exp_dir),
        "process": process,
        "signal_count": len(signal_records),
        "outcome_count": outcome_count,
        **metrics,
        "per_playbook": grouped_metrics(per_playbook_outcomes, per_playbook_signals, per_playbook_returns),
        "per_candidate": grouped_metrics(per_candidate_outcomes, per_candidate_signals, per_candidate_returns),
        "recent_signals": tail_jsonl(signals_file, 10),
        "recent_outcomes": tail_jsonl(outcomes_file, 10),
        "stderr": {
            "path": str(stderr_file),
            "exists": stderr_file.exists(),
            "size_bytes": stderr_size,
            "empty": stderr_size == 0 if stderr_size is not None else None,
            "tail": tail_text(stderr_file, 2000),
        },
        "safety": {
            "live_trading_enabled": bool(live_trading_enabled),
            "promotion_allowed": bool(promotion_allowed),
            "user_data_oms_enabled": bool(user_data_oms_enabled),
            "all_false": not bool(live_trading_enabled)
            and not bool(promotion_allowed)
            and not bool(user_data_oms_enabled),
        },
        "allowed_playbooks": sorted(allowed) if allowed else None,
        "recent_records_only_allowed": len(prohibited_recent_records) == 0,
        "prohibited_recent_records": prohibited_recent_records,
        "decision": decision,
        "decision_reason": decision_reason(outcome_count, metrics, decision),
    }


def system_status() -> dict[str, Any]:
    status: dict[str, Any] = {}
    for name, command in {
        "memory": ["free", "-m"],
        "disk_root": ["df", "-h", "/"],
    }.items():
        try:
            status[name] = subprocess.check_output(command, text=True, errors="replace")
        except Exception as exc:
            status[name] = f"{type(exc).__name__}: {exc}"
    return status


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix Forward Shadow Monitor",
        "",
        f"- Generated at UTC: `{report['generated_at']}`",
        "- Mode: reporting-only; no strategy controls changed.",
        "",
    ]
    for experiment in report["experiments"]:
        process = experiment.get("process") or {}
        lines.extend(
            [
                f"## {experiment['name']}",
                "",
                f"- Dir: `{experiment['dir']}`",
                f"- PID/RSS: `{process.get('pid')}` / `{process.get('rss_mb')} MB`",
                f"- Signals / outcomes: `{experiment['signal_count']}` / `{experiment['outcome_count']}`",
                f"- Avg after-fee return: `{experiment['avg_after_fee_return_pct']}`",
                f"- Profit factor: `{experiment['profit_factor']}`",
                f"- Win rate: `{experiment['win_rate_pct']}`",
                f"- Max drawdown pct: `{experiment['max_drawdown_pct']}`",
                f"- Stderr empty: `{experiment['stderr']['empty']}`",
                f"- Safety all false: `{experiment['safety']['all_false']}` "
                f"(live={experiment['safety']['live_trading_enabled']}, "
                f"promotion={experiment['safety']['promotion_allowed']}, "
                f"user_data_oms={experiment['safety']['user_data_oms_enabled']})",
                f"- Decision: `{experiment['decision']}`",
                f"- Decision reason: {experiment['decision_reason']}",
                "",
                "### Per Playbook",
                "",
                "| playbook | signals | outcomes | avg_after_fee | PF | win_rate | max_DD | decision |",
                "|---|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for playbook, metrics in experiment["per_playbook"].items():
            lines.append(
                "| {playbook} | {signal_count} | {outcome_count} | {avg} | {pf} | {win} | {dd} | {decision} |".format(
                    playbook=playbook,
                    signal_count=metrics["signal_count"],
                    outcome_count=metrics["outcome_count"],
                    avg=metrics["avg_after_fee_return_pct"],
                    pf=metrics["profit_factor"],
                    win=metrics["win_rate_pct"],
                    dd=metrics["max_drawdown_pct"],
                    decision=metrics["decision"],
                )
            )
        lines.extend(
            [
                "",
                "### Per Candidate",
                "",
                "| candidate | signals | outcomes | avg_after_fee | PF | win_rate | max_DD | decision |",
                "|---|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for candidate, metrics in experiment["per_candidate"].items():
            lines.append(
                "| {candidate} | {signal_count} | {outcome_count} | {avg} | {pf} | {win} | {dd} | {decision} |".format(
                    candidate=candidate,
                    signal_count=metrics["signal_count"],
                    outcome_count=metrics["outcome_count"],
                    avg=metrics["avg_after_fee_return_pct"],
                    pf=metrics["profit_factor"],
                    win=metrics["win_rate_pct"],
                    dd=metrics["max_drawdown_pct"],
                    decision=metrics["decision"],
                )
            )
        lines.extend(["", "### Recent Signals", ""])
        if experiment["recent_signals"]:
            lines.append("```json")
            lines.append(json.dumps(experiment["recent_signals"], ensure_ascii=False, indent=2))
            lines.append("```")
        else:
            lines.append("_No signals yet._")
        lines.extend(["", "### Recent Outcomes", ""])
        if experiment["recent_outcomes"]:
            lines.append("```json")
            lines.append(json.dumps(experiment["recent_outcomes"], ensure_ascii=False, indent=2))
            lines.append("```")
        else:
            lines.append("_No outcomes yet._")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    rows = process_rows()
    report = {
        "generated_at": utc_now().isoformat(),
        "report_type": "phoenix_forward_shadow_monitor",
        "policy": {
            "outcome_count_lt_30_no_conclusion": True,
            "paper_allowed_min_outcomes": 100,
            "paper_allowed_min_profit_factor": 1.2,
            "paper_requires_positive_avg_after_fee": True,
            "reporting_only": True,
        },
        "experiments": [summarize_experiment(config, rows) for config in EXPERIMENTS],
        "processes": rows,
        "system_status": system_status(),
    }
    JSON_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    MD_REPORT.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps({"json_report": str(JSON_REPORT), "md_report": str(MD_REPORT)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
