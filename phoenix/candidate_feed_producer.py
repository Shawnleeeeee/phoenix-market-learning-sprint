from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HEARTBEAT_FILE_NAME = "candidate_producer_heartbeat.json"
EVENT_LOG_FILE_NAME = "candidate_producer_events.jsonl"
STDOUT_LOG_FILE_NAME = "candidate_producer_stdout.log"
STDERR_LOG_FILE_NAME = "candidate_producer_stderr.log"


@dataclass(slots=True)
class CandidateFeedHealth:
    running: bool
    latest_candidate_at: str | None
    latest_candidate_age_sec: float | None
    candidate_count: int
    fresh_count: int
    stale_count: int
    event_snapshots_path: str
    bridge_event_feed_path: str
    event_snapshots_mtime: str | None
    bridge_event_feed_mtime: str | None
    stale_after_sec: int
    process_pid: int | None = None
    last_error: str | None = None
    producer_alive: bool = False
    candidate_fresh: bool = False
    candidate_available: bool = False
    candidate_tradeable: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "latest_candidate_at": self.latest_candidate_at,
            "latest_candidate_age_sec": self.latest_candidate_age_sec,
            "candidate_count": self.candidate_count,
            "fresh_count": self.fresh_count,
            "stale_count": self.stale_count,
            "event_snapshots_path": self.event_snapshots_path,
            "bridge_event_feed_path": self.bridge_event_feed_path,
            "event_snapshots_mtime": self.event_snapshots_mtime,
            "bridge_event_feed_mtime": self.bridge_event_feed_mtime,
            "stale_after_sec": self.stale_after_sec,
            "process_pid": self.process_pid,
            "last_error": self.last_error,
            "producer_alive": self.producer_alive,
            "candidate_fresh": self.candidate_fresh,
            "candidate_available": self.candidate_available,
            "candidate_tradeable": self.candidate_tradeable,
            "created_at": utc_now(),
        }


class CandidateProducerService:
    def __init__(
        self,
        *,
        output_dir: str | Path,
        python_bin: str | Path | None = None,
        project_root: str | Path = ".",
        env: str = "prod",
        cycle_sec: int = 45,
        horizons_sec: str = "60,180,300,900,1800,3600",
        scan_top: int = 120,
        universe_top: int = 200,
        scan_step: int = 60,
        dedupe_sec: int = 30,
        symbol_cooldown_sec: int = 60,
        max_active_per_symbol: int = 4,
        max_events_per_cycle: int = 50,
        ranked_samples_per_cycle: int = 0,
        ranked_min_trigger_ratio: float = 0.75,
        worker_count: int = 3,
        kline_concurrency: int = 5,
        min_quote_volume: int = 5_000_000,
        trigger_min_quote_volume: int = 5_000_000,
        round_trip_fee_bps: int = 8,
        heartbeat_interval_sec: float = 30.0,
        stale_after_sec: int = 60,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.project_root = Path(project_root)
        self.python_bin = str(python_bin or sys.executable)
        self.env = env
        self.cycle_sec = int(cycle_sec)
        self.horizons_sec = str(horizons_sec)
        self.scan_top = int(scan_top)
        self.universe_top = int(universe_top)
        self.scan_step = int(scan_step)
        self.dedupe_sec = int(dedupe_sec)
        self.symbol_cooldown_sec = int(symbol_cooldown_sec)
        self.max_active_per_symbol = int(max_active_per_symbol)
        self.max_events_per_cycle = int(max_events_per_cycle)
        self.ranked_samples_per_cycle = int(ranked_samples_per_cycle)
        self.ranked_min_trigger_ratio = float(ranked_min_trigger_ratio)
        self.worker_count = int(worker_count)
        self.kline_concurrency = int(kline_concurrency)
        self.min_quote_volume = int(min_quote_volume)
        self.trigger_min_quote_volume = int(trigger_min_quote_volume)
        self.round_trip_fee_bps = int(round_trip_fee_bps)
        self.heartbeat_interval_sec = max(1.0, float(heartbeat_interval_sec))
        self.stale_after_sec = max(1, int(stale_after_sec))

    @property
    def heartbeat_path(self) -> Path:
        return self.output_dir / HEARTBEAT_FILE_NAME

    @property
    def event_log_path(self) -> Path:
        return self.output_dir / EVENT_LOG_FILE_NAME

    def build_command(self) -> list[str]:
        return build_collect_command(
            python_bin=self.python_bin,
            output_dir=self.output_dir,
            env=self.env,
            cycle_sec=self.cycle_sec,
            horizons_sec=self.horizons_sec,
            scan_top=self.scan_top,
            universe_top=self.universe_top,
            scan_step=self.scan_step,
            dedupe_sec=self.dedupe_sec,
            symbol_cooldown_sec=self.symbol_cooldown_sec,
            max_active_per_symbol=self.max_active_per_symbol,
            max_events_per_cycle=self.max_events_per_cycle,
            ranked_samples_per_cycle=self.ranked_samples_per_cycle,
            ranked_min_trigger_ratio=self.ranked_min_trigger_ratio,
            worker_count=self.worker_count,
            kline_concurrency=self.kline_concurrency,
            min_quote_volume=self.min_quote_volume,
            trigger_min_quote_volume=self.trigger_min_quote_volume,
            round_trip_fee_bps=self.round_trip_fee_bps,
        )

    def run_forever(self) -> int:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        command = self.build_command()
        stop_requested = {"stop": False}
        child: subprocess.Popen[Any] | None = None

        def _request_stop(signum: int, _frame: Any) -> None:
            stop_requested["stop"] = True
            append_jsonl(self.event_log_path, {"event": "stop_requested", "signal": signum, "created_at": utc_now()})
            if child is not None and child.poll() is None:
                child.terminate()

        signal.signal(signal.SIGTERM, _request_stop)
        signal.signal(signal.SIGINT, _request_stop)

        stdout_path = self.output_dir / STDOUT_LOG_FILE_NAME
        stderr_path = self.output_dir / STDERR_LOG_FILE_NAME
        append_jsonl(
            self.event_log_path,
            {
                "event": "candidate_producer_starting",
                "created_at": utc_now(),
                "command": command,
                "output_dir": str(self.output_dir),
            },
        )
        with stdout_path.open("ab") as stdout_handle, stderr_path.open("ab") as stderr_handle:
            child_env = os.environ.copy()
            child_env.update(
                {
                    "PHOENIX_MAINNET_LIVE_ENABLED": "false",
                    "PHOENIX_PROMOTION_ALLOWED": "false",
                    "PHOENIX_AUTO_PROMOTION": "false",
                }
            )
            child = subprocess.Popen(
                command,
                cwd=str(self.project_root),
                stdout=stdout_handle,
                stderr=stderr_handle,
                env=child_env,
            )
            last_heartbeat = 0.0
            while child.poll() is None and not stop_requested["stop"]:
                now = time.monotonic()
                if now - last_heartbeat >= self.heartbeat_interval_sec:
                    self.write_heartbeat(running=True, process_pid=child.pid)
                    last_heartbeat = now
                time.sleep(1.0)
            return_code = child.poll()
            if return_code is None:
                child.terminate()
                try:
                    return_code = child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    return_code = child.wait(timeout=10)
        self.write_heartbeat(running=False, process_pid=child.pid if child is not None else None)
        append_jsonl(
            self.event_log_path,
            {
                "event": "candidate_producer_stopped",
                "created_at": utc_now(),
                "return_code": return_code,
            },
        )
        return int(return_code or 0)

    def write_heartbeat(self, *, running: bool, process_pid: int | None = None) -> None:
        health = collect_candidate_feed_health(
            self.output_dir,
            running=running,
            process_pid=process_pid,
            stale_after_sec=self.stale_after_sec,
        )
        write_json_atomic(self.heartbeat_path, health.to_dict())


def build_collect_command(
    *,
    python_bin: str,
    output_dir: str | Path,
    env: str,
    cycle_sec: int,
    horizons_sec: str,
    scan_top: int,
    universe_top: int,
    scan_step: int = 60,
    dedupe_sec: int = 30,
    symbol_cooldown_sec: int = 60,
    max_active_per_symbol: int = 4,
    max_events_per_cycle: int = 50,
    ranked_samples_per_cycle: int = 0,
    ranked_min_trigger_ratio: float = 0.75,
    worker_count: int = 3,
    kline_concurrency: int = 5,
    min_quote_volume: int = 5_000_000,
    trigger_min_quote_volume: int = 5_000_000,
    round_trip_fee_bps: int = 8,
) -> list[str]:
    return [
        str(python_bin),
        "phoenix_signal_lab.py",
        "collect",
        "--env",
        str(env),
        "--output-dir",
        str(output_dir),
        "--duration-sec",
        "0",
        "--cycle-sec",
        str(cycle_sec),
        "--horizons-sec",
        str(horizons_sec),
        "--scan-top",
        str(scan_top),
        "--universe-top",
        str(universe_top),
        "--scan-step",
        str(scan_step),
        "--ranked-samples-per-cycle",
        str(ranked_samples_per_cycle),
        "--ranked-min-trigger-ratio",
        str(ranked_min_trigger_ratio),
        "--dedupe-sec",
        str(dedupe_sec),
        "--symbol-cooldown-sec",
        str(symbol_cooldown_sec),
        "--max-active-per-symbol",
        str(max_active_per_symbol),
        "--max-events-per-cycle",
        str(max_events_per_cycle),
        "--worker-count",
        str(worker_count),
        "--kline-concurrency",
        str(kline_concurrency),
        "--min-quote-volume",
        str(min_quote_volume),
        "--trigger-min-quote-volume",
        str(trigger_min_quote_volume),
        "--round-trip-fee-bps",
        str(round_trip_fee_bps),
    ]


def collect_candidate_feed_health(
    output_dir: str | Path,
    *,
    running: bool,
    process_pid: int | None = None,
    stale_after_sec: int = 60,
    tail_lines: int = 500,
) -> CandidateFeedHealth:
    output_path = Path(output_dir)
    event_snapshots = output_path / "event_snapshots.jsonl"
    bridge_feed = output_path / "bridge_event_feed.jsonl"
    rows = read_jsonl_tail(event_snapshots, max_lines=tail_lines)
    latest_candidate_at: str | None = None
    latest_age: float | None = None
    fresh_count = 0
    stale_count = 0
    for row in rows:
        candidate_at = candidate_timestamp(row)
        age = age_sec(candidate_at)
        if age is not None and (latest_age is None or age < latest_age):
            latest_age = age
            latest_candidate_at = candidate_at
        if age is None or age > stale_after_sec:
            stale_count += 1
        else:
            fresh_count += 1
    producer_alive = bool(running and process_pid)
    candidate_fresh = bool(latest_age is not None and latest_age <= stale_after_sec and fresh_count > 0)
    candidate_available = bool(len(rows) > 0)
    return CandidateFeedHealth(
        running=running,
        latest_candidate_at=latest_candidate_at,
        latest_candidate_age_sec=round(latest_age, 3) if latest_age is not None else None,
        candidate_count=len(rows),
        fresh_count=fresh_count,
        stale_count=stale_count,
        event_snapshots_path=str(event_snapshots),
        bridge_event_feed_path=str(bridge_feed),
        event_snapshots_mtime=file_mtime(event_snapshots),
        bridge_event_feed_mtime=file_mtime(bridge_feed),
        stale_after_sec=max(1, int(stale_after_sec)),
        process_pid=process_pid,
        producer_alive=producer_alive,
        candidate_fresh=candidate_fresh,
        candidate_available=candidate_available,
        candidate_tradeable=candidate_fresh,
    )


def read_jsonl_tail(path: Path, *, max_lines: int = 500, max_bytes: int = 2_000_000) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes), os.SEEK_SET)
            payload = handle.read().decode("utf-8", errors="replace")
    except OSError:
        return []
    lines = payload.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    rows: deque[dict[str, Any]] = deque(maxlen=max_lines)
    for line in lines:
        text = line.strip()
        if not text or not text.startswith("{"):
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return list(rows)


def candidate_timestamp(row: dict[str, Any]) -> str | None:
    market_snapshot = row.get("market_snapshot") if isinstance(row.get("market_snapshot"), dict) else {}
    return coerce_timestamp_value(
        row.get("signal_time")
        or row.get("simulated_entry_time")
        or row.get("observed_at")
        or row.get("created_at")
        or row.get("generated_at")
        or row.get("signal_time_ms")
        or row.get("observed_at_ms")
        or market_snapshot.get("observed_at_ms")
        or market_snapshot.get("updated_at")
    )


def coerce_timestamp_value(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, dict):
        for key in ("updated_at", "updatedAt", "timestamp", "generated_at", "executedAt", "observed_at"):
            nested = coerce_timestamp_value(value.get(key))
            if nested is not None:
                return nested
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric <= 0:
            return None
        if numeric >= 1_000_000_000_000:
            return datetime.fromtimestamp(numeric / 1000.0, tz=timezone.utc).isoformat()
        return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return coerce_timestamp_value(int(text))
    parsed = parse_dt(text)
    return parsed.isoformat() if parsed is not None else None


def age_sec(value: Any) -> float | None:
    parsed = parse_dt(value)
    if parsed is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def file_mtime(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
    except OSError:
        return None


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = Path(str(path) + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persistent Phoenix candidate feed producer wrapper")
    parser.add_argument("--output-dir", type=Path, default=Path("/opt/phoenix-testnet/signal_lab_runs/event_collect_v6_speed_boost"))
    parser.add_argument("--project-root", type=Path, default=Path("/opt/phoenix-testnet"))
    parser.add_argument("--python-bin", default="/opt/phoenix-testnet/.venv/bin/python")
    parser.add_argument("--env", default="prod", choices=["prod", "testnet", "demo"])
    parser.add_argument("--cycle-sec", type=int, default=45)
    parser.add_argument("--horizons-sec", default="60,180,300,900,1800,3600")
    parser.add_argument("--scan-top", type=int, default=120)
    parser.add_argument("--universe-top", type=int, default=200)
    parser.add_argument("--scan-step", type=int, default=60)
    parser.add_argument("--dedupe-sec", type=int, default=30)
    parser.add_argument("--symbol-cooldown-sec", type=int, default=60)
    parser.add_argument("--max-active-per-symbol", type=int, default=4)
    parser.add_argument("--ranked-samples-per-cycle", type=int, default=0)
    parser.add_argument("--ranked-min-trigger-ratio", type=float, default=0.75)
    parser.add_argument("--max-events-per-cycle", type=int, default=50)
    parser.add_argument("--worker-count", type=int, default=3)
    parser.add_argument("--kline-concurrency", type=int, default=5)
    parser.add_argument("--min-quote-volume", type=int, default=5_000_000)
    parser.add_argument("--trigger-min-quote-volume", type=int, default=5_000_000)
    parser.add_argument("--round-trip-fee-bps", type=int, default=8)
    parser.add_argument("--heartbeat-interval-sec", type=float, default=30.0)
    parser.add_argument("--stale-after-sec", type=int, default=60)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    service = CandidateProducerService(
        output_dir=args.output_dir,
        python_bin=args.python_bin,
        project_root=args.project_root,
        env=args.env,
        cycle_sec=args.cycle_sec,
        horizons_sec=args.horizons_sec,
        scan_top=args.scan_top,
        universe_top=args.universe_top,
        scan_step=args.scan_step,
        dedupe_sec=args.dedupe_sec,
        symbol_cooldown_sec=args.symbol_cooldown_sec,
        max_active_per_symbol=args.max_active_per_symbol,
        max_events_per_cycle=args.max_events_per_cycle,
        ranked_samples_per_cycle=args.ranked_samples_per_cycle,
        ranked_min_trigger_ratio=args.ranked_min_trigger_ratio,
        worker_count=args.worker_count,
        kline_concurrency=args.kline_concurrency,
        min_quote_volume=args.min_quote_volume,
        trigger_min_quote_volume=args.trigger_min_quote_volume,
        round_trip_fee_bps=args.round_trip_fee_bps,
        heartbeat_interval_sec=args.heartbeat_interval_sec,
        stale_after_sec=args.stale_after_sec,
    )
    return service.run_forever()


if __name__ == "__main__":
    raise SystemExit(main())
