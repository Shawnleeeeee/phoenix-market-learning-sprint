from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix.direction_regime_matrix import direction_regime_fields, evaluate_direction_regime
from phoenix.entry_quality_filter import entry_quality_fields, evaluate_entry_quality
from phoenix.hermes_decision import validate_hermes_decision
from phoenix.hermes_file_bridge import (
    DEFAULT_HERMES_ARCHIVE,
    DEFAULT_HERMES_INBOX,
    DEFAULT_HERMES_LOGS,
    DEFAULT_HERMES_OUTBOX,
)
from phoenix.risk_governor import append_jsonl

WRITER_NAME = "Hermes Trader Brain"
SOURCE_NAME = "HERMES"
HEARTBEAT_FILE_NAME = "hermes_decision_loop_heartbeat.json"
EVENT_LOG_FILE_NAME = "hermes_decision_loop_events.jsonl"


@dataclass(slots=True)
class HermesDecisionLoopState:
    running: bool = True
    last_seen_snapshot_trace_id: str | None = None
    last_written_decision_trace_id: str | None = None
    last_success_at: str | None = None
    processed_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    last_error: str | None = None
    last_loop_at: str | None = None
    loop_lag_sec: float = 0.0
    pending_decision_count: int = 0
    oldest_pending_snapshot_age_sec: float | None = None
    last_pending_snapshot_trace_id: str | None = None
    restart_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "last_seen_snapshot_trace_id": self.last_seen_snapshot_trace_id,
            "last_written_decision_trace_id": self.last_written_decision_trace_id,
            "last_success_at": self.last_success_at,
            "processed_count": self.processed_count,
            "skipped_count": self.skipped_count,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "last_loop_at": self.last_loop_at,
            "loop_lag_sec": self.loop_lag_sec,
            "pending_decision_count": self.pending_decision_count,
            "oldest_pending_snapshot_age_sec": self.oldest_pending_snapshot_age_sec,
            "last_pending_snapshot_trace_id": self.last_pending_snapshot_trace_id,
            "restart_reason": self.restart_reason,
            "created_at": utc_now(),
        }


class HermesDecisionLoop:
    def __init__(
        self,
        *,
        inbox_dir: str | Path = DEFAULT_HERMES_INBOX,
        outbox_dir: str | Path = DEFAULT_HERMES_OUTBOX,
        log_dir: str | Path = DEFAULT_HERMES_LOGS,
        archive_dir: str | Path = DEFAULT_HERMES_ARCHIVE,
        heartbeat_interval_sec: float = 30.0,
        max_files_per_pass: int = 500,
        archive_processed_after_sec: float = 600.0,
        max_archive_per_pass: int = 250,
        watchdog_timeout_sec: float = 60.0,
        decision_policy: str = "conservative",
    ) -> None:
        self.inbox_dir = Path(inbox_dir)
        self.outbox_dir = Path(outbox_dir)
        self.log_dir = Path(log_dir)
        self.archive_dir = Path(archive_dir)
        self.heartbeat_interval_sec = max(1.0, float(heartbeat_interval_sec))
        self.max_files_per_pass = max(1, int(max_files_per_pass))
        self.archive_processed_after_sec = max(0.0, float(archive_processed_after_sec))
        self.max_archive_per_pass = max(0, int(max_archive_per_pass))
        self.watchdog_timeout_sec = max(0.0, float(watchdog_timeout_sec))
        self.decision_policy = normalize_decision_policy(decision_policy)
        self.state = HermesDecisionLoopState()
        self._last_heartbeat_monotonic = 0.0
        self._seen_existing_decisions: set[str] = set()

    @property
    def heartbeat_path(self) -> Path:
        return self.log_dir / HEARTBEAT_FILE_NAME

    @property
    def event_log_path(self) -> Path:
        return self.log_dir / EVENT_LOG_FILE_NAME

    def ensure_dirs(self) -> None:
        for directory in (self.inbox_dir, self.outbox_dir, self.log_dir, self.archive_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def run_forever(self, *, poll_interval_sec: float = 1.0) -> None:
        self.ensure_dirs()
        stop_requested = {"stop": False}

        def _request_stop(signum: int, _frame: Any) -> None:
            stop_requested["stop"] = True
            self.log_event("stop_requested", signal=signum)

        signal.signal(signal.SIGTERM, _request_stop)
        signal.signal(signal.SIGINT, _request_stop)
        self.write_heartbeat(force=True)
        while not stop_requested["stop"]:
            self.process_once()
            self.write_heartbeat()
            if self.should_restart_for_lag():
                self.state.restart_reason = f"decision_loop_lag_exceeded:{self.state.loop_lag_sec:.3f}s"
                self.state.last_error = self.state.restart_reason
                self.log_event(
                    "watchdog_restart_requested",
                    reason=self.state.restart_reason,
                    pending_decision_count=self.state.pending_decision_count,
                    oldest_pending_snapshot_age_sec=self.state.oldest_pending_snapshot_age_sec,
                    last_pending_snapshot_trace_id=self.state.last_pending_snapshot_trace_id,
                )
                self.write_heartbeat(force=True)
                raise SystemExit(75)
            time.sleep(max(0.2, float(poll_interval_sec)))
        self.state.running = False
        self.write_heartbeat(force=True)

    def process_once(self) -> dict[str, Any]:
        self.ensure_dirs()
        self.state.last_loop_at = utc_now()
        processed: list[dict[str, Any]] = []
        snapshot_paths = sorted(self.inbox_dir.glob("snapshot_*.json"), key=_path_sort_key)
        if snapshot_paths:
            newest_snapshot = max(snapshot_paths, key=_path_sort_key)
            self.state.last_seen_snapshot_trace_id = trace_id_from_snapshot_path(newest_snapshot)

        pending_paths = [
            path
            for path in snapshot_paths
            if not self.decision_path(trace_id_from_snapshot_path(path)).exists()
        ]
        existing_decision_count = len(snapshot_paths) - len(pending_paths)
        pending_paths = sorted(pending_paths, key=_path_sort_key, reverse=True)
        self._update_pending_metrics(pending_paths)

        for snapshot_path in pending_paths[: self.max_files_per_pass]:
            processed.append(self.process_snapshot_path(snapshot_path))
        archived = self.archive_processed_pairs()

        remaining_pending_paths = [
            path
            for path in self.inbox_dir.glob("snapshot_*.json")
            if not self.decision_path(trace_id_from_snapshot_path(path)).exists()
        ]
        self._update_pending_metrics(sorted(remaining_pending_paths, key=_path_sort_key, reverse=True))
        return {
            "processed_this_pass": sum(1 for item in processed if item.get("status") == "written"),
            "skipped_this_pass": existing_decision_count,
            "errors_this_pass": sum(1 for item in processed if item.get("status") == "error"),
            "archived_this_pass": archived,
            "pending_decision_count": self.state.pending_decision_count,
            "loop_lag_sec": self.state.loop_lag_sec,
            "items": processed,
        }

    def process_snapshot_path(self, snapshot_path: Path) -> dict[str, Any]:
        trace_id = trace_id_from_snapshot_path(snapshot_path)
        self.state.last_seen_snapshot_trace_id = trace_id
        decision_path = self.decision_path(trace_id)
        if decision_path.exists():
            if trace_id not in self._seen_existing_decisions:
                self.state.skipped_count += 1
                self._seen_existing_decisions.add(trace_id)
                self.log_event(
                    "decision_exists_skip",
                    trace_id=trace_id,
                    snapshot_path=str(snapshot_path),
                    decision_path=str(decision_path),
                )
                return {"status": "skipped_existing_decision", "trace_id": trace_id, "decision_path": str(decision_path)}
            return {"status": "skipped_seen_existing_decision", "trace_id": trace_id, "decision_path": str(decision_path)}
        try:
            snapshot = read_snapshot_json(snapshot_path)
            decision = build_decision(snapshot, trace_id=trace_id, policy=self.decision_policy)
            validation = validate_hermes_decision(decision)
            if not validation.valid:
                raise ValueError("generated_invalid_decision:" + ",".join(validation.reasons))
            write_json_atomic(decision_path, decision)
            self.state.processed_count += 1
            self.state.last_written_decision_trace_id = trace_id
            self.state.last_success_at = utc_now()
            self.state.last_error = None
            self.log_event(
                "decision_written",
                trace_id=trace_id,
                snapshot_path=str(snapshot_path),
                decision_path=str(decision_path),
                decision_policy=self.decision_policy,
                action=decision.get("action"),
                reason=decision.get("reason"),
            )
            return {
                "status": "written",
                "trace_id": trace_id,
                "decision_path": str(decision_path),
                "action": decision.get("action"),
            }
        except Exception as exc:  # noqa: BLE001
            message = str(exc)[:500]
            self.state.error_count += 1
            self.state.last_error = message
            self.log_event(
                "decision_error",
                trace_id=trace_id,
                snapshot_path=str(snapshot_path),
                decision_path=str(decision_path),
                error=message,
            )
            return {"status": "error", "trace_id": trace_id, "error": message}

    def archive_processed_pairs(self) -> int:
        if self.max_archive_per_pass <= 0:
            return 0
        archive_immediately = self.archive_processed_after_sec <= 0
        cutoff = time.time() - self.archive_processed_after_sec
        archived = 0
        for snapshot_path in sorted(self.inbox_dir.glob("snapshot_*.json"), key=_path_sort_key):
            if archived >= self.max_archive_per_pass:
                break
            if not archive_immediately and snapshot_path.stat().st_mtime > cutoff:
                continue
            trace_id = trace_id_from_snapshot_path(snapshot_path)
            decision_path = self.decision_path(trace_id)
            if not decision_path.exists() or (not archive_immediately and decision_path.stat().st_mtime > cutoff):
                continue
            archive_day = datetime.fromtimestamp(snapshot_path.stat().st_mtime, tz=timezone.utc).strftime("%Y%m%d")
            target_dir = self.archive_dir / "processed" / archive_day
            target_dir.mkdir(parents=True, exist_ok=True)
            snapshot_target = unique_target_path(target_dir / snapshot_path.name)
            decision_target = unique_target_path(target_dir / decision_path.name)
            shutil.move(str(snapshot_path), str(snapshot_target))
            shutil.move(str(decision_path), str(decision_target))
            archived += 1
            self.log_event(
                "processed_trace_archived",
                trace_id=trace_id,
                snapshot_archive_path=str(snapshot_target),
                decision_archive_path=str(decision_target),
            )
        return archived

    def should_restart_for_lag(self) -> bool:
        if self.watchdog_timeout_sec <= 0:
            return False
        age = self.state.oldest_pending_snapshot_age_sec
        return self.state.pending_decision_count > 0 and age is not None and age > self.watchdog_timeout_sec

    def _update_pending_metrics(self, pending_paths: list[Path]) -> None:
        self.state.pending_decision_count = len(pending_paths)
        if not pending_paths:
            self.state.loop_lag_sec = 0.0
            self.state.oldest_pending_snapshot_age_sec = None
            self.state.last_pending_snapshot_trace_id = None
            return
        now = time.time()
        oldest = min(pending_paths, key=_path_sort_key)
        newest = max(pending_paths, key=_path_sort_key)
        oldest_age = max(0.0, now - oldest.stat().st_mtime)
        self.state.loop_lag_sec = round(oldest_age, 3)
        self.state.oldest_pending_snapshot_age_sec = round(oldest_age, 3)
        self.state.last_pending_snapshot_trace_id = trace_id_from_snapshot_path(newest)

    def decision_path(self, trace_id: str) -> Path:
        return self.outbox_dir / f"decision_{trace_id}.json"

    def write_heartbeat(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_heartbeat_monotonic < self.heartbeat_interval_sec:
            return
        self.ensure_dirs()
        write_json_atomic(self.heartbeat_path, self.state.to_dict())
        self._last_heartbeat_monotonic = now

    def log_event(self, event: str, **payload: Any) -> None:
        self.ensure_dirs()
        append_jsonl(
            self.event_log_path,
            {
                "event": event,
                "created_at": utc_now(),
                **payload,
            },
        )


def normalize_decision_policy(policy: str | None) -> str:
    normalized = str(policy or "conservative").strip().lower().replace("-", "_")
    if normalized not in {"conservative", "calibration"}:
        raise ValueError(f"unsupported_decision_policy:{policy}")
    return normalized


def build_decision(snapshot: dict[str, Any], *, trace_id: str, policy: str = "conservative") -> dict[str, Any]:
    normalized = normalize_decision_policy(policy)
    if normalized == "calibration":
        return build_calibration_decision(snapshot, trace_id=trace_id)
    return build_conservative_decision(snapshot, trace_id=trace_id)


def build_conservative_decision(snapshot: dict[str, Any], *, trace_id: str) -> dict[str, Any]:
    status = snapshot.get("system_status") if isinstance(snapshot, dict) else {}
    status = status if isinstance(status, dict) else {}
    candidates = snapshot.get("top_candidates") if isinstance(snapshot, dict) else []
    candidates = candidates if isinstance(candidates, list) else []
    top_candidate = next((item for item in candidates if isinstance(item, dict)), None)

    safety_reasons = snapshot_safety_reasons(status)
    if safety_reasons:
        return base_decision(
            trace_id=trace_id,
            action="NO_TRADE",
            reason="snapshot safety not ready: " + ",".join(safety_reasons),
            confidence=1.0,
        )
    if top_candidate is None:
        candidate_state = status.get("candidate_state") or "empty"
        empty_reason = status.get("candidate_empty_reason") or "top_candidates empty"
        return base_decision(
            trace_id=trace_id,
            action="NO_TRADE",
            reason=f"无新鲜候选机会；candidate_state={candidate_state}; reason={empty_reason}",
            confidence=1.0,
        )

    symbol = str(top_candidate.get("symbol") or "").upper() or None
    bias = str(top_candidate.get("bias") or "NEUTRAL").upper()
    score = top_candidate.get("score")
    spread = top_candidate.get("spread_bps")
    liquidity_ok = top_candidate.get("liquidity_ok")
    return base_decision(
        trace_id=trace_id,
        action="WAIT_FOR_TRIGGER",
        symbol=symbol,
        reason=(
            "有候选但未触发入场；"
            f"symbol={symbol}; bias={bias}; score={score}; spread_bps={spread}; liquidity_ok={liquidity_ok}"
        ),
        confidence=0.65,
    )


def build_calibration_decision(snapshot: dict[str, Any], *, trace_id: str) -> dict[str, Any]:
    status = snapshot.get("system_status") if isinstance(snapshot, dict) else {}
    status = status if isinstance(status, dict) else {}
    candidates = snapshot.get("top_candidates") if isinstance(snapshot, dict) else []
    candidates = candidates if isinstance(candidates, list) else []
    top_candidate = next((item for item in candidates if isinstance(item, dict)), None)

    safety_reasons = snapshot_safety_reasons(status)
    if safety_reasons:
        return base_decision(
            trace_id=trace_id,
            action="NO_TRADE",
            reason="candidate 信息不足：snapshot safety not ready: " + ",".join(safety_reasons),
            confidence=1.0,
        )
    if top_candidate is None:
        candidate_state = status.get("candidate_state") or "empty"
        empty_reason = status.get("candidate_empty_reason") or "top_candidates empty"
        return base_decision(
            trace_id=trace_id,
            action="NO_TRADE",
            reason=f"candidate 信息不足：未有新鲜候选币；candidate_state={candidate_state}; reason={empty_reason}",
            confidence=1.0,
        )

    market_regime = snapshot.get("market_regime") if isinstance(snapshot.get("market_regime"), dict) else {}
    blocker = calibration_candidate_blocker(top_candidate, status, market_regime)
    if blocker is not None:
        if len(blocker) == 2:
            reason, confidence = blocker
            extras = {}
        else:
            reason, confidence, extras = blocker
        decision = base_decision(
            trace_id=trace_id,
            action="NO_TRADE",
            symbol=_candidate_symbol(top_candidate),
            reason=reason,
            confidence=confidence,
        )
        decision.update(extras)
        return decision

    symbol = _candidate_symbol(top_candidate)
    bias = _candidate_bias_value(top_candidate)
    action = "ENTER_LONG" if bias == "LONG" else "ENTER_SHORT"
    score = _optional_float(top_candidate.get("score")) or 0.0
    spread = _optional_float(top_candidate.get("spread_bps"))
    stop_pct = _optional_float(top_candidate.get("suggested_stop_pct"))
    tp_pct = _optional_float(top_candidate.get("suggested_tp_pct"))
    current_price = _optional_float(top_candidate.get("current_price"))
    max_holding = int(_optional_float(top_candidate.get("max_holding_time_sec")) or 0)
    invalidation = str(top_candidate.get("invalidation_hint") or top_candidate.get("invalidation_condition") or "").strip()
    side_text = "LONG" if action == "ENTER_LONG" else "SHORT"
    reason = (
        f"Stage 2 testnet exploration: candidate ready; {symbol} direction={side_text}; "
        f"score={score:g}; spread={spread:g} bps; stop={stop_pct:g}%; "
        f"take profit={tp_pct:g}%; risk reward acceptable for Binance testnet sandbox."
    )
    decision = base_decision(
        trace_id=trace_id,
        action=action,
        symbol=symbol,
        reason=reason,
        confidence=_calibration_confidence(score),
    )
    decision.update(
        {
            "side": "LONG" if action == "ENTER_LONG" else "SHORT",
            "trade_type": _trade_type_from_candidate(top_candidate),
            "entry_price_hint": current_price,
            "stop_loss_pct": stop_pct,
            "take_profit_pct": tp_pct,
            "max_holding_time_sec": max_holding,
            "invalidation_condition": invalidation,
        }
    )
    direction_result = evaluate_direction_regime(
        action=action,
        candidate_direction=bias,
        market_regime=market_regime,
        candidate=top_candidate,
    )
    decision.update(direction_regime_fields(direction_result))
    decision.update(entry_quality_fields(evaluate_entry_quality(top_candidate, market_regime, action=action)))
    return decision


def calibration_candidate_blocker(
    candidate: dict[str, Any],
    status: dict[str, Any],
    market_regime: dict[str, Any] | None = None,
) -> tuple[str, float] | tuple[str, float, dict[str, Any]] | None:
    symbol = _candidate_symbol(candidate)
    bias = _candidate_bias_value(candidate)
    score = _optional_float(candidate.get("score"))
    current_price = _optional_float(candidate.get("current_price"))
    spread = _optional_float(candidate.get("spread_bps"))
    slippage = _optional_float(candidate.get("estimated_slippage_bps"))
    stop_pct = _optional_float(candidate.get("suggested_stop_pct"))
    tp_pct = _optional_float(candidate.get("suggested_tp_pct"))
    max_holding = _optional_float(candidate.get("max_holding_time_sec"))
    invalidation = str(candidate.get("invalidation_hint") or candidate.get("invalidation_condition") or "").strip()
    liquidity_ok = candidate.get("liquidity_ok")

    if not symbol or symbol == "UNKNOWN":
        return "candidate 信息不足：缺少 symbol，暂时唔入场。", 1.0
    if bias not in {"LONG", "SHORT"}:
        return f"BTC / 大盘方向不支持：{symbol} 方向未清晰，暂时唔入场。", 1.0
    if score is None:
        return f"candidate 信息不足：{symbol} 缺少 score，暂时唔入场。", 1.0
    if not _score_passes(score):
        return f"score 不够：{symbol} score={score:g} 未过 calibration threshold，暂时唔入场。", 1.0
    if current_price is None or current_price <= 0:
        return f"candidate 信息不足：{symbol} 缺少有效 current_price，暂时唔入场。", 1.0
    if liquidity_ok is not True:
        return f"liquidity 不够：{symbol} liquidity_ok={liquidity_ok}，暂时唔入场。", 1.0
    if spread is None:
        return f"candidate 信息不足：{symbol} 缺少 spread，暂时唔入场。", 1.0
    if spread > 12.0:
        return f"spread 太大：{symbol} spread={spread:g} bps，暂时唔入场。", 1.0
    if slippage is None:
        return f"candidate 信息不足：{symbol} 缺少 estimated slippage，暂时唔入场。", 1.0
    if slippage > 8.0:
        return f"spread 太大：{symbol} slippage={slippage:g} bps 偏高，暂时唔入场。", 1.0
    if stop_pct is None or tp_pct is None or stop_pct <= 0 or tp_pct <= 0:
        return f"stop / tp 不合理：{symbol} stop 或 take profit 不完整，暂时唔入场。", 1.0
    if tp_pct / stop_pct < 1.2:
        return f"risk reward 不够：{symbol} take profit / stop loss 比例未够，暂时唔入场。", 1.0
    if max_holding is None or max_holding <= 0:
        return f"candidate 信息不足：{symbol} 缺少 max holding time，暂时唔入场。", 1.0
    if not invalidation or invalidation.lower() == "unavailable":
        return f"candidate 信息不足：{symbol} 缺少 invalidation 条件，暂时唔入场。", 1.0

    if candidate.get("exchange_filter_checked") is True and candidate.get("symbol_tradeable") is False:
        return f"exchange filter 拒绝：{symbol} 当前唔系 testnet 可交易 perpetual，唔入场。", 1.0
    if candidate.get("exchange_filter_checked") is True and candidate.get("micro_notional_feasible") is False:
        reason = str(candidate.get("infeasible_reason") or "micro_notional_infeasible")
        required = candidate.get("required_quote_allocation_usdt")
        configured = candidate.get("configured_quote_allocation_usdt")
        max_quote = candidate.get("max_quote_allocation_usdt")
        return (
            f"micro notional 唔可行：{symbol} reason={reason}; "
            f"configured_quote={configured}; required_quote={required}; max_quote={max_quote}，唔入场。",
            1.0,
        )

    market_regime = market_regime or {}
    if status.get("direction_lock") and not market_regime.get("direction_lock"):
        market_regime = {**market_regime, "direction_lock": status.get("direction_lock")}
    direction_result = evaluate_direction_regime(
        candidate_direction=bias,
        market_regime=market_regime,
        candidate=candidate,
    )
    if not direction_result.direction_regime_allowed:
        return (
            f"BTC / market regime block: {direction_result.direction_regime_reason} {symbol} no entry.",
            1.0,
            {
                **direction_regime_fields(direction_result),
                "blocked_by": direction_result.blocked_by,
            },
        )
    entry_quality = evaluate_entry_quality(candidate, market_regime, action="ENTER_LONG" if bias == "LONG" else "ENTER_SHORT")
    if not entry_quality.entry_quality_allowed:
        return (
            f"entry quality block: {entry_quality.entry_quality_reason}",
            1.0,
            {
                **entry_quality_fields(entry_quality),
                "blocked_by": entry_quality.blocked_by,
            },
        )
    regime = str(market_regime.get("regime") or "").upper()
    direction_lock = str(status.get("direction_lock") or market_regime.get("direction_lock") or "").upper()
    if regime in {"", "UNKNOWN"}:
        return f"BTC / 大盘方向未确认：market_regime={regime or 'UNKNOWN'}，{symbol} 暂时唔入场。", 1.0
    if regime == "HIGH_VOL" and direction_lock != "NO_TRADE":
        direction_lock = "NO_TRADE"
    if direction_lock == "NO_TRADE":
        return f"BTC / 大盘方向不支持：direction_lock=NO_TRADE，{symbol} 暂时唔入场。", 1.0
    if bias == "LONG" and direction_lock == "SHORT_ONLY_OR_NO_TRADE":
        return f"BTC / 大盘方向不支持：direction lock 只准做空，{symbol} 做多暂时唔入场。", 1.0
    if bias == "SHORT" and direction_lock == "LONG_ONLY_OR_NO_TRADE":
        return f"BTC / 大盘方向不支持：direction lock 只准做多，{symbol} 做空暂时唔入场。", 1.0
    return None


def snapshot_safety_reasons(status: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    source = str(status.get("snapshot_source") or status.get("source") or "").lower()
    if source != "runtime":
        reasons.append("snapshot_source_not_runtime")
    if status.get("trusted_runtime_snapshot") is not True:
        reasons.append("trusted_runtime_snapshot_false")
    if status.get("data_fresh") is not True:
        reasons.append("data_fresh_false")
    websocket_status = str(status.get("websocket_status") or "unknown").lower()
    if websocket_status not in {"healthy", "degraded_but_usable"}:
        reasons.append(f"websocket_status_{websocket_status}")
    exchange_status = str(status.get("exchange_status") or "unknown").lower()
    if exchange_status not in {"healthy", "degraded_but_usable"}:
        reasons.append(f"exchange_status_{exchange_status}")
    return reasons


def _candidate_symbol(candidate: dict[str, Any]) -> str | None:
    symbol = str(candidate.get("symbol") or "").strip().upper()
    return symbol or None


def _candidate_bias_value(candidate: dict[str, Any]) -> str:
    return str(candidate.get("bias") or candidate.get("side") or "NEUTRAL").strip().upper()


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.strip().lower() in {"unavailable", "unknown", "none", "nan"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _score_passes(score: float) -> bool:
    # Runtime candidates use two known scales: normalized 0-1 confidence and
    # signal-lab trigger scores around 10+. Treat both explicitly for Stage 2 exploration.
    threshold = 0.55 if score <= 1.0 else 10.0
    return score >= threshold


def _calibration_confidence(score: float) -> float:
    normalized_score = max(0.0, min(1.0, score if score <= 1.0 else score / 30.0))
    return round(max(0.55, min(0.9, 0.55 + normalized_score * 0.35)), 4)


def _trade_type_from_candidate(candidate: dict[str, Any]) -> str:
    setup_type = str(candidate.get("setup_type") or "QUICK_TRADE").strip().upper()
    if setup_type in {"QUICK_TRADE", "SWING_TRADE", "TREND_HOLD"}:
        return setup_type
    if setup_type == "TREND_HOLD_CANDIDATE":
        return "TREND_HOLD"
    return "QUICK_TRADE"


def base_decision(
    *,
    trace_id: str,
    action: str,
    reason: str,
    confidence: float,
    symbol: str | None = None,
) -> dict[str, Any]:
    created_at = utc_now()
    return {
        "trace_id": trace_id,
        "source": SOURCE_NAME,
        "writer": WRITER_NAME,
        "decision": action,
        "action": action,
        "symbol": symbol,
        "candidate_symbol": symbol,
        "trade_type": "NONE",
        "confidence": confidence,
        "reason": reason,
        "no_trade_reason": reason if action == "NO_TRADE" else None,
        "entry_price_hint": None,
        "stop_loss_pct": None,
        "stop_loss_price": None,
        "take_profit_pct": None,
        "take_profit_price": None,
        "max_holding_time_sec": None,
        "invalidation_condition": None,
        "reduce_only": False,
        "created_at": created_at,
        "timestamp": created_at,
    }


def read_snapshot_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("snapshot_payload_not_object")
    return payload


def trace_id_from_snapshot_path(path: Path) -> str:
    name = path.name
    if not name.startswith("snapshot_") or not name.endswith(".json"):
        raise ValueError(f"invalid_snapshot_filename:{name}")
    return name[len("snapshot_") : -len(".json")]


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _path_sort_key(path: Path) -> tuple[float, str]:
    return (path.stat().st_mtime, path.name)


def unique_target_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    for index in range(1, 10000):
        candidate = path.with_name(f"{stem}.{index}{suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"archive_target_collision:{path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run persistent Hermes file-bridge decision loop.")
    parser.add_argument("--inbox-dir", type=Path, default=DEFAULT_HERMES_INBOX)
    parser.add_argument("--outbox-dir", type=Path, default=DEFAULT_HERMES_OUTBOX)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_HERMES_LOGS)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_HERMES_ARCHIVE)
    parser.add_argument("--poll-interval-sec", type=float, default=1.0)
    parser.add_argument("--heartbeat-interval-sec", type=float, default=30.0)
    parser.add_argument("--max-files-per-pass", type=int, default=500)
    parser.add_argument("--archive-processed-after-sec", type=float, default=600.0)
    parser.add_argument("--max-archive-per-pass", type=int, default=250)
    parser.add_argument("--watchdog-timeout-sec", type=float, default=60.0)
    parser.add_argument("--decision-policy", choices=("conservative", "calibration"), default="conservative")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    loop = HermesDecisionLoop(
        inbox_dir=args.inbox_dir,
        outbox_dir=args.outbox_dir,
        log_dir=args.log_dir,
        archive_dir=args.archive_dir,
        heartbeat_interval_sec=args.heartbeat_interval_sec,
        max_files_per_pass=args.max_files_per_pass,
        archive_processed_after_sec=args.archive_processed_after_sec,
        max_archive_per_pass=args.max_archive_per_pass,
        watchdog_timeout_sec=args.watchdog_timeout_sec,
        decision_policy=args.decision_policy,
    )
    if args.once:
        result = loop.process_once()
        loop.write_heartbeat(force=True)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if result.get("errors_this_pass") == 0 else 1
    loop.run_forever(poll_interval_sec=args.poll_interval_sec)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
