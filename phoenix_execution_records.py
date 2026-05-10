from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from phoenix_research_diagnostics import (
    OUTCOME_EVENT_NAME,
    branch_id,
    normalized_text,
    read_jsonl_records,
    return_pct,
    round_optional,
    safe_float,
    source_event_id,
    strategy_id,
    time_value,
)


@dataclass(frozen=True, slots=True)
class ExecutionResearchRecord:
    event: str
    record_source: str
    source_event_id: str
    branch_id: str
    symbol: str
    side: str
    playbook: str
    strategy_id: str
    event_time: str
    exit_reason: str
    after_fee_and_slippage_return_pct: float | None
    net_pnl_usdt: float | None
    effective_quote_allocation_usdt: float | None
    max_runup_pct: float | None
    max_drawdown_pct: float | None
    market_state: str
    raw: dict[str, Any]

    def to_report_row(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.update(
            {
                "shadow_branch_id": self.branch_id,
                "after_real_cost_return_pct": self.after_fee_and_slippage_return_pct,
                "return_pct": self.after_fee_and_slippage_return_pct,
            }
        )
        if self.raw:
            for key in (
                "horizon_sec",
                "estimated_slippage_bps",
                "spread_bps_at_entry",
                "close_return_pct",
                "after_fee_return_pct",
                "realized_pnl_usdt",
                "commission_usdt",
                "latency_ms",
                "order_latency_ms",
                "partial_fill",
                "reject_reason",
            ):
                if key in self.raw and key not in payload:
                    payload[key] = self.raw.get(key)
        return payload


def _pct_from_testnet_trade(row: dict[str, Any]) -> float | None:
    for key in ("after_real_cost_return_pct", "after_fee_and_slippage_return_pct", "return_pct"):
        value = safe_float(row.get(key))
        if value is not None:
            return value
    net_pnl = safe_float(row.get("net_pnl_usdt"))
    allocation = safe_float(row.get("quote_allocation_usdt") or row.get("effective_quote_allocation_usdt"))
    if net_pnl is not None and allocation is not None and abs(allocation) > 1e-12:
        return net_pnl / allocation * 100.0
    return net_pnl


def normalize_shadow_outcome(row: dict[str, Any]) -> ExecutionResearchRecord:
    pct = return_pct(row)
    return ExecutionResearchRecord(
        event=OUTCOME_EVENT_NAME,
        record_source=normalized_text(row.get("record_source"), default="MAINNET_SHADOW"),
        source_event_id=source_event_id(row) or normalized_text(row.get("event_id")),
        branch_id=branch_id(row),
        symbol=normalized_text(row.get("symbol")).upper(),
        side=normalized_text(row.get("side")).upper(),
        playbook=normalized_text(row.get("playbook")),
        strategy_id=strategy_id(row),
        event_time=time_value(row),
        exit_reason=normalized_text(row.get("exit_reason") or row.get("final_exit_reason")),
        after_fee_and_slippage_return_pct=round_optional(pct),
        net_pnl_usdt=round_optional(safe_float(row.get("net_pnl_usdt"))),
        effective_quote_allocation_usdt=round_optional(safe_float(row.get("effective_quote_allocation_usdt"))),
        max_runup_pct=round_optional(safe_float(row.get("max_runup_pct") or row.get("max_favorable_return_pct"))),
        max_drawdown_pct=round_optional(safe_float(row.get("max_drawdown_pct") or row.get("max_adverse_return_pct"))),
        market_state=normalized_text(row.get("market_state")),
        raw=dict(row),
    )


def normalize_testnet_trade(row: dict[str, Any], *, fallback_source: str = "TESTNET_LIVE") -> ExecutionResearchRecord:
    round_no = normalized_text(row.get("round"), default="")
    entry_order_id = normalized_text(row.get("entry_order_id"), default="")
    source_id = normalized_text(
        row.get("source_event_id")
        or row.get("testnet_trade_id")
        or row.get("event_id")
        or f"testnet:{round_no}:{entry_order_id}:{row.get('symbol') or 'unknown'}",
    )
    pct = _pct_from_testnet_trade(row)
    branch = normalized_text(row.get("shadow_branch_id") or row.get("branch_id") or row.get("setup"), default="TESTNET_EXECUTION")
    return ExecutionResearchRecord(
        event=OUTCOME_EVENT_NAME,
        record_source=fallback_source,
        source_event_id=source_id,
        branch_id=branch,
        symbol=normalized_text(row.get("symbol")).upper(),
        side=normalized_text(row.get("side")).upper(),
        playbook=normalized_text(row.get("playbook") or row.get("setup")),
        strategy_id=normalized_text(row.get("strategy_id") or row.get("setup"), default="testnet_runner"),
        event_time=normalized_text(row.get("closed_at") or row.get("recorded_at") or row.get("entry_time") or row.get("time")),
        exit_reason=normalized_text(row.get("exit_reason") or row.get("final_exit_reason")),
        after_fee_and_slippage_return_pct=round_optional(pct),
        net_pnl_usdt=round_optional(safe_float(row.get("net_pnl_usdt"))),
        effective_quote_allocation_usdt=round_optional(safe_float(row.get("quote_allocation_usdt") or row.get("effective_quote_allocation_usdt"))),
        max_runup_pct=round_optional(safe_float(row.get("mfe_pct") or row.get("max_runup_pct"))),
        max_drawdown_pct=round_optional(safe_float(row.get("mae_pct") or row.get("max_drawdown_pct"))),
        market_state=normalized_text(row.get("market_state")),
        raw={**row, "record_source": fallback_source},
    )


def read_testnet_trade_rows(input_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if input_dir.is_file():
        return read_jsonl_records(input_dir)
    for path in sorted(input_dir.glob("round_*_trades.jsonl")):
        rows.extend(read_jsonl_records(path))
    for name in ("testnet_trades.jsonl", "testnet_execution_trades.jsonl"):
        path = input_dir / name
        if path.exists():
            rows.extend(read_jsonl_records(path))
    return rows


def load_testnet_research_outcomes(*, testnet_dir: Path | None = None, testnet_trades_file: Path | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if testnet_dir is not None:
        rows.extend(read_testnet_trade_rows(testnet_dir))
    if testnet_trades_file is not None:
        rows.extend(read_jsonl_records(testnet_trades_file))
    normalized = [normalize_testnet_trade(row).to_report_row() for row in rows if isinstance(row, dict)]
    return normalized


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
