from __future__ import annotations

from pathlib import Path
from typing import Any

from phoenix_research_diagnostics import OUTCOME_EVENT_NAME, read_jsonl_records, safe_float


EXECUTION_RECORDS_VERSION = "v1.0"


def _identity(row: dict[str, Any], *, source_name: str, index: int) -> str:
    for key in ("source_event_id", "trade_id", "id", "client_order_id", "entry_order_id", "order_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return f"{source_name}:{index}:{row.get('symbol') or 'unknown'}"


def _testnet_return_pct(row: dict[str, Any]) -> float | None:
    for key in ("after_real_cost_return_pct", "after_fee_and_slippage_return_pct", "return_pct"):
        value = safe_float(row.get(key))
        if value is not None:
            return value
    net = safe_float(row.get("net_pnl_usdt"))
    allocation = safe_float(row.get("effective_quote_allocation_usdt") or row.get("quote_allocation_usdt"))
    if net is not None and allocation is not None and abs(allocation) > 1e-12:
        return net / allocation * 100.0
    return net


def normalize_testnet_trade(row: dict[str, Any], *, source_name: str = "testnet_trades", index: int = 1) -> dict[str, Any]:
    source_id = str(row.get("source_event_id") or "").strip() or f"{source_name}:{_identity(row, source_name=source_name, index=index)}"
    strategy = row.get("strategy_id") or row.get("playbook") or row.get("setup") or "phoenix_testnet_round_runner"
    normalized = dict(row)
    normalized.update(
        {
            "event": OUTCOME_EVENT_NAME,
            "execution_records_version": EXECUTION_RECORDS_VERSION,
            "research_source": "TESTNET_LIVE",
            "_research_source": "TESTNET_LIVE",
            "execution_mode": "TESTNET_LIVE",
            "event_id": str(row.get("event_id") or f"{source_id}:TESTNET_LIVE"),
            "source_event_id": source_id,
            "shadow_branch_id": "TESTNET_LIVE",
            "branch_id": "TESTNET_LIVE",
            "strategy_id": str(strategy),
            "playbook": str(row.get("playbook") or row.get("setup") or strategy),
            "effective_quote_allocation_usdt": row.get("effective_quote_allocation_usdt") or row.get("quote_allocation_usdt"),
            "after_real_cost_return_pct": _testnet_return_pct(row),
            "final_exit_reason": row.get("final_exit_reason") or row.get("exit_reason") or row.get("close_reason"),
        }
    )
    return normalized


def discover_testnet_trade_files(testnet_dir: Path | None) -> list[Path]:
    if testnet_dir is None or not testnet_dir.exists():
        return []
    if testnet_dir.is_file():
        return [testnet_dir]
    files = sorted(testnet_dir.glob("round_*_trades.jsonl"))
    if not files:
        files = sorted(testnet_dir.glob("*trades*.jsonl"))
    return [path for path in files if path.is_file()]


def load_testnet_research_outcomes(
    *,
    testnet_dir: Path | None = None,
    trades_file: Path | None = None,
) -> list[dict[str, Any]]:
    files = [trades_file] if trades_file is not None else discover_testnet_trade_files(testnet_dir)
    rows: list[dict[str, Any]] = []
    for path in files:
        if path is None:
            continue
        for index, row in enumerate(read_jsonl_records(path), start=1):
            if str(row.get("event") or "").strip() == "trade_skipped":
                continue
            rows.append(normalize_testnet_trade(row, source_name=path.stem, index=index))
    return rows
