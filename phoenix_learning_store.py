from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from phoenix_execution_records import normalize_shadow_outcome, normalize_testnet_trade
from phoenix_position_manager import summarize_trade_path_from_record
from phoenix_research_diagnostics import read_jsonl_records, return_pct, round_optional, safe_float, write_json, write_text


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class LearningStore:
    path: Path

    def append(self, event: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(event)
        payload.setdefault("recorded_at", now_iso())
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")

    def read(self, *, max_records: int = 0) -> list[dict[str, Any]]:
        return read_jsonl_records(self.path, max_records=max_records)


def build_learning_event(
    row: dict[str, Any],
    *,
    source: str,
    dynamic_exit: dict[str, Any] | None = None,
    loss_reason: str | None = None,
) -> dict[str, Any]:
    if source == "TESTNET_LIVE":
        record = normalize_testnet_trade(row)
        path_metrics = summarize_trade_path_from_record(row)
    else:
        record = normalize_shadow_outcome(row)
        path_metrics = {
            "mfe_pct": safe_float(row.get("max_runup_pct")),
            "mae_pct": abs(safe_float(row.get("max_drawdown_pct")) or 0.0),
            "profit_giveback_pct": None,
            "final_return_pct": return_pct(row),
        }
    payload = {
        "event": "phoenix_learning_event",
        "source": source,
        "source_event_id": record.source_event_id,
        "branch_id": record.branch_id,
        "symbol": record.symbol,
        "side": record.side,
        "strategy_id": record.strategy_id,
        "strategy_version": row.get("strategy_version") or row.get("config_version") or "unknown",
        "config_version": row.get("config_version") or "unknown",
        "playbook": record.playbook,
        "entry_reason": row.get("entry_reason") or row.get("setup") or row.get("playbook"),
        "entry_features": row.get("entry_features") or row.get("trigger_features") or row.get("sample") or {},
        "market_state": record.market_state,
        "exit_reason": record.exit_reason,
        "loss_reason": loss_reason or row.get("loss_reason") or "unknown",
        "cost_impact": {
            "commission_usdt": row.get("commission_usdt"),
            "estimated_slippage_bps": row.get("estimated_slippage_bps"),
            "spread_bps_at_entry": row.get("spread_bps_at_entry"),
        },
        "outcome": {
            "after_real_cost_return_pct": record.after_fee_and_slippage_return_pct,
            "net_pnl_usdt": record.net_pnl_usdt,
            **path_metrics,
        },
        "dynamic_exit_simulation": dynamic_exit or row.get("dynamic_exit_simulation") or {},
    }
    payload["dynamic_exit_would_improve"] = _dynamic_exit_would_improve(payload)
    return payload


def _dynamic_exit_would_improve(payload: dict[str, Any]) -> bool | None:
    sim = payload.get("dynamic_exit_simulation")
    outcome = payload.get("outcome")
    if not isinstance(sim, dict) or not isinstance(outcome, dict):
        return None
    sim_return = safe_float(sim.get("final_return_pct"))
    actual = safe_float(outcome.get("after_real_cost_return_pct") or outcome.get("final_return_pct"))
    if sim_return is None or actual is None:
        return None
    return sim_return > actual


def build_daily_learning_report(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(events)
    by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_loss_reason: Counter[str] = Counter()
    proposals: list[dict[str, Any]] = []
    for row in rows:
        strategy = str(row.get("strategy_id") or "unknown")
        by_strategy[strategy].append(row)
        reason = str(row.get("loss_reason") or "unknown")
        if reason != "unknown":
            by_loss_reason[reason] += 1

    strategy_rows = []
    for strategy, values in by_strategy.items():
        returns = [
            safe_float(((row.get("outcome") or {}) if isinstance(row.get("outcome"), dict) else {}).get("after_real_cost_return_pct"))
            for row in values
        ]
        clean_returns = [value for value in returns if value is not None]
        improved = sum(1 for row in values if row.get("dynamic_exit_would_improve") is True)
        losses = sum(1 for value in clean_returns if value < 0)
        avg_return = sum(clean_returns) / len(clean_returns) if clean_returns else None
        strategy_rows.append(
            {
                "strategy_id": strategy,
                "sample_count": len(values),
                "loss_count": losses,
                "avg_after_real_cost_return_pct": round_optional(avg_return),
                "dynamic_exit_improvement_count": improved,
            }
        )
        if improved >= max(2, len(values) // 4):
            proposals.append(
                {
                    "strategy_id": strategy,
                    "proposal_type": "dynamic_exit_tuning",
                    "reason": "dynamic exit simulation improved multiple observed trades",
                    "sample_count": len(values),
                    "improvement_count": improved,
                    "direct_config_change_allowed": False,
                }
            )

    strategy_rows.sort(key=lambda item: (item["avg_after_real_cost_return_pct"] is None, item["avg_after_real_cost_return_pct"] or 0.0))
    return {
        "report_type": "daily_learning_report",
        "generated_at": now_iso(),
        "event_count": len(rows),
        "strategy_summary": strategy_rows,
        "top_loss_reasons": [{"loss_reason": key, "count": value} for key, value in by_loss_reason.most_common(10)],
        "strategy_proposals": proposals,
        "promotion_allowed": False,
        "live_trading_enabled": False,
    }


def render_daily_learning_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix Daily Learning Report",
        "",
        f"- Generated: `{report.get('generated_at')}`",
        f"- Events: `{report.get('event_count', 0)}`",
        "- Promotion allowed: `false`",
        "- Live trading enabled: `false`",
        "",
        "## Strategy Summary",
    ]
    for row in report.get("strategy_summary", []):
        lines.append(
            f"- `{row.get('strategy_id')}` samples={row.get('sample_count')} "
            f"avg={row.get('avg_after_real_cost_return_pct')} "
            f"dynamic_exit_improvements={row.get('dynamic_exit_improvement_count')}"
        )
    lines.append("")
    lines.append("## Proposals")
    proposals = report.get("strategy_proposals") or []
    if not proposals:
        lines.append("- No config change proposals. Keep collecting samples.")
    for proposal in proposals:
        lines.append(
            f"- `{proposal.get('strategy_id')}`: {proposal.get('proposal_type')} "
            f"({proposal.get('reason')}); direct_config_change_allowed=false"
        )
    return "\n".join(lines)


def write_daily_learning_outputs(store_path: Path, *, output_json: Path, output_md: Path, proposals_dir: Path) -> dict[str, Any]:
    events = read_jsonl_records(store_path)
    report = build_daily_learning_report(events)
    write_json(output_json, report)
    write_text(output_md, render_daily_learning_markdown(report))
    proposals_dir.mkdir(parents=True, exist_ok=True)
    for index, proposal in enumerate(report.get("strategy_proposals") or [], start=1):
        proposal_path = proposals_dir / f"proposal_{index:03d}_{proposal.get('strategy_id', 'unknown')}.json"
        write_json(proposal_path, proposal)
    return report
