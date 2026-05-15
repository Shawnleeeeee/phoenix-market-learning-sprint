from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from phoenix_research_diagnostics import markdown_table, normalized_text, read_json, read_jsonl_records, round_optional, safe_float, write_json, write_text


REPORT_VERSION = "v1.0"
UNKNOWN = "unknown"

LATENCY_KEYS = (
    "latency_ms",
    "order_latency_ms",
    "entry_latency_ms",
    "close_latency_ms",
    "execution_latency_ms",
    "submit_to_fill_ms",
    "entry_submit_to_fill_ms",
    "close_submit_to_fill_ms",
)
SLIPPAGE_KEYS = (
    "estimated_slippage_bps",
    "slippage_bps",
    "entry_slippage_bps",
    "close_slippage_bps",
    "estimated_entry_slippage_bps",
    "estimated_close_slippage_bps",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a Phoenix testnet execution report from round runner outputs.")
    parser.add_argument("--input-dir", type=Path, required=True, help="Directory containing round_*_trades.jsonl and round_*_report.json files.")
    parser.add_argument("--shadow-report", type=Path, default=None, help="Optional research shadow report JSON for future promotion cross-validation.")
    return parser.parse_args()


def pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator * 100.0


def first_number(row: dict[str, Any], keys: Iterable[str]) -> float | None:
    for key in keys:
        value = safe_float(row.get(key))
        if value is not None:
            return value
    return None


def trade_net_pnl(row: dict[str, Any]) -> float | None:
    explicit = safe_float(row.get("net_pnl_usdt"))
    if explicit is not None:
        return explicit
    realized = safe_float(row.get("realized_pnl_usdt"))
    commission = safe_float(row.get("commission_usdt")) or 0.0
    if realized is not None:
        return realized - commission
    return safe_float(row.get("pnl_usdt"))


def trade_realized_pnl(row: dict[str, Any]) -> float | None:
    value = safe_float(row.get("realized_pnl_usdt"))
    if value is not None:
        return value
    net = safe_float(row.get("net_pnl_usdt"))
    commission = safe_float(row.get("commission_usdt"))
    if net is not None and commission is not None:
        return net + commission
    return safe_float(row.get("gross_realized_pnl_usdt"))


def trade_gross_pnl(row: dict[str, Any]) -> float | None:
    value = first_number(row, ("gross_pnl_usdt", "gross_realized_pnl_usdt"))
    if value is not None:
        return value
    return trade_realized_pnl(row)


def field_text(row: dict[str, Any], *keys: str, default: str = UNKNOWN) -> str:
    for key in keys:
        text = str(row.get(key) or "").strip()
        if text:
            return text
    return default


def is_partial_fill(row: dict[str, Any]) -> bool:
    for key in ("partial_fill", "is_partial_fill", "entry_partial_fill", "close_partial_fill"):
        value = row.get(key)
        if isinstance(value, bool):
            return value
        if str(value or "").strip().lower() in {"1", "true", "yes", "y"}:
            return True
    status_text = " ".join(str(row.get(key) or "") for key in ("status", "order_status", "fill_status", "entry_status", "close_status"))
    if "partial" in status_text.lower():
        return True
    filled = first_number(row, ("filled_quantity", "executed_qty", "executedQty"))
    quantity = first_number(row, ("quantity", "orig_qty", "origQty"))
    return filled is not None and quantity is not None and 0 < filled < quantity


def is_reject(row: dict[str, Any]) -> bool:
    if row.get("reject_reason") or row.get("rejection_reason") or row.get("error"):
        return True
    status_text = " ".join(str(row.get(key) or "") for key in ("status", "order_status", "fill_status", "execution_status"))
    normalized = status_text.lower()
    return "reject" in normalized or "expired" in normalized or "canceled" in normalized


def stats(values: Iterable[float]) -> dict[str, Any]:
    rows = sorted(float(value) for value in values)
    if not rows:
        return {"count": 0, "min": None, "avg": None, "median": None, "p95": None, "max": None}
    p95_index = min(len(rows) - 1, int((len(rows) - 1) * 0.95))
    return {
        "count": len(rows),
        "min": round_optional(rows[0]),
        "avg": round_optional(sum(rows) / len(rows)),
        "median": round_optional(statistics.median(rows)),
        "p95": round_optional(rows[p95_index]),
        "max": round_optional(rows[-1]),
    }


def max_drawdown(values: Iterable[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        cumulative += float(value)
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return drawdown


def metric_totals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    net_values = [trade_net_pnl(row) for row in rows]
    realized_values = [trade_realized_pnl(row) for row in rows]
    gross_values = [trade_gross_pnl(row) for row in rows]
    commission_values = [safe_float(row.get("commission_usdt")) for row in rows]
    valid_net = [value for value in net_values if value is not None]
    win_count = len([value for value in valid_net if value > 0])
    win_rate_pct = round_optional(pct(win_count, count))
    gross_pnl = round_optional(sum(value for value in gross_values if value is not None))
    commission = round_optional(sum(value for value in commission_values if value is not None))
    net_pnl = round_optional(sum(valid_net))
    realized_pnl = round_optional(sum(value for value in realized_values if value is not None))
    return {
        "trade_count": count,
        "win_count": win_count,
        "loss_count": len([value for value in valid_net if value < 0]),
        "win_rate": round_optional((win_count / count) if count else 0.0),
        "win_rate_pct": win_rate_pct,
        "gross_pnl": gross_pnl,
        "gross_pnl_usdt": gross_pnl,
        "commission": commission,
        "commission_usdt": commission,
        "net_pnl": net_pnl,
        "net_pnl_usdt": net_pnl,
        "realized_pnl": realized_pnl,
        "realized_pnl_usdt": realized_pnl,
    }


def build_breakdown(rows: list[dict[str, Any]], key_func) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[normalized_text(key_func(row))].append(row)
    payload = []
    for key, values in grouped.items():
        item = {"key": key, **metric_totals(values)}
        payload.append(item)
    payload.sort(key=lambda item: (-int(item["trade_count"]), str(item["key"])))
    return payload


def discover_inputs(input_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[Path], list[Path]]:
    trade_files = sorted(input_dir.glob("round_*_trades.jsonl"))
    report_files = sorted(input_dir.glob("round_*_report.json"))
    trades: list[dict[str, Any]] = []
    for path in trade_files:
        for index, row in enumerate(read_jsonl_records(path), start=1):
            row["_source_file"] = path.name
            row["_source_line"] = index
            trades.append(row)
    reports = [payload for path in report_files if isinstance((payload := read_json(path)), dict)]
    return trades, reports, trade_files, report_files


def build_promotion_cross_validation(shadow_report_path: Path | None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "not_provided",
        "shadow_report_path": None,
        "matched_bucket_count": None,
        "notes": [
            "Placeholder only: use this section to compare execution cohorts against a research shadow report before promotion.",
            "No strategy config changes are made by this report.",
        ],
    }
    if shadow_report_path is None:
        return payload
    payload["shadow_report_path"] = str(shadow_report_path)
    shadow = read_json(shadow_report_path)
    if not isinstance(shadow, dict):
        payload["status"] = "unreadable"
        return payload
    buckets = shadow.get("buckets")
    payload["status"] = "loaded"
    payload["matched_bucket_count"] = 0
    payload["shadow_bucket_count"] = len(buckets) if isinstance(buckets, list) else None
    payload["shadow_report_version"] = shadow.get("version")
    return payload


def sample_status(trades: list[dict[str, Any]], reports: list[dict[str, Any]], trade_files: list[Path], report_files: list[Path]) -> dict[str, Any]:
    required = ("symbol", "side", "net_pnl_usdt", "commission_usdt", "exit_reason")
    missing_counts = {key: sum(1 for row in trades if row.get(key) in (None, "")) for key in required}
    return {
        "status": "ready" if trades else "no_trades",
        "round_trade_file_count": len(trade_files),
        "round_report_file_count": len(report_files),
        "loaded_round_report_count": len(reports),
        "missing_field_counts": missing_counts,
        "trade_files": [path.name for path in trade_files],
        "report_files": [path.name for path in report_files],
    }


def round_report_summary(reports: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "round_count": len(reports),
        "reported_trade_count": round_optional(sum(safe_float(row.get("trade_count")) or 0.0 for row in reports)),
        "reported_net_pnl_usdt": round_optional(sum(safe_float(row.get("net_pnl_usdt")) or 0.0 for row in reports)),
        "reported_commission_usdt": round_optional(sum(safe_float(row.get("commission_usdt")) or 0.0 for row in reports)),
        "reported_balance_delta_usdt": round_optional(sum(safe_float(row.get("balance_delta_usdt")) or 0.0 for row in reports)),
        "rounds": [
            {
                "round": row.get("round"),
                "trade_count": row.get("trade_count"),
                "net_pnl_usdt": row.get("net_pnl_usdt"),
                "win_rate_pct": row.get("win_rate_pct"),
            }
            for row in reports
        ],
    }


def build_testnet_execution_report(input_dir: Path, *, shadow_report_path: Path | None = None) -> dict[str, Any]:
    trades, reports, trade_files, report_files = discover_inputs(input_dir)
    net_path = [trade_net_pnl(row) or 0.0 for row in trades]
    partial_count = sum(1 for row in trades if is_partial_fill(row))
    reject_count = sum(1 for row in trades if is_reject(row))
    latency_values = [value for row in trades for key in LATENCY_KEYS if (value := safe_float(row.get(key))) is not None]
    slippage_values = [value for row in trades for key in SLIPPAGE_KEYS if (value := safe_float(row.get(key))) is not None]
    totals = metric_totals(trades)
    totals.update(
        {
            "partial_fill_count": partial_count,
            "partial_fill_rate_pct": round_optional(pct(partial_count, len(trades))),
            "reject_count": reject_count,
            "reject_rate_pct": round_optional(pct(reject_count, len(trades))),
            "max_drawdown_usdt": round_optional(max_drawdown(net_path)),
        }
    )
    return {
        "version": REPORT_VERSION,
        "generated_at": now_iso(),
        "input_dir": str(input_dir),
        "sample_status": sample_status(trades, reports, trade_files, report_files),
        "round_report_summary": round_report_summary(reports),
        "metrics": totals,
        "latency_stats_ms": stats(latency_values),
        "slippage_estimate_stats_bps": stats(slippage_values),
        "breakdowns": {
            "symbol": build_breakdown(trades, lambda row: str(row.get("symbol") or "").upper()),
            "setup": build_breakdown(trades, lambda row: field_text(row, "setup", "strategy_id", "playbook")),
            "exit": build_breakdown(trades, lambda row: field_text(row, "exit_reason", "final_exit_reason")),
        },
        "promotion_cross_validation": build_promotion_cross_validation(shadow_report_path),
    }


def render_markdown(report: dict[str, Any]) -> str:
    metrics = report["metrics"]
    lines = [
        "# Phoenix Testnet Execution Report",
        "",
        f"- Input dir: `{report['input_dir']}`",
        f"- Generated at: `{report['generated_at']}`",
        f"- Sample status: `{report['sample_status']['status']}`",
        "",
        "## Summary",
        "",
        markdown_table(
            ["metric", "value"],
            [
                ["trade_count", metrics["trade_count"]],
                ["win_rate_pct", metrics["win_rate_pct"]],
                ["gross_pnl_usdt", metrics["gross_pnl_usdt"]],
                ["commission_usdt", metrics["commission_usdt"]],
                ["net_pnl_usdt", metrics["net_pnl_usdt"]],
                ["realized_pnl_usdt", metrics["realized_pnl_usdt"]],
                ["partial_fill_rate_pct", metrics["partial_fill_rate_pct"]],
                ["reject_rate_pct", metrics["reject_rate_pct"]],
                ["max_drawdown_usdt", metrics["max_drawdown_usdt"]],
            ],
        ),
        "",
        "## Latency And Slippage",
        "",
        markdown_table(
            ["series", "count", "avg", "median", "p95", "max"],
            [
                ["latency_ms", *[report["latency_stats_ms"][key] for key in ("count", "avg", "median", "p95", "max")]],
                ["slippage_bps", *[report["slippage_estimate_stats_bps"][key] for key in ("count", "avg", "median", "p95", "max")]],
            ],
        ),
    ]
    for label in ("symbol", "setup", "exit"):
        rows = report["breakdowns"][label][:20]
        lines.extend(
            [
                "",
                f"## {label.title()} Breakdown",
                "",
                markdown_table(
                    ["key", "trades", "win_rate_pct", "net_pnl_usdt", "commission_usdt"],
                    [[row["key"], row["trade_count"], row["win_rate_pct"], row["net_pnl_usdt"], row["commission_usdt"]] for row in rows],
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## Promotion Cross Validation",
            "",
            f"- Status: `{report['promotion_cross_validation']['status']}`",
            "- This report does not change strategy configs.",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    report = build_testnet_execution_report(input_dir, shadow_report_path=args.shadow_report)
    json_path = input_dir / "testnet_execution_report.json"
    md_path = input_dir / "testnet_execution_report.md"
    write_json(json_path, report)
    write_text(md_path, render_markdown(report))
    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    print(f"trade_count={report['metrics']['trade_count']} net_pnl_usdt={report['metrics']['net_pnl_usdt']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
