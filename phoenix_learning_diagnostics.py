from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix_learning_analyzer import read_json, read_jsonl, read_round_trade_rows
from phoenix_trade_attribution import classify_execution_record, safe_float


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def build_learning_diagnostics(input_dir: Path, *, stage: str = "after", experiments_dir: Path | None = None) -> dict[str, Any]:
    trades = read_round_trade_rows(input_dir)
    learning = read_jsonl(input_dir / "learning_store.jsonl")
    experiments_root = experiments_dir or input_dir / "strategy_experiments"
    experiment_files = list(experiments_root.glob("*.json")) if experiments_root.exists() else []
    result_files = list((input_dir / "experiment_results").glob("*.json")) if (input_dir / "experiment_results").exists() else []
    registry = read_json(input_dir / "strategy_registry.json") or read_json(Path("strategy_registry.json")) or {}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in trades:
        grouped[(str(row.get("symbol") or "UNKNOWN"), str(row.get("setup") or row.get("strategy_id") or "unknown"))].append(row)
    by_symbol_setup = []
    for (symbol, setup), rows in grouped.items():
        pnls = [safe_float(row.get("net_pnl_usdt")) for row in rows]
        wins = [value for value in pnls if value > 0]
        by_symbol_setup.append(
            {
                "symbol": symbol,
                "setup": setup,
                "trade_count": len(rows),
                "win_rate_pct": round(len(wins) / len(rows) * 100.0, 6) if rows else 0.0,
                "net_pnl_usdt": round(sum(pnls), 6),
                "avg_fee_usdt": round(avg([safe_float(row.get("commission_usdt")) for row in rows]), 6),
                "avg_slippage_bps": round(avg([safe_float(row.get("estimated_slippage_bps")) for row in rows]), 6),
                "median_slippage_bps": round(median([safe_float(row.get("estimated_slippage_bps")) for row in rows]), 6),
                "exit_breakdown": dict(Counter(str(row.get("exit_reason") or "unknown") for row in rows)),
            }
        )
    by_symbol_setup.sort(key=lambda item: item["net_pnl_usdt"])
    annotated = [normalize_execution_record(row) for row in trades]
    loss_reason_counts = Counter(row.get("loss_reason") for row in annotated)
    tp_net_loss = [
        row
        for row in trades
        if "take_profit" in str(row.get("exit_reason") or "").lower() and safe_float(row.get("net_pnl_usdt")) <= 0
    ]
    return {
        "report_type": "mechanical_learning_diagnostics",
        "stage": stage,
        "generated_at": utc_now_iso(),
        "trade_count": len(trades),
        "learning_row_count": len(learning),
        "overall": {
            "win_rate_pct": round(sum(1 for row in trades if safe_float(row.get("net_pnl_usdt")) > 0) / len(trades) * 100.0, 6) if trades else 0.0,
            "net_pnl_usdt": round(sum(safe_float(row.get("net_pnl_usdt")) for row in trades), 6),
        },
        "by_symbol_setup": by_symbol_setup,
        "loss_reason_counts": dict(loss_reason_counts),
        "tp_net_loss_count": len(tp_net_loss),
        "tp_net_loss_samples": [
            {
                key: row.get(key)
                for key in (
                    "symbol",
                    "setup",
                    "strategy_id",
                    "experiment_id",
                    "entry_price",
                    "intended_take_profit_price",
                    "trigger_price",
                    "last_mark_price",
                    "actual_close_avg_price",
                    "close_price",
                    "commission_usdt",
                    "estimated_slippage_bps",
                    "gross_pnl_usdt",
                    "realized_pnl_usdt",
                    "net_pnl_usdt",
                    "exit_reason",
                )
            }
            for row in tp_net_loss[:50]
        ],
        "unknown_counts": {
            "learning_loss_reason_unknown": sum(1 for row in learning if str(row.get("loss_reason") or "unknown") == "unknown"),
            "trade_strategy_id_unknown": sum(1 for row in trades if str(row.get("strategy_id") or "unknown") == "unknown"),
            "trade_experiment_or_baseline_missing": sum(1 for row in trades if not row.get("experiment_id") and not row.get("baseline_id")),
            "normalized_strategy_id_unknown": sum(1 for row in annotated if str(row.get("strategy_id") or "unknown") == "unknown"),
            "normalized_experiment_or_baseline_missing": sum(1 for row in annotated if not row.get("experiment_id") and not row.get("baseline_id")),
        },
        "experiment_state": {
            "strategy_experiment_file_count": len(experiment_files),
            "experiment_result_file_count": len(result_files),
            "registry_strategy_count": len(registry.get("strategies", {})) if isinstance(registry, dict) else 0,
            "registry_experiment_count": len(registry.get("experiments", {})) if isinstance(registry, dict) else 0,
            "runner_trades_tagged_with_experiment": sum(1 for row in trades if row.get("experiment_id")),
            "runner_trades_tagged_with_baseline": sum(1 for row in trades if row.get("baseline_id")),
        },
    }


def normalize_execution_record(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    setup = str(normalized.get("setup") or normalized.get("strategy_id") or "unknown")
    if str(normalized.get("strategy_id") or "unknown") == "unknown" and setup != "unknown":
        normalized["strategy_id"] = setup
    if not normalized.get("experiment_id") and not normalized.get("baseline_id"):
        normalized["baseline_id"] = f"testnet_runner_v1:{setup}"
    attribution = classify_execution_record(normalized)
    normalized.update(attribution)
    normalized["original_exit_reason"] = normalized.get("original_exit_reason") or normalized.get("exit_reason")
    if attribution["loss_reason"] == "tp_net_loss" and "take_profit" in str(normalized.get("exit_reason") or "").lower():
        normalized["exit_reason"] = "fee_slippage_tp_fail"
    normalized["normalized_record_version"] = "v1.0"
    return normalized


def write_normalized_execution_records(input_dir: Path, output_path: Path | None = None) -> Path:
    output_path = output_path or input_dir / "normalized_execution_records.jsonl"
    rows = [normalize_execution_record(row) for row in read_round_trade_rows(input_dir)]
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    return output_path


def format_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix Mechanical Learning Diagnostics",
        "",
        f"- stage: {report.get('stage')}",
        f"- generated_at: {report.get('generated_at')}",
        f"- trade_count: {report.get('trade_count')}",
        f"- learning_row_count: {report.get('learning_row_count')}",
        f"- win_rate_pct: {report.get('overall', {}).get('win_rate_pct')}",
        f"- net_pnl_usdt: {report.get('overall', {}).get('net_pnl_usdt')}",
        f"- tp_net_loss_count: {report.get('tp_net_loss_count')}",
        f"- unknown_counts: {report.get('unknown_counts')}",
        "",
        "## Worst Symbol Setup",
    ]
    for row in (report.get("by_symbol_setup") or [])[:20]:
        lines.append(
            f"- {row['symbol']} / {row['setup']}: trades={row['trade_count']} "
            f"win={row['win_rate_pct']} net={row['net_pnl_usdt']} "
            f"avg_fee={row['avg_fee_usdt']} avg_slip={row['avg_slippage_bps']} exits={row['exit_breakdown']}"
        )
    lines.append("")
    return "\n".join(lines)


def write_learning_diagnostics(
    *,
    input_dir: Path,
    stage: str,
    output_json: Path | None = None,
    output_md: Path | None = None,
    experiments_dir: Path | None = None,
) -> dict[str, Any]:
    report = build_learning_diagnostics(input_dir, stage=stage, experiments_dir=experiments_dir)
    output_json = output_json or input_dir / f"mechanical_learning_{stage}_report.json"
    output_md = output_md or input_dir / f"mechanical_learning_{stage}_report.md"
    normalized_path = write_normalized_execution_records(input_dir)
    report["normalized_execution_records_path"] = str(normalized_path)
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(format_markdown(report), encoding="utf-8")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write Phoenix mechanical learning diagnostics reports.")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--stage", default="after", choices=["before", "after"])
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=None)
    parser.add_argument("--experiments-dir", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = write_learning_diagnostics(
        input_dir=args.input_dir,
        stage=args.stage,
        output_json=args.output_json,
        output_md=args.output_md,
        experiments_dir=args.experiments_dir,
    )
    print(
        "mechanical_learning_%s_report trades=%s tp_net_loss=%s"
        % (args.stage, report["trade_count"], report["tp_net_loss_count"])
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
