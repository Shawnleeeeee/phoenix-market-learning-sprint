#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phoenix_playbook_backtest import (
    build_promotion_backtest_report,
    render_console_summary,
    round4,
    safe_float,
    simulate_portfolio,
    summarize_trades,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            token = line.strip()
            if not token:
                continue
            try:
                payload = json.loads(token)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def trade_key(trade: dict[str, Any]) -> tuple[Any, ...]:
    return (
        str(trade.get("symbol") or "").upper(),
        int(trade.get("signal_time_ms") or 0),
        str(trade.get("playbook") or ""),
        str(trade.get("side") or ""),
        round(safe_float(trade.get("entry_price")), 12),
        round(safe_float(trade.get("exit_price")), 12),
    )


def unique_sorted_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for trade in trades:
        deduped[trade_key(trade)] = trade
    return sorted(deduped.values(), key=lambda item: (int(item.get("signal_time_ms") or 0), str(item.get("symbol") or "")))


def merge_config(source_reports: list[dict[str, Any]]) -> dict[str, Any]:
    base_config = dict(source_reports[0].get("config") or {}) if source_reports else {}
    selected_symbols: list[str] = []
    requested_symbols: list[str] = []
    for report in source_reports:
        config = report.get("config") if isinstance(report.get("config"), dict) else {}
        for symbol in config.get("selected_symbols") or []:
            normalized = str(symbol)
            if normalized and normalized not in selected_symbols:
                selected_symbols.append(normalized)
        for symbol in config.get("requested_symbols") or []:
            normalized = str(symbol)
            if normalized and normalized not in requested_symbols:
                requested_symbols.append(normalized)
    base_config["selected_symbols"] = selected_symbols
    base_config["requested_symbols"] = requested_symbols
    base_config["selected_symbol_count"] = len(selected_symbols)
    base_config["requested_symbol_count"] = len(requested_symbols)
    base_config["universe_top"] = max(len(selected_symbols), int(base_config.get("universe_top") or 0))
    base_config["merged_source_count"] = len(source_reports)
    return base_config


def build_merged_report(*, source_dirs: list[Path], output_dir: Path) -> dict[str, Any]:
    source_reports: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    source_summaries: list[dict[str, Any]] = []

    for source_dir in source_dirs:
        summary_path = source_dir / "playbook_backtest_report.json"
        trades_path = source_dir / "playbook_backtest_trades.jsonl"
        summary = read_json(summary_path)
        if not isinstance(summary, dict):
            source_summaries.append(
                {
                    "source_dir": str(source_dir),
                    "status": "missing_report",
                    "summary_path": str(summary_path),
                    "trades_path": str(trades_path),
                }
            )
            continue
        trades = iter_jsonl(trades_path)
        source_reports.append(summary)
        all_trades.extend(trades)
        source_summaries.append(
            {
                "source_dir": str(source_dir),
                "status": "loaded",
                "summary_path": str(summary_path),
                "trades_path": str(trades_path),
                "trade_count": len(trades),
                "selected_symbol_count": len((summary.get("config") or {}).get("selected_symbols") or []),
            }
        )

    merged_trades = unique_sorted_trades(all_trades)
    config = merge_config(source_reports)
    starting_equity = safe_float(config.get("starting_equity")) or 10_000.0
    quote_allocation = safe_float(config.get("quote_allocation")) or 10.0
    max_open_positions = int(config.get("portfolio_max_open_positions") or 50)
    raw_summary = summarize_trades(merged_trades, starting_equity=starting_equity)
    portfolio_summary = simulate_portfolio(
        merged_trades,
        starting_equity=starting_equity,
        margin_per_trade=quote_allocation,
        max_open_positions=max_open_positions,
    )
    seen_playbooks = sorted({str(trade.get("playbook") or "") for trade in merged_trades if str(trade.get("playbook") or "")})
    allowed_playbooks = {str(item) for item in config.get("allowed_playbooks") or [] if str(item)}
    report = {
        "generated_at": utc_now_iso(),
        "report_type": "phoenix_merged_playbook_backtest",
        "config": config,
        "raw_signal_summary": raw_summary,
        "portfolio_summary": portfolio_summary,
        "parameter_scan": {
            "status": "per_source_only",
            "reason": "Source reports do not persist per-combo trade paths, so the merged portfolio matrix cannot be reconstructed exactly without rerunning the grid.",
            "source_best_sharpe": [
                {
                    "source_dir": item["source_dir"],
                    "best_sharpe_combo": (summary.get("parameter_scan") or {}).get("best_sharpe_combo"),
                    "best_total_return_combo": (summary.get("parameter_scan") or {}).get("best_total_return_combo"),
                }
                for item, summary in zip(source_summaries, source_reports, strict=False)
                if item.get("status") == "loaded"
            ],
        },
        "seen_playbooks": seen_playbooks,
        "missing_allowed_playbooks": sorted(allowed_playbooks.difference(seen_playbooks)),
        "merge": {
            "source_dirs": [str(path) for path in source_dirs],
            "source_summaries": source_summaries,
            "input_trade_count": len(all_trades),
            "deduped_trade_count": len(merged_trades),
            "duplicate_trade_count": len(all_trades) - len(merged_trades),
        },
        "notes": [
            "Merged report recomputes base-parameter raw and portfolio metrics from per-run trade JSONL files.",
            "Merged parameter-scan ranking is marked per_source_only unless per-combo trades are available from a future run.",
            "This merge does not alter source reports and does not change historical sampling precision.",
        ],
    }
    report["portfolio_summary"]["total_return_pct"] = round4(report["portfolio_summary"].get("total_return_pct") or 0.0)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge Phoenix playbook backtest reports from exact parallel shards.")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-dir", action="append", type=Path, default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    source_dirs = [Path(item) for item in args.run_dir]
    report = build_merged_report(source_dirs=source_dirs, output_dir=output_dir)
    summary_path = output_dir / "playbook_backtest_report.json"
    trades_path = output_dir / "playbook_backtest_trades.jsonl"
    promotion_path = output_dir / "backtest_report.json"
    merged_trades = unique_sorted_trades(
        [
            trade
            for source_dir in source_dirs
            for trade in iter_jsonl(source_dir / "playbook_backtest_trades.jsonl")
        ]
    )
    summary_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with trades_path.open("w", encoding="utf-8") as handle:
        for trade in merged_trades:
            handle.write(json.dumps(trade, ensure_ascii=False) + "\n")
    promotion_report = build_promotion_backtest_report(report)
    promotion_report["source_report"] = str(summary_path)
    promotion_report["trades_file"] = str(trades_path)
    promotion_report["source"] = "phoenix_merge_backtest_reports"
    promotion_path.write_text(json.dumps(promotion_report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(render_console_summary(report))
    print(f"Merged report: {summary_path}")
    print(f"Promotion report: {promotion_path}")
    print(f"Merged trades: {trades_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
