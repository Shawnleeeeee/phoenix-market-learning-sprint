from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from phoenix_trade_attribution import classify_execution_record


ANALYZER_VERSION = "v1.0"
KNOWN_BAD_SYMBOL_SETUPS = {
    ("XRPUSDT", "trend_pullback_long"),
    ("UBUSDT", "volatility_long"),
    ("AIOTUSDT", "volatility_long"),
}
KNOWN_HIGH_SLIPPAGE_SYMBOLS = {"BTCDOMUSDT", "FXSUSDT"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    except (OSError, json.JSONDecodeError):
        return rows
    return rows


def read_round_trade_rows(input_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(input_dir.glob("round_*_trades.jsonl")):
        for row in read_jsonl(path):
            row.setdefault("source_file", str(path))
            rows.append(row)
    return rows


def entry_feature(row: dict[str, Any], key: str, default: Any = None) -> Any:
    if key in row:
        return row.get(key)
    features = row.get("entry_features")
    if isinstance(features, dict) and key in features:
        return features.get(key)
    return default


def side_is_long(row: dict[str, Any]) -> bool:
    return str(row.get("side") or "").upper() in {"BUY", "LONG"}


def infer_setup(row: dict[str, Any]) -> str:
    return str(row.get("setup") or row.get("strategy_id") or row.get("target_strategy") or "unknown")


def infer_symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or "UNKNOWN")


def infer_return(row: dict[str, Any]) -> float:
    if row.get("net_pnl_usdt") is not None:
        return safe_float(row.get("net_pnl_usdt"))
    for key in (
        "after_real_cost_return_pct",
        "after_fee_and_slippage_return_pct",
        "after_fee_return_pct",
        "return_pct",
        "close_return_pct",
    ):
        if row.get(key) is not None:
            return safe_float(row.get(key))
    return 0.0


def compute_trade_path_metrics(row: dict[str, Any]) -> dict[str, float]:
    if row.get("mfe_pct") is not None or row.get("mae_pct") is not None:
        mfe = safe_float(row.get("mfe_pct"))
        mae = safe_float(row.get("mae_pct"))
        ret = safe_float(row.get("return_pct"), infer_return(row))
        giveback = safe_float(row.get("profit_giveback_pct"), max(0.0, mfe - ret))
        return {"mfe_pct": mfe, "mae_pct": mae, "return_pct": ret, "profit_giveback_pct": giveback}
    entry = safe_float(row.get("entry_price"))
    close = safe_float(row.get("close_price"))
    best = safe_float(row.get("best_mark_price"), entry)
    worst = safe_float(row.get("worst_mark_price"), entry)
    if entry <= 0:
        return {"mfe_pct": 0.0, "mae_pct": 0.0, "return_pct": infer_return(row), "profit_giveback_pct": 0.0}
    if side_is_long(row):
        mfe = max(0.0, (best / entry - 1.0) * 100.0)
        mae = max(0.0, (entry / max(worst, 1e-12) - 1.0) * 100.0)
        ret = (close / entry - 1.0) * 100.0 if close > 0 else infer_return(row)
    else:
        mfe = max(0.0, (entry / max(worst, 1e-12) - 1.0) * 100.0)
        mae = max(0.0, (best / entry - 1.0) * 100.0)
        ret = (entry / max(close, 1e-12) - 1.0) * 100.0 if close > 0 else infer_return(row)
    return {
        "mfe_pct": round(mfe, 6),
        "mae_pct": round(mae, 6),
        "return_pct": round(ret, 6),
        "profit_giveback_pct": round(max(0.0, mfe - ret), 6),
    }


def classify_trade_problems(row: dict[str, Any]) -> list[str]:
    metrics = compute_trade_path_metrics(row)
    net = safe_float(row.get("net_pnl_usdt"), infer_return(row))
    gross = safe_float(row.get("realized_pnl_usdt"))
    commission = safe_float(row.get("commission_usdt"))
    exit_reason = str(row.get("exit_reason") or row.get("final_exit_reason") or "").lower()
    setup = infer_setup(row)
    problems: list[str] = []

    if net <= 0 and metrics["mfe_pct"] < 0.10 and exit_reason in {"", "horizon_close", "timeout", "time_stop", "stale_exit", "no_follow_through_exit"}:
        problems.append("no_follow_through")
    if net <= 0 and metrics["mae_pct"] >= max(0.20, metrics["mfe_pct"] * 2.0):
        problems.append("immediate_adverse_move")
    if net <= 0 and metrics["mfe_pct"] >= 0.20 and metrics["profit_giveback_pct"] >= 0.20:
        problems.append("gave_back_profit")
    if net <= 0 and (gross > 0 or (gross == 0 and commission > 0)):
        problems.append("cost_slippage_edge_destroyed")
        problems.append("fee_drag_exit")
    if "take_profit" in exit_reason and net <= 0:
        problems.append("take_profit_net_loss")
        problems.append("tp_net_loss")
    if net <= 0 and side_is_long(row):
        price_change_24h = safe_float(entry_feature(row, "price_change_24h_pct"))
        ret_1m = safe_float(entry_feature(row, "ret_1m_pct"))
        ret_3m = safe_float(entry_feature(row, "ret_3m_pct"))
        if price_change_24h >= 10.0 and min(ret_1m, ret_3m) <= 0:
            problems.append("hot_chase_failure")
    if net <= 0 and safe_float(row.get("estimated_slippage_bps")) >= 5.0:
        problems.append("high_slippage_failure")
    if net <= 0 and safe_float(row.get("estimated_slippage_bps")) >= 25.0:
        problems.append("high_slippage")
    if exit_reason in {"stale_exit", "time_stop", "time_decay_exit"}:
        problems.append("stale_or_time_decay")
    if row.get("bad_symbol_setup_combo") and net <= 0:
        problems.append("bad_symbol_setup_combo")
    if setup == "unknown" and net <= 0:
        problems.append("unknown")
    return sorted(set(problems))


def bucket_slippage(row: dict[str, Any]) -> str:
    value = safe_float(row.get("estimated_slippage_bps"))
    if value >= 10:
        return "slippage_ge_10bps"
    if value >= 5:
        return "slippage_5_10bps"
    if value > 0:
        return "slippage_lt_5bps"
    return "slippage_missing"


def bucket_fee_drag(row: dict[str, Any]) -> str:
    commission = abs(safe_float(row.get("commission_usdt")))
    gross = abs(safe_float(row.get("realized_pnl_usdt")))
    if commission <= 0:
        return "fee_missing"
    if gross <= 1e-12:
        return "fee_dominates_zero_gross"
    ratio = commission / gross
    if ratio >= 1:
        return "fee_ge_gross"
    if ratio >= 0.5:
        return "fee_50_100pct_gross"
    return "fee_lt_50pct_gross"


def summarize_group(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "unknown")].append(row)
    out: list[dict[str, Any]] = []
    for group_key, group_rows in grouped.items():
        returns = [infer_return(row) for row in group_rows]
        wins = [value for value in returns if value > 0]
        losses = [value for value in returns if value < 0]
        loss_count = len(losses)
        gains = sum(wins)
        loss_abs = abs(sum(losses))
        out.append(
            {
                "key": group_key,
                "sample_count": len(group_rows),
                "loss_count": loss_count,
                "win_rate": round((len(wins) / len(group_rows)) * 100.0, 6) if group_rows else 0.0,
                "avg_return": round(sum(returns) / len(returns), 6) if returns else 0.0,
                "profit_factor": round(gains / loss_abs, 6) if loss_abs > 0 else (999.0 if gains > 0 else 0.0),
                "problem_counts": dict(Counter(problem for row in group_rows for problem in row.get("problem_types", []))),
            }
        )
    return sorted(out, key=lambda item: (item["loss_count"], abs(item["avg_return"])), reverse=True)


def add_repeated_symbol_setup_losses(rows: list[dict[str, Any]], threshold: int = 3) -> None:
    streaks: dict[tuple[str, str], deque[bool]] = defaultdict(lambda: deque(maxlen=threshold))
    for row in rows:
        key = (infer_symbol(row), infer_setup(row))
        is_loss = infer_return(row) <= 0
        streaks[key].append(is_loss)
        if len(streaks[key]) >= threshold and all(streaks[key]):
            row.setdefault("problem_types", [])
            if "repeated_symbol_setup_loss" not in row["problem_types"]:
                row["problem_types"].append("repeated_symbol_setup_loss")
            if "bad_symbol_setup_combo" not in row["problem_types"]:
                row["problem_types"].append("bad_symbol_setup_combo")


def add_poor_state_fit(rows: list[dict[str, Any]], min_samples: int = 5) -> None:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        state = str(row.get("market_state") or row.get("hidden_state") or "unknown")
        grouped[(infer_setup(row), state)].append(row)
    bad_keys: set[tuple[str, str]] = set()
    for key, group_rows in grouped.items():
        if len(group_rows) < min_samples:
            continue
        avg = sum(infer_return(row) for row in group_rows) / len(group_rows)
        if avg < 0:
            bad_keys.add(key)
    for row in rows:
        key = (infer_setup(row), str(row.get("market_state") or row.get("hidden_state") or "unknown"))
        if key in bad_keys:
            row.setdefault("problem_types", [])
            if "poor_state_fit" not in row["problem_types"]:
                row["problem_types"].append("poor_state_fit")


def normalize_records(
    *,
    learning_rows: Iterable[dict[str, Any]] = (),
    trade_rows: Iterable[dict[str, Any]] = (),
    shadow_rows: Iterable[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for source, rows in (
        ("learning_store", learning_rows),
        ("testnet_trades", trade_rows),
        ("mainnet_shadow", shadow_rows),
    ):
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized = dict(row)
            normalized["analysis_source"] = source
            normalized["symbol"] = infer_symbol(normalized)
            normalized["setup"] = infer_setup(normalized)
            symbol_setup_key = (str(normalized["symbol"]).upper(), str(normalized["setup"]))
            if symbol_setup_key in KNOWN_BAD_SYMBOL_SETUPS:
                normalized["bad_symbol_setup_combo"] = True
            if str(normalized["symbol"]).upper() in KNOWN_HIGH_SLIPPAGE_SYMBOLS and safe_float(normalized.get("estimated_slippage_bps")) >= 5.0:
                normalized["bad_symbol_setup_combo"] = True
            metrics = compute_trade_path_metrics(normalized)
            for key, value in metrics.items():
                normalized.setdefault(key, value)
            attribution = classify_execution_record(normalized)
            normalized["loss_reason"] = attribution["loss_reason"]
            normalized["secondary_loss_reasons"] = attribution["secondary_loss_reasons"]
            normalized["all_loss_reasons"] = attribution["all_loss_reasons"]
            normalized["return_value"] = infer_return(normalized)
            normalized["slippage_bucket"] = bucket_slippage(normalized)
            normalized["fee_drag_bucket"] = bucket_fee_drag(normalized)
            normalized["problem_types"] = sorted(set(classify_trade_problems(normalized) + attribution["all_loss_reasons"]))
            if len(normalized["problem_types"]) > 1 and "unknown" in normalized["problem_types"]:
                normalized["problem_types"] = [problem for problem in normalized["problem_types"] if problem != "unknown"]
            records.append(normalized)
    add_repeated_symbol_setup_losses(records)
    add_poor_state_fit(records)
    for row in records:
        row["problem_types"] = sorted(set(row.get("problem_types", [])))
    return records


def build_learning_analysis_report(
    *,
    learning_rows: Iterable[dict[str, Any]] = (),
    trade_rows: Iterable[dict[str, Any]] = (),
    shadow_rows: Iterable[dict[str, Any]] = (),
    supplemental_reports: dict[str, Any] | None = None,
) -> dict[str, Any]:
    records = normalize_records(learning_rows=learning_rows, trade_rows=trade_rows, shadow_rows=shadow_rows)
    problem_counts = Counter(problem for row in records for problem in row.get("problem_types", []))
    problem_examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        for problem in row.get("problem_types", []):
            if len(problem_examples[problem]) >= 8:
                continue
            problem_examples[problem].append(
                {
                    "symbol": row.get("symbol"),
                    "setup": row.get("setup"),
                    "exit_reason": row.get("exit_reason"),
                    "net_pnl_usdt": row.get("net_pnl_usdt"),
                    "return_value": row.get("return_value"),
                    "mfe_pct": row.get("mfe_pct"),
                    "mae_pct": row.get("mae_pct"),
                    "estimated_slippage_bps": row.get("estimated_slippage_bps"),
                    "source_file": row.get("source_file"),
                }
            )
    by_symbol_setup = []
    grouped_symbol_setup: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        grouped_symbol_setup[(str(row["symbol"]), str(row["setup"]))].append(row)
    for (symbol, setup), group_rows in grouped_symbol_setup.items():
        returns = [infer_return(row) for row in group_rows]
        by_symbol_setup.append(
            {
                "symbol": symbol,
                "setup": setup,
                "sample_count": len(group_rows),
                "loss_count": sum(1 for value in returns if value <= 0),
                "avg_return": round(sum(returns) / len(returns), 6) if returns else 0.0,
                "avg_mfe_pct": round(sum(safe_float(row.get("mfe_pct")) for row in group_rows) / len(group_rows), 6),
                "problem_counts": dict(Counter(problem for row in group_rows for problem in row.get("problem_types", []))),
            }
        )
    by_symbol_setup.sort(key=lambda item: (item["loss_count"], -item["avg_return"]), reverse=True)
    supplemental = supplemental_reports or {}
    return {
        "report_type": "learning_analysis_report",
        "version": ANALYZER_VERSION,
        "generated_at": utc_now_iso(),
        "policy": {
            "agent_enabled": False,
            "direct_live_change_allowed": False,
            "mainnet_live_trading_enabled": False,
            "allowed_action_layer": ["research", "shadow", "testnet_candidate"],
        },
        "input_counts": {
            "records": len(records),
            "learning_rows": sum(1 for row in records if row.get("analysis_source") == "learning_store"),
            "testnet_trade_rows": sum(1 for row in records if row.get("analysis_source") == "testnet_trades"),
            "shadow_rows": sum(1 for row in records if row.get("analysis_source") == "mainnet_shadow"),
        },
        "problem_counts": dict(problem_counts),
        "problem_examples": dict(problem_examples),
        "by_setup": summarize_group(records, "setup"),
        "by_symbol": summarize_group(records, "symbol"),
        "by_exit_reason": summarize_group(records, "exit_reason"),
        "by_slippage_bucket": summarize_group(records, "slippage_bucket"),
        "by_fee_drag_bucket": summarize_group(records, "fee_drag_bucket"),
        "by_market_state": summarize_group(records, "market_state"),
        "by_symbol_setup": by_symbol_setup[:100],
        "records": [
            {
                "symbol": row.get("symbol"),
                "setup": row.get("setup"),
                "exit_reason": row.get("exit_reason"),
                "net_pnl_usdt": row.get("net_pnl_usdt"),
                "return_value": row.get("return_value"),
                "mfe_pct": row.get("mfe_pct"),
                "mae_pct": row.get("mae_pct"),
                "profit_giveback_pct": row.get("profit_giveback_pct"),
                "estimated_slippage_bps": row.get("estimated_slippage_bps"),
                "commission_usdt": row.get("commission_usdt"),
                "loss_reason": row.get("loss_reason"),
                "secondary_loss_reasons": row.get("secondary_loss_reasons", []),
                "problem_types": row.get("problem_types", []),
                "analysis_source": row.get("analysis_source"),
                "source_file": row.get("source_file"),
            }
            for row in records[:2000]
        ],
        "supplemental_reports_loaded": sorted(k for k, v in supplemental.items() if v),
        "supplemental_summaries": {
            "loss_attribution": (supplemental.get("loss_attribution") or {}).get("deduplication"),
            "monte_carlo": (supplemental.get("monte_carlo") or {}).get("overall_independent_event_summary"),
            "markov": (supplemental.get("markov") or {}).get("recommendations"),
            "hmm": {
                "hmm_available": (supplemental.get("hmm") or {}).get("hmm_available"),
                "trading_gate_enabled": (supplemental.get("hmm") or {}).get("trading_gate_enabled"),
                "position_manager_enabled": (supplemental.get("hmm") or {}).get("position_manager_enabled"),
            }
            if supplemental.get("hmm")
            else None,
        },
    }


def format_learning_analysis_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phoenix Learning Analysis Report",
        "",
        f"- generated_at: {report.get('generated_at')}",
        f"- records: {report.get('input_counts', {}).get('records', 0)}",
        f"- agent_enabled: {str(report.get('policy', {}).get('agent_enabled')).lower()}",
        f"- direct_live_change_allowed: {str(report.get('policy', {}).get('direct_live_change_allowed')).lower()}",
        "",
        "## Problem Counts",
    ]
    for key, value in sorted((report.get("problem_counts") or {}).items(), key=lambda item: item[1], reverse=True):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Worst Symbol + Setup"])
    for row in (report.get("by_symbol_setup") or [])[:12]:
        lines.append(
            f"- {row.get('symbol')} / {row.get('setup')}: losses={row.get('loss_count')} "
            f"avg={row.get('avg_return')} problems={row.get('problem_counts')}"
        )
    lines.append("")
    return "\n".join(lines)


def load_supplemental_reports(input_dir: Path, shadow_dir: Path | None = None) -> dict[str, Any]:
    roots = [input_dir]
    if shadow_dir is not None:
        roots.append(shadow_dir)
    names = {
        "loss_attribution": "loss_attribution_report.json",
        "monte_carlo": "monte_carlo_report.json",
        "markov": "markov_state_report.json",
        "hmm": "hmm_state_report.json",
    }
    reports: dict[str, Any] = {}
    for key, filename in names.items():
        for root in roots:
            data = read_json(root / filename)
            if data:
                reports[key] = data
                break
    return reports


def run_learning_analysis(
    *,
    input_dir: Path,
    shadow_dir: Path | None = None,
    learning_store_file: Path | None = None,
    output_json: Path | None = None,
    output_md: Path | None = None,
) -> dict[str, Any]:
    learning_path = learning_store_file or input_dir / "learning_store.jsonl"
    learning_rows = read_jsonl(learning_path)
    trade_rows = read_round_trade_rows(input_dir)
    shadow_rows: list[dict[str, Any]] = []
    if shadow_dir is not None:
        shadow_rows = read_jsonl(shadow_dir / "signal_bridge_shadow_outcomes.jsonl")
    report = build_learning_analysis_report(
        learning_rows=learning_rows,
        trade_rows=trade_rows,
        shadow_rows=shadow_rows,
        supplemental_reports=load_supplemental_reports(input_dir, shadow_dir),
    )
    output_json = output_json or input_dir / "learning_analysis_report.json"
    output_md = output_md or input_dir / "learning_analysis_report.md"
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(format_learning_analysis_markdown(report), encoding="utf-8")
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Phoenix learning data and classify mechanical failure modes.")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--shadow-dir", type=Path, default=None)
    parser.add_argument("--learning-store-file", type=Path, default=None)
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-md", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = run_learning_analysis(
        input_dir=args.input_dir,
        shadow_dir=args.shadow_dir,
        learning_store_file=args.learning_store_file,
        output_json=args.output_json,
        output_md=args.output_md,
    )
    print(
        "learning_analysis_report records=%s problems=%s output_json=%s"
        % (report["input_counts"]["records"], len(report["problem_counts"]), args.output_json or args.input_dir / "learning_analysis_report.json")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
