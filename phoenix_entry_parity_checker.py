from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ENTRY_PARITY_VERSION = "entry_parity_v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bool(payload: dict[str, Any], key: str, default: bool = False) -> bool:
    value = payload.get(key)
    if value is None:
        return default
    return bool(value)


def _number(payload: dict[str, Any], key: str) -> float | None:
    try:
        value = payload.get(key)
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def evaluate_entry_parity(payload: dict[str, Any]) -> dict[str, Any]:
    failures: list[dict[str, str]] = []

    feature_ts = _number(payload, "feature_timestamp_ms")
    entry_ts = _number(payload, "entry_timestamp_ms")
    if feature_ts is None or entry_ts is None:
        failures.append({"check": "feature_timestamp_before_entry", "reason": "missing_feature_or_entry_timestamp"})
    elif feature_ts > entry_ts:
        failures.append({"check": "feature_timestamp_before_entry", "reason": "feature_timestamp_after_entry_timestamp"})

    boolean_checks = [
        ("backtest_uses_close_as_entry", False, "backtest_uses_unfillable_close_price"),
        ("backtest_has_spread", True, "backtest_missing_spread"),
        ("backtest_has_slippage", True, "backtest_missing_slippage"),
        ("backtest_has_fee", True, "backtest_missing_fee"),
        ("backtest_has_funding", True, "backtest_missing_funding"),
        ("signal_delay_matches", True, "signal_delay_mismatch"),
        ("strategy_conditions_match", True, "strategy_condition_mismatch"),
        ("symbol_universe_matches", True, "symbol_universe_mismatch"),
        ("top200_survivor_bias_checked", True, "top200_survivor_bias_not_checked"),
        ("replay_shadow_entry_match", True, "replay_shadow_entry_mismatch"),
        ("shadow_testnet_entry_match", True, "shadow_testnet_entry_mismatch"),
    ]
    for key, expected, reason in boolean_checks:
        if _bool(payload, key, default=not expected) is not expected:
            failures.append({"check": key, "reason": reason})

    return {
        "report_type": "entry_parity_report",
        "version": ENTRY_PARITY_VERSION,
        "generated_at": utc_now_iso(),
        "strategy_id": payload.get("strategy_id"),
        "experiment_id": payload.get("experiment_id"),
        "passed": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "evidence_level_cap": None if not failures else 1,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }


def build_entry_parity_report(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    reports = [evaluate_entry_parity(row) for row in rows]
    failures = [failure for report in reports for failure in report["failures"]]
    affected = sorted({str(report.get("strategy_id") or "unknown") for report in reports if not report.get("passed")})
    return {
        "report_type": "entry_parity_batch_report",
        "version": ENTRY_PARITY_VERSION,
        "generated_at": utc_now_iso(),
        "checked_count": len(reports),
        "passed_count": sum(1 for report in reports if report.get("passed")),
        "failed_count": sum(1 for report in reports if not report.get("passed")),
        "affected_strategies": affected,
        "failures": failures,
        "reports": reports,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }


def write_entry_parity_outputs(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "entry_parity_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "parity_failures.jsonl").open("w", encoding="utf-8") as handle:
        for failure in report.get("failures") or []:
            handle.write(json.dumps(failure, ensure_ascii=False, sort_keys=True) + "\n")
    lines = ["# Affected Strategies", ""]
    for strategy_id in report.get("affected_strategies") or []:
        lines.append(f"- {strategy_id}")
    lines.append("")
    (output_dir / "affected_strategies.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Phoenix backtest/replay/shadow/testnet entry parity.")
    parser.add_argument("--input-json", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = json.loads(args.input_json.read_text(encoding="utf-8"))
    rows = payload if isinstance(payload, list) else [payload]
    report = build_entry_parity_report([row for row in rows if isinstance(row, dict)])
    write_entry_parity_outputs(report, args.output_dir)
    print(f"entry_parity failed={report['failed_count']} output_dir={args.output_dir}")
    return 0 if report["failed_count"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
