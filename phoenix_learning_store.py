from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


LEARNING_STORE_VERSION = "v1.0"
SUPPORTED_ROW_MODES = {"shadow", "testnet"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_row_mode(row: dict[str, Any]) -> str:
    raw_mode = row.get("mode") or row.get("run_mode") or row.get("environment") or row.get("venue_mode")
    mode = str(raw_mode or "shadow").strip().lower()
    if mode in {"mainnet_shadow", "research_shadow", "paper"}:
        return "shadow"
    if mode in {"test_net", "test-net"}:
        return "testnet"
    if mode not in SUPPORTED_ROW_MODES:
        return "shadow"
    return mode


def normalize_learning_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["event"] = str(normalized.get("event") or "phoenix_learning_row")
    normalized["recorded_at"] = str(normalized.get("recorded_at") or utc_now_iso())
    normalized["mode"] = normalize_row_mode(normalized)
    normalized["learning_store_version"] = LEARNING_STORE_VERSION
    normalized["live_trading_enabled"] = False
    normalized["direct_config_change_allowed"] = False
    if normalized.get("strategy_proposals") is None:
        normalized["strategy_proposals"] = []
    normalized["strategy_proposals"] = normalize_strategy_proposals(normalized["strategy_proposals"])
    return normalized


def normalize_strategy_proposals(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    proposals: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        proposal = dict(item)
        proposal["type"] = "suggestion"
        proposal["suggestion_only"] = True
        proposal["direct_config_change_allowed"] = False
        proposal.pop("write_config", None)
        proposal.pop("apply_live", None)
        proposals.append(proposal)
    return proposals


def append_learning_row(jsonl_path: Path, row: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_learning_row(row)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(normalized, ensure_ascii=False, sort_keys=True) + "\n")
    return normalized


def read_learning_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(normalize_learning_row(payload))
    return rows


def _metric_average(rows: Iterable[dict[str, Any]], key: str) -> float | None:
    values = [safe_float(row.get(key)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 6)


def build_daily_learning_report(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    normalized_rows = [normalize_learning_row(row) for row in rows if isinstance(row, dict)]
    mode_counts = Counter(row["mode"] for row in normalized_rows)
    suggestion_proposals: list[dict[str, Any]] = []
    for row in normalized_rows:
        suggestion_proposals.extend(normalize_strategy_proposals(row.get("strategy_proposals")))
    return {
        "generated_at": utc_now_iso(),
        "learning_store_version": LEARNING_STORE_VERSION,
        "row_count": len(normalized_rows),
        "supported_modes": sorted(SUPPORTED_ROW_MODES),
        "mode_counts": dict(sorted(mode_counts.items())),
        "shadow_rows": mode_counts.get("shadow", 0),
        "testnet_rows": mode_counts.get("testnet", 0),
        "avg_return_pct": _metric_average(normalized_rows, "return_pct"),
        "avg_mfe_pct": _metric_average(normalized_rows, "mfe_pct"),
        "avg_mae_pct": _metric_average(normalized_rows, "mae_pct"),
        "strategy_proposals": suggestion_proposals,
        "direct_config_change_allowed": False,
        "live_trading_enabled": False,
    }


def write_daily_learning_report(
    *,
    rows: Iterable[dict[str, Any]],
    output_json: Path,
    output_md: Path | None = None,
) -> dict[str, Any]:
    report = build_daily_learning_report(rows)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if output_md is not None:
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(format_daily_learning_markdown(report), encoding="utf-8")
    return report


def format_daily_learning_markdown(report: dict[str, Any]) -> str:
    mode_counts = report.get("mode_counts") if isinstance(report.get("mode_counts"), dict) else {}
    lines = [
        "# Phoenix Daily Learning Report",
        "",
        f"- row_count: {int(report.get('row_count') or 0)}",
        f"- shadow_rows: {int(mode_counts.get('shadow') or 0)}",
        f"- testnet_rows: {int(mode_counts.get('testnet') or 0)}",
        f"- direct_config_change_allowed: {str(bool(report.get('direct_config_change_allowed'))).lower()}",
        f"- strategy_proposals: suggestion_only",
        "",
    ]
    return "\n".join(lines)
