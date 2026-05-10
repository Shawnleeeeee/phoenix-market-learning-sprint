"""Dataset builder for Leiting BTC research and training."""

from __future__ import annotations

import argparse
import csv
import json
from bisect import bisect_right
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btc_engine.config import DATASETS_DIR, DATA_DIR, RESEARCH_RUNTIME_DIR, ensure_runtime_dirs

from .features import baseline_signal_score, engineer_feature_rows
from .labels import attach_direction_labels


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _load_raw_tables(raw_dir: Path) -> dict[str, list[dict[str, Any]]]:
    files = {
        "klines": raw_dir / "klines.csv",
        "premium": raw_dir / "premium_index_klines.csv",
        "funding": raw_dir / "funding_rate_history.csv",
        "oi": raw_dir / "open_interest_hist_5m.csv",
        "taker": raw_dir / "taker_buy_sell_ratio_5m.csv",
        "klines_1m": raw_dir / "klines_1m_recent.csv",
        "global_ratio": raw_dir / "global_long_short_ratio_5m.csv",
        "runtime_micro": RESEARCH_RUNTIME_DIR / "market_micro_snapshots.jsonl",
    }
    required = {k: v for k, v in files.items() if k not in {"klines_1m", "global_ratio"}}
    missing = [str(path) for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing raw BTC dataset files: {missing}")
    loaded: dict[str, list[dict[str, Any]]] = {}
    for name, path in files.items():
        if name == "runtime_micro":
            loaded[name] = _read_jsonl(path)
        else:
            loaded[name] = _read_csv(path) if path.exists() else []
    return loaded


def _index_by_time(rows: list[dict[str, Any]], key: str) -> tuple[list[int], list[dict[str, Any]]]:
    keyed: list[tuple[int, dict[str, Any]]] = []
    for row in rows:
        try:
            ts = int(float(row[key]))
        except (KeyError, TypeError, ValueError):
            continue
        keyed.append((ts, row))
    keyed.sort(key=lambda item: item[0])
    return [ts for ts, _ in keyed], [row for _, row in keyed]


def _at_or_before(index_times: list[int], indexed_rows: list[dict[str, Any]], target_ts: int) -> dict[str, Any] | None:
    if not index_times:
        return None
    pos = bisect_right(index_times, target_ts) - 1
    if pos < 0:
        return None
    return indexed_rows[pos]


def _window_rows(index_times: list[int], indexed_rows: list[dict[str, Any]], start_ts: int, end_ts: int) -> list[dict[str, Any]]:
    if not index_times:
        return []
    start_pos = bisect_right(index_times, start_ts - 1)
    end_pos = bisect_right(index_times, end_ts)
    if start_pos < 0 or end_pos <= start_pos:
        return []
    return indexed_rows[start_pos:end_pos]


def _parse_iso_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return None


def _index_runtime_rows(rows: list[dict[str, Any]]) -> tuple[list[int], list[dict[str, Any]]]:
    keyed: list[tuple[int, dict[str, Any]]] = []
    for row in rows:
        ts = _parse_iso_ms(row.get("generated_at"))
        if ts is None:
            continue
        keyed.append((ts, row))
    keyed.sort(key=lambda item: item[0])
    return [ts for ts, _ in keyed], [row for _, row in keyed]


def _merge_context(raw: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    premium_map = {int(float(row["open_time"])): row for row in raw["premium"] if row.get("open_time")}
    funding_times, funding_rows = _index_by_time(raw["funding"], "fundingTime")
    oi_times, oi_rows = _index_by_time(raw["oi"], "timestamp")
    taker_times, taker_rows = _index_by_time(raw["taker"], "timestamp")
    micro_times, micro_rows = _index_by_time(raw.get("klines_1m", []), "open_time")
    global_ratio_times, global_ratio_rows = _index_by_time(raw.get("global_ratio", []), "timestamp")
    runtime_times, runtime_rows = _index_runtime_rows(raw.get("runtime_micro", []))

    merged: list[dict[str, Any]] = []
    for row in raw["klines"]:
        open_time = int(float(row["open_time"]))
        close_time = int(float(row["close_time"]))
        premium = premium_map.get(open_time)
        funding = _at_or_before(funding_times, funding_rows, open_time)
        oi = _at_or_before(oi_times, oi_rows, open_time)
        taker = _at_or_before(taker_times, taker_rows, open_time)
        global_ratio = _at_or_before(global_ratio_times, global_ratio_rows, open_time)
        micro_window = _window_rows(micro_times, micro_rows, open_time, close_time)
        runtime_window = _window_rows(runtime_times, runtime_rows, open_time, close_time)
        oi_timestamp = int(float(oi["timestamp"])) if oi and oi.get("timestamp") not in (None, "") else None
        taker_timestamp = int(float(taker["timestamp"])) if taker and taker.get("timestamp") not in (None, "") else None
        global_ratio_timestamp = int(float(global_ratio["timestamp"])) if global_ratio and global_ratio.get("timestamp") not in (None, "") else None
        bar_ms = 5 * 60 * 1000
        oi_age_bars = int((open_time - oi_timestamp) // bar_ms) if oi_timestamp is not None and open_time >= oi_timestamp else None
        taker_age_bars = int((open_time - taker_timestamp) // bar_ms) if taker_timestamp is not None and open_time >= taker_timestamp else None
        global_ratio_age_bars = int((open_time - global_ratio_timestamp) // bar_ms) if global_ratio_timestamp is not None and open_time >= global_ratio_timestamp else None

        micro_last = micro_window[-1] if micro_window else None
        micro_buy_quote = 0.0
        micro_total_quote = 0.0
        micro_minute_returns: list[float] = []
        micro_minute_ranges: list[float] = []
        micro_imbalance_values: list[float] = []
        for micro in micro_window:
            m_open = _safe_float(micro.get("open"))
            m_close = _safe_float(micro.get("close"))
            m_high = _safe_float(micro.get("high"))
            m_low = _safe_float(micro.get("low"))
            m_quote = _safe_float(micro.get("quote_asset_volume")) or 0.0
            m_buy = _safe_float(micro.get("taker_buy_quote_volume")) or 0.0
            micro_buy_quote += m_buy
            micro_total_quote += m_quote
            if m_open not in (None, 0.0) and m_close is not None:
                micro_minute_returns.append(((m_close - m_open) / m_open) * 100.0)
            if m_close not in (None, 0.0) and m_high is not None and m_low is not None:
                micro_minute_ranges.append(((m_high - m_low) / m_close) * 100.0)
            sell = max(m_quote - m_buy, 0.0)
            total = m_buy + sell
            if total > 0:
                micro_imbalance_values.append((m_buy - sell) / total)
        micro_last_buy = _safe_float(micro_last.get("taker_buy_quote_volume")) if micro_last else None
        micro_last_quote = _safe_float(micro_last.get("quote_asset_volume")) if micro_last else None
        micro_last_sell = max((micro_last_quote or 0.0) - (micro_last_buy or 0.0), 0.0) if micro_last else None
        micro_last_total = ((micro_last_buy or 0.0) + (micro_last_sell or 0.0)) if micro_last else 0.0
        micro_last_imbalance = (
            ((micro_last_buy or 0.0) - (micro_last_sell or 0.0)) / micro_last_total
            if micro_last and micro_last_total > 0
            else None
        )
        micro_total_sell = max(micro_total_quote - micro_buy_quote, 0.0)
        micro_total = micro_buy_quote + micro_total_sell
        micro_mean_imbalance = sum(micro_imbalance_values) / len(micro_imbalance_values) if micro_imbalance_values else None
        micro_std_imbalance = None
        if len(micro_imbalance_values) > 1:
            avg = micro_mean_imbalance or 0.0
            micro_std_imbalance = (sum((v - avg) ** 2 for v in micro_imbalance_values) / len(micro_imbalance_values)) ** 0.5
        micro_last_return = micro_minute_returns[-1] if micro_minute_returns else None
        micro_mean_range = sum(micro_minute_ranges) / len(micro_minute_ranges) if micro_minute_ranges else None

        runtime_spreads = [_safe_float(item.get("spread_bps")) for item in runtime_window]
        runtime_spreads = [item for item in runtime_spreads if item is not None]
        runtime_depth_imbalances = [_safe_float(item.get("depth_imbalance")) for item in runtime_window]
        runtime_depth_imbalances = [item for item in runtime_depth_imbalances if item is not None]
        runtime_slippage_buy = [_safe_float(item.get("estimated_slippage_bps_buy")) for item in runtime_window]
        runtime_slippage_buy = [item for item in runtime_slippage_buy if item is not None]
        runtime_slippage_sell = [_safe_float(item.get("estimated_slippage_bps_sell")) for item in runtime_window]
        runtime_slippage_sell = [item for item in runtime_slippage_sell if item is not None]
        runtime_exec_scores = [_safe_float(item.get("execution_quality_score")) for item in runtime_window]
        runtime_exec_scores = [item for item in runtime_exec_scores if item is not None]
        runtime_micro_scores = [_safe_float(item.get("microstructure_score")) for item in runtime_window]
        runtime_micro_scores = [item for item in runtime_micro_scores if item is not None]
        runtime_taker_ratio = [_safe_float(item.get("taker_buy_ratio_5m")) for item in runtime_window]
        runtime_taker_ratio = [item for item in runtime_taker_ratio if item is not None]

        runtime_depth_imbalance_mean = (
            sum(runtime_depth_imbalances) / len(runtime_depth_imbalances) if runtime_depth_imbalances else None
        )
        runtime_depth_imbalance_std = None
        if len(runtime_depth_imbalances) > 1:
            avg = runtime_depth_imbalance_mean or 0.0
            runtime_depth_imbalance_std = (
                sum((value - avg) ** 2 for value in runtime_depth_imbalances) / len(runtime_depth_imbalances)
            ) ** 0.5

        merged.append(
            {
                **row,
                "premium_close": premium.get("close") if premium else None,
                "funding_rate": funding.get("fundingRate") if funding else None,
                "funding_mark_price": funding.get("markPrice") if funding else None,
                "funding_time": funding.get("fundingTime") if funding else None,
                "sum_open_interest": oi.get("sumOpenInterest") if oi else None,
                "sum_open_interest_value": oi.get("sumOpenInterestValue") if oi else None,
                "oi_timestamp": oi_timestamp,
                "oi_age_bars": oi_age_bars,
                "has_oi_context": int(oi is not None),
                "buy_sell_ratio": taker.get("buySellRatio") if taker else None,
                "buy_vol": taker.get("buyVol") if taker else None,
                "sell_vol": taker.get("sellVol") if taker else None,
                "taker_timestamp": taker_timestamp,
                "taker_age_bars": taker_age_bars,
                "has_taker_context": int(taker is not None),
                "micro_window_count": len(micro_window),
                "micro_last_return_1m_pct": micro_last_return,
                "micro_mean_range_1m_pct": micro_mean_range,
                "micro_last_imbalance_1m": micro_last_imbalance,
                "micro_mean_imbalance_1m": micro_mean_imbalance,
                "micro_std_imbalance_1m": micro_std_imbalance,
                "micro_buy_share_5m": (micro_buy_quote / micro_total_quote) if micro_total_quote > 0 else None,
                "micro_window_imbalance_5m": ((micro_buy_quote - micro_total_sell) / micro_total) if micro_total > 0 else None,
                "has_micro_context": int(bool(micro_window)),
                "global_long_short_ratio": global_ratio.get("longShortRatio") if global_ratio else None,
                "global_long_account": global_ratio.get("longAccount") if global_ratio else None,
                "global_short_account": global_ratio.get("shortAccount") if global_ratio else None,
                "global_ratio_timestamp": global_ratio_timestamp,
                "global_ratio_age_bars": global_ratio_age_bars,
                "has_global_ratio_context": int(global_ratio is not None),
                "runtime_snapshot_count": len(runtime_window),
                "runtime_spread_bps_mean": (sum(runtime_spreads) / len(runtime_spreads)) if runtime_spreads else None,
                "runtime_spread_bps_max": max(runtime_spreads) if runtime_spreads else None,
                "runtime_depth_imbalance_last": runtime_depth_imbalances[-1] if runtime_depth_imbalances else None,
                "runtime_depth_imbalance_mean": runtime_depth_imbalance_mean,
                "runtime_depth_imbalance_std": runtime_depth_imbalance_std,
                "runtime_slippage_bps_buy_mean": (sum(runtime_slippage_buy) / len(runtime_slippage_buy)) if runtime_slippage_buy else None,
                "runtime_slippage_bps_sell_mean": (sum(runtime_slippage_sell) / len(runtime_slippage_sell)) if runtime_slippage_sell else None,
                "runtime_execution_quality_mean": (sum(runtime_exec_scores) / len(runtime_exec_scores)) if runtime_exec_scores else None,
                "runtime_microstructure_score_mean": (sum(runtime_micro_scores) / len(runtime_micro_scores)) if runtime_micro_scores else None,
                "runtime_taker_buy_ratio_mean": (sum(runtime_taker_ratio) / len(runtime_taker_ratio)) if runtime_taker_ratio else None,
                "has_runtime_micro_context": int(bool(runtime_window)),
            }
        )
    return merged


def build_dataset(
    *,
    raw_dir: Path,
    output_dir: Path | None = None,
    horizon_bars: int = 12,
    horizon_fast_bars: int = 6,
    horizon_slow_bars: int = 24,
    direction_threshold_pct: float = 0.25,
    opportunity_target_pct: float = 0.35,
    adverse_move_limit_pct: float = 0.35,
    entry_edge_min_pct: float = 0.10,
    entry_edge_high_pct: float = 0.25,
    cost_buffer_pct: float = 0.06,
) -> dict[str, Any]:
    ensure_runtime_dirs()
    output_dir = output_dir or DATASETS_DIR / "BTCUSDT_5m"
    output_dir.mkdir(parents=True, exist_ok=True)

    raw = _load_raw_tables(raw_dir)
    merged = _merge_context(raw)
    features = engineer_feature_rows(merged)
    labeled = attach_direction_labels(
        features,
        horizon_bars=horizon_bars,
        horizon_fast_bars=horizon_fast_bars,
        horizon_slow_bars=horizon_slow_bars,
        direction_threshold_pct=direction_threshold_pct,
        opportunity_target_pct=opportunity_target_pct,
        adverse_move_limit_pct=adverse_move_limit_pct,
        entry_edge_min_pct=entry_edge_min_pct,
        entry_edge_high_pct=entry_edge_high_pct,
        cost_buffer_pct=cost_buffer_pct,
    )
    dataset_rows: list[dict[str, Any]] = []
    for row in labeled:
        enriched = dict(row)
        enriched["baseline_signal_score"] = baseline_signal_score(enriched)
        dataset_rows.append(enriched)

    dataset_path = output_dir / "dataset.csv"
    metadata_path = output_dir / "dataset_manifest.json"

    if not dataset_rows:
        raise RuntimeError("No dataset rows were produced from the raw BTC files.")

    fieldnames = list(dataset_rows[0].keys())
    with dataset_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(dataset_rows)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "raw_dir": str(raw_dir),
        "dataset_path": str(dataset_path),
        "rows": len(dataset_rows),
        "feature_columns": [
            key
            for key in fieldnames
            if key
            not in {
                "label_direction",
                "label_direction_fast",
                "label_direction_base",
                "label_direction_slow",
                "label_direction_regime",
                "label_entry_quality",
                "label_entry_quality_fast",
                "label_entry_quality_base",
                "label_entry_quality_slow",
                "label_entry_quality_long",
                "label_entry_quality_long_fast",
                "label_entry_quality_long_base",
                "label_entry_quality_long_slow",
                "label_regime",
                "label_horizon_bars",
                "label_long_opportunity",
                "label_long_opportunity_fast",
                "label_long_opportunity_slow",
                "label_short_opportunity",
                "label_short_opportunity_fast",
                "label_short_opportunity_slow",
                "future_return_horizon_pct",
                "future_return_fast_pct",
                "future_return_slow_pct",
                "future_max_up_horizon_pct",
                "future_max_up_fast_pct",
                "future_max_up_slow_pct",
                "future_max_down_horizon_pct",
                "future_max_down_fast_pct",
                "future_max_down_slow_pct",
                "future_long_edge_fast_pct",
                "future_short_edge_fast_pct",
                "future_long_edge_horizon_pct",
                "future_short_edge_horizon_pct",
                "future_long_edge_slow_pct",
                "future_short_edge_slow_pct",
                "future_entry_edge_horizon_pct",
                "future_entry_edge_long_horizon_pct",
                "label_direction_long",
                "label_direction_long_fast",
                "label_direction_long_base",
                "label_direction_long_slow",
            }
        ],
        "label_config": {
            "horizon_bars": horizon_bars,
            "horizon_fast_bars": horizon_fast_bars,
            "horizon_slow_bars": horizon_slow_bars,
            "direction_threshold_pct": direction_threshold_pct,
            "opportunity_target_pct": opportunity_target_pct,
            "adverse_move_limit_pct": adverse_move_limit_pct,
            "entry_edge_min_pct": entry_edge_min_pct,
            "entry_edge_high_pct": entry_edge_high_pct,
            "cost_buffer_pct": cost_buffer_pct,
        },
    }
    metadata_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Leiting BTC training dataset from raw market files.")
    parser.add_argument("--raw-dir", default=str(DATA_DIR / "raw" / "BTCUSDT" / "5m"))
    parser.add_argument("--output-dir", default=str(DATASETS_DIR / "BTCUSDT_5m"))
    parser.add_argument("--horizon-bars", type=int, default=12)
    parser.add_argument("--horizon-fast-bars", type=int, default=6)
    parser.add_argument("--horizon-slow-bars", type=int, default=24)
    parser.add_argument("--direction-threshold-pct", type=float, default=0.25)
    parser.add_argument("--opportunity-target-pct", type=float, default=0.35)
    parser.add_argument("--adverse-move-limit-pct", type=float, default=0.35)
    parser.add_argument("--entry-edge-min-pct", type=float, default=0.10)
    parser.add_argument("--entry-edge-high-pct", type=float, default=0.25)
    parser.add_argument("--cost-buffer-pct", type=float, default=0.06)
    args = parser.parse_args()
    manifest = build_dataset(
        raw_dir=Path(args.raw_dir),
        output_dir=Path(args.output_dir),
        horizon_bars=args.horizon_bars,
        horizon_fast_bars=args.horizon_fast_bars,
        horizon_slow_bars=args.horizon_slow_bars,
        direction_threshold_pct=args.direction_threshold_pct,
        opportunity_target_pct=args.opportunity_target_pct,
        adverse_move_limit_pct=args.adverse_move_limit_pct,
        entry_edge_min_pct=args.entry_edge_min_pct,
        entry_edge_high_pct=args.entry_edge_high_pct,
        cost_buffer_pct=args.cost_buffer_pct,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
