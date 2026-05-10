from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def normalize_depth_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Normalize 雷霆 runtime snapshots for research-only feature extraction."""

    bids = snapshot.get("bids") or []
    asks = snapshot.get("asks") or []
    return {
        "ts": snapshot.get("ts") or snapshot.get("timestamp"),
        "symbol": snapshot.get("symbol", "BTCUSDT"),
        "best_bid": snapshot.get("best_bid") or (bids[0][0] if bids else None),
        "best_ask": snapshot.get("best_ask") or (asks[0][0] if asks else None),
        "spread_bps": snapshot.get("spread_bps"),
        "depth_imbalance": snapshot.get("depth_imbalance"),
        "estimated_slippage_bps": snapshot.get("estimated_slippage_bps"),
        "bids": bids,
        "asks": asks,
        "taker_buy_ratio_1m": snapshot.get("taker_buy_ratio_1m"),
        "taker_buy_ratio_5m": snapshot.get("taker_buy_ratio_5m"),
        "aggressive_flow_delta": snapshot.get("aggressive_flow_delta"),
    }


def snapshot_to_feature_input(snapshot: dict[str, Any]) -> dict[str, float]:
    normalized = normalize_depth_snapshot(snapshot)
    best_bid = normalized.get("best_bid") or 0.0
    best_ask = normalized.get("best_ask") or 0.0
    mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0.0
    spread_bps = normalized.get("spread_bps") or 0.0
    depth_imbalance = normalized.get("depth_imbalance") or 0.0
    slippage_bps = normalized.get("estimated_slippage_bps") or 0.0
    return {
        "micro_mid_price": mid,
        "micro_spread_bps": float(spread_bps),
        "micro_depth_imbalance": float(depth_imbalance),
        "micro_slippage_bps": float(slippage_bps),
        "micro_taker_buy_ratio_1m": float(normalized.get("taker_buy_ratio_1m") or 0.0),
        "micro_taker_buy_ratio_5m": float(normalized.get("taker_buy_ratio_5m") or 0.0),
        "micro_aggressive_flow_delta": float(normalized.get("aggressive_flow_delta") or 0.0),
    }


def load_snapshot_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    p = Path(path)
    if not p.exists():
        return rows
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows
