from __future__ import annotations

from typing import Any


def _sum_size(levels: list[list[Any]], depth: int = 5) -> float:
    total = 0.0
    for _price, size, *_rest in levels[:depth]:
        try:
            total += float(size)
        except (TypeError, ValueError):
            continue
    return total


def compute_orderbook_imbalance_features(snapshot: dict[str, Any]) -> dict[str, float]:
    bids = snapshot.get("bids") or []
    asks = snapshot.get("asks") or []
    bid_depth_5 = _sum_size(bids, 5)
    ask_depth_5 = _sum_size(asks, 5)
    total = bid_depth_5 + ask_depth_5
    obi_5 = ((bid_depth_5 - ask_depth_5) / total) if total else 0.0
    return {
        "flow_bid_depth_5": bid_depth_5,
        "flow_ask_depth_5": ask_depth_5,
        "flow_obi_5": obi_5,
    }
