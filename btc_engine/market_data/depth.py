"""Depth / order book sampling for Leiting."""

from __future__ import annotations

from .public_client import BinancePublicFuturesClient


def _sum_notional(levels: list[list[str]], limit: int = 5) -> float:
    total = 0.0
    for price_raw, qty_raw in levels[:limit]:
        total += float(price_raw) * float(qty_raw)
    return total


def estimate_slippage_bps(levels: list[list[str]], order_notional_usdt: float, *, side: str) -> tuple[float, float]:
    if order_notional_usdt <= 0:
        return 0.0, 0.0
    remaining = order_notional_usdt
    fill_notional = 0.0
    fill_cost = 0.0
    for price_raw, qty_raw in levels:
        price = float(price_raw)
        level_notional = price * float(qty_raw)
        take = min(remaining, level_notional)
        fill_notional += take
        fill_cost += take * price
        remaining -= take
        if remaining <= 1e-9:
            break
    if fill_notional <= 0:
        return 9999.0, order_notional_usdt
    best_price = float(levels[0][0])
    avg_fill_price = fill_cost / fill_notional
    if side == "BUY":
        slippage_bps = max(0.0, (avg_fill_price - best_price) / best_price * 10_000)
    else:
        slippage_bps = max(0.0, (best_price - avg_fill_price) / best_price * 10_000)
    uncovered = max(0.0, remaining)
    return round(slippage_bps, 4), round(uncovered, 4)


def analyze_depth_levels(*, bids: list[list[str]], asks: list[list[str]], order_notional_usdt: float = 1000.0) -> dict:
    """Compute spread, imbalance, and slippage from pre-fetched depth levels."""
    if not bids or not asks:
        return {
            "best_bid": 0.0,
            "best_ask": 0.0,
            "spread_bps": 9999.0,
            "depth_bid_5": 0.0,
            "depth_ask_5": 0.0,
            "depth_imbalance": 0.0,
            "estimated_slippage_bps_buy": 9999.0,
            "estimated_slippage_bps_sell": 9999.0,
            "estimated_uncovered_usdt_buy": round(order_notional_usdt, 4),
            "estimated_uncovered_usdt_sell": round(order_notional_usdt, 4),
            "raw": {"bids": bids, "asks": asks},
        }
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid = (best_bid + best_ask) / 2
    spread_bps = ((best_ask - best_bid) / mid * 10_000) if mid else 0.0
    depth_bid_5 = _sum_notional(bids, 5)
    depth_ask_5 = _sum_notional(asks, 5)
    depth_total = depth_bid_5 + depth_ask_5
    depth_imbalance = ((depth_bid_5 - depth_ask_5) / depth_total) if depth_total else 0.0
    buy_slippage_bps, buy_uncovered = estimate_slippage_bps(asks, order_notional_usdt, side="BUY")
    sell_slippage_bps, sell_uncovered = estimate_slippage_bps(bids, order_notional_usdt, side="SELL")
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_bps": round(spread_bps, 4),
        "depth_bid_5": round(depth_bid_5, 4),
        "depth_ask_5": round(depth_ask_5, 4),
        "depth_imbalance": round(depth_imbalance, 4),
        "estimated_slippage_bps_buy": buy_slippage_bps,
        "estimated_slippage_bps_sell": sell_slippage_bps,
        "estimated_uncovered_usdt_buy": buy_uncovered,
        "estimated_uncovered_usdt_sell": sell_uncovered,
        "raw": {"bids": bids, "asks": asks},
    }


def sample_depth(client: BinancePublicFuturesClient, *, symbol: str, order_notional_usdt: float = 1000.0) -> dict:
    """Collect spread, depth, imbalance, and slippage inputs."""
    payload = client.get_json("/fapi/v1/depth", {"symbol": symbol, "limit": 20})
    analyzed = analyze_depth_levels(bids=payload["bids"], asks=payload["asks"], order_notional_usdt=order_notional_usdt)
    analyzed["raw"] = payload
    return analyzed
