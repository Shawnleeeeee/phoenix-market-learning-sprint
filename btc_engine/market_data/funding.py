"""Funding and premium helpers."""

from __future__ import annotations

from pathlib import Path

from .backfill import BackfillConfig, run_backfill
from .public_client import BinancePublicFuturesClient


def fetch_funding_context(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "5m",
    days: int = 90,
    output_dir: str | Path | None = None,
) -> dict:
    """Backfill funding, premium, and related public market context."""
    config = BackfillConfig(
        symbol=symbol,
        interval=interval,
        days=days,
        output_dir=Path(output_dir) if output_dir else None,
    )
    return run_backfill(config)


def fetch_current_funding_context(client: BinancePublicFuturesClient, *, symbol: str) -> dict:
    premium = client.get_json("/fapi/v1/premiumIndex", {"symbol": symbol})
    funding_rate = float(premium.get("lastFundingRate", 0.0))
    mark_price = float(premium.get("markPrice", 0.0))
    index_price = float(premium.get("indexPrice", 0.0))
    estimated_settle_price = float(premium.get("estimatedSettlePrice", 0.0))
    premium_index = float(premium.get("interestRate", 0.0))
    basis_pct = ((mark_price - index_price) / index_price * 100.0) if index_price else 0.0
    ticker = client.get_json("/fapi/v1/ticker/24hr", {"symbol": symbol})
    return {
        "mark_price": mark_price,
        "index_price": index_price,
        "estimated_settle_price": estimated_settle_price,
        "funding_rate": funding_rate,
        "next_funding_time_ms": int(premium.get("nextFundingTime", 0)),
        "premium_index": premium_index,
        "mark_index_basis_pct": round(basis_pct, 6),
        "volume_24h_usdt": float(ticker.get("quoteVolume", 0.0)),
        "price_change_24h_pct": float(ticker.get("priceChangePercent", 0.0)),
        "raw": {"premium": premium, "ticker_24h": ticker},
    }
