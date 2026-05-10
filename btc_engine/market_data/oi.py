"""Open-interest and taker-flow helpers."""

from __future__ import annotations

from pathlib import Path

from .backfill import BackfillConfig, run_backfill
from .public_client import BinancePublicFuturesClient


def fetch_oi_context(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "5m",
    days: int = 90,
    output_dir: str | Path | None = None,
) -> dict:
    """Backfill OI history and taker buy/sell ratio history."""
    config = BackfillConfig(
        symbol=symbol,
        interval=interval,
        days=days,
        output_dir=Path(output_dir) if output_dir else None,
    )
    return run_backfill(config)


def fetch_current_oi_context(client: BinancePublicFuturesClient, *, symbol: str, period: str = "5m") -> dict:
    open_interest = client.get_json("/fapi/v1/openInterest", {"symbol": symbol})
    taker_ratio = client.get_json("/futures/data/takerlongshortRatio", {"symbol": symbol, "period": period, "limit": 1})
    global_ratio = client.get_json("/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": 1})
    entry = taker_ratio[0] if taker_ratio else {}
    global_entry = global_ratio[0] if global_ratio else {}
    return {
        "open_interest": float(open_interest.get("openInterest", 0.0)),
        "taker_buy_ratio_5m": float(entry["buySellRatio"]) if entry.get("buySellRatio") is not None else None,
        "aggressive_flow_delta": (
            float(entry.get("buyVol", 0.0)) - float(entry.get("sellVol", 0.0))
            if entry
            else None
        ),
        "global_long_short_ratio_5m": float(global_entry["longShortRatio"]) if global_entry.get("longShortRatio") is not None else None,
        "global_long_account_5m": float(global_entry["longAccount"]) if global_entry.get("longAccount") is not None else None,
        "global_short_account_5m": float(global_entry["shortAccount"]) if global_entry.get("shortAccount") is not None else None,
        "raw": {"open_interest": open_interest, "taker_ratio": entry, "global_ratio": global_entry},
    }
