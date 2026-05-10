"""5m kline backfill helpers for BTC execution research."""

from __future__ import annotations

from pathlib import Path

from .backfill import BackfillConfig, run_backfill


def backfill_klines(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "5m",
    days: int = 90,
    output_dir: str | Path | None = None,
) -> dict:
    """Backfill BTCUSDT klines and companion market datasets into raw storage."""
    config = BackfillConfig(
        symbol=symbol,
        interval=interval,
        days=days,
        output_dir=Path(output_dir) if output_dir else None,
    )
    return run_backfill(config)
