from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable


STATUS_SHADOW_ONLY = "shadow_only"
STATUS_ACTIVE_SHADOW = "active_shadow"
STATUS_WATCHLIST = "watchlist"
STATUS_PAUSED = "paused"
STATUS_PROMOTION_CANDIDATE_SHADOW_ONLY = "promotion_candidate_shadow_only"


@dataclass(frozen=True, slots=True)
class StrategyDefinition:
    strategy_id: str
    strategy_family: str
    version: str
    source: str
    status: str = STATUS_SHADOW_ONLY
    live_trading_enabled: bool = False
    promotion_allowed: bool = False
    shadow_only: bool = True
    paper_record_only: bool = True
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def default_strategy_definitions() -> list[StrategyDefinition]:
    return [
        StrategyDefinition(
            strategy_id="ONE_MIN_MOMENTUM_SCALP_PLUS",
            strategy_family="ONE_MIN_MOMENTUM_SCALP_PLUS",
            version="v1",
            source="phoenix_momentum_scalp_plus",
            status=STATUS_ACTIVE_SHADOW,
            notes="1m momentum shadow/paper experiment; never auto-promotes to live.",
        ),
        StrategyDefinition(
            strategy_id="OI_BUILD_BREAKOUT_QUALITY_SHADOW",
            strategy_family="OI_BUILD_BREAKOUT",
            version="v1",
            source="phoenix_signal_bridge",
            status=STATUS_ACTIVE_SHADOW,
            notes="High-quality low-frequency shadow branch.",
        ),
        StrategyDefinition(
            strategy_id="OI_BUILD_BREAKOUT_BALANCED_SHADOW",
            strategy_family="OI_BUILD_BREAKOUT",
            version="v1",
            source="phoenix_signal_bridge",
            status=STATUS_ACTIVE_SHADOW,
            notes="Balanced shadow branch.",
        ),
    ]


def strategy_registry_payload(strategies: Iterable[StrategyDefinition] | None = None) -> dict[str, Any]:
    rows = [strategy.to_dict() for strategy in (strategies or default_strategy_definitions())]
    return {
        "strategy_count": len(rows),
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "auto_promotion_allowed": False,
        "strategies": rows,
    }


def assert_registry_shadow_safe(payload: dict[str, Any]) -> None:
    if payload.get("live_trading_enabled") is True or payload.get("promotion_allowed") is True:
        raise ValueError("Phoenix strategy registry cannot enable live trading or promotion.")
    for row in payload.get("strategies") or []:
        if row.get("live_trading_enabled") is True or row.get("promotion_allowed") is True:
            raise ValueError(f"Unsafe strategy registry row: {row.get('strategy_id')}")
