"""Shared types for the BTC execution engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


Side = Literal["BUY", "SELL"]
Bias = Literal["LONG", "SHORT", "NONE"]


@dataclass(slots=True)
class TradeIntent:
    symbol: str
    side: Side
    leverage: int
    quote_allocation_usdt: float
    reason: str


@dataclass(slots=True)
class StrategyConfig:
    symbol: str = "BTCUSDT"
    interval: str = "5m"
    allow_auto_short: bool = False
    max_open_positions: int = 1
    momentum_threshold: float = 0.65
    directional_confidence_min: float = 0.6
    execution_quality_min: float = 5.0
    event_risk_max: float = 4.0
    engine_poll_interval_sec: int = 30


@dataclass(slots=True)
class RiskConfig:
    default_leverage: int = 10
    risk_budget_pct: float = 1.0
    max_daily_loss_pct: float = 3.0
    cooldown_minutes_after_close: int = 30
    max_slippage_bps: float = 8.0
    max_spread_bps: float = 5.0
    funding_hard_cap_bps_long: float = 8.0
    funding_hard_cap_bps_short: float = 8.0
    fixed_quote_allocation_usdt: float = 100.0


@dataclass(slots=True)
class MarketSnapshot:
    symbol: str
    interval: str
    mark_price: float
    index_price: float
    estimated_settle_price: float
    funding_rate: float
    next_funding_time_ms: int
    open_interest: float
    open_interest_notional_usd: float
    spread_bps: float
    depth_bid_5: float
    depth_ask_5: float
    depth_imbalance: float
    estimated_slippage_bps: float
    estimated_slippage_for_order_usdt: float
    taker_buy_ratio_5m: float | None
    aggressive_flow_delta: float | None
    price_change_5m_pct: float | None
    price_change_15m_pct: float | None
    price_change_1h_pct: float | None
    premium_index: float | None
    mark_index_basis_pct: float | None
    volume_24h_usdt: float | None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SignalDecision:
    bias: Bias
    directional_confidence: float
    regime: str
    momentum_score: float
    microstructure_score: float
    execution_quality_score: float
    event_risk_score: float
    gates_passed: bool
    gate_reasons: list[str]
    directional_reasons: list[str]
    recommended_quote_allocation_usdt: float


@dataclass(slots=True)
class SymbolRules:
    symbol: str
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float


@dataclass(slots=True)
class ExecutionPlan:
    symbol: str
    side: Side
    leverage: int
    quote_allocation_usdt: float
    reference_price: float
    quantity: float
    notional_usdt: float
    initial_stop_price: float
    breakeven_trigger_price: float
    breakeven_stop_price: float
    trailing_callback_rate: float
    rules: SymbolRules
    reason: str
    entry_order_type: str = "MARKET"
    entry_limit_price: float | None = None
    entry_time_in_force: str | None = None
    entry_timeout_sec: float | None = None
    maker_first: bool = False
    take_profit_net_roi_pct: float | None = None
    stop_loss_net_roi_pct: float | None = None
    estimated_fee_bps_per_side: float = 5.0
    max_hold_minutes: float | None = None
    enable_breakeven: bool = True
    enable_trailing: bool = True
