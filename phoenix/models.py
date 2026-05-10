from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class CandidateSignal:
    symbol: str
    alpha_symbol: str
    chain_id: str
    contract_address: str
    score: float
    confidence: str
    directional_bias: str = "NONE"
    directional_score: float = 0.0
    directional_breakdown: dict[str, float] = field(default_factory=dict)
    blocked_reasons: list[str] = field(default_factory=list)
    score_breakdown: dict[str, float] = field(default_factory=dict)
    mark_price: float | None = None
    oi_delta_pct: float | None = None
    oi_delta_1m_pct: float | None = None
    oi_delta_5m_pct: float | None = None
    oi_delta_15m_pct: float | None = None
    price_change_1m_pct: float | None = None
    price_change_5m_pct: float | None = None
    price_change_15m_pct: float | None = None
    oi_notional_usd: float | None = None
    volume_5m_ratio: float | None = None
    volume_5m_ratio_short: float | None = None
    volume_5m_ratio_medium: float | None = None
    volume_5m_ratio_long: float | None = None
    volume_24h: float | None = None
    market_cap: float | None = None
    spread_bps: float | None = None
    depth_bid_5: float | None = None
    depth_ask_5: float | None = None
    depth_imbalance: float | None = None
    estimated_slippage_bps: float | None = None
    estimated_slippage_for_order_usdt: float | None = None
    funding_rate: float | None = None
    next_funding_time_ms: int | None = None
    premium_index: float | None = None
    mark_index_basis_pct: float | None = None
    onboard_date_ms: int | None = None
    listing_age_hours: float | None = None
    taker_buy_ratio_1m: float | None = None
    taker_buy_ratio_5m: float | None = None
    aggressive_flow_delta: float | None = None
    taker_buy_volume_5m: float | None = None
    taker_sell_volume_5m: float | None = None
    liquidation_long_usd: float | None = None
    liquidation_short_usd: float | None = None
    liquidation_event_count: int | None = None
    squeeze_probability_hint: float | None = None
    market_confirmation_score: float | None = None
    execution_quality_score: float | None = None
    event_risk_score: float | None = None
    directional_conflicts: list[str] = field(default_factory=list)
    event_flags: list[str] = field(default_factory=list)
    social_rank: int | None = None
    social_hype: float | None = None
    social_hype_change_pct: float | None = None
    social_signal_freshness: float | None = None
    social_sentiment: str | None = None
    social_summary: str | None = None
    square_trending_rank: int | None = None
    square_search_rank: int | None = None
    square_discovery_score: float | None = None
    smart_money_buy_signals: int = 0
    smart_money_sell_signals: int = 0
    smart_money_active_signals: int = 0
    smart_money_traders: int = 0
    smart_money_latest_age_minutes: float | None = None
    smart_money_weighted_buy_strength: float | None = None
    smart_money_tags: list[str] = field(default_factory=list)
    topic_names: list[str] = field(default_factory=list)
    topic_net_inflow_usd: float | None = None
    topic_net_inflow_1h_usd: float | None = None
    topic_latest_age_minutes: float | None = None
    topic_freshness: float | None = None
    audit_risk_level: int | None = None
    audit_risk_label: str | None = None
    audit_flags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SymbolRules:
    symbol: str
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float
    trigger_protect: float | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SymbolRules":
        return cls(
            symbol=str(payload["symbol"]),
            tick_size=float(payload["tick_size"]),
            step_size=float(payload["step_size"]),
            min_qty=float(payload["min_qty"]),
            min_notional=float(payload["min_notional"]),
            trigger_protect=(
                float(payload["trigger_protect"])
                if payload.get("trigger_protect") not in (None, "")
                else None
            ),
        )


@dataclass(slots=True)
class TradeIntent:
    symbol: str
    side: str
    entry_price: float
    quantity: float
    leverage: int
    quote_allocation_usdt: float
    notional_usdt: float
    margin_type: str
    working_type: str
    initial_stop_price: float
    take_profit_price: float | None
    breakeven_trigger_price: float
    breakeven_stop_price: float
    trailing_callback_rate: float
    allocation_mode: str
    allocation_cap_usdt: float | None
    risk_budget_usdt: float | None
    rules: SymbolRules

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TradeIntent":
        return cls(
            symbol=str(payload["symbol"]),
            side=str(payload["side"]),
            entry_price=float(payload["entry_price"]),
            quantity=float(payload["quantity"]),
            leverage=int(payload["leverage"]),
            quote_allocation_usdt=float(payload["quote_allocation_usdt"]),
            notional_usdt=float(payload["notional_usdt"]),
            margin_type=str(payload["margin_type"]),
            working_type=str(payload["working_type"]),
            initial_stop_price=float(payload["initial_stop_price"]),
            take_profit_price=(
                float(payload["take_profit_price"])
                if payload.get("take_profit_price") not in (None, "")
                else None
            ),
            breakeven_trigger_price=float(payload["breakeven_trigger_price"]),
            breakeven_stop_price=float(payload["breakeven_stop_price"]),
            trailing_callback_rate=float(payload["trailing_callback_rate"]),
            allocation_mode=str(payload.get("allocation_mode") or "FIXED"),
            allocation_cap_usdt=(
                float(payload["allocation_cap_usdt"])
                if payload.get("allocation_cap_usdt") not in (None, "")
                else None
            ),
            risk_budget_usdt=(
                float(payload["risk_budget_usdt"])
                if payload.get("risk_budget_usdt") not in (None, "")
                else None
            ),
            rules=SymbolRules.from_dict(payload["rules"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "entry_price": self.entry_price,
            "quantity": self.quantity,
            "leverage": self.leverage,
            "quote_allocation_usdt": self.quote_allocation_usdt,
            "notional_usdt": self.notional_usdt,
            "margin_type": self.margin_type,
            "working_type": self.working_type,
            "initial_stop_price": self.initial_stop_price,
            "take_profit_price": self.take_profit_price,
            "breakeven_trigger_price": self.breakeven_trigger_price,
            "breakeven_stop_price": self.breakeven_stop_price,
            "trailing_callback_rate": self.trailing_callback_rate,
            "allocation_mode": self.allocation_mode,
            "allocation_cap_usdt": self.allocation_cap_usdt,
            "risk_budget_usdt": self.risk_budget_usdt,
            "rules": asdict(self.rules),
        }


@dataclass(slots=True)
class OrderInstruction:
    name: str
    endpoint: str
    payload: dict[str, Any]
    description: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OrderInstruction":
        return cls(
            name=str(payload["name"]),
            endpoint=str(payload["endpoint"]),
            payload=dict(payload["payload"]),
            description=str(payload["description"]),
        )
