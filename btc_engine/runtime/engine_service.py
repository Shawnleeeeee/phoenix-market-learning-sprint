"""Live public-market BTC engine service for Leiting."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from btc_engine.config import (
    RESEARCH_RUNTIME_DIR,
    STATE_DIR,
    ensure_runtime_dirs,
    environment_summary,
    get_demo_leverage,
    get_demo_position_fraction,
    get_demo_start_equity_usdt,
    get_execution_mode,
    get_market_stream_freshness_sec,
    get_runtime_engine_mode,
    load_risk_config,
    load_strategy_config,
    market_stream_enabled,
)
from btc_engine.market_data.depth import analyze_depth_levels, sample_depth
from btc_engine.market_data.funding import fetch_current_funding_context
from btc_engine.market_data.oi import fetch_current_oi_context
from btc_engine.market_data.public_client import BinancePublicFuturesClient
from btc_engine.risk.cooldown import cooldown_active
from btc_engine.risk.gating import passes_risk_gates
from btc_engine.risk.kill_switch import kill_switch_active
from btc_engine.risk.sizing import compute_position_size
from btc_engine.runtime.service_health import heartbeat
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state
from btc_engine.signals.directional_bias import determine_bias
from btc_engine.signals.microstructure import compute_microstructure_score
from btc_engine.signals.momentum import compute_momentum_signal
from btc_engine.signals.regime import classify_regime


def _parse_dt(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fetch_recent_klines(client: BinancePublicFuturesClient, symbol: str, interval: str, limit: int = 20) -> list[list]:
    return client.get_json("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})


def _price_changes(klines: list[list]) -> tuple[float | None, float | None, float | None]:
    if len(klines) < 13:
        return None, None, None
    closes = [float(row[4]) for row in klines]
    latest = closes[-1]
    def pct(back: int) -> float:
        base = closes[-1 - back]
        return ((latest - base) / base) * 100 if base else 0.0
    return round(pct(1), 6), round(pct(3), 6), round(pct(12), 6)


def _load_market_stream_snapshot(*, symbol: str, interval: str, order_notional_usdt: float) -> dict | None:
    if not market_stream_enabled():
        return None
    payload = read_runtime_state("market_stream_snapshot")
    if not payload:
        return None
    if str(payload.get("symbol") or "").upper() != symbol.upper():
        return None
    if str(payload.get("interval") or "").strip() != interval:
        return None
    generated_at = _parse_dt(payload.get("generated_at"))
    if generated_at is None:
        return None
    age_sec = (datetime.now(timezone.utc) - generated_at).total_seconds()
    if age_sec > get_market_stream_freshness_sec():
        return None
    depth_levels = payload.get("depth_levels") or {}
    bids = depth_levels.get("bids") or []
    asks = depth_levels.get("asks") or []
    if not bids or not asks:
        return None
    depth = analyze_depth_levels(bids=bids, asks=asks, order_notional_usdt=order_notional_usdt)
    return {
        "source": "ws",
        "generated_at": payload.get("generated_at"),
        "last_event_type": payload.get("last_event_type"),
        "last_event_at": payload.get("last_event_at"),
        "last_trade_at": payload.get("last_trade_at"),
        "last_kline_at": payload.get("last_kline_at"),
        "last_depth_at": payload.get("last_depth_at"),
        "depth": depth,
        "trade": payload.get("trade") or {},
        "price_change_5m_pct": payload.get("price_change_5m_pct"),
        "price_change_15m_pct": payload.get("price_change_15m_pct"),
        "price_change_1h_pct": payload.get("price_change_1h_pct"),
    }


def _build_snapshot() -> dict:
    ensure_runtime_dirs()
    strategy = load_strategy_config()
    risk = load_risk_config()
    client = BinancePublicFuturesClient()
    symbol = strategy.symbol
    execution_mode = get_execution_mode()

    quote_allocation_usdt = risk.fixed_quote_allocation_usdt
    order_notional_usdt = risk.fixed_quote_allocation_usdt * risk.default_leverage
    if execution_mode in {"demo_auto", "testnet_auto"}:
        demo_account = read_runtime_state("demo_account") or {}
        virtual_equity = float(demo_account.get("equity_usdt") or get_demo_start_equity_usdt())
        quote_allocation_usdt = round(virtual_equity * get_demo_position_fraction(), 4)
        order_notional_usdt = quote_allocation_usdt * get_demo_leverage()

    funding = fetch_current_funding_context(client, symbol=symbol)
    oi = fetch_current_oi_context(client, symbol=symbol, period=strategy.interval)
    stream_market = _load_market_stream_snapshot(symbol=symbol, interval=strategy.interval, order_notional_usdt=order_notional_usdt)
    if stream_market:
        depth = stream_market["depth"]
        price_change_5m_pct = stream_market.get("price_change_5m_pct")
        price_change_15m_pct = stream_market.get("price_change_15m_pct")
        price_change_1h_pct = stream_market.get("price_change_1h_pct")
        market_data_source = "ws"
    else:
        depth = sample_depth(client, symbol=symbol, order_notional_usdt=order_notional_usdt)
        klines = _fetch_recent_klines(client, symbol, strategy.interval, limit=20)
        price_change_5m_pct, price_change_15m_pct, price_change_1h_pct = _price_changes(klines)
        market_data_source = "rest"

    momentum = compute_momentum_signal(
        price_change_5m_pct=price_change_5m_pct,
        price_change_15m_pct=price_change_15m_pct,
        price_change_1h_pct=price_change_1h_pct,
    )
    micro = compute_microstructure_score(
        spread_bps=depth["spread_bps"],
        depth_imbalance=depth["depth_imbalance"],
        taker_buy_ratio_5m=oi["taker_buy_ratio_5m"],
        aggressive_flow_delta=oi["aggressive_flow_delta"],
        estimated_slippage_bps_buy=depth["estimated_slippage_bps_buy"],
        estimated_slippage_bps_sell=depth["estimated_slippage_bps_sell"],
    )
    bias, directional_confidence, directional_reasons = determine_bias(
        momentum_score=momentum,
        microstructure_score=micro,
        funding_rate=funding["funding_rate"],
        allow_auto_short=strategy.allow_auto_short,
        directional_confidence_min=strategy.directional_confidence_min,
    )
    regime = classify_regime(
        momentum_score=momentum,
        microstructure_score=micro,
        funding_rate=funding["funding_rate"],
    )

    execution_quality_score = max(
        0.0,
        min(
            10.0,
            10.0
            - (depth["spread_bps"] / max(risk.max_spread_bps, 0.1)) * 2.0
            - (min(depth["estimated_slippage_bps_buy"], depth["estimated_slippage_bps_sell"]) / max(risk.max_slippage_bps, 0.1)) * 3.0
            + max(0.0, depth["depth_imbalance"]) * 1.5,
        ),
    )
    event_risk_score = 0.0
    if abs(funding["funding_rate"]) * 10_000 > max(risk.funding_hard_cap_bps_long, risk.funding_hard_cap_bps_short):
        event_risk_score += 3.0
    if abs(funding["mark_index_basis_pct"]) > 1.0:
        event_risk_score += 2.0
    if abs(oi["aggressive_flow_delta"] or 0.0) > 5000:
        event_risk_score += 1.0
    event_risk_score = round(min(event_risk_score, 10.0), 4)

    cooldown_on, cooldown_until = cooldown_active(
        last_close_time_iso=(read_runtime_state("last_close") or {}).get("closed_at"),
        cooldown_minutes=risk.cooldown_minutes_after_close,
    )
    kill_on, kill_reason = kill_switch_active(
        max_daily_loss_pct=risk.max_daily_loss_pct,
        daily_pnl_pct=(read_runtime_state("daily_pnl") or {}).get("daily_pnl_pct"),
    )
    estimated_slippage_bps = depth["estimated_slippage_bps_buy"] if bias != "SHORT" else depth["estimated_slippage_bps_sell"]
    gates_passed, gate_reasons = passes_risk_gates(
        strategy=strategy,
        risk=risk,
        bias=bias,
        directional_confidence=directional_confidence,
        execution_quality_score=execution_quality_score,
        event_risk_score=event_risk_score,
        spread_bps=depth["spread_bps"],
        estimated_slippage_bps=estimated_slippage_bps,
        funding_rate=funding["funding_rate"],
    )
    if cooldown_on:
        gates_passed = False
        gate_reasons.append(f"冷却中，直到 {cooldown_until}")
    if kill_on:
        gates_passed = False
        gate_reasons.append(kill_reason or "熔断触发")

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": environment_summary(),
        "strategy": asdict(strategy),
        "risk": asdict(risk),
        "market": {
            **funding,
            **oi,
            **depth,
            "market_data_source": market_data_source,
            "market_stream_generated_at": (stream_market or {}).get("generated_at"),
            "market_stream_last_event_at": (stream_market or {}).get("last_event_at"),
            "last_trade_price": ((stream_market or {}).get("trade") or {}).get("price"),
            "last_trade_quantity": ((stream_market or {}).get("trade") or {}).get("quantity"),
            "last_trade_is_buyer_maker": ((stream_market or {}).get("trade") or {}).get("is_buyer_maker"),
            "price_change_5m_pct": price_change_5m_pct,
            "price_change_15m_pct": price_change_15m_pct,
            "price_change_1h_pct": price_change_1h_pct,
        },
        "signal": {
            "momentum_score": momentum,
            "microstructure_score": micro,
            "regime": regime,
            "bias": bias,
            "directional_confidence": directional_confidence,
            "directional_reasons": directional_reasons,
            "execution_quality_score": round(execution_quality_score, 4),
            "event_risk_score": event_risk_score,
            "gates_passed": gates_passed,
            "gate_reasons": gate_reasons,
            "recommended_quote_allocation_usdt": compute_position_size(
                account_equity_usdt=None,
                risk_budget_pct=risk.risk_budget_pct,
                fixed_quote_allocation_usdt=quote_allocation_usdt,
            ),
        },
    }
    return snapshot


def run_once() -> dict:
    if get_runtime_engine_mode() in {"v3_simple", "v4_microstructure"}:
        from btc_engine.runtime.engine_service_v3_simple import run_once as run_once_v3

        return run_once_v3()
    snapshot = _build_snapshot()
    update_runtime_state("engine_snapshot", snapshot)
    RESEARCH_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    micro_path = RESEARCH_RUNTIME_DIR / "market_micro_snapshots.jsonl"
    micro_packet = {
        "generated_at": snapshot["generated_at"],
        "symbol": snapshot["environment"]["symbol"],
        "bias": snapshot["signal"]["bias"],
        "regime": snapshot["signal"]["regime"],
        "momentum_score": snapshot["signal"]["momentum_score"],
        "microstructure_score": snapshot["signal"]["microstructure_score"],
        "execution_quality_score": snapshot["signal"]["execution_quality_score"],
        "event_risk_score": snapshot["signal"]["event_risk_score"],
        "mark_price": snapshot["market"]["mark_price"],
        "funding_rate": snapshot["market"]["funding_rate"],
        "mark_index_basis_pct": snapshot["market"]["mark_index_basis_pct"],
        "open_interest": snapshot["market"]["open_interest"],
        "taker_buy_ratio_5m": snapshot["market"]["taker_buy_ratio_5m"],
        "aggressive_flow_delta": snapshot["market"]["aggressive_flow_delta"],
        "global_long_short_ratio_5m": snapshot["market"].get("global_long_short_ratio_5m"),
        "global_long_account_5m": snapshot["market"].get("global_long_account_5m"),
        "global_short_account_5m": snapshot["market"].get("global_short_account_5m"),
        "best_bid": snapshot["market"]["best_bid"],
        "best_ask": snapshot["market"]["best_ask"],
        "spread_bps": snapshot["market"]["spread_bps"],
        "depth_bid_5": snapshot["market"]["depth_bid_5"],
        "depth_ask_5": snapshot["market"]["depth_ask_5"],
        "depth_imbalance": snapshot["market"]["depth_imbalance"],
        "estimated_slippage_bps_buy": snapshot["market"]["estimated_slippage_bps_buy"],
        "estimated_slippage_bps_sell": snapshot["market"]["estimated_slippage_bps_sell"],
    }
    with micro_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(micro_packet, ensure_ascii=False) + "\n")
    markdown = "\n".join(
        [
            "# 雷霆 BTC 引擎状态",
            "",
            f"- 生成时间：{snapshot['generated_at']}",
            f"- 标的：{snapshot['environment']['symbol']}",
            f"- 偏向：{snapshot['signal']['bias']}",
            f"- 方向置信度：{snapshot['signal']['directional_confidence']}",
            f"- 市场阶段：{snapshot['signal']['regime']}",
            f"- 动量分：{snapshot['signal']['momentum_score']}",
            f"- 微观结构分：{snapshot['signal']['microstructure_score']}",
            f"- 执行质量分：{snapshot['signal']['execution_quality_score']}",
            f"- 事件风险分：{snapshot['signal']['event_risk_score']}",
            f"- 风控放行：{'是' if snapshot['signal']['gates_passed'] else '否'}",
            f"- 原因：{'; '.join(snapshot['signal']['gate_reasons'] or snapshot['signal']['directional_reasons'])}",
            "",
            "## 市场",
            f"- 标记价：{snapshot['market']['mark_price']}",
            f"- 市场数据源：{snapshot['market']['market_data_source']}",
            f"- 指数价：{snapshot['market']['index_price']}",
            f"- 资金费：{snapshot['market']['funding_rate']}",
            f"- 基差：{snapshot['market']['mark_index_basis_pct']}%",
            f"- OI：{snapshot['market']['open_interest']}",
            f"- 5m 主动买入比：{snapshot['market']['taker_buy_ratio_5m']}",
            f"- 5m 主动流向差：{snapshot['market']['aggressive_flow_delta']}",
            f"- 点差(bps)：{snapshot['market']['spread_bps']}",
            f"- 预估滑点(bps)：{snapshot['market']['estimated_slippage_bps_buy']}",
        ]
    )
    (STATE_DIR / "engine_snapshot.md").write_text(markdown, encoding="utf-8")
    heartbeat("engine", status="running", details={"bias": snapshot["signal"]["bias"], "gates_passed": snapshot["signal"]["gates_passed"]})
    return snapshot


def main() -> None:
    if get_runtime_engine_mode() in {"v3_simple", "v4_microstructure"}:
        from btc_engine.runtime.engine_service_v3_simple import main as main_v3

        main_v3()
        return
    parser = argparse.ArgumentParser(description="Run the Leiting BTC public-market engine.")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    ensure_runtime_dirs()
    strategy = load_strategy_config()
    while True:
        snapshot = run_once()
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        if args.once:
            break
        time.sleep(strategy.engine_poll_interval_sec)


if __name__ == "__main__":
    main()
