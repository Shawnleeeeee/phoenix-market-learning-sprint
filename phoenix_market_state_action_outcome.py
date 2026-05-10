import csv
import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone


ROOT = "/opt/phoenix-testnet"
COLLECTOR_HORIZON = os.path.join(
    ROOT, "signal_lab_runs/event_collect_v6_speed_boost/event_horizon_labels.jsonl"
)
SHADOW_OUTCOMES = os.path.join(
    ROOT,
    "signal_lab_runs/vps_forward_shadow_mainnet_active/mainnet_shadow/"
    "signal_bridge_shadow_outcomes.jsonl",
)
TESTNET_DIR = os.path.join(ROOT, "round_runner_reports_req2_telemetry_20260506_162834")
OUT_DIR = os.path.join(
    ROOT,
    "analyst_reports",
    "market_state_action_outcome_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
)
HORIZONS = (60, 180, 300)


def sf(value, default=None):
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except Exception:
        return default


def bps_from_pct(value):
    return (sf(value, 0.0) or 0.0) * 100.0


def bucket_liquidity(quote_volume):
    value = sf(quote_volume, None)
    if value is None:
        return "unknown"
    if value >= 1_000_000_000:
        return "mega"
    if value >= 250_000_000:
        return "major"
    if value >= 50_000_000:
        return "large"
    if value >= 10_000_000:
        return "mid"
    return "small"


def bucket_ret(value, prefix):
    parsed = sf(value, None)
    if parsed is None:
        return prefix + "_unknown"
    if parsed >= 0.50:
        return prefix + "_strong_up"
    if parsed >= 0.15:
        return prefix + "_up"
    if parsed <= -0.50:
        return prefix + "_strong_down"
    if parsed <= -0.15:
        return prefix + "_down"
    return prefix + "_flat"


def regime_label(short_ret, slow_ret, prefix):
    short_value = sf(short_ret, None)
    slow_value = sf(slow_ret, None)
    if short_value is None and slow_value is None:
        return prefix + "_unknown"
    short_value = short_value or 0.0
    slow_value = slow_value or 0.0
    if short_value >= 0.25 and slow_value >= 0.50:
        return prefix + "_risk_on"
    if short_value <= -0.25 and slow_value <= -0.50:
        return prefix + "_risk_off"
    if short_value >= 0.20:
        return prefix + "_short_up"
    if short_value <= -0.20:
        return prefix + "_short_down"
    if slow_value >= 0.60:
        return prefix + "_slow_up"
    if slow_value <= -0.60:
        return prefix + "_slow_down"
    return prefix + "_flat_mixed"


def bucket_oi(value, prefix):
    parsed = sf(value, None)
    if parsed is None:
        return prefix + "_unknown"
    if parsed >= 2.0:
        return prefix + "_build_strong"
    if parsed >= 0.5:
        return prefix + "_build"
    if parsed <= -2.0:
        return prefix + "_unwind_strong"
    if parsed <= -0.5:
        return prefix + "_unwind"
    return prefix + "_flat"


def bucket_volume(value):
    parsed = sf(value, None)
    if parsed is None:
        return "vol_unknown"
    if parsed >= 3.0:
        return "volume_extreme"
    if parsed >= 1.5:
        return "volume_burst"
    if parsed >= 0.8:
        return "volume_normal"
    return "volume_quiet"


def bucket_cost(spread_bps, slippage_bps):
    spread = sf(spread_bps, None)
    slippage = sf(slippage_bps, None)
    if spread is None or slippage is None:
        return "cost_unknown"
    if spread <= 2 and slippage <= 3:
        return "cost_clean"
    if spread <= 5 and slippage <= 5:
        return "cost_ok"
    if spread <= 10 and slippage <= 10:
        return "cost_high"
    return "cost_block"


def bucket_depth(imbalance):
    value = sf(imbalance, None)
    if value is None:
        return "depth_unknown"
    if value >= 0.20:
        return "bid_depth_dominant"
    if value <= -0.20:
        return "ask_depth_dominant"
    return "depth_neutral"


def trigger_family(trigger_types):
    values = sorted(str(item) for item in (trigger_types or []) if item)
    if not values:
        return "trigger_unknown"
    if values == ["baseline_control"]:
        return "baseline_control"
    family = []
    for key in [
        "liquidation",
        "oi_build",
        "oi_unwind",
        "volume_burst",
        "range_expansion",
        "breakout",
        "trend",
        "pullback",
        "reclaim",
    ]:
        if any(key in item for item in values):
            family.append(key)
    return "+".join(family[:3] or values[:2])


def iso_time(ms):
    parsed = sf(ms, None)
    if parsed is None:
        return None
    return datetime.fromtimestamp(parsed / 1000, timezone.utc).isoformat()


def build_events():
    events = {}
    rows = 0
    parse_errors = 0
    with open(COLLECTOR_HORIZON, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows += 1
            try:
                raw = json.loads(line)
            except Exception:
                parse_errors += 1
                continue
            event_id = raw.get("event_id") or f"unknown-{rows}"
            event = events.get(event_id)
            if event is None:
                sample = raw.get("sample") or {}
                enrich = raw.get("enrichments") or {}
                event = {
                    "event_id": event_id,
                    "timestamp": raw.get("observed_at") or iso_time(raw.get("observed_at_ms")),
                    "observed_at_ms": raw.get("observed_at_ms"),
                    "symbol": raw.get("symbol") or sample.get("symbol") or "UNKNOWN",
                    "bar_interval": raw.get("bar_interval") or sample.get("bar_interval"),
                    "trading_session": raw.get("trading_session"),
                    "trigger_types": raw.get("trigger_types") or sample.get("trigger_types") or [],
                    "trigger_score": raw.get("trigger_score") or sample.get("trigger_score"),
                    "sample": sample,
                    "enrich": enrich,
                    "horizons": {},
                }
                events[event_id] = event
            horizon = raw.get("horizon") or {}
            horizon_sec = int(sf(horizon.get("horizon_sec"), 0) or 0)
            if not horizon_sec:
                continue
            close_pct = sf(horizon.get("close_return_pct"), 0.0) or 0.0
            after_fee_pct = sf(horizon.get("after_fee_return_pct"), None)
            fee_bps = 8.0
            if after_fee_pct is not None:
                fee_bps = max(0.0, (close_pct - after_fee_pct) * 100.0) or 8.0
            spread_bps = max(0.0, sf(event["enrich"].get("spread_bps"), 0.0) or 0.0)
            slippage_bps = max(
                0.0, sf(event["enrich"].get("estimated_slippage_bps"), 0.0) or 0.0
            )
            funding_rate = sf(event["enrich"].get("funding_rate"), 0.0) or 0.0
            funding_bps = abs(funding_rate) * 10000.0 * (horizon_sec / 28800.0)
            total_cost_bps = fee_bps + spread_bps + slippage_bps + funding_bps
            gross_long_bps = close_pct * 100.0
            gross_short_bps = -gross_long_bps
            event["horizons"][horizon_sec] = {
                "price_return_bps": gross_long_bps,
                "fee_bps": fee_bps,
                "funding_bps": funding_bps,
                "total_cost_bps": total_cost_bps,
                "long_net_bps": gross_long_bps - total_cost_bps,
                "short_net_bps": gross_short_bps - total_cost_bps,
                "long_gross_bps": gross_long_bps,
                "short_gross_bps": gross_short_bps,
                "long_mfe_bps": bps_from_pct(horizon.get("max_runup_pct")),
                "short_mfe_bps": max(0.0, -bps_from_pct(horizon.get("max_drawdown_pct"))),
            }
    return events, rows, parse_errors


def decision_row(event):
    sample = event["sample"]
    enrich = event["enrich"]
    quote_volume = sample.get("quote_volume_24h")
    liquidity_bucket = bucket_liquidity(quote_volume)
    spread_bps = sf(enrich.get("spread_bps"), None)
    slippage_bps = sf(enrich.get("estimated_slippage_bps"), None)
    bid_depth = sf(enrich.get("bid_depth_notional_5"), None)
    ask_depth = sf(enrich.get("ask_depth_notional_5"), None)
    total_depth = (bid_depth or 0.0) + (ask_depth or 0.0)
    bidwall_strength = (bid_depth / total_depth) if total_depth > 0 else None
    askwall_strength = (ask_depth / total_depth) if total_depth > 0 else None
    bar_interval = event.get("bar_interval")
    volume_delta = sf(sample.get("volume_burst_ratio"), None)
    volume_delta_value = (volume_delta - 1.0) if volume_delta is not None else None
    volume_delta_1m = volume_delta_value if bar_interval == "1m" else None
    volume_delta_5m = volume_delta_value if bar_interval == "5m" else None
    volatility = sf(sample.get("atr_20_pct"), None)
    volatility_1m = volatility if bar_interval == "1m" else None
    volatility_5m = volatility if bar_interval == "5m" else None
    btc_regime = regime_label(
        enrich.get("btcusdt_ret_5m_pct"), enrich.get("btcusdt_ret_60m_pct"), "btc"
    )
    eth_regime = regime_label(
        enrich.get("ethusdt_ret_5m_pct"), enrich.get("ethusdt_ret_60m_pct"), "eth"
    )
    cost_bucket = bucket_cost(spread_bps, slippage_bps)
    state_label = "|".join(
        [
            str(bar_interval or "bar_unknown"),
            liquidity_bucket,
            cost_bucket,
            bucket_ret(sample.get("ret_5bar_pct"), "mom5"),
            bucket_ret(sample.get("ret_15bar_pct"), "mom15"),
            bucket_volume(sample.get("volume_burst_ratio")),
            bucket_oi(enrich.get("oi_change_5m_pct"), "oi5"),
            bucket_depth(enrich.get("depth_imbalance")),
            btc_regime,
            eth_regime,
        ]
    )
    horizon_300 = event["horizons"].get(300) or {}
    funding_bps = horizon_300.get("funding_bps")
    recent_liquidation_pressure = (
        1.0 if "liquidation" in trigger_family(event.get("trigger_types")) else 0.0
    )
    row = {
        "timestamp": event.get("timestamp"),
        "event_id": event.get("event_id"),
        "symbol": event.get("symbol"),
        "btc_regime": btc_regime,
        "eth_regime": eth_regime,
        "symbol_liquidity_bucket": liquidity_bucket,
        "spread_bps": spread_bps,
        "estimated_slippage_bps": slippage_bps,
        "funding_bps": funding_bps,
        "volume_delta_1m": volume_delta_1m,
        "volume_delta_5m": volume_delta_5m,
        "price_return_30s_bps": None,
        "price_return_60s_bps": (event["horizons"].get(60) or {}).get("price_return_bps"),
        "price_return_180s_bps": (event["horizons"].get(180) or {}).get("price_return_bps"),
        "price_return_300s_bps": (event["horizons"].get(300) or {}).get("price_return_bps"),
        "volatility_1m": volatility_1m,
        "volatility_5m": volatility_5m,
        "volatility_15m": None,
        "oi_delta_1m": None,
        "oi_delta_5m": sf(enrich.get("oi_change_5m_pct"), None),
        "oi_delta_15m": sf(enrich.get("oi_change_15m_pct"), None),
        "orderbook_imbalance": sf(enrich.get("depth_imbalance"), None),
        "bidwall_strength": bidwall_strength,
        "askwall_strength": askwall_strength,
        "recent_liquidation_pressure": recent_liquidation_pressure,
        "current_market_state_label": state_label,
        "action_long_outcome_net_bps": horizon_300.get("long_net_bps"),
        "action_short_outcome_net_bps": horizon_300.get("short_net_bps"),
        "no_trade_baseline": 0.0,
        "total_cost_bps": horizon_300.get("total_cost_bps"),
        "bar_interval": bar_interval,
        "trading_session": event.get("trading_session"),
        "trigger_family": trigger_family(event.get("trigger_types")),
    }
    return row


class Agg:
    def __init__(self):
        self.n = 0
        self.net = 0.0
        self.gross = 0.0
        self.cost = 0.0
        self.wins = 0
        self.pos = 0.0
        self.neg = 0.0
        self.fakeouts = 0
        self.symbol_net = defaultdict(float)
        self.symbol_pos = defaultdict(float)
        self.liquidity = Counter()
        self.regime = Counter()
        self.regime_pos = defaultdict(float)
        self.returns = []

    def add(self, event, action, horizon_sec):
        horizon = event["horizons"].get(horizon_sec)
        if not horizon:
            return
        row = decision_row(event)
        net = horizon["long_net_bps"] if action == "LONG" else horizon["short_net_bps"]
        gross = horizon["long_gross_bps"] if action == "LONG" else horizon["short_gross_bps"]
        mfe = horizon["long_mfe_bps"] if action == "LONG" else horizon["short_mfe_bps"]
        self.n += 1
        self.net += net
        self.gross += gross
        self.cost += horizon["total_cost_bps"]
        if net > 0:
            self.wins += 1
            self.pos += net
            self.symbol_pos[event["symbol"]] += net
            self.regime_pos[row["btc_regime"] + "/" + row["eth_regime"]] += net
        else:
            self.neg += net
        if net < 0 and mfe < max(horizon["total_cost_bps"], 1.0):
            self.fakeouts += 1
        self.symbol_net[event["symbol"]] += net
        self.liquidity[row["symbol_liquidity_bucket"]] += 1
        self.regime[row["btc_regime"] + "/" + row["eth_regime"]] += 1
        self.returns.append(net)


def max_consecutive_losses(values):
    best = current = 0
    for value in values:
        if value <= 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def max_drawdown(values):
    equity = peak = drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return drawdown


def concentration(pos_map, total_pos):
    if total_pos <= 0:
        return 1.0
    return max(pos_map.values() or [0.0]) / total_pos


def agg_row(template, state, action, horizon_sec, agg):
    avg_net = agg.net / agg.n if agg.n else 0.0
    avg_gross = agg.gross / agg.n if agg.n else 0.0
    avg_cost = agg.cost / agg.n if agg.n else 0.0
    gross_cost_ratio = avg_gross / avg_cost if avg_cost > 0 else 0.0
    profit_factor = agg.pos / abs(agg.neg) if agg.neg < 0 else (999.0 if agg.pos > 0 else 0.0)
    return {
        "template": template,
        "market_state": state,
        "action": action,
        "exit_window_sec": horizon_sec,
        "unique_samples": agg.n,
        "net_bps_sum": round(agg.net, 4),
        "avg_net_bps": round(avg_net, 4),
        "avg_gross_bps": round(avg_gross, 4),
        "avg_cost_bps": round(avg_cost, 4),
        "gross_cost_ratio": round(gross_cost_ratio, 4),
        "win_rate": round(agg.wins / agg.n, 4) if agg.n else 0.0,
        "profit_factor": round(profit_factor, 4),
        "fakeout_rate": round(agg.fakeouts / agg.n, 4) if agg.n else 0.0,
        "top_symbol_concentration": round(concentration(agg.symbol_pos, agg.pos), 4),
        "top_symbol": max(agg.symbol_net, key=agg.symbol_net.get) if agg.symbol_net else None,
        "dominant_liquidity_bucket": agg.liquidity.most_common(1)[0][0] if agg.liquidity else None,
        "dominant_regime": agg.regime.most_common(1)[0][0] if agg.regime else None,
        "dominant_regime_share": round(agg.regime.most_common(1)[0][1] / agg.n, 4)
        if agg.regime and agg.n
        else 0.0,
        "top_regime_profit_concentration": round(concentration(agg.regime_pos, agg.pos), 4),
        "max_consecutive_losses": max_consecutive_losses(agg.returns),
        "max_drawdown_bps": round(max_drawdown(agg.returns), 4),
    }


def state_templates(row):
    parts = (row["current_market_state_label"] or "").split("|")
    while len(parts) < 10:
        parts.append("unknown")
    bar, liquidity, cost, mom5, mom15, volume, oi5, depth, btc, eth = parts[:10]
    return {
        "full_state": row["current_market_state_label"],
        "cost_momentum": "|".join(
            [
                bar,
                liquidity,
                cost,
                mom5,
                mom15,
                volume,
                btc,
                eth,
            ]
        ),
        "flow_micro": "|".join(
            [
                bar,
                liquidity,
                oi5,
                depth,
                volume,
                mom5,
            ]
        ),
        "trigger_cost": "|".join(
            [
                str(row["bar_interval"]),
                row["trigger_family"],
                row["symbol_liquidity_bucket"],
                bucket_cost(row["spread_bps"], row["estimated_slippage_bps"]),
            ]
        ),
        "macro_momentum": "|".join(
            [
                bar,
                liquidity,
                btc,
                eth,
                mom5,
                mom15,
            ]
        ),
    }


def build_aggregates(events):
    aggs = defaultdict(Agg)
    no_trade_pairs = {}
    for event in events.values():
        row = decision_row(event)
        templates = state_templates(row)
        for horizon_sec in HORIZONS:
            if horizon_sec not in event["horizons"]:
                continue
            for template, state in templates.items():
                for action in ("LONG", "SHORT"):
                    key = (template, state, action, horizon_sec)
                    aggs[key].add(event, action, horizon_sec)
    rows = []
    for key, agg in aggs.items():
        if agg.n >= 50:
            rows.append(agg_row(key[0], key[1], key[2], key[3], agg))
    rows.sort(key=lambda item: (item["avg_net_bps"], item["profit_factor"]), reverse=True)
    for row in rows:
        pair_key = (row["template"], row["market_state"], row["exit_window_sec"])
        no_trade_pairs.setdefault(pair_key, {})[row["action"]] = row
    no_trade_states = []
    for (_, _, _), pair in no_trade_pairs.items():
        long_row = pair.get("LONG")
        short_row = pair.get("SHORT")
        if long_row and short_row and long_row["avg_net_bps"] < 0 and short_row["avg_net_bps"] < 0:
            worst = min(long_row["avg_net_bps"], short_row["avg_net_bps"])
            if long_row["unique_samples"] >= 100 and short_row["unique_samples"] >= 100:
                no_trade_states.append(
                    {
                        "template": long_row["template"],
                        "market_state": long_row["market_state"],
                        "exit_window_sec": long_row["exit_window_sec"],
                        "long_avg_net_bps": long_row["avg_net_bps"],
                        "short_avg_net_bps": short_row["avg_net_bps"],
                        "unique_samples": min(long_row["unique_samples"], short_row["unique_samples"]),
                        "worst_side_avg_net_bps": worst,
                    }
                )
    no_trade_states.sort(key=lambda item: item["worst_side_avg_net_bps"])
    return rows, no_trade_states


def summarize_shadow():
    if not os.path.exists(SHADOW_OUTCOMES):
        return {"rows": 0}
    total = 0
    by_horizon = Counter()
    positives = []
    with open(SHADOW_OUTCOMES, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            total += 1
            horizon_sec = int(sf(row.get("horizon_sec"), 0) or 0)
            if horizon_sec in HORIZONS:
                by_horizon[horizon_sec] += 1
                gross = bps_from_pct(row.get("close_return_pct"))
                cost = (
                    (sf(row.get("round_trip_fee_bps"), 8.0) or 8.0)
                    + (sf(row.get("spread_bps_at_entry"), 0.0) or 0.0)
                    + (sf(row.get("estimated_slippage_bps"), 0.0) or 0.0)
                    + abs(sf(row.get("funding_paid_during_hold"), 0.0) or 0.0) * 100.0
                )
                positives.append(gross - cost)
    return {
        "rows": total,
        "target_horizon_rows": dict(by_horizon),
        "avg_full_cost_net_bps_target_horizons": round(sum(positives) / len(positives), 4)
        if positives
        else None,
    }


def summarize_testnet():
    rows = 0
    net = 0.0
    gross = 0.0
    for name in sorted(os.listdir(TESTNET_DIR)) if os.path.isdir(TESTNET_DIR) else []:
        if not name.endswith("_trades.jsonl"):
            continue
        with open(os.path.join(TESTNET_DIR, name), "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                rows += 1
                net += sf(row.get("net_pnl_usdt"), 0.0) or 0.0
                gross += sf(row.get("gross_pnl_usdt"), 0.0) or 0.0
    return {"rows": rows, "net_pnl_usdt": round(net, 4), "gross_pnl_usdt": round(gross, 4)}


def write_outputs(events, aggregate_rows, no_trade_states, meta):
    os.makedirs(OUT_DIR, exist_ok=True)
    sample_fields = [
        "timestamp",
        "event_id",
        "symbol",
        "btc_regime",
        "eth_regime",
        "symbol_liquidity_bucket",
        "spread_bps",
        "estimated_slippage_bps",
        "funding_bps",
        "volume_delta_1m",
        "volume_delta_5m",
        "price_return_30s_bps",
        "price_return_60s_bps",
        "price_return_180s_bps",
        "price_return_300s_bps",
        "volatility_1m",
        "volatility_5m",
        "volatility_15m",
        "oi_delta_1m",
        "oi_delta_5m",
        "oi_delta_15m",
        "orderbook_imbalance",
        "bidwall_strength",
        "askwall_strength",
        "recent_liquidation_pressure",
        "current_market_state_label",
        "action_long_outcome_net_bps",
        "action_short_outcome_net_bps",
        "no_trade_baseline",
        "total_cost_bps",
        "bar_interval",
        "trading_session",
        "trigger_family",
    ]
    sample_path = os.path.join(OUT_DIR, "decision_moment_sample_table.csv")
    with open(sample_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=sample_fields)
        writer.writeheader()
        for event in events.values():
            writer.writerow(decision_row(event))

    aggregate_path = os.path.join(OUT_DIR, "state_action_outcome_aggregate.csv")
    aggregate_fields = list(aggregate_rows[0].keys()) if aggregate_rows else []
    with open(aggregate_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=aggregate_fields)
        writer.writeheader()
        for row in aggregate_rows:
            writer.writerow(row)

    candidates = [
        row
        for row in aggregate_rows
        if row["unique_samples"] >= 50
        and row["avg_net_bps"] > 0
        and row["net_bps_sum"] > 0
        and row["gross_cost_ratio"] >= 3.0
        and row["top_symbol_concentration"] <= 0.35
        and row["top_regime_profit_concentration"] <= 0.70
    ]
    candidates.sort(
        key=lambda item: (
            item["unique_samples"] >= 100,
            item["avg_net_bps"],
            item["gross_cost_ratio"],
            -item["top_symbol_concentration"],
        ),
        reverse=True,
    )
    cost_eaten = [
        row
        for row in aggregate_rows
        if row["unique_samples"] >= 100
        and row["avg_gross_bps"] > 0
        and row["avg_net_bps"] <= 0
    ]
    cost_eaten.sort(key=lambda item: item["avg_gross_bps"], reverse=True)
    fakeout_high = [row for row in aggregate_rows if row["unique_samples"] >= 100]
    fakeout_high.sort(key=lambda item: item["fakeout_rate"], reverse=True)
    losing_liquidity = defaultdict(lambda: {"n": 0, "long": 0.0, "short": 0.0})
    losing_symbol = defaultdict(lambda: {"n": 0, "long": 0.0, "short": 0.0})
    for event in events.values():
        horizon = event["horizons"].get(300)
        if not horizon:
            continue
        row = decision_row(event)
        losing_liquidity[row["symbol_liquidity_bucket"]]["n"] += 1
        losing_liquidity[row["symbol_liquidity_bucket"]]["long"] += horizon["long_net_bps"]
        losing_liquidity[row["symbol_liquidity_bucket"]]["short"] += horizon["short_net_bps"]
        losing_symbol[event["symbol"]]["n"] += 1
        losing_symbol[event["symbol"]]["long"] += horizon["long_net_bps"]
        losing_symbol[event["symbol"]]["short"] += horizon["short_net_bps"]

    def avg_loss_rows(mapping):
        output = []
        for key, value in mapping.items():
            if value["n"] < 50:
                continue
            output.append(
                {
                    "key": key,
                    "n": value["n"],
                    "avg_long_net_bps": round(value["long"] / value["n"], 4),
                    "avg_short_net_bps": round(value["short"] / value["n"], 4),
                }
            )
        output.sort(key=lambda item: min(item["avg_long_net_bps"], item["avg_short_net_bps"]))
        return output

    summary = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_policy": "collector/shadow/testnet realtime-forward data only; no backtest PnL",
        "output_dir": OUT_DIR,
        "decision_moment_sample_table": sample_path,
        "state_action_outcome_aggregate": aggregate_path,
        "collector": meta,
        "shadow": summarize_shadow(),
        "testnet": summarize_testnet(),
        "candidate_count": len(candidates),
        "shadow_candidate_candidates": candidates[:10],
        "cost_eaten_states": cost_eaten[:10],
        "highest_fakeout_states": fakeout_high[:10],
        "worst_liquidity_buckets": avg_loss_rows(losing_liquidity),
        "worst_symbols": avg_loss_rows(losing_symbol)[:20],
        "no_trade_states": no_trade_states[:20],
        "data_gaps": {
            "price_return_30s": "missing in current collector horizon labels",
            "volatility_15m": "not native in current collector label rows; 1m/5m ATR proxy exists by bar interval",
            "oi_delta_1m": "missing; oi_delta_5m and oi_delta_15m exist",
            "bidwall_strength_askwall_strength": "proxy from top-5 depth notional share, not true wall persistence",
            "recent_liquidation_pressure": "proxy from trigger family only; no stable liquidation feed column in collector rows",
        },
    }
    with open(os.path.join(OUT_DIR, "market_state_decision_summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    report_lines = [
        "# Market State -> Action -> Outcome Decision",
        "",
        "【老板判断区】",
    ]
    if candidates:
        verdict = "A. 有新 candidate，可以进入 shadow candidate；不申请 testnet。"
        status = "黄"
    else:
        verdict = "B. 没有发现满足硬条件的 edge。"
        status = "红/黄"
    report_lines.append(f"1. 结论：{verdict}")
    report_lines.append(f"2. 状态：{status}。")
    report_lines.append("3. 是否真进步：证据不足；这是市场状态学习，不是实盘赚钱证据。")
    report_lines.append("4. 是否可以 testnet：不可以。是否可以 mainnet：不可以。")
    report_lines.append("")
    report_lines.append("## 样本表")
    report_lines.append(
        f"- 已建立：{sample_path}，一行一个 decision moment，共 {len(events)} 行。"
    )
    report_lines.append(f"- 聚合表：{aggregate_path}。")
    report_lines.append(
        "- 缺口：30s outcome、OI 1m、真实 bidwall/askwall persistence、稳定 liquidation pressure、HMM regime。"
    )
    report_lines.append("")
    report_lines.append("## 做多/做空正期望状态")
    if candidates:
        for index, row in enumerate(candidates[:3], 1):
            report_lines.append(
                f"{index}. {row['action']} {row['exit_window_sec']}s | {row['market_state']} | "
                f"samples {row['unique_samples']} | avg net {row['avg_net_bps']} bps | "
                f"gross/cost {row['gross_cost_ratio']} | concentration {row['top_symbol_concentration']} | "
                f"fakeout {row['fakeout_rate']}。"
            )
    else:
        report_lines.append("- 无组合同时满足：samples >= 50、net > 0、gross/cost >= 3、symbol concentration <= 35%、不靠单一 regime。")
    report_lines.append("")
    report_lines.append("## 成本吃掉 edge")
    for row in cost_eaten[:3]:
        report_lines.append(
            f"- {row['action']} {row['exit_window_sec']}s | {row['market_state']} | "
            f"gross {row['avg_gross_bps']} bps，但 net {row['avg_net_bps']} bps，成本 {row['avg_cost_bps']} bps。"
        )
    report_lines.append("")
    report_lines.append("## 永远 no trade / 高 fakeout")
    for row in no_trade_states[:3]:
        report_lines.append(
            f"- NO_TRADE {row['exit_window_sec']}s | {row['market_state']} | "
            f"long {row['long_avg_net_bps']} bps，short {row['short_avg_net_bps']} bps。"
        )
    for row in fakeout_high[:2]:
        report_lines.append(
            f"- fakeout 高：{row['action']} {row['exit_window_sec']}s | {row['market_state']} | "
            f"fakeout {row['fakeout_rate']}，samples {row['unique_samples']}。"
        )
    report_lines.append("")
    report_lines.append("## 最终")
    report_lines.append(verdict)
    with open(os.path.join(OUT_DIR, "market_state_decision_report.md"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(report_lines) + "\n")
    return summary


def main():
    events, raw_rows, parse_errors = build_events()
    aggregate_rows, no_trade_states = build_aggregates(events)
    meta = {
        "raw_horizon_rows": raw_rows,
        "parse_errors": parse_errors,
        "decision_moments": len(events),
    }
    summary = write_outputs(events, aggregate_rows, no_trade_states, meta)
    print(
        json.dumps(
            {
                "output_dir": OUT_DIR,
                "decision_moments": len(events),
                "candidate_count": summary["candidate_count"],
                "top_candidates": summary["shadow_candidate_candidates"][:3],
                "data_gaps": summary["data_gaps"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
