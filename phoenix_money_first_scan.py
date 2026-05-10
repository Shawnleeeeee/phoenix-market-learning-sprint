import csv
import glob
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
SHADOW_SIGNALS = os.path.join(
    ROOT,
    "signal_lab_runs/vps_forward_shadow_mainnet_active/mainnet_shadow/"
    "signal_bridge_shadow_signals.jsonl",
)
TESTNET_GLOB = os.path.join(
    ROOT, "round_runner_reports_req2_telemetry_20260506_162834/round_*_trades.jsonl"
)
OUT_DIR = os.path.join(
    ROOT,
    "analyst_reports",
    "money_first_market_learning_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
)
TARGET_HORIZONS = {30, 60, 180, 300, 900}


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


def pct_to_bps(value):
    return (sf(value, 0.0) or 0.0) * 100.0


def bucket_liq(quote_volume):
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


def bucket_shadow_liq(value):
    return str(value or "unknown").lower()


def bucket_cost(spread, slippage):
    spread_value = sf(spread, None)
    slippage_value = sf(slippage, None)
    if spread_value is None or slippage_value is None:
        return "cost_unknown"
    if spread_value <= 2 and slippage_value <= 3:
        return "cost_clean"
    if spread_value <= 5 and slippage_value <= 5:
        return "cost_ok"
    if spread_value <= 10 and slippage_value <= 10:
        return "cost_high"
    return "cost_block"


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


def bucket_regime(value_5m, value_60m, prefix):
    short = sf(value_5m, None)
    slow = sf(value_60m, None)
    if short is None and slow is None:
        return prefix + "_unknown"
    short = short or 0.0
    slow = slow or 0.0
    if short >= 0.25 and slow >= 0.50:
        return prefix + "_risk_on"
    if short <= -0.25 and slow <= -0.50:
        return prefix + "_risk_off"
    if short >= 0.20:
        return prefix + "_short_up"
    if short <= -0.20:
        return prefix + "_short_down"
    if slow >= 0.60:
        return prefix + "_slow_up"
    if slow <= -0.60:
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
        return "vol_extreme_burst"
    if parsed >= 1.5:
        return "vol_burst"
    if parsed >= 0.8:
        return "vol_normal"
    return "vol_quiet"


def bucket_depth(value):
    parsed = sf(value, None)
    if parsed is None:
        return "depth_unknown"
    if parsed >= 0.20:
        return "bid_depth_dominant"
    if parsed <= -0.20:
        return "ask_depth_dominant"
    return "depth_neutral"


def trigger_family(types):
    values = sorted(str(item) for item in (types or []) if item)
    value_set = set(values)
    if not values:
        return "trigger_unknown"
    if value_set == {"baseline_control"}:
        return "baseline_control"
    family = []
    for key in [
        "oi_build",
        "oi_unwind",
        "liquidation",
        "volume_burst",
        "range_expansion",
        "breakout",
        "reclaim",
        "trend",
        "pullback",
        "volatility",
    ]:
        if any(key in item for item in values):
            family.append(key)
    if not family:
        family = values[:2]
    return "+".join(family[:3])


def safe_time(ms):
    parsed = sf(ms, None)
    if parsed is None:
        return None
    try:
        return datetime.fromtimestamp(parsed / 1000, timezone.utc).isoformat()
    except Exception:
        return None


class Agg:
    def __init__(self):
        self.n = 0
        self.sum_net = 0.0
        self.sum_gross = 0.0
        self.sum_cost = 0.0
        self.sum_fee = 0.0
        self.sum_spread = 0.0
        self.sum_slip = 0.0
        self.sum_funding = 0.0
        self.wins = 0
        self.pos = 0.0
        self.neg = 0.0
        self.symbols = defaultdict(float)
        self.symbol_pos = defaultdict(float)
        self.liqs = Counter()
        self.regimes = Counter()
        self.sessions = Counter()
        self.sum_mfe = 0.0
        self.fakeout = 0
        self.returns = []

    def add(
        self,
        symbol,
        liquidity,
        regime,
        session,
        net,
        gross,
        cost,
        fee,
        spread,
        slippage,
        funding,
        mfe,
    ):
        self.n += 1
        self.sum_net += net
        self.sum_gross += gross
        self.sum_cost += cost
        self.sum_fee += fee
        self.sum_spread += spread
        self.sum_slip += slippage
        self.sum_funding += funding
        if net > 0:
            self.wins += 1
            self.pos += net
            self.symbol_pos[symbol] += net
        else:
            self.neg += net
        self.symbols[symbol] += net
        self.liqs[liquidity] += 1
        self.regimes[regime] += 1
        self.sessions[session] += 1
        self.sum_mfe += mfe
        if net < 0 and mfe < max(cost, 1.0):
            self.fakeout += 1
        self.returns.append(net)


def profit_factor(agg):
    if agg.neg < 0:
        return agg.pos / abs(agg.neg)
    return 999.0 if agg.pos > 0 else 0.0


def max_consecutive_losses(values):
    max_losses = 0
    current = 0
    for value in values:
        if value <= 0:
            current += 1
            max_losses = max(max_losses, current)
        else:
            current = 0
    return max_losses


def max_drawdown(values):
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return drawdown


def top_symbol_concentration(agg):
    if agg.pos <= 0:
        return 1.0 if agg.n else 0.0
    return max(agg.symbol_pos.values() or [0.0]) / agg.pos


def most_common(counter):
    return counter.most_common(1)[0][0] if counter else "unknown"


def row_from_agg(key, agg, source):
    template, action, horizon = key[0], key[1], key[2]
    state = " | ".join(str(item) for item in key[3:])
    avg_net = agg.sum_net / agg.n if agg.n else 0.0
    avg_gross = agg.sum_gross / agg.n if agg.n else 0.0
    avg_cost = agg.sum_cost / agg.n if agg.n else 0.0
    ratio = avg_gross / avg_cost if avg_cost > 0 else 0.0
    return {
        "source": source,
        "template": template,
        "action": action,
        "horizon_sec": horizon,
        "state_rule": state,
        "unique_signals": agg.n,
        "net_return_bps_sum": round(agg.sum_net, 4),
        "avg_net_bps": round(avg_net, 4),
        "expected_net_edge_bps": round(avg_net, 4),
        "avg_gross_move_bps": round(avg_gross, 4),
        "avg_total_cost_bps": round(avg_cost, 4),
        "gross_cost_ratio": round(ratio, 4),
        "win_rate": round(agg.wins / agg.n, 4) if agg.n else 0.0,
        "profit_factor": round(profit_factor(agg), 4),
        "fakeout_rate": round(agg.fakeout / agg.n, 4) if agg.n else 0.0,
        "avg_fee_bps": round(agg.sum_fee / agg.n, 4) if agg.n else 0.0,
        "avg_spread_bps": round(agg.sum_spread / agg.n, 4) if agg.n else 0.0,
        "avg_slippage_bps": round(agg.sum_slip / agg.n, 4) if agg.n else 0.0,
        "avg_funding_bps": round(agg.sum_funding / agg.n, 6) if agg.n else 0.0,
        "top_symbol_profit_concentration": round(top_symbol_concentration(agg), 4),
        "top_symbol_by_net": most_common(Counter({k: v for k, v in agg.symbols.items()})),
        "dominant_liquidity_bucket": most_common(agg.liqs),
        "dominant_regime": most_common(agg.regimes),
        "dominant_session": most_common(agg.sessions),
        "max_consecutive_losses": max_consecutive_losses(agg.returns),
        "max_drawdown_bps": round(max_drawdown(agg.returns), 4),
    }


def load_collector_events():
    events = {}
    line_count = 0
    parse_errors = 0
    first_ms = None
    last_ms = None
    horizon_counts = Counter()
    with open(COLLECTOR_HORIZON, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            line_count += 1
            try:
                row = json.loads(line)
            except Exception:
                parse_errors += 1
                continue
            event_id = row.get("event_id") or f"anon-{line_count}"
            observed_ms = sf(row.get("observed_at_ms"), None)
            if observed_ms is not None:
                first_ms = observed_ms if first_ms is None else min(first_ms, observed_ms)
                last_ms = observed_ms if last_ms is None else max(last_ms, observed_ms)
            event = events.get(event_id)
            if event is None:
                sample = row.get("sample") or {}
                event = {
                    "event_id": event_id,
                    "symbol": row.get("symbol") or sample.get("symbol") or "UNKNOWN",
                    "observed_at_ms": row.get("observed_at_ms"),
                    "trading_session": row.get("trading_session") or "unknown_session",
                    "bar_interval": row.get("bar_interval") or sample.get("bar_interval") or "unknown_bar",
                    "trigger_types": row.get("trigger_types") or sample.get("trigger_types") or [],
                    "sample": sample,
                    "enrich": row.get("enrichments") or {},
                    "horizons": {},
                }
                events[event_id] = event
            horizon = row.get("horizon") or {}
            horizon_sec = int(sf(horizon.get("horizon_sec"), 0) or 0)
            if not horizon_sec:
                continue
            horizon_counts[horizon_sec] += 1
            close_pct = sf(horizon.get("close_return_pct"), 0.0) or 0.0
            after_fee_pct = sf(horizon.get("after_fee_return_pct"), None)
            fee_bps = 8.0
            if after_fee_pct is not None:
                fee_bps = max(0.0, (close_pct - after_fee_pct) * 100.0)
                if fee_bps == 0.0:
                    fee_bps = 8.0
            spread_bps = max(0.0, sf(event["enrich"].get("spread_bps"), 0.0) or 0.0)
            slippage_bps = max(0.0, sf(event["enrich"].get("estimated_slippage_bps"), 0.0) or 0.0)
            funding_rate = sf(event["enrich"].get("funding_rate"), 0.0) or 0.0
            funding_bps = abs(funding_rate) * 10000.0 * (horizon_sec / 28800.0)
            event["horizons"][horizon_sec] = {
                "close_bps": close_pct * 100.0,
                "mfe_long_bps": pct_to_bps(horizon.get("max_runup_pct")),
                "mae_long_bps": pct_to_bps(horizon.get("max_drawdown_pct")),
                "fee_bps": fee_bps,
                "spread_bps": spread_bps,
                "slip_bps": slippage_bps,
                "funding_bps": funding_bps,
                "cost_bps": fee_bps + spread_bps + slippage_bps + funding_bps,
            }
    return events, line_count, parse_errors, first_ms, last_ms, horizon_counts


TEMPLATES = [
    ("trigger_liq", ["bar", "trigger_family", "liq"]),
    ("trigger_liq_cost", ["bar", "trigger_family", "liq", "cost_bucket"]),
    ("flow_oi_mom", ["bar", "liq", "oi5", "mom5", "volburst"]),
    ("btc_eth_mom", ["bar", "liq", "mom15", "btc5", "eth5"]),
    ("depth_oi_mom", ["bar", "liq", "depth", "oi5", "mom5"]),
    ("session_trigger", ["bar", "session", "trigger_family", "liq"]),
    ("oi_vol_regime", ["bar", "liq", "oi15", "volburst", "btc60"]),
    ("cost_momentum", ["bar", "liq", "cost_bucket", "mom5", "volburst"]),
]


def feature_map(event):
    sample = event["sample"]
    enrich = event["enrich"]
    quote_volume = sample.get("quote_volume_24h") or enrich.get("quote_volume_24h")
    return {
        "bar": str(event.get("bar_interval") or "unknown_bar"),
        "session": str(event.get("trading_session") or "unknown_session"),
        "trigger_family": trigger_family(event.get("trigger_types")),
        "liq": bucket_liq(quote_volume),
        "cost_bucket": bucket_cost(enrich.get("spread_bps"), enrich.get("estimated_slippage_bps")),
        "mom5": bucket_ret(sample.get("ret_5bar_pct"), "mom5"),
        "mom15": bucket_ret(sample.get("ret_15bar_pct"), "mom15"),
        "btc5": bucket_ret(enrich.get("btcusdt_ret_5m_pct"), "btc5"),
        "btc60": bucket_ret(enrich.get("btcusdt_ret_60m_pct"), "btc60"),
        "eth5": bucket_ret(enrich.get("ethusdt_ret_5m_pct"), "eth5"),
        "btc_regime": bucket_regime(
            enrich.get("btcusdt_ret_5m_pct"), enrich.get("btcusdt_ret_60m_pct"), "btc"
        ),
        "eth_regime": bucket_regime(
            enrich.get("ethusdt_ret_5m_pct"), enrich.get("ethusdt_ret_60m_pct"), "eth"
        ),
        "oi5": bucket_oi(enrich.get("oi_change_5m_pct"), "oi5"),
        "oi15": bucket_oi(enrich.get("oi_change_15m_pct"), "oi15"),
        "volburst": bucket_volume(sample.get("volume_burst_ratio")),
        "depth": bucket_depth(enrich.get("depth_imbalance")),
    }


def build_collector_aggs(events):
    aggs = defaultdict(Agg)
    action_summary = defaultdict(Agg)
    for event in events.values():
        features = feature_map(event)
        regime = features["btc_regime"] + "/" + features["eth_regime"]
        liquidity = features["liq"]
        session = features["session"]
        symbol = event["symbol"]
        for horizon_sec, horizon in event["horizons"].items():
            if horizon_sec not in TARGET_HORIZONS:
                continue
            for action in ("LONG", "SHORT"):
                gross = horizon["close_bps"] if action == "LONG" else -horizon["close_bps"]
                mfe = (
                    horizon["mfe_long_bps"]
                    if action == "LONG"
                    else max(0.0, -horizon["mae_long_bps"])
                )
                net = gross - horizon["cost_bps"]
                summary_key = ("collector_action", action, horizon_sec, "ALL")
                action_summary[summary_key].add(
                    symbol,
                    liquidity,
                    regime,
                    session,
                    net,
                    gross,
                    horizon["cost_bps"],
                    horizon["fee_bps"],
                    horizon["spread_bps"],
                    horizon["slip_bps"],
                    horizon["funding_bps"],
                    mfe,
                )
                for template_name, dims in TEMPLATES:
                    key = (template_name, action, horizon_sec) + tuple(features[dim] for dim in dims)
                    aggs[key].add(
                        symbol,
                        liquidity,
                        regime,
                        session,
                        net,
                        gross,
                        horizon["cost_bps"],
                        horizon["fee_bps"],
                        horizon["spread_bps"],
                        horizon["slip_bps"],
                        horizon["funding_bps"],
                        mfe,
                    )
    return aggs, action_summary


def candidate_match(event, key):
    dims = dict(TEMPLATES)[key[0]]
    features = feature_map(event)
    return tuple(features[dim] for dim in dims) == tuple(key[3:])


def followthrough_for_key(events, key):
    action = key[1]
    target_horizon = key[2]
    followthrough = {30: [0, 0], 60: [0, 0], 180: [0, 0], 300: [0, 0]}
    for event in events.values():
        if target_horizon not in event["horizons"] or not candidate_match(event, key):
            continue
        for horizon_sec, counts in followthrough.items():
            horizon = event["horizons"].get(horizon_sec)
            if not horizon:
                continue
            gross = horizon["close_bps"] if action == "LONG" else -horizon["close_bps"]
            counts[1] += 1
            if gross > horizon["cost_bps"]:
                counts[0] += 1
    return {
        str(horizon_sec): (round(values[0] / values[1], 4) if values[1] else None)
        for horizon_sec, values in followthrough.items()
    }


def build_shadow_rows():
    signal_by_event = {}
    if os.path.exists(SHADOW_SIGNALS):
        with open(SHADOW_SIGNALS, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                event_id = row.get("event_id")
                if event_id:
                    signal_by_event[event_id] = row

    aggs = defaultdict(Agg)
    seen = set()
    rows_total = 0
    parse_errors = 0
    if not os.path.exists(SHADOW_OUTCOMES):
        return [], rows_total, parse_errors, seen

    with open(SHADOW_OUTCOMES, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows_total += 1
            try:
                row = json.loads(line)
            except Exception:
                parse_errors += 1
                continue
            source_event_id = row.get("source_event_id") or row.get("event_id")
            playbook = row.get("playbook") or "unknown_playbook"
            side = row.get("side") or row.get("base_side") or "UNKNOWN"
            horizon_sec = int(sf(row.get("horizon_sec"), 0) or 0)
            dedupe_key = (
                source_event_id,
                playbook,
                side,
                horizon_sec,
                row.get("shadow_branch_label") or row.get("branch_type"),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            signal = signal_by_event.get(source_event_id, {})
            factors = row.get("factors") or signal.get("factors") or {}
            liquidity = bucket_shadow_liq(row.get("liquidity_bucket") or signal.get("liquidity_bucket"))
            gross = pct_to_bps(row.get("close_return_pct"))
            fee = sf(row.get("round_trip_fee_bps"), 8.0) or 8.0
            spread = sf(row.get("spread_bps_at_entry"), sf(signal.get("spread_bps_at_entry"), 0.0)) or 0.0
            slippage = sf(row.get("estimated_slippage_bps"), sf(signal.get("estimated_slippage_bps"), 0.0)) or 0.0
            funding = abs(sf(row.get("funding_paid_during_hold"), 0.0) or 0.0) * 100.0
            cost = fee + spread + slippage + funding
            net = gross - cost
            mfe = pct_to_bps(row.get("max_favorable_return_pct"))
            regime = "/".join(
                str(factors.get(name) or "unknown")
                for name in ["market_regime_bucket", "trend_bucket", "momentum_bucket"]
            )
            action = "LONG" if str(side).upper() in {"BUY", "LONG"} else "SHORT"
            key = (
                "shadow_playbook_factor_state",
                action,
                horizon_sec,
                str(playbook),
                liquidity,
                str(factors.get("market_regime_bucket") or "regime_unknown"),
                str(factors.get("momentum_bucket") or "momentum_unknown"),
                str(factors.get("oi_build_bucket") or "oi_build_unknown"),
            )
            aggs[key].add(
                row.get("symbol") or "UNKNOWN",
                liquidity,
                regime,
                signal.get("trading_session") or "unknown_session",
                net,
                gross,
                cost,
                fee,
                spread,
                slippage,
                funding,
                mfe,
            )

    rows = []
    for key, agg in aggs.items():
        if agg.n < 20:
            continue
        result = row_from_agg(key, agg, "active_forward_shadow")
        result["passes_shadow_candidate_screen"] = (
            result["avg_net_bps"] > 0
            and result["unique_signals"] >= 20
            and result["top_symbol_profit_concentration"] <= 0.50
        )
        result["passes_limited_testnet_math_only"] = (
            result["unique_signals"] >= 100
            and result["avg_net_bps"] > 0
            and result["gross_cost_ratio"] >= 3.0
            and result["top_symbol_profit_concentration"] <= 0.35
            and result["dominant_liquidity_bucket"] in {"mega", "major"}
        )
        result["followthrough_rate_30_60_180_300"] = "{}"
        rows.append(result)
    rows.sort(
        key=lambda item: (
            item["passes_limited_testnet_math_only"],
            item["passes_shadow_candidate_screen"],
            item["avg_net_bps"],
            item["profit_factor"],
        ),
        reverse=True,
    )
    return rows, rows_total, parse_errors, seen


def summarize_testnet():
    result = {
        "trade_files": 0,
        "trades": 0,
        "net_pnl_usdt": 0.0,
        "gross_pnl_usdt": 0.0,
        "commission_usdt": 0.0,
        "wins": 0,
        "avg_fee_bps": 0.0,
        "avg_spread_entry_bps": 0.0,
        "avg_slippage_bps": 0.0,
        "by_side": defaultdict(lambda: {"n": 0, "net": 0.0, "gross": 0.0}),
        "by_liq": defaultdict(lambda: {"n": 0, "net": 0.0, "gross": 0.0}),
        "hmm_available": 0,
        "btc_regime_available": 0,
    }
    for path in sorted(glob.glob(TESTNET_GLOB)):
        result["trade_files"] += 1
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                result["trades"] += 1
                net = sf(row.get("net_pnl_usdt"), 0.0) or 0.0
                gross = sf(row.get("gross_pnl_usdt"), 0.0) or 0.0
                result["net_pnl_usdt"] += net
                result["gross_pnl_usdt"] += gross
                result["commission_usdt"] += sf(row.get("commission_usdt"), 0.0) or 0.0
                result["wins"] += 1 if net > 0 else 0
                result["avg_fee_bps"] += sf(row.get("fee_bps_effective"), 0.0) or 0.0
                result["avg_spread_entry_bps"] += (
                    sf(row.get("spread_proxy_at_entry_bps"), 0.0) or 0.0
                )
                result["avg_slippage_bps"] += sf(row.get("estimated_slippage_bps"), 0.0) or 0.0
                side = str(row.get("side") or "UNKNOWN")
                liquidity = bucket_shadow_liq(row.get("liquidity_bucket"))
                result["by_side"][side]["n"] += 1
                result["by_side"][side]["net"] += net
                result["by_side"][side]["gross"] += gross
                result["by_liq"][liquidity]["n"] += 1
                result["by_liq"][liquidity]["net"] += net
                result["by_liq"][liquidity]["gross"] += gross
                if row.get("hmm_state_if_available") is not None:
                    result["hmm_available"] += 1
                if row.get("btc_regime") is not None:
                    result["btc_regime_available"] += 1
    if result["trades"]:
        for key in ["avg_fee_bps", "avg_spread_entry_bps", "avg_slippage_bps"]:
            result[key] = round(result[key] / result["trades"], 4)
    return result


def simple_nested(mapping):
    output = {}
    for key, value in mapping.items():
        output[key] = {
            inner_key: round(inner_value, 4) if isinstance(inner_value, float) else inner_value
            for inner_key, inner_value in value.items()
        }
    return output


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    events, line_count, parse_errors, first_ms, last_ms, horizon_counts = load_collector_events()
    collector_aggs, action_summary = build_collector_aggs(events)

    collector_rows = []
    key_by_signature = {}
    for key, agg in collector_aggs.items():
        if agg.n < 100:
            continue
        row = row_from_agg(key, agg, "collector_forward_labels")
        row["passes_shadow_candidate_screen"] = (
            row["avg_net_bps"] > 0
            and row["net_return_bps_sum"] > 0
            and row["gross_cost_ratio"] >= 1.5
            and row["top_symbol_profit_concentration"] <= 0.50
        )
        row["passes_limited_testnet_math_only"] = (
            row["unique_signals"] >= 100
            and row["avg_net_bps"] > 0
            and row["expected_net_edge_bps"] > 0
            and row["gross_cost_ratio"] >= 3.0
            and row["top_symbol_profit_concentration"] <= 0.35
            and row["dominant_liquidity_bucket"] in {"mega", "major"}
        )
        row["followthrough_rate_30_60_180_300"] = "{}"
        collector_rows.append(row)
        key_by_signature[(row["template"], row["action"], row["horizon_sec"], row["state_rule"])] = key

    collector_rows.sort(
        key=lambda item: (
            item["passes_limited_testnet_math_only"],
            item["passes_shadow_candidate_screen"],
            item["avg_net_bps"],
            item["profit_factor"],
            -item["top_symbol_profit_concentration"],
        ),
        reverse=True,
    )
    for row in collector_rows[:80]:
        key = key_by_signature.get(
            (row["template"], row["action"], row["horizon_sec"], row["state_rule"])
        )
        if key:
            row["followthrough_rate_30_60_180_300"] = json.dumps(
                followthrough_for_key(events, key), ensure_ascii=False
            )

    shadow_rows, shadow_total, shadow_parse_errors, shadow_seen = build_shadow_rows()
    testnet = summarize_testnet()

    selected = []
    seen_rules = set()
    for row in collector_rows:
        if not row["passes_shadow_candidate_screen"]:
            continue
        if "baseline_control" in row["state_rule"]:
            continue
        signature = (row["action"], row["horizon_sec"], row["template"], row["state_rule"])
        if signature in seen_rules:
            continue
        selected.append(row)
        seen_rules.add(signature)
        if len(selected) >= 5:
            break
    if len(selected) < 3:
        for row in collector_rows:
            if not row["passes_shadow_candidate_screen"]:
                continue
            signature = (row["action"], row["horizon_sec"], row["template"], row["state_rule"])
            if signature in seen_rules:
                continue
            selected.append(row)
            seen_rules.add(signature)
            if len(selected) >= 5:
                break

    shadow_selected = [row for row in shadow_rows if row["passes_shadow_candidate_screen"]][:5]
    limited_testnet_math = [
        row for row in collector_rows + shadow_rows if row.get("passes_limited_testnet_math_only")
    ]
    action_summary_rows = [
        row_from_agg(key, agg, "collector_action_all_states")
        for key, agg in action_summary.items()
    ]
    action_summary_rows.sort(key=lambda item: (item["action"], item["horizon_sec"]))

    worst_rows = []
    for row in sorted(collector_rows, key=lambda item: item["avg_net_bps"]):
        if row["unique_signals"] >= 300 and row["avg_net_bps"] < -10:
            worst_rows.append(row)
        if len(worst_rows) >= 20:
            break

    fieldnames = [
        "source",
        "template",
        "action",
        "horizon_sec",
        "state_rule",
        "unique_signals",
        "net_return_bps_sum",
        "avg_net_bps",
        "expected_net_edge_bps",
        "avg_gross_move_bps",
        "avg_total_cost_bps",
        "gross_cost_ratio",
        "win_rate",
        "profit_factor",
        "fakeout_rate",
        "avg_fee_bps",
        "avg_spread_bps",
        "avg_slippage_bps",
        "avg_funding_bps",
        "top_symbol_profit_concentration",
        "top_symbol_by_net",
        "dominant_liquidity_bucket",
        "dominant_regime",
        "dominant_session",
        "max_consecutive_losses",
        "max_drawdown_bps",
        "passes_shadow_candidate_screen",
        "passes_limited_testnet_math_only",
        "followthrough_rate_30_60_180_300",
    ]
    with open(os.path.join(OUT_DIR, "market_state_candidate_table.csv"), "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in collector_rows[:300] + shadow_rows[:100]:
            writer.writerow({key: row.get(key, "") for key in fieldnames})

    with open(os.path.join(OUT_DIR, "selected_shadow_candidate_rules.json"), "w", encoding="utf-8") as handle:
        json.dump(selected[:3], handle, ensure_ascii=False, indent=2)
    with open(os.path.join(OUT_DIR, "active_shadow_positive_states.json"), "w", encoding="utf-8") as handle:
        json.dump(shadow_selected[:5], handle, ensure_ascii=False, indent=2)
    with open(os.path.join(OUT_DIR, "collector_action_summary.json"), "w", encoding="utf-8") as handle:
        json.dump(action_summary_rows, handle, ensure_ascii=False, indent=2)

    testnet_summary = {
        "trade_files": testnet["trade_files"],
        "trades": testnet["trades"],
        "gross_pnl_usdt": round(testnet["gross_pnl_usdt"], 4),
        "net_pnl_usdt": round(testnet["net_pnl_usdt"], 4),
        "commission_usdt": round(testnet["commission_usdt"], 4),
        "win_rate": round(testnet["wins"] / testnet["trades"], 4) if testnet["trades"] else None,
        "avg_fee_bps": testnet["avg_fee_bps"],
        "avg_spread_entry_bps": testnet["avg_spread_entry_bps"],
        "avg_slippage_bps": testnet["avg_slippage_bps"],
        "by_side": simple_nested(testnet["by_side"]),
        "by_liquidity": simple_nested(testnet["by_liq"]),
        "btc_regime_available_rows": testnet["btc_regime_available"],
        "hmm_available_rows": testnet["hmm_available"],
    }
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_policy": (
            "forward/live collector labels + active forward shadow + current testnet telemetry only; "
            "no backtest PnL used"
        ),
        "collector": {
            "path": COLLECTOR_HORIZON,
            "jsonl_rows": line_count,
            "parse_errors": parse_errors,
            "unique_events": len(events),
            "first_observed_at_utc": safe_time(first_ms),
            "last_observed_at_utc": safe_time(last_ms),
            "horizon_counts": dict(sorted(horizon_counts.items())),
        },
        "shadow": {
            "outcome_rows": shadow_total,
            "parse_errors": shadow_parse_errors,
            "deduped_outcomes": len(shadow_seen),
            "candidate_groups_ge20": len(shadow_rows),
        },
        "testnet_execution_sanity": testnet_summary,
        "selected_market_state_rules_for_shadow_candidate_observation": selected[:3],
        "active_shadow_positive_state_groups": shadow_selected[:3],
        "limited_testnet_math_pass_count": len(limited_testnet_math),
        "limited_testnet_math_pass_examples": limited_testnet_math[:5],
        "worst_no_trade_state_examples": worst_rows[:10],
        "artifacts": {
            "candidate_table_csv": os.path.join(OUT_DIR, "market_state_candidate_table.csv"),
            "selected_rules_json": os.path.join(OUT_DIR, "selected_shadow_candidate_rules.json"),
            "shadow_positive_json": os.path.join(OUT_DIR, "active_shadow_positive_states.json"),
            "action_summary_json": os.path.join(OUT_DIR, "collector_action_summary.json"),
        },
    }
    with open(os.path.join(OUT_DIR, "money_first_market_learning_summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    verdict = "C. 数据不足，继续 shadow 补证据"
    if limited_testnet_math:
        verdict = "A. 有候选通过数学筛选，但仍需人工和 Auditor 检查后才可讨论 limited testnet"
    elif selected:
        verdict = "A. 发现 1-3 个只适合 shadow/live-candidate 观察的市场状态规则，不可 testnet"
    elif not selected:
        verdict = "B. 暂未发现可交易 edge"

    report_lines = [
        "# Trading Candidate Decision Report - Money-First Market Learning Sprint",
        "",
        "【老板判断区】",
        f"1. 结论：{verdict}。本报告只用实时 collector 标签、active forward shadow 和 testnet telemetry，不用 backtest PnL。",
        "2. 状态：黄。可以继续 shadow 观察；不可以 limited testnet；不可以 mainnet。",
        "3. 是否真进步：证据不足。这是市场学习进步，不是交易盈利证据；还没有真实 testnet 正期望。",
        "4. 是否新策略：不是完整新策略，是从市场状态学习出的候选入场规则。",
        "5. 是否可以 testnet：不可以，除非后续 forward shadow 独立样本满足硬条件并经 Shawn 批准。",
        "6. 是否可以 mainnet：不可以。",
        "",
        "## 样本",
        f"- collector forward labels：{len(events)} unique events，{line_count} horizon rows，时间 {safe_time(first_ms)} 到 {safe_time(last_ms)}。",
        f"- active forward shadow：{len(shadow_seen)} deduped outcomes。",
        f"- testnet telemetry：{testnet['trades']} trades，gross {testnet['gross_pnl_usdt']:.4f} USDT，net {testnet['net_pnl_usdt']:.4f} USDT。",
        "",
        "## 最接近 edge 的候选，只能 shadow",
    ]
    if selected[:3]:
        for index, row in enumerate(selected[:3], 1):
            report_lines.append(
                f"{index}. {row['action']} {row['horizon_sec']}s | {row['state_rule']} | "
                f"signals {row['unique_signals']} | avg net {row['avg_net_bps']} bps | "
                f"cost {row['avg_total_cost_bps']} bps | gross/cost {row['gross_cost_ratio']} | "
                f"PF {row['profit_factor']} | fakeout {row['fakeout_rate']} | "
                f"top symbol concentration {row['top_symbol_profit_concentration']} | "
                f"follow-through {row.get('followthrough_rate_30_60_180_300', '{}')}。"
            )
    else:
        report_lines.append("- 无。")
    report_lines.extend(
        [
            "",
            "## 为什么不能 limited testnet",
            f"- 通过 limited-testnet 数学硬筛的组合数量：{len(limited_testnet_math)}。即使数学过筛，也还缺独立 shadow manifest、entry parity 和 Auditor 验证；当前没有 Shawn 批准。",
            "- testnet 现有样本仍是旧策略执行 sanity，不是这些 market-state 规则的真实执行结果。",
            f"- HMM regime 在 testnet 样本中可用行数为 {testnet['hmm_available']}；BTC regime 可用行数为 {testnet['btc_regime_available']}；这两个字段不能作为当前盈利结论。",
            "",
            "## 立即屏蔽/不交易方向",
        ]
    )
    for row in worst_rows[:5]:
        report_lines.append(
            f"- {row['action']} {row['horizon_sec']}s | {row['state_rule']} | "
            f"signals {row['unique_signals']} | avg net {row['avg_net_bps']} bps | "
            f"fakeout {row['fakeout_rate']}。"
        )
    report_lines.extend(
        [
            "",
            "## Kill 条件",
            "- 新候选进入 shadow 后，100 个 unique forward shadow signals 仍 net <= 0，kill。",
            "- avg net <= 0 或 expected_net_edge_bps <= 0，kill。",
            "- gross move < 3x total cost，不能 testnet。",
            "- top symbol profit concentration > 35%，不能 testnet。",
            "- 主要盈利不来自 major/mega liquidity，不能 testnet。",
            "- 不允许调参硬救，不允许 mainnet。",
        ]
    )
    with open(os.path.join(OUT_DIR, "trading_candidate_decision_report.md"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(report_lines) + "\n")

    print(
        json.dumps(
            {
                "out_dir": OUT_DIR,
                "collector_unique_events": len(events),
                "collector_rows": line_count,
                "shadow_deduped_outcomes": len(shadow_seen),
                "testnet_trades": testnet["trades"],
                "selected_count": len(selected[:3]),
                "limited_testnet_math_pass_count": len(limited_testnet_math),
                "top_selected": selected[:3],
                "testnet": testnet_summary,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
