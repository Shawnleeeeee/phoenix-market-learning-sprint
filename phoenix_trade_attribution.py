from __future__ import annotations

from typing import Any


ATTRIBUTION_VERSION = "v1.0"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def side_is_short(row: dict[str, Any]) -> bool:
    return str(row.get("side") or "").upper() in {"SELL", "SHORT"}


def return_pct_from_prices(row: dict[str, Any]) -> float:
    entry = safe_float(row.get("entry_price") or row.get("simulated_entry_price"))
    close = safe_float(row.get("close_price") or row.get("actual_close_avg_price") or row.get("final_exit_price"))
    if entry <= 0 or close <= 0:
        return safe_float(row.get("return_pct") or row.get("close_return_pct") or row.get("final_return_pct"))
    if side_is_short(row):
        return (entry / max(close, 1e-12) - 1.0) * 100.0
    return (close / entry - 1.0) * 100.0


def path_metrics(row: dict[str, Any]) -> dict[str, float]:
    if row.get("mfe_pct") is not None or row.get("mae_pct") is not None:
        mfe = safe_float(row.get("mfe_pct"))
        mae = safe_float(row.get("mae_pct"))
        realized = safe_float(row.get("return_pct"), return_pct_from_prices(row))
        return {
            "mfe_pct": mfe,
            "mae_pct": mae,
            "return_pct": realized,
            "profit_giveback_pct": safe_float(row.get("profit_giveback_pct"), max(0.0, mfe - realized)),
        }
    entry = safe_float(row.get("entry_price") or row.get("simulated_entry_price"))
    best = safe_float(row.get("best_mark_price") or row.get("max_mark_price"), entry)
    worst = safe_float(row.get("worst_mark_price") or row.get("min_mark_price"), entry)
    realized = return_pct_from_prices(row)
    if entry <= 0:
        return {"mfe_pct": 0.0, "mae_pct": 0.0, "return_pct": realized, "profit_giveback_pct": 0.0}
    if side_is_short(row):
        mfe = max(0.0, (entry / max(worst, 1e-12) - 1.0) * 100.0)
        mae = max(0.0, (best / entry - 1.0) * 100.0)
    else:
        mfe = max(0.0, (best / entry - 1.0) * 100.0)
        mae = max(0.0, (entry / max(worst, 1e-12) - 1.0) * 100.0)
    return {
        "mfe_pct": round(mfe, 6),
        "mae_pct": round(mae, 6),
        "return_pct": round(realized, 6),
        "profit_giveback_pct": round(max(0.0, mfe - realized), 6),
    }


def infer_net_pnl(row: dict[str, Any]) -> float:
    if row.get("net_pnl_usdt") is not None:
        return safe_float(row.get("net_pnl_usdt"))
    for key in (
        "after_real_cost_return_pct",
        "after_fee_and_slippage_return_pct",
        "after_fee_return_pct",
        "return_pct",
        "close_return_pct",
        "final_return_pct",
    ):
        if row.get(key) is not None:
            return safe_float(row.get(key))
    return return_pct_from_prices(row)


def classify_execution_record(row: dict[str, Any]) -> dict[str, Any]:
    metrics = path_metrics(row)
    net = infer_net_pnl(row)
    gross = safe_float(row.get("gross_pnl_usdt"), safe_float(row.get("realized_pnl_usdt")))
    commission = abs(safe_float(row.get("commission_usdt")))
    slippage = safe_float(row.get("estimated_slippage_bps"))
    exit_reason = str(row.get("exit_reason") or row.get("final_exit_reason") or "").lower()
    setup = str(row.get("setup") or row.get("strategy_id") or "").lower()
    symbol = str(row.get("symbol") or "").upper()
    is_loss = net <= 0
    reasons: list[str] = []

    if not is_loss:
        reasons.append("not_a_loss")
    else:
        if row.get("reject") or "reject" in exit_reason or "filter" in str(row.get("reject_reason") or "").lower():
            reasons.append("order_reject_microstructure")
        if "take_profit" in exit_reason and net <= 0:
            reasons.append("tp_net_loss")
        if gross >= 0 and commission > 0 and net <= 0:
            reasons.append("fee_drag_exit")
        if slippage >= 25.0:
            reasons.append("high_slippage")
        if exit_reason in {"stale_exit", "time_stop", "time_decay_exit", "no_follow_through_exit", "fee_slippage_tp_fail"} and metrics["mfe_pct"] < 0.10:
            reasons.append("no_follow_through")
        if exit_reason in {"stop_loss", "dynamic_breakeven_exit"} or metrics["mae_pct"] >= max(0.20, metrics["mfe_pct"] * 2.0):
            reasons.append("immediate_adverse_move")
        if symbol and setup and row.get("bad_symbol_setup_combo"):
            reasons.append("bad_symbol_setup_combo")

    if not reasons:
        reasons.append("unknown")
    if len(reasons) > 1 and "unknown" in reasons:
        reasons = [reason for reason in reasons if reason != "unknown"]

    priority = [
        "not_a_loss",
        "order_reject_microstructure",
        "tp_net_loss",
        "fee_drag_exit",
        "high_slippage",
        "bad_symbol_setup_combo",
        "no_follow_through",
        "immediate_adverse_move",
        "unknown",
    ]
    primary = next((reason for reason in priority if reason in reasons), reasons[0])
    secondary = [reason for reason in reasons if reason != primary]
    return {
        "attribution_version": ATTRIBUTION_VERSION,
        "loss_reason": primary,
        "secondary_loss_reasons": secondary,
        "all_loss_reasons": reasons,
        "is_loss": is_loss,
        "gross_pnl_usdt": gross,
        "net_pnl_usdt": net,
        "commission_usdt": commission,
        "estimated_slippage_bps": slippage,
        "mfe_pct": metrics["mfe_pct"],
        "mae_pct": metrics["mae_pct"],
        "return_pct": metrics["return_pct"],
        "profit_giveback_pct": metrics["profit_giveback_pct"],
    }
