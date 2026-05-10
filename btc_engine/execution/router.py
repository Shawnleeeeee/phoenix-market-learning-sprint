"""Execution router CLI for Leiting BTC testnet."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from btc_engine.config import get_journal_mode_for_execution, get_symbol, load_risk_config
from btc_engine.execution.binance_signed import BinanceSignedFuturesClient
from btc_engine.execution.order_manager import build_execution_plan, place_entry_order, preflight_test_order
from btc_engine.execution.orphan_recovery import recover_orphan_positions
from btc_engine.execution.post_fill_worker import _clear_pending_close_request, _write_pending_close_request, manage_active_trade_once
from btc_engine.execution.protective_orders import sync_protection
from btc_engine.execution.safe_gateway import submit_btc_client_order
from btc_engine.review.export_review import maybe_export_formal_review
from btc_engine.review.journal import build_close_journal, write_trade_journal
from btc_engine.runtime.demo_account import update_demo_account_after_close
from btc_engine.runtime.state_store import read_runtime_state, update_runtime_state


def _client() -> BinanceSignedFuturesClient:
    return BinanceSignedFuturesClient()


def _parse_args() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Leiting BTC testnet execution router")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("account")

    p_probe = sub.add_parser("probe")
    p_probe.add_argument("--symbol", default=get_symbol())

    p_preflight = sub.add_parser("preflight")
    p_preflight.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    p_preflight.add_argument("--quote-allocation", type=float, default=100.0)
    p_preflight.add_argument("--leverage", type=int, default=10)
    p_preflight.add_argument("--reason", default="manual_preflight")

    p_enter = sub.add_parser("enter")
    p_enter.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    p_enter.add_argument("--quote-allocation", type=float, default=100.0)
    p_enter.add_argument("--leverage", type=int, default=10)
    p_enter.add_argument("--reason", default="manual_entry")

    p_orders = sub.add_parser("orders")
    p_orders.add_argument("--symbol", default=get_symbol())

    p_cancel = sub.add_parser("cancel-all")
    p_cancel.add_argument("--symbol", default=get_symbol())

    p_flatten = sub.add_parser("flatten")
    p_flatten.add_argument("--symbol", default=get_symbol())
    p_flatten.add_argument("--reason", default="manual_flatten")

    p_recover = sub.add_parser("recover")
    p_recover.add_argument("--symbol", default=get_symbol())

    sub.add_parser("manage-once")
    return parser


def _print(data: dict | list) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def route_trade() -> None:
    """Route an approved BTC trade to the correct Binance API family."""
    parser = _parse_args()
    args = parser.parse_args()
    client = _client()

    if args.cmd == "account":
        _print({"account": client.account(), "balance": client.ping_account()})
        return

    if args.cmd == "probe":
        _print(
            {
                "account": client.account(),
                "position": client.position_risk(args.symbol),
                "orders": client.open_orders(args.symbol),
                "algo_orders": client.open_algo_orders(args.symbol),
            }
        )
        return

    if args.cmd == "preflight":
        plan = build_execution_plan(
            client=client,
            side=args.side,
            quote_allocation_usdt=args.quote_allocation,
            leverage=args.leverage,
            reason=args.reason,
        )
        _print(preflight_test_order(client, plan))
        return

    if args.cmd == "enter":
        plan = build_execution_plan(
            client=client,
            side=args.side,
            quote_allocation_usdt=args.quote_allocation,
            leverage=args.leverage,
            reason=args.reason,
        )
        order = place_entry_order(client, plan)
        protection = sync_protection(client, plan)
        entry_avg_price = float(order.get("avgPrice") or plan.reference_price)
        opened_at = datetime.now(timezone.utc).isoformat()
        active_trade = {
            "status": "open",
            "phase": "INITIAL_STOP",
            "opened_at": opened_at,
            "symbol": plan.symbol,
            "side": plan.side,
            "plan": asdict(plan),
            "entry_order": order,
            "entry_order_id": order.get("orderId"),
            "entry_avg_price": entry_avg_price,
            "entry_update_time_ms": int(order.get("updateTime") or order.get("transactTime") or 0),
            "protection": protection,
            "current_stop_trigger_price": plan.initial_stop_price,
            "high_watermark": entry_avg_price,
            "low_watermark": entry_avg_price,
            "events": [
                {
                    "event": "entry_filled",
                    "at": opened_at,
                    "avg_price": entry_avg_price,
                    "quantity": plan.quantity,
                },
                {
                    "event": "initial_stop_armed",
                    "at": opened_at,
                    "trigger_price": plan.initial_stop_price,
                },
            ],
        }
        update_runtime_state("active_trade", active_trade)
        _print(active_trade)
        return

    if args.cmd == "orders":
        _print(
            {
                "open_orders": client.open_orders(args.symbol),
                "open_algo_orders": client.open_algo_orders(args.symbol),
                "positions": client.position_risk(args.symbol),
            }
        )
        return

    if args.cmd == "cancel-all":
        _print(
            {
                "orders": client.cancel_all_open_orders(args.symbol),
                "algo_orders": client.cancel_all_open_algo_orders(args.symbol),
            }
        )
        return

    if args.cmd == "flatten":
        positions = client.position_risk(args.symbol)
        position = next((item for item in positions if abs(float(item.get("positionAmt", 0.0))) > 0.0), None)
        active_trade = read_runtime_state("active_trade")
        if position is None:
            _print({"ok": True, "flattened": False, "reason": "no_open_position"})
            return
        amount = abs(float(position["positionAmt"]))
        side = "SELL" if float(position["positionAmt"]) > 0 else "BUY"
        if isinstance(active_trade, dict):
            _write_pending_close_request(symbol=args.symbol, side=side, reason=args.reason, active_trade=active_trade)
        client.cancel_all_open_orders(args.symbol)
        client.cancel_all_open_algo_orders(args.symbol)
        close_result = submit_btc_client_order(
            client,
            {
                "symbol": args.symbol,
                "side": side,
                "type": "MARKET",
                "quantity": amount,
                "reduceOnly": "true",
                "newOrderRespType": "RESULT",
            },
            source="btc_engine.execution.router:flatten",
            purpose="emergency",
        )
        now = datetime.now(timezone.utc)
        opened_at_str = active_trade.get("opened_at") if isinstance(active_trade, dict) else None
        entry_order_id = active_trade.get("entry_order_id") if isinstance(active_trade, dict) else None
        close_order_id = close_result.get("orderId")
        if opened_at_str:
            opened_at = datetime.fromisoformat(opened_at_str.replace("Z", "+00:00"))
        else:
            opened_at = now - timedelta(hours=1)
        start_ms = int((opened_at - timedelta(minutes=2)).timestamp() * 1000)
        end_ms = int((now + timedelta(minutes=2)).timestamp() * 1000)
        trades_all = client.user_trades(args.symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000)
        target_order_ids = {item for item in (entry_order_id, close_order_id) if item is not None}
        trades = [item for item in trades_all if item.get("orderId") in target_order_ids] if target_order_ids else trades_all
        incomes = client.income_history(args.symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=200)
        journal = build_close_journal(
            symbol=args.symbol,
            side=side,
            quantity=amount,
            trades=trades,
            incomes=incomes,
            reason=args.reason,
            opened_at=opened_at,
            entry_order_id=entry_order_id,
            close_order_id=close_order_id,
            mode=get_journal_mode_for_execution(),
        )
        job_id = f"{args.symbol}_{int(time.time())}"
        journal_path = write_trade_journal(job_id, journal)
        if journal.get("mode") == "demo_auto":
            update_demo_account_after_close(job_id, journal)
        _clear_pending_close_request()
        update_runtime_state("last_close", journal)
        update_runtime_state(
            "active_trade",
            {
                **(active_trade or {}),
                "status": "closed",
                "closed_at": journal["closed_at"],
                "close_reason": args.reason,
                "close_order": close_result,
            },
        )
        formal_review = maybe_export_formal_review()
        _print(
            {
                "close_result": close_result,
                "journal_path": str(journal_path),
                "journal": journal,
                "formal_review": formal_review,
            }
        )
        return

    if args.cmd == "recover":
        _print(recover_orphan_positions(client, symbol=args.symbol))
        return

    if args.cmd == "manage-once":
        _print(manage_active_trade_once(client))
        return


def main() -> None:
    route_trade()


if __name__ == "__main__":
    main()
