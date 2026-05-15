from __future__ import annotations

import unittest
from decimal import Decimal

from phoenix.exchange_filter_feasibility import (
    enrich_candidates_with_exchange_filters,
    evaluate_micro_notional_feasibility,
    parse_symbol_exchange_filters,
)
from phoenix.trader_snapshot import build_trader_snapshot


def exchange_symbol_info(
    symbol: str = "AVAXUSDT",
    *,
    status: str = "TRADING",
    contract_type: str = "PERPETUAL",
    price_precision: int = 2,
    quantity_precision: int = 0,
    min_qty: str = "1",
    step_size: str = "1",
    min_notional: str = "5",
    market_min_qty: str | None = None,
    market_step_size: str | None = None,
    notional_filter: str = "MIN_NOTIONAL",
) -> dict:
    return {
        "symbol": symbol,
        "status": status,
        "contractType": contract_type,
        "triggerProtect": "0.05",
        "pricePrecision": price_precision,
        "quantityPrecision": quantity_precision,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "minQty": min_qty, "stepSize": step_size},
            {
                "filterType": "MARKET_LOT_SIZE",
                "minQty": market_min_qty if market_min_qty is not None else min_qty,
                "stepSize": market_step_size if market_step_size is not None else step_size,
            },
            {"filterType": notional_filter, "notional": min_notional, "minNotional": min_notional},
        ],
    }


class ExchangeFilterFeasibilityTests(unittest.TestCase):
    def test_avax_stage2_micro_infeasible_even_at_max_quote_allocation(self) -> None:
        result = evaluate_micro_notional_feasibility(
            exchange_symbol_info(),
            current_price=24.0,
            configured_quote_allocation_usdt=5.0,
            configured_leverage=2,
            max_quote_allocation_usdt=10.0,
        )

        self.assertTrue(result.exchange_filter_checked)
        self.assertTrue(result.symbol_tradeable)
        self.assertFalse(result.micro_notional_feasible)
        self.assertEqual(result.rounded_qty, 0.0)
        self.assertEqual(result.min_qty, 1.0)
        self.assertEqual(result.step_size, 1.0)
        self.assertEqual(result.configured_quote_allocation_usdt, 5.0)
        self.assertEqual(result.configured_leverage, 2)
        self.assertEqual(result.max_quote_allocation_usdt, 10.0)
        self.assertEqual(result.required_quote_allocation_usdt, 12.0)
        self.assertIn("min_qty_not_met", result.infeasible_reason or "")
        self.assertIn("required_quote_allocation_exceeds_max", result.infeasible_reason or "")

    def test_feasible_symbol_rounded_qty_satisfies_step_min_qty_and_min_notional(self) -> None:
        result = evaluate_micro_notional_feasibility(
            exchange_symbol_info("SOLUSDT", min_qty="0.01", step_size="0.01", quantity_precision=2),
            current_price=142.42,
            configured_quote_allocation_usdt=5.0,
            configured_leverage=2,
            max_quote_allocation_usdt=10.0,
        )

        self.assertTrue(result.micro_notional_feasible)
        self.assertIsNone(result.infeasible_reason)
        self.assertGreaterEqual(result.rounded_qty or 0.0, result.min_qty or 0.0)
        self.assertGreaterEqual((result.rounded_qty or 0.0) * 142.42, result.min_notional or 0.0)
        self.assertEqual(Decimal(str(result.rounded_qty)) % Decimal(str(result.step_size)), Decimal("0.00"))

    def test_notional_filter_is_supported_when_min_notional_filter_absent(self) -> None:
        parsed = parse_symbol_exchange_filters(
            exchange_symbol_info("BTCUSDT", min_qty="0.001", step_size="0.001", notional_filter="NOTIONAL")
        )

        self.assertEqual(parsed.min_notional, 5.0)

    def test_market_lot_size_is_used_for_market_entry_quantity(self) -> None:
        parsed = parse_symbol_exchange_filters(
            exchange_symbol_info(
                "ETHUSDT",
                min_qty="0.001",
                step_size="0.001",
                market_min_qty="0.01",
                market_step_size="0.01",
                quantity_precision=2,
            )
        )

        self.assertEqual(parsed.min_qty, 0.01)
        self.assertEqual(parsed.step_size, 0.01)
        self.assertEqual(parsed.market_lot_min_qty, 0.01)
        self.assertEqual(parsed.market_lot_step_size, 0.01)

    def test_non_trading_or_non_perpetual_symbol_is_not_symbol_tradeable(self) -> None:
        stopped = evaluate_micro_notional_feasibility(
            exchange_symbol_info(status="BREAK"),
            current_price=24.0,
            configured_quote_allocation_usdt=5.0,
            configured_leverage=2,
            max_quote_allocation_usdt=10.0,
        )
        delivery = evaluate_micro_notional_feasibility(
            exchange_symbol_info(contract_type="CURRENT_QUARTER"),
            current_price=24.0,
            configured_quote_allocation_usdt=5.0,
            configured_leverage=2,
            max_quote_allocation_usdt=10.0,
        )

        self.assertFalse(stopped.symbol_tradeable)
        self.assertFalse(delivery.symbol_tradeable)
        self.assertIn("symbol_not_tradeable", stopped.infeasible_reason or "")
        self.assertIn("symbol_not_tradeable", delivery.infeasible_reason or "")

    def test_feasibility_does_not_auto_raise_quote_allocation_or_leverage(self) -> None:
        candidate = {"symbol": "AVAXUSDT", "current_price": 24.0}
        [enriched] = enrich_candidates_with_exchange_filters(
            [candidate],
            {"symbols": [exchange_symbol_info()]},
            configured_quote_allocation_usdt=5.0,
            configured_leverage=2,
            max_quote_allocation_usdt=10.0,
        )

        self.assertFalse(enriched["micro_notional_feasible"])
        self.assertEqual(enriched["configured_quote_allocation_usdt"], 5.0)
        self.assertEqual(enriched["configured_leverage"], 2)
        self.assertEqual(enriched["max_quote_allocation_usdt"], 10.0)
        self.assertGreater(enriched["required_quote_allocation_usdt"], enriched["max_quote_allocation_usdt"])

    def test_trader_snapshot_candidate_preserves_exchange_filter_feasibility_fields(self) -> None:
        [candidate] = enrich_candidates_with_exchange_filters(
            [{"symbol": "AVAXUSDT", "current_price": 24.0, "score": 90.0, "bias": "LONG"}],
            {"symbols": [exchange_symbol_info()]},
            configured_quote_allocation_usdt=5.0,
            configured_leverage=2,
            max_quote_allocation_usdt=10.0,
        )

        snapshot = build_trader_snapshot(candidates=[candidate])
        normalized = snapshot["top_candidates"][0]

        self.assertTrue(normalized["exchange_filter_checked"])
        self.assertFalse(normalized["micro_notional_feasible"])
        self.assertEqual(normalized["rounded_qty"], 0.0)
        self.assertEqual(normalized["configured_quote_allocation_usdt"], 5.0)
        self.assertEqual(normalized["configured_leverage"], 2)
        self.assertEqual(normalized["max_quote_allocation_usdt"], 10.0)
        self.assertIn("required_quote_allocation_exceeds_max", normalized["infeasible_reason"])


if __name__ == "__main__":
    unittest.main()
