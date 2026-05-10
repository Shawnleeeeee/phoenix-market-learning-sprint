import json
import tempfile
import unittest
from pathlib import Path

from phoenix.models import OrderInstruction, SymbolRules, TradeIntent
from phoenix.safe_order_gateway import SafeOrderGatewayBlocked, build_gateway_snapshot, submit_order_intent
from phoenix_post_fill_worker import ensure_unique_protection_order
from phoenix_live_execute import place_instruction as live_place_instruction
from phoenix_post_fill_worker import place_instruction as guardian_place_instruction
from btc_engine.execution.order_manager import place_entry_order as btc_place_entry_order
from btc_engine.types import ExecutionPlan as BtcExecutionPlan, SymbolRules as BtcSymbolRules


def market_instruction() -> OrderInstruction:
    return OrderInstruction(
        name="entry_market",
        endpoint="/fapi/v1/order",
        description="entry",
        payload={"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": 0.01},
    )


def conditional_instruction() -> OrderInstruction:
    return OrderInstruction(
        name="initial_protective_stop",
        endpoint="/fapi/v1/algoOrder",
        description="stop",
        payload={
            "symbol": "BTCUSDT",
            "side": "SELL",
            "type": "STOP_MARKET",
            "triggerPrice": 99.0,
            "stopPrice": 99.0,
            "closePosition": "true",
        },
    )


def build_intent() -> TradeIntent:
    return TradeIntent(
        symbol="BTCUSDT",
        side="BUY",
        entry_price=100.0,
        quantity=0.01,
        leverage=5,
        quote_allocation_usdt=10.0,
        notional_usdt=50.0,
        margin_type="ISOLATED",
        working_type="MARK_PRICE",
        initial_stop_price=99.0,
        take_profit_price=None,
        breakeven_trigger_price=101.0,
        breakeven_stop_price=100.1,
        trailing_callback_rate=0.5,
        allocation_mode="FIXED",
        allocation_cap_usdt=10.0,
        risk_budget_usdt=0.6,
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            trigger_protect=0.05,
        ),
    )


class FakeFutures:
    def __init__(self, *, log_path: Path | None = None) -> None:
        self.log_path = log_path
        self.new_order_calls: list[dict[str, object]] = []
        self.conditional_calls: list[dict[str, object]] = []

    async def new_order(self, payload: dict[str, object]) -> dict[str, object]:
        self.new_order_calls.append(dict(payload))
        return {"orderId": 1, **payload}

    async def new_conditional_order(self, payload: dict[str, object]) -> dict[str, object]:
        self.conditional_calls.append(dict(payload))
        return {"algoId": 2, **payload}

    async def open_conditional_orders(self, symbol: str | None = None) -> list[dict[str, object]]:
        return []


class FakeBtcClient:
    def __init__(self) -> None:
        self.new_order_calls: list[dict[str, object]] = []

    def assert_one_way_mode(self) -> None:
        return None

    def change_leverage(self, symbol: str, leverage: int) -> dict[str, object]:
        return {"symbol": symbol, "leverage": leverage}

    def new_order(self, payload: dict[str, object]) -> dict[str, object]:
        self.new_order_calls.append(dict(payload))
        return {"orderId": 99, **payload}


def build_btc_plan() -> BtcExecutionPlan:
    return BtcExecutionPlan(
        symbol="BTCUSDT",
        side="BUY",
        leverage=5,
        quote_allocation_usdt=10.0,
        reference_price=100.0,
        quantity=0.01,
        notional_usdt=1.0,
        initial_stop_price=99.0,
        breakeven_trigger_price=101.0,
        breakeven_stop_price=100.1,
        trailing_callback_rate=0.5,
        rules=BtcSymbolRules(symbol="BTCUSDT", tick_size=0.1, step_size=0.001, min_qty=0.001, min_notional=5.0),
        reason="unit_test",
    )


class OldExecutionSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_place_instruction_submits_intent_before_futures_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "intents.jsonl"
            futures = FakeFutures(log_path=log_path)

            result = await live_place_instruction(
                futures,
                market_instruction(),
                environment_name="testnet",
                intent_log_path=log_path,
                intent=build_intent(),
                purpose="entry",
            )

            self.assertEqual(result["orderId"], 1)
            self.assertEqual(len(futures.new_order_calls), 1)
            row = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(row["event"], "safe_order_approved")
            self.assertTrue(row["approved"])

    async def test_live_place_instruction_blocks_mainnet_without_futures_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "intents.jsonl"
            futures = FakeFutures()

            with self.assertRaises(SafeOrderGatewayBlocked) as ctx:
                await live_place_instruction(
                    futures,
                    market_instruction(),
                    environment_name="prod",
                    intent_log_path=log_path,
                )

            self.assertIn("mainnet_env_blocked", ctx.exception.blocked_by)
            self.assertEqual(futures.new_order_calls, [])
            row = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(row["event"], "safe_order_rejected")
            self.assertFalse(row["approved"])

    async def test_guardian_place_instruction_blocks_mainnet_without_futures_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "intents.jsonl"
            futures = FakeFutures()

            with self.assertRaises(SafeOrderGatewayBlocked):
                await guardian_place_instruction(
                    futures,
                    conditional_instruction(),
                    environment_name="mainnet",
                    intent_log_path=log_path,
                )

            self.assertEqual(futures.conditional_calls, [])
            row = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertIn("mainnet_env_blocked", row["blocked_by"])

    async def test_guardian_reconcile_passes_environment_to_place_instruction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "intents.jsonl"
            futures = FakeFutures()

            with self.assertRaises(SafeOrderGatewayBlocked):
                await ensure_unique_protection_order(
                    futures,
                    conditional_instruction(),
                    environment_name="prod",
                    intent_log_path=log_path,
                )

            self.assertEqual(futures.conditional_calls, [])
            row = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertIn("mainnet_env_blocked", row["blocked_by"])

    async def test_entry_without_protective_stop_is_risk_governor_blocked_before_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "risk.jsonl"

            gateway = await submit_order_intent(
                build_intent(),
                build_gateway_snapshot(
                    symbol="BTCUSDT",
                    side="BUY",
                    data_fresh=True,
                    websocket_status="healthy",
                    exchange_status="healthy",
                    position_state="known",
                    stop_protection_status="healthy",
                    protective_stop_path_available=False,
                    emergency_close_available=True,
                ),
                {"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                "test_entry_preflight",
                dry_run=True,
                audit_log_path=log_path,
                extra_context={
                    "protective_stop_path_available": False,
                    "emergency_close_path_available": True,
                },
            )

            self.assertFalse(gateway.approved)
            self.assertIn("protective_stop_path_unavailable", gateway.blocked_by)
            rows = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(rows[0]["event"], "safe_order_rejected")
            self.assertFalse(rows[0]["approved"])

    def test_legacy_btc_engine_entry_is_blocked_before_client_new_order(self) -> None:
        client = FakeBtcClient()

        with self.assertRaises(SafeOrderGatewayBlocked) as ctx:
            btc_place_entry_order(client, build_btc_plan())

        self.assertEqual(client.new_order_calls, [])
        self.assertIn("protective_stop_path_unavailable", ctx.exception.blocked_by)


class StaticOrderPathSafetyTests(unittest.TestCase):
    def test_business_order_paths_do_not_call_binance_order_methods_directly(self) -> None:
        root = Path(__file__).resolve().parents[1]
        allowed = {
            root / "phoenix" / "safe_order_gateway.py",
            root / "btc_engine" / "execution" / "safe_gateway.py",
        }
        skipped_parts = {
            ".git",
            ".tmp",
            ".venv",
            ".venv-win",
            "__pycache__",
            "audit_artifacts",
            "dist",
            "logs",
            "round_runner_reports_smoke",
            "round_runner_reports_smoke2",
            "round_runner_reports_smoke3",
            "signal_lab_replay",
        }
        offenders: list[str] = []
        for path in root.rglob("*.py"):
            if "tests" in path.parts or any(part in skipped_parts or part.startswith("round_runner_reports") for part in path.parts):
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if ".new_order(" in text or ".new_conditional_order(" in text or ".new_algo_order(" in text:
                if path not in allowed:
                    offenders.append(str(path.relative_to(root)))

        self.assertEqual(offenders, [])

    def test_known_legacy_entrypoints_use_safe_order_gateway(self) -> None:
        root = Path(__file__).resolve().parents[1]
        required_files = [
            "phoenix_live_execute.py",
            "phoenix_post_fill_worker.py",
            "phoenix_signal_bridge.py",
            "phoenix_testnet_round_runner.py",
            "phoenix/guardian_workers.py",
            "phoenix/hermes_trader_mode.py",
        ]
        for relative in required_files:
            text = (root / relative).read_text(encoding="utf-8", errors="ignore")
            self.assertIn("safe_order_gateway", text, relative)

    def test_trader_mode_does_not_call_binance_new_order_directly(self) -> None:
        root = Path(__file__).resolve().parents[1]
        text = (root / "phoenix" / "hermes_trader_mode.py").read_text(encoding="utf-8", errors="ignore")

        self.assertNotIn(".new_order(", text)
        self.assertNotIn(".new_conditional_order(", text)


if __name__ == "__main__":
    unittest.main()
