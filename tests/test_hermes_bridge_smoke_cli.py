import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from phoenix.hermes_file_bridge import HermesFileBridge
from phoenix.hermes_trader_mode import run_bridge_smoke_test_async
from phoenix.trader_snapshot import build_trader_snapshot


def untrusted_runtime_snapshot() -> dict:
    return build_trader_snapshot(
        market_data={"regime": "UNKNOWN", "direction_lock": "BOTH_ALLOWED"},
        account_state={"trading_allowed": True, "max_open_positions": 1, "open_positions_count": 0},
        candidates=[
            {
                "symbol": "BTCUSDT",
                "bias": "LONG",
                "setup_type": "QUICK_TRADE",
                "score": 1,
                "current_price": 100.0,
                "spread_bps": 1.0,
                "estimated_slippage_bps": 1.0,
                "liquidity_ok": True,
            }
        ],
        system_status={
            "source": "runtime",
            "snapshot_source": "runtime",
            "trusted_runtime_snapshot": False,
            "data_fresh": False,
            "websocket_status": "unavailable",
            "exchange_status": "unavailable",
            "position_state": "unknown",
            "position_state_source": "unknown",
            "stop_protection_status": "unknown",
            "candidate_state": "known",
            "protective_stop_path_available": False,
            "emergency_close_available": False,
            "protective_stop_capability_source": "unverified",
            "emergency_close_capability_source": "unverified",
            "freeze_reason": "runtime_snapshot_not_trusted_for_testnet_order",
        },
    )


class HermesBridgeSmokeCliTests(unittest.TestCase):
    def test_bridge_smoke_three_cycles_never_submit_orders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bridge = HermesFileBridge(
                inbox_dir=root / "hermes_inbox",
                outbox_dir=root / "hermes_outbox",
                archive_dir=root / "hermes_archive",
                log_dir=root / "hermes_logs",
            )
            with patch("phoenix.hermes_trader_mode.build_dashboard_api_trader_snapshot", return_value=untrusted_runtime_snapshot()):
                summary = asyncio.run(
                    run_bridge_smoke_test_async(
                        provider=None,
                        bridge=bridge,
                        output_dir=root / "run_logs",
                        decision_timeout_sec=0.01,
                    )
                )

            cycles = summary["cycles"]
            self.assertEqual(len(cycles), 3)
            self.assertEqual(summary["orders_submitted"], 0)
            self.assertEqual(summary["mainnet_order_submitted"], 0)
            self.assertTrue(all(cycle["executor_called"] is False for cycle in cycles))
            self.assertTrue(all(cycle["order_submitted"] is False for cycle in cycles))
            self.assertEqual(cycles[0]["action"], "NO_TRADE")
            self.assertEqual(cycles[0]["result_type"], "soft_reject")
            self.assertTrue(cycles[0]["fallback_used"])
            self.assertEqual(cycles[0]["decision_origin"], "timeout_fallback")
            self.assertIn("invalid_source", cycles[0]["blocked_by"])
            self.assertEqual(cycles[1]["action"], "NO_TRADE")
            self.assertEqual(cycles[1]["result_type"], "soft_reject")
            self.assertFalse(cycles[1]["fallback_used"])
            self.assertEqual(cycles[1]["decision_origin"], "outbox_file")
            self.assertIn("no_trade_from_hermes", cycles[1]["blocked_by"])
            self.assertEqual(cycles[2]["action"], "ENTER_LONG")
            self.assertFalse(cycles[2]["order_submitted"])
            self.assertIn(cycles[2]["result_type"], {"soft_reject", "hard_freeze"})
            self.assertTrue(all(Path(item["snapshot_path"]).exists() for item in cycles))
            self.assertTrue((root / "hermes_logs" / "hermes_file_bridge_events.jsonl").exists())

            replay_rows = [
                json.loads(line)
                for line in next((root / "run_logs").glob("hermes_file_bridge_smoke_*")).joinpath("replay_events.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            snapshot_rows = [row for row in replay_rows if row.get("event") == "snapshot"]
            self.assertEqual(len(snapshot_rows), 3)
            self.assertTrue(all(row.get("snapshot_path") for row in snapshot_rows))
            self.assertTrue(all(row.get("snapshot_source") == "runtime" for row in snapshot_rows))
            self.assertTrue(all(row.get("trusted_runtime_snapshot") is False for row in snapshot_rows))


if __name__ == "__main__":
    unittest.main()
