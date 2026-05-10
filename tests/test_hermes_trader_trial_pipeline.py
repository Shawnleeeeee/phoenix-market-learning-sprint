import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from phoenix.hermes_decision import validate_hermes_decision
from phoenix.hermes_decision_provider import FileHermesDecisionProvider, HttpHermesDecisionProvider, MockHermesDecisionProvider
from phoenix.hermes_trader_mode import run_trader_cycle
from phoenix.review_reporter import build_review_report
from phoenix.risk_governor import evaluate_risk
from phoenix.trader_snapshot import build_trader_snapshot
from phoenix.trader_snapshot_runtime import build_runtime_trader_snapshot


def fresh_snapshot(**overrides):
    base = build_trader_snapshot(
        market_data={
            "regime": "TREND_UP",
            "direction_lock": "BOTH_ALLOWED",
            "btc_trend_1m": 0.2,
            "btc_trend_5m": 0.4,
            "eth_trend_1m": 0.1,
            "eth_trend_5m": 0.3,
            "volatility": 1.0,
        },
        account_state={
            "trading_allowed": True,
            "daily_pnl_pct": 0.0,
            "daily_loss_remaining_pct": 3.0,
            "open_positions_count": 0,
            "max_open_positions": 1,
            "loss_streak": 0,
            "cooldown_active": False,
        },
        positions=[],
        candidates=[
            {
                "symbol": "BTCUSDT",
                "bias": "LONG",
                "setup_type": "QUICK_TRADE",
                "score": 90.0,
                "current_price": 100.0,
                "spread_bps": 1.0,
                "estimated_slippage_bps": 1.0,
                "liquidity_ok": True,
                "suggested_stop_pct": 0.6,
                "suggested_tp_pct": 1.2,
                "max_holding_time_sec": 900,
            }
        ],
        system_status={
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
            "protective_stop_path_available": True,
            "emergency_close_available": True,
            "position_state": "known",
            "stop_protection_status": "healthy",
            "candidate_state": "known",
            "source": "unit_test",
        },
    )
    for key, value in overrides.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key].update(value)
        else:
            base[key] = value
    return base


def valid_enter_long():
    return {
        "action": "ENTER_LONG",
        "symbol": "BTCUSDT",
        "trade_type": "QUICK_TRADE",
        "confidence": 0.8,
        "reason": "Hermes saw a clean continuation setup",
        "stop_loss_pct": 0.6,
        "take_profit_pct": 1.2,
        "max_holding_time_sec": 900,
        "source": "HERMES",
    }


class HermesTraderTrialPipelineTests(unittest.TestCase):
    def test_runtime_snapshot_unavailable_marks_no_trade_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = build_runtime_trader_snapshot(root=tmp)
            risk = evaluate_risk(
                validate_hermes_decision(valid_enter_long()).decision or {},
                snapshot,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            )

            self.assertFalse(snapshot["system_status"]["data_fresh"])
            self.assertEqual(snapshot["system_status"]["websocket_status"], "unavailable")
            self.assertFalse(risk.approved)
            self.assertIn("stale_snapshot", risk.blocked_by)

    def test_runtime_snapshot_uses_compact_market_stream_and_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "btc_data" / "state"
            state.mkdir(parents=True)
            now = datetime.now(timezone.utc).isoformat()
            (state / "market_stream_state.json").write_text(json.dumps({"status": "connected", "last_event_at": now}), encoding="utf-8")
            (state / "market_stream_snapshot.json").write_text(
                json.dumps(
                    {
                        "generated_at": now,
                        "source": "ws",
                        "per_symbol": {
                            "BTCUSDT": {
                                "price_change_5m_pct": 0.3,
                                "price_change_15m_pct": 0.5,
                                "mid_price": 100.0,
                                "depth": {
                                    "spread_bps": 1.0,
                                    "depth_bid_5": 100000,
                                    "depth_ask_5": 90000,
                                    "depth_imbalance": 0.05,
                                    "estimated_slippage_bps_buy": 1.1,
                                },
                            },
                            "ETHUSDT": {
                                "price_change_5m_pct": 0.2,
                                "price_change_15m_pct": 0.4,
                                "mid_price": 50.0,
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            (root / "phoenix_candidates.json").write_text(
                json.dumps({"top_candidates": [{"symbol": "BTCUSDT", "score": 88.0, "directional_bias": "LONG"}]}),
                encoding="utf-8",
            )

            snapshot = build_runtime_trader_snapshot(root=root, protective_stop_path_available=True, emergency_close_available=True)

            self.assertTrue(snapshot["system_status"]["data_fresh"])
            self.assertEqual(snapshot["system_status"]["websocket_status"], "healthy")
            self.assertEqual(snapshot["top_candidates"][0]["symbol"], "BTCUSDT")
            self.assertEqual(snapshot["top_candidates"][0]["spread_bps"], 1.0)

    def test_invalid_file_provider_json_falls_back_to_no_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "decision.json"
            path.write_text(json.dumps({"action": "ENTER_LONG", "symbol": "BTCUSDT", "source": "HERMES"}), encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(path).decide(fresh_snapshot()))

            self.assertFalse(result.ok)
            self.assertEqual(result.decision["action"], "NO_TRADE")
            self.assertIn("hermes_decision_invalid", result.fallback_reason or "")

    def test_file_provider_valid_decision_passes_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "decision.json"
            path.write_text(json.dumps(valid_enter_long()), encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(path).decide(fresh_snapshot()))

            self.assertTrue(result.ok)
            self.assertEqual(result.decision["action"], "ENTER_LONG")

    def test_http_provider_timeout_falls_back_to_no_trade(self) -> None:
        provider = HttpHermesDecisionProvider("http://127.0.0.1:1", timeout_sec=0.01)

        async def slow(_snapshot):
            await asyncio.sleep(0.1)
            return valid_enter_long()

        provider._post_snapshot = slow  # type: ignore[method-assign]
        result = asyncio.run(provider.decide(fresh_snapshot()))

        self.assertFalse(result.ok)
        self.assertEqual(result.decision["action"], "NO_TRADE")
        self.assertEqual(result.fallback_reason, "http_provider_timeout")

    def test_approved_testnet_cycle_calls_executor_callback_after_risk(self) -> None:
        calls = []

        async def callback(intent):
            calls.append(intent)
            return {"order_submitted": True, "status": "fake_testnet_order"}

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_payload=fresh_snapshot(),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=callback,
            )

            self.assertEqual(len(calls), 1)
            self.assertTrue(result["risk_governor_result"]["approved"])
            self.assertTrue(result["execution_result"]["order_submitted"])

    def test_risk_reject_prevents_executor_callback(self) -> None:
        calls = []

        async def callback(intent):
            calls.append(intent)
            return {"order_submitted": True}

        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_payload=fresh_snapshot(system_status={"data_fresh": False}),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=False,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
                executor_callback=callback,
            )

            self.assertEqual(calls, [])
            self.assertFalse(result["risk_governor_result"]["approved"])
            self.assertFalse(result["execution_result"]["order_submitted"])

    def test_replay_log_contains_full_cycle_events_with_trace_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_trader_cycle(
                snapshot_payload=fresh_snapshot(),
                decision_provider=MockHermesDecisionProvider(valid_enter_long()),
                output_dir=tmp,
                dry_run=True,
                environment={"runtime_mode": "TESTNET_LIVE", "env": "testnet"},
            )
            rows = [json.loads(line) for line in (Path(tmp) / "replay_events.jsonl").read_text(encoding="utf-8").splitlines()]
            events = {row["event"] for row in rows}

            self.assertIn("snapshot", events)
            self.assertIn("hermes_decision_raw", events)
            self.assertIn("hermes_decision_validated", events)
            self.assertIn("risk_governor_result", events)
            self.assertIn("safe_order_gateway_result", events)
            self.assertIn("execution_intent", events)
            self.assertIn("execution_result", events)
            self.assertIn("review_report", events)
            self.assertTrue(all(row.get("trace_id") for row in rows))
            self.assertEqual(result["execution_result"]["status"], "dry_run_intent_only")

    def test_review_reporter_outputs_readable_cantonese(self) -> None:
        report = build_review_report("RISK_REJECT", {"action": "ENTER_LONG", "symbol": "BTCUSDT", "blocked_by": ["spread_too_wide"]})

        self.assertIn("【Risk Governor 拒绝】", report.text)
        self.assertIn("Phoenix 唔会执行", report.text)
        self.assertIn("BTCUSDT", report.text)


if __name__ == "__main__":
    unittest.main()
