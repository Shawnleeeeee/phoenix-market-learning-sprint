import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from phoenix.hermes_decision_loop import (
    HEARTBEAT_FILE_NAME,
    WRITER_NAME,
    HermesDecisionLoop,
    build_calibration_decision,
    build_conservative_decision,
    build_decision,
    write_json_atomic,
)


def runtime_snapshot(trace_id: str, *, candidates: list[dict] | None = None, market_regime: dict | None = None) -> dict:
    return {
        "trace_id": trace_id,
        "system_status": {
            "trace_id": trace_id,
            "source": "runtime",
            "snapshot_source": "runtime",
            "trusted_runtime_snapshot": True,
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
            "candidate_state": "fresh" if candidates else "empty",
        },
        "market_regime": market_regime
        or {
            "regime": "TREND_UP",
            "direction_lock": "BOTH_ALLOWED",
            "btc_trend_1m": 0.12,
            "btc_trend_5m": 0.30,
            "eth_trend_1m": 0.06,
            "eth_trend_5m": 0.18,
        },
        "top_candidates": candidates or [],
    }


def strong_candidate(**overrides: object) -> dict:
    payload = {
        "symbol": "SOLUSDT",
        "bias": "LONG",
        "setup_type": "QUICK_TRADE",
        "score": 18.0,
        "current_price": 142.42,
        "spread_bps": 0.7,
        "estimated_slippage_bps": 1.1,
        "estimated_fee_bps": 4.0,
        "liquidity_ok": True,
        "price_change_1m_pct": 0.12,
        "price_change_5m_pct": 0.42,
        "momentum_follow_through": True,
        "invalidation_distance_bps": 60.0,
        "suggested_stop_pct": 0.6,
        "suggested_tp_pct": 1.2,
        "max_holding_time_sec": 300,
        "invalidation_hint": "breakout candle loses follow-through",
    }
    payload.update(overrides)
    return payload


class HermesDecisionLoopTests(unittest.TestCase):
    def test_process_once_writes_matching_decision_with_hermes_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inbox = root / "inbox"
            outbox = root / "outbox"
            logs = root / "logs"
            inbox.mkdir()
            trace_id = "loop_trace_001"
            (inbox / f"snapshot_{trace_id}.json").write_text(
                json.dumps(runtime_snapshot(trace_id), ensure_ascii=False),
                encoding="utf-8",
            )

            loop = HermesDecisionLoop(inbox_dir=inbox, outbox_dir=outbox, log_dir=logs)
            result = loop.process_once()

            decision_path = outbox / f"decision_{trace_id}.json"
            decision = json.loads(decision_path.read_text(encoding="utf-8"))
            self.assertEqual(result["processed_this_pass"], 1)
            self.assertEqual(decision["trace_id"], trace_id)
            self.assertEqual(decision["source"], "HERMES")
            self.assertEqual(decision["writer"], WRITER_NAME)
            self.assertEqual(decision["action"], "NO_TRADE")
            self.assertEqual(decision["decision"], "NO_TRADE")
            self.assertEqual(decision["timestamp"], decision["created_at"])
            self.assertIn("无新鲜候选机会", decision["reason"])

    def test_candidate_snapshot_writes_wait_for_trigger_not_entry(self) -> None:
        trace_id = "loop_trace_002"
        decision = build_conservative_decision(
            runtime_snapshot(
                trace_id,
                candidates=[
                    {
                        "symbol": "TRXUSDT",
                        "bias": "SHORT",
                        "score": 0.73,
                        "spread_bps": 0.2,
                        "liquidity_ok": True,
                    }
                ],
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "WAIT_FOR_TRIGGER")
        self.assertEqual(decision["decision"], "WAIT_FOR_TRIGGER")
        self.assertEqual(decision["timestamp"], decision["created_at"])
        self.assertEqual(decision["source"], "HERMES")
        self.assertEqual(decision["writer"], WRITER_NAME)
        self.assertEqual(decision["symbol"], "TRXUSDT")

    def test_calibration_policy_candidate_can_emit_complete_enter_long(self) -> None:
        trace_id = "calibration_enter_long"
        decision = build_calibration_decision(
            runtime_snapshot(trace_id, candidates=[strong_candidate()]),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "ENTER_LONG")
        self.assertEqual(decision["decision"], "ENTER_LONG")
        self.assertEqual(decision["timestamp"], decision["created_at"])
        self.assertEqual(decision["symbol"], "SOLUSDT")
        self.assertEqual(decision["side"], "LONG")
        self.assertEqual(decision["trade_type"], "QUICK_TRADE")
        self.assertEqual(decision["entry_price_hint"], 142.42)
        self.assertEqual(decision["stop_loss_pct"], 0.6)
        self.assertEqual(decision["take_profit_pct"], 1.2)
        self.assertEqual(decision["max_holding_time_sec"], 300)
        self.assertEqual(decision["invalidation_condition"], "breakout candle loses follow-through")
        self.assertGreaterEqual(decision["confidence"], 0.55)
        self.assertIn("Stage 2 testnet exploration", decision["reason"])
        self.assertIn("Binance testnet sandbox", decision["reason"])
        self.assertNotIn("dry-run", decision["reason"])
        self.assertEqual(decision["entry_quality_filter"], "stage2_v0.4_entry_quality")
        self.assertTrue(decision["entry_quality_allowed"])
        self.assertIn("Entry quality passed", decision["entry_quality_reason"])

    def test_calibration_policy_candidate_can_emit_complete_enter_short(self) -> None:
        trace_id = "calibration_enter_short"
        decision = build_calibration_decision(
            runtime_snapshot(
                trace_id,
                candidates=[
                    strong_candidate(
                        symbol="ETHUSDT",
                        bias="SHORT",
                        price_change_1m_pct=-0.12,
                        price_change_5m_pct=-0.42,
                    )
                ],
                market_regime={
                    "regime": "TREND_DOWN",
                    "btc_trend_1m": -0.10,
                    "btc_trend_5m": -0.25,
                    "eth_trend_1m": 0.02,
                    "eth_trend_5m": -0.08,
                },
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "ENTER_SHORT")
        self.assertEqual(decision["symbol"], "ETHUSDT")
        self.assertEqual(decision["side"], "SHORT")
        self.assertEqual(decision["stop_loss_pct"], 0.6)
        self.assertEqual(decision["take_profit_pct"], 1.2)
        self.assertTrue(decision["entry_quality_allowed"])

    def test_calibration_trend_down_short_late_chase_returns_no_trade(self) -> None:
        trace_id = "calibration_entry_quality_late_short"
        decision = build_calibration_decision(
            runtime_snapshot(
                trace_id,
                candidates=[
                    strong_candidate(
                        symbol="ETHUSDT",
                        bias="SHORT",
                        price_change_1m_pct=-1.2,
                        price_change_5m_pct=-2.1,
                    )
                ],
                market_regime={
                    "regime": "TREND_DOWN",
                    "btc_trend_1m": -0.10,
                    "btc_trend_5m": -0.25,
                },
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertFalse(decision["entry_quality_allowed"])
        self.assertIn("entry_quality_filter_failed", decision["blocked_by"])
        self.assertIn("late_chase_short", decision["blocked_by"])

    def test_calibration_trend_down_short_without_btc_eth_weakness_returns_no_trade(self) -> None:
        trace_id = "calibration_entry_quality_alignment"
        decision = build_calibration_decision(
            runtime_snapshot(
                trace_id,
                candidates=[
                    strong_candidate(
                        symbol="ETHUSDT",
                        bias="SHORT",
                        price_change_1m_pct=-0.12,
                        price_change_5m_pct=-0.42,
                    )
                ],
                market_regime={
                    "regime": "TREND_DOWN",
                    "btc_trend_1m": 0.03,
                    "btc_trend_5m": 0.01,
                    "eth_trend_1m": 0.02,
                },
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertFalse(decision["entry_quality_allowed"])
        self.assertIn("btc_eth_alignment_missing", decision["blocked_by"])

    def test_calibration_micro_notional_infeasible_candidate_returns_no_trade(self) -> None:
        trace_id = "calibration_micro_infeasible"
        decision = build_calibration_decision(
            runtime_snapshot(
                trace_id,
                candidates=[
                    strong_candidate(
                        symbol="AVAXUSDT",
                        current_price=24.0,
                        exchange_filter_checked=True,
                        symbol_tradeable=True,
                        micro_notional_feasible=False,
                        rounded_qty=0.0,
                        min_qty=1.0,
                        step_size=1.0,
                        min_notional=5.0,
                        required_quote_allocation_usdt=12.0,
                        configured_quote_allocation_usdt=5.0,
                        configured_leverage=2,
                        max_quote_allocation_usdt=10.0,
                        infeasible_reason="min_qty_not_met,required_quote_allocation_exceeds_max",
                    )
                ],
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertIn("micro notional", decision["reason"])
        self.assertIn("AVAXUSDT", decision["reason"])

    def test_calibration_policy_never_waits_for_trigger(self) -> None:
        trace_id = "calibration_no_wait"
        decision = build_decision(runtime_snapshot(trace_id, candidates=[strong_candidate()]), trace_id=trace_id, policy="calibration")

        self.assertNotEqual(decision["action"], "WAIT_FOR_TRIGGER")

    def test_calibration_low_score_returns_no_trade_with_specific_blocker(self) -> None:
        trace_id = "calibration_low_score"
        decision = build_calibration_decision(
            runtime_snapshot(trace_id, candidates=[strong_candidate(score=5.0)]),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertIn("score 不够", decision["reason"])

    def test_calibration_spread_too_wide_returns_no_trade_with_specific_blocker(self) -> None:
        trace_id = "calibration_spread"
        decision = build_calibration_decision(
            runtime_snapshot(trace_id, candidates=[strong_candidate(spread_bps=20.0)]),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertIn("spread 太大", decision["reason"])

    def test_calibration_liquidity_poor_returns_no_trade_with_specific_blocker(self) -> None:
        trace_id = "calibration_liquidity"
        decision = build_calibration_decision(
            runtime_snapshot(trace_id, candidates=[strong_candidate(liquidity_ok=False)]),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertIn("liquidity 不够", decision["reason"])

    def test_calibration_missing_risk_plan_returns_no_trade_with_specific_blocker(self) -> None:
        trace_id = "calibration_missing_risk_plan"
        decision = build_calibration_decision(
            runtime_snapshot(trace_id, candidates=[strong_candidate(suggested_stop_pct="unavailable")]),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertIn("stop / tp 不合理", decision["reason"])

    def test_calibration_direction_lock_conflict_returns_no_trade(self) -> None:
        trace_id = "calibration_direction_lock"
        snapshot = runtime_snapshot(trace_id, candidates=[strong_candidate(bias="LONG")])
        snapshot["market_regime"] = {"regime": "TREND_DOWN", "direction_lock": "SHORT_ONLY_OR_NO_TRADE"}

        decision = build_calibration_decision(snapshot, trace_id=trace_id)

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertFalse(decision["direction_regime_allowed"])
        self.assertEqual(decision["allowed_direction"], "SHORT")
        self.assertIn("direction_regime_mismatch", decision["blocked_by"])

    def test_calibration_trend_up_short_returns_no_trade_without_lock(self) -> None:
        trace_id = "calibration_trend_up_short"
        decision = build_calibration_decision(
            runtime_snapshot(
                trace_id,
                candidates=[strong_candidate(bias="SHORT")],
                market_regime={"regime": "TREND_UP"},
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertFalse(decision["direction_regime_allowed"])
        self.assertEqual(decision["allowed_direction"], "LONG")
        self.assertIn("direction_regime_mismatch", decision["blocked_by"])

    def test_calibration_high_vol_returns_no_trade(self) -> None:
        trace_id = "calibration_high_vol"
        decision = build_calibration_decision(
            runtime_snapshot(
                trace_id,
                candidates=[strong_candidate(bias="LONG")],
                market_regime={"regime": "HIGH_VOL", "direction_lock": "BOTH_ALLOWED"},
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertFalse(decision["direction_regime_allowed"])
        self.assertEqual(decision["allowed_direction"], "NONE")
        self.assertIn("direction_regime_high_vol", decision["blocked_by"])

    def test_calibration_chop_non_edge_returns_no_trade(self) -> None:
        trace_id = "calibration_chop_non_edge"
        decision = build_calibration_decision(
            runtime_snapshot(
                trace_id,
                candidates=[strong_candidate(setup_type="QUICK_TRADE")],
                market_regime={"regime": "CHOP", "direction_lock": "BOTH_ALLOWED"},
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertFalse(decision["direction_regime_allowed"])
        self.assertEqual(decision["allowed_direction"], "RANGE_EDGE_ONLY")
        self.assertIn("chop_requires_range_edge", decision["blocked_by"])

    def test_calibration_unknown_market_regime_returns_no_trade(self) -> None:
        trace_id = "calibration_unknown_regime"
        decision = build_calibration_decision(
            runtime_snapshot(
                trace_id,
                candidates=[strong_candidate()],
                market_regime={"regime": "UNKNOWN", "direction_lock": "BOTH_ALLOWED"},
            ),
            trace_id=trace_id,
        )

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertIn("market_regime=UNKNOWN", decision["reason"])

    def test_conservative_policy_still_waits_for_trigger(self) -> None:
        trace_id = "conservative_still_waits"
        decision = build_decision(runtime_snapshot(trace_id, candidates=[strong_candidate()]), trace_id=trace_id)

        self.assertEqual(decision["action"], "WAIT_FOR_TRIGGER")

    def test_loop_can_use_calibration_policy_without_overwriting_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inbox = root / "inbox"
            outbox = root / "outbox"
            logs = root / "logs"
            inbox.mkdir()
            trace_id = "loop_calibration_policy"
            (inbox / f"snapshot_{trace_id}.json").write_text(
                json.dumps(runtime_snapshot(trace_id, candidates=[strong_candidate()]), ensure_ascii=False),
                encoding="utf-8",
            )

            result = HermesDecisionLoop(inbox_dir=inbox, outbox_dir=outbox, log_dir=logs, decision_policy="calibration").process_once()

            decision = json.loads((outbox / f"decision_{trace_id}.json").read_text(encoding="utf-8"))
            self.assertEqual(result["processed_this_pass"], 1)
            self.assertEqual(decision["action"], "ENTER_LONG")
            self.assertEqual(decision["decision"], "ENTER_LONG")
            self.assertEqual(decision["timestamp"], decision["created_at"])
            self.assertEqual(decision["source"], "HERMES")
            self.assertEqual(decision["writer"], WRITER_NAME)

    def test_untrusted_snapshot_fails_closed_to_no_trade(self) -> None:
        trace_id = "loop_trace_003"
        snapshot = runtime_snapshot(trace_id)
        snapshot["system_status"]["trusted_runtime_snapshot"] = False

        decision = build_conservative_decision(snapshot, trace_id=trace_id)

        self.assertEqual(decision["action"], "NO_TRADE")
        self.assertIn("trusted_runtime_snapshot_false", decision["reason"])

    def test_existing_decision_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inbox = root / "inbox"
            outbox = root / "outbox"
            logs = root / "logs"
            inbox.mkdir()
            outbox.mkdir()
            trace_id = "loop_trace_004"
            snapshot_path = inbox / f"snapshot_{trace_id}.json"
            decision_path = outbox / f"decision_{trace_id}.json"
            snapshot_path.write_text(json.dumps(runtime_snapshot(trace_id)), encoding="utf-8")
            decision_path.write_text('{"trace_id":"loop_trace_004","action":"NO_TRADE","source":"HERMES"}\n', encoding="utf-8")

            result = HermesDecisionLoop(inbox_dir=inbox, outbox_dir=outbox, log_dir=logs).process_once()

            self.assertEqual(result["processed_this_pass"], 0)
            self.assertEqual(result["skipped_this_pass"], 1)
            self.assertEqual(
                decision_path.read_text(encoding="utf-8"),
                '{"trace_id":"loop_trace_004","action":"NO_TRADE","source":"HERMES"}\n',
            )

    def test_existing_decision_backlog_does_not_starve_new_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inbox = root / "inbox"
            outbox = root / "outbox"
            logs = root / "logs"
            inbox.mkdir()
            outbox.mkdir()
            old_mtime = time.time() - 3600
            for index in range(20):
                trace_id = f"old_trace_{index:03d}"
                snapshot_path = inbox / f"snapshot_{trace_id}.json"
                decision_path = outbox / f"decision_{trace_id}.json"
                snapshot_path.write_text(json.dumps(runtime_snapshot(trace_id)), encoding="utf-8")
                decision_path.write_text(
                    json.dumps({"trace_id": trace_id, "source": "HERMES", "action": "NO_TRADE"}),
                    encoding="utf-8",
                )
                os.utime(snapshot_path, (old_mtime, old_mtime))
                os.utime(decision_path, (old_mtime, old_mtime))
            trace_id = "new_trace_should_not_starve"
            (inbox / f"snapshot_{trace_id}.json").write_text(
                json.dumps(runtime_snapshot(trace_id), ensure_ascii=False),
                encoding="utf-8",
            )

            result = HermesDecisionLoop(
                inbox_dir=inbox,
                outbox_dir=outbox,
                log_dir=logs,
                max_files_per_pass=1,
                max_archive_per_pass=0,
            ).process_once()

            self.assertEqual(result["processed_this_pass"], 1)
            self.assertTrue((outbox / f"decision_{trace_id}.json").exists())
            decision = json.loads((outbox / f"decision_{trace_id}.json").read_text(encoding="utf-8"))
            self.assertEqual(decision["trace_id"], trace_id)
            self.assertEqual(decision["source"], "HERMES")

    def test_processed_pairs_are_archived_after_age_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inbox = root / "inbox"
            outbox = root / "outbox"
            logs = root / "logs"
            archive = root / "archive"
            inbox.mkdir()
            outbox.mkdir()
            trace_id = "archive_trace_001"
            snapshot_path = inbox / f"snapshot_{trace_id}.json"
            decision_path = outbox / f"decision_{trace_id}.json"
            snapshot_path.write_text(json.dumps(runtime_snapshot(trace_id)), encoding="utf-8")
            decision_path.write_text(
                json.dumps({"trace_id": trace_id, "source": "HERMES", "action": "NO_TRADE"}),
                encoding="utf-8",
            )

            result = HermesDecisionLoop(
                inbox_dir=inbox,
                outbox_dir=outbox,
                log_dir=logs,
                archive_dir=archive,
                archive_processed_after_sec=0,
            ).process_once()

            self.assertEqual(result["archived_this_pass"], 1)
            self.assertFalse(snapshot_path.exists())
            self.assertFalse(decision_path.exists())
            archived = list((archive / "processed").glob("**/snapshot_archive_trace_001.json"))
            self.assertEqual(len(archived), 1)

    def test_heartbeat_contains_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            loop = HermesDecisionLoop(inbox_dir=root / "inbox", outbox_dir=root / "outbox", log_dir=root / "logs")

            loop.write_heartbeat(force=True)

            heartbeat = json.loads((root / "logs" / HEARTBEAT_FILE_NAME).read_text(encoding="utf-8"))
            self.assertTrue(heartbeat["running"])
            self.assertIn("last_seen_snapshot_trace_id", heartbeat)
            self.assertIn("last_written_decision_trace_id", heartbeat)
            self.assertIn("last_success_at", heartbeat)
            self.assertIn("processed_count", heartbeat)
            self.assertIn("error_count", heartbeat)
            self.assertIn("last_error", heartbeat)
            self.assertIn("loop_lag_sec", heartbeat)
            self.assertIn("pending_decision_count", heartbeat)
            self.assertIn("oldest_pending_snapshot_age_sec", heartbeat)
            self.assertIn("restart_reason", heartbeat)

    def test_watchdog_flags_old_pending_snapshot_for_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inbox = root / "inbox"
            outbox = root / "outbox"
            logs = root / "logs"
            inbox.mkdir()
            trace_id = "bad_pending_trace"
            snapshot_path = inbox / f"snapshot_{trace_id}.json"
            snapshot_path.write_text("{not-json", encoding="utf-8")
            old_mtime = time.time() - 120
            os.utime(snapshot_path, (old_mtime, old_mtime))
            loop = HermesDecisionLoop(
                inbox_dir=inbox,
                outbox_dir=outbox,
                log_dir=logs,
                watchdog_timeout_sec=60,
                max_archive_per_pass=0,
            )

            result = loop.process_once()

            self.assertEqual(result["errors_this_pass"], 1)
            self.assertTrue(loop.should_restart_for_lag())
            self.assertEqual(loop.state.pending_decision_count, 1)
            self.assertGreaterEqual(loop.state.loop_lag_sec, 60)

    def test_atomic_write_uses_tmp_then_final_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "decision_trace.json"

            write_json_atomic(path, {"trace_id": "trace", "source": "HERMES"})

            self.assertTrue(path.exists())
            self.assertFalse(Path(str(path) + ".tmp").exists())
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["source"], "HERMES")


if __name__ == "__main__":
    unittest.main()
