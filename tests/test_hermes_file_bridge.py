import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from phoenix.hermes_decision_provider import FileHermesDecisionProvider
from phoenix.hermes_file_bridge import HermesFileBridge


def snapshot_with_trace(trace_id: str) -> dict[str, object]:
    return {
        "trace_id": trace_id,
        "system_status": {
            "snapshot_source": "unit_test",
            "data_fresh": True,
            "websocket_status": "healthy",
            "exchange_status": "healthy",
            "trusted_runtime_snapshot": True,
        },
    }


def valid_enter_long_decision() -> dict[str, object]:
    return {
        "action": "ENTER_LONG",
        "symbol": "BTCUSDT",
        "trade_type": "QUICK_TRADE",
        "confidence": 0.8,
        "reason": "unit test decision file",
        "stop_loss_pct": 0.6,
        "take_profit_pct": 1.2,
        "source": "HERMES",
    }


class HermesFileBridgeTests(unittest.TestCase):
    def test_write_snapshot_uses_trace_id_in_inbox_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace_id = "trace_file_bridge_001"
            bridge = HermesFileBridge(
                inbox_dir=root / "inbox",
                outbox_dir=root / "outbox",
                archive_dir=root / "archive",
                log_dir=root / "logs",
            )

            path = bridge.write_snapshot(trace_id, snapshot_with_trace(trace_id))
            payload = json.loads(path.read_text(encoding="utf-8"))
            log_rows = [
                json.loads(line)
                for line in (root / "logs" / "hermes_file_bridge_events.jsonl").read_text(encoding="utf-8").splitlines()
            ]

            self.assertEqual(path, root / "inbox" / f"snapshot_{trace_id}.json")
            self.assertEqual(payload["trace_id"], trace_id)
            self.assertEqual(payload["system_status"]["trace_id"], trace_id)
            self.assertEqual(log_rows[-1]["event"], "snapshot_written")
            self.assertEqual(log_rows[-1]["trace_id"], trace_id)

    def test_file_provider_reads_decision_from_outbox_trace_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_002"
            decision_path = outbox / f"decision_{trace_id}.json"
            decision_path.write_text(json.dumps({"decision": valid_enter_long_decision()}), encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(outbox).decide(snapshot_with_trace(trace_id)))

            self.assertTrue(result.ok)
            self.assertEqual(result.decision["action"], "ENTER_LONG")
            self.assertEqual(result.decision["symbol"], "BTCUSDT")
            self.assertEqual(result.raw_response["_decision_path"], str(decision_path))
            self.assertFalse(result.to_dict()["fallback_used"])
            self.assertEqual(result.to_dict()["decision_origin"], "outbox_file")
            self.assertEqual(result.to_dict()["normalized_decision"]["decision"], "ENTER_LONG")
            self.assertIn("raw_decision", result.to_dict())

    def test_missing_decision_times_out_to_no_trade_for_trace_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_003"

            result = asyncio.run(
                FileHermesDecisionProvider(outbox, timeout_sec=0.0).decide(snapshot_with_trace(trace_id))
            )

            self.assertFalse(result.ok)
            self.assertEqual(result.decision["action"], "NO_TRADE")
            self.assertIn(f"decision_{trace_id}.json", result.fallback_reason or "")
            self.assertEqual(result.raw_response["path"], str(outbox / f"decision_{trace_id}.json"))
            self.assertTrue(result.to_dict()["fallback_used"])
            self.assertEqual(result.to_dict()["decision_origin"], "timeout_fallback")
            self.assertIsNone(result.to_dict()["normalized_decision"])
            self.assertEqual(result.to_dict()["fallback_decision"]["action"], "NO_TRADE")
            self.assertNotEqual(result.to_dict()["fallback_decision"].get("source"), "HERMES")
            self.assertNotEqual(result.to_dict()["fallback_decision"].get("writer"), "Hermes Trader Brain")
            self.assertNotEqual(result.decision.get("source"), "HERMES")

    def test_file_provider_normalizes_action_only_decision_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_action_only"
            payload = valid_enter_long_decision()
            payload["created_at"] = "2026-05-12T22:00:00+00:00"
            (outbox / f"decision_{trace_id}.json").write_text(json.dumps(payload), encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(outbox).decide(snapshot_with_trace(trace_id)))
            result_payload = result.to_dict()

            self.assertTrue(result.ok)
            self.assertEqual(result.decision["action"], "ENTER_LONG")
            self.assertEqual(result_payload["normalized_decision"]["decision"], "ENTER_LONG")
            self.assertEqual(result_payload["normalized_decision"]["timestamp"], "2026-05-12T22:00:00+00:00")

    def test_file_provider_normalizes_decision_only_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_decision_only"
            (outbox / f"decision_{trace_id}.json").write_text(
                json.dumps(
                    {
                        "decision": "NO_TRADE",
                        "timestamp": "2026-05-12T22:01:00+00:00",
                        "reason": "no clean setup",
                        "source": "HERMES",
                        "writer": "Hermes Trader Brain",
                    }
                ),
                encoding="utf-8",
            )

            result = asyncio.run(FileHermesDecisionProvider(outbox).decide(snapshot_with_trace(trace_id)))
            result_payload = result.to_dict()

            self.assertTrue(result.ok)
            self.assertEqual(result.decision["action"], "NO_TRADE")
            self.assertEqual(result_payload["normalized_decision"]["action"], "NO_TRADE")
            self.assertEqual(result_payload["normalized_decision"]["created_at"], "2026-05-12T22:01:00+00:00")
            self.assertEqual(result_payload["normalized_decision"]["writer"], "Hermes Trader Brain")

    def test_file_provider_preserves_no_trade_direction_regime_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_direction_regime_no_trade"
            (outbox / f"decision_{trace_id}.json").write_text(
                json.dumps(
                    {
                        "decision": "NO_TRADE",
                        "action": "NO_TRADE",
                        "timestamp": "2026-05-12T22:01:00+00:00",
                        "reason": "BTC / market regime block",
                        "source": "HERMES",
                        "writer": "Hermes Trader Brain",
                        "symbol": "BTCUSDT",
                        "market_regime": "TREND_DOWN",
                        "candidate_direction": "LONG",
                        "allowed_direction": "SHORT",
                        "direction_regime_allowed": False,
                        "direction_regime_reason": "direction-regime mismatch: TREND_DOWN permits SHORT only; candidate_direction=LONG.",
                        "blocked_by": ["direction_regime_mismatch", "direction_lock_conflict"],
                        "direction_regime_source": "direction_regime_matrix.v1",
                        "candidate_symbol": "BTCUSDT",
                        "no_trade_reason": "BTC / market regime block",
                    }
                ),
                encoding="utf-8",
            )

            result = asyncio.run(FileHermesDecisionProvider(outbox).decide(snapshot_with_trace(trace_id)))
            result_payload = result.to_dict()

            self.assertTrue(result.ok)
            normalized = result_payload["normalized_decision"]
            self.assertEqual(normalized["action"], "NO_TRADE")
            self.assertEqual(normalized["market_regime"], "TREND_DOWN")
            self.assertEqual(normalized["candidate_direction"], "LONG")
            self.assertEqual(normalized["allowed_direction"], "SHORT")
            self.assertFalse(normalized["direction_regime_allowed"])
            self.assertIn("TREND_DOWN permits SHORT", normalized["direction_regime_reason"])
            self.assertIn("direction_regime_mismatch", normalized["blocked_by"])
            self.assertEqual(normalized["candidate_symbol"], "BTCUSDT")
            self.assertEqual(normalized["normalized_decision"], "NO_TRADE")
            self.assertEqual(normalized["no_trade_reason"], "BTC / market regime block")

    def test_file_provider_rejects_action_decision_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_mismatch"
            payload = valid_enter_long_decision()
            payload["decision"] = "ENTER_SHORT"
            payload["created_at"] = "2026-05-12T22:02:00+00:00"
            (outbox / f"decision_{trace_id}.json").write_text(json.dumps(payload), encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(outbox).decide(snapshot_with_trace(trace_id)))
            result_payload = result.to_dict()

            self.assertFalse(result.ok)
            self.assertIn("hermes_decision_contract_invalid:decision_action_mismatch", result.fallback_reason or "")
            self.assertIn("decision_action_mismatch", result_payload["contract_errors"])
            self.assertEqual(result_payload["decision_origin"], "file_provider_fallback")

    def test_file_provider_marks_missing_timestamp_contract_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_missing_timestamp"
            payload = valid_enter_long_decision()
            (outbox / f"decision_{trace_id}.json").write_text(json.dumps(payload), encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(outbox).decide(snapshot_with_trace(trace_id)))
            result_payload = result.to_dict()

            self.assertTrue(result.ok)
            self.assertIn("decision_timestamp_missing", result_payload["contract_warnings"])
            self.assertIn("created_at", result_payload["normalized_decision"])
            self.assertIn("timestamp", result_payload["normalized_decision"])
            self.assertEqual(result_payload["normalized_decision"]["created_at"], result_payload["normalized_decision"]["timestamp"])

    def test_invalid_decision_json_falls_back_to_no_trade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_004"
            (outbox / f"decision_{trace_id}.json").write_text("{not valid json", encoding="utf-8")

            result = asyncio.run(FileHermesDecisionProvider(outbox).decide(snapshot_with_trace(trace_id)))

            self.assertFalse(result.ok)
            self.assertEqual(result.decision["action"], "NO_TRADE")
            self.assertIn("file_provider_error", result.fallback_reason or "")

    def test_file_provider_accepts_hermes_shorthand_decision_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp) / "outbox"
            outbox.mkdir()
            trace_id = "trace_file_bridge_004b"
            (outbox / f"decision_{trace_id}.json").write_text(
                json.dumps({"trace_id": trace_id, "decision": "NO_TRADE", "reason": "snapshot 未通过安全检查，唔交易。"}),
                encoding="utf-8",
            )

            result = asyncio.run(FileHermesDecisionProvider(outbox).decide(snapshot_with_trace(trace_id)))

            self.assertTrue(result.ok)
            self.assertEqual(result.decision["action"], "NO_TRADE")
            self.assertEqual(result.decision["trade_type"], "NONE")
            self.assertEqual(result.decision["source"], "HERMES")

    def test_archive_trace_copies_snapshot_and_decision_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace_id = "trace_file_bridge_005"
            bridge = HermesFileBridge(
                inbox_dir=root / "inbox",
                outbox_dir=root / "outbox",
                archive_dir=root / "archive",
                log_dir=root / "logs",
            )
            snapshot_path = bridge.write_snapshot(trace_id, snapshot_with_trace(trace_id))
            decision_path = bridge.decision_path(trace_id)
            decision_path.write_text(json.dumps({"decision": valid_enter_long_decision()}), encoding="utf-8")

            archived = bridge.archive_trace(trace_id)
            snapshot_archive = Path(str(archived["snapshot_archive_path"]))
            decision_archive = Path(str(archived["decision_archive_path"]))

            self.assertEqual(snapshot_archive, root / "archive" / f"snapshot_{trace_id}.json")
            self.assertEqual(decision_archive, root / "archive" / f"decision_{trace_id}.json")
            self.assertTrue(snapshot_path.exists())
            self.assertTrue(decision_path.exists())
            self.assertEqual(snapshot_archive.read_text(encoding="utf-8"), snapshot_path.read_text(encoding="utf-8"))
            self.assertEqual(decision_archive.read_text(encoding="utf-8"), decision_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
