import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from phoenix.candidate_feed_producer import (
    HEARTBEAT_FILE_NAME,
    build_collect_command,
    collect_candidate_feed_health,
    write_json_atomic,
)


def iso_age(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


class CandidateFeedProducerTests(unittest.TestCase):
    def test_build_collect_command_is_persistent_and_orderless(self) -> None:
        command = build_collect_command(
            python_bin="/opt/phoenix-testnet/.venv/bin/python",
            output_dir="/opt/phoenix-testnet/signal_lab_runs/event_collect_v6_speed_boost",
            env="prod",
            cycle_sec=45,
            horizons_sec="60,180,300,900,1800,3600",
            scan_top=120,
            universe_top=200,
            max_events_per_cycle=50,
            worker_count=3,
            kline_concurrency=5,
            min_quote_volume=5_000_000,
            trigger_min_quote_volume=5_000_000,
            round_trip_fee_bps=8,
        )

        self.assertIn("phoenix_signal_lab.py", command)
        self.assertIn("collect", command)
        self.assertIn("--duration-sec", command)
        self.assertEqual(command[command.index("--duration-sec") + 1], "0")
        self.assertNotIn("phoenix_testnet_round_runner.py", command)
        self.assertNotIn("phoenix_live_execute.py", command)
        self.assertNotIn("new_order", " ".join(command))

    def test_health_counts_fresh_and_stale_real_event_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            event_snapshots = output_dir / "event_snapshots.jsonl"
            rows = [
                {"event": "market_event_created", "symbol": "BTCUSDT", "observed_at": iso_age(10)},
                {"event": "market_event_created", "symbol": "ETHUSDT", "observed_at": iso_age(300)},
                {"event": "market_event_created", "symbol": "SOLUSDT", "observed_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000)},
            ]
            event_snapshots.write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            (output_dir / "bridge_event_feed.jsonl").write_text("{}", encoding="utf-8")

            health = collect_candidate_feed_health(output_dir, running=True, process_pid=123, stale_after_sec=60)
            payload = health.to_dict()

            self.assertTrue(payload["running"])
            self.assertEqual(payload["candidate_count"], 3)
            self.assertEqual(payload["fresh_count"], 2)
            self.assertEqual(payload["stale_count"], 1)
            self.assertEqual(payload["process_pid"], 123)
            self.assertIsNotNone(payload["latest_candidate_at"])
            self.assertLessEqual(payload["latest_candidate_age_sec"], 60)

    def test_health_does_not_fabricate_fresh_candidates_when_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            health = collect_candidate_feed_health(tmp, running=True, stale_after_sec=60).to_dict()

            self.assertTrue(health["running"])
            self.assertEqual(health["candidate_count"], 0)
            self.assertEqual(health["fresh_count"], 0)
            self.assertEqual(health["stale_count"], 0)
            self.assertIsNone(health["latest_candidate_at"])
            self.assertIsNone(health["latest_candidate_age_sec"])

    def test_atomic_heartbeat_write_uses_tmp_then_final_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / HEARTBEAT_FILE_NAME

            write_json_atomic(path, {"running": True, "candidate_count": 1})

            self.assertTrue(path.exists())
            self.assertFalse(Path(str(path) + ".tmp").exists())
            self.assertTrue(json.loads(path.read_text(encoding="utf-8"))["running"])


if __name__ == "__main__":
    unittest.main()
