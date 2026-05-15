import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from phoenix.trader_snapshot_runtime import build_trader_snapshot_from_dashboard_payload


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso_minutes_ago(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def dashboard_payload(
    *,
    account_ok: bool = True,
    generated_at: str | None = None,
    shadow_updated_at: str | None = None,
    account_probe: dict | None = None,
    include_account_probe: bool = True,
    market_data_updated_at: str | None = None,
    bridge_heartbeat_at: str | None = None,
    candidates_updated_at: str | None = None,
    seed_runtime_defaults: bool = True,
) -> dict:
    now = _iso_now()
    generated_at = generated_at or now
    shadow_updated_at = shadow_updated_at or now
    market_data_updated_at = market_data_updated_at or (now if seed_runtime_defaults else None)
    bridge_heartbeat_at = bridge_heartbeat_at or (now if seed_runtime_defaults else None)
    candidates_updated_at = candidates_updated_at or shadow_updated_at

    runtime: dict = {
        "phoenixProcesses": [
            {"pid": "123", "command": "python phoenix_signal_bridge.py --execution-mode TESTNET_LIVE"},
        ]
        if seed_runtime_defaults
        else [],
        "bridgeState": {"updated_at": bridge_heartbeat_at} if bridge_heartbeat_at else {},
        "bridgeHeartbeatAt": bridge_heartbeat_at,
        "marketDataUpdatedAt": market_data_updated_at,
        "candidatesUpdatedAt": candidates_updated_at,
    }

    testnet_payload: dict = {
        "accountSummary": {"availableBalance": 1000.0, "totalWalletBalance": 1000.0},
        "openPositions": [],
    }
    if include_account_probe:
        testnet_payload["accountProbe"] = account_probe or {
            "ok": account_ok,
            "source": "python_probe",
            "executedAt": now,
        }

    return {
        "generatedAt": generated_at,
        "paths": {
            "projectRoot": "/tmp/phoenix_project",
            "shadowRoot": "/tmp/phoenix_project/signal_lab_runs/vps_forward_shadow_mainnet_active/mainnet_shadow",
        },
        "runtime": runtime,
        "shadowSnapshot": {
            "updatedAt": shadow_updated_at,
            "orders": [
                {
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "simulated_entry_price": 100.0,
                    "spread_bps": 1.0,
                    "slippage_bps": 1.0,
                    "score": 99,
                }
            ],
        },
        "environmentSnapshots": {"testnet": testnet_payload},
    }


class TraderSnapshotDashboardAdapterTests(unittest.TestCase):
    def test_runtime_market_data_and_bridge_health_can_produce_trusted_snapshot_even_when_candidates_are_stale(self) -> None:
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(
                shadow_updated_at=_iso_minutes_ago(10),
                market_data_updated_at=_iso_now(),
                bridge_heartbeat_at=_iso_now(),
                candidates_updated_at=_iso_minutes_ago(10),
            ),
            protective_stop_path_available=True,
            emergency_close_available=True,
            protective_stop_capability_source="verified_code_path:stop",
            emergency_close_capability_source="verified_code_path:emergency",
        )

        status = snapshot["system_status"]
        self.assertEqual(status["snapshot_source"], "runtime")
        self.assertEqual(status["source"], "runtime")
        self.assertTrue(status["trusted_runtime_snapshot"])
        self.assertTrue(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "healthy")
        self.assertEqual(status["exchange_status"], "healthy")
        self.assertEqual(status["market_data_updated_at"] is not None, True)
        self.assertEqual(status["bridge_heartbeat_at"] is not None, True)
        self.assertEqual(status["candidates_updated_at"].endswith("+00:00"), True)
        self.assertTrue(status["freshness_reason"] is None)
        self.assertEqual(status["account_state_source"], "signed_account")
        self.assertEqual(status["position_state_source"], "signed_positions")
        self.assertEqual(status["position_state"], "known")
        self.assertTrue(status["take_profit_path_available"])
        self.assertEqual(status["take_profit_capability_source"], "verified_code_path:stop")
        self.assertTrue(status["take_profit_order_supported"])
        self.assertEqual(status["shadow_orders_count"], 1)
        self.assertEqual(status["candidate_source"], "unavailable")
        self.assertEqual(status["candidate_empty_reason"], "candidate_source_missing")
        self.assertEqual(snapshot["top_candidates"], [])

    def test_missing_account_probe_does_not_fabricate_exchange_health(self) -> None:
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(include_account_probe=False),
            protective_stop_path_available=True,
            emergency_close_available=True,
            protective_stop_capability_source="verified_code_path:stop",
            emergency_close_capability_source="verified_code_path:emergency",
        )

        status = snapshot["system_status"]
        self.assertFalse(status["trusted_runtime_snapshot"])
        self.assertEqual(status["exchange_status"], "unavailable")
        self.assertEqual(status["account_state_source"], "dashboard_api_unavailable")
        self.assertEqual(status["position_state"], "unknown")
        self.assertEqual(status["position_state_source"], "unknown")
        self.assertIn("account_state_unknown", status["freshness_reason"])

    def test_old_account_probe_timestamp_is_not_trusted_and_does_not_fabricate_health(self) -> None:
        old_probe_time = _iso_minutes_ago(10)
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(
                account_probe={
                    "ok": True,
                    "source": "python_probe",
                    "executedAt": old_probe_time,
                    "checkNames": ["signed_balance_v3", "signed_position_v3"],
                },
                market_data_updated_at=_iso_now(),
                bridge_heartbeat_at=_iso_now(),
            ),
            protective_stop_path_available=True,
            emergency_close_available=True,
            protective_stop_capability_source="verified_code_path:stop",
            emergency_close_capability_source="verified_code_path:emergency",
        )

        status = snapshot["system_status"]
        self.assertEqual(status["snapshot_source"], "runtime")
        self.assertEqual(status["source"], "runtime")
        self.assertFalse(status["trusted_runtime_snapshot"])
        self.assertEqual(status["exchange_status"], "unavailable")
        self.assertEqual(status["account_state_source"], "signed_account")
        self.assertEqual(status["position_state"], "known")
        self.assertEqual(status["position_state_source"], "signed_positions")
        self.assertIn("exchange_unknown", status["freshness_reason"] or "")
        self.assertEqual(status["freeze_reason"], "runtime_snapshot_not_trusted_for_testnet_order")

    def test_stale_shadow_timestamp_does_not_block_fresh_market_data(self) -> None:
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(
                shadow_updated_at=_iso_minutes_ago(10),
                market_data_updated_at=_iso_now(),
                bridge_heartbeat_at=_iso_now(),
                candidates_updated_at=_iso_minutes_ago(10),
            ),
            protective_stop_path_available=True,
            emergency_close_available=True,
            protective_stop_capability_source="verified_code_path:stop",
            emergency_close_capability_source="verified_code_path:emergency",
        )

        status = snapshot["system_status"]
        self.assertTrue(status["trusted_runtime_snapshot"])
        self.assertTrue(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "healthy")
        self.assertIn("candidates_updated_at", status)
        self.assertTrue(str(status["candidates_updated_at"]).endswith("+00:00"))
        self.assertNotIn("candidates_state_stale", status.get("freshness_reason") or "")

    def test_market_data_stale_forces_untrusted_snapshot(self) -> None:
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(
                market_data_updated_at=_iso_minutes_ago(10),
                bridge_heartbeat_at=_iso_now(),
            ),
            protective_stop_path_available=True,
            emergency_close_available=True,
            protective_stop_capability_source="verified_code_path:stop",
            emergency_close_capability_source="verified_code_path:emergency",
        )

        status = snapshot["system_status"]
        self.assertFalse(status["trusted_runtime_snapshot"])
        self.assertFalse(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "unavailable")
        self.assertIn("market_data_stale", status["freshness_reason"] or "")

    def test_bridge_heartbeat_stale_forces_untrusted_snapshot(self) -> None:
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(
                market_data_updated_at=_iso_now(),
                bridge_heartbeat_at=_iso_minutes_ago(10),
            ),
            protective_stop_path_available=True,
            emergency_close_available=True,
            protective_stop_capability_source="verified_code_path:stop",
            emergency_close_capability_source="verified_code_path:emergency",
        )

        status = snapshot["system_status"]
        self.assertFalse(status["trusted_runtime_snapshot"])
        self.assertFalse(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "unavailable")
        self.assertIn("heartbeat_stale", status["freshness_reason"] or "")

    def test_generated_at_fresh_but_market_data_stale_is_not_fresh(self) -> None:
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(
                generated_at=_iso_now(),
                market_data_updated_at=_iso_minutes_ago(10),
                bridge_heartbeat_at=_iso_now(),
            ),
            protective_stop_path_available=True,
            emergency_close_available=True,
            protective_stop_capability_source="verified_code_path:stop",
            emergency_close_capability_source="verified_code_path:emergency",
        )

        status = snapshot["system_status"]
        self.assertFalse(status["data_fresh"])
        self.assertFalse(status["trusted_runtime_snapshot"])
        self.assertIn("market_data_stale", status["freshness_reason"] or "")

    def test_no_heartbeat_does_not_create_fake_healthy_defaults(self) -> None:
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(
                seed_runtime_defaults=False,
                market_data_updated_at=None,
                bridge_heartbeat_at=None,
                include_account_probe=False,
            ),
            protective_stop_path_available=False,
            emergency_close_available=False,
            protective_stop_capability_source="unverified",
            emergency_close_capability_source="unverified",
        )

        status = snapshot["system_status"]
        self.assertFalse(status["trusted_runtime_snapshot"])
        self.assertFalse(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "unavailable")
        self.assertEqual(status["exchange_status"], "unavailable")
        self.assertIn("market_data_missing", status["freshness_reason"] or "")
        self.assertIn("heartbeat_missing", status["freshness_reason"] or "")
        self.assertIn("account_state_unknown", status["freshness_reason"] or "")

    def test_bridge_state_inside_mainnet_shadow_uses_active_market_feed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            feed_path = base / "signal_lab_runs" / "event_collect_v6_speed_boost" / "bridge_event_feed.jsonl"
            feed_path.parent.mkdir(parents=True)
            feed_path.write_text(
                json.dumps(
                    {
                        "event": "market_event_created",
                        "symbol": "BTCUSDT",
                        "observed_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (shadow_root / "bridge-state.json").write_text(
                json.dumps({"snapshots_file": str(feed_path), "updated_at": _iso_now()}),
                encoding="utf-8",
            )
            payload = dashboard_payload(
                shadow_updated_at=_iso_minutes_ago(10),
                market_data_updated_at=None,
                bridge_heartbeat_at=None,
                seed_runtime_defaults=False,
            )
            payload["paths"]["shadowRoot"] = str(shadow_root)

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        status = snapshot["system_status"]
        self.assertTrue(status["trusted_runtime_snapshot"])
        self.assertTrue(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "healthy")
        self.assertEqual(status["market_data_updated_at"] is not None, True)
        self.assertEqual(status["bridge_heartbeat_at"] is not None, True)
        self.assertIsNone(status["freshness_reason"])

    def test_stale_event_feed_can_use_fresh_bridge_market_fetch_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            feed_path = base / "signal_lab_runs" / "event_collect_v6_speed_boost" / "bridge_event_feed.jsonl"
            feed_path.parent.mkdir(parents=True)
            stale_ms = int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp() * 1000)
            feed_path.write_text(
                json.dumps({"event": "market_event_created", "symbol": "BTCUSDT", "observed_at_ms": stale_ms}) + "\n",
                encoding="utf-8",
            )
            (shadow_root / "bridge-state.json").write_text(
                json.dumps({"snapshots_file": str(feed_path), "updated_at": _iso_now()}),
                encoding="utf-8",
            )
            log_dir = base / "logs"
            log_dir.mkdir()
            (log_dir / "mainnet_shadow_bridge_active.log").write_text(
                json.dumps({"event": "some_other_event", "at": _iso_minutes_ago(5)}) + "\n"
                + json.dumps(
                    {
                        "event": "signal_bridge_live_universe_refreshed",
                        "at": _iso_now(),
                        "tradable_symbol_count": 68,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            payload = dashboard_payload(
                shadow_updated_at=_iso_minutes_ago(10),
                market_data_updated_at=None,
                bridge_heartbeat_at=None,
                seed_runtime_defaults=False,
            )
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                root=base,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        status = snapshot["system_status"]
        self.assertTrue(status["trusted_runtime_snapshot"])
        self.assertTrue(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "healthy")
        self.assertEqual(status["feed_status"], "healthy")
        self.assertEqual(status["market_data_source_event"], "signal_bridge_live_universe_refreshed")
        self.assertIsNone(status["freshness_reason"])

    def test_public_book_ticker_heartbeat_can_refresh_market_data_when_bridge_feed_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            feed_path = base / "signal_lab_runs" / "event_collect_v6_speed_boost" / "bridge_event_feed.jsonl"
            feed_path.parent.mkdir(parents=True)
            feed_path.write_text(
                json.dumps(
                    {
                        "event": "market_event_created",
                        "symbol": "BTCUSDT",
                        "observed_at_ms": int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp() * 1000),
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (shadow_root / "bridge-state.json").write_text(
                json.dumps({"snapshots_file": str(feed_path), "updated_at": _iso_now()}),
                encoding="utf-8",
            )
            payload = dashboard_payload(
                shadow_updated_at=_iso_minutes_ago(10),
                market_data_updated_at=None,
                bridge_heartbeat_at=None,
                seed_runtime_defaults=False,
            )
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)

            with patch(
                "phoenix.trader_snapshot_runtime._fetch_public_book_ticker_heartbeat",
                return_value=(_iso_now(), "binance_public_book_ticker:BTCUSDT"),
            ):
                snapshot = build_trader_snapshot_from_dashboard_payload(
                    payload,
                    root=base,
                    public_market_heartbeat=True,
                    protective_stop_path_available=True,
                    emergency_close_available=True,
                    protective_stop_capability_source="verified_code_path:stop",
                    emergency_close_capability_source="verified_code_path:emergency",
                )

        status = snapshot["system_status"]
        self.assertTrue(status["trusted_runtime_snapshot"])
        self.assertTrue(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "healthy")
        self.assertEqual(status["feed_status"], "healthy")
        self.assertEqual(status["market_data_source_event"], "binance_public_book_ticker:BTCUSDT")
        self.assertIsNone(status["freshness_reason"])

    def test_public_book_ticker_heartbeat_refreshes_stale_bridge_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            (shadow_root / "bridge-state.json").write_text(
                json.dumps({"updated_at": _iso_minutes_ago(10)}),
                encoding="utf-8",
            )
            payload = dashboard_payload(
                shadow_updated_at=_iso_minutes_ago(10),
                market_data_updated_at=None,
                bridge_heartbeat_at=None,
                seed_runtime_defaults=False,
            )
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)

            with patch(
                "phoenix.trader_snapshot_runtime._fetch_public_book_ticker_heartbeat",
                return_value=(_iso_now(), "binance_public_book_ticker:BTCUSDT"),
            ):
                snapshot = build_trader_snapshot_from_dashboard_payload(
                    payload,
                    root=base,
                    public_market_heartbeat=True,
                    protective_stop_path_available=True,
                    emergency_close_available=True,
                    protective_stop_capability_source="verified_code_path:stop",
                    emergency_close_capability_source="verified_code_path:emergency",
                )

        status = snapshot["system_status"]
        self.assertTrue(status["trusted_runtime_snapshot"])
        self.assertTrue(status["data_fresh"])
        self.assertEqual(status["websocket_status"], "healthy")
        self.assertEqual(status["market_data_source_event"], "binance_public_book_ticker:BTCUSDT")
        self.assertEqual(status["bridge_heartbeat_source_event"], "binance_public_book_ticker:BTCUSDT")
        self.assertNotIn("heartbeat_stale", status.get("freshness_reason") or "")

    def test_orders_empty_can_use_fresh_bridge_state_pending_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            (shadow_root / "bridge-state.shadow.json").write_text(
                json.dumps(
                    {
                        "updated_at": _iso_now(),
                        "pending_by_event_id": {
                            "evt-1": {
                                "event_id": "evt-1",
                                "symbol": "ETHUSDT",
                                "side": "BUY",
                                "signal_time": _iso_now(),
                                "sample_price": 3200.5,
                                "spread_bps_at_entry": 0.8,
                                "estimated_slippage_bps": 1.2,
                                "liquidity_bucket": "major",
                                "one_min_return_pct": 0.45,
                                "one_min_volume_burst_ratio": 2.5,
                                "stop_loss_price_pct": 0.6,
                                "first_take_profit_price_pct": 1.2,
                                "max_hold_minutes": 8,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        status = snapshot["system_status"]
        self.assertEqual(status["candidate_source"], "bridge_state_shadow.pending_by_event_id")
        self.assertEqual(status["candidate_state"], "known")
        self.assertIsNone(status["candidate_empty_reason"])
        self.assertEqual(status["candidate_pool_count"], 1)
        self.assertEqual(snapshot["top_candidates"][0]["symbol"], "ETHUSDT")
        self.assertEqual(snapshot["top_candidates"][0]["bias"], "LONG")
        self.assertEqual(snapshot["top_candidates"][0]["current_price"], 3200.5)
        self.assertEqual(snapshot["top_candidates"][0]["liquidity_ok"], True)
        self.assertEqual(snapshot["top_candidates"][0]["spread_bps"], 0.8)
        self.assertEqual(snapshot["top_candidates"][0]["suggested_stop_pct"], 0.6)
        self.assertEqual(snapshot["top_candidates"][0]["suggested_tp_pct"], 1.2)
        self.assertEqual(snapshot["top_candidates"][0]["max_holding_time_sec"], 480)
        self.assertNotEqual(snapshot["top_candidates"][0]["invalidation_hint"], "unavailable")

    def test_stale_pending_candidates_are_not_exported_as_live_opportunities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            (shadow_root / "bridge-state.shadow.json").write_text(
                json.dumps(
                    {
                        "updated_at": _iso_now(),
                        "pending_by_event_id": {
                            "evt-old": {
                                "event_id": "evt-old",
                                "symbol": "BTCUSDT",
                                "side": "BUY",
                                "signal_time": _iso_minutes_ago(30),
                                "simulated_entry_price": 100000,
                                "spread_bps_at_entry": 0.5,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        status = snapshot["system_status"]
        self.assertEqual(snapshot["top_candidates"], [])
        self.assertEqual(status["candidate_source"], "bridge_state_shadow.pending_by_event_id")
        self.assertEqual(status["candidate_state"], "stale")
        self.assertEqual(status["candidate_empty_reason"], "candidate_source_stale")
        self.assertEqual(status["candidate_pool_count"], 1)
        self.assertEqual(status["candidate_ignored_stale_count"], 1)

    def test_stale_event_snapshots_keep_event_source_even_when_pending_pool_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            (shadow_root / "bridge-state.shadow.json").write_text(
                json.dumps(
                    {
                        "updated_at": _iso_now(),
                        "pending_by_event_id": {
                            "evt-old-pending": {
                                "event_id": "evt-old-pending",
                                "symbol": "BTCUSDT",
                                "side": "BUY",
                                "signal_time": _iso_minutes_ago(30),
                                "simulated_entry_price": 100000,
                                "spread_bps_at_entry": 0.5,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            event_path = base / "signal_lab_runs" / "event_collect_v6_speed_boost" / "event_snapshots.jsonl"
            event_path.parent.mkdir(parents=True)
            event_path.write_text(
                json.dumps(
                    {
                        "event": "market_event_created",
                        "event_id": "evt-old-event",
                        "symbol": "SOLUSDT",
                        "observed_at": _iso_minutes_ago(20),
                        "trigger_score": 20.0,
                        "sample": {"price": 140.0, "candle_direction": "up", "quote_volume_24h": 50_000_000},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        status = snapshot["system_status"]
        self.assertEqual(snapshot["top_candidates"], [])
        self.assertEqual(status["candidate_source"], "signal_lab.event_snapshots")
        self.assertEqual(status["candidate_state"], "stale")
        self.assertEqual(status["candidate_empty_reason"], "candidate_source_stale")
        self.assertEqual(status["candidate_pool_count"], 1)
        self.assertEqual(status["candidate_ignored_stale_count"], 1)

    def test_fresh_event_snapshots_are_used_when_pending_pool_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            (shadow_root / "bridge-state.shadow.json").write_text(
                json.dumps(
                    {
                        "updated_at": _iso_now(),
                        "pending_by_event_id": {
                            "evt-old": {
                                "event_id": "evt-old",
                                "symbol": "BTCUSDT",
                                "side": "BUY",
                                "signal_time": _iso_minutes_ago(30),
                                "simulated_entry_price": 100000,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            event_path = base / "signal_lab_runs" / "event_collect_v6_speed_boost" / "event_snapshots.jsonl"
            event_path.parent.mkdir(parents=True)
            event_path.write_text(
                json.dumps(
                    {
                        "event": "market_event_created",
                        "event_id": "evt-fresh",
                        "symbol": "SOLUSDT",
                        "observed_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                        "trigger_score": 12.5,
                        "trigger_types": ["volume_burst"],
                        "sample": {
                            "price": 142.42,
                            "candle_direction": "up",
                            "volume_burst_ratio": 2.4,
                            "ret_1bar_pct": 0.35,
                            "quote_volume_24h": 50_000_000,
                        },
                        "enrichments": {
                            "spread_bps": 0.7,
                            "estimated_slippage_bps": 1.1,
                            "funding_rate": 0.0001,
                            "oi_change_5m_pct": 0.22,
                        },
                        "factors": {"liquidity_bucket": "liquidity_extreme"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (event_path.parent / "candidate_producer_heartbeat.json").write_text(
                json.dumps(
                    {
                        "running": True,
                        "producer_alive": True,
                        "candidate_fresh": True,
                        "candidate_available": True,
                        "candidate_tradeable": True,
                        "created_at": _iso_now(),
                        "error_count": 0,
                    }
                ),
                encoding="utf-8",
            )
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        status = snapshot["system_status"]
        candidate = snapshot["top_candidates"][0]
        self.assertEqual(status["candidate_source"], "signal_lab.event_snapshots")
        self.assertEqual(status["top_candidates_count"], 1)
        self.assertEqual(status["candidate_state"], "known")
        self.assertIsNone(status["candidate_empty_reason"])
        self.assertEqual(candidate["symbol"], "SOLUSDT")
        self.assertEqual(candidate["bias"], "LONG")
        self.assertEqual(candidate["score"], 12.5)
        self.assertEqual(candidate["current_price"], 142.42)
        self.assertEqual(candidate["spread_bps"], 0.7)
        self.assertEqual(candidate["estimated_slippage_bps"], 1.1)
        self.assertTrue(candidate["liquidity_ok"])
        self.assertTrue(status["producer_alive"])
        self.assertTrue(status["candidate_producer_running"])
        self.assertEqual(status["candidate_producer_error_count"], 0)
        self.assertTrue(status["candidate_fresh"])
        self.assertTrue(status["candidate_available"])
        self.assertTrue(status["candidate_tradeable"])

    def test_fresh_event_snapshot_enrichments_derive_known_market_regime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            event_path = base / "signal_lab_runs" / "event_collect_v6_speed_boost" / "event_snapshots.jsonl"
            event_path.parent.mkdir(parents=True)
            event_path.write_text(
                json.dumps(
                    {
                        "event": "market_event_created",
                        "event_id": "evt-regime-1",
                        "symbol": "ETHUSDT",
                        "observed_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                        "trigger_score": 30.0,
                        "sample": {
                            "price": 4210.0,
                            "candle_direction": "up",
                            "quote_volume_24h": 1_000_000_000,
                            "atr_20_pct": 0.45,
                            "candle_range_pct": 0.52,
                        },
                        "enrichments": {
                            "btcusdt_ret_5m_pct": 0.32,
                            "btcusdt_ret_60m_pct": 0.84,
                            "ethusdt_ret_5m_pct": 0.41,
                            "ethusdt_ret_60m_pct": 0.92,
                            "spread_bps": 0.4,
                            "estimated_slippage_bps": 0.8,
                        },
                        "factors": {
                            "market_regime_score": 0.71,
                            "market_regime_bucket": "market_long_strong",
                            "liquidity_bucket": "liquidity_extreme",
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        market = snapshot["market_regime"]
        self.assertEqual(market["regime"], "TREND_UP")
        self.assertEqual(market["direction_lock"], "LONG_ONLY_OR_NO_TRADE")
        self.assertEqual(market["market_regime_source"], "signal_lab.event_snapshots.enrichments")
        self.assertIn("derived_from_fresh_signal_lab_event_snapshots", market["market_regime_reason"])
        self.assertGreater(market["market_regime_confidence"], 0.0)
        self.assertEqual(market["source"], market["market_regime_source"])
        self.assertEqual(market["reason"], market["market_regime_reason"])
        self.assertEqual(market["confidence"], market["market_regime_confidence"])
        self.assertEqual(market["market_regime_bucket"], "market_long_strong")
        self.assertEqual(market["market_regime_data_points"], 1)
        self.assertTrue(str(market["market_regime_source_path"]).endswith("event_snapshots.jsonl"))

    def test_missing_event_snapshot_regime_source_stays_unknown_without_fake_known_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        market = snapshot["market_regime"]
        self.assertEqual(market["regime"], "UNKNOWN")
        self.assertEqual(market["direction_lock"], "BOTH_ALLOWED")
        self.assertEqual(market["market_regime_source"], "signal_lab.event_snapshots.enrichments")
        self.assertEqual(market["market_regime_reason"], "market_regime_source_stale_or_empty")
        self.assertEqual(market["market_regime_confidence"], 0.0)
        self.assertEqual(market["source"], market["market_regime_source"])
        self.assertEqual(market["reason"], market["market_regime_reason"])
        self.assertEqual(market["confidence"], market["market_regime_confidence"])
        self.assertEqual(market["market_regime_data_points"], 0)

    def test_event_snapshot_fallback_uses_active_bridge_state_snapshots_file_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            active_run = base / "custom_active_collect"
            active_run.mkdir()
            bridge_feed = active_run / "bridge_event_feed.jsonl"
            bridge_feed.write_text("", encoding="utf-8")
            (shadow_root / "bridge-state.json").write_text(
                json.dumps({"snapshots_file": str(bridge_feed), "updated_at": _iso_now()}),
                encoding="utf-8",
            )
            (active_run / "event_snapshots.jsonl").write_text(
                json.dumps(
                    {
                        "event": "market_event_created",
                        "event_id": "evt-active",
                        "symbol": "ETHUSDT",
                        "observed_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                        "trigger_score": 18.0,
                        "sample": {"price": 2338.32, "candle_direction": "down", "quote_volume_24h": 1_000_000_000},
                        "enrichments": {"spread_bps": 0.05, "estimated_slippage_bps": 1.2},
                        "factors": {"liquidity_bucket": "liquidity_extreme"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        status = snapshot["system_status"]
        candidate = snapshot["top_candidates"][0]
        self.assertEqual(status["candidate_source"], "signal_lab.event_snapshots")
        self.assertEqual(candidate["symbol"], "ETHUSDT")
        self.assertEqual(candidate["bias"], "SHORT")
        self.assertEqual(candidate["current_price"], 2338.32)
        self.assertTrue(candidate["liquidity_ok"])

    def test_event_snapshot_shadow_branch_risk_plan_is_mapped_without_faking_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            event_path = base / "signal_lab_runs" / "event_collect_v6_speed_boost" / "event_snapshots.jsonl"
            event_path.parent.mkdir(parents=True)
            event_path.write_text(
                json.dumps(
                    {
                        "event": "market_event_created",
                        "event_id": "evt-branch",
                        "symbol": "SOLUSDT",
                        "observed_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                        "trigger_score": 21.0,
                        "sample": {"price": 142.42, "candle_direction": "up", "quote_volume_24h": 100_000_000},
                        "enrichments": {"spread_bps": 0.4, "estimated_slippage_bps": 0.9},
                        "factors": {"liquidity_bucket": "liquidity_extreme"},
                        "shadow_branches": [
                            {
                                "stop_loss_pct": 0.42,
                                "take_profit_pct": 0.88,
                                "max_hold_sec": 240,
                                "invalidation_condition": "breakout candle loses follow-through",
                            }
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        candidate = snapshot["top_candidates"][0]
        self.assertEqual(snapshot["system_status"]["candidate_source"], "signal_lab.event_snapshots")
        self.assertEqual(candidate["symbol"], "SOLUSDT")
        self.assertEqual(candidate["suggested_stop_pct"], 0.42)
        self.assertEqual(candidate["suggested_tp_pct"], 0.88)
        self.assertEqual(candidate["max_holding_time_sec"], 240)
        self.assertEqual(candidate["invalidation_hint"], "breakout candle loses follow-through")

    def test_event_snapshot_volatility_fields_derive_candidate_risk_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shadow_root = base / "signal_lab_runs" / "vps_forward_shadow_mainnet_active" / "mainnet_shadow"
            shadow_root.mkdir(parents=True)
            event_path = base / "signal_lab_runs" / "event_collect_v6_speed_boost" / "event_snapshots.jsonl"
            event_path.parent.mkdir(parents=True)
            event_path.write_text(
                json.dumps(
                    {
                        "event": "market_event_created",
                        "event_id": "evt-vol",
                        "symbol": "ETHUSDT",
                        "observed_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                        "trigger_score": 24.0,
                        "horizons_sec": [30, 60, 180, 300],
                        "sample": {
                            "price": 4200.0,
                            "candle_direction": "down",
                            "quote_volume_24h": 1_000_000_000,
                            "atr_20_pct": 0.5,
                            "candle_range_pct": 0.4,
                            "candle_body_pct": 0.2,
                        },
                        "enrichments": {"spread_bps": 1.0, "estimated_slippage_bps": 2.0},
                        "factors": {"liquidity_bucket": "liquidity_extreme"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            payload = dashboard_payload()
            payload["paths"]["projectRoot"] = str(base)
            payload["paths"]["shadowRoot"] = str(shadow_root)
            payload["shadowSnapshot"]["orders"] = []

            snapshot = build_trader_snapshot_from_dashboard_payload(
                payload,
                protective_stop_path_available=True,
                emergency_close_available=True,
                protective_stop_capability_source="verified_code_path:stop",
                emergency_close_capability_source="verified_code_path:emergency",
            )

        candidate = snapshot["top_candidates"][0]
        self.assertEqual(candidate["symbol"], "ETHUSDT")
        self.assertEqual(candidate["bias"], "SHORT")
        self.assertEqual(candidate["suggested_stop_pct"], 0.6)
        self.assertEqual(candidate["suggested_tp_pct"], 1.08)
        self.assertEqual(candidate["max_holding_time_sec"], 300)
        self.assertIn("source signal becomes stale", candidate["invalidation_hint"])

    def test_numeric_account_probe_timestamp_is_trusted_when_fresh(self) -> None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        snapshot = build_trader_snapshot_from_dashboard_payload(
            dashboard_payload(account_probe={"ok": True, "source": "python_probe", "executedAt": now_ms}),
            protective_stop_path_available=True,
            emergency_close_available=True,
            protective_stop_capability_source="verified_code_path:stop",
            emergency_close_capability_source="verified_code_path:emergency",
        )

        status = snapshot["system_status"]
        self.assertEqual(status["account_state_source"], "signed_account")
        self.assertEqual(status["position_state_source"], "signed_positions")
        self.assertEqual(status["exchange_status"], "healthy")
        self.assertTrue(status["trusted_runtime_snapshot"])


if __name__ == "__main__":
    unittest.main()
