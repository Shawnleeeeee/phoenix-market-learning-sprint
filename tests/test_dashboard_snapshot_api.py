import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from phoenix_dashboard_snapshot_api import (
    DEFAULT_ENDPOINT,
    DashboardSnapshotService,
    parse_env_key_presence,
    parse_args,
    redact_text,
    make_server,
    sanitize,
)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


class DashboardSnapshotApiTests(unittest.TestCase):
    def build_project(self, root: Path) -> Path:
        shadow_root = root / "signal_lab_runs" / "vps_forward_shadow_baseline_clean_test" / "mainnet_shadow"
        run_root = shadow_root.parent
        write_json(
            run_root / "bridge-state.shadow.json",
            {
                "updated_at": "2026-05-01T00:00:00+00:00",
                "pending_by_event_id": {
                    "evt-1": {
                        "event_id": "evt-1",
                        "strategy_family": "ONE_MIN_MOMENTUM_SCALP_PLUS",
                        "strategy_id": "OMSP_FAST_FIXED_TP08_SL04_H3",
                        "entry_profile": "SCALP_FAST_ENTRY",
                        "exit_profile": "EXIT_FIXED_QUICK_TAKE",
                        "status": "shadow_only",
                        "symbol": "TESTUSDT",
                        "side": "BUY",
                        "signal_time": "2026-05-01T00:00:00+00:00",
                        "simulated_entry_price": 1.23,
                        "trigger_features": {
                            "oi_confirmation_source": "five_min_fallback",
                            "oi5_direction_bucket": "oi5_aligned",
                        },
                        "secret": "must-not-leak",
                        "apiSecret": "camel-api-secret",
                        "accessToken": "camel-access-token",
                        "privateKey": "camel-private-key",
                        "clientSecret": "camel-client-secret",
                        "passwordHash": "camel-password-hash",
                    }
                },
            },
        )
        write_json(
            run_root / "bridge-state.json",
            {"updated_at": "2026-05-01T00:00:00+00:00", "offset": 10},
        )
        write_json(
            shadow_root / "mainnet_shadow_readiness.json",
            {
                "generated_at": "2026-05-01T00:00:01+00:00",
                "execution_mode": "MAINNET_SHADOW",
                "live_trading_enabled": False,
                "closed_shadow_trades": 12,
                "win_rate_pct": 55.0,
                "profit_factor": 1.3,
                "max_drawdown_pct": 2.1,
                "estimated_total_pnl_usdt": 8.5,
            },
        )
        write_json(
            shadow_root / "Total_Sample_V9_Final.json",
            {
                "generated_at": "2026-05-01T00:00:01+00:00",
                "real_account": {
                    "available_balance_usdt": 10,
                    "open_position_count": 0,
                    "open_positions": [],
                    "account_overview_error": None,
                },
                "shadow_all": {
                    "signal_count": 1,
                    "branch_signal_count": 1,
                    "pending_signal_count": 1,
                    "pending_branch_count": 1,
                    "outcome_count": 1,
                },
            },
        )
        write_json(
            shadow_root / "strategy_shadow_league_report.json",
            {
                "live_trading_enabled": False,
                "promotion_allowed": False,
                "paper_record_only": True,
                "input_counts": {"shadow_signals": 1, "shadow_outcomes": 1},
                "strategies": [],
            },
        )
        append_jsonl(
            shadow_root / "signal_bridge_shadow_signals.jsonl",
            [
                {
                    "event_id": "evt-1",
                    "live_trading_enabled": False,
                    "live_order_submission_blocked": True,
                    "api_key": "must-not-leak",
                    "apiKey": "camel-api-key",
                }
            ],
        )
        append_jsonl(
            shadow_root / "signal_bridge_shadow_outcomes.jsonl",
            [
                {
                    "event_id": "evt-1",
                    "symbol": "TESTUSDT",
                    "after_fee_and_slippage_return_pct": 0.4,
                    "exit_reason": "horizon_close",
                    "token": "must-not-leak",
                    "authToken": "camel-auth-token",
                }
            ],
        )
        collector_root = root / "signal_lab_runs" / "event_collect_v6_speed_boost"
        append_jsonl(collector_root / "bridge_event_feed.jsonl", [{"event_id": "evt-1"}])
        append_jsonl(collector_root / "event_snapshots.jsonl", [{"event_id": "evt-1"}])
        append_jsonl(collector_root / "event_horizon_labels.jsonl", [{"event_id": "evt-1"}])
        (collector_root / "collector_stdout.log").write_text(
            "Authorization: Bearer secret-token\nnormal line\n",
            encoding="utf-8",
        )
        (root / ".env").write_text(
            "BINANCE_API_KEY=abc\nBINANCE_API_SECRET=def\nPLAIN_FLAG=true\n",
            encoding="utf-8",
        )
        return shadow_root

    def test_snapshot_contains_required_fields_and_redacts_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shadow_root = self.build_project(root)
            service = DashboardSnapshotService(root, shadow_root, cache_ttl_sec=0)
            snapshot = service.snapshot()

        self.assertIn("generatedAt", snapshot)
        self.assertEqual(snapshot["paths"]["projectRoot"], str(root.resolve()))
        self.assertEqual(snapshot["paths"]["shadowRoot"], str(shadow_root.resolve()))
        self.assertIn("accountProbe", snapshot)
        self.assertIn("accountSummary", snapshot)
        self.assertIn("openPositions", snapshot)
        self.assertIn("shadowSnapshot", snapshot)
        self.assertIn("mainnet", snapshot["environmentSnapshots"])
        self.assertIn("testnet", snapshot["environmentSnapshots"])
        self.assertIn("recentLogs", snapshot)
        self.assertIn("recentTradeJournals", snapshot)
        self.assertIn("roundRunnerReports", snapshot)
        self.assertIn("sampleCounts", snapshot)
        self.assertIn("runtime", snapshot)

        self.assertTrue(
            {
                "source",
                "root",
                "statePath",
                "updatedAt",
                "executionMode",
                "liveTradingEnabled",
                "liveOrderSubmissionBlocked",
                "openOrderCount",
                "conditionalOrderCount",
                "branchCount",
                "closedShadowTrades",
                "winRatePct",
                "profitFactor",
                "maxDrawdownPct",
                "estimatedTotalPnlUsdt",
                "orders",
                "recentOutcomes",
            }.issubset(snapshot["shadowSnapshot"].keys())
        )
        self.assertTrue({"source", "updatedAt", "ok", "error", "mode"}.issubset(snapshot["accountProbe"].keys()))
        self.assertTrue({"endpoint", "host", "port", "cacheTtlSec"}.issubset(snapshot["runtime"].keys()))

        shadow = snapshot["shadowSnapshot"]
        self.assertEqual(shadow["executionMode"], "MAINNET_SHADOW")
        self.assertFalse(shadow["liveTradingEnabled"])
        self.assertTrue(shadow["liveOrderSubmissionBlocked"])
        self.assertEqual(shadow["openOrderCount"], 1)
        self.assertEqual(shadow["closedShadowTrades"], 12)
        self.assertEqual(shadow["orders"][0]["oiConfirmationSource"], "five_min_fallback")

        body = json.dumps(snapshot)
        self.assertNotIn("must-not-leak", body)
        self.assertNotIn("secret-token", body)
        self.assertNotIn("abc", body)
        self.assertNotIn("def", body)
        self.assertNotIn("camel-api-key", body)
        self.assertNotIn("camel-api-secret", body)
        self.assertNotIn("camel-access-token", body)
        self.assertNotIn("camel-private-key", body)
        self.assertNotIn("camel-client-secret", body)
        self.assertNotIn("camel-password-hash", body)

    def test_testnet_environment_snapshot_uses_readonly_account_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shadow_root = self.build_project(root)
            testnet_env = root / "testnet.env"
            testnet_env.write_text(
                "\n".join(
                    [
                        "export PHOENIX_BINANCE_ENV='testnet'",
                        "export PHOENIX_BINANCE_API_KEY='test-key'",
                        "export PHOENIX_BINANCE_API_SECRET='test-secret'",
                    ]
                ),
                encoding="utf-8",
            )

            calls: list[str] = []

            def fake_signed_get(**kwargs):
                calls.append(kwargs["path"])
                if kwargs["path"] == "/fapi/v3/balance":
                    return [
                        {
                            "asset": "USDT",
                            "balance": "4440.3468",
                            "availableBalance": "4430.12",
                            "crossWalletBalance": "4440.3468",
                            "crossUnPnl": "-1.23",
                            "maxWithdrawAmount": "4430.12",
                        }
                    ]
                if kwargs["path"] == "/fapi/v3/positionRisk":
                    return [
                        {
                            "symbol": "DOGEUSDT",
                            "positionAmt": "100",
                            "entryPrice": "0.2",
                            "markPrice": "0.21",
                            "notional": "21",
                            "unRealizedProfit": "1",
                            "leverage": "2",
                            "liquidationPrice": "0.1",
                            "marginType": "isolated",
                            "positionSide": "BOTH",
                        },
                        {"symbol": "BTCUSDT", "positionAmt": "0"},
                    ]
                if kwargs["path"] == "/fapi/v1/openOrders":
                    return [{"orderId": 123}]
                raise AssertionError(kwargs["path"])

            service = DashboardSnapshotService(
                root,
                shadow_root,
                cache_ttl_sec=0,
                testnet_env_file=testnet_env,
                signed_get=fake_signed_get,
            )
            snapshot = service.snapshot()

        testnet = snapshot["environmentSnapshots"]["testnet"]
        self.assertEqual(
            calls,
            ["/fapi/v3/balance", "/fapi/v3/positionRisk", "/fapi/v1/openOrders"],
        )
        self.assertEqual(testnet["mode"], "testnet")
        self.assertEqual(testnet["environment"], "testnet")
        self.assertTrue(testnet["accountProbe"]["ok"])
        self.assertEqual(testnet["accountProbe"]["source"], "python_probe")
        self.assertEqual(testnet["accountSummary"]["accountType"], "binance_futures_testnet_readonly")
        self.assertEqual(testnet["accountSummary"]["totalWalletBalance"], 4440.3468)
        self.assertEqual(testnet["accountSummary"]["openOrdersCount"], 1)
        self.assertEqual(testnet["openPositions"][0]["symbol"], "DOGEUSDT")
        self.assertEqual(testnet["openPositions"][0]["side"], "LONG")
        body = json.dumps(snapshot)
        self.assertNotIn("test-key", body)
        self.assertNotIn("test-secret", body)

    def test_parse_env_key_presence_handles_export_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_file = Path(tmp) / "sample.env"
            env_file.write_text("export PHOENIX_BINANCE_ENV='testnet'\nPLAIN_FLAG=true\n", encoding="utf-8")
            self.assertEqual(parse_env_key_presence(env_file), {"PHOENIX_BINANCE_ENV", "PLAIN_FLAG"})

    def test_sanitize_redacts_sensitive_keys(self) -> None:
        cleaned = sanitize(
            {
                "nested": {
                    "api_key": "abc",
                    "apiKey": "camel",
                    "accessToken": "token",
                    "authorization": "Bearer abc",
                    "bearerToken": "bearer",
                    "privateKey": "private",
                    "secretKey": "secret-key",
                    "clientSecret": "client",
                    "passwordHash": "hash",
                    "passphrase": "phrase",
                    "passwd": "passwd",
                    "items": [{"token": "nested"}],
                    "tupleItems": ({"apiSecret": "tuple-secret"},),
                    "safe": "value",
                }
            }
        )
        self.assertEqual(cleaned["nested"]["api_key"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["apiKey"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["accessToken"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["authorization"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["bearerToken"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["privateKey"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["secretKey"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["clientSecret"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["passwordHash"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["passphrase"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["passwd"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["items"][0]["token"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["tupleItems"][0]["apiSecret"], "[REDACTED]")
        self.assertEqual(cleaned["nested"]["safe"], "value")

    def test_redact_text_handles_space_separated_secret_args(self) -> None:
        command = (
            "python app.py --api-key abc --token xyz --password hunter2 "
            "--client-secret=clientsecretvalue --passphrase passphrasevalue --safe value"
        )
        cleaned = redact_text(command)
        self.assertNotIn("abc", cleaned)
        self.assertNotIn("xyz", cleaned)
        self.assertNotIn("hunter2", cleaned)
        self.assertNotIn("clientsecretvalue", cleaned)
        self.assertNotIn("passphrasevalue", cleaned)
        self.assertIn("--safe value", cleaned)

    def test_redact_text_truncates_long_values(self) -> None:
        cleaned = redact_text("x" * 4100, max_len=100)
        self.assertLessEqual(len(cleaned), 115)
        self.assertTrue(cleaned.endswith("...[truncated]"))

    def test_default_host_is_loopback(self) -> None:
        self.assertEqual(parse_args([]).host, "127.0.0.1")

    def test_http_requires_bearer_token_and_rejects_post(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shadow_root = self.build_project(root)
            service = DashboardSnapshotService(root, shadow_root, cache_ttl_sec=0)
            server = make_server(
                service,
                "x" * 40,
                host="127.0.0.1",
                port=0,
                allowed_origins={"http://localhost:5173"},
            )
            port = server.server_address[1]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                url = f"http://127.0.0.1:{port}{DEFAULT_ENDPOINT}"
                with self.assertRaises(urllib.error.HTTPError) as unauth:
                    urllib.request.urlopen(url, timeout=5)
                self.assertEqual(unauth.exception.code, 401)

                wrong_req = urllib.request.Request(
                    url,
                    headers={"Authorization": "Bearer " + "y" * 40},
                )
                with self.assertRaises(urllib.error.HTTPError) as wrong_auth:
                    urllib.request.urlopen(wrong_req, timeout=5)
                self.assertEqual(wrong_auth.exception.code, 401)
                self.assertEqual(wrong_auth.exception.headers.get("WWW-Authenticate"), "Bearer")

                for bad_auth in ["Basic " + "x" * 40, "bearer " + "x" * 40, "Bearer "]:
                    bad_req = urllib.request.Request(url, headers={"Authorization": bad_auth})
                    with self.assertRaises(urllib.error.HTTPError) as bad_error:
                        urllib.request.urlopen(bad_req, timeout=5)
                    self.assertEqual(bad_error.exception.code, 401)

                req = urllib.request.Request(url, headers={"Authorization": "Bearer " + "x" * 40})
                with urllib.request.urlopen(req, timeout=5) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.status, 200)
                self.assertIn("shadowSnapshot", payload)

                missing_path_req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/missing",
                    headers={"Authorization": "Bearer " + "x" * 40},
                )
                with self.assertRaises(urllib.error.HTTPError) as missing_path:
                    urllib.request.urlopen(missing_path_req, timeout=5)
                self.assertEqual(missing_path.exception.code, 404)

                options_req = urllib.request.Request(url, method="OPTIONS")
                with urllib.request.urlopen(options_req, timeout=5) as options_response:
                    self.assertEqual(options_response.status, 204)
                    self.assertEqual(options_response.read(), b"")

                for method in ["POST", "PUT", "PATCH", "DELETE", "HEAD"]:
                    req_kwargs = {
                        "headers": {"Authorization": "Bearer " + "x" * 40},
                        "method": method,
                    }
                    if method not in {"HEAD"}:
                        req_kwargs["data"] = b"{}"
                    method_req = urllib.request.Request(url, **req_kwargs)
                    with self.assertRaises(urllib.error.HTTPError) as method_error:
                        urllib.request.urlopen(method_req, timeout=5)
                    self.assertEqual(method_error.exception.code, 405)
                    self.assertEqual(method_error.exception.headers.get("Allow"), "GET, OPTIONS")
            finally:
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    unittest.main()
