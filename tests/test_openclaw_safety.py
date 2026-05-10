import argparse
import importlib.util
import subprocess
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def load_openclaw_module():
    root = Path(__file__).resolve().parents[1]
    target = root / "skills" / "phoenix-operator" / "phoenix_openclaw.py"
    spec = importlib.util.spec_from_file_location("phoenix_openclaw_test_module", target)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


class OpenClawSafetyTests(unittest.TestCase):
    def test_auto_confirm_dispatch_is_hard_rejected_without_handle_confirm(self) -> None:
        module = load_openclaw_module()
        args = argparse.Namespace(
            env="testnet",
            symbol=None,
            candidate_file="phoenix_candidates.json",
            candidate_rank=1,
            quote_allocation=10.0,
            leverage=1,
            side="BUY",
            entry_price=None,
            dispatch_reason="unit_test",
            ttl_sec=60,
        )

        with patch.object(module, "load_execution_settings", return_value=SimpleNamespace(execution_mode="AUTO_CONFIRM_WHEN_RULES_PASS")), patch.object(
            module, "handle_confirm", side_effect=AssertionError("auto confirm must not call handle_confirm")
        ):
            code = module.handle_dispatch(args)

        self.assertEqual(code, 1)

    def test_confirm_passes_record_env_to_live_execute(self) -> None:
        module = load_openclaw_module()
        captured: dict[str, list[str]] = {}
        record = {
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            "env": "testnet",
        }

        def fake_run_capture(cmd: list[str]) -> subprocess.CompletedProcess[str]:
            captured["cmd"] = cmd
            return subprocess.CompletedProcess(cmd, 0, "{}", "")

        with patch.object(module, "cleanup_expired_confirmations"), patch.object(module, "load_confirmation_record", return_value=record), patch.object(
            module, "confirmation_record_path", return_value=Path("dummy-confirmation.json")
        ), patch.object(module, "run_capture", side_effect=fake_run_capture), patch.object(module, "consume_confirmation_record"):
            code = module.handle_confirm(argparse.Namespace(token="unit-token"))

        self.assertEqual(code, 0)
        self.assertIn("--env", captured["cmd"])
        self.assertEqual(captured["cmd"][captured["cmd"].index("--env") + 1], "testnet")

    def test_confirm_rejects_non_testnet_record_env_before_run_capture(self) -> None:
        module = load_openclaw_module()
        record = {
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            "env": "prod",
        }

        with patch.object(module, "cleanup_expired_confirmations"), patch.object(module, "load_confirmation_record", return_value=record), patch.object(
            module, "run_capture", side_effect=AssertionError("prod confirm must stop before signed API path")
        ), patch.object(module, "consume_confirmation_record"):
            code = module.handle_confirm(argparse.Namespace(token="unit-token"))

        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
