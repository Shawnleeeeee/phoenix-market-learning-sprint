import unittest

from phoenix_config import load_phoenix_config, redact_secrets
from phoenix_runtime_modes import (
    MAINNET_LIVE,
    MAINNET_SHADOW,
    TESTNET_LIVE,
    RuntimeModeSafetyError,
    assert_runtime_mode_safe,
    normalize_runtime_mode,
)


class PhoenixRuntimeModeTests(unittest.TestCase):
    def test_runtime_modes_normalize_supported_aliases(self) -> None:
        self.assertEqual(normalize_runtime_mode(None), TESTNET_LIVE)
        self.assertEqual(normalize_runtime_mode("testnet"), TESTNET_LIVE)
        self.assertEqual(normalize_runtime_mode("shadow"), MAINNET_SHADOW)
        self.assertEqual(normalize_runtime_mode("mainnet"), MAINNET_LIVE)

    def test_mainnet_live_requires_explicit_enable_flag(self) -> None:
        with self.assertRaises(RuntimeModeSafetyError):
            assert_runtime_mode_safe(MAINNET_LIVE, binance_env="prod")

        self.assertEqual(
            assert_runtime_mode_safe(MAINNET_LIVE, binance_env="prod", enable_mainnet_live=True),
            MAINNET_LIVE,
        )

    def test_mainnet_live_requires_mainnet_environment(self) -> None:
        with self.assertRaises(RuntimeModeSafetyError):
            assert_runtime_mode_safe(MAINNET_LIVE, binance_env="testnet", enable_mainnet_live=True)


class PhoenixConfigTests(unittest.TestCase):
    def test_default_config_is_testnet_live_and_safe(self) -> None:
        config = load_phoenix_config({})

        self.assertEqual(config.runtime_mode, TESTNET_LIVE)
        self.assertEqual(config.binance_env, "testnet")
        self.assertEqual(config.max_open_positions, 10)
        self.assertEqual(config.margin_type, "ISOLATED")
        self.assertFalse(config.research_agent_enabled)
        self.assertTrue(config.hmm_report_enabled)
        self.assertFalse(config.hmm_trading_gate_enabled)
        self.assertFalse(config.hmm_position_manager_enabled)
        self.assertFalse(config.mainnet_live_orders_enabled)

    def test_mainnet_live_is_not_enabled_by_default_even_with_prod_env(self) -> None:
        with self.assertRaises(RuntimeModeSafetyError):
            load_phoenix_config(
                {
                    "PHOENIX_RUNTIME_MODE": MAINNET_LIVE,
                    "PHOENIX_BINANCE_ENV": "prod",
                }
            )

    def test_explicit_mainnet_live_requires_both_mode_and_enable_flag(self) -> None:
        config = load_phoenix_config(
            {
                "PHOENIX_RUNTIME_MODE": MAINNET_LIVE,
                "PHOENIX_BINANCE_ENV": "prod",
                "PHOENIX_ENABLE_MAINNET_LIVE": "true",
            }
        )

        self.assertTrue(config.mainnet_live_orders_enabled)

    def test_secrets_are_redacted_from_config_snapshot(self) -> None:
        config = load_phoenix_config({"OPENAI_API_KEY": "sk-test-secret-value"})

        redacted = config.redacted_dict()

        self.assertEqual(redacted["openai_api_key"], "<redacted>")
        self.assertNotIn("sk-test-secret-value", str(redacted))

    def test_redact_secrets_handles_generic_secret_fields(self) -> None:
        redacted = redact_secrets(
            {
                "binance_api_secret": "super-secret",
                "openai_base_url": "https://api.example.test/v1",
            }
        )

        self.assertEqual(redacted["binance_api_secret"], "<redacted>")
        self.assertEqual(redacted["openai_base_url"], "https://api.example.test/v1")


if __name__ == "__main__":
    unittest.main()
