import unittest
import zipfile
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from phoenix_playbook_backtest import (
    BinancePublicZipBackfillClient,
    build_promotion_backtest_report,
    build_public_kline_zip_url,
    build_historical_ticker_item,
    choose_playbook_classifier,
    compute_profit_factor_from_pnl,
    classify_playbook_record,
    classify_playbook_record_with_proxies,
    compute_daily_sharpe_ratio,
    fetch_symbol_history,
    iter_public_kline_rows_from_zip_bytes,
    parse_symbol_list,
    resolve_backtest_window,
    set_process_nice_value,
    simulate_trade_path,
    wait_for_safe_system_load,
)


def make_5m_candle(
    open_time_ms: int,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    quote_volume: float,
) -> list[object]:
    return [
        open_time_ms,
        str(open_price),
        str(high_price),
        str(low_price),
        str(close_price),
        "1",
        open_time_ms + 299_999,
        str(quote_volume),
        1,
        "0",
        "0",
        "0",
    ]


class PlaybookBacktestHelpersTests(unittest.TestCase):
    def test_parse_symbol_list_dedupes_and_normalizes(self) -> None:
        self.assertEqual(parse_symbol_list("btcusdt, ETHUSDT\nbtcusdt"), ["BTCUSDT", "ETHUSDT"])

    def test_resolve_backtest_window_supports_inclusive_end_date(self) -> None:
        start_dt, end_dt = resolve_backtest_window(
            days=14,
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 3),
        )

        self.assertEqual(start_dt, datetime(2026, 4, 1, tzinfo=timezone.utc))
        self.assertEqual(end_dt, datetime(2026, 4, 4, tzinfo=timezone.utc))

    def test_public_zip_url_uses_official_futures_daily_layout(self) -> None:
        self.assertEqual(
            build_public_kline_zip_url(symbol="btcusdt", interval="5m", day=date(2026, 4, 27)),
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/5m/BTCUSDT-5m-2026-04-27.zip",
        )

    def test_public_zip_parser_skips_header(self) -> None:
        payload = BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            archive.writestr(
                "BTCUSDT-5m-2026-04-27.csv",
                "\n".join(
                    [
                        "open_time,open,high,low,close,volume,close_time,quote_asset_volume,number_of_trades,taker_buy_base_volume,taker_buy_quote_volume,ignore",
                        "1777248000000,100,101,99,100.5,1,1777248299999,100.5,10,0.4,40,0",
                    ]
                ),
            )

        rows = iter_public_kline_rows_from_zip_bytes(payload.getvalue(), symbol="BTCUSDT")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["open_time"], "1777248000000")
        self.assertEqual(rows[0]["quote_asset_volume"], "100.5")

    def test_public_zip_client_reads_cached_daily_zip_without_network(self) -> None:
        with TemporaryDirectory() as temp_dir:
            cache_dir = temp_dir
            zip_path = (
                Path(cache_dir)
                / "futures"
                / "um"
                / "daily"
                / "klines"
                / "BTCUSDT"
                / "5m"
                / "BTCUSDT-5m-2026-04-27.zip"
            )
            zip_path.parent.mkdir(parents=True)
            payload = BytesIO()
            with zipfile.ZipFile(payload, "w") as archive:
                archive.writestr(
                    "BTCUSDT-5m-2026-04-27.csv",
                    "\n".join(
                        [
                            "1777247700000,99,100,98,99.5,1,1777247999999,99.5,10,0.4,40,0",
                            "1777248000000,100,101,99,100.5,1,1777248299999,100.5,10,0.4,40,0",
                            "1777248300000,100.5,102,100,101.5,1,1777248599999,101.5,10,0.4,40,0",
                        ]
                    ),
                )
            zip_path.write_bytes(payload.getvalue())
            client = BinancePublicZipBackfillClient(
                proxy=None,
                timeout_sec=1,
                pause_sec=0,
                max_retries=1,
                cache_dir=Path(cache_dir),
                offline_cache_only=True,
            )

            rows = client.fetch_klines(
                symbol="BTCUSDT",
                interval="5m",
                start_ms=1777248000000,
                end_ms=1777248600000,
            )

            self.assertEqual([row["open_time"] for row in rows], ["1777248000000", "1777248300000"])

    def test_choose_playbook_classifier_supports_proxy_mode(self) -> None:
        self.assertIs(choose_playbook_classifier("proxy"), classify_playbook_record_with_proxies)
        self.assertIs(choose_playbook_classifier("strict"), classify_playbook_record)

    def test_profit_factor_and_promotion_backtest_report_are_gate_compatible(self) -> None:
        self.assertAlmostEqual(compute_profit_factor_from_pnl([10.0, -5.0, 5.0]), 3.0)

        report = build_promotion_backtest_report(
            {
                "generated_at": "2026-04-29T00:00:00+00:00",
                "config": {
                    "data_source": "public-zip",
                    "interval": "5m",
                    "selected_symbols": ["BTCUSDT"],
                    "allowed_playbooks": ["oi_build_breakout"],
                },
                "raw_signal_summary": {"trade_count": 1200},
                "portfolio_summary": {
                    "accepted_trade_count": 1200,
                    "avg_after_fee_return_pct": 0.04,
                    "profit_factor": 1.31,
                    "win_rate_pct": 53.0,
                    "sharpe_ratio": 1.2,
                    "max_drawdown_pct": 3.4,
                    "total_pnl_usdt": 120.0,
                    "total_return_pct": 2.4,
                },
            }
        )

        self.assertEqual(report["report_type"], "phoenix_promotion_backtest")
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["profit_factor"], 1.31)
        self.assertEqual(report["avg_return_pct"], 0.04)

    def test_build_historical_ticker_item_uses_rolling_24h_volume(self) -> None:
        candles = [
            make_5m_candle(index * 300_000, 100.0, 101.0, 99.0, 100.0 + index, 10.0)
            for index in range(289)
        ]

        item = build_historical_ticker_item("BTCUSDT", candles, current_index=288, rolling_bars=288)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["symbol"], "BTCUSDT")
        self.assertAlmostEqual(float(item["quoteVolume"]), 2880.0, places=6)
        self.assertAlmostEqual(float(item["priceChangePercent"]), 288.0, places=6)

    def test_classify_playbook_record_detects_oi_build_breakout(self) -> None:
        record = {
            "symbol": "BTCUSDT",
            "sample_type": "trigger",
            "bar_interval": "5m",
            "trigger_types": ["volume_burst", "range_expansion"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "5m",
                "candle_direction": "up",
                "trigger_types": ["volume_burst", "range_expansion"],
            },
            "enrichments": {
                "oi_change_5m_pct": 0.8,
                "oi_change_15m_pct": 1.2,
            },
        }

        self.assertEqual(classify_playbook_record(record), "oi_build_breakout")

    def test_classify_playbook_record_requires_reversal_confirmation(self) -> None:
        record = {
            "symbol": "REVUSDT",
            "sample_type": "trigger",
            "bar_interval": "1m",
            "trigger_types": ["volume_burst", "range_expansion", "body_expansion"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "1m",
                "candle_direction": "down",
                "trigger_candle_direction": "down",
                "confirmation_candle_direction": "up",
                "reversal_confirmation_passed": True,
                "trigger_types": ["volume_burst", "range_expansion", "body_expansion"],
            },
            "enrichments": {
                "btcusdt_ret_5m_pct": 0.08,
                "oi_change_5m_pct": -0.04,
                "oi_change_15m_pct": -0.08,
                "depth_imbalance": 0.41,
            },
        }

        self.assertEqual(classify_playbook_record(record), "other_trigger")
        unconfirmed = {
            **record,
            "sample": {
                **record["sample"],
                "confirmation_candle_direction": "down",
                "reversal_confirmation_passed": False,
            },
        }
        self.assertEqual(classify_playbook_record(unconfirmed), "other_trigger")
        self.assertEqual(classify_playbook_record_with_proxies(unconfirmed), "other_trigger")

        adverse_btc = {
            **record,
            "enrichments": {
                **record["enrichments"],
                "btcusdt_ret_5m_pct": -0.44,
            },
        }
        self.assertEqual(classify_playbook_record(adverse_btc), "other_trigger")

        wrong_interval = {
            **record,
            "bar_interval": "5m",
            "sample": {
                **record["sample"],
                "bar_interval": "5m",
            },
        }
        self.assertEqual(classify_playbook_record(wrong_interval), "other_trigger")
        self.assertEqual(classify_playbook_record_with_proxies(wrong_interval), "other_trigger")

    def test_classify_playbook_record_with_proxies_detects_liquidation_flush(self) -> None:
        record = {
            "symbol": "BTCUSDT",
            "sample_type": "trigger",
            "bar_interval": "5m",
            "trigger_types": ["volume_burst", "range_expansion", "body_expansion"],
            "sample": {
                "sample_type": "trigger",
                "bar_interval": "5m",
                "candle_direction": "down",
                "volume_burst_ratio": 3.9,
                "range_to_atr": 2.4,
                "lower_wick_ratio": 0.58,
                "upper_wick_ratio": 0.08,
            },
            "enrichments": {
                "oi_change_5m_pct": -0.9,
                "oi_change_15m_pct": -1.4,
            },
        }

        self.assertEqual(classify_playbook_record_with_proxies(record), "liquidation_flush")

    def test_compute_daily_sharpe_ratio_uses_realized_trade_pnl(self) -> None:
        trades = [
            {"exit_time_ms": 1_700_000_000_000, "pnl_usdt": 100.0},
            {"exit_time_ms": 1_700_000_000_000 + 86_400_000, "pnl_usdt": -40.0},
            {"exit_time_ms": 1_700_000_000_000 + (2 * 86_400_000), "pnl_usdt": 120.0},
        ]

        sharpe_ratio = compute_daily_sharpe_ratio(trades, starting_equity=10_000.0)

        self.assertGreater(sharpe_ratio, 0.0)

    def test_simulate_trade_path_hits_take_profit_for_long(self) -> None:
        future_candles = [
            make_5m_candle(300_000, 100.0, 100.4, 99.8, 100.2, 10.0),
            make_5m_candle(600_000, 100.2, 103.0, 100.1, 102.8, 10.0),
        ]

        result = simulate_trade_path(
            side="BUY",
            entry_price=100.0,
            future_candles=future_candles,
            stop_loss_pct=1.0,
            take_profit_pct=2.5,
            breakeven_trigger_pct=0.8,
            breakeven_lock_pct=0.1,
            trailing_callback_pct=0.5,
            round_trip_fee_bps=8.0,
            notional_usdt=5_000.0,
        )

        self.assertEqual(result["exit_reason"], "take_profit")
        self.assertAlmostEqual(float(result["exit_price"]), 102.5, places=6)
        self.assertGreater(float(result["after_fee_return_pct"]), 2.0)

    def test_simulate_trade_path_moves_to_breakeven_then_trails_out(self) -> None:
        future_candles = [
            make_5m_candle(300_000, 100.0, 101.0, 100.0, 100.9, 10.0),
            make_5m_candle(600_000, 100.9, 101.1, 100.2, 100.3, 10.0),
        ]

        result = simulate_trade_path(
            side="BUY",
            entry_price=100.0,
            future_candles=future_candles,
            stop_loss_pct=1.0,
            take_profit_pct=2.5,
            breakeven_trigger_pct=0.8,
            breakeven_lock_pct=0.1,
            trailing_callback_pct=0.5,
            round_trip_fee_bps=8.0,
            notional_usdt=5_000.0,
        )

        self.assertEqual(result["exit_reason"], "trailing_stop")
        self.assertGreater(float(result["exit_price"]), 100.1)
        self.assertGreater(float(result["after_fee_return_pct"]), 0.0)

    def test_set_process_nice_value_uses_19_when_supported(self) -> None:
        recorded: list[int] = []

        def failing_set_priority(*_: object) -> None:
            raise PermissionError("fall back to nice")

        changed = set_process_nice_value(
            target_value=19,
            set_priority_func=failing_set_priority,
            nice_func=recorded.append,
        )

        self.assertTrue(changed)
        self.assertEqual(recorded, [19])

    def test_wait_for_safe_system_load_sleeps_until_load_drops(self) -> None:
        load_values = iter([2.2, 1.8, 1.4])
        sleep_calls: list[float] = []

        with patch("phoenix_playbook_backtest.emit_event"):
            pause_count = wait_for_safe_system_load(
                max_load_average=1.5,
                sleep_seconds=0.25,
                get_load_average=lambda: next(load_values),
                sleep_func=sleep_calls.append,
            )

        self.assertEqual(pause_count, 2)
        self.assertEqual(sleep_calls, [0.25, 0.25])

    def test_fetch_symbol_history_uses_six_hour_chunks(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.kline_calls: list[tuple[int, int]] = []
                self.oi_calls: list[tuple[int, int]] = []

            def fetch_klines(self, *, symbol: str, interval: str, start_ms: int, end_ms: int) -> list[dict[str, object]]:
                self.kline_calls.append((start_ms, end_ms))
                return [
                    {
                        "open_time": start_ms,
                        "open": "100",
                        "high": "101",
                        "low": "99",
                        "close": "100.5",
                        "volume": "1",
                        "close_time": end_ms - 1,
                        "quote_asset_volume": "10",
                        "number_of_trades": 1,
                        "taker_buy_base_volume": "0",
                        "taker_buy_quote_volume": "0",
                        "ignore": "0",
                    }
                ]

            def fetch_open_interest_hist(self, *, symbol: str, period: str, start_ms: int, end_ms: int) -> list[dict[str, object]]:
                self.oi_calls.append((start_ms, end_ms))
                return [{"timestamp": start_ms, "sumOpenInterest": "10"}]

        throttle_markers: list[str] = []
        client = FakeClient()
        six_hours_ms = 6 * 60 * 60 * 1000
        candles, oi_rows = fetch_symbol_history(
            client,
            symbol="BTCUSDT",
            start_ms=0,
            end_ms=(13 * 60 * 60 * 1000),
            chunk_hours=6,
            throttle_callback=lambda: throttle_markers.append("tick"),
        )

        self.assertEqual(
            client.kline_calls,
            [
                (0, six_hours_ms),
                (six_hours_ms, 2 * six_hours_ms),
                (2 * six_hours_ms, 13 * 60 * 60 * 1000),
            ],
        )
        self.assertEqual(client.oi_calls, client.kline_calls)
        self.assertEqual(len(candles), 3)
        self.assertEqual(len(oi_rows), 3)
        self.assertEqual(throttle_markers, ["tick", "tick", "tick"])


if __name__ == "__main__":
    unittest.main()
