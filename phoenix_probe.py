#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json

import aiohttp

from phoenix.binance_futures import BinanceAPIError, BinanceFuturesClient
from phoenix.config import load_credentials, load_proxy_settings, resolve_environment


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe Binance Futures auth and permissions for Project Phoenix.")
    parser.add_argument("--env", default=None, choices=["prod", "testnet", "demo"])
    parser.add_argument("--symbol", default="BTCUSDT")
    return parser.parse_args()


async def call(name: str, awaitable):
    try:
        payload = await awaitable
        return {"name": name, "ok": True, "payload": payload}
    except (BinanceAPIError, RuntimeError) as exc:
        return {"name": name, "ok": False, "error": str(exc)}


async def async_main() -> int:
    args = parse_args()
    credentials = load_credentials(required=True)
    environment = credentials.environment if credentials is not None else resolve_environment(args.env or "prod")
    proxy_settings = load_proxy_settings()

    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceFuturesClient(
            session=session,
            environment=environment,
            credentials=credentials,
            proxy_settings=proxy_settings,
        )
        account_api_error: str | None = None
        try:
            account_api_mode = await client.get_account_api_mode()
        except (BinanceAPIError, RuntimeError) as exc:
            account_api_mode = client.planned_account_api_mode()
            account_api_error = str(exc)

        results = {
            "environment": environment.name,
            "account_api": {
                "requested": client.requested_account_api_mode(),
                "resolved": client.resolved_account_api_mode() or account_api_mode,
            },
            "checks": [
                await call("public_exchange_info", client.exchange_info()),
                await call("signed_account_overview", client.account_overview()),
                await call("signed_position_v3", client.position_information_v3(args.symbol)),
            ],
        }
        if account_api_error is not None:
            results["account_api"]["error"] = account_api_error
        if account_api_mode == "classic":
            results["checks"].append(
                await call(
                    "signed_test_order_minimal",
                    client.test_order(
                        {
                            "symbol": args.symbol,
                            "side": "BUY",
                            "type": "MARKET",
                            "quantity": "0.001",
                            "newOrderRespType": "RESULT",
                        }
                    ),
                )
            )
        else:
            results["checks"].append(
                {
                    "name": "signed_test_order_minimal",
                    "ok": None,
                    "skipped": True,
                    "reason": "Portfolio Margin UM APIs do not provide an order/test endpoint.",
                }
            )

    print(json.dumps(results, ensure_ascii=False, separators=(",", ":")))
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
