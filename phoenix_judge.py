#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import aiohttp

from phoenix.binance_web3 import BinanceWeb3Client
from phoenix.config import load_proxy_settings, resolve_environment
from phoenix.judge import JudgeConfig, PhoenixJudge


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank Phoenix scout universe using Binance Web3 signals.")
    parser.add_argument("--snapshot-file", type=Path, default=Path("phoenix_snapshot.json"))
    parser.add_argument("--output-file", type=Path, default=Path("phoenix_candidates.json"))
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--audit-top-n", type=int, default=20)
    parser.add_argument("--env", default="prod", choices=["prod", "testnet", "demo"])
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    timeout = aiohttp.ClientTimeout(total=45, sock_connect=15, sock_read=30)
    proxy_settings = load_proxy_settings()
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceWeb3Client(
            session=session,
            environment=resolve_environment(args.env),
            proxy_settings=proxy_settings,
        )
        judge = PhoenixJudge(
            web3_client=client,
            config=JudgeConfig(top_n=args.top_n, audit_top_n=args.audit_top_n),
        )
        result = await judge.evaluate_snapshot(args.snapshot_file)

    args.output_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
