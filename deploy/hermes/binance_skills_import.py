#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from deploy.hermes.hermes_telegram import telegram_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Binance Skills payloads into Hermes memories.")
    parser.add_argument("--hermes-home", default=os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes"))
    parser.add_argument("--assets-json")
    parser.add_argument("--market-rank-json")
    parser.add_argument("--trading-signal-json")
    parser.add_argument("--fetch-assets", action="store_true")
    parser.add_argument("--fetch-market-rank", action="store_true")
    parser.add_argument("--fetch-trading-signal", action="store_true")
    parser.add_argument("--market-rank-page", type=int, default=1)
    parser.add_argument("--market-rank-size", type=int, default=5)
    parser.add_argument("--market-rank-period", type=int, default=50)
    parser.add_argument("--smart-money-chain-id", action="append", dest="smart_money_chain_ids")
    parser.add_argument("--smart-money-period", default="24h")
    parser.add_argument("--signal-chain-id", action="append", dest="signal_chain_ids")
    parser.add_argument("--signal-page", type=int, default=1)
    parser.add_argument("--signal-page-size", type=int, default=100)
    parser.add_argument("--notify-telegram", action="store_true")
    return parser.parse_args()


def _load_payload(source: str | None) -> dict[str, Any] | None:
    if not source:
        return None
    path = Path(source)
    if path.exists():
        text = path.read_text(encoding="utf-8")
    else:
        text = source
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    return payload


def _strip_matching_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_env_file(home: Path) -> dict[str, str]:
    env_path = home / ".env"
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _strip_matching_quotes(value.strip())
    return values


def _load_config_values(home: Path) -> dict[str, str]:
    config_path = home / "config.yaml"
    values: dict[str, str] = {}
    if not config_path.exists():
        return values
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if key not in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"}:
            continue
        values[key] = value.strip().strip("'").strip('"')
    return values


def _get_setting(home: Path, key: str) -> str:
    if os.environ.get(key):
        return str(os.environ[key]).strip()
    env = _load_env_file(home)
    if env.get(key):
        return env[key].strip()
    cfg = _load_config_values(home)
    if cfg.get(key):
        return cfg[key].strip()
    return ""


def _proxy_handler(home: Path) -> urllib.request.ProxyHandler | None:
    proxies: dict[str, str] = {}
    http_proxy = _get_setting(home, "HTTP_PROXY")
    https_proxy = _get_setting(home, "HTTPS_PROXY")
    all_proxy = _get_setting(home, "ALL_PROXY")
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    elif all_proxy:
        proxies["https"] = all_proxy
    if all_proxy and "http" not in proxies:
        proxies["http"] = all_proxy
    return urllib.request.ProxyHandler(proxies) if proxies else None


def _signal_endpoint() -> str:
    return "https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai"


def _market_rank_endpoint() -> str:
    return "https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list/ai"


def _smart_money_endpoint() -> str:
    return "https://web3.binance.com/bapi/defi/v1/public/wallet-direct/tracker/wallet/token/inflow/rank/query/ai"


def _fetch_json(url: str, payload: dict[str, Any], *, home: Path) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "binance-web3/1.1 (Skill)",
        },
    )
    opener = urllib.request.build_opener()
    proxy_handler = _proxy_handler(home)
    if proxy_handler is not None:
        opener.add_handler(proxy_handler)
    with opener.open(request, timeout=20) as response:
        body = json.loads(response.read().decode("utf-8"))
    if not isinstance(body, dict):
        raise ValueError("Binance trading-signal returned non-object JSON")
    if str(body.get("code") or "") != "000000" and body.get("success") is not True:
        raise ValueError(f"Binance trading-signal error: {body.get('code')} {body.get('message')}")
    return body


def _relay_settings(home: Path) -> tuple[str, str]:
    settings = telegram_settings(home)
    target = ((settings.get("relay_target") or "").strip() or "root@43.98.174.32")
    key_path = (settings.get("relay_key_path") or "").strip()
    return target, key_path


def _run_remote_json(home: Path, remote_cmd: str) -> dict[str, Any]:
    target, key_path = _relay_settings(home)
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no"]
    if key_path:
        cmd.extend(["-i", key_path])
    cmd.extend([target, remote_cmd])
    completed = subprocess.run(cmd, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout or f"ssh_failed:{completed.returncode}").strip())
    payload = json.loads((completed.stdout or "").strip())
    if not isinstance(payload, dict):
        raise ValueError("remote command returned non-object JSON")
    return payload


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_signal_record(item: dict[str, Any], *, chain_id: str) -> dict[str, Any]:
    direction_raw = str(item.get("direction") or "").strip().lower()
    direction = "LONG" if direction_raw == "buy" else "SHORT" if direction_raw == "sell" else direction_raw.upper()
    max_gain = _coerce_float(item.get("maxGain"))
    exit_rate = _coerce_float(item.get("exitRate"))
    confidence = round(exit_rate / 100.0, 4) if exit_rate is not None else None
    return {
        "signal_id": item.get("signalId"),
        "symbol": item.get("ticker"),
        "direction": direction,
        "score": max_gain,
        "confidence": confidence,
        "status": item.get("status"),
        "chain_id": item.get("chainId") or chain_id,
        "contract_address": item.get("contractAddress"),
        "smart_money_count": item.get("smartMoneyCount"),
        "signal_count": item.get("signalCount"),
        "trigger_time": item.get("signalTriggerTime"),
        "alert_price": item.get("alertPrice"),
        "current_price": item.get("currentPrice"),
        "highest_price": item.get("highestPrice"),
        "max_gain_pct": max_gain,
        "exit_rate_pct": exit_rate,
        "launch_platform": item.get("launchPlatform"),
        "is_alpha": item.get("isAlpha"),
        "raw": item,
    }


def _normalize_rank_token(item: dict[str, Any], *, category: str) -> dict[str, Any]:
    symbol = item.get("symbol") or item.get("token") or item.get("ticker") or item.get("baseAsset")
    return {
        "symbol": symbol,
        "chain_id": item.get("chainId"),
        "price": _coerce_float(item.get("price")),
        "market_cap": _coerce_float(item.get("marketCap")),
        "liquidity": _coerce_float(item.get("liquidity")),
        "percent_change_24h": _coerce_float(item.get("percentChange24h")),
        "volume_24h": _coerce_float(item.get("volume24h")),
        "holders": _coerce_float(item.get("holders")),
        "category": category,
        "raw": item,
    }


def _normalize_inflow_token(item: dict[str, Any], *, chain_id: str) -> dict[str, Any]:
    symbol = item.get("symbol") or item.get("tokenName") or item.get("token") or item.get("ticker")
    return {
        "symbol": symbol,
        "chain_id": item.get("chainId") or chain_id,
        "price": _coerce_float(item.get("price")),
        "market_cap": _coerce_float(item.get("marketCap")),
        "liquidity": _coerce_float(item.get("liquidity")),
        "percent_change_24h": _coerce_float(item.get("priceChangeRate")),
        "volume_24h": _coerce_float(item.get("volume")),
        "holders": _coerce_float(item.get("holders")),
        "inflow": _coerce_float(item.get("inflow")) or _coerce_float(item.get("inflowAmount")),
        "traders": _coerce_float(item.get("traders")) or _coerce_float(item.get("traderCount")),
        "raw": item,
    }


def _fetch_assets_payload(*, home: Path) -> dict[str, Any]:
    account = _run_remote_json(
        home,
        "cd /opt/leiting-btc && ./.venv/bin/python -m btc_engine.execution.router account",
    )
    raw_account = account.get("account") or {}
    balances = raw_account.get("assets") or raw_account.get("balances") or raw_account.get("asset") or []
    distribution: list[dict[str, Any]] = []
    if isinstance(balances, list):
        for item in balances:
            if not isinstance(item, dict):
                continue
            asset = item.get("asset")
            wallet_balance = _coerce_float(item.get("walletBalance") or item.get("balance"))
            available_balance = _coerce_float(item.get("availableBalance") or item.get("available"))
            if asset and ((wallet_balance or 0.0) > 0 or (available_balance or 0.0) > 0):
                distribution.append(
                    {
                        "asset": asset,
                        "wallet_balance": wallet_balance,
                        "available_balance": available_balance,
                    }
                )
    distribution.sort(key=lambda row: (row.get("wallet_balance") or 0.0), reverse=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "leiting_demo_account",
        "environment": "binance_demo_usds_futures",
        "equity_usdt": _coerce_float(raw_account.get("totalMarginBalance") or raw_account.get("totalWalletBalance")),
        "available_balance_usdt": _coerce_float(raw_account.get("availableBalance")),
        "unrealized_pnl_usdt": _coerce_float(raw_account.get("totalUnrealizedProfit")),
        "distribution": distribution,
        "account_alias": account.get("account_api") or account.get("environment") or "demo",
        "raw_account": raw_account,
    }


def _fetch_market_rank_payload(
    *,
    home: Path,
    page: int,
    size: int,
    period: int,
    smart_money_chain_ids: list[str],
    smart_money_period: str,
) -> dict[str, Any]:
    unified_endpoint = _market_rank_endpoint()
    inflow_endpoint = _smart_money_endpoint()
    rank_type_map = {
        "trending": 10,
        "top_search": 11,
        "alpha": 20,
    }
    ranked_sections: dict[str, list[dict[str, Any]]] = {}
    for name, rank_type in rank_type_map.items():
        response = _fetch_json(
            unified_endpoint,
            {
                "rankType": rank_type,
                "period": period,
                "page": page,
                "size": size,
            },
            home=home,
        )
        items = response.get("data") or []
        if isinstance(items, dict):
            items = items.get("tokens") or items.get("list") or items.get("rows") or []
        ranked_sections[name] = [
            _normalize_rank_token(item, category=name)
            for item in items
            if isinstance(item, dict)
        ]

    smart_money: list[dict[str, Any]] = []
    counts_by_chain: dict[str, int] = {}
    for chain_id in smart_money_chain_ids:
        response = _fetch_json(
            inflow_endpoint,
            {
                "chainId": chain_id,
                "period": smart_money_period,
                "tagType": 2,
            },
            home=home,
        )
        items = response.get("data") or []
        if isinstance(items, dict):
            items = items.get("list") or items.get("rows") or []
        normalized = [
            _normalize_inflow_token(item, chain_id=chain_id)
            for item in items
            if isinstance(item, dict)
        ]
        counts_by_chain[chain_id] = len(normalized)
        smart_money.extend(normalized)
    smart_money.sort(key=lambda item: (item.get("inflow") or float("-inf")), reverse=True)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "binance_crypto_market_rank_skill",
        "unified_rank_endpoint": unified_endpoint,
        "smart_money_endpoint": inflow_endpoint,
        "page": page,
        "size": size,
        "period": period,
        "smart_money_period": smart_money_period,
        "smart_money_chain_ids": smart_money_chain_ids,
        "smart_money_counts_by_chain": counts_by_chain,
        "trending": ranked_sections.get("trending") or [],
        "top_search": ranked_sections.get("top_search") or [],
        "alpha": ranked_sections.get("alpha") or [],
        "smart_money": smart_money[: max(size, 5)],
    }


def _fetch_trading_signal_payload(*, home: Path, chain_ids: list[str], page: int, page_size: int) -> dict[str, Any]:
    all_signals: list[dict[str, Any]] = []
    per_chain_counts: dict[str, int] = {}
    endpoint = _signal_endpoint()
    for chain_id in chain_ids:
        response = _fetch_json(
            endpoint,
            {
                "smartSignalType": "",
                "page": page,
                "pageSize": page_size,
                "chainId": chain_id,
            },
            home=home,
        )
        items = response.get("data") or []
        if not isinstance(items, list):
            items = []
        normalized = [_normalize_signal_record(item, chain_id=chain_id) for item in items if isinstance(item, dict)]
        per_chain_counts[chain_id] = len(normalized)
        all_signals.extend(normalized)
    buy_signals = [item for item in all_signals if item.get("direction") == "LONG"]
    sell_signals = [item for item in all_signals if item.get("direction") == "SHORT"]
    buy_signals.sort(key=lambda item: ((item.get("score") or float("-inf")), (item.get("confidence") or float("-inf"))), reverse=True)
    sell_signals.sort(key=lambda item: ((item.get("score") or float("-inf")), (item.get("confidence") or float("-inf"))), reverse=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "binance_trading_signal_skill",
        "endpoint": endpoint,
        "chain_ids": chain_ids,
        "page": page,
        "page_size": page_size,
        "counts_by_chain": per_chain_counts,
        "signals": all_signals,
        "buy_signals": buy_signals,
        "sell_signals": sell_signals,
    }


def main() -> None:
    args = parse_args()
    home = Path(args.hermes_home)
    skills_dir = home / "memories" / "binance_skills"
    skills_dir.mkdir(parents=True, exist_ok=True)

    imported: dict[str, str] = {}
    assets: dict[str, Any] | None = None
    if args.assets_json:
        assets = _load_payload(args.assets_json)
    elif args.fetch_assets:
        assets = _fetch_assets_payload(home=home)
    if assets:
        target = skills_dir / "assets_latest.json"
        target.write_text(json.dumps(assets, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        imported["assets"] = str(target)
    market: dict[str, Any] | None = None
    if args.market_rank_json:
        market = _load_payload(args.market_rank_json)
    elif args.fetch_market_rank:
        market = _fetch_market_rank_payload(
            home=home,
            page=args.market_rank_page,
            size=args.market_rank_size,
            period=args.market_rank_period,
            smart_money_chain_ids=args.smart_money_chain_ids or ["56", "CT_501"],
            smart_money_period=args.smart_money_period,
        )
    if market:
        target = skills_dir / "crypto_market_rank_latest.json"
        target.write_text(json.dumps(market, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        imported["crypto_market_rank"] = str(target)
    signal: dict[str, Any] | None = None
    if args.trading_signal_json:
        signal = _load_payload(args.trading_signal_json)
    elif args.fetch_trading_signal:
        chain_ids = args.signal_chain_ids or ["CT_501", "56"]
        signal = _fetch_trading_signal_payload(
            home=home,
            chain_ids=chain_ids,
            page=args.signal_page,
            page_size=args.signal_page_size,
        )
    if signal:
        target = skills_dir / "trading_signal_latest.json"
        target.write_text(json.dumps(signal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        imported["trading_signal"] = str(target)

    digest_script = Path(__file__).resolve().with_name("leiting_skill_digest.py")
    cmd = ["python3", str(digest_script), "--hermes-home", str(home)]
    if args.notify_telegram:
        cmd.append("--notify-telegram")
    completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
    print(
        json.dumps(
            {
                "ok": True,
                "imported": imported,
                "digest_result": json.loads(completed.stdout or "{}"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
