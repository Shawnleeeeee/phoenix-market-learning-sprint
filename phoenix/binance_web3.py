from __future__ import annotations

import uuid
from typing import Any

import aiohttp

from phoenix.config import BinanceEnvironment, ProxySettings, load_proxy_settings

WEB3_USER_AGENT = "binance-web3/phoenix"
AUDIT_USER_AGENT = "binance-web3/1.4 (Skill)"


class BinanceWeb3Client:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        environment: BinanceEnvironment,
        proxy_settings: ProxySettings | None = None,
    ) -> None:
        self.session = session
        self.base_url = environment.web3_base.rstrip("/")
        self.proxy_settings = proxy_settings or load_proxy_settings()

    async def fetch_alpha_tokens(self, page_size: int = 200) -> list[dict[str, Any]]:
        page = 1
        items: list[dict[str, Any]] = []
        total = None
        while total is None or len(items) < total:
            payload = {
                "rankType": 20,
                "period": 50,
                "page": page,
                "size": page_size,
            }
            response = await self._post(
                "/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list/ai",
                payload,
            )
            data = response["data"]
            total = int(data["total"])
            items.extend(data["tokens"])
            page += 1
        return items

    async def fetch_social_hype(self, chain_id: str, time_range: int = 1) -> list[dict[str, Any]]:
        payload = await self._get(
            "/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/social/hype/rank/leaderboard/ai",
            {
                "chainId": chain_id,
                "sentiment": "All",
                "socialLanguage": "ALL",
                "targetLanguage": "en",
                "timeRange": time_range,
            },
        )
        return payload["data"]["leaderBoardList"]

    async def fetch_unified_rank(
        self,
        rank_type: int,
        *,
        chain_id: str | None = None,
        period: int = 50,
        page_size: int = 200,
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "rankType": rank_type,
            "period": period,
            "page": 1,
            "size": page_size,
        }
        if chain_id:
            body["chainId"] = chain_id
        payload = await self._post(
            "/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/unified/rank/list/ai",
            body,
        )
        data = payload.get("data") or {}
        tokens = data.get("tokens") or []
        return tokens if isinstance(tokens, list) else []

    async def fetch_smart_money_signals(self, chain_id: str, page_size: int = 100) -> list[dict[str, Any]]:
        payload = await self._post(
            "/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai",
            {
                "smartSignalType": "",
                "page": 1,
                "pageSize": page_size,
                "chainId": chain_id,
            },
        )
        return payload["data"]

    async def fetch_topic_rush(
        self,
        chain_id: str,
        rank_type: int = 20,
        sort: int = 20,
        asc: bool = False,
    ) -> list[dict[str, Any]]:
        payload = await self._get(
            "/bapi/defi/v2/public/wallet-direct/buw/wallet/market/token/social-rush/rank/list/ai",
            {
                "chainId": chain_id,
                "rankType": rank_type,
                "sort": sort,
                "asc": str(asc).lower(),
            },
        )
        return payload["data"]

    async def fetch_token_audit(self, chain_id: str, contract_address: str) -> dict[str, Any]:
        url = f"{self.base_url}/bapi/defi/v1/public/wallet-direct/security/token/audit"
        headers = {
            "Accept-Encoding": "identity",
            "Content-Type": "application/json",
            "User-Agent": AUDIT_USER_AGENT,
            "source": "agent",
        }
        body = {
            "binanceChainId": chain_id,
            "contractAddress": contract_address,
            "requestId": str(uuid.uuid4()),
        }
        async with self.session.post(
            url,
            json=body,
            headers=headers,
            proxy=self.proxy_settings.proxy_for_url(url),
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {
            "Accept-Encoding": "identity",
            "User-Agent": WEB3_USER_AGENT,
        }
        async with self.session.get(
            url,
            params=params,
            headers=headers,
            proxy=self.proxy_settings.proxy_for_url(url),
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {
            "Accept-Encoding": "identity",
            "Content-Type": "application/json",
            "User-Agent": WEB3_USER_AGENT,
        }
        async with self.session.post(
            url,
            json=payload,
            headers=headers,
            proxy=self.proxy_settings.proxy_for_url(url),
        ) as response:
            response.raise_for_status()
            return await response.json()
