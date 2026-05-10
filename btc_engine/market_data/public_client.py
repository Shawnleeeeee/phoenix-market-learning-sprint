"""Public Binance futures client for the Leiting BTC engine."""

from __future__ import annotations

import json
import os
import time
from json import JSONDecodeError
from typing import Any
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener

from btc_engine.config import get_rest_base_url

FAPI_BASE_URL = "https://fapi.binance.com"
USER_AGENT = "Leiting-BTC-Engine/1.0"


def resolve_proxy() -> str | None:
    for key in ("BTC_ALL_PROXY", "ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        value = os.environ.get(key)
        if value:
            return value
    return None


class BinancePublicFuturesClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        proxy: str | None = None,
        timeout_sec: int = 15,
        max_retries: int = 4,
        pause_sec: float = 0.1,
    ) -> None:
        self.base_url = (base_url if base_url is not None else get_rest_base_url()).rstrip("/") or FAPI_BASE_URL
        self.proxy = proxy if proxy is not None else resolve_proxy()
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.pause_sec = pause_sec
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else {}
        self.opener = build_opener(ProxyHandler(proxies))
        self.opener.addheaders = [("User-Agent", USER_AGENT)]

    def _build_url(self, path: str, params: dict[str, Any] | None = None, *, base_url: str | None = None) -> str:
        params = params or {}
        query = urlencode(params)
        url = f"{(base_url or self.base_url).rstrip('/')}{path}"
        if query:
            url = f"{url}?{query}"
        return url

    def _load_json(self, url: str) -> Any:
        with self.opener.open(url, timeout=self.timeout_sec) as response:
            return json.loads(response.read().decode("utf-8"))

    def _should_retry_on_mainnet(self, path: str) -> bool:
        return path.startswith("/futures/data/") or path.startswith("/fapi/v1/openInterest") or path.startswith("/fapi/v1/premiumIndex")

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = self._build_url(path, params)
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return self._load_json(url)
            except JSONDecodeError as exc:
                last_error = exc
                if self.base_url != FAPI_BASE_URL and self._should_retry_on_mainnet(path):
                    try:
                        return self._load_json(self._build_url(path, params, base_url=FAPI_BASE_URL))
                    except Exception as fallback_exc:  # noqa: BLE001
                        last_error = fallback_exc
                if attempt >= self.max_retries:
                    raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if self.base_url != FAPI_BASE_URL and self._should_retry_on_mainnet(path):
                    try:
                        return self._load_json(self._build_url(path, params, base_url=FAPI_BASE_URL))
                    except Exception as fallback_exc:  # noqa: BLE001
                        last_error = fallback_exc
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(attempt * 0.5, 2.0))
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Unexpected public request failure: {url}")
