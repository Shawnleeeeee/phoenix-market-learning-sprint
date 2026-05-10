"""Signed Binance futures client for Leiting testnet/classic UM."""

from __future__ import annotations

import hashlib
import hmac
import json
import socket
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener

from btc_engine.config import get_api_key, get_api_secret, get_rest_base_url, get_symbol
from btc_engine.market_data.public_client import resolve_proxy

USER_AGENT = "Leiting-BTC-Signed/1.0"


class BinanceSignedError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, payload: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.payload = payload

    @property
    def code(self) -> int | None:
        if not isinstance(self.payload, dict):
            return None
        code = self.payload.get("code")
        return code if isinstance(code, int) else None


class BinanceSignedFuturesClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str | None = None,
        proxy: str | None = None,
        timeout_sec: int = 15,
        recv_window_ms: int = 5000,
    ) -> None:
        self.api_key = api_key if api_key is not None else get_api_key()
        self.api_secret = api_secret if api_secret is not None else get_api_secret()
        self.base_url = (base_url if base_url is not None else get_rest_base_url()).rstrip("/")
        self.proxy = proxy if proxy is not None else resolve_proxy()
        self.timeout_sec = timeout_sec
        self.recv_window_ms = recv_window_ms
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else {}
        self.opener = build_opener(ProxyHandler(proxies))
        self.opener.addheaders = [("User-Agent", USER_AGENT)]
        if not self.api_key or not self.api_secret:
            raise RuntimeError("BTC testnet API credentials are missing in btc_config/live.env")

    @staticmethod
    def _is_unknown_execution_error(exc: Exception) -> bool:
        if isinstance(exc, BinanceSignedError):
            if exc.status == 503:
                return True
            message = str(exc).lower()
            return "unknown error" in message or "service unavailable" in message
        if isinstance(exc, (TimeoutError, socket.timeout)):
            return True
        if isinstance(exc, URLError) and "timed out" in str(exc).lower():
            return True
        return "timed out" in str(exc).lower()

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None, *, signed: bool = True) -> Any:
        params = params or {}
        headers: list[tuple[str, str]] = [("User-Agent", USER_AGENT), ("X-MBX-APIKEY", self.api_key)]
        if signed:
            normalized = {k: str(v) for k, v in params.items() if v is not None}
            normalized["recvWindow"] = str(self.recv_window_ms)
            normalized["timestamp"] = str(int(time.time() * 1000))
            query = urlencode(list(normalized.items()))
            signature = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
            query = f"{query}&signature={signature}"
        else:
            query = urlencode([(k, str(v)) for k, v in params.items() if v is not None])
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = __import__("urllib.request", fromlist=["Request"]).Request(url=url, method=method, headers=dict(headers))
        try:
            with self.opener.open(request, timeout=self.timeout_sec) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = None
            try:
                body = json.loads(exc.read().decode("utf-8"))
            except Exception:  # noqa: BLE001
                body = None
            if isinstance(body, dict) and "code" in body:
                raise BinanceSignedError(
                    f"Binance signed error: {body}",
                    status=exc.code,
                    payload=body,
                ) from exc
            raise RuntimeError(f"Signed request failed: {method} {path}: HTTP {exc.code}") from exc
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Signed request failed: {method} {path}: {exc}") from exc
        if isinstance(payload, dict) and "code" in payload and isinstance(payload["code"], int) and payload["code"] < 0:
            raise BinanceSignedError(f"Binance signed error: {payload}", payload=payload)
        return payload

    def ping_account(self) -> Any:
        return self._request("GET", "/fapi/v3/balance")

    def account(self) -> Any:
        return self._request("GET", "/fapi/v2/account")

    def position_risk(self, symbol: str | None = None) -> Any:
        params = {"symbol": symbol} if symbol else {}
        return self._request("GET", "/fapi/v3/positionRisk", params)

    def position_mode(self) -> Any:
        return self._request("GET", "/fapi/v1/positionSide/dual")

    def assert_one_way_mode(self) -> None:
        payload = self.position_mode()
        dual = payload.get("dualSidePosition")
        dual_enabled = str(dual).lower() == "true"
        if dual_enabled:
            raise RuntimeError("Leiting requires Binance One-way position mode. Current account is Hedge Mode.")

    def change_leverage(self, symbol: str, leverage: int) -> Any:
        return self._request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})

    def test_order(self, payload: dict[str, Any]) -> Any:
        return self._request("POST", "/fapi/v1/order/test", payload)

    def get_order(self, symbol: str, *, order_id: int | None = None, client_order_id: str | None = None) -> Any:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id
        return self._request("GET", "/fapi/v1/order", params)

    def get_open_algo_order(self, symbol: str, *, algo_id: int | None = None, client_algo_id: str | None = None) -> Any | None:
        for item in self.open_algo_orders(symbol):
            if algo_id is not None and int(item.get("algoId") or 0) == int(algo_id):
                return item
            if client_algo_id is not None and item.get("clientAlgoId") == client_algo_id:
                return item
        return None

    def new_order(self, payload: dict[str, Any]) -> Any:
        try:
            return self._request("POST", "/fapi/v1/order", payload)
        except Exception as exc:  # noqa: BLE001
            if not self._is_unknown_execution_error(exc):
                raise
            symbol = str(payload.get("symbol") or "")
            client_order_id = payload.get("newClientOrderId")
            if not symbol or not client_order_id:
                raise
            for _ in range(3):
                time.sleep(0.35)
                try:
                    order = self.get_order(symbol, client_order_id=str(client_order_id))
                except Exception:  # noqa: BLE001
                    continue
                if order:
                    if isinstance(order, dict):
                        order["recovered_after_unknown"] = True
                    return order
            raise

    def new_algo_order(self, payload: dict[str, Any]) -> Any:
        try:
            return self._request("POST", "/fapi/v1/algoOrder", payload)
        except Exception as exc:  # noqa: BLE001
            if not self._is_unknown_execution_error(exc):
                raise
            symbol = str(payload.get("symbol") or "")
            client_algo_id = payload.get("clientAlgoId")
            if not symbol or not client_algo_id:
                raise
            for _ in range(3):
                time.sleep(0.35)
                order = self.get_open_algo_order(symbol, client_algo_id=str(client_algo_id))
                if order:
                    if isinstance(order, dict):
                        order["recovered_after_unknown"] = True
                    return order
            raise

    def cancel_algo_order(self, *, algo_id: int | None = None, client_algo_id: str | None = None) -> Any:
        params: dict[str, Any] = {}
        if algo_id is not None:
            params["algoId"] = algo_id
        if client_algo_id is not None:
            params["clientAlgoId"] = client_algo_id
        return self._request("DELETE", "/fapi/v1/algoOrder", params)

    def cancel_order(self, symbol: str, order_id: int | None = None, client_order_id: str | None = None) -> Any:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id
        return self._request("DELETE", "/fapi/v1/order", params)

    def cancel_all_open_orders(self, symbol: str) -> Any:
        return self._request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})

    def cancel_all_open_algo_orders(self, symbol: str) -> list[Any]:
        response = self._request("DELETE", "/fapi/v1/algoOpenOrders", {"symbol": symbol})
        return [response]

    def open_orders(self, symbol: str | None = None) -> Any:
        params = {"symbol": symbol} if symbol else {}
        return self._request("GET", "/fapi/v1/openOrders", params)

    def open_algo_orders(self, symbol: str | None = None) -> Any:
        params = {"symbol": symbol} if symbol else {}
        return self._request("GET", "/fapi/v1/openAlgoOrders", params)

    def user_trades(self, symbol: str, *, start_time_ms: int | None = None, end_time_ms: int | None = None, limit: int = 100) -> Any:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return self._request("GET", "/fapi/v1/userTrades", params)

    def income_history(self, symbol: str | None = None, *, start_time_ms: int | None = None, end_time_ms: int | None = None, limit: int = 100) -> Any:
        params: dict[str, Any] = {"limit": limit}
        if symbol is not None:
            params["symbol"] = symbol
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return self._request("GET", "/fapi/v1/income", params)

    def start_user_data_stream(self) -> Any:
        return self._request("POST", "/fapi/v1/listenKey", {}, signed=False)

    def keepalive_user_data_stream(self, listen_key: str | None = None) -> Any:
        params = {"listenKey": listen_key} if listen_key else {}
        return self._request("PUT", "/fapi/v1/listenKey", params, signed=False)

    def close_user_data_stream(self, listen_key: str | None = None) -> Any:
        params = {"listenKey": listen_key} if listen_key else {}
        return self._request("DELETE", "/fapi/v1/listenKey", params, signed=False)

    def exchange_info(self) -> Any:
        url = f"{self.base_url}/fapi/v1/exchangeInfo?symbol={get_symbol()}"
        request = __import__("urllib.request", fromlist=["Request"]).Request(url=url, method="GET", headers={"User-Agent": USER_AGENT})
        with self.opener.open(request, timeout=self.timeout_sec) as response:
            return json.loads(response.read().decode("utf-8"))

    def depth(self, symbol: str, *, limit: int = 20) -> Any:
        return self._request("GET", "/fapi/v1/depth", {"symbol": symbol, "limit": limit}, signed=False)
