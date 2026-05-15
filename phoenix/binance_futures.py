from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from typing import Any
from urllib.parse import urlencode

import aiohttp

from phoenix.config import BinanceCredentials, BinanceEnvironment, ProxySettings, load_proxy_settings

CLASSIC_ALGO_ORDER_PATH = "/fapi/v1/algoOrder"
CLASSIC_OPEN_ALGO_ORDERS_PATH = "/fapi/v1/openAlgoOrders"
CLASSIC_CANCEL_ALL_ALGO_ORDERS_PATH = "/fapi/v1/algoOpenOrders"


class BinanceAPIError(RuntimeError):
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


class BinanceFuturesClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        environment: BinanceEnvironment,
        credentials: BinanceCredentials | None = None,
        proxy_settings: ProxySettings | None = None,
    ) -> None:
        self.session = session
        self.environment = environment
        self.credentials = credentials
        self.proxy_settings = proxy_settings or load_proxy_settings()
        self.base_url = environment.futures_rest_base.rstrip("/")
        self.classic_base_url = environment.futures_rest_base.rstrip("/")
        self.portfolio_base_url = environment.portfolio_rest_base.rstrip("/")
        self._resolved_account_api_mode: str | None = None
        self._time_offset_ms = 0

    async def exchange_info(self) -> dict[str, Any]:
        return await self._public_request("GET", "/fapi/v1/exchangeInfo")

    async def mark_price(self, symbol: str) -> dict[str, Any]:
        return await self._public_request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})

    async def book_ticker(self, symbol: str) -> dict[str, Any]:
        return await self._public_request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol})

    async def depth(self, symbol: str, *, limit: int = 5) -> dict[str, Any]:
        return await self._public_request("GET", "/fapi/v1/depth", {"symbol": symbol, "limit": limit})

    async def open_interest(self, symbol: str) -> dict[str, Any]:
        return await self._public_request("GET", "/fapi/v1/openInterest", {"symbol": symbol})

    async def open_interest_hist(
        self,
        symbol: str,
        *,
        period: str = "5m",
        limit: int = 4,
    ) -> list[dict[str, Any]]:
        payload = await self._public_request(
            "GET",
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": limit},
        )
        return payload if isinstance(payload, list) else []

    async def server_time(self) -> dict[str, Any]:
        return await self._public_request("GET", "/fapi/v1/time")

    async def sync_server_time(self) -> int:
        return await self._sync_server_time()

    async def ticker_24hr(self) -> list[dict[str, Any]]:
        payload = await self._public_request("GET", "/fapi/v1/ticker/24hr")
        return payload if isinstance(payload, list) else []

    async def klines(
        self,
        symbol: str,
        *,
        interval: str = "1m",
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 100,
    ) -> list[list[Any]]:
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        payload = await self._public_request(
            "GET",
            "/fapi/v1/klines",
            params,
        )
        return payload if isinstance(payload, list) else []

    async def position_information_v3(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol} if symbol else None
        return await self._signed_account_request(
            "GET",
            classic_path="/fapi/v3/positionRisk",
            portfolio_path="/papi/v1/um/positionRisk",
            params=params,
        )

    async def account_overview(self) -> dict[str, Any] | list[dict[str, Any]]:
        return await self._signed_account_request(
            "GET",
            classic_path="/fapi/v3/balance",
            portfolio_path="/papi/v1/account",
        )

    async def futures_account_balance_v3(self) -> dict[str, Any] | list[dict[str, Any]]:
        return await self.account_overview()

    async def change_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        return await self._signed_account_request(
            "POST",
            classic_path="/fapi/v1/marginType",
            portfolio_path=None,
            params={"symbol": symbol, "marginType": margin_type},
        )

    async def change_initial_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        return await self._signed_account_request(
            "POST",
            classic_path="/fapi/v1/leverage",
            portfolio_path="/papi/v1/um/leverage",
            params={"symbol": symbol, "leverage": leverage},
        )

    async def um_account_config(self) -> dict[str, Any]:
        return await self._signed_account_request(
            "GET",
            classic_path=None,
            portfolio_path="/papi/v1/um/accountConfig",
        )

    async def um_symbol_config(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol} if symbol else None
        return await self._signed_account_request(
            "GET",
            classic_path=None,
            portfolio_path="/papi/v1/um/symbolConfig",
            params=params,
        )

    async def start_user_data_stream(self) -> dict[str, Any]:
        return await self._signed_account_request(
            "POST",
            classic_path="/fapi/v1/listenKey",
            portfolio_path="/papi/v1/listenKey",
            signed=False,
        )

    async def keepalive_user_data_stream(self, listen_key: str) -> dict[str, Any]:
        return await self._signed_account_request(
            "PUT",
            classic_path="/fapi/v1/listenKey",
            portfolio_path="/papi/v1/listenKey",
            params={"listenKey": listen_key},
            signed=False,
        )

    async def test_order(
        self,
        payload: dict[str, Any],
        *,
        endpoint: str | None = None,
    ) -> dict[str, Any]:
        mode = await self.get_account_api_mode()
        if mode == "classic" and endpoint == CLASSIC_ALGO_ORDER_PATH:
            return {
                "previewOnly": True,
                "endpoint": endpoint,
                "reason": "Binance USD-M algo orders do not expose a /test endpoint; payload preview only.",
                "payload": payload,
            }
        return await self._signed_account_request(
            "POST",
            classic_path="/fapi/v1/order/test",
            portfolio_path=None,
            params=payload,
        )

    async def new_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._signed_account_request(
            "POST",
            classic_path="/fapi/v1/order",
            portfolio_path="/papi/v1/um/order",
            params=payload,
        )

    async def new_conditional_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        mode = await self.get_account_api_mode()
        if mode == "portfolio_margin":
            return await self._signed_request_to(
                self.portfolio_base_url,
                "POST",
                "/papi/v1/um/conditional/order",
                payload,
            )
        response = await self._signed_request_to(
            self.classic_base_url,
            "POST",
            CLASSIC_ALGO_ORDER_PATH,
            self._normalize_classic_algo_payload(payload),
        )
        return self._normalize_classic_algo_response(response)

    async def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol} if symbol else None
        return await self._signed_account_request(
            "GET",
            classic_path="/fapi/v1/openOrders",
            portfolio_path="/papi/v1/um/openOrders",
            params=params,
        )

    async def open_conditional_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol} if symbol else None
        mode = await self.get_account_api_mode()
        if mode == "portfolio_margin":
            return await self._signed_request_to(
                self.portfolio_base_url,
                "GET",
                "/papi/v1/um/conditional/openOrders",
                params,
            )
        response = await self._signed_request_to(
            self.classic_base_url,
            "GET",
            CLASSIC_OPEN_ALGO_ORDERS_PATH,
            params,
        )
        return self._normalize_classic_algo_response(response)

    async def user_trades(
        self,
        symbol: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return await self._signed_account_request(
            "GET",
            classic_path="/fapi/v1/userTrades",
            portfolio_path="/papi/v1/um/userTrades",
            params=params,
        )

    async def income_history(
        self,
        symbol: str | None = None,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        income_type: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if symbol is not None:
            params["symbol"] = symbol
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        if income_type is not None:
            params["incomeType"] = income_type
        return await self._signed_account_request(
            "GET",
            classic_path="/fapi/v1/income",
            portfolio_path="/papi/v1/um/income",
            params=params,
        )

    async def cancel_order(self, symbol: str, order_id: int | None = None, client_order_id: str | None = None) -> dict[str, Any]:
        params = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id
        return await self._signed_account_request(
            "DELETE",
            classic_path="/fapi/v1/order",
            portfolio_path="/papi/v1/um/order",
            params=params,
        )

    async def cancel_all_open_orders(self, symbol: str) -> dict[str, Any] | list[dict[str, Any]]:
        return await self._signed_account_request(
            "DELETE",
            classic_path="/fapi/v1/allOpenOrders",
            portfolio_path="/papi/v1/um/allOpenOrders",
            params={"symbol": symbol},
        )

    async def cancel_conditional_order(
        self,
        symbol: str,
        *,
        strategy_id: int | None = None,
        client_strategy_id: str | None = None,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        mode = await self.get_account_api_mode()
        if mode == "portfolio_margin":
            params: dict[str, Any] = {"symbol": symbol}
            if strategy_id is not None:
                params["strategyId"] = strategy_id
            if client_strategy_id is not None:
                params["newClientStrategyId"] = client_strategy_id
            return await self._signed_request_to(
                self.portfolio_base_url,
                "DELETE",
                "/papi/v1/um/conditional/order",
                params,
            )
        params = {}
        if strategy_id is not None:
            params["algoId"] = strategy_id
        elif order_id is not None:
            params["algoId"] = order_id
        if client_strategy_id is not None:
            params["clientAlgoId"] = client_strategy_id
        elif client_order_id is not None:
            params["clientAlgoId"] = client_order_id
        if not params:
            raise RuntimeError("Cancel conditional order requires an algoId/clientAlgoId (or legacy order id/client order id).")
        response = await self._signed_request_to(
            self.classic_base_url,
            "DELETE",
            CLASSIC_ALGO_ORDER_PATH,
            params,
        )
        return self._normalize_classic_algo_response(response)

    async def cancel_all_open_conditional_orders(self, symbol: str) -> dict[str, Any] | list[dict[str, Any]]:
        mode = await self.get_account_api_mode()
        if mode == "portfolio_margin":
            return await self._signed_request_to(
                self.portfolio_base_url,
                "DELETE",
                "/papi/v1/um/conditional/allOpenOrders",
                {"symbol": symbol},
            )
        response = await self._signed_request_to(
            self.classic_base_url,
            "DELETE",
            CLASSIC_CANCEL_ALL_ALGO_ORDERS_PATH,
            {"symbol": symbol},
        )
        if isinstance(response, list):
            return [self._normalize_classic_algo_response(item) for item in response]
        return self._normalize_classic_algo_response(response)

    def requested_account_api_mode(self) -> str:
        if self.credentials is None:
            return "classic"
        return self.credentials.account_api_preference

    def planned_account_api_mode(self) -> str:
        requested = self.requested_account_api_mode()
        if requested != "auto":
            return requested
        return self._resolved_account_api_mode or "classic"

    def resolved_account_api_mode(self) -> str | None:
        return self._resolved_account_api_mode

    async def get_account_api_mode(self) -> str:
        requested = self.requested_account_api_mode()
        if requested != "auto":
            self._resolved_account_api_mode = requested
            return requested
        if self._resolved_account_api_mode is not None:
            return self._resolved_account_api_mode
        return await self._autodetect_account_api_mode()

    async def _public_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                async with self.session.request(
                    method,
                    url,
                    params=params,
                    proxy=self.proxy_settings.proxy_for_url(url),
                ) as response:
                    return await self._decode_response(response)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt >= 2:
                    break
                await asyncio.sleep(0.35 * (attempt + 1))
        raise RuntimeError(f"Public Binance Futures request failed after retries: {url}; error={last_error}")

    async def _signed_request_to(
        self,
        base_url: str,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        signed: bool = True,
    ) -> dict[str, Any]:
        if self.credentials is None:
            raise RuntimeError("Signed Binance Futures endpoint requested without credentials.")

        normalized_base = self._normalize_params(params or {})
        headers = {"X-MBX-APIKEY": self.credentials.api_key}
        recv_window_ms = self.credentials.recv_window_ms

        max_attempts = 3 if signed else 1
        last_error: BinanceAPIError | None = None
        last_network_error: Exception | None = None
        for attempt in range(max_attempts):
            normalized = dict(normalized_base)
            if signed:
                if attempt > 0:
                    recv_window_ms = max(recv_window_ms, 15000)
                normalized["recvWindow"] = str(recv_window_ms)
                normalized["timestamp"] = str(int(time.time() * 1000) + self._time_offset_ms)
                query = urlencode(list(normalized.items()))
                signature = hmac.new(
                    self.credentials.api_secret.encode("utf-8"),
                    query.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()
                query = f"{query}&signature={signature}"
            else:
                query = urlencode(list(normalized.items()))

            url = f"{base_url}{path}"
            if query:
                url = f"{url}?{query}"

            try:
                async with self.session.request(
                    method,
                    url,
                    headers=headers,
                    proxy=self.proxy_settings.proxy_for_url(url),
                ) as response:
                    try:
                        return await self._decode_response(response)
                    except BinanceAPIError as exc:
                        last_error = exc
                        if signed and exc.code == -1021 and attempt < max_attempts - 1:
                            await self._sync_server_time()
                            await asyncio.sleep(0.25 * (attempt + 1))
                            continue
                        raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_network_error = exc
                if attempt < max_attempts - 1:
                    await asyncio.sleep(0.35 * (attempt + 1))
                    continue
                raise RuntimeError(
                    f"Signed Binance Futures request failed after retries: {method} {path}; error={exc}"
                ) from exc

        if last_error is not None:
            raise last_error
        if last_network_error is not None:
            raise RuntimeError(
                f"Signed Binance Futures request failed after retries: {method} {path}; error={last_network_error}"
            )
        raise RuntimeError("Signed Binance request did not return a response.")

    async def _signed_account_request(
        self,
        method: str,
        *,
        classic_path: str | None,
        portfolio_path: str | None,
        params: dict[str, Any] | None = None,
        signed: bool = True,
    ) -> dict[str, Any]:
        mode = await self.get_account_api_mode()
        if mode == "portfolio_margin":
            if portfolio_path is None:
                raise RuntimeError(
                    "The selected Binance Portfolio Margin account does not expose this endpoint via PAPI."
                )
            return await self._signed_request_to(
                self.portfolio_base_url,
                method,
                portfolio_path,
                params,
                signed=signed,
            )

        if classic_path is None:
            raise RuntimeError("This operation requires the classic USD-M futures API.")
        return await self._signed_request_to(
            self.classic_base_url,
            method,
            classic_path,
            params,
            signed=signed,
        )

    async def _autodetect_account_api_mode(self) -> str:
        if self._time_offset_ms == 0:
            try:
                await self._sync_server_time()
            except Exception:
                pass
        classic_error: BinanceAPIError | None = None
        try:
            await self._signed_request_to(self.classic_base_url, "GET", "/fapi/v3/balance")
            self._resolved_account_api_mode = "classic"
            return "classic"
        except BinanceAPIError as exc:
            classic_error = exc
            if not self._should_try_portfolio_margin(exc):
                raise

        try:
            await self._signed_request_to(self.portfolio_base_url, "GET", "/papi/v1/um/account")
            self._resolved_account_api_mode = "portfolio_margin"
            return "portfolio_margin"
        except BinanceAPIError as exc:
            raise BinanceAPIError(
                f"Could not resolve Binance account API mode. classic={classic_error}; portfolio_margin={exc}",
                status=exc.status,
                payload={
                    "classic": classic_error.payload if classic_error is not None else None,
                    "portfolio_margin": exc.payload,
                },
            ) from exc

    @staticmethod
    def _should_try_portfolio_margin(exc: BinanceAPIError) -> bool:
        return exc.code in {-2015, -2014} or exc.status in {401, 403, 404}

    async def _sync_server_time(self) -> int:
        payload = await self.server_time()
        server_time = payload.get("serverTime")
        if not isinstance(server_time, int):
            raise RuntimeError(f"Unexpected Binance server time payload: {payload}")
        self._time_offset_ms = int(server_time - int(time.time() * 1000))
        return self._time_offset_ms

    async def _decode_response(self, response: aiohttp.ClientResponse) -> dict[str, Any]:
        text = await response.text()
        try:
            payload = json.loads(text) if text else {}
        except json.JSONDecodeError as exc:
            raise BinanceAPIError(f"Binance returned non-JSON payload: {text[:400]}", status=response.status) from exc

        if response.status >= 400:
            raise BinanceAPIError(f"HTTP {response.status}: {payload}", status=response.status, payload=payload)
        if isinstance(payload, dict) and "code" in payload and isinstance(payload["code"], int) and payload["code"] < 0:
            raise BinanceAPIError(f"Binance error: {payload}", status=response.status, payload=payload)
        return payload

    @staticmethod
    def _normalize_params(params: dict[str, Any]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, bool):
                normalized[key] = "true" if value else "false"
            else:
                normalized[key] = str(value)
        return normalized

    @staticmethod
    def _normalize_classic_algo_payload(payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload)
        if normalized.get("algoType") in (None, ""):
            normalized["algoType"] = "CONDITIONAL"
        if normalized.get("triggerPrice") in (None, "") and normalized.get("stopPrice") not in (None, ""):
            normalized["triggerPrice"] = normalized.get("stopPrice")
        if normalized.get("activatePrice") in (None, "") and normalized.get("activationPrice") not in (None, ""):
            normalized["activatePrice"] = normalized.get("activationPrice")
        if normalized.get("priceRate") in (None, "") and normalized.get("callbackRate") not in (None, ""):
            normalized["priceRate"] = normalized.get("callbackRate")
        if normalized.get("clientAlgoId") in (None, "") and normalized.get("newClientStrategyId") not in (None, ""):
            normalized["clientAlgoId"] = normalized.get("newClientStrategyId")
        normalized.pop("stopPrice", None)
        normalized.pop("activationPrice", None)
        normalized.pop("callbackRate", None)
        normalized.pop("strategyId", None)
        normalized.pop("newClientStrategyId", None)
        return normalized

    @staticmethod
    def _normalize_classic_algo_response(payload: Any) -> Any:
        if isinstance(payload, list):
            return [BinanceFuturesClient._normalize_classic_algo_response(item) for item in payload]
        if not isinstance(payload, dict):
            return payload
        normalized = dict(payload)
        if normalized.get("strategyId") in (None, "") and normalized.get("algoId") not in (None, ""):
            normalized["strategyId"] = normalized.get("algoId")
        if normalized.get("newClientStrategyId") in (None, "") and normalized.get("clientAlgoId") not in (None, ""):
            normalized["newClientStrategyId"] = normalized.get("clientAlgoId")
        if normalized.get("stopPrice") in (None, "") and normalized.get("triggerPrice") not in (None, ""):
            normalized["stopPrice"] = normalized.get("triggerPrice")
        if normalized.get("activationPrice") in (None, "") and normalized.get("activatePrice") not in (None, ""):
            normalized["activationPrice"] = normalized.get("activatePrice")
        if normalized.get("callbackRate") in (None, "") and normalized.get("priceRate") not in (None, ""):
            normalized["callbackRate"] = normalized.get("priceRate")
        return normalized
