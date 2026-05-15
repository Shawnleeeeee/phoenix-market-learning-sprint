from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN
from typing import Any


EXCHANGE_FILTER_FIELD_NAMES = (
    "symbol_tradeable",
    "micro_notional_feasible",
    "exchange_filter_checked",
    "rounded_qty",
    "min_qty",
    "step_size",
    "min_notional",
    "required_quote_allocation_usdt",
    "configured_quote_allocation_usdt",
    "configured_leverage",
    "max_quote_allocation_usdt",
    "infeasible_reason",
    "price_precision",
    "quantity_precision",
    "market_lot_min_qty",
    "market_lot_step_size",
)


@dataclass(frozen=True, slots=True)
class ParsedExchangeFilters:
    symbol: str
    symbol_tradeable: bool
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float
    trigger_protect: float | None = None
    price_precision: int | None = None
    quantity_precision: int | None = None
    market_lot_min_qty: float | None = None
    market_lot_step_size: float | None = None
    status: str | None = None
    contract_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MicroNotionalFeasibility:
    symbol: str
    symbol_tradeable: bool
    micro_notional_feasible: bool
    exchange_filter_checked: bool
    rounded_qty: float | None
    min_qty: float | None
    step_size: float | None
    min_notional: float | None
    required_quote_allocation_usdt: float | None
    configured_quote_allocation_usdt: float | None
    configured_leverage: int | None
    max_quote_allocation_usdt: float | None
    infeasible_reason: str | None
    price_precision: int | None = None
    quantity_precision: int | None = None
    market_lot_min_qty: float | None = None
    market_lot_step_size: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_symbol_exchange_filters(symbol_info: dict[str, Any]) -> ParsedExchangeFilters:
    symbol = str(symbol_info.get("symbol") or "UNKNOWN").upper()
    status = str(symbol_info.get("status") or "").upper() or None
    contract_type = str(symbol_info.get("contractType") or "").upper() or None
    filters = symbol_info.get("filters") if isinstance(symbol_info.get("filters"), list) else []
    by_type = {str(item.get("filterType") or "").upper(): item for item in filters if isinstance(item, dict)}

    price_filter = by_type.get("PRICE_FILTER") or {}
    lot_filter = by_type.get("LOT_SIZE") or {}
    market_lot_filter = by_type.get("MARKET_LOT_SIZE") or {}
    notional_filter = by_type.get("NOTIONAL") or by_type.get("MIN_NOTIONAL") or {}

    lot_step = _decimal(lot_filter.get("stepSize"))
    lot_min_qty = _decimal(lot_filter.get("minQty"))
    market_step = _decimal(market_lot_filter.get("stepSize"))
    market_min_qty = _decimal(market_lot_filter.get("minQty"))
    effective_step = market_step if market_step is not None and market_step > 0 else lot_step
    effective_min_qty = max(
        [value for value in (lot_min_qty, market_min_qty) if value is not None],
        default=Decimal("0"),
    )
    min_notional = (
        _decimal(notional_filter.get("minNotional"))
        or _decimal(notional_filter.get("notional"))
        or Decimal("0")
    )
    tick_size = _decimal(price_filter.get("tickSize")) or Decimal("0")
    tradeable = status in {None, "", "TRADING"} and contract_type in {None, "", "PERPETUAL"}

    return ParsedExchangeFilters(
        symbol=symbol,
        symbol_tradeable=bool(tradeable),
        tick_size=_to_float(tick_size) or 0.0,
        step_size=_to_float(effective_step) or 0.0,
        min_qty=_to_float(effective_min_qty) or 0.0,
        min_notional=_to_float(min_notional) or 0.0,
        trigger_protect=_to_float(_decimal(symbol_info.get("triggerProtect"))),
        price_precision=_optional_int(symbol_info.get("pricePrecision")),
        quantity_precision=_optional_int(symbol_info.get("quantityPrecision")),
        market_lot_min_qty=_to_float(market_min_qty),
        market_lot_step_size=_to_float(market_step),
        status=status,
        contract_type=contract_type,
    )


def find_symbol_info(exchange_info: dict[str, Any] | None, symbol: str) -> dict[str, Any] | None:
    wanted = str(symbol or "").upper()
    symbols = exchange_info.get("symbols") if isinstance(exchange_info, dict) else None
    if not isinstance(symbols, list):
        return None
    for item in symbols:
        if isinstance(item, dict) and str(item.get("symbol") or "").upper() == wanted:
            return item
    return None


def evaluate_micro_notional_feasibility(
    symbol_info: dict[str, Any] | None,
    *,
    current_price: Any,
    configured_quote_allocation_usdt: Any,
    configured_leverage: Any,
    max_quote_allocation_usdt: Any,
    symbol: str | None = None,
) -> MicroNotionalFeasibility:
    resolved_symbol = str(symbol or (symbol_info or {}).get("symbol") or "UNKNOWN").upper()
    quote = _decimal(configured_quote_allocation_usdt)
    leverage_decimal = _decimal(configured_leverage)
    leverage_int = _optional_int(configured_leverage)
    max_quote = _decimal(max_quote_allocation_usdt)
    price = _decimal(current_price)

    if symbol_info is None:
        return _unchecked_result(
            resolved_symbol,
            quote,
            leverage_int,
            max_quote,
            "exchange_info_symbol_missing",
        )
    parsed = parse_symbol_exchange_filters(symbol_info)
    resolved_symbol = parsed.symbol
    if quote is None or quote <= 0:
        return _infeasible_result(parsed, quote, leverage_int, max_quote, "configured_quote_allocation_invalid")
    if leverage_decimal is None or leverage_decimal <= 0:
        return _infeasible_result(parsed, quote, leverage_int, max_quote, "configured_leverage_invalid")
    if price is None or price <= 0:
        return _infeasible_result(parsed, quote, leverage_int, max_quote, "price_unavailable")
    step = _decimal(parsed.step_size)
    min_qty = _decimal(parsed.min_qty) or Decimal("0")
    min_notional = _decimal(parsed.min_notional) or Decimal("0")
    if step is None or step <= 0:
        return _infeasible_result(parsed, quote, leverage_int, max_quote, "step_size_unavailable")

    notional = quote * leverage_decimal
    raw_qty = notional / price
    rounded_qty = _round_down_to_step(raw_qty, step)
    required_qty = max(min_qty, (min_notional / price) if min_notional > 0 else Decimal("0"))
    required_qty = _round_up_to_step(required_qty, step)
    required_quote = (required_qty * price / leverage_decimal) if leverage_decimal > 0 else None
    rounded_notional = rounded_qty * price

    reasons: list[str] = []
    if not parsed.symbol_tradeable:
        reasons.append("symbol_not_tradeable")
    if rounded_qty < min_qty:
        reasons.append("min_qty_not_met")
    if rounded_notional < min_notional:
        reasons.append("min_notional_not_met")
    if required_quote is not None and max_quote is not None and required_quote > max_quote:
        reasons.append("required_quote_allocation_exceeds_max")

    return MicroNotionalFeasibility(
        symbol=resolved_symbol,
        symbol_tradeable=parsed.symbol_tradeable,
        micro_notional_feasible=not reasons,
        exchange_filter_checked=True,
        rounded_qty=_to_float(rounded_qty),
        min_qty=parsed.min_qty,
        step_size=parsed.step_size,
        min_notional=parsed.min_notional,
        required_quote_allocation_usdt=_to_float(required_quote),
        configured_quote_allocation_usdt=_to_float(quote),
        configured_leverage=leverage_int,
        max_quote_allocation_usdt=_to_float(max_quote),
        infeasible_reason=",".join(reasons) if reasons else None,
        price_precision=parsed.price_precision,
        quantity_precision=parsed.quantity_precision,
        market_lot_min_qty=parsed.market_lot_min_qty,
        market_lot_step_size=parsed.market_lot_step_size,
    )


def enrich_candidates_with_exchange_filters(
    candidates: list[dict[str, Any]],
    exchange_info: dict[str, Any] | None,
    *,
    configured_quote_allocation_usdt: Any,
    configured_leverage: Any,
    max_quote_allocation_usdt: Any,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for candidate in candidates:
        item = dict(candidate)
        symbol = str(item.get("symbol") or "").upper()
        symbol_info = find_symbol_info(exchange_info, symbol)
        result = evaluate_micro_notional_feasibility(
            symbol_info,
            current_price=item.get("current_price") or item.get("entry_price_hint") or item.get("mark_price"),
            configured_quote_allocation_usdt=configured_quote_allocation_usdt,
            configured_leverage=configured_leverage,
            max_quote_allocation_usdt=max_quote_allocation_usdt,
            symbol=symbol,
        )
        item.update(result.to_dict())
        enriched.append(item)
    return enriched


def enrich_snapshot_with_exchange_filters(
    snapshot: dict[str, Any],
    exchange_info: dict[str, Any] | None,
    *,
    configured_quote_allocation_usdt: Any,
    configured_leverage: Any,
    max_quote_allocation_usdt: Any,
) -> dict[str, Any]:
    updated = dict(snapshot)
    candidates = [dict(item) for item in snapshot.get("top_candidates") or [] if isinstance(item, dict)]
    updated["top_candidates"] = enrich_candidates_with_exchange_filters(
        candidates,
        exchange_info,
        configured_quote_allocation_usdt=configured_quote_allocation_usdt,
        configured_leverage=configured_leverage,
        max_quote_allocation_usdt=max_quote_allocation_usdt,
    )
    status = dict(snapshot.get("system_status") or {})
    checked = sum(1 for item in updated["top_candidates"] if item.get("exchange_filter_checked") is True)
    feasible = sum(1 for item in updated["top_candidates"] if item.get("micro_notional_feasible") is True)
    status.update(
        {
            "exchange_filter_checked": bool(updated["top_candidates"]) and checked == len(updated["top_candidates"]),
            "exchange_filter_checked_count": checked,
            "micro_notional_feasible_count": feasible,
            "micro_notional_infeasible_count": max(0, len(updated["top_candidates"]) - feasible),
        }
    )
    updated["system_status"] = status
    updated["top_candidates_count"] = len(updated["top_candidates"])
    return updated


def exchange_filter_fields(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return {key: payload.get(key) for key in EXCHANGE_FILTER_FIELD_NAMES if key in payload}


def _unchecked_result(
    symbol: str,
    quote: Decimal | None,
    leverage: int | None,
    max_quote: Decimal | None,
    reason: str,
) -> MicroNotionalFeasibility:
    return MicroNotionalFeasibility(
        symbol=str(symbol or "UNKNOWN").upper(),
        symbol_tradeable=False,
        micro_notional_feasible=False,
        exchange_filter_checked=False,
        rounded_qty=None,
        min_qty=None,
        step_size=None,
        min_notional=None,
        required_quote_allocation_usdt=None,
        configured_quote_allocation_usdt=_to_float(quote),
        configured_leverage=leverage,
        max_quote_allocation_usdt=_to_float(max_quote),
        infeasible_reason=reason,
    )


def _infeasible_result(
    parsed: ParsedExchangeFilters,
    quote: Decimal | None,
    leverage: int | None,
    max_quote: Decimal | None,
    reason: str,
) -> MicroNotionalFeasibility:
    return MicroNotionalFeasibility(
        symbol=parsed.symbol,
        symbol_tradeable=parsed.symbol_tradeable,
        micro_notional_feasible=False,
        exchange_filter_checked=True,
        rounded_qty=None,
        min_qty=parsed.min_qty,
        step_size=parsed.step_size,
        min_notional=parsed.min_notional,
        required_quote_allocation_usdt=None,
        configured_quote_allocation_usdt=_to_float(quote),
        configured_leverage=leverage,
        max_quote_allocation_usdt=_to_float(max_quote),
        infeasible_reason=reason,
        price_precision=parsed.price_precision,
        quantity_precision=parsed.quantity_precision,
        market_lot_min_qty=parsed.market_lot_min_qty,
        market_lot_step_size=parsed.market_lot_step_size,
    )


def _round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _round_up_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
