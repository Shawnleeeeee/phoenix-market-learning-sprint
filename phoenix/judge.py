from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from phoenix.binance_web3 import BinanceWeb3Client
from phoenix.models import CandidateSignal


FAKE_HEAT_KEYWORDS = (
    "giveaway",
    "airdrop",
    "lottery",
    "raffle",
    "sweepstakes",
    "红包",
    "抽奖",
    "空投",
    "领空投",
)


@dataclass(slots=True)
class JudgeConfig:
    top_n: int = 20
    audit_top_n: int = 20
    minimum_score: float = 0.0
    fake_heat_keywords: tuple[str, ...] = field(default_factory=lambda: FAKE_HEAT_KEYWORDS)


class PhoenixJudge:
    def __init__(self, web3_client: BinanceWeb3Client, config: JudgeConfig | None = None) -> None:
        self.web3_client = web3_client
        self.config = config or JudgeConfig()

    async def evaluate_snapshot(self, snapshot_path: Path) -> dict[str, Any]:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        return await self.evaluate_universe(snapshot)

    async def evaluate_universe(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        universe = snapshot.get("symbols", [])
        by_chain: dict[str, list[dict[str, Any]]] = {}
        for item in universe:
            by_chain.setdefault(str(item["chain_id"]), []).append(item)

        social_tasks = {
            chain_id: asyncio.create_task(self.web3_client.fetch_social_hype(chain_id))
            for chain_id in by_chain
        }
        smart_money_tasks = {
            chain_id: asyncio.create_task(self.web3_client.fetch_smart_money_signals(chain_id))
            for chain_id in by_chain
        }
        topic_tasks = {
            chain_id: asyncio.create_task(self.web3_client.fetch_topic_rush(chain_id))
            for chain_id in by_chain
        }
        square_trending_tasks = {
            chain_id: asyncio.create_task(self.web3_client.fetch_unified_rank(10, chain_id=chain_id))
            for chain_id in by_chain
        }
        square_search_tasks = {
            chain_id: asyncio.create_task(self.web3_client.fetch_unified_rank(11, chain_id=chain_id))
            for chain_id in by_chain
        }

        social_by_chain = {chain: await task for chain, task in social_tasks.items()}
        smart_money_by_chain = {chain: await task for chain, task in smart_money_tasks.items()}
        topic_by_chain = {chain: await task for chain, task in topic_tasks.items()}
        square_trending_by_chain = {chain: await task for chain, task in square_trending_tasks.items()}
        square_search_by_chain = {chain: await task for chain, task in square_search_tasks.items()}

        candidates: list[CandidateSignal] = []
        for item in universe:
            candidate = self._score_item(
                item,
                social_by_chain.get(item["chain_id"], []),
                smart_money_by_chain.get(item["chain_id"], []),
                topic_by_chain.get(item["chain_id"], []),
                square_trending_by_chain.get(item["chain_id"], []),
                square_search_by_chain.get(item["chain_id"], []),
            )
            if candidate.score >= self.config.minimum_score or candidate.blocked_reasons:
                candidates.append(candidate)

        candidates.sort(key=lambda item: item.score, reverse=True)
        audit_window = max(self.config.audit_top_n, self.config.top_n)
        await self._apply_audit(candidates[:audit_window])
        candidates.sort(key=lambda item: item.score, reverse=True)

        return {
            "generated_at": snapshot.get("generated_at"),
            "universe_size": snapshot.get("universe_size", len(universe)),
            "candidate_count": len(candidates),
            "top_candidates": [item.to_dict() for item in candidates[: self.config.top_n]],
        }

    def _score_item(
        self,
        item: dict[str, Any],
        social_items: list[dict[str, Any]],
        smart_money_items: list[dict[str, Any]],
        topic_items: list[dict[str, Any]],
        square_trending_items: list[dict[str, Any]],
        square_search_items: list[dict[str, Any]],
    ) -> CandidateSignal:
        alpha_symbol = str(item["alpha_symbol"]).upper()
        symbol = str(item["symbol"]).upper()
        chain_id = str(item["chain_id"])
        now_ms = int(time.time() * 1000)

        social_index = {
            str(entry["metaInfo"]["symbol"]).upper(): (rank, entry)
            for rank, entry in enumerate(social_items, start=1)
        }
        social_match = social_index.get(alpha_symbol)
        square_trending_index = {
            str(entry.get("symbol") or "").upper(): (rank, entry)
            for rank, entry in enumerate(square_trending_items, start=1)
        }
        square_search_index = {
            str(entry.get("symbol") or "").upper(): (rank, entry)
            for rank, entry in enumerate(square_search_items, start=1)
        }
        square_trending_match = square_trending_index.get(alpha_symbol)
        square_search_match = square_search_index.get(alpha_symbol)

        smart_money_match = [entry for entry in smart_money_items if str(entry.get("ticker", "")).upper() == alpha_symbol]
        topic_match = []
        for topic in topic_items:
            for token in topic.get("tokenList", []):
                if str(token.get("symbol", "")).upper() == alpha_symbol:
                    topic_match.append((topic, token))
                    break

        breakdown: dict[str, float] = {}
        blocked: list[str] = []

        oi_delta_1m_pct = self._float(item.get("oi_delta_1m_pct"))
        oi_delta_5m_pct = self._float(item.get("oi_delta_5m_pct"))
        oi_delta_15m_pct = self._float(item.get("oi_delta_15m_pct"))
        price_change_1m_pct = self._float(item.get("price_change_1m_pct"))
        price_change_5m_pct = self._float(item.get("price_change_5m_pct"))
        price_change_15m_pct = self._float(item.get("price_change_15m_pct"))
        oi_delta_pct = oi_delta_5m_pct
        if oi_delta_pct is None:
            oi_delta_pct = self._float(item.get("oi_delta_pct"))
        if oi_delta_1m_pct is not None:
            breakdown["oi_1m"] = self._scaled_positive_or_negative(oi_delta_1m_pct, positive_scale=8.0, negative_scale=3.5, cap=5.0, floor=-4.0)
        if oi_delta_5m_pct is not None:
            breakdown["oi_5m"] = self._scaled_positive_or_negative(oi_delta_5m_pct, positive_scale=10.0, negative_scale=4.5, cap=9.0, floor=-6.0)
        if oi_delta_15m_pct is not None:
            breakdown["oi_15m"] = self._scaled_positive_or_negative(oi_delta_15m_pct, positive_scale=6.0, negative_scale=3.0, cap=7.0, floor=-5.0)
        if (oi_delta_5m_pct is not None and oi_delta_5m_pct < -0.75) or (
            oi_delta_15m_pct is not None and oi_delta_15m_pct < -1.25
        ):
            blocked.append("open_interest_contracting")

        volume_ratio = self._float(item.get("volume_5m_ratio"))
        volume_ratio_short = self._float(item.get("volume_5m_ratio_short"))
        volume_ratio_medium = self._float(item.get("volume_5m_ratio_medium"))
        volume_ratio_long = self._float(item.get("volume_5m_ratio_long"))
        spread_bps = self._float(item.get("spread_bps"))
        depth_bid_5 = self._float(item.get("depth_bid_5"))
        depth_ask_5 = self._float(item.get("depth_ask_5"))
        depth_imbalance = self._float(item.get("depth_imbalance"))
        estimated_slippage_bps = self._float(item.get("estimated_slippage_bps"))
        estimated_slippage_for_order_usdt = self._float(item.get("estimated_slippage_for_order_usdt"))
        funding_rate = self._float(item.get("funding_rate"))
        next_funding_time_ms = self._int(item.get("next_funding_time_ms"))
        premium_index = self._float(item.get("premium_index"))
        mark_index_basis_pct = self._float(item.get("mark_index_basis_pct"))
        onboard_date_ms = self._int(item.get("onboard_date_ms"))
        taker_buy_ratio_1m = self._float(item.get("taker_buy_ratio_1m"))
        taker_buy_ratio_5m = self._float(item.get("taker_buy_ratio_5m"))
        aggressive_flow_delta = self._float(item.get("aggressive_flow_delta"))
        taker_buy_volume_5m = self._float(item.get("taker_buy_volume_5m"))
        taker_sell_volume_5m = self._float(item.get("taker_sell_volume_5m"))
        liquidation_long_usd = self._float(item.get("liquidation_long_usd"))
        liquidation_short_usd = self._float(item.get("liquidation_short_usd"))
        liquidation_event_count = self._int(item.get("liquidation_event_count"))
        listing_age_hours = None
        if onboard_date_ms is not None:
            listing_age_hours = max(0.0, (now_ms - onboard_date_ms) / 3_600_000.0)
        if volume_ratio_short is not None:
            breakdown["volume_short"] = self._scaled_volume_ratio(volume_ratio_short, cap=4.0)
        if volume_ratio_medium is not None:
            breakdown["volume_medium"] = self._scaled_volume_ratio(volume_ratio_medium, cap=6.0)
        if volume_ratio_long is not None:
            breakdown["volume_long"] = self._scaled_volume_ratio(volume_ratio_long, cap=5.0)
        if volume_ratio is None:
            volume_ratio = volume_ratio_medium or volume_ratio_short or volume_ratio_long

        if social_match is not None:
            rank, payload = social_match
            hype = self._float(payload["socialHypeInfo"].get("socialHype")) or 0.0
            social_change_pct = self._float(payload["socialHypeInfo"].get("socialHypeChangePercentByTimeRange")) or 0.0
            sentiment = str(payload["socialHypeInfo"].get("sentiment") or "Neutral")
            summary = self._pick_social_summary(payload["socialHypeInfo"])
            social_freshness = math.tanh(social_change_pct / 120.0)
            breakdown["social_rank"] = max(2.0, 12.0 - ((rank - 1) * 0.8))
            breakdown["social_hype"] = min(6.0, max(0.0, math.log10(max(hype, 1.0)) - 4.8) * 2.2)
            breakdown["social_freshness"] = max(-4.5, min(4.5, social_freshness * 4.5))
            breakdown["sentiment"] = {"Positive": 4.0, "Neutral": 1.0, "Negative": -4.0}.get(sentiment, 0.0)
            if self._contains_fake_heat(summary):
                blocked.append("fake_heat_social_campaign")
                breakdown["fake_heat_penalty"] = -20.0

        square_discovery_score = 0.0
        if square_trending_match is not None:
            rank, _payload = square_trending_match
            contribution = max(0.0, 7.0 - ((rank - 1) * 0.55))
            breakdown["square_trending"] = contribution
            square_discovery_score += contribution
        if square_search_match is not None:
            rank, _payload = square_search_match
            contribution = max(0.0, 6.0 - ((rank - 1) * 0.45))
            breakdown["square_search"] = contribution
            square_discovery_score += contribution

        smart_money_latest_age_minutes = None
        smart_money_weighted_buy_strength = 0.0
        smart_money_weighted_sell_strength = 0.0
        if smart_money_match:
            weighted_buy = 0.0
            weighted_sell = 0.0
            weighted_active = 0.0
            weighted_traders = 0.0
            timeout_or_exit_penalty = 0.0
            ages: list[float] = []
            for entry in smart_money_match:
                age_minutes = self._age_minutes(now_ms, self._signal_timestamp(entry, "signalTriggerTime", "highestPriceTime"))
                freshness = self._exp_decay(age_minutes, half_life_minutes=240.0, floor=0.12)
                status = str(entry.get("status") or "").lower()
                status_multiplier = {
                    "active": 1.0,
                    "valid": 1.0,
                    "timeout": 0.55,
                    "closed": 0.2,
                    "finished": 0.2,
                }.get(status, 0.35)
                exit_rate = self._float(entry.get("exitRate")) or 0.0
                if exit_rate >= 80:
                    status_multiplier *= 0.35
                    timeout_or_exit_penalty -= 2.5
                elif exit_rate >= 60:
                    status_multiplier *= 0.6
                    timeout_or_exit_penalty -= 1.0
                strength = freshness * status_multiplier
                direction = str(entry.get("direction") or "").lower()
                if direction == "buy":
                    weighted_buy += strength
                elif direction == "sell":
                    weighted_sell += strength
                if status in {"active", "valid"}:
                    weighted_active += strength
                weighted_traders += min(8.0, float(int(entry.get("smartMoneyCount") or 0))) * strength
                if age_minutes is not None:
                    ages.append(age_minutes)

            smart_money_latest_age_minutes = min(ages) if ages else None
            smart_money_weighted_buy_strength = round(weighted_buy, 4)
            smart_money_weighted_sell_strength = round(weighted_sell, 4)
            breakdown["smart_money"] = min(16.0, weighted_buy * 5.0 + weighted_active * 2.0 + weighted_traders * 0.45)
            if timeout_or_exit_penalty:
                breakdown["smart_money_decay_penalty"] = timeout_or_exit_penalty
            if smart_money_latest_age_minutes is not None and smart_money_latest_age_minutes > 360:
                breakdown["smart_money_stale_penalty"] = -4.0
            if weighted_sell > weighted_buy * 1.1:
                breakdown["smart_money_sell_activity"] = min(10.0, weighted_sell * 4.0)
            if any(
                str(tag.get("tagName") or "").lower() == "dex paid"
                for entry in smart_money_match
                for tags in entry.get("tokenTag", {}).values()
                for tag in tags
            ):
                breakdown["promotion_penalty"] = -6.0
                blocked.append("paid_promotion_signal")

        topic_latest_age_minutes = None
        topic_freshness = None
        if topic_match:
            best_topic, best_token = max(topic_match, key=lambda pair: self._topic_priority(now_ms, pair))
            topic_inflow = self._float(best_token.get("netInflow")) or 0.0
            topic_latest_age_minutes = self._age_minutes(
                now_ms,
                self._signal_timestamp(best_topic, "viralTime", "risingTime", "createTime"),
            )
            topic_freshness = self._exp_decay(topic_latest_age_minutes, half_life_minutes=360.0, floor=0.12)
            breakdown["topic_inflow"] = min(
                12.0,
                max(0.0, math.log10(max(topic_inflow, 1.0)) - 2.0) * 4.0 * topic_freshness,
            )
            if topic_latest_age_minutes is not None and topic_latest_age_minutes > 720:
                breakdown["topic_stale_penalty"] = -3.5
            summary = self._pick_topic_summary(best_topic)
            if self._contains_fake_heat(summary):
                blocked.append("fake_heat_topic_campaign")
                breakdown["topic_fake_heat_penalty"] = -15.0

        has_oi_confirmation = any(
            value is not None and value >= threshold
            for value, threshold in (
                (oi_delta_1m_pct, 0.1),
                (oi_delta_5m_pct, 0.2),
                (oi_delta_15m_pct, 0.35),
            )
        )
        has_volume_confirmation = any(
            value is not None and value >= threshold
            for value, threshold in (
                (volume_ratio_short, 1.6),
                (volume_ratio_medium, 1.3),
                (volume_ratio_long, 1.2),
            )
        )
        has_smart_money_confirmation = bool(smart_money_match) and smart_money_weighted_buy_strength >= 0.8
        has_topic_confirmation = bool(topic_match) and (topic_freshness or 0.0) >= 0.18
        has_external_confirmation = has_smart_money_confirmation or has_topic_confirmation
        has_market_confirmation = has_oi_confirmation or has_volume_confirmation

        if social_match is not None and not has_market_confirmation:
            if not has_external_confirmation:
                breakdown["social_only_penalty"] = -8.0
                blocked.append("unconfirmed_social_only_signal")
            else:
                breakdown["social_unconfirmed_penalty"] = -4.0

        sentiment_value = breakdown.get("sentiment", 0.0)
        if sentiment_value < 0 and not has_market_confirmation and not has_external_confirmation:
            breakdown["negative_unconfirmed_penalty"] = -4.0

        execution_quality_raw = 5.0
        if spread_bps is not None:
            if spread_bps <= 6:
                execution_quality_raw += 2.0
            elif spread_bps <= 12:
                execution_quality_raw += 1.0
            elif spread_bps >= 40:
                execution_quality_raw -= 2.0
            elif spread_bps >= 20:
                execution_quality_raw -= 1.0
        if estimated_slippage_bps is not None:
            if estimated_slippage_bps <= 8:
                execution_quality_raw += 2.0
            elif estimated_slippage_bps <= 15:
                execution_quality_raw += 1.0
            elif estimated_slippage_bps >= 60:
                execution_quality_raw -= 2.5
            elif estimated_slippage_bps >= 30:
                execution_quality_raw -= 1.25
        total_depth_notional = (depth_bid_5 or 0.0) + (depth_ask_5 or 0.0)
        if total_depth_notional > 0:
            if total_depth_notional >= 50_000:
                execution_quality_raw += 1.5
            elif total_depth_notional >= 20_000:
                execution_quality_raw += 0.8
            elif total_depth_notional <= 5_000:
                execution_quality_raw -= 1.25
        if depth_imbalance is not None and abs(depth_imbalance) >= 0.65:
            execution_quality_raw -= 0.5
        execution_quality_score = round(max(0.0, min(10.0, execution_quality_raw)), 4)

        score = round(sum(breakdown.values()), 4)
        directional_breakdown: dict[str, float] = {}
        if social_match is not None:
            sentiment = str(payload["socialHypeInfo"].get("sentiment") or "Neutral")
            directional_breakdown["sentiment"] = {"Positive": 2.0, "Neutral": 0.0, "Negative": -2.0}.get(sentiment, 0.0)
        smart_money_net_strength = smart_money_weighted_buy_strength - smart_money_weighted_sell_strength
        if smart_money_net_strength:
            directional_breakdown["smart_money_flow"] = max(-6.0, min(6.0, smart_money_net_strength * 4.0))
        if topic_match:
            topic_flow = self._float(best_token.get("netInflow")) or 0.0
            if topic_flow:
                directional_breakdown["topic_flow"] = max(
                    -2.5,
                    min(2.5, math.copysign(max(0.0, math.log10(abs(topic_flow) + 1.0) - 2.0), topic_flow)),
                )
        price_direction_score = 0.0
        for value, weight, scale in (
            (price_change_1m_pct, 0.8, 0.55),
            (price_change_5m_pct, 1.2, 0.45),
            (price_change_15m_pct, 1.5, 0.35),
        ):
            if value is None:
                continue
            price_direction_score += math.copysign(min(weight, abs(value) * scale), value)
        if price_direction_score:
            directional_breakdown["price_trend"] = max(-4.5, min(4.5, price_direction_score))
        market_confirmation_strength = 0.0
        for value, threshold, weight, magnitude_scale in (
            (oi_delta_1m_pct, 0.1, 0.6, 0.25),
            (oi_delta_5m_pct, 0.2, 1.0, 0.25),
            (oi_delta_15m_pct, 0.35, 1.2, 0.2),
            (volume_ratio_short, 1.2, 0.4, 0.1),
            (volume_ratio_medium, 1.15, 0.6, 0.1),
            (volume_ratio_long, 1.1, 0.8, 0.1),
        ):
            if value is None:
                continue
            magnitude = abs(float(value))
            if value >= threshold:
                market_confirmation_strength += weight + min(1.0, magnitude * magnitude_scale)
        alignment_score = 0.0
        for price_value, oi_value, weight in (
            (price_change_1m_pct, oi_delta_1m_pct, 0.7),
            (price_change_5m_pct, oi_delta_5m_pct, 1.2),
            (price_change_15m_pct, oi_delta_15m_pct, 1.4),
        ):
            if price_value is None or oi_value is None:
                continue
            if abs(price_value) < 0.08 or abs(oi_value) < 0.1:
                continue
            if math.copysign(1.0, price_value) == math.copysign(1.0, oi_value):
                alignment_score += math.copysign(
                    min(weight, abs(price_value) * 0.45 + abs(oi_value) * 0.2),
                    price_value,
                )
        for price_value, volume_value, weight in (
            (price_change_1m_pct, volume_ratio_short, 0.5),
            (price_change_5m_pct, volume_ratio_medium, 0.9),
            (price_change_15m_pct, volume_ratio_long, 1.0),
        ):
            if price_value is None or volume_value is None:
                continue
            if abs(price_value) < 0.08 or volume_value < 1.1:
                continue
            alignment_score += math.copysign(
                min(weight, abs(price_value) * 0.4 + max(0.0, volume_value - 1.0) * 0.35),
                price_value,
            )
        if alignment_score:
            directional_breakdown["market_direction"] = max(-5.0, min(5.0, alignment_score))
        taker_flow_signal = 0.0
        if aggressive_flow_delta is not None:
            taker_flow_signal += max(-2.5, min(2.5, aggressive_flow_delta * 6.0))
        elif taker_buy_ratio_5m is not None:
            taker_flow_signal += max(-2.0, min(2.0, (taker_buy_ratio_5m - 1.0) * 5.0))
        if taker_flow_signal:
            directional_breakdown["taker_flow"] = taker_flow_signal
        liquidation_signal = 0.0
        total_liquidation = (liquidation_long_usd or 0.0) + (liquidation_short_usd or 0.0)
        liquidation_imbalance = None
        if total_liquidation > 0:
            liquidation_imbalance = ((liquidation_short_usd or 0.0) - (liquidation_long_usd or 0.0)) / total_liquidation
            if abs(liquidation_imbalance) >= 0.2:
                liquidation_signal = max(-1.8, min(1.8, liquidation_imbalance * 2.5))
        if liquidation_signal:
            directional_breakdown["liquidation_flow"] = liquidation_signal
        market_confirmation_raw = market_confirmation_strength
        if abs(alignment_score) > 0:
            market_confirmation_raw += min(3.5, abs(alignment_score) * 0.7)
        if oi_delta_15m_pct is not None and abs(oi_delta_15m_pct) >= 0.35:
            market_confirmation_raw += 1.0
        if volume_ratio_long is not None and volume_ratio_long >= 1.2:
            market_confirmation_raw += 0.75
        if smart_money_weighted_buy_strength >= 0.8:
            market_confirmation_raw += 0.75
        if abs(taker_flow_signal) >= 0.4:
            market_confirmation_raw += min(1.5, abs(taker_flow_signal) * 0.6)
        if total_liquidation >= 50_000:
            market_confirmation_raw += 0.75
        market_confirmation_score = round(max(0.0, min(10.0, market_confirmation_raw)), 4)
        base_directional_score = sum(directional_breakdown.values())
        if base_directional_score > 0 and market_confirmation_strength > 0:
            directional_breakdown["market_alignment"] = min(3.5, market_confirmation_strength) * min(1.0, base_directional_score / 4.0)
        elif base_directional_score < 0 and market_confirmation_strength > 0:
            directional_breakdown["market_alignment"] = -min(3.5, market_confirmation_strength) * min(1.0, abs(base_directional_score) / 4.0)
        if funding_rate is not None:
            directional_breakdown["funding_skew"] = max(-1.5, min(1.5, -(funding_rate * 4000.0)))
        if mark_index_basis_pct is not None:
            directional_breakdown["basis_skew"] = max(-1.2, min(1.2, -(mark_index_basis_pct * 3.0)))
        directional_score = round(sum(directional_breakdown.values()), 4)
        directional_bias = "NONE"
        if directional_score >= 2.5:
            directional_bias = "LONG"
        elif directional_score <= -2.5:
            directional_bias = "SHORT"
        directional_conflicts: list[str] = []
        if price_direction_score >= 0.75 and smart_money_net_strength <= -0.25:
            directional_conflicts.append("价格上行但聪明钱偏卖")
        if price_direction_score <= -0.75 and smart_money_net_strength >= 0.25:
            directional_conflicts.append("价格下行但聪明钱偏买")
        if price_direction_score >= 0.75 and sentiment_value <= -2.0:
            directional_conflicts.append("价格上行但社交情绪偏负面")
        if price_direction_score <= -0.75 and sentiment_value >= 2.0:
            directional_conflicts.append("价格下行但社交情绪偏正面")
        if price_direction_score >= 0.75 and taker_flow_signal <= -0.5:
            directional_conflicts.append("价格上行但主动卖盘更强")
        if price_direction_score <= -0.75 and taker_flow_signal >= 0.5:
            directional_conflicts.append("价格下行但主动买盘更强")
        if market_confirmation_score >= 4.5 and directional_bias == "NONE":
            directional_conflicts.append("市场确认存在但方向仍不明确")
        if funding_rate is not None and funding_rate >= 0.0008 and directional_bias == "LONG":
            directional_conflicts.append("多头资金费率偏拥挤")
        if funding_rate is not None and funding_rate <= -0.0008 and directional_bias == "SHORT":
            directional_conflicts.append("空头资金费率偏拥挤")
        if mark_index_basis_pct is not None and mark_index_basis_pct >= 0.18 and directional_bias == "LONG":
            directional_conflicts.append("正基差偏热，追多需谨慎")
        if mark_index_basis_pct is not None and mark_index_basis_pct <= -0.18 and directional_bias == "SHORT":
            directional_conflicts.append("负基差偏热，追空需谨慎")
        if total_liquidation >= 50_000 and liquidation_imbalance is not None and directional_bias == "NONE":
            directional_conflicts.append("清算密集但方向仍不明确")
        confidence = "LOW"
        if score >= 16:
            confidence = "HIGH"
        elif score >= 9:
            confidence = "MEDIUM"
        event_flags: list[str] = []
        if funding_rate is not None and abs(funding_rate) >= 0.0008:
            event_flags.append("extreme_funding_rate")
        if mark_index_basis_pct is not None and abs(mark_index_basis_pct) >= 0.18:
            event_flags.append("basis_dislocation")
        if estimated_slippage_bps is not None and estimated_slippage_bps >= 25:
            event_flags.append("slippage_risk")
        if spread_bps is not None and spread_bps >= 20:
            event_flags.append("wide_spread")
        if next_funding_time_ms is not None and 0 <= (next_funding_time_ms - now_ms) <= 30 * 60_000:
            event_flags.append("funding_window_soon")
        if listing_age_hours is not None and listing_age_hours <= 72:
            event_flags.append("recent_listing")
        if total_liquidation >= 50_000:
            event_flags.append("liquidation_cluster")
        if liquidation_imbalance is not None and liquidation_imbalance >= 0.45:
            event_flags.append("short_squeeze_risk")
        if liquidation_imbalance is not None and liquidation_imbalance <= -0.45:
            event_flags.append("long_flush_risk")
        event_risk_score = 0.0
        event_risk_score += len(set(blocked)) * 1.5
        event_risk_score += len(set(event_flags)) * 1.25
        if listing_age_hours is not None and listing_age_hours <= 24:
            event_risk_score += 1.5
        elif listing_age_hours is not None and listing_age_hours <= 72:
            event_risk_score += 0.75

        candidate = CandidateSignal(
            symbol=symbol,
            alpha_symbol=alpha_symbol,
            chain_id=chain_id,
            contract_address=str(item.get("contract_address") or ""),
            score=score,
            confidence=confidence,
            directional_bias=directional_bias,
            directional_score=directional_score,
            directional_breakdown=directional_breakdown,
            blocked_reasons=sorted(set(blocked)),
            score_breakdown=breakdown,
            mark_price=self._float(item.get("mark_price")),
            oi_delta_pct=oi_delta_pct,
            oi_delta_1m_pct=oi_delta_1m_pct,
            oi_delta_5m_pct=oi_delta_5m_pct,
            oi_delta_15m_pct=oi_delta_15m_pct,
            price_change_1m_pct=price_change_1m_pct,
            price_change_5m_pct=price_change_5m_pct,
            price_change_15m_pct=price_change_15m_pct,
            oi_notional_usd=self._float(item.get("oi_notional_usd")),
            volume_5m_ratio=volume_ratio,
            volume_5m_ratio_short=volume_ratio_short,
            volume_5m_ratio_medium=volume_ratio_medium,
            volume_5m_ratio_long=volume_ratio_long,
            volume_24h=self._float(item.get("volume_24h")),
            market_cap=self._float(item.get("market_cap")),
            spread_bps=spread_bps,
            depth_bid_5=depth_bid_5,
            depth_ask_5=depth_ask_5,
            depth_imbalance=depth_imbalance,
            estimated_slippage_bps=estimated_slippage_bps,
            estimated_slippage_for_order_usdt=estimated_slippage_for_order_usdt,
            funding_rate=funding_rate,
            next_funding_time_ms=next_funding_time_ms,
            premium_index=premium_index,
            mark_index_basis_pct=mark_index_basis_pct,
            onboard_date_ms=onboard_date_ms,
            listing_age_hours=listing_age_hours,
            taker_buy_ratio_1m=taker_buy_ratio_1m,
            taker_buy_ratio_5m=taker_buy_ratio_5m,
            aggressive_flow_delta=aggressive_flow_delta,
            taker_buy_volume_5m=taker_buy_volume_5m,
            taker_sell_volume_5m=taker_sell_volume_5m,
            liquidation_long_usd=liquidation_long_usd,
            liquidation_short_usd=liquidation_short_usd,
            liquidation_event_count=liquidation_event_count,
            squeeze_probability_hint=(
                round(max(0.0, min(1.0, abs(liquidation_imbalance or 0.0) * 1.4)), 4)
                if liquidation_imbalance is not None and total_liquidation >= 50_000
                else None
            ),
            market_confirmation_score=market_confirmation_score,
            execution_quality_score=execution_quality_score,
            event_risk_score=round(max(0.0, min(10.0, event_risk_score)), 4),
            directional_conflicts=directional_conflicts,
            event_flags=sorted(set(event_flags)),
        )

        if social_match is not None:
            rank, payload = social_match
            candidate.social_hype = self._float(payload["socialHypeInfo"].get("socialHype"))
            candidate.social_rank = rank
            candidate.social_hype_change_pct = self._float(payload["socialHypeInfo"].get("socialHypeChangePercentByTimeRange"))
            candidate.social_signal_freshness = breakdown.get("social_freshness")
            candidate.social_sentiment = payload["socialHypeInfo"].get("sentiment")
            candidate.social_summary = self._pick_social_summary(payload["socialHypeInfo"])
        if square_trending_match is not None:
            candidate.square_trending_rank = square_trending_match[0]
        if square_search_match is not None:
            candidate.square_search_rank = square_search_match[0]
        if square_discovery_score:
            candidate.square_discovery_score = round(square_discovery_score, 4)

        if smart_money_match:
            candidate.smart_money_buy_signals = sum(
                1 for entry in smart_money_match if str(entry.get("direction")).lower() == "buy"
            )
            candidate.smart_money_sell_signals = sum(
                1 for entry in smart_money_match if str(entry.get("direction")).lower() == "sell"
            )
            candidate.smart_money_active_signals = sum(
                1 for entry in smart_money_match if str(entry.get("status")).lower() == "active"
            )
            candidate.smart_money_traders = sum(int(entry.get("smartMoneyCount") or 0) for entry in smart_money_match)
            candidate.smart_money_latest_age_minutes = smart_money_latest_age_minutes
            candidate.smart_money_weighted_buy_strength = smart_money_weighted_buy_strength
            candidate.smart_money_tags = sorted(
                {
                    tag["tagName"]
                    for entry in smart_money_match
                    for tags in entry.get("tokenTag", {}).values()
                    for tag in tags
                    if tag.get("tagName")
                }
            )

        if topic_match:
            candidate.topic_names = [topic["name"].get("topicNameEn") or "" for topic, _ in topic_match if topic["name"].get("topicNameEn")]
            candidate.topic_net_inflow_usd = max(self._float(token.get("netInflow")) or 0.0 for _, token in topic_match)
            candidate.topic_net_inflow_1h_usd = max(self._float(token.get("netInflow1h")) or 0.0 for _, token in topic_match)
            candidate.topic_latest_age_minutes = topic_latest_age_minutes
            candidate.topic_freshness = topic_freshness

        return candidate

    async def _apply_audit(self, candidates: list[CandidateSignal]) -> None:
        tasks = [
            asyncio.create_task(self.web3_client.fetch_token_audit(item.chain_id, item.contract_address))
            for item in candidates
            if item.contract_address
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        result_iter = iter(results)
        for candidate in candidates:
            if not candidate.contract_address:
                continue
            result = next(result_iter)
            if isinstance(result, Exception):
                candidate.audit_flags.append(f"audit_error:{type(result).__name__}")
                continue
            data = result.get("data", {})
            if not data.get("hasResult") or not data.get("isSupported"):
                candidate.audit_flags.append("audit_unavailable")
                continue
            candidate.audit_risk_level = int(data.get("riskLevel") or 0)
            candidate.audit_risk_label = str(data.get("riskLevelEnum") or "")
            for category in data.get("riskItems", []):
                for detail in category.get("details", []):
                    if detail.get("isHit"):
                        candidate.audit_flags.append(str(detail.get("title") or category.get("id") or "risk"))
            if candidate.audit_risk_level >= 4:
                candidate.blocked_reasons.append("audit_high_risk")
                candidate.score_breakdown["audit_penalty"] = -25.0
            elif candidate.audit_risk_level >= 2:
                candidate.score_breakdown["audit_penalty"] = -10.0
            candidate.score = round(sum(candidate.score_breakdown.values()), 4)
            candidate.event_flags = sorted(set(candidate.event_flags + candidate.audit_flags))
            event_risk = (candidate.event_risk_score or 0.0) + min(6.0, float(candidate.audit_risk_level or 0) * 1.5)
            candidate.event_risk_score = round(max(0.0, min(10.0, event_risk)), 4)

    @staticmethod
    def _scaled_positive_or_negative(
        value: float,
        *,
        positive_scale: float,
        negative_scale: float,
        cap: float,
        floor: float,
    ) -> float:
        if value >= 0:
            return min(cap, value * positive_scale)
        return max(floor, value * negative_scale)

    @staticmethod
    def _scaled_volume_ratio(value: float, *, cap: float) -> float:
        return max(-2.0, min(cap, (value - 1.0) * 4.0))

    @staticmethod
    def _signal_timestamp(payload: dict[str, Any], *keys: str) -> int | None:
        for key in keys:
            value = payload.get(key)
            if value in (None, "", "null"):
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        return None

    @classmethod
    def _age_minutes(cls, now_ms: int, observed_ms: int | None) -> float | None:
        if observed_ms is None:
            return None
        return max(0.0, (now_ms - observed_ms) / 60_000.0)

    @staticmethod
    def _exp_decay(age_minutes: float | None, *, half_life_minutes: float, floor: float) -> float:
        if age_minutes is None:
            return floor
        return max(floor, math.exp(-math.log(2.0) * (age_minutes / half_life_minutes)))

    @classmethod
    def _topic_priority(cls, now_ms: int, pair: tuple[dict[str, Any], dict[str, Any]]) -> float:
        topic, token = pair
        inflow = cls._float(token.get("netInflow")) or 0.0
        observed_ms = cls._signal_timestamp(topic, "viralTime", "risingTime", "createTime")
        freshness = cls._exp_decay(cls._age_minutes(now_ms, observed_ms), half_life_minutes=360.0, floor=0.12)
        return inflow * freshness

    def _contains_fake_heat(self, text: str | None) -> bool:
        lowered = (text or "").lower()
        return any(keyword in lowered for keyword in self.config.fake_heat_keywords)

    @staticmethod
    def _pick_social_summary(payload: dict[str, Any]) -> str:
        return (
            payload.get("socialSummaryBriefTranslated")
            or payload.get("socialSummaryDetailTranslated")
            or payload.get("socialSummaryBrief")
            or payload.get("socialSummaryDetail")
            or ""
        )

    @staticmethod
    def _pick_topic_summary(payload: dict[str, Any]) -> str:
        ai_summary = payload.get("aiSummary", {})
        return ai_summary.get("aiSummaryEn") or ai_summary.get("aiSummaryCn") or ""

    @staticmethod
    def _float(value: Any) -> float | None:
        if value in (None, "", "null"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _int(value: Any) -> int | None:
        if value in (None, "", "null"):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
