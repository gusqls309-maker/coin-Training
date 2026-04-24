"""종목 선정 + 계좌 스냅샷 + 시장 메타 유틸."""
from __future__ import annotations

import logging

from .caches import ChanceCache, MarketInfoCache
from .client import UpbitClient, get_chance_cached
from .config import Config


def extract_market_warning(info: dict) -> str:
    value = info.get("market_warning", "")
    if value in (None, ""):
        return "NONE"
    return str(value).upper()


def is_bot_order(order: dict, prefix: str) -> bool:
    identifier = str(order.get("identifier", "") or "")
    return identifier.startswith(f"{prefix}-")


def accounts_to_map(accounts: list[dict]) -> dict[str, dict]:
    return {item["currency"]: item for item in accounts}


def get_position_snapshot(accounts: list[dict], market: str, current_price: float) -> dict:
    quote_currency, base_currency = market.split("-")
    amap = accounts_to_map(accounts)

    quote = amap.get(quote_currency, {"balance": "0", "locked": "0"})
    base = amap.get(base_currency, {"balance": "0", "locked": "0", "avg_buy_price": "0"})

    quote_balance = float(quote.get("balance", 0) or 0)
    quote_locked = float(quote.get("locked", 0) or 0)
    base_balance = float(base.get("balance", 0) or 0)
    base_locked = float(base.get("locked", 0) or 0)
    base_total = base_balance + base_locked
    avg_buy_price = float(base.get("avg_buy_price", 0) or 0)

    position_krw = base_total * current_price
    pnl_pct = 0.0
    if base_total > 0 and avg_buy_price > 0:
        pnl_pct = (current_price - avg_buy_price) / avg_buy_price

    return {
        "quote_balance": quote_balance,
        "quote_locked": quote_locked,
        "base_balance": base_balance,
        "base_locked": base_locked,
        "base_total": base_total,
        "avg_buy_price": avg_buy_price,
        "position_krw": position_krw,
        "pnl_pct": pnl_pct,
    }


def get_fee_rates(client: UpbitClient, chance_cache: ChanceCache, cfg: Config, market: str) -> tuple[float, float]:
    chance = get_chance_cached(client, chance_cache, market)
    bid_fee = float(chance.get("bid_fee", 0) or 0)
    ask_fee = float(chance.get("ask_fee", 0) or 0)
    maker_bid_fee = float(chance.get("maker_bid_fee", bid_fee) or bid_fee)
    maker_ask_fee = float(chance.get("maker_ask_fee", ask_fee) or ask_fee)

    if cfg.limit_time_in_force == "post_only":
        return maker_bid_fee, maker_ask_fee
    return bid_fee, ask_fee


def select_dynamic_markets(
    cfg: Config,
    client: UpbitClient,
    market_info_cache: MarketInfoCache,
) -> list[str]:
    infos = market_info_cache.data or {}

    quote_markets = []
    for market, info in infos.items():
        if "-" not in market:
            continue
        quote = market.split("-")[0]
        if quote not in cfg.dynamic_quote_currencies:
            continue
        if market in cfg.excluded_markets:
            continue

        warning = extract_market_warning(info)
        if cfg.exclude_warning_markets and warning not in {"NONE", ""}:
            continue

        quote_markets.append(market)

    if not quote_markets:
        return []

    ticker_map = client.get_tickers(quote_markets)
    candidates = []
    extra_set = set(cfg.extra_fixed_markets)

    for market in quote_markets:
        ticker = ticker_map.get(market)
        if not ticker:
            continue

        acc_trade_price_24h = float(ticker.get("acc_trade_price_24h", 0) or 0)
        if market not in extra_set and acc_trade_price_24h < cfg.dynamic_min_acc_trade_price_24h:
            continue

        # ── 품질 필터 (use_quality_filter=True 시) ──────────────────────────
        if cfg.dynamic_use_quality_filter and market not in extra_set:
            trade_price = float(ticker.get("trade_price", 0) or 0)
            high_price  = float(ticker.get("high_price", 0) or 0)
            low_price   = float(ticker.get("low_price", 0) or 0)
            if trade_price > 0 and high_price > 0 and low_price > 0:
                daily_range_pct = (high_price - low_price) / trade_price
                if daily_range_pct > cfg.dynamic_max_daily_range_pct:
                    continue  # 과열/급등 종목 제외

        candidates.append({
            "market": market,
            "acc_trade_price_24h": acc_trade_price_24h,
        })

    candidates.sort(key=lambda x: x["acc_trade_price_24h"], reverse=True)

    selected: list[str] = []
    seen = set()

    for market in cfg.extra_fixed_markets:
        if market in cfg.excluded_markets:
            continue
        info = infos.get(market, {})
        warning = extract_market_warning(info)
        if cfg.exclude_warning_markets and warning not in {"NONE", ""}:
            continue
        if market not in seen:
            selected.append(market)
            seen.add(market)

    # 캔들 부족 종목 대체를 위해 예비 후보 포함 (TOP_N + 5개)
    reserve_count = cfg.dynamic_top_n + 5
    
    for item in candidates:
        market = item["market"]
        if market in seen:
            continue
        selected.append(market)
        seen.add(market)
        if len(selected) >= reserve_count:  # 예비 포함
            break

    return selected[:reserve_count]  # TOP_N + 예비 5개 반환


