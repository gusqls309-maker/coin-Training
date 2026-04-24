"""주문 실행/취소/재호가 + 주문 사전 검증 + 호가창 뎁스 체크."""
from __future__ import annotations

import logging
from decimal import Decimal

from .caches import ChanceCache
from .client import UpbitClient, get_chance_cached
from .config import Config
from .display import make_identifier, safe_json_dumps
from .indicators import age_seconds, calc_buy_volume
from .market_selector import is_bot_order
from .state_store import CsvJournal


def needs_reprice(existing_order: dict, target_price: float, reprice_sec: float) -> tuple[bool, str]:
    existing_price = Decimal(str(existing_order.get("price", 0) or 0))
    target_price_dec = Decimal(str(target_price))
    age_sec = age_seconds(existing_order.get("created_at"))

    if existing_price == target_price_dec:
        return False, f"가격 동일({existing_price})"
    if age_sec < reprice_sec:
        return False, f"경과시간 부족({age_sec:.0f}s < {reprice_sec:.0f}s)"
    return True, f"재주문 필요({existing_price} -> {target_price_dec})"


def get_remaining_volume_for_reprice(existing_order: dict, fallback_volume: float) -> float:
    remaining = float(existing_order.get("remaining_volume", 0) or 0)
    return remaining if remaining > 0 else fallback_volume


def precheck_buy_order(client: UpbitClient, chance_cache: ChanceCache, cfg: Config,
                       market: str, best_bid: float) -> tuple[bool, str, dict]:
    chance = get_chance_cached(client, chance_cache, market)
    market_info = chance.get("market", {})
    bid_account = chance.get("bid_account", {})

    bid_types = market_info.get("bid_types", []) or []
    bid_balance = float(bid_account.get("balance", 0) or 0)

    bid_fee = float(chance.get("bid_fee", 0) or 0)
    maker_bid_fee = float(chance.get("maker_bid_fee", bid_fee) or bid_fee)
    fee_rate = maker_bid_fee if cfg.limit_time_in_force == "post_only" else bid_fee

    bid_rule = market_info.get("bid", {}) or {}
    min_total = float(bid_rule.get("min_total", 0) or 0)

    limit_supported = any(t.startswith("limit") or t == "limit" for t in bid_types)
    if not limit_supported:
        return False, f"limit 주문 미지원 | bid_types={bid_types}", {}

    spendable_krw = min(cfg.buy_krw_amount, bid_balance / (1.0 + fee_rate)) if fee_rate >= 0 else min(cfg.buy_krw_amount, bid_balance)
    target_volume = calc_buy_volume(spendable_krw, best_bid, cfg.volume_decimals)
    order_notional = best_bid * target_volume
    total_required_with_fee = order_notional * (1.0 + fee_rate)

    if target_volume <= 0 or order_notional <= 0:
        return False, "계산된 주문 수량이 0입니다.", {}
    if min_total > 0 and order_notional < min_total:
        return False, f"최소 주문금액 미만 | order_notional={order_notional:.0f}, min_total={min_total:.0f}", {}
    if bid_balance + 1e-9 < total_required_with_fee:
        return False, f"수수료 포함 잔고 부족 | balance={bid_balance:.0f}, need={total_required_with_fee:.0f}", {}

    return True, "매수 주문 사전체크 통과", {
        "fee_rate": fee_rate,
        "target_volume": target_volume,
        "order_notional": order_notional,
        "total_required_with_fee": total_required_with_fee,
    }


def precheck_sell_order(client: UpbitClient, chance_cache: ChanceCache, market: str,
                        volume_to_sell: float) -> tuple[bool, str]:
    chance = get_chance_cached(client, chance_cache, market)
    market_info = chance.get("market", {})
    ask_account = chance.get("ask_account", {})
    ask_types = market_info.get("ask_types", []) or []
    ask_balance = float(ask_account.get("balance", 0) or 0)

    limit_supported = any(t.startswith("limit") or t == "limit" for t in ask_types)
    if not limit_supported:
        return False, f"limit 주문 미지원 | ask_types={ask_types}"
    if ask_balance <= 0 or volume_to_sell <= 0:
        return False, f"매도 잔고 부족 | balance={ask_balance:.12f}"
    return True, "매도 주문 사전체크 통과"


def cancel_orders_for_market(client: UpbitClient, cfg: Config, journal: CsvJournal,
                             orders: list[dict], reason: str) -> int:
    count = 0
    for order in orders:
        market = order.get("market", "")
        side = order.get("side", "")
        price = float(order.get("price", 0) or 0)
        remain = float(order.get("remaining_volume", 0) or 0)
        msg = f"주문 취소 | market={market} side={side} price={price:.0f} remain={remain:.12f} | reason={reason}"

        if cfg.order_mode != "live":
            logging.info("[SIGNAL] %s", msg)
            journal.append_trade(
                event_type="cancel_signal",
                mode=cfg.order_mode,
                market=market,
                side=side,
                price=price,
                volume=remain,
                krw_amount=None,
                order_uuid=order.get("uuid"),
                identifier=order.get("identifier"),
                state=order.get("state"),
                message=msg,
                response_json=order,
            )
            count += 1
            continue

        try:
            result = client.cancel_order(order_uuid=order.get("uuid"), identifier=order.get("identifier"))
            logging.info("%s | 완료", msg)
            journal.append_trade(
                event_type="cancel_live",
                mode="live",
                market=market,
                side=side,
                price=price,
                volume=remain,
                krw_amount=None,
                order_uuid=result.get("uuid"),
                identifier=result.get("identifier"),
                state=result.get("state"),
                message=msg,
                response_json=result,
            )
            count += 1
        except Exception as cancel_err:
            err_msg = f"{msg} | ⚠ 취소 API 실패: {cancel_err}"
            logging.warning(err_msg)
            journal.append_trade(
                event_type="cancel_live_failed",
                mode="live",
                market=market,
                side=side,
                price=price,
                volume=remain,
                krw_amount=None,
                order_uuid=order.get("uuid"),
                identifier=order.get("identifier"),
                state="cancel_failed",
                message=err_msg,
                response_json={"error": str(cancel_err)},
            )
    return count


def place_or_reprice_limit_buy(
    client: UpbitClient,
    cfg: Config,
    journal: CsvJournal,
    market: str,
    market_open_orders: list[dict],
    best_bid: float,
    target_volume: float,
    target_krw_amount: float,
) -> None:
    buy_orders = [o for o in market_open_orders if o.get("side") == "bid" and is_bot_order(o, cfg.bot_id_prefix)]
    tif = cfg.limit_time_in_force or None

    if not buy_orders:
        identifier = make_identifier(cfg.bot_id_prefix, "buy")
        msg = f"신규 지정가 매수 | market={market} price={best_bid:.0f} volume={target_volume:.12f}"
        result = client.place_limit_buy(
            market=market,
            price=best_bid,
            volume=target_volume,
            order_mode=cfg.order_mode,
            identifier=identifier,
            time_in_force=tif,
            volume_decimals=cfg.volume_decimals,
        )
        logging.info("%s | 결과=%s", msg, safe_json_dumps(result))
        journal.append_trade(
            event_type="limit_buy_new",
            mode=cfg.order_mode,
            market=market,
            side="bid",
            price=best_bid,
            volume=target_volume,
            krw_amount=target_krw_amount,
            order_uuid=result.get("uuid") if isinstance(result, dict) else None,
            identifier=result.get("identifier") if isinstance(result, dict) else identifier,
            state=result.get("state") if isinstance(result, dict) else None,
            message=msg,
            response_json=result,
        )
        return

    existing = buy_orders[0]
    do_reprice, reason = needs_reprice(existing, best_bid, cfg.limit_reprice_sec)
    logging.info("기존 매수 주문 점검 | market=%s | %s", market, reason)
    if not do_reprice:
        return

    reprice_volume = min(get_remaining_volume_for_reprice(existing, target_volume), target_volume)
    new_identifier = make_identifier(cfg.bot_id_prefix, "buy")
    msg = f"매수 재주문 | market={market} old={float(existing.get('price',0) or 0):.0f} -> new={best_bid:.0f}"

    if cfg.order_mode != "live":
        logging.info("[SIGNAL] %s", msg)
        journal.append_trade(
            event_type="limit_buy_reprice_signal",
            mode=cfg.order_mode,
            market=market,
            side="bid",
            price=best_bid,
            volume=reprice_volume,
            krw_amount=target_krw_amount,
            order_uuid=existing.get("uuid"),
            identifier=existing.get("identifier"),
            state=existing.get("state"),
            message=msg,
            response_json=existing,
        )
        return

    if cfg.use_cancel_and_new:
        result = client.cancel_and_new_limit(
            prev_order_uuid=existing.get("uuid"),
            prev_order_identifier=existing.get("identifier"),
            new_price=best_bid,
            new_volume=reprice_volume,
            new_identifier=new_identifier,
            new_time_in_force=tif,
            volume_decimals=cfg.volume_decimals,
        )
        logging.info("%s | cancel_and_new 결과=%s", msg, safe_json_dumps(result))
        journal.append_trade(
            event_type="limit_buy_reprice_live",
            mode="live",
            market=market,
            side="bid",
            price=best_bid,
            volume=reprice_volume,
            krw_amount=target_krw_amount,
            order_uuid=None,
            identifier=new_identifier,
            state="replaced",
            message=msg,
            response_json=result,
        )
    else:
        cancel_result = client.cancel_order(order_uuid=existing.get("uuid"), identifier=existing.get("identifier"))
        new_result = client.place_limit_buy(
            market=market,
            price=best_bid,
            volume=reprice_volume,
            order_mode="live",
            identifier=new_identifier,
            time_in_force=tif,
            volume_decimals=cfg.volume_decimals,
        )
        logging.info("%s | 수동 취소+재주문 결과=%s", msg, safe_json_dumps(new_result))
        journal.append_trade(
            event_type="limit_buy_reprice_live_manual",
            mode="live",
            market=market,
            side="bid",
            price=best_bid,
            volume=reprice_volume,
            krw_amount=target_krw_amount,
            order_uuid=new_result.get("uuid") if isinstance(new_result, dict) else None,
            identifier=new_identifier,
            state=new_result.get("state") if isinstance(new_result, dict) else None,
            message=msg,
            response_json={"cancel": cancel_result, "new": new_result},
        )


def check_orderbook_depth(
    cfg: Config,
    market: str,
    orderbook_units: list[dict],
    sell_volume: float,
    current_price: float,
) -> tuple[bool, str, float]:
    """
    매도 시 Market Impact 체크.
    내 매도 수량이 매수호가(bid) 잔량을 얼마나 소화하는지 계산합니다.
    (매도 주문은 bid 측 잔량을 소화하므로 ask가 아닌 bid 기준이 올바릅니다.)

    Returns:
        (impact_warning, reason, safe_price)
    """
    if not cfg.orderbook_depth_check or not orderbook_units:
        best_bid = float(orderbook_units[0]["bid_price"]) if orderbook_units else current_price
        return False, "뎁스 체크 비활성화", best_bid

    best_bid_price = float(orderbook_units[0]["bid_price"])
    best_bid_size  = float(orderbook_units[0]["bid_size"])   # 1호가 매수 잔량 (코인 수량)
    best_ask_price = float(orderbook_units[0]["ask_price"])

    if best_bid_size <= 0:
        return False, "1호가 매수잔량 정보 없음", best_bid_price

    impact_ratio = sell_volume / best_bid_size

    if impact_ratio <= cfg.orderbook_depth_impact_ratio:
        reason = (
            f"뎁스 충분 | 내매도={sell_volume:.6f} / 1호가매수잔량={best_bid_size:.6f}"
            f" ({impact_ratio * 100:.1f}%)"
        )
        return False, reason, best_ask_price  # 충격 없으면 best_ask(maker)로 매도

    # bid 잔량 소진 — 2호가 이하로 밀릴 수 있음 → 가중평균 체결가 계산
    accumulated = 0.0
    weighted_price_sum = 0.0
    for unit in orderbook_units:
        bid_price = float(unit["bid_price"])
        bid_size  = float(unit["bid_size"])
        take = min(bid_size, sell_volume - accumulated)
        weighted_price_sum += bid_price * take
        accumulated += take
        if accumulated >= sell_volume:
            break

    avg_fill_price = weighted_price_sum / sell_volume if sell_volume > 0 else best_bid_price
    slippage_pct = (best_bid_price - avg_fill_price) / best_bid_price * 100

    reason = (
        f"⚠ Market Impact 경고 | 내매도={sell_volume:.6f} > 1호가매수잔량={best_bid_size:.6f}"
        f" ({impact_ratio * 100:.1f}%) | 예상평균체결가={avg_fill_price:.2f}"
        f" | 슬리피지≈{slippage_pct:+.4f}%"
    )
    # 충격 발생 시 best_bid(즉시 taker) 기준으로 청산 권장
    return True, reason, best_bid_price


def place_or_reprice_limit_sell(
    client: UpbitClient,
    cfg: Config,
    journal: CsvJournal,
    market: str,
    market_open_orders: list[dict],
    target_price: float,   # 공격적 매도 시 best_bid, 일반 시 best_ask 모두 수용
    fallback_volume: float,
) -> None:
    sell_orders = [o for o in market_open_orders if o.get("side") == "ask" and is_bot_order(o, cfg.bot_id_prefix)]
    tif = cfg.limit_time_in_force or None

    if not sell_orders:
        identifier = make_identifier(cfg.bot_id_prefix, "sell")
        msg = f"신규 지정가 매도 | market={market} price={target_price:.0f} volume={fallback_volume:.12f}"
        result = client.place_limit_sell(
            market=market,
            price=target_price,
            volume=fallback_volume,
            order_mode=cfg.order_mode,
            identifier=identifier,
            time_in_force=tif,
            volume_decimals=cfg.volume_decimals,
        )
        logging.info("%s | 결과=%s", msg, safe_json_dumps(result))
        journal.append_trade(
            event_type="limit_sell_new",
            mode=cfg.order_mode,
            market=market,
            side="ask",
            price=target_price,
            volume=fallback_volume,
            krw_amount=None,
            order_uuid=result.get("uuid") if isinstance(result, dict) else None,
            identifier=result.get("identifier") if isinstance(result, dict) else identifier,
            state=result.get("state") if isinstance(result, dict) else None,
            message=msg,
            response_json=result,
        )
        return

    existing = sell_orders[0]
    do_reprice, reason = needs_reprice(existing, target_price, cfg.limit_reprice_sec)
    logging.info("기존 매도 주문 점검 | market=%s | %s", market, reason)
    if not do_reprice:
        return

    reprice_volume = get_remaining_volume_for_reprice(existing, fallback_volume)
    new_identifier = make_identifier(cfg.bot_id_prefix, "sell")
    msg = f"매도 재주문 | market={market} old={float(existing.get('price',0) or 0):.0f} -> new={target_price:.0f}"

    if cfg.order_mode != "live":
        logging.info("[SIGNAL] %s", msg)
        journal.append_trade(
            event_type="limit_sell_reprice_signal",
            mode=cfg.order_mode,
            market=market,
            side="ask",
            price=target_price,
            volume=reprice_volume,
            krw_amount=None,
            order_uuid=existing.get("uuid"),
            identifier=existing.get("identifier"),
            state=existing.get("state"),
            message=msg,
            response_json=existing,
        )
        return

    if cfg.use_cancel_and_new:
        result = client.cancel_and_new_limit(
            prev_order_uuid=existing.get("uuid"),
            prev_order_identifier=existing.get("identifier"),
            new_price=target_price,
            new_volume=reprice_volume,
            new_identifier=new_identifier,
            new_time_in_force=tif,
            volume_decimals=cfg.volume_decimals,
        )
        logging.info("%s | cancel_and_new 결과=%s", msg, safe_json_dumps(result))
        journal.append_trade(
            event_type="limit_sell_reprice_live",
            mode="live",
            market=market,
            side="ask",
            price=target_price,
            volume=reprice_volume,
            krw_amount=None,
            order_uuid=None,
            identifier=new_identifier,
            state="replaced",
            message=msg,
            response_json=result,
        )
    else:
        cancel_result = client.cancel_order(
            order_uuid=existing.get("uuid"),
            identifier=existing.get("identifier"),
        )
        new_result = client.place_limit_sell(
            market=market,
            price=target_price,
            volume=reprice_volume,
            order_mode="live",
            identifier=new_identifier,
            time_in_force=tif,
            volume_decimals=cfg.volume_decimals,
        )
        logging.info("%s | 수동 취소+재주문 결과=%s", msg, safe_json_dumps(new_result))
        journal.append_trade(
            event_type="limit_sell_reprice_live_manual",
            mode="live",
            market=market,
            side="ask",
            price=target_price,
            volume=reprice_volume,
            krw_amount=None,
            order_uuid=new_result.get("uuid") if isinstance(new_result, dict) else None,
            identifier=new_identifier,
            state=new_result.get("state") if isinstance(new_result, dict) else None,
            message=msg,
            response_json={"cancel": cancel_result, "new": new_result},
        )


