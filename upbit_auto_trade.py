"""
upbit_auto_trade.py — 업비트 자동매매 봇 메인 엔트리포인트

하위 모듈:
    modules/config.py         - Config dataclass
    modules/client.py         - UpbitClient REST API 래퍼
    modules/indicators.py     - 지표 계산 (SMA/EMA/RSI/ATR/OBI)
    modules/strategy.py       - 진입/청산 전략 판단
    modules/orders.py         - 주문 실행/취소/재호가
    modules/state_store.py    - 영속 상태 저장 + 거래 저널
    modules/caches.py         - 각종 캐시 클래스
    modules/market_selector.py - 종목 선정 + 계좌 스냅샷
    modules/display.py        - 콘솔/로깅/시간 유틸
    modules/wfa_scheduler.py  - WFA 자동 최적화 스케줄러
"""
from __future__ import annotations

# ── 경로 보정 (Windows 환경에서 modules 패키지를 찾지 못하는 경우 대비) ──────
# 이 파일이 위치한 디렉토리를 sys.path 맨 앞에 추가합니다.
# python upbit_auto_trade.py 로 실행 시 작업 디렉토리가 다를 때도 안전합니다.
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import logging
import re
import threading
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# PyJWT 버전에 따라 InsecureKeyLengthWarning 존재 여부가 다름 (2.4+ 에서 추가)
try:
    from jwt import InsecureKeyLengthWarning
    warnings.filterwarnings("ignore", category=InsecureKeyLengthWarning)
except ImportError:
    pass
load_dotenv()

# ── 하위 모듈 import ───────────────────────────────────────────────────────
from modules.config import Config, parse_bool
from modules.client import UpbitClient, get_chance_cached, BASE_URL
from modules.indicators import (
    age_seconds, has_young_order, calc_buy_volume,
    extract_candles_asc, estimate_net_pnl_pct, required_candle_count,
    sma, sma_prev, ema, ema_prev, calc_rsi_wilder, calc_atr, calc_obi,
)
from modules.strategy import (
    build_tf_trend_snapshot, build_current_tf_filters, build_strategy_snapshot,
    compute_score, should_sell, classify_exit_reason,
    is_aggressive_sell_reason, check_partial_exit, calc_dynamic_buy_amount,
)
from modules.orders import (
    needs_reprice, get_remaining_volume_for_reprice,
    precheck_buy_order, precheck_sell_order,
    cancel_orders_for_market, place_or_reprice_limit_buy,
    check_orderbook_depth, place_or_reprice_limit_sell,
)
from modules.state_store import MultiMarketStateStore, CsvJournal
from modules.caches import (
    MarketInfoCache, ChanceCache, AccountCache,
    ClosedCandleCache, DynamicMarketCache,
)
from modules.market_selector import (
    extract_market_warning, is_bot_order, accounts_to_map,
    get_position_snapshot, get_fee_rates, select_dynamic_markets,
)
from modules.display import (
    now_str, safe_json_dumps, num_to_str, make_identifier, parse_upbit_dt,
    render_table, clear_console, move_cursor_up, clear_from_cursor,
    hide_cursor, show_cursor, setup_logging, describe_position_state,
)
from modules.wfa_scheduler import WFAScheduler


def main():
    cfg = Config()
    cfg.validate()
    setup_logging(cfg.log_file)

    client = UpbitClient(cfg.access_key, cfg.secret_key)
    journal = CsvJournal(cfg.status_csv_file, cfg.trade_csv_file)
    state_store = MultiMarketStateStore(cfg.bot_state_file)
    candle_cache = ClosedCandleCache(cfg.candle_min_interval_sec)
    market_info_cache = MarketInfoCache(cfg.market_info_refresh_sec)
    chance_cache = ChanceCache(cfg.chance_cache_sec)
    account_cache = AccountCache(ttl_sec=5)  # 계좌 정보 캐시 추가
    dynamic_market_cache = DynamicMarketCache(cfg.dynamic_refresh_sec)

    logging.info(
        "멀티코인 스캐너 시작 | mode=%s | max_positions=%d | dynamic_markets=%s",
        cfg.order_mode,
        cfg.max_active_positions,
        cfg.use_dynamic_markets,
    )

    # ── WFA 자동 최적화 스케줄러 ────────────────────────────────────────────
    cfg_lock = threading.Lock()   # cfg 동시 수정 방지용 잠금
    wfa_scheduler: WFAScheduler | None = None
    if cfg.auto_wfa_enabled:
        wfa_scheduler = WFAScheduler(cfg, cfg_lock)
        wfa_scheduler.start()
        logging.info("WFA 스케줄러 시작 | interval=%dh | auto_apply=%s",
                     cfg.auto_wfa_interval_hours, cfg.auto_wfa_apply_params)
    
    # 부드러운 갱신을 위한 변수
    prev_output_lines = 0
    
    # 모드 선택: 부드러운 갱신 vs 전체 clear
    if cfg.use_smooth_refresh:
        # 부드러운 갱신 모드: 커서 숨김
        hide_cursor()

    while True:
        try:
            # ── WFA 파라미터 자동 적용 ──────────────────────────────────────
            if wfa_scheduler and cfg.auto_wfa_apply_params:
                best_p = wfa_scheduler.get_best_params()
                if best_p:
                    with cfg_lock:   # WFAScheduler 스레드와 동시 수정 방지
                        for k, v in best_p.items():
                            if hasattr(cfg, k):
                                try:
                                    old_v = getattr(cfg, k)
                                    new_v = type(old_v)(v)
                                    if old_v != new_v:
                                        setattr(cfg, k, new_v)
                                        logging.info("WFA 파라미터 자동 적용 | %s: %s → %s", k, old_v, new_v)
                                except Exception:
                                    pass

            # 화면 갱신 처리
            if cfg.console_clear_each_loop:
                if cfg.use_smooth_refresh:
                    # 부드러운 갱신: 커서 이동 후 지우기
                    if prev_output_lines > 0:
                        move_cursor_up(prev_output_lines)
                        clear_from_cursor()
                else:
                    # 전체 clear 모드
                    clear_console()

            if market_info_cache.expired():
                infos = client.get_markets(is_details=True)
                market_info_cache.update(infos)

            if cfg.use_dynamic_markets:
                if dynamic_market_cache.expired() or not dynamic_market_cache.markets:
                    selected = select_dynamic_markets(cfg, client, market_info_cache)
                    dynamic_market_cache.update(selected)
                    logging.info("동적 종목 선정 | markets=%s", ",".join(selected))
                eligible_markets = dynamic_market_cache.markets[:]
            else:
                eligible_markets = []
                for market in cfg.markets:
                    if market in cfg.excluded_markets:
                        continue
                    info = market_info_cache.get(market)
                    if not info:
                        logging.warning("마켓 정보 없음 | %s", market)
                        continue
                    warning = extract_market_warning(info)
                    if cfg.exclude_warning_markets and warning not in {"NONE", ""}:
                        logging.info("시장경보 제외 | market=%s | warning=%s", market, warning)
                        continue
                    eligible_markets.append(market)

            # 보유 종목 집합 생성 (API 호출 최적화용)
            all_open_orders = client.get_all_open_orders(states=["wait", "watch"])

            # current_positions = 미체결 주문 종목 + 실제 보유 종목(잔고 기반)
            # open_orders 기반만 쓰면 주문 없는 보유 종목이 누락되어
            # AccountCache.needs_update 판단이 부정확해짐
            order_markets = {str(o.get("market", "")).upper() for o in all_open_orders if o.get("market")}

            # TTL 만료 시 먼저 accounts를 조회해 보유 종목까지 포함한 current_positions 구성
            # (TTL 내라면 이전 캐시 데이터로 종목 목록을 추출)
            prev_accounts = account_cache.data or []
            held_markets = {
                f"KRW-{str(acc.get('currency', '')).upper()}"
                for acc in prev_accounts
                if acc.get("currency") and acc.get("currency") != "KRW"
                and (float(acc.get("balance", 0) or 0) + float(acc.get("locked", 0) or 0)) > 0
            }
            current_positions = order_markets | held_markets

            # 계좌 정보는 스마트 캐시 사용 (변화 있을 때만 API 호출)
            accounts = account_cache.get(client, current_positions)

            # 동적 선정 종목 외에도, 현재 보유 중이거나 미체결 주문이 있는 종목은
            # 반드시 감시 목록에 포함해야 손절/익절/재주문 관리가 끊기지 않습니다.
            supplemental_markets: list[str] = []
            market_infos = market_info_cache.data or {}

            for acc in accounts:
                currency = str(acc.get("currency", "") or "").upper()
                if not currency or currency == "KRW":
                    continue
                total = float(acc.get("balance", 0) or 0) + float(acc.get("locked", 0) or 0)
                if total <= 0:
                    continue

                preferred_market = None
                krw_market = f"KRW-{currency}"
                if krw_market in market_infos:
                    preferred_market = krw_market
                else:
                    candidates = [m for m in market_infos.keys() if m.endswith(f"-{currency}")]
                    if cfg.dynamic_quote_currencies:
                        preferred = [m for m in candidates if m.split("-")[0] in cfg.dynamic_quote_currencies]
                        candidates = preferred or candidates
                    if candidates:
                        preferred_market = sorted(candidates)[0]

                if preferred_market:
                    supplemental_markets.append(preferred_market)

            supplemental_markets.extend(
                str(o.get("market", "") or "").upper()
                for o in all_open_orders
                if o.get("market")
            )

            if supplemental_markets:
                seen = set(m.upper() for m in eligible_markets)
                added_markets: list[str] = []
                for market in supplemental_markets:
                    market = market.upper()
                    if market and market not in seen:
                        eligible_markets.append(market)
                        seen.add(market)
                        added_markets.append(market)
                if added_markets:
                    logging.info("보유/미체결 종목 감시 추가 | markets=%s", ",".join(added_markets))

            if not eligible_markets:
                logging.warning("분석 가능한 종목이 없습니다.")
                time.sleep(cfg.poll_interval_sec)
                continue

            tickers = client.get_tickers(eligible_markets)
            orderbooks = client.get_orderbooks(eligible_markets, count=cfg.orderbook_print_depth)

            analyses: dict[str, dict] = {}
            insufficient_candle_markets: list[str] = []  # 캔들 부족 종목 추적

            for market in eligible_markets:
                ticker = tickers.get(market)
                orderbook = orderbooks.get(market)
                if not ticker or not orderbook:
                    logging.warning("시세/호가 누락 | market=%s", market)
                    continue

                units = orderbook.get("orderbook_units") or []
                if not units:
                    logging.warning("호가창 비어 있음 | market=%s", market)
                    continue

                current_price = float(ticker["trade_price"])
                best_bid = float(units[0]["bid_price"])
                best_ask = float(units[0]["ask_price"])

                # 캔들 데이터 조회 (에러 발생 시 종목 스킵)
                try:
                    cur_candles = candle_cache.get_closed_candles(
                        client=client,
                        market=market,
                        unit=cfg.candle_unit,
                        count=cfg.candle_count,
                        refresh_only_on_new_bucket=cfg.refresh_candles_only_on_new_bucket,
                    )
                    htf_candles = candle_cache.get_closed_candles(
                        client=client,
                        market=market,
                        unit=cfg.htf_candle_unit,
                        count=cfg.htf_candle_count,
                        refresh_only_on_new_bucket=cfg.refresh_candles_only_on_new_bucket,
                    )
                except ValueError as e:
                    # 429 에러 또는 기타 API 오류
                    if "429" in str(e) or "레이트 리밋" in str(e):
                        logging.warning(f"{market} 레이트 리밋 | 이번 루프 스킵")
                    else:
                        logging.warning(f"{market} 캔들 조회 실패 | {e}")
                    continue
                except Exception as e:
                    logging.warning(f"{market} 캔들 조회 예외 | {type(e).__name__}: {e}")
                    continue

                cur_required = required_candle_count(
                    cfg.ma_short_period,
                    cfg.ma_long_period,
                    cfg.rsi_period,
                )
                htf_required = required_candle_count(
                    cfg.htf_ma_short_period,
                    cfg.htf_ma_long_period,
                    None,
                )

                if len(cur_candles) < cur_required:
                    logging.warning(
                        "캔들 데이터 부족으로 종목 제외 | market=%s | tf=%dm | need=%d | got=%d",
                        market,
                        cfg.candle_unit,
                        cur_required,
                        len(cur_candles),
                    )
                    insufficient_candle_markets.append(market)
                    continue

                if len(htf_candles) < htf_required:
                    logging.warning(
                        "캔들 데이터 부족으로 종목 제외 | market=%s | htf=%dm | need=%d | got=%d",
                        market,
                        cfg.htf_candle_unit,
                        htf_required,
                        len(htf_candles),
                    )
                    insufficient_candle_markets.append(market)
                    continue

                current_tf = build_current_tf_filters(cur_candles, current_price, cfg, best_bid, best_ask)
                higher_tf = build_tf_trend_snapshot(
                    htf_candles,
                    current_price,
                    cfg.htf_ma_short_period,
                    cfg.htf_ma_long_period,
                    None,
                    use_ema=cfg.use_ema,   # ← 현재 TF와 동일한 방식으로 계산
                )

                snap = get_position_snapshot(accounts, market, current_price)

                prev_base_total = state_store.get_prev_base_total(market)
                # 진입시각이 0일 때만 새로 설정 (재시작 시 보유시간 초기화 방지)
                if prev_base_total <= 0 and snap["base_total"] > 0:
                    if state_store.hold_seconds(market) <= 0:
                        state_store.set_entry_now(market)
                        logging.info(f"{market} 신규 진입 감지 | base_total={snap['base_total']:.8f}")

                if prev_base_total > 0 and snap["base_total"] <= 0:
                    exit_reason = state_store.pop_pending_exit_reason(market) or "exit"

                    # partial_exit 사유로 전체 잔량이 0이 됐다면 실제로 전량 매도된 것
                    # 이 경우 exit_reason을 "sell"로 처리해 쿨다운 적용
                    if exit_reason == "partial_exit":
                        exit_reason = "sell"
                        logging.info("부분익절 후 전량 체결 완료 | market=%s", market)

                    cooldown_sec = cfg.cooldown_after_stop_loss_sec if exit_reason == "stop_loss" else cfg.cooldown_after_exit_sec
                    state_store.set_cooldown(market, cooldown_sec, exit_reason)
                    state_store.clear_entry(market)
                    logging.info("포지션 종료 감지 | market=%s | cooldown=%ds | reason=%s", market, cooldown_sec, exit_reason)

                    # 손절 감지 → 서킷브레이커 카운터 갱신
                    if exit_reason == "stop_loss" and getattr(cfg, "circuit_breaker_enabled", False):
                        stop_count = state_store.record_stop_loss(cfg.circuit_breaker_window_sec)
                        logging.info(
                            "손절 기록 | market=%s | %dh 내 누적 손절=%d회",
                            market, cfg.circuit_breaker_window_sec // 3600, stop_count,
                        )
                        if stop_count >= cfg.circuit_breaker_max_stops:
                            state_store.activate_circuit_breaker(cfg.circuit_breaker_cooldown_sec)

                elif prev_base_total > 0 and snap["base_total"] < prev_base_total:
                    # 수량이 줄었지만 0은 아님 → 부분 익절 체결된 것
                    # pending_exit_reason을 소비하지 않고, 쿨다운도 걸지 않음
                    if state_store.is_partial_exited(market):
                        logging.info(
                            "부분익절 체결 확인 | market=%s | %.8f → %.8f",
                            market, prev_base_total, snap["base_total"],
                        )

                # ── 보유 중 최고가 갱신 (트레일링 스탑용) ─────────────────────
                if snap["base_total"] > 0:
                    state_store.update_max_price(market, current_price)

                # ── OBI 계산 ──────────────────────────────────────────────
                ob_units_for_obi = (orderbooks.get(market, {}) or {}).get("orderbook_units") or []
                obi_val = calc_obi(ob_units_for_obi, depth=cfg.obi_depth) if ob_units_for_obi else 0.0

                strat = build_strategy_snapshot(
                    current_tf=current_tf,
                    higher_tf=higher_tf,
                    cooldown_active=state_store.cooldown_active(market),
                    cooldown_remaining_sec=state_store.cooldown_remaining_sec(market),
                    market_warning=extract_market_warning(market_info_cache.get(market)),
                    obi=obi_val,
                )

                score, score_reason = compute_score(cfg, strat)
                
                # 수수료는 실제 거래 가능성이 있는 종목만 조회 (API 호출 최적화)
                # 1) 보유 중이거나 2) 매수 스코어가 양수인 종목만
                has_position = snap["base_total"] > 0
                has_buy_signal = score > 0
                
                if has_position or has_buy_signal:
                    buy_fee_rate, sell_fee_rate = get_fee_rates(client, chance_cache, cfg, market)
                else:
                    # 보유도 없고 매수 신호도 없으면 기본 수수료 사용 (API 호출 생략)
                    buy_fee_rate = 0.0005
                    sell_fee_rate = 0.0005
                
                hold_sec = state_store.hold_seconds(market)
                max_price = state_store.get_max_price(market)
                be_activated = state_store.is_breakeven_activated(market)

                sell_ok, sell_reason, new_be_activated = should_sell(
                    cfg=cfg,
                    snap=snap,
                    strat=strat,
                    hold_sec=hold_sec,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    current_price=current_price,
                    max_price_since_entry=max_price,
                    breakeven_activated=be_activated,
                )
                # 브레이크이븐 활성화 상태를 영속 저장 (포지션 유지 중에만)
                if snap["base_total"] > 0 and new_be_activated and not be_activated:
                    state_store.set_breakeven_activated(market)
                net_pnl_pct = estimate_net_pnl_pct(
                    current_price=current_price,
                    avg_buy_price=snap["avg_buy_price"],
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                )

                market_open_orders = [o for o in all_open_orders if o.get("market") == market]
                market_buy_orders = [o for o in market_open_orders if o.get("side") == "bid" and is_bot_order(o, cfg.bot_id_prefix)]
                market_sell_orders = [o for o in market_open_orders if o.get("side") == "ask" and is_bot_order(o, cfg.bot_id_prefix)]

                # OBI 하드 필터 (use_obi_filter=True 이고 임계 미달 시 매수 차단)
                obi_blocks_entry = (
                    cfg.use_obi_filter
                    and obi_val < cfg.obi_min_threshold
                )

                # ── 히스테리시스 적용된 buy_ok ───────────────────────────────
                # HTF 추세 + 현재 추세 + 눌림목 + 거래대금 + RSI 범위는
                # 점수 보완이 아닌 하드 필터로 재확인합니다.
                # 단 하나라도 미충족이면 점수가 아무리 높아도 진입하지 않습니다.
                rsi_in_range = (
                    cfg.rsi_buy_min <= strat.get("rsi", 0) <= cfg.rsi_buy_max
                )
                buy_ok = (
                    score >= cfg.min_entry_score
                    and strat.get("htf_trend_up", False)   # 상위 추세 필수
                    and strat.get("trend_up", False)        # 현재 추세 필수
                    and strat.get("pullback_ok", False)     # 눌림목 필수
                    and strat.get("volume_ok", False)       # 거래대금 필수
                    and strat.get("spread_ok", False)       # 스프레드 필수 (넓은 구간 진입 차단)
                    and rsi_in_range                        # RSI 범위 필수
                    and not obi_blocks_entry
                    and snap["position_krw"] < cfg.min_order_krw
                    and len(market_buy_orders) == 0
                )
                buy_reason = score_reason
                if obi_blocks_entry:
                    buy_reason = f"OBI 필터 차단 | OBI={obi_val:+.3f} < {cfg.obi_min_threshold}"

                # ── 부분 익절 판단 ─────────────────────────────────────────
                partial_ok, partial_ratio, partial_reason = check_partial_exit(
                    cfg=cfg,
                    snap=snap,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    current_price=current_price,
                    already_partial_exited=state_store.is_partial_exited(market),
                )

                analyses[market] = {
                    "market": market,
                    "current_price": current_price,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "snap": snap,
                    "strat": strat,
                    "score": score,
                    "score_reason": score_reason,
                    "buy_ok": buy_ok,
                    "buy_reason": buy_reason,
                    "sell_ok": sell_ok,
                    "sell_reason": sell_reason,
                    "net_pnl_pct": net_pnl_pct,
                    "hold_sec": hold_sec,
                    "open_orders": market_open_orders,
                    "buy_orders": market_buy_orders,
                    "sell_orders": market_sell_orders,
                    "partial_ok": partial_ok,
                    "partial_ratio": partial_ratio,
                    "partial_reason": partial_reason,
                    "obi": obi_val,
                }

                journal.append_status(
                    market=market,
                    score=score,
                    current_price=current_price,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    strat=strat,
                    snap=snap,
                    open_order_count=len(market_open_orders),
                    buy_ok=buy_ok,
                    buy_reason=buy_reason,
                    sell_ok=sell_ok,
                    sell_reason=sell_reason,
                )

            if not analyses:
                logging.warning("분석 결과가 없습니다.")
                time.sleep(cfg.poll_interval_sec)
                continue
            
            # 동적 종목 선정 시 캔들 부족 종목 대체
            if cfg.use_dynamic_markets and insufficient_candle_markets:
                # 캔들 부족으로 제외된 종목이 있고, 분석 가능 종목이 목표치보다 적으면
                actual_analyzed = len(analyses)
                target_count = cfg.dynamic_top_n
                
                if actual_analyzed < target_count:
                    # 예비 후보 중 아직 사용 안 한 종목 찾기
                    all_candidates = dynamic_market_cache.markets  # 예비 포함 목록
                    already_tried = set(eligible_markets)
                    
                    replacement_candidates = [
                        m for m in all_candidates 
                        if m not in already_tried and m not in insufficient_candle_markets
                    ]
                    
                    shortage = target_count - actual_analyzed
                    if replacement_candidates and shortage > 0:
                        replacements = replacement_candidates[:shortage]
                        logging.info(
                            "캔들 부족 종목 대체 | 제외=%s | 대체=%s",
                            ",".join(insufficient_candle_markets[:3]),  # 처음 3개만 표시
                            ",".join(replacements)
                        )
                        # 다음 루프에서 대체 종목 포함하도록 갱신
                        # (현재 루프는 그대로 진행, 다음 루프부터 적용)
                        dynamic_market_cache.markets = [
                            m for m in all_candidates 
                            if m not in insufficient_candle_markets
                        ][:target_count + 5]  # 예비 포함

            score_rows = []
            for market, a in sorted(analyses.items(), key=lambda x: x[1]["score"], reverse=True):
                score_rows.append([
                    market,
                    f"{a['score']:.2f}",
                    "Y" if a["buy_ok"] else "N",
                    "Y" if a["sell_ok"] else "N",
                    f"{a['strat']['rsi']:.2f}",
                    f"{a['strat']['volume_ratio']:.2f}",
                    f"{a['strat']['spread_pct'] * 100:.4f}%",
                    f"{a['strat']['deviation_from_ma_short_pct'] * 100:+.2f}%",
                ])
            logging.info("스코어표:\n%s", render_table(
                ["market", "score", "buy", "sell", "rsi", "vol", "spread", "maΔ"], score_rows
            ))

            holding_rows = []
            for market, a in sorted(analyses.items(), key=lambda x: x[1]["snap"]["position_krw"], reverse=True):
                if a["snap"]["base_total"] <= 0:
                    continue
                state_label, wait_reason = describe_position_state(a)
                holding_rows.append([
                    market,
                    f"{a['snap']['base_total']:.8f}",
                    f"{a['snap']['avg_buy_price']:.4f}",
                    f"{a['current_price']:.4f}",
                    f"{a['snap']['pnl_pct'] * 100:+.2f}%",
                    f"{a['net_pnl_pct'] * 100:+.2f}%",
                    f"{a['hold_sec'] / 60:.1f}m",
                    state_label,
                    wait_reason[:48],
                ])
            if holding_rows:
                logging.info("보유 종목 상태표:\n%s", render_table(
                    ["market", "qty", "avg", "now", "gross", "net", "hold", "state", "waiting"],
                    holding_rows,
                ))

            # 매수 신호 해제 또는 포지션 보유 시 기존 매수 주문 정리
            for market, a in analyses.items():
                try:
                    if not a["buy_orders"]:
                        continue

                    # ── 히스테리시스: 취소 임계값은 진입 임계값보다 낮게 설정 ──
                    # min_entry_score(예: 60)에 buy_cancel_score_ratio(예: 0.85)를 곱한
                    # 값(=51점) 미만일 때만 취소 → score가 60점 언저리에서 진동해도
                    # 매수→취소 무한반복(oscillation)을 방지합니다.
                    cancel_score_threshold = cfg.min_entry_score * cfg.buy_cancel_score_ratio
                    score_below_cancel = a["score"] < cancel_score_threshold
                    has_position = a["snap"]["position_krw"] >= cfg.min_order_krw

                    # ── 최소 보유 시간: 주문 제출 직후에는 취소하지 않음 ─────────
                    # 예: LIMIT_REPRICE_SEC=15 이면 15초 이내 주문은 건드리지 않음
                    order_too_young = has_young_order(a["buy_orders"], cfg.buy_min_order_age_sec)

                    should_cancel_buy = (
                        not order_too_young          # 최소 보유 시간 경과 후에만
                        and (score_below_cancel or has_position)  # 취소 임계 이하 or 포지션 보유
                    )

                    if should_cancel_buy:
                        cancel_reason = (
                            f"포지션 보유" if has_position
                            else f"매수 신호 해제 (score={a['score']:.1f} < 취소임계={cancel_score_threshold:.1f})"
                        )
                        logging.info("매수 주문 취소 예정 | market=%s | %s", market, cancel_reason)
                        canceled = cancel_orders_for_market(
                            client, cfg, journal, a["buy_orders"], cancel_reason
                        )
                        if canceled and cfg.order_mode == "live":
                            a["open_orders"] = client.get_all_open_orders(states=["wait", "watch"], market=market)
                            a["buy_orders"] = [o for o in a["open_orders"] if o.get("side") == "bid" and is_bot_order(o, cfg.bot_id_prefix)]
                            a["sell_orders"] = [o for o in a["open_orders"] if o.get("side") == "ask" and is_bot_order(o, cfg.bot_id_prefix)]
                    elif order_too_young and (score_below_cancel or has_position):
                        # 취소하고 싶지만 아직 너무 어린 주문 → 다음 루프에서 재판단
                        logging.info(
                            "매수 주문 취소 보류 | market=%s | 주문 경과=%ds < 최소=%ds | score=%.1f",
                            market,
                            min(age_seconds(o.get("created_at")) for o in a["buy_orders"]),
                            cfg.buy_min_order_age_sec,
                            a["score"],
                        )
                except requests.exceptions.HTTPError as e:
                    if e.response and e.response.status_code in (400, 404):
                        logging.warning(f"{market} 매수 주문 취소 실패 (무시 가능): {e}")
                    else:
                        logging.error(f"{market} 매수 주문 취소 오류: {e}")
                except Exception as e:
                    logging.exception(f"{market} 매수 주문 처리 예외 (계속 진행): {e}")

            # 매도 신호가 있거나 기존 매도 주문이 있는 종목만 매도 관리
            for market, a in analyses.items():
                try:
                    # ── 부분 익절 처리 (전체 청산보다 우선 판단) ──────────────
                    if a.get("partial_ok") and not a["sell_ok"] and not a["sell_orders"]:
                        partial_volume = a["snap"]["base_total"] * a["partial_ratio"]
                        partial_volume = round(partial_volume, cfg.volume_decimals)
                        if partial_volume > 0:
                            logging.info(
                                "부분익절 실행 | market=%s | ratio=%.0f%% | volume=%.8f | %s",
                                market, a["partial_ratio"] * 100, partial_volume, a["partial_reason"]
                            )
                            state_store.set_pending_exit_reason(market, "partial_exit")
                            place_or_reprice_limit_sell(
                                client, cfg, journal, market,
                                a["open_orders"], a["best_ask"], partial_volume
                            )
                            state_store.set_partial_exited(market)
                            continue  # 부분 익절 실행 후 전체 청산 스킵

                    # ── 전량 청산 신호 + 기존 부분익절 주문 충돌 처리 ────────────
                    # 손절/방어청산 신호가 오면 부분 익절 주문을 먼저 취소하고
                    # 전체 보유 수량 기준으로 전량 매도해야 합니다.
                    if a["sell_ok"] and a["sell_orders"]:
                        sell_class = classify_exit_reason(a["sell_reason"])
                        is_full_exit = sell_class in (
                            "stop_loss", "htf_trend_exit", "trend_exit"
                        )
                        # 기존 주문의 사유가 partial_exit이면 부분익절 주문
                        # get(조회만) 사용 — 취소 성공 시에만 실제 소비(pop/override)
                        existing_reason = state_store.get_pending_exit_reason(market) or "sell"
                        existing_is_partial = classify_exit_reason(existing_reason) == "partial_exit"

                        if is_full_exit and existing_is_partial:
                            logging.info(
                                "전량 청산 신호 — 부분익절 주문 취소 후 전량 매도 | market=%s | 청산사유=%s",
                                market, a["sell_reason"][:50],
                            )
                            canceled = cancel_orders_for_market(
                                client, cfg, journal, a["sell_orders"],
                                f"전량 청산 우선 (부분익절 주문 대체): {a['sell_reason'][:30]}"
                            )
                            if cfg.order_mode == "live":
                                a["open_orders"] = client.get_all_open_orders(states=["wait", "watch"], market=market)
                                a["sell_orders"] = [o for o in a["open_orders"] if o.get("side") == "ask" and is_bot_order(o, cfg.bot_id_prefix)]

                            if canceled:
                                # 취소 성공 시에만 상태 교체
                                account_cache.invalidate()
                                # 취소 후 최신 잔고로 실제 가용 수량 재계산 (지적 5 수정)
                                if cfg.order_mode == "live":
                                    fresh_accounts = client.get_accounts()
                                    fresh_snap = get_position_snapshot(fresh_accounts, market, a["current_price"])
                                    full_volume = fresh_snap["base_balance"]
                                else:
                                    full_volume = a["snap"]["base_total"]

                                if full_volume > 0:
                                    state_store.set_pending_exit_reason(market, a["sell_reason"])
                                    # partial_exited 초기화 (취소됐으므로)
                                    state_store.clear_partial_exited(market)
                                    sell_price = a["best_bid"]
                                    place_or_reprice_limit_sell(
                                        client, cfg, journal, market, a["open_orders"],
                                        sell_price, full_volume
                                    )
                                    account_cache.invalidate()
                                    continue
                            # 취소 실패 시 — 기존 pending_exit_reason 유지 (get이라 지워지지 않음)

                    manage_sell = a["sell_ok"] or bool(a["sell_orders"])

                    # ── 익절 주문 조건 소멸 시 취소 (하락 추격 방지) ──────────────
                    # 기존 매도 주문이 있고 현재 sell_ok가 False인 경우:
                    # - 익절/부분익절 주문 → 조건 소멸이므로 취소
                    # - 손절/방어청산 계열 → 조건 여부 무관하게 유지
                    if a["sell_orders"] and not a["sell_ok"] and manage_sell:
                        # get(조회만) 사용 — 취소 성공 시에만 실제 소비
                        existing_reason = state_store.get_pending_exit_reason(market) or "sell"
                        existing_class = classify_exit_reason(existing_reason)
                        is_take_profit_order = existing_class in ("take_profit", "partial_exit")

                        if is_take_profit_order:
                            logging.info(
                                "익절 조건 소멸 → 매도 주문 취소 | market=%s | 원래사유=%s",
                                market, existing_reason[:50],
                            )
                            canceled = cancel_orders_for_market(
                                client, cfg, journal, a["sell_orders"],
                                f"익절 조건 소멸, 하락 추격 방지 (원래 사유: {existing_reason[:30]})"
                            )
                            if canceled:
                                # 취소 성공 시에만 상태 소비
                                state_store.set_pending_exit_reason(market, "")
                                # 부분익절 주문이 취소됐으면 partial_exited 초기화 (재시도 허용)
                                if existing_class == "partial_exit":
                                    state_store.clear_partial_exited(market)
                                if cfg.order_mode == "live":
                                    a["open_orders"] = client.get_all_open_orders(states=["wait", "watch"], market=market)
                                    a["sell_orders"] = [o for o in a["open_orders"] if o.get("side") == "ask" and is_bot_order(o, cfg.bot_id_prefix)]
                                account_cache.invalidate()
                                continue
                            # 취소 실패 시 — pending_exit_reason 그대로 유지 (get이라 지워지지 않음)
                    if manage_sell:
                        if a["buy_orders"]:
                            # ── 매도 우선 시 매수 주문 취소 ────────────────────
                            buy_cancel_min_age = cfg.buy_min_order_age_sec / 2
                            if not has_young_order(a["buy_orders"], buy_cancel_min_age):
                                canceled = cancel_orders_for_market(
                                    client, cfg, journal, a["buy_orders"], "매도 우선, 반대편 매수 주문 정리"
                                )
                                if canceled and cfg.order_mode == "live":
                                    a["open_orders"] = client.get_all_open_orders(states=["wait", "watch"], market=market)
                                    a["buy_orders"] = [o for o in a["open_orders"] if o.get("side") == "bid" and is_bot_order(o, cfg.bot_id_prefix)]
                                    a["sell_orders"] = [o for o in a["open_orders"] if o.get("side") == "ask" and is_bot_order(o, cfg.bot_id_prefix)]
                            else:
                                logging.info(
                                    "매도 우선 처리 대기 | market=%s | 매수 주문 경과=%ds < %ds",
                                    market,
                                    min(age_seconds(o.get("created_at")) for o in a["buy_orders"]),
                                    buy_cancel_min_age,
                                )

                        if a["sell_orders"]:
                            # 기존 매도 주문 관리 — remaining_volume 사용 (이미 실제 잔량 반영)
                            sell_volume = float(a["sell_orders"][0].get("remaining_volume", 0) or 0)
                            precheck_ok = sell_volume > 0
                            precheck_reason = f"기존 매도 주문 관리 | remaining_volume={sell_volume:.12f}"
                        else:
                            # 신규 매도 — base_balance가 실제 가용 수량 (locked 제외)
                            # 부분 익절 주문이 체결 대기 중이면 base_balance가 이미 줄어 있음
                            # base_balance=0이면 locked(부분익절 주문 중)만 남은 것 → 전량 처리 대기
                            sell_volume = a["snap"]["base_balance"]
                            if sell_volume <= 0:
                                # base_balance=0, base_locked>0 → 부분 익절 주문 체결 대기 중
                                logging.info(
                                    "매도 대기 | market=%s | 가용잔량 없음(부분익절 주문 체결 대기 중)",
                                    market,
                                )
                                continue
                            precheck_ok, precheck_reason = precheck_sell_order(client, chance_cache, market, sell_volume)

                        logging.info("매도 사전체크 | market=%s | %s", market, precheck_reason)

                        if precheck_ok and sell_volume > 0:
                            # 매도 사유를 항상 저장 (조건 소멸 시 취소 판단에 사용)
                            # get(조회만) 사용 — 기존 사유를 소비하지 않고 참조
                            exit_class = classify_exit_reason(a["sell_reason"]) if a["sell_ok"] else (
                                state_store.get_pending_exit_reason(market) or "sell"
                            )
                            state_store.set_pending_exit_reason(market, a["sell_reason"] if a["sell_ok"] else exit_class)

                            # ── 호가창 뎁스 체크 ─────────────────────────────
                            ob = orderbooks.get(market, {})
                            ob_units = ob.get("orderbook_units") or []
                            depth_warn, depth_reason, safe_sell_price = check_orderbook_depth(
                                cfg=cfg,
                                market=market,
                                orderbook_units=ob_units,
                                sell_volume=sell_volume,
                                current_price=a["current_price"],
                            )
                            if depth_warn:
                                logging.warning("호가창 뎁스 경고 | market=%s | %s", market, depth_reason)
                            else:
                                logging.info("호가창 뎁스 확인 | market=%s | %s", market, depth_reason)

                            # ── 공격적/일반 매도 분기 ─────────────────────────
                            # 손절·방어청산 등 긴급 사유 → best_bid taker 즉시 체결
                            # 익절·일반 → 기존 지정가 재호가 방식
                            sell_reason_str = a.get("sell_reason", "")
                            if cfg.use_aggressive_stop and is_aggressive_sell_reason(sell_reason_str):
                                sell_price = a["best_bid"]  # taker 즉시 체결
                                logging.info("공격적 매도 적용 | market=%s | price=best_bid=%.0f | reason=%s",
                                             market, sell_price, sell_reason_str[:40])
                            else:
                                sell_price = safe_sell_price if depth_warn else a["best_ask"]
                            # ─────────────────────────────────────────────────

                            if cfg.order_mode == "live":
                                a["open_orders"] = client.get_all_open_orders(states=["wait", "watch"], market=market)
                            place_or_reprice_limit_sell(
                                client, cfg, journal, market, a["open_orders"], sell_price, sell_volume
                            )
                            account_cache.invalidate()  # 매도 주문 후 잔고 즉시 재조회
                except requests.exceptions.HTTPError as e:
                    if e.response and e.response.status_code in (400, 404):
                        logging.warning(f"{market} 매도 주문 처리 실패 (무시 가능): {e}")
                    else:
                        logging.error(f"{market} 매도 주문 오류: {e}")
                except Exception as e:
                    logging.exception(f"{market} 매도 처리 예외 (계속 진행): {e}")

            current_position_count = 0
            for a in analyses.values():
                if a["snap"]["position_krw"] >= cfg.min_order_krw or a["sell_orders"]:
                    current_position_count += 1

            available_slots = max(0, cfg.max_active_positions - current_position_count)

            reserved_krw = 0.0
            for a in analyses.values():
                for o in a["buy_orders"]:
                    price = float(o.get("price", 0) or 0)
                    remain = float(o.get("remaining_volume", 0) or 0)
                    reserved_krw += price * remain

            accounts_map = accounts_to_map(accounts)
            available_krw = float(accounts_map.get("KRW", {}).get("balance", 0) or 0)

            candidates = [
                a for a in analyses.values()
                if a["buy_ok"]
                and a["snap"]["position_krw"] < cfg.min_order_krw
                and not a["buy_orders"]
                and not a["sell_orders"]
            ]
            candidates.sort(key=lambda x: x["score"], reverse=True)

            # ── 서킷브레이커 체크 — 발동 중이면 신규 진입 전면 차단 ──────────
            if getattr(cfg, "circuit_breaker_enabled", False) and state_store.is_circuit_breaker_active():
                remaining_cb = state_store.circuit_breaker_remaining_sec()
                logging.warning(
                    "⚡ 서킷브레이커 활성 | 신규 진입 차단 | 해제까지 %.0f초", remaining_cb
                )
                candidates = []  # 매수 후보 전원 제거

            selected_entries = []
            remaining_budget = max(0.0, available_krw - reserved_krw)
            remaining_slots = available_slots

            for a in candidates:
                if remaining_slots <= 0:
                    break

                # ── 점수 기반 매수 공격성 결정 (선정 단계에서 미리 확정) ────────
                if a["score"] >= cfg.score_aggressive_threshold:
                    buy_price = a["best_ask"]   # 고확신 → taker 즉시 체결
                else:
                    buy_price = a["best_bid"]   # 일반 → maker 지정가

                # ── ATR 기반 동적 포지션 사이징 ─────────────────────────────────
                atr_for_sizing = a["strat"].get("atr", 0.0)
                dynamic_buy_krw = calc_dynamic_buy_amount(cfg, a["current_price"], atr_for_sizing)

                # precheck를 dynamic_buy_krw 기준으로 단 한 번만 수행
                orig_buy_krw = cfg.buy_krw_amount
                try:
                    with cfg_lock:
                        cfg.buy_krw_amount = dynamic_buy_krw
                    precheck_ok, precheck_reason, buy_meta = precheck_buy_order(
                        client, chance_cache, cfg, a["market"], buy_price
                    )
                finally:
                    with cfg_lock:
                        cfg.buy_krw_amount = orig_buy_krw  # 예외 발생 시에도 반드시 원복

                if not precheck_ok:
                    logging.info("매수 사전체크 실패 | market=%s | %s", a["market"], precheck_reason)
                    continue

                need = float(buy_meta["total_required_with_fee"])
                if need <= remaining_budget + 1e-9:
                    # 결과를 저장해 실행 단계에서 재사용 (중복 API 호출 방지)
                    a["_buy_price"] = buy_price
                    a["_buy_meta"] = buy_meta
                    a["_dynamic_buy_krw"] = dynamic_buy_krw
                    selected_entries.append((a, buy_meta))
                    remaining_budget -= need
                    remaining_slots -= 1

            if selected_entries:
                logging.info(
                    "진입 후보 | slots=%d | budget=%.0f | selected=%s",
                    available_slots,
                    available_krw - reserved_krw,
                    ",".join(x[0]["market"] for x in selected_entries),
                )
            else:
                logging.info("진입 후보 없음 | slots=%d | budget=%.0f", available_slots, available_krw - reserved_krw)

            for a, _ in selected_entries:
                market = a["market"]
                try:
                    # 선정 단계에서 이미 계산된 값 재사용 (중복 API 호출 방지)
                    buy_price       = a["_buy_price"]
                    refreshed_meta  = a["_buy_meta"]
                    dynamic_buy_krw = a["_dynamic_buy_krw"]

                    if buy_price == a["best_ask"]:
                        logging.info("공격적 매수 | market=%s | score=%.1f >= %.1f | price=best_ask=%.0f",
                                     market, a["score"], cfg.score_aggressive_threshold, buy_price)

                    logging.info("매수 실행 | market=%s | buy_krw=%.0f | volume=%.8f | price=%.0f",
                                 market, dynamic_buy_krw,
                                 refreshed_meta["target_volume"], buy_price)

                    if cfg.order_mode == "live":
                        a["open_orders"] = client.get_all_open_orders(states=["wait", "watch"], market=market)

                    place_or_reprice_limit_buy(
                        client, cfg, journal, market, a["open_orders"], buy_price,
                        refreshed_meta["target_volume"], refreshed_meta["order_notional"]
                    )
                    account_cache.invalidate()  # 주문 제출 후 다음 루프에서 잔고 즉시 재조회
                except requests.exceptions.HTTPError as e:
                    if e.response and e.response.status_code in (400, 404):
                        logging.warning(f"{market} 매수 주문 실패 (무시 가능): {e}")
                    else:
                        logging.error(f"{market} 매수 주문 오류: {e}")
                except Exception as e:
                    logging.exception(f"{market} 매수 처리 예외 (계속 진행): {e}")

            for market, a in analyses.items():
                state_store.set_prev_base_total(market, a["snap"]["base_total"])
            
            # 부드러운 갱신 사용 시 출력 줄 수 추정
            if cfg.use_smooth_refresh and cfg.console_clear_each_loop:
                # 대략적인 출력 줄 수 계산: 테이블 + 로그 메시지
                # 스코어표: 헤더(1) + 구분선(1) + 데이터행(N) = N+2
                # 보유표: 헤더(1) + 구분선(1) + 데이터행(M) = M+2  
                # 기타 로그: 약 5~10줄
                prev_output_lines = len(score_rows) + 2 + len(holding_rows) + 2 + 10

            time.sleep(cfg.poll_interval_sec)

        except KeyboardInterrupt:
            logging.info("사용자 중단")
            if cfg.use_smooth_refresh:
                show_cursor()  # 커서 복원 (조건 단순화)
            break
        except requests.exceptions.HTTPError as e:
            # 서버 점검/장애 시 장기 백오프 (.env 설정 사용)
            if e.response and e.response.status_code in (502, 503, 504):
                wait_sec = cfg.backoff_5xx_gateway_sec
                logging.warning(f"서버 점검/장애 ({e.response.status_code}) | {wait_sec}초 대기")
            elif e.response and e.response.status_code == 500:
                wait_sec = cfg.backoff_500_sec
                logging.error(f"서버 내부 오류 (500) | {wait_sec}초 대기")
            elif e.response and e.response.status_code == 429:
                wait_sec = cfg.backoff_429_sec
                logging.warning(f"레이트 리밋 초과 (429) | {wait_sec}초 대기")
            else:
                wait_sec = max(cfg.poll_interval_sec, 5)
                logging.error(f"HTTP 오류: {e} | {wait_sec}초 후 재시도")
            time.sleep(wait_sec)
        except Exception as e:
            logging.exception("오류 발생: %s", e)
            wait_sec = max(cfg.poll_interval_sec, 5)
            logging.info("오류 복구 대기 | %.1f초 후 재시도", wait_sec)
            time.sleep(wait_sec)


if __name__ == "__main__":
    main()
