#!/usr/bin/env python3
"""
업비트 자동매매 백테스트 + 워크포워드 검증 도구
upbit_auto_trade.py 의 전략 로직을 그대로 재사용합니다.

사용법:
    # 1. 캔들 데이터 수집 (업비트 REST API)
    python backtest.py fetch --market KRW-BTC --unit 5 --count 2000 --out data/btc_5m.csv

    # 2. 단일 백테스트
    python backtest.py run --market KRW-BTC \
        --candle data/btc_5m.csv --htf-unit 60 \
        --take-profit 0.015 --stop-loss -0.012

    # 3. 파라미터 그리드 서치
    python backtest.py grid --market KRW-BTC --candle data/btc_5m.csv --htf-unit 60

    # 4. 워크포워드 검증 (WFA)
    python backtest.py wfa --market KRW-BTC --candle data/btc_5m.csv --htf-unit 60 \
        --is-ratio 0.7 --windows 5
"""
from __future__ import annotations

import argparse
import csv
import itertools
import inspect
import json
import math
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# upbit_auto_trade.py 와 같은 디렉토리에서 실행한다고 가정
sys.path.insert(0, str(Path(__file__).parent))
try:
    from upbit_auto_trade import (
        Config,
        build_tf_trend_snapshot,
        build_current_tf_filters,
        build_strategy_snapshot,
        compute_score,
        should_sell,
        estimate_net_pnl_pct,
        calc_buy_volume,
        calc_dynamic_buy_amount,
        check_partial_exit,
        required_candle_count,
        extract_candles_asc,
    )
except ImportError as e:
    print(f"오류: upbit_auto_trade.py 임포트 실패 — {e}")
    print("backtest.py 와 같은 디렉토리에 upbit_auto_trade.py 가 있어야 합니다.")
    sys.exit(1)


def _call_build_current_tf_filters(window_desc, current_price, cfg, best_bid, best_ask):
    sig = inspect.signature(build_current_tf_filters)
    if "orderbook" in sig.parameters:
        return build_current_tf_filters(window_desc, current_price, cfg, best_bid, best_ask, orderbook=None)
    return build_current_tf_filters(window_desc, current_price, cfg, best_bid, best_ask)


def _call_build_strategy_snapshot(current_tf, higher_tf, cooldown_active, cooldown_remaining_sec, market_warning):
    sig = inspect.signature(build_strategy_snapshot)
    if "btc_trend_ok" in sig.parameters:
        return build_strategy_snapshot(current_tf, higher_tf, cooldown_active, cooldown_remaining_sec, market_warning, btc_trend_ok=True)
    return build_strategy_snapshot(current_tf, higher_tf, cooldown_active, cooldown_remaining_sec, market_warning)


def _call_should_sell(
    cfg, snap, strat, hold_sec, buy_fee_rate, sell_fee_rate, current_price,
    max_price_since_entry=0.0, breakeven_activated=False
):
    """
    should_sell() 래퍼.
    시그니처 변경(2→3 반환값)에 안전하게 대응합니다.
    Returns: (sell_ok, sell_reason, new_breakeven_activated)
    """
    sig = inspect.signature(should_sell)
    kwargs = dict(
        cfg=cfg, snap=snap, strat=strat,
        hold_sec=hold_sec, buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate, current_price=current_price,
    )
    if "max_price_since_entry" in sig.parameters:
        kwargs["max_price_since_entry"] = max_price_since_entry
    if "breakeven_activated" in sig.parameters:
        kwargs["breakeven_activated"] = breakeven_activated

    result = should_sell(**kwargs)

    # 반환값이 3개(최신 버전)이면 그대로, 2개(구버전)이면 be_activated=False 보완
    if isinstance(result, tuple) and len(result) == 3:
        return result[0], result[1], result[2]
    elif isinstance(result, tuple) and len(result) == 2:
        return result[0], result[1], breakeven_activated
    return bool(result), "", breakeven_activated


# =============================================================================
# 데이터 구조
# =============================================================================
@dataclass
class TradeRecord:
    market: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    volume: float
    krw_amount: float
    exit_reason: str
    gross_pnl_krw: float
    fee_krw: float
    net_pnl_krw: float
    net_pnl_pct: float
    hold_candles: int


@dataclass
class BacktestResult:
    market: str
    candle_unit: int
    htf_candle_unit: int
    total_candles: int
    sim_start: str
    sim_end: str
    trades: list[TradeRecord] = field(default_factory=list)
    params: dict = field(default_factory=dict)

    # 성과 지표 (finalize() 호출 후 채워짐)
    n_trades: int = 0
    n_wins: int = 0
    n_losses: int = 0
    win_rate: float = 0.0
    total_net_pnl_krw: float = 0.0
    total_net_pnl_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    avg_hold_candles: float = 0.0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    profit_factor: float = 0.0

    def finalize(self, initial_capital: float = 1_000_000) -> None:
        if not self.trades:
            return
        self.n_trades = len(self.trades)
        wins   = [t for t in self.trades if t.net_pnl_krw > 0]
        losses = [t for t in self.trades if t.net_pnl_krw <= 0]
        self.n_wins   = len(wins)
        self.n_losses = len(losses)
        self.win_rate = self.n_wins / self.n_trades if self.n_trades else 0.0
        self.total_net_pnl_krw = sum(t.net_pnl_krw for t in self.trades)
        self.total_net_pnl_pct = self.total_net_pnl_krw / initial_capital * 100
        self.avg_hold_candles  = sum(t.hold_candles for t in self.trades) / self.n_trades

        gross_wins   = sum(t.net_pnl_krw for t in wins)
        gross_losses = abs(sum(t.net_pnl_krw for t in losses))
        self.profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")
        self.avg_win_pct  = (sum(t.net_pnl_pct for t in wins) / len(wins) * 100) if wins else 0.0
        self.avg_loss_pct = (sum(t.net_pnl_pct for t in losses) / len(losses) * 100) if losses else 0.0

        # 최대 낙폭 (MDD)
        equity = initial_capital
        peak   = equity
        max_dd = 0.0
        for t in self.trades:
            equity += t.net_pnl_krw
            peak = max(peak, equity)
            dd = (peak - equity) / peak if peak > 0 else 0.0
            max_dd = max(max_dd, dd)
        self.max_drawdown_pct = max_dd * 100

        # Sharpe ratio (거래별 수익률 기준, 연환산 없음)
        rets = [t.net_pnl_pct for t in self.trades]
        if len(rets) > 1:
            mean = sum(rets) / len(rets)
            std  = math.sqrt(sum((r - mean) ** 2 for r in rets) / len(rets))
            self.sharpe_ratio = (mean / std) * math.sqrt(len(rets)) if std > 0 else 0.0

    def summary(self) -> str:
        lines = [
            f"=== 백테스트 결과 [{self.market}] ===",
            f"기간    : {self.sim_start} ~ {self.sim_end}",
            f"캔들    : {self.total_candles}개 ({self.candle_unit}분/{self.htf_candle_unit}분)",
            f"거래횟수 : {self.n_trades} (승:{self.n_wins} 패:{self.n_losses})",
            f"승률     : {self.win_rate * 100:.1f}%",
            f"순수익    : {self.total_net_pnl_krw:+,.0f}원  ({self.total_net_pnl_pct:+.2f}%)",
            f"최대낙폭  : {self.max_drawdown_pct:.2f}%",
            f"Sharpe   : {self.sharpe_ratio:.3f}",
            f"Profit F : {self.profit_factor:.2f}",
            f"평균승  : {self.avg_win_pct:+.3f}%   평균패: {self.avg_loss_pct:+.3f}%",
            f"평균보유 : {self.avg_hold_candles:.1f}봉",
        ]
        if self.params:
            lines.append(f"파라미터 : {self.params}")
        return "\n".join(lines)


# =============================================================================
# 캔들 데이터 로더
# =============================================================================
class CandleLoader:
    BASE_URL = "https://api.upbit.com/v1/candles/minutes"

    @staticmethod
    def fetch(market: str, unit: int, total_count: int, out_path: str) -> list[dict]:
        """업비트 REST API로 과거 캔들 수집 후 CSV 저장."""
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        all_candles: list[dict] = []
        to_param: str | None = None
        batch = 200
        remaining = total_count

        print(f"[fetch] {market} {unit}분봉 {total_count}개 수집 중...")
        while remaining > 0:
            cnt = min(batch, remaining)
            params: dict = {"market": market, "count": cnt}
            if to_param:
                params["to"] = to_param
            try:
                resp = requests.get(
                    f"{CandleLoader.BASE_URL}/{unit}",
                    params=params,
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  API 오류: {e} — 5초 후 재시도")
                time.sleep(5)
                continue

            if not data:
                break
            all_candles.extend(data)
            remaining -= len(data)
            # 다음 배치의 to: 현재 배치 마지막(가장 오래된) 캔들보다 1분 이전
            last_dt = data[-1].get("candle_date_time_utc", "")
            if last_dt:
                to_param = last_dt
            else:
                break
            time.sleep(0.12)  # 레이트 리밋
            print(f"  수집: {len(all_candles)}/{total_count}", end="\r")

        print(f"\n[fetch] 완료: {len(all_candles)}개")

        # 시간 오름차순 정렬
        all_candles.sort(key=lambda c: c.get("candle_date_time_utc", ""))
        CandleLoader._save_csv(all_candles, out_path)
        return all_candles

    @staticmethod
    def _save_csv(candles: list[dict], path: str) -> None:
        if not candles:
            return
        keys = ["candle_date_time_utc", "opening_price", "high_price", "low_price",
                "trade_price", "candle_acc_trade_price", "candle_acc_trade_volume"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for c in candles:
                w.writerow({k: c.get(k, "") for k in keys})
        print(f"[save] {path} ({len(candles)}행)")

    @staticmethod
    def load_csv(path: str) -> list[dict]:
        """CSV → 시간 오름차순 캔들 리스트 반환."""
        candles: list[dict] = []
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                candles.append({
                    "candle_date_time_utc":   row["candle_date_time_utc"],
                    "opening_price":          float(row["opening_price"] or 0),
                    "high_price":             float(row["high_price"] or 0),
                    "low_price":              float(row["low_price"] or 0),
                    "trade_price":            float(row["trade_price"] or 0),
                    "candle_acc_trade_price": float(row["candle_acc_trade_price"] or 0),
                    "candle_acc_trade_volume":float(row.get("candle_acc_trade_volume") or 0),
                })
        candles.sort(key=lambda c: c["candle_date_time_utc"])
        return candles

    @staticmethod
    def resample(candles_asc: list[dict], from_unit: int, to_unit: int) -> list[dict]:
        """
        낮은 타임프레임 캔들을 높은 타임프레임으로 리샘플링.
        예: 5분봉 → 60분봉 (ratio = 12)
        look-ahead bias 없이 현재까지 완성된 봉만 생성합니다.
        """
        if to_unit % from_unit != 0:
            raise ValueError(f"to_unit({to_unit}) 이 from_unit({from_unit}) 의 배수여야 합니다.")
        ratio = to_unit // from_unit
        htf: list[dict] = []
        # --- ratio 단위로 그룹 묶기 ---
        groups: list[list[dict]] = []
        buf: list[dict] = []
        gap_warned = False
        for c in candles_asc:
            # 시간 공백 감지: buf가 비어 있지 않을 때 직전 봉과 시간 간격 확인
            if buf and not gap_warned:
                from datetime import datetime as _dt
                try:
                    prev_dt = _dt.fromisoformat(buf[-1]["candle_date_time_utc"].replace("Z", "+00:00"))
                    cur_dt  = _dt.fromisoformat(c["candle_date_time_utc"].replace("Z", "+00:00"))
                    expected_gap_sec = from_unit * 60
                    actual_gap_sec   = (cur_dt - prev_dt).total_seconds()
                    if abs(actual_gap_sec - expected_gap_sec) > expected_gap_sec * 0.5:
                        print(
                            f"[resample] 경고: 시간 공백 감지 "
                            f"({buf[-1]['candle_date_time_utc']} -> {c['candle_date_time_utc']}, "
                            f"예상 {expected_gap_sec}초, 실제 {actual_gap_sec:.0f}초). "
                            f"HTF 봉 품질이 저하될 수 있습니다."
                        )
                        gap_warned = True
                        buf = []  # 공백 발생 시 현재 버퍼를 버리고 새 봉부터 시작
                except Exception:
                    pass
            buf.append(c)
            if len(buf) == ratio:
                groups.append(buf)
                buf = []
        for g in groups:
            htf.append({
                "candle_date_time_utc":   g[0]["candle_date_time_utc"],
                "opening_price":          g[0]["opening_price"],
                "high_price":             max(x["high_price"] for x in g),
                "low_price":              min(x["low_price"] for x in g),
                "trade_price":            g[-1]["trade_price"],
                "candle_acc_trade_price": sum(x["candle_acc_trade_price"] for x in g),
                "candle_acc_trade_volume":sum(x["candle_acc_trade_volume"] for x in g),
            })
        return htf

    @staticmethod
    def resample_by_time_bucket(candles_asc: list[dict], from_unit: int, to_unit: int) -> list[dict]:
        """
        시각 기준 정시 버킷 리샘플링 (실거래 거래소 봉과 일치)
        예: 5분봉 → 60분봉, 14:23의 5분봉은 14:00~14:59 버킷에 포함
        """
        if to_unit % from_unit != 0:
            raise ValueError(f"to_unit({to_unit}) 이 from_unit({from_unit}) 의 배수여야 합니다.")
        
        buckets: dict[str, list[dict]] = {}
        
        for c in candles_asc:
            dt_str = c["candle_date_time_utc"]
            try:
                # ISO 형식 파싱
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            except Exception:
                # 파싱 실패 시 스킵
                continue
            
            # 정시 버킷 계산 (분 단위를 to_unit으로 내림)
            bucket_minute = (dt.minute // to_unit) * to_unit
            bucket_start = dt.replace(minute=bucket_minute, second=0, microsecond=0)
            bucket_key = bucket_start.isoformat()
            
            if bucket_key not in buckets:
                buckets[bucket_key] = []
            buckets[bucket_key].append(c)
        
        # 버킷별 OHLC 생성
        htf: list[dict] = []
        for bucket_time in sorted(buckets.keys()):
            group = buckets[bucket_time]
            if not group:
                continue
            htf.append({
                "candle_date_time_utc":   bucket_time,
                "opening_price":          group[0]["opening_price"],
                "high_price":             max(x["high_price"] for x in group),
                "low_price":              min(x["low_price"] for x in group),
                "trade_price":            group[-1]["trade_price"],
                "candle_acc_trade_price": sum(x["candle_acc_trade_price"] for x in group),
                "candle_acc_trade_volume":sum(x["candle_acc_trade_volume"] for x in group),
            })
        
        return htf


# =============================================================================
# 지정가 체결 시뮬레이터
# =============================================================================
@dataclass
class LimitOrder:
    """미체결 지정가 주문"""
    order_id: str
    market: str
    side: str  # "bid" or "ask"
    price: float
    volume: float
    submitted_at: int  # 캔들 인덱스
    status: str = "wait"  # "wait", "done", "cancelled"


class LimitOrderSimulator:
    """
    지정가 주문 체결 시뮬레이터
    - 실거래의 지정가 미체결, 재호가를 반영
    - 확률적 체결 모델 사용
    """
    
    def __init__(self, fill_rate: float = 0.7, reprice_delay_candles: int = 2):
        """
        Args:
            fill_rate: 가격 조건 만족 시 체결 확률 (0.7 = 70%)
            reprice_delay_candles: 재호가까지 대기 캔들 수
        """
        self.fill_rate = fill_rate
        self.reprice_delay = reprice_delay_candles
        self.pending_orders: dict[str, LimitOrder] = {}
        self.order_counter = 0
    
    def place_order(self, market: str, side: str, price: float, volume: float, candle_idx: int) -> str:
        """지정가 주문 제출"""
        self.order_counter += 1
        order_id = f"{market}_{side}_{candle_idx}_{self.order_counter}"
        
        order = LimitOrder(
            order_id=order_id,
            market=market,
            side=side,
            price=price,
            volume=volume,
            submitted_at=candle_idx,
        )
        
        self.pending_orders[order_id] = order
        return order_id
    
    def check_fill(self, order_id: str, current_candle: dict, candle_idx: int) -> tuple[bool, str, float | None]:
        """
        체결 여부 확인
        
        Returns:
            (체결여부, 상태메시지, 체결가격 or None)
            - (True, "filled", price): 체결됨
            - (False, "waiting", None): 대기 중
            - (False, "need_reprice", None): 재호가 필요
        """
        if order_id not in self.pending_orders:
            return False, "not_found", None
        
        order = self.pending_orders[order_id]
        
        if order.status != "wait":
            return False, order.status, None
        
        # 1. 가격 조건 확인
        can_fill = False
        fill_price = order.price
        
        if order.side == "bid":  # 매수 지정가
            # 지정가 >= 현재 저가면 체결 가능성 있음
            if order.price >= current_candle["low_price"]:
                can_fill = True
                # 실제 체결가는 지정가와 현재가 사이
                fill_price = min(order.price, current_candle["trade_price"])
        
        else:  # 매도 지정가
            # 지정가 <= 현재 고가면 체결 가능성 있음
            if order.price <= current_candle["high_price"]:
                can_fill = True
                fill_price = max(order.price, current_candle["trade_price"])
        
        # 2. 확률적 체결 (호가창 깊이를 간접 반영)
        if can_fill:
            if random.random() < self.fill_rate:
                order.status = "done"
                return True, "filled", fill_price
        
        # 3. 재호가 타이밍 체크
        wait_time = candle_idx - order.submitted_at
        if wait_time >= self.reprice_delay:
            return False, "need_reprice", None
        
        return False, "waiting", None
    
    def cancel_order(self, order_id: str) -> bool:
        """주문 취소"""
        if order_id in self.pending_orders:
            self.pending_orders[order_id].status = "cancelled"
            return True
        return False
    
    def get_pending_orders(self, market: str | None = None, side: str | None = None) -> list[LimitOrder]:
        """미체결 주문 조회"""
        orders = [o for o in self.pending_orders.values() if o.status == "wait"]
        
        if market:
            orders = [o for o in orders if o.market == market]
        if side:
            orders = [o for o in orders if o.side == side]
        
        return orders


# =============================================================================
# 백테스트 시뮬레이터
# =============================================================================
@dataclass
class Position:
    entry_time: str
    entry_price: float
    volume: float
    krw_amount: float
    entry_idx: int          # 현재 TF 기준 진입 캔들 인덱스
    max_price: float = 0.0
    pending_sell_reason: str = ""  # 지정가 매도 주문 제출 시 사유 보존 (체결 지연 대응)
    partial_exited: bool = False   # 부분 익절 완료 여부
    breakeven_activated: bool = False  # 브레이크이븐 스탑 활성화 여부


class BacktestSimulator:
    """
    단일 종목 단일 파라미터 세트 백테스트.
    look-ahead bias 제거:
      - 진입/청산 가격은 해당 시점 캔들의 close 를 사용합니다.
      - 지표 계산 창은 현재 캔들을 포함하지 않고 직전까지만 사용합니다.
    """
    DEFAULT_FEE = 0.0005  # 업비트 기본 수수료 (0.05%)

    def __init__(
        self,
        cfg: Config,
        market: str,
        candles_asc: list[dict],       # 낮은 TF (시간 오름차순)
        htf_candles_asc: list[dict],   # 높은 TF (시간 오름차순, 이미 리샘플된 것)
        initial_capital: float = 1_000_000,
        buy_fee: float = DEFAULT_FEE,
        sell_fee: float = DEFAULT_FEE,
        use_limit_order_sim: bool = False,  # 지정가 시뮬레이터 사용 여부
        limit_fill_rate: float = 0.7,        # 지정가 체결률
        signal_next_candle: bool = False,    # True시 신호는 i-1, 체결은 i
    ):
        self.cfg = cfg
        self.market = market
        self.candles = candles_asc
        self.htf_candles = htf_candles_asc
        self.initial_capital = initial_capital
        self.buy_fee = buy_fee
        self.sell_fee = sell_fee
        self.use_limit_order_sim = use_limit_order_sim
        self.signal_next_candle = signal_next_candle
        
        if use_limit_order_sim:
            self.limit_sim = LimitOrderSimulator(fill_rate=limit_fill_rate, reprice_delay_candles=2)
        else:
            self.limit_sim = None

    def _htf_window_at(self, cur_idx: int) -> list[dict]:
        """cur_idx 시점(포함)까지의 HTF 창 반환. DESC 순서."""
        # 현재 LTF 인덱스에 대응하는 HTF 인덱스 찾기 (시간 기준 매칭)
        cur_dt = self.candles[cur_idx]["candle_date_time_utc"]
        htf_up_to = [c for c in self.htf_candles if c["candle_date_time_utc"] <= cur_dt]
        if not htf_up_to:
            return []
        window = htf_up_to[-self.cfg.htf_candle_count:]
        return list(reversed(window))  # DESC

    def run(self) -> BacktestResult:
        cfg = self.cfg
        candles = self.candles
        n = len(candles)

        cur_required = required_candle_count(cfg.ma_short_period, cfg.ma_long_period, cfg.rsi_period)
        htf_required = required_candle_count(cfg.htf_ma_short_period, cfg.htf_ma_long_period)
        warmup = max(cur_required, cfg.candle_count) + 1

        result = BacktestResult(
            market=self.market,
            candle_unit=cfg.candle_unit,
            htf_candle_unit=cfg.htf_candle_unit,
            total_candles=n,
            sim_start=candles[warmup]["candle_date_time_utc"] if warmup < n else "",
            sim_end=candles[-1]["candle_date_time_utc"] if candles else "",
            params=self._params_snapshot(),
        )

        position: Position | None = None
        cooldown_until_idx = 0
        simulated_half_spread = 0.0001  # 0.01% 한쪽 스프레드 가정

        # 지정가 체결 추적용
        pending_buy_order_id: str | None = None
        pending_sell_order_id: str | None = None

        # signal_next_candle 용: 이전 봉의 스코어를 보존
        prev_score: float = 0.0
        prev_score_reason: str = ""

        # 서킷브레이커 시뮬레이션
        cb_stop_times: list[int] = []   # 손절 발생 캔들 인덱스 목록
        cb_block_until_idx: int = 0     # 서킷브레이커 해제 캔들 인덱스
        cb_window_candles = (
            getattr(cfg, "circuit_breaker_window_sec", 3600) // (cfg.candle_unit * 60)
        ) if getattr(cfg, "circuit_breaker_enabled", False) else 0

        for i in range(warmup, n):
            c = candles[i]
            current_price = float(c["trade_price"])
            if current_price <= 0:
                continue

            # ── 현재 봉 기준 스프레드 (모든 모드 공통) ────────────────────────
            best_bid = current_price * (1 - simulated_half_spread)
            best_ask = current_price * (1 + simulated_half_spread)

            # ── signal_next_candle: 신호 계산 기준 봉 결정 ────────────────────
            # False(기본): 현재 봉(i)으로 신호 계산, 현재 close로 체결
            # True        : 전 봉(i-1) 신호를 사용, 현재 봉 시가(opening_price)로 체결
            if self.signal_next_candle:
                # 진입/청산 체결가는 현재 봉의 시가 기준
                open_price = float(c.get("opening_price") or current_price)
                exec_best_ask = open_price * (1 + simulated_half_spread)
                exec_best_bid = open_price * (1 - simulated_half_spread)
            else:
                exec_best_ask = best_ask
                exec_best_bid = best_bid

            # ── 현재 봉 직전까지의 창 (DESC) — 신호 계산용 ───────────────────
            window_desc = list(reversed(candles[max(0, i - cfg.candle_count):i]))
            if len(window_desc) < cur_required:
                prev_score = 0.0
                prev_score_reason = ""
                continue

            htf_window_desc = self._htf_window_at(i - 1)  # 직전까지만
            if len(htf_window_desc) < htf_required:
                prev_score = 0.0
                prev_score_reason = ""
                continue

            current_tf = _call_build_current_tf_filters(window_desc, current_price, cfg, best_bid, best_ask)
            higher_tf = build_tf_trend_snapshot(
                htf_window_desc, current_price,
                cfg.htf_ma_short_period, cfg.htf_ma_long_period,
            )
            strat = _call_build_strategy_snapshot(
                current_tf=current_tf,
                higher_tf=higher_tf,
                cooldown_active=False,
                cooldown_remaining_sec=0.0,
                market_warning="NONE",
            )

            # ── 현재 봉의 스코어 계산 (다음 봉 signal_next_candle용으로도 저장) ─
            score, score_reason = compute_score(cfg, strat)

            # signal_next_candle 분기: 실제 진입 판단에 쓸 스코어 결정
            if self.signal_next_candle:
                effective_score = prev_score
                effective_score_reason = prev_score_reason
            else:
                effective_score = score
                effective_score_reason = score_reason

            # ── 지정가 체결 확인 (use_limit_order_sim=True 시) ────────────────
            if self.use_limit_order_sim and self.limit_sim:
                # 매수 주문 체결 확인
                if pending_buy_order_id and position is None:
                    filled, status, fill_price = self.limit_sim.check_fill(pending_buy_order_id, c, i)
                    if filled and fill_price:
                        order = self.limit_sim.pending_orders[pending_buy_order_id]
                        position = Position(
                            entry_time=c["candle_date_time_utc"],
                            entry_price=fill_price,
                            volume=order.volume,
                            krw_amount=fill_price * order.volume,
                            entry_idx=i,
                            max_price=fill_price,
                        )
                        pending_buy_order_id = None
                    elif status == "need_reprice":
                        self.limit_sim.cancel_order(pending_buy_order_id)
                        pending_buy_order_id = None

                # 매도 주문 체결 확인
                if pending_sell_order_id and position is not None:
                    filled, status, fill_price = self.limit_sim.check_fill(pending_sell_order_id, c, i)
                    if filled and fill_price:
                        exit_price = fill_price
                        # ▶ 주문 제출 시 보존해 둔 원래 sell_reason 사용
                        original_sell_reason = position.pending_sell_reason or "지정가 체결"
                        gross_pnl = (exit_price - position.entry_price) * position.volume
                        fee_krw = (position.krw_amount * self.buy_fee
                                   + exit_price * position.volume * self.sell_fee)
                        net_pnl = gross_pnl - fee_krw
                        net_pct = estimate_net_pnl_pct(
                            current_price=exit_price,
                            avg_buy_price=position.entry_price,
                            buy_fee_rate=self.buy_fee,
                            sell_fee_rate=self.sell_fee,
                        )
                        result.trades.append(TradeRecord(
                            market=self.market,
                            entry_time=position.entry_time,
                            exit_time=c["candle_date_time_utc"],
                            entry_price=position.entry_price,
                            exit_price=exit_price,
                            volume=position.volume,
                            krw_amount=position.krw_amount,
                            exit_reason=original_sell_reason[:80],
                            gross_pnl_krw=gross_pnl,
                            fee_krw=fee_krw,
                            net_pnl_krw=net_pnl,
                            net_pnl_pct=net_pct,
                            hold_candles=i - position.entry_idx,
                        ))
                        # 쿨다운: 원래 사유 기반으로 판단
                        is_stop = "손절" in original_sell_reason
                        cooldown_candles = (
                            cfg.cooldown_after_stop_loss_sec
                            if is_stop else cfg.cooldown_after_exit_sec
                        ) // (cfg.candle_unit * 60)
                        cooldown_until_idx = i + max(1, cooldown_candles)
                        position = None
                        pending_sell_order_id = None
                    elif status == "need_reprice":
                        self.limit_sim.cancel_order(pending_sell_order_id)
                        pending_sell_order_id = None

            # ── 포지션 보유 중: 매도 판단 ─────────────────────────────────────
            if position is not None:
                position.max_price = max(position.max_price, current_price)
                hold_sec = (i - position.entry_idx) * cfg.candle_unit * 60
                snap = {
                    "base_total":    position.volume,
                    "base_balance":  position.volume,
                    "base_locked":   0.0,
                    "avg_buy_price": position.entry_price,
                    "position_krw":  position.volume * current_price,
                    "pnl_pct":       (current_price - position.entry_price) / position.entry_price,
                }

                # ── 부분 익절 시뮬레이션 ─────────────────────────────────────
                p_ok, p_ratio, p_reason = check_partial_exit(
                    cfg=cfg,
                    snap=snap,
                    buy_fee_rate=self.buy_fee,
                    sell_fee_rate=self.sell_fee,
                    current_price=current_price,
                    already_partial_exited=position.partial_exited,
                )
                if p_ok:
                    partial_vol = round(position.volume * p_ratio, cfg.volume_decimals)
                    if partial_vol > 0:
                        partial_exit_price = exec_best_bid
                        partial_krw  = position.krw_amount * p_ratio
                        p_gross = (partial_exit_price - position.entry_price) * partial_vol
                        p_fee   = partial_krw * self.buy_fee + partial_exit_price * partial_vol * self.sell_fee
                        p_net   = p_gross - p_fee
                        p_pct   = estimate_net_pnl_pct(
                            current_price=partial_exit_price,
                            avg_buy_price=position.entry_price,
                            buy_fee_rate=self.buy_fee,
                            sell_fee_rate=self.sell_fee,
                        )
                        result.trades.append(TradeRecord(
                            market=self.market,
                            entry_time=position.entry_time,
                            exit_time=c["candle_date_time_utc"],
                            entry_price=position.entry_price,
                            exit_price=partial_exit_price,
                            volume=partial_vol,
                            krw_amount=partial_krw,
                            exit_reason=p_reason[:80],
                            gross_pnl_krw=p_gross,
                            fee_krw=p_fee,
                            net_pnl_krw=p_net,
                            net_pnl_pct=p_pct,
                            hold_candles=i - position.entry_idx,
                        ))
                        # 포지션 수량·투자금 감소
                        remaining_vol = position.volume - partial_vol
                        position.volume    = remaining_vol
                        position.krw_amount = position.entry_price * remaining_vol
                        position.partial_exited = True

                sell_ok, sell_reason, new_be_activated = _call_should_sell(
                    cfg=cfg, snap=snap, strat=strat,
                    hold_sec=hold_sec,
                    buy_fee_rate=self.buy_fee,
                    sell_fee_rate=self.sell_fee,
                    current_price=current_price,
                    max_price_since_entry=position.max_price,
                    breakeven_activated=position.breakeven_activated,
                )
                # 브레이크이븐 활성화 상태 포지션에 저장
                if new_be_activated and not position.breakeven_activated:
                    position.breakeven_activated = True

                if sell_ok:
                    if self.use_limit_order_sim and self.limit_sim and not pending_sell_order_id:
                        # ▶ 지정가 주문 제출 — sell_reason을 position에 보존
                        position.pending_sell_reason = sell_reason
                        pending_sell_order_id = self.limit_sim.place_order(
                            market=self.market,
                            side="ask",
                            price=exec_best_bid,
                            volume=position.volume,
                            candle_idx=i,
                        )
                    else:
                        # 즉시 체결 모델
                        exit_price = exec_best_bid
                        gross_pnl = (exit_price - position.entry_price) * position.volume
                        fee_krw   = (position.krw_amount * self.buy_fee
                                     + exit_price * position.volume * self.sell_fee)
                        net_pnl   = gross_pnl - fee_krw
                        net_pct   = estimate_net_pnl_pct(
                            current_price=exit_price,
                            avg_buy_price=position.entry_price,
                            buy_fee_rate=self.buy_fee,
                            sell_fee_rate=self.sell_fee,
                        )
                        result.trades.append(TradeRecord(
                            market=self.market,
                            entry_time=position.entry_time,
                            exit_time=c["candle_date_time_utc"],
                            entry_price=position.entry_price,
                            exit_price=exit_price,
                            volume=position.volume,
                            krw_amount=position.krw_amount,
                            exit_reason=sell_reason[:80],
                            gross_pnl_krw=gross_pnl,
                            fee_krw=fee_krw,
                            net_pnl_krw=net_pnl,
                            net_pnl_pct=net_pct,
                            hold_candles=i - position.entry_idx,
                        ))
                        is_stop = "손절" in sell_reason
                        cooldown_candles = (
                            cfg.cooldown_after_stop_loss_sec
                            if is_stop else cfg.cooldown_after_exit_sec
                        ) // (cfg.candle_unit * 60)
                        cooldown_until_idx = i + max(1, cooldown_candles)
                        position = None

                        # ── 서킷브레이커 카운팅 ────────────────────────────
                        if is_stop and cb_window_candles > 0:
                            cb_stop_times = [t for t in cb_stop_times if i - t <= cb_window_candles]
                            cb_stop_times.append(i)
                            if len(cb_stop_times) >= getattr(cfg, "circuit_breaker_max_stops", 3):
                                cb_cooldown = (
                                    getattr(cfg, "circuit_breaker_cooldown_sec", 7200)
                                    // (cfg.candle_unit * 60)
                                )
                                cb_block_until_idx = i + max(1, cb_cooldown)
                                cb_stop_times = []  # 카운터 리셋

                # ── 다음 루프를 위해 prev 갱신 후 매수 판단 스킵 ───────────────
                prev_score = score
                prev_score_reason = score_reason
                continue

            # ── 쿨다운 중이면 스킵 ────────────────────────────────────────────
            if i < cooldown_until_idx:
                prev_score = score
                prev_score_reason = score_reason
                continue

            # ── 서킷브레이커 발동 중이면 신규 진입 차단 ──────────────────────
            if cb_window_candles > 0 and i < cb_block_until_idx:
                prev_score = score
                prev_score_reason = score_reason
                continue

            # ── 포지션 없음: 매수 판단 ────────────────────────────────────────
            # signal_next_candle=True 시 effective_score 는 전 봉 스코어
            min_score = getattr(cfg, "min_entry_score", 0.0)  # 0이면 기존 score>0 동작 유지
            entry_threshold = min_score if min_score > 0 else 0.0
            if effective_score >= entry_threshold and effective_score > 0:
                # ATR 기반 동적 포지션 사이징
                atr_val = strat.get("atr", 0.0)
                dynamic_buy_krw = calc_dynamic_buy_amount(cfg, current_price, atr_val)

                entry_price = exec_best_ask
                volume = calc_buy_volume(dynamic_buy_krw, entry_price, cfg.volume_decimals)
                if volume > 0 and entry_price * volume >= cfg.min_order_krw:
                    if self.use_limit_order_sim and self.limit_sim and not pending_buy_order_id:
                        pending_buy_order_id = self.limit_sim.place_order(
                            market=self.market,
                            side="bid",
                            price=exec_best_ask,
                            volume=volume,
                            candle_idx=i,
                        )
                    else:
                        position = Position(
                            entry_time=c["candle_date_time_utc"],
                            entry_price=entry_price,
                            volume=volume,
                            krw_amount=entry_price * volume,
                            entry_idx=i,
                            max_price=entry_price,
                        )

            # ── 다음 루프를 위해 prev 갱신 ────────────────────────────────────
            prev_score = score
            prev_score_reason = score_reason

        # ── 마지막 봉에서 미청산 포지션 강제 청산 ────────────────────────────
        if position is not None and candles:
            last = candles[-1]
            exit_price = float(last["trade_price"])
            gross_pnl = (exit_price - position.entry_price) * position.volume
            fee_krw   = (position.krw_amount * self.buy_fee
                         + exit_price * position.volume * self.sell_fee)
            net_pnl   = gross_pnl - fee_krw
            net_pct   = estimate_net_pnl_pct(
                current_price=exit_price,
                avg_buy_price=position.entry_price,
                buy_fee_rate=self.buy_fee,
                sell_fee_rate=self.sell_fee,
            )
            result.trades.append(TradeRecord(
                market=self.market,
                entry_time=position.entry_time,
                exit_time=last["candle_date_time_utc"],
                entry_price=position.entry_price,
                exit_price=exit_price,
                volume=position.volume,
                krw_amount=position.krw_amount,
                exit_reason="시뮬레이션 종료 강제청산",
                gross_pnl_krw=gross_pnl,
                fee_krw=fee_krw,
                net_pnl_krw=net_pnl,
                net_pnl_pct=net_pct,
                hold_candles=len(candles) - 1 - position.entry_idx,
            ))

        result.finalize(self.initial_capital)
        return result

    def _params_snapshot(self) -> dict:
        cfg = self.cfg
        data = {
            "take_profit": cfg.take_profit_pct,
            "stop_loss": cfg.stop_loss_pct,
            "rsi_min": cfg.rsi_buy_min,
            "rsi_max": cfg.rsi_buy_max,
            "ma_short": cfg.ma_short_period,
            "ma_long": cfg.ma_long_period,
            "volume_min_ratio": cfg.volume_min_ratio,
        }
        if hasattr(cfg, "trailing_activate_pct"):
            data["trailing_activate"] = cfg.trailing_activate_pct
        if hasattr(cfg, "trailing_stop_pct"):
            data["trailing_stop"] = cfg.trailing_stop_pct
        if hasattr(cfg, "use_trailing_stop"):
            data["use_trailing"] = cfg.use_trailing_stop
        if hasattr(cfg, "volume_max_ratio"):
            data["volume_max_ratio"] = cfg.volume_max_ratio
        return data


# =============================================================================
# 성과 보고서 출력
# =============================================================================
def save_trades_csv(result: BacktestResult, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fields = [f.name for f in TradeRecord.__dataclass_fields__.values()]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for t in result.trades:
            w.writerow({k: getattr(t, k) for k in fields})
    print(f"[save] 거래 내역 → {path}")


def save_result_json(result: BacktestResult, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    d = {k: v for k, v in result.__dict__.items() if k != "trades"}
    d["trades"] = [t.__dict__ for t in result.trades]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    print(f"[save] 결과 JSON → {path}")


# =============================================================================
# 그리드 서치
# =============================================================================
GRID_PARAMS = {
    "take_profit_pct":           [0.010, 0.015, 0.020, 0.025],
    "stop_loss_pct":             [-0.008, -0.012, -0.015],
    "rsi_buy_min":               [30, 35, 40],
    "rsi_buy_max":               [60, 65, 70],
    "volume_min_ratio":          [0.5, 1.0, 1.5],
    "pullback_max_below_ma_pct": [0.010, 0.015, 0.020],
    "ma_short_period":           [10, 20],
    "ma_long_period":            [40, 60],
    "min_entry_score":           [50.0, 60.0, 70.0],
}


def run_grid_search(
    market: str,
    candles_asc: list[dict],
    htf_candles_asc: list[dict],
    candle_unit: int,
    htf_unit: int,
    initial_capital: float = 1_000_000,
    out_dir: str = "results",
    max_combos: int = 200,
) -> list[BacktestResult]:
    keys = list(GRID_PARAMS.keys())
    combos = list(itertools.product(*[GRID_PARAMS[k] for k in keys]))
    if len(combos) > max_combos:
        import random
        random.shuffle(combos)
        combos = combos[:max_combos]
        print(f"[grid] 총 {len(combos)}개 조합 중 {max_combos}개 무작위 샘플")
    else:
        print(f"[grid] 총 {len(combos)}개 조합 실행")

    results: list[BacktestResult] = []
    for idx, combo in enumerate(combos, 1):
        overrides = dict(zip(keys, combo))
        # rsi 범위 논리적 유효성 체크
        if overrides.get("rsi_buy_min", 35) >= overrides.get("rsi_buy_max", 65):
            continue
        cfg = _make_cfg(candle_unit, htf_unit, overrides)
        sim = BacktestSimulator(cfg, market, candles_asc, htf_candles_asc,
                                initial_capital=initial_capital)
        r = sim.run()
        results.append(r)
        print(f"  [{idx}/{len(combos)}] trades={r.n_trades} wr={r.win_rate*100:.0f}% "
              f"pnl={r.total_net_pnl_pct:+.2f}% mdd={r.max_drawdown_pct:.2f}% "
              f"sharpe={r.sharpe_ratio:.2f}  {overrides}")

    # 복합 점수 기준 정렬: Sharpe / (1 + MDD/100)
    results.sort(
        key=lambda r: (r.sharpe_ratio / (1 + r.max_drawdown_pct / 100)) if r.n_trades >= 5 else -999,
        reverse=True,
    )
    print("\n=== 상위 5개 파라미터 세트 ===")
    for r in results[:5]:
        print(r.summary())
        print()

    # CSV 저장
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    grid_csv = f"{out_dir}/grid_{market.replace('-','_')}.csv"
    with open(grid_csv, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["trades","win_rate","total_net_pnl_pct","max_drawdown_pct",
                      "sharpe_ratio","profit_factor","params"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow({
                "trades":             r.n_trades,
                "win_rate":           f"{r.win_rate*100:.1f}",
                "total_net_pnl_pct":  f"{r.total_net_pnl_pct:+.3f}",
                "max_drawdown_pct":   f"{r.max_drawdown_pct:.3f}",
                "sharpe_ratio":       f"{r.sharpe_ratio:.3f}",
                "profit_factor":      f"{r.profit_factor:.2f}",
                "params":             json.dumps(r.params, ensure_ascii=False),
            })
    print(f"[save] 그리드 결과 → {grid_csv}")
    return results


# =============================================================================
# 워크포워드 분석 (WFA)
# =============================================================================
@dataclass
class WFAWindow:
    window_no: int
    is_start: int   # in-sample 시작 인덱스
    is_end:   int   # in-sample 종료 인덱스 (exclusive)
    oos_start: int  # out-of-sample 시작
    oos_end:   int  # out-of-sample 종료 (exclusive)
    best_is_params: dict
    is_result: BacktestResult | None = None
    oos_result: BacktestResult | None = None


def run_wfa(
    market: str,
    candles_asc: list[dict],
    htf_candles_asc: list[dict],
    candle_unit: int,
    htf_unit: int,
    n_windows: int = 5,
    is_ratio: float = 0.7,
    initial_capital: float = 1_000_000,
    out_dir: str = "results",
) -> list[WFAWindow]:
    """
    워크포워드 검증.
    전체 데이터를 n_windows개 앵커 윈도우로 분할합니다.
    각 윈도우: 앞 is_ratio 로 그리드 최적화 → 뒤 (1-is_ratio) 로 OOS 검증
    """
    n = len(candles_asc)
    window_size = n // n_windows
    if window_size < 200:
        print(f"[wfa] 경고: 윈도우당 캔들 수({window_size})가 너무 적습니다.")

    windows: list[WFAWindow] = []
    for w_idx in range(n_windows):
        w_start = w_idx * window_size
        w_end   = w_start + window_size if w_idx < n_windows - 1 else n
        split   = w_start + int((w_end - w_start) * is_ratio)

        print(f"\n{'='*60}")
        print(f"WFA 윈도우 {w_idx+1}/{n_windows} "
              f"IS [{w_start}:{split}] OOS [{split}:{w_end}]")
        print(f"  IS 기간 : {candles_asc[w_start]['candle_date_time_utc']} ~ "
              f"{candles_asc[split-1]['candle_date_time_utc']}")
        print(f"  OOS 기간: {candles_asc[split]['candle_date_time_utc']} ~ "
              f"{candles_asc[w_end-1]['candle_date_time_utc']}")

        # IS: 그리드 서치로 최적 파라미터 찾기
        is_candles  = candles_asc[w_start:split]
        # HTF는 워밍업을 위해 윈도우 시작 이전 이력도 포함한다.
        is_htf      = [c for c in htf_candles_asc
                       if c["candle_date_time_utc"] <= candles_asc[split-1]["candle_date_time_utc"]]
        print(f"  IS 그리드 서치 시작 (캔들 {len(is_candles)}개)...")
        is_results = run_grid_search(
            market, is_candles, is_htf, candle_unit, htf_unit,
            initial_capital=initial_capital,
            out_dir=f"{out_dir}/wfa_w{w_idx+1}",
            max_combos=100,
        )
        best = is_results[0] if is_results else None
        best_params = best.params if best else {}

        # OOS: 최적 파라미터로 OOS 검증
        oos_candles = candles_asc[split:w_end]
        # OOS 구간 시뮬레이터에도 OOS 시작 이전의 HTF 이력을 함께 전달한다.
        # 이는 look-ahead가 아니라 이동평균 등 지표의 초반 워밍업을 위한 것이며,
        # BacktestSimulator 내부의 _htf_window_at(i-1) 시간 필터가 실제 OOS 기간만 사용하도록 보장한다.
        oos_htf     = [c for c in htf_candles_asc
                       if c["candle_date_time_utc"] <= candles_asc[w_end-1]["candle_date_time_utc"]]
        oos_overrides = {k: v for k, v in best_params.items() if k in GRID_PARAMS}
        oos_cfg = _make_cfg(candle_unit, htf_unit, oos_overrides)
        print(f"\n  OOS 검증 (캔들 {len(oos_candles)}개, 최적 파라미터: {oos_overrides})...")
        oos_sim = BacktestSimulator(oos_cfg, market, oos_candles, oos_htf,
                                    initial_capital=initial_capital)
        oos_r = oos_sim.run()
        print(oos_r.summary())

        wfa_win = WFAWindow(
            window_no=w_idx + 1,
            is_start=w_start, is_end=split,
            oos_start=split,  oos_end=w_end,
            best_is_params=best_params,
            is_result=best,
            oos_result=oos_r,
        )
        windows.append(wfa_win)

    # WFA 요약 출력
    print(f"\n{'='*60}")
    print("워크포워드 요약")
    print(f"{'윈도우':>4} {'IS수익':>8} {'OOS수익':>8} {'IS Sharpe':>10} "
          f"{'OOS Sharpe':>10} {'IS MDD':>8} {'OOS MDD':>8}")
    for w in windows:
        is_pnl  = f"{w.is_result.total_net_pnl_pct:+.2f}%"  if w.is_result  else "N/A"
        oos_pnl = f"{w.oos_result.total_net_pnl_pct:+.2f}%" if w.oos_result else "N/A"
        is_sh   = f"{w.is_result.sharpe_ratio:.2f}"          if w.is_result  else "N/A"
        oos_sh  = f"{w.oos_result.sharpe_ratio:.2f}"         if w.oos_result else "N/A"
        is_mdd  = f"{w.is_result.max_drawdown_pct:.2f}%"     if w.is_result  else "N/A"
        oos_mdd = f"{w.oos_result.max_drawdown_pct:.2f}%"    if w.oos_result else "N/A"
        print(f"{w.window_no:>4} {is_pnl:>8} {oos_pnl:>8} {is_sh:>10} "
              f"{oos_sh:>10} {is_mdd:>8} {oos_mdd:>8}")

    # IS/OOS 수익 상관 체크 (과적합 판단)
    is_pnls  = [w.is_result.total_net_pnl_pct  for w in windows if w.is_result]
    oos_pnls = [w.oos_result.total_net_pnl_pct for w in windows if w.oos_result]
    if len(is_pnls) >= 2:
        mean_is  = sum(is_pnls)  / len(is_pnls)
        mean_oos = sum(oos_pnls) / len(oos_pnls)
        cov = sum((i-mean_is)*(o-mean_oos) for i,o in zip(is_pnls,oos_pnls)) / len(is_pnls)
        std_is  = math.sqrt(sum((x-mean_is) **2 for x in is_pnls)  / len(is_pnls))
        std_oos = math.sqrt(sum((x-mean_oos)**2 for x in oos_pnls) / len(oos_pnls))
        corr = cov / (std_is * std_oos) if std_is > 0 and std_oos > 0 else 0.0
        print(f"\nIS/OOS 수익 상관계수: {corr:.3f}")
        if corr < 0.3:
            print("  ⚠  상관이 낮습니다. 파라미터가 과적합되어 있을 가능성이 있습니다.")
        elif corr > 0.7:
            print("  ✓  IS/OOS 수익이 일관됩니다. 전략 견고성이 양호합니다.")
        else:
            print("  △  중간 수준의 일관성. 추가 검증을 권장합니다.")

    return windows


# =============================================================================
# 헬퍼
# =============================================================================
def _make_cfg(candle_unit: int, htf_unit: int, overrides: dict) -> Config:
    """
    Config 인스턴스 생성 후 setattr 로 값을 직접 주입합니다.
    Config 필드 기본값은 모듈 임포트 시 os.getenv()로 평가되어 고정되므로,
    임포트 후 os.environ 변경이 반영되지 않습니다. setattr 방식으로 우회합니다.
    """
    os.environ.setdefault("UPBIT_ACCESS_KEY", "dummy_key_for_backtest")
    os.environ.setdefault("UPBIT_SECRET_KEY", "dummy_secret_for_backtest")
    os.environ.setdefault("USE_DYNAMIC_MARKETS", "false")
    os.environ.setdefault("MULTI_MARKETS", "KRW-BTC")
    cfg = Config()
    cfg.candle_unit     = candle_unit
    cfg.htf_candle_unit = htf_unit
    # 구버전 live 파일과의 호환 기본값 보강
    compat_defaults = {
        "use_trailing_stop": False,
        "trailing_activate_pct": 0.010,
        "trailing_stop_pct": 0.008,
        "trailing_overrides_take_profit": True,
        "volume_max_ratio": 0.0,
        "use_btc_filter": False,
        "btc_filter_skip_on_down": True,
    }
    for k, v in compat_defaults.items():
        if not hasattr(cfg, k):
            setattr(cfg, k, v)
    # 필드 타입에 맞게 변환하여 직접 주입
    for k, v in overrides.items():
        if not hasattr(cfg, k):
            continue
        cur = getattr(cfg, k)
        try:
            setattr(cfg, k, type(cur)(v))
        except (TypeError, ValueError):
            setattr(cfg, k, v)
    return cfg


def _load_data(candle_path: str, htf_unit: int, candle_unit: int, use_time_bucket: bool = True) -> tuple[list[dict], list[dict]]:
    """
    캔들 데이터 로드 및 HTF 리샘플링
    
    Args:
        use_time_bucket: True시 정시 기준 버킷 리샘플링 (실거래 일치), False시 단순 그룹핑
    """
    print(f"[load] {candle_path}")
    candles_asc = CandleLoader.load_csv(candle_path)
    print(f"  {len(candles_asc)}개 봉 로드 완료")
    
    if use_time_bucket:
        print(f"[resample] {candle_unit}분 → {htf_unit}분 (정시 기준 버킷)...")
        htf_candles_asc = CandleLoader.resample_by_time_bucket(candles_asc, candle_unit, htf_unit)
    else:
        print(f"[resample] {candle_unit}분 → {htf_unit}분 (단순 그룹핑)...")
        htf_candles_asc = CandleLoader.resample(candles_asc, candle_unit, htf_unit)
    
    print(f"  {len(htf_candles_asc)}개 HTF 봉 생성")
    return candles_asc, htf_candles_asc


# =============================================================================
# CLI
# =============================================================================
def cmd_fetch(args: argparse.Namespace) -> None:
    CandleLoader.fetch(
        market=args.market,
        unit=args.unit,
        total_count=args.count,
        out_path=args.out,
    )


def cmd_run(args: argparse.Namespace) -> None:
    candles_asc, htf_candles_asc = _load_data(args.candle, args.htf_unit, args.unit)
    overrides: dict = {}
    if args.take_profit is not None: overrides["take_profit_pct"]   = args.take_profit
    if args.stop_loss   is not None: overrides["stop_loss_pct"]     = args.stop_loss
    if args.rsi_min     is not None: overrides["rsi_buy_min"]       = args.rsi_min
    if args.rsi_max     is not None: overrides["rsi_buy_max"]       = args.rsi_max
    cfg = _make_cfg(args.unit, args.htf_unit, overrides)
    sim = BacktestSimulator(
        cfg, args.market, candles_asc, htf_candles_asc,
        initial_capital=args.capital,
        use_limit_order_sim=args.use_limit_order_sim,
        limit_fill_rate=args.limit_fill_rate,
        signal_next_candle=args.signal_next_candle,
    )
    result = sim.run()
    print(result.summary())
    out_dir = args.out_dir
    save_trades_csv(result, f"{out_dir}/trades_{args.market.replace('-','_')}.csv")
    save_result_json(result, f"{out_dir}/result_{args.market.replace('-','_')}.json")


def cmd_grid(args: argparse.Namespace) -> None:
    candles_asc, htf_candles_asc = _load_data(args.candle, args.htf_unit, args.unit)
    run_grid_search(
        market=args.market,
        candles_asc=candles_asc,
        htf_candles_asc=htf_candles_asc,
        candle_unit=args.unit,
        htf_unit=args.htf_unit,
        initial_capital=args.capital,
        out_dir=args.out_dir,
        max_combos=args.max_combos,
    )


def cmd_wfa(args: argparse.Namespace) -> None:
    candles_asc, htf_candles_asc = _load_data(args.candle, args.htf_unit, args.unit)
    run_wfa(
        market=args.market,
        candles_asc=candles_asc,
        htf_candles_asc=htf_candles_asc,
        candle_unit=args.unit,
        htf_unit=args.htf_unit,
        n_windows=args.windows,
        is_ratio=args.is_ratio,
        initial_capital=args.capital,
        out_dir=args.out_dir,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="업비트 자동매매 백테스트 & 워크포워드 검증",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- fetch ---
    pf = sub.add_parser("fetch", help="업비트에서 캔들 데이터 수집")
    pf.add_argument("--market",  required=True, help="예: KRW-BTC")
    pf.add_argument("--unit",    type=int, default=5, help="분봉 단위 (기본 5)")
    pf.add_argument("--count",   type=int, default=2000, help="수집할 캔들 수 (기본 2000)")
    pf.add_argument("--out",     default="data/candles.csv", help="저장 경로")
    pf.set_defaults(func=cmd_fetch)

    # 공통 인수
    def add_common(px):
        px.add_argument("--market",   required=True)
        px.add_argument("--candle",   required=True, help="CSV 파일 경로")
        px.add_argument("--unit",     type=int, default=5,  help="낮은 TF 분봉 (기본 5)")
        px.add_argument("--htf-unit", type=int, default=60, help="높은 TF 분봉 (기본 60)")
        px.add_argument("--capital",  type=float, default=1_000_000, help="초기 자금 KRW (기본 100만)")
        px.add_argument("--out-dir",  default="results", help="결과 저장 디렉토리")

    # --- run ---
    pr = sub.add_parser("run", help="단일 백테스트 실행")
    add_common(pr)
    pr.add_argument("--take-profit",         type=float, default=None)
    pr.add_argument("--stop-loss",           type=float, default=None)
    pr.add_argument("--rsi-min",             type=float, default=None)
    pr.add_argument("--rsi-max",             type=float, default=None)
    pr.add_argument("--use-limit-order-sim", action="store_true", default=False,
                    help="지정가 체결 시뮬레이터 활성화 (기본 OFF)")
    pr.add_argument("--limit-fill-rate",     type=float, default=0.7,
                    help="지정가 체결 확률 0~1 (기본 0.7, --use-limit-order-sim 활성 시 유효)")
    pr.add_argument("--signal-next-candle",  action="store_true", default=False,
                    help="신호는 i-1봉, 체결은 i봉 시가로 처리 (look-ahead 제거 강화)")
    pr.set_defaults(func=cmd_run)

    # --- grid ---
    pg = sub.add_parser("grid", help="파라미터 그리드 서치")
    add_common(pg)
    pg.add_argument("--max-combos", type=int, default=200, help="최대 조합 수 (기본 200)")
    pg.set_defaults(func=cmd_grid)

    # --- wfa ---
    pw = sub.add_parser("wfa", help="워크포워드 검증")
    add_common(pw)
    pw.add_argument("--windows",  type=int,   default=5,   help="분할 윈도우 수 (기본 5)")
    pw.add_argument("--is-ratio", type=float, default=0.7, help="IS 비율 0~1 (기본 0.7)")
    pw.set_defaults(func=cmd_wfa)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    # htf_unit을 언더스코어로 통일
    if hasattr(args, "htf_unit") and not hasattr(args, "htf-unit"):
        pass
    args.func(args)


if __name__ == "__main__":
    main()
