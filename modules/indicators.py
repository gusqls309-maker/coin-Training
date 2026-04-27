"""지표 계산 + 기본 수치 유틸 (SMA/EMA/RSI/ATR/OBI 등)."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

from .display import parse_upbit_dt


def age_seconds(created_at: str | None) -> float:
    dt = parse_upbit_dt(created_at)
    if not dt:
        return 0.0
    return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())


def has_young_order(orders: list[dict], min_age_sec: float) -> bool:
    """
    주문 목록 중 제출 후 min_age_sec 미만인 주문이 하나라도 있으면 True.
    매수 주문을 너무 빠르게 취소하는 oscillation 방지에 사용합니다.
    """
    for o in orders:
        if age_seconds(o.get("created_at")) < min_age_sec:
            return True
    return False


def sma(values: list[float], period: int) -> float:
    if len(values) < period:
        raise ValueError(f"SMA 계산용 데이터 부족: need={period}, got={len(values)}")
    return sum(values[-period:]) / period


def sma_prev(values: list[float], period: int) -> float:
    if len(values) < period + 1:
        raise ValueError(f"SMA 이전값 계산용 데이터 부족: need={period + 1}, got={len(values)}")
    return sum(values[-period - 1:-1]) / period


def ema(values: list[float], period: int) -> float:
    """지수이동평균(EMA) — 최신 가격에 더 높은 가중치 부여"""
    if len(values) < period:
        raise ValueError(f"EMA 데이터 부족: need={period}, got={len(values)}")
    k = 2.0 / (period + 1)
    result = sum(values[:period]) / period   # 초기값 = SMA
    for v in values[period:]:
        result = v * k + result * (1.0 - k)
    return result


def ema_prev(values: list[float], period: int) -> float:
    """직전 봉 기준 EMA (현재 봉 제외)"""
    if len(values) < period + 1:
        raise ValueError(f"EMA 이전값 데이터 부족: need={period + 1}, got={len(values)}")
    return ema(values[:-1], period)


def calc_rsi_wilder(closes: list[float], period: int) -> float:
    if len(closes) < period + 1:
        raise ValueError(f"RSI 계산용 데이터 부족: need={period + 1}, got={len(closes)}")

    gains = []
    losses = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calc_atr(candles_asc: list[dict], period: int = 14) -> float:
    """
    Wilder 평활 ATR(Average True Range).
    변동성 크기를 가격 단위로 반환합니다.
    """
    if len(candles_asc) < period + 1:
        return 0.0
    true_ranges: list[float] = []
    for i in range(1, len(candles_asc)):
        high       = float(candles_asc[i]["high_price"])
        low        = float(candles_asc[i]["low_price"])
        prev_close = float(candles_asc[i - 1]["trade_price"])
        tr = max(high - low,
                 abs(high - prev_close),
                 abs(low  - prev_close))
        true_ranges.append(tr)
    # 초기 ATR = 단순평균
    atr_val = sum(true_ranges[:period]) / period
    # 이후 Wilder 평활
    for tr in true_ranges[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
    return atr_val


def calc_obi(orderbook_units: list[dict], depth: int = 5) -> float:
    """
    Orderbook Imbalance (OBI) = (bid_총잔량 - ask_총잔량) / (bid_총잔량 + ask_총잔량)
    +1에 가까울수록 매수벽 우세, -1에 가까울수록 매도벽 우세.
    """
    if not orderbook_units:
        return 0.0
    bid_total = sum(
        float(u.get("bid_size", 0) or 0) * float(u.get("bid_price", 0) or 0)
        for u in orderbook_units[:depth]
    )
    ask_total = sum(
        float(u.get("ask_size", 0) or 0) * float(u.get("ask_price", 0) or 0)
        for u in orderbook_units[:depth]
    )
    total = bid_total + ask_total
    return (bid_total - ask_total) / total if total > 0 else 0.0


def calc_buy_volume(krw_amount: float, price: float, decimals: int) -> float:
    if price <= 0 or krw_amount <= 0:
        return 0.0
    return float(
        (Decimal(str(krw_amount)) / Decimal(str(price))).quantize(
            Decimal("1").scaleb(-decimals),
            rounding=ROUND_DOWN,
        )
    )


def extract_candles_asc(candles_desc: list[dict]) -> list[dict]:
    return list(reversed(candles_desc))


def estimate_net_pnl_pct(
    current_price: float,
    avg_buy_price: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
) -> float:
    if current_price <= 0 or avg_buy_price <= 0:
        return 0.0
    sell_after_fee = current_price * (1.0 - max(0.0, sell_fee_rate))
    buy_with_fee = avg_buy_price * (1.0 + max(0.0, buy_fee_rate))
    return (sell_after_fee / buy_with_fee) - 1.0


def required_candle_count(ma_short_period: int, ma_long_period: int, rsi_period: int | None = None) -> int:
    required = max(ma_short_period, ma_long_period) + 1
    if rsi_period is not None:
        required = max(required, rsi_period + 1)
    return required


