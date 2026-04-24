"""진입/청산 전략 판단 — 지표 스냅샷 생성, 스코어 계산, 매도 판단."""
from __future__ import annotations

import logging

from .config import Config
from .indicators import (
    calc_atr, calc_rsi_wilder, ema, ema_prev, sma, sma_prev,
    extract_candles_asc, estimate_net_pnl_pct,
)


def build_tf_trend_snapshot(
    candles_desc: list[dict],
    current_price: float,
    ma_short_period: int,
    ma_long_period: int,
    rsi_period: int | None = None,
    use_ema: bool = False,
) -> dict:
    candles_asc = extract_candles_asc(candles_desc)
    closes = [float(c["trade_price"]) for c in candles_asc]

    # use_ema=True 면 EMA, False 면 기존 SMA 사용
    ma_fn      = ema      if use_ema else sma
    ma_prev_fn = ema_prev if use_ema else sma_prev

    ma_short      = ma_fn(closes, ma_short_period)
    ma_long       = ma_fn(closes, ma_long_period)
    ma_short_prev = ma_prev_fn(closes, ma_short_period)
    ma_long_prev  = ma_prev_fn(closes, ma_long_period)

    trend_up = ma_short > ma_long and ma_short > ma_short_prev and ma_long >= ma_long_prev
    trend_broken = ma_short < ma_long or (current_price < ma_long and ma_short <= ma_short_prev)

    result = {
        "ma_short": ma_short,
        "ma_long": ma_long,
        "trend_up": trend_up,
        "trend_broken": trend_broken,
    }

    if rsi_period is not None:
        result["rsi"] = calc_rsi_wilder(closes, rsi_period)

    return result


def build_current_tf_filters(
    candles_desc: list[dict],
    current_price: float,
    cfg: Config,
    best_bid: float,
    best_ask: float,
) -> dict:
    trend = build_tf_trend_snapshot(
        candles_desc,
        current_price,
        cfg.ma_short_period,
        cfg.ma_long_period,
        cfg.rsi_period,
        use_ema=cfg.use_ema,
    )

    deviation = (current_price - trend["ma_short"]) / trend["ma_short"] if trend["ma_short"] > 0 else 0.0
    pullback_ok = (
        deviation >= -cfg.pullback_max_below_ma_pct
        and deviation <= cfg.pullback_max_above_ma_pct
        and current_price >= trend["ma_long"]
    )

    candles_asc = extract_candles_asc(candles_desc)
    trade_values = [float(c["candle_acc_trade_price"]) for c in candles_asc]

    if len(trade_values) >= cfg.volume_lookback + 1:
        recent_value = trade_values[-1]
        avg_trade_value = sum(trade_values[-cfg.volume_lookback - 1:-1]) / cfg.volume_lookback
    else:
        recent_value = trade_values[-1]
        hist = trade_values[:-1] if len(trade_values) > 1 else trade_values
        avg_trade_value = sum(hist) / max(1, len(hist))

    volume_ratio = (recent_value / avg_trade_value) if avg_trade_value > 0 else 0.0
    volume_ok = True if cfg.volume_min_ratio <= 0 else (volume_ratio >= cfg.volume_min_ratio)

    mid_price = (best_bid + best_ask) / 2 if (best_bid + best_ask) > 0 else 0.0
    spread_pct = ((best_ask - best_bid) / mid_price) if mid_price > 0 else 0.0
    spread_ok = True if cfg.max_spread_pct <= 0 else (spread_pct <= cfg.max_spread_pct)

    # ATR 계산 (use_atr_stop 여부와 무관하게 항상 계산 — should_sell에서 활용)
    atr = calc_atr(candles_asc, cfg.atr_period) if hasattr(cfg, "atr_period") else 0.0
    if atr == 0.0 and any([
        getattr(cfg, "use_atr_stop", False),
        getattr(cfg, "use_chandelier_exit", False),
        getattr(cfg, "use_atr_position_sizing", False),
    ]):
        logging.debug(
            "ATR=0 (데이터 부족 또는 계산 불가) — ATR 기반 기능이 이번 봉에서 비활성화됩니다. "
            "atr_period=%d, 보유 캔들 수=%d",
            getattr(cfg, "atr_period", 14),
            len(candles_asc),
        )

    return {
        **trend,
        "deviation_from_ma_short_pct": deviation,
        "pullback_ok": pullback_ok,
        "last_trade_value": recent_value,
        "avg_trade_value": avg_trade_value,
        "volume_ratio": volume_ratio,
        "volume_ok": volume_ok,
        "spread_pct": spread_pct,
        "spread_ok": spread_ok,
        "atr": atr,
    }


def build_strategy_snapshot(
    current_tf: dict,
    higher_tf: dict,
    cooldown_active: bool,
    cooldown_remaining_sec: float,
    market_warning: str,
    obi: float = 0.0,
) -> dict:
    return {
        "ma_short": current_tf["ma_short"],
        "ma_long": current_tf["ma_long"],
        "rsi": current_tf["rsi"],
        "trend_up": current_tf["trend_up"],
        "trend_broken": current_tf["trend_broken"],
        "pullback_ok": current_tf["pullback_ok"],
        "deviation_from_ma_short_pct": current_tf["deviation_from_ma_short_pct"],
        "last_trade_value": current_tf["last_trade_value"],
        "avg_trade_value": current_tf["avg_trade_value"],
        "volume_ratio": current_tf["volume_ratio"],
        "volume_ok": current_tf["volume_ok"],
        "spread_pct": current_tf["spread_pct"],
        "spread_ok": current_tf["spread_ok"],
        "atr": current_tf.get("atr", 0.0),
        "obi": obi,
        "htf_ma_short": higher_tf["ma_short"],
        "htf_ma_long": higher_tf["ma_long"],
        "htf_trend_up": higher_tf["trend_up"],
        "htf_trend_broken": higher_tf["trend_broken"],
        "cooldown_active": cooldown_active,
        "cooldown_remaining_sec": cooldown_remaining_sec,
        "market_warning": market_warning or "NONE",
    }


def compute_score(cfg: Config, strat: dict) -> tuple[float, str]:
    """
    가중치 기반 진입 스코어 계산.
    HTF 추세만 필수 하드 게이트, 나머지는 가중치 점수 합산.
    cfg.min_entry_score 이상이면 진입 허용.

    최대 점수 구조:
        HTF    (필수): 통과 시 30점
        현재추세     : 30점
        눌림목       : 20점
        RSI          : 15점
        거래대금     : 15점
        스프레드     : 10점
        OBI 보너스   :  5점
        MA 근접도    :  5점
        합계 최대   130점 (HTF 제외 기여분 100점)
    """
    reasons = []
    score = 0.0

    # ── 하드 게이트 ───────────────────────────────────────────────────────────
    if strat["market_warning"] not in {"NONE", "", None}:
        return -999.0, f"시장경보 제외 | market_warning={strat['market_warning']}"
    if strat["cooldown_active"]:
        return -999.0, f"쿨다운 중 | 남은시간={strat['cooldown_remaining_sec']:.0f}s"
    if not strat["htf_trend_up"]:
        return -100.0, "상위 추세 미충족"
    score += 30.0
    reasons.append("HTF")

    # ── 소프트 스코어링 ───────────────────────────────────────────────────────
    # 현재 추세 (30점)
    if strat["trend_up"]:
        score += 30.0
        reasons.append("CUR")
    else:
        reasons.append("CUR✗")

    # 눌림목 (20점)
    if strat["pullback_ok"]:
        score += 20.0
        reasons.append("PULLBACK")
    else:
        reasons.append(f"PULLBACK✗({strat['deviation_from_ma_short_pct'] * 100:+.2f}%)")

    # RSI (15점 — 범위 내일수록 높은 점수)
    rsi = strat["rsi"]
    rsi_min = cfg.rsi_buy_min
    rsi_max = cfg.rsi_buy_max
    if rsi_min <= rsi <= rsi_max:
        rsi_center = (rsi_min + rsi_max) / 2
        rsi_half   = (rsi_max - rsi_min) / 2
        rsi_score  = 15.0 * max(0.0, 1.0 - abs(rsi - rsi_center) / rsi_half)
        score += rsi_score
        reasons.append(f"RSI={rsi:.1f}")
    else:
        # 범위 외: 얼마나 벗어났는지에 따라 패널티
        overshoot = max(rsi_min - rsi, rsi - rsi_max)
        penalty = min(15.0, overshoot * 0.5)
        score -= penalty
        reasons.append(f"RSI✗={rsi:.1f}(-{penalty:.1f})")

    # 거래대금 (15점)
    if strat["volume_ok"]:
        vol_score = min(15.0, strat["volume_ratio"] * 5.0)
        score += vol_score
        reasons.append(f"VOL={strat['volume_ratio']:.2f}")
    else:
        reasons.append(f"VOL✗={strat['volume_ratio']:.2f}")

    # 스프레드 (10점)
    if strat["spread_ok"]:
        spread_score = max(0.0, 10.0 - strat["spread_pct"] * 10000)
        score += spread_score
        reasons.append(f"SPR={strat['spread_pct'] * 100:.4f}%")
    else:
        reasons.append(f"SPR✗={strat['spread_pct'] * 100:.4f}%")

    # OBI 보너스/패널티 (±5점)
    obi = strat.get("obi", 0.0)
    obi_score = obi * 5.0  # OBI +1 → +5점, -1 → -5점
    score += obi_score
    if abs(obi) > 0.05:
        reasons.append(f"OBI={obi:+.2f}")

    # MA 근접도 보너스 (5점)
    closeness = max(0.0, 5.0 - abs(strat["deviation_from_ma_short_pct"]) * 500)
    score += closeness
    reasons.append(f"MAΔ={strat['deviation_from_ma_short_pct'] * 100:+.2f}%")

    return score, " | ".join(reasons)


def should_sell(
    cfg: Config,
    snap: dict,
    strat: dict,
    hold_sec: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
    current_price: float,
    max_price_since_entry: float = 0.0,
    breakeven_activated: bool = False,
) -> tuple[bool, str, bool]:
    """
    Returns: (sell_ok, reason, new_breakeven_activated)
    new_breakeven_activated: 이번 호출에서 브레이크이븐이 활성화됐으면 True
    """
    if snap["base_total"] <= 0:
        return False, "보유 수량 없음", breakeven_activated
    if snap["avg_buy_price"] <= 0:
        return False, "평단 없음", breakeven_activated

    avg_buy = snap["avg_buy_price"]
    net_pnl_pct = estimate_net_pnl_pct(
        current_price=current_price,
        avg_buy_price=avg_buy,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
    )

    required_net_profit = cfg.min_net_profit_pct + cfg.slippage_buffer_pct

    # ── ATR 기반 동적 손절/익절 레벨 계산 ────────────────────────────────────
    atr = strat.get("atr", 0.0)
    if getattr(cfg, "use_atr_stop", False) and atr > 0 and avg_buy > 0:
        atr_pct = atr / avg_buy
        dynamic_stop_pct  = -(cfg.atr_stop_multiplier * atr_pct)
        dynamic_take_pct  =  cfg.atr_take_multiplier  * atr_pct
        # 설정값보다 극단적인 쪽은 사용하지 않음 (안전망 유지)
        effective_stop_pct = max(dynamic_stop_pct, cfg.stop_loss_pct)   # 덜 가혹한 쪽
        effective_take_pct = max(dynamic_take_pct, cfg.take_profit_pct) # 더 높은 쪽
        atr_note = f"ATRx{cfg.atr_stop_multiplier:.1f}/{cfg.atr_take_multiplier:.1f}"
    else:
        effective_stop_pct = cfg.stop_loss_pct
        effective_take_pct = cfg.take_profit_pct
        atr_note = "고정"

    # ── 익절 ──────────────────────────────────────────────────────────────────
    take_threshold = max(effective_take_pct, required_net_profit)
    if net_pnl_pct >= take_threshold:
        return True, (
            f"익절 조건 충족 [{atr_note}] | 순수익률={net_pnl_pct * 100:+.2f}%"
            f" >= 목표={take_threshold * 100:+.2f}%"
        ), breakeven_activated

    # ── 손절 ──────────────────────────────────────────────────────────────────
    if net_pnl_pct <= effective_stop_pct:
        return True, (
            f"손절 조건 충족 [{atr_note}] | 순수익률={net_pnl_pct * 100:+.2f}%"
            f" <= 손절={effective_stop_pct * 100:+.2f}%"
        ), breakeven_activated

    # ── 브레이크이븐 스탑 ─────────────────────────────────────────────────────
    # activate_pct 이상 수익 도달 시 플래그 활성화 (영속 보존)
    # 이후 buffer_pct 이하로 되돌아오면 청산
    new_be_activated = breakeven_activated
    if getattr(cfg, "use_breakeven_stop", False):
        if net_pnl_pct >= cfg.breakeven_activate_pct:
            new_be_activated = True  # 이번 루프에서 처음 활성화
        if new_be_activated and net_pnl_pct < cfg.breakeven_buffer_pct:
            return True, (
                f"브레이크이븐 스탑 | 순수익률={net_pnl_pct * 100:+.2f}%"
                f" < 보호선={cfg.breakeven_buffer_pct * 100:.2f}%"
                f" (활성화={cfg.breakeven_activate_pct * 100:.2f}% 도달 후)"
            ), new_be_activated

    # ── 최소 보유시간 미충족 — 아래 동적 스탑들보다 먼저 체크 ─────────────────
    # 익절/손절은 min_hold_sec 무시, 브레이크이븐/트레일링/샹들리에/타임스탑은 이후에만 발동
    if hold_sec < cfg.min_hold_sec:
        return (
            False,
            f"매도 대기 | 최소 보유시간 미충족 | hold={hold_sec:.0f}s < {cfg.min_hold_sec}s"
            f" | 순수익률={net_pnl_pct * 100:+.2f}%",
            new_be_activated,
        )

    # ── 트레일링 스탑 ─────────────────────────────────────────────────────────
    if getattr(cfg, "use_trailing_stop", False) and max_price_since_entry > 0:
        max_net_pct = estimate_net_pnl_pct(
            current_price=max_price_since_entry,
            avg_buy_price=avg_buy,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
        )
        if max_net_pct >= cfg.trailing_activate_pct:
            drawdown_from_peak = max_net_pct - net_pnl_pct
            if drawdown_from_peak >= cfg.trailing_stop_pct:
                return True, (
                    f"트레일링 스탑 | 최고={max_net_pct * 100:+.2f}%"
                    f" → 현재={net_pnl_pct * 100:+.2f}%"
                    f" | 낙폭={drawdown_from_peak * 100:.2f}%"
                    f" >= 기준={cfg.trailing_stop_pct * 100:.2f}%"
                ), new_be_activated

    # ── 샹들리에 청산 (ATR 기반 동적 트레일링) ───────────────────────────────
    atr = strat.get("atr", 0.0)
    if getattr(cfg, "use_chandelier_exit", False) and max_price_since_entry > 0 and atr > 0:
        chandelier_stop = max_price_since_entry - atr * cfg.chandelier_multiplier
        if current_price <= chandelier_stop:
            return True, (
                f"샹들리에 청산 | 최고가={max_price_since_entry:.2f}"
                f" - ATR×{cfg.chandelier_multiplier:.1f}({atr:.2f})"
                f" = 스탑={chandelier_stop:.2f} | 현재={current_price:.2f}"
            ), new_be_activated

    # ── 타임 스탑 ─────────────────────────────────────────────────────────────
    if getattr(cfg, "use_time_stop", False):
        time_stop_sec = cfg.time_stop_candles * cfg.candle_unit * 60
        if hold_sec >= time_stop_sec and net_pnl_pct < cfg.time_stop_min_profit_pct:
            return True, (
                f"타임 스탑 | {cfg.time_stop_candles}봉({hold_sec / 60:.0f}분) 경과"
                f" | 순수익률={net_pnl_pct * 100:+.2f}%"
                f" < 목표={cfg.time_stop_min_profit_pct * 100:.2f}%"
            ), new_be_activated

    # ── 추세 이탈 청산 ────────────────────────────────────────────────────────
    if strat["htf_trend_broken"]:
        if net_pnl_pct >= required_net_profit:
            return True, f"상위 추세 이탈 + 순익 확보 | 순수익률={net_pnl_pct * 100:+.2f}%", new_be_activated
        if net_pnl_pct <= -cfg.slippage_buffer_pct:
            return True, f"상위 추세 이탈 방어 청산 | 순수익률={net_pnl_pct * 100:+.2f}%", new_be_activated
        return False, f"상위 추세 이탈이지만 비용권 보류 | 순수익률={net_pnl_pct * 100:+.2f}%", new_be_activated

    if strat["trend_broken"]:
        if net_pnl_pct >= required_net_profit:
            return True, f"추세 이탈 + 순익 확보 | 순수익률={net_pnl_pct * 100:+.2f}%", new_be_activated
        if net_pnl_pct <= -cfg.slippage_buffer_pct:
            return True, f"추세 이탈 방어 청산 | 순수익률={net_pnl_pct * 100:+.2f}%", new_be_activated
        return False, f"추세 이탈이지만 비용권 보류 | 순수익률={net_pnl_pct * 100:+.2f}%", new_be_activated

    return False, f"매도 대기 | 순수익률={net_pnl_pct * 100:+.2f}% | hold={hold_sec:.0f}s", new_be_activated


def is_aggressive_sell_reason(sell_reason: str) -> bool:
    """
    즉시 taker 체결이 필요한 매도 사유 판단.
    best_bid 가격으로 즉시 청산해야 하는 모든 방어/긴급 사유를 포함합니다.
    """
    urgent = [
        "손절",
        "브레이크이븐",       # 원금 보호선 이탈 → 즉시 청산 필요
        "방어 청산",           # 추세 이탈 + 손실권 → 즉시 청산
        "추세 이탈",           # 추세 이탈 방어청산 포함
        "상위 추세 이탈",
        "트레일링 스탑",
        "타임 스탑",
        "샹들리에",
        "서킷",
    ]
    return any(kw in sell_reason for kw in urgent)


def check_partial_exit(
    cfg: "Config",
    snap: dict,
    buy_fee_rate: float,
    sell_fee_rate: float,
    current_price: float,
    already_partial_exited: bool = False,
) -> tuple[bool, float, str]:
    """
    부분 익절 조건 확인.
    Returns: (trigger, exit_ratio, reason)
    """
    if not getattr(cfg, "use_partial_exit", False) or already_partial_exited:
        return False, 0.0, ""
    if snap["base_total"] <= 0 or snap["avg_buy_price"] <= 0:
        return False, 0.0, ""
    net_pnl_pct = estimate_net_pnl_pct(
        current_price=current_price,
        avg_buy_price=snap["avg_buy_price"],
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
    )
    if net_pnl_pct >= cfg.partial_exit_pct:
        ratio = cfg.partial_exit_ratio
        return True, ratio, (
            f"부분익절 ({ratio * 100:.0f}%) | 순수익률={net_pnl_pct * 100:+.2f}%"
            f" >= 목표={cfg.partial_exit_pct * 100:.2f}%"
        )
    return False, 0.0, ""


def calc_dynamic_buy_amount(cfg: "Config", current_price: float, atr: float) -> float:
    """
    ATR 기반 동적 포지션 사이징.
    손실 허용 금액(risk_per_trade_krw) ÷ ATR 손절 거리 = 투자 금액.
    """
    if not getattr(cfg, "use_atr_position_sizing", False) or atr <= 0 or current_price <= 0:
        return cfg.buy_krw_amount
    stop_distance_pct = getattr(cfg, "atr_stop_multiplier", 2.0) * atr / current_price
    if stop_distance_pct <= 0:
        return cfg.buy_krw_amount
    dynamic_amount = cfg.risk_per_trade_krw / stop_distance_pct
    return max(cfg.min_order_krw, min(dynamic_amount, cfg.buy_krw_amount * 3.0))


def classify_exit_reason(sell_reason: str) -> str:
    """
    매도 사유를 내부 분류 코드로 변환합니다.

    우선순위:
    1. 내부 상태 코드 문자열(stop_loss, partial_exit 등)은 그대로 반환
    2. "부분익절"은 "익절"보다 반드시 먼저 체크 ("부분익절" 안에 "익절" 포함)
    3. 손실성/방어성 청산은 모두 stop_loss로 분류해 서킷브레이커 카운터에 포함
    """
    reason = str(sell_reason or "")

    # ── 내부 상태 코드 직접 인식 (set_pending_exit_reason에서 저장한 값) ──────
    _INTERNAL_CODES = {
        "stop_loss", "take_profit", "partial_exit",
        "htf_trend_exit", "trend_exit", "sell",
    }
    if reason in _INTERNAL_CODES:
        return reason

    # ── "부분익절"은 반드시 "익절"보다 먼저 체크 ─────────────────────────────
    if "부분익절" in reason:
        return "partial_exit"

    # ── 손실성/방어성 청산 → stop_loss ───────────────────────────────────────
    _STOP_KEYWORDS = [
        "손절",
        "방어 청산",
        "브레이크이븐",
        "트레일링 스탑",
        "타임 스탑",
        "샹들리에",
    ]
    if any(k in reason for k in _STOP_KEYWORDS):
        return "stop_loss"

    if "익절" in reason:
        return "take_profit"
    if "상위 추세 이탈" in reason:
        return "htf_trend_exit"
    if "추세 이탈" in reason:
        return "trend_exit"

    return "sell"


