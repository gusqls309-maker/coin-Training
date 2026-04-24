"""Config dataclass — 환경변수 기반 설정값 관리."""
from __future__ import annotations

import os
from dataclasses import dataclass


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class Config:
    access_key: str = os.getenv("UPBIT_ACCESS_KEY", "")
    secret_key: str = os.getenv("UPBIT_SECRET_KEY", "")

    markets: list[str] | None = None
    dynamic_quote_currencies: list[str] | None = None
    extra_fixed_markets: list[str] | None = None
    excluded_markets: list[str] | None = None

    order_mode: str = os.getenv("ORDER_MODE", "signal").lower()
    max_active_positions: int = int(os.getenv("MAX_ACTIVE_POSITIONS", "3"))
    exclude_warning_markets: bool = parse_bool(os.getenv("EXCLUDE_WARNING_MARKETS"), True)

    use_dynamic_markets: bool = parse_bool(os.getenv("USE_DYNAMIC_MARKETS"), True)
    dynamic_top_n: int = int(os.getenv("DYNAMIC_TOP_N", "10"))
    dynamic_refresh_sec: int = int(os.getenv("DYNAMIC_REFRESH_SEC", "300"))
    dynamic_min_acc_trade_price_24h: float = float(os.getenv("DYNAMIC_MIN_ACC_TRADE_PRICE_24H", "5000000000"))

    buy_krw_amount: float = float(os.getenv("BUY_KRW_AMOUNT", "10000"))
    take_profit_pct: float = float(os.getenv("TAKE_PROFIT_PCT", "0.015"))
    stop_loss_pct: float = float(os.getenv("STOP_LOSS_PCT", "-0.012"))
    poll_interval_sec: float = float(os.getenv("POLL_INTERVAL_SEC", "3"))
    min_order_krw: float = float(os.getenv("MIN_ORDER_KRW", "5000"))

    min_net_profit_pct: float = float(os.getenv("MIN_NET_PROFIT_PCT", "0.0010"))
    slippage_buffer_pct: float = float(os.getenv("SLIPPAGE_BUFFER_PCT", "0.0005"))
    min_hold_sec: int = int(os.getenv("MIN_HOLD_SEC", "180"))

    bot_id_prefix: str = os.getenv("BOT_ID_PREFIX", "bot")
    limit_reprice_sec: float = float(os.getenv("LIMIT_REPRICE_SEC", "15"))
    use_cancel_and_new: bool = parse_bool(os.getenv("USE_CANCEL_AND_NEW"), True)
    limit_time_in_force: str = os.getenv("LIMIT_TIME_IN_FORCE", "").strip().lower()
    volume_decimals: int = int(os.getenv("VOLUME_DECIMALS", "8"))
    orderbook_print_depth: int = int(os.getenv("ORDERBOOK_PRINT_DEPTH", "5"))

    candle_unit: int = int(os.getenv("CANDLE_UNIT", "5"))
    candle_count: int = int(os.getenv("CANDLE_COUNT", "200"))
    ma_short_period: int = int(os.getenv("MA_SHORT_PERIOD", "20"))
    ma_long_period: int = int(os.getenv("MA_LONG_PERIOD", "60"))
    rsi_period: int = int(os.getenv("RSI_PERIOD", "14"))

    htf_candle_unit: int = int(os.getenv("HTF_CANDLE_UNIT", "60"))
    htf_candle_count: int = int(os.getenv("HTF_CANDLE_COUNT", "200"))
    htf_ma_short_period: int = int(os.getenv("HTF_MA_SHORT_PERIOD", "20"))
    htf_ma_long_period: int = int(os.getenv("HTF_MA_LONG_PERIOD", "60"))

    rsi_buy_min: float = float(os.getenv("RSI_BUY_MIN", "35"))
    rsi_buy_max: float = float(os.getenv("RSI_BUY_MAX", "65"))
    pullback_max_below_ma_pct: float = float(os.getenv("PULLBACK_MAX_BELOW_MA_PCT", "0.015"))
    pullback_max_above_ma_pct: float = float(os.getenv("PULLBACK_MAX_ABOVE_MA_PCT", "0.003"))

    volume_lookback: int = int(os.getenv("VOLUME_LOOKBACK", "20"))
    volume_min_ratio: float = float(os.getenv("VOLUME_MIN_RATIO", "0.50"))
    max_spread_pct: float = float(os.getenv("MAX_SPREAD_PCT", "0.0015"))

    cooldown_after_exit_sec: int = int(os.getenv("COOLDOWN_AFTER_EXIT_SEC", "1800"))
    cooldown_after_stop_loss_sec: int = int(os.getenv("COOLDOWN_AFTER_STOP_LOSS_SEC", "3600"))

    refresh_candles_only_on_new_bucket: bool = parse_bool(os.getenv("REFRESH_CANDLES_ONLY_ON_NEW_BUCKET"), True)
    market_info_refresh_sec: int = int(os.getenv("MARKET_INFO_REFRESH_SEC", "600"))
    chance_cache_sec: int = int(os.getenv("CHANCE_CACHE_SEC", "10"))
    candle_min_interval_sec: float = float(os.getenv("CANDLE_MIN_INTERVAL_SEC", "0.12"))

    status_csv_file: str = os.getenv("STATUS_CSV_FILE", "status_log.csv")
    trade_csv_file: str = os.getenv("TRADE_CSV_FILE", "trade_log.csv")
    bot_state_file: str = os.getenv("BOT_STATE_FILE", "bot_state.json")
    log_file: str = os.getenv("LOG_FILE", "upbit_auto_trade.log")
    console_clear_each_loop: bool = parse_bool(os.getenv("CONSOLE_CLEAR_EACH_LOOP"), True)
    use_smooth_refresh: bool = parse_bool(os.getenv("USE_SMOOTH_REFRESH"), True)
    
    # 5xx 에러 백오프 설정 (초 단위)
    backoff_5xx_gateway_sec: int = int(os.getenv("BACKOFF_5XX_GATEWAY_SEC", "60"))  # 502, 503, 504
    backoff_500_sec: int = int(os.getenv("BACKOFF_500_SEC", "30"))  # 500 Internal Server Error
    backoff_429_sec: int = int(os.getenv("BACKOFF_429_SEC", "5"))   # 429 Too Many Requests

    # ── 호가창 뎁스 체크 ─────────────────────────────────────────────────────
    orderbook_depth_check: bool = parse_bool(os.getenv("ORDERBOOK_DEPTH_CHECK"), True)
    orderbook_depth_impact_ratio: float = float(os.getenv("ORDERBOOK_DEPTH_IMPACT_RATIO", "0.5"))

    # ── EMA ──────────────────────────────────────────────────────────────────
    use_ema: bool = parse_bool(os.getenv("USE_EMA"), False)

    # ── 트레일링 스탑 ────────────────────────────────────────────────────────
    use_trailing_stop: bool = parse_bool(os.getenv("USE_TRAILING_STOP"), False)
    trailing_activate_pct: float = float(os.getenv("TRAILING_ACTIVATE_PCT", "0.010"))
    trailing_stop_pct: float = float(os.getenv("TRAILING_STOP_PCT", "0.008"))

    # ── ATR 기반 동적 손절/익절 ──────────────────────────────────────────────
    use_atr_stop: bool = parse_bool(os.getenv("USE_ATR_STOP"), False)
    atr_period: int = int(os.getenv("ATR_PERIOD", "14"))
    atr_stop_multiplier: float = float(os.getenv("ATR_STOP_MULTIPLIER", "2.0"))
    atr_take_multiplier: float = float(os.getenv("ATR_TAKE_MULTIPLIER", "3.0"))

    # ── 연속 손절 서킷브레이커 ───────────────────────────────────────────────
    circuit_breaker_enabled: bool = parse_bool(os.getenv("CIRCUIT_BREAKER_ENABLED"), True)
    circuit_breaker_max_stops: int = int(os.getenv("CIRCUIT_BREAKER_MAX_STOPS", "3"))
    circuit_breaker_window_sec: int = int(os.getenv("CIRCUIT_BREAKER_WINDOW_SEC", "3600"))
    circuit_breaker_cooldown_sec: int = int(os.getenv("CIRCUIT_BREAKER_COOLDOWN_SEC", "7200"))

    # ── 브레이크이븐 스탑 ────────────────────────────────────────────────────
    # activate_pct 이상 수익 시 원금+buffer 위에 스탑을 걸어 손실 없이 청산 보장
    use_breakeven_stop: bool = parse_bool(os.getenv("USE_BREAKEVEN_STOP"), False)
    breakeven_activate_pct: float = float(os.getenv("BREAKEVEN_ACTIVATE_PCT", "0.005"))
    breakeven_buffer_pct: float = float(os.getenv("BREAKEVEN_BUFFER_PCT", "0.001"))

    # ── 부분 익절 ────────────────────────────────────────────────────────────
    use_partial_exit: bool = parse_bool(os.getenv("USE_PARTIAL_EXIT"), False)
    partial_exit_pct: float = float(os.getenv("PARTIAL_EXIT_PCT", "0.012"))
    partial_exit_ratio: float = float(os.getenv("PARTIAL_EXIT_RATIO", "0.5"))

    # ── 타임 스탑 ────────────────────────────────────────────────────────────
    # N봉 경과 후에도 목표의 절반에 못 미치면 강제 청산
    use_time_stop: bool = parse_bool(os.getenv("USE_TIME_STOP"), False)
    time_stop_candles: int = int(os.getenv("TIME_STOP_CANDLES", "24"))
    time_stop_min_profit_pct: float = float(os.getenv("TIME_STOP_MIN_PROFIT_PCT", "0.003"))

    # ── 샹들리에 청산 (ATR 기반 동적 트레일링) ──────────────────────────────
    use_chandelier_exit: bool = parse_bool(os.getenv("USE_CHANDELIER_EXIT"), False)
    chandelier_multiplier: float = float(os.getenv("CHANDELIER_MULTIPLIER", "2.0"))

    # ── OBI 진입 필터 ────────────────────────────────────────────────────────
    use_obi_filter: bool = parse_bool(os.getenv("USE_OBI_FILTER"), False)
    obi_min_threshold: float = float(os.getenv("OBI_MIN_THRESHOLD", "-0.3"))
    obi_depth: int = int(os.getenv("OBI_DEPTH", "5"))

    # ── 진입 스코어 임계값 / 점수제 ─────────────────────────────────────────
    min_entry_score: float = float(os.getenv("MIN_ENTRY_SCORE", "60.0"))

    # ── 매수 주문 취소 히스테리시스 ──────────────────────────────────────────
    # 진입 임계값보다 낮은 취소 임계값을 설정해 score 진동으로 인한
    # 매수→취소 무한반복(oscillation)을 방지합니다.
    # 예: min_entry_score=60, buy_cancel_score_ratio=0.85 → score < 51점일 때만 취소
    buy_cancel_score_ratio: float = float(os.getenv("BUY_CANCEL_SCORE_RATIO", "0.85"))

    # 매수 주문 제출 후 이 시간(초) 이내에는 신호가 해제돼도 취소하지 않음
    # LIMIT_REPRICE_SEC보다 충분히 크게 설정 권장 (기본: LIMIT_REPRICE_SEC와 동일)
    buy_min_order_age_sec: float = float(os.getenv("BUY_MIN_ORDER_AGE_SEC", "15.0"))

    # ── 점수 기반 매수 공격성 ────────────────────────────────────────────────
    # 이 점수 이상이면 best_ask(즉시 체결), 미만이면 best_bid(지정가)
    score_aggressive_threshold: float = float(os.getenv("SCORE_AGGRESSIVE_THRESHOLD", "80.0"))

    # ── 손절/방어청산 공격적 즉시 체결 ──────────────────────────────────────
    # True: 손절/상위추세이탈/트레일링 등 긴급 청산 시 best_bid taker 가격 사용
    use_aggressive_stop: bool = parse_bool(os.getenv("USE_AGGRESSIVE_STOP"), True)

    # ── ATR 기반 포지션 사이징 ───────────────────────────────────────────────
    use_atr_position_sizing: bool = parse_bool(os.getenv("USE_ATR_POSITION_SIZING"), False)
    risk_per_trade_krw: float = float(os.getenv("RISK_PER_TRADE_KRW", "500.0"))

    # ── 동적 종목 선정 품질 필터 ─────────────────────────────────────────────
    dynamic_use_quality_filter: bool = parse_bool(os.getenv("DYNAMIC_USE_QUALITY_FILTER"), True)
    dynamic_max_daily_range_pct: float = float(os.getenv("DYNAMIC_MAX_DAILY_RANGE_PCT", "0.15"))

    # ── WFA 자동 스케줄러 ────────────────────────────────────────────────────
    auto_wfa_enabled: bool = parse_bool(os.getenv("AUTO_WFA_ENABLED"), False)
    auto_wfa_interval_hours: int = int(os.getenv("AUTO_WFA_INTERVAL_HOURS", "168"))  # 기본 1주일
    auto_wfa_apply_params: bool = parse_bool(os.getenv("AUTO_WFA_APPLY_PARAMS"), False)
    auto_wfa_market: str = os.getenv("AUTO_WFA_MARKET", "KRW-BTC")
    auto_wfa_candle_count: int = int(os.getenv("AUTO_WFA_CANDLE_COUNT", "2000"))

    def __post_init__(self):
        raw = os.getenv("MULTI_MARKETS", "")
        self.markets = [m.strip().upper() for m in raw.split(",") if m.strip()]

        quote_raw = os.getenv("DYNAMIC_QUOTE_CURRENCIES", "KRW")
        self.dynamic_quote_currencies = [q.strip().upper() for q in quote_raw.split(",") if q.strip()]

        extra_raw = os.getenv("EXTRA_FIXED_MARKETS", "KRW-BTC,KRW-ETH")
        self.extra_fixed_markets = [m.strip().upper() for m in extra_raw.split(",") if m.strip()]

        excluded_raw = os.getenv("EXCLUDED_MARKETS", "KRW-USDT,KRW-USDC")
        self.excluded_markets = [m.strip().upper() for m in excluded_raw.split(",") if m.strip()]

    def validate(self) -> None:
        # API 키 검증 강화
        if not self.access_key or not self.secret_key:
            raise ValueError(
                "UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY 가 비어 있습니다.\n"
                ".env 파일을 확인하세요:\n"
                "  UPBIT_ACCESS_KEY=your_actual_access_key\n"
                "  UPBIT_SECRET_KEY=your_actual_secret_key"
            )
        
        # 더미 키 체크
        if self.access_key in ("dummy", "dummy_key_for_backtest", "your_access_key_here"):
            raise ValueError(
                "실제 업비트 API Access Key를 입력하세요.\n"
                "현재 값: dummy 또는 예시 값\n"
                "업비트 웹사이트 > 마이페이지 > Open API 관리에서 발급받으세요."
            )
        
        if self.secret_key in ("dummy", "dummy_secret_for_backtest", "your_secret_key_here"):
            raise ValueError(
                "실제 업비트 API Secret Key를 입력하세요.\n"
                "현재 값: dummy 또는 예시 값"
            )
        
        # Access Key 형식 검증 (업비트 키는 보통 영숫자 조합)
        if len(self.access_key) < 20:
            raise ValueError(
                f"Access Key가 너무 짧습니다 (현재 길이: {len(self.access_key)}).\n"
                "올바른 업비트 API 키인지 확인하세요."
            )
        
        if len(self.secret_key) < 20:
            raise ValueError(
                f"Secret Key가 너무 짧습니다 (현재 길이: {len(self.secret_key)}).\n"
                "올바른 업비트 API 키인지 확인하세요."
            )
        
        if not self.use_dynamic_markets and not self.markets:
            raise ValueError("USE_DYNAMIC_MARKETS=false 이면 MULTI_MARKETS 가 필요합니다.")
        if any("-" not in m for m in self.markets):
            raise ValueError("MULTI_MARKETS 형식이 잘못되었습니다.")
        if self.order_mode not in {"signal", "test", "live"}:
            raise ValueError("ORDER_MODE 는 signal, test, live 중 하나여야 합니다.")
        if self.limit_time_in_force not in {"", "ioc", "fok", "post_only"}:
            raise ValueError("LIMIT_TIME_IN_FORCE 는 빈 값, ioc, fok, post_only 중 하나여야 합니다.")
        if self.volume_decimals < 1 or self.volume_decimals > 12:
            raise ValueError("VOLUME_DECIMALS 는 1~12 범위여야 합니다.")
        if self.orderbook_print_depth < 1 or self.orderbook_print_depth > 30:
            raise ValueError("ORDERBOOK_PRINT_DEPTH 는 1~30 범위여야 합니다.")
        if self.candle_count > 200 or self.htf_candle_count > 200:
            raise ValueError("CANDLE_COUNT / HTF_CANDLE_COUNT 는 200 이하여야 합니다.")
        if self.ma_short_period >= self.ma_long_period:
            raise ValueError("MA_SHORT_PERIOD 는 MA_LONG_PERIOD 보다 작아야 합니다.")
        if self.htf_ma_short_period >= self.htf_ma_long_period:
            raise ValueError("HTF_MA_SHORT_PERIOD 는 HTF_MA_LONG_PERIOD 보다 작아야 합니다.")
        if self.max_active_positions < 1:
            raise ValueError("MAX_ACTIVE_POSITIONS 는 1 이상이어야 합니다.")
        if self.dynamic_top_n < 1:
            raise ValueError("DYNAMIC_TOP_N 는 1 이상이어야 합니다.")
        if self.dynamic_refresh_sec < 10:
            raise ValueError("DYNAMIC_REFRESH_SEC 는 10 이상이어야 합니다.")
        if self.buy_krw_amount < self.min_order_krw:
            raise ValueError("BUY_KRW_AMOUNT 는 MIN_ORDER_KRW 이상이어야 합니다.")
        if self.take_profit_pct < 0:
            raise ValueError("TAKE_PROFIT_PCT 는 0 이상이어야 합니다.")
        if self.stop_loss_pct > 0:
            raise ValueError("STOP_LOSS_PCT 는 0 이하이어야 합니다.")
        if self.min_net_profit_pct < 0:
            raise ValueError("MIN_NET_PROFIT_PCT 는 0 이상이어야 합니다.")
        if self.slippage_buffer_pct < 0:
            raise ValueError("SLIPPAGE_BUFFER_PCT 는 0 이상이어야 합니다.")
        if self.min_hold_sec < 0:
            raise ValueError("MIN_HOLD_SEC 는 0 이상이어야 합니다.")
