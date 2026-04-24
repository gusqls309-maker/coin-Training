from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import subprocess
import sys
import re
import threading
import time
import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from urllib.parse import urlencode, unquote

import jwt
import requests
from dotenv import load_dotenv

# PyJWT 버전에 따라 InsecureKeyLengthWarning 존재 여부가 다름 (2.4+ 에서 추가)
try:
    from jwt import InsecureKeyLengthWarning
    warnings.filterwarnings("ignore", category=InsecureKeyLengthWarning)
except ImportError:
    pass  # 해당 버전에 경고 클래스가 없으면 무시
load_dotenv()

BASE_URL = "https://api.upbit.com"


def _build_query_string(params: dict | None) -> str:
    if not params:
        return ""
    normalized: list[tuple[str, str]] = []
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                normalized.append((str(key), str(item)))
        else:
            normalized.append((str(key), str(value)))
    return unquote(urlencode(normalized, doseq=True))


class UpbitClient:
    def __init__(self, access_key: str, secret_key: str, base_url: str = BASE_URL, timeout: int = 10):
        self.access_key = access_key
        self.secret_key = secret_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def _make_auth_headers(self, params: dict | None = None) -> dict[str, str]:
        payload = {
            "access_key": self.access_key,
            "nonce": str(uuid.uuid4()),
        }

        query_string = _build_query_string(params)
        if query_string:
            payload["query_hash"] = hashlib.sha512(query_string.encode("utf-8")).hexdigest()
            payload["query_hash_alg"] = "SHA512"

        try:
            # 업비트 공식 스펙: HS512 (HS256 아님!)
            token = jwt.encode(payload, self.secret_key, algorithm="HS512")
            if isinstance(token, bytes):
                token = token.decode("utf-8")
        except Exception as e:
            raise ValueError(f"JWT 토큰 생성 실패 (API 키 확인 필요): {e}")
        
        return {"Authorization": f"Bearer {token}"}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        private: bool = False,
        json_body: bool = False,
    ):
        method = method.upper().strip()
        headers: dict[str, str] = {}
        if private:
            headers.update(self._make_auth_headers(params))

        url = f"{self.base_url}{path}"
        
        # 429 에러 재시도 로직
        max_retries = 3
        retry_delay = 1.0  # 초
        
        for attempt in range(max_retries):
            try:
                if method == "GET":
                    resp = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
                elif method == "DELETE":
                    resp = self.session.delete(url, params=params, headers=headers, timeout=self.timeout)
                elif method == "POST":
                    if json_body:
                        headers["Content-Type"] = "application/json"
                        resp = self.session.post(url, json=params or {}, headers=headers, timeout=self.timeout)
                    else:
                        resp = self.session.post(url, data=params or {}, headers=headers, timeout=self.timeout)
                else:
                    raise ValueError(f"지원하지 않는 HTTP 메서드: {method}")

                try:
                    resp.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    # 429 에러 재시도
                    if resp.status_code == 429:
                        if attempt < max_retries - 1:
                            wait_time = retry_delay * (2 ** attempt)  # 지수 백오프
                            logging.warning(f"429 Too Many Requests | {attempt+1}/{max_retries} | {wait_time:.1f}초 대기 후 재시도")
                            time.sleep(wait_time)
                            continue
                        else:
                            raise ValueError(f"API 레이트 리밋 초과 (429) | {max_retries}회 재시도 실패") from e
                    
                    # 401 에러 상세 메시지
                    if resp.status_code == 401:
                        error_msg = (
                            f"\n{'='*60}\n"
                            f"❌ API 인증 실패 (401 Unauthorized)\n"
                            f"{'='*60}\n\n"
                            f"원인:\n"
                            f"  1. API 키가 잘못되었거나 만료됨\n"
                            f"  2. API 키 권한이 부족함\n"
                            f"  3. IP 주소가 허용 목록에 없음\n\n"
                            f"해결 방법:\n"
                            f"  1. 업비트 웹사이트 로그인\n"
                            f"  2. 마이페이지 > Open API 관리 접속\n"
                            f"  3. API 키 확인 또는 재발급\n"
                            f"  4. 필요 권한 체크:\n"
                            f"     ☑ 자산 조회\n"
                            f"     ☑ 주문 조회\n"
                            f"     ☑ 주문하기 (실거래 시)\n"
                            f"  5. .env 파일에 올바른 키 입력:\n"
                            f"     UPBIT_ACCESS_KEY=실제_액세스_키\n"
                            f"     UPBIT_SECRET_KEY=실제_시크릿_키\n\n"
                            f"현재 Access Key 앞 10자: {self.access_key[:10]}...\n"
                            f"{'='*60}\n"
                        )
                        raise ValueError(error_msg) from e
                    raise
                
                return resp.json()
                
            except (requests.exceptions.ConnectionError, 
                    requests.exceptions.Timeout,
                    requests.exceptions.RequestException) as e:
                # 네트워크 오류 재시도
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logging.warning(f"네트워크 오류 | {attempt+1}/{max_retries} | {wait_time:.1f}초 대기 후 재시도 | {type(e).__name__}")
                    time.sleep(wait_time)
                    continue
                else:
                    raise ValueError(f"네트워크 오류 | {max_retries}회 재시도 실패 | {e}") from e

    def get_markets(self, is_details: bool = True) -> list[dict]:
        return self._request("GET", "/v1/market/all", params={"isDetails": str(is_details).lower()}, private=False)

    def get_tickers(self, markets: list[str]) -> dict[str, dict]:
        if not markets:
            return {}
        items = self._request("GET", "/v1/ticker", params={"markets": ",".join(markets)}, private=False)
        return {item["market"]: item for item in items}

    def get_orderbooks(self, markets: list[str], count: int = 5) -> dict[str, dict]:
        if not markets:
            return {}
        items = self._request("GET", "/v1/orderbook", params={"markets": ",".join(markets)}, private=False)
        out: dict[str, dict] = {}
        for item in items:
            copied = dict(item)
            units = list(copied.get("orderbook_units", []) or [])
            copied["orderbook_units"] = units[:count] if count > 0 else units
            out[item["market"]] = copied
        return out

    def get_minute_candles(self, market: str, unit: int, count: int, to: str | None = None) -> list[dict]:
        params = {"market": market, "count": count}
        if to:
            params["to"] = to
        return self._request("GET", f"/v1/candles/minutes/{unit}", params=params, private=False)

    def get_accounts(self) -> list[dict]:
        return self._request("GET", "/v1/accounts", private=True)

    def get_order_chance(self, market: str) -> dict:
        return self._request("GET", "/v1/orders/chance", params={"market": market}, private=True)

    def get_all_open_orders(self, states: list[str] | None = None, market: str | None = None, limit: int = 100, order_by: str = "desc") -> list[dict]:
        results: list[dict] = []
        seen: set[tuple[str, str]] = set()
        page = 1
        while True:
            params: dict[str, object] = {"page": page, "limit": limit, "order_by": order_by}
            if market:
                params["market"] = market
            if states:
                if len(states) == 1:
                    params["state"] = states[0]
                else:
                    params["states[]"] = states
            items = self._request("GET", "/v1/orders/open", params=params, private=True)
            if not items:
                break
            for item in items:
                key = (str(item.get("uuid", "") or ""), str(item.get("identifier", "") or ""))
                if key in seen:
                    continue
                seen.add(key)
                results.append(item)
            if len(items) < limit:
                break
            page += 1
        return results

    def cancel_order(self, order_uuid: str | None = None, identifier: str | None = None) -> dict:
        params = {}
        if order_uuid:
            params["uuid"] = order_uuid
        elif identifier:
            params["identifier"] = identifier
        else:
            raise ValueError("order_uuid 또는 identifier 중 하나는 필요합니다.")
        return self._request("DELETE", "/v1/order", params=params, private=True)

    def _normalize_limit_order_params(self, *, market: str, side: str, price: float, volume: float, identifier: str | None, time_in_force: str | None, volume_decimals: int) -> dict[str, str]:
        params: dict[str, str] = {
            "market": market,
            "side": side,
            "ord_type": "limit",
            "price": num_to_str(price),
            "volume": num_to_str(volume, places=volume_decimals),
        }
        if identifier:
            params["identifier"] = identifier
        if time_in_force:
            params["time_in_force"] = time_in_force
        return params

    def place_limit_buy(self, *, market: str, price: float, volume: float, order_mode: str, identifier: str | None = None, time_in_force: str | None = None, volume_decimals: int = 8) -> dict:
        params = self._normalize_limit_order_params(market=market, side="bid", price=price, volume=volume, identifier=identifier, time_in_force=time_in_force, volume_decimals=volume_decimals)
        if order_mode == "live":
            return self._request("POST", "/v1/orders", params=params, private=True, json_body=True)
        if order_mode == "test":
            return self._request("POST", "/v1/orders/test", params=params, private=True, json_body=True)
        return {"uuid": None, "identifier": identifier, "state": "signal", **params}

    def place_limit_sell(self, *, market: str, price: float, volume: float, order_mode: str, identifier: str | None = None, time_in_force: str | None = None, volume_decimals: int = 8) -> dict:
        params = self._normalize_limit_order_params(market=market, side="ask", price=price, volume=volume, identifier=identifier, time_in_force=time_in_force, volume_decimals=volume_decimals)
        if order_mode == "live":
            return self._request("POST", "/v1/orders", params=params, private=True, json_body=True)
        if order_mode == "test":
            return self._request("POST", "/v1/orders/test", params=params, private=True, json_body=True)
        return {"uuid": None, "identifier": identifier, "state": "signal", **params}

    def cancel_and_new_limit(self, *, prev_order_uuid: str | None, prev_order_identifier: str | None, new_price: float, new_volume: float, new_identifier: str | None = None, new_time_in_force: str | None = None, volume_decimals: int = 8) -> dict:
        params: dict[str, str] = {
            "new_ord_type": "limit",
            "new_price": num_to_str(new_price),
            "new_volume": num_to_str(new_volume, places=volume_decimals),
        }
        if prev_order_uuid:
            params["prev_order_uuid"] = prev_order_uuid
        elif prev_order_identifier:
            params["prev_order_identifier"] = prev_order_identifier
        else:
            raise ValueError("prev_order_uuid 또는 prev_order_identifier 중 하나는 필요합니다.")
        if new_identifier:
            params["new_identifier"] = new_identifier
        if new_time_in_force:
            params["new_time_in_force"] = new_time_in_force
        return self._request("POST", "/v1/orders/cancel_and_new", params=params, private=True, json_body=True)


def get_chance_cached(client: UpbitClient, chance_cache: "ChanceCache", market: str) -> dict:
    cached = chance_cache.get(market)
    if cached is not None:
        return cached
    chance = client.get_order_chance(market)
    chance_cache.set(market, chance)
    return chance


# =========================
# 공통 유틸
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_json_dumps(value) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def num_to_str(value: float | Decimal, places: int | None = None) -> str:
    d = Decimal(str(value))
    if places is not None:
        q = Decimal("1").scaleb(-places)
        d = d.quantize(q, rounding=ROUND_DOWN)
    return format(d.normalize(), "f")


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def make_identifier(prefix: str, side: str) -> str:
    return f"{prefix}-{side}-{int(time.time() * 1000)}"


def parse_upbit_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


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


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "(없음)"

    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def fmt(row: list[str]) -> str:
        return " | ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))

    sep = "-+-".join("-" * w for w in widths)
    out = [fmt(headers), sep]
    for row in rows:
        out.append(fmt(row))
    return "\n".join(out)


def clear_console() -> None:
    try:
        if not sys.stdout.isatty():
            return
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def move_cursor_up(lines: int) -> None:
    """커서를 위로 이동 (ANSI 이스케이프)"""
    if lines > 0 and sys.stdout.isatty():
        sys.stdout.write(f"\033[{lines}A")
        sys.stdout.flush()


def clear_from_cursor() -> None:
    """커서 위치부터 화면 끝까지 지우기"""
    if sys.stdout.isatty():
        sys.stdout.write("\033[J")
        sys.stdout.flush()


def hide_cursor() -> None:
    """커서 숨기기 (깜빡임 방지)"""
    if sys.stdout.isatty():
        sys.stdout.write("\033[?25l")
        sys.stdout.flush()


def show_cursor() -> None:
    """커서 보이기"""
    if sys.stdout.isatty():
        sys.stdout.write("\033[?25h")
        sys.stdout.flush()



def setup_logging(log_file: str) -> None:
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)


# =========================
# 지표 계산
# =========================
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


# =========================
# 보조 함수
# =========================
def required_candle_count(ma_short_period: int, ma_long_period: int, rsi_period: int | None = None) -> int:
    required = max(ma_short_period, ma_long_period) + 1
    if rsi_period is not None:
        required = max(required, rsi_period + 1)
    return required


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


# =========================
# 설정
# =========================
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


# =========================
# 상태 / 로그 저장
# =========================
class MultiMarketStateStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.state = {"markets": {}, "global": {}}
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
            self.state.update(loaded)
            # 구버전 파일에 global 키 없을 경우 보완
            if "global" not in self.state:
                self.state["global"] = {}
        except Exception as e:
            logging.warning("bot_state 파일 로드 실패. 기본값 사용. path=%s error=%s", self.path, e)

    def save(self) -> None:
        """임시 파일에 먼저 쓴 뒤 rename하여 atomic write를 보장합니다.
        크래시나 전원 차단 시에도 기존 파일이 손상되지 않습니다."""
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp_path.replace(self.path)   # POSIX에서 rename은 원자적 연산

    def _ensure_market(self, market: str) -> None:
        if market not in self.state["markets"]:
            self.state["markets"][market] = {
                "cooldown_until_epoch": 0.0,
                "cooldown_reason": "",
                "pending_exit_reason": "",
                "last_exit_time": "",
                "prev_base_total": 0.0,
                "entry_epoch": 0.0,
                "max_price_since_entry": 0.0,
                "partial_exited": False,
                "breakeven_activated": False,
            }
        if "max_price_since_entry" not in self.state["markets"][market]:
            self.state["markets"][market]["max_price_since_entry"] = 0.0
        if "partial_exited" not in self.state["markets"][market]:
            self.state["markets"][market]["partial_exited"] = False
        if "breakeven_activated" not in self.state["markets"][market]:
            self.state["markets"][market]["breakeven_activated"] = False

    def _ensure_global(self) -> None:
        g = self.state.setdefault("global", {})
        g.setdefault("stop_loss_times", [])        # 손절 발생 epoch 리스트
        g.setdefault("circuit_breaker_until", 0.0) # 서킷브레이커 해제 epoch

    def cooldown_remaining_sec(self, market: str) -> float:
        self._ensure_market(market)
        remaining = float(self.state["markets"][market]["cooldown_until_epoch"]) - time.time()
        return max(0.0, remaining)

    def cooldown_active(self, market: str) -> bool:
        return self.cooldown_remaining_sec(market) > 0

    def set_cooldown(self, market: str, seconds: int, reason: str) -> None:
        self._ensure_market(market)
        self.state["markets"][market]["cooldown_until_epoch"] = time.time() + max(0, seconds)
        self.state["markets"][market]["cooldown_reason"] = reason
        self.state["markets"][market]["last_exit_time"] = now_str()
        self.save()

    def set_pending_exit_reason(self, market: str, reason: str) -> None:
        self._ensure_market(market)
        self.state["markets"][market]["pending_exit_reason"] = reason
        self.save()

    def pop_pending_exit_reason(self, market: str) -> str:
        self._ensure_market(market)
        reason = str(self.state["markets"][market].get("pending_exit_reason", "") or "")
        self.state["markets"][market]["pending_exit_reason"] = ""
        self.save()
        return reason

    def get_pending_exit_reason(self, market: str) -> str:
        """
        pending_exit_reason을 읽기만 하고 지우지 않습니다.
        단순 조회 목적에는 pop 대신 이 메서드를 사용하세요.
        pop은 포지션이 실제로 종료됐을 때만 사용합니다.
        """
        self._ensure_market(market)
        return str(self.state["markets"][market].get("pending_exit_reason", "") or "")

    def get_prev_base_total(self, market: str) -> float:
        self._ensure_market(market)
        return float(self.state["markets"][market].get("prev_base_total", 0.0) or 0.0)

    def set_prev_base_total(self, market: str, value: float) -> None:
        self._ensure_market(market)
        self.state["markets"][market]["prev_base_total"] = float(value)
        self.save()

    def set_entry_now(self, market: str) -> None:
        self._ensure_market(market)
        self.state["markets"][market]["entry_epoch"] = time.time()
        self.state["markets"][market]["max_price_since_entry"] = 0.0
        self.save()

    def clear_entry(self, market: str) -> None:
        self._ensure_market(market)
        self.state["markets"][market]["entry_epoch"] = 0.0
        self.state["markets"][market]["max_price_since_entry"] = 0.0
        self.state["markets"][market]["partial_exited"] = False
        self.state["markets"][market]["breakeven_activated"] = False
        self.save()

    def hold_seconds(self, market: str) -> float:
        self._ensure_market(market)
        entry_epoch = float(self.state["markets"][market].get("entry_epoch", 0.0) or 0.0)
        if entry_epoch <= 0:
            return 0.0
        return max(0.0, time.time() - entry_epoch)

    # ── 최고가 추적 (트레일링 스탑용) ─────────────────────────────────────────
    def update_max_price(self, market: str, current_price: float) -> None:
        """보유 중 최고가 갱신 — 매 루프마다 호출"""
        self._ensure_market(market)
        prev = float(self.state["markets"][market].get("max_price_since_entry", 0.0) or 0.0)
        if current_price > prev:
            self.state["markets"][market]["max_price_since_entry"] = current_price
            self.save()

    def get_max_price(self, market: str) -> float:
        self._ensure_market(market)
        return float(self.state["markets"][market].get("max_price_since_entry", 0.0) or 0.0)

    # ── 연속 손절 서킷브레이커 ───────────────────────────────────────────────
    def record_stop_loss(self, window_sec: int) -> int:
        """손절 시간 기록 후 window 내 손절 횟수 반환"""
        self._ensure_global()
        now = time.time()
        times: list = self.state["global"]["stop_loss_times"]
        times.append(now)
        # window 밖의 오래된 기록 제거
        self.state["global"]["stop_loss_times"] = [t for t in times if now - t <= window_sec]
        self.save()
        return len(self.state["global"]["stop_loss_times"])

    def activate_circuit_breaker(self, cooldown_sec: int) -> None:
        self._ensure_global()
        until = time.time() + cooldown_sec
        self.state["global"]["circuit_breaker_until"] = until
        self.state["global"]["stop_loss_times"] = []   # 카운터 리셋
        logging.warning(
            "⚡ 서킷브레이커 발동 | %d초 동안 신규 진입 차단 | 해제: %s",
            cooldown_sec,
            datetime.fromtimestamp(until).strftime("%Y-%m-%d %H:%M:%S"),
        )
        self.save()

    def is_circuit_breaker_active(self) -> bool:
        self._ensure_global()
        return time.time() < float(self.state["global"].get("circuit_breaker_until", 0.0) or 0.0)

    def circuit_breaker_remaining_sec(self) -> float:
        self._ensure_global()
        remaining = float(self.state["global"].get("circuit_breaker_until", 0.0) or 0.0) - time.time()
        return max(0.0, remaining)

    # ── 부분 익절 추적 ──────────────────────────────────────────────────────
    def is_partial_exited(self, market: str) -> bool:
        self._ensure_market(market)
        return bool(self.state["markets"][market].get("partial_exited", False))

    def set_partial_exited(self, market: str) -> None:
        self._ensure_market(market)
        self.state["markets"][market]["partial_exited"] = True
        self.save()

    def clear_partial_exited(self, market: str) -> None:
        """
        부분 익절 주문이 취소된 경우 partial_exited를 False로 초기화합니다.
        미체결 주문 취소 후 다음 조건 충족 시 재시도를 허용합니다.
        """
        self._ensure_market(market)
        if self.state["markets"][market].get("partial_exited"):
            self.state["markets"][market]["partial_exited"] = False
            self.save()

    # ── 브레이크이븐 스탑 활성화 추적 ─────────────────────────────────────────
    def is_breakeven_activated(self, market: str) -> bool:
        self._ensure_market(market)
        return bool(self.state["markets"][market].get("breakeven_activated", False))

    def set_breakeven_activated(self, market: str) -> None:
        self._ensure_market(market)
        if not self.state["markets"][market].get("breakeven_activated"):
            self.state["markets"][market]["breakeven_activated"] = True
            self.save()


class CsvJournal:
    def __init__(self, status_csv_file: str, trade_csv_file: str):
        self.status_csv_path = Path(status_csv_file)
        self.trade_csv_path = Path(trade_csv_file)
        self._ensure_headers()

    def _ensure_headers(self) -> None:
        if not self.status_csv_path.exists():
            with self.status_csv_path.open("w", newline="", encoding="utf-8-sig") as f:
                csv.writer(f).writerow([
                    "timestamp", "market", "score", "current_price", "best_bid", "best_ask", "spread_pct",
                    "ma_short", "ma_long", "rsi", "htf_ma_short", "htf_ma_long",
                    "trend_up", "htf_trend_up", "pullback_ok", "volume_ratio", "volume_ok", "spread_ok",
                    "cooldown_active", "cooldown_remaining_sec", "quote_balance", "base_total", "avg_buy_price",
                    "position_krw", "pnl_pct", "open_order_count", "buy_ok", "buy_reason", "sell_ok", "sell_reason"
                ])

        if not self.trade_csv_path.exists():
            with self.trade_csv_path.open("w", newline="", encoding="utf-8-sig") as f:
                csv.writer(f).writerow([
                    "timestamp", "event_type", "mode", "market", "side", "price", "volume", "krw_amount",
                    "uuid", "identifier", "state", "message", "response_json"
                ])

    def append_status(
        self,
        *,
        market: str,
        score: float,
        current_price: float,
        best_bid: float,
        best_ask: float,
        strat: dict,
        snap: dict,
        open_order_count: int,
        buy_ok: bool,
        buy_reason: str,
        sell_ok: bool,
        sell_reason: str,
    ) -> None:
        with self.status_csv_path.open("a", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow([
                now_str(), market, f"{score:.4f}", f"{current_price:.0f}", f"{best_bid:.0f}", f"{best_ask:.0f}",
                f"{strat['spread_pct']:.8f}", f"{strat['ma_short']:.8f}", f"{strat['ma_long']:.8f}",
                f"{strat['rsi']:.8f}", f"{strat['htf_ma_short']:.8f}", f"{strat['htf_ma_long']:.8f}",
                int(strat["trend_up"]), int(strat["htf_trend_up"]), int(strat["pullback_ok"]),
                f"{strat['volume_ratio']:.8f}", int(strat["volume_ok"]), int(strat["spread_ok"]),
                int(strat["cooldown_active"]), f"{strat['cooldown_remaining_sec']:.2f}",
                f"{snap['quote_balance']:.8f}", f"{snap['base_total']:.8f}", f"{snap['avg_buy_price']:.8f}",
                f"{snap['position_krw']:.8f}", f"{snap['pnl_pct']:.8f}", open_order_count,
                int(buy_ok), buy_reason, int(sell_ok), sell_reason
            ])

    def append_trade(
        self,
        *,
        event_type: str,
        mode: str,
        market: str,
        side: str,
        price: float | None,
        volume: float | None,
        krw_amount: float | None,
        order_uuid: str | None,
        identifier: str | None,
        state: str | None,
        message: str,
        response_json,
    ) -> None:
        with self.trade_csv_path.open("a", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow([
                now_str(), event_type, mode, market, side,
                "" if price is None else f"{price:.8f}",
                "" if volume is None else f"{volume:.12f}",
                "" if krw_amount is None else f"{krw_amount:.8f}",
                order_uuid or "", identifier or "", state or "", message, safe_json_dumps(response_json)
            ])


# =========================
# 캐시
# =========================
class MarketInfoCache:
    def __init__(self, refresh_sec: int):
        self.refresh_sec = refresh_sec
        self.data: dict[str, dict] = {}
        self.last_refresh_epoch = 0.0

    def expired(self) -> bool:
        return (time.time() - self.last_refresh_epoch) >= self.refresh_sec

    def update(self, items: list[dict]) -> None:
        self.data = {item["market"]: item for item in items}
        self.last_refresh_epoch = time.time()

    def get(self, market: str) -> dict:
        return self.data.get(market, {})


class ChanceCache:
    def __init__(self, ttl_sec: int):
        self.ttl_sec = ttl_sec
        self.data: dict[str, dict] = {}

    def get(self, market: str) -> dict | None:
        item = self.data.get(market)
        if not item:
            return None
        if (time.time() - item["ts"]) > self.ttl_sec:
            return None
        return item["value"]

    def set(self, market: str, value: dict) -> None:
        self.data[market] = {"ts": time.time(), "value": value}


class AccountCache:
    """계좌 정보 스마트 캐시 - 변화 감지 기반 갱신"""

    def __init__(self, ttl_sec: int = 3):   # 기본 TTL 5→3초로 단축
        self.ttl = ttl_sec
        self.last_update = 0.0
        self.data: list[dict] = []
        self.prev_positions: set[str] = set()   # 보유 종목 추적 (market 형식)
        self.prev_balances: dict[str, float] = {}  # 통화별 잔고 추적
        self._force_refresh = False  # 주문 체결 직후 강제 갱신 플래그

    def invalidate(self) -> None:
        """주문 체결/취소 직후 호출하여 다음 루프에서 즉시 재조회"""
        self._force_refresh = True

    def needs_update(self, current_positions: set[str]) -> bool:
        """보유 종목이 변했는지 확인"""
        return self.prev_positions != current_positions

    def has_balance_changed(self, new_accounts: list[dict]) -> bool:
        """잔고가 유의미하게 변했는지 확인 (0.1% 이상)"""
        if not self.prev_balances:
            return True

        for acc in new_accounts:
            currency = acc.get("currency")
            if not currency:
                continue
            new_total = float(acc.get("balance", 0) or 0) + float(acc.get("locked", 0) or 0)
            old_total = self.prev_balances.get(currency, 0.0)

            if old_total == 0:
                if new_total > 0:
                    return True
            else:
                change_pct = abs(new_total - old_total) / old_total
                if change_pct > 0.001:
                    return True

        # 사라진 종목 체크
        current_currencies = {acc.get("currency") for acc in new_accounts if acc.get("currency")}
        if set(self.prev_balances.keys()) != current_currencies:
            return True

        return False

    def _refresh(self, client, current_positions: set[str]) -> None:
        """내부 갱신 — API 호출 후 스냅샷 저장"""
        self.data = client.get_accounts()
        self.last_update = time.time()
        self._force_refresh = False
        self.prev_positions = current_positions.copy()
        self.prev_balances = {
            acc.get("currency"): float(acc.get("balance", 0) or 0) + float(acc.get("locked", 0) or 0)
            for acc in self.data if acc.get("currency")
        }

    def get(self, client, current_positions: set[str]) -> list[dict]:
        """계좌 정보 조회 (필요 시에만 API 호출)"""
        now = time.time()

        # 1. 강제 갱신 플래그 (주문 체결/취소 직후)
        if self._force_refresh:
            self._refresh(client, current_positions)
            return self.data

        # 2. TTL 만료 시 무조건 갱신
        if now - self.last_update > self.ttl:
            self._refresh(client, current_positions)
            return self.data

        # 3. TTL 내라도 보유 종목 변화 감지 시 갱신 여부 확인
        if self.needs_update(current_positions):
            temp_data = client.get_accounts()
            if self.has_balance_changed(temp_data):
                self.data = temp_data
                self.last_update = now
                self._force_refresh = False
                self.prev_positions = current_positions.copy()
                self.prev_balances = {
                    acc.get("currency"): float(acc.get("balance", 0) or 0) + float(acc.get("locked", 0) or 0)
                    for acc in self.data if acc.get("currency")
                }
            else:
                # 잔고 변화 없으면 prev_positions만 업데이트
                self.prev_positions = current_positions.copy()

        return self.data


class ClosedCandleCache:
    def __init__(self, min_interval_sec: float = 0.5):  # 0.12 → 0.5초로 확대
        self.cache: dict[tuple[str, int], dict] = {}
        self.min_interval_sec = min_interval_sec
        self.last_fetch_epoch = 0.0
        self.rate_limit_cooldown_until = 0.0  # 429 에러 후 쿨다운

    @staticmethod
    def current_bucket_start_utc(unit: int) -> datetime:
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        minute = (now.minute // unit) * unit
        return now.replace(minute=minute)

    @staticmethod
    def to_param_from_bucket_start(bucket_start_utc: datetime) -> str:
        return bucket_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _pace(self) -> None:
        """API 호출 속도 제어"""
        # 1. 429 에러 후 쿨다운 체크
        if time.time() < self.rate_limit_cooldown_until:
            wait = self.rate_limit_cooldown_until - time.time()
            if wait > 0:
                logging.warning(f"레이트 리밋 쿨다운 중 | {wait:.1f}초 대기")
                time.sleep(wait)
        
        # 2. 최소 간격 준수
        gap = time.time() - self.last_fetch_epoch
        if gap < self.min_interval_sec:
            time.sleep(self.min_interval_sec - gap)
        self.last_fetch_epoch = time.time()
    
    def set_rate_limit_cooldown(self, seconds: float = 3.0) -> None:
        """429 에러 발생 시 쿨다운 설정"""
        self.rate_limit_cooldown_until = time.time() + seconds
        logging.warning(f"레이트 리밋 쿨다운 설정 | {seconds}초")

    def get_closed_candles(
        self,
        *,
        client,
        market: str,
        unit: int,
        count: int,
        refresh_only_on_new_bucket: bool,
    ) -> list[dict]:
        bucket = self.current_bucket_start_utc(unit)
        key = (market, unit)
        cached = self.cache.get(key)

        if refresh_only_on_new_bucket and cached:
            if cached.get("bucket") == bucket and cached.get("count", 0) >= count:
                return cached["data"][:count]

        request_count = max(count, cached.get("count", 0) if cached else 0)
        
        try:
            self._pace()
            data = client.get_minute_candles(
                market=market,
                unit=unit,
                count=request_count,
                to=self.to_param_from_bucket_start(bucket),
            )
            self.cache[key] = {"bucket": bucket, "count": request_count, "data": data}
            return data[:count]
        
        except ValueError as e:
            # 429 에러 발생 시 쿨다운 설정
            if "429" in str(e) or "레이트 리밋" in str(e):
                self.set_rate_limit_cooldown(5.0)
            raise


class DynamicMarketCache:
    def __init__(self, refresh_sec: int):
        self.refresh_sec = refresh_sec
        self.last_refresh_epoch = 0.0
        self.markets: list[str] = []

    def expired(self) -> bool:
        return (time.time() - self.last_refresh_epoch) >= self.refresh_sec

    def update(self, markets: list[str]) -> None:
        self.markets = markets[:]
        self.last_refresh_epoch = time.time()


# =========================
# 전략 계산
# =========================
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


def get_fee_rates(client: UpbitClient, chance_cache: ChanceCache, cfg: Config, market: str) -> tuple[float, float]:
    chance = get_chance_cached(client, chance_cache, market)
    bid_fee = float(chance.get("bid_fee", 0) or 0)
    ask_fee = float(chance.get("ask_fee", 0) or 0)
    maker_bid_fee = float(chance.get("maker_bid_fee", bid_fee) or bid_fee)
    maker_ask_fee = float(chance.get("maker_ask_fee", ask_fee) or ask_fee)

    if cfg.limit_time_in_force == "post_only":
        return maker_bid_fee, maker_ask_fee
    return bid_fee, ask_fee


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


# =========================
# 주문 관련
# =========================
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


def describe_position_state(analysis: dict) -> tuple[str, str]:
    snap = analysis["snap"]
    sell_orders = analysis.get("sell_orders", []) or []
    buy_orders = analysis.get("buy_orders", []) or []
    sell_ok = bool(analysis.get("sell_ok"))
    sell_reason = str(analysis.get("sell_reason", "") or "")

    if snap.get("base_total", 0) <= 0:
        if buy_orders:
            return "매수 주문 대기", "매수 체결 대기"
        return "포지션 없음", "보유 수량 없음"

    if sell_orders:
        return "매도 주문 대기", f"기존 매도 주문 체결 대기 ({len(sell_orders)}건)"
    if sell_ok:
        return "매도 조건 충족", sell_reason or "매도 조건 충족"
    if buy_orders:
        return "보유+매수주문", "기존 매수 주문 정리/체결 대기"
    return "보유 대기", sell_reason or "매도 조건 대기"


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


# =========================
# 동적 종목 선정
# =========================
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


# =========================
# WFA 자동 최적화 스케줄러
# =========================
class WFAScheduler(threading.Thread):
    """
    백그라운드 데몬 스레드로 주기적으로 grid search를 실행하고
    최적 파라미터를 wfa_best_params.json에 저장합니다.
    auto_wfa_apply_params=True 시 봇이 다음 루프에서 파라미터를 자동 적용합니다.
    """

    PARAMS_FILE = Path("wfa_best_params.json")

    def __init__(self, cfg: Config, cfg_lock: threading.Lock):
        super().__init__(daemon=True, name="WFAScheduler")
        self.cfg = cfg
        self.cfg_lock = cfg_lock   # 메인 루프와 cfg 공유 잠금
        self.last_run_epoch: float = 0.0
        self._load_last_run()

    def _load_last_run(self) -> None:
        if self.PARAMS_FILE.exists():
            try:
                data = json.loads(self.PARAMS_FILE.read_text(encoding="utf-8"))
                self.last_run_epoch = float(data.get("run_epoch", 0.0))
            except Exception:
                pass

    def run(self) -> None:
        while True:
            try:
                interval_sec = self.cfg.auto_wfa_interval_hours * 3600
                if time.time() - self.last_run_epoch >= interval_sec:
                    self._run_optimization()
                    self.last_run_epoch = time.time()
            except Exception as e:
                logging.error("WFAScheduler 오류: %s", e)
            time.sleep(600)  # 10분마다 체크

    def _run_optimization(self) -> None:
        candle_file = (
            f"data/{self.cfg.auto_wfa_market.replace('-', '_')}"
            f"_{self.cfg.candle_unit}m.csv"
        )
        backtest_script = Path(__file__).parent / "backtest_improved.py"
        if not backtest_script.exists():
            backtest_script = Path(__file__).parent / "backtest.py"

        if not backtest_script.exists():
            logging.warning("WFA: backtest.py 파일 없음 — 최적화 건너뜀")
            return
        if not Path(candle_file).exists():
            logging.warning("WFA: 캔들 파일 없음 | path=%s", candle_file)
            return

        logging.info("WFA 자동 최적화 시작 | market=%s | candle=%s",
                     self.cfg.auto_wfa_market, candle_file)
        cmd = [
            sys.executable, str(backtest_script), "grid",
            "--market", self.cfg.auto_wfa_market,
            "--candle", candle_file,
            "--unit", str(self.cfg.candle_unit),
            "--htf-unit", str(self.cfg.htf_candle_unit),
            "--max-combos", "50",
            "--out-dir", "wfa_results",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                self._save_best_params()
                logging.info("WFA 자동 최적화 완료")
            else:
                logging.warning("WFA 최적화 실패 | stderr=%s", result.stderr[:200])
        except subprocess.TimeoutExpired:
            logging.warning("WFA 최적화 타임아웃 (5분 초과)")
        except Exception as e:
            logging.error("WFA 최적화 실행 오류: %s", e)

    def _save_best_params(self) -> None:
        import glob as _glob
        json_files = _glob.glob("wfa_results/result_*.json")
        best = None
        best_composite = -float("inf")

        # 파일을 수정 시간 기준 최신순 정렬 후 최근 실행 결과만 사용
        # (같은 실행 배치에서 나온 파일들만 비교 — 오래된 결과 혼입 방지)
        import os as _os
        json_files_sorted = sorted(json_files, key=lambda f: _os.path.getmtime(f), reverse=True)

        # 가장 최근 파일의 mtime 기준 ±10분 이내 파일만 사용
        if not json_files_sorted:
            return
        latest_mtime = _os.path.getmtime(json_files_sorted[0])
        recent_files = [
            f for f in json_files_sorted
            if abs(_os.path.getmtime(f) - latest_mtime) <= 600  # 10분 이내
        ]

        for f in recent_files:
            try:
                data = json.loads(Path(f).read_text(encoding="utf-8"))
                if data.get("n_trades", 0) < 5:
                    continue
                mdd       = data.get("max_drawdown_pct", 100.0)
                sharpe    = data.get("sharpe_ratio", 0.0)
                composite = sharpe / (1 + mdd / 100)
                if composite > best_composite:
                    best_composite = composite
                    best = data
            except Exception:
                continue
        if best is None:
            return
        output = {
            "run_epoch": time.time(),
            "run_time": now_str(),
            "market": self.cfg.auto_wfa_market,
            "best_params": best.get("params", {}),
            "composite_score": best_composite,
            "n_trades": best.get("n_trades", 0),
            "win_rate": best.get("win_rate", 0.0),
            "sharpe": best.get("sharpe_ratio", 0.0),
            "mdd": best.get("max_drawdown_pct", 0.0),
        }
        self.PARAMS_FILE.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logging.info(
            "WFA 최적 파라미터 저장 | score=%.3f | trades=%d | params=%s",
            best_composite, output["n_trades"], output["best_params"],
        )

    def get_best_params(self) -> dict:
        if not self.PARAMS_FILE.exists():
            return {}
        try:
            data = json.loads(self.PARAMS_FILE.read_text(encoding="utf-8"))
            return data.get("best_params", {})
        except Exception:
            return {}


# =========================
# 메인 실행
# =========================
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
