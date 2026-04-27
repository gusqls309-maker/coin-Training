"""캐시 클래스 — 시장 정보, 주문 기회, 계좌, 캔들, 동적 종목."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone


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


