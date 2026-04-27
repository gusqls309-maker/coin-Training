"""UpbitClient — 업비트 REST API 래퍼."""
from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode, unquote

import jwt
import requests

from .display import num_to_str

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
