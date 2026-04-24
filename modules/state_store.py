"""영속 상태 저장소 — bot_state.json + 거래 저널 CSV."""
from __future__ import annotations

import csv
import json
import logging
import time
from datetime import datetime
from pathlib import Path

from .display import now_str, safe_json_dumps


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
