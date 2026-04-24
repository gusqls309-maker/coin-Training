"""콘솔 출력, 로깅 설정, 시간/문자열 유틸."""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path


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


def make_identifier(prefix: str, side: str) -> str:
    return f"{prefix}-{side}-{int(time.time() * 1000)}"


def parse_upbit_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


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
