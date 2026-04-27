"""WFAScheduler — 백그라운드 주기적 백테스트 실행 + 최적 파라미터 적용."""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from .config import Config
from .display import now_str


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
