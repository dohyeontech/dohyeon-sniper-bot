"""
1분봉 캐시 (v4.0) — RSI·볼린저 전략용 저속 백그라운드 수집기.

설계 원칙 (업비트 REST 레이트리밋 429 방지):
  · 24h 거래대금 상위 15개 종목만 순환 수집
  · 종목당 60초 주기 (같은 종목을 1분에 한 번만 조회)
  · 요청 사이 0.25초 슬립 → 실효 1초당 최대 4회 (업비트 허용치 10회/초의 40%)
  · 별도 스레드에서만 REST를 호출하고, 엔진(1.5초 루프)은 메모리 스냅샷만 읽는다

계산 지표 (모두 '완료된' 1분봉 기준 — 진행 중인 마지막 봉은 제외):
  · RSI(14) 현재값 / 직전값
  · 볼린저밴드(20, 2) 중심·상단·하단, 밴드폭(%)과 최근 40봉 내 백분위
  · 마지막 완료봉 거래량 / 직전 20봉 평균 거래량
"""

import json
import threading
import time
import urllib.request

CANDLE_URL = "https://api.upbit.com/v1/candles/minutes/1"
USER_AGENT = {"User-Agent": "Mozilla/5.0 (compatible; dohyeon-sniper-bot/4.0)"}


def _get_json(url, timeout=8):
    req = urllib.request.Request(url, headers=USER_AGENT)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def rsi(closes, period=14):
    """단순 평균 방식 RSI. closes는 시간 오름차순. 표본 부족 시 None."""
    if not closes or len(closes) < period + 1:
        return None
    window = closes[-(period + 1):]
    gains, losses = 0.0, 0.0
    for i in range(1, len(window)):
        diff = window[i] - window[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def bollinger(closes, period=20, k=2.0):
    """(중심, 상단, 하단) — 모집단 표준편차."""
    if not closes or len(closes) < period:
        return None
    window = closes[-period:]
    mid = sum(window) / period
    var = sum((c - mid) ** 2 for c in window) / period
    sd = var ** 0.5
    return mid, mid + k * sd, mid - k * sd


def _bandwidth(closes, period=20, k=2.0):
    bb = bollinger(closes, period, k)
    if not bb or bb[0] <= 0:
        return None
    mid, up, low = bb
    return (up - low) / mid * 100.0


class CandleCache:
    """거래대금 상위 종목 1분봉을 60초 주기로 순환 캐싱하는 백그라운드 수집기."""

    def __init__(self, market_provider, top_n=15, refresh_sec=60.0,
                 sleep_between=0.25, count=60):
        self.market_provider = market_provider
        self.top_n = top_n
        self.refresh_sec = refresh_sec
        self.sleep_between = sleep_between
        self.count = count

        self._lock = threading.RLock()
        self._data = {}              # market -> snapshot dict
        self._last_fetch = {}        # market -> epoch
        self._stop = threading.Event()
        self._thread = None
        self._started_at = None
        self._requests = 0
        self._errors = 0
        self._last_error = None
        self._last_cycle_at = None
        self._market_count = 0

    # ---------------- lifecycle ----------------
    def start(self):
        if self._thread and self._thread.is_alive():
            return True
        self._stop.clear()
        self._started_at = time.time()
        self._thread = threading.Thread(target=self._run, name="candle-cache", daemon=True)
        self._thread.start()
        print(f"[Candles] 1분봉 캐시 스레드 시작 (상위 {self.top_n}종목 · {self.refresh_sec:.0f}초 주기 · "
              f"{self.sleep_between}s 슬립)")
        return True

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                markets = [m for m in (self.market_provider() or [])][: self.top_n]
                self._market_count = len(markets)
                now = time.time()
                did_fetch = False
                for market in markets:
                    if self._stop.is_set():
                        return
                    if now - self._last_fetch.get(market, 0.0) < self.refresh_sec:
                        continue
                    self._refresh(market)
                    did_fetch = True
                    self._last_fetch[market] = time.time()
                    if self._stop.wait(self.sleep_between):
                        return
                if did_fetch:
                    self._last_cycle_at = time.time()
            except Exception as e:      # 스레드는 어떤 경우에도 죽지 않는다
                self._last_error = f"{type(e).__name__}: {e}"
                self._errors += 1
                print(f"[Candles] loop error: {self._last_error}")
            if self._stop.wait(1.0):
                return

    def _refresh(self, market):
        try:
            raw = _get_json(f"{CANDLE_URL}?market={market}&count={self.count}")
            self._requests += 1
        except Exception as e:
            self._errors += 1
            self._last_error = f"{market}: {type(e).__name__}: {e}"
            print(f"[Candles] fetch failed {market}: {e}")
            return
        if not isinstance(raw, list) or len(raw) < 25:
            self._errors += 1
            self._last_error = f"{market}: insufficient candles"
            return
        snap = self._snapshot_from_raw(market, raw)
        if snap:
            with self._lock:
                self._data[market] = snap

    def _snapshot_from_raw(self, market, raw):
        # raw[0]은 진행 중인 봉 → 완료봉만 사용
        completed = raw[1:]
        if len(completed) < 25:
            return None
        chrono = list(reversed(completed))          # 과거 → 최신
        closes = [float(c["trade_price"]) for c in chrono]
        volumes = [float(c.get("candle_acc_trade_volume") or 0.0) for c in chrono]

        rsi_now = rsi(closes)
        rsi_prev = rsi(closes[:-1]) if len(closes) > 15 else None
        bb = bollinger(closes)
        bw_now = _bandwidth(closes)
        bw_hist = []
        for i in range(max(0, len(closes) - 40), len(closes)):
            bw = _bandwidth(closes[: i + 1])
            if bw is not None:
                bw_hist.append(bw)
        bw_rank = None
        if bw_now is not None and bw_hist:
            bw_rank = sum(1 for v in bw_hist if v <= bw_now) / len(bw_hist)

        vol_ratio = None
        if len(volumes) >= 21:
            base = volumes[-21:-1]
            avg = sum(base) / len(base)
            if avg > 0:
                vol_ratio = volumes[-1] / avg

        return {
            "market": market,
            "updated_ts": time.time(),
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "candles": len(completed),
            "last_close": closes[-1],
            "last_candle_open": float(chrono[-1]["opening_price"]),
            "rsi": rsi_now,
            "rsi_prev": rsi_prev,
            "bb_mid": bb[0] if bb else None,
            "bb_upper": bb[1] if bb else None,
            "bb_lower": bb[2] if bb else None,
            "bandwidth_pct": bw_now,
            "bw_rank": bw_rank,
            "bandwidth_history": [round(v, 4) for v in bw_hist[-40:]],
            "vol_ratio": vol_ratio,
        }

    # ---------------- queries ----------------
    def snapshot(self, market):
        with self._lock:
            snap = self._data.get(market)
            if not snap:
                return None
            out = dict(snap)
        out["age_sec"] = round(time.time() - out["updated_ts"], 1)
        return out

    def markets(self):
        with self._lock:
            return list(self._data.keys())

    def stats(self):
        with self._lock:
            count = len(self._data)
            sample = None
            if self._data:
                newest = max(self._data.values(), key=lambda s: s["updated_ts"])
                sample = {"market": newest["market"],
                          "age_sec": round(time.time() - newest["updated_ts"], 1),
                          "rsi": round(newest["rsi"], 1) if newest.get("rsi") is not None else None,
                          "bb_upper": newest.get("bb_upper")}
        return {
            "enabled": True,
            "markets_cached": count,
            "markets_target": self._market_count or self.top_n,
            "refresh_sec": self.refresh_sec,
            "sleep_between_sec": self.sleep_between,
            "requests": self._requests,
            "errors": self._errors,
            "last_error": self._last_error,
            "last_cycle_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._last_cycle_at))
            if self._last_cycle_at else None,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._started_at))
            if self._started_at else None,
            "sample": sample,
        }


if __name__ == "__main__":
    from ws_stream import UpbitTradeStream
    st = UpbitTradeStream()
    cache = CandleCache(lambda: st.markets(), top_n=3)
    cache.start()
    time.sleep(8)
    print(json.dumps(cache.stats(), ensure_ascii=False, indent=1))
    for m in cache.markets():
        print(json.dumps(cache.snapshot(m), ensure_ascii=False))
        break
    cache.stop()
