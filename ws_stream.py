"""
업비트 실시간 체결(trade) WebSocket 스트림 수신기 — v3.0 초단타 엔진용.

- 업비트 KRW 마켓 중 유의종목 제외 + 24시간 거래대금 30억 이상 종목을 구독
- 각 종목의 틱(체결)을 메모리에 최근 N초(기본 200초) 롤링 윈도우로 보관
- 네트워크 끊김 시 자동 재연결(백오프), 10분마다 종목 리스트 갱신을 위해 재구독
- ask_bid 필드 실측 검증 결과: 'BID' = 매수 체결, 'ASK' = 매도 체결

백그라운드 스레드에서 동작하며, 엔진은 market_metrics()/last_price()로 조회한다.
"""

import json
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))


def _kst(ts):
    """epoch 초를 KST 문자열로 (Render 서버는 UTC 기준)."""
    return datetime.fromtimestamp(ts, KST).strftime("%Y-%m-%d %H:%M:%S")

try:
    from websockets.sync.client import connect as _ws_connect
    WEBSOCKETS_AVAILABLE = True
    WEBSOCKETS_ERROR = None
except Exception as _e:  # pragma: no cover - 라이브러리 미설치 시
    _ws_connect = None
    WEBSOCKETS_AVAILABLE = False
    WEBSOCKETS_ERROR = f"{type(_e).__name__}: {_e}"

WS_URL = "wss://api.upbit.com/websocket/v1"
MAX_SUBSCRIBE_CODES = 180  # 업비트 웹소켓 1회 요청 최대 종목 수
USER_AGENT = {"User-Agent": "Mozilla/5.0 (compatible; dohyeon-sniper-bot/3.0)"}


def _get_json(url, timeout=10):
    req = urllib.request.Request(url, headers=USER_AGENT)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def fetch_active_markets(min_24h_vol_krw=3_000_000_000):
    """유의종목(market_warning != NONE) 제외 + 24시간 거래대금 기준 필터 후 거래대금 내림차순."""
    markets = _get_json("https://api.upbit.com/v1/market/all?is_details=true", timeout=15)
    names, codes = {}, []
    for m in markets:
        code = m.get("market", "")
        if not code.startswith("KRW-"):
            continue
        if m.get("market_warning", "NONE") != "NONE":
            continue  # 유의종목 제외
        codes.append(code)
        names[code] = m.get("korean_name", code)

    tickers = []
    for i in range(0, len(codes), 50):
        batch = codes[i:i + 50]
        try:
            tickers.extend(_get_json(
                "https://api.upbit.com/v1/ticker?markets=" + ",".join(batch), timeout=10))
        except Exception as e:
            print(f"[WS] ticker batch failed: {e}")
    active = [t for t in tickers if t.get("acc_trade_price_24h", 0) >= min_24h_vol_krw]
    active.sort(key=lambda t: t["acc_trade_price_24h"], reverse=True)
    return [t["market"] for t in active], names


class UpbitTradeStream:
    def __init__(self, min_24h_vol_krw=3_000_000_000, window_sec=200, market_refresh_sec=600):
        self.min_vol = min_24h_vol_krw
        self.window_ms = int(window_sec * 1000)
        self.market_refresh_sec = market_refresh_sec

        self._lock = threading.RLock()
        self._ticks = {}          # market -> deque[(ts_ms, price, trade_krw, is_bid)]
        self._names = {}
        self._codes = []          # 구독 중인 기본 종목 리스트
        self._extra = set()       # 보유 포지션 등 추가 구독 종목

        self.connected = False
        self.connected_at = None
        self.last_message_at = None
        self.reconnects = 0
        self.last_error = None
        self._ticks_total = 0
        self._tps_window = deque(maxlen=20000)
        self._started_at = None
        self._markets_loaded_at = 0.0
        self._thread = None
        self._stop = threading.Event()

    # ---------------- lifecycle ----------------
    def start(self):
        if not WEBSOCKETS_AVAILABLE:
            self.last_error = f"websockets library unavailable ({WEBSOCKETS_ERROR})"
            print(f"[WS] {self.last_error}")
            return False
        if self._thread and self._thread.is_alive():
            return True
        self._stop.clear()
        self._started_at = time.time()
        self._thread = threading.Thread(target=self._run, name="upbit-trade-ws", daemon=True)
        self._thread.start()
        print("[WS] Trade stream thread started.")
        return True

    def stop(self):
        self._stop.set()

    # ---------------- subscription ----------------
    def add_extra_markets(self, codes):
        with self._lock:
            self._extra = {c for c in codes if c}

    def markets(self):
        with self._lock:
            return list(self._codes)

    def names(self):
        with self._lock:
            return dict(self._names)

    def _subscribed_codes(self):
        with self._lock:
            extra = set(self._extra)
        base = list(self._codes)
        merged = base + [c for c in extra if c not in base]
        return merged[:MAX_SUBSCRIBE_CODES]

    def _refresh_markets(self):
        try:
            codes, names = fetch_active_markets(self.min_vol)
            with self._lock:
                self._codes = codes[:MAX_SUBSCRIBE_CODES]
                self._names = names
                self._markets_loaded_at = time.time()
            print(f"[WS] Market list refreshed: {len(self._codes)} markets "
                  f"(24h vol >= {self.min_vol/1e8:.0f}억, warning excluded)")
        except Exception as e:
            self.last_error = f"market refresh failed: {type(e).__name__}: {e}"
            print(f"[WS] {self.last_error}")

    # ---------------- stream loop ----------------
    def _run(self):
        backoff = 1
        while not self._stop.is_set():
            try:
                if (time.time() - self._markets_loaded_at) > self.market_refresh_sec \
                        or not self._codes:
                    self._refresh_markets()
                codes = self._subscribed_codes()
                if not codes:
                    raise RuntimeError("no subscribed codes")
                self._consume(codes)
                backoff = 1
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                print(f"[WS] stream error: {self.last_error}")
            finally:
                self.connected = False
            if self._stop.is_set():
                break
            self.reconnects += 1
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, 30)

    def _consume(self, codes):
        sub = [
            {"ticket": f"sniper-v3-{int(time.time())}"},
            {"type": "trade", "codes": codes},
            {"format": "DEFAULT"},
        ]
        with _ws_connect(WS_URL, ping_interval=20, ping_timeout=20,
                         close_timeout=5, max_size=None) as ws:
            ws.send(json.dumps(sub))
            self.connected = True
            self.connected_at = time.time()
            self.last_error = None
            print(f"[WS] Connected to Upbit trade stream ({len(codes)} markets).")
            last_msg = time.time()
            while not self._stop.is_set():
                try:
                    raw = ws.recv(timeout=5)
                except TimeoutError:
                    idle = time.time() - last_msg
                    if idle > 60:
                        raise ConnectionError(f"no message for {idle:.0f}s")
                    if (time.time() - self._markets_loaded_at) > self.market_refresh_sec:
                        print("[WS] Periodic market refresh -> resubscribe")
                        return
                    continue
                if raw is None:
                    raise ConnectionError("socket closed")
                last_msg = time.time()
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get("type") != "trade":
                    continue
                self._on_trade(msg)

    def _on_trade(self, msg):
        try:
            market = msg["code"]
            price = float(msg["trade_price"])
            vol = float(msg["trade_volume"])
            ts = int(msg.get("trade_timestamp") or 0)
            now_ms = int(time.time() * 1000)
            # 서버 시각과 로컬 시각이 크게 벌어지면 로컬 시각으로 보정 (윈도우 계산 왜곡 방지)
            if ts <= 0 or abs(now_ms - ts) > 60_000:
                ts = now_ms
            is_bid = msg.get("ask_bid") == "BID"  # 실측 검증: BID = 매수 체결
        except (KeyError, ValueError, TypeError):
            return

        cutoff = ts - self.window_ms
        with self._lock:
            dq = self._ticks.get(market)
            if dq is None:
                dq = deque()
                self._ticks[market] = dq
            dq.append((ts, price, price * vol, is_bid))
            while dq and dq[0][0] < cutoff:
                dq.popleft()
            self._ticks_total += 1
        self._tps_window.append(time.time())
        self.last_message_at = time.time()

    # ---------------- queries ----------------
    def last_price(self, market):
        with self._lock:
            dq = self._ticks.get(market)
            if not dq:
                return None
            return dq[-1][1]

    def market_metrics(self, market, now_ms=None, short_sec=15, base_sec=180):
        """최근 short_sec초 거래대금/매수체결비율과 직전 base_sec초 평균 15초 거래대금 비교 지표."""
        now_ms = now_ms or int(time.time() * 1000)
        short_start = now_ms - short_sec * 1000
        base_start = now_ms - base_sec * 1000
        with self._lock:
            dq = self._ticks.get(market)
            rows = list(dq) if dq else None
        if not rows:
            return None

        short_krw = short_bid_krw = base_krw = 0.0
        base_ticks = 0
        earliest_ts = None
        last_ts = 0
        first_price = None
        last_price = None
        for ts, price, krw, is_bid in rows:
            if ts < base_start or ts > now_ms + 5_000:
                continue
            if ts <= last_ts:      # 순서 역전 틱 무시
                continue
            last_ts = ts
            if earliest_ts is None:
                earliest_ts = ts
            if ts < short_start:
                base_krw += krw
                base_ticks += 1
                if first_price is None:
                    first_price = price
            else:
                short_krw += krw
                if is_bid:
                    short_bid_krw += krw
                if first_price is None:
                    first_price = price
            last_price = price

        if earliest_ts is None or last_price is None:
            return None

        base_span_sec = max((short_start - max(base_start, earliest_ts)) / 1000.0, 0.0)
        base_span_sec = min(base_span_sec, base_sec - short_sec)
        baseline15 = (base_krw / base_span_sec * short_sec) if base_span_sec > 1.0 else 0.0
        if baseline15 > 0:
            surge_ratio = short_krw / baseline15
        else:
            surge_ratio = 999.0 if short_krw > 0 else 0.0

        chg_pct = ((last_price - first_price) / first_price * 100.0) if first_price else 0.0
        return {
            "market": market,
            "last_price": last_price,
            "short_krw": short_krw,
            "short_bid_krw": short_bid_krw,
            "bid_ratio": (short_bid_krw / short_krw) if short_krw > 0 else 0.0,
            "base_krw": base_krw,
            "base_ticks": base_ticks,
            "baseline_15s_krw": baseline15,
            "surge_ratio": surge_ratio,
            "chg_3m_pct": chg_pct,
            "coverage_sec": (now_ms - earliest_ts) / 1000.0,
            "ticks_in_window": len(rows),
            "last_tick_age_sec": (now_ms - last_ts) / 1000.0,
        }

    def stats(self):
        now = time.time()
        while self._tps_window and self._tps_window[0] < now - 10:
            self._tps_window.popleft()
        span = min(10.0, max(now - (self.connected_at or now), 0.5))
        tps = round(len(self._tps_window) / span, 1)
        with self._lock:
            market_count = len(self._codes)
            tracked = len(self._ticks)
        return {
            "ws_available": WEBSOCKETS_AVAILABLE,
            "ws_connected": self.connected,
            "ws_markets_count": market_count,
            "ws_markets_tracked": tracked,
            "ws_ticks_per_sec": tps,
            "ws_ticks_total": self._ticks_total,
            "ws_reconnects": self.reconnects,
            "ws_last_message_at": _kst(self.last_message_at) if self.last_message_at else None,
            "ws_message_age_sec": round(now - self.last_message_at, 1) if self.last_message_at else None,
            "ws_connected_at": _kst(self.connected_at) if self.connected_at else None,
            "ws_last_error": self.last_error,
            "ws_url": WS_URL,
        }


if __name__ == "__main__":
    s = UpbitTradeStream()
    s.start()
    time.sleep(12)
    print(json.dumps(s.stats(), ensure_ascii=False, indent=1))
    for m in s.markets()[:8]:
        print(json.dumps(s.market_metrics(m), ensure_ascii=False))
    s.stop()
