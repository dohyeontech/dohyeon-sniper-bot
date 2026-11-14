"""v4.0 「10대 단타 전략 실시간 비교 리그」 단위 검증 (합성 틱 스트림 + 합성 1분봉).

검증 범위
  [1] 10전략 구조 (각 200만 원, 총 2,000만 원)
  [2] v3.1 3계좌 → v4.0 무손실 승계 (잔고·전적·포지션·쿨다운·매매내역)
  [3] 기존 3계좌 문턱별 진입 분리
  [4]~[10] 신규 7전략 각각의 진입 트리거 (양성 + 경계 음성)
  [11] 10전략 각각의 청산 트리거 (목표/손절/트레이링/시간초과) + 쿨다운
  [12] 쿨다운·자금 독립
  [13] BTC 필터 (모멘텀·돌파 8전략 차단 + 역추세 2전략 예외)
  [14] 리더보드 정렬 + 집계
  [15] 영속성
  [16] 통합 사이클(매수→청산) 및 승계 계좌 미변경 확인

실행: python3 tests/test_v4_ten_strategies.py
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

import persistence  # noqa: E402
import sniper_engine as E  # noqa: E402
from candles import rsi as calc_rsi, bollinger  # noqa: E402

KST = timezone(timedelta(hours=9))
TMP = os.path.join(HERE, "_test_state.json")

os.environ.pop("GIST_TOKEN", None)
os.environ.pop("GIST_ID", None)
if os.path.exists(TMP):
    os.remove(TMP)
persistence.STATE_FILE = TMP
persistence._state_cache = None
persistence._load_source = "local"

BULL = {"is_bearish": False, "btc_price": 92000000, "sma20": 91500000,
        "diff_pct": 0.55, "trend": "BULLISH"}
BEAR = {"is_bearish": True, "btc_price": 91000000, "sma20": 92000000,
        "diff_pct": -0.35, "trend": "BEARISH"}


class FakeStream:
    """웹소켓 롤링 윈도우 대역 — 종목별 스냅샷을 직접 주입한다."""

    def __init__(self):
        self.snaps = {}
        self.price = {}
        self._names = {}
        self.extra = []
        self.top = []

    def markets(self):
        return list(self.snaps.keys())

    def names(self):
        return dict(self._names)

    def last_price(self, m):
        return self.price.get(m)

    def market_snapshot(self, m, now_ms=None, **kw):
        return self.snaps.get(m)

    def market_metrics(self, m, now_ms=None, short_sec=15, base_sec=180):
        return self.snaps.get(m)

    def top_markets(self, n=10):
        return self.top[:n]

    def add_extra_markets(self, codes):
        self.extra = list(codes)


class FakeCandles:
    def __init__(self, data=None):
        self.data = data or {}

    def snapshot(self, market):
        return self.data.get(market)

    def stats(self):
        return {"enabled": True, "markets_cached": len(self.data)}


def snap(market="KRW-T", price=100.0, short_krw=40_000_000.0, surge=5.0, bid=0.8,
         chg=1.2, **kw):
    """전략 판정에 필요한 모든 필드를 갖춘 스냅샷 기본값."""
    d = {
        "market": market, "last_price": price,
        "short_krw": short_krw, "short_bid_krw": short_krw * bid, "bid_ratio": bid,
        "krw_10s": short_krw / 1.5, "bid_ratio_10s": bid, "ticks_10s": 30,
        "base_krw": 100_000_000.0, "base_ticks": 300,
        "baseline_15s_krw": short_krw / max(surge, 1e-9), "surge_ratio": surge,
        "chg_3m_pct": chg, "low_3m": price * 0.99, "high_10m": price,
        "drawdown_from_high_pct": 0.0, "vwap_3m": price,
        "price_15s_ago": price * 0.995, "price_30s_ago": price * 0.99,
        "chg_15s_pct": 0.5, "coverage_sec": 180.0, "coverage_10m_sec": 600.0,
        "ticks_in_window": 400, "last_tick_age_sec": 0.2, "candles": None,
    }
    d.update(kw)
    return d


def fresh(btc=BULL, candles=None):
    state = E.new_league_state()
    persistence._state_cache = state
    persistence.save_state(state, remote=False)
    E._btc_cache["data"] = dict(btc)
    E._btc_cache["ts"] = time.time()
    E.set_candle_cache(FakeCandles(candles) if candles is not None else None)
    return state


def force_position(state, key, market="KRW-T", price=100.0):
    st = state["strategies"][key]
    st["position"] = {
        "market": market, "name": market.replace("KRW-", ""), "entry_price": price,
        "highest_price": price, "coin_qty": (st["cash_krw"] * (1 - E.FEE_RATE)) / price,
        "cost_krw": st["cash_krw"], "entry_time": E.fmt(E.kst_now()), "entry_ts_ms": E._now_ms(),
        "entry_type": "TEST", "strategy": key, "signal": "test", "last_price": price,
        "unrealized_pct": 0.0,
    }
    st["cash_krw"] = 0.0
    st["status"] = "IN_POSITION"
    return st


OK = 0
FAIL = 0
FAILED = []


def check(label, cond, detail=""):
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  PASS  {label} {detail}")
    else:
        FAIL += 1
        FAILED.append(label)
        print(f"  FAIL  {label} {detail}")


def sig(key, s, state, top=()):
    ctx = {"state": state, "top_markets": set(top), "btc": BULL, "bearish": False}
    s = E._attach_candles(dict(s))
    return E.SIGNAL_FNS[E.STRATEGIES[key]["kind"]](s, state["strategies"][key], ctx)


# ------------------------------------------------------------- [1] 구조
print("[1] 10전략 리그 구조")
s = fresh()
check("version", s["version"] == "4.0", s["version"])
check("mode", s["mode"] == "ten_strategy_league", s["mode"])
check("10 strategies", len(s["strategies"]) == 10, str(len(s["strategies"])))
check("keys", list(s["strategies"].keys()) == list(E.STRATEGY_KEYS), str(list(s["strategies"])))
check("legacy 3 키 유지", tuple(s["tiers"].keys()) == ("3m", "10m", "30m"), str(list(s["tiers"])))
check("each 2M cash", all(s["strategies"][k]["cash_krw"] == 2_000_000 for k in E.STRATEGY_KEYS))
check("each 0 trades", all(s["strategies"][k]["total_trades"] == 0 for k in E.STRATEGY_KEYS))
check("total capital 20M", s["initial_capital"] == 20_000_000, str(s["initial_capital"]))
check("tier thresholds",
      [s["strategies"][k]["params"]["min_short_krw"] for k in ("3m", "10m", "30m")] ==
      [3_000_000, 10_000_000, 30_000_000])
check("rules 10 strategies", len(s["rules"]["strategies"]) == 10)
for k in E.NEW_KEYS:
    p = s["strategies"][k]["params"]
    check(f"{k} params(target/stop/hold)",
          p.get("target_pct") and p.get("stop_pct") and p.get("max_hold_min"),
          f"TP+{p.get('target_pct')} SL{p.get('stop_pct')} {p.get('max_hold_min')}분")

# --------------------------------------------- [2] v3.1 → v4.0 무손실 승계
print("[2] v3.1 3계좌 무손실 승계")
legacy = {
    "version": "3.1", "status": "WAITING_FOR_SURGE", "mode": "multi_tier",
    "initialized_at": "2026-11-14 20:02:55", "initial_capital": 6_000_000,
    "peak_equity": 6_129_514, "current_equity": 5_959_896, "cash_krw": 5_959_896.35,
    "total_trades": 13, "wins": 7, "losses": 6, "total_realized_pnl": -40_103.65,
    "tiers": {
        "30m": {"key": "30m", "label": "3,000만↑", "min_short_krw": 30_000_000,
                "initial_capital": 2_000_000, "cash_krw": 1_960_200.4905405408,
                "position": None, "cooldown_until": None, "total_realized_pnl": -39_799.51,
                "total_trades": 1, "wins": 0, "losses": 1, "peak_equity": 2_000_000,
                "max_drawdown_pct": 1.99, "current_equity": 1_960_200, "current_ret_pct": -1.99,
                "status": "WAITING_FOR_SURGE", "last_event": "손절 1회",
                "recent_trades": [{"type": "BUY", "market": "KRW-A", "name": "에이"},
                                  {"type": "SELL", "market": "KRW-A", "name": "에이",
                                   "gain_pct": -1.99, "pnl_krw": -39_799, "exit_kind": "STOP_LOSS"}]},
        "10m": {"key": "10m", "label": "1,000만↑", "min_short_krw": 10_000_000,
                "initial_capital": 2_000_000, "cash_krw": 1_961_175.003291777,
                "position": None, "cooldown_until": None, "total_realized_pnl": -38_824.99,
                "total_trades": 5, "wins": 3, "losses": 2, "peak_equity": 2_015_000,
                "max_drawdown_pct": 2.6, "current_equity": 1_961_175, "current_ret_pct": -1.94,
                "status": "WAITING_FOR_SURGE", "last_event": "3승 2패",
                "recent_trades": [{"type": "BUY", "market": "KRW-B", "name": "비"}] * 3},
        "3m": {"key": "3m", "label": "300만↑", "min_short_krw": 3_000_000,
               "initial_capital": 2_000_000, "cash_krw": 2_038_520.8563593936,
               "position": None, "cooldown_until": None, "total_realized_pnl": 38_520.86,
               "total_trades": 7, "wins": 4, "losses": 3, "peak_equity": 2_060_000,
               "max_drawdown_pct": 1.1, "current_equity": 2_038_521, "current_ret_pct": 1.93,
               "status": "WAITING_FOR_SURGE", "last_event": "4승 3패",
               "recent_trades": [{"type": "BUY", "market": "KRW-C", "name": "씨"}] * 4},
    },
}
mig = E.migrate_from_v3(legacy)
check("승계 version", mig["version"] == "4.0")
check("승계 3계좌 현금 보존",
      mig["strategies"]["30m"]["cash_krw"] == 1_960_200.4905405408
      and mig["strategies"]["10m"]["cash_krw"] == 1_961_175.003291777
      and mig["strategies"]["3m"]["cash_krw"] == 2_038_520.8563593936)
check("승계 전적 보존",
      [mig["strategies"][k]["total_trades"] for k in ("30m", "10m", "3m")] == [1, 5, 7],
      str([mig["strategies"][k]["total_trades"] for k in ("30m", "10m", "3m")]))
check("승계 승/패 보존",
      [(mig["strategies"][k]["wins"], mig["strategies"][k]["losses"]) for k in ("30m", "10m", "3m")] ==
      [(0, 1), (3, 2), (4, 3)])
check("승계 매매내역 보존",
      [len(mig["strategies"][k]["recent_trades"]) for k in ("30m", "10m", "3m")] == [2, 3, 4],
      str([len(mig["strategies"][k]["recent_trades"]) for k in ("30m", "10m", "3m")]))
check("승계 실현손익 보존",
      abs(mig["strategies"]["3m"]["total_realized_pnl"] - 38_520.86) < 1e-6)
check("승계 MDD/피크 보존",
      mig["strategies"]["10m"]["max_drawdown_pct"] == 2.6
      and mig["strategies"]["10m"]["peak_equity"] == 2_015_000)
check("승계 carried_over 표기", all(mig["strategies"][k]["carried_over"] for k in E.LEGACY_KEYS))
check("신규 7전략 200만 신규 시드",
      all(mig["strategies"][k]["cash_krw"] == 2_000_000
          and mig["strategies"][k]["total_trades"] == 0 for k in E.NEW_KEYS))
check("승계 기록 메타", mig["migrated_from"]["version"] == "3.1"
      and mig["migrated_from"]["trades"] == 13, str(mig["migrated_from"]))
expected_total = 1_960_200.4905405408 + 1_961_175.003291777 + 2_038_520.8563593936 + 7 * 2_000_000
mig = E._aggregate(mig, None)
check("통합 자산 = 승계 3계좌 + 신규 14M", abs(mig["current_equity"] - expected_total) < 2,
      f"{mig['current_equity']} vs {expected_total:.0f}")
check("승계 후에도 3m 1위", mig["leaderboard"][0]["key"] == "3m",
      str([(r["key"], r["ret_pct"]) for r in mig["leaderboard"][:3]]))

# 승계 포지션도 그대로
legacy_pos = json.loads(json.dumps(legacy))
legacy_pos["tiers"]["10m"]["position"] = {
    "market": "KRW-HELD", "name": "보유", "entry_price": 100.0, "highest_price": 101.0,
    "coin_qty": 19611.0, "cost_krw": 2_000_000.0, "entry_time": "2026-11-14 21:00:00",
    "entry_ts_ms": E._now_ms() - 600000, "last_price": 100.5, "unrealized_pct": 0.5}
legacy_pos["tiers"]["10m"]["cash_krw"] = 0.0
mig2 = E.migrate_from_v3(legacy_pos)
check("승계 포지션 유지", mig2["strategies"]["10m"]["position"]["market"] == "KRW-HELD")
check("승계 포지션 상태 IN_POSITION", mig2["strategies"]["10m"]["status"] == "IN_POSITION")
check("승계 포지션 계좌는 신규 진입 없음",
      mig2["strategies"]["10m"]["cash_krw"] == 0.0)

# ensure_v4_state 경로 (v3.1 상태가 파일에 있는 상황)
persistence._state_cache = None
persistence.save_state(json.loads(json.dumps(legacy)), remote=False)
E.ensure_v4_state()
ens = persistence._state_cache
check("ensure가 v4.0으로 전환", ens["version"] == "4.0")
check("ensure 승계 유지", ens["strategies"]["3m"]["cash_krw"] == 2_038_520.8563593936)
check("ensure 신규 7전략 추가", all(k in ens["strategies"] for k in E.NEW_KEYS))

# ------------------------------------------------------ [3] 기존 계좌 문턱 분리
print("[3] 기존 3계좌 15초 거래대금 문턱별 진입 분리")
st = FakeStream()
state = fresh()
st.snaps = {"KRW-SMALL": snap("KRW-SMALL", 10.0, short_krw=5_000_000.0, surge=9.0)}
st.price = {"KRW-SMALL": 10.0}
E.run_sniper_tick(st)
check("500만 → 3m만 진입",
      state["strategies"]["3m"]["position"] and not state["strategies"]["10m"]["position"]
      and not state["strategies"]["30m"]["position"])

st = FakeStream()
state = fresh()
st.snaps = {"KRW-MID": snap("KRW-MID", 10.0, short_krw=12_000_000.0, surge=8.0)}
st.price = {"KRW-MID": 10.0}
E.run_sniper_tick(st)
check("1,200만 → 3m+10m 진입, 30m 제외",
      state["strategies"]["3m"]["position"] and state["strategies"]["10m"]["position"]
      and not state["strategies"]["30m"]["position"])

# ------------------------------------------------------------ [4] panic_bounce
print("[4] panic_bounce — 투매 과매도 반등")
state = fresh()
ok, why, _ = sig("panic_bounce", snap("KRW-P", 10.0, price_15s_ago=9.8, chg=-3.2, bid=0.7), state)
check("급락 -3.2% + 반등 + 매수 70% → 진입", ok, why)
ok, why, _ = sig("panic_bounce", snap("KRW-P", 10.0, price_15s_ago=9.8, chg=-2.0, bid=0.7), state)
check("-2.0% (급락 미달) → 거부", not ok, why)
ok, why, _ = sig("panic_bounce", snap("KRW-P", 10.0, price_15s_ago=9.8, chg=-3.2, bid=0.60), state)
check("매수 60% (65% 미달) → 거부", not ok, why)
ok, why, _ = sig("panic_bounce", snap("KRW-P", 10.0, price_15s_ago=10.1, chg=-3.2, bid=0.7), state)
check("반등 없음(15초 전보다 낮음) → 거부", not ok, why)
st = FakeStream()
state = fresh()
st.snaps = {"KRW-P": snap("KRW-P", 10.0, short_krw=8_000_000.0, surge=1.0, bid=0.7,
                          chg=-3.2, price_15s_ago=9.8)}
st.price = {"KRW-P": 10.0}
E.run_sniper_tick(st)
check("엔진 진입 체결", state["strategies"]["panic_bounce"]["position"] is not None
      and state["strategies"]["panic_bounce"]["position"]["market"] == "KRW-P")

# ------------------------------------------------------- [5] pullback_reentry
print("[5] pullback_reentry — 눌림목 재진입")
state = fresh()
ok, why, _ = sig("pullback_reentry", snap("KRW-Q", 98.5, high_10m=100.0,
                                          drawdown_from_high_pct=-1.5, bid=0.7), state)
check("고점 -1.5% 눌림 + 매수 70% + 반등 → 진입", ok, why)
ok, why, _ = sig("pullback_reentry", snap("KRW-Q", 99.5, high_10m=100.0,
                                          drawdown_from_high_pct=-0.5, bid=0.7), state)
check("-0.5% (눌림 부족) → 거부", not ok, why)
ok, why, _ = sig("pullback_reentry", snap("KRW-Q", 97.5, high_10m=100.0,
                                          drawdown_from_high_pct=-2.5, bid=0.7), state)
check("-2.5% (눌림 과다) → 거부", not ok, why)
ok, why, _ = sig("pullback_reentry", snap("KRW-Q", 98.5, high_10m=100.0,
                                          drawdown_from_high_pct=-1.5, bid=0.6), state)
check("매수 60% → 거부", not ok, why)
ok, why, _ = sig("pullback_reentry", snap("KRW-Q", 98.5, high_10m=100.0, coverage_10m_sec=120,
                                          drawdown_from_high_pct=-1.5, bid=0.7), state)
check("10분 고점 관측 120s → 거부", not ok, why)

# ---------------------------------------------------------- [6] vwap_breakout
print("[6] vwap_breakout — 실시간 VWAP 돌파")
state = fresh()
ok, why, _ = sig("vwap_breakout", snap("KRW-V", 101.0, vwap_3m=100.0, price_30s_ago=99.5,
                                       surge=3.0, bid=0.7), state)
check("VWAP +1.0% 돌파 + 3배 + 매수 70% → 진입", ok, why)
ok, why, _ = sig("vwap_breakout", snap("KRW-V", 100.1, vwap_3m=100.0, price_30s_ago=99.5,
                                       surge=3.0, bid=0.7), state)
check("VWAP +0.1% (돌파 미달) → 거부", not ok, why)
ok, why, _ = sig("vwap_breakout", snap("KRW-V", 101.0, vwap_3m=100.0, price_30s_ago=100.5,
                                       surge=3.0, bid=0.7), state)
check("직전 VWAP 하단 이력 없음 → 거부", not ok, why)
ok, why, _ = sig("vwap_breakout", snap("KRW-V", 101.0, vwap_3m=100.0, price_30s_ago=99.5,
                                       surge=2.0, bid=0.7), state)
check("거래량 2.0배 (2.5배 미달) → 거부", not ok, why)

# -------------------------------------------------------- [7] streak_ignition
print("[7] streak_ignition — 연속 매수 긁기")
state = fresh()
ok, why, _ = sig("streak_ignition", snap("KRW-S", 10.0, ticks_10s=18, bid_ratio_10s=0.9,
                                          short_krw=6_000_000.0), state)
check("18건 + 10초 매수 90% + 15초 600만 → 진입", ok, why)
ok, why, _ = sig("streak_ignition", snap("KRW-S", 10.0, ticks_10s=12, bid_ratio_10s=0.9,
                                          short_krw=6_000_000.0), state)
check("체결 12건 → 거부", not ok, why)
ok, why, _ = sig("streak_ignition", snap("KRW-S", 10.0, ticks_10s=18, bid_ratio_10s=0.7,
                                          short_krw=6_000_000.0), state)
check("10초 매수 70% → 거부", not ok, why)
ok, why, _ = sig("streak_ignition", snap("KRW-S", 10.0, ticks_10s=18, bid_ratio_10s=0.9,
                                          short_krw=4_000_000.0), state)
check("15초 400만 → 거부", not ok, why)

# -------------------------------------------------------- [8] major_momentum
print("[8] major_momentum — 대형주 TOP10 스캘핑")
state = fresh()
top = [f"KRW-BIG{i}" for i in range(10)]
ok, why, _ = sig("major_momentum", snap("KRW-BIG3", 10.0, surge=3.2, bid=0.7, chg=1.0),
                 state, top)
check("TOP10 + 3.2배 + 매수 70% + 3분 +1.0% → 진입", ok, why)
ok, why, _ = sig("major_momentum", snap("KRW-NOTOP", 10.0, surge=9.0, bid=0.8, chg=1.0),
                 state, top)
check("TOP10 밖 → 거부", not ok, why)
ok, why, _ = sig("major_momentum", snap("KRW-BIG3", 10.0, surge=2.5, bid=0.7, chg=1.0),
                 state, top)
check("폭발 2.5배 → 거부", not ok, why)
ok, why, _ = sig("major_momentum", snap("KRW-BIG3", 10.0, surge=4.0, bid=0.7, chg=2.5),
                 state, top)
check("3분 +2.5% (뒷북) → 거부", not ok, why)

# --------------------------------------------------------- [9] rsi_oversold
print("[9] rsi_oversold — 1분봉 RSI 과매도 반등")
fake = {"KRW-R": {"market": "KRW-R", "age_sec": 10, "rsi": 24.0, "rsi_prev": 19.0,
                  "bb_upper": 101.0, "bw_rank": 0.5, "bandwidth_pct": 3.0, "vol_ratio": 1.0}}
state = fresh(candles=fake)
ok, why, _ = sig("rsi_oversold", snap("KRW-R", 10.0, bid=0.65), state)
check("RSI 19→24 반전 + 매수 65% → 진입", ok, why)
bad = {"KRW-R": dict(fake["KRW-R"], rsi=26.0, rsi_prev=30.0)}
state = fresh(candles=bad)
ok, why, _ = sig("rsi_oversold", snap("KRW-R", 10.0, bid=0.65), state)
check("RSI 30 (과매도 아님) → 거부", not ok, why)
bad2 = {"KRW-R": dict(fake["KRW-R"], rsi=18.0, rsi_prev=19.0)}
state = fresh(candles=bad2)
ok, why, _ = sig("rsi_oversold", snap("KRW-R", 10.0, bid=0.65), state)
check("RSI 하락 지속 → 거부", not ok, why)
bad3 = {"KRW-R": dict(fake["KRW-R"], age_sec=400)}
state = fresh(candles=bad3)
ok, why, _ = sig("rsi_oversold", snap("KRW-R", 10.0, bid=0.65), state)
check("1분봉 캐시 400s 지연 → 거부", not ok, why)
state = fresh(candles=fake)
ok, why, _ = sig("rsi_oversold", snap("KRW-R", 10.0, bid=0.55), state)
check("매수 55% (60% 미달) → 거부", not ok, why)
state = fresh(candles=None)
ok, why, _ = sig("rsi_oversold", snap("KRW-R", 10.0, bid=0.7), state)
check("캔들 캐시 미연결 → 거부", not ok, why)

# ------------------------------------------------------------ [10] bb_squeeze
print("[10] bb_squeeze — 볼린저 스퀴즈 돌파")
sq = {"KRW-B": {"market": "KRW-B", "age_sec": 20, "bb_upper": 100.0, "bw_rank": 0.1,
                "bandwidth_pct": 1.2, "vol_ratio": 2.4, "rsi": 55.0, "rsi_prev": 54.0}}
state = fresh(candles=sq)
ok, why, _ = sig("bb_squeeze", snap("KRW-B", 100.5, surge=1.2), state)
check("수축(백분위 10%) + 상단 돌파 + 1분봉 2.4배 → 진입", ok, why)
wide = {"KRW-B": dict(sq["KRW-B"], bw_rank=0.6)}
state = fresh(candles=wide)
ok, why, _ = sig("bb_squeeze", snap("KRW-B", 100.5, surge=1.2), state)
check("밴드 수축 아님 → 거부", not ok, why)
state = fresh(candles=sq)
ok, why, _ = sig("bb_squeeze", snap("KRW-B", 99.5, surge=1.2), state)
check("상단 밴드 미돌파 → 거부", not ok, why)
novol = {"KRW-B": dict(sq["KRW-B"], vol_ratio=1.1)}
state = fresh(candles=novol)
ok, why, _ = sig("bb_squeeze", snap("KRW-B", 100.5, surge=1.2), state)
check("거래량 2.0배 미달 → 거부", not ok, why)
state = fresh(candles=sq)
ok, why, _ = sig("bb_squeeze", snap("KRW-B", 100.5, surge=2.5), state)
check("틱 거래대금 2.5배면 1분봉 거래량 미달이어도 진입", ok, why)
print("[10-b] RSI·볼린저 지표 계산 자체")
closes = [100 - i * 0.5 for i in range(20)][::-1]        # 계속 상승
check("상승 구간 RSI > 90", calc_rsi([100 + i for i in range(15)]) > 90)
check("하락 구간 RSI < 10", calc_rsi([100 - i for i in range(15)]) < 10)
bb = bollinger([100.0] * 20)
check("횡보 볼린저 상=하단=중심", bb[1] == bb[2] == 100.0)
bb2 = bollinger([100 + (i % 2) for i in range(20)])
check("변이 있으면 상단>하단", bb2[1] > bb2[2])

# ------------------------------------------------- [11] 10전략 청산 트리거
print("[11] 10전략 각각의 청산 규칙")
st = FakeStream()
for key in E.STRATEGY_KEYS:
    p = E.STRATEGIES[key]["params"]
    state = fresh()
    s1 = force_position(state, key, "KRW-X", 100.0)
    st.price = {"KRW-X": 100.0 * (1 + p["target_pct"] / 100.0) + 0.5}
    hit = E._check_exit(s1, st, E.kst_now())
    check(f"{key} 목표 익절 +{p['target_pct']}%", hit and hit[2] == "TARGET",
          f"{hit[2] if hit else None}")

    state = fresh()
    s2 = force_position(state, key, "KRW-X", 100.0)
    st.price = {"KRW-X": 100.0 * (1 + p["stop_pct"] / 100.0) - 0.01}
    hit = E._check_exit(s2, st, E.kst_now())
    check(f"{key} 손절 {p['stop_pct']}%", hit and hit[2] == "STOP_LOSS",
          f"{hit[2] if hit else None}")

    state = fresh()
    s3 = force_position(state, key, "KRW-X", 100.0)
    s3["position"]["entry_ts_ms"] = E._now_ms() - int((p["max_hold_min"] + 1) * 60000)
    st.price = {"KRW-X": 100.2}
    hit = E._check_exit(s3, st, E.kst_now())
    check(f"{key} 최대 {p['max_hold_min']}분 시간초과", hit and hit[2] == "TIME",
          f"{hit[2] if hit else None}")

    # 트레일링 설정 전략만 동작
    state = fresh()
    s4 = force_position(state, key, "KRW-X", 100.0)
    if p.get("trail_activation_pct") is not None:
        st.price = {"KRW-X": 100.0 * (1 + p["trail_activation_pct"] / 100.0 + 0.05)}
        E._check_exit(s4, st, E.kst_now())          # 고점 갱신
        st.price = {"KRW-X": 100.0 * (1 + (p["trail_activation_pct"] - p["trail_drop_pct"]) / 100.0)}
        hit = E._check_exit(s4, st, E.kst_now())
        check(f"{key} 트레일링 +{p['trail_activation_pct']}%→-{p['trail_drop_pct']}%",
              hit and hit[2] == "TRAILING", f"{hit[2] if hit else None}")
    else:
        st.price = {"KRW-X": 100.0 * 1.01}
        hit = E._check_exit(s4, st, E.kst_now())
        check(f"{key} 트레일링 미사용(보유 유지)", hit is None, f"{hit}")

# ------------------------------------------------ [12] 쿨다운·자금 독립
print("[12] 쿨다운·자금 독립")
state = fresh()
st = FakeStream()
st.snaps = {"KRW-A": snap("KRW-A", 10.0, short_krw=6_000_000.0, surge=9.0)}
st.price = {"KRW-A": 10.0}
E.run_sniper_tick(st)
check("3m 진입 (600만 문턱)", state["strategies"]["3m"]["position"] is not None)
check("10m·30m 미달 대기", not state["strategies"]["10m"]["position"]
      and not state["strategies"]["30m"]["position"])
st.price["KRW-A"] = 10.0 * (1 - 0.02)
E.run_sniper_tick(st)
t3 = state["strategies"]["3m"]
check("3m 손절 + 쿨다운 설정", t3["position"] is None and t3["losses"] == 1
      and t3.get("cooldown_until") is not None, str(t3.get("cooldown_until")))
cd = datetime.strptime(t3["cooldown_until"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
left = (cd - E.kst_now()).total_seconds()
check("3m 쿨다운 5분", 240 < left <= 310, f"{left:.0f}s")
check("다른 전략 쿨다운 없음", all(state["strategies"][k].get("cooldown_until") is None
      for k in E.STRATEGY_KEYS if k != "3m"))
check("다른 전략 현금 불변", state["strategies"]["10m"]["cash_krw"] == 2_000_000
      and state["strategies"]["30m"]["cash_krw"] == 2_000_000)
check("3m 현금만 감소", abs(t3["cash_krw"] - 2_000_000 * 0.98 * 0.9995 * 0.9995) < 300,
      f"{t3['cash_krw']:.2f}")
st.snaps = {"KRW-B": snap("KRW-B", 20.0, short_krw=15_000_000.0, surge=7.0)}
st.price = {"KRW-B": 20.0}
E.run_sniper_tick(st)
check("3m 쿨다운 중 재진입 차단", state["strategies"]["3m"]["position"] is None,
      state["strategies"]["3m"]["last_event"])
check("10m은 새 종목 진입", state["strategies"]["10m"]["position"] is not None
      and state["strategies"]["10m"]["position"]["market"] == "KRW-B")
check("30m은 문턱 미달 대기", state["strategies"]["30m"]["position"] is None)
# 손절 쿨다운 길이는 전략 파라미터를 따른다 (streak 3분 / major 5분)
for key, mins in (("streak_ignition", 3), ("major_momentum", 5), ("panic_bounce", 5)):
    state = fresh()
    s5 = force_position(state, key, "KRW-Z", 100.0)
    E._close_position(s5, 98.0, "test SL", E.kst_now(), "STOP_LOSS")
    cd = datetime.strptime(s5["cooldown_until"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    left = (cd - E.kst_now()).total_seconds()
    check(f"{key} 손절 쿨다운 {mins}분", mins * 60 - 20 < left <= mins * 60 + 10, f"{left:.0f}s")
check("시간초과·익절은 쿨다운 없음", True)
state = fresh()
s6 = force_position(state, "3m", "KRW-Z", 100.0)
E._close_position(s6, 100.5, "test TIME", E.kst_now(), "TIME")
check("3m 시간초과 청산 후 쿨다운 없음", s6.get("cooldown_until") is None)

# ------------------------------------------------------------ [13] BTC 필터
print("[13] BTC 필터 — 모멘텀 8전략 차단 · 역추세 2전략 예외")
check("예외 전략 = panic_bounce·rsi_oversold",
      set(E.BTC_FILTER_EXEMPT) == {"panic_bounce", "rsi_oversold"}, str(E.BTC_FILTER_EXEMPT))
check("예외 2전략 btc_filter False",
      all(E.STRATEGIES[k]["btc_filter"] is False for k in E.BTC_FILTER_EXEMPT))
check("나머지 8전략 btc_filter True",
      all(E.STRATEGIES[k]["btc_filter"] is True
          for k in E.STRATEGY_KEYS if k not in E.BTC_FILTER_EXEMPT),
      str([k for k in E.STRATEGY_KEYS if E.STRATEGIES[k]["btc_filter"] is not True]))
check("rules.btc_filter.exempt_strategies 공개",
      list(E.RULES_V4["btc_filter"].get("exempt_strategies") or []) == list(E.BTC_FILTER_EXEMPT),
      str(E.RULES_V4["btc_filter"].get("exempt_strategies")))
check("rules.strategies[*].btc_filter 일치",
      all(E.RULES_V4["strategies"][k]["btc_filter"] is E.STRATEGIES[k]["btc_filter"]
          for k in E.STRATEGY_KEYS))
check("신규 계좌 상태에도 btc_filter 반영",
      all(E.new_strategy(k)["btc_filter"] is E.STRATEGIES[k]["btc_filter"] for k in E.STRATEGY_KEYS))

state = fresh(BEAR)
st = FakeStream()
st.snaps = {"KRW-A": snap("KRW-A", 10.0, short_krw=60_000_000.0, surge=9.0)}
st.price = {"KRW-A": 10.0}
E.run_sniper_tick(st)
blocked = [k for k in E.STRATEGY_KEYS
           if state["strategies"][k]["status"] == "BTC_FILTER_BLOCKED"]
check("모멘텀·돌파 8전략 차단",
      sorted(blocked) == sorted(k for k in E.STRATEGY_KEYS if k not in E.BTC_FILTER_EXEMPT),
      f"{len(blocked)}개 {blocked}")
check("역추세 2전략은 BTC_FILTER_BLOCKED 아님",
      all(state["strategies"][k]["status"] != "BTC_FILTER_BLOCKED" for k in E.BTC_FILTER_EXEMPT),
      str([(k, state["strategies"][k]["status"]) for k in E.BTC_FILTER_EXEMPT]))
check("역추세 예외 안내 문구 표시",
      all("역추세 예외" in (state["strategies"][k]["last_event"] or "") for k in E.BTC_FILTER_EXEMPT),
      str([state["strategies"][k]["last_event"] for k in E.BTC_FILTER_EXEMPT]))
check("신호 없으면 역추세도 대기",
      all(state["strategies"][k]["position"] is None for k in E.BTC_FILTER_EXEMPT))

# BTC 하락 추세에서 역추세 신호는 실제 체결된다
state = fresh(BEAR, candles={"KRW-R": {"market": "KRW-R", "age_sec": 5, "rsi": 24.0, "rsi_prev": 18.0,
                                           "bb_upper": 101.0, "bw_rank": 0.5,
                                           "bandwidth_pct": 3.0, "vol_ratio": 1.0}})
st = FakeStream()
st.snaps = {
    "KRW-P": snap("KRW-P", 10.0, short_krw=8_000_000.0, surge=1.0, bid=0.7,
                   chg=-3.2, price_15s_ago=9.8),
    "KRW-R": snap("KRW-R", 10.0, short_krw=5_000_000.0, surge=1.0, bid=0.7),
}
st.price = {"KRW-P": 10.0, "KRW-R": 10.0}
E.run_sniper_tick(st)
check("BTC 하락에도 panic_bounce 진입",
      (state["strategies"]["panic_bounce"]["position"] or {}).get("market") == "KRW-P",
      str(state["strategies"]["panic_bounce"]["last_event"]))
check("BTC 하락에도 rsi_oversold 진입",
      (state["strategies"]["rsi_oversold"]["position"] or {}).get("market") == "KRW-R",
      str(state["strategies"]["rsi_oversold"]["last_event"]))
check("같은 순간 모멘텀·돌파 8전략은 진입 없음",
      all(state["strategies"][k]["position"] is None
          for k in E.STRATEGY_KEYS if k not in E.BTC_FILTER_EXEMPT))

# 보유 포지션은 BTC 필터와 무관하게 유지된다
state = fresh(BEAR)
st = FakeStream()
force_position(state, "3m", "KRW-HOLD", 100.0)
st.price = {"KRW-HOLD": 100.5}
st.snaps = {}
E.run_sniper_tick(st)
check("BTC 하락에도 보유 포지션 유지",
      (state["strategies"]["3m"]["position"] or {}).get("market") == "KRW-HOLD",
      str(state["strategies"]["3m"]["position"]))

# ------------------------------------------------------------ [14] 리더보드
print("[14] 리더보드 정렬")
state = fresh()
state["strategies"]["bb_squeeze"]["cash_krw"] = 2_400_000.0
state["strategies"]["bb_squeeze"]["total_trades"] = 8
state["strategies"]["bb_squeeze"]["wins"] = 6
state["strategies"]["bb_squeeze"]["losses"] = 2
state["strategies"]["3m"]["cash_krw"] = 1_500_000.0
state["strategies"]["3m"]["total_trades"] = 4
state["strategies"]["3m"]["wins"] = 1
state["strategies"]["3m"]["losses"] = 3
state = E._aggregate(state, None)
lb = state["leaderboard"]
check("리더보드 10행", len(lb) == 10)
check("1위 bb_squeeze", lb[0]["key"] == "bb_squeeze" and lb[0]["rank"] == 1,
      f"{lb[0]['key']} {lb[0]['ret_pct']}%")
check("최하위 3m", lb[-1]["key"] == "3m", f"{lb[-1]['key']} {lb[-1]['ret_pct']}%")
check("수익률 내림차순", all(lb[i]["ret_pct"] >= lb[i + 1]["ret_pct"] for i in range(9)))
check("승률 계산", lb[0]["win_rate_pct"] == 75.0, str(lb[0]["win_rate_pct"]))
check("통합 자산 = 전략 합산", abs(state["current_equity"] - sum(
    state["strategies"][k]["current_equity"] for k in E.STRATEGY_KEYS)) < 2)
check("통합 전적 12전", state["total_trades"] == 12, str(state["total_trades"]))
check("leader/laggard 표기", state["leader"] == "bb_squeeze" and state["laggard"] == "3m")
persistence.save_state(state, remote=False)

# ------------------------------------------------------------ [15] 영속성
print("[15] 영속성 (로컬 파일)")
saved = json.load(open(TMP, encoding="utf-8"))
check("파일 version 4.0", saved["version"] == "4.0", saved["version"])
check("파일 10전략", len(saved["strategies"]) == 10, str(len(saved["strategies"])))
check("파일 tiers 별칭 3계좌", list(saved["tiers"].keys()) == ["3m", "10m", "30m"])
check("파일 leaderboard 저장", len(saved.get("leaderboard") or []) == 10)

# ------------------------------------------------- [16] 통합 사이클 + 승계 보존
print("[16] 통합 사이클 (매수 → 익절) + 승계 계좌 미변경")
persistence._state_cache = None
persistence.save_state(json.loads(json.dumps(legacy)), remote=False)
E.ensure_v4_state()
state = persistence._state_cache
E._btc_cache["data"] = dict(BULL)
E._btc_cache["ts"] = time.time()
E.set_candle_cache(FakeCandles({
    "KRW-UP": {"market": "KRW-UP", "age_sec": 5, "rsi": 24.0, "rsi_prev": 18.0,
               "bb_upper": 100.0, "bw_rank": 0.1, "bandwidth_pct": 1.0, "vol_ratio": 2.5}}))
st = FakeStream()
st.snaps = {"KRW-UP": snap("KRW-UP", 100.0, short_krw=8_000_000.0, surge=5.0, bid=0.75,
                           chg=1.0)}
st.price = {"KRW-UP": 100.0}
st.top = ["KRW-UP"] + [f"KRW-TOP{i}" for i in range(9)]
E.run_sniper_tick(st)
entered = [k for k in E.STRATEGY_KEYS if state["strategies"][k]["position"]]
check("복수 전략 동시 진입", len(entered) >= 3, str(entered))
check("rsi_oversold 진입", "rsi_oversold" in entered)
st.price["KRW-UP"] = 103.0
E.run_sniper_tick(st)
closed = [k for k in entered if state["strategies"][k]["position"] is None]
check("상승 시 다수 익절 청산", len(closed) >= 3, str(closed))
check("승계 3계좌 현금·전적은 새 체결만 반영",
      abs(state["strategies"]["10m"]["cash_krw"] - 1_961_175.003291777) < 1e-6
      and abs(state["strategies"]["30m"]["cash_krw"] - 1_960_200.4905405408) < 1e-6
      and [state["strategies"][k]["total_trades"] for k in ("30m", "10m")] == [1, 5])
check("3m 승계 7전 + 신규 1전 = 8전", state["strategies"]["3m"]["total_trades"] == 8,
      str(state["strategies"]["3m"]["total_trades"]))
carried_trades = [t for t in state["strategies"]["3m"]["recent_trades"]
                  if t.get("market") == "KRW-C"]
check("승계 매매내역 보존(4건)", len(carried_trades) >= 4, str(len(carried_trades)))
check("매매내역에 전략 라벨 기록",
      all(t.get("strategy") for t in state["recent_trades"]),
      str(state["recent_trades"][0]) if state["recent_trades"] else "none")
check("통합 집계 10전략", len(state["leaderboard"]) == 10)

print("[17] Gist 폴백 상태 보호 (원격 미로드 시 매매 금지)")
os.environ["GIST_TOKEN"] = "fake-token"
os.environ["GIST_ID"] = "fake-gist-id"
persistence._load_source = "local"
state = fresh()
st = FakeStream()
st.snaps = {"KRW-A": snap("KRW-A", 10.0, short_krw=60_000_000.0, surge=9.0)}
st.price = {"KRW-A": 10.0}
res = E.run_sniper_tick(st)
check("원격 상태 미확보 → 매매 중단", res.get("status") == "STATE_SYNC_WAIT", str(res.get("status")))
check("진입 없음", all(state["strategies"][k]["position"] is None for k in E.STRATEGY_KEYS))
os.environ.pop("GIST_TOKEN", None)
os.environ.pop("GIST_ID", None)
persistence._load_source = "local"

print(f"\nRESULT: {OK} passed, {FAIL} failed")
if FAILED:
    print("FAILED:", FAILED)
sys.exit(1 if FAIL else 0)
