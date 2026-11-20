"""v5.2 「단타 전략 비교 리그 (정체 청산 철폐 + 목표 현실화·노이즈 차단)」 단위 검증.

검증 범위
  [1] 10전략 구조 (규칙 유지 3 / 재설계 4 / 신규 3, 퇴출 3, 시드 2,000만 원)
  [2] 체결 모델 — 업비트 호가단위·왕복 실비용·매수/매도 0.5틱(왕복 1틱)
  [3] ws_stream 스냅샷 신규 필드 (10분 레인지 · 박스 고점 · 대형 매수 체결)
  [4] v4.0 → v5.0 승계 (잔고·전적 보존 / 재설계 포지션 정리 / 퇴출 아카이브 / 신규 시드)
  [5] 진입 게이트 — 변동성 하한 · 비용 배수 · 1일 거래 상한 (대형주 차단)
  [6] 10전략 신호 (각 양성 + 경계 음성)
  [7] 청산 — 목표/손절/트레일링/시간초과 + 스프레드 반영
  [8] 재설계 승계 포지션 강제 정리 (RESET_EXIT)
  [9] BTC 필터 (9전략 차단 + 역추세 1전략 예외)
  [10] 리더보드 집계
  [11] 스테이블코인 전역 제외
  [12] Gist 폴백 상태 보호
  [13] 통합 사이클(틱) — 진입 → 청산 회계
  [14] v5.2 규칙 — 정체 청산 철폐 · 목표 현실화 · 3m/alt_rs 스팸 차단 · 30m/tape_sweep 유지
  [15] v5.1 → v5.2 무손실 승계 (잔고·전적·포지션·v5_fix 보존)

실행: python3 tests/test_v5_league.py
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from collections import deque

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

import persistence  # noqa: E402
import sniper_engine as E  # noqa: E402
from ws_stream import UpbitTradeStream  # noqa: E402

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


def snap(market="KRW-T", price=50_000.0, short_krw=40_000_000.0, surge=5.0, bid=0.8,
         chg=1.2, **kw):
    """전략 판정에 필요한 모든 필드를 갖춘 스냅샷 기본값 (가격 5만 원 → 1틱 10원)."""
    d = {
        "market": market, "last_price": price,
        "short_krw": short_krw, "short_bid_krw": short_krw * bid, "bid_ratio": bid,
        "krw_10s": short_krw / 1.5, "bid_ratio_10s": bid, "ticks_10s": 30,
        "base_krw": 100_000_000.0, "base_ticks": 300,
        "baseline_15s_krw": short_krw / max(surge, 1e-9), "surge_ratio": surge,
        "chg_3m_pct": chg, "low_3m": price * 0.99, "high_10m": price,
        "low_10m": price * 0.98, "high_10m_ex_short": price * 0.999,
        "ticks_10m": 240, "range_10m_pct": 2.0, "box_range_pct": 2.0,
        "max_bid_krw_10s": 1_000_000.0, "avg_tick_krw_3m": 400_000.0,
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


def force_position(state, key, market="KRW-T", price=50_000.0):
    st = state["strategies"][key]
    st["position"] = {
        "market": market, "name": market.replace("KRW-", ""), "entry_price": price,
        "highest_price": price, "coin_qty": (st["cash_krw"] * (1 - E.FEE_RATE)) / price,
        "cost_krw": st["cash_krw"], "entry_time": E.fmt(E.kst_now()), "entry_ts_ms": E._now_ms(),
        "entry_type": "TEST", "strategy": key, "signal": "test", "last_price": price,
        "unrealized_pct": 0.0, "ref_price": price,
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


def sig(key, s, state, top=(), btc_snap=None):
    ctx = {"state": state, "top_markets": set(top), "btc": BULL, "bearish": False,
           "btc_snap": btc_snap}
    s = dict(s)
    if s.get("candles") is None:
        s = E._attach_candles(s)
    return E.SIGNAL_FNS[E.STRATEGIES[key]["kind"]](s, state["strategies"][key], ctx)


# ------------------------------------------------------------- [1] 구조
print("[1] 10전략 리그 구조 (v5.2)")
s = fresh()
check("version 5.2", s["version"] == "5.2", s["version"])
check("mode", s["mode"] == "ten_strategy_league", s["mode"])
check("10 strategies", len(s["strategies"]) == 10, str(len(s["strategies"])))
check("keys", list(s["strategies"].keys()) == list(E.STRATEGY_KEYS), str(list(s["strategies"])))
check("legacy 3 키 유지", tuple(s["tiers"].keys()) == ("3m", "10m", "30m"), str(list(s["tiers"])))
check("each 2M cash", all(s["strategies"][k]["cash_krw"] == 2_000_000 for k in E.STRATEGY_KEYS))
check("total capital 20M", s["initial_capital"] == 20_000_000, str(s["initial_capital"]))
check("퇴출 3전략이 리스트에 없음",
      not set(E.RETIRED_KEYS) & set(E.STRATEGY_KEYS), str(E.RETIRED_KEYS))
check("퇴출 진단 문구 존재",
      all(E.RETIRED_DIAGNOSIS.get(k) for k in E.RETIRED_KEYS))
check("재설계 진단 문구 존재",
      all(E.REBUILT_DIAGNOSIS.get(k) for k in E.REBUILT_KEYS))
check("3m v5.2 밸런스 (목표 1.5 / 손졍 -1.3 / 800만 / BID 78%)",
      s["strategies"]["3m"]["params"]["target_pct"] == 1.5
      and s["strategies"]["3m"]["params"]["stop_pct"] == -1.3
      and s["strategies"]["3m"]["params"]["min_short_krw"] == 8_000_000
      and s["strategies"]["3m"]["params"]["min_bid_ratio"] == 0.78)
check("10m 밸런스 (목표 1.6 / 손졍 -1.35)",
      s["strategies"]["10m"]["params"]["target_pct"] == 1.6
      and s["strategies"]["10m"]["params"]["stop_pct"] == -1.35)
check("panic_bounce 급락 기준 유지 (-2.5%)",
      s["strategies"]["panic_bounce"]["params"]["min_drop_pct"] == -2.5)
check("role 라벨", [E.STRATEGIES[k]["role"] for k in E.STRATEGY_KEYS] ==
      ["kept", "kept", "rebuilt", "kept", "rebuilt", "rebuilt", "rebuilt", "new", "new", "new"],
      str([E.STRATEGIES[k]["role"] for k in E.STRATEGY_KEYS]))
check("BTC 필터 예외는 panic_bounce 하나", E.BTC_FILTER_EXEMPT == ("panic_bounce",),
      str(E.BTC_FILTER_EXEMPT))
check("rules 10 strategies", len(s["rules"]["strategies"]) == 10)
check("rules fill_model 존재", s["rules"]["fill_model"]["fee_round_trip_pct"] == 0.1)
for k in ("30m", "pullback_reentry", "vwap_breakout", "bb_squeeze") + E.NEW_KEYS:
    p = s["strategies"][k]["params"]
    check(f"{k} 게이트 파라미터",
          p.get("cost_mult") == 5 and p.get("max_trades_per_day")
          and p.get("symbol_cooldown_scope") == "all",
          f"cost×{p.get('cost_mult')} cap{p.get('max_trades_per_day')}")

# ------------------------------------------------------------- [2] 체결 모델
print("[2] 체결 모델 — 업비트 호가단위 · 왕복 실비용 · 0.5틱")
check("tick 90,161,000원", E.tick_size(90_161_000) == 1000)
check("tick 50,000원", E.tick_size(50_000) == 10)
check("tick 1,324원", E.tick_size(1_324) == 5)
check("tick 128원", E.tick_size(128) == 1)
check("tick 93.8원", E.tick_size(93.8) == 0.1)
check("BTC 왕복비용 ≈ 0.101%", abs(E.cost_pct(90_161_000) - 0.101) < 0.005, f"{E.cost_pct(90_161_000):.3f}%")
check("128원 왕복비용 ≈ 0.881%", abs(E.cost_pct(128) - 0.881) < 0.01, f"{E.cost_pct(128):.3f}%")
check("수수료 0.10% 고정분", E.cost_pct(50_000) > 0.1)
check("fill_buy = 관측가+0.5틱", E.fill_buy(50_000) == 50_005.0, str(E.fill_buy(50_000)))
check("fill_sell = 관측가-0.5틱", E.fill_sell(50_000) == 49_995.0, str(E.fill_sell(50_000)))
check("fill_sell 하한", E.fill_sell(0.001) > 0)
check("왕복 호가 = 정확히 1틱", E.fill_buy(50_000) - E.fill_sell(50_000) == E.tick_size(50_000),
      f"{E.fill_buy(50_000) - E.fill_sell(50_000):.4f}원")
check("SPREAD_TICKS = 편도 0.5틱", E.SPREAD_TICKS == 0.5, str(E.SPREAD_TICKS))
check("왕복비용 = 수수료 0.10% + 1틱",
      abs(E.cost_pct(50_000) - (0.10 + E.tick_size(50_000) / 50_000 * 100)) < 1e-9,
      f"{E.cost_pct(50_000):.3f}%")

# ------------------------------------------------------------- [3] 스냅샷 신규 필드
print("[3] ws_stream 스냅샷 신규 필드 (10분 레인지 · 박스 고점 · 대형 매수)")
ws = UpbitTradeStream()
now_ms = int(time.time() * 1000)
rows = deque()
# 10분 전 ~ 20초 전: 100~102 사이 박스, 마지막 15초: 103으로 돌파 + 대형 매수 1건
for i in range(60):
    ts = now_ms - 590_000 + i * 9_000
    rows.append((ts, 100.0 + (i % 5) * 0.4, 1_000_000.0, i % 2 == 0))
rows.append((now_ms - 8_000, 103.0, 6_000_000.0, True))
rows.append((now_ms - 2_000, 103.2, 500_000.0, True))
ws._ticks["KRW-T"] = rows
sp = ws.market_snapshot("KRW-T", now_ms=now_ms)
check("low_10m 계산", sp["low_10m"] is not None and sp["low_10m"] <= 100.01, str(sp.get("low_10m")))
check("range_10m_pct 계산", 2.5 < sp["range_10m_pct"] < 3.5, f"{sp['range_10m_pct']:.2f}%")
check("box 고점 = 직전 15초 제외 최고", sp["high_10m_ex_short"] < 103.0,
      str(sp["high_10m_ex_short"]))
check("box_range_pct 계산", sp["box_range_pct"] is not None, f"{sp['box_range_pct']:.2f}%")
check("ticks_10m 계산", sp["ticks_10m"] >= 60, str(sp["ticks_10m"]))
check("max_bid_krw_10s = 대형 매수", sp["max_bid_krw_10s"] == 6_000_000.0,
      str(sp["max_bid_krw_10s"]))
check("avg_tick_krw_3m 존재", sp["avg_tick_krw_3m"] and sp["avg_tick_krw_3m"] > 0)
check("스테이블코인은 스냅샷 없음", ws.market_snapshot("KRW-USDT", now_ms=now_ms) is None)

# ------------------------------------------------------------- [4] v4 → v5 승계
print("[4] v4.0 → v5.0 승계 (보존 · 재설계 · 퇴출 · 신규)")
old = {
    "version": "4.0", "status": "IN_POSITION", "mode": "ten_strategy_league",
    "initialized_at": "2026-11-14 22:23:53", "initial_capital": 20_000_000,
    "current_equity": 18_674_530, "current_ret_pct": -6.63, "peak_equity": 20_500_000,
    "total_trades": 390, "wins": 160, "losses": 230, "total_realized_pnl": -1_304_527.37,
    "strategies": {},
}
seed_rows = {
    "3m": (23, 25, -52_211.86, 1_948_763, None),
    "10m": (17, 20, 27_515.74, 2_026_502, {"market": "KRW-ERA", "name": "칼데라",
                                           "entry_price": 100.0, "highest_price": 101.0,
                                           "coin_qty": 20_000.0, "cost_krw": 2_000_000,
                                           "entry_time": "2026-11-15 21:31:31",
                                           "entry_ts_ms": 1, "last_price": 100.5}),
    "30m": (7, 13, -93_894.84, 1_906_105, None),
    "panic_bounce": (19, 13, 39_930.08, 2_039_930, None),
    "pullback_reentry": (15, 23, -210_087.45, 1_789_018,
                         {"market": "KRW-SOPH", "name": "소파", "entry_price": 100.0,
                          "highest_price": 100.5, "coin_qty": 17_800.0, "cost_krw": 1_789_018,
                          "entry_time": "2026-11-15 21:37:22", "entry_ts_ms": 2,
                          "last_price": 100.2}),
    "vwap_breakout": (10, 20, -137_895.66, 1_850_095, {"market": "KRW-XLM", "name": "스텔라",
                                                       "entry_price": 50.0, "highest_price": 50.1,
                                                       "coin_qty": 37_000.0, "cost_krw": 1_850_095,
                                                       "entry_time": "2026-11-15 21:18:45",
                                                       "entry_ts_ms": 3, "last_price": 50.0}),
    "bb_squeeze": (10, 14, -29_048.38, 1_969_966,
                   {"market": "KRW-ONDO", "name": "온도", "entry_price": 700.0,
                    "highest_price": 701.0, "coin_qty": 2_814.0, "cost_krw": 1_969_966,
                    "entry_time": "2026-11-15 21:18:49", "entry_ts_ms": 4, "last_price": 700.0}),
    "rsi_oversold": (17, 35, -442_281.52, 1_556_940, None),
    "major_momentum": (15, 22, -285_109.82, 1_708_655, None),
    "streak_ignition": (27, 45, -121_443.64, 1_878_556, None),
}
for key, (w, l, pnl, eq, pos) in seed_rows.items():
    cap = 2_000_000.0
    old["strategies"][key] = {
        "key": key, "label": key, "name": key, "style": "test", "kind": "test",
        "initial_capital": cap, "cash_krw": 0.0 if pos else float(eq),
        "position": dict(pos) if pos else None, "cooldown_until": None,
        "symbol_cooldowns": {}, "total_realized_pnl": pnl, "total_trades": w + l,
        "wins": w, "losses": l, "current_equity": eq,
        "current_ret_pct": round((eq - cap) / cap * 100, 2), "peak_equity": cap,
        "max_drawdown_pct": 5.0, "entry": "v4 entry", "exit": "v4 exit",
        "status": "IN_POSITION" if pos else "WAITING_FOR_SIGNAL",
        "last_event": "v4 event",
        "recent_trades": [
            {"type": "BUY", "strategy": key, "market": "KRW-A", "name": "에이"},
            {"type": "SELL", "strategy": key, "market": "KRW-A", "name": "에이",
             "exit_time": "2026-11-15 20:00:00", "gain_pct": 0.0,
             "pnl_krw": -1900, "exit_kind": "TIME", "hold_min": 20.0},
            {"type": "SELL", "strategy": key, "market": "KRW-B", "name": "비",
             "exit_time": "2026-11-15 20:30:00", "gain_pct": -1.2,
             "pnl_krw": -24000, "exit_kind": "STOP_LOSS", "hold_min": 4.0},
        ],
    }
mig = E.migrate_from_v4(old)
check("승계 version 5.2 (v4→v5 규칙)", mig["version"] == "5.2", mig["version"])
check("승계 잔고 보존 (7계좌)",
      [mig["strategies"][k]["current_equity"] for k in
       ("3m", "10m", "30m", "panic_bounce", "pullback_reentry", "vwap_breakout", "bb_squeeze")] ==
      [1_948_763, 2_026_502, 1_906_105, 2_039_930, 1_789_018, 1_850_095, 1_969_966],
      str([mig["strategies"][k]["current_equity"] for k in E.STRATEGY_KEYS]))
check("승계 전적 보존",
      [mig["strategies"][k]["total_trades"] for k in ("3m", "10m", "panic_bounce")] == [48, 37, 32])
check("승계 실현손익 보존",
      mig["strategies"]["panic_bounce"]["total_realized_pnl"] == 39_930.08)
check("승계 매매내역 라벨링",
      mig["strategies"]["3m"]["recent_trades"][-1]["strategy"] == "3m")
check("규칙 유지 계좌는 포지션 유지",
      mig["strategies"]["10m"]["position"] is not None
      and not mig["strategies"]["10m"]["position"].get("force_exit_reason"))
check("재설계 계좌 포지션은 강제 정리 예약",
      mig["strategies"]["bb_squeeze"]["position"].get("force_exit_reason")
      and mig["strategies"]["pullback_reentry"]["position"].get("force_exit_reason"))
check("신규 3전략은 200만 원 신규 시드",
      all(mig["strategies"][k]["cash_krw"] == 2_000_000 and mig["strategies"][k]["total_trades"] == 0
          for k in E.NEW_KEYS))
arc = mig.get("v4_archive") or {}
check("아카이브 생성", isinstance(arc, dict) and arc.get("version") == "4.0")
ret_rows = [r for r in arc.get("strategies", []) if r["decision"] == "retired"]
check("퇴출 3건 아카이브", len(ret_rows) == 3, str([r["key"] for r in ret_rows]))
check("아카이브 성적 (rsi -22.15%)",
      [r["ret_pct"] for r in ret_rows if r["key"] == "rsi_oversold"] == [-22.15],
      str([r["ret_pct"] for r in ret_rows]))
check("아카이브 청산사유 분포", ret_rows[0]["exit_kinds"].get("TIME") == 1,
      str(ret_rows[0]["exit_kinds"]))
check("아카이브 진단 문구", "설계 결함" in (ret_rows[0]["diagnosis"] or ""))
check("퇴출 합계", arc["retired_totals"]["count"] == 3
      and arc["retired_totals"]["final_equity"] == 1_556_940 + 1_708_655 + 1_878_556,
      str(arc["retired_totals"]))
check("재설계 4건도 아카이브 기록",
      len([r for r in arc["strategies"] if r["decision"] == "rebuilt"]) == 4)
check("migrated_from 요약", mig["migrated_from"]["retired_keys"] == list(E.RETIRED_KEYS)
      and mig["migrated_from"]["new_keys"] == list(E.NEW_KEYS))
check("아카이브는 라이브 자본에 미포함",
      mig["initial_capital"] == 20_000_000, str(mig["initial_capital"]))

# ------------------------------------------------------------- [5] 게이트
print("[5] 진입 게이트 — 변동성 · 비용 · 1일 상한")
st = mig["strategies"]
ok, why = E._market_gates(snap(range_10m_pct=0.3), st["pullback_reentry"])
check("대형주 레인지 0.3% → 차단", not ok and "변동성" in why, why)
ok, why = E._market_gates(snap(range_10m_pct=1.5), st["pullback_reentry"])
check("알트 레인지 1.5% → 통과", ok, why)
ok, why = E._market_gates(snap(price=128.0, range_10m_pct=1.5), st["pullback_reentry"])
check("128원 종목(1틱 0.78%) → 비용 게이트 차단", not ok and "왕복비용" in why, why)
ok, why = E._market_gates(snap(price=50_000.0, range_10m_pct=1.5), st["pullback_reentry"])
check("5만 원 종목(1틱 0.02%) → 비용 통과", ok, why)
ok, why = E._market_gates(snap(range_10m_pct=0.5), st["tape_sweep"])
check("tape_sweep 하한 0.8% 차단", not ok)
check("30m 은 변동성 게이트 없음 (대형주 전략)", "min_range_10m_pct" not in st["30m"]["params"])
check("3m·10m도 v5.2부터 비용 게이트 적용",
      st["3m"]["params"].get("cost_mult") == 5 and st["10m"]["params"].get("cost_mult") == 5)
check("승계 계좌는 변동성 게이트 없음 (전략별 유니버스 그대로)",
      "min_range_10m_pct" not in st["3m"]["params"]
      and "min_range_10m_pct" not in st["10m"]["params"])
st["pullback_reentry"]["recent_trades"] = [
    {"type": "SELL", "exit_time": E.fmt(E.kst_now()), "exit_kind": "TIME"}] * 12
check("1일 상한 도달 감지", E._trades_today(st["pullback_reentry"]) == 12,
      str(E._trades_today(st["pullback_reentry"])))
st["pullback_reentry"]["recent_trades"] = [
    {"type": "SELL", "exit_time": "2026-11-14 10:00:00", "exit_kind": "TIME"}]
check("어제 거래는 상한에 미포함", E._trades_today(st["pullback_reentry"]) == 0)
today = E.fmt(E.kst_now())[:10]
st["pullback_reentry"]["rules_effective_from"] = f"{today} 23:00:00"
st["pullback_reentry"]["recent_trades"] = [
    {"type": "SELL", "exit_time": f"{today} 22:00:00", "exit_kind": "TIME"}] * 12
check("규칙 적용 이전 거래는 예산에 미포함", E._trades_today(st["pullback_reentry"]) == 0)
st["pullback_reentry"]["recent_trades"] = [
    {"type": "SELL", "exit_time": f"{today} 23:30:00", "exit_kind": "TIME"}] * 3
check("규칙 적용 이후 거래만 카운트", E._trades_today(st["pullback_reentry"]) == 3)
check("재설계 계좌는 규칙 적용 시점 기록",
      mig["strategies"]["bb_squeeze"].get("rules_effective_from") is not None)
check("규칙 유지 계좌는 1일 상한 없음 (예산 미적용)",
      "max_trades_per_day" not in mig["strategies"]["10m"]["params"])

# ------------------------------------------------------------- [6] 신호
print("[6] 10전략 신호 (양성 + 경계 음성)")
s2 = fresh()
btc_flat = snap("KRW-BTC", price=90_000_000.0, chg=0.1, range_10m_pct=0.25)

ok, why, _ = sig("3m", snap(market="KRW-A", short_krw=9_000_000, surge=5.0, bid=0.85,
                            chg=1.0), s2)
check("3m 급등 포착 (800만↑ · BID 78%↑)", ok, why)
ok, why, _ = sig("3m", snap(market="KRW-A", short_krw=5_000_000, surge=5.0, bid=0.85,
                            chg=1.0), s2)
check("3m 500만은 문턱 미달", not ok and "15초 거래대금" in why, why)
ok, why, _ = sig("10m", snap(market="KRW-A", short_krw=5_000_000, surge=5.0), s2)
check("10m 문턱 미달", not ok and "15초 거래대금" in why, why)

ok, why, _ = sig("panic_bounce", snap(market="KRW-B", chg=-3.0, bid=0.8,
                                      price_15s_ago=50_000.0 * 0.99), s2)
check("panic_bounce 급락 반등", ok, why)
ok, why, _ = sig("panic_bounce", snap(market="KRW-B", chg=-1.0), s2)
check("panic_bounce 급락 아님", not ok)

ok, why, _ = sig("30m", snap(market="KRW-BTC", price=90_000_000.0, chg=0.1,
                             drawdown_from_high_pct=-0.6, range_10m_pct=0.3), s2,
                 top=("KRW-BTC", "KRW-ETH"))
check("30m 대형주 레인지 눌림", ok, why)
ok, why, _ = sig("30m", snap(market="KRW-ALT", drawdown_from_high_pct=-0.6), s2,
                 top=("KRW-BTC", "KRW-ETH"))
check("30m TOP10 아님", not ok and "TOP10" in why, why)
ok, why, _ = sig("30m", snap(market="KRW-BTC", price=90_000_000.0,
                             drawdown_from_high_pct=-2.0), s2, top=("KRW-BTC",))
check("30m 하락 과다(칼날) 차단", not ok, why)

ok, why, _ = sig("pullback_reentry", snap(market="KRW-A", chg=1.0, range_10m_pct=1.5,
                                          drawdown_from_high_pct=-1.4), s2)
check("눌림 재진입 조건 충족", ok, why)
ok, why, _ = sig("pullback_reentry", snap(market="KRW-A", chg=0.2, range_10m_pct=1.5,
                                          drawdown_from_high_pct=-1.4), s2)
check("눌림 재진입 상승 없음 차단", not ok and "3분 상승" in why, why)
ok, why, _ = sig("pullback_reentry", snap(market="KRW-A", chg=1.0, range_10m_pct=1.5,
                                          drawdown_from_high_pct=-0.3), s2)
check("눌림 부족 차단", not ok, why)

ok, why, _ = sig("vwap_breakout", snap(market="KRW-A", price=50_300.0, vwap_3m=50_000.0,
                                       range_10m_pct=1.5, surge=3.5,
                                       price_30s_ago=49_900.0), s2)
check("VWAP 돌파 조건 충족", ok, why)
ok, why, _ = sig("vwap_breakout", snap(market="KRW-A", price=50_300.0, range_10m_pct=1.5,
                                       surge=3.5, price_30s_ago=50_400.0, vwap_3m=50_000.0), s2)
check("VWAP 상단 이력 없음 차단", not ok, why)

candle_ok = {"age_sec": 20, "bb_upper": 50_500.0, "bb_mid": 49_900.0, "bb_lower": 49_300.0,
             "last_close": 50_100.0, "bw_rank": 0.1, "bandwidth_pct": 1.1, "vol_ratio": 2.5}
ok, why, _ = sig("bb_squeeze", snap(market="KRW-A", price=50_600.0, range_10m_pct=1.5,
                                    candles=candle_ok), s2)
check("BB 수축 + 중심선 위 돌파", ok, why)
ok, why, _ = sig("bb_squeeze", snap(market="KRW-A", price=50_600.0, range_10m_pct=1.5,
                                    candles=dict(candle_ok, last_close=49_000.0)), s2)
check("BB 중심선 아래(하락 국면) 차단", not ok and "중심선" in why, why)
ok, why, _ = sig("bb_squeeze", snap(market="KRW-A", price=50_600.0, range_10m_pct=1.5,
                                    candles=dict(candle_ok, age_sec=200)), s2)
check("BB 캔들 지연 차단", not ok)

ok, why, _ = sig("box_breakout", snap(market="KRW-A", price=50_500.0, box_range_pct=0.9,
                                      high_10m_ex_short=50_300.0, low_10m=49_850.0,
                                      range_10m_pct=1.4, surge=3.5, bid=0.7,
                                      chg_15s_pct=0.4), s2)
check("박스 돌파 조건 충족", ok, why)
ok, why, _ = sig("box_breakout", snap(market="KRW-A", price=50_500.0, box_range_pct=2.4,
                                      high_10m_ex_short=50_300.0, low_10m=49_100.0,
                                      range_10m_pct=2.5, surge=3.5), s2)
check("박스 수축 아님 차단", not ok and "박스 폭" in why, why)
ok, why, _ = sig("box_breakout", snap(market="KRW-A", price=51_500.0, box_range_pct=0.9,
                                      high_10m_ex_short=50_300.0, low_10m=49_850.0,
                                      range_10m_pct=1.4, surge=3.5, chg_15s_pct=1.8), s2)
check("박스 상단 급등 추격 차단", not ok and "추격" in why, why)

ok, why, _ = sig("alt_rs", snap(market="KRW-A", chg=1.6, range_10m_pct=1.5, surge=3.0),
                 s2, btc_snap=btc_flat)
check("알트 상대강도 +1.5%p", ok, why)
ok, why, _ = sig("alt_rs", snap(market="KRW-A", chg=1.6, range_10m_pct=1.5, surge=3.0),
                 s2, btc_snap=snap("KRW-BTC", chg=1.2))
check("BTC도 같이 오르면 디커플링 아님", not ok, why)
ok, why, _ = sig("alt_rs", snap(market="KRW-BTC", price=90_000_000.0, chg=2.0),
                 s2, btc_snap=btc_flat)
check("BTC 자신은 제외", not ok, why)

ok, why, _ = sig("tape_sweep", snap(market="KRW-A", range_10m_pct=1.2,
                                    max_bid_krw_10s=6_000_000.0, avg_tick_krw_3m=1_000_000.0,
                                    ticks_10s=12, chg_15s_pct=0.3), s2)
check("대형 매수 체결 스파이크", ok, why)
ok, why, _ = sig("tape_sweep", snap(market="KRW-A", range_10m_pct=1.2,
                                    max_bid_krw_10s=6_000_000.0, avg_tick_krw_3m=3_000_000.0,
                                    ticks_10s=12, chg_15s_pct=0.3), s2)
check("평균 대비 2배는 미달", not ok and "배수" in why, why)
ok, why, _ = sig("tape_sweep", snap(market="KRW-A", range_10m_pct=1.2,
                                    max_bid_krw_10s=6_000_000.0, avg_tick_krw_3m=1_000_000.0,
                                    ticks_10s=12, chg_15s_pct=0.0), s2)
check("체결 후 상승 미확인 차단", not ok, why)

# ------------------------------------------------------------- [7] 청산
print("[7] 청산 — 목표/손절/트레일링/시간초과 + 스프레드")
s3 = fresh()
fs = FakeStream()


def exit_case(key, price_now, minutes=1.0, entry=50_000.0, high=None):
    stx = s3["strategies"][key]
    force_position(s3, key, market="KRW-T", price=entry)
    if high:
        stx["position"]["highest_price"] = high
    stx["position"]["entry_ts_ms"] = E._now_ms() - int(minutes * 60_000)
    fs.price["KRW-T"] = price_now
    fs.snaps["KRW-T"] = snap(market="KRW-T", price=price_now)
    return E._check_exit(stx, fs, E.kst_now())


hit = exit_case("pullback_reentry", 51_200.0)
check("목표 익절 트리거", hit and hit[2] == "TARGET", str(hit))
pnl, gain = E._close_position(s3["strategies"]["pullback_reentry"], hit[0], hit[1],
                              E.kst_now(), hit[2])
check("목표 체결가 = 목표가 -0.5틱 (스프레드 반영)",
      abs(gain - 1.69) < 0.02, f"{gain:.2f}%")
check("목표 익절 손익 > 0", pnl > 25_000, f"{pnl:,.0f}원")

hit = exit_case("pullback_reentry", 49_200.0)
check("손절 트리거", hit and hit[2] == "STOP_LOSS", str(hit))
pnl, gain = E._close_position(s3["strategies"]["pullback_reentry"], hit[0], hit[1],
                              E.kst_now(), hit[2])
check("손절 손실 ≈ -1.6% (관통 체결 + 스프레드)", -1.7 < gain < -1.2, f"{gain:.2f}%")
check("손절 후 쿨다운 설정", s3["strategies"]["pullback_reentry"]["cooldown_until"] is not None)

hit = exit_case("bb_squeeze", 50_500.0, high=50_800.0)
check("트레일링 트리거", hit and hit[2] == "TRAILING", str(hit))

hit = exit_case("tape_sweep", 50_050.0, minutes=9.0)
check("시간초과 트리거", hit and hit[2] == "TIME", str(hit))

hit = exit_case("3m", 49_750.0, minutes=13.0)
check("3m 12분 시간초과 (-0.5%는 정체 밖)", hit and hit[2] == "TIME", str(hit))

hit = exit_case("3m", 50_100.0, minutes=1.0)
check("보유 유지 (조건 미충족)", hit is None, str(hit))

# ------------------------------------------------------------- [8] 강제 정리
print("[8] 재설계 승계 포지션 강제 정리")
s4 = E.migrate_from_v4(old)
persistence._state_cache = s4
fs2 = FakeStream()
fs2.price["KRW-ONDO"] = 705.0
fs2.snaps["KRW-ONDO"] = snap(market="KRW-ONDO", price=705.0)
E.set_candle_cache(None)
E._btc_cache["data"] = dict(BULL)
E._btc_cache["ts"] = time.time()
E.run_sniper_tick(fs2)
bb = s4["strategies"]["bb_squeeze"]
check("재설계 포지션 정리됨", bb["position"] is None and bb["total_trades"] == 25,
      f"trades={bb['total_trades']}")
check("정리 청산 사유 기록", bb["recent_trades"][-1]["exit_kind"] == "RESET_EXIT",
      bb["recent_trades"][-1]["exit_kind"])
check("규칙 유지 계좌 포지션은 유지", s4["strategies"]["10m"]["position"] is not None
      or s4["strategies"]["10m"]["recent_trades"][-1].get("exit_kind") in ("TARGET", "STOP_LOSS",
                                                                           "TIME", "TRAILING"))

# ------------------------------------------------------------- [9] BTC 필터
print("[9] BTC 필터 (9전략 차단 + 역추세 1전략 예외)")
s5 = fresh(btc=BEAR)
persistence._state_cache = s5
fs3 = FakeStream()
fs3.snaps["KRW-ALT"] = snap(market="KRW-ALT", chg=-3.0, bid=0.9, range_10m_pct=2.0)
fs3.price["KRW-ALT"] = 50_000.0
E.set_candle_cache(None)
E._btc_cache["data"] = dict(BEAR)
E._btc_cache["ts"] = time.time()
E.run_sniper_tick(fs3)
blocked = [k for k in E.STRATEGY_KEYS
           if s5["strategies"][k]["status"] == "BTC_FILTER_BLOCKED"]
check("9전략 차단", len(blocked) == 9, f"{len(blocked)}개: {blocked}")
check("panic_bounce 예외", "panic_bounce" not in blocked)
check("panic_bounce 실제 진입", s5["strategies"]["panic_bounce"]["position"] is not None,
      str(s5["strategies"]["panic_bounce"]["position"] and
          s5["strategies"]["panic_bounce"]["position"]["market"]))

# ------------------------------------------------------------- [10] 집계
print("[10] 리더보드 · 집계")
agg = E._aggregate(s4, None)
board = agg["leaderboard"]
check("10행", len(board) == 10, str(len(board)))
check("정렬", [r["ret_pct"] for r in board] == sorted([r["ret_pct"] for r in board], reverse=True))
check("1위 = 최고 수익률", board[0]["ret_pct"] == max(r["ret_pct"] for r in board),
      f"{board[0]['key']} {board[0]['ret_pct']}%")
check("role 노출", all(r.get("role") in ("kept", "rebuilt", "new") for r in board),
      str(sorted({r.get("role") for r in board})))
check("합산 자산", agg["current_equity"] > 0 and agg["initial_capital"] == 20_000_000)
check("퇴출 계좌는 합산 제외", agg["total_trades"] == sum(
    s4["strategies"][k]["total_trades"] for k in E.STRATEGY_KEYS))

# ------------------------------------------------------------- [11] 스테이블코인
print("[11] 스테이블코인 전역 제외")
s6 = fresh()
persistence._state_cache = s6
fs4 = FakeStream()
fs4.snaps["KRW-USDT"] = snap(market="KRW-USDT", price=1_380.0, range_10m_pct=0.05)
fs4.price["KRW-USDT"] = 1_380.0
E._btc_cache["data"] = dict(BULL)
E._btc_cache["ts"] = time.time()
E.run_sniper_tick(fs4)
check("스테이블코인 진입 없음",
      all(s6["strategies"][k]["position"] is None for k in E.STRATEGY_KEYS))
check("market_filter 유지", E.is_excluded_market("KRW-USDC") and E.is_excluded_market("KRW-DAI"))
check("제외 목록이 rules에 노출", "KRW-USDT" in agg["rules"]["market_filter"]["excluded_markets"])

# ------------------------------------------------------------- [12] Gist 보호
print("[12] Gist 폴백 상태 보호")
E._ensure_guard["ts"] = 0.0
E.gist_enabled = lambda: True
E.last_load_source = lambda: "local"
res = E.run_sniper_tick(FakeStream())
check("STATE_SYNC_WAIT", res["status"] in ("STATE_SYNC_WAIT", "MIGRATION_WAIT"), res["status"])
E.gist_enabled = lambda: False
E.last_load_source = lambda: "local"

# ------------------------------------------------------------- [13] 통합 사이클
print("[13] 통합 사이클 회계")
s7 = fresh()
persistence._state_cache = s7
fs5 = FakeStream()
fs5.snaps["KRW-BIG"] = snap(market="KRW-BIG", price=50_000.0, short_krw=20_000_000.0,
                            surge=6.0, bid=0.85, chg=1.2, range_10m_pct=2.0)
fs5.price["KRW-BIG"] = 50_000.0
E.set_candle_cache(None)
E._btc_cache["data"] = dict(BULL)
E._btc_cache["ts"] = time.time()
r1 = E.run_sniper_tick(fs5)
ten = s7["strategies"]["10m"]
check("10m 진입", ten["position"] is not None, str(ten["position"] and ten["position"]["market"]))
entry = ten["position"]["entry_price"] if ten["position"] else 0
check("매수 체결가 = 50,005 (관측가+0.5틱)", entry == 50_005.0, str(entry))
check("현금 0", ten["cash_krw"] == 0.0)
check("포지션에 비용 기록", ten["position"] and ten["position"]["cost_pct"] > 0.1,
      str(ten["position"] and ten["position"]["cost_pct"]))
fs5.price["KRW-BIG"] = 51_500.0
fs5.snaps["KRW-BIG"] = snap(market="KRW-BIG", price=51_500.0, range_10m_pct=2.0)
r2 = E.run_sniper_tick(fs5)
ten = s7["strategies"]["10m"]
check("10m 목표 청산", ten["position"] is None and ten["recent_trades"][-1]["type"] == "SELL",
      ten["recent_trades"][-1].get("exit_kind"))
check("목표 +1.6% → 스프레드 반영 1.58%", abs(ten["recent_trades"][-1]["gain_pct"] - 1.58) < 0.03,
      str(ten["recent_trades"][-1]["gain_pct"]))
check("승 기록", ten["wins"] == 1 and ten["losses"] == 0)
check("현금 복원", 2_020_000 < ten["cash_krw"] < 2_040_000, f"{ten['cash_krw']:,.0f}원")

# ------------------------------------------------------------- [14] v5.2 규칙
print("[14] v5.2 규칙 — 정체 청산 철폐 · 목표 현실화 · 노이즈 차단")

# (1) 실측 스냅샷의 coverage_sec 은 구조상 180에 닿지 않는다 → 179.x초에서도 alt_rs 가 진입해야 한다
ws2 = UpbitTradeStream()
now2 = int(time.time() * 1000)
dq = deque()
for i in range(180):
    ts = now2 - 179_500 + i * 1_000
    px = 5_000.0 + i * (100.0 / 179)
    krw = 9_000_000.0 if i >= 165 else 1_000_000.0
    dq.append((ts, px, krw, True))
ws2._ticks["KRW-ALT"] = dq
sp2 = ws2.market_snapshot("KRW-ALT", now_ms=now2)
check("실측 스냅샷 coverage_sec < 180s", sp2["coverage_sec"] < 180.0, f"{sp2['coverage_sec']:.1f}s")
ok, why, _ = sig("alt_rs", sp2, s2, btc_snap=snap("KRW-BTC", price=90_000_000.0, chg=0.1))
check("alt_rs 179.x초 관측에서 진입 (0전 버그 회귀)", ok, why)
ok, why, _ = sig("alt_rs", snap(market="KRW-A", chg=1.6, range_10m_pct=1.5, surge=3.0,
                                coverage_sec=149.0), s2, btc_snap=btc_flat)
check("alt_rs 관측 149초는 여전히 대기", not ok and "3분 관측" in why, why)
check("alt_rs min_coverage_sec = 150",
      E.STRATEGIES["alt_rs"]["params"].get("min_coverage_sec") == 150)

# (2) 6분 무풍 정체 청산(STAGNATION)은 로직·파라미터·규칙표 어디에도 없다
check("STAGNATION 파라미터 전면 제거",
      all("stagnation_min" not in E.STRATEGIES[k]["params"]
          and "stagnation_band_pct" not in E.STRATEGIES[k]["params"] for k in E.STRATEGY_KEYS))
check("rules 에 정체 청산 항목 없음 (보유 상한으로 대체)",
      "stagnation_exit" not in E.RULES_V4["cost_gates"]
      and "hold_cap" in E.RULES_V4["cost_gates"])

s8 = fresh()
fs6 = FakeStream()


def hold_case(key, gain_pct, minutes, entry=50_000.0):
    stx = s8["strategies"][key]
    stx["cash_krw"] = 2_000_000.0
    stx["cooldown_until"] = None
    force_position(s8, key, market="KRW-T", price=entry)
    stx["position"]["entry_ts_ms"] = E._now_ms() - int(minutes * 60_000)
    fs6.price["KRW-T"] = entry * (1 + gain_pct / 100.0)
    fs6.snaps["KRW-T"] = snap(market="KRW-T", price=fs6.price["KRW-T"])
    return E._check_exit(stx, fs6, E.kst_now())


hit = hold_case("pullback_reentry", 0.10, 7.0)
check("7분 +0.10% 무풍 → 청산 없음 (STAGNATION 철폐)", hit is None, str(hit))
hit = hold_case("pullback_reentry", -0.20, 8.0)
check("8분 -0.20% 무풍 → 청산 없음", hit is None, str(hit))
hit = hold_case("3m", 0.05, 11.5)
check("11.5분 무풍 → 아직 보유 (12분 상한 전)", hit is None, str(hit))
hit = hold_case("pullback_reentry", 0.10, 12.5)
check("12분을 넘기면 시간초과 청산", hit and hit[2] == "TIME", str(hit))
hit = hold_case("tape_sweep", 0.10, 8.5)
check("tape_sweep 8분 상한 → 시간초과", hit and hit[2] == "TIME", str(hit))
hit = hold_case("panic_bounce", 0.10, 14.0)
check("panic_bounce 14분 무풍 → 보유", hit is None, str(hit))

# (3) 모멘텀·돌파 6전략 밴드 현실화
FAMILY = ("3m", "10m", "pullback_reentry", "vwap_breakout", "bb_squeeze", "box_breakout")
for k in FAMILY:
    p = E.STRATEGIES[k]["params"]
    check(f"{k} 목표 1.4~1.7%", 1.4 <= p["target_pct"] <= 1.7, str(p["target_pct"]))
    check(f"{k} 트레일 발동 0.9~1.0%", 0.9 <= p["trail_activation_pct"] <= 1.0,
          str(p["trail_activation_pct"]))
    check(f"{k} 트레일 반락 0.30~0.35%", 0.30 <= p["trail_drop_pct"] <= 0.35,
          str(p["trail_drop_pct"]))
    check(f"{k} 손절 -1.2~-1.4%", -1.4 <= p["stop_pct"] <= -1.2, str(p["stop_pct"]))
    check(f"{k} 트레일 발동 < 목표 (수익 확정 구조)",
          p["trail_activation_pct"] < p["target_pct"])

# (4) 3m 문턱 상향 (허매수 차단)
p3 = E.STRATEGIES["3m"]["params"]
check("3m 15초 800만↑", p3["min_short_krw"] == 8_000_000)
check("3m BID 78%↑", p3["min_bid_ratio"] == 0.78)
check("3m 1일 상한 10회", p3["max_trades_per_day"] == 10)
ok, why, _ = sig("3m", snap(market="KRW-A", short_krw=7_000_000, surge=5.0, bid=0.9,
                            chg=1.0), s2)
check("3m 700만은 문턱 미달", not ok and "15초 거래대금" in why, why)
ok, why, _ = sig("3m", snap(market="KRW-A", short_krw=9_000_000, surge=5.0, bid=0.77,
                            chg=1.0), s2)
check("3m BID 77% 차단", not ok and "매수비율" in why, why)
ok, why, _ = sig("3m", snap(market="KRW-A", short_krw=9_000_000, surge=5.0, bid=0.85,
                            chg=1.0), s2)
check("3m 900만·BID 85% 진입", ok, why)
ok, why, _ = sig("3m", snap(market="KRW-A", price=128.0, short_krw=30_000_000, surge=5.0,
                            bid=0.9, chg=1.0), s2)
check("3m 저가 종목(왕복 0.88%)은 비용 게이트 차단", not ok and "왕복비용" in why, why)

# (5) alt_rs 엄선
p9 = E.STRATEGIES["alt_rs"]["params"]
check("alt_rs 디커플링 +1.3%p", p9["min_rs_pct"] == 1.3, str(p9["min_rs_pct"]))
check("alt_rs 15초 700만↑", p9["min_short_krw"] == 7_000_000)
check("alt_rs BID 75%↑", p9["min_bid_ratio"] == 0.75)
check("alt_rs 1일 상한 6회", p9["max_trades_per_day"] == 6)
ok, why, _ = sig("alt_rs", snap(market="KRW-A", chg=1.6, range_10m_pct=1.5, surge=3.0),
                 s2, btc_snap=btc_flat)
check("alt_rs +1.5%p 진입", ok, why)
ok, why, _ = sig("alt_rs", snap(market="KRW-A", chg=1.3, range_10m_pct=1.5, surge=3.0),
                 s2, btc_snap=btc_flat)
check("alt_rs +1.2%p는 대기", not ok and "디커플링" in why, why)
ok, why, _ = sig("alt_rs", snap(market="KRW-A", chg=2.0, bid=0.70, range_10m_pct=1.5,
                                surge=3.0), s2, btc_snap=btc_flat)
check("alt_rs BID 70% 차단", not ok and "매수비율" in why, why)
ok, why, _ = sig("alt_rs", snap(market="KRW-A", chg=2.0, short_krw=5_000_000,
                                range_10m_pct=1.5, surge=3.0), s2, btc_snap=btc_flat)
check("alt_rs 500만은 문턱 미달", not ok and "15초 거래대금" in why, why)

# (6) panic_bounce 밸런스 · 30m/tape_sweep 유지
pb = E.STRATEGIES["panic_bounce"]["params"]
check("panic_bounce 목표 +1.8%", pb["target_pct"] == 1.8)
check("panic_bounce 트레일 +1.2%→고점 -0.35%",
      pb["trail_activation_pct"] == 1.2 and pb["trail_drop_pct"] == 0.35)
check("panic_bounce 손절 -1.25%", pb["stop_pct"] == -1.25)
check("panic_bounce 최대 15분 유지", pb["max_hold_min"] == 15)
p30 = E.STRATEGIES["30m"]["params"]
check("30m 규칙 유지 (0.75 / 0.50→0.20 / -0.50 / 12분)",
      p30["target_pct"] == 0.75 and p30["trail_activation_pct"] == 0.50
      and p30["trail_drop_pct"] == 0.20 and p30["stop_pct"] == -0.50
      and p30["max_hold_min"] == 12)
pt = E.STRATEGIES["tape_sweep"]["params"]
check("tape_sweep 규칙 유지 (1.8 / -1.0 / 8분)",
      pt["target_pct"] == 1.8 and pt["stop_pct"] == -1.0 and pt["max_hold_min"] == 8)

# ------------------------------------------------------------- [15] v5.1 → v5.2
print("[15] v5.1 → v5.2 무손실 승계")

TA_OLD = "2026-11-16 20:20:47"
V51 = {
    "3m": {"min_short_krw": 3_000_000, "min_bid_ratio": 0.70, "target_pct": 2.5,
           "trail_activation_pct": 1.5, "trail_drop_pct": 0.4, "stop_pct": -1.8,
           "stagnation_min": 6, "stagnation_band_pct": 0.25,
           "cost_mult": None, "max_trades_per_day": None},
    "10m": {"target_pct": 2.5, "trail_activation_pct": 1.5, "trail_drop_pct": 0.4,
            "stop_pct": -1.8, "stagnation_min": 6, "stagnation_band_pct": 0.25,
            "cost_mult": None},
    "pullback_reentry": {"target_pct": 2.4, "trail_activation_pct": 1.5, "trail_drop_pct": 0.45,
                         "stop_pct": -1.3, "stagnation_min": 6, "stagnation_band_pct": 0.25},
    "vwap_breakout": {"target_pct": 2.0, "trail_activation_pct": 1.1, "trail_drop_pct": 0.35,
                      "stop_pct": -1.2, "stagnation_min": 6, "stagnation_band_pct": 0.25},
    "bb_squeeze": {"target_pct": 2.2, "trail_activation_pct": 1.2, "trail_drop_pct": 0.35,
                   "stop_pct": -1.3, "stagnation_min": 6, "stagnation_band_pct": 0.25},
    "box_breakout": {"target_pct": 2.2, "trail_activation_pct": 1.4, "trail_drop_pct": 0.4,
                     "stop_pct": -1.2, "stagnation_min": 6, "stagnation_band_pct": 0.25},
    "alt_rs": {"min_rs_pct": 0.8, "min_short_krw": 3_000_000, "min_bid_ratio": 0.62,
               "target_pct": 2.2, "trail_activation_pct": 1.3, "trail_drop_pct": 0.4,
               "stagnation_min": 6, "stagnation_band_pct": 0.25, "max_trades_per_day": 10},
    "panic_bounce": {"target_pct": 2.0, "trail_activation_pct": 1.6, "trail_drop_pct": 0.40,
                     "stop_pct": -1.3},
}


def _v51_params(k):
    """v5.1 라이브 파라미터 (v5.2 코드 정의에서 바뀐 값만 되돌린다)."""
    p = dict(E.STRATEGIES[k]["params"])
    for key, val in V51.get(k, {}).items():
        if val is None:
            p.pop(key, None)
        else:
            p[key] = val
    return p


row_src = {
    "3m": (82, 26, 56, -628_073, 1_371_927,
           {"market": "KRW-NEAR", "name": "니어", "entry_price": 2_400.0, "coin_qty": 570.0,
            "cost_krw": 1_368_000, "highest_price": 2_400.0, "entry_time": "2026-11-16 20:00:00",
            "entry_ts_ms": 1, "last_price": 2_400.0}),
    "10m": (65, 24, 41, -360_424, 1_639_576,
            {"market": "KRW-NEAR", "name": "니어", "entry_price": 2_400.0, "coin_qty": 680.0,
             "cost_krw": 1_632_000, "highest_price": 2_400.0, "entry_time": "2026-11-16 20:05:00",
             "entry_ts_ms": 2, "last_price": 2_400.0}),
    "30m": (38, 11, 27, -212_494, 1_787_506, None),
    "panic_bounce": (59, 30, 29, -443_174, 1_556_826, None),
    "pullback_reentry": (55, 20, 35, -374_242, 1_625_758, None),
    "vwap_breakout": (46, 14, 32, -338_803, 1_661_197, None),
    "bb_squeeze": (38, 14, 24, -119_012, 1_880_988,
                   {"market": "KRW-PIEVERSE", "name": "피버스", "entry_price": 100.0,
                    "coin_qty": 18_800.0, "cost_krw": 1_880_000, "highest_price": 100.0,
                    "entry_time": "2026-11-16 20:10:00", "entry_ts_ms": 3, "last_price": 100.0}),
    "box_breakout": (13, 3, 10, -89_240, 1_910_760, None),
    "alt_rs": (0, 0, 0, 0, 2_000_000, None),
    "tape_sweep": (20, 6, 14, -94_339, 1_905_661, None),
}
v51 = {"version": "5.1", "mode": "ten_strategy_league", "initialized_at": "2026-11-15 22:08:48",
       "initial_capital": 20_000_000, "current_equity": 15_956_806, "current_ret_pct": -20.22,
       "peak_equity": 20_000_000, "max_drawdown_pct": 20.22,
       "total_trades": sum(r[0] for r in row_src.values()),
       "wins": sum(r[1] for r in row_src.values()), "losses": sum(r[2] for r in row_src.values()),
       "total_realized_pnl": round(sum(r[3] for r in row_src.values()), 2),
       "v4_archive": {"version": "4.0", "retired_totals": {"count": 3, "final_equity": 5_219_340,
                                                          "initial_capital": 6_000_000,
                                                          "ret_pct": -13.01}},
       "migrated_from": {"version": "4.0", "at": "2026-11-15 22:08:48", "carried_trades": 230},
       "v5_fix": {"from_version": "5.0", "to_version": "5.1", "at": TA_OLD,
                  "carried_trades": 418, "rules_changed_keys": ["3m", "10m", "30m"]},
       "strategies": {}}
for k, (tr, w, l, pnl, cash, pos) in row_src.items():
    v51["strategies"][k] = {
        "key": k, "label": s["strategies"][k]["label"], "name": s["strategies"][k]["name"],
        "kind": E.STRATEGIES[k]["kind"], "initial_capital": 2_000_000.0, "cash_krw": float(cash),
        "position": dict(pos) if pos else None, "cooldown_until": None, "symbol_cooldowns": {},
        "total_realized_pnl": float(pnl), "total_trades": tr, "wins": w, "losses": l,
        "current_equity": float(cash), "peak_equity": 2_000_000.0, "max_drawdown_pct": 10.0,
        "params": _v51_params(k), "entry": "old entry", "exit": "old exit",
        "rules_effective_from": TA_OLD,
        "status": "IN_POSITION" if pos else "WAITING_FOR_SIGNAL",
        "recent_trades": [{"type": "SELL", "strategy": k, "market": "KRW-X",
                           "exit_time": "2026-11-16 19:00:00", "exit_kind": "STAGNATION",
                           "gain_pct": -0.4, "pnl_krw": -8000}],
    }

m52 = E.migrate_from_v5(v51)
check("v5.2 버전 기록", m52["version"] == "5.2", m52["version"])
check("10계좌 전적 무손실",
      [m52["strategies"][k]["total_trades"] for k in E.STRATEGY_KEYS]
      == [row_src[k][0] for k in E.STRATEGY_KEYS],
      str([m52["strategies"][k]["total_trades"] for k in E.STRATEGY_KEYS]))
check("10계좌 잔고 무손실",
      [m52["strategies"][k]["cash_krw"] for k in E.STRATEGY_KEYS]
      == [float(row_src[k][4]) for k in E.STRATEGY_KEYS],
      str([m52["strategies"][k]["cash_krw"] for k in E.STRATEGY_KEYS]))
check("10계좌 실현손익 무손실",
      [round(m52["strategies"][k]["total_realized_pnl"]) for k in E.STRATEGY_KEYS]
      == [row_src[k][3] for k in E.STRATEGY_KEYS])
check("승·패 무손실",
      [m52["strategies"][k]["wins"] for k in E.STRATEGY_KEYS]
      == [row_src[k][1] for k in E.STRATEGY_KEYS])
check("보유 포지션 그대로 승계 (강제 정리 없음)",
      (m52["strategies"]["3m"]["position"] or {}).get("market") == "KRW-NEAR"
      and (m52["strategies"]["bb_squeeze"]["position"] or {}).get("market") == "KRW-PIEVERSE")
check("매매내역 유지", m52["strategies"]["3m"]["recent_trades"][-1]["exit_kind"] == "STAGNATION")
check("v4_archive 보존",
      ((m52.get("v4_archive") or {}).get("retired_totals") or {}).get("count") == 3)
check("v4→v5 승계 이력 보존",
      (m52.get("migrated_from") or {}).get("carried_trades") == 230)
check("v5_fix(v5.0→v5.1) 이력 보존",
      (m52.get("v5_fix") or {}).get("from_version") == "5.0"
      and m52["v5_fix"]["carried_trades"] == 418)
check("v5_2_fix 승계 기록",
      (m52.get("v5_2_fix") or {}).get("from_version") == "5.1"
      and m52["v5_2_fix"]["carried_trades"] == sum(row_src[k][0] for k in E.STRATEGY_KEYS))
check("v5_2_fix 승계 자산 = 10계좌 합",
      m52["v5_2_fix"]["carried_equity"] == sum(row_src[k][4] for k in E.STRATEGY_KEYS),
      str(m52["v5_2_fix"]["carried_equity"]))
check("v5_2_fix 보유 포지션 기록",
      sorted(m52["v5_2_fix"]["carried_positions"]) == ["10m", "3m", "bb_squeeze"],
      str(m52["v5_2_fix"]["carried_positions"]))
check("파라미터 교체 3m (1.5 / -1.3 / 800만 / BID 78% / 정체 청산 없음)",
      m52["strategies"]["3m"]["params"]["target_pct"] == 1.5
      and m52["strategies"]["3m"]["params"]["stop_pct"] == -1.3
      and m52["strategies"]["3m"]["params"]["min_short_krw"] == 8_000_000
      and m52["strategies"]["3m"]["params"]["min_bid_ratio"] == 0.78
      and "stagnation_min" not in m52["strategies"]["3m"]["params"])
check("파라미터 교체 alt_rs (1.3%p / 700만 / BID 75% / 1일 6회)",
      m52["strategies"]["alt_rs"]["params"]["min_rs_pct"] == 1.3
      and m52["strategies"]["alt_rs"]["params"]["min_short_krw"] == 7_000_000
      and m52["strategies"]["alt_rs"]["params"]["min_bid_ratio"] == 0.75
      and m52["strategies"]["alt_rs"]["params"]["max_trades_per_day"] == 6)
check("파라미터 교체 panic_bounce (1.8 / 1.2→0.35 / -1.25)",
      m52["strategies"]["panic_bounce"]["params"]["target_pct"] == 1.8
      and m52["strategies"]["panic_bounce"]["params"]["trail_activation_pct"] == 1.2
      and m52["strategies"]["panic_bounce"]["params"]["stop_pct"] == -1.25)
check("규칙 바뀐 계좌는 예산 시계 리셋",
      m52["strategies"]["3m"]["rules_effective_from"] != TA_OLD
      and "3m" in m52["v5_2_fix"]["rules_changed_keys"])
check("규칙 그대로인 30m·tape_sweep 은 시계 유지",
      m52["strategies"]["30m"]["rules_effective_from"] == TA_OLD
      and m52["strategies"]["tape_sweep"]["rules_effective_from"] == TA_OLD
      and "30m" not in m52["v5_2_fix"]["rules_changed_keys"]
      and "tape_sweep" not in m52["v5_2_fix"]["rules_changed_keys"],
      str(m52["v5_2_fix"]["rules_changed_keys"]))
check("총 시드·최고점 보존", m52["initial_capital"] == 20_000_000 and m52["peak_equity"] == 20_000_000)

# ensure_league_state 경로 (Gist 대신 로컬 v5.1 파일로 확인)
E.gist_enabled = lambda: False
E.last_load_source = lambda: "local"
persistence._state_cache = None
persistence.save_state(json.loads(json.dumps(v51)), remote=False)
ens = E.ensure_league_state()
check("ensure: v5.1 파일 → v5.2 승계", ens["version"] == "5.2", str(ens.get("version")))
check("ensure: 전적 유지", ens["total_trades"] == sum(row_src[k][0] for k in E.STRATEGY_KEYS),
      str(ens["total_trades"]))
check("ensure: alt_rs 현금 유지", ens["strategies"]["alt_rs"]["cash_krw"] == 2_000_000)
check("ensure: 3m 포지션 유지",
      (ens["strategies"]["3m"].get("position") or {}).get("market") == "KRW-NEAR")
check("ensure: v5_2_fix 기록", (ens.get("v5_2_fix") or {}).get("from_version") == "5.1")

# ------------------------------------------------------------- [16] Gist 압축
print("[16] Gist 업로드 gzip 압축 (Render 대역폭 절감)")


class _FakeResp:
    def __init__(self, body=b"{}"):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


captured = {"calls": 0}
_real_urlopen = persistence.urllib.request.urlopen


def _fake_urlopen(req, timeout=None):
    captured["calls"] += 1
    captured["url"] = req.full_url
    captured["data"] = req.data
    return _FakeResp()


big = json.loads(json.dumps(ens))
plain_payload = json.dumps({"files": {"state.json": {
    "content": json.dumps(big, indent=2, ensure_ascii=False)}}}, ensure_ascii=False).encode("utf-8")
persistence.urllib.request.urlopen = _fake_urlopen
os.environ["GIST_TOKEN"] = "test-token"
os.environ["GIST_ID"] = "test-gist"
persistence._last_sync_ts = 0.0
try:
    sent_ok = persistence.save_state(big, force_remote=True)
    first_calls = captured["calls"]
    throttled_ok = persistence.save_state(big, remote=True)   # 300초 안 → 원격 저장 안 함
except Exception as _e:  # noqa: BLE001
    sent_ok, throttled_ok, first_calls = f"EXC {_e}", None, -1
finally:
    persistence.urllib.request.urlopen = _real_urlopen
    os.environ.pop("GIST_TOKEN", None)
    os.environ.pop("GIST_ID", None)
    persistence._last_sync_ts = 0.0

sent = json.loads(captured["data"].decode("utf-8"))["files"]["state.json"]["content"]
check("gzip: 업로드 성공", sent_ok is True)
check("gzip: GZB64 접두사", sent.startswith(persistence.GIST_COMPRESS_PREFIX))
check("gzip: 왕복 복원 일치", persistence._decode_gist_content(sent) == big)
check("gzip: 업로드 본문 20% 이하",
      len(captured["data"]) < len(plain_payload) * 0.2,
      f"{len(captured['data'])}B / 평문 {len(plain_payload)}B")
check("gzip: 동기화 간격 300초", persistence.GIST_SYNC_MIN_INTERVAL == 300.0,
      str(persistence.GIST_SYNC_MIN_INTERVAL))
check("gzip: 300초 안 재전송 억제", throttled_ok is False and captured["calls"] == first_calls,
      f"calls {captured['calls']}")
check("gzip: 예전 평문 상태 하위 호환",
      persistence._decode_gist_content(json.dumps(big, indent=2, ensure_ascii=False)) == big)


print()
print(f"총 {OK + FAIL}개 검사 · PASS {OK} · FAIL {FAIL}")
if FAILED:
    print("실패:", FAILED)
sys.exit(1 if FAIL else 0)
