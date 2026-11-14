"""v3.1 대금별 3계좌 병렬 엔진 단위 검증 (합성 틱 스트림).

핵심 검증: 3개 계좌(30m/10m/3m)가 같은 웹소켓 틱과 같은 판단 루프를 공유하면서도
잔고·포지션·전적·쿨다운이 서로 간섭하지 않고 독립적으로 매수/청산되는지.

실행: python3 tests/test_multi_tier.py
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

KST = timezone(timedelta(hours=9))
TMP = os.path.join(HERE, "_test_state.json")

os.environ.pop("GIST_TOKEN", None)
os.environ.pop("GIST_ID", None)
if os.path.exists(TMP):
    os.remove(TMP)
persistence.STATE_FILE = TMP
persistence._state_cache = None

BULL = {"is_bearish": False, "btc_price": 92000000, "sma20": 91500000,
        "diff_pct": 0.55, "trend": "BULLISH"}
BEAR = {"is_bearish": True, "btc_price": 91000000, "sma20": 92000000,
        "diff_pct": -0.35, "trend": "BEARISH"}


class FakeStream:
    """웹소켓 롤링 윈도우 대역 — 종목별 지표/가격을 직접 주입한다."""

    def __init__(self):
        self.price = {}
        self.metrics = {}
        self._names = {}
        self.extra = []

    def markets(self):
        return list(self.metrics.keys())

    def names(self):
        return dict(self._names)

    def last_price(self, m):
        return self.price.get(m)

    def market_metrics(self, m, now_ms=None, short_sec=15, base_sec=180):
        return self.metrics.get(m)

    def add_extra_markets(self, codes):
        self.extra = list(codes)


def metric(market="KRW-TEST", price=100.0, short_krw=40_000_000.0, surge=5.0,
           bid=0.8, chg=1.2, **kw):
    d = {"market": market, "last_price": price, "short_krw": short_krw,
         "short_bid_krw": short_krw * bid, "bid_ratio": bid, "base_krw": 100_000_000.0,
         "base_ticks": 300, "baseline_15s_krw": short_krw / max(surge, 1e-9),
         "surge_ratio": surge, "chg_3m_pct": chg, "coverage_sec": 180.0,
         "ticks_in_window": 400, "last_tick_age_sec": 0.2}
    d.update(kw)
    return d


def fresh(btc=BULL):
    state = E.new_multi_state()
    persistence._state_cache = state
    persistence.save_state(state, remote=False)
    E._btc_cache["data"] = dict(btc)
    E._btc_cache["ts"] = time.time()
    return state


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


def tiers(state):
    return state["tiers"]


# ------------------------------------------------------------------ [1] 구조
print("[1] 3계좌 초기 구조")
s = fresh()
check("version", s["version"] == "3.1", s["version"])
check("3 tiers", sorted(tiers(s).keys()) == ["10m", "30m", "3m"], str(list(tiers(s).keys())))
check("each 2M", all(tiers(s)[k]["cash_krw"] == 2_000_000 for k in E.TIER_KEYS))
check("each 0 trades", all(tiers(s)[k]["total_trades"] == 0 for k in E.TIER_KEYS))
check("total capital 6M", s["initial_capital"] == 6_000_000, str(s["initial_capital"]))
check("thresholds", [tiers(s)[k]["min_short_krw"] for k in ("30m", "10m", "3m")] ==
      [30_000_000, 10_000_000, 3_000_000])
check("rules tier map", s["rules"]["tier_min_short_krw"] ==
      {"30m": 30_000_000, "10m": 10_000_000, "3m": 3_000_000})

# ------------------------------------------------------- [2] 문턱별 진입 분리
print("[2] 15초 거래대금 문턱별 진입 계좌 분리")

st = FakeStream()
s = fresh()
st.metrics = {"KRW-SMALL": metric("KRW-SMALL", 10.0, short_krw=5_000_000.0, surge=9.0)}
st.price = {"KRW-SMALL": 10.0}
st._names = {"KRW-SMALL": "소형"}
E.run_sniper_tick(st)
check("500만 → 3m만 진입", tiers(s)["3m"]["position"] and not tiers(s)["10m"]["position"]
      and not tiers(s)["30m"]["position"],
      f"3m={bool(tiers(s)['3m']['position'])} 10m={bool(tiers(s)['10m']['position'])}")

st = FakeStream()
s = fresh()
st.metrics = {"KRW-MID": metric("KRW-MID", 10.0, short_krw=12_000_000.0, surge=8.0)}
st.price = {"KRW-MID": 10.0}
st._names = {"KRW-MID": "중형"}
E.run_sniper_tick(st)
check("1,200만 → 3m+10m 진입, 30m 제외",
      tiers(s)["3m"]["position"] and tiers(s)["10m"]["position"] and not tiers(s)["30m"]["position"])
check("30m 대기 문구", "문턱" in tiers(s)["30m"]["last_event"] or "대기" in tiers(s)["30m"]["last_event"],
      tiers(s)["30m"]["last_event"])
check("3m·10m 각자 현금 0", tiers(s)["3m"]["cash_krw"] == 0.0 and tiers(s)["10m"]["cash_krw"] == 0.0)
check("30m 현금 그대로", tiers(s)["30m"]["cash_krw"] == 2_000_000)

st = FakeStream()
s = fresh()
st.metrics = {"KRW-BIG": metric("KRW-BIG", 10.0, short_krw=35_000_000.0, surge=6.0)}
st.price = {"KRW-BIG": 10.0}
st._names = {"KRW-BIG": "대형"}
E.run_sniper_tick(st)
check("3,500만 → 3계좌 모두 진입", all(tiers(s)[k]["position"] for k in E.TIER_KEYS))
check("동일 종목 독립 보유", all(tiers(s)[k]["position"]["market"] == "KRW-BIG" for k in E.TIER_KEYS))
check("계좌별 수량 동일(각 200만)", all(abs(tiers(s)[k]["position"]["coin_qty"] -
      tiers(s)["30m"]["position"]["coin_qty"]) < 1e-9 for k in E.TIER_KEYS))

# ------------------------------------------------ [3] 청산 독립성 (간섭 없음)
print("[3] 한 계좌 손절이 다른 계좌에 영향 없음")
st = FakeStream()
s = fresh()
st.metrics = {"KRW-FAST": metric("KRW-FAST", 10.0, short_krw=5_000_000.0, surge=9.0),
              "KRW-HEAVY": metric("KRW-HEAVY", 50.0, short_krw=40_000_000.0, surge=5.0)}
st.price = {"KRW-FAST": 10.0, "KRW-HEAVY": 50.0}
st._names = {"KRW-FAST": "급등", "KRW-HEAVY": "대형"}
E.run_sniper_tick(st)
check("3m→급등 / 30m·10m→대형",
      tiers(s)["3m"]["position"]["market"] == "KRW-FAST"
      and tiers(s)["10m"]["position"]["market"] == "KRW-HEAVY"
      and tiers(s)["30m"]["position"]["market"] == "KRW-HEAVY")

st.price["KRW-FAST"] = 10.0 * (1 - 0.018)   # 3m 계좌만 손절
E.run_sniper_tick(st)
t3 = tiers(s)["3m"]
check("3m 손절 청산", t3["position"] is None and t3["losses"] == 1 and t3["total_trades"] == 1,
      str(t3["recent_trades"][-1].get("exit_kind")) if t3["recent_trades"] else "no trade")
check("3m 쿨다운 설정", t3["cooldown_until"] is not None, str(t3["cooldown_until"]))
check("10m·30m 포지션 유지", tiers(s)["10m"]["position"] is not None
      and tiers(s)["30m"]["position"] is not None)
check("10m·30m 전적 0", tiers(s)["10m"]["total_trades"] == 0 and tiers(s)["30m"]["total_trades"] == 0)
check("10m·30m 쿨다운 없음", tiers(s)["10m"]["cooldown_until"] is None)

# 대형주 상승 → 10m/30m 각자 목표 익절
st.price["KRW-HEAVY"] = 50.0 * 1.03
E.run_sniper_tick(st)
for k in ("10m", "30m"):
    tr = tiers(s)[k]["recent_trades"][-1]
    check(f"{k} 목표 익절", tr.get("exit_kind") == "TARGET" and tiers(s)[k]["wins"] == 1,
          f"{tr.get('gain_pct')}%")
check("3m 전적 불변", tiers(s)["3m"]["wins"] == 0 and tiers(s)["3m"]["losses"] == 1)

# --------------------------------------------- [4] 쿨다운 격리 + 계좌별 자금
print("[4] 쿨다운·자금 독립")
s = fresh()
st = FakeStream()
st.metrics = {"KRW-A": metric("KRW-A", 10.0, short_krw=5_000_000.0, surge=9.0)}
st.price = {"KRW-A": 10.0}
E.run_sniper_tick(st)
st.price["KRW-A"] = 10.0 * (1 - 0.02)
E.run_sniper_tick(st)
check("3m 손절", tiers(s)["3m"]["losses"] == 1)
st.metrics = {"KRW-B": metric("KRW-B", 20.0, short_krw=15_000_000.0, surge=7.0)}
st.price = {"KRW-B": 20.0}
E.run_sniper_tick(st)
check("3m 쿨다운 중 재진입 차단", tiers(s)["3m"]["position"] is None, tiers(s)["3m"]["last_event"])
check("10m은 새 종목 진입", tiers(s)["10m"]["position"] and
      tiers(s)["10m"]["position"]["market"] == "KRW-B")
check("30m은 문턱 미달 대기", tiers(s)["30m"]["position"] is None)
expected_cash = 2_000_000 * 0.98 * 0.9995 * 0.9995   # -2% 관측가 손절 체결 + 왕복 수수료
check("계좌별 현금 독립", abs(tiers(s)["3m"]["cash_krw"] - expected_cash) < 200,
      f"{tiers(s)['3m']['cash_krw']:.2f} vs {expected_cash:.2f}")

# ------------------------------------------------------ [5] BTC 필터 (공통)
print("[5] BTC 필터 — 신규 진입만 차단, 보유는 유지")
s = fresh()
st = FakeStream()
st.metrics = {"KRW-A": metric("KRW-A", 10.0, short_krw=40_000_000.0, surge=6.0)}
st.price = {"KRW-A": 10.0}
E.run_sniper_tick(st)
check("필터 전 3계좌 진입", all(tiers(s)[k]["position"] for k in E.TIER_KEYS))
E._btc_cache["data"] = dict(BEAR)
E._btc_cache["ts"] = time.time()
E.run_sniper_tick(st)
check("보유 유지", all(tiers(s)[k]["position"] for k in E.TIER_KEYS))
s2 = fresh(BEAR)
st2 = FakeStream()
st2.metrics = {"KRW-A": metric("KRW-A", 10.0, short_krw=40_000_000.0, surge=6.0)}
st2.price = {"KRW-A": 10.0}
E.run_sniper_tick(st2)
check("하락장 전 계좌 차단", all(tiers(s2)[k]["position"] is None for k in E.TIER_KEYS))
check("차단 사유 표기", all("BTC" in tiers(s2)[k]["last_event"] for k in E.TIER_KEYS))

# ------------------------------------------------- [6] 청산 규칙 계좌별 동작
print("[6] 트레일링·시간초과 청산")
s = fresh()
st = FakeStream()
st.metrics = {"KRW-T": metric("KRW-T", 10.0, short_krw=5_000_000.0, surge=9.0)}
st.price = {"KRW-T": 10.0}
E.run_sniper_tick(st)
st.price["KRW-T"] = 10.0 * 1.016
E.run_sniper_tick(st)
st.price["KRW-T"] = 10.0 * 1.016 * 0.995
E.run_sniper_tick(st)
tr = tiers(s)["3m"]["recent_trades"][-1]
check("트레일링 청산", tr.get("exit_kind") == "TRAILING", tr.get("reason", "")[:40])

s = fresh()
st = FakeStream()
st.metrics = {"KRW-H": metric("KRW-H", 10.0, short_krw=5_000_000.0, surge=9.0)}
st.price = {"KRW-H": 10.0}
E.run_sniper_tick(st)
tiers(s)["3m"]["position"]["entry_ts_ms"] = E._now_ms() - 21 * 60 * 1000
E.run_sniper_tick(st)
tr = tiers(s)["3m"]["recent_trades"][-1]
check("20분 시간초과", tr.get("exit_kind") == "TIME", tr.get("reason", "")[:40])
check("시간초과는 쿨다운 없음", tiers(s)["3m"]["cooldown_until"] is None)

# ------------------------------------------------------------ [7] 감시 패널
print("[7] 감시 패널 계좌별 집계")
s = fresh()
st = FakeStream()
st.metrics = {"KRW-M": metric("KRW-M", 10.0, short_krw=12_000_000.0, surge=8.0),
              "KRW-S": metric("KRW-S", 5.0, short_krw=8_000_000.0, surge=6.0),
              "KRW-B": metric("KRW-B", 20.0, short_krw=20_000_000.0, surge=5.0, bid=0.5)}
st.price = {"KRW-M": 10.0, "KRW-S": 5.0, "KRW-B": 20.0}
E.run_sniper_tick(st)
qbt = s["live_scan"]["qualified_by_tier"]
check("qualified_by_tier", qbt["3m"] == 2 and qbt["10m"] == 1 and qbt["30m"] == 0, str(qbt))
tops = {m["market"]: m for m in s["live_scan"]["top"]}
check("qualified_tiers 표기", tops["KRW-M"]["qualified_tiers"] == ["10m", "3m"],
      str(tops["KRW-M"].get("qualified_tiers")))
check("3m만 가능한 종목", tops["KRW-S"]["qualified_tiers"] == ["3m"],
      str(tops["KRW-S"].get("qualified_tiers")))
check("공통조건 미달 사유", "매수비율" in tops["KRW-B"]["reject"], tops["KRW-B"]["reject"][:60])

# --------------------------------------------------------- [8] 집계/영속성
print("[8] 집계 · 영속성")
s = fresh()
st = FakeStream()
st.metrics = {"KRW-X": metric("KRW-X", 10.0, short_krw=35_000_000.0, surge=6.0)}
st.price = {"KRW-X": 10.0}
E.run_sniper_tick(st)
st.price["KRW-X"] = 10.0 * 1.03
E.run_sniper_tick(st)
agg_eq = s["current_equity"]
manual = sum(tiers(s)[k]["current_equity"] for k in E.TIER_KEYS)
check("통합 자산 = 계좌 합", abs(agg_eq - manual) < 2, f"{agg_eq} vs {manual}")
check("통합 전적 3승", s["wins"] == 3 and s["total_trades"] == 3, f"{s['wins']}/{s['total_trades']}")
check("통합 최근매매 6건(매수3+매도3)", len(s["recent_trades"]) == 6, str(len(s["recent_trades"])))
saved = json.load(open(TMP, encoding="utf-8"))
check("파일 저장 v3.1", saved["version"] == "3.1")
check("파일 저장 3계좌", sorted(saved["tiers"].keys()) == ["10m", "30m", "3m"])
check("계좌별 전적 저장", all(saved["tiers"][k]["total_trades"] == 1 for k in E.TIER_KEYS))

# ------------------------------------------------------------ [9] 마이그레이션
print("[9] v3.0 단일 계좌 → 3계좌 승계")
legacy = {
    "version": "3.0", "status": "IN_POSITION", "initialized_at": "2026-11-14 19:19:12",
    "initial_capital": 2000000, "cash_krw": 0.0,
    "position": {"market": "KRW-OLD", "name": "올드", "entry_price": 100.0,
                 "highest_price": 101.0, "coin_qty": 19990.0, "cost_krw": 2000000.0,
                 "entry_time": "2026-11-14 19:30:00", "entry_ts_ms": E._now_ms()},
    "cooldown_until": None, "total_realized_pnl": 12345.0, "total_trades": 4,
    "wins": 2, "losses": 2,
    "recent_trades": [{"type": "BUY", "market": "KRW-OLD", "name": "올드",
                       "entry_time": "2026-11-14 19:30:00", "entry_price": 100.0,
                       "cost_krw": 2000000}],
}
mig = E.migrate_single_state(legacy)
check("승계 버전", mig["version"] == "3.1")
check("30m 슬롯 승계", mig["tiers"]["30m"]["position"]["market"] == "KRW-OLD",
      str(mig["tiers"]["30m"].get("position", {}).get("market")))
check("30m 전적 승계", mig["tiers"]["30m"]["total_trades"] == 4 and mig["tiers"]["30m"]["wins"] == 2)
check("30m 실현손익 승계", mig["tiers"]["30m"]["total_realized_pnl"] == 12345.0)
check("10m/3m 신규 200만", mig["tiers"]["10m"]["cash_krw"] == 2_000_000
      and mig["tiers"]["3m"]["cash_krw"] == 2_000_000)
check("10m/3m 0전", mig["tiers"]["10m"]["total_trades"] == 0
      and mig["tiers"]["3m"]["total_trades"] == 0)
check("마이그레이션 기록", mig["migrated_from"]["trades"] == 4 and mig["migrated_from"]["had_position"])

empty_legacy = dict(legacy, position=None, cash_krw=2_000_000.0, total_trades=0, wins=0,
                    losses=0, recent_trades=[], total_realized_pnl=0.0)
mig2 = E.migrate_single_state(empty_legacy)
check("무체결 v3.0 → 3계좌 모두 200만", all(mig2["tiers"][k]["cash_krw"] == 2_000_000
      and mig2["tiers"][k]["total_trades"] == 0 for k in E.TIER_KEYS))

# --------------------------------------------- [10] ensure_v3_state 마이그레이션
print("[10] ensure_v3_state 경로")
persistence._state_cache = None
persistence.save_state(dict(legacy), remote=False)
E.ensure_v3_state()
check("ensure가 v3.1로 전환", persistence._state_cache["version"] == "3.1")
check("ensure 승계 유지", persistence._state_cache["tiers"]["30m"]["position"]["market"] == "KRW-OLD")
persistence._state_cache = None
persistence.save_state({"version": "9.9", "cash_krw": 5}, remote=False)
E.ensure_v3_state()
st_new = persistence._state_cache
check("미지 버전 → 신규 3계좌", st_new["version"] == "3.1"
      and all(st_new["tiers"][k]["cash_krw"] == 2_000_000 for k in E.TIER_KEYS))

print(f"\nRESULT: {OK} passed, {FAIL} failed")
if FAILED:
    print("FAILED:", FAILED)
sys.exit(1 if FAIL else 0)
