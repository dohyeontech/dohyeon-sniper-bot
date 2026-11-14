"""
스나이퍼 봇 v4.0 — 「10대 단타 전략 실시간 비교 리그」.

업비트 실시간 WebSocket 체결 스트림 **1개** 위에서, 잔고·포지션·전적·쿨다운·매매내역이
완전히 분리된 **10개의 독립 가상 계좌(각 시드 2,000,000원)** 를 동시에 운용한다.
같은 시장 스냅샷을 모든 전략이 공유하므로 "같은 순간에 어떤 규칙이 돈을 버는가"를
조건이 동일한 상태에서 비교할 수 있다.

  기존 승계 (v3.1 3계좌, 잔고·전적·포지션 그대로 이관)
    1. 3m               — 300만↑ 급등 추격
    2. 10m              — 1,000만↑ 급등 추격
    3. 30m              — 3,000만↑ 급등 추격
  신규 7전략 (각 200만 원 신규 시드)
    4. panic_bounce     — 투매 과매도 반등
    5. pullback_reentry — 눌림목 재진입
    6. vwap_breakout    — 실시간 VWAP 돌파
    7. streak_ignition  — 연속 매수 긁기
    8. major_momentum   — 대형주 TOP10 스캘핑
    9. rsi_oversold     — 1분봉 RSI 과매도 반등
   10. bb_squeeze       — 1분봉 볼린저 스퀴즈 돌파

공통 리스크 규칙: BTC 15분봉 20이평 대비 -0.2% 초과 하회 시 **모멘텀·돌파 8전략 신규 진입 차단**
(보유 포지션은 유지). 역추세 2전략(`panic_bounce`, `rsi_oversold`)만 `btc_filter: False`로 예외 적용되어
BTC 하락 추세에서도 진입한다. 손절 시에만 전략별 쿨다운이 걸리고, 그 쿨다운도 계좌별로 독립이다.

청산은 1.5초 판단 루프에서 틱 가격으로 감시한다. 목표 익절은 목표가 체결(초과분 미반영,
보수적), 손절·트레일링·시간초과는 관측 틱 가격 체결(슬리피지 반영). 수수료 왕복 0.05%.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from persistence import load_state, save_state, last_load_source, gist_enabled  # noqa: E402
from scanner import check_btc_trend, fetch_tickers  # noqa: E402

VERSION = "4.0"
FEE_RATE = 0.0005           # 업비트 수수료 0.05%
TICK_INTERVAL_SEC = 1.5     # 엔진 판단 주기
DEFAULT_SEED = 2_000_000    # 전략별 모의 시드
KST = timezone(timedelta(hours=9))

LEGACY_KEYS = ("3m", "10m", "30m")
NEW_KEYS = ("panic_bounce", "pullback_reentry", "vwap_breakout", "streak_ignition",
            "major_momentum", "rsi_oversold", "bb_squeeze")
STRATEGY_KEYS = LEGACY_KEYS + NEW_KEYS

# ---------------------------------------------------------------- 전략 정의
STRATEGIES = {
    # ---- v3.1에서 승계되는 3계좌 (규칙 동일, 15초 거래대금 문턱만 다름) ----
    "3m": {
        "key": "3m", "label": "300만↑", "name": "300만↑ 급등 추격",
        "style": "공격형 · 초민감 스캘핑", "kind": "surge", "legacy": True, "btc_filter": True,
        "entry": "15초 거래대금 300만↑ · 직전 3분 평균 대비 4.0배 · 매수(BID) 70%↑ · 3분변동 +0.3~+3.0%",
        "exit": "목표 +2.5% · 트레일링 +1.5%→고점 -0.4% · 손절 -1.8% · 최대 20분 · 쿨다운 5분",
        "params": {"min_short_krw": 3_000_000, "min_surge_ratio": 4.0, "min_bid_ratio": 0.70,
                   "min_chg_pct": 0.3, "max_chg_pct": 3.0, "target_pct": 2.5,
                   "trail_activation_pct": 1.5, "trail_drop_pct": 0.4, "stop_pct": -1.8,
                   "max_hold_min": 20, "cooldown_min": 5},
    },
    "10m": {
        "key": "10m", "label": "1,000만↑", "name": "1,000만↑ 급등 추격",
        "style": "균형형 · 중형 슈팅", "kind": "surge", "legacy": True, "btc_filter": True,
        "entry": "15초 거래대금 1,000만↑ · 직전 3분 평균 대비 4.0배 · 매수(BID) 70%↑ · 3분변동 +0.3~+3.0%",
        "exit": "목표 +2.5% · 트레일링 +1.5%→고점 -0.4% · 손절 -1.8% · 최대 20분 · 쿨다운 5분",
        "params": {"min_short_krw": 10_000_000, "min_surge_ratio": 4.0, "min_bid_ratio": 0.70,
                   "min_chg_pct": 0.3, "max_chg_pct": 3.0, "target_pct": 2.5,
                   "trail_activation_pct": 1.5, "trail_drop_pct": 0.4, "stop_pct": -1.8,
                   "max_hold_min": 20, "cooldown_min": 5},
    },
    "30m": {
        "key": "30m", "label": "3,000만↑", "name": "3,000만↑ 급등 추격",
        "style": "보수형 · 대형 슈팅", "kind": "surge", "legacy": True, "btc_filter": True,
        "entry": "15초 거래대금 3,000만↑ · 직전 3분 평균 대비 4.0배 · 매수(BID) 70%↑ · 3분변동 +0.3~+3.0%",
        "exit": "목표 +2.5% · 트레일링 +1.5%→고점 -0.4% · 손절 -1.8% · 최대 20분 · 쿨다운 5분",
        "params": {"min_short_krw": 30_000_000, "min_surge_ratio": 4.0, "min_bid_ratio": 0.70,
                   "min_chg_pct": 0.3, "max_chg_pct": 3.0, "target_pct": 2.5,
                   "trail_activation_pct": 1.5, "trail_drop_pct": 0.4, "stop_pct": -1.8,
                   "max_hold_min": 20, "cooldown_min": 5},
    },
    # ---- 신규 7전략 ----
    "panic_bounce": {
        "key": "panic_bounce", "label": "투매 반등", "name": "투매 과매도 반등",
        "style": "역추세 · 급락 되돌림", "kind": "panic_bounce", "legacy": False, "btc_filter": False,
        "entry": "3분 내 -2.5% 이상 급락 후 최근 15초 매수(BID) 비율 65%↑ & 직전 15초 대비 가격 반등",
        "exit": "목표 +2.0% · 손절 -1.5% · 최대 15분 · 쿨다운 5분",
        "params": {"min_drop_pct": -2.5, "min_bid_ratio": 0.65, "target_pct": 2.0,
                   "trail_activation_pct": None, "trail_drop_pct": None, "stop_pct": -1.5,
                   "max_hold_min": 15, "cooldown_min": 5},
    },
    "pullback_reentry": {
        "key": "pullback_reentry", "label": "눌림 재진입", "name": "눌림목 재진입",
        "style": "추세추종 · 눌림 매수", "kind": "pullback", "legacy": False, "btc_filter": True,
        "entry": "3~10분 고점 대비 -1.0~-2.2% 눌림 구간에서 15초 BID 65%↑ + 가격 상승 전환",
        "exit": "목표 +2.2% · 손절 -1.6% · 최대 25분 · 쿨다운 5분",
        "params": {"dd_min_pct": -2.2, "dd_max_pct": -1.0, "min_bid_ratio": 0.65,
                   "min_coverage_10m_sec": 180, "target_pct": 2.2,
                   "trail_activation_pct": None, "trail_drop_pct": None, "stop_pct": -1.6,
                   "max_hold_min": 25, "cooldown_min": 5},
    },
    "vwap_breakout": {
        "key": "vwap_breakout", "label": "VWAP 돌파", "name": "실시간 VWAP 돌파",
        "style": "추세추종 · 평균가 돌파", "kind": "vwap", "legacy": False, "btc_filter": True,
        "entry": "직전 3분 VWAP 하단에서 15초 거래대금 2.5배↑ + BID 65%↑ 동반, VWAP +0.3%↑ 상향 돌파",
        "exit": "목표 +2.0% · 손절 -1.5% · 최대 25분 · 쿨다운 5분",
        "params": {"min_vol_ratio": 2.5, "min_bid_ratio": 0.65, "breakout_pct": 0.3,
                   "min_coverage_sec": 120, "target_pct": 2.0,
                   "trail_activation_pct": None, "trail_drop_pct": None, "stop_pct": -1.5,
                   "max_hold_min": 25, "cooldown_min": 5},
    },
    "streak_ignition": {
        "key": "streak_ignition", "label": "매수 긁기", "name": "연속 매수 긁기",
        "style": "모멘텀 · 순간 초동", "kind": "streak", "legacy": False, "btc_filter": True,
        "entry": "최근 10초 BID 비중 85%↑ + 체결 15건↑ + 15초 거래대금 500만↑",
        "exit": "목표 +1.8% · 손절 -1.2% · 최대 10분 · 쿨다운 3분",
        "params": {"min_ticks_10s": 15, "min_bid_10s": 0.85, "min_short_krw": 5_000_000,
                   "min_coverage_sec": 20, "target_pct": 1.8,
                   "trail_activation_pct": None, "trail_drop_pct": None, "stop_pct": -1.2,
                   "max_hold_min": 10, "cooldown_min": 3},
    },
    "major_momentum": {
        "key": "major_momentum", "label": "대형주 스캘핑", "name": "대형주 TOP10 스캘핑",
        "style": "모멘텀 · 유동성 상위", "kind": "major", "legacy": False, "btc_filter": True,
        "entry": "24h 거래대금 TOP10만 · 15초 거래대금 3.0배↑ + BID 65%↑ + 3분변동 +0.2~+2.0%",
        "exit": "목표 +1.5% · 트레일링 +1.0%→고점 -0.3% · 손절 -1.2% · 최대 30분 · 쿨다운 5분",
        "params": {"top_n": 10, "min_surge_ratio": 3.0, "min_bid_ratio": 0.65,
                   "min_chg_pct": 0.2, "max_chg_pct": 2.0, "target_pct": 1.5,
                   "trail_activation_pct": 1.0, "trail_drop_pct": 0.3, "stop_pct": -1.2,
                   "max_hold_min": 30, "cooldown_min": 5},
    },
    "rsi_oversold": {
        "key": "rsi_oversold", "label": "RSI 과매도", "name": "1분봉 RSI 과매도 반등",
        "style": "역추세 · 지표 반전", "kind": "rsi", "legacy": False, "btc_filter": False,
        "entry": "1분봉 RSI(14) 25 이하 과매도에서 RSI 상향 반전 + 15초 BID 60%↑",
        "exit": "목표 +1.8% · 손절 -1.5% · 최대 25분 · 쿨다운 5분",
        "params": {"rsi_period": 14, "rsi_threshold": 25.0, "min_bid_ratio": 0.60,
                   "max_candle_age_sec": 180, "target_pct": 1.8,
                   "trail_activation_pct": None, "trail_drop_pct": None, "stop_pct": -1.5,
                   "max_hold_min": 25, "cooldown_min": 5},
    },
    "bb_squeeze": {
        "key": "bb_squeeze", "label": "BB 스퀴즈", "name": "1분봉 볼린저 스퀴즈 돌파",
        "style": "변동성 확장 · 밴드 돌파", "kind": "bb", "legacy": False, "btc_filter": True,
        "entry": "1분봉 볼린저밴드(20,2) 수축(최근 40봉 밴드폭 하위 25%) 후 상단 밴드 거래량 2.0배↑ 돌파",
        "exit": "목표 +2.4% · 손절 -1.6% · 최대 30분 · 쿨다운 5분",
        "params": {"bb_period": 20, "bb_k": 2.0, "squeeze_rank": 0.25, "min_vol_ratio": 2.0,
                   "max_candle_age_sec": 180, "target_pct": 2.4,
                   "trail_activation_pct": None, "trail_drop_pct": None, "stop_pct": -1.6,
                   "max_hold_min": 30, "cooldown_min": 5},
    },
}


# BTC 하락 추세에서도 신규 진입을 허용하는 역추세 예외 전략 (나머지는 btc_filter: True)
BTC_FILTER_EXEMPT = tuple(k for k in STRATEGY_KEYS if not STRATEGIES[k].get("btc_filter", True))


def _rules_block():
    """대시보드·Gist에 저장하는 규칙 요약 (정적)."""
    return {
        "engine": "Upbit WebSocket trade stream (tick-level) · 10전략 병렬 리그",
        "tick_interval_sec": TICK_INTERVAL_SEC,
        "seed_per_strategy": DEFAULT_SEED,
        "short_window_sec": 15,
        "base_window_sec": 180,
        "btc_filter": {"enabled": True, "sma_window": "15분봉 20이평", "threshold_pct": -0.2,
                       "note": ("BTC가 20이평 대비 -0.2% 초과 하회하면 모멘텀·돌파 8전략 신규 진입 차단(보유 유지). "
                                "역추세 2전략(투매 과매도 반등·RSI 과매도 반등)은 예외로 진입 허용"),
                       "exempt_strategies": list(BTC_FILTER_EXEMPT)},
        "candle_cache": {"top_n": 15, "refresh_sec": 60, "sleep_sec": 0.25,
                         "note": "RSI·볼린저 전략용 1분봉은 별도 저속 스레드가 거래대금 상위 15종목만 순환 수집"},
        "gist_sync_interval_sec": 60,
        "fee_rate": FEE_RATE,
        "strategies": {
            k: {"key": k, "label": STRATEGIES[k]["label"], "name": STRATEGIES[k]["name"],
                "style": STRATEGIES[k]["style"], "entry": STRATEGIES[k]["entry"],
                "exit": STRATEGIES[k]["exit"], "params": dict(STRATEGIES[k]["params"]),
                "btc_filter": bool(STRATEGIES[k]["btc_filter"]),
                "carried_over": STRATEGIES[k]["legacy"]}
            for k in STRATEGY_KEYS
        },
    }


RULES_V4 = _rules_block()

_btc_cache = {"ts": 0.0, "data": None}
_rest_price_cache = {}
_candle_cache = None          # main.py가 set_candle_cache()로 주입
_ensure_guard = {"ts": 0.0, "last_note": None}


def kst_now():
    return datetime.now(KST)


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _now_ms():
    return int(time.time() * 1000)


def set_candle_cache(cache):
    global _candle_cache
    _candle_cache = cache


def get_candle_cache():
    return _candle_cache


def _btc_trend_cached(ttl=30.0):
    now = time.time()
    if _btc_cache["data"] is None or (now - _btc_cache["ts"]) > ttl:
        try:
            _btc_cache["data"] = check_btc_trend()
            _btc_cache["ts"] = now
        except Exception as e:
            if _btc_cache["data"] is None:
                _btc_cache["data"] = {"is_bearish": False, "btc_price": 0, "sma20": 0,
                                      "diff_pct": 0.0, "trend": "UNKNOWN"}
            print(f"[Engine] BTC trend fetch failed: {e}")
    return _btc_cache["data"]


def _rest_price(market, ttl=2.0):
    """웹소켓 틱이 끊긴 종목용 REST 시세 폴백 (2초 캐시)."""
    now = time.time()
    hit = _rest_price_cache.get(market)
    if hit and (now - hit[1]) < ttl:
        return hit[0]
    try:
        data = fetch_tickers([market])
        if data:
            price = float(data[0]["trade_price"])
            _rest_price_cache[market] = (price, now)
            return price
    except Exception:
        pass
    return hit[0] if hit else None


# ---------------------------------------------------------------- 상태 구조
def new_strategy(key, seed=DEFAULT_SEED, now_str=None):
    meta = STRATEGIES[key]
    now_str = now_str or fmt(kst_now())
    return {
        "key": key,
        "label": meta["label"],
        "name": meta["name"],
        "style": meta["style"],
        "kind": meta["kind"],
        "btc_filter": bool(meta["btc_filter"]),
        "carried_over": meta["legacy"],
        "entry": meta["entry"],
        "exit": meta["exit"],
        "params": dict(meta["params"]),
        "initialized_at": now_str,
        "initial_capital": float(seed),
        "cash_krw": float(seed),
        "position": None,
        "cooldown_until": None,
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "status": "WAITING_FOR_SIGNAL",
        "last_event": f"{meta['name']} 계좌 초기화 — 신호 대기",
        "watch": None,
        "current_equity": float(seed),
        "current_ret_pct": 0.0,
        "peak_equity": float(seed),
        "max_drawdown_pct": 0.0,
    }


def new_league_state(seed=DEFAULT_SEED):
    now_str = fmt(kst_now())
    strategies = {k: new_strategy(k, seed, now_str) for k in STRATEGY_KEYS}
    total = float(seed) * len(STRATEGY_KEYS)
    state = {
        "version": VERSION,
        "status": "WAITING_FOR_SIGNAL",
        "engine": "ws_tick_v4_league",
        "mode": "ten_strategy_league",
        "initialized_at": now_str,
        "initial_capital": total,
        "cash_krw": total,
        "position": None,
        "cooldown_until": None,
        "btc_trend": None,
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "last_checked_time": now_str,
        "last_event": "v4.0 10전략 리그 초기화 — 웹소켓 틱 스트림 대기",
        "rules": RULES_V4,
        "strategies": strategies,
        "strategy_order": list(STRATEGY_KEYS),
        "leaderboard": [],
        "current_equity": total,
        "current_ret_pct": 0.0,
        "peak_equity": total,
        "max_drawdown_pct": 0.0,
        "live_scan": {"scanned": 0, "qualified": 0, "top": []},
        "exit_rules_note": {
            "target": "목표 익절 체결가는 목표가로 계산 (틱 초과분 미반영, 보수적)",
            "stop": "손절/트레일링/시간초과는 관측 틱 가격으로 체결 (슬리피지 반영)",
        },
    }
    _alias_legacy(state)
    return state


def _alias_legacy(state):
    """v3.x 소비자 호환: state['tiers']는 승계 3계좌를 같은 객체로 가리킨다."""
    strat = state.get("strategies") or {}
    state["tiers"] = {k: strat[k] for k in LEGACY_KEYS if k in strat}
    return state


CARRY_FIELDS = ("initialized_at", "initial_capital", "cash_krw", "position", "cooldown_until",
                "total_realized_pnl", "total_trades", "wins", "losses", "recent_trades",
                "status", "last_event", "watch", "current_equity", "current_ret_pct",
                "peak_equity", "max_drawdown_pct")


def migrate_from_v3(old):
    """v3.0/v3.1 상태를 v4.0 리그로 승계한다.

    기존 3계좌(3m/10m/30m)의 잔고·전적·보유 포지션·쿨다운·매매내역을 **손대지 않고**
    같은 키의 전략 슬롯으로 그대로 옮기고, 신규 7전략만 200만 원 신규 시드로 채운다.
    """
    state = new_league_state()
    old_tiers = (old or {}).get("tiers") or {}
    carried = []
    for key in LEGACY_KEYS:
        src = old_tiers.get(key)
        if not isinstance(src, dict):
            continue
        dst = state["strategies"][key]
        for field in CARRY_FIELDS:
            if field in src and src[field] is not None:
                dst[field] = src[field]
        if src.get("initial_capital"):
            dst["initial_capital"] = float(src["initial_capital"])
        dst["carried_over"] = True
        dst["carried_from_version"] = (old or {}).get("version")
        # 승계된 매매내역에도 전략 라벨을 붙여 대시보드 필터가 끊기지 않게 한다
        for trade in dst.get("recent_trades") or []:
            if isinstance(trade, dict):
                trade.setdefault("strategy", key)
                trade.setdefault("strategy_label", dst["label"])
                trade.setdefault("strategy_name", dst["name"])
                if key in LEGACY_KEYS:
                    trade.setdefault("tier", key)
        if dst.get("position"):
            dst["status"] = "IN_POSITION"
            dst["last_event"] = f"v{(old or {}).get('version', '3.x')} 보유 포지션 그대로 승계"
        elif dst.get("total_trades"):
            dst["last_event"] = (f"v{(old or {}).get('version', '3.x')} 전적 {dst['total_trades']}전 "
                                 f"{dst.get('wins', 0)}승 {dst.get('losses', 0)}패 승계")
        else:
            dst["last_event"] = f"{dst['name']} — v{(old or {}).get('version', '3.x')} 기록 승계 (0전)"
        carried.append(key)

    state["migrated_from"] = {
        "version": (old or {}).get("version"),
        "at": fmt(kst_now()),
        "carried_keys": carried,
        "trades": sum(int(state["strategies"][k].get("total_trades") or 0) for k in carried),
        "equity": sum(float(state["strategies"][k].get("cash_krw") or 0) for k in carried),
        "had_position": any(bool(state["strategies"][k].get("position")) for k in carried),
        "note": "기존 3계좌는 잔고·전적·포지션 그대로 승계, 신규 7전략은 200만 원 신규 시드",
    }
    print(f"[Engine] Migrated v{(old or {}).get('version')} -> v{VERSION}: "
          f"carried {carried} (trades={state['migrated_from']['trades']}, "
          f"positions={state['migrated_from']['had_position']})")
    return state


def ensure_v4_state():
    """Gist/로컬 상태를 v4.0 10전략 리그로 맞춘다.

    - 이미 v4.0이면 누락 전략만 채우고 규칙 정의를 최신화
    - v3.x (3계좌)면 3계좌를 그대로 승계하고 신규 7전략 추가
    - 그 외/손상 상태면 10전략 × 200만 원 신규 시드
    - Gist가 설정돼 있는데 원격 로드에 실패한 경우에는 **절대 마이그레이션/원격 저장을 하지 않는다**
      (낡은 로컬 state.json으로 라이브 전적을 덮어쓰는 사고 방지). 엔진이 5초 간격으로 재시도한다.
    """
    state = load_state(force_remote=True)
    if gist_enabled() and last_load_source() != "gist":
        note = f"Gist 로드 실패(소스={last_load_source()}) — 승계 대기, 원격 저장 보류"
        if _ensure_guard["last_note"] != note:
            _ensure_guard["last_note"] = note
            print(f"[Engine] {note}")
        return state

    version = state.get("version")
    if version == VERSION and isinstance(state.get("strategies"), dict):
        missing = [k for k in STRATEGY_KEYS if k not in state["strategies"]]
        for k in missing:
            state["strategies"][k] = new_strategy(k)
        if missing:
            print(f"[Engine] Added missing strategies: {missing}")
        if state.get("rules") != RULES_V4:
            state["rules"] = RULES_V4
        for k in STRATEGY_KEYS:     # 규칙 텍스트/파라미터는 코드 정의를 따른다
            state["strategies"][k]["entry"] = STRATEGIES[k]["entry"]
            state["strategies"][k]["exit"] = STRATEGIES[k]["exit"]
            state["strategies"][k]["btc_filter"] = bool(STRATEGIES[k]["btc_filter"])
            state["strategies"][k].setdefault("params", dict(STRATEGIES[k]["params"]))
        state["strategy_order"] = list(STRATEGY_KEYS)
        _alias_legacy(state)
        _aggregate(state, None)
        if missing:
            save_state(state, remote=True, force_remote=True)
        return state

    if isinstance(state.get("tiers"), dict) and str(version or "").startswith("3"):
        new = migrate_from_v3(state)
    else:
        print(f"[Engine] State reset from v{version} -> v{VERSION} "
              f"({len(STRATEGY_KEYS)} strategies x {DEFAULT_SEED:,} KRW)")
        new = new_league_state()
    _aggregate(new, None)
    save_state(new, remote=True, force_remote=True)
    return new


# ---------------------------------------------------------------- 지표 계산
def _position_price(pos, stream):
    price = stream.last_price(pos["market"]) if stream else None
    if price is None:
        price = _rest_price(pos["market"])
    return price


def _strategy_equity(st, stream):
    cash = float(st.get("cash_krw") or 0.0)
    pos = st.get("position")
    if not pos:
        return cash
    price = _position_price(pos, stream) or pos.get("entry_price", 0)
    return cash + float(pos.get("coin_qty", 0.0)) * float(price)


def _close_position(st, price, reason, now, exit_kind):
    pos = st["position"]
    p = st.get("params") or {}
    entry_price = float(pos["entry_price"])
    qty = float(pos["coin_qty"])
    net = qty * price * (1 - FEE_RATE)
    pnl = net - float(pos["cost_krw"])
    gain_pct = (price - entry_price) / entry_price * 100.0

    st["cash_krw"] = net
    st["total_realized_pnl"] = float(st.get("total_realized_pnl", 0.0)) + pnl
    st["total_trades"] = int(st.get("total_trades", 0)) + 1
    if pnl > 0:
        st["wins"] = int(st.get("wins", 0)) + 1
    else:
        st["losses"] = int(st.get("losses", 0)) + 1
        if exit_kind == "STOP_LOSS":
            mins = p.get("cooldown_min", 5)
            st["cooldown_until"] = fmt(now + timedelta(minutes=mins))

    st["recent_trades"].append({
        "type": "SELL",
        "strategy": st["key"],
        "strategy_label": st["label"],
        "strategy_name": st["name"],
        "tier": st["key"] if st["key"] in LEGACY_KEYS else None,
        "market": pos["market"],
        "name": pos.get("name", pos["market"]),
        "entry_time": pos.get("entry_time"),
        "exit_time": fmt(now),
        "entry_price": entry_price,
        "exit_price": round(price, 8),
        "gain_pct": round(gain_pct, 2),
        "pnl_krw": round(pnl),
        "hold_min": round((_now_ms() - int(pos.get("entry_ts_ms") or _now_ms())) / 60000.0, 1),
        "reason": reason,
        "exit_kind": exit_kind,
    })
    st["position"] = None
    st["status"] = "WAITING_FOR_SIGNAL"
    return pnl, gain_pct


def _check_exit(st, stream, now):
    """보유 포지션 틱 단위 청산 판정. 반환: (exit_price, reason, exit_kind) 또는 None"""
    pos = st["position"]
    p = st.get("params") or {}
    price = _position_price(pos, stream)
    if not price:
        return None
    entry = float(pos["entry_price"])

    high = max(float(pos.get("highest_price") or entry), price)
    pos["highest_price"] = high

    gain = (price - entry) / entry * 100.0
    pos["last_price"] = price
    pos["unrealized_pct"] = round(gain, 2)
    high_gain = (high - entry) / entry * 100.0
    held_min = (_now_ms() - int(pos.get("entry_ts_ms") or _now_ms())) / 60000.0

    stop_price = entry * (1 + p["stop_pct"] / 100.0)
    target_price = entry * (1 + p["target_pct"] / 100.0)

    # 1) 기계적 손절 (관측 가격 체결)
    if price <= stop_price:
        return price, (f"🛑 기계적 손절 발동 ({gain:.2f}%) · "
                       f"{p.get('cooldown_min', 5)}분 쿨다운"), "STOP_LOSS"

    # 2) 목표 익절 (목표가 체결, 보수적)
    if price >= target_price:
        return target_price, f"🎯 목표 익절 달성 (+{p['target_pct']:.2f}%)", "TARGET"

    # 3) 트레일링 스탑 (설정된 전략만)
    if p.get("trail_activation_pct") is not None and high_gain >= p["trail_activation_pct"]:
        drop = (price - high) / high * 100.0
        if drop <= -p["trail_drop_pct"]:
            return price, (f"🛡️ 트레일링 스탑 청산 ({gain:+.2f}%, 고점 {high_gain:+.2f}% "
                           f"대비 {drop:.2f}%)"), "TRAILING"

    # 4) 최대 보유 시간
    if held_min >= p["max_hold_min"]:
        return price, f"⏱️ {p['max_hold_min']}분 보유 제한 초과 청산 ({gain:+.2f}%)", "TIME"

    return None


# ---------------------------------------------------------------- 진입 신호
def _sig_surge(snap, st, ctx):
    """v3.1 계승: 15초 거래대금 폭발 + 매수 우위 + 초입 구간."""
    p = st["params"]
    if snap["coverage_sec"] < 60:
        return False, f"관측 {snap['coverage_sec']:.0f}s < 60s", 0.0
    if snap["base_krw"] <= 0 or snap["base_ticks"] < 3:
        return False, f"기준 표본 부족({snap['base_ticks']}틱)", 0.0
    if snap["short_krw"] < float(p["min_short_krw"]):
        return False, (f"15초 거래대금 {snap['short_krw']/1e4:,.0f}만 < "
                       f"{p['min_short_krw']/1e4:,.0f}만"), snap["surge_ratio"]
    if snap["surge_ratio"] < p["min_surge_ratio"]:
        return False, f"폭발배수 {snap['surge_ratio']:.1f}x < {p['min_surge_ratio']:.1f}x", snap["surge_ratio"]
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", snap["surge_ratio"]
    if snap["chg_3m_pct"] < p["min_chg_pct"]:
        return False, f"3분변동 {snap['chg_3m_pct']:+.2f}% < +{p['min_chg_pct']}%", snap["surge_ratio"]
    if snap["chg_3m_pct"] > p["max_chg_pct"]:
        return False, f"3분변동 {snap['chg_3m_pct']:+.2f}% > +{p['max_chg_pct']}% (뒷북)", snap["surge_ratio"]
    return True, (f"15초 {snap['short_krw']/1e4:,.0f}만 · {snap['surge_ratio']:.1f}배 · "
                  f"매수 {snap['bid_ratio']*100:.0f}% · 3분 {snap['chg_3m_pct']:+.2f}%"), \
        snap["surge_ratio"] * 1e6 + snap["short_krw"] / 1e4


def _sig_panic_bounce(snap, st, ctx):
    """3분 -2.5% 급락 후 매수 우위 + 가격 반등."""
    p = st["params"]
    if snap["coverage_sec"] < 60:
        return False, f"관측 {snap['coverage_sec']:.0f}s < 60s", 0.0
    if snap["chg_3m_pct"] > p["min_drop_pct"]:
        return False, f"3분변동 {snap['chg_3m_pct']:+.2f}% > {p['min_drop_pct']}% (급락 아님)", -snap["chg_3m_pct"]
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", 0.0
    if snap["price_15s_ago"] is None or snap["last_price"] <= snap["price_15s_ago"]:
        return False, "직전 15초 대비 반등 미확인", 0.0
    reb = (snap["last_price"] - snap["price_15s_ago"]) / snap["price_15s_ago"] * 100.0
    return True, (f"3분 {snap['chg_3m_pct']:+.2f}% 급락 후 15초 반등 {reb:+.2f}% · "
                  f"매수 {snap['bid_ratio']*100:.0f}%"), reb


def _sig_pullback(snap, st, ctx):
    """3~10분 고점 대비 -1.0~-2.2% 눌림 + 매수 우위 + 상승 전환."""
    p = st["params"]
    if snap["coverage_10m_sec"] < p["min_coverage_10m_sec"]:
        return False, f"10분 고점 관측 {snap['coverage_10m_sec']:.0f}s < {p['min_coverage_10m_sec']}s", 0.0
    dd = snap.get("drawdown_from_high_pct")
    if dd is None:
        return False, "고점 표본 없음", 0.0
    if dd > p["dd_max_pct"]:
        return False, f"고점 대비 {dd:+.2f}% (눌림 부족, 기준 -1.0%↓)", -dd
    if dd < p["dd_min_pct"]:
        return False, f"고점 대비 {dd:+.2f}% (눌림 과다, 기준 -2.2%↑)", -dd
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", 0.0
    if snap["price_15s_ago"] is None or snap["last_price"] <= snap["price_15s_ago"]:
        return False, "상승 전환 미확인", 0.0
    return True, (f"고점 대비 {dd:+.2f}% 눌림 · 매수 {snap['bid_ratio']*100:.0f}% · "
                  f"15초 반등 확인"), snap["short_krw"] / 1e4


def _sig_vwap(snap, st, ctx):
    """직전 3분 VWAP 하단 → 거래대금 2.5배 + 매수 우위로 VWAP +0.3% 돌파."""
    p = st["params"]
    if snap["coverage_sec"] < p["min_coverage_sec"]:
        return False, f"관측 {snap['coverage_sec']:.0f}s < {p['min_coverage_sec']}s", 0.0
    vwap = snap.get("vwap_3m")
    if not vwap:
        return False, "VWAP 표본 없음", 0.0
    bo = (snap["last_price"] - vwap) / vwap * 100.0
    if bo < p["breakout_pct"]:
        return False, f"VWAP 대비 {bo:+.2f}% < +{p['breakout_pct']}%", bo
    ago = snap.get("price_30s_ago")
    if ago is None or ago > vwap:
        return False, "직전 VWAP 하단 이력 없음 (돌파 아님)", bo
    if snap["surge_ratio"] < p["min_vol_ratio"]:
        return False, f"거래량 {snap['surge_ratio']:.1f}배 < {p['min_vol_ratio']:.1f}배", bo
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", bo
    return True, (f"VWAP {vwap:,.4f} → {bo:+.2f}% 돌파 · 거래량 {snap['surge_ratio']:.1f}배 · "
                  f"매수 {snap['bid_ratio']*100:.0f}%"), bo


def _sig_streak(snap, st, ctx):
    """10초 연속 매수 체결 + 체결 건수 + 15초 거래대금."""
    p = st["params"]
    if snap["coverage_sec"] < p["min_coverage_sec"]:
        return False, f"관측 {snap['coverage_sec']:.0f}s < {p['min_coverage_sec']}s", 0.0
    score = float(snap["ticks_10s"])
    if snap["ticks_10s"] < p["min_ticks_10s"]:
        return False, f"10초 체결 {snap['ticks_10s']}건 < {p['min_ticks_10s']}건", score
    if snap["bid_ratio_10s"] < p["min_bid_10s"]:
        return False, f"10초 매수비중 {snap['bid_ratio_10s']*100:.0f}% < {p['min_bid_10s']*100:.0f}%", score
    if snap["short_krw"] < p["min_short_krw"]:
        return False, (f"15초 거래대금 {snap['short_krw']/1e4:,.0f}만 < "
                       f"{p['min_short_krw']/1e4:,.0f}만"), score
    return True, (f"10초 {snap['ticks_10s']}건 · 매수 {snap['bid_ratio_10s']*100:.0f}% · "
                  f"15초 {snap['short_krw']/1e4:,.0f}만"), score + snap["short_krw"] / 1e5


def _sig_major(snap, st, ctx):
    """24h 거래대금 TOP10 + 15초 폭발 + 매수 우위 + 3분 상승."""
    p = st["params"]
    if snap["market"] not in ctx["top_markets"]:
        return False, "24h 거래대금 TOP10 아님", 0.0
    if snap["coverage_sec"] < 60 or snap["base_ticks"] < 3:
        return False, "기준 표본 부족", 0.0
    if snap["surge_ratio"] < p["min_surge_ratio"]:
        return False, f"폭발배수 {snap['surge_ratio']:.1f}x < {p['min_surge_ratio']:.1f}x", snap["surge_ratio"]
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", snap["surge_ratio"]
    if snap["chg_3m_pct"] < p["min_chg_pct"]:
        return False, f"3분변동 {snap['chg_3m_pct']:+.2f}% < +{p['min_chg_pct']}%", snap["surge_ratio"]
    if snap["chg_3m_pct"] > p["max_chg_pct"]:
        return False, f"3분변동 {snap['chg_3m_pct']:+.2f}% > +{p['max_chg_pct']}% (뒷북)", snap["surge_ratio"]
    return True, (f"TOP10 · {snap['surge_ratio']:.1f}배 · 매수 {snap['bid_ratio']*100:.0f}% · "
                  f"3분 {snap['chg_3m_pct']:+.2f}%"), snap["surge_ratio"] * 1e6 + snap["short_krw"] / 1e4


def _sig_rsi(snap, st, ctx):
    """1분봉 RSI(14) 25 이하 과매도 → 상향 반전 + 매수 우위."""
    p = st["params"]
    c = snap.get("candles")
    if not c:
        return False, "1분봉 캐시 대기 (거래대금 상위 15종목만 수집)", 0.0
    if c.get("age_sec", 999) > p["max_candle_age_sec"]:
        return False, f"1분봉 캐시 {c['age_sec']:.0f}s 지연", 0.0
    rsi_now, rsi_prev = c.get("rsi"), c.get("rsi_prev")
    if rsi_now is None or rsi_prev is None:
        return False, "RSI 표본 부족", 0.0
    score = p["rsi_threshold"] - rsi_prev
    if rsi_prev > p["rsi_threshold"]:
        return False, f"RSI {rsi_prev:.1f} > {p['rsi_threshold']:.0f} (과매도 아님)", score
    if rsi_now <= rsi_prev:
        return False, f"RSI 반전 미확인 ({rsi_prev:.1f}→{rsi_now:.1f})", score
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", score
    return True, (f"RSI {rsi_prev:.1f}→{rsi_now:.1f} 반전 · 매수 {snap['bid_ratio']*100:.0f}%"), score


def _sig_bb(snap, st, ctx):
    """1분봉 볼린저(20,2) 수축 후 상단 밴드 거래량 2.0배 돌파."""
    p = st["params"]
    c = snap.get("candles")
    if not c:
        return False, "1분봉 캐시 대기 (거래대금 상위 15종목만 수집)", 0.0
    if c.get("age_sec", 999) > p["max_candle_age_sec"]:
        return False, f"1분봉 캐시 {c['age_sec']:.0f}s 지연", 0.0
    up, rank, bw = c.get("bb_upper"), c.get("bw_rank"), c.get("bandwidth_pct")
    if up is None or rank is None:
        return False, "볼린저 표본 부족", 0.0
    score = snap["surge_ratio"]
    if rank > p["squeeze_rank"]:
        return False, (f"밴드 수축 아님 (폭 {bw:.2f}% · 최근 40봉 백분위 {rank*100:.0f}% > "
                       f"{p['squeeze_rank']*100:.0f}%)"), score
    if snap["last_price"] <= up:
        return False, f"상단 밴드 미돌파 ({snap['last_price']:,.4f} ≤ {up:,.4f})", score
    vol_ratio = c.get("vol_ratio")
    if snap["surge_ratio"] < p["min_vol_ratio"] and (vol_ratio or 0) < p["min_vol_ratio"]:
        return False, f"돌파 거래량 미달 ({snap['surge_ratio']:.1f}배)", score
    return True, (f"수축(백분위 {rank*100:.0f}%) 후 {up:,.4f} 돌파 · "
                  f"15초 거래대금 {snap['surge_ratio']:.1f}배"), score


SIGNAL_FNS = {
    "surge": _sig_surge,
    "panic_bounce": _sig_panic_bounce,
    "pullback": _sig_pullback,
    "vwap": _sig_vwap,
    "streak": _sig_streak,
    "major": _sig_major,
    "rsi": _sig_rsi,
    "bb": _sig_bb,
}


def _attach_candles(snap):
    if _candle_cache is not None:
        snap["candles"] = _candle_cache.snapshot(snap["market"])
    else:
        snap["candles"] = None
    return snap


def _evaluate_strategy(key, snaps, ctx):
    """전략 하나를 전체 시장 스냅샷에 적용. (최적 진입 후보, 최적 근접 후보) 반환."""
    st = ctx["state"]["strategies"][key]
    fn = SIGNAL_FNS[STRATEGIES[key]["kind"]]
    best_ok = best_any = None
    for snap in snaps:
        ok, why, score = fn(snap, st, ctx)
        if ok:
            if best_ok is None or score > best_ok[0]:
                best_ok = (score, snap, why)
        elif best_any is None or score > best_any[0]:
            best_any = (score, snap, why)
    return best_ok, best_any


def _enter(st, snap, now, stream, why):
    market = snap["market"]
    name = (stream.names().get(market) if stream else None) or market.replace("KRW-", "")
    invest = float(st["cash_krw"])
    if invest <= 0:
        return None
    price = float(snap["last_price"])
    if price <= 0:
        return None
    qty = (invest * (1 - FEE_RATE)) / price
    st["position"] = {
        "market": market,
        "name": name,
        "entry_price": price,
        "highest_price": price,
        "coin_qty": qty,
        "cost_krw": invest,
        "entry_time": fmt(now),
        "entry_ts_ms": _now_ms(),
        "entry_type": f"WS_{st['kind'].upper()}",
        "strategy": st["key"],
        "strategy_label": st["label"],
        "signal": why,
        "surge_ratio": round(snap["surge_ratio"], 2),
        "bid_ratio": round(snap["bid_ratio"], 3),
        "chg_3m_pct": round(snap["chg_3m_pct"], 2),
        "short_krw": round(snap["short_krw"]),
        "last_price": price,
        "unrealized_pct": 0.0,
    }
    st["cash_krw"] = 0.0
    st["status"] = "IN_POSITION"
    st["recent_trades"].append({
        "type": "BUY",
        "strategy": st["key"],
        "strategy_label": st["label"],
        "strategy_name": st["name"],
        "tier": st["key"] if st["key"] in LEGACY_KEYS else None,
        "market": market,
        "name": name,
        "entry_time": fmt(now),
        "entry_price": price,
        "cost_krw": round(invest),
        "surge_ratio": round(snap["surge_ratio"], 2),
        "bid_ratio": round(snap["bid_ratio"], 3),
        "short_krw": round(snap["short_krw"]),
        "chg_3m_pct": round(snap["chg_3m_pct"], 2),
        "reason": f"⚡ {why}",
    })
    return market


def _watch_line(st, best_any, snap_count):
    if not snap_count:
        return "🔍 웹소켓 틱 수신 대기", None
    if not best_any:
        return "🔍 신호 대기", None
    _, snap, why = best_any
    name = snap["market"].replace("KRW-", "")
    return f"🔍 {name} — {why}", snap


def _aggregate(state, stream):
    """10전략 합산 + 수익률 리더보드."""
    total_equity = total_capital = cash = pnl = 0.0
    trades = wins = losses = 0
    merged = []
    in_position = cooling = 0
    board = []

    for key in STRATEGY_KEYS:
        st = state["strategies"][key]
        equity = _strategy_equity(st, stream)
        capital = float(st.get("initial_capital") or DEFAULT_SEED)
        st["current_equity"] = round(equity)
        st["current_ret_pct"] = round(((equity - capital) / capital) * 100, 2) if capital else 0.0
        st["peak_equity"] = round(max(float(st.get("peak_equity") or capital), equity))
        dd = ((st["peak_equity"] - equity) / st["peak_equity"] * 100) if st["peak_equity"] else 0.0
        st["max_drawdown_pct"] = round(max(float(st.get("max_drawdown_pct") or 0.0), dd), 2)
        st["initial_capital"] = capital
        st["entry"] = STRATEGIES[key]["entry"]
        st["exit"] = STRATEGIES[key]["exit"]
        st["btc_filter"] = bool(STRATEGIES[key]["btc_filter"])
        if len(st.get("recent_trades", [])) > 40:
            st["recent_trades"] = st["recent_trades"][-40:]

        total_equity += equity
        total_capital += capital
        cash += float(st.get("cash_krw") or 0.0)
        pnl += float(st.get("total_realized_pnl") or 0.0)
        trades += int(st.get("total_trades") or 0)
        wins += int(st.get("wins") or 0)
        losses += int(st.get("losses") or 0)
        if st.get("position"):
            in_position += 1
        elif st.get("cooldown_until"):
            cooling += 1
        merged.extend(st.get("recent_trades", []))
        tr = int(st.get("total_trades") or 0)
        board.append({
            "key": key,
            "label": st["label"],
            "name": st["name"],
            "style": st["style"],
            "carried_over": bool(st.get("carried_over")),
            "equity": round(equity),
            "ret_pct": st["current_ret_pct"],
            "trades": tr,
            "wins": int(st.get("wins") or 0),
            "losses": int(st.get("losses") or 0),
            "win_rate_pct": round((int(st.get("wins") or 0) / tr) * 100, 1) if tr else 0.0,
            "realized_pnl": round(float(st.get("total_realized_pnl") or 0.0)),
            "status": st.get("status"),
            "in_position": bool(st.get("position")),
            "position_market": (st.get("position") or {}).get("market"),
            "position_name": (st.get("position") or {}).get("name"),
            "unrealized_pct": (st.get("position") or {}).get("unrealized_pct"),
            "last_event": st.get("last_event"),
        })

    board.sort(key=lambda r: (-r["ret_pct"], -r["equity"], r["key"]))
    for i, row in enumerate(board, 1):
        row["rank"] = i
    state["leaderboard"] = board

    merged.sort(key=lambda t: (t.get("exit_time") or t.get("entry_time") or ""))
    state["recent_trades"] = merged[-120:]

    state["current_equity"] = round(total_equity)
    state["initial_capital"] = round(total_capital)
    state["current_ret_pct"] = round(((total_equity - total_capital) / total_capital) * 100, 2) \
        if total_capital else 0.0
    state["cash_krw"] = round(cash, 2)
    state["total_realized_pnl"] = round(pnl, 2)
    state["total_trades"] = trades
    state["wins"] = wins
    state["losses"] = losses
    state["position"] = None
    state["peak_equity"] = round(max(float(state.get("peak_equity") or total_capital), total_equity))
    dd = ((state["peak_equity"] - total_equity) / state["peak_equity"] * 100) \
        if state["peak_equity"] else 0.0
    state["max_drawdown_pct"] = round(max(float(state.get("max_drawdown_pct") or 0.0), dd), 2)
    if in_position:
        state["status"] = "IN_POSITION"
    elif cooling:
        state["status"] = "COOLDOWN"
    else:
        state["status"] = "WAITING_FOR_SIGNAL"
    state["cooldown_until"] = max(
        [state["strategies"][k]["cooldown_until"] for k in STRATEGY_KEYS
         if state["strategies"][k].get("cooldown_until")] or [None], key=lambda x: x or "")
    state["active_positions"] = in_position
    state["leader"] = board[0]["key"] if board else None
    state["laggard"] = board[-1]["key"] if board else None
    _alias_legacy(state)
    return state


def run_sniper_tick(stream=None):
    state = load_state()

    # ---- 안전장치: Gist가 설정된 환경에서는 원격 상태를 실제로 읽었을 때만 매매/저장한다 ----
    # (원격 로드 실패 → 로컬 폴백 상태로 체결하면 낡은 데이터로 라이브 전적을 덮어쓸 수 있다)
    if gist_enabled() and last_load_source() != "gist":
        now_ts = time.time()
        if (now_ts - _ensure_guard["ts"]) >= 5.0:
            _ensure_guard["ts"] = now_ts
            state = ensure_v4_state()
        if gist_enabled() and last_load_source() != "gist":
            return {"status": "STATE_SYNC_WAIT",
                    "event": "Gist 원격 상태 복구 대기 — 매매·저장 중단 (로컬 폴백 상태 사용 금지)",
                    "equity": state.get("current_equity"),
                    "ret_pct": state.get("current_ret_pct"),
                    "trades": state.get("total_trades")}

    if state.get("version") != VERSION or not isinstance(state.get("strategies"), dict):
        now_ts = time.time()
        if (now_ts - _ensure_guard["ts"]) >= 5.0:
            _ensure_guard["ts"] = now_ts
            state = ensure_v4_state()
        if state.get("version") != VERSION or not isinstance(state.get("strategies"), dict):
            return {"status": "MIGRATION_WAIT", "event": "v4.0 승계 대기 (Gist 로드 재시도 중)",
                    "equity": state.get("current_equity"), "ret_pct": state.get("current_ret_pct"),
                    "trades": state.get("total_trades")}

    rules = state.get("rules") or RULES_V4
    if rules != RULES_V4:
        state["rules"] = RULES_V4
    now = kst_now()
    now_ms = _now_ms()
    state["last_checked_time"] = fmt(now)
    events = []
    exited_now = set()

    # ---------- 1. 전략별 보유 포지션 청산 감시 ----------
    for key in STRATEGY_KEYS:
        st = state["strategies"][key]
        if not st.get("position"):
            continue
        hit = _check_exit(st, stream, now)
        if hit:
            price, reason, kind = hit
            pnl, gain = _close_position(st, price, reason, now, kind)
            st["last_event"] = f"SELL {st['recent_trades'][-1]['market']} {gain:+.2f}% / {pnl:+,.0f}원"
            events.append(f"[{st['label']}] {st['last_event']}")
            exited_now.add(key)
        else:
            pos = st["position"]
            cur = _position_price(pos, stream)
            gain = ((cur - pos["entry_price"]) / pos["entry_price"] * 100.0) if cur else 0.0
            pos["unrealized_pct"] = round(gain, 2)
            st["last_event"] = f"HOLDING {pos.get('name')}({pos['market']}) {gain:+.2f}%"

    # ---------- 2. 공유 시장 스냅샷 (10전략이 같은 데이터를 동시에 판단) ----------
    btc = _btc_trend_cached()
    state["btc_trend"] = btc
    bearish = bool(RULES_V4["btc_filter"]["enabled"] and btc.get("is_bearish"))

    snaps = []
    if stream is not None:
        for market in stream.markets():
            snap = stream.market_snapshot(market, now_ms)
            if snap:
                snaps.append(_attach_candles(snap))

    top_markets = set(stream.top_markets(10)) if (stream is not None and hasattr(stream, "top_markets")) else set()
    ctx = {"state": state, "top_markets": top_markets, "btc": btc, "bearish": bearish}

    # ---------- 3. 전략별 신규 진입 ----------
    for key in STRATEGY_KEYS:
        st = state["strategies"][key]
        if st.get("position") or key in exited_now:
            continue

        if st.get("cooldown_until"):
            try:
                cd = datetime.strptime(st["cooldown_until"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
                if now < cd:
                    rem = int((cd - now).total_seconds())
                    st["status"] = "COOLDOWN"
                    st["last_event"] = f"⏳ 손절 쿨다운 {rem // 60}분 {rem % 60}초 남음"
                    continue
                st["cooldown_until"] = None
            except Exception:
                st["cooldown_until"] = None

        exempt = not bool(STRATEGIES[key]["btc_filter"])
        if bearish and not exempt:
            st["status"] = "BTC_FILTER_BLOCKED"
            st["last_event"] = (f"🛑 BTC 15분봉 20이평 하회 ({btc.get('diff_pct', 0):+.2f}%) "
                                f"— 신규 진입 차단")
            continue

        st["status"] = "WAITING_FOR_SIGNAL"
        if stream is None:
            continue

        best_ok, best_any = _evaluate_strategy(key, snaps, ctx)
        if best_ok:
            _, snap, why = best_ok
            market = _enter(st, snap, now, stream, why)
            if market:
                st["last_event"] = f"⚡ BUY {market} — {why}"
                events.append(f"[{st['label']}] {st['last_event']}")
                continue
        line, watch = _watch_line(st, best_any, len(snaps))
        if bearish and exempt:
            line = (f"♻️ 역추세 예외 — BTC 하락({btc.get('diff_pct', 0):+.2f}%)에도 진입 허용 · "
                    f"{line}")
        st["last_event"] = line
        st["watch"] = {
            "market": watch["market"],
            "name": (stream.names().get(watch["market"]) or watch["market"]).replace("KRW-", ""),
            "short_krw": round(watch["short_krw"]),
            "surge_ratio": round(watch["surge_ratio"], 2),
            "bid_ratio": round(watch["bid_ratio"], 3),
            "reason": best_any[2] if best_any else None,
        } if watch else None

    # ---------- 4. 실시간 감시 패널 (스냅샷 재사용) ----------
    solid = [s for s in snaps if s["base_krw"] > 0 and s["base_ticks"] >= 3]
    display_pool = [s for s in solid if s["short_krw"] >= 3_000_000] or \
        sorted(solid, key=lambda x: x["short_krw"], reverse=True)[:6]
    top = sorted(display_pool, key=lambda x: x["surge_ratio"], reverse=True)[:6]
    names = stream.names() if stream else {}
    scan_rows = []
    for s in top:
        qualified = [k for k in STRATEGY_KEYS
                     if SIGNAL_FNS[STRATEGIES[k]["kind"]](s, state["strategies"][k], ctx)[0]]
        scan_rows.append({
            "market": s["market"],
            "name": (names.get(s["market"]) or s["market"]).replace("KRW-", ""),
            "price": s["last_price"],
            "short_krw": round(s["short_krw"]),
            "surge_ratio": round(s["surge_ratio"], 2),
            "bid_ratio": round(s["bid_ratio"], 3),
            "chg_3m_pct": round(s["chg_3m_pct"], 2),
            "drawdown_from_high_pct": round(s["drawdown_from_high_pct"], 2)
            if s.get("drawdown_from_high_pct") is not None else None,
            "qualified": bool(qualified),
            "qualified_strategies": qualified,
            "qualified_tiers": [k for k in qualified if k in LEGACY_KEYS],
        })
    state["live_scan"] = {
        "scanned": len(snaps),
        "qualified": len([r for r in scan_rows if r["qualified"]]),
        "top_markets": sorted(top_markets)[:10],
        "top": scan_rows,
    }

    # ---------- 5. 보유 종목은 구독 유지 ----------
    if stream is not None and hasattr(stream, "add_extra_markets"):
        try:
            stream.add_extra_markets([state["strategies"][k]["position"]["market"]
                                      for k in STRATEGY_KEYS if state["strategies"][k].get("position")])
        except Exception as e:
            print(f"[Engine] extra markets update failed: {e}")

    # ---------- 6. 집계 ----------
    state = _aggregate(state, stream)
    if events:
        state["last_event"] = " | ".join(events)
    else:
        lead = state["leaderboard"][0] if state.get("leaderboard") else None
        state["last_event"] = (
            f"10전략 대기 · 선두 {lead['name']}({lead['ret_pct']:+.2f}%)" if lead
            else "10전략 대기")

    trade_event = any(("BUY" in e or "SELL" in e) for e in events)
    save_state(state, remote=True, force_remote=trade_event)

    return {
        "status": state["status"],
        "event": state["last_event"],
        "equity": state["current_equity"],
        "ret_pct": state["current_ret_pct"],
        "trades": state["total_trades"],
        "wins": state["wins"],
        "losses": state["losses"],
        "btc_trend": state.get("btc_trend"),
        "leader": state.get("leader"),
        "leaderboard": [{"rank": r["rank"], "key": r["key"], "ret_pct": r["ret_pct"]}
                        for r in state.get("leaderboard", [])],
    }


if __name__ == "__main__":
    from candles import CandleCache
    from ws_stream import UpbitTradeStream
    st = UpbitTradeStream()
    st.start()
    cache = CandleCache(lambda: st.markets(), top_n=15)
    cache.start()
    set_candle_cache(cache)
    secs = float(os.environ.get("V4_TEST_SECONDS", "25"))
    t_end = time.time() + secs
    while time.time() < t_end:
        print(json.dumps(run_sniper_tick(st), ensure_ascii=False, default=str))
        time.sleep(TICK_INTERVAL_SEC)
    print("WS STATS:", json.dumps(st.stats(), ensure_ascii=False))
    print("CANDLES:", json.dumps(cache.stats(), ensure_ascii=False))
    cache.stop()
    st.stop()
