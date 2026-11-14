"""
스나이퍼 봇 v3.1 — 업비트 실시간 WebSocket 체결 스트림 기반 초단타 엔진 · 대금별 3계좌 병렬.

v3.0(단일 계좌)을 확장했다. 웹소켓 연결과 BTC 추세 체크는 1개만 공유하고,
1.5초 판단 루프에서 **15초 거래대금 문턱이 다른 3개의 독립 가상 계좌**를 동시에 운용한다.

  · 30m — 3,000만원↑ (보수형 / 대형 슈팅)
  · 10m — 1,000만원↑ (균형형 / 중형 슈팅)
  · 3m  —   300만원↑ (공격형 / 초민감 스캘핑)

각 계좌는 독립 cash_krw / position / wins / losses / total_realized_pnl /
cooldown_until / recent_trades 를 가지며, 한 계좌의 체결·손절 쿨다운이 다른 계좌에 영향을 주지 않는다.

진입 (모든 계좌 공통, 금액 문턱만 다름):
  1) BTC 15분봉 20이평 대비 -0.2% 초과 하회 시 진입 차단
  2) 최근 15초 거래대금 >= 직전 3분 평균 15초 거래대금 x 4.0
  3) 최근 15초 거래대금 >= 계좌별 문턱 (3,000만 / 1,000만 / 300만)
  4) 최근 15초 매수 체결(BID) 금액 비율 >= 70%
  5) 3분 전 가격 대비 현재가 +0.3% ~ +3.0% (초입 구간만)

청산 (1.5초 단위 틱 감시, 계좌별 독립):
  목표 +2.5% / 트레일링(+1.5% 도달 후 고점 대비 -0.4% 반락) / 손절 -1.8%(5분 쿨다운) / 20분 시간 초과
"""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from persistence import load_state, save_state, sync_status  # noqa: E402
from scanner import check_btc_trend, fetch_tickers  # noqa: E402

VERSION = "3.1"
FEE_RATE = 0.0005          # 업비트 수수료 0.05%
TICK_INTERVAL_SEC = 1.5    # 엔진 판단 주기
KST = timezone(timedelta(hours=9))

# 대금별 3계좌 정의 — 15초 거래대금 문턱만 다르고 나머지 규칙은 동일
TIER_KEYS = ["30m", "10m", "3m"]
TIERS = {
    "30m": {"key": "30m", "label": "3,000만↑", "style": "보수형 · 대형 슈팅", "min_short_krw": 30_000_000},
    "10m": {"key": "10m", "label": "1,000만↑", "style": "균형형 · 중형 슈팅", "min_short_krw": 10_000_000},
    "3m":  {"key": "3m",  "label": "300만↑",  "style": "공격형 · 초민감 스캘핑", "min_short_krw": 3_000_000},
}
DEFAULT_TIER_CAPITAL = 2_000_000

DEFAULT_RULES_V3 = {
    "engine": "Upbit WebSocket trade stream (tick-level) · 3계좌 병렬",
    "tick_interval_sec": TICK_INTERVAL_SEC,
    "short_window_sec": 15,               # 폭발 판정 창
    "base_window_sec": 180,               # 3분 평균 기준 창
    "min_surge_ratio": 4.0,               # 직전 3분 평균 15초 거래대금 대비 4.0배
    "min_short_krw": 30_000_000,          # 보수형 계좌 문턱 (하위 호환용 대표값)
    "tier_min_short_krw": {"30m": 30_000_000, "10m": 10_000_000, "3m": 3_000_000},
    "min_bid_ratio": 0.70,                # 매수 체결(BID) 비율 70% 이상
    "min_chg_pct": 0.3,                   # 3분 전 대비 +0.3% 이상
    "max_chg_pct": 3.0,                   # 3분 전 대비 +3.0% 이하 (뒷북 추격 차단)
    "min_coverage_sec": 60,               # 틱 윈도우 최소 관측 시간
    "min_base_ticks": 3,                 # 직전 3분 평균 산출용 최소 체결 표본 수
    "target_profit_pct": 2.5,
    "trailing_activation_pct": 1.5,
    "trailing_drop_pct": 0.4,
    "stop_loss_pct": -1.8,
    "max_hold_minutes": 20,
    "loss_cooldown_minutes": 5,
    "btc_filter_enabled": True,
    "btc_sma_diff_threshold_pct": -0.2,
    "gist_sync_interval_sec": 60,
    "entry_size": "계좌별 가용 현금 100%",
    "tiers": [
        {"key": "30m", "label": "3,000만↑", "style": "보수형", "min_short_krw": 30_000_000},
        {"key": "10m", "label": "1,000만↑", "style": "균형형", "min_short_krw": 10_000_000},
        {"key": "3m", "label": "300만↑", "style": "공격형", "min_short_krw": 3_000_000},
    ],
}

_btc_cache = {"ts": 0.0, "data": None}
_rest_price_cache = {}


def kst_now():
    return datetime.now(KST)


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _now_ms():
    return int(time.time() * 1000)


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


def new_tier(key, initial_capital=DEFAULT_TIER_CAPITAL, now_str=None):
    meta = TIERS[key]
    now_str = now_str or fmt(kst_now())
    return {
        "key": key,
        "label": meta["label"],
        "style": meta["style"],
        "min_short_krw": meta["min_short_krw"],
        "initialized_at": now_str,
        "initial_capital": initial_capital,
        "cash_krw": float(initial_capital),
        "position": None,
        "cooldown_until": None,
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "status": "WAITING_FOR_SURGE",
        "last_event": f"{meta['label']} 계좌 초기화 — 웹소켓 틱 스트림 대기",
        "watch": None,
        "current_equity": initial_capital,
        "current_ret_pct": 0.0,
        "peak_equity": float(initial_capital),
        "max_drawdown_pct": 0.0,
    }


def new_multi_state(tier_capital=DEFAULT_TIER_CAPITAL):
    now_str = fmt(kst_now())
    tiers = {k: new_tier(k, tier_capital, now_str) for k in TIER_KEYS}
    total_capital = tier_capital * len(TIER_KEYS)
    return {
        "version": VERSION,
        "status": "WAITING_FOR_SURGE",
        "engine": "ws_tick_v3_multi",
        "mode": "multi_tier",
        "initialized_at": now_str,
        "initial_capital": total_capital,
        "cash_krw": float(total_capital),
        "position": None,
        "cooldown_until": None,
        "btc_trend": None,
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "last_checked_time": now_str,
        "last_event": "v3.1 3계좌 병렬 엔진 초기화 — 웹소켓 틱 스트림 대기",
        "rules": DEFAULT_RULES_V3,
        "tiers": tiers,
        "current_equity": total_capital,
        "current_ret_pct": 0.0,
        "peak_equity": float(total_capital),
        "max_drawdown_pct": 0.0,
        "live_scan": {"scanned": 0, "qualified": 0, "top": []},
        "exit_rules_note": {
            "target": "목표 익절 체결가는 목표가로 계산 (틱 초과분 미반영, 보수적)",
            "stop": "손절/트레일링/시간초과는 관측 틱 가격으로 체결 (슬리피지 반영)",
        },
    }


def migrate_single_state(old, tier_capital=DEFAULT_TIER_CAPITAL):
    """v3.0 단일 계좌 상태를 3계좌 구조로 승계한다.

    기존 계좌의 잔고·포지션·전적은 3,000만↑(보수형) 슬롯에 그대로 넘기고,
    1,000만↑ / 300만↑ 슬롯은 신규 시드로 시작한다.
    """
    state = new_multi_state(tier_capital)
    if not isinstance(old, dict) or old.get("version") != "3.0":
        return state

    heir = state["tiers"]["30m"]
    for field in ("initialized_at", "cash_krw", "position", "cooldown_until",
                  "total_realized_pnl", "total_trades", "wins", "losses",
                  "recent_trades", "peak_equity", "max_drawdown_pct"):
        if field in old:
            heir[field] = old[field]
    if old.get("initial_capital"):
        heir["initial_capital"] = old["initial_capital"]
    if heir["position"]:
        heir["status"] = "IN_POSITION"
        heir["last_event"] = "v3.0 단일 계좌에서 보유 포지션 승계"
    elif heir["total_trades"]:
        heir["last_event"] = f"v3.0 단일 계좌 {heir['total_trades']}전 승계"
    else:
        heir["last_event"] = "3,000만↑ 계좌 — v3.0 기록 승계 (0전 0승 0패)"
    state["migrated_from"] = {"version": "3.0", "at": fmt(kst_now()),
                              "trades": old.get("total_trades", 0),
                              "had_position": bool(old.get("position"))}
    print(f"[Engine] Migrated v3.0 single account -> 30m slot "
          f"(trades={old.get('total_trades', 0)}, position={bool(old.get('position'))})")
    return state


def ensure_v3_state():
    """Gist/로컬 상태를 v3.1 3계좌 구조로 맞춘다.

    - 이미 v3.1이면 규칙 정의만 최신화
    - v3.0이면 3,000만↑ 슬롯으로 승계 (포지션·전적 보존)
    - 그 외/손상 상태면 3계좌 × 200만 원 신규 시드
    """
    state = load_state(force_remote=True)
    version = state.get("version")
    if version == VERSION and isinstance(state.get("tiers"), dict):
        missing = [k for k in TIER_KEYS if k not in state["tiers"]]
        if missing:
            for k in missing:
                state["tiers"][k] = new_tier(k)
            save_state(state, remote=True, force_remote=True)
        if state.get("rules") != DEFAULT_RULES_V3:
            state["rules"] = DEFAULT_RULES_V3
            save_state(state, remote=False)
        return state

    if version == "3.0":
        state = migrate_single_state(state)
    else:
        print(f"[Engine] State reset from v{version} -> v{VERSION} (3 accounts x "
              f"{DEFAULT_TIER_CAPITAL:,} KRW)")
        state = new_multi_state()
    save_state(state, remote=True, force_remote=True)
    return state


# ---------------------------------------------------------------- 지표 계산
def _position_price(pos, stream):
    price = stream.last_price(pos["market"]) if stream else None
    if price is None:
        price = _rest_price(pos["market"])
    return price


def _tier_equity(tier, stream):
    cash = float(tier.get("cash_krw") or 0.0)
    pos = tier.get("position")
    if not pos:
        return cash
    price = _position_price(pos, stream) or pos.get("entry_price", 0)
    return cash + float(pos.get("coin_qty", 0.0)) * float(price)


def _close_position(tier, rules, exit_price, reason, now, exit_kind):
    pos = tier["position"]
    entry_price = float(pos["entry_price"])
    qty = float(pos["coin_qty"])
    gross = qty * exit_price
    net = gross * (1 - FEE_RATE)
    pnl = net - float(pos["cost_krw"])
    gain_pct = (exit_price - entry_price) / entry_price * 100.0

    tier["cash_krw"] = net
    tier["total_realized_pnl"] = float(tier.get("total_realized_pnl", 0.0)) + pnl
    tier["total_trades"] = int(tier.get("total_trades", 0)) + 1
    if pnl > 0:
        tier["wins"] = int(tier.get("wins", 0)) + 1
    else:
        tier["losses"] = int(tier.get("losses", 0)) + 1
        if exit_kind == "STOP_LOSS":
            mins = rules.get("loss_cooldown_minutes", 5)
            tier["cooldown_until"] = fmt(now + timedelta(minutes=mins))

    tier["recent_trades"].append({
        "type": "SELL",
        "tier": tier["key"],
        "tier_label": tier["label"],
        "market": pos["market"],
        "name": pos.get("name", pos["market"]),
        "entry_time": pos.get("entry_time"),
        "exit_time": fmt(now),
        "entry_price": entry_price,
        "exit_price": round(exit_price, 8),
        "gain_pct": round(gain_pct, 2),
        "pnl_krw": round(pnl),
        "hold_min": round((_now_ms() - int(pos.get("entry_ts_ms") or _now_ms())) / 60000.0, 1),
        "reason": reason,
        "exit_kind": exit_kind,
    })
    tier["position"] = None
    tier["status"] = "WAITING_FOR_SURGE"
    return pnl, gain_pct


def _check_exit(tier, rules, stream, now):
    """보유 포지션 틱 단위 청산 판정. 반환: (exit_price, reason, exit_kind) 또는 None"""
    pos = tier["position"]
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
    entry_ts = int(pos.get("entry_ts_ms") or _now_ms())
    held_min = (_now_ms() - entry_ts) / 60000.0

    stop_price = entry * (1 + rules["stop_loss_pct"] / 100.0)
    target_price = entry * (1 + rules["target_profit_pct"] / 100.0)

    # 1) 기계적 손절 (관측 가격 체결)
    if price <= stop_price:
        return price, f"🛑 기계적 손절 발동 ({gain:.2f}%) · {rules['loss_cooldown_minutes']}분 쿨다운", "STOP_LOSS"

    # 2) 목표 익절 (목표가 체결, 보수적)
    if price >= target_price:
        tg = (target_price - entry) / entry * 100.0
        return target_price, f"🎯 목표 익절 달성 (+{tg:.2f}%)", "TARGET"

    # 3) 트레일링 스탑
    if high_gain >= rules["trailing_activation_pct"]:
        drop = (price - high) / high * 100.0
        if drop <= -rules["trailing_drop_pct"]:
            return price, (f"🛡️ 트레일링 스탑 청산 ({gain:+.2f}%, "
                           f"고점 {high_gain:+.2f}% 대비 {drop:.2f}%)"), "TRAILING"

    # 4) 최대 보유 시간
    if held_min >= rules["max_hold_minutes"]:
        return price, f"⏱️ {rules['max_hold_minutes']}분 보유 제한 초과 청산 ({gain:+.2f}%)", "TIME"

    return None


def _common_reject_reasons(m, rules):
    """계좌 공통 진입 조건 위반 사유 (금액 문턱 제외)."""
    reasons = []
    if m["coverage_sec"] < rules["min_coverage_sec"]:
        reasons.append(f"관측 {m['coverage_sec']:.0f}s < {rules['min_coverage_sec']}s")
    if m["base_krw"] <= 0 or m["base_ticks"] < rules["min_base_ticks"]:
        reasons.append(f"기준 표본 부족({m['base_ticks']}틱)")
    if m["surge_ratio"] < rules["min_surge_ratio"]:
        reasons.append(f"폭발배수 {m['surge_ratio']:.1f}x < {rules['min_surge_ratio']:.1f}x")
    if m["bid_ratio"] < rules["min_bid_ratio"]:
        reasons.append(f"매수비율 {m['bid_ratio']*100:.0f}% < {rules['min_bid_ratio']*100:.0f}%")
    if m["chg_3m_pct"] < rules["min_chg_pct"]:
        reasons.append(f"3분변동 {m['chg_3m_pct']:+.2f}% < +{rules['min_chg_pct']}%")
    if m["chg_3m_pct"] > rules["max_chg_pct"]:
        reasons.append(f"3분변동 {m['chg_3m_pct']:+.2f}% > +{rules['max_chg_pct']}% (뒷북)")
    return reasons


def _tier_open_ok(m, tier, rules):
    """해당 계좌가 이 종목에 진입할 수 있는지 (금액 문턱 포함)."""
    if _common_reject_reasons(m, rules):
        return False
    return m["short_krw"] >= float(tier["min_short_krw"])


def _tier_reject(m, tier, rules):
    reasons = _common_reject_reasons(m, rules)
    if m["short_krw"] < float(tier["min_short_krw"]):
        reasons.insert(0, f"15초 거래대금 {m['short_krw']/1e4:,.0f}만 < {tier['min_short_krw']/1e4:,.0f}만")
    return reasons


def _evaluate_market(stream, market, rules, now_ms):
    m = stream.market_metrics(market, now_ms, rules["short_window_sec"], rules["base_window_sec"])
    if not m:
        return None
    common = _common_reject_reasons(m, rules)
    m["common_ok"] = not common
    m["common_reasons"] = common
    tiers_ok = [k for k in TIER_KEYS if m["common_ok"] and m["short_krw"] >= float(TIERS[k]["min_short_krw"])]
    m["qualified_tiers"] = tiers_ok
    m["qualified"] = bool(tiers_ok)
    m["reject"] = "진입 조건 충족" if tiers_ok else " · ".join(
        common or [f"15초 거래대금 {m['short_krw']/1e4:,.0f}만 < 300만 (전 계좌 미달)"])
    return m


def _enter(tier, rules, stream, m, now):
    market = m["market"]
    name = (stream.names().get(market) if stream else None) or market.replace("KRW-", "")
    invest = float(tier["cash_krw"])
    if invest <= 0:
        return None
    net = invest * (1 - FEE_RATE)
    price = float(m["last_price"])
    qty = net / price
    tier["position"] = {
        "market": market,
        "name": name,
        "entry_price": price,
        "highest_price": price,
        "coin_qty": qty,
        "cost_krw": invest,
        "entry_time": fmt(now),
        "entry_ts_ms": _now_ms(),
        "entry_type": "WS_TICK_SURGE",
        "tier": tier["key"],
        "surge_ratio": round(m["surge_ratio"], 2),
        "bid_ratio": round(m["bid_ratio"], 3),
        "chg_3m_pct": round(m["chg_3m_pct"], 2),
        "short_krw": round(m["short_krw"]),
        "last_price": price,
        "unrealized_pct": 0.0,
    }
    tier["cash_krw"] = 0.0
    tier["status"] = "IN_POSITION"
    tier["recent_trades"].append({
        "type": "BUY",
        "tier": tier["key"],
        "tier_label": tier["label"],
        "market": market,
        "name": name,
        "entry_time": fmt(now),
        "entry_price": price,
        "cost_krw": round(invest),
        "surge_ratio": round(m["surge_ratio"], 2),
        "bid_ratio": round(m["bid_ratio"], 3),
        "short_krw": round(m["short_krw"]),
        "chg_3m_pct": round(m["chg_3m_pct"], 2),
        "reason": (f"⚡ 15초 거래대금 {m['short_krw']/1e4:,.0f}만원 · 직전3분 대비 "
                   f"{m['surge_ratio']:.1f}배 · 매수 {m['bid_ratio']*100:.0f}% · 3분변동 {m['chg_3m_pct']:+.2f}%"),
    })
    return market


def _watch_line(tier, evaluations):
    """대기 중인 계좌의 관찰 문구 (문턱 대비 현황)."""
    pool = [m for m in evaluations if m["last_tick_age_sec"] < 30]
    if not pool:
        return "🔍 웹소켓 틱 수신 대기", None
    common_ok = [m for m in pool if m["common_ok"]]
    if common_ok:
        best = max(common_ok, key=lambda x: (x["short_krw"], x["surge_ratio"]))
        tiers_ok = [k for k in TIER_KEYS if best["short_krw"] >= float(TIERS[k]["min_short_krw"])]
        return (f"🔍 문턱 근접 — {best['market'].replace('KRW-', '')} "
                f"15초 {best['short_krw']/1e4:,.0f}만 (문턱 {tier['min_short_krw']/1e4:,.0f}만) · "
                f"{best['surge_ratio']:.1f}배 · 매수 {best['bid_ratio']*100:.0f}%", best)
    best = max(pool, key=lambda x: x["short_krw"])
    return (f"🔍 초입 포착 대기 (최대 {best['surge_ratio']:.1f}배 · "
            f"15초 {best['short_krw']/1e4:,.0f}만 · 매수 {best['bid_ratio']*100:.0f}%)"), best


def _aggregate(state, stream):
    """3계좌 합산 지표 + 상위 호환용 단일 필드 갱신."""
    total_equity = 0.0
    total_capital = 0.0
    cash = 0.0
    pnl = 0.0
    trades = wins = losses = 0
    merged = []
    in_position = 0
    cooling = 0
    for key in TIER_KEYS:
        tier = state["tiers"][key]
        equity = _tier_equity(tier, stream)
        capital = float(tier.get("initial_capital") or DEFAULT_TIER_CAPITAL)
        tier["current_equity"] = round(equity)
        tier["current_ret_pct"] = round(((equity - capital) / capital) * 100, 2) if capital else 0.0
        tier["peak_equity"] = round(max(float(tier.get("peak_equity") or capital), equity))
        dd = ((tier["peak_equity"] - equity) / tier["peak_equity"] * 100) if tier["peak_equity"] else 0.0
        tier["max_drawdown_pct"] = round(max(float(tier.get("max_drawdown_pct") or 0.0), dd), 2)
        tier["initial_capital"] = capital
        if len(tier.get("recent_trades", [])) > 40:
            tier["recent_trades"] = tier["recent_trades"][-40:]
        total_equity += equity
        total_capital += capital
        cash += float(tier.get("cash_krw") or 0.0)
        pnl += float(tier.get("total_realized_pnl") or 0.0)
        trades += int(tier.get("total_trades") or 0)
        wins += int(tier.get("wins") or 0)
        losses += int(tier.get("losses") or 0)
        if tier.get("position"):
            in_position += 1
        elif tier.get("cooldown_until"):
            cooling += 1
        merged.extend(tier.get("recent_trades", []))

    merged.sort(key=lambda t: (t.get("exit_time") or t.get("entry_time") or ""))
    state["recent_trades"] = merged[-60:]

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
    dd = ((state["peak_equity"] - total_equity) / state["peak_equity"] * 100) if state["peak_equity"] else 0.0
    state["max_drawdown_pct"] = round(max(float(state.get("max_drawdown_pct") or 0.0), dd), 2)
    if in_position:
        state["status"] = "IN_POSITION"
    elif cooling:
        state["status"] = "COOLDOWN"
    else:
        state["status"] = "WAITING_FOR_SURGE"
    state["cooldown_until"] = max(
        [state["tiers"][k]["cooldown_until"] for k in TIER_KEYS
         if state["tiers"][k].get("cooldown_until")] or [None], key=lambda x: x or "")
    state["active_positions"] = in_position
    return state


def run_sniper_tick(stream=None):
    state = load_state()
    if state.get("version") != VERSION or not isinstance(state.get("tiers"), dict):
        state = ensure_v3_state()

    rules = state.get("rules") or DEFAULT_RULES_V3
    if state.get("rules") != DEFAULT_RULES_V3:
        state["rules"] = DEFAULT_RULES_V3
        rules = DEFAULT_RULES_V3
    now = kst_now()
    now_ms = _now_ms()
    state["last_checked_time"] = fmt(now)
    events = []
    exited_now = set()   # 같은 틱에서 청산 직후 재매수하지 않는다 (v3.0과 동일한 동작)

    # ---------- 1. 계좌별 보유 포지션 청산 감시 ----------
    for key in TIER_KEYS:
        tier = state["tiers"][key]
        if tier.get("position"):
            exit_hit = _check_exit(tier, rules, stream, now)
            if exit_hit:
                price, reason, kind = exit_hit
                pnl, gain = _close_position(tier, rules, price, reason, now, kind)
                tier["last_event"] = f"SELL {tier['recent_trades'][-1]['market']} {gain:+.2f}% / {pnl:+,.0f}원"
                events.append(f"[{tier['label']}] {tier['last_event']}")
                exited_now.add(key)
            else:
                pos = tier["position"]
                cur = _position_price(pos, stream)
                if cur:
                    pos["last_price"] = cur
                gain = ((cur - pos["entry_price"]) / pos["entry_price"] * 100.0) if cur else 0.0
                pos["unrealized_pct"] = round(gain, 2)
                tier["last_event"] = f"HOLDING {pos.get('name')}({pos['market']}) {gain:+.2f}%"

    # ---------- 2. 공유 시장 스캔 (웹소켓 1회 수신분에서 모든 계좌가 동시에 판단) ----------
    btc = _btc_trend_cached()
    state["btc_trend"] = btc
    bearish = bool(rules.get("btc_filter_enabled", True) and btc.get("is_bearish"))

    evaluations = []
    if stream is not None:
        for market in stream.markets():
            m = _evaluate_market(stream, market, rules, now_ms)
            if m:
                evaluations.append(m)

    # ---------- 3. 계좌별 신규 진입 (서로 간섭 없음) ----------
    for key in TIER_KEYS:
        tier = state["tiers"][key]
        if tier.get("position") or key in exited_now:
            continue

        if tier.get("cooldown_until"):
            try:
                cd = datetime.strptime(tier["cooldown_until"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
                if now < cd:
                    rem = int((cd - now).total_seconds())
                    tier["status"] = "COOLDOWN"
                    tier["last_event"] = f"⏳ 손절 쿨다운 {rem // 60}분 {rem % 60}초 남음"
                    continue
                tier["cooldown_until"] = None
            except Exception:
                tier["cooldown_until"] = None

        if bearish:
            tier["status"] = "BTC_FILTER_BLOCKED"
            tier["last_event"] = (f"🛑 BTC 15분봉 20이평 하회 ({btc.get('diff_pct', 0):+.2f}%) "
                                  f"— 신규 진입 차단")
            continue

        tier["status"] = "WAITING_FOR_SURGE"
        if stream is None:
            continue

        qualified = [m for m in evaluations if _tier_open_ok(m, tier, rules)]
        if qualified:
            best = max(qualified, key=lambda x: (x["surge_ratio"], x["short_krw"]))
            market = _enter(tier, rules, stream, best, now)
            if market:
                tier["last_event"] = (f"⚡ BUY {best['market']} {best['surge_ratio']:.1f}배 "
                                      f"매수 {best['bid_ratio']*100:.0f}% 3분변동 {best['chg_3m_pct']:+.2f}%")
                events.append(f"[{tier['label']}] {tier['last_event']}")
                continue
        line, watch = _watch_line(tier, evaluations)
        tier["last_event"] = line
        tier["watch"] = {
            "market": watch["market"],
            "name": (stream.names().get(watch["market"]) or watch["market"]).replace("KRW-", ""),
            "short_krw": round(watch["short_krw"]),
            "surge_ratio": round(watch["surge_ratio"], 2),
            "bid_ratio": round(watch["bid_ratio"], 3),
            "common_ok": watch["common_ok"],
        } if watch else None

    # ---------- 4. 감시 패널 (공유 스캔 결과) ----------
    solid = [m for m in evaluations if m["base_krw"] > 0 and m["base_ticks"] >= 3]
    display_pool = [m for m in solid if m["short_krw"] >= 3_000_000] or \
        sorted(solid, key=lambda x: x["short_krw"], reverse=True)[:6]
    top = sorted(display_pool, key=lambda x: x["surge_ratio"], reverse=True)[:6]
    names = stream.names() if stream else {}
    state["live_scan"] = {
        "scanned": len(evaluations),
        "qualified": len([m for m in evaluations if m["qualified"]]),
        "qualified_by_tier": {k: len([m for m in evaluations
                                      if m["common_ok"] and m["short_krw"] >= float(TIERS[k]["min_short_krw"])])
                              for k in TIER_KEYS},
        "top": [{
            "market": m["market"],
            "name": (names.get(m["market"]) or m["market"]).replace("KRW-", ""),
            "price": m["last_price"],
            "short_krw": round(m["short_krw"]),
            "surge_ratio": round(m["surge_ratio"], 2),
            "bid_ratio": round(m["bid_ratio"], 3),
            "chg_3m_pct": round(m["chg_3m_pct"], 2),
            "qualified": m["qualified"],
            "qualified_tiers": m["qualified_tiers"],
            "reject": m["reject"],
        } for m in top],
    }

    # ---------- 5. 보유 종목은 구독 유지 (상위 43종목에서 밀려나도 틱 계속 수신) ----------
    if stream is not None and hasattr(stream, "add_extra_markets"):
        try:
            stream.add_extra_markets([state["tiers"][k]["position"]["market"] for k in TIER_KEYS
                                      if state["tiers"][k].get("position")])
        except Exception as e:
            print(f"[Engine] extra markets update failed: {e}")

    # ---------- 6. 집계 ----------
    state = _aggregate(state, stream)
    if events:
        state["last_event"] = " | ".join(events)
    else:
        state["last_event"] = " · ".join(f"{state['tiers'][k]['label']}: {state['tiers'][k]['last_event']}"
                                         for k in TIER_KEYS)

    trade_event = any(e for e in events if "BUY" in e or "SELL" in e)
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
        "tiers": {k: {
            "equity": state["tiers"][k]["current_equity"],
            "ret_pct": state["tiers"][k]["current_ret_pct"],
            "status": state["tiers"][k]["status"],
            "trades": state["tiers"][k]["total_trades"],
            "event": state["tiers"][k]["last_event"],
        } for k in TIER_KEYS},
    }


if __name__ == "__main__":
    from ws_stream import UpbitTradeStream
    st = UpbitTradeStream()
    st.start()
    secs = float(os.environ.get("V3_TEST_SECONDS", "20"))
    t_end = time.time() + secs
    while time.time() < t_end:
        print(json.dumps(run_sniper_tick(st), ensure_ascii=False, default=str))
        time.sleep(TICK_INTERVAL_SEC)
    print("WS STATS:", json.dumps(st.stats(), ensure_ascii=False))
    print("GIST:", json.dumps(sync_status(), ensure_ascii=False))
    st.stop()
