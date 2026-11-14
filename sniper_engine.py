"""
스나이퍼 봇 v3.0 — 업비트 실시간 WebSocket 체결 스트림 기반 초단타 엔진.

v2.0(5분봉 60초 폴링)을 대체한다. 1.5초마다 메모리의 틱 롤링 윈도우를 계산해
초 단위로 거래대금 폭발 초입을 포착하고, 틱 단위로 청산한다.

진입 (모두 충족):
  1) BTC 15분봉 20이평 대비 -0.2% 초과 하회 시 진입 차단
  2) 최근 15초 거래대금 >= 직전 3분 평균 15초 거래대금 x 4.0
  3) 최근 15초 거래대금 >= 3,000만 원
  4) 최근 15초 매수 체결(BID) 금액 비율 >= 70%
  5) 3분 전 가격 대비 현재가 +0.3% ~ +3.0% (초입 구간만)

청산 (1.5초 단위 틱 감시):
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

VERSION = "3.0"
FEE_RATE = 0.0005          # 업비트 수수료 0.05%
TICK_INTERVAL_SEC = 1.5    # 엔진 판단 주기
KST = timezone(timedelta(hours=9))

DEFAULT_RULES_V3 = {
    "engine": "Upbit WebSocket trade stream (tick-level)",
    "tick_interval_sec": TICK_INTERVAL_SEC,
    "short_window_sec": 15,               # 폭발 판정 창
    "base_window_sec": 180,               # 3분 평균 기준 창
    "min_surge_ratio": 4.0,               # 직전 3분 평균 15초 거래대금 대비 4.0배
    "min_short_krw": 30_000_000,          # 최근 15초 거래대금 최소 3,000만 원
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
    "entry_size": "가용 현금 100%",
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


def new_v3_state(initial_capital=2000000):
    now_str = fmt(kst_now())
    return {
        "version": VERSION,
        "status": "WAITING_FOR_SURGE",
        "engine": "ws_tick_v3",
        "initialized_at": now_str,
        "initial_capital": initial_capital,
        "cash_krw": float(initial_capital),
        "position": None,
        "cooldown_until": None,
        "btc_trend": None,
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "last_checked_time": now_str,
        "last_event": "v3.0 엔진 초기화 — 웹소켓 틱 스트림 대기",
        "rules": DEFAULT_RULES_V3,
        "current_equity": initial_capital,
        "current_ret_pct": 0.0,
        "peak_equity": initial_capital,
        "max_drawdown_pct": 0.0,
        "live_scan": {"scanned": 0, "qualified": 0, "top": []},
        "exit_rules_note": {
            "target": "목표 익절 체결가는 목표가로 계산 (틱 초과분 미반영, 보수적)",
            "stop": "손절/트레일링/시간초과는 관측 틱 가격으로 체결 (슬리피지 반영)",
        },
    }


def ensure_v3_state():
    """Gist/로컬 상태가 v3.0이 아니면 신규 시드 200만 원으로 초기화한다."""
    state = load_state(force_remote=True)
    if state.get("version") != VERSION:
        prev = state.get("version")
        state = new_v3_state()
        save_state(state, remote=True, force_remote=True)
        print(f"[Engine] State reset from v{prev} -> v{VERSION} "
              f"(initial capital {state['initial_capital']:,} KRW, 0 trades)")
    else:
        # 규칙 정의가 바뀌었으면 규칙만 최신화 (전적/잔고는 유지)
        if state.get("rules") != DEFAULT_RULES_V3:
            state["rules"] = DEFAULT_RULES_V3
            save_state(state, remote=False)
    return state


def _position_price(state, stream):
    pos = state["position"]
    price = stream.last_price(pos["market"]) if stream else None
    if price is None:
        price = _rest_price(pos["market"])
    return price


def _equity(state, stream):
    cash = float(state.get("cash_krw") or 0.0)
    pos = state.get("position")
    if not pos:
        return cash
    price = _position_price(state, stream) or pos.get("entry_price", 0)
    return cash + float(pos.get("coin_qty", 0.0)) * float(price)


def _close_position(state, exit_price, reason, now, exit_kind):
    pos = state["position"]
    entry_price = float(pos["entry_price"])
    qty = float(pos["coin_qty"])
    gross = qty * exit_price
    net = gross * (1 - FEE_RATE)
    pnl = net - float(pos["cost_krw"])
    gain_pct = (exit_price - entry_price) / entry_price * 100.0

    state["cash_krw"] = net
    state["total_realized_pnl"] = float(state.get("total_realized_pnl", 0.0)) + pnl
    state["total_trades"] = int(state.get("total_trades", 0)) + 1
    if pnl > 0:
        state["wins"] = int(state.get("wins", 0)) + 1
    else:
        state["losses"] = int(state.get("losses", 0)) + 1
        if exit_kind == "STOP_LOSS":
            mins = state["rules"].get("loss_cooldown_minutes", 5)
            state["cooldown_until"] = fmt(now + timedelta(minutes=mins))

    state["recent_trades"].append({
        "type": "SELL",
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
    state["position"] = None
    state["status"] = "WAITING_FOR_SURGE"
    return pnl, gain_pct


def _check_exit(state, stream, now):
    """보유 포지션 틱 단위 청산 판정. 반환: (exit_price, reason, exit_kind) 또는 None"""
    pos = state["position"]
    rules = state["rules"]
    price = _position_price(state, stream)
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


def _evaluate_market(stream, market, rules, now_ms):
    m = stream.market_metrics(market, now_ms, rules["short_window_sec"], rules["base_window_sec"])
    if not m:
        return None
    reasons = []
    if m["coverage_sec"] < rules["min_coverage_sec"]:
        reasons.append(f"관측 {m['coverage_sec']:.0f}s < {rules['min_coverage_sec']}s")
    if m["base_krw"] <= 0 or m["base_ticks"] < rules["min_base_ticks"]:
        reasons.append(f"기준 표본 부족({m['base_ticks']}틱)")
    if m["short_krw"] < rules["min_short_krw"]:
        reasons.append(f"15초 거래대금 {m['short_krw']/1e4:.0f}만 < {rules['min_short_krw']/1e4:.0f}만")
    if m["surge_ratio"] < rules["min_surge_ratio"]:
        reasons.append(f"폭발배수 {m['surge_ratio']:.1f}x < {rules['min_surge_ratio']:.1f}x")
    if m["bid_ratio"] < rules["min_bid_ratio"]:
        reasons.append(f"매수비율 {m['bid_ratio']*100:.0f}% < {rules['min_bid_ratio']*100:.0f}%")
    if m["chg_3m_pct"] < rules["min_chg_pct"]:
        reasons.append(f"3분변동 {m['chg_3m_pct']:+.2f}% < +{rules['min_chg_pct']}%")
    if m["chg_3m_pct"] > rules["max_chg_pct"]:
        reasons.append(f"3분변동 {m['chg_3m_pct']:+.2f}% > +{rules['max_chg_pct']}% (뒷북)")
    m["qualified"] = not reasons
    m["reject"] = " · ".join(reasons) if reasons else "진입 조건 충족"
    return m


def _enter(state, stream, m, now):
    market = m["market"]
    name = (stream.names().get(market) if stream else None) or market.replace("KRW-", "")
    invest = float(state["cash_krw"])
    if invest <= 0:
        return None
    net = invest * (1 - FEE_RATE)
    price = float(m["last_price"])
    qty = net / price
    state["position"] = {
        "market": market,
        "name": name,
        "entry_price": price,
        "highest_price": price,
        "coin_qty": qty,
        "cost_krw": invest,
        "entry_time": fmt(now),
        "entry_ts_ms": _now_ms(),
        "entry_type": "WS_TICK_SURGE",
        "surge_ratio": round(m["surge_ratio"], 2),
        "bid_ratio": round(m["bid_ratio"], 3),
        "chg_3m_pct": round(m["chg_3m_pct"], 2),
        "short_krw": round(m["short_krw"]),
        "last_price": price,
        "unrealized_pct": 0.0,
    }
    state["cash_krw"] = 0.0
    state["status"] = "IN_POSITION"
    state["recent_trades"].append({
        "type": "BUY",
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


def run_sniper_tick(stream=None):
    state = load_state()
    if state.get("version") != VERSION:
        state = ensure_v3_state()

    rules = state.get("rules") or DEFAULT_RULES_V3
    now = kst_now()
    now_ms = _now_ms()
    state["last_checked_time"] = fmt(now)
    event = None

    # ---------- 1. 보유 포지션: 틱 단위 청산 감시 ----------
    if state.get("position"):
        exit_hit = _check_exit(state, stream, now)
        if exit_hit:
            price, reason, kind = exit_hit
            pnl, gain = _close_position(state, price, reason, now, kind)
            event = f"SELL {state['recent_trades'][-1]['market']} {gain:+.2f}% / {pnl:+,.0f}원"
        else:
            pos = state["position"]
            cur = _position_price(state, stream)
            if cur:
                pos["last_price"] = cur
            gain = ((cur - pos["entry_price"]) / pos["entry_price"] * 100.0) if cur else 0.0
            pos["unrealized_pct"] = round(gain, 2)
            event = f"HOLDING {pos.get('name')}({pos['market']}) {gain:+.2f}%"

    # ---------- 2. 현금 보유: 쿨다운 / BTC 필터 / 초입 포착 ----------
    else:
        if state.get("cooldown_until"):
            try:
                cd = datetime.strptime(state["cooldown_until"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
                if now < cd:
                    rem = int((cd - now).total_seconds())
                    event = f"⏳ 손절 쿨다운 {rem // 60}분 {rem % 60}초 남음"
                else:
                    state["cooldown_until"] = None
            except Exception:
                state["cooldown_until"] = None

        btc = _btc_trend_cached()
        state["btc_trend"] = btc
        if event is None and rules.get("btc_filter_enabled", True) and btc.get("is_bearish"):
            event = f"🛑 BTC 15분봉 20이평 하회 ({btc.get('diff_pct', 0):+.2f}%) — 신규 진입 차단"

        if event is None and stream is not None:
            markets = stream.markets()
            evaluated = []
            for market in markets:
                m = _evaluate_market(stream, market, rules, now_ms)
                if m:
                    evaluated.append(m)
            qualified = [m for m in evaluated if m["qualified"]]
            solid = [m for m in evaluated if m["base_krw"] > 0 and m["base_ticks"] >= 3]
            # 표시용: 초저유동성 틱 튐(수십만 원대)이 1위로 보이지 않도록 최소 300만 원 이상만 노출
            display_pool = [m for m in solid if m["short_krw"] >= 3_000_000]
            if not display_pool:
                display_pool = sorted(solid, key=lambda x: x["short_krw"], reverse=True)[:6]
            top = sorted(display_pool, key=lambda x: x["surge_ratio"], reverse=True)[:6]
            state["live_scan"] = {
                "scanned": len(evaluated),
                "qualified": len(qualified),
                "top": [{
                    "market": m["market"],
                    "name": (stream.names().get(m["market"]) or m["market"]).replace("KRW-", ""),
                    "price": m["last_price"],
                    "short_krw": round(m["short_krw"]),
                    "surge_ratio": round(m["surge_ratio"], 2),
                    "bid_ratio": round(m["bid_ratio"], 3),
                    "chg_3m_pct": round(m["chg_3m_pct"], 2),
                    "qualified": m["qualified"],
                    "reject": m["reject"],
                } for m in top],
            }
            if qualified:
                best = max(qualified, key=lambda x: (x["surge_ratio"], x["short_krw"]))
                market = _enter(state, stream, best, now)
                if market:
                    event = (f"⚡ BUY {best['market']} {best['surge_ratio']:.1f}배 "
                             f"매수 {best['bid_ratio']*100:.0f}% 3분변동 {best['chg_3m_pct']:+.2f}%")
            elif event is None:
                best = top[0] if top else None
                if best:
                    event = (f"🔍 초입 포착 대기 (최대 {best['surge_ratio']:.1f}배 · "
                             f"매수 {best['bid_ratio']*100:.0f}%)")
                else:
                    event = "🔍 웹소켓 틱 수신 대기"

    # ---------- 3. 평가 자산 / 전적 정리 ----------
    if len(state.get("recent_trades", [])) > 40:
        state["recent_trades"] = state["recent_trades"][-40:]

    equity = _equity(state, stream)
    state["current_equity"] = round(equity)
    state["current_ret_pct"] = round(
        ((equity - state["initial_capital"]) / state["initial_capital"]) * 100, 2)
    state["peak_equity"] = max(float(state.get("peak_equity") or state["initial_capital"]), equity)
    dd = (state["peak_equity"] - equity) / state["peak_equity"] * 100 if state["peak_equity"] else 0.0
    state["max_drawdown_pct"] = round(max(float(state.get("max_drawdown_pct") or 0.0), dd), 2)
    state["last_event"] = event

    trade_event = event is not None and (event.startswith("BUY") or event.startswith("SELL"))
    save_state(state, remote=True, force_remote=trade_event)

    return {
        "status": state["status"],
        "event": event,
        "equity": state["current_equity"],
        "ret_pct": state["current_ret_pct"],
        "cash": round(float(state["cash_krw"])),
        "trades": state["total_trades"],
        "wins": state["wins"],
        "losses": state["losses"],
        "btc_trend": state.get("btc_trend"),
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
