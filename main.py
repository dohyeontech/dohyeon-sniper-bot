import json
import os
import sys
import threading
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

KST = timezone(timedelta(hours=9))


def kst_now_str():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from sniper_engine import (STRATEGIES, STRATEGY_KEYS, TICK_INTERVAL_SEC,  # noqa: E402
                           ensure_v4_state, get_candle_cache, run_sniper_tick,
                           set_candle_cache, VERSION)
from candles import CandleCache  # noqa: E402
from persistence import load_state, sync_status  # noqa: E402
from ws_stream import UpbitTradeStream  # noqa: E402

is_running = True
stream = UpbitTradeStream()
_candle_cache = CandleCache(lambda: stream.markets(), top_n=15, refresh_sec=60.0,
                            sleep_between=0.25)
_boot = {"started_at": None, "ticks": 0, "last_error": None}


def engine_loop():
    """1.5초 주기 판단 루프 (단일 스레드 — 10전략 상태 경합 방지)."""
    print(f"[Bot Engine] v{VERSION} 10-strategy league loop started "
          f"(interval {TICK_INTERVAL_SEC}s, strategies {len(STRATEGY_KEYS)}).")
    time.sleep(1.0)
    while is_running:
        t0 = time.time()
        try:
            res = run_sniper_tick(stream)
            _boot["ticks"] += 1
            lead = ""
            lb = res.get("leaderboard") or []
            if lb:
                top = lb[0]
                lead = f" | 1위 {top['key']} {top['ret_pct']:+.2f}%"
            print(f"[Tick {datetime.now(KST).strftime('%H:%M:%S')}] {res.get('status')} | "
                  f"{res.get('event')} | Total {res.get('equity')} KRW{lead}")
        except Exception as e:
            _boot["last_error"] = f"{type(e).__name__}: {e}"
            print(f"[Bot Engine Error] {e}")
            traceback.print_exc()
        elapsed = time.time() - t0
        time.sleep(max(0.2, TICK_INTERVAL_SEC - elapsed))


def ws_watch_loop():
    """웹소켓·캔들·10전략 상태 60초 주기 로그 (Render 로그 검증용)."""
    while is_running:
        try:
            s = stream.stats()
            st = load_state()
            c = _candle_cache.stats()
            board = st.get("leaderboard") or []
            top3 = " ".join(f"{r['key']}={r['ret_pct']:+.2f}%" for r in board[:3])
            print(f"[WS-STATUS] connected={s['ws_connected']} markets={s['ws_markets_count']} "
                  f"ticks/s={s['ws_ticks_per_sec']} total_ticks={s['ws_ticks_total']} "
                  f"reconnects={s['ws_reconnects']} msg_age={s['ws_message_age_sec']}s | "
                  f"candles={c['markets_cached']}/{c['markets_target']} req={c['requests']} | "
                  f"equity={st.get('current_equity')} trades={st.get('total_trades')} | "
                  f"top3 {top3} | gist={sync_status().get('gist_last_sync_at')}")
        except Exception as e:
            print(f"[WS-STATUS] log error: {e}")
        time.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global is_running
    is_running = True
    _boot["started_at"] = kst_now_str()
    try:
        ensure_v4_state()
    except Exception as e:
        print(f"[Boot] state init error: {e}")
    stream.start()
    set_candle_cache(_candle_cache)
    _candle_cache.start()
    threads = [
        threading.Thread(target=engine_loop, name="sniper-engine", daemon=True),
        threading.Thread(target=ws_watch_loop, name="ws-watch", daemon=True),
    ]
    for t in threads:
        t.start()
    yield
    is_running = False
    _candle_cache.stop()
    stream.stop()
    print("[Bot Engine] Shutting down.")


app = FastAPI(title="도현의 테크브리프 · 10대 단타 전략 실시간 비교 리그 v4.0", lifespan=lifespan)


def _trim_trades(strategy_state, keep=0):
    """매매내역은 통합 recent_trades 한 벌만 내려보낸다 (페이로드 절감)."""
    out = dict(strategy_state)
    trades = out.get("recent_trades") or []
    out["recent_trades"] = trades[-keep:] if keep else []
    out["recent_trade_count"] = len(trades)
    return out


def _merged_status():
    state = load_state()
    data = dict(state)
    ws = stream.stats()
    data["ws"] = ws
    data["ws_connected"] = ws["ws_connected"]
    data["ws_markets_count"] = ws["ws_markets_count"]
    data["ws_ticks_per_sec"] = ws["ws_ticks_per_sec"]
    data["ws_ticks_total"] = ws["ws_ticks_total"]
    data["ws_last_message_at"] = ws["ws_last_message_at"]
    data["ws_reconnects"] = ws["ws_reconnects"]
    data["persistence"] = sync_status()
    data["candles"] = _candle_cache.stats()
    data["server_time"] = kst_now_str()
    data["engine_ticks"] = _boot["ticks"]
    data["engine_started_at"] = _boot["started_at"]
    data["engine_last_error"] = _boot["last_error"]

    strategies = state.get("strategies") or {}
    data["strategies"] = {k: _trim_trades(strategies[k]) for k in STRATEGY_KEYS if k in strategies}
    data["strategy_order"] = list(STRATEGY_KEYS)
    data["strategy_meta"] = {
        k: {"key": k, "label": STRATEGIES[k]["label"], "name": STRATEGIES[k]["name"],
            "style": STRATEGIES[k]["style"], "entry": STRATEGIES[k]["entry"],
            "exit": STRATEGIES[k]["exit"], "params": STRATEGIES[k]["params"],
            "carried_over": STRATEGIES[k]["legacy"]}
        for k in STRATEGY_KEYS
    }
    data["leaderboard"] = state.get("leaderboard") or []
    data["bots"] = data["strategies"]        # 구 대시보드 호환 별칭
    data["tiers"] = {k: data["strategies"][k] for k in ("3m", "10m", "30m")
                     if k in data["strategies"]}
    data["aggregate"] = {
        "equity": data.get("current_equity"),
        "initial_capital": data.get("initial_capital"),
        "ret_pct": data.get("current_ret_pct"),
        "total_trades": data.get("total_trades"),
        "wins": data.get("wins"),
        "losses": data.get("losses"),
        "realized_pnl": data.get("total_realized_pnl"),
        "active_positions": data.get("active_positions", 0),
        "leader": data.get("leader"),
        "laggard": data.get("laggard"),
        "strategy_count": len(STRATEGY_KEYS),
    }
    return data


@app.get("/")
def root():
    return RedirectResponse(url="/dashboard")


@app.get("/health")
def health():
    state = load_state()
    ws = stream.stats()
    strategies = state.get("strategies") or {}
    board = state.get("leaderboard") or []
    return {
        "status": "ok",
        "service": "sniper-bot",
        "version": state.get("version", "unknown"),
        "mode": state.get("mode"),
        "bot_status": state.get("status", "UNKNOWN"),
        "current_equity": state.get("current_equity", 0),
        "strategy_count": len(strategies),
        "strategies": {k: (strategies.get(k) or {}).get("current_equity") for k in STRATEGY_KEYS},
        "leader": board[0] if board else None,
        "ws_connected": ws["ws_connected"],
        "ws_ticks_per_sec": ws["ws_ticks_per_sec"],
        "candles_cached": _candle_cache.stats()["markets_cached"],
        "last_checked": state.get("last_checked_time", ""),
        "server_time": datetime.now(KST).isoformat(),
    }


@app.get("/api/status")
def api_status():
    return JSONResponse(content=_merged_status())


def _archive_response(filename, key):
    state = load_state()
    path = os.path.join(current_dir, filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return JSONResponse(content=json.load(f))
    return JSONResponse(content=state.get(key, {}))


@app.get("/api/v1-archive")
def api_v1_archive():
    return _archive_response("v1_archive.json", "v1_archive")


@app.get("/api/v2-archive")
def api_v2_archive():
    return _archive_response("v2_archive.json", "v2_archive")


@app.get("/api/ws")
def api_ws():
    return JSONResponse(content={"ws": stream.stats(), "candles": _candle_cache.stats()})


@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    with open(os.path.join(current_dir, "dashboard.html"), "r", encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
