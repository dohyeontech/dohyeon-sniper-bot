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

from sniper_engine import (TICK_INTERVAL_SEC, TIER_KEYS, ensure_v3_state,  # noqa: E402
                           run_sniper_tick, VERSION)
from persistence import load_state, sync_status  # noqa: E402
from ws_stream import UpbitTradeStream  # noqa: E402

is_running = True
stream = UpbitTradeStream()
_boot = {"started_at": None, "ticks": 0, "last_error": None}


def engine_loop():
    """1.5초 주기 초단타 판단 루프 (단일 스레드 — 3계좌 상태 경합 방지)."""
    print(f"[Bot Engine] v{VERSION} multi-tier engine loop started "
          f"(interval {TICK_INTERVAL_SEC}s, tiers {'/'.join(TIER_KEYS)}).")
    time.sleep(1.0)
    while is_running:
        t0 = time.time()
        try:
            res = run_sniper_tick(stream)
            _boot["ticks"] += 1
            print(f"[Tick {datetime.now(KST).strftime('%H:%M:%S')}] {res.get('status')} | "
                  f"{res.get('event')} | Total {res.get('equity'):,} KRW")
        except Exception as e:
            _boot["last_error"] = f"{type(e).__name__}: {e}"
            print(f"[Bot Engine Error] {e}")
            traceback.print_exc()
        elapsed = time.time() - t0
        time.sleep(max(0.2, TICK_INTERVAL_SEC - elapsed))


def ws_watch_loop():
    """웹소켓 수신 상태 60초 주기 로그 (Render 로그에서 실시간 틱 수신 검증용)."""
    while is_running:
        try:
            s = stream.stats()
            st = load_state()
            per_tier = " ".join(
                f"{k}={((st.get('tiers') or {}).get(k) or {}).get('current_equity')}" for k in TIER_KEYS)
            print(f"[WS-STATUS] connected={s['ws_connected']} markets={s['ws_markets_count']} "
                  f"ticks/s={s['ws_ticks_per_sec']} total_ticks={s['ws_ticks_total']} "
                  f"reconnects={s['ws_reconnects']} msg_age={s['ws_message_age_sec']}s | "
                  f"equity={st.get('current_equity'):,} trades={st.get('total_trades')} | "
                  f"tiers {per_tier} | gist={sync_status().get('gist_last_sync_at')}")
        except Exception as e:
            print(f"[WS-STATUS] log error: {e}")
        time.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global is_running
    is_running = True
    _boot["started_at"] = kst_now_str()
    try:
        ensure_v3_state()
    except Exception as e:
        print(f"[Boot] state init error: {e}")
    stream.start()
    threads = [
        threading.Thread(target=engine_loop, name="sniper-engine", daemon=True),
        threading.Thread(target=ws_watch_loop, name="ws-watch", daemon=True),
    ]
    for t in threads:
        t.start()
    yield
    is_running = False
    stream.stop()
    print("[Bot Engine] Shutting down.")


app = FastAPI(title="도현의 테크브리프 · 실시간 스나이퍼 봇 v3.1 (3계좌 병렬)", lifespan=lifespan)


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
    data["server_time"] = kst_now_str()
    data["engine_ticks"] = _boot["ticks"]
    data["engine_started_at"] = _boot["started_at"]
    data["engine_last_error"] = _boot["last_error"]

    tiers = data.get("tiers") or {}
    # 계좌별 요약 (dashboard 비교 카드용)
    data["tier_order"] = TIER_KEYS
    data["tier_summary"] = {
        k: {
            "key": k,
            "label": (tiers.get(k) or {}).get("label"),
            "style": (tiers.get(k) or {}).get("style"),
            "min_short_krw": (tiers.get(k) or {}).get("min_short_krw"),
            "status": (tiers.get(k) or {}).get("status"),
            "current_equity": (tiers.get(k) or {}).get("current_equity"),
            "current_ret_pct": (tiers.get(k) or {}).get("current_ret_pct"),
            "win_rate_pct": round(((tiers.get(k) or {}).get("wins", 0) /
                                   max((tiers.get(k) or {}).get("total_trades", 0), 1)) * 100, 1),
            "wins": (tiers.get(k) or {}).get("wins", 0),
            "losses": (tiers.get(k) or {}).get("losses", 0),
            "total_trades": (tiers.get(k) or {}).get("total_trades", 0),
            "position": (tiers.get(k) or {}).get("position"),
            "cooldown_until": (tiers.get(k) or {}).get("cooldown_until"),
            "last_event": (tiers.get(k) or {}).get("last_event"),
        } for k in TIER_KEYS
    }
    data["bots"] = tiers  # 하위 호환 별칭
    data["aggregate"] = {
        "equity": data.get("current_equity"),
        "initial_capital": data.get("initial_capital"),
        "ret_pct": data.get("current_ret_pct"),
        "total_trades": data.get("total_trades"),
        "wins": data.get("wins"),
        "losses": data.get("losses"),
        "realized_pnl": data.get("total_realized_pnl"),
        "active_positions": data.get("active_positions", 0),
    }
    return data


@app.get("/")
def root():
    return RedirectResponse(url="/dashboard")


@app.get("/health")
def health():
    state = load_state()
    ws = stream.stats()
    tiers = state.get("tiers") or {}
    return {
        "status": "ok",
        "service": "sniper-bot",
        "version": state.get("version", "unknown"),
        "bot_status": state.get("status", "UNKNOWN"),
        "current_equity": state.get("current_equity", 0),
        "tiers": {k: (tiers.get(k) or {}).get("current_equity") for k in TIER_KEYS},
        "ws_connected": ws["ws_connected"],
        "ws_ticks_per_sec": ws["ws_ticks_per_sec"],
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
    return JSONResponse(content=stream.stats())


@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    with open(os.path.join(current_dir, "dashboard.html"), "r", encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
