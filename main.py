import os, sys, asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from sniper_engine import run_sniper_tick
from persistence import load_state

is_running = True

async def bot_loop():
    print("[Bot Engine] 24/7 Background sniper loop started.")
    # Short initial sleep so server binds port immediately on Render
    await asyncio.sleep(2)
    while is_running:
        try:
            loop = asyncio.get_running_loop()
            res = await loop.run_in_executor(None, run_sniper_tick)
            print(f"[Bot Tick {datetime.now().strftime('%H:%M:%S')}] Status: {res.get('status')} | Event: {res.get('event')} | Equity: {res.get('equity'):,} KRW")
        except Exception as e:
            print(f"[Bot Engine Error] {e}")
            
        # Scan every 60 seconds
        await asyncio.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global is_running
    is_running = True
    task = asyncio.create_task(bot_loop())
    yield
    is_running = False
    task.cancel()

app = FastAPI(title="도현의 테크브리프 · 가상 거래량 스나이퍼 봇", lifespan=lifespan)

@app.get("/")
def root():
    return RedirectResponse(url="/dashboard")

@app.get("/health")
def health():
    state = load_state()
    return {
        "status": "ok",
        "service": "sniper-bot",
        "bot_status": state.get("status", "UNKNOWN"),
        "current_equity": state.get("current_equity", 0),
        "last_checked": state.get("last_checked_time", ""),
        "server_time": datetime.now().isoformat()
    }

@app.get("/api/status")
def api_status():
    return load_state()

@app.get("/api/v1-archive")
def api_v1_archive():
    archive_path = os.path.join(current_dir, "v1_archive.json")
    if os.path.exists(archive_path):
        with open(archive_path, "r", encoding="utf-8") as f:
            return JSONResponse(content=json.load(f))
    state = load_state()
    return JSONResponse(content=state.get("v1_archive", {}))

@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    html_path = os.path.join(current_dir, "dashboard.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
