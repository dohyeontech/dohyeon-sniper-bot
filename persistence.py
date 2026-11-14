"""
상태 영속성 (v4.0): 메모리 캐시 + 로컬 파일 + (주기적) 비공개 Gist 동기화.

v4.0 엔진은 1.5초마다 상태를 갱신하므로
- 읽기: 프로세스 메모리 캐시 사용 (Gist API 호출 없음, 시작 시 1회만 원격 조회)
- 쓰기: 로컬 파일은 매번 원자적 저장, Gist는 체결/포지션 변경 시 즉시 + 그 외 60초 간격

`last_load_source()`로 이 프로세스가 실제로 어디서 상태를 읽었는지 알 수 있다.
Gist가 설정된 환경에서 원격 로드가 실패하고 로컬 파일로 폴백한 경우, 엔진은
그 상태를 원격에 다시 쓰지 않는다 (낡은 파일로 라이브 전적을 덮어쓰는 사고 방지).
"""

import json
import os
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")

GIST_SYNC_MIN_INTERVAL = 60.0   # 초 단위 최소 Gist 동기화 간격
USER_AGENT = "SniperBot/4.0"

_lock = threading.RLock()
_state_cache = None
_last_sync_ts = 0.0
_last_sync_ok = None
_last_sync_error = None
_last_sync_at = None
_pending = False
_load_source = None


def _creds():
    return os.environ.get("GIST_TOKEN"), os.environ.get("GIST_ID")


def gist_enabled():
    token, gist_id = _creds()
    return bool(token and gist_id)


def last_load_source():
    return _load_source


def _default_state():
    """파일도 Gist도 없을 때의 최소 상태 — 엔진이 v4.0 10전략 리그로 초기화한다."""
    return {
        "version": "4.0",
        "status": "WAITING_FOR_SIGNAL",
        "initial_capital": 20000000,
        "cash_krw": 20000000.0,
        "position": None,
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "current_equity": 20000000,
        "current_ret_pct": 0.0,
    }


def _fetch_gist(token, gist_id, timeout=8):
    req = urllib.request.Request(
        f"https://api.github.com/gists/{gist_id}",
        headers={"Authorization": f"token {token}",
                 "Accept": "application/vnd.github.v3+json",
                 "User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read().decode())
    return json.loads(data["files"]["state.json"]["content"])


def _write_local(state):
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        print(f"[Persistence] local save failed: {e}")


def load_state(force_remote=False):
    global _state_cache, _last_sync_error, _load_source
    with _lock:
        if _state_cache is not None and not force_remote:
            return _state_cache

    state = None
    token, gist_id = _creds()
    if token and gist_id:
        try:
            state = _fetch_gist(token, gist_id)
            _load_source = "gist"
            print(f"[Persistence] Loaded state from Gist {gist_id} (version {state.get('version')})")
        except Exception as e:
            _last_sync_error = f"load failed: {type(e).__name__}: {e}"
            print(f"[Persistence] Gist load failed: {e} -> falling back to local file")

    if state is None and os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            _load_source = "local"
            print("[Persistence] Loaded state from local state.json")
        except Exception as e:
            print(f"[Persistence] local load failed: {e}")

    if state is None:
        state = _default_state()
        _load_source = "default"

    with _lock:
        _state_cache = state
    return state


def _sync_gist(state):
    global _last_sync_ts, _last_sync_ok, _last_sync_error, _last_sync_at
    token, gist_id = _creds()
    if not (token and gist_id):
        return False
    try:
        payload = json.dumps({"files": {"state.json": {
            "content": json.dumps(state, indent=2, ensure_ascii=False)}}}).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.github.com/gists/{gist_id}", data=payload, method="PATCH",
            headers={"Authorization": f"token {token}",
                     "Accept": "application/vnd.github.v3+json",
                     "User-Agent": USER_AGENT,
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            resp.read()
        _last_sync_ts = time.time()
        _last_sync_ok = True
        _last_sync_error = None
        _last_sync_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        return True
    except Exception as e:
        _last_sync_ok = False
        _last_sync_error = f"{type(e).__name__}: {e}"
        print(f"[Persistence] Gist sync failed: {e}")
        return False


def save_state(state, remote=True, force_remote=False):
    """로컬 저장은 항상, Gist 동기화는 force 또는 60초 간격."""
    global _state_cache, _pending
    with _lock:
        _state_cache = state
    _write_local(state)

    if not remote:
        _pending = True
        return False
    token, gist_id = _creds()
    if not (token and gist_id):
        return False
    now = time.time()
    if not force_remote and (now - _last_sync_ts) < GIST_SYNC_MIN_INTERVAL:
        _pending = True
        return False
    ok = _sync_gist(state)
    if ok:
        _pending = False
    return ok


def sync_status():
    token, gist_id = _creds()
    return {
        "gist_enabled": bool(token and gist_id),
        "gist_id": gist_id if gist_id else None,
        "gist_last_sync_at": _last_sync_at,
        "gist_last_sync_ok": _last_sync_ok,
        "gist_last_error": _last_sync_error,
        "gist_pending": _pending,
        "load_source": _load_source,
    }
