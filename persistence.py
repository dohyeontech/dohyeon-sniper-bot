import os, json, urllib.request

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state.json')

def load_state():
    gist_token = os.environ.get('GIST_TOKEN')
    gist_id = os.environ.get('GIST_ID')
    
    if gist_token and gist_id:
        try:
            url = f"https://api.github.com/gists/{gist_id}"
            req = urllib.request.Request(url, headers={
                'Authorization': f'token {gist_token}',
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': 'SniperBot/1.0'
            })
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                content = data['files']['state.json']['content']
                state = json.loads(content)
                print(f"[Persistence] Successfully loaded state from Gist {gist_id}")
                return state
        except Exception as e:
            print(f"[Persistence] Failed to fetch Gist: {e}. Falling back to local.")
            
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
            
    # Default fallback state
    return {
        "version": "1.0",
        "status": "WAITING_FOR_SURGE",
        "initial_capital": 2000000,
        "cash_krw": 2000000.0,
        "position": None,
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "rules": {
            "min_volume_surge": 2.2,
            "target_profit_pct": 2.0,
            "trailing_stop_activation_pct": 1.5,
            "trailing_stop_drop_pct": 0.5,
            "stop_loss_pct": -1.2,
            "max_hold_minutes": 30
        },
        "current_equity": 2000000,
        "current_ret_pct": 0.0
    }

def save_state(state):
    # 1. Save local
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
        
    # 2. Sync to Gist if credentials present
    gist_token = os.environ.get('GIST_TOKEN')
    gist_id = os.environ.get('GIST_ID')
    if gist_token and gist_id:
        try:
            url = f"https://api.github.com/gists/{gist_id}"
            payload = json.dumps({
                "files": {
                    "state.json": {
                        "content": json.dumps(state, indent=2, ensure_ascii=False)
                    }
                }
            }).encode('utf-8')
            
            req = urllib.request.Request(url, data=payload, headers={
                'Authorization': f'token {gist_token}',
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': 'SniperBot/1.0',
                'Content-Type': 'application/json'
            }, method='PATCH')
            with urllib.request.urlopen(req, timeout=5) as resp:
                pass
        except Exception as e:
            print(f"[Persistence] Failed to sync to Gist: {e}")
