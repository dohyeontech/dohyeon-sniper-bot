import os, sys, json, time, urllib.request
from datetime import datetime, timezone, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)
from persistence import load_state, save_state
from scanner import scan_volume_surges, fetch_tickers, check_btc_trend

FEE_RATE = 0.0005 # 0.05% Upbit fee

DEFAULT_RULES_V2 = {
    'min_volume_surge': 2.2,             # 5분봉 거래량 2.2배 이상 폭발
    'pullback_pct': 1.0,                 # 거래량 폭발 고점 대비 -1.0% 눌림목 진입 (고점 추격 방지)
    'target_profit_pct': 2.8,            # 목표 익절 +2.8%
    'trailing_stop_activation_pct': 1.8, # +1.8% 이상 상승 시 트레일링 스탑 활성화
    'trailing_stop_drop_pct': 0.5,       # 고점 대비 0.5% 반락 시 자동 청산
    'stop_loss_pct': -2.4,               # 기계적 손절 라인 -2.4% (노이즈 방어)
    'max_hold_minutes': 45,              # 최대 보유 45분
    'loss_cooldown_minutes': 15,         # 손절 후 재진입 쿨다운 15분
    'btc_filter_enabled': True           # 비트코인 15분봉 20이평 하회 시 진입 차단
}

def init_sniper_bot(initial_capital=2000000):
    now_str = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')
    state = {
        'version': '2.0',
        'status': 'WAITING_FOR_SURGE', # WAITING_FOR_SURGE or IN_POSITION
        'initialized_at': now_str,
        'initial_capital': initial_capital,
        'cash_krw': float(initial_capital),
        'position': None,
        'pending_order': None,
        'cooldown_until': None,
        'btc_trend': None,
        'total_realized_pnl': 0.0,
        'total_trades': 0,
        'wins': 0,
        'losses': 0,
        'recent_trades': [],
        'last_checked_time': now_str,
        'rules': DEFAULT_RULES_V2,
        'current_equity': initial_capital,
        'current_ret_pct': 0.0
    }
    save_state(state)
    return state

def check_position_exit_candles(pos, rules, now):
    market = pos['market']
    entry_price = pos['entry_price']
    entry_dt = datetime.strptime(pos['entry_time'], '%Y-%m-%d %H:%M:%S')
    
    target_price = entry_price * (1 + rules.get('target_profit_pct', 2.8) / 100.0)
    stop_price = entry_price * (1 + rules.get('stop_loss_pct', -2.4) / 100.0)
    highest_price = max(pos.get('highest_price', entry_price), entry_price)
    
    url = f"https://api.upbit.com/v1/candles/minutes/1?market={market}&count=120"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as res:
            candles = list(reversed(json.loads(res.read())))
    except Exception as e:
        return None
        
    for c in candles:
        cdt = datetime.strptime(c['candle_date_time_kst'].replace('T', ' '), '%Y-%m-%d %H:%M:%S')
        if cdt <= entry_dt:
            continue
            
        high = c['high_price']
        low = c['low_price']
        close = c['trade_price']
        open_p = c['opening_price']
        
        if high > highest_price:
            highest_price = high
            
        minutes_held = (cdt - entry_dt).total_seconds() / 60.0
        
        # 1. Stop loss check
        if low <= stop_price:
            exit_price = stop_price if open_p > stop_price else open_p
            gain_pct = (exit_price - entry_price) / entry_price * 100.0
            return {
                'exit_time': cdt.strftime('%Y-%m-%d %H:%M:%S'),
                'exit_price': exit_price,
                'gain_pct': gain_pct,
                'reason': f"🛑 기계적 손절 발동 ({gain_pct:.2f}%)"
            }
            
        # 2. Target profit check
        if high >= target_price:
            exit_price = target_price if open_p < target_price else open_p
            gain_pct = (exit_price - entry_price) / entry_price * 100.0
            return {
                'exit_time': cdt.strftime('%Y-%m-%d %H:%M:%S'),
                'exit_price': exit_price,
                'gain_pct': gain_pct,
                'reason': f"🎯 목표 익절 달성 (+{gain_pct:.2f}%)"
            }
            
        # 3. Trailing stop check
        act_gain = (highest_price - entry_price) / entry_price * 100.0
        if act_gain >= rules.get('trailing_stop_activation_pct', 1.8):
            pullback = (close - highest_price) / highest_price * 100.0
            if pullback <= -rules.get('trailing_stop_drop_pct', 0.5):
                exit_price = close
                gain_pct = (exit_price - entry_price) / entry_price * 100.0
                return {
                    'exit_time': cdt.strftime('%Y-%m-%d %H:%M:%S'),
                    'exit_price': exit_price,
                    'gain_pct': gain_pct,
                    'reason': f"🛡️ 트레일링 스탑 익절 (+{gain_pct:.2f}%, 고점대비 {pullback:.2f}%)"
                }
                
        # 4. Max hold minutes check
        max_mins = rules.get('max_hold_minutes', 45)
        if minutes_held >= max_mins:
            exit_price = close
            gain_pct = (exit_price - entry_price) / entry_price * 100.0
            return {
                'exit_time': cdt.strftime('%Y-%m-%d %H:%M:%S'),
                'exit_price': exit_price,
                'gain_pct': gain_pct,
                'reason': f"⏱️ {max_mins}분 보유 제한 초과 청산 ({gain_pct:+.2f}%)"
            }
            
    pos['highest_price'] = highest_price
    return None

def build_return(state, event):
    return {
        'status': state['status'],
        'event': event,
        'equity': state.get('current_equity', 0),
        'ret_pct': state.get('current_ret_pct', 0.0),
        'cash': round(state.get('cash_krw', 0.0)),
        'trades': state.get('total_trades', 0),
        'wins': state.get('wins', 0),
        'losses': state.get('losses', 0),
        'btc_trend': state.get('btc_trend', {})
    }

def run_sniper_tick():
    state = load_state()
        
    now = datetime.now(timezone(timedelta(hours=9)))
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    state['last_checked_time'] = now_str
    
    # Ensure v1_archive is present in state
    if 'v1_archive' not in state:
        archive_path = os.path.join(current_dir, 'v1_archive.json')
        if os.path.exists(archive_path):
            try:
                with open(archive_path, 'r', encoding='utf-8') as f:
                    state['v1_archive'] = json.load(f)
            except Exception:
                pass
    if state.get('version') != '2.0' or state.get('rules', {}).get('stop_loss_pct') == -1.2:
        state['version'] = '2.0'
        state['rules'] = DEFAULT_RULES_V2
        if 'pending_order' not in state:
            state['pending_order'] = None
        if 'cooldown_until' not in state:
            state['cooldown_until'] = None
        print("[Sniper Engine] Upgraded live state to v2.0 (Pullback & BTC Filter & SL -2.4%)")
        
    event = None
    
    # CASE 1: Currently Holding Position -> Check Exit
    if state['position'] is not None:
        pos = state['position']
        market = pos['market']
        entry_price = pos['entry_price']
        coin_qty = pos['coin_qty']
        
        exit_result = check_position_exit_candles(pos, state['rules'], now)
        
        if exit_result:
            exit_price = exit_result['exit_price']
            cur_gain_pct = exit_result['gain_pct']
            exit_reason = exit_result['reason']
            exit_time_str = exit_result['exit_time']
            
            gross_sell_krw = coin_qty * exit_price
            net_sell_krw = gross_sell_krw * (1 - FEE_RATE)
            pnl_krw = net_sell_krw - pos['cost_krw']
            
            state['cash_krw'] = net_sell_krw
            state['total_realized_pnl'] += pnl_krw
            state['total_trades'] += 1
            if pnl_krw > 0:
                state['wins'] += 1
            else:
                state['losses'] += 1
                # Enforce cooldown on loss to prevent churn
                cd_mins = state['rules'].get('loss_cooldown_minutes', 15)
                state['cooldown_until'] = (now + timedelta(minutes=cd_mins)).strftime('%Y-%m-%d %H:%M:%S')
                
            trade_log = {
                'type': 'SELL',
                'market': market,
                'name': pos['name'],
                'entry_time': pos['entry_time'],
                'exit_time': exit_time_str,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'gain_pct': round(cur_gain_pct, 2),
                'pnl_krw': round(pnl_krw),
                'reason': exit_reason
            }
            state['recent_trades'].append(trade_log)
            state['position'] = None
            state['status'] = 'WAITING_FOR_SURGE'
            event = f"SELL executed: {exit_reason} | PnL: {pnl_krw:+,.0f}원"
        else:
            # Still holding, update highest price
            t_data = fetch_tickers([market])
            if t_data:
                cur_price = t_data[0]['trade_price']
                pos['highest_price'] = max(pos.get('highest_price', entry_price), cur_price)
            event = f"HOLDING: {pos['name']}({market}) 포지션 유지 중"
            
    # CASE 2: Holding 100% Cash -> Process Cooldown / BTC Filter / Pullback Entry
    else:
        # 1. Cooldown check
        if state.get('cooldown_until'):
            try:
                cd_dt = datetime.strptime(state['cooldown_until'], '%Y-%m-%d %H:%M:%S')
                if now < cd_dt:
                    rem_sec = int((cd_dt - now).total_seconds())
                    event = f"⏳ 손절 후 쿨다운 대기 중 ({rem_sec//60}분 {rem_sec%60}초 남음) - 연쇄 손절 방지"
                else:
                    state['cooldown_until'] = None
            except Exception:
                state['cooldown_until'] = None

        # 2. BTC Trend Filter check
        btc_info = check_btc_trend()
        state['btc_trend'] = btc_info
        btc_filter_on = state['rules'].get('btc_filter_enabled', True)
        
        if event is None and btc_filter_on and btc_info['is_bearish']:
            event = f"🛑 BTC 15분봉 하락 추세 (BTC {btc_info['diff_pct']:+.2f}%) - 알트 신규 진입 차단"

        # 3. Process Pending Pullback Order
        if event is None and state.get('pending_order'):
            p_order = state['pending_order']
            try:
                exp_dt = datetime.strptime(p_order['expires_at'], '%Y-%m-%d %H:%M:%S')
                if now > exp_dt:
                    event = f"⏱️ {p_order['name']}({p_order['market']}) 눌림목 대기 시간(10분) 만료로 취소"
                    state['pending_order'] = None
                else:
                    market = p_order['market']
                    target_p = p_order['target_price']
                    dump_limit_p = p_order['surge_high'] * 0.970
                    
                    t_data = fetch_tickers([market])
                    if t_data:
                        cur_p = t_data[0]['trade_price']
                        if cur_p <= target_p and cur_p >= dump_limit_p:
                            # Touch pullback target -> BUY!
                            entry_price = cur_p
                            invest_krw = state['cash_krw']
                            net_invest_krw = invest_krw * (1 - FEE_RATE)
                            coin_qty = net_invest_krw / entry_price
                            
                            state['position'] = {
                                'market': market,
                                'name': p_order['name'],
                                'entry_price': entry_price,
                                'highest_price': entry_price,
                                'coin_qty': coin_qty,
                                'cost_krw': invest_krw,
                                'entry_time': now_str,
                                'surge_ratio': p_order['surge_ratio'],
                                'entry_type': 'PULLBACK'
                            }
                            state['cash_krw'] = 0.0
                            state['status'] = 'IN_POSITION'
                            state['pending_order'] = None
                            
                            trade_log = {
                                'type': 'BUY',
                                'market': market,
                                'name': p_order['name'],
                                'entry_time': now_str,
                                'entry_price': entry_price,
                                'cost_krw': round(invest_krw),
                                'surge_ratio': p_order['surge_ratio'],
                                'reason': f"🎯 -1.0% 눌림목 체결 ({entry_price:,.1f}원)"
                            }
                            state['recent_trades'].append(trade_log)
                            event = f"🎯 PULLBACK BUY: {p_order['name']}({market}) -1.0% 눌림목({entry_price:,.1f}원) 체결!"
                        elif cur_p < dump_limit_p:
                            event = f"⚠️ {p_order['name']}({market}) -3% 이상 덤핑 감지되어 주문 자동 취소"
                            state['pending_order'] = None
                        elif cur_p > p_order['surge_high'] * 1.025:
                            event = f"🚀 {p_order['name']}({market}) +2.5% 초과 급등하여 추격 방지 취소"
                            state['pending_order'] = None
                        else:
                            event = f"⏳ {p_order['name']}({market}) 눌림목 대기 중 (목표: {target_p:,.1f}원 / 현재: {cur_p:,.1f}원)"
            except Exception as e:
                state['pending_order'] = None

        # 4. Scan for Volume Surges if still waiting
        if event is None and state['position'] is None and state.get('pending_order') is None:
            candidates = scan_volume_surges()
            surges = [c for c in candidates if c['is_surge']]
            
            if surges:
                best = surges[0]
                market = best['code']
                name = best['name']
                cur_price = best['price']
                surge_high = max(best.get('high_price', cur_price), cur_price)
                target_entry = surge_high * (1 - state['rules'].get('pullback_pct', 1.0) / 100.0)
                dump_limit = surge_high * 0.970
                
                # If price is ALREADY in healthy pullback zone (e.g. upper shadow pullback)
                if cur_price <= target_entry and cur_price >= dump_limit:
                    entry_price = cur_price
                    invest_krw = state['cash_krw']
                    net_invest_krw = invest_krw * (1 - FEE_RATE)
                    coin_qty = net_invest_krw / entry_price
                    
                    state['position'] = {
                        'market': market,
                        'name': name,
                        'entry_price': entry_price,
                        'highest_price': entry_price,
                        'coin_qty': coin_qty,
                        'cost_krw': invest_krw,
                        'entry_time': now_str,
                        'surge_ratio': best['vol_surge_ratio'],
                        'entry_type': 'INSTANT_PULLBACK'
                    }
                    state['cash_krw'] = 0.0
                    state['status'] = 'IN_POSITION'
                    
                    trade_log = {
                        'type': 'BUY',
                        'market': market,
                        'name': name,
                        'entry_time': now_str,
                        'entry_price': entry_price,
                        'cost_krw': round(invest_krw),
                        'surge_ratio': best['vol_surge_ratio'],
                        'reason': f"🎯 즉시 눌림목 진입 ({entry_price:,.1f}원)"
                    }
                    state['recent_trades'].append(trade_log)
                    event = f"🎯 PULLBACK BUY: {name}({market}) 거래량 {best['vol_surge_ratio']}배 폭발 후 눌림목({entry_price:,.1f}원) 즉시 진입!"
                else:
                    # Register pending pullback order
                    state['pending_order'] = {
                        'market': market,
                        'name': name,
                        'surge_high': surge_high,
                        'target_price': target_entry,
                        'surge_ratio': best['vol_surge_ratio'],
                        'created_at': now_str,
                        'expires_at': (now + timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
                    }
                    event = f"⚡ 거래량 {best['vol_surge_ratio']}배 폭발: {name}({market}) 고점 {surge_high:,.1f}원 → -1% 눌림목({target_entry:,.1f}원) 대기 등록"
            else:
                event = "🔍 거래량 폭발 알트 스캔 중 (조건 미충족으로 대기)"
                
    # Keep only last 20 trade logs
    if len(state.get('recent_trades', [])) > 20:
        state['recent_trades'] = state['recent_trades'][-20:]
        
    # Calculate current total valuation
    cur_equity = state['cash_krw']
    if state['position'] is not None:
        t_data = fetch_tickers([state['position']['market']])
        if t_data:
            cur_equity += state['position']['coin_qty'] * t_data[0]['trade_price']
            
    state['current_equity'] = round(cur_equity)
    state['current_ret_pct'] = round(((cur_equity - state['initial_capital']) / state['initial_capital']) * 100, 2)
    
    save_state(state)
    return build_return(state, event)

if __name__ == '__main__':
    res = run_sniper_tick()
    print("Sniper Tick Result:", res)
