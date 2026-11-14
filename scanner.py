import urllib.request, json, time, math

FEE_RATE = 0.0005 # Upbit fee 0.05%

def get_krw_markets():
    url = "https://api.upbit.com/v1/market/all?is_details=true"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read().decode())
        
    valid_markets = []
    for m in data:
        code = m['market']
        if not code.startswith('KRW-'):
            continue
        # Filter out investment warning / caution coins
        if m.get('market_warning', 'NONE') != 'NONE':
            continue
        valid_markets.append((code, m['korean_name']))
    return valid_markets

def fetch_tickers(market_codes):
    # Upbit allows multiple markets separated by comma
    results = []
    batch_size = 50
    for i in range(0, len(market_codes), batch_size):
        batch = market_codes[i:i+batch_size]
        url = f"https://api.upbit.com/v1/ticker?markets={','.join(batch)}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                results.extend(data)
            time.sleep(0.08)
        except Exception as e:
            pass
    return results

def scan_volume_surges(min_24h_vol_krw=3_000_000_000): # 최소 30억 이상 거래대금
    markets = get_krw_markets()
    market_dict = {m[0]: m[1] for m in markets}
    
    tickers = fetch_tickers(list(market_dict.keys()))
    
    # 1. Filter by 24h volume (> 30억)
    active_tickers = [t for t in tickers if t['acc_trade_price_24h'] >= min_24h_vol_krw]
    
    candidates = []
    
    # For top active tickers, check 5-minute candle volume surge
    # Sort by highest 24h change & volume to prioritize candidates
    active_tickers.sort(key=lambda x: (x['signed_change_rate'], x['acc_trade_price_24h']), reverse=True)
    
    for t in active_tickers[:25]: # Deep inspect top 25 high-activity coins
        code = t['market']
        name = market_dict.get(code, code)
        cur_price = t['trade_price']
        
        try:
            url_c = f"https://api.upbit.com/v1/candles/minutes/5?market={code}&count=12" # past 1 hour (12 * 5min)
            req_c = urllib.request.Request(url_c, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req_c, timeout=3) as resp:
                candles = json.loads(resp.read().decode())
                candles.reverse()
            time.sleep(0.05)
        except Exception:
            continue
            
        if len(candles) < 6:
            continue
            
        latest_c = candles[-1]
        prev_candles = candles[:-1]
        
        # Latest 5-min trade volume vs average of previous 5-min candles
        avg_prev_vol = sum(c['candle_acc_trade_volume'] for c in prev_candles) / len(prev_candles)
        cur_vol = latest_c['candle_acc_trade_volume']
        
        vol_surge_ratio = (cur_vol / avg_prev_vol) if avg_prev_vol > 0 else 1.0
        
        # 5-minute price change %
        c_open = latest_c['opening_price']
        c_high = latest_c['high_price']
        c_low = latest_c['low_price']
        c_close = latest_c['trade_price']
        
        candle_change_pct = (c_close - c_open) / c_open * 100
        upper_shadow = (c_high - max(c_open, c_close)) / (c_high - c_low) if (c_high > c_low) else 0.0
        
        # Surge trigger criteria:
        # 1. 5-min volume surge >= 2.5x (250%+)
        # 2. Bullish candle (candle_change_pct > 0.6%)
        # 3. Not a dump pin (upper shadow < 0.40)
        is_surge = (vol_surge_ratio >= 2.2 and candle_change_pct >= 0.5 and upper_shadow < 0.45)
        
        candidates.append({
            'code': code,
            'name': name,
            'price': cur_price,
            'high_price': c_high,
            'low_price': c_low,
            'open_price': c_open,
            'vol_24h_krw': round(t['acc_trade_price_24h'] / 100_000_000, 1), # 억 단위
            'vol_surge_ratio': round(vol_surge_ratio, 2),
            'candle_5m_change_pct': round(candle_change_pct, 2),
            'upper_shadow': round(upper_shadow, 2),
            'is_surge': is_surge
        })
        
    candidates.sort(key=lambda x: (x['is_surge'], x['vol_surge_ratio']), reverse=True)
    return candidates

def check_btc_trend():
    """
    Checks Upbit KRW-BTC 15-minute candles (20 candles).
    Returns dict:
      'is_bearish': bool (True if price is below 20-SMA by > 0.2% or dropping),
      'btc_price': float,
      'sma20': float,
      'diff_pct': float,
      'trend': 'BULLISH' | 'NEUTRAL' | 'BEARISH'
    """
    try:
        url = 'https://api.upbit.com/v1/candles/minutes/15?market=KRW-BTC&count=20'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            candles = json.loads(resp.read().decode())
        closes = [c['trade_price'] for c in candles]
        cur_p = closes[0]
        sma20 = sum(closes) / len(closes)
        diff_pct = (cur_p - sma20) / sma20 * 100
        trend = 'BULLISH' if diff_pct >= 0.1 else ('NEUTRAL' if diff_pct >= -0.2 else 'BEARISH')
        return {
            'is_bearish': trend == 'BEARISH',
            'btc_price': cur_p,
            'sma20': round(sma20),
            'diff_pct': round(diff_pct, 2),
            'trend': trend
        }
    except Exception as e:
        return {'is_bearish': False, 'btc_price': 0, 'sma20': 0, 'diff_pct': 0.0, 'trend': 'UNKNOWN'}

if __name__ == '__main__':
    print("Scanning active Upbit coins for volume surge...")
    cands = scan_volume_surges()
    surges = [c for c in cands if c['is_surge']]
    print(f"Total inspected: {len(cands)}, Surges detected: {len(surges)}")
    for c in cands[:8]:
        status = "🔥 SURGE DETECTED" if c['is_surge'] else "대기"
        print(f"[{status}] {c['name']} ({c['code']}) | 가격: {c['price']:,}원 | 5분봉거래량: {c['vol_surge_ratio']}배 | 5분변동: {c['candle_5m_change_pct']}% | 24h거래대금: {c['vol_24h_krw']}억")
