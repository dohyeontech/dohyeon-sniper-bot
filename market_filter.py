"""
전 시장 공통 제외 필터 — 스테이블코인(달러·유로 페그) 마켓.

왜 필요한가
  스테이블코인은 설계상 가격이 페그에 고정돼 변동성이 없다. 1분봉 밴드폭이
  극단적으로 좁아지므로 `bb_squeeze`(밴드 수축 → 상단 돌파) 조건이 항상 참에
  가깝게 성립하고, `major_momentum`(24h 거래대금 TOP10) 같은 유동성 상위 조건도
  늘 만족한다. 그런데 실제로는 1틱(=0.01%) 이상 움직이지 않으므로 목표 익절에
  닿을 수 없고, 시간초과로 나가면서 왕복 수수료 0.05%만 계좌에서 사라진다.
  → 진입 자체를 원천 차단한다 (웹소켓 구독 · 1분봉 수집 · 신호 판정 · 체결).

단일 정의 원칙
  이 파일 하나에만 정의하고 ws_stream / candles / sniper_engine 이 모두
  `is_excluded_market()` 을 통해 참조한다. 새 스테이블코인이 상장되면
  STABLE_BASES 에 심볼만 추가하면 전 경로에 동시에 적용된다.
"""

# 달러 페그 스테이블코인 (업비트 KRW 마켓 상장 심볼 + 주요 미상장 심볼 포함)
STABLE_BASES = frozenset({
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "PYUSD", "USDD", "FDUSD",
    "GUSD", "USDE", "USDS", "USD1", "USDG", "RLUSD", "LUSD", "CRVUSD", "GHO",
    "FRAX", "SUSD", "USTC",
    # 유로 페그
    "EURC", "EURS", "EURT", "EURE",
})

# 원화 마켓에서 제외할 마켓 코드 (예: KRW-USDT) — STABLE_BASES 로부터 유도
EXCLUDED_MARKETS = frozenset(f"KRW-{b}" for b in STABLE_BASES)

NOTE = ("스테이블코인(KRW-USDT·KRW-USDC 등)은 가격이 페그에 고정돼 목표 익절 도달이 불가능하고 "
        "변이 0에 가까워 밴드 수축·유동성 상위 조건만 통과해 수수료만 누수되므로 "
        "구독·1분봉 수집·신호 판정·진입에서 전면 제외")


def base_symbol(market):
    """'KRW-USDT' → 'USDT' (마켓 코드가 아니면 입력 그대로)."""
    if not market or not isinstance(market, str):
        return market
    return market.split("-", 1)[1] if "-" in market else market


def is_stablecoin(market):
    """마켓 코드의 기초자산이 스테이블코인인가."""
    return str(base_symbol(market)).upper() in STABLE_BASES


def is_excluded_market(market):
    """전략 판정·수집·구독에서 제외해야 하는 마켓인가 (현재는 스테이블코인)."""
    if not market:
        return False
    if str(market).upper() in EXCLUDED_MARKETS:
        return True
    return is_stablecoin(market)


def filter_markets(markets):
    """마켓 리스트에서 제외 대상을 걸러낸다 (순서 유지)."""
    return [m for m in (markets or []) if not is_excluded_market(m)]
