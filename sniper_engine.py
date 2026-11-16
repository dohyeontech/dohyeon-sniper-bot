"""
스나이퍼 봇 v5.2 — 「단타 전략 실시간 비교 리그 (정체 청산 철폐 + 목표 현실화·노이즈 차단)」.

업비트 실시간 WebSocket 체결 스트림 **1개** 위에서, 잔고·포지션·전적·쿨다운·매매내역이
완전히 분리된 **10개의 독립 가상 계좌(각 시드 2,000,000원)** 를 동시에 운용한다.
같은 시장 스냅샷을 모든 전략이 공유하므로 "같은 순간에 어떤 규칙이 돈을 버는가"를
조건이 동일한 상태에서 비교할 수 있다.

v5.0 개편 근거 (v4.0 실거래 390건 진단)
  · v4.0 손실의 정체는 방향이 아니라 **왕복 비용**이었다. 최근 200청산의 가격손익은
    +119,993원이었지만 수수료 378,008원이 이를 뒤집어 순 -258,015원이 됐다.
  · 손실은 **대형주(BTC·ETH·XRP·SOL·DOGE 등)** 에 집중됐다. 70건이 청산의 90%를 시간초과로
    맞으며 건당 -0.141%(-313,638원), 알트 130건은 건당 +0.135%(+55,623원).
    → BTC의 20분 변동(0.2~0.4%)에 목표 +1.5~2.5%를 건 **유니버스·밴드 불일치**가 원인.
  · 호가 스프레드를 0으로 가정한 체결 모델도 낙관적이었다. 93.8원 종목은 1틱이 0.107%,
    128원 종목은 0.78%라 왕복 실비용이 수수료 0.10% + 호가 1틱 = 0.21~0.88%까지 벌어진다.

그래서 v5.0은 세 가지를 공통 규칙으로 넣는다.
  1. **체결 모델**: 매수 = 관측가 + 0.5틱, 매도 = 관측가 - 0.5틱 → 호가 스프레드 1틱을
     매수·매도에 나눠 문다. 왕복 실비용 = 수수료 0.10% + 호가 1틱.
     (목표 익절은 목표가 체결, 그 외는 관측 틱 체결 — 초과분 미반영의 보수적 규칙 유지)
  2. **변동성 게이트**: 10분 레인지가 전략별 하한 미만인 종목은 진입 금지 (`min_range_10m_pct`).
     BTC·ETH 같은 대형주는 이 게이트에서 자동 제외되어, 목표에 닿을 수 없는 거래를 하지 않는다.
  3. **비용 게이트**: 목표 익절폭이 왕복 실비용의 `cost_mult`배(기본 5배) 미만이면 진입 금지.
     + **1일 거래 상한**(`max_trades_per_day`)으로 수수료 예산을 강제한다.

  규칙 그대로 승계 (v4.0 잔고·전적·포지션 무손실)
    1. 3m               — 300만↑ 급등 추격 (대조군)
    2. 10m              — 1,000만↑ 급등 추격 (v4 리그 2위)
    3. panic_bounce     — 투매 과매도 반등 (v4 리그 1위)
  조건 재설계 (잔고·전적 승계, 규칙만 교체)
    4. 30m              — 대형주 10분 레인지 역추세 (구 3,000만↑ 급등 추격 → 비용 인식 밴드)
    5. pullback_reentry — 눌림목 재진입 (변동성·비용 게이트 + 트레일링 + 20분 상한)
    6. vwap_breakout    — 실시간 VWAP 돌파 (거래량 3배·변동성 게이트 + 트레일링)
    7. bb_squeeze       — 볼린저 스퀴즈 돌파 (중심선 위 상승 국면 한정 + 트레일링)
  신규 3전략 (각 200만 원 신규 시드)
    8. box_breakout     — 10분 박스권 돌파 (수축 후 거래량 동반 확장)
    9. alt_rs           — 알트-BTC 디커플링 상대강도
   10. tape_sweep       — 대형 매수 체결 스파이크 (체결 불균형)

  퇴출·아카이브(v4_archive.json/state): `rsi_oversold`(-22.15%), `major_momentum`(-14.55%),
  `streak_ignition`(-6.07%) — 대형주에 알트 밴드를 씌운 구조적 결함 또는 거래 빈도 자체가
  수수료를 만들던 구조. 전적·청산사유 분포·진단 문구를 아카이브에 그대로 보존한다.

공통 리스크 규칙: BTC 15분봉 20이평 대비 -0.2% 초과 하회 시 모멘텀·돌파 9전략 신규 진입 차단
(보유 포지션은 유지). 역추세 1전략(`panic_bounce`)만 `btc_filter: False`로 예외 적용.
손절 시에만 전략별 쿨다운이 걸리고, 그 쿨다운도 계좌별로 독립이다.

시장 전역 제외: 스테이블코인(`KRW-USDT`·`KRW-USDC`·`KRW-DAI`·`KRW-BUSD` 등)은 가격이 페그에
고정돼 목표 익절 도달이 불가능하고 변이 0이라 밴드 수축·유동성 상위 조건만 통과해 수수료만
누수된다. `market_filter.py` 정의 하나로 구독·1분봉 수집·신호 판정·체결에서 전면 제외하며,
제외 대상 마켓을 보유 중이면 다음 틱에 정상 정리(청산)한다.

종목별 재진입 쿨다운: 재설계·신규 전략은 청산 사유와 무관하게 같은 종목 재진입을 15~20분
금지한다(한 종목 몰빵·연속 손절 차단). `panic_bounce`는 기존대로 손절 후 15분 금지.

v5.1 수정 (2026-11-16, 라이브 v5.0 진단)
  · **alt_rs 0전 버그**: `ws_stream.market_snapshot()`은 `base_sec=180` 이전 틱을 버리므로
    `coverage_sec`은 구조상 180에 닿을 수 없다(최대 179.x초). `if coverage_sec < 180`은 항상
    참이 되어 alt_rs가 한 번도 진입하지 못했다(라이브 last_event "3분 관측 179s < 180s").
    → 전략 파라미터 `min_coverage_sec: 150`(기본값도 150)로 완화.
  · **손익비 역전**: panic_bounce는 승률 50.8%인데 평균 익절 +1.04% vs 평균 손절 -2.20%로
    기대값이 음수였다. 트레일링 발동 +1.3%→+1.6%(고점 대비 -0.40%), 손절 -1.5%→-1.3%.
  · **시간초과(최근 청산의 45%)**: 모멘텀·돌파 7전략(3m·10m·pullback·vwap·bb·box·alt_rs)의
    최대 보유를 20분→12분으로 줄이고, 진입 6분 뒤에도 -0.25~+0.25% 무풍 정체면 `STAGNATION`
    (무풍 정체 청산)으로 먼저 끊는다. 12분을 묶여 있던 손실을 만들지 않는 것이 목적.
  · **30m(대형주 레인지 역추세)**: 직전 12전 0승 12패. 대형주 12분 변동폭에 맞춰 목표
    +0.9%→+0.75%, 트레일 +0.6%→+0.50%(고점 -0.25%→-0.20%), 손절 -0.6%→-0.50%,
    최대 20분→12분.
  · **계좌 무손실 승계**: v5.0의 10계좌 잔고·전적·포지션·매매내역을 그대로 이어받는다
    (`migrate_from_v5`). v4_archive와 v4→v5 승계 이력은 보존하고, 규칙 파라미터가 실제로
    바뀐 계좌만 1일 거래 예산 시계(`rules_effective_from`)를 새 규칙 시점으로 옮긴다.

v5.2 정비 (2026-11-17, 라이브 v5.1 밤사이 190전 51승 139패 -133만 원 진단)
  · **6분 무풍 정체 청산(STAGNATION) 철폐**: v5.1이 넣은 정체 청산은 최근 청산 60건 가운데
    33건(-86,026원)을 6분 만에 끊었다. 왕복 비용(0.2~0.5%)을 넘기기 전에 손실만 확정하고
    TARGET 0건 · STAGNATION 33건 · TIME 15건이라는 결과를 만든 직접 원인이다. v5.2는 정체 청산
    로직과 파라미터를 전부 제거하고, 손절 · 목표 · 트레일링 · 보유시간 상한(`max_hold_min`)으로만
    청산한다.
  · **모멘텀·돌파 6전략 목표 현실화**: 횡보/약세 변동성 장에서 +2.0~+2.5% 목표는 찍히지 않고
    시간초과로 밀렸다. 3m·10m·pullback_reentry·vwap_breakout·bb_squeeze·box_breakout의 목표를
    +1.4~+1.7%, 트레일링 발동을 +0.9~+1.0%(고점 대비 -0.30~-0.35%), 손절을 -1.2~-1.4%로 맞춰
    수익이 날 때 확실히 챙기고 손실은 짧게 끊는다.
  · **3m 초민감 스캘핑 조이기**: 15초 거래대금 하한 300만→800만 원, 매수(BID) 70%→78%,
    1일 상한 10회. 3m는 125전 -49.06%로 밤 손실의 26%를 냈다 — 낮은 문턱의 허매수 진입이 원인.
  · **alt_rs 엄선**: 180초 관측 버그를 고친 뒤에도 16전 3승 13패(18.8%)였다. 디커플링 기준
    +0.8%p→+1.3%p, 15초 거래대금 300만→700만 원, 매수(BID) 62%→75%, 1일 상한 6회.
  · **panic_bounce 밴드 조정**: 트레일 발동(+1.6%)이 목표(+2.0%)에 너무 가까워 이익을 못 챙겼다.
    목표 +1.8%, 트레일 +1.2%(고점 -0.35%), 손절 -1.25%로 밸런스를 맞춘다.
  · **30m · tape_sweep 유지**: 30m는 최근 7건 -1,056원(거의 본전), tape_sweep는 목표 +1.8% ·
    손절 -1.0% · 8분 상한으로 이미 비용 대비 손익비가 서 있다. 규칙을 바꾸지 않으므로 두 계좌의
    1일 거래 예산 시계도 그대로 유지된다.
  · **무손실 승계**: `migrate_from_v5`가 v5.1의 10계좌 잔고·전적·포지션·매매내역·쿨다운을 그대로
    이어받고 v4_archive · v5_fix 이력도 보존한다. 규칙이 바뀐 계좌만 `rules_effective_from`을
    승계 시각으로 옮겨 1일 예산을 새로 센다.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from persistence import load_state, save_state, last_load_source, gist_enabled  # noqa: E402
from scanner import check_btc_trend, fetch_tickers  # noqa: E402
from market_filter import (  # noqa: E402
    is_excluded_market, STABLE_BASES, EXCLUDED_MARKETS, NOTE as MARKET_FILTER_NOTE,
)

VERSION = "5.2"
FEE_RATE = 0.0005           # 업비트 수수료 0.05%
TICK_INTERVAL_SEC = 1.5     # 엔진 판단 주기
DEFAULT_SEED = 2_000_000    # 전략별 모의 시드
KST = timezone(timedelta(hours=9))
SPREAD_TICKS = 0.5          # 편도 0.5틱 = 왕복 호가 1틱 (매수 +0.5틱, 매도 -0.5틱)

LEGACY_KEYS = ("3m", "10m", "30m")
KEPT_KEYS = ("3m", "10m", "panic_bounce")                  # 잔고 승계 계좌 (v5.2에서 밴드·문턱 조정)
REBUILT_KEYS = ("30m", "pullback_reentry", "vwap_breakout", "bb_squeeze")
NEW_KEYS = ("box_breakout", "alt_rs", "tape_sweep")
RETIRED_KEYS = ("rsi_oversold", "major_momentum", "streak_ignition")
STRATEGY_KEYS = ("3m", "10m", "30m", "panic_bounce", "pullback_reentry", "vwap_breakout",
                 "bb_squeeze") + NEW_KEYS

# v4.0에서 퇴출한 전략의 진단 (아카이브에 함께 보존)
RETIRED_DIAGNOSIS = {
    "rsi_oversold": (
        "47전 17승 30패(승률 32.7%) -22.15%. 최근 20청산 중 15건이 시간초과였고, 실제 거래 종목은 "
        "1분봉 캐시 대상인 거래대금 상위 15종목(BTC·ETH·SOL·XRP·DOGE 등 대형주)이었다. "
        "1분봉 RSI(14) 25 이하라는 신호에 +1.8%/-1.5% 밴드를 걸었지만 대형주의 25분 변동은 "
        "0.3~0.6%라 목표에 닿을 수 없었고, 60초 주기(최대 180초 허용) 캐시라 신호 자체도 늦었다. "
        "→ 장세가 아니라 유니버스·밴드·데이터 지연의 설계 결함."),
    "major_momentum": (
        "37전 15승 22패(승률 40.5%) -14.55%. 24h 거래대금 TOP10은 정의상 대형주뿐인데 "
        "+1.5%/-1.2% 밴드와 트레일링(+1.0% 활성→고점 -0.3%)을 걸어, 이익은 0.62~0.91%에서 잘리고 "
        "손실은 -1.2~-1.5%로 남았다(승 +0.81% / 패 -0.90%). 눌림 8건이 -1.41% 평균 손절. "
        "→ 승률 40.5%에서 기대값 음수가 확정된 구조."),
    "streak_ignition": (
        "72전 27승 45패(승률 37.5%) -6.07%. 리그에서 가장 많이 거래한 계좌로, 10초 BID 85% 조건이 "
        "BTC·XRP 같은 대형주에서 10분간 ±0.1% 횡보를 사들이며 왕복 수수료만 누적했다"
        "(대형주 12/20건, 평균 +0.08%). 목표 +1.8%/-1.2%에 10분 상한이라 대형주에서는 도달 불가. "
        "→ 신호가 아니라 거래 빈도 자체가 비용이 된 사례."),
}

# 컨셉이 유효해 재설계로 살린 전략의 진단 (대시보드·보고서 근거)
REBUILT_DIAGNOSIS = {
    "30m": ("20전 7승 13패 -4.69%. 20청산 중 17건이 시간초과, 18건이 대형주였다. 3,000만↑/15초 "
            "문턱은 구조상 BTC·ETH·XRP·SOL만 통과시키는데 목표 +2.5%는 20분 안에 도달 불가. "
            "→ 대형주에 맞는 0.9%/0.6% 밴드의 레인지 역추세로 교체."),
    "pullback_reentry": ("38전 15승 23패(39.5%) -10.55%. 목표 도달 5건 vs 손절 8건(평균 -1.41%), "
                         "시간초과 7건. 눌림 구간(-1.0~-2.2%)이 변동성과 무관한 고정값이라 "
                         "죽은 종목에서도 진입했고, 트레일링이 없어 +1.5%까지 갔다가 시간초과로 "
                         "되돌려 나왔다. → 변동성·비용 게이트 + 트레일링 + 20분 상한."),
    "vwap_breakout": ("30전 10승 20패(33.3%) -7.5%. 20청산 중 13건이 시간초과, 목표 도달 2건. "
                      "VWAP 돌파 후 2.0%까지 가는 종목이 드물었고 상승이 꺾여도 잡아줄 트레일링이 "
                      "없었다. → 거래량 문턱 3배·변동성 게이트 + 트레일링 + 20분 상한."),
    "bb_squeeze": ("24전 10승 14패(41.7%) -1.5%. 목표 도달 4건이 각 +2.4%(+4.2만 원)로 벌어줬지만 "
                   "시간초과 12건이 ±0%대로 수수료만 냈다. 30분 상한이 길고, 하락 국면의 수축 "
                   "돌파까지 잡았다. → 중심선 위 상승 국면 한정 + 트레일링 + 20분 상한."),
}


def tick_size(price):
    """업비트 원화마켓 호가단위(틱) — 가격대별 최소 호가."""
    try:
        p = float(price)
    except (TypeError, ValueError):
        return 0.0
    if p <= 0:
        return 0.0
    if p >= 2_000_000:
        return 1000.0
    if p >= 1_000_000:
        return 500.0
    if p >= 500_000:
        return 100.0
    if p >= 100_000:
        return 50.0
    if p >= 10_000:
        return 10.0
    if p >= 1_000:
        return 5.0
    if p >= 100:
        return 1.0
    if p >= 10:
        return 0.1
    if p >= 1:
        return 0.01
    if p >= 0.1:
        return 0.001
    if p >= 0.01:
        return 0.0001
    if p >= 0.001:
        return 0.00001
    return 0.000001


def cost_pct(price, spread_ticks=SPREAD_TICKS):
    """왕복 실비용(%) = 수수료 0.05%×2 + 호가 스프레드(편도 tick×spread_ticks)."""
    t = tick_size(price)
    if not t or not price:
        return 2 * FEE_RATE * 100.0
    return 2 * FEE_RATE * 100.0 + 2 * (t * spread_ticks / float(price) * 100.0)


def fill_buy(price, spread_ticks=SPREAD_TICKS):
    """매수 체결가 — 관측 체결가 + 0.5틱(ask 쪽). 왕복 호가 1틱을 매수·매도가 나눠 문다."""
    p = float(price)
    return p + tick_size(p) * spread_ticks


def fill_sell(price, spread_ticks=SPREAD_TICKS):
    """매도 체결가 — 관측 체결가 - 0.5틱(bid 쪽)."""
    p = float(price)
    out = p - tick_size(p) * spread_ticks
    return max(out, p * 0.5)


# ---------------------------------------------------------------- 전략 정의
STRATEGIES = {
    # ---- 규칙 불변 승계 (v4.0 리그 상위 3계좌 = 대조군) ----
    "3m": {
        "key": "3m", "label": "300만↑", "name": "300만↑ 급등 추격",
        "style": "공격형 · 초민감 스캘핑", "kind": "surge", "legacy": True, "btc_filter": True,
        "role": "kept",
        "entry": "15초 거래대금 800만↑ · 직전 3분 평균 대비 4.0배 · 매수(BID) 78%↑ · 3분변동 +0.3~+3.0%",
        "exit": "목표 +1.5% · 트레일링 +0.9%→고점 -0.30% · 손절 -1.3% · 최대 12분 · 쿨다운 5분 · 1일 10회",
        "params": {"min_short_krw": 8_000_000, "min_surge_ratio": 4.0, "min_bid_ratio": 0.78,
                   "min_chg_pct": 0.3, "max_chg_pct": 3.0, "target_pct": 1.5,
                   "trail_activation_pct": 0.9, "trail_drop_pct": 0.30, "stop_pct": -1.3,
                   "max_hold_min": 12, "cooldown_min": 5,
                   "cost_mult": 5, "max_trades_per_day": 10},
    },
    "10m": {
        "key": "10m", "label": "1,000만↑", "name": "1,000만↑ 급등 추격",
        "style": "균형형 · 중형 슈팅", "kind": "surge", "legacy": True, "btc_filter": True,
        "role": "kept",
        "entry": "15초 거래대금 1,000만↑ · 직전 3분 평균 대비 4.0배 · 매수(BID) 70%↑ · 3분변동 +0.3~+3.0%",
        "exit": "목표 +1.6% · 트레일링 +0.95%→고점 -0.32% · 손절 -1.35% · 최대 12분 · 쿨다운 5분",
        "params": {"min_short_krw": 10_000_000, "min_surge_ratio": 4.0, "min_bid_ratio": 0.70,
                   "min_chg_pct": 0.3, "max_chg_pct": 3.0, "target_pct": 1.6,
                   "trail_activation_pct": 0.95, "trail_drop_pct": 0.32, "stop_pct": -1.35,
                   "max_hold_min": 12, "cooldown_min": 5,
                   "cost_mult": 5},
    },
    # ---- 조건 재설계 4전략 (잔고·전적 승계) ----
    "30m": {
        "key": "30m", "label": "3,000만↑", "name": "대형주 10분 레인지 역추세",
        "style": "역추세 · 대형주 레인지", "kind": "range_rev", "legacy": True, "btc_filter": True,
        "role": "rebuilt",
        "entry": ("24h 거래대금 TOP10 대형주 · 10분 고점 대비 -0.35~-1.2% 눌림에서 "
                  "15초 BID 62%↑ + 15초 반등 0.05%↑"),
        "exit": ("목표 +0.75% · 트레일링 +0.50%→고점 -0.20% · 손절 -0.50% · 최대 12분 · "
                 "쿨다운 5분 · 동일종목 10분"),
        "params": {"top_n": 10, "dd_min_pct": -1.2, "dd_max_pct": -0.35, "min_bid_ratio": 0.62,
                   "min_rebound_pct": 0.05, "min_coverage_10m_sec": 540,
                   "target_pct": 0.75, "trail_activation_pct": 0.50, "trail_drop_pct": 0.20,
                   "stop_pct": -0.50, "max_hold_min": 12, "cooldown_min": 5,
                   "symbol_cooldown_min": 10, "symbol_cooldown_scope": "all",
                   "cost_mult": 5, "max_trades_per_day": 14},
    },
    "pullback_reentry": {
        "key": "pullback_reentry", "label": "눌림 재진입", "name": "눌림목 재진입",
        "style": "추세추종 · 눌림 매수", "kind": "pullback", "legacy": False, "btc_filter": True,
        "role": "rebuilt",
        "entry": ("3분 +0.5%↑ 상승 후 10분 고점 대비 -0.8~-2.0% 눌림 · 10분 레인지 1.0%↑ · "
                  "15초 BID 62%↑ + 상승 전환"),
        "exit": ("목표 +1.7% · 트레일링 +1.0%→고점 -0.35% · 손절 -1.4% · 최대 12분 · "
                 "쿨다운 5분 · 동일종목 20분"),
        "params": {"dd_min_pct": -2.0, "dd_max_pct": -0.8, "min_bid_ratio": 0.62,
                   "min_chg_3m_pct": 0.5, "min_coverage_10m_sec": 300,
                   "min_range_10m_pct": 1.0, "target_pct": 1.7,
                   "trail_activation_pct": 1.0, "trail_drop_pct": 0.35, "stop_pct": -1.4,
                   "max_hold_min": 12, "cooldown_min": 5,
                   "symbol_cooldown_min": 20, "symbol_cooldown_scope": "all",
                   "cost_mult": 5, "max_trades_per_day": 12},
    },
    "vwap_breakout": {
        "key": "vwap_breakout", "label": "VWAP 돌파", "name": "실시간 VWAP 돌파",
        "style": "추세추종 · 평균가 돌파", "kind": "vwap", "legacy": False, "btc_filter": True,
        "role": "rebuilt",
        "entry": ("3분 VWAP 하단에서 15초 거래대금 3.0배↑ + BID 62%↑ 동반, VWAP +0.35%↑ 돌파 · "
                  "10분 레인지 1.0%↑"),
        "exit": ("목표 +1.5% · 트레일링 +0.9%→고점 -0.30% · 손절 -1.25% · 최대 12분 · "
                 "쿨다운 5분 · 동일종목 20분"),
        "params": {"min_vol_ratio": 3.0, "min_bid_ratio": 0.62, "breakout_pct": 0.35,
                   "min_coverage_sec": 120, "min_range_10m_pct": 1.0,
                   "target_pct": 1.5, "trail_activation_pct": 0.9, "trail_drop_pct": 0.30,
                   "stop_pct": -1.25, "max_hold_min": 12, "cooldown_min": 5,
                   "symbol_cooldown_min": 20, "symbol_cooldown_scope": "all",
                   "cost_mult": 5, "max_trades_per_day": 12},
    },
    "bb_squeeze": {
        "key": "bb_squeeze", "label": "BB 스퀴즈", "name": "1분봉 볼린저 스퀴즈 돌파",
        "style": "변동성 확장 · 밴드 돌파", "kind": "bb", "legacy": False, "btc_filter": True,
        "role": "rebuilt",
        "entry": ("1분봉 볼린저(20,2) 수축(40봉 밴드폭 하위 20%) + 종가 > 중심선 + "
                  "상단 밴드 거래량 2.0배↑ 돌파 · 10분 레인지 0.9%↑"),
        "exit": ("목표 +1.6% · 트레일링 +0.95%→고점 -0.32% · 손절 -1.3% · 최대 12분 · "
                 "쿨다운 5분 · 동일종목 20분"),
        "params": {"bb_period": 20, "bb_k": 2.0, "squeeze_rank": 0.20, "min_vol_ratio": 2.0,
                   "max_candle_age_sec": 120, "require_above_mid": True,
                   "min_range_10m_pct": 0.9, "target_pct": 1.6,
                   "trail_activation_pct": 0.95, "trail_drop_pct": 0.32, "stop_pct": -1.3,
                   "max_hold_min": 12, "cooldown_min": 5,
                   "symbol_cooldown_min": 20, "symbol_cooldown_scope": "all",
                   "cost_mult": 5, "max_trades_per_day": 12},
    },
    "panic_bounce": {
        "key": "panic_bounce", "label": "투매 반등", "name": "투매 과매도 반등",
        "style": "역추세 · 급락 되돌림", "kind": "panic_bounce", "legacy": False, "btc_filter": False,
        "role": "kept",
        "entry": "3분 내 -2.5% 이상 급락 후 최근 15초 매수(BID) 비율 65%↑ & 직전 15초 대비 가격 반등",
        "exit": "목표 +1.8% · 트레일링 +1.2%→고점 -0.35% · 손절 -1.25% · 최대 15분 · 쿨다운 5분 · 동일종목 15분",
        "params": {"min_drop_pct": -2.5, "min_bid_ratio": 0.65, "target_pct": 1.8,
                   "trail_activation_pct": 1.2, "trail_drop_pct": 0.35, "stop_pct": -1.25,
                   "max_hold_min": 15, "cooldown_min": 5,
                   "symbol_cooldown_min": 15, "symbol_cooldown_scope": "stop"},
    },
    # ---- 신규 3전략 ----
    "box_breakout": {
        "key": "box_breakout", "label": "박스 돌파", "name": "10분 박스권 돌파",
        "style": "추세추종 · 수축 후 확장", "kind": "box", "legacy": False, "btc_filter": True,
        "role": "new",
        "entry": ("10분 박스 폭 1.1% 이하로 수축한 뒤 직전 15초 제외 박스 고점 +0.15% 돌파 · "
                  "15초 거래대금 3.0배↑ · BID 65%↑"),
        "exit": ("목표 +1.4% · 트레일링 +0.9%→고점 -0.30% · 손절 -1.2% · 최대 12분 · "
                 "쿨다운 5분 · 동일종목 20분"),
        "params": {"box_max_range_pct": 1.1, "breakout_pct": 0.15, "min_surge_ratio": 3.0,
                   "min_bid_ratio": 0.65, "min_short_krw": 5_000_000,
                   "min_coverage_10m_sec": 540, "min_range_10m_pct": 0.3,
                   "max_chg_15s_pct": 1.2, "target_pct": 1.4,
                   "trail_activation_pct": 0.9, "trail_drop_pct": 0.30, "stop_pct": -1.2,
                   "max_hold_min": 12, "cooldown_min": 5,
                   "symbol_cooldown_min": 20, "symbol_cooldown_scope": "all",
                   "cost_mult": 5, "max_trades_per_day": 10},
    },
    "alt_rs": {
        "key": "alt_rs", "label": "알트 상대강도", "name": "알트-BTC 디커플링 강세",
        "style": "로테이션 · 상대강도", "kind": "alt_rs", "legacy": False, "btc_filter": True,
        "role": "new",
        "entry": ("3분 수익률이 BTC 3분 수익률보다 +1.3%p 이상 앞서고(디커플링) 15초 거래대금 "
                  "700만↑ · BID 75%↑ · 10분 레인지 0.8%↑"),
        "exit": ("목표 +1.6% · 트레일링 +0.95%→고점 -0.32% · 손절 -1.3% · 최대 12분 · "
                 "쿨다운 5분 · 동일종목 20분 · 1일 6회"),
        "params": {"min_rs_pct": 1.3, "min_chg_3m_pct": 0.5, "min_surge_ratio": 2.5,
                   "min_bid_ratio": 0.75, "min_short_krw": 7_000_000,
                   "min_coverage_sec": 150, "min_range_10m_pct": 0.8,
                   "exclude_btc": True, "target_pct": 1.6,
                   "trail_activation_pct": 0.95, "trail_drop_pct": 0.32, "stop_pct": -1.3,
                   "max_hold_min": 12, "cooldown_min": 5,
                   "symbol_cooldown_min": 20, "symbol_cooldown_scope": "all",
                   "cost_mult": 5, "max_trades_per_day": 6},
    },
    "tape_sweep": {
        "key": "tape_sweep", "label": "체결 스파이크", "name": "대형 매수 체결 스파이크",
        "style": "체결불균형 · 초단타", "kind": "tape", "legacy": False, "btc_filter": True,
        "role": "new",
        "entry": ("10초 내 단일 매수 체결 300만↑ & 3분 평균 체결 금액의 4배↑(대형 매수 스윕) + "
                  "15초 가격 +0.12%↑ · BID 60%↑"),
        "exit": ("목표 +1.8% · 트레일링 +1.1%→고점 -0.3% · 손절 -1.0% · 최대 8분 · "
                 "쿨다운 3분 · 동일종목 15분"),
        "params": {"min_print_krw": 3_000_000, "min_print_mult": 4.0, "min_ticks_10s": 5,
                   "min_chg_15s_pct": 0.12, "min_bid_10s": 0.60,
                   "min_range_10m_pct": 0.6, "target_pct": 1.8,
                   "trail_activation_pct": 1.1, "trail_drop_pct": 0.3, "stop_pct": -1.0,
                   "max_hold_min": 8, "cooldown_min": 3,
                   "symbol_cooldown_min": 15, "symbol_cooldown_scope": "all",
                   "cost_mult": 5, "max_trades_per_day": 14},
    },
}


# BTC 하락 추세에서도 신규 진입을 허용하는 역추세 예외 전략 (나머지는 btc_filter: True)
BTC_FILTER_EXEMPT = tuple(k for k in STRATEGY_KEYS if not STRATEGIES[k].get("btc_filter", True))


def _rules_block():
    """대시보드·Gist에 저장하는 규칙 요약 (정적)."""
    return {
        "engine": f"Upbit WebSocket trade stream (tick-level) · 10전략 병렬 리그 v{VERSION}",
        "tick_interval_sec": TICK_INTERVAL_SEC,
        "seed_per_strategy": DEFAULT_SEED,
        "short_window_sec": 15,
        "base_window_sec": 180,
        "fee_rate": FEE_RATE,
        "fill_model": {
            "fee_round_trip_pct": round(2 * FEE_RATE * 100, 3),
            "spread": f"왕복 호가 1틱 (매수 관측가+{SPREAD_TICKS}틱, 매도 관측가-{SPREAD_TICKS}틱)",
            "note": ("v4.0까지는 호가 스프레드를 0으로 가정해 낙관적이었다. 93.8원 종목은 1틱이 "
                     "0.107%, 128원 종목은 0.78%다. v5.0부터 왕복 호가 1틱을 매수·매도에 "
                     "0.5틱씩 나눠 반영한다."),
            "target": "목표 익절 체결가는 목표가로 계산 (틱 초과분 미반영, 보수적)",
            "other_exits": "손절/트레일링/시간초과는 관측 틱 가격 -0.5틱으로 체결",
        },
        "cost_gates": {
            "volatility_gate": ("10분 레인지(%)가 전략별 하한 미만인 종목은 진입 금지 "
                                "(대형주는 목표 도달이 불가능하므로 자동 제외)"),
            "cost_mult": ("목표 익절폭이 왕복 실비용(수수료 0.10% + 호가 1틱)의 cost_mult배(기본 5배) "
                          "미만이면 진입 금지 — 수수료·호가 벽 방지 (v5.2부터 3m·10m 포함 8전략 적용)"),
            "daily_cap": "전략별 1일 거래(청산) 상한으로 수수료 예산을 강제",
            "hold_cap": ("보유 시간 상한만으로 시간 청산: 모멘텀·돌파 6전략(3m·10m·pullback·vwap·bb·box)과 "
                         "alt_rs는 12분, 30m는 12분, panic_bounce 15분, tape_sweep 8분 — "
                         "중간에 인위적으로 끊는 정체 청산(STAGNATION)은 v5.2에서 철폐"),
        },
        "btc_filter": {"enabled": True, "sma_window": "15분봉 20이평", "threshold_pct": -0.2,
                       "note": ("BTC가 20이평 대비 -0.2% 초과 하회하면 모멘텀·돌파 9전략 신규 진입 "
                                "차단(보유 유지). 역추세 1전략(투매 과매도 반등)만 예외"),
                       "exempt_strategies": list(BTC_FILTER_EXEMPT)},
        "market_filter": {"stablecoin_excluded": True,
                          "excluded_markets": sorted(EXCLUDED_MARKETS),
                          "excluded_bases": sorted(STABLE_BASES),
                          "note": MARKET_FILTER_NOTE},
        "candle_cache": {"top_n": 15, "refresh_sec": 60, "sleep_sec": 0.25,
                         "note": ("볼린저 전략용 1분봉은 별도 저속 스레드가 거래대금 상위 15종목만 "
                                  "순환 수집 (v5.0은 RSI 전략 퇴출로 캔들 의존 전략 1개)")},
        "gist_sync_interval_sec": 60,
        "strategies": {
            k: {"key": k, "label": STRATEGIES[k]["label"], "name": STRATEGIES[k]["name"],
                "style": STRATEGIES[k]["style"], "entry": STRATEGIES[k]["entry"],
                "exit": STRATEGIES[k]["exit"], "params": dict(STRATEGIES[k]["params"]),
                "btc_filter": bool(STRATEGIES[k]["btc_filter"]),
                "role": STRATEGIES[k].get("role"),
                "carried_over": STRATEGIES[k]["legacy"]}
            for k in STRATEGY_KEYS
        },
        "retired": {k: {"key": k, "diagnosis": RETIRED_DIAGNOSIS.get(k, "")} for k in RETIRED_KEYS},
    }


RULES_V4 = _rules_block()

_btc_cache = {"ts": 0.0, "data": None}
_rest_price_cache = {}
_candle_cache = None          # main.py가 set_candle_cache()로 주입
_ensure_guard = {"ts": 0.0, "last_note": None}


def kst_now():
    return datetime.now(KST)


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _now_ms():
    return int(time.time() * 1000)


def set_candle_cache(cache):
    global _candle_cache
    _candle_cache = cache


def get_candle_cache():
    return _candle_cache


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


# ---------------------------------------------------------------- 상태 구조
def new_strategy(key, seed=DEFAULT_SEED, now_str=None):
    meta = STRATEGIES[key]
    now_str = now_str or fmt(kst_now())
    return {
        "key": key,
        "label": meta["label"],
        "name": meta["name"],
        "style": meta["style"],
        "kind": meta["kind"],
        "btc_filter": bool(meta["btc_filter"]),
        "carried_over": meta["legacy"],
        "role": meta.get("role", "new"),
        "entry": meta["entry"],
        "exit": meta["exit"],
        "params": dict(meta["params"]),
        "initialized_at": now_str,
        "rules_effective_from": now_str,
        "initial_capital": float(seed),
        "cash_krw": float(seed),
        "position": None,
        "cooldown_until": None,
        "symbol_cooldowns": {},
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "status": "WAITING_FOR_SIGNAL",
        "last_event": f"{meta['name']} 계좌 초기화 — 신호 대기",
        "watch": None,
        "current_equity": float(seed),
        "current_ret_pct": 0.0,
        "peak_equity": float(seed),
        "max_drawdown_pct": 0.0,
    }


def new_league_state(seed=DEFAULT_SEED):
    now_str = fmt(kst_now())
    strategies = {k: new_strategy(k, seed, now_str) for k in STRATEGY_KEYS}
    total = float(seed) * len(STRATEGY_KEYS)
    state = {
        "version": VERSION,
        "status": "WAITING_FOR_SIGNAL",
        "engine": "ws_tick_v5_league",
        "mode": "ten_strategy_league",
        "initialized_at": now_str,
        "initial_capital": total,
        "cash_krw": total,
        "position": None,
        "cooldown_until": None,
        "btc_trend": None,
        "total_realized_pnl": 0.0,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "recent_trades": [],
        "last_checked_time": now_str,
        "last_event": f"v{VERSION} 10전략 리그 초기화 — 웹소켓 틱 스트림 대기",
        "rules": RULES_V4,
        "strategies": strategies,
        "strategy_order": list(STRATEGY_KEYS),
        "leaderboard": [],
        "current_equity": total,
        "current_ret_pct": 0.0,
        "peak_equity": total,
        "max_drawdown_pct": 0.0,
        "live_scan": {"scanned": 0, "qualified": 0, "top": []},
        "v4_archive": None,
        "exit_rules_note": {
            "target": "목표 익절 체결가는 목표가로 계산 (틱 초과분 미반영, 보수적)",
            "stop": "손절/트레일링/시간초과는 관측 틱 가격 -0.5틱으로 체결 (호가 반영)",
        },
    }
    _alias_legacy(state)
    return state


def _alias_legacy(state):
    """v3.x 소비자 호환: state['tiers']는 3계좌를 같은 객체로 가리킨다."""
    strat = state.get("strategies") or {}
    state["tiers"] = {k: strat[k] for k in LEGACY_KEYS if k in strat}
    return state


CARRY_FIELDS = ("initialized_at", "initial_capital", "cash_krw", "position", "cooldown_until",
                "symbol_cooldowns",
                "total_realized_pnl", "total_trades", "wins", "losses", "recent_trades",
                "status", "last_event", "watch", "current_equity", "current_ret_pct",
                "peak_equity", "max_drawdown_pct")


def _exit_kind_counts(trades):
    out = {}
    for t in trades or []:
        if t.get("type") != "SELL":
            continue
        k = t.get("exit_kind") or "UNKNOWN"
        out[k] = out.get(k, 0) + 1
    return out


def _v4_strategy_row(src, key, decision, diagnosis):
    """v4.0 전략 하나의 최종 성적표 (아카이브 행)."""
    src = src or {}
    trades = int(src.get("total_trades") or 0)
    wins = int(src.get("wins") or 0)
    losses = int(src.get("losses") or 0)
    cap = float(src.get("initial_capital") or DEFAULT_SEED)
    equity = float(src.get("current_equity") or src.get("cash_krw") or cap)
    cash = float(src.get("cash_krw") or 0.0)
    pos = src.get("position") or None
    if pos:      # 미청산 포지션은 마지막 관측가로 평가
        px = pos.get("last_price") or pos.get("entry_price") or 0
        equity = cash + float(pos.get("coin_qty") or 0.0) * float(px or 0)
    sells = [t for t in (src.get("recent_trades") or []) if t.get("type") == "SELL"]
    time_exits = sum(1 for t in sells if t.get("exit_kind") == "TIME")
    return {
        "key": key,
        "label": src.get("label") or STRATEGIES.get(key, {}).get("label", key),
        "name": src.get("name") or STRATEGIES.get(key, {}).get("name", key),
        "style": src.get("style"),
        "decision": decision,
        "diagnosis": diagnosis,
        "initial_capital": round(cap),
        "final_equity": round(equity),
        "ret_pct": round((equity - cap) / cap * 100, 2) if cap else 0.0,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(wins / trades * 100, 1) if trades else 0.0,
        "realized_pnl": round(float(src.get("total_realized_pnl") or 0.0)),
        "max_drawdown_pct": src.get("max_drawdown_pct"),
        "exit_kinds": _exit_kind_counts(sells),
        "recent_window": {
            "n": len(sells),
            "time_exit_pct": round(time_exits / len(sells) * 100, 0) if sells else None,
            "avg_gain_pct": round(sum(t.get("gain_pct", 0) for t in sells) / len(sells), 3) if sells else None,
            "sells": [{k: t.get(k) for k in ("exit_time", "market", "name", "gain_pct",
                                             "pnl_krw", "hold_min", "exit_kind")}
                      for t in sells[-20:]],
        },
        "open_position_at_archive": ({
            "market": pos.get("market"), "name": pos.get("name"),
            "entry_time": pos.get("entry_time"), "entry_price": pos.get("entry_price"),
            "price_at_archive": pos.get("last_price") or pos.get("entry_price"),
            "unrealized_pct": pos.get("unrealized_pct"),
        } if pos else None),
    }


def _build_v4_archive(old, now=None):
    """퇴출 3전략 + 재설계 4전략 + 승계 3전략의 v4.0 최종 성적표를 만든다."""
    old = old or {}
    strats = old.get("strategies") or {}
    rows = []
    for key in RETIRED_KEYS:
        if isinstance(strats.get(key), dict):
            rows.append(_v4_strategy_row(strats[key], key, "retired",
                                         RETIRED_DIAGNOSIS.get(key, "")))
    for key in REBUILT_KEYS:
        if isinstance(strats.get(key), dict):
            rows.append(_v4_strategy_row(strats[key], key, "rebuilt",
                                         REBUILT_DIAGNOSIS.get(key, "")))
    for key in ("3m", "10m", "panic_bounce"):
        if isinstance(strats.get(key), dict):
            rows.append(_v4_strategy_row(
                strats[key], key, "kept",
                "규칙을 바꾸지 않고 잔고·전적만 승계한 대조군. v5.0 개편 효과를 나란히 비교하기 위해 남긴다."))
    retired = [r for r in rows if r["decision"] == "retired"]
    return {
        "version": old.get("version"),
        "period": {"from": old.get("initialized_at"), "to": fmt(now or kst_now())},
        "archived_at": fmt(now or kst_now()),
        "league": {
            "initial_capital": old.get("initial_capital"),
            "final_equity": old.get("current_equity"),
            "ret_pct": old.get("current_ret_pct"),
            "trades": old.get("total_trades"),
            "wins": old.get("wins"),
            "losses": old.get("losses"),
            "realized_pnl": old.get("total_realized_pnl"),
        },
        "retired_totals": {
            "count": len(retired),
            "initial_capital": sum(r["initial_capital"] for r in retired),
            "final_equity": sum(r["final_equity"] for r in retired),
            "trades": sum(r["trades"] for r in retired),
            "realized_pnl": sum(r["realized_pnl"] for r in retired),
            "ret_pct": (round((sum(r["final_equity"] for r in retired)
                               - sum(r["initial_capital"] for r in retired))
                              / sum(r["initial_capital"] for r in retired) * 100, 2)
                        if retired else 0.0),
        },
        "note": ("v4.0 라이브 리그의 최종 성적표. 퇴출 3전략은 v5.0 리그에서 빠지고 그 손익은 "
                 "라이브 수익률에 합산되지 않는다(별도 보존). 재설계 4전략과 승계 3전략은 "
                 "잔고·전적을 그대로 이어받아 v5.0에서 계속 기록된다."),
        "fill_model": {
            "v4": "수수료 왕복 0.10% · 호가 스프레드 0(가정)",
            "v5": "수수료 왕복 0.10% · 호가 1틱(왕복) 반영",
            "note": "v4.0 수치는 스프레드를 빼고 계산돼 실제보다 낙관적이다.",
        },
        "strategies": rows,
    }


def migrate_from_v4(old):
    """v4.0 상태를 v5.0 리그로 옮긴다.

    · 3m / 10m / panic_bounce : 규칙·잔고·전적·포지션을 **그대로** 승계 (대조군)
    · 30m / pullback_reentry / vwap_breakout / bb_squeeze : 잔고·전적은 승계하고 규칙만 재설계.
      재설계 시점에 보유 중이던 포지션은 다음 틱에 시장가로 정리한다(규칙이 바뀐 포지션을
      새 규칙으로 억지 청산하지 않도록).
    · rsi_oversold / major_momentum / streak_ignition : 퇴출. v4_archive에 성적·진단을 보존.
    · box_breakout / alt_rs / tape_sweep : 200만 원 신규 시드.
    """
    state = new_league_state()
    old = old or {}
    old_strats = old.get("strategies") or {}
    carried, archived = [], []
    for key in STRATEGY_KEYS:
        src = old_strats.get(key)
        if not isinstance(src, dict):
            continue
        dst = state["strategies"][key]
        for field in CARRY_FIELDS:
            if field in src and src[field] is not None:
                dst[field] = src[field]
        if src.get("initial_capital"):
            dst["initial_capital"] = float(src["initial_capital"])
        dst["carried_over"] = True
        dst["carried_from_version"] = old.get("version")
        dst["rules_changed"] = key in REBUILT_KEYS
        if key in REBUILT_KEYS:
            dst["rules_effective_from"] = fmt(kst_now())
        for trade in dst.get("recent_trades") or []:
            if isinstance(trade, dict):
                trade.setdefault("strategy", key)
                trade.setdefault("strategy_label", dst["label"])
                trade.setdefault("strategy_name", dst["name"])
                if key in LEGACY_KEYS:
                    trade.setdefault("tier", key)
        if dst.get("position"):
            if key in REBUILT_KEYS:
                dst["position"]["force_exit_reason"] = (
                    f"v{VERSION} 규칙 재설계 — 이전 규칙 포지션 정리(시장가)")
                dst["last_event"] = "재설계 승계 — 보유 포지션을 다음 틱에 정리합니다"
            else:
                dst["status"] = "IN_POSITION"
                dst["last_event"] = f"v{old.get('version', '4.x')} 보유 포지션 그대로 승계"
        elif dst.get("total_trades"):
            dst["last_event"] = (f"v{old.get('version', '4.x')} 전적 {dst['total_trades']}전 "
                                 f"{dst.get('wins', 0)}승 {dst.get('losses', 0)}패 승계"
                                 + (" · 규칙 재설계" if key in REBUILT_KEYS else ""))
        else:
            dst["last_event"] = f"{dst['name']} — v{old.get('version', '4.x')} 기록 승계 (0전)"
        carried.append(key)

    for key in RETIRED_KEYS:
        if isinstance(old_strats.get(key), dict):
            archived.append(key)

    state["v4_archive"] = _build_v4_archive(old)
    retired_rows = [r for r in state["v4_archive"]["strategies"] if r["decision"] == "retired"]
    state["migrated_from"] = {
        "version": old.get("version"),
        "at": fmt(kst_now()),
        "carried_keys": carried,
        "rules_changed_keys": [k for k in carried if k in REBUILT_KEYS],
        "retired_keys": archived,
        "new_keys": [k for k in NEW_KEYS],
        "carried_trades": sum(int(state["strategies"][k].get("total_trades") or 0) for k in carried),
        "carried_equity": round(sum(float(state["strategies"][k].get("current_equity")
                                         or state["strategies"][k].get("cash_krw") or 0)
                                    for k in carried)),
        "retired_equity": round(sum(r["final_equity"] for r in retired_rows)),
        "retired_ret_pct": state["v4_archive"]["retired_totals"]["ret_pct"],
        "had_position": any(bool(state["strategies"][k].get("position")) for k in carried),
        "note": ("3계좌(3m·10m·panic_bounce)는 규칙·잔고 그대로, 4전략은 잔고·전적 승계 + 규칙 재설계, "
                 "3전략 퇴출(아카이브), 신규 3전략은 200만 원 신규 시드"),
    }
    print(f"[Engine] Migrated v{old.get('version')} -> v{VERSION}: carried {carried} "
          f"(retired {archived}, new {list(NEW_KEYS)})")
    return state


def migrate_from_v5(old):
    """v5.x → v5.2(정체 청산 철폐 + 목표 현실화) **무손실 승계**.

    · 10계좌의 잔고·전적·포지션·쿨다운·매매내역을 그대로 이어받는다(리셋 없음).
    · 규칙 파라미터·설명만 코드 정의(v5.2)로 교체한다. 파라미터가 실제로 바뀐 계좌는
      `rules_effective_from`을 승계 시각으로 옮겨 1일 거래 예산을 새 규칙부터 다시 센다
      (이전 규칙의 거래가 새 규칙의 하루 예산을 미리 태우지 않게 하는 기존 정책과 같다).
    · v4_archive(퇴출 계좌 성적표)·v4→v5 승계 이력·v5_fix(v5.0→v5.1 기록)는 보존하고,
      이번 승계 내역은 `v5_2_fix`에 남긴다.
    """
    state = new_league_state()
    old = old or {}
    old_strats = old.get("strategies") or {}
    now = fmt(kst_now())
    carried, changed = [], []
    for key in STRATEGY_KEYS:
        src = old_strats.get(key)
        dst = state["strategies"][key]
        if not isinstance(src, dict):
            continue
        for field in CARRY_FIELDS:
            if field in src and src[field] is not None:
                dst[field] = src[field]
        if src.get("initial_capital"):
            dst["initial_capital"] = float(src["initial_capital"])
        old_params = src.get("params") if isinstance(src.get("params"), dict) else {}
        if old_params != STRATEGIES[key]["params"]:
            changed.append(key)
            dst["rules_effective_from"] = now
        else:
            dst["rules_effective_from"] = src.get("rules_effective_from") or now
        dst["carried_over"] = bool(src.get("carried_over", True))
        dst["carried_from_version"] = src.get("carried_from_version") or old.get("version")
        dst["rules_changed"] = key in changed
        dst["rules_changed_in"] = VERSION if key in changed else src.get("rules_changed_in")
        if dst.get("position"):
            dst["status"] = "IN_POSITION"
        carried.append(key)

    state["v4_archive"] = old.get("v4_archive")
    state["migrated_from"] = old.get("migrated_from")
    state["initialized_at"] = old.get("initialized_at") or state["initialized_at"]
    if old.get("peak_equity") is not None:
        state["peak_equity"] = old["peak_equity"]
    if old.get("max_drawdown_pct") is not None:
        state["max_drawdown_pct"] = old["max_drawdown_pct"]

    def _equity_of(key):
        row = old_strats.get(key) or {}
        return float(row.get("current_equity") or row.get("cash_krw") or 0)

    state["v5_fix"] = old.get("v5_fix")
    state["v5_2_fix"] = {
        "from_version": old.get("version"),
        "to_version": VERSION,
        "at": now,
        "carried_keys": carried,
        "rules_changed_keys": changed,
        "carried_trades": sum(int((old_strats.get(k) or {}).get("total_trades") or 0)
                              for k in carried),
        "carried_equity": round(sum(_equity_of(k) for k in carried)),
        "carried_positions": [k for k in carried if (old_strats.get(k) or {}).get("position")],
        "changes": [
            "STAGNATION(6분 무풍 정체 청산) 로직·파라미터 철폐 — 손절/목표/트레일링/시간초과로만 청산",
            "3m 15초 거래대금 800만↑ · BID 78%↑ · 1일 10회 · 목표 +1.5%/트레일 +0.9%→-0.30%/손절 -1.3%",
            "10m 목표 +1.6%/트레일 +0.95%→-0.32%/손절 -1.35%",
            "pullback +1.7%/-1.4% · vwap +1.5%/-1.25% · bb +1.6%/-1.3% · box +1.4%/-1.2%",
            "alt_rs 디커플링 +1.3%p · 15초 700만↑ · BID 75%↑ · 1일 6회 · 목표 +1.6%",
            "panic_bounce 목표 +1.8% · 트레일 +1.2%→-0.35% · 손절 -1.25%",
            "30m · tape_sweep 규칙 유지 (1일 거래 예산 시계도 유지)",
        ],
        "note": ("잔고·전적·포지션·매매내역 무손실 승계. 파라미터가 바뀐 계좌만 1일 거래 예산 시계를 리셋"),
    }
    print(f"[Engine] Migrated v{old.get('version')} -> v{VERSION}: carried {len(carried)} accounts, "
          f"rules changed {changed}")
    return state


def ensure_league_state():
    """Gist/로컬 상태를 현재 리그(v5.2)로 맞춘다.

    - 이미 v5.2면 누락 전략만 채우고 규칙 정의를 최신화
    - v5.x(5.0·5.1)면 **잔고·전적 무손실 승계** (파라미터만 교체, `migrate_from_v5`)
    - v4.0이면 승계·재설계·퇴출 마이그레이션 (아카이브 생성)
    - 그 외/손상 상태면 10전략 × 200만 원 신규 시드
    - Gist가 설정돼 있는데 원격 로드에 실패한 경우에는 **절대 마이그레이션/원격 저장을 하지 않는다**
      (낡은 로컬 state.json으로 라이브 전적을 덮어쓰는 사고 방지). 엔진이 5초 간격으로 재시도한다.
    """
    state = load_state(force_remote=True)
    if gist_enabled() and last_load_source() != "gist":
        note = f"Gist 로드 실패(소스={last_load_source()}) — 승계 대기, 원격 저장 보류"
        if _ensure_guard["last_note"] != note:
            _ensure_guard["last_note"] = note
            print(f"[Engine] {note}")
        return state

    version = state.get("version")
    if version == VERSION and isinstance(state.get("strategies"), dict):
        missing = [k for k in STRATEGY_KEYS if k not in state["strategies"]]
        for k in missing:
            state["strategies"][k] = new_strategy(k)
        if missing:
            print(f"[Engine] Added missing strategies: {missing}")
        if state.get("rules") != RULES_V4:
            state["rules"] = RULES_V4
        for k in STRATEGY_KEYS:     # 규칙 텍스트/파라미터는 코드 정의를 따른다
            state["strategies"][k]["entry"] = STRATEGIES[k]["entry"]
            state["strategies"][k]["exit"] = STRATEGIES[k]["exit"]
            state["strategies"][k]["btc_filter"] = bool(STRATEGIES[k]["btc_filter"])
            state["strategies"][k]["role"] = STRATEGIES[k].get("role", "new")
            # 규칙 파라미터는 코드 정의를 따른다
            state["strategies"][k]["params"] = dict(STRATEGIES[k]["params"])
            state["strategies"][k].setdefault("symbol_cooldowns", {})
        state["strategy_order"] = list(STRATEGY_KEYS)
        state.setdefault("v4_archive", None)
        # 규칙 적용 시점: 이미 마이그레이션된 상태에는 승계 시각을 한 번만 채운다
        migrated_at = ((state.get("migrated_from") or {}).get("at")) or fmt(kst_now())
        for k in STRATEGY_KEYS:
            stk = state["strategies"][k]
            if STRATEGIES[k].get("role") in ("rebuilt", "new") and not stk.get("rules_effective_from"):
                stk["rules_effective_from"] = migrated_at
        _alias_legacy(state)
        _aggregate(state, None)
        if missing:
            save_state(state, remote=True, force_remote=True)
        return state

    if isinstance(state.get("strategies"), dict) and str(version or "").startswith("5"):
        new = migrate_from_v5(state)
    elif isinstance(state.get("strategies"), dict) and str(version or "").startswith("4"):
        new = migrate_from_v4(state)
    elif isinstance(state.get("tiers"), dict) and str(version or "").startswith("3"):
        # v3.x를 건너뛴 경우: v4와 동일하게 3계좌만 잔고 승계하고 나머지는 신규
        old4 = dict(state)
        old4["strategies"] = dict(state.get("tiers") or {})
        new = migrate_from_v4(old4)
    else:
        print(f"[Engine] State reset from v{version} -> v{VERSION} "
              f"({len(STRATEGY_KEYS)} strategies x {DEFAULT_SEED:,} KRW)")
        new = new_league_state()
    _aggregate(new, None)
    save_state(new, remote=True, force_remote=True)
    return new


# 하위 호환 (main.py·기존 테스트가 쓰던 이름)
ensure_v4_state = ensure_league_state


# ---------------------------------------------------------------- 지표 계산
def _position_price(pos, stream):
    price = stream.last_price(pos["market"]) if stream else None
    if price is None:
        price = _rest_price(pos["market"])
    return price


def _strategy_equity(st, stream):
    cash = float(st.get("cash_krw") or 0.0)
    pos = st.get("position")
    if not pos:
        return cash
    price = _position_price(pos, stream) or pos.get("entry_price", 0)
    return cash + float(pos.get("coin_qty", 0.0)) * float(price)


def _parse_dt(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    except Exception:
        return None


def _symbol_cooldown_until(st, market, now):
    """전략별 '동일 종목 재진입 쿨다운'의 남은 종료 시각 문자열 (없으면 None)."""
    boosts = st.get("symbol_cooldowns") or {}
    if not isinstance(boosts, dict):
        return None
    until = boosts.get(market)
    if not until:
        return None
    dt = _parse_dt(until)
    if dt is None or now >= dt:
        return None
    return until


def _trades_today(st, now=None, window=40):
    """오늘(KST) 청산한 거래 수 — 1일 거래 상한(수수료 예산) 판정용.

    `rules_effective_from`이 있으면 그 시점 이후 청산만 센다. 승계·재설계 직후에
    이전 규칙으로 한 거래가 새 규칙의 하루 예산을 미리 태우지 않게 하기 위해서다.
    """
    day = fmt(now or kst_now())[:10]
    since = st.get("rules_effective_from") or ""
    trades = (st.get("recent_trades") or [])[-window:]
    out = 0
    for t in trades:
        if not isinstance(t, dict) or t.get("type") != "SELL":
            continue
        exit_time = str(t.get("exit_time") or "")
        if exit_time[:10] != day:
            continue
        if since and exit_time < since:
            continue
        out += 1
    return out


def _prune_symbol_cooldowns(st, now):
    """만료된 종목 쿨다운 정리 (상태가 무한정 커지는 것 방지)."""
    boosts = st.get("symbol_cooldowns")
    if not isinstance(boosts, dict) or not boosts:
        return
    st["symbol_cooldowns"] = {m: u for m, u in boosts.items()
                              if _parse_dt(u) and _parse_dt(u) > now}


def _set_symbol_cooldown(st, market, now, exit_kind):
    """청산 직후 종목별 재진입 쿨다운 기록 (전략 파라미터를 따른다)."""
    p = st.get("params") or {}
    mins = p.get("symbol_cooldown_min")
    scope = p.get("symbol_cooldown_scope") or "stop"
    if not mins or not market:
        return None
    if scope == "stop" and exit_kind != "STOP_LOSS":
        return None
    if scope not in ("stop", "all"):
        return None
    until = fmt(now + timedelta(minutes=float(mins)))
    boosts = st.get("symbol_cooldowns")
    if not isinstance(boosts, dict):
        boosts = {}
    boosts[market] = until
    st["symbol_cooldowns"] = boosts
    return until


def _close_position(st, price, reason, now, exit_kind):
    pos = st["position"]
    p = st.get("params") or {}
    entry_price = float(pos["entry_price"])
    qty = float(pos["coin_qty"])
    # v5.1 체결 모델: 매도는 관측가 -0.5틱(bid 쪽). 목표 청산도 목표가에서 0.5틱을 뺀다.
    sell_price = fill_sell(price)
    net = qty * sell_price * (1 - FEE_RATE)
    pnl = net - float(pos["cost_krw"])
    gain_pct = (sell_price - entry_price) / entry_price * 100.0

    st["cash_krw"] = net
    st["total_realized_pnl"] = float(st.get("total_realized_pnl", 0.0)) + pnl
    st["total_trades"] = int(st.get("total_trades", 0)) + 1
    if pnl > 0:
        st["wins"] = int(st.get("wins", 0)) + 1
    else:
        st["losses"] = int(st.get("losses", 0)) + 1
        if exit_kind == "STOP_LOSS":
            mins = p.get("cooldown_min", 5)
            st["cooldown_until"] = fmt(now + timedelta(minutes=mins))

    st["recent_trades"].append({
        "type": "SELL",
        "strategy": st["key"],
        "strategy_label": st["label"],
        "strategy_name": st["name"],
        "tier": st["key"] if st["key"] in LEGACY_KEYS else None,
        "market": pos["market"],
        "name": pos.get("name", pos["market"]),
        "entry_time": pos.get("entry_time"),
        "exit_time": fmt(now),
        "entry_price": entry_price,
        "exit_price": round(sell_price, 8),
        "gain_pct": round(gain_pct, 2),
        "pnl_krw": round(pnl),
        "hold_min": round((_now_ms() - int(pos.get("entry_ts_ms") or _now_ms())) / 60000.0, 1),
        "reason": reason,
        "exit_kind": exit_kind,
        "fill_model": f"fee {2 * FEE_RATE * 100:.2f}% + spread {SPREAD_TICKS}t/leg",
        "cost_pct": round(cost_pct(entry_price), 3),
    })
    st["position"] = None
    st["status"] = "WAITING_FOR_SIGNAL"
    # 동일 종목 재진입 쿨다운 (rsi_oversold 30분 전 청산 / panic_bounce 15분 손절)
    if exit_kind != "STABLE_EXIT":
        _set_symbol_cooldown(st, pos["market"], now, exit_kind)
    return pnl, gain_pct


def _check_exit(st, stream, now):
    """보유 포지션 틱 단위 청산 판정. 반환: (exit_price, reason, exit_kind) 또는 None"""
    pos = st["position"]
    p = st.get("params") or {}
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
    held_min = (_now_ms() - int(pos.get("entry_ts_ms") or _now_ms())) / 60000.0

    stop_price = entry * (1 + p["stop_pct"] / 100.0)
    target_price = entry * (1 + p["target_pct"] / 100.0)

    # 1) 기계적 손절 (관측 가격 -0.5틱 체결)
    if price <= stop_price:
        return price, (f"🛑 기계적 손절 발동 ({gain:.2f}%) · "
                       f"{p.get('cooldown_min', 5)}분 쿨다운"), "STOP_LOSS"

    # 2) 목표 익절 (목표가 체결, 보수적)
    if price >= target_price:
        return target_price, f"🎯 목표 익절 달성 (+{p['target_pct']:.2f}%)", "TARGET"

    # 3) 트레일링 스탑 (설정된 전략만)
    if p.get("trail_activation_pct") is not None and high_gain >= p["trail_activation_pct"]:
        drop = (price - high) / high * 100.0
        if drop <= -p["trail_drop_pct"]:
            return price, (f"🛡️ 트레일링 스탑 청산 ({gain:+.2f}%, 고점 {high_gain:+.2f}% "
                           f"대비 {drop:.2f}%)"), "TRAILING"

    # 4) 최대 보유 시간 — v5.2부터 유일한 '시간 청산'이다 (정체 청산은 철폐).
    if held_min >= p["max_hold_min"]:
        return price, f"⏱️ {p['max_hold_min']}분 보유 제한 초과 청산 ({gain:+.2f}%)", "TIME"

    return None


# ---------------------------------------------------------------- 진입 신호
def _sig_surge(snap, st, ctx):
    """v3.1 계승: 15초 거래대금 폭발 + 매수 우위 + 초입 구간."""
    p = st["params"]
    if snap["coverage_sec"] < 60:
        return False, f"관측 {snap['coverage_sec']:.0f}s < 60s", 0.0
    ok, why = _market_gates(snap, st)      # v5.2: 목표폭이 왕복 실비용×5 미만인 종목은 추격 금지
    if not ok:
        return False, why, 0.0
    if snap["base_krw"] <= 0 or snap["base_ticks"] < 3:
        return False, f"기준 표본 부족({snap['base_ticks']}틱)", 0.0
    if snap["short_krw"] < float(p["min_short_krw"]):
        return False, (f"15초 거래대금 {snap['short_krw']/1e4:,.0f}만 < "
                       f"{p['min_short_krw']/1e4:,.0f}만"), snap["surge_ratio"]
    if snap["surge_ratio"] < p["min_surge_ratio"]:
        return False, f"폭발배수 {snap['surge_ratio']:.1f}x < {p['min_surge_ratio']:.1f}x", snap["surge_ratio"]
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", snap["surge_ratio"]
    if snap["chg_3m_pct"] < p["min_chg_pct"]:
        return False, f"3분변동 {snap['chg_3m_pct']:+.2f}% < +{p['min_chg_pct']}%", snap["surge_ratio"]
    if snap["chg_3m_pct"] > p["max_chg_pct"]:
        return False, f"3분변동 {snap['chg_3m_pct']:+.2f}% > +{p['max_chg_pct']}% (뒷북)", snap["surge_ratio"]
    return True, (f"15초 {snap['short_krw']/1e4:,.0f}만 · {snap['surge_ratio']:.1f}배 · "
                  f"매수 {snap['bid_ratio']*100:.0f}% · 3분 {snap['chg_3m_pct']:+.2f}%"), \
        snap["surge_ratio"] * 1e6 + snap["short_krw"] / 1e4


def _sig_panic_bounce(snap, st, ctx):
    """3분 -2.5% 급락 후 매수 우위 + 가격 반등."""
    p = st["params"]
    if snap["coverage_sec"] < 60:
        return False, f"관측 {snap['coverage_sec']:.0f}s < 60s", 0.0
    if snap["chg_3m_pct"] > p["min_drop_pct"]:
        return False, f"3분변동 {snap['chg_3m_pct']:+.2f}% > {p['min_drop_pct']}% (급락 아님)", -snap["chg_3m_pct"]
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", 0.0
    if snap["price_15s_ago"] is None or snap["last_price"] <= snap["price_15s_ago"]:
        return False, "직전 15초 대비 반등 미확인", 0.0
    reb = (snap["last_price"] - snap["price_15s_ago"]) / snap["price_15s_ago"] * 100.0
    return True, (f"3분 {snap['chg_3m_pct']:+.2f}% 급락 후 15초 반등 {reb:+.2f}% · "
                  f"매수 {snap['bid_ratio']*100:.0f}%"), reb


def _market_gates(snap, st):
    """v5.0 공통 진입 게이트 — 변동성 하한 · 유동성 · 왕복비용 대비 목표폭.

    지역시장(대형주)에 알트용 밴드를 씌우던 v4.0의 실패를 막는 장치다.
    · 10분 레인지가 하한 미만 → 그 종목은 보유 시간 안에 목표까지 못 간다 (대형주 자동 제외)
    · 목표폭 < 왕복 실비용 × cost_mult → 맞아도 수수료·호가에 먹힌다
    """
    p = st.get("params") or {}
    mr = p.get("min_range_10m_pct")
    if mr:
        rng = snap.get("range_10m_pct")
        if rng is None:
            return False, "10분 레인지 표본 없음"
        if rng < float(mr):
            return False, (f"10분 레인지 {rng:.2f}% < {mr:.2f}% "
                           f"(변동성 부족 — 비용 대비 손익비 불성립)")
    mt = p.get("min_ticks_10m")
    if mt:
        ticks = int(snap.get("ticks_10m") or 0)
        if ticks < int(mt):
            return False, f"10분 체결 {ticks}건 < {mt}건 (호가 얇음)"
    cm = p.get("cost_mult")
    if cm:
        c = cost_pct(snap.get("last_price"))
        tgt = float(p.get("target_pct") or 0)
        if c > 0 and tgt < float(cm) * c:
            return False, (f"목표 {tgt:.2f}% < 왕복비용 {c:.2f}%×{cm} "
                           f"(수수료·호가 벽 — 손익비 불성립)")
    return True, ""


def _sig_pullback(snap, st, ctx):
    """3분 상승 후 10분 고점 대비 눌림 + 매수 우위 + 상승 전환 (v5.0 변동성·비용 게이트)."""
    p = st["params"]
    if snap["coverage_10m_sec"] < p["min_coverage_10m_sec"]:
        return False, f"10분 고점 관측 {snap['coverage_10m_sec']:.0f}s < {p['min_coverage_10m_sec']}s", 0.0
    ok, why = _market_gates(snap, st)
    if not ok:
        return False, why, 0.0
    if snap["chg_3m_pct"] < float(p.get("min_chg_3m_pct") or 0.0):
        return False, (f"3분 상승 {snap['chg_3m_pct']:+.2f}% < +{p.get('min_chg_3m_pct')}% "
                       f"(눌릴 상승이 없음)"), 0.0
    dd = snap.get("drawdown_from_high_pct")
    if dd is None:
        return False, "고점 표본 없음", 0.0
    if dd > p["dd_max_pct"]:
        return False, f"고점 대비 {dd:+.2f}% (눌림 부족, 기준 {p['dd_max_pct']}%↓)", -dd
    if dd < p["dd_min_pct"]:
        return False, f"고점 대비 {dd:+.2f}% (눌림 과다, 기준 {p['dd_min_pct']}%↑)", -dd
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", 0.0
    if snap["price_15s_ago"] is None or snap["last_price"] <= snap["price_15s_ago"]:
        return False, "상승 전환 미확인", 0.0
    return True, (f"3분 {snap['chg_3m_pct']:+.2f}% 상승 후 고점 대비 {dd:+.2f}% 눌림 · "
                  f"매수 {snap['bid_ratio']*100:.0f}% · 15초 반등 확인"), snap["short_krw"] / 1e4


def _sig_vwap(snap, st, ctx):
    """직전 3분 VWAP 하단 → 거래대금 3배 + 매수 우위로 VWAP +0.35% 돌파 (v5.0)."""
    p = st["params"]
    if snap["coverage_sec"] < p["min_coverage_sec"]:
        return False, f"관측 {snap['coverage_sec']:.0f}s < {p['min_coverage_sec']}s", 0.0
    ok, why = _market_gates(snap, st)
    if not ok:
        return False, why, 0.0
    vwap = snap.get("vwap_3m")
    if not vwap:
        return False, "VWAP 표본 없음", 0.0
    bo = (snap["last_price"] - vwap) / vwap * 100.0
    if bo < p["breakout_pct"]:
        return False, f"VWAP 대비 {bo:+.2f}% < +{p['breakout_pct']}%", bo
    ago = snap.get("price_30s_ago")
    if ago is None or ago > vwap:
        return False, "직전 VWAP 하단 이력 없음 (돌파 아님)", bo
    if snap["surge_ratio"] < p["min_vol_ratio"]:
        return False, f"거래량 {snap['surge_ratio']:.1f}배 < {p['min_vol_ratio']:.1f}배", bo
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", bo
    return True, (f"VWAP {vwap:,.4f} → {bo:+.2f}% 돌파 · 거래량 {snap['surge_ratio']:.1f}배 · "
                  f"매수 {snap['bid_ratio']*100:.0f}%"), bo


def _sig_bb(snap, st, ctx):
    """볼린저(20,2) 수축 + 종가 > 중심선 + 상단 밴드 거래량 돌파 (v5.0 상승 국면 한정)."""
    p = st["params"]
    c = snap.get("candles")
    if not c:
        return False, "1분봉 캐시 대기 (거래대금 상위 15종목만 수집)", 0.0
    if c.get("age_sec", 999) > p["max_candle_age_sec"]:
        return False, f"1분봉 캐시 {c['age_sec']:.0f}s 지연", 0.0
    ok, why = _market_gates(snap, st)
    if not ok:
        return False, why, 0.0
    up, rank, bw = c.get("bb_upper"), c.get("bw_rank"), c.get("bandwidth_pct")
    mid = c.get("bb_mid")
    if up is None or rank is None:
        return False, "볼린저 표본 부족", 0.0
    score = snap["surge_ratio"]
    if p.get("require_above_mid") and mid:
        if (c.get("last_close") or 0) < float(mid):
            return False, (f"중심선 {float(mid):,.4f} 아래 (하락 국면 수축 — 돌파 신뢰도 낮음)"), score
    if rank > p["squeeze_rank"]:
        return False, (f"밴드 수축 아님 (폭 {bw:.2f}% · 최근 40봉 백분위 {rank*100:.0f}% > "
                       f"{p['squeeze_rank']*100:.0f}%)"), score
    if snap["last_price"] <= up:
        return False, f"상단 밴드 미돌파 ({snap['last_price']:,.4f} ≤ {up:,.4f})", score
    vol_ratio = c.get("vol_ratio")
    if snap["surge_ratio"] < p["min_vol_ratio"] and (vol_ratio or 0) < p["min_vol_ratio"]:
        return False, f"돌파 거래량 미달 ({snap['surge_ratio']:.1f}배)", score
    return True, (f"수축(백분위 {rank*100:.0f}%) 후 {up:,.4f} 돌파 · 중심선 위 · "
                  f"15초 거래대금 {snap['surge_ratio']:.1f}배"), score


def _sig_box(snap, st, ctx):
    """10분 박스권 수축 → 박스 고점 돌파 + 거래량·매수 우위 (신규).

    좁게 수축한 구간의 상단을 거래대금이 실리며 넘는 자리는 손절선(박스 하단)이
    가까워 손익비가 좋다. '이미 급등한 뒤 추격'을 막기 위해 15초 급등 상한을 둔다.
    """
    p = st["params"]
    if snap["coverage_10m_sec"] < p["min_coverage_10m_sec"]:
        return False, f"10분 박스 관측 {snap['coverage_10m_sec']:.0f}s < {p['min_coverage_10m_sec']}s", 0.0
    ok, why = _market_gates(snap, st)
    if not ok:
        return False, why, 0.0
    box = snap.get("box_range_pct")
    box_high = snap.get("high_10m_ex_short")
    low = snap.get("low_10m")
    if box is None or not box_high or not low:
        return False, "박스 표본 없음", 0.0
    if box > p["box_max_range_pct"]:
        return False, f"박스 폭 {box:.2f}% > {p['box_max_range_pct']}% (수축 아님)", 0.0
    bo = (snap["last_price"] - box_high) / box_high * 100.0
    if bo < p["breakout_pct"]:
        return False, f"박스 고점 대비 {bo:+.2f}% < +{p['breakout_pct']}%", bo
    chg15 = snap.get("chg_15s_pct")
    if chg15 is not None and chg15 > float(p.get("max_chg_15s_pct") or 99.0):
        return False, f"15초 {chg15:+.2f}% 급등 (추격 금지 — 박스 상단 이탈)", bo
    if snap["short_krw"] < p["min_short_krw"]:
        return False, (f"15초 거래대금 {snap['short_krw']/1e4:,.0f}만 < "
                       f"{p['min_short_krw']/1e4:,.0f}만"), bo
    if snap["surge_ratio"] < p["min_surge_ratio"]:
        return False, f"돌파 거래량 {snap['surge_ratio']:.1f}배 < {p['min_surge_ratio']:.1f}배", bo
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", bo
    return True, (f"박스 폭 {box:.2f}% 수축 → 고점 {bo:+.2f}% 돌파 · "
                  f"거래량 {snap['surge_ratio']:.1f}배 · 매수 {snap['bid_ratio']*100:.0f}%"), \
        bo * 10 + snap["surge_ratio"]


def _sig_alt_rs(snap, st, ctx):
    """알트-BTC 디커플링 상대강도 (신규).

    BTC가 제자리이거나 밀리는 동안 그 종목만 3분 수익률이 앞서는 구간은 자금 로테이션의
    흔적이다(BTC는 알트 변동의 절반만 설명하고 리더십이 순환한다). BTC 자체는 대상에서 뺀다.
    """
    p = st["params"]
    if p.get("exclude_btc") and snap.get("market") == "KRW-BTC":
        return False, "BTC 자신은 상대강도 대상 아님", -1e9
    if snap["coverage_sec"] < float(p.get("min_coverage_sec", 150)):
        return False, (f"3분 관측 {snap['coverage_sec']:.0f}s < "
                       f"{float(p.get('min_coverage_sec', 150)):.0f}s"), 0.0
    ok, why = _market_gates(snap, st)
    if not ok:
        return False, why, 0.0
    btc = ctx.get("btc_snap")
    if not btc or btc.get("chg_3m_pct") is None:
        return False, "BTC 스냅샷 없음 (상대강도 계산 불가)", 0.0
    btc_chg = float(btc.get("chg_3m_pct") or 0.0)
    rs = snap["chg_3m_pct"] - btc_chg
    if rs < p["min_rs_pct"]:
        return False, f"BTC 대비 {rs:+.2f}%p < +{p['min_rs_pct']}%p (디커플링 부족)", rs
    if snap["chg_3m_pct"] < p["min_chg_3m_pct"]:
        return False, f"3분 {snap['chg_3m_pct']:+.2f}% < +{p['min_chg_3m_pct']}%", rs
    if snap["short_krw"] < p["min_short_krw"]:
        return False, (f"15초 거래대금 {snap['short_krw']/1e4:,.0f}만 < "
                       f"{p['min_short_krw']/1e4:,.0f}만"), rs
    if snap["surge_ratio"] < p["min_surge_ratio"]:
        return False, f"거래량 {snap['surge_ratio']:.1f}배 < {p['min_surge_ratio']:.1f}배", rs
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", rs
    return True, (f"BTC 대비 {rs:+.2f}%p 강세 (3분 {snap['chg_3m_pct']:+.2f}% vs BTC {btc_chg:+.2f}%) · "
                  f"거래량 {snap['surge_ratio']:.1f}배 · 매수 {snap['bid_ratio']*100:.0f}%"), rs


def _sig_tape(snap, st, ctx):
    """대형 매수 체결 스파이크 = 체결 불균형 (신규).

    체결 방향 불균형은 1~5분 수익률 예측력이 있고 30분 안에 사라진다(학술·실무 공통).
    그래서 집계 비율이 아니라 '단일 대형 매수 체결'(스윕)을 보고, 보유 상한도 8분으로 짧다.
    """
    p = st["params"]
    if snap["coverage_sec"] < 60:
        return False, f"관측 {snap['coverage_sec']:.0f}s < 60s", 0.0
    ok, why = _market_gates(snap, st)
    if not ok:
        return False, why, 0.0
    mx = float(snap.get("max_bid_krw_10s") or 0.0)
    if mx < p["min_print_krw"]:
        return False, (f"10초 최대 매수 체결 {mx/1e4:,.0f}만 < "
                       f"{p['min_print_krw']/1e4:,.0f}만"), 0.0
    avg = snap.get("avg_tick_krw_3m")
    ratio = (mx / avg) if avg else 0.0
    if ratio < p["min_print_mult"]:
        return False, f"대형 체결 배수 {ratio:.1f}x < {p['min_print_mult']:.1f}x", ratio
    if int(snap.get("ticks_10s") or 0) < p["min_ticks_10s"]:
        return False, f"10초 체결 {int(snap.get('ticks_10s') or 0)}건 < {p['min_ticks_10s']}건", ratio
    chg15 = snap.get("chg_15s_pct")
    if chg15 is None or chg15 < p["min_chg_15s_pct"]:
        shown = "없음" if chg15 is None else f"{chg15:+.2f}%"
        return False, f"15초 가격 {shown} < +{p['min_chg_15s_pct']}% (체결 후 상승 미확인)", ratio
    if snap["bid_ratio_10s"] < p["min_bid_10s"]:
        return False, f"10초 매수비중 {snap['bid_ratio_10s']*100:.0f}% < {p['min_bid_10s']*100:.0f}%", ratio
    return True, (f"10초 최대 매수 체결 {mx/1e4:,.0f}만 ({ratio:.1f}x) · 15초 {chg15:+.2f}% · "
                  f"10초 매수 {snap['bid_ratio_10s']*100:.0f}%"), ratio * snap["short_krw"] / 1e6


def _sig_range_rev(snap, st, ctx):
    """대형주 TOP10 10분 레인지 눌림 역추세 (30m 재설계).

    대형주는 20분 안에 2%를 못 간다. 대신 10분 레인지의 하단(-0.35~-1.2%)에서 매수 우위가
    나오는 자리는 되돌림 폭이 작아 0.9%/0.6% 밴드로도 손익비가 성립한다.
    """
    p = st["params"]
    if snap["market"] not in ctx["top_markets"]:
        return False, f"24h 거래대금 TOP{p['top_n']} 대형주 아님", 0.0
    if snap["coverage_10m_sec"] < p["min_coverage_10m_sec"]:
        return False, f"10분 관측 {snap['coverage_10m_sec']:.0f}s < {p['min_coverage_10m_sec']}s", 0.0
    ok, why = _market_gates(snap, st)
    if not ok:
        return False, why, 0.0
    dd = snap.get("drawdown_from_high_pct")
    if dd is None:
        return False, "고점 표본 없음", 0.0
    if dd > p["dd_max_pct"]:
        return False, (f"고점 대비 {dd:+.2f}% (눌림 부족, {p['dd_min_pct']}~{p['dd_max_pct']}% 필요)"), -dd
    if dd < p["dd_min_pct"]:
        return False, f"고점 대비 {dd:+.2f}% (하락 과다 — 떨어지는 칼날)", -dd
    if snap["bid_ratio"] < p["min_bid_ratio"]:
        return False, f"매수비율 {snap['bid_ratio']*100:.0f}% < {p['min_bid_ratio']*100:.0f}%", 0.0
    ago = snap.get("price_15s_ago")
    if not ago:
        return False, "15초 가격 이력 없음", 0.0
    reb = (snap["last_price"] - ago) / ago * 100.0
    if reb < p["min_rebound_pct"]:
        return False, f"15초 반등 {reb:+.2f}% < +{p['min_rebound_pct']}%", reb
    return True, (f"TOP{p['top_n']} 레인지 눌림 {dd:+.2f}% · 15초 반등 {reb:+.2f}% · "
                  f"매수 {snap['bid_ratio']*100:.0f}%"), reb


SIGNAL_FNS = {
    "surge": _sig_surge,
    "panic_bounce": _sig_panic_bounce,
    "pullback": _sig_pullback,
    "vwap": _sig_vwap,
    "bb": _sig_bb,
    "box": _sig_box,
    "alt_rs": _sig_alt_rs,
    "tape": _sig_tape,
    "range_rev": _sig_range_rev,
}


def _attach_candles(snap):
    if _candle_cache is not None:
        snap["candles"] = _candle_cache.snapshot(snap["market"])
    else:
        snap["candles"] = None
    return snap


def _evaluate_strategy(key, snaps, ctx):
    """전략 하나를 전체 시장 스냅샷에 적용. (최적 진입 후보, 최적 근접 후보) 반환.

    · 스테이블코인 등 제외 마켓은 판정 전에 걸러낸다 (원천 차단)
    · 전략별 '동일 종목 재진입 쿨다운' 중인 종목도 후보에서 제외한다
    """
    st = ctx["state"]["strategies"][key]
    now = ctx.get("now") or kst_now()
    fn = SIGNAL_FNS[STRATEGIES[key]["kind"]]
    best_ok = best_any = None
    for snap in snaps:
        market = snap.get("market")
        if is_excluded_market(market):
            continue
        if _symbol_cooldown_until(st, market, now):
            continue
        ok, why, score = fn(snap, st, ctx)
        if ok:
            if best_ok is None or score > best_ok[0]:
                best_ok = (score, snap, why)
        elif best_any is None or score > best_any[0]:
            best_any = (score, snap, why)
    return best_ok, best_any


def _enter(st, snap, now, stream, why):
    market = snap["market"]
    if is_excluded_market(market):      # 이중 안전장치 — 제외 마켓은 절대 체결하지 않는다
        return None
    name = (stream.names().get(market) if stream else None) or market.replace("KRW-", "")
    invest = float(st["cash_krw"])
    if invest <= 0:
        return None
    ref_price = float(snap["last_price"])
    if ref_price <= 0:
        return None
    price = fill_buy(ref_price)        # v5.1: 관측 체결가 + 0.5틱(ask)로 산다
    qty = (invest * (1 - FEE_RATE)) / price
    st["position"] = {
        "market": market,
        "name": name,
        "entry_price": price,
        "ref_price": ref_price,
        "cost_pct": round(cost_pct(price), 3),
        "highest_price": price,
        "coin_qty": qty,
        "cost_krw": invest,
        "entry_time": fmt(now),
        "entry_ts_ms": _now_ms(),
        "entry_type": f"WS_{st['kind'].upper()}",
        "strategy": st["key"],
        "strategy_label": st["label"],
        "signal": why,
        "surge_ratio": round(snap["surge_ratio"], 2),
        "bid_ratio": round(snap["bid_ratio"], 3),
        "chg_3m_pct": round(snap["chg_3m_pct"], 2),
        "short_krw": round(snap["short_krw"]),
        "last_price": price,
        "unrealized_pct": 0.0,
    }
    st["cash_krw"] = 0.0
    st["status"] = "IN_POSITION"
    st["recent_trades"].append({
        "type": "BUY",
        "strategy": st["key"],
        "strategy_label": st["label"],
        "strategy_name": st["name"],
        "tier": st["key"] if st["key"] in LEGACY_KEYS else None,
        "market": market,
        "name": name,
        "entry_time": fmt(now),
        "entry_price": price,
        "ref_price": ref_price,
        "cost_pct": round(cost_pct(price), 3),
        "cost_krw": round(invest),
        "surge_ratio": round(snap["surge_ratio"], 2),
        "bid_ratio": round(snap["bid_ratio"], 3),
        "short_krw": round(snap["short_krw"]),
        "chg_3m_pct": round(snap["chg_3m_pct"], 2),
        "reason": f"⚡ {why}",
    })
    return market


def _watch_line(st, best_any, snap_count):
    if not snap_count:
        return "🔍 웹소켓 틱 수신 대기", None
    if not best_any:
        return "🔍 신호 대기", None
    _, snap, why = best_any
    name = snap["market"].replace("KRW-", "")
    return f"🔍 {name} — {why}", snap


def _aggregate(state, stream):
    """10전략 합산 + 수익률 리더보드."""
    total_equity = total_capital = cash = pnl = 0.0
    trades = wins = losses = 0
    merged = []
    in_position = cooling = 0
    board = []

    for key in STRATEGY_KEYS:
        st = state["strategies"][key]
        equity = _strategy_equity(st, stream)
        capital = float(st.get("initial_capital") or DEFAULT_SEED)
        st["current_equity"] = round(equity)
        st["current_ret_pct"] = round(((equity - capital) / capital) * 100, 2) if capital else 0.0
        st["peak_equity"] = round(max(float(st.get("peak_equity") or capital), equity))
        dd = ((st["peak_equity"] - equity) / st["peak_equity"] * 100) if st["peak_equity"] else 0.0
        st["max_drawdown_pct"] = round(max(float(st.get("max_drawdown_pct") or 0.0), dd), 2)
        st["initial_capital"] = capital
        st["entry"] = STRATEGIES[key]["entry"]
        st["exit"] = STRATEGIES[key]["exit"]
        st["btc_filter"] = bool(STRATEGIES[key]["btc_filter"])
        st["role"] = STRATEGIES[key].get("role", st.get("role"))
        st["params"] = dict(STRATEGIES[key]["params"])      # 규칙 파라미터는 코드 정의를 따른다
        if not isinstance(st.get("symbol_cooldowns"), dict):
            st["symbol_cooldowns"] = {}
        st["symbol_cooldown_markets"] = sorted(st["symbol_cooldowns"])
        if len(st.get("recent_trades", [])) > 40:
            st["recent_trades"] = st["recent_trades"][-40:]

        total_equity += equity
        total_capital += capital
        cash += float(st.get("cash_krw") or 0.0)
        pnl += float(st.get("total_realized_pnl") or 0.0)
        trades += int(st.get("total_trades") or 0)
        wins += int(st.get("wins") or 0)
        losses += int(st.get("losses") or 0)
        if st.get("position"):
            in_position += 1
        elif st.get("cooldown_until"):
            cooling += 1
        merged.extend(st.get("recent_trades", []))
        tr = int(st.get("total_trades") or 0)
        board.append({
            "key": key,
            "label": st["label"],
            "name": st["name"],
            "style": st["style"],
            "carried_over": bool(st.get("carried_over")),
            "role": STRATEGIES[key].get("role"),
            "equity": round(equity),
            "ret_pct": st["current_ret_pct"],
            "trades": tr,
            "wins": int(st.get("wins") or 0),
            "losses": int(st.get("losses") or 0),
            "win_rate_pct": round((int(st.get("wins") or 0) / tr) * 100, 1) if tr else 0.0,
            "realized_pnl": round(float(st.get("total_realized_pnl") or 0.0)),
            "status": st.get("status"),
            "in_position": bool(st.get("position")),
            "position_market": (st.get("position") or {}).get("market"),
            "position_name": (st.get("position") or {}).get("name"),
            "unrealized_pct": (st.get("position") or {}).get("unrealized_pct"),
            "last_event": st.get("last_event"),
            "symbol_cooldowns": dict(st.get("symbol_cooldowns") or {}),
        })

    board.sort(key=lambda r: (-r["ret_pct"], -r["equity"], r["key"]))
    for i, row in enumerate(board, 1):
        row["rank"] = i
    state["leaderboard"] = board

    merged.sort(key=lambda t: (t.get("exit_time") or t.get("entry_time") or ""))
    state["recent_trades"] = merged[-120:]

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
    dd = ((state["peak_equity"] - total_equity) / state["peak_equity"] * 100) \
        if state["peak_equity"] else 0.0
    state["max_drawdown_pct"] = round(max(float(state.get("max_drawdown_pct") or 0.0), dd), 2)
    if in_position:
        state["status"] = "IN_POSITION"
    elif cooling:
        state["status"] = "COOLDOWN"
    else:
        state["status"] = "WAITING_FOR_SIGNAL"
    state["cooldown_until"] = max(
        [state["strategies"][k]["cooldown_until"] for k in STRATEGY_KEYS
         if state["strategies"][k].get("cooldown_until")] or [None], key=lambda x: x or "")
    state["active_positions"] = in_position
    state["leader"] = board[0]["key"] if board else None
    state["laggard"] = board[-1]["key"] if board else None
    _alias_legacy(state)
    return state


def run_sniper_tick(stream=None):
    state = load_state()

    # ---- 안전장치: Gist가 설정된 환경에서는 원격 상태를 실제로 읽었을 때만 매매/저장한다 ----
    # (원격 로드 실패 → 로컬 폴백 상태로 체결하면 낡은 데이터로 라이브 전적을 덮어쓸 수 있다)
    if gist_enabled() and last_load_source() != "gist":
        now_ts = time.time()
        if (now_ts - _ensure_guard["ts"]) >= 5.0:
            _ensure_guard["ts"] = now_ts
            state = ensure_v4_state()
        if gist_enabled() and last_load_source() != "gist":
            return {"status": "STATE_SYNC_WAIT",
                    "event": "Gist 원격 상태 복구 대기 — 매매·저장 중단 (로컬 폴백 상태 사용 금지)",
                    "equity": state.get("current_equity"),
                    "ret_pct": state.get("current_ret_pct"),
                    "trades": state.get("total_trades")}

    if state.get("version") != VERSION or not isinstance(state.get("strategies"), dict):
        now_ts = time.time()
        if (now_ts - _ensure_guard["ts"]) >= 5.0:
            _ensure_guard["ts"] = now_ts
            state = ensure_v4_state()
        if state.get("version") != VERSION or not isinstance(state.get("strategies"), dict):
            return {"status": "MIGRATION_WAIT", "event": f"v{VERSION} 승계 대기 (Gist 로드 재시도 중)",
                    "equity": state.get("current_equity"), "ret_pct": state.get("current_ret_pct"),
                    "trades": state.get("total_trades")}

    rules = state.get("rules") or RULES_V4
    if rules != RULES_V4:
        state["rules"] = RULES_V4
    now = kst_now()
    now_ms = _now_ms()
    state["last_checked_time"] = fmt(now)
    events = []
    exited_now = set()

    # ---------- 1. 전략별 보유 포지션 청산 감시 ----------
    for key in STRATEGY_KEYS:
        st = state["strategies"][key]
        if not st.get("position"):
            continue
        pos = st["position"]
        # v5.0 규칙 재설계로 승계된 포지션은 새 청산 규칙을 적용하지 않고 시장가로 정리한다
        if pos.get("force_exit_reason"):
            price = _position_price(pos, stream)
            if not price:
                st["last_event"] = "⏳ 재설계 포지션 정리 대기 — 시세 수신 중"
                continue
            pnl, gain = _close_position(
                st, price, f"♻️ {pos['force_exit_reason']}", now, "RESET_EXIT")
            st["last_event"] = (f"♻️ {pos.get('name', pos['market'])}({pos['market']}) "
                                f"규칙 재설계 정리 {gain:+.2f}% / {pnl:+,.0f}원")
            events.append(f"[{st['label']}] {st['last_event']}")
            exited_now.add(key)
            continue
        # 제외 마켓(스테이블코인) 보유 포지션은 규칙 평가 없이 정상 정리한다
        if is_excluded_market(pos.get("market")):
            price = _position_price(pos, stream)
            if not price:
                st["last_event"] = "⏳ 스테이블코인 포지션 정리 대기 — 시세 수신 중"
                continue
            pnl, gain = _close_position(
                st, price, "🧹 제외 마켓(스테이블코인) 정리 청산", now, "STABLE_EXIT")
            st["last_event"] = (f"🧹 {pos.get('name', pos['market'])}({pos['market']}) "
                                f"스테이블코인 제외 → 정리 청산 {gain:+.2f}% / {pnl:+,.0f}원")
            events.append(f"[{st['label']}] {st['last_event']}")
            exited_now.add(key)
            continue
        hit = _check_exit(st, stream, now)
        if hit:
            price, reason, kind = hit
            pnl, gain = _close_position(st, price, reason, now, kind)
            st["last_event"] = f"SELL {st['recent_trades'][-1]['market']} {gain:+.2f}% / {pnl:+,.0f}원"
            events.append(f"[{st['label']}] {st['last_event']}")
            exited_now.add(key)
        else:
            pos = st["position"]
            cur = _position_price(pos, stream)
            gain = ((cur - pos["entry_price"]) / pos["entry_price"] * 100.0) if cur else 0.0
            pos["unrealized_pct"] = round(gain, 2)
            st["last_event"] = f"HOLDING {pos.get('name')}({pos['market']}) {gain:+.2f}%"

    # ---------- 2. 공유 시장 스냅샷 (10전략이 같은 데이터를 동시에 판단) ----------
    btc = _btc_trend_cached()
    state["btc_trend"] = btc
    bearish = bool(RULES_V4["btc_filter"]["enabled"] and btc.get("is_bearish"))

    snaps = []
    if stream is not None:
        for market in stream.markets():
            snap = stream.market_snapshot(market, now_ms)
            if snap:
                snaps.append(_attach_candles(snap))

    top_markets = set(stream.top_markets(10)) if (stream is not None and hasattr(stream, "top_markets")) else set()
    btc_snap = next((s for s in snaps if s.get("market") == "KRW-BTC"), None)
    ctx = {"state": state, "top_markets": top_markets, "btc": btc, "bearish": bearish,
           "btc_snap": btc_snap, "now": now}

    # ---------- 3. 전략별 신규 진입 ----------
    for key in STRATEGY_KEYS:
        st = state["strategies"][key]
        if st.get("position") or key in exited_now:
            continue

        _prune_symbol_cooldowns(st, now)

        if st.get("cooldown_until"):
            try:
                cd = datetime.strptime(st["cooldown_until"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
                if now < cd:
                    rem = int((cd - now).total_seconds())
                    st["status"] = "COOLDOWN"
                    st["last_event"] = f"⏳ 손절 쿨다운 {rem // 60}분 {rem % 60}초 남음"
                    continue
                st["cooldown_until"] = None
            except Exception:
                st["cooldown_until"] = None

        exempt = not bool(STRATEGIES[key]["btc_filter"])
        if bearish and not exempt:
            st["status"] = "BTC_FILTER_BLOCKED"
            st["last_event"] = (f"🛑 BTC 15분봉 20이평 하회 ({btc.get('diff_pct', 0):+.2f}%) "
                                f"— 신규 진입 차단")
            continue

        st["status"] = "WAITING_FOR_SIGNAL"
        if stream is None:
            continue

        cap = st["params"].get("max_trades_per_day")
        if cap and _trades_today(st, now) >= int(cap):
            st["status"] = "DAILY_CAP"
            st["last_event"] = (f"🧾 1일 거래 상한 {int(cap)}회 도달 — 수수료 예산 소진, "
                                f"신규 진입 중단 (내일 리셋)")
            continue

        best_ok, best_any = _evaluate_strategy(key, snaps, ctx)
        if best_ok:
            _, snap, why = best_ok
            market = _enter(st, snap, now, stream, why)
            if market:
                st["last_event"] = f"⚡ BUY {market} — {why}"
                events.append(f"[{st['label']}] {st['last_event']}")
                continue
        line, watch = _watch_line(st, best_any, len(snaps))
        if bearish and exempt:
            line = (f"♻️ 역추세 예외 — BTC 하락({btc.get('diff_pct', 0):+.2f}%)에도 진입 허용 · "
                    f"{line}")
        st["last_event"] = line
        st["watch"] = {
            "market": watch["market"],
            "name": (stream.names().get(watch["market"]) or watch["market"]).replace("KRW-", ""),
            "short_krw": round(watch["short_krw"]),
            "surge_ratio": round(watch["surge_ratio"], 2),
            "bid_ratio": round(watch["bid_ratio"], 3),
            "reason": best_any[2] if best_any else None,
        } if watch else None

    # ---------- 4. 실시간 감시 패널 (스냅샷 재사용) ----------
    solid = [s for s in snaps if s["base_krw"] > 0 and s["base_ticks"] >= 3
             and not is_excluded_market(s.get("market"))]
    display_pool = [s for s in solid if s["short_krw"] >= 3_000_000] or \
        sorted(solid, key=lambda x: x["short_krw"], reverse=True)[:6]
    top = sorted(display_pool, key=lambda x: x["surge_ratio"], reverse=True)[:6]
    names = stream.names() if stream else {}
    scan_rows = []
    for s in top:
        qualified = [k for k in STRATEGY_KEYS
                     if SIGNAL_FNS[STRATEGIES[k]["kind"]](s, state["strategies"][k], ctx)[0]]
        scan_rows.append({
            "market": s["market"],
            "name": (names.get(s["market"]) or s["market"]).replace("KRW-", ""),
            "price": s["last_price"],
            "short_krw": round(s["short_krw"]),
            "surge_ratio": round(s["surge_ratio"], 2),
            "bid_ratio": round(s["bid_ratio"], 3),
            "chg_3m_pct": round(s["chg_3m_pct"], 2),
            "drawdown_from_high_pct": round(s["drawdown_from_high_pct"], 2)
            if s.get("drawdown_from_high_pct") is not None else None,
            "qualified": bool(qualified),
            "qualified_strategies": qualified,
            "qualified_tiers": [k for k in qualified if k in LEGACY_KEYS],
        })
    state["live_scan"] = {
        "scanned": len(snaps),
        "qualified": len([r for r in scan_rows if r["qualified"]]),
        "top_markets": sorted(top_markets)[:10],
        "top": scan_rows,
    }

    # ---------- 5. 보유 종목은 구독 유지 ----------
    if stream is not None and hasattr(stream, "add_extra_markets"):
        try:
            stream.add_extra_markets([state["strategies"][k]["position"]["market"]
                                      for k in STRATEGY_KEYS if state["strategies"][k].get("position")])
        except Exception as e:
            print(f"[Engine] extra markets update failed: {e}")

    # ---------- 6. 집계 ----------
    state = _aggregate(state, stream)
    if events:
        state["last_event"] = " | ".join(events)
    else:
        lead = state["leaderboard"][0] if state.get("leaderboard") else None
        state["last_event"] = (
            f"10전략 대기 · 선두 {lead['name']}({lead['ret_pct']:+.2f}%)" if lead
            else "10전략 대기")

    trade_event = any(("BUY" in e or "SELL" in e) for e in events)
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
        "leader": state.get("leader"),
        "leaderboard": [{"rank": r["rank"], "key": r["key"], "ret_pct": r["ret_pct"]}
                        for r in state.get("leaderboard", [])],
    }


if __name__ == "__main__":
    from candles import CandleCache
    from ws_stream import UpbitTradeStream
    st = UpbitTradeStream()
    st.start()
    cache = CandleCache(lambda: st.markets(), top_n=15)
    cache.start()
    set_candle_cache(cache)
    secs = float(os.environ.get("V4_TEST_SECONDS", "25"))
    t_end = time.time() + secs
    while time.time() < t_end:
        print(json.dumps(run_sniper_tick(st), ensure_ascii=False, default=str))
        time.sleep(TICK_INTERVAL_SEC)
    print("WS STATS:", json.dumps(st.stats(), ensure_ascii=False))
    print("CANDLES:", json.dumps(cache.stats(), ensure_ascii=False))
    cache.stop()
    st.stop()
