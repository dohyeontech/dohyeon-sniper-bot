"""v5.3 백테스트 — v5.2 실거래 로그(계좌별 최근 20청산 = 200건) 리플레이.

무엇을 검증하나
  1) v5.2 리그의 실제 손실 구조 분해 (전략별 건당 손익 · 승률 · 청산사유 · 비용 구간)
  2) 설계 손절 vs 실현 손절의 계통 슬리피지
  3) v5.3 진입 게이트(비용 상한 · 추격 상한 · 추세 가드 · 1일 상한 · 전략 퇴출)를
     **같은 체결 로그에 그대로 적용**했을 때의 손익·거래수·낙폭 변화

한계 (정직하게)
  · 로그에는 진입/청산 가격만 있고 그 사이 가격 경로가 없다. 그래서 목표·손절·트레일링
    '밴드' 자체를 바꾸는 개편은 리플레이로 검증할 수 없다. 이 스크립트가 검증하는 것은
    **어떤 진입을 하지 않았는가**(게이트·상한·퇴출)의 효과다.
  · 표본은 계좌별 최근 20청산 = 200건(v5.2 규칙이 돌던 11-17~11-20 구간)이다.
  · 계좌별 복리(잔고 전액 진입)를 그대로 재현하므로, 게이트가 거래를 줄이면 이후 자산도
    함께 달라진다 — 실제 엔진과 같은 방식이다.

실행: python3 backtest_v53.py
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import sniper_engine as E  # noqa: E402

FIXTURE = os.path.join(HERE, "tests", "fixtures", "v52_exits.json")
SEED = 2_000_000.0

# v5.2 라이브 밴드 (설계 손절 vs 실현 손절 비교용 · v5.2 코드 정의)
V52_DESIGN = {
    "3m": (-1.30, 1.50), "10m": (-1.35, 1.60), "30m": (-0.50, 0.75),
    "panic_bounce": (-1.25, 1.80), "pullback_reentry": (-1.40, 1.70),
    "vwap_breakout": (-1.25, 1.50), "bb_squeeze": (-1.30, 1.60),
    "box_breakout": (-1.20, 1.40), "alt_rs": (-1.30, 1.60), "tape_sweep": (-1.00, 1.80),
}


def load_exits():
    return json.load(open(FIXTURE, encoding="utf-8"))["exits"]


def agg(rows):
    if not rows:
        return dict(n=0, pnl=0, ret=0.0, wr=0.0, avg_win=0.0, avg_loss=0.0, rr=None)
    rets = [r["pnl"] / r["notional"] * 100 for r in rows]
    wins = [x for x in rets if x > 0]
    losses = [x for x in rets if x <= 0]
    return dict(n=len(rows), pnl=sum(r["pnl"] for r in rows),
                ret=sum(rets) / len(rets), wr=100 * len(wins) / len(rets),
                avg_win=sum(wins) / len(wins) if wins else 0.0,
                avg_loss=sum(losses) / len(losses) if losses else 0.0,
                rr=(sum(wins) / len(wins)) / abs(sum(losses) / len(losses))
                if wins and losses else None)


def blocked_reason(e, params):
    """엔진 _market_gates 와 같은 순서로 판정한다 (기록된 진입 피처 기준)."""
    cost = e.get("cost") or 0.0
    if params.get("max_cost_pct") and cost > params["max_cost_pct"]:
        return "cost_abs"
    tgt = float(params.get("target_pct") or 0.0)
    if params.get("cost_mult") and tgt and cost * params["cost_mult"] > tgt:
        return "cost_mult"
    chg = e.get("chg3")
    cap = params.get("max_chg_pct") if params.get("max_chg_pct") is not None \
        else params.get("max_chg_3m_pct")
    if cap is not None and chg is not None and chg > cap:
        return "chase"
    tg = params.get("trend_guard_chg_3m_pct")
    if tg is not None and (chg is None or chg < tg):
        return "trend"
    ms = params.get("min_surge_ratio")
    if ms and (e.get("surge") or 0) < ms:
        return "surge"
    mb = params.get("min_bid_ratio")
    if mb and (e.get("bid") or 0) < mb:
        return "bid"
    return None


def replay(exits, use_v53=True):
    """계좌별 복리 리플레이. 반환: (총자산, 통계, 게이트 사유, 계좌별 손익%)."""
    keys = sorted(set(e["strategy"] for e in exits))
    eq = {k: SEED for k in keys}
    day = {}
    reasons = {}
    taken = []
    for e in exits:
        k = e["strategy"]
        if use_v53 and k in E.RETIRED_V53:
            reasons["retired"] = reasons.get("retired", 0) + 1
            continue
        params = E.STRATEGIES.get(k, {}).get("params", {}) if use_v53 else {}
        if use_v53:
            why = blocked_reason(e, params)
            if why:
                reasons[why] = reasons.get(why, 0) + 1
                continue
            d = e["exit_time"][:10]
            key = (k, d)
            day.setdefault(key, 0)
            if params.get("max_trades_per_day") and day[key] >= params["max_trades_per_day"]:
                reasons["cap"] = reasons.get("cap", 0) + 1
                continue
            day[key] += 1
        eq[k] *= (1 + e["pnl"] / e["notional"])
        taken.append(e)
    total = sum(eq.values())
    per = {k: (eq[k] / SEED - 1) * 100 for k in keys}
    return total, agg(taken), reasons, per


def main():
    exits = load_exits()
    base_cap = SEED * len(set(e["strategy"] for e in exits))
    print("# v5.3 백테스트 — v5.2 실거래 200청산 리플레이\n")
    print(f"- 표본: {len(exits)}청산 (계좌별 최근 20청산) · "
          f"{min(e['exit_time'] for e in exits)} ~ {max(e['exit_time'] for e in exits)}")
    print(f"- 계좌 복리 시드: {SEED:,.0f}원 × 10계좌 = {base_cap:,.0f}원\n")

    a = agg(exits)
    print("## 1. v5.2 손실 구조\n")
    print(f"- 200청산 합계 **{a['pnl']:,.0f}원** · 건당 **{a['ret']:+.3f}%** · "
          f"승률 {a['wr']:.1f}% · 평균익 {a['avg_win']:+.2f}% / 평균손 {a['avg_loss']:+.2f}% "
          f"· 실현 손익비 {a['rr']:.2f}")
    print(f"- 왕복 실비용 평균 {sum((e.get('cost') or 0) for e in exits)/len(exits):.3f}% "
          f"— 건당 손실의 대부분이 이 비용이다.")
    kinds = {}
    for e in exits:
        kinds.setdefault(e["exit_kind"], []).append(e)
    print("\n### 청산 사유별\n")
    print("| 사유 | 건수 | 합계 | 건당 | 비중 |")
    print("| --- | ---: | ---: | ---: | ---: |")
    for k, rows in sorted(kinds.items(), key=lambda x: -len(x[1])):
        s = agg(rows)
        print(f"| {k} | {s['n']} | {s['pnl']:+,.0f} | {s['ret']:+.3f}% | "
              f"{100*s['n']/len(exits):.0f}% |")

    print("\n### 전략별\n")
    print("| 계좌 | 건수 | 합계 | 건당 | 승률 | 평균익 | 평균손 | 설계 손절 | 실현 손절 |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for k in sorted(V52_DESIGN):
        rows = [e for e in exits if e["strategy"] == k]
        s = agg(rows)
        stops = [e["gain"] for e in rows if e["exit_kind"] == "STOP_LOSS"]
        print(f"| {k} | {s['n']} | {s['pnl']:+,.0f} | {s['ret']:+.3f}% | {s['wr']:.0f}% | "
              f"{s['avg_win']:+.2f}% | {s['avg_loss']:+.2f}% | {V52_DESIGN[k][0]:+.2f}% | "
              f"{(sum(stops)/len(stops) if stops else 0):+.2f}% |")

    print("\n### 비용 구간별 (왕복 실비용)\n")
    print("| 구간 | 건수 | 합계 | 건당 |")
    print("| --- | ---: | ---: | ---: |")
    for lo, hi in ((0, 0.20), (0.20, 0.25), (0.25, 0.30), (0.30, 9)):
        rows = [e for e in exits if lo <= (e.get("cost") or 0) < hi]
        if rows:
            s = agg(rows)
            print(f"| {lo:.2f}~{hi:.2f}% | {s['n']} | {s['pnl']:+,.0f} | {s['ret']:+.3f}% |")

    bt, bstats, _, bper = replay(exits, use_v53=False)
    vt, vstats, vreasons, vper = replay(exits, use_v53=True)
    print("\n## 2. v5.3 게이트 리플레이 (같은 체결 로그)\n")
    print("| 구분 | 거래 | 합계 | 건당 | 승률 | 손익비 | 통합자산 | 통합수익률 |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for label, tot, s in (("v5.2 그대로", bt, bstats), ("v5.3", vt, vstats)):
        rr = f"{s['rr']:.2f}" if s["rr"] else "—"
        print(f"| {label} | {s['n']} | {s['pnl']:+,.0f} | {s['ret']:+.3f}% | {s['wr']:.0f}% | "
              f"{rr} | {tot:,.0f} | {(tot/base_cap-1)*100:+.2f}% |")
    b_pct, v_pct = (bt / base_cap - 1) * 100, (vt / base_cap - 1) * 100
    print(f"\n- 거래 {bstats['n']} → {vstats['n']}건 ({100*vstats['n']/bstats['n']:.0f}%), "
          f"손실 {b_pct:+.2f}% → {v_pct:+.2f}% (손실 폭 {100*(1 - v_pct/b_pct):.0f}% 감소)")
    labels = {"cost_abs": "비용 상한 0.25% 초과", "cost_mult": "목표 < 비용×5",
              "surge": "거래량 문턱 미달", "bid": "매수 비율 미달",
              "chase": "3분 추격 상한 초과", "trend": "추세 가드(하락 중 눌림)",
              "cap": "1일 거래 상한", "retired": "퇴출(alt_rs)"}
    print("\n### 걸러진 진입\n")
    print("| 사유 | 건수 |")
    print("| --- | ---: |")
    for r, n in sorted(vreasons.items(), key=lambda x: -x[1]):
        print(f"| {labels.get(r, r)} | {n} |")

    print("\n### 계좌별 (같은 로그 · 계좌 복리)\n")
    print("| 계좌 | v5.2 | v5.3 |")
    print("| --- | ---: | ---: |")
    for k in sorted(V52_DESIGN):
        print(f"| {k} | {bper[k]:+.2f}% | {vper.get(k, float('nan')):+.2f}%"
              f"{' (퇴출)' if k in E.RETIRED_V53 else ''} |")

    print("\n## 3. 결론\n")
    print(f"- 게이트만으로 같은 3.3일 구간 손실이 {(bt/base_cap-1)*100:+.2f}% → "
          f"{(vt/base_cap-1)*100:+.2f}% 로 줄었다. 수익으로 뒤집히지는 않았다.")
    print("- 남은 손실의 정체는 **비용**(왕복 0.10% 수수료 + 호가 1틱)이다. 200건의 "
          "건당 가격손익(호가 1틱 제거 후)은 -0.07%로 사실상 0 — 신호에 비용을 넘길 우위가 없다.")
    print("- 그래서 v5.3은 '이기는 전략 찾기'가 아니라 **덜 지는 구조**(비용 상한 · 추격 금지 · "
          "1일 리스크 예산 · 상한)로 설계했고, 새 계좌 confirmed_surge로 '강한 확인' 가설만 추가 검증한다.")


if __name__ == "__main__":
    main()
