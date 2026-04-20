"""
engine_us.py — 미장 숏스퀴즈 스캐너
실행: python engine_us.py
스케줄: 07:00 KST (미장 종료 후)
"""
import os, json, datetime
import numpy as np
import yfinance as yf

from engine_common import (
    ko_date, now_label, send_telegram, fetch_macro_summary,
    build_news_briefing
)



DATA_FILE_US = 'stock_data_us.json'


# ── 워치리스트 (2026년 4월 기준 실제 고공매도 활성 종목) ────────

WATCHLIST = [
    # 공매도 비율 TOP — 렌터카/여행
    'CAR','HTZ','GRPN',
    # 핀테크/성장주
    'UPST','SOFI','AFRM','OPEN','HIMS',
    # 기술/AI
    'PLTR','AI','SNAP','MRVL','ASTS','IONQ','ACHR',
    # 에너지/소재
    'CLF','PBF','DK','LYB','DOW',
    # 바이오
    'OCGN','NVAX','SAVA','PACB',
    # 밈주식 생존종목
        'GME','AMC','MVIS','TLRY','KOSS','SPCE',
]


# ══════════════════════════════════════════════════════════════
#  메인 스캔
# ══════════════════════════════════════════════════════════════

def run_us_scan():
    print("\n🇺🇸 미국 숏스퀴즈 스캐닝 시작...")
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    us_picks  = []; skipped = 0

    for symbol in WATCHLIST:
        try:
            t = yf.Ticker(symbol)

            # 가격 데이터 먼저
            hist = t.history(period="3mo")
            if hist is None or len(hist) < 20:
                skipped += 1; continue

            cur_price  = round(float(hist['Close'].iloc[-1]), 2)
            avg_vol_20 = hist['Volume'].iloc[-21:-1].mean()
            cur_vol    = hist['Volume'].iloc[-1]
            vol_spike  = round(cur_vol / (avg_vol_20 + 1), 2)

            # RSI
            delta = hist['Close'].diff()
            gain  = delta.where(delta > 0, 0).rolling(14).mean()
            loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs    = gain / loss.replace(0, np.nan)
            rsi   = round(float((100 - 100 / (1 + rs)).iloc[-1]), 1)

            # info 조회
            try:    info = t.info
            except: info = {}

            float_shares = info.get('floatShares') or info.get('impliedSharesOutstanding')
            short_pct    = info.get('shortPercentOfFloat')
            short_ratio  = info.get('shortRatio') or 0
            short_name   = info.get('shortName', symbol)

            float_m     = float_shares / 1e6 if float_shares else 999
            short_pct_p = short_pct * 100     if short_pct    else 0

            # ── 숏스퀴즈 3대 핵심 필터 ──────────────────────────
            # ① 유통주 100M 이하 (소형주 — 커버링 물량 부족)
            if float_m > 100 and float_m < 999: continue
            # ② 공매도 비율 15% 이상 (강제 청산 압력 충분)
            if short_pct_p < 15: continue
            # ③ 거래량 스파이크 1.5배 이상
            if vol_spike < 1.5: continue

            # ── 스퀴즈 점수 (100점 만점) ─────────────────────────
            # 공매도 강도 (최대 40점): 비율이 높을수록
            short_score = min((short_pct_p - 20) / 40 * 35 + 5, 40)
            # 거래량 폭발 (최대 30점): 스파이크 클수록
            vol_score   = min((vol_spike - 2.0) / 8 * 25 + 5, 30)
            # 유통주 희소성 (최대 20점): 적을수록
            float_score = max(20 - (float_m / 50 * 20), 0) if float_m < 999 else 0
            # 커버 소요일 (최대 10점): 길수록 압박 강함
            ratio_score = min(short_ratio * 1.5, 10) if short_ratio else 0
            total_score = int(short_score + vol_score + float_score + ratio_score)

            if total_score < 40: continue

            squeeze_level = (
                "🔥 EXTREME" if total_score >= 80 else
                "⚡ HIGH"    if total_score >= 60 else
                "📈 MEDIUM"
            )
            short_str = f"{round(short_pct_p,1)}%" if short_pct_p > 0 else "데이터없음"
            float_str = f"{round(float_m,1)}M"     if float_m < 999  else "데이터없음"

            us_picks.append({
                "rank": len(us_picks) + 1,
                "name": short_name, "code": symbol,
                "company_summary": (info.get('longBusinessSummary','')[:180]+'...')
                                   if info.get('longBusinessSummary') else symbol,
                "supply": f"공매도 {short_str} | {squeeze_level}",
                "cur_price": cur_price,
                "score": f"SQUEEZE {total_score}점/100",
                "score_detail": {
                    "공매도강도": int(short_score), "거래량급증": int(vol_score),
                    "유통주희소": int(float_score), "커버소요일": int(ratio_score),
                },
                "tags": f"유통주 {float_str} · 숏비율 {short_str} · RSI {rsi}",
                "expected_return": "EXPLOSIVE",
                "meta": {
                    "float_m": round(float_m,1) if float_m < 999 else 0,
                    "short_pct": round(short_pct_p,1), "vol_spike": vol_spike,
                    "rsi": rsi, "short_ratio": round(short_ratio,1),
                }
            })
            print(f"  ✅ {symbol} | {total_score}점 | 공매도 {short_str} | 거래량 {vol_spike}x")

        except Exception as e:
            print(f"  ⚠️  {symbol} 스킵: {e}")
            skipped += 1

    us_picks.sort(key=lambda x: int(x['score'].split()[1].replace('점/100','')), reverse=True)
    top5 = us_picks[:5]
    for i, p in enumerate(top5): p['rank'] = i + 1

    us_output = {
        "today_picks": top5, "total_candidates": len(us_picks),
        "total_screened": len(WATCHLIST) - skipped, "base_date": today_str,
    }
    with open(DATA_FILE_US, 'w', encoding='utf-8') as f:
        json.dump(us_output, f, ensure_ascii=False, indent=4)

    print(f"🏁 미국 완료! 후보 {len(us_picks)}건 → TOP {len(top5)}\n")
    return us_output


# ── 텔레그램 메시지 ────────────────────────────────────────────

def build_us_message(us_data: dict) -> str:
    picks    = us_data.get("today_picks", [])
    screened = us_data.get("total_screened", 0)
    base_date = us_data.get("base_date", "")

    lines = [
        f"🇺🇸 <b>미장 숏스퀴즈 — {ko_date(base_date)} 장 종료 후</b>",
        f"📋 워치리스트 {screened}종목 스캔",
        "━" * 24,
    ]
    if not picks:
        lines.append("⚠️ 오늘 조건 충족 종목 없음")
    else:
        for p in picks:
            meta = p.get("meta", {})
            lines += [
                f"<b>#{p['rank']} {p['name']} (${p['code']})</b>",
                f"점수: {p.get('score','')}",
                f"현재가: ${p.get('cur_price',0)}",
                f"공매도: {meta.get('short_pct',0)}% | 유통주: {meta.get('float_m',0)}M",
                f"거래량스파이크: {meta.get('vol_spike',0)}배 | RSI: {meta.get('rsi',0)}",
                f"숏커버소요: {meta.get('short_ratio',0)}일",
                f"{p.get('supply','')}",
                "━" * 24,
            ]
    return "\n".join(lines)


# ── 실행 ───────────────────────────────────────────────────────

if __name__ == "__main__":
    us_result = run_us_scan()
    send_telegram(fetch_macro_summary())
    send_telegram(build_news_briefing())
    send_telegram(build_us_message(us_result))
