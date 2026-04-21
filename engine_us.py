"""
engine_us.py — 미장 숏스퀴즈 스캐너
실행: python engine_us.py
스케줄: 07:00 KST (미장 종료 후)
"""
import os, json, datetime
import numpy as np
import yfinance as yf
from zoneinfo import ZoneInfo                           # [FIX] KST

from engine_common import (
    ko_date, now_label, send_telegram, fetch_macro_summary,
    build_news_briefing, calc_rsi,                      # [FIX] calc_rsi import — 인라인 SMA 제거
    json_safe                                           # [FIX] numpy bool 직렬화
)

KST = ZoneInfo("Asia/Seoul")

DATA_FILE_US = 'stock_data_us.json'

WATCHLIST = [
    'CAR','HTZ','GRPN',
    'UPST','SOFI','AFRM','OPEN','HIMS',
    'PLTR','AI','SNAP','MRVL','ASTS','IONQ','ACHR',
    'CLF','PBF','DK','LYB','DOW',
    'OCGN','NVAX','SAVA','PACB',
    'GME','AMC','MVIS','TLRY','KOSS','SPCE',
]


def run_us_scan():
    print("\n🇺🇸 미국 숏스퀴즈 스캐닝 시작...")
    today_str = datetime.datetime.now(KST).strftime("%Y%m%d")  # [FIX] UTC→KST
    us_picks = []; skipped = 0

    for symbol in WATCHLIST:
        try:
            t = yf.Ticker(symbol)

            # 가격/거래량 데이터
            hist = t.history(period="3mo")
            if hist is None or len(hist) < 20:
                print(f"  ⚠️  {symbol} 데이터 없음")
                skipped += 1; continue

            cur_price  = round(float(hist['Close'].iloc[-1]), 2)
            avg_vol_20 = hist['Volume'].iloc[-21:-1].mean()
            cur_vol    = hist['Volume'].iloc[-1]
            vol_spike  = round(float(cur_vol / (avg_vol_20 + 1)), 2)  # [FIX] np.float64 → float

            # [FIX] RSI — engine_common calc_rsi(EWM) 통일, 인라인 SMA 제거
            rsi = round(float(calc_rsi(hist['Close']).iloc[-1]), 1)

            # [FIX] fast_info 우선 → info fallback (속도 개선 + rate limit 방지)
            float_shares = short_pct = short_name = None
            short_ratio = 0
            fi = t.fast_info
            float_shares = getattr(fi, 'shares_outstanding', None)  # fast_info 근사값

            try:
                info = t.info
                float_shares = info.get('floatShares') or info.get('impliedSharesOutstanding') or float_shares
                short_pct    = info.get('shortPercentOfFloat')
                short_ratio  = info.get('shortRatio') or 0
                short_name   = info.get('shortName', symbol)
                long_summary = info.get('longBusinessSummary', '')
            except Exception:
                info = {}
                short_name   = symbol
                long_summary = ''

            float_m     = float(float_shares) / 1e6 if float_shares else 0  # [FIX] np scalar → float
            short_pct_p = float(short_pct) * 100    if short_pct    else 0  # [FIX] np scalar → float

            print(f"  📌 {symbol} | 거래량 {vol_spike}x | 공매도 {round(short_pct_p,1)}% | float {round(float_m,1)}M")

            # 채점
            short_score = min((short_pct_p - 10) / 30 * 35 + 5, 40) if short_pct_p >= 10 else 0
            vol_score   = min((vol_spike - 1.0) / 5 * 25 + 5, 30)   if vol_spike >= 1.0 else 0
            float_score = max(20 - (float_m / 100 * 20), 0)          if 0 < float_m < 100 else 5
            ratio_score = min(float(short_ratio) * 1.5, 10)           if short_ratio else 0  # [FIX]
            total_score = int(short_score + vol_score + float_score + ratio_score)

            short_str = f"{round(short_pct_p,1)}%" if short_pct_p > 0 else "N/A"
            float_str = f"{round(float_m,1)}M"     if float_m > 0     else "N/A"

            squeeze_level = (
                "🔥 EXTREME" if total_score >= 70 else
                "⚡ HIGH"    if total_score >= 45 else
                "📈 MEDIUM"  if total_score >= 20 else
                "📊 LOW"
            )

            us_picks.append({
                "rank": len(us_picks) + 1,
                "name": short_name, "code": symbol,
                "company_summary": (long_summary[:150] + '...') if long_summary else symbol,
                "supply": f"공매도 {short_str} | {squeeze_level}",
                "cur_price": cur_price,
                "score": total_score,                           # [FIX] 정수 저장 — 문자열 파싱 제거
                "score_detail": {
                    "공매도강도": int(short_score), "거래량급증": int(vol_score),
                    "유통주희소": int(float_score), "커버소요일": int(ratio_score),
                },
                "tags": f"유통주 {float_str} · 숏비율 {short_str} · RSI {rsi}",
                "expected_return": "HIGH RISK",
                "meta": {
                    "float_m":    round(float_m, 1),
                    "short_pct":  round(short_pct_p, 1),
                    "vol_spike":  vol_spike,
                    "rsi":        rsi,
                    "short_ratio": round(float(short_ratio), 1),  # [FIX] np scalar → float
                }
            })

        except Exception as e:
            print(f"  ⚠️  {symbol} 오류: {e}")
            skipped += 1

    # [FIX] 정수 score로 직접 정렬 — 문자열 파싱 크래시 제거
    us_picks.sort(key=lambda x: x['score'], reverse=True)
    top5 = us_picks[:5]
    for i, p in enumerate(top5): p['rank'] = i + 1

    print(f"\n🏁 미국 완료! 전체 {len(us_picks)}건 -> TOP {len(top5)}")
    for p in top5:
        print(f"  #{p['rank']} {p['name']} | SQUEEZE {p['score']}점/100")

    # 저장용 score는 표시 문자열로 변환
    for p in us_picks:
        p['score'] = f"SQUEEZE {p['score']}점/100"

    us_output = {
        "today_picks": top5, "total_candidates": len(us_picks),
        "total_screened": len(WATCHLIST) - skipped, "base_date": today_str,
    }
    with open(DATA_FILE_US, 'w', encoding='utf-8') as f:
        json.dump(us_output, f, ensure_ascii=False, indent=4, default=json_safe)  # [FIX]

    return us_output


def build_us_message(us_data: dict) -> str:
    picks     = us_data.get("today_picks", [])
    screened  = us_data.get("total_screened", 0)
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


if __name__ == "__main__":
    us_result = run_us_scan()
    send_telegram(fetch_macro_summary())
    send_telegram(build_news_briefing())
    send_telegram(build_us_message(us_result))

