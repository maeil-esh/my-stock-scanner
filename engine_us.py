"""
engine_us.py — 미장 숏스퀴즈 스캐너
실행: python engine_us.py
스케줄: 07:00 KST (미장 종료 후)
"""
import os, json, datetime
import numpy as np
import yfinance as yf
from zoneinfo import ZoneInfo

from engine_common import (
    ko_date, now_label, send_telegram, fetch_macro_summary,
    build_news_briefing, calc_rsi,
    json_safe
)

KST = ZoneInfo("Asia/Seoul")

DATA_FILE_US = 'stock_data_us.json'
MAX_SCORE_US = 100  # 공매도강도(40)+거래량급증(30)+유통주희소(20)+커버소요일(10)

WATCHLIST = [
    'CAR','HTZ','GRPN',
    'UPST','SOFI','AFRM','OPEN','HIMS',
    'PLTR','AI','SNAP','MRVL','ASTS','IONQ','ACHR',
    'CLF','PBF','DK','LYB','DOW',
    'OCGN','NVAX','SAVA','PACB',
    'GME','AMC','MVIS','TLRY','KOSS','SPCE',
]


# ══════════════════════════════════════════════════════════════
#  100점 환산 + 등급 이모티콘
# ══════════════════════════════════════════════════════════════

def normalize_score(score, max_score):
    if max_score <= 0:
        return 0
    return int(round(score / max_score * 100))


def grade_emoji(score_100):
    if score_100 >= 90: return "🟢🟢🟢"
    if score_100 >= 70: return "🟢🟢"
    if score_100 >= 50: return "🟡"
    if score_100 >= 30: return "🟠"
    return "🔴"


def score_to_bar(score_100):
    """100점 → 시각화 바 (10칸)"""
    filled = int(score_100 / 10)
    return '█' * filled + '░' * (10 - filled)


def squeeze_stars(score_100):
    star_count = min(score_100 // 20, 5)
    return "★" * star_count + "☆" * (5 - star_count)


# ══════════════════════════════════════════════════════════════
#  메인 스캔
# ══════════════════════════════════════════════════════════════

def run_us_scan():
    print("\n🇺🇸 미국 숏스퀴즈 스캐닝 시작...")
    today_str = datetime.datetime.now(KST).strftime("%Y%m%d")
    us_picks = []; skipped = 0

    for symbol in WATCHLIST:
        try:
            t = yf.Ticker(symbol)

            hist = t.history(period="3mo")
            if hist is None or len(hist) < 20:
                print(f"  ⚠️  {symbol} 데이터 없음")
                skipped += 1; continue

            cur_price  = round(float(hist['Close'].iloc[-1]), 2)
            avg_vol_20 = hist['Volume'].iloc[-21:-1].mean()
            cur_vol    = hist['Volume'].iloc[-1]
            vol_spike  = round(float(cur_vol / (avg_vol_20 + 1)), 2)
            rsi        = round(float(calc_rsi(hist['Close']).iloc[-1]), 1)

            float_shares = short_pct = short_name = None
            short_ratio = 0
            fi = t.fast_info
            float_shares = getattr(fi, 'shares_outstanding', None)

            try:
                info         = t.info
                float_shares = info.get('floatShares') or info.get('impliedSharesOutstanding') or float_shares
                short_pct    = info.get('shortPercentOfFloat')
                short_ratio  = info.get('shortRatio') or 0
                short_name   = info.get('shortName', symbol)
                long_summary = info.get('longBusinessSummary', '')
            except Exception:
                info         = {}
                short_name   = symbol
                long_summary = ''

            float_m     = float(float_shares) / 1e6 if float_shares else 0
            short_pct_p = float(short_pct) * 100    if short_pct    else 0

            print(f"  📌 {symbol} | 거래량 {vol_spike}x | 공매도 {round(short_pct_p,1)}% | float {round(float_m,1)}M")

            # 채점
            short_score = min((short_pct_p - 10) / 30 * 35 + 5, 40) if short_pct_p >= 10 else 0
            vol_score   = min((vol_spike - 1.0) / 5 * 25 + 5, 30)   if vol_spike >= 1.0 else 0
            float_score = max(20 - (float_m / 100 * 20), 0)          if 0 < float_m < 100 else 5
            ratio_score = min(float(short_ratio) * 1.5, 10)           if short_ratio else 0
            total_score = int(short_score + vol_score + float_score + ratio_score)

            # 100점 환산
            score_100 = normalize_score(total_score, MAX_SCORE_US)

            short_str = f"{round(short_pct_p,1)}%" if short_pct_p > 0 else "N/A"
            float_str = f"{round(float_m,1)}M"     if float_m > 0     else "N/A"

            squeeze_level = (
                "🔥 EXTREME" if score_100 >= 70 else
                "⚡ HIGH"    if score_100 >= 45 else
                "📈 MEDIUM"  if score_100 >= 20 else
                "📊 LOW"
            )

            us_picks.append({
                "rank":            len(us_picks) + 1,
                "name":            short_name,
                "code":            symbol,
                "company_summary": (long_summary[:100] + '...') if long_summary else symbol,
                "cur_price":       cur_price,
                "score_raw":       total_score,
                "score_100":       score_100,
                "squeeze_level":   squeeze_level,
                "score_detail": {
                    "공매도강도":   int(short_score),
                    "거래량급증":   int(vol_score),
                    "유통주희소":   int(float_score),
                    "커버소요일":   int(ratio_score),
                },
                "meta": {
                    "float_m":     round(float_m, 1),
                    "short_pct":   round(short_pct_p, 1),
                    "vol_spike":   vol_spike,
                    "rsi":         rsi,
                    "short_ratio": round(float(short_ratio), 1),
                    "short_str":   short_str,
                    "float_str":   float_str,
                }
            })

        except Exception as e:
            print(f"  ⚠️  {symbol} 오류: {e}")
            skipped += 1

    us_picks.sort(key=lambda x: x['score_100'], reverse=True)
    top5 = us_picks[:5]
    for i, p in enumerate(top5):
        p['rank'] = i + 1

    print(f"\n🏁 미국 완료! 전체 {len(us_picks)}건 → TOP {len(top5)}")
    for p in top5:
        bar = score_to_bar(p['score_100'])
        print(f"  #{p['rank']} {p['name']} | {bar} {p['score_100']}점 | {p['squeeze_level']}")

    us_output = {
        "today_picks":      top5,
        "total_candidates": len(us_picks),
        "total_screened":   len(WATCHLIST) - skipped,
        "base_date":        today_str,
    }
    with open(DATA_FILE_US, 'w', encoding='utf-8') as f:
        json.dump(us_output, f, ensure_ascii=False, indent=4, default=json_safe)

    return us_output


# ══════════════════════════════════════════════════════════════
#  텔레그램 메시지 조립
# ══════════════════════════════════════════════════════════════

def build_us_message(us_data: dict) -> str:
    picks     = us_data.get("today_picks", [])
    screened  = us_data.get("total_screened", 0)
    base_date = us_data.get("base_date", "")

    lines = [
        f"🇺🇸 <b>미장 숏스퀴즈 TOP {len(picks)} — {ko_date(base_date)} 장 종료 후</b>",
        f"📋 워치리스트 {screened}종목 스캔",
        "━" * 24,
    ]

    if not picks:
        lines.append("⚠️ 오늘 조건 충족 종목 없음")
    else:
        for p in picks:
            meta      = p.get("meta", {})
            sd        = p.get("score_detail", {})
            score_100 = p.get("score_100", 0)
            bar       = score_to_bar(score_100)
            stars     = squeeze_stars(score_100)

            short_100 = normalize_score(sd.get("공매도강도", 0), 40)
            vol_100   = normalize_score(sd.get("거래량급증", 0), 30)
            float_100 = normalize_score(sd.get("유통주희소", 0), 20)
            ratio_100 = normalize_score(sd.get("커버소요일", 0), 10)

            lines += [
                f"<b>#{p['rank']} {p['name']} (${p['code']})</b>",
                f"  {p.get('company_summary', '')}",
                f"",
                f"  <code>{bar}</code> <b>{score_100}점</b> {stars}",
                f"  {p.get('squeeze_level', '')}",
                f"",
                f"  💰 현재가: <b>${p.get('cur_price', 0)}</b>",
                f"",
                f"  📊 <b>숏스퀴즈 지표</b>",
                f"    {grade_emoji(short_100)} 공매도 강도   {short_100}점  ({meta.get('short_pct', 0)}%)",
                f"    {grade_emoji(vol_100)}   거래량 급증   {vol_100}점   ({meta.get('vol_spike', 0)}배)",
                f"    {grade_emoji(float_100)} 유통주 희소   {float_100}점  ({meta.get('float_str', 'N/A')})",
                f"    {grade_emoji(ratio_100)} 숏커버 소요   {ratio_100}점  ({meta.get('short_ratio', 0)}일)",
                f"",
                f"  📐 RSI: {meta.get('rsi', 0)}",
                "━" * 24,
            ]

    lines.append("💡 <i>숏스퀴즈는 고위험 — 손절 규율 필수</i>")
    return "\n".join(lines)


if __name__ == "__main__":
    us_result = run_us_scan()
    send_telegram(fetch_macro_summary())
    send_telegram(build_news_briefing())
    send_telegram(build_us_message(us_result))
