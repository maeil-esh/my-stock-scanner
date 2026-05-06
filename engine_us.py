"""
engine_us.py — 미장 숏스퀴즈 스캐너 (REGIME v2.0)
실행: python engine_us.py
스케줄: 07:43 KST (미장 종료 후)

[v2.0 변경]
  ① NASDAQ/Russell/VIX 기반 시장 위험선호 판단 추가
  ② TOP 5 강제 추천 → 조건 충족 후보만 today_picks 저장
  ③ 공매도·커버소요일·거래량·가격확인 하드 필터 추가
  ④ 조건 미충족 종목은 watch_picks로 분리
  ⑤ stock_data_us.json 구조 확장: today_picks / watch_picks / regime_info
"""
import json
import datetime
import numpy as np
import yfinance as yf
from zoneinfo import ZoneInfo

from engine_common import (
    ko_date, send_telegram, fetch_macro_summary,
    build_news_briefing, calc_rsi, json_safe
)

KST = ZoneInfo("Asia/Seoul")

DATA_FILE_US = 'stock_data_us.json'
MAX_SCORE_US = 100  # 공매도강도(40)+거래량급증(30)+유통주희소(20)+커버소요일(10)
US_TOP_N = 5
WATCH_TOP_N = 5

# 최소 추천 기준
MIN_BUY_SCORE = 45
MIN_SHORT_PCT = 20.0
MIN_SHORT_RATIO = 3.0
MIN_VOL_SPIKE = 1.0
VIX_RISK_OFF = 25.0

WATCHLIST = [
    'CAR','HTZ','GRPN',
    'UPST','SOFI','AFRM','OPEN','HIMS',
    'PLTR','AI','SNAP','MRVL','ASTS','IONQ','ACHR',
    'CLF','PBF','DK','LYB','DOW',
    'OCGN','NVAX','SAVA','PACB',
    'GME','AMC','MVIS','TLRY','KOSS','SPCE',
]

REGIME_LABELS = {
    "RISK_ON": "위험선호 ON",
    "NEUTRAL": "중립/선별장",
    "RISK_OFF": "위험회피 OFF",
    "UNKNOWN": "판단불가",
}


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        if isinstance(v, float) and np.isnan(v):
            return default
        return float(v)
    except Exception:
        return default


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
    filled = max(0, min(int(score_100 / 10), 10))
    return '█' * filled + '░' * (10 - filled)


def squeeze_stars(score_100):
    star_count = max(0, min(score_100 // 20, 5))
    return "★" * star_count + "☆" * (5 - star_count)


# ══════════════════════════════════════════════════════════════
#  미국 시장 국면 판단
# ══════════════════════════════════════════════════════════════

def _index_state(symbol: str, period: str = "3mo") -> dict:
    try:
        hist = yf.Ticker(symbol).history(period=period)
        if hist is None or len(hist) < 30:
            return {"symbol": symbol, "state": "UNKNOWN"}

        close = hist['Close']
        cur = _safe_float(close.iloc[-1])
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma20_v = _safe_float(ma20.iloc[-1])
        ma20_5 = _safe_float(ma20.iloc[-5]) if len(ma20) >= 5 else ma20_v
        ma50_v = _safe_float(ma50.iloc[-1]) if len(ma50) >= 50 else ma20_v

        ret5 = (cur / _safe_float(close.iloc[-5], cur) - 1) * 100 if len(close) >= 5 else 0.0
        ret20 = (cur / _safe_float(close.iloc[-20], cur) - 1) * 100 if len(close) >= 20 else 0.0
        dist_ma20 = (cur / ma20_v - 1) * 100 if ma20_v > 0 else 0.0
        ma20_slope = (ma20_v / ma20_5 - 1) * 100 if ma20_5 > 0 else 0.0

        if cur > ma20_v and ma20_v >= ma20_5 and cur > ma50_v:
            state = "BULL"
        elif cur < ma20_v and ma20_v < ma20_5:
            state = "BEAR"
        else:
            state = "BOX"

        return {
            "symbol": symbol,
            "state": state,
            "close": round(cur, 2),
            "ma20": round(ma20_v, 2),
            "ma50": round(ma50_v, 2),
            "ret5": round(ret5, 2),
            "ret20": round(ret20, 2),
            "dist_ma20": round(dist_ma20, 2),
            "ma20_slope": round(ma20_slope, 2),
        }
    except Exception as e:
        print(f"  ⚠️  {symbol} 지수 판단 실패: {e}")
        return {"symbol": symbol, "state": "UNKNOWN"}


def _vix_metrics() -> dict:
    try:
        hist = yf.Ticker("^VIX").history(period="1mo")
        if hist is None or hist.empty:
            return {"state": "UNKNOWN", "close": 0.0}
        close = hist['Close']
        cur = _safe_float(close.iloc[-1])
        prev = _safe_float(close.iloc[-2], cur) if len(close) >= 2 else cur
        chg = (cur / prev - 1) * 100 if prev else 0.0
        state = "HIGH" if cur >= VIX_RISK_OFF else "STABLE"
        return {"state": state, "close": round(cur, 2), "chg": round(chg, 2)}
    except Exception as e:
        print(f"  ⚠️  VIX 조회 실패: {e}")
        return {"state": "UNKNOWN", "close": 0.0}


def detect_us_market_regime() -> dict:
    nasdaq = _index_state("^IXIC")
    russell = _index_state("^RUT")
    vix = _vix_metrics()

    n_state = nasdaq.get('state', 'UNKNOWN')
    r_state = russell.get('state', 'UNKNOWN')
    v_state = vix.get('state', 'UNKNOWN')
    vix_close = _safe_float(vix.get('close'), 0.0)

    if n_state == "UNKNOWN" or v_state == "UNKNOWN":
        regime = "UNKNOWN"
    elif vix_close >= VIX_RISK_OFF or (n_state == "BEAR" and r_state == "BEAR"):
        regime = "RISK_OFF"
    elif n_state == "BULL" and vix_close < VIX_RISK_OFF and r_state in ["BULL", "BOX"]:
        regime = "RISK_ON"
    else:
        regime = "NEUTRAL"

    return {
        "regime": regime,
        "regime_label": REGIME_LABELS.get(regime, regime),
        "base_date": datetime.datetime.now(KST).strftime("%Y%m%d"),
        "nasdaq": nasdaq,
        "russell": russell,
        "vix": vix,
    }


# ══════════════════════════════════════════════════════════════
#  종목 스캔
# ══════════════════════════════════════════════════════════════

def _price_confirm(hist, cur_price: float) -> dict:
    try:
        close = hist['Close']
        volume = hist['Volume']
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma20_v = _safe_float(ma20.iloc[-1])
        ma20_5 = _safe_float(ma20.iloc[-5]) if len(ma20) >= 5 else ma20_v
        ma50_v = _safe_float(ma50.iloc[-1]) if len(ma50) >= 50 else ma20_v
        high20 = _safe_float(hist['High'].iloc[-21:-1].max()) if len(hist) >= 21 else _safe_float(hist['High'].max())
        avg_vol_20 = _safe_float(volume.iloc[-21:-1].mean()) if len(volume) >= 21 else _safe_float(volume.mean())
        cur_vol = _safe_float(volume.iloc[-1])
        vol_spike = cur_vol / (avg_vol_20 + 1)

        above_ma20 = cur_price > ma20_v if ma20_v > 0 else False
        ma20_up = ma20_v >= ma20_5 if ma20_5 > 0 else False
        breakout20 = cur_price > high20 if high20 > 0 else False
        above_ma50 = cur_price > ma50_v if ma50_v > 0 else False
        confirmed = (above_ma20 and ma20_up) or breakout20

        return {
            "confirmed": confirmed,
            "above_ma20": above_ma20,
            "above_ma50": above_ma50,
            "ma20_up": ma20_up,
            "breakout20": breakout20,
            "ma20": round(ma20_v, 2),
            "ma50": round(ma50_v, 2),
            "dist_ma20": round((cur_price / ma20_v - 1) * 100, 2) if ma20_v > 0 else 0.0,
            "vol_spike": round(float(vol_spike), 2),
        }
    except Exception:
        return {"confirmed": False, "vol_spike": 0.0, "dist_ma20": 0.0}


def _scan_symbol(symbol: str) -> dict | None:
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="3mo")
        if hist is None or len(hist) < 20:
            print(f"  ⚠️  {symbol} 데이터 없음")
            return None

        cur_price = round(_safe_float(hist['Close'].iloc[-1]), 2)
        price_meta = _price_confirm(hist, cur_price)
        vol_spike = price_meta.get('vol_spike', 0.0)
        rsi = round(_safe_float(calc_rsi(hist['Close']).iloc[-1], 50.0), 1)

        float_shares = None
        short_pct = None
        short_ratio = 0.0
        short_name = symbol
        long_summary = ''

        try:
            fi = t.fast_info
            float_shares = getattr(fi, 'shares_outstanding', None)
        except Exception:
            float_shares = None

        try:
            info = t.info
            float_shares = info.get('floatShares') or info.get('impliedSharesOutstanding') or float_shares
            short_pct = info.get('shortPercentOfFloat')
            short_ratio = info.get('shortRatio') or 0
            short_name = info.get('shortName', symbol)
            long_summary = info.get('longBusinessSummary', '')
        except Exception:
            pass

        float_m = _safe_float(float_shares) / 1e6 if float_shares else 0.0
        short_pct_p = _safe_float(short_pct) * 100 if short_pct else 0.0
        short_ratio = _safe_float(short_ratio)

        print(f"  📌 {symbol} | 거래량 {vol_spike}x | 공매도 {round(short_pct_p,1)}% | DTC {round(short_ratio,1)} | float {round(float_m,1)}M")

        # 채점: 점수는 순위용, 추천 여부는 하드 필터로 별도 판단
        short_score = min((short_pct_p - 10) / 30 * 35 + 5, 40) if short_pct_p >= 10 else 0
        vol_score = min((vol_spike - 1.0) / 5 * 25 + 5, 30) if vol_spike >= 1.0 else 0
        float_score = max(20 - (float_m / 100 * 20), 0) if 0 < float_m < 100 else 5
        ratio_score = min(short_ratio * 1.5, 10) if short_ratio else 0
        total_score = int(short_score + vol_score + float_score + ratio_score)
        score_100 = normalize_score(total_score, MAX_SCORE_US)

        setup_pass = (
            short_pct_p >= MIN_SHORT_PCT and
            short_ratio >= MIN_SHORT_RATIO and
            vol_spike >= MIN_VOL_SPIKE
        )
        price_confirmed = bool(price_meta.get('confirmed'))
        buy_candidate = setup_pass and price_confirmed and score_100 >= MIN_BUY_SCORE

        short_str = f"{round(short_pct_p,1)}%" if short_pct_p > 0 else "N/A"
        float_str = f"{round(float_m,1)}M" if float_m > 0 else "N/A"

        squeeze_level = (
            "🔥 EXTREME" if score_100 >= 70 and buy_candidate else
            "⚡ HIGH" if score_100 >= 55 else
            "📈 MEDIUM" if score_100 >= 35 else
            "📊 WATCH"
        )

        return {
            "rank": 0,
            "name": short_name,
            "code": symbol,
            "company_summary": (long_summary[:100] + '...') if long_summary else symbol,
            "cur_price": cur_price,
            "score_raw": total_score,
            "score_100": score_100,
            "squeeze_level": squeeze_level,
            "score_detail": {
                "공매도강도": int(short_score),
                "거래량급증": int(vol_score),
                "유통주희소": int(float_score),
                "커버소요일": int(ratio_score),
            },
            "meta": {
                "float_m": round(float_m, 1),
                "short_pct": round(short_pct_p, 1),
                "vol_spike": round(float(vol_spike), 2),
                "rsi": rsi,
                "short_ratio": round(short_ratio, 1),
                "short_str": short_str,
                "float_str": float_str,
                "setup_pass": setup_pass,
                "price_confirm": price_confirmed,
                "buy_candidate": buy_candidate,
                "above_ma20": price_meta.get('above_ma20', False),
                "above_ma50": price_meta.get('above_ma50', False),
                "ma20_up": price_meta.get('ma20_up', False),
                "breakout20": price_meta.get('breakout20', False),
                "dist_ma20": price_meta.get('dist_ma20', 0.0),
            }
        }
    except Exception as e:
        print(f"  ⚠️  {symbol} 오류: {e}")
        return None


def run_us_scan():
    print("\n🇺🇸 미국 숏스퀴즈 스캐닝 시작...")
    today_str = datetime.datetime.now(KST).strftime("%Y%m%d")
    regime_info = detect_us_market_regime()
    regime = regime_info.get('regime', 'UNKNOWN')
    print(f"🧭 US 장세: {regime_info.get('regime_label')} ({regime})")

    all_picks = []
    skipped = 0

    for symbol in WATCHLIST:
        pick = _scan_symbol(symbol)
        if pick is None:
            skipped += 1
            continue
        all_picks.append(pick)

    all_picks.sort(key=lambda x: x['score_100'], reverse=True)

    if regime == "RISK_OFF":
        qualified = []
        notes = ["NASDAQ/Russell 약세 또는 VIX 고위험 구간 — 매수 후보 차단"]
    elif regime == "UNKNOWN":
        qualified = [p for p in all_picks if p['meta'].get('buy_candidate')]
        notes = ["시장 판단 일부 실패 — 종목 조건 충족 후보만 제한 전송"]
    else:
        qualified = [p for p in all_picks if p['meta'].get('buy_candidate')]
        notes = ["공매도·커버소요일·거래량·가격확인 동시 충족 후보만 매수 후보"]

    top = qualified[:US_TOP_N]
    watch = [p for p in all_picks if p not in top][:WATCH_TOP_N]

    for i, p in enumerate(top, 1):
        p['rank'] = i
    for i, p in enumerate(watch, 1):
        p['rank'] = i

    print(f"\n🏁 미국 완료! 스캔 {len(all_picks)}건 → 매수 후보 {len(top)}건 / 관찰 {len(watch)}건")
    for p in top:
        bar = score_to_bar(p['score_100'])
        print(f"  ✅ #{p['rank']} {p['name']} | {bar} {p['score_100']}점 | {p['squeeze_level']}")

    us_output = {
        "today_picks": top,
        "buy_picks": top,
        "watch_picks": watch,
        "market_regime": regime,
        "market_regime_label": regime_info.get('regime_label', ''),
        "regime_info": regime_info,
        "strategy_used": ["US_SQUEEZE_HARD_FILTER"] if regime != "RISK_OFF" else ["US_DEFENSE"],
        "notes": notes,
        "total_candidates": len(top),
        "total_watch": len(watch),
        "total_screened": len(WATCHLIST) - skipped,
        "base_date": today_str,
    }
    with open(DATA_FILE_US, 'w', encoding='utf-8') as f:
        json.dump(us_output, f, ensure_ascii=False, indent=4, default=json_safe)

    return us_output


# ══════════════════════════════════════════════════════════════
#  텔레그램 메시지 조립
# ══════════════════════════════════════════════════════════════

def build_us_regime_header(us_data: dict) -> str:
    ri = us_data.get('regime_info', {})
    regime = us_data.get('market_regime') or ri.get('regime', 'UNKNOWN')
    label = us_data.get('market_regime_label') or ri.get('regime_label', regime)
    base_date = us_data.get('base_date', '')
    n = ri.get('nasdaq', {})
    r = ri.get('russell', {})
    v = ri.get('vix', {})
    return "\n".join([
        f"🇺🇸 <b>미장 숏스퀴즈 — {ko_date(base_date)} 장 종료 후</b>",
        "━" * 24,
        f"🧭 장세: <b>{label}</b> ({regime})",
        f"NASDAQ {n.get('state', 'NA')} | MA20 {n.get('dist_ma20', 0):+}% | 20일 {n.get('ret20', 0):+}%",
        f"Russell {r.get('state', 'NA')} | MA20 {r.get('dist_ma20', 0):+}% | 20일 {r.get('ret20', 0):+}%",
        f"VIX {v.get('close', 0)} | {v.get('state', 'NA')}",
    ])


def build_us_message(us_data: dict) -> str:
    picks = us_data.get("today_picks", [])
    watch = us_data.get("watch_picks", [])
    screened = us_data.get("total_screened", 0)
    notes = us_data.get("notes", [])

    lines = [
        build_us_regime_header(us_data),
        f"📋 워치리스트 {screened}종목 스캔",
        "",
    ]

    if notes:
        lines.append("📌 <b>전략 판단</b>")
        for n in notes:
            lines.append(f"  - {n}")
        lines.append("")

    if not picks:
        lines += [
            "⚠️ <b>오늘 매수 후보 없음</b>",
            "조건 충족 후보가 없거나 위험회피 장세입니다.",
            "",
        ]
    else:
        lines += [f"✅ <b>매수 후보 {len(picks)}종목</b>", "━" * 24]
        for p in picks:
            meta = p.get("meta", {})
            sd = p.get("score_detail", {})
            score_100 = p.get("score_100", 0)
            bar = score_to_bar(score_100)
            stars = squeeze_stars(score_100)

            short_100 = normalize_score(sd.get("공매도강도", 0), 40)
            vol_100 = normalize_score(sd.get("거래량급증", 0), 30)
            float_100 = normalize_score(sd.get("유통주희소", 0), 20)
            ratio_100 = normalize_score(sd.get("커버소요일", 0), 10)

            lines += [
                f"<b>#{p['rank']} {p['name']} (${p['code']})</b>",
                f"  <code>{bar}</code> <b>{score_100}점</b> {stars} {p.get('squeeze_level', '')}",
                f"  💰 현재가: <b>${p.get('cur_price', 0)}</b>",
                f"  📊 공매도 {meta.get('short_pct', 0)}% | DTC {meta.get('short_ratio', 0)}일 | Float {meta.get('float_str', 'N/A')}",
                f"  🔥 거래량 {meta.get('vol_spike', 0)}배 | RSI {meta.get('rsi', 0)} | MA20 {meta.get('dist_ma20', 0):+}%",
                f"  ✅ setup={meta.get('setup_pass')} / price={meta.get('price_confirm')}",
                f"    {grade_emoji(short_100)} 공매도 강도 {short_100}점",
                f"    {grade_emoji(vol_100)} 거래량 급증 {vol_100}점",
                f"    {grade_emoji(float_100)} 유통주 희소 {float_100}점",
                f"    {grade_emoji(ratio_100)} 숏커버 소요 {ratio_100}점",
                "",
            ]

    if watch:
        lines += ["👀 <b>관찰 후보</b> — 매수 조건 미충족", "━" * 24]
        for p in watch[:3]:
            meta = p.get('meta', {})
            lines.append(
                f"- {p.get('name')}(${p.get('code')}) | {p.get('score_100')}점 | "
                f"SI {meta.get('short_pct', 0)}% | Vol {meta.get('vol_spike', 0)}x | "
                f"setup={meta.get('setup_pass')} price={meta.get('price_confirm')}"
            )
        lines.append("")

    lines.append("💡 <i>숏스퀴즈는 고위험 — 조건 충족 전에는 관찰만, 손절 규율 필수</i>")
    return "\n".join(lines)


if __name__ == "__main__":
    us_result = run_us_scan()
    send_telegram(fetch_macro_summary())
    send_telegram(build_news_briefing())
    send_telegram(build_us_message(us_result))
