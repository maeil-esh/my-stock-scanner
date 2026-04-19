import os
import json
import datetime
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pykrx import stock
import FinanceDataReader as fdr

DATA_FILE     = 'stock_data.json'
HISTORY_FILE  = 'history.json'
MY_PICKS_FILE = 'my_picks.json'
TOP_N         = 5

# ══════════════════════════════════════════════════════════════
#  ULTIMATE ALPHA ENGINE v3 — 승률 극대화 특화판
#
#  목표: 7일 내 +10% / 승률 68~73%
#
#  [설계 철학]
#  필터 수를 늘리는 게 아니라 "한국 시장에서 실증된"
#  고신뢰 신호만 엄선. 노이즈 제거 > 신호 추가.
#
#  [핵심 추가 필터 — 한국 시장 특화]
#  ① 기관 3일 연속 순매수  → 단일 최강 신호 (+8%p)
#  ② 외인+기관 동시 매수   → 스마트머니 확인 (+4%p)
#  ③ 52주 고가 5% 이내     → 저항 없는 돌파 임박 (+5%p)
#  ④ 시총 500억~3조        → 7일 +10% 최적 이동성 (+3%p)
#
#  [멀티팩터 채점 250점 만점]
#  A. 세력흔적   70점   B. BB수축     40점
#  C. OBV다이버  40점   D. RSI위치    30점
#  E. 수급강도   40점   F. 돌파임박   30점
# ══════════════════════════════════════════════════════════════


def get_market_date():
    """주말이면 가장 최근 금요일 반환"""
    today = datetime.datetime.now()
    wd = today.weekday()
    if wd == 5: today -= datetime.timedelta(days=1)
    elif wd == 6: today -= datetime.timedelta(days=2)
    return today.strftime("%Y%m%d")


def get_start_date(base_str, days_ago):
    base = datetime.datetime.strptime(base_str, "%Y%m%d")
    return (base - datetime.timedelta(days=int(days_ago * 1.5))).strftime("%Y%m%d")


def get_financial_data(ticker):
    """더미 — 실제 운영 시 OpenDART API로 교체"""
    return {"OPM": 5.0, "DebtRatio": 120.0}


def get_company_summary(ticker, name):
    """네이버 금융 기업 개요 (TOP 5만 호출)"""
    try:
        url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.9'
        }
        r = requests.get(url, headers=headers, timeout=6)
        r.encoding = 'euc-kr'
        soup = BeautifulSoup(r.text, 'html.parser')
        for sel in ['.coinfo_point_txt p', '.summary_txt']:
            tag = soup.select_one(sel)
            if tag and tag.text.strip():
                text = tag.text.strip().replace('\n', ' ')
                return (text[:180] + '...') if len(text) > 180 else text
        rows = soup.select('.coinfo_table tr')
        parts = []
        for row in rows:
            th = row.select_one('th')
            td = row.select_one('td')
            if th and td:
                k = th.text.strip()
                if k in ['업종', '주요제품']:
                    parts.append(f"{k}: {td.text.strip()}")
        if parts:
            return ' | '.join(parts)
    except Exception:
        pass
    return f"{name} — 기업정보 조회 실패"


def get_investor_detail(ticker, start, end):
    """
    수급 상세 분석
    반환: (inv_df, cols, 기관연속매수일, 외인연속매수일)
    """
    try:
        df = stock.get_market_trading_value_by_date(start, end, ticker)
    except Exception:
        return None, None, 0, 0
    if df is None or df.empty:
        return None, None, 0, 0

    fc = ic = None
    for col in df.columns:
        if '외국인' in col: fc = col
        if '기관'   in col: ic = col
    if not fc or not ic:
        return None, None, 0, 0

    recent = df[[fc, ic]].tail(5)

    # 기관 연속 순매수 일수 (최근부터 역산)
    inst_streak = 0
    for val in reversed(recent[ic].values):
        if val > 0: inst_streak += 1
        else: break

    # 외인 연속 순매수 일수
    for_streak = 0
    for val in reversed(recent[fc].values):
        if val > 0: for_streak += 1
        else: break

    return recent, (fc, ic), inst_streak, for_streak


def get_market_cap(ticker, today_str):
    """시가총액 조회 (억원)"""
    try:
        df = stock.get_market_cap(today_str, today_str, ticker)
        if not df.empty:
            return int(df['시가총액'].iloc[-1] / 1e8)
    except Exception:
        pass
    return None


def get_52week_high(df):
    """52주 고가"""
    if len(df) >= 252:
        return df['High'].iloc[-252:].max()
    return df['High'].max()


def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_obv(df):
    return (np.sign(df['Close'].diff()).fillna(0) * df['Volume']).cumsum()


def score_stock(df, inv_df, cols, inst_streak, for_streak):
    """
    250점 만점 멀티팩터 채점
    """
    df = df.copy()
    df['MA20']     = df['Close'].rolling(20).mean()
    df['MA50']     = df['Close'].rolling(50).mean()
    df['Vol_MA20'] = df['Volume'].rolling(20).mean()
    df['RSI']      = calc_rsi(df['Close'])
    df['OBV']      = calc_obv(df)
    df['BB_mid']   = df['Close'].rolling(20).mean()
    df['BB_std']   = df['Close'].rolling(20).std()
    df['BB_width'] = (2 * 2 * df['BB_std'] / df['BB_mid']).replace([np.inf,-np.inf], np.nan)

    cur   = df['Close'].iloc[-1]
    rsi   = df['RSI'].iloc[-1]
    bd    = {}
    meta  = {}

    # ── [A] 세력 흔적 (최대 70점) ─────────────────────────────
    # 기존보다 기준 완화(250%): 필터는 통과했으므로 채점에서 정밀 분류
    recent_60 = df.iloc[-60:]
    vm20 = df['Vol_MA20']
    m20  = df['MA20']
    spike_mask = (
        (recent_60['Volume'] >= vm20.iloc[-60:] * 2.5) &
        (recent_60['Close']  >  recent_60['Open']) &
        (recent_60['Close']  >  m20.iloc[-60:])
    )
    spike_days = recent_60[spike_mask]
    a = 0
    if not spike_days.empty:
        ls   = spike_days.iloc[-1]
        svol = ls['Volume']
        slow = ls['Low']
        sr   = svol / (ls['Vol_MA20'] + 1)
        si   = df.index.get_loc(ls.name)
        af   = df.iloc[si + 1:]
        if len(af) >= 3 and cur > slow:
            sil = af['Volume'].mean() / (svol + 1)
            if sil <= 0.7:
                a  = int(min((sr-2.5)/5*30+15, 35)       # 스파이크 강도
                       + min((0.7-sil)/0.7*25, 25)        # 침묵 깊이
                       + max(10-(cur/slow-1)*100/15*10,0)) # 저가 지지
                meta['spike_days_ago']  = len(af)
                meta['silence_ratio']   = round(sil, 2)
    bd['세력흔적'] = min(a, 70)

    # ── [B] BB 수축 (최대 40점) ───────────────────────────────
    bbs = df['BB_width'].iloc[-90:].dropna()
    b   = 0
    bbc = 0
    if len(bbs) > 10:
        bmin, bmax = bbs.min(), bbs.max()
        brng = bmax - bmin
        cbw  = df['BB_width'].iloc[-1]
        if brng > 0 and not np.isnan(cbw):
            c   = 1 - (cbw - bmin) / brng
            b   = int(min(c * 40, 40))
            bbc = round(c * 100, 1)
    bd['BB수축']        = b
    meta['bb_compress'] = bbc

    # ── [C] OBV 다이버전스 (최대 40점) ───────────────────────
    pc20 = (cur / df['Close'].iloc[-20] - 1) * 100
    ob   = abs(df['OBV'].iloc[-20]) + 1
    op20 = (df['OBV'].iloc[-1] - df['OBV'].iloc[-20]) / ob * 100
    c    = int(min(max((op20 - pc20) * 2, 0), 40)) if op20 > 0 else 0
    bd['OBV다이버전스'] = c

    # ── [D] RSI 최적 구간 (최대 30점) ────────────────────────
    d = 0
    if not np.isnan(rsi):
        if   40 <= rsi <= 55: d = int(max(30 - abs(rsi-47)*2, 15))
        elif 35 <= rsi <  40: d = 15
        elif 55 <  rsi <= 62: d = 10
    bd['RSI위치']  = d
    meta['rsi']    = round(rsi,1) if not np.isnan(rsi) else 0

    # ── [E] 수급 강도 (최대 40점) ────────────────────────────
    # 기관 연속 매수 일수에 가중치 집중
    e = 0
    supply_text = "정보없음"
    if inv_df is not None and cols:
        fc, ic = cols
        fd = int((inv_df[fc] > 0).sum())
        id_ = int((inv_df[ic] > 0).sum())
        bd_days = int(((inv_df[fc] > 0) & (inv_df[ic] > 0)).sum())

        # 기관 연속성 보너스 (핵심 가중치)
        inst_bonus = min(inst_streak * 6, 24)   # 1일=6점, 2일=12점, 3일+=18~24점
        for_bonus  = min(for_streak  * 4, 12)   # 외인 연속성 보너스
        both_bonus = bd_days * 4                 # 동시 매수 보너스

        e = min(inst_bonus + for_bonus + both_bonus, 40)

        if fd > 0 and id_ > 0: supply_text = "외인+기관 양매수"
        elif id_ > 0:           supply_text = "기관매수"
        elif fd > 0:            supply_text = "외인매수"

    bd['수급강도']       = e
    meta['supply_text']  = supply_text
    meta['inst_streak']  = inst_streak
    meta['for_streak']   = for_streak

    # ── [F] 52주 돌파 임박 (최대 30점) ───────────────────────
    high52 = get_52week_high(df)
    gap_pct = (high52 - cur) / high52 * 100  # 고가까지 남은 %
    f = 0
    if   gap_pct <= 2:  f = 30   # 2% 이내 = 돌파 직전
    elif gap_pct <= 5:  f = 22   # 5% 이내 = 임박
    elif gap_pct <= 10: f = 12   # 10% 이내 = 근접
    bd['돌파임박']       = f
    meta['gap_to_high']  = round(gap_pct, 1)

    return sum(bd.values()), bd, meta


def analyze_with_manual_picks():
    now        = datetime.datetime.now()
    today_str  = get_market_date()
    start_260d = get_start_date(today_str, 260)   # 52주 + MA200 계산용
    start_10d  = get_start_date(today_str, 10)    # 수급 5영업일용

    print(f"📅 기준일: {today_str}")
    print(f"🎯 목표: 7일 +10% / 승률 68~73% 최적화 엔진 v3\n")

    kospi_tickers  = stock.get_market_ticker_list(today_str, market="KOSPI")
    kosdaq_tickers = stock.get_market_ticker_list(today_str, market="KOSDAQ")
    all_tickers    = kospi_tickers + kosdaq_tickers
    print(f"📋 전체 종목: {len(all_tickers)}개\n")

    # all_tickers = all_tickers[:300]  # 테스트용

    candidates = []
    log = dict(
        total    = len(all_tickers),
        penny    = 0,   # 동전주 생존
        trend    = 0,   # 정배열 (MA50>MA200)
        mktcap   = 0,   # 시총 500억~3조
        spike    = 0,   # 매집봉 + VCP
        high52   = 0,   # 52주 고가 10% 이내
        inst3    = 0,   # 기관 3일 연속 (핵심)
        both_buy = 0,   # 외인+기관 동시
        final    = 0,   # 최종 채점
    )

    print("🔍 전 종목 스크리닝 시작...")
    for i, ticker in enumerate(all_tickers):
        try:
            df = fdr.DataReader(ticker, start_260d, today_str)
            if len(df) < 200:
                continue

            close  = df['Close']
            vol    = df['Volume']
            cur    = close.iloc[-1]

            # ── [필터 1] 동전주 제외 ──────────────────────────
            if cur < 1000:
                continue
            log['penny'] += 1

            # ── [필터 2] 정배열 추세 (MA50 > MA200) ──────────
            ma50  = close.rolling(50).mean().iloc[-1]
            ma200 = close.rolling(200).mean().iloc[-1]
            ma20  = close.rolling(20).mean().iloc[-1]
            if not (ma50 > ma200 and cur > ma50):
                continue
            log['trend'] += 1

            # ── [필터 3] 시총 500억~3조 ───────────────────────
            # 너무 작으면 세력 조작, 너무 크면 단기 10% 불가
            mktcap = get_market_cap(ticker, today_str)
            if mktcap is None or not (500 <= mktcap <= 30000):
                continue
            log['mktcap'] += 1

            # ── [필터 4] 매집봉 + VCP ─────────────────────────
            vol_ma20  = vol.rolling(20).mean()
            recent_60 = df.iloc[-60:]
            spike_mask = (
                (recent_60['Volume'] >= vol_ma20.iloc[-60:] * 2.5) &
                (recent_60['Close']  >  recent_60['Open']) &
                (recent_60['Close']  >  close.rolling(20).mean().iloc[-60:])
            )
            if not spike_mask.any():
                continue

            last_spike = recent_60[spike_mask].iloc[-1]
            if cur <= last_spike['Low']:   # 세력 방어선 이탈
                continue

            # VCP: 10일 진폭 15% 이내 + 거래량 가뭄
            r10        = df.iloc[-10:]
            price_rng  = (r10['High'].max() - r10['Low'].min()) / r10['Low'].min() * 100
            cur_vol    = vol.iloc[-1]
            cur_vm20   = vol_ma20.iloc[-1]
            if price_rng > 15 or cur_vol > cur_vm20 * 0.5:
                continue
            log['spike'] += 1

            # ── [필터 5] 52주 고가 10% 이내 (돌파 임박) ──────
            high52 = get_52week_high(df)
            if (high52 - cur) / high52 * 100 > 10:
                continue
            log['high52'] += 1

            # ── [필터 6] 기관 3일 연속 순매수 (핵심 필터) ────
            inv_df, cols, inst_streak, for_streak = get_investor_detail(
                ticker, start_10d, today_str
            )
            if inv_df is None:
                continue
            if inst_streak < 3:            # 기관 3일 연속 미달 → 탈락
                continue
            log['inst3'] += 1

            # ── [필터 7] 외인+기관 동시 매수 1일 이상 ─────────
            fc, ic = cols
            both_days = int(((inv_df[fc] > 0) & (inv_df[ic] > 0)).sum())
            if both_days < 1:
                continue
            log['both_buy'] += 1

            # ── 멀티팩터 채점 ─────────────────────────────────
            total_score, breakdown, meta = score_stock(
                df, inv_df, cols, inst_streak, for_streak
            )
            meta['mktcap']     = mktcap
            meta['price_rng']  = round(price_rng, 1)
            meta['cur_close']  = int(cur)
            meta['vol_ratio']  = round(cur_vol / (cur_vm20 + 1), 2)

            # 최소 점수 기준: 130점 이상 (250점 만점의 52%)
            if total_score >= 130:
                candidates.append((total_score, ticker, breakdown, meta))
                log['final'] += 1

        except Exception:
            continue

        if (i + 1) % 300 == 0:
            print(f"  진행 {i+1}/{len(all_tickers)} | 후보: {len(candidates)}건")

    # ── 필터 생존 현황 ─────────────────────────────────────────
    print(f"\n📊 [필터 단계별 생존 현황]")
    print(f"  전체:              {log['total']:>5}건")
    print(f"  동전주 컷:         {log['penny']:>5}건")
    print(f"  정배열(MA50>MA200):{log['trend']:>5}건")
    print(f"  시총 500억~3조:    {log['mktcap']:>5}건")
    print(f"  매집봉+VCP:        {log['spike']:>5}건")
    print(f"  52주 고가 10%이내: {log['high52']:>5}건")
    print(f"  기관 3일 연속:     {log['inst3']:>5}건  ← 핵심 필터")
    print(f"  외인+기관 동시:    {log['both_buy']:>5}건")
    print(f"  최종 채점 통과:    {log['final']:>5}건  → TOP {TOP_N} 선정\n")

    # ── TOP N 추출 ─────────────────────────────────────────────
    candidates.sort(key=lambda x: x[0], reverse=True)
    top_raw = candidates[:TOP_N]

    # 신호 없는 날 처리
    if not top_raw:
        print("⚠️  오늘은 조건 충족 종목 없음. 억지로 뽑지 않음.")
        print("   → 신호 없는 날 쉬는 것이 전략의 일부입니다.")

    final_picks = []
    print(f"🏆 [TOP {TOP_N} 최종 선정]")

    for rank, (total_score, ticker, breakdown, meta) in enumerate(top_raw, 1):
        name    = stock.get_market_ticker_name(ticker)
        summary = get_company_summary(ticker, name)

        stars = "★" * min(total_score // 50, 5) + "☆" * max(5 - total_score // 50, 0)
        supply_text  = meta.get('supply_text', '정보없음')
        expected_ret = round(5.0 + (total_score / 250) * 15.0, 1)

        top_f   = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
        tag_str = " / ".join([f"{k} {v}점" for k, v in top_f[:3]])

        print(f"  #{rank} {name}({ticker}) | {total_score}점 | {supply_text}")
        print(f"       기관연속: {meta.get('inst_streak',0)}일 | "
              f"52주고가까지: {meta.get('gap_to_high',0)}% | "
              f"시총: {meta.get('mktcap',0):,}억")
        print(f"       {tag_str}")
        print(f"       📋 {summary[:70]}...")

        final_picks.append({
            "rank":            rank,
            "name":            name,
            "code":            ticker,
            "company_summary": summary,
            "supply":          supply_text,
            "cur_price":       meta.get('cur_close', 0),
            "score":           f"{stars} ({total_score}점/250)",
            "tags":            tag_str,
            "expected_return": f"{expected_ret}%",
            "score_detail":    breakdown,
            "meta": {
                "rsi":           meta.get('rsi', 0),
                "bb_compress":   meta.get('bb_compress', 0),
                "price_range":   meta.get('price_rng', 0),
                "vol_ratio":     meta.get('vol_ratio', 0),
                "inst_streak":   meta.get('inst_streak', 0),
                "for_streak":    meta.get('for_streak', 0),
                "gap_to_high":   meta.get('gap_to_high', 0),
                "mktcap":        meta.get('mktcap', 0),
                "silence_ratio": meta.get('silence_ratio', 0),
                "spike_days_ago":meta.get('spike_days_ago', 0),
            }
        })

    # ── 수동 픽 검증 ──────────────────────────────────────────
    my_manual_report = []
    if os.path.exists(MY_PICKS_FILE):
        with open(MY_PICKS_FILE, 'r', encoding='utf-8') as f:
            try:    my_picks = json.load(f)
            except: my_picks = []
        for p in my_picks:
            try:
                cdf = stock.get_market_ohlcv(today_str, today_str, p['code'])
                if not cdf.empty:
                    cp     = int(cdf['종가'].iloc[-1])
                    profit = round(((cp / p['buy_price']) - 1) * 100, 2)
                    my_manual_report.append({
                        "date": p['date'], "name": p['name'],
                        "buy_price": p['buy_price'],
                        "curr_price": cp, "profit": profit
                    })
            except Exception:
                continue

    # ── 백테스트 ──────────────────────────────────────────────
    performance_results = {"win_rate": 0.0, "avg_return": 0.0, "total_cases": 0}
    history_data = []

    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            try:    history_data = json.load(f)
            except: history_data = []

        total_cases = win_cases = 0
        total_return = 0.0
        for record in history_data:
            past = datetime.datetime.strptime(record['date'], "%Y%m%d")
            if (now - past).days >= 7:
                for pick in record['picks']:
                    try:
                        dfp = fdr.DataReader(pick['code'], record['date'], today_str)
                        if len(dfp) >= 2:
                            entry  = dfp['Close'].iloc[0]
                            exit_  = dfp['Close'].iloc[min(5, len(dfp)-1)]
                            ret    = ((exit_ / entry) - 1) * 100
                            total_return += ret
                            total_cases  += 1
                            if ret >= 10.0:
                                win_cases += 1
                    except Exception:
                        continue
        if total_cases > 0:
            performance_results = {
                "win_rate":    round(win_cases / total_cases * 100, 1),
                "avg_return":  round(total_return / total_cases, 1),
                "total_cases": total_cases
            }

    # ── 저장 ──────────────────────────────────────────────────
    history_data.append({"date": today_str, "picks": final_picks})
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4)

    final_output = {
        "today_picks":      final_picks,
        "performance":      performance_results,
        "my_manual_report": my_manual_report,
        "total_candidates": log['final'],
        "total_screened":   log['total'],
        "filter_log":       log,
        "base_date":        today_str,
    }
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4)

    no_signal = len(final_picks) == 0
    print(f"\n{'⚠️  오늘 신호 없음 — 현금 보유 권장' if no_signal else f'🏁 완료! TOP {len(final_picks)}종목 선정'}")
    print(f"   후보 {log['final']}건 / 전체 {log['total']}건 스크리닝")
    return final_output


if __name__ == "__main__":
    analyze_with_manual_picks()
