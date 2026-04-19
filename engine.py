import os
import json
import datetime
import numpy as np
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from pykrx import stock
import FinanceDataReader as fdr
import yfinance as yf

# [환경설정] 파일 경로 및 마켓 설정
DATA_FILE_KR  = 'stock_data.json'
DATA_FILE_US  = 'stock_data_us.json'
HISTORY_FILE  = 'history.json'
MY_PICKS_FILE = 'my_picks.json'
TOP_N         = 5

# ══════════════════════════════════════════════════════════════
# [1] 공통 유틸리티 및 시간 계산 (52주 업무 원칙 준수)
# ══════════════════════════════════════════════════════════════

def get_market_date():
    """주말/공휴일 대응: 가장 최근 장이 열린 날 반환"""
    today = datetime.datetime.now()
    wd = today.weekday()
    if wd == 5: today -= datetime.timedelta(days=1)
    elif wd == 6: today -= datetime.timedelta(days=2)
    return today.strftime("%Y%m%d")

def get_start_date(base_str, days_ago):
    """분석에 필요한 과거 시점 계산 (1.5배 보정으로 충분한 데이터 확보)"""
    base = datetime.datetime.strptime(base_str, "%Y%m%d")
    return (base - datetime.timedelta(days=int(days_ago * 1.5))).strftime("%Y%m%d")

# ══════════════════════════════════════════════════════════════
# [2] 기술적 지표 계산 모듈 (정밀 분석용)
# ══════════════════════════════════════════════════════════════

def calc_rsi(series, period=14):
    """상대강도지수: 과매수/과매도 탐지"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs = gain / (loss.replace(0, np.nan))
    return 100 - (100 / (1 + rs))

def calc_obv(df):
    """거래량 분산 지표: 주가 횡보 중 매집 흔적 추적"""
    return (np.sign(df['Close'].diff()).fillna(0) * df['Volume']).cumsum()

def analyze_vcp_pattern(df):
    """VCP(변동성 축소 패턴) 정밀 로직"""
    if len(df) < 30: return 0, False
    r10 = df.iloc[-10:]
    r20 = df.iloc[-20:]
    
    # 가격 진폭 계산
    amp10 = (r10['High'].max() - r10['Low'].min()) / r10['Low'].min() * 100
    amp20 = (r20['High'].max() - r20['Low'].min()) / r20['Low'].min() * 100
    
    # 거래량 가뭄(Dry up) 확인
    avg_vol = df['Volume'].rolling(20).mean().iloc[-1]
    is_dry = df['Volume'].iloc[-1] < avg_vol * 0.5
    
    return round(amp10, 1), is_dry
    # ══════════════════════════════════════════════════════════════
# [3] 외부 데이터 수집 엔진 (Naver & KRX)
# ══════════════════════════════════════════════════════════════

def get_company_summary(ticker, name):
    """네이버 금융 크롤링: 기업 개요 및 주요 제품 추출"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        r = requests.get(url, headers=headers, timeout=5)
        r.encoding = 'euc-kr'
        soup = BeautifulSoup(r.text, 'html.parser')
        summary = soup.select_one('.summary_txt')
        if summary:
            return summary.text.strip().replace('\n', ' ')[:160] + "..."
    except: pass
    return f"{name} - 섹터 모멘텀 및 기업 가치 분석 중"

def get_investor_detail(ticker, start, end):
    """수급 상세 분석: 기관/외인 연속 매수 및 스마트머니 포착"""
    try:
        df = stock.get_market_trading_value_by_date(start, end, ticker)
        if df is None or df.empty: return None, 0, 0
        ic = [c for c in df.columns if '기관' in c][0]
        fc = [c for c in df.columns if '외국인' in c][0]
        
        # 기관 연속 매수일 계산
        i_streak = 0
        for v in reversed(df[ic].tail(5).values):
            if v > 0: i_streak += 1
            else: break
            
        # 외인 연속 매수일 계산
        f_streak = 0
        for v in reversed(df[fc].tail(5).values):
            if v > 0: f_streak += 1
            else: break
        return df, i_streak, f_streak
    except: return None, 0, 0

# ══════════════════════════════════════════════════════════════
# [4] 🇰🇷 한국 시장 채점 엔진 (250점 만점 풀스펙)
# ══════════════════════════════════════════════════════════════

def score_kr_stock(df, i_streak, f_streak):
    """멀티팩터 정밀 가중치 시스템"""
    df = df.copy()
    cur = df['Close'].iloc[-1]
    
    # 1. 기술적 지표 생성
    df['MA20'] = df['Close'].rolling(20).mean()
    df['RSI'] = calc_rsi(df['Close'])
    df['OBV'] = calc_obv(df)
    df['std'] = df['Close'].rolling(20).std()
    df['BB_width'] = (4 * df['std'] / df['MA20']) * 100
    
    bd = {} # Breakdown
    # A. 세력 흔적 (70점): 대량거래 장대양봉 후 눌림목 지지
    v_ma = df['Volume'].rolling(20).mean().iloc[-1]
    v_ratio = df['Volume'].iloc[-1] / (v_ma + 1)
    bd['세력흔적'] = min(int(v_ratio * 15) + 30, 70)
    
    # B. BB 수축 (40점): 변동성 감소(VCP) 확인
    bbw_recent = df['BB_width'].tail(20)
    compress = 1 - (bbw_recent.iloc[-1] / (bbw_recent.max() + 0.1))
    bd['BB수축'] = int(compress * 40)
    
    # C. OBV 다이버전스 (40점): 수급 매집 확인
    obv_slope = df['OBV'].iloc[-1] - df['OBV'].iloc[-5]
    bd['OBV다이버전스'] = 40 if obv_slope > 0 else 10
    
    # D. RSI 위치 (30점): 40~60 사이의 안정적 상승 구간
    rsi_now = df['RSI'].iloc[-1]
    bd['RSI위치'] = 30 if 45 <= rsi_now <= 60 else 15
    
    # E. 수급 강도 (40점): 기관 연속성 가중치
    bd['수급강도'] = min(i_streak * 10 + f_streak * 5, 40)
    
    # F. 돌파 임박 (30점): 52주 고가 근접도
    h52 = df['High'].iloc[-252:].max()
    gap = (h52 - cur) / h52 * 100
    bd['돌파임박'] = max(30 - int(gap * 2), 0)
    
    meta = {"rsi": round(rsi_now, 1), "gap": round(gap, 1), "streak": i_streak, "compress": round(compress*100, 1)}
    return sum(bd.values()), bd, meta
    # ══════════════════════════════════════════════════════════════
# [5] 🇺🇸 미국 시장 분석 엔진 (Short Squeeze Hunter)
# ══════════════════════════════════════════════════════════════

def analyze_us_market():
    """미국 숏스퀴즈 & 메가러너 포착 로직"""
    print("\n🇺🇸 미국 시장 스캐닝 시작...")
    tickers = ['GME', 'AMC', 'TSLA', 'NVDA', 'CVNA', 'AI', 'UPST', 'PLTR', 'SOFI', 'RIVN', 'MARA', 'RIOT']
    us_picks = []
    
    for s in tickers:
        try:
            t = yf.Ticker(s)
            info = t.info
            hist = t.history(period="1mo")
            if len(hist) < 20: continue
            
            # 공매도 비율 및 유통주식수
            f_shares = info.get('floatShares', 1e15) / 1e6
            s_ratio = info.get('shortPercentOfFloat', 0) * 100
            v_spike = hist['Volume'].iloc[-1] / (hist['Volume'].iloc[:-1].mean() + 1)
            
            if f_shares < 120 and s_ratio > 10:
                score = min((s_ratio * 1.5) + (v_spike * 15) + (120/f_shares), 100)
                us_picks.append({
                    "name": s, "code": s, "cur_price": round(hist['Close'].iloc[-1], 2),
                    "score": f"🚀 ({int(score)}점/100)", "supply": f"공매도 {round(s_ratio, 1)}%",
                    "company_summary": f"유통 {round(f_shares, 1)}M / 거래폭발 {round(v_spike, 1)}배",
                    "expected_return": "25.0",
                    "score_detail": {"공매도비율": int(s_ratio), "거래폭발": int(v_spike*10)},
                    "meta": {"inst_streak": round(v_spike, 1), "gap_to_high": round(s_ratio, 1)}
                })
        except: continue
    
    us_picks.sort(key=lambda x: x['score'], reverse=True)
    with open(DATA_FILE_US, 'w', encoding='utf-8') as f:
        json.dump({"today_picks": us_picks[:TOP_N], "base_date": get_market_date()}, f, ensure_ascii=False, indent=4)

# ══════════════════════════════════════════════════════════════
# [6] 🇰🇷 한국 시장 통합 스캐너 (7단계 필터링)
# ══════════════════════════════════════════════════════════════

def analyze_kr_market():
    today_str = get_market_date()
    start_260d = get_start_date(today_str, 260)
    print(f"🚀 한국 시장 엔진 가동: {today_str}")
    
    all_t = stock.get_market_ticker_list(today_str, market="KOSPI") + stock.get_market_ticker_list(today_str, market="KOSDAQ")
    final_candidates = []
    log = {"total": len(all_t), "inst3": 0}

    for i, t in enumerate(all_t):
        try:
            df = fdr.DataReader(t, start_260d, today_str)
            if len(df) < 250: continue # 52주 데이터 부족 컷
            cur = int(df['Close'].iloc[-1])
            if cur < 1000: continue # 동전주 컷
            
            # 수급 분석 (기관 3일 연속 필터)
            inv_df, i_streak, f_streak = get_investor_detail(t, get_start_date(today_str, 10), today_str)
            if i_streak < 3: continue
            log['inst3'] += 1
            
            # 채점 및 최종 선정
            score, bd, meta = score_kr_stock(df, i_streak, f_streak)
            if score >= 135:
                name = stock.get_market_ticker_name(t)
                final_candidates.append({
                    "name": name, "code": t, "cur_price": cur,
                    "score": f"★ ({score}점/250)", "supply": f"기관 {i_streak}일 연속",
                    "company_summary": get_company_summary(t, name),
                    "expected_return": str(round(5 + (score/250)*10, 1)),
                    "score_detail": bd, "meta": {"inst_streak": i_streak, "gap_to_high": meta['gap'], "mktcap": 0}
                })
        except: continue
        if (i+1) % 500 == 0: print(f"  스캔 중... {i+1}/{len(all_t)}")
        
    final_candidates.sort(key=lambda x: int(x['score'].split('(')[1].split('점')[0]), reverse=True)
    return final_candidates[:TOP_N], log
    # ══════════════════════════════════════════════════════════════
# [7] 성과 측정 및 데이터 저장 (Backtest & Report)
# ══════════════════════════════════════════════════════════════

def run_backtest(today_str):
    """7일 전 추천 종목의 수익률 추적"""
    perf = {"win_rate": 0.0, "avg_return": 0.0, "total_cases": 0}
    if not os.path.exists(HISTORY_FILE): return perf
    
    with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
        history = json.load(f)
    
    total_ret, wins = 0.0, 0
    for record in history:
        past_date = datetime.datetime.strptime(record['date'], "%Y%m%d")
        if (datetime.datetime.now() - past_date).days >= 7:
            for p in record['picks']:
                try:
                    df = fdr.DataReader(p['code'], record['date'], today_str)
                    if len(df) >= 2:
                        ret = ((df['Close'].iloc[-1] / df['Close'].iloc[0]) - 1) * 100
                        total_ret += ret
                        perf['total_cases'] += 1
                        if ret >= 10.0: wins += 1
                except: continue
    
    if perf['total_cases'] > 0:
        perf['win_rate'] = round(wins / perf['total_cases'] * 100, 1)
        perf['avg_return'] = round(total_ret / perf['total_cases'], 1)
    return perf

def main():
    start_time = time.time()
    today_str = get_market_date()
    
    # 1. 한국 시장 실행
    kr_picks, kr_log = analyze_kr_market()
    
    # 2. 미국 시장 실행
    analyze_us_market()
    
    # 3. 백테스트 실행
    perf = run_backtest(today_str)
    
    # 4. 히스토리 업데이트
    history = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f: history = json.load(f)
    history.append({"date": today_str, "picks": kr_picks})
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f: json.dump(history[-30:], f, ensure_ascii=False, indent=4)
    
    # 5. 최종 데이터 파일 저장
    final_data = {
        "today_picks": kr_picks,
        "performance": perf,
        "filter_log": kr_log,
        "base_date": today_str,
        "my_manual_report": [] # 수동 픽 데이터 연동시 채움
    }
    
    with open(DATA_FILE_KR, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
    print(f"\n🏁 모든 분석 완료! 소요시간: {int(time.time() - start_time)}초")

if __name__ == "__main__":
    main()
