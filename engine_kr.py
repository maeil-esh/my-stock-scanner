"""
engine_kr.py — 국장 바닥반등+거래량 스캐너 (PRO v2)
실행: python engine_kr.py
스케줄: 09:30 / 13:00 / 16:00 KST

[변경 이력 v2]
① TOP_N 3 고정
② 실시간 현재가 (네이버 polling API) → 트레이드 플랜 실시간 재산출
③ 스레드 5→10, sleep 제거 → 실행 시간 단축
④ DART 코드 로딩 제거 (스캔 로직 미사용)
⑤ 시총 필터 실패 시 조기 종료
"""
import os, json, datetime
import threading
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr

from engine_common import (
    get_market_date, get_start_date,
    ko_date, now_label, send_telegram, fetch_macro_summary,
    build_news_briefing, calc_rsi, calc_obv,
    json_safe
)

DATA_FILE    = 'stock_data.json'
HISTORY_FILE = 'history.json'
TOP_N        = 5
MAX_SCORE    = 305  # A(50)+B(50)+C(30)+D(40)+F(15)+H(15)+I(10)+J(35)+L(20)+M(20)+N(20)
MKTCAP_MIN   = 1000
MKTCAP_MAX   = 30000

# 네이버 모바일 API 응답 캐시 (ticker → dict) — 가격/업종 공용
_naver_basic_cache: dict = {}

# 스캔 1회당 1번만 조회하는 상승 테마 캐시
_rising_themes_cache: list = []


def fetch_spike_news(ticker: str, spike_date_strs: list) -> list:
    """
    스파이크 발생 시점 전후 3일 뉴스 수집 (정보용, 점수 없음)
    반환: [{'date': '2024-03-15', 'title': '...', 'spike': '1차'}, ...]
    """
    results = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ko-KR,ko;q=0.9'}
        for idx, date_str in enumerate(spike_date_strs[-3:], 1):
            label = f"{idx}차 스파이크({date_str})"
            url   = f"https://finance.naver.com/item/news_news.naver?code={ticker}&page=1"
            r     = requests.get(url, headers=headers, timeout=5)
            r.encoding = 'euc-kr'
            soup  = BeautifulSoup(r.text, 'html.parser')
            for a in soup.select('.tb_cont .title a, dl dt a')[:3]:
                title = a.get_text(strip=True)
                if title:
                    results.append({'spike': label, 'title': title})
                    break
    except Exception:
        pass
    return results


def fetch_rising_themes() -> list:
    """
    네이버 테마 시세 → 당일 상승 테마 TOP10
    반환: [{'name': '방산', 'chg': 4.2}, ...]
    """
    global _rising_themes_cache
    if _rising_themes_cache:
        return _rising_themes_cache
    try:
        r = requests.get(
            "https://finance.naver.com/sise/theme.naver",
            headers={'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ko-KR,ko;q=0.9'},
            timeout=8
        )
        r.encoding = 'euc-kr'
        soup   = BeautifulSoup(r.text, 'html.parser')
        themes = []
        for row in soup.select('table.type_1 tr'):
            cols     = row.select('td')
            name_tag = row.select_one('td a')
            if not name_tag or len(cols) < 2:
                continue
            try:
                chg = float(cols[1].get_text(strip=True).replace('+','').replace('%','').replace(',',''))
                if chg > 0:
                    themes.append({'name': name_tag.get_text(strip=True), 'chg': chg})
            except Exception:
                continue
        themes.sort(key=lambda x: x['chg'], reverse=True)
        _rising_themes_cache = themes[:10]
        print(f"  📌 상승 테마: {', '.join([t['name'] for t in _rising_themes_cache[:5]])}")
        return _rising_themes_cache
    except Exception as e:
        print(f"  ⚠️  테마 조회 실패: {e}")
        return []


def score_theme_match(ticker: str, rising_themes: list) -> tuple:
    """
    종목 뉴스 제목 + 업종 설명 vs 현재 상승 테마 키워드 매칭
    반환: (점수, 매칭된 테마명 문자열)
    MAX: 15점
    """
    if not rising_themes:
        return 0, ''
    try:
        # 1. 종목 뉴스 제목 수집 (최근 20건)
        url = f"https://finance.naver.com/item/news_news.naver?code={ticker}&page=1"
        r   = requests.get(
            url,
            headers={'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ko-KR,ko;q=0.9'},
            timeout=6
        )
        r.encoding = 'euc-kr'
        soup        = BeautifulSoup(r.text, 'html.parser')
        news_titles = [a.get_text(strip=True) for a in soup.select('.tb_cont .title a, dl dt a') if a.get_text(strip=True)]

        # 2. 업종/설명 (naver basic API에서 캐시된 값)
        basic       = _fetch_naver_basic(ticker)
        description = basic.get('_industry', '')

        all_text = ' '.join(news_titles[:20]) + ' ' + description

        # 3. 테마 키워드 매칭
        matched = []
        for theme in rising_themes:
            tname = theme['name']
            # 테마명 자체 + 앞 2글자 + 공백 분리 단어로 매칭
            keywords = [tname, tname[:2]] + [w for w in tname.split() if len(w) >= 2]
            if any(kw in all_text for kw in keywords):
                matched.append(f"{tname}({theme['chg']:+.1f}%)")

        if   len(matched) >= 2: score = 15
        elif len(matched) == 1: score = 8
        else:                   score = 0

        return score, ', '.join(matched[:3])

    except Exception as e:
        print(f"  ⚠️  테마 매칭 실패({ticker}): {e}")
        return 0, ''


def _fetch_naver_basic(ticker: str) -> dict:
    """
    네이버 모바일 주식 기본 API
    실제 필드: stockName, sosok(0=KOSPI,1=KOSDAQ), closePrice 등
    """
    if ticker in _naver_basic_cache:
        return _naver_basic_cache[ticker]
    try:
        url = f"https://m.stock.naver.com/api/stock/{ticker}/basic"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer':    'https://m.stock.naver.com/'
        }
        r    = requests.get(url, headers=headers, timeout=5)
        data = r.json()

        # sosok → 시장 구분 문자열 변환
        sosok = str(data.get('sosok', ''))
        data['_market'] = 'KOSPI' if sosok == '0' else 'KOSDAQ' if sosok == '1' else ''

        # 업종/설명 — integration API description 필드 사용
        try:
            r2   = requests.get(
                f"https://m.stock.naver.com/api/stock/{ticker}/integration",
                headers=headers, timeout=5
            )
            d2            = r2.json()
            desc          = (d2.get('description') or '').strip()
            data['_industry'] = (desc[:60] + '...') if len(desc) > 60 else desc
        except Exception:
            data['_industry'] = ''

        _naver_basic_cache[ticker] = data
        return data
    except Exception:
        _naver_basic_cache[ticker] = {}
        return {}


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


# ══════════════════════════════════════════════════════════════
#  실시간 현재가 (네이버 polling API)
# ══════════════════════════════════════════════════════════════

def get_realtime_price(ticker):
    """
    실시간 현재가 — 네이버 모바일 API 1순위, PC 스크랩 2순위
    실패 시 None → 호출부에서 전일 종가 fallback
    """
    # 1순위: 네이버 모바일 JSON API (확인된 필드: closePrice)
    try:
        data      = _fetch_naver_basic(ticker)
        price_str = data.get('closePrice', '')
        if price_str:
            return int(str(price_str).replace(',', ''))
    except Exception:
        pass

    # 2순위: 네이버 PC 시세 페이지 스크랩
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ko-KR'}
        r = requests.get(url, headers=headers, timeout=5)
        r.encoding = 'euc-kr'
        soup  = BeautifulSoup(r.text, 'html.parser')
        tag   = soup.select_one('p.no_today strong')
        if tag:
            return int(tag.text.strip().replace(',', ''))
    except Exception:
        pass

    print(f"  ⚠️  실시간가 실패({ticker}): 전일 종가 사용")
    return None


# ══════════════════════════════════════════════════════════════
#  시장 레짐 감지
# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════
#  상대강도(RS)
# ══════════════════════════════════════════════════════════════

def calc_relative_strength(stock_df, index_df, period=63):
    if index_df is None:
        return 0.0
    try:
        if len(stock_df) < period or len(index_df) < period:
            return 0.0
        stock_ret = stock_df['Close'].iloc[-1] / stock_df['Close'].iloc[-period] - 1
        index_ret = index_df['Close'].iloc[-1] / index_df['Close'].iloc[-period] - 1
        return round(float((stock_ret - index_ret) * 100), 1)
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════════════
#  ATR 손절/목표가
# ══════════════════════════════════════════════════════════════

def calc_trade_levels(df, cur_price):
    try:
        high_low   = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close  = (df['Low']  - df['Close'].shift()).abs()
        tr  = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]

        stop_loss  = int(cur_price - atr * 1.5)
        target_1   = int(cur_price + atr * 3.0)
        target_2   = int(cur_price + atr * 5.0)
        risk_pct   = round(float((cur_price - stop_loss) / cur_price * 100), 1)
        reward_pct = round(float((target_1 - cur_price) / cur_price * 100), 1)

        return {
            "entry":      int(cur_price),
            "stop_loss":  stop_loss,
            "target_1":   target_1,
            "target_2":   target_2,
            "risk_pct":   risk_pct,
            "reward_pct": reward_pct,
            "rr_ratio":   round(reward_pct / risk_pct, 1) if risk_pct > 0 else 0,
            "atr":        round(float(atr), 1),
        }
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  유틸
# ══════════════════════════════════════════════════════════════

def safe_get_market_ticker_list(market):
    try:
        df  = fdr.StockListing('KOSPI' if market == "KOSPI" else 'KOSDAQ')
        col = 'Code' if 'Code' in df.columns else df.columns[0]
        return list(df[col].dropna().astype(str).str.zfill(6))
    except Exception as e:
        print(f"  ❌ fdr {market} 실패: {e}")
        return []


def _find_cap_col(df):
    candidates = ['Marcap', 'MarCap', 'marcap', 'mktcap', 'MktCap', 'Market Cap']
    for c in candidates:
        if c in df.columns: return c
    for col in df.columns:
        if 'cap' in col.lower(): return col
    return None


def get_company_summary(ticker, name):
    # 1순위: 네이버 모바일 API (_market, _industry 전처리 필드)
    try:
        data     = _fetch_naver_basic(ticker)
        mkt      = data.get('_market', '')
        industry = data.get('_industry', '')
        parts    = []
        if mkt:      parts.append(f"[{mkt}]")
        if industry: parts.append(industry)
        if parts:    return ' '.join(parts)
    except Exception:
        pass

    # 2순위: 네이버 PC 기업정보 스크랩
    try:
        url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ko-KR,ko;q=0.9'}
        r = requests.get(url, headers=headers, timeout=6)
        r.encoding = 'euc-kr'
        soup  = BeautifulSoup(r.text, 'html.parser')
        rows  = soup.select('.coinfo_table tr')
        parts = {}
        for row in rows:
            th = row.select_one('th'); td = row.select_one('td')
            if th and td:
                key = th.text.strip()
                if key in ['업종', '주요제품', '주요사업']:
                    val = td.text.strip().replace('\n', ' ')
                    if val: parts[key] = val[:40]
        if parts:
            desc = []
            if '업종'     in parts: desc.append(f"[{parts['업종']}]")
            if '주요제품'  in parts: desc.append(parts['주요제품'])
            elif '주요사업' in parts: desc.append(parts['주요사업'])
            return ' '.join(desc)
    except Exception:
        pass

    return name


def get_investor_detail(ticker, start, end):
    """
    외인·기관 수급 — 네이버 금융 PC 페이지 스크랩
    """
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept-Language': 'ko-KR,ko;q=0.9'
        }
        r = requests.get(url, headers=headers, timeout=8)
        r.encoding = 'euc-kr'
        soup = BeautifulSoup(r.text, 'html.parser')

        rows = soup.select('table.type2 tr')
        foreign_vals = []
        inst_vals    = []

        for row in rows[1:]:
            tds = row.select('td')
            if len(tds) < 5:
                continue
            try:
                # 외국인 순매수: td[2], 기관 순매수: td[4]
                f_txt = tds[2].get_text(strip=True).replace(',', '').replace('+', '')
                i_txt = tds[4].get_text(strip=True).replace(',', '').replace('+', '')
                if f_txt and i_txt and f_txt != '-' and i_txt != '-':
                    foreign_vals.append(int(f_txt))
                    inst_vals.append(int(i_txt))
            except Exception:
                continue
            if len(foreign_vals) >= 5:
                break

        if not foreign_vals:
            return None, None, 0, 0

        inv_df = pd.DataFrame({'외국인': foreign_vals, '기관': inst_vals})
        cols   = ('외국인', '기관')

        inst_streak = for_streak = 0
        for val in inst_vals:
            if val > 0: inst_streak += 1
            else: break
        for val in foreign_vals:
            if val > 0: for_streak += 1
            else: break

        return inv_df, cols, inst_streak, for_streak

    except Exception as e:
        print(f"  ⚠️  수급 조회 실패({ticker}): {e}")
        return None, None, 0, 0


# ══════════════════════════════════════════════════════════════
#  채점
# ══════════════════════════════════════════════════════════════

def score_stock(df, inv_df, cols, inst_streak, for_streak):
    df = df.copy()
    df['MA20']     = df['Close'].rolling(20).mean()
    df['Vol_MA20'] = df['Volume'].rolling(20).mean()
    df['RSI']      = calc_rsi(df['Close'])
    df['OBV']      = calc_obv(df)

    cur      = df['Close'].iloc[-1]
    prev_hi  = df['High'].iloc[-2]
    rsi      = df['RSI'].iloc[-1]
    rsi_prev = df['RSI'].iloc[-2]
    vol_ma20 = df['Vol_MA20'].iloc[-1]
    cur_vol  = df['Volume'].iloc[-1]
    bd = {}
    meta = {}

    # [A] 세력전환 — OBV 0선 돌파 (최대 50점)
    # 단기 OBV 상승만으론 높은 점수 안 나오게 기준 강화
    obv_now   = df['OBV'].iloc[-1]
    obv_prev  = df['OBV'].iloc[-2]
    obv_5d    = df['OBV'].iloc[-6] if len(df) >= 6 else df['OBV'].iloc[0]
    obv_chg   = (obv_now - obv_5d) / (abs(obv_5d) + 1) * 100
    obv_cross = bool(obv_now > 0 and obv_prev <= 0)
    a = 0
    if obv_cross:
        a = 50                              # 0선 돌파 = 최고점
    elif obv_chg > 10:
        a = min(int(obv_chg * 1.0), 20)    # 상승률 10%↑ 부터만 점수, max 20점
    bd['세력전환'] = a
    meta['obv_chg']   = round(float(obv_chg), 2)
    meta['obv_cross'] = obv_cross

    # [B] 세력매집 기간 — DS단석 핵심 (최대 50점, 기존 30점에서 상향)
    # 장기 매집일수록 대시세 확률↑ → 가중치 가장 높게
    obv_diff   = df['OBV'].diff().iloc[-120:]
    green_days = int((obv_diff > 0).sum())
    b = 0
    if   green_days >= 120: b = 50   # 6개월 매집 — 최강 신호
    elif green_days >= 80:  b = 40   # 4개월
    elif green_days >= 60:  b = 30   # 3개월 — DS단석 기준
    elif green_days >= 40:  b = 18
    elif green_days >= 20:  b = 8
    bd['세력매집'] = b
    meta['green_days'] = green_days

    # [C] 매매신호 — 0점이면 페널티 (최대 30점, 0점 시 -20점)
    low_14  = df['Low'].rolling(14).min()
    high_14 = df['High'].rolling(14).max()
    denom   = (high_14 - low_14).iloc[-1]
    stoch_k = (cur - low_14.iloc[-1]) / denom * 100 if denom > 0 else 50
    stoch_p = (df['Close'].iloc[-2] - low_14.iloc[-2]) / \
              max((high_14.iloc[-2] - low_14.iloc[-2]), 1) * 100
    signal_now  = stoch_k * 0.6 + rsi * 0.4
    signal_prev = stoch_p * 0.6 + rsi_prev * 0.4
    c = 0
    signal_cross = bool(signal_now > 30 and signal_prev <= 30)
    if signal_cross:
        c = 30
    elif signal_now > 60:
        c = 25                             # 강한 신호 구간
    elif 40 <= signal_now <= 60:
        c = 20                             # 중립 신호 구간
    elif 25 <= signal_now < 40:
        c = 10                             # 약한 신호
    else:
        c = -20                            # 신호 없음 = 페널티
    bd['매매신호']       = c
    meta['signal']       = round(float(signal_now), 1)
    meta['signal_cross'] = signal_cross

    # [D] 거래량 스파이크 (최대 40점, 유지)
    vol_ma20_ser = df['Volume'].rolling(20).mean()
    recent_60    = df.iloc[-60:]
    spike_ratios = recent_60['Volume'] / (vol_ma20_ser.iloc[-60:] + 1)
    max_spike    = spike_ratios.max() if not spike_ratios.empty else 0
    cur_ratio    = cur_vol / (vol_ma20 + 1)
    d = 0
    if   max_spike >= 10: d = 40
    elif max_spike >= 5:  d = 30
    elif max_spike >= 3:  d = 20
    elif max_spike >= 2:  d = 10
    bd['거래량스파이크'] = d
    meta['max_spike'] = round(float(max_spike), 1)
    meta['vol_ratio'] = round(float(cur_ratio), 2)

    # [F] 수급 강도 (최대 15점)
    f = 0
    supply_text = "정보없음"
    if inv_df is not None and cols:
        fc_col, ic_col = cols  # '외국인', '기관'
        fd        = int((inv_df[fc_col] > 0).sum())
        id_       = int((inv_df[ic_col] > 0).sum())
        both_days = int(((inv_df[fc_col] > 0) & (inv_df[ic_col] > 0)).sum())
        f = min(inst_streak * 3 + for_streak * 2 + both_days * 2, 15)
        if fd > 0 and id_ > 0:
            supply_text = "외인+기관 양매수"
        elif id_ > 0:
            supply_text = "기관매수"
        elif fd > 0:
            supply_text = "외인매수"
    bd['수급강도']      = f
    meta['supply_text'] = supply_text
    meta['inst_streak'] = inst_streak
    meta['for_streak']  = for_streak
    meta['rsi']         = round(float(rsi), 1)
    meta['bb_compress'] = 0

    # ── 대상승 사전 패턴 ───────────────────────────────────────

    # [H] 스파이크 30일 이내 (최대 15점)
    h = 0
    days_since = -1
    try:
        vol_ma20_ser2 = df['Volume'].rolling(20).mean()
        recent_30     = df.iloc[-30:]
        spike_mask_30 = recent_30['Volume'] >= vol_ma20_ser2.iloc[-30:] * 2.0
        if spike_mask_30.any():
            last_spike_30 = recent_30[spike_mask_30].iloc[-1]
            days_since    = len(df) - df.index.get_loc(last_spike_30.name) - 1
            if   days_since <= 5:  h = 15
            elif days_since <= 10: h = 12
            elif days_since <= 20: h = 8
            elif days_since <= 30: h = 4
    except Exception:
        pass
    bd['근거리스파이크'] = h
    meta['days_since_spike'] = days_since

    # [I] OBV 0선 직전 구간 (최대 10점)
    i = 0
    obv_pct = 0.0
    try:
        obv_range = abs(df['OBV'].iloc[-60:].max() - df['OBV'].iloc[-60:].min()) + 1
        obv_pct   = obv_now / obv_range * 100
        if obv_now < 0 and -15 <= obv_pct <= 0:
            i = 10
        elif obv_now < 0 and -30 <= obv_pct < -15:
            i = 5
    except Exception:
        pass
    bd['OBV0선직전'] = i
    meta['obv_pct'] = round(float(obv_pct), 1)

    # ── 대시세 사전 패턴 ───────────────────────────────────────

    # [J] 스파이크 반복 + 바닥 수렴 패턴 (최대 35점 — 70%)
    # 180일 내 2회 이상 스파이크 발생 후 현재 바닥 수렴 중
    j = 0
    spike_count  = 0
    last_spikes  = []
    try:
        vol_ma20_full = df['Volume'].rolling(20).mean()
        spike_mask_full = df['Volume'] >= vol_ma20_full * 2.0

        # 스파이크 날짜 추출 (연속된 스파이크는 1회로 묶음)
        spike_dates = df.index[spike_mask_full].tolist()
        grouped = []
        for d in spike_dates:
            if not grouped or (d - grouped[-1][-1]).days > 5:
                grouped.append([d])
            else:
                grouped[-1].append(d)
        spike_count = len(grouped)
        last_spikes = [g[-1] for g in grouped[-3:]]  # 최근 3회

        if spike_count >= 2:
            # 마지막 스파이크 이후 가격 수렴 확인
            # 현재가가 마지막 스파이크 종가 대비 -15%~+10% = 바닥 수렴 구간
            last_spike_close = float(df.loc[last_spikes[-1], 'Close'])
            price_since = (cur - last_spike_close) / last_spike_close * 100
            converging  = -15 <= price_since <= 10

            # 스파이크 간 간격 — 2회 사이 가격이 다시 내려왔는지
            retest = False
            if len(last_spikes) >= 2:
                idx1 = df.index.get_loc(last_spikes[-2])
                idx2 = df.index.get_loc(last_spikes[-1])
                between = df.iloc[idx1:idx2]['Close']
                spike1_close = float(df.loc[last_spikes[-2], 'Close'])
                if len(between) > 0:
                    min_between = float(between.min())
                    retest = (min_between / spike1_close) < 0.95  # 5% 이상 눌림

            if spike_count >= 3 and converging and retest:
                j = 35
            elif spike_count >= 2 and converging and retest:
                j = 28
            elif spike_count >= 2 and converging:
                j = 18
            elif spike_count >= 2:
                j = 8
    except Exception:
        pass
    bd['반복스파이크'] = j
    meta['spike_count']  = spike_count
    meta['last_spikes']  = [str(d.date()) for d in last_spikes] if last_spikes else []

    # [L] 이격도 — MA20/MA60 대비 현재가 위치 (최대 20점)
    # 바닥에 가까울수록 고점수 — 민혁님 컨셉 핵심
    l = 0
    disp20 = disp60 = 0.0
    try:
        ma20_v = float(df['Close'].rolling(20).mean().iloc[-1])
        ma60_v = float(df['Close'].rolling(60).mean().iloc[-1]) if len(df) >= 60 else ma20_v
        disp20 = round((cur / ma20_v - 1) * 100, 1) if ma20_v > 0 else 0.0
        disp60 = round((cur / ma60_v - 1) * 100, 1) if ma60_v > 0 else 0.0

        # MA20 이격도 기준 채점
        # -3~+3%  : MA20에 붙어있음 = 바닥 수렴 최적 → 20점
        # +3~+8%  : 약간 올라온 상태 → 12점
        # -8~-3%  : MA20 아래 눌림 → 10점
        # +8~+15% : 어느정도 오름 → 5점
        # 나머지   : 0점
        if   -3 <= disp20 <= 3:   l = 20
        elif  3 <  disp20 <= 8:   l = 12
        elif -8 <= disp20 < -3:   l = 10
        elif  8 <  disp20 <= 15:  l = 5
    except Exception:
        pass
    bd['이격도'] = l
    meta['disp20'] = disp20
    meta['disp60'] = disp60

    # [M] 횡보 기간 — 박스권이 길수록 에너지 축적 (최대 20점)
    # 최근 N일간 고저 변동폭이 20% 이내인 구간 길이 측정
    m = 0
    sideways_days = 0
    try:
        for window in [120, 90, 60, 40]:
            if len(df) < window:
                continue
            seg       = df.iloc[-window:]
            hi        = float(seg['High'].max())
            lo        = float(seg['Low'].min())
            rng_pct   = (hi - lo) / lo * 100 if lo > 0 else 999
            if rng_pct <= 25:          # 25% 이내 박스권 유지
                sideways_days = window
                break                  # 가장 긴 구간 우선

        if   sideways_days >= 120: m = 20
        elif sideways_days >= 90:  m = 15
        elif sideways_days >= 60:  m = 10
        elif sideways_days >= 40:  m = 5
    except Exception:
        pass
    bd['횡보기간'] = m
    meta['sideways_days'] = sideways_days

    # [N] 52주 고점 괴리율 — 고점과 멀수록 상승 여력 큼 (최대 20점)
    n = 0
    high52_gap = 0.0
    try:
        high52     = float(df['High'].iloc[-252:].max()) if len(df) >= 252 else float(df['High'].max())
        high52_gap = round((high52 - cur) / high52 * 100, 1)  # 양수 = 고점 대비 하락률

        if   high52_gap >= 60: n = 20   # 60% 이상 하락 — 극바닥
        elif high52_gap >= 45: n = 17
        elif high52_gap >= 35: n = 13
        elif high52_gap >= 25: n = 8
        elif high52_gap >= 15: n = 4
    except Exception:
        pass
    bd['고점괴리'] = n
    meta['high52_gap'] = high52_gap

    return sum(bd.values()), bd, meta


# ══════════════════════════════════════════════════════════════
#  메인 스캔
# ══════════════════════════════════════════════════════════════

def run_kr_scan():
    global _rising_themes_cache
    _rising_themes_cache = []  # 스캔마다 테마 새로 조회

    today_str  = get_market_date()
    start_260d = get_start_date(today_str, 180)
    start_10d  = get_start_date(today_str, 10)
    label      = now_label()

    # 레짐 제거 — 고정 설정
    SCORE_THRESHOLD = 70
    RS_THRESHOLD    = 0

    print(f"📅 기준일: {today_str} | {label}")
    print(f"🎯 전략: 선행매집 스캔 | TOP {TOP_N} | 임계 {SCORE_THRESHOLD}점 | RS ≥ {RS_THRESHOLD:+d}%\n")

    # ── 시총 사전 필터링 ────────────────────────────────────────
    print(f"⚡ 시총 사전 필터링 ({MKTCAP_MIN}억↑ ~ {MKTCAP_MAX}억↓)...")
    mktcap_cache = {}
    all_tickers  = []
    try:
        for market in ["KOSPI", "KOSDAQ"]:
            df_cap   = fdr.StockListing(market)
            code_col = 'Code' if 'Code' in df_cap.columns else df_cap.columns[0]
            cap_col  = _find_cap_col(df_cap)
            if not cap_col:
                raise ValueError(f"{market} 시총 컬럼 없음")

            # 업종·제품 컬럼 탐색
            sector_col  = next((c for c in ['Sector','sector','업종'] if c in df_cap.columns), None)
            product_col = next((c for c in ['Industry','industry','주요제품'] if c in df_cap.columns), None)

            for _, row in df_cap.iterrows():
                ticker_s = str(row[code_col]).zfill(6)
                cap_raw  = row[cap_col]
                if not cap_raw or cap_raw != cap_raw:
                    continue
                cap = int(cap_raw / 1e8) if cap_raw > 1e6 else int(cap_raw)
                mktcap_cache[ticker_s] = cap

                if MKTCAP_MIN <= cap <= MKTCAP_MAX:
                    all_tickers.append(ticker_s)
        print(f"  → 필터 통과: {len(all_tickers)}종목")
    except Exception as e:
        print(f"  ❌ 시총 필터 실패: {e} → 스캔 중단")
        return []

    # 상승 테마 사전 조회 (1회)
    rising_themes = fetch_rising_themes()

    candidates = []
    log = dict(total=len(all_tickers), penny=0, mktcap=len(all_tickers),
               bottom=0, uptrend=0, vol_spike=0, rs_pass=0,
               obv_ok=0, signal_ok=0, trend_ok=0, pullback_ok=0,
               vol_contract_ok=0, seforce=0, final=0)
    lock = threading.Lock()
    passed_stage1 = []

    def scan_ticker_stage1(ticker):
        try:
            df    = fdr.DataReader(ticker, start_260d, today_str)
            if len(df) < 120:
                return
            close = df['Close']
            vol   = df['Volume']
            cur   = close.iloc[-1]

            if cur < 1000:
                return
            with lock:
                log['penny'] += 1

            mktcap = mktcap_cache.get(ticker)
            if mktcap is None or not (MKTCAP_MIN <= mktcap <= MKTCAP_MAX):
                return

            low52       = df['Low'].iloc[-252:].min() if len(df) >= 252 else df['Low'].min()
            if low52 <= 0:
                return
            rebound_pct = (cur - low52) / low52 * 100
            if not (0 <= rebound_pct <= 50):   # 30→50%
                return
            with lock:
                log['bottom'] += 1

            vol_ma20   = vol.rolling(20).mean()
            ma20_s     = close.rolling(20).mean()
            ma20_slope = ma20_s.iloc[-1] / ma20_s.iloc[-20] if ma20_s.iloc[-20] > 0 else 1
            if not (0.95 <= ma20_slope <= 1.20):   # 0.98~1.15 → 0.95~1.20
                return
            if cur < ma20_s.iloc[-1] * 0.95:       # 0.97 → 0.95
                return
            with lock:
                log['uptrend'] += 1

            recent_60  = df.iloc[-60:]
            spike_mask = recent_60['Volume'] >= vol_ma20.iloc[-60:] * 1.5   # 2.0→1.5배
            if not spike_mask.any():
                return
            last_spike    = recent_60[spike_mask].iloc[-1]
            if cur <= last_spike['Low']:
                return
            si            = df.index.get_loc(last_spike.name)
            af            = df.iloc[si + 1:]
            silence_ratio = 0.0
            if len(af) >= 3:
                sil = af['Volume'].mean() / (last_spike['Volume'] + 1)
                silence_ratio = round(float(sil), 2)
                if sil > 1.2:                  # 0.85→1.2
                    return
            with lock:
                log['vol_spike'] += 1

            rs = calc_relative_strength(df, None)
            if rs < RS_THRESHOLD:
                return
            with lock:
                log['rs_pass'] += 1

            # [F1] 선행 매집 — OBV↑ / 가격 정체
            obv         = calc_obv(df)
            obv_now     = obv.iloc[-1]
            obv_20ago   = obv.iloc[-20] if len(obv) >= 20 else obv.iloc[0]
            obv_change  = (obv_now - obv_20ago) / (abs(obv_20ago) + 1) * 100
            price_20ago = close.iloc[-20] if len(close) >= 20 else close.iloc[0]
            price_change = (cur - price_20ago) / price_20ago * 100 if price_20ago > 0 else 0
            if obv_change <= 0 or price_change > 25:   # 15→25%
                return
            with lock:
                log['obv_ok'] += 1

            # [F2] 매매신호 25~80 (기존 30~70)
            rsi_ser   = calc_rsi(close)
            rsi_cur   = float(rsi_ser.iloc[-1])
            low_14    = df['Low'].rolling(14).min().iloc[-1]
            high_14   = df['High'].rolling(14).max().iloc[-1]
            denom     = high_14 - low_14
            stoch_cur = (cur - low_14) / denom * 100 if denom > 0 else 50.0
            signal_cur = stoch_cur * 0.6 + rsi_cur * 0.4
            if not (25 <= signal_cur <= 80):           # 30~70→25~80
                return
            with lock:
                log['signal_ok'] += 1

            # [F3] 스파이크 후 가격 반응 +0~15%
            spike_close = float(last_spike['Close'])
            post_high   = float(af['High'].max()) if len(af) >= 1 else spike_close
            if spike_close <= 0:
                return
            post_return = post_high / spike_close
            if not (1.00 <= post_return <= 1.35):   # 1.15→1.35
                return
            with lock:
                log['trend_ok'] += 1

            with lock:
                passed_stage1.append((
                    ticker, df, vol_ma20, mktcap, rebound_pct, cur,
                    vol.iloc[-1], vol_ma20.iloc[-1], silence_ratio, rs
                ))
        except Exception:
            return

    print(f"🔍 1단계: 병렬 스크리닝 (10스레드)...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scan_ticker_stage1, t): t for t in all_tickers}
        done = 0
        for fut in as_completed(futures):
            done += 1
            if done % 200 == 0:
                print(f"  진행 {done}/{len(all_tickers)} | 1차 통과: {len(passed_stage1)}건")

    print(f"\n2단계: 수급 스크리닝 {len(passed_stage1)}건...")
    for row in passed_stage1:
        ticker, df, vol_ma20, mktcap, rebound_pct, cur, cur_vol, cur_vm20, silence_ratio, rs = row
        try:
            inv_df, cols, inst_streak, for_streak = get_investor_detail(ticker, start_10d, today_str)
            if inv_df is not None and cols is not None:
                fc_col, ic_col = cols
                any_buy = int(((inv_df[fc_col] > 0) | (inv_df[ic_col] > 0)).sum())
                if any_buy >= 1:
                    log['seforce'] += 1
            else:
                inv_df = None; cols = None
                inst_streak = for_streak = 0

            total_score, breakdown, meta = score_stock(df, inv_df, cols, inst_streak, for_streak)
            meta['mktcap']        = mktcap
            meta['rebound_pct']   = round(float(rebound_pct), 1)
            meta['cur_close']     = int(cur)
            meta['vol_ratio']     = round(float(cur_vol) / (float(cur_vm20) + 1), 2)
            meta['silence_ratio'] = silence_ratio
            meta['rs']            = rs
            meta['trade']         = calc_trade_levels(df, cur)
            # 스파이크 뉴스는 TOP5 확정 후 수집 (속도 최적화)
            meta['spike_news']    = []

            if total_score >= SCORE_THRESHOLD:
                candidates.append((total_score, ticker, breakdown, meta, df))
                log['final'] += 1
        except Exception:
            continue

    # ── 필터 현황 출력 ─────────────────────────────────────────
    print(f"\n📊 [필터 현황 — {regime} 레짐]")
    rs_label = f"⑥ RS ≥ {RS_THRESHOLD:+d}%"
    for lbl, key in [
        ("전체", "total"), ("① 동전주 제외", "penny"),
        ("② 시총 필터", "mktcap"), ("③ 저점대비 0~30%", "bottom"),
        ("④ MA20 박스권", "uptrend"), ("⑤ 거래량 스파이크", "vol_spike"),
        (rs_label, "rs_pass"),
        ("⑦ 선행매집(OBV↑/가격↔)", "obv_ok"),
        ("⑧ 매매신호 30~70", "signal_ok"),
        ("⑨ 스파이크후 +0~15%", "trend_ok"),
        ("⑩ 안정박스 -10%이내", "pullback_ok"),
        ("⑪ 변동성수축 ≤25%", "vol_contract_ok"),
        ("⑫ 수급 확인", "seforce"),
        ("최종 통과", "final"),
    ]:
        print(f"  {lbl:<24} {log[key]:>5}건")

    candidates.sort(key=lambda x: x[0], reverse=True)
    top_raw = candidates[:TOP_N]

    if not top_raw:
        print("⚠️  오늘 조건 충족 종목 없음")

    final_picks = []
    print(f"\n🏆 [TOP {TOP_N}] — 실시간 가격 조회 중...")
    for rank, (total_score, ticker, breakdown, meta, df) in enumerate(top_raw, 1):
        # ── 종목명 ──────────────────────────────────────────────
        name = ticker
        try:
            data = _fetch_naver_basic(ticker)
            name = data.get('stockName') or data.get('name') or ticker
        except Exception:
            pass
        if name == ticker:
            try:
                for mkt in ["KOSPI", "KOSDAQ"]:
                    df_lst  = fdr.StockListing(mkt)
                    col     = 'Code' if 'Code' in df_lst.columns else df_lst.columns[0]
                    nm_col  = next((c for c in ['Name','name'] if c in df_lst.columns), None)
                    if nm_col:
                        row = df_lst[df_lst[col].astype(str).str.zfill(6) == ticker]
                        if not row.empty:
                            name = row.iloc[0][nm_col]; break
            except Exception:
                pass

        # ── 실시간 가격 → 트레이드 플랜 재산출 ─────────────────
        rt_price = get_realtime_price(ticker)
        if rt_price:
            meta['cur_close'] = rt_price
            meta['trade']     = calc_trade_levels(df, rt_price)
            print(f"  #{rank} {name}({ticker}) | 실시간가: {rt_price:,}원")
        else:
            print(f"  #{rank} {name}({ticker}) | 실시간가 조회 실패 → 전일 종가 사용: {meta['cur_close']:,}원")

        # ── 스파이크 시점 뉴스 수집 (정보용) ───────────────────
        spike_date_strs = meta.get('last_spikes', [])
        meta['spike_news'] = fetch_spike_news(ticker, spike_date_strs)

        summary      = get_company_summary(ticker, name)
        score_100    = int(round(total_score / MAX_SCORE * 100))  # 100점 환산
        star_count   = min(score_100 // 20, 5)
        stars        = "★" * star_count + "☆" * (5 - star_count)
        supply_text  = meta.get('supply_text', '정보없음')
        expected_ret = round(5.0 + (score_100 / 100) * 15.0, 1)
        top_f        = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
        tag_str      = " / ".join([f"{k}" for k, v in top_f[:3] if v > 0])

        final_picks.append({
            "rank": rank, "name": name, "code": ticker,
            "company_summary": summary, "supply": supply_text,
            "cur_price": meta.get('cur_close', 0),
            "score_100": score_100,
            "score": f"{stars} {score_100}점",
            "tags": tag_str, "expected_return": f"{expected_ret}%",
            "score_detail": breakdown,
            "meta": {
                "rsi":           meta.get('rsi', 0),
                "bb_compress":   meta.get('bb_compress', 0),
                "rebound_pct":   meta.get('rebound_pct', 0),
                "vol_ratio":     meta.get('vol_ratio', 0),
                "inst_streak":   meta.get('inst_streak', 0),
                "for_streak":    meta.get('for_streak', 0),
                "mktcap":        meta.get('mktcap', 0),
                "silence_ratio": meta.get('silence_ratio', 0),
                "green_days":    meta.get('green_days', 0),
                "obv_cross":     meta.get('obv_cross', False),
                "signal":        meta.get('signal', 0),
                "max_spike":     meta.get('max_spike', 0),
                "rs":            meta.get('rs', 0),
                "trade":         meta.get('trade', None),
                "regime":        regime,
                "rt_price_used": rt_price is not None,
                "spike_count":   meta.get('spike_count', 0),
                "disp20":        meta.get('disp20', 0.0),
                "disp60":        meta.get('disp60', 0.0),
                "last_spikes":   meta.get('last_spikes', []),
                "spike_news":    meta.get('spike_news', []),
                "sideways_days": meta.get('sideways_days', 0),
                "high52_gap":    meta.get('high52_gap', 0.0),
            }
        })

    # ── 결과 저장 ───────────────────────────────────────────────
    history_data = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            try:
                history_data = json.load(f)
            except Exception:
                history_data = []
    history_data.append({
        "date": today_str, "label": label,
        "picks": final_picks
    })
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4, default=json_safe)

    final_output = {
        "today_picks": final_picks, "scan_label": label,
        "total_candidates": log['final'], "total_screened": log['total'],
        "filter_log": log, "base_date": today_str,
    }
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4, default=json_safe)

    print(f"\n🏁 완료! TOP {len(final_picks)}종목")
    return final_picks


# ══════════════════════════════════════════════════════════════
#  텔레그램 메시지 조립
# ══════════════════════════════════════════════════════════════

def build_telegram_message(picks):
    today_str = get_market_date()
    label     = now_label()

    if not picks:
        return "⚠️ 오늘 조건 충족 종목 없음"

    lines = [
        f"🏆 <b>KR 선행매집 TOP {len(picks)} — {ko_date(today_str)} {label}</b>",
        "━" * 24,
    ]

    for p in picks:
        chart_url  = f"https://m.stock.naver.com/domestic/stock/{p['code']}/total"
        trade      = p['meta'].get('trade')
        rs_val     = p['meta'].get('rs', 0)
        rt_flag    = "📡 실시간" if p['meta'].get('rt_price_used') else "📋 전일종가"
        sd         = p.get('score_detail', {})
        score_100  = p.get('score_100', 0)

        # 시각화 바 (10칸 = 10점)
        filled = int(score_100 / 10)
        bar    = '█' * filled + '░' * (10 - filled)

        gather_100   = normalize_score(sd.get('세력매집',     0), 50)
        spike_100    = normalize_score(sd.get('거래량스파이크', 0), 40)
        signal_100   = normalize_score(sd.get('매매신호',      0), 30)
        near_100     = normalize_score(sd.get('근거리스파이크', 0), 15)
        obv0_100     = normalize_score(sd.get('OBV0선직전',    0), 10)
        repeat_100   = normalize_score(sd.get('반복스파이크',  0), 35)
        disp_100     = normalize_score(sd.get('이격도',        0), 20)
        side_100     = normalize_score(sd.get('횡보기간',      0), 20)
        gap_100      = normalize_score(sd.get('고점괴리',      0), 20)

        spike_cnt     = p['meta'].get('spike_count', 0)
        disp20_val    = p['meta'].get('disp20', 0.0)
        disp60_val    = p['meta'].get('disp60', 0.0)
        sideways_days = p['meta'].get('sideways_days', 0)
        high52_gap    = p['meta'].get('high52_gap', 0.0)
        spike_news    = p['meta'].get('spike_news', [])

        ds_score    = sd.get('근거리스파이크', 0) + sd.get('OBV0선직전', 0)
        ds_flag     = "🔥 <b>DS패턴 감지</b>" if ds_score >= 20 else ""
        repeat_flag = "🔁 <b>반복패턴 감지</b>" if sd.get('반복스파이크', 0) >= 28 else ""

        lines += [
            f"#{p['rank']} <b><a href='{chart_url}'>{p['name']}</a></b> ({p['code']})",
            f"  🏢 {p['company_summary']}",
            f"",
            f"  <code>{bar}</code> <b>{score_100}점</b> {p['score'].split(' ')[0]}",
            f"",
            f"  💰 현재가: {p['cur_price']:,}원 {rt_flag} | RS: {rs_val:+}%",
            f"  📈 기대수익: {p['expected_return']}",
            f"",
            f"  📊 <b>핵심 지표</b>",
            f"    {grade_emoji(gather_100)} 세력매집        {gather_100}점",
            f"    {grade_emoji(spike_100)} 거래량스파이크   {spike_100}점",
            f"    {grade_emoji(signal_100)} 매매신호        {signal_100}점",
            f"",
            f"  📐 <b>이격도</b>  MA20 {disp20_val:+.1f}% | MA60 {disp60_val:+.1f}%",
            f"    {grade_emoji(disp_100)} 바닥 근접도      {disp_100}점",
            f"    {grade_emoji(side_100)} 횡보 {sideways_days}일      {side_100}점",
            f"    {grade_emoji(gap_100)} 고점대비 -{high52_gap:.1f}%  {gap_100}점",
            f"",
            f"  💎 <b>대상승 패턴</b>{('  ' + ds_flag) if ds_flag else ''}",
            f"    {grade_emoji(near_100)} 근거리스파이크  {near_100}점",
            f"    {grade_emoji(obv0_100)} OBV 0선직전     {obv0_100}점",
            f"    {grade_emoji(repeat_100)} 스파이크 {spike_cnt}회 반복  {repeat_100}점{('  ' + repeat_flag) if repeat_flag else ''}",
            f"",
            f"  🏦 수급: {p['supply']}",
        ]

        if spike_news:
            lines.append(f"")
            lines.append(f"  🗞 <b>스파이크 시점 뉴스</b> (잠재 테마 힌트)")
            for sn in spike_news:
                lines.append(f"    📌 [{sn['spike']}] {sn['title']}")

        if trade:
            risk_won   = int(trade['risk_pct']   * 10000)
            reward_won = int(trade['reward_pct'] * 10000)
            lines += [
                f"",
                f"  🎯 <b>트레이드 플랜</b>",
                f"    ├ 진입: {trade['entry']:,}원",
                f"    ├ 🛑 손절: {trade['stop_loss']:,}원 (-{trade['risk_pct']}%)",
                f"    ├ 🥇 1차: {trade['target_1']:,}원 (+{trade['reward_pct']}%)",
                f"    └ 🥈 2차: {trade['target_2']:,}원",
                f"    💡 <b>{risk_won:,}원 잃을 각오, {reward_won:,}원 벌 자리</b>",
                f"    <i>(100만원 투자 기준)</i>",
            ]

        lines.append("")

    lines.append("━" * 24)
    lines.append("💡 <i>손절은 진입 즉시 예약 주문 — 규율이 곧 승률</i>")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  엔트리포인트
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    send_telegram(fetch_macro_summary())
    send_telegram(build_news_briefing())

    picks = run_kr_scan()
    send_telegram(build_telegram_message(picks))
