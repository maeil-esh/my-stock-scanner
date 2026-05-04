"""
engine_kr.py — 국장 바닥반등+거래량 스캐너 (PRO v4.1)
실행: python engine_kr.py
스케줄: 09:41 / 13:23 / 16:43 KST

[변경 이력 v4.1] — QUANT VALUE 엔진 개선
  ① CAPM 잔차 버그 수정 (인덱스 불일치 → 전부 0.0% 문제)
  ② VALUE 1차/2차 병렬화 (56분 → ~15분 목표)
  ③ 모집단 변경: 시총 상위N → 중소형 구간 (1000억~1조)
  ④ 채점 가중치 재조정: 성장률 15→35점, PER/PBR 각 30→20점

[변경 이력 v4.0] — QUANT VALUE 엔진 추가 (별도 섹션)
  기존 BASIC 엔진(v3.6) 완전 유지 + TOP_N 5→3
  ★ BASIC 엔진 내부 코드 무변경

[변경 이력 v3.6] — 적자 기업 필터 추가
[변경 이력 v3.5] — 영업이익률 필터 + 점수 (DART)
[변경 이력 v3.4] — 업종 정보 단순화
[변경 이력 v3.0] — 큰 흐름 매크로 모드 (전면 재설계)
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
TOP_N        = 3
MAX_SCORE    = 220
MKTCAP_MIN   = 1000
MKTCAP_MAX   = 30000

DART_BASE_URL = "https://opendart.fss.or.kr/api"

_naver_basic_cache: dict = {}


def fetch_spike_news(ticker: str, spike_date_strs: list) -> list:
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


# ══════════════════════════════════════════════════════════════
#  DART 영업이익률 (v3.5)
# ══════════════════════════════════════════════════════════════

_dart_corp_code_cache: dict = {}
_dart_op_margin_cache: dict = {}


def _load_dart_corp_codes():
    global _dart_corp_code_cache
    if _dart_corp_code_cache:
        return _dart_corp_code_cache

    api_key = os.environ.get('DART_API_KEY', '')
    if not api_key:
        print("  ⚠️  DART_API_KEY 없음 — 영업이익률 조회 비활성")
        _dart_corp_code_cache = {}
        return {}

    try:
        import zipfile, io, xml.etree.ElementTree as ET
        url = f"{DART_BASE_URL}/corpCode.xml?crtfc_key={api_key}"
        r = requests.get(url, timeout=15)
        
        # ★ 진단: 응답 상태 확인
        print(f"  🔍 DART API 응답: HTTP {r.status_code}, Content-Type: {r.headers.get('Content-Type', 'N/A')}")
        
        if r.status_code != 200:
            print(f"  ⚠️  DART corpCode 다운로드 실패: HTTP {r.status_code}")
            print(f"  🔍 응답 내용 (처음 200자): {r.text[:200]}")
            _dart_corp_code_cache = {}
            return {}

        # ★ 진단: zip 파일인지 확인
        if not r.content.startswith(b'PK'):
            print(f"  ⚠️  DART 응답이 zip 형식이 아님")
            print(f"  🔍 응답 내용 (처음 200자): {r.text[:200]}")
            _dart_corp_code_cache = {}
            return {}

        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            xml_data = zf.read('CORPCODE.xml').decode('utf-8')

        root = ET.fromstring(xml_data)
        for child in root.findall('list'):
            stock_code = (child.findtext('stock_code') or '').strip()
            corp_code  = (child.findtext('corp_code') or '').strip()
            if stock_code and corp_code and stock_code != ' ':
                _dart_corp_code_cache[stock_code.zfill(6)] = corp_code

        print(f"  ✅ DART corpCode 로드: {len(_dart_corp_code_cache)}건")
        return _dart_corp_code_cache
    except Exception as e:
        print(f"  ⚠️  DART corpCode 처리 실패: {e}")
        _dart_corp_code_cache = {}
        return {}


def _get_op_margin_quarters(ticker: str, debug: bool = False) -> dict:
    if ticker in _dart_op_margin_cache:
        return _dart_op_margin_cache[ticker]

    api_key = os.environ.get('DART_API_KEY', '')
    if not api_key:
        return {'quarters': [], 'annual': None}

    corp_codes = _load_dart_corp_codes()
    corp_code = corp_codes.get(ticker)
    if not corp_code:
        if debug:
            print(f"    🔍 DART {ticker}: corp_code 없음")
        result = {'quarters': [], 'annual': None}
        _dart_op_margin_cache[ticker] = result
        return result

    quarters = []
    annual   = None
    current_year = datetime.datetime.now().year

    quarter_codes = [
        ('11014', '3Q', current_year,     False),
        ('11012', '2Q', current_year,     False),
        ('11013', '1Q', current_year,     False),
        ('11011', '연간', current_year - 1, True),
        ('11014', '3Q', current_year - 1, False),
        ('11012', '2Q', current_year - 1, False),
        ('11013', '1Q', current_year - 1, False),
        ('11011', '연간', current_year - 2, True),
    ]

    for reprt_code, q_label, year, is_annual in quarter_codes:
        try:
            url = f"{DART_BASE_URL}/fnlttSinglAcntAll.json"
            params = {
                'crtfc_key':  api_key,
                'corp_code':  corp_code,
                'bsns_year':  str(year),
                'reprt_code': reprt_code,
                'fs_div':     'CFS',
            }
            r = requests.get(url, params=params, timeout=8)
            data = r.json()

            if data.get('status') != '000':
                params['fs_div'] = 'OFS'
                r = requests.get(url, params=params, timeout=8)
                data = r.json()
                if data.get('status') != '000':
                    continue

            items = data.get('list', [])
            revenue = op_profit = None

            for item in items:
                account = (item.get('account_nm') or '').strip()
                amount  = (item.get('thstrm_amount') or '0').replace(',', '').replace(' ', '')
                if not amount or amount == '-':
                    continue
                try:
                    amount_int = int(amount)
                except ValueError:
                    continue

                if account in ['매출액', '수익(매출액)', '영업수익', '매출']:
                    if revenue is None:
                        revenue = amount_int
                elif account in ['영업이익', '영업이익(손실)']:
                    if op_profit is None:
                        op_profit = amount_int

            if revenue and revenue > 0 and op_profit is not None:
                op_margin = round(op_profit / revenue * 100, 2)
                entry = {
                    'period':    f"{year}{q_label}",
                    'year':      year,
                    'revenue':   revenue,
                    'op_profit': op_profit,
                    'op_margin': op_margin,
                }
                if is_annual:
                    if annual is None:
                        annual = entry
                else:
                    quarters.append(entry)

                if debug:
                    tag = "연간" if is_annual else "분기"
                    print(f"    🔍 DART {ticker} [{tag}] {year}{q_label}: 매출={revenue:,} 영업={op_profit:,} 마진={op_margin}%")

        except Exception as e:
            if debug:
                print(f"    🔍 DART {ticker} {year}{q_label} 실패: {e}")
            continue

    quarters.sort(key=lambda x: x['period'], reverse=True)
    result = {'quarters': quarters[:6], 'annual': annual}
    _dart_op_margin_cache[ticker] = result
    return result


def check_op_margin_filter(ticker: str, debug: bool = False) -> tuple:
    result = _get_op_margin_quarters(ticker, debug=debug)
    quarters = result['quarters']
    annual   = result['annual']

    if annual is None:
        return False, 0, {'reason': '전년도 연간 데이터 없음'}

    if annual['op_profit'] <= 0:
        return False, 0, {
            'reason':        f"전년도 영업이익 적자",
            'annual_year':   annual['year'],
            'annual_op':     annual['op_profit'],
            'annual_margin': annual['op_margin'],
        }

    if len(quarters) < 2:
        return True, 0, {
            'reason':        'DART 분기 데이터 부족',
            'annual_year':   annual['year'],
            'annual_margin': annual['op_margin'],
        }

    latest = quarters[0]
    prev   = quarters[1]
    m_latest = latest['op_margin']
    m_prev   = prev['op_margin']

    if m_latest <= m_prev:
        return False, 0, {
            'reason':   '영업이익률 미증가',
            'q_latest': latest['period'], 'm_latest': m_latest,
            'q_prev':   prev['period'],   'm_prev':   m_prev,
        }

    if m_prev <= 0:
        change_pct = 100.0
    else:
        change_pct = round((m_latest - m_prev) / abs(m_prev) * 100, 1)

    if   change_pct >= 30: score = 20
    elif change_pct >= 10: score = 15
    else:                  score = 5

    info = {
        'q_latest':      latest['period'], 'm_latest':  m_latest,
        'q_prev':        prev['period'],   'm_prev':    m_prev,
        'change_pct':    change_pct,
        'annual_year':   annual['year'],
        'annual_margin': annual['op_margin'],
    }
    return True, score, info


def _fetch_naver_basic(ticker: str) -> dict:
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

        sosok = str(data.get('sosok', ''))
        data['_market'] = 'KOSPI' if sosok == '0' else 'KOSDAQ' if sosok == '1' else ''

        try:
            r2  = requests.get(
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
#  실시간 현재가
# ══════════════════════════════════════════════════════════════

def get_realtime_price(ticker):
    try:
        data      = _fetch_naver_basic(ticker)
        price_str = data.get('closePrice', '')
        if price_str:
            return int(str(price_str).replace(',', ''))
    except Exception:
        pass

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

def _find_cap_col(df):
    candidates = ['Marcap', 'MarCap', 'marcap', 'mktcap', 'MktCap', 'Market Cap']
    for c in candidates:
        if c in df.columns: return c
    for col in df.columns:
        if 'cap' in col.lower(): return col
    return None


def _get_investor_via_pykrx(ticker, start, end):
    try:
        from pykrx import stock
    except ImportError:
        return None

    try:
        def to_pykrx_date(d):
            s = str(d).replace('-', '').replace('.', '')
            return s[:8]

        from_date = to_pykrx_date(start)
        to_date   = to_pykrx_date(end)

        df = stock.get_market_trading_value_by_date(from_date, to_date, ticker)
        if df is None or df.empty:
            return None

        inst_col = next((c for c in df.columns if '기관합계' in c or '기관' == c), None)
        for_col  = next((c for c in df.columns if '외국인합계' in c or '외국인' == c), None)
        if not inst_col or not for_col:
            return None

        df_sorted    = df.sort_index(ascending=False).head(5)
        inst_vals    = df_sorted[inst_col].astype(int).tolist()
        foreign_vals = df_sorted[for_col].astype(int).tolist()

        if not foreign_vals:
            return None

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

    except Exception:
        return None


def _get_investor_via_naver(ticker):
    headers = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'),
        'Accept': ('text/html,application/xhtml+xml,application/xml;q=0.9,'
                   'image/avif,image/webp,*/*;q=0.8'),
        'Accept-Language':  'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding':  'gzip, deflate, br',
        'Referer':          f'https://finance.naver.com/item/main.naver?code={ticker}',
        'Connection':       'keep-alive',
    }

    urls_to_try = [
        f"https://finance.naver.com/item/frgn.naver?code={ticker}&page=1",
        f"https://finance.naver.com/item/frgn.naver?code={ticker}",
    ]

    for url in urls_to_try:
        try:
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code != 200:
                continue
            r.encoding = 'euc-kr'
            soup = BeautifulSoup(r.text, 'html.parser')

            rows = (soup.select('table.type2 tr')
                    or soup.select('table[class*="type2"] tr')
                    or soup.select('table.tb_type1 tr'))
            if not rows:
                continue

            foreign_vals = []
            inst_vals    = []
            for row in rows[1:]:
                tds = row.select('td')
                if len(tds) < 5:
                    continue
                try:
                    f_txt = tds[2].get_text(strip=True).replace(',', '').replace('+', '')
                    i_txt = tds[4].get_text(strip=True).replace(',', '').replace('+', '')
                    if (f_txt and i_txt and f_txt != '-' and i_txt != '-'
                            and f_txt != '\xa0' and i_txt != '\xa0'):
                        foreign_vals.append(int(f_txt))
                        inst_vals.append(int(i_txt))
                except (ValueError, IndexError):
                    continue
                if len(foreign_vals) >= 5:
                    break

            if not foreign_vals:
                continue

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

        except Exception:
            continue

    return None


def get_investor_detail(ticker, start, end):
    result = _get_investor_via_pykrx(ticker, start, end)
    if result is not None:
        return result

    result = _get_investor_via_naver(ticker)
    if result is not None:
        return result

    print(f"  ⚠️  수급 조회 실패({ticker}): pykrx + 네이버 모두 실패")
    return None, None, 0, 0


# ══════════════════════════════════════════════════════════════
#  채점 (BASIC)
# ══════════════════════════════════════════════════════════════

def score_stock(df, inv_df, cols, inst_streak, for_streak):
    df = df.copy()
    df['MA20']     = df['Close'].rolling(20).mean()
    df['Vol_MA20'] = df['Volume'].rolling(20).mean()
    df['RSI']      = calc_rsi(df['Close'])
    df['OBV']      = calc_obv(df)

    cur      = df['Close'].iloc[-1]
    rsi      = df['RSI'].iloc[-1]
    vol_ma20 = df['Vol_MA20'].iloc[-1]
    cur_vol  = df['Volume'].iloc[-1]
    bd   = {}
    meta = {}

    # A. OBV 60일 매집 추세 (최대 30점)
    obv_now = df['OBV'].iloc[-1]
    obv_60_change = 0.0
    a = 0
    try:
        if len(df) >= 60:
            obv_60ago = df['OBV'].iloc[-60]
            obv_60_change = (obv_now - obv_60ago) / (abs(obv_60ago) + 1) * 100
            if   obv_60_change >= 50: a = 30
            elif obv_60_change >= 30: a = 25
            elif obv_60_change >= 15: a = 18
            elif obv_60_change >= 5:  a = 10
            elif obv_60_change > 0:   a = 5
    except Exception:
        pass
    bd['OBV60일매집'] = a
    meta['obv_60_change'] = round(float(obv_60_change), 2)

    # B. OBV 매집 일수 (최대 25점)
    obv_diff   = df['OBV'].diff().iloc[-120:]
    green_days = int((obv_diff > 0).sum())
    b = 0
    if   green_days >= 70: b = 25
    elif green_days >= 60: b = 20
    elif green_days >= 50: b = 15
    elif green_days >= 40: b = 10
    elif green_days >= 30: b = 5
    bd['매집일수'] = b
    meta['green_days'] = green_days

    # C. 거래량 살아있음/회복 (최대 25점)
    c = 0
    vol_alive_ratio = 0.0
    try:
        if len(df) >= 120:
            recent_30  = float(df['Volume'].iloc[-30:].mean())
            prior_90   = float(df['Volume'].iloc[-120:-30].mean())
            if prior_90 > 0:
                vol_alive_ratio = recent_30 / prior_90
                if   vol_alive_ratio >= 1.5:  c = 25
                elif vol_alive_ratio >= 1.2:  c = 20
                elif vol_alive_ratio >= 1.0:  c = 15
                elif vol_alive_ratio >= 0.85: c = 10
                elif vol_alive_ratio >= 0.7:  c = 5
    except Exception:
        pass
    bd['거래량살아있음'] = c
    meta['vol_alive_ratio'] = round(float(vol_alive_ratio), 2)

    # D. 장기 횡보 기간 (최대 25점)
    d_score = 0
    sideways_days = 0
    try:
        for window in [200, 180, 120, 90, 60, 40]:
            if len(df) < window:
                continue
            seg     = df.iloc[-window:]
            hi      = float(seg['High'].max())
            lo      = float(seg['Low'].min())
            rng_pct = (hi - lo) / lo * 100 if lo > 0 else 999
            if rng_pct <= 50:
                sideways_days = window
                break

        if   sideways_days >= 180: d_score = 25
        elif sideways_days >= 120: d_score = 20
        elif sideways_days >= 90:  d_score = 15
        elif sideways_days >= 60:  d_score = 10
        elif sideways_days >= 40:  d_score = 5
    except Exception:
        pass
    bd['횡보기간'] = d_score
    meta['sideways_days'] = sideways_days

    # E. 1~3년 고점 괴리율 (최대 25점)
    e = 0
    high_long_gap = 0.0
    try:
        long_window = min(len(df), 756)
        high_long   = float(df['High'].iloc[-long_window:].max())
        high_long_gap = round((high_long - cur) / high_long * 100, 1)
        if   high_long_gap >= 60: e = 25
        elif high_long_gap >= 50: e = 20
        elif high_long_gap >= 40: e = 15
        elif high_long_gap >= 30: e = 10
        elif high_long_gap >= 20: e = 5
    except Exception:
        pass
    bd['고점괴리'] = e
    meta['high52_gap'] = high_long_gap

    # F. 매매신호 위치 (최대 25점)
    low_14  = df['Low'].rolling(14).min()
    high_14 = df['High'].rolling(14).max()
    denom   = (high_14 - low_14).iloc[-1]
    stoch_k = (cur - low_14.iloc[-1]) / denom * 100 if denom > 0 else 50
    signal_now = stoch_k * 0.6 + rsi * 0.4
    f_score = 0
    if   30 <= signal_now <= 55:  f_score = 25
    elif 25 <= signal_now < 30:   f_score = 18
    elif 55 <  signal_now <= 70:  f_score = 18
    elif 70 <  signal_now <= 85:  f_score = 10
    elif 20 <= signal_now < 25:   f_score = 8
    bd['매매신호'] = f_score
    meta['signal'] = round(float(signal_now), 1)

    # G. 거래량 스파이크 강도 (최대 25점)
    vol_ma20_ser = df['Volume'].rolling(20).mean()
    recent_60_d  = df.iloc[-60:]
    spike_ratios = recent_60_d['Volume'] / (vol_ma20_ser.iloc[-60:] + 1)
    max_spike    = spike_ratios.max() if not spike_ratios.empty else 0
    g = 0
    if   max_spike >= 8: g = 25
    elif max_spike >= 5: g = 20
    elif max_spike >= 3: g = 15
    elif max_spike >= 2: g = 10
    bd['거래량스파이크'] = g
    meta['max_spike'] = round(float(max_spike), 1)
    meta['vol_ratio'] = round(float(cur_vol / (vol_ma20 + 1)), 2)

    # H. 외인/기관 수급 (최대 20점)
    h = 0
    supply_text = "정보없음"
    if inv_df is not None and cols:
        fc_col, ic_col = cols
        fd_count   = int((inv_df[fc_col] > 0).sum())
        id_count   = int((inv_df[ic_col] > 0).sum())
        both_days  = int(((inv_df[fc_col] > 0) & (inv_df[ic_col] > 0)).sum())
        h = min(inst_streak * 3 + for_streak * 2 + both_days * 3, 20)
        if fd_count > 0 and id_count > 0:
            supply_text = "외인+기관 양매수"
        elif id_count > 0:
            supply_text = "기관매수"
        elif fd_count > 0:
            supply_text = "외인매수"
    bd['수급강도'] = h
    meta['supply_text'] = supply_text
    meta['inst_streak'] = inst_streak
    meta['for_streak']  = for_streak
    meta['rsi']         = round(float(rsi), 1)

    # 반복 스파이크 횟수 (참고)
    spike_count = 0
    last_spikes = []
    try:
        vol_ma20_full   = df['Volume'].rolling(20).mean()
        spike_mask_full = df['Volume'] >= vol_ma20_full * 2.0
        spike_dates     = df.index[spike_mask_full].tolist()
        grouped = []
        for sd in spike_dates:
            if not grouped or (sd - grouped[-1][-1]).days > 5:
                grouped.append([sd])
            else:
                grouped[-1].append(sd)
        spike_count = len(grouped)
        last_spikes = [g[-1] for g in grouped[-3:]]
    except Exception:
        pass
    meta['spike_count'] = spike_count
    meta['last_spikes'] = [str(d.date()) for d in last_spikes] if last_spikes else []

    disp20 = disp60 = 0.0
    try:
        ma20_v = float(df['Close'].rolling(20).mean().iloc[-1])
        ma60_v = float(df['Close'].rolling(60).mean().iloc[-1]) if len(df) >= 60 else ma20_v
        disp20 = round((cur / ma20_v - 1) * 100, 1) if ma20_v > 0 else 0.0
        disp60 = round((cur / ma60_v - 1) * 100, 1) if ma60_v > 0 else 0.0
    except Exception:
        pass
    meta['disp20'] = disp20
    meta['disp60'] = disp60

    return sum(bd.values()), bd, meta


# ══════════════════════════════════════════════════════════════
#  메인 스캔 (BASIC)
# ══════════════════════════════════════════════════════════════

def run_kr_scan():
    today_str  = get_market_date()
    start_260d = get_start_date(today_str, 180)
    start_10d  = get_start_date(today_str, 10)
    label      = now_label()

    kospi_df = None
    for sym in ['KS11', '^KS11', 'KOSPI']:
        try:
            df_k = fdr.DataReader(sym, start_260d, today_str)
            if df_k is not None and len(df_k) >= 60:
                kospi_df = df_k
                break
        except Exception:
            continue

    SCORE_THRESHOLD_100 = 50

    print(f"📅 기준일: {today_str} | {label}")
    print(f"🎯 전략: 큰 흐름 매크로 스캔 (v3.0) | TOP {TOP_N} | 임계 {SCORE_THRESHOLD_100}점 (200점→100환산)\n")

    print(f"⚡ 시총 사전 필터링 ({MKTCAP_MIN}억↑ ~ {MKTCAP_MAX}억↓)...")
    mktcap_cache = {}
    market_cache = {}
    all_tickers  = []
    try:
        for market in ["KOSPI", "KOSDAQ"]:
            df_cap   = fdr.StockListing(market)
            code_col = 'Code' if 'Code' in df_cap.columns else df_cap.columns[0]
            cap_col  = _find_cap_col(df_cap)
            if not cap_col:
                raise ValueError(f"{market} 시총 컬럼 없음")

            for _, row in df_cap.iterrows():
                ticker_s = str(row[code_col]).zfill(6)
                cap_raw  = row[cap_col]
                if not cap_raw or cap_raw != cap_raw:
                    continue
                cap = int(cap_raw / 1e8) if cap_raw > 1e6 else int(cap_raw)
                mktcap_cache[ticker_s] = cap
                market_cache[ticker_s] = market

                if MKTCAP_MIN <= cap <= MKTCAP_MAX:
                    all_tickers.append(ticker_s)
        print(f"  → 필터 통과: {len(all_tickers)}종목")
    except Exception as e:
        print(f"  ❌ 시총 필터 실패: {e} → 스캔 중단")
        return []

    candidates    = []
    log = dict(total=len(all_tickers), penny=0, mktcap=len(all_tickers),
               bottom=0, high_gap=0, sideways=0,
               vol_alive=0, vol_recent=0,
               obv_trend=0, signal_ok=0,
               seforce=0, final=0)
    lock          = threading.Lock()
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

            low52 = df['Low'].iloc[-252:].min() if len(df) >= 252 else df['Low'].min()
            if low52 <= 0:
                return
            rebound_pct = (cur - low52) / low52 * 100
            if not (0 <= rebound_pct <= 80):
                return
            with lock:
                log['bottom'] += 1

            long_window   = min(len(df), 756)
            high_long     = df['High'].iloc[-long_window:].max()
            if high_long <= 0:
                return
            long_gap_pct  = (high_long - cur) / high_long * 100
            if long_gap_pct < 30:
                return
            with lock:
                log['high_gap'] += 1

            sideways_days_f = 0
            for window_s in [200, 120, 90, 60]:
                if len(df) < window_s:
                    continue
                seg_s   = df.iloc[-window_s:]
                hi_s    = float(seg_s['High'].max())
                lo_s    = float(seg_s['Low'].min())
                rng_pct = (hi_s - lo_s) / lo_s * 100 if lo_s > 0 else 999
                if rng_pct <= 50:
                    sideways_days_f = window_s
                    break
            if sideways_days_f < 60:
                return
            with lock:
                log['sideways'] += 1

            if len(vol) >= 120:
                recent_30  = float(vol.iloc[-30:].mean())
                prior_90   = float(vol.iloc[-120:-30].mean())
                if prior_90 <= 0:
                    return
                vol_alive_ratio = recent_30 / prior_90
                if vol_alive_ratio < 0.7:
                    return
            else:
                return
            with lock:
                log['vol_alive'] += 1

            vol_ma20_full = vol.rolling(20).mean()
            recent_60_vol = vol.iloc[-60:]
            recent_60_ma  = vol_ma20_full.iloc[-60:]
            spike_mask_60 = recent_60_vol >= recent_60_ma * 2.0
            if not spike_mask_60.any():
                return
            with lock:
                log['vol_recent'] += 1

            obv = calc_obv(df)
            if len(obv) < 60:
                return
            obv_60ago     = float(obv.iloc[-60])
            obv_now_v     = float(obv.iloc[-1])
            obv_60_change = (obv_now_v - obv_60ago) / (abs(obv_60ago) + 1) * 100
            if obv_60_change <= 0:
                return
            with lock:
                log['obv_trend'] += 1

            rsi_ser    = calc_rsi(close)
            rsi_cur    = float(rsi_ser.iloc[-1])
            low_14     = df['Low'].rolling(14).min().iloc[-1]
            high_14    = df['High'].rolling(14).max().iloc[-1]
            denom      = high_14 - low_14
            stoch_cur  = (cur - low_14) / denom * 100 if denom > 0 else 50.0
            signal_cur = stoch_cur * 0.6 + rsi_cur * 0.4
            if not (20 <= signal_cur <= 85):
                return
            with lock:
                log['signal_ok'] += 1

            rs       = calc_relative_strength(df, kospi_df)
            vol_ma20 = vol.rolling(20).mean()

            with lock:
                passed_stage1.append((
                    ticker, df, vol_ma20, mktcap, rebound_pct, cur,
                    vol.iloc[-1], vol_ma20.iloc[-1], rs
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
        ticker, df, vol_ma20, mktcap, rebound_pct, cur, cur_vol, cur_vm20, rs = row
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

            debug_dart = (log.get('opmargin_checked', 0) < 3)
            log['opmargin_checked'] = log.get('opmargin_checked', 0) + 1

            op_passed, op_score, op_info = check_op_margin_filter(ticker, debug=debug_dart)
            # ★ v4.1: 임시로 영업이익 필터 무시 (DART corpCode 커버리지 낮음)
            # if not op_passed:
            #     continue
            if op_passed:
                log['opmargin_pass'] = log.get('opmargin_pass', 0) + 1

            breakdown['영업이익률증가'] = op_score
            total_score += op_score
            meta['op_margin_info'] = op_info

            meta['mktcap']      = mktcap
            meta['rebound_pct'] = round(float(rebound_pct), 1)
            meta['cur_close']   = int(cur)
            meta['vol_ratio']   = round(float(cur_vol) / (float(cur_vm20) + 1), 2)
            meta['rs']          = rs
            meta['trade']       = calc_trade_levels(df, cur)
            meta['spike_news']  = []

            score_100_check = int(round(total_score / MAX_SCORE * 100))
            if score_100_check >= SCORE_THRESHOLD_100:
                candidates.append((total_score, ticker, breakdown, meta, df))
                log['final'] += 1
        except Exception:
            continue

    print(f"\n📊 [필터 현황 — v3.5 큰 흐름 + 영업이익률]")
    for lbl, key in [
        ("전체",                    "total"),
        ("① 동전주 제외",           "penny"),
        ("② 시총 필터",             "mktcap"),
        ("③ 저점대비 0~80%",        "bottom"),
        ("④ 1~3년고점 -30%↑ 하락",  "high_gap"),
        ("⑤ 장기 횡보 60일↑ (50%↓)", "sideways"),
        ("⑥ 거래량 살아있음 (≥0.7)", "vol_alive"),
        ("⑦ 60일내 스파이크 1회↑",  "vol_recent"),
        ("⑧ OBV 60일 매집 추세↑",   "obv_trend"),
        ("⑨ 매매신호 20~85",        "signal_ok"),
        ("⑩ 수급 확인",             "seforce"),
        ("⑪ 영업이익률 증가",       "opmargin_pass"),
        ("최종 통과",               "final"),
    ]:
        print(f"  {lbl:<28} {log.get(key, 0):>5}건")

    candidates.sort(key=lambda x: x[0], reverse=True)
    top_raw = candidates[:TOP_N]

    if not top_raw:
        print("⚠️  오늘 조건 충족 종목 없음")

    final_picks = []
    print(f"\n🏆 [TOP {TOP_N}] — 실시간 가격 조회 중...")
    for rank, (total_score, ticker, breakdown, meta, df) in enumerate(top_raw, 1):
        name = ticker
        try:
            data = _fetch_naver_basic(ticker)
            name = data.get('stockName') or data.get('name') or ticker
        except Exception:
            pass
        if name == ticker:
            try:
                for mkt in ["KOSPI", "KOSDAQ"]:
                    df_lst = fdr.StockListing(mkt)
                    col    = 'Code' if 'Code' in df_lst.columns else df_lst.columns[0]
                    nm_col = next((c for c in ['Name', 'name'] if c in df_lst.columns), None)
                    if nm_col:
                        row = df_lst[df_lst[col].astype(str).str.zfill(6) == ticker]
                        if not row.empty:
                            name = row.iloc[0][nm_col]; break
            except Exception:
                pass

        rt_price = get_realtime_price(ticker)
        if rt_price:
            meta['cur_close'] = rt_price
            meta['trade']     = calc_trade_levels(df, rt_price)
            print(f"  #{rank} {name}({ticker}) | 실시간가: {rt_price:,}원")
        else:
            print(f"  #{rank} {name}({ticker}) | 실시간가 조회 실패 → 전일 종가 사용: {meta['cur_close']:,}원")

        spike_date_strs    = meta.get('last_spikes', [])
        meta['spike_news'] = fetch_spike_news(ticker, spike_date_strs)

        market       = market_cache.get(ticker, '')
        summary      = f"[{market}]" if market else ""
        score_100    = int(round(total_score / MAX_SCORE * 100))
        star_count   = min(score_100 // 20, 5)
        stars        = "★" * star_count + "☆" * (5 - star_count)
        supply_text  = meta.get('supply_text', '정보없음')
        expected_ret = round(5.0 + (score_100 / 100) * 15.0, 1)
        top_f        = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
        tag_str      = " / ".join([f"{k}" for k, v in top_f[:3] if v > 0])

        final_picks.append({
            "rank": rank, "name": name, "code": ticker,
            "company_summary": summary, "supply": supply_text,
            "cur_price":   meta.get('cur_close', 0),
            "score_100":   score_100,
            "score":       f"{stars} {score_100}점",
            "tags":        tag_str, "expected_return": f"{expected_ret}%",
            "score_detail": breakdown,
            "meta": {
                "rsi":             meta.get('rsi', 0),
                "rebound_pct":     meta.get('rebound_pct', 0),
                "vol_ratio":       meta.get('vol_ratio', 0),
                "inst_streak":     meta.get('inst_streak', 0),
                "for_streak":      meta.get('for_streak', 0),
                "mktcap":          meta.get('mktcap', 0),
                "green_days":      meta.get('green_days', 0),
                "signal":          meta.get('signal', 0),
                "max_spike":       meta.get('max_spike', 0),
                "rs":              meta.get('rs', 0),
                "trade":           meta.get('trade', None),
                "rt_price_used":   rt_price is not None,
                "spike_count":     meta.get('spike_count', 0),
                "disp20":          meta.get('disp20', 0.0),
                "disp60":          meta.get('disp60', 0.0),
                "last_spikes":     meta.get('last_spikes', []),
                "spike_news":      meta.get('spike_news', []),
                "sideways_days":   meta.get('sideways_days', 0),
                "high52_gap":      meta.get('high52_gap', 0.0),
                "vol_alive_ratio": meta.get('vol_alive_ratio', 0.0),
                "obv_60_change":   meta.get('obv_60_change', 0.0),
                "op_margin_info":  meta.get('op_margin_info', {}),
            }
        })

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
        "today_picks":       final_picks,
        "scan_label":        label,
        "total_candidates":  log['final'],
        "total_screened":    log['total'],
        "filter_log":        log,
        "base_date":         today_str,
    }
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4, default=json_safe)

    print(f"\n🏁 완료! TOP {len(final_picks)}종목")
    return final_picks


# ══════════════════════════════════════════════════════════════
#  텔레그램 메시지 조립 (BASIC)
# ══════════════════════════════════════════════════════════════

def build_telegram_message(picks):
    today_str = get_market_date()
    label     = now_label()

    if not picks:
        return "⚠️ 오늘 조건 충족 종목 없음"

    lines = [
        f"⚡ <b>폭풍전야 TOP {len(picks)} — {ko_date(today_str)} {label}</b>",
        "━" * 24,
    ]

    for p in picks:
        chart_url = f"https://m.stock.naver.com/domestic/stock/{p['code']}/total"
        trade     = p['meta'].get('trade')
        rs_val    = p['meta'].get('rs', 0)
        rt_flag   = "📡 실시간" if p['meta'].get('rt_price_used') else "📋 전일종가"
        sd        = p.get('score_detail', {})
        score_100 = p.get('score_100', 0)

        filled = int(score_100 / 10)
        bar    = '█' * filled + '░' * (10 - filled)

        obv60_100  = normalize_score(sd.get('OBV60일매집',   0), 30)
        gather_100 = normalize_score(sd.get('매집일수',      0), 25)
        valive_100 = normalize_score(sd.get('거래량살아있음', 0), 25)
        side_100   = normalize_score(sd.get('횡보기간',      0), 25)
        gap_100    = normalize_score(sd.get('고점괴리',      0), 25)
        signal_100 = normalize_score(sd.get('매매신호',      0), 25)
        spike_100  = normalize_score(sd.get('거래량스파이크', 0), 25)
        supply_100 = normalize_score(sd.get('수급강도',      0), 20)

        spike_cnt     = p['meta'].get('spike_count', 0)
        disp20_val    = p['meta'].get('disp20', 0.0)
        disp60_val    = p['meta'].get('disp60', 0.0)
        sideways_days = p['meta'].get('sideways_days', 0)
        high52_gap    = p['meta'].get('high52_gap', 0.0)
        vol_alive     = p['meta'].get('vol_alive_ratio', 0.0)
        obv_60_chg    = p['meta'].get('obv_60_change', 0.0)
        spike_news    = p['meta'].get('spike_news', [])

        macro_raw  = sd.get('OBV60일매집', 0) + sd.get('매집일수', 0) + sd.get('거래량살아있음', 0)
        macro_100  = int(round(macro_raw / 80 * 100))
        macro_flag = "🔥 <b>매크로 매집 강세</b>" if macro_100 >= 70 else ""

        opmargin_100 = normalize_score(sd.get('영업이익률증가', 0), 20)
        op_info      = p['meta'].get('op_margin_info', {})

        lines += [
            f"#{p['rank']} <b><a href='{chart_url}'>{p['name']}</a></b> ({p['code']})",
            f"  🏢 {p['company_summary']}",
            f"",
            f"  <code>{bar}</code> <b>{score_100}점</b> {p['score'].split(' ')[0]}",
            f"",
            f"  💰 현재가: {p['cur_price']:,}원 {rt_flag} | RS: {rs_val:+}%",
            f"  📈 기대수익: {p['expected_return']}",
            f"",
            f"  📊 <b>큰 흐름 매크로</b>{('  ' + macro_flag) if macro_flag else ''}",
            f"    {grade_emoji(obv60_100)} OBV 60일 매집     {obv60_100}점 ({obv_60_chg:+.1f}%)",
            f"    {grade_emoji(gather_100)} 매집 일수         {gather_100}점",
            f"    {grade_emoji(valive_100)} 거래량 살아있음  {valive_100}점 ({vol_alive:.2f}배)",
            f"    {grade_emoji(side_100)} 장기 횡보 {sideways_days}일  {side_100}점",
            f"    {grade_emoji(gap_100)} 1~3년 고점 -{high52_gap:.0f}%  {gap_100}점",
            f"",
            f"  📐 MA20 {disp20_val:+.1f}% | MA60 {disp60_val:+.1f}%",
            f"",
            f"  🎯 <b>최근 신호</b>",
            f"    {grade_emoji(signal_100)} 매매신호          {signal_100}점",
            f"    {grade_emoji(spike_100)} 거래량스파이크   {spike_100}점 ({spike_cnt}회)",
            f"    {grade_emoji(supply_100)} 외인/기관 수급  {supply_100}점",
            f"",
            f"  🏦 수급: {p['supply']}",
        ]

        if op_info.get('q_latest'):
            lines += [
                f"",
                f"  💹 <b>영업이익률</b>  {grade_emoji(opmargin_100)} {opmargin_100}점",
                f"    {op_info['q_prev']}: {op_info['m_prev']:.2f}% → "
                f"{op_info['q_latest']}: {op_info['m_latest']:.2f}% "
                f"({op_info['change_pct']:+.1f}%)",
            ]
        elif opmargin_100 == 0 and op_info.get('reason'):
            lines.append(f"  💹 영업이익률: <i>{op_info['reason']}</i>")

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
#  ★ QUANT VALUE 엔진 (v4.1)
#  변경: 중소형 모집단 + 성장률 가중치 UP + CAPM 버그 수정 + 병렬화
# ══════════════════════════════════════════════════════════════

VALUE_TOP_N       = 3
# v4.1: 시총 상위N → 중소형 구간 (1000억~1조)
VALUE_MKTCAP_MIN  = 1000    # 억
VALUE_MKTCAP_MAX  = 10000   # 억 (1조)

_value_dart_cache: dict = {}


def _get_value_dart_data(ticker: str) -> dict:
    if ticker in _value_dart_cache:
        return _value_dart_cache[ticker]

    api_key = os.environ.get('DART_API_KEY', '')
    if not api_key:
        _value_dart_cache[ticker] = {}
        return {}

    corp_codes = _load_dart_corp_codes()
    corp_code  = corp_codes.get(ticker)
    if not corp_code:
        _value_dart_cache[ticker] = {}
        return {}

    current_year = datetime.datetime.now().year
    fetch_list = []
    for year in [current_year, current_year - 1, current_year - 2]:
        fetch_list += [
            ('11013', 'Q1', year),
            ('11012', 'Q2', year),
            ('11014', 'Q3', year),
            ('11011', 'Y',  year),
        ]

    result = {}
    for reprt_code, label, year in fetch_list:
        try:
            url    = f"{DART_BASE_URL}/fnlttSinglAcntAll.json"
            params = {'crtfc_key': api_key, 'corp_code': corp_code,
                      'bsns_year': str(year), 'reprt_code': reprt_code, 'fs_div': 'CFS'}
            r    = requests.get(url, params=params, timeout=8)
            data = r.json()
            if data.get('status') != '000':
                params['fs_div'] = 'OFS'
                r    = requests.get(url, params=params, timeout=8)
                data = r.json()
                if data.get('status') != '000':
                    continue

            revenue = op_profit = None
            for item in data.get('list', []):
                acct = (item.get('account_nm') or '').strip()
                amt  = (item.get('thstrm_amount') or '0').replace(',', '').replace(' ', '')
                if not amt or amt == '-': continue
                try: amt_int = int(amt)
                except ValueError: continue
                if acct in ['매출액', '수익(매출액)', '영업수익', '매출']:
                    if revenue is None: revenue = amt_int
                elif acct in ['영업이익', '영업이익(손실)']:
                    if op_profit is None: op_profit = amt_int

            if revenue and revenue > 0 and op_profit is not None:
                result[(year, label)] = {'revenue': revenue, 'op_profit': op_profit}
        except Exception:
            continue

    _value_dart_cache[ticker] = result
    return result


def _check_4q_yoy(ticker: str) -> tuple:
    data = _get_value_dart_data(ticker)
    if not data:
        return False, {'reason': 'DART 데이터 없음'}

    _lbl_rank = {'Q1': 1, 'Q2': 2, 'Q3': 3, 'Y': 4}
    pairs = []
    for (year, label) in sorted(data.keys(),
                                key=lambda k: (k[0], _lbl_rank.get(k[1], 0)),
                                reverse=True):
        prev_key = (year - 1, label)
        if prev_key in data:
            cur = data[(year, label)]
            prv = data[prev_key]
            pairs.append({
                'period':   f"{year}{'Y' if label == 'Y' else label}",
                'cur_rev':  cur['revenue'],   'prev_rev': prv['revenue'],
                'cur_op':   cur['op_profit'], 'prev_op':  prv['op_profit'],
            })
            if len(pairs) >= 4:
                break

    if len(pairs) < 4:
        return False, {'reason': f'YoY 비교 가능 {len(pairs)}개 (4개 미만)'}

    for p in pairs:
        if p['cur_rev'] <= p['prev_rev']:
            return False, {'reason': f"{p['period']} 매출 YoY-"}
        if p['cur_op']  <= p['prev_op']:
            return False, {'reason': f"{p['period']} 영업이익 YoY-"}
        if p['cur_op']  <= 0:
            return False, {'reason': f"{p['period']} 영업이익 적자"}

    rev_g = [(p['cur_rev'] - p['prev_rev']) / max(abs(p['prev_rev']), 1) * 100 for p in pairs]
    op_g  = [(p['cur_op']  - p['prev_op'])  / max(abs(p['prev_op']),  1) * 100 for p in pairs]
    return True, {
        'pairs':          pairs,
        'avg_rev_growth': round(float(np.mean(rev_g)), 1),
        'avg_op_growth':  round(float(np.mean(op_g)),  1),
    }


def _calc_capm_residual(stock_df, market_df, period: int = 60) -> tuple:
    """
    60일 CAPM OLS 회귀 → 누적 잔차(%) + 베타
    v4.1 버그 수정: iloc[-period:] 슬라이싱 제거, 공통 인덱스 기반으로 변경
    """
    try:
        if len(stock_df) < 30 or len(market_df) < 30:
            return 0.0, 1.0

        s = stock_df['Close'].pct_change().dropna()
        m = market_df['Close'].pct_change().dropna()

        # 공통 날짜 기준으로 정렬 후 최근 period개
        common = s.index.intersection(m.index)
        if len(common) < 30:
            return 0.0, 1.0

        common_recent = common[-period:] if len(common) >= period else common
        x = m.loc[common_recent].values
        y = s.loc[common_recent].values

        xm, ym = x.mean(), y.mean()
        denom = float(np.sum((x - xm) ** 2))
        if denom == 0:
            return 0.0, 1.0

        beta      = float(np.sum((x - xm) * (y - ym)) / denom)
        alpha     = float(ym - beta * xm)
        cum_resid = float(np.sum(y - (alpha + beta * x)) * 100)
        return cum_resid, beta
    except Exception:
        return 0.0, 1.0


def run_value_scan():
    """QUANT VALUE 메인 — 중소형 성장주 (v4.1)"""
    if not os.environ.get('DART_API_KEY', ''):
        print("  ⚠️  DART_API_KEY 없음 — VALUE 스캔 건너뜀")
        return []

    today_str  = get_market_date()
    start_date = get_start_date(today_str, 90)

    print("\n" + "═" * 60)
    print(f"💎 QUANT VALUE 엔진 — TOP {VALUE_TOP_N} (중소형 성장주 v4.1)")
    print("═" * 60)

    # ── 모집단: 중소형 구간 (1000억~1조) ────────────────────
    universe = []; market_map = {}; mktcap_map = {}
    try:
        for mkt in ["KOSPI", "KOSDAQ"]:
            df_lst  = fdr.StockListing(mkt)
            cap_col = _find_cap_col(df_lst)
            cod_col = 'Code' if 'Code' in df_lst.columns else df_lst.columns[0]
            if not cap_col: continue
            df_lst = df_lst.dropna(subset=[cap_col]).copy()
            df_lst['_c'] = df_lst[cap_col].apply(
                lambda x: int(x / 1e8) if x > 1e6 else int(x))
            # v4.1: 시총 상위N 방식 → 구간 필터 방식
            df_lst = df_lst[
                (df_lst['_c'] >= VALUE_MKTCAP_MIN) &
                (df_lst['_c'] <= VALUE_MKTCAP_MAX)
            ]
            for _, row in df_lst.iterrows():
                t = str(row[cod_col]).zfill(6)
                universe.append(t)
                market_map[t] = mkt
                mktcap_map[t] = int(row['_c'])
    except Exception as e:
        print(f"  ❌ 모집단 실패: {e}"); return []

    print(f"  📊 모집단: {len(universe)}종목 (시총 {VALUE_MKTCAP_MIN}억~{VALUE_MKTCAP_MAX}억)")

    # ── KOSPI 지수 (CAPM 기준) ───────────────────────────────
    mkt_df = None
    for sym in ['KS11', '^KS11', 'KOSPI']:
        try:
            df_m = fdr.DataReader(sym, start_date, today_str)
            if df_m is not None and len(df_m) >= 60:
                mkt_df = df_m; break
        except Exception: continue
    if mkt_df is None:
        print("  ❌ KOSPI 지수 실패"); return []

    # ── PER/PBR 일괄 조회 ────────────────────────────────────
    per_pbr_map = {}
    try:
        from pykrx import stock as pkstock
        df_fund = pkstock.get_market_fundamental_by_ticker(today_str.replace('-', ''))
        for t in universe:
            if t in df_fund.index:
                per_pbr_map[t] = (float(df_fund.loc[t].get('PER', 0)),
                                  float(df_fund.loc[t].get('PBR', 0)))
        print(f"  ✅ PER/PBR: {len(per_pbr_map)}건")
    except Exception as e:
        print(f"  ⚠️  PER/PBR 실패: {e}")

    # ── 1차: 4분기 YoY+ 병렬화 ──────────────────────────────
    print(f"\n🔍 1차: 4분기 YoY+ 필터 (병렬 8스레드)...")
    growth_ok  = []
    growth_lock = threading.Lock()

    def _check_yoy_wrapper(t):
        ok, info = _check_4q_yoy(t)
        return (t, info) if ok else None

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_check_yoy_wrapper, t): t for t in universe}
        done = 0
        for fut in as_completed(futs):
            done += 1
            res = fut.result()
            if res:
                with growth_lock:
                    growth_ok.append(res)
            if done % 50 == 0:
                print(f"  진행 {done}/{len(universe)} | 통과 {len(growth_ok)}")

    print(f"  → {len(growth_ok)}종목")
    if not growth_ok: return []

    # ── 시장별 PER/PBR 통계 ──────────────────────────────────
    mp = {'KOSPI': [], 'KOSDAQ': []}
    mb = {'KOSPI': [], 'KOSDAQ': []}
    for t in universe:
        per, pbr = per_pbr_map.get(t, (0, 0))
        m = market_map.get(t)
        if 0 < per < 200 and 0 < pbr < 20:
            mp[m].append(per); mb[m].append(pbr)

    per_stat = {m: (float(np.median(v)), float(np.std(v)) or 1) for m, v in mp.items() if v}
    pbr_stat = {m: (float(np.median(v)), float(np.std(v)) or 1) for m, v in mb.items() if v}

    # ── 2차: 채점 병렬화 ─────────────────────────────────────
    print(f"\n🔍 2차: PER/PBR + CAPM 잔차 채점 (병렬 8스레드)...")
    cands      = []
    cands_lock = threading.Lock()

    def _score_value_ticker(args):
        t, ginfo = args
        try:
            per, pbr = per_pbr_map.get(t, (0, 0))
            if per <= 0 or pbr <= 0: return

            df = fdr.DataReader(t, start_date, today_str)
            if len(df) < 60: return

            resid, beta = _calc_capm_residual(df, mkt_df)
            if not (0.3 <= beta <= 2.0): return

            m = market_map.get(t, 'KOSPI')
            pm, ps = per_stat.get(m, (20, 10))
            bm, bs = pbr_stat.get(m, (1, 0.5))

            def _z2score(z, mx):
                if   z <= -1.5: return mx
                elif z <= -1.0: return int(mx * 0.83)
                elif z <= -0.5: return int(mx * 0.60)
                elif z <= 0:    return int(mx * 0.33)
                return 0

            # v4.1 가중치: PER 20점, PBR 20점, CAPM 25점, 성장률 35점
            per_s = _z2score((per - pm) / ps, 20)
            pbr_s = _z2score((pbr - bm) / bs, 20)
            res_s = (25 if resid <= -20 else 20 if resid <= -10
                     else 12 if resid <= -5 else 5 if resid <= 0 else 0)
            avg_g = (ginfo['avg_rev_growth'] + ginfo['avg_op_growth']) / 2
            # v4.1 성장률 35점 구간
            gr_s  = (35 if avg_g >= 50
                     else 28 if avg_g >= 30
                     else 20 if avg_g >= 20
                     else 12 if avg_g >= 10
                     else 5 if avg_g > 0 else 0)
            total = per_s + pbr_s + res_s + gr_s

            name = t
            try:
                nd   = _fetch_naver_basic(t)
                name = nd.get('stockName') or t
            except Exception: pass

            with cands_lock:
                cands.append({
                    't': t, 'name': name, 'mkt': m,
                    'mktcap': mktcap_map.get(t, 0),
                    'score': total,
                    'bd': {'PER': per_s, 'PBR': pbr_s, 'CAPM잔차': res_s, '성장률': gr_s},
                    'per': round(per, 1), 'pbr': round(pbr, 2),
                    'per_med': round(pm, 1), 'pbr_med': round(bm, 2),
                    'beta': round(beta, 2), 'resid': round(resid, 2),
                    'rev_g': ginfo['avg_rev_growth'], 'op_g': ginfo['avg_op_growth'],
                    'pairs': ginfo['pairs'],
                    'cur': int(df['Close'].iloc[-1]), 'df': df,
                })
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_score_value_ticker, growth_ok))

    cands.sort(key=lambda x: x['score'], reverse=True)
    top = cands[:VALUE_TOP_N]

    print(f"\n💎 [VALUE TOP {VALUE_TOP_N}]")
    final = []
    for rank, c in enumerate(top, 1):
        rt    = get_realtime_price(c['t'])
        cp    = rt if rt else c['cur']
        trade = calc_trade_levels(c['df'], cp)
        print(f"  #{rank} {c['name']}({c['t']}) {c['score']}/100점 "
              f"PER {c['per']} PBR {c['pbr']} 잔차 {c['resid']:+.1f}%")

        fair_per = round(cp * (c['per_med'] / c['per'])) if c['per'] > 0 else 0
        fair_pbr = round(cp * (c['pbr_med'] / c['pbr'])) if c['pbr'] > 0 else 0
        fair_avg = round((fair_per + fair_pbr) / 2) if fair_per and fair_pbr else 0
        fair_gap = round((fair_avg - cp) / cp * 100, 1) if cp > 0 and fair_avg > 0 else 0

        final.append({
            'rank': rank, 'ticker': c['t'], 'name': c['name'],
            'market': c['mkt'], 'mktcap': c['mktcap'],
            'total_score': c['score'], 'breakdown': c['bd'],
            'cur_price': cp, 'rt_price_used': rt is not None,
            'per': c['per'], 'pbr': c['pbr'],
            'per_med': c['per_med'], 'pbr_med': c['pbr_med'],
            'beta': c['beta'], 'cum_resid_pct': c['resid'],
            'avg_rev_growth': c['rev_g'], 'avg_op_growth': c['op_g'],
            'pairs': c['pairs'], 'trade': trade,
            'fair_per': fair_per, 'fair_pbr': fair_pbr,
            'fair_avg': fair_avg, 'fair_gap': fair_gap,
        })
    return final


def build_value_message(picks):
    if not picks: return ""
    today_str = get_market_date()
    label     = now_label()

    lines = [
        f"📈 <b>퀀트투자 TOP {len(picks)} — {ko_date(today_str)} {label}</b>",
        "<i>중소형 4분기 실적개선 + 동종업 저평가 + 비체계적 mispricing</i>",
        "═" * 24,
    ]
    for p in picks:
        chart = f"https://m.stock.naver.com/domestic/stock/{p['ticker']}/total"
        bd    = p['breakdown']
        trade = p.get('trade')
        rt    = "📡 실시간" if p['rt_price_used'] else "📋 전일종가"
        bar   = '█' * int(p['total_score'] / 10) + '░' * (10 - int(p['total_score'] / 10))

        rf = ("🔥 <b>강한 비체계저평가</b>" if p['cum_resid_pct'] <= -10
              else "🟢 <b>비체계저평가</b>"   if p['cum_resid_pct'] <= -5 else "")

        lines += [
            f"#{p['rank']} <b><a href='{chart}'>{p['name']}</a></b> ({p['ticker']}) [{p['market']}]",
            f"  🏢 시총 {p['mktcap']:,}억  <code>{bar}</code> <b>{p['total_score']}/100점</b>",
            f"  💰 현재가: {p['cur_price']:,}원 {rt}",
            f"  🎯 적정가: {p['fair_avg']:,}원  <b>(+{p['fair_gap']}% 상승여력)</b>",
            f"     └ PER기준 {p['fair_per']:,}원 / PBR기준 {p['fair_pbr']:,}원",
            f"",
            f"  📐 <b>동종업 저평가</b> ({p['market']} 내 z-score)",
            f"    PER {p['per']} (중앙 {p['per_med']}) → {bd['PER']}점",
            f"    PBR {p['pbr']} (중앙 {p['pbr_med']}) → {bd['PBR']}점",
            f"",
            f"  📊 <b>4분기 연속 실적개선</b>  ({bd['성장률']}점)",
            f"    매출 avg +{p['avg_rev_growth']}% / 영업이익 avg +{p['avg_op_growth']}%",
        ]
        for pr in p['pairs'][:4]:
            rv = (pr['cur_rev'] - pr['prev_rev']) / max(abs(pr['prev_rev']), 1) * 100
            op = (pr['cur_op']  - pr['prev_op'])  / max(abs(pr['prev_op']),  1) * 100
            lines.append(f"      {pr['period']}: 매출 +{rv:.1f}% / 영업 +{op:.1f}%")

        lines += [
            f"",
            f"  🎲 <b>비체계적 저평가</b> (CAPM 60일){('  ' + rf) if rf else ''}",
            f"    누적잔차 {p['cum_resid_pct']:+.1f}% | β {p['beta']} → {bd['CAPM잔차']}점",
        ]
        if trade:
            lines += [
                f"",
                f"  🎯 <b>트레이드 플랜</b>",
                f"    ├ 진입: {trade['entry']:,}원",
                f"    ├ 🛑 손절: {trade['stop_loss']:,}원 (-{trade['risk_pct']}%)",
                f"    ├ 🥇 1차: {trade['target_1']:,}원 (+{trade['reward_pct']}%)",
                f"    └ 🥈 2차: {trade['target_2']:,}원",
            ]
        lines.append("")

    lines.append("═" * 24)
    lines.append("💎 <i>VALUE: 중장기 — 시간이 나의 편</i>")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  엔트리포인트
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    send_telegram(fetch_macro_summary())
    send_telegram(build_news_briefing())

    # BASIC (TOP 3)
    picks = run_kr_scan()
    send_telegram(build_telegram_message(picks))

    # VALUE (TOP 3)
    try:
        value_picks = run_value_scan()
        if value_picks:
            send_telegram(build_value_message(value_picks))
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, 'r', encoding='utf-8') as f:
                    d = json.load(f)
                d['value_picks'] = value_picks
                with open(DATA_FILE, 'w', encoding='utf-8') as f:
                    json.dump(d, f, ensure_ascii=False, indent=4, default=json_safe)
    except Exception as e:
        import traceback
        print(f"\n❌ VALUE 엔진 예외: {e}")
        traceback.print_exc()
