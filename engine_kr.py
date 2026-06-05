"""
engine_kr.py — 국장 장세전환형 스캐너 (REGIME ROUTER v5.3 CLOSE NEXT)
실행: python engine_kr.py
스케줄: 09:41 / 13:23 / 16:43 KST

[v5.3 핵심] — 시간대별 역할 분리 + 종가 NEXT 오버레이
  ① 아침(09:41): 전일 증시 이슈 + 전일 특이 종목 브리핑
  ② 장중(13:23): 기존 로직 기반 검토 종목, history 미저장
  ③ 장마감(16:43): 당일 증시 이슈 + 당일 특이 종목 + NEXT 종가매매 검토 종목
  ④ NEXT는 기존 장세별 BUY 후보에만 추가 적용하는 익일 대응 오버레이
  ⑤ 기존 BASIC / VALUE / TREND / PRE-SIGNAL 선별 로직은 유지
  ⑥ history.json은 NEXT 종가매매 검토 결과만 저장

[v5.1 기존 핵심]
  ① 시장 국면을 먼저 판단: BROAD_BULL / CONCENTRATED_BULL / BOX / BEAR / UNKNOWN
  ② 상승장: TREND 엔진 중심 — RS 양수, MA20 회복, 신고가/고점근접 종목
  ③ 횡보장: VALUE 엔진 중심 — 실적개선+저평가 종목
  ④ 하락장: DEFENSE 모드 — 매수 후보 전송 중단
  ⑤ 기존 BASIC 바닥반등 엔진은 반등·관찰 후보용으로 재활용
  ⑥ PRE_BULL / PRE-SIGNAL 엔진 추가 — MA20 돌파 전 조기 관심후보 감지

[변경 이력 v5.1] — 후행성 보완 PRE-SIGNAL 엔진 추가
  ① 지수 PRE_BULL 판단 추가: MA20 근접 + 단기반등 + 하락기울기 둔화
  ② 종목 PRE-SIGNAL 추가: MA20 근접, RS 개선, 20일고점 접근, 거래량 회복
  ③ BUY와 WATCH 분리: 선행 관심후보는 매수 후보/history 저장에서 제외

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

# v5.0: 마스터 라우터가 최종 저장을 담당하기 때문에
# 기존 BASIC 엔진의 내부 저장은 기본적으로 끌 수 있게 한다.
BASIC_PERSIST_ENABLED = True

# TREND 엔진 설정 — 상승장/쏠림장용
TREND_TOP_N          = 3
TREND_MKTCAP_MIN     = 3000      # 억
TREND_MKTCAP_MAX     = 500000    # 억
TREND_MAX_UNIVERSE   = 550       # 런타임 보호용
TREND_SCORE_MIN      = 60

# PRE-SIGNAL 엔진 설정 — 돌파 전 선행 관심후보용
# 주의: PRE-SIGNAL은 매수 후보가 아니라 WATCH 전용이다.
PRE_TOP_N          = 5
PRE_MKTCAP_MIN     = 1000      # 억
PRE_MKTCAP_MAX     = 100000    # 억
PRE_MAX_UNIVERSE   = 800       # 런타임 보호용
PRE_SCORE_MIN      = 58

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

    # ★ 1. 로컬 캐시 파일 확인 (7일 유효)
    cache_file = 'dart_corpcode_cache.json'
    if os.path.exists(cache_file):
        try:
            import time
            file_age = time.time() - os.path.getmtime(cache_file)
            if file_age < 7 * 24 * 3600:  # 7일 이내
                with open(cache_file, 'r', encoding='utf-8') as f:
                    _dart_corp_code_cache = json.load(f)
                    print(f"  ✅ DART corpCode 캐시 로드: {len(_dart_corp_code_cache)}건 (파일: {int(file_age/3600)}시간 전)")
                    return _dart_corp_code_cache
            else:
                print(f"  ⏰ 캐시 만료 ({int(file_age/86400)}일 전) — 새로 다운로드")
        except Exception as e:
            print(f"  ⚠️  캐시 파일 읽기 실패: {e}")

    # ★ 2. API 호출 (캐시 없거나 만료 시)
    api_key = os.environ.get('DART_API_KEY', '')
    if not api_key:
        print("  ⚠️  DART_API_KEY 없음 — 영업이익률 조회 비활성")
        _dart_corp_code_cache = {}
        return {}

    try:
        import zipfile, io, xml.etree.ElementTree as ET
        url = f"{DART_BASE_URL}/corpCode.xml?crtfc_key={api_key}"
        r = requests.get(url, timeout=15)
        
        if r.status_code != 200:
            print(f"  ⚠️  DART HTTP {r.status_code} — 기존 캐시 유지")
            _dart_corp_code_cache = {}
            return {}
        
        # ★ 3. 에러 응답 체크 (사용량 초과 등)
        if r.content.startswith(b'<?xml'):
            print(f"  ⚠️  DART API 한도 초과 — 기존 캐시 유지")
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

        # ★ 4. 파일로 저장 (다음 실행에서 재사용)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(_dart_corp_code_cache, f, ensure_ascii=False)
        
        print(f"  ✅ DART corpCode 다운로드 및 캐시 저장: {len(_dart_corp_code_cache)}건")
        return _dart_corp_code_cache
        
    except Exception as e:
        print(f"  ⚠️  DART 처리 실패: {e} — 기존 캐시 유지")
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

    if BASIC_PERSIST_ENABLED:
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
    else:
        print("  ℹ️  BASIC 내부 저장 생략 — 마스터 라우터가 최종 저장")

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
#  ★ REGIME ROUTER v5.0 — 시장 국면 판단 + 자동 전략 전환
# ══════════════════════════════════════════════════════════════

REGIME_LABELS = {
    "BROAD_BULL":        "전체 상승장",
    "CONCENTRATED_BULL": "쏠림 상승장",
    "PRE_BULL":          "상승 전환 조기감지",
    "BOX":               "횡보/중립장",
    "BEAR":              "하락장",
    "UNKNOWN":           "판단불가",
}


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        if value != value:  # NaN
            return default
        return float(value)
    except Exception:
        return default


def _load_json_file(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _fetch_index_df(symbols: list[str], start: str, end: str):
    for sym in symbols:
        try:
            df = fdr.DataReader(sym, start, end)
            if df is not None and len(df) >= 60:
                return df
        except Exception:
            continue
    return None


def _index_metrics(index_df) -> dict:
    """지수 상태를 BULL / PRE_BULL / BOX / BEAR로 분류한다.

    기존 v5.0은 MA20 위 + MA20 상승을 확인한 뒤 BULL로 판단해 후행성이 컸다.
    v5.1은 PRE_BULL을 별도로 둔다.
      - 지수가 MA20 아래 -3% 이내까지 접근
      - 최근 5일 수익률이 양수
      - MA20 하락 기울기가 둔화 또는 완만
      - 20일 수익률 급락 구간이 아님
    """
    if index_df is None or index_df.empty or len(index_df) < 60:
        return {
            "state": "UNKNOWN", "close": 0.0, "ma20": 0.0, "ma60": 0.0,
            "ret5": 0.0, "ret20": 0.0, "ma20_slope": 0.0,
            "ma20_slope_prev": 0.0, "slope_delta": 0.0,
            "dist_ma20": 0.0, "dist_ma60": 0.0, "pre_bull_score": 0,
        }

    close = index_df['Close']
    ma20  = close.rolling(20).mean()
    ma60  = close.rolling(60).mean()
    cur   = _safe_float(close.iloc[-1])
    m20   = _safe_float(ma20.iloc[-1])
    m60   = _safe_float(ma60.iloc[-1])
    m20_5 = _safe_float(ma20.iloc[-5]) if len(ma20) >= 5 else m20
    m20_10 = _safe_float(ma20.iloc[-10]) if len(ma20) >= 10 else m20_5

    ret5  = (cur / _safe_float(close.iloc[-5], cur) - 1) * 100 if len(close) >= 5 else 0.0
    ret20 = (cur / _safe_float(close.iloc[-20], cur) - 1) * 100 if len(close) >= 20 else 0.0
    ma20_slope = (m20 / m20_5 - 1) * 100 if m20_5 > 0 else 0.0
    ma20_slope_prev = (m20_5 / m20_10 - 1) * 100 if m20_10 > 0 else 0.0
    slope_delta = ma20_slope - ma20_slope_prev
    dist_ma20  = (cur / m20 - 1) * 100 if m20 > 0 else 0.0
    dist_ma60  = (cur / m60 - 1) * 100 if m60 > 0 else 0.0

    # 기존 확인형 상승장
    bull_cond = cur > m20 and m20 > m20_5 and cur > m60

    # v5.1 선행 상승 전환 감지
    near_ma20 = -3.0 <= dist_ma20 <= 2.5
    short_rebound = ret5 > 0.0
    slope_not_bad = ma20_slope >= -1.2
    slope_improving = slope_delta >= -0.15  # 악화가 멈추거나 둔화
    not_deep_selloff = ret20 >= -8.0 and dist_ma60 >= -8.0

    pre_score = 0
    if near_ma20:       pre_score += 30
    if short_rebound:   pre_score += 25
    if slope_not_bad:   pre_score += 20
    if slope_improving: pre_score += 15
    if not_deep_selloff: pre_score += 10

    pre_bull_cond = (not bull_cond) and pre_score >= 65

    # 기존 BEAR는 너무 쉽게 켜졌기 때문에, 단기 반등 조짐이 있으면 PRE_BULL을 우선한다.
    hard_bear_cond = cur < m20 and m20 < m20_5 and ret5 < -1.0 and dist_ma20 < -3.0

    if bull_cond:
        state = "BULL"
    elif pre_bull_cond:
        state = "PRE_BULL"
    elif hard_bear_cond:
        state = "BEAR"
    else:
        state = "BOX"

    return {
        "state": state,
        "close": round(cur, 2),
        "ma20": round(m20, 2),
        "ma60": round(m60, 2),
        "ret5": round(ret5, 2),
        "ret20": round(ret20, 2),
        "ma20_slope": round(ma20_slope, 2),
        "ma20_slope_prev": round(ma20_slope_prev, 2),
        "slope_delta": round(slope_delta, 2),
        "dist_ma20": round(dist_ma20, 2),
        "dist_ma60": round(dist_ma60, 2),
        "pre_bull_score": int(pre_score),
    }

def detect_market_regime() -> dict:
    """KOSPI/KOSDAQ을 분리 판단하고 최종 장세를 반환."""
    today_str = get_market_date()
    start     = get_start_date(today_str, 120)

    kospi_df  = _fetch_index_df(['KS11', '^KS11', 'KOSPI'], start, today_str)
    kosdaq_df = _fetch_index_df(['KQ11', '^KQ11', 'KOSDAQ'], start, today_str)

    kospi  = _index_metrics(kospi_df)
    kosdaq = _index_metrics(kosdaq_df)

    k_state = kospi.get('state', 'UNKNOWN')
    q_state = kosdaq.get('state', 'UNKNOWN')

    if k_state == "UNKNOWN" or q_state == "UNKNOWN":
        regime = "UNKNOWN"
    elif k_state == "BULL" and q_state == "BULL":
        regime = "BROAD_BULL"
    elif k_state == "BULL" and q_state != "BULL":
        regime = "CONCENTRATED_BULL"
    elif k_state == "BEAR" and q_state == "BEAR":
        regime = "BEAR"
    elif k_state in ["PRE_BULL", "BULL"] or q_state in ["PRE_BULL", "BULL"]:
        regime = "PRE_BULL"
    else:
        regime = "BOX"

    return {
        "regime": regime,
        "regime_label": REGIME_LABELS.get(regime, regime),
        "base_date": today_str,
        "kospi": kospi,
        "kosdaq": kosdaq,
    }

def _pick_market(p: dict) -> str:
    return str(p.get('company_summary') or p.get('market') or '').upper()


def _basic_pick_is_buyable(p: dict, min_rs: float = 0.0, allow_kosdaq: bool = True,
                           min_score: int = 60, require_ma20_recover: bool = True) -> bool:
    meta = p.get('meta', {})
    rs   = _safe_float(meta.get('rs'), 0.0)
    disp20 = _safe_float(meta.get('disp20'), 0.0)
    score  = int(_safe_float(p.get('score_100'), 0))
    market = _pick_market(p)

    if score < min_score:
        return False
    if rs < min_rs:
        return False
    if require_ma20_recover and disp20 < 0:
        return False
    if not allow_kosdaq and 'KOSDAQ' in market:
        return False
    return True


def filter_basic_buy_candidates(picks: list[dict], min_rs: float = 0.0,
                                allow_kosdaq: bool = True,
                                min_score: int = 60) -> list[dict]:
    filtered = [p for p in picks if _basic_pick_is_buyable(
        p, min_rs=min_rs, allow_kosdaq=allow_kosdaq, min_score=min_score)]
    for i, p in enumerate(filtered, 1):
        p['rank'] = i
    return filtered


def _normalize_value_for_history(p: dict) -> dict:
    """VALUE pick을 backtest_kr.py가 읽을 수 있는 최소 구조로 변환."""
    score = int(_safe_float(p.get('total_score'), 0))
    stars = "★" * min(score // 20, 5) + "☆" * (5 - min(score // 20, 5))
    return {
        "rank": p.get('rank', 0),
        "name": p.get('name', ''),
        "code": p.get('ticker', ''),
        "company_summary": f"[{p.get('market', '')}]",
        "supply": "VALUE",
        "cur_price": p.get('cur_price', 0),
        "score_100": score,
        "score": f"{stars} {score}점",
        "tags": "VALUE / 실적개선 / 저평가",
        "expected_return": "",
        "score_detail": p.get('breakdown', {}),
        "meta": {
            "rs": 0,
            "disp20": 0,
            "trade": p.get('trade'),
            "rt_price_used": p.get('rt_price_used', False),
            "mktcap": p.get('mktcap', 0),
            "value_origin": True,
            "per": p.get('per'),
            "pbr": p.get('pbr'),
            "cum_resid_pct": p.get('cum_resid_pct'),
            "avg_rev_growth": p.get('avg_rev_growth'),
            "avg_op_growth": p.get('avg_op_growth'),
        }
    }


def _normalize_trend_for_history(p: dict) -> dict:
    return p


def _history_ready_picks(picks: list[dict]) -> list[dict]:
    converted = []
    for p in picks:
        if 'code' in p:
            converted.append(_normalize_trend_for_history(p))
        elif 'ticker' in p:
            converted.append(_normalize_value_for_history(p))
    for i, p in enumerate(converted, 1):
        p['rank'] = i
    return converted



def _dedupe_picks(picks: list[dict]) -> list[dict]:
    """code/ticker 기준 중복 제거 후 rank 재정렬."""
    seen = set()
    out = []
    for p in picks or []:
        key = p.get('code') or p.get('ticker') or p.get('name')
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(p)
    for i, p in enumerate(out, 1):
        p['rank'] = i
    return out


def _exclude_codes(picks: list[dict], exclude: set[str]) -> list[dict]:
    out = []
    for p in picks or []:
        code = p.get('code') or p.get('ticker')
        if code and code in exclude:
            continue
        out.append(p)
    for i, p in enumerate(out, 1):
        p['rank'] = i
    return out


# ─────────────────────────────────────────────────────────────
#  PRE-SIGNAL 엔진 — 돌파 전 선행 관심후보용
# ─────────────────────────────────────────────────────────────

def _pre_signal_score_stock(df, index_df) -> tuple[int, dict, dict] | None:
    """MA20 확인 돌파 전, 관심후보로만 올릴 종목을 채점한다.

    BUY가 아니라 WATCH 전용이다. 조건을 너무 강하게 잡지 않되,
    무너지는 종목은 제외한다.
    """
    try:
        if df is None or len(df) < 90 or index_df is None or len(index_df) < 60:
            return None

        close = df['Close']
        volume = df['Volume']
        cur = _safe_float(close.iloc[-1])
        if cur <= 0:
            return None

        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        vol20 = volume.rolling(20).mean()
        vol5 = volume.rolling(5).mean()

        ma20_v = _safe_float(ma20.iloc[-1])
        ma20_5 = _safe_float(ma20.iloc[-5])
        ma20_10 = _safe_float(ma20.iloc[-10])
        ma60_v = _safe_float(ma60.iloc[-1])
        vol20_v = _safe_float(vol20.iloc[-1])
        vol5_v = _safe_float(vol5.iloc[-1])
        vol_now = _safe_float(volume.iloc[-1])

        if ma20_v <= 0 or ma60_v <= 0 or vol20_v <= 0:
            return None

        disp20 = (cur / ma20_v - 1) * 100
        disp60 = (cur / ma60_v - 1) * 100
        ma20_slope = (ma20_v / ma20_5 - 1) * 100 if ma20_5 > 0 else 0.0
        ma20_slope_prev = (ma20_5 / ma20_10 - 1) * 100 if ma20_10 > 0 else 0.0
        slope_delta = ma20_slope - ma20_slope_prev
        ret5 = (cur / _safe_float(close.iloc[-5], cur) - 1) * 100 if len(close) >= 5 else 0.0
        ret1 = (cur / _safe_float(close.iloc[-2], cur) - 1) * 100 if len(close) >= 2 else 0.0
        rsi = _safe_float(calc_rsi(close).iloc[-1], 50.0)
        vol_ratio = vol_now / (vol20_v + 1)
        vol5_ratio = vol5_v / (vol20_v + 1)

        rs20 = calc_relative_strength(df, index_df, period=20)
        rs63 = calc_relative_strength(df, index_df, period=63)
        rs_improve = rs20 - rs63

        high20 = _safe_float(df['High'].iloc[-21:-1].max()) if len(df) >= 21 else _safe_float(df['High'].max())
        high60 = _safe_float(df['High'].iloc[-61:-1].max()) if len(df) >= 61 else _safe_float(df['High'].max())
        high20_gap = (high20 - cur) / high20 * 100 if high20 > 0 else 999.0
        high60_gap = (high60 - cur) / high60 * 100 if high60 > 0 else 999.0

        # 강제 탈락: 선행 관심이라도 무너지는 종목은 제외
        if not (-3.5 <= disp20 <= 6.0):
            return None
        if ret5 < -3.0:
            return None
        if ret1 < -7.0:
            return None
        if ma20_slope < -1.8:
            return None
        if disp60 < -15.0:
            return None
        if rsi < 35 or rsi > 72:
            return None
        if high20_gap > 10.0:
            return None
        if vol_ratio < 0.55 and vol5_ratio < 0.75:
            return None

        bd = {}
        bd['MA20근접'] = 20 if -1.0 <= disp20 <= 3.0 else 16 if -3.5 <= disp20 < -1.0 else 12 if 3.0 < disp20 <= 6.0 else 0
        bd['RS개선'] = 25 if (rs20 >= 5 and rs_improve >= 5) else 20 if (rs20 >= 0 and rs_improve >= 3) else 14 if rs_improve > 0 else 6 if rs20 > -5 else 0
        bd['고점접근'] = 20 if high20_gap <= 3.0 else 16 if high20_gap <= 5.0 else 10 if high20_gap <= 10.0 else 0
        bd['거래량회복'] = 15 if (vol_ratio >= 1.2 or vol5_ratio >= 1.2) else 11 if (vol_ratio >= 0.9 or vol5_ratio >= 1.0) else 7 if vol5_ratio >= 0.75 else 0
        bd['기울기둔화'] = 10 if ma20_slope >= 0 else 8 if slope_delta > 0 else 5 if ma20_slope >= -0.8 else 2
        bd['과열제어'] = 10 if 40 <= rsi <= 65 else 7 if 35 <= rsi < 40 or 65 < rsi <= 72 else 0

        meta = {
            "rs": round(float(rs20), 1),
            "rs63": round(float(rs63), 1),
            "rs_improve": round(float(rs_improve), 1),
            "disp20": round(float(disp20), 1),
            "disp60": round(float(disp60), 1),
            "ma20_slope": round(float(ma20_slope), 2),
            "ma20_slope_prev": round(float(ma20_slope_prev), 2),
            "slope_delta": round(float(slope_delta), 2),
            "ret5": round(float(ret5), 1),
            "vol_ratio": round(float(vol_ratio), 2),
            "vol5_ratio": round(float(vol5_ratio), 2),
            "rsi": round(float(rsi), 1),
            "high20_gap": round(float(high20_gap), 1),
            "high60_gap": round(float(high60_gap), 1),
            "pre_signal_origin": True,
        }
        return int(sum(bd.values())), bd, meta
    except Exception:
        return None


def run_pre_signal_scan(allow_kosdaq: bool = True, top_n: int = PRE_TOP_N) -> list[dict]:
    """PRE_BULL/BOX 구간에서 돌파 전 관심후보를 찾는다."""
    today_str = get_market_date()
    start     = get_start_date(today_str, 120)

    print("\n" + "═" * 60)
    print(f"👀 PRE-SIGNAL 엔진 — TOP {top_n} | allow_kosdaq={allow_kosdaq}")
    print("═" * 60)

    kospi_df  = _fetch_index_df(['KS11', '^KS11', 'KOSPI'], start, today_str)
    kosdaq_df = _fetch_index_df(['KQ11', '^KQ11', 'KOSDAQ'], start, today_str)

    universe = []
    market_map = {}
    name_map = {}
    mktcap_map = {}

    try:
        for mkt in ["KOSPI", "KOSDAQ"]:
            if mkt == "KOSDAQ" and not allow_kosdaq:
                continue
            df_lst = fdr.StockListing(mkt)
            cap_col = _find_cap_col(df_lst)
            code_col = 'Code' if 'Code' in df_lst.columns else df_lst.columns[0]
            name_col = next((c for c in ['Name', 'name'] if c in df_lst.columns), None)
            if not cap_col:
                continue
            df_lst = df_lst.dropna(subset=[cap_col]).copy()
            df_lst['_cap_억'] = df_lst[cap_col].apply(lambda x: int(x / 1e8) if x > 1e6 else int(x))
            df_lst = df_lst[(df_lst['_cap_억'] >= PRE_MKTCAP_MIN) & (df_lst['_cap_억'] <= PRE_MKTCAP_MAX)]
            df_lst = df_lst.sort_values('_cap_억', ascending=False).head(PRE_MAX_UNIVERSE)
            for _, row in df_lst.iterrows():
                t = str(row[code_col]).zfill(6)
                universe.append(t)
                market_map[t] = mkt
                name_map[t] = str(row[name_col]) if name_col else t
                mktcap_map[t] = int(row['_cap_억'])
    except Exception as e:
        print(f"  ❌ PRE-SIGNAL 모집단 실패: {e}")
        return []

    print(f"  📊 PRE-SIGNAL 모집단: {len(universe)}종목")
    if not universe:
        return []

    cands = []
    lock = threading.Lock()

    def _scan(ticker):
        try:
            mkt = market_map.get(ticker, 'KOSPI')
            idx_df = kosdaq_df if mkt == 'KOSDAQ' else kospi_df
            df = fdr.DataReader(ticker, start, today_str)
            if df is None or len(df) < 90:
                return
            cur = _safe_float(df['Close'].iloc[-1])
            if cur < 1000:
                return
            scored = _pre_signal_score_stock(df, idx_df)
            if not scored:
                return
            score, bd, meta = scored
            if score < PRE_SCORE_MIN:
                return
            with lock:
                cands.append((score, ticker, bd, meta, df))
        except Exception:
            return

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(_scan, t) for t in universe]
        done = 0
        for _ in as_completed(futs):
            done += 1
            if done % 150 == 0:
                print(f"  진행 {done}/{len(universe)} | PRE 후보 {len(cands)}")

    cands.sort(key=lambda x: x[0], reverse=True)
    top = cands[:top_n]

    final = []
    for rank, (score, ticker, bd, meta, df) in enumerate(top, 1):
        rt = get_realtime_price(ticker)
        cp = rt if rt else int(_safe_float(df['Close'].iloc[-1]))
        trade = calc_trade_levels(df, cp)
        mkt = market_map.get(ticker, '')
        name = name_map.get(ticker, ticker)
        stars = "★" * min(score // 20, 5) + "☆" * (5 - min(score // 20, 5))
        tags = " / ".join([k for k, v in sorted(bd.items(), key=lambda x: x[1], reverse=True)[:3] if v > 0])
        final.append({
            "rank": rank,
            "name": name,
            "code": ticker,
            "company_summary": f"[{mkt}]" if mkt else "",
            "supply": "PRE-SIGNAL",
            "cur_price": cp,
            "score_100": int(min(score, 100)),
            "score": f"{stars} {int(min(score, 100))}점",
            "tags": tags,
            "expected_return": "",
            "score_detail": bd,
            "meta": {
                **meta,
                "mktcap": mktcap_map.get(ticker, 0),
                "trade": trade,
                "rt_price_used": rt is not None,
                "pre_signal_origin": True,
            }
        })
        print(f"  #{rank} {name}({ticker}) {score}점 | RS20 {meta.get('rs', 0):+}% | MA20 {meta.get('disp20', 0):+}% | 고점이격 {meta.get('high20_gap', 0):.1f}%")

    print(f"\n🏁 PRE-SIGNAL 완료! TOP {len(final)}종목")
    return final


def build_pre_signal_message(picks: list[dict]) -> str:
    today_str = get_market_date()
    label = now_label()
    if not picks:
        return f"👀 <b>PRE-SIGNAL — {ko_date(today_str)} {label}</b>\n━" + "━" * 23 + "\n관심후보 없음"

    lines = [
        f"👀 <b>선행 관심후보 PRE-SIGNAL TOP {len(picks)} — {ko_date(today_str)} {label}</b>",
        "<i>MA20 돌파 전 관심후보입니다. 매수 신호가 아닙니다.</i>",
        "═" * 24,
    ]
    for p in picks:
        chart = f"https://m.stock.naver.com/domestic/stock/{p['code']}/total"
        meta = p.get('meta', {})
        trade = meta.get('trade')
        score = int(p.get('score_100', 0))
        bar = '█' * int(score / 10) + '░' * (10 - int(score / 10))
        lines += [
            f"#{p['rank']} <b><a href='{chart}'>{p['name']}</a></b> ({p['code']}) {p.get('company_summary', '')}",
            f"  <code>{bar}</code> <b>{score}점</b> {p.get('score', '').split(' ')[0] if p.get('score') else ''}",
            f"  💰 현재가: {p.get('cur_price', 0):,}원 {'📡 실시간' if meta.get('rt_price_used') else '📋 전일종가'}",
            f"  📊 RS20 {meta.get('rs', 0):+}% / RS개선 {meta.get('rs_improve', 0):+}% | MA20 {meta.get('disp20', 0):+}%",
            f"  🔎 20일고점 이격 {meta.get('high20_gap', 0)}% | 거래량 {meta.get('vol_ratio', 0)}배 | RSI {meta.get('rsi', 0)}",
            f"  🏷 {p.get('tags', '')}",
        ]
        if trade:
            lines.append(
                f"  🎯 확인진입 참고: 진입 {trade['entry']:,}원 / 손절 {trade['stop_loss']:,}원 (-{trade['risk_pct']}%) / 1차 {trade['target_1']:,}원 (+{trade['reward_pct']}%)"
            )
        lines.append("")
    lines.append("═" * 24)
    lines.append("💡 <i>PRE-SIGNAL은 선행 감시용입니다. 실제 BUY는 MA20 회복·RS 유지·거래량 확인 후 판단합니다.</i>")
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────
#  TREND 엔진 — 상승장/쏠림장용
# ─────────────────────────────────────────────────────────────

def _trend_score_stock(df, index_df) -> tuple[int, dict, dict] | None:
    try:
        if df is None or len(df) < 90 or index_df is None or len(index_df) < 60:
            return None

        close = df['Close']
        volume = df['Volume']
        cur = _safe_float(close.iloc[-1])
        if cur <= 0:
            return None

        ma5  = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        vol20 = volume.rolling(20).mean()

        ma5_v   = _safe_float(ma5.iloc[-1])
        ma20_v  = _safe_float(ma20.iloc[-1])
        ma20_5  = _safe_float(ma20.iloc[-5])
        ma60_v  = _safe_float(ma60.iloc[-1])
        vol20_v = _safe_float(vol20.iloc[-1])
        vol_now = _safe_float(volume.iloc[-1])

        if ma20_v <= 0 or ma60_v <= 0:
            return None

        rs = calc_relative_strength(df, index_df)
        disp20 = (cur / ma20_v - 1) * 100
        disp60 = (cur / ma60_v - 1) * 100
        ma20_slope = (ma20_v / ma20_5 - 1) * 100 if ma20_5 > 0 else 0.0
        vol_ratio = vol_now / (vol20_v + 1)
        high20 = _safe_float(df['High'].iloc[-21:-1].max()) if len(df) >= 21 else _safe_float(df['High'].max())
        high60 = _safe_float(df['High'].iloc[-61:-1].max()) if len(df) >= 61 else _safe_float(df['High'].max())
        high60_gap = (high60 - cur) / high60 * 100 if high60 > 0 else 999.0
        breakout20 = cur > high20 if high20 > 0 else False
        near60_high = high60_gap <= 7.0
        rsi = _safe_float(calc_rsi(close).iloc[-1], 50.0)

        # 강제 탈락: 상승장용이므로 시장보다 약하거나 MA20 미회복이면 제외
        if rs < 0:
            return None
        if cur < ma20_v:
            return None
        if ma20_slope < 0:
            return None
        if vol_ratio < 0.7:
            return None
        if rsi > 82:
            return None

        bd = {}
        bd['상대강도'] = 30 if rs >= 20 else 24 if rs >= 12 else 18 if rs >= 6 else 12 if rs >= 0 else 0
        bd['추세회복'] = 25 if (cur > ma5_v > ma20_v > ma60_v) else 20 if (cur > ma20_v > ma60_v) else 12
        bd['20일돌파'] = 20 if breakout20 else 14 if near60_high else 8 if high60_gap <= 12 else 0
        bd['거래대금'] = 15 if vol_ratio >= 2.0 else 12 if vol_ratio >= 1.5 else 8 if vol_ratio >= 1.0 else 4
        bd['과열제어'] = 10 if 45 <= rsi <= 70 else 6 if 35 <= rsi < 45 or 70 < rsi <= 78 else 2

        meta = {
            "rs": round(float(rs), 1),
            "disp20": round(float(disp20), 1),
            "disp60": round(float(disp60), 1),
            "ma20_slope": round(float(ma20_slope), 2),
            "vol_ratio": round(float(vol_ratio), 2),
            "rsi": round(float(rsi), 1),
            "breakout20": breakout20,
            "near60_high": near60_high,
            "high60_gap": round(float(high60_gap), 1),
        }
        return int(sum(bd.values())), bd, meta
    except Exception:
        return None


def run_trend_scan(allow_kosdaq: bool = True, top_n: int = TREND_TOP_N) -> list[dict]:
    """상승장/쏠림장용 주도주 스캐너."""
    today_str = get_market_date()
    start     = get_start_date(today_str, 120)

    print("\n" + "═" * 60)
    print(f"🚀 TREND 엔진 — TOP {top_n} | allow_kosdaq={allow_kosdaq}")
    print("═" * 60)

    kospi_df  = _fetch_index_df(['KS11', '^KS11', 'KOSPI'], start, today_str)
    kosdaq_df = _fetch_index_df(['KQ11', '^KQ11', 'KOSDAQ'], start, today_str)

    universe = []
    market_map = {}
    name_map = {}
    mktcap_map = {}

    try:
        for mkt in ["KOSPI", "KOSDAQ"]:
            if mkt == "KOSDAQ" and not allow_kosdaq:
                continue
            df_lst = fdr.StockListing(mkt)
            cap_col = _find_cap_col(df_lst)
            code_col = 'Code' if 'Code' in df_lst.columns else df_lst.columns[0]
            name_col = next((c for c in ['Name', 'name'] if c in df_lst.columns), None)
            if not cap_col:
                continue
            df_lst = df_lst.dropna(subset=[cap_col]).copy()
            df_lst['_cap_억'] = df_lst[cap_col].apply(lambda x: int(x / 1e8) if x > 1e6 else int(x))
            df_lst = df_lst[(df_lst['_cap_억'] >= TREND_MKTCAP_MIN) & (df_lst['_cap_억'] <= TREND_MKTCAP_MAX)]
            df_lst = df_lst.sort_values('_cap_억', ascending=False).head(TREND_MAX_UNIVERSE)
            for _, row in df_lst.iterrows():
                t = str(row[code_col]).zfill(6)
                universe.append(t)
                market_map[t] = mkt
                name_map[t] = str(row[name_col]) if name_col else t
                mktcap_map[t] = int(row['_cap_억'])
    except Exception as e:
        print(f"  ❌ TREND 모집단 실패: {e}")
        return []

    print(f"  📊 TREND 모집단: {len(universe)}종목")
    if not universe:
        return []

    cands = []
    lock = threading.Lock()

    def _scan(ticker):
        try:
            mkt = market_map.get(ticker, 'KOSPI')
            idx_df = kosdaq_df if mkt == 'KOSDAQ' else kospi_df
            df = fdr.DataReader(ticker, start, today_str)
            if df is None or len(df) < 90:
                return
            cur = _safe_float(df['Close'].iloc[-1])
            if cur < 1000:
                return
            scored = _trend_score_stock(df, idx_df)
            if not scored:
                return
            score, bd, meta = scored
            if score < TREND_SCORE_MIN:
                return
            with lock:
                cands.append((score, ticker, bd, meta, df))
        except Exception:
            return

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(_scan, t) for t in universe]
        done = 0
        for _ in as_completed(futs):
            done += 1
            if done % 150 == 0:
                print(f"  진행 {done}/{len(universe)} | TREND 후보 {len(cands)}")

    cands.sort(key=lambda x: x[0], reverse=True)
    top = cands[:top_n]

    final = []
    for rank, (score, ticker, bd, meta, df) in enumerate(top, 1):
        rt = get_realtime_price(ticker)
        cp = rt if rt else int(_safe_float(df['Close'].iloc[-1]))
        trade = calc_trade_levels(df, cp)
        mkt = market_map.get(ticker, '')
        name = name_map.get(ticker, ticker)
        stars = "★" * min(score // 20, 5) + "☆" * (5 - min(score // 20, 5))
        tags = " / ".join([k for k, v in sorted(bd.items(), key=lambda x: x[1], reverse=True)[:3] if v > 0])
        final.append({
            "rank": rank,
            "name": name,
            "code": ticker,
            "company_summary": f"[{mkt}]" if mkt else "",
            "supply": "TREND",
            "cur_price": cp,
            "score_100": int(score),
            "score": f"{stars} {int(score)}점",
            "tags": tags,
            "expected_return": "",
            "score_detail": bd,
            "meta": {
                **meta,
                "mktcap": mktcap_map.get(ticker, 0),
                "trade": trade,
                "rt_price_used": rt is not None,
                "trend_origin": True,
            }
        })
        print(f"  #{rank} {name}({ticker}) {score}점 | RS {meta.get('rs', 0):+}% | MA20 {meta.get('disp20', 0):+}%")

    print(f"\n🏁 TREND 완료! TOP {len(final)}종목")
    return final


def build_trend_message(picks: list[dict], regime_info: dict | None = None) -> str:
    today_str = get_market_date()
    label = now_label()
    if not picks:
        return f"🚀 <b>TREND 엔진 — {ko_date(today_str)} {label}</b>\n━" + "━" * 23 + "\n⚠️ 상승장 조건을 통과한 주도주 없음"

    lines = [
        f"🚀 <b>상승장 TREND TOP {len(picks)} — {ko_date(today_str)} {label}</b>",
        "<i>RS 양수 + MA20 회복 + 고점근접/돌파 중심</i>",
        "═" * 24,
    ]
    for p in picks:
        chart = f"https://m.stock.naver.com/domestic/stock/{p['code']}/total"
        meta = p.get('meta', {})
        trade = meta.get('trade')
        score = int(p.get('score_100', 0))
        bar = '█' * int(score / 10) + '░' * (10 - int(score / 10))
        lines += [
            f"#{p['rank']} <b><a href='{chart}'>{p['name']}</a></b> ({p['code']}) {p.get('company_summary', '')}",
            f"  <code>{bar}</code> <b>{score}점</b> {p.get('score', '').split(' ')[0] if p.get('score') else ''}",
            f"  💰 현재가: {p.get('cur_price', 0):,}원 {'📡 실시간' if meta.get('rt_price_used') else '📋 전일종가'}",
            f"  📊 RS {meta.get('rs', 0):+}% | MA20 {meta.get('disp20', 0):+}% | MA60 {meta.get('disp60', 0):+}%",
            f"  🔥 거래량 {meta.get('vol_ratio', 0)}배 | RSI {meta.get('rsi', 0)} | 60일고점 이격 {meta.get('high60_gap', 0)}%",
            f"  🏷 {p.get('tags', '')}",
        ]
        if trade:
            lines += [
                f"  🎯 진입 {trade['entry']:,}원 / 손절 {trade['stop_loss']:,}원 (-{trade['risk_pct']}%) / 1차 {trade['target_1']:,}원 (+{trade['reward_pct']}%)",
            ]
        lines.append("")
    lines.append("═" * 24)
    lines.append("💡 <i>TREND: 약한 종목을 싸게 사는 장이 아니라 강한 종목을 추적하는 장</i>")
    return "\n".join(lines)


def build_regime_header(regime_info: dict) -> str:
    r = regime_info.get('regime', 'UNKNOWN')
    label = regime_info.get('regime_label', r)
    k = regime_info.get('kospi', {})
    q = regime_info.get('kosdaq', {})
    today_str = regime_info.get('base_date') or get_market_date()
    lines = [
        f"🧭 <b>장세 판단 — {ko_date(today_str)} {now_label()}</b>",
        "━" * 24,
        f"판단: <b>{label}</b> ({r})",
        f"KOSPI  {k.get('state', 'NA')} | MA20 {k.get('dist_ma20', 0):+}% | 5일 {k.get('ret5', 0):+}% | 20일 {k.get('ret20', 0):+}% | 기울기 {k.get('ma20_slope', 0):+}% | PRE {k.get('pre_bull_score', 0)}",
        f"KOSDAQ {q.get('state', 'NA')} | MA20 {q.get('dist_ma20', 0):+}% | 5일 {q.get('ret5', 0):+}% | 20일 {q.get('ret20', 0):+}% | 기울기 {q.get('ma20_slope', 0):+}% | PRE {q.get('pre_bull_score', 0)}",
    ]
    return "\n".join(lines)


def build_defense_message(regime_info: dict, watch_picks: list[dict] | None = None) -> str:
    lines = [
        build_regime_header(regime_info),
        "",
        "⛔ <b>DEFENSE 모드</b>",
        "오늘은 자동 매수 후보를 전송하지 않습니다.",
        "시장지수 하락 또는 MA20 하락 구간에서는 점수가 높은 낙폭주도 추가 하락할 수 있습니다.",
    ]
    if watch_picks:
        lines += ["", "👀 <b>관찰 후보</b> — 매수 아님"]
        for p in watch_picks[:3]:
            meta = p.get('meta', {})
            lines.append(f"  - {p.get('name')}({p.get('code')}) | {p.get('score_100')}점 | RS {meta.get('rs', 0):+}% | MA20 {meta.get('disp20', 0):+}%")
    lines += ["", "💡 <i>현금 보유도 전략입니다. 시장 ON 조건 확인 후 재가동합니다.</i>"]
    return "\n".join(lines)


def build_master_message(result: dict) -> str:
    regime_info = result.get('regime_info', {})
    regime = regime_info.get('regime', 'UNKNOWN')
    buy_picks = result.get('buy_picks', [])
    watch_picks = result.get('watch_picks', [])
    value_picks = result.get('value_picks', [])
    trend_picks = result.get('trend_picks', [])
    pre_signal_picks = result.get('pre_signal_picks', [])
    notes = result.get('notes', [])

    lines = [build_regime_header(regime_info), ""]
    if notes:
        lines.append("📌 <b>전략 판단</b>")
        for n in notes:
            lines.append(f"  - {n}")
        lines.append("")

    if regime == "BEAR":
        return build_defense_message(regime_info, watch_picks)

    if not buy_picks:
        lines += [
            "⚠️ <b>오늘 매수 후보 없음</b>",
            "BUY 조건에 맞는 종목만 매수 후보로 분리했습니다.",
        ]
    else:
        lines += [
            f"✅ <b>매수 후보 {len(buy_picks)}종목</b>",
            "━" * 24,
        ]
        # TREND 형식과 VALUE 형식이 섞일 수 있어 간단 요약형으로 통합
        for p in buy_picks:
            if 'code' in p:
                chart = f"https://m.stock.naver.com/domestic/stock/{p['code']}/total"
                meta = p.get('meta', {})
                lines.append(
                    f"#{p.get('rank')} <b><a href='{chart}'>{p.get('name')}</a></b> ({p.get('code')}) "
                    f"{p.get('company_summary', '')} | {p.get('score_100')}점 | RS {meta.get('rs', 0):+}% | MA20 {meta.get('disp20', 0):+}%"
                )
            elif 'ticker' in p:
                chart = f"https://m.stock.naver.com/domestic/stock/{p['ticker']}/total"
                lines.append(
                    f"#{p.get('rank')} <b><a href='{chart}'>{p.get('name')}</a></b> ({p.get('ticker')}) "
                    f"[{p.get('market')}] | VALUE {p.get('total_score')}/100점 | PER {p.get('per')} | PBR {p.get('pbr')}"
                )
        lines.append("")

    if pre_signal_picks:
        lines += [
            "👀 <b>선행 관심후보</b> — 매수 아님",
            "━" * 24,
        ]
        for p in pre_signal_picks[:PRE_TOP_N]:
            meta = p.get('meta', {})
            lines.append(
                f"- {p.get('name')}({p.get('code')}) | {p.get('score_100')}점 | "
                f"RS20 {meta.get('rs', 0):+}% | MA20 {meta.get('disp20', 0):+}% | 20일고점 {meta.get('high20_gap', 0)}%"
            )
        lines.append("")

    # PRE-SIGNAL이 아닌 일반 관찰 후보만 간단 표시
    general_watch = [p for p in (watch_picks or []) if not p.get('meta', {}).get('pre_signal_origin')]
    if general_watch:
        lines += ["👁 <b>일반 관찰 후보</b> — 매수 조건 미충족", "━" * 24]
        for p in general_watch[:3]:
            meta = p.get('meta', {})
            lines.append(
                f"- {p.get('name')}({p.get('code')}) | {p.get('score_100')}점 | RS {meta.get('rs', 0):+}% | MA20 {meta.get('disp20', 0):+}%"
            )
        lines.append("")

    if trend_picks:
        lines += ["", build_trend_message(trend_picks, regime_info)]
    elif value_picks:
        msg = build_value_message(value_picks)
        if msg:
            lines += ["", msg]

    if pre_signal_picks:
        lines += ["", build_pre_signal_message(pre_signal_picks)]

    lines += ["", "💡 <i>v5.1: BUY와 WATCH를 분리합니다. PRE-SIGNAL은 선행 감시용이며 history 저장 대상이 아닙니다.</i>"]
    return "\n".join(lines)

def save_master_result(result: dict):
    today_str = get_market_date()
    label = now_label()
    buy_picks = result.get('buy_picks', [])
    watch_picks = result.get('watch_picks', [])
    trend_picks = result.get('trend_picks', [])
    value_picks = result.get('value_picks', [])
    pre_signal_picks = result.get('pre_signal_picks', [])
    next_picks = result.get('next_picks', [])
    base_buy_picks = result.get('base_buy_picks', [])
    next_rejected = result.get('next_rejected', [])
    regime_info = result.get('regime_info', {})

    history_picks = _history_ready_picks(buy_picks)

    final_output = {
        "today_picks": history_picks,
        "buy_picks": buy_picks,
        "watch_picks": watch_picks,
        "pre_signal_picks": pre_signal_picks,
        "next_picks": next_picks,
        "base_buy_picks": base_buy_picks,
        "next_rejected": next_rejected,
        "trend_picks": trend_picks,
        "value_picks": value_picks,
        "market_regime": regime_info.get('regime', 'UNKNOWN'),
        "market_regime_label": regime_info.get('regime_label', ''),
        "regime_info": regime_info,
        "strategy_used": result.get('strategy_used', []),
        "notes": result.get('notes', []),
        "scan_label": label,
        "total_candidates": len(buy_picks),
        "total_watch_candidates": len(watch_picks),
        "total_pre_signal_candidates": len(pre_signal_picks),
        "total_next_candidates": len(next_picks),
        "total_screened": result.get('total_screened', 0),
        "filter_log": result.get('filter_log', {}),
        "base_date": today_str,
    }
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4, default=json_safe)
    print(f"✅ {DATA_FILE} 저장 완료")

    # history는 실제 매수 후보가 있을 때만 추가한다. PRE-SIGNAL/관찰 후보는 백테스트 대상에서 제외.
    if history_picks:
        history_data = _load_json_file(HISTORY_FILE, [])
        history_data.append({
            "date": today_str,
            "label": label,
            "market_regime": regime_info.get('regime', 'UNKNOWN'),
            "strategy_used": result.get('strategy_used', []),
            "selection_mode": "CLOSE_NEXT" if result.get('next_picks') is not None else "BASE",
            "picks": history_picks,
        })
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history_data, f, ensure_ascii=False, indent=4, default=json_safe)
        print(f"✅ {HISTORY_FILE} 업데이트 완료 ({len(history_picks)}종목)")
    else:
        print("ℹ️ 매수 후보 없음 — history.json 추가 생략")

def run_master_kr_engine() -> dict:
    """장세 판단 후 TREND/VALUE/BASIC/DEFENSE/PRE-SIGNAL 중 필요한 엔진만 실행."""
    global BASIC_PERSIST_ENABLED
    BASIC_PERSIST_ENABLED = False

    regime_info = detect_market_regime()
    regime = regime_info.get('regime', 'UNKNOWN')
    print("\n" + build_regime_header(regime_info).replace('<b>', '').replace('</b>', ''))

    result = {
        "regime_info": regime_info,
        "strategy_used": [],
        "buy_picks": [],
        "watch_picks": [],
        "pre_signal_picks": [],
        "trend_picks": [],
        "value_picks": [],
        "notes": [],
    }

    def _run_pre_safely(allow_kosdaq: bool = True, top_n: int = PRE_TOP_N) -> list[dict]:
        try:
            return run_pre_signal_scan(allow_kosdaq=allow_kosdaq, top_n=top_n)
        except Exception as e:
            import traceback
            print(f"\n❌ PRE-SIGNAL 엔진 예외: {e}")
            traceback.print_exc()
            return []

    try:
        if regime == "BROAD_BULL":
            result["strategy_used"] = ["TREND", "PRE_SIGNAL_WATCH"]
            result["notes"].append("전체 상승장: 저점매수보다 주도주 추세 추종을 우선합니다.")
            result["notes"].append("단, 후행성을 낮추기 위해 PRE-SIGNAL 관심후보도 별도 표시합니다.")
            trend_picks = run_trend_scan(allow_kosdaq=True, top_n=TREND_TOP_N)
            result["trend_picks"] = trend_picks
            result["buy_picks"] = trend_picks

            pre_picks = _run_pre_safely(allow_kosdaq=True, top_n=PRE_TOP_N)
            exclude = {p.get('code') for p in trend_picks if p.get('code')}
            result["pre_signal_picks"] = _exclude_codes(pre_picks, exclude)
            result["watch_picks"] = result["pre_signal_picks"]

        elif regime == "CONCENTRATED_BULL":
            result["strategy_used"] = ["TREND_KOSPI_ONLY", "PRE_SIGNAL_KOSPI_WATCH"]
            result["notes"].append("쏠림 상승장: KOSDAQ 중소형 바닥주는 매수 후보에서 제외합니다.")
            result["notes"].append("KOSPI 중심 PRE-SIGNAL 관심후보를 별도 표시합니다.")
            trend_picks = run_trend_scan(allow_kosdaq=False, top_n=TREND_TOP_N)
            result["trend_picks"] = trend_picks
            result["buy_picks"] = trend_picks

            pre_picks = _run_pre_safely(allow_kosdaq=False, top_n=PRE_TOP_N)
            exclude = {p.get('code') for p in trend_picks if p.get('code')}
            result["pre_signal_picks"] = _exclude_codes(pre_picks, exclude)
            result["watch_picks"] = result["pre_signal_picks"]

        elif regime == "PRE_BULL":
            result["strategy_used"] = ["PRE_SIGNAL"]
            result["notes"].append("상승 전환 조기감지: BUY는 보류하고 PRE-SIGNAL 관심후보만 전송합니다.")
            result["notes"].append("MA20 완전 회복 전 구간이므로 실제 매수 후보/history에는 저장하지 않습니다.")
            pre_picks = _run_pre_safely(allow_kosdaq=True, top_n=PRE_TOP_N)
            result["pre_signal_picks"] = pre_picks
            result["watch_picks"] = pre_picks
            result["buy_picks"] = []

        elif regime == "BOX":
            result["strategy_used"] = ["VALUE", "PRE_SIGNAL", "BASIC_FILTERED"]
            result["notes"].append("횡보/중립장: 실적개선 VALUE를 우선하되, PRE-SIGNAL로 돌파 전 관심후보를 먼저 잡습니다.")
            result["notes"].append("BASIC은 RS·MA20 필터를 통과한 종목만 BUY 후보로 허용합니다.")

            pre_picks = _run_pre_safely(allow_kosdaq=True, top_n=PRE_TOP_N)
            result["pre_signal_picks"] = pre_picks

            value_picks = []
            try:
                value_picks = run_value_scan()
            except Exception as e:
                import traceback
                print(f"\n❌ VALUE 엔진 예외: {e}")
                traceback.print_exc()
            result["value_picks"] = value_picks

            # VALUE가 있으면 우선 매수 후보로 사용. 없으면 BASIC 필터 후보 사용.
            if value_picks:
                result["buy_picks"] = value_picks
                result["watch_picks"] = pre_picks
            else:
                basic_picks = run_kr_scan()
                basic_buy = filter_basic_buy_candidates(basic_picks, min_rs=0, allow_kosdaq=True, min_score=65)
                result["buy_picks"] = basic_buy
                exclude = {p.get('code') for p in basic_buy if p.get('code')}
                result["watch_picks"] = _dedupe_picks(_exclude_codes(pre_picks, exclude) + basic_picks)

        elif regime == "BEAR":
            result["strategy_used"] = ["DEFENSE"]
            result["notes"].append("하락장: 자동 매수 후보 전송을 중단합니다.")
            result["buy_picks"] = []
            result["watch_picks"] = []
            result["pre_signal_picks"] = []

        else:
            result["strategy_used"] = ["WATCH"]
            result["notes"].append("시장 판단 불가: 자동 추천을 중단합니다.")
            result["buy_picks"] = []
            result["watch_picks"] = []
            result["pre_signal_picks"] = []

    finally:
        BASIC_PERSIST_ENABLED = True

    return result

# ══════════════════════════════════════════════════════════════
#  ★ SESSION ROUTER v5.3 — 시간대별 역할 분리 + CLOSE NEXT
#
#  09:41  MORNING  : 전일 시장 이슈 + 전일 특이 종목
#  13:23  INTRADAY : 기존 로직 기반 장중 검토 종목 (history 저장 금지)
#  16:43  CLOSE    : 당일 시장 이슈 + 당일 특이 종목 + NEXT 종가매매 검토 종목
#
#  기존 종목 선별 로직은 변경하지 않는다. CLOSE에서 기존 BUY 후보에 NEXT 오버레이만 추가한다.
# ══════════════════════════════════════════════════════════════

INTRADAY_DATA_FILE = 'stock_data_intraday.json'
ANOMALY_TOP_N      = 12
NEWS_TOP_N         = 8

# NEXT 종가매매 오버레이 — 기존 장세별 BUY 후보를 익일 대응 관점으로 재검증한다.
# 신규 전종목 추천기가 아니다. 기존 BASIC / VALUE / TREND 선별식을 반드시 먼저 통과해야 한다.
NEXT_TOP_N              = 3
NEXT_SCORE_MIN          = 60
NEXT_MIN_CLOSE_LOCATION = 0.65   # 당일 저가~고가 구간 중 종가 위치
NEXT_MIN_VOLUME_RATIO   = 0.80   # 당일 거래량 / 직전 20거래일 평균
NEXT_MAX_DAILY_CHANGE   = 12.0   # 급등 추격 방지
NEXT_MAX_RET5           = 18.0   # 단기 과열 방지
NEXT_MAX_DISP20         = 12.0   # MA20 과도 이격 방지
NEXT_MIN_TURNOVER       = 2_000_000_000  # 근사 거래대금 20억원

# 동일 거래일 스냅샷을 뉴스 날짜 판단과 특이 종목 브리핑이 재사용한다.
# pykrx/KRX에 불필요한 반복 요청을 보내지 않기 위한 실행 단위 캐시다.
_pykrx_snapshot_cache: dict[str, pd.DataFrame] = {}
_pykrx_snapshot_disabled = False

SESSION_MORNING  = 'MORNING'
SESSION_INTRADAY = 'INTRADAY'
SESSION_CLOSE    = 'CLOSE'


def _kst_now() -> datetime.datetime:
    """서버 타임존과 무관하게 한국 시간을 반환."""
    kst = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(kst)


def get_run_session(now: datetime.datetime | None = None) -> str:
    """실행 시간대 판단. 테스트 시 KR_SCAN_SESSION 환경변수로 강제 지정 가능."""
    forced = str(os.environ.get('KR_SCAN_SESSION', '')).strip().upper()
    aliases = {
        'MORNING': SESSION_MORNING, 'AM': SESSION_MORNING,
        'INTRADAY': SESSION_INTRADAY, 'DAY': SESSION_INTRADAY,
        'CLOSE': SESSION_CLOSE, 'AFTER_CLOSE': SESSION_CLOSE, 'PM': SESSION_CLOSE,
    }
    if forced in aliases:
        return aliases[forced]

    now = now or _kst_now()
    hhmm = now.hour * 100 + now.minute
    if hhmm < 1130:
        return SESSION_MORNING
    if hhmm < 1540:
        return SESSION_INTRADAY
    return SESSION_CLOSE


def _fmt_kr_market_date(date_str: str) -> str:
    raw = str(date_str or '').replace('-', '').replace('.', '')
    if len(raw) >= 8:
        return f"{raw[:4]}.{raw[4:6]}.{raw[6:8]}"
    return str(date_str or '')


def _date8(value) -> str:
    if isinstance(value, datetime.datetime):
        value = value.date()
    if isinstance(value, datetime.date):
        return value.strftime('%Y%m%d')
    return str(value or '').replace('-', '').replace('.', '')[:8]


def _pick_col(df: pd.DataFrame, candidates: list[str]):
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _fetch_pykrx_daily_snapshot(date8: str) -> pd.DataFrame:
    """pykrx 일별 전종목 스냅샷. 인증/소스 장애 시 빈 DataFrame 반환."""
    global _pykrx_snapshot_disabled
    if _pykrx_snapshot_disabled:
        return pd.DataFrame()
    if date8 in _pykrx_snapshot_cache:
        return _pykrx_snapshot_cache[date8].copy()
    try:
        from pykrx import stock as pkstock
        ohlcv = pkstock.get_market_ohlcv_by_ticker(date8, market='ALL')
        if ohlcv is None or ohlcv.empty:
            return pd.DataFrame()
        ohlcv = ohlcv.copy()
        ohlcv.index = ohlcv.index.astype(str).str.zfill(6)

        try:
            caps = pkstock.get_market_cap_by_ticker(date8, market='ALL')
            if caps is not None and not caps.empty:
                caps = caps.copy()
                caps.index = caps.index.astype(str).str.zfill(6)
                cap_col = _pick_col(caps, ['시가총액', 'Marcap', 'Market Cap'])
                if cap_col:
                    ohlcv['시가총액'] = caps[cap_col]
        except Exception:
            pass
        _pykrx_snapshot_cache[date8] = ohlcv.copy()
        return ohlcv
    except Exception as e:
        print(f"  ⚠️ pykrx 일별 스냅샷 실패({date8}): {e}")
        _pykrx_snapshot_disabled = True
        empty = pd.DataFrame()
        _pykrx_snapshot_cache[date8] = empty
        return empty.copy()


def _find_recent_market_snapshots(anchor_date: datetime.date, count: int = 2) -> list[tuple[str, pd.DataFrame]]:
    """anchor_date부터 역순으로 실제 거래일 스냅샷을 찾는다."""
    found = []
    cursor = anchor_date
    for _ in range(15):
        date8 = _date8(cursor)
        df = _fetch_pykrx_daily_snapshot(date8)
        if df is not None and not df.empty:
            vol_col = _pick_col(df, ['거래량', 'Volume'])
            if vol_col is None or float(pd.to_numeric(df[vol_col], errors='coerce').fillna(0).sum()) > 0:
                found.append((date8, df))
                if len(found) >= count:
                    break
        cursor -= datetime.timedelta(days=1)
    return found


def _fdr_latest_anomaly_frame() -> tuple[str, pd.DataFrame]:
    """pykrx 조회 불가 시 FinanceDataReader 최신 KRX 리스팅으로 최소 브리핑 생성."""
    try:
        df = fdr.StockListing('KRX')
        if df is None or df.empty:
            return '', pd.DataFrame()
        df = df.copy()
        code_col = _pick_col(df, ['Code', 'Symbol', 'code'])
        name_col = _pick_col(df, ['Name', 'name', '종목명'])
        close_col = _pick_col(df, ['Close', '종가'])
        change_col = _pick_col(df, ['ChagesRatio', 'ChangeRate', 'ChangesRatio', '등락률'])
        amount_col = _pick_col(df, ['Amount', '거래대금'])
        volume_col = _pick_col(df, ['Volume', '거래량'])
        cap_col = _find_cap_col(df)
        if not code_col or not name_col or not close_col:
            return '', pd.DataFrame()

        out = pd.DataFrame(index=df.index)
        out['code'] = df[code_col].astype(str).str.zfill(6)
        out['name'] = df[name_col].astype(str)
        out['close'] = pd.to_numeric(df[close_col], errors='coerce').fillna(0)
        out['change_pct'] = pd.to_numeric(df[change_col], errors='coerce').fillna(0) if change_col else 0.0
        out['amount'] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0) if amount_col else 0.0
        out['volume'] = pd.to_numeric(df[volume_col], errors='coerce').fillna(0) if volume_col else 0.0
        out['market_cap'] = pd.to_numeric(df[cap_col], errors='coerce').fillna(0) if cap_col else 0.0
        out['volume_ratio'] = np.nan
        return str(get_market_date()).replace('-', ''), out
    except Exception as e:
        print(f"  ⚠️ FDR 최신 특이 종목 조회 실패: {e}")
        return '', pd.DataFrame()


def _normalize_pykrx_anomaly_frame(cur_df: pd.DataFrame, prev_df: pd.DataFrame | None = None) -> pd.DataFrame:
    close_col  = _pick_col(cur_df, ['종가', 'Close'])
    change_col = _pick_col(cur_df, ['등락률', 'ChangeRate', 'ChagesRatio'])
    amount_col = _pick_col(cur_df, ['거래대금', 'Amount'])
    volume_col = _pick_col(cur_df, ['거래량', 'Volume'])
    cap_col    = _pick_col(cur_df, ['시가총액', 'Marcap', 'Market Cap'])
    if not close_col or not volume_col:
        return pd.DataFrame()

    out = pd.DataFrame(index=cur_df.index.astype(str).str.zfill(6))
    out['code'] = out.index
    out['close'] = pd.to_numeric(cur_df[close_col], errors='coerce').fillna(0).values
    out['change_pct'] = pd.to_numeric(cur_df[change_col], errors='coerce').fillna(0).values if change_col else 0.0
    out['amount'] = pd.to_numeric(cur_df[amount_col], errors='coerce').fillna(0).values if amount_col else 0.0
    out['volume'] = pd.to_numeric(cur_df[volume_col], errors='coerce').fillna(0).values
    out['market_cap'] = pd.to_numeric(cur_df[cap_col], errors='coerce').fillna(0).values if cap_col else 0.0
    out['volume_ratio'] = np.nan

    if prev_df is not None and not prev_df.empty:
        prev_vol_col = _pick_col(prev_df, ['거래량', 'Volume'])
        if prev_vol_col:
            prev_vol = pd.to_numeric(prev_df[prev_vol_col], errors='coerce').fillna(0)
            prev_vol.index = prev_vol.index.astype(str).str.zfill(6)
            out['prev_volume'] = out['code'].map(prev_vol.to_dict()).fillna(0)
            out['volume_ratio'] = np.where(out['prev_volume'] > 0, out['volume'] / out['prev_volume'], np.nan)
    return out


def _stock_name_map(codes: list[str]) -> dict[str, str]:
    names = {}
    try:
        from pykrx import stock as pkstock
        for code in codes:
            try:
                names[code] = pkstock.get_market_ticker_name(code) or code
            except Exception:
                names[code] = code
    except Exception:
        pass
    return names


def _select_anomaly_rows(frame: pd.DataFrame, top_n: int = ANOMALY_TOP_N) -> list[dict]:
    if frame is None or frame.empty:
        return []
    df = frame.copy()
    for c in ['close', 'change_pct', 'amount', 'volume', 'market_cap', 'volume_ratio']:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # 체결이 거의 없는 초저유동성 종목은 브리핑에서 제외한다.
    df = df[(df['close'].fillna(0) >= 1000) & (df['amount'].fillna(0) >= 5_000_000_000)]
    if 'market_cap' in df.columns and float(df['market_cap'].fillna(0).max()) > 0:
        df = df[(df['market_cap'].fillna(0) >= 50_000_000_000)]  # 500억원 이상
    if df.empty:
        return []

    df['is_fast_move'] = df['change_pct'].abs() >= 7.0
    df['is_turnover']  = df['amount'] >= 50_000_000_000
    df['is_vol_spike'] = df['volume_ratio'].fillna(0) >= 3.0
    special = df[df['is_fast_move'] | df['is_turnover'] | df['is_vol_spike']].copy()
    if special.empty:
        special = df.nlargest(top_n, 'amount').copy()

    # 급등락, 거래량 변화, 거래대금을 함께 반영한 브리핑용 우선순위다.
    special['brief_score'] = (
        special['change_pct'].abs().fillna(0) * 2.0
        + np.minimum(special['volume_ratio'].fillna(0), 10.0) * 3.0
        + np.log10(np.maximum(special['amount'].fillna(0), 1.0))
    )
    special = special.sort_values(['brief_score', 'amount'], ascending=False).head(top_n)
    name_map = _stock_name_map(special['code'].astype(str).tolist())

    rows = []
    for _, row in special.iterrows():
        code = str(row.get('code', '')).zfill(6)
        tags = []
        chg = float(row.get('change_pct', 0) or 0)
        vr = row.get('volume_ratio')
        amount = float(row.get('amount', 0) or 0)
        if chg >= 7.0:
            tags.append('급등')
        elif chg <= -7.0:
            tags.append('급락')
        if pd.notna(vr) and float(vr) >= 3.0:
            tags.append('거래량급증')
        if amount >= 50_000_000_000:
            tags.append('거래대금집중')
        if not tags:
            tags.append('거래활발')
        rows.append({
            'code': code,
            'name': str(row.get('name') or name_map.get(code) or code),
            'close': int(float(row.get('close', 0) or 0)),
            'change_pct': round(chg, 2),
            'amount': int(amount),
            'volume_ratio': round(float(vr), 2) if pd.notna(vr) else None,
            'tags': tags,
        })
    return rows


def build_daily_anomaly_message(scope: str = 'PREVIOUS') -> str:
    """전일 또는 당일 기준 특이 종목 브리핑. 추천 종목 로직과 완전히 분리."""
    now = _kst_now()
    is_previous = str(scope).upper() == 'PREVIOUS'
    anchor = now.date() - datetime.timedelta(days=1) if is_previous else now.date()
    snapshots = _find_recent_market_snapshots(anchor, count=2)

    source_note = 'pykrx 일별 스냅샷'
    date8 = ''
    rows = []
    if snapshots:
        date8, cur_df = snapshots[0]
        prev_df = snapshots[1][1] if len(snapshots) >= 2 else None
        rows = _select_anomaly_rows(_normalize_pykrx_anomaly_frame(cur_df, prev_df))
    if not rows:
        date8, fallback = _fdr_latest_anomaly_frame()
        rows = _select_anomaly_rows(fallback)
        source_note = 'FDR 최신 KRX 스냅샷 대체값'

    title = '🌅 <b>전일 특이 종목</b>' if is_previous else '🌙 <b>당일 특이 종목</b>'
    lines = [f"{title} — {_fmt_kr_market_date(date8)}", '━' * 24]
    if not rows:
        lines += ['특이 종목 데이터를 불러오지 못했습니다.', '<i>pykrx 또는 FDR 조회 상태를 확인하세요.</i>']
        return '\n'.join(lines)

    for idx, row in enumerate(rows, 1):
        chart = f"https://m.stock.naver.com/domestic/stock/{row['code']}/total"
        amount_uk = row['amount'] / 100_000_000
        vr_text = f" | 거래량 {row['volume_ratio']:.1f}배" if row.get('volume_ratio') is not None else ''
        lines.append(
            f"#{idx} <b><a href='{chart}'>{row['name']}</a></b> ({row['code']}) "
            f"{row['change_pct']:+.1f}% | 거래대금 {amount_uk:,.0f}억{vr_text} | {'·'.join(row['tags'])}"
        )
    lines += ['', f"<i>기준: {source_note} / 추천이 아니라 이상 움직임 확인용입니다.</i>"]
    return '\n'.join(lines)


def _fetch_naver_finance_headlines(date8: str, limit: int = NEWS_TOP_N) -> list[dict]:
    """네이버 금융 주요뉴스의 특정 일자 제목을 가져온다. 실패 시 빈 목록."""
    try:
        from urllib.parse import urljoin
        url = f"https://finance.naver.com/news/mainnews.naver?date={date8}"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept-Language': 'ko-KR,ko;q=0.9',
            'Referer': 'https://finance.naver.com/',
        }
        r = requests.get(url, headers=headers, timeout=8)
        r.encoding = 'euc-kr'
        soup = BeautifulSoup(r.text, 'html.parser')
        anchors = []
        for selector in ['dd.articleSubject a', '.articleSubject a', '.newsList a', 'a[href*="news_read.naver"]']:
            anchors.extend(soup.select(selector))

        out = []
        seen = set()
        for a in anchors:
            title = a.get_text(' ', strip=True)
            href = str(a.get('href') or '')
            if len(title) < 8 or title in seen:
                continue
            if 'news' not in href:
                continue
            seen.add(title)
            out.append({'title': title, 'url': urljoin('https://finance.naver.com', href)})
            if len(out) >= limit:
                break
        return out
    except Exception as e:
        print(f"  ⚠️ 네이버 금융 뉴스 실패({date8}): {e}")
        return []


def _resolve_news_market_date(scope: str) -> str:
    now = _kst_now()
    is_previous = str(scope).upper() == 'PREVIOUS'
    anchor = now.date() - datetime.timedelta(days=1) if is_previous else now.date()
    snapshots = _find_recent_market_snapshots(anchor, count=1)
    if snapshots:
        return snapshots[0][0]
    return str(get_market_date()).replace('-', '')


def build_stock_issue_briefing(scope: str = 'PREVIOUS') -> str:
    """아침은 전일, 장마감은 당일 기준 주요 증시 뉴스. 기존 공통 브리핑은 장애 시 fallback."""
    is_previous = str(scope).upper() == 'PREVIOUS'
    date8 = _resolve_news_market_date(scope)
    headlines = _fetch_naver_finance_headlines(date8, NEWS_TOP_N)
    title = '🌅 <b>전일 주식 관련 이슈</b>' if is_previous else '🌙 <b>장 마감 주식 관련 이슈</b>'
    lines = [f"{title} — {_fmt_kr_market_date(date8)}", '━' * 24]
    if headlines:
        for idx, item in enumerate(headlines, 1):
            lines.append(f"{idx}. <a href='{item['url']}'>{item['title']}</a>")
        lines += ['', '<i>시장 맥락 확인용입니다. 종목 후보 선별 점수에는 반영하지 않습니다.</i>']
        return '\n'.join(lines)

    # 네이버 특정일 뉴스가 실패한 경우 기존 engine_common 브리핑을 그대로 사용한다.
    try:
        fallback = build_news_briefing()
        if fallback:
            lines += ['특정 일자 뉴스 조회 실패 — 기존 최신 뉴스 브리핑으로 대체합니다.', '', str(fallback)]
            return '\n'.join(lines)
    except Exception as e:
        print(f"  ⚠️ 기존 뉴스 브리핑 fallback 실패: {e}")
    lines.append('뉴스 데이터를 불러오지 못했습니다.')
    return '\n'.join(lines)



# ══════════════════════════════════════════════════════════════
#  ★ CLOSE NEXT 오버레이 v5.3 — 종가매매는 익일 대응 후보만
#
#  원칙
#  ① 기존 장세 라우터가 만든 BUY 후보만 입력으로 사용한다.
#  ② PRE-SIGNAL / WATCH 후보는 승격하지 않는다. 기존 의미를 유지한다.
#  ③ 종가 위치·거래량·20일 고점 접근·RS·MA20 이격·과열을 재확인한다.
#  ④ NEXT 통과 종목만 종가매매 검토 및 history 저장 대상으로 사용한다.
# ══════════════════════════════════════════════════════════════

def _clone_history_ready_pick(p: dict) -> dict | None:
    """VALUE/TREND/BASIC 형식을 NEXT 공통 구조(code 기반)로 복제한다."""
    try:
        import copy
        cloned = copy.deepcopy(p)
        converted = _history_ready_picks([cloned])
        return converted[0] if converted else None
    except Exception:
        return None


def _next_origin_label(p: dict) -> str:
    meta = p.get('meta', {}) or {}
    if meta.get('trend_origin'):
        return 'TREND'
    if meta.get('value_origin'):
        return 'VALUE'
    # BASIC의 supply 필드는 외인·기관 수급 설명이 들어갈 수 있으므로
    # 문자열을 그대로 엔진명으로 사용하지 않는다.
    return 'BASIC'


def _score_close_next_candidate(df: pd.DataFrame, index_df, origin: str) -> tuple[int, dict, dict, list[str]] | None:
    """기존 BUY 후보가 다음 거래일 종가매매 검토에 적합한지 재채점한다.

    이 함수는 신규 후보를 만들지 않는다. 기존 BUY 후보 중 과열 추격과
    장 막판 약세 종목을 제외하고, 종가가 견조한 후보만 남긴다.
    """
    try:
        if df is None or len(df) < 60:
            return None

        close = df['Close']
        high = df['High']
        low = df['Low']
        volume = df['Volume']
        cur = _safe_float(close.iloc[-1])
        prev_close = _safe_float(close.iloc[-2]) if len(close) >= 2 else cur
        day_high = _safe_float(high.iloc[-1], cur)
        day_low = _safe_float(low.iloc[-1], cur)
        vol_now = _safe_float(volume.iloc[-1])
        vol20_prev = _safe_float(volume.iloc[-21:-1].mean()) if len(volume) >= 21 else _safe_float(volume.iloc[:-1].mean())
        ma20 = _safe_float(close.rolling(20).mean().iloc[-1])
        high20_prev = _safe_float(high.iloc[-21:-1].max()) if len(high) >= 21 else _safe_float(high.iloc[:-1].max())
        rsi = _safe_float(calc_rsi(close).iloc[-1], 50.0)

        if cur <= 0 or prev_close <= 0 or ma20 <= 0 or vol20_prev <= 0:
            return None

        change_pct = (cur / prev_close - 1) * 100
        ret5 = (cur / _safe_float(close.iloc[-6], cur) - 1) * 100 if len(close) >= 6 else 0.0
        disp20 = (cur / ma20 - 1) * 100
        vol_ratio = vol_now / (vol20_prev + 1)
        turnover = cur * vol_now
        close_location = (cur - day_low) / (day_high - day_low) if day_high > day_low else 0.5
        high20_gap = (high20_prev - cur) / high20_prev * 100 if high20_prev > 0 else 999.0
        breakout20 = cur > high20_prev if high20_prev > 0 else False
        rs20 = calc_relative_strength(df, index_df, period=20) if index_df is not None else 0.0

        reject_reasons = []
        if cur < 1000:
            reject_reasons.append('1,000원 미만')
        if close_location < NEXT_MIN_CLOSE_LOCATION:
            reject_reasons.append(f'종가위치 {close_location * 100:.0f}%')
        if change_pct < -1.5:
            reject_reasons.append(f'당일 약세 {change_pct:+.1f}%')
        if change_pct > NEXT_MAX_DAILY_CHANGE:
            reject_reasons.append(f'급등 추격 {change_pct:+.1f}%')
        if vol_ratio < NEXT_MIN_VOLUME_RATIO:
            reject_reasons.append(f'거래량 부족 {vol_ratio:.2f}배')
        if turnover < NEXT_MIN_TURNOVER:
            reject_reasons.append(f'거래대금 부족 {turnover / 100_000_000:.0f}억')
        if disp20 < -2.0:
            reject_reasons.append(f'MA20 미회복 {disp20:+.1f}%')
        if disp20 > NEXT_MAX_DISP20:
            reject_reasons.append(f'MA20 과이격 {disp20:+.1f}%')
        if high20_gap > 10.0:
            reject_reasons.append(f'20일고점 이격 {high20_gap:.1f}%')
        if ret5 > NEXT_MAX_RET5:
            reject_reasons.append(f'5일 과열 {ret5:+.1f}%')
        if rsi > 78:
            reject_reasons.append(f'RSI 과열 {rsi:.1f}')
        if rs20 < -3.0:
            reject_reasons.append(f'RS20 약세 {rs20:+.1f}%')

        if reject_reasons:
            return 0, {}, {
                'change_pct': round(float(change_pct), 1),
                'close_location': round(float(close_location * 100), 1),
                'vol_ratio': round(float(vol_ratio), 2),
                'turnover': int(turnover),
                'disp20': round(float(disp20), 1),
                'high20_gap': round(float(high20_gap), 1),
                'ret5': round(float(ret5), 1),
                'rsi': round(float(rsi), 1),
                'rs20': round(float(rs20), 1),
            }, reject_reasons

        bd = {}
        bd['종가강도'] = 25 if close_location >= 0.90 else 21 if close_location >= 0.80 else 17 if close_location >= 0.70 else 12
        bd['당일흐름'] = 15 if 1.0 <= change_pct <= 6.0 else 12 if 0.0 <= change_pct < 1.0 or 6.0 < change_pct <= 9.0 else 7
        bd['거래량확인'] = 20 if vol_ratio >= 2.0 else 16 if vol_ratio >= 1.5 else 12 if vol_ratio >= 1.0 else 7
        bd['고점접근'] = 15 if breakout20 else 13 if high20_gap <= 2.0 else 10 if high20_gap <= 5.0 else 6
        bd['RS20'] = 15 if rs20 >= 8.0 else 12 if rs20 >= 3.0 else 9 if rs20 >= 0.0 else 5
        bd['MA20위치'] = 10 if 0.0 <= disp20 <= 5.0 else 8 if -2.0 <= disp20 < 0.0 or 5.0 < disp20 <= 8.0 else 5

        origin_bonus = 0
        if origin == 'TREND':
            origin_bonus = 5
        elif origin == 'BASIC':
            origin_bonus = 3
        elif origin == 'VALUE':
            origin_bonus = 2
        bd['기존엔진확인'] = origin_bonus

        penalty = 0
        if change_pct > 8.0:
            penalty += 5
        if ret5 > 12.0:
            penalty += 5
        if rsi > 72.0:
            penalty += 4

        raw_score = max(0, sum(bd.values()) - penalty)
        score = int(min(raw_score, 100))
        meta = {
            'change_pct': round(float(change_pct), 1),
            'close_location': round(float(close_location * 100), 1),
            'vol_ratio': round(float(vol_ratio), 2),
            'turnover': int(turnover),
            'disp20': round(float(disp20), 1),
            'high20_gap': round(float(high20_gap), 1),
            'breakout20': bool(breakout20),
            'ret5': round(float(ret5), 1),
            'rsi': round(float(rsi), 1),
            'rs20': round(float(rs20), 1),
            'overheat_penalty': int(penalty),
            'next_origin': origin,
        }
        return score, bd, meta, []
    except Exception:
        return None


def run_close_next_overlay(master_result: dict, top_n: int = NEXT_TOP_N) -> tuple[list[dict], list[dict]]:
    """기존 BUY 후보만 NEXT 관점으로 필터링한다. PRE-SIGNAL/WATCH는 승격하지 않는다."""
    regime_info = master_result.get('regime_info', {}) or {}
    regime = regime_info.get('regime', 'UNKNOWN')
    base_buy_picks = master_result.get('buy_picks', []) or []

    print('\n' + '═' * 60)
    print(f'🌙 CLOSE NEXT 오버레이 — 기존 BUY {len(base_buy_picks)}종목 재검증 | regime={regime}')
    print('═' * 60)

    if regime in ['BEAR', 'UNKNOWN'] or not base_buy_picks:
        print('  ℹ️ NEXT 검토 대상 없음')
        return [], []

    today_str = get_market_date()
    start = get_start_date(today_str, 120)
    kospi_df = _fetch_index_df(['KS11', '^KS11', 'KOSPI'], start, today_str)
    kosdaq_df = _fetch_index_df(['KQ11', '^KQ11', 'KOSDAQ'], start, today_str)

    passed = []
    rejected = []
    for source_pick in base_buy_picks:
        p = _clone_history_ready_pick(source_pick)
        if not p:
            continue
        code = str(p.get('code') or '').zfill(6)
        if not code or code == '000000':
            continue
        market = _pick_market(p)
        index_df = kosdaq_df if 'KOSDAQ' in market else kospi_df
        origin = _next_origin_label(p)
        try:
            df = fdr.DataReader(code, start, today_str)
        except Exception as e:
            rejected.append({'code': code, 'name': p.get('name', code), 'reasons': [f'시세조회 실패: {e}']})
            continue
        scored = _score_close_next_candidate(df, index_df, origin)
        if not scored:
            rejected.append({'code': code, 'name': p.get('name', code), 'reasons': ['NEXT 계산 불가']})
            continue
        next_score, bd, next_meta, reasons = scored
        if reasons:
            rejected.append({'code': code, 'name': p.get('name', code), 'reasons': reasons, 'meta': next_meta})
            print(f"  ⛔ {p.get('name')}({code}) | {' / '.join(reasons)}")
            continue
        if next_score < NEXT_SCORE_MIN:
            rejected.append({'code': code, 'name': p.get('name', code), 'reasons': [f'NEXT 점수 {next_score}점'], 'meta': next_meta})
            print(f"  ⛔ {p.get('name')}({code}) | NEXT {next_score}점 < {NEXT_SCORE_MIN}")
            continue

        meta = dict(p.get('meta', {}) or {})
        base_score = int(_safe_float(p.get('score_100'), 0))
        cur_price = int(_safe_float(df['Close'].iloc[-1], p.get('cur_price', 0)))
        trade = calc_trade_levels(df, cur_price)
        meta.update(next_meta)
        meta.update({
            'next_origin': origin,
            'next_overlay': True,
            'base_score_100': base_score,
            'trade': trade,
            'rt_price_used': False,
        })
        p['cur_price'] = cur_price
        p['base_score_100'] = base_score
        p['score_100'] = next_score
        p['score_detail'] = bd
        p['score'] = f"{'★' * min(next_score // 20, 5)}{'☆' * (5 - min(next_score // 20, 5))} {next_score}점"
        p['tags'] = ' / '.join([k for k, v in sorted(bd.items(), key=lambda x: x[1], reverse=True)[:3] if v > 0])
        p['supply'] = f'NEXT-{origin}'
        p['meta'] = meta
        passed.append(p)
        print(f"  ✅ {p.get('name')}({code}) | NEXT {next_score}점 | 종가위치 {next_meta.get('close_location')}% | 거래량 {next_meta.get('vol_ratio')}배 | RS20 {next_meta.get('rs20'):+}%")

    passed.sort(key=lambda x: (x.get('score_100', 0), x.get('meta', {}).get('close_location', 0)), reverse=True)
    passed = passed[:top_n]
    for idx, p in enumerate(passed, 1):
        p['rank'] = idx
    print(f'🏁 CLOSE NEXT 완료! 통과 {len(passed)}종목 / 제외 {len(rejected)}종목')
    return passed, rejected


def apply_close_next_overlay(master_result: dict) -> dict:
    """CLOSE 세션 전용 결과. 원본 BUY 후보는 보관하고, NEXT만 실제 종가매매 후보로 교체한다."""
    import copy
    result = copy.deepcopy(master_result)
    result['base_buy_picks'] = copy.deepcopy(master_result.get('buy_picks', []))
    next_picks, rejected = run_close_next_overlay(master_result)
    result['next_picks'] = next_picks
    result['next_rejected'] = rejected
    result['buy_picks'] = next_picks
    used = list(result.get('strategy_used', []) or [])
    if 'CLOSE_NEXT_OVERLAY' not in used:
        used.append('CLOSE_NEXT_OVERLAY')
    result['strategy_used'] = used
    result.setdefault('notes', []).append('종가매매는 기존 BUY 후보 중 NEXT 오버레이를 통과한 익일 대응 후보만 사용합니다.')
    return result


def build_close_next_message(result: dict) -> str:
    """장마감 텔레그램 메시지: NEXT 종가매매 후보와 PRE-SIGNAL 관찰 목록을 분리한다."""
    regime_info = result.get('regime_info', {}) or {}
    next_picks = result.get('next_picks', []) or []
    base_buy_picks = result.get('base_buy_picks', []) or []
    rejected = result.get('next_rejected', []) or []
    pre_picks = result.get('pre_signal_picks', []) or []
    today_str = get_market_date()

    lines = [
        f"🌙 <b>NEXT 종가매매 검토 — {ko_date(today_str)} {now_label()}</b>",
        '<i>기존 장세별 BUY 후보를 익일 대응 관점으로 다시 검증한 목록입니다.</i>',
        '═' * 24,
        f"장세: <b>{regime_info.get('regime_label', regime_info.get('regime', 'UNKNOWN'))}</b> | 기존 BUY {len(base_buy_picks)} → NEXT {len(next_picks)}",
        '',
    ]

    if not next_picks:
        lines += [
            '⚠️ <b>NEXT 종가매매 후보 없음</b>',
            '종가 강도·거래량·고점 접근·RS·과열 조건을 모두 충족한 종목이 없습니다.',
            '<i>조건이 없으면 종가매매를 쉬는 것이 원칙입니다.</i>',
        ]
    else:
        for p in next_picks:
            code = p.get('code', '')
            chart = f"https://m.stock.naver.com/domestic/stock/{code}/total"
            meta = p.get('meta', {}) or {}
            trade = meta.get('trade')
            amount_uk = _safe_float(meta.get('turnover'), 0) / 100_000_000
            lines += [
                f"#{p.get('rank')} <b><a href='{chart}'>{p.get('name')}</a></b> ({code}) {p.get('company_summary', '')}",
                f"  🌙 NEXT <b>{p.get('score_100', 0)}점</b> | 원본 {meta.get('next_origin', '')} {meta.get('base_score_100', 0)}점",
                f"  💰 종가 {p.get('cur_price', 0):,}원 | 당일 {meta.get('change_pct', 0):+}% | 거래대금 {amount_uk:,.0f}억",
                f"  📍 종가위치 {meta.get('close_location', 0)}% | 거래량 {meta.get('vol_ratio', 0)}배 | RS20 {meta.get('rs20', 0):+}%",
                f"  📐 MA20 {meta.get('disp20', 0):+}% | 20일고점 이격 {meta.get('high20_gap', 0)}% | RSI {meta.get('rsi', 0)}",
                f"  🏷 {p.get('tags', '')}",
            ]
            if trade:
                lines.append(
                    f"  🎯 익일 대응: 진입 {trade['entry']:,}원 / 손절 {trade['stop_loss']:,}원 (-{trade['risk_pct']}%) / 1차 {trade['target_1']:,}원 (+{trade['reward_pct']}%)"
                )
            lines.append('')

    if rejected:
        lines += [f"ℹ️ 기존 BUY 후보 중 NEXT 제외 {len(rejected)}종목", '']

    if pre_picks:
        lines += ['👀 <b>익일 관찰 PRE-SIGNAL</b> — 종가매매 아님']
        for p in pre_picks[:3]:
            meta = p.get('meta', {}) or {}
            lines.append(
                f"  - {p.get('name')}({p.get('code')}) | PRE {p.get('score_100', 0)}점 | RS20 {meta.get('rs', 0):+}% | MA20 {meta.get('disp20', 0):+}%"
            )
        lines.append('')

    lines += [
        '═' * 24,
        '💡 <i>NEXT는 신규 추천기가 아닙니다. 기존 BUY 후보 중 익일 대응에 적합한 종목만 남깁니다.</i>',
    ]
    return '\n'.join(lines)

def build_session_master_message(result: dict, session: str) -> str:
    """선별식은 그대로 두고, 시간대별로 결과의 용도를 명확히 표시."""
    if session == SESSION_INTRADAY:
        header = [
            '📡 <b>장중 검토 종목</b>',
            '<i>기존 로직으로 선별한 실시간 검토 목록입니다. 장중 추격매수 신호가 아닙니다.</i>',
            '═' * 24,
        ]
    else:
        # CLOSE 세션은 apply_close_next_overlay()를 거친 NEXT 전용 메시지를 사용한다.
        if 'next_picks' in result:
            return build_close_next_message(result)
        header = [
            '🌙 <b>NEXT 종가매매 검토 준비</b>',
            '<i>기존 BUY 후보에 NEXT 오버레이를 적용하기 전 결과입니다.</i>',
            '═' * 24,
        ]
    return '\n'.join(header + ['', build_master_message(result)])


def save_intraday_snapshot(result: dict):
    """장중 스캔은 참고용 JSON만 저장하고 history.json에는 넣지 않는다."""
    payload = {
        'session': SESSION_INTRADAY,
        'saved_at_kst': _kst_now().isoformat(),
        'scan_label': now_label(),
        'base_date': get_market_date(),
        'market_regime': result.get('regime_info', {}).get('regime', 'UNKNOWN'),
        'market_regime_label': result.get('regime_info', {}).get('regime_label', ''),
        'regime_info': result.get('regime_info', {}),
        'strategy_used': result.get('strategy_used', []),
        'notes': result.get('notes', []),
        'buy_picks': result.get('buy_picks', []),
        'watch_picks': result.get('watch_picks', []),
        'pre_signal_picks': result.get('pre_signal_picks', []),
        'trend_picks': result.get('trend_picks', []),
        'value_picks': result.get('value_picks', []),
    }
    with open(INTRADAY_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=4, default=json_safe)
    print(f"✅ {INTRADAY_DATA_FILE} 저장 완료 — history.json 미반영")


def _send_if_text(message):
    if message:
        send_telegram(str(message))


def run_scheduled_kr_engine() -> dict:
    """현재 시간대에 맞는 브리핑 또는 스캔을 실행."""
    session = get_run_session()
    print(f"\n🕒 SESSION ROUTER v5.3 → {session} | KST {_kst_now().strftime('%Y-%m-%d %H:%M')}")

    if session == SESSION_MORNING:
        # 아침에는 전일 복기만 한다. 추천 엔진은 돌리지 않는다.
        _send_if_text(fetch_macro_summary())
        _send_if_text(build_stock_issue_briefing('PREVIOUS'))
        _send_if_text(build_daily_anomaly_message('PREVIOUS'))
        return {'session': session, 'status': 'briefing_only'}

    if session == SESSION_INTRADAY:
        # 장중 결과는 검토 목록이며 history 백테스트를 오염시키지 않는다.
        master_result = run_master_kr_engine()
        _send_if_text(build_session_master_message(master_result, session))
        save_intraday_snapshot(master_result)
        return {'session': session, 'status': 'scan_complete', 'result': master_result}

    # 장 마감 후: 당일 맥락 + 특이 종목 + NEXT 종가매매 후보를 보낸다.
    # 기존 라우터 BUY 후보를 먼저 만든 뒤, 익일 대응 NEXT 오버레이를 추가 적용한다.
    _send_if_text(build_stock_issue_briefing('TODAY'))
    _send_if_text(build_daily_anomaly_message('TODAY'))
    master_result = run_master_kr_engine()
    close_result = apply_close_next_overlay(master_result)
    _send_if_text(build_close_next_message(close_result))
    save_master_result(close_result)
    return {'session': SESSION_CLOSE, 'status': 'scan_complete', 'result': close_result}


# ══════════════════════════════════════════════════════════════
#  엔트리포인트 — v5.3 시간대별 라우터 + CLOSE NEXT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        run_scheduled_kr_engine()
    except Exception as e:
        import traceback
        print(f"\n❌ SESSION ROUTER 예외: {e}")
        traceback.print_exc()
        send_telegram(f"❌ <b>KR SESSION ROUTER 오류</b>\n{e}")
