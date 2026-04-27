"""
engine_kr.py — 국장 바닥반등+거래량 스캐너 (PRO v3.3)
실행: python engine_kr.py
스케줄: 09:33 / 13:07 / 16:17 KST

[변경 이력 v3.3] — 회사 정보(업종) 안정화
  네이버 모바일 API의 description 필드 미작동 문제 해결:
  ① FinanceDataReader의 Sector/Industry 컬럼을 1순위 데이터 소스로
  ② 시총 필터링 시 sector_cache에 함께 저장 (추가 API 호출 없음)
  ③ 네이버 API는 fallback으로만 유지
  ④ 외부 API 의존도 ↓ — GitHub Actions 안정성 ↑

[변경 이력 v3.2] — 수급 데이터 하이브리드 소스
  ① 1순위: pykrx (KRX 공식 데이터)
     - GitHub Actions IP 차단 위험 없음
     - 가장 정확한 데이터
     - requirements.txt에 'pykrx' 추가 필요
  ② 2순위: 네이버 스크래핑 (v3.1 강화 로직 유지)
     - pykrx 실패 시 fallback
  ③ 모두 실패 시: 수급 0점 처리

[변경 이력 v3.1] — 수급 스크래핑 강화
  네이버 frgn.naver 페이지에서 수급 0건 문제 해결:
  ① User-Agent를 Chrome 풀 정보로
  ② Referer/Accept/Accept-Encoding 헤더 추가
  ③ iframe URL 직접 호출 (page=1 명시)
  ④ fallback 셀렉터 체인
  ⑤ HTTP 상태 코드 검증 + 다중 URL 재시도

[변경 이력 v3.0] — 큰 흐름 매크로 모드 (전면 재설계)
  방향: A.매우 관대 (10~30건) + 최근 신호 위주

  ── 필터 재설계 ───────────────────────────────────
  미시 비교 필터 전면 제거:
   ✗ 어제vs오늘 단기 거래량 비교
   ✗ 스파이크 직후 5일 평균 비교
   ✗ MA20 ±10% 같은 일일 위치 체크
   ✗ 반복 스파이크 2회 강제 (1회로 완화)
   ✗ 스파이크후 +0~35% 가격 반응 체크

  매크로 추세 필터로 전환:
   ✓ 저점 대비 0~80% (반등 시작 종목 포함)
   ✓ 1~3년 고점 -30%↑ 하락 (장기 흐름)
   ✓ 60일↑ 횡보 (50% 범위 관대)
   ✓ 거래량 살아있음: 최근30일 ≥ 직전90일 × 0.7 (큰 평균 비교)
   ✓ 60일내 스파이크 1회↑ (최소 신호)
   ✓ OBV 60일 누적 매집 추세↑ (큰 흐름 매집)
   ✓ 매매신호 20~85 (바닥주 폭 확대)

  ── 채점 시스템 재설계 (200점 만점) ────────────────
  [큰 흐름 매크로] 130점
    A. OBV 60일 매집 추세        30점
    B. OBV 매집 일수 (120일)     25점
    C. 거래량 살아있음 (30/90)   25점
    D. 장기 횡보 기간             25점
    E. 1~3년 고점 괴리율          25점
  [신호 + 수급] 70점
    F. 매매신호 위치              25점
    G. 거래량 스파이크 강도        25점
    H. 외인/기관 수급             20점

  제거된 채점 항목:
   ✗ 세력전환 (OBV 0선 돌파) — 바닥주 거의 0점
   ✗ 근거리스파이크 (30일내) — 30일 지난 종목 0점
   ✗ OBV 0선 직전 — 음수 OBV만 점수
   ✗ 반복스파이크 — 너무 빡빡한 만점 조건

  점수 임계: 50점 (200점→100환산), TOP_N: 5 (유지)

[변경 이력 v2.5.1] — 거래량 회복 필터 완화
[변경 이력 v2.4] — 필터 병목 해소 (v2.3 실행 결과 0건 원인 제거)
① MA20 박스권 완화: "cur > MA20 탈락" → "MA20 ±10% 범위"
   (서서히 반응 시작된 종목이 MA20 살짝 위에 있을 수 있음)
② 장기 횡보 범위 30% → 45% 완화
   (스파이크 2회 요구와 30% 범위 요구는 물리적 충돌 — 1회 스파이크만으로도 20~30% 범위 발생)

[변경 이력 v2.3] — 민혁님 전략 패턴 필수 필터화
① 52주 고점 → 3년 고점 -35% 필터로 확장
② 장기 횡보 60일 이상 필수 (이전: 채점에만 있었음)
③ 반복 스파이크 2회 이상 필수 (이전: 1회만 있어도 통과)
④ 스파이크 후 거래량 "죽음" 강화 (silence < 0.7)
⑤ 최근 5일 거래량 "회복" 필수 (직전 20일 대비 5%↑)

[변경 이력 v2.2]
★ SCORE_THRESHOLD 원점수 → 100점 환산 50점 기준으로 변경

[변경 이력 v2.1]
★ RS 필터 제거 — 바닥 매집 전략과 논리 충돌
  (고점대비 -25%↑ 바닥주이므로 3개월 상대강도가 양수일 수 없음)
  RS는 표시/참고용으로만 유지 (점수 산정·필터에 사용 안 함)

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
MAX_SCORE    = 200  # v3.0: A(30)+B(25)+C(25)+D(25)+E(25)+F(25)+G(25)+H(20)
MKTCAP_MIN   = 1000
MKTCAP_MAX   = 30000

# 네이버 모바일 API 응답 캐시 (ticker → dict) — 가격/업종 공용
_naver_basic_cache: dict = {}

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
#  상대강도(RS) — 표시/참고용 (필터 아님)
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


def _get_investor_via_pykrx(ticker, start, end):
    """
    수급 데이터 — pykrx (KRX 공식 데이터, 1순위)

    반환: (inv_df, cols, inst_streak, for_streak) 또는 None (실패 시)
    """
    try:
        from pykrx import stock
    except ImportError:
        return None

    try:
        # pykrx는 'YYYYMMDD' 형식 사용
        # start/end는 'YYYY-MM-DD' 또는 datetime — 문자열 변환
        def to_pykrx_date(d):
            s = str(d).replace('-', '').replace('.', '')
            return s[:8]  # YYYYMMDD만

        from_date = to_pykrx_date(start)
        to_date   = to_pykrx_date(end)

        # 종목별 일자별 거래원 매수 거래대금
        df = stock.get_market_trading_value_by_date(from_date, to_date, ticker)
        if df is None or df.empty:
            return None

        # 컬럼명: '기관합계', '기타법인', '개인', '외국인합계', '전체'
        # 일부 종목은 '기타외국인' 또는 컬럼명 차이 있을 수 있음
        inst_col = next((c for c in df.columns if '기관합계' in c or '기관' == c), None)
        for_col  = next((c for c in df.columns if '외국인합계' in c or '외국인' == c), None)
        if not inst_col or not for_col:
            return None

        # 최근 5일 (역순 정렬 — 최신이 [0])
        df_sorted = df.sort_index(ascending=False).head(5)
        inst_vals    = df_sorted[inst_col].astype(int).tolist()
        foreign_vals = df_sorted[for_col].astype(int).tolist()

        if not foreign_vals:
            return None

        inv_df = pd.DataFrame({'외국인': foreign_vals, '기관': inst_vals})
        cols   = ('외국인', '기관')

        # 연속 매수 일수 계산 (최신부터)
        inst_streak = for_streak = 0
        for val in inst_vals:
            if val > 0: inst_streak += 1
            else: break
        for val in foreign_vals:
            if val > 0: for_streak += 1
            else: break

        return inv_df, cols, inst_streak, for_streak

    except Exception as e:
        # pykrx 실패 (네트워크, 종목 데이터 없음 등) — fallback으로 넘김
        return None


def _get_investor_via_naver(ticker):
    """
    수급 데이터 — 네이버 금융 스크래핑 (2순위 fallback)

    반환: (inv_df, cols, inst_streak, for_streak) 또는 None (실패 시)
    """
    headers = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'),
        'Accept': ('text/html,application/xhtml+xml,application/xml;q=0.9,'
                   'image/avif,image/webp,*/*;q=0.8'),
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': f'https://finance.naver.com/item/main.naver?code={ticker}',
        'Connection': 'keep-alive',
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
    """
    외인·기관 수급 — 하이브리드 (v3.2)

    1순위: pykrx (KRX 공식 데이터, IP 차단 없음)
    2순위: 네이버 스크래핑 (강화된 헤더)

    반환: (inv_df, cols, inst_streak, for_streak)
          - 둘 다 실패 시 (None, None, 0, 0)
    """
    # 1순위: pykrx
    result = _get_investor_via_pykrx(ticker, start, end)
    if result is not None:
        return result

    # 2순위: 네이버
    result = _get_investor_via_naver(ticker)
    if result is not None:
        return result

    # 모두 실패
    print(f"  ⚠️  수급 조회 실패({ticker}): pykrx + 네이버 모두 실패")
    return None, None, 0, 0


# ══════════════════════════════════════════════════════════════
#  채점
# ══════════════════════════════════════════════════════════════

def score_stock(df, inv_df, cols, inst_streak, for_streak):
    """
    v3.0 채점 시스템 — 큰 흐름 매크로 위주
    총점 200점 만점 (8개 항목)

    [큰 흐름 매크로] 130점
      A. 장기 OBV 매집 추세 (60일)        30점
      B. OBV 매집 일수 (120일)             25점
      C. 거래량 살아있음/회복 (30/90)      25점
      D. 장기 횡보 기간                    25점
      E. 1~3년 고점 괴리율                 25점

    [신호 + 수급] 70점
      F. 매매신호 위치                     25점
      G. 거래량 스파이크 강도               25점
      H. 외인/기관 수급                    20점
    """
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

    # ════════════════════════════════════════════════════════
    #  [큰 흐름 매크로] 130점
    # ════════════════════════════════════════════════════════

    # ─ A. OBV 60일 매집 추세 (최대 30점) ──────────────
    # 큰 흐름 매집의 핵심 지표
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

    # ─ B. OBV 매집 일수 (최대 25점) ──────────────────
    # 120일 중 OBV 상승일이 많을수록 꾸준한 매집
    obv_diff   = df['OBV'].diff().iloc[-120:]
    green_days = int((obv_diff > 0).sum())
    b = 0
    if   green_days >= 70: b = 25  # 58%↑
    elif green_days >= 60: b = 20
    elif green_days >= 50: b = 15
    elif green_days >= 40: b = 10
    elif green_days >= 30: b = 5
    bd['매집일수'] = b
    meta['green_days'] = green_days

    # ─ C. 거래량 살아있음/회복 (최대 25점) ───────────
    # 최근 30일 평균 vs 직전 90일 평균 — 큰 평균 비교 (노이즈 제거)
    c = 0
    vol_alive_ratio = 0.0
    try:
        if len(df) >= 120:
            recent_30  = float(df['Volume'].iloc[-30:].mean())
            prior_90   = float(df['Volume'].iloc[-120:-30].mean())
            if prior_90 > 0:
                vol_alive_ratio = recent_30 / prior_90
                if   vol_alive_ratio >= 1.5:  c = 25  # 거래량 폭발 시작
                elif vol_alive_ratio >= 1.2:  c = 20  # 명확한 회복
                elif vol_alive_ratio >= 1.0:  c = 15  # 살아남
                elif vol_alive_ratio >= 0.85: c = 10
                elif vol_alive_ratio >= 0.7:  c = 5
    except Exception:
        pass
    bd['거래량살아있음'] = c
    meta['vol_alive_ratio'] = round(float(vol_alive_ratio), 2)

    # ─ D. 장기 횡보 기간 (최대 25점) ─────────────────
    # 50% 범위 내 횡보가 길수록 에너지 축적
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

    # ─ E. 1~3년 고점 괴리율 (최대 25점) ──────────────
    # 고점에서 멀수록 상승 여력 큼
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
    meta['high52_gap'] = high_long_gap   # 키는 호환성 위해 high52_gap 유지

    # ════════════════════════════════════════════════════════
    #  [신호 + 수급] 70점
    # ════════════════════════════════════════════════════════

    # ─ F. 매매신호 위치 (최대 25점) ──────────────────
    # 바닥주 특성 반영 — 25~50 구간 가산점 (반등 시작)
    low_14  = df['Low'].rolling(14).min()
    high_14 = df['High'].rolling(14).max()
    denom   = (high_14 - low_14).iloc[-1]
    stoch_k = (cur - low_14.iloc[-1]) / denom * 100 if denom > 0 else 50
    signal_now  = stoch_k * 0.6 + rsi * 0.4
    f_score = 0
    if   30 <= signal_now <= 55:  f_score = 25  # 반등 초기 — 최적
    elif 25 <= signal_now < 30:   f_score = 18  # 바닥 직전
    elif 55 <  signal_now <= 70:  f_score = 18  # 반등 진행 중
    elif 70 <  signal_now <= 85:  f_score = 10  # 강세 진행 (이미 늦음)
    elif 20 <= signal_now < 25:   f_score = 8
    bd['매매신호'] = f_score
    meta['signal'] = round(float(signal_now), 1)

    # ─ G. 거래량 스파이크 강도 (최대 25점) ────────────
    # 60일 내 최대 스파이크 배수
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

    # ─ H. 외인/기관 수급 (최대 20점) ─────────────────
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

    # ════════════════════════════════════════════════════════
    #  [참고 메타 — 점수 산정 안 함]
    # ════════════════════════════════════════════════════════

    # 반복 스파이크 횟수 (참고)
    spike_count  = 0
    last_spikes  = []
    try:
        vol_ma20_full = df['Volume'].rolling(20).mean()
        spike_mask_full = df['Volume'] >= vol_ma20_full * 2.0
        spike_dates = df.index[spike_mask_full].tolist()
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

    # 이격도 (참고)
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
#  메인 스캔
# ══════════════════════════════════════════════════════════════

def run_kr_scan():
    today_str  = get_market_date()
    start_260d = get_start_date(today_str, 180)
    start_10d  = get_start_date(today_str, 10)
    label      = now_label()

    # KOSPI 데이터 독립 조회 (RS 표시용)
    kospi_df = None
    for sym in ['KS11', '^KS11', 'KOSPI']:
        try:
            df_k = fdr.DataReader(sym, start_260d, today_str)
            if df_k is not None and len(df_k) >= 60:
                kospi_df = df_k
                break
        except Exception:
            continue

    # ★ v3.0 채점 정합성 — 200점 만점, 100점 환산 50점 = 절반
    SCORE_THRESHOLD_100 = 50   # 채점이 v3.0과 정합되어 50점이 진짜 50%
    # ★ RS_THRESHOLD 제거 — 바닥 매집 전략과 논리 충돌 (v2.1)

    print(f"📅 기준일: {today_str} | {label}")
    print(f"🎯 전략: 큰 흐름 매크로 스캔 (v3.0) | TOP {TOP_N} | 임계 {SCORE_THRESHOLD_100}점 (200점→100환산)\n")

    # ── 시총 사전 필터링 ────────────────────────────────────────
    print(f"⚡ 시총 사전 필터링 ({MKTCAP_MIN}억↑ ~ {MKTCAP_MAX}억↓)...")
    mktcap_cache = {}
    sector_cache = {}   # ★ v3.3: 업종/산업 정보 캐시 (FDR 기반)
    all_tickers  = []
    try:
        for market in ["KOSPI", "KOSDAQ"]:
            df_cap   = fdr.StockListing(market)
            code_col = 'Code' if 'Code' in df_cap.columns else df_cap.columns[0]
            cap_col  = _find_cap_col(df_cap)
            if not cap_col:
                raise ValueError(f"{market} 시총 컬럼 없음")

            sector_col  = next((c for c in ['Sector','sector','업종'] if c in df_cap.columns), None)
            product_col = next((c for c in ['Industry','industry','주요제품'] if c in df_cap.columns), None)

            # ★ v3.3 디버그: FDR 실제 컬럼/데이터 확인 (1회만)
            print(f"  🔍 [{market}] FDR 전체 컬럼 ({len(df_cap.columns)}개):")
            for i, col in enumerate(df_cap.columns):
                print(f"       {i+1}. {col}")
            print(f"  🔍 [{market}] sector_col={sector_col}, product_col={product_col}")
            if sector_col or product_col:
                # 첫 3개 종목의 실제 값 샘플
                sample = df_cap.head(3)
                for _, r in sample.iterrows():
                    tk = str(r.get(code_col, '?')).zfill(6)
                    sv = r.get(sector_col, 'N/A') if sector_col else 'N/A'
                    iv = r.get(product_col, 'N/A') if product_col else 'N/A'
                    print(f"  🔍   {tk}: sector='{sv}', industry='{iv}'")

            for _, row in df_cap.iterrows():
                ticker_s = str(row[code_col]).zfill(6)
                cap_raw  = row[cap_col]
                if not cap_raw or cap_raw != cap_raw:
                    continue
                cap = int(cap_raw / 1e8) if cap_raw > 1e6 else int(cap_raw)
                mktcap_cache[ticker_s] = cap

                # ★ v3.3: 업종/산업 정보 캐시
                sector_info = {
                    'market':   market,
                    'sector':   str(row[sector_col]) if sector_col and pd.notna(row.get(sector_col)) else '',
                    'industry': str(row[product_col]) if product_col and pd.notna(row.get(product_col)) else '',
                }
                sector_cache[ticker_s] = sector_info

                if MKTCAP_MIN <= cap <= MKTCAP_MAX:
                    all_tickers.append(ticker_s)
        print(f"  → 필터 통과: {len(all_tickers)}종목")
    except Exception as e:
        print(f"  ❌ 시총 필터 실패: {e} → 스캔 중단")
        return []

    candidates = []
    # ★ v3.0: 큰 흐름 위주 필터 — 미시 비교 제거
    log = dict(total=len(all_tickers), penny=0, mktcap=len(all_tickers),
               bottom=0, high_gap=0, sideways=0,
               vol_alive=0, vol_recent=0,
               obv_trend=0, signal_ok=0,
               seforce=0, final=0)
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

            # ════════════════════════════════════════════════════
            #  v3.0 큰 흐름 필터 — 매크로 추세 중심
            # ════════════════════════════════════════════════════

            # ─ ① 저점 대비 위치 (관대: 0~80%) ─────────────────
            # 이미 좀 반등 시작한 종목까지 포함
            low52 = df['Low'].iloc[-252:].min() if len(df) >= 252 else df['Low'].min()
            if low52 <= 0:
                return
            rebound_pct = (cur - low52) / low52 * 100
            if not (0 <= rebound_pct <= 80):
                return
            with lock:
                log['bottom'] += 1

            # ─ ② 1~3년 고가 대비 -30%↑ 하락 (관대화) ────────
            long_window = min(len(df), 756)
            high_long = df['High'].iloc[-long_window:].max()
            if high_long <= 0:
                return
            long_gap_pct = (high_long - cur) / high_long * 100
            if long_gap_pct < 30:   # 35→30 완화
                return
            with lock:
                log['high_gap'] += 1

            # ─ ③ 장기 횡보 (60일↑, 범위 50% 관대) ──────────
            sideways_days_f = 0
            for window_s in [200, 120, 90, 60]:
                if len(df) < window_s:
                    continue
                seg_s   = df.iloc[-window_s:]
                hi_s    = float(seg_s['High'].max())
                lo_s    = float(seg_s['Low'].min())
                rng_pct = (hi_s - lo_s) / lo_s * 100 if lo_s > 0 else 999
                if rng_pct <= 50:    # 45→50 더 관대
                    sideways_days_f = window_s
                    break
            if sideways_days_f < 60:
                return
            with lock:
                log['sideways'] += 1

            # ════════════════════════════════════════════════════
            #  최근 신호 위주 — "거래량이 깨어나고 있다"
            # ════════════════════════════════════════════════════

            # ─ ④ 거래량 살아있음 (최근 30일 ≥ 직전 90일 × 0.7) ─
            # "죽지 않고 살아있다"가 핵심 — 큰 흐름에서 거래량은 노이즈 많아 관대화
            if len(vol) >= 120:
                recent_30  = float(vol.iloc[-30:].mean())
                prior_90   = float(vol.iloc[-120:-30].mean())
                if prior_90 <= 0:
                    return
                vol_alive_ratio = recent_30 / prior_90
                if vol_alive_ratio < 0.7:   # 죽어버린 종목만 탈락
                    return
            else:
                return
            with lock:
                log['vol_alive'] += 1

            # ─ ⑤ 최근 60일 내 의미있는 스파이크 1회↑ ───────
            # 2회 강제 → 1회로 완화 (관대 모드)
            vol_ma20_full   = vol.rolling(20).mean()
            recent_60_vol   = vol.iloc[-60:]
            recent_60_ma    = vol_ma20_full.iloc[-60:]
            spike_mask_60   = recent_60_vol >= recent_60_ma * 2.0
            if not spike_mask_60.any():
                return
            with lock:
                log['vol_recent'] += 1

            # ─ ⑥ OBV 누적 매집 추세 (큰 흐름) ──────────────
            # 60일 OBV 기울기가 양수 = 세력 매집 진행 중
            obv = calc_obv(df)
            if len(obv) < 60:
                return
            obv_60ago = float(obv.iloc[-60])
            obv_now_v = float(obv.iloc[-1])
            obv_60_change = (obv_now_v - obv_60ago) / (abs(obv_60ago) + 1) * 100
            if obv_60_change <= 0:   # OBV 60일 하락 = 세력 이탈 = 탈락
                return
            with lock:
                log['obv_trend'] += 1

            # ─ ⑦ 매매신호 폭 확대 20~85 (관대) ──────────────
            # 바닥주는 시그널 25 미만도 많음 — 20으로 하향
            rsi_ser   = calc_rsi(close)
            rsi_cur   = float(rsi_ser.iloc[-1])
            low_14    = df['Low'].rolling(14).min().iloc[-1]
            high_14   = df['High'].rolling(14).max().iloc[-1]
            denom     = high_14 - low_14
            stoch_cur = (cur - low_14) / denom * 100 if denom > 0 else 50.0
            signal_cur = stoch_cur * 0.6 + rsi_cur * 0.4
            if not (20 <= signal_cur <= 85):
                return
            with lock:
                log['signal_ok'] += 1

            # RS 표시용 (필터 아님)
            rs = calc_relative_strength(df, kospi_df)

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
            meta['mktcap']        = mktcap
            meta['rebound_pct']   = round(float(rebound_pct), 1)
            meta['cur_close']     = int(cur)
            meta['vol_ratio']     = round(float(cur_vol) / (float(cur_vm20) + 1), 2)
            meta['rs']            = rs
            meta['trade']         = calc_trade_levels(df, cur)
            meta['spike_news']    = []

            # ★ 100점 환산 점수로 임계 비교 (v2.2)
            score_100_check = int(round(total_score / MAX_SCORE * 100))
            if score_100_check >= SCORE_THRESHOLD_100:
                candidates.append((total_score, ticker, breakdown, meta, df))
                log['final'] += 1
        except Exception:
            continue

    # ── 필터 현황 출력 (v3.0) ──────────────────────────────
    print(f"\n📊 [필터 현황 — v3.0 큰 흐름 모드]")
    for lbl, key in [
        ("전체", "total"),
        ("① 동전주 제외", "penny"),
        ("② 시총 필터", "mktcap"),
        ("③ 저점대비 0~80%", "bottom"),
        ("④ 1~3년고점 -30%↑ 하락", "high_gap"),
        ("⑤ 장기 횡보 60일↑ (50%↓)", "sideways"),
        ("⑥ 거래량 살아있음 (≥0.7)", "vol_alive"),
        ("⑦ 60일내 스파이크 1회↑", "vol_recent"),
        ("⑧ OBV 60일 매집 추세↑", "obv_trend"),
        ("⑨ 매매신호 20~85", "signal_ok"),
        ("⑩ 수급 확인", "seforce"),
        ("최종 통과", "final"),
    ]:
        print(f"  {lbl:<28} {log[key]:>5}건")

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

        # ── 스파이크 시점 뉴스 수집 ──────────────────────────
        spike_date_strs = meta.get('last_spikes', [])
        meta['spike_news'] = fetch_spike_news(ticker, spike_date_strs)

        # ── 회사 요약 (v3.3): FDR 캐시 1순위, 네이버 fallback ──
        sector_info = sector_cache.get(ticker, {})
        cached_market   = sector_info.get('market', '')
        cached_sector   = sector_info.get('sector', '').strip()
        cached_industry = sector_info.get('industry', '').strip()

        summary_parts = []
        if cached_market:
            summary_parts.append(f"[{cached_market}]")
        if cached_sector and cached_sector.lower() not in ['nan', 'none', '']:
            summary_parts.append(cached_sector)
        if (cached_industry and cached_industry.lower() not in ['nan', 'none', '']
                and cached_industry != cached_sector):
            summary_parts.append(cached_industry[:30])

        if len(summary_parts) >= 2:
            # FDR로 충분한 정보 확보 (시장 + 업종 이상)
            summary = ' '.join(summary_parts)
        else:
            # FDR 정보 부족 시 네이버 fallback
            summary = get_company_summary(ticker, name)

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
            "cur_price": meta.get('cur_close', 0),
            "score_100": score_100,
            "score": f"{stars} {score_100}점",
            "tags": tag_str, "expected_return": f"{expected_ret}%",
            "score_detail": breakdown,
            "meta": {
                "rsi":           meta.get('rsi', 0),
                "rebound_pct":   meta.get('rebound_pct', 0),
                "vol_ratio":     meta.get('vol_ratio', 0),
                "inst_streak":   meta.get('inst_streak', 0),
                "for_streak":    meta.get('for_streak', 0),
                "mktcap":        meta.get('mktcap', 0),
                "green_days":    meta.get('green_days', 0),
                "signal":        meta.get('signal', 0),
                "max_spike":     meta.get('max_spike', 0),
                "rs":            meta.get('rs', 0),
                "trade":         meta.get('trade', None),
                "rt_price_used": rt_price is not None,
                "spike_count":   meta.get('spike_count', 0),
                "disp20":        meta.get('disp20', 0.0),
                "disp60":        meta.get('disp60', 0.0),
                "last_spikes":   meta.get('last_spikes', []),
                "spike_news":    meta.get('spike_news', []),
                "sideways_days": meta.get('sideways_days', 0),
                "high52_gap":    meta.get('high52_gap', 0.0),
                # v3.0 신규 메타
                "vol_alive_ratio": meta.get('vol_alive_ratio', 0.0),
                "obv_60_change":   meta.get('obv_60_change', 0.0),
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

        filled = int(score_100 / 10)
        bar    = '█' * filled + '░' * (10 - filled)

        # ★ v3.0 채점 키 매핑 ────────────────────────────
        obv60_100    = normalize_score(sd.get('OBV60일매집',  0), 30)
        gather_100   = normalize_score(sd.get('매집일수',     0), 25)
        valive_100   = normalize_score(sd.get('거래량살아있음', 0), 25)
        side_100     = normalize_score(sd.get('횡보기간',     0), 25)
        gap_100      = normalize_score(sd.get('고점괴리',     0), 25)
        signal_100   = normalize_score(sd.get('매매신호',     0), 25)
        spike_100    = normalize_score(sd.get('거래량스파이크', 0), 25)
        supply_100   = normalize_score(sd.get('수급강도',     0), 20)

        spike_cnt     = p['meta'].get('spike_count', 0)
        disp20_val    = p['meta'].get('disp20', 0.0)
        disp60_val    = p['meta'].get('disp60', 0.0)
        sideways_days = p['meta'].get('sideways_days', 0)
        high52_gap    = p['meta'].get('high52_gap', 0.0)
        vol_alive     = p['meta'].get('vol_alive_ratio', 0.0)
        obv_60_chg    = p['meta'].get('obv_60_change', 0.0)
        spike_news    = p['meta'].get('spike_news', [])

        # 매크로 흐름 강도 = OBV60 + 매집일수 + 거래량살아있음 (130점 만점 → 100환산)
        macro_raw   = sd.get('OBV60일매집', 0) + sd.get('매집일수', 0) + sd.get('거래량살아있음', 0)
        macro_100   = int(round(macro_raw / 80 * 100))
        macro_flag  = "🔥 <b>매크로 매집 강세</b>" if macro_100 >= 70 else ""

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
