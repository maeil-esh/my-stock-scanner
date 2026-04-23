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
MAX_SCORE    = 245  # A(50)+B(50)+C(30)+D(40)+E(15)+F(15)+G(20)+H(15)+I(10)
MKTCAP_MIN   = 1000
MKTCAP_MAX   = 30000

# 네이버 모바일 API 응답 캐시 (ticker → dict) — 가격/업종 공용
_naver_basic_cache: dict = {}


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

        # 업종 — integration API 추가 조회
        try:
            r2   = requests.get(
                f"https://m.stock.naver.com/api/stock/{ticker}/integration",
                headers=headers, timeout=5
            )
            d2   = r2.json()
            data['_industry'] = (
                d2.get('industryGroupKorName') or
                d2.get('indutyNm') or
                d2.get('wicsSectorName') or
                d2.get('sectorName') or ''
            ).strip()
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

def detect_market_regime(start, end):
    kospi = None
    for sym in ['KS11', '^KS11', 'KOSPI']:
        try:
            df = fdr.DataReader(sym, start, end)
            if df is not None and len(df) >= 60:
                kospi = df
                break
        except Exception:
            continue

    if kospi is None or len(kospi) < 60:
        print("  ⚠️  KOSPI 데이터 부족 → NEUTRAL")
        return "NEUTRAL", None

    try:
        ma200 = kospi['Close'].rolling(200).mean().iloc[-1]
        cur   = kospi['Close'].iloc[-1]
        ratio = cur / ma200
        if ratio > 1.02:
            return "BULL", kospi
        elif ratio < 0.98:
            return "BEAR", kospi
        else:
            return "NEUTRAL", kospi
    except Exception as e:
        print(f"  ⚠️  레짐 계산 실패: {e}")
        return "NEUTRAL", kospi


def regime_config(regime):
    if regime == "BULL":
        return {"threshold": 60, "rs_threshold": 5,   "emoji": "🚀", "desc": "공격 모드"}
    elif regime == "BEAR":
        return {"threshold": 90, "rs_threshold": -10, "emoji": "🛡️", "desc": "방어 모드"}
    else:
        return {"threshold": 70, "rs_threshold": -3,  "emoji": "⚖️", "desc": "중립 모드"}


# ══════════════════════════════════════════════════════════════
#  상대강도(RS)
# ══════════════════════════════════════════════════════════════

def calc_relative_strength(stock_df, index_df, period=63):
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
    외인·기관 수급 조회 — 네이버 모바일 investorPurchase API
    """
    try:
        url = f"https://m.stock.naver.com/api/stock/{ticker}/investorPurchase"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer':    'https://m.stock.naver.com/'
        }
        r    = requests.get(url, headers=headers, timeout=6)
        data = r.json()

        items = data.get('list', [])
        if not items:
            return None, None, 0, 0

        rows = []
        for item in items[-5:]:
            try:
                foreign = int(str(item.get('foreignerPureBuyQuant', 0) or 0).replace(',', ''))
                inst    = int(str(item.get('organPureBuyQuant',    0) or 0).replace(',', ''))
                rows.append({'외국인': foreign, '기관': inst})
            except Exception:
                continue

        if not rows:
            return None, None, 0, 0

        inv_df = pd.DataFrame(rows)
        cols   = ('외국인', '기관')

        inst_streak = for_streak = 0
        for val in reversed(inv_df['기관'].values):
            if val > 0: inst_streak += 1
            else: break
        for val in reversed(inv_df['외국인'].values):
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

    # [E] 전일 고가 돌파 (최대 15점, 유지)
    e = 15 if cur > prev_hi else 0
    bd['고가돌파'] = e

    # [F] 수급 강도 (최대 15점, 유지)
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

    # ── DS단석 패턴 가산점 ─────────────────────────────────────

    # [G] VWAP 매물대 돌파 임박 (최대 20점)
    g = 0
    vwap_ratio = 1.0
    try:
        vwap_60    = (df['Close'] * df['Volume']).iloc[-60:].sum() / (df['Volume'].iloc[-60:].sum() + 1)
        vwap_ratio = cur / vwap_60 if vwap_60 > 0 else 1.0
        if   0.98 <= vwap_ratio <= 1.02: g = 20
        elif 0.95 <= vwap_ratio <= 1.05: g = 12
        elif 0.90 <= vwap_ratio <= 1.08: g = 6
    except Exception:
        pass
    bd['매물대근접'] = g
    meta['vwap_ratio'] = round(float(vwap_ratio), 3)

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

    return sum(bd.values()), bd, meta


# ══════════════════════════════════════════════════════════════
#  메인 스캔
# ══════════════════════════════════════════════════════════════

def run_kr_scan():
    today_str  = get_market_date()
    start_260d = get_start_date(today_str, 180)
    start_10d  = get_start_date(today_str, 10)
    label      = now_label()

    regime, kospi_df = detect_market_regime(start_260d, today_str)
    cfg = regime_config(regime)
    SCORE_THRESHOLD = cfg['threshold']
    RS_THRESHOLD    = cfg['rs_threshold']

    print(f"📅 기준일: {today_str} | {label}")
    print(f"{cfg['emoji']} 시장 레짐: {regime} ({cfg['desc']}) | TOP {TOP_N}, 임계값 {SCORE_THRESHOLD}점 | RS ≥ {RS_THRESHOLD:+d}%")
    print(f"🎯 전략: 선행매집 스캔 (OBV↑ / 가격↔ / 변동성수축 / 스파이크 직후)\n")

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
        return [], regime, cfg

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

            rs = calc_relative_strength(df, kospi_df) if kospi_df is not None else 0
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

            # [F4] 안정 박스권 — 최근 10일 고점 대비 -15% 이내 (기존 -10%)
            recent_high_10d = float(df['High'].iloc[-10:].max())
            if recent_high_10d <= 0:
                return
            pullback_pct = (cur - recent_high_10d) / recent_high_10d * 100
            if pullback_pct < -15:                  # -10→-15%
                return
            with lock:
                log['pullback_ok'] += 1

            # [F5] 변동성 수축 ≤ 35% (기존 25%)
            high_20  = float(df['High'].iloc[-20:].max())
            low_20   = float(df['Low'].iloc[-20:].min())
            if low_20 <= 0:
                return
            range_pct = (high_20 - low_20) / low_20 * 100
            if range_pct > 35:                      # 25→35%
                return
            with lock:
                log['vol_contract_ok'] += 1

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

        summary      = get_company_summary(ticker, name)
        star_count   = min(total_score // 34, 5)
        stars        = "★" * star_count + "☆" * (5 - star_count)
        supply_text  = meta.get('supply_text', '정보없음')
        expected_ret = round(5.0 + (total_score / MAX_SCORE) * 15.0, 1)
        top_f        = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
        tag_str      = " / ".join([f"{k} {v}점" for k, v in top_f[:3]])

        final_picks.append({
            "rank": rank, "name": name, "code": ticker,
            "company_summary": summary, "supply": supply_text,
            "cur_price": meta.get('cur_close', 0),
            "score": f"{stars} ({total_score}점/{MAX_SCORE})",
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
                "rt_price_used": rt_price is not None,   # 실시간가 사용 여부
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
        "regime": regime, "picks": final_picks
    })
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4, default=json_safe)

    final_output = {
        "today_picks": final_picks, "scan_label": label,
        "regime": regime, "regime_desc": cfg['desc'],
        "total_candidates": log['final'], "total_screened": log['total'],
        "filter_log": log, "base_date": today_str,
    }
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4, default=json_safe)

    print(f"\n🏁 완료! TOP {len(final_picks)}종목")
    return final_picks, regime, cfg


# ══════════════════════════════════════════════════════════════
#  텔레그램 메시지 조립
# ══════════════════════════════════════════════════════════════

def build_telegram_message(picks, regime, cfg):
    today_str = get_market_date()
    label     = now_label()

    if not picks:
        return (
            f"{cfg['emoji']} 시장 레짐: <b>{regime}</b> ({cfg['desc']})\n"
            f"⚠️ 오늘 조건 충족 종목 없음"
        )

    lines = [
        f"🏆 <b>KR 선행매집 TOP {len(picks)} — {ko_date(today_str)} {label}</b>",
        f"{cfg['emoji']} 시장 레짐: <b>{regime}</b> ({cfg['desc']})",
        "━" * 24,
    ]

    for p in picks:
        chart_url  = f"https://m.stock.naver.com/domestic/stock/{p['code']}/total"
        trade      = p['meta'].get('trade')
        rs_val     = p['meta'].get('rs', 0)
        rt_flag    = "📡 실시간" if p['meta'].get('rt_price_used') else "📋 전일종가"
        sd         = p.get('score_detail', {})

        gather_100  = normalize_score(sd.get('세력매집',     0), 30)
        spike_100   = normalize_score(sd.get('거래량스파이크', 0), 40)
        signal_100  = normalize_score(sd.get('매매신호',      0), 30)
        vwap_100    = normalize_score(sd.get('매물대근접',    0), 20)
        near_100    = normalize_score(sd.get('근거리스파이크', 0), 15)
        obv0_100    = normalize_score(sd.get('OBV0선직전',    0), 10)

        # DS단석 패턴 여부 판단 (3개 가산점 합계 25점 이상)
        ds_score = sd.get('매물대근접', 0) + sd.get('근거리스파이크', 0) + sd.get('OBV0선직전', 0)
        ds_flag  = "🔥 <b>DS패턴 감지</b>" if ds_score >= 25 else ""

        lines += [
            f"#{p['rank']} <b><a href='{chart_url}'>{p['name']}</a></b> ({p['code']}) {p['score']}",
            f"  🏢 {p['company_summary']}",
            f"",
            f"  💰 현재가: {p['cur_price']:,}원 {rt_flag} | RS: {rs_val:+}%",
            f"  📈 기대수익: {p['expected_return']}",
            f"",
            f"  📊 <b>핵심 지표</b>",
            f"    {grade_emoji(gather_100)} 세력매집        {gather_100}점",
            f"    {grade_emoji(spike_100)} 거래량스파이크   {spike_100}점",
            f"    {grade_emoji(signal_100)} 매매신호        {signal_100}점",
            f"",
            f"  💎 <b>대상승 패턴</b>{('  ' + ds_flag) if ds_flag else ''}",
            f"    {grade_emoji(vwap_100)} 매물대근접      {vwap_100}점",
            f"    {grade_emoji(near_100)} 근거리스파이크  {near_100}점",
            f"    {grade_emoji(obv0_100)} OBV 0선직전     {obv0_100}점",
            f"",
            f"  🏦 수급: {p['supply']}",
        ]

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

    picks, regime, cfg = run_kr_scan()
    send_telegram(build_telegram_message(picks, regime, cfg))
