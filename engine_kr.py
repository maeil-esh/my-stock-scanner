"""
engine_kr.py — 국장 바닥반등+거래량 스캐너 (프로 버전)
실행: python engine_kr.py
스케줄: 09:30 / 13:00 / 16:00 KST

[PRO 기능]
① 상대강도(RS) 필터 — KOSPI 대비 초과수익 종목만
② 시장 레짐 감지 — BULL/NEUTRAL/BEAR 자동 전환
③ ATR 손절/목표가 — 진입 즉시 규율 명시
④ 100점 환산 + 등급 이모티콘
⑤ 네이버 차트 직링크
"""
import os, json, datetime, zipfile, io, time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pykrx import stock
import FinanceDataReader as fdr

from engine_common import (
    DART_API_KEY, get_market_date, get_start_date,
    ko_date, now_label, send_telegram, fetch_macro_summary,
    build_news_briefing, calc_rsi, calc_obv,
    json_safe
)


DATA_FILE    = 'stock_data.json'
HISTORY_FILE = 'history.json'
TOP_N        = 5

_dart_corp_cache = {}

MAX_SCORE = 170
MKTCAP_MIN  = 2000
MKTCAP_MAX  = 30000


# ══════════════════════════════════════════════════════════════
#  100점 환산 + 등급 이모티콘
# ══════════════════════════════════════════════════════════════

def normalize_score(score, max_score):
    """항목 점수를 100점 만점으로 환산"""
    if max_score <= 0:
        return 0
    return int(round(score / max_score * 100))


def grade_emoji(score_100):
    """100점 환산 점수 → 등급 이모티콘"""
    if score_100 >= 90: return "🟢🟢🟢"
    if score_100 >= 70: return "🟢🟢"
    if score_100 >= 50: return "🟡"
    if score_100 >= 30: return "🟠"
    return "🔴"


# ══════════════════════════════════════════════════════════════
#  [PRO-2] 시장 레짐 감지
# ══════════════════════════════════════════════════════════════

def detect_market_regime(start, end):
    """KOSPI MA200 기준 시장 레짐 판단"""
    try:
        kospi = fdr.DataReader('KS11', start, end)
        if len(kospi) < 200:
            return "NEUTRAL", None
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
        print(f"  ⚠️  레짐 감지 실패: {e}")
        return "NEUTRAL", None


def regime_config(regime):
    """레짐별 스캐너 설정"""
    if regime == "BULL":
        return {"top_n": 5, "threshold": 60, "emoji": "🚀", "desc": "공격 모드"}
    elif regime == "BEAR":
        return {"top_n": 2, "threshold": 90, "emoji": "🛡️", "desc": "방어 모드"}
    else:
        return {"top_n": 5, "threshold": 70, "emoji": "⚖️", "desc": "중립 모드"}


# ══════════════════════════════════════════════════════════════
#  [PRO-1] 상대강도(RS) 계산
# ══════════════════════════════════════════════════════════════

def calc_relative_strength(stock_df, index_df, period=63):
    """종목 수익률 vs KOSPI 수익률 차이 (%)"""
    try:
        if len(stock_df) < period or len(index_df) < period:
            return 0.0
        stock_ret = stock_df['Close'].iloc[-1] / stock_df['Close'].iloc[-period] - 1
        index_ret = index_df['Close'].iloc[-1] / index_df['Close'].iloc[-period] - 1
        return round(float((stock_ret - index_ret) * 100), 1)
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════════════
#  [PRO-3] ATR 손절/목표가
# ══════════════════════════════════════════════════════════════

def calc_trade_levels(df, cur_price):
    """ATR 기반 손절/목표가 산출"""
    try:
        high_low   = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close  = (df['Low']  - df['Close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]

        stop_loss = int(cur_price - atr * 1.5)
        target_1  = int(cur_price + atr * 3.0)
        target_2  = int(cur_price + atr * 5.0)
        risk_pct  = round(float((cur_price - stop_loss) / cur_price * 100), 1)
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


# ── pykrx 유틸 ─────────────────────────────────────────────────

def safe_get_market_ticker_list(date_str, market):
    try:
        df = fdr.StockListing('KOSPI' if market == "KOSPI" else 'KOSDAQ')
        col = 'Code' if 'Code' in df.columns else df.columns[0]
        return list(df[col].dropna().astype(str).str.zfill(6))
    except Exception as e:
        print(f"  ❌ fdr {market} 실패: {e}")
        return []


def _find_cap_col(df):
    candidates = ['Marcap','MarCap','marcap','mktcap','MktCap','Market Cap']
    for c in candidates:
        if c in df.columns: return c
    for col in df.columns:
        if 'cap' in col.lower(): return col
    return None


def safe_get_market_cap(ticker, date_str, cache=None):
    if cache and ticker in cache:
        return cache[ticker]
    try:
        for market in ["KOSPI", "KOSDAQ"]:
            df = fdr.StockListing(market)
            col = 'Code' if 'Code' in df.columns else df.columns[0]
            row = df[df[col].astype(str).str.zfill(6) == ticker]
            if not row.empty:
                cap_col = _find_cap_col(row)
                if cap_col:
                    cap = row.iloc[0][cap_col]
                    if cap and cap > 0:
                        return int(cap / 1e8) if cap > 1e6 else int(cap)
    except Exception:
        pass
    try:
        import yfinance as yf
        for suffix in ['.KS', '.KQ']:
            info = yf.Ticker(ticker + suffix).fast_info
            mc = getattr(info, 'market_cap', None)
            if mc and mc > 0:
                return int(mc / 1e8)
    except Exception:
        pass
    return None


# ── DART ───────────────────────────────────────────────────────

def load_dart_corp_codes():
    global _dart_corp_cache
    if _dart_corp_cache: return
    try:
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
        r   = requests.get(url, timeout=15)
        zf  = zipfile.ZipFile(io.BytesIO(r.content))
        xml = zf.read(zf.namelist()[0]).decode('utf-8')
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml)
        for item in root.findall('list'):
            code = (item.findtext('stock_code') or '').strip()
            corp = (item.findtext('corp_code')  or '').strip()
            if code: _dart_corp_cache[code] = corp
        print(f"  📋 DART {len(_dart_corp_cache)}건 로드")
    except Exception as e:
        print(f"  ⚠️  DART 로드 실패: {e}")


# ── 네이버 기업 개요 ───────────────────────────────────────────

def get_company_summary(ticker, name):
    try:
        url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ko-KR,ko;q=0.9'}
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
            th = row.select_one('th'); td = row.select_one('td')
            if th and td and th.text.strip() in ['업종', '주요제품']:
                parts.append(f"{th.text.strip()}: {td.text.strip()}")
        if parts: return ' | '.join(parts)
    except Exception:
        pass
    return f"{name}"


# ── 수급 ───────────────────────────────────────────────────────

def get_investor_detail(ticker, start, end):
    df = None
    try:
        df = stock.get_market_trading_value_by_date(start, end, ticker)
        if df is None or df.empty:
            raise ValueError("pykrx 빈 결과")
    except Exception:
        try:
            raw = fdr.DataReader(f"KRX:{ticker}", start, end)
            if raw is not None and not raw.empty:
                df = raw
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
    inst_streak = 0
    for val in reversed(recent[ic].values):
        if val > 0: inst_streak += 1
        else: break
    for_streak = 0
    for val in reversed(recent[fc].values):
        if val > 0: for_streak += 1
        else: break
    return recent, (fc, ic), inst_streak, for_streak


# ── 채점 ───────────────────────────────────────────────────────

def score_stock(df, inv_df, cols, inst_streak, for_streak):
    """최대 점수: 170점 (A:40 + B:30 + C:30 + D:40 + E:15 + F:15)"""
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

    # [A] 세력값 0선 돌파
    obv_now  = df['OBV'].iloc[-1]
    obv_prev = df['OBV'].iloc[-2]
    obv_5d   = df['OBV'].iloc[-6] if len(df) >= 6 else df['OBV'].iloc[0]
    obv_chg  = (obv_now - obv_5d) / (abs(obv_5d) + 1) * 100
    obv_cross = bool(obv_now > 0 and obv_prev <= 0)
    a = 0
    if obv_cross:
        a = 40
    elif obv_chg > 0:
        a = min(int(obv_chg * 2), 30)
    bd['세력전환'] = a
    meta['obv_chg']   = round(float(obv_chg), 2)
    meta['obv_cross'] = obv_cross

    # [B] 녹색 매집 기간
    obv_diff   = df['OBV'].diff().iloc[-120:]
    green_days = int((obv_diff > 0).sum())
    b = 0
    if   green_days >= 120: b = 30
    elif green_days >= 80:  b = 25
    elif green_days >= 60:  b = 20
    elif green_days >= 30:  b = 12
    elif green_days >= 20:  b = 6
    bd['세력매집']    = b
    meta['green_days'] = green_days

    # [C] 매매신호 30 돌파
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
    elif 30 < signal_now <= 70:
        c = int(20 - abs(signal_now - 50) * 0.4)
    bd['매매신호']      = max(c, 0)
    meta['signal']       = round(float(signal_now), 1)
    meta['signal_cross'] = signal_cross

    # [D] 거래량 스파이크
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

    # [E] 전일 고가 돌파
    e = 15 if cur > prev_hi else 0
    bd['고가돌파'] = e

    # [F] 수급 강도
    f = 0
    supply_text = "정보없음"
    if inv_df is not None and cols:
        fc_col, ic_col = cols
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
    bd['수급강도']     = f
    meta['supply_text'] = supply_text
    meta['inst_streak'] = inst_streak
    meta['for_streak']  = for_streak
    meta['rsi']         = round(float(rsi), 1)
    meta['bb_compress'] = 0

    return sum(bd.values()), bd, meta


# ══════════════════════════════════════════════════════════════
#  메인 스캔
# ══════════════════════════════════════════════════════════════

def run_kr_scan():
    now        = datetime.datetime.now()
    today_str  = get_market_date()
    start_260d = get_start_date(today_str, 180)
    start_10d  = get_start_date(today_str, 10)
    label      = now_label()

    regime, kospi_df = detect_market_regime(start_260d, today_str)
    cfg = regime_config(regime)
    TOP_N_LOCAL     = cfg['top_n']
    SCORE_THRESHOLD = cfg['threshold']

    print(f"📅 기준일: {today_str} | {label}")
    print(f"{cfg['emoji']} 시장 레짐: {regime} ({cfg['desc']}) | TOP {TOP_N_LOCAL}, 임계값 {SCORE_THRESHOLD}점")
    print(f"🎯 전략: 바닥반등 + MA20 우상향 + 거래량 스파이크 + RS>0 + 수급\n")

    load_dart_corp_codes()

    kospi_tickers  = safe_get_market_ticker_list(today_str, "KOSPI")
    kosdaq_tickers = safe_get_market_ticker_list(today_str, "KOSDAQ")
    all_tickers    = kospi_tickers + kosdaq_tickers
    print(f"📋 전체 종목: {len(all_tickers)}개")

    print(f"⚡ 시총 사전 필터링 중 ({MKTCAP_MIN}억↑) — fdr StockListing...")
    mktcap_cache = {}
    pre_filtered = []
    supra_success = False
    try:
        for market in ["KOSPI", "KOSDAQ"]:
            df_cap = fdr.StockListing(market)
            code_col = 'Code' if 'Code' in df_cap.columns else df_cap.columns[0]
            cap_col  = _find_cap_col(df_cap)
            if cap_col:
                for _, row in df_cap.iterrows():
                    ticker_s = str(row[code_col]).zfill(6)
                    cap_raw  = row[cap_col]
                    if not cap_raw or cap_raw != cap_raw:
                        continue
                    cap = int(cap_raw / 1e8) if cap_raw > 1e6 else int(cap_raw)
                    mktcap_cache[ticker_s] = cap
                    if cap >= MKTCAP_MIN:
                        pre_filtered.append(ticker_s)
        if pre_filtered:
            print(f"  → 필터 통과: {len(pre_filtered)}종목")
            supra_success = True
    except Exception as e:
        print(f"  ⚠️  시총 필터 실패: {e} → 전체 스캔")

    all_tickers = pre_filtered if supra_success and pre_filtered else all_tickers

    candidates = []
    log = dict(total=len(all_tickers), penny=0, mktcap=0,
               bottom=0, uptrend=0, vol_spike=0, rs_pass=0, seforce=0, final=0)
    lock = threading.Lock()
    passed_stage1 = []

    def scan_ticker_stage1(ticker):
        time.sleep(0.05)
        try:
            df = fdr.DataReader(ticker, start_260d, today_str)
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
            if mktcap is None:
                mktcap = safe_get_market_cap(ticker, today_str, cache=mktcap_cache)
                if mktcap is not None:
                    mktcap_cache[ticker] = mktcap
            if mktcap is None or not (MKTCAP_MIN <= mktcap <= MKTCAP_MAX):
                return
            with lock:
                log['mktcap'] += 1

            low52 = df['Low'].iloc[-252:].min() if len(df) >= 252 else df['Low'].min()
            if low52 <= 0:
                return
            rebound_pct = (cur - low52) / low52 * 100
            if not (5 <= rebound_pct <= 60):
                return
            with lock:
                log['bottom'] += 1

            vol_ma20 = vol.rolling(20).mean()
            ma20_s   = close.rolling(20).mean()
            if not (ma20_s.iloc[-1] > ma20_s.iloc[-20] and cur > ma20_s.iloc[-1]):
                return
            with lock:
                log['uptrend'] += 1

            recent_60  = df.iloc[-60:]
            spike_mask = recent_60['Volume'] >= vol_ma20.iloc[-60:] * 2.0
            if not spike_mask.any():
                return
            last_spike = recent_60[spike_mask].iloc[-1]
            if cur <= last_spike['Low']:
                return
            si = df.index.get_loc(last_spike.name)
            af = df.iloc[si + 1:]
            silence_ratio = 0.0
            if len(af) >= 3:
                sil = af['Volume'].mean() / (last_spike['Volume'] + 1)
                silence_ratio = round(float(sil), 2)
                if sil > 0.6:
                    return
            with lock:
                log['vol_spike'] += 1

            # [PRO-1] 상대강도 필터
            rs = calc_relative_strength(df, kospi_df) if kospi_df is not None else 0
            if rs <= 0:
                return
            with lock:
                log['rs_pass'] += 1
                passed_stage1.append((ticker, df, vol_ma20, mktcap, rebound_pct, cur,
                                      vol.iloc[-1], vol_ma20.iloc[-1], silence_ratio, rs))
        except Exception:
            return

    print(f"🔍 1단계: 병렬 가격+RS 스크리닝 (5스레드)...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scan_ticker_stage1, t): t for t in all_tickers}
        done = 0
        for fut in as_completed(futures):
            done += 1
            if done % 200 == 0:
                print(f"  진행 {done}/{len(all_tickers)} | 1차 통과: {len(passed_stage1)}건")

    print(f"\n2단계: 수급 스크리닝 {len(passed_stage1)}건 순차 조회...")
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
                inv_df = None
                cols = None
                inst_streak = 0
                for_streak = 0

            total_score, breakdown, meta = score_stock(df, inv_df, cols, inst_streak, for_streak)
            meta['mktcap']        = mktcap
            meta['rebound_pct']   = round(float(rebound_pct), 1)
            meta['cur_close']     = int(cur)
            meta['vol_ratio']     = round(float(cur_vol) / (float(cur_vm20) + 1), 2)
            meta['silence_ratio'] = silence_ratio
            meta['rs']            = rs

            trade_levels = calc_trade_levels(df, cur)
            meta['trade'] = trade_levels

            if total_score >= SCORE_THRESHOLD:
                candidates.append((total_score, ticker, breakdown, meta))
                log['final'] += 1
        except Exception:
            continue

    print(f"\n📊 [필터 현황 — {regime} 레짐]")
    for lbl, key in [("전체","total"),("① 동전주 제외","penny"),
                      ("② 시총 필터","mktcap"),("③ 바닥반등 조건","bottom"),
                      ("④ MA20 우상향","uptrend"),("⑤ 거래량 스파이크","vol_spike"),
                      ("⑥ RS > 0","rs_pass"),("⑦ 수급 확인","seforce"),("최종 통과","final")]:
        print(f"  {lbl:<18} {log[key]:>5}건")

    candidates.sort(key=lambda x: x[0], reverse=True)
    top_raw = candidates[:TOP_N_LOCAL]
    if not top_raw:
        print("⚠️  오늘 조건 충족 종목 없음")

    final_picks = []
    print(f"\n🏆 [TOP {TOP_N_LOCAL}]")
    for rank, (total_score, ticker, breakdown, meta) in enumerate(top_raw, 1):
        try:
            name = stock.get_market_ticker_name(ticker)
        except Exception:
            name = ticker
            try:
                for mkt in ["KOSPI","KOSDAQ"]:
                    df_lst = fdr.StockListing(mkt)
                    col = 'Code' if 'Code' in df_lst.columns else df_lst.columns[0]
                    nm_col = 'Name' if 'Name' in df_lst.columns else 'name' if 'name' in df_lst.columns else None
                    if nm_col:
                        row = df_lst[df_lst[col].astype(str).str.zfill(6)==ticker]
                        if not row.empty:
                            name = row.iloc[0][nm_col]
                            break
            except Exception:
                pass

        summary = get_company_summary(ticker, name)
        star_count = min(total_score // 34, 5)
        stars      = "★" * star_count + "☆" * (5 - star_count)
        supply_text  = meta.get('supply_text', '정보없음')
        expected_ret = round(5.0 + (total_score / MAX_SCORE) * 15.0, 1)
        top_f   = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
        tag_str = " / ".join([f"{k} {v}점" for k, v in top_f[:3]])
        print(f"  #{rank} {name}({ticker}) | {total_score}점 | RS+{meta.get('rs',0)}% | {supply_text}")

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
            }
        })

    # 히스토리 저장
    history_data = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            try:
                history_data = json.load(f)
            except Exception:
                history_data = []
    history_data.append({"date": today_str, "label": label, "regime": regime, "picks": final_picks})
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


if __name__ == "__main__":
    # 매크로 브리핑 + 뉴스
    send_telegram(fetch_macro_summary())
    send_telegram(build_news_briefing())

    # 스캔 실행
    picks, regime, cfg = run_kr_scan()

    # 텔레그램 결과 발송
    if not picks:
        send_telegram(f"{cfg['emoji']} 시장 레짐: <b>{regime}</b> ({cfg['desc']})\n⚠️ 오늘 조건 충족 종목 없음")
    else:
        today_str = get_market_date()
        label     = now_label()
        lines = [
            f"🏆 <b>KR 바닥반등 TOP {len(picks)} — {ko_date(today_str)} {label}</b>",
            f"{cfg['emoji']} 시장 레짐: <b>{regime}</b> ({cfg['desc']})",
            "━" * 24,
        ]
        for p in picks:
            chart_url = f"https://finance.naver.com/item/fchart.naver?code={p['code']}"
            trade  = p['meta'].get('trade')
            rs_val = p['meta'].get('rs', 0)
            sd     = p.get('score_detail', {})

            # 100점 환산
            gather_100 = normalize_score(sd.get('세력매집', 0),       30)
            spike_100  = normalize_score(sd.get('거래량스파이크', 0), 40)
            signal_100 = normalize_score(sd.get('매매신호', 0),       30)

            lines += [
                f"#{p['rank']} <b><a href='{chart_url}'>{p['name']}</a></b> ({p['code']}) {p['score']}",
                f"  💰 현재가: {p['cur_price']:,}원 | RS: +{rs_val}%",
                f"  📈 기대수익: {p['expected_return']}",
                f"",
                f"  📊 <b>핵심 지표</b>",
                f"    {grade_emoji(gather_100)} 세력매집      {gather_100}점",
                f"    {grade_emoji(spike_100)} 거래량스파이크 {spike_100}점",
                f"    {grade_emoji(signal_100)} 매매신호      {signal_100}점",
                f"",
                f"  🏦 수급: {p['supply']}",
            ]

            # 손절/목표가 + A안 금액 환산
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

            lines += [f"  📝 {p['company_summary']}", ""]

        lines.append("━" * 24)
        lines.append("💡 <i>손절은 진입 즉시 예약 주문 — 규율이 곧 승률</i>")
        send_telegram("\n".join(lines))
