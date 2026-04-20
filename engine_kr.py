"""
engine_kr.py — 국장 바닥반등+거래량 스캐너
실행: python engine_kr.py
스케줄: 09:30 / 13:00 / 16:00 KST
"""
import os, json, datetime, zipfile, io
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pykrx import stock
import FinanceDataReader as fdr

from engine_common import (
    DART_API_KEY, get_market_date, get_start_date,
    ko_date, now_label, send_telegram, fetch_macro_summary,
    build_news_briefing, calc_rsi, calc_obv
)


DATA_FILE    = 'stock_data.json'
HISTORY_FILE = 'history.json'
TOP_N        = 5

_dart_corp_cache = {}


# ── pykrx 유틸 ─────────────────────────────────────────────────

def safe_get_market_ticker_list(date_str, market):
    try:
        tickers = stock.get_market_ticker_list(date_str, market=market)
        if tickers and len(tickers) > 0: return list(tickers)
    except Exception as e:
        print(f"  ⚠️  pykrx {market} 실패: {e} → fdr 백업")
    try:
        df = fdr.StockListing('KOSPI' if market == "KOSPI" else 'KOSDAQ')
        return list(df['Code'].dropna().astype(str).str.zfill(6))
    except Exception as e:
        print(f"  ❌ fdr {market} 백업 실패: {e}")
        return []


def _find_cap_col(df):
    candidates = ['시가총액','Mktcap','MktCap','mktcap','marcap','Marcap']
    for c in candidates:
        if c in df.columns: return c
    for col in df.columns:
        if '시가' in col or 'cap' in col.lower(): return col
    return None


def safe_get_market_cap(ticker, date_str):
    try:
        df = stock.get_market_cap(date_str, date_str, ticker)
        if df is not None and not df.empty:
            cap_col = _find_cap_col(df)
            if cap_col:
                val = df[cap_col].iloc[-1]
                if val > 0: return int(val / 1e8)
    except Exception:
        pass
    try:
        for market in ["KOSPI","KOSDAQ"]:
            df = fdr.StockListing(market)
            row = df[df['Code'] == ticker]
            if not row.empty:
                for col in ['Marcap','MarCap','marcap','mktcap','MktCap']:
                    if col in row.columns:
                        cap = row.iloc[0][col]
                        if cap and cap > 0: return int(cap / 1e8)
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
    try:
        df = stock.get_market_trading_value_by_date(start, end, ticker)
    except Exception:
        return None, None, 0, 0
    if df is None or df.empty: return None, None, 0, 0
    fc = ic = None
    for col in df.columns:
        if '외국인' in col: fc = col
        if '기관'   in col: ic = col
    if not fc or not ic: return None, None, 0, 0
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


# ── 채점 — 민혁님 실전 수익 6종목 기반 매집→매수타점 패턴 ────

def score_stock(df, inv_df, cols, inst_streak, for_streak):
    """
    매수타점 3단계:
    [필수] 세력값 0선 돌파 + 매매신호 30 돌파 + 거래량 2배↑
    [가산] 녹색기간 60일↑ + 전일고가 돌파 + 저가반등 20~80%
    """
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
    bd = {}; meta = {}

    # ── [A] 세력값 0선 돌파 (핵심 — 최대 40점) ─────────────────
    # OBV 5일 변화율로 세력 전환 감지
    obv_now  = df['OBV'].iloc[-1]
    obv_prev = df['OBV'].iloc[-2]
    obv_5d   = df['OBV'].iloc[-6] if len(df) >= 6 else df['OBV'].iloc[0]
    obv_chg  = (obv_now - obv_5d) / (abs(obv_5d) + 1) * 100

    # 전일 음수 → 당일 양수 전환 = 세력 매집 완료 신호
    obv_cross = obv_now > 0 and obv_prev <= 0
    a = 0
    if obv_cross:
        a = 40  # 전환 당일 최고점
    elif obv_chg > 0:
        a = min(int(obv_chg * 2), 30)  # 상승 중이면 비례 점수
    bd['세력전환'] = a
    meta['obv_chg'] = round(obv_chg, 2)
    meta['obv_cross'] = obv_cross

    # ── [B] 녹색 매집 기간 (최대 30점) ─────────────────────────
    # OBV가 하락(음수)했던 일수 = 세력 저점 매집 기간
    obv_diff = df['OBV'].diff().iloc[-120:]  # 최근 120거래일
    green_days = int((obv_diff < 0).sum())
    b = 0
    if   green_days >= 120: b = 30  # 6개월↑ — 엔켐형 대시세
    elif green_days >= 80:  b = 25  # 4개월↑ — 한컴위드형
    elif green_days >= 60:  b = 20  # 3개월↑ — 글로벌텍스프리형
    elif green_days >= 30:  b = 12  # 1.5개월↑ — PI첨단소재형
    elif green_days >= 20:  b = 6   # 1개월↑ — 테크윙형
    bd['세력매집'] = b
    meta['green_days'] = green_days

    # ── [C] 매매신호 30 돌파 (최대 30점) ───────────────────────
    # 스토캐스틱 + RSI 합성
    low_14  = df['Low'].rolling(14).min()
    high_14 = df['High'].rolling(14).max()
    denom   = (high_14 - low_14).iloc[-1]
    stoch_k = (cur - low_14.iloc[-1]) / denom * 100 if denom > 0 else 50
    stoch_p = (df['Close'].iloc[-2] - low_14.iloc[-2]) /               max((high_14.iloc[-2] - low_14.iloc[-2]), 1) * 100
    signal_now  = stoch_k * 0.6 + rsi * 0.4
    signal_prev = stoch_p * 0.6 + rsi_prev * 0.4

    c = 0
    signal_cross = signal_now > 30 and signal_prev <= 30  # 30선 돌파
    if signal_cross:
        c = 30  # 돌파 당일 최고
    elif 30 < signal_now <= 70:
        c = int(20 - abs(signal_now - 50) * 0.4)  # 30~70 구간 비례
    bd['매매신호'] = max(c, 0)
    meta['signal'] = round(signal_now, 1)
    meta['signal_cross'] = signal_cross

    # ── [D] 거래량 스파이크 강도 (최대 40점) ────────────────────
    # 최근 60일 중 최대 스파이크 배수로 등급 산정
    vol_ma20_ser = df['Volume'].rolling(20).mean()
    recent_60    = df.iloc[-60:]
    spike_ratios = recent_60['Volume'] / (vol_ma20_ser.iloc[-60:] + 1)
    max_spike    = spike_ratios.max() if not spike_ratios.empty else 0
    cur_ratio    = cur_vol / (vol_ma20 + 1)

    d = 0
    if   max_spike >= 10: d = 40  # 엔켐형 — 10배↑ 대시세
    elif max_spike >= 5:  d = 30  # 5~10배 — 강한 신호
    elif max_spike >= 3:  d = 20  # 3~5배  — 중간 신호
    elif max_spike >= 2:  d = 10  # 2~3배  — 기본 신호
    bd['거래량스파이크'] = d
    meta['max_spike']  = round(max_spike, 1)
    meta['vol_ratio']  = round(cur_ratio, 2)

    # ── [E] 전일 고가 돌파 (최대 15점) ──────────────────────────
    e = 15 if cur > prev_hi else 0
    bd['고가돌파'] = e

    # ── [F] 수급 강도 (최대 15점) ────────────────────────────────
    f = 0; supply_text = "정보없음"
    if inv_df is not None and cols:
        fc, ic = cols
        fd        = int((inv_df[fc] > 0).sum())
        id_       = int((inv_df[ic] > 0).sum())
        both_days = int(((inv_df[fc] > 0) & (inv_df[ic] > 0)).sum())
        f = min(inst_streak * 3 + for_streak * 2 + both_days * 2, 15)
        if fd > 0 and id_ > 0: supply_text = "외인+기관 양매수"
        elif id_ > 0:           supply_text = "기관매수"
        elif fd > 0:            supply_text = "외인매수"
    bd['수급강도'] = f
    meta['supply_text'] = supply_text
    meta['inst_streak'] = inst_streak
    meta['for_streak']  = for_streak
    meta['rsi']         = round(rsi, 1)
    meta['bb_compress'] = 0  # 하위 호환용


    return sum(bd.values()), bd, meta


# ══════════════════════════════════════════════════════════════
#  메인 스캔
# ══════════════════════════════════════════════════════════════

def run_kr_scan():
    now        = datetime.datetime.now()
    today_str  = get_market_date()
    start_260d = get_start_date(today_str, 260)
    start_10d  = get_start_date(today_str, 10)
    label      = now_label()

    print(f"📅 기준일: {today_str} | {label}")
    print(f"🎯 전략: 바닥반등 5~60% + MA20 우상향 + 거래량 스파이크 + 세력(기관OR외인)\n")

    load_dart_corp_codes()

    kospi_tickers  = safe_get_market_ticker_list(today_str, "KOSPI")
    kosdaq_tickers = safe_get_market_ticker_list(today_str, "KOSDAQ")
    all_tickers    = kospi_tickers + kosdaq_tickers
    print(f"📋 전체 종목: {len(all_tickers)}개")

    # 시총 사전 필터 (1000억~3조)
    print("⚡ 시총 사전 필터링 중 (1000억~3조)...")
    mktcap_cache = {}; pre_filtered = []; supra_success = False
    try:
        df_cap = stock.get_market_cap(today_str)
        if df_cap is not None and not df_cap.empty:
            cap_col = _find_cap_col(df_cap)
            if cap_col:
                for idx, row in df_cap.iterrows():
                    cap = int(row[cap_col] / 1e8)
                    mktcap_cache[str(idx)] = cap
                    if 1000 <= cap <= 30000: pre_filtered.append(str(idx))
                print(f"  → 필터 통과: {len(pre_filtered)}종목")
                supra_success = True
    except Exception as e:
        print(f"  ⚠️  시총 필터 실패: {e} → 전체 스캔")

    all_tickers = pre_filtered if supra_success and pre_filtered else all_tickers

    candidates = []
    log = dict(total=len(all_tickers), penny=0, mktcap=0, obv_up=0, vol_spike=0, seforce=0, final=0)
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed
    lock = threading.Lock()

    # 1단계: 병렬 — 가격/기술적 필터 (①~⑤)
    passed_stage1 = []  # (ticker, df, vol_ma20, mktcap, rebound_pct, cur, cur_vol, cur_vm20)

    def scan_ticker_stage1(ticker):
        try:
            df = fdr.DataReader(ticker, start_260d, today_str)
            if len(df) < 120: return
            close = df['Close']; vol = df['Volume']; cur = close.iloc[-1]

            # ① 동전주
            if cur < 1000: return
            with lock: log['penny'] += 1

            # ② 시총 2000억~3조
            mktcap = mktcap_cache.get(ticker)
            if mktcap is None:
                mktcap = safe_get_market_cap(ticker, today_str)
                if mktcap is not None: mktcap_cache[ticker] = mktcap
            if mktcap is None or not (2000 <= mktcap <= 30000): return
            with lock: log['mktcap'] += 1

            # ③ 바닥 확인: 52주 저가 대비 +5~60%
            low52 = df['Low'].iloc[-252:].min() if len(df) >= 252 else df['Low'].min()
            if low52 <= 0: return
            rebound_pct = (cur - low52) / low52 * 100
            if not (5 <= rebound_pct <= 60): return
            with lock: log['bottom'] += 1

            # ④ MA20 완만한 우상향 + 현재가 MA20 위
            vol_ma20 = vol.rolling(20).mean()
            ma20_s   = close.rolling(20).mean()
            if not (ma20_s.iloc[-1] > ma20_s.iloc[-20] and cur > ma20_s.iloc[-1]): return
            with lock: log['uptrend'] += 1

            # ⑤ 최근 60일 거래량 스파이크 + 이후 수축
            recent_60  = df.iloc[-60:]
            spike_mask = recent_60['Volume'] >= vol_ma20.iloc[-60:] * 2.0
            if not spike_mask.any(): return
            last_spike = recent_60[spike_mask].iloc[-1]
            if cur <= last_spike['Low']: return
            si = df.index.get_loc(last_spike.name)
            af = df.iloc[si + 1:]
            if len(af) >= 3:
                sil = af['Volume'].mean() / (last_spike['Volume'] + 1)
                if sil > 0.6: return
            with lock:
                log['vol_spike'] += 1
                passed_stage1.append((ticker, df, vol_ma20, mktcap, rebound_pct, cur,
                                      vol.iloc[-1], vol_ma20.iloc[-1]))
        except Exception:
            return

    print(f"🔍 1단계: 병렬 가격 스크리닝 (10스레드)...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scan_ticker_stage1, t): t for t in all_tickers}
        done = 0
        for f in as_completed(futures):
            done += 1
            if done % 200 == 0:
                print(f"  진행 {done}/{len(all_tickers)} | 1차 통과: {len(passed_stage1)}건")

    # 2단계: 순차 — 수급 조회 (pykrx 멀티스레드 불안정 방지)
    print("\n2단계: 수급 스크리닝 " + str(len(passed_stage1)) + "건 순차 조회...")
    for ticker, df, vol_ma20, mktcap, rebound_pct, cur, cur_vol, cur_vm20 in passed_stage1:
        try:
            inv_df, cols, inst_streak, for_streak = get_investor_detail(ticker, start_10d, today_str)
            # 수급 조회 실패해도 탈락 안 함 — 점수 0점으로 처리
            if inv_df is not None and cols is not None:
                fc, ic  = cols
                any_buy = int(((inv_df[fc] > 0) | (inv_df[ic] > 0)).sum())
                if any_buy >= 1: log['seforce'] += 1
            else:
                inv_df = None; cols = None; inst_streak = 0; for_streak = 0

            # 채점
            total_score, breakdown, meta = score_stock(df, inv_df, cols, inst_streak, for_streak)
            meta['mktcap']      = mktcap
            meta['rebound_pct'] = round(rebound_pct, 1)
            meta['cur_close']   = int(cur)
            meta['vol_ratio']   = round(cur_vol / (cur_vm20 + 1), 2)

            if total_score >= 60:
                candidates.append((total_score, ticker, breakdown, meta))
                log['final'] += 1
        except Exception:
            continue

    print(f"\n📊 [필터 현황]")
    for lbl, key in [("전체","total"),("① 동전주","penny"),("② 시총 2000억~3조","mktcap"),
                      ("③ 바닥반등 5~60%","bottom"),("④ MA20 우상향","uptrend"),
                      ("⑤ 거래량 스파이크","vol_spike"),("⑥ 세력(기관OR외인)","seforce"),
                      ("최종 통과","final")]:
        print(f"  {lbl:<18} {log[key]:>5}건")

    candidates.sort(key=lambda x: x[0], reverse=True)
    top_raw = candidates[:TOP_N]
    if not top_raw: print("⚠️  오늘 조건 충족 종목 없음")

    final_picks = []
    print(f"\n🏆 [TOP {TOP_N}]")
    for rank, (total_score, ticker, breakdown, meta) in enumerate(top_raw, 1):
        name         = stock.get_market_ticker_name(ticker)
        summary      = get_company_summary(ticker, name)
        stars        = "★" * min(total_score // 50, 5) + "☆" * max(5 - total_score // 50, 0)
        supply_text  = meta.get('supply_text', '정보없음')
        expected_ret = round(5.0 + (total_score / 250) * 15.0, 1)
        top_f        = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
        tag_str      = " / ".join([f"{k} {v}점" for k, v in top_f[:3]])
        print(f"  #{rank} {name}({ticker}) | {total_score}점 | {supply_text}")

        final_picks.append({
            "rank": rank, "name": name, "code": ticker,
            "company_summary": summary, "supply": supply_text,
            "cur_price": meta.get('cur_close', 0),
            "score": f"{stars} ({total_score}점/250)",
            "tags": tag_str, "expected_return": f"{expected_ret}%",
            "score_detail": breakdown,
            "meta": {
                "rsi": meta.get('rsi', 0), "bb_compress": meta.get('bb_compress', 0),
                "rebound_pct": meta.get('rebound_pct', 0), "vol_ratio": meta.get('vol_ratio', 0),
                "inst_streak": meta.get('inst_streak', 0), "for_streak": meta.get('for_streak', 0),
                "mktcap": meta.get('mktcap', 0), "silence_ratio": meta.get('silence_ratio', 0),
            }
        })

    # 히스토리 저장
    history_data = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            try: history_data = json.load(f)
            except: history_data = []
    history_data.append({"date": today_str, "label": label, "picks": final_picks})
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4)

    final_output = {
        "today_picks": final_picks, "scan_label": label,
        "total_candidates": log['final'], "total_screened": log['total'],
        "filter_log": log, "base_date": today_str,
    }
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4)

    print(f"\n🏁 완료! TOP {len(final_picks)}종목")
    return final_output


# ── 텔레그램 메시지 빌드 ───────────────────────────────────────


def _signal_label(v):
    if v >= 70:  return f"{v} 🔴 과매수"
    elif v >= 50: return f"{v} 🟡 상승중"
    elif v >= 30: return f"{v} 🟢 진입구간"
    else:         return f"{v} ⚪ 매집중"

def _spike_label(v):
    if v >= 10: return "🔥 극강 (엔켐형)"
    elif v >= 5: return "⚡ 강함"
    elif v >= 3: return "📈 중간"
    elif v >= 2: return "📊 기본"
    else:        return "😴 미발생"

def build_kr_message(kr_data: dict) -> str:
    picks     = kr_data.get("today_picks", [])
    log       = kr_data.get("filter_log", {})
    base_date = kr_data.get("base_date", "")
    label     = kr_data.get("scan_label", "스캔")

    lines = [
        f"🇰🇷 <b>국장 알파 — {ko_date(base_date)} {label}</b>",
        f"📋 {log.get('total',0):,}종목 스캔 → 후보 {log.get('final',0)}건",
        "━" * 24,
    ]

    if not picks:
        lines += ["⚠️ 오늘 조건 충족 종목 없음", "💡 신호 없는 날 쉬는 것도 전략입니다."]
    else:
        for p in picks:
            meta = p.get("meta", {})
            lines += [
                f"<b>#{p['rank']} {p['name']} ({p['code']})</b>",
                f"점수: {p.get('score','')}",
                f"현재가: {p.get('cur_price',0):,}원 | 시총: {meta.get('mktcap',0):,}억",
                f"수급: {p.get('supply','')}",
                f"저가반등: +{meta.get('rebound_pct',0)}% | 세력매집: {meta.get('green_days',0)}일",
                f"세력전환: {'✅ 전환완료' if meta.get('obv_cross') else '⏳ 매집중'} | 매매신호: {_signal_label(meta.get('signal',0))}",
                f"거래량: {_spike_label(meta.get('max_spike',0))} (최대 {meta.get('max_spike',0)}배) | 현재 {meta.get('vol_ratio',0)}배",
                f"기대수익: +{p.get('expected_return','')} (7일 목표)",
                "━" * 24,
            ]
    return "\n".join(lines)


# ── 실행 ───────────────────────────────────────────────────────

if __name__ == "__main__":
    kr_result = run_kr_scan()
    send_telegram(fetch_macro_summary())
    send_telegram(build_news_briefing())
    send_telegram(build_kr_message(kr_result))
