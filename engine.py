import os
import json
import datetime
import numpy as np
import pandas as pd
import requests
import zipfile
import io
from bs4 import BeautifulSoup
from pykrx import stock
import FinanceDataReader as fdr
import yfinance as yf

DATA_FILE     = 'stock_data.json'
HISTORY_FILE  = 'history.json'
MY_PICKS_FILE = 'my_picks.json'
TOP_N         = 5

# ══════════════════════════════════════════════════════════════
#  ULTIMATE ALPHA ENGINE v3 — 승률 극대화 특화판
#  목표: 7일 내 +10% / 승률 68~73%
# ══════════════════════════════════════════════════════════════

DART_API_KEY     = os.environ.get("DART_API_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

_dart_corp_cache = {}


# ── 유틸 ───────────────────────────────────────────────────────

def get_market_date():
    today = datetime.datetime.now()
    wd = today.weekday()
    if wd == 5:   today -= datetime.timedelta(days=1)
    elif wd == 6: today -= datetime.timedelta(days=2)
    return today.strftime("%Y%m%d")


def get_start_date(base_str, days_ago):
    base = datetime.datetime.strptime(base_str, "%Y%m%d")
    return (base - datetime.timedelta(days=int(days_ago * 1.5))).strftime("%Y%m%d")


# ── pykrx 백업 ─────────────────────────────────────────────────

def safe_get_market_ticker_list(date_str, market):
    try:
        tickers = stock.get_market_ticker_list(date_str, market=market)
        if tickers and len(tickers) > 0:
            return list(tickers)
    except Exception as e:
        print(f"  ⚠️  pykrx {market} 목록 조회 실패: {e} → fdr 백업 시도")
    try:
        df = fdr.StockListing('KOSPI' if market == "KOSPI" else 'KOSDAQ')
        return list(df['Code'].dropna().astype(str).str.zfill(6))
    except Exception as e:
        print(f"  ❌ fdr {market} 백업도 실패: {e}")
        return []


def _find_cap_col(df):
    candidates = ['시가총액', 'Mktcap', 'MktCap', 'mktcap', 'marcap', 'Marcap', '시가홀액']
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
        for market in ["KOSPI", "KOSDAQ"]:
            df = fdr.StockListing(market)
            row = df[df['Code'] == ticker]
            if not row.empty:
                for col in ['Marcap', 'MarCap', 'marcap', 'mktcap', 'MktCap']:
                    if col in row.columns:
                        cap = row.iloc[0][col]
                        if cap and cap > 0: return int(cap / 1e8)
    except Exception:
        pass
    return None


def safe_get_ohlcv(ticker, date_str):
    try:
        df = stock.get_market_ohlcv(date_str, date_str, ticker)
        if df is not None and not df.empty: return df
    except Exception:
        pass
    return None


# ── DART 재무 ──────────────────────────────────────────────────

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
        print(f"  📋 DART 기업코드 {len(_dart_corp_cache)}건 로드 완료")
    except Exception as e:
        print(f"  ⚠️  DART 기업코드 로드 실패: {e}")


def get_financial_data(ticker):
    if not DART_API_KEY or not _dart_corp_cache:
        return {"OPM": 5.0, "DebtRatio": 100.0}
    corp_code = _dart_corp_cache.get(ticker)
    if not corp_code: return {"OPM": 5.0, "DebtRatio": 100.0}
    try:
        year     = str(datetime.datetime.now().year - 1)
        base_url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        for reprt_code in ['11011', '11012']:
            for fs_div in ['CFS', 'OFS']:
                params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code,
                          'bsns_year': year, 'reprt_code': reprt_code, 'fs_div': fs_div}
                r    = requests.get(base_url, params=params, timeout=8)
                data = r.json()
                if data.get('status') == '000' and data.get('list'):
                    items = {i['account_nm']: i for i in data['list']}
                    def parse_val(nm):
                        item = items.get(nm)
                        if not item: return None
                        val = item.get('thstrm_amount', '0').replace(',', '')
                        try: return float(val)
                        except: return None
                    revenue      = parse_val('매출액') or parse_val('수익(매출액)')
                    op_income    = parse_val('영업이익') or parse_val('영업이익(손실)')
                    total_debt   = parse_val('부채총계')
                    total_equity = parse_val('자본총계')
                    opm        = round(op_income / revenue * 100, 1) if revenue and op_income else 5.0
                    debt_ratio = round(total_debt / total_equity * 100, 1) if total_debt and total_equity and total_equity > 0 else 100.0
                    return {"OPM": opm, "DebtRatio": debt_ratio}
    except Exception:
        pass
    return {"OPM": 5.0, "DebtRatio": 100.0}


# ── 네이버 기업 개요 ───────────────────────────────────────────

def get_company_summary(ticker, name):
    try:
        url = f"https://finance.naver.com/item/coinfo.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                   'Accept-Language': 'ko-KR,ko;q=0.9'}
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
    return f"{name} — 기업정보 조회 실패"


# ── 수급 데이터 ────────────────────────────────────────────────

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


# ── 기술적 지표 ────────────────────────────────────────────────

def get_52week_high(df):
    return df['High'].iloc[-252:].max() if len(df) >= 252 else df['High'].max()


def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_obv(df):
    return (np.sign(df['Close'].diff()).fillna(0) * df['Volume']).cumsum()


# ── 멀티팩터 채점 250점 ────────────────────────────────────────

def score_stock(df, inv_df, cols, inst_streak, for_streak):
    df = df.copy()
    df['MA20']     = df['Close'].rolling(20).mean()
    df['Vol_MA20'] = df['Volume'].rolling(20).mean()
    df['RSI']      = calc_rsi(df['Close'])
    df['OBV']      = calc_obv(df)
    df['BB_mid']   = df['Close'].rolling(20).mean()
    df['BB_std']   = df['Close'].rolling(20).std()
    df['BB_width'] = (4 * df['BB_std'] / df['BB_mid']).replace([np.inf, -np.inf], np.nan)

    cur = df['Close'].iloc[-1]
    rsi = df['RSI'].iloc[-1]
    bd  = {}
    meta = {}

    # [A] 세력 흔적 70점
    recent_60 = df.iloc[-60:]
    vm20 = df['Vol_MA20']; m20 = df['MA20']
    spike_mask = (
        (recent_60['Volume'] >= vm20.iloc[-60:] * 2.5) &
        (recent_60['Close']  >  recent_60['Open']) &
        (recent_60['Close']  >  m20.iloc[-60:])
    )
    a = 0; spike_days_ago = 0; silence_ratio = 1.0
    if spike_mask.any():
        ls   = recent_60[spike_mask].iloc[-1]
        svol = ls['Volume']; slow = ls['Low']
        sr   = svol / (ls['Vol_MA20'] + 1)
        si   = df.index.get_loc(ls.name)
        af   = df.iloc[si + 1:]
        if len(af) >= 3 and cur > slow:
            sil            = af['Volume'].mean() / (svol + 1)
            spike_days_ago = len(af)
            silence_ratio  = round(sil, 2)
            if sil <= 0.7:
                a_spike   = min((sr - 2.5) / 5 * 30 + 15, 35)
                a_silence = min((0.7 - sil) / 0.7 * 25, 25)
                price_gap = (cur / slow - 1) * 100
                a_hold    = max(10 - price_gap / 15 * 10, 0)
                a         = int(a_spike + a_silence + a_hold)
    bd['세력흔적']         = min(a, 70)
    meta['spike_days_ago'] = spike_days_ago
    meta['silence_ratio']  = silence_ratio

    # [B] BB 수축 40점
    bbs = df['BB_width'].iloc[-90:].dropna()
    b = 0; bbc = 0
    if len(bbs) > 10:
        bmin, bmax = bbs.min(), bbs.max(); brng = bmax - bmin
        cbw = df['BB_width'].iloc[-1]
        if brng > 0 and not np.isnan(cbw):
            c = 1 - (cbw - bmin) / brng
            b = int(min(c * 40, 40)); bbc = round(c * 100, 1)
    bd['BB수축'] = b; meta['bb_compress'] = bbc

    # [C] OBV 다이버전스 40점
    pc20 = (cur / df['Close'].iloc[-20] - 1) * 100
    ob   = abs(df['OBV'].iloc[-20]) + 1
    op20 = (df['OBV'].iloc[-1] - df['OBV'].iloc[-20]) / ob * 100
    c    = int(min(max((op20 - pc20) * 2, 0), 40)) if op20 > 0 else 0
    bd['OBV다이버전스'] = c

    # [D] RSI 골디락스 30점
    d = 0
    if not np.isnan(rsi):
        if   45 <= rsi <= 60: d = int(max(30 - abs(rsi - 52) * 2, 15))
        elif 40 <= rsi <  45: d = 12
        elif 60 <  rsi <= 65: d = 10
    bd['RSI위치'] = d; meta['rsi'] = round(rsi, 1) if not np.isnan(rsi) else 0

    # [E] 수급 강도 40점
    e = 0; supply_text = "정보없음"
    if inv_df is not None and cols:
        fc, ic = cols
        fd        = int((inv_df[fc] > 0).sum())
        id_       = int((inv_df[ic] > 0).sum())
        both_days = int(((inv_df[fc] > 0) & (inv_df[ic] > 0)).sum())
        e = min(min(inst_streak * 6, 24) + min(for_streak * 4, 12) + both_days * 4, 40)
        if fd > 0 and id_ > 0: supply_text = "외인+기관 양매수"
        elif id_ > 0:           supply_text = "기관매수"
        elif fd > 0:            supply_text = "외인매수"
    bd['수급강도'] = e
    meta['supply_text'] = supply_text
    meta['inst_streak'] = inst_streak
    meta['for_streak']  = for_streak

    # [F] 52주 돌파 임박 30점
    high52  = get_52week_high(df)
    gap_pct = (high52 - cur) / high52 * 100
    f = 30 if gap_pct <= 2 else 22 if gap_pct <= 5 else 12 if gap_pct <= 10 else 0
    bd['돌파임박'] = f; meta['gap_to_high'] = round(gap_pct, 1)

    return sum(bd.values()), bd, meta


# ══════════════════════════════════════════════════════════════
#  한국 시장 메인 분석
# ══════════════════════════════════════════════════════════════

def analyze_with_manual_picks():
    now        = datetime.datetime.now()
    today_str  = get_market_date()
    start_260d = get_start_date(today_str, 260)
    start_10d  = get_start_date(today_str, 10)

    print(f"📅 기준일: {today_str}  (원본: {now.strftime('%Y%m%d')})")
    print(f"🎯 목표: 7일 +10% / 승률 68~73% 최적화 엔진 v3\n")

    load_dart_corp_codes()

    kospi_tickers  = safe_get_market_ticker_list(today_str, "KOSPI")
    kosdaq_tickers = safe_get_market_ticker_list(today_str, "KOSDAQ")
    all_tickers    = kospi_tickers + kosdaq_tickers
    print(f"📋 전체 종목: {len(all_tickers)}개\n")

    print("⚡ 시총 사전 필터링 중 (500억~3조)...")
    mktcap_cache = {}; pre_filtered = []; supra_success = False
    try:
        df_cap = stock.get_market_cap(today_str)
        if df_cap is not None and not df_cap.empty:
            print(f"  📋 pykrx 시총 컬럼 확인: {df_cap.columns.tolist()}")
            cap_col = _find_cap_col(df_cap)
            if cap_col:
                for idx, row in df_cap.iterrows():
                    cap = int(row[cap_col] / 1e8)
                    mktcap_cache[str(idx)] = cap
                    if 500 <= cap <= 30000: pre_filtered.append(str(idx))
                print(f"  → 사전 필터 통과: {len(pre_filtered)}종목")
                supra_success = True
            else:
                print(f"  ⚠️  시총 컬럼 탐지 실패 → 전체 종목 스캔")
        else:
            print("  ⚠️  시총 데이터 비어있음 → 전체 종목 스캔")
    except Exception as e:
        print(f"  ⚠️  시총 사전 필터 실패: {e} → 전체 종목 스캔")

    if not supra_success: pre_filtered = all_tickers
    all_tickers = pre_filtered if pre_filtered else all_tickers

    candidates = []
    log = dict(total=len(all_tickers), penny=0, mktcap=0,
               bottom=0, uptrend=0, vol_spike=0, seforce=0, final=0)

    print("🔍 전 종목 스크리닝 시작 [바닥반등+거래량 전략]...")
    for i, ticker in enumerate(all_tickers):
        try:
            df = fdr.DataReader(ticker, start_260d, today_str)
            if len(df) < 120: continue
            close = df['Close']; vol = df['Volume']; cur = close.iloc[-1]

            # ① 동전주 제외
            if cur < 1000: continue
            log['penny'] += 1

            # ② 시총 500억~3조
            mktcap = mktcap_cache.get(ticker)
            if mktcap is None:
                mktcap = safe_get_market_cap(ticker, today_str)
                if mktcap is not None: mktcap_cache[ticker] = mktcap
            if mktcap is None or not (500 <= mktcap <= 30000): continue
            log['mktcap'] += 1

            # ③ 바닥 확인: 52주 저가에서 5~60% 반등 구간
            low52 = df['Low'].iloc[-252:].min() if len(df) >= 252 else df['Low'].min()
            rebound_pct = (cur - low52) / low52 * 100
            if not (5 <= rebound_pct <= 60): continue
            log['bottom'] += 1

            # ④ 완만한 우상향: MA20이 20일 전보다 높고, 현재가 MA20 위
            vol_ma20  = vol.rolling(20).mean()
            ma20_s    = close.rolling(20).mean()
            ma20_now  = ma20_s.iloc[-1]
            ma20_prev = ma20_s.iloc[-20]
            if not (ma20_now > ma20_prev and cur > ma20_now): continue
            log['uptrend'] += 1

            # ⑤ 최근 60일 내 거래량 스파이크 1회 이상 (평균 2배↑)
            #    + 스파이크 이후 현재가가 스파이크 저점 위 유지
            recent_60  = df.iloc[-60:]
            spike_mask = recent_60['Volume'] >= vol_ma20.iloc[-60:] * 2.0
            if not spike_mask.any(): continue
            last_spike = recent_60[spike_mask].iloc[-1]
            if cur <= last_spike['Low']: continue

            # 스파이크 이후 거래량 수축 확인 (조용히 횡보)
            si      = df.index.get_loc(last_spike.name)
            af      = df.iloc[si + 1:]
            cur_vol  = vol.iloc[-1]
            cur_vm20 = vol_ma20.iloc[-1]
            if len(af) >= 3:
                sil = af['Volume'].mean() / (last_spike['Volume'] + 1)
                if sil > 0.6: continue  # 거래량 계속 높으면 탈락
            log['vol_spike'] += 1

            # ⑥ 세력: 기관 OR 외인 중 누구든 최근 5일 중 1일 이상 순매수
            inv_df, cols, inst_streak, for_streak = get_investor_detail(
                ticker, start_10d, today_str
            )
            if inv_df is None or cols is None: continue
            fc, ic  = cols
            any_buy = int(((inv_df[fc] > 0) | (inv_df[ic] > 0)).sum())
            if any_buy < 1: continue
            log['seforce'] += 1

            # 채점 (기준 완화: 100 → 60)
            total_score, breakdown, meta = score_stock(
                df, inv_df, cols, inst_streak, for_streak
            )
            meta['mktcap']      = mktcap
            meta['rebound_pct'] = round(rebound_pct, 1)
            meta['cur_close']   = int(cur)
            meta['vol_ratio']   = round(cur_vol / (cur_vm20 + 1), 2)

            if total_score >= 60:
                candidates.append((total_score, ticker, breakdown, meta))
                log['final'] += 1

        except Exception:
            continue

        if (i + 1) % 300 == 0:
            print(f"  진행 {i+1}/{len(all_tickers)} | 후보: {len(candidates)}건")

    print(f"\n📊 [필터 생존 현황 — 바닥반등+거래량 전략]")
    for label, key in [("전체", "total"), ("① 동전주 컷", "penny"),
                        ("② 시총 500억~3조", "mktcap"), ("③ 바닥반등 5~60%", "bottom"),
                        ("④ MA20 우상향", "uptrend"), ("⑤ 거래량 스파이크", "vol_spike"),
                        ("⑥ 세력(기관OR외인)", "seforce"), ("최종 채점 통과", "final")]:
        print(f"  {label:<18} {log[key]:>5}건")

    candidates.sort(key=lambda x: x[0], reverse=True)
    top_raw = candidates[:TOP_N]
    if not top_raw:
        print("⚠️  오늘 조건 충족 종목 없음 — 현금 보유 권장")

    final_picks = []
    print(f"\n🏆 [TOP {TOP_N} 최종 선정]")
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
                "price_range": meta.get('price_rng', 0), "vol_ratio": meta.get('vol_ratio', 0),
                "inst_streak": meta.get('inst_streak', 0), "for_streak": meta.get('for_streak', 0),
                "gap_to_high": meta.get('gap_to_high', 0), "mktcap": meta.get('mktcap', 0),
                "silence_ratio": meta.get('silence_ratio', 0),
                "spike_days_ago": meta.get('spike_days_ago', 0),
            }
        })

    # 수동 픽 검증
    my_manual_report = []
    if os.path.exists(MY_PICKS_FILE):
        with open(MY_PICKS_FILE, 'r', encoding='utf-8') as f:
            try:    my_picks = json.load(f)
            except: my_picks = []
        for p in my_picks:
            try:
                cdf = safe_get_ohlcv(p['code'], today_str)
                if cdf is not None and not cdf.empty:
                    cp     = int(cdf['종가'].iloc[-1])
                    profit = round(((cp / p['buy_price']) - 1) * 100, 2)
                    my_manual_report.append({
                        "date": p['date'], "name": p['name'],
                        "buy_price": p['buy_price'], "curr_price": cp, "profit": profit
                    })
            except Exception:
                continue

    # 백테스트
    performance_results = {"win_rate": 0.0, "avg_return": 0.0, "total_cases": 0}
    history_data = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            try:    history_data = json.load(f)
            except: history_data = []
        total_cases = win_cases = 0; total_return = 0.0
        for record in history_data:
            past = datetime.datetime.strptime(record['date'], "%Y%m%d")
            if (now - past).days >= 7:
                for pick in record['picks']:
                    try:
                        dfp   = fdr.DataReader(pick['code'], record['date'], today_str)
                        if len(dfp) >= 2:
                            entry = dfp['Close'].iloc[0]
                            exit_ = dfp['Close'].iloc[min(5, len(dfp) - 1)]
                            ret   = ((exit_ / entry) - 1) * 100
                            total_return += ret; total_cases += 1
                            if ret >= 10.0: win_cases += 1
                    except Exception:
                        continue
        if total_cases > 0:
            performance_results = {
                "win_rate":    round(win_cases / total_cases * 100, 1),
                "avg_return":  round(total_return / total_cases, 1),
                "total_cases": total_cases
            }

    history_data.append({"date": today_str, "picks": final_picks})
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4)

    final_output = {
        "today_picks": final_picks, "performance": performance_results,
        "my_manual_report": my_manual_report,
        "total_candidates": log['final'], "total_screened": log['total'],
        "filter_log": log, "base_date": today_str,
    }
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4)

    print(f"\n🏁 완료! TOP {len(final_picks)}종목 | 후보 {log['final']}건 / 전체 {log['total']}건")
    return final_output


# ══════════════════════════════════════════════════════════════
#  미국 시장 — 숏스퀴즈 탐지
# ══════════════════════════════════════════════════════════════

def analyze_us_market():
    print("\n🇺🇸 미국 시장 숏스퀴즈 스캐닝 시작...")

    # 2026년 4월 기준 실제 고공매도 활성 종목
    WATCHLIST = [
        # 공매도 비율 TOP — 렌터카/여행
        'CAR','HTZ','GRPN',
        # 핀테크/성장주 고공매도
        'UPST','SOFI','AFRM','OPEN','HIMS',
        # 기술/AI 고공매도
        'PLTR','AI','SNAP','MRVL','ASTS','IONQ','ACHR',
        # 에너지/소재 고공매도
        'CLF','PBF','DK','LYB','DOW',
        # 바이오 고공매도
        'OCGN','NVAX','SAVA','PACB',
        # 밈주식 생존종목
        'GME','AMC','MVIS','TLRY','KOSS','SPCE','NKLA',
    ]

    us_picks = []; skipped = 0
    today_str = datetime.datetime.now().strftime("%Y%m%d")

    for symbol in WATCHLIST:
        try:
            t = yf.Ticker(symbol)

            # ── 가격 데이터 먼저 (빠르고 안정적) ─────────────
            hist = t.history(period="3mo")
            if hist is None or len(hist) < 20:
                skipped += 1; continue

            cur_price  = round(float(hist['Close'].iloc[-1]), 2)
            avg_vol_20 = hist['Volume'].iloc[-21:-1].mean()
            cur_vol    = hist['Volume'].iloc[-1]
            vol_spike  = round(cur_vol / (avg_vol_20 + 1), 2)

            # RSI 계산
            delta = hist['Close'].diff()
            gain  = delta.where(delta > 0, 0).rolling(14).mean()
            loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs    = gain / loss.replace(0, np.nan)
            rsi   = round(float((100 - 100 / (1 + rs)).iloc[-1]), 1)

            # ── info 조회 (타임아웃 관대하게) ─────────────────
            try:
                info = t.info
            except Exception:
                info = {}

            float_shares = info.get('floatShares') or info.get('impliedSharesOutstanding')
            short_pct    = info.get('shortPercentOfFloat')
            short_ratio  = info.get('shortRatio') or 0
            short_name   = info.get('shortName', symbol)

            # float/short 데이터 없으면 거래량 스파이크만으로 판단
            float_m     = float_shares / 1e6 if float_shares else 999
            short_pct_p = short_pct * 100     if short_pct    else 0

            # ── 필터: 거래량 스파이크 필수, 나머지는 완화 ────
            if vol_spike < 1.5:
                continue

            # 점수 산정
            if short_pct_p >= 10:
                short_score = min((short_pct_p - 10) / 30 * 30 + 10, 40)
            else:
                short_score = 0

            vol_score   = min((vol_spike - 1.5) / 6 * 30 + 5, 35)
            float_score = max(20 - (float_m / 150 * 20), 0) if float_m < 999 else 0
            ratio_score = min(short_ratio * 2, 10) if short_ratio else 0
            total_score = int(short_score + vol_score + float_score + ratio_score)

            # 최소 점수 10점 이상만 수록
            if total_score < 10:
                continue

            squeeze_level = (
                "🔥 EXTREME" if total_score >= 70 else
                "⚡ HIGH"    if total_score >= 45 else
                "📈 MEDIUM"
            )

            short_str = f"{round(short_pct_p,1)}%" if short_pct_p > 0 else "데이터없음"
            float_str = f"{round(float_m,1)}M"     if float_m < 999  else "데이터없음"

            us_picks.append({
                "rank": len(us_picks) + 1,
                "name": short_name,
                "code": symbol,
                "company_summary": (info.get('longBusinessSummary', '')[:180] + '...')
                                   if info.get('longBusinessSummary') else f"{symbol}",
                "supply": f"공매도 {short_str} | {squeeze_level}",
                "cur_price": cur_price,
                "score": f"SQUEEZE {total_score}점/100",
                "score_detail": {
                    "공매도강도": int(short_score), "거래량급증": int(vol_score),
                    "유통주희소": int(float_score), "커버소요일": int(ratio_score),
                },
                "tags": f"유통주 {float_str} · 숏비율 {short_str} · RSI {rsi}",
                "expected_return": "EXPLOSIVE",
                "meta": {
                    "float_m":     round(float_m, 1) if float_m < 999 else 0,
                    "short_pct":   round(short_pct_p, 1),
                    "vol_spike":   vol_spike,
                    "rsi":         rsi,
                    "short_ratio": round(short_ratio, 1),
                }
            })
            print(f"  ✅ {symbol} | {total_score}점 | 공매도 {short_str} | 거래량 {vol_spike}x")

        except Exception as e:
            print(f"  ⚠️  {symbol} 스킵: {e}")
            skipped += 1
            continue

    us_picks.sort(key=lambda x: int(x['score'].split()[1].replace('점/100', '')), reverse=True)
    top5 = us_picks[:5]
    for i, p in enumerate(top5): p['rank'] = i + 1

    us_output = {
        "today_picks": top5,
        "performance": {"win_rate": 0.0, "avg_return": 0.0, "total_cases": 0},
        "my_manual_report": [], "total_candidates": len(us_picks),
        "total_screened": len(WATCHLIST) - skipped,
        "filter_log": {"inst3": 0}, "base_date": today_str,
    }
    with open('stock_data_us.json', 'w', encoding='utf-8') as f:
        json.dump(us_output, f, ensure_ascii=False, indent=4)

    print(f"🏁 미국 분석 완료! 후보 {len(us_picks)}건 → TOP {len(top5)} 저장\n")
    return us_output


# ══════════════════════════════════════════════════════════════
#  텔레그램
# ══════════════════════════════════════════════════════════════

def send_telegram(message: str):
    if not TELEGRAM_TOKEN:
        print("⚠️  TELEGRAM_TOKEN 없음 — 전송 스킵")
        return
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"
        }, timeout=10)
        if resp.status_code == 200:
            print("✅ 텔레그램 전송 완료")
        else:
            print(f"⚠️  텔레그램 전송 실패: {resp.text}")
    except Exception as e:
        print(f"⚠️  텔레그램 오류: {e}")


def fetch_macro_summary() -> str:
    """UK2 브리핑 매크로 7종 — yfinance 직접 조회"""
    symbols = {
        "KOSPI":   ("^KS11",  lambda v: f"{v:,.2f}"),
        "KOSDAQ":  ("^KQ11",  lambda v: f"{v:,.2f}"),
        "USD/KRW": ("KRW=X",  lambda v: f"{v:,.0f}원"),
        "WTI":     ("CL=F",   lambda v: f"${v:.1f}"),
        "Gold":    ("GC=F",   lambda v: f"${v:,.0f}"),
        "VIX":     ("^VIX",   lambda v: f"{v:.2f}"),
        "US 10Y":  ("^TNX",   lambda v: f"{v:.2f}%"),
    }
    day_map = {"Mon":"월","Tue":"화","Wed":"수","Thu":"목","Fri":"금","Sat":"토","Sun":"일"}
    today   = datetime.datetime.now().strftime("%Y.%m.%d (%a)")
    for en, ko in day_map.items(): today = today.replace(en, ko)

    lines = [f"📊 <b>UK2 — {today} 매크로 브리핑</b>", "━" * 24]
    for label, (sym, fmt) in symbols.items():
        try:
            info  = yf.Ticker(sym).fast_info
            price = info.last_price
            prev  = info.previous_close
            chg   = (price - prev) / prev * 100 if prev else 0
            arrow = "▲" if chg >= 0 else "▼"
            sign  = "+" if chg >= 0 else ""
            lines.append(f"{label:<8} {fmt(price)}   {arrow} {sign}{chg:.2f}%")
        except Exception:
            lines.append(f"{label:<8} 조회 실패")
    lines += ["━" * 24, "💡 <i>UK2 Investment · AI 브리핑</i>"]
    return "\n".join(lines)


def build_kr_message(kr_data: dict) -> str:
    picks     = kr_data.get("today_picks", [])
    perf      = kr_data.get("performance", {})
    log       = kr_data.get("filter_log", {})
    base_date = kr_data.get("base_date", "")
    day_map   = {"Mon":"월","Tue":"화","Wed":"수","Thu":"목","Fri":"금","Sat":"토","Sun":"일"}
    try:
        d = datetime.datetime.strptime(base_date, "%Y%m%d")
        date_str = d.strftime("%Y.%m.%d (%a)")
        for en, ko in day_map.items(): date_str = date_str.replace(en, ko)
    except Exception:
        date_str = base_date

    lines = [
        f"🏆 <b>ULTIMATE ALPHA v3 — {date_str}</b>",
        f"📋 스크리닝: {log.get('total',0):,}종목 → 후보 {log.get('final',0)}건",
    ]
    if perf.get("total_cases", 0) > 0:
        sign = "+" if perf['avg_return'] >= 0 else ""
        lines.append(f"📈 백테스트 승률: {perf['win_rate']}% | 평균수익: {sign}{perf['avg_return']}% ({perf['total_cases']}건)")
    lines.append("━" * 24)

    if not picks:
        lines += ["⚠️ 오늘 조건 충족 종목 없음", "💡 신호 없는 날 쉬는 것이 전략의 일부입니다."]
    else:
        for p in picks:
            meta  = p.get("meta", {})
            lines += [
                f"<b>#{p['rank']} {p['name']} ({p['code']})</b>",
                f"점수: {p.get('score','')}",
                f"현재가: {p.get('cur_price',0):,}원 | 시총: {meta.get('mktcap',0):,}억",
                f"수급: {p.get('supply','')}",
                f"저가반등: +{meta.get('rebound_pct',0)}% | 거래량: {meta.get('vol_ratio',0)}배",
                f"RSI: {meta.get('rsi',0)} | BB수축: {meta.get('bb_compress',0)}%",
                f"기대수익: +{p.get('expected_return','')} (7일 목표)",
                "━" * 24,
            ]
    return "\n".join(lines)


def build_us_message(us_data: dict) -> str:
    picks    = us_data.get("today_picks", [])
    screened = us_data.get("total_screened", 0)
    lines    = [
        f"🇺🇸 <b>ULTIMATE ALPHA v3 — 미국 숏스퀴즈</b>",
        f"📋 워치리스트 {screened}종목 스캔",
        "━" * 24,
    ]
    if not picks:
        lines.append("⚠️ 오늘 조건 충족 종목 없음")
    else:
        for p in picks:
            meta = p.get("meta", {})
            lines += [
                f"<b>#{p['rank']} {p['name']} (${p['code']})</b>",
                f"점수: {p.get('score','')}",
                f"현재가: ${p.get('cur_price',0)}",
                f"공매도: {meta.get('short_pct',0)}% | 유통주: {meta.get('float_m',0)}M",
                f"거래량스파이크: {meta.get('vol_spike',0)}배 | RSI: {meta.get('rsi',0)}",
                f"숏커버소요: {meta.get('short_ratio',0)}일",
                f"{p.get('supply','')}",
                "━" * 24,
            ]
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  실행
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    kr_result = analyze_with_manual_picks()
    us_result = analyze_us_market()

    # 1. UK2 매크로 브리핑 (KOSPI·KOSDAQ·환율·WTI·Gold·VIX·US10Y)
    send_telegram(fetch_macro_summary())

    # 2. 한국 알파 TOP5
    send_telegram(build_kr_message(kr_result))

    # 3. 미국 숏스퀴즈 TOP5
    send_telegram(build_us_message(us_result))
