import os
import json
import datetime
import pandas as pd
from pykrx import stock
import FinanceDataReader as fdr

DATA_FILE     = 'stock_data.json'
HISTORY_FILE  = 'history.json'
MY_PICKS_FILE = 'my_picks.json'

# ──────────────────────────────────────────────────────────────
# 컨셉: 저평가 우량주 + 간헐적 거래량 신호 기반 상승 예상
#
# [기존] 세력 매집봉(300%↑) → 거래량 가뭄 → 급등 대기
# [변경] 저평가 종목에서 거래량 신호(150%↑)가 간헐적으로 발생 →
#        수급 뒷받침 → 완만하고 안정적인 상승 기대
# ──────────────────────────────────────────────────────────────


def get_financial_data(ticker):
    """
    현재: 테스트용 더미 (PER 10, PBR 0.8, OPM 5%, 부채비율 120%)
    실제 운영 시 OpenDART API로 교체:
    https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json
    """
    return {"PER": 10.0, "PBR": 0.8, "OPM": 5.0, "DebtRatio": 120.0}


def get_start_date(days_ago):
    return (datetime.datetime.now() - datetime.timedelta(days=int(days_ago * 1.5))).strftime("%Y%m%d")


def get_investor_df(ticker, start, end):
    """pykrx 버전별 컬럼명 자동 대응"""
    df = stock.get_market_trading_value_by_date(start, end, ticker)
    if df is None or df.empty:
        return None, None

    foreign_col, inst_col = None, None
    for col in df.columns:
        if '외국인' in col: foreign_col = col
        if '기관'   in col: inst_col    = col

    if not foreign_col or not inst_col:
        return None, None

    return df[[foreign_col, inst_col]].tail(5), (foreign_col, inst_col)


def analyze_with_manual_picks():
    now       = datetime.datetime.now()
    today_str = now.strftime("%Y%m%d")

    start_90d = get_start_date(90)
    start_7d  = get_start_date(7)

    print(f"📅 분석 기준일: {today_str}")

    kospi_tickers  = stock.get_market_ticker_list(today_str, market="KOSPI")
    kosdaq_tickers = stock.get_market_ticker_list(today_str, market="KOSDAQ")
    all_tickers    = kospi_tickers + kosdaq_tickers

    print(f"📋 전체 종목 수: {len(all_tickers)}개 (KOSPI {len(kospi_tickers)} + KOSDAQ {len(kosdaq_tickers)})")

    # all_tickers = all_tickers[:200]  # 빠른 테스트 시 주석 해제

    final_picks = []
    filter_log  = {
        "total":    len(all_tickers),
        "penny":    0,   # 동전주 컷 생존
        "trend":    0,   # 상승추세 (현재가 > 20일선 > 60일선)
        "vol_sig":  0,   # 거래량 신호 존재
        "momentum": 0,   # 최근 모멘텀 양호
        "supply":   0,   # 수급 충족
        "fin":      0,   # 재무 통과 (최종 픽)
    }

    print("🚀 [1] 종목 스크리닝 시작...")
    for ticker in all_tickers:
        try:
            df = fdr.DataReader(ticker, start_90d, today_str)
            if len(df) < 60:
                continue

            # 이동평균 계산
            df['MA20']     = df['Close'].rolling(window=20).mean()
            df['MA60']     = df['Close'].rolling(window=60).mean()
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()

            current_close = df['Close'].iloc[-1]
            current_vol   = df['Volume'].iloc[-1]
            ma20_now      = df['MA20'].iloc[-1]
            ma60_now      = df['MA60'].iloc[-1]

            # [방어막] 동전주 제외 (1,000원 미만)
            if current_close < 1000:
                continue
            filter_log['penny'] += 1

            # ── [필터 1] 이중 상승추세 ────────────────────────────────
            # 현재가 > 20일선 > 60일선: 단기·중기 모두 우상향
            if not (current_close > ma20_now > ma60_now):
                continue
            filter_log['trend'] += 1

            # ── [필터 2] 간헐적 거래량 신호 ──────────────────────────
            # 최근 60영업일 내 평균 거래량의 150% 이상인 날이 2회 이상
            # → 급등(세력)이 아닌 "관심이 들어온 흔적" 탐지
            recent = df.iloc[-60:].copy()
            vol_signal_days = (recent['Volume'] >= recent['Vol_MA20'] * 1.5).sum()
            if vol_signal_days < 2:
                continue
            filter_log['vol_sig'] += 1

            # 거래량 신호 강도 (점수용): 평균 배율 계산
            signal_rows    = recent[recent['Volume'] >= recent['Vol_MA20'] * 1.5]
            avg_vol_ratio  = (signal_rows['Volume'] / signal_rows['Vol_MA20']).mean()

            # ── [필터 3] 최근 모멘텀 ──────────────────────────────────
            # 20일 수익률 > 0% (상승 중인 종목만)
            price_20d_ago  = df['Close'].iloc[-20]
            momentum_20d   = (current_close / price_20d_ago - 1) * 100
            if momentum_20d <= 0:
                continue
            filter_log['momentum'] += 1

            # ── [필터 4] 수급 ─────────────────────────────────────────
            # 최근 5일 중 외인 or 기관 순매수 2일 이상
            inv_df, cols = get_investor_df(ticker, start_7d, today_str)
            if inv_df is None:
                continue

            foreign_col, inst_col = cols
            foreign_buy_days = int((inv_df[foreign_col] > 0).sum())
            inst_buy_days    = int((inv_df[inst_col]    > 0).sum())
            valid_buy_days   = int(((inv_df[foreign_col] > 0) | (inv_df[inst_col] > 0)).sum())

            if valid_buy_days < 2:
                continue
            filter_log['supply'] += 1

            # ── [필터 5] 재무 ─────────────────────────────────────────
            # PER < 20, PBR < 2.0, OPM > 0%, 부채비율 < 200%
            # (더미 데이터 → 실제 DART 연동 시 실효 발동)
            fin = get_financial_data(ticker)
            if (fin['PER']      >= 20.0 or
                fin['PBR']      >= 2.0  or
                fin['OPM']      <  0.0  or
                fin['DebtRatio'] >= 200.0):
                continue
            filter_log['fin'] += 1

            # ── 점수 산출 (100점 만점) ────────────────────────────────
            # 거래량 신호 강도 (최대 30점): 150%면 10점, 400% 이상이면 30점
            vol_score      = min(((avg_vol_ratio - 1.5) / 2.5) * 20 + 10, 30)

            # 최근 모멘텀 (최대 25점): 5% 상승이면 10점, 20% 이상이면 25점
            momentum_score = min(((momentum_20d - 0) / 20) * 15 + 10, 25)

            # 수급 강도 (최대 25점)
            supply_score   = 12 if valid_buy_days == 2 else 20 if valid_buy_days >= 3 else 25

            # 재무 점수 (최대 20점): PBR 낮을수록 저평가
            pbr_score      = min((2.0 - fin['PBR']) / 2.0 * 20, 20)

            total_score = int(vol_score + momentum_score + supply_score + pbr_score)
            total_score = min(total_score, 100)  # 100점 상한

            stars = "★" * (total_score // 20) + "☆" * (5 - total_score // 20)

            # 수급 텍스트
            if foreign_buy_days > 0 and inst_buy_days > 0:
                supply_text = "외인+기관"
            elif foreign_buy_days > 0:
                supply_text = "외인매수"
            else:
                supply_text = "기관매수"

            # 거래량 신호 횟수 태그
            vol_tag = f"거래량신호 {vol_signal_days}회"

            name         = stock.get_market_ticker_name(ticker)
            expected_ret = round(3.0 + (total_score / 100) * 12.0, 1)  # 3~15% 범위

            final_picks.append({
                "name":            name,
                "code":            ticker,
                "supply":          supply_text,
                "volume_growth":   f"+{int(avg_vol_ratio * 100)}% (평균)",
                "score":           f"{stars} ({total_score}점)",
                "tags":            f"저평가 / {vol_tag} / 모멘텀 {momentum_20d:.1f}%",
                "expected_return": f"{expected_ret}%"
            })

            print(f"  ✅ {name}({ticker}) | {stars} {total_score}점 | {supply_text} | 모멘텀 {momentum_20d:.1f}%")

        except Exception:
            continue

    # 필터 단계별 결과
    print("\n📊 [필터 단계별 생존 현황]")
    print(f"  전체 검사:       {filter_log['total']:>5}건")
    print(f"  동전주 컷:       {filter_log['penny']:>5}건 (생존)")
    print(f"  이중 상승추세:   {filter_log['trend']:>5}건")
    print(f"  거래량 신호 2회: {filter_log['vol_sig']:>5}건")
    print(f"  모멘텀 양호:     {filter_log['momentum']:>5}건")
    print(f"  수급 2일 이상:   {filter_log['supply']:>5}건")
    print(f"  재무 통과:       {filter_log['fin']:>5}건  ← 최종 픽")

    # ── 수동 픽 검증 ──────────────────────────────────────────────
    my_manual_report = []
    if os.path.exists(MY_PICKS_FILE):
        with open(MY_PICKS_FILE, 'r', encoding='utf-8') as f:
            try:    my_picks = json.load(f)
            except: my_picks = []

        for p in my_picks:
            try:
                curr_df = stock.get_market_ohlcv(today_str, today_str, p['code'])
                if not curr_df.empty:
                    curr_price = int(curr_df['종가'].iloc[-1])
                    profit     = round(((curr_price / p['buy_price']) - 1) * 100, 2)
                    my_manual_report.append({
                        "date":       p['date'],
                        "name":       p['name'],
                        "buy_price":  p['buy_price'],
                        "curr_price": curr_price,
                        "profit":     profit
                    })
            except Exception:
                continue

    # ── 백테스트 ──────────────────────────────────────────────────
    performance_results = {"win_rate": 0.0, "avg_return": 0.0, "total_cases": 0}
    history_data = []

    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            try:    history_data = json.load(f)
            except: history_data = []

        total_cases = win_cases = 0
        total_return = 0.0

        for record in history_data:
            past_date = datetime.datetime.strptime(record['date'], "%Y%m%d")
            if (now - past_date).days >= 14:   # 2주 후 수익률 체크 (완만한 상승 컨셉)
                for pick in record['picks']:
                    try:
                        df_p = fdr.DataReader(pick['code'], record['date'], today_str)
                        if len(df_p) >= 2:
                            entry = df_p['Close'].iloc[0]
                            exit_ = df_p['Close'].iloc[min(10, len(df_p) - 1)]  # 10영업일(2주) 후
                            ret   = ((exit_ / entry) - 1) * 100
                            total_return += ret
                            total_cases  += 1
                            if ret >= 5.0:   # 목표: 2주 내 +5% (급등 컨셉의 +10%에서 완화)
                                win_cases += 1
                    except Exception:
                        continue

        if total_cases > 0:
            performance_results = {
                "win_rate":    round((win_cases / total_cases) * 100, 1),
                "avg_return":  round(total_return / total_cases, 1),
                "total_cases": total_cases
            }

    # ── history 누적 저장 ──────────────────────────────────────────
    history_data.append({"date": today_str, "picks": final_picks})
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4)

    # ── stock_data.json 저장 ───────────────────────────────────────
    final_output = {
        "today_picks": sorted(
            final_picks,
            key=lambda x: int(x['score'].split('(')[1].replace('점)', '')),
            reverse=True
        ),
        "performance":      performance_results,
        "my_manual_report": my_manual_report
    }
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=4)

    print(f"\n✅ 분석 완료! 오늘의 픽: {len(final_picks)}건")
    return final_output


if __name__ == "__main__":
    analyze_with_manual_picks()
