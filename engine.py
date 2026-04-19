import os
import json
import datetime
import pandas as pd
from pykrx import stock
import FinanceDataReader as fdr

DATA_FILE    = 'stock_data.json'
HISTORY_FILE = 'history.json'
MY_PICKS_FILE = 'my_picks.json'


def get_financial_data(ticker):
    """
    재무 데이터 - 현재는 더미(OPM 15%, 부채비율 100%)
    실제 운영 시 OpenDART API로 교체:
    https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json
    """
    return {"OPM": 15.0, "DebtRatio": 100.0}


def get_start_date(days_ago):
    """달력 기준 N일 전 날짜 (주말 여유 포함)"""
    return (datetime.datetime.now() - datetime.timedelta(days=int(days_ago * 1.5))).strftime("%Y%m%d")


def get_investor_df(ticker, start, end):
    """
    pykrx 수급 데이터 조회 + 컬럼명 자동 대응
    pykrx 버전마다 '외국인합계'/'기관합계' 또는 '외국인'/'기관'으로 다를 수 있어 양쪽 처리
    """
    df = stock.get_market_trading_value_by_date(start, end, ticker)
    if df is None or df.empty:
        return None, None

    foreign_col = None
    inst_col    = None
    for col in df.columns:
        if '외국인' in col: foreign_col = col
        if '기관'   in col: inst_col    = col

    if not foreign_col or not inst_col:
        return None, None

    return df[[foreign_col, inst_col]].tail(3), (foreign_col, inst_col)


def analyze_with_manual_picks():
    now       = datetime.datetime.now()
    today_str = now.strftime("%Y%m%d")

    start_90d = get_start_date(90)
    start_5d  = get_start_date(5)

    print(f"📅 분석 기준일: {today_str}")

    kospi_tickers  = stock.get_market_ticker_list(today_str, market="KOSPI")
    kosdaq_tickers = stock.get_market_ticker_list(today_str, market="KOSDAQ")
    all_tickers    = kospi_tickers + kosdaq_tickers

    print(f"📋 전체 종목 수: {len(all_tickers)}개 (KOSPI {len(kospi_tickers)} + KOSDAQ {len(kosdaq_tickers)})")

    # 빠른 테스트 원하면 아래 주석 해제
    # all_tickers = all_tickers[:200]

    final_picks = []
    filter_log  = {"total": len(all_tickers), "ma20": 0, "accum": 0, "drought": 0, "supply": 0, "fin": 0}

    print("🚀 [1] 종목 필터링 시작...")
    for ticker in all_tickers:
        try:
            df = fdr.DataReader(ticker, start_90d, today_str)
            if len(df) < 25:
                continue

            df['MA20']     = df['Close'].rolling(window=20).mean()
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()

            current_close = df['Close'].iloc[-1]
            current_vol   = df['Volume'].iloc[-1]
            ma20_now      = df['MA20'].iloc[-1]

            # [필터 1] 현재가 > 20일선
            if current_close <= ma20_now:
                continue
            filter_log['ma20'] += 1

            # [필터 2] 최근 60영업일 내 매집봉 (200% 이상, 양봉, 20일선 위)
            recent = df.iloc[-60:].copy()
            recent['Is_Accum'] = (
                (recent['Volume'] >= recent['Vol_MA20'] * 2) &
                (recent['Close']  >  recent['Open']) &
                (recent['Close']  >  recent['MA20'])
            )
            accum_days = recent[recent['Is_Accum']]
            if accum_days.empty:
                continue
            filter_log['accum'] += 1

            last_accum      = accum_days.iloc[-1]
            accum_vol       = last_accum['Volume']
            accum_vol_ratio = accum_vol / last_accum['Vol_MA20']

            # [필터 3] 거래량 가뭄 (현재 거래량 ≤ 매집봉의 60%)
            if current_vol > accum_vol * 0.6:
                continue
            vol_drop_ratio = current_vol / accum_vol
            filter_log['drought'] += 1

            # [필터 4] 수급: 최근 3일 중 1일 이상 외인/기관 순매수
            inv_df, cols = get_investor_df(ticker, start_5d, today_str)
            if inv_df is None:
                continue

            foreign_col, inst_col = cols
            foreign_buy_days = int((inv_df[foreign_col] > 0).sum())
            inst_buy_days    = int((inv_df[inst_col]    > 0).sum())
            valid_buy_days   = int(((inv_df[foreign_col] > 0) | (inv_df[inst_col] > 0)).sum())

            if valid_buy_days < 1:
                continue
            filter_log['supply'] += 1

            # [필터 5] 재무 (OPM > 5%, 부채비율 < 200%)
            fin = get_financial_data(ticker)
            if fin['OPM'] <= 5.0 or fin['DebtRatio'] >= 200.0:
                continue
            filter_log['fin'] += 1

            # ── 점수 산출 (100점 만점) ────────────────────────────
            vol_score  = min(((accum_vol_ratio - 2) / 8) * 20 + 10, 30)
            dry_score  = min((0.6 - vol_drop_ratio) / 0.4 * 15 + 5, 20)
            buy_score  = 10 if valid_buy_days == 1 else 20
            opm_score  = min((fin['OPM']        -   5) / 20 * 15, 15)
            debt_score = min((200 - fin['DebtRatio']) / 100 * 15, 15)
            total_score = int(vol_score + dry_score + buy_score + opm_score + debt_score)

            stars = "★" * (total_score // 20) + "☆" * (5 - total_score // 20)

            if foreign_buy_days > 0 and inst_buy_days > 0:
                supply_text = "외인+기관 양매수"
            elif foreign_buy_days >= inst_buy_days:
                supply_text = "외인매수"
            else:
                supply_text = "기관매수"

            name         = stock.get_market_ticker_name(ticker)
            expected_ret = round(5.0 + (total_score / 100) * 8.0, 1)

            final_picks.append({
                "name":            name,
                "code":            ticker,
                "supply":          supply_text,
                "volume_growth":   f"+{int(accum_vol_ratio * 100)}%",
                "score":           f"{stars} ({total_score}점)",
                "tags":            "매집/가뭄/수급 충족",
                "expected_return": f"{expected_ret}%"
            })

            print(f"  ✅ {name}({ticker}) | {stars} {total_score}점 | {supply_text}")

        except Exception:
            continue

    # 필터 단계별 결과 출력
    print("\n📊 [필터 단계별 통과 현황]")
    print(f"  전체:        {filter_log['total']:>5}건")
    print(f"  20일선 위:   {filter_log['ma20']:>5}건")
    print(f"  매집봉 확인: {filter_log['accum']:>5}건")
    print(f"  거래량 가뭄: {filter_log['drought']:>5}건")
    print(f"  수급 충족:   {filter_log['supply']:>5}건")
    print(f"  재무 통과:   {filter_log['fin']:>5}건  ← 최종 픽")

    # ── 수동 픽 검증 ──────────────────────────────────────────────
    my_manual_report = []
    if os.path.exists(MY_PICKS_FILE):
        with open(MY_PICKS_FILE, 'r', encoding='utf-8') as f:
            try:    my_picks = json.load(f)
            except: my_picks = []

        print("\n📒 [수동 픽 성적 계산 중...]")
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
                    sign = "+" if profit >= 0 else ""
                    print(f"  {p['name']}: {sign}{profit}%")
            except Exception:
                continue

    # ── 백테스트 ──────────────────────────────────────────────────
    performance_results = {"win_rate": 0.0, "avg_return": 0.0, "total_cases": 0}
    history_data = []

    if os.path.exists(HISTORY_FILE):
        print("\n🔍 [백테스트 계산 중...]")
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            try:    history_data = json.load(f)
            except: history_data = []

        total_cases = win_cases = 0
        total_return = 0.0

        for record in history_data:
            past_date = datetime.datetime.strptime(record['date'], "%Y%m%d")
            if (now - past_date).days >= 7:
                for pick in record['picks']:
                    try:
                        df_p = fdr.DataReader(pick['code'], record['date'], today_str)
                        if len(df_p) >= 2:
                            entry = df_p['Close'].iloc[0]
                            exit_ = df_p['Close'].iloc[min(5, len(df_p) - 1)]
                            ret   = ((exit_ / entry) - 1) * 100
                            total_return += ret
                            total_cases  += 1
                            if ret >= 10.0:
                                win_cases += 1
                    except Exception:
                        continue

        if total_cases > 0:
            performance_results = {
                "win_rate":    round((win_cases / total_cases) * 100, 1),
                "avg_return":  round(total_return / total_cases, 1),
                "total_cases": total_cases
            }
            print(f"  승률: {performance_results['win_rate']}% | 평균수익: {performance_results['avg_return']}% | {total_cases}건")

    # ── history 누적 저장 ──────────────────────────────────────────
    history_data.append({"date": today_str, "picks": final_picks})
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4)

    # ── stock_data.json 저장 (index.html이 읽는 파일) ──────────────
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
