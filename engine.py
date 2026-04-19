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
# 컨셉: 세력 흔적 탐지 (정밀형)
#
# [핵심 로직]
# 1. 최근 20일 내 거래량 스파이크(250%↑) 발생 → 세력 개입 봉 탐지
# 2. 스파이크 이후 거래량 50% 이하로 안정 → 세력이 조용히 대기 중
# 3. 현재가 스파이크 봉 저가 위에서 유지 → 세력이 물량 지키는 중
# 4. 수급 외인/기관 최근 3일 중 1일 이상 → 매수 주체 확인
# ──────────────────────────────────────────────────────────────


def get_financial_data(ticker):
    """
    현재: 테스트용 더미
    실제 운영 시 OpenDART API로 교체 필요
    https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json
    """
    return {"OPM": 5.0, "DebtRatio": 120.0}


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

    # all_tickers = all_tickers[:200]  # 빠른 테스트 시 주석 해제

    final_picks = []
    filter_log  = {
        "total":   len(all_tickers),
        "penny":   0,   # 동전주 컷 생존
        "spike":   0,   # 스파이크 봉 발견 (20일 이내)
        "silence": 0,   # 스파이크 후 거래량 침묵
        "hold":    0,   # 스파이크 봉 저가 위에서 주가 유지
        "supply":  0,   # 수급 확인
        "fin":     0,   # 재무 통과 (최종 픽)
    }

    print("🚀 [1] 세력 흔적 탐지 시작...")
    for ticker in all_tickers:
        try:
            df = fdr.DataReader(ticker, start_90d, today_str)
            if len(df) < 25:
                continue

            df['MA20']     = df['Close'].rolling(window=20).mean()
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()

            current_close = df['Close'].iloc[-1]
            current_vol   = df['Volume'].iloc[-1]

            # [방어막] 동전주 제외 (1,000원 미만)
            if current_close < 1000:
                continue
            filter_log['penny'] += 1

            # ── [필터 1] 최근 20일 내 스파이크 봉 탐지 ──────────────
            # 조건: 거래량 250%↑, 양봉, 20일선 위 마감
            # "최근 20일 이내"로 제한 → 오래된 신호 제거
            recent_20 = df.iloc[-20:].copy()
            recent_20['Is_Spike'] = (
                (recent_20['Volume'] >= recent_20['Vol_MA20'] * 2.5) &
                (recent_20['Close']  >  recent_20['Open']) &
                (recent_20['Close']  >  recent_20['MA20'])
            )
            spike_days = recent_20[recent_20['Is_Spike']]
            if spike_days.empty:
                continue
            filter_log['spike'] += 1

            # 가장 최근 스파이크 봉 기준
            last_spike     = spike_days.iloc[-1]
            spike_vol      = last_spike['Volume']
            spike_low      = last_spike['Low']      # 스파이크 봉 저가 (세력 방어선)
            spike_vol_ratio = spike_vol / last_spike['Vol_MA20']

            # 스파이크 발생 이후 인덱스 추출
            spike_idx    = df.index.get_loc(last_spike.name)
            after_spike  = df.iloc[spike_idx + 1:]  # 스파이크 다음날부터 현재까지

            if after_spike.empty:
                # 스파이크가 오늘 발생한 경우 → 침묵 구간 없음, 패스
                continue

            # ── [필터 2] 스파이크 이후 거래량 침묵 확인 ─────────────
            # 스파이크 이후 평균 거래량이 스파이크 봉의 50% 이하
            # → "세력이 물량 쌓고 조용히 기다리는 중" 신호
            avg_vol_after = after_spike['Volume'].mean()
            if avg_vol_after > spike_vol * 0.5:
                continue
            filter_log['silence'] += 1

            silence_ratio = avg_vol_after / spike_vol  # 낮을수록 침묵 깊음

            # ── [필터 3] 스파이크 봉 저가 위에서 주가 유지 ───────────
            # 현재가 > 스파이크 봉 저가
            # → "세력이 평균 단가 지키고 있다" = 아직 매집 완료 전
            if current_close <= spike_low:
                continue
            filter_log['hold'] += 1

            # 저가 대비 현재 위치 (높을수록 좋음)
            price_vs_low = (current_close / spike_low - 1) * 100

            # ── [필터 4] 수급 확인 ────────────────────────────────────
            # 최근 3일 중 외인 or 기관 순매수 1일 이상
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

            # ── [필터 5] 재무 ─────────────────────────────────────────
            fin = get_financial_data(ticker)
            if fin['OPM'] < 0.0 or fin['DebtRatio'] >= 300.0:
                continue
            filter_log['fin'] += 1

            # ── 점수 산출 (100점 만점) ────────────────────────────────

            # 1. 스파이크 강도 (최대 35점): 250%면 15점, 700% 이상이면 35점
            spike_score = min(((spike_vol_ratio - 2.5) / 4.5) * 20 + 15, 35)

            # 2. 침묵 깊이 (최대 30점): 비율 낮을수록 고점 (0%면 30점, 50%면 0점)
            silence_score = min((0.5 - silence_ratio) / 0.5 * 30, 30)

            # 3. 수급 강도 (최대 20점)
            supply_score = 10 if valid_buy_days == 1 else 15 if valid_buy_days == 2 else 20

            # 4. 저가 지지 강도 (최대 15점): 저가 대비 많이 오를수록 감점
            #    너무 많이 오르면 이미 늦은 것 → 5% 이하면 15점, 20% 이상이면 5점
            hold_score = max(15 - (price_vs_low / 20) * 10, 5)

            total_score = int(spike_score + silence_score + supply_score + hold_score)
            total_score = min(total_score, 100)

            stars = "★" * (total_score // 20) + "☆" * (5 - total_score // 20)

            if foreign_buy_days > 0 and inst_buy_days > 0:
                supply_text = "외인+기관 양매수"
            elif foreign_buy_days > 0:
                supply_text = "외인매수"
            else:
                supply_text = "기관매수"

            # 스파이크 이후 경과 일수
            days_after_spike = len(after_spike)

            name         = stock.get_market_ticker_name(ticker)
            expected_ret = round(5.0 + (total_score / 100) * 12.0, 1)

            final_picks.append({
                "name":            name,
                "code":            ticker,
                "supply":          supply_text,
                "volume_growth":   f"+{int(spike_vol_ratio * 100)}%",
                "score":           f"{stars} ({total_score}점)",
                "tags":            f"스파이크 {days_after_spike}일전 / 침묵중 / 저가지지",
                "expected_return": f"{expected_ret}%"
            })

            print(f"  ✅ {name}({ticker}) | {stars} {total_score}점 | {supply_text} | 스파이크 {days_after_spike}일전")

        except Exception:
            continue

    # 필터 단계별 결과
    print("\n📊 [세력 흔적 탐지 필터 생존 현황]")
    print(f"  전체 검사:         {filter_log['total']:>5}건")
    print(f"  동전주 컷:         {filter_log['penny']:>5}건  (생존)")
    print(f"  스파이크 발견:     {filter_log['spike']:>5}건  (20일 내 250%↑ 양봉)")
    print(f"  거래량 침묵:       {filter_log['silence']:>5}건  (스파이크 대비 50%↓)")
    print(f"  저가 위 유지:      {filter_log['hold']:>5}건  (세력 방어선 위)")
    print(f"  수급 확인:         {filter_log['supply']:>5}건  (외인/기관 1일↑)")
    print(f"  재무 통과:         {filter_log['fin']:>5}건  ← 최종 픽")

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

    # ── 백테스트 (7일 후 수익률) ──────────────────────────────────
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
                            if ret >= 7.0:
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
