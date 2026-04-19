import os
import json
import datetime
import pandas as pd
from pykrx import stock
import FinanceDataReader as fdr

# 파일 경로
DATA_FILE = 'stock_data.json'
HISTORY_FILE = 'history.json'
MY_PICKS_FILE = 'my_picks.json'


# --- [재무 데이터] ---
# 실제 운영 시 OpenDART API 또는 FnGuide 크롤링 코드로 교체하세요.
def get_financial_data(ticker):
    return {"OPM": 15.0, "DebtRatio": 100.0}


def get_start_date(days_ago):
    """달력 기준 N일 전 날짜 반환 (주말 포함 여유 계산)"""
    return (datetime.datetime.now() - datetime.timedelta(days=int(days_ago * 1.5))).strftime("%Y%m%d")


def analyze_with_manual_picks():
    now = datetime.datetime.now()
    today_str = now.strftime("%Y%m%d")

    start_90d = get_start_date(90)   # 매집봉 탐색용
    start_5d  = get_start_date(5)    # 수급 확인용 (3영업일 커버)

    # 1. 전 종목 목록
    kospi_tickers  = stock.get_market_ticker_list(today_str, market="KOSPI")
    kosdaq_tickers = stock.get_market_ticker_list(today_str, market="KOSDAQ")
    all_tickers    = kospi_tickers + kosdaq_tickers
    kospi_set      = set(kospi_tickers)

    # 빠른 테스트 시 아래 주석 해제 (상위 100종목만)
    # all_tickers = all_tickers[:100]

    final_picks = []

    print("🚀 [1] 종목 필터링 및 점수 산출 시작...")
    for ticker in all_tickers:
        try:
            # ── 주가/거래량 데이터 ─────────────────────────────────────
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

            # [필터 2] 최근 60영업일 내 매집봉 존재 여부
            recent = df.iloc[-60:].copy()
            recent['Is_Accum'] = (
                (recent['Volume'] >= recent['Vol_MA20'] * 3) &
                (recent['Close']  >  recent['Open']) &
                (recent['Close']  >  recent['MA20'])
            )
            accum_days = recent[recent['Is_Accum']]
            if accum_days.empty:
                continue

            last_accum      = accum_days.iloc[-1]
            accum_vol       = last_accum['Volume']
            accum_vol_ratio = accum_vol / last_accum['Vol_MA20']

            # [필터 3] 거래량 가뭄: 현재 거래량 ≤ 매집봉의 40%
            if current_vol > accum_vol * 0.4:
                continue
            vol_drop_ratio = current_vol / accum_vol

            # [필터 4] 수급: 종목별 1회 API 호출 (전체시장 스캔 제거 → 속도 대폭 개선)
            try:
                df_inv = stock.get_market_trading_value_by_investor(start_5d, today_str, ticker)
                df_inv_3d       = df_inv.tail(3)
                foreign_buy_days = int((df_inv_3d['외국인합계'] > 0).sum())
                inst_buy_days    = int((df_inv_3d['기관합계']   > 0).sum())
                valid_buy_days   = int(((df_inv_3d['외국인합계'] > 0) | (df_inv_3d['기관합계'] > 0)).sum())
            except Exception:
                continue

            if valid_buy_days < 2:
                continue

            # [필터 5] 재무: OPM > 10%, 부채비율 < 150%
            fin = get_financial_data(ticker)
            if fin['OPM'] <= 10.0 or fin['DebtRatio'] >= 150.0:
                continue

            # ── 점수 산출 (100점 만점) ─────────────────────────────────
            vol_score  = min(((accum_vol_ratio - 3) / 7) * 20 + 10, 30)   # 최대 30점
            dry_score  = min((0.4 - vol_drop_ratio) / 0.3 * 15 + 5, 20)   # 최대 20점
            buy_score  = 10 if valid_buy_days == 2 else 20                  # 최대 20점
            opm_score  = min((fin['OPM']       - 10)  / 20  * 15, 15)      # 최대 15점
            debt_score = min((150 - fin['DebtRatio'])  / 100 * 15, 15)     # 최대 15점
            total_score = int(vol_score + dry_score + buy_score + opm_score + debt_score)

            stars = "★" * (total_score // 20) + "☆" * (5 - total_score // 20)

            # 수급 텍스트
            if foreign_buy_days > 0 and inst_buy_days > 0:
                supply_text = "외인+기관 양매수"
            elif foreign_buy_days >= inst_buy_days:
                supply_text = "외인매수"
            else:
                supply_text = "기관매수"

            name = stock.get_market_ticker_name(ticker)
            expected_ret = round(7.0 + (total_score / 100) * 8.0, 1)

            final_picks.append({
                "name":            name,
                "code":            ticker,
                "supply":          supply_text,
                "volume_growth":   f"+{int(accum_vol_ratio * 100)}%",
                "score":           f"{stars} ({total_score}점)",
                "tags":            "매집봉확인 / 거래량가뭄 / 20일선돌파",
                "expected_return": f"{expected_ret}%"
            })

        except Exception:
            continue

    # ── [C] 선생님 수동 픽 검증 ────────────────────────────────────────
    my_manual_report = []
    if os.path.exists(MY_PICKS_FILE):
        with open(MY_PICKS_FILE, 'r', encoding='utf-8') as f:
            my_picks = json.load(f)

        for p in my_picks:
            try:
                curr_df = stock.get_market_ohlcv(today_str, today_str, p['code'])
                if not curr_df.empty:
                    curr_price = int(curr_df['종가'].iloc[-1])
                    profit = round(((curr_price / p['buy_price']) - 1) * 100, 2)
                    my_manual_report.append({
                        "date":      p['date'],
                        "name":      p['name'],
                        "buy_price": p['buy_price'],
                        "curr_price": curr_price,
                        "profit":    profit
                    })
            except Exception:
                continue

    # ── [D] 백테스트 (history.json 기반 승률 산출) ─────────────────────
    performance_results = {"win_rate": 0.0, "avg_return": 0.0, "total_cases": 0}
    history_data = []

    if os.path.exists(HISTORY_FILE):
        print("🔍 [4] 백테스트 계산 중...")
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            try:
                history_data = json.load(f)
            except json.JSONDecodeError:
                history_data = []

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

    # ── [E] history.json 누적 저장 ─────────────────────────────────────
    print("💾 [5] history.json 저장 중...")
    history_data.append({"date": today_str, "picks": final_picks})
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=4)

    # ── [F] stock_data.json 저장 (index.html이 읽는 파일) ──────────────
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

    print(f"✅ 완료! 오늘의 픽: {len(final_picks)}건")
    return final_output


if __name__ == "__main__":
    analyze_with_manual_picks()
