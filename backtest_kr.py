"""
backtest_kr.py — 주간 복기 리포트
스케줄: 매주 토요일 13:00 KST (cron: '0 4 * * 6')

[동작]
1. history.json 에서 20일 이상 경과된 pick 추출
2. 진입일 종가 + 20일 내 최고가 + 20일 후 종가 조회
3. ATR 1차/2차 목표 달성 여부 확인
4. 주간 추천 결과 + 누적 성적표 텔레그램 발송
5. history.json 에 actual 필드 업데이트
"""
import os
import json
import datetime
import time
import FinanceDataReader as fdr
from zoneinfo import ZoneInfo

from engine_common import (
    send_telegram, ko_date, get_market_date, json_safe, DAY_MAP
)

KST          = ZoneInfo("Asia/Seoul")
HISTORY_FILE = 'history.json'
BACKTEST_DAYS = 20       # 복기 기준일
MIN_ELAPSED   = 20       # 진입 후 최소 경과일


# ══════════════════════════════════════════════════════════════
#  날짜 유틸
# ══════════════════════════════════════════════════════════════

def str_to_date(date_str: str) -> datetime.date:
    return datetime.datetime.strptime(date_str, "%Y%m%d").date()


def date_to_str(d: datetime.date) -> str:
    return d.strftime("%Y%m%d")


def trading_days_elapsed(entry_str: str) -> int:
    """진입일로부터 오늘까지 거래일(영업일) 수"""
    try:
        entry = str_to_date(entry_str)
        today = datetime.datetime.now(KST).date()
        # 단순 캘린더 기준 (공휴일 미반영) — 보수적 추정
        delta = (today - entry).days
        return int(delta * 5 / 7)
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════
#  가격 조회
# ══════════════════════════════════════════════════════════════

def fetch_price_data(ticker: str, entry_date_str: str) -> dict | None:
    """
    진입일 종가, 20일 내 최고가, 20일 후 종가 조회
    반환: {entry_price, max_price, max_day, close_20d, dates_fetched}
    """
    try:
        entry_dt  = str_to_date(entry_date_str)
        # 진입일 전후 충분한 범위 조회
        fetch_start = date_to_str(entry_dt - datetime.timedelta(days=5))
        fetch_end   = date_to_str(entry_dt + datetime.timedelta(days=40))

        df = fdr.DataReader(ticker, fetch_start, fetch_end)
        if df is None or df.empty:
            return None

        df.index = df.index.normalize()  # 시간 제거

        # 진입일 종가 — 당일 없으면 가장 가까운 다음 거래일
        entry_rows = df[df.index.date >= entry_dt]
        if entry_rows.empty:
            return None
        entry_price = float(entry_rows['Close'].iloc[0])
        actual_entry_date = entry_rows.index[0].date()

        # 이후 20 거래일 슬라이스
        after = df[df.index.date >= actual_entry_date]
        after_20 = after.iloc[:BACKTEST_DAYS + 1]  # 진입일 포함 21행

        if len(after_20) < 2:
            return None

        # 20일 내 최고가
        max_price = float(after_20['High'].max())
        max_idx   = after_20['High'].idxmax()
        max_day   = max_idx.date()
        days_to_max = len(after_20[after_20.index <= max_idx]) - 1

        # 20일 후 종가 (마지막 행)
        close_20d = float(after_20['Close'].iloc[-1])
        days_fetched = len(after_20) - 1

        return {
            "entry_price":   entry_price,
            "max_price":     max_price,
            "max_day":       date_to_str(max_day),
            "days_to_max":   days_to_max,
            "close_20d":     close_20d,
            "days_fetched":  days_fetched,
        }
    except Exception as e:
        print(f"  ⚠️  {ticker} 가격 조회 실패: {e}")
        return None


# ══════════════════════════════════════════════════════════════
#  ATR 목표 달성 여부
# ══════════════════════════════════════════════════════════════

def check_targets(price_data: dict, trade: dict | None) -> dict:
    """ATR 1차/2차 목표 달성 여부 확인"""
    result = {"target1_hit": False, "target2_hit": False,
              "target1_day": None, "target2_day": None}
    if not trade or not price_data:
        return result

    max_p = price_data['max_price']
    if max_p >= trade.get('target_1', float('inf')):
        result['target1_hit'] = True
    if max_p >= trade.get('target_2', float('inf')):
        result['target2_hit'] = True
    return result


# ══════════════════════════════════════════════════════════════
#  복기 계산
# ══════════════════════════════════════════════════════════════

def calc_backtest(history_data: list) -> tuple[list, list]:
    """
    반환: (updated_entries, weekly_results)
    weekly_results: 이번 주 새로 복기된 항목들
    """
    today      = datetime.datetime.now(KST).date()
    week_start = today - datetime.timedelta(days=today.weekday() + 1)  # 지난 일요일
    week_end   = today

    updated  = []
    weekly   = []

    for entry in history_data:
        date_str = entry.get('date', '')
        picks    = entry.get('picks', [])

        elapsed = trading_days_elapsed(date_str)
        new_picks = []

        for pick in picks:
            # 이미 복기 완료된 항목은 스킵
            if pick.get('actual'):
                new_picks.append(pick)
                continue

            # 20 거래일 미경과 — 아직 복기 불가
            if elapsed < MIN_ELAPSED:
                new_picks.append(pick)
                continue

            ticker = pick.get('code', '')
            trade  = pick.get('meta', {}).get('trade')

            print(f"  복기 중: {pick.get('name')}({ticker}) 진입일 {date_str}")
            time.sleep(0.3)

            price_data = fetch_price_data(ticker, date_str)
            if not price_data:
                new_picks.append(pick)
                continue

            targets = check_targets(price_data, trade)

            entry_p  = price_data['entry_price']
            max_p    = price_data['max_price']
            close_p  = price_data['close_20d']

            max_ret   = round((max_p   - entry_p) / entry_p * 100, 1)
            close_ret = round((close_p - entry_p) / entry_p * 100, 1)

            actual = {
                "entry_price":   entry_p,
                "max_price":     max_p,
                "max_ret":       max_ret,
                "days_to_max":   price_data['days_to_max'],
                "close_20d":     close_p,
                "close_ret":     close_ret,
                "days_fetched":  price_data['days_fetched'],
                "target1_hit":   targets['target1_hit'],
                "target2_hit":   targets['target2_hit'],
                "win":           max_ret >= 10,   # 20일 내 최고가 +10% 이상 = 승
                "backtest_date": date_to_str(today),
            }
            pick['actual'] = actual

            # 이번 주 복기분만 weekly에 추가
            entry_date = str_to_date(date_str)
            if week_start <= entry_date <= week_end or True:  # 전체 포함 (주간 리포트용)
                weekly.append({
                    "entry_date": date_str,
                    "name":       pick.get('name', ''),
                    "code":       ticker,
                    "score":      pick.get('score', ''),
                    "actual":     actual,
                    "trade":      trade,
                })

            new_picks.append(pick)

        entry['picks'] = new_picks
        updated.append(entry)

    return updated, weekly


# ══════════════════════════════════════════════════════════════
#  텔레그램 메시지 조립
# ══════════════════════════════════════════════════════════════

def build_backtest_message(weekly: list, history_data: list) -> str:
    today = datetime.datetime.now(KST).strftime("%Y.%m.%d (%a)")
    for en, ko in DAY_MAP.items():
        today = today.replace(en, ko)

    lines = [
        f"📋 <b>주간 복기 리포트 — {today}</b>",
        "━" * 24,
    ]

    # ── 일자별 추천 결과 ───────────────────────────────────────
    # entry_date 기준으로 그룹핑
    from collections import defaultdict
    by_date = defaultdict(list)
    for item in weekly:
        by_date[item['entry_date']].append(item)

    for date_str in sorted(by_date.keys()):
        d = str_to_date(date_str)
        date_label = d.strftime("%Y.%m.%d (%a)")
        for en, ko in DAY_MAP.items():
            date_label = date_label.replace(en, ko)

        lines.append(f"\n📅 <b>{date_label} 추천</b>")
        for item in by_date[date_str]:
            act   = item.get('actual', {})
            trade = item.get('trade') or {}

            entry_p  = act.get('entry_price', 0)
            max_p    = act.get('max_price', 0)
            max_ret  = act.get('max_ret', 0)
            close_p  = act.get('close_20d', 0)
            close_ret = act.get('close_ret', 0)
            days_max = act.get('days_to_max', 0)
            t1_hit   = act.get('target1_hit', False)
            t2_hit   = act.get('target2_hit', False)
            win      = act.get('win', False)

            result_emoji = "✅" if win else "❌"
            max_arrow    = "📈" if max_ret >= 0 else "📉"
            close_arrow  = "📈" if close_ret >= 0 else "📉"

            t1 = trade.get('target_1', 0)
            t2 = trade.get('target_2', 0)

            lines += [
                f"",
                f"  {result_emoji} <b>{item['name']}</b> ({item['code']})",
                f"    진입가: {entry_p:,}원",
                f"    {max_arrow} 20일 내 최고가: {max_p:,}원 ({max_ret:+.1f}%, Day {days_max})",
                f"    {close_arrow} 20일 후 종가:  {close_p:,}원 ({close_ret:+.1f}%)",
            ]

            # ATR 목표 달성 여부
            if t1:
                t1_mark = "✅" if t1_hit else "❌"
                lines.append(f"    🥇 1차목표({t1:,}원): {t1_mark}")
            if t2:
                t2_mark = "✅" if t2_hit else "❌"
                lines.append(f"    🥈 2차목표({t2:,}원): {t2_mark}")

    # ── 누적 성적표 ────────────────────────────────────────────
    lines += ["", "━" * 24, "📊 <b>누적 성적표</b>"]

    all_actuals = []
    for entry in history_data:
        for pick in entry.get('picks', []):
            act = pick.get('actual')
            if act:
                all_actuals.append(act)

    if all_actuals:
        total    = len(all_actuals)
        wins     = sum(1 for a in all_actuals if a.get('win'))
        win_rate = round(wins / total * 100, 1)
        avg_max  = round(sum(a.get('max_ret', 0) for a in all_actuals) / total, 1)
        avg_close = round(sum(a.get('close_ret', 0) for a in all_actuals) / total, 1)
        best     = max(all_actuals, key=lambda a: a.get('max_ret', 0))
        worst    = min(all_actuals, key=lambda a: a.get('close_ret', 0))
        t1_hits  = sum(1 for a in all_actuals if a.get('target1_hit'))
        t2_hits  = sum(1 for a in all_actuals if a.get('target2_hit'))

        lines += [
            f"  총 분석:      {total}건",
            f"  승률:         {win_rate}% ({wins}승 {total-wins}패)",
            f"  평균 최고수익: {avg_max:+.1f}%",
            f"  평균 20일종가: {avg_close:+.1f}%",
            f"  1차목표 달성: {t1_hits}/{total}건 ({round(t1_hits/total*100,1)}%)",
            f"  2차목표 달성: {t2_hits}/{total}건 ({round(t2_hits/total*100,1)}%)",
            f"  최고 수익:    {best.get('max_ret',0):+.1f}%",
            f"  최대 손실:    {worst.get('close_ret',0):+.1f}%",
        ]
    else:
        lines.append("  아직 복기 완료된 종목 없음")

    lines += ["━" * 24, "💡 <i>승: 20일 내 최고가 +10% 이상 달성</i>"]
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  엔트리포인트
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("📋 주간 복기 시작...")

    # history.json 로드
    if not os.path.exists(HISTORY_FILE):
        print("⚠️  history.json 없음 — 종료")
        exit(0)

    with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
        try:
            history_data = json.load(f)
        except Exception as e:
            print(f"❌ history.json 파싱 실패: {e}")
            exit(1)

    if not history_data:
        print("⚠️  history.json 비어있음 — 종료")
        exit(0)

    # 복기 실행
    updated, weekly = calc_backtest(history_data)

    # history.json 업데이트 저장
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(updated, f, ensure_ascii=False, indent=2, default=json_safe)
    print(f"✅ history.json 업데이트 완료")

    # 복기 결과 발송
    if not weekly:
        msg = "📋 주간 복기 리포트\n복기 가능한 종목 없음 (20 거래일 미경과)"
    else:
        msg = build_backtest_message(weekly, updated)

    send_telegram(msg)
    print("✅ 텔레그램 발송 완료")
