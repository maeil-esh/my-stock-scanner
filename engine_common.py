"""
engine_common.py — 공통 유틸, 텔레그램, 매크로, 뉴스 브리핑
engine_kr.py / engine_us.py 에서 import해서 사용
"""
import os
import datetime
import requests
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo   # [FIX] UTC→KST 통일

KST = ZoneInfo("Asia/Seoul")

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DART_API_KEY     = os.environ.get("DART_API_KEY", "")

DAY_MAP = {"Mon":"월","Tue":"화","Wed":"수","Thu":"목","Fri":"금","Sat":"토","Sun":"일"}


# ── 날짜 ───────────────────────────────────────────────────────

def get_market_date():
    """KST 기준 가장 최근 거래일 반환"""                        # [FIX] UTC→KST
    today = datetime.datetime.now(KST)
    wd = today.weekday()
    if wd == 5:   today -= datetime.timedelta(days=1)
    elif wd == 6: today -= datetime.timedelta(days=2)
    return today.strftime("%Y%m%d")


def get_start_date(base_str, days_ago):
    base = datetime.datetime.strptime(base_str, "%Y%m%d")
    return (base - datetime.timedelta(days=int(days_ago * 1.5))).strftime("%Y%m%d")


def ko_date(date_str: str) -> str:
    try:
        d = datetime.datetime.strptime(date_str, "%Y%m%d")
    except Exception:
        d = datetime.datetime.now(KST)                          # [FIX] KST
    s = d.strftime("%Y.%m.%d (%a)")
    for en, ko in DAY_MAP.items():
        s = s.replace(en, ko)
    return s


def now_label() -> str:
    h = datetime.datetime.now(KST).hour                        # [FIX] UTC→KST
    if h < 11:   return "🔔 장 시작 스캔"
    elif h < 15: return "📊 장 중간 스캔"
    elif h < 19: return "🏁 장 마감 스캔"
    else:        return "🌙 미장 스캔"


# ── 텔레그램 ───────────────────────────────────────────────────

def send_telegram(message: str):
    if not TELEGRAM_TOKEN:
        print("⚠️  TELEGRAM_TOKEN 없음 — 스킵")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    MAX_LEN = 4096                                              # [FIX] 길이 초과 분할
    chunks = [message[i:i+MAX_LEN] for i in range(0, len(message), MAX_LEN)]
    for chunk in chunks:
        try:
            resp = requests.post(url, json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       chunk,
                "parse_mode": "HTML"
            }, timeout=10)
            if resp.status_code == 200:
                print(f"✅ 텔레그램 전송 완료 ({len(chunk)}자)")
            else:
                print(f"⚠️  텔레그램 실패: {resp.text}")
        except Exception as e:
            print(f"⚠️  텔레그램 오류: {e}")


# ── 매크로 브리핑 ──────────────────────────────────────────────

def fetch_macro_summary() -> str:
    symbols = {
        "KOSPI":   ("^KS11",  lambda v: f"{v:,.2f}"),
        "KOSDAQ":  ("^KQ11",  lambda v: f"{v:,.2f}"),
        "USD/KRW": ("KRW=X",  lambda v: f"{v:,.0f}원"),
        "WTI":     ("CL=F",   lambda v: f"${v:.1f}"),
        "Gold":    ("GC=F",   lambda v: f"${v:,.0f}"),
        "VIX":     ("^VIX",   lambda v: f"{v:.2f}"),
        "US 10Y":  ("^TNX",   lambda v: f"{v:.2f}%"),
    }
    today = datetime.datetime.now(KST).strftime("%Y.%m.%d (%a)")  # [FIX] KST
    for en, ko in DAY_MAP.items():
        today = today.replace(en, ko)

    label = now_label()
    lines = [f"📊 <b>UK2 — {today} {label}</b>", "━" * 24]

    for name, (sym, fmt) in symbols.items():
        try:
            info  = yf.Ticker(sym).fast_info
            # [FIX] fast_info 속성 안전 접근 — 없으면 대체 속성 시도
            price = (getattr(info, 'last_price', None)
                     or getattr(info, 'regularMarketPrice', None))
            prev  = (getattr(info, 'previous_close', None)
                     or getattr(info, 'regularMarketPreviousClose', None))
            if not price:
                raise ValueError("price 없음")
            chg   = (price - prev) / prev * 100 if prev else 0
            arrow = "▲" if chg >= 0 else "▼"
            sign  = "+" if chg >= 0 else ""
            lines.append(f"{name:<8} {fmt(price)}   {arrow} {sign}{chg:.2f}%")
        except Exception:
            lines.append(f"{name:<8} 조회 실패")

    lines += ["━" * 24, "💡 <i>UK2 Investment · AI 브리핑</i>"]
    return "\n".join(lines)


# ── 뉴스 크롤링 ────────────────────────────────────────────────

def fetch_naver_news(max_items: int = 10) -> list:
    """네이버 금융 뉴스 헤드라인 수집"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    news = []
    urls = [
        "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258",
        "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=259",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=8)
            r.encoding = "euc-kr"
            soup = BeautifulSoup(r.text, "html.parser")
            for item in soup.select("dl dd.articleSubject a"):
                title = item.get_text(strip=True)
                if title and len(title) > 5:
                    news.append(title)
                if len(news) >= max_items:
                    break
        except Exception:
            continue
        if len(news) >= max_items:
            break
    return news[:max_items]


def fetch_theme_news(max_items: int = 5) -> list:
    """네이버 금융 테마 뉴스 수집"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    news = []
    try:
        r = requests.get(
            "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=260",
            headers=headers, timeout=8
        )
        r.encoding = "euc-kr"
        soup = BeautifulSoup(r.text, "html.parser")
        for item in soup.select("dl dd.articleSubject a"):
            title = item.get_text(strip=True)
            if title and len(title) > 5:
                news.append(title)
            if len(news) >= max_items:
                break
    except Exception:
        pass
    return news[:max_items]


def build_news_briefing() -> str:
    """전일 이슈 + 금일 유력 테마 텔레그램 메시지 생성"""
    today = datetime.datetime.now(KST).strftime("%Y.%m.%d (%a)")  # [FIX] KST
    for en, ko in DAY_MAP.items():
        today = today.replace(en, ko)

    lines = [f"🗞 <b>시황 브리핑 — {today}</b>", "━" * 24]

    market_news = fetch_naver_news(max_items=8)
    lines.append("📌 <b>전일 주요 이슈</b>")
    if market_news:
        for i, title in enumerate(market_news, 1):
            lines.append(f"  {i}. {title}")
    else:
        lines.append("  뉴스 수집 실패")

    lines.append("")

    theme_news = fetch_theme_news(max_items=5)
    lines.append("🔥 <b>금일 유력 테마</b>")
    if theme_news:
        for i, title in enumerate(theme_news, 1):
            lines.append(f"  {i}. {title}")
    else:
        lines.append("  테마 수집 실패")

    lines += ["━" * 24, "💡 <i>출처: 네이버 금융</i>"]
    return "\n".join(lines)


# ── 기술 지표 ──────────────────────────────────────────────────

def calc_rsi(series, period=14):
    """Wilder EWM 방식 RSI — TradingView 기준과 일치"""         # [FIX] SMA→EWM
    delta = series.diff()
    gain  = delta.where(delta > 0, 0.0).ewm(alpha=1/period, adjust=False).mean()
    loss  = (-delta.where(delta < 0, 0.0)).ewm(alpha=1/period, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_obv(df):
    return (np.sign(df['Close'].diff()).fillna(0) * df['Volume']).cumsum()
