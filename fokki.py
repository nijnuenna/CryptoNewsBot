import requests
import os
import urllib3
from datetime import datetime
import pytz

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# 환경변수
# ============================================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

DAYS_KR = ['월', '화', '수', '목', '금', '토', '일']


# ============================================================
# 텔레그램 전송
# ============================================================

def send_telegram(text):
    """텔레그램 채팅방에 HTML 형식 메시지 전송"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("[ERROR] TELEGRAM_TOKEN 또는 CHAT_ID 미설정")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN.strip()}/sendMessage"
    payload = {
        "chat_id": CHAT_ID.strip(),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        resp = requests.post(url, json=payload, timeout=15, verify=False)
        resp.raise_for_status()
        print("[OK] 텔레그램 전송 완료")
        return True
    except Exception as e:
        print(f"[ERROR] 텔레그램 전송 실패: {e}")
        return False


# ============================================================
# 네이버 환율 조회
# ============================================================

def get_usd_krw_rate():
    """네이버 증권에서 USD/KRW 환율 크롤링. 실패 시 None 반환"""
    try:
        url = "https://finance.naver.com/marketindex/"
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        resp.encoding = 'euc-kr'
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, 'html.parser')
        value_tag = soup.select_one('#exchangeList .value')
        if value_tag:
            return float(value_tag.text.strip().replace(',', ''))
    except Exception:
        pass
    return None


# ============================================================
# 시세 조회 (업비트 공개 API)
# ============================================================

def get_market_data():
    """업비트 시세 + 네이버 환율을 조회하여 텔레그램 메시지용 문자열 반환"""
    btc_krw, btc_usd = "연결 실패", "연결 실패"
    eth_krw, eth_usd = "연결 실패", "연결 실패"
    usdt_krw = "연결 실패"
    usd_krw_display = "연결 실패"
    fetch_time = "시간 미확인"

    # --- 1) 네이버 환율 (USD 환산에도 사용) ---
    rate = get_usd_krw_rate()
    if rate:
        usd_krw_display = f"{rate:,.2f}"

    # --- 2) 업비트 시세 ---
    try:
        url = "https://api.upbit.com/v1/ticker"
        params = {"markets": "KRW-BTC,KRW-ETH,KRW-USDT"}
        resp = requests.get(url, params=params, timeout=10, verify=False)
        resp.raise_for_status()
        data = {item['market']: item for item in resp.json()}

        # BTC
        if 'KRW-BTC' in data:
            price = data['KRW-BTC']['trade_price']
            btc_krw = f"{price:,.0f}"
            if rate:
                btc_usd = f"{price / rate / 1000:.1f}K"

        # ETH
        if 'KRW-ETH' in data:
            price = data['KRW-ETH']['trade_price']
            eth_krw = f"{price:,.0f}"
            if rate:
                eth_usd = f"{price / rate / 1000:.1f}K"

        # USDT
        if 'KRW-USDT' in data:
            usdt_krw = f"{data['KRW-USDT']['trade_price']:,.0f}"

        # 시간 (아무 종목이나 타임스탬프 사용)
        ts = data.get('KRW-BTC', {}).get('trade_timestamp')
        if ts:
            dt_kst = datetime.fromtimestamp(ts / 1000, tz=pytz.timezone('Asia/Seoul'))
            fetch_time = dt_kst.strftime('%H:%M')

    except Exception as e:
        print(f"[ERROR] 업비트 시세 조회 실패: {e}")

    return (
        f"📊 <b>오늘의 가격 : {fetch_time} 기준</b>\n"
        f"🟡 비트코인: ₩{btc_krw}\n"
        f"⚪ 이더리움: ₩{eth_krw}\n"
        f"🟢 테더: {usdt_krw}원\n"
        f"💱 환율: {usd_krw_display}원\n\n"
    )


# ============================================================
# 실행
# ============================================================

if __name__ == "__main__":
    message = get_market_data()
    print(message)
    send_telegram(message)