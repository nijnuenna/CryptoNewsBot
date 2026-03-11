import requests
from bs4 import BeautifulSoup
import os
import urllib3
from datetime import datetime
import pytz

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 환경 변수 설정
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

#TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
#CHAT_ID = os.environ.get('CHAT_ID')

# 자사 관련 키워드
MY_COMPANY_KEYWORDS = ["포블", "포블게이트", "FOBL"] 

def get_korean_date():
    """한국 시간 기준 [m/d(요일)] 형식 반환 (평일 전용)"""
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    # 주말(토, 일) 삭제 - 평일 실행 전제
    days = ['월', '화', '수', '목', '금']
    return f"{now.month}/{now.day}({days[now.weekday()]})"

def get_market_data():
    """비트코인, 테더 가격(업비트) 및 환율(네이버) 정보 수집"""
    btc_raw, usdt_raw = 0, 0
    exchange_rate = "연결 실패"
    
    try:
        # 1. 업비트에서 BTC, USDT 가격 가져오기
        ticker_url = "https://api.upbit.com/v1/ticker?markets=KRW-BTC,KRW-USDT"
        ticker_data = requests.get(ticker_url, verify=False, timeout=10).json()
        
        prices = {item['market']: item['trade_price'] for item in ticker_data}
        btc_raw = prices.get('KRW-BTC', 0)
        usdt_raw = prices.get('KRW-USDT', 0)
        
        # 원화 가격 포맷팅
        btc_price_str = f"{btc_raw:,.0f}"
        usdt_price_str = f"{usdt_raw:,.2f}"
        
        # [수정] 테더 기준 가격 계산 및 K 표기 (예: 67.6K)
        if usdt_raw > 0:
            usdt_val = btc_raw / usdt_raw
            # 1000으로 나누고 소수점 첫째 자리까지 반올림
            btc_in_usdt_k = f"{round(usdt_val / 1000, 1)}K"
        else:
            btc_in_usdt_k = "계산 불가"
            
    except Exception as e:
        print(f"업비트 API 호출 오류: {e}")
        btc_price_str, usdt_price_str, btc_in_usdt_k = "연결 실패", "연결 실패", "연결 실패"

    try:
        # 2. 네이버 금융에서 환율 정보 가져오기
        ex_url = "https://finance.naver.com/marketindex/"
        headers = {'User-Agent': 'Mozilla/5.0'}
        ex_res = requests.get(ex_url, headers=headers, verify=False, timeout=10)
        ex_soup = BeautifulSoup(ex_res.text, 'html.parser')
        rate_tag = ex_soup.select_one(".value")
        exchange_rate = rate_tag.text if rate_tag else "확인 불가"
    except Exception as e:
        print(f"환율 수집 오류: {e}")
        exchange_rate = "연결 실패"

    market_str = (
        f"📊 <b>오늘의 가격</b>\n"
        f"🟡 비트코인: ₩{btc_price_str} ({btc_in_usdt_k})\n"
        f"💵 테더(USDT): ₩{usdt_price_str}\n"
        f"💲 원/달러 환율: ₩{exchange_rate}\n\n"
        f"---------------------------------------------------------------------------------------------------------\n\n"
    )
    return market_str

def is_duplicate(title, seen_titles):
    """제목 유사도 검사"""
    short_title = title.replace(" ", "")[:15]
    for seen in seen_titles:
        if short_title in seen.replace(" ", ""):
            return True
    return False

def get_news():
    categories = {
        "자사 기사": [],
        "업계 전반": [],
        "비트코인": []
    }
    
    search_map = [
        ("자사", MY_COMPANY_KEYWORDS, "자사 기사", False, 1),
        ("가상자산", ["스테이블코인", "STO", "토큰증권","업비트","빗썸","코인원","코빗","고팍스", "가상자산", "디지털 자산",
        "금융당국 스테이블코인", "스테이블코인 법안", "금융감독원 스테이블코인", "금융위 스테이블코인", 
        "디지털자산법", "코인거래소", "디지털자산거래소", "가상자산 규제", "커스터디", "가상자산 거래소", "암호화폐"], "업계 전반", True, 20),
        ("비트코인", ["비트코인", "비트코인 현물 ETF", "비트코인 채굴", "비트코인 ETF"], "비트코인", True, 5)
    ]

    for label, keywords, cat_name, is_24h, limit in search_map:
        query = " OR ".join(f'"{kw}"' for kw in keywords)
        url = f"https://news.google.com/rss/search?q={query}"
        if is_24h:
            url += "+when:1d"
        url += "&hl=ko&gl=KR&ceid=KR:ko"
        
        try:
            response = requests.get(url, verify=False, timeout=15)
            soup = BeautifulSoup(response.content, 'xml')
            items = soup.find_all('item')
            seen_titles = []
            count = 0
            
            for item in items:
                if count >= limit: break
                title = item.title.text
                link = item.link.text
                source = item.source.text if item.source else "뉴스"
                
                if is_duplicate(title, seen_titles):
                    continue
                
                date_str = get_korean_date()
                formatted_item = f"▲ <a href='{link}'>{title}</a> ({source} {date_str})"
                categories[cat_name].append(formatted_item)
                
                seen_titles.append(title)
                count += 1
        except Exception as e:
            print(f"Error crawling {label}: {e}")
            
    return categories

def send_telegram(market_data, categories):
    date_str = get_korean_date()
    header = f"<b>[{date_str} 뉴스클리핑]</b>\n\n"
    
    body = ""
    order = ["자사 기사", "업계 전반", "비트코인"]
    for cat_name in order:
        news_list = categories.get(cat_name)
        if news_list:
            body += f"<b>□ {cat_name}</b>\n\n"
            body += "\n\n".join(news_list)
            body += "\n\n\n"

    if not body.strip():
        return

    final_text = header + market_data + body
    
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("환경 변수 오류")
        return

    send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN.strip()}/sendMessage"
    payload = {
        "chat_id": CHAT_ID.strip(),
        "text": final_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    requests.post(send_url, json=payload, verify=False)

if __name__ == "__main__":
    market_info = get_market_data()
    news_data = get_news()

    send_telegram(market_info, news_data)
