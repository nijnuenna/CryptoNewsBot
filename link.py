import requests
from bs4 import BeautifulSoup
import os
import urllib3
from datetime import datetime
import pytz
from collections import Counter
import html # 특수문자 처리를 위해 추

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

# 자사 관련 키워드
MY_COMPANY_KEYWORDS = ["포블", "포블게이트", "FOBL"] 

def get_korean_date():
    """한국 시간 기준 [m/d(요일)] 형식 반환 (평일 전용)"""
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    days = ['월', '화', '수', '목', '금']
    return f"{now.month}/{now.day}({days[now.weekday()]})"

def get_market_data():
    """비트코인, 테더 가격(업비트) 및 환율(네이버) 정보 수집"""
    btc_raw, usdt_raw = 0, 0
    exchange_rate = "연결 실패"
    
    try:
        ticker_url = "https://api.upbit.com/v1/ticker?markets=KRW-BTC,KRW-USDT"
        ticker_data = requests.get(ticker_url, verify=False, timeout=10).json()
        
        prices = {item['market']: item['trade_price'] for item in ticker_data}
        btc_raw = prices.get('KRW-BTC', 0)
        usdt_raw = prices.get('KRW-USDT', 0)
        
        btc_price_str = f"{btc_raw:,.0f}"
        usdt_price_str = f"{usdt_raw:,.2f}"
        
        if usdt_raw > 0:
            usdt_val = btc_raw / usdt_raw
            btc_in_usdt_k = f"{round(usdt_val / 1000, 1)}K"
        else:
            btc_in_usdt_k = "계산 불가"
            
    except Exception as e:
        print(f"업비트 API 호출 오류: {e}")
        btc_price_str, usdt_price_str, btc_in_usdt_k = "연결 실패", "연결 실패", "연결 실패"

    try:
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
        f"💲 원/달러 환율: ₩{exchange_rate}\n"
        f"📚 자사 기사 ➡️ 업계 전반 ➡️ 비트코인 관련 기사\n"
        f"----------------------------\n\n"
    )
    return market_str

def is_duplicate(title, seen_titles):
    """제목 단어 중첩도 기반 중복 판단 (유사도 40% 이상 시 차단)"""
    new_words = set([w for w in title.split() if len(w) >= 2])
    if not new_words: return False
    
    for seen in seen_titles:
        seen_words = set([w for w in seen.split() if len(w) >= 2])
        if not seen_words: continue
        
        intersection = new_words & seen_words
        if len(intersection) / len(new_words) >= 0.4:
            return True
    return False

def get_news():
    categories = {
        "자사 기사": [],
        "업계 전반": [],
        "비트코인": []
    }
    global_seen_titles = []
    
    search_map = [
        ("자사", MY_COMPANY_KEYWORDS, "자사 기사", False, 1),
        ("가상자산", ["스테이블코인", "STO", "토큰증권","업비트","빗썸","코인원","코빗","고팍스", "가상자산", "디지털 자산",
        "금융당국 스테이블코인", "스테이블코인 법안", "금융감독원 스테이블코인", "금융위 스테이블코인", 
        "디지털자산법","디지털자산기본법", "코인거래소", "디지털자산거래소", "가상자산 규제", "커스터디", "가상자산 거래소", "암호화폐",
        "토큰화 증권"], "업계 전반", True, 20),
        ("비트코인", ["비트코인", "비트코인 현물 ETF", "비트코인 채굴", "비트코인 ETF"], "비트코인", True, 5)
    ]

    for label, keywords, cat_name, is_24h, limit in search_map:
        query = " OR ".join(f'"{kw}"' for kw in keywords)
        url = f"https://news.google.com/rss/search?q={query}"
        if is_24h: url += "+when:1d"
        url += "&hl=ko&gl=KR&ceid=KR:ko"
        
        word_freq = Counter()
        try:
            response = requests.get(url, verify=False, timeout=15)
            soup = BeautifulSoup(response.content, 'xml')
            items = soup.find_all('item')
            count = 0
            
            for item in items:
                if count >= limit: break
                title = html.escape(item.title.text)
                link = item.link.text
                source = html.escape(item.source.text) if item.source else "뉴스"
                
                if is_duplicate(title, global_seen_titles):
                    continue
                
                title_words = [w for w in title.split() if len(w) >= 2]
                if any(word_freq[w] >= 2 for w in title_words):
                    continue

                date_str = get_korean_date()
                formatted_item = f"▲ {title} ({source} {date_str})\n{link}"
                categories[cat_name].append(formatted_item)
                
                word_freq.update(title_words)
                global_seen_titles.append(title)
                count += 1
        except Exception as e:
            print(f"Error crawling {label}: {e}")
            
    return categories

def send_telegram(market_data, categories):
    date_str = get_korean_date()
    header = f"<b>[{date_str} 뉴스클리핑]</b>\n\n"
    
    # 메시지 조각들을 담을 리스트
    messages = []
    # 현재 작성 중인 메시지 버퍼 (헤더와 마켓 데이터로 시작)
    current_msg = header + market_data
    
    order = ["자사 기사", "업계 전반", "비트코인"]
    MAX_CHAR = 4000 # 안전을 위해 4000자로 설정

    for cat_name in order:
        news_list = categories.get(cat_name)
        if not news_list:
            continue
            
        # 카테고리 제목 추가
        cat_header = f"<b>✅ {cat_name}</b>\n\n"
        
        # 현재 메시지에 카테고리 제목을 넣을 공간이 있는지 확인
        if len(current_msg) + len(cat_header) > MAX_CHAR:
            messages.append(current_msg)
            current_msg = "<b>(계속)</b>\n\n" + cat_header
        else:
            current_msg += cat_header

        for item in news_list:
            # 기사 하나는 '▲ 제목... \n 링크' 형태의 한 세트임
            item_text = item + "\n\n\n"
            
            # 이번 기사를 추가했을 때 제한을 넘는지 확인
            if len(current_msg) + len(item_text) > MAX_CHAR:
                # 넘는다면 지금까지의 메시지를 저장하고 새로 시작
                messages.append(current_msg)
                current_msg = "<b>(계속)</b>\n\n" + item_text
            else:
                # 안 넘으면 계속 추가
                current_msg += item_text

    # 마지막에 남은 메시지 추가
    if current_msg.strip():
        messages.append(current_msg)

    if not TELEGRAM_TOKEN or not CHAT_ID:
        return

    send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN.strip()}/sendMessage"
    
    for msg in messages:
        # 열린 HTML 태그 닫기 보정 (굵게 표시 등)
        if msg.count("<b>") > msg.count("</b>"):
            msg += "</b>"
            
        payload = {
            "chat_id": CHAT_ID.strip(),
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        res = requests.post(send_url, json=payload, verify=False)
        if res.status_code != 200:
            print(f"전송 실패: {res.text}")

if __name__ == "__main__":
    market_info = get_market_data()
    news_data = get_news()

    send_telegram(market_info, news_data)
