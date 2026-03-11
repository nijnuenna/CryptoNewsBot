import requests
from bs4 import BeautifulSoup
import os
import urllib3
from datetime import datetime
import pytz
from collections import Counter
import html
import re
import base64
import json
import time
from urllib.parse import urlparse, quote

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 환경 변수 설정
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '8611748109:AAEihME-JpRsIUmY-3_Wz9vEOkTfSCdjv8k')
CHAT_ID = os.environ.get('CHAT_ID', '5017611906')
CMC_API_KEY = os.environ.get('CMC_API_KEY',"3ebd2f754fba44cda22cc4c88990e04f")

# 키워드 및 요일 설정
MY_COMPANY_KEYWORDS = ["포블", "포블게이트", "FOBL"] 
DAYS_KR = ['월', '화', '수', '목', '금', '토', '일']

def clean_text(text):
    text = re.sub(r'\[.*?\]|\(.*?\)', '', text)
    text = re.sub(r'[^가-힣a-zA-Z0-9\s]', ' ', text)
    return set([w for w in text.split() if len(w) >= 2])

def get_korean_date():
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    return f"{now.month}/{now.day}({DAYS_KR[now.weekday()]})"

def get_market_data():
    """BTC, ETH 가격 및 코인마켓캡 실제 업데이트 시각 통합"""
    btc_krw, btc_usd, eth_krw, eth_usd, fetch_time = "연결 실패", "연결 실패", "연결 실패", "연결 실패", "시간 미확인"
    
    if not CMC_API_KEY: 
        return "📊 <b>오늘의 가격</b>\n⚠️ CMC_API_KEY 미설정\n\n"

    try:
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
        headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY.strip()}
        
        res_k = requests.get(url, headers=headers, params={'symbol': 'BTC,ETH', 'convert': 'KRW'}, timeout=10, verify=False).json()
        if 'data' in res_k:
            btc_krw = f"{res_k['data']['BTC']['quote']['KRW']['price']:,.0f}"
            eth_krw = f"{res_k['data']['ETH']['quote']['KRW']['price']:,.0f}"
            dt_utc = datetime.strptime(res_k['data']['BTC']['quote']['KRW']['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.utc)
            fetch_time = dt_utc.astimezone(pytz.timezone('Asia/Seoul')).strftime('%H:%M')

        res_u = requests.get(url, headers=headers, params={'symbol': 'BTC,ETH', 'convert': 'USD'}, timeout=10, verify=False).json()
        if 'data' in res_u:
            btc_usd = f"{round(res_u['data']['BTC']['quote']['USD']['price'] / 1000, 1)}K"
            eth_usd = f"{round(res_u['data']['ETH']['quote']['USD']['price'] / 1000, 1)}K"
    except Exception: pass

    return (
        f"📊 <b>오늘의 가격 : 코인마켓캡 {fetch_time} 기준</b>\n"
        f"🟡 비트코인: ₩{btc_krw} ({btc_usd})\n"
        f"⚪ 이더리움: ₩{eth_krw} ({eth_usd})\n\n"
        f"📚 자사 기사 ➡️ 파트너사 기사 ➡️ 업계 전반\n"
        f"----------------------------\n\n"
    )

def is_duplicate(title, seen_title_sets):
    new_words = clean_text(title)
    if not new_words: return False
    for seen_words in seen_title_sets:
        if len(new_words & seen_words) / len(new_words) >= 0.4: return True
    return False

def format_pub_date_only(pub_date_str):
    try:
        dt = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %Z')
        kst = pytz.timezone('Asia/Seoul')
        dt_kst = dt.replace(tzinfo=pytz.utc).astimezone(kst)
        return dt_kst.strftime(f'%m/%d({DAYS_KR[dt_kst.weekday()]}) %H:%M'), dt_kst
    except Exception: return "시간 확인 불가", None


# ============================================================
# Google News URL 디코딩 (외부 패키지 없이 자체 구현)
# ============================================================

def _decode_base64_url(article_id):
    """
    구형 Google News URL: base64 내부에 원본 URL이 직접 인코딩된 경우.
    CBMi... 로 시작하고 디코딩하면 http로 시작하는 URL이 보이는 케이스.
    """
    try:
        padded = article_id + '=' * (4 - len(article_id) % 4) if len(article_id) % 4 else article_id
        decoded = base64.urlsafe_b64decode(padded)
        
        # prefix bytes 제거 (보통 0x08, 0x13, 0x22)
        prefix = bytes([0x08, 0x13, 0x22])
        if decoded.startswith(prefix):
            decoded = decoded[len(prefix):]
        
        # 첫 바이트가 길이 정보
        length = decoded[0]
        if length >= 0x80:
            url_bytes = decoded[2:length+2]
        else:
            url_bytes = decoded[1:length+1]
        
        url_str = url_bytes.decode('utf-8', errors='ignore')
        
        # 유효한 URL인지 확인 (http로 시작해야 함)
        if url_str.startswith('http'):
            return url_str
    except Exception:
        pass
    return None

def _decode_via_batchexecute(article_id):
    """
    신형 Google News URL: AU_yqL... 로 시작하여 batchexecute API 호출이 필요한 경우.
    1단계: news.google.com/articles/{id} 페이지에서 signature, timestamp 추출
    2단계: batchexecute API로 원본 URL 디코딩
    """
    headers_common = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }
    
    # 1단계: signature, timestamp 가져오기
    signature, timestamp = None, None
    for url_template in [
        f"https://news.google.com/articles/{article_id}",
        f"https://news.google.com/rss/articles/{article_id}",
    ]:
        try:
            resp = requests.get(url_template, headers=headers_common, timeout=15, verify=False)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # c-wiz > div 에서 data-n-a-sg, data-n-a-ts 추출
            div = soup.select_one('c-wiz > div[data-n-a-sg]')
            if div:
                signature = div.get('data-n-a-sg')
                timestamp = div.get('data-n-a-ts')
                break
        except Exception:
            continue
    
    if not signature or not timestamp:
        return None
    
    # 2단계: batchexecute API 호출
    try:
        payload = [
            "Fbv4je",
            f'["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],"{article_id}",{timestamp},"{signature}"]',
        ]
        
        req_headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'User-Agent': headers_common['User-Agent'],
        }
        
        resp = requests.post(
            'https://news.google.com/_/DotsSplashUi/data/batchexecute',
            headers=req_headers,
            data=f'f.req={quote(json.dumps([[payload]]))}',
            timeout=15,
            verify=False,
        )
        resp.raise_for_status()
        
        # 응답 파싱: 두 번째 빈 줄 이후의 JSON에서 URL 추출
        parsed = json.loads(resp.text.split('\n\n')[1])[:-2]
        decoded_url = json.loads(parsed[0][2])[1]
        
        if decoded_url and decoded_url.startswith('http'):
            return decoded_url
    except Exception:
        pass
    
    return None

def get_original_url(google_url):
    """
    Google News 링크 → 원본 기사 URL 변환.
    1) base64 직접 디코딩 시도 (구형 URL)
    2) 실패 시 batchexecute API 호출 (신형 URL)
    3) 모두 실패 시 원래 URL 반환
    """
    try:
        parsed = urlparse(google_url)
        if parsed.hostname != 'news.google.com':
            return google_url
        
        path_parts = parsed.path.split('/')
        # /rss/articles/XXXX 또는 /articles/XXXX 형태에서 article_id 추출
        article_id = None
        for i, part in enumerate(path_parts):
            if part in ('articles', 'read') and i + 1 < len(path_parts):
                article_id = path_parts[i + 1]
                break
        
        if not article_id:
            return google_url
        
        # query string 제거 (예: ?oc=5)
        if '?' in article_id:
            article_id = article_id.split('?')[0]
        
        # 방법 1: base64 직접 디코딩 (구형)
        direct_url = _decode_base64_url(article_id)
        if direct_url:
            return direct_url
        
        # 방법 2: batchexecute API (신형 - AU_yqL 등)
        api_url = _decode_via_batchexecute(article_id)
        if api_url:
            return api_url
        
    except Exception:
        pass
    
    return google_url


# ============================================================

def get_news():
    """뉴스 수집"""
    categories = {"자사 기사": [], "업계 전반": [], "파트너사 기사": []}
    global_seen_sets = []
    
    search_map = [
        ("자사", MY_COMPANY_KEYWORDS, "자사 기사", False, 1),
        ("가상자산", ["스테이블코인", "STO", "토큰증권","업비트","빗썸","코인원","코빗","고팍스", "가상자산", "디지털 자산",
        "디지털자산법", "코인거래소", "가상자산 규제", "커스터디", "암호화폐","비트코인", "비트코인 현물 ETF", "비트코인 채굴"], "업계 전반", True, 20),
        ("파트너사 기사", ["트래블룰 코드","트래블룰 CODE","트래블룰 기업 코드","쟁글","'쟁글'","체이널리시스","람다256","'람다256'","DAXA","한국핀테크산업협회"], "파트너사 기사", True, 5)
    ]

    for label, keywords, cat_name, is_24h, limit in search_map:
        query = " OR ".join(f'"{kw}"' for kw in keywords)
        url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
        if is_24h: url += "+when:1d"
        
        try:
            res = requests.get(url, verify=False, timeout=15)
            items = BeautifulSoup(res.content, 'xml').find_all('item')
            count = 0
            for item in items:
                if count >= limit: break
                
                title_raw = item.title.text
                if is_duplicate(title_raw, global_seen_sets): continue
                
                article_time_str, dt_kst = format_pub_date_only(item.pubDate.text if item.pubDate else "")
                if cat_name == "업계 전반" and dt_kst:
                    if (datetime.now(pytz.timezone('Asia/Seoul')) - dt_kst).total_seconds() > 172800:
                        continue

                # 구글 링크 -> 원본 링크 변환
                original_url = get_original_url(item.link.text)
                
                title = html.escape(title_raw)
                source = html.escape(item.source.text) if item.source else "뉴스"
                
                # 제목 아래에 URL 배치 포맷
                formatted_item = f"▲ {title} - {source} ({article_time_str})\n{original_url}"
                categories[cat_name].append(formatted_item)
                
                global_seen_sets.append(clean_text(title_raw))
                count += 1
                
                # Google 429 방지를 위한 짧은 딜레이
                time.sleep(0.3)
        except Exception: pass
        
    return categories

def send_telegram(market_data, categories):
    header = f"<b>[{get_korean_date()} 뉴스클리핑]</b>\n\n"
    messages, current_msg = [], header + market_data
    order = ["자사 기사", "파트너사 기사", "업계 전반"]
    
    for cat_name in order:
        news_list = categories.get(cat_name)
        if not news_list: continue
        cat_header = f"<b>✅ {cat_name}</b>\n\n"
        
        if len(current_msg) + len(cat_header) > 4000:
            messages.append(current_msg); current_msg = "<b>(계속)</b>\n\n" + cat_header
        else: current_msg += cat_header

        for item in news_list:
            item_text = item + "\n\n\n"
            if len(current_msg) + len(item_text) > 4000:
                messages.append(current_msg); current_msg = "<b>(계속)</b>\n\n" + item_text
            else: current_msg += item_text

    if current_msg.strip(): messages.append(current_msg)
    
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN.strip()}/sendMessage"
    
    for msg in messages:
        if msg.count("<b>") > msg.count("</b>"): msg += "</b>"
        requests.post(send_url, json={
            "chat_id": CHAT_ID.strip(), 
            "text": msg, 
            "parse_mode": "HTML", 
            "disable_web_page_preview": True
        }, verify=False)

if __name__ == "__main__":
    send_telegram(get_market_data(), get_news())