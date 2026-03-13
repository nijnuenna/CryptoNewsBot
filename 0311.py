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
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
CMC_API_KEY = os.environ.get('CMC_API_KEY')

# 키워드 및 요일 설정
MY_COMPANY_KEYWORDS = ["포블", "포블게이트", "FOBL"] 
DAYS_KR = ['월', '화', '수', '목', '금', '토', '일']

# ============================================================
# 매체 품질 관리
# ============================================================

# 1순위: 주요 경제/금융 매체 (업계 관계자가 신뢰하는 매체)
TIER1_SOURCES = [
    "연합뉴스", "연합인포맥스", "한국경제", "매일경제", "서울경제", "머니투데이",
    "이데일리", "파이낸셜뉴스", "헤럴드경제", "아시아경제", "뉴스1", "뉴시스",
    "조선비즈", "중앙일보", "조선일보", "동아일보", "한겨레", "경향신문",
    "KBS", "MBC", "SBS", "JTBC", "YTN", "채널A",
    "블룸버그", "로이터", "Reuters", "Bloomberg",
]

# 2순위: IT/블록체인 전문 매체
TIER2_SOURCES = [
    "코인데스크", "코인텔레그래프", "블록미디어", "디지털애셋", "토큰포스트",
    "디지털투데이", "지디넷코리아", "ZDNet", "전자신문", "디지털타임스",
    "테크M", "바이라인네트워크", "IT조선", "더블록", "The Block",
    "비인크립토", "코인니스", "데일리블록체인",
    "딜사이트", "비즈니스포스트", "뉴스토마토", "이코노미스트",
    "한국블록체인뉴스", "블록체인투데이", "디센터",
]

# 제외: 포털 재배포, 블로그형, 저품질 매체
EXCLUDED_SOURCES = [
    "v.daum.net", "blog.naver", "tistory", "brunch",
    "post.naver", "youtube.com", "n.news.naver",
]

# 저품질 기사 제목 패턴 (단순 시세 나열, 광고성, 낚시성)
LOW_QUALITY_PATTERNS = [
    r"^비트코인\s*\d+만\s*원",           # "비트코인 1억210만 원대 하락" 같은 단순 시세
    r"오늘\s*시세",                        # "비트코인 오늘 시세"
    r"^\[속보\]",                          # 속보 태그 (보통 한줄짜리)
    r"^(코인|가상자산)\s*시세",
    r"실시간\s*(시세|가격)",
    r"^\[광고\]",
    r"^\[후원\]",
]


def clean_text(text):
    text = re.sub(r'\[.*?\]|\(.*?\)', '', text)
    text = re.sub(r'[^가-힣a-zA-Z0-9\s]', ' ', text)
    return set([w for w in text.split() if len(w) >= 2])

def get_korean_date():
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    return f"{now.month}/{now.day}({DAYS_KR[now.weekday()]})"

def get_daily_quote():
    """한국어 명언 API에서 랜덤 명언 추출"""
    try:
        resp = requests.get(
            "https://korean-advice-open-api.vercel.app/api/advice",
            timeout=10,
            verify=False
        )
        resp.raise_for_status()
        data = resp.json()
        message = data.get("message", "")
        author = data.get("author", "")
        profile = data.get("authorProfile", "")
        if message and author:
            if profile:
                return f'"{message}" - {author} ({profile})'
            return f'"{message}" - {author}'
    except Exception:
        pass
    
    try:
        resp = requests.get(
            "https://api.sobabear.com/happiness/random-quote",
            timeout=10,
            verify=False
        )
        resp.raise_for_status()
        data = resp.json()
        quote_data = data.get("data", {})
        content = quote_data.get("content", "")
        author = quote_data.get("author", "")
        if content and author:
            return f'"{content}" - {author}'
    except Exception:
        pass
    
    return "추출 실패"

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

    quote_text = get_daily_quote()

    return (
        f"📊 <b>오늘의 가격 : 코인마켓캡 {fetch_time} 기준</b>\n"
        f"🟡 비트코인: ₩{btc_krw} ({btc_usd})\n"
        f"⚪ 이더리움: ₩{eth_krw} ({eth_usd})\n"
        f"💬 오늘의 명언 : {quote_text}\n\n"
    )

def is_duplicate(title, seen_title_sets):
    new_words = clean_text(title)
    if not new_words: return False
    for seen_words in seen_title_sets:
        if not seen_words: continue
        overlap = len(new_words & seen_words)
        ratio_a = overlap / len(new_words)
        ratio_b = overlap / len(seen_words)
        if max(ratio_a, ratio_b) >= 0.25: return True
        if overlap >= 2: return True
    return False

def is_low_quality_title(title):
    """저품질 기사 제목 패턴 필터"""
    for pattern in LOW_QUALITY_PATTERNS:
        if re.search(pattern, title):
            return True
    return False

def get_source_tier(source_name):
    """매체 등급 반환: 1(주요) > 2(전문) > 3(기타)"""
    for name in TIER1_SOURCES:
        if name in source_name or source_name in name:
            return 1
    for name in TIER2_SOURCES:
        if name in source_name or source_name in name:
            return 2
    return 3

def is_excluded_source(source_name, article_url):
    """제외 대상 매체/URL 체크"""
    for excluded in EXCLUDED_SOURCES:
        if excluded in source_name or excluded in article_url:
            return True
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
    try:
        padded = article_id + '=' * (4 - len(article_id) % 4) if len(article_id) % 4 else article_id
        decoded = base64.urlsafe_b64decode(padded)
        prefix = bytes([0x08, 0x13, 0x22])
        if decoded.startswith(prefix):
            decoded = decoded[len(prefix):]
        length = decoded[0]
        if length >= 0x80:
            url_bytes = decoded[2:length+2]
        else:
            url_bytes = decoded[1:length+1]
        url_str = url_bytes.decode('utf-8', errors='ignore')
        if url_str.startswith('http'):
            return url_str
    except Exception:
        pass
    return None

def _decode_via_batchexecute(article_id):
    headers_common = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }
    signature, timestamp = None, None
    for url_template in [
        f"https://news.google.com/articles/{article_id}",
        f"https://news.google.com/rss/articles/{article_id}",
    ]:
        try:
            resp = requests.get(url_template, headers=headers_common, timeout=15, verify=False)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            div = soup.select_one('c-wiz > div[data-n-a-sg]')
            if div:
                signature = div.get('data-n-a-sg')
                timestamp = div.get('data-n-a-ts')
                break
        except Exception:
            continue
    if not signature or not timestamp:
        return None
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
        parsed = json.loads(resp.text.split('\n\n')[1])[:-2]
        decoded_url = json.loads(parsed[0][2])[1]
        if decoded_url and decoded_url.startswith('http'):
            return decoded_url
    except Exception:
        pass
    return None

def get_original_url(google_url):
    try:
        parsed = urlparse(google_url)
        if parsed.hostname != 'news.google.com':
            return google_url
        path_parts = parsed.path.split('/')
        article_id = None
        for i, part in enumerate(path_parts):
            if part in ('articles', 'read') and i + 1 < len(path_parts):
                article_id = path_parts[i + 1]
                break
        if not article_id:
            return google_url
        if '?' in article_id:
            article_id = article_id.split('?')[0]
        direct_url = _decode_base64_url(article_id)
        if direct_url:
            return direct_url
        api_url = _decode_via_batchexecute(article_id)
        if api_url:
            return api_url
    except Exception:
        pass
    return google_url


# ============================================================

def get_news():
    """뉴스 수집 — 매체 품질 필터링 및 등급별 정렬 적용"""
    categories = {"자사 기사": [], "업계 전반": [], "파트너사 기사": []}
    global_seen_sets = []
    
    search_map = [
        ("자사", MY_COMPANY_KEYWORDS, "자사 기사", False, 1),
        ("가상자산", ["스테이블코인", "STO", "토큰증권", "가상자산", "디지털 자산",
        "디지털자산법", "코인거래소", "가상자산 규제", "커스터디", "암호화폐","비트코인", "비트코인 현물 ETF", "비트코인 채굴"], "업계 전반", True, 20),
    ]

    # 파트너사: 회사별 검색 키워드 (각 회사당 최신 1개씩)
    PARTNER_MAP = [
        ("트래블룰 코드", ["트래블룰 솔루션 코드", "트래블룰 솔루션 CODE", "트래블룰 솔루션사 코드"]),
        ("쟁글", ["쟁글"]),
        ("체이널리시스", ["체이널리시스","chainalysis"]),
        ("람다256", ["람다256"]),
        ("DAXA", ["닥사", "DAXA"]),
        ("한국핀테크산업협회", ["한국핀테크산업협회", "핀산협"]),
    ]

    EXCLUDE_KEYWORDS = ["업비트","두나무","빗썸","빗썸나눔","코인원","코빗","고팍스","스트리미"]

    for label, keywords, cat_name, is_24h, limit in search_map:
        query = " OR ".join(f'"{kw}"' for kw in keywords)
        url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
        if is_24h: url += "+when:1d"
        
        # 후보 기사를 먼저 모은 뒤, 등급순 정렬 후 limit개 선별
        candidates = []
        
        try:
            res = requests.get(url, verify=False, timeout=15)
            items = BeautifulSoup(res.content, 'xml').find_all('item')
            
            for item in items:
                title_raw = item.title.text
                source_raw = item.source.text if item.source else "뉴스"
                
                # 1. 중복 체크
                if is_duplicate(title_raw, global_seen_sets): continue

                # 2. 제외 키워드 (타 거래소)
                if any(kw in title_raw for kw in EXCLUDE_KEYWORDS): continue
                
                # 3. 저품질 제목 필터 (업계 전반만)
                if cat_name == "업계 전반" and is_low_quality_title(title_raw): continue

                # 4. 시간 필터
                article_time_str, dt_kst = format_pub_date_only(item.pubDate.text if item.pubDate else "")
                if cat_name == "업계 전반" and dt_kst:
                    if (datetime.now(pytz.timezone('Asia/Seoul')) - dt_kst).total_seconds() > 172800:
                        continue

                # 5. 원본 URL 추출
                original_url = get_original_url(item.link.text)

                # 6. 제외 매체/URL 필터 (업계 전반만)
                if cat_name == "업계 전반" and is_excluded_source(source_raw, original_url): continue
                
                # 7. 매체 등급 판정 (업계 전반만 등급 적용, 나머지는 동일 등급)
                tier = get_source_tier(source_raw) if cat_name == "업계 전반" else 0
                
                # 제목 정리
                clean_title = title_raw
                if clean_title.endswith(f" - {source_raw}"):
                    clean_title = clean_title[: -len(f" - {source_raw}")]
                title = html.escape(clean_title)
                source = html.escape(source_raw)
                
                formatted_item = f"▲ {title} - {source} ({article_time_str})\n{original_url}"
                candidates.append((tier, formatted_item))
                
                global_seen_sets.append(clean_text(title_raw))
                
                time.sleep(0.3)
        except Exception: pass
        
        # 등급순 정렬 (1순위 매체 먼저) 후 limit개 선별
        candidates.sort(key=lambda x: x[0])
        for _, formatted_item in candidates[:limit]:
            categories[cat_name].append(formatted_item)
    
    # ============================================================
    # 파트너사 기사: 회사별 최신 1개씩 개별 검색
    # ============================================================
    for partner_name, partner_keywords in PARTNER_MAP:
        query = " OR ".join(f'"{kw}"' for kw in partner_keywords)
        url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
        
        found = False
        try:
            res = requests.get(url, verify=False, timeout=15)
            items = BeautifulSoup(res.content, 'xml').find_all('item')
            
            for item in items:
                if found: break
                
                title_raw = item.title.text
                
                article_time_str, dt_kst = format_pub_date_only(item.pubDate.text if item.pubDate else "")
                original_url = get_original_url(item.link.text)
                
                source_raw = item.source.text if item.source else "뉴스"
                clean_title = title_raw
                if clean_title.endswith(f" - {source_raw}"):
                    clean_title = clean_title[: -len(f" - {source_raw}")]
                title = html.escape(clean_title)
                source = html.escape(source_raw)
                
                formatted_item = f"▲ {title} - {source} ({article_time_str})\n{original_url}"
                categories["파트너사 기사"].append(formatted_item)
                
                global_seen_sets.append(clean_text(title_raw))
                found = True
                
                time.sleep(0.3)
        except Exception: pass
        
        # 검색 결과가 없어도 무조건 1줄 출력
        if not found:
            categories["파트너사 기사"].append(f"▲ {partner_name} - 최신 기사 없음")

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
