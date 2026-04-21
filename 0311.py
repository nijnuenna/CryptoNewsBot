import requests
import os
import urllib3
from datetime import datetime, timedelta
import pytz
import html as html_module
import re
import json
import time

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
CMC_API_KEY = os.environ.get('CMC_API_KEY')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
NAVER_CLIENT_ID = os.environ.get('NAVER_CLIENT_ID')
NAVER_CLIENT_SECRET = os.environ.get('NAVER_CLIENT_SECRET')

DAYS_KR = ['월', '화', '수', '목', '금', '토', '일']

# ============================================================
# 날짜 컷오프 설정
# ============================================================
OWN_COMPANY_MAX_DAYS = 30      # 자사 기사: 30일 이내만
PARTNER_MAX_DAYS = 60          # 파트너사: 60일 이내만 (소식 빈도 낮음)

# ============================================================
# 키워드 / 필터 설정
# ============================================================

MY_COMPANY_KEYWORDS = ["포블게이트", "포블", "FOBL"]

EXCLUDE_TITLE_KEYWORDS = [
    "업비트", "두나무", "빗썸", "코인원", "코빗", "고팍스", "스트리미",
    "보이스피싱", "피싱", "사기", "해킹", "랜섬웨어", "자금세탁",
    "범죄", "검거", "구속", "피해자", "피해액", "폰지", "먹튀",
    "시니어", "리빙", "요양", "아파트", "분양", "부동산", "재건축",
    "골프", "야구", "축구", "농구", "배구",
    "드라마", "영화", "연예", "아이돌", "게임",
    "리플", "XRP", "솔라나", "이더리움", "트럼프",
    "코인 갱신 일지", "[크립토 브리핑]",
]

# 가격·변동 중심, 감성 과장, 광고성 등 '산업 흐름과 무관한' 제목 패턴
LOW_QUALITY_PATTERNS = [
    # 가격 변동 중심
    r"(급등|급락|폭등|폭락)",
    r"\d+\s*%\s*(상승|하락|반등|급등|급락)",
    r"\d+\s*만\s*원.*(돌파|붕괴|터치|회복)",
    r"\d+\s*(만|천)?\s*달러.*(돌파|붕괴|회복)",
    r"목표가",
    r"(바닥|저점)\s*(다졌|찍|탈출|확인)",

    # 실시간 시세
    r"오늘\s*시세",
    r"실시간\s*(시세|가격)",
    r"^(코인|가상자산)\s*시세",
    r"^비트코인\s*\d+만\s*원",

    # 감성·과장
    r"(드디어|마침내).*(터졌|돌파|날았)",
    r"동반\s*랠리",
    r"불장|떡상",
    r"투심\s*살아",

    # 광고·속보·초기 투자 보도자료
    r"^\[속보\]",
    r"^\[광고\]",
    r"^\[후원\]",
    r"시드\s*라운드",
    r"시리즈\s*[A-D]",
    r"프리\s*시리즈",
    r"코인\s*갱신\s*일지",
    r"\[크립토\s*브리핑\]",
]

EXCLUDED_DOMAINS = [
    "contents.premium.naver.com",
]

TIER1_SOURCES = [
    "연합뉴스", "한국경제", "매일경제", "서울경제", "머니투데이",
    "이데일리", "파이낸셜뉴스", "이투데이", "블록미디어",
    "헤럴드경제", "아시아경제", "뉴스1", "뉴시스",
    "조선비즈", "중앙일보", "조선일보", "동아일보", "한겨레", "경향신문",
    "KBS", "MBC", "SBS", "SBS BIZ", "JTBC", "YTN", "채널A",
]

TIER2_SOURCES = [
    "코인데스크", "코인데스크코리아", "브릿지경제", "토큰포스트",
    "디지털투데이", "지디넷코리아", "전자신문", "디지털타임스",
    "더블록미디어", "비인크립토", "코인니스", "디센터",
    "코인리더스", "블루밍비트", "뉴스토마토", "딜사이트",
    "테크M", "한스경제", "the bell", "글로벌이코노믹", "블로터", "쿠키뉴스",
]

TOPIC_MAP = [
    ("정책·규제", ["규제", "법안", "통과", "국회", "금융위", "금감원", "금융당국", "가이드라인",
                  "제도", "입법", "법률", "시행령", "감독", "인가", "허가", "디지털자산기본법", "코인 과세", "가상자산 과세", "제도권 편입","코인거래소"]),
    ("스테이블코인", ["스테이블코인", "스테이블", "USDT", "USDC", "원화코인", "원화스테이블", "달러코인", "금가분리"]),
    ("STO·토큰증권", ["STO", "토큰증권", "증권형토큰", "조각투자", "토큰화", "RWA"]),
    ("비트코인·시장", ["비트코인", "이더리움", "ETF", "강세", "약세", "급등", "급락",
                     "상승", "하락", "반등", "매수", "매도", "채굴", "반감기"]),
    ("디지털자산", ["디지털자산", "디지털 자산", "가상자산", "암호화폐", "커스터디", "VASP"]),
    ("글로벌", ["미국", "SEC", "CFTC", "EU", "영국", "일본", "중국", "홍콩",
              "월가", "글로벌", "해외", "유럽", "트럼프"]),
    ("기업·산업", ["MOU", "협약", "파트너십", "투자", "인수", "상장", "IPO", "협업",
                 "서비스", "출시", "론칭"]),
]

COMPANY_NAMES = [
    "하나금융", "신한금융", "신한은행", "KB금융", "KB국민", "우리금융", "우리은행",
    "NH농협", "카카오", "네이버", "삼성", "SK", "LG", "현대", "롯데", "유안타증권", "블랙록", "모건스탠리",
]

PARTNER_MAP = [
    ("트래블룰 코드", ["코드 VASP","CodeVASP","트래블룰 솔루션 코드", "트래블룰 솔루션 CODE", "트래블룰 솔루션사 코드", "코드,"]),
    ("쟁글", ["쟁글", "Xangle"]),
    ("체이널리시스", ["체이널리시스", "Chainalysis"]),
    ("람다256", ["람다256"]),
    ("DAXA", ["닥사", "DAXA", "디지털자산거래소공동협의체"]),
    ("한국핀테크산업협회", ["한국핀테크산업협회", "핀산협"]),
    ("코넛", ["대체불가능회사", "코넛", "코넛코인", "코넛 코인", "CONUT"]),
    ("타이거리서치", ["타이거리서치","타이거 리서치"]),
]

# ============================================================
# 네이버 뉴스 검색 API
# ============================================================

DOMAIN_MAP = {
    "yna.co.kr": "연합뉴스", "yonhapnews.co.kr": "연합뉴스",
    "hankyung.com": "한국경제", "mk.co.kr": "매일경제",
    "sedaily.com": "서울경제", "mt.co.kr": "머니투데이",
    "edaily.co.kr": "이데일리", "fnnews.com": "파이낸셜뉴스",
    "etoday.co.kr": "이투데이", "viva100.com": "브릿지경제",
    "heraldcorp.com": "헤럴드경제", "asiae.co.kr": "아시아경제",
    "news1.kr": "뉴스1", "newsis.com": "뉴시스",
    "biz.chosun.com": "조선비즈", "joongang.co.kr": "중앙일보",
    "chosun.com": "조선일보", "donga.com": "동아일보",
    "hani.co.kr": "한겨레", "khan.co.kr": "경향신문",
    "kbs.co.kr": "KBS", "imbc.com": "MBC",
    "sbs.co.kr": "SBS", "biz.sbs.co.kr": "SBS BIZ", "jtbc.co.kr": "JTBC",
    "ytn.co.kr": "YTN", "ichannela.com": "채널A",
    "coindesk.com": "코인데스크", "coindeskkorea.com": "코인데스크코리아",
    "blockmedia.co.kr": "블록미디어", "tokenpost.kr": "토큰포스트",
    "digitaltoday.co.kr": "디지털투데이", "zdnet.co.kr": "지디넷코리아",
    "etnews.com": "전자신문", "dt.co.kr": "디지털타임스",
    "theblockmedia.com": "더블록미디어", "beinews.net": "비인크립토",
    "coinnews.co.kr": "코인니스", "decenter.kr": "디센터",
    "bloomingbit.io": "블루밍비트", "newstomato.com": "뉴스토마토",
    "dealsite.co.kr": "딜사이트", "businesspost.co.kr": "비즈니스포스트",
    "coinreaders.com": "코인리더스", "khgames.co.kr": "경향게임스", "hansbiz.co.kr": "한스경제",
    "thebell.co.kr": "the bell", "pinpointnews.co.kr": "핀포인트뉴스",
    "techm.kr": "테크M", "newsdream.kr": "뉴스드림", "g-enews.com": "글로벌이코노믹",
    "bloter.net": "블로터", "kukinews.com": "쿠키뉴스",
}


def search_naver_news(query, display=100, sort="date"):
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        return []

    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID.strip(),
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET.strip(),
    }
    params = {"query": query, "display": display, "start": 1, "sort": sort}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15, verify=False)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get("items", []):
            title_clean = re.sub(r'<.*?>', '', item.get("title", ""))
            title_clean = html_module.unescape(title_clean)

            original_url = item.get("originallink", "").strip()
            if not original_url:
                original_url = item.get("link", "").strip()

            if any(d in original_url for d in EXCLUDED_DOMAINS):
                continue

            time_str, dt_kst = _parse_naver_date(item.get("pubDate", ""))
            source = _extract_source(original_url)

            results.append({
                "title_raw": title_clean,
                "original_url": original_url,
                "source_raw": source,
                "time_str": time_str,
                "dt_kst": dt_kst,
            })
        return results
    except Exception as e:
        print(f"[ERROR] 네이버 검색 실패 ({query}): {e}")
        return []


def _parse_naver_date(pub_date_str):
    try:
        dt = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z')
        dt_kst = dt.astimezone(pytz.timezone('Asia/Seoul'))
        return dt_kst.strftime(f'%m/%d({DAYS_KR[dt_kst.weekday()]}) %H:%M'), dt_kst
    except Exception:
        return "시간 확인 불가", None


def _extract_source(url):
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).hostname or ""
        domain = domain.replace("www.", "").replace("m.", "")
        for key, name in DOMAIN_MAP.items():
            if key in domain:
                return name
        if domain.endswith(".co.kr"):
            return domain.replace(".co.kr", "").split(".")[-1]
        parts = domain.split(".")
        if len(parts) >= 2:
            return parts[-2]
    except Exception:
        pass
    return "뉴스"


# ============================================================
# 코드 기반 필터링 함수
# ============================================================

def _within_days(dt_kst, days):
    """dt_kst가 현재 기준 days일 이내인지"""
    if not dt_kst:
        return False
    now_kst = datetime.now(pytz.timezone('Asia/Seoul'))
    return (now_kst - dt_kst).days <= days


def clean_text(text):
    text = re.sub(r'\[.*?\]|\(.*?\)', '', text)
    text = re.sub(r'[^가-힣a-zA-Z0-9\s]', ' ', text)
    return set([w for w in text.split() if len(w) >= 2])


def is_duplicate(title, seen_title_sets):
    new_words = clean_text(title)
    if not new_words or len(new_words) < 3:
        return False
    for seen_words in seen_title_sets:
        if not seen_words or len(seen_words) < 3:
            continue
        overlap = len(new_words & seen_words)
        ratio = overlap / min(len(new_words), len(seen_words))
        if ratio >= 0.45:
            return True
    return False


def is_low_quality(title):
    for pattern in LOW_QUALITY_PATTERNS:
        if re.search(pattern, title):
            return True
    return False


def get_source_tier(source_name):
    for name in TIER1_SOURCES:
        if name in source_name or source_name in name:
            return 1
    for name in TIER2_SOURCES:
        if name in source_name or source_name in name:
            return 2
    return 3


def get_topic(title):
    title_lower = title.lower()
    for idx, (topic_name, keywords) in enumerate(TOPIC_MAP):
        for kw in keywords:
            if kw.lower() in title_lower:
                return idx, topic_name
    return len(TOPIC_MAP), "기타"


def extract_company(title):
    for name in COMPANY_NAMES:
        if name in title:
            return name
    return None


def _title_has_keyword(title, kw):
    """제목에서 키워드가 주어(주인공) 위치에 있는지 체크"""
    title_no_tag = re.sub(r'^\[.*?\]\s*', '', title.strip()).lower()
    kw_lower = kw.lower()

    # 인물 활동 기사 제외 (대표/CEO 출연, 인터뷰 등)
    PERSON_PATTERNS = ["대표 출연", "ceo 출연", "대표 인터뷰", "ceo 인터뷰", "대표가 말하", "대표 말"]
    if any(p in title_no_tag for p in PERSON_PATTERNS):
        return False

    # 구분자(···, …, |, -, :) 기준으로 세그먼트 분리
    segments = re.split(r'[···…|\-:]', title_no_tag)
    return any(seg.strip().startswith(kw_lower) for seg in segments)


# ============================================================
# Groq API (Llama) — 최종 선별용
# ============================================================

LLM_PROMPT = """당신은 한국 가상자산 거래소 '포블게이트'의 아침 뉴스 브리핑 에디터입니다.
거래소 임직원이 출근길에 읽을 10~15분짜리 브리핑을 편집하는 것이 목표입니다.

## 입력
아래 목록은 코드로 1차 필터링된 가상자산·블록체인 관련 기사입니다.
형식: [번호] (토픽) 제목 - 매체명 (발행일시)

## 당신의 임무
후보 기사 중 **진짜 의미있는 기사 15~20건**을 엄선합니다.
핵심 역할은 코드가 걸러내지 못한 **저품질·중복·피상적 기사를 배제**하는 것입니다.
애매하면 배제가 기본값입니다. 15건이 안 되더라도 품질이 우선입니다.

## 편집 철학
"디지털자산·가상자산이 글로벌하게 제도권에 편입되는 과정"을 잘 드러내는 기사를 우선합니다.
임직원이 산업 전반의 흐름·정책·제도 변화를 한 번에 파악할 수 있어야 합니다.

## 반드시 선택 (우선순위순)
1. 국내 정책·규제 확정 기사 (법안 통과, 시행령 확정, 금융당국·국회 공식 발표)
2. 글로벌 제도권 편입 (ETF 승인, 월가·블랙록·JP모건 등 대형 금융사 진입)
3. 스테이블코인·STO·RWA·토큰증권 제도화 진전
4. 주요 기업의 디지털자산 사업 전략 (하나·신한·KB·네이버·카카오·삼성 등)
5. 비트코인·이더리움 심층 분석 (시세가 아닌 배경·맥락·구조 분석)

## 반드시 제외 (하나라도 해당하면 배제)
- **가격 중심**: "X% 급등/급락", "X만원/달러 돌파", 목표가, 차트·기술적 분석, 시황 요약
- **중복 기사**: 같은 사건을 여러 매체가 다루면 **매체 등급이 높은 1개만** 선택
- **소규모 보도자료**: 작은 회사·프로젝트의 단순 출시·MOU·업무협약·리브랜딩
- **인물 이벤트**: "XX 대표/CEO 출연/강연/인터뷰", 개인의 컨퍼런스 발언
- **추측·전망성 단독 기사**: "~될 수도", "~가능성", "~우려", "~지연되나", 전문가 개인 예상
- **비관련 기사**: 블록체인과 직접 관련 없는 AI·일반 핀테크·부동산·증권 일반
- **분위기만 전하는 기사**: "투심 살아나", "회복세", "심리 개선", "관망세"
- **특정 알트코인 시세·가격 전망** 기사

## 중복 판단 기준
제목의 핵심 명사·동사가 겹치면 중복으로 판단합니다. 표현이 달라도 같은 사건이면 1개만.
예시)
- "비트코인 10만달러 돌파" vs "BTC 10만불 시대" → 동일 이슈, 매체 등급 높은 1개
- "금융위 가상자산 법안 발표" vs "金委, 디지털자산법 공개" → 동일 이슈, 1개
매체 등급이 같으면 더 구체적·심층적인 제목을 선택하세요.

## 토픽 균형
가능하면 여러 토픽에 걸쳐 골고루 선택하세요. 한 토픽이 전체의 절반을 넘지 않도록 합니다.
단, 정책·규제 기사는 예외적으로 많이 담아도 괜찮습니다.

## 응답 형식
JSON 배열만 출력. 설명·코드펜스·추가 텍스트 일절 없음.
[1, 5, 12, 23, 45]

## 기사 목록
{article_list}
"""


def _call_groq(prompt, max_retries=3):
    if not GROQ_API_KEY:
        print("[WARN] GROQ_API_KEY 미설정")
        return None

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY.strip()}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 2048,
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30, verify=False)
            if resp.status_code == 429:
                wait = attempt * 30
                print(f"[WARN] Groq 429 — {wait}초 대기 ({attempt}/{max_retries})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            text = data["choices"][0]["message"]["content"]

            json_match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
            if json_match:
                text = json_match.group(1)
            else:
                json_match = re.search(r'\[.*\]', text, re.DOTALL)
                if json_match:
                    text = json_match.group(0)

            return json.loads(text)
        except Exception as e:
            print(f"[ERROR] Groq 실패 (시도 {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                time.sleep(attempt * 10)

    print("[ERROR] Groq 최종 실패")
    return None


# ============================================================
# 유틸리티
# ============================================================

def get_korean_date():
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    return f"{now.month}/{now.day}({DAYS_KR[now.weekday()]})"


def get_daily_quote():
    """이전에 사용된 명언과 중복되지 않는 명언 추출"""
    QUOTE_FILE = "used_quotes.txt"

    # 기존 기록 읽기
    used = set()
    try:
        with open(QUOTE_FILE, "r", encoding="utf-8") as f:
            used = set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        pass

    # API 여러 번 호출해서 후보 수집
    candidates = []
    for _ in range(10):
        try:
            resp = requests.get(
                "https://korean-advice-open-api.vercel.app/api/advice",
                timeout=10, verify=False
            )
            resp.raise_for_status()
            data = resp.json()
            message = data.get("message", "")
            author = data.get("author", "")
            profile = data.get("authorProfile", "")
            if message and author:
                text = f'{message} - {author}, {profile}' if profile else f'{message} - {author}'
                if text not in candidates:
                    candidates.append(text)
        except Exception:
            pass
        time.sleep(0.3)

    # 기록에 없는 명언 우선 선택
    new_quotes = [q for q in candidates if q not in used]
    selected = new_quotes[0] if new_quotes else (candidates[0] if candidates else "추출 실패")

    # 기록에 추가
    if selected != "추출 실패":
        try:
            with open(QUOTE_FILE, "a", encoding="utf-8") as f:
                f.write(selected + "\n")
        except Exception:
            pass

    return selected


def get_market_data():
    btc_krw, btc_usd, eth_krw, eth_usd, fetch_time = "연결 실패", "연결 실패", "연결 실패", "연결 실패", "시간 미확인"
    if not CMC_API_KEY:
        return "📊 <b>오늘의 가격</b>\n⚠️ CMC_API_KEY 미설정\n\n"
    try:
        api_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
        headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY.strip()}

        res_k = requests.get(api_url, headers=headers, params={'symbol': 'BTC,ETH', 'convert': 'KRW'}, timeout=10, verify=False).json()
        if 'data' in res_k:
            btc_krw = f"{res_k['data']['BTC']['quote']['KRW']['price']:,.0f}"
            eth_krw = f"{res_k['data']['ETH']['quote']['KRW']['price']:,.0f}"
            dt_utc = datetime.strptime(res_k['data']['BTC']['quote']['KRW']['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.utc)
            fetch_time = dt_utc.astimezone(pytz.timezone('Asia/Seoul')).strftime('%H:%M')

        res_u = requests.get(api_url, headers=headers, params={'symbol': 'BTC,ETH', 'convert': 'USD'}, timeout=10, verify=False).json()
        if 'data' in res_u:
            btc_usd = f"{round(res_u['data']['BTC']['quote']['USD']['price'] / 1000, 1)}K"
            eth_usd = f"{round(res_u['data']['ETH']['quote']['USD']['price'] / 1000, 1)}K"
    except Exception:
        pass

    quote_text = get_daily_quote()
    return (
        f"📊 <b>오늘의 가격 : 코인마켓캡 {fetch_time} 기준</b>\n"
        f"🟡 비트코인: ₩{btc_krw} ({btc_usd})\n"
        f"⚪ 이더리움: ₩{eth_krw} ({eth_usd})\n\n"
        f"💬 오늘의 명언 : {quote_text}\n"
    )

# ============================================================
# 뉴스 수집 — 코드 필터링 → LLM 최종 선별
# ============================================================

TOPIC_ORDER = [t[0] for t in TOPIC_MAP] + ["기타"]


def get_news():
    categories = {"자사 기사": [], "업계 전반": [], "파트너사 기사": []}
    global_seen_sets = []
    now_kst = datetime.now(pytz.timezone('Asia/Seoul'))
    yesterday_noon = (now_kst - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)

    # ============================================================
    # 1. 자사 기사 — 최근 N일 이내 + 최신순 우선
    # ============================================================
    my_candidates = []
    for kw in MY_COMPANY_KEYWORDS:
        for sort_type in ["date", "sim"]:
            results = search_naver_news(kw, display=100, sort=sort_type)
            for r in results:
                if not _title_has_keyword(r["title_raw"], kw):
                    continue
                # 날짜 컷오프: 너무 오래된 기사는 배제
                if not _within_days(r["dt_kst"], OWN_COMPANY_MAX_DAYS):
                    continue
                if is_duplicate(r["title_raw"], [clean_text(c["title_raw"]) for c in my_candidates]):
                    continue
                r["tier"] = get_source_tier(r["source_raw"])
                my_candidates.append(r)
        time.sleep(0.1)

    if my_candidates:
        # 정렬: 최신순 우선(1차) → 동일 날짜면 매체 등급(2차)
        my_candidates.sort(key=lambda x: (
            -(x["dt_kst"].timestamp() if x["dt_kst"] else 0),
            x["tier"],
        ))
        r = my_candidates[0]
        title = html_module.escape(r["title_raw"])
        source = html_module.escape(r["source_raw"])
        categories["자사 기사"].append(f"▲ {title} - {source} ({r['time_str']})\n{r['original_url']}")
        global_seen_sets.append(clean_text(r["title_raw"]))
    else:
        categories["자사 기사"].append(f"▲ 포블게이트 - 최근 {OWN_COMPANY_MAX_DAYS}일 내 기사 없음")

    print(f"[LOG] 자사 기사: {len(categories['자사 기사'])}건 (후보 {len(my_candidates)}건)")

    # ============================================================
    # 2. 업계 전반 — 수집 → 코드 필터링 → LLM 최종 선별
    # ============================================================
    industry_queries = [
        "가상자산", "비트코인", "스테이블코인", "토큰증권", "디지털자산",
        "가상자산 규제", "블록체인 금융", "RWA 토큰화",
    ]

    raw_all = []
    for q in industry_queries:
        results = search_naver_news(q, display=100, sort="date")
        before = len(raw_all)
        for r in results:
            if any(kw in r["title_raw"] for kw in EXCLUDE_TITLE_KEYWORDS):
                continue
            # 전날 12시 이후 기사만
            if r["dt_kst"] and r["dt_kst"] < yesterday_noon:
                continue
            if is_low_quality(r["title_raw"]):
                continue
            if is_duplicate(r["title_raw"], global_seen_sets):
                continue
            raw_all.append(r)
            global_seen_sets.append(clean_text(r["title_raw"]))
        print(f"[LOG] 수집 '{q}': {len(results)}건 → 신규 {len(raw_all)-before}건 (누적 {len(raw_all)}건)")
        time.sleep(0.1)

    print(f"[LOG] 수집 후 총: {len(raw_all)}건")

    for r in raw_all:
        r["topic_idx"], r["topic_name"] = get_topic(r["title_raw"])
        r["tier"] = get_source_tier(r["source_raw"])

    raw_all.sort(key=lambda x: (
        x["topic_idx"],
        x["tier"],
        -(x["dt_kst"].timestamp() if x["dt_kst"] else 0),
    ))

    seen_companies = set()
    filtered = []
    for r in raw_all:
        company = extract_company(r["title_raw"])
        if company:
            if company in seen_companies:
                continue
            seen_companies.add(company)
        filtered.append(r)

    print(f"[LOG] 코드 필터 후: {len(filtered)}건 (기업 중복 제거 {len(raw_all)-len(filtered)}건)")

    # LLM에 전달할 풀 확대 (60 → 80): 선택지 넓혀서 LLM이 더 좋은 기사 고르게
    llm_pool = filtered[:80]

    if llm_pool:
        article_list_text = "\n".join(
            f"[{i+1}] ({c['topic_name']}) {c['title_raw']} - {c['source_raw']} ({c['time_str']})"
            for i, c in enumerate(llm_pool)
        )
        print(f"[LOG] LLM에 전달: {len(llm_pool)}건")

        llm_result = _call_groq(LLM_PROMPT.replace("{article_list}", article_list_text))

        if llm_result and isinstance(llm_result, list):
            selected_ids = [x for x in llm_result if isinstance(x, int) and 1 <= x <= len(llm_pool)]
            print(f"[LOG] LLM 선택: {len(selected_ids)}건 → {selected_ids}")

            # 토픽·매체 순으로 재정렬해서 읽기 좋게
            selected_items = [llm_pool[aid - 1] for aid in selected_ids]
            selected_items.sort(key=lambda x: (
                x["topic_idx"],
                x["tier"],
                -(x["dt_kst"].timestamp() if x["dt_kst"] else 0),
            ))

            for c in selected_items:
                title = html_module.escape(c["title_raw"])
                source = html_module.escape(c["source_raw"])
                categories["업계 전반"].append(f"▲ {title} - {source} ({c['time_str']})\n{c['original_url']}")
        else:
            print("[WARN] LLM 실패 — 코드 필터 결과 상위 20건 fallback")
            for c in filtered[:20]:
                title = html_module.escape(c["title_raw"])
                source = html_module.escape(c["source_raw"])
                categories["업계 전반"].append(f"▲ {title} - {source} ({c['time_str']})\n{c['original_url']}")

    print(f"[LOG] 업계 전반 최종: {len(categories['업계 전반'])}건")

    # ============================================================
    # 3. 파트너사 — 회사별 최근 N일 이내 최신 기사 1개
    # ============================================================
    for partner_name, partner_keywords in PARTNER_MAP:
        best = None

        for kw in partner_keywords:
            for sort_type in ["date", "sim"]:
                results = search_naver_news(kw, display=100, sort=sort_type)
                for r in results:
                    if not _title_has_keyword(r["title_raw"], kw):
                        continue
                    # 날짜 컷오프: 너무 오래된 기사는 배제
                    if not _within_days(r["dt_kst"], PARTNER_MAX_DAYS):
                        continue
                    if best is None:
                        best = r
                    elif r["dt_kst"] and best["dt_kst"] and r["dt_kst"] > best["dt_kst"]:
                        best = r
            time.sleep(0.1)

        if best:
            title = html_module.escape(best["title_raw"])
            source = html_module.escape(best["source_raw"])
            categories["파트너사 기사"].append(f"▲ {title} - {source} ({best['time_str']})\n{best['original_url']}")
        else:
            categories["파트너사 기사"].append(f"▲ {partner_name} - 최근 {PARTNER_MAX_DAYS}일 내 기사 없음")

    print(f"[LOG] 파트너사 최종: {len(categories['파트너사 기사'])}건")
    return categories


# ============================================================
# 텔레그램 발송
# ============================================================

def send_telegram(market_data, categories):
    header = f"<b>[{get_korean_date()} 뉴스클리핑]</b>\n\n"
    messages, current_msg = [], header + market_data
    order = ["자사 기사", "파트너사 기사", "업계 전반"]

    for cat_name in order:
        news_list = categories.get(cat_name)
        if not news_list:
            continue
        cat_header = f"\n<b>✅ {cat_name}</b>\n\n"

        if len(current_msg) + len(cat_header) > 4000:
            messages.append(current_msg)
            current_msg = "<b>(계속)</b>\n\n" + cat_header
        else:
            current_msg += cat_header

        for item in news_list:
            item_text = item + "\n\n\n"
            if len(current_msg) + len(item_text) > 4000:
                messages.append(current_msg)
                current_msg = "<b>(계속)</b>\n\n" + item_text
            else:
                current_msg += item_text

    if current_msg.strip():
        messages.append(current_msg)

    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN.strip()}/sendMessage"

    for msg in messages:
        if msg.count("<b>") > msg.count("</b>"):
            msg += "</b>"
        requests.post(send_url, json={
            "chat_id": CHAT_ID.strip(),
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, verify=False)


if __name__ == "__main__":
    news = get_news()
    if news is None:
        print("[ERROR] get_news() 실패")
        news = {"자사 기사": [], "업계 전반": [], "파트너사 기사": []}
    send_telegram(get_market_data(), news)
