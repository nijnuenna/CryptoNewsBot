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

# 공통 요청 헤더 (네이버가 봇성 트래픽 차단하는 경우 대비)
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

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

LOW_QUALITY_PATTERNS = [
    r"^비트코인\s*\d+만\s*원",
    r"오늘\s*시세",
    r"^\[속보\]",
    r"^(코인|가상자산)\s*시세",
    r"실시간\s*(시세|가격)",
    r"^\[광고\]",
    r"^\[후원\]",
    r"시드\s*라운드",
    r"시리즈\s*[A-D]",
    r"프리\s*시리즈",
    r"급등|급락|상승|하락|반등|바닥|전망|목표가|원대",
    r"\d+.*달러",
    r"\d+만\s*원\s*(돌파|붕괴|터치)",
    r"\d+%\s*(달러|급등|급락|상승|하락|반등|바닥|전망|목표가)",
    r"드디어\s*터졌다",
    r"동반\s*랠리",
    r"불장|떡상|폭등|폭락",
    # --- v2 추가: 낚시성/리스트형 제목 ---
    r"\.\.\.\s*무슨\s*일",
    r"왜\s*올랐나|왜\s*떨어졌나",
    r"이것만은|총정리|한눈에|모아보기",
    r"\d+\s*가지",
]

EXCLUDED_DOMAINS = [    "contents.premium.naver.com",
    "post.naver.com",        # v2 추가: 네이버 포스트(블로그성)
    "blog.naver.com",        # v2 추가: 블로그
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
                  "제도", "입법", "법률", "시행령", "감독", "인가", "허가", "디지털자산기본법", "코인 과세", "가상자산 과세", "제도권 편입","코인거래소",
                  "지방선거", "대선", "공약", "발의", "입법시계", "제도화", "2단계", "코인법", "기본법", "정부", "여당", "야당", "국정", "선점"]),
    ("스테이블코인", ["스테이블코인", "스테이블", "USDT", "USDC", "원화코인", "원화스테이블", "달러코인", "금가분리"]),
    ("STO·토큰증권", ["STO", "토큰증권", "증권형토큰", "조각투자", "토큰화", "RWA","토큰 주식","토큰주식","토큰화 주식"]),
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

            # v2: 검색 API가 주는 본문 요약(description)도 함께 저장
            desc_clean = re.sub(r'<.*?>', '', item.get("description", ""))
            desc_clean = html_module.unescape(desc_clean)

            original_url = item.get("originallink", "").strip()
            if not original_url:
                original_url = item.get("link", "").strip()

            if any(d in original_url for d in EXCLUDED_DOMAINS):
                continue

            time_str, dt_kst = _parse_naver_date(item.get("pubDate", ""))
            source = _extract_source(original_url)

            results.append({
                "title_raw": title_clean,
                "desc_raw": desc_clean,          # v2 추가
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

def clean_text(text):
    text = re.sub(r'\[.*?\]|\(.*?\)', '', text)
    text = re.sub(r'[^가-힣a-zA-Z0-9\s]', ' ', text)
    return set([w for w in text.split() if len(w) >= 2])


def is_duplicate(title, seen_title_sets, threshold=0.45):
    new_words = clean_text(title)
    if not new_words or len(new_words) < 3:
        return False
    for seen_words in seen_title_sets:
        if not seen_words or len(seen_words) < 3:
            continue
        overlap = len(new_words & seen_words)
        ratio = overlap / min(len(new_words), len(seen_words))
        if ratio >= threshold:
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


# v4: 업계 전반용 — 인물 발언 인용 / 특정사 행사 홍보성 기사 제외
#     (단, 사업·제도·플랫폼 등 '내용'이 주제인 기사는 살림)
_PERSON_TITLE = r'(회장|부회장|사장|대표|대표이사|ceo|cto|cfo|의장|위원장|센터장|본부장|이사|교수|애널리스트|연구원)'

def is_promo_or_person(title, subject_names=None):
    """발언 인용·홍보성 기사면 True (= 제외 대상)
    subject_names: 이 이름들이 발언 주체이면 '회사가 주인공'으로 보고 발언 패턴은 통과
                   (예: 파트너사 '쟁글'이 따옴표로 입장을 밝히는 정상 기사)
    """
    t = re.sub(r'^\[.*?\]\s*', '', title.strip())
    t_low = t.lower()

    QUOTE_CHARS = "\"'\u201c\u201d\u2018\u2019"  # " ' “ ” ‘ ’
    quote_cls = "[" + re.escape(QUOTE_CHARS) + "]"

    # 1) "직함" 뒤에 따옴표 발언이 오는 패턴 → 인물 발언 인용 기사
    #    예: 박현주 회장 "킬러 ETF로...", 김OO 대표 "..."
    #    주의: 직함이 명시된 경우만 인물로 판단. 회사명은 대부분 2~4글자
    #    한글이라, "이름+따옴표"만으로 인물 단정하면 '쟁글 "..."' 같은
    #    회사 입장 기사까지 오인 제외하므로 그 규칙은 쓰지 않음.
    if re.search(_PERSON_TITLE + r'\s*' + quote_cls, t_low):
        return True

    # 2) 발언/주장 동사 → 누군가의 말 전달.
    #    단, subject_names(파트너사 등)가 제목 맨 앞 주어이면 회사 발표로 보고 통과.
    SPEECH_VERBS = ["밝혀", "강조", "전망했", "내다봤", "말했", "주장", "예상했",
                    "언급", "지적했", "당부", "역설", "토로", "단언"]
    if any(v in t_low for v in SPEECH_VERBS):
        subj_is_company = False
        if subject_names:
            for nm in subject_names:
                if t_low.lstrip().startswith(nm.lower()):
                    subj_is_company = True
                    break
        if not subj_is_company:
            return True

    # 3) 행사·세미나·컨퍼런스 홍보성 기사
    EVENT_WORDS = ["컨퍼런스", "콘퍼런스", "세미나", "포럼", "rally", "summit",
                   "데이", " day ", "밋업", "간담회", "기자회견", "출범식", "개최"]
    if any(w in t_low for w in EVENT_WORDS):
        return True

    return False


# ============================================================
# Groq API — 최종 선별용 (v2: gpt-oss-120b 추론 모델 사용)
# ============================================================

LLM_PROMPT = """당신은 한국 가상자산 거래소의 아침 뉴스 브리핑 에디터입니다.

아래는 코드로 1차 선별된 뉴스 기사 목록입니다.
각 기사는 다음 형식입니다:
[번호] (토픽) 제목 - 매체명 (발행일시)
요약: 기사 본문 첫 문장 요약

## 당신의 역할
1차 필터를 통과한 기사 중에서 "진짜 의미있는 기사"를 엄선하세요.
제목뿐 아니라 '요약'까지 읽고, 낚시성 제목인지 실제 알맹이가 있는지 판단하세요.
목표는 15~20개입니다. 애매하면 빼세요. 양보다 질입니다.

## 편집 방향
"디지털자산과 가상자산이 글로벌하게 제도권에 편입되고 있다"는 긍정적 흐름을 보여주는 기사를 우선하세요.

## 반드시 선택해야 하는 기사 (우선순위순)
1. 국내 정책이 확정·통과·시행된 기사 (법안 통과, 시행령 확정 등)
2. 국내 제도·입법 '동향·전망' 기사 — 진행 상황을 짚는 분석. 확정 전이라도 중요.
   예: "지방선거 이후 디지털자산 제도화 논의 재개", "2단계 코인법 다시 속도",
       "입법시계 돌아갈까", "금가분리 해제 임박", "디지털자산 공약 표류",
       "증권사, 디지털자산 인프라 선점 경쟁" 같은 제도·산업 구조 변화 기사
3. 글로벌 금융기관(월가, 블랙록, JP모건 등)이 가상자산에 진입하는 기사
4. ETF 승인, 기관 투자, 대형 금융사의 디지털자산 사업 확장
5. 스테이블코인, STO, 토큰증권의 제도화 진전
6. 비트코인·이더리움에 대한 깊이 있는 분석 기사 (원인·배경·맥락 포함)

## 반드시 제외해야 하는 기사 (하나라도 해당하면 제외)
- 가격만 전하는 기사: "X% 급등", "X만원 돌파", "랠리", "반등", "바닥", 목표가 전망
- 같은 이슈를 다른 매체가 보도한 중복 기사: 요약이 비슷하면 매체 등급이 높은 1개만
- 소규모 기업의 투자 유치, MOU, 서비스 출시 같은 단순 보도자료
- 인물 발언·주장 인용 기사: 'XX 회장/대표/CEO "…"', 'XX "…할 것"', "밝혔다/강조했다/전망했다"로 끝나는 기사 → 특정 인물 홍보로 간주해 제외
- 특정 증권사·운용사의 행사·세미나·컨퍼런스 홍보 기사 (예: 'OO자산운용 Rally 2026', 'OO증권 간담회 개최')
- 인물 중심 기사: "XX 대표 출연", "XX CEO 인터뷰", 컨퍼런스 발언 인용
- 블록체인·가상자산과 직접 관련 없는 기사 (AI, 핀테크, 일반 금융, 부동산 등)
- 추측성 '가격' 기사: "비트코인 10만달러 갈까", "더 오를까" 등 시세 방향 추측
  (주의: '법안 통과될까', '제도화 재개될까', '입법 속도낼까' 같은 정책·제도 진행
   전망은 추측성이 아니라 유의미한 정책 동향 분석이므로 반드시 살릴 것)
- 제목과 요약이 따로 노는 낚시성 기사

## 주의: 증권사·금융사가 나와도 살려야 하는 기사
특정 회사가 제목에 있다고 무조건 제외하지 마세요. 회사의 '사업·제도·플랫폼·서비스 구조' 자체가 주제이면 의미 있는 기사입니다.
- 살림: "한국투자증권, 제도권 금융과 가상자산 잇는 플랫폼 구상" (사업 구상이 주제)
- 제외: "박현주 회장 '킬러 ETF로 시장 기준 바꿀 것'" (인물 발언이 주제)
판단 기준은 '회사 등장 여부'가 아니라 '인물의 말/홍보가 핵심인가, 사업·제도 내용이 핵심인가'입니다.

## 응답 형식
JSON 배열만 출력. 다른 텍스트(설명, 사고 과정 포함) 절대 없이.
[1, 5, 12, 23]

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
        # v2: llama-3.3-70b-versatile -> openai/gpt-oss-120b 로 교체
        # (Llama 4 Maverick / Kimi K2-0905 가 2026년 2~3월 deprecate되며
        #  Groq이 공식 후속 모델로 gpt-oss-120b 권장)
        "model": "openai/gpt-oss-120b",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        # v4: gpt-oss-120b는 추론 모델이라 답변 공간이 작으면 "생각"에 토큰을
        #     다 쓰고 빈 답을 줌. 413 방지를 위해 줄였던 1024는 빠듯해서 2048로.
        "max_tokens": 2048,
        # v2: 추론 노력 낮춤 — 선별 작업엔 과한 사고 불필요, 속도/비용 절약
        "reasoning_effort": "low",
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60, verify=False)
            if resp.status_code == 429:
                wait = attempt * 30
                print(f"[WARN] Groq 429 — {wait}초 대기 ({attempt}/{max_retries})")
                time.sleep(wait)
                continue

            # v3: 413(Payload Too Large) — 요청이 너무 크면 프롬프트 길이를
            #     70%로 줄여서 즉시 재시도 (기사 목록 뒷부분이 잘림)
            if resp.status_code == 413:
                cur = payload["messages"][0]["content"]
                new_len = int(len(cur) * 0.7)
                payload["messages"][0]["content"] = cur[:new_len]
                print(f"[WARN] Groq 413 — 프롬프트 {len(cur)}→{new_len}자로 축소 후 재시도 ({attempt}/{max_retries})")
                continue

            resp.raise_for_status()
            data = resp.json()
            text = data["choices"][0]["message"]["content"]

            # v4: 빈 응답 방어 — 추론에 토큰을 다 써 답을 못 준 경우,
            #     출력 공간을 2배로 늘려 재시도(최대 8192)
            if not text or not text.strip():
                print(f"[WARN] Groq 빈 응답 — 출력 공간 늘려 재시도 ({attempt}/{max_retries})")
                payload["max_tokens"] = min(payload["max_tokens"] * 2, 8192)
                continue

            # v2: reasoning 모델이 가끔 ```json 펜스 없이 순수 배열만 주거나
            #     앞에 사고 흔적을 남기므로, 마지막 [...] 블록을 우선 추출
            json_match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
            if json_match:
                text = json_match.group(1)
            else:
                # 가장 마지막에 등장하는 대괄호 배열을 사용
                matches = re.findall(r'\[[\d,\s]*\]', text, re.DOTALL)
                if matches:
                    text = matches[-1]

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

    used = set()
    try:
        with open(QUOTE_FILE, "r", encoding="utf-8") as f:
            used = set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        pass

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

    new_quotes = [q for q in candidates if q not in used]
    selected = new_quotes[0] if new_quotes else (candidates[0] if candidates else "추출 실패")

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
    # 1. 자사 기사
    # ============================================================
    my_candidates = []
    for kw in MY_COMPANY_KEYWORDS:
        for sort_type in ["date", "sim"]:
            results = search_naver_news(kw, display=100, sort=sort_type)
            for r in results:
                if not _title_has_keyword(r["title_raw"], kw):
                    continue
                if is_duplicate(r["title_raw"], [clean_text(c["title_raw"]) for c in my_candidates]):
                    continue
                r["tier"] = get_source_tier(r["source_raw"])
                my_candidates.append(r)
        time.sleep(0.1)

    if my_candidates:
        my_candidates.sort(key=lambda x: (
            x["tier"],
            -(x["dt_kst"].timestamp() if x["dt_kst"] else 0),
        ))
        r = my_candidates[0]
        title = html_module.escape(r["title_raw"])
        source = html_module.escape(r["source_raw"])
        categories["자사 기사"].append(f"▲ {title} - {source} ({r['time_str']})\n{r['original_url']}")
        global_seen_sets.append(clean_text(r["title_raw"]))

    print(f"[LOG] 자사 기사: {len(categories['자사 기사'])}건")

    # ============================================================
    # 2. 업계 전반 — 수집 → 코드 필터링 → LLM 최종 선별
    # ============================================================
    # v6: 정책·제도 동향 기사를 확실히 끌어오기 위해 정책 지향 검색어 추가
    industry_queries = ["가상자산", "비트코인", "스테이블코인", "토큰증권", "디지털자산",
                        "가상자산 법안", "디지털자산 제도", "코인법"]

    raw_all = []
    for q in industry_queries:
        results = search_naver_news(q, display=100, sort="date")
        before = len(raw_all)
        for r in results:
            if any(kw in r["title_raw"] for kw in EXCLUDE_TITLE_KEYWORDS):
                continue
            if r["dt_kst"] and r["dt_kst"] < yesterday_noon:
                continue
            if is_low_quality(r["title_raw"]):
                continue
            # v4: 인물 발언 인용·행사 홍보성 기사 제외
            if is_promo_or_person(r["title_raw"]):
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

    # v3: 413 해결 — LLM에 넘기는 기사 수 60 -> 40 으로 축소
    llm_pool = filtered[:40]

    if llm_pool:
        # v2: 제목 + 본문 요약(description)을 함께 LLM에 전달
        # v3: 413 해결 — 요약 길이 120 -> 80자로 축소
        article_list_text = "\n".join(
            f"[{i+1}] ({c['topic_name']}) {c['title_raw']} - {c['source_raw']} ({c['time_str']})\n"
            f"    요약: {c.get('desc_raw', '')[:80]}"
            for i, c in enumerate(llm_pool)
        )
        print(f"[LOG] LLM에 전달: {len(llm_pool)}건")

        llm_result = _call_groq(LLM_PROMPT.replace("{article_list}", article_list_text))

        if llm_result and isinstance(llm_result, list):
            selected_ids = [x for x in llm_result if isinstance(x, int) and 1 <= x <= len(llm_pool)]
            print(f"[LOG] LLM 선택: {len(selected_ids)}건 → {selected_ids}")

            for aid in selected_ids:
                c = llm_pool[aid - 1]
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
    # 3. 파트너사 — 파트너사가 '주인공'인 '가장 최근' 기사 (회사당 1건)
    #    날짜 제한 없음: 오래됐어도 그 회사의 최신 기사면 가져옴
    # ============================================================
    for partner_name, partner_keywords in PARTNER_MAP:
        candidates = []
        seen_urls = set()

        for kw in partner_keywords:
            for sort_type in ["date", "sim"]:
                results = search_naver_news(kw, display=100, sort=sort_type)
                for r in results:
                    # (1) 제목에서 키워드가 주어(주인공) 위치인지
                    if not _title_has_keyword(r["title_raw"], kw):
                        continue
                    # (2) v6: 발행시각을 못 읽은 기사만 제외(최신순 비교 불가).
                    #     날짜 컷오프는 두지 않음 — 회사의 '가장 최근' 기사를 가져옴.
                    if not r["dt_kst"]:
                        continue
                    # (3) v5: 인물 발언·행사 홍보성 제외.
                    #     단, 파트너사 자신이 주어인 발언("쟁글 '...'")은 통과시키려
                    #     해당 파트너 키워드들을 발언 주체 예외로 전달
                    if is_promo_or_person(r["title_raw"], subject_names=partner_keywords):
                        continue
                    # (4) v4: 저품질(시세·광고 등) 패턴 제외
                    if is_low_quality(r["title_raw"]):
                        continue
                    # (5) v4: 키워드가 본문(요약)에도 실제로 등장하는지 확인
                    #     → 동음이의 키워드("코드","데이" 등) 오탐 방지
                    body = (r["title_raw"] + " " + r.get("desc_raw", "")).lower()
                    if kw.lower() not in body:
                        continue
                    # 중복 URL 제거
                    if r["original_url"] in seen_urls:
                        continue
                    seen_urls.add(r["original_url"])
                    candidates.append(r)
            time.sleep(0.1)

        # 발행시각 기준 최신순 정렬 → 가장 최근 1건
        candidates.sort(key=lambda x: x["dt_kst"].timestamp(), reverse=True)

        if candidates:
            best = candidates[0]
            title = html_module.escape(best["title_raw"])
            source = html_module.escape(best["source_raw"])
            categories["파트너사 기사"].append(f"▲ {title} - {source} ({best['time_str']})\n{best['original_url']}")
        else:
            categories["파트너사 기사"].append(f"▲ {partner_name} - 최신 기사 없음")

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
