# -*- coding: utf-8 -*-
"""
문봇 블로그랩 - 네이버 블로그 데이터 수집기
- 닉네임      : requests + BeautifulSoup
- 방문자 수   : NVisitorgp4Ajax API
- 포스팅 수   : RSS 피드 (rss.blog.naver.com)
- 유효키워드  : RSS <tag> 파싱 → 네이버 블로그 검색 API 검증
- Firebase Firestore 자동 업데이트

사용법:
  python collect.py                  # 전체 참가자 업데이트
  python collect.py tripatdawn       # 특정 블로그 테스트 출력

환경변수 (유효키워드 수집 시 필요):
  NAVER_CLIENT_ID       네이버 검색 API Client ID
  NAVER_CLIENT_SECRET   네이버 검색 API Client Secret
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore

# GitHub Actions: serviceAccountKey.json / 로컬: 원본 파일명
SERVICE_ACCOUNT_KEY = (
    "serviceAccountKey.json"
    if os.path.exists("serviceAccountKey.json")
    else "moonbotrank-firebase-adminsdk-fbsvc-84d19b29e1.json"
)

# 네이버 검색 API 키 (환경변수)
NAVER_CLIENT_ID     = os.environ.get("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://www.naver.com/",
}

KST = timezone(timedelta(hours=9))

# ──────────────────────────────────────────────
# 블로그 주제 키워드 매핑
# ──────────────────────────────────────────────
TOPIC_KEYWORDS = {
    "문학·책"    : ["책", "독서", "소설", "문학", "베스트셀러", "도서", "서평", "작가", "시집", "에세이"],
    "영화"       : ["영화", "영화리뷰", "넷플릭스", "cgv", "메가박스", "영화관", "개봉", "감독", "주연"],
    "미술·디자인": ["미술", "디자인", "그림", "전시회", "일러스트", "포토샵", "그래픽", "작품", "갤러리"],
    "공연·전시"  : ["공연", "전시", "뮤지컬", "콘서트", "연극", "오페라", "아트페어", "팝업", "박물관"],
    "음악"       : ["음악", "노래", "앨범", "아이돌", "가수", "음반", "플레이리스트", "멜론", "스포티파이"],
    "드라마"     : ["드라마", "드라마리뷰", "tvn", "mbc", "sbs", "넷플릭스드라마", "ott", "시즌"],
    "스타·연예인": ["연예인", "아이돌", "스타", "배우", "연예", "팬미팅", "직캠", "팬덤", "케이팝", "kpop"],
    "만화·애니"  : ["만화", "애니", "웹툰", "네이버웹툰", "카카오웹툰", "애니메이션", "manga"],
    "방송"       : ["방송", "예능", "라디오", "유튜브", "tv프로", "방송리뷰", "런닝맨", "나혼자산다"],
    "일상·생각"  : ["일상", "하루", "생각", "감성", "일기", "vlog", "브이로그", "소소한", "모닝"],
    "육아·결혼"  : ["육아", "아이", "육아일기", "신생아", "어린이집", "결혼", "신혼", "임신", "출산", "태교"],
    "반려동물"   : ["강아지", "고양이", "반려동물", "펫", "댕댕이", "냥이", "반려견", "반려묘", "동물병원"],
    "좋은글·이미지": ["좋은글", "명언", "감동", "힐링", "사진", "풍경사진", "감성사진", "좋은말"],
    "패션·미용"  : ["패션", "뷰티", "화장품", "스킨케어", "메이크업", "코디", "ootd", "오오티디",
                    "립스틱", "향수", "하울", "피부관리", "헤어"],
    "인테리어·DIY": ["인테리어", "diy", "집꾸미기", "홈데코", "리모델링", "소품", "가구", "셀프인테리어"],
    "요리·레시피": ["요리", "레시피", "베이킹", "홈쿡", "밀키트", "집밥", "반찬", "디저트", "쿠킹"],
    "상품리뷰"   : ["리뷰", "사용후기", "제품리뷰", "언박싱", "체험단", "협찬", "내돈내산", "솔직후기"],
    "원예·재배"  : ["원예", "식물", "가드닝", "텃밭", "화분", "다육이", "꽃", "재배", "정원"],
    "게임"       : ["게임", "롤", "리그오브레전드", "로블록스", "마인크래프트", "게임리뷰", "fps", "rpg"],
    "스포츠"     : ["스포츠", "축구", "야구", "농구", "배구", "골프", "테니스", "수영", "러닝", "마라톤"],
    "사진"       : ["사진", "카메라", "dslr", "미러리스", "포토그래피", "출사", "사진작가", "렌즈"],
    "자동차"     : ["자동차", "차", "드라이브", "전기차", "suv", "세단", "자동차리뷰", "시승기", "튜닝"],
    "취미"       : ["취미", "DIY", "공예", "뜨개질", "그림그리기", "독서", "캘리그라피", "낚시"],
    "국내여행"   : ["국내여행", "제주도", "부산", "경주", "강원도", "여수", "전주", "펜션", "캠핑",
                    "글램핑", "숙소", "국내", "여행코스", "축제", "행사"],
    "세계여행"   : ["해외여행", "유럽", "일본", "동남아", "미국", "도쿄", "오사카", "파리", "배낭여행",
                    "세계여행", "여행기", "호텔", "항공권"],
    "맛집"       : ["맛집", "카페", "식당", "음식점", "맛집추천", "카페투어", "먹방", "맛있는",
                    "초밥", "라멘", "한식", "브런치", "베이커리"],
    "IT·컴퓨터"  : ["it", "컴퓨터", "개발", "프로그래밍", "코딩", "인공지능", "ai", "챗gpt",
                    "소프트웨어", "앱", "스마트폰", "반도체", "클라우드", "파이썬"],
    "사회·정치"  : ["사회이슈", "정치", "뉴스분석", "정부정책", "선거", "시사", "입법", "국회"],
    "건강·의학"  : ["건강", "의학", "다이어트", "병원", "의료", "영양제", "비타민", "헬스",
                    "요가", "필라테스", "증상", "치료", "한방"],
    "비즈니스·경제": ["경제", "주식", "투자", "부동산", "재테크", "창업", "비즈니스", "마케팅",
                      "금융", "펀드", "코인", "etf", "배당", "사업"],
    "어학·외국어": ["영어", "일본어", "중국어", "어학", "토익", "토플", "외국어", "회화", "유학"],
    "교육·학문"  : ["교육", "공부", "학교", "대입", "수능", "학원", "자격증", "공무원", "취업"],
}

TOPIC_OPTIONS = list(TOPIC_KEYWORDS.keys())  # 드롭다운 옵션과 동일한 순서


def _kw_match(text: str, keyword: str) -> int:
    """
    키워드가 텍스트에서 독립된 단어로 등장하는 횟수 반환.
    3자 이상은 substring 허용, 2자 이하는 공백 경계 필요.
    """
    import re
    kw = keyword.lower()
    if len(kw) <= 2:
        # 짧은 키워드: 공백 경계 필요
        pattern = r'(?<!\S)' + re.escape(kw) + r'(?!\S)'
        return len(re.findall(pattern, text))
    return text.count(kw)


def detect_topic(titles: list[str], categories: list[str] = None, description: str = "") -> str:
    """
    1순위: 블로그 카테고리명으로 주제 매칭 (가중치 8배)
    2순위: RSS 포스팅 제목 키워드 분석
    매칭 없으면 '기타' 반환
    """
    scores: dict[str, float] = {}

    # 1순위: 카테고리 (공백 단위로 토큰화 후 매칭)
    cat_tokens = " ".join(categories or []).lower()
    if cat_tokens:
        for topic, keywords in TOPIC_KEYWORDS.items():
            cnt = sum(_kw_match(cat_tokens, kw) for kw in keywords)
            if cnt > 0:
                scores[topic] = scores.get(topic, 0) + cnt * 8

    # 2순위: RSS 제목 전체 텍스트
    title_text = " ".join(titles[:20] + [description]).lower()
    for topic, keywords in TOPIC_KEYWORDS.items():
        cnt = sum(_kw_match(title_text, kw) for kw in keywords)
        if cnt > 0:
            scores[topic] = scores.get(topic, 0) + cnt

    if not scores:
        return "기타"
    return max(scores, key=scores.get)


# ──────────────────────────────────────────────
# 닉네임 + 프로필 이미지 수집
# ──────────────────────────────────────────────
def fetch_blog_meta(blog_id: str) -> dict:
    """닉네임과 프로필 이미지 URL을 함께 수집"""
    result = {"nickname": blog_id, "profileImg": ""}
    try:
        r = requests.get(f"https://blog.naver.com/{blog_id}", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        frame = soup.find("iframe", {"id": "mainFrame"})
        if frame:
            src = frame.get("src", "")
            url2 = src if src.startswith("http") else "https://blog.naver.com" + src
            r2 = requests.get(url2, headers=HEADERS, timeout=15)
            r2.encoding = "utf-8"
            soup2 = BeautifulSoup(r2.text, "html.parser")
        else:
            soup2 = soup

        # 닉네임
        for sel in [".nickName", ".nick", ".blog_title", ".BlogTitle", "[class*='nick']"]:
            el = soup2.select_one(sel)
            if el and el.get_text(strip=True):
                result["nickname"] = el.get_text(strip=True)
                break

        # 프로필 이미지 탐색 (우선순위 순)
        profile_img = ""

        # 1순위: profile_widget onclick 속성
        img_tag = soup2.find("img", onclick=lambda x: x and "profile_widget" in x)
        if img_tag:
            profile_img = img_tag.get("src", "")

        # 2순위: blogpfthumb (커스텀 프로필)
        if not profile_img:
            for img in soup2.find_all("img"):
                s = img.get("src", "")
                if "blogpfthumb" in s:
                    profile_img = s
                    break

        # 3순위: 네이버 기본 프리셋 이미지
        if not profile_img:
            for img in soup2.find_all("img"):
                s = img.get("src", "")
                if "img_profile_preset" in s:
                    profile_img = s
                    break

        # 크기 파라미터 정리
        if profile_img:
            base = profile_img.split("?")[0]
            result["profileImg"] = base + "?type=w80" if "pstatic.net" in base else base

        # 블로그 카테고리 수집 (주제 감지에 활용)
        categories = []
        for el in soup2.select("[class*=category]"):
            txt = el.get_text(strip=True)
            if txt and 1 < len(txt) < 30:
                categories.append(txt)
        result["categories"] = list(set(categories))  # 중복 제거

    except Exception as e:
        print(f"  블로그 메타 실패: {e}")
    return result


def fetch_nickname(blog_id: str) -> str:
    return fetch_blog_meta(blog_id)["nickname"]


# ──────────────────────────────────────────────
# 방문자 수 수집 (NVisitorgp4Ajax API)
# ──────────────────────────────────────────────
def fetch_visitors(blog_id: str) -> dict:
    url = f"https://blog.naver.com/NVisitorgp4Ajax.nhn?blogId={blog_id}"
    h = {**HEADERS,
         "Referer": f"https://blog.naver.com/PostList.naver?blogId={blog_id}",
         "X-Requested-With": "XMLHttpRequest"}
    result = {"today": 0, "yesterday": 0, "week_total": 0, "daily": {}}
    try:
        r = requests.get(url, headers=h, timeout=10)
        root = ET.fromstring(r.text)
        today_str = datetime.now(KST).strftime("%Y%m%d")
        yesterday_str = (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d")
        for item in root.findall("visitorcnt"):
            date_id = item.get("id", "")
            cnt = int(item.get("cnt", 0))
            result["daily"][date_id] = cnt
            result["week_total"] += cnt
            if date_id == today_str:
                result["today"] = cnt
            elif date_id == yesterday_str:
                result["yesterday"] = cnt
    except Exception as e:
        print(f"  방문자 실패: {e}")
    return result


# ──────────────────────────────────────────────
# 포스팅 수 수집 (RSS 피드)
# ──────────────────────────────────────────────
def fetch_posts(blog_id: str, challenge_start: datetime = None) -> dict:
    """
    RSS 피드로 포스팅 수 집계
    반환: {
        today_count    : 오늘 포스팅 수,
        challenge_count: 챌린지 시작일 이후 포스팅 수,
        posts          : [{ title, date }]  최근 포스팅 목록
    }
    """
    result = {"today_count": 0, "challenge_count": 0, "posts": [], "rss_img_url": ""}
    try:
        r = requests.get(f"https://rss.blog.naver.com/{blog_id}.xml",
                         headers=HEADERS, timeout=10)
        root = ET.fromstring(r.content)
        channel = root.find("channel")
        if channel is None:
            return result

        # RSS 채널 프로필 이미지 추출
        img_el = channel.find("image")
        if img_el is not None:
            rss_img = img_el.findtext("url", "").strip()
            if rss_img:
                result["rss_img_url"] = rss_img

        now_kst = datetime.now(KST)
        today_date = now_kst.date()

        for item in channel.findall("item"):
            title = item.findtext("title", "").strip()
            pub_date_str = item.findtext("pubDate", "").strip()
            if not pub_date_str:
                continue

            try:
                pub_dt = parsedate_to_datetime(pub_date_str)
                pub_dt_kst = pub_dt.astimezone(KST)
                pub_date = pub_dt_kst.date()
            except Exception:
                continue

            result["posts"].append({"title": title, "date": pub_dt_kst.strftime("%Y-%m-%d %H:%M")})

            # 오늘 포스팅
            if pub_date == today_date:
                result["today_count"] += 1

            # 챌린지 기간 포스팅
            if challenge_start:
                start_date = challenge_start.astimezone(KST).date() if challenge_start.tzinfo else challenge_start.date()
                if pub_date >= start_date:
                    result["challenge_count"] += 1

    except Exception as e:
        print(f"  RSS 실패: {e}")
    return result


# ──────────────────────────────────────────────
# 점수 계산
# ──────────────────────────────────────────────
def calc_score(p: dict, config: dict) -> float:
    visitor_delta = (p.get("currentVisitors") or 0) - (p.get("startVisitors") or 0)
    keyword_delta = (p.get("currentKeywords") or 0) - (p.get("startKeywords") or 0)
    post_count = p.get("postCount") or 0
    score = (
        visitor_delta / 1000 * config.get("wVisitor", 1.0) +
        post_count * config.get("wPost", 2.0) +
        keyword_delta * config.get("wKeyword", 0.5)
    )
    return round(max(score, 0), 1)



# ──────────────────────────────────────────────
# 프로필 이미지 → Firebase Storage 업로드
# ──────────────────────────────────────────────
def fetch_profile_as_base64(blog_id: str, rss_img_url: str = "", naver_img_url: str = "") -> str:
    """
    RSS 또는 blogpfthumb URL에서 프로필 이미지 다운로드 후 base64 data URI로 반환.
    Firestore에 저장하면 hotlink/CORS 문제 없이 어디서든 표시 가능.
    80×80 JPEG 기준 약 3~8KB → Firestore 문서 크기 문제 없음.
    """
    import base64

    # 시도할 URL 목록 (RSS URL 우선 - 더 안정적)
    candidates = [u for u in [rss_img_url, naver_img_url] if u]

    for url in candidates:
        # pstatic.net URL만 사용 (SSL 안정적)
        # blogpfthumb.phinf.naver.net → SSL 문제 있음 → 스킵
        if "blogpfthumb.phinf.naver.net" in url:
            # pstatic 버전으로 변환 시도
            url = url.replace(
                "http://blogpfthumb.phinf.naver.net",
                "https://blogpfthumb-phinf.pstatic.net"
            )
        url = url.replace("http://", "https://")

        try:
            resp = requests.get(
                url,
                headers={
                    "User-Agent": HEADERS["User-Agent"],
                    "Referer": "https://blog.naver.com/",
                },
                timeout=10,
                verify=True,
            )
            if resp.status_code == 200 and len(resp.content) > 200:
                mime = resp.headers.get("content-type", "image/jpeg").split(";")[0]
                b64 = base64.b64encode(resp.content).decode("ascii")
                data_uri = f"data:{mime};base64,{b64}"
                print(f"  프로필 base64 변환 완료 ({len(resp.content)//1024}KB, {len(data_uri)} chars)")
                return data_uri
        except Exception as e:
            print(f"  프로필 다운로드 실패({url[:50]}): {e}")
            continue

    return ""


# ──────────────────────────────────────────────
# 유효키워드 수집
# ──────────────────────────────────────────────
def fetch_rss_tags(blog_id: str) -> list[str]:
    """
    RSS 피드의 각 <item> 에서 <tag> 요소를 파싱해 고유 키워드 목록 반환.
    Naver RSS <tag> 형식: 콤마 구분 문자열 또는 개별 태그
    """
    tags: set[str] = set()
    try:
        r = requests.get(
            f"https://rss.blog.naver.com/{blog_id}.xml",
            headers=HEADERS, timeout=10
        )
        root = ET.fromstring(r.content)
        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item"):
            tag_el = item.find("tag")
            if tag_el is not None and tag_el.text:
                # 콤마·슬래시 구분 모두 대응
                for kw in tag_el.text.replace("/", ",").split(","):
                    kw = kw.strip()
                    if kw:
                        tags.add(kw)

    except Exception as e:
        print(f"  RSS 태그 파싱 실패: {e}")

    return list(tags)


def count_valid_keywords(blog_id: str, keywords: list[str]) -> int:
    """
    키워드 목록 각각을 네이버 블로그 검색 API로 조회해
    상위 10개 결과에 blog_id가 포함된 키워드 수를 반환.

    필요 환경변수: NAVER_CLIENT_ID, NAVER_CLIENT_SECRET
    API 제한: 0.3초 딜레이, 오류 키워드는 skip
    """
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        print("  ⚠ NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정 → 키워드 수집 건너뜀")
        return -1   # -1: API 미설정 상태임을 구분

    if not keywords:
        return 0

    api_headers = {
        "X-Naver-Client-Id"    : NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    valid_count = 0
    checked = 0

    for kw in keywords:
        try:
            resp = requests.get(
                "https://openapi.naver.com/v1/search/blog",
                params={"query": kw, "display": 10},
                headers=api_headers,
                timeout=8,
            )
            if resp.status_code != 200:
                time.sleep(0.3)
                continue

            items = resp.json().get("items", [])
            # 상위 10개 결과 중 blogId 포함 여부 확인
            for item in items:
                link        = item.get("link", "")
                blogger_link = item.get("bloggerlink", "")
                if blog_id.lower() in link.lower() or blog_id.lower() in blogger_link.lower():
                    valid_count += 1
                    break

            checked += 1
        except Exception as e:
            print(f"  키워드 '{kw}' 조회 실패(skip): {e}")

        time.sleep(0.3)  # rate limit 방지

    print(f"  유효키워드: {valid_count}/{len(keywords)}개 (조회 {checked}건)")
    return valid_count


# ──────────────────────────────────────────────
# 단일 블로그 테스트 출력
# ──────────────────────────────────────────────
def test_blog(blog_id: str):
    print(f"\n[테스트] {blog_id}")
    print("─" * 45)

    nickname = fetch_nickname(blog_id)
    visitors = fetch_visitors(blog_id)
    posts    = fetch_posts(blog_id)
    tags     = fetch_rss_tags(blog_id)

    print(f"닉네임      : {nickname}")
    print(f"오늘 방문자 : {visitors['today']:,}명")
    print(f"어제 방문자 : {visitors['yesterday']:,}명")
    print(f"5일 합계    : {visitors['week_total']:,}명")
    print(f"오늘 포스팅 : {posts['today_count']}개")
    print(f"RSS 태그    : {len(tags)}개 → {tags[:10]}")

    if NAVER_CLIENT_ID:
        valid = count_valid_keywords(blog_id, tags)
        print(f"유효키워드  : {valid}개")
    else:
        print("유효키워드  : API 키 미설정 (NAVER_CLIENT_ID 환경변수 필요)")

    print(f"\n[최근 포스팅]")
    for p in posts["posts"][:5]:
        print(f"  {p['date']}  {p['title'][:40]}")


# ──────────────────────────────────────────────
# 전체 수집 실행
# ──────────────────────────────────────────────
def run_collection():
    print("=" * 50)
    print("문봇 블로그 데이터 수집")
    print(datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 50)

    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    # 활성 챌린지
    active_doc = db.collection("settings").document("active").get()
    if not active_doc.exists:
        print("활성 챌린지 없음. 관리자 패널에서 챌린지를 먼저 등록하세요.")
        return
    challenge_id = active_doc.to_dict().get("challengeId")

    # 챌린지 시작일
    ch_doc = db.collection("challenges").document(challenge_id).get()
    challenge_start = None
    if ch_doc.exists:
        start_ts = ch_doc.to_dict().get("startDate")
        if start_ts:
            challenge_start = start_ts.replace(tzinfo=KST) if hasattr(start_ts, "replace") else start_ts.astimezone(KST)

    # 점수 설정
    score_doc = db.collection("settings").document("scoreConfig").get()
    score_config = score_doc.to_dict() if score_doc.exists else {"wVisitor": 1.0, "wPost": 2.0, "wKeyword": 0.5}

    participants_ref = db.collection("challenges").document(challenge_id).collection("participants")
    participants = list(participants_ref.stream())
    print(f"참가자: {len(participants)}명\n")

    for snap in participants:
        p = snap.to_dict()
        blog_id = p.get("blogId", "").strip()
        if not blog_id:
            continue

        print(f"수집: {blog_id} ({p.get('nickname', '')})")

        # ── 1. 메타 (닉네임·카테고리·프로필URL) ──────────────
        meta     = fetch_blog_meta(blog_id)
        nickname = meta["nickname"] if meta["nickname"] != blog_id else p.get("nickname", blog_id)

        # ── 2. 방문자 수 ──────────────────────────────────
        visitors = fetch_visitors(blog_id)
        today_v  = visitors["today"]
        prev_curr = p.get("currentVisitors") or p.get("startVisitors") or 0
        new_curr  = prev_curr + today_v if today_v > 0 else prev_curr

        # ── 3. 포스팅 수 (RSS) ────────────────────────────
        posts = fetch_posts(blog_id, challenge_start)

        # ── 4. 프로필 이미지 (base64 재사용 또는 새 다운로드) ─
        stored_img = p.get("profileImg", "")
        if stored_img and stored_img.startswith("data:"):
            profile_img = stored_img
        else:
            rss_img   = posts.get("rss_img_url", "")
            naver_img = meta.get("profileImg", "")
            profile_img = fetch_profile_as_base64(blog_id, rss_img, naver_img) or stored_img

        # ── 5. 유효키워드 수집 ────────────────────────────
        tags = fetch_rss_tags(blog_id)
        valid_kw = count_valid_keywords(blog_id, tags)
        # valid_kw == -1 이면 API 미설정 → 기존 값 유지
        current_kw = valid_kw if valid_kw >= 0 else p.get("currentKeywords", 0)

        # ── 6. 주제 자동 감지 ─────────────────────────────
        post_titles    = [pp["title"] for pp in posts["posts"]]
        categories     = meta.get("categories", [])
        detected_topic = detect_topic(post_titles, categories)

        # ── 7. Firestore 업데이트 데이터 구성 ─────────────
        update_data = {
            "nickname"        : nickname,
            "profileImg"      : profile_img,
            "todayVisitors"   : today_v,
            "currentVisitors" : new_curr,
            "weekVisitors"    : visitors["week_total"],
            "postCount"       : posts["challenge_count"],
            "todayPostCount"  : posts["today_count"],
            "recentPosts"     : posts["posts"][:10],
            "currentKeywords" : current_kw,
            "updatedAt"       : firestore.SERVER_TIMESTAMP,
        }

        merged = {**p, **update_data}
        update_data["score"] = calc_score(merged, score_config)

        # ── 8. blog_cache 저장 (관리자 자동조회용) ─────────
        db.collection("blog_cache").document(blog_id).set({
            "blogId"          : blog_id,
            "nickname"        : nickname,
            "profileImg"      : profile_img,
            "topic"           : detected_topic,
            "todayVisitors"   : today_v,
            "weekVisitors"    : visitors["week_total"],
            "todayPosts"      : posts["today_count"],
            "currentKeywords" : current_kw,
            "fetchedAt"       : datetime.now(KST).isoformat(),
        }, merge=True)

        # ── 9. 참가자 문서 업데이트 ───────────────────────
        participants_ref.document(snap.id).update(update_data)

        print(f"  닉네임={nickname} | 방문자={today_v:,} | 누적={new_curr:,}"
              f" | 포스팅={posts['challenge_count']} | 키워드={current_kw}"
              f" | 주제={detected_topic} | 점수={update_data['score']}")
        time.sleep(1.5)

    print(f"\n완료: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            test_blog(sys.argv[1])
        else:
            run_collection()
    except Exception as e:
        import traceback
        print("=" * 50)
        print("[ERROR] 수집 실패:")
        traceback.print_exc()
        print("=" * 50)
        sys.exit(1)
