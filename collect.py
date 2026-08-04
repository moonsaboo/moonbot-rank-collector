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
import re
import json
import time
import hmac
import hashlib
import base64
import urllib.parse

# .env 파일 자동 로드 (로컬 개발용)
# scraper/ 또는 부모 디렉토리의 .env 파일을 탐색
try:
    from dotenv import load_dotenv
    _env_paths = [
        os.path.join(os.path.dirname(__file__), ".env"),          # scraper/.env
        os.path.join(os.path.dirname(__file__), "..", ".env"),    # blog-challenge/.env
    ]
    for _p in _env_paths:
        if os.path.exists(_p):
            load_dotenv(_p)
            break
except ImportError:
    pass  # GitHub Actions 환경 등에서는 dotenv 없이 환경변수 직접 사용
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
    else "moonsaboochallenge-firebase-adminsdk-fbsvc-5ce8261153.json"
)

# 네이버 검색 API 키 (환경변수)
NAVER_CLIENT_ID     = os.environ.get("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")

# 네이버 검색광고 API 키 (키워드 검색량 조회)
NAVER_AD_CUSTOMER_ID    = os.environ.get("NAVER_AD_CUSTOMER_ID", "")
NAVER_AD_ACCESS_LICENSE = os.environ.get("NAVER_AD_ACCESS_LICENSE", "")
NAVER_AD_SECRET_KEY     = os.environ.get("NAVER_AD_SECRET_KEY", "")
RUN_KEYWORDS = os.environ.get("RUN_KEYWORDS", "").lower() in ("1", "true", "yes", "y")

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
    """방문자 통계를 비공개로 설정한 블로그는 API가 빈 응답을 주므로,
    실패와 '정상적으로 0명'을 구분할 수 있게 ok 플래그를 함께 반환한다."""
    url = f"https://blog.naver.com/NVisitorgp4Ajax.nhn?blogId={blog_id}"
    h = {**HEADERS,
         "Referer": f"https://blog.naver.com/PostList.naver?blogId={blog_id}",
         "X-Requested-With": "XMLHttpRequest"}
    result = {"today": 0, "yesterday": 0, "week_total": 0, "daily": {}, "ok": True}
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
        result["ok"] = False
    return result


# ──────────────────────────────────────────────
# 챌린지 시작 전 5일 평균 방문자 계산
# (네이버 NVisitorgp4Ajax API가 최근 5일치만 제공하므로 5일 기준)
# ──────────────────────────────────────────────
def calc_pre_challenge_avg(daily_data: dict, challenge_start: datetime,
                            weekday_only: bool = False) -> int:
    """챌린지 시작일 직전 5일(D-1 ~ D-5) 일평균 방문자.
    weekday_only=True(평일만 챌린지)면 그 중 주말(토·일)은 제외하고 평일만 평균.
    없는 날은 건너뜀."""
    if not challenge_start or not daily_data:
        return 0
    start_date = challenge_start.date() if hasattr(challenge_start, "date") else challenge_start
    counts = []
    for i in range(1, 6):
        d = start_date - timedelta(days=i)
        if weekday_only and d.weekday() >= 5:
            continue
        d_str = d.strftime("%Y%m%d")
        if d_str in daily_data:
            counts.append(daily_data[d_str])
    return round(sum(counts) / len(counts)) if counts else 0


# ──────────────────────────────────────────────
# 포스팅 수 수집 (RSS 피드)
# ──────────────────────────────────────────────
def fetch_posts(blog_id: str, challenge_start: datetime = None, weekday_only: bool = False) -> dict:
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
            link  = item.findtext("link", "").strip()
            pub_date_str = item.findtext("pubDate", "").strip()
            if not pub_date_str:
                continue

            try:
                pub_dt = parsedate_to_datetime(pub_date_str)
                pub_dt_kst = pub_dt.astimezone(KST)
                pub_date = pub_dt_kst.date()
            except Exception:
                continue

            result["posts"].append({
                "title": title,
                "date" : pub_dt_kst.strftime("%Y-%m-%d %H:%M"),
                "link" : link,
            })

            is_weekend = pub_dt_kst.weekday() >= 5  # 토(5)·일(6)

            # 오늘 포스팅 (weekday_only면 주말 제외)
            if pub_date == today_date:
                if not (weekday_only and is_weekend):
                    result["today_count"] += 1

            # 챌린지 기간 포스팅 (weekday_only면 주말 제외)
            if challenge_start:
                start_date = challenge_start.astimezone(KST).date() if challenge_start.tzinfo else challenge_start.date()
                if pub_date >= start_date:
                    if not (weekday_only and is_weekend):
                        result["challenge_count"] += 1

    except Exception as e:
        print(f"  RSS 실패: {e}")
    return result


# ──────────────────────────────────────────────
# 포스팅 수 수집 (네이버 자체 글목록 API - RSS 50개 제한 없음)
# ──────────────────────────────────────────────
def _parse_add_date(s: str, now_kst: datetime):
    """PostTitleListAsync의 addDate 문자열을 date로 변환.
    네이버는 발행 후 24시간 이내인 글을 'N분 전'/'N시간 전' 같은 상대 표기로 보여준다.
    24시간 이내라도 자정을 넘겼으면 실제로는 전날 글일 수 있으므로,
    now_kst에서 경과 시간을 빼서 실제 날짜를 계산한다(무조건 '오늘'로 찍지 않음)."""
    s = (s or "").strip()
    if not s:
        return None
    if s == "방금":
        return now_kst.date()
    m = re.match(r"(\d+)\s*분\s*전", s)
    if m:
        return (now_kst - timedelta(minutes=int(m.group(1)))).date()
    m = re.match(r"(\d+)\s*시간\s*전", s)
    if m:
        return (now_kst - timedelta(hours=int(m.group(1)))).date()
    m = re.match(r"(\d+)\s*일\s*전", s)
    if m:
        return (now_kst - timedelta(days=int(m.group(1)))).date()
    if "전" in s:
        return now_kst.date()
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?", s)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d).date()
    return None


def fetch_posts_full(blog_id: str, challenge_start: datetime = None, challenge_end: datetime = None,
                      weekday_only: bool = False, max_pages: int = 60) -> dict:
    """
    RSS(rss.blog.naver.com)는 최근 50개 글만 반환하므로, 하루 포스팅이 많은 블로그는
    챌린지 초반 날짜가 며칠 안에 RSS 창 밖으로 밀려나 영구적으로 누락될 수 있다.
    대신 블로그 자체 글목록 API(PostTitleListAsync)를 챌린지 시작일까지 페이지네이션해
    전체 발행 이력을 정확히 집계한다.
    반환 형식은 fetch_posts()와 동일: {today_count, challenge_count, posts}
    """
    result = {"today_count": 0, "challenge_count": 0, "posts": []}
    now_kst    = datetime.now(KST)
    today_date = now_kst.date()
    start_date = challenge_start.astimezone(KST).date() if (challenge_start and challenge_start.tzinfo) \
        else (challenge_start.date() if challenge_start else None)
    end_date   = challenge_end.astimezone(KST).date() if (challenge_end and challenge_end.tzinfo) \
        else (challenge_end.date() if challenge_end else today_date)

    seen_lognos = set()
    day_minute  = {}   # 날짜별로 부여할 분(정렬용 가짜 시각, 큰 값부터 감소)
    page = 1
    total_count = None

    while page <= max_pages:
        try:
            r = requests.get(
                "https://blog.naver.com/PostTitleListAsync.naver",
                params={
                    "blogId": blog_id, "currentPage": page,
                    "categoryNo": 0, "parentCategoryNo": "",
                    "countPerPage": 30,
                },
                headers=HEADERS, timeout=10,
            )
            # 네이버 응답의 pagingHtml에 JSON 스펙상 무효한 \' 이스케이프가 섞여 있어 정리 후 파싱
            data = json.loads(r.text.replace("\\'", "'"))
        except Exception as e:
            print(f"  글목록 조회 실패(page {page}): {e}")
            break

        posts = data.get("postList", [])
        if not posts:
            break
        if total_count is None:
            try:
                total_count = int(data.get("totalCount", 0))
            except (TypeError, ValueError):
                total_count = None

        oldest_on_page = None
        new_item_seen  = False
        for item in posts:
            log_no = item.get("logNo")
            if not log_no or log_no in seen_lognos:
                continue
            seen_lognos.add(log_no)
            new_item_seen = True

            d = _parse_add_date(item.get("addDate", ""), now_kst)
            if d is None:
                continue
            oldest_on_page = d if oldest_on_page is None or d < oldest_on_page else oldest_on_page

            if start_date and d < start_date:
                continue

            minute = day_minute.get(d, 59)
            day_minute[d] = max(minute - 1, 0)
            title = urllib.parse.unquote(item.get("title", ""))

            result["posts"].append({
                "title" : title,
                "date"  : f"{d.isoformat()} 23:{minute:02d}",
                "link"  : f"https://blog.naver.com/{blog_id}/{log_no}",
                "logNo" : str(log_no),
            })

            is_weekend = d.weekday() >= 5
            if not (weekday_only and is_weekend):
                if d == today_date:
                    result["today_count"] += 1
                if not end_date or d <= end_date:
                    result["challenge_count"] += 1

        if not new_item_seen:
            break
        if total_count is not None and page * 30 >= total_count:
            break
        if oldest_on_page and start_date and oldest_on_page <= start_date:
            break
        page += 1
        time.sleep(0.3)

    return result


# ──────────────────────────────────────────────
# 점수 / 지수 계산
# ──────────────────────────────────────────────
def calc_avg_visitors_7d(visitor_log: dict, start_visitors: int = 0) -> float:
    """
    visitorLog에서 최근 7일 평균 방문자 수 계산 (KST 기준).
    7일 미만이면 기록된 일수로 평균.
    """
    today = datetime.now(KST).date()
    values = []
    for i in range(7):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        if d in visitor_log:
            values.append(visitor_log[d])
    if not values:
        return float(start_visitors)
    return round(sum(values) / len(values), 1)


def calc_challenge_week_avg_visitors(visitor_log: dict, challenge_start: datetime,
                                     weekday_only: bool = False) -> float:
    """
    Challenge-week average visitor count.
    Day 1 divides by 1, day 2 by 2, day 7 by 7, and day 8 starts over at 1.
    weekday_only=True면 주말을 건너뛰고 평일 5일 단위로 주기가 리셋된다
    (주말에는 직전 평일까지의 값이 그대로 유지됨).
    """
    today = datetime.now(KST).date()
    if not challenge_start:
        return calc_avg_visitors_7d(visitor_log, 0)

    start_date = challenge_start.astimezone(KST).date() if challenge_start.tzinfo else challenge_start.date()
    if today < start_date:
        return 0.0

    if not weekday_only:
        elapsed_days = (today - start_date).days
        week_day_index = elapsed_days % 7
        period_start = today - timedelta(days=week_day_index)
        divisor = week_day_index + 1

        total = 0
        for i in range(divisor):
            d = (period_start + timedelta(days=i)).strftime("%Y-%m-%d")
            total += int(visitor_log.get(d, 0) or 0)
        return round(total / divisor, 1)

    # 평일만: start_date~today의 평일 목록을 모아 마지막 5일(주기) 단위로 평균
    weekdays = []
    cur = start_date
    while cur <= today:
        if cur.weekday() < 5:
            weekdays.append(cur)
        cur += timedelta(days=1)
    if not weekdays:
        return 0.0

    cycle_len   = (len(weekdays) - 1) % 5 + 1
    period_days = weekdays[-cycle_len:]
    total = sum(int(visitor_log.get(d.strftime("%Y-%m-%d"), 0) or 0) for d in period_days)
    return round(total / cycle_len, 1)


# ──────────────────────────────────────────────
# 통합 점수 시스템 v3 (Business Marketer Edition)
# 활동(40점) + 성장(60점) = 100점 만점
# ──────────────────────────────────────────────

def calc_daily_post_log(posts_list: list, challenge_start: datetime,
                        weekday_only: bool) -> dict:
    """RSS 포스팅 목록 → 챌린지 기간 일별 포스팅 수 {YYYY-MM-DD: count}"""
    if not challenge_start:
        return {}
    start_date = challenge_start.date() if hasattr(challenge_start, "date") else challenge_start
    log: dict = {}
    for post in posts_list:
        try:
            date_str = post.get("date", "")[:10]
            d = datetime.fromisoformat(date_str).date()
            if d < start_date:
                continue
            if weekday_only and d.weekday() >= 5:
                continue
            log[date_str] = log.get(date_str, 0) + 1
        except Exception:
            pass
    return log


def calc_consecutive_days(daily_post_log: dict, challenge_start: datetime,
                           goal_per_day: int, weekday_only: bool) -> int:
    """오늘부터 역산해 연속 달성 일수 (일 목표 포스팅 충족 기준)"""
    if not challenge_start:
        return 0
    start_date = challenge_start.date() if hasattr(challenge_start, "date") else challenge_start
    today = datetime.now(KST).date()
    count = 0
    d = today
    while d >= start_date:
        if weekday_only and d.weekday() >= 5:
            d -= timedelta(days=1)
            continue
        if daily_post_log.get(d.isoformat(), 0) >= max(goal_per_day, 1):
            count += 1
            d -= timedelta(days=1)
        else:
            break
    return count


def calc_achieved_days(daily_post_log: dict, challenge_start: datetime,
                       challenge_end: datetime, goal_per_day: int,
                       weekday_only: bool) -> float:
    """챌린지 기간 중 목표 달성 일수.
    하루 목표를 초과한 포스팅은 1일로 제한하고, 목표 미만은 비율만큼 반영합니다.
    """
    if not challenge_start:
        return 0.0
    start_date = challenge_start.date() if hasattr(challenge_start, "date") else challenge_start
    end_date = challenge_end.date() if challenge_end and hasattr(challenge_end, "date") else datetime.now(KST).date()
    today = datetime.now(KST).date()
    end_date = min(end_date, today)
    goal = max(goal_per_day, 1)
    count = 0.0
    d = start_date
    while d <= end_date:
        if weekday_only and d.weekday() >= 5:
            d += timedelta(days=1)
            continue
        count += min((daily_post_log.get(d.isoformat(), 0) or 0) / goal, 1.0)
        d += timedelta(days=1)
    return round(count, 2)


def calc_activity_score(achieved_days: float,
                        consec_days: int, total_days: int) -> float:
    """활동 점수 (최대 40점)
    - 날짜별 달성률 30점 + 연속 달성 보너스 10점
    - 하루 목표를 넘겨 작성해도 해당 날짜는 최대 1일 달성으로만 계산
    """
    posting = min(achieved_days / total_days, 1.0) * 30 if total_days > 0 else 0.0
    consec  = min(consec_days / total_days, 1.0) * 10 if total_days > 0 else 0.0
    return round(posting + consec, 2)


def calc_growth_score(curr_visitors: int, start_visitors: int,
                      curr_keywords: int) -> float:
    """성장 점수 (최대 60점)
    - 방문자 절대값 30점 + 방문자 성장률 10점 + 키워드 수 20점
    """
    # 방문자 절대값 (30점)
    v = curr_visitors
    if v >= 10001:  v_abs = 30.0
    elif v >= 5001: v_abs = 25.0
    elif v >= 2001: v_abs = 20.0
    elif v >= 1001: v_abs = 15.0
    elif v >= 301:  v_abs = 10.0
    elif v >= 101:  v_abs = 5.0
    elif v >= 1:    v_abs = 2.0
    else:           v_abs = 0.0

    # 방문자 성장률 (10점)
    if start_visitors > 0:
        v_rate = (curr_visitors - start_visitors) / start_visitors * 100
    else:
        v_rate = 100.0 if curr_visitors > 0 else 0.0

    if v_rate >= 100:  v_growth = 10.0
    elif v_rate >= 50: v_growth = 7.0
    elif v_rate >= 10: v_growth = 5.0
    else:              v_growth = 0.0

    # 키워드 수 (20점)
    k = curr_keywords
    if k >= 100:  k_score = 20.0
    elif k >= 50: k_score = 15.0
    elif k >= 20: k_score = 10.0
    else:         k_score = 5.0

    return round(v_abs + v_growth + k_score, 2)


def calc_integrated_score(activity: float, growth: float) -> float:
    """통합 점수 = 활동(최대40) + 성장(최대60) = 최대 100점"""
    return round(min(activity + growth, 100.0), 2)


DEFAULT_LEVEL_CONFIG = {
    "루키"    : 0,
    "플레이어": 21,
    "러너"    : 36,
    "프로"    : 51,
    "리더"    : 66,
    "마스터"  : 81,
}


def score_to_blog_level(score: float, level_config: dict = None) -> dict:
    cfg = level_config or DEFAULT_LEVEL_CONFIG
    for label in ["마스터", "리더", "프로", "러너", "플레이어", "루키"]:
        if score >= cfg.get(label, DEFAULT_LEVEL_CONFIG.get(label, 0)):
            return {"label": label, "score": score}
    return {"label": "루키", "score": score}



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


def _parse_search_volume(value) -> int:
    """검색량 값 파싱 - '< 10' 문자열 포함 처리"""
    if value is None:
        return 0
    if isinstance(value, str):
        if value.strip() == "< 10":
            return 5
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0
    return int(value)


def fetch_keyword_volumes(keywords: list[str]) -> dict[str, int]:
    """
    네이버 검색광고 API로 키워드 월간 검색량(PC+모바일) 조회.
    반환: {키워드명: 월간검색량} dict
    """
    if not (NAVER_AD_CUSTOMER_ID and NAVER_AD_ACCESS_LICENSE and NAVER_AD_SECRET_KEY):
        return {}
    if not keywords:
        return {}

    def _sign(timestamp: str, method: str, uri: str) -> str:
        message = f"{timestamp}.{method}.{uri}"
        sig = hmac.new(
            NAVER_AD_SECRET_KEY.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(sig).decode("utf-8")

    def _clean(kw: str) -> str:
        return kw.replace(" ", "")

    volumes: dict[str, int] = {}
    uri = "/keywordstool"
    base_url = "https://api.naver.com"

    for i in range(0, len(keywords), 100):
        chunk = keywords[i:i + 100]
        cleaned = [_clean(k) for k in chunk]
        ts = str(int(time.time() * 1000))
        try:
            resp = requests.get(
                base_url + uri,
                params={"hintKeywords": ",".join(cleaned), "showDetail": "1"},
                headers={
                    "Content-Type": "application/json",
                    "X-Timestamp":  ts,
                    "X-API-KEY":    NAVER_AD_ACCESS_LICENSE,
                    "X-Customer":   str(NAVER_AD_CUSTOMER_ID),
                    "X-Signature":  _sign(ts, "GET", uri),
                },
                timeout=10,
            )
            if resp.status_code == 200:
                for item in resp.json().get("keywordList", []):
                    kw  = item.get("relKeyword", "")
                    vol = (_parse_search_volume(item.get("monthlyPcQcCnt"))
                           + _parse_search_volume(item.get("monthlyMobileQcCnt")))
                    volumes[kw] = vol
            else:
                print(f"  ⚠ 검색광고 API {resp.status_code}: {resp.text[:120]}")
        except Exception as e:
            print(f"  ⚠ 키워드 검색량 조회 실패: {e}")

    return volumes


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
    valid_list  = []
    checked     = 0

    for kw in keywords:
        try:
            resp = requests.get(
                "https://openapi.naver.com/v1/search/blog",
                params={"query": kw, "display": 10},
                headers=api_headers,
                timeout=8,
            )
            if resp.status_code != 200:
                time.sleep(0.2)
                continue

            items = resp.json().get("items", [])
            for rank_i, item in enumerate(items, start=1):
                link         = item.get("link", "")
                blogger_link = item.get("bloggerlink", "")
                if blog_id.lower() in link.lower() or blog_id.lower() in blogger_link.lower():
                    valid_count += 1
                    valid_list.append({"name": kw, "rank": rank_i, "volume": 0})
                    break

            checked += 1
        except Exception as e:
            print(f"  키워드 '{kw}' 조회 실패(skip): {e}")

        time.sleep(0.2)

    print(f"  유효키워드: {valid_count}/{len(keywords)}개 (조회 {checked}건)")

    # 검색광고 API로 검색량 보강
    if valid_list:
        kw_names = [k["name"] for k in valid_list]
        volumes  = fetch_keyword_volumes(kw_names)
        if volumes:
            for k in valid_list:
                k["volume"] = volumes.get(k["name"], 0)
            print(f"  검색량 조회 완료: {len(volumes)}개")

    return valid_count, valid_list


# ──────────────────────────────────────────────
# 단일 블로그 테스트 출력
# ──────────────────────────────────────────────
def test_blog(blog_id: str):
    print(f"\n[테스트] {blog_id}")
    print("─" * 45)

    nickname = fetch_nickname(blog_id)
    visitors = fetch_visitors(blog_id)
    posts    = fetch_posts(blog_id)
    all_tags = fetch_rss_tags(blog_id)
    tags     = all_tags[:350]

    print(f"닉네임      : {nickname}")
    print(f"오늘 방문자 : {visitors['today']:,}명")
    print(f"어제 방문자 : {visitors['yesterday']:,}명")
    print(f"5일 합계    : {visitors['week_total']:,}명")
    print(f"오늘 포스팅 : {posts['today_count']}개")
    print(f"RSS 태그    : 전체 {len(all_tags)}개 → 상위 100개 조회")

    if NAVER_CLIENT_ID:
        result = count_valid_keywords(blog_id, tags)
        valid_cnt = result[0] if isinstance(result, tuple) else result
        print(f"유효키워드  : {valid_cnt}개")
    else:
        print("유효키워드  : API 키 미설정 (NAVER_CLIENT_ID 환경변수 필요)")

    print(f"\n[최근 포스팅]")
    for p in posts["posts"][:5]:
        print(f"  {p['date']}  {p['title'][:40]}")


# ──────────────────────────────────────────────
# 전체 수집 실행
# ──────────────────────────────────────────────
def run_collection(single_id_override: str | None = None, reverse_order: bool = False, progress_callback=None, challenge_id_override: str | None = None):
    print("=" * 50)
    print("문봇 블로그 데이터 수집")
    print(datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 50)

    if not firebase_admin._apps:
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("FIREBASE_PROJECT_ID")
        cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or (SERVICE_ACCOUNT_KEY if os.path.exists(SERVICE_ACCOUNT_KEY) else "")
        if cred_path:
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred, {"projectId": project_id} if project_id else None)
        else:
            firebase_admin.initialize_app(options={"projectId": project_id} if project_id else None)
    db = firestore.client()

    # 활성 챌린지 (오버라이드 가능)
    if challenge_id_override:
        challenge_id = challenge_id_override
        print(f"[챌린지 오버라이드] {challenge_id}")
    else:
        active_doc = db.collection("settings").document("active").get()
        if not active_doc.exists:
            print("활성 챌린지 없음. 관리자 패널에서 챌린지를 먼저 등록하세요.")
            return
        challenge_id = active_doc.to_dict().get("challengeId")

    # 챌린지 정보
    ch_doc = db.collection("challenges").document(challenge_id).get()
    challenge_start = None
    challenge_end   = None
    weekday_only    = False
    goal_per_day    = 1
    total_days      = 25
    if ch_doc.exists:
        ch_data      = ch_doc.to_dict()
        start_ts     = ch_data.get("startDate")
        end_ts       = ch_data.get("endDate")
        if start_ts:
            challenge_start = start_ts.replace(tzinfo=KST) if hasattr(start_ts, "replace") else start_ts.astimezone(KST)
        if end_ts:
            challenge_end = end_ts.replace(tzinfo=KST) if hasattr(end_ts, "replace") else end_ts.astimezone(KST)
        weekday_only        = ch_data.get("weekdayOnly", False)
        goal_per_day        = int(ch_data.get("goalPostsPerDay", 1) or 1)
        total_days          = int(ch_data.get("totalDays", 25) or 25)
        skip_start_visitors = bool(ch_data.get("skipStartVisitors", False))
    # 점수/등급 설정
    score_doc    = db.collection("settings").document("scoreConfig").get()
    score_config = score_doc.to_dict() if score_doc.exists else {}

    participants_ref = db.collection("challenges").document(challenge_id).collection("participants")
    participants = list(participants_ref.stream())
    if reverse_order:
        participants.reverse()

    # 단일 블로그 수집 모드 체크 (Firestore 신호 또는 환경변수)
    single_id = (single_id_override if single_id_override is not None else os.environ.get("BLOG_ID", "")).strip()
    if not single_id and single_id_override is None:
        sc_doc = db.collection("settings").document("singleCollect").get()
        if sc_doc.exists:
            sc = sc_doc.to_dict()
            req_blog_id = sc.get("blogId", "").strip()
            req_at_str  = sc.get("requestedAt", "")
            if req_blog_id and req_at_str:
                try:
                    req_at = datetime.fromisoformat(req_at_str)
                    if req_at.tzinfo is None:
                        req_at = req_at.replace(tzinfo=KST)
                    age_sec = (datetime.now(KST) - req_at).total_seconds()
                    if age_sec < 600:  # 10분 이내 요청만 유효
                        single_id = req_blog_id
                except Exception:
                    pass
            # 요청 문서 삭제 (1회성)
            db.collection("settings").document("singleCollect").delete()

    if single_id:
        participants = [p for p in participants if p.to_dict().get("blogId", "").strip() == single_id]
        if not participants:
            print(f"[단일 수집] '{single_id}' 참가자를 찾을 수 없습니다.")
            return
        print(f"[단일 수집] {single_id}\n")
    else:
        direction = "아래에서 위" if reverse_order else "위에서 아래"
        print(f"참가자: {len(participants)}명 ({direction})\n")

    total_participants = len(participants)
    for idx, snap in enumerate(participants, start=1):
        p = snap.to_dict()
        blog_id = p.get("blogId", "").strip()
        if not blog_id:
            continue

        if progress_callback:
            progress_callback(idx, total_participants, blog_id, p.get("nickname", ""))

        print(f"수집: {blog_id} ({p.get('nickname', '')})")
        try:
            # ── 1. 메타 ──────────────────────────────────────
            meta     = fetch_blog_meta(blog_id)
            nickname = meta["nickname"] if meta["nickname"] != blog_id else p.get("nickname", blog_id)

            # ── 2. 방문자 수 ──────────────────────────────────
            visitors    = fetch_visitors(blog_id)
            today_str   = datetime.now(KST).strftime("%Y-%m-%d")
            visitor_log = dict(p.get("visitorLog") or {})
            if visitors["today"] > 0:
                visitor_log[today_str] = visitors["today"]
            yesterday_str = (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
            if visitors["yesterday"] > 0:
                visitor_log[yesterday_str] = visitors["yesterday"]
            start_visitors = p.get("startVisitors") or 0
            auto_start = False
            if not skip_start_visitors and start_visitors == 0:
                # startVisitors 미설정이면 챌린지 전 5일 평균으로 자동 계산
                # (weekdayOnly 챌린지는 그 중 평일만 평균)
                auto_start = True
                start_visitors = calc_pre_challenge_avg(visitors["daily"], challenge_start, weekday_only)
                if start_visitors > 0:
                    period_label = "챌린지 전 평일 평균" if weekday_only else "챌린지 전 5일 평균"
                    print(f"  startVisitors 자동 설정: {start_visitors:,} ({period_label})")
            new_curr = calc_challenge_week_avg_visitors(visitor_log, challenge_start, weekday_only)

            # ── 3. 포스팅 수 (네이버 글목록 API - RSS 50개 제한 없음) ──
            posts = fetch_posts_full(blog_id, challenge_start, challenge_end, weekday_only)

            # ── 4. 프로필 이미지 ─────────────────────────────────
            # 항상 최신 프로필 수집 (기존 base64 이미지가 있어도 갱신)
            stored_img  = p.get("profileImg", "")
            rss_img     = fetch_posts(blog_id).get("rss_img_url", "")  # 프로필 이미지 후보용 (가벼운 RSS 조회)
            naver_img   = meta.get("profileImg", "")
            profile_img = fetch_profile_as_base64(blog_id, rss_img, naver_img) or stored_img

            # ── 5. 유효키워드 수집 ────────────────────────────
            # 기본 수집에서는 속도를 위해 키워드 검증을 건너뜁니다.
            # 정밀 진단이 필요할 때만 RUN_KEYWORDS=1 환경변수로 실행하세요.
            if RUN_KEYWORDS:
                tags = fetch_rss_tags(blog_id)[:350]
                result_kw = count_valid_keywords(blog_id, tags)
                if isinstance(result_kw, tuple):
                    valid_cnt, valid_list = result_kw
                else:
                    valid_cnt, valid_list = result_kw, []
                current_kw     = valid_cnt  if valid_cnt  >= 0 else p.get("currentKeywords", 0)
                current_kwlist = valid_list if valid_cnt >= 0 else p.get("validKeywordList", [])
            else:
                current_kw     = p.get("currentKeywords", 0)
                current_kwlist = p.get("validKeywordList", [])

            # ── 6. 주제 자동 감지 ─────────────────────────────
            post_titles    = [pp["title"] for pp in posts["posts"]]
            categories     = meta.get("categories", [])
            detected_topic = detect_topic(post_titles, categories)

            # ── 7. 통합 점수 계산 (v2) ────────────────────────
            avg7d          = new_curr
            # RSS 기반 일별 포스팅 수를 기존 기록과 병합 (수동 수정값 보호: 날짜별 최댓값 유지)
            new_daily_log      = calc_daily_post_log(posts["posts"], challenge_start, weekday_only)
            existing_daily_log = dict(p.get("dailyPostLog") or {})
            all_dates          = set(existing_daily_log) | set(new_daily_log)
            daily_post_log     = {dt: max(existing_daily_log.get(dt, 0), new_daily_log.get(dt, 0)) for dt in all_dates}
            consec_days        = calc_consecutive_days(daily_post_log, challenge_start, goal_per_day, weekday_only)
            achieved_days      = calc_achieved_days(daily_post_log, challenge_start, challenge_end, goal_per_day, weekday_only)
            progress_rate      = round((achieved_days / total_days) * 100, 1) if total_days > 0 else 0.0
            act_score  = calc_activity_score(
                achieved_days = achieved_days,
                consec_days = consec_days,
                total_days  = total_days,
            )
            grow_score = calc_growth_score(
                curr_visitors  = new_curr,
                start_visitors = start_visitors,
                curr_keywords  = current_kw,
            )
            intg_score = calc_integrated_score(act_score, grow_score)
            level_cfg  = score_config.get("levelConfig", {})
            blog_level = score_to_blog_level(intg_score, level_cfg)

            # ── 8. Firestore 업데이트 ─────────────────────────
            # RSS는 최근 50개 글만 반환하므로, 챌린지 기간 포스팅이 그 창을 벗어나면
            # 기존에 저장해둔 recentPosts(링크 포함)를 유지하고 새 항목만 추가 병합한다.
            new_recent_posts      = [
                pp for pp in posts["posts"]
                if (not challenge_start or pp["date"][:10] >= challenge_start.strftime("%Y-%m-%d"))
                and (not challenge_end   or pp["date"][:10] <= challenge_end.strftime("%Y-%m-%d"))
            ]
            def _post_key(rp):
                # logNo 필드가 없는 과거 저장 항목은 링크에서 숫자 ID를 추출해 대조한다
                # (RSS 링크 포맷 → 글목록 API 링크 포맷으로 바뀌어도 동일 글로 인식)
                if rp.get("logNo"):
                    return str(rp["logNo"])
                m = re.search(r"/(\d+)(?:\?|$)", rp.get("link", ""))
                return m.group(1) if m else rp.get("link")

            existing_recent_posts = list(p.get("recentPosts") or [])
            existing_keys          = {_post_key(rp) for rp in existing_recent_posts}
            merged_recent_posts    = existing_recent_posts + [
                rp for rp in new_recent_posts if _post_key(rp) not in existing_keys
            ]
            merged_recent_posts.sort(key=lambda rp: rp.get("date", ""), reverse=True)

            update_data = {
                "nickname"           : nickname,
                "profileImg"         : profile_img,
                "todayVisitors"      : visitors["today"],
                "currentVisitors"    : new_curr,
                "visitorDataAvailable": visitors.get("ok", True),
                "visitorLog"         : visitor_log,
                "weekVisitors"       : visitors["week_total"],
                "postCount"          : posts["challenge_count"],
                "todayPostCount"     : posts["today_count"],
                "recentPosts"        : merged_recent_posts,
                "currentKeywords"    : current_kw,
                "validKeywordList"   : current_kwlist,
                "dailyPostLog"       : daily_post_log,
                "consecutiveDays"    : consec_days,
                "achievedDays"       : achieved_days,
                "progressRate"       : progress_rate,
                "score"              : intg_score,
                "totalScore"         : intg_score,
                "totalActivityScore" : act_score,
                "growthScore"        : grow_score,
                "blogLevelScore"     : intg_score,
                "averageVisitors7D"  : avg7d,
                "currentBlogLevel"   : blog_level["label"],
                "updatedAt"          : firestore.SERVER_TIMESTAMP,
                "lastError"          : None,
            }
            if auto_start and start_visitors > 0:
                update_data["startVisitors"] = start_visitors
            db.collection("blog_cache").document(blog_id).set({
                "blogId"          : blog_id,
                "nickname"        : nickname,
                "profileImg"      : profile_img,
                "topic"           : detected_topic,
                "todayVisitors"   : visitors["today"],
                "weekVisitors"    : visitors["week_total"],
                "todayPosts"      : posts["today_count"],
                "currentKeywords" : current_kw,
                "fetchedAt"       : datetime.now(KST).isoformat(),
            }, merge=True)
            participants_ref.document(snap.id).update(update_data)

            print(f"  ✓ {nickname} | 포스팅={posts['challenge_count']}개 | 달성일={achieved_days}/{total_days}일({progress_rate}%) | 연속={consec_days}일"
                  f" | 7일평균={avg7d:,.0f} | 키워드={current_kw}"
                  f" | 활동={act_score} 성장={grow_score} 통합={intg_score} | 등급={blog_level['label']}")

        except Exception as e:
            import traceback
            err_msg = f"{type(e).__name__}: {e}"
            print(f"  ✗ [ERROR] {blog_id} 수집 실패 → 다음으로 넘어감")
            traceback.print_exc()
            try:
                participants_ref.document(snap.id).update({
                    "lastError": {
                        "message": err_msg,
                        "at"     : datetime.now(KST).isoformat(),
                    }
                })
            except Exception:
                pass

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
