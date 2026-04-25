# -*- coding: utf-8 -*-
"""
문봇 블로그랩 - 네이버 블로그 데이터 수집기
- 닉네임   : requests + BeautifulSoup
- 방문자 수 : NVisitorgp4Ajax API
- 포스팅 수 : RSS 피드 (rss.blog.naver.com)
- Firebase Firestore 자동 업데이트

사용법:
  python collect.py                  # 전체 참가자 업데이트
  python collect.py tripatdawn       # 특정 블로그 테스트 출력
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore

# GitHub Actions: serviceAccountKey.json / 로컬: 원본 파일명
import os as _os
SERVICE_ACCOUNT_KEY = (
    "serviceAccountKey.json"
    if _os.path.exists("serviceAccountKey.json")
    else "moonbotrank-firebase-adminsdk-fbsvc-84d19b29e1.json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://www.naver.com/",
}

KST = timezone(timedelta(hours=9))


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
    result = {"today_count": 0, "challenge_count": 0, "posts": []}
    try:
        r = requests.get(f"https://rss.blog.naver.com/{blog_id}.xml",
                         headers=HEADERS, timeout=10)
        root = ET.fromstring(r.content)
        channel = root.find("channel")
        if not channel:
            return result

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
# 단일 블로그 테스트 출력
# ──────────────────────────────────────────────
def test_blog(blog_id: str):
    print(f"\n[테스트] {blog_id}")
    print("─" * 45)

    nickname = fetch_nickname(blog_id)
    visitors = fetch_visitors(blog_id)
    posts    = fetch_posts(blog_id)

    print(f"닉네임      : {nickname}")
    print(f"오늘 방문자 : {visitors['today']:,}명")
    print(f"어제 방문자 : {visitors['yesterday']:,}명")
    print(f"5일 합계    : {visitors['week_total']:,}명")
    print(f"오늘 포스팅 : {posts['today_count']}개")
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

        # 닉네임/프로필 이미지 (blog_id 그대로면 새로 조회)
        meta = fetch_blog_meta(blog_id)
        nickname   = meta["nickname"] if meta["nickname"] != blog_id else p.get("nickname", blog_id)
        profile_img = meta.get("profileImg") or p.get("profileImg", "")

        visitors = fetch_visitors(blog_id)
        posts    = fetch_posts(blog_id, challenge_start)

        today_v   = visitors["today"]
        prev_curr = p.get("currentVisitors") or p.get("startVisitors") or 0
        new_curr  = prev_curr + today_v if today_v > 0 else prev_curr

        update_data = {
            "nickname"        : nickname,
            "profileImg"      : profile_img,
            "todayVisitors"   : today_v,
            "currentVisitors" : new_curr,
            "weekVisitors"    : visitors["week_total"],
            "postCount"       : posts["challenge_count"],   # 챌린지 기간 누적 포스팅
            "todayPostCount"  : posts["today_count"],       # 오늘 포스팅 수
            "recentPosts"     : posts["posts"][:10],        # 최근 10개 제목+날짜
            "updatedAt"       : firestore.SERVER_TIMESTAMP,
        }

        merged = {**p, **update_data}
        update_data["score"] = calc_score(merged, score_config)

        # blog_cache 저장 (자동조회용)
        db.collection("blog_cache").document(blog_id).set({
            "blogId"       : blog_id,
            "nickname"     : nickname,
            "profileImg"   : profile_img,
            "todayVisitors": today_v,
            "weekVisitors" : visitors["week_total"],
            "todayPosts"   : posts["today_count"],
            "fetchedAt"    : datetime.now(KST).isoformat(),
        }, merge=True)

        participants_ref.document(snap.id).update(update_data)

        print(f"  닉네임={nickname} | 오늘방문자={today_v:,} | 누적방문자={new_curr:,} "
              f"| 챌린지포스팅={posts['challenge_count']} | 오늘포스팅={posts['today_count']} "
              f"| 점수={update_data['score']}")
        time.sleep(1.5)

    print(f"\n완료: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_blog(sys.argv[1])
    else:
        run_collection()
