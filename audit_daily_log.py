# -*- coding: utf-8 -*-
"""
전 참가자 dailyPostLog 정합성 점검
- Naver PostTitleListAsync (RSS 50개 제한 없음)로 실제 발행 이력을 페이지네이션하며 수집
- Firestore에 저장된 dailyPostLog와 날짜별로 대조
- 실제 > 저장값 인 날짜만 리포트 (과소 집계 케이스)
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import re
import time
from datetime import datetime, timedelta, timezone

import requests
import firebase_admin
from firebase_admin import credentials, firestore

KST = timezone(timedelta(hours=9))
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://blog.naver.com/",
}

cred = credentials.Certificate("moonsaboochallenge-firebase-adminsdk-fbsvc-5ce8261153.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

active = db.collection("settings").document("active").get()
cid = active.to_dict().get("challengeId")
ch = db.collection("challenges").document(cid).get().to_dict()

start_date = ch["startDate"].astimezone(KST).date()
end_ts = ch.get("endDate")
end_date = end_ts.astimezone(KST).date() if end_ts else datetime.now(KST).date()
today = datetime.now(KST).date()
end_date = min(end_date, today)
weekday_only = bool(ch.get("weekdayOnly", False))

print(f"챌린지 기간: {start_date} ~ {end_date} (weekdayOnly={weekday_only})")


def parse_add_date(s: str, now: datetime):
    s = s.strip()
    if "전" in s or s == "방금":
        return now.date()
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?", s)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d).date()
    return None


def fetch_real_daily_counts(blog_id: str, start_date, end_date, weekday_only: bool, max_pages: int = 20):
    """start_date까지 페이지네이션하며 실제 날짜별 포스팅 수 집계.
    총 개수(totalCount) 도달 시 중단 + logNo 중복 제거로, 네이버가 마지막 페이지
    이후 동일 페이지를 반복 반환하는 현상에 대비한다."""
    counts = {}
    seen_lognos = set()
    now = datetime.now(KST)
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
            import json as _json
            data = _json.loads(r.text.replace("\\'", "'"))
        except Exception as e:
            print(f"  [{blog_id}] page {page} 요청 실패: {e}")
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
        new_item_seen = False
        for item in posts:
            log_no = item.get("logNo")
            if log_no in seen_lognos:
                continue
            seen_lognos.add(log_no)
            new_item_seen = True

            d = parse_add_date(item.get("addDate", ""), now)
            if d is None:
                continue
            oldest_on_page = d if oldest_on_page is None or d < oldest_on_page else oldest_on_page
            if start_date <= d <= end_date:
                if not (weekday_only and d.weekday() >= 5):
                    counts[d.isoformat()] = counts.get(d.isoformat(), 0) + 1

        # 더 이상 새로운 글이 없으면(마지막 페이지 반복) 중단
        if not new_item_seen:
            break
        if total_count is not None and page * 30 >= total_count:
            break
        if oldest_on_page and oldest_on_page <= start_date:
            break
        page += 1
        time.sleep(0.4)

    return counts


parts = list(db.collection("challenges").document(cid).collection("participants").stream())
print(f"참가자 {len(parts)}명 점검 시작\n")

mismatches_found = False
for snap in parts:
    p = snap.to_dict()
    blog_id = (p.get("blogId") or "").strip()
    nickname = p.get("nickname") or blog_id
    if not blog_id:
        continue

    stored_log = p.get("dailyPostLog") or {}
    real_counts = fetch_real_daily_counts(blog_id, start_date, end_date, weekday_only)

    diffs = []
    for d, real_cnt in real_counts.items():
        stored_cnt = stored_log.get(d, 0)
        if real_cnt > stored_cnt:
            diffs.append((d, stored_cnt, real_cnt))

    if diffs:
        mismatches_found = True
        print(f"[!] {blog_id} ({nickname}) - {len(diffs)}건 과소집계")
        for d, stored_cnt, real_cnt in sorted(diffs):
            print(f"     {d}: 저장={stored_cnt}  실제={real_cnt}")
    else:
        print(f"[OK] {blog_id} ({nickname}) - 이상 없음")

    time.sleep(0.5)

print("\n완료.")
if not mismatches_found:
    print("모든 참가자 정상 - 추가 정정 불필요")
