# -*- coding: utf-8 -*-
"""
자정 근처(23시대) 작성 글이 수집 시점(자정 직후)에 다음날 글로
잘못 분류되는 버그의 영향 범위를 점검하는 1회성 진단 스크립트.
각 참가자의 최근 글 목록(PostTitleListAsync 1페이지)을 조회해
기존(내림) 방식과 수정(상한 보정) 방식의 날짜 판정이 다른 글을 찾아낸다.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import re
import json
import requests
from datetime import datetime, timedelta

import firebase_admin
from firebase_admin import credentials, firestore

from collect import SERVICE_ACCOUNT_KEY, HEADERS, KST

if not firebase_admin._apps:
    cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
    firebase_admin.initialize_app(cred)
db = firestore.client()


def parse_old(s, now_kst):
    s = (s or "").strip()
    if s == "방금":
        return now_kst.date()
    m = re.match(r"(\d+)\s*분\s*전", s)
    if m:
        return (now_kst - timedelta(minutes=int(m.group(1)))).date()
    m = re.match(r"(\d+)\s*시간\s*전", s)
    if m:
        return (now_kst - timedelta(hours=int(m.group(1)))).date()
    if "전" in s:
        return now_kst.date()
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?", s)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d).date()
    return None


def parse_new(s, now_kst):
    s = (s or "").strip()
    if s == "방금":
        return now_kst.date()
    m = re.match(r"(\d+)\s*분\s*전", s)
    if m:
        return (now_kst - timedelta(minutes=int(m.group(1)), seconds=59)).date()
    m = re.match(r"(\d+)\s*시간\s*전", s)
    if m:
        return (now_kst - timedelta(hours=int(m.group(1)), minutes=59, seconds=59)).date()
    if "전" in s:
        return now_kst.date()
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?", s)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d).date()
    return None


def main():
    active_doc = db.collection("settings").document("active").get()
    challenge_id = active_doc.to_dict().get("challengeId")
    participants = [p.to_dict() for p in
                    db.collection("challenges").document(challenge_id).collection("participants").stream()]

    now_kst = datetime.now(KST)
    print(f"기준 시각(KST): {now_kst.strftime('%Y-%m-%d %H:%M:%S')}\n")

    affected = []
    for p in participants:
        blog_id = p.get("blogId", "").strip()
        nickname = p.get("nickname", "")
        if not blog_id:
            continue
        try:
            r = requests.get(
                "https://blog.naver.com/PostTitleListAsync.naver",
                params={"blogId": blog_id, "currentPage": 1, "categoryNo": 0,
                        "parentCategoryNo": "", "countPerPage": 30},
                headers=HEADERS, timeout=10,
            )
            data = json.loads(r.text.replace("\\'", "'"))
        except Exception as e:
            print(f"{blog_id}: 조회 실패 - {e}")
            continue

        for item in data.get("postList", []):
            add_date = item.get("addDate", "")
            old_d = parse_old(add_date, now_kst)
            new_d = parse_new(add_date, now_kst)
            if old_d != new_d:
                title = item.get("title", "")
                log_no = item.get("logNo")
                affected.append((blog_id, nickname, title, add_date, old_d, new_d, log_no))
                print(f"[영향] {blog_id}({nickname}) logNo={log_no} addDate='{add_date}' "
                      f"기존판정={old_d} → 수정판정={new_d} : {title[:40]}")

    print(f"\n총 영향받은 글: {len(affected)}건")
    if not affected:
        print("영향받은 참가자 없음 - 지금 시점 기준으로는 다른 참가자에게 동일 문제 없음.")


if __name__ == "__main__":
    main()
