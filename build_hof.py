# -*- coding: utf-8 -*-
"""
챌린지 종료 후 명예의 전당(hall_of_fame) 문서를 생성/갱신하는 스크립트.

기준: 챌린지 100% 완주자 중 방문자 절대 증가량(currentVisitors - startVisitors) 순위.
완주자는 전원 순위를 매기되, 방문자가 늘지 않았거나 통계 자체가 없는
사람은 마이너스/숫자를 노출하지 않고 "OO일 완주"만 표시한다.
챌린지 주최자 계정은 공정성을 위해 명예의 전당 순위에서 항상 제외한다.

사용법:
  python build_hof.py <challengeId>          # Firestore에 저장
  python build_hof.py <challengeId> --dry-run  # 저장 없이 결과만 출력
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import firebase_admin
from firebase_admin import credentials, firestore

from collect import SERVICE_ACCOUNT_KEY

# 챌린지 주최자 계정 - 본인 챌린지의 명예의 전당 순위에는 넣지 않는다
ORGANIZER_BLOG_IDS = {"newmoon929"}


def format_num(n: float) -> str:
    return str(int(n)) if float(n).is_integer() else f"{n:.1f}"


def build_hof(challenge_id: str, dry_run: bool = False):
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    ch = db.collection("challenges").document(challenge_id).get()
    if not ch.exists:
        print(f"챌린지를 찾을 수 없습니다: {challenge_id}")
        return
    ch_data     = ch.to_dict()
    total_days  = ch_data.get("totalDays", 0)
    start_date  = ch_data.get("startDate")
    year, month = (start_date.year, start_date.month) if start_date else (0, 0)

    participants = [p.to_dict() for p in
                    db.collection("challenges").document(challenge_id).collection("participants").stream()]

    completers = [p for p in participants if (p.get("progressRate") or 0) >= 99.9]
    organizers = [p for p in completers if p.get("blogId") in ORGANIZER_BLOG_IDS]
    completers = [p for p in completers if p.get("blogId") not in ORGANIZER_BLOG_IDS]
    print(f"전체 참가자 {len(participants)}명 중 완주자 {len(completers)}명"
          + (f" (주최자 {len(organizers)}명 제외)" if organizers else ""))

    # 전원 순위를 매기되, 정렬 우선순위는 [방문자 증가 > 방문자 데이터 있음(비증가) > 데이터 없음] 순.
    # 증가하지 않았거나 데이터가 없으면 화면에는 수치를 노출하지 않는다.
    entries = []
    for p in completers:
        has_visitor_data = p.get("visitorDataAvailable", True)
        diff = (p.get("currentVisitors") or 0) - (p.get("startVisitors") or 0)
        entries.append((p, diff, has_visitor_data))

    def sort_key(e):
        _, diff, has_data = e
        if not has_data:
            return (-2, 0)
        return (1 if diff > 0 else 0, diff)

    entries.sort(key=sort_key, reverse=True)

    awards = []
    for i, (p, diff, has_visitor_data) in enumerate(entries, start=1):
        emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, "")
        award = "최우수참여자" if i == 1 else "완주자"
        if not has_visitor_data:
            stat = f"{total_days}일 완주 · 방문자 통계 비공개"
        elif diff > 0:
            stat = (f"{total_days}일 완주 · 방문자 {diff:+.1f}명 "
                    f"({format_num(p.get('startVisitors') or 0)}→{format_num(p.get('currentVisitors') or 0)}명)")
        else:
            stat = f"{total_days}일 완주"
        awards.append({
            "rank": i, "emoji": emoji, "award": award, "stat": stat,
            "nickname": p.get("nickname", ""), "blogId": p.get("blogId", ""),
            "profileImg": p.get("profileImg", ""),
        })

    for a in awards:
        print(f"  [{a['rank']}위] {a['nickname']} ({a['blogId']}) - {a['stat']}")

    if dry_run:
        print("\n--dry-run: Firestore에 저장하지 않았습니다.")
        return

    db.collection("hall_of_fame").document(challenge_id).set({
        "year": year, "month": month,
        "challengeId": challenge_id,
        "challengeName": ch_data.get("name", ""),
        "totalDays": total_days,
        "awards": awards,
    })
    print(f"\n저장 완료: hall_of_fame/{challenge_id}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python build_hof.py <challengeId> [--dry-run]")
        sys.exit(1)
    build_hof(sys.argv[1], dry_run="--dry-run" in sys.argv)
