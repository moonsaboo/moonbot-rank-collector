# -*- coding: utf-8 -*-
"""
챌린지 종료 후 명예의 전당(hall_of_fame) 문서를 생성/갱신하는 스크립트.

기준: 챌린지 100% 완주자 중 방문자 절대 증가량(currentVisitors - startVisitors) 순위.
방문자가 증가한 완주자만 1위부터 순번을 매겨 큰 카드로 보여주고,
방문자가 늘지 않았거나(감소·0) 통계 자체가 없는 완주자는 마이너스 수치가
단독으로 부각되지 않도록 순위 없이 "순위 미반영 완주자" 목록에 함께 담는다.

사용법:
  python build_hof.py <challengeId>          # Firestore에 저장
  python build_hof.py <challengeId> --dry-run  # 저장 없이 결과만 출력
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import firebase_admin
from firebase_admin import credentials, firestore

from collect import SERVICE_ACCOUNT_KEY


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
    print(f"전체 참가자 {len(participants)}명 중 완주자 {len(completers)}명")

    # 방문자가 증가한 완주자만 순위(큰 카드)를 매기고, 그렇지 않은 완주자는
    # (감소·변화없음·데이터없음 불문) 전부 순위 없이 한 그룹으로 묶어
    # 특정 인원이 혼자 눈에 띄지 않게 한다.
    ranked, unranked = [], []
    for p in completers:
        has_visitor_data = p.get("visitorDataAvailable", True)
        diff = (p.get("currentVisitors") or 0) - (p.get("startVisitors") or 0)
        if has_visitor_data and diff > 0:
            ranked.append((p, diff))
        else:
            unranked.append((p, diff, has_visitor_data))

    ranked.sort(key=lambda x: -x[1])

    awards = []
    for i, (p, diff) in enumerate(ranked, start=1):
        emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, "")
        award = "최우수참여자" if i == 1 else "완주자"
        stat = (f"{total_days}일 완주 · 방문자 {diff:+.1f}명 "
                f"({format_num(p.get('startVisitors') or 0)}→{format_num(p.get('currentVisitors') or 0)}명)")
        awards.append({
            "rank": i, "emoji": emoji, "award": award, "stat": stat,
            "nickname": p.get("nickname", ""), "blogId": p.get("blogId", ""),
            "profileImg": p.get("profileImg", ""),
        })

    for p, diff, has_visitor_data in unranked:
        stat = f"{total_days}일 완주" if has_visitor_data else f"{total_days}일 완주 · 방문자 통계 비공개"
        awards.append({
            "rank": 0, "emoji": "", "award": "완주자", "stat": stat,
            "nickname": p.get("nickname", ""), "blogId": p.get("blogId", ""),
            "profileImg": p.get("profileImg", ""),
        })

    for a in awards:
        tag = f"{a['rank']}위" if a["rank"] else "순위미반영"
        print(f"  [{tag}] {a['nickname']} ({a['blogId']}) - {a['stat']}")

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
