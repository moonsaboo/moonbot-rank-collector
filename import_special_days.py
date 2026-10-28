# -*- coding: utf-8 -*-
"""
공공데이터포털 - 한국천문연구원 특일 정보 API로 공휴일/국가기념일을 가져와
festival_calendar/all에 축제 데이터와 함께 병합 저장한다.

사용법:
  python import_special_days.py 2026              # 2026년 1~12월
  python import_special_days.py 2026 2027         # 2026~2027년
  python import_special_days.py 2026 --dry-run    # 저장 없이 결과만 출력

환경변수:
  DATA_GO_KR_API_KEY   공공데이터포털 "한국천문연구원_특일 정보" 서비스키
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import xml.etree.ElementTree as ET

try:
    from dotenv import load_dotenv
    _env_paths = [
        os.path.join(os.path.dirname(__file__), ".env"),
        os.path.join(os.path.dirname(__file__), "..", ".env"),
    ]
    for _p in _env_paths:
        if os.path.exists(_p):
            load_dotenv(_p)
            break
except ImportError:
    pass

import requests
import firebase_admin
from firebase_admin import credentials, firestore
from collect import SERVICE_ACCOUNT_KEY

API_KEY = os.environ.get("DATA_GO_KR_API_KEY", "")
BASE = "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/"


def fetch_month(op: str, year: int, month: int):
    """op: getAnniversaryInfo(기념일) / getRestDeInfo(공휴일)"""
    r = requests.get(BASE + op, params={
        "serviceKey": API_KEY, "solYear": str(year), "solMonth": f"{month:02d}",
        "numOfRows": "50", "pageNo": "1",
    }, timeout=10)
    root = ET.fromstring(r.content)
    result_code = root.findtext(".//resultCode")
    if result_code != "00":
        print(f"  [!] {op} {year}-{month:02d} 실패: {root.findtext('.//resultMsg')}")
        return []
    out = []
    for item in root.findall(".//item"):
        locdate = item.findtext("locdate")
        name = item.findtext("dateName")
        if not locdate or not name:
            continue
        d = f"{locdate[0:4]}-{locdate[4:6]}-{locdate[6:8]}"
        out.append({"title": name.strip(), "start": d, "end": d})
    return out


def collect_years(years):
    anniversaries, holidays = [], []
    for y in years:
        for m in range(1, 13):
            anniversaries += fetch_month("getAnniversaryInfo", y, m)
            holidays += fetch_month("getRestDeInfo", y, m)
    return anniversaries, holidays


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry_run = "--dry-run" in sys.argv
    if not args:
        print("사용법: python import_special_days.py <year> [year2] [--dry-run]")
        sys.exit(1)
    years = [int(a) for a in args]

    if not API_KEY:
        print("DATA_GO_KR_API_KEY가 설정되지 않았습니다 (.env 확인)")
        sys.exit(1)

    print(f"조회 연도: {years}")
    anniversaries, holidays = collect_years(years)
    print(f"기념일 {len(anniversaries)}건, 공휴일 {len(holidays)}건 수집")

    def tag(events, type_):
        out = []
        for e in events:
            e2 = dict(e)
            e2["type"] = type_
            e2["id"] = f"{e2['start']}_{e2['end']}_{type_}_{e2['title']}"
            out.append(e2)
        return out

    new_events = tag(anniversaries, "anniversary") + tag(holidays, "holiday")

    print("\n샘플:")
    for e in new_events[:10]:
        print(f"  [{e['type']}] {e['start']} {e['title']}")

    if dry_run:
        print("\n--dry-run: 저장하지 않았습니다.")
        return

    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    doc_ref = db.collection("festival_calendar").document("all")
    snap = doc_ref.get()
    existing = snap.to_dict().get("events", []) if snap.exists else []
    # 기존 축제(festival) 데이터는 유지하고, 기념일/공휴일만 이번에 가져온 걸로 교체한다
    # (같은 연도를 다시 돌리면 중복 없이 최신으로 덮어써짐)
    kept = [e for e in existing if e.get("type", "festival") == "festival" or e.get("id", "").split("_")[0][:4] not in [str(y) for y in years]]
    merged = {e["id"]: e for e in kept}
    for e in new_events:
        merged[e["id"]] = e
    final_events = list(merged.values())

    source_files = list(set((snap.to_dict().get("sourceFiles", []) if snap.exists else []) + [f"공공데이터포털 특일정보 {years}"]))
    doc_ref.set({
        "events": final_events,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "updatedBy": "import_special_days.py",
        "sourceFiles": source_files,
    })
    print(f"\n저장 완료: festival_calendar/all (전체 {len(final_events)}개, 신규/갱신 {len(new_events)}개)")


if __name__ == "__main__":
    main()
