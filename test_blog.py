# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import re, sys, time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

def fetch_blog_info(blog_id):
    result = {"blogId": blog_id, "nickname": blog_id, "todayVisitors": 0, "totalVisitors": 0}

    # ── 1. 닉네임 (requests 충분) ──────────────────────────────
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
        for sel in [".nickName", ".nick", ".blog_title", ".BlogTitle", "[class*='nick']"]:
            el = soup2.select_one(sel)
            if el and el.get_text(strip=True):
                result["nickname"] = el.get_text(strip=True)
                break
    except Exception as e:
        print(f"닉네임 실패: {e}")

    # ── 2. 방문자 수 (Selenium, PostList URL로 사이드바 접근) ───
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By

        opts = Options()
        opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1280,900")
        opts.add_argument(f"user-agent={HEADERS['User-Agent']}")

        driver = webdriver.Chrome(options=opts)

        # PostList URL → 사이드바가 있는 레이아웃
        target_url = f"https://blog.naver.com/PostList.naver?blogId={blog_id}"
        driver.get(target_url)
        time.sleep(3)

        # iframe 진입
        try:
            iframe = driver.find_element(By.ID, "mainFrame")
            driver.switch_to.frame(iframe)
            time.sleep(2)
        except Exception:
            pass

        body_text = driver.find_element(By.TAG_NAME, "body").text

        # 방문자 수 패턴 탐색
        patterns = [
            r"Today\s+([0-9,]+)\s+Total\s+([0-9,]+)",
            r"오늘\s+([0-9,]+)\s+전체\s+([0-9,]+)",
            r"Today[^0-9]*([0-9,]+)[^0-9]*Total[^0-9]*([0-9,]+)",
        ]
        for pat in patterns:
            m = re.search(pat, body_text, re.IGNORECASE | re.DOTALL)
            if m:
                result["todayVisitors"] = int(m.group(1).replace(",", ""))
                result["totalVisitors"] = int(m.group(2).replace(",", ""))
                break

        # 못 찾으면 디버그용 텍스트 출력
        if result["totalVisitors"] == 0:
            lines = [l.strip() for l in body_text.split("\n") if l.strip()]
            print("[디버그 텍스트 (앞 40줄)]")
            for l in lines[:40]:
                print(" ", l)

        driver.quit()

    except Exception as e:
        print(f"Selenium 실패: {e}")

    print(f"\n닉네임: {result['nickname']}")
    print(f"오늘 방문자: {result['todayVisitors']:,}")
    print(f"전체 방문자: {result['totalVisitors']:,}")
    return result


if __name__ == "__main__":
    blog_id = sys.argv[1] if len(sys.argv) > 1 else "tripatdawn"
    fetch_blog_info(blog_id)
