# 수집 API 서버 실행 가이드

GitHub Actions를 수동으로 깨우는 대신, 관리자 화면에서 이 API 서버를 바로 호출합니다.

## 로컬 실행

```bash
cd scraper
pip install -r requirements.txt
python server.py
```

기본 주소는 `http://localhost:5000` 입니다.

## Render/Railway 배포

`scraper` 폴더를 서비스 루트로 잡고 배포합니다.

- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn server:app --bind 0.0.0.0:$PORT --timeout 900`

`Procfile`도 같은 명령으로 준비되어 있습니다.

## 필요한 파일/환경변수

- `serviceAccountKey.json` 또는 `moonbotrank-firebase-adminsdk-*.json`
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`
- `NAVER_AD_CUSTOMER_ID`
- `NAVER_AD_ACCESS_LICENSE`
- `NAVER_AD_SECRET_KEY`
- `ALLOWED_ORIGINS`: 허용할 프론트 주소. 예: `https://moonbotrank.web.app,http://localhost:5000`

## 프론트 연결

배포한 API 주소를 `public/js/runtime-config.js`에 넣습니다.

```js
window.COLLECT_API_URL = "https://your-collect-api.example.com";
```

Firebase Hosting에서 같은 도메인의 `/api`로 프록시한다면 빈 문자열로 둘 수 있습니다.

## 엔드포인트

- `POST /api/collect`: 전체 수집 시작
- `POST /api/collect` with `{ "blogId": "naverId" }`: 특정 블로그만 수집
- `GET /api/collect/status`: 실행 상태 확인
- `GET /healthz`: 서버 상태 확인

모든 `/api/collect*` 요청은 Firebase 로그인 ID 토큰이 필요하고, `admins/{uid}` 문서가 있는 사용자만 실행할 수 있습니다.
