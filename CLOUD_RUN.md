# Cloud Run 배포 메모

Cloud Run에는 `scraper/` 폴더를 서비스로 배포합니다.

권장 설정:

- Service name: `moonsaboo-collect-api`
- Region: `asia-northeast3` 또는 `asia-northeast1`
- Authentication: unauthenticated 허용
- Minimum instances: `0`
- Maximum instances: `1`
- Memory: `1Gi`
- Timeout: `1800s`
- Environment variables:
  - `FIREBASE_PROJECT_ID=moonsaboochallenge`
  - `ALLOWED_ORIGINS=https://moonsaboochallenge.web.app,http://localhost:5177`

Google Cloud SDK가 설치되어 있으면:

```powershell
gcloud auth login
gcloud config set project moonsaboochallenge
gcloud run deploy moonsaboo-collect-api `
  --source .\scraper `
  --region asia-northeast3 `
  --allow-unauthenticated `
  --memory 1Gi `
  --timeout 1800 `
  --max-instances 1 `
  --min-instances 0 `
  --set-env-vars FIREBASE_PROJECT_ID=moonsaboochallenge,ALLOWED_ORIGINS=https://moonsaboochallenge.web.app,http://localhost:5177
```

배포 후 나온 URL을 `public/js/runtime-config.js`의 `window.COLLECT_API_URL`에 넣고 Firebase Hosting을 다시 배포합니다.
