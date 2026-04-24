# 배포 가이드

## 구성

| 컴포넌트 | 호스팅 | URL 형태 |
|---|---|---|
| 프론트 (Next.js) | Vercel | `https://<project>.vercel.app` |
| 백엔드 (FastAPI) | Render | `https://<service>.onrender.com` |
| 소스 | GitHub | `https://github.com/OXODOX/<repo>` |

## 최초 배포 순서

### 1. GitHub repo 준비
1. https://github.com/new 에서 repo 생성 (예: `real-estate-price`)
2. Public/Private 어느 쪽이든 OK. 무료 배포 둘 다 가능.
3. 로컬에서 push:
   ```bash
   cd /path/to/real-estate-price
   git init
   git add .
   git commit -m "Initial commit"
   git branch -M main
   git remote add origin https://github.com/OXODOX/real-estate-price.git
   git push -u origin main
   ```

### 2. Render 에서 백엔드 배포
1. https://dashboard.render.com 접속 (GitHub 로 로그인)
2. **New +** → **Blueprint** 클릭
3. GitHub repo 연결 → `real-estate-price` 선택
4. Render 가 `render.yaml` 을 자동 인식
5. 각 환경변수 값 입력 (backend/.env 참고):
   - `ALLOWED_ORIGINS`: 배포 후 Vercel URL 넣기 (초기엔 `*` 임시)
   - `DATA_GO_KR_API_KEY`
   - `KAKAO_REST_API_KEY`
   - `JUSO_API_KEY`
   - `BLDG_REG_API_KEY`
   - `VWORLD_API_KEY`
6. **Apply** → 빌드 시작 (5~10 분)
7. 완료되면 URL 확인: `https://real-estate-price-api.onrender.com`
8. 동작 확인: `<URL>/api/v1/health` → `{"status":"ok"}`

### 3. Vercel 에서 프론트 배포
1. https://vercel.com 접속 (GitHub 로 로그인)
2. **Add New → Project** → GitHub repo 선택
3. **Root Directory** 를 `frontend` 로 지정
4. 환경변수 설정:
   - `NEXT_PUBLIC_API_BASE` = `https://real-estate-price-api.onrender.com` (2 단계에서 얻은 URL)
   - `NEXT_PUBLIC_KAKAO_MAP_KEY` = 카카오 JavaScript 키
5. **Deploy** (2~3 분)
6. 완료되면 URL 확인: `https://real-estate-price.vercel.app`

### 4. 마무리 설정
1. **Render 의 `ALLOWED_ORIGINS`** 를 Vercel URL 로 교체:
   ```
   https://real-estate-price.vercel.app,https://real-estate-price-*.vercel.app
   ```
   저장 → 자동 재배포
2. **카카오 콘솔** 의 JavaScript SDK 도메인에 Vercel URL 추가:
   ```
   https://real-estate-price.vercel.app
   ```

## 재배포 (코드 수정 후)
```bash
git add .
git commit -m "메시지"
git push
```
Vercel / Render 둘 다 자동 감지 → 재배포.

## Free 플랜 주의사항
- **Render Free**: 15 분 무응답 시 슬립 → 첫 요청 시 30 초 지연
- **Vercel**: 사실상 무제한 무료 (대역폭 월 100GB)
- SQLite 캐시는 Render 재배포 시 초기화됨 (실기능엔 영향 없음)

## 환경변수 참고
backend/.env 파일에서 값 복사 (절대 GitHub 에 커밋하지 말 것):
```
DATA_GO_KR_API_KEY=...
KAKAO_REST_API_KEY=...
JUSO_API_KEY=...
BLDG_REG_API_KEY=...
VWORLD_API_KEY=...
```
