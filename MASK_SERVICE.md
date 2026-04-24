# Mask Service 설정 가이드

## 개요

마스킹 지번 복원에 필요한 **4.5GB bldg.db** 를 클라우드에 올리지 않고,
**내 PC 에 그대로 두고** Render 백엔드가 호출할 수 있게 하는 구조.

```
[사용자] → [Vercel] → [Render] → (선택) [Cloudflare Tunnel] → [내 PC: mask-service]
                                                                 └── bldg.db (4.5GB)
```

- 내 PC 가 켜져있으면: 마스킹 완전 복원 ✅
- 내 PC 가 꺼져있으면: 마스킹된 상태로 응답 (사이트 자체는 정상) ⚠️

---

## 1. 내 PC 에서 mask-service 실행

### 최초 설정

1. 토큰 문자열 하나 정해두기 (아무거나, 20자 이상 랜덤 추천):
   ```
   예) abc123xyz789-myproject-secret-token
   ```
   이 토큰을 `run-mask-service.bat` 상단에 넣고, 나중에 Render 쪽에도 동일하게 설정.

2. `run-mask-service.bat` 편집:
   ```batch
   if "%MASK_SERVICE_TOKEN%"=="" set MASK_SERVICE_TOKEN=<여기에 내 토큰>
   ```

### 실행

그냥 `run-mask-service.bat` 더블클릭. 8100 포트에서 대기.

## 2. Cloudflare Tunnel 설정 (무료, 영구)

### 왜 Cloudflare Tunnel 인가?

- 무료, 세션 제한 없음 (ngrok 무료는 8시간마다 끊김)
- 고정 URL (재시작해도 주소 안 바뀜)
- HTTPS 자동 처리
- 내 PC 공유기/방화벽 건드릴 필요 없음

### 설치

1. https://one.dash.cloudflare.com 가입 (무료)
2. 왼쪽 메뉴 **Zero Trust** → **Networks** → **Tunnels**
3. **Create a tunnel** → Cloudflared 선택 → 이름 짓기 (예: `real-estate-mask`)
4. 화면의 **Install connector** 섹션에서 Windows 선택
5. 다운로드 받은 설치 명령어를 관리자 권한 PowerShell 에서 실행 (한 줄 통째로 붙여넣기)
   - 완료되면 Cloudflare 대시보드에 "Connected" 표시됨

### 라우팅 연결

6. **Next** → **Public Hostname** 탭
   - **Subdomain**: `mask` (원하는 이름)
   - **Domain**: 본인 도메인 선택 (도메인 없으면 Cloudflare 무료 도메인 구입 필요 — 연 $10 정도)
   - **Type**: `HTTP`
   - **URL**: `localhost:8100`
7. **Save tunnel**

→ 이제 `https://mask.<본인-도메인>.com` 주소로 내 PC 의 mask-service 에 접속 가능.

> **도메인 없는 대안**: Cloudflare Tunnel 은 도메인이 필수예요. 도메인이 없으면:
> - **Tailscale Funnel** (무료, 도메인 불필요, `ts.net` 서브도메인 제공) → 설정 더 간단
> - **ngrok 유료** ($10/월) → 고정 도메인 제공
> - **cloudflared quick tunnel** (무료, 임시 URL, 재시작마다 바뀜) → 가이드 아래 참고

## 2-b. 간이 옵션: Quick Tunnel (도메인 불필요, 단점: URL 변동)

도메인 없이 바로 써보려면:

1. `cloudflared` 만 다운로드:
   - https://github.com/cloudflare/cloudflared/releases/latest
   - `cloudflared-windows-amd64.exe` 받기 → 작업 폴더에 두기
2. 실행:
   ```powershell
   .\cloudflared.exe tunnel --url http://localhost:8100
   ```
3. 터미널에 `https://<랜덤>.trycloudflare.com` 주소가 뜸 → 그게 공개 URL
4. 재시작하면 URL 이 바뀌므로 그때마다 Render 환경변수를 업데이트해야 함

테스트 용도엔 좋지만 상시 운영은 부적합.

## 3. Render 에 환경변수 추가

Render 대시보드 → real-estate-price-api → Environment 에 **3개** 추가:

| 키 | 값 | 설명 |
|---|---|---|
| `MASK_SERVICE_URL` | `https://mask.본인도메인.com/enrich-masked` | 위에서 만든 공개 URL 에 `/enrich-masked` 붙임 |
| `MASK_SERVICE_TOKEN` | 아까 정한 토큰 문자열 | 내 PC 의 `run-mask-service.bat` 와 동일값 |
| `MASK_SERVICE_TIMEOUT` | `10` | 초 단위, 기본 10초 |

**Save Changes** → Render 자동 재배포 (1~2분).

## 4. 확인

1. 내 PC 에서 `run-mask-service.bat` 실행 중인지 확인
2. Render 재배포 완료 후 Vercel 사이트에서 조회 테스트:
   - `경기도 평택시 안중읍 덕우리` + 단독다가구 + 1년
   - 마스킹된 지번(`6**` 등)이 실제 번호로 복원되면 성공 ✅
3. 이제 `run-mask-service.bat` 를 끄고 다시 조회 → 마스킹된 채 응답되면 graceful degradation 정상 작동 ✅

---

## 트러블슈팅

### 조회 결과에 여전히 마스킹이 남아있음
- 내 PC 의 `run-mask-service.bat` 가 실행 중인지 확인
- Cloudflare Tunnel 연결 상태 확인 (대시보드 Tunnels 탭)
- Render 로그에서 `[mask_client] remote call failed` 메시지 확인
  - `401` → 토큰 불일치, 양쪽 값 재확인
  - `timeout` → `MASK_SERVICE_TIMEOUT` 값 올려보기 (예: 20)
  - `connection refused` → 내 PC 서비스 안 켜져있음

### 너무 느림
- Cloudflare Tunnel 경로가 길면 해외 경유해서 느릴 수 있음
- `MASK_SERVICE_TIMEOUT` 늘려 대응
- 자주 조회하는 지역은 mask-service 프로세스 내부 `_BLDG_CACHE` 에 들어가 이후 호출은 매우 빨라짐 (서비스를 껐다 켜면 캐시 사라짐)

### 내 PC 가 자주 꺼짐/재부팅됨
- `run-mask-service.bat` 를 Windows 시작 프로그램에 등록:
  1. `Win + R` → `shell:startup` → Enter
  2. 열린 폴더에 `run-mask-service.bat` 바로가기 복사
- 또는 Task Scheduler 로 로그온 시 자동 실행 등록

## 보안 주의사항

- `MASK_SERVICE_TOKEN` 은 GitHub 에 절대 커밋하지 말 것 (env.example 에만 빈 값으로 둠)
- 토큰이 유출되면 아무나 내 PC 의 mask-service 를 호출할 수 있지만, 노출되는 정보는 건축물대장(공공데이터) 이므로 피해는 제한적
- mask-service 는 **쓰기 기능이 없음** (읽기 전용 SQLite 쿼리). bldg.db 가 손상될 일은 없음
- Cloudflare Tunnel 은 공유기 포트 포워딩이 필요 없음 (아웃바운드 연결만 사용)
