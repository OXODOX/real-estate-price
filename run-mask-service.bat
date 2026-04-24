@echo off
REM ─── Mask Service 전용 실행기 (내 PC 에서만 실행) ───
REM Render 가 필요할 때 호출할 마스킹 복원 서비스. 8100 포트에서 대기.
REM MASK_SERVICE_TOKEN 은 Render 측 환경변수와 동일하게 맞춰야 함.

cd /d "%~dp0backend"

REM 토큰을 환경변수로 설정 (여기 값은 Render 쪽 MASK_SERVICE_TOKEN 과 동일해야 함).
REM 실서비스 시 이 값을 충분히 긴 랜덤 문자열로 바꿀 것.
if "%MASK_SERVICE_TOKEN%"=="" set MASK_SERVICE_TOKEN=change-me-to-random-string

echo.
echo ===========================================
echo  Mask Service (local only)
echo  Port: 8100
echo  Token: %MASK_SERVICE_TOKEN%
echo ===========================================
echo.

python -m uvicorn mask_service.main:app --host 0.0.0.0 --port 8100
