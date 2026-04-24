@echo off
chcp 65001 > nul
REM 부동산 가격 산정 서비스 실행 스크립트
REM 더블클릭 또는 cmd에서 run-server.bat 실행

echo ============================================================
echo  부동산 가격 산정 서비스 시작
echo ============================================================
echo.
echo  브라우저에서 아래 주소로 접속하세요:
echo  http://127.0.0.1:8000
echo.
echo  서버를 종료하려면 이 창에서 Ctrl+C 를 누르세요.
echo ============================================================
echo.

cd /d "%~dp0backend"
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000

pause
