"""부동산 가격 산정 서비스 - FastAPI 메인 앱"""
import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.routers.estimate import router as estimate_router
from app.routers.registry import router as registry_router

app = FastAPI(
    title="부동산 가격 산정 API",
    description=(
        "주소를 입력하면 국토교통부 실거래가 데이터를 기반으로 해당 지역의 "
        "과거 실거래 내역을 돌려준다.\n\n"
        "**주요 엔드포인트**\n"
        "- `POST /api/v1/estimate` : 주소 → 실거래 내역\n"
        "- `GET  /api/v1/health`   : 서버 헬스체크\n\n"
        "**지원 부동산 유형**: 아파트, 연립다세대, 단독다가구, 오피스텔, 토지, "
        "상업업무용, 분양권전매, 공장창고.\n\n"
        "**데이터 출처**: 국토교통부 실거래가 OpenAPI (공공데이터포털), "
        "건축물대장·토지특성 공공데이터(vworld)."
    ),
    version="0.1.0",
)

# CORS 설정
# - 개발(환경변수 미설정): 모든 origin 허용
# - 운영(ALLOWED_ORIGINS 설정): 콤마로 구분한 도메인만 허용
#   예) ALLOWED_ORIGINS="https://my-app.vercel.app,https://my-app-xyz.vercel.app"
_raw_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
if _raw_origins:
    _origins: list[str] = [o.strip() for o in _raw_origins.split(",") if o.strip()]
else:
    _origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API 라우터
app.include_router(estimate_router)
app.include_router(registry_router)

# 정적 파일(HTML/JS 프런트엔드) 서빙
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def root():
    """루트 접속 시 웹 화면 반환 (정적 파일이 있으면) 또는 API 정보"""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(
            str(index_path),
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    return {
        "service": "부동산 가격 산정 API",
        "version": "0.1.0",
        "docs": "/docs",
    }
