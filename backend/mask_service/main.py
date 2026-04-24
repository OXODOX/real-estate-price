"""Mask Service - 로컬 PC 전용 마스킹 지번 복원 서비스

구조:
  [Render 백엔드] --HTTP--> [Cloudflare Tunnel] --> [내 PC: 이 서비스]
                                                    └── bldg.db (4.5GB)

Render 쪽에서 마스킹된 거래 배치를 보내면, 여기서 건축물대장/토지대장
데이터(SQLite)로 매칭해 추정 지번을 돌려준다.

실행:
  cd backend
  uvicorn mask_service.main:app --host 0.0.0.0 --port 8100

인증:
  MASK_SERVICE_TOKEN 환경변수로 설정한 토큰을 요청 헤더
  `X-Mask-Token` 에 실어야 한다 (외부 악용 방지).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# backend 디렉토리를 path에 추가 (app.* import 가능하게)
_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.models.schemas import Transaction
from app.services.bldg_registry import enrich_masked_jibun

# ─── 설정 ───
_EXPECTED_TOKEN = os.getenv("MASK_SERVICE_TOKEN", "").strip()


# ─── 요청/응답 스키마 ───

class EnrichRequest(BaseModel):
    transactions: list[Transaction] = Field(
        description="마스킹 복원을 시도할 거래 목록 (Transaction 스키마 그대로)."
    )
    target_dong: str | None = Field(
        default=None,
        description="동 단위 필터. 주어지면 해당 동 거래만 처리.",
    )


class EnrichResultItem(BaseModel):
    index: int = Field(description="요청 transactions 배열 내 원본 인덱스.")
    estimated_jibun: str = Field(description="추정된 실제 지번 (예: '698-1').")
    address_estimated: bool = Field(description="복원 성공 여부.")
    address_estimated_certain: bool = Field(
        description="유일하게 결정된 경우 True (신뢰도 높음)."
    )


class EnrichResponse(BaseModel):
    results: list[EnrichResultItem] = Field(
        description="마스킹 복원에 성공한 거래만 담김. 실패/마스킹 없음은 제외."
    )
    processed: int = Field(description="복원 시도한 거래 수 (마스킹된 것만).")
    recovered: int = Field(description="추정이 성공한 거래 수.")


# ─── FastAPI 앱 ───

app = FastAPI(
    title="Mask Service (local-only)",
    description="건축물대장·토지대장 기반 마스킹 지번 복원 서비스 (내 PC 전용)",
    version="1.0.0",
)

# Render 에서 호출하므로 CORS 필요 없음 (서버 간 통신). 하지만 혹시 몰라 허용.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    """헬스체크. Cloudflare Tunnel 이 살아있는지 확인용."""
    return {"status": "ok", "service": "mask-service"}


@app.post("/enrich-masked", response_model=EnrichResponse)
async def enrich_masked(
    req: EnrichRequest,
    x_mask_token: str = Header(default=""),
):
    """배치로 마스킹 지번 복원.

    1) 요청 거래들을 내부 `enrich_masked_jibun` 에 그대로 넘긴다
       (함수가 Transaction 객체를 in-place로 수정).
    2) 복원에 성공한 거래만 골라서 결과 배열로 반환.
    """
    # 토큰 검증 (설정되어 있는 경우에만)
    if _EXPECTED_TOKEN:
        if x_mask_token != _EXPECTED_TOKEN:
            raise HTTPException(status_code=401, detail="invalid token")

    txs = req.transactions
    if not txs:
        return EnrichResponse(results=[], processed=0, recovered=0)

    # 마스킹된 거래 수 (참고용 통계)
    processed = sum(1 for t in txs if "*" in (t.jibun or ""))

    # 내부 함수 호출 (Transaction을 in-place 수정)
    await enrich_masked_jibun(txs, target_dong=req.target_dong)

    # 복원 성공한 것만 뽑기
    results: list[EnrichResultItem] = []
    for i, t in enumerate(txs):
        if t.address_estimated and t.estimated_jibun:
            results.append(
                EnrichResultItem(
                    index=i,
                    estimated_jibun=t.estimated_jibun,
                    address_estimated=True,
                    address_estimated_certain=t.address_estimated_certain,
                )
            )

    return EnrichResponse(
        results=results,
        processed=processed,
        recovered=len(results),
    )
