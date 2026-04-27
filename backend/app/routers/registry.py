"""건축물대장 / 토지대장 정보 조회 API.

특정 거래의 (시군구코드 + 동 + 지번) 을 받아 해당 위치의 건물·필지 정보를
SQLite (bldg.db) 에서 조회해 돌려준다. 프론트는 거래 행 클릭 시 팝업으로 표시.

배포 구조
--------
- 로컬 mask-service 가 bldg.db 를 보유 → 실제 조회는 mask-service /registry 가 수행
- Render 의 이 라우터는 MASK_SERVICE_URL 이 설정돼 있으면 그쪽으로 proxy
  (enrich-masked 와 동일 패턴)
- 둘 다 없으면 로컬 SQLite 기반으로 직접 조회 (개발 환경)
"""
from __future__ import annotations

import os
import re

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.bldg_registry import (
    _get_bjdong_cd,
    _fetch_all_buildings,
    _fetch_all_parcels,
)


router = APIRouter(prefix="/api/v1", tags=["registry"])

_MASK_BASE = (os.getenv("MASK_SERVICE_URL", "") or "").strip().rstrip("/")
# enrich-masked 같은 풀 URL 이 들어와 있을 수 있음 → 호스트만 추출
if _MASK_BASE.endswith("/enrich-masked"):
    _MASK_BASE = _MASK_BASE[: -len("/enrich-masked")]
_MASK_TOKEN = (os.getenv("MASK_SERVICE_TOKEN", "") or "").strip()
_MASK_TIMEOUT = float(os.getenv("MASK_SERVICE_TIMEOUT", "20"))


class RegistryRequest(BaseModel):
    sgg_cd: str = Field(..., description="시군구코드 5자리")
    dong: str = Field(..., description="법정동(읍/면/동/리) 명")
    jibun: str = Field("", description="지번 (예: '67-49' 또는 '산67-1')")


class ParcelInfo(BaseModel):
    bun: str
    ji: str
    sanji: str
    jimok_nm: str
    land_area: float | None
    land_use: str
    usage_nm: str
    price: float | None


class BuildingInfo(BaseModel):
    bun: str
    ji: str
    bld_nm: str
    main_purps_nm: str
    plat_area: float | None
    arch_area: float | None
    tot_area: float | None
    use_apr_day: str
    status: str
    demolish_day: str


class RegistryResponse(BaseModel):
    sgg_cd: str
    bjdong_cd: str | None
    dong: str
    jibun: str
    parcels: list[ParcelInfo]
    buildings: list[BuildingInfo]
    note: str = ""


def _split_jibun(jibun: str) -> tuple[str, str, str]:
    """지번 문자열 → (sanji, bun, ji). zero-pad 4자리.

    '67-49' → ('1','0067','0049'), '산67-1' → ('2','0067','0001'),
    '67' → ('1','0067','0000').
    """
    s = (jibun or "").strip()
    sanji = "1"
    if s.startswith("산"):
        sanji = "2"
        s = s[1:].strip()
    m = re.match(r"^(\d+)(?:-(\d+))?$", s)
    if not m:
        return sanji, "", ""
    bun = m.group(1).zfill(4)
    ji = (m.group(2) or "0").zfill(4)
    return sanji, bun, ji


async def _compute_registry_local(req: RegistryRequest) -> RegistryResponse:
    """bldg.db 가 있는 환경에서 실제 조회 수행.

    mask-service 가 직접 호출하고, Render 도 proxy 가 안 잡혔을 때 폴백으로 사용.
    """
    if not req.jibun.strip():
        return RegistryResponse(
            sgg_cd=req.sgg_cd, bjdong_cd=None, dong=req.dong, jibun="",
            parcels=[], buildings=[], note="지번이 없어 조회 불가",
        )
    if "*" in req.jibun:
        return RegistryResponse(
            sgg_cd=req.sgg_cd, bjdong_cd=None, dong=req.dong, jibun=req.jibun,
            parcels=[], buildings=[], note="마스킹된 지번으로 조회 불가",
        )

    sanji, bun, ji = _split_jibun(req.jibun)
    if not bun:
        return RegistryResponse(
            sgg_cd=req.sgg_cd, bjdong_cd=None, dong=req.dong, jibun=req.jibun,
            parcels=[], buildings=[], note="지번 형식 인식 실패",
        )

    bjdong = await _get_bjdong_cd(req.sgg_cd, req.dong)
    if not bjdong:
        return RegistryResponse(
            sgg_cd=req.sgg_cd, bjdong_cd=None, dong=req.dong, jibun=req.jibun,
            parcels=[], buildings=[], note=f"법정동 코드 조회 실패: {req.dong}",
        )

    # 건축물·필지 일괄 조회 후 본번/부번/산여부 일치 항목만 추출
    parcels = await _fetch_all_parcels(req.sgg_cd, bjdong)
    buildings = await _fetch_all_buildings(req.sgg_cd, bjdong)

    def _matches(item: dict) -> bool:
        b = str(item.get("bun") or "").zfill(4)
        if b != bun:
            return False
        j = str(item.get("ji") or "").zfill(4)
        if ji != "0000" and j != ji:
            return False
        # parcels 만 sanji 필드 있음. buildings 는 모두 일반 토지로 가정.
        s = item.get("sanji")
        if s is not None and s != sanji:
            return False
        return True

    parcel_hits = [p for p in parcels if _matches(p)]
    building_hits = [b for b in buildings if _matches(b)]

    return RegistryResponse(
        sgg_cd=req.sgg_cd,
        bjdong_cd=bjdong,
        dong=req.dong,
        jibun=req.jibun,
        parcels=[
            ParcelInfo(
                bun=str(p.get("bun") or "").lstrip("0") or "0",
                ji=str(p.get("ji") or "").lstrip("0"),
                sanji=p.get("sanji") or "1",
                jimok_nm=p.get("jimokNm") or "",
                land_area=p.get("landArea"),
                land_use=p.get("landUse") or "",
                usage_nm=p.get("usageNm") or "",
                price=p.get("price"),
            )
            for p in parcel_hits
        ],
        buildings=[
            BuildingInfo(
                bun=str(b.get("bun") or "").lstrip("0") or "0",
                ji=str(b.get("ji") or "").lstrip("0"),
                bld_nm=b.get("bldNm") or "",
                main_purps_nm=b.get("mainPurpsCdNm") or "",
                plat_area=b.get("platArea"),
                arch_area=b.get("archArea"),
                tot_area=b.get("totArea"),
                use_apr_day=b.get("useAprDay") or "",
                status=b.get("status") or "active",
                demolish_day=b.get("demolishDay") or "",
            )
            for b in building_hits
        ],
    )


async def _proxy_to_mask_service(req: RegistryRequest) -> RegistryResponse | None:
    """mask-service 의 /registry 엔드포인트로 프록시. 실패 시 None."""
    if not _MASK_BASE:
        return None
    headers = {"Content-Type": "application/json"}
    if _MASK_TOKEN:
        headers["X-Mask-Token"] = _MASK_TOKEN
    try:
        async with httpx.AsyncClient(timeout=_MASK_TIMEOUT) as client:
            resp = await client.post(
                f"{_MASK_BASE}/registry",
                headers=headers,
                json=req.model_dump(),
            )
            if resp.status_code != 200:
                return None
            return RegistryResponse(**resp.json())
    except Exception:
        return None


@router.post(
    "/registry",
    response_model=RegistryResponse,
    summary="건축물대장 / 토지대장 정보 조회",
    description=(
        "거래의 (시군구코드 + 동 + 지번) 을 받아 해당 위치의 건축물대장 + "
        "토지대장 정보를 돌려준다. 기본 흐름은 mask-service /registry 로 proxy.\n\n"
        "- 지번이 비면 동 단위 정보 미반환\n"
        "- 마스킹된 지번(`6**`) 도 빈 결과\n"
        "- 본번만 정해진 경우(`67-*`) 본번 일치 후보 모두 반환"
    ),
)
async def get_registry(req: RegistryRequest) -> RegistryResponse:
    """프록시 우선, 폴백으로 로컬 조회."""
    proxied = await _proxy_to_mask_service(req)
    if proxied is not None:
        return proxied
    # mask-service 가 없거나 실패한 경우 로컬 폴백 (개발 환경 / 디버그용).
    return await _compute_registry_local(req)
