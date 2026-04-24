"""실거래가 조회 API 라우터"""
from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    PropertyType,
    TransactionType,
    PriceRequest,
    TransactionResult,
)
from app.services.address_lookup import (
    find_lawd_code,
    find_lawd_codes,
    extract_dong,
    extract_jibun,
    extract_building_name,
)
from app.services.juso_api import (
    is_road_address,
    convert_road_to_jibun,
    enrich_road_addresses,
)
from app.services.molit_api import fetch_transactions
from app.services.price_estimator import group_transactions
from app.services.mask_client import enrich_masked_jibun_any as enrich_masked_jibun

router = APIRouter(prefix="/api/v1", tags=["transactions"])

# step 7(표시 거래 전체 마스킹 복원)의 비용이 nearby 크기에 비례(법정동 수 × 거래 수)
# 하므로, 사용자에게 실제로 보여줄 수 있는 상한선까지만 자르고 enrich 한다.
# 날짜 내림차순으로 이미 정렬돼 있어 상위 N개가 가장 최근 거래.
NEARBY_DISPLAY_LIMIT = 200


@router.post(
    "/estimate",
    response_model=TransactionResult,
    summary="주소로 실거래 내역 조회",
    description=(
        "주소를 입력받아 해당 위치의 과거 실거래 내역을 돌려준다.\n\n"
        "처리 흐름:\n"
        "1. 도로명이면 지번으로 변환 (JUSO API)\n"
        "2. 시/군/구 법정동코드 추출 → 부천·화성처럼 서브코드가 있으면 자동 fan-out\n"
        "3. MOLIT 실거래가 API 로 해당 기간 전체 거래 조회 (프로세스+디스크 캐시)\n"
        "4. 건축물대장/토지대장으로 마스킹된 지번(예: '9**') 복원 시도\n"
        "5. 주요(`recent_transactions`) + 인근(`nearby_transactions`) 두 그룹으로 분류\n\n"
        "정확 매칭이 없으면 단계별 폴백(동/본번/면적/단지명/지목) 후 해당 "
        "`*_fallback` 플래그를 True 로 돌려준다."
    ),
    responses={
        400: {"description": "주소에서 시/군/구를 식별하지 못함."},
        404: {"description": "해당 지역에 해당 부동산 유형 거래 데이터 없음."},
    },
)
async def get_transactions(req: PriceRequest):
    """주소를 입력받아 실거래 내역을 반환합니다."""

    working_address = req.address
    target_dong = None
    target_jibun = None

    # 1. 도로명 주소 → 지번 주소 변환
    if is_road_address(req.address):
        juso = await convert_road_to_jibun(req.address)
        if juso:
            target_dong = juso["dong"]
            target_jibun = juso["jibun"]
            if juso["full_jibun_address"]:
                working_address = juso["full_jibun_address"]

    # 2. 법정동코드 조회
    # 부천·화성처럼 MOLIT 가 대표코드 대신 여러 서브코드로만 반환하는 경우를
    # 지원하기 위해 복수 코드를 받는다. 대표코드 로깅 및 에러메시지에는
    # 첫 코드를 사용.
    lawd_codes = find_lawd_codes(working_address)
    lawd_cd = lawd_codes[0] if lawd_codes else None
    if not lawd_cd:
        raise HTTPException(
            status_code=400,
            detail={
                "type": "address_not_found",
                "message": f"주소에서 지역을 찾을 수 없습니다: '{req.address}'",
                "suggestion": "예시: '서울 강남구 역삼동', '강남구 테헤란로 152'",
            },
        )

    # 3. 동·지번·단지명 추출
    if not target_dong:
        target_dong = extract_dong(working_address)
    if not target_jibun:
        target_jibun = extract_jibun(req.address)
    target_building = req.building_name or extract_building_name(req.address)

    prop_type = req.property_type or PropertyType.APT

    # 4. 실거래 데이터 조회 (fan-out 시 여러 코드 병렬 조회 후 병합)
    import asyncio as _asyncio

    months_back_eff = 0 if req.months_back == 0 else max(1, min(req.months_back, 60))
    fetch_results = await _asyncio.gather(
        *[
            fetch_transactions(
                lawd_cd=code,
                property_type=prop_type,
                transaction_type=TransactionType.TRADE,
                months_back=months_back_eff,
            )
            for code in lawd_codes
        ]
    )
    transactions = []
    for batch in fetch_results:
        transactions.extend(batch)
    # 날짜 내림차순 재정렬 (fan-out 병합 시 순서 섞임)
    transactions.sort(
        key=lambda t: (t.deal_year, t.deal_month, t.deal_day), reverse=True
    )

    if not transactions:
        raise HTTPException(
            status_code=404,
            detail={
                "type": "no_data",
                "message": f"'{req.address}' 지역의 {prop_type.value} 매매 거래 데이터가 없습니다.",
                "suggestion": None,
            },
        )

    # 5. 마스킹 지번 복원 (그룹핑 전에 먼저 수행)
    # 건축물대장과 정확일치하는 경우만 복원되므로 primary 오탐 위험 없음.
    # 사용자가 구체적 지번을 입력했을 때만 수행 (동 단위 조회엔 불필요 + 속도).
    if target_jibun:
        await enrich_masked_jibun(transactions, target_dong=target_dong)
        for t in transactions:
            if t.address_estimated and t.estimated_jibun:
                t.jibun = t.estimated_jibun

    # 6. 거래 그룹 분류 (복원된 지번 반영 후 분류 → 타겟 거래가 primary로 올라옴)
    result = group_transactions(
        transactions=transactions,
        target_dong=target_dong,
        target_building=target_building,
        target_jibun=target_jibun,
        target_address=req.address,
        target_area_m2=req.area_m2,
        target_jimok=req.jimok if prop_type == PropertyType.LAND else None,
    )

    if result:
        # nearby는 표시 상한까지만 남긴다 (step 7 enrich 비용 제한).
        if len(result.nearby_transactions) > NEARBY_DISPLAY_LIMIT:
            result.nearby_transactions = result.nearby_transactions[:NEARBY_DISPLAY_LIMIT]

        # 7. 출력되는 모든 거래(primary + nearby)에 대해 마스킹 복원 시도.
        #    target_dong 필터 없이 실제 표시 대상만 대상으로 하므로 범위 제한적.
        #    _BJDONG_CACHE / _BLDG_CACHE 덕에 동일 동 반복 조회는 즉시 반환.
        displayed = list(result.recent_transactions) + list(result.nearby_transactions)
        await enrich_masked_jibun(displayed)
        for t in displayed:
            if t.address_estimated and t.estimated_jibun:
                t.jibun = t.estimated_jibun

        # 도로명 reverse lookup은 기본 표시되는 주요 거래(primary)만.
        await enrich_road_addresses(result.recent_transactions)

    return result


@router.get(
    "/health",
    summary="서버 헬스체크",
    description="서버가 살아있는지 확인하는 단순 핑. `{\"status\":\"ok\"}` 반환.",
)
async def health_check():
    return {"status": "ok"}
