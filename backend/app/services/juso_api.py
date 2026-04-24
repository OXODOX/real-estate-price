"""행정안전부 도로명주소 검색 API

도로명 주소를 입력받아 법정동명 + 지번 정보를 반환합니다.
API 문서: https://business.juso.go.kr
"""
import asyncio
import httpx
from app.config import get_settings

JUSO_URL = "https://business.juso.go.kr/addrlink/addrLinkApi.do"

_ROAD_CACHE: dict[str, str] = {}
_ROAD_CACHE_MAX = 5000
_JUSO_SEM = asyncio.Semaphore(20)

# 도로명 주소 판단용 접미사
_ROAD_SUFFIXES = ("로", "길", "대로", "번길")


def is_road_address(address: str) -> bool:
    """도로명 주소 여부 판단 (로/길/대로 포함 여부)."""
    tokens = address.split()
    for token in tokens:
        if any(token.endswith(s) for s in _ROAD_SUFFIXES) and len(token) >= 3:
            return True
    return False


async def convert_road_to_jibun(address: str) -> dict | None:
    """도로명 주소 → 법정동명 + 지번 변환.

    Returns:
        {
            "dong": "역삼동",
            "jibun": "679-13",
            "road_address": "테헤란로 152",
            "full_jibun_address": "서울특별시 강남구 역삼동 679-13",
        }
        또는 None (변환 실패 시)
    """
    settings = get_settings()
    if not settings.JUSO_API_KEY:
        return None

    params = {
        "confmKey": settings.JUSO_API_KEY,
        "currentPage": "1",
        "countPerPage": "5",
        "keyword": address,
        "resultType": "json",
    }

    try:
        async with httpx.AsyncClient(timeout=2.5) as client:
            resp = await client.get(JUSO_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

        results = data.get("results", {})
        juso_list = results.get("juso", [])

        if not juso_list:
            return None

        # 첫 번째 결과 사용
        item = juso_list[0]

        # 법정동명 추출 (emdNm: 읍면동, lnbrMnnm: 지번 본번, lnbrSlno: 지번 부번)
        dong = item.get("emdNm", "")  # 예: "역삼동"
        lnbr_main = item.get("lnbrMnnm", "").strip()
        lnbr_sub = item.get("lnbrSlno", "").strip()

        if lnbr_main:
            jibun = lnbr_main if not lnbr_sub or lnbr_sub == "0" else f"{lnbr_main}-{lnbr_sub}"
        else:
            jibun = None

        return {
            "dong": dong or None,
            "jibun": jibun,
            "road_address": item.get("roadAddr", address),
            "full_jibun_address": item.get("jibunAddr", ""),
            "sgg_nm": item.get("siNm", "") + " " + item.get("sggNm", ""),
        }

    except Exception:
        return None


async def _lookup_one(client: httpx.AsyncClient, jibun_address: str) -> str:
    """단일 JUSO 조회. 캐시 쓰기/읽기 포함. 공유 client 사용."""
    if not jibun_address:
        return ""
    if jibun_address in _ROAD_CACHE:
        return _ROAD_CACHE[jibun_address]

    settings = get_settings()
    if not settings.JUSO_API_KEY:
        return ""

    params = {
        "confmKey": settings.JUSO_API_KEY,
        "currentPage": "1",
        "countPerPage": "1",
        "keyword": jibun_address,
        "resultType": "json",
    }

    try:
        async with _JUSO_SEM:
            resp = await client.get(JUSO_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        juso_list = data.get("results", {}).get("juso", [])
        road = juso_list[0].get("roadAddr", "") if juso_list else ""
    except Exception:
        road = ""

    if len(_ROAD_CACHE) >= _ROAD_CACHE_MAX:
        _ROAD_CACHE.pop(next(iter(_ROAD_CACHE)))
    _ROAD_CACHE[jibun_address] = road
    return road


async def jibun_to_road(jibun_address: str) -> str:
    """단일 조회용 (하위 호환). 매번 클라이언트를 새로 만들어 비효율적이므로
    대량 조회에는 enrich_road_addresses를 사용."""
    async with httpx.AsyncClient(timeout=2.5) as client:
        return await _lookup_one(client, jibun_address)


async def enrich_road_addresses(transactions: list, limit: int | None = 40) -> None:
    """Transaction 리스트에 road_address 채우기 (고유 주소만 조회).

    limit: 고유 주소 개수 상한. 대량 거래 시 JUSO API 호출 폭증을 막기 위함.
           캐시된 주소는 상한에 관계없이 즉시 채움.
    """
    unique: dict[str, list] = {}
    for t in transactions:
        key = t.full_address.strip()
        if not key:
            continue
        # 마스킹된 지번(예: '1**')은 정확한 도로명 매칭 불가 → 스킵
        if "*" in key:
            continue
        unique.setdefault(key, []).append(t)

    if not unique:
        return

    # 1) 캐시 히트는 즉시 적용
    keys_to_fetch: list[str] = []
    for key, txs in unique.items():
        if key in _ROAD_CACHE:
            road = _ROAD_CACHE[key]
            for t in txs:
                t.road_address = road
        else:
            keys_to_fetch.append(key)

    # 2) 네트워크 조회는 상한 내에서만 수행
    if limit is not None and len(keys_to_fetch) > limit:
        keys_to_fetch = keys_to_fetch[:limit]

    if not keys_to_fetch:
        return

    # 공유 AsyncClient로 HTTP keep-alive 활용 → TLS/DNS 반복 비용 제거
    async with httpx.AsyncClient(timeout=2.5) as client:
        results = await asyncio.gather(
            *(_lookup_one(client, k) for k in keys_to_fetch)
        )
    for key, road in zip(keys_to_fetch, results):
        for t in unique[key]:
            t.road_address = road
