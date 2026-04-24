"""국토교통부 실거래가 API 연동 모듈

부동산 유형별로 다른 API 엔드포인트를 호출하여 실거래 데이터를 조회합니다.
각 API는 공공데이터포털(data.go.kr)에서 개별 신청이 필요합니다.

유형별 응답 필드 차이:
┌─────────────┬──────────────┬──────────────────���──────────┐
│ 유형        │ 이름 필드     │ 면적 필드         │ 엔드포인트 │
├─────────────┼──────────────┼──────────────────┼──────────┤
│ 아파트매매   │ aptNm        │ excluUseAr       │ AptTrade │
│ 연립다세대  │ mhouseNm     │ excluUseAr       │ RHTrade  │
│ 오피스텔    │ offiNm       │ excluUseAr       │ OffiTrade│
│ 단독다가구  │ (없음)        │ totalFloorAr     │ SHTrade  │
│ 토지        │ (없음)        │ dealArea         │ LandTrade│
│ 상업업무용  │ (없음)        │ buildingAr       │ NrgTrade │
│ 분양권전매  │ aptNm        │ excluUseAr       │ SilvTrade│
│ 공장창고    │ (없음)        │ buildingAr       │ InduTrade│
└─────────────┴──────────────┴──────────────────┴──────────┘
"""
import asyncio
import httpx
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, date
from pathlib import Path
from urllib.parse import unquote

from app.config import get_settings
from app.models.schemas import PropertyType, TransactionType, Transaction
from app.services.address_lookup import LAWD_CODE_MAP

# 시군구코드 → 시군구명 역매핑 (sggNm이 없는 API에서 사용)
_SGG_CODE_TO_NAME: dict[str, str] = {}
for _key, _code in LAWD_CODE_MAP.items():
    if _code not in _SGG_CODE_TO_NAME:
        # "서울 강남구" → "강남구", "경기 용인 수지구" → "용인시 수지구"
        parts = _key.split()
        if len(parts) >= 2:
            _SGG_CODE_TO_NAME[_code] = " ".join(parts[1:])
        else:
            _SGG_CODE_TO_NAME[_code] = _key


# 부동산 유형 + 거래유형별 API 엔드포인트 및 응답 필드 매핑
API_ENDPOINTS: dict[tuple[PropertyType, TransactionType], dict] = {
    # === 매매 ===
    (PropertyType.APT, TransactionType.TRADE): {
        "path": "/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
        "name_field": "aptNm",
        "area_field": "excluUseAr",
        "area_type": "전용면적",
    },
    (PropertyType.VILLA, TransactionType.TRADE): {
        "path": "/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
        "name_field": "mhouseNm",
        "area_field": "excluUseAr",
        "area_type": "전용면적",
    },
    (PropertyType.OFFICETEL, TransactionType.TRADE): {
        "path": "/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
        "name_field": "offiNm",
        "area_field": "excluUseAr",
        "area_type": "전용면적",
    },
    (PropertyType.HOUSE, TransactionType.TRADE): {
        "path": "/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
        "name_field": None,
        "area_field": "totalFloorAr",
        "area_type": "연면적",
    },
    (PropertyType.LAND, TransactionType.TRADE): {
        "path": "/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade",
        "name_field": None,
        "area_field": "dealArea",
        "area_type": "거래면적",
    },
    (PropertyType.COMMERCIAL, TransactionType.TRADE): {
        "path": "/RTMSDataSvcNrgTrade/getRTMSDataSvcNrgTrade",
        "name_field": None,
        "area_field": "buildingAr",
        "area_type": "건물면적",
    },
    (PropertyType.SILV, TransactionType.TRADE): {
        "path": "/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade",
        "name_field": "aptNm",
        "area_field": "excluUseAr",
        "area_type": "전용면적",
    },
    (PropertyType.INDU, TransactionType.TRADE): {
        "path": "/RTMSDataSvcInduTrade/getRTMSDataSvcInduTrade",
        "name_field": None,
        "area_field": "buildingAr",
        "area_type": "건물면적",
    },
    # === 전월세 (추후 신청 시 활성화) ===
    (PropertyType.APT, TransactionType.RENT): {
        "path": "/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
        "name_field": "aptNm",
        "area_field": "excluUseAr",
        "area_type": "전용면적",
    },
    (PropertyType.VILLA, TransactionType.RENT): {
        "path": "/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
        "name_field": "mhouseNm",
        "area_field": "excluUseAr",
        "area_type": "전용면적",
    },
    (PropertyType.HOUSE, TransactionType.RENT): {
        "path": "/RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
        "name_field": None,
        "area_field": "totalFloorAr",
        "area_type": "연면적",
    },
    (PropertyType.OFFICETEL, TransactionType.RENT): {
        "path": "/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
        "name_field": "offiNm",
        "area_field": "excluUseAr",
        "area_type": "전용면적",
    },
}


def _parse_int(text: str | None) -> int | None:
    """문자열을 정수로 변환. 콤마, 공백 제거."""
    if not text:
        return None
    try:
        return int(text.strip().replace(",", ""))
    except ValueError:
        return None


def _parse_float(text: str | None) -> float | None:
    """문자열을 실수로 변환."""
    if not text:
        return None
    try:
        return float(text.strip().replace(",", ""))
    except ValueError:
        return None


def _text(item: ET.Element, tag: str) -> str:
    """XML 요소에서 텍스트 추출 (공백 정리)."""
    val = item.findtext(tag)
    return val.strip() if val else ""


def _parse_transactions(
    items: list[ET.Element],
    property_type: PropertyType,
    transaction_type: TransactionType,
    name_field: str | None,
    area_field: str,
    area_type: str,
) -> list[Transaction]:
    """XML item 요소들을 Transaction 리스트로 변환.

    유형별로 이름/면적 필드가 다르므로 name_field, area_field를 동적으로 사용.
    모든 부가 정보(주소, 면적 상세, 지목, 거래유형, 해제여부 등)도 함께 파싱.
    """
    transactions = []
    for item in items:
        price = _parse_int(item.findtext("dealAmount"))
        if price is None:
            continue

        # 단지명/건물명
        name = ""
        if name_field:
            name = _text(item, name_field)

        # 주 면적
        area = _parse_float(item.findtext(area_field)) or 0.0

        # 주소 정보
        sgg_cd = _text(item, "sggCd")
        sgg_nm = _text(item, "sggNm")
        # sggNm이 없는 API(아파트 등)는 sggCd로 변환
        if not sgg_nm and sgg_cd:
            sgg_nm = _SGG_CODE_TO_NAME.get(sgg_cd, "")
        dong = _text(item, "umdNm")
        jibun = _text(item, "jibun")

        # 면적 상세 (유형별로 존재하는 필드만 파싱)
        exclu_use_ar = _parse_float(item.findtext("excluUseAr"))
        land_ar = _parse_float(item.findtext("landAr"))
        building_ar = _parse_float(item.findtext("buildingAr"))
        plottage_ar = _parse_float(item.findtext("plottageAr"))
        deal_area = _parse_float(item.findtext("dealArea"))
        total_floor_ar = _parse_float(item.findtext("totalFloorAr"))

        # 토지/건물 정보
        jimok = _text(item, "jimok")
        land_use = _text(item, "landUse")
        house_type = _text(item, "houseType")
        building_type = _text(item, "buildingType")
        building_use = _text(item, "buildingUse")
        share_dealing_type = _text(item, "shareDealingType")

        # 거래 정보
        dealing_gbn = _text(item, "dealingGbn")
        buyer_gbn = _text(item, "buyerGbn")
        sler_gbn = _text(item, "slerGbn")

        # 해제 정보
        cdeal_day = _text(item, "cdealDay")
        cdeal_type = _text(item, "cdealType")

        transactions.append(
            Transaction(
                property_type=property_type,
                transaction_type=transaction_type,
                name=name,
                # 주소
                sgg_cd=sgg_cd,
                sgg_nm=sgg_nm,
                dong=dong,
                jibun=jibun,
                # 거래
                price_man_won=price,
                deal_year=int(item.findtext("dealYear") or 0),
                deal_month=int(item.findtext("dealMonth") or 0),
                deal_day=_parse_int(item.findtext("dealDay")) or 0,
                floor=_parse_int(item.findtext("floor")),
                build_year=_parse_int(item.findtext("buildYear")),
                dealing_gbn=dealing_gbn,
                buyer_gbn=buyer_gbn,
                sler_gbn=sler_gbn,
                # 해제
                cdeal_day=cdeal_day,
                cdeal_type=cdeal_type,
                # 면적
                area_m2=area,
                area_type=area_type,
                exclu_use_ar=exclu_use_ar,
                land_ar=land_ar,
                building_ar=building_ar,
                plottage_ar=plottage_ar,
                deal_area=deal_area,
                total_floor_ar=total_floor_ar,
                # 토지/건물
                jimok=jimok,
                land_use=land_use,
                house_type=house_type,
                building_type=building_type,
                building_use=building_use,
                share_dealing_type=share_dealing_type,
            )
        )
    return transactions


# MOLIT 실거래가 데이터 시작 연월 (아파트 기준 2006년 1월)
_DATA_START = date(2006, 1, 1)

# 병렬 요청 동시 제한 (공공API 과부하 방지)
# 부천·화성 fan-out(3~4 코드)으로 실질 요청 수가 곱절이 되므로 보수적으로 설정.
_CONCURRENCY = 6

# MOLIT 월별 응답 캐시: (path, lawd_cd, deal_ymd) → parsed transactions
# 같은 (지역·유형·월) 조합은 어차피 immutable(과거 월) 이거나 느리게 변해
# 프로세스 수명 동안 캐시해도 안전. 재시도 실패 후에도 이전 성공 결과를 재사용해
# "검색할 때마다 거래 수가 요동"치는 문제를 막는다.
#
# 영속(persist): 아래 _CACHE_DB_PATH SQLite 파일에도 동시에 기록하므로
# 서버 재시작 후 첫 요청에서도 즉시 워밍업됨 (_load_cache_from_disk 가 기동 시 호출).
_MOLIT_CACHE: dict[tuple[str, str, str], list[Transaction]] = {}

# 캐시 영속화용 SQLite 파일 (건축물대장 DB 와 분리)
_CACHE_DB_PATH = Path(__file__).resolve().parent.parent.parent / ".cache" / "molit_cache.db"
_CACHE_LOADED = False
_CACHE_WRITE_LOCK = asyncio.Lock()


def _get_cache_conn() -> sqlite3.Connection:
    """캐시 DB 연결을 반환. 파일이 없으면 스키마 생성."""
    _CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_CACHE_DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS molit_cache (
            path     TEXT NOT NULL,
            lawd_cd  TEXT NOT NULL,
            deal_ymd TEXT NOT NULL,
            data     TEXT NOT NULL,     -- Transaction 리스트를 JSON 으로 직렬화
            updated_at INTEGER NOT NULL, -- epoch seconds
            PRIMARY KEY (path, lawd_cd, deal_ymd)
        )
        """
    )
    return conn


def _load_cache_from_disk() -> None:
    """SQLite 에 저장된 캐시를 메모리 dict 로 일괄 로드.
    서버 기동 시 한 번 호출. 예외는 삼킨다(캐시 누락은 치명적이지 않음)."""
    global _CACHE_LOADED
    if _CACHE_LOADED:
        return
    _CACHE_LOADED = True  # 실패해도 재진입 방지 (무한 재시도 X)
    try:
        conn = _get_cache_conn()
        rows = conn.execute(
            "SELECT path, lawd_cd, deal_ymd, data FROM molit_cache"
        ).fetchall()
        conn.close()
        loaded = 0
        for path, lawd_cd, deal_ymd, data in rows:
            try:
                # data 는 Transaction JSON 배열 문자열.
                # Pydantic v2: model_validate_json(each element) 로 복원.
                import json as _json

                arr = _json.loads(data)
                txs = [Transaction.model_validate(x) for x in arr]
                _MOLIT_CACHE[(path, lawd_cd, deal_ymd)] = txs
                loaded += 1
            except Exception:
                continue
        if loaded:
            print(f"[molit_api] cache preloaded: {loaded} entries from {_CACHE_DB_PATH}")
    except Exception as e:  # noqa: BLE001
        print(f"[molit_api] cache preload failed (ignored): {e}")


async def _persist_cache_entry(
    path: str, lawd_cd: str, deal_ymd: str, txs: list[Transaction]
) -> None:
    """한 건의 (path,lawd,ymd) 캐시를 DB 에 upsert.
    SQLite write 는 동기·블로킹이므로 to_thread 로 오프로드."""
    import json as _json
    import time as _time

    payload = _json.dumps(
        [t.model_dump(mode="json") for t in txs],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    now_ts = int(_time.time())

    def _write() -> None:
        conn = _get_cache_conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO molit_cache "
                "(path, lawd_cd, deal_ymd, data, updated_at) VALUES (?,?,?,?,?)",
                (path, lawd_cd, deal_ymd, payload, now_ts),
            )
            conn.commit()
        finally:
            conn.close()

    # 동시 write 로부터 보호
    async with _CACHE_WRITE_LOCK:
        try:
            await asyncio.to_thread(_write)
        except Exception as e:  # noqa: BLE001
            # 쓰기 실패해도 메모리 캐시는 유지됨(다음 요청도 정상 동작)
            print(f"[molit_api] cache write failed (ignored): {e}")

# 현재 월은 신규 거래가 쌓이므로 캐시에서 제외 (항상 fresh fetch)
def _current_ymd() -> str:
    now = datetime.now()
    return f"{now.year:04d}{now.month:02d}"


# 엔드포인트별 "쿼터 소진" 플래그: path → 재개 가능 시점(epoch 초).
# MOLIT 가 "API token quota exceeded" 본문과 함께 429 를 반환하면, 이 엔드포인트의
# 모든 후속 호출을 해당 시간까지 즉시 단락시켜(요청당 수십초 재시도 낭비 방지).
# 쿼터는 하루 단위로 리셋되므로 1시간 단위로 재시도 허용.
import time as _time  # 지역 import(기존 상단 import 영역에 영향 최소화)
_QUOTA_BLOCKED: dict[str, float] = {}
_QUOTA_BLOCK_SECONDS = 3600.0  # 1h 후 다시 시도


def _months_to_fetch(months_back: int) -> list[str]:
    """조회할 연월 목록 생성. months_back=0이면 2006년부터 전체."""
    now = datetime.now()
    if months_back == 0:
        total = (now.year - _DATA_START.year) * 12 + (now.month - _DATA_START.month) + 1
    else:
        total = months_back

    result = []
    for i in range(total):
        m = now.month - i
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        result.append(f"{y:04d}{m:02d}")
    return result


async def fetch_transactions(
    lawd_cd: str,
    property_type: PropertyType,
    transaction_type: TransactionType = TransactionType.TRADE,
    months_back: int = 6,
    max_rows: int = 1000,
) -> list[Transaction]:
    """특정 지역의 실거래 데이터를 병렬로 조회합니다.

    months_back=0 이면 2006년부터 전체 데이터를 조회합니다.
    """
    # 프로세스 최초 호출 시 디스크 캐시 preload (멱등).
    _load_cache_from_disk()

    settings = get_settings()
    endpoint_info = API_ENDPOINTS.get((property_type, transaction_type))
    if not endpoint_info:
        return []

    url = f"{settings.MOLIT_BASE_URL}{endpoint_info['path']}"
    decoded_key = unquote(settings.DATA_GO_KR_API_KEY)
    deal_ymds = _months_to_fetch(months_back)

    sem = asyncio.Semaphore(_CONCURRENCY)
    cur_ymd = _current_ymd()
    path = endpoint_info["path"]

    async def fetch_one(deal_ymd: str, client: httpx.AsyncClient) -> list[Transaction]:
        # 과거 월은 프로세스 캐시에서 즉시 반환 (실패→빈결과 변동성 제거)
        cache_key = (path, lawd_cd, deal_ymd)
        if deal_ymd != cur_ymd and cache_key in _MOLIT_CACHE:
            return _MOLIT_CACHE[cache_key]

        # 엔드포인트 쿼터 소진 상태면 즉시 단락
        until = _QUOTA_BLOCKED.get(path)
        if until and _time.time() < until:
            return []

        params = {
            "serviceKey": decoded_key,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "pageNo": "1",
            "numOfRows": str(max_rows),
        }
        # 일시적 타임아웃/5xx/파싱오류 시 재시도 (지수 백오프).
        # 429(Too Many Requests) 는 MOLIT rate limit 으로 특히 길게 대기한다.
        # 재시도로도 실패하면 빈 리스트 반환하되, 캐시에는 저장하지 않아
        # 다음 요청에서 재시도될 수 있게 한다.
        last_err: Exception | None = None
        MAX_ATTEMPTS = 6
        for attempt in range(MAX_ATTEMPTS):
            is_rate_limit = False
            async with sem:
                try:
                    resp = await client.get(url, params=params)
                    if resp.status_code == 429:
                        # 본문에 "quota exceeded" 가 포함되면 엔드포인트 전체를 차단 플래그
                        if "quota" in (resp.text or "").lower():
                            _QUOTA_BLOCKED[path] = _time.time() + _QUOTA_BLOCK_SECONDS
                            print(
                                f"[molit_api] quota exceeded for {path}; "
                                f"blocking {int(_QUOTA_BLOCK_SECONDS)}s"
                            )
                            return []
                        is_rate_limit = True
                        raise RuntimeError("HTTP 429")
                    if resp.status_code != 200:
                        raise RuntimeError(f"HTTP {resp.status_code}")
                    root = ET.fromstring(resp.text)
                    rc = root.findtext(".//resultCode")
                    if rc not in ("00", "000"):
                        # 정상 응답이지만 데이터 없음(코드 03 등)은 재시도 의미 없음
                        if rc in ("03",):
                            _MOLIT_CACHE[cache_key] = []
                            if deal_ymd != cur_ymd:
                                asyncio.create_task(
                                    _persist_cache_entry(path, lawd_cd, deal_ymd, [])
                                )
                            return []
                        raise RuntimeError(f"resultCode={rc}")
                    items = root.findall(".//item")
                    parsed = _parse_transactions(
                        items,
                        property_type,
                        transaction_type,
                        endpoint_info["name_field"],
                        endpoint_info["area_field"],
                        endpoint_info["area_type"],
                    )
                    if deal_ymd != cur_ymd:
                        _MOLIT_CACHE[cache_key] = parsed
                        # SQLite persist (비동기, 실패 무시). current month 는 변동성
                        # 때문에 디스크에 남기지 않는다.
                        asyncio.create_task(
                            _persist_cache_entry(path, lawd_cd, deal_ymd, parsed)
                        )
                    return parsed
                except Exception as e:
                    last_err = e
            # backoff (세마포어 밖에서 대기해 다른 요청 진행시키기)
            if is_rate_limit:
                # 429: 더 긴 백오프 (2 → 4 → 8 → 16 → 30 → 30초)
                delay = min(30.0, 2.0 * (2 ** attempt))
            else:
                delay = 0.5 * (2 ** attempt)
            await asyncio.sleep(delay)
        # 전체 실패: 로그 남기고 빈 결과 (캐시 X)
        print(f"[molit_api] fetch failed ymd={deal_ymd} lawd={lawd_cd}: {last_err}")
        return []

    # 타임아웃을 넉넉히(30s) + connection pool 공유
    async with httpx.AsyncClient(timeout=30.0) as client:
        results = await asyncio.gather(*[fetch_one(d, client) for d in deal_ymds])

    all_transactions: list[Transaction] = []
    for batch in results:
        all_transactions.extend(batch)
    return all_transactions
