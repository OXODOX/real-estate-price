"""건축물대장 + VWorld 기반 마스킹 지번 복원 서비스

단독/다가구/상업업무용 등 MOLIT에서 지번이 '1**'처럼 마스킹되는 유형에 대해,
같은 법정동의 건축물대장 전체를 조회하고 연면적/대지면적/건축연도를 매칭해
실제 지번을 추정합니다.
"""
from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from pathlib import Path
from urllib.parse import unquote

import httpx
from app.config import get_settings

_VWORLD_URL = "https://api.vworld.kr/req/search"
_BLDG_URL = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"

_CACHE_ROOT = Path(__file__).resolve().parent.parent.parent / ".cache"
_CACHE_DIR = _CACHE_ROOT / "bldg"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_BJDONG_CACHE_FILE = _CACHE_DIR / "bjdong.json"
_BLDG_DB_PATH = _CACHE_ROOT / "bldg.db"  # scripts/import_bldg_titles.py가 생성

_BJDONG_CACHE: dict[tuple[str, str], str] = {}   # (sigunguCd, dong_name) → bjdongCd
_BLDG_CACHE: dict[tuple[str, str], list[dict]] = {}  # (sigunguCd, bjdongCd) → buildings
_PARCELS_CACHE: dict[tuple[str, str], list[dict]] = {}  # (sigunguCd, bjdongCd) → parcels
# 토지이동이력: 시점별 매칭용. 한 bjdong 의 모든 이력 행을 한 번에 캐싱.
_PARCELS_HIST_CACHE: dict[tuple[str, str], list[dict]] = {}


def _load_bjdong_cache() -> None:
    if _BJDONG_CACHE_FILE.exists():
        try:
            data = json.loads(_BJDONG_CACHE_FILE.read_text(encoding="utf-8"))
            for k, v in data.items():
                sgg, dong = k.split("|", 1)
                _BJDONG_CACHE[(sgg, dong)] = v
        except Exception:
            pass


def _save_bjdong_cache() -> None:
    try:
        data = {f"{sgg}|{dong}": v for (sgg, dong), v in _BJDONG_CACHE.items()}
        _BJDONG_CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _bldg_cache_path(sigungu_cd: str, bjdong_cd: str) -> Path:
    return _CACHE_DIR / f"{sigungu_cd}_{bjdong_cd}.json"


def _load_bldg_from_disk(sigungu_cd: str, bjdong_cd: str) -> list[dict] | None:
    p = _bldg_cache_path(sigungu_cd, bjdong_cd)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_bldg_to_disk(sigungu_cd: str, bjdong_cd: str, items: list[dict]) -> None:
    try:
        _bldg_cache_path(sigungu_cd, bjdong_cd).write_text(
            json.dumps(items, ensure_ascii=False), encoding="utf-8"
        )
    except Exception:
        pass


_load_bjdong_cache()


def _query_sqlite(sigungu_cd: str, bjdong_cd: str) -> list[dict] | None:
    """벌크파일에서 import된 SQLite DB가 있으면 거기서 조회.

    없거나 해당 법정동 행이 0건이면 None 반환 (→ API 폴백).
    반환 dict 스키마는 API 응답 필드명(bun/ji/totArea 등)과 동일하게 맞춰
    기존 매칭 로직을 그대로 쓸 수 있다.
    """
    if not _BLDG_DB_PATH.exists():
        return None
    try:
        with sqlite3.connect(f"file:{_BLDG_DB_PATH}?mode=ro", uri=True) as conn:
            # status/demolish_day 컬럼이 없는 구 스키마 호환
            cols = {r[1] for r in conn.execute("PRAGMA table_info(buildings)").fetchall()}
            has_status = "status" in cols
            has_demolish = "demolish_day" in cols
            status_col = "status" if has_status else "'active' AS status"
            demolish_col = "demolish_day" if has_demolish else "NULL AS demolish_day"
            cur = conn.execute(
                f"""
                SELECT bun, ji, bld_nm, plat_area, arch_area, tot_area,
                       main_purps_nm, use_apr_day, {status_col}, {demolish_col}
                FROM buildings
                WHERE sigungu_cd = ? AND bjdong_cd = ?
                """,
                (sigungu_cd, bjdong_cd),
            )
            rows = cur.fetchall()
    except sqlite3.Error:
        return None
    if not rows:
        return None
    return [
        {
            "bun": r[0],
            "ji": r[1],
            "bldNm": r[2] or "",
            "platArea": r[3],
            "archArea": r[4],
            "totArea": r[5],
            "mainPurpsCdNm": r[6] or "",
            "useAprDay": r[7] or "",
            "status": r[8] or "active",
            "demolishDay": r[9] or "",
        }
        for r in rows
    ]


def _sigungu_name(sigungu_cd: str) -> str:
    """LAWD_CODE_MAP 의 reverse 조회로 시군구 이름 획득. 실패 시 빈 문자열.

    예: '41220' → '경기 평택시'. VWorld 검색 시 동명이 다른 시군구와 안 섞이게
    검색어 앞에 붙여 사용한다.
    """
    try:
        from app.services.address_lookup import LAWD_CODE_MAP
    except Exception:
        return ""
    for name, code in LAWD_CODE_MAP.items():
        if code == sigungu_cd:
            return name
    return ""


async def _get_bjdong_cd(sigungu_cd: str, dong_name: str) -> str | None:
    """VWorld DISTRICT/ADDRESS 검색으로 법정동코드(5자리) 획득."""
    key = (sigungu_cd, dong_name)
    if key in _BJDONG_CACHE:
        return _BJDONG_CACHE[key]

    settings = get_settings()
    if not settings.VWORLD_API_KEY or not dong_name:
        return None

    sgg_nm = _sigungu_name(sigungu_cd)  # 예: '경기 평택시', '서울 강남구'

    # "삼죽면 내장리" 같이 공백이 섞이면 VWorld가 못 찾는 경우가 많음.
    # 공백이 있으면 마지막 토큰(리/동)만 우선, 실패 시 전체 쿼리로 폴백.
    queries: list[str] = []
    if " " in dong_name:
        last = dong_name.split()[-1]
        if last:
            queries.append(last)
    queries.append(dong_name)

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # 1순위: DISTRICT 검색 (동·읍·면 단위; 빠르고 정확)
            for q in queries:
                params = {
                    "service": "search",
                    "request": "search",
                    "version": "2.0",
                    "key": settings.VWORLD_API_KEY,
                    "query": q,
                    "type": "DISTRICT",
                    "category": "L4",
                    "format": "json",
                    "size": "30",
                }
                resp = await client.get(_VWORLD_URL, params=params)
                data = resp.json()
                items = data.get("response", {}).get("result", {}).get("items", [])
                for it in items:
                    rid = it.get("id", "")
                    # id 예: "11680101" → sigunguCd(5) + bjdong_short(3)
                    if rid.startswith(sigungu_cd) and len(rid) >= 8:
                        bjdong = rid[5:8] + "00"
                        _BJDONG_CACHE[key] = bjdong
                        _save_bjdong_cache()
                        return bjdong

            # 2순위: ADDRESS PARCEL 검색 (리 단위까지 지원)
            # id 는 19자리 PNU: sigunguCd(5) + bjdongCd(5) + ...
            # 동명만 넣으면 다른 시군구가 더 위로 잡혀 30건 안에 우리 시군구가
            # 안 들어올 수 있음. 시군구 이름을 앞에 붙여 정확도 보강.
            addr_queries = []
            if sgg_nm:
                addr_queries.append(f"{sgg_nm} {dong_name}")
            addr_queries.append(dong_name)

            for q in addr_queries:
                params = {
                    "service": "search",
                    "request": "search",
                    "version": "2.0",
                    "key": settings.VWORLD_API_KEY,
                    "query": q,
                    "type": "ADDRESS",
                    "category": "PARCEL",
                    "format": "json",
                    "size": "30",
                }
                resp = await client.get(_VWORLD_URL, params=params)
                data = resp.json()
                items = data.get("response", {}).get("result", {}).get("items", [])
                for it in items:
                    rid = it.get("id", "")
                    if len(rid) >= 10 and rid.startswith(sigungu_cd):
                        bjdong = rid[5:10]
                        _BJDONG_CACHE[key] = bjdong
                        _save_bjdong_cache()
                        return bjdong
    except Exception:
        pass
    return None


_BLDG_SEM = asyncio.Semaphore(8)


async def _fetch_all_buildings(sigungu_cd: str, bjdong_cd: str) -> list[dict]:
    """법정동 전체 건축물대장 표제부 조회 (병렬 페이지네이션)."""
    key = (sigungu_cd, bjdong_cd)
    if key in _BLDG_CACHE:
        return _BLDG_CACHE[key]

    # 1순위: 벌크파일 SQLite (전국 데이터, 즉시 응답)
    sql_rows = _query_sqlite(sigungu_cd, bjdong_cd)
    if sql_rows is not None:
        _BLDG_CACHE[key] = sql_rows
        return sql_rows

    # 2순위: 이전 API 호출로 저장된 JSON 디스크 캐시
    disk = _load_bldg_from_disk(sigungu_cd, bjdong_cd)
    if disk is not None:
        _BLDG_CACHE[key] = disk
        return disk

    # 3순위: API 실시간 조회 (느림)
    settings = get_settings()
    if not settings.BLDG_REG_API_KEY:
        return []

    service_key = unquote(settings.BLDG_REG_API_KEY)
    rows = 100  # API 하드 상한

    async def fetch_page(client: httpx.AsyncClient, page: int) -> tuple[list[dict], int]:
        async with _BLDG_SEM:
            params = {
                "serviceKey": service_key,
                "sigunguCd": sigungu_cd,
                "bjdongCd": bjdong_cd,
                "pageNo": str(page),
                "numOfRows": str(rows),
                "_type": "json",
            }
            resp = await client.get(_BLDG_URL, params=params)
            data = resp.json()
        body = data.get("response", {}).get("body", {}) or {}
        items = body.get("items", {}) or {}
        item_list = items.get("item", [])
        if isinstance(item_list, dict):
            item_list = [item_list]
        total = int(body.get("totalCount", 0) or 0)
        return item_list, total

    all_items: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            first, total = await fetch_page(client, 1)
            all_items.extend(first)
            if total > rows:
                last_page = min((total + rows - 1) // rows, 100)  # 최대 10,000건 안전 상한
                results = await asyncio.gather(
                    *(fetch_page(client, p) for p in range(2, last_page + 1)),
                    return_exceptions=True,
                )
                for r in results:
                    if isinstance(r, Exception):
                        continue
                    items, _ = r
                    all_items.extend(items)
    except Exception:
        pass

    _BLDG_CACHE[key] = all_items
    if all_items:
        _save_bldg_to_disk(sigungu_cd, bjdong_cd, all_items)
    return all_items


def _query_parcels_sqlite(sigungu_cd: str, bjdong_cd: str) -> list[dict] | None:
    """토지특성정보 SQLite(parcels 테이블)에서 법정동 내 필지 조회.

    parcels 테이블 스키마가 없거나(구축 안 됨) 해당 법정동 행이 0건이면 None.
    """
    if not _BLDG_DB_PATH.exists():
        return None
    try:
        with sqlite3.connect(f"file:{_BLDG_DB_PATH}?mode=ro", uri=True) as conn:
            # parcels 테이블 존재 여부 선확인
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='parcels'"
            ).fetchone()
            if not exists:
                return None
            cur = conn.execute(
                """
                SELECT bun, ji, sanji, jimok_cd, jimok_nm, land_area,
                       land_use, usage_nm, price
                FROM parcels
                WHERE sigungu_cd = ? AND bjdong_cd = ?
                """,
                (sigungu_cd, bjdong_cd),
            )
            rows = cur.fetchall()
    except sqlite3.Error:
        return None
    if not rows:
        return None
    return [
        {
            "bun": r[0],
            "ji": r[1],
            "sanji": r[2] or "1",
            "jimokCd": r[3] or "",
            "jimokNm": r[4] or "",
            "landArea": r[5],
            "landUse": r[6] or "",
            "usageNm": r[7] or "",
            "price": r[8],
        }
        for r in rows
    ]


async def _fetch_all_parcels(sigungu_cd: str, bjdong_cd: str) -> list[dict]:
    """법정동 내 전체 필지(parcels) 조회 (SQLite 전용). 캐시 포함."""
    key = (sigungu_cd, bjdong_cd)
    if key in _PARCELS_CACHE:
        return _PARCELS_CACHE[key]
    rows = _query_parcels_sqlite(sigungu_cd, bjdong_cd) or []
    _PARCELS_CACHE[key] = rows
    return rows


def _query_parcels_history_sqlite(sigungu_cd: str, bjdong_cd: str) -> list[dict] | None:
    """토지이동이력(parcels_history) 테이블에서 법정동 전체 이력 조회.

    parcels_history 테이블이 없거나 0건이면 None 반환 → 호출부는 현재 스냅샷
    (parcels) 으로 자연 폴백.
    """
    if not _BLDG_DB_PATH.exists():
        return None
    try:
        with sqlite3.connect(f"file:{_BLDG_DB_PATH}?mode=ro", uri=True) as conn:
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='parcels_history'"
            ).fetchone()
            if not exists:
                return None
            cur = conn.execute(
                """
                SELECT sanji, bun, ji, seq,
                       jimok_nm, land_area, start_day, end_day, is_jjjs
                FROM parcels_history
                WHERE sigungu_cd = ? AND bjdong_cd = ?
                """,
                (sigungu_cd, bjdong_cd),
            )
            rows = cur.fetchall()
    except sqlite3.Error:
        return None
    if not rows:
        return None
    return [
        {
            "sanji": r[0] or "1",
            "bun": r[1],
            "ji": r[2],
            "seq": r[3],
            "jimok_nm": r[4] or "",
            "land_area": r[5],
            "start_day": r[6] or "",
            "end_day": r[7] or "",
            "is_jjjs": bool(r[8]),
        }
        for r in rows
    ]


async def _fetch_all_parcels_history(sigungu_cd: str, bjdong_cd: str) -> list[dict]:
    """법정동 내 토지이동이력 전체 조회. 캐시 포함."""
    key = (sigungu_cd, bjdong_cd)
    if key in _PARCELS_HIST_CACHE:
        return _PARCELS_HIST_CACHE[key]
    rows = _query_parcels_history_sqlite(sigungu_cd, bjdong_cd) or []
    _PARCELS_HIST_CACHE[key] = rows
    return rows


def _filter_history_by_date(
    history: list[dict], deal_ymd: str
) -> list[dict]:
    """deal_ymd(YYYYMMDD) 시점에 유효했던 이력 행만 추림.

    유효 조건: start_day ≤ deal_ymd AND (end_day == '' OR end_day ≥ deal_ymd).
    end_day 가 빈 값이면 '현재까지 유효' 의미.
    """
    if not deal_ymd or len(deal_ymd) < 8:
        return list(history)
    out: list[dict] = []
    for r in history:
        s = r.get("start_day") or ""
        e = r.get("end_day") or ""
        if s and s > deal_ymd:
            continue
        if e and e < deal_ymd:
            continue
        out.append(r)
    return out


def _has_jijeokjaejosa_after(history: list[dict], deal_ymd: str) -> bool:
    """이 법정동에 deal_ymd 이후 '지적재조사' 이력이 다수 존재하는지.

    True 면 거래시점 옛 지번이 이후 일괄 폐쇄·재부여된 영향구역으로 간주.
    이 경우 현재 스냅샷으로 매칭하면 false positive 위험이 큼 → 매칭 보류.
    """
    if not deal_ymd:
        return False
    cnt = 0
    for r in history:
        if r.get("is_jjjs"):
            s = r.get("start_day") or ""
            if s and s > deal_ymd:
                cnt += 1
                if cnt >= 5:  # 일괄성 판정 임계
                    return True
    return False


def _is_masked(jibun: str) -> bool:
    return "*" in (jibun or "")


def _bun_from_jibun(jibun: str) -> str:
    """지번에서 본번만 추출 ('1**-2' → '1**', '109-16' → '109')."""
    return (jibun or "").split("-")[0].strip()


def _bun_matches(padded_bun: str, masked_bun: str) -> bool:
    """4자리 zero-padded bun이 마스킹 패턴에 맞는지.

    '7**' → padded '07**' 로 확장해 위치별 비교 (0700~0799 매칭).
    """
    pattern = masked_bun.zfill(4)
    if len(pattern) != 4 or len(padded_bun) != 4:
        return False
    for p, b in zip(pattern, padded_bun):
        if p == "*":
            continue
        if p != b:
            return False
    return True


def _deal_ymd(tx) -> str:
    """거래 연월일을 YYYYMMDD 문자열로. 누락 시 '' 반환."""
    y = getattr(tx, "deal_year", 0) or 0
    m = getattr(tx, "deal_month", 0) or 0
    d = getattr(tx, "deal_day", 0) or 0
    if y <= 0 or m <= 0:
        return ""
    if d <= 0:
        d = 1
    return f"{y:04d}{m:02d}{d:02d}"


def _existed_at(b: dict, deal_ymd: str) -> bool:
    """해당 건물이 거래일 시점에 존재했는지 판정.

    - status='active': use_apr_day <= deal_ymd (승인일 비면 통과)
    - status='closed': use_apr_day <= deal_ymd <= demolish_day
      (승인일 비면 상한만 체크, 말소일 비면 하한만 체크)
    거래일 자체가 비면 전부 통과 (필터 적용 안 함).
    """
    if not deal_ymd:
        return True
    apr = str(b.get("useAprDay") or "").strip()
    if apr and len(apr) >= 8 and apr.isdigit() and apr > deal_ymd:
        return False  # 승인일 이후에 생긴 거래일 수 없음
    if b.get("status") == "closed":
        dmo = str(b.get("demolishDay") or "").strip()
        if dmo and len(dmo) >= 8 and dmo.isdigit() and dmo < deal_ymd:
            return False  # 이미 멸실된 이후라면 거래 불가능
    return True


_USE_PREFIX_RE = re.compile(r"^제\s*[0-9]+\s*종\s*")


def _purpose_compatible(tx_use: str, mp: str) -> bool:
    """MOLIT buildingUse 와 대장 mainPurpsCdNm 이 같은 용도 카테고리인지.

    예: 거래 '제2종근린생활' ↔ 대장 '근린생활시설' (포괄 분류) → 호환.
        거래 '업무'         ↔ 대장 '업무시설'             → 호환.
        거래 '제2종근린생활' ↔ 대장 '단독주택'             → 비호환.

    각각의 '제N종' 접두 + '시설' 접미를 제거한 뒤 한쪽이 다른 쪽의 부분
    문자열이면 호환으로 간주. 둘 중 하나가 비어 있으면 호출자가 skip.
    """
    a = _USE_PREFIX_RE.sub("", tx_use).removesuffix("시설").strip()
    b = _USE_PREFIX_RE.sub("", mp).removesuffix("시설").strip()
    if not a or not b:
        return False
    return a in b or b in a


def _verified_count(
    b: dict,
    tx_tot: float,
    tx_plat: float,
    tx_arch: float,
    tx_year: int,
    tx_use: str,
    is_nonresi: bool,
    tol: float,
    parcel_area_by_bunji: dict | None = None,
) -> int:
    """후보 건물이 거래의 어떤 필드들을 통과시키는지 점수화.

    정책:
    - 주거(단독·다가구·연립): MOLIT 필드 의미가 잘 정의돼 있어 어긋나면 즉시 탈락.
    - 상업업무용·공장창고: MOLIT 의 buildingAr / plottageAr / buildingUse 가
      각각 대장 totArea / platArea / mainPurpsCdNm 와 종종 다른 의미로
      쓰이는 게 관찰됨. 따라서 어긋나도 즉시 탈락 대신 그 필드만 검증
      생략. 점수 기반 유일 최댓값 후보를 호출자가 선택.
    - 사용승인연도(buildYear)는 모든 유형에서 strict (어긋나면 즉시 탈락).
      가장 신뢰도 높고 신축·옛건물 혼동을 막는 핵심 signal.
    """
    verified = 0

    # 연면적 (totArea ↔ tx_tot)
    if tx_tot > 0:
        bv = b.get("totArea")
        if bv is not None and bv > 0:
            if abs(bv - tx_tot) <= tol:
                verified += 1
            elif not is_nonresi:
                return 0  # 주거: 정확일치 강제

    # 주용도 (mainPurpsCdNm ↔ tx_use)
    if is_nonresi and tx_use:
        mp = (b.get("mainPurpsCdNm") or "").strip()
        if mp:
            if _purpose_compatible(tx_use, mp):
                verified += 1
            # 비호환이어도 reject 하지 않음 — '숙박' vs '제2종근린생활시설' 처럼
            # 등록 분류와 실제 사용 용도가 다른 케이스가 흔함.

    # 대지면적 (buildings.platArea OR parcels.land_area ↔ tx_plat)
    if tx_plat > 0:
        bv = b.get("platArea") or 0
        bv_parcel = 0.0
        if parcel_area_by_bunji is not None:
            key = (str(b.get("bun", "")).zfill(4), str(b.get("ji", "")).zfill(4))
            bv_parcel = parcel_area_by_bunji.get(key) or 0
        bv_match = (bv > 0 and abs(bv - tx_plat) <= tol)
        bp_match = (bv_parcel > 0 and abs(bv_parcel - tx_plat) <= tol)
        if bv_match or bp_match:
            verified += 1
        elif (bv > 0 or bv_parcel > 0) and not is_nonresi:
            # 주거: 양쪽 다 결측이 아닌데 어긋남 → 탈락
            return 0
        # 상업/공장: 어긋나도 skip
    if tx_arch > 0:
        bv = b.get("archArea")
        if bv is not None and bv > 0:
            if abs(bv - tx_arch) > tol:
                return 0
            verified += 1
    if tx_year > 1900:
        apr = str(b.get("useAprDay") or "").strip()
        if apr and len(apr) >= 4:
            try:
                by = int(apr[:4])
                if by != tx_year:
                    return 0
                verified += 1
            except ValueError:
                pass
    return verified


def _match_building(
    tx,
    buildings: list[dict],
    parcel_area_by_bunji: dict | None = None,
) -> tuple[str | None, bool]:
    """거래와 건축물대장 후보들을 완전일치 기반으로 매칭.

    정책:
    1) 마스킹 bun 패턴 일치 후보 수집
    2) 거래일 기준 시점 필터 (존치/멸실 시점이 거래일과 양립해야 함)
    3) 거래가 제공한 수치 필드(연면적/대지면적/건축면적/연도)가 건축물대장과
       **정확히 일치**하는 후보가 **딱 하나**일 때만 복원
    """
    masked_bun = _bun_from_jibun(tx.jibun)
    if not masked_bun or "*" not in masked_bun:
        return None, False

    deal_ymd = _deal_ymd(tx)

    # 1) 마스킹 bun 일치 + 시점 유효 후보
    candidates = []
    for b in buildings:
        padded = str(b.get("bun", "")).zfill(4)
        if not _bun_matches(padded, masked_bun):
            continue
        if not _existed_at(b, deal_ymd):
            continue
        candidates.append(b)
    if not candidates:
        return None, False

    # MOLIT API 는 유형별로 buildingAr 의 의미가 다르다:
    #   - SHTrade(단독다가구): totalFloorAr=연면적, buildingAr=건축면적, plottageAr=대지
    #   - NrgTrade(상업업무용) / InduTrade(공장창고):
    #       buildingAr 가 대장 totArea(연면적) 와 정확 일치하지 않는 케이스가
    #       흔하다 (예: 마포 동교 179-27 → MOLIT 243.2 vs 대장 259.14, 6%차).
    #       정확일치를 강제하면 정상 매물도 빠지므로, 상업/공장은 tx_tot 자체를
    #       사용하지 않고 **대지면적 + 사용승인연도 + 주용도** 로 특정.
    pt_val = getattr(getattr(tx, "property_type", None), "value", "") or ""
    is_nonresi = pt_val in ("상업업무용", "공장창고")
    if is_nonresi:
        # 상업/공장: buildingAr 가 대장 totArea 와 정확 일치하는 케이스도
        # 흔하므로(예: 노고산 57-20 → 둘 다 929.09) signal 로 활용.
        # 다만 어긋나도 reject 하지 않고 skip (의미 불일치 가능성).
        tx_tot = tx.building_ar or tx.total_floor_ar or 0
        tx_plat = tx.plottage_ar or 0
        tx_arch = 0
    else:
        tx_tot = tx.total_floor_ar or 0
        tx_plat = tx.plottage_ar or 0
        tx_arch = tx.building_ar or 0
    tx_year = tx.build_year or 0
    tx_use = (getattr(tx, "building_use", "") or "").strip()  # 상업/공장 보조

    # 거래가 제공한 필드가 하나도 없으면 검증 불가 → 보류
    if (
        tx_tot <= 0
        and tx_plat <= 0
        and tx_arch <= 0
        and tx_year <= 0
        and not (is_nonresi and tx_use)
    ):
        return None, False

    TOL = 0.01  # 면적 허용 오차 (㎡)

    # 검증 점수(verified count) 까지 같이 뽑아 best score 만 채택.
    # 같은 점수의 후보가 여럿이면 모호 → 보류.
    scored: list[tuple[int, dict]] = []
    for b in candidates:
        v = _verified_count(
            b, tx_tot, tx_plat, tx_arch, tx_year, tx_use, is_nonresi, TOL,
            parcel_area_by_bunji,
        )
        if v > 0:
            scored.append((v, b))

    if not scored:
        return None, False

    max_v = max(s for s, _ in scored)
    top = [b for s, b in scored if s == max_v]
    if len(top) != 1:
        return None, False

    b = top[0]
    bun = str(b.get("bun", "")).lstrip("0") or "0"
    ji = str(b.get("ji", "")).lstrip("0")
    return (f"{bun}-{ji}" if ji and ji != "0" else bun), True


def _match_parcel_history(
    tx, history_at_date: list[dict]
) -> tuple[str | None, bool]:
    """거래 시점 유효 이력 행들로 매칭. parcels_history 가 있을 때 우선 사용.

    history_at_date 는 이미 deal_ymd 시점으로 필터링된 이력 행. 본번 마스킹
    패턴 + 산여부 일치 후보를 추리고, 면적 우선 매칭 정책 적용.
    """
    masked_bun = _bun_from_jibun(tx.jibun)
    if not masked_bun or "*" not in masked_bun:
        return None, False

    share = (getattr(tx, "share_dealing_type", "") or "").strip()
    if share and share not in ("일반거래", "전체거래"):
        return None, False

    tx_area = tx.deal_area or 0
    tx_jimok = (tx.jimok or "").strip()

    if tx_area <= 0 and not tx_jimok:
        return None, False

    TOL = 0.5
    want_sanji = "2" if tx.jibun and "산" in tx.jibun else "1"

    candidates: list[dict] = []
    for r in history_at_date:
        if (r.get("sanji") or "1") != want_sanji:
            continue
        bun = str(r.get("bun") or "").zfill(4)
        if not _bun_matches(bun, masked_bun):
            continue
        candidates.append(r)
    if not candidates:
        return None, False

    def _area_match(r: dict) -> bool:
        a = r.get("land_area")
        return a is not None and a > 0 and abs(a - tx_area) <= TOL

    def _jimok_ok(r: dict) -> bool:
        if not tx_jimok:
            return True
        jn = (r.get("jimok_nm") or "").strip()
        return (not jn) or jn == tx_jimok

    if tx_area > 0:
        area_hits = [r for r in candidates if _area_match(r)]
        if len(area_hits) == 1:
            r = area_hits[0]
            bun = str(r.get("bun") or "").lstrip("0") or "0"
            ji = str(r.get("ji") or "").lstrip("0")
            return (f"{bun}-{ji}" if ji and ji != "0" else bun), True
        if len(area_hits) >= 2:
            refined = [r for r in area_hits if _jimok_ok(r)]
            if len(refined) == 1:
                r = refined[0]
                bun = str(r.get("bun") or "").lstrip("0") or "0"
                ji = str(r.get("ji") or "").lstrip("0")
                return (f"{bun}-{ji}" if ji and ji != "0" else bun), True
            return None, False
        # 면적 매칭 실패 → 보류 (이력 데이터가 정확해야 의미 있음)
        return None, False

    # 면적이 거래에 없을 때 — 지목으로만 좁힘
    refined = [
        r for r in candidates
        if _jimok_ok(r) and (r.get("jimok_nm") or "").strip()
    ]
    if len(refined) != 1:
        return None, False
    r = refined[0]
    bun = str(r.get("bun") or "").lstrip("0") or "0"
    ji = str(r.get("ji") or "").lstrip("0")
    return (f"{bun}-{ji}" if ji and ji != "0" else bun), True


def _match_parcel(tx, parcels: list[dict]) -> tuple[str | None, bool]:
    """토지 거래와 토지특성정보(parcels) 후보를 매칭.

    정책 (면적 우선):
    1) 본번 마스킹 패턴 + sanji(일반/산) 일치 후보 수집
    2) 지분거래(share_dealing_type)는 dealArea≠필지면적이라 보류
    3) **면적 우선 매칭**:
       - tx 면적이 있으면 면적 정확일치(TOL 이내) 후보를 먼저 추림
       - 면적 일치 후보가 유일 → **즉시 복원** (지목/용도지역 검증 생략)
         · 사유: 지목/용도지역은 시간이 지나면 변경될 수 있어
           (지목변경, 도시계획 변경 등) 거래시점과 현 토지대장이
           일치하지 않을 수 있음. 면적은 시간 불변에 가깝다.
       - 면적 일치 후보가 2개 이상 → 지목/용도지역으로 추가 필터링
    4) 면적이 없으면 지목/용도지역만으로 정확일치 후보 추리고 유일하면 복원.
    """
    masked_bun = _bun_from_jibun(tx.jibun)
    if not masked_bun or "*" not in masked_bun:
        return None, False

    # 지분거래: 거래면적이 필지면적의 일부 → 정확일치 불가
    share = (getattr(tx, "share_dealing_type", "") or "").strip()
    if share and share not in ("일반거래", "전체거래"):
        # "구분지분" 등 지분거래면 보류. 표현이 다양할 수 있어 안전하게
        # "일반/전체" 외 문자열은 모두 지분으로 간주.
        return None, False

    tx_area = tx.deal_area or 0
    tx_jimok = (tx.jimok or "").strip()
    tx_use = (tx.land_use or "").strip()

    # 검증 가능한 필드가 하나도 없으면 보류
    if tx_area <= 0 and not tx_jimok and not tx_use:
        return None, False

    TOL = 0.5  # 토지면적 허용 오차 (㎡) — 공시 데이터와 거래 데이터 반올림 차이 감안

    # MOLIT LandTrade 는 일반 지번만 취급 (산지는 "산"이 지번에 명시됨).
    # tx.jibun 에 '산' 이 없으면 sanji='1' 후보만 허용.
    want_sanji = "2" if tx.jibun and "산" in tx.jibun else "1"

    candidates: list[dict] = []
    for p in parcels:
        padded = str(p.get("bun", "")).zfill(4)
        if not _bun_matches(padded, masked_bun):
            continue
        if (p.get("sanji") or "1") != want_sanji:
            continue
        candidates.append(p)
    if not candidates:
        return None, False

    def _jimok_ok(p: dict) -> bool:
        if not tx_jimok:
            return True  # 거래에 지목 정보 없음 → 검증 생략
        jn = (p.get("jimokNm") or "").strip()
        if not jn:
            return True  # 대장 지목 없음 → 검증 생략
        return jn == tx_jimok

    def _use_ok(p: dict) -> bool:
        if not tx_use:
            return True
        lu = (p.get("landUse") or "").strip()
        if not lu:
            return True
        return lu == tx_use

    def _area_match(p: dict) -> bool:
        la = p.get("landArea")
        if la is None or la <= 0:
            return False
        return abs(la - tx_area) <= TOL

    # === 면적 우선 매칭 ===
    if tx_area > 0:
        area_matches = [p for p in candidates if _area_match(p)]
        if len(area_matches) == 1:
            # 면적이 유일하게 매칭되면 즉시 복원
            # (지목/용도지역은 시점차로 다를 수 있어 강제하지 않음)
            p = area_matches[0]
            bun = str(p.get("bun", "")).lstrip("0") or "0"
            ji = str(p.get("ji", "")).lstrip("0")
            return (f"{bun}-{ji}" if ji and ji != "0" else bun), True
        if len(area_matches) >= 2:
            # 면적 동률 후보 다수 → 지목/용도지역으로 추가 분리 시도
            refined = [p for p in area_matches if _jimok_ok(p) and _use_ok(p)]
            if len(refined) == 1:
                p = refined[0]
                bun = str(p.get("bun", "")).lstrip("0") or "0"
                ji = str(p.get("ji", "")).lstrip("0")
                return (f"{bun}-{ji}" if ji and ji != "0" else bun), True
            return None, False
        # 면적이 있는데 매칭 후보 0건이면 보류 (대장 면적 결측 다수일 수 있음).
        # 면적 검증을 생략하고 지목/용도로만 매칭 시도하는 폴백.
        if not (tx_jimok or tx_use):
            return None, False
        # fall-through: 지목/용도 기반 매칭

    # === 면적이 없거나 면적 매칭이 비어 폴백된 경우: 지목+용도 정확일치 ===
    refined = [p for p in candidates if _jimok_ok(p) and _use_ok(p)]
    # 검증 가능한 필드가 하나도 없는 후보는 제외 (무조건 통과 방지)
    def _has_any_check(p: dict) -> bool:
        if tx_jimok and (p.get("jimokNm") or "").strip():
            return True
        if tx_use and (p.get("landUse") or "").strip():
            return True
        return False
    refined = [p for p in refined if _has_any_check(p)]
    if len(refined) != 1:
        return None, False
    p = refined[0]
    bun = str(p.get("bun", "")).lstrip("0") or "0"
    ji = str(p.get("ji", "")).lstrip("0")
    return (f"{bun}-{ji}" if ji and ji != "0" else bun), True


async def enrich_masked_jibun(
    transactions: list,
    target_dong: str | None = None,
) -> None:
    """마스킹된 거래에 대해 추정 지번을 채웁니다.

    target_dong 이 주어지면 해당 동(부분일치) 거래만 복원 대상으로 삼아
    VWorld/SQLite 조회 횟수를 크게 줄입니다. 검색한 지번이 속한 동의
    거래만 primary 판별에 필요하므로 이 최적화는 결과에 영향 없음.
    """
    # 법정동별로 그룹핑 (sigunguCd, dong_name). 토지/건물 분기.
    groups: dict[tuple[str, str], list] = {}         # 건물 매칭 대상
    land_groups: dict[tuple[str, str], list] = {}    # 토지 매칭 대상
    for t in transactions:
        if not _is_masked(t.jibun):
            continue
        if not t.sgg_cd or not t.dong:
            continue
        # target_dong 이 있으면 해당 동에 속한 거래만 처리
        if target_dong:
            if target_dong not in t.dong and t.dong not in target_dong:
                continue
        pt_val = getattr(getattr(t, "property_type", None), "value", "") or ""
        if pt_val == "토지":
            land_groups.setdefault((t.sgg_cd, t.dong), []).append(t)
        else:
            groups.setdefault((t.sgg_cd, t.dong), []).append(t)

    if not groups and not land_groups:
        return

    # 각 그룹별로 법정동 건축물대장을 1번만 조회
    async def process_group(sgg_cd: str, dong_name: str, txs: list):
        bjdong = await _get_bjdong_cd(sgg_cd, dong_name)
        if not bjdong:
            return
        buildings = await _fetch_all_buildings(sgg_cd, bjdong)
        if not buildings:
            return

        # 토지대장(parcels) 면적도 같이 인덱싱해 매칭에 활용.
        # MOLIT plottageAr 가 종종 건축물대장 plat_area 가 아닌
        # 토지대장 land_area 를 따라가는 케이스 보정용.
        parcels = await _fetch_all_parcels(sgg_cd, bjdong)
        parcel_area_by_bunji: dict[tuple[str, str], float] = {}
        for p in parcels:
            la = p.get("landArea")
            if la is None or la <= 0:
                continue
            k = (str(p.get("bun", "")).zfill(4), str(p.get("ji", "")).zfill(4))
            # 동일 (bun,ji) 가 여러 개 있을 일은 거의 없으나, 있을 경우 첫값 유지
            parcel_area_by_bunji.setdefault(k, la)

        # 동일 마스킹 패턴(예: "1**")을 공유하는 거래가 많을 때,
        # 패턴별로 후보 건물을 한 번만 필터링해 O(B*T) → O(B*P + C*T) 로 축소.
        pattern_cache: dict[str, list[dict]] = {}

        def candidates_for(masked_bun: str) -> list[dict]:
            cached = pattern_cache.get(masked_bun)
            if cached is not None:
                return cached
            result = [
                b for b in buildings
                if _bun_matches(str(b.get("bun", "")).zfill(4), masked_bun)
            ]
            pattern_cache[masked_bun] = result
            return result

        for t in txs:
            masked_bun = _bun_from_jibun(t.jibun)
            if not masked_bun or "*" not in masked_bun:
                continue
            subset = candidates_for(masked_bun)
            if not subset:
                continue
            jibun, certain = _match_building(t, subset, parcel_area_by_bunji)
            if jibun:
                t.estimated_jibun = jibun
                t.address_estimated = True
                t.address_estimated_certain = certain

    # 토지 거래 그룹 처리: 시점 매칭(parcels_history) → 현재 스냅샷(parcels) 폴백
    async def process_land_group(sgg_cd: str, dong_name: str, txs: list):
        bjdong = await _get_bjdong_cd(sgg_cd, dong_name)
        if not bjdong:
            return
        # 1순위: 토지이동이력 (시점별 면적·지목)
        history = await _fetch_all_parcels_history(sgg_cd, bjdong)
        # 2순위 폴백: 현재 스냅샷
        parcels = await _fetch_all_parcels(sgg_cd, bjdong) if not history else []
        if not history and not parcels:
            return

        # 같은 거래일을 공유하는 거래가 많을 때, 시점 필터링은 비싸지 않지만
        # 거래일별로 캐싱해 반복 비용 최소화.
        time_filter_cache: dict[str, list[dict]] = {}

        def history_at(deal_ymd: str) -> list[dict]:
            cached = time_filter_cache.get(deal_ymd)
            if cached is not None:
                return cached
            cached = _filter_history_by_date(history, deal_ymd)
            time_filter_cache[deal_ymd] = cached
            return cached

        for t in txs:
            masked_bun = _bun_from_jibun(t.jibun)
            if not masked_bun or "*" not in masked_bun:
                continue

            jibun: str | None = None
            certain = False

            if history:
                deal_ymd = _deal_ymd(t)
                snapshot = history_at(deal_ymd)
                if snapshot:
                    jibun, certain = _match_parcel_history(t, snapshot)
                # 매칭 실패 + 지적재조사 영향구역이면 폴백 안 함 (false positive 차단)
                if not jibun and _has_jijeokjaejosa_after(history, deal_ymd):
                    continue
                # 매칭 실패 + 재조사 영향 없음 → 현재 스냅샷 폴백
                if not jibun:
                    parcels_now = await _fetch_all_parcels(sgg_cd, bjdong)
                    if parcels_now:
                        want_sanji = "2" if t.jibun and "산" in t.jibun else "1"
                        subset = [
                            p for p in parcels_now
                            if _bun_matches(str(p.get("bun", "")).zfill(4), masked_bun)
                            and (p.get("sanji") or "1") == want_sanji
                        ]
                        if subset:
                            jibun, certain = _match_parcel(t, subset)
            else:
                # parcels_history 미보유 환경: 기존 로직 그대로
                want_sanji = "2" if t.jibun and "산" in t.jibun else "1"
                subset = [
                    p for p in parcels
                    if _bun_matches(str(p.get("bun", "")).zfill(4), masked_bun)
                    and (p.get("sanji") or "1") == want_sanji
                ]
                if subset:
                    jibun, certain = _match_parcel(t, subset)

            if jibun:
                t.estimated_jibun = jibun
                t.address_estimated = True
                t.address_estimated_certain = certain

    # 건물·토지 그룹 모두 병렬 실행 (각 그룹은 독립적인 법정동)
    await asyncio.gather(
        *(process_group(sgg, dong, txs) for (sgg, dong), txs in groups.items()),
        *(process_land_group(sgg, dong, txs) for (sgg, dong), txs in land_groups.items()),
    )
