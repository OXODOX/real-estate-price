"""건축물대장 + VWorld 기반 마스킹 지번 복원 서비스

단독/다가구/상업업무용 등 MOLIT에서 지번이 '1**'처럼 마스킹되는 유형에 대해,
같은 법정동의 건축물대장 전체를 조회하고 연면적/대지면적/건축연도를 매칭해
실제 지번을 추정합니다.
"""
from __future__ import annotations

import asyncio
import json
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


async def _get_bjdong_cd(sigungu_cd: str, dong_name: str) -> str | None:
    """VWorld DISTRICT 검색으로 법정동코드(10자리 중 bjdongCd 5자리) 획득."""
    key = (sigungu_cd, dong_name)
    if key in _BJDONG_CACHE:
        return _BJDONG_CACHE[key]

    settings = get_settings()
    if not settings.VWORLD_API_KEY or not dong_name:
        return None

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
            addr_query = (
                f"{sigungu_cd} {dong_name}" if not dong_name.startswith(sigungu_cd) else dong_name
            )
            # sigunguCd 숫자로는 의미 없으니 dong_name 만 쿼리에 넣되,
            # 매칭은 id 선두 5자리로 검증해 다른 시군구의 동명이 들어가지 않게 함.
            params = {
                "service": "search",
                "request": "search",
                "version": "2.0",
                "key": settings.VWORLD_API_KEY,
                "query": dong_name,
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


def _match_building(tx, buildings: list[dict]) -> tuple[str | None, bool]:
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
    #       buildingAr 이 **연면적**(totArea) 의미이며 건축면적/연면적 구분 필드 없음.
    # 따라서 상업/공장 유형은 tx.building_ar 를 totArea 후보값으로 매핑해야
    # 건축물대장 tot_area 와 정확일치 비교가 가능.
    pt_val = getattr(getattr(tx, "property_type", None), "value", "") or ""
    if pt_val in ("상업업무용", "공장창고"):
        tx_tot = tx.building_ar or tx.total_floor_ar or 0
        tx_plat = tx.plottage_ar or 0
        tx_arch = 0  # 건축면적 정보 없음 → 검증 생략
    else:
        tx_tot = tx.total_floor_ar or 0
        tx_plat = tx.plottage_ar or 0
        tx_arch = tx.building_ar or 0
    tx_year = tx.build_year or 0

    # 거래가 제공한 필드가 하나도 없으면 검증 불가 → 보류
    if tx_tot <= 0 and tx_plat <= 0 and tx_arch <= 0 and tx_year <= 0:
        return None, False

    TOL = 0.01  # 면적 허용 오차 (㎡)

    def exactly_matches(b: dict) -> bool:
        # 거래가 제공한 각 필드가 건축물대장과 정확히 일치해야 함.
        # 거래에 없는 필드 / 대장에 값 없는 필드(=0 또는 공백) 는 검증 생략.
        # (데이터 품질 이슈로 결측된 필드까지 정확일치를 요구하면 오래된 건물이
        #  전부 복원 실패하므로, "값이 있는 필드만 정확일치" 정책으로 완화.)
        # 단, 실제로 검증된 필드가 하나라도 있어야 매칭 인정 (무검증 통과 방지).
        verified = 0
        if tx_tot > 0:
            bv = b.get("totArea")
            if bv is not None and bv > 0:
                if abs(bv - tx_tot) > TOL:
                    return False
                verified += 1
        if tx_plat > 0:
            bv = b.get("platArea")
            # 대장 대지면적=0 은 대지 분리등록 안 된 구건물 케이스 → 검증 생략
            if bv is not None and bv > 0:
                if abs(bv - tx_plat) > TOL:
                    return False
                verified += 1
        if tx_arch > 0:
            bv = b.get("archArea")
            if bv is not None and bv > 0:
                if abs(bv - tx_arch) > TOL:
                    return False
                verified += 1
        # 연도 체크: MOLIT year 가 1900 이하(placeholder)이거나
        # 대장 사용승인일이 비어있으면 연도 검증 생략.
        if tx_year > 1900:
            apr = str(b.get("useAprDay") or "").strip()
            if apr and len(apr) >= 4:
                try:
                    by = int(apr[:4])
                    if by != tx_year:
                        return False
                    verified += 1
                except ValueError:
                    pass
        return verified > 0

    exact = [b for b in candidates if exactly_matches(b)]

    # 정확히 하나일 때만 복원. 0개(검증 실패) 또는 2개 이상(모호)은 보류.
    if len(exact) != 1:
        return None, False

    b = exact[0]
    bun = str(b.get("bun", "")).lstrip("0") or "0"
    ji = str(b.get("ji", "")).lstrip("0")
    return (f"{bun}-{ji}" if ji and ji != "0" else bun), True


def _match_parcel(tx, parcels: list[dict]) -> tuple[str | None, bool]:
    """토지 거래와 토지특성정보(parcels) 후보를 완전일치 기반으로 매칭.

    정책:
    1) 본번 마스킹 패턴 일치 후보 + sanji(일반/산) 일치 수집
    2) 지분거래(share_dealing_type 존재)는 dealArea≠필지면적이라 보류
    3) 거래가 제공한 면적(deal_area) / 지목 / 용도지역(landUse) 중 값 있는 것이
       필지 데이터와 **정확 일치**. 유일 후보일 때만 복원.
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

    def matches(p: dict) -> bool:
        verified = 0
        if tx_area > 0:
            la = p.get("landArea")
            if la is not None and la > 0:
                if abs(la - tx_area) > TOL:
                    return False
                verified += 1
        if tx_jimok:
            jn = (p.get("jimokNm") or "").strip()
            if jn:
                if jn != tx_jimok:
                    return False
                verified += 1
        if tx_use:
            lu = (p.get("landUse") or "").strip()
            if lu:
                # 용도지역명 정확 일치 요구 (축약/전개 차이는 있을 수 있음)
                if lu != tx_use:
                    return False
                verified += 1
        return verified > 0

    exact = [p for p in candidates if matches(p)]
    if len(exact) != 1:
        return None, False

    p = exact[0]
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
            jibun, certain = _match_building(t, subset)
            if jibun:
                t.estimated_jibun = jibun
                t.address_estimated = True
                t.address_estimated_certain = certain

    # 토지 거래 그룹 처리: parcels 테이블로 본번 패턴 + 면적/지목/용도지역 매칭
    async def process_land_group(sgg_cd: str, dong_name: str, txs: list):
        bjdong = await _get_bjdong_cd(sgg_cd, dong_name)
        if not bjdong:
            return
        parcels = await _fetch_all_parcels(sgg_cd, bjdong)
        if not parcels:
            return
        # 건물 그룹과 동일하게 동일 마스킹 패턴 공유 시 후보를 캐싱
        pattern_cache: dict[tuple[str, str], list[dict]] = {}

        def candidates_for(masked_bun: str, want_sanji: str) -> list[dict]:
            key = (masked_bun, want_sanji)
            cached = pattern_cache.get(key)
            if cached is not None:
                return cached
            result = [
                p for p in parcels
                if _bun_matches(str(p.get("bun", "")).zfill(4), masked_bun)
                and (p.get("sanji") or "1") == want_sanji
            ]
            pattern_cache[key] = result
            return result

        for t in txs:
            masked_bun = _bun_from_jibun(t.jibun)
            if not masked_bun or "*" not in masked_bun:
                continue
            want_sanji = "2" if t.jibun and "산" in t.jibun else "1"
            subset = candidates_for(masked_bun, want_sanji)
            if not subset:
                continue
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
