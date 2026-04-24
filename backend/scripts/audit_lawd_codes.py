"""LAWD_CODE_MAP 전체 시군구 코드를 MOLIT 실거래가 API에 순차 조회하여
실제 데이터가 반환되는지 검증(좀비 코드 탐지).

- 부천시 41190 처럼 행정상 존재하지만 MOLIT 이 옛 구코드(41192/41194/41196)로만
  데이터를 반환하는 경우를 찾아낸다.

사용법:
    python scripts/audit_lawd_codes.py
    python scripts/audit_lawd_codes.py --months 202603 202602 202601
    python scripts/audit_lawd_codes.py --types apt villa house

출력:
    code      name              apt  villa house  (마지막 줄) ZOMBIE 후보 목록
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import unquote

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.services.address_lookup import LAWD_CODE_MAP  # noqa: E402

PATHS = {
    "apt":   "/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
    "villa": "/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
    "house": "/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
    "offi":  "/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
    "land":  "/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade",
    "nrg":   "/RTMSDataSvcNrgTrade/getRTMSDataSvcNrgTrade",
    "indu":  "/RTMSDataSvcInduTrade/getRTMSDataSvcInduTrade",
}


async def fetch_count(
    client: httpx.AsyncClient,
    base_url: str,
    key: str,
    path: str,
    lawd: str,
    ymd: str,
) -> tuple[int, str]:
    """(row_count, error_or_code). error 는 비정상인 경우만 채워진다."""
    url = f"{base_url}{path}"
    params = {
        "serviceKey": key,
        "LAWD_CD": lawd,
        "DEAL_YMD": ymd,
        "pageNo": "1",
        "numOfRows": "10",
    }
    try:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return 0, f"http={r.status_code}"
        root = ET.fromstring(r.text)
        rc = root.findtext(".//resultCode") or ""
        total = int(root.findtext(".//totalCount") or "0")
        if rc not in ("00", "000", "03"):
            return total, f"rc={rc}"
        return total, ""
    except Exception as e:  # noqa: BLE001
        return 0, f"err={type(e).__name__}:{e}"


async def audit(months: list[str], types: list[str], concurrency: int) -> None:
    s = get_settings()
    base_url = s.MOLIT_BASE_URL
    key = unquote(s.DATA_GO_KR_API_KEY)

    # code → display name (첫 매핑만 사용)
    code_name: dict[str, str] = {}
    for k, c in LAWD_CODE_MAP.items():
        code_name.setdefault(c, k)

    codes = sorted(code_name.keys())
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=20.0) as client:
        header = f"{'code':<6} {'name':<24}" + "".join(f" {t:>6}" for t in types)
        print(header)
        print("-" * len(header))

        zombies: list[tuple[str, str]] = []

        async def run_one(code: str) -> tuple[str, dict[str, int], list[str]]:
            totals: dict[str, int] = {}
            errs: list[str] = []
            for t in types:
                path = PATHS[t]
                total = 0
                for ymd in months:
                    async with sem:
                        cnt, err = await fetch_count(client, base_url, key, path, code, ymd)
                    if err:
                        errs.append(f"{t}/{ymd}:{err}")
                    total += cnt
                totals[t] = total
            return code, totals, errs

        # 순차 실행 (concurrency semaphore 는 내부 월별 호출에 사용)
        for code in codes:
            code, totals, errs = await run_one(code)
            line = f"{code:<6} {code_name[code]:<24}" + "".join(
                f" {totals[t]:>6}" for t in types
            )
            if errs:
                line += f"  ! {errs[0]}"
            print(line)
            if all(totals[t] == 0 for t in types) and not errs:
                zombies.append((code, code_name[code]))

        print()
        if zombies:
            print(f"== ZOMBIE 후보 ({len(zombies)}) ==")
            for c, n in zombies:
                print(f"  {c}  {n}")
        else:
            print("== ZOMBIE 없음 ==")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="+", default=["202603", "202602", "202601"])
    # LAND 는 일일 쿼터 한도가 타이트하므로 좀비 탐지 기본셋에서 제외.
    # 좀비 판별은 housing 3종(apt/villa/house)이 동시에 0 이면 충분.
    ap.add_argument("--types", nargs="+", default=["apt", "villa", "house"])
    ap.add_argument("--concurrency", type=int, default=3)
    args = ap.parse_args()

    asyncio.run(audit(args.months, args.types, args.concurrency))


if __name__ == "__main__":
    main()
