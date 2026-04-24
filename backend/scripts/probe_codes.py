"""후보 신코드 탐색: 강원/전북/화성 좀비 코드에 대해 예상 대체 코드를 MOLIT 에 조회.

- 강원특별자치도 (2023): 42xxx → 51xxx 로 대응 추정
- 전북특별자치도 (2024): 45xxx → 52xxx
- 화성시 41590 → 분구(동부/서부) 가능성: 41591/41593/41597 등 탐색
"""
from __future__ import annotations

import asyncio
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import unquote

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402

PATH = "/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"

# (원본코드, 이름, [후보들])
CASES: list[tuple[str, str, list[str]]] = [
    # 강원 42xxx → 51xxx
    ("42110", "춘천",   ["51110"]),
    ("42130", "원주",   ["51130"]),
    ("42150", "강릉",   ["51150"]),
    ("42170", "동해",   ["51170"]),
    ("42190", "태백",   ["51190"]),
    ("42210", "속초",   ["51210"]),
    ("42230", "삼척",   ["51230"]),
    # 전북 45xxx → 52xxx
    ("45111", "전주완산", ["52111"]),
    ("45113", "전주덕진", ["52113"]),
    ("45130", "군산",   ["52130"]),
    ("45140", "익산",   ["52140"]),
    # 화성시 분구 가능성
    ("41590", "화성",   ["41591", "41593", "41595", "41597", "41599",
                          "41600", "41610"]),  # 41610은 광주시라 제외 예정
]


async def probe(client: httpx.AsyncClient, base_url: str, key: str, code: str, ymd: str) -> int:
    params = {"serviceKey": key, "LAWD_CD": code, "DEAL_YMD": ymd,
              "pageNo": "1", "numOfRows": "1"}
    r = await client.get(f"{base_url}{PATH}", params=params)
    if r.status_code != 200:
        return -1
    root = ET.fromstring(r.text)
    return int(root.findtext(".//totalCount") or "0")


async def main() -> None:
    s = get_settings()
    base = s.MOLIT_BASE_URL
    key = unquote(s.DATA_GO_KR_API_KEY)
    ymd = "202603"
    async with httpx.AsyncClient(timeout=20.0) as client:
        for orig, name, cands in CASES:
            orig_cnt = await probe(client, base, key, orig, ymd)
            print(f"{orig} {name}  orig={orig_cnt}")
            for c in cands:
                cnt = await probe(client, base, key, c, ymd)
                mark = "  <-- HIT" if cnt > 0 else ""
                print(f"  {c}: {cnt}{mark}")


if __name__ == "__main__":
    asyncio.run(main())
