"""원격 mask-service HTTP 클라이언트.

환경변수 `MASK_SERVICE_URL` 이 설정되어 있으면 해당 URL 로 마스킹 복원을
위임한다. 설정되지 않으면 로컬 `enrich_masked_jibun` 을 직접 호출한다.

Render 배포본에서는 `MASK_SERVICE_URL=https://<cloudflared-url>/enrich-masked`
와 `MASK_SERVICE_TOKEN=<공유토큰>` 을 설정해두면, 사용자 PC(4.5GB DB 보유)
가 켜져 있을 때만 마스킹 복원이 동작하고, 꺼져 있으면 조용히 스킵된다.
"""
from __future__ import annotations

import os

import httpx

from app.models.schemas import Transaction
from app.services.bldg_registry import enrich_masked_jibun as _local_enrich


_REMOTE_URL = os.getenv("MASK_SERVICE_URL", "").strip()
_REMOTE_TOKEN = os.getenv("MASK_SERVICE_TOKEN", "").strip()
# Cloudflare Tunnel 경유 기준 왕복 시간 + 서버 처리 + 여유
_TIMEOUT_SECONDS = float(os.getenv("MASK_SERVICE_TIMEOUT", "10.0"))


async def enrich_masked_jibun_any(
    transactions: list[Transaction],
    target_dong: str | None = None,
) -> None:
    """원격 mask-service 가 설정되어 있으면 호출, 아니면 로컬 함수 호출.

    어느 쪽이든 `transactions` 를 in-place 수정 (estimated_jibun /
    address_estimated / address_estimated_certain).

    원격 호출 실패(네트워크 오류/타임아웃/5xx/401 등)는 조용히 무시한다
    — 호출자는 복원 성공 여부에 의존하지 않고, 실패 시 원본 마스킹 지번이
    유지될 뿐이라 UX 상 안전하다.
    """
    if not transactions:
        return

    if not _REMOTE_URL:
        # 로컬 모드: 기존 동작
        await _local_enrich(transactions, target_dong=target_dong)
        return

    # 원격 모드
    payload = {
        "transactions": [t.model_dump() for t in transactions],
        "target_dong": target_dong,
    }
    headers = {"Content-Type": "application/json"}
    if _REMOTE_TOKEN:
        headers["X-Mask-Token"] = _REMOTE_TOKEN

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            resp = await client.post(_REMOTE_URL, json=payload, headers=headers)
        if resp.status_code != 200:
            print(f"[mask_client] remote returned {resp.status_code}: {resp.text[:200]}")
            return
        data = resp.json()
    except Exception as e:
        # 타임아웃, 연결거부, DNS 실패 등 — 마스킹 복원 스킵 (정상 동작)
        print(f"[mask_client] remote call failed (skip mask recovery): {e}")
        return

    # 결과 병합: 서버가 돌려준 index 에 맞춰 원본 transactions 를 수정
    results = data.get("results", []) or []
    for item in results:
        idx = item.get("index")
        if not isinstance(idx, int) or idx < 0 or idx >= len(transactions):
            continue
        est = item.get("estimated_jibun") or ""
        if not est:
            continue
        t = transactions[idx]
        t.estimated_jibun = est
        t.address_estimated = bool(item.get("address_estimated", False))
        t.address_estimated_certain = bool(item.get("address_estimated_certain", False))
