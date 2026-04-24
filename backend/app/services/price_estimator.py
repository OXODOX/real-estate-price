"""실거래 데이터 필터링 및 그룹 분류

입력한 주소에 해당하는 거래(primary)와 같은 동의 인근 거래(nearby)를 분리합니다.

매칭 우선순위: 지번 일치 > 단지명 일치 > 동 일치 > 전체
"""
from app.models.schemas import Transaction, TransactionResult


def _normalize(s: str | None) -> str:
    if not s:
        return ""
    return "".join(c for c in s if c.isalnum()).lower()


def _building_similarity(name1: str | None, name2: str | None) -> float:
    a, b = _normalize(name1), _normalize(name2)
    if not a or not b:
        return 0
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.85
    common = set(a[i : i + 3] for i in range(len(a) - 2))
    common &= set(b[i : i + 3] for i in range(len(b) - 2))
    return 0.5 if common else 0


def _jibun_match(target: str, actual: str) -> bool:
    """사용자가 입력한 지번과 거래 지번이 일치하는지.

    - target 에 부번이 있으면(예: "1506-18") 정확일치만 True.
      같은 본번 다른 부번(1506-10 등)은 별개 필지이므로 primary 에 섞지 않음.
    - target 이 본번만(예: "1506") 이면 본번 일치 거래 전부 True.
    """
    if not target or not actual:
        return False
    t = target.replace(" ", "").strip()
    a = actual.replace(" ", "").strip()
    if t == a:
        return True
    # target 에 부번이 있으면 정확일치만 인정
    if "-" in t:
        return False
    # target 이 본번만이면 본번 매칭 허용
    return t == a.split("-")[0]


def _area_match(target: float | None, actual: float) -> bool:
    if not target or actual <= 0:
        return True
    tol = max(5.0, target * 0.1)
    return abs(actual - target) <= tol


def group_transactions(
    transactions: list[Transaction],
    target_dong: str | None = None,
    target_building: str | None = None,
    target_jibun: str | None = None,
    target_address: str = "",
    target_area_m2: float | None = None,
    target_jimok: str | None = None,
) -> TransactionResult | None:
    """거래 내역을 주요/인근으로 분리합니다.

    Returns:
        TransactionResult 또는 None (단지/지번 지정 후 매칭 없는 경우)
    """
    if not transactions:
        return None

    def sort_key(t: Transaction) -> int:
        return t.deal_year * 10000 + t.deal_month * 100 + t.deal_day

    def _dong_match(target: str | None, actual: str) -> bool:
        if not target:
            return True
        return target == actual or target in actual or actual in target

    jibun_group = [
        t for t in transactions
        if target_jibun and _jibun_match(target_jibun, t.jibun)
        and _dong_match(target_dong, t.dong)
    ]
    building_group = [
        t for t in transactions
        if target_building and _building_similarity(target_building, t.name) >= 0.85
        and _dong_match(target_dong, t.dong)
    ]
    dong_group = [
        t for t in transactions
        if target_dong and _dong_match(target_dong, t.dong)
    ]

    # target 이 부번까지 지정된 경우 같은 본번(다른 부번) 거래를 중간 폴백으로 수집.
    # 예: target="1506-18" → 정확일치 없을 때 1506-* 가 fallback_bun 그룹.
    bun_group: list[Transaction] = []
    target_bun = ""
    if target_jibun and "-" in target_jibun:
        target_bun = target_jibun.split("-")[0].strip()
        if target_bun:
            bun_group = [
                t for t in transactions
                if _dong_match(target_dong, t.dong)
                and t.jibun and t.jibun.split("-")[0].strip() == target_bun
            ]

    is_fallback = False
    fallback_dong = ""
    bun_fallback = False
    fallback_bun = ""

    if target_building or target_jibun:
        if jibun_group:
            primary = sorted(jibun_group, key=sort_key, reverse=True)
        elif building_group:
            primary = sorted(building_group, key=sort_key, reverse=True)
        elif bun_group:
            # 정확 지번 매칭 없지만 같은 본번 거래 존재 → 본번 폴백
            primary = sorted(bun_group, key=sort_key, reverse=True)
            bun_fallback = True
            fallback_bun = target_bun
        else:
            # 정확 매칭 없음 → 읍면동 단위 폴백
            is_fallback = True
            if dong_group:
                primary = sorted(dong_group, key=sort_key, reverse=True)
                fallback_dong = target_dong or ""
            else:
                primary = sorted(transactions, key=sort_key, reverse=True)

        primary_ids = {id(t) for t in primary}
        nearby = sorted(
            [t for t in dong_group if id(t) not in primary_ids],
            key=sort_key, reverse=True,
        )
    elif dong_group:
        primary = sorted(dong_group, key=sort_key, reverse=True)
        nearby = []
    else:
        primary = sorted(transactions, key=sort_key, reverse=True)
        nearby = []

    building_fallback = False
    if target_building and primary:
        matched = [t for t in primary if _building_similarity(target_building, t.name) >= 0.85]
        if matched:
            primary = matched
        else:
            building_fallback = True

    jimok_fallback = False
    if target_jimok and primary:
        matched = [t for t in primary if t.jimok == target_jimok]
        if matched:
            primary = matched
        else:
            jimok_fallback = True
        nearby_matched = [t for t in nearby if t.jimok == target_jimok]
        if nearby_matched:
            nearby = nearby_matched

    area_fallback = False
    if target_area_m2:
        primary_filtered = [t for t in primary if _area_match(target_area_m2, t.area_m2)]
        if primary_filtered:
            primary = primary_filtered
        elif primary:
            # 주소는 일치하지만 면적 조건 맞는 거래 없음 → 안내 후 원본 유지
            area_fallback = True
        nearby_filtered = [t for t in nearby if _area_match(target_area_m2, t.area_m2)]
        if nearby_filtered:
            nearby = nearby_filtered

    return TransactionResult(
        address=target_address or (transactions[0].address if transactions else ""),
        property_type=transactions[0].property_type,
        recent_transactions=primary,
        nearby_transactions=nearby,
        is_fallback=is_fallback,
        fallback_dong=fallback_dong,
        bun_fallback=bun_fallback,
        fallback_bun=fallback_bun,
        area_fallback=area_fallback,
        building_fallback=building_fallback,
        jimok_fallback=jimok_fallback,
    )
