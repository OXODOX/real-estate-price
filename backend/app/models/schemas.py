"""FastAPI 요청/응답 모델.

이 파일은 API 입출력 계약(contract)을 정의한다. 여기 정의된 필드가
그대로 자동 생성 API 문서(/docs 페이지) 에 노출되므로, 사용자·프론트
개발자가 이해할 수 있도록 ``description`` 을 명확히 작성한다.

규칙:
- 새 필드 추가 시 반드시 ``description`` 채우기
- 입력 필드(``PriceRequest``) 는 ``examples`` 도 포함
- 사소한 내부 플래그는 ``description`` 에 "(내부용)" 표기
"""
from pydantic import BaseModel, Field
from enum import Enum


class PropertyType(str, Enum):
    """부동산 유형 (MOLIT 실거래가 카테고리와 1:1 매핑)."""
    APT = "아파트"
    VILLA = "연립다세대"
    HOUSE = "단독다가구"
    OFFICETEL = "오피스텔"
    LAND = "토지"
    COMMERCIAL = "상업업무용"
    SILV = "분양권전매"       # 아파트 분양권 전매
    INDU = "공장창고"         # 공장 및 창고


class TransactionType(str, Enum):
    """거래 유형."""
    TRADE = "매매"
    RENT = "전월세"


# ──────────────────────────────────────────────────────────
# 요청(Request)
# ──────────────────────────────────────────────────────────


class PriceRequest(BaseModel):
    """실거래가 조회 요청 본문."""

    address: str = Field(
        ...,
        description=(
            "조회하려는 주소. 지번 또는 도로명 모두 허용. "
            "시/군/구 + 동 최소 조합 권장. 예) '서울 강남구 역삼동 679-13', "
            "'강남구 테헤란로 152', '부천시 괴안동'."
        ),
        examples=["서울 강남구 역삼동 679-13 래미안그레이튼"],
    )
    property_type: PropertyType | None = Field(
        default=None,
        description=(
            "부동산 유형. 생략 시 '아파트'(APT). 토지/상업 등 다른 유형은 "
            "명시해야 함."
        ),
        examples=["아파트"],
    )
    area_m2: float | None = Field(
        default=None,
        description=(
            "타겟 전용면적(㎡). 값이 주어지면 해당 면적과 가까운 거래를 우선 "
            "정렬·선택. 생략 시 모든 면적 포함."
        ),
        examples=[84.99],
    )
    building_name: str | None = Field(
        default=None,
        description=(
            "단지/건물 이름 필터. 주소에 포함된 이름으로도 자동 추출되지만, "
            "명시하면 더 정확하게 매칭."
        ),
        examples=["래미안그레이튼"],
    )
    months_back: int = Field(
        default=6,
        ge=0,
        le=60,
        description=(
            "지금으로부터 몇 개월치 거래를 조회할지. 0 이면 MOLIT 데이터 "
            "시작(2006) 부터 전체 기간 조회. 1~60 은 해당 개월 수."
        ),
        examples=[12],
    )
    jimok: str | None = Field(
        default=None,
        description=(
            "지목 필터 (예: '대', '전', '답'). 토지(`property_type=토지`) "
            "검색일 때만 의미. 다른 유형이면 무시."
        ),
        examples=["대"],
    )


# ──────────────────────────────────────────────────────────
# 응답(Response) - 단일 거래
# ──────────────────────────────────────────────────────────


class Transaction(BaseModel):
    """개별 실거래 건 하나."""

    # --- 기본 분류 ---
    property_type: PropertyType = Field(description="부동산 유형.")
    transaction_type: TransactionType = Field(description="거래 유형(매매/전월세).")
    name: str = Field(
        default="",
        description="단지/건물 이름. 토지·상업용은 빈 문자열일 수 있음.",
    )

    # --- 주소 ---
    sgg_cd: str = Field(default="", description="시군구 법정동코드 5자리 (예: '11680').")
    sgg_nm: str = Field(default="", description="시군구명 (예: '강남구').")
    dong: str = Field(default="", description="법정동 (예: '역삼동').")
    jibun: str = Field(
        default="",
        description=(
            "지번. 국토부 원본이 마스킹된 경우 끝자리가 '*' 로 채워져 있을 수 있음. "
            "본 서버가 건축물대장·토지대장으로 복원 가능하면 복원된 값으로 대체."
        ),
    )
    road_address: str = Field(
        default="",
        description="JUSO API 로 조회한 도로명 주소 (주요 거래에만 부가).",
    )
    estimated_jibun: str = Field(
        default="",
        description="마스킹 지번을 건축물/토지대장으로 추정한 결과 (내부용).",
    )
    address_estimated: bool = Field(
        default=False,
        description="지번이 원본(마스킹)이 아닌 추정값임을 표시.",
    )
    address_estimated_certain: bool = Field(
        default=False,
        description="추정 결과가 유일하게 결정된 경우(신뢰도 높음) True.",
    )

    # --- 거래 ---
    price_man_won: int = Field(description="거래금액 (단위: 만원).")
    deal_year: int = Field(description="계약 연도.")
    deal_month: int = Field(description="계약 월 (1~12).")
    deal_day: int = Field(description="계약 일 (1~31).")
    floor: int | None = Field(default=None, description="층수. 토지·단층 등은 null.")
    build_year: int | None = Field(default=None, description="건축년도.")
    dealing_gbn: str = Field(
        default="", description="거래 방식: '직거래', '중개거래' 등."
    )
    buyer_gbn: str = Field(default="", description="매수자 구분: '개인', '법인', '공공기관' 등.")
    sler_gbn: str = Field(default="", description="매도자 구분: '개인', '법인', '공공기관' 등.")

    # --- 해제 ---
    cdeal_day: str = Field(
        default="",
        description="계약 해제 사유 발생일 (YYYYMMDD). 해제되지 않았으면 빈 값.",
    )
    cdeal_type: str = Field(
        default="", description="해제 여부 표시(원본 값 그대로)."
    )

    # --- 면적 ---
    area_m2: float = Field(
        default=0,
        description=(
            "주 면적(㎡). 유형별 대표 면적을 여기에 담는다 — 아파트/빌라는 "
            "전용면적, 토지는 거래면적, 단독/상업은 연면적 또는 건물면적."
        ),
    )
    area_type: str = Field(
        default="",
        description="area_m2 가 무엇인지 설명(예: '전용면적', '거래면적', '연면적').",
    )
    exclu_use_ar: float | None = Field(
        default=None, description="전용면적 (아파트/빌라/오피스텔)."
    )
    land_ar: float | None = Field(
        default=None, description="대지권면적 (연립다세대)."
    )
    building_ar: float | None = Field(
        default=None, description="건물(건축)면적 (단독/상업/공장)."
    )
    plottage_ar: float | None = Field(
        default=None, description="대지면적 (단독/상업/공장)."
    )
    deal_area: float | None = Field(default=None, description="거래면적 (토지).")
    total_floor_ar: float | None = Field(
        default=None, description="연면적 (단독다가구)."
    )

    # --- 부가 ---
    jimok: str = Field(default="", description="지목 (토지: '전', '답', '대' 등).")
    land_use: str = Field(
        default="", description="용도지역 (예: '제1종일반주거지역')."
    )
    house_type: str = Field(
        default="", description="주택 유형 (연립/다세대 구분 등)."
    )
    building_type: str = Field(default="", description="건물 유형.")
    building_use: str = Field(default="", description="건물 용도.")
    share_dealing_type: str = Field(
        default="",
        description="지분 거래 유형 (토지/상업). 빈 값이면 '일반'(전체 거래).",
    )

    @property
    def full_address(self) -> str:
        """전체 주소 조합 문자열 (예: '강남구 역삼동 972')."""
        parts = []
        if self.sgg_nm:
            parts.append(self.sgg_nm)
        if self.dong:
            parts.append(self.dong)
        if self.jibun:
            parts.append(self.jibun)
        return " ".join(parts)

    @property
    def address(self) -> str:
        """동 + 지번 (하위 호환용)."""
        return f"{self.dong} {self.jibun}".strip()


# ──────────────────────────────────────────────────────────
# 응답(Response) - 조회 결과
# ──────────────────────────────────────────────────────────


class TransactionResult(BaseModel):
    """``POST /api/v1/estimate`` 응답."""

    address: str = Field(
        description="원본 요청 주소를 그대로 되돌려 준 값 (디버깅/표시용)."
    )
    property_type: PropertyType = Field(description="조회된 부동산 유형.")

    recent_transactions: list[Transaction] = Field(
        description=(
            "주요 거래 목록. 지번·단지·동 기준으로 타겟과 일치도가 높은 거래를 "
            "최신순으로 담는다. UI 에서 기본으로 강조 표시되는 리스트."
        )
    )
    nearby_transactions: list[Transaction] = Field(
        description=(
            "인근 참고 거래. 같은 동 내 다른 거래를 최신순으로 담는다. "
            "기본 숨김 권장, '더 보기' 시 노출."
        )
    )

    # --- 폴백(fallback) 플래그: 왜 '정확 매칭' 이 아니라 '차선' 을 돌려줬는지 표시 ---
    is_fallback: bool = Field(
        default=False,
        description="정확 매칭 거래가 없어 읍/면/동 단위로 폴백했는지 여부.",
    )
    fallback_dong: str = Field(default="", description="폴백된 읍/면/동 이름.")
    bun_fallback: bool = Field(
        default=False,
        description="정확 지번은 없지만 같은 본번의 다른 부번을 반환했는지.",
    )
    fallback_bun: str = Field(default="", description="폴백된 본번 (예: '1506').")
    area_fallback: bool = Field(
        default=False,
        description="요청한 전용면적과 완전 일치 거래가 없어 다른 면적도 포함했는지.",
    )
    building_fallback: bool = Field(
        default=False,
        description="요청한 단지/건물명 매칭 실패 → 다른 기준으로 반환했는지.",
    )
    jimok_fallback: bool = Field(
        default=False,
        description="요청한 지목 매칭 실패 (토지 검색) → 다른 지목 포함했는지.",
    )
