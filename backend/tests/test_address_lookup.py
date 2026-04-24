"""주소 → 법정동코드/동/지번/단지명 파싱 단위 테스트.

네트워크·DB 의존 없는 pure-logic 테스트이므로 `fast` 마커.
아래 케이스들은 과거에 실제 버그를 발견했던 회귀(regression) 보호용.
"""
import pytest

from app.services.address_lookup import (
    LAWD_CODE_MAP,
    FAN_OUT_CODES,
    find_lawd_code,
    find_lawd_codes,
    extract_dong,
    extract_jibun,
    extract_building_name,
    normalize_address,
)


pytestmark = pytest.mark.fast


# ──────────────────────────────────────────────────────────
# 법정동코드 해석
# ──────────────────────────────────────────────────────────


class TestFindLawdCode:
    """단일 대표 코드 반환 (`find_lawd_code`)."""

    def test_seoul_full(self):
        assert find_lawd_code("서울특별시 강남구 역삼동") == "11680"

    def test_seoul_abbrev(self):
        assert find_lawd_code("서울 강남구") == "11680"

    def test_seoul_district_only(self):
        # 서울 구 단축 입력 지원
        assert find_lawd_code("강남구 역삼동") == "11680"

    def test_gyeonggi_subdistrict(self):
        # 분구 있는 시: "용인시 수지구" → "용인 수지구"
        assert find_lawd_code("경기 용인시 수지구") == "41465"
        assert find_lawd_code("용인 수지구") == "41465"

    def test_sejong(self):
        assert find_lawd_code("세종특별자치시") == "36110"
        assert find_lawd_code("세종시") == "36110"

    def test_unknown_returns_none(self):
        assert find_lawd_code("화성") is None  # "화성시" 가 아닌 단일 "화성"
        assert find_lawd_code("없는지역") is None


class TestFanOutCodes:
    """부천/화성처럼 MOLIT 서브코드로 분리 반환되는 경우 (`find_lawd_codes`)."""

    def test_bucheon_fans_out(self):
        codes = find_lawd_codes("부천시 괴안동")
        assert set(codes) == {"41192", "41194", "41196"}

    def test_bucheon_explicit_subdistrict_no_fan(self):
        # 구를 명시하면 해당 옛 코드만 반환 (fan-out 안 됨)
        assert find_lawd_codes("경기 부천시 소사구") == ["41194"]

    def test_hwaseong_fans_out(self):
        codes = find_lawd_codes("경기 화성시 남양읍 시리")
        assert set(codes) == {"41591", "41593", "41595", "41597"}

    def test_gangwon_remapped_no_zombie(self):
        # 강원 좀비 42xxx → 51xxx 리매핑 확인
        assert find_lawd_codes("강원 춘천시") == ["51110"]
        assert find_lawd_codes("강원특별자치도 원주시") == ["51130"]

    def test_jeonbuk_remapped_no_zombie(self):
        # 전북 좀비 45xxx → 52xxx 리매핑 확인
        assert find_lawd_codes("전북 전주시 완산구") == ["52111"]
        assert find_lawd_codes("전북 익산시") == ["52140"]


class TestFanOutMapIntegrity:
    """FAN_OUT_CODES 자기일관성(self-consistency) 검사."""

    def test_parents_exist_in_main_map(self):
        # 모든 부모 코드는 LAWD_CODE_MAP 에도 있어야 (대표 입력 지원용).
        known = set(LAWD_CODE_MAP.values())
        for parent in FAN_OUT_CODES:
            assert parent in known, f"fan-out 부모코드 {parent} 가 LAWD_CODE_MAP 에 없음"

    def test_subs_have_valid_format(self):
        # 서브코드는 5자리 숫자 문자열이어야 함
        for parent, subs in FAN_OUT_CODES.items():
            assert subs, f"{parent} 의 fan-out 리스트가 비어 있음"
            for s in subs:
                assert isinstance(s, str) and len(s) == 5 and s.isdigit(), (
                    f"fan-out 서브코드 형식 오류: parent={parent}, sub={s!r}"
                )

    def test_bucheon_subs_are_named(self):
        # 부천 서브코드는 명시 입력(예: '부천시 소사구') 을 위해 LAWD_CODE_MAP 에
        # 개별 등록되어 있어야 한다. 화성은 공식 구명이 없으므로 해당 없음.
        known = set(LAWD_CODE_MAP.values())
        for s in FAN_OUT_CODES["41190"]:  # 부천
            assert s in known, f"부천 서브 {s} 가 LAWD_CODE_MAP 에 없음"


# ──────────────────────────────────────────────────────────
# 동·지번·단지명 추출
# ──────────────────────────────────────────────────────────


class TestExtractDong:
    def test_normal_dong(self):
        assert extract_dong("서울 강남구 역삼동 679-13") == "역삼동"

    def test_eup(self):
        assert extract_dong("경기도 화성시 남양읍 시리 250-1") == "남양읍"

    def test_ri_wins_over_myeon(self):
        # "○○면 ○○리" 주소는 좀더 세분화된 리를 반환
        assert extract_dong("경기도 양평군 지평면 일신리") == "일신리"

    def test_no_dong(self):
        assert extract_dong("서울 강남구") is None


class TestExtractJibun:
    def test_basic(self):
        assert extract_jibun("역삼동 679-13") == "679-13"

    def test_single_number(self):
        assert extract_jibun("역삼동 123") == "123"

    def test_san(self):
        assert extract_jibun("○○면 산 45-2") == "산45-2"
        assert extract_jibun("○○면 산45") == "산45"

    def test_area_not_confused_with_jibun(self):
        # 면적 표기(84.99㎡) 를 지번으로 오인하지 않는지
        assert extract_jibun("역삼동 래미안 84㎡") is None

    def test_no_jibun(self):
        assert extract_jibun("서울 강남구 역삼동") is None


class TestExtractBuildingName:
    def test_normal(self):
        assert (
            extract_building_name("서울 강남구 역삼동 679-13 래미안그레이튼")
            == "래미안그레이튼"
        )

    def test_sido_not_building(self):
        # "서울" 단독 토큰은 단지명이 될 수 없음
        assert extract_building_name("서울 강남구") is None

    def test_none_when_only_address(self):
        assert extract_building_name("서울 강남구 역삼동 679-13") is None


# ──────────────────────────────────────────────────────────
# 정규화
# ──────────────────────────────────────────────────────────


class TestNormalize:
    def test_sido_abbrev(self):
        assert normalize_address("서울특별시 강남구") == "서울 강남구"
        assert normalize_address("경기도 용인시 수지구") == "경기 용인 수지구"

    def test_extra_spaces(self):
        assert normalize_address("서울   강남구   역삼동") == "서울 강남구 역삼동"
