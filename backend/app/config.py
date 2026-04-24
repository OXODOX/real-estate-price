from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # 공공데이터포털 API 키
    DATA_GO_KR_API_KEY: str = ""

    # 카카오 지도 API 키 (지오코딩용)
    KAKAO_REST_API_KEY: str = ""

    # 행정안전부 도로명주소 검색 API 키
    JUSO_API_KEY: str = ""

    # 공공데이터포털 건축물대장정보 서비스 키 (마스킹된 지번 추정용)
    BLDG_REG_API_KEY: str = ""

    # VWorld 인증키 (법정동코드 조회용)
    VWORLD_API_KEY: str = ""

    # API 기본 설정
    MOLIT_BASE_URL: str = "https://apis.data.go.kr/1613000"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
