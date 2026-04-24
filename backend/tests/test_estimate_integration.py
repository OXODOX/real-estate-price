"""실제 MOLIT API 를 호출하는 통합(integration) 테스트.

- `slow` 마커 → 기본 실행에서 제외. 실행하려면:
    pytest -m slow                 # slow 만
    pytest -m "fast or slow"       # 전부
- 이 테스트들은 공공 API 쿼터를 소비하므로 자주 돌리지 말 것.
- 과거 버그에 대한 회귀 보호가 목적. 예:
    * 부천/화성 fan-out 이 데이터를 합쳐서 돌려주는가
    * 강원/전북 좀비 리매핑이 실제 데이터까지 가져오는가
    * 양천구 신정동 980-8 같은 마스킹 복원이 동작하는가
"""
import pytest
from httpx import AsyncClient, ASGITransport

from app.main import app

pytestmark = [pytest.mark.slow, pytest.mark.network]


@pytest.fixture
async def client():
    """FastAPI 앱에 직접 붙는 테스트 클라이언트 (HTTP 서버 안 띄움)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _estimate(client: AsyncClient, **payload):
    r = await client.post("/api/v1/estimate", json=payload, timeout=120.0)
    assert r.status_code == 200, f"status={r.status_code} body={r.text[:400]}"
    return r.json()


class TestHealth:
    async def test_health_ok(self, client: AsyncClient):
        r = await client.get("/api/v1/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


class TestEstimateBasic:
    async def test_seoul_apt_returns_data(self, client: AsyncClient):
        data = await _estimate(
            client,
            address="서울 강남구 역삼동",
            property_type="아파트",
            months_back=3,
        )
        total = len(data["recent_transactions"]) + len(data["nearby_transactions"])
        assert total > 0, "서울 강남구 역삼동 아파트는 3개월 내 거래가 있어야 함"


class TestFanOut:
    async def test_bucheon_fanout_returns_data(self, client: AsyncClient):
        """부천시 41190 은 MOLIT 가 빈 데이터 → 41192/41194/41196 fan-out 후 합쳐져야."""
        data = await _estimate(
            client, address="부천시 괴안동", property_type="아파트", months_back=3
        )
        total = len(data["recent_transactions"]) + len(data["nearby_transactions"])
        assert total > 0, "부천시 괴안동 아파트가 3개월 내에 있어야 (fan-out 정상)"

    async def test_hwaseong_fanout_returns_data(self, client: AsyncClient):
        """화성시 41590 fan-out 4개 서브코드 병합 검증."""
        data = await _estimate(
            client, address="경기 화성시", property_type="아파트", months_back=3
        )
        total = len(data["recent_transactions"]) + len(data["nearby_transactions"])
        assert total > 0


class TestRemappedRegions:
    """강원/전북 좀비 코드 리매핑 회귀 보호."""

    async def test_gangwon_chuncheon(self, client: AsyncClient):
        data = await _estimate(
            client, address="강원 춘천시", property_type="아파트", months_back=3
        )
        total = len(data["recent_transactions"]) + len(data["nearby_transactions"])
        assert total > 0

    async def test_jeonbuk_iksan(self, client: AsyncClient):
        data = await _estimate(
            client, address="전북 익산시", property_type="아파트", months_back=3
        )
        total = len(data["recent_transactions"]) + len(data["nearby_transactions"])
        assert total > 0


class TestErrorResponses:
    async def test_bad_address(self, client: AsyncClient):
        r = await client.post(
            "/api/v1/estimate",
            json={"address": "외계어외계어외계어", "property_type": "아파트"},
        )
        assert r.status_code == 400
        body = r.json()
        assert "detail" in body
