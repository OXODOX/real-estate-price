# 테스트 안내

## 빠른 실행

```bash
cd backend
pytest                      # fast 테스트만 (기본. 1초 이내)
pytest -m slow              # 느린(네트워크) 테스트만
pytest -m ""                # 전부 (fast + slow)
pytest -v                   # 자세한 로그
pytest tests/test_xxx.py    # 특정 파일만
```

## 테스트 종류

| 마커 | 파일 | 설명 | 속도 |
|---|---|---|---|
| `fast` | `test_address_lookup.py` | 주소 파싱·법정동코드 매핑 등 순수 로직 | < 1초 |
| `slow`, `network` | `test_estimate_integration.py` | 실제 MOLIT/JUSO API 호출 | 수 초~수십 초 |

`slow` 는 공공 API 쿼터를 소비하므로 자주 돌리지 말고, 로직 변경 후
회귀(regression) 점검이 필요할 때만 실행하세요.

## 테스트 추가 요령

새 버그를 고치면 그 버그를 재현하는 케이스를 `fast` 테스트로 남겨두세요.
다음에 다른 수정을 하다가 실수로 같은 버그를 되살리면 `pytest` 가 즉시
알려줍니다.
