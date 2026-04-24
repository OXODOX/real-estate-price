# 건축물대장 벌크 데이터 운영 가이드

마스킹된 지번(`1**`, `7**` 등) 복원 속도를 높이기 위해, 매월 공공데이터포털에서 제공되는 전국 건축물대장 표제부 파일을 SQLite로 변환해 쓴다.

## 1. 파일 다운로드

- 사이트: 건축데이터 민간개방 시스템 — https://open.eais.go.kr
- 경로: **대용량 제공 서비스 → 건축물대장 표제부 (`mart_djy_03`)**
- 형식: `|` 구분자 TXT, **CP949** 인코딩, 헤더 없음
- 크기: 전국 비압축 약 3~5GB (약 786만 행)
- 갱신: 매월 전체 스냅샷
- 회원가입 필요 (API 키는 불필요)

전국 한 번에 받기 부담되면 **시도별 분할본**을 받아 여러 파일을 한 번에 import해도 된다.

## 2. 컬럼 위치 검증

파일 컬럼 순서가 공식 스펙과 다를 수 있으니 첫 실행 전 반드시 확인:

```bash
python scripts/import_bldg_titles.py --inspect path/to/mart_djy_03.txt
```

출력된 각 컬럼 옆 `<-- sigungu_cd` 같은 마킹이 실제 필드와 일치하는지 확인. 불일치 시 `scripts/import_bldg_titles.py` 상단의 `COLS` 딕셔너리 인덱스를 수정.

## 3. SQLite로 import

```bash
# 단일 파일
python scripts/import_bldg_titles.py path/to/mart_djy_03.txt

# 시도별 분할 여러 개
python scripts/import_bldg_titles.py "data/mart_djy_03_*.txt"
```

- 결과: `backend/.cache/bldg.db` (약 500MB~1GB, 행 수에 따라 다름)
- 기존 DB는 자동 삭제 후 재생성 (월 1회 전체 스냅샷 특성 반영)

## 4. 서버 재시작

`bldg_registry.py`가 `.cache/bldg.db` 존재 시 자동으로 우선 조회한다. 조회 우선순위:

1. 메모리 캐시
2. **SQLite (있으면)** — 즉시 응답
3. 기존 JSON 디스크 캐시 (API로 받아뒀던 것)
4. 공공데이터포털 API — 쿼터/지연 발생, 최후 수단

따라서 SQLite가 준비된 후에는 API 호출이 사실상 사라지고, 마스킹된 지번 조회가 1초 미만으로 끝난다.

## 5. 월간 갱신 루틴

1. `open.eais.go.kr`에서 새 `mart_djy_03.txt` 다운로드
2. `python scripts/import_bldg_titles.py <새파일>` 재실행 (DB 자동 덮어쓰기)
3. 서버 재시작 (프로세스 메모리 캐시만 초기화되면 되므로)

## 토지 마스킹에 대해

건축물대장은 "건물이 있는 필지"만 수록한다. 나대지(전/답/임야) 거래 마스킹 복원은 별도로 **개별공시지가 정보** 파일이 필요하며, 이는 추후 작업.
