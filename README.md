# 부동산 가격 산정 서비스

국토교통부 실거래가 API를 활용한 부동산 가격 산정 웹 서비스입니다.

## 실행 방법

### Windows (간편)
`run-server.bat` 파일을 더블클릭

### 수동 실행
```bash
cd backend
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

서버가 기동되면 브라우저에서 **http://127.0.0.1:8000** 접속

## 프로젝트 구조

```
real-estate-price/
├── run-server.bat         # 원클릭 실행 스크립트 (Windows)
├── README.md
└── backend/
    ├── .env               # API 키 (버전관리 제외)
    ├── .env.example
    ├── requirements.txt
    ├── static/
    │   └── index.html     # 웹 화면 (프로토타입)
    └── app/
        ├── main.py        # FastAPI 진입점
        ├── config.py      # 환경변수 관리
        ├── routers/
        │   └── estimate.py        # POST /api/v1/estimate
        ├── services/
        │   ├── molit_api.py       # 국토교통부 API 연동
        │   ├── price_estimator.py # 가격 산정 로직
        │   └── address_lookup.py  # 주소 → 법정동코드
        └── models/
            └── schemas.py # 데이터 모델
```

## 지원 부동산 유형 (매매)

- 아파트
- 연립/다세대 (빌라)
- 오피스텔
- 단독/다가구
- 토지
- 상업업무용
- 아파트 분양권전매
- 공장/창고

## API 엔드포인트

- `GET /` — 웹 화면
- `POST /api/v1/estimate` — 가격 산정
- `GET /api/v1/health` — 헬스체크
- `GET /docs` — 자동 생성된 API 문서 (Swagger UI)

## 데이터 출처

국토교통부 실거래가 공개시스템 (공공데이터포털, data.go.kr)
