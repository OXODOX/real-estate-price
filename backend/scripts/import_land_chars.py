"""토지특성정보(AL_D195) 시도별 CSV → SQLite 변환 스크립트.

사용 흐름
--------
1. 브이월드 오픈마켓(https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?svcCde=NA&dsId=30)
   에서 "토지특성정보" (AL_D195) 시도별 CSV 다운로드.
   - 형식: CP949, 콤마 구분, 헤더 있음, 26 컬럼
   - 기준일 예: AL_D195_11_20260402.csv (서울특별시)
2. 본 스크립트로 SQLite `parcels` 테이블 생성 (backend/.cache/bldg.db 공용).
3. 서버 재시작 시 bldg_registry 가 토지 거래의 마스킹 복원에 사용.

컬럼 인덱스 검증이 필요하면 --inspect 로 상위 2행 출력 후 종료.

사용 예시
--------
    # 1) 전체 시도 일괄 임포트 (DB parcels 테이블 재생성)
    python scripts/import_land_chars.py "C:/Users/User/Desktop/토지특성정보/AL_D195_*_*/*.csv"

    # 2) 구조 확인만
    python scripts/import_land_chars.py --inspect path/to/AL_D195_11_*.csv
"""
from __future__ import annotations

import argparse
import csv
import glob
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / ".cache" / "bldg.db"

# AL_D195 CSV 0-base 컬럼 인덱스 (컬럼정의서 AL_D195 기준)
COLS = {
    "pnu": 0,            # 고유번호 19자리: sigunguCd(5)+bjdongCd(5)+특수지(1)+본번(4)+부번(4)
    "bjdong10": 1,       # 법정동코드 10자리
    "ledger_cd": 3,      # 대장구분코드 (1=일반, 2=산)
    "jibun_disp": 5,     # 지번 표시용 ("1-1" 등)
    "jimok_cd": 9,
    "jimok_nm": 10,
    "land_area": 11,     # 토지면적 (㎡, 소수)
    "land_use_cd1": 12,
    "land_use_nm1": 13,
    "usage_cd": 16,      # 토지이용상황코드
    "usage_nm": 17,
    "price": 24,         # 공시지가 (원/㎡)
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS parcels (
    sigungu_cd   TEXT NOT NULL,
    bjdong_cd    TEXT NOT NULL,
    bun          TEXT NOT NULL,
    ji           TEXT NOT NULL,
    sanji        TEXT NOT NULL,     -- '1'=일반 '2'=산 '3'=가지번 …
    jimok_cd     TEXT,
    jimok_nm     TEXT,
    land_area    REAL,
    land_use     TEXT,              -- 용도지역명1 (제1종일반주거 등)
    usage_nm     TEXT,              -- 토지이용상황
    price        INTEGER            -- 공시지가(원/㎡)
);
CREATE INDEX IF NOT EXISTS idx_parcel_loc ON parcels(sigungu_cd, bjdong_cd);
"""


def _parse_float(x: str) -> float | None:
    x = (x or "").strip()
    if not x:
        return None
    try:
        return float(x)
    except ValueError:
        return None


def _parse_int(x: str) -> int | None:
    x = (x or "").strip()
    if not x:
        return None
    try:
        return int(x)
    except ValueError:
        return None


def _split_pnu(pnu: str) -> tuple[str, str, str, str, str]:
    """19자리 PNU → (sigunguCd5, bjdongCd5, sanji1, bun4, ji4)."""
    p = (pnu or "").strip()
    if len(p) < 19:
        return "", "", "", "", ""
    return p[:5], p[5:10], p[10:11], p[11:15], p[15:19]


def _iter_rows(path: Path):
    with open(path, "r", encoding="cp949", errors="replace", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # 헤더 스킵
        for row in reader:
            if len(row) < 26:
                continue
            try:
                sgg, bjd, sanji, bun, ji = _split_pnu(row[COLS["pnu"]])
                if not sgg or not bjd:
                    continue
                yield (
                    sgg,
                    bjd,
                    bun,
                    ji,
                    sanji,
                    row[COLS["jimok_cd"]].strip(),
                    row[COLS["jimok_nm"]].strip(),
                    _parse_float(row[COLS["land_area"]]),
                    row[COLS["land_use_nm1"]].strip(),
                    row[COLS["usage_nm"]].strip(),
                    _parse_int(row[COLS["price"]]),
                )
            except IndexError:
                continue


def inspect(path: Path) -> None:
    print(f"=== INSPECT: {path} ===")
    with open(path, "r", encoding="cp949", errors="replace") as f:
        for i, line in enumerate(f):
            if i >= 2:
                break
            parts = line.rstrip("\r\n").split(",")
            print(f"--- row {i} (total {len(parts)} columns) ---")
            for j, p in enumerate(parts):
                marker = ""
                for name, idx in COLS.items():
                    if idx == j:
                        marker = f"  <-- {name}"
                        break
                print(f"  [{j:2d}] {p[:40]!r}{marker}")
    print("\n위 마킹이 실제 필드와 맞는지 확인. 안 맞으면 COLS 인덱스 조정.\n")


def import_files(paths: list[Path]) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    # 재실행 시 기존 parcels 전체 재생성 (건축물 테이블은 건드리지 않음)
    conn.execute("DROP TABLE IF EXISTS parcels")
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")

    total = 0
    batch: list[tuple] = []
    BATCH = 10_000
    t0 = time.time()
    sql = (
        "INSERT INTO parcels "
        "(sigungu_cd, bjdong_cd, bun, ji, sanji, jimok_cd, jimok_nm, "
        "land_area, land_use, usage_nm, price) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    )
    for path in paths:
        print(f"→ {path}")
        for row in _iter_rows(path):
            batch.append(row)
            if len(batch) >= BATCH:
                conn.executemany(sql, batch)
                total += len(batch)
                batch.clear()
                if total % 500_000 == 0:
                    print(f"  inserted {total:,} rows ({time.time()-t0:.1f}s)")
    if batch:
        conn.executemany(sql, batch)
        total += len(batch)

    conn.commit()
    print(f"\n✔ {total:,} parcels imported in {time.time()-t0:.1f}s")
    print(f"  DB: {DB_PATH} ({DB_PATH.stat().st_size/(1024*1024):.1f} MB)")
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+", help="AL_D195 CSV 파일(들). glob 가능.")
    ap.add_argument("--inspect", action="store_true", help="첫 2행만 출력 후 종료")
    args = ap.parse_args()

    files: list[Path] = []
    for p in args.paths:
        expanded = glob.glob(p)
        if not expanded:
            print(f"⚠ 파일 없음: {p}", file=sys.stderr)
            continue
        files.extend(Path(x) for x in expanded)
    if not files:
        sys.exit(1)

    if args.inspect:
        inspect(files[0])
        return

    import_files(files)


if __name__ == "__main__":
    main()
