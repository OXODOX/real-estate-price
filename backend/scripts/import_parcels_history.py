"""토지이동이력(AL_D157) 시도별 CSV → SQLite 변환 스크립트.

목적
----
거래 시점의 면적·지목으로 정확 매칭하기 위한 시계열 토지대장 데이터.
분필·합필·면적정정·지목변경·행정관할변경 등 모든 변동 이력을 보존하므로
거래 deal_ymd 시점에 유효했던 필지 상태를 역추적할 수 있음.

사용 흐름
--------
1. 국가공간정보포털에서 AL_D157 시도별 CSV 다운로드 후 압축해제.
   - 형식: CP949, 콤마 구분, 헤더 있음, 18 컬럼
   - 파일명 예: AL_D157_41_20260331.csv (경기도, 기준일 2026-03-31)
2. 본 스크립트로 SQLite `parcels_history` 테이블 생성/갱신 (bldg.db 공용).
3. bldg_registry 가 토지 거래 매칭 시 시점 필터로 사용.

사용 예시
--------
    # 전체 시도 일괄 임포트 (parcels_history 재생성)
    python scripts/import_parcels_history.py "C:/Users/User/Desktop/토지이동이력정보/AL_D157_*_*/*.csv"

    # 구조 확인만
    python scripts/import_parcels_history.py --inspect path/to/AL_D157_43_*.csv
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

# AL_D157 CSV 0-base 컬럼 인덱스
COLS = {
    "pnu": 0,            # 고유번호 19자리
    "bjdong10": 1,       # 법정동코드 10자리
    "bjdong_nm": 2,      # 법정동명
    "ledger_cd": 3,      # 대장구분코드 (1=토지대장, 2=임야대장)
    "ledger_nm": 4,      # 대장구분명
    "jibun_disp": 5,     # 지번 표시
    "move_seq": 6,       # 토지이동이력순번
    "closed_seq": 7,     # 폐쇄순번 (000=활성, 그 외=폐쇄)
    "jimok_cd": 8,
    "jimok_nm": 9,
    "land_area": 10,     # 토지면적 (㎡)
    "reason_cd": 11,
    "reason_nm": 12,     # 토지이동사유
    "start_day": 13,     # 토지이동일자 YYYY-MM-DD
    "end_day": 14,       # 토지이동말소일자 YYYY-MM-DD or 빈값
    "hist_seq": 15,      # 토지이력순번 (PNU 내 순서)
    "data_day": 16,
    "src_sgg_cd": 17,
}

# CSV 변환 csv 모듈 필드 크기 한계 상향 (한국어 긴 사유 대응)
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def _ymd(s: str) -> str:
    """'YYYY-MM-DD' → 'YYYYMMDD'. 빈값/9999 처리."""
    s = (s or "").strip()
    if not s:
        return ""
    return s.replace("-", "")


def _zero_pad(s: str, width: int) -> str:
    s = (s or "").strip()
    if not s.isdigit():
        return s.zfill(width)
    return s.zfill(width)


def _parse_pnu(pnu: str) -> tuple[str, str, str, str, str] | None:
    """PNU 19자리 → (sigungu_cd, bjdong_cd, sanji, bun, ji)."""
    if not pnu or len(pnu) != 19 or not pnu.isdigit():
        return None
    return (pnu[0:5], pnu[5:10], pnu[10:11], pnu[11:15], pnu[15:19])


def _create_schema(conn: sqlite3.Connection) -> None:
    """공간 효율화 스키마.

    PNU/이력순번 같은 식별자는 (시군구+법정동+산여부+본번+부번+seq) 로 대체.
    긴 한글 텍스트(대장구분명, 사유명, 폐쇄순번 텍스트)는 제거하고
    매칭에 실제로 필요한 정보만 보존. 사유는 '지적재조사' 여부만 1바이트
    플래그로 보존(False positive 차단 로직에 사용).
    """
    conn.executescript(
        """
        DROP TABLE IF EXISTS parcels_history;
        CREATE TABLE parcels_history (
            sigungu_cd TEXT NOT NULL,    -- 5자리
            bjdong_cd  TEXT NOT NULL,    -- 5자리
            sanji      TEXT NOT NULL,    -- '1'=일반, '2'=산
            bun        TEXT NOT NULL,    -- 4자리 zero-pad
            ji         TEXT NOT NULL,    -- 4자리 zero-pad
            seq        INTEGER NOT NULL, -- 토지이력순번 (PNU 내 순서)
            jimok_nm   TEXT,             -- 짧은 한글 (대/임야/전 등)
            land_area  REAL,
            start_day  TEXT NOT NULL,    -- YYYYMMDD
            end_day    TEXT,             -- YYYYMMDD 또는 '' = 현재 유효
            is_jjjs    INTEGER NOT NULL, -- 지적재조사 이력 여부 (1/0)
            PRIMARY KEY (sigungu_cd, bjdong_cd, sanji, bun, ji, seq)
        ) WITHOUT ROWID;
        """
    )
    # 시점 필터링용 인덱스: (시군구+법정동+본번+산여부) 로 후보 추리고
    # start_day 로 시점 비교 효율화.
    conn.executescript(
        """
        CREATE INDEX idx_parcels_history_loc
          ON parcels_history (sigungu_cd, bjdong_cd, bun, sanji, start_day);
        """
    )


def _import_csv(conn: sqlite3.Connection, path: Path, *, batch: int = 50_000) -> int:
    """CSV 1개 파일을 parcels_history 테이블에 적재. 반환: 적재된 행수."""
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    buf: list[tuple] = []
    t0 = time.time()
    with open(path, "r", encoding="cp949", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None or len(header) < 18:
            print(f"  ! 헤더 이상: {path.name}")
            return 0
        for row in reader:
            if len(row) < 18:
                skipped += 1
                continue
            pnu = row[COLS["pnu"]].strip()
            parsed = _parse_pnu(pnu)
            if not parsed:
                skipped += 1
                continue
            sigungu, bjdong, sanji, bun, ji = parsed
            try:
                area = float(row[COLS["land_area"]] or 0)
            except ValueError:
                area = 0.0
            try:
                seq = int(row[COLS["hist_seq"]] or 0)
            except ValueError:
                seq = 0
            reason = row[COLS["reason_nm"]] or ""
            is_jjjs = 1 if "지적재조사" in reason else 0
            buf.append((
                sigungu, bjdong, sanji, bun, ji, seq,
                (row[COLS["jimok_nm"]] or "").strip(),
                area,
                _ymd(row[COLS["start_day"]]),
                _ymd(row[COLS["end_day"]]),
                is_jjjs,
            ))
            if len(buf) >= batch:
                cur.executemany(
                    "INSERT OR REPLACE INTO parcels_history "
                    "(sigungu_cd, bjdong_cd, sanji, bun, ji, seq, "
                    " jimok_nm, land_area, start_day, end_day, is_jjjs) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    buf,
                )
                inserted += len(buf)
                buf.clear()
                conn.commit()
        if buf:
            cur.executemany(
                "INSERT OR REPLACE INTO parcels_history "
                "(sigungu_cd, bjdong_cd, sanji, bun, ji, seq, "
                " jimok_nm, land_area, start_day, end_day, is_jjjs) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                buf,
            )
            inserted += len(buf)
            buf.clear()
            conn.commit()
    elapsed = time.time() - t0
    print(f"  ✓ {path.name}: {inserted:,} rows ({elapsed:.1f}s, skipped={skipped})")
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("pattern", help="CSV glob pattern (예: ...AL_D157_*.csv)")
    parser.add_argument("--inspect", action="store_true", help="첫 2행만 확인하고 종료")
    parser.add_argument("--db", default=str(DB_PATH), help="대상 SQLite 경로")
    args = parser.parse_args()

    files = sorted(glob.glob(args.pattern))
    if not files:
        print(f"매칭 파일 없음: {args.pattern}")
        return 1

    if args.inspect:
        for p in files[:1]:
            with open(p, "r", encoding="cp949") as f:
                for i, line in enumerate(f):
                    print(line.rstrip())
                    if i >= 2:
                        break
        return 0

    db = Path(args.db)
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    # 빠른 일괄 적재 모드
    conn.executescript(
        "PRAGMA journal_mode=WAL; "
        "PRAGMA synchronous=NORMAL; "
        "PRAGMA temp_store=MEMORY; "
        "PRAGMA cache_size=-200000;"  # ~200MB cache
    )
    print(f"DB: {db}")
    print(f"입력 파일 {len(files)}개")
    _create_schema(conn)
    total = 0
    t0 = time.time()
    for p in files:
        total += _import_csv(conn, Path(p))
    print(f"\n총 {total:,} rows imported in {time.time()-t0:.1f}s")
    print("ANALYZE 실행 중...")
    conn.execute("ANALYZE parcels_history")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
