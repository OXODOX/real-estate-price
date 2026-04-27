"""건축물대장 표제부 벌크파일(TXT) → SQLite 변환 스크립트.

사용 흐름
--------
1. 건축데이터 민간개방 시스템(https://open.eais.go.kr)에서
   - "대용량 제공 서비스 → 건축물대장 표제부(mart_djy_03)" (현행)
   - "건축물대장 폐쇄/말소 표제부(mart_shtreg_03)" (철거·멸실) ← --closed 로 임포트
   - 파일 형식: `|` 구분자 TXT, CP949 인코딩, 헤더 없음.
2. 이 스크립트로 SQLite DB(backend/.cache/bldg.db)로 변환.
3. 서버 재시작 시 bldg_registry가 SQLite를 우선 조회.

두 파일의 관계
--------------
폐쇄말소대장(mart_shtreg_03)은 현행(mart_djy_03)과 컬럼이 **+3 시프트**로 일치하며,
선두에 관리번호·말소구분·말소일자 3 컬럼이 추가돼 있다. 스크립트는 --closed 옵션으로
이 시프트된 인덱스를 사용하고, 테이블의 status='closed', demolish_day 를 채운다.

현행 import 는 DB를 삭제 후 새로 만들고,
폐쇄 import 는 기존 DB 에 append 만 한다 (스냅샷 병합).

사용 예시
--------
    # 1) 현행 표제부 (DB 재생성)
    python scripts/import_bldg_titles.py path/to/mart_djy_03.txt

    # 2) 폐쇄말소 표제부 (현행 위에 append)
    python scripts/import_bldg_titles.py --closed path/to/mart_shtreg_03.txt

    # 3) 구조 확인만
    python scripts/import_bldg_titles.py --inspect path/to/any.txt
"""
from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / ".cache" / "bldg.db"

# 현행 표제부(mart_djy_03) 공식 컬럼 인덱스.
COLS_ACTIVE = {
    "sigungu_cd": 8,
    "bjdong_cd": 9,
    "bun": 11,
    "ji": 12,
    "bld_nm": 7,
    "plat_area": 25,
    "arch_area": 26,
    "tot_area": 28,
    "main_purps_nm": 35,
    "use_apr_day": 60,
    "demolish_day": None,  # 현행 레코드는 말소일자 없음
}

# 폐쇄말소 표제부(mart_shtreg_03): 현행 대비 +3 시프트 + 말소일자 at [3].
COLS_CLOSED = {
    "sigungu_cd": 11,
    "bjdong_cd": 12,
    "bun": 14,
    "ji": 15,
    "bld_nm": 10,
    "plat_area": 28,
    "arch_area": 29,
    "tot_area": 31,
    "main_purps_nm": 38,
    "use_apr_day": 63,
    "demolish_day": 3,   # 말소일자 (YYYYMMDD)
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS buildings (
    sigungu_cd    TEXT NOT NULL,
    bjdong_cd     TEXT NOT NULL,
    bun           TEXT NOT NULL,
    ji            TEXT NOT NULL,
    bld_nm        TEXT,
    plat_area     REAL,
    arch_area     REAL,
    tot_area      REAL,
    main_purps_nm TEXT,
    use_apr_day   TEXT,
    status        TEXT NOT NULL DEFAULT 'active',   -- 'active' | 'closed'
    demolish_day  TEXT                               -- 'closed' 행만 값 존재
);
CREATE INDEX IF NOT EXISTS idx_loc ON buildings(sigungu_cd, bjdong_cd);
"""


def _parse_float(x: str) -> float | None:
    x = (x or "").strip()
    if not x:
        return None
    try:
        return float(x)
    except ValueError:
        return None


def _detect_encoding(path: Path) -> str:
    """파일 선두를 UTF-8 으로 디코딩해보고 성공하면 utf-8, 실패하면 cp949.

    국토교통부 벌크파일은 시기에 따라 인코딩이 다를 수 있어
    (구판: cp949, 최근: utf-8) 자동 판별이 필요.
    """
    with open(path, "rb") as f:
        head = f.read(4096)
    try:
        head.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "cp949"


def _iter_rows(path: Path, cols: dict, status: str):
    """| 구분 텍스트 파일을 한 줄씩 파싱 (UTF-8/CP949 자동 판별)."""
    max_idx = max(v for v in cols.values() if v is not None)
    demolish_idx = cols.get("demolish_day")
    enc = _detect_encoding(path)
    with open(path, "r", encoding=enc, errors="replace") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < max_idx + 1:
                continue
            try:
                demolish_day = None
                if demolish_idx is not None:
                    demolish_day = parts[demolish_idx].strip() or None
                yield (
                    parts[cols["sigungu_cd"]].strip(),
                    parts[cols["bjdong_cd"]].strip(),
                    parts[cols["bun"]].strip().zfill(4),
                    parts[cols["ji"]].strip().zfill(4),
                    parts[cols["bld_nm"]].strip(),
                    _parse_float(parts[cols["plat_area"]]),
                    _parse_float(parts[cols["arch_area"]]),
                    _parse_float(parts[cols["tot_area"]]),
                    parts[cols["main_purps_nm"]].strip(),
                    parts[cols["use_apr_day"]].strip(),
                    status,
                    demolish_day,
                )
            except IndexError:
                continue


def inspect(path: Path, cols: dict, label: str) -> None:
    """파일 상단 2행을 출력해 컬럼 인덱스 검증."""
    print(f"=== INSPECT [{label}]: {path} ===")
    enc = _detect_encoding(path)
    print(f"  (encoding: {enc})")
    with open(path, "r", encoding=enc, errors="replace") as f:
        for i, line in enumerate(f):
            if i >= 2:
                break
            parts = line.rstrip("\r\n").split("|")
            print(f"--- row {i} (total {len(parts)} columns) ---")
            for j, p in enumerate(parts):
                marker = ""
                for name, idx in cols.items():
                    if idx == j:
                        marker = f"  <-- {name}"
                        break
                print(f"  [{j:3d}] {p[:40]!r}{marker}")
    print("\n위 마킹이 실제 필드와 맞는지 확인. 안 맞으면 COLS 인덱스 조정.\n")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """테이블이 구 스키마면(status/demolish_day 없음) 확장해 재임포트 안전하게."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(buildings)").fetchall()]
    if not cols:
        conn.executescript(SCHEMA)
        return
    if "status" not in cols:
        conn.execute("ALTER TABLE buildings ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
    if "demolish_day" not in cols:
        conn.execute("ALTER TABLE buildings ADD COLUMN demolish_day TEXT")


def import_files(paths: list[Path], closed: bool) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    if not closed:
        # 현행 import: buildings 테이블만 드롭하고 parcels/parcels_history 는 보존.
        # (옛 동작은 DB 파일 자체를 unlink 했으나, 같은 DB 안의 다른 테이블까지
        #  날아가서 재구축에 시간이 너무 오래 걸림.)
        conn.execute("DROP TABLE IF EXISTS buildings")
        conn.commit()
    _ensure_schema(conn)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")

    cols = COLS_CLOSED if closed else COLS_ACTIVE
    status = "closed" if closed else "active"

    if closed:
        # 재실행 시 중복 방지: 기존 closed 행 삭제 후 재삽입
        conn.execute("DELETE FROM buildings WHERE status='closed'")
        conn.commit()

    total = 0
    batch: list[tuple] = []
    BATCH = 10_000
    t0 = time.time()
    insert_sql = (
        "INSERT INTO buildings "
        "(sigungu_cd, bjdong_cd, bun, ji, bld_nm, plat_area, arch_area, tot_area, "
        "main_purps_nm, use_apr_day, status, demolish_day) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
    )
    for path in paths:
        print(f"→ {path}  (status={status})")
        for row in _iter_rows(path, cols, status):
            batch.append(row)
            if len(batch) >= BATCH:
                conn.executemany(insert_sql, batch)
                total += len(batch)
                batch.clear()
                if total % 500_000 == 0:
                    elapsed = time.time() - t0
                    print(f"  inserted {total:,} rows ({elapsed:.1f}s)")
    if batch:
        conn.executemany(insert_sql, batch)
        total += len(batch)

    conn.commit()
    print(f"\n✔ {total:,} rows imported ({status}) in {time.time()-t0:.1f}s")
    # 전체 현황 출력
    for st, cnt in conn.execute("SELECT status, COUNT(*) FROM buildings GROUP BY status"):
        print(f"  {st}: {cnt:,} rows")
    print(f"  DB: {DB_PATH}  ({DB_PATH.stat().st_size / (1024*1024):.1f} MB)")
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+", help="표제부 TXT 파일(들). glob 패턴 가능.")
    ap.add_argument("--inspect", action="store_true", help="첫 2행만 출력 후 종료")
    ap.add_argument("--closed", action="store_true",
                    help="폐쇄말소 대장(mart_shtreg_03)으로 파싱. DB에 append 됨 (status='closed').")
    args = ap.parse_args()

    cols = COLS_CLOSED if args.closed else COLS_ACTIVE
    label = "closed" if args.closed else "active"

    # glob 확장
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
        inspect(files[0], cols, label)
        return

    import_files(files, closed=args.closed)


if __name__ == "__main__":
    main()
