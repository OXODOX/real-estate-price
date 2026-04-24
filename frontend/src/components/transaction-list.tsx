"use client";

import { useMemo, useState } from "react";
import type { Transaction, TransactionResult } from "@/lib/api";

const PY_PER_M2 = 3.3058;
type Unit = "m2" | "py";

interface Props {
  data: TransactionResult;
  selectedKey: string | null;
  onSelect: (tx: Transaction, key: string) => void;
}

export function TransactionList({ data, selectedKey, onSelect }: Props) {
  const [unit, setUnit] = useState<Unit>("m2");
  const [pageSize, setPageSize] = useState(10);
  const [page, setPage] = useState(1);
  const [showNearby, setShowNearby] = useState(false);
  const [nearbyPageSize, setNearbyPageSize] = useState(10);
  const [nearbyPage, setNearbyPage] = useState(1);

  const recent = data.recent_transactions;
  const nearby = data.nearby_transactions;

  const totalPages = Math.max(1, Math.ceil(recent.length / pageSize));
  const pageItems = recent.slice((page - 1) * pageSize, page * pageSize);

  const nearbyTotalPages = Math.max(1, Math.ceil(nearby.length / nearbyPageSize));
  const nearbyItems = nearby.slice(
    (nearbyPage - 1) * nearbyPageSize,
    nearbyPage * nearbyPageSize,
  );

  return (
    <>
      {/* ─── 주의/폴백 배너 ─── */}
      <FallbackBanners data={data} />

      {/* ─── 결과 헤더 ─── */}
      <div className="result-header">
        <div className="title">
          <strong>{data.address}</strong>
          <span>{data.property_type}</span>
        </div>
        <AverageBox transactions={recent} unit={unit} />
      </div>

      {/* ─── 주요 거래 ─── */}
      <div className="trans-header">
        <div className="trans-title">근거 거래 (최신순)</div>
        <div className="trans-controls">
          <label>단위:</label>
          <select
            value={unit}
            onChange={(e) => setUnit(e.target.value as Unit)}
          >
            <option value="m2">㎡</option>
            <option value="py">평</option>
          </select>
          <label>표시 개수:</label>
          <select
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setPage(1);
            }}
          >
            <option value={10}>10개</option>
            <option value={20}>20개</option>
            <option value={50}>50개</option>
            <option value={100}>100개</option>
          </select>
        </div>
      </div>

      {recent.length === 0 ? (
        <EmptyState />
      ) : (
        <>
          <TxTable
            items={pageItems}
            unit={unit}
            selectedKey={selectedKey}
            onSelect={onSelect}
            keyPrefix="r"
          />
          <Pagination
            page={page}
            totalPages={totalPages}
            total={recent.length}
            onChange={setPage}
          />
        </>
      )}

      {/* ─── 인근 거래 토글 ─── */}
      {nearby.length > 0 && (
        <div style={{ marginTop: 16 }}>
          {!showNearby ? (
            <div style={{ textAlign: "center" }}>
              <button
                type="button"
                className="nearby-btn"
                onClick={() => setShowNearby(true)}
              >
                인근 거래 보기 (같은 동, {nearby.length}건)
              </button>
            </div>
          ) : (
            <div style={{ marginTop: 12 }}>
              <div className="trans-header">
                <div className="trans-title">
                  인근 참고 거래 ({nearby.length}건)
                </div>
                <div className="trans-controls">
                  <label>표시 개수:</label>
                  <select
                    value={nearbyPageSize}
                    onChange={(e) => {
                      setNearbyPageSize(Number(e.target.value));
                      setNearbyPage(1);
                    }}
                  >
                    <option value={10}>10개</option>
                    <option value={20}>20개</option>
                    <option value={50}>50개</option>
                    <option value={100}>100개</option>
                  </select>
                  <button
                    type="button"
                    className="nearby-btn"
                    style={{ padding: "4px 10px", fontSize: 12 }}
                    onClick={() => setShowNearby(false)}
                  >
                    숨기기
                  </button>
                </div>
              </div>
              <TxTable
                items={nearbyItems}
                unit={unit}
                selectedKey={selectedKey}
                onSelect={onSelect}
                keyPrefix="n"
                muted
              />
              <Pagination
                page={nearbyPage}
                totalPages={nearbyTotalPages}
                total={nearby.length}
                onChange={setNearbyPage}
              />
            </div>
          )}
        </div>
      )}
    </>
  );
}

// ─────────────── sub components ───────────────

function FallbackBanners({ data }: { data: TransactionResult }) {
  const msgs: string[] = [];
  if (data.is_fallback)
    msgs.push(
      `정확 주소 매칭 실패 → ${data.fallback_dong || "동"} 전체로 확장 조회`,
    );
  if (data.bun_fallback)
    msgs.push(
      `같은 지번 거래 없음 → 본번 ${data.fallback_bun} 의 다른 부번 포함`,
    );
  if (data.building_fallback) msgs.push("단지/건물명 매칭 실패 → 다른 기준으로 반환");
  if (data.area_fallback)
    msgs.push("요청 면적과 정확히 같은 거래 없음 → 인근 면적 포함");
  if (data.jimok_fallback) msgs.push("요청 지목 매칭 실패 → 다른 지목 포함");

  if (msgs.length === 0) return null;
  return (
    <div className="info-banner">
      {msgs.map((m, i) => (
        <div key={i}>· {m}</div>
      ))}
    </div>
  );
}

function EmptyState() {
  return (
    <div
      style={{
        padding: "40px 16px",
        textAlign: "center",
        color: "#888",
        fontSize: 14,
        background: "#f8f9fb",
        borderRadius: 8,
      }}
    >
      매칭되는 거래가 없어요. 기간을 늘리거나 주소를 동 단위로 간결하게 입력해
      보세요.
    </div>
  );
}

function AverageBox({
  transactions,
  unit,
}: {
  transactions: Transaction[];
  unit: Unit;
}) {
  const info = useMemo(() => {
    const valid = transactions.filter(
      (t) => t.area_m2 > 0 && t.price_man_won > 0,
    );
    if (valid.length === 0) return null;
    let sumPer = 0;
    for (const t of valid) {
      const denom = unit === "py" ? t.area_m2 / PY_PER_M2 : t.area_m2;
      sumPer += t.price_man_won / denom;
    }
    const avg = Math.round(sumPer / valid.length);
    const dates = valid.map(
      (t) => t.deal_year * 10000 + t.deal_month * 100 + t.deal_day,
    );
    const minD = Math.min(...dates);
    const maxD = Math.max(...dates);
    const fmtD = (d: number) =>
      `${Math.floor(d / 10000)}.${String(Math.floor(d / 100) % 100).padStart(
        2,
        "0",
      )}.${String(d % 100).padStart(2, "0")}`;
    const range = minD === maxD ? fmtD(minD) : `${fmtD(minD)} ~ ${fmtD(maxD)}`;
    return { avg, range, count: valid.length };
  }, [transactions, unit]);

  if (!info) return null;
  const unitLabel = unit === "py" ? "만원/평" : "만원/㎡";
  return (
    <div className="avg">
      <div className="avg-price">
        {info.avg.toLocaleString()}
        <span style={{ fontSize: 12, fontWeight: 500 }}> {unitLabel}</span>
      </div>
      <div>
        평균 {unitLabel} ({info.count}건)
      </div>
      <div className="avg-meta">{info.range}</div>
    </div>
  );
}

function TxTable({
  items,
  unit,
  selectedKey,
  onSelect,
  keyPrefix,
  muted,
}: {
  items: Transaction[];
  unit: Unit;
  selectedKey: string | null;
  onSelect: (tx: Transaction, key: string) => void;
  keyPrefix: string;
  muted?: boolean;
}) {
  return (
    <div className="trans-table-wrap">
      <table className="trans-table">
        <thead>
          <tr>
            <th>계약일</th>
            <th>단지/건물</th>
            <th>주소</th>
            <th>면적</th>
            <th>층</th>
            <th>거래금액</th>
            <th>기타</th>
          </tr>
        </thead>
        <tbody>
          {items.map((t, i) => {
            const key = `${keyPrefix}-${t.deal_year}${t.deal_month}${t.deal_day}-${t.jibun}-${t.price_man_won}-${i}`;
            const selected = selectedKey === key;
            return (
              <tr
                key={key}
                className={selected ? "selected" : ""}
                style={muted ? { opacity: 0.9 } : undefined}
                onClick={() => onSelect(t, key)}
              >
                <td data-label="계약일">
                  {formatDate(t.deal_year, t.deal_month, t.deal_day)}
                </td>
                <td data-label="단지/건물">{t.name || "-"}</td>
                <td data-label="주소" className="addr-cell">
                  <div className="addr-main">
                    {t.sgg_nm} {t.dong} {t.jibun || ""}
                    {t.address_estimated && <span className="est-tag">추정</span>}
                  </div>
                  {t.road_address && (
                    <div className="addr-sub">{t.road_address}</div>
                  )}
                </td>
                <td data-label="면적" className="area-cell">
                  <AreaCell tx={t} unit={unit} />
                </td>
                <td data-label="층">{t.floor != null ? `${t.floor}층` : "-"}</td>
                <td data-label="거래금액" className="price-cell">
                  {formatPrice(t.price_man_won)}
                </td>
                <td data-label="기타">
                  <TagsCell tx={t} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function AreaCell({ tx, unit }: { tx: Transaction; unit: Unit }) {
  const fmt = (m2: number | null | undefined) => {
    if (!m2 || m2 <= 0) return "";
    if (unit === "py") return `${(m2 / PY_PER_M2).toFixed(1)}평`;
    return `${m2}㎡`;
  };
  const lines: { main?: boolean; text: string }[] = [];
  if (tx.area_m2 > 0 && tx.area_type) {
    lines.push({ main: true, text: `${tx.area_type}: ${fmt(tx.area_m2)}` });
  } else if (tx.area_m2 > 0) {
    lines.push({ main: true, text: fmt(tx.area_m2) });
  }
  if (tx.exclu_use_ar && tx.area_type !== "전용면적")
    lines.push({ text: `전용: ${fmt(tx.exclu_use_ar)}` });
  if (tx.land_ar) lines.push({ text: `대지권: ${fmt(tx.land_ar)}` });
  if (tx.building_ar && tx.area_type !== "건물면적")
    lines.push({ text: `건물: ${fmt(tx.building_ar)}` });
  if (tx.plottage_ar) lines.push({ text: `대지: ${fmt(tx.plottage_ar)}` });
  if (tx.deal_area && tx.area_type !== "거래면적")
    lines.push({ text: `거래: ${fmt(tx.deal_area)}` });
  if (tx.total_floor_ar && tx.area_type !== "연면적")
    lines.push({ text: `연면적: ${fmt(tx.total_floor_ar)}` });

  if (lines.length === 0) return <>-</>;
  return (
    <>
      {lines.map((l, i) => (
        <span key={i} className={l.main ? "area-main" : "area-sub"}>
          {l.text}
        </span>
      ))}
    </>
  );
}

function TagsCell({ tx }: { tx: Transaction }) {
  const tags: { cls: string; text: string }[] = [];
  if (tx.dealing_gbn) tags.push({ cls: "tag tag-trade", text: tx.dealing_gbn });
  if (tx.cdeal_type) tags.push({ cls: "tag tag-cancel", text: `해제: ${tx.cdeal_type}` });
  if (tx.cdeal_day) tags.push({ cls: "tag tag-cancel", text: `해제일: ${tx.cdeal_day}` });
  if (tx.jimok) tags.push({ cls: "tag tag-jimok", text: `지목: ${tx.jimok}` });
  if (tx.land_use) tags.push({ cls: "tag tag-landuse", text: tx.land_use });
  if (tx.house_type) tags.push({ cls: "tag tag-housetype", text: tx.house_type });
  if (tx.share_dealing_type && tx.share_dealing_type !== "일반")
    tags.push({ cls: "tag tag-share", text: tx.share_dealing_type });

  if (tags.length === 0) return <>-</>;
  return (
    <>
      {tags.map((t, i) => (
        <span key={i} className={t.cls}>
          {t.text}
        </span>
      ))}
    </>
  );
}

function Pagination({
  page,
  totalPages,
  total,
  onChange,
}: {
  page: number;
  totalPages: number;
  total: number;
  onChange: (p: number) => void;
}) {
  if (totalPages <= 1) return null;
  const maxShow = 5;
  const start = Math.max(1, Math.min(page - 2, totalPages - maxShow + 1));
  const end = Math.min(totalPages, start + maxShow - 1);
  const pages: number[] = [];
  for (let i = start; i <= end; i++) pages.push(i);

  return (
    <div
      style={{
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        gap: 4,
        marginTop: 16,
        flexWrap: "wrap",
      }}
    >
      <PageBtn disabled={page === 1} onClick={() => onChange(page - 1)}>
        ‹
      </PageBtn>
      {pages.map((p) => (
        <PageBtn key={p} active={p === page} onClick={() => onChange(p)}>
          {p}
        </PageBtn>
      ))}
      <PageBtn
        disabled={page === totalPages}
        onClick={() => onChange(page + 1)}
      >
        ›
      </PageBtn>
      <span style={{ fontSize: 13, color: "#666", marginLeft: 8 }}>
        {page} / {totalPages} (총 {total}건)
      </span>
    </div>
  );
}

function PageBtn({
  children,
  active,
  disabled,
  onClick,
}: {
  children: React.ReactNode;
  active?: boolean;
  disabled?: boolean;
  onClick?: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: "6px 12px",
        border: active ? "1px solid #4a7cf7" : "1px solid #ddd",
        borderRadius: 6,
        background: active ? "#4a7cf7" : "#fff",
        color: active ? "#fff" : disabled ? "#bbb" : "#444",
        fontSize: 13,
        cursor: disabled ? "default" : "pointer",
        minWidth: 36,
      }}
    >
      {children}
    </button>
  );
}

// ─────────────── format helpers ───────────────

function formatPrice(manWon: number): string {
  if (!manWon || manWon <= 0) return "0원";
  const eok = Math.floor(manWon / 10000);
  const man = manWon % 10000;
  if (eok >= 1 && man === 0) return `${eok}억원`;
  if (eok >= 1) return `${eok}억 ${man.toLocaleString()}만원`;
  return `${man.toLocaleString()}만원`;
}

function formatDate(y: number, m: number, d: number): string {
  return `${y}.${String(m).padStart(2, "0")}.${String(d).padStart(2, "0")}`;
}
