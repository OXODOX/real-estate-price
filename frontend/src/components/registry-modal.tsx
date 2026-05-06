"use client";

import { useEffect, useState } from "react";
import { fetchRegistry, type RegistryResult } from "@/lib/api";

interface Props {
  sgg_cd: string;
  dong: string;
  jibun: string;
  sgg_nm?: string;
  onClose: () => void;
}

export function RegistryModal({ sgg_cd, dong, jibun, sgg_nm, onClose }: Props) {
  const [data, setData] = useState<RegistryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError(null);
    fetchRegistry({ sgg_cd, dong, jibun })
      .then((r) => {
        if (alive) setData(r);
      })
      .catch((e: unknown) => {
        if (alive) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [sgg_cd, dong, jibun]);

  // ESC 키로 닫기
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div className="reg-overlay" onClick={onClose}>
      <div className="reg-modal" onClick={(e) => e.stopPropagation()}>
        <div className="reg-header">
          <div>
            <div className="reg-title">대장 정보</div>
            <div className="reg-subtitle">
              {sgg_nm ? `${sgg_nm} ` : ""}
              {dong} {jibun}
            </div>
          </div>
          <button type="button" className="reg-close" onClick={onClose}>
            ✕
          </button>
        </div>

        <div className="reg-body">
          {loading && <div className="reg-loading">불러오는 중…</div>}
          {error && <div className="reg-error">조회 실패: {error}</div>}
          {!loading && !error && data && (
            <>
              {data.note && <div className="reg-note">· {data.note}</div>}

              <Section title={`토지대장 (${data.parcels.length}건)`}>
                {data.parcels.length === 0 ? (
                  <div className="reg-empty">해당 지번의 토지대장 정보 없음</div>
                ) : (
                  <table className="reg-table">
                    <thead>
                      <tr>
                        <th>지번</th>
                        <th>지목</th>
                        <th>면적(㎡)</th>
                        <th>용도지역</th>
                        <th>이용상황</th>
                        <th>공시지가(원/㎡)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.parcels.map((p, i) => (
                        <tr key={i}>
                          <td>
                            {p.sanji === "2" ? "산" : ""}
                            {p.bun}
                            {p.ji && p.ji !== "0" ? `-${p.ji}` : ""}
                          </td>
                          <td>{p.jimok_nm || "-"}</td>
                          <td>{p.land_area != null ? p.land_area.toLocaleString() : "-"}</td>
                          <td>{p.land_use || "-"}</td>
                          <td>{p.usage_nm || "-"}</td>
                          <td>
                            {p.price != null ? p.price.toLocaleString() : "-"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </Section>

              <Section title={`건축물대장 (${data.buildings.length}건)`}>
                {data.buildings.length === 0 ? (
                  <div className="reg-empty">해당 지번의 건축물대장 정보 없음</div>
                ) : (
                  <div className="reg-bldg-list">
                    {data.buildings.map((b, i) => (
                      <BuildingCard key={i} b={b} />
                    ))}
                  </div>
                )}
              </Section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function BuildingCard({ b }: { b: import("@/lib/api").BuildingInfo }) {
  const jibunLabel = `${b.bun}${b.ji && b.ji !== "0" ? `-${b.ji}` : ""}`;
  const statusLabel =
    b.status === "closed"
      ? `멸실 ${formatYmd(b.demolish_day)}`
      : "활성";
  const floorsLabel = (() => {
    const g = b.ground_floors ?? 0;
    const u = b.under_floors ?? 0;
    if (g <= 0 && u <= 0) return "";
    const parts: string[] = [];
    if (u > 0) parts.push(`지하 ${u}층`);
    if (g > 0) parts.push(`지상 ${g}층`);
    return parts.join(" / ");
  })();
  const fmt = (v: number | null | undefined, unit = "") =>
    v == null || v === 0 ? "-" : `${v.toLocaleString()}${unit}`;
  const use = b.etc_purps && b.etc_purps !== b.main_purps_nm
    ? `${b.main_purps_nm} (${b.etc_purps})`
    : b.main_purps_nm || "-";

  return (
    <div className="reg-bldg-card">
      <div className="reg-bldg-head">
        <div className="reg-bldg-title">
          <span className="reg-bldg-jibun">{jibunLabel}</span>
          {b.bld_nm && <span className="reg-bldg-name">{b.bld_nm}</span>}
        </div>
        <div className="reg-bldg-status">
          {statusLabel} · 사용승인 {formatYmd(b.use_apr_day)}
        </div>
      </div>
      <div className="reg-bldg-grid">
        <KV k="주용도" v={use} wide />
        {floorsLabel && <KV k="층수" v={floorsLabel} />}
        {b.struct_nm && <KV k="구조" v={b.struct_nm} />}
        {b.roof_nm && <KV k="지붕" v={b.roof_nm} />}
        <KV k="대지면적" v={fmt(b.plat_area, " ㎡")} />
        <KV k="건축면적" v={fmt(b.arch_area, " ㎡")} />
        <KV k="연면적" v={fmt(b.tot_area, " ㎡")} />
        {b.coverage_ratio != null && (
          <KV k="건폐율" v={`${b.coverage_ratio}%`} />
        )}
        {b.floor_ratio != null && <KV k="용적률" v={`${b.floor_ratio}%`} />}
        {b.household_cnt != null && b.household_cnt > 0 && (
          <KV k="세대수" v={`${b.household_cnt} 세대`} />
        )}
        {b.family_cnt != null && b.family_cnt > 0 && (
          <KV k="가구수" v={`${b.family_cnt} 가구`} />
        )}
        {b.parking_cnt != null && b.parking_cnt > 0 && (
          <KV k="주차" v={`${b.parking_cnt} 대`} />
        )}
      </div>
    </div>
  );
}

function KV({ k, v, wide }: { k: string; v: string; wide?: boolean }) {
  return (
    <div className={`reg-kv${wide ? " wide" : ""}`}>
      <span className="reg-kv-k">{k}</span>
      <span className="reg-kv-v">{v}</span>
    </div>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="reg-section">
      <div className="reg-section-title">{title}</div>
      {children}
    </div>
  );
}

function formatYmd(s: string): string {
  if (!s || s.length < 8) return s || "-";
  return `${s.slice(0, 4)}.${s.slice(4, 6)}.${s.slice(6, 8)}`;
}
