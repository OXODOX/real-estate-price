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
                  <table className="reg-table">
                    <thead>
                      <tr>
                        <th>지번</th>
                        <th>건물명</th>
                        <th>주용도</th>
                        <th>대지(㎡)</th>
                        <th>건축(㎡)</th>
                        <th>연면적(㎡)</th>
                        <th>사용승인</th>
                        <th>상태</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.buildings.map((b, i) => (
                        <tr key={i}>
                          <td>
                            {b.bun}
                            {b.ji && b.ji !== "0" ? `-${b.ji}` : ""}
                          </td>
                          <td>{b.bld_nm || "-"}</td>
                          <td>{b.main_purps_nm || "-"}</td>
                          <td>
                            {b.plat_area != null ? b.plat_area.toLocaleString() : "-"}
                          </td>
                          <td>
                            {b.arch_area != null ? b.arch_area.toLocaleString() : "-"}
                          </td>
                          <td>
                            {b.tot_area != null ? b.tot_area.toLocaleString() : "-"}
                          </td>
                          <td>{formatYmd(b.use_apr_day)}</td>
                          <td>
                            {b.status === "closed"
                              ? `멸실 (${formatYmd(b.demolish_day)})`
                              : "활성"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </Section>
            </>
          )}
        </div>
      </div>
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
