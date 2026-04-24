"use client";

import { useEffect, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import {
  ApiError,
  fetchEstimate,
  warmupBackend,
  type PriceRequest,
  type Transaction,
  type TransactionResult,
} from "@/lib/api";
import { SearchForm } from "@/components/search-form";
import { TransactionList } from "@/components/transaction-list";
import { MapPanel } from "@/components/map-panel";

export default function Page() {
  const [result, setResult] = useState<TransactionResult | null>(null);
  const [selected, setSelected] = useState<{
    tx: Transaction;
    key: string;
  } | null>(null);

  // 페이지 로드 시 Render 서버 미리 깨우기 (cold start 완화)
  useEffect(() => {
    warmupBackend();
  }, []);

  const mutation = useMutation<TransactionResult, Error, PriceRequest>({
    mutationFn: (req) => fetchEstimate(req),
    retry: (failureCount, error) => {
      // 400/404 같은 정상 응답은 재시도 안 함, 네트워크 오류만 최대 2회 재시도
      if (error instanceof ApiError && error.status >= 400) return false;
      return failureCount < 2;
    },
    retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 5000),
    onSuccess: (data) => {
      setResult(data);
      const first =
        data.recent_transactions[0] ?? data.nearby_transactions[0] ?? null;
      if (first) {
        const key = `r-${first.deal_year}${first.deal_month}${first.deal_day}-${first.jibun}-${first.price_man_won}-0`;
        setSelected({ tx: first, key });
      } else {
        setSelected(null);
      }
    },
  });

  const errorMsg = (() => {
    const err = mutation.error;
    if (!err) return null;
    if (err instanceof ApiError) {
      if (err.status === 400) return err.message || "주소 해석에 실패했어요.";
      return `서버 오류 (${err.status}): ${err.message}`;
    }
    return `네트워크 오류: ${err.message}`;
  })();

  return (
    <div className="container-narrow">
      <header className="page-header">
        <h1>부동산 실거래가 조회</h1>
        <p>국토교통부 실거래가 공개시스템 기반 실거래 내역을 제공합니다</p>
      </header>

      {/* 검색 폼 */}
      <div className="card">
        <SearchForm
          loading={mutation.isPending}
          onSubmit={(req) => {
            setResult(null);
            setSelected(null);
            mutation.mutate(req);
          }}
        />
      </div>

      {/* 로딩 / 에러 */}
      {mutation.isPending && (
        <div className="loader">실거래 데이터를 불러오고 있습니다...</div>
      )}
      {errorMsg && <div className="error-banner">{errorMsg}</div>}

      {/* 결과 */}
      {result && !mutation.isPending && (
        <>
          <div className="card">
            <TransactionList
              data={result}
              selectedKey={selected?.key ?? null}
              onSelect={(tx, key) => setSelected({ tx, key })}
            />
          </div>

          {/* 지도 카드 */}
          <div className="card">
            <div
              style={{
                fontSize: 14,
                fontWeight: 600,
                color: "#444",
                marginBottom: 10,
              }}
            >
              선택한 거래 위치
            </div>
            <MapPanel transaction={selected?.tx ?? null} />
          </div>
        </>
      )}

      <footer className="page-footer">
        데이터 출처: 국토교통부 실거래가 공개시스템 ·
        본 추정가는 참고용이며 실제 거래가와 다를 수 있습니다.
      </footer>
    </div>
  );
}
