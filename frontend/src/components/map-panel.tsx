"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { Transaction } from "@/lib/api";

const KAKAO_KEY = process.env.NEXT_PUBLIC_KAKAO_MAP_KEY ?? "";

type Status = "idle" | "loading" | "ready" | "error";

/**
 * layout.tsx 에서 `<Script>` 로 미리 SDK 를 내려받는다.
 * 이 훅은 `window.kakao.maps` 가 준비될 때까지 폴링한 뒤,
 * `kakao.maps.load()` 를 호출해 실제 네임스페이스를 초기화한다.
 */
function useKakaoReady(): Status {
  const [status, setStatus] = useState<Status>("idle");

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (!KAKAO_KEY) {
      setStatus("error");
      return;
    }

    setStatus("loading");
    let cancelled = false;
    let tries = 0;
    const MAX_TRIES = 60; // 6초 (100ms × 60)

    const tick = () => {
      if (cancelled) return;
      // @ts-expect-error kakao global
      if (window.kakao?.maps) {
        // @ts-expect-error kakao global
        window.kakao.maps.load(() => {
          if (!cancelled) setStatus("ready");
        });
        return;
      }
      if (++tries >= MAX_TRIES) {
        setStatus("error");
        return;
      }
      setTimeout(tick, 100);
    };
    tick();

    return () => {
      cancelled = true;
    };
  }, []);

  return status;
}

interface Props {
  transaction: Transaction | null;
}

export function MapPanel({ transaction }: Props) {
  const status = useKakaoReady();
  const mapRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mapObjRef = useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const markerRef = useRef<any>(null);
  const [notFound, setNotFound] = useState(false);
  const [approx, setApprox] = useState(false);

  // 조회용 주소 문자열 (정확 / 폴백)
  const { precise, fallback } = useMemo(() => {
    if (!transaction) return { precise: "", fallback: "" };
    const sgg = transaction.sgg_nm;
    const dong = transaction.dong;
    const jibunRaw = transaction.jibun || "";
    const jibun = jibunRaw.includes("*") ? "" : jibunRaw;
    return {
      precise: [sgg, dong, jibun].filter(Boolean).join(" ").trim(),
      fallback: [sgg, dong].filter(Boolean).join(" ").trim(),
    };
  }, [transaction]);

  // 지도 1회 초기화
  useEffect(() => {
    if (status !== "ready" || !mapRef.current || mapObjRef.current) return;
    // @ts-expect-error kakao global
    const { kakao } = window;
    mapObjRef.current = new kakao.maps.Map(mapRef.current, {
      center: new kakao.maps.LatLng(37.5665, 126.978),
      level: 8,
    });
  }, [status]);

  // 선택된 거래 변경 시 마커 이동
  useEffect(() => {
    if (status !== "ready" || !mapObjRef.current) return;
    if (!precise && !fallback) return;

    // @ts-expect-error kakao global
    const { kakao } = window;
    const geocoder = new kakao.maps.services.Geocoder();

    setNotFound(false);
    setApprox(false);

    const place = (lat: number, lng: number, isApprox: boolean) => {
      const pos = new kakao.maps.LatLng(lat, lng);
      mapObjRef.current.setLevel(isApprox ? 6 : 3);
      mapObjRef.current.setCenter(pos);
      if (markerRef.current) markerRef.current.setMap(null);
      markerRef.current = new kakao.maps.Marker({
        position: pos,
        map: mapObjRef.current,
      });
      setApprox(isApprox);
    };

    const trySearch = (
      q: string,
      isApprox: boolean,
      onFail: () => void,
    ) => {
      geocoder.addressSearch(
        q,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (result: any[], s: string) => {
          if (s === kakao.maps.services.Status.OK && result.length > 0) {
            place(Number(result[0].y), Number(result[0].x), isApprox);
          } else {
            onFail();
          }
        },
      );
    };

    if (precise) {
      trySearch(precise, false, () => {
        if (fallback && fallback !== precise) {
          trySearch(fallback, true, () => setNotFound(true));
        } else {
          setNotFound(true);
        }
      });
    } else if (fallback) {
      trySearch(fallback, true, () => setNotFound(true));
    }
  }, [status, precise, fallback]);

  // ─── 렌더 ───

  if (!KAKAO_KEY) {
    return (
      <div className="map-wrap">
        <div className="map-hint">카카오 지도 키가 설정되지 않았어요.</div>
      </div>
    );
  }

  if (status === "error") {
    return (
      <div className="map-wrap">
        <div className="map-hint">
          지도 SDK 로드 실패. 카카오 개발자 콘솔 → 플랫폼에
          http://localhost:3000 이 등록되었는지 확인하세요.
        </div>
      </div>
    );
  }

  return (
    <div className="map-wrap">
      <div ref={mapRef} style={{ width: "100%", height: "100%" }} />

      {status !== "ready" && (
        <div className="map-hint">지도 불러오는 중…</div>
      )}

      {status === "ready" && !transaction && (
        <div className="map-hint">거래를 클릭하면 위치가 표시돼요.</div>
      )}

      {status === "ready" && notFound && (
        <div
          style={{
            position: "absolute",
            top: 8,
            left: 8,
            padding: "4px 10px",
            background: "rgba(255,255,255,0.95)",
            border: "1px solid #f5c2c0",
            borderRadius: 6,
            fontSize: 12,
            color: "#b3261e",
          }}
        >
          주소 좌표를 찾지 못했어요
        </div>
      )}

      {status === "ready" && approx && !notFound && (
        <div
          style={{
            position: "absolute",
            top: 8,
            left: 8,
            padding: "4px 10px",
            background: "rgba(255,255,255,0.95)",
            border: "1px solid #e0e0e0",
            borderRadius: 6,
            fontSize: 12,
            color: "#666",
          }}
        >
          동 단위 위치 (지번 매칭 실패)
        </div>
      )}
    </div>
  );
}
