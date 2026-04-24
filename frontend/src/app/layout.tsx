import type { Metadata } from "next";
import Script from "next/script";
import "./globals.css";
import { Providers } from "@/components/providers";

export const metadata: Metadata = {
  title: "부동산 실거래가 조회",
  description:
    "국토교통부 실거래가 데이터를 기반으로 한 실거래가 조회 서비스",
};

const KAKAO_KEY = process.env.NEXT_PUBLIC_KAKAO_MAP_KEY ?? "";

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko">
      <body>
        {/* 카카오 지도 SDK 전역 로드 (autoload=false → 컴포넌트에서 수동 초기화) */}
        {KAKAO_KEY && (
          <Script
            id="kakao-map-sdk"
            strategy="afterInteractive"
            src={`https://dapi.kakao.com/v2/maps/sdk.js?appkey=${KAKAO_KEY}&libraries=services&autoload=false`}
          />
        )}
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
