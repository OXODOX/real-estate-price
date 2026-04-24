/**
 * 백엔드(FastAPI) 와의 통신 + TypeScript 타입 정의.
 *
 * 백엔드 스키마는 `backend/app/models/schemas.py` 와 1:1 로 대응된다.
 * 백엔드 필드가 바뀌면 여기도 같이 갱신할 것.
 */

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8000";

// ─────────────── Enums ───────────────

export type PropertyType =
  | "아파트"
  | "연립다세대"
  | "단독다가구"
  | "오피스텔"
  | "토지"
  | "상업업무용"
  | "분양권전매"
  | "공장창고";

export const PROPERTY_TYPES: PropertyType[] = [
  "아파트",
  "연립다세대",
  "단독다가구",
  "오피스텔",
  "토지",
  "상업업무용",
  "분양권전매",
  "공장창고",
];

export type TransactionType = "매매" | "전월세";

// ─────────────── Request ───────────────

export interface PriceRequest {
  address: string;
  property_type?: PropertyType | null;
  area_m2?: number | null;
  building_name?: string | null;
  months_back?: number;
  jimok?: string | null;
}

// ─────────────── Response ───────────────

export interface Transaction {
  property_type: PropertyType;
  transaction_type: TransactionType;
  name: string;

  sgg_cd: string;
  sgg_nm: string;
  dong: string;
  jibun: string;
  road_address: string;
  estimated_jibun: string;
  address_estimated: boolean;
  address_estimated_certain: boolean;

  price_man_won: number;
  deal_year: number;
  deal_month: number;
  deal_day: number;
  floor: number | null;
  build_year: number | null;
  dealing_gbn: string;
  buyer_gbn: string;
  sler_gbn: string;

  cdeal_day: string;
  cdeal_type: string;

  area_m2: number;
  area_type: string;
  exclu_use_ar: number | null;
  land_ar: number | null;
  building_ar: number | null;
  plottage_ar: number | null;
  deal_area: number | null;
  total_floor_ar: number | null;

  jimok: string;
  land_use: string;
  house_type: string;
  building_type: string;
  building_use: string;
  share_dealing_type: string;
}

export interface TransactionResult {
  address: string;
  property_type: PropertyType;
  recent_transactions: Transaction[];
  nearby_transactions: Transaction[];
  is_fallback: boolean;
  fallback_dong: string;
  bun_fallback: boolean;
  fallback_bun: string;
  area_fallback: boolean;
  building_fallback: boolean;
  jimok_fallback: boolean;
}

// ─────────────── Fetch ───────────────

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

export async function fetchEstimate(
  req: PriceRequest,
  signal?: AbortSignal,
): Promise<TransactionResult> {
  const res = await fetch(`${API_BASE}/api/v1/estimate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
    signal,
  });

  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      if (body?.detail) detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    } catch {
      // ignore
    }
    throw new ApiError(res.status, detail);
  }

  return res.json();
}
