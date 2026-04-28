"use client";

import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import type { PriceRequest, PropertyType } from "@/lib/api";

const JIMOK_OPTIONS = [
  "", "전", "답", "대", "임야", "과수원", "목장용지", "공장용지", "창고용지",
  "학교용지", "주차장", "주유소용지", "도로", "하천", "제방", "구거", "유지",
  "양어장", "수도용지", "공원", "체육용지", "유원지", "종교용지", "사적지",
  "묘지", "잡종지", "철도용지", "염전",
];

const MONTHS_OPTIONS: Array<{ value: number; label: string }> = [
  { value: 3, label: "최근 3개월" },
  { value: 6, label: "최근 6개월" },
  { value: 12, label: "최근 1년" },
  { value: 24, label: "최근 2년" },
  { value: 36, label: "최근 3년" },
  { value: 60, label: "최근 5년" },
];

const schema = z.object({
  address: z.string().min(2, "주소를 2자 이상 입력하세요"),
  property_type: z.enum([
    "아파트",
    "연립다세대",
    "오피스텔",
    "단독다가구",
    "토지",
    "상업업무용",
    "분양권전매",
    "공장창고",
  ]),
  area_m2: z.string().optional(),
  area_unit: z.enum(["m2", "py"]),
  building_name: z.string().optional(),
  jimok: z.string().optional(),
  months_back: z.number().int().min(0).max(60),
});

type FormValues = z.infer<typeof schema>;

interface Props {
  onSubmit: (req: PriceRequest) => void;
  loading?: boolean;
}

const PY_PER_M2 = 3.3058;

export function SearchForm({ onSubmit, loading }: Props) {
  const {
    register,
    handleSubmit,
    watch,
    formState: { errors },
  } = useForm<FormValues>({
    resolver: zodResolver(schema),
    defaultValues: {
      address: "",
      property_type: "아파트",
      area_unit: "m2",
      months_back: 6,
    },
  });

  const selectedType = watch("property_type");
  const isLand = selectedType === "토지";

  const submit = handleSubmit((v) => {
    // 면적 단위 변환
    let areaM2: number | null = null;
    if (v.area_m2 && v.area_m2.trim()) {
      const raw = Number(v.area_m2);
      if (!Number.isNaN(raw) && raw > 0) {
        areaM2 = v.area_unit === "py" ? raw * PY_PER_M2 : raw;
      }
    }

    const req: PriceRequest = {
      address: v.address.trim(),
      property_type: v.property_type as PropertyType,
      months_back: v.months_back,
      area_m2: areaM2,
      building_name: v.building_name?.trim() || null,
      jimok: isLand && v.jimok?.trim() ? v.jimok.trim() : null,
    };
    onSubmit(req);
  });

  return (
    <form onSubmit={submit} autoComplete="off">
      <div className="form-group">
        <label>
          주소 <span className="required">*</span>
        </label>
        <input
          type="text"
          placeholder="예: 서울 강남구 역삼동 679-13 역삼래미안"
          {...register("address")}
        />
        {errors.address ? (
          <span className="hint" style={{ color: "#c33" }}>
            {errors.address.message}
          </span>
        ) : (
          <span className="hint">
            최소 <b>시·군·구</b> 단위까지는 입력해야 조회됩니다 (예: &quot;경기도 용인시 수지구&quot;,
            &quot;강남구&quot;).<br />
            읍·면·동·지번·단지명까지 입력할수록 정확도가 높아집니다.
          </span>
        )}
      </div>

      <div className="form-row">
        <div className="form-group">
          <label>
            부동산 유형 <span className="required">*</span>
          </label>
          <select {...register("property_type")}>
            <option value="아파트">아파트</option>
            <option value="연립다세대">연립/다세대 (빌라)</option>
            <option value="오피스텔">오피스텔</option>
            <option value="단독다가구">단독/다가구</option>
            <option value="토지">토지</option>
            <option value="상업업무용">상업업무용</option>
            <option value="분양권전매">아파트 분양권</option>
            <option value="공장창고">공장/창고</option>
          </select>
        </div>
        <div className="form-group">
          <label>
            전용면적 <span className="optional">(선택)</span>
          </label>
          <div style={{ display: "flex", gap: 6 }}>
            <input
              type="number"
              step="0.01"
              min="0"
              placeholder="예: 84"
              style={{ flex: 1 }}
              {...register("area_m2")}
            />
            <select
              style={{ width: 70 }}
              {...register("area_unit")}
            >
              <option value="m2">㎡</option>
              <option value="py">평</option>
            </select>
          </div>
          <span className="hint">±10%(최소 ±5㎡) 범위로 매칭됩니다.</span>
        </div>
      </div>

      <div className="form-group">
        <label>
          단지명/건물명 <span className="optional">(선택)</span>
        </label>
        <input
          type="text"
          placeholder="예: 역삼래미안"
          {...register("building_name")}
        />
        <span className="hint">
          입력 시 동일 단지 실거래 기준으로 정확도 대폭 향상
        </span>
      </div>

      {isLand && (
        <div className="form-group">
          <label>
            지목 <span className="optional">(토지, 선택)</span>
          </label>
          <select {...register("jimok")}>
            {JIMOK_OPTIONS.map((j) => (
              <option key={j} value={j}>
                {j || "전체"}
              </option>
            ))}
          </select>
        </div>
      )}

      <div className="form-group">
        <label>
          조회 기간 <span className="optional">(선택)</span>
        </label>
        <select {...register("months_back", { valueAsNumber: true })}>
          {MONTHS_OPTIONS.map((m) => (
            <option key={m.value} value={m.value}>
              {m.label}
            </option>
          ))}
        </select>
      </div>

      <button type="submit" className="submit-btn" disabled={loading}>
        {loading ? "조회 중..." : "가격 조회"}
      </button>
    </form>
  );
}
