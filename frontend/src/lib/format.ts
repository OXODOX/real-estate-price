/**
 * 화면 표시용 포맷 함수 모음.
 */

/** 만원 → "12억 3,456만원" 스타일 문자열. */
export function formatPriceManWon(manWon: number): string {
  if (!manWon || manWon <= 0) return "-";
  const eok = Math.floor(manWon / 10000);
  const rem = manWon % 10000;
  if (eok > 0 && rem > 0) {
    return `${eok.toLocaleString()}억 ${rem.toLocaleString()}만원`;
  }
  if (eok > 0) return `${eok.toLocaleString()}억원`;
  return `${rem.toLocaleString()}만원`;
}

/** (2024, 3, 15) → "2024.03.15". */
export function formatDealDate(y: number, m: number, d: number): string {
  const mm = String(m).padStart(2, "0");
  const dd = String(d).padStart(2, "0");
  return `${y}.${mm}.${dd}`;
}

/** ㎡ → "84.99㎡ (25.7평)". */
export function formatArea(m2: number | null | undefined): string {
  if (!m2 || m2 <= 0) return "-";
  const pyeong = m2 / 3.3058;
  return `${m2.toFixed(2)}㎡ (${pyeong.toFixed(1)}평)`;
}
