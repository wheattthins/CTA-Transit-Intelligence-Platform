// Formatting. Instants arrive as epoch ms; Chicago wall-clock values as ISO strings without zone.

const CHICAGO = "America/Chicago";

export function formatInstant(ms: number, opts: Intl.DateTimeFormatOptions = {}): string {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: CHICAGO,
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
    ...opts,
  }).format(new Date(ms));
}

/** Epoch ms -> "2:50:18 PM CDT": Chicago time of day to the second, for live data. */
export function formatTimeOfDay(ms: number): string {
  return formatInstant(ms, { month: undefined, day: undefined, second: "2-digit" });
}

/** "2026-10-15T08:01:05" (already Chicago local) -> "8:01 AM". Parsed without time-zone conversion. */
export function formatLocalTime(iso: string, withSeconds = false): string {
  const [, time] = iso.split("T");
  const [h, m, s] = time.split(":").map(Number);
  const suffix = h >= 12 ? "PM" : "AM";
  const hour = h % 12 || 12;
  return `${hour}:${String(m).padStart(2, "0")}${withSeconds ? `:${String(s).padStart(2, "0")}` : ""} ${suffix}`;
}

/** Minutes since local midnight for a Chicago wall-clock ISO string (for time-of-day axes). */
export function localMinutes(iso: string): number {
  const [h, m, s] = iso.split("T")[1].split(":").map(Number);
  return h * 60 + m + s / 60;
}

export function formatClock(minutes: number): string {
  const h = Math.floor(minutes / 60) % 24;
  const m = Math.round(minutes % 60);
  return `${h % 12 || 12}${m ? `:${String(m).padStart(2, "0")}` : ""} ${h >= 12 ? "PM" : "AM"}`;
}

export function formatServiceDate(date: string): string {
  return new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" })
    .format(new Date(`${date}T12:00:00Z`));
}

export function formatAge(ms: number, now = Date.now()): string {
  const minutes = Math.max(0, Math.round((now - ms) / 60000));
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours} h ago`;
  return `${Math.round(hours / 24)} days ago`;
}

export const fmt = {
  int: (v: number | null | undefined) => (v === null || v === undefined ? "—" : Math.round(v).toLocaleString("en-US")),
  seconds: (v: number | null | undefined) => (v === null || v === undefined ? "—" : `${Math.round(v)} s`),
  signedSeconds: (v: number | null | undefined) =>
    v === null || v === undefined ? "—" : `${v > 0 ? "+" : v < 0 ? "−" : ""}${Math.abs(Math.round(v))} s`,
  minutes: (v: number | null | undefined, digits = 1) => (v === null || v === undefined ? "—" : `${v.toFixed(digits)} min`),
  pct: (v: number | null | undefined, digits = 0) => (v === null || v === undefined ? "—" : `${v.toFixed(digits)}%`),
  ratio: (v: number | null | undefined) => (v === null || v === undefined ? "—" : v.toFixed(2)),
};

/** Round axis ticks: 0..max in 1/2/5 x 10^n steps (never 65s, 130s, 195s). */
export function niceTicks(max: number, target = 4): number[] {
  if (!Number.isFinite(max) || max <= 0) return [0, 1];
  const raw = max / target;
  const pow = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * pow).find((s) => s >= raw) ?? 10 * pow;
  const ticks: number[] = [];
  for (let t = 0; t < max + step * 0.999; t += step) ticks.push(Number(t.toFixed(6)));
  return ticks;
}
