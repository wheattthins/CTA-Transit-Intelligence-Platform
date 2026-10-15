// Filtering and exact re-aggregation of the additive daily Gold rows.
// Every measure is derived from summed counts/sums, so any filter combination is exact.

import { PERIOD_ORDER, sortRoutes } from "./routes";
import type {
  AccuracySums,
  DailyAccuracyByHorizon,
  DailyDims,
  DailyLineMetrics,
  DayType,
  HorizonBucket,
  RouteCode,
  TimePeriod,
} from "./types";

export type DatePreset = "all" | "last_day" | "last_7" | "last_30";
export type TimeFilter = "all" | "rush" | "non_rush" | TimePeriod;

export interface Filters {
  datePreset: DatePreset;
  routes: RouteCode[]; // empty = all
  stationId: string | "all";
  dayType: DayType | "all";
  time: TimeFilter;
}

export const DEFAULT_FILTERS: Filters = { datePreset: "all", routes: [], stationId: "all", dayType: "all", time: "all" };

export const HORIZON_ORDER: HorizonBucket[] = ["00-02 min", "02-05 min", "05-10 min", "10-15 min", "15-20 min", "20+ min"];

/** Earliest service date included by a preset, relative to the latest date in the data. */
export function dateFloor(preset: DatePreset, lastServiceDate: string | null): string | null {
  if (!lastServiceDate || preset === "all") return null;
  const days = preset === "last_day" ? 0 : preset === "last_7" ? 6 : 29;
  const d = new Date(`${lastServiceDate}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

export function matches(row: DailyDims, f: Filters, floor: string | null): boolean {
  if (floor && row.service_date < floor) return false;
  if (f.routes.length && !f.routes.includes(row.route_code)) return false;
  if (f.stationId !== "all" && row.station_id !== f.stationId) return false;
  if (f.dayType !== "all" && row.day_type !== f.dayType) return false;
  if (f.time === "rush" && !row.is_rush_hour) return false;
  if (f.time === "non_rush" && row.is_rush_hour) return false;
  if (f.time !== "all" && f.time !== "rush" && f.time !== "non_rush" && row.time_period !== f.time) return false;
  return true;
}

export function filterRows<T extends DailyDims>(rows: T[], f: Filters, lastServiceDate: string | null): T[] {
  const floor = dateFloor(f.datePreset, lastServiceDate);
  return rows.filter((r) => matches(r, f, floor));
}

// ---------------------------------------------------------------------------- aggregation

export interface AccuracySummary {
  scored: number;
  maeSeconds: number | null; // mean absolute error
  biasSeconds: number | null; // mean signed error (+ = trains came sooner than predicted)
  pctWithin1: number | null;
  pctWithin2: number | null;
}

export function summarizeAccuracy(rows: AccuracySums[]): AccuracySummary {
  const n = sum(rows, (r) => r.scored_predictions);
  if (!n) return { scored: 0, maeSeconds: null, biasSeconds: null, pctWithin1: null, pctWithin2: null };
  return {
    scored: n,
    maeSeconds: sum(rows, (r) => r.abs_error_sum_seconds) / n,
    biasSeconds: sum(rows, (r) => r.error_sum_seconds) / n,
    pctWithin1: (100 * sum(rows, (r) => r.within_1_min)) / n,
    pctWithin2: (100 * sum(rows, (r) => r.within_2_min)) / n,
  };
}

export interface ServiceSummary extends AccuracySummary {
  trainVisits: number;
  inferredArrivals: number;
  vanished: number;
  delayedPct: number | null;
  headways: number;
  avgHeadway: number | null;
  stddevHeadway: number | null;
  headwayCv: number | null; // stddev / mean: 0 = perfectly even spacing
  bunched: number;
  bunchingPct: number | null;
  unexpectedGaps: number;
}

export function summarizeService(rows: DailyLineMetrics[]): ServiceSummary {
  const n = sum(rows, (r) => r.reliable_headways);
  const s1 = sum(rows, (r) => r.headway_sum_minutes);
  const s2 = sum(rows, (r) => r.headway_sumsq_minutes);
  const mean = n ? s1 / n : null;
  // Sample variance from sums; clamp tiny negative values from floating-point error.
  const variance = n > 1 ? Math.max(0, (s2 - (s1 * s1) / n) / (n - 1)) : null;
  const stddev = variance === null ? null : Math.sqrt(variance);
  const visits = sum(rows, (r) => r.train_visits);
  const bunched = sum(rows, (r) => r.bunched_arrivals);
  return {
    ...summarizeAccuracy(rows),
    trainVisits: visits,
    inferredArrivals: sum(rows, (r) => r.inferred_arrivals),
    vanished: sum(rows, (r) => r.vanished_before_arrival),
    delayedPct: visits ? (100 * sum(rows, (r) => r.flagged_delayed_visits)) / visits : null,
    headways: n,
    avgHeadway: mean,
    stddevHeadway: stddev,
    headwayCv: mean && stddev !== null ? stddev / mean : null,
    bunched,
    bunchingPct: n ? (100 * bunched) / n : null,
    unexpectedGaps: sum(rows, (r) => r.unexpected_gaps),
  };
}

/** Group rows by a key and summarize each group. */
export function groupSummaries<T, K extends string, S>(
  rows: T[],
  key: (r: T) => K,
  summarize: (rows: T[]) => S,
): Map<K, S> {
  const groups = new Map<K, T[]>();
  for (const r of rows) {
    const k = key(r);
    const g = groups.get(k);
    if (g) g.push(r);
    else groups.set(k, [r]);
  }
  return new Map([...groups].map(([k, g]) => [k, summarize(g)]));
}

export function routesIn(rows: { route_code: RouteCode }[]): RouteCode[] {
  return sortRoutes(rows.map((r) => r.route_code));
}

/** Accuracy by lead time: one row per horizon bucket, one numeric field per route. */
export function accuracyByHorizon(rows: DailyAccuracyByHorizon[], metric: "mae" | "within1") {
  const routes = routesIn(rows);
  const byKey = groupSummaries(rows, (r) => `${r.horizon_bucket}|${r.route_code}`, summarizeAccuracy);
  return {
    routes,
    data: HORIZON_ORDER.map((bucket) => {
      const row: Record<string, number | string | null> = { bucket };
      for (const route of routes) {
        const s = byKey.get(`${bucket}|${route}`);
        row[route] = s ? (metric === "mae" ? s.maeSeconds : s.pctWithin1) : null;
        row[`${route}__n`] = s?.scored ?? 0;
      }
      return row;
    }),
  };
}

/** Per-route series over service dates or time periods. */
export function seriesBy(
  rows: DailyLineMetrics[],
  axis: "service_date" | "time_period",
  value: (s: ServiceSummary) => number | null,
) {
  const routes = routesIn(rows);
  const keys = axis === "service_date"
    ? [...new Set(rows.map((r) => r.service_date))].sort()
    : PERIOD_ORDER.filter((p) => rows.some((r) => r.time_period === p));
  const byKey = groupSummaries(rows, (r) => `${r[axis]}|${r.route_code}`, summarizeService);
  return {
    routes,
    data: keys.map((k) => {
      const row: Record<string, number | string | null> = { key: k };
      for (const route of routes) {
        const s = byKey.get(`${k}|${route}`);
        row[route] = s ? value(s) : null;
      }
      return row;
    }),
  };
}

function sum<T>(rows: T[], f: (r: T) => number): number {
  let total = 0;
  for (const r of rows) total += f(r) || 0;
  return total;
}
