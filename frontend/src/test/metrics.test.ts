import { describe, expect, it } from "vitest";
import {
  DEFAULT_FILTERS,
  MIN_HEADWAYS,
  MIN_SCORED_PREDICTIONS,
  accuracyByHorizon,
  dateFloor,
  filterRows,
  ifEnoughHeadways,
  ifEnoughPredictions,
  seriesBy,
  summarizeAccuracy,
  summarizeService,
} from "../lib/metrics";
import { formatLocalTime, localMinutes } from "../lib/format";
import type { DailyAccuracyByHorizon, DailyLineMetrics } from "../lib/types";

function daily(over: Partial<DailyLineMetrics>): DailyLineMetrics {
  return {
    service_date: "2026-10-15", station_id: "41320", route_code: "Red", day_type: "weekday",
    time_period: "am_peak", is_rush_hour: true, train_visits: 10, inferred_arrivals: 8,
    vanished_before_arrival: 1, flagged_delayed_visits: 2, scored_predictions: 100,
    abs_error_sum_seconds: 3000, error_sum_seconds: -1000, within_1_min: 70, within_2_min: 90,
    reliable_headways: 0, headway_sum_minutes: 0, headway_sumsq_minutes: 0, bunched_arrivals: 0,
    unexpected_gaps: 0, ...over,
  };
}

function headwayRow(values: number[], over: Partial<DailyLineMetrics> = {}): DailyLineMetrics {
  return daily({
    reliable_headways: values.length,
    headway_sum_minutes: values.reduce((a, b) => a + b, 0),
    headway_sumsq_minutes: values.reduce((a, b) => a + b * b, 0),
    ...over,
  });
}

describe("exact re-aggregation", () => {
  it("weights accuracy by prediction count, not by row", () => {
    const s = summarizeAccuracy([
      daily({ scored_predictions: 100, abs_error_sum_seconds: 3000, within_1_min: 70 }),
      daily({ scored_predictions: 300, abs_error_sum_seconds: 3000, within_1_min: 300 }),
    ]);
    expect(s.scored).toBe(400);
    expect(s.maeSeconds).toBe(15); // 6000 / 400, not the mean of 30 and 10
    expect(s.pctWithin1).toBeCloseTo(92.5);
  });

  it("combines headway mean and stddev exactly across rows", () => {
    const a = [4, 6, 5], b = [10, 2];
    const all = [...a, ...b];
    const mean = all.reduce((x, y) => x + y) / all.length;
    const sd = Math.sqrt(all.reduce((acc, x) => acc + (x - mean) ** 2, 0) / (all.length - 1));
    const s = summarizeService([headwayRow(a), headwayRow(b, { time_period: "midday", is_rush_hour: false })]);
    expect(s.headways).toBe(5);
    expect(s.avgHeadway).toBeCloseTo(mean);
    expect(s.stddevHeadway).toBeCloseTo(sd);
    expect(s.headwayCv).toBeCloseTo(sd / mean);
  });

  it("returns nulls, not NaN, for empty slices", () => {
    const s = summarizeService([daily({ scored_predictions: 0, train_visits: 0 })]);
    expect(s.maeSeconds).toBeNull();
    expect(s.avgHeadway).toBeNull();
    expect(s.headwayCv).toBeNull();
    expect(s.delayedPct).toBeNull();
  });

  it("needs two headways for a standard deviation", () => {
    expect(summarizeService([headwayRow([6])]).stddevHeadway).toBeNull();
  });
});

describe("filters", () => {
  const rows = [
    daily({ service_date: "2026-10-01" }),
    daily({ service_date: "2026-10-14", route_code: "Brn" }),
    daily({ service_date: "2026-10-15", time_period: "midday", is_rush_hour: false }),
    daily({ service_date: "2026-10-11", day_type: "sunday", is_rush_hour: false }),
  ];

  it("date presets are relative to the newest service date", () => {
    expect(dateFloor("last_day", "2026-10-15")).toBe("2026-10-15");
    expect(dateFloor("last_7", "2026-10-15")).toBe("2026-10-09");
    expect(dateFloor("last_30", "2026-03-15")).toBe("2026-02-14");
    expect(dateFloor("all", "2026-10-15")).toBeNull();
    expect(filterRows(rows, { ...DEFAULT_FILTERS, datePreset: "last_7" }, "2026-10-15")).toHaveLength(3);
  });

  it("filters by line, day type and time", () => {
    expect(filterRows(rows, { ...DEFAULT_FILTERS, routes: ["Brn"] }, null)).toHaveLength(1);
    expect(filterRows(rows, { ...DEFAULT_FILTERS, dayType: "sunday" }, null)).toHaveLength(1);
    expect(filterRows(rows, { ...DEFAULT_FILTERS, time: "rush" }, null)).toHaveLength(2);
    expect(filterRows(rows, { ...DEFAULT_FILTERS, time: "midday" }, null)).toHaveLength(1);
  });
});

describe("chart shaping", () => {
  it("orders horizons and keeps per-route sample sizes", () => {
    const h = (bucket: DailyAccuracyByHorizon["horizon_bucket"], route: DailyAccuracyByHorizon["route_code"], n: number) =>
      ({ ...daily({ route_code: route }), horizon_bucket: bucket, scored_predictions: n,
         abs_error_sum_seconds: n * 30 } as DailyAccuracyByHorizon);
    const { routes, data } = accuracyByHorizon([h("20+ min", "P", 400), h("00-02 min", "Red", 200)], "mae");
    expect(routes).toEqual(["Red", "P"]); // canonical line order, not data order
    expect(data.map((d) => d.bucket)[0]).toBe("00-02 min");
    expect(data[0].Red).toBe(30);
    expect(data[0].P).toBeNull();
    expect(data[0]["Red__n"]).toBe(200);
  });

  it("hides accuracy resting on too few predictions, but keeps the count", () => {
    const h = (n: number) => ({ ...daily({ route_code: "Y" }), horizon_bucket: "00-02 min", scored_predictions: n,
      abs_error_sum_seconds: n * 600 } as DailyAccuracyByHorizon);
    const small = accuracyByHorizon([h(MIN_SCORED_PREDICTIONS - 1)], "mae").data[0];
    expect(small.Y).toBeNull();
    expect(small["Y__n"]).toBe(MIN_SCORED_PREDICTIONS - 1);
    expect(accuracyByHorizon([h(MIN_SCORED_PREDICTIONS)], "mae").data[0].Y).toBe(600);
  });

  it("gates each figure on its own sample: predictions for accuracy, headways for headways", () => {
    const s = summarizeService([headwayRow([5, 6, 7], { scored_predictions: MIN_SCORED_PREDICTIONS })]);
    expect(ifEnoughPredictions(s, s.maeSeconds)).toBe(30);
    expect(ifEnoughHeadways(s, s.avgHeadway)).toBeNull(); // 3 headways < MIN_HEADWAYS
    const many = summarizeService([headwayRow(Array(MIN_HEADWAYS).fill(6), { scored_predictions: 1 })]);
    expect(ifEnoughHeadways(many, many.avgHeadway)).toBe(6);
    expect(ifEnoughPredictions(many, many.maeSeconds)).toBeNull();
  });

  it("builds time-of-day series in period order", () => {
    const { data } = seriesBy(
      [daily({ time_period: "midday" }), daily({ time_period: "am_peak" })],
      "time_period",
      (s) => s.maeSeconds,
    );
    expect(data.map((d) => d.key)).toEqual(["am_peak", "midday"]);
  });
});

describe("formatting", () => {
  it("treats Chicago wall-clock strings as local, without time-zone conversion", () => {
    expect(formatLocalTime("2026-10-15T08:01:05")).toBe("8:01 AM");
    expect(formatLocalTime("2026-10-15T00:05:00")).toBe("12:05 AM");
    expect(formatLocalTime("2026-10-15T13:30:59", true)).toBe("1:30:59 PM");
    expect(localMinutes("2026-10-15T07:30:30")).toBeCloseTo(450.5);
  });
});

describe("axis ticks", () => {
  it("uses round steps", async () => {
    const { niceTicks } = await import("../lib/format");
    expect(niceTicks(255)).toEqual([0, 100, 200, 300]);
    expect(niceTicks(130)).toEqual([0, 50, 100, 150]);
    expect(niceTicks(0.67)).toEqual([0, 0.2, 0.4, 0.6, 0.8]);
    expect(niceTicks(0)).toEqual([0, 1]);
  });
});
