import { useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmt, formatServiceDate, niceTicks } from "../../lib/format";
import {
  MIN_HEADWAYS,
  MIN_SCORED_PREDICTIONS,
  ifEnoughHeadways,
  ifEnoughPredictions,
  seriesBy,
  type ServiceSummary,
} from "../../lib/metrics";
import { PERIOD_SHORT, ROUTE_NAMES, routeColor } from "../../lib/routes";
import type { DailyLineMetrics, TimePeriod } from "../../lib/types";
import { Card, EmptyState } from "../ui/Card";
import { TooltipBox } from "../ui/ChartTooltip";
import { RouteLegend } from "../ui/Legend";
import { Segmented } from "../ui/Segmented";

type Metric = "mae" | "within1" | "headway" | "cv";
const METRICS: Record<Metric, { label: string; value: (s: ServiceSummary) => number | null; format: (v: number | null) => string; unit: string }> = {
  mae: { label: "Avg prediction error", value: (s) => ifEnoughPredictions(s, s.maeSeconds), format: fmt.seconds, unit: "s" },
  within1: { label: "Within 1 minute", value: (s) => ifEnoughPredictions(s, s.pctWithin1), format: (v) => fmt.pct(v), unit: "%" },
  headway: { label: "Avg headway", value: (s) => ifEnoughHeadways(s, s.avgHeadway), format: (v) => fmt.minutes(v), unit: "m" },
  cv: { label: "Headway regularity (CV)", value: (s) => ifEnoughHeadways(s, s.headwayCv), format: fmt.ratio, unit: "" },
};

export function ReliabilityTrend({ rows }: { rows: DailyLineMetrics[] }) {
  const days = new Set(rows.map((r) => r.service_date)).size;
  const [axis, setAxis] = useState<"service_date" | "time_period">(days > 1 ? "service_date" : "time_period");
  const [metric, setMetric] = useState<Metric>("mae");
  const m = METRICS[metric];
  const { routes, data } = seriesBy(rows, axis, m.value);
  const label = (k: string) => (axis === "service_date" ? formatServiceDate(k) : PERIOD_SHORT[k as TimePeriod]);
  const plotted = data.filter((d) => routes.some((r) => d[r] !== null));
  const charted = routes.filter((r) => data.some((d) => d[r] !== null)); // lines with at least one point
  const yMax = Math.max(0, ...data.flatMap((d) => routes.map((r) => (d[r] as number | null) ?? 0)));
  const yTicks = metric === "within1" ? [0, 25, 50, 75, 100] : niceTicks(yMax);

  return (
    <Card
      title="Service reliability over time"
      subtitle={`${m.label} by line, ${axis === "service_date" ? "per service day" : "by time of day (Chicago)"}`}
      actions={
        <>
          <select aria-label="Reliability metric" value={metric} onChange={(e) => setMetric(e.target.value as Metric)}
            className="rounded-lg border border-hairline bg-surface px-2 py-1 text-[12px] text-ink">
            {Object.entries(METRICS).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
          </select>
          <Segmented label="Time axis" value={axis} onChange={setAxis}
            options={[{ value: "service_date", label: "By day" }, { value: "time_period", label: "By time of day" }]} />
        </>
      }
      table={{
        rows: data.flatMap((d) => routes.map((r) => ({ key: d.key as string, route: r, v: d[r] as number | null }))).filter((r) => r.v !== null),
        columns: [
          { key: "k", header: axis === "service_date" ? "Service day" : "Time of day", render: (r) => label(r.key) },
          { key: "l", header: "Line", render: (r) => ROUTE_NAMES[r.route] },
          { key: "v", header: m.label, align: "right", render: (r) => m.format(r.v) },
        ],
      }}
      footer={[
        axis === "service_date" && days < 2 ? "Only one service day collected so far - the daily trend fills in as collection continues." : "",
        `Points resting on fewer than ${MIN_SCORED_PREDICTIONS} scored predictions (${MIN_HEADWAYS} headways for headway metrics) are left out.`,
      ].filter(Boolean).join(" ")}
    >
      {plotted.length === 0 ? (
        <EmptyState>Not enough data for this metric and filter combination.</EmptyState>
      ) : (
        <>
          <RouteLegend routes={charted} mark="line" />
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: -8 }}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="key" tickFormatter={label} tickLine={false} />
              <YAxis tickLine={false} axisLine={false} width={48} ticks={yTicks} domain={[0, yTicks[yTicks.length - 1]]}
                tickFormatter={(v: number) => (metric === "cv" ? v.toFixed(1) : `${v}${m.unit}`)} />
              <Tooltip
                cursor={{ stroke: "var(--axis)", strokeWidth: 1 }}
                content={({ active, payload, label: l }) =>
                  active && payload?.length ? (
                    <TooltipBox title={label(String(l))} rows={payload.filter((p) => p.value !== null).map((p) => ({
                      key: String(p.dataKey), color: routeColor(p.dataKey as never),
                      label: `${ROUTE_NAMES[p.dataKey as keyof typeof ROUTE_NAMES]} Line`, value: m.format(p.value as number),
                    }))} />
                  ) : null
                }
              />
              {charted.map((r) => (
                <Line key={r} type="linear" dataKey={r} stroke={routeColor(r)} strokeWidth={2} strokeLinecap="round"
                  strokeLinejoin="round" connectNulls={false} isAnimationActive={false}
                  dot={{ r: 4, fill: routeColor(r), stroke: "var(--surface)", strokeWidth: 2 }}
                  activeDot={{ r: 5, fill: routeColor(r), stroke: "var(--surface)", strokeWidth: 2 }} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </>
      )}
    </Card>
  );
}
