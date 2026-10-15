import { useState } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmt, niceTicks } from "../../lib/format";
import { groupSummaries, routesIn, summarizeService, type ServiceSummary } from "../../lib/metrics";
import { ROUTE_NAMES } from "../../lib/routes";
import type { DailyLineMetrics } from "../../lib/types";
import { Card, EmptyState } from "../ui/Card";
import { TooltipBox } from "../ui/ChartTooltip";
import { Key } from "../ui/Legend";
import { Segmented } from "../ui/Segmented";

type Metric = "headway" | "cv" | "mae" | "within1";
const METRICS: Record<Metric, { label: string; get: (s: ServiceSummary) => number | null; format: (v: number | null) => string }> = {
  headway: { label: "Avg headway", get: (s) => s.avgHeadway, format: (v) => fmt.minutes(v) },
  cv: { label: "Regularity (CV)", get: (s) => s.headwayCv, format: fmt.ratio },
  mae: { label: "Avg error", get: (s) => s.maeSeconds, format: fmt.seconds },
  within1: { label: "Within 1 min", get: (s) => s.pctWithin1, format: (v) => fmt.pct(v) },
};

/** Rush hour vs the rest, per line: two steps of one hue (ordered comparison, not identity). */
export function RushComparison({ rows }: { rows: DailyLineMetrics[] }) {
  const [metric, setMetric] = useState<Metric>("headway");
  const m = METRICS[metric];
  const routes = routesIn(rows);
  const groups = groupSummaries(rows, (r) => `${r.route_code}|${r.is_rush_hour ? "rush" : "off"}`, summarizeService);
  const data = routes.map((r) => ({
    route: ROUTE_NAMES[r],
    rush: m.get(groups.get(`${r}|rush`) ?? summarizeService([])),
    off: m.get(groups.get(`${r}|off`) ?? summarizeService([])),
  }));
  const hasRush = rows.some((r) => r.is_rush_hour);
  const yMax = Math.max(0, ...data.flatMap((d) => [d.rush ?? 0, d.off ?? 0]));
  const yTicks = metric === "within1" ? [0, 25, 50, 75, 100] : niceTicks(yMax);

  return (
    <Card
      title="Rush hour vs the rest of the day"
      subtitle="Weekday 6–9 AM and 4–7 PM (Chicago) compared with all other times"
      actions={<Segmented label="Comparison metric" value={metric} onChange={setMetric}
        options={(Object.keys(METRICS) as Metric[]).map((k) => ({ value: k, label: METRICS[k].label }))} />}
      table={{
        rows: data,
        columns: [
          { key: "l", header: "Line", render: (d) => d.route },
          { key: "r", header: "Rush hour", align: "right", render: (d) => m.format(d.rush) },
          { key: "o", header: "Other times", align: "right", render: (d) => m.format(d.off) },
        ],
      }}
      footer={!hasRush ? "No rush-hour data in this slice." : !rows.some((r) => !r.is_rush_hour) ? "No off-peak data in this slice yet - collect outside rush hours to compare." : undefined}
    >
      {!routes.length ? (
        <EmptyState>No data for these filters.</EmptyState>
      ) : (
        <>
          <ul className="mb-2 flex gap-4 text-[12px] text-ink-2" aria-label="Legend">
            <li className="flex items-center gap-1.5"><Key color="var(--ord-strong)" mark="rect" />Rush hour</li>
            <li className="flex items-center gap-1.5"><Key color="var(--ord-light)" mark="rect" />Other times</li>
          </ul>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={data} barGap={2} barCategoryGap="30%" margin={{ top: 8, right: 8, bottom: 0, left: -8 }}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="route" tickLine={false} />
              <YAxis tickLine={false} axisLine={false} width={48} ticks={yTicks} domain={[0, yTicks[yTicks.length - 1]]} />
              <Tooltip cursor={{ fill: "var(--surface-2)" }}
                content={({ active, payload, label }) => active && payload?.length ? (
                  <TooltipBox title={`${label} Line · ${m.label}`} rows={payload.map((p) => ({
                    key: String(p.dataKey), color: p.dataKey === "rush" ? "var(--ord-strong)" : "var(--ord-light)",
                    label: p.dataKey === "rush" ? "Rush hour" : "Other times", value: m.format(p.value as number | null),
                  }))} />
                ) : null} />
              <Bar dataKey="rush" fill="var(--ord-strong)" radius={[4, 4, 0, 0]} maxBarSize={24} isAnimationActive={false} />
              <Bar dataKey="off" fill="var(--ord-light)" radius={[4, 4, 0, 0]} maxBarSize={24} isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </>
      )}
    </Card>
  );
}
