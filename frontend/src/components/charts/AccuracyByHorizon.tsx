import { useState } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmt, niceTicks } from "../../lib/format";
import { accuracyByHorizon } from "../../lib/metrics";
import { ROUTE_NAMES, routeColor } from "../../lib/routes";
import type { DailyAccuracyByHorizon, RouteCode } from "../../lib/types";
import { Card, EmptyState } from "../ui/Card";
import { TooltipBox } from "../ui/ChartTooltip";
import { RouteLegend } from "../ui/Legend";
import { Segmented } from "../ui/Segmented";

type Metric = "mae" | "within1";

export function AccuracyByHorizon({ rows }: { rows: DailyAccuracyByHorizon[] }) {
  const [metric, setMetric] = useState<Metric>("mae");
  const { routes, data } = accuracyByHorizon(rows, metric);
  const format = metric === "mae" ? fmt.seconds : (v: number | null) => fmt.pct(v);
  const yMax = Math.max(0, ...data.flatMap((d) => routes.map((r) => (d[r] as number | null) ?? 0)));
  const yTicks = metric === "within1" ? [0, 25, 50, 75, 100] : niceTicks(yMax);
  const tableRows = data
    .flatMap((d) =>
      routes.map((r) => ({ bucket: d.bucket as string, route: r, value: d[r] as number | null, n: d[`${r}__n`] as number })),
    )
    .filter((r) => r.n > 0);

  return (
    <Card
      title="How accurate are predictions, by lead time?"
      subtitle={
        metric === "mae"
          ? "Average error vs the inferred arrival, by how far ahead CTA made the prediction"
          : "Share of predictions within 1 minute of the inferred arrival, by lead time"
      }
      actions={
        <Segmented
          label="Accuracy metric"
          value={metric}
          onChange={setMetric}
          options={[{ value: "mae", label: "Avg error" }, { value: "within1", label: "Within 1 min" }]}
        />
      }
      table={{
        rows: tableRows,
        columns: [
          { key: "b", header: "Lead time", render: (r) => r.bucket },
          { key: "l", header: "Line", render: (r) => ROUTE_NAMES[r.route] },
          { key: "v", header: metric === "mae" ? "Avg error" : "Within 1 min", align: "right", render: (r) => format(r.value) },
          { key: "n", header: "Predictions", align: "right", render: (r) => fmt.int(r.n) },
        ],
      }}
      footer="Scored: live predictions on high/medium-confidence inferred arrivals. Small samples are noisy - check the counts in the table."
    >
      {tableRows.length === 0 ? (
        <EmptyState>No scored predictions for these filters yet.</EmptyState>
      ) : (
        <>
          <RouteLegend routes={routes} mark="rect" />
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={data} barGap={2} barCategoryGap="22%" margin={{ top: 8, right: 8, bottom: 0, left: -8 }}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="bucket" tickLine={false} />
              <YAxis
                tickLine={false}
                axisLine={false}
                width={48}
                tickFormatter={(v: number) => (metric === "mae" ? `${v}s` : `${v}%`)}
                ticks={yTicks}
                domain={[0, yTicks[yTicks.length - 1]]}
              />
              <Tooltip
                cursor={{ fill: "var(--surface-2)" }}
                content={({ active, payload, label }) =>
                  active && payload?.length ? (
                    <TooltipBox
                      title={`Lead time ${label}`}
                      rows={payload
                        .filter((p) => p.value !== null && p.value !== undefined)
                        .map((p) => {
                          const route = p.dataKey as RouteCode;
                          return {
                            key: route,
                            color: routeColor(route),
                            label: `${ROUTE_NAMES[route]} Line`,
                            value: format(p.value as number),
                            note: `n=${(p.payload as Record<string, number>)[`${route}__n`]}`,
                          };
                        })}
                    />
                  ) : null
                }
              />
              {routes.map((r) => (
                <Bar key={r} dataKey={r} fill={routeColor(r)} radius={[4, 4, 0, 0]} maxBarSize={24} isAnimationActive={false} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </>
      )}
    </Card>
  );
}
