import { useMemo } from "react";
import {
  Bar, BarChart, CartesianGrid, ReferenceLine, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis,
} from "recharts";
import { fmt, formatClock, formatLocalTime, formatServiceDate, localMinutes } from "../../lib/format";
import { ROUTE_NAMES, routeColor, sortRoutes } from "../../lib/routes";
import type { Headway, RouteCode } from "../../lib/types";
import { Card, EmptyState } from "../ui/Card";
import { TooltipBox } from "../ui/ChartTooltip";
import { RouteLegend } from "../ui/Legend";

const BINS = [0, 2, 4, 6, 8, 10, 12, 15, 20, 30];

/** Gaps between consecutive (inferred) arrivals of the same line at the same platform. */
export function HeadwayCharts({ headways }: { headways: Headway[] }) {
  const routes = sortRoutes(headways.map((h) => h.route_code));
  const points = useMemo(
    () => headways.map((h) => ({ ...h, x: localMinutes(h.arrival_chicago), y: h.headway_minutes })),
    [headways],
  );
  const histogram = useMemo(() => {
    return BINS.map((lo, i) => {
      const hi = BINS[i + 1];
      const row: Record<string, number | string> = { bin: hi ? `${lo}–${hi}` : `${lo}+` };
      for (const r of routes) {
        row[r] = headways.filter((h) => h.route_code === r && h.headway_minutes >= lo && (hi === undefined || h.headway_minutes < hi)).length;
      }
      return row;
    });
  }, [headways, routes]);

  const xs = points.map((p) => p.x);
  const xDomain: [number, number] = xs.length ? [Math.floor(Math.min(...xs) / 60) * 60, Math.ceil(Math.max(...xs) / 60) * 60] : [0, 1440];
  const ticks: number[] = [];
  const step = xDomain[1] - xDomain[0] > 360 ? 120 : 60;
  for (let t = xDomain[0]; t <= xDomain[1]; t += step) ticks.push(t);
  const flagged = headways.filter((h) => h.is_bunched || h.is_unexpected_gap);

  const table = {
    rows: [...headways].reverse(),
    columns: [
      { key: "d", header: "Day", render: (h: Headway) => formatServiceDate(h.service_date) },
      { key: "t", header: "Arrival (inferred)", render: (h: Headway) => formatLocalTime(h.arrival_chicago) },
      { key: "l", header: "Line", render: (h: Headway) => ROUTE_NAMES[h.route_code] },
      { key: "p", header: "Platform", render: (h: Headway) => h.stop_description },
      { key: "h", header: "Headway", align: "right" as const, render: (h: Headway) => fmt.minutes(h.headway_minutes) },
      { key: "f", header: "Flag", render: (h: Headway) => (h.is_bunched ? "Bunched" : h.is_unexpected_gap ? "Unexpected gap" : "") },
    ],
  };

  if (!headways.length) {
    return (
      <Card title="Gaps between trains (headways)">
        <EmptyState>No reliable headways for these filters. Headways need consecutive inferred arrivals with no collection gap between them.</EmptyState>
      </Card>
    );
  }

  return (
    <div className="grid gap-4 xl:grid-cols-5">
      <Card
        className="xl:col-span-3"
        title="Gaps between trains through the day"
        subtitle="Each dot is the time since the previous train of the same line at the same platform"
        table={table}
        footer={`${flagged.length} flagged: ringed dots are bunched (< 2 min) or unexpected gaps (≥ 2× typical). Arrival times are inferred.`}
      >
        <RouteLegend routes={routes} mark="dot" />
        <ResponsiveContainer width="100%" height={280}>
          <ScatterChart margin={{ top: 8, right: 16, bottom: 0, left: -8 }}>
            <CartesianGrid vertical={false} />
            <XAxis type="number" dataKey="x" domain={xDomain} ticks={ticks} tickFormatter={formatClock} tickLine={false} />
            <YAxis type="number" dataKey="y" tickLine={false} axisLine={false} width={44} tickFormatter={(v: number) => `${v}m`} />
            <ReferenceLine y={2} stroke="var(--axis)" label={{ value: "bunching < 2 min", position: "insideTopRight" }} />
            <Tooltip
              cursor={false}
              content={({ active, payload }) => {
                const h = active && payload?.[0]?.payload as (Headway & { x: number }) | undefined;
                if (!h) return null;
                return (
                  <TooltipBox
                    title={`${formatServiceDate(h.service_date)}, ${formatLocalTime(h.arrival_chicago)} · ${h.stop_description}`}
                    rows={[{ key: "h", color: routeColor(h.route_code), label: `${ROUTE_NAMES[h.route_code]} run ${h.run_number}`, value: fmt.minutes(h.headway_minutes) }]}
                    footer={[
                      `after run ${h.previous_run_number}`,
                      h.typical_headway_minutes ? `typical ${fmt.minutes(h.typical_headway_minutes)}` : null,
                      h.is_bunched ? "BUNCHED" : h.is_unexpected_gap ? "UNEXPECTED GAP" : null,
                    ].filter(Boolean).join(" · ")}
                  />
                );
              }}
            />
            {routes.map((r: RouteCode) => (
              <Scatter key={r} data={points.filter((p) => p.route_code === r)} isAnimationActive={false}
                shape={(props: { cx?: number; cy?: number; payload?: Headway }) => {
                  const { cx = 0, cy = 0, payload } = props;
                  const flag = payload?.is_bunched || payload?.is_unexpected_gap;
                  return (
                    <g>
                      <circle cx={cx} cy={cy} r={12} fill="transparent" />{/* 24px hit target */}
                      {flag && <circle cx={cx} cy={cy} r={8} fill="none" stroke="var(--ink)" strokeWidth={1.5} />}
                      <circle cx={cx} cy={cy} r={4.5} fill={routeColor(r)} stroke="var(--surface)" strokeWidth={2} />
                    </g>
                  );
                }} />
            ))}
          </ScatterChart>
        </ResponsiveContainer>
      </Card>

      <Card
        className="xl:col-span-2"
        title="Headway distribution"
        subtitle="How often each gap length occurs (minutes)"
        table={{
          rows: histogram.flatMap((b) => routes.map((r) => ({ bin: b.bin as string, route: r, n: b[r] as number }))).filter((r) => r.n),
          columns: [
            { key: "b", header: "Headway (min)", render: (r) => r.bin },
            { key: "l", header: "Line", render: (r) => ROUTE_NAMES[r.route] },
            { key: "n", header: "Count", align: "right", render: (r) => fmt.int(r.n) },
          ],
        }}
      >
        <RouteLegend routes={routes} mark="rect" />
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={histogram} barGap={2} margin={{ top: 8, right: 8, bottom: 0, left: -16 }}>
            <CartesianGrid vertical={false} />
            <XAxis dataKey="bin" tickLine={false} interval={0} fontSize={10} />
            <YAxis allowDecimals={false} tickLine={false} axisLine={false} width={40} />
            <Tooltip cursor={{ fill: "var(--surface-2)" }}
              content={({ active, payload, label }) => active && payload?.length ? (
                <TooltipBox title={`${label} min`} rows={payload.map((p) => ({
                  key: String(p.dataKey), color: routeColor(p.dataKey as never),
                  label: `${ROUTE_NAMES[p.dataKey as keyof typeof ROUTE_NAMES]} Line`, value: fmt.int(p.value as number),
                }))} />
              ) : null} />
            {routes.map((r) => (
              <Bar key={r} dataKey={r} fill={routeColor(r)} radius={[4, 4, 0, 0]} maxBarSize={24} isAnimationActive={false} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </Card>
    </div>
  );
}
