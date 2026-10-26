import { useMemo } from "react";
import {
  CartesianGrid, ReferenceArea, ReferenceLine, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis,
} from "recharts";
import { fmt, formatClock, formatLocalTime, formatServiceDate, localMinutes } from "../../lib/format";
import { ROUTE_NAMES, routeColor, sortRoutes } from "../../lib/routes";
import type { Headway, RouteCode } from "../../lib/types";
import { Card, EmptyState } from "../ui/Card";
import { TooltipBox } from "../ui/ChartTooltip";
import { DataTable } from "../ui/DataTable";
import { Key } from "../ui/Legend";

const Y_MIN = 0.5;
const Y_TOP = 60; // gaps longer than this are pinned to the top edge as ▲
const Y_DOMAIN: [number, number] = [0.4, Y_TOP];
const Y_TICKS = [2, 5, 15, 60];
const BUNCHED = 2;
const MIN_RATE_N = 10; // below this many headways, a percentage says more about luck than service

type Point = Headway & { x: number; y: number; flag: boolean; pinned: boolean };
type MedianPoint = { x: number; y: number };
type LineStats = { route: RouteCode; n: number; typical: number; p90: number; bunchedPct: number; gapPct: number };

/** Linearly interpolated quantile (q = 0.5 is the usual median). */
const quantile = (values: number[], q: number) => {
  const sorted = [...values].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  return sorted[lo] + (sorted[Math.ceil(pos)] - sorted[lo]) * (pos - lo);
};
const median = (values: number[]) => quantile(values, 0.5);
const pct = (v: number) => fmt.pct(v, v > 0 && v < 1 ? 1 : 0);

/** Gaps between consecutive (inferred) arrivals of the same line at the same platform. */
export function HeadwayCharts({ headways }: { headways: Headway[] }) {
  const routes = sortRoutes(headways.map((h) => h.route_code));
  const points = useMemo(
    () => headways.map((h, i): Point => {
      // Arrivals are inferred from a once-a-minute poll, so headways stack on whole minutes.
      // Spread each dot within ±half a minute (deterministically) without crossing the bunching line.
      const jitter = (((i * 2654435761) >>> 0) % 1000) / 1000 - 0.5;
      const y = h.headway_minutes + jitter;
      const pinned = h.headway_minutes > Y_TOP;
      return {
        ...h,
        // After-midnight arrivals belong to the end of the service day, not its start.
        x: localMinutes(h.arrival_chicago) + (h.arrival_chicago.slice(0, 10) > h.service_date ? 1440 : 0),
        // Log scale needs a positive value. Reflect (rather than clamp) at the bunching line so dots don't pile up along it.
        y: pinned ? Y_TOP * 0.85
          : h.is_bunched ? Math.max(y >= BUNCHED ? 2 * BUNCHED - y - 0.05 : y, Y_MIN)
          : y < BUNCHED ? 2 * BUNCHED - y : y,
        flag: h.is_bunched || h.is_unexpected_gap,
        pinned,
      };
    }),
    [headways],
  );
  // Window the time axis to hours with real service; a handful of stragglers would otherwise
  // stretch every panel. Anything outside stays in the table view and is counted in the footer.
  const xDomain = useMemo((): [number, number] => {
    if (!points.length) return [0, 1440];
    const perHour = new Map<number, number>();
    for (const p of points) perHour.set(Math.floor(p.x / 60), (perHour.get(Math.floor(p.x / 60)) ?? 0) + 1);
    const floor = Math.max(3, Math.max(...perHour.values()) * 0.05);
    const busy = [...perHour].filter(([, n]) => n >= floor).map(([hour]) => hour);
    return [Math.min(...busy) * 60, (Math.max(...busy) + 1) * 60];
  }, [points]);
  const shown = points.filter((p) => p.x >= xDomain[0] && p.x <= xDomain[1]);
  // Typical (median) headway per line for each hour with enough trains to be meaningful.
  const medians = useMemo(() => {
    const byKey = new Map<string, number[]>();
    for (const p of shown) {
      const key = `${p.route_code}|${Math.floor(p.x / 60)}`;
      byKey.set(key, [...(byKey.get(key) ?? []), p.headway_minutes]);
    }
    const out = new Map<RouteCode, MedianPoint[]>();
    for (const [key, ys] of byKey) {
      if (ys.length < 5) continue;
      const [route, hour] = key.split("|") as [RouteCode, string];
      out.set(route, [...(out.get(route) ?? []), { x: Number(hour) * 60 + 30, y: Math.min(Math.max(median(ys), Y_MIN), Y_TOP) }]);
    }
    for (const line of out.values()) line.sort((a, b) => a.x - b.x);
    return out;
  }, [shown]);
  // Per-line figures over every headway (not just the plotted window), shared by the panel headers and the summary table.
  const stats = useMemo(() => routes.map((route): LineStats => {
    const rows = headways.filter((h) => h.route_code === route);
    const mins = rows.map((h) => h.headway_minutes);
    return {
      route,
      n: rows.length,
      typical: median(mins),
      p90: quantile(mins, 0.9),
      bunchedPct: (100 * rows.filter((h) => h.is_bunched).length) / rows.length,
      gapPct: (100 * rows.filter((h) => h.is_unexpected_gap).length) / rows.length,
    };
  }), [headways, routes]);
  const maxBunched = Math.max(1, ...stats.filter((s) => s.n >= MIN_RATE_N).map((s) => s.bunchedPct));
  const maxGap = Math.max(1, ...stats.filter((s) => s.n >= MIN_RATE_N).map((s) => s.gapPct));

  // Few ticks: each panel is narrow.
  const ticks: number[] = [];
  const step = xDomain[1] - xDomain[0] > 360 ? 240 : 120;
  for (let t = Math.ceil(xDomain[0] / step) * step; t <= xDomain[1]; t += step) ticks.push(t);
  const flagged = headways.filter((h) => h.is_bunched || h.is_unexpected_gap);
  const hidden = points.length - shown.length;
  const pinned = shown.filter((p) => p.pinned).length;

  const table = {
    rows: [...headways].reverse(),
    columns: [
      { key: "d", header: "Day", render: (h: Headway) => formatServiceDate(h.service_date) },
      { key: "t", header: "Arrival (inferred)", render: (h: Headway) => formatLocalTime(h.arrival_chicago) },
      { key: "l", header: "Line", render: (h: Headway) => ROUTE_NAMES[h.route_code] },
      { key: "p", header: "Platform", render: (h: Headway) => h.stop_description },
      { key: "h", header: "Headway", align: "right" as const, render: (h: Headway) => fmt.minutes(h.headway_minutes) },
      { key: "f", header: "Flag", render: (h: Headway) => (h.is_bunched ? "Bunched" : h.is_unexpected_gap ? "Long gap" : "") },
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
        subtitle="Each dot is one gap between trains at a platform; the line is the typical gap by hour"
        table={table}
        footer={[
          `${flagged.length} flagged (solid dots): bunched under 2 min (shaded band) or long gaps ≥ 2× typical.`,
          pinned ? `▲ ${pinned} gap${pinned === 1 ? "" : "s"} over 1 h, drawn at the top.` : null,
          hidden ? `${hidden} headway${hidden === 1 ? "" : "s"} outside ${formatClock(xDomain[0])}–${formatClock(xDomain[1])} are in the table.` : null,
          "Log scale; arrival times inferred to the nearest minute.",
        ].filter(Boolean).join(" ")}
      >
        {/* Small multiples: eight overlapping colors can't be told apart in one plot, so each line gets its own panel on shared axes. */}
        <div className="grid grid-cols-2 gap-x-4 gap-y-4 sm:grid-cols-4">
          {stats.map((s) => (
            <HeadwayPanel key={s.route} stats={s} points={shown.filter((p) => p.route_code === s.route)}
              medianLine={medians.get(s.route) ?? []} xDomain={xDomain} xTicks={ticks} />
          ))}
        </div>
      </Card>

      <Card
        className="xl:col-span-2"
        title="Headway summary by line"
        subtitle="How long riders usually wait, how bad it gets, and how often trains bunch or leave long gaps"
        footer={`Typical = median gap; 90th pct = 1 in 10 gaps is longer. Bunched = under 2 min; long gap = at least 2× the platform's typical gap. Rates are hidden for lines with fewer than ${MIN_RATE_N} headways.`}
      >
        <DataTable
          caption="Headway summary by line"
          rows={stats}
          maxHeight={420}
          columns={[
            { key: "l", header: "Line", render: (s) => (
              <span className="flex items-center gap-1.5">
                <Key color={routeColor(s.route)} mark="dot" />
                {ROUTE_NAMES[s.route]}
              </span>
            ) },
            { key: "n", header: "Headways", align: "right", render: (s) => fmt.int(s.n) },
            { key: "t", header: "Typical", align: "right", render: (s) => fmt.minutes(s.typical) },
            { key: "p", header: "90th pct", align: "right", render: (s) => fmt.minutes(s.p90) },
            { key: "b", header: "Bunched", align: "right", render: (s) => <Rate stats={s} value={s.bunchedPct} max={maxBunched} /> },
            { key: "g", header: "Long gaps", align: "right", render: (s) => <Rate stats={s} value={s.gapPct} max={maxGap} /> },
          ]}
        />
      </Card>
    </div>
  );
}

/** One line's headways on the shared axes: faint dots, the hourly median on top, flagged gaps solid. */
function HeadwayPanel({ stats, points, medianLine, xDomain, xTicks }: {
  stats: LineStats;
  points: Point[];
  medianLine: MedianPoint[];
  xDomain: [number, number];
  xTicks: number[];
}) {
  const { route } = stats;
  const color = routeColor(route);
  const dots = (flag: boolean) => (
    <Scatter data={points.filter((p) => p.flag === flag)} isAnimationActive={false}
      shape={(props: { cx?: number; cy?: number; payload?: Point }) => {
        const { cx = 0, cy = 0, payload } = props;
        return (
          <g>
            <circle cx={cx} cy={cy} r={flag ? 8 : 5} fill="transparent" />{/* hover target */}
            {payload?.pinned
              ? <path d={`M${cx},${cy - 5} L${cx + 4.5},${cy + 3} L${cx - 4.5},${cy + 3} Z`} fill={color} stroke="var(--surface)" strokeWidth={1} />
              : <circle cx={cx} cy={cy} r={flag ? 3 : 2.25} fill={color} fillOpacity={flag ? 1 : 0.25}
                  stroke={flag ? "var(--surface)" : "none"} strokeWidth={1} />}
          </g>
        );
      }} />
  );

  return (
    <figure className="min-w-0">
      <figcaption className="mb-1.5 text-[12px] leading-tight">
        <span className="flex items-baseline justify-between gap-2">
          <span className="flex items-center gap-1.5 font-semibold text-ink">
            <Key color={color} mark="dot" />
            {ROUTE_NAMES[route]}
          </span>
          <span className="tabular truncate text-[11px] text-ink-muted">typ. {fmt.minutes(stats.typical)}</span>
        </span>
        <span className="tabular mt-0.5 block truncate text-[11px] text-ink-muted">
          {stats.n < MIN_RATE_N
            ? `only ${stats.n} headway${stats.n === 1 ? "" : "s"}, too few for rates`
            : `${pct(stats.bunchedPct)} bunched · ${pct(stats.gapPct)} long gaps`}
        </span>
      </figcaption>
      <ResponsiveContainer width="100%" height={130}>
        <ScatterChart margin={{ top: 6, right: 6, bottom: 0, left: 0 }}>
          <CartesianGrid vertical={false} />
          <ReferenceArea y1={Y_DOMAIN[0]} y2={BUNCHED} fill="var(--surface-2)" fillOpacity={0.8} ifOverflow="hidden" />
          <XAxis type="number" dataKey="x" domain={xDomain} ticks={xTicks} tickFormatter={formatClock}
            allowDataOverflow tickLine={false} fontSize={10} />
          <YAxis type="number" dataKey="y" scale="log" domain={Y_DOMAIN} ticks={Y_TICKS} allowDataOverflow interval={0}
            tickLine={false} axisLine={false} width={30} fontSize={10}
            tickFormatter={(v: number) => (v >= 60 ? `${v / 60}h` : `${v}m`)} />
          <ReferenceLine y={BUNCHED} stroke="var(--axis)" strokeDasharray="3 3" />
          <Tooltip
            cursor={false}
            content={({ active, payload }) => {
              const h = active ? (payload?.[0]?.payload as Point | undefined) : undefined;
              if (!h?.service_date) return null; // median-line points carry no train details
              return (
                <TooltipBox
                  title={`${formatServiceDate(h.service_date)}, ${formatLocalTime(h.arrival_chicago)} · ${h.stop_description}`}
                  rows={[{ key: "h", color, label: `${ROUTE_NAMES[h.route_code]} run ${h.run_number}`, value: fmt.minutes(h.headway_minutes) }]}
                  footer={[
                    `after run ${h.previous_run_number}`,
                    h.typical_headway_minutes ? `typical ${fmt.minutes(h.typical_headway_minutes)}` : null,
                    h.is_bunched ? "BUNCHED" : h.is_unexpected_gap ? "LONG GAP" : null,
                  ].filter(Boolean).join(" · ")}
                />
              );
            }}
          />
          {dots(false)}
          {medianLine.length >= 2 && (
            <Scatter data={medianLine} line={{ stroke: "var(--surface)", strokeWidth: 5 }} shape={() => <g />} isAnimationActive={false} />
          )}
          {medianLine.length >= 2 && (
            <Scatter data={medianLine} line={{ stroke: color, strokeWidth: 2 }} shape={() => <g />} isAnimationActive={false} />
          )}
          {dots(true)}
        </ScatterChart>
      </ResponsiveContainer>
    </figure>
  );
}

/** A rate with a small neutral bar (scaled to the worst line) so the table scans at a glance. */
function Rate({ stats, value, max }: { stats: LineStats; value: number; max: number }) {
  if (stats.n < MIN_RATE_N) return <span className="text-ink-muted" title={`Only ${stats.n} headways`}>—</span>;
  return (
    <span className="inline-flex items-center justify-end gap-2">
      <span aria-hidden className="h-1 w-7 overflow-hidden rounded-full bg-surface-2">
        <span className="block h-full rounded-full bg-ink-muted" style={{ width: `${(100 * value) / max}%` }} />
      </span>
      <span className="min-w-[3ch]">{pct(value)}</span>
    </span>
  );
}
