import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmt, formatInstant, formatLocalTime, formatServiceDate } from "../lib/format";
import type { CoverageHour, PipelineRun, VisitOutcome } from "../lib/types";
import { Card, EmptyState } from "./ui/Card";
import { TooltipBox } from "./ui/ChartTooltip";
import { DataTable } from "./ui/DataTable";
import { StatusBadge } from "./ui/StatusBadge";

/** Freshness and coverage: how much of the clock was observed, and how the pipeline is doing. */
export function DataHealth({ coverage, runs, outcomes }: { coverage: CoverageHour[]; runs: PipelineRun[]; outcomes: VisitOutcome[] }) {
  const hours = coverage.map((c) => ({ ...c, label: `${formatServiceDate(c.service_date)} ${formatLocalTime(c.hour_start_chicago)}` }));
  const totals = { inferred: 0, high: 0, vanished: 0, unresolved: 0 };
  for (const o of outcomes) {
    if (o.outcome === "inferred_arrival") {
      totals.inferred += o.visits;
      if (o.inference_confidence === "high") totals.high += o.visits;
    } else if (o.outcome === "vanished_before_arrival") totals.vanished += o.visits;
    else totals.unresolved += o.visits;
  }
  const all = totals.inferred + totals.vanished + totals.unresolved;

  return (
    <div className="grid gap-4 xl:grid-cols-5">
      <Card
        className="xl:col-span-3"
        title="Collection coverage"
        subtitle="Share of each hour with at least one successful poll. Gaps lower inference confidence."
        table={{
          rows: hours,
          columns: [
            { key: "h", header: "Hour (CT)", render: (c) => c.label },
            { key: "p", header: "Polls", align: "right", render: (c) => fmt.int(c.polls) },
            { key: "f", header: "Failed", align: "right", render: (c) => fmt.int(c.failed_polls) },
            { key: "c", header: "Coverage", align: "right", render: (c) => fmt.pct(c.coverage_pct) },
            { key: "g", header: "Max gap", align: "right", render: (c) => fmt.seconds(c.max_gap_seconds) },
          ],
        }}
      >
        {!hours.length ? (
          <EmptyState>No collection in this range.</EmptyState>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={hours} margin={{ top: 8, right: 8, bottom: 0, left: -12 }}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="label" tickLine={false} interval="preserveStartEnd" minTickGap={24} fontSize={10} />
              <YAxis domain={[0, 100]} ticks={[0, 50, 100]} tickFormatter={(v: number) => `${v}%`} tickLine={false} axisLine={false} width={44} />
              <Tooltip cursor={{ fill: "var(--surface-2)" }}
                content={({ active, payload }) => {
                  const c = active && (payload?.[0]?.payload as (CoverageHour & { label: string }) | undefined);
                  return c ? (
                    <TooltipBox title={c.label} rows={[{ key: "c", color: "var(--accent)", label: "covered", value: fmt.pct(c.coverage_pct) }]}
                      footer={`${c.successful_polls}/${c.polls} polls ok · max gap ${fmt.seconds(c.max_gap_seconds)}`} />
                  ) : null;
                }} />
              <Bar dataKey="coverage_pct" fill="var(--accent)" radius={[4, 4, 0, 0]} maxBarSize={24} isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </Card>

      <Card className="xl:col-span-2" title="Pipeline health" subtitle="Recorded by the Databricks job's health task on every run">
        <dl className="mb-3 grid grid-cols-3 gap-2 text-center">
          {[
            ["Trains tracked", fmt.int(all)],
            ["Inferred arrivals", `${fmt.int(totals.inferred)} (${fmt.int(totals.high)} high)`],
            ["Vanished · unresolved", `${fmt.int(totals.vanished)} · ${fmt.int(totals.unresolved)}`],
          ].map(([k, v]) => (
            <div key={k} className="rounded-lg bg-surface-2 px-2 py-2">
              <dt className="text-[11px] text-ink-2">{k}</dt>
              <dd className="text-[14px] font-semibold text-ink">{v}</dd>
            </div>
          ))}
        </dl>
        {!runs.length ? (
          <EmptyState>No pipeline runs in this export (local exports have no job history).</EmptyState>
        ) : (
          <DataTable
            caption="Recent pipeline runs"
            maxHeight={220}
            rows={runs.slice(0, 20)}
            columns={[
              { key: "t", header: "Checked", render: (r) => formatInstant(r.checked_at_ms, { timeZoneName: undefined }) },
              { key: "s", header: "Status", render: (r) => <StatusBadge status={r.status} /> },
              { key: "f", header: "New files", align: "right", render: (r) => fmt.int(r.bronze_files_read) },
              { key: "a", header: "Data age", align: "right", render: (r) => fmt.minutes(r.staleness_minutes, 0) },
              { key: "i", header: "Issues", render: (r) => r.issues.join(", ") || "—" },
            ]}
          />
        )}
      </Card>
    </div>
  );
}
