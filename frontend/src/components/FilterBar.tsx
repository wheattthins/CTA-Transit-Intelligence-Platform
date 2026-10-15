import { DEFAULT_FILTERS, type DatePreset, type Filters, type TimeFilter } from "../lib/metrics";
import { PERIOD_LABELS, PERIOD_ORDER, ROUTE_NAMES, routeColor } from "../lib/routes";
import type { DayType, RouteCode } from "../lib/types";
import { Segmented } from "./ui/Segmented";

const selectClass =
  "rounded-lg border border-hairline bg-surface px-2.5 py-1.5 text-[13px] text-ink focus-visible:outline-2 focus-visible:outline-accent";

/** One row of filters above everything they scope. Every chart, tile and table uses the same slice. */
export function FilterBar({ filters, onChange, routes, stations, dateRange }: {
  filters: Filters;
  onChange: (f: Filters) => void;
  routes: RouteCode[];
  stations: { id: string; name: string }[];
  dateRange: string;
}) {
  const set = (patch: Partial<Filters>) => onChange({ ...filters, ...patch });
  const toggleRoute = (r: RouteCode) =>
    set({ routes: filters.routes.includes(r) ? filters.routes.filter((x) => x !== r) : [...filters.routes, r] });
  const isDefault = JSON.stringify(filters) === JSON.stringify(DEFAULT_FILTERS);

  return (
    <div className="sticky top-0 z-20 border-b border-hairline bg-page/90 backdrop-blur">
      <div className="mx-auto flex max-w-[1400px] flex-wrap items-center gap-x-4 gap-y-2 px-4 py-2.5 sm:px-6">
        <Segmented<DatePreset>
          label="Date range"
          value={filters.datePreset}
          onChange={(v) => set({ datePreset: v })}
          options={[
            { value: "all", label: "All data" },
            { value: "last_day", label: "Latest day" },
            { value: "last_7", label: "7 days" },
            { value: "last_30", label: "30 days" },
          ]}
        />
        <span className="hidden text-[12px] text-ink-muted lg:inline">{dateRange}</span>

        <div className="flex flex-wrap items-center gap-1.5" role="group" aria-label="Lines">
          {routes.map((r) => {
            const active = filters.routes.length === 0 || filters.routes.includes(r);
            return (
              <button
                key={r}
                type="button"
                aria-pressed={filters.routes.includes(r)}
                onClick={() => toggleRoute(r)}
                className={`flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[12px] font-medium transition-colors focus-visible:outline-2 focus-visible:outline-accent ${
                  filters.routes.includes(r) ? "border-ink/30 bg-surface text-ink" : "border-hairline text-ink-2 hover:text-ink"
                } ${active ? "" : "opacity-55"}`}
              >
                <span aria-hidden className="size-2.5 rounded-full" style={{ background: routeColor(r) }} />
                {ROUTE_NAMES[r]}
              </button>
            );
          })}
        </div>

        <label className="flex items-center gap-1.5 text-[12px] text-ink-2">
          Station
          <select className={selectClass} value={filters.stationId} onChange={(e) => set({ stationId: e.target.value })}>
            <option value="all">All monitored</option>
            {stations.map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}
          </select>
        </label>
        <label className="flex items-center gap-1.5 text-[12px] text-ink-2">
          Days
          <select className={selectClass} value={filters.dayType} onChange={(e) => set({ dayType: e.target.value as DayType | "all" })}>
            <option value="all">All days</option>
            <option value="weekday">Weekdays</option>
            <option value="saturday">Saturdays</option>
            <option value="sunday">Sundays</option>
          </select>
        </label>
        <label className="flex items-center gap-1.5 text-[12px] text-ink-2">
          Time
          <select className={selectClass} value={filters.time} onChange={(e) => set({ time: e.target.value as TimeFilter })}>
            <option value="all">All day</option>
            <option value="rush">Rush hours</option>
            <option value="non_rush">Outside rush hours</option>
            {PERIOD_ORDER.map((p) => <option key={p} value={p}>{PERIOD_LABELS[p]}</option>)}
          </select>
        </label>
        {!isDefault && (
          <button type="button" onClick={() => onChange(DEFAULT_FILTERS)}
            className="text-[12px] font-medium text-accent hover:underline focus-visible:outline-2 focus-visible:outline-accent">
            Reset
          </button>
        )}
      </div>
    </div>
  );
}
