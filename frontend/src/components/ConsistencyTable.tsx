import { fmt } from "../lib/format";
import { groupSummaries, routesIn, summarizeService } from "../lib/metrics";
import { ROUTE_NAMES, routeColor } from "../lib/routes";
import type { DailyLineMetrics, RouteCode } from "../lib/types";
import { Card, EmptyState } from "./ui/Card";
import { DataTable } from "./ui/DataTable";

/** Which lines run most consistently? (More than ~7 measures carry meaning: a table, not colors.) */
export function ConsistencyTable({ rows, stationNames }: { rows: DailyLineMetrics[]; stationNames: Map<string, string> }) {
  const groups = groupSummaries(rows, (r) => `${r.station_id}|${r.route_code}`, summarizeService);
  const order = routesIn(rows);
  const table = [...groups]
    .map(([key, s]) => {
      const [station, route] = key.split("|") as [string, RouteCode];
      return { station, route, s };
    })
    .sort((a, b) => a.station.localeCompare(b.station) || order.indexOf(a.route) - order.indexOf(b.route));

  return (
    <Card
      title="Service consistency by line"
      subtitle="Lower headway CV = more evenly spaced trains. Counts show how much data each figure rests on."
    >
      {!table.length ? (
        <EmptyState>No data for these filters.</EmptyState>
      ) : (
        <DataTable
          caption="Service consistency by line"
          rows={table}
          columns={[
            { key: "l", header: "Line", render: ({ route }) => (
              <span className="flex items-center gap-1.5">
                <span aria-hidden className="size-2.5 rounded-full" style={{ background: routeColor(route) }} />
                {ROUTE_NAMES[route]}
              </span>
            ) },
            { key: "st", header: "Station", render: ({ station }) => stationNames.get(station) ?? station },
            { key: "v", header: "Trains seen", align: "right", render: ({ s }) => fmt.int(s.trainVisits) },
            { key: "a", header: "Inferred arrivals", align: "right", render: ({ s }) => fmt.int(s.inferredArrivals) },
            { key: "h", header: "Avg headway", align: "right", render: ({ s }) => fmt.minutes(s.avgHeadway) },
            { key: "cv", header: "Headway CV", align: "right", render: ({ s }) => fmt.ratio(s.headwayCv) },
            { key: "b", header: "Bunched", align: "right", render: ({ s }) => fmt.pct(s.bunchingPct) },
            { key: "g", header: "Long gaps", align: "right", render: ({ s }) => fmt.int(s.unexpectedGaps) },
            { key: "e", header: "Avg error", align: "right", render: ({ s }) => fmt.seconds(s.maeSeconds) },
            { key: "w", header: "≤ 1 min", align: "right", render: ({ s }) => fmt.pct(s.pctWithin1) },
            { key: "d", header: "Flagged delayed", align: "right", render: ({ s }) => fmt.pct(s.delayedPct) },
            { key: "x", header: "Vanished", align: "right", render: ({ s }) => fmt.int(s.vanished) },
          ]}
        />
      )}
    </Card>
  );
}
