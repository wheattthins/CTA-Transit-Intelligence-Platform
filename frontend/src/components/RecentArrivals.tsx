import { useState } from "react";
import { fmt, formatLocalTime } from "../lib/format";
import { ROUTE_NAMES, routeColor } from "../lib/routes";
import type { Confidence, RecentArrival } from "../lib/types";
import { Card, EmptyState } from "./ui/Card";
import { DataTable } from "./ui/DataTable";

const CONFIDENCE: Record<Confidence, string> = { high: "High", medium: "Medium", low: "Low", none: "—" };

export function RecentArrivals({ arrivals }: { arrivals: RecentArrival[] }) {
  const [limit, setLimit] = useState(25);
  return (
    <Card
      title="Recent inferred arrivals"
      subtitle="When each train most likely reached the platform, with the uncertainty of that estimate"
      footer="Inferred from the moment a train disappears from CTA's feed (midpoint of the disappearance window). Not a measured arrival."
    >
      {!arrivals.length ? (
        <EmptyState>No inferred arrivals for these filters.</EmptyState>
      ) : (
        <>
          <DataTable
            caption="Recent inferred arrivals"
            maxHeight={420}
            rows={arrivals.slice(0, limit)}
            columns={[
              { key: "t", header: "Arrived (CT)", render: (a) => (
                <span className="tabular">{a.inferred_arrival_chicago.slice(5, 10).replace("-", "/")} {formatLocalTime(a.inferred_arrival_chicago, true)}</span>
              ) },
              { key: "u", header: "±", align: "right", render: (a) => fmt.seconds(a.uncertainty_seconds) },
              { key: "l", header: "Line", render: (a) => (
                <span className="flex items-center gap-1.5">
                  <span aria-hidden className="size-2.5 rounded-full" style={{ background: routeColor(a.route_code) }} />
                  {ROUTE_NAMES[a.route_code]} #{a.run_number}
                </span>
              ) },
              { key: "d", header: "Toward", render: (a) => a.destination_name },
              { key: "c", header: "Confidence", render: (a) => CONFIDENCE[a.inference_confidence] },
              { key: "r", header: "Prediction moved", align: "right", render: (a) => fmt.signedSeconds(a.total_revision_seconds) },
              { key: "s", header: "Sightings", align: "right", render: (a) => fmt.int(a.sightings) },
            ]}
          />
          {arrivals.length > limit && (
            <button type="button" onClick={() => setLimit((l) => l + 50)}
              className="mt-2 text-[12px] font-medium text-accent hover:underline">
              Show more ({arrivals.length - limit} remaining)
            </button>
          )}
        </>
      )}
    </Card>
  );
}
