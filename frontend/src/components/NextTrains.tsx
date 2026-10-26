import { formatInstant, formatLocalTime, formatTimeOfDay } from "../lib/format";
import type { LiveFeed } from "../lib/live";
import { ROUTE_NAMES, routeColor } from "../lib/routes";
import type { SnapshotPrediction } from "../lib/types";
import { Card, EmptyState } from "./ui/Card";
import { LiveBadge } from "./ui/LiveBadge";

/** Predictions for the monitored stations as a departures board: live when the feed is up, else the latest export. */
export function NextTrains({ predictions, live }: { predictions: SnapshotPrediction[]; live: LiveFeed }) {
  const liveAt = live.data?.fetched_at_ms;
  const observed = predictions[0]?.observed_at_ms;
  const byPlatform = new Map<string, SnapshotPrediction[]>();
  for (const p of predictions) {
    const key = `${p.station_name} · ${p.stop_description}`;
    byPlatform.set(key, [...(byPlatform.get(key) ?? []), p]);
  }
  return (
    <Card
      title={liveAt ? "Next trains" : "Latest snapshot"}
      subtitle={liveAt
        ? <><LiveBadge status={live.status} />CTA predictions as of {formatTimeOfDay(liveAt)}</>
        : observed ? `CTA predictions as of ${formatInstant(observed)} - a static export, not live` : "No snapshot"}
    >
      {!predictions.length ? (
        <EmptyState>{liveAt ? "No live predictions for these filters." : "No predictions in the latest snapshot for these filters."}</EmptyState>
      ) : (
        <div className="max-h-[420px] space-y-4 overflow-auto pr-1">
          {[...byPlatform].map(([platform, preds]) => (
            <div key={platform}>
              <h3 className="mb-1 text-[12px] font-medium text-ink-2">{platform}</h3>
              <ul className="divide-y divide-[var(--border)]">
                {preds.slice(0, 5).map((p) => (
                  <li key={`${p.route_code}-${p.run_number}-${p.stop_id}`} className="flex items-center gap-2.5 py-1.5 text-[13px]">
                    <span aria-hidden className="h-4 w-1 rounded-full" style={{ background: routeColor(p.route_code) }} />
                    <span className="min-w-0 flex-1 truncate text-ink">
                      {p.destination_name}
                      <span className="text-ink-muted"> · {ROUTE_NAMES[p.route_code]} #{p.run_number}</span>
                    </span>
                    {p.is_delayed && <span className="rounded bg-surface-2 px-1.5 text-[11px] text-ink-2">Delayed</span>}
                    {p.is_scheduled && <span className="rounded bg-surface-2 px-1.5 text-[11px] text-ink-2" title="Schedule-based, not tracked live">Sched</span>}
                    <span className="tabular w-16 text-right font-medium text-ink">
                      {p.is_approaching ? "Due" : `${Math.max(1, Math.round(p.seconds_until_arrival / 60))} min`}
                    </span>
                    <span className="tabular hidden w-16 text-right text-[12px] text-ink-muted sm:inline">
                      {formatLocalTime(p.predicted_arrival_chicago)}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
