export function Methodology() {
  return (
    <details className="group rounded-xl border border-hairline bg-surface p-4 sm:p-5">
      <summary className="cursor-pointer list-none text-[15px] font-semibold text-ink">
        <span className="mr-1.5 inline-block transition-transform group-open:rotate-90">›</span>
        How these numbers are made, and their limits
      </summary>
      <div className="mt-3 grid gap-4 text-[13px] leading-relaxed text-ink-2 md:grid-cols-2">
        <div>
          <h3 className="mb-1 font-medium text-ink">Arrivals are inferred, not measured</h3>
          <p>
            CTA Train Tracker publishes predictions only. A train drops out of the feed when it reaches the station, so each
            arrival is estimated as the midpoint between the last snapshot that listed the train and the first that didn't.
            At 60-second polling that's about ±30 s. Confidence drops when there are collection gaps, when a train was still
            far away at its last sighting, or when only schedule-based predictions existed. Trains that vanish while still
            predicted minutes away are excluded.
          </p>
        </div>
        <div>
          <h3 className="mb-1 font-medium text-ink">Accuracy and headways</h3>
          <p>
            Prediction error = predicted − inferred arrival, using live predictions on high/medium-confidence arrivals.
            Headway = time between consecutive inferred arrivals of a line at a platform, counted only when no collection
            gap lies between them. Rush hour is weekdays 6–9 AM and 4–7 PM, Chicago time. Holidays aren't modeled.
          </p>
        </div>
        <div>
          <h3 className="mb-1 font-medium text-ink">Pipeline</h3>
          <p>
            A Python collector polls the CTA API every minute and lands raw JSON in Databricks. Bronze, Silver and Gold Delta
            tables are built by a scheduled Lakeflow Job. This dashboard reads a static export of the Gold tables, and every
            filter re-aggregates exact sums in the browser.
          </p>
        </div>
        <div>
          <h3 className="mb-1 font-medium text-ink">Read small samples carefully</h3>
          <p>
            Each table shows how many predictions or headways a figure rests on. With little data, especially early on or
            for narrow filters, percentages swing widely. Data © Chicago Transit Authority; not affiliated with the CTA.
          </p>
        </div>
      </div>
    </details>
  );
}
