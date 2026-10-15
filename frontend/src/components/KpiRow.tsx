import { fmt } from "../lib/format";
import type { ServiceSummary } from "../lib/metrics";

interface Tile {
  label: string;
  value: string;
  detail: string;
}

/** Headline numbers as stat tiles (the number is the chart). */
export function KpiRow({ s }: { s: ServiceSummary }) {
  const bias = s.biasSeconds;
  const tiles: Tile[] = [
    {
      label: "Avg prediction error",
      value: fmt.seconds(s.maeSeconds),
      detail: s.scored ? `mean |predicted − inferred| over ${fmt.int(s.scored)} predictions` : "no scored predictions",
    },
    {
      label: "Within 1 minute",
      value: fmt.pct(s.pctWithin1),
      detail: `${fmt.pct(s.pctWithin2)} within 2 minutes`,
    },
    {
      label: "Prediction bias",
      value: fmt.signedSeconds(bias),
      detail: bias === null ? "—" : bias < 0 ? "trains arrive later than predicted" : "trains arrive sooner than predicted",
    },
    {
      label: "Avg headway",
      value: fmt.minutes(s.avgHeadway),
      detail: `${fmt.int(s.headways)} reliable gaps between trains`,
    },
    {
      label: "Headway regularity (CV)",
      value: fmt.ratio(s.headwayCv),
      detail: "stddev ÷ mean · 0 = perfectly even",
    },
    {
      label: "Bunching · long gaps",
      value: `${fmt.pct(s.bunchingPct)} · ${fmt.int(s.unexpectedGaps)}`,
      detail: "trains < 2 min apart · gaps ≥ 2× typical",
    },
  ];
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
      {tiles.map((t) => (
        <div key={t.label} className="rounded-xl border border-hairline bg-surface p-3.5">
          <div className="text-[12px] text-ink-2">{t.label}</div>
          <div className="mt-1 text-[24px] font-semibold leading-tight text-ink">{t.value}</div>
          <div className="mt-1 text-[11.5px] leading-snug text-ink-muted">{t.detail}</div>
        </div>
      ))}
    </div>
  );
}
