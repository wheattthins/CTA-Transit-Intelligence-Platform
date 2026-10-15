import { formatAge, formatInstant } from "../lib/format";
import type { ThemeChoice } from "../lib/theme";
import type { Manifest } from "../lib/types";
import { Segmented } from "./ui/Segmented";
import { StatusBadge } from "./ui/StatusBadge";

export function Header({ manifest, theme, onTheme }: {
  manifest: Manifest;
  theme: ThemeChoice;
  onTheme: (t: ThemeChoice) => void;
}) {
  const latest = manifest.freshness?.latest_observation_ms;
  const health = manifest.latest_health;
  return (
    <header className="border-b border-hairline bg-surface">
      <div className="mx-auto flex max-w-[1400px] flex-wrap items-center justify-between gap-4 px-4 py-4 sm:px-6">
        <div>
          <h1 className="text-[20px] font-semibold tracking-tight text-ink">CTA Transit Intelligence</h1>
          <p className="text-[13px] text-ink-2">
            Chicago 'L' prediction accuracy, headways and service reliability, from CTA Train Tracker data
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-[12px] text-ink-2">
          {latest && (
            <span title={`Newest observation: ${formatInstant(latest)}`}>
              Data through <span className="font-medium text-ink">{formatInstant(latest)}</span>
              <span className="text-ink-muted"> · {formatAge(latest)}</span>
            </span>
          )}
          <span title={`Source: ${manifest.source}${manifest.git_commit ? ` @ ${manifest.git_commit}` : ""}`}>
            Exported {formatAge(manifest.exported_at_ms)}
          </span>
          {health && (
            <span className="flex items-center gap-1.5" title={health.issues.length ? `Issues: ${health.issues.join(", ")}` : "No issues"}>
              Pipeline <StatusBadge status={health.status} />
            </span>
          )}
          <Segmented
            label="Color theme"
            value={theme}
            onChange={onTheme}
            options={[{ value: "system", label: "Auto" }, { value: "light", label: "Light" }, { value: "dark", label: "Dark" }]}
          />
        </div>
      </div>
    </header>
  );
}
