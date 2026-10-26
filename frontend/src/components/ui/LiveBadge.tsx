import type { LiveStatus } from "../../lib/live";

/** Live-feed state as dot + label (never color alone). Nothing while the feed isn't being shown. */
export function LiveBadge({ status }: { status: LiveStatus }) {
  if (status !== "live" && status !== "unavailable") return null;
  const live = status === "live";
  return (
    <span className="mr-1.5 inline-flex items-center gap-1 font-medium text-ink">
      <span aria-hidden className={`size-2 rounded-full ${live ? "motion-safe:animate-pulse" : ""}`}
        style={{ background: live ? "var(--status-good)" : "var(--status-serious)" }} />
      {live ? "Live" : "Reconnecting"}
      <span aria-hidden className="text-ink-muted">·</span>
    </span>
  );
}
