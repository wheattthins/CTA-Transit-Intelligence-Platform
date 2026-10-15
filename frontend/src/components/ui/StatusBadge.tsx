import type { HealthStatus } from "../../lib/types";

const META: Record<HealthStatus, { label: string; color: string; icon: string }> = {
  healthy: { label: "Healthy", color: "var(--status-good)", icon: "✓" },
  degraded: { label: "Degraded", color: "var(--status-warning)", icon: "!" },
  stale: { label: "Stale", color: "var(--status-serious)", icon: "◷" },
  failed: { label: "Failed", color: "var(--status-critical)", icon: "✕" },
};

/** Status always ships as icon + label, never color alone. */
export function StatusBadge({ status }: { status: HealthStatus }) {
  const m = META[status];
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-hairline bg-surface px-2 py-0.5 text-[12px] text-ink">
      <span aria-hidden className="flex size-4 items-center justify-center rounded-full text-[10px] font-bold text-white"
        style={{ background: m.color }}>{m.icon}</span>
      {m.label}
    </span>
  );
}
