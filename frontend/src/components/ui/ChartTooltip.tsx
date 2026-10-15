import type { ReactNode } from "react";

export interface TooltipRow {
  key: string;
  color: string;
  label: string;
  value: ReactNode;
  note?: ReactNode;
}

/** Values lead, labels follow; series keyed by a short line stroke (not a box). */
export function TooltipBox({ title, rows, footer }: { title: ReactNode; rows: TooltipRow[]; footer?: ReactNode }) {
  return (
    <div className="pointer-events-none min-w-[160px] rounded-lg border border-hairline bg-surface px-3 py-2 text-[12px] shadow-lg">
      <div className="mb-1 text-ink-2">{title}</div>
      <ul className="space-y-0.5">
        {rows.map((r) => (
          <li key={r.key} className="flex items-center gap-2">
            <span aria-hidden className="h-[2px] w-3 rounded" style={{ background: r.color }} />
            <span className="tabular font-semibold text-ink">{r.value}</span>
            <span className="text-ink-2">{r.label}</span>
            {r.note && <span className="ml-auto pl-2 text-ink-muted">{r.note}</span>}
          </li>
        ))}
      </ul>
      {footer && <div className="mt-1.5 border-t border-hairline pt-1.5 text-ink-muted">{footer}</div>}
    </div>
  );
}
