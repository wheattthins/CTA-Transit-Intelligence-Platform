import { useId, useState, type ReactNode } from "react";
import { DataTable, type Column } from "./DataTable";
import { Segmented } from "./Segmented";

interface CardProps<T> {
  title: string;
  subtitle?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  /** When provided, a Chart | Table toggle shows the same data as an accessible table. */
  table?: { columns: Column<T>[]; rows: T[] };
  className?: string;
  footer?: ReactNode;
}

export function Card<T>({ title, subtitle, actions, children, table, className = "", footer }: CardProps<T>) {
  const [view, setView] = useState<"chart" | "table">("chart");
  const headingId = useId();
  return (
    <section aria-labelledby={headingId} className={`min-w-0 rounded-xl border border-hairline bg-surface p-4 sm:p-5 ${className}`}>
      <header className="mb-3 flex flex-wrap items-start justify-between gap-x-4 gap-y-2">
        <div className="min-w-0">
          <h2 id={headingId} className="text-[15px] font-semibold text-ink">{title}</h2>
          {subtitle && <p className="mt-0.5 text-[13px] leading-snug text-ink-2">{subtitle}</p>}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {actions}
          {table && (
            <Segmented
              label={`${title} view`}
              value={view}
              onChange={setView}
              options={[{ value: "chart", label: "Chart" }, { value: "table", label: "Table" }]}
            />
          )}
        </div>
      </header>
      {view === "table" && table ? <DataTable columns={table.columns} rows={table.rows} caption={title} /> : children}
      {footer && <footer className="mt-3 text-[12px] leading-snug text-ink-muted">{footer}</footer>}
    </section>
  );
}

export function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="flex min-h-[180px] items-center justify-center rounded-lg bg-surface-2 px-6 text-center text-[13px] text-ink-2">
      <p className="max-w-md">{children}</p>
    </div>
  );
}
