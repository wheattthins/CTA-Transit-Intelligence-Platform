import type { ReactNode } from "react";

export interface Column<T> {
  key: string;
  header: string;
  render: (row: T) => ReactNode;
  align?: "left" | "right";
}

export function DataTable<T>({ columns, rows, caption, maxHeight = 340 }: {
  columns: Column<T>[];
  rows: T[];
  caption: string;
  maxHeight?: number;
}) {
  if (!rows.length) return <p className="py-6 text-center text-[13px] text-ink-2">No rows for the current filters.</p>;
  return (
    <div className="overflow-auto rounded-lg border border-hairline" style={{ maxHeight }}>
      <table className="w-full border-collapse text-[12.5px]">
        <caption className="sr-only">{caption}</caption>
        <thead className="sticky top-0 bg-surface-2">
          <tr>
            {columns.map((c) => (
              <th key={c.key} scope="col"
                className={`whitespace-nowrap border-b border-hairline px-3 py-2 font-medium text-ink-2 ${c.align === "right" ? "text-right" : "text-left"}`}>
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-b border-hairline last:border-0">
              {columns.map((c) => (
                <td key={c.key} className={`whitespace-nowrap px-3 py-1.5 text-ink ${c.align === "right" ? "tabular text-right" : ""}`}>
                  {c.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
