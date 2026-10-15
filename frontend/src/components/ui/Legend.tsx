import { ROUTE_NAMES, routeColor } from "../../lib/routes";
import type { RouteCode } from "../../lib/types";

/** Legend keys mirror the mark: "line" for line charts, "rect" for bars, "dot" for points. */
export function RouteLegend({ routes, mark = "rect" }: { routes: RouteCode[]; mark?: "rect" | "line" | "dot" }) {
  if (routes.length < 2) return null; // a single series is named by the title
  return (
    <ul className="mb-2 flex flex-wrap gap-x-4 gap-y-1 text-[12px] text-ink-2" aria-label="Legend">
      {routes.map((r) => (
        <li key={r} className="flex items-center gap-1.5">
          <Key color={routeColor(r)} mark={mark} />
          {ROUTE_NAMES[r]} Line
        </li>
      ))}
    </ul>
  );
}

export function Key({ color, mark }: { color: string; mark: "rect" | "line" | "dot" }) {
  if (mark === "line") return <span aria-hidden className="inline-block h-[2px] w-3.5 rounded" style={{ background: color }} />;
  if (mark === "dot") return <span aria-hidden className="inline-block size-2.5 rounded-full" style={{ background: color }} />;
  return <span aria-hidden className="inline-block size-2.5 rounded-[3px]" style={{ background: color }} />;
}
