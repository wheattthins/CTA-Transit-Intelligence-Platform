import type { RouteCode, TimePeriod } from "./types";

/** Canonical order - also the palette order. Color follows the line, never its rank. */
export const ROUTE_ORDER: RouteCode[] = ["Red", "Blue", "Brn", "G", "Org", "P", "Pink", "Y"];

export const ROUTE_NAMES: Record<RouteCode, string> = {
  Red: "Red",
  Blue: "Blue",
  Brn: "Brown",
  G: "Green",
  Org: "Orange",
  P: "Purple",
  Pink: "Pink",
  Y: "Yellow",
};

/** CSS custom property holding each line's color (light/dark steps live in index.css). */
export const routeColor = (route: RouteCode): string => `var(--line-${route.toLowerCase()})`;

export const sortRoutes = (routes: Iterable<RouteCode>): RouteCode[] =>
  [...new Set(routes)].sort((a, b) => ROUTE_ORDER.indexOf(a) - ROUTE_ORDER.indexOf(b));

export const PERIOD_ORDER: TimePeriod[] = ["early_morning", "am_peak", "midday", "pm_peak", "evening", "late_night"];

export const PERIOD_LABELS: Record<TimePeriod, string> = {
  early_morning: "Early morning (4–6)",
  am_peak: "AM peak (6–9)",
  midday: "Midday (9–16)",
  pm_peak: "PM peak (16–19)",
  evening: "Evening (19–22)",
  late_night: "Late night (22–4)",
};

export const PERIOD_SHORT: Record<TimePeriod, string> = {
  early_morning: "4–6",
  am_peak: "AM peak",
  midday: "Midday",
  pm_peak: "PM peak",
  evening: "Evening",
  late_night: "Late night",
};
