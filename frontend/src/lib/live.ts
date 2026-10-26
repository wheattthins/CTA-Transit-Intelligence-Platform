// Live CTA data from the Vercel function in live-api/, which relays Train Tracker responses
// unmodified (it exists to hold the API key and cache). Parsed here into the dashboard's own shapes.
// CTA JSON quirks: every value is a string, timestamps are Chicago wall clock with no offset, and
// a list with one item can arrive as a bare object.

import { useEffect, useState } from "react";
import { formatLocalTime } from "./format";
import { ROUTE_NAMES, ROUTE_ORDER } from "./routes";
import type { RouteCode, SnapshotPrediction } from "./types";

/** Set at build time (dashboard.yml). Unset means the dashboard shows only the exported snapshot. */
export const LIVE_API_URL: string | undefined = import.meta.env.VITE_LIVE_API_URL || undefined;
/** The function's CDN cache refreshes every 30 s; polling faster would only return the same data. */
export const LIVE_POLL_MS = 30_000;

/** A train from the Locations API: every train on the network, not just ones due at monitored stations. */
export interface TrainPosition {
  route_code: RouteCode;
  run_number: string;
  destination_name: string;
  next_station_name: string;
  next_arrival_chicago: string; // predicted arrival at next_station_name, Chicago wall clock
  is_approaching: boolean;
  is_delayed: boolean;
  latitude: number;
  longitude: number;
  heading: number | null;
}

export interface LiveData {
  fetched_at_ms: number;
  positions: TrainPosition[];
  predictions: SnapshotPrediction[]; // Arrivals API, monitored stations only
}

/** off: no live URL configured. unavailable: the last request failed (data, if any, is older). */
export type LiveStatus = "off" | "connecting" | "live" | "unavailable";

export interface LiveFeed {
  status: LiveStatus;
  data: LiveData | null;
}

/** What the map draws for one train, from either the live feed or the exported snapshot. */
export interface TrainMarker {
  route_code: RouteCode;
  latitude: number;
  longitude: number;
  title: string;
  detail: string;
}

type CtaRecord = Record<string, unknown>;

interface LiveResponse {
  fetched_at_ms: number;
  positions: { route?: CtaRecord[] | CtaRecord };
  arrivals: { tmst?: string; eta?: CtaRecord[] | CtaRecord }[];
}

// Locations API names routes in lowercase; Arrivals uses the same codes as the rest of the dashboard.
const POSITION_ROUTES: Record<string, RouteCode> = {
  red: "Red", blue: "Blue", brn: "Brn", g: "G", org: "Org", p: "P", pink: "Pink", y: "Y",
};

const list = (v: unknown): CtaRecord[] =>
  Array.isArray(v) ? v : v && typeof v === "object" ? [v as CtaRecord] : [];
const str = (v: unknown): string => (typeof v === "string" ? v : "");
const flag = (v: unknown): boolean => v === "1";
const routeCode = (v: unknown): RouteCode | null =>
  ROUTE_ORDER.includes(v as RouteCode) ? (v as RouteCode) : null;

function optionalNumber(v: unknown): number | null {
  const n = typeof v === "string" && v.trim() !== "" ? Number(v) : NaN;
  return Number.isFinite(n) ? n : null;
}

/** Schedule-based trains report blank or 0,0 coordinates; both mean "unknown". */
function coordinates(r: CtaRecord): { latitude: number; longitude: number } | null {
  const latitude = optionalNumber(r.lat);
  const longitude = optionalNumber(r.lon);
  if (latitude === null || longitude === null || (latitude === 0 && longitude === 0)) return null;
  return { latitude, longitude };
}

/** Seconds from a to b, both Chicago wall-clock "yyyy-MM-ddTHH:mm:ss" (read as UTC: same offset). */
function secondsBetween(a: string, b: string): number {
  return Math.round((Date.parse(`${b}Z`) - Date.parse(`${a}Z`)) / 1000);
}

function parsePosition(route: RouteCode, t: CtaRecord): TrainPosition | null {
  const where = coordinates(t);
  if (!where || !str(t.rn)) return null;
  return {
    route_code: route,
    run_number: str(t.rn),
    destination_name: str(t.destNm),
    next_station_name: str(t.nextStaNm),
    next_arrival_chicago: str(t.arrT),
    is_approaching: flag(t.isApp),
    is_delayed: flag(t.isDly),
    ...where,
    heading: optionalNumber(t.heading),
  };
}

function parseEta(eta: CtaRecord, asOf: string, fetchedAtMs: number): SnapshotPrediction | null {
  const route = routeCode(eta.rt);
  if (!route || !["staId", "stpId", "rn", "prdt", "arrT"].every((f) => str(eta[f]))) return null;
  const where = coordinates(eta);
  return {
    station_id: str(eta.staId),
    station_name: str(eta.staNm),
    stop_id: str(eta.stpId),
    stop_description: str(eta.stpDe),
    route_code: route,
    run_number: str(eta.rn),
    destination_name: str(eta.destNm),
    predicted_arrival_chicago: str(eta.arrT),
    // As of CTA's response time, like the export's seconds_until_arrival.
    seconds_until_arrival: secondsBetween(asOf || str(eta.prdt), str(eta.arrT)),
    is_approaching: flag(eta.isApp),
    is_delayed: flag(eta.isDly),
    is_scheduled: flag(eta.isSch),
    latitude: where?.latitude ?? null,
    longitude: where?.longitude ?? null,
    heading: optionalNumber(eta.heading),
    observed_at_ms: fetchedAtMs,
  };
}

/** Parse the live-api response. Malformed records are skipped; a malformed envelope throws. */
export function parseLive(raw: unknown): LiveData {
  const r = raw as LiveResponse;
  if (typeof r?.fetched_at_ms !== "number" || !r.positions || !Array.isArray(r.arrivals)) {
    throw new Error("Unexpected live feed response");
  }
  const positions = list(r.positions.route).flatMap((group) => {
    const route = POSITION_ROUTES[str(group["@name"])];
    return route ? list(group.train).flatMap((t) => parsePosition(route, t) ?? []) : [];
  });
  const predictions = r.arrivals
    .flatMap((a) => list(a.eta).flatMap((eta) => parseEta(eta, str(a.tmst), r.fetched_at_ms) ?? []))
    // Same order as the export: station, then arrival.
    .sort((a, b) => a.station_id.localeCompare(b.station_id)
      || a.predicted_arrival_chicago.localeCompare(b.predicted_arrival_chicago));
  return { fetched_at_ms: r.fetched_at_ms, positions, predictions };
}

export function positionMarkers(positions: TrainPosition[]): TrainMarker[] {
  return positions.map((p) => ({
    route_code: p.route_code,
    latitude: p.latitude,
    longitude: p.longitude,
    title: `${ROUTE_NAMES[p.route_code]} Line run ${p.run_number} to ${p.destination_name}`,
    detail: p.next_arrival_chicago
      ? `${p.is_approaching ? "Approaching" : "Next stop"} ${p.next_station_name} ${formatLocalTime(p.next_arrival_chicago)}${p.is_delayed ? " · delayed" : ""}`
      : `Next stop ${p.next_station_name}`,
  }));
}

export function snapshotMarkers(snapshot: SnapshotPrediction[]): TrainMarker[] {
  return snapshot.flatMap((p) => p.latitude === null || p.longitude === null ? [] : [{
    route_code: p.route_code,
    latitude: p.latitude,
    longitude: p.longitude,
    title: `${ROUTE_NAMES[p.route_code]} Line run ${p.run_number} to ${p.destination_name}`,
    detail: `Predicted at ${p.station_name} ${formatLocalTime(p.predicted_arrival_chicago)}${p.is_delayed ? " · delayed" : ""}`,
  }]);
}

/** Polls the live feed while the tab is visible. Keeps the last good data through failed polls. */
export function useLiveFeed(url = LIVE_API_URL): LiveFeed {
  const [feed, setFeed] = useState<LiveFeed>({ status: url ? "connecting" : "off", data: null });

  useEffect(() => {
    if (!url) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let controller: AbortController | undefined;

    const poll = async () => {
      clearTimeout(timer);
      controller?.abort();
      const current = (controller = new AbortController());
      try {
        const res = await fetch(url, { cache: "no-store", signal: current.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = parseLive(await res.json());
        setFeed({ status: "live", data });
      } catch {
        if (current.signal.aborted) return; // hidden tab or a newer poll; that path reschedules
        setFeed((f) => ({ status: "unavailable", data: f.data }));
      }
      timer = setTimeout(poll, LIVE_POLL_MS);
    };
    // A hidden tab doesn't poll (no point spending CTA's quota); returning catches up at once.
    const onVisibility = () => {
      if (document.hidden) {
        clearTimeout(timer);
        controller?.abort();
      } else {
        void poll();
      }
    };

    document.addEventListener("visibilitychange", onVisibility);
    if (!document.hidden) void poll();
    return () => {
      document.removeEventListener("visibilitychange", onVisibility);
      clearTimeout(timer);
      controller?.abort();
    };
  }, [url]);

  return feed;
}
