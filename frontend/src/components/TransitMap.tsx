import { Map as MapLibre, NavigationControl, Popup, setWorkerUrl, type GeoJSONSource, type MapLayerMouseEvent } from "maplibre-gl";
// MapLibre 6 renders tiles in a web worker that ships as a separate module. Bundle it (with its
// imports) via Vite's worker import, and tell MapLibre where it lives - works in dev and in builds.
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";
import { useEffect, useMemo, useRef } from "react";
import { dataUrl } from "../lib/data";
import { formatTimeOfDay } from "../lib/format";
import type { LiveFeed, TrainMarker } from "../lib/live";
import { ROUTE_NAMES, ROUTE_ORDER, routeColor } from "../lib/routes";
import type { RouteCode } from "../lib/types";
import { Card } from "./ui/Card";
import { Key } from "./ui/Legend";
import { LiveBadge } from "./ui/LiveBadge";

// Free OpenStreetMap-based vector basemaps (no API key): https://openfreemap.org
const BASEMAP = {
  light: "https://tiles.openfreemap.org/styles/positron",
  dark: "https://tiles.openfreemap.org/styles/dark",
};
setWorkerUrl(maplibreWorkerUrl);

const CHICAGO_BOUNDS: [[number, number], [number, number]] = [[-87.92, 41.72], [-87.6, 42.07]];

interface MonitoredStation {
  id: string;
  name: string;
  lon: number;
  lat: number;
  routes: RouteCode[];
}

function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function popupContent(title: string, lines: string[]): HTMLElement {
  // textContent only: station and destination names are data, never HTML.
  const root = document.createElement("div");
  const h = document.createElement("div");
  h.style.fontWeight = "600";
  h.textContent = title;
  root.appendChild(h);
  for (const line of lines) {
    const p = document.createElement("div");
    p.style.color = "var(--ink-2)";
    p.textContent = line;
    root.appendChild(p);
  }
  return root;
}

/** CTA 'L' network with monitored stations and train positions: live when the feed is up, else the latest export. */
export function TransitMap({ isDark, trains: markers, live, monitored, selectedStation, onSelectStation }: {
  isDark: boolean;
  trains: TrainMarker[];
  live: LiveFeed;
  monitored: MonitoredStation[];
  selectedStation: string | "all";
  onSelectStation: (id: string | "all") => void;
}) {
  const container = useRef<HTMLDivElement>(null);
  const mapRef = useRef<MapLibre | null>(null);
  const styleRef = useRef<string | null>(null);
  const onSelectRef = useRef(onSelectStation);
  onSelectRef.current = onSelectStation;

  const trains = useMemo(
    () => ({
      type: "FeatureCollection" as const,
      features: markers.map((m) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [m.longitude, m.latitude] },
        properties: { route: m.route_code, title: m.title, detail: m.detail },
      })),
    }),
    [markers],
  );
  const stationsGeo = useMemo(
    () => ({
      type: "FeatureCollection" as const,
      features: monitored.map((s) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [s.lon, s.lat] },
        properties: { station_id: s.id, name: s.name, lines: s.routes.map((r) => ROUTE_NAMES[r]).join(", ") },
      })),
    }),
    [monitored],
  );

  // Create the map once; restyle on theme change (setStyle drops our layers, so re-add them).
  useEffect(() => {
    if (!container.current) return;
    styleRef.current = isDark ? BASEMAP.dark : BASEMAP.light;
    const map = new MapLibre({
      container: container.current,
      style: styleRef.current,
      bounds: CHICAGO_BOUNDS,
      attributionControl: { compact: true },
      cooperativeGestures: true, // page scroll isn't hijacked by the map
    });
    map.addControl(new NavigationControl({ showCompass: false }), "top-right");
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const addLayers = () => {
      if (map.getSource("cta-lines")) return;
      // Read theme colors now (after the theme is stamped), not when the effect starts.
      const lineColor = ["match", ["get", "route"], ...ROUTE_ORDER.flatMap((r) => [r, cssVar(`--line-${r.toLowerCase()}`)]), "#888"];
      const surface = cssVar("--surface");
      const ink = cssVar("--ink");
      map.addSource("cta-lines", { type: "geojson", data: dataUrl("geo/cta_lines.geojson") });
      map.addSource("cta-stations", { type: "geojson", data: dataUrl("geo/cta_stations.geojson") });
      map.addSource("monitored", { type: "geojson", data: stationsGeo });
      map.addSource("trains", { type: "geojson", data: trains });
      map.addLayer({
        id: "cta-lines", type: "line", source: "cta-lines",
        layout: { "line-join": "round", "line-cap": "round" },
        paint: {
          "line-color": lineColor as never,
          "line-width": ["interpolate", ["linear"], ["zoom"], 10, 2, 14, 4],
          // Shared track (e.g. the Loop) draws each line as a parallel stroke.
          // zoom must be the top-level interpolate input, so the per-feature offset goes inside each stop.
          "line-offset": ["interpolate", ["linear"], ["zoom"],
            10, ["*", ["get", "offset_index"], 2], 14, ["*", ["get", "offset_index"], 4.5]],
        },
      });
      map.addLayer({
        id: "cta-stations", type: "circle", source: "cta-stations",
        paint: { "circle-radius": ["interpolate", ["linear"], ["zoom"], 10, 1.8, 14, 4], "circle-color": surface,
          "circle-stroke-color": cssVar("--axis-muted"), "circle-stroke-width": 1 },
      });
      map.addLayer({
        id: "monitored", type: "circle", source: "monitored",
        paint: { "circle-radius": 8, "circle-color": ink, "circle-stroke-color": surface, "circle-stroke-width": 2.5 },
      });
      map.addLayer({
        id: "monitored-label", type: "symbol", source: "monitored",
        layout: { "text-field": ["get", "name"], "text-size": 12, "text-offset": [0, 1.4], "text-anchor": "top",
          "text-font": ["Noto Sans Bold"] },
        paint: { "text-color": ink, "text-halo-color": surface, "text-halo-width": 1.5 },
      });
      map.addLayer({
        id: "trains", type: "circle", source: "trains",
        paint: { "circle-radius": 5, "circle-color": lineColor as never, "circle-stroke-color": surface, "circle-stroke-width": 2 },
      });
    };

    const popup = new Popup({ closeButton: false, closeOnClick: false, offset: 10 });
    const showPopup = (e: MapLayerMouseEvent, build: (p: Record<string, string>) => HTMLElement) => {
      const f = e.features?.[0];
      if (!f) return;
      map.getCanvas().style.cursor = "pointer";
      popup.setLngLat(e.lngLat).setDOMContent(build(f.properties as Record<string, string>)).addTo(map);
    };
    const hide = () => { map.getCanvas().style.cursor = ""; popup.remove(); };
    const onTrain = (e: MapLayerMouseEvent) => showPopup(e, (p) => popupContent(p.title, [p.detail]));
    const onStation = (e: MapLayerMouseEvent) => showPopup(e, (p) => popupContent(p.name, [`Monitored · ${p.lines}`, "Click to filter"]));
    const onClick = (e: MapLayerMouseEvent) => {
      const id = e.features?.[0]?.properties?.station_id as string | undefined;
      if (id) onSelectRef.current(id);
    };

    const url = isDark ? BASEMAP.dark : BASEMAP.light;
    if (styleRef.current !== url) {
      styleRef.current = url;
      map.once("style.load", addLayers);
      map.setStyle(url); // drops our layers; addLayers re-creates them with this theme's colors
    } else if (map.isStyleLoaded()) {
      addLayers();
    } else {
      map.once("load", addLayers);
    }
    map.on("mousemove", "trains", onTrain);
    map.on("mouseleave", "trains", hide);
    map.on("mousemove", "monitored", onStation);
    map.on("mouseleave", "monitored", hide);
    map.on("click", "monitored", onClick);
    return () => {
      map.off("mousemove", "trains", onTrain);
      map.off("mouseleave", "trains", hide);
      map.off("mousemove", "monitored", onStation);
      map.off("mouseleave", "monitored", hide);
      map.off("click", "monitored", onClick);
      popup.remove();
    };
  }, [isDark, stationsGeo, trains]);

  // Keep data sources current without a restyle.
  useEffect(() => {
    (mapRef.current?.getSource("trains") as GeoJSONSource | undefined)?.setData(trains);
    (mapRef.current?.getSource("monitored") as GeoJSONSource | undefined)?.setData(stationsGeo);
  }, [trains, stationsGeo]);

  // Fly to a selected station.
  useEffect(() => {
    const s = monitored.find((m) => m.id === selectedStation);
    if (s) mapRef.current?.flyTo({ center: [s.lon, s.lat], zoom: 13, duration: 800 });
  }, [selectedStation, monitored]);

  const liveAt = live.data?.fetched_at_ms;
  return (
    <Card
      title="Chicago 'L' network"
      subtitle={liveAt
        ? <><LiveBadge status={live.status} />Monitored stations, and every train on the 'L' as of {formatTimeOfDay(liveAt)}</>
        : `Monitored stations, and train positions from the latest exported snapshot (${live.status === "unavailable" ? "live feed unavailable" : "not live"})`}
      actions={selectedStation !== "all" ? (
        <button type="button" onClick={() => onSelectStation("all")} className="text-[12px] font-medium text-accent hover:underline">
          Show all stations
        </button>
      ) : undefined}
      footer="Line and station geometry: City of Chicago Data Portal. Basemap © OpenStreetMap contributors, OpenFreeMap."
    >
      <div ref={container} className="h-[420px] w-full overflow-hidden rounded-lg" role="img"
        aria-label={`Map of the CTA 'L' network. Monitored stations: ${monitored.map((m) => m.name).join(", ") || "none"}. ${trains.features.length} train positions shown.`} />
      <ul className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-[12px] text-ink-2" aria-label="Map legend">
        {ROUTE_ORDER.map((r) => (
          <li key={r} className="flex items-center gap-1.5"><Key color={routeColor(r)} mark="line" />{ROUTE_NAMES[r]}</li>
        ))}
        <li className="flex items-center gap-1.5">
          <span aria-hidden className="size-3 rounded-full border-2 border-surface bg-ink" />Monitored station
        </li>
        <li className="flex items-center gap-1.5">
          <span aria-hidden className="size-2.5 rounded-full border border-axis bg-surface" />Other station
        </li>
      </ul>
    </Card>
  );
}
