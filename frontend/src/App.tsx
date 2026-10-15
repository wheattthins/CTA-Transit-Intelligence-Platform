import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { AccuracyByHorizon } from "./components/charts/AccuracyByHorizon";
import { HeadwayCharts } from "./components/charts/HeadwayCharts";
import { ReliabilityTrend } from "./components/charts/ReliabilityTrend";
import { RushComparison } from "./components/charts/RushComparison";
import { ConsistencyTable } from "./components/ConsistencyTable";
import { DataHealth } from "./components/DataHealth";
import { FilterBar } from "./components/FilterBar";
import { Header } from "./components/Header";
import { KpiRow } from "./components/KpiRow";
import { Methodology } from "./components/Methodology";
import { NextTrains } from "./components/NextTrains";
import { RecentArrivals } from "./components/RecentArrivals";
import { dataUrl, loadDashboardData } from "./lib/data";
import { formatServiceDate } from "./lib/format";
import { DEFAULT_FILTERS, dateFloor, filterRows, routesIn, summarizeService, type Filters } from "./lib/metrics";
import { sortRoutes } from "./lib/routes";
import { useTheme } from "./lib/theme";
import type { DashboardData, RouteCode } from "./lib/types";

// MapLibre is ~1 MB; load it in its own chunk so charts render first.
const TransitMap = lazy(() => import("./components/TransitMap").then((m) => ({ default: m.TransitMap })));

interface StationPoint {
  station_id: string;
  coordinates: [number, number];
}

export default function App() {
  const { choice, setChoice, isDark } = useTheme();
  const [data, setData] = useState<DashboardData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [geoStations, setGeoStations] = useState<StationPoint[]>([]);
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);

  useEffect(() => {
    loadDashboardData().then(setData).catch((e: Error) => setError(e.message));
    fetch(dataUrl("geo/cta_stations.geojson"))
      .then((r) => r.json())
      .then((g: { features: { geometry: { coordinates: [number, number] }; properties: { station_id: string } }[] }) =>
        setGeoStations(g.features.map((f) => ({ station_id: f.properties.station_id, coordinates: f.geometry.coordinates }))),
      )
      .catch(() => setGeoStations([])); // the map still renders lines without station points
  }, []);

  const view = useMemo(() => {
    if (!data) return null;
    const last = data.manifest.last_service_date;
    const floor = dateFloor(filters.datePreset, last);
    const byRouteStation = <T extends { route_code: RouteCode; station_id: string }>(rows: T[]) =>
      rows.filter((r) => (!filters.routes.length || filters.routes.includes(r.route_code))
        && (filters.stationId === "all" || r.station_id === filters.stationId));
    const daily = filterRows(data.daily, filters, last);
    return {
      daily,
      horizon: filterRows(data.horizon, filters, last),
      headways: filterRows(data.headways, filters, last),
      summary: summarizeService(daily),
      arrivals: byRouteStation(data.arrivals).filter((a) => !floor || a.inferred_arrival_chicago.slice(0, 10) >= floor),
      snapshot: byRouteStation(data.snapshot),
      outcomes: byRouteStation(data.outcomes).filter((o) => !floor || o.service_date >= floor),
      coverage: data.coverage.filter((c) => (filters.stationId === "all" || c.station_id === filters.stationId)
        && (!floor || c.service_date >= floor) && (filters.dayType === "all" || c.day_type === filters.dayType)),
    };
  }, [data, filters]);

  const stations = useMemo(() => {
    const names = new Map<string, string>();
    const routes = new Map<string, Set<RouteCode>>();
    for (const s of data?.stations ?? []) {
      names.set(s.station_id, s.station_name);
      const set = routes.get(s.station_id) ?? new Set<RouteCode>();
      s.routes.forEach((r) => set.add(r));
      routes.set(s.station_id, set);
    }
    const coords = new Map(geoStations.map((g) => [g.station_id, g.coordinates]));
    const monitored = [...names].flatMap(([id, name]) => {
      const c = coords.get(id);
      return c ? [{ id, name, lon: c[0], lat: c[1], routes: sortRoutes(routes.get(id) ?? []) }] : [];
    });
    return { names, monitored, list: [...names].map(([id, name]) => ({ id, name })) };
  }, [data, geoStations]);

  if (error) {
    return (
      <main className="mx-auto max-w-xl p-8 text-[14px] text-ink">
        <h1 className="mb-2 text-lg font-semibold">Couldn't load dashboard data</h1>
        <p className="text-ink-2">{error}. Run <code>python databricks/export_dashboard_data.py</code> to create
          the files in <code>frontend/public/data</code>.</p>
      </main>
    );
  }
  if (!data || !view) {
    return <main className="p-8 text-[14px] text-ink-2" aria-busy="true">Loading CTA data…</main>;
  }

  const m = data.manifest;
  const dateRange = m.first_service_date && m.last_service_date
    ? m.first_service_date === m.last_service_date
      ? formatServiceDate(m.last_service_date)
      : `${formatServiceDate(m.first_service_date)} – ${formatServiceDate(m.last_service_date)}`
    : "no data";
  const allRoutes = routesIn(data.daily);

  return (
    <div className="min-h-screen bg-page">
      <Header manifest={m} theme={choice} onTheme={setChoice} />
      <FilterBar filters={filters} onChange={setFilters} routes={allRoutes} stations={stations.list} dateRange={dateRange} />
      <main className="mx-auto max-w-[1400px] space-y-4 px-4 py-5 sm:px-6">
        <p className="rounded-lg border border-hairline bg-surface-2 px-3 py-2 text-[12.5px] text-ink-2">
          <strong className="font-medium text-ink">Arrival times on this page are inferred.</strong> CTA publishes
          predictions, not actual arrivals; each arrival is estimated from when the train left the prediction feed, with
          stated uncertainty. See the methodology at the bottom.
        </p>
        <KpiRow s={view.summary} />
        <div className="grid gap-4 xl:grid-cols-2">
          <AccuracyByHorizon rows={view.horizon} />
          <ReliabilityTrend rows={view.daily} />
        </div>
        <HeadwayCharts headways={view.headways} />
        <div className="grid gap-4 xl:grid-cols-5">
          <div className="min-w-0 xl:col-span-3">
            <Suspense fallback={<div className="h-[560px] rounded-xl border border-hairline bg-surface" aria-busy="true" />}>
              <TransitMap isDark={isDark} snapshot={view.snapshot} monitored={stations.monitored}
                selectedStation={filters.stationId} onSelectStation={(id) => setFilters({ ...filters, stationId: id })} />
            </Suspense>
          </div>
          <div className="min-w-0 xl:col-span-2">
            <NextTrains snapshot={view.snapshot} />
          </div>
        </div>
        <div className="grid gap-4 xl:grid-cols-2">
          <RushComparison rows={view.daily} />
          <ConsistencyTable rows={view.daily} stationNames={stations.names} />
        </div>
        <RecentArrivals arrivals={view.arrivals} />
        <DataHealth coverage={view.coverage} runs={data.pipelineRuns} outcomes={view.outcomes} />
        <Methodology />
        <footer className="pb-6 pt-2 text-center text-[12px] text-ink-muted">
          Data: Chicago Transit Authority Train Tracker · Exported from {m.source}
          {m.git_commit ? ` @ ${m.git_commit}` : ""} · Not affiliated with the CTA
        </footer>
      </main>
    </div>
  );
}
