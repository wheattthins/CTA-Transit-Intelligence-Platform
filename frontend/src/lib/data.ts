import type { DashboardData } from "./types";

const FILES = {
  manifest: "manifest.json",
  daily: "daily_line_metrics.json",
  horizon: "daily_accuracy_by_horizon.json",
  headways: "headways.json",
  arrivals: "recent_arrivals.json",
  outcomes: "visit_outcomes.json",
  coverage: "coverage.json",
  snapshot: "latest_snapshot.json",
  stations: "stations.json",
  pipelineRuns: "pipeline_runs.json",
} as const;

export const dataUrl = (file: string) => `${import.meta.env.BASE_URL}data/${file}`;

async function getJson<T>(file: string): Promise<T> {
  const res = await fetch(dataUrl(file));
  if (!res.ok) throw new Error(`${file}: HTTP ${res.status}`);
  return (await res.json()) as T;
}

export async function loadDashboardData(): Promise<DashboardData> {
  const entries = await Promise.all(
    Object.entries(FILES).map(async ([key, file]) => [key, await getJson(file)] as const),
  );
  return Object.fromEntries(entries) as unknown as DashboardData;
}
