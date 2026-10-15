// Shapes of the files written by databricks/export_dashboard_data.py (frontend/public/data/*.json).

export type RouteCode = "Red" | "Blue" | "Brn" | "G" | "Org" | "P" | "Pink" | "Y";
export type DayType = "weekday" | "saturday" | "sunday";
export type TimePeriod = "early_morning" | "am_peak" | "midday" | "pm_peak" | "evening" | "late_night";
export type HorizonBucket = "00-02 min" | "02-05 min" | "05-10 min" | "10-15 min" | "15-20 min" | "20+ min";
export type HealthStatus = "healthy" | "degraded" | "stale" | "failed";
export type Confidence = "high" | "medium" | "low" | "none";

/** Dimensions shared by every daily (additive) row. */
export interface DailyDims {
  service_date: string; // yyyy-MM-dd, Chicago service day (starts 04:00)
  station_id: string;
  route_code: RouteCode;
  day_type: DayType;
  time_period: TimePeriod;
  is_rush_hour: boolean;
}

export interface AccuracySums {
  scored_predictions: number;
  abs_error_sum_seconds: number;
  error_sum_seconds: number;
  within_1_min: number;
  within_2_min: number;
}

export interface DailyLineMetrics extends DailyDims, AccuracySums {
  train_visits: number;
  inferred_arrivals: number;
  vanished_before_arrival: number;
  flagged_delayed_visits: number;
  reliable_headways: number;
  headway_sum_minutes: number;
  headway_sumsq_minutes: number;
  bunched_arrivals: number;
  unexpected_gaps: number;
}

export interface DailyAccuracyByHorizon extends DailyDims, AccuracySums {
  horizon_bucket: HorizonBucket;
}

export interface Headway {
  service_date: string;
  station_id: string;
  stop_id: string;
  stop_description: string;
  route_code: RouteCode;
  run_number: string;
  previous_run_number: string;
  arrival_chicago: string; // yyyy-MM-ddTHH:mm:ss, Chicago wall clock (inferred)
  headway_minutes: number;
  typical_headway_minutes: number | null;
  is_bunched: boolean;
  is_unexpected_gap: boolean;
  day_type: DayType;
  time_period: TimePeriod;
  is_rush_hour: boolean;
}

export interface RecentArrival {
  visit_id: string;
  station_id: string;
  stop_id: string;
  stop_description: string;
  route_code: RouteCode;
  run_number: string;
  destination_name: string;
  inferred_arrival_chicago: string;
  uncertainty_seconds: number;
  inference_confidence: Confidence;
  sightings: number;
  distinct_predictions: number;
  total_revision_seconds: number | null;
  final_prediction_offset_seconds: number | null;
  was_flagged_delayed: boolean;
}

export interface VisitOutcome {
  service_date: string;
  station_id: string;
  route_code: RouteCode;
  outcome: "inferred_arrival" | "vanished_before_arrival" | "unresolved";
  inference_confidence: Confidence;
  visits: number;
}

export interface CoverageHour {
  station_id: string;
  service_date: string;
  hour_start_chicago: string;
  day_type: DayType;
  time_period: TimePeriod;
  polls: number;
  successful_polls: number;
  failed_polls: number;
  minutes_covered: number;
  coverage_pct: number;
  max_gap_seconds: number | null;
}

export interface SnapshotPrediction {
  station_id: string;
  station_name: string;
  stop_id: string;
  stop_description: string;
  route_code: RouteCode;
  run_number: string;
  destination_name: string;
  predicted_arrival_chicago: string;
  seconds_until_arrival: number;
  is_approaching: boolean;
  is_delayed: boolean;
  is_scheduled: boolean;
  latitude: number | null;
  longitude: number | null;
  heading: number | null;
  observed_at_ms: number;
}

export interface StationStop {
  station_id: string;
  station_name: string;
  stop_id: string;
  stop_description: string;
  routes: RouteCode[];
}

export interface PipelineRun {
  checked_at_ms: number;
  job_run_id: string;
  status: HealthStatus;
  issues: string[];
  bronze_files_read: number | null;
  silver_rows_processed: number | null;
  gold_train_visits: number | null;
  staleness_minutes: number | null;
  polls_24h: number;
  api_success_rate_24h: number | null;
  reject_rate_24h: number | null;
  coverage_pct_24h: number | null;
  bronze_backlog: number;
  collector_failed_requests_24h: number;
}

export interface Manifest {
  schema_version: number;
  exported_at_ms: number;
  source: string;
  git_commit: string | null;
  history_days: number;
  first_service_date: string | null;
  last_service_date: string | null;
  freshness: {
    latest_observation_ms: number;
    first_observation_ms: number;
    observations: number;
    successful_observations: number;
  } | null;
  latest_health: PipelineRun | null;
  files: Record<string, number>;
  notes: string[];
}

export interface DashboardData {
  manifest: Manifest;
  daily: DailyLineMetrics[];
  horizon: DailyAccuracyByHorizon[];
  headways: Headway[];
  arrivals: RecentArrival[];
  outcomes: VisitOutcome[];
  coverage: CoverageHour[];
  snapshot: SnapshotPrediction[];
  stations: StationStop[];
  pipelineRuns: PipelineRun[];
}
