"""Table definitions for every Delta table, in one place.

Notebooks generate ``CREATE TABLE`` statements from these, and unit tests check that the
DataFrames produced by the transformations match them exactly (names, order and types). A
schema drift between code and table therefore fails in CI, not halfway through a MERGE.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    comment: str = ""
    nullable: bool = True


@dataclass(frozen=True)
class TableSpec:
    name: str  # unqualified table name
    comment: str
    columns: tuple[Column, ...]
    key: str  # MERGE / uniqueness key
    properties: tuple[tuple[str, str], ...] = (("delta.enableChangeDataFeed", "true"),)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def create_sql(self, full_name: str) -> str:
        def esc(text: str) -> str:
            return text.replace("'", "\\'")

        cols = ",\n  ".join(
            f"{c.name} {c.type}{'' if c.nullable else ' NOT NULL'}"
            + (f" COMMENT '{esc(c.comment)}'" if c.comment else "")
            for c in self.columns
        )
        props = ", ".join(f"'{k}' = '{v}'" for k, v in self.properties)
        return (
            f"CREATE TABLE IF NOT EXISTS {full_name} (\n  {cols}\n)\nUSING DELTA\n"
            f"COMMENT '{esc(self.comment)}'\nTBLPROPERTIES ({props})"
        )

    def ddl_schema(self) -> str:
        """Schema string usable with spark.createDataFrame / StructType.fromDDL (for tests)."""
        return ", ".join(f"{c.name} {c.type}" for c in self.columns)


# --------------------------------------------------------------------------- Bronze

BRONZE_ARRIVALS = TableSpec(
    name="train_tracker_arrivals",
    comment="Bronze: one row per raw CTA Train Tracker arrivals API response",
    key="record_key",
    columns=(
        Column("record_key", "STRING", "Dedup key: collection_id, or SHA-256 of raw_json if unparseable", nullable=False),
        Column("collection_id", "STRING", "UUID assigned by the collector to this API call"),
        Column("source_system", "STRING", "cta_train_tracker (or *_SAMPLE for fixture data)"),
        Column("source_endpoint", "STRING", "CTA API endpoint, e.g. ttarrivals"),
        Column("station_id", "STRING", "Requested CTA parent station id (mapid)"),
        Column("requested_at_utc", "TIMESTAMP", "When the collector sent the request (UTC)"),
        Column("received_at_utc", "TIMESTAMP", "When the collector received the response (UTC)"),
        Column("collection_date", "DATE", "UTC date of received_at_utc"),
        Column("http_status", "INT"),
        Column("elapsed_ms", "INT"),
        Column("attempts", "INT", "HTTP attempts including retries"),
        Column("is_sample", "BOOLEAN", "TRUE for fixture data - must be excluded from analytics"),
        Column("envelope_schema_version", "INT"),
        Column("collector_version", "STRING"),
        Column("cta_tmst_local", "STRING", "CTA response timestamp, America/Chicago local, as received"),
        Column("cta_error_code", "STRING", "CTA errCd; 0 = success"),
        Column("cta_error_name", "STRING"),
        Column("eta_count", "INT", "Number of predictions in the response"),
        Column("is_parse_error", "BOOLEAN", "TRUE if the file is not a valid collector envelope", nullable=False),
        Column("raw_json", "STRING", "Complete file contents, unmodified", nullable=False),
        Column("source_file_path", "STRING", nullable=False),
        Column("source_file_size", "BIGINT"),
        Column("source_file_modified_at", "TIMESTAMP"),
        Column("ingested_at_utc", "TIMESTAMP", "When this row was written to Bronze", nullable=False),
        Column("ingestion_batch_id", "BIGINT", "Auto Loader micro-batch id"),
    ),
)

# --------------------------------------------------------------------------- Silver

SILVER_OBSERVATIONS = TableSpec(
    name="train_tracker_observations",
    comment="Silver: one row per collector API call (including CTA error responses); basis for coverage metrics",
    key="collection_id",
    columns=(
        Column("collection_id", "STRING", "Collector UUID for the API call", nullable=False),
        Column("record_key", "STRING", "Bronze record_key"),
        Column("station_id", "STRING", "Requested parent station id"),
        Column("requested_at_utc", "TIMESTAMP"),
        Column("observed_at_utc", "TIMESTAMP", "When the response was received (UTC)"),
        Column("observed_at_chicago", "TIMESTAMP_NTZ", "observed_at_utc as America/Chicago wall-clock time"),
        Column("cta_response_at_utc", "TIMESTAMP", "CTA tmst converted to UTC"),
        Column("cta_error_code", "STRING"),
        Column("cta_error_name", "STRING"),
        Column("is_successful", "BOOLEAN", "CTA errCd = 0"),
        Column("eta_count", "INT", "Predictions in the raw response"),
        Column("valid_prediction_count", "INT", "Predictions that passed validation"),
        Column("rejected_prediction_count", "INT", "Predictions sent to the rejects table"),
        Column("http_status", "INT"),
        Column("elapsed_ms", "INT"),
        Column("attempts", "INT"),
        Column("collector_version", "STRING"),
        Column("source_file_path", "STRING"),
        Column("bronze_ingested_at_utc", "TIMESTAMP"),
        Column("silver_processed_at_utc", "TIMESTAMP"),
    ),
)

SILVER_PREDICTIONS = TableSpec(
    name="train_arrival_predictions",
    comment="Silver: one validated row per CTA arrival prediction per observation (times in UTC)",
    key="prediction_key",
    columns=(
        Column("prediction_key", "STRING", "SHA-256(collection_id:eta_index) - unique per row", nullable=False),
        Column("prediction_fingerprint", "STRING", "SHA-256(stop|run|prdt|arrT) - same CTA prediction seen in several polls"),
        Column("collection_id", "STRING"),
        Column("eta_index", "INT", "Position in the response eta array"),
        Column("station_id", "STRING"),
        Column("station_name", "STRING"),
        Column("stop_id", "STRING", "Platform/direction stop id (3xxxx)"),
        Column("stop_description", "STRING"),
        Column("route_code", "STRING", "CTA route code: Red, Blue, Brn, G, Org, P, Pink, Y"),
        Column("route_name", "STRING"),
        Column("run_number", "STRING", "CTA run number; NOT unique per visit (Loop lines pass stations twice)"),
        Column("destination_stop_id", "STRING", "NULL when CTA sends 0 (schedule-based)"),
        Column("destination_name", "STRING"),
        Column("direction_code", "STRING", "CTA trDr: 1 or 5"),
        Column("observed_at_utc", "TIMESTAMP", "When we received this prediction"),
        Column("predicted_at_utc", "TIMESTAMP", "CTA prdt: when the prediction was generated"),
        Column("predicted_arrival_utc", "TIMESTAMP", "CTA arrT: predicted arrival - an estimate, not an actual arrival"),
        Column("predicted_arrival_chicago", "TIMESTAMP_NTZ", "predicted arrival as Chicago wall-clock time"),
        Column("minutes_until_arrival", "DOUBLE", "arrT - prdt in minutes (CTA-recommended)"),
        Column("seconds_until_arrival_at_observation", "INT", "arrT - observed_at"),
        Column("prediction_age_seconds", "INT", "observed_at - prdt"),
        Column("is_approaching", "BOOLEAN"),
        Column("is_scheduled", "BOOLEAN", "Schedule-based rather than live-tracked"),
        Column("is_delayed", "BOOLEAN"),
        Column("is_fault", "BOOLEAN"),
        Column("latitude", "DOUBLE", "NULL if missing or outside the Chicago area"),
        Column("longitude", "DOUBLE"),
        Column("heading", "INT", "0-359, NULL if missing/invalid"),
        Column("quality_warnings", "ARRAY<STRING>", "Soft data-quality issues; row still valid"),
        Column("record_key", "STRING", "Bronze record_key (lineage)"),
        Column("bronze_ingested_at_utc", "TIMESTAMP"),
        Column("silver_processed_at_utc", "TIMESTAMP"),
    ),
)

SILVER_REJECTS = TableSpec(
    name="train_tracker_rejects",
    comment="Silver quarantine: records that failed validation, with reasons. Never silently dropped.",
    key="reject_key",
    columns=(
        Column("reject_key", "STRING", nullable=False),
        Column("reject_level", "STRING", "response (whole file) or prediction (one eta element)"),
        Column("record_key", "STRING"),
        Column("collection_id", "STRING"),
        Column("station_id", "STRING"),
        Column("eta_index", "INT"),
        Column("rejection_reasons", "ARRAY<STRING>"),
        Column("raw_record", "STRING", "Original JSON of the rejected file or element"),
        Column("source_file_path", "STRING"),
        Column("bronze_ingested_at_utc", "TIMESTAMP"),
        Column("rejected_at_utc", "TIMESTAMP"),
    ),
)


# --------------------------------------------------------------------------- Gold
#
# Gold tables are fully recomputed from Silver on every run (overwrite), so their column *types*
# come from the code. These specs fix the column names, order and documentation, and a unit
# test asserts that the transformation output matches them.


@dataclass(frozen=True)
class GoldTableSpec:
    name: str
    comment: str
    columns: tuple[tuple[str, str], ...]  # (name, comment)
    key: tuple[str, ...]  # uniqueness key (checked by the notebook)

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]


def _cols(*items: str | tuple[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple((i, "") if isinstance(i, str) else i for i in items)


_TIME_CONTEXT = (
    ("service_date", "Chicago service day (starts 04:00 local)"),
    ("day_type", "weekday / saturday / sunday (holidays not modeled)"),
    ("time_period", "early_morning, am_peak, midday, pm_peak, evening, late_night (Chicago clock)"),
    ("is_rush_hour", "Weekday am_peak (06-09) or pm_peak (16-19)"),
    "rush_period",
)

GOLD_TRAIN_VISITS = GoldTableSpec(
    name="train_visits",
    comment="Gold: one row per train visit (run approaching a platform) with an INFERRED arrival - not a measured one",
    key=("visit_id",),
    columns=_cols(
        "visit_id", "station_id", "station_name", "stop_id", "stop_description", "route_code", "route_name",
        ("run_number", "Run numbers repeat: Loop lines pass a station twice, at different stops"),
        "direction_code", "destination_name",
        ("first_seen_at_utc", "First CTA snapshot containing the train"),
        ("last_seen_at_utc", "Last CTA snapshot containing the train"),
        ("first_missing_at_utc", "Next non-empty snapshot without the train; NULL = unresolved"),
        ("sightings", "Distinct CTA snapshots containing the train"),
        ("distinct_predictions", "Distinct predicted arrivals issued for this visit"),
        "first_predicted_arrival_utc", "final_predicted_arrival_utc",
        ("total_revision_seconds", "final - first predicted arrival"),
        ("max_abs_revision_seconds", "Largest single change between consecutive sightings"),
        "final_is_approaching", "schedule_based_only", "was_flagged_delayed",
        ("outcome", "inferred_arrival | vanished_before_arrival | unresolved"),
        ("outcome_reason", "Why a visit is not an inferred arrival (NULL for arrivals)"),
        ("inferred_arrival_utc", "INFERRED: midpoint of the disappearance window"),
        ("inferred_arrival_chicago", "INFERRED arrival, Chicago wall-clock time"),
        ("arrival_window_seconds", "first_missing_at - last_seen_at"),
        ("arrival_uncertainty_seconds", "+/- half the window"),
        ("inference_confidence", "high | medium | low | none"),
        ("confidence_reasons", "Why confidence was downgraded"),
        ("final_lead_seconds", "final predicted arrival - last_seen_at"),
        ("final_prediction_offset_seconds", "final predicted - inferred arrival"),
        *_TIME_CONTEXT,
    ),
)

GOLD_PREDICTION_REVISIONS = GoldTableSpec(
    name="prediction_revisions",
    comment="Gold: each distinct prediction issued for a train visit, how it changed, and its error vs the inferred arrival",
    key=("visit_id", "prediction_fingerprint"),
    columns=_cols(
        "visit_id", "prediction_fingerprint",
        ("revision_number", "0 = first prediction seen for the visit"),
        "station_id", "stop_id", "route_code", "run_number",
        ("first_seen_at_utc", "CTA snapshot in which this prediction first appeared"),
        "predicted_at_utc", "predicted_arrival_utc",
        ("revision_seconds", "Change from the previous distinct prediction (+ = later)"),
        ("minutes_until_arrival", "CTA's own lead time: arrT - prdt"),
        "horizon_bucket", "matches_final_prediction", "is_scheduled", "is_delayed",
        "outcome", "inference_confidence", "inferred_arrival_utc", "arrival_uncertainty_seconds",
        ("error_seconds", "predicted - INFERRED arrival; + = train came sooner than predicted"),
        "minutes_before_inferred_arrival",
        *_TIME_CONTEXT,
    ),
)

GOLD_PREDICTION_ACCURACY = GoldTableSpec(
    name="prediction_accuracy",
    comment="Gold: ESTIMATED prediction accuracy vs inferred arrivals (high/medium confidence, live predictions only). 'ALL' = rolled up",
    key=("station_id", "route_code", "horizon_bucket", "rush_period"),
    columns=_cols(
        "station_id", "route_code", "horizon_bucket", "rush_period",
        "predictions", "train_visits",
        ("mean_error_seconds", "Bias: + = trains tend to arrive sooner than predicted"),
        "mean_abs_error_seconds", "median_abs_error_seconds", "p90_abs_error_seconds",
        "pct_within_1_min", "pct_within_2_min",
        ("avg_arrival_uncertainty_seconds", "Measurement uncertainty of the inferred arrivals"),
    ),
)

GOLD_HEADWAYS = GoldTableSpec(
    name="headways",
    comment="Gold: time between consecutive INFERRED arrivals of a route at a platform, with reliability and gap flags",
    key=("visit_id",),
    columns=_cols(
        "visit_id", "station_id", "station_name", "stop_id", "stop_description", "route_code", "route_name",
        "run_number", "inferred_arrival_utc", "arrival_chicago", "inference_confidence",
        "previous_visit_id", "previous_run_number", "previous_arrival_utc", "previous_confidence",
        "headway_minutes",
        ("coverage_ok", "No collection gap between the two arrivals (a missed train would inflate the headway)"),
        ("is_reliable", "coverage_ok and both arrivals high/medium confidence"),
        ("typical_headway_minutes", "Median reliable headway for this platform/route/day type/period"),
        "headway_ratio",
        ("is_bunched", "Reliable headway < 2 minutes (trains arriving nearly together)"),
        ("is_unexpected_gap", "Reliable headway >= 2x typical and >= 10 minutes"),
        *_TIME_CONTEXT,
    ),
)

GOLD_SERVICE_CONSISTENCY = GoldTableSpec(
    name="service_consistency",
    comment="Gold: headway regularity and reliability signals by line/station/platform/period. 'ALL' = rolled up",
    key=("route_code", "station_id", "stop_id", "day_type", "time_period"),
    columns=_cols(
        "route_code", "station_id", "stop_id", "day_type", "time_period",
        "train_visits", "inferred_arrivals", "vanished_before_arrival", "pct_flagged_delayed",
        "avg_max_revision_seconds",
        "headways", "avg_headway_minutes", "median_headway_minutes", "stddev_headway_minutes",
        "p90_headway_minutes", "max_headway_minutes", "unexpected_gaps", "bunched_arrivals",
        ("headway_cv", "stddev/mean headway: 0 = perfectly regular, higher = bunching and gaps"),
    ),
)

GOLD_RUSH_HOUR_COMPARISON = GoldTableSpec(
    name="rush_hour_comparison",
    comment="Gold: rush hour vs non-rush hour service and prediction accuracy. 'ALL' = rolled up",
    key=("route_code", "station_id", "rush_period"),
    columns=_cols(
        "route_code", "station_id", "rush_period",
        "train_visits", "pct_flagged_delayed",
        "headways", "avg_headway_minutes", "headway_cv", "unexpected_gaps",
        "scored_predictions", "mean_abs_error_seconds", "pct_within_1_min",
    ),
)

GOLD_COLLECTION_COVERAGE = GoldTableSpec(
    name="collection_coverage",
    comment="Gold: how completely each station was observed, per Chicago clock hour. Low coverage = lower confidence",
    key=("station_id", "hour_start_chicago"),
    columns=_cols(
        "station_id", "service_date", "hour_start_chicago", "day_type", "time_period",
        "polls", "successful_polls", "failed_polls",
        ("minutes_covered", "Clock minutes with at least one successful poll"),
        "coverage_pct",
        ("max_gap_seconds", "Longest time to the next poll"),
    ),
)

GOLD_TABLES = {
    "train_visits": GOLD_TRAIN_VISITS,
    "prediction_revisions": GOLD_PREDICTION_REVISIONS,
    "prediction_accuracy": GOLD_PREDICTION_ACCURACY,
    "headways": GOLD_HEADWAYS,
    "service_consistency": GOLD_SERVICE_CONSISTENCY,
    "rush_hour_comparison": GOLD_RUSH_HOUR_COMPARISON,
    "collection_coverage": GOLD_COLLECTION_COVERAGE,
}
