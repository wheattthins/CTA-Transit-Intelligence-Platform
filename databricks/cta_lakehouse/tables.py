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
