"""Bronze transformation: raw landing-file text -> one Bronze row per API response.

Pure PySpark (no Databricks-only APIs) so it can be unit-tested locally. The notebook
``databricks/bronze_ingestion.py`` supplies the Auto Loader input and performs the MERGE.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ENVELOPE_SCHEMA = """
  metadata STRUCT<
    schema_version: INT, collection_id: STRING, source_system: STRING, source_endpoint: STRING,
    station_id: STRING, requested_at_utc: STRING, received_at_utc: STRING, http_status: INT,
    elapsed_ms: INT, attempts: INT, is_sample: BOOLEAN, collector_version: STRING
  >,
  response STRUCT<ctatt: STRUCT<tmst: STRING, errCd: STRING, errNm: STRING>>
"""

# Expected input columns (from Auto Loader text/wholetext + _metadata).
INPUT_COLUMNS = ("raw_json", "source_file_path", "source_file_size", "source_file_modified_at")


def _try_timestamp(col_name: str) -> Column:
    # try_cast returns NULL instead of failing under ANSI mode (the serverless default).
    return F.expr(f"try_cast({col_name} AS TIMESTAMP)")


def to_bronze_rows(raw: DataFrame, batch_id: int) -> DataFrame:
    """Extract envelope metadata; keep ``raw_json`` untouched; flag unparseable files."""
    parsed = raw.withColumn("env", F.from_json("raw_json", ENVELOPE_SCHEMA))
    m = "env.metadata"
    received = _try_timestamp(f"{m}.received_at_utc")
    is_parse_error = F.col(f"{m}.collection_id").isNull() | F.col("env.response.ctatt").isNull()
    return parsed.select(
        F.coalesce(F.col(f"{m}.collection_id"), F.sha2("raw_json", 256)).alias("record_key"),
        F.col(f"{m}.collection_id").alias("collection_id"),
        F.col(f"{m}.source_system").alias("source_system"),
        F.col(f"{m}.source_endpoint").alias("source_endpoint"),
        F.col(f"{m}.station_id").alias("station_id"),
        _try_timestamp(f"{m}.requested_at_utc").alias("requested_at_utc"),
        received.alias("received_at_utc"),
        F.to_date(received).alias("collection_date"),
        F.col(f"{m}.http_status").alias("http_status"),
        F.col(f"{m}.elapsed_ms").alias("elapsed_ms"),
        F.col(f"{m}.attempts").alias("attempts"),
        F.col(f"{m}.is_sample").alias("is_sample"),
        F.col(f"{m}.schema_version").alias("envelope_schema_version"),
        F.col(f"{m}.collector_version").alias("collector_version"),
        F.col("env.response.ctatt.tmst").alias("cta_tmst_local"),
        F.col("env.response.ctatt.errCd").alias("cta_error_code"),
        F.col("env.response.ctatt.errNm").alias("cta_error_name"),
        F.coalesce(
            F.expr("json_array_length(get_json_object(raw_json, '$.response.ctatt.eta'))"), F.lit(0)
        ).cast("int").alias("eta_count"),
        is_parse_error.alias("is_parse_error"),
        "raw_json",
        "source_file_path",
        "source_file_size",
        "source_file_modified_at",
        F.current_timestamp().alias("ingested_at_utc"),
        F.lit(batch_id).cast("bigint").alias("ingestion_batch_id"),
    )
