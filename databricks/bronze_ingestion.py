# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion: CTA Train Tracker arrivals
# MAGIC
# MAGIC Loads raw observation files from the Unity Catalog **landing volume** into the Bronze Delta table
# MAGIC `<catalog>.<schema>.train_tracker_arrivals`.
# MAGIC
# MAGIC **Bronze layer rules followed here**
# MAGIC - Keep the source exactly as received: each file's full text is stored in `raw_json`.
# MAGIC - Add only *metadata* columns: collection details from the envelope, the source file, and ingestion time.
# MAGIC - Never drop data. Unparseable files are kept and flagged with `is_parse_error`, not discarded.
# MAGIC - Re-running is safe and never creates duplicates (see *Incremental processing* below).
# MAGIC
# MAGIC **Incremental processing has two safeguards**
# MAGIC 1. **Auto Loader** (`cloudFiles`) records every file it has processed in a *checkpoint*, so each run
# MAGIC    reads only new files.
# MAGIC 2. A Delta **MERGE** on `record_key` (the collector's `collection_id`) inserts only unseen
# MAGIC    observations. Duplicates stay out even if the checkpoint is deleted or a file is uploaded twice
# MAGIC    under another name.
# MAGIC
# MAGIC Runs on serverless compute (Databricks Free Edition) with `trigger(availableNow=True)`: it processes
# MAGIC everything new, then stops. This is batch-style incremental processing that a scheduled job can run.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "cta_bronze", "Bronze schema")
dbutils.widgets.text("landing_volume", "landing", "Landing volume")
dbutils.widgets.dropdown("reset", "false", ["false", "true"], "Reset checkpoint (reprocess all files)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing_volume = dbutils.widgets.get("landing_volume")
reset_checkpoint = dbutils.widgets.get("reset") == "true"

for name, value in {"catalog": catalog, "schema": schema, "landing_volume": landing_volume}.items():
    if not value.replace("_", "").isalnum():
        raise ValueError(f"{name} must be letters, digits and underscores only: {value!r}")

spark.conf.set("spark.sql.session.timeZone", "UTC")  # all stored timestamps are UTC

BRONZE_TABLE = f"{catalog}.{schema}.train_tracker_arrivals"
LANDING_PATH = f"/Volumes/{catalog}/{schema}/{landing_volume}/train_tracker/arrivals"
CHECKPOINT_PATH = f"/Volumes/{catalog}/{schema}/_checkpoints/bronze_train_tracker_arrivals"

print(f"Source:     {LANDING_PATH}")
print(f"Target:     {BRONZE_TABLE}")
print(f"Checkpoint: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the schema, volumes and table (idempotent)
# MAGIC
# MAGIC Delta tables in Unity Catalog are addressed by a three-level name, `catalog.schema.table`. Free Edition
# MAGIC provides the `workspace` catalog. The table schema is declared explicitly rather than inferred so its
# MAGIC structure is documented and stable. Change Data Feed is enabled so the Silver layer can later read only
# MAGIC the rows that changed.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'CTA Bronze layer: raw, append-only source data'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{landing_volume} COMMENT 'Landing zone for raw CTA API files'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}._checkpoints COMMENT 'Streaming checkpoints'")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
  record_key               STRING    NOT NULL COMMENT 'Dedup key: collection_id, or SHA-256 of raw_json if unparseable',
  collection_id            STRING    COMMENT 'UUID assigned by the collector to this API call',
  source_system            STRING    COMMENT 'cta_train_tracker (or *_SAMPLE for fixture data)',
  source_endpoint          STRING    COMMENT 'CTA API endpoint, e.g. ttarrivals',
  station_id               STRING    COMMENT 'Requested CTA parent station id (mapid)',
  requested_at_utc         TIMESTAMP COMMENT 'When the collector sent the request (UTC)',
  received_at_utc          TIMESTAMP COMMENT 'When the collector received the response (UTC)',
  collection_date          DATE      COMMENT 'UTC date of received_at_utc',
  http_status              INT,
  elapsed_ms               INT,
  attempts                 INT       COMMENT 'HTTP attempts including retries',
  is_sample                BOOLEAN   COMMENT 'TRUE for fixture data - must be excluded from analytics',
  envelope_schema_version  INT,
  collector_version        STRING,
  cta_tmst_local           STRING    COMMENT 'CTA response timestamp, America/Chicago local, as received',
  cta_error_code           STRING    COMMENT 'CTA errCd; 0 = success',
  cta_error_name           STRING,
  eta_count                INT       COMMENT 'Number of predictions in the response',
  is_parse_error           BOOLEAN   NOT NULL COMMENT 'TRUE if the file is not a valid collector envelope',
  raw_json                 STRING    NOT NULL COMMENT 'Complete file contents, unmodified',
  source_file_path         STRING    NOT NULL,
  source_file_size         BIGINT,
  source_file_modified_at  TIMESTAMP,
  ingested_at_utc          TIMESTAMP NOT NULL COMMENT 'When this row was written to Bronze',
  ingestion_batch_id       BIGINT    COMMENT 'Auto Loader micro-batch id'
)
USING DELTA
COMMENT 'Bronze: one row per raw CTA Train Tracker arrivals API response'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

if reset_checkpoint:
    # Safe because MERGE on record_key prevents duplicates; this only forces every file to be re-read.
    print("Reset requested: deleting checkpoint")
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read new files incrementally with Auto Loader
# MAGIC
# MAGIC `cloudFiles.format = text` with `wholetext = true` reads each file as **one row** holding its full,
# MAGIC unparsed text. Bronze therefore can't fail on a schema change in the CTA response. Parsing into columns
# MAGIC happens deliberately in Silver. The hidden `_metadata` column provides the source file path, size and
# MAGIC modification time.

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("wholetext", "true")
    .option("pathGlobFilter", "arrivals_*.json")
    .load(LANDING_PATH)
    .select(
        F.col("value").alias("raw_json"),
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_size").alias("source_file_size"),
        F.col("_metadata.file_modification_time").alias("source_file_modified_at"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract envelope metadata and merge into Bronze
# MAGIC
# MAGIC Only the collector's `metadata` block and a few top-level response fields are extracted, for
# MAGIC deduplication and monitoring. The predictions (`eta`) stay inside `raw_json` until Silver.
# MAGIC
# MAGIC `try_cast` and `from_json` return NULL instead of failing on bad input, so one corrupt file can't stop
# MAGIC the pipeline. It is flagged instead.

# COMMAND ----------

ENVELOPE_SCHEMA = """
  metadata STRUCT<
    schema_version: INT, collection_id: STRING, source_system: STRING, source_endpoint: STRING,
    station_id: STRING, requested_at_utc: STRING, received_at_utc: STRING, http_status: INT,
    elapsed_ms: INT, attempts: INT, is_sample: BOOLEAN, collector_version: STRING
  >,
  response STRUCT<ctatt: STRUCT<tmst: STRING, errCd: STRING, errNm: STRING>>
"""


def to_bronze_rows(raw: DataFrame, batch_id: int) -> DataFrame:
    env = F.from_json("raw_json", ENVELOPE_SCHEMA)
    parsed = raw.withColumn("env", env)
    m = "env.metadata"
    received = F.expr(f"try_cast({m}.received_at_utc AS TIMESTAMP)")
    is_parse_error = F.col(f"{m}.collection_id").isNull() | F.col("env.response.ctatt").isNull()
    return parsed.select(
        F.coalesce(F.col(f"{m}.collection_id"), F.sha2("raw_json", 256)).alias("record_key"),
        F.col(f"{m}.collection_id").alias("collection_id"),
        F.col(f"{m}.source_system").alias("source_system"),
        F.col(f"{m}.source_endpoint").alias("source_endpoint"),
        F.col(f"{m}.station_id").alias("station_id"),
        F.expr(f"try_cast({m}.requested_at_utc AS TIMESTAMP)").alias("requested_at_utc"),
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


def merge_into_bronze(batch: DataFrame, batch_id: int) -> None:
    rows = to_bronze_rows(batch, batch_id).dropDuplicates(["record_key"])
    (
        DeltaTable.forName(batch.sparkSession, BRONZE_TABLE)
        .alias("t")
        .merge(rows.alias("s"), "t.record_key = s.record_key")
        .whenNotMatchedInsertAll()
        .execute()
    )


query = (
    raw_stream.writeStream
    .foreachBatch(merge_into_bronze)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .queryName("bronze_train_tracker_arrivals")
    .start()
)
query.awaitTermination()

def _input_rows(p) -> int:
    # Progress is a dict on older runtimes and a StreamingQueryProgress object on newer ones.
    value = p.get("numInputRows") if isinstance(p, dict) else getattr(p, "numInputRows", 0)
    return int(value or 0)


files_read = sum(_input_rows(p) for p in query.recentProgress or [])
print(f"Files read this run: {files_read} (0 means no new files since the last run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate the Bronze table
# MAGIC
# MAGIC These checks make the job **fail loudly** when a guarantee is broken, instead of passing bad data
# MAGIC downstream. Parse errors and CTA API errors are reported but don't fail the run, because Bronze is
# MAGIC meant to hold them.

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)

stats = bronze.agg(
    F.count("*").alias("rows"),
    F.countDistinct("record_key").alias("distinct_keys"),
    F.sum(F.col("is_parse_error").cast("int")).alias("parse_errors"),
    F.sum(F.when(F.col("is_sample"), 1).otherwise(0)).alias("sample_rows"),
    F.sum(F.when(F.col("cta_error_code") != "0", 1).otherwise(0)).alias("cta_error_responses"),
    F.sum("eta_count").alias("total_predictions"),
    F.max("received_at_utc").alias("latest_observation_utc"),
    F.max("ingested_at_utc").alias("latest_ingestion_utc"),
).first()

for key, value in stats.asDict().items():
    print(f"{key:>24}: {value}")

assert stats["rows"] == stats["distinct_keys"], (
    f"Duplicate record_key values found: {stats['rows'] - stats['distinct_keys']}"
)
if stats["parse_errors"]:
    print(f"WARNING: {stats['parse_errors']} file(s) could not be parsed - see is_parse_error = true")
if stats["sample_rows"]:
    print(f"WARNING: {stats['sample_rows']} SAMPLE row(s) present - Silver must exclude is_sample = true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Explore
# MAGIC Observations per UTC day and station. The `eta_count` average shows how many predictions a typical
# MAGIC response contains.

# COMMAND ----------

display(
    bronze.groupBy("collection_date", "station_id")
    .agg(
        F.count("*").alias("observations"),
        F.sum(F.col("is_parse_error").cast("int")).alias("parse_errors"),
        F.sum(F.when(F.col("cta_error_code") != "0", 1).otherwise(0)).alias("api_errors"),
        F.round(F.avg("eta_count"), 1).alias("avg_predictions"),
        F.min("received_at_utc").alias("first_utc"),
        F.max("received_at_utc").alias("last_utc"),
    )
    .orderBy("collection_date", "station_id")
)
