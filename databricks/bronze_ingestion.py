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
# MAGIC The transformation logic lives in `cta_lakehouse/bronze.py` and is unit-tested locally. This notebook
# MAGIC handles only I/O: reading files, writing Delta, and validating.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Load the project's Python package
# MAGIC `cta_lakehouse` sits next to this notebook in the Git folder. Registering it with cloudpickle ships its
# MAGIC code along with the `foreachBatch` function, which serverless compute runs in a separate Python process.

# COMMAND ----------

import os
import sys


def _find_package_dir() -> str:
    candidates = []
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        nb_dir = os.path.dirname(nb_path)
        candidates.append(nb_dir if nb_dir.startswith("/Workspace") else f"/Workspace{nb_dir}")
    except Exception:
        pass
    candidates.append(os.getcwd())
    for candidate in candidates:
        if os.path.isdir(os.path.join(candidate, "cta_lakehouse")):
            return candidate
    raise ImportError(f"cta_lakehouse not found in {candidates}. Run this notebook from the repo's Git folder.")


_package_dir = _find_package_dir()
if _package_dir not in sys.path:
    sys.path.insert(0, _package_dir)

import cta_lakehouse
from pyspark import cloudpickle

cloudpickle.register_pickle_by_value(cta_lakehouse)

from cta_lakehouse.bronze import to_bronze_rows
from cta_lakehouse.tables import BRONZE_ARRIVALS

print(f"Loaded cta_lakehouse from {_package_dir}")

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

BRONZE_TABLE = f"{catalog}.{schema}.{BRONZE_ARRIVALS.name}"
LANDING_PATH = f"/Volumes/{catalog}/{schema}/{landing_volume}/train_tracker/arrivals"
CHECKPOINT_PATH = f"/Volumes/{catalog}/{schema}/_checkpoints/bronze_train_tracker_arrivals"

print(f"Source:     {LANDING_PATH}")
print(f"Target:     {BRONZE_TABLE}")
print(f"Checkpoint: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the schema, volumes and table (idempotent)
# MAGIC
# MAGIC Delta tables in Unity Catalog are addressed by a three-level name, `catalog.schema.table`. The table
# MAGIC schema is declared explicitly in `cta_lakehouse/tables.py` rather than inferred, and a unit test checks
# MAGIC that the transformation output matches it. Change Data Feed is enabled for downstream consumers.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'CTA Bronze layer: raw, append-only source data'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{landing_volume} COMMENT 'Landing zone for raw CTA API files'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}._checkpoints COMMENT 'Streaming checkpoints'")
spark.sql(BRONZE_ARRIVALS.create_sql(BRONZE_TABLE))

if reset_checkpoint:
    # Safe because MERGE on record_key prevents duplicates; this only forces every file to be re-read.
    print("Reset requested: deleting checkpoint")
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read new files incrementally with Auto Loader, then MERGE
# MAGIC
# MAGIC `cloudFiles.format = text` with `wholetext = true` reads each file as **one row** holding its full,
# MAGIC unparsed text, so Bronze can't fail on a schema change in the CTA response. The hidden `_metadata`
# MAGIC column provides the source file path, size and modification time.
# MAGIC
# MAGIC `trigger(availableNow=True)` processes everything new, then stops. Serverless compute doesn't run
# MAGIC always-on streams, and this mode is ideal for a scheduled job.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

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


def merge_into_bronze(batch: DataFrame, batch_id: int) -> None:
    rows = to_bronze_rows(batch, batch_id).dropDuplicates([BRONZE_ARRIVALS.key])
    (
        DeltaTable.forName(batch.sparkSession, BRONZE_TABLE)
        .alias("t")
        .merge(rows.alias("s"), f"t.{BRONZE_ARRIVALS.key} = s.{BRONZE_ARRIVALS.key}")
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

def _publish_task_value(key: str, value) -> None:
    """Report a result to later tasks in the job (pipeline_health). A no-op outside a job."""
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate the Bronze table
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

_publish_task_value("files_read", files_read)  # only reached if every check above passed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explore
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
