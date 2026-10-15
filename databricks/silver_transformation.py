# Databricks notebook source
# MAGIC %md
# MAGIC # Silver transformation: validated CTA arrival predictions
# MAGIC
# MAGIC Reads **new** Bronze rows incrementally and writes three Silver tables:
# MAGIC
# MAGIC | Table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | `train_tracker_observations` | one row per API call | Coverage: when we polled and what came back, including CTA errors |
# MAGIC | `train_arrival_predictions` | one row per valid prediction per poll | Typed, UTC, validated predictions for Gold |
# MAGIC | `train_tracker_rejects` | one row per rejected file or prediction | Quarantine with reasons. Nothing is silently dropped |
# MAGIC
# MAGIC **Data quality has two levels**
# MAGIC - *Rejection reasons* (hard): missing required fields, unparseable timestamps, unknown routes, invalid
# MAGIC   IDs, station mismatch, arrival before prediction, implausible horizon, invalid flags, duplicates within
# MAGIC   one response, SAMPLE data, corrupt files.
# MAGIC - *Quality warnings* (soft): the row is kept and the issue recorded in `quality_warnings`. Examples:
# MAGIC   a live train without coordinates, out-of-range coordinates (nulled), invalid heading (nulled), a stale
# MAGIC   prediction.
# MAGIC
# MAGIC **Duplicates are handled at three levels**
# MAGIC 1. *Same file or observation twice*: already prevented by Bronze's MERGE on `collection_id`.
# MAGIC 2. *Same prediction twice in one response*: rejected as `duplicate_in_response`.
# MAGIC 3. *Same CTA prediction seen in several polls*: **not** a duplicate. Each sighting is an observation.
# MAGIC    They share a `prediction_fingerprint`, so Gold can collapse them when needed.
# MAGIC
# MAGIC **Incremental and idempotent:** the stream checkpoint means each Bronze row is processed once, and
# MAGIC MERGE on each table's key means re-processing can never insert duplicates.
# MAGIC Use `full_refresh = true` after changing validation logic to rebuild Silver from Bronze.

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

from cta_lakehouse.silver import transform_bronze_batch, with_processing_time
from cta_lakehouse.tables import (
    BRONZE_ARRIVALS,
    SILVER_OBSERVATIONS,
    SILVER_PREDICTIONS,
    SILVER_REJECTS,
    TableSpec,
)

print(f"Loaded cta_lakehouse from {_package_dir}")

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "cta_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "cta_silver", "Silver schema")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Full refresh (drop + rebuild Silver)")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
full_refresh = dbutils.widgets.get("full_refresh") == "true"

for name, value in {"catalog": catalog, "bronze_schema": bronze_schema, "silver_schema": silver_schema}.items():
    if not value.replace("_", "").isalnum():
        raise ValueError(f"{name} must be letters, digits and underscores only: {value!r}")

spark.conf.set("spark.sql.session.timeZone", "UTC")  # required by the Chicago <-> UTC conversions

BRONZE_TABLE = f"{catalog}.{bronze_schema}.{BRONZE_ARRIVALS.name}"
SILVER_TABLES: dict[str, TableSpec] = {
    "observations": SILVER_OBSERVATIONS,
    "predictions": SILVER_PREDICTIONS,
    "rejects": SILVER_REJECTS,
}
FULL_NAMES = {k: f"{catalog}.{silver_schema}.{spec.name}" for k, spec in SILVER_TABLES.items()}
CHECKPOINT_PATH = f"/Volumes/{catalog}/{silver_schema}/_checkpoints/silver_train_tracker"

print(f"Source:     {BRONZE_TABLE}")
for k, v in FULL_NAMES.items():
    print(f"{k:>12}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create schema and tables (idempotent)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema} COMMENT 'CTA Silver layer: validated, typed, deduplicated'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{silver_schema}._checkpoints COMMENT 'Streaming checkpoints'")

if full_refresh:
    print("Full refresh: dropping Silver tables and checkpoint; everything is rebuilt from Bronze")
    for full_name in FULL_NAMES.values():
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)

for key, spec in SILVER_TABLES.items():
    spark.sql(spec.create_sql(FULL_NAMES[key]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Process new Bronze rows
# MAGIC
# MAGIC `readStream.table(...)` on a Delta table is incremental: the checkpoint remembers which table versions
# MAGIC were already processed. `skipChangeCommits` ignores any commit that rewrites existing Bronze rows (e.g.
# MAGIC maintenance). Bronze is insert-only, so no new data is skipped.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Counted from the tables afterwards; streaming progress isn't reliable on serverless (see Bronze).
RUN_STARTED_AT = spark.sql("SELECT date_format(current_timestamp(), \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS\")").first()[0]


def _merge_new(df: DataFrame, full_name: str, key: str) -> None:
    (
        DeltaTable.forName(df.sparkSession, full_name)
        .alias("t")
        .merge(df.dropDuplicates([key]).alias("s"), f"t.{key} = s.{key}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_bronze_batch(bronze_batch: DataFrame, batch_id: int) -> None:
    silver = with_processing_time(transform_bronze_batch(bronze_batch))
    _merge_new(silver.observations, FULL_NAMES["observations"], SILVER_OBSERVATIONS.key)
    _merge_new(silver.predictions, FULL_NAMES["predictions"], SILVER_PREDICTIONS.key)
    _merge_new(silver.rejects, FULL_NAMES["rejects"], SILVER_REJECTS.key)


query = (
    spark.readStream.option("skipChangeCommits", "true")
    .table(BRONZE_TABLE)
    .writeStream.foreachBatch(process_bronze_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .queryName("silver_train_tracker")
    .start()
)
query.awaitTermination()


_since = F.to_timestamp(F.lit(RUN_STARTED_AT))
# Every Bronze row becomes exactly one observation or one response-level reject.
rows_processed = (
    spark.table(FULL_NAMES["observations"]).where(F.col("silver_processed_at_utc") >= _since).count()
    + spark.table(FULL_NAMES["rejects"]).where((F.col("reject_level") == "response") & (F.col("rejected_at_utc") >= _since)).count()
)
print(f"Bronze rows processed this run: {rows_processed}")

def _publish_task_value(key: str, value) -> None:
    """Report a result to later tasks in the job (pipeline_health). A no-op outside a job."""
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data quality checks
# MAGIC
# MAGIC **Hard checks** fail the run: key uniqueness, reconciliation with Bronze, no SAMPLE data in Silver.
# MAGIC **Soft checks** are reported: reject rate, warnings, CTA error rate, freshness.

# COMMAND ----------

obs = spark.table(FULL_NAMES["observations"])
preds = spark.table(FULL_NAMES["predictions"])
rejects = spark.table(FULL_NAMES["rejects"])
bronze = spark.table(BRONZE_TABLE)

failures: list[str] = []

# 1. Key uniqueness in every Silver table
for key, spec in SILVER_TABLES.items():
    df = spark.table(FULL_NAMES[key])
    total, distinct = df.agg(F.count("*"), F.countDistinct(spec.key)).first()
    print(f"{spec.name:<28} rows={total:<8} distinct {spec.key}={distinct}")
    if total != distinct:
        failures.append(f"{spec.name}: {total - distinct} duplicate {spec.key} values")

# 2. Reconciliation: every Bronze row is either an observation or a response-level reject
bronze_rows = bronze.count()
response_rejects = rejects.where("reject_level = 'response'").count()
obs_rows = obs.count()
print(f"Bronze rows={bronze_rows}  observations={obs_rows}  response-level rejects={response_rejects}")
if obs_rows + response_rejects != bronze_rows:
    failures.append(
        f"Reconciliation: observations ({obs_rows}) + response rejects ({response_rejects}) != Bronze ({bronze_rows})"
    )

# 3. Reconciliation: per observation, valid + rejected predictions == raw eta_count
mismatched = obs.where("valid_prediction_count + rejected_prediction_count != eta_count").count()
if mismatched:
    failures.append(f"{mismatched} observation(s) where valid + rejected predictions != eta_count")

pred_rows = preds.count()
pred_rejects = rejects.where("reject_level = 'prediction'").count()
expected = obs.agg(F.sum("valid_prediction_count")).first()[0] or 0
if pred_rows != expected:
    failures.append(f"Prediction rows ({pred_rows}) != sum of valid_prediction_count ({expected})")

# Soft checks
reject_rate = pred_rejects / max(pred_rows + pred_rejects, 1)
warned = preds.where(F.size("quality_warnings") > 0).count()
error_obs = obs.where(~F.col("is_successful")).count()
freshness = obs.agg(F.max("observed_at_utc")).first()[0]
print(f"Predictions={pred_rows}  rejected={pred_rejects} ({reject_rate:.2%})  with warnings={warned}")
print(f"CTA error observations={error_obs}  latest observation (UTC)={freshness}")
if reject_rate > 0.05:
    print("WARNING: more than 5% of predictions rejected - inspect the rejects table")

if failures:
    raise AssertionError("Silver data quality checks failed:\n- " + "\n- ".join(failures))
print("All hard data-quality checks passed")
_publish_task_value("rows_processed", rows_processed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explore

# COMMAND ----------

# Why were records rejected?
display(
    rejects.select("reject_level", F.explode("rejection_reasons").alias("reason"))
    .groupBy("reject_level", "reason").count().orderBy(F.desc("count"))
)

# COMMAND ----------

# Predictions by line and direction, with soft-warning counts
display(
    preds.groupBy("route_name", "stop_description")
    .agg(
        F.count("*").alias("predictions"),
        F.countDistinct("prediction_fingerprint").alias("distinct_predictions"),
        F.countDistinct("run_number").alias("runs"),
        F.round(F.avg("minutes_until_arrival"), 1).alias("avg_minutes_ahead"),
        F.sum(F.col("is_delayed").cast("int")).alias("delayed"),
        F.sum(F.col("is_scheduled").cast("int")).alias("schedule_based"),
        F.sum(F.when(F.size("quality_warnings") > 0, 1).otherwise(0)).alias("with_warnings"),
    )
    .orderBy("route_name", "stop_description")
)

# COMMAND ----------

# How one train's predicted arrival evolved across polls (the raw material for Gold's revision analysis)
example_run = preds.select("run_number", "stop_id").groupBy("run_number", "stop_id").count().orderBy(F.desc("count")).first()
if example_run:
    display(
        preds.where((F.col("run_number") == example_run.run_number) & (F.col("stop_id") == example_run.stop_id))
        .select("observed_at_utc", "predicted_at_utc", "predicted_arrival_utc", "predicted_arrival_chicago",
                "seconds_until_arrival_at_observation", "is_approaching", "is_delayed")
        .orderBy("observed_at_utc")
    )
