# Databricks notebook source
# MAGIC %md
# MAGIC # Gold metrics: inferred arrivals, prediction accuracy, headways, service consistency
# MAGIC
# MAGIC > **CTA Train Tracker never reports actual arrival times.** Every arrival in these tables is
# MAGIC > **inferred**, and it carries a confidence level and an uncertainty in seconds. Accuracy figures are
# MAGIC > *estimates* measured against those inferred arrivals.
# MAGIC
# MAGIC **How arrivals are inferred: the disappearance window.** CTA drops a train from the feed once it
# MAGIC reaches the station. For each *train visit* (one run approaching one platform), the arrival happened
# MAGIC between the last CTA snapshot that listed the train and the next successful snapshot that didn't.
# MAGIC The midpoint is the estimate, with ±½ window as its uncertainty. This uses only *when the train
# MAGIC disappeared*, never CTA's predicted times, so scoring predictions against it is not circular.
# MAGIC
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `train_visits` | one row per train visit: outcome, inferred arrival, confidence and reasons |
# MAGIC | `prediction_revisions` | each distinct prediction for a visit: revision and error |
# MAGIC | `prediction_accuracy` | error statistics by station / line / horizon / rush period (`ALL` = rolled up) |
# MAGIC | `headways` | time between consecutive inferred arrivals, with reliability and unexpected-gap flags |
# MAGIC | `service_consistency` | headway regularity (coefficient of variation) by line / station / platform / period |
# MAGIC | `rush_hour_comparison` | rush vs non-rush service and accuracy |
# MAGIC | `collection_coverage` | how completely each station was observed, per hour |
# MAGIC
# MAGIC **Processing model:** Gold is fully recomputed from Silver on each run and overwritten atomically.
# MAGIC Visits can span many Silver batches, so recomputation is simpler and safer than incremental
# MAGIC updates at this data volume. Delta keeps every previous version (`DESCRIBE HISTORY`). At larger
# MAGIC scale, recompute only recent service dates with `replaceWhere`.

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

from cta_lakehouse.gold import ALL, GoldConfig, build_gold
from cta_lakehouse.tables import GOLD_TABLES, SILVER_OBSERVATIONS, SILVER_PREDICTIONS, GoldTableSpec

print(f"Loaded cta_lakehouse from {_package_dir}")

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("silver_schema", "cta_silver", "Silver schema")
dbutils.widgets.text("gold_schema", "cta_gold", "Gold schema")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
for name, value in {"catalog": catalog, "silver_schema": silver_schema, "gold_schema": gold_schema}.items():
    if not value.replace("_", "").isalnum():
        raise ValueError(f"{name} must be letters, digits and underscores only: {value!r}")

spark.conf.set("spark.sql.session.timeZone", "UTC")

PREDICTIONS_TABLE = f"{catalog}.{silver_schema}.{SILVER_PREDICTIONS.name}"
OBSERVATIONS_TABLE = f"{catalog}.{silver_schema}.{SILVER_OBSERVATIONS.name}"
gold_name = {key: f"{catalog}.{gold_schema}.{spec.name}" for key, spec in GOLD_TABLES.items()}
config = GoldConfig()  # thresholds are documented in cta_lakehouse/gold.py
print(config)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema} COMMENT 'CTA Gold layer: inferred arrivals and service metrics'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build and write the Gold tables
# MAGIC The base tables (visits, revisions, headways, coverage) are written first and read back before the
# MAGIC aggregates use them. Without that, every aggregate would recompute the whole inference chain, and
# MAGIC serverless can't cache DataFrames.

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F


def _esc(text: str) -> str:
    return text.replace("'", "\\'")


def write_gold(key: str, df: DataFrame) -> DataFrame:
    spec: GoldTableSpec = GOLD_TABLES[key]
    full_name = gold_name[key]
    # Column comments travel in the schema metadata, so the table is written in a single commit
    # rather than one ALTER COLUMN commit per comment.
    columns = [
        F.col(name).alias(name, metadata={"comment": comment}) if comment else F.col(name)
        for name, comment in spec.columns
    ]
    (
        df.select(*columns)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{_esc(spec.comment)}'")
    print(f"wrote {full_name}")
    return spark.table(full_name)


# Guard: Gold must run AFTER Silver has finished, never in parallel with it. (A Lakeflow Job
# enforces this order with task dependencies; when running notebooks by hand, run them one at a time.)
silver_predictions = spark.table(PREDICTIONS_TABLE)
silver_observations = spark.table(OBSERVATIONS_TABLE)
silver_prediction_count = silver_predictions.count()
silver_observation_count = silver_observations.count()
print(f"Silver input: {silver_observation_count} observations, {silver_prediction_count} predictions")
if silver_observation_count == 0:
    raise RuntimeError(
        "Silver is empty. Run bronze_ingestion, then silver_transformation, to completion before gold_metrics."
    )

gold = build_gold(silver_predictions, silver_observations, config, materialize=write_gold)
for key in ("prediction_accuracy", "service_consistency", "rush_hour_comparison"):
    write_gold(key, getattr(gold, key))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quality checks
# MAGIC Hard checks fail the run. The confidence and coverage summaries show how much the metrics can be trusted.

# COMMAND ----------

failures = []
for key, spec in GOLD_TABLES.items():
    df = spark.table(gold_name[key])
    total = df.count()
    distinct = df.select(*spec.key).distinct().count()
    print(f"{spec.name:<24} rows={total}")
    if total != distinct:
        failures.append(f"{spec.name}: {total - distinct} duplicate keys on {spec.key}")

visits = spark.table(gold_name["train_visits"])
bad_inference = visits.where(
    "outcome = 'inferred_arrival' AND NOT (inferred_arrival_utc BETWEEN last_seen_at_utc AND first_missing_at_utc)"
).count()
if bad_inference:
    failures.append(f"{bad_inference} inferred arrivals fall outside their disappearance window")
if visits.where("outcome != 'inferred_arrival' AND inferred_arrival_utc IS NOT NULL").count():
    failures.append("Non-arrival visits must not have an inferred arrival time")
if silver_prediction_count > 0 and visits.count() == 0:
    failures.append(f"Silver has {silver_prediction_count} predictions but Gold produced 0 train visits")

if failures:
    raise AssertionError("Gold checks failed:\n- " + "\n- ".join(failures))
print("All hard Gold checks passed")

display(
    visits.groupBy("outcome", "inference_confidence")
    .agg(F.count("*").alias("visits"), F.round(F.avg("arrival_uncertainty_seconds"), 1).alias("avg_uncertainty_s"))
    .orderBy("outcome", "inference_confidence")
)

# COMMAND ----------

# Collection coverage: hours with low coverage produce low-confidence or missing arrivals
display(spark.table(gold_name["collection_coverage"]).orderBy("hour_start_chicago"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Insights

# COMMAND ----------

# How accurate are predictions, by how far ahead they were made?
display(
    spark.table(gold_name["prediction_accuracy"])
    .where(f"route_code != '{ALL}' AND station_id != '{ALL}' AND rush_period = '{ALL}' AND horizon_bucket != '{ALL}'")
    .orderBy("route_code", "horizon_bucket")
)

# COMMAND ----------

# Which lines run most regularly? Lower headway_cv = more even spacing.
display(
    spark.table(gold_name["service_consistency"])
    .where(f"station_id != '{ALL}' AND stop_id != '{ALL}' AND day_type = '{ALL}' AND time_period = '{ALL}'")
    .select("route_code", "stop_id", "headways", "avg_headway_minutes", "headway_cv", "unexpected_gaps",
            "pct_flagged_delayed", "vanished_before_arrival")
    .orderBy("headway_cv")
)

# COMMAND ----------

# Rush hour vs the rest of the day
display(spark.table(gold_name["rush_hour_comparison"]).where(f"station_id != '{ALL}' AND route_code != '{ALL}'").orderBy("route_code", "rush_period"))

# COMMAND ----------

# Unexpected gaps in service
display(
    spark.table(gold_name["headways"])
    .where("is_unexpected_gap")
    .select("route_name", "stop_description", "previous_arrival_utc", "arrival_chicago", "headway_minutes",
            "typical_headway_minutes", "headway_ratio")
    .orderBy(F.desc("headway_ratio"))
)
