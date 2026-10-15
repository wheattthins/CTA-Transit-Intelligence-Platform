# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline health: run status, freshness, coverage and collector failures
# MAGIC
# MAGIC The last task of the `cta-lakehouse-pipeline` job. It runs **even when an earlier task failed**
# MAGIC (`run_if = ALL_DONE`) and appends one row per run to `cta_ops.pipeline_runs`:
# MAGIC
# MAGIC - **Task results:** what Bronze, Silver and Gold reported processing. A missing value means the
# MAGIC   task failed or didn't run.
# MAGIC - **Freshness:** minutes since the newest observation. Stale data usually means the collector on your
# MAGIC   machine isn't running or isn't uploading.
# MAGIC - **Last 24 hours:** polls, API success rate, predictions, reject rate, coverage.
# MAGIC - **Backlog:** Bronze rows that haven't reached Silver. This should be 0 after a run.
# MAGIC - **Collector:** runs, failed requests and upload failures, from the run summaries the scheduled
# MAGIC   collector uploads. These capture failures that never produced a data file.
# MAGIC
# MAGIC Status: `healthy`, `degraded` (warnings), `stale` (no recent data) or `failed` (pipeline broken).
# MAGIC The rules are in `cta_lakehouse/ops.py::evaluate_health`.

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
    raise ImportError(f"cta_lakehouse not found in {candidates}.")


_package_dir = _find_package_dir()
if _package_dir not in sys.path:
    sys.path.insert(0, _package_dir)

from cta_lakehouse.ops import (
    UPSTREAM_TASKS,
    HealthThresholds,
    collector_runs_from_text,
    compute_health_metrics,
    evaluate_health,
)
from cta_lakehouse.tables import (
    BRONZE_ARRIVALS,
    GOLD_TRAIN_VISITS,
    OPS_COLLECTOR_RUNS,
    OPS_PIPELINE_RUNS,
    SILVER_OBSERVATIONS,
    SILVER_REJECTS,
)

# COMMAND ----------

for name, default in {
    "catalog": "workspace", "bronze_schema": "cta_bronze", "silver_schema": "cta_silver",
    "gold_schema": "cta_gold", "ops_schema": "cta_ops", "landing_volume": "landing",
}.items():
    dbutils.widgets.text(name, default, name)
dbutils.widgets.text("job_run_id", "manual", "Job run id")
dbutils.widgets.text("max_staleness_minutes", "180", "Max minutes without new data")
dbutils.widgets.dropdown("fail_on_stale", "false", ["false", "true"], "Fail the job when data is stale")

w = {k: dbutils.widgets.get(k) for k in ("catalog", "bronze_schema", "silver_schema", "gold_schema", "ops_schema", "landing_volume")}
for name, value in w.items():
    if not value.replace("_", "").isalnum():
        raise ValueError(f"{name} must be letters, digits and underscores only: {value!r}")
job_run_id = dbutils.widgets.get("job_run_id")
in_job = job_run_id not in ("", "manual") and not job_run_id.startswith("{{")
thresholds = HealthThresholds(max_staleness_minutes=float(dbutils.widgets.get("max_staleness_minutes")))
fail_on_stale = dbutils.widgets.get("fail_on_stale") == "true"

spark.conf.set("spark.sql.session.timeZone", "UTC")
c = w["catalog"]
T = {
    "bronze": f"{c}.{w['bronze_schema']}.{BRONZE_ARRIVALS.name}",
    "observations": f"{c}.{w['silver_schema']}.{SILVER_OBSERVATIONS.name}",
    "rejects": f"{c}.{w['silver_schema']}.{SILVER_REJECTS.name}",
    "visits": f"{c}.{w['gold_schema']}.{GOLD_TRAIN_VISITS.name}",
    "collector_runs": f"{c}.{w['ops_schema']}.{OPS_COLLECTOR_RUNS.name}",
    "pipeline_runs": f"{c}.{w['ops_schema']}.{OPS_PIPELINE_RUNS.name}",
}
COLLECTOR_RUNS_PATH = f"/Volumes/{c}/{w['bronze_schema']}/{w['landing_volume']}/collector_runs"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.{w['ops_schema']} COMMENT 'CTA pipeline operations and monitoring'")
spark.sql(OPS_COLLECTOR_RUNS.create_sql(T["collector_runs"]))
spark.sql(OPS_PIPELINE_RUNS.create_sql(T["pipeline_runs"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Refresh `collector_runs` from the uploaded run summaries
# MAGIC Run summaries are tiny (one per collector hour), so the table is rebuilt from all of them each run.
# MAGIC This is simple and idempotent.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.errors import AnalysisException

try:
    raw_runs = (
        spark.read.option("wholetext", "true").option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "collector_run_*.json").text(COLLECTOR_RUNS_PATH)
        .select(F.col("value").alias("raw_json"), F.col("_metadata.file_path").alias("source_file_path"))
    )
    collector_runs = collector_runs_from_text(raw_runs)
    collector_runs.select(*OPS_COLLECTOR_RUNS.column_names).write.mode("overwrite").saveAsTable(T["collector_runs"])
    print(f"collector_runs: {spark.table(T['collector_runs']).count()} run(s)")
except AnalysisException as exc:
    if "PATH_NOT_FOUND" not in str(exc) and "does not exist" not in str(exc).lower():
        raise
    print("No collector run summaries uploaded yet (scheduled collection not set up?)")

collector_runs_df = spark.table(T["collector_runs"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute metrics and status

# COMMAND ----------


def _table_or_none(name):
    return spark.table(name) if spark.catalog.tableExists(name) else None


def _task_value(task_key: str, key: str):
    try:
        return dbutils.jobs.taskValues.get(taskKey=task_key, key=key, default=None, debugValue=None)
    except Exception:
        return None


TASK_VALUE_KEYS = {
    "bronze_ingestion": ("files_read", "bronze_files_read"),
    "silver_transformation": ("rows_processed", "silver_rows_processed"),
    "gold_metrics": ("train_visits", "gold_train_visits"),
}
task_values = {task: _task_value(task, key) for task, (key, _) in TASK_VALUE_KEYS.items()}
print("Task values:", task_values, "(in job)" if in_job else "(manual run: upstream checks skipped)")

metrics_df = compute_health_metrics(
    now=F.current_timestamp(),
    bronze=spark.table(T["bronze"]),
    observations=spark.table(T["observations"]),
    rejects=spark.table(T["rejects"]),
    visits=_table_or_none(T["visits"]),
    collector_runs=collector_runs_df,
)
metrics = metrics_df.first().asDict()
status, issues = evaluate_health(metrics, task_values if in_job else None, thresholds)

row = metrics_df.withColumns(
    {
        "job_run_id": F.lit(job_run_id),
        "status": F.lit(status),
        "issues": F.array(*[F.lit(i) for i in issues]).cast("array<string>"),
        **{
            column: F.lit(task_values[task]).cast("bigint")
            for task, (_, column) in TASK_VALUE_KEYS.items()
        },
    }
).select(*OPS_PIPELINE_RUNS.column_names)
row.write.mode("append").saveAsTable(T["pipeline_runs"])

print(f"STATUS: {status.upper()}")
for issue in issues:
    print(f"  - {issue}")
for k in ("latest_observation_utc", "staleness_minutes", "polls_24h", "api_success_rate_24h", "reject_rate_24h",
          "coverage_pct_24h", "bronze_backlog", "collector_runs_24h", "collector_failed_requests_24h"):
    print(f"  {k:>32}: {metrics[k]}")

# COMMAND ----------

display(spark.table(T["pipeline_runs"]).orderBy(F.desc("checked_at_utc")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fail the task when the pipeline is broken
# MAGIC The job is already marked failed when an upstream task fails. This task also fails on a Silver
# MAGIC backlog or no data, and on stale data if `fail_on_stale = true`. Stale data is off by default,
# MAGIC because a laptop collector legitimately stops overnight.

# COMMAND ----------

if status == "failed" or (status == "stale" and fail_on_stale):
    raise RuntimeError(f"Pipeline health {status}: {', '.join(issues)}")
