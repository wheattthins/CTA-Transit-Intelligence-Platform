"""Operational monitoring: collector run history, freshness, coverage and pipeline health.

``pipeline_health`` (the last task of the Lakeflow Job) uses these to append one row per job run
to ``cta_ops.pipeline_runs``. Every rule that decides the status lives in ``evaluate_health``, a
plain function that is easy to test and to tune.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# --------------------------------------------------------------------------- collector runs

COLLECTOR_RUN_JSON_SCHEMA = (
    "record_type STRING, schema_version INT, run_id STRING, started_at_utc STRING, finished_at_utc STRING, "
    "station_ids ARRAY<STRING>, interval_seconds INT, planned_cycles INT, requests INT, succeeded INT, "
    "failed INT, files_written INT, files_uploaded INT, upload_failures INT, uploads_disabled_reason STRING, "
    "interrupted BOOLEAN, exit_code INT, collector_version STRING, "
    "failures ARRAY<STRUCT<station_id: STRING, attempted_at_utc: STRING, error_type: STRING, "
    "error_code: STRING, message: STRING>>"
)


def _ts(col: Column) -> Column:
    return F.call_function("try_to_timestamp", col)


def read_collector_run_files(spark, path: str) -> DataFrame:
    """Read every collector run summary under ``path``, one row per file.

    NOTE: pass ``wholetext`` as an argument to ``.text()``. ``.option("wholetext", "true").text(p)``
    is silently overridden by ``.text()``'s own ``wholetext=False`` default, which would split
    pretty-printed JSON into unparseable lines.
    """
    return spark.read.text(
        path, wholetext=True, recursiveFileLookup=True, pathGlobFilter="collector_run_*.json"
    ).select(F.col("value").alias("raw_json"), F.col("_metadata.file_path").alias("source_file_path"))


def collector_runs_from_text(raw: DataFrame) -> DataFrame:
    """Parse collector run summary files (columns: raw_json, source_file_path) into typed rows."""
    r = raw.withColumn("r", F.from_json("raw_json", COLLECTOR_RUN_JSON_SCHEMA))
    return r.where(F.col("r.run_id").isNotNull()).select(
        F.col("r.run_id").alias("run_id"),
        _ts(F.col("r.started_at_utc")).alias("started_at_utc"),
        _ts(F.col("r.finished_at_utc")).alias("finished_at_utc"),
        F.col("r.station_ids").alias("station_ids"),
        F.col("r.interval_seconds").alias("interval_seconds"),
        F.col("r.planned_cycles").alias("planned_cycles"),
        F.col("r.requests").alias("requests"),
        F.col("r.succeeded").alias("succeeded"),
        F.col("r.failed").alias("failed"),
        F.col("r.files_written").alias("files_written"),
        F.col("r.files_uploaded").alias("files_uploaded"),
        F.col("r.upload_failures").alias("upload_failures"),
        F.col("r.uploads_disabled_reason").alias("uploads_disabled_reason"),
        F.col("r.interrupted").alias("interrupted"),
        F.col("r.exit_code").alias("exit_code"),
        F.col("r.collector_version").alias("collector_version"),
        F.transform(
            F.coalesce(F.col("r.failures"), F.array()),
            lambda f: F.struct(
                f["station_id"].alias("station_id"),
                _ts(f["attempted_at_utc"]).alias("attempted_at_utc"),
                f["error_type"].alias("error_type"),
                f["error_code"].alias("error_code"),
                f["message"].alias("message"),
            ),
        ).alias("failures"),
        "source_file_path",
    ).dropDuplicates(["run_id"])


# --------------------------------------------------------------------------- metrics


def _div(a: Column, b: Column) -> Column:
    """NULL instead of an error when dividing by zero (ANSI mode)."""
    return F.call_function("try_divide", a.cast("double"), b.cast("double"))


def _hours_ago(now: Column, hours: int) -> Column:
    return now - F.expr(f"INTERVAL {hours} HOURS")


def compute_health_metrics(
    *,
    now: Column,
    bronze: DataFrame,
    observations: DataFrame,
    rejects: DataFrame,
    visits: DataFrame | None,
    collector_runs: DataFrame | None,
    window_hours: int = 24,
) -> DataFrame:
    """One-row DataFrame of freshness, volume, quality, backlog and collector metrics.

    Computed in Spark (not Python) so timestamps stay in UTC regardless of driver time zone.
    """
    since = _hours_ago(now, window_hours)
    obs_window = observations.where(F.col("observed_at_utc") >= since)
    ok = F.col("is_successful")

    freshness = observations.agg(F.max("observed_at_utc").alias("latest_observation_utc"))
    volume = obs_window.agg(
        F.count("*").alias("polls_24h"),
        F.sum(ok.cast("long")).alias("successful_polls_24h"),
        F.sum("valid_prediction_count").cast("long").alias("predictions_24h"),
        F.sum("rejected_prediction_count").cast("long").alias("rejected_predictions_24h"),
        F.countDistinct(F.when(ok, F.struct("station_id", F.date_trunc("minute", "observed_at_utc")))).alias("_covered_minutes"),
        F.countDistinct("station_id").alias("_stations"),
    )
    backlog = (
        bronze.agg(F.count("*").alias("_bronze"))
        .crossJoin(observations.agg(F.count("*").alias("_obs")))
        .crossJoin(rejects.where("reject_level = 'response'").agg(F.count("*").alias("_resp_rejects")))
        .select((F.col("_bronze") - F.col("_obs") - F.col("_resp_rejects")).alias("bronze_backlog"))
    )
    out = freshness.crossJoin(volume).crossJoin(backlog)

    if visits is not None:
        out = out.crossJoin(
            visits.agg(
                F.count("*").alias("gold_train_visits_total"),
                F.max("inferred_arrival_utc").alias("gold_latest_inferred_arrival_utc"),
            )
        )
    else:
        out = out.withColumns(
            {"gold_train_visits_total": F.lit(None).cast("long"), "gold_latest_inferred_arrival_utc": F.lit(None).cast("timestamp")}
        )

    if collector_runs is not None:
        runs = collector_runs.where(F.col("started_at_utc") >= since)
        out = out.crossJoin(
            runs.agg(
                F.count("*").alias("collector_runs_24h"),
                F.sum("requests").cast("long").alias("collector_requests_24h"),
                F.sum("failed").cast("long").alias("collector_failed_requests_24h"),
                F.sum("upload_failures").cast("long").alias("collector_upload_failures_24h"),
            )
        ).crossJoin(collector_runs.agg(F.max("finished_at_utc").alias("last_collector_run_finished_utc")))
    else:
        out = out.withColumns(
            {
                "collector_runs_24h": F.lit(0).cast("long"),
                "collector_requests_24h": F.lit(0).cast("long"),
                "collector_failed_requests_24h": F.lit(0).cast("long"),
                "collector_upload_failures_24h": F.lit(0).cast("long"),
                "last_collector_run_finished_utc": F.lit(None).cast("timestamp"),
            }
        )

    seconds_since = F.call_function("unix_micros", now) - F.call_function("unix_micros", F.col("latest_observation_utc"))
    minutes_in_window = window_hours * 60
    return out.select(
        now.alias("checked_at_utc"),
        "latest_observation_utc",
        F.round(seconds_since / 60_000_000, 1).cast("double").alias("staleness_minutes"),
        F.coalesce("polls_24h", F.lit(0)).cast("long").alias("polls_24h"),
        F.coalesce("successful_polls_24h", F.lit(0)).cast("long").alias("successful_polls_24h"),
        F.round(_div(F.col("successful_polls_24h"), F.col("polls_24h")), 4).cast("double").alias("api_success_rate_24h"),
        F.coalesce("predictions_24h", F.lit(0)).cast("long").alias("predictions_24h"),
        F.coalesce("rejected_predictions_24h", F.lit(0)).cast("long").alias("rejected_predictions_24h"),
        F.round(
            _div(F.col("rejected_predictions_24h"), F.col("predictions_24h") + F.col("rejected_predictions_24h")), 4
        ).cast("double").alias("reject_rate_24h"),
        F.round(_div(F.col("_covered_minutes"), F.col("_stations") * minutes_in_window) * 100, 1)
        .cast("double").alias("coverage_pct_24h"),
        F.col("bronze_backlog").cast("long").alias("bronze_backlog"),
        F.col("gold_train_visits_total").cast("long").alias("gold_train_visits_total"),
        "gold_latest_inferred_arrival_utc",
        F.coalesce("collector_runs_24h", F.lit(0)).cast("long").alias("collector_runs_24h"),
        F.coalesce("collector_requests_24h", F.lit(0)).cast("long").alias("collector_requests_24h"),
        F.coalesce("collector_failed_requests_24h", F.lit(0)).cast("long").alias("collector_failed_requests_24h"),
        F.coalesce("collector_upload_failures_24h", F.lit(0)).cast("long").alias("collector_upload_failures_24h"),
        "last_collector_run_finished_utc",
    )


# --------------------------------------------------------------------------- health rules


@dataclass(frozen=True)
class HealthThresholds:
    max_staleness_minutes: float = 180  # no new observation for 3 hours
    max_reject_rate: float = 0.05
    min_api_success_rate: float = 0.90
    max_collector_failure_rate: float = 0.05


UPSTREAM_TASKS = ("bronze_ingestion", "silver_transformation", "gold_metrics")


def evaluate_health(
    metrics: Mapping[str, Any],
    task_values: Mapping[str, Any] | None = None,
    thresholds: HealthThresholds = HealthThresholds(),
) -> tuple[str, list[str]]:
    """Return (status, issues).

    status: ``failed`` (pipeline broken: an upstream task failed, Silver is behind Bronze, or there
    is no data), ``stale`` (pipeline fine, but no recent observations, so check the collector),
    ``degraded`` (working, with quality or reliability warnings), or ``healthy``.

    ``task_values`` maps upstream task -> the value it published, or None if it didn't publish one
    (it failed or didn't run). Pass None outside a job run to skip that check.
    """
    issues: list[str] = []
    failed: list[str] = []

    if task_values is not None:
        failed += [f"upstream_task_failed:{t}" for t in UPSTREAM_TASKS if task_values.get(t) is None]

    if metrics.get("latest_observation_utc") is None:
        failed.append("no_data")
    if (metrics.get("bronze_backlog") or 0) > 0:
        failed.append("silver_backlog")

    stale = (metrics.get("staleness_minutes") or 0) > thresholds.max_staleness_minutes
    if stale:
        issues.append("stale_data")

    reject_rate = metrics.get("reject_rate_24h")
    if reject_rate is not None and reject_rate > thresholds.max_reject_rate:
        issues.append("high_reject_rate")
    success = metrics.get("api_success_rate_24h")
    if success is not None and success < thresholds.min_api_success_rate:
        issues.append("low_api_success_rate")
    requests = metrics.get("collector_requests_24h") or 0
    if requests and (metrics.get("collector_failed_requests_24h") or 0) / requests > thresholds.max_collector_failure_rate:
        issues.append("collector_request_failures")
    if (metrics.get("collector_upload_failures_24h") or 0) > 0:
        issues.append("collector_upload_failures")

    all_issues = failed + issues
    if failed:
        return "failed", all_issues
    if stale:
        return "stale", all_issues
    if issues:
        return "degraded", all_issues
    return "healthy", all_issues
