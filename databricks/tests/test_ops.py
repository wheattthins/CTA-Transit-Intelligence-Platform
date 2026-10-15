import json
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F
from simulate import Poller, Train, simulate
from test_gold import T0, at

from cta_lakehouse import tables
from cta_lakehouse.gold import build_gold
from cta_lakehouse.ops import (
    HealthThresholds,
    collector_runs_from_text,
    compute_health_metrics,
    evaluate_health,
)
from cta_lakehouse.silver import transform_bronze_batch, with_processing_time

# ------------------------------------------------------------------ health rules (pure Python)

HEALTHY = {
    "latest_observation_utc": "2026-10-13T13:00:00",
    "staleness_minutes": 5.0,
    "bronze_backlog": 0,
    "reject_rate_24h": 0.0,
    "api_success_rate_24h": 1.0,
    "collector_requests_24h": 1000,
    "collector_failed_requests_24h": 3,
    "collector_upload_failures_24h": 0,
}
ALL_TASKS_OK = {"bronze_ingestion": 12, "silver_transformation": 12, "gold_metrics": 80}


def test_healthy():
    assert evaluate_health(HEALTHY, ALL_TASKS_OK) == ("healthy", [])


def test_upstream_failure_is_failed():
    status, issues = evaluate_health(HEALTHY, {**ALL_TASKS_OK, "silver_transformation": None})
    assert status == "failed" and issues == ["upstream_task_failed:silver_transformation"]


def test_manual_runs_skip_upstream_checks():
    assert evaluate_health(HEALTHY, None)[0] == "healthy"


@pytest.mark.parametrize(
    ("override", "status", "issue"),
    [
        ({"latest_observation_utc": None, "staleness_minutes": None}, "failed", "no_data"),
        ({"bronze_backlog": 4}, "failed", "silver_backlog"),
        ({"staleness_minutes": 600}, "stale", "stale_data"),
        ({"reject_rate_24h": 0.2}, "degraded", "high_reject_rate"),
        ({"api_success_rate_24h": 0.5}, "degraded", "low_api_success_rate"),
        ({"collector_failed_requests_24h": 200}, "degraded", "collector_request_failures"),
        ({"collector_upload_failures_24h": 1}, "degraded", "collector_upload_failures"),
    ],
)
def test_rules(override, status, issue):
    got_status, issues = evaluate_health({**HEALTHY, **override}, ALL_TASKS_OK)
    assert got_status == status and issue in issues


def test_a_few_collector_failures_are_tolerated():
    # 3 of 1000 requests failed (0.3%) - transient errors with retries are normal
    assert evaluate_health(HEALTHY, ALL_TASKS_OK)[0] == "healthy"


def test_staleness_threshold_is_configurable():
    m = {**HEALTHY, "staleness_minutes": 100}
    assert evaluate_health(m, ALL_TASKS_OK)[0] == "healthy"
    assert evaluate_health(m, ALL_TASKS_OK, HealthThresholds(max_staleness_minutes=60))[0] == "stale"


# ------------------------------------------------------------------ Spark metrics


def _run_summary(run_id, started, requests=60, failed=0, upload_failures=0):
    return json.dumps({
        "record_type": "collector_run", "schema_version": 1, "run_id": run_id,
        "started_at_utc": started, "finished_at_utc": started.replace("T12:00", "T12:59"),
        "station_ids": ["41320"], "interval_seconds": 60, "planned_cycles": 60,
        "requests": requests, "succeeded": requests - failed, "failed": failed,
        "files_written": requests - failed, "files_uploaded": requests - failed,
        "upload_failures": upload_failures, "uploads_disabled_reason": None,
        "interrupted": False, "exit_code": 1 if failed else 0, "collector_version": "0.1.0",
        "failures": [{"station_id": "41320", "attempted_at_utc": "2026-10-13T12:05:00.000000Z",
                      "error_type": "CTATransientError", "error_code": None, "message": "down"}] * failed,
    })


def test_collector_runs_parse_and_match_table(spark):
    raw = spark.createDataFrame(
        [(_run_summary("r1", "2026-10-13T12:00:00.000000Z", failed=2), "f1"), ("{garbage", "f2")],
        "raw_json STRING, source_file_path STRING",
    )
    runs = collector_runs_from_text(raw)
    rows = runs.collect()
    assert len(rows) == 1 and rows[0].failed == 2 and len(rows[0].failures) == 2
    assert rows[0].failures[0].error_type == "CTATransientError"
    expected = spark.createDataFrame([], tables.OPS_COLLECTOR_RUNS.ddl_schema()).schema
    assert [(f.name, f.dataType.simpleString()) for f in runs.select(*tables.OPS_COLLECTOR_RUNS.column_names).schema] == [
        (f.name, f.dataType.simpleString()) for f in expected
    ]


@pytest.fixture
def pipeline_tables(bronze_df, spark):
    """Bronze/Silver/Gold for 60 minutes of simulated polling with one collector failure."""
    poller = Poller(start=T0, minutes=60, errors={10})
    bronze = bronze_df(*simulate([Train("822", "30256", at(30)), Train("999", "30257", at(170), visible_minutes=180)], poller))
    bronze = bronze.localCheckpoint()
    silver = with_processing_time(transform_bronze_batch(bronze))
    obs, preds, rejects = (silver.observations.localCheckpoint(), silver.predictions.localCheckpoint(),
                           silver.rejects.localCheckpoint())
    visits = build_gold(preds, obs, materialize=lambda _n, d: d.localCheckpoint()).train_visits
    runs = collector_runs_from_text(spark.createDataFrame(
        [(_run_summary("r1", "2026-10-13T12:00:00.000000Z", requests=61, failed=1), "f")],
        "raw_json STRING, source_file_path STRING"))
    return dict(bronze=bronze, observations=obs, rejects=rejects, visits=visits, collector_runs=runs)


def _now(minutes_after_t0):
    return F.to_timestamp(F.lit((T0 + timedelta(minutes=minutes_after_t0)).isoformat()))


def test_health_metrics_on_simulated_pipeline(pipeline_tables):
    m = compute_health_metrics(now=_now(65), **pipeline_tables).first().asDict()
    assert m["polls_24h"] == 61 and m["successful_polls_24h"] == 60
    assert m["api_success_rate_24h"] == pytest.approx(60 / 61, abs=1e-4)
    assert m["staleness_minutes"] == pytest.approx(5.0, abs=0.1)  # last poll at T0+60
    assert m["bronze_backlog"] == 0
    assert m["reject_rate_24h"] == 0.0
    assert m["coverage_pct_24h"] == pytest.approx(60 / 1440 * 100, abs=0.1)
    assert m["collector_runs_24h"] == 1 and m["collector_failed_requests_24h"] == 1
    assert m["gold_train_visits_total"] >= 1
    assert evaluate_health(m, None)[0] == "healthy"


def test_stale_and_backlog_detection(pipeline_tables, bronze_df):
    # Bronze has 3 extra files Silver hasn't processed, and it's now 5 hours later.
    extra = bronze_df(*simulate([], Poller(start=T0 + timedelta(hours=2), minutes=2)))
    tables_ = {**pipeline_tables, "bronze": pipeline_tables["bronze"].unionByName(extra)}
    m = compute_health_metrics(now=_now(300), **tables_).first().asDict()
    assert m["bronze_backlog"] == 3
    status, issues = evaluate_health(m, None)
    assert status == "failed" and {"silver_backlog", "stale_data"} <= set(issues)


def test_metrics_handle_empty_tables(spark, pipeline_tables):
    empty = {k: v.limit(0) for k, v in pipeline_tables.items()}
    m = compute_health_metrics(now=_now(0), **empty).first().asDict()
    assert m["polls_24h"] == 0 and m["api_success_rate_24h"] is None and m["coverage_pct_24h"] is None
    assert evaluate_health(m, None) == ("failed", ["no_data"])


def test_pipeline_runs_row_matches_table(spark, pipeline_tables):
    m = compute_health_metrics(now=_now(65), **pipeline_tables)
    row = m.withColumns({
        "job_run_id": F.lit("123"), "status": F.lit("healthy"), "issues": F.array().cast("array<string>"),
        "bronze_files_read": F.lit(1).cast("bigint"), "silver_rows_processed": F.lit(1).cast("bigint"),
        "gold_train_visits": F.lit(1).cast("bigint"),
    }).select(*tables.OPS_PIPELINE_RUNS.column_names)
    expected = spark.createDataFrame([], tables.OPS_PIPELINE_RUNS.ddl_schema()).schema
    assert [(f.name, f.dataType.simpleString()) for f in row.schema] == [
        (f.name, f.dataType.simpleString()) for f in expected
    ]


def test_reads_real_collector_summary_files_from_disk(spark, tmp_path):
    """Regression: files are pretty-printed JSON written by the collector itself. A line-by-line
    read (the .option('wholetext') trap) turned each file into unparseable fragments."""
    import sys
    from datetime import datetime, timezone
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from backend.jobs.scheduled_collection import RunSummary, summary_path
    from backend.storage.raw_store import write_json_exclusive

    from cta_lakehouse.ops import read_collector_run_files

    for i, day in enumerate((14, 15)):
        s = RunSummary(run_id=f"run-{i}-0000", started_at_utc=datetime(2026, 10, day, 12, tzinfo=timezone.utc),
                       station_ids=("41320",), interval_seconds=60, planned_cycles=60,
                       finished_at_utc=datetime(2026, 10, day, 12, 59, tzinfo=timezone.utc), exit_code=0)
        write_json_exclusive(summary_path(tmp_path, s), s.to_dict())
    (tmp_path / "collector_runs" / "notes.txt").write_text("ignore me")

    raw = read_collector_run_files(spark, str(tmp_path / "collector_runs"))
    assert raw.count() == 2  # one row per file, not per line
    runs = collector_runs_from_text(raw).orderBy("run_id").collect()
    assert [r.run_id for r in runs] == ["run-0-0000", "run-1-0000"]
    assert runs[0].planned_cycles == 60 and runs[0].failures == []
