"""The job definition (no Spark needed)."""

import json

from cta_lakehouse.job_spec import JOB_NAME, build_job_settings

DIR = "/Users/someone@example.com/cta-pipeline"


def _tasks(settings):
    return {t["task_key"]: t for t in settings["tasks"]}


def test_tasks_run_strictly_in_order():
    t = _tasks(build_job_settings(DIR))
    assert "depends_on" not in t["bronze_ingestion"]
    assert t["silver_transformation"]["depends_on"] == [{"task_key": "bronze_ingestion"}]
    assert t["gold_metrics"]["depends_on"] == [{"task_key": "silver_transformation"}]
    assert t["pipeline_health"]["depends_on"] == [{"task_key": "gold_metrics"}]


def test_health_runs_even_after_failures_and_is_not_retried():
    h = _tasks(build_job_settings(DIR))["pipeline_health"]
    assert h["run_if"] == "ALL_DONE" and h["max_retries"] == 0
    assert h["notebook_task"]["base_parameters"]["job_run_id"] == "{{job.run_id}}"


def test_data_tasks_retry_and_time_out():
    for key in ("bronze_ingestion", "silver_transformation", "gold_metrics"):
        task = _tasks(build_job_settings(DIR))[key]
        assert task["max_retries"] == 2 and task["retry_on_timeout"] is True
        assert task["timeout_seconds"] > 0 and task["min_retry_interval_millis"] >= 60_000


def test_no_overlapping_runs_and_queueing():
    s = build_job_settings(DIR)
    assert s["max_concurrent_runs"] == 1 and s["queue"] == {"enabled": True}


def test_notebook_paths_and_parameters():
    t = _tasks(build_job_settings(DIR + "/", catalog="workspace"))
    assert t["bronze_ingestion"]["notebook_task"]["notebook_path"] == f"{DIR}/bronze_ingestion"
    assert t["silver_transformation"]["notebook_task"]["base_parameters"]["full_refresh"] == "false"
    assert t["bronze_ingestion"]["notebook_task"]["base_parameters"]["reset"] == "false"
    # Serverless: no cluster configuration on any task
    assert all("new_cluster" not in x and "existing_cluster_id" not in x for x in t.values())


def test_schedule_and_notifications():
    s = build_job_settings(DIR, alert_emails=["me@example.com"], cron="0 0 * * * ?", paused=True)
    assert s["name"] == JOB_NAME
    assert s["schedule"] == {"quartz_cron_expression": "0 0 * * * ?", "timezone_id": "America/Chicago", "pause_status": "PAUSED"}
    assert s["email_notifications"]["on_failure"] == ["me@example.com"]
    assert "email_notifications" not in build_job_settings(DIR)


def test_settings_contain_no_secrets():
    text = json.dumps(build_job_settings(DIR, alert_emails=["me@example.com"]))
    assert "token" not in text.lower() and "CTA_API_KEY" not in text


def test_deploy_sync_plan_covers_every_task_notebook_and_the_package():
    import importlib.util
    import sys
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "deploy_job.py"
    spec = importlib.util.spec_from_file_location("deploy_job", path)
    deploy = importlib.util.module_from_spec(spec)
    sys.modules["deploy_job"] = deploy  # @dataclass needs the module registered
    spec.loader.exec_module(deploy)

    items = deploy.sync_plan(DIR)
    notebooks = {i.remote for i in items if i.kind == "notebook"}
    task_paths = {t["notebook_task"]["notebook_path"] for t in build_job_settings(DIR)["tasks"]}
    assert task_paths <= notebooks  # every task's notebook gets deployed
    files = {i.remote.rsplit("/", 1)[1] for i in items if i.kind == "file"}
    assert {"__init__.py", "bronze.py", "silver.py", "gold.py", "ops.py", "tables.py", "job_spec.py"} <= files
    assert all(i.remote.startswith(f"{DIR}/cta_lakehouse/") for i in items if i.kind == "file")
