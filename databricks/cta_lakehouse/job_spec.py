"""Lakeflow Job definition for the Bronze -> Silver -> Gold -> health pipeline, as code.

``build_job_settings`` returns the Jobs API 2.2 settings dict, which is version-controlled and unit
tested. ``databricks/deploy_job.py`` creates or updates the job from it.

Reliability choices:
  * tasks run strictly in order (``depends_on``), so Gold never reads a half-written Silver layer
  * data tasks retry twice, 2 minutes apart. Safe because every layer is idempotent (Auto Loader
    checkpoints + MERGE on keys; Gold is a full overwrite)
  * ``max_concurrent_runs = 1`` with queueing: a slow run delays the next instead of overlapping
    it, which would fight over streaming checkpoints
  * ``pipeline_health`` runs even when an upstream task failed (``run_if = ALL_DONE``) and records
    the failure in ``cta_ops.pipeline_runs``
  * email on failure; per-task and per-job timeouts
"""

from __future__ import annotations

from typing import Any

JOB_NAME = "cta-lakehouse-pipeline"
DEFAULT_CRON = "0 10 0/2 * * ?"  # every 2 hours at :10 (Quartz syntax)
TIMEZONE = "America/Chicago"

DATA_TASK_RETRIES = 2
RETRY_INTERVAL_MS = 120_000
TASK_TIMEOUT_SECONDS = 30 * 60
JOB_TIMEOUT_SECONDS = 90 * 60


def build_job_settings(
    notebook_dir: str,
    *,
    catalog: str = "workspace",
    bronze_schema: str = "cta_bronze",
    silver_schema: str = "cta_silver",
    gold_schema: str = "cta_gold",
    ops_schema: str = "cta_ops",
    landing_volume: str = "landing",
    alert_emails: list[str] | None = None,
    cron: str = DEFAULT_CRON,
    paused: bool = False,
) -> dict[str, Any]:
    """Jobs API settings. ``notebook_dir`` is the workspace folder holding the notebooks
    (the repo's ``databricks/`` folder inside a Git folder)."""
    notebook_dir = notebook_dir.rstrip("/")

    def data_task(key: str, notebook: str, params: dict[str, str], depends_on: str | None) -> dict[str, Any]:
        task: dict[str, Any] = {
            "task_key": key,
            "description": f"Runs databricks/{notebook}.py",
            "notebook_task": {
                "notebook_path": f"{notebook_dir}/{notebook}",
                "source": "WORKSPACE",
                "base_parameters": params,
            },
            "max_retries": DATA_TASK_RETRIES,
            "min_retry_interval_millis": RETRY_INTERVAL_MS,
            "retry_on_timeout": True,
            "timeout_seconds": TASK_TIMEOUT_SECONDS,
        }
        if depends_on:
            task["depends_on"] = [{"task_key": depends_on}]
        return task

    tasks = [
        data_task(
            "bronze_ingestion", "bronze_ingestion",
            {"catalog": catalog, "schema": bronze_schema, "landing_volume": landing_volume, "reset": "false"},
            None,
        ),
        data_task(
            "silver_transformation", "silver_transformation",
            {"catalog": catalog, "bronze_schema": bronze_schema, "silver_schema": silver_schema, "full_refresh": "false"},
            "bronze_ingestion",
        ),
        data_task(
            "gold_metrics", "gold_metrics",
            {"catalog": catalog, "silver_schema": silver_schema, "gold_schema": gold_schema},
            "silver_transformation",
        ),
        {
            "task_key": "pipeline_health",
            "description": "Records run status, freshness and coverage in cta_ops.pipeline_runs (runs even if upstream failed)",
            "depends_on": [{"task_key": "gold_metrics"}],
            "run_if": "ALL_DONE",
            "notebook_task": {
                "notebook_path": f"{notebook_dir}/pipeline_health",
                "source": "WORKSPACE",
                "base_parameters": {
                    "catalog": catalog,
                    "bronze_schema": bronze_schema,
                    "silver_schema": silver_schema,
                    "gold_schema": gold_schema,
                    "ops_schema": ops_schema,
                    "landing_volume": landing_volume,
                    "job_run_id": "{{job.run_id}}",
                    "fail_on_stale": "false",
                },
            },
            "max_retries": 0,
            "timeout_seconds": TASK_TIMEOUT_SECONDS,
        },
    ]

    settings: dict[str, Any] = {
        "name": JOB_NAME,
        "description": (
            "CTA Transit Intelligence: Bronze ingestion -> Silver validation -> Gold metrics -> health check. "
            "Managed by databricks/deploy_job.py - edit the code, not the UI."
        ),
        "tags": {"project": "cta-transit-intelligence", "managed_by": "deploy_job.py"},
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "timeout_seconds": JOB_TIMEOUT_SECONDS,
        "schedule": {
            "quartz_cron_expression": cron,
            "timezone_id": TIMEZONE,
            "pause_status": "PAUSED" if paused else "UNPAUSED",
        },
        "tasks": tasks,
    }
    if alert_emails:
        settings["email_notifications"] = {"on_failure": list(alert_emails), "no_alert_for_skipped_runs": True}
    return settings
