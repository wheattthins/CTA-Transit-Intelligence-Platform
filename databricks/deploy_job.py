"""Deploy the lakehouse pipeline: sync the code to the workspace and create/update the Lakeflow Job.

    python databricks/deploy_job.py --dry-run        # show the plan; change nothing
    python databricks/deploy_job.py                  # sync code + create/update the job
    python databricks/deploy_job.py --run-now        # ...then run it once and wait for the result
    python databricks/deploy_job.py --paused         # deploy with the schedule paused

What it does:
  1. Uploads the notebooks and the ``cta_lakehouse`` package from the *committed* code to a
     dedicated workspace folder (default ``/Users/<you>/cta-pipeline``). The job runs that copy, so
     it always matches a git commit, and your interactive notebooks are never touched.
  2. Creates the job ``cta-lakehouse-pipeline`` or, if it exists, replaces its settings with the
     definition in ``cta_lakehouse/job_spec.py`` (so re-running deploy is idempotent).

Authentication: DATABRICKS_HOST / DATABRICKS_TOKEN from .env (same as the uploader).
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATABRICKS_DIR = REPO_ROOT / "databricks"
sys.path.insert(0, str(DATABRICKS_DIR))

from cta_lakehouse.job_spec import DEFAULT_CRON, JOB_NAME, build_job_settings  # noqa: E402

NOTEBOOKS = ("bronze_ingestion", "silver_transformation", "gold_metrics", "pipeline_health")
PACKAGE = "cta_lakehouse"
NOTEBOOK_HEADER = "# Databricks notebook source"


@dataclass(frozen=True)
class SyncItem:
    local: Path
    remote: str
    kind: str  # "notebook" | "file"


def sync_plan(target_dir: str) -> list[SyncItem]:
    """Every file the job needs, and where it goes in the workspace."""
    items = []
    for name in NOTEBOOKS:
        local = DATABRICKS_DIR / f"{name}.py"
        if not local.read_text(encoding="utf-8").startswith(NOTEBOOK_HEADER):
            raise ValueError(f"{local} is missing the '{NOTEBOOK_HEADER}' header")
        items.append(SyncItem(local, f"{target_dir}/{name}", "notebook"))
    for local in sorted((DATABRICKS_DIR / PACKAGE).glob("*.py")):
        items.append(SyncItem(local, f"{target_dir}/{PACKAGE}/{local.name}", "file"))
    return items


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, check=True, capture_output=True, text=True).stdout.strip()


def git_state() -> tuple[str, bool]:
    commit = _git("rev-parse", "--short", "HEAD")
    dirty = bool(_git("status", "--porcelain", "--", "databricks/"))
    return commit, dirty


def sync_code(w, items: list[SyncItem], target_dir: str) -> None:
    from databricks.sdk.service.workspace import ImportFormat, Language

    w.workspace.mkdirs(f"{target_dir}/{PACKAGE}")
    w.workspace.import_(
        f"{target_dir}/README.md",
        content=base64.b64encode(
            b"# Managed by databricks/deploy_job.py\n\nDeployed copy of the pipeline code. "
            b"Do not edit here - changes are overwritten on the next deploy. Edit in git and redeploy.\n"
        ).decode(),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    for item in items:
        content = base64.b64encode(item.local.read_bytes()).decode()
        if item.kind == "notebook":
            w.workspace.import_(item.remote, content=content, format=ImportFormat.SOURCE,
                                language=Language.PYTHON, overwrite=True)
        else:
            w.workspace.import_(item.remote, content=content, format=ImportFormat.AUTO, overwrite=True)
        print(f"  synced {item.local.relative_to(REPO_ROOT)} -> {item.remote}")

    # Remove package files that no longer exist in git (e.g. a renamed module).
    expected = {i.remote for i in items if i.kind == "file"}
    for obj in w.workspace.list(f"{target_dir}/{PACKAGE}"):
        if obj.path not in expected and obj.path.endswith(".py"):
            w.workspace.delete(obj.path)
            print(f"  removed stale {obj.path}")


def upsert_job(w, settings: dict) -> tuple[int, str]:
    existing = list(w.jobs.list(name=JOB_NAME))
    if len(existing) > 1:
        raise RuntimeError(f"Found {len(existing)} jobs named {JOB_NAME!r}; delete the duplicates first.")
    if existing:
        job_id = existing[0].job_id
        w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": settings})
        return job_id, "updated"
    response = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)
    return int(response["job_id"]), "created"


def run_and_wait(w, job_id: int, timeout_minutes: int = 60) -> bool:
    run_id = w.jobs.run_now(job_id=job_id).run_id
    print(f"Started run {run_id}: {w.config.host}/jobs/{job_id}/runs/{run_id}")
    deadline = time.time() + timeout_minutes * 60
    last = None
    while time.time() < deadline:
        run = w.jobs.get_run(run_id)
        states = {t.task_key: (t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else "?",
                               t.state.result_state.value if t.state and t.state.result_state else "")
                  for t in (run.tasks or [])}
        if states != last:
            print("  " + "  ".join(f"{k}={v[0]}{('/' + v[1]) if v[1] else ''}" for k, v in states.items()))
            last = states
        life = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else ""
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = run.state.result_state.value if run.state.result_state else life
            print(f"Run finished: {result}")
            return result == "SUCCESS"
        time.sleep(15)
    print("Timed out waiting for the run; it is still going in Databricks.")
    return False


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Sync pipeline code and create/update the Lakeflow Job.")
    p.add_argument("--target-dir", help="Workspace folder for the deployed code (default /Users/<you>/cta-pipeline)")
    p.add_argument("--cron", default=DEFAULT_CRON, help=f"Quartz cron schedule (default '{DEFAULT_CRON}' = every 2h)")
    p.add_argument("--paused", action="store_true", help="Deploy with the schedule paused")
    p.add_argument("--no-email", action="store_true", help="Don't email you on failure")
    p.add_argument("--run-now", action="store_true", help="Trigger one run after deploying and wait for it")
    p.add_argument("--allow-dirty", action="store_true", help="Deploy even with uncommitted changes in databricks/")
    p.add_argument("--dry-run", action="store_true", help="Print the plan and job settings; change nothing")
    args = p.parse_args(argv)

    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    me = w.current_user.me().user_name
    target_dir = (args.target_dir or f"/Users/{me}/cta-pipeline").rstrip("/")

    commit, dirty = git_state()
    if dirty and not (args.allow_dirty or args.dry_run):
        print("databricks/ has uncommitted changes. Commit first (the job should run code that's in git), "
              "or pass --allow-dirty.")
        return 2

    settings = build_job_settings(
        target_dir, alert_emails=None if args.no_email else [me], cron=args.cron, paused=args.paused
    )
    settings["tags"]["git_commit"] = commit + ("-dirty" if dirty else "")
    items = sync_plan(target_dir)

    print(f"Workspace: {w.config.host}  user: {me}")
    print(f"Code: commit {commit}{' (uncommitted changes!)' if dirty else ''} -> {target_dir}")
    if args.dry_run:
        for i in items:
            print(f"  would sync {i.local.relative_to(REPO_ROOT)} -> {i.remote} ({i.kind})")
        print(json.dumps(settings, indent=2))
        return 0

    sync_code(w, items, target_dir)
    job_id, action = upsert_job(w, settings)
    schedule = "PAUSED" if args.paused else f"'{args.cron}' {settings['schedule']['timezone_id']}"
    print(f"Job {action}: {JOB_NAME} (id {job_id}), schedule {schedule}")
    print(f"  {w.config.host}/jobs/{job_id}")

    if args.run_now:
        return 0 if run_and_wait(w, job_id) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
