# Operations runbook

How the pipeline runs unattended, how to tell whether it's healthy, and how to recover when it
isn't. Every step is safe to repeat: each layer is idempotent, so **re-running is always the first
recovery step**.

```
 GitHub Actions (hourly)                       Databricks (every 2 h, Lakeflow Job)
 ───────────────────────                       ─────────────────────────────────────────────
 scheduled_collection ──upload every 15 min──► landing volume ──► bronze_ingestion
   ├─ polls CTA every 60 s                                         └► silver_transformation
   ├─ data/raw/**/arrivals_*.json                                     └► gold_metrics
   └─ data/raw/collector_runs/*.json  (run summary,                      └► pipeline_health ──► cta_ops.pipeline_runs
                                       incl. failed requests)                (always runs)     cta_ops.collector_runs
```

## 1. Set up

### Scheduled collection (GitHub Actions)
The **Collector** workflow (`.github/workflows/collector.yml`) runs hourly at :10. Each run polls
every 60 s until the first :30 at least 30 minutes away, uploads to the landing volume every 15
minutes, and checks that every file got there before the runner is discarded.

GitHub often starts scheduled runs late, so a run ends on the half hour rather than after a fixed
time. The next run waits in the queue and starts as soon as the previous one finishes. That leaves
a polling gap of about a minute each hour. A run that GitHub starts more than 20 minutes late adds a
gap of the extra delay.

| Setting | Where (Settings → Secrets and variables → Actions) |
|---|---|
| `CTA_API_KEY`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | Secrets |
| `CTA_STATION_IDS` (optional, default `41320,40380,40900`) | Variables |

| Task | How |
|---|---|
| Check | Actions → Collector. Each run's log is in its **Collect** step |
| Run now | Actions → Collector → **Run workflow**, or `gh workflow run collector.yml` |
| Pause / resume | `gh workflow disable collector.yml` / `gh workflow enable collector.yml` |

Runs are free because the repository is public. That's about 24 runner-hours a day, far more than a
private repository's free minutes. GitHub disables scheduled workflows after 60 days without a
commit; re-enable them on the Actions page.

### Scheduled collection on your own machine (alternative)
| OS | Install | Remove | Check |
|---|---|---|---|
| Windows | `.\scripts\schedule_collection_windows.ps1` | `... -Unregister` | `Get-ScheduledTaskInfo -TaskName "CTA Collector"` |
| macOS | `scripts/schedule_collection_macos.sh install` | `... uninstall` | `... status` |

**Don't run this and the Collector workflow at the same time.** Every poll is a separate
observation, so two collectors double-count polls.

Each hourly run is a 59-minute session, so consecutive runs leave one normal polling gap at the
boundary. The machine must be on and logged in. Laptop sleep shows up as stale data and coverage
gaps; it doesn't break anything.

Logs: `data/logs/collector.log` (rotating). Test without the scheduler:
`python -m backend.jobs.scheduled_collection --duration-minutes 2`

### Lakeflow Job (Databricks)
```powershell
git commit ...                                    # deploy only committed code
python databricks/deploy_job.py --dry-run         # review
python databricks/deploy_job.py --run-now         # deploy + one verified run
```
Re-run `deploy_job.py` after every change to `databricks/`. It re-syncs the code to
`/Users/<you>/cta-pipeline` and updates the job in place. **Don't edit the job or that folder in the UI.**
Changes there are overwritten on the next deploy.

| Setting | Value | Why |
|---|---|---|
| Schedule | every 2 h at :10, America/Chicago | Fresh enough for analytics; light on the Free Edition quota |
| Task order | bronze → silver → gold → health | Gold never reads a half-written Silver |
| Retries | data tasks ×2, 2 min apart | Safe because every layer is idempotent |
| Concurrency | 1 run, others queue | Streaming checkpoints must not be shared by two runs |
| Timeouts | 30 min per task, 90 min per job | A hung task can't block the schedule forever |
| Alerts | email to you on failure | |
| Health task | `run_if: ALL_DONE`, no retries | Records failures too |

To pause: `python databricks/deploy_job.py --paused`, or use the job page's **Pause** button.

## 2. Is it healthy?

```sql
SELECT checked_at_utc, status, issues, staleness_minutes, polls_24h, coverage_pct_24h,
       bronze_backlog, collector_failed_requests_24h
FROM workspace.cta_ops.pipeline_runs ORDER BY checked_at_utc DESC LIMIT 12;
```

| Status | Meaning | Typical cause |
|---|---|---|
| `healthy` | All tasks succeeded, data is fresh, no warnings | |
| `degraded` | Working, with warnings: rejects > 5%, API success < 90%, collector failures > 5%, upload failures | CTA outage, network trouble |
| `stale` | Pipeline fine, but no new observation for > 3 h | Collector workflow failing or disabled (or, for a local collector, the machine is off or asleep) |
| `failed` | An upstream task failed, Silver is behind Bronze, or there's no data at all | See section 3 |

Collector-side failures (requests that never produced a file):
```sql
SELECT started_at_utc, requests, failed, upload_failures, uploads_disabled_reason, failures
FROM workspace.cta_ops.collector_runs ORDER BY started_at_utc DESC LIMIT 24;
```

## 3. Recovery playbook

| Symptom | Diagnose | Fix |
|---|---|---|
| Job failed email | Job page → failed run → red task → output | Fix the cause, then **Repair run**, which re-runs only the failed tasks and their dependents |
| `stale` status | Actions → Collector: are runs failing, or not running at all? Open the latest run's **Collect** step | Fix the cause (usually a secret), then **Run workflow** to test. No recent runs: GitHub disabled the schedule after 60 days without commits; re-enable it. Local collector: wake or start the machine, check the scheduler (`status` / `Get-ScheduledTaskInfo`), and test with a 2-minute run |
| `uploads_disabled_reason` set, or Collector runs fail in "Upload anything not yet in Databricks" | Token expired or wrong | New token (Settings → Developer), update the `DATABRICKS_TOKEN` GitHub secret (and `.env`, if you collect locally). Then restore the files those runs kept (next row) |
| A failed Collector run has a `raw-data-<run id>` artifact | Its files never reached Databricks | Once uploads work again: `gh run download <run id> -n raw-data-<run id> -D data/raw`, then `python -m backend.uploaders.volume_uploader`. Artifacts expire after 14 days |
| Dashboard says "Exported" more than ~3 h ago | Actions → Dashboard: did the scheduled run happen? Open its "Export fresh data from Databricks" step | Missing-secret notice: add `DATABRICKS_HOST`/`DATABRICKS_TOKEN`. Auth error: the token expired; make a new one and update the secret (and `.env`). No recent runs: GitHub disabled the schedule after 60 days without commits; re-enable it |
| `collector_request_failures` | `collector_runs.failures` (error_type, message) | `CTATransientError`: network or CTA outage, retried automatically. `CTAApiError 101/102`: bad key or daily limit; fix the `CTA_API_KEY` secret or reduce stations/frequency |
| Run skipped, exit code 3 | Previous run still holds `data/.collector.lock` | Normal if a run overlapped. A lock from a crash is replaced automatically after ~90 min, or delete the file |
| `silver_backlog` | Silver failed after Bronze succeeded | Re-run the job (or Repair). Silver resumes from its checkpoint |
| Bronze fails reading files | Error mentions a file or checkpoint | Re-run. If the checkpoint is corrupt, run `bronze_ingestion` with `reset = true`; MERGE prevents duplicates |
| Silver/Gold logic changed | | Deploy, then run `silver_transformation` with `full_refresh = true` once. Gold rebuilds itself every run |
| Bad data reached a table | `DESCRIBE HISTORY <table>` | `RESTORE TABLE <table> TO VERSION AS OF <n>`, then fix and re-run |
| Need to rebuild everything | | Raw files in the volume are the source of truth: drop the Bronze/Silver tables, re-run with `reset = true` / `full_refresh = true` |
| Daily quota exceeded | Compute unavailable until tomorrow | Nothing is lost: files keep landing and the next run catches up. Consider `--cron "0 10 0/4 * * ?"` (every 4 h) |
| CTA changed its response format | Rejects spike (`high_reject_rate`) | Bronze kept the raw JSON. Update Silver, deploy, `full_refresh` |
| Dashboard map says "live feed unavailable" or "Reconnecting" | `curl https://cta-transit-live.vercel.app/api/live`: the `error` field names the failing CTA call; `npx vercel logs` in `live-api/` | CTA outage: nothing to do, it recovers on its own. `CTA error 101/102`: fix `CTA_API_KEY` in Vercel and redeploy, or give the function its own key. Analytics are unaffected either way |
| Every `cta-transit-live.vercel.app` URL returns 404 `NOT_FOUND` | Vercel → project → Settings → Build: is Root Directory `live-api`? The deployment's build log finishes in ~100 ms with no TypeScript step | Set Root Directory to `live-api` and redeploy. To restore service at once, promote the last good deployment: `npx vercel promote <deployment-url>` |

**Why re-running is always safe:** the collector never overwrites files (UUID names, exclusive
create); the uploader skips files already in the volume; Bronze reads each file once (Auto Loader
checkpoint) and MERGEs on `collection_id`; Silver streams Bronze with a checkpoint and MERGEs on
row keys; Gold is fully recomputed and atomically overwritten.

## 4. Cost and quota (Free Edition)
Each job run is 4 short serverless notebook tasks, roughly 3–6 minutes of compute. At every 2
hours that's about 12 runs a day. If you hit the fair-use quota, lower the frequency. Data is never
lost, only delayed.
