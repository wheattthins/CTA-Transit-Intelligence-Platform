# Architecture

## Target pipeline

```
 CTA APIs ──► Python collectors ──► raw JSON files ──► Databricks Bronze ──► Silver ──► Gold ──► React dashboard
             (local / scheduled)   (immutable,         (Delta, append,      (clean,    (KPIs)
                                    partitioned)        source metadata)     dedup)
```

Collection is kept **outside** Databricks. Free Edition restricts outbound internet access,
and decoupling means a Databricks outage never loses observations: files queue up locally and
are ingested later.

## Phase 1 decisions

### Raw data is the system of record
The collector saves the CTA response body **exactly as received**, inside an envelope that
adds collection metadata. Nothing is dropped or renamed. Parsing in the collector is only for
display and sanity checks. Authoritative normalization happens in Silver, where it can be
re-run over full history if the logic changes.

### Envelope metadata
| Field | Purpose |
|---|---|
| `collection_id` | UUID, the unique key for deduplicating ingestion in Bronze |
| `requested_at_utc` / `received_at_utc` | When *we* observed the data (UTC) |
| `request_params` | Reproducibility; the API key is never stored |
| `http_status`, `elapsed_ms`, `attempts` | Operational monitoring (API health, latency) |
| `is_sample`, `source_system` | Prevents fixture data from contaminating analytics |
| `schema_version`, `collector_version` | Lets Bronze handle future envelope changes |

### File layout
`collection_date=YYYY-MM-DD/station_id=NNNNN/` is Hive-style partitioning, which Spark and
Databricks Auto Loader recognize, so these become partition columns automatically. The
date is the **UTC** collection date, so it can differ from the Chicago service date late in
the evening. Silver will derive a Chicago `service_date`.

Files are written to a temp file and then hard-linked into place. A reader never sees a
half-written file, and an existing observation is never replaced.

### Time zones
CTA timestamps (`tmst`, `prdt`, `arrT`) are **America/Chicago local time with no offset**.
They are stored untouched in raw files. Code converts them to UTC with `zoneinfo`. One known
edge case: during the November DST fall-back, 1:00–2:00 AM occurs twice and the local
timestamp is ambiguous. Silver will need to resolve it using `tmst` ordering. Our own
timestamps are always UTC.

### Error handling
| Failure | Behavior |
|---|---|
| Network error, timeout, HTTP 5xx/429, CTA `errCd 900` | Retry with exponential backoff (default 3 attempts) |
| HTTP 4xx, non-JSON body, missing `ctatt` | Fail that request, continue the session |
| CTA `errCd` 101 (bad key) / 102 (daily limit) | Save the error response, **stop the session** |
| Other CTA error codes | Save the error response, continue |
| Malformed individual `eta` records | Count as rejected, keep the rest; raw file is untouched |

### Secret handling
The key comes only from the environment or `.env`. `Settings` and `CTAClient` hide it from
`repr`. `urllib3` debug logging (which prints full URLs) is suppressed. Network exception
messages are redacted and not chained. The saved `request_params` exclude it. Tests assert
all of this.

### Deliberate omissions
- No infinite polling loop. Sessions are bounded, and continuous collection will be a
  scheduler's job (Phase 5).
- No `frontend/` placeholder yet. It will be added when built.
- `pandas` is not installed yet because nothing in Phase 1 needs it.

## Phase 2 decisions: Bronze

**Landing:** `backend/uploaders/volume_uploader.py` copies `data/raw` into the Unity Catalog volume
`/Volumes/workspace/cta_bronze/landing/`, keeping the same partition folders. It lists each remote
partition once and uploads only missing files, so it is idempotent and never overwrites. Files
marked `is_sample` are refused.

**Table:** `workspace.cta_bronze.train_tracker_arrivals` has one row per API response (per file).
- `raw_json` holds the **entire file as text**, read with Auto Loader `text` + `wholetext`. If CTA adds,
  removes or renames a field, Bronze keeps working and loses nothing. Typed parsing of `eta`
  is Silver's job.
- Envelope metadata is extracted into columns (`collection_id`, `station_id`, `received_at_utc`,
  `cta_error_code`, `eta_count`, ...) for deduplication, monitoring and partition-free filtering.
- Lineage columns: `source_file_path`, `source_file_size`, `source_file_modified_at`,
  `ingested_at_utc`, `ingestion_batch_id`.
- Corrupt files are **kept** with `is_parse_error = true` and `record_key = sha2(raw_json)`.
- Change Data Feed is enabled so Silver can process only new Bronze rows.

**Incremental processing and deduplication:**
1. The Auto Loader checkpoint means each file is read once.
2. Inside `foreachBatch`, `MERGE ... ON record_key WHEN NOT MATCHED THEN INSERT` means each
   observation is stored once, even if the checkpoint is reset or a file is uploaded twice.

**Why not partition the table?** At this volume (thousands to low millions of small rows),
partitioning would create many tiny files. Delta's data skipping is enough. Liquid clustering on
`collection_date` can be added later without rewriting code.

**Code layout:** transformations live in the `databricks/cta_lakehouse/` package (pure PySpark,
no Databricks-only APIs) and are unit-tested locally. Notebooks only do I/O: Auto Loader, MERGE,
validation. Notebooks find the package next to themselves in the Git folder and register it with
cloudpickle, because serverless runs `foreachBatch` functions in a separate Python process that
needs the module's code.

**Testing gap:** Auto Loader, Unity Catalog volumes and MERGE are Databricks-only, so they are
verified by running the notebooks, whose validation cells check the guarantees.

## Phase 3 decisions: Silver

Three tables in `workspace.cta_silver`, all defined in `cta_lakehouse/tables.py`. A test asserts
that the transformation output matches each definition column for column.

| Table | Grain | Key |
|---|---|---|
| `train_tracker_observations` | one row per API call (success or CTA error) | `collection_id` |
| `train_arrival_predictions` | one row per valid `eta` element per call | `prediction_key` = sha256(collection_id:eta_index) |
| `train_tracker_rejects` | one row per rejected file or `eta` element | `reject_key` |

**Why a separate observations table?** Gold must tell "no trains" apart from "no data". Every poll,
including CTA error responses and polls returning zero predictions, is recorded with its time, so
coverage and gaps can be measured rather than guessed.

**Validation.** Each record gets a `rejection_reasons` array (hard failures, so the record goes to rejects)
and a `quality_warnings` array (soft issues, so the record is kept). All reasons are reported, not just the
first, which makes the rejects table useful for debugging.

| Hard rejection | Rule |
|---|---|
| `missing_<field>` | `staId`, `stpId`, `rn`, `rt`, `prdt`, `arrT` must be non-blank |
| `invalid_prdt` / `invalid_arrT` | Must parse as `yyyy-MM-ddTHH:mm:ss` |
| `unknown_route` | `rt` must be one of the 8 CTA route codes |
| `invalid_station_id` / `invalid_stop_id` | `4xxxx` / `3xxxx` |
| `station_mismatch` | `staId` must equal the requested station |
| `arrival_before_prediction` | `arrT` < `prdt` |
| `prediction_horizon_too_long` | `arrT - prdt` > 180 min |
| `invalid_isApp/isSch/isDly/isFlt` | Must be `0` or `1` |
| `duplicate_in_response` | Same stop + run + `arrT` twice in one response (first kept) |
| `unparseable_envelope`, `sample_data` | Whole-file rejects |

| Soft warning | Effect |
|---|---|
| `missing_coordinates_for_live_train` | Recorded only (scheduled trains legitimately lack coordinates) |
| `coordinates_out_of_range` | Coordinates outside the Chicago area are nulled |
| `invalid_heading` | Heading outside 0-359 is nulled |
| `stale_prediction` | Prediction generated more than 5 min before we observed it |

**Duplicates have three meanings.** (1) Same observation twice: prevented by Bronze's MERGE.
(2) Same prediction twice in one response: rejected. (3) Same CTA prediction seen in consecutive
polls: **kept**, because each sighting is a real observation, and linked by `prediction_fingerprint`
(sha256 of stop, run, `prdt`, `arrT`). The first real data showed 2 of 64 rows repeated across polls 33
seconds apart, consistent with CTA caching responses.

**Run numbers are not unique per visit.** Brown and Purple Line trains pass Belmont twice (to and
from the Loop) under the same run number, at different stops. Validation treats them as distinct, and
Gold must group by `(run_number, stop_id)` *and* a time window.

**Time.** All instants are stored as UTC `TIMESTAMP`. Chicago wall-clock copies
(`predicted_arrival_chicago`, `observed_at_chicago`) are `TIMESTAMP_NTZ`, so rush-hour logic doesn't
depend on session settings. During the November fall-back hour, Spark resolves ambiguous local
times to the earlier (CDT) instant. This is tested and documented, not hidden.

**Incremental and idempotent.** Silver streams from the Bronze Delta table (`availableNow`, with a
checkpoint) and MERGEs on each table's key. A `full_refresh` widget drops and rebuilds Silver from
Bronze after logic changes. Bronze is the replayable source of truth.

**Hard checks that fail the notebook:** key uniqueness in every table; Bronze rows = observations +
response-level rejects; per observation, valid + rejected = `eta_count`; prediction rows = sum of
`valid_prediction_count`.

## Phase 4 decisions: Gold

### The core problem
CTA Train Tracker publishes **predictions only**. It never says when a train actually arrived. Any
claim about prediction accuracy or headways therefore depends on *inferring* arrivals. The method
must be defensible, its uncertainty quantified, and its output never presented as measured data.

### Method: the disappearance window
CTA documents that trains which reach a station drop out of the arrivals feed. For each **train
visit** (one run approaching one platform):

```
last_seen_at      CTA snapshot (tmst) of the last response listing the train
first_missing_at  next successful snapshot for the station without it
inferred arrival  = midpoint of the window,   uncertainty = ± window / 2
```

- **Not circular.** The estimate uses *when the train disappeared*, never CTA's predicted times, so
  scoring predictions against it measures something real. (`final_prediction_offset_seconds` is
  reported separately as a consistency check.)
- **CTA snapshot time, not poll time.** CTA caches responses, and two polls can return the same
  snapshot. Windows use `tmst`.
- **Visits** are split by `(station, stop, route, run)` and start anew when the predicted arrival
  jumps by more than 30 minutes or the train goes unseen for more than 60 minutes. That handles Loop
  lines passing twice and runs that come back hours later.

### Outcomes and confidence

| Outcome | When |
|---|---|
| `inferred_arrival` | Disappeared with a window ≤ 15 min, and was not still predicted > 5 min away |
| `vanished_before_arrival` | Disappeared while still predicted > 5 min after the window, e.g. cancelled, re-routed or renumbered. **Not** counted as an arrival |
| `unresolved` | Collection ended first, or a collection gap > 15 min spans the disappearance |

| Confidence | Criteria |
|---|---|
| **high** | window ≤ 150 s, last seen ≤ 3 min from predicted arrival, ≥ 2 sightings, next snapshot non-empty, live (not schedule-based) |
| **medium** | window ≤ 300 s, last seen ≤ 10 min out; or a single sighting; or the next snapshot was empty |
| **low** | anything wider or farther; or schedule-based only |

`confidence_reasons` records every downgrade. Accuracy metrics use **high and medium only**, and
**live predictions only**: schedule-based predictions are excluded.

**Empty responses.** A failed request proves nothing and is ignored. A successful but empty
response is accepted as evidence, because a quiet station's last train leaves an empty list behind,
but it caps confidence at medium since an empty list could also be a glitch.

### Validation without ground truth
Real data can't validate the method because there is no ground truth. `databricks/tests/simulate.py` therefore
generates CTA-shaped polling from **known** arrival times, following the documented behaviour
(listed until arrival, cached snapshots, prediction errors that converge as the train approaches).
Tests check that the inferred arrival lands within its stated uncertainty, that collection gaps and
API errors lower confidence, that vanished trains are excluded, and that headways, bunching and gaps
match the simulated schedule.

The first real run (one hour of Belmont, AM peak) is consistent with CTA's published claim of
roughly ±1 minute accuracy: Red Line MAE 29 s, with 83% of predictions within 1 minute. That's
16 trains, so it is indicative only.

### Metrics
- **Prediction error** = predicted − inferred arrival (+ means the train came sooner than predicted),
  bucketed by CTA's lead time (0–2, 2–5, 5–10, 10–15, 15–20, 20+ min).
- **Headway** = time between consecutive inferred arrivals of the same route at the same platform.
  It is *reliable* only if both arrivals are high/medium confidence and no collection gap (> 3 min) lies
  between them, because a missed train would double the headway.
- **Unexpected gap**: reliable headway ≥ 2× the typical (median) headway for that platform, route,
  day type and period, and ≥ 10 min. Needs ≥ 5 headways to establish "typical".
- **Bunching**: reliable headway < 2 min.
- **Consistency**: headway coefficient of variation (stddev / mean). 0 means perfectly even spacing.
- **Rush hour**: weekdays 06:00–09:00 and 16:00–19:00 Chicago time. Service day starts at 04:00.

### Processing
Gold is recomputed from Silver on each run and overwritten atomically, because visits span many
Silver batches. Base tables are written first and read back before aggregation, which keeps query plans
small (serverless can't cache). At larger scale, recompute only recent service dates with
`replaceWhere`.

### Limitations
- Arrivals are inferred, never measured. CTA's documentation suggests trains drop out on arrival,
  but the exact trigger (arrival vs departure, cache delay) isn't specified, so there may be a small
  systematic offset that no amount of data from this API can detect.
- Resolution is bounded by the polling interval: ±30 s at 60 s polling.
- Holidays are treated as normal weekdays or weekends.
- Run numbers are CTA's operational IDs and can change mid-trip. Such trains appear as
  `vanished_before_arrival` plus a new visit.
- Small samples: rolled-up rows show `predictions` and `train_visits` counts. Read them before
  trusting a percentage.

## Phase 5 decisions: automation and reliability

**Two schedulers, joined by the landing volume.** Collection must run outside Databricks (Free
Edition restricts outbound internet), so a local scheduler (Task Scheduler or launchd) runs
`scheduled_collection` hourly, and a Lakeflow Job processes whatever has landed every 2 hours.
Neither depends on the other being up. Files simply wait.

**Collector reliability.** Hour-long bounded sessions (not a forever-loop) mean a crash costs at
most one hour, and memory or connection state resets regularly. A lock file prevents overlap,
judged stale by age only (probing a PID with `os.kill` *terminates* processes on Windows). Uploads
run in the idle time between polls through an `after_cycle` hook, so the polling cadence doesn't
drift, and an upload failure can never stop collection.

**Failures that leave no data.** A failed request produces no observation file, so it would be
invisible downstream. Each run therefore writes a *collector run summary*, with every failed
request, its error type and its redacted message, which is uploaded and loaded into
`cta_ops.collector_runs`.

**Jobs as code.** `job_spec.py` builds the Jobs API settings (unit-tested). `deploy_job.py` syncs the
committed notebooks and package to a dedicated workspace folder and creates or resets the job. This is
the same idea as Databricks Asset Bundles, without adding the CLI as a dependency. The job is tagged
with the git commit it runs, and the deploy refuses uncommitted changes.

**Retries are safe because every layer is idempotent.** This is what makes `max_retries`, *Repair
run* and "just re-run it" correct: exclusive-create files → skip-existing upload → Auto Loader
checkpoint + MERGE (Bronze) → stream checkpoint + MERGE (Silver) → atomic full overwrite (Gold).

**Health as data.** `pipeline_health` runs even after failures (`run_if: ALL_DONE`) and appends one
row per job run to `cta_ops.pipeline_runs`: task results (via task values), freshness, 24-hour volume,
API success, reject rate, coverage, Silver backlog and collector failures. Status rules live in a
pure function (`evaluate_health`) with unit tests. Staleness doesn't fail the job by default,
because a laptop collector legitimately stops overnight. Set `fail_on_stale` for an always-on collector.

## Roadmap
1. **Collector** (done)
2. **Bronze** (built): Auto Loader + MERGE of raw envelopes into a Delta table, deduplicated on `collection_id`
3. **Silver** (built): observations, typed predictions, rejects with reasons, quality warnings
4. **Gold** (built): inferred arrivals with confidence, prediction revisions and accuracy, headways, bunching and gaps, rush-hour comparison, coverage
5. **Automation** (built): scheduled collection, Lakeflow Job as code, health and failure tracking, runbook
6. **Dashboard** (built): React + TypeScript + Recharts + MapLibre on static Gold exports. See [dashboard.md](dashboard.md)

## Known limitations
- CTA predictions are estimates. The API never reports actual arrivals, so arrival times in
  later phases are **inferred** (for example, from the last prediction before a train drops
  off the feed) and will be labeled as such.
- Coverage depends on how often and how consistently the collector runs. Gaps in collection
  look like gaps in service unless they are tracked separately, which is why every attempt
  carries metadata.
