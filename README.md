# CTA Transit Intelligence Platform

A data engineering portfolio project that collects Chicago Transit Authority (CTA) train
arrival predictions, stores every observation, and (in later phases) processes them through a
Databricks Bronze/Silver/Gold lakehouse to analyze prediction accuracy, headways and service
reliability.

> **Status:** Phases 1–4 done; **Phase 5 (automation and reliability)** built. See
> [docs/operations.md](docs/operations.md) for scheduling, monitoring and recovery.
> See [docs/architecture.md](docs/architecture.md) for the design and roadmap and
> [docs/databricks_setup.md](docs/databricks_setup.md) for the Databricks walkthrough.

## What Phase 1 does

- Calls the [CTA Train Tracker Arrivals API](https://www.transitchicago.com/developers/ttdocs/)
  for one or more stations (default: Belmont, `41320`)
- Retries transient failures (network errors, timeouts, HTTP 5xx/429, CTA `errCd 900`) with
  exponential backoff; stops immediately on unrecoverable errors (invalid key, daily limit)
- Saves each **complete, unmodified** API response as an immutable JSON file with UTC
  collection metadata, partitioned for later ingestion into Databricks
- Prints a readable arrivals table in Chicago local time
- Keeps the API key out of source code, logs, saved files and Git
- Runs offline in a clearly labeled **sample mode** when you don't have a key yet

## Requirements

- Python 3.11 or newer (3.11/3.12 recommended)
- Git
- A free CTA Train Tracker API key (for live data only)

## Setup

### Windows (PowerShell)

```powershell
git clone <your-repo-url>
cd CTA-Transit-Intelligence-Platform

py -3.12 -m venv .venv            # or: py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1      # if blocked: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

python -m pip install --upgrade pip
pip install -r requirements-dev.txt

Copy-Item .env.example .env       # then edit .env in Cursor
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
```

### Dependencies

| Package | Why |
|---|---|
| `requests` | HTTP client for the CTA API |
| `python-dotenv` | Loads settings and the API key from `.env` |
| `tenacity` | Retry with exponential backoff |
| `tzdata` | Time zone database so `America/Chicago` works on Windows |
| `databricks-sdk` | Uploads raw files to a Unity Catalog volume (Phase 2) |
| `pytest` (dev) | Unit tests |
| `pyspark` (dev) | Runs the lakehouse transformations locally in tests (needs Java 17+) |

## Getting a CTA API key

1. Apply at <https://www.transitchicago.com/developers/traintrackerapply/>. The key is free
   and arrives by email (it may take a while).
2. Open `.env` (not `.env.example`) in your editor and set `CTA_API_KEY=<your key>`.
3. `.env` is listed in `.gitignore`. Confirm with `git status` that it is never staged.
   Never paste your key into chat, issues or commits.

## Usage

All commands run from the repository root with the virtual environment activated.

```powershell
# Offline sample mode: no key needed, output is labeled SAMPLE DATA
python -m backend.collectors.train_tracker --sample

# One live collection for the stations in CTA_STATION_IDS
python -m backend.collectors.train_tracker

# One live collection for a specific station (repeat --station for more)
python -m backend.collectors.train_tracker --station 41320

# Bounded session: 10 polling cycles, 60 seconds apart
python -m backend.collectors.train_tracker --iterations 10 --interval 60

# Bounded session by time: about 30 minutes at the configured interval
python -m backend.collectors.train_tracker --duration-minutes 30

# Display only, don't write files
python -m backend.collectors.train_tracker --no-save

python -m backend.collectors.train_tracker --help
```

Exit codes: `0` all requests succeeded, `1` one or more failed, `2` configuration error.

### Configuration (`.env`)

| Variable | Default | Notes |
|---|---|---|
| `CTA_API_KEY` | none | Required for live mode |
| `CTA_STATION_IDS` | `41320` | Comma-separated parent station IDs (`4xxxx`) |
| `CTA_POLL_INTERVAL_SECONDS` | `60` | Minimum 30 |
| `CTA_REQUEST_TIMEOUT_SECONDS` | `10` | |
| `CTA_MAX_ATTEMPTS` | `3` | Total attempts for transient failures |
| `CTA_RAW_DATA_DIR` | `data/raw` | |
| `LOG_LEVEL` | `INFO` | |

Station IDs are listed in the
[CTA 'L' Stops dataset](https://data.cityofchicago.org/Transportation/CTA-System-Information-List-of-L-Stops/8pix-ypme)
(column `MAP_ID`).

### Polling responsibly

CTA's default limit is 100,000 requests per key per day, and heavy traffic from one IP can be
temporarily blocked. The collector makes one request per station per cycle, waits 1 second
between stations, and refuses intervals under 30 seconds. Five stations every 60 seconds is
7,200 requests/day.

## Output

Live observations are written to:

```
data/raw/train_tracker/arrivals/collection_date=YYYY-MM-DD/station_id=41320/
    arrivals_41320_20261015T130501123456Z_1a2b3c4d.json
```

Each file is an envelope:

```json
{
  "metadata": {
    "schema_version": 1,
    "collection_id": "uuid",
    "source_system": "cta_train_tracker",
    "station_id": "41320",
    "request_params": {"mapid": "41320", "outputType": "JSON"},
    "requested_at_utc": "2026-10-15T13:05:01.000000Z",
    "received_at_utc": "2026-10-15T13:05:01.123456Z",
    "http_status": 200, "elapsed_ms": 123, "attempts": 1,
    "is_sample": false, "collector_version": "0.1.0"
  },
  "response": { "ctatt": { "...": "exact CTA response" } }
}
```

Sample-mode runs go to `data/sample_runs/` instead, with `"is_sample": true`. Both directories
are git-ignored.

## How to confirm the collector is working

1. **Tests pass:** `python -m pytest`. All tests use mocked responses, with no network or key.
   Spark tests need Java 17+ and `JAVA_HOME` (see *Running the Spark tests*); without it they're skipped.
2. **Sample mode works:** `python -m backend.collectors.train_tracker --sample` prints a
   Belmont table marked `SAMPLE DATA` and writes one file under `data/sample_runs/`.
3. **Live request works** (after adding your key):
   `python -m backend.collectors.train_tracker --station 41320`
   - The log says `Station 41320: N predictions ... HTTP 200` and `Saved raw observation: ...`.
   - The table has **no** SAMPLE banner, and arrival times are a few minutes from now.
   - Open the saved file: `response.ctatt.errCd` is `"0"`, `tmst` is close to the current
     Chicago time, and `metadata.is_sample` is `false`.
   - Cross-check against <https://www.transitchicago.com/traintracker/> for Belmont.
4. **Key is safe:** search the saved files and logs for your key; it must not appear.
   PowerShell: `Select-String -Path data\raw -Pattern "<your key>" -Recurse` should return nothing.
5. **Error handling:** `CTA_API_KEY` set to a wrong value should log
   `CTA API error 101: Invalid API key`, stop without retrying, and exit with code `1`.

## Running unattended (Phase 5)

```powershell
# Your machine: hourly collection with uploads every 15 minutes
.\scripts\schedule_collection_windows.ps1        # Windows Task Scheduler
scripts/schedule_collection_macos.sh install     # macOS launchd

# Databricks: deploy the Bronze -> Silver -> Gold -> health job (every 2 hours)
python databricks/deploy_job.py --run-now
```

Health, freshness and failures are recorded in `workspace.cta_ops.pipeline_runs` and
`workspace.cta_ops.collector_runs`. The runbook is in [docs/operations.md](docs/operations.md).

## Loading data into Databricks (Phase 2)

```powershell
python -m backend.uploaders.volume_uploader --dry-run
python -m backend.uploaders.volume_uploader
```

Then run `databricks/bronze_ingestion.py`, `databricks/silver_transformation.py` and `databricks/gold_metrics.py` in your workspace.

To run the whole lakehouse pipeline locally on `data/raw` (no Databricks; needs Java):

```powershell
python databricks/run_local.py
``` Full steps, including account and token
setup, are in [docs/databricks_setup.md](docs/databricks_setup.md).

## Running the Spark tests

The lakehouse transformations (`databricks/cta_lakehouse`) are tested with a local Spark session.

- **Windows:** install a JDK 17 (e.g. `winget install Microsoft.OpenJDK.17`), then
  `$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17..."` (match the installed folder).
- **macOS:** `brew install openjdk@17`, then `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`.

```powershell
python -m pytest databricks/tests -v
```

## Project structure

```
backend/
  collectors/train_tracker.py   CLI + collection session orchestration
  services/cta_client.py        HTTP client: retries, CTA error codes, key redaction
  services/arrival_parser.py    Typed view of eta records, Chicago -> UTC conversion
  storage/raw_store.py          Immutable, partitioned raw JSON writer
  uploaders/volume_uploader.py  Idempotent upload of raw files to a Databricks volume
  jobs/scheduled_collection.py  Scheduled session: collect, upload periodically, write run summary
  jobs/run_lock.py              Prevents overlapping scheduled runs
  config/settings.py            Environment-driven configuration
  tests/                        pytest suite and fixtures
data/
  raw/                          Live observations (git-ignored)
  sample_runs/                  Sample-mode output (git-ignored)
  samples/                      SAMPLE fixture (committed, clearly labeled)
databricks/
  bronze_ingestion.py           Notebook: Auto Loader + MERGE into the Bronze Delta table
  silver_transformation.py      Notebook: validate, type and deduplicate into Silver tables
  gold_metrics.py               Notebook: inferred arrivals, accuracy, headways, consistency
  pipeline_health.py            Notebook: run status, freshness, coverage -> cta_ops tables
  deploy_job.py                 Sync code + create/update the Lakeflow Job (jobs as code)
  run_local.py                  Run Bronze -> Silver -> Gold locally on data/raw
  cta_lakehouse/                Tested PySpark transformations + table definitions
  tests/                        Local Spark tests + a CTA polling simulator with known arrivals
scripts/                        Schedulers for the collector (Windows Task Scheduler, macOS launchd)
docs/
  architecture.md               Design decisions and roadmap
  operations.md                 Scheduling, monitoring and recovery runbook
  databricks_setup.md           Phase 2 setup, verification and recovery
```

## Disclaimer

Data is provided by the Chicago Transit Authority. This project is not affiliated with or
endorsed by the CTA. CTA predictions are estimates, not verified arrival times, and the arrival
times in this project's Gold layer are **inferred** from the prediction feed, with stated
confidence and uncertainty. See [docs/architecture.md](docs/architecture.md#phase-4-decisions-gold).
