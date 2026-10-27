# CTA Transit Intelligence Platform

[![CI](https://github.com/wheattthins/CTA-Transit-Intelligence-Platform/actions/workflows/ci.yml/badge.svg)](https://github.com/wheattthins/CTA-Transit-Intelligence-Platform/actions/workflows/ci.yml)
[![Dashboard](https://github.com/wheattthins/CTA-Transit-Intelligence-Platform/actions/workflows/dashboard.yml/badge.svg)](https://github.com/wheattthins/CTA-Transit-Intelligence-Platform/actions/workflows/dashboard.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**How accurate are the CTA's train arrival predictions, and how reliable is 'L' service, really?**

The CTA publishes *predictions* of when trains will arrive, but never reports when they actually
did. This project collects every prediction, **infers real arrival times** from the feed, and runs
the data through an automated Databricks lakehouse to measure prediction accuracy, headways,
bunching and service gaps, all shown on an interactive dashboard.

**[Live dashboard →](https://wheattthins.github.io/CTA-Transit-Intelligence-Platform/)**

![Dashboard](docs/images/dashboard.png)

<!-- TODO(launch): paste the dashboard URL into https://www.linkedin.com/post-inspector/ to check
     the preview card (frontend/index.html og: tags) and refresh LinkedIn's cache before posting. -->

## Key findings

*Early results: 10 service days (Oct 15–26, 2026) at 9 stations (3 since Oct 19), from 5,955
inferred arrivals and 117,105 scored predictions. Since Oct 19, collection has run on weekdays,
mostly 7 AM–5 PM, so evenings, weekends and the Pink and Yellow lines are too thin to report yet.*

- **Predictions get less accurate the further ahead they are.** Under 2 minutes out they're off by
  40 s on average, and 88% land within a minute (4,934 predictions). At 20+ minutes out it's 155 s
  and 29% (40,287 predictions). These figures exclude Howard (see below).
- **Trains tend to arrive later than predicted,** by 45 s on average across all lead times
  (96,716 predictions, excluding Howard).
- **Howard is the outlier, and it's an open question.** Red Line predictions under 2 minutes out
  are off by 244 s there (776 predictions), against 25–26 s at Belmont and Roosevelt. The error is
  on trains *ending* their trip at Howard, which may stay listed in the feed while they lay over.
  That is where inferring arrivals from the feed is weakest, so it may say more about the method
  than about CTA.
- **Bunching is rare in this sample.** The Red Line bunches most: 2.7% of 1,148 headways were
  under 2 minutes. About 6–8% of gaps on the Red, Blue, Brown and Orange lines were at least twice
  the typical headway.

## How it works

```mermaid
flowchart LR
    API["CTA Train Tracker API"] --> C["Python collector<br/>(GitHub Actions, retries)"]
    C --> R["Raw JSON<br/>(immutable, partitioned)"]
    R --> V["Unity Catalog volume"]
    subgraph DB["Databricks Lakehouse: Lakeflow Job every 2 h"]
        V --> B["Bronze<br/>Auto Loader + MERGE"]
        B --> S["Silver<br/>validated, typed, deduplicated"]
        S --> G["Gold<br/>inferred arrivals, accuracy, headways"]
        G --> H["Pipeline health<br/>cta_ops tables"]
    end
    G --> E["Static JSON export"]
    E --> D["React dashboard<br/>(GitHub Pages)"]
    API --> L["Vercel function<br/>(holds key, 30 s CDN cache)"]
    L -->|live map + next trains| D
```

Collection runs **outside** Databricks, because Databricks Free Edition restricts outbound internet
access. It runs on GitHub Actions as back-to-back hour-long runs, so no machine has to stay on.
The collector and the Databricks Job are connected only through the landing volume, so if either
one is down the other keeps working, and files simply wait to be processed.

## Engineering highlights

- **Measuring what the API never reports.** CTA only publishes predictions. Arrivals are inferred
  from *when a train disappears from the feed* (the midpoint of the disappearance window ± half its
  width), using CTA's own snapshot time. Because this never uses predicted times, scoring
  predictions against it isn't circular. Every arrival carries a confidence level and its reasons,
  and accuracy metrics use high- and medium-confidence live predictions only.
- **Validated without ground truth.** A polling simulator generates CTA-shaped data from *known*
  arrival times (including cached snapshots and converging prediction errors), and tests check that
  inferred arrivals land within their stated uncertainty.
- **Idempotent at every layer, so retries are always safe.** Exclusive-create raw files →
  skip-existing upload → Auto Loader checkpoint + `MERGE` (Bronze) → streaming checkpoint + `MERGE`
  (Silver) → atomic overwrite (Gold).
- **Data quality is recorded, not dropped.** Silver records *every* rejection reason and soft
  quality warning per record in a rejects table, and hard checks fail the run if row counts don't
  reconcile from one layer to the next.
- **"No trains" is distinguishable from "no data".** Every poll is recorded, including errors and
  empty responses, so collection gaps are never mistaken for service gaps.
- **Operated like production.** The Lakeflow Job is defined as code and tagged with its git commit.
  Pipeline health (freshness, API success rate, reject rate, coverage, collector failures) is
  written to tables after every run, and there's a [runbook](docs/operations.md).
- **Exact dashboard filtering.** Gold stores counts and sums, not averages, so every filter
  combination is re-aggregated exactly in the browser (never an average of averages).
- **Secrets stay secret.** The API key is redacted from logs, `repr`, exception messages and saved
  files, and tests assert it.
- **Tested.** 177 Python tests, including PySpark transformation tests run on local Spark in CI,
  plus frontend tests for the aggregation math.

## Design decisions

The main trade-offs, what each one costs, and how it would change at larger scale.

| Decision | Instead of | Why, and the cost |
|---|---|---|
| **Collect outside Databricks**, connected only through a landing volume | A Databricks job calling the API | Free Edition restricts outbound internet, and decoupling means a Databricks outage never loses observations. *Cost:* two schedulers to operate. |
| **Keep raw responses unmodified**; Bronze stores each whole file as text | Parsing into typed columns at ingestion | CTA can add or rename fields without breaking ingestion or losing data, and Silver can be rebuilt from Bronze when its logic changes. *Cost:* parsing happens later, in Silver. |
| **Infer arrivals from when a train disappears** from the feed | Treating the last prediction as the arrival | Scoring predictions against predictions would be circular. *Cost:* each arrival is uncertain by about ±30 s at 60 s polling, so every arrival carries a confidence level. |
| **Keep the same prediction seen in consecutive polls**, linked by a fingerprint | Deduplicating it away | Each sighting is a real observation, and the arrival inference depends on them. True duplicates (same file twice, same record twice in a response) are still removed. |
| **Recompute Gold in full** and overwrite atomically | Incremental Gold updates | A train visit spans many Silver batches, so incremental logic would be fragile. *At scale:* recompute only recent service dates with `replaceWhere`. |
| **No table partitioning** | Partitioning by date | At thousands to low millions of small rows, partitions mean many tiny files, and Delta data skipping is enough. *At scale:* add liquid clustering without code changes. |
| **Static JSON export** with exact aggregation in the browser | An API server or live warehouse queries | The data is small and changes only when the pipeline runs, so it hosts free on GitHub Pages with no server. *Cost:* the analytics are as fresh as the last export (re-exported and redeployed every 2 hours by GitHub Actions), and say so. |
| **Live map and board through a caching Vercel function** | Calling CTA from the browser | CTA sends no CORS headers, and a key in a static site is public. The function holds the key and relays CTA's responses unmodified, and the CDN caches them for 30 s, so CTA traffic doesn't grow with visitors. *Cost:* one more thing to deploy; without it the map and board fall back to the export. |
| **Hour-long scheduled collector sessions** | A long-running polling daemon | A crash costs at most an hour, and connection and memory state reset regularly. A lock file prevents overlapping runs. |
| **Jobs as code with the Databricks SDK**, tagged with the git commit | Asset Bundles or configuring the job in the UI | Same idea as bundles without adding the CLI as a dependency, and the deploy refuses uncommitted changes. |

The full reasoning, including the validation rules, confidence criteria and known limitations, is
in [docs/architecture.md](docs/architecture.md).

## Tech stack

| Layer | Tools |
|---|---|
| Collection | Python 3.11+, `requests`, `tenacity`, GitHub Actions (scheduled) |
| Lakehouse | Databricks (Free Edition), Unity Catalog, Delta Lake, Auto Loader, PySpark, Lakeflow Jobs |
| Dashboard | React, TypeScript, Vite, Tailwind CSS, Recharts, MapLibre GL; Vercel Functions for live data |
| Quality and delivery | pytest, Vitest, Ruff, oxlint, GitHub Actions, GitHub Pages |

## Quick start

No API key or Databricks account needed to try it:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

python -m backend.collectors.train_tracker --sample   # offline sample mode, clearly labeled
python -m pytest                                      # Spark tests need Java 17+

cd frontend && npm install && npm run dev             # dashboard at http://localhost:5173
```

For live collection, Databricks and scheduling, see the full [setup and usage guide](docs/setup.md).

## Documentation

| Doc | Contents |
|---|---|
| [setup.md](docs/setup.md) | Install, collector usage and configuration, output format, running every phase |
| [architecture.md](docs/architecture.md) | Design decisions per layer, the arrival-inference method, metrics, limitations |
| [databricks_setup.md](docs/databricks_setup.md) | Databricks workspace setup, verification and recovery |
| [operations.md](docs/operations.md) | Scheduling, monitoring and recovery runbook |
| [dashboard.md](docs/dashboard.md) | Dashboard setup, exact aggregation and design decisions |

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
databricks/
  bronze_ingestion.py           Notebook: Auto Loader + MERGE into the Bronze Delta table
  silver_transformation.py      Notebook: validate, type and deduplicate into Silver tables
  gold_metrics.py               Notebook: inferred arrivals, accuracy, headways, consistency
  pipeline_health.py            Notebook: run status, freshness, coverage -> cta_ops tables
  deploy_job.py                 Sync code + create/update the Lakeflow Job (jobs as code)
  export_dashboard_data.py      Gold -> static JSON for the dashboard (Databricks or --local)
  run_local.py                  Run Bronze -> Silver -> Gold locally on data/raw
  cta_lakehouse/                Tested PySpark transformations + table definitions
  tests/                        Local Spark tests + a CTA polling simulator with known arrivals
frontend/                       React dashboard (src/lib = data + exact aggregation, src/components = UI)
live-api/                       Vercel function relaying live CTA positions + arrivals (holds the key, CDN-cached)
scripts/                        Collector schedulers; map geodata builder
data/                           Raw and sample-run output (git-ignored); labeled SAMPLE fixture
docs/                           Setup, architecture, operations and dashboard docs
```

## Disclaimer

Data is provided by the Chicago Transit Authority. This project is not affiliated with or
endorsed by the CTA. CTA predictions are estimates, not verified arrival times, and the arrival
times in this project's Gold layer are **inferred** from the prediction feed, with stated
confidence and uncertainty. See [docs/architecture.md](docs/architecture.md#phase-4-decisions-gold).
