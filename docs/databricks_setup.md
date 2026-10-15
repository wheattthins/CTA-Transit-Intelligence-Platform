# Databricks setup: Bronze and Silver

This guide takes you from no Databricks account to a populated Bronze Delta table.
Steps marked **(you)** need your browser or account. Everything else is a command.

```
data/raw/*.json ──(volume_uploader)──► /Volumes/workspace/cta_bronze/landing/...
                                               │
                                     (bronze_ingestion notebook:
                                      Auto Loader + MERGE)
                                               ▼
                                workspace.cta_bronze.train_tracker_arrivals
```

## Key concepts

| Concept | What it is | Why we use it |
|---|---|---|
| **Delta Lake** | Parquet files plus a transaction log | ACID writes, `MERGE`, time travel (`VERSION AS OF`), schema enforcement |
| **Unity Catalog** | Governance layer with three-level names: `catalog.schema.table` | One place for tables, files (volumes) and permissions |
| **Volume** | A governed folder for *files* at `/Volumes/<catalog>/<schema>/<volume>` | Landing zone for raw JSON before it becomes a table |
| **Medallion architecture** | Bronze (raw) → Silver (clean) → Gold (business metrics) | Each layer can be rebuilt from the one before it |
| **Auto Loader** | `spark.readStream.format("cloudFiles")` | Tracks processed files in a checkpoint, so each run reads only new files |
| **`trigger(availableNow=True)`** | Process everything new, then stop | Incremental batch processing on serverless compute, which doesn't support always-on streams |
| **MERGE** | Upsert into Delta by key | Second safeguard against duplicates, keyed on `collection_id` |

## 1. Create a Free Edition account (you)

1. Sign up at <https://www.databricks.com/learn/free-edition>.
2. Note your workspace URL from the browser address bar, e.g.
   `https://dbc-1234abcd-5678.cloud.databricks.com`.

Free Edition limits that shape this design: serverless compute only, restricted outbound
internet (which is why collection runs on your machine), one SQL warehouse, 5 concurrent job tasks,
and a daily fair-use quota.

## 2. Create a personal access token (you)

1. In the workspace, click your avatar (top right) → **Settings** → **Developer** →
   **Access tokens** → **Generate new token**.
2. Name it `cta-uploader` and set a lifetime (e.g. 90 days).
3. Copy the token **directly into your `.env`** (next step). Never paste it into chat or commit it.

## 3. Configure `.env` (you)

Add these lines to `.env`. The defaults for catalog, schema and volume are already correct
for Free Edition:

```
DATABRICKS_HOST=https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
DATABRICKS_TOKEN=<your token>
```

## 4. Upload raw files

```powershell
python -m backend.uploaders.volume_uploader --dry-run   # lists what would be uploaded
python -m backend.uploaders.volume_uploader             # creates schema + volume if missing, uploads new files
```

Expected output: `Uploaded N file(s); 0 already present; 0 sample file(s) skipped; 0 failed.`
Run it again and you should see `Uploaded 0 file(s); N already present`. That shows the upload is idempotent.

Check in the UI: **Catalog** → `workspace` → `cta_bronze` → **Volumes** → `landing` → browse to
`train_tracker/arrivals/collection_date=.../station_id=.../`.

*Fallback without a token:* in that volume, click **Upload to this volume** and upload the
JSON files. Recreating the folders isn't required, because the notebook reads the collection details
from inside each file.

## 5. Get the notebook into Databricks (you)

**Option A (recommended): Git folder.** This keeps Databricks in sync with GitHub.
1. Push this repo to GitHub (`git push`).
2. **Workspace** → **Create** → **Git folder** → paste your repo URL → **Create Git folder**.
   A public repo needs no credentials. A private one needs a GitHub token under
   Settings → Linked accounts.
3. Open `databricks/bronze_ingestion`. The `# Databricks notebook source` header makes the `.py`
   file open as a notebook. Use **Pull** on the Git folder to get later changes.

**Option B: Import.** Not recommended now. The notebooks import the `cta_lakehouse` package, so
you would have to import the whole `databricks/` folder with the same layout. Use a Git folder.

## 6. Run the Bronze notebook (you)

1. Open the notebook and attach **Serverless** compute (top-right compute selector).
2. Leave the widgets at their defaults (`workspace`, `cta_bronze`, `landing`, reset `false`).
3. Click **Run all**.

Expected results:
- `Files read this run: N`, where N is the number of uploaded files.
- The stats cell shows `rows == distinct_keys`, `parse_errors: 0`, `sample_rows: 0`.
- The final table shows one row per day and station, with `avg_predictions` around 20–35 for Belmont.

**Prove incremental ingestion and deduplication:**
1. **Run all** again. It should print `Files read this run: 0`, and the row count should be unchanged.
2. Collect and upload more data, then run again. Only the new files are read.
3. Set `reset` = `true` and run. Every file is re-read but the row count is **unchanged**, because the
   MERGE skipped every duplicate. Set `reset` back to `false`.

**Explore with SQL** (in a new cell or the SQL editor):

```sql
-- Latest responses
SELECT station_id, received_at_utc, cta_error_code, eta_count
FROM workspace.cta_bronze.train_tracker_arrivals
ORDER BY received_at_utc DESC LIMIT 20;

-- Delta keeps every version of the table
DESCRIBE HISTORY workspace.cta_bronze.train_tracker_arrivals;

-- Peek inside the raw JSON without a schema (Silver will do this properly)
SELECT received_at_utc, get_json_object(raw_json, '$.response.ctatt.eta[0].destNm') AS first_dest
FROM workspace.cta_bronze.train_tracker_arrivals LIMIT 5;
```

## 7. Run the Silver notebook (you)

1. **Pull** the latest code into the Git folder.
2. Open `databricks/silver_transformation` on Serverless compute and choose **Run all** (widgets: `workspace`,
   `cta_bronze`, `cta_silver`, full refresh `false`).

Expected results:
- Three tables in `workspace.cta_silver`: `train_tracker_observations`, `train_arrival_predictions`,
  `train_tracker_rejects`.
- `All hard data-quality checks passed`. If a check fails, the notebook stops with the reasons.
- On real data, the reject rate should be at or near 0%. The first live sample had 0 rejects in 64 predictions.

**Prove incrementality:** run it again and it prints `Bronze rows processed this run: 0`. Collect, upload
and run Bronze, then Silver, and only the new rows are processed.

**After changing validation logic:** set `full_refresh = true`, run, then set it back to `false`.

```sql
-- Valid predictions in Chicago time
SELECT route_name, run_number, stop_description, predicted_arrival_chicago, minutes_until_arrival, quality_warnings
FROM workspace.cta_silver.train_arrival_predictions ORDER BY observed_at_utc DESC LIMIT 20;

-- What gets rejected and why
SELECT reason, count(*) FROM workspace.cta_silver.train_tracker_rejects
LATERAL VIEW explode(rejection_reasons) AS reason GROUP BY reason;
```

## Recovery

| Problem | Fix |
|---|---|
| Upload interrupted | Re-run the uploader. It skips files already in the volume. |
| Notebook failed mid-run | Re-run it. Auto Loader resumes from the checkpoint, and MERGE ignores rows already written. |
| Need to reprocess everything | Run with `reset = true`. There are no duplicates thanks to the MERGE. |
| Need to rebuild Bronze from scratch | `DROP TABLE workspace.cta_bronze.train_tracker_arrivals;` then run with `reset = true`. The raw files in the volume are the source of truth. |
| `cloudFiles` / checkpoint errors after changing the read options | Run with `reset = true`. |
| Silver failed mid-run | Re-run. MERGE skips rows already written. |
| Silver logic changed | Run Silver with `full_refresh = true`. It is rebuilt from Bronze. |
| `cta_lakehouse not found` | Run the notebooks from the Git folder (Option A), not a standalone import. |
| Daily quota exceeded | Compute stops for the day. Data is safe. Re-run tomorrow. |

## Troubleshooting

- **Uploader: `Databricks authentication failed`**: check `DATABRICKS_HOST` (it must include
  `https://`) and `DATABRICKS_TOKEN` in `.env`, and that the token hasn't expired.
- **`PERMISSION_DENIED` creating a schema**: confirm you are in your own workspace and the
  `workspace` catalog exists (**Catalog** explorer). Set `DATABRICKS_CATALOG` and the notebook's
  `catalog` widget if yours is named differently.
- **`Files read this run: 0` on the first run**: the landing path is wrong or empty. Check that the volume
  contains `train_tracker/arrivals/...`.
