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
- No `databricks/` or `frontend/` placeholders yet. They will be added when built.
- `pandas` is not installed yet because nothing in Phase 1 needs it.

## Roadmap
1. **Collector** (done)
2. **Bronze**: Auto Loader / `COPY INTO` the raw envelopes into a Delta table, deduplicated on `collection_id`
3. **Silver**: explode `eta`, typed columns, UTC timestamps, Chicago service date, rejected-record table
4. **Gold**: prediction revisions, *inferred* arrivals with confidence and coverage, headways, rush-hour comparisons
5. **Automation**: scheduled collection, Lakeflow Jobs, freshness and failure tracking
6. **Dashboard**: React + TypeScript + Recharts + MapLibre on static Gold exports

## Known limitations
- CTA predictions are estimates. The API never reports actual arrivals, so arrival times in
  later phases are **inferred** (for example, from the last prediction before a train drops
  off the feed) and will be labeled as such.
- Coverage depends on how often and how consistently the collector runs. Gaps in collection
  look like gaps in service unless they are tracked separately, which is why every attempt
  carries metadata.
