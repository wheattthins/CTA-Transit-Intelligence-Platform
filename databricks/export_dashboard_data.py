"""Export Gold (plus a little Silver/Ops) as static JSON for the React dashboard.

    python databricks/export_dashboard_data.py            # from your Databricks workspace (SQL warehouse)
    python databricks/export_dashboard_data.py --local    # run the pipeline locally on data/raw, then export

Writes ``frontend/public/data/*.json``. The dashboard is a static site: it reads these files and
does all filtering in the browser. The daily Gold tables are additive (counts and sums), so any
filter combination is aggregated exactly.

Both sources run the *same SQL*. Instants are exported as epoch milliseconds (time-zone
independent) and Chicago wall-clock values as ISO strings, so the browser never has to guess
a time zone.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "databricks"))

OUT_DIR = REPO_ROOT / "frontend" / "public" / "data"
EXPORT_SCHEMA_VERSION = 1

# {g}, {s}, {o} = table-name prefixes for Gold, Silver and Ops; {days} = row-level history window.
QUERIES: dict[str, str] = {
    "daily_line_metrics": """
        SELECT date_format(service_date, 'yyyy-MM-dd') AS service_date, station_id, route_code, day_type,
               time_period, is_rush_hour, train_visits, inferred_arrivals, vanished_before_arrival,
               flagged_delayed_visits, scored_predictions, abs_error_sum_seconds, error_sum_seconds,
               within_1_min, within_2_min, reliable_headways, headway_sum_minutes, headway_sumsq_minutes,
               bunched_arrivals, unexpected_gaps
        FROM {g}daily_line_metrics ORDER BY 1, 2, 3""",
    "daily_accuracy_by_horizon": """
        SELECT date_format(service_date, 'yyyy-MM-dd') AS service_date, station_id, route_code, day_type,
               time_period, is_rush_hour, horizon_bucket, scored_predictions, abs_error_sum_seconds, error_sum_seconds, within_1_min, within_2_min
        FROM {g}daily_accuracy_by_horizon ORDER BY 1, 2, 3, 4""",
    "headways": """
        SELECT date_format(service_date, 'yyyy-MM-dd') AS service_date, station_id, stop_id, stop_description,
               route_code, run_number, previous_run_number,
               date_format(arrival_chicago, "yyyy-MM-dd'T'HH:mm:ss") AS arrival_chicago,
               round(headway_minutes, 2) AS headway_minutes, round(typical_headway_minutes, 2) AS typical_headway_minutes,
               is_bunched, is_unexpected_gap, day_type, time_period, is_rush_hour
        FROM {g}headways
        WHERE is_reliable AND service_date >= date_sub((SELECT max(service_date) FROM {g}headways), {days})
        ORDER BY arrival_chicago""",
    "recent_arrivals": """
        SELECT visit_id, station_id, stop_id, stop_description, route_code, run_number, destination_name,
               date_format(inferred_arrival_chicago, "yyyy-MM-dd'T'HH:mm:ss") AS inferred_arrival_chicago,
               round(arrival_uncertainty_seconds, 0) AS uncertainty_seconds, inference_confidence,
               sightings, distinct_predictions, round(total_revision_seconds, 0) AS total_revision_seconds,
               round(final_prediction_offset_seconds, 0) AS final_prediction_offset_seconds, was_flagged_delayed
        FROM {g}train_visits WHERE outcome = 'inferred_arrival'
        ORDER BY inferred_arrival_utc DESC LIMIT 300""",
    "visit_outcomes": """
        SELECT date_format(service_date, 'yyyy-MM-dd') AS service_date, station_id, route_code, outcome,
               inference_confidence, count(*) AS visits
        FROM {g}train_visits GROUP BY ALL ORDER BY 1, 2, 3""",
    "coverage": """
        SELECT station_id, date_format(service_date, 'yyyy-MM-dd') AS service_date,
               date_format(hour_start_chicago, "yyyy-MM-dd'T'HH:mm:ss") AS hour_start_chicago, day_type, time_period,
               polls, successful_polls, failed_polls, minutes_covered, coverage_pct, max_gap_seconds
        FROM {g}collection_coverage
        WHERE service_date >= date_sub((SELECT max(service_date) FROM {g}collection_coverage), {days})
        ORDER BY hour_start_chicago""",
    "latest_snapshot": """
        WITH latest AS (
          SELECT station_id, max_by(collection_id, observed_at_utc) AS collection_id, max(observed_at_utc) AS observed_at_utc
          FROM {s}train_tracker_observations WHERE is_successful GROUP BY station_id
        )
        SELECT p.station_id, p.station_name, p.stop_id, p.stop_description, p.route_code, p.run_number,
               p.destination_name, date_format(p.predicted_arrival_chicago, "yyyy-MM-dd'T'HH:mm:ss") AS predicted_arrival_chicago,
               p.seconds_until_arrival_at_observation AS seconds_until_arrival, p.is_approaching, p.is_delayed,
               p.is_scheduled, p.latitude, p.longitude, p.heading, unix_millis(l.observed_at_utc) AS observed_at_ms
        FROM {s}train_arrival_predictions p JOIN latest l ON p.collection_id = l.collection_id
        ORDER BY p.station_id, p.predicted_arrival_utc""",
    "stations": """
        SELECT station_id, max(station_name) AS station_name, stop_id, max(stop_description) AS stop_description,
               sort_array(collect_set(route_code)) AS routes
        FROM {s}train_arrival_predictions GROUP BY station_id, stop_id ORDER BY station_id, stop_id""",
    "freshness": """
        SELECT unix_millis(max(observed_at_utc)) AS latest_observation_ms,
               unix_millis(min(observed_at_utc)) AS first_observation_ms,
               count(*) AS observations, sum(CAST(is_successful AS INT)) AS successful_observations
        FROM {s}train_tracker_observations""",
    "pipeline_runs": """
        SELECT unix_millis(checked_at_utc) AS checked_at_ms, job_run_id, status, issues, bronze_files_read,
               silver_rows_processed, gold_train_visits, staleness_minutes, polls_24h, api_success_rate_24h,
               reject_rate_24h, coverage_pct_24h, bronze_backlog, collector_failed_requests_24h
        FROM {o}pipeline_runs ORDER BY checked_at_utc DESC LIMIT 100""",
}
OPTIONAL = {"pipeline_runs"}  # the local pipeline has no ops tables


# --------------------------------------------------------------------------- sources


class DatabricksSource:
    """Runs SQL on the workspace's SQL warehouse through the Statement Execution API."""

    def __init__(self, catalog: str, gold: str, silver: str, ops: str) -> None:
        from dotenv import load_dotenv

        load_dotenv(REPO_ROOT / ".env")
        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient()
        warehouses = list(self.w.warehouses.list())
        if not warehouses:
            raise RuntimeError("No SQL warehouse found in the workspace")
        self.warehouse_id = warehouses[0].id
        self.prefixes = {"g": f"{catalog}.{gold}.", "s": f"{catalog}.{silver}.", "o": f"{catalog}.{ops}."}
        self.label = f"databricks:{catalog}"

    def query(self, sql: str) -> list[dict[str, Any]]:
        from databricks.sdk.service.sql import StatementState

        r = self.w.statement_execution.execute_statement(warehouse_id=self.warehouse_id, statement=sql, wait_timeout="50s")
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            r = self.w.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(r.status.error.message if r.status.error else str(r.status.state))
        cols = [(c.name, (c.type_name.value if c.type_name else "STRING")) for c in r.manifest.schema.columns]
        rows = list(r.result.data_array or []) if r.result else []
        chunk = r.result.next_chunk_index if r.result else None
        while chunk is not None:
            part = self.w.statement_execution.get_statement_result_chunk_n(r.statement_id, chunk)
            rows += part.data_array or []
            chunk = part.next_chunk_index
        return [{name: _coerce(v, t) for (name, t), v in zip(cols, row, strict=True)} for row in rows]


def _coerce(value: Any, type_name: str) -> Any:
    """The Statement API returns every value as a string; restore JSON types."""
    if value is None:
        return None
    if type_name in ("INT", "LONG", "SHORT", "BYTE"):
        return int(value)
    if type_name in ("DOUBLE", "FLOAT", "DECIMAL"):
        return float(value)
    if type_name == "BOOLEAN":
        return value in ("true", True)
    if type_name == "ARRAY":
        return json.loads(value)
    return value


class LocalSource:
    """Runs the full pipeline on data/raw in local Spark and queries temp views."""

    def __init__(self, raw_dir: Path) -> None:
        from run_local import build_local_tables, local_spark

        self.spark = local_spark()
        for name, df in build_local_tables(self.spark, raw_dir).items():
            df.createOrReplaceTempView(f"local_{name}")
        self.prefixes = {"g": "local_", "s": "local_", "o": "local_"}
        self.label = f"local:{raw_dir.relative_to(REPO_ROOT) if raw_dir.is_relative_to(REPO_ROOT) else raw_dir}"

    def query(self, sql: str) -> list[dict[str, Any]]:
        return [r.asDict(recursive=True) for r in self.spark.sql(sql).collect()]


# --------------------------------------------------------------------------- export


def render(sql: str, prefixes: dict[str, str], days: int) -> str:
    return sql.format(g=prefixes["g"], s=prefixes["s"], o=prefixes["o"], days=days)


def export(source, out_dir: Path, days: int) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    data: dict[str, Any] = {}
    for name, sql in QUERIES.items():
        try:
            rows = source.query(render(sql, source.prefixes, days))
        except Exception as exc:
            if name in OPTIONAL:
                print(f"  {name}: skipped ({type(exc).__name__})")
                rows = []
            else:
                raise
        data[name] = rows
        counts[name] = len(rows)
        if name != "freshness":
            (out_dir / f"{name}.json").write_text(json.dumps(rows, separators=(",", ":")), encoding="utf-8")
        print(f"  {name}: {len(rows)} rows")

    try:
        commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, capture_output=True,
                                text=True, check=True).stdout.strip()
    except Exception:  # noqa: BLE001 - no git or not a checkout: the manifest just omits the commit
        commit = None
    dates = sorted({r["service_date"] for r in data["daily_line_metrics"]})
    manifest = {
        "schema_version": EXPORT_SCHEMA_VERSION,
        "exported_at_ms": int(datetime.now(UTC).timestamp() * 1000),
        "source": source.label,
        "git_commit": commit,
        "history_days": days,
        "first_service_date": dates[0] if dates else None,
        "last_service_date": dates[-1] if dates else None,
        "freshness": data["freshness"][0] if data["freshness"] else None,
        "latest_health": data["pipeline_runs"][0] if data["pipeline_runs"] else None,
        "files": {k: v for k, v in counts.items() if k != "freshness"},
        "notes": [
            (
                "Arrival times are INFERRED from when trains disappear from CTA's prediction feed; "
                "CTA does not publish actual arrival times."
            ),
            "Accuracy uses live predictions on high/medium-confidence inferred arrivals only.",
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Export Gold tables as static JSON for the dashboard.")
    p.add_argument("--local", action="store_true", help="Run the pipeline locally on --raw-dir instead of Databricks")
    p.add_argument("--raw-dir", type=Path, default=REPO_ROOT / "data" / "raw")
    p.add_argument("--out", type=Path, default=OUT_DIR)
    p.add_argument("--days", type=int, default=30, help="History window for row-level files (headways, coverage)")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--gold-schema", default="cta_gold")
    p.add_argument("--silver-schema", default="cta_silver")
    p.add_argument("--ops-schema", default="cta_ops")
    args = p.parse_args(argv)

    source = LocalSource(args.raw_dir) if args.local else DatabricksSource(
        args.catalog, args.gold_schema, args.silver_schema, args.ops_schema
    )
    print(f"Exporting from {source.label} -> {args.out}")
    manifest = export(source, args.out, args.days)
    print(f"Done: service dates {manifest['first_service_date']} .. {manifest['last_service_date']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
