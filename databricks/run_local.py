"""Run Bronze -> Silver -> Gold locally on the files in data/raw (no Databricks, no Delta).

A development aid: it runs exactly the transformations the notebooks run and prints summaries,
so you can check new data or logic changes before uploading. Requires Java 17+ (JAVA_HOME).

    python databricks/run_local.py
    python databricks/run_local.py --raw-dir data/raw --show 15
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "databricks"))

from cta_lakehouse.bronze import to_bronze_rows  # noqa: E402
from cta_lakehouse.gold import ALL, build_gold  # noqa: E402
from cta_lakehouse.silver import transform_bronze_batch, with_processing_time  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", type=Path, default=REPO_ROOT / "data" / "raw")
    parser.add_argument("--show", type=int, default=20, help="Rows to print per table")
    args = parser.parse_args()

    files = sorted(args.raw_dir.rglob("arrivals_*.json"))
    if not files:
        print(f"No raw files under {args.raw_dir}")
        return 1

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("cta-local-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    now = datetime.now(timezone.utc)
    rows = [(f.read_text(encoding="utf-8"), str(f), f.stat().st_size, now) for f in files]
    landing = spark.createDataFrame(
        rows, "raw_json STRING, source_file_path STRING, source_file_size BIGINT, source_file_modified_at TIMESTAMP"
    )
    bronze = to_bronze_rows(landing, 0).localCheckpoint()
    silver = with_processing_time(transform_bronze_batch(bronze))
    preds, obs, rejects = (silver.predictions.localCheckpoint(), silver.observations.localCheckpoint(),
                           silver.rejects.localCheckpoint())
    gold = build_gold(preds, obs, materialize=lambda _n, df: df.localCheckpoint())

    def section(title: str) -> None:
        print(f"\n=== {title} " + "=" * max(0, 70 - len(title)))

    section(f"Bronze: {bronze.count()} files")
    section(f"Silver: {obs.count()} observations, {preds.count()} predictions, {rejects.count()} rejects")
    if rejects.count():
        rejects.select(F.explode("rejection_reasons").alias("reason")).groupBy("reason").count().show()

    visits = gold.train_visits
    section("Gold: train visits by outcome and confidence")
    visits.groupBy("outcome", "inference_confidence").agg(
        F.count("*").alias("visits"), F.round(F.avg("arrival_uncertainty_seconds"), 1).alias("avg_uncertainty_s")
    ).orderBy("outcome", "inference_confidence").show()

    section("Inferred arrivals (Chicago time)")
    visits.where("outcome = 'inferred_arrival'").select(
        "route_code", "run_number", "stop_id", "inferred_arrival_chicago", "arrival_uncertainty_seconds",
        "inference_confidence", "sightings", "total_revision_seconds", "final_prediction_offset_seconds",
    ).orderBy("inferred_arrival_chicago").show(args.show, truncate=False)

    section("Prediction accuracy by line (ALL horizons)")
    gold.prediction_accuracy.where(
        f"station_id != '{ALL}' AND route_code != '{ALL}' AND horizon_bucket = '{ALL}' AND rush_period = '{ALL}'"
    ).drop("station_id", "horizon_bucket", "rush_period").show(truncate=False)

    section("Prediction accuracy by horizon (all lines)")
    gold.prediction_accuracy.where(
        f"station_id != '{ALL}' AND route_code = '{ALL}' AND horizon_bucket != '{ALL}' AND rush_period = '{ALL}'"
    ).select("horizon_bucket", "predictions", "train_visits", "mean_error_seconds", "mean_abs_error_seconds",
             "pct_within_1_min").orderBy("horizon_bucket").show()

    section("Headways")
    gold.headways.select(
        "route_code", "stop_id", "previous_run_number", "run_number", "arrival_chicago",
        F.round("headway_minutes", 1).alias("headway_min"), "is_reliable", "is_unexpected_gap",
    ).orderBy("route_code", "stop_id", "arrival_chicago").show(args.show, truncate=False)

    section("Service consistency by line and platform")
    gold.service_consistency.where(
        f"station_id != '{ALL}' AND stop_id != '{ALL}' AND day_type = '{ALL}' AND time_period = '{ALL}'"
    ).select("route_code", "stop_id", "train_visits", "headways", "avg_headway_minutes", "headway_cv",
             "unexpected_gaps", "vanished_before_arrival").orderBy("route_code", "stop_id").show()

    section("Collection coverage")
    gold.collection_coverage.orderBy("hour_start_chicago").show(args.show, truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
