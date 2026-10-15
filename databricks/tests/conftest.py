"""Local Spark fixtures. Tests are skipped (not failed) when PySpark or Java is unavailable."""

import copy
import json
import os
import sys
import uuid
from datetime import UTC
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_PATH = REPO_ROOT / "data" / "samples" / "SAMPLE_ttarrivals_belmont_41320.json"


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    # Workers must use the same interpreter as the driver (the venv), not the system Python.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("cta-lakehouse-tests")
            .config("spark.sql.session.timeZone", "UTC")  # same as the notebooks / serverless default
            .config("spark.sql.ansi.enabled", "true")  # serverless default: fail on bad casts
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # most often: no Java runtime / JAVA_HOME not set
        # CI sets REQUIRE_SPARK=1 so a broken Spark setup fails loudly instead of skipping.
        if os.environ.get("REQUIRE_SPARK") == "1":
            raise
        pytest.skip(f"Local Spark unavailable ({type(exc).__name__}); install Java 17+ and set JAVA_HOME")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def sample_response() -> dict[str, Any]:
    return json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))


def make_envelope(
    response: dict[str, Any] | None = None,
    *,
    station_id: str = "41320",
    received_at: str = "2026-10-15T13:00:15.000000Z",
    is_sample: bool = False,
    collection_id: str | None = None,
) -> str:
    """A collector envelope as it appears in a landing file (live data unless is_sample)."""
    return json.dumps(
        {
            "metadata": {
                "schema_version": 1,
                "collection_id": collection_id or str(uuid.uuid4()),
                "source_system": "cta_train_tracker" + ("_SAMPLE" if is_sample else ""),
                "source_endpoint": "ttarrivals",
                "station_id": station_id,
                "request_params": {"mapid": station_id, "outputType": "JSON"},
                "requested_at_utc": received_at.replace("15.000000Z", "14.900000Z"),
                "received_at_utc": received_at,
                "http_status": 200,
                "elapsed_ms": 100,
                "attempts": 1,
                "is_sample": is_sample,
                "collector_version": "0.1.0",
            },
            "response": copy.deepcopy(response) if response is not None else sample_response(),
        }
    )


@pytest.fixture
def landing_df(spark):
    """Build a DataFrame shaped like the Auto Loader input from raw file texts."""
    from datetime import datetime

    def _make(*raw_texts: str):
        rows = [
            (text, f"/Volumes/workspace/cta_bronze/landing/file_{i}.json", len(text), datetime(2026, 10, 15, tzinfo=UTC))
            for i, text in enumerate(raw_texts)
        ]
        return spark.createDataFrame(
            rows, "raw_json STRING, source_file_path STRING, source_file_size BIGINT, source_file_modified_at TIMESTAMP"
        )

    return _make


@pytest.fixture
def bronze_df(landing_df):
    from cta_lakehouse.bronze import to_bronze_rows

    def _make(*raw_texts: str):
        return to_bronze_rows(landing_df(*raw_texts), batch_id=0)

    return _make


def utc(value):
    """PySpark's collect() turns TIMESTAMP values into *naive local-time* datetimes (the machine's
    time zone). Convert back to naive UTC so assertions don't depend on where tests run.
    TIMESTAMP_NTZ values are returned unchanged by Spark and must not go through this."""

    return None if value is None else value.astimezone(UTC).replace(tzinfo=None)
