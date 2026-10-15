"""Persist raw API observations as immutable JSON files.

Each file is an *envelope*:

    {
      "metadata": { ...who/when/how this observation was collected... },
      "response": { ...the CTA JSON body, exactly as received... }
    }

Layout (Hive-style partitions, which Spark/Databricks Auto Loader understand natively):

    <root>/train_tracker/arrivals/collection_date=2026-10-15/station_id=41320/
        arrivals_41320_20261015T130501123456Z_1a2b3c4d.json

Design choices:
  * partition date is the UTC collection date (all stored timestamps are UTC)
  * filename = station + UTC timestamp to microseconds + short UUID, so files sort by time
    and two collectors can never collide
  * files are written to a temp name and atomically renamed; existing files are never replaced
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ENVELOPE_SCHEMA_VERSION = 1
SOURCE_SYSTEM = "cta_train_tracker"
SOURCE_ENDPOINT = "ttarrivals"
COLLECTOR_VERSION = "0.1.0"


@dataclass(frozen=True)
class ObservationMetadata:
    collection_id: str
    source_system: str
    source_endpoint: str
    station_id: str
    request_params: dict[str, str]  # must never contain the API key
    requested_at_utc: datetime
    received_at_utc: datetime
    http_status: int | None
    elapsed_ms: int | None
    attempts: int
    is_sample: bool
    collector_version: str = COLLECTOR_VERSION
    schema_version: int = ENVELOPE_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "collection_id": self.collection_id,
            "source_system": self.source_system,
            "source_endpoint": self.source_endpoint,
            "station_id": self.station_id,
            "request_params": self.request_params,
            "requested_at_utc": _iso_utc(self.requested_at_utc),
            "received_at_utc": _iso_utc(self.received_at_utc),
            "http_status": self.http_status,
            "elapsed_ms": self.elapsed_ms,
            "attempts": self.attempts,
            "is_sample": self.is_sample,
            "collector_version": self.collector_version,
        }


def new_collection_id() -> str:
    return str(uuid.uuid4())


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("Timestamps must be timezone-aware")
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def build_observation_path(root: Path, metadata: ObservationMetadata) -> Path:
    ts = metadata.received_at_utc.astimezone(timezone.utc)
    partition = (
        root
        / "train_tracker"
        / "arrivals"
        / f"collection_date={ts:%Y-%m-%d}"
        / f"station_id={metadata.station_id}"
    )
    short_id = metadata.collection_id.replace("-", "")[:8]
    filename = f"arrivals_{metadata.station_id}_{ts:%Y%m%dT%H%M%S%f}Z_{short_id}.json"
    return partition / filename


def save_observation(root: Path, metadata: ObservationMetadata, response: dict[str, Any]) -> Path:
    """Write the envelope atomically. Raises FileExistsError rather than overwrite."""
    path = build_observation_path(root, metadata)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite existing observation: {path}")

    envelope = {"metadata": metadata.to_dict(), "response": response}
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(envelope, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        # os.link fails if the target exists, giving us a race-free "create only" rename.
        try:
            os.link(tmp_name, path)
        except FileExistsError:
            raise
        except OSError:
            # Some filesystems (e.g. certain network/synced drives) don't support hard links.
            # Fall back to a rename; the existence check above plus the UUID in the filename
            # make a collision practically impossible.
            os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
    return path
