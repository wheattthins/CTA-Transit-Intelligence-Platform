"""Scheduled collection: one bounded session, periodic uploads, and a run summary.

Designed to be started every hour by GitHub Actions (.github/workflows/collector.yml), Windows Task
Scheduler, macOS launchd or cron:

    python -m backend.jobs.scheduled_collection                       # 59-minute session
    python -m backend.jobs.scheduled_collection --duration-minutes 29 --upload-every-minutes 10
    python -m backend.jobs.scheduled_collection --end-at-minute 30    # until the first HH:30 >= 30 min away
    python -m backend.jobs.scheduled_collection --no-upload           # collect only

What one run does:
  1. Takes a lock, so a slow run and the next scheduled run never overlap (a second run exits).
  2. Polls the configured stations on a fixed cadence for the session duration.
  3. Every ``--upload-every-minutes``, uses the idle time between polls to upload new files to the
     Databricks landing volume. Upload problems are logged and never stop collection.
  4. Writes a *collector run summary* (requests, failures with reasons, uploads) next to the raw
     data. It is uploaded too, so ingestion failures that never produced a file are visible in
     Databricks (``cta_ops.collector_runs``).

A 59-minute session started hourly polls at :00 through :59, so consecutive runs leave about
one interval between polls and no coverage gap. Schedulers that start runs late (GitHub Actions)
use ``--end-at-minute`` instead: every run ends at the same minute however late it started, and the
next run, queued behind it, starts there.

Exit codes: 0 all requests succeeded; 1 some failed; 2 configuration error; 3 skipped (already running).
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from backend.collectors.train_tracker import (
    EXIT_CONFIG_ERROR,
    EXIT_OK,
    EXIT_PARTIAL_FAILURE,
    ArrivalsSource,
    CollectionOutcome,
    resolve_iterations,
    run_session,
)
from backend.config.settings import (
    PROJECT_ROOT,
    Settings,
    SettingsError,
    load_settings,
    parse_station_ids,
    validate_poll_interval,
)
from backend.jobs.run_lock import RunLock
from backend.services.cta_client import CTAClient
from backend.storage.raw_store import COLLECTOR_VERSION, write_json_exclusive
from backend.uploaders.volume_uploader import RemoteFileStore, upload_new_files

logger = logging.getLogger("cta.scheduled")

EXIT_SKIPPED = 3
LOCK_PATH = PROJECT_ROOT / "data" / ".collector.lock"
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "collector.log"
MIN_SESSION_MINUTES = 30  # --end-at-minute never ends a session sooner than this


def minutes_until_end(end_minute: int, now: datetime, min_minutes: float = MIN_SESSION_MINUTES) -> float:
    """Minutes from ``now`` to the first HH:``end_minute`` that is at least ``min_minutes`` away."""
    if not 0 <= end_minute <= 59:
        raise SettingsError(f"--end-at-minute must be between 0 and 59 (got {end_minute}).")
    end = now.replace(minute=end_minute, second=0, microsecond=0)
    while end - now < timedelta(minutes=min_minutes):
        end += timedelta(hours=1)
    return (end - now).total_seconds() / 60


# --------------------------------------------------------------------------- uploads


@dataclass
class PeriodicUploader:
    """Uploads new raw files at most every ``every_seconds``. Disabled if Databricks isn't configured."""

    settings: Settings
    every_seconds: float
    store_factory: Any = None  # callable returning a RemoteFileStore; injectable for tests
    monotonic: Any = time.monotonic
    uploaded: int = 0
    failures: int = 0
    attempts: int = 0
    disabled_reason: str | None = None
    _store: RemoteFileStore | None = None
    _last: float | None = None

    def _get_store(self) -> RemoteFileStore | None:
        if self.disabled_reason:
            return None
        if self._store is None:
            try:
                if self.store_factory is not None:
                    self._store = self.store_factory()
                else:
                    from backend.uploaders.volume_uploader import DatabricksVolumeStore

                    self._store = DatabricksVolumeStore()
                s = self.settings
                self._store.ensure_volume(s.databricks_catalog, s.databricks_schema, s.databricks_landing_volume)
            except Exception as exc:  # noqa: BLE001 - any setup failure disables uploads, never collection
                self.disabled_reason = f"{type(exc).__name__}: {exc}"
                logger.warning("Uploads disabled for this run (%s). Files stay local.", self.disabled_reason)
                return None
        return self._store

    def maybe_upload(self, _cycle: int = 0) -> None:
        now = self.monotonic()
        if self._last is not None and now - self._last < self.every_seconds:
            return
        self.upload_now()

    def upload_now(self) -> None:
        self._last = self.monotonic()
        store = self._get_store()
        if store is None:
            return
        self.attempts += 1
        try:
            summary = upload_new_files(store, self.settings.raw_data_dir, self.settings.landing_volume_path)
        except Exception as exc:  # noqa: BLE001 - network down, token expired, ... - try again next time
            self.failures += 1
            logger.error("Upload attempt failed: %s: %s", type(exc).__name__, exc)
            return
        self.uploaded += len(summary.uploaded)
        self.failures += len(summary.failed)
        if summary.uploaded or summary.failed:
            logger.info("Uploaded %d file(s), %d failed", len(summary.uploaded), len(summary.failed))


# --------------------------------------------------------------------------- run summary


@dataclass
class RunSummary:
    run_id: str
    started_at_utc: datetime
    station_ids: tuple[str, ...]
    interval_seconds: int
    planned_cycles: int
    finished_at_utc: datetime | None = None
    outcomes: list[CollectionOutcome] = field(default_factory=list)
    interrupted: bool = False
    exit_code: int | None = None
    uploader: PeriodicUploader | None = None

    def to_dict(self) -> dict[str, Any]:
        def iso(dt: datetime | None) -> str | None:
            return dt.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if dt else None

        failures = [o for o in self.outcomes if not o.success]
        up = self.uploader
        return {
            "record_type": "collector_run",
            "schema_version": 1,
            "run_id": self.run_id,
            "started_at_utc": iso(self.started_at_utc),
            "finished_at_utc": iso(self.finished_at_utc),
            "station_ids": list(self.station_ids),
            "interval_seconds": self.interval_seconds,
            "planned_cycles": self.planned_cycles,
            "requests": len(self.outcomes),
            "succeeded": len(self.outcomes) - len(failures),
            "failed": len(failures),
            "files_written": sum(1 for o in self.outcomes if o.saved_path is not None),
            "files_uploaded": up.uploaded if up else 0,
            "upload_failures": up.failures if up else 0,
            "uploads_disabled_reason": up.disabled_reason if up else "not requested",
            "interrupted": self.interrupted,
            "exit_code": self.exit_code,
            "collector_version": COLLECTOR_VERSION,
            # Messages are already redacted by the CTA client (no API key).
            "failures": [
                {
                    "station_id": o.station_id,
                    "attempted_at_utc": iso(o.attempted_at_utc),
                    "error_type": o.error_type,
                    "error_code": o.error_code,
                    "message": (o.error or "")[:500],
                }
                for o in failures
            ],
        }


def summary_path(root: Path, summary: RunSummary) -> Path:
    ts = summary.started_at_utc.astimezone(UTC)
    return (
        root / "collector_runs" / f"run_date={ts:%Y-%m-%d}"
        / f"collector_run_{ts:%Y%m%dT%H%M%S}Z_{summary.run_id[:8]}.json"
    )


# --------------------------------------------------------------------------- CLI


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m backend.jobs.scheduled_collection",
        description="One scheduled collection session with periodic uploads to Databricks.",
    )
    length = p.add_mutually_exclusive_group()
    length.add_argument("--duration-minutes", type=float, default=59, help="Session length (default 59 for hourly schedules).")
    length.add_argument(
        "--end-at-minute", type=int, metavar="MM",
        help=f"End at the first HH:MM at least {MIN_SESSION_MINUTES} minutes away, for schedulers that start runs late.",
    )
    p.add_argument("-i", "--interval", type=int, help="Seconds between polls (default: CTA_POLL_INTERVAL_SECONDS).")
    p.add_argument("-s", "--station", action="append", metavar="MAPID", help="Override CTA_STATION_IDS.")
    p.add_argument("--upload-every-minutes", type=float, default=15, help="Upload cadence during the session.")
    p.add_argument("--no-upload", action="store_true", help="Collect only; don't upload to Databricks.")
    p.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def configure_logging(level: str, log_path: Path = LOG_PATH) -> None:
    """Log to stderr and to a rotating file (scheduled runs have no visible console)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s", "%Y-%m-%dT%H:%M:%S%z")
    root = logging.getLogger()
    root.setLevel(level)
    for handler in (
        logging.StreamHandler(sys.stderr),
        logging.handlers.RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5, encoding="utf-8"),
    ):
        handler.setFormatter(fmt)
        root.addHandler(handler)


def main(
    argv: Sequence[str] | None = None,
    *,
    settings: Settings | None = None,
    source: ArrivalsSource | None = None,
    store_factory: Any = None,
    lock_path: Path = LOCK_PATH,
    sleep: Any = time.sleep,
    monotonic: Any = time.monotonic,
    log_path: Path | None = LOG_PATH,
) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        settings = settings or load_settings()
    except SettingsError as exc:
        logging.basicConfig(level="INFO")
        logger.error("Configuration error: %s", exc)
        return EXIT_CONFIG_ERROR
    if log_path is not None:
        configure_logging(args.log_level or settings.log_level, log_path)

    try:
        station_ids = parse_station_ids(",".join(args.station)) if args.station else settings.station_ids
        interval = validate_poll_interval(args.interval) if args.interval is not None else settings.poll_interval_seconds
        duration_minutes = (
            minutes_until_end(args.end_at_minute, datetime.now(UTC))
            if args.end_at_minute is not None
            else args.duration_minutes
        )
        iterations = resolve_iterations(argparse.Namespace(duration_minutes=duration_minutes, iterations=None), interval)
        source = source or CTAClient(
            settings.require_api_key(),
            base_url=settings.arrivals_url,
            timeout_seconds=settings.request_timeout_seconds,
            max_attempts=settings.max_attempts,
        )
    except SettingsError as exc:
        logger.error("Configuration error: %s", exc)
        return EXIT_CONFIG_ERROR

    # A lock older than the session plus a margin can only be left over from a crash.
    lock = RunLock(lock_path, stale_after_seconds=duration_minutes * 60 + 1800)
    if not lock.acquire():
        logger.warning("Another collection run holds %s; skipping this run.", lock_path)
        return EXIT_SKIPPED

    with lock:
        summary = RunSummary(
            run_id=str(uuid.uuid4()),
            started_at_utc=datetime.now(UTC),
            station_ids=station_ids,
            interval_seconds=interval,
            planned_cycles=iterations,
        )
        uploader = None
        if not args.no_upload:
            uploader = PeriodicUploader(
                settings, every_seconds=args.upload_every_minutes * 60, store_factory=store_factory, monotonic=monotonic
            )
            summary.uploader = uploader
        logger.info(
            "Scheduled run %s: stations=%s cycles=%d interval=%ds uploads=%s",
            summary.run_id[:8], ",".join(station_ids), iterations, interval,
            "off" if uploader is None else f"every {args.upload_every_minutes:g} min",
        )
        try:
            summary.outcomes = run_session(
                source,
                station_ids,
                iterations=iterations,
                interval_seconds=interval,
                output_dir=settings.raw_data_dir,
                sleep=sleep,
                monotonic=monotonic,
                after_cycle=uploader.maybe_upload if uploader else None,
            )
        except KeyboardInterrupt:
            summary.interrupted = True
            logger.warning("Interrupted; writing summary for the cycles completed so far.")

        failed = sum(1 for o in summary.outcomes if not o.success)
        summary.exit_code = EXIT_PARTIAL_FAILURE if (failed or summary.interrupted or not summary.outcomes) else EXIT_OK
        if uploader:
            uploader.upload_now()  # final data upload, so the summary's counts are complete
        summary.finished_at_utc = datetime.now(UTC)
        path = summary_path(settings.raw_data_dir, summary)
        write_json_exclusive(path, summary.to_dict())
        if uploader:
            uploader.upload_now()  # the summary itself
        logger.info(
            "Run %s finished: %d/%d requests succeeded, %d file(s) uploaded. Summary: %s",
            summary.run_id[:8], len(summary.outcomes) - failed, len(summary.outcomes),
            uploader.uploaded if uploader else 0, path.name,
        )
        return summary.exit_code


if __name__ == "__main__":
    sys.exit(main())
