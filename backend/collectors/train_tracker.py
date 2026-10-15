"""Collect CTA Train Tracker arrival predictions and store raw observations.

Examples (run from the repository root):

    python -m backend.collectors.train_tracker                      # one collection, stations from .env
    python -m backend.collectors.train_tracker --station 41320      # one station
    python -m backend.collectors.train_tracker --iterations 10 --interval 60
    python -m backend.collectors.train_tracker --duration-minutes 30
    python -m backend.collectors.train_tracker --sample             # offline, uses the SAMPLE fixture

Sessions are always bounded (by --iterations or --duration-minutes). Continuous collection
will be handled by a scheduler in a later phase, not by an infinite loop here.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from backend.config.settings import (
    MIN_POLL_INTERVAL_SECONDS,
    PROJECT_ROOT,
    Settings,
    SettingsError,
    load_settings,
    parse_station_ids,
    validate_poll_interval,
)
from backend.services.arrival_parser import CHICAGO_TZ, ArrivalPrediction, parse_arrivals
from backend.services.cta_client import (
    ArrivalsResponse,
    CTAApiError,
    CTAClient,
    CTAClientError,
)
from backend.storage.raw_store import (
    SOURCE_ENDPOINT,
    SOURCE_SYSTEM,
    ObservationMetadata,
    new_collection_id,
    save_observation,
)

logger = logging.getLogger("cta.collector")

SAMPLE_FIXTURE = PROJECT_ROOT / "data" / "samples" / "SAMPLE_ttarrivals_belmont_41320.json"

# Errors where continuing the session would only waste requests.
FATAL_CTA_ERROR_CODES = frozenset({"101", "102"})  # invalid key, daily limit exceeded

# Brief pause between stations within one polling cycle, to avoid request bursts.
INTER_STATION_DELAY_SECONDS = 1.0

EXIT_OK, EXIT_PARTIAL_FAILURE, EXIT_CONFIG_ERROR = 0, 1, 2


class ArrivalsSource(Protocol):
    def get_arrivals(self, station_id: str) -> ArrivalsResponse: ...


class SampleArrivalsSource:
    """Serves the bundled SAMPLE fixture instead of calling CTA. Never presented as live data."""

    def __init__(self, fixture_path: Path = SAMPLE_FIXTURE) -> None:
        self._payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        etas = self._payload["ctatt"].get("eta") or []
        self.supported_stations = {e["staId"] for e in etas}

    def get_arrivals(self, station_id: str) -> ArrivalsResponse:
        if station_id not in self.supported_stations:
            raise CTAClientError(
                f"Sample mode only has fixture data for stations {sorted(self.supported_stations)}"
            )
        now = datetime.now(UTC)
        return ArrivalsResponse(
            station_id=station_id,
            payload=json.loads(json.dumps(self._payload)),  # deep copy
            http_status=200,
            requested_at_utc=now,
            received_at_utc=now,
            elapsed_ms=0,
            attempts=1,
            request_params={"mapid": station_id, "outputType": "JSON"},
        )


@dataclass(frozen=True)
class CollectionOutcome:
    station_id: str
    success: bool
    prediction_count: int = 0
    rejected_count: int = 0
    saved_path: Path | None = None
    error: str | None = None
    error_code: str | None = None
    error_type: str | None = None  # exception class, e.g. CTATransientError
    attempted_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))


def _metadata_for(response: ArrivalsResponse, *, is_sample: bool) -> ObservationMetadata:
    return ObservationMetadata(
        collection_id=new_collection_id(),
        source_system=SOURCE_SYSTEM if not is_sample else f"{SOURCE_SYSTEM}_SAMPLE",
        source_endpoint=SOURCE_ENDPOINT,
        station_id=response.station_id,
        request_params=response.request_params,
        requested_at_utc=response.requested_at_utc,
        received_at_utc=response.received_at_utc,
        http_status=response.http_status,
        elapsed_ms=response.elapsed_ms,
        attempts=response.attempts,
        is_sample=is_sample,
    )


def collect_station(
    source: ArrivalsSource,
    station_id: str,
    *,
    output_dir: Path,
    save: bool = True,
    is_sample: bool = False,
    display: Callable[[str, list[ArrivalPrediction], bool], None] | None = None,
) -> CollectionOutcome:
    """Fetch, (optionally) save, and summarize one station. Never raises for API failures."""
    try:
        response = source.get_arrivals(station_id)
    except CTAApiError as exc:
        saved = None
        # A CTA error response is still a real observation worth keeping (e.g. for
        # tracking outages), so persist it when we have the body.
        if save and exc.response is not None:
            saved = save_observation(output_dir, _metadata_for(exc.response, is_sample=is_sample), exc.response.payload)
        logger.error("Station %s: %s", station_id, exc)
        return CollectionOutcome(
            station_id, False, saved_path=saved, error=str(exc), error_code=exc.code, error_type=type(exc).__name__
        )
    except CTAClientError as exc:
        logger.error("Station %s: collection failed: %s", station_id, exc)
        return CollectionOutcome(station_id, False, error=str(exc), error_type=type(exc).__name__)

    saved_path = None
    if save:
        saved_path = save_observation(output_dir, _metadata_for(response, is_sample=is_sample), response.payload)
        logger.info("Saved raw observation: %s", _display_path(saved_path))

    parsed = parse_arrivals(response.payload)
    logger.info(
        "Station %s: %d predictions (%d rejected), HTTP %s in %d ms, %d attempt(s)",
        station_id,
        len(parsed.predictions),
        len(parsed.rejected),
        response.http_status,
        response.elapsed_ms,
        response.attempts,
    )
    if display:
        display(station_id, parsed.predictions, is_sample)
    return CollectionOutcome(
        station_id,
        True,
        prediction_count=len(parsed.predictions),
        rejected_count=len(parsed.rejected),
        saved_path=saved_path,
    )


def run_session(
    source: ArrivalsSource,
    station_ids: Sequence[str],
    *,
    iterations: int,
    interval_seconds: float,
    output_dir: Path,
    save: bool = True,
    is_sample: bool = False,
    display: Callable[[str, list[ArrivalPrediction], bool], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    after_cycle: Callable[[int], None] | None = None,
) -> list[CollectionOutcome]:
    """Run ``iterations`` polling cycles on a fixed cadence of ``interval_seconds``.

    ``after_cycle(cycle_index)`` runs after each cycle, inside the interval, before sleeping. The
    scheduled job uses it to upload files between polls. Its time counts against the interval, so
    the polling cadence doesn't drift.
    """
    outcomes: list[CollectionOutcome] = []
    for i in range(iterations):
        cycle_start = monotonic()
        if iterations > 1:
            logger.info("Polling cycle %d/%d", i + 1, iterations)
        for j, station_id in enumerate(station_ids):
            if j > 0:
                sleep(INTER_STATION_DELAY_SECONDS)
            outcome = collect_station(
                source, station_id, output_dir=output_dir, save=save, is_sample=is_sample, display=display
            )
            outcomes.append(outcome)
            if outcome.error_code in FATAL_CTA_ERROR_CODES:
                logger.critical("Stopping session: CTA error %s cannot be fixed by retrying.", outcome.error_code)
                return outcomes
        if after_cycle is not None:
            try:
                after_cycle(i)
            except Exception:  # a hook failure (e.g. upload) must never stop collection
                logger.exception("after_cycle hook failed; continuing collection")
        if i < iterations - 1:
            remaining = interval_seconds - (monotonic() - cycle_start)
            if remaining > 0:
                sleep(remaining)
    return outcomes


# --------------------------------------------------------------------------- display


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def print_predictions(station_id: str, predictions: list[ArrivalPrediction], is_sample: bool) -> None:
    banner = "  *** SAMPLE DATA - NOT LIVE CTA INFORMATION ***" if is_sample else ""
    name = predictions[0].station_name if predictions else station_id
    print(f"\n{name} ({station_id}){banner}")
    if not predictions:
        print("  No predictions returned (station may be closed or service suspended).")
        return
    header = f"  {'Line':<12}{'Run':<6}{'Toward':<18}{'Arrives (CT)':<14}{'Min':>5}  Status"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for p in predictions:
        status = []
        if p.is_approaching:
            status.append("Due")
        if p.is_delayed:
            status.append("Delayed")
        if p.is_scheduled:
            status.append("Scheduled")
        if p.is_fault:
            status.append("Fault")
        local = p.arrival_at_utc.astimezone(CHICAGO_TZ)
        print(
            f"  {p.route_name:<12}{p.run_number:<6}{p.destination_name[:17]:<18}"
            f"{local:%I:%M:%S %p}   {p.minutes_until_arrival:>5.1f}  {', '.join(status) or 'Live'}"
        )


# --------------------------------------------------------------------------- CLI


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m backend.collectors.train_tracker",
        description="Collect CTA Train Tracker arrival predictions and save raw JSON observations.",
    )
    parser.add_argument(
        "-s", "--station", action="append", metavar="MAPID",
        help="CTA parent station ID (e.g. 41320 for Belmont). Repeat or comma-separate for several. "
             "Defaults to CTA_STATION_IDS.",
    )
    bound = parser.add_mutually_exclusive_group()
    bound.add_argument("-n", "--iterations", type=int, help="Number of polling cycles (default: 1).")
    bound.add_argument("--duration-minutes", type=float, help="Poll for about this many minutes.")
    parser.add_argument(
        "-i", "--interval", type=int, metavar="SECONDS",
        help=f"Seconds between cycles (min {MIN_POLL_INTERVAL_SECONDS}). Defaults to CTA_POLL_INTERVAL_SECONDS.",
    )
    parser.add_argument("--sample", action="store_true", help="Use the bundled SAMPLE fixture; no API call or key needed.")
    parser.add_argument("--no-save", action="store_true", help="Display results without writing raw JSON files.")
    parser.add_argument("--quiet", action="store_true", help="Do not print the predictions table.")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Overrides LOG_LEVEL.")
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stderr,
    )


def resolve_iterations(args: argparse.Namespace, interval: int) -> int:
    if args.duration_minutes is not None:
        if args.duration_minutes <= 0:
            raise SettingsError("--duration-minutes must be positive.")
        return max(1, math.floor(args.duration_minutes * 60 / interval) + 1)
    iterations = args.iterations if args.iterations is not None else 1
    if iterations < 1:
        raise SettingsError("--iterations must be at least 1.")
    return iterations


def main(argv: Sequence[str] | None = None, settings: Settings | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        settings = settings or load_settings()
    except SettingsError as exc:
        configure_logging("INFO")
        logger.error("Configuration error: %s", exc)
        return EXIT_CONFIG_ERROR
    configure_logging(args.log_level or settings.log_level)

    try:
        station_ids = parse_station_ids(",".join(args.station)) if args.station else settings.station_ids
        interval = validate_poll_interval(args.interval) if args.interval is not None else settings.poll_interval_seconds
        iterations = resolve_iterations(args, interval)
        if args.sample:
            source: ArrivalsSource = SampleArrivalsSource()
            output_dir = settings.sample_runs_dir
            logger.warning("SAMPLE MODE: using fixture %s - this is NOT live CTA data.", _display_path(SAMPLE_FIXTURE))
        else:
            source = CTAClient(
                settings.require_api_key(),
                base_url=settings.arrivals_url,
                timeout_seconds=settings.request_timeout_seconds,
                max_attempts=settings.max_attempts,
            )
            output_dir = settings.raw_data_dir
    except SettingsError as exc:
        logger.error("Configuration error: %s", exc)
        return EXIT_CONFIG_ERROR

    logger.info(
        "Starting collection: stations=%s iterations=%d interval=%ds save=%s",
        ",".join(station_ids), iterations, interval, not args.no_save,
    )
    try:
        outcomes = run_session(
            source,
            station_ids,
            iterations=iterations,
            interval_seconds=interval,
            output_dir=output_dir,
            save=not args.no_save,
            is_sample=args.sample,
            display=None if args.quiet else print_predictions,
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user; files already written are complete.")
        return EXIT_PARTIAL_FAILURE

    succeeded = sum(o.success for o in outcomes)
    logger.info("Finished: %d/%d requests succeeded.", succeeded, len(outcomes))
    return EXIT_OK if outcomes and succeeded == len(outcomes) else EXIT_PARTIAL_FAILURE


if __name__ == "__main__":
    sys.exit(main())
