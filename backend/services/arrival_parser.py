"""Parse CTA arrival payloads into typed records for display and validation.

This is *not* the system of record: raw JSON is saved untouched by the storage layer and
the authoritative normalization will happen in the Databricks Silver layer. This module
gives the collector a lightweight, tested view of the data for console output and sanity checks.

CTA JSON quirks (verified against the Train Tracker docs):
  * every value is a string, including flags ("0"/"1") and coordinates
  * timestamps look like "2026-10-15T08:01:30" and are America/Chicago local time with no offset
  * ``eta`` may be missing when there are no predictions or the request failed
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

CHICAGO_TZ = ZoneInfo("America/Chicago")

ROUTE_NAMES = {
    "Red": "Red Line",
    "Blue": "Blue Line",
    "Brn": "Brown Line",
    "G": "Green Line",
    "Org": "Orange Line",
    "P": "Purple Line",
    "Pink": "Pink Line",
    "Y": "Yellow Line",
}

REQUIRED_ETA_FIELDS = ("staId", "stpId", "rn", "rt", "prdt", "arrT")


@dataclass(frozen=True)
class ArrivalPrediction:
    station_id: str
    station_name: str
    stop_id: str
    stop_description: str
    run_number: str
    route: str
    destination_name: str
    direction_code: str
    predicted_at_utc: datetime
    arrival_at_utc: datetime
    is_approaching: bool
    is_scheduled: bool
    is_delayed: bool
    is_fault: bool
    latitude: float | None
    longitude: float | None
    heading: int | None

    @property
    def route_name(self) -> str:
        return ROUTE_NAMES.get(self.route, self.route)

    @property
    def minutes_until_arrival(self) -> float:
        """Minutes from prediction generation to arrival (CTA recommends arrT - prdt)."""
        return (self.arrival_at_utc - self.predicted_at_utc).total_seconds() / 60


def parse_cta_timestamp(value: str) -> datetime:
    """Convert a CTA local timestamp into an aware UTC datetime.

    Accepts the JSON format ("2026-10-15T08:01:30") and the XML format ("20261015 08:01:30").
    During the fall-back DST hour the local time is ambiguous; zoneinfo picks the first
    occurrence (fold=0). That edge case is noted for the Silver layer.
    """
    value = value.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y%m%d %H:%M:%S"):
        try:
            naive = datetime.strptime(value, fmt)  # noqa: DTZ007 - CTA sends Chicago time with no offset; zone added below
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Unrecognized CTA timestamp: {value!r}")
    return naive.replace(tzinfo=CHICAGO_TZ).astimezone(UTC)


def _flag(value: Any) -> bool:
    return str(value).strip() == "1"


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def parse_eta(eta: dict[str, Any]) -> ArrivalPrediction:
    missing = [f for f in REQUIRED_ETA_FIELDS if not eta.get(f)]
    if missing:
        raise ValueError(f"eta record missing required fields: {missing}")
    lat = _optional_float(eta.get("lat"))
    lon = _optional_float(eta.get("lon"))
    # Schedule-based predictions report blank (or zero) coordinates; treat 0,0 as unknown.
    if lat == 0 and lon == 0:
        lat = lon = None
    return ArrivalPrediction(
        station_id=str(eta["staId"]),
        station_name=str(eta.get("staNm", "")),
        stop_id=str(eta["stpId"]),
        stop_description=str(eta.get("stpDe", "")),
        run_number=str(eta["rn"]),
        route=str(eta["rt"]),
        destination_name=str(eta.get("destNm", "")),
        direction_code=str(eta.get("trDr", "")),
        predicted_at_utc=parse_cta_timestamp(eta["prdt"]),
        arrival_at_utc=parse_cta_timestamp(eta["arrT"]),
        is_approaching=_flag(eta.get("isApp")),
        is_scheduled=_flag(eta.get("isSch")),
        is_delayed=_flag(eta.get("isDly")),
        is_fault=_flag(eta.get("isFlt")),
        latitude=lat,
        longitude=lon,
        heading=_optional_int(eta.get("heading")),
    )


@dataclass(frozen=True)
class ParseResult:
    predictions: list[ArrivalPrediction]
    rejected: list[tuple[dict[str, Any], str]]  # (raw record, reason)


def parse_arrivals(payload: dict[str, Any]) -> ParseResult:
    """Parse all ``eta`` records, keeping (not raising on) malformed ones for reporting."""
    etas = payload.get("ctatt", {}).get("eta") or []
    if isinstance(etas, dict):  # defensive: some XML->JSON APIs collapse single-item lists
        etas = [etas]
    predictions: list[ArrivalPrediction] = []
    rejected: list[tuple[dict[str, Any], str]] = []
    for eta in etas:
        try:
            predictions.append(parse_eta(eta))
        except (ValueError, TypeError, AttributeError) as exc:
            rejected.append((eta, str(exc)))
            logger.warning("Skipping malformed eta record: %s", exc)
    predictions.sort(key=lambda p: p.arrival_at_utc)
    return ParseResult(predictions=predictions, rejected=rejected)
