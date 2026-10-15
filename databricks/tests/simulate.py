"""Simulate CTA Train Tracker polling with KNOWN ground-truth arrivals.

Real CTA data has no actual arrival times, so the inference can't be validated against it directly.
The simulator generates responses that follow the documented CTA behaviour (trains are listed until
they reach the station, then drop out; responses are cached for a few seconds). Tests then check
that Gold recovers the true arrivals within the expected uncertainty.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

CHICAGO = ZoneInfo("America/Chicago")

STOPS = {
    # stop_id: (route, stop description, direction code, destination)
    "30256": ("Red", "Service toward 95th or Loop", "5", "95th/Dan Ryan"),
    "30255": ("Red", "Service toward Howard or Linden", "1", "Howard"),
    "30257": ("Brn", "Service toward Kimball", "1", "Kimball"),
    "30258": ("Brn", "Service toward Loop", "5", "Loop"),
}


@dataclass
class Train:
    run: str
    stop: str
    arrival: datetime  # ground truth, naive UTC
    route: str | None = None  # defaults from STOPS
    visible_minutes: float = 20  # first appears this long before arrival
    # Prediction error that shrinks linearly as the train approaches:
    # predicted = actual + bias * (time_to_arrival / visible window)
    bias_seconds: float = 0
    # Optional delay revealed mid-approach: before ``delay_revealed_at`` CTA predicted the train
    # ``delay_seconds`` earlier than it really arrives; from then on the prediction is corrected.
    delay_revealed_at: datetime | None = None
    delay_seconds: float = 0
    vanish_at: datetime | None = None  # disappears early and never arrives
    scheduled: bool = False
    delayed: bool = False

    def predicted_arrival(self, snapshot: datetime) -> datetime:
        lead = (self.arrival - snapshot).total_seconds()
        offset = self.bias_seconds * max(lead, 0) / (self.visible_minutes * 60)
        if self.delay_revealed_at and snapshot < self.delay_revealed_at:
            offset -= self.delay_seconds
        return self.arrival + timedelta(seconds=offset)

    def visible(self, snapshot: datetime) -> bool:
        if self.vanish_at and snapshot >= self.vanish_at:
            return False
        return self.arrival - timedelta(minutes=self.visible_minutes) <= snapshot < self.arrival


@dataclass
class Poller:
    start: datetime  # naive UTC
    minutes: int
    interval_seconds: int = 60
    cache_lag_seconds: int = 10  # CTA snapshot (tmst) is this much older than our request
    cache_seconds: int | None = None  # if set, snapshots only change every N seconds (CTA caching)
    skip: set[int] = field(default_factory=set)  # poll indexes with no request (collector down)
    errors: set[int] = field(default_factory=set)  # poll indexes returning CTA errCd 900
    station: str = "41320"

    def poll_times(self) -> list[tuple[int, datetime]]:
        n = self.minutes * 60 // self.interval_seconds + 1
        return [(i, self.start + timedelta(seconds=i * self.interval_seconds)) for i in range(n) if i not in self.skip]


def _local(ts_utc: datetime) -> str:
    return ts_utc.replace(tzinfo=timezone.utc).astimezone(CHICAGO).strftime("%Y-%m-%dT%H:%M:%S")


def _iso_z(ts_utc: datetime) -> str:
    return ts_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def simulate(trains: list[Train], poller: Poller) -> list[str]:
    """Return landing-file texts (collector envelopes), one per poll."""
    files = []
    for i, polled_at in poller.poll_times():
        snapshot = polled_at - timedelta(seconds=poller.cache_lag_seconds)
        if poller.cache_seconds:
            steps = int((snapshot - poller.start).total_seconds() // poller.cache_seconds)
            snapshot = poller.start + timedelta(seconds=steps * poller.cache_seconds)
        if i in poller.errors:
            ctatt = {"tmst": _local(snapshot), "errCd": "900", "errNm": "Server Error", "eta": None}
        else:
            etas = []
            for t in trains:
                if not t.visible(snapshot):
                    continue
                route, desc, direction, dest = STOPS[t.stop]
                prdt = snapshot - timedelta(seconds=5)
                arr = t.predicted_arrival(snapshot)
                etas.append(
                    {
                        "staId": poller.station, "stpId": t.stop, "staNm": "Belmont", "stpDe": desc,
                        "rn": t.run, "rt": t.route or route, "destSt": "0" if t.scheduled else "30173",
                        "destNm": dest, "trDr": direction,
                        "prdt": _local(prdt), "arrT": _local(arr),
                        "isApp": "1" if (arr - prdt).total_seconds() <= 60 else "0",
                        "isSch": "1" if t.scheduled else "0",
                        "isDly": "1" if t.delayed else "0", "isFlt": "0", "flags": None,
                        "lat": "" if t.scheduled else "41.94", "lon": "" if t.scheduled else "-87.65",
                        "heading": "" if t.scheduled else "180",
                    }
                )
            ctatt = {"tmst": _local(snapshot), "errCd": "0", "errNm": None, "eta": etas}
        files.append(
            json.dumps(
                {
                    "metadata": {
                        "schema_version": 1, "collection_id": str(uuid.uuid4()),
                        "source_system": "cta_train_tracker", "source_endpoint": "ttarrivals",
                        "station_id": poller.station,
                        "request_params": {"mapid": poller.station, "outputType": "JSON"},
                        "requested_at_utc": _iso_z(polled_at - timedelta(milliseconds=150)),
                        "received_at_utc": _iso_z(polled_at),
                        "http_status": 200, "elapsed_ms": 150, "attempts": 1,
                        "is_sample": False, "collector_version": "0.1.0",
                    },
                    "response": {"ctatt": ctatt},
                }
            )
        )
    return files
