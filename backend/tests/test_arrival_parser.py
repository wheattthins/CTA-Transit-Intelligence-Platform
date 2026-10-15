from datetime import datetime, timezone

import pytest

from backend.services.arrival_parser import parse_arrivals, parse_cta_timestamp
from backend.tests.conftest import load_fixture


def test_parse_cta_timestamp_converts_chicago_local_to_utc():
    # October: Chicago is on CDT (UTC-5)
    assert parse_cta_timestamp("2026-10-15T08:01:05") == datetime(2026, 10, 15, 13, 1, 5, tzinfo=timezone.utc)
    # January: CST (UTC-6)
    assert parse_cta_timestamp("2026-01-15T08:01:05") == datetime(2026, 1, 15, 14, 1, 5, tzinfo=timezone.utc)


def test_parse_cta_timestamp_accepts_xml_format():
    assert parse_cta_timestamp("20261015 08:01:05") == datetime(2026, 10, 15, 13, 1, 5, tzinfo=timezone.utc)


def test_parse_cta_timestamp_rejects_garbage():
    with pytest.raises(ValueError):
        parse_cta_timestamp("not-a-time")


def test_parse_sample_payload(sample_payload):
    result = parse_arrivals(sample_payload)

    assert len(result.predictions) == 5
    assert result.rejected == []
    arrivals = [p.arrival_at_utc for p in result.predictions]
    assert arrivals == sorted(arrivals)

    first = result.predictions[0]
    assert first.route == "Red" and first.route_name == "Red Line"
    assert first.is_approaching and not first.is_scheduled
    assert first.minutes_until_arrival == pytest.approx(1.0)
    assert first.latitude == pytest.approx(41.94612)
    assert first.heading == 178


def test_schedule_based_prediction_has_no_location(sample_payload):
    scheduled = [p for p in parse_arrivals(sample_payload).predictions if p.is_scheduled]
    assert len(scheduled) == 1
    assert scheduled[0].latitude is None and scheduled[0].longitude is None and scheduled[0].heading is None


def test_missing_eta_means_no_predictions():
    result = parse_arrivals(load_fixture("no_predictions.json"))
    assert result.predictions == [] and result.rejected == []


def test_malformed_records_are_rejected_not_fatal():
    result = parse_arrivals(load_fixture("malformed_eta.json"))
    assert len(result.predictions) == 1
    assert len(result.rejected) == 2
    reasons = " ".join(reason for _, reason in result.rejected)
    assert "not-a-time" in reasons
    assert "rn" in reasons
