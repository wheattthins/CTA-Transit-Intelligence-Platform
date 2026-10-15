import json
from datetime import UTC, datetime

import pytest

from backend.storage.raw_store import ObservationMetadata, build_observation_path, save_observation


def _metadata(**overrides) -> ObservationMetadata:
    ts = datetime(2026, 10, 15, 13, 5, 1, 123456, tzinfo=UTC)
    values = dict(
        collection_id="1a2b3c4d-0000-0000-0000-000000000000",
        source_system="cta_train_tracker",
        source_endpoint="ttarrivals",
        station_id="41320",
        request_params={"mapid": "41320", "outputType": "JSON"},
        requested_at_utc=ts,
        received_at_utc=ts,
        http_status=200,
        elapsed_ms=120,
        attempts=1,
        is_sample=False,
    )
    values.update(overrides)
    return ObservationMetadata(**values)


def test_path_uses_utc_partitions_and_sortable_name(tmp_path):
    path = build_observation_path(tmp_path, _metadata())
    rel = path.relative_to(tmp_path).as_posix()
    assert rel == (
        "train_tracker/arrivals/collection_date=2026-10-15/station_id=41320/"
        "arrivals_41320_20261015T130501123456Z_1a2b3c4d.json"
    )


def test_partition_date_is_utc_even_for_late_chicago_evening(tmp_path):
    # 11:30 PM Chicago on Oct 14 is 04:30 UTC on Oct 15
    ts = datetime(2026, 10, 15, 4, 30, tzinfo=UTC)
    path = build_observation_path(tmp_path, _metadata(received_at_utc=ts))
    assert "collection_date=2026-10-15" in path.as_posix()


def test_save_writes_envelope_with_unmodified_response(tmp_path, sample_payload):
    path = save_observation(tmp_path, _metadata(), sample_payload)

    envelope = json.loads(path.read_text(encoding="utf-8"))
    assert envelope["response"] == sample_payload
    assert envelope["metadata"]["received_at_utc"] == "2026-10-15T13:05:01.123456Z"
    assert envelope["metadata"]["is_sample"] is False
    assert envelope["metadata"]["schema_version"] == 1
    # No temp files left behind
    assert [p.name for p in path.parent.iterdir()] == [path.name]


def test_save_never_overwrites_existing_observation(tmp_path, sample_payload):
    save_observation(tmp_path, _metadata(), sample_payload)
    with pytest.raises(FileExistsError):
        save_observation(tmp_path, _metadata(), {"different": True})


def test_naive_timestamps_are_rejected(tmp_path):
    naive = datetime(2026, 10, 15, 13, 5)
    with pytest.raises(ValueError):
        save_observation(tmp_path, _metadata(requested_at_utc=naive), {})
