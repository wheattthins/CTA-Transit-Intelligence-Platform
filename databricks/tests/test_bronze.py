from datetime import datetime

from conftest import make_envelope, sample_response, utc


def test_valid_envelope_extracts_metadata_and_keeps_raw(bronze_df):
    raw = make_envelope(collection_id="c-1")
    row = bronze_df(raw).first()

    assert row.record_key == "c-1" and row.collection_id == "c-1"
    assert row.raw_json == raw  # untouched
    assert row.is_parse_error is False and row.is_sample is False
    assert row.station_id == "41320"
    assert utc(row.received_at_utc) == datetime(2026, 10, 15, 13, 0, 15)
    assert str(row.collection_date) == "2026-10-15"
    assert row.cta_error_code == "0" and row.cta_tmst_local == "2026-10-15T08:00:12"
    assert row.eta_count == len(sample_response()["ctatt"]["eta"])


def test_corrupt_file_is_kept_and_flagged(bronze_df):
    row = bronze_df("{not json").first()
    assert row.is_parse_error is True
    assert row.raw_json == "{not json"
    assert len(row.record_key) == 64  # sha-256 fallback key


def test_cta_error_response_has_zero_etas(bronze_df):
    err = {"ctatt": {"tmst": "2026-10-15T08:00:12", "errCd": "101", "errNm": "Invalid API key", "eta": None}}
    row = bronze_df(make_envelope(err)).first()
    assert row.is_parse_error is False and row.cta_error_code == "101" and row.eta_count == 0


def test_bad_timestamp_does_not_fail_batch(bronze_df):
    row = bronze_df(make_envelope(received_at="garbage")).first()
    assert row.received_at_utc is None and row.is_parse_error is False
