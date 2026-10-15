import copy
from datetime import datetime

import pytest
from conftest import make_envelope, sample_response, utc
from cta_lakehouse.silver import transform_bronze_batch

BASE_ETA = sample_response()["ctatt"]["eta"][0]  # Red Line run 822, live, approaching


def _response(*etas, err="0"):
    return {"ctatt": {"tmst": "2026-10-15T08:00:12", "errCd": err, "errNm": None, "eta": list(etas)}}


def _eta(**overrides):
    eta = copy.deepcopy(BASE_ETA)
    eta.update(overrides)
    return eta


def _run(bronze_df, *envelopes):
    batch = transform_bronze_batch(bronze_df(*envelopes))
    return batch.observations.collect(), batch.predictions.collect(), batch.rejects.collect()


# ---------------------------------------------------------------- happy path


def test_sample_shaped_response_produces_typed_predictions(bronze_df):
    obs, preds, rejects = _run(bronze_df, make_envelope(collection_id="c-1"))

    assert rejects == []
    assert len(preds) == 5 and len(obs) == 1
    first = next(p for p in preds if p.eta_index == 0)
    assert first.route_code == "Red" and first.route_name == "Red Line"
    assert first.run_number == "822" and first.stop_id == "30256"
    # 08:00:05 / 08:01:05 Chicago (CDT, UTC-5)
    assert utc(first.predicted_at_utc) == datetime(2026, 10, 15, 13, 0, 5)
    assert utc(first.predicted_arrival_utc) == datetime(2026, 10, 15, 13, 1, 5)
    assert first.predicted_arrival_chicago == datetime(2026, 10, 15, 8, 1, 5)
    assert first.minutes_until_arrival == pytest.approx(1.0)
    assert first.prediction_age_seconds == 10  # observed 13:00:15
    assert first.is_approaching is True and first.is_scheduled is False
    assert first.latitude == pytest.approx(41.94612) and first.heading == 178
    assert first.quality_warnings == []
    assert len(first.prediction_key) == 64


def test_scheduled_prediction_without_coordinates_is_valid(bronze_df):
    _, preds, _ = _run(bronze_df, make_envelope())
    sched = [p for p in preds if p.is_scheduled]
    assert len(sched) == 1
    p = sched[0]
    assert p.latitude is None and p.longitude is None and p.heading is None
    assert p.destination_stop_id is None  # CTA sends "0" for scheduled trips
    assert p.quality_warnings == []


def test_observation_row_counts_reconcile(bronze_df):
    env = make_envelope(_response(_eta(), _eta(rn="999", rt="Nope")), collection_id="c-2")
    obs, _preds, _rejects = _run(bronze_df, env)
    o = obs[0]
    assert o.eta_count == 2
    assert o.valid_prediction_count == 1 and o.rejected_prediction_count == 1
    assert o.valid_prediction_count + o.rejected_prediction_count == o.eta_count
    assert o.is_successful is True
    assert o.observed_at_chicago == datetime(2026, 10, 15, 8, 0, 15)
    assert utc(o.cta_response_at_utc) == datetime(2026, 10, 15, 13, 0, 12)


def test_cta_error_response_is_an_observation_not_a_reject(bronze_df):
    obs, preds, rejects = _run(bronze_df, make_envelope(_response(err="102")))
    assert len(obs) == 1 and obs[0].is_successful is False and obs[0].eta_count == 0
    assert preds == [] and rejects == []


def test_prediction_fingerprint_matches_across_polls(bronze_df):
    """The same CTA prediction seen in two polls shares a fingerprint but not a prediction_key."""
    a = make_envelope(_response(_eta()), received_at="2026-10-15T13:00:15.000000Z")
    b = make_envelope(_response(_eta()), received_at="2026-10-15T13:00:45.000000Z")
    _, preds, _ = _run(bronze_df, a, b)
    assert len({p.prediction_fingerprint for p in preds}) == 1
    assert len({p.prediction_key for p in preds}) == 2


# ---------------------------------------------------------------- rejections


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"rn": ""}, "missing_rn"),
        ({"rt": None}, "missing_rt"),
        ({"arrT": "soon"}, "invalid_arrT"),
        ({"prdt": "2026-13-45T99:00:00"}, "invalid_prdt"),
        ({"rt": "Silver"}, "unknown_route"),
        ({"stpId": "41320"}, "invalid_stop_id"),
        ({"staId": "40380"}, "station_mismatch"),
        ({"staId": "4132"}, "invalid_station_id"),
        ({"arrT": "2026-10-15T07:59:00"}, "arrival_before_prediction"),
        ({"arrT": "2026-10-15T12:00:00"}, "prediction_horizon_too_long"),
        ({"isDly": "yes"}, "invalid_isDly"),
    ],
)
def test_invalid_predictions_are_rejected_with_reason(bronze_df, overrides, reason):
    _, preds, rejects = _run(bronze_df, make_envelope(_response(_eta(**overrides))))
    assert preds == []
    assert len(rejects) == 1
    r = rejects[0]
    assert reason in r.rejection_reasons
    assert r.reject_level == "prediction" and r.eta_index == 0
    assert r.raw_record  # original element preserved for debugging


def test_multiple_reasons_are_all_reported(bronze_df):
    _, _, rejects = _run(bronze_df, make_envelope(_response(_eta(rn="", rt="Silver"))))
    assert set(rejects[0].rejection_reasons) == {"missing_rn", "unknown_route"}


def test_duplicate_within_response_keeps_first(bronze_df):
    _, preds, rejects = _run(bronze_df, make_envelope(_response(_eta(), _eta())))
    assert [p.eta_index for p in preds] == [0]
    assert rejects[0].eta_index == 1 and rejects[0].rejection_reasons == ["duplicate_in_response"]


def test_same_run_at_different_stops_is_not_a_duplicate(bronze_df):
    """Brown Line runs pass Belmont twice (to and from the Loop) - both are real predictions."""
    loop = _eta(rt="Brn", rn="419", stpId="30258", arrT="2026-10-15T08:01:05")
    kimball = _eta(rt="Brn", rn="419", stpId="30257", arrT="2026-10-15T08:49:05")
    _, preds, rejects = _run(bronze_df, make_envelope(_response(loop, kimball)))
    assert len(preds) == 2 and rejects == []


def test_sample_and_corrupt_files_are_rejected_at_response_level(bronze_df):
    obs, preds, rejects = _run(bronze_df, make_envelope(is_sample=True), "{broken")
    assert obs == [] and preds == []
    reasons = sorted(r.rejection_reasons[0] for r in rejects)
    assert reasons == ["sample_data", "unparseable_envelope"]
    assert all(r.reject_level == "response" for r in rejects)


# ---------------------------------------------------------------- quality warnings


def test_out_of_range_coordinates_are_nulled_with_warning(bronze_df):
    _, preds, _ = _run(bronze_df, make_envelope(_response(_eta(lat="0.5", lon="10.0"))))
    p = preds[0]
    assert p.latitude is None and p.longitude is None
    assert "coordinates_out_of_range" in p.quality_warnings


def test_live_train_without_coordinates_gets_warning(bronze_df):
    _, preds, _ = _run(bronze_df, make_envelope(_response(_eta(lat="", lon="", heading=""))))
    assert preds[0].quality_warnings == ["missing_coordinates_for_live_train"]


def test_bad_heading_is_nulled_with_warning(bronze_df):
    _, preds, _ = _run(bronze_df, make_envelope(_response(_eta(heading="400"))))
    assert preds[0].heading is None and preds[0].quality_warnings == ["invalid_heading"]


def test_stale_prediction_warning(bronze_df):
    env = make_envelope(_response(_eta()), received_at="2026-10-15T13:10:15.000000Z")  # 10 min after prdt
    _, preds, _ = _run(bronze_df, env)
    assert "stale_prediction" in preds[0].quality_warnings


# ---------------------------------------------------------------- time zones


def test_winter_timestamps_use_cst(bronze_df):
    eta = _eta(prdt="2026-01-15T08:00:05", arrT="2026-01-15T08:01:05")
    env = make_envelope(_response(eta), received_at="2026-01-15T14:00:15.000000Z")
    _, preds, _ = _run(bronze_df, env)
    assert utc(preds[0].predicted_arrival_utc) == datetime(2026, 1, 15, 14, 1, 5)  # CST = UTC-6
    assert preds[0].predicted_arrival_chicago == datetime(2026, 1, 15, 8, 1, 5)


def test_dst_fall_back_ambiguous_hour_resolves_consistently(bronze_df):
    # 2026-11-01 01:30 happens twice in Chicago; Spark picks the earlier (CDT) instant.
    eta = _eta(prdt="2026-11-01T01:29:00", arrT="2026-11-01T01:30:00")
    env = make_envelope(_response(eta), received_at="2026-11-01T06:29:10.000000Z")
    _, preds, _ = _run(bronze_df, env)
    assert utc(preds[0].predicted_arrival_utc) == datetime(2026, 11, 1, 6, 30, 0)
    assert preds[0].predicted_arrival_chicago == datetime(2026, 11, 1, 1, 30, 0)
