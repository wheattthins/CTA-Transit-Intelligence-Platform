"""Gold tests against simulated polling with known ground-truth arrivals."""

from datetime import datetime, timedelta

import pytest
from conftest import utc
from cta_lakehouse import tables
from cta_lakehouse.gold import ALL, build_gold, time_context
from cta_lakehouse.silver import transform_bronze_batch, with_processing_time
from pyspark.sql import functions as F
from simulate import Poller, Train, simulate

# Tuesday 2026-10-13, 12:00 UTC = 07:00 Chicago (CDT): weekday AM peak.
T0 = datetime(2026, 10, 13, 12, 0, 0)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


# Belmont always has other trains approaching, so a response is rarely empty. This far-off Brown
# Line train keeps every simulated response non-empty unless a test opts out.
BACKGROUND_TRAIN = Train("999", "30257", at(170), visible_minutes=180)


@pytest.fixture
def run_gold(bronze_df):
    def _run(trains, poller=None, background=True):
        poller = poller or Poller(start=T0, minutes=60)
        all_trains = [*trains, BACKGROUND_TRAIN] if background else list(trains)
        silver = with_processing_time(transform_bronze_batch(bronze_df(*simulate(all_trains, poller))))
        # Checkpoint Silver and each Gold base table, as the notebook does by writing Delta tables.
        preds, obs = silver.predictions.localCheckpoint(), silver.observations.localCheckpoint()
        return build_gold(preds, obs, materialize=lambda _name, df: df.localCheckpoint())

    return _run


def _visits(gold):
    """Visits keyed by (run, stop), excluding the background train."""
    return {(v.run_number, v.stop_id): v for v in gold.train_visits.collect() if v.run_number != "999"}


# ------------------------------------------------------------------ arrival inference


def test_inferred_arrival_recovers_ground_truth_within_uncertainty(run_gold):
    truth = at(30) + timedelta(seconds=17)
    v = _visits(run_gold([Train("822", "30256", truth)]))[("822", "30256")]

    assert v.outcome == "inferred_arrival"
    assert v.inference_confidence == "high" and v.confidence_reasons == []
    assert v.arrival_window_seconds == 60  # one poll interval
    assert v.arrival_uncertainty_seconds == 30
    error = abs((utc(v.inferred_arrival_utc) - truth).total_seconds())
    assert error <= v.arrival_uncertainty_seconds
    assert v.sightings == 20  # visible for 20 minutes at 1-minute polls
    assert v.inferred_arrival_chicago.hour == 7  # 07:30 CDT


def test_empty_response_after_last_train_caps_confidence_at_medium(run_gold):
    """Quiet station: the last train leaves an empty list behind. Still an arrival, but less certain."""
    truth = at(30) + timedelta(seconds=17)
    v = _visits(run_gold([Train("822", "30256", truth)], background=False))[("822", "30256")]
    assert v.outcome == "inferred_arrival" and v.inference_confidence == "medium"
    assert v.confidence_reasons == ["empty_response_after_last_sighting"]
    assert abs((utc(v.inferred_arrival_utc) - truth).total_seconds()) <= 30


def test_unresolved_when_collection_ends_before_arrival(run_gold):
    v = _visits(run_gold([Train("822", "30256", at(70))]))[("822", "30256")]
    assert v.outcome == "unresolved" and v.inferred_arrival_utc is None
    assert v.inference_confidence == "none"
    assert v.outcome_reason == "collection_ended_before_disappearance"


def test_train_vanishing_far_from_station_is_not_an_arrival(run_gold):
    gold = run_gold([Train("822", "30256", at(40), vanish_at=at(25))])
    v = _visits(gold)[("822", "30256")]
    assert v.outcome == "vanished_before_arrival" and v.inferred_arrival_utc is None
    assert v.outcome_reason == "disappeared_while_still_predicted_minutes_away"
    assert gold.headways.where("run_number != '999'").count() == 0


def test_collection_gap_lowers_confidence(run_gold):
    # Collector down for 6 polls around the arrival.
    poller = Poller(start=T0, minutes=60, skip=set(range(29, 35)))
    v = _visits(run_gold([Train("822", "30256", at(30) + timedelta(seconds=20))], poller))[("822", "30256")]
    assert v.outcome == "inferred_arrival"
    assert v.inference_confidence == "low"
    assert "wide_disappearance_window" in v.confidence_reasons
    assert v.arrival_window_seconds > 300


def test_long_collection_gap_leaves_visit_unresolved(run_gold):
    """A midpoint across a 40-minute outage is not an estimate - don't pretend it is."""
    poller = Poller(start=T0, minutes=90, skip=set(range(25, 65)))
    v = _visits(run_gold([Train("822", "30256", at(40))], poller))[("822", "30256")]
    assert v.outcome == "unresolved" and v.inferred_arrival_utc is None
    assert v.outcome_reason == "collection_gap_spans_disappearance"


def test_api_errors_are_not_evidence_of_departure(run_gold):
    truth = at(30) + timedelta(seconds=20)
    # One failed poll right after the arrival: window 120s, still within the high threshold (150s).
    v = _visits(run_gold([Train("822", "30256", truth)], Poller(start=T0, minutes=60, errors={31})))[("822", "30256")]
    assert v.arrival_window_seconds == 120 and v.inference_confidence == "high"
    # Two failed polls: window 180s -> medium, and the estimate is still within its uncertainty.
    v = _visits(run_gold([Train("822", "30256", truth)], Poller(start=T0, minutes=60, errors={31, 32})))[("822", "30256")]
    assert v.arrival_window_seconds == 180
    assert v.inference_confidence == "medium" and "moderate_disappearance_window" in v.confidence_reasons
    assert abs((utc(v.inferred_arrival_utc) - truth).total_seconds()) <= v.arrival_uncertainty_seconds


def test_schedule_based_only_visit_is_low_confidence(run_gold):
    v = _visits(run_gold([Train("508", "30258", at(30), scheduled=True)]))[("508", "30258")]
    assert v.inference_confidence == "low" and "schedule_based_only" in v.confidence_reasons


def test_cached_duplicate_snapshots_do_not_change_inference(run_gold):
    # Poll every 30s while CTA's snapshot only changes every 60s: pairs of polls share a tmst.
    truth = at(30) + timedelta(seconds=5)
    poller = Poller(start=T0, minutes=60, interval_seconds=30, cache_seconds=60)
    v = _visits(run_gold([Train("822", "30256", truth)], poller))[("822", "30256")]
    assert v.inference_confidence == "high"
    assert v.sightings == 20  # distinct snapshots, not polls (40 polls saw the train)
    assert abs((utc(v.inferred_arrival_utc) - truth).total_seconds()) <= v.arrival_uncertainty_seconds


# ------------------------------------------------------------------ visit segmentation


def test_same_run_at_two_platforms_is_two_visits(run_gold):
    """Brown Line runs pass Belmont toward the Loop and back under the same run number."""
    gold = run_gold([Train("419", "30258", at(10)), Train("419", "30257", at(55))])
    assert set(_visits(gold)) == {("419", "30258"), ("419", "30257")}


def test_same_run_same_platform_hours_apart_is_two_visits(run_gold):
    poller = Poller(start=T0, minutes=200)
    gold = run_gold([Train("419", "30258", at(30)), Train("419", "30258", at(170))], poller)
    rows = gold.train_visits.where("run_number = '419'").collect()
    assert len(rows) == 2 and len({r.visit_id for r in rows}) == 2
    assert all(r.outcome == "inferred_arrival" for r in rows)


# ------------------------------------------------------------------ revisions & accuracy


def test_prediction_converging_on_arrival(run_gold):
    # Early predictions are ~2 minutes too late and converge as the train approaches.
    gold = run_gold([Train("901", "30255", at(25), bias_seconds=120)])
    v = _visits(gold)[("901", "30255")]
    assert v.total_revision_seconds == pytest.approx(-120, abs=10)  # prediction moved earlier
    revs = gold.prediction_revisions.where("run_number = '901'").orderBy("revision_number").collect()
    assert revs[0].revision_number == 0 and revs[0].revision_seconds is None
    assert all(r.revision_seconds <= 0 for r in revs[1:])
    # Positive error = the train came sooner than predicted.
    assert revs[0].error_seconds == pytest.approx(120, abs=40)
    assert revs[-1].error_seconds == pytest.approx(0, abs=40)
    assert revs[0].horizon_bucket == "20+ min" and revs[-1].horizon_bucket == "00-02 min"


def test_delay_revealed_mid_approach_is_a_single_large_revision(run_gold):
    truth = at(32)
    gold = run_gold([Train("822", "30256", truth, delay_revealed_at=at(20), delay_seconds=120)])
    v = _visits(gold)[("822", "30256")]
    assert v.outcome == "inferred_arrival" and v.inference_confidence == "high"
    assert v.max_abs_revision_seconds == pytest.approx(120, abs=1)
    assert v.total_revision_seconds == pytest.approx(120, abs=1)
    revs = gold.prediction_revisions.orderBy("revision_number").collect()
    jumps = [r for r in revs if r.revision_seconds and abs(r.revision_seconds) > 60]
    assert len(jumps) == 1 and jumps[0].revision_seconds == pytest.approx(120, abs=1)
    # Before the delay was known, predictions were ~2 minutes early (negative error).
    assert revs[0].error_seconds == pytest.approx(-120, abs=40)


def test_accuracy_uses_only_scoreable_predictions(run_gold):
    gold = run_gold(
        [
            Train("901", "30255", at(25), bias_seconds=120),  # scored
            Train("508", "30258", at(30), scheduled=True),  # schedule-based: excluded
            Train("822", "30256", at(40), vanish_at=at(30)),  # vanished: excluded
        ]
    )
    overall = gold.prediction_accuracy.where(
        " AND ".join(f"{d} = '{ALL}'" for d in ("station_id", "route_code", "horizon_bucket", "rush_period"))
    ).collect()
    assert len(overall) == 1
    row = overall[0]
    assert row.train_visits == 1
    assert row.mean_error_seconds > 0  # predictions were late -> positive bias
    assert 0 <= row.pct_within_1_min <= 100
    by_route = gold.prediction_accuracy.where("station_id = 'ALL' AND route_code = 'Brn'").count()
    assert by_route == 0  # the only Brown visit was schedule-based


# ------------------------------------------------------------------ headways


def _regular_trains(stop: str, first: float, spacing: float, count: int, start_run: int = 800):
    return [Train(str(start_run + i), stop, at(first + i * spacing) + timedelta(seconds=13)) for i in range(count)]


def test_headways_match_simulated_spacing(run_gold):
    gold = run_gold(_regular_trains("30256", 21, 8, 5))
    hw = gold.headways.orderBy("inferred_arrival_utc").collect()
    assert len(hw) == 4
    for h in hw:
        assert h.headway_minutes == pytest.approx(8, abs=1.0)
        assert h.coverage_ok and h.is_reliable


def test_headway_across_collection_gap_is_unreliable(run_gold):
    poller = Poller(start=T0, minutes=60, skip=set(range(33, 40)))
    gold = run_gold(_regular_trains("30256", 21, 8, 4), poller)
    unreliable = [h for h in gold.headways.collect() if not h.coverage_ok]
    assert unreliable and all(not h.is_reliable for h in unreliable)


def test_unexpected_gap_is_flagged(run_gold):
    poller = Poller(start=T0, minutes=120)
    trains = _regular_trains("30256", 21, 6, 7)  # six ~6-minute headways
    trains.append(Train("899", "30256", at(21 + 6 * 6 + 25)))  # then a 25-minute gap
    gold = run_gold(trains, poller)
    hw = gold.headways.orderBy("inferred_arrival_utc").collect()
    assert [h.is_unexpected_gap for h in hw] == [False] * 6 + [True]
    assert hw[-1].typical_headway_minutes == pytest.approx(6, abs=1)
    consistency = gold.service_consistency.where(
        "route_code = 'Red' AND station_id = '41320' AND stop_id = 'ALL'"
    ).first()
    assert consistency.unexpected_gaps == 1 and consistency.headway_cv > 0


def test_bunching_is_flagged(run_gold):
    trains = _regular_trains("30256", 21, 8, 3) + [Train("870", "30256", at(37) + timedelta(seconds=73))]
    gold = run_gold(trains)
    hw = gold.headways.where("stop_id = '30256'").orderBy("inferred_arrival_utc").collect()
    bunched = [h for h in hw if h.is_bunched]
    assert len(bunched) == 1 and bunched[0].headway_minutes < 2
    row = gold.service_consistency.where("route_code = 'Red' AND station_id = '41320' AND stop_id = 'ALL'").first()
    assert row.bunched_arrivals == 1


# ------------------------------------------------------------------ time context & coverage


@pytest.mark.parametrize(
    ("utc_ts", "service_date", "day_type", "period", "rush"),
    [
        (datetime(2026, 10, 13, 12, 30), "2026-10-13", "weekday", "am_peak", True),  # Tue 07:30 CDT
        (datetime(2026, 10, 17, 12, 30), "2026-10-17", "saturday", "am_peak", False),  # Sat 07:30
        (datetime(2026, 10, 13, 22, 0), "2026-10-13", "weekday", "pm_peak", True),  # Tue 17:00
        (datetime(2026, 10, 14, 7, 30), "2026-10-13", "weekday", "late_night", False),  # Wed 02:30 -> Tue service day
        (datetime(2026, 10, 19, 7, 30), "2026-10-18", "sunday", "late_night", False),  # Mon 02:30 -> Sun service day
        (datetime(2026, 1, 13, 13, 30), "2026-01-13", "weekday", "am_peak", True),  # Tue 07:30 CST
    ],
)
def test_time_context(spark, utc_ts, service_date, day_type, period, rush):
    # createDataFrame interprets naive datetimes in local machine time; rebuild the UTC instant in Spark.
    df = spark.createDataFrame([(utc_ts.isoformat(),)], "s STRING").select(F.to_timestamp("s").alias("ts"))
    ctx = time_context(F.col("ts"))
    row = df.select(*[c.alias(k) for k, c in ctx.items()]).first()
    assert str(row.service_date) == service_date
    assert (row.day_type, row.time_period, row.is_rush_hour) == (day_type, period, rush)


def test_collection_coverage_counts_minutes(run_gold):
    # 30 minutes of polling starting 07:00 Chicago -> 31 polls in the 07:00 hour (minute 0..30).
    gold = run_gold([], Poller(start=T0, minutes=30, errors={5}), background=False)
    row = gold.collection_coverage.first()
    assert row.polls == 31 and row.failed_polls == 1 and row.successful_polls == 30
    assert row.minutes_covered == 30 and row.coverage_pct == pytest.approx(50.0)
    assert row.time_period == "am_peak" and row.max_gap_seconds == 60


# ------------------------------------------------------------------ table contracts


@pytest.mark.parametrize("name", list(tables.GOLD_TABLES))
def test_gold_output_matches_table_spec(run_gold, name):
    gold = run_gold(_regular_trains("30256", 21, 8, 4) + [Train("419", "30258", at(30))])
    df = getattr(gold, name)
    assert sorted(df.columns) == sorted(tables.GOLD_TABLES[name].column_names)


@pytest.mark.parametrize("name", list(tables.GOLD_TABLES))
def test_gold_keys_are_unique(run_gold, name):
    gold = run_gold(_regular_trains("30256", 21, 8, 6) + [Train("419", "30258", at(30), bias_seconds=60)])
    df = getattr(gold, name)
    key = list(tables.GOLD_TABLES[name].key)
    assert df.count() == df.select(*key).distinct().count()


# ------------------------------------------------------------------ daily additive tables


def test_daily_metrics_reaggregate_exactly_to_rollups(run_gold):
    """Summing the daily rows must reproduce the rolled-up metrics (the dashboard relies on it)."""
    import math

    gold = run_gold(_regular_trains("30256", 21, 8, 5) + [Train("901", "30255", at(25), bias_seconds=120)])
    daily = gold.daily_line_metrics.collect()
    scored = sum(r.scored_predictions for r in daily)
    mae = sum(r.abs_error_sum_seconds for r in daily) / scored
    acc = gold.prediction_accuracy.where(
        "station_id = '41320' AND route_code = 'ALL' AND horizon_bucket = 'ALL' AND rush_period = 'ALL'"
    ).first()
    assert scored == acc.predictions
    assert mae == pytest.approx(acc.mean_abs_error_seconds, abs=0.06)

    red = [r for r in daily if r.route_code == "Red"]
    n = sum(r.reliable_headways for r in red)
    s1 = sum(r.headway_sum_minutes for r in red)
    s2 = sum(r.headway_sumsq_minutes for r in red)
    hw = [h.headway_minutes for h in gold.headways.where("is_reliable AND route_code = 'Red'").collect()]
    assert n == len(hw) and s1 / n == pytest.approx(sum(hw) / len(hw))
    mean = sum(hw) / len(hw)
    assert math.sqrt((s2 - s1 * s1 / n) / (n - 1)) == pytest.approx(
        math.sqrt(sum((x - mean) ** 2 for x in hw) / (len(hw) - 1))
    )


def test_daily_horizon_sums_match_accuracy_table(run_gold):
    gold = run_gold([Train("901", "30255", at(25), bias_seconds=120)])
    rows = gold.daily_accuracy_by_horizon.collect()
    acc = {r.horizon_bucket: r for r in gold.prediction_accuracy.where(
        "station_id = '41320' AND route_code = 'Red' AND rush_period = 'ALL' AND horizon_bucket != 'ALL'").collect()}
    for bucket, a in acc.items():
        n = sum(r.scored_predictions for r in rows if r.horizon_bucket == bucket)
        assert n == a.predictions
