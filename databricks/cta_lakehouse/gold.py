"""Gold layer: inferred train arrivals, prediction accuracy, headways and service consistency.

IMPORTANT: CTA Train Tracker never reports actual arrival times. Every "arrival" here is
**inferred** and labeled as such, with a confidence level and an uncertainty in seconds.

Inference method ("disappearance window")
-----------------------------------------
CTA documents that trains which reach a station drop out of the arrivals feed. So for each
*train visit* (one run approaching one platform):

    last_seen_at     = CTA snapshot time (tmst) of the last response containing the train
    first_missing_at = CTA snapshot time of the next successful response *without* it
    inferred_arrival = midpoint of [last_seen_at, first_missing_at],  uncertainty = +/- half the window

The estimate never uses CTA's predicted times, so comparing predictions with it is not circular.
Confidence is downgraded when the window is wide (collection gaps, API errors), when the train
was still far from the station at its last sighting (it may have been cancelled or re-routed), or
when the only predictions were schedule-based. A train that vanishes while still predicted several
minutes away is classified ``vanished_before_arrival``, not as an arrival.

Time handling: all instants are UTC. Rider-facing context (service date, day type, time period,
rush hour) is derived in America/Chicago, with the service day starting at 04:00 local.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

CHICAGO_TZ = "America/Chicago"
ALL = "ALL"  # dimension value for rolled-up rows


@dataclass(frozen=True)
class GoldConfig:
    # Visit segmentation: a new visit starts if the predicted arrival jumps by more than this,
    # or the train wasn't seen for longer than ``visit_max_silence_minutes``.
    visit_split_minutes: int = 30
    visit_max_silence_minutes: int = 60
    # Inference confidence thresholds.
    high_max_window_seconds: int = 150
    medium_max_window_seconds: int = 300
    high_max_final_lead_seconds: int = 180
    medium_max_final_lead_seconds: int = 600
    # Disappeared while still predicted this far after first_missing_at -> vanished, not arrived.
    vanish_threshold_seconds: int = 300
    # A disappearance window wider than this (e.g. the collector was off) can't locate the arrival.
    max_inference_window_seconds: int = 900
    # Evidence gaps longer than this make headways across them unreliable.
    coverage_gap_seconds: int = 180
    # Unexpected gap: headway >= ratio x typical headway AND >= minimum minutes.
    unexpected_gap_ratio: float = 2.0
    unexpected_gap_min_minutes: float = 10.0
    min_headways_for_baseline: int = 5
    # Bunching: a reliable headway shorter than this (trains arriving nearly together).
    bunching_max_minutes: float = 2.0
    service_day_start_hour: int = 4


DEFAULT_CONFIG = GoldConfig()

HORIZON_BUCKETS = ((2, "00-02 min"), (5, "02-05 min"), (10, "05-10 min"), (15, "10-15 min"), (20, "15-20 min"))
HORIZON_OVERFLOW = "20+ min"


# --------------------------------------------------------------------------- helpers


def _micros(col: Column | str) -> Column:
    return F.call_function("unix_micros", F.col(col) if isinstance(col, str) else col)


def _seconds_between(later: Column | str, earlier: Column | str) -> Column:
    return (_micros(later) - _micros(earlier)) / 1_000_000


def _midpoint(a: str, b: str) -> Column:
    return F.call_function("timestamp_micros", ((_micros(a) + _micros(b)) / 2).cast("long"))


def to_chicago(ts_utc: Column) -> Column:
    return F.from_utc_timestamp(ts_utc, CHICAGO_TZ).cast("timestamp_ntz")


def time_context(ts_utc: Column, cfg: GoldConfig = DEFAULT_CONFIG) -> dict[str, Column]:
    """Service date, day type, time period and rush-hour flag for a UTC instant, in Chicago time.

    Periods (local clock): early_morning 04-06, am_peak 06-09, midday 09-16, pm_peak 16-19,
    evening 19-22, late_night 22-04. ``is_rush_hour`` = weekday AND am_peak/pm_peak.
    Holidays are not modeled (documented limitation).
    """
    local = to_chicago(ts_utc)
    service_date = F.to_date(local - F.expr(f"INTERVAL {cfg.service_day_start_hour} HOURS"))
    dow = F.dayofweek(service_date)  # 1 = Sunday, 7 = Saturday
    day_type = F.when(dow == 1, "sunday").when(dow == 7, "saturday").otherwise("weekday")
    hour = F.hour(local)
    period = (
        F.when((hour >= 4) & (hour < 6), "early_morning")
        .when((hour >= 6) & (hour < 9), "am_peak")
        .when((hour >= 9) & (hour < 16), "midday")
        .when((hour >= 16) & (hour < 19), "pm_peak")
        .when((hour >= 19) & (hour < 22), "evening")
        .otherwise("late_night")
    )
    is_rush = (day_type == "weekday") & period.isin("am_peak", "pm_peak")
    return {
        "service_date": service_date,
        "day_type": day_type,
        "time_period": period,
        "is_rush_hour": is_rush,
        "rush_period": F.when(is_rush, "rush_hour").otherwise("non_rush_hour"),
    }


def horizon_bucket(minutes: Column) -> Column:
    expr = None
    for upper, label in HORIZON_BUCKETS:
        expr = F.when(minutes < upper, label) if expr is None else expr.when(minutes < upper, label)
    return expr.otherwise(HORIZON_OVERFLOW)


def rollup(
    df: DataFrame, dims: Sequence[str], grains: Iterable[Sequence[str]], aggs: Sequence[Column]
) -> DataFrame:
    """Aggregate at several grains; dimensions not in a grain are reported as 'ALL'.

    Percentiles can't be re-aggregated from finer rows, so each grain is computed from detail.
    """
    frames = []
    for grain in grains:
        grouped = df.groupBy(*grain).agg(*aggs)
        for d in dims:
            if d not in grain:
                grouped = grouped.withColumn(d, F.lit(ALL))
        frames.append(grouped.select(*dims, *[c for c in grouped.columns if c not in dims]))
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


# --------------------------------------------------------------------------- snapshots & coverage


def snapshots(observations: DataFrame) -> DataFrame:
    """Observations with the CTA snapshot time (tmst) used as the as-of time of each response.

    CTA caches responses, so two polls can return the same snapshot; tmst identifies it.
    """
    return observations.withColumn(
        "snapshot_at", F.coalesce("cta_response_at_utc", "observed_at_utc")
    )


def evidence_timeline(observations: DataFrame) -> DataFrame:
    """Distinct successful snapshots per station, with the next snapshot and its prediction count.

    Failed requests (CTA errors) prove nothing about any train and are excluded. Successful but
    *empty* responses are kept as evidence, because a quiet station's last train leaves an empty
    list behind. Visits that rely on one are capped at medium confidence, since an empty list could
    also be a CTA glitch.
    """
    evidence = (
        snapshots(observations)
        .where(F.col("is_successful"))
        .groupBy("station_id", "snapshot_at")
        .agg(F.max("valid_prediction_count").alias("prediction_count"))
    )
    w = Window.partitionBy("station_id").orderBy("snapshot_at")
    return evidence.withColumns(
        {
            "next_snapshot_at": F.lead("snapshot_at").over(w),
            "next_prediction_count": F.lead("prediction_count").over(w),
        }
    )


def evidence_gaps(observations: DataFrame, cfg: GoldConfig = DEFAULT_CONFIG) -> DataFrame:
    tl = evidence_timeline(observations)
    return (
        tl.where(_seconds_between("next_snapshot_at", "snapshot_at") > cfg.coverage_gap_seconds)
        .select("station_id", F.col("snapshot_at").alias("gap_start"), F.col("next_snapshot_at").alias("gap_end"))
    )


def collection_coverage(observations: DataFrame, cfg: GoldConfig = DEFAULT_CONFIG) -> DataFrame:
    """Per station and Chicago clock hour: how completely we observed it."""
    snaps = snapshots(observations)
    ctx = time_context(F.col("observed_at_utc"), cfg)
    w = Window.partitionBy("station_id").orderBy("observed_at_utc")
    ok = F.col("is_successful")
    enriched = snaps.select(
        "station_id",
        "observed_at_utc",
        "is_successful",
        ctx["service_date"].alias("service_date"),
        ctx["day_type"].alias("day_type"),
        ctx["time_period"].alias("time_period"),
        F.date_trunc("hour", to_chicago(F.col("observed_at_utc"))).cast("timestamp_ntz").alias("hour_start_chicago"),
        F.date_trunc("minute", F.col("observed_at_utc")).alias("_minute"),
    ).withColumn(
        "_gap_to_next_s",
        _seconds_between(F.lead("observed_at_utc").over(w), F.col("observed_at_utc")),
    )
    return (
        enriched.groupBy("station_id", "service_date", "hour_start_chicago", "day_type", "time_period")
        .agg(
            F.count("*").alias("polls"),
            F.sum(ok.cast("int")).cast("int").alias("successful_polls"),
            F.sum((~ok).cast("int")).cast("int").alias("failed_polls"),
            F.countDistinct(F.when(ok, F.col("_minute"))).cast("int").alias("minutes_covered"),
            F.round(F.max("_gap_to_next_s"), 1).cast("double").alias("max_gap_seconds"),
        )
        .withColumn("coverage_pct", F.round(F.col("minutes_covered") / 60 * 100, 1))
    )


# --------------------------------------------------------------------------- visits


VISIT_KEY = ("station_id", "stop_id", "route_code", "run_number")


def assign_visits(predictions: DataFrame, observations: DataFrame, cfg: GoldConfig = DEFAULT_CONFIG) -> DataFrame:
    """Attach ``visit_seq`` and snapshot time to each prediction sighting."""
    snap = snapshots(observations).select("collection_id", "snapshot_at")
    p = predictions.join(snap, "collection_id")
    w = Window.partitionBy(*VISIT_KEY).orderBy("snapshot_at", "observed_at_utc", "prediction_key")
    prev_arr = F.lag("predicted_arrival_utc").over(w)
    prev_snap = F.lag("snapshot_at").over(w)
    new_visit = (
        prev_arr.isNull()
        | (F.abs(_seconds_between(F.col("predicted_arrival_utc"), prev_arr)) > cfg.visit_split_minutes * 60)
        | (_seconds_between(F.col("snapshot_at"), prev_snap) > cfg.visit_max_silence_minutes * 60)
    )
    p = p.withColumn("_new_visit", new_visit.cast("int"))
    p = p.withColumn("visit_seq", F.sum("_new_visit").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    vw = Window.partitionBy(*VISIT_KEY, "visit_seq")
    first_seen = F.min("snapshot_at").over(vw)
    return p.withColumn(
        "visit_id",
        F.sha2(F.concat_ws("|", *VISIT_KEY, F.date_format(first_seen, "yyyy-MM-dd'T'HH:mm:ss")), 256),
    ).drop("_new_visit")


def build_visits(sightings: DataFrame, observations: DataFrame, cfg: GoldConfig = DEFAULT_CONFIG) -> DataFrame:
    """One row per train visit with its inferred arrival, outcome and confidence."""
    # Revision between consecutive sightings, for max_abs_revision_seconds.
    w = Window.partitionBy("visit_id").orderBy("snapshot_at", "observed_at_utc")
    s = sightings.withColumn(
        "_revision_s", _seconds_between(F.col("predicted_arrival_utc"), F.lag("predicted_arrival_utc").over(w))
    )
    v = s.groupBy("visit_id", *VISIT_KEY).agg(
        F.max_by("station_name", "snapshot_at").alias("station_name"),
        F.max_by("stop_description", "snapshot_at").alias("stop_description"),
        F.max_by("route_name", "snapshot_at").alias("route_name"),
        F.max_by("direction_code", "snapshot_at").alias("direction_code"),
        F.max_by("destination_name", "snapshot_at").alias("destination_name"),
        F.min("snapshot_at").alias("first_seen_at_utc"),
        F.max("snapshot_at").alias("last_seen_at_utc"),
        F.countDistinct("snapshot_at").cast("int").alias("sightings"),
        F.countDistinct("prediction_fingerprint").cast("int").alias("distinct_predictions"),
        F.min_by("predicted_arrival_utc", "snapshot_at").alias("first_predicted_arrival_utc"),
        F.max_by("predicted_arrival_utc", "snapshot_at").alias("final_predicted_arrival_utc"),
        F.max_by("is_approaching", "snapshot_at").alias("final_is_approaching"),
        F.min(F.col("is_scheduled").cast("int")).cast("boolean").alias("schedule_based_only"),
        F.max(F.col("is_delayed").cast("int")).cast("boolean").alias("was_flagged_delayed"),
        F.max(F.abs("_revision_s")).cast("double").alias("max_abs_revision_seconds"),
    )

    tl = evidence_timeline(observations).select(
        "station_id",
        F.col("snapshot_at").alias("last_seen_at_utc"),
        F.col("next_snapshot_at").alias("first_missing_at_utc"),
        F.col("next_prediction_count").alias("_next_count"),
    )
    v = v.join(tl, ["station_id", "last_seen_at_utc"], "left")

    window_s = _seconds_between("first_missing_at_utc", "last_seen_at_utc")
    final_lead_s = _seconds_between("final_predicted_arrival_utc", "last_seen_at_utc")
    vanished = _seconds_between("final_predicted_arrival_utc", "first_missing_at_utc") > cfg.vanish_threshold_seconds
    too_wide = window_s > cfg.max_inference_window_seconds
    outcome_reason = (
        F.when(F.col("first_missing_at_utc").isNull(), "collection_ended_before_disappearance")
        .when(too_wide, "collection_gap_spans_disappearance")
        .when(vanished, "disappeared_while_still_predicted_minutes_away")
    )
    outcome = (
        F.when(F.col("first_missing_at_utc").isNull() | too_wide, "unresolved")
        .when(vanished, "vanished_before_arrival")
        .otherwise("inferred_arrival")
    )
    v = v.withColumns(
        {
            "outcome": outcome,
            "outcome_reason": outcome_reason,
            "arrival_window_seconds": window_s.cast("double"),
            "final_lead_seconds": final_lead_s.cast("double"),
            "total_revision_seconds": _seconds_between("final_predicted_arrival_utc", "first_predicted_arrival_utc").cast("double"),
        }
    )

    is_arrival = F.col("outcome") == "inferred_arrival"
    low_reasons = [
        (F.col("arrival_window_seconds") > cfg.medium_max_window_seconds, "wide_disappearance_window"),
        (F.col("final_lead_seconds") > cfg.medium_max_final_lead_seconds, "far_from_station_when_last_seen"),
        (F.col("schedule_based_only"), "schedule_based_only"),
    ]
    medium_reasons = [
        (F.col("arrival_window_seconds") > cfg.high_max_window_seconds, "moderate_disappearance_window"),
        (F.col("final_lead_seconds") > cfg.high_max_final_lead_seconds, "not_close_when_last_seen"),
        (F.col("sightings") < 2, "single_sighting"),
        (F.col("_next_count") == 0, "empty_response_after_last_sighting"),
    ]

    def reasons(checks):
        return F.array_compact(F.array(*[F.when(c, F.lit(r)) for c, r in checks]))

    v = v.withColumns({"_low": reasons(low_reasons), "_medium": reasons(medium_reasons)})
    confidence = (
        F.when(~is_arrival, "none")
        .when(F.size("_low") > 0, "low")
        .when(F.size("_medium") > 0, "medium")
        .otherwise("high")
    )
    inferred = F.when(is_arrival, _midpoint("last_seen_at_utc", "first_missing_at_utc"))
    v = v.withColumns(
        {
            "inferred_arrival_utc": inferred,
            "inference_confidence": confidence,
            "confidence_reasons": F.when(is_arrival, F.concat("_low", "_medium")).otherwise(F.array().cast("array<string>")),
            "arrival_uncertainty_seconds": F.when(is_arrival, F.col("arrival_window_seconds") / 2),
        }
    )
    # Context from the inferred arrival, falling back to the final prediction for non-arrivals.
    ctx_ts = F.coalesce("inferred_arrival_utc", "final_predicted_arrival_utc")
    ctx = time_context(ctx_ts, cfg)
    return v.withColumns(
        {
            "inferred_arrival_chicago": to_chicago(F.col("inferred_arrival_utc")),
            "final_prediction_offset_seconds": F.when(
                is_arrival, _seconds_between("final_predicted_arrival_utc", "inferred_arrival_utc")
            ).cast("double"),
            **ctx,
        }
    ).drop("_low", "_medium", "_next_count")


# --------------------------------------------------------------------------- revisions & accuracy


def build_prediction_revisions(sightings: DataFrame, visits: DataFrame) -> DataFrame:
    """Each *distinct* prediction within a visit (first sighting), its revision and its error.

    error_seconds = predicted arrival - inferred arrival: positive means the prediction was later
    than the (inferred) arrival, i.e. the train came sooner than predicted.
    """
    first_w = Window.partitionBy("visit_id", "prediction_fingerprint").orderBy("snapshot_at", "observed_at_utc")
    distinct_preds = (
        sightings.withColumn("_rn", F.row_number().over(first_w)).where("_rn = 1").drop("_rn")
    )
    w = Window.partitionBy("visit_id").orderBy("snapshot_at", "observed_at_utc")
    v = visits.select(
        "visit_id", "outcome", "inference_confidence", "inferred_arrival_utc", "arrival_uncertainty_seconds",
        "final_predicted_arrival_utc", "service_date", "day_type", "time_period", "is_rush_hour", "rush_period",
    )
    r = distinct_preds.join(v, "visit_id").withColumns(
        {
            "revision_number": (F.row_number().over(w) - 1).cast("int"),
            "revision_seconds": _seconds_between(
                F.col("predicted_arrival_utc"), F.lag("predicted_arrival_utc").over(w)
            ).cast("double"),
        }
    )
    error = _seconds_between("predicted_arrival_utc", "inferred_arrival_utc")
    return r.select(
        "visit_id",
        "prediction_fingerprint",
        "revision_number",
        *VISIT_KEY,
        F.col("snapshot_at").alias("first_seen_at_utc"),
        "predicted_at_utc",
        "predicted_arrival_utc",
        "revision_seconds",
        "minutes_until_arrival",
        horizon_bucket(F.col("minutes_until_arrival")).alias("horizon_bucket"),
        (F.col("predicted_arrival_utc") == F.col("final_predicted_arrival_utc")).alias("matches_final_prediction"),
        "is_scheduled",
        "is_delayed",
        "outcome",
        "inference_confidence",
        "inferred_arrival_utc",
        "arrival_uncertainty_seconds",
        error.cast("double").alias("error_seconds"),
        (_seconds_between("inferred_arrival_utc", "snapshot_at") / 60).cast("double").alias("minutes_before_inferred_arrival"),
        "service_date",
        "day_type",
        "time_period",
        "is_rush_hour",
        "rush_period",
    )


def accuracy_eligible(revisions: DataFrame) -> DataFrame:
    """Predictions we can score: live (not schedule-based), on high/medium-confidence arrivals."""
    return revisions.where(
        (F.col("outcome") == "inferred_arrival")
        & F.col("inference_confidence").isin("high", "medium")
        & ~F.col("is_scheduled")
    )


def _accuracy_aggs() -> list[Column]:
    abs_err = F.abs("error_seconds")
    return [
        F.count("*").cast("int").alias("predictions"),
        F.countDistinct("visit_id").cast("int").alias("train_visits"),
        F.round(F.avg("error_seconds"), 1).alias("mean_error_seconds"),
        F.round(F.avg(abs_err), 1).alias("mean_abs_error_seconds"),
        F.percentile_approx(abs_err, 0.5).cast("double").alias("median_abs_error_seconds"),
        F.percentile_approx(abs_err, 0.9).cast("double").alias("p90_abs_error_seconds"),
        F.round(F.avg((abs_err <= 60).cast("double")) * 100, 1).alias("pct_within_1_min"),
        F.round(F.avg((abs_err <= 120).cast("double")) * 100, 1).alias("pct_within_2_min"),
        F.round(F.avg("arrival_uncertainty_seconds"), 1).alias("avg_arrival_uncertainty_seconds"),
    ]


ACCURACY_DIMS = ("station_id", "route_code", "horizon_bucket", "rush_period")
ACCURACY_GRAINS = (
    ACCURACY_DIMS,
    ("station_id", "route_code", "horizon_bucket"),
    ("station_id", "route_code", "rush_period"),
    ("station_id", "route_code"),
    ("station_id", "horizon_bucket"),
    ("station_id", "rush_period"),
    ("station_id",),
    ("route_code",),
    ("route_code", "rush_period"),
    (),
)


def build_prediction_accuracy(revisions: DataFrame) -> DataFrame:
    return rollup(accuracy_eligible(revisions), ACCURACY_DIMS, ACCURACY_GRAINS, _accuracy_aggs())


# --------------------------------------------------------------------------- headways & consistency


def build_headways(visits: DataFrame, observations: DataFrame, cfg: GoldConfig = DEFAULT_CONFIG) -> DataFrame:
    """Time between consecutive inferred arrivals of the same route at the same platform."""
    arrivals = visits.where(F.col("outcome") == "inferred_arrival")
    w = Window.partitionBy("station_id", "stop_id", "route_code").orderBy("inferred_arrival_utc", "visit_id")
    h = arrivals.select(
        "station_id", "station_name", "stop_id", "stop_description", "route_code", "route_name",
        "visit_id", "run_number", "inferred_arrival_utc", "inference_confidence",
        F.lag("visit_id").over(w).alias("previous_visit_id"),
        F.lag("run_number").over(w).alias("previous_run_number"),
        F.lag("inferred_arrival_utc").over(w).alias("previous_arrival_utc"),
        F.lag("inference_confidence").over(w).alias("previous_confidence"),
    ).where(F.col("previous_arrival_utc").isNotNull())

    h = h.withColumn("headway_minutes", (_seconds_between("inferred_arrival_utc", "previous_arrival_utc") / 60).cast("double"))

    gaps = evidence_gaps(observations, cfg)
    overlapping = (
        h.alias("h")
        .join(
            gaps.alias("g"),
            (F.col("h.station_id") == F.col("g.station_id"))
            & (F.col("g.gap_start") < F.col("h.inferred_arrival_utc"))
            & (F.col("g.gap_end") > F.col("h.previous_arrival_utc")),
        )
        .select(F.col("h.visit_id").alias("visit_id"))
        .distinct()
        .withColumn("_gap", F.lit(True))
    )
    h = h.join(overlapping, "visit_id", "left").withColumn("coverage_ok", F.col("_gap").isNull()).drop("_gap")
    good = F.array(F.lit("high"), F.lit("medium"))
    h = h.withColumn(
        "is_reliable",
        F.col("coverage_ok")
        & F.array_contains(good, F.col("inference_confidence"))
        & F.array_contains(good, F.col("previous_confidence")),
    )
    ctx = time_context(F.col("inferred_arrival_utc"), cfg)
    h = h.withColumns(ctx).withColumn("arrival_chicago", to_chicago(F.col("inferred_arrival_utc")))

    baseline_keys = ["station_id", "stop_id", "route_code", "day_type", "time_period"]
    baseline = (
        h.where("is_reliable")
        .groupBy(*baseline_keys)
        .agg(
            F.percentile_approx("headway_minutes", 0.5).cast("double").alias("typical_headway_minutes"),
            F.count("*").alias("_n"),
        )
        .where(F.col("_n") >= cfg.min_headways_for_baseline)
        .drop("_n")
    )
    h = h.join(baseline, baseline_keys, "left")
    return h.withColumns(
        {
            "headway_ratio": F.round(F.col("headway_minutes") / F.col("typical_headway_minutes"), 2),
            "is_bunched": F.col("is_reliable") & (F.col("headway_minutes") < cfg.bunching_max_minutes),
            "is_unexpected_gap": F.coalesce(
                F.col("is_reliable")
                & (F.col("headway_minutes") >= F.col("typical_headway_minutes") * cfg.unexpected_gap_ratio)
                & (F.col("headway_minutes") >= cfg.unexpected_gap_min_minutes),
                F.lit(False),
            ),
        }
    )


CONSISTENCY_DIMS = ("route_code", "station_id", "stop_id", "day_type", "time_period")
CONSISTENCY_GRAINS = (
    CONSISTENCY_DIMS,
    ("route_code", "station_id", "stop_id"),
    ("route_code", "station_id"),
    ("route_code", "day_type", "time_period"),
    ("route_code",),
    ("station_id",),
)


def build_service_consistency(headways: DataFrame, visits: DataFrame) -> DataFrame:
    """Headway regularity (coefficient of variation) and reliability signals at several grains."""
    hw = headways.where("is_reliable")
    headway_stats = rollup(
        hw,
        CONSISTENCY_DIMS,
        CONSISTENCY_GRAINS,
        [
            F.count("*").cast("int").alias("headways"),
            F.round(F.avg("headway_minutes"), 2).alias("avg_headway_minutes"),
            F.percentile_approx("headway_minutes", 0.5).cast("double").alias("median_headway_minutes"),
            F.round(F.stddev_samp("headway_minutes"), 2).alias("stddev_headway_minutes"),
            F.percentile_approx("headway_minutes", 0.9).cast("double").alias("p90_headway_minutes"),
            F.round(F.max("headway_minutes"), 2).alias("max_headway_minutes"),
            F.sum(F.col("is_unexpected_gap").cast("int")).cast("int").alias("unexpected_gaps"),
            F.sum(F.col("is_bunched").cast("int")).cast("int").alias("bunched_arrivals"),
        ],
    ).withColumn(
        # CV = stddev / mean: 0 = perfectly regular; higher = more bunching and gaps.
        "headway_cv", F.round(F.col("stddev_headway_minutes") / F.col("avg_headway_minutes"), 3)
    )
    visit_stats = rollup(
        visits,
        CONSISTENCY_DIMS,
        CONSISTENCY_GRAINS,
        [
            F.count("*").cast("int").alias("train_visits"),
            F.sum((F.col("outcome") == "inferred_arrival").cast("int")).cast("int").alias("inferred_arrivals"),
            F.sum((F.col("outcome") == "vanished_before_arrival").cast("int")).cast("int").alias("vanished_before_arrival"),
            F.round(F.avg(F.col("was_flagged_delayed").cast("double")) * 100, 1).alias("pct_flagged_delayed"),
            F.round(F.avg(F.col("max_abs_revision_seconds")), 1).alias("avg_max_revision_seconds"),
        ],
    )
    return visit_stats.join(headway_stats, list(CONSISTENCY_DIMS), "full_outer")


def build_rush_hour_comparison(headways: DataFrame, revisions: DataFrame, visits: DataFrame) -> DataFrame:
    """Rush vs non-rush hour, long format: one row per (route, station, rush_period)."""
    dims = ("route_code", "station_id", "rush_period")
    grains = (dims, ("route_code", "rush_period"), ("station_id", "rush_period"), ("rush_period",))
    hw = rollup(
        headways.where("is_reliable"),
        dims,
        grains,
        [
            F.count("*").cast("int").alias("headways"),
            F.round(F.avg("headway_minutes"), 2).alias("avg_headway_minutes"),
            F.round(F.stddev_samp("headway_minutes") / F.avg("headway_minutes"), 3).alias("headway_cv"),
            F.sum(F.col("is_unexpected_gap").cast("int")).cast("int").alias("unexpected_gaps"),
        ],
    )
    acc = rollup(
        accuracy_eligible(revisions),
        dims,
        grains,
        [
            F.count("*").cast("int").alias("scored_predictions"),
            F.round(F.avg(F.abs("error_seconds")), 1).alias("mean_abs_error_seconds"),
            F.round(F.avg((F.abs("error_seconds") <= 60).cast("double")) * 100, 1).alias("pct_within_1_min"),
        ],
    )
    vs = rollup(
        visits,
        dims,
        grains,
        [
            F.count("*").cast("int").alias("train_visits"),
            F.round(F.avg(F.col("was_flagged_delayed").cast("double")) * 100, 1).alias("pct_flagged_delayed"),
        ],
    )
    return vs.join(hw, list(dims), "full_outer").join(acc, list(dims), "full_outer")


# --------------------------------------------------------------------------- entry point


@dataclass(frozen=True)
class GoldBatch:
    train_visits: DataFrame
    prediction_revisions: DataFrame
    prediction_accuracy: DataFrame
    headways: DataFrame
    service_consistency: DataFrame
    rush_hour_comparison: DataFrame
    collection_coverage: DataFrame


Materialize = Callable[[str, DataFrame], DataFrame]


def _no_materialize(_name: str, df: DataFrame) -> DataFrame:
    return df


def build_gold(
    predictions: DataFrame,
    observations: DataFrame,
    cfg: GoldConfig = DEFAULT_CONFIG,
    materialize: Materialize = _no_materialize,
) -> GoldBatch:
    """Compute every Gold table from Silver, in dependency order.

    ``materialize(name, df)`` is called for each *base* table (visits, revisions, headways,
    coverage) before anything reads it. The notebook writes the Delta table and returns it read
    back. That cuts the lineage, so the aggregates don't each recompute the whole inference
    chain, and their query plans stay small. Tests pass a local checkpoint instead.
    """
    sightings = assign_visits(predictions, observations, cfg)
    visits = materialize("train_visits", build_visits(sightings, observations, cfg))
    revisions = materialize("prediction_revisions", build_prediction_revisions(sightings, visits))
    headways = materialize("headways", build_headways(visits, observations, cfg))
    coverage = materialize("collection_coverage", collection_coverage(observations, cfg))
    return GoldBatch(
        train_visits=visits,
        prediction_revisions=revisions,
        prediction_accuracy=build_prediction_accuracy(revisions),
        headways=headways,
        service_consistency=build_service_consistency(headways, visits),
        rush_hour_comparison=build_rush_hour_comparison(headways, revisions, visits),
        collection_coverage=coverage,
    )
