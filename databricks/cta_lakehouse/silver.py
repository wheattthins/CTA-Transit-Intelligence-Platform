"""Silver transformation: Bronze responses -> validated, typed, deduplicated records.

Outputs (see ``transform_bronze_batch``):

* **observations**: one row per collector API call (successful or CTA error). Gold uses this for
  *coverage*: it tells "we polled and saw no trains" apart from "we weren't polling".
* **predictions**: one row per valid arrival prediction (``eta`` element), typed, in UTC.
* **rejects**: quarantined records with machine-readable reasons, never silently dropped.

Validation has two levels:
* *rejection reasons*: hard failures; the record goes to rejects, not predictions.
* *quality warnings*: soft issues; the record is kept, the issue is recorded, and bad optional
  values (such as out-of-range coordinates) are nulled.

Pure PySpark, so the notebook and local unit tests run exactly the same code.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

CHICAGO_TZ = "America/Chicago"
CTA_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

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
FLAG_FIELDS = ("isApp", "isSch", "isDly", "isFlt")

# Generous bounding box around the CTA rail network (Kenosha-ish to Indiana border).
LAT_RANGE = (41.6, 42.2)
LON_RANGE = (-88.0, -87.5)

MAX_PREDICTION_HORIZON_MINUTES = 180  # CTA predicts at most ~1 hour ahead; 3h is clearly wrong
STALE_PREDICTION_SECONDS = 300  # prediction generated >5 min before we observed it

ETA_FIELDS = (
    "staId", "stpId", "staNm", "stpDe", "rn", "rt", "destSt", "destNm", "trDr",
    "prdt", "arrT", "isApp", "isSch", "isDly", "isFlt", "flags", "lat", "lon", "heading",
)
# Everything as STRING: CTA sends strings, and typing happens after validation.
RESPONSE_SCHEMA = (
    "response STRUCT<ctatt: STRUCT<eta: ARRAY<STRUCT<"
    + ", ".join(f"{f}: STRING" for f in ETA_FIELDS)
    + ">>>>"
)


@dataclass(frozen=True)
class SilverBatch:
    observations: DataFrame
    predictions: DataFrame
    rejects: DataFrame


# --------------------------------------------------------------------------- helpers


def cta_local_to_utc(col: Column) -> Column:
    """CTA local (America/Chicago, no offset) string -> UTC TIMESTAMP; NULL if unparseable.

    Uses ``try_to_timestamp`` so malformed values become NULL (and are rejected) instead of
    failing the whole batch under ANSI mode.

    The session time zone must be UTC (set by the notebooks and the test session). During the
    November fall-back hour, Spark resolves the ambiguous local time to the earlier (CDT) instant.
    """
    local = F.call_function("try_to_timestamp", col, F.lit(CTA_TIMESTAMP_FORMAT))
    return F.to_utc_timestamp(local, CHICAGO_TZ)


def utc_to_chicago_ntz(col: Column) -> Column:
    """UTC TIMESTAMP -> TIMESTAMP_NTZ holding Chicago wall-clock time (for rush-hour logic)."""
    return F.from_utc_timestamp(col, CHICAGO_TZ).cast("timestamp_ntz")


def _blank_to_null(col: Column) -> Column:
    return F.when(F.trim(col) == "", None).otherwise(F.trim(col))


def _route_name(col: Column) -> Column:
    mapping = F.create_map(*[F.lit(x) for kv in ROUTE_NAMES.items() for x in kv])
    return mapping[col]


def _reasons(*checks: tuple[Column, str]) -> Column:
    """Build an array of reason codes for the checks whose condition is TRUE (NULL counts as pass)."""
    return F.array_compact(F.array(*[F.when(cond, F.lit(code)) for cond, code in checks]))


# --------------------------------------------------------------------------- stages


def usable_responses(bronze: DataFrame) -> DataFrame:
    return bronze.where(~F.col("is_parse_error") & ~F.coalesce(F.col("is_sample"), F.lit(False)))


def response_level_rejects(bronze: DataFrame) -> DataFrame:
    """Whole files that can't feed Silver: unparseable envelopes and SAMPLE fixture data."""
    reasons = _reasons(
        (F.col("is_parse_error"), "unparseable_envelope"),
        (F.coalesce(F.col("is_sample"), F.lit(False)), "sample_data"),
    )
    return (
        bronze.withColumn("rejection_reasons", reasons)
        .where(F.size("rejection_reasons") > 0)
        .select(
            F.sha2(F.concat_ws(":", "record_key", F.lit("response")), 256).alias("reject_key"),
            F.lit("response").alias("reject_level"),
            "record_key",
            "collection_id",
            "station_id",
            F.lit(None).cast("int").alias("eta_index"),
            "rejection_reasons",
            F.col("raw_json").alias("raw_record"),
            "source_file_path",
            F.col("ingested_at_utc").alias("bronze_ingested_at_utc"),
        )
    )


def explode_predictions(responses: DataFrame) -> DataFrame:
    """One row per ``eta`` element, with its position and the parent response's context."""
    parsed = responses.withColumn("_resp", F.from_json("raw_json", RESPONSE_SCHEMA))
    return parsed.select(
        "record_key",
        "collection_id",
        F.col("station_id").alias("requested_station_id"),
        "received_at_utc",
        "source_file_path",
        F.col("ingested_at_utc").alias("bronze_ingested_at_utc"),
        F.posexplode(F.coalesce(F.col("_resp.response.ctatt.eta"), F.array())).alias("eta_index", "eta"),
    )


def validate_predictions(exploded: DataFrame) -> DataFrame:
    """Add ``rejection_reasons`` and ``quality_warnings`` arrays and typed helper columns."""
    e = {f: _blank_to_null(F.col(f"eta.{f}")) for f in ETA_FIELDS}
    df = exploded.select(
        "*",
        cta_local_to_utc(e["prdt"]).alias("_prdt_utc"),
        cta_local_to_utc(e["arrT"]).alias("_arrt_utc"),
        F.expr("try_cast(nullif(trim(eta.lat), '') AS DOUBLE)").alias("_lat"),
        F.expr("try_cast(nullif(trim(eta.lon), '') AS DOUBLE)").alias("_lon"),
        F.expr("try_cast(nullif(trim(eta.heading), '') AS INT)").alias("_heading"),
    )

    # Duplicate = same stop, run and predicted arrival twice in one response; keep the first.
    dup_window = Window.partitionBy("collection_id", "eta.stpId", "eta.rn", "eta.arrT").orderBy("eta_index")
    df = df.withColumn("_dup_rank", F.row_number().over(dup_window))

    horizon_min = (F.col("_arrt_utc").cast("long") - F.col("_prdt_utc").cast("long")) / 60
    checks: list[tuple[Column, str]] = [(e[f].isNull(), f"missing_{f}") for f in REQUIRED_ETA_FIELDS]
    checks += [
        (e["prdt"].isNotNull() & F.col("_prdt_utc").isNull(), "invalid_prdt"),
        (e["arrT"].isNotNull() & F.col("_arrt_utc").isNull(), "invalid_arrT"),
        (e["rt"].isNotNull() & ~e["rt"].isin(list(ROUTE_NAMES)), "unknown_route"),
        (e["staId"].isNotNull() & ~e["staId"].rlike(r"^4\d{4}$"), "invalid_station_id"),
        (e["stpId"].isNotNull() & ~e["stpId"].rlike(r"^3\d{4}$"), "invalid_stop_id"),
        (e["staId"].isNotNull() & (e["staId"] != F.col("requested_station_id")), "station_mismatch"),
        (F.col("_arrt_utc") < F.col("_prdt_utc"), "arrival_before_prediction"),
        (horizon_min > MAX_PREDICTION_HORIZON_MINUTES, "prediction_horizon_too_long"),
        (F.col("_dup_rank") > 1, "duplicate_in_response"),
    ]
    checks += [
        (e[f].isNotNull() & ~e[f].isin("0", "1"), f"invalid_{f}") for f in FLAG_FIELDS
    ]

    is_scheduled = e["isSch"] == "1"
    coords_present = F.col("_lat").isNotNull() & F.col("_lon").isNotNull() & ~(
        (F.col("_lat") == 0) & (F.col("_lon") == 0)
    )
    coords_in_range = F.col("_lat").between(*LAT_RANGE) & F.col("_lon").between(*LON_RANGE)
    stale_seconds = F.col("received_at_utc").cast("long") - F.col("_prdt_utc").cast("long")
    warnings: list[tuple[Column, str]] = [
        (~is_scheduled & ~coords_present, "missing_coordinates_for_live_train"),
        (coords_present & ~coords_in_range, "coordinates_out_of_range"),
        (F.col("_heading").isNotNull() & ~F.col("_heading").between(0, 359), "invalid_heading"),
        (e["heading"].isNotNull() & F.col("_heading").isNull(), "invalid_heading"),
        (stale_seconds > STALE_PREDICTION_SECONDS, "stale_prediction"),
    ]

    return df.withColumns(
        {
            "rejection_reasons": _reasons(*checks),
            "quality_warnings": F.array_distinct(_reasons(*warnings)),
            "_coords_ok": coords_present & coords_in_range,
        }
    )


def to_silver_predictions(valid: DataFrame) -> DataFrame:
    e = {f: _blank_to_null(F.col(f"eta.{f}")) for f in ETA_FIELDS}

    def flag(field: str) -> Column:
        return F.coalesce(e[field] == "1", F.lit(False))

    def seconds(later: str, earlier: str) -> Column:
        return F.col(later).cast("long") - F.col(earlier).cast("long")

    return valid.select(
        F.sha2(F.concat_ws(":", "collection_id", F.col("eta_index").cast("string")), 256).alias("prediction_key"),
        F.sha2(F.concat_ws("|", e["stpId"], e["rn"], e["prdt"], e["arrT"]), 256).alias("prediction_fingerprint"),
        "collection_id",
        "eta_index",
        e["staId"].alias("station_id"),
        e["staNm"].alias("station_name"),
        e["stpId"].alias("stop_id"),
        e["stpDe"].alias("stop_description"),
        e["rt"].alias("route_code"),
        _route_name(e["rt"]).alias("route_name"),
        e["rn"].alias("run_number"),
        F.when(e["destSt"] != "0", e["destSt"]).alias("destination_stop_id"),  # "0" = unknown (scheduled)
        e["destNm"].alias("destination_name"),
        e["trDr"].alias("direction_code"),
        F.col("received_at_utc").alias("observed_at_utc"),
        F.col("_prdt_utc").alias("predicted_at_utc"),
        F.col("_arrt_utc").alias("predicted_arrival_utc"),
        utc_to_chicago_ntz(F.col("_arrt_utc")).alias("predicted_arrival_chicago"),
        (seconds("_arrt_utc", "_prdt_utc") / 60).cast("double").alias("minutes_until_arrival"),
        seconds("_arrt_utc", "received_at_utc").cast("int").alias("seconds_until_arrival_at_observation"),
        seconds("received_at_utc", "_prdt_utc").cast("int").alias("prediction_age_seconds"),
        flag("isApp").alias("is_approaching"),
        flag("isSch").alias("is_scheduled"),
        flag("isDly").alias("is_delayed"),
        flag("isFlt").alias("is_fault"),
        F.when(F.col("_coords_ok"), F.col("_lat")).alias("latitude"),
        F.when(F.col("_coords_ok"), F.col("_lon")).alias("longitude"),
        F.when(F.col("_heading").between(0, 359), F.col("_heading")).alias("heading"),
        "quality_warnings",
        "record_key",
        "bronze_ingested_at_utc",
    )


def prediction_level_rejects(rejected: DataFrame) -> DataFrame:
    return rejected.select(
        F.sha2(F.concat_ws(":", "record_key", F.col("eta_index").cast("string")), 256).alias("reject_key"),
        F.lit("prediction").alias("reject_level"),
        "record_key",
        "collection_id",
        F.col("requested_station_id").alias("station_id"),
        "eta_index",
        "rejection_reasons",
        F.to_json("eta").alias("raw_record"),
        "source_file_path",
        "bronze_ingested_at_utc",
    )


def build_observations(responses: DataFrame, validated: DataFrame) -> DataFrame:
    counts = validated.groupBy("collection_id").agg(
        F.sum(F.when(F.size("rejection_reasons") == 0, 1).otherwise(0)).alias("valid_prediction_count"),
        F.sum(F.when(F.size("rejection_reasons") > 0, 1).otherwise(0)).alias("rejected_prediction_count"),
    )
    return (
        responses.join(counts, "collection_id", "left")
        .select(
            "collection_id",
            "record_key",
            "station_id",
            "requested_at_utc",
            F.col("received_at_utc").alias("observed_at_utc"),
            utc_to_chicago_ntz(F.col("received_at_utc")).alias("observed_at_chicago"),
            cta_local_to_utc(F.col("cta_tmst_local")).alias("cta_response_at_utc"),
            "cta_error_code",
            "cta_error_name",
            (F.col("cta_error_code") == "0").alias("is_successful"),
            "eta_count",
            F.coalesce("valid_prediction_count", F.lit(0)).cast("int").alias("valid_prediction_count"),
            F.coalesce("rejected_prediction_count", F.lit(0)).cast("int").alias("rejected_prediction_count"),
            "http_status",
            "elapsed_ms",
            "attempts",
            "collector_version",
            "source_file_path",
            F.col("ingested_at_utc").alias("bronze_ingested_at_utc"),
        )
    )


def transform_bronze_batch(bronze: DataFrame) -> SilverBatch:
    """Run all Silver stages on a batch of Bronze rows. Callers add processing timestamps."""
    responses = usable_responses(bronze)
    validated = validate_predictions(explode_predictions(responses))
    is_valid = F.size("rejection_reasons") == 0
    rejects = response_level_rejects(bronze).unionByName(prediction_level_rejects(validated.where(~is_valid)))
    return SilverBatch(
        observations=build_observations(responses, validated),
        predictions=to_silver_predictions(validated.where(is_valid)),
        rejects=rejects,
    )


def with_processing_time(batch: SilverBatch, processed_at: Column | None = None) -> SilverBatch:
    """Stamp processing time and order columns exactly as the target tables declare them."""
    from cta_lakehouse.tables import SILVER_OBSERVATIONS, SILVER_PREDICTIONS, SILVER_REJECTS

    ts = processed_at if processed_at is not None else F.current_timestamp()
    return SilverBatch(
        observations=batch.observations.withColumn("silver_processed_at_utc", ts).select(*SILVER_OBSERVATIONS.column_names),
        predictions=batch.predictions.withColumn("silver_processed_at_utc", ts).select(*SILVER_PREDICTIONS.column_names),
        rejects=batch.rejects.withColumn("rejected_at_utc", ts).select(*SILVER_REJECTS.column_names),
    )
