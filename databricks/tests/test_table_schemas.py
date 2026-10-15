"""Transformation output must match the declared table schemas exactly (names, order, types)."""

import pytest
from conftest import make_envelope
from cta_lakehouse import tables
from cta_lakehouse.silver import transform_bronze_batch, with_processing_time


def _types(spark, spec):
    return [(f.name, f.dataType.simpleString()) for f in spark.createDataFrame([], spec.ddl_schema()).schema]


def _df_types(df):
    return [(f.name, f.dataType.simpleString()) for f in df.schema]


def test_bronze_output_matches_table(spark, bronze_df):
    assert _df_types(bronze_df(make_envelope())) == _types(spark, tables.BRONZE_ARRIVALS)


@pytest.mark.parametrize(
    ("attr", "spec"),
    [
        ("observations", tables.SILVER_OBSERVATIONS),
        ("predictions", tables.SILVER_PREDICTIONS),
        ("rejects", tables.SILVER_REJECTS),
    ],
)
def test_silver_output_matches_table(spark, bronze_df, attr, spec):
    batch = with_processing_time(transform_bronze_batch(bronze_df(make_envelope())))
    assert _df_types(getattr(batch, attr)) == _types(spark, spec)


def test_create_sql_is_valid_ddl():
    sql = tables.SILVER_PREDICTIONS.create_sql("workspace.cta_silver.train_arrival_predictions")
    assert sql.startswith("CREATE TABLE IF NOT EXISTS workspace.cta_silver.train_arrival_predictions (")
    assert "prediction_key STRING NOT NULL" in sql
    assert "'delta.enableChangeDataFeed' = 'true'" in sql


def test_keys_are_declared_columns():
    for spec in (tables.BRONZE_ARRIVALS, tables.SILVER_OBSERVATIONS, tables.SILVER_PREDICTIONS, tables.SILVER_REJECTS):
        assert spec.key in spec.column_names
