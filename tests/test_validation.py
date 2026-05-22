import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.utils.validation import _append_errors, build_error_column, validate_df

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def evaluate(spark, col_name, rules, data, schema):
    df = spark.createDataFrame(data, schema)
    error_col = build_error_column(col_name, rules)
    result = df.withColumn("_error", error_col).select("_error").collect()
    return [row["_error"] for row in result]


# --- build_error_column ---


def test_required_null(spark):
    errors = evaluate(
        spark,
        "name",
        {"required": True},
        [(None,), ("valid",)],
        StructType([StructField("name", StringType())]),
    )
    assert errors[0] == "name is required"
    assert errors[1] == ""


def test_min_violation(spark):
    errors = evaluate(
        spark,
        "age",
        {"min": 18},
        [(17,), (18,), (25,)],
        StructType([StructField("age", IntegerType())]),
    )
    assert errors[0] == "age below min 18"
    assert errors[1] == ""
    assert errors[2] == ""


def test_max_violation(spark):
    errors = evaluate(
        spark,
        "age",
        {"max": 100},
        [(101,), (100,)],
        StructType([StructField("age", IntegerType())]),
    )
    assert errors[0] == "age above max 100"
    assert errors[1] == ""


def test_length_violation(spark):
    errors = evaluate(
        spark,
        "code",
        {"length": 3},
        [("EU",), ("EUR",), ("EURO",)],
        StructType([StructField("code", StringType())]),
    )
    assert errors[0] == "code must be 3 characters long"
    assert errors[1] == ""
    assert errors[2] == "code must be 3 characters long"


def test_no_rules_returns_none():
    result = build_error_column("name", {})
    assert result is None


def test_multiple_rules_combined(spark):
    errors = evaluate(
        spark,
        "age",
        {"min": 0, "max": 150},
        [(-1,), (200,), (50,)],
        StructType([StructField("age", IntegerType())]),
    )
    assert "below min" in errors[0]
    assert "above max" in errors[1]
    assert errors[2] == ""


def test_min_date_violation(spark):
    errors = evaluate(
        spark,
        "rate_date",
        {"min_date": "2020-01-01"},
        [(datetime(2019, 1, 1),), (datetime(2021, 1, 1),)],
        StructType([StructField("rate_date", TimestampType())]),
    )
    assert "must be later than" in errors[0]
    assert errors[1] == ""


def test_max_rate_value_violation(spark):
    errors = evaluate(
        spark,
        "rate",
        {"max_rate_value": 1000},
        [(1001.0,), (999.0,)],
        StructType([StructField("rate", DoubleType())]),
    )
    assert "must be less than" in errors[0]
    assert errors[1] == ""


def test_min_rate_value_violation(spark):
    errors = evaluate(
        spark,
        "rate",
        {"min_rate_value": 0},
        [(-1.0,), (1.0,)],
        StructType([StructField("rate", DoubleType())]),
    )
    assert "must be more than" in errors[0]
    assert errors[1] == ""


# --- _append_errors ---


def test_append_errors_creates_column(spark):
    df = spark.createDataFrame(
        [(None,)], StructType([StructField("name", StringType())])
    )
    result = _append_errors(df, [build_error_column("name", {"required": True})])
    assert "_validation_errors" in result.columns


def test_append_errors_accumulates(spark):
    # Second call must not overwrite errors added by the first call
    schema = StructType(
        [
            StructField("code", StringType()),
            StructField("age", IntegerType()),
        ]
    )
    df = spark.createDataFrame([("X", 5)], schema)
    df = _append_errors(df, [build_error_column("code", {"length": 3})])
    df = _append_errors(df, [build_error_column("age", {"min": 18})])
    errors = df.select("_validation_errors").collect()[0][0]
    assert "code" in errors
    assert "age" in errors


def test_append_errors_no_separator_artifacts_on_valid_row(spark):
    # A row passing all rules should produce "" not "; " or "; ; ;"
    schema = StructType([StructField("age", IntegerType())])
    df = spark.createDataFrame([(25,)], schema)
    df = _append_errors(df, [build_error_column("age", {"min": 18})])
    df = _append_errors(df, [build_error_column("age", {"max": 100})])
    errors = df.select("_validation_errors").collect()[0][0]
    assert errors == ""


# --- validate_df ---


def test_validate_df_adds_errors(spark):
    schema = StructType([StructField("age", IntegerType())])
    df = spark.createDataFrame([(5,), (25,)], schema)
    result = validate_df(df, ["age"], {"age": {"min": 18}})
    errors = [
        row["_validation_errors"]
        for row in result.select("_validation_errors").collect()
    ]
    assert "below min" in errors[0]
    assert errors[1] == ""


def test_validate_df_skips_missing_columns(spark):
    # Columns in the list but absent from the DataFrame should be silently ignored
    schema = StructType([StructField("age", IntegerType())])
    df = spark.createDataFrame([(5,)], schema)
    result = validate_df(df, ["age", "nonexistent"], {"age": {"min": 18}})
    assert "_validation_errors" in result.columns
