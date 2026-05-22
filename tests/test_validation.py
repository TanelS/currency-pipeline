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

from spark.utils.validation import build_error_column

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def evaluate(spark, col_name, rules, data, schema):
    """
    Evaluates a specified column in a DataFrame against a set of validation rules. This function creates a
    Spark DataFrame using the provided data and schema, applies the validation rules to the specified
    column, and collects the resulting error data for each row that fails validation.

    :param spark: A Spark session used to create and manipulate the DataFrame.
    :param col_name: The name of the column to be validated.
    :param rules: A list of validation rules to apply to the column. Each rule specifies a condition that
        the column's data must satisfy.
    :param data: The data to be used for creating the DataFrame. This should be a list of rows where each
        row corresponds to a record in the DataFrame.
    :param schema: Schema definition of the DataFrame, describing the structure and data types of each
        column.
    :return: A list containing validation error results for each row where the specified column does not
        satisfy the validation rules.
    """
    df = spark.createDataFrame(data, schema)
    error_col = build_error_column(col_name, rules)
    result = df.withColumn("_error", error_col).select("_error").collect()
    return [row["_error"] for row in result]


def test_required_null(spark):
    """
    Evaluates a dataset against a set of validation rules to check if the specified field
    is required and ensures that it does not accept null or missing values.

    :param spark: The Spark session object.
    :return: None. The function performs assertions to validate the dataset.
    """
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
    """
    Evaluates a given column against a minimum value constraint in the provided dataset
    and returns a list of errors corresponding to rows that violate the constraint.

    :param spark: SparkSession object used to process the dataset.
    :type spark: SparkSession
    :return: A list of error messages for each row in the dataset evaluated
             against the constraint. An empty string represents no violation
             for that row.
    :rtype: List[str]

    """
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
    """
    Evaluates and validates the given dataset against a specified maximum value constraint
    for the "age" field. If the value in the "age" field exceeds the maximum allowed value,
    an error message is recorded for that row.

    :param spark: Spark session used for creating and processing the dataset.
    :type spark: pyspark.sql.SparkSession
    :return: Validated errors generated from the dataset as messages indicating whether
             rows exceed the maximum value constraint or are valid.
    :rtype: list[str]
    """
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
    """
    Tests length validation logic on input data using the `evaluate` function. This function
    validates whether the provided string values meet the required length constraint and
    identifies violations accordingly.

    :param spark: SparkSession instance utilized for the validation process.
    :type spark: pyspark.sql.SparkSession

    :return: None
    """
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


def test_no_rules_returns_none(spark):
    """
    Builds an error column for a given name and a set of rules. Returns None if no
    rules are provided.

    :param spark: Spark session object.
    :type spark: SparkSession
    :return: None if no rules are provided, otherwise returns the error column.
    """
    result = build_error_column("name", {})
    assert result is None


def test_multiple_rules_combined(spark):
    """
    Evaluates a set of rules against test cases for a given Spark DataFrame column and
    asserts the error messages generated. This function applies a range validation
    rule (minimum and maximum values) for the specified column, checks the resulting
    errors for each test case, and verifies that the errors align with expected outputs.

    :param spark: SparkSession object used to create and process the DataFrame
    :type spark: pyspark.sql.SparkSession
    :return: None
    :rtype: NoneType
    """
    errors = evaluate(
        spark,
        "age",
        {"min": 0, "max": 150},
        [(-1,), (200,), (50,)],
        StructType([StructField("age", IntegerType())]),
    )

    print(f"**** {errors = }")

    assert "below min" in errors[0]
    assert "above max" in errors[1]
    assert errors[2] == ""


def test_min_date_violation(spark):
    """
    Validates the functionality of the `evaluate` function by testing the enforcement of a
    minimum date constraint on a datetime field within a dataset. This test checks if the
    function correctly identifies a violation when a date earlier than the specified minimum
    date is encountered in the dataset.

    :param spark: SparkSession instance used to create and manage the test's Spark context.
    :type spark: SparkSession

    :return: None
    """
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
    """
    Evaluates whether a given dataset violates the maximum allowable rate value.

    This function checks the dataset for values that exceed the specified maximum
    rate value constraint and returns relevant error messages.

    :param spark: Spark session object used to execute the evaluation.
    :type spark: pyspark.sql.SparkSession
    :return: List of error messages for each row in the dataset. If a row violates
             the constraint, the corresponding error message indicates the issue.
             Otherwise, the corresponding value is an empty string.
    :rtype: list[str]
    """
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
    """
    Validates that the `evaluate` function correctly identifies and handles a violation of
    the minimum rate value constraint. Specifically, this function checks whether a
    rate value less than the minimum allowed value triggers the expected error message.

    :param spark: SparkSession object used to execute and analyze the data.
    :type spark: SparkSession
    :return: None
    :rtype: NoneType
    """
    errors = evaluate(
        spark,
        "rate",
        {"min_rate_value": 0},
        [(-1.0,), (1.0,)],
        StructType([StructField("rate", DoubleType())]),
    )
    assert "must be more than" in errors[0]
    assert errors[1] == ""
