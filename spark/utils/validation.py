import html
import re

import unicodedata
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    TimestampType,
    StructType,
)

from typing import List, Union, Optional

CARRIAGE_RETURN_PATTERN = re.compile(r"\r+")
WHITESPACE_PATTERN = re.compile(r"[\n\t]+")
ZERO_WIDTH_PATTERN = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")
MULTIPLE_SPACES_PATTERN = re.compile(r"\s{2,}")
CONTROL_CHARS_PATTERN = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]")


# with open('./conf/base/parameters.yml', 'r') as file:
#     validation_params = yaml.safe_load(file)
#
# if validation_params is None:
#     raise ValueError("Validation parameters cannot be None")
#
# currency_rules = validation_params['validation']['currencies']['columns']
# rates_rules = validation_params['validation']['rates']

def clean_string(s: Union[str, int, float, None]) -> Union[str, int, float, None]:
    """
    Cleans a given string by normalizing, removing unwanted characters, and trimming.
    Handles strings, numbers, floats, and None values. Ensures the input is treated
    properly and unwanted or ill-formed strings return a cleaned result.

    :param s: The input value that can be a string, integer, float, or None.
               If the input is None, the method returns None. If the input
               is a string, it will normalize, process, and clean it.
               Non-string inputs are returned unchanged.
    :return: A cleaned and normalized string, the original non-string value, or None
             if the string is empty after processing.
    """
    if s is None:
        return None

    if not isinstance(s, str):
        return s

    if not s or not s.strip():
        return None

    s = html.unescape(s)
    s = unicodedata.normalize("NFKC", s)
    s = CONTROL_CHARS_PATTERN.sub("", s)
    s = CARRIAGE_RETURN_PATTERN.sub("", s)
    s = WHITESPACE_PATTERN.sub(" ", s)
    s = ZERO_WIDTH_PATTERN.sub("", s)
    s = MULTIPLE_SPACES_PATTERN.sub(" ", s)
    s = s.strip()

    return s if s else None


clean_string_udf = udf(clean_string, StringType()) # slower than native functions when dealing with large datasets


def build_error_column(col_name: str, rules: dict):
    errors = []
    if rules.get('required'):
        errors.append(F.when(F.col(col_name).isNull(), F.lit(f'{col_name} is required')))
    elif rules.get('min') is not None:
        errors.append(F.when(F.col(col_name) < rules['min'], F.lit(f'{col_name} below min {rules["min"]}')))
    elif rules.get('max') is not None:
        errors.append(F.when(F.col(col_name) > rules['max'], F.lit(f'{col_name} above max {rules["max"]}')))
    return errors


def clean_string_df(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Clean string-based columns in a DataFrame by applying a cleaning function and replacing empty strings
    with None. This is performed only for specified column names.

    :param df: The input DataFrame containing the data to be cleaned.
    :type df: DataFrame
    :param columns: A list of column names to be cleaned. Only columns present in the DataFrame will
        be processed.
    :type columns: List[str]
    :return: A DataFrame with specified columns cleaned by trimming whitespace, replacing empty strings
        with None, and applying the `clean_string_udf` function.
    :rtype: DataFrame
    """
    if df.count() == 0:
        return df

    cleaned_df = df

    for col_name in columns:
        if col_name in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", None)
                .otherwise(clean_string_udf(F.col(col_name)))
            )
    return cleaned_df




def validate_int_df(df: DataFrame, columns: List[str], rules: dict) -> DataFrame:
    """
    Validates the specified columns in the provided DataFrame according to customizable validation rules.

    This function checks if specified columns meet the given validation rules. These rules can include
    requirements such as checking for null values (if a column is required) and ensuring that column values
    are within a specified minimum and maximum range. Columns that violate any rule will result in
    corresponding validation error messages being added. All validation error messages are concatenated
    into a single column named '_validation_errors' within the DataFrame.

    :param df: The input DataFrame to be validated.
    :type df: DataFrame
    :param columns: A list of column names in the DataFrame to validate.
    :type columns: List[str]
    :param rules: A dictionary where each key corresponds to a column name, and the value is a dictionary of
        validation rules. Possible rules include:
        - required: A boolean indicating whether the column is required (no null values allowed).
        - min: A numeric value specifying the minimum allowed value for the column.
        - max: A numeric value specifying the maximum allowed value for the column.
    :type rules: dict
    :return: A DataFrame with an added '_validation_errors' column containing concatenated error messages
        for rows where validation rules are violated.
    :rtype: DataFrame
    """
    error_cols = []
    for col_name in columns:
        if col_name in df.columns:
            col_rules = rules.get(col_name)
            if not col_rules:
                continue
            if col_rules.get('required'):
                error_cols.append(F.when(F.col(col_name).isNull(), F.lit(f'{col_name} is required')))
            if col_rules.get('min') is not None:
                error_cols.append(F.when(F.col(col_name) < col_rules['min'], F.lit(f'{col_name} below min')))
            if col_rules.get('max') is not None:
                error_cols.append(F.when(F.col(col_name) > col_rules['max'], F.lit(f'{col_name} above max')))

    if error_cols:
        df = df.withColumn('_validation_errors', F.concat_ws('; ', *error_cols))
    return df


def validate_timestamp_df(df: DataFrame, columns: List[str], rules: dict) -> DataFrame:
    """
    Validates timestamp columns in a Spark DataFrame against a set of rules. The function checks whether the
    columns specified in the input are present in the DataFrame and validates them based on the provided
    rules, which can include conditions such as whether column values are required or have a minimum allowed
    date. The resulting DataFrame is returned with validation errors concatenated into a new column named
    `_validation_errors` if any issues are found.

    :param df: Spark DataFrame to be validated.
    :type df: DataFrame
    :param columns: List of column names that need to be validated.
    :type columns: List[str]
    :param rules: Dictionary specifying validation rules for each column. The key is the column name, and the
                  value is another dictionary containing optional keys such as `required` (boolean) and
                  `min_date` (date string in format compatible with Spark's date handling).
    :type rules: dict
    :return: Spark DataFrame with validation errors captured in a new column `_validation_errors`.
    :rtype: DataFrame
    """
    error_cols = []
    for col_name in columns:
        if col_name in df.columns:
            col_rules = rules.get(col_name)
            if not col_rules:
                continue
            if col_rules.get('required'):
                error_cols.append(F.when(F.col(col_name).isNull(), F.lit(f'{col_name} is required')))
            if col_rules.get('min_date'):
                error_cols.append(
                    F.when(F.col(col_name) < F.lit(col_rules['min_date'])
                    .cast(TimestampType()), F.lit(f'{col_name} must be later than "{col_rules["min_date"]}"'))
                )
    if error_cols:
        df = df.withColumn('_validation_errors', F.concat_ws('; ', *error_cols))
    return df


