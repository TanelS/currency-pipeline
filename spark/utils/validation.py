import html
import re
import unicodedata
from typing import List, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, TimestampType

CARRIAGE_RETURN_PATTERN = re.compile(r"\r+")
WHITESPACE_PATTERN = re.compile(r"[\n\t]+")
ZERO_WIDTH_PATTERN = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")
MULTIPLE_SPACES_PATTERN = re.compile(r"\s{2,}")
CONTROL_CHARS_PATTERN = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]")


def clean_string(s: Union[str, int, float, None]) -> Union[str, int, float, None]:
    """
    Cleans and normalizes a given string by removing unwanted characters, patterns, and
    unnecessary spaces. If the input is not a string (or is None), it will be returned as is.
    The normalization ensures that the string conforms to a consistent format.

    :param s: The input value to be cleaned and normalized. Can be of type str, int, float,
        or None.
    :return: The cleaned and normalized string if the input was a string. If the input was
        of another type, the same value is returned without modification. None is returned
        if the input was empty or consisted only of whitespace.
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


def build_error_column(col_name: str, rules: dict) -> list:
    """
    Builds a list of error conditions based on validation rules for a specified column.

    This function takes a column name and a dictionary of validation rules, and generates
    a list of PySpark expressions that define error conditions for that column. These conditions
    can be used to validate data against the provided rules, such as checking for null values,
    minimum values, or maximum values.

    :param col_name: The name of the column to apply the validation rules to.
    :type col_name: str
    :param rules: A dictionary of validation rules. Keys may include:
                  - "required": A boolean indicating whether the column is mandatory.
                  - "min": A numeric value specifying the minimum permissible value.
                  - "max": A numeric value specifying the maximum permissible value.
                  Each key is optional, and the function will generate error conditions
                  only for the rules that are specified.
    :type rules: dict
    :return: A list of PySpark `when` expressions that define the error conditions for
             the specified column. Each expression represents a validation error if the
             corresponding rule is violated.
    :rtype: list
    """
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
    Cleans specified string columns in a DataFrame by applying a user-defined string cleaning function
    and replacing empty strings with `None`.

    This function iterates over the provided column list and, for each column that exists in the
    DataFrame, applies a cleaning operation using a UDF. If a cell contains an empty string after
    trimming, it is replaced with `None`.

    :param df: The input DataFrame to be cleaned. Must be a valid PySpark DataFrame.
    :type df: DataFrame
    :param columns: A list of string column names to be cleaned, where each column should exist in
        the provided DataFrame. Columns not in the DataFrame are ignored.
    :type columns: List[str]
    :return: A DataFrame with the specified columns cleaned. Unspecified or missing columns are left
        unchanged.
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
    Validates specific integer columns in a DataFrame based on given rules. The validation checks can include whether a column is
    required, and if its values fall within specified minimum and maximum thresholds. Any validation errors are aggregated into a
    new column named '_validation_errors'.

    :param df: The input DataFrame to validate.
    :type df: DataFrame
    :param columns: A list of column names to validate.
    :type columns: List[str]
    :param rules: A dictionary where keys are column names and values are dictionaries specifying validation rules. Rules include:
                  - 'required' (bool): Indicates if the column must not contain null values.
                  - 'min' (int, optional): Specifies the minimum permissible value for the column.
                  - 'max' (int, optional): Specifies the maximum permissible value for the column.
    :type rules: dict
    :return: A DataFrame with validation errors aggregated in a '_validation_errors' column if any issues are detected.
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
    Validates timestamp columns in a Spark DataFrame based on specified rules and
    adds a validation error column if any rules are violated.

    :param df: Spark DataFrame to be validated.
    :type df: DataFrame
    :param columns: List of column names to apply validation rules on.
    :type columns: List[str]
    :param rules: Dictionary mapping column names to validation rules. Each column's rules can include
        - `required` (bool): Indicates if the column must not have null values.
        - `min_date` (str): Specifies the minimum allowable timestamp in ISO-8601 format.
    :type rules: dict
    :return: Spark DataFrame with an additional '_validation_errors' column containing error messages for
        violations, or no additional column if no violations are found.
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


def validate_boolean_df(df: DataFrame, columns: List[str], rules: dict) -> DataFrame:
    """
    Validates columns in a DataFrame based on user-defined rules. Each column's rules dictate whether it
    is required to have non-null values or not. If any of the specified columns fail the validation, the
    function appends a new column '_validation_errors' to the DataFrame, containing error messages.

    :param df: Input DataFrame to be validated.
    :type df: DataFrame
    :param columns: List of column names in the DataFrame to validate against the rules.
    :type columns: List[str]
    :param rules: Dictionary where the key is the column name, and the value is another dictionary
        specifying validation rules for that column. The rules dictionary can contain a 'required' key
        with a boolean value indicating whether the column must not have null values.
    :type rules: dict
    :return: A DataFrame with an additional '_validation_errors' column if validation errors are found
        for the specified columns; otherwise, the input DataFrame is returned unchanged.
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
    if error_cols:
        df = df.withColumn('_validation_errors', F.concat_ws('; ', *error_cols))
    return df


def validate_string_df(df: DataFrame, columns: List[str], rules: dict) -> DataFrame:
    """
    Validates a DataFrame's string columns based on specified rules. This function checks the
    columns identified in the `columns` parameter against corresponding validation rules in
    the `rules` dictionary. Supported validations include checking for required values and
    enforcing specific string lengths. If validation errors are found, a new column
    `_validation_errors` is added to the DataFrame with error messages.

    :param df: The DataFrame to validate.
    :type df: DataFrame
    :param columns: The list of column names to validate in the DataFrame.
    :type columns: List[str]
    :param rules: A dictionary where the keys correspond to column names and the values
        define validation rules for those columns. Supported rules are:
        - 'required' (bool): Whether the column value is mandatory.
        - 'length' (int): The required length of the string value.
    :type rules: dict
    :return: A DataFrame with validation errors (if any) stored in the `_validation_errors` column.
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
            if col_rules.get('length'):
                error_cols.append(
                    F.when(F.length(F.col(col_name)) != col_rules['length'],
                           F.lit(f'{col_name} must be {col_rules["length"]} characters long')))
    if error_cols:
        df = df.withColumn('_validation_errors', F.concat_ws('; ', *error_cols))
    return df


def validate_decimal_df(df: DataFrame, columns: List[str], rules: dict) -> DataFrame:
    """
    Validates specified columns in a DataFrame against defined rules for decimal constraints.

    This function checks a set of rules on specified columns in a given DataFrame. The rules may
    include mandatory presence of values, maximum allowable values, and minimum allowable values.
    Validation errors are recorded in a new column '_validation_errors' for rows that fail validation.

    :param df: Input DataFrame to be validated.
    :type df: DataFrame
    :param columns: List of column names in the DataFrame to validate.
    :type columns: List[str]
    :param rules: A dictionary of validation rules for the columns. Each key is a column name, and its
                  value is another dictionary containing optional validation keys such as
                  'required', 'max_rate_value', and 'min_rate_value'.
                  - 'required': A boolean indicating whether the column values are mandatory.
                  - 'max_rate_value': A maximum permissible numeric value for the column.
                  - 'min_rate_value': A minimum permissible numeric value for the column.
    :type rules: dict
    :return: A DataFrame with validation errors recorded in a new column '_validation_errors', if any.
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
            if col_rules.get('max_rate_value'):
                error_cols.append(F.when(F.col(col_name) > col_rules['max_rate_value'],
                                         F.lit(f'{col_name} must be less than {col_rules["max_rate_value"]}')))
            if col_rules.get('min_rate_value'):
                error_cols.append(F.when(F.col(col_name) < col_rules['min_rate_value'],
                                         F.lit(f'{col_name} must be more than {col_rules["min_rate_value"]}')))
    if error_cols:
        df = df.withColumn('_validation_errors', F.concat_ws('; ', *error_cols))
    return df
