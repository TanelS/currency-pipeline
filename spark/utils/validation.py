import html
import re
import unicodedata
from typing import List, Optional, Union, Dict

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, TimestampType

CARRIAGE_RETURN_PATTERN = re.compile(r"\r+")
WHITESPACE_PATTERN = re.compile(r"[\n\t]+")
ZERO_WIDTH_PATTERN = re.compile(r"[​-‏‪-‮﻿]")
MULTIPLE_SPACES_PATTERN = re.compile(r"\s{2,}")
CONTROL_CHARS_PATTERN = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]")


def clean_string(s: Union[str, int, float, None]) -> Union[str, int, float, None]:
    """Normalizes a string: unescapes HTML, strips control chars, collapses whitespace; returns None for empty input."""
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

# User Defined Function
clean_string_udf = udf(clean_string, StringType())  # slower than native functions when dealing with large datasets


def build_error_column(col_name: str, rules: dict) -> Optional[Column]:
    """Builds a single comma-joined error Column for col_name covering all rule violations; returns None if no rules apply."""
    errors = []
    if rules.get('required'):
        errors.append(F.when(F.col(col_name).isNull(), F.lit(f'{col_name} is required')))
    if rules.get('min') is not None:
        errors.append(F.when(F.col(col_name) < rules['min'], F.lit(f'{col_name} below min {rules["min"]}')))
    if rules.get('max') is not None:
        errors.append(F.when(F.col(col_name) > rules['max'], F.lit(f'{col_name} above max {rules["max"]}')))
    if rules.get('min_date'):
        errors.append(
            F.when(
                F.col(col_name) < F.lit(rules['min_date']).cast(TimestampType()),
                F.lit(f'{col_name} must be later than "{rules["min_date"]}"'),
            )
        )
    if rules.get('max_rate_value') is not None:
        errors.append(F.when(F.col(col_name) > rules['max_rate_value'],
                             F.lit(f'{col_name} must be less than {rules["max_rate_value"]}')))
    if rules.get('min_rate_value') is not None:
        errors.append(F.when(F.col(col_name) < rules['min_rate_value'],
                             F.lit(f'{col_name} must be more than {rules["min_rate_value"]}')))
    if rules.get('length'):
        errors.append(
            F.when(F.length(F.col(col_name)) != rules['length'],
                   F.lit(f'{col_name} must be {rules["length"]} characters long'))
        )
    return F.concat_ws(', ', *errors) if errors else None


def _append_errors(df: DataFrame, error_cols: list) -> DataFrame:
    """Merges new error expressions into _validation_errors without overwriting errors from prior validate calls."""
    if not error_cols:
        return df
    # Each error_col is the result of build_error_column: F.concat_ws(', ', *whens).
    # For a valid row all when() arms are null, so concat_ws returns "" (not null).
    # Wrapping with F.when(c != '', c) converts those empty strings back to null so
    # the outer concat_ws does not produce "; ; ; ; ; ;" artefacts.
    non_empty = [F.when(c != '', c) for c in error_cols]
    new_errors = F.concat_ws('; ', *non_empty)
    if '_validation_errors' in df.columns:
        existing = F.when(F.col('_validation_errors') != '', F.col('_validation_errors'))
        return df.withColumn('_validation_errors', F.concat_ws('; ', existing, F.when(new_errors != '', new_errors)))
    return df.withColumn('_validation_errors', new_errors)


def clean_string_df(df: DataFrame, columns: List[str]) -> DataFrame:
    """Applies clean_string_udf to each named string column; replaces empty strings with None."""
    if df.count() == 0:
        return df

    cleaned_df = df
    for col_name in columns:
        if col_name in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", None)
                .otherwise(clean_string_udf(F.col(col_name)))  # run through user-defined function
            )
    return cleaned_df


def validate_df(df: DataFrame, columns: List[str], rules: Dict[str, dict]) -> DataFrame:
    """Validates columns against configured rules; accumulates into _validation_errors."""
    error_cols = []
    for col_name in columns:
        if col_name in df.columns:
            col_rules = rules.get(col_name)
            if col_rules:
                result = build_error_column(col_name, dict(col_rules))
                if result is not None:
                    error_cols.append(result)
    return _append_errors(df, error_cols)
