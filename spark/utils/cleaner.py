import html
import re

import unicodedata

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from typing import List, Union, Optional

CARRIAGE_RETURN_PATTERN = re.compile(r"\r+")
WHITESPACE_PATTERN = re.compile(r"[\n\t]+")
ZERO_WIDTH_PATTERN = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")
MULTIPLE_SPACES_PATTERN = re.compile(r"\s{2,}")
CONTROL_CHARS_PATTERN = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]")


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


def validate_int_df(df: DataFrame, columns: List[str]) -> DataFrame:
    ...


def validate_timestamps_df(df: DataFrame, columns: List[str]) -> DataFrame:
    ...
