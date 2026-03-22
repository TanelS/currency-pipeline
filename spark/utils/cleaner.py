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

# Register the Python function as a UDF
clean_string_udf = udf(clean_string, StringType()) # <--- Register UDF with expected return type

def clean_df(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Cleans the specified columns in a DataFrame by trimming whitespace and replacing empty
    strings with null values. Optionally applies a user-defined function to clean the column
    values further.

    This function processes the specified columns in the given DataFrame as follows:
    1. If the DataFrame is empty (row count is zero), it returns the original DataFrame.
    2. For each column in the provided list of columns, if the column exists in the DataFrame:
       - Trims leading and trailing whitespace from the column values.
       - Replaces empty strings with null values.
       - Applies a user-defined function to clean the column values if applicable.

    Intended for use cases where handling of whitespace and empty strings in data values
    is required for predefined columns.

    :param df: The input DataFrame to be cleaned.
    :type df: DataFrame
    :param columns: The list of column names to be processed during cleaning.
    :type columns: List[str]
    :return: A DataFrame with specified columns cleaned by replacing empty strings with nulls,
             trimming whitespace, and applying additional cleaning logic.
    :rtype: DataFrame
    """
    if not df.count() == 0:
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

