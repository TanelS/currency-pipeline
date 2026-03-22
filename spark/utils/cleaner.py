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
clean_string_udf = udf(clean_string, StringType()) # slower than native functions when dealing with large datasets

def clean_df(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Cleans the specified columns in a given DataFrame by applying string cleaning logic. Empty string values
    in the specified columns are replaced with `None`, while non-empty values are processed using a
    custom-defined string cleaning function.

    :param df: Input DataFrame to be cleaned.
    :type df: DataFrame
    :param columns: List of column names within the DataFrame that need to be cleaned.
    :type columns: List[str]
    :return: A DataFrame with the specified columns cleaned. If the DataFrame is empty, returns the
        original DataFrame.
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

