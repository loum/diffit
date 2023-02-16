"""Diffit :mod:`diffit.reporter`.
"""
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Text, Union

from logga import log
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import IntegerType, LongType, StructField, StructType
import pyspark.sql.functions as F

from diffit.reporter.rangefilter import RangeFilter
import diffit


def row_level(
    left: DataFrame,
    right: DataFrame,
    columns_to_drop: Optional[List[Text]] = None,
    range_filter: Optional[RangeFilter] = None,
) -> DataFrame:
    """Wrapper function to report on differences between *left* and *right*
    Spark SQL DataFrames.

    Parameters:
        left: Source DataFrame orientation.
        right: Source DataFrame orientation.
        columns_to_drop: List of columns that can be omitted from the diffit check.
        range_filter: Data structure that sets the thresholds for range filtering.

    Returns:
        Spark DataFrame of different rows between the source DataFrames under test.

    """
    if columns_to_drop is None:
        columns_to_drop = []

    left = left.drop(*columns_to_drop)
    right = right.drop(*columns_to_drop)

    if range_filter is not None and range_filter.column in left.columns:
        filter_clause: Optional[Column] = range_filter.range_filter_clause(left.schema)

        if filter_clause is not None:
            left = left.filter(filter_clause)
            right = right.filter(filter_clause)

    log.info("Starting diff report ...")
    symmetric = diffit.symmetric_level(left, right)

    return diffit.symmetric_filter(left, symmetric).union(
        diffit.symmetric_filter(right, symmetric, orientation="right")
    )


def distinct_rows(
    diff: DataFrame, column_key: Text, diffit_ref: Text = "left"
) -> DataFrame:
    """Return a DataFrame of unique rows relative to *diff*.

    Works on a Differ output DataFrame.

    """
    return diff.filter(
        diff[column_key].isin(
            grouped_rows(diff, column_key).rdd.flatMap(lambda x: x).collect()
        )
    ).filter(diff.diffit_ref == diffit_ref)


def altered_rows(
    diff: DataFrame,
    column_key: Text,
    range_filter: Optional[RangeFilter] = None,
) -> DataFrame:
    """Return a DataFrame of altered rows relative to *diff*.

    Works on a Differ output DataFrame.

    Returns:
        DataFrame of rows that different.

    """
    if range_filter is not None and range_filter.column in diff.columns:
        condition: Optional[Column] = range_filter.range_filter_clause(diff.schema)

        if condition is not None:
            diff = diff.filter(condition)

    return diff.filter(
        diff[column_key].isin(
            grouped_rows(diff, column_key, group_count=2)
            .rdd.flatMap(lambda x: x)
            .collect()
        )
    )


def grouped_rows(diff: DataFrame, column_key: Text, group_count: int = 1) -> DataFrame:
    """Return a DataFrame of grouped rows from DataFrame *diff* where column *column_key*
    acts as the unique constraint.

    """
    return (
        diff.groupBy(column_key)
        .agg(F.count(column_key).alias("count"))
        .filter(F.col("count") == group_count)
        .select(column_key)
    )


def altered_rows_column_diffs(
    diff: DataFrame, column_key: Text, key_val: Union[int, Text]
) -> Iterable[Dict]:
    """Helper function that creates a new, reduced DataFrame from the Differ output *diff*
    and captures only the columns that are different. Column value differences
    are reported as a Python dictionary.

    *column_key* provides unique constraint behaviour while its value *key_val* filters
    a targeted row set.

    """
    if key_val is not None:
        diff = diff.filter(F.col(column_key) == key_val)

    col_diffs = altered_rows(diff, column_key)

    left = col_diffs.filter(F.col("diffit_ref") == "left").drop(F.col("diffit_ref"))
    right = col_diffs.filter(F.col("diffit_ref") == "right").drop(F.col("diffit_ref"))

    return diffit.column_level_diff(left, right)
