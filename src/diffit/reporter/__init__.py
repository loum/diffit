"""Diffit :mod:`diffit.reporter`.
"""
from typing import Any, Dict, Iterable, List, Optional, Text, Union

from logga import log
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, count
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

import diffit


def row_level(
    left: DataFrame,
    right: DataFrame,
    columns_to_drop: Optional[List[Text]] = None,
    range_filter: Optional[Dict[Text, Any]] = None,
) -> DataFrame:
    """Wrapper function to report on differences between *left* and *right*
    Spark SQL DataFrames.

    *columns_to_drop* is a optinal list of columns that can be omitted from the diffit check.

    """
    if columns_to_drop is None:
        columns_to_drop = []

    left = left.drop(*columns_to_drop)
    right = right.drop(*columns_to_drop)

    if range_filter is None:
        range_filter = {}

    column: Text = range_filter.get("column", "")
    if column and column in left.columns:
        condition: Optional[Column] = range_filter_clause(
            left.schema,
            column,
            range_filter.get("lower"),
            range_filter.get("upper"),
            range_filter.get("force", False),
        )

        if condition is not None:
            left = left.filter(condition)
            right = right.filter(condition)

    log.info("Starting diff report")
    symmetric = diffit.symmetric_level(left, right)

    return diffit.symmetric_filter(left, symmetric).union(
        diffit.symmetric_filter(right, symmetric, orientation="right")
    )


def range_filter_clause(
    df_schema: StructType,
    column: Text,
    lower: Union[None, int, Text],
    upper: Union[None, int, Text],
    force: bool = False,
) -> Optional[Column]:
    """Set up range search clause to filter.

    Checks if the provided column is supported as a range condition in the clause.

    """
    condition: Optional[Column] = None

    if force or is_supported_range_condition_types(df_schema[column]):
        # Only supporting numerics at this time.
        if force or is_numeric(df_schema[column]):
            if lower is not None and upper is None:
                condition = col(column) >= int(lower)
            elif lower is None and upper is not None:
                condition = col(column) <= int(upper)
            elif lower is not None and upper is not None:
                condition = (col(column) >= int(lower)) & (col(column) <= int(upper))

    return condition


def is_supported_range_condition_types(column: StructField) -> bool:
    """Closure to maintain list of supported range types.

    Add supported types to this list as capability evolves
    to support Spark SQL range condition filtering.

    """

    def check() -> bool:
        # Only supporting numerics at this time.
        return is_numeric(column)

    return check()


def is_numeric(column: StructField) -> bool:
    """Closure to check for numeric types."""

    def check() -> bool:
        return isinstance(column.dataType, (IntegerType, LongType))

    return check()


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
    diff: DataFrame, column_key: Text, range_filter: Optional[Dict] = None
) -> DataFrame:
    """Return a DataFrame of altered rows relative to *diff*.

    Works on a Differ output DataFrame.

    Returns:
        DataFrame of rows that different.

    """
    if range_filter is None:
        range_filter = {}

    column: Text = range_filter.get("column", "")
    if column and column in diff.columns:
        condition: Optional[Column] = range_filter_clause(
            diff.schema,
            column,
            range_filter.get("lower"),
            range_filter.get("upper"),
            range_filter.get("force", False),
        )

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
        .agg(count(column_key).alias("count"))
        .filter(col("count") == group_count)
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
        diff = diff.filter(col(column_key) == key_val)

    col_diffs = altered_rows(diff, column_key)

    left = col_diffs.filter(col("diffit_ref") == "left").drop(col("diffit_ref"))
    right = col_diffs.filter(col("diffit_ref") == "right").drop(col("diffit_ref"))

    return diffit.column_level_diff(left, right)
