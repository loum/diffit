"""Diffit :mod:`diffit.reporter`.
"""
import logging
from typing import Iterable, List, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count
import pyspark.sql.column
import pyspark.sql.types

import diffit


def row_level(left: DataFrame,
              right: DataFrame,
              columns_to_drop: Optional[List[str]] = None,
              range_filter: Optional[dict] = None) -> DataFrame:
    """Wrapper function to report on differences between *left* and *right*
    Spark SQL DataFrames.

    *columns_to_drop* is a optinal list of columns that can be omitted from the diffit check.

    """
    if columns_to_drop is None:
        columns_to_drop = []

    left = left.drop(*columns_to_drop)
    right = right.drop(*columns_to_drop)

    if range_filter is not None:
        if (range_filter.get('column') is not None
                and range_filter.get('column') in left.columns):
            condition = range_filter_clause(left.schema,
                                            range_filter.get('column'),
                                            range_filter.get('lower'),
                                            range_filter.get('upper'),
                                            range_filter.get('force'))
            left = left.filter(condition)
            right = right.filter(condition)

    logging.info('Starting diff report')
    symmetric = diffit.symmetric_level(left, right)

    return diffit.symmetric_filter(left, symmetric)\
        .union(diffit.symmetric_filter(right, symmetric, orientation='right'))


def range_filter_clause(df_schema: pyspark.sql.types.StructType,
                        column: str,
                        lower: Union[None, int, str],
                        upper: Union[None, int, str],
                        force: Union[bool] = False) -> pyspark.sql.column.Column:
    """Set up range search clause to filter.

    Checks if the provided column is supported as a range condition in the clause.

    """
    condition = None

    if force or is_supported_range_condition_types(df_schema[column]):
        # Only supporting numerics at this time.
        if force or is_numeric(df_schema[column]):
            if lower is not None and upper is None:
                condition = (col(column) >= int(lower))
            elif lower is None and upper is not None:
                condition = (col(column) <= int(upper))
            elif lower is not None and upper is not None:
                condition = ((col(column) >= int(lower)) & (col(column) <= int(upper)))

    return condition


def is_supported_range_condition_types(column: pyspark.sql.types.StructField) -> bool:
    """Closure to maintain list of supported range types.

    Add supported types to this list as capability evolves
    to support Spark SQL range condition filtering.

    """
    def check():
        # Only supporting numerics at this time.
        return is_numeric(column)

    return check()


def is_numeric(column: pyspark.sql.types.StructField) -> bool:
    """Closure to check for numeric types.

    """
    def check():
        return isinstance(column.dataType,
                          (pyspark.sql.types.IntegerType, pyspark.sql.types.LongType))

    return check()


def distinct_rows(diff: DataFrame, column_key: str, diffit_ref: str = 'left') -> DataFrame:
    """Return a DataFrame of unique rows relative to *diff*.

    Works on a Differ output DataFrame.

    """
    return diff\
        .filter(diff[column_key]\
            .isin(grouped_rows(diff, column_key)\
            .rdd.flatMap(lambda x: x)\
            .collect()))\
        .filter(diff.diffit_ref == diffit_ref)


def altered_rows(diff: DataFrame,
                 column_key: str,
                 range_filter: Optional[dict] = None) -> DataFrame:
    """Return a DataFrame of altered rows relative to *diff*.

    Works on a Differ output DataFrame.

    """
    if range_filter is not None:
        if (range_filter.get('column') is not None
                and range_filter.get('column') in diff.columns):
            condition = range_filter_clause(diff.schema,
                                            range_filter.get('column'),
                                            range_filter.get('lower'),
                                            range_filter.get('upper'),
                                            range_filter.get('force'))
            diff = diff.filter(condition)

    return diff\
        .filter(diff[column_key]\
            .isin(grouped_rows(diff, column_key, group_count=2)\
            .rdd.flatMap(lambda x: x)\
            .collect()))


def grouped_rows(diff: DataFrame, column_key: str, group_count: int = 1) -> DataFrame:
    """Return a DataFrame of grouped rows from DataFrame *diff* where column *column_key*
    acts as the unique constraint.

    """
    return diff\
        .groupBy(column_key)\
        .agg(count(column_key).alias('count'))\
        .filter(col('count') == group_count)\
        .select(column_key)


def altered_rows_column_diffs(diff: DataFrame,
                              column_key: str,
                              key_val: str) -> Iterable[dict]:
    """Helper function that creates a new, reduced DataFrame from the Differ output *diff*
    and captures only the columns that are different.  Column value differences
    are reported as a Python dictionary.

    *column_key* provides unique constraint behaviour while its value *key_val* filters
    a targeted row set.

    """
    if key_val is not None:
        diff = diff.filter(col(column_key) == key_val)

    col_diffs = altered_rows(diff, column_key)

    left = col_diffs.filter(col('diffit_ref') == 'left').drop(col('diffit_ref'))
    right = col_diffs.filter(col('diffit_ref') == 'right').drop(col('diffit_ref'))

    return diffit.column_level_diff(left, right)
