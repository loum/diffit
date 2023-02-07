"""Compare two Spark SQL DataFrames.

"""
__app_name__ = "diffit"

from typing import Any, Dict, Iterable, List, Text, Optional

from logga import log
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

import diffit.utils


def right_subtract_row_level(left: DataFrame, right: DataFrame) -> DataFrame:
    """Compare two Spark SQL DataFrames denoted as *left* and *right*
    for differences at the row level.

    Returns a DataFrame with rows that are present in *left* dataframe but
    not present in the *right* dataframe

    """
    log.info("Starting row-level diff check")
    return left.select(left.columns).subtract(right.select(right.columns))


def symmetric_level(left: DataFrame, right: DataFrame) -> DataFrame:
    """Symmetric differences between two Spark DataFrames denoted as *left* and
    *right*.

    For context, the symmetric difference of two sets is the set of elements
    which are in either of the sets, but not in their intersection.  For example,
    the symmetric difference of the sets ``{1,2,3}`` and ``{3,4}`` is
    ``{1,2,4}``.

    """
    log.info("Starting symmetric diff check")
    return left.union(right).subtract(left.intersect(right))


def symmetric_filter(
    target: DataFrame, symmetric: DataFrame, orientation: str = "left"
) -> DataFrame:
    """Filter out the *symmetric* DataFrame rows that co-exist against the *target* DataFrame.

    *orientation* denotes the reference of the *source* DataFrame in the
    context of the :func:`symmetric` operation.

    """
    log.info("Starting report for %s DataFrame reference point", orientation)
    return target.intersect(symmetric).withColumn("diffit_ref", lit(orientation))


def column_level_diff(left: DataFrame, right: DataFrame) -> Iterable[Dict[Text, Any]]:
    """DataFrame column-level diff check."""

    def col_diff(col_name: str) -> DataFrame:
        return left.select(col(col_name)).subtract(right.select(col(col_name)))

    return diffit.utils.dataframe_to_dict(map(col_diff, left.columns))
