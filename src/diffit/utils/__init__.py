"""Differ generic DataFrame utils.

"""
from typing import Dict, Iterable, List, Optional, cast
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import lit


def dataframe_to_dict(df_iter: Iterable[DataFrame]) -> Iterable[Dict]:
    """Take an iterable of DataFrames *df_iter* and convert to a
    iterable dictionary construct.

    """
    return map(
        lambda d: cast(Row, d.first()).asDict(), [x for x in df_iter if x.count() > 0]
    )


def merge_dataframe(left: DataFrame, right: DataFrame) -> DataFrame:
    """Merge two Spark SQL DataFrames into one."""
    for column in [column for column in right.columns if column not in left.columns]:
        left = left.withColumn(column, lit(None))

    for column in [column for column in left.columns if column not in right.columns]:
        right = right.withColumn(column, lit(None))

    return left.unionByName(right)
