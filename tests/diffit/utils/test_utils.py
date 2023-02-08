""":mod:`diffit.utils` unit test cases.

"""
from typing import List, Text, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
import pytest

import diffit.utils


def test_dataframe_to_dict(spark: SparkSession) -> None:
    """Test DataFrame to JSON dump."""
    # Given an iterable of Spark SQL DataFrames
    df1 = spark.createDataFrame([[1]], ["a"])
    df2 = spark.createDataFrame([[2]], ["a"])
    df_iter = [df1, df2]

    # when I convert to a JSON construct
    received: List[dict] = list(diffit.utils.dataframe_to_dict(df_iter))

    # then the resulting JSON should match
    expected = [{"a": 1}, {"a": 2}]
    msg = "Dictionary construct from Spark SQL DataFrame error"
    assert received == expected, msg


@pytest.mark.parametrize("dummy_count", [2])
def test_merge_dataframe_with_empty_target_dataframe(
    spark: SparkSession, dummy: DataFrame
) -> None:
    """Test merge DataFrames with different columns."""
    # Given a Spark SQL DataFrame
    # dummy

    # when I merge the Spark SQL DataFrame into an empty DataFrame
    received: DataFrame = diffit.utils.merge_dataframe(
        spark.createDataFrame([], StructType([])), dummy
    )

    # then I should receive a newly structured Spark SQL DataFrame
    expected: List[List[Union[int, Text]]] = [
        [1, "dummy_col02_val0000000001"],
        [2, "dummy_col02_val0000000002"],
    ]
    msg = "Merged Spark SQL DataFrame value error"
    assert [
        list(row) for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg

    # and all columns should be present
    msg = "Merge Spark SQL is missing columns"
    assert sorted(received.columns) == ["dummy_col01", "dummy_col02"], msg


@pytest.mark.parametrize("dummy_count", [2])
def test_merge_dataframe_with_like_schema_target_dataframe(
    dummy: DataFrame, test_schema: StructType
) -> None:
    """Test merge DataFrames with different columns."""
    # Given a Spark SQL DataFrame
    # dummy

    # and another Spark SQL DataFrame with same schema
    dummy_dup = dummy.rdd.zipWithIndex().map(lambda l: list(l[0])).toDF(test_schema)

    # when I merge the Spark SQL DataFrame into the like-schema DataFrame
    received: DataFrame = diffit.utils.merge_dataframe(dummy, dummy_dup)

    # then I should receive a newly structured Spark SQL DataFrame
    expected: List[List[Union[int, Text]]] = [
        [1, "dummy_col02_val0000000001"],
        [1, "dummy_col02_val0000000001"],
        [2, "dummy_col02_val0000000002"],
        [2, "dummy_col02_val0000000002"],
    ]
    msg = "Merged Spark SQL DataFrame value error"
    assert [
        list(row) for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg

    # and all columns should be present
    msg = "Merge Spark SQL is missing columns"
    assert sorted(received.columns) == ["dummy_col01", "dummy_col02"], msg
