""":mod:`diffit.utils` unit test cases.

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import diffit.utils


def test_dataframe_to_dict(spark_context):
    """Test DataFrame to JSON dump.
    """
    # Given an iterable of Spark SQL DataFrames
    spark = SparkSession(spark_context)
    df1 = spark.createDataFrame([[1]], ['a'])
    df2 = spark.createDataFrame([[2]], ['a'])
    df_iter = [df1, df2]

    # when I convert to a JSON construct
    received = list(diffit.utils.dataframe_to_dict(df_iter))

    # then the resulting JSON should match
    expected = [{'a': 1}, {'a': 2}]
    msg = 'Dictionary construct from Spark SQL DataFrame error'
    assert received == expected, msg


def test_merge_dataframe_with_empty_target_dataframe(spark_context, data_df):
    """Test merge DataFrames with different columns.
    """
    # Given a Spark SQL DataFrame
    # data_df

    # when I merge the Spark SQL DataFrame into an empty DataFrame
    spark = SparkSession(spark_context)
    received = diffit.utils.merge_dataframe(spark.createDataFrame([], StructType([])), data_df)

    # then I should receive a newly structured Spark SQL DataFrame
    expected = [[1, 'col02_val01'], [2, 'col02_val02']]
    msg = 'Merged Spark SQL DataFrame value error'
    assert [list(row) for row in received.orderBy('col01').collect()] == expected, msg

    # and all columns should be present
    expected = ['col01', 'col02']
    msg = 'Merge Spark SQL is missing columns'
    assert sorted(received.columns) == expected, msg


def test_merge_dataframe_with_like_schema_target_dataframe(data_df, test_schema):
    """Test merge DataFrames with different columns.
    """
    # Given a Spark SQL DataFrame
    # data_df

    # and another Spark SQL DataFrame with same schema
    schema = StructType().fromJson(test_schema)
    data_df_dup = data_df.rdd.zipWithIndex().map(lambda l: list(l[0])).toDF(schema)

    # when I merge the Spark SQL DataFrame into the like-schema DataFrame
    received = diffit.utils.merge_dataframe(data_df, data_df_dup)

    # then I should receive a newly structured Spark SQL DataFrame
    expected = [[1, 'col02_val01'], [1, 'col02_val01'], [2, 'col02_val02'], [2, 'col02_val02']]
    msg = 'Merged Spark SQL DataFrame value error'
    assert [list(row) for row in received.orderBy('col01').collect()] == expected, msg

    # and all columns should be present
    expected = ['col01', 'col02']
    msg = 'Merge Spark SQL is missing columns'
    assert sorted(received.columns) == expected, msg
