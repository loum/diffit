"""Unit test cases for :mod:`conftest.py`.
"""
import pyspark


def test_spark_context(spark_context):
    """Load the Spark context fixture.
    """
    # Given an environment that provides a Spark context (spark_context)

    # then I should have a pyspark.SparkContext instance
    msg = 'conftest does not provide us a pyspark.SparkContext instance'
    assert isinstance(spark_context, pyspark.SparkContext), msg


def test_working_dir(working_dir):
    """Test the "working_dir" fixture.
    """
    msg = 'conftest "working_dir" fixture should provide string type'
    assert isinstance(working_dir, str), msg
