"""Unit test cases for :mod:`conftest.py`.
"""
import pyspark


def test_spark_context(spark_context):
    """Load the Spark context fixture.
    """
    # Given an environment that provides a Spark context
    # spark_context

    # then I should have a pyspark.SparkContext instance
    msg = 'conftest does not provide us a pyspark.SparkContext instance'
    assert isinstance(spark_context, pyspark.SparkContext), msg

    # and the default "spark.driver.memory" should be "1g"
    msg = 'pyspark.SparkContext "spark.driver.memory" setting error'
    # pylint: disable=protected-access
    assert spark_context._conf.get('spark.driver.memory') == '1g', msg
    # pylint: enable=protected-access


def test_spark_session(spark):
    """Access the SparkSession
    """
    # Given a SparkSession
    # spark

    # when I check the PySpark version
    received = spark.version

    # then the version number should the currently supported value
    msg = 'Supported SparkSession version error'
    assert received.startswith('3.3'), msg


def test_working_dir(working_dir):
    """Test the "working_dir" fixture.
    """
    msg = 'conftest "working_dir" fixture should provide string type'
    assert isinstance(working_dir, str), msg
