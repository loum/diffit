"""Unit test cases for :mod:`conftest.py`.
"""
from typing import Text

import pyspark.sql


def test_spark_session(spark: pyspark.sql.SparkSession):
    """Access the SparkSession
    """
    # Given a SparkSession
    # spark

    # when I check the PySpark version
    received = spark.version

    # then the version number should the currently supported value
    msg = 'Supported SparkSession version error'
    assert received.startswith('3'), msg


def test_working_dir(working_dir: Text):
    """Test the "working_dir" fixture.
    """
    msg = 'conftest "working_dir" fixture should provide string type'
    assert isinstance(working_dir, str), msg
