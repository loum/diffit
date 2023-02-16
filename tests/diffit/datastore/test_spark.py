"""`diffit.datastore.spark` unit test cases.

"""
from typing import Text
import os

from pyspark.sql import DataFrame, SparkSession
import pytest

import diffit.datastore.spark


def test_parquet_reader(working_dir: Text, spark: SparkSession) -> None:
    """Read in a custom Parquet file."""
    # Given a SparkSession
    # spark

    # and a path to parquet data
    data = [
        ("James", "", "Smith", "36636", "M", 3000),
        ("Michael", "Rose", "", "40288", "M", 4000),
        ("Robert", "", "Williams", "42114", "M", 4000),
        ("Maria", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1),
    ]
    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    _df = spark.createDataFrame(data, columns)
    parquet_path = os.path.join(working_dir, "parquet.out")
    _df.write.parquet(parquet_path)

    # when I read into a Spark SQL DataFrame
    received = diffit.datastore.spark.parquet_reader(spark, parquet_path)

    # then I should receive content
    msg = "Parquet Spark SQL reader DataFrame error"
    assert received.count() == 5, msg


@pytest.mark.parametrize("dummy_count", [10000])
def test_parquet_write_read(
    working_dir: Text, spark: SparkSession, dummy: DataFrame
) -> None:
    """Write and then read back in a custom Parquet file."""
    # Given a large, Dummy Spark DataFrame
    # dummy

    # and a target output/source path
    # working_dir

    # when I write out the Dummy Spark DataFrame as Spark Parquet
    diffit.datastore.spark.parquet_writer(dummy, working_dir)

    # and then read back in
    received = diffit.datastore.spark.parquet_reader(spark, working_dir)

    # then I should get a matching count
    expected = 10000
    msg = "Large, source Dummy DataFrame error for overridden row count creation"
    assert received.count() == expected, msg
