"""Generic, file-based utilities and helpers.

"""
import os
from collections import deque
from typing import Union
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType


def spark_csv_reader(spark: SparkSession, schema: StructType, csv_path: str) -> DataFrame:
    """Spark CSV reader.

    """
    return spark.read\
        .schema(schema)\
        .option('delimiter', ';')\
        .option('ignoreTrailingWhiteSpace', 'true')\
        .option('ignoreLeadingWhiteSpace', 'true')\
        .option('header', 'false')\
        .option('emptyValue', '')\
        .option('quote', '\u0000')\
        .csv(csv_path)


def spark_parquet_reader(spark: SparkSession, parquet_path: str) -> DataFrame:
    """Spark Parquet reader.

    """
    return spark.read.parquet(parquet_path)


def split_dir(directory_path: str, directory_token: str) -> Union[str, None]:
    """Helper function to strip leading directory parts from *directory*
    until *directory_token* is matched.

    Returns the remaining parts of *directory_path* as a string.  For example::

    """
    directory_parts = deque(directory_path.split(os.sep))

    new_path = None
    while directory_parts:
        if directory_parts.popleft() == directory_token:
            new_path = os.path.join(*directory_parts)
            break

    return new_path
