"""Generic, file-based utilities and helpers.

"""
import os
from collections import deque
from typing import Union
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType


def spark_csv_reader(spark: SparkSession,
                     schema: StructType,
                     csv_path: str,
                     delimiter: str = ',',
                     header: bool = True) -> DataFrame:
    """Spark CSV reader.

    Setting such as *delimiter* and *header* can be adjusted during the read.

    Returns a DataFrame representation of the CSV.

    """
    return spark.read\
        .schema(schema)\
        .option('delimiter', delimiter)\
        .option('ignoreTrailingWhiteSpace', 'true')\
        .option('ignoreLeadingWhiteSpace', 'true')\
        .option('header', header)\
        .option('emptyValue', None)\
        .option('quote', '"')\
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


def sanitise_columns(source: DataFrame, problematic_chars=',;{}()=') -> DataFrame:
    """As the diffit engine produces a parquet output, we may need
    to remove special characters from the source headers that do not
    align with the parquet conventions. The column sanitise step will:
    - convert to lower case
    - replace spaces with under-score
    - strip out the *problematic_char* set

    Returns a new DataFrame with adjusted columns

    """
    new_columns = []

    for column in source.columns:
        column = column.lower()
        column = column.replace(' ', '_')
        for _char in problematic_chars:
            column = column.replace(_char, '')
        new_columns.append(column)

    return source.toDF(*new_columns)
