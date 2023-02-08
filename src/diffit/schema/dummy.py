"""Dummy schema `tests.schema.dummy`.

"""
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def schema() -> StructType:
    """Dummy Apache Spark DataFrame schema."""

    def definition() -> StructType:
        return StructType(
            [
                StructField("dummy_col01", IntegerType(), True),
                StructField("dummy_col02", StringType(), True),
            ]
        )

    return definition()
