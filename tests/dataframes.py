"""PyTest pluging that defines sample Spark SQL DataFrames.

"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


DATA_SCHEMA = {
    'type': 'struct',
    'fields': [
        {
            'name' : 'col01',
            'type' : 'integer',
            'nullable': True,
            'metadata': {}
        },
        {
            'name': 'col02',
            'type': 'string',
            'nullable': True,
            'metadata': {}
        }
    ]
}
DATA_STRUCT = StructType().fromJson(DATA_SCHEMA)

DIFFER_SCHEMA = {
    'type': 'struct',
    'fields': [
        {
            'name' : 'col01',
            'type' : 'integer',
            'nullable': True,
            'metadata': {}
        },
        {
            'name': 'col02',
            'type': 'string',
            'nullable': True,
            'metadata': {}
        },
        {
            'name': 'diffit_ref',
            'type': 'string',
            'nullable': True,
            'metadata': {}
        }
    ]
}
DIFFER_STRUCT = StructType().fromJson(DIFFER_SCHEMA)

DATA = [
    (1, 'col02_val01'),
    (2, 'col02_val02'),
]

ALT_DATA = [
    (1, 'col02_val01'),
    (2, 'col02_valXX'),
]

EXTRA_DATA = [
    (3, 'col02_val03'),
]

MORE_EXTRA_DATA = [
    (4, 'col02_val04'),
    (5, 'col02_val05'),
    (6, 'col02_val06'),
    (7, 'col02_val07'),
    (8, 'col02_val08'),
    (9, 'col02_val09'),
]

SYMMETRIC_DATA = [
    (2, 'col02_val02'),
    (2, 'col02_valXX'),
]

DIFFER_DATA = [
    (2, 'col02_val02', 'left'),
    (2, 'col02_valXX', 'right'),
    (3, 'col03_val03', 'left'),
]


@pytest.fixture
def data_df(spark_context):
    """Sample DataFrame.
    """
    spark = SparkSession(spark_context)

    return spark.createDataFrame(data=DATA, schema=DATA_STRUCT)


@pytest.fixture
def alt_data_df(spark_context):
    """Sample DataFrame with alternate data (diffit trigger).
    """
    spark = SparkSession(spark_context)

    return spark.createDataFrame(data=ALT_DATA, schema=DATA_STRUCT)


@pytest.fixture
def extra_data_df(spark_context):
    """Sample DataFrame with extra data (extended diffit).
    """
    spark = SparkSession(spark_context)

    return spark.createDataFrame(data=EXTRA_DATA, schema=DATA_STRUCT)


@pytest.fixture
def big_data_df(spark_context):
    """Sample DataFrame with (more) extra data (extended diffit).
    """
    spark = SparkSession(spark_context)

    data = DATA[:]
    data.extend(EXTRA_DATA)
    data.extend(MORE_EXTRA_DATA)

    return spark.createDataFrame(data=data, schema=DATA_STRUCT)


@pytest.fixture
def symmetric_data_df(spark_context):
    """Sample DataFrame with symmetric output data.
    """
    spark = SparkSession(spark_context)

    return spark.createDataFrame(data=SYMMETRIC_DATA, schema=DATA_STRUCT)


@pytest.fixture
def diffit_data_df(spark_context):
    """Sample DataFrame with Differ output data.
    """
    spark = SparkSession(spark_context)

    return spark.createDataFrame(data=DIFFER_DATA, schema=DIFFER_STRUCT)


@pytest.fixture
def test_schema():
    """The sample DataFrame schema.
    """
    return DATA_SCHEMA
