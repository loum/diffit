"""PyTest plugin that defines sample Spark SQL DataFrames.

"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
import pytest

import diffit.data.factory.dummy


DATA_SCHEMA = {
    'type': 'struct',
    'fields': [
        {
            'name' : 'dummy_col01',
            'type' : 'integer',
            'nullable': True,
            'metadata': {}
        },
        {
            'name': 'dummy_col02',
            'type': 'string',
            'nullable': True,
            'metadata': {}
        }
    ]
}
DATA_STRUCT = StructType().fromJson(DATA_SCHEMA)

DIFFIT_SCHEMA = {
    'type': 'struct',
    'fields': [
        {
            'name' : 'dummy_col01',
            'type' : 'integer',
            'nullable': True,
            'metadata': {}
        },
        {
            'name': 'dummy_col02',
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
DIFFIT_STRUCT = StructType().fromJson(DIFFIT_SCHEMA)

SYMMETRIC_DATA = [
    (1, 'dummy_col02_val0000000001'),
    (1, 'dummy_col02_val0000000002'),
    (2, 'dummy_col02_val0000000002'),
    (2, 'dummy_col02_val0000000003'),
]

DIFFIT_DATA = [
    (2, 'dummy_col02_val02', 'left'),
    (2, 'dummy_col02_valXX', 'right'),
    (3, 'dummy_col03_val03', 'left'),
]


@pytest.fixture
def symmetric_data_df(spark: SparkSession):
    """Sample DataFrame with symmetric output data.
    """
    return spark.createDataFrame(data=SYMMETRIC_DATA, schema=DATA_STRUCT)


@pytest.fixture
def diffit_data_df(spark: SparkSession):
    """Sample DataFrame with Differ output data.
    """
    return spark.createDataFrame(data=DIFFIT_DATA, schema=DIFFIT_STRUCT)


@pytest.fixture
def test_schema() -> StructType:
    """The sample DataFrame schema.
    """
    return DATA_STRUCT


@pytest.fixture()
def dummy(spark: SparkSession, dummy_count: int) -> DataFrame:
    """Sample Dummy DataFrame.

    """
    _factory = diffit.data.factory.dummy.Data(dummy_count)

    return spark.createDataFrame(*(_factory.args()))


@pytest.fixture()
def dummy_skewed(spark: SparkSession, dummy_skewed_count: int) -> DataFrame:
    """Sample Dummy DataFrame with altered values.

    """
    _factory = diffit.data.factory.dummy.Data(dummy_skewed_count, skew=True)

    return spark.createDataFrame(*(_factory.args()))


@pytest.fixture()
def dummy_extra(spark: SparkSession, dummy_extra_count: int) -> DataFrame:
    """Sample Dummy DataFrame with extra rows.

    """
    _factory = diffit.data.factory.dummy.Data(dummy_extra_count)

    return spark.createDataFrame(*(_factory.args()))
