"""Dummy data unit tests.

"""
from pyspark.sql import DataFrame
import pytest


@pytest.mark.parametrize("dummy_count", [2])
def test_data(dummy: DataFrame) -> None:
    """Dummy data validation."""
    # Given a Dummy Spark DataFrame
    # dummy

    # when I collect the DataFrame values
    received = dummy.orderBy("dummy_col01", "dummy_col02").collect()

    # then rows should match the expected values
    expected = [[1, "dummy_col02_val0000000001"], [2, "dummy_col02_val0000000002"]]
    msg = "Source Dummy DataFrame error"
    assert [list(row) for row in received] == expected, msg


@pytest.mark.parametrize("dummy_count", [10])
def test_data_count_create(dummy: DataFrame) -> None:
    """Dummy data validation: count create."""
    # Given a Dummy Spark DataFrame
    # dummy

    # when I collect the DataFrame values
    received = dummy.orderBy("dummy_col01", "dummy_col02").collect()

    # then rows should match the expected values
    expected = [
        [1, "dummy_col02_val0000000001"],
        [2, "dummy_col02_val0000000002"],
        [3, "dummy_col02_val0000000003"],
        [4, "dummy_col02_val0000000004"],
        [5, "dummy_col02_val0000000005"],
        [6, "dummy_col02_val0000000006"],
        [7, "dummy_col02_val0000000007"],
        [8, "dummy_col02_val0000000008"],
        [9, "dummy_col02_val0000000009"],
        [10, "dummy_col02_val0000000010"],
    ]
    msg = "Source Dummy DataFrame error for overridden row count creation"
    assert [list(row) for row in received] == expected, msg
