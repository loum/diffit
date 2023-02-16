"""`diffit.reporter.rangefilter` unit test cases.

"""
from typing import Optional, cast
import pytest

from pyspark.sql import Column, DataFrame

from diffit.reporter.rangefilter import RangeFilter


# Tuple elements:
# 1. RangeFilter
# 2. [thresholds_are_valid_result]

# Test scenarios:
# 1. No range thresholds provided: False
# 2. Upper threshold provided: True
# 3. Lower threshold provided: True
# 4. Upper and lower thresholds provided: True
# 5. Upper and lower thresholds the same: True
RANGE_FILTER_ARGS = (
    [RangeFilter(column="test"), False],
    [RangeFilter(column="test", lower=0), True],
    [RangeFilter(column="test", upper=100), True],
    [RangeFilter(column="test", lower=0, upper=100), True],
    [RangeFilter(column="test", lower=100, upper=100), True],
)


@pytest.mark.parametrize("range_filter,threshold_status", RANGE_FILTER_ARGS)
def test_thresholds_are_valid(
    range_filter: RangeFilter, threshold_status: bool
) -> None:
    """Check if the threshold rules are workable."""
    # Given a set of range thresholds
    # range_filter

    # when I validate the thresholds
    msg = "Range filter threshold error"
    assert range_filter.thresholds_are_valid == threshold_status, msg


@pytest.mark.parametrize("dummy_extra_count", [9])
def test_is_supported_range_condition_types_integertype(dummy_extra: DataFrame) -> None:
    """Test is_supported_range_condition_types: pyspark.sql.types.IntegerType."""
    # Given a Spark SQL DataFrame
    # dummy_extra

    # and a column name that exists in the Spark SQL DataFrame
    column_name = "dummy_col01"

    # when I check if the Spark SQL Column IntegerType is supported
    received: bool = RangeFilter.is_supported_range_condition_types(
        dummy_extra.schema[column_name]
    )

    # then the return value should be True
    assert received, "IntegerType columns should be supported"


@pytest.mark.parametrize("dummy_extra_count", [9])
def test_is_supported_range_condition_types_not_supported(
    dummy_extra: DataFrame,
) -> None:
    """Test is_supported_range_condition_types: not supported."""
    # Given a Spark SQL DataFrame
    # dummy_extra

    # and a column name that exists in the Spark SQL DataFrame
    # NOTE: StringTypes currently not supported -- but that may change ...
    column_name = "dummy_col02"

    # when I check if the Spark SQL Column StringType is supported
    received: bool = RangeFilter.is_supported_range_condition_types(
        dummy_extra.schema[column_name]
    )

    # then the return value should be False
    assert not received, "StringType columns should not be supported"


@pytest.mark.parametrize("dummy_extra_count", [9])
def test_range_filter_clause(dummy_extra: DataFrame) -> None:
    """Test range_filter clause."""
    # Given a Spark SQL DataFrame
    # dummy_extra

    # and a column name that exists in the Spark SQL DataFrame
    column_name = "dummy_col01"

    # and an inclusive lower boundary bound
    lower = 2

    # and an inclusive upper boundary bound
    upper = 5

    # when I set up the range filter clause
    range_filter = RangeFilter(column=column_name, lower=lower, upper=upper)
    condition: Optional[Column] = range_filter.range_filter_clause(dummy_extra.schema)

    # and filter the original Spark SQL DataFrame
    received: DataFrame = dummy_extra.filter(cast(Column, condition)).select(
        column_name
    )

    # then I should get a slice of the original Spark SQL DataFrame
    msg = "Range filtered Spark SQL DataFrame slice error"
    expected = [2, 3, 4, 5]
    assert [
        row.dummy_col01 for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg


@pytest.mark.parametrize("dummy_extra_count", [9])
def test_range_filter_clause_lower_only(dummy_extra: DataFrame) -> None:
    """Test range_filter clause: lower boundary only."""
    # Given a Spark SQL DataFrame
    # dummy_extra

    # and a column name that exists in the Spark SQL DataFrame
    column_name = "dummy_col01"

    # and an inclusive lower boundary bound
    lower = 8

    # and an undefined inclusive upper boundary bound
    upper = None

    # when I set up the range filter clause
    range_filter = RangeFilter(column=column_name, lower=lower, upper=upper)
    condition = range_filter.range_filter_clause(dummy_extra.schema)

    # and filter the original Spark SQL DataFrame
    received: DataFrame = dummy_extra.filter(cast(Column, condition)).select(
        column_name
    )

    # then I should get a slice of the original Spark SQL DataFrame
    msg = "Range filtered Spark SQL DataFrame slice error: lower boundary only"
    expected = [8, 9]
    assert [
        row.dummy_col01 for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg


@pytest.mark.parametrize("dummy_extra_count", [9])
def test_range_filter_clause_upper_only(dummy_extra: DataFrame) -> None:
    """Test range_filter clause: upper boundary only."""
    # Given a Spark SQL DataFrame
    # dummy_extra

    # and a column name that exists in the Spark SQL DataFrame
    column_name = "dummy_col01"

    # and an undefined inclusive lower boundary bound
    lower = None

    # and an inclusive upper boundary bound
    upper = 2

    # when I set up the range filter clause
    range_filter = RangeFilter(column=column_name, lower=lower, upper=upper)
    condition = range_filter.range_filter_clause(dummy_extra.schema)

    # and filter the original Spark SQL DataFrame
    received: DataFrame = dummy_extra.filter(cast(Column, condition)).select(
        column_name
    )

    # then I should get a slice of the original Spark SQL DataFrame
    msg = "Range filtered Spark SQL DataFrame slice error: lower boundary only"
    expected = [1, 2]
    assert [
        row.dummy_col01 for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg
