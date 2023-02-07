"""`diffit.reporter` unit test cases.

"""
from typing import Any, Dict, Iterable, List, Optional, Text, cast

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
import pytest

import diffit.reporter


@pytest.mark.parametrize("dummy_count", [2])
def test_report_against_like_dataframes(dummy: DataFrame) -> None:
    """Test a diffit report output."""
    # Given a set of like Spark SQL DataFrame schemas with different values
    # data_df for both left and right

    # when I report on differences
    received: DataFrame = diffit.reporter.row_level(left=dummy, right=dummy)

    # then I should receive an empty list
    msg = "DataFrame diffit report produced incorrect results"
    assert not [list(row) for row in received.collect()], msg


@pytest.mark.parametrize("dummy_extra_count,dummy_skewed_count", [(3, 2)])
def test_report_against_differing_dataframes(
    dummy_extra: DataFrame, dummy_skewed: DataFrame
) -> None:
    """Test a diffit report output."""
    # Given a set of like Spark SQL DataFrame schemas with different values
    # dummy_extra for the left and dummy for the right

    # when I report on differences
    received: DataFrame = diffit.reporter.row_level(
        left=dummy_extra, right=dummy_skewed
    ).sort(F.col("dummy_col01"), F.col("dummy_col02"))

    # then I should recieve the different rows across both source DataFrames
    expected = [
        [1, "dummy_col02_val0000000001", "left"],
        [1, "dummy_col02_val0000000002", "right"],
        [2, "dummy_col02_val0000000002", "left"],
        [2, "dummy_col02_val0000000003", "right"],
        [3, "dummy_col02_val0000000003", "left"],
    ]
    msg = "DataFrame diffit report produced incorrect results"
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize("dummy_extra_count,dummy_skewed_count", [(3, 2)])
def test_report_against_differing_dataframes_range_filtering(
    dummy_extra: DataFrame, dummy_skewed: DataFrame
) -> None:
    """Test a diffit report output: with range filter."""
    # Given a set of like Spark SQL DataFrame schemas with different values
    # dummy_extra for the left and dummy_skewed for the right

    # and range filtering
    range_filter = {
        "column": "dummy_col01",
        "lower": 2,
        "upper": 2,
    }

    # when I report on differences
    received: DataFrame = diffit.reporter.row_level(
        left=dummy_extra, right=dummy_skewed, range_filter=range_filter
    ).sort(F.col("dummy_col01"), F.col("dummy_col02"))

    # then I should recieve the different rows across both source DataFrames
    expected = [
        [2, "dummy_col02_val0000000002", "left"],
        [2, "dummy_col02_val0000000003", "right"],
    ]
    msg = "Spark DataFrame diffit report produced incorrect results"
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize("dummy_extra_count,dummy_skewed_count", [(3, 2)])
def test_report_against_differing_dataframes_with_dropped_col(
    dummy_extra: DataFrame, dummy_skewed: DataFrame
) -> None:
    """Test a diffit report output: dropped column."""
    # Given a set of like Spark SQL DataFrame schemas with different values
    # dummy_extra for the left and dummy_skewed for the right

    # when I report on differences
    received: DataFrame = diffit.reporter.row_level(
        left=dummy_extra, right=dummy_skewed, columns_to_drop=["dummy_col02"]
    )

    # then I should recieve the different rows across both source DataFrames
    expected = [[3, "left"]]
    msg = "Spark DataFrame diffit report produced incorrect results"
    assert [
        list(row) for row in received.sort("dummy_col01").collect()
    ] == expected, msg


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
    condition: Optional[Column] = diffit.reporter.range_filter_clause(
        dummy_extra.schema, column_name, lower, upper
    )

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
    condition = diffit.reporter.range_filter_clause(
        dummy_extra.schema, column_name, lower, upper
    )

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
    condition = diffit.reporter.range_filter_clause(
        dummy_extra.schema, column_name, lower, upper
    )

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


@pytest.mark.parametrize("dummy_extra_count", [9])
def test_is_supported_range_condition_types_integertype(dummy_extra: DataFrame) -> None:
    """Test is_supported_range_condition_types: pyspark.sql.types.IntegerType."""
    # Given a Spark SQL DataFrame
    # dummy_extra

    # and a column name that exists in the Spark SQL DataFrame
    column_name = "dummy_col01"

    # when I check if the Spark SQL Column IntegerType is supported
    received: bool = diffit.reporter.is_supported_range_condition_types(
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
    received: bool = diffit.reporter.is_supported_range_condition_types(
        dummy_extra.schema[column_name]
    )

    # then the return value should be False
    assert not received, "StringType columns should not be supported"


def test_grouped_rows_unique_rows(diffit_data_df: DataFrame) -> None:
    """Test a diffit row where distinct rows are detected."""
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = "dummy_col01"

    # when I report on distinct rows
    received: DataFrame = diffit.reporter.grouped_rows(
        diffit_data_df, column_key=column_key
    )

    # then I should receive a non-empty DataFrame
    expected = [[3]]
    msg = "Distinct diffit rows should be returned"
    assert [
        list(row) for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg


def test_grouped_rows_matched_rows(diffit_data_df: DataFrame) -> None:
    """Test a diffit row where a change is detected."""
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = "dummy_col01"

    # when I report on modified rows
    received: DataFrame = diffit.reporter.grouped_rows(
        diffit_data_df, column_key=column_key, group_count=2
    )

    # then I should receive a non-empty DataFrame
    expected = [[2]]
    msg = "Modified diffit rows should be returned"
    assert [
        list(row) for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg


def test_distinct_rows(diffit_data_df: DataFrame) -> None:
    """Test the filtering of distinct diffit rows."""
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = "dummy_col01"

    # when I filter distinct rows
    received: DataFrame = diffit.reporter.distinct_rows(
        diffit_data_df, column_key=column_key, diffit_ref="left"
    )

    # then I should receive a non-empty DataFrame
    expected = [[3, "dummy_col03_val03", "left"]]
    msg = "Source DataFrame referenced distinct diffit rows should be returned"
    assert [
        list(row) for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg


def test_distinct_rows_no_match(diffit_data_df: DataFrame) -> None:
    """Test the filtering of distinct diffit rows: no match."""
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = "dummy_col01"

    # when I filter distinct rows
    received: DataFrame = diffit.reporter.distinct_rows(
        diffit_data_df, column_key=column_key, diffit_ref="right"
    )

    # then I should receive a non-empty DataFrame
    expected: List[Text] = []
    msg = "Source DataFrame referenced distinct diffit rows should return an empty DataFrame"
    assert [
        list(row) for row in received.orderBy("dummy_col01").collect()
    ] == expected, msg


def test_altered_rows(diffit_data_df: DataFrame) -> None:
    """Test the filtering of altered diffit rows."""
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = "dummy_col01"

    # when I filter altered rows
    received: DataFrame = diffit.reporter.altered_rows(
        diffit_data_df, column_key=column_key
    ).sort(F.col("dummy_col01"), F.col("diffit_ref"))

    # then I should receive a non-empty DataFrame
    expected = [
        [2, "dummy_col02_val02", "left"],
        [2, "dummy_col02_valXX", "right"],
    ]
    msg = "Source DataFrame referenced altered diffit rows should be returned"
    assert [list(row) for row in received.collect()] == expected, msg


def test_altered_rows_no_match(diffit_data_df: DataFrame) -> None:
    """Test the filtering of altered diffit rows: no match."""
    # Given a Differ DataFrame
    # diffit_data_df (with rows filtered out)

    # and a column name to ensure uniqueness across the rows
    column_key = "dummy_col01"

    # when I filter altered rows
    received: DataFrame = diffit.reporter.altered_rows(
        diffit_data_df.where(F.col(column_key).isNull()), column_key=column_key
    ).sort("dummy_col01")

    # then I should receive a non-empty DataFrame
    expected: List[Text] = []
    msg = "Source DataFrame referenced altered diffit rows should return an empty DataFrame"
    assert [list(row) for row in received.collect()] == expected, msg


def test_altered_rows_column_diffs(diffit_data_df: DataFrame) -> None:
    """Test the altered rows column specific differences."""
    # Given a Differ DataFrame
    # diffit_data_df (with rows filtered out)

    # and a column name to ensure uniqueness across the rows
    column_key: Text = "dummy_col01"

    # and an existing column value to filter against
    column_value: int = 2

    # when I filter altered rows
    received: Iterable[Dict] = diffit.reporter.altered_rows_column_diffs(
        diff=diffit_data_df, column_key=column_key, key_val=column_value
    )

    # then I should receive a non-empty DataFrame
    msg = "Altered rows column-level diff DataFrame should produce results"
    assert list(received) == [{"dummy_col02": "dummy_col02_val02"}], msg


def test_altered_rows_column_diffs_filtered_key_value_missing(
    diffit_data_df: DataFrame,
) -> None:
    """Test the altered rows column specific differences: key not matched."""
    # Given a Differ DataFrame
    # diffit_data_df (with rows filtered out)

    # and a column name to ensure uniqueness across the rows
    column_key = "dummy_col01"

    # and an existing column value to filter against
    column_value = -1

    # when I filter altered rows
    received: Iterable[Dict[Text, Any]] = diffit.reporter.altered_rows_column_diffs(
        diff=diffit_data_df, column_key=column_key, key_val=column_value
    )

    # then I should receive a non-empty DataFrame
    msg = "Altered rows column-level diff DataFrame should produce results"
    assert not list(received), msg
