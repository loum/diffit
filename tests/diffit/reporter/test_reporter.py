"""`diffit.reporter` unit test cases.

"""
from typing import Any, Dict, Iterable, List, Text

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pytest

from diffit.reporter.rangefilter import RangeFilter
import diffit.reporter


# Tuple elements:
# 1. List of columns to add.
# 2. List of columns to drop.
# 3. <expected_result>

# Test scenarios:
# 1. No columns to drop: no changes to original list.
# 2. No columns to drop in unordered list: original list sorted.
# 3. One column dropped: original list adjusted.
# 4. All columns dropped: empty list.


GET_COLUMNS_ARGS = (
    (["dummy_col01", "dummy_col02"], [], ["dummy_col01", "dummy_col02"]),
    (["dummy_col02", "dummy_col01"], [], ["dummy_col01", "dummy_col02"]),
    (["dummy_col01", "dummy_col02"], ["dummy_col02"], ["dummy_col01"]),
    (["dummy_col01", "dummy_col02"], ["dummy_col01", "dummy_col02"], []),
)


@pytest.mark.parametrize(
    "columns_to_add,columns_to_drop,columns_result", GET_COLUMNS_ARGS
)
def test_get_columns(
    columns_to_add: List[Text], columns_to_drop: List[Text], columns_result: List[Text]
) -> None:
    """Determine the columns to include in the symantic check."""
    # Given a list of columns to add to the check
    # columns_to_add

    # and a list of columns to drop from the check
    # columns_to_drop

    # when I generate the list of columns to check
    columns: List[Text] = diffit.reporter.get_columns(columns_to_add, columns_to_drop)

    # then the resultant list should match
    msg = "List of columns generated error"
    assert columns == columns_result, msg


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

    # then I should receive the different rows across both source DataFrames
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
    range_filter = RangeFilter(column="dummy_col01", lower=2, upper=2)

    # when I report on differences
    received: DataFrame = diffit.reporter.row_level(
        left=dummy_extra, right=dummy_skewed, range_filter=range_filter
    ).sort(F.col("dummy_col01"), F.col("dummy_col02"))

    # then I should receive the different rows across both source DataFrames
    expected = [
        [2, "dummy_col02_val0000000002", "left"],
        [2, "dummy_col02_val0000000003", "right"],
    ]
    msg = "Spark DataFrame diffit report produced incorrect results"
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize("dummy_extra_count,dummy_skewed_count", [(3, 2)])
def test_report_against_differing_dataframes_with_added_col(
    dummy_extra: DataFrame, dummy_skewed: DataFrame
) -> None:
    """Test a diffit report output: added column."""
    # Given a set of like Spark SQL DataFrame schemas with different values
    # dummy_extra for the left and dummy_skewed for the right

    # when I report on differences
    received: DataFrame = diffit.reporter.row_level(
        left=dummy_extra,
        right=dummy_skewed,
        columns_to_add=["dummy_col01", "dummy_col02"],
    )

    # then I should receive the different rows across both source DataFrames
    expected = [
        [1, "dummy_col02_val0000000001", "left"],
        [1, "dummy_col02_val0000000002", "right"],
        [2, "dummy_col02_val0000000002", "left"],
        [2, "dummy_col02_val0000000003", "right"],
        [3, "dummy_col02_val0000000003", "left"],
    ]
    msg = "Spark DataFrame diffit report with columns added produced incorrect results"
    assert [
        list(row) for row in received.sort("dummy_col01").collect()
    ] == expected, msg


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

    # then I should receive the different rows across both source DataFrames
    expected = [[3, "left"]]
    msg = "Spark DataFrame diffit report produced incorrect results"
    assert [
        list(row) for row in received.sort("dummy_col01").collect()
    ] == expected, msg


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
