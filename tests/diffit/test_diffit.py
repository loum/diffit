""":mod:`diffit` unit test cases.

"""
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pytest

import diffit
import diffit.utils


@pytest.mark.parametrize('dummy_count', [2])
def test_right_subtract_row_level(dummy: DataFrame):
    """Right subtract row-level Spark DataFrame diff check: no differences.
    """
    # Given a Spark SQL DataFrame
    # dummy

    # when I check for differences at the right subtract row level
    received: DataFrame = diffit.right_subtract_row_level(left=dummy, right=dummy)

    # then I should receive no differences
    msg = 'Like Spark DataFrames should not detect a difference'
    assert not [list(row) for row in received.collect()], msg


@pytest.mark.parametrize('dummy_count,dummy_extra_count', [(2,3)])
def test_right_subtract_row_level_extra_row_left(dummy: DataFrame, dummy_extra: DataFrame):
    """Right subtract row-level Spark DataFrame diff check: extra row in left DataFrame.
    """
    # Given a Spark SQL DataFrame
    # dummy

    # and a second Spark SQL DataFrame with an extra row
    # dummy_extra

    # when I check for differences at the right subtract left-row level
    received: DataFrame = diffit.right_subtract_row_level(left=dummy_extra, right=dummy)

    # then I should receive a difference
    expected = [[3, 'dummy_col02_val0000000003']]
    msg = 'Right Spark DataFrame should detect an extra row'
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize('dummy_count,dummy_extra_count', [(2,3)])
def test_right_subtract_row_level_extra_row_right(dummy: DataFrame, dummy_extra: DataFrame):
    """Right subtract row-level Spark DataFrame diff check: extra row in right DataFrame.
    """
    # Given a Spark SQL DataFrame
    # dummy

    # and a second Spark SQL DataFrame with an extra row
    # dummy_extra

    # when I check for differences at the right subtract left-row level
    received: DataFrame = diffit.right_subtract_row_level(left=dummy, right=dummy_extra)

    # then I should receive a difference
    expected = []
    msg = 'Left Spark DataFrame should not detect an extra row'
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize('dummy_count,dummy_skewed_count', [(2,2)])
def test_row_level_different_row_in_left(dummy: DataFrame, dummy_skewed: DataFrame):
    """Row-level Spark DataFrame diff check: different row in left Spark DataFrame.
    """
    # Given a set of different Spark SQL DataFrames
    # dummy and dummy_skewed

    # when I check for differences at the row level
    received: DataFrame = diffit.right_subtract_row_level(dummy, dummy_skewed)

    # then I should receive a difference
    expected = [[2, 'dummy_col02_val0000000002'], [1, 'dummy_col02_val0000000001']]
    msg = 'Alternate right Spark DataFrame should detect a difference'
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize('dummy_count', [2])
def test_symmetric(dummy: DataFrame):
    """Symmetric Spark DataFrame diff check: no differences.
    """
    # Given a set of like Spark SQL DataFrames
    # dummy for both left and right

    # when I check for differences at the row level
    received: DataFrame = diffit.symmetric_level(left=dummy, right=dummy)

    # then I should receive no differences
    msg = 'Like Spark DataFrames should not detect a difference'
    assert not [list(row) for row in received.collect()], msg


@pytest.mark.parametrize('dummy_count,dummy_skewed_count', [(2,3)])
def test_symmetric_different_rows_left_control(dummy: DataFrame, dummy_skewed: DataFrame):
    """Symmetric Spark DataFrame diff check: left control to diffit left.
    """
    # Given a set of same schema, different valued Spark SQL DataFrames
    # dummy and dummy_skewed

    # when I check for symmetric differences at the row level
    received: DataFrame = diffit.symmetric_level(left=dummy, right=dummy_skewed)\
        .sort(F.col('dummy_col01'), F.col('dummy_col02'))

    # then I should receive a difference
    expected = [
        [1, 'dummy_col02_val0000000001'],
        [1, 'dummy_col02_val0000000002'],
        [2, 'dummy_col02_val0000000002'],
        [2, 'dummy_col02_val0000000003'],
        [3, 'dummy_col02_val0000000004'],
    ]
    msg = 'Symmetric check over different Spark DataFrames should detect difference'
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize('dummy_skewed_count,dummy_count', [(3,2)])
def test_symmetric_different_rows_left_control_on_right(dummy_skewed: DataFrame, dummy: DataFrame):
    """Symmetric Spark DataFrame diff check: left control placed on diffit right.
    """
    # Given a set of same schema, different valued Spark SQL DataFrames
    # dummy_skewed for left and dummy for right

    # when I check for differences at the row level
    received: DataFrame = diffit.symmetric_level(left=dummy_skewed, right=dummy)\
        .sort(F.col('dummy_col01'), F.col('dummy_col02'))

    # then I should receive a difference
    expected = [
        [1, 'dummy_col02_val0000000001'],
        [1, 'dummy_col02_val0000000002'],
        [2, 'dummy_col02_val0000000002'],
        [2, 'dummy_col02_val0000000003'],
        [3, 'dummy_col02_val0000000004'],
    ]
    msg = ('Symmetric check over different Spark DataFrames (left control on right)'
           'should detect difference')
    assert sorted([list(row) for row in received.collect()]) == expected, msg


@pytest.mark.parametrize('dummy_count,dummy_extra_count', [(2,3)])
def test_symmetric_extra_row_in_right(dummy: DataFrame, dummy_extra: DataFrame):
    """Symmetric Spark DataFrame diff check: extra row in right Spark DataFrame.
    """
    # Given Spark SQL DataFrame
    # dummy

    # and another Spark DataFrame with same schema, but extra row
    # dummy_extra

    # when I check for symmetric differences at the row level
    received: DataFrame = diffit.symmetric_level(left=dummy, right=dummy_extra)

    # then I should receive a difference
    expected = [[3, 'dummy_col02_val0000000003']]
    msg = 'Symmetric diff should find extra fow in right Spark DataFrame'
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize('dummy_extra_count,dummy_count', [(3,2)])
def test_symmetric_extra_row_in_left(dummy_extra: DataFrame, dummy: DataFrame):
    """Symmetric Spark DataFrame diff check: extra row in left Spark DataFrame.
    """
    # Given Spark SQL DataFrame with same schema, but extra row
    # dummy_extra

    # and another Spark DataFrame
    # dummy

    # when I check for symmetric differences at the row level
    received: DataFrame = diffit.symmetric_level(left=dummy_extra, right=dummy)

    # then I should receive a difference
    expected = [[3, 'dummy_col02_val0000000003']]
    msg = 'Symmetric diff should find extra fow in left Spark DataFrame'
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize('dummy_count', [2])
def test_symmetric_filter_left_orientation(dummy: DataFrame, symmetric_data_df: DataFrame):
    """Differ reporter: left Spark DataFrame as a reference.
    """
    # Given a Spark DataFrame
    # dummy

    # and a Spark DataFrame that has captured the symmetric differences from a previous step
    # symmetric_data_df

    # when I filter against the left diffit target
    received: DataFrame = diffit.symmetric_filter(
            target=dummy,
            symmetric=symmetric_data_df)\
        .sort(F.col('dummy_col01'), F.col('dummy_col02'))

    # then I should receive the left-orientation differences
    expected = [
        [1, 'dummy_col02_val0000000001', 'left'],
        [2, 'dummy_col02_val0000000002', 'left'],
    ]
    msg = 'Symmetric filter should detect differences that are left-oriented'
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize('dummy_skewed_count', [2])
def test_symmetric_filter_right_orientation(dummy_skewed: DataFrame, symmetric_data_df: DataFrame):
    """Differ reporter: right Spark DataFrame as a reference.
    """
    # Given a Spark DataFrame
    # dummy_skewed

    # and a Spark DataFrame that has captured the symmetric differences from a previous step
    # symmetric_data_df

    # when I filter against the right diffit target
    received: DataFrame = diffit.symmetric_filter(
            target=dummy_skewed,
            symmetric=symmetric_data_df,
            orientation='right')\
        .sort(F.col('dummy_col01'), F.col('dummy_col02'))

    # then I should receive a difference
    expected = [
        [1, 'dummy_col02_val0000000002', 'right'],
        [2, 'dummy_col02_val0000000003', 'right'],
    ]
    msg = 'Symmetric filter should detect differences that are left-oriented'
    assert [list(row) for row in received.collect()] == expected, msg


@pytest.mark.parametrize('dummy_count,dummy_skewed_count', [(2,2)])
def test_column_level_diff(dummy: DataFrame, dummy_skewed: DataFrame):
    """Test Spark DataFrame column-level diff check.
    """
    # Given a set of same schema, different valued Spark SQL DataFrames
    left = dummy
    right = dummy_skewed

    # when I check for diffs at the column level
    received: DataFrame = diffit.column_level_diff(left, right)

    # then the results should present as a list of dictionaries
    msg = 'Column level diff of different Spark DataFrames should produce a list of dictionaries'
    expected = [{'dummy_col02': 'dummy_col02_val0000000001'}]
    assert list(received) == expected, msg


@pytest.mark.parametrize('dummy_count', [2])
def test_column_level_diff_with_no_diff_to_report(dummy: DataFrame):
    """Test Spark DataFrame column-level diff check: no diff to report.
    """
    # Given a set of different like Spark SQL DataFrames
    left = right = dummy

    # when I check for diffs at the column level
    received: DataFrame = diffit.column_level_diff(left, right)

    # then the results should present as an empty list
    msg = 'Column level diff of different Spark DataFrames should produce a result'
    assert not list(received), msg
