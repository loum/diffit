""":mod:`diffit` unit test cases.

"""
import diffit
import diffit.utils


def test_row_level(data_df):
    """Row-level DataFrame diff check: no differences.
    """
    # Given a Spark SQL DataFrame
    # data_df

    # when I check for differences at the row level
    received = diffit.row_level(left=data_df, right=data_df)

    # then I should receive no differences
    msg = 'Like DataFrames should not detect a difference'
    assert not [list(row) for row in received.collect()], msg


def test_row_level_extra_row_in_left(data_df, extra_data_df):
    """Row-level DataFrame diff check: extra row in left DataFrame.
    """
    # Given a set of different Spark SQL DataFrames
    # data_df
    # Motivated by https://kb.databricks.com/data/append-a-row-to-rdd-or-dataframe.html
    extra_left = data_df.union(extra_data_df)

    # when I check for differences at the row level
    received = diffit.row_level(left=extra_left, right=data_df)

    # then I should receive a difference
    expected = [[3, 'col02_val03']]
    msg = 'Left-extra DataFrame should detect a difference'
    assert [list(row) for row in received.collect()] == expected, msg


def test_row_level_different_row_in_left(data_df, alt_data_df):
    """Row-level DataFrame diff check: different row in left DataFrame.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df and alt_data_df

    # when I check for differences at the row level
    received = diffit.row_level(data_df, alt_data_df)

    # then I should receive a difference
    expected = [[2, 'col02_val02']]
    msg = 'Alternate right DataFrame should detect a difference'
    assert [list(row) for row in received.collect()] == expected, msg


def test_row_level_extra_row_in_right(data_df, extra_data_df):
    """Row-level DataFrame diff check: extra row in right DataFrame.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df
    # Motivated by https://kb.databricks.com/data/append-a-row-to-rdd-or-dataframe.html
    extra_right = data_df.union(extra_data_df)

    # when I check for differences at the row level
    received = diffit.row_level(left=data_df, right=extra_right)

    # then I should receive no differences
    msg = 'Left-extra DataFrame should detect a difference'
    assert not [list(row) for row in received.collect()], msg


def test_symmetric(data_df):
    """Symmetric DataFrame diff check: no differences.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df for both left and right

    # when I check for differences at the row level
    received = diffit.symmetric_level(left=data_df, right=data_df)

    # then I should receive no differences
    msg = 'Like DataFrames should not detect a difference'
    assert not [list(row) for row in received.collect()], msg


def test_symmetric_extra_row_in_left(data_df, extra_data_df):
    """Symmetric DataFrame diff check: extra row in left DataFrame.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df for right
    # Motivated by https://kb.databricks.com/data/append-a-row-to-rdd-or-dataframe.html
    extra_left = data_df.union(extra_data_df)

    # when I check for differences at the row level
    received = diffit.symmetric_level(extra_left, right=data_df)

    # then I should receive a difference
    expected = [[3, 'col02_val03']]
    msg = 'Left-extra DataFrame should detect a difference'
    assert [list(row) for row in received.collect()] == expected, msg


def test_symmetric_different_row_in_left(data_df, alt_data_df):
    """Symmetric DataFrame diff check: different row in left DataFrame.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df for left and alt_data_df for right

    # when I check for differences at the row level
    received = diffit.symmetric_level(left=data_df, right=alt_data_df)

    # then I should receive a difference
    expected = [[2, 'col02_val02'], [2, 'col02_valXX']]
    msg = 'Alternate right DataFrame should detect a difference'
    assert [list(row) for row in received.collect()] == expected, msg


def test_symmetric_different_row_in_right(alt_data_df, data_df):
    """Symmetric DataFrame diff check: different row in right DataFrame.
    """
    # Given a set of like Spark SQL DataFrames
    # alt_data_df for left and data_df for right

    # when I check for differences at the row level
    received = diffit.symmetric_level(left=alt_data_df, right=data_df)

    # then I should receive a difference
    expected = [[2, 'col02_val02'], [2, 'col02_valXX']]
    msg = 'Alternate right DataFrame should detect a difference'
    assert sorted([list(row) for row in received.collect()]) == expected, msg


def test_symmetric_extra_row_in_right(data_df, extra_data_df):
    """Row-level DataFrame diff check: extra row in right DataFrame.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df
    # Motivated by https://kb.databricks.com/data/append-a-row-to-rdd-or-dataframe.html
    extra_right = data_df.union(extra_data_df)

    # when I check for differences at the row level
    received = diffit.symmetric_level(left=data_df, right=extra_right)

    # then I should receive a difference
    expected = [[3, 'col02_val03']]
    msg = 'Left-extra DataFrame should detect a difference'
    assert [list(row) for row in received.collect()] == expected, msg


def test_symmetric_filter_left_orientation(data_df, symmetric_data_df):
    """Differ reporter: left DataFrame as a reference.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df for target and symmetric_data_df for the right

    # when I check for differences at the row level
    received = diffit.symmetric_filter(target=data_df, symmetric=symmetric_data_df)

    # then I should receive a difference
    expected = [[2, 'col02_val02', 'left']]
    msg = 'Alternate right DataFrame should detect a difference'
    assert [list(row) for row in received.collect()] == expected, msg


def test_symmetric_filter_right_orientation(alt_data_df, symmetric_data_df):
    """Differ reporter: right DataFrame as a reference.
    """
    # Given a set of like Spark SQL DataFrames
    # alt_data_df for target and symmetric_data_df for left

    # when I check for differences at the row level
    received = diffit.symmetric_filter(target=alt_data_df,
                                       symmetric=symmetric_data_df,
                                       orientation='right')

    # then I should receive a difference
    expected = [[2, 'col02_valXX', 'right']]
    msg = 'Alternate right DataFrame should detect a difference'
    assert [list(row) for row in received.collect()] == expected, msg


def test_column_level_diff(data_df, alt_data_df):
    """Test DataFrame column-level diff check.
    """
    # Given a set of different Spark SQL DataFrames
    left = data_df
    right = alt_data_df

    # when I check for diffs at the column level
    received = diffit.column_level_diff(left, right)

    # then I should receive ...
    msg = 'Column level diff of different DataFrames should produce a result'
    assert list(received) == [{'col02': 'col02_val02'}], msg


def test_column_level_diff_with_no_diff_to_report(data_df):
    """Test DataFrame column-level diff check: no diff to report.
    """
    # Given a set of different like Spark SQL DataFrames
    left = right = data_df

    # when I check for diffs at the column level
    received = diffit.column_level_diff(left, right)

    # then I should receive ...
    msg = 'Column level diff of different DataFrames should produce a result'
    assert not list(received), msg
