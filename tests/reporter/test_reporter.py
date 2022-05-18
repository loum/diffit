""":mod:`diffit.reporter` unit test cases.

"""
from pyspark.sql.functions import col

import diffit.reporter


def test_report_against_like_dataframes(data_df):
    """Test a diffit report output.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df for both left and right

    # when I report on differences
    received = diffit.reporter.row_level(left=data_df, right=data_df)

    # then I should receive an empty list
    msg = 'DataFrame diffit report produced incorrect results'
    assert not [list(row) for row in received.collect()], msg


def test_report_against_differing_dataframes(data_df, alt_data_df, extra_data_df):
    """Test a diffit report output.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df + extra_data_df for left and alt_data_df for right
    # Motivated by https://kb.databricks.com/data/append-a-row-to-rdd-or-dataframe.html
    extra_left = data_df.union(extra_data_df)

    # when I report on differences
    received = diffit.reporter.row_level(left=extra_left, right=alt_data_df)

    # then I should recieve the different rows across both source DataFrames
    expected = [
        [2, 'col02_val02', 'left'],
        [2, 'col02_valXX', 'right'],
        [3, 'col02_val03', 'left']
    ]
    msg = 'DataFrame diffit report produced incorrect results'
    assert [list(row) for row in received.orderBy('col01', 'col02').collect()] == expected, msg


def test_report_against_differing_dataframes_range_filtering(data_df, alt_data_df, extra_data_df):
    """Test a diffit report output: with range filter.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df + extra_data_df for left and alt_data_df for right
    # Motivated by https://kb.databricks.com/data/append-a-row-to-rdd-or-dataframe.html
    extra_left = data_df.union(extra_data_df)

    # and range filtering
    range_filter = {
        'column': 'col01',
        'lower': 2,
        'upper': 2,
    }

    # when I report on differences
    received = diffit.reporter.row_level(left=extra_left,
                                         right=alt_data_df,
                                         range_filter=range_filter)

    # then I should recieve the different rows across both source DataFrames
    expected = [
        [2, 'col02_val02', 'left'],
        [2, 'col02_valXX', 'right'],
    ]
    msg = 'DataFrame diffit report produced incorrect results'
    assert [list(row) for row in received.orderBy('col01', 'col02').collect()] == expected, msg


def test_report_against_differing_dataframes_with_dropped_col(data_df, alt_data_df, extra_data_df):
    """Test a diffit report output: dropped column.
    """
    # Given a set of like Spark SQL DataFrames
    # data_df + extra_data_df for left and alt_data_df for right
    # Motivated by https://kb.databricks.com/data/append-a-row-to-rdd-or-dataframe.html
    extra_left = data_df.union(extra_data_df)

    # when I report on differences
    received = diffit.reporter.row_level(left=extra_left,
                                         right=alt_data_df,
                                         columns_to_drop=['col02'])

    # then I should recieve the different rows across both source DataFrames
    expected = [[3, 'left']]
    msg = 'DataFrame diffit report produced incorrect results'
    assert [list(row) for row in received.orderBy('col01').collect()] == expected, msg


def test_range_filter_clause(big_data_df):
    """Test range_filter clause.
    """
    # Given a Spark SQL DataFrame
    # big_data_df

    # and a column name that exists in the Spark SQL DataFrame
    column_name = 'col01'

    # and an inclusive lower boundary bound
    lower = 2

    # and an inclusive upper boundary bound
    upper = 5

    # when I set up the range filter clause
    condition = diffit.reporter.range_filter_clause(big_data_df.schema, column_name, lower, upper)

    # and filter the original Spark SQL DataFrame
    received = big_data_df.filter(condition).select(column_name)

    # then I should get a slice of the original Spark SQL DataFrame
    msg = 'Range filtered Spark SQL DataFrame slice error'
    expected = [2, 3, 4, 5]
    assert [row.col01 for row in received.orderBy('col01').collect()] == expected, msg


def test_range_filter_clause_lower_only(big_data_df):
    """Test range_filter clause: lower boundary only.
    """
    # Given a Spark SQL DataFrame
    # big_data_df

    # and a column name that exists in the Spark SQL DataFrame
    column_name = 'col01'

    # and an inclusive lower boundary bound
    lower = 8

    # and an undefined inclusive upper boundary bound
    upper = None

    # when I set up the range filter clause
    condition = diffit.reporter.range_filter_clause(big_data_df.schema, column_name, lower, upper)

    # and filter the original Spark SQL DataFrame
    received = big_data_df.filter(condition).select(column_name)

    # then I should get a slice of the original Spark SQL DataFrame
    msg = 'Range filtered Spark SQL DataFrame slice error: lower boundary only'
    expected = [8, 9]
    assert [row.col01 for row in received.orderBy('col01').collect()] == expected, msg


def test_range_filter_clause_upper_only(big_data_df):
    """Test range_filter clause: upper boundary only.
    """
    # Given a Spark SQL DataFrame
    # big_data_df

    # and a column name that exists in the Spark SQL DataFrame
    column_name = 'col01'

    # and an undefined inclusive lower boundary bound
    lower = None

    # and an inclusive upper boundary bound
    upper = 2

    # when I set up the range filter clause
    condition = diffit.reporter.range_filter_clause(big_data_df.schema, column_name, lower, upper)

    # and filter the original Spark SQL DataFrame
    received = big_data_df.filter(condition).select(column_name)

    # then I should get a slice of the original Spark SQL DataFrame
    msg = 'Range filtered Spark SQL DataFrame slice error: lower boundary only'
    expected = [1, 2]
    assert [row.col01 for row in received.orderBy('col01').collect()] == expected, msg


def test_is_supported_range_condition_types_integertype(big_data_df):
    """Test is_supported_range_condition_types: pyspark.sql.types.IntegerType.
    """
    # Given a Spark SQL DataFrame
    # big_data_df

    # and a column name that exists in the Spark SQL DataFrame
    column_name = 'col01'

    # when I check if the Spark SQL Column IntegerType is supported
    received = diffit.reporter.is_supported_range_condition_types(big_data_df.schema[column_name])

    # then the return value should be True
    assert received, 'IntegerType columns should be supported'


def test_is_supported_range_condition_types_not_supported(big_data_df):
    """Test is_supported_range_condition_types: not supported.
    """
    # Given a Spark SQL DataFrame
    # big_data_df

    # and a column name that exists in the Spark SQL DataFrame
    # NOTE: StringTypes currently not supported -- but that may change ...
    column_name = 'col02'

    # when I check if the Spark SQL Column StringType is supported
    received = diffit.reporter.is_supported_range_condition_types(big_data_df.schema[column_name])

    # then the return value should be False
    assert not received, 'StringType columns should not be supported'


def test_grouped_rows_unique_rows(diffit_data_df):
    """Test a diffit row where distinct rows are detected.
    """
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = 'col01'

    # when I report on distinct rows
    received = diffit.reporter.grouped_rows(diffit_data_df, column_key=column_key)

    # then I should receive a non-empty DataFrame
    expected = [[3]]
    msg = 'Distinct diffit rows should be returned'
    assert [list(row) for row in received.orderBy('col01').collect()] == expected, msg


def test_grouped_rows_matched_rows(diffit_data_df):
    """Test a diffit row where a change is detected.
    """
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = 'col01'

    # when I report on modified rows
    received = diffit.reporter.grouped_rows(diffit_data_df, column_key=column_key, group_count=2)

    # then I should receive a non-empty DataFrame
    expected = [[2]]
    msg = 'Modified diffit rows should be returned'
    assert [list(row) for row in received.orderBy('col01').collect()] == expected, msg


def test_distinct_rows(diffit_data_df):
    """Test the filtering of distinct diffit rows.
    """
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = 'col01'

    # when I filter distinct rows
    received = diffit.reporter.distinct_rows(diffit_data_df,
                                             column_key=column_key,
                                             diffit_ref='left')

    # then I should receive a non-empty DataFrame
    expected = [[3, 'col03_val03', 'left']]
    msg = 'Source DataFrame referenced distinct diffit rows should be returned'
    assert [list(row) for row in received.orderBy('col01').collect()] == expected, msg


def test_distinct_rows_no_match(diffit_data_df):
    """Test the filtering of distinct diffit rows: no match.
    """
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = 'col01'

    # when I filter distinct rows
    received = diffit.reporter.distinct_rows(diffit_data_df,
                                             column_key=column_key,
                                             diffit_ref='right')

    # then I should receive a non-empty DataFrame
    expected = []
    msg = 'Source DataFrame referenced distinct diffit rows should return an empty DataFrame'
    assert [list(row) for row in received.orderBy('col01').collect()] == expected, msg


def test_altered_rows(diffit_data_df):
    """Test the filtering of altered diffit rows.
    """
    # Given a Differ DataFrame
    # diffit_data_df

    # and a column name to ensure uniqueness across the rows
    column_key = 'col01'

    # when I filter altered rows
    received = diffit.reporter.altered_rows(diffit_data_df, column_key=column_key)

    # then I should receive a non-empty DataFrame
    expected = [
        [2, 'col02_val02', 'left'],
        [2, 'col02_valXX', 'right']
    ]
    msg = 'Source DataFrame referenced altered diffit rows should be returned'
    assert [list(row) for row in received.orderBy('col01', 'diffit_ref').collect()] == expected, msg


def test_altered_rows_no_match(diffit_data_df):
    """Test the filtering of altered diffit rows: no match.
    """
    # Given a Differ DataFrame
    # diffit_data_df (with rows filtered out)

    # and a column name to ensure uniqueness across the rows
    column_key = 'col01'

    # when I filter altered rows
    received = diffit.reporter.altered_rows(diffit_data_df.where(col(column_key).isNull()),
                                            column_key=column_key)

    # then I should receive a non-empty DataFrame
    expected = []
    msg = 'Source DataFrame referenced altered diffit rows should return an empty DataFrame'
    assert [list(row) for row in received.orderBy('col01').collect()] == expected, msg


def test_altered_rows_column_diffs(diffit_data_df):
    """Test the altered rows column specific differences.
    """
    # Given a Differ DataFrame
    # diffit_data_df (with rows filtered out)

    # and a column name to ensure uniqueness across the rows
    column_key = 'col01'

    # and an existing column value to filter against
    column_value = 2

    # when I filter altered rows
    received = diffit.reporter.altered_rows_column_diffs(diff=diffit_data_df,
                                                         column_key=column_key,
                                                         key_val=column_value)

    # then I should receive a non-empty DataFrame
    msg = 'Altered rows column-level diff DataFrame should produce results'
    assert list(received) == [{'col02': 'col02_val02'}], msg


def test_altered_rows_column_diffs_filtered_key_value_missing(diffit_data_df):
    """Test the altered rows column specific differences: key not matched.
    """
    # Given a Differ DataFrame
    # diffit_data_df (with rows filtered out)

    # and a column name to ensure uniqueness across the rows
    column_key = 'col01'

    # and an existing column value to filter against
    column_value = -1

    # when I filter altered rows
    received = diffit.reporter.altered_rows_column_diffs(diff=diffit_data_df,
                                                         column_key=column_key,
                                                         key_val=column_value)

    # then I should receive a non-empty DataFrame
    msg = 'Altered rows column-level diff DataFrame should produce results'
    assert not list(received), msg
