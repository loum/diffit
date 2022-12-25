"""Unit tests for :module:`files`.

"""
import os
import tempfile
from pyspark.sql import SparkSession

import diffit.files
import diffit.schema


def test_spark_csv_reader_delimiter_as_semicolon(working_dir, spark):
    """Read in a CSV file: delimited with semi-colon.
    """
    # Given a path to CSV data delimited with a semi-colon (";")
    path_to_csv = None
    with tempfile.NamedTemporaryFile(mode='w', dir=working_dir, delete=False) as _fh:
        _fh.write('col01;col02\n')
        for count in range(1, 10):
            _fh.write(f'{count};col02_val0{count}\n')
        path_to_csv = _fh.name

    # and a CSV schema struct context
    schema = diffit.schema.get('Dummy')

    # and a SparkSession
    # spark

    # and override the CSV column delimiter with a semi-colon
    delimiter = ';'

    # when I read into a Spark SQL DataFrame
    received = diffit.files.spark_csv_reader(spark, schema, path_to_csv, delimiter)

    # then I should receive content
    msg = 'CSV semi-colon delimted Spark SQL reader DataFrame error'
    assert received.count() == 9, msg


def test_spark_csv_reader_delimiter_as_comma(working_dir, spark):
    """Read in a custom CSV file: delimiter as comma.
    """
    # Given a path to CSV data delimited with a comma (",")
    path_to_csv = None
    with tempfile.NamedTemporaryFile(mode='w', dir=working_dir, delete=False) as _fh:
        _fh.write('col01,col02\n')
        for count in range(1, 10):
            _fh.write(f'{count},col02_val0{count}\n')
        path_to_csv = _fh.name

    # and a CSV schema struct context
    schema = diffit.schema.get('Dummy')

    # and a SparkSession
    # spark

    # when I read into a Spark SQL DataFrame
    received = diffit.files.spark_csv_reader(spark, schema, path_to_csv)

    # then I should receive content
    msg = 'CSV comma delimted Spark SQL reader DataFrame error'
    assert received.count() == 9, msg


def test_spark_parquet_reader(working_dir, spark):
    """Read in a custom Parquet file.
    """
    # Given a SparkSession
    # spark

    # and a path to parquet data
    data =[
        ('James', '', 'Smith', '36636', 'M', 3000),
        ('Michael', 'Rose', '', '40288', 'M', 4000),
        ('Robert', '', 'Williams', '42114', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '39192', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '', 'F', -1)
    ]
    columns = ['firstname', 'middlename', 'lastname', 'dob', 'gender', 'salary']
    _df = spark.createDataFrame(data, columns)
    parquet_path = os.path.join(working_dir, 'parquet.out')
    _df.write.parquet(parquet_path)

    # when I read into a Spark SQL DataFrame
    received = diffit.files.spark_parquet_reader(spark, parquet_path)

    # then I should receive content
    msg = 'Parquet Spark SQL reader DataFrame error'
    assert received.count() == 5, msg


def test_split_dir():
    """Split directory path on a directory name.
    """
    # Given a directory path
    directory_path = os.path.join('data',
                                  'dependencies.zip',
                                  'data',
                                  'Dummy.json')

    # and a directory name to split against
    directory_token = 'dependencies.zip'

    # when I split the directory path against the token
    received = diffit.files.split_dir(directory_path, directory_token)

    # then I should received a new directory path
    msg = 'Directory split against known part should return reduced directory path'
    assert received == 'data/Dummy.json', msg


def test_split_dir_no_token_match():
    """Split directory path on a directory name: no token match.
    """
    # Given a directory path
    directory_path = os.path.join('data',
                                  'dependencies.zip',
                                  'data',
                                  'Dummy.json')

    # and a directory name to split against
    directory_token = 'banana'

    # when I split the directory path against the token
    received = diffit.files.split_dir(directory_path, directory_token)

    # then I should received None
    msg = 'Directory split against unknown part should return None'
    assert not received, msg


def test_sanitise_columns(spark):
    """Santise columns that are acceptable parquet.
    """
    # Given a SparkSession
    # spark

    # and a path to parquet data
    data =[
        ('James', '', 'Smith', '36636', 'M', 3000),
    ]
    columns = ['firstname', 'middlename', 'lastname', 'dob', 'gender', 'salary']
    _df = spark.createDataFrame(data, columns)

    # when I santise the DataFrame columns
    received = diffit.files.sanitise_columns(_df)

    # then I should receive content
    msg = 'Sanitising good parquet columns should not change the columns names'
    assert received.columns == columns, msg


def test_sanitise_columns_rejected_characters(spark):
    """Santise columns that do not align with parquet naming convention.
    """
    # Given a SparkSession
    # spark

    # and a path to parquet data
    data =[
        ('James', '', 'Smith', '36636', 'M', 3000),
    ]
    columns = ['First name;', 'Middle, name', '{lastname}', '(DOB)', 'gender=', 'salary']
    _df = spark.createDataFrame(data, columns)

    # when I santise the DataFrame columns
    received = diffit.files.sanitise_columns(_df)

    # then I should receive content
    expected = ['first_name', 'middle_name', 'lastname', 'dob', 'gender', 'salary']
    msg = 'Sanitising parquet columns should change the columns names'
    assert received.columns == expected, msg
