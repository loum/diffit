"""Unit tests for :module:`files`.

"""
import os
import tempfile
from pyspark.sql import SparkSession

import diffit.files
import diffit.schema


def test_spark_csv_reader(working_dir, spark_context):
    """Read in a custom Seek Anayltics CSV file.
    """
    # Given a path to CSV data
    path_to_csv = None
    with tempfile.NamedTemporaryFile(mode='w', dir=working_dir, delete=False) as _fh:
        for count in range(1, 10):
            _fh.write(f'{count};col02_val0{count}\n')
        path_to_csv = _fh.name

    # and a CSV schema struct context
    schema = diffit.schema.get('Dummy')

    # and a Spark context
    spark = SparkSession(spark_context)

    # when I read into a Spark SQL DataFrame
    received = diffit.files.spark_csv_reader(spark, schema, path_to_csv)

    # then I should receive content
    msg = 'CSV Spark SQL reader DataFrame error'
    assert received.count() == 9, msg


def test_spark_parquet_reader(working_dir, spark_context):
    """Read in a custom Seek Anayltics Parquet file.
    """
    # Given a Spark context
    spark = SparkSession(spark_context)

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
